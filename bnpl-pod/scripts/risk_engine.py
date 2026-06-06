"""
Gray-Swan Risk Engine
=====================

Portfolio-aware position sizing, stop placement, and dynamic trailing,
all derived from a single CVaR budget parameter.

Architecture follows the locked spec:
  Tier 2 (gray swan) only -- routine + statistical-tail risk.
  Tier 3 (black swan) -- concentration caps, EVT, convex hedges, scenario tests
                          live in a separate module (risk_engine_tier3.py).

Dynamic by design:
  * All tunables in RiskConfig dataclass
  * JSON config hot-reloads on every pass (RISK_ENGINE_CONFIG env var or
    ./risk_engine_config.json)
  * Per-name conviction / archetype come from a SQLite table that the
    engine auto-creates; can be populated by any upstream BSI pipeline.
  * GARCH(1,1) sigma with graceful EWMA fallback
  * Multi-account safe (--username flag)

CLI
---
  python risk_engine.py                       # live morning pass for default account
  python risk_engine.py --username <name>     # other account
  python risk_engine.py --dry-run             # show actions, no DB writes
  python risk_engine.py --explain PYPL        # full math trace for one name
  python risk_engine.py --status              # current regime + utilization

Environment
-----------
  APOLLO_DB           SQLite path holding portfolio + journal tables.
                      Defaults to <repo>/bnpl-pod/data/apollo.db.
  RISK_ENGINE_CONFIG  Optional JSON file with RiskConfig overrides.
                      Defaults to ./risk_engine_config.json next to this script.

Academic anchors
----------------
  Rockafellar & Uryasev (2000) -- CVaR optimization
  Maillard, Roncalli & Teiletche (2010) -- risk-parity allocation
  Ang, Chen & Xing (2006) -- downside beta
  Engle & Patton (2007) -- volatility model evaluation
  Daniel & Moskowitz (2016) -- momentum-crash regime overlay
  Pearson (2002), "Risk Budgeting" (Wiley) -- portfolio VaR allocation
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
import warnings
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Configuration  --  everything tunable lives here
# ---------------------------------------------------------------------------

DEFAULT_DB = os.environ.get(
    "APOLLO_DB",
    str(Path(__file__).resolve().parents[1] / "data" / "apollo.db"),
)
DEFAULT_CONFIG_PATH = (
    Path(os.environ.get("RISK_ENGINE_CONFIG", "")) if os.environ.get("RISK_ENGINE_CONFIG")
    else Path(__file__).with_name("risk_engine_config.json")
)


@dataclass
class RiskConfig:
    """Single source of truth for every tunable parameter."""

    # ---- portfolio anchor ----
    portfolio_cvar_budget_pct: float = 0.03        # 3% of capital, daily 95% CVaR
    cvar_confidence: float = 0.95
    cash_buffer_pct: float = 0.10                  # keep at least 10% cash
    single_name_cap_pct: float = 0.25              # Tier-3 hard concentration cap

    # ---- volatility estimation ----
    garch_lookback_days: int = 252
    ewma_lambda: float = 0.94                      # RiskMetrics standard
    min_history_days: int = 60
    sector_fallback_sigma: float = 0.035           # 3.5%/day for thin-history names

    # ---- downside beta ----
    downside_beta_window: int = 60
    downside_beta_floor: float = 0.5               # never oversize on defensive names

    # ---- trailing stop (Chandelier) ----
    trail_activation_mfe: float = 0.20             # only trail above +20% MFE
    mfe_lock_pct: float = 0.25                     # lock at least 25% of MFE
    archetype_k_base: dict = field(default_factory=lambda: {
        "BLITZ":    1.5,
        "SCOUT":    2.0,
        "ROBO":     2.5,
        "GUARDIAN": 3.5,
    })
    bsi_trend_multiplier: dict = field(default_factory=lambda: {
        "rising":  1.5,   # let it run when thesis intensifying
        "stable":  1.0,
        "falling": 0.5,   # tighten when thesis cooling
    })
    bsi_trend_threshold: float = 0.3               # 5-day delta-z threshold

    # ---- milestones (independent of trail) ----
    milestone_levels_pct: list = field(default_factory=lambda: [0.50, 1.00])
    milestone_partial_fraction: float = 1.0 / 3.0

    # ---- regime overlay (Daniel-Moskowitz) ----
    regime_short_window: int = 20
    regime_long_window: int = 60
    regime_panic_ratio: float = 2.0                # tighten everything in panic
    regime_normal_ratio: float = 1.5               # hysteresis floor
    regime_panic_size_multiplier: float = 0.5
    regime_panic_stop_multiplier: float = 0.5

    # ---- conviction mapping (BSI z -> 0..1) ----
    conviction_z_floor: float = 1.0                # below this, no allocation
    conviction_z_ceiling: float = 4.0              # saturate
    conviction_default: float = 0.5                # if BSI snapshot missing

    # ---- closed-loop writeback (Gap 1 fix) ----
    apply_writeback: bool = True                   # write trail stops back to journal.stop_price
    apply_flatten: bool = True                     # close positions on FLATTEN actions (e.g. earnings shield)
    apply_trim: bool = False                       # leave TRIM as advisory until partial-close is wired

    # ---- exit engine (T-2 earnings shield + 5-trigger overlay) ----
    earnings_shield_days: int = 2                  # flatten T-N trading days before earnings
    earnings_shield_enabled: bool = True

    @classmethod
    def load(cls, path: Optional[Path] = None) -> "RiskConfig":
        """Hot-reload from JSON if available; otherwise use defaults."""
        path = path or DEFAULT_CONFIG_PATH
        cfg = cls()
        if path and path.exists():
            try:
                overrides = json.loads(path.read_text())
                for k, v in overrides.items():
                    if hasattr(cfg, k):
                        setattr(cfg, k, v)
            except Exception as e:
                print(f"[risk_engine] config load failed ({path}): {e}; using defaults",
                      file=sys.stderr)
        return cfg

    def save_template(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=2, default=str))


# ---------------------------------------------------------------------------
# Schema migration -- idempotent
# ---------------------------------------------------------------------------

SCHEMA_DDL = [
    """CREATE TABLE IF NOT EXISTS bsi_snapshot (
        ticker        TEXT PRIMARY KEY,
        ts            TEXT NOT NULL,
        z_score       REAL,
        z_trend       TEXT,
        archetype     TEXT,
        conviction    REAL,
        delta_z_5d    REAL
    )""",
    """CREATE TABLE IF NOT EXISTS risk_engine_log (
        ts            TEXT NOT NULL,
        username      TEXT NOT NULL,
        regime        TEXT,
        sigma_ratio   REAL,
        budget_usd    REAL,
        utilization   REAL,
        hhi           REAL,
        n_positions   INTEGER,
        payload       TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS position_sizing (
        trade_id      INTEGER NOT NULL,
        ts            TEXT NOT NULL,
        ticker        TEXT,
        garch_sigma   REAL,
        downside_beta REAL,
        cvar_budget   REAL,
        max_notional  REAL,
        initial_stop  REAL,
        trail_stop    REAL,
        mfe_pct       REAL,
        regime        TEXT,
        PRIMARY KEY (trade_id, ts)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_risk_log_ts ON risk_engine_log(ts)""",
    """CREATE INDEX IF NOT EXISTS idx_pos_sizing_ts ON position_sizing(ts)""",
]


def migrate(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    for stmt in SCHEMA_DDL:
        cur.execute(stmt)
    con.commit()


# ---------------------------------------------------------------------------
# Pure math layer
# ---------------------------------------------------------------------------

def cvar_multiplier(confidence: float) -> float:
    """Normal-CVaR multiplier: phi(z_alpha) / (1 - alpha).

    For 95%: ~2.063. For 99%: ~2.665.
    """
    from scipy.stats import norm
    z = norm.ppf(confidence)
    return float(norm.pdf(z) / (1.0 - confidence))


def estimate_sigma_garch(returns: pd.Series, cfg: RiskConfig) -> tuple[float, str]:
    """1-day-ahead conditional sigma. Returns (sigma, method)."""
    rets = returns.dropna().tail(cfg.garch_lookback_days)
    if len(rets) < cfg.min_history_days:
        return cfg.sector_fallback_sigma, "fallback_sector"

    # try GARCH(1,1)
    try:
        from arch import arch_model
        # arch expects returns in % units for numerical stability
        am = arch_model(rets * 100.0, vol="Garch", p=1, q=1, mean="Zero", rescale=False)
        res = am.fit(disp="off", show_warning=False)
        forecast = res.forecast(horizon=1, reindex=False)
        sigma_pct = float(np.sqrt(forecast.variance.values[-1, 0]))
        sigma = sigma_pct / 100.0
        if 0.001 < sigma < 0.30:   # sanity band
            return sigma, "garch"
    except Exception:
        pass

    # EWMA fallback (RiskMetrics)
    lam = cfg.ewma_lambda
    var = rets.var()
    for r in rets:
        var = lam * var + (1.0 - lam) * r * r
    sigma = float(math.sqrt(var))
    return sigma, "ewma"


def estimate_downside_beta(
    name_returns: pd.Series,
    portfolio_returns: pd.Series,
    cfg: RiskConfig,
) -> float:
    """β⁻ = cov(r_i, r_p | r_p < 0) / var(r_p | r_p < 0). Ang-Chen-Xing."""
    df = pd.concat([name_returns, portfolio_returns], axis=1, join="inner").dropna()
    if len(df) < cfg.min_history_days:
        return 1.0  # neutral default
    df = df.tail(cfg.downside_beta_window)
    df.columns = ["i", "p"]
    down = df[df["p"] < 0]
    if len(down) < 5 or down["p"].var() == 0:
        return 1.0
    beta = down["i"].cov(down["p"]) / down["p"].var()
    if not np.isfinite(beta):
        return 1.0
    return max(float(beta), cfg.downside_beta_floor)


def conviction_from_z(z: float, cfg: RiskConfig) -> float:
    """Map BSI z-score to [0,1] conviction."""
    if z is None or not np.isfinite(z):
        return cfg.conviction_default
    z = max(0.0, z)
    if z <= cfg.conviction_z_floor:
        return 0.0
    if z >= cfg.conviction_z_ceiling:
        return 1.0
    return (z - cfg.conviction_z_floor) / (cfg.conviction_z_ceiling - cfg.conviction_z_floor)


def bsi_trend(delta_z_5d: Optional[float], cfg: RiskConfig) -> str:
    if delta_z_5d is None or not np.isfinite(delta_z_5d):
        return "stable"
    if delta_z_5d > cfg.bsi_trend_threshold:
        return "rising"
    if delta_z_5d < -cfg.bsi_trend_threshold:
        return "falling"
    return "stable"


def allocate_cvar_budgets(
    convictions: dict[str, float],
    downside_betas: dict[str, float],
    portfolio_budget_usd: float,
    cfg: RiskConfig,
) -> dict[str, float]:
    """Maillard risk-parity, conviction-tilted, downside-beta-inverted."""
    raw = {}
    for tk, c in convictions.items():
        b = max(downside_betas.get(tk, 1.0), cfg.downside_beta_floor)
        raw[tk] = c / b
    total = sum(raw.values())
    if total <= 0:
        return {tk: 0.0 for tk in convictions}
    return {tk: portfolio_budget_usd * (raw[tk] / total) for tk in convictions}


def size_position(
    cvar_budget_name: float,
    sigma: float,
    price: float,
    cvar_mult: float,
) -> tuple[float, float, int]:
    """Returns (max_notional, stop_distance_pct, shares)."""
    if sigma <= 0 or price <= 0 or cvar_budget_name <= 0:
        return 0.0, 0.0, 0
    stop_dist = sigma * cvar_mult
    max_notional = cvar_budget_name / stop_dist
    shares = int(max_notional // price)
    return max_notional, stop_dist, shares


def initial_stop_price(entry: float, side: str, stop_dist: float) -> float:
    if side.upper() == "SHORT":
        return entry * (1.0 + stop_dist)
    return entry * (1.0 - stop_dist)


def chandelier_trail(
    entry: float,
    side: str,
    best_extreme: float,
    sigma: float,
    price: float,
    holding_days: int,
    k_eff: float,
    mfe_pct: float,
    cfg: RiskConfig,
) -> float:
    """Trail stop with MFE lock floor.

    For shorts:   trail = best_low + k * sigma * price * sqrt(h)
                  lock  = entry * (1 - mfe_lock_pct * mfe_pct)
                  active stop = min(trail, lock)   (tighter wins for shorts)

    For longs:    mirror.
    """
    h = max(1, holding_days)
    band = k_eff * sigma * price * math.sqrt(h)
    if side.upper() == "SHORT":
        cha = best_extreme + band
        lock = entry * (1.0 - cfg.mfe_lock_pct * mfe_pct)
        return min(cha, lock)
    else:
        cha = best_extreme - band
        lock = entry * (1.0 + cfg.mfe_lock_pct * mfe_pct)
        return max(cha, lock)


# ---------------------------------------------------------------------------
# I/O layer
# ---------------------------------------------------------------------------

def load_portfolio(con: sqlite3.Connection, username: str) -> dict:
    row = con.execute(
        "SELECT cash, starting_capital, hwm FROM portfolio WHERE username=?",
        (username,),
    ).fetchone()
    if not row:
        raise SystemExit(f"no portfolio row for username={username}")
    cash, sc, hwm = row
    return {"cash": cash, "starting_capital": sc, "hwm": hwm}


def load_open_positions(con: sqlite3.Connection, username: str) -> list[dict]:
    rows = con.execute(
        """SELECT id, ts, ticker, side, shares, entry_price, stop_price, target_price,
                  notional_usd, verdict, status
           FROM journal
           WHERE username=? AND status='OPEN'
           ORDER BY id""",
        (username,),
    ).fetchall()
    out = []
    for r in rows:
        verdict = r[9] or ""
        archetype = next(
            (a for a in ("BLITZ", "SCOUT", "GUARDIAN", "ROBO") if a in verdict.upper()),
            "SCOUT",
        )
        out.append({
            "id": r[0], "ts": r[1][:10], "ticker": r[2], "side": r[3],
            "shares": r[4], "entry": r[5], "stop": r[6], "target": r[7],
            "notional": r[8], "verdict": verdict, "archetype": archetype,
        })
    return out


def load_bsi_snapshot(con: sqlite3.Connection, ticker: str) -> Optional[dict]:
    row = con.execute(
        "SELECT z_score, z_trend, archetype, conviction, delta_z_5d FROM bsi_snapshot WHERE ticker=?",
        (ticker,),
    ).fetchone()
    if not row:
        return None
    return {
        "z_score": row[0], "z_trend": row[1], "archetype": row[2],
        "conviction": row[3], "delta_z_5d": row[4],
    }


_PX_CACHE: dict[str, pd.DataFrame] = {}


def get_history(ticker: str, days: int = 300) -> pd.DataFrame:
    if ticker in _PX_CACHE:
        return _PX_CACHE[ticker]
    try:
        df = yf.Ticker(ticker).history(period=f"{days}d", auto_adjust=True)
        if df.empty:
            df = pd.DataFrame(columns=["Close"])
    except Exception:
        df = pd.DataFrame(columns=["Close"])
    _PX_CACHE[ticker] = df
    return df


def daily_returns(ticker: str, days: int = 300) -> pd.Series:
    h = get_history(ticker, days)
    if h.empty or "Close" not in h:
        return pd.Series(dtype=float)
    return h["Close"].pct_change().dropna()


def last_price(ticker: str) -> Optional[float]:
    h = get_history(ticker)
    if h.empty:
        return None
    return float(h["Close"].iloc[-1])


def history_extreme(ticker: str, side: str, since: date) -> Optional[float]:
    """Best favorable price since entry (lowest low for short, highest high for long)."""
    h = get_history(ticker)
    if h.empty:
        return None
    h = h[h.index.date >= since]
    if h.empty:
        return None
    return float(h["Close"].min() if side.upper() == "SHORT" else h["Close"].max())


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class RiskEngine:
    def __init__(self, db_path: str, username: str, cfg: RiskConfig, dry_run: bool = False):
        self.db_path = db_path
        self.username = username
        self.cfg = cfg
        self.dry_run = dry_run
        self.cvar_mult = cvar_multiplier(cfg.cvar_confidence)
        self.con = sqlite3.connect(db_path)
        migrate(self.con)

    # -- portfolio-level diagnostics -------------------------------------

    def portfolio_returns(self, positions: list[dict], window: int) -> pd.Series:
        """Synthetic portfolio daily return = notional-weighted sum of name returns."""
        if not positions:
            return pd.Series(dtype=float)
        total = sum(p["notional"] for p in positions) or 1.0
        series = []
        for p in positions:
            w = p["notional"] / total
            r = daily_returns(p["ticker"]).tail(window) * (-w if p["side"] == "SHORT" else w)
            series.append(r)
        df = pd.concat(series, axis=1, join="outer").fillna(0.0)
        return df.sum(axis=1)

    def compute_regime(self, positions: list[dict]) -> dict:
        long_w = self.cfg.regime_long_window
        port_ret = self.portfolio_returns(positions, long_w)
        if len(port_ret) < self.cfg.regime_short_window:
            return {"regime": "NORMAL", "sigma_ratio": 1.0,
                    "sigma_20": None, "sigma_60": None,
                    "size_mult": 1.0, "stop_mult": 1.0}
        s_short = port_ret.tail(self.cfg.regime_short_window).std()
        s_long = port_ret.tail(self.cfg.regime_long_window).std()
        ratio = float(s_short / s_long) if s_long and s_long > 0 else 1.0
        panic = ratio > self.cfg.regime_panic_ratio
        return {
            "regime": "PANIC" if panic else "NORMAL",
            "sigma_ratio": ratio,
            "sigma_20": float(s_short),
            "sigma_60": float(s_long),
            "size_mult": self.cfg.regime_panic_size_multiplier if panic else 1.0,
            "stop_mult": self.cfg.regime_panic_stop_multiplier if panic else 1.0,
        }

    # -- per-name math ---------------------------------------------------

    def enrich_position(
        self,
        pos: dict,
        port_ret: pd.Series,
        regime: dict,
    ) -> dict:
        tk = pos["ticker"]
        side = pos["side"]
        entry_d = date.fromisoformat(pos["ts"])
        holding = max(1, (date.today() - entry_d).days)

        rets = daily_returns(tk)
        sigma, sigma_method = estimate_sigma_garch(rets, self.cfg)
        beta_minus = estimate_downside_beta(rets, port_ret, self.cfg)
        price = last_price(tk)

        bsi = load_bsi_snapshot(self.con, tk) or {}
        z = bsi.get("z_score")
        conviction = bsi.get("conviction") or conviction_from_z(z, self.cfg)
        trend = bsi.get("z_trend") or bsi_trend(bsi.get("delta_z_5d"), self.cfg)
        archetype = bsi.get("archetype") or pos["archetype"]

        # MFE
        best_ext = history_extreme(tk, side, entry_d)
        if best_ext is not None and pos["entry"]:
            if side == "SHORT":
                mfe_pct = max(0.0, (pos["entry"] - best_ext) / pos["entry"])
            else:
                mfe_pct = max(0.0, (best_ext - pos["entry"]) / pos["entry"])
        else:
            mfe_pct = 0.0

        # trailing stop
        trail_stop = None
        if mfe_pct >= self.cfg.trail_activation_mfe and price is not None and best_ext is not None:
            k_base = self.cfg.archetype_k_base.get(archetype, 2.0)
            k_eff = k_base * self.cfg.bsi_trend_multiplier.get(trend, 1.0) * regime["stop_mult"]
            trail_stop = chandelier_trail(
                entry=pos["entry"],
                side=side,
                best_extreme=best_ext,
                sigma=sigma,
                price=price,
                holding_days=holding,
                k_eff=k_eff,
                mfe_pct=mfe_pct,
                cfg=self.cfg,
            )
            # stop only ever tightens
            if pos["stop"] is not None:
                if side == "SHORT":
                    trail_stop = min(trail_stop, pos["stop"])
                else:
                    trail_stop = max(trail_stop, pos["stop"])

        # milestone partials
        milestones_due = []
        for level in self.cfg.milestone_levels_pct:
            tag = f"MILESTONE_{int(level*100)}"
            if mfe_pct >= level and tag not in pos["verdict"]:
                milestones_due.append(tag)

        return {
            **pos,
            "price": price, "sigma": sigma, "sigma_method": sigma_method,
            "beta_minus": beta_minus, "conviction": conviction,
            "z_score": z, "z_trend": trend, "archetype": archetype,
            "holding_days": holding, "best_extreme": best_ext,
            "mfe_pct": mfe_pct, "trail_stop": trail_stop,
            "milestones_due": milestones_due,
        }

    # -- main pass -------------------------------------------------------

    def morning_pass(self) -> dict:
        portfolio = load_portfolio(self.con, self.username)
        positions = load_open_positions(self.con, self.username)
        capital = (portfolio["cash"] or 0) + sum(p["notional"] for p in positions)
        budget_usd = capital * self.cfg.portfolio_cvar_budget_pct

        regime = self.compute_regime(positions)
        port_ret = self.portfolio_returns(positions, self.cfg.regime_long_window)

        enriched = [self.enrich_position(p, port_ret, regime) for p in positions]

        # CVaR budget allocation
        convictions = {p["ticker"]: p["conviction"] for p in enriched}
        betas = {p["ticker"]: p["beta_minus"] for p in enriched}
        budgets = allocate_cvar_budgets(convictions, betas, budget_usd, self.cfg)

        # CVaR check per name
        actions = []
        for p in enriched:
            tk = p["ticker"]
            b = budgets.get(tk, 0.0)
            sigma = p["sigma"]
            current_cvar_loss = p["notional"] * sigma * self.cvar_mult
            p["cvar_budget"] = b
            p["cvar_used"] = current_cvar_loss
            p["cvar_room"] = b - current_cvar_loss

            # action: trim if overbudget by >20%
            if b > 0 and current_cvar_loss > 1.2 * b:
                target_notional = b / (sigma * self.cvar_mult)
                actions.append({
                    "trade_id": p["id"], "ticker": tk, "type": "TRIM",
                    "reason": f"CVaR {current_cvar_loss:.0f} > 1.2*budget {b:.0f}",
                    "current_notional": p["notional"],
                    "target_notional": target_notional,
                })
            # action: milestone partials
            for ms in p["milestones_due"]:
                actions.append({
                    "trade_id": p["id"], "ticker": tk, "type": "PARTIAL_EXIT",
                    "reason": ms,
                    "fraction": self.cfg.milestone_partial_fraction,
                })
            # action: trail tightening
            if p["trail_stop"] is not None and p["stop"] is not None:
                tightened = (
                    (p["side"] == "SHORT" and p["trail_stop"] < p["stop"]) or
                    (p["side"] == "LONG"  and p["trail_stop"] > p["stop"])
                )
                if tightened:
                    actions.append({
                        "trade_id": p["id"], "ticker": tk, "type": "UPDATE_STOP",
                        "reason": f"trail (archetype={p['archetype']}, trend={p['z_trend']})",
                        "old_stop": p["stop"],
                        "new_stop": p["trail_stop"],
                    })

        # earnings shield (exit engine integration)
        if self.cfg.earnings_shield_enabled:
            try:
                from exit_engine import earnings_shield_actions
                actions.extend(earnings_shield_actions(enriched, self.cfg.earnings_shield_days))
            except ImportError:
                pass  # exit_engine optional

        # HHI on open notional
        total_notl = sum(p["notional"] for p in enriched) or 1.0
        hhi = sum((p["notional"] / total_notl) ** 2 for p in enriched)
        utilization = sum(p["cvar_used"] for p in enriched) / budget_usd if budget_usd else 0.0

        report = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "username": self.username,
            "capital": capital,
            "budget_usd": budget_usd,
            "utilization": utilization,
            "hhi": hhi,
            "regime": regime,
            "positions": enriched,
            "actions": actions,
            "config": asdict(self.cfg),
        }

        if not self.dry_run:
            self._journal(report)
            writebacks = self._apply_writeback(report)
            report["writebacks"] = writebacks
        else:
            report["writebacks"] = []
        return report

    # -- closed-loop writeback (Gap 1) ----------------------------------

    def _apply_writeback(self, report: dict) -> list[dict]:
        """Enforce engine actions against the live journal.

        UPDATE_STOP: rewrites journal.stop_price (trail enforcement).
        FLATTEN:     marks position CLOSED with current price as exit (earnings shield, BSI thesis-cool).
        TRIM:        advisory unless cfg.apply_trim is True (partial-close TBD).
        PARTIAL_EXIT: advisory unless cfg.apply_trim is True.

        Returns a list of {action, trade_id, ticker, before, after, status} entries
        that go into the next risk_engine_log payload via _append_writebacks_to_log.
        """
        cur = self.con.cursor()
        applied: list[dict] = []
        now = report["ts"]
        # build a price lookup from enriched positions
        px_by_tid = {p["id"]: p.get("price") for p in report["positions"]}
        for a in report["actions"]:
            tid = a.get("trade_id")
            if tid is None:
                continue
            if a["type"] == "UPDATE_STOP" and self.cfg.apply_writeback:
                old = a.get("old_stop")
                new = a["new_stop"]
                cur.execute(
                    "UPDATE journal SET stop_price=? WHERE id=? AND status='OPEN'",
                    (new, tid),
                )
                applied.append({
                    "action": "UPDATE_STOP", "trade_id": tid, "ticker": a["ticker"],
                    "before": old, "after": new,
                    "status": "applied" if cur.rowcount else "skipped_no_row",
                })
            elif a["type"] == "FLATTEN" and self.cfg.apply_flatten:
                px = px_by_tid.get(tid)
                if px is None:
                    applied.append({"action": "FLATTEN", "trade_id": tid,
                                    "ticker": a["ticker"], "status": "skipped_no_price"})
                    continue
                cur.execute(
                    "UPDATE journal SET status='CLOSED', exit_price=?, exit_ts=?, "
                    "verdict=verdict || ' [FLATTEN:' || ? || ']' "
                    "WHERE id=? AND status='OPEN'",
                    (px, now, a.get("reason", "flatten"), tid),
                )
                applied.append({
                    "action": "FLATTEN", "trade_id": tid, "ticker": a["ticker"],
                    "after": px, "reason": a.get("reason"),
                    "status": "applied" if cur.rowcount else "skipped_no_row",
                })
        self.con.commit()
        # backfill the log payload to include writebacks
        if applied:
            cur.execute(
                "UPDATE risk_engine_log SET payload=? WHERE ts=? AND username=?",
                (json.dumps(
                    {"actions": report["actions"], "writebacks": applied},
                    default=str,
                ), now, self.username),
            )
            self.con.commit()
        return applied

    # -- journaling ------------------------------------------------------

    def _journal(self, report: dict) -> None:
        cur = self.con.cursor()
        cur.execute(
            """INSERT INTO risk_engine_log
               (ts, username, regime, sigma_ratio, budget_usd, utilization, hhi, n_positions, payload)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                report["ts"], report["username"],
                report["regime"]["regime"], report["regime"]["sigma_ratio"],
                report["budget_usd"], report["utilization"], report["hhi"],
                len(report["positions"]),
                json.dumps(
                    {"actions": report["actions"]},
                    default=str,
                ),
            ),
        )
        for p in report["positions"]:
            cur.execute(
                """INSERT OR REPLACE INTO position_sizing
                   (trade_id, ts, ticker, garch_sigma, downside_beta, cvar_budget,
                    max_notional, initial_stop, trail_stop, mfe_pct, regime)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    p["id"], report["ts"], p["ticker"], p["sigma"], p["beta_minus"],
                    p["cvar_budget"],
                    p["cvar_budget"] / (p["sigma"] * self.cvar_mult) if p["sigma"] else 0,
                    p["stop"], p["trail_stop"], p["mfe_pct"],
                    report["regime"]["regime"],
                ),
            )
        self.con.commit()

    # -- presentation ----------------------------------------------------

    def print_report(self, report: dict, *, explain: Optional[str] = None) -> None:
        r = report
        print("=" * 78)
        print(f"  GRAY-SWAN RISK ENGINE  |  {r['ts']}  |  user={r['username']}")
        print("=" * 78)
        reg = r["regime"]
        s20 = f"{100*reg['sigma_20']:.2f}%" if reg["sigma_20"] is not None else "n/a"
        s60 = f"{100*reg['sigma_60']:.2f}%" if reg["sigma_60"] is not None else "n/a"
        print(f"  Regime          : {reg['regime']}   (sigma_20={s20}  sigma_60={s60}  "
              f"ratio={reg['sigma_ratio']:.2f})")
        print(f"  Capital         : ${r['capital']:,.0f}")
        print(f"  CVaR budget     : ${r['budget_usd']:,.0f}/day  "
              f"({100*self.cfg.portfolio_cvar_budget_pct:.1f}% @ "
              f"{int(100*self.cfg.cvar_confidence)}%)")
        print(f"  Utilization     : {100*r['utilization']:.0f}% of budget")
        print(f"  HHI             : {r['hhi']:.2f}   ({len(r['positions'])} open names)")
        print("-" * 78)
        hdr = f"  {'TICKER':<7} {'SIDE':<6} {'SHRS':>5} {'PX':>8} {'STOP':>8} " \
              f"{'TRAIL':>8} {'MFE':>6} {'σ':>6} {'β-':>5} {'CVaR$':>7} {'BDG$':>7} {'ARCH':<9}"
        print(hdr)
        for p in r["positions"]:
            trail = f"${p['trail_stop']:.2f}" if p["trail_stop"] else "—"
            stop  = f"${p['stop']:.2f}" if p["stop"] else "—"
            px    = f"${p['price']:.2f}" if p["price"] else "—"
            print(f"  {p['ticker']:<7} {p['side']:<6} {p['shares']:>5.0f} {px:>8} {stop:>8} "
                  f"{trail:>8} {100*p['mfe_pct']:>5.0f}% {100*p['sigma']:>5.1f}% "
                  f"{p['beta_minus']:>5.2f} {p['cvar_used']:>7.0f} {p['cvar_budget']:>7.0f} "
                  f"{p['archetype']:<9}")
        print("-" * 78)
        if r["actions"]:
            print(f"  ACTIONS ({len(r['actions'])}):")
            for a in r["actions"]:
                extras = " ".join(f"{k}={v}" for k, v in a.items()
                                  if k not in ("ticker", "type", "reason", "trade_id"))
                print(f"    [{a['type']}] {a['ticker']}: {a['reason']}  {extras}".rstrip())
        else:
            print("  ACTIONS: none -- all positions within budget.")
        wb = r.get("writebacks") or []
        if wb:
            print("-" * 78)
            print(f"  WRITEBACKS ({len(wb)}) -- live journal updates applied:")
            for w in wb:
                if w["action"] == "UPDATE_STOP":
                    print(f"    [{w['status']}] {w['ticker']} stop "
                          f"${w.get('before')} -> ${w['after']:.2f}  (trade #{w['trade_id']})")
                elif w["action"] == "FLATTEN":
                    print(f"    [{w['status']}] {w['ticker']} FLATTENED @ "
                          f"${w.get('after')}  -- {w.get('reason')}  (trade #{w['trade_id']})")
        elif not self.dry_run and r["actions"]:
            print(f"  WRITEBACKS: 0 (actions were advisory-only this pass)")
        print("=" * 78)

        if explain:
            self._print_explain(report, explain)

    def _print_explain(self, report: dict, ticker: str) -> None:
        p = next((x for x in report["positions"] if x["ticker"] == ticker.upper()), None)
        if not p:
            print(f"\n[explain] {ticker} not in open book.")
            return
        cm = self.cvar_mult
        print("")
        print(f"  EXPLAIN  {ticker.upper()}")
        print(f"  ─────────────────────────────────────────────────────────────────")
        print(f"  conviction c      = {p['conviction']:.3f}  (z={p['z_score']}, "
              f"trend={p['z_trend']})")
        print(f"  downside β        = {p['beta_minus']:.3f}")
        print(f"  raw weight        = c / max(β,floor) = {p['conviction']:.3f} / "
              f"{max(p['beta_minus'], self.cfg.downside_beta_floor):.3f}")
        print(f"  CVaR budget       = ${p['cvar_budget']:.0f}/day  "
              f"(share of ${report['budget_usd']:.0f})")
        print(f"  σ (GARCH/{p['sigma_method']})   = {100*p['sigma']:.2f}%/day")
        print(f"  CVaR multiplier   = φ(z_0.95)/0.05 = {cm:.3f}")
        print(f"  max notional      = budget / (σ × mult) = "
              f"${p['cvar_budget'] / (p['sigma']*cm):.0f}")
        print(f"  current notional  = ${p['notional']:.0f}  "
              f"({100*p['notional']/(p['cvar_budget']/(p['sigma']*cm) or 1):.0f}% of cap)")
        print(f"  CVaR loss now     = ${p['cvar_used']:.0f}  (vs budget ${p['cvar_budget']:.0f})")
        print(f"  initial stop dist = σ × mult = {100*p['sigma']*cm:.2f}%")
        print(f"  current stop      = ${p['stop']}  trail={p['trail_stop']}")
        print(f"  MFE since entry   = {100*p['mfe_pct']:.1f}%  "
              f"(trail activates at {100*self.cfg.trail_activation_mfe:.0f}%)")
        if p["mfe_pct"] >= self.cfg.trail_activation_mfe:
            k_base = self.cfg.archetype_k_base.get(p["archetype"], 2.0)
            mult = self.cfg.bsi_trend_multiplier.get(p["z_trend"], 1.0)
            k_eff = k_base * mult * report["regime"]["stop_mult"]
            print(f"  trail k_base      = {k_base}  ({p['archetype']})")
            print(f"  trail trend mult  = {mult}  ({p['z_trend']})")
            print(f"  trail regime mult = {report['regime']['stop_mult']}  ({report['regime']['regime']})")
            print(f"  trail k_eff       = {k_eff:.2f}")
            print(f"  mfe-lock floor    = entry × (1 - {self.cfg.mfe_lock_pct} × "
                  f"{p['mfe_pct']:.2f}) = "
                  f"${p['entry']*(1 - self.cfg.mfe_lock_pct*p['mfe_pct']):.2f}")
        print("")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(
        description="Gray-swan risk engine: CVaR-budgeted sizing, stops, regime overlay.",
    )
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--username", default="default",
                   help="account name in portfolio table (column: username)")
    p.add_argument("--config", default=None, help="path to RiskConfig JSON overrides")
    p.add_argument("--dry-run", action="store_true", help="compute & display, no DB writes")
    p.add_argument("--explain", metavar="TICKER", help="full math trace for one name")
    p.add_argument("--status", action="store_true", help="last persisted run, no recompute")
    p.add_argument("--dump-config", action="store_true",
                   help="write a template risk_engine_config.json next to this script")
    args = p.parse_args(argv)

    cfg = RiskConfig.load(Path(args.config) if args.config else None)

    if args.dump_config:
        path = Path(__file__).with_name("risk_engine_config.json")
        cfg.save_template(path)
        print(f"wrote config template: {path}")
        return 0

    eng = RiskEngine(args.db, args.username, cfg, dry_run=args.dry_run)

    if args.status:
        row = eng.con.execute(
            """SELECT ts, regime, sigma_ratio, budget_usd, utilization, hhi, n_positions, payload
               FROM risk_engine_log WHERE username=? ORDER BY ts DESC LIMIT 1""",
            (args.username,),
        ).fetchone()
        if not row:
            print("no prior runs.")
            return 0
        print(f"  last run         : {row[0]}")
        print(f"  regime           : {row[1]}   sigma_ratio={row[2]:.2f}")
        print(f"  budget / used    : ${row[3]:,.0f}  ({100*row[4]:.0f}% used)")
        print(f"  HHI              : {row[5]:.2f}   ({row[6]} positions)")
        print(f"  payload          : {row[7][:200]}{'...' if len(row[7]) > 200 else ''}")
        return 0

    report = eng.morning_pass()
    eng.print_report(report, explain=args.explain)
    return 0


if __name__ == "__main__":
    sys.exit(main())
