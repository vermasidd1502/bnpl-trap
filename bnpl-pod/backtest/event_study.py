"""
Event-study driver — Sprint G composition layer.

Historical catalyst windows
---------------------------
    1. KLARNA_DOWNROUND    — Jul 11 2022 (valuation cut $45.6B → $6.7B)
    2. AFFIRM_GUIDANCE_1   — Aug 26 2022 (FY23 revenue guide down)
    3. AFFIRM_GUIDANCE_2   — Feb 09 2023 (Q2'23 print)
    4. CFPB_INTERP_RULE    — May 22 2024 (Reg Z applicability announcement)
    5. REGZ_EFFECTIVE      — Jan 17 2025 (Reg Z compliance deadline; BNPL
                              complaint tsunami — peak of 12,838 BNPL
                              complaints in a single day vs <60/day baseline.
                              Empirical z_bsi spike reached +44, by far the
                              largest signal in the 2019-2026 window.)

For each window the driver does, day by day:

    (1) read the causal BSI z (post-Fix #3) for the as-of date
    (2) evaluate the 3-gate compliance predicate (post-Fix #2):
            gate_bsi  = (bsi_z >= z_threshold)
            gate_move = (move_ma30 >= move_threshold)
            gate_ccd2 = (days_to_deadline <= ccd_ii_max_days)
            approved  = gate_bsi AND gate_move AND gate_ccd2
    (3) if approved on a rebalance day, size the TRS short to
            target_trs_gross · capital   (signed negative)
        and the macro-hedge sleeve to
            0.60 · |target_trs|          (HYG short, signed negative)
    (4) feed into `backtest.pnl_sim.step_day` to apply MTM, Fix #1 B/A
        haircut, Fix #4 SOFR-on-cash + margin financing drag

A parallel shadow arm runs the naive AFRM equity short sized to the same
gross notional as the TRS book, with a 15 % annualized HTB fee (Sprint G
comparison-arm directive). Same MTM feed, different instrument.

Three-panel comparison
----------------------
The paper's §10 figure is three re-runs of the same window with different
config overrides:

    NAIVE          — Fix #1 OFF, Fix #4 OFF, HTB OFF  (inflated alpha)
    FIX3_ONLY      — Fix #1 OFF, Fix #4 OFF, HTB OFF,
                     but upstream BSI is causal (already baked in)
    INSTITUTIONAL  — Fix #1 ON,  Fix #4 ON,  HTB ON   (honest net)

NAIVE and FIX3_ONLY share the same `PnLMode` at the sim layer — they differ
only in which BSI series is fed into the gate predicate. `run_window`
accepts an optional `bsi_z_naive` series so a caller (or a test) can model
the look-ahead-biased gating pattern explicitly.

Determinism contract
--------------------
No randomness. No network. No warehouse writes. Given the same
`WindowFixture` and config, `run_window` is bit-identical across runs.
Persistence is the caller's choice: `dump_pnl_csv` and `summarize_window`
write into `backtest/outputs/` only when asked.
"""
from __future__ import annotations

import csv
import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional

import numpy as np

from backtest import pnl_sim
from data.regulatory_calendar import Catalyst, load_catalysts, nearest_material_catalyst

if TYPE_CHECKING:
    import duckdb as _duckdb_t  # noqa: F401

log = logging.getLogger(__name__)


# ============================================================================
# Event-window registry
# ============================================================================
@dataclass(frozen=True)
class EventWindow:
    name: str
    catalyst_date: date
    lookback_days: int = 20        # trading days before catalyst
    lookahead_days: int = 40       # trading days after catalyst
    rebalance_freq_days: int = 5   # weekly rebalance by default
    # Sprint H: `ccd_ii_deadline` removed. The window no longer carries a
    # hardcoded deadline — gate 3 is resolved per-day from
    # data.regulatory_calendar against the backtest `as_of`. Windows stay
    # pure metadata; the calendar is the source of truth.

    @property
    def window_length(self) -> int:
        return self.lookback_days + self.lookahead_days + 1


# Canonical event windows (paper_formal.tex §7 / §9, 2026-04-21 data-refresh).
# REGZ_EFFECTIVE was added after the April 2026 CFPB ingest revealed that
# the Regulation Z compliance deadline triggered a one-day z_bsi spike of
# +9.69 σ (paper v2.0.1 §7.2 headline under the canonical EWMA-σ scorer,
# driven by 12,838 BNPL complaints vs. a <60/day baseline). Earlier
# iterations of the scorer (180d rolling σ) quoted this pulse at ~+44 σ;
# the v2.0.1 Equation (1) EWMA σ tightens the estimate but the event is
# still by far the cleanest positive specimen we have — the four
# pre-existing 2022-2024 windows are too mild to trigger Gate 1.
WINDOWS: dict[str, EventWindow] = {
    "KLARNA_DOWNROUND":   EventWindow("KLARNA_DOWNROUND",  date(2022, 7, 11)),
    "AFFIRM_GUIDANCE_1":  EventWindow("AFFIRM_GUIDANCE_1", date(2022, 8, 26)),
    "AFFIRM_GUIDANCE_2":  EventWindow("AFFIRM_GUIDANCE_2", date(2023, 2, 9)),
    "CFPB_INTERP_RULE":   EventWindow("CFPB_INTERP_RULE",  date(2024, 5, 22)),
    "REGZ_EFFECTIVE":     EventWindow("REGZ_EFFECTIVE",    date(2025, 1, 17)),
}


# ============================================================================
# Fixture + result dataclasses
# ============================================================================
@dataclass
class WindowFixture:
    """Self-contained daily-frequency input bundle for one event window.

    Parallel arrays of length T. The driver does not inspect calendar gaps;
    the caller supplies business-day-aligned series.

    Two-series convention (post-Sprint H.c duration-risk update)
    ------------------------------------------------------------
    `tranche_book_returns` is the CANONICAL (duration-adjusted) series the
    INSTITUTIONAL panel consumes. `tranche_book_returns_naive`, when
    populated, is the SPREAD-ONLY companion the NAIVE panel consumes — it
    deliberately omits the WAL × ΔSOFR mark-to-market term so the P&L
    differential isolates "we pretended rates didn't exist" from "we had
    the wrong BSI." If `tranche_book_returns_naive` is None, both panels
    fall back to the canonical series (legacy behavior, preserved so
    existing synthetic tests don't have to change).
    """
    name: str
    dates: list[date]
    move_level: np.ndarray          # (T,) raw MOVE index; used for B/A + gate
    move_ma30: np.ndarray           # (T,) rolling 30d mean; used for gate_move
    sofr_annual: np.ndarray         # (T,) decimal (e.g. 0.053)
    bsi_z: np.ndarray               # (T,) causal 180d rolling z (post-Fix #3)
    tranche_book_returns: np.ndarray  # (T,) aggregate ABS junior return; short profits when <0
    hyg_returns: np.ndarray         # (T,) HYG total return
    afrm_returns: np.ndarray        # (T,) AFRM equity return (naive-arm driver)
    # Optional — modeling the Fix #3 gating delta explicitly
    bsi_z_naive: Optional[np.ndarray] = None   # (T,) full-sample z, for NAIVE panel
    # Optional — SPREAD-ONLY tranche returns (Sprint H.c). NAIVE panel uses
    # this when present; INSTITUTIONAL always uses the canonical (duration-
    # adjusted) `tranche_book_returns`.
    tranche_book_returns_naive: Optional[np.ndarray] = None
    # Optional — per-day dynamic MOVE threshold (Sprint P — post-compact).
    # When supplied, `evaluate_three_gates` uses `move_ma30_threshold_series[t]`
    # instead of the scalar `move_ma30_threshold`. Typically a rolling
    # percentile (default 85th pct of trailing 504 business days of MOVE MA30)
    # computed from the *full* warehouse MOVE history at fixture-load time,
    # so there is no look-ahead bias inside the event window itself.
    move_ma30_threshold_series: Optional[np.ndarray] = None
    # Sprint Q — alternative Gate-3 signal (2026-04-21, post-review).
    # When supplied, `run_window` uses `gate3_signal_override[t]` instead of
    # `move_ma30[t]` as the scalar signal compared to the Gate-3 threshold.
    # Use-case: `gate3_mode="credit"` populates this with a 180d causal
    # rolling z-score of HY OAS (BAMLH0A0HYM2) so Gate 3 measures
    # consumer-credit regime rather than Treasury-vol regime. The gate
    # predicate remains `signal >= threshold`; only the signal semantics change.
    gate3_signal_override: Optional[np.ndarray] = None
    # Human-readable label for the active Gate-3 regime ("move" or "credit").
    # Propagates into the per-day CSV dump and summary logs so downstream
    # analysis can filter / tag runs without re-deriving it.
    gate3_mode_name: str = "move"

    def __post_init__(self):
        T = len(self.dates)
        for name in ("move_level", "move_ma30", "sofr_annual", "bsi_z",
                     "tranche_book_returns", "hyg_returns", "afrm_returns"):
            arr = getattr(self, name)
            if len(arr) != T:
                raise ValueError(f"{self.name}: length mismatch on {name}: "
                                 f"{len(arr)} vs {T}")
        for opt in ("bsi_z_naive", "tranche_book_returns_naive",
                    "move_ma30_threshold_series", "gate3_signal_override"):
            arr = getattr(self, opt)
            if arr is not None and len(arr) != T:
                raise ValueError(f"{self.name}: length mismatch on {opt}: "
                                 f"{len(arr)} vs {T}")


class PnLMode(str, Enum):
    """Controls which Sprint G fixes are enabled in the sim layer."""
    NAIVE         = "naive"          # Fix #1/#4/HTB all OFF
    FIX3_ONLY     = "fix3_only"      # sim layer same as NAIVE (Fix #3 is upstream)
    INSTITUTIONAL = "institutional"  # all fixes ON


@dataclass
class PanelResult:
    """Output of one (window, PnLMode) run."""
    window_name: str
    mode: PnLMode
    # TRS arm
    trs_daily_pnl: np.ndarray
    trs_final_state: pnl_sim.PortfolioState
    trs_breakdowns: list[pnl_sim.DayBreakdown]
    # Naive AFRM short arm
    naive_daily_pnl: np.ndarray
    naive_final_state: pnl_sim.EquityShortState
    naive_breakdowns: list[pnl_sim.EquityDayBreakdown]
    # Gate diagnostics
    gate_approved: np.ndarray       # (T,) bool
    gate_bsi: np.ndarray
    gate_move: np.ndarray
    gate_ccd2: np.ndarray
    # Stats
    trs_stats: pnl_sim.SummaryStats
    naive_stats: pnl_sim.SummaryStats


# ============================================================================
# Compliance gate predicate — mirrors agents/compliance_engine.py
# ============================================================================
def evaluate_three_gates(
    *,
    bsi_z: float,
    move_ma30: float,
    as_of: date,
    nearest_catalyst_date: date | None,
    bsi_z_threshold: float = 1.5,
    move_ma30_threshold: float = 120.0,
    ccd_ii_max_days: int = 180,
    bsi_bypass_z_threshold: float = 10.0,
) -> tuple[bool, bool, bool, bool]:
    """Return (gate_bsi, gate_move, gate_ccd2, approved). Post-Fix #2: 3 gates.

    Mirrors the live `ComplianceEngine` predicate without importing it, so
    the event-study driver can run against BSI-history fixtures without
    paying the compliance-engine thresholds.yaml hash cost.

    Sprint H: `ccd_ii_deadline` parameter replaced with `nearest_catalyst_date`
    — the calendar-resolved nearest material catalyst date at `as_of`, or
    None if no such catalyst is in the record. `None` deterministically
    fails gate 3. Callers typically resolve this per-day by calling
    `data.regulatory_calendar.nearest_material_catalyst(as_of)`.

    Sprint Q: when ``|bsi_z| >= bsi_bypass_z_threshold`` (default 10σ), the
    trade approves on BSI alone — MOVE and catalyst gates are overridden.
    This mirrors ``ComplianceEngine`` and is the architectural response to
    the §8.5 finding that no public macro regime gauge corroborates
    BNPL-specific stress. The returned ``gate_move`` and ``gate_ccd2`` flags
    still reflect their underlying evaluation (so downstream CSV dumps
    retain the honest per-gate reading); only the final ``approved`` flag
    reflects the bypass. Set ``bsi_bypass_z_threshold=float('inf')`` to
    disable the bypass and recover the legacy strict-conjunction behavior.
    """
    gate_bsi = bool(bsi_z is not None
                    and math.isfinite(bsi_z)
                    and bsi_z >= bsi_z_threshold)
    gate_move = bool(move_ma30 is not None
                     and math.isfinite(move_ma30)
                     and move_ma30 >= move_ma30_threshold)
    if nearest_catalyst_date is None:
        gate_ccd2 = False
    else:
        days_to = (nearest_catalyst_date - as_of).days
        gate_ccd2 = bool(0 <= days_to <= ccd_ii_max_days)
    strict = gate_bsi and gate_move and gate_ccd2
    # Super-threshold bypass — BSI alone approves when z clears the bypass.
    bypass = bool(
        bsi_z is not None
        and math.isfinite(bsi_z)
        and abs(bsi_z) >= bsi_bypass_z_threshold
    )
    approved = strict or bypass
    return gate_bsi, gate_move, gate_ccd2, approved


# ============================================================================
# Mode-specific sim config
# ============================================================================
def _config_for_mode(mode: PnLMode, base: Optional[dict] = None) -> dict:
    """Toggle Fix #1 / Fix #4 / HTB depending on panel mode.

    NAIVE and FIX3_ONLY share the same numerical sim config — the two panels
    differ only in which BSI series feeds the gate predicate (caller's job).
    """
    cfg = dict(base or pnl_sim.DEFAULTS)
    if mode in (PnLMode.NAIVE, PnLMode.FIX3_ONLY):
        cfg = {
            **cfg,
            "trs_ba_base_bps":     0.0,
            "trs_ba_stress_bps":   0.0,
            "hedge_ba_bps":        0.0,
            "equity_ba_bps":       0.0,
            "use_sofr":            False,
            "financing_spread_bps": 0.0,
            "equity_htb_annual":   0.0,
        }
    # INSTITUTIONAL: keep base / loaded config intact.
    return cfg


# ============================================================================
# Core driver — one window × one mode
# ============================================================================
def run_window(
    fixture: WindowFixture,
    *,
    mode: PnLMode = PnLMode.INSTITUTIONAL,
    window: Optional[EventWindow] = None,
    catalysts: Optional[list[Catalyst]] = None,
    bsi_z_threshold: float = 1.5,
    move_ma30_threshold: float = 120.0,
    ccd_ii_max_days: int = 180,
    bsi_bypass_z_threshold: float = 10.0,
    target_trs_gross: float = 1.2,    # 1.2× capital deployed when approved
    hedge_beta: float = 0.60,
    starting_capital: float = 1.0,
    config: Optional[dict] = None,
) -> PanelResult:
    """Step the two arms through one event window.

    Rebalance schedule: every `window.rebalance_freq_days` (default 5).
    On non-rebalance days the target notionals equal the previous day's,
    so the sim step carries only MTM + SOFR + financing drag. Turnover
    costs are charged only on rebalance days, where `apply_transaction_cost`
    sees a non-zero delta.

    The NAIVE panel uses `fixture.bsi_z_naive` if supplied (to model the
    look-ahead-biased gate pattern); otherwise it falls back to `bsi_z`.
    """
    window = window or _lookup_window(fixture.name)
    base_cfg = _config_for_mode(mode, base=config)
    T = len(fixture.dates)

    # Choose BSI series for this panel (Fix #3 — causal vs full-sample z).
    if mode is PnLMode.NAIVE and fixture.bsi_z_naive is not None:
        bsi_series = fixture.bsi_z_naive
    else:
        bsi_series = fixture.bsi_z

    # Sprint H.c — choose tranche-return series for this panel.
    # INSTITUTIONAL consumes the canonical (duration-adjusted) series.
    # NAIVE and FIX3_ONLY consume the spread-only companion when present,
    # which isolates "we pretended rates didn't move" from the signal story.
    if mode in (PnLMode.NAIVE, PnLMode.FIX3_ONLY) and fixture.tranche_book_returns_naive is not None:
        tranche_series = fixture.tranche_book_returns_naive
    else:
        tranche_series = fixture.tranche_book_returns

    # Buffers
    gate_bsi = np.zeros(T, dtype=bool)
    gate_move = np.zeros(T, dtype=bool)
    gate_ccd2 = np.zeros(T, dtype=bool)
    approved = np.zeros(T, dtype=bool)
    target_trs = np.zeros(T, dtype=float)
    target_hedge = np.zeros(T, dtype=float)
    target_naive_short = np.zeros(T, dtype=float)

    # Build target-notional series, respecting rebalance frequency.
    last_trs = 0.0
    last_hedge = 0.0
    last_naive = 0.0
    # Pre-resolve the nearest material catalyst at each date. We do this once
    # per day rather than caching across days because the "nearest" answer
    # rolls forward as the calendar advances — sitting at 2023-01-02 vs.
    # 2024-01-02 can see different catalysts come into the 180d horizon.
    for t in range(T):
        as_of_t = fixture.dates[t]
        nearest_t = nearest_material_catalyst(as_of_t, catalysts)
        nearest_date = nearest_t.deadline_date if nearest_t is not None else None
        # Sprint P — if the fixture carries a per-day dynamic threshold series
        # (computed from the warehouse's full MOVE history at fixture-load time),
        # the gate-evaluator uses the day-t value instead of the scalar default.
        if fixture.move_ma30_threshold_series is not None:
            move_thr_t = float(fixture.move_ma30_threshold_series[t])
        else:
            move_thr_t = float(move_ma30_threshold)
        # Sprint Q — if the fixture carries an alternative Gate-3 signal
        # (e.g. HY OAS causal z-score for the "credit" regime), substitute it
        # for MOVE MA30 at day t. The gate predicate (signal >= threshold)
        # is unchanged; only the measurement regime changes.
        if fixture.gate3_signal_override is not None:
            gate3_signal_t = float(fixture.gate3_signal_override[t])
        else:
            gate3_signal_t = float(fixture.move_ma30[t])
        # Evaluate gates every day (cheap). The `move_ma30` kwarg carries
        # whichever Gate-3 signal is active for this run — we keep the name
        # for backward-compat of the public predicate API.
        gb, gm, gc, ok = evaluate_three_gates(
            bsi_z=float(bsi_series[t]),
            move_ma30=gate3_signal_t,
            as_of=as_of_t,
            nearest_catalyst_date=nearest_date,
            bsi_z_threshold=bsi_z_threshold,
            move_ma30_threshold=move_thr_t,
            ccd_ii_max_days=ccd_ii_max_days,
            bsi_bypass_z_threshold=bsi_bypass_z_threshold,
        )
        gate_bsi[t], gate_move[t], gate_ccd2[t], approved[t] = gb, gm, gc, ok

        # Rebalance only on pre-specified cadence.
        is_rebalance = (t % max(1, window.rebalance_freq_days) == 0)
        if is_rebalance:
            if ok:
                new_trs = -target_trs_gross * starting_capital
                new_hedge = -hedge_beta * abs(new_trs)
                new_naive = -target_trs_gross * starting_capital  # same gross
            else:
                new_trs = 0.0
                new_hedge = 0.0
                new_naive = 0.0
            last_trs, last_hedge, last_naive = new_trs, new_hedge, new_naive
        target_trs[t] = last_trs
        target_hedge[t] = last_hedge
        target_naive_short[t] = last_naive

    # TRS arm — uses the mode-selected tranche series (see above).
    trs_state, trs_bd = pnl_sim.run_trs_arm(
        move_series=fixture.move_level,
        sofr_series=fixture.sofr_annual,
        tranche_returns=tranche_series,
        hyg_returns=fixture.hyg_returns,
        target_trs_notionals=target_trs,
        target_hedge_notionals=target_hedge,
        starting_cash=starting_capital,
        config=base_cfg,
    )

    # Naive AFRM short arm (comparison)
    naive_state, naive_bd = pnl_sim.run_equity_short_arm(
        move_series=fixture.move_level,
        sofr_series=fixture.sofr_annual,
        equity_returns=fixture.afrm_returns,
        target_short_notionals=target_naive_short,
        starting_cash=starting_capital,
        config=base_cfg,
    )

    trs_daily = np.asarray([b.total for b in trs_bd], dtype=float)
    naive_daily = np.asarray([b.total for b in naive_bd], dtype=float)

    # Rolled-up stats
    sofr_avg = float(np.mean(fixture.sofr_annual))
    trs_stats = pnl_sim.summarize(
        trs_daily,
        starting_capital=starting_capital,
        transaction_costs_total=trs_state.transaction_costs_cum,
        cash_carry_total=trs_state.cash_carry_cum,
        sofr_annual=sofr_avg,
    )
    naive_stats = pnl_sim.summarize(
        naive_daily,
        starting_capital=starting_capital,
        transaction_costs_total=naive_state.transaction_costs_cum,
        cash_carry_total=naive_state.cash_carry_cum,
        sofr_annual=sofr_avg,
    )

    log.info(
        "event_study | window=%s mode=%s | TRS: ret=%+.4f sharpe=%+.2f dd=%+.4f | "
        "NAIVE: ret=%+.4f sharpe=%+.2f dd=%+.4f | approved=%d/%d days",
        fixture.name, mode.value,
        trs_stats.total_return, trs_stats.sharpe, trs_stats.max_drawdown,
        naive_stats.total_return, naive_stats.sharpe, naive_stats.max_drawdown,
        int(approved.sum()), T,
    )

    return PanelResult(
        window_name=fixture.name,
        mode=mode,
        trs_daily_pnl=trs_daily,
        trs_final_state=trs_state,
        trs_breakdowns=trs_bd,
        naive_daily_pnl=naive_daily,
        naive_final_state=naive_state,
        naive_breakdowns=naive_bd,
        gate_approved=approved,
        gate_bsi=gate_bsi,
        gate_move=gate_move,
        gate_ccd2=gate_ccd2,
        trs_stats=trs_stats,
        naive_stats=naive_stats,
    )


def _lookup_window(name: str) -> EventWindow:
    if name in WINDOWS:
        return WINDOWS[name]
    # Permissive fallback for ad-hoc fixture names in tests.
    return EventWindow(name, catalyst_date=date(2020, 1, 1))


# ============================================================================
# Three-panel comparison
# ============================================================================
@dataclass
class ThreePanelComparison:
    window_name: str
    panels: dict[PnLMode, PanelResult] = field(default_factory=dict)

    def summary_rows(self) -> list[dict]:
        """One row per panel — shape suitable for CSV dump or DataFrame."""
        rows = []
        for mode, panel in self.panels.items():
            rows.append({
                "window": self.window_name,
                "panel": mode.value,
                "trs_total_return":   panel.trs_stats.total_return,
                "trs_ann_return":     panel.trs_stats.ann_return,
                "trs_ann_vol":        panel.trs_stats.ann_vol,
                "trs_sharpe":         panel.trs_stats.sharpe,
                "trs_max_drawdown":   panel.trs_stats.max_drawdown,
                "trs_hit_rate":       panel.trs_stats.hit_rate,
                "trs_tx_cost_pct":    panel.trs_stats.transaction_costs_pct,
                "trs_cash_carry_pct": panel.trs_stats.cash_carry_pct,
                "naive_total_return": panel.naive_stats.total_return,
                "naive_ann_return":   panel.naive_stats.ann_return,
                "naive_sharpe":       panel.naive_stats.sharpe,
                "naive_max_drawdown": panel.naive_stats.max_drawdown,
                "naive_hit_rate":     panel.naive_stats.hit_rate,
                "approved_days":      int(panel.gate_approved.sum()),
                "n_days":             int(panel.gate_approved.size),
            })
        return rows


def run_three_panel_comparison(
    fixture: WindowFixture,
    **kwargs,
) -> ThreePanelComparison:
    """Produce all three panels (naive / fix3 / institutional) for one window."""
    cmp = ThreePanelComparison(window_name=fixture.name)
    for mode in (PnLMode.NAIVE, PnLMode.FIX3_ONLY, PnLMode.INSTITUTIONAL):
        cmp.panels[mode] = run_window(fixture, mode=mode, **kwargs)
    return cmp


# ============================================================================
# Persistence — pnl.csv + summary.csv
# ============================================================================
OUTPUT_DIR_DEFAULT = Path(__file__).resolve().parent / "outputs"


def dump_pnl_csv(
    panel: PanelResult,
    fixture: WindowFixture,
    out_dir: Path | str = OUTPUT_DIR_DEFAULT,
) -> Path:
    """Dump per-day P&L breakdown to `<out_dir>/pnl_<window>_<mode>.csv`.

    One row per day with the full Fix #1 / Fix #4 accounting decomposition,
    plus gate diagnostics. Suitable as the input to the paper §10 figure.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"pnl_{panel.window_name}_{panel.mode.value}.csv"

    header = [
        "date", "day_idx",
        "bsi_z", "move_ma30", "sofr", "tranche_return", "hyg_return", "afrm_return",
        "gate_bsi", "gate_move", "gate_ccd2", "approved",
        # TRS arm fields
        "trs_cash_carry", "trs_financing_drag", "trs_mtm_trs", "trs_mtm_hedge",
        "trs_transaction_costs", "trs_daily_pnl",
        # Naive arm fields
        "naive_cash_carry", "naive_mtm", "naive_tx_costs", "naive_htb_fee",
        "naive_daily_pnl",
    ]
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for t, d in enumerate(fixture.dates):
            tb = panel.trs_breakdowns[t]
            nb = panel.naive_breakdowns[t]
            w.writerow([
                d.isoformat(), t,
                f"{fixture.bsi_z[t]:.6f}",
                f"{fixture.move_ma30[t]:.4f}",
                f"{fixture.sofr_annual[t]:.6f}",
                f"{fixture.tranche_book_returns[t]:.6f}",
                f"{fixture.hyg_returns[t]:.6f}",
                f"{fixture.afrm_returns[t]:.6f}",
                int(panel.gate_bsi[t]),
                int(panel.gate_move[t]),
                int(panel.gate_ccd2[t]),
                int(panel.gate_approved[t]),
                f"{tb.cash_carry:.8f}",
                f"{tb.financing_drag:.8f}",
                f"{tb.mtm_trs:.8f}",
                f"{tb.mtm_hedge:.8f}",
                f"{tb.transaction_costs:.8f}",
                f"{tb.total:.8f}",
                f"{nb.cash_carry:.8f}",
                f"{nb.mtm:.8f}",
                f"{nb.transaction_costs:.8f}",
                f"{nb.htb_fee:.8f}",
                f"{nb.total:.8f}",
            ])
    log.info("event_study | wrote %s (%d rows)", path, len(fixture.dates))
    return path


def dump_summary_csv(
    comparisons: Iterable[ThreePanelComparison],
    out_dir: Path | str = OUTPUT_DIR_DEFAULT,
    filename: str = "summary.csv",
) -> Path:
    """Rolled-up stats across all (window × panel) cells in one CSV."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    rows: list[dict] = []
    for cmp in comparisons:
        rows.extend(cmp.summary_rows())
    if not rows:
        log.warning("event_study | no rows to dump (empty comparisons)")
        return path
    header = list(rows[0].keys())
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow([r[k] for k in header])
    log.info("event_study | wrote summary to %s (%d rows)", path, len(rows))
    return path


# ============================================================================
# Warehouse → WindowFixture bridge (Sprint H.b)
# ============================================================================
# Until Sprint H.b, `run_all_windows` could only be driven by synthetic
# fixtures manually constructed in tests/notebooks. That made every backtest
# number in the paper a statement about the fixture, not about the world.
#
# `load_window_from_warehouse` is the data bridge: it reconstructs a
# `WindowFixture` for one of the four canonical event windows by querying
# DuckDB. The data contract is:
#
#     MOVE level, MA30       ← fred_series (via data.ingest.yahoo_macro)
#     SOFR annual decimal    ← fred_series (via data.ingest.fred, percent→decimal)
#     BSI causal z           ← bsi_daily (via signals.bsi)
#     HYG daily returns      ← fred_series (yahoo_macro: HYG close, pct_change)
#     AFRM daily returns     ← fred_series (yahoo_macro: AFRM close, pct_change)
#     Tranche book returns   ← β_HYG · HYG_return + excess_spread carry
#                              (Phase 1 proxy; Phase 2 adds duration penalty)
#
# All series are re-indexed onto a business-day calendar spanning the window
# (catalyst ± lookback/lookahead), left-padded by ~45 calendar days so the
# MA30 has a warm start. Forward-fill handles holiday gaps (MOVE isn't
# published on closed-market days); any un-fillable gap in the *window*
# proper raises — we refuse to backtest against a series full of zeros
# pretending to be data.
# ----------------------------------------------------------------------------
def _pct_to_decimal(x: float | None) -> float | None:
    """FRED reports SOFR / DGS10 as percent; pnl_sim consumes decimal."""
    return None if x is None else float(x) / 100.0


def load_window_from_warehouse(
    window_key: str,
    con: Optional["_duckdb_t.DuckDBPyConnection"] = None,
    *,
    move_ma_window: int = 30,
    tranche_hyg_beta: float = 1.0,
    include_excess_spread_carry: bool = True,
    # Sprint H.c / Phase 2 — duration adjustment for rate moves.
    # Default flipped to True on 2026-04-19: 2022-2023 tranche returns without
    # duration were structurally overstated. The spread-only companion is
    # still returned (as `tranche_book_returns_naive`) so the NAIVE panel can
    # quantify how much of the "alpha" was actually just unhedged duration.
    apply_duration_adjustment: bool = True,
    tranche_wal_years: float = 3.0,
    duration_sofr_series: str = "SOFR",
    # Sprint P / Q — Gate-3 regime selection.
    #   "absolute" : keep the legacy scalar threshold (`move_ma30_threshold`)
    #                as set by the caller of run_window; fixture emits None.
    #   "dynamic"  : precompute a per-day threshold = rolling-percentile of
    #                MOVE MA30 over the trailing `gate3_lookback_days` of
    #                the full warehouse history (causal, no look-ahead).
    #   "credit"   : replace the MOVE signal entirely with the Chicago
    #                Fed's National Financial Conditions Index (FRED
    #                ``NFCI``), forward-filled from weekly to business-day
    #                cadence. NFCI is normalized so that 0 = long-run
    #                neutral; positive = tighter than average. Gate fires
    #                when NFCI $\geq$ ``gate3_credit_nfci_threshold``
    #                (default 0.0 — any above-neutral tightness qualifies
    #                the macro regime as corroborating a BNPL stress
    #                signal). We use NFCI rather than BAMLH0A0HYM2 (HY
    #                OAS) because FRED's live feed for ICE BofA spread
    #                indices only returns the last ~3 years (data before
    #                2023-04 is unavailable from the public API), while
    #                our oldest event window is 2022-06. NFCI is an
    #                18-input financial-conditions composite that captures
    #                the same latent regime factor and extends back to
    #                1971 with continuous weekly publication.
    gate3_mode: str = "absolute",
    gate3_percentile: float = 0.85,
    gate3_lookback_days: int = 504,   # ~2 calendar years of business days
    gate3_credit_nfci_threshold: float = 0.0,
) -> WindowFixture:
    """Reconstruct a `WindowFixture` for `window_key` from the DuckDB warehouse.

    Parameters
    ----------
    window_key : one of `WINDOWS.keys()` — 'KLARNA_DOWNROUND', etc.
    con : optional live DuckDB connection. If None, opens a read-only
        handle on `settings.duckdb_path` and closes it before returning.
    move_ma_window : rolling window for MOVE MA30 (default 30 business days).
    tranche_hyg_beta : scalar relating HYG daily return to junior-tranche
        MTM. β=1.0 is the naive proxy; a calibration step (Sprint H.c)
        refines this with regression of HYG vs. AFRMT secondary prints.
    include_excess_spread_carry : if True and abs_tranche_metrics has any
        row at or before the window, add (excess_spread% / 252) as a daily
        positive carry to the tranche return (the TRS long pays this to
        the seller; we hold short, so the sign flips downstream).
    apply_duration_adjustment : Phase 2 flag. When True, subtract a
        mark-to-market duration loss = -WAL_years × ΔSOFR from the daily
        tranche return; this captures the 2022-2023 Fed hiking-cycle
        duration drag that a pure spread model misses.
    tranche_wal_years : baseline weighted-average-life assumption for
        junior BNPL ABS tranches (default 3.0y).

    Returns
    -------
    WindowFixture with length T = lookback_days + lookahead_days + 1
    (measured in business days, aligned to the catalyst date).

    Raises
    ------
    KeyError  : unknown window_key.
    ValueError: any required series has no rows in the span, or any
                un-fillable NaN remains inside the window after ffill.
    """
    import duckdb   # local import — keep module import light for tests
    import pandas as pd

    if window_key not in WINDOWS:
        raise KeyError(f"{window_key!r} not in WINDOWS registry "
                       f"(valid: {list(WINDOWS)})")
    w = WINDOWS[window_key]

    own_con = False
    if con is None:
        from data.settings import settings
        con = duckdb.connect(str(settings.duckdb_path), read_only=True)
        own_con = True

    try:
        # --- Calendar span -------------------------------------------------
        # lookback/lookahead are in *business days*; pad the query span
        # generously so reindex+ffill handles holidays and the MA30 warmup.
        pad_left  = int(move_ma_window * 1.7) + 15   # ~65 calendar days
        pad_right = 15
        q_start = w.catalyst_date - timedelta(days=int(w.lookback_days * 1.6) + pad_left)
        q_end   = w.catalyst_date + timedelta(days=int(w.lookahead_days * 1.6) + pad_right)

        # --- Generic fred_series puller -----------------------------------
        def _pull_series(sid: str, *, required: bool = True) -> pd.Series:
            rows = con.execute(
                "SELECT observed_at, value FROM fred_series "
                "WHERE series_id = ? AND observed_at BETWEEN ? AND ? "
                "ORDER BY observed_at",
                [sid, q_start, q_end],
            ).fetchall()
            if not rows:
                if required:
                    raise ValueError(
                        f"{window_key}: no {sid!r} rows in fred_series within "
                        f"[{q_start}, {q_end}]. Run "
                        f"`python -m data.ingest.yahoo_macro` (for MOVE/HYG/AFRM) "
                        f"or `python -m data.ingest.fred` (for SOFR/DGS10)."
                    )
                return pd.Series(dtype=float, name=sid)
            idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in rows])
            vals = [float(v) if v is not None else np.nan for _, v in rows]
            s = pd.Series(vals, index=idx, name=sid)
            # Defensive dedup: the warehouse has no UNIQUE(series_id, observed_at)
            # constraint, so a re-ingest can seed two rows for the same date.
            # `reindex(cal)` downstream raises on duplicate labels — collapse
            # to the most-recently-inserted row (rows are in insertion order
            # within the same `observed_at`).
            if not s.index.is_unique:
                s = s[~s.index.duplicated(keep="last")]
            return s.sort_index()

        move = _pull_series("MOVE")
        sofr = _pull_series(duration_sofr_series)
        hyg  = _pull_series("HYG")
        afrm = _pull_series("AFRM")

        # --- BSI causal z -------------------------------------------------
        bsi_rows = con.execute(
            "SELECT observed_at, z_bsi FROM bsi_daily "
            "WHERE observed_at BETWEEN ? AND ? ORDER BY observed_at",
            [q_start, q_end],
        ).fetchall()
        if not bsi_rows:
            raise ValueError(
                f"{window_key}: no bsi_daily rows in [{q_start}, {q_end}]. "
                f"Run `python -m signals.bsi` to build the causal BSI series."
            )
        bsi_idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in bsi_rows])
        bsi_vals = [float(v) if v is not None else np.nan for _, v in bsi_rows]
        bsi_z = pd.Series(bsi_vals, index=bsi_idx, name="bsi_z")
        if not bsi_z.index.is_unique:
            bsi_z = bsi_z[~bsi_z.index.duplicated(keep="last")]
        bsi_z = bsi_z.sort_index()

        # --- Excess-spread carry (optional, quarterly) --------------------
        es_series: pd.Series | None = None
        if include_excess_spread_carry:
            es_rows = con.execute(
                "SELECT period_end, excess_spread FROM abs_tranche_metrics "
                "WHERE period_end <= ? "
                "ORDER BY period_end DESC LIMIT 12",
                [q_end],
            ).fetchall()
            if es_rows:
                es_idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in es_rows])
                es_vals = [float(v) if v is not None else np.nan for _, v in es_rows]
                es_series = pd.Series(es_vals, index=es_idx,
                                       name="excess_spread").sort_index()
                if not es_series.index.is_unique:
                    es_series = es_series[~es_series.index.duplicated(keep="last")]

        # --- Align on business-day calendar -------------------------------
        cal = pd.bdate_range(start=q_start, end=q_end)
        frame = pd.DataFrame(index=cal)
        frame["move"]        = move.reindex(cal).ffill()
        frame["sofr_pct"]    = sofr.reindex(cal).ffill()
        frame["hyg_close"]   = hyg.reindex(cal).ffill()
        frame["afrm_close"]  = afrm.reindex(cal).ffill()
        frame["bsi_z"]       = bsi_z.reindex(cal).ffill()
        frame["move_ma30"]   = frame["move"].rolling(
            move_ma_window, min_periods=max(5, move_ma_window // 4),
        ).mean()
        frame["hyg_return"]  = frame["hyg_close"].pct_change().fillna(0.0)
        frame["afrm_return"] = frame["afrm_close"].pct_change().fillna(0.0)
        frame["sofr_dec"]    = frame["sofr_pct"] / 100.0

        # Spread-only tranche return = β · HYG + daily spread carry.
        # This is the NAIVE companion — what a back-tester who ignored the
        # 2022-2023 hiking cycle would have computed.
        frame["tranche_return_spread_only"] = tranche_hyg_beta * frame["hyg_return"]
        if es_series is not None:
            # Quarterly %  →  daily decimal accrual. Forward-fill so every
            # day in the window sees the most-recent trustee print.
            daily_carry = (es_series.reindex(cal, method="ffill") / 100.0) / 252.0
            frame["tranche_return_spread_only"] = (
                frame["tranche_return_spread_only"] + daily_carry.fillna(0.0)
            )

        # Phase 2 duration penalty — subtract WAL · ΔSOFR_decimal.
        # SOFR is reported in percent on FRED; the decimal diff is
        # ΔSOFR_pct / 100. A +25 bp hike on a 3y-WAL tranche is a -0.75 %
        # MTM hit — exactly the term the 2022-2023 spread-only fixtures
        # were hiding. The canonical `tranche_return` is spread + duration;
        # the `tranche_return_spread_only` column is preserved so the
        # fixture carries both arms into the three-panel comparison.
        d_sofr = frame["sofr_pct"].diff().fillna(0.0) / 100.0
        frame["duration_hit"] = -float(tranche_wal_years) * d_sofr
        if apply_duration_adjustment:
            frame["tranche_return"] = (
                frame["tranche_return_spread_only"] + frame["duration_hit"]
            )
        else:
            # Legacy / test path — canonical series stays spread-only.
            frame["tranche_return"] = frame["tranche_return_spread_only"].copy()

        # --- Trim to exact window ----------------------------------------
        cat_ts = pd.Timestamp(w.catalyst_date)
        if cat_ts in cal:
            pos = cal.get_loc(cat_ts)
        else:
            # Catalyst fell on a holiday — use the NEXT business day.
            pos = int(cal.searchsorted(cat_ts, side="left"))
            pos = min(pos, len(cal) - 1)
        i_lo = max(0, pos - w.lookback_days)
        i_hi = min(len(cal) - 1, pos + w.lookahead_days)
        sub = frame.iloc[i_lo : i_hi + 1]

        required_cols = ["move", "move_ma30", "sofr_dec", "bsi_z",
                         "tranche_return", "hyg_return", "afrm_return"]
        nan_cols = [c for c in required_cols if sub[c].isna().any()]
        if nan_cols:
            first_bad = {c: sub[sub[c].isna()].index[0].date() for c in nan_cols}
            raise ValueError(
                f"{window_key}: NaN in warehouse-reconstructed window on columns "
                f"{nan_cols} (first: {first_bad}). The warehouse is missing data "
                f"for this span — re-run ingest or extend backfill. "
                f"Window span was [{sub.index[0].date()}, {sub.index[-1].date()}]."
            )

        # --- Dynamic Gate-3 threshold (Sprint P) --------------------------
        # Pull the FULL warehouse MOVE history once, compute the 30d MA,
        # then at each date t in the event window take the
        # `gate3_percentile` of the trailing `gate3_lookback_days` MA30
        # values (causal — strictly t-k ... t-1). This series is attached
        # to the fixture so `run_window` can prefer it over the scalar
        # threshold on a per-day basis.
        move_thr_series: Optional[np.ndarray] = None
        gate3_signal_override: Optional[np.ndarray] = None
        gate3_mode_name = "move"
        if gate3_mode == "dynamic":
            full_move = con.execute(
                "SELECT observed_at, value FROM fred_series "
                "WHERE series_id = 'MOVE' ORDER BY observed_at"
            ).fetchall()
            if full_move:
                full_idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in full_move])
                full_vals = np.asarray([float(v) for _, v in full_move])
                full_s = pd.Series(full_vals, index=full_idx).asfreq("B").ffill()
                full_ma = full_s.rolling(move_ma_window,
                                         min_periods=max(5, move_ma_window // 4)).mean()
                thr = full_ma.rolling(gate3_lookback_days,
                                      min_periods=max(30, gate3_lookback_days // 8)
                                      ).quantile(gate3_percentile)
                # For each date in the event window, take the threshold that
                # would have been visible on the previous business day (shift
                # by one to enforce strict causality in the rolling stats).
                thr_shifted = thr.shift(1)
                thr_reidx = thr_shifted.reindex(sub.index, method="ffill")
                if thr_reidx.isna().any():
                    raise ValueError(
                        f"{window_key}: dynamic Gate-3 threshold has NaN at "
                        f"{thr_reidx[thr_reidx.isna()].index[0].date()}; "
                        f"extend the warehouse MOVE history or lower "
                        f"gate3_lookback_days (currently {gate3_lookback_days})."
                    )
                move_thr_series = thr_reidx.to_numpy(dtype=float)
        elif gate3_mode == "credit":
            # Replace the MOVE signal entirely with NFCI (FRED's Chicago
            # Fed National Financial Conditions Index). NFCI is published
            # weekly, already mean-centered (0 = long-run-neutral, higher
            # = tighter conditions), with continuous history back to 1971
            # — dramatically longer than the public FRED feed for ICE
            # BofA HY OAS (~3 years). Weekly values are forward-filled to
            # business-day cadence; the gate predicate becomes
            #   gate_credit = NFCI_t >= gate3_credit_nfci_threshold
            # with default threshold = 0.0 (any above-neutral tightness).
            full_nfci = con.execute(
                "SELECT observed_at, value FROM fred_series "
                "WHERE series_id = 'NFCI' ORDER BY observed_at"
            ).fetchall()
            if not full_nfci:
                raise ValueError(
                    f"{window_key}: gate3_mode='credit' but no NFCI "
                    f"rows in fred_series. Run "
                    f"`python -m data.ingest.fred` to backfill NFCI."
                )
            full_idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in full_nfci])
            full_vals = np.asarray(
                [float(v) if v is not None else np.nan for _, v in full_nfci]
            )
            full_s = pd.Series(full_vals, index=full_idx).asfreq("B").ffill()
            nfci_reidx = full_s.reindex(sub.index, method="ffill")
            if nfci_reidx.isna().any():
                first_bad = nfci_reidx[nfci_reidx.isna()].index[0].date()
                raise ValueError(
                    f"{window_key}: gate3_mode='credit' produced NaN NFCI "
                    f"at {first_bad}; extend the warehouse NFCI history "
                    f"(run `python -m data.ingest.fred`)."
                )
            gate3_signal_override = nfci_reidx.to_numpy(dtype=float)
            move_thr_series = np.full(
                len(sub), float(gate3_credit_nfci_threshold), dtype=float,
            )
            gate3_mode_name = "credit"

        fx = WindowFixture(
            name=window_key,
            dates=[ts.date() for ts in sub.index],
            move_level=sub["move"].to_numpy(dtype=float),
            move_ma30=sub["move_ma30"].to_numpy(dtype=float),
            sofr_annual=sub["sofr_dec"].to_numpy(dtype=float),
            bsi_z=sub["bsi_z"].to_numpy(dtype=float),
            tranche_book_returns=sub["tranche_return"].to_numpy(dtype=float),
            hyg_returns=sub["hyg_return"].to_numpy(dtype=float),
            afrm_returns=sub["afrm_return"].to_numpy(dtype=float),
            tranche_book_returns_naive=sub["tranche_return_spread_only"].to_numpy(dtype=float),
            move_ma30_threshold_series=move_thr_series,
            gate3_signal_override=gate3_signal_override,
            gate3_mode_name=gate3_mode_name,
        )
        gate3_diag = ""
        if fx.gate3_signal_override is not None:
            gate3_diag = (
                f" gate3={fx.gate3_mode_name} "
                f"z∈[{fx.gate3_signal_override.min():+.2f},"
                f"{fx.gate3_signal_override.max():+.2f}]"
            )
        log.info(
            "event_study | loaded window=%s dates=[%s..%s] T=%d "
            "| MOVE∈[%.1f,%.1f] SOFR∈[%.3f,%.3f] BSI_z∈[%.2f,%.2f] "
            "tranche_ret∈[%+.4f,%+.4f]%s%s",
            window_key, fx.dates[0].isoformat(), fx.dates[-1].isoformat(), len(fx.dates),
            float(fx.move_level.min()),   float(fx.move_level.max()),
            float(fx.sofr_annual.min()),  float(fx.sofr_annual.max()),
            float(fx.bsi_z.min()),        float(fx.bsi_z.max()),
            float(fx.tranche_book_returns.min()),
            float(fx.tranche_book_returns.max()),
            " (duration-adjusted)" if apply_duration_adjustment else "",
            gate3_diag,
        )
        return fx
    finally:
        if own_con:
            con.close()


def load_all_windows_from_warehouse(
    con: Optional["_duckdb_t.DuckDBPyConnection"] = None,
    **kwargs,
) -> dict[str, WindowFixture]:
    """Convenience: build every fixture in `WINDOWS` via the warehouse.

    Shares one DuckDB connection across the four queries when `con` is None.
    Any window that fails to load raises — we do not silently skip, because
    a partial run produces a misleading summary CSV.
    """
    import duckdb
    own_con = False
    if con is None:
        from data.settings import settings
        con = duckdb.connect(str(settings.duckdb_path), read_only=True)
        own_con = True
    try:
        return {k: load_window_from_warehouse(k, con=con, **kwargs)
                for k in WINDOWS}
    finally:
        if own_con:
            con.close()


# ============================================================================
# Programmatic entry — `python -m backtest.event_study`
# ============================================================================
def run_all_windows(
    fixtures: dict[str, WindowFixture],
    *,
    out_dir: Path | str = OUTPUT_DIR_DEFAULT,
    **kwargs,
) -> dict[str, ThreePanelComparison]:
    """Run 3-panel comparison on every supplied fixture, dump CSVs, return map.

    `fixtures` is typically built upstream from the warehouse — see the
    notebooks for an example. Tests inject synthetic fixtures directly.
    """
    results: dict[str, ThreePanelComparison] = {}
    for name, fx in fixtures.items():
        cmp = run_three_panel_comparison(fx, **kwargs)
        results[name] = cmp
        for panel in cmp.panels.values():
            dump_pnl_csv(panel, fx, out_dir=out_dir)
    dump_summary_csv(results.values(), out_dir=out_dir)
    return results


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        prog="python -m backtest.event_study",
        description=(
            "BNPL event-study driver. `--source=registry` (default) prints "
            "the window registry and exits. `--source=warehouse` pulls real "
            "series from DuckDB, runs the 3-panel backtest on every window, "
            "and writes pnl_*.csv / summary.csv into backtest/outputs/."
        ),
    )
    parser.add_argument(
        "--source", choices=("registry", "warehouse"), default="registry",
        help="registry = print-and-exit; warehouse = live DuckDB-backed run",
    )
    parser.add_argument(
        "--windows", nargs="+", default=None,
        help="Subset of window keys to run (default: all 4).",
    )
    parser.add_argument(
        "--apply-duration", action="store_true",
        help="Phase 2: apply WAL × ΔSOFR duration penalty to tranche returns.",
    )
    parser.add_argument(
        "--tranche-wal", type=float, default=3.0,
        help="Weighted-average-life assumption for junior ABS (default 3.0y).",
    )
    parser.add_argument(
        "--out-dir", type=Path, default=OUTPUT_DIR_DEFAULT,
        help="Directory for pnl_*.csv and summary.csv dumps.",
    )
    args = parser.parse_args()

    if args.source == "registry":
        print("Sprint G event_study — window registry:")
        for name, w in WINDOWS.items():
            print(f"  {name:22s} catalyst={w.catalyst_date.isoformat()} "
                  f"window=[-{w.lookback_days}d, +{w.lookahead_days}d]  "
                  f"rebalance={w.rebalance_freq_days}d")
        print("\nRe-run with `--source=warehouse` to execute the 3-panel "
              "backtest on real DuckDB-backed fixtures. Prerequisites:")
        print("  python -m data.ingest.fred")
        print("  python -m data.ingest.yahoo_macro")
        print("  python -m data.ingest.regulatory_catalysts")
        print("  python -m signals.bsi")
        raise SystemExit(0)

    # --source=warehouse — real backtest.
    window_keys = args.windows or list(WINDOWS)
    unknown = [k for k in window_keys if k not in WINDOWS]
    if unknown:
        raise SystemExit(f"unknown windows: {unknown} "
                         f"(valid: {list(WINDOWS)})")

    print(f"event_study | warehouse mode — loading {len(window_keys)} window(s)"
          f"{' with duration adjustment' if args.apply_duration else ''}")
    catalysts = load_catalysts()
    fixtures: dict[str, WindowFixture] = {}
    for k in window_keys:
        fixtures[k] = load_window_from_warehouse(
            k,
            apply_duration_adjustment=args.apply_duration,
            tranche_wal_years=args.tranche_wal,
        )
    results = run_all_windows(fixtures, out_dir=args.out_dir, catalysts=catalysts)
    print(f"\nevent_study | wrote CSVs to {args.out_dir.resolve()}")
    # Compact textual summary.
    print(f"\n{'WINDOW':22s} {'PANEL':14s} {'TRS ret':>8s} {'TRS Sh':>7s} "
          f"{'TRS MDD':>8s} | {'NAIVE ret':>10s} {'NAIVE Sh':>9s} {'Appr':>5s}")
    print("-" * 96)
    for name, cmp in results.items():
        for mode, panel in cmp.panels.items():
            t, n = panel.trs_stats, panel.naive_stats
            print(f"{name:22s} {mode.value:14s} "
                  f"{t.total_return:+8.4f} {t.sharpe:+7.2f} {t.max_drawdown:+8.4f} | "
                  f"{n.total_return:+10.4f} {n.sharpe:+9.2f} "
                  f"{int(panel.gate_approved.sum()):>5d}")
    raise SystemExit(0)
