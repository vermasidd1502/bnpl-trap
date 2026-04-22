"""
Daily-step P&L simulator — Sprint G primitive.

Composes the two PENDING four-critique fixes into a single accounting loop
that the event-study driver calls one day at a time.

Fix #1  —  regime-dependent bid/ask haircut
--------------------------------------------
    ba_bps_t  =  ba_base  +  ba_stress · max(0, MOVE_t / MOVE_median − 1)

with ba_base = 35 bps and ba_stress = 80 bps (config/thresholds.yaml
§transaction_costs). Charged as a HALF-spread on absolute turnover at every
rebalance — round-trip cost = 2 × half-spread. A separate, thinner bps
charge (default 2 bps) is levied on the macro-hedge sleeve (HYG short;
liquid ETF → friction ≪ TRS).

Fix #4  —  SOFR credit on unallocated cash + financing spread on margin
-----------------------------------------------------------------------
    daily_pnl  =  (SOFR_t / 252) · cash_t                   ← cash carry
                + Δmtm_trs_t
                + Δmtm_hedge_t
                − transaction_cost_t                        ← Fix #1
                − financing_spread · trs_margin_t / 252     ← margin drag

The "two-book" state (cash vs margin) is the whole point — without it the
strategy shows a 0% return in quiet regimes while T-bills earn SOFR, which
flips the Sharpe artificially negative vs any cash-inclusive benchmark.

Sign convention (inherited from portfolio/book.py)
--------------------------------------------------
- `trs_notional < 0`   means a TRS SHORT on junior ABS. When the tranche
  loses value (tranche_return < 0 in the MTM feed), the short profits →
  Δmtm_trs = -trs_notional · tranche_return > 0.
- `hedge_notional < 0` means an HYG short (or equivalent). Same MTM sign
  mechanics as the TRS leg.
- `short_equity_notional < 0` for the naive AFRM comparison arm.

The simulator is deterministic: every input series is provided by the
caller (no I/O, no randomness). The event-study driver feeds realized
tranche / HYG / equity returns from the DuckDB warehouse.

This module has NO dependency on CVXPY, LangGraph, LLM clients, or
network — it is a pure numerical primitive so the test budget stays tight.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field, replace
from typing import Iterable, Optional

import numpy as np

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Defaults (mirror config/thresholds.yaml). The event-study driver loads the
# YAML and overrides these; direct callers of the sim can rely on the defaults.
# -----------------------------------------------------------------------------
DEFAULTS = {
    # Fix #1
    "trs_ba_base_bps":    35.0,
    "trs_ba_stress_bps":  80.0,
    "move_median_level":  95.0,
    "hedge_ba_bps":        2.0,
    "equity_ba_bps":      10.0,   # AFRM equity short round-trip ≈ 20 bps
    # Fix #4
    "use_sofr":            True,
    "financing_spread_bps": 50.0,
    "margin_ratio_trs":    0.20,  # 20% of TRS notional posted as margin
    # Naive equity short comparison arm
    "equity_htb_annual":   0.15,  # 15% hard-to-borrow fee on AFRM short
    "equity_margin_ratio": 0.30,  # Reg T + borrow = 30%
    # Accounting
    "days_per_year":       252,
}


# ============================================================================
# Section 1 — Transaction-cost primitives (Fix #1)
# ============================================================================
def regime_scaled_ba_bps(
    move_level: float,
    *,
    ba_base: float = DEFAULTS["trs_ba_base_bps"],
    ba_stress: float = DEFAULTS["trs_ba_stress_bps"],
    move_median: float = DEFAULTS["move_median_level"],
) -> float:
    """Fix #1: regime-dependent B/A in basis points.

        ba_bps  =  ba_base  +  ba_stress · max(0, MOVE / MOVE_median − 1)

    Examples (defaults 35 / 80 / 95):
      - MOVE = 95   →  35 bps       (at median, pure baseline)
      - MOVE = 142  →  35 + 80·0.5  = 75 bps
      - MOVE = 190  →  35 + 80·1.0  = 115 bps
      - MOVE = 285  →  35 + 80·2.0  = 195 bps

    MOVE levels below the median do NOT receive a rebate — dealer B/A is
    asymmetric; quiet regimes still cost at least `ba_base`.
    """
    if move_level is None or not math.isfinite(move_level):
        return float(ba_base)
    excess = max(0.0, float(move_level) / float(move_median) - 1.0)
    return float(ba_base) + float(ba_stress) * excess


def apply_transaction_cost(notional_delta: float, ba_bps: float) -> float:
    """Half-spread charged on absolute notional turnover.

    Round-trip cost = 2 × half-spread, but we only charge one side per
    rebalance; the matching side is paid when the position is unwound.
    Always returns a non-negative number (a cost).
    """
    return abs(float(notional_delta)) * (float(ba_bps) / 10_000.0) / 2.0


# ============================================================================
# Section 2 — PortfolioState + daily step (TRS book + macro-hedge sleeve)
# ============================================================================
@dataclass
class PortfolioState:
    """Two-book state — cash vs margin — carried across daily steps.

    All monetary fields are in strategy units (1.0 = one unit of capital);
    scale by `starting_capital` at reporting time.
    """
    # Balances
    cash: float                                # unallocated; earns SOFR (Fix #4)
    trs_margin: float = 0.0                    # posted against TRS notional
    trs_notional: float = 0.0                  # signed; short → negative
    hedge_notional: float = 0.0                # signed; HYG short → negative
    # Cumulative accounting
    mtm_trs_cum: float = 0.0                   # cumulative MTM on TRS leg
    mtm_hedge_cum: float = 0.0                 # cumulative MTM on sleeve
    transaction_costs_cum: float = 0.0         # cumulative B/A paid (Fix #1)
    cash_carry_cum: float = 0.0                # cumulative SOFR earned (Fix #4)
    financing_drag_cum: float = 0.0            # cumulative broker spread paid

    # Bookkeeping
    n_steps: int = 0

    @property
    def equity(self) -> float:
        """Strategy NAV = cash + margin posted + unrealized MTM."""
        return self.cash + self.trs_margin + self.mtm_trs_cum + self.mtm_hedge_cum


@dataclass
class DayBreakdown:
    """Per-day P&L decomposition. Every field is signed from the P&L POV
    (positive = profit, negative = loss)."""
    date_idx: int
    cash_carry: float = 0.0
    financing_drag: float = 0.0
    mtm_trs: float = 0.0
    mtm_hedge: float = 0.0
    transaction_costs: float = 0.0
    total: float = 0.0

    def __post_init__(self):
        self.total = (self.cash_carry + self.financing_drag
                      + self.mtm_trs + self.mtm_hedge
                      + self.transaction_costs)


def step_day(
    state: PortfolioState,
    *,
    move_level: float,
    sofr_annual: float,
    tranche_return: float,                   # realized tranche return; short profits when < 0
    hyg_return: float,                       # realized HYG return; HYG short profits when < 0
    target_trs_notional: float,              # signed; pod may have rebalanced
    target_hedge_notional: float,            # signed
    config: Optional[dict] = None,
    date_idx: int = 0,
) -> tuple[PortfolioState, DayBreakdown]:
    """Advance the two-book state by one trading day.

    Order of operations (matters for P&L attribution):
      1. Mark existing positions to the day's returns (Δmtm).
      2. Accrue cash carry at SOFR on unallocated cash (Fix #4).
      3. Debit margin-financing spread at (SOFR + spread) − SOFR = spread.
      4. Rebalance to targets; charge B/A half-spread on turnover (Fix #1).
      5. Update margin posted so it matches the new TRS notional.

    This order intentionally marks BEFORE rebalancing so the cost of rolling
    is charged on today's target, not on stale weights.
    """
    cfg = {**DEFAULTS, **(config or {})}
    days_per_year = float(cfg["days_per_year"])

    # --- (1) Mark-to-market on existing positions ---------------------------
    # trs_notional < 0 for a short. Δmtm = -notional · return means:
    # if tranche_return = -0.01 (spreads widened, tranche lost 1%), a short of
    # notional=-1.0 earns +1.0 · 0.01 = +0.01 — which is what we want.
    mtm_trs = -state.trs_notional * float(tranche_return)
    mtm_hedge = -state.hedge_notional * float(hyg_return)

    # --- (2) SOFR credit on cash (Fix #4) -----------------------------------
    if cfg.get("use_sofr", True):
        cash_carry = (float(sofr_annual) / days_per_year) * state.cash
    else:
        cash_carry = 0.0

    # --- (3) Financing-spread drag on posted margin (Fix #4) ----------------
    fin_spread = float(cfg["financing_spread_bps"]) / 10_000.0
    financing_drag = -(fin_spread / days_per_year) * state.trs_margin

    # --- (4) Rebalance + transaction cost (Fix #1) --------------------------
    trs_delta = float(target_trs_notional) - state.trs_notional
    hedge_delta = float(target_hedge_notional) - state.hedge_notional

    trs_ba_bps = regime_scaled_ba_bps(
        move_level,
        ba_base=cfg["trs_ba_base_bps"],
        ba_stress=cfg["trs_ba_stress_bps"],
        move_median=cfg["move_median_level"],
    )
    hedge_ba_bps = float(cfg["hedge_ba_bps"])

    trs_cost = apply_transaction_cost(trs_delta, trs_ba_bps)
    hedge_cost = apply_transaction_cost(hedge_delta, hedge_ba_bps)
    total_tx_cost = trs_cost + hedge_cost
    transaction_costs_signed = -total_tx_cost   # P&L view (always ≤ 0)

    # --- (5) Update margin: posted against |target TRS notional| ------------
    new_trs_notional = float(target_trs_notional)
    new_hedge_notional = float(target_hedge_notional)
    new_margin = abs(new_trs_notional) * float(cfg["margin_ratio_trs"])
    margin_delta = new_margin - state.trs_margin   # >0 posts more, <0 frees

    # Cash flow = MTM in + carry + drag + tx costs − margin posted
    new_cash = (
        state.cash
        + mtm_trs + mtm_hedge
        + cash_carry + financing_drag
        + transaction_costs_signed
        - margin_delta
    )

    new_state = PortfolioState(
        cash=new_cash,
        trs_margin=new_margin,
        trs_notional=new_trs_notional,
        hedge_notional=new_hedge_notional,
        mtm_trs_cum=state.mtm_trs_cum + mtm_trs,
        mtm_hedge_cum=state.mtm_hedge_cum + mtm_hedge,
        transaction_costs_cum=state.transaction_costs_cum + total_tx_cost,
        cash_carry_cum=state.cash_carry_cum + cash_carry,
        financing_drag_cum=state.financing_drag_cum + abs(financing_drag),
        n_steps=state.n_steps + 1,
    )

    breakdown = DayBreakdown(
        date_idx=date_idx,
        cash_carry=cash_carry,
        financing_drag=financing_drag,
        mtm_trs=mtm_trs,
        mtm_hedge=mtm_hedge,
        transaction_costs=transaction_costs_signed,
    )
    return new_state, breakdown


# ============================================================================
# Section 3 — Naive AFRM equity-short comparison arm
# ============================================================================
@dataclass
class EquityShortState:
    """Single-book equity short — cash + margin + MTM. HTB fee accrues daily."""
    cash: float
    equity_margin: float = 0.0
    short_notional: float = 0.0        # signed; <0 = short
    mtm_cum: float = 0.0
    transaction_costs_cum: float = 0.0
    cash_carry_cum: float = 0.0
    htb_cum: float = 0.0
    n_steps: int = 0

    @property
    def equity(self) -> float:
        return self.cash + self.equity_margin + self.mtm_cum


@dataclass
class EquityDayBreakdown:
    date_idx: int
    cash_carry: float = 0.0
    mtm: float = 0.0
    transaction_costs: float = 0.0
    htb_fee: float = 0.0                # signed ≤ 0 from P&L POV
    total: float = 0.0

    def __post_init__(self):
        self.total = (self.cash_carry + self.mtm
                      + self.transaction_costs + self.htb_fee)


def step_equity_short_day(
    state: EquityShortState,
    *,
    move_level: float,
    sofr_annual: float,
    equity_return: float,                    # realized AFRM return; short profits when < 0
    target_short_notional: float,            # signed; <0 for shorts
    config: Optional[dict] = None,
    date_idx: int = 0,
) -> tuple[EquityShortState, EquityDayBreakdown]:
    """Advance the naive AFRM short arm by one trading day.

    Mechanics differ from TRS:
      - NO macro-hedge sleeve (this is the naive comparison).
      - HTB fee debited daily on |short_notional| at an annualized rate.
      - B/A scales with MOVE like the TRS arm (same regime-dependence) but
        with its own base (equity is liquid; narrower spread).
      - SOFR still accrues on unallocated cash (we're honest about both arms).
    """
    cfg = {**DEFAULTS, **(config or {})}
    days_per_year = float(cfg["days_per_year"])

    # --- MTM ----------------------------------------------------------------
    mtm = -state.short_notional * float(equity_return)

    # --- SOFR on cash -------------------------------------------------------
    if cfg.get("use_sofr", True):
        cash_carry = (float(sofr_annual) / days_per_year) * state.cash
    else:
        cash_carry = 0.0

    # --- HTB borrow fee (daily accrual, signed ≤ 0) -------------------------
    htb_annual = float(cfg["equity_htb_annual"])
    htb_fee = -(htb_annual / days_per_year) * abs(state.short_notional)

    # --- Rebalance + B/A ----------------------------------------------------
    eq_delta = float(target_short_notional) - state.short_notional
    eq_ba_bps = regime_scaled_ba_bps(
        move_level,
        ba_base=cfg["equity_ba_bps"],
        ba_stress=cfg["trs_ba_stress_bps"],  # stress scaling mirrors TRS (dealer-inventory-risk broadens both)
        move_median=cfg["move_median_level"],
    )
    tx_cost = apply_transaction_cost(eq_delta, eq_ba_bps)
    tx_signed = -tx_cost

    # --- Margin update ------------------------------------------------------
    new_short = float(target_short_notional)
    new_margin = abs(new_short) * float(cfg["equity_margin_ratio"])
    margin_delta = new_margin - state.equity_margin

    new_cash = (
        state.cash
        + mtm + cash_carry
        + tx_signed + htb_fee
        - margin_delta
    )

    new_state = EquityShortState(
        cash=new_cash,
        equity_margin=new_margin,
        short_notional=new_short,
        mtm_cum=state.mtm_cum + mtm,
        transaction_costs_cum=state.transaction_costs_cum + tx_cost,
        cash_carry_cum=state.cash_carry_cum + cash_carry,
        htb_cum=state.htb_cum + abs(htb_fee),
        n_steps=state.n_steps + 1,
    )
    breakdown = EquityDayBreakdown(
        date_idx=date_idx,
        cash_carry=cash_carry,
        mtm=mtm,
        transaction_costs=tx_signed,
        htb_fee=htb_fee,
    )
    return new_state, breakdown


# ============================================================================
# Section 4 — Rolled-up summary statistics
# ============================================================================
@dataclass
class SummaryStats:
    total_return: float
    ann_return: float
    ann_vol: float
    sharpe: float
    max_drawdown: float
    hit_rate: float
    n_days: int
    transaction_costs_pct: float
    cash_carry_pct: float


def summarize(
    daily_pnl: Iterable[float],
    *,
    starting_capital: float = 1.0,
    transaction_costs_total: float = 0.0,
    cash_carry_total: float = 0.0,
    days_per_year: int = 252,
    sofr_annual: float = 0.0,
) -> SummaryStats:
    """Compute Sharpe / MaxDD / hit-rate from a daily P&L series.

    Parameters
    ----------
    daily_pnl : iterable of SIGNED per-day P&L in strategy units
                (same units as `starting_capital`).
    starting_capital : divides every statistic to normalize.
    sofr_annual : if nonzero, Sharpe is computed vs SOFR risk-free rate
                  (excess return); otherwise Sharpe is raw / vol.

    NaN-safe: an empty series yields zeros.
    """
    arr = np.asarray(list(daily_pnl), dtype=float)
    if arr.size == 0 or starting_capital <= 0:
        return SummaryStats(0, 0, 0, 0, 0, 0, 0, 0, 0)

    daily_ret = arr / float(starting_capital)
    total_return = float(daily_ret.sum())
    n = int(daily_ret.size)
    ann_return = total_return * (days_per_year / n)

    if n > 1:
        vol = float(daily_ret.std(ddof=1))
        ann_vol = vol * math.sqrt(days_per_year)
    else:
        ann_vol = 0.0

    rf_daily = float(sofr_annual) / float(days_per_year)
    excess = daily_ret - rf_daily
    if ann_vol > 1e-12:
        sharpe = float(excess.mean() / excess.std(ddof=1)) * math.sqrt(days_per_year) \
            if n > 1 and excess.std(ddof=1) > 1e-12 else 0.0
    else:
        sharpe = 0.0

    # Max drawdown on the cumulative P&L curve (not on log returns — strategy
    # can go negative through zero; log is undefined there).
    cum = np.cumsum(daily_ret)
    running_max = np.maximum.accumulate(cum)
    drawdown = cum - running_max
    max_dd = float(drawdown.min()) if drawdown.size > 0 else 0.0

    nonzero = daily_ret[daily_ret != 0.0]
    hit_rate = float((nonzero > 0).mean()) if nonzero.size > 0 else 0.0

    return SummaryStats(
        total_return=total_return,
        ann_return=ann_return,
        ann_vol=ann_vol,
        sharpe=sharpe,
        max_drawdown=max_dd,
        hit_rate=hit_rate,
        n_days=n,
        transaction_costs_pct=float(transaction_costs_total) / float(starting_capital),
        cash_carry_pct=float(cash_carry_total) / float(starting_capital),
    )


# ============================================================================
# Section 5 — Convenience runners (vectorized daily loops)
# ============================================================================
def run_trs_arm(
    *,
    move_series: np.ndarray,
    sofr_series: np.ndarray,
    tranche_returns: np.ndarray,
    hyg_returns: np.ndarray,
    target_trs_notionals: np.ndarray,
    target_hedge_notionals: np.ndarray,
    starting_cash: float = 1.0,
    config: Optional[dict] = None,
) -> tuple[PortfolioState, list[DayBreakdown]]:
    """Thin driver: step the TRS arm over an aligned set of daily series.

    All input arrays must have identical length T. The caller is responsible
    for ensuring target notionals were produced by a legal pod decision
    (3-gate compliance on that day) — passing 0.0 on blocked days is the
    natural way to represent "gate failed → no book."
    """
    T = len(move_series)
    arrays = {
        "sofr_series": sofr_series, "tranche_returns": tranche_returns,
        "hyg_returns": hyg_returns,
        "target_trs_notionals": target_trs_notionals,
        "target_hedge_notionals": target_hedge_notionals,
    }
    for name, arr in arrays.items():
        if len(arr) != T:
            raise ValueError(f"length mismatch: {name} has {len(arr)}, expected {T}")

    state = PortfolioState(cash=float(starting_cash))
    breakdowns: list[DayBreakdown] = []
    for t in range(T):
        state, bd = step_day(
            state,
            move_level=float(move_series[t]),
            sofr_annual=float(sofr_series[t]),
            tranche_return=float(tranche_returns[t]),
            hyg_return=float(hyg_returns[t]),
            target_trs_notional=float(target_trs_notionals[t]),
            target_hedge_notional=float(target_hedge_notionals[t]),
            config=config,
            date_idx=t,
        )
        breakdowns.append(bd)
    return state, breakdowns


def run_equity_short_arm(
    *,
    move_series: np.ndarray,
    sofr_series: np.ndarray,
    equity_returns: np.ndarray,
    target_short_notionals: np.ndarray,
    starting_cash: float = 1.0,
    config: Optional[dict] = None,
) -> tuple[EquityShortState, list[EquityDayBreakdown]]:
    """Thin driver for the naive AFRM comparison arm."""
    T = len(move_series)
    if not (len(sofr_series) == len(equity_returns) == len(target_short_notionals) == T):
        raise ValueError("naive-arm series must have identical length")

    state = EquityShortState(cash=float(starting_cash))
    breakdowns: list[EquityDayBreakdown] = []
    for t in range(T):
        state, bd = step_equity_short_day(
            state,
            move_level=float(move_series[t]),
            sofr_annual=float(sofr_series[t]),
            equity_return=float(equity_returns[t]),
            target_short_notional=float(target_short_notionals[t]),
            config=config,
            date_idx=t,
        )
        breakdowns.append(bd)
    return state, breakdowns


# ============================================================================
# Config loader — pulls the two Sprint G YAML blocks with safe fallbacks
# ============================================================================
def load_sim_config() -> dict:
    """Merge config/thresholds.yaml §transaction_costs + §cash_carry onto DEFAULTS.

    Called by `backtest.event_study`. Module-level DEFAULTS are the source of
    truth when thresholds.yaml lacks a key (forward compat with older YAMLs).
    """
    from data.settings import load_thresholds
    th = load_thresholds() or {}
    cfg = dict(DEFAULTS)
    tx = th.get("transaction_costs") or {}
    cc = th.get("cash_carry") or {}
    mapping = {
        "trs_ba_base_bps":      tx.get("trs_ba_base_bps"),
        "trs_ba_stress_bps":    tx.get("trs_ba_stress_bps"),
        "move_median_level":    tx.get("move_median_level"),
        "hedge_ba_bps":         tx.get("hedge_ba_bps"),
        "equity_ba_bps":        tx.get("equity_ba_bps"),
        "equity_htb_annual":    tx.get("equity_htb_annual"),
        "use_sofr":             cc.get("use_sofr"),
        "financing_spread_bps": cc.get("financing_spread_bps"),
        "margin_ratio_trs":     cc.get("margin_ratio_trs"),
        "equity_margin_ratio":  cc.get("equity_margin_ratio"),
    }
    for k, v in mapping.items():
        if v is not None:
            cfg[k] = v
    return cfg
