"""
Execution-discipline gates -- HFT-style pre-trade and post-trade discipline
=============================================================================

Bundles four upgrades that close the gap between "amateur fires market orders
at mid" and "professionally defensible execution."

  Bundle 1  liquidity_gate_check(ticker, notional)        -> dict
            Five-question pre-trade liquidity gate (spread, ADV, time-of-day,
            halt, tape alive). Returns PASS / WARN / BLOCK plus diagnostics.

  Bundle 2  compute_borrow_drag(ticker, notional, days)   -> float USD
            Subtracts the borrow-rental cost from short P&L so reported
            returns reflect what a real prime broker would charge.

  Bundle 3  round_number_aware_stop(price, side)          -> float
            Nudges stops 3-7 cents away from whole-dollar levels where the
            order book clusters and stop-hunting is common.

  Bundle 4  TCA helpers: simulate_fill(), tca_record()
            Records arrival-mid, spread, simulated fill, slippage per trade
            so realized returns are net of execution cost.

Designed to plug into risk_engine.py at the action-emission layer. No state
held here; pure functions plus small dataclasses for cleanliness.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from datetime import datetime, time
from typing import Optional

import yfinance as yf

# ---------------------------------------------------------------------------
# Heuristics -- replace with real data when available
# ---------------------------------------------------------------------------

# Borrow rates by market-cap bucket (annualized). Best-effort defaults; can be
# overridden per ticker via BORROW_RATE_OVERRIDES. Real prime-broker rates
# live in the IBKR / Fidelity / Schwab daily borrow file.
BORROW_RATE_BUCKETS = [
    (50_000_000_000, 0.005),   # > $50B mcap : 50 bps
    (10_000_000_000, 0.010),   # $10-50B     : 100 bps
    ( 1_000_000_000, 0.030),   # $1-10B      : 300 bps
    (   100_000_000, 0.080),   # $100M-1B    : 800 bps
    (             0, 0.200),   # < $100M     : 2000 bps
]

BORROW_RATE_OVERRIDES = {
    # Known hard-to-borrow names (high short interest, recent IPO, distressed)
    "KLAR": 0.15,    # IPO Sep 2025
    "WRLD": 0.08,
    "CRMT": 0.06,
    "OPFI": 0.07,
    "SEZL": 0.06,
    "CURO": 0.20,    # delisted; pre-delisting rates were brutal
    "CVNA": 0.05,
    "AFRM": 0.04,
    "UPST": 0.06,
}

# Spread heuristic by 20-day average daily dollar volume (ADV_USD).
SPREAD_BPS_BY_ADV = [
    (1_000_000_000, 2.0),   # > $1B  ADV : 2  bps (top-tier liquid)
    (  100_000_000, 8.0),   # $100M-1B    : 8  bps
    (   10_000_000, 25.0),  # $10-100M    : 25 bps
    (    1_000_000, 80.0),  # $1-10M      : 80 bps
    (            0, 250.0), # < $1M       : 250 bps (illiquid)
]

# Pre-trade gate thresholds
MAX_SPREAD_BPS = 50.0          # block if spread exceeds this
MAX_PCT_OF_ADV = 0.02          # warn if position > 2% of ADV
TOD_WINDOW = (time(10, 0), time(15, 30))  # ET tradeable window


# ---------------------------------------------------------------------------
# Light-weight price/volume cache
# ---------------------------------------------------------------------------

_INFO_CACHE: dict[str, dict] = {}
_HIST_CACHE: dict[str, "pd.DataFrame"] = {}


def _get_info(ticker: str) -> dict:
    if ticker in _INFO_CACHE:
        return _INFO_CACHE[ticker]
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        info = {}
    _INFO_CACHE[ticker] = info
    return info


def _get_hist(ticker: str, days: int = 30) -> "pd.DataFrame":
    if ticker in _HIST_CACHE:
        return _HIST_CACHE[ticker]
    try:
        df = yf.Ticker(ticker).history(period=f"{days}d", auto_adjust=True)
    except Exception:
        import pandas as pd
        df = pd.DataFrame()
    _HIST_CACHE[ticker] = df
    return df


# ---------------------------------------------------------------------------
# Bundle 1 -- pre-trade liquidity gate
# ---------------------------------------------------------------------------

def estimate_adv_usd(ticker: str) -> float:
    """20-day average daily dollar volume."""
    h = _get_hist(ticker, days=30)
    if h.empty or "Volume" not in h or "Close" not in h:
        return 0.0
    tail = h.tail(20)
    return float((tail["Volume"] * tail["Close"]).mean())


def estimate_spread_bps(ticker: str, current_price: Optional[float] = None) -> float:
    """Best effort: use yfinance bid/ask if recent AND sane, else ADV-bucketed heuristic.

    yfinance bid/ask is often stale (last close) or garbage (0 or wild). Sanity
    rules: bid > 0, ask > bid, implied spread <= 300bps, mid within +-10% of
    recent close. Anything else falls back to the volume-bucket heuristic.
    """
    info = _get_info(ticker)
    bid = info.get("bid")
    ask = info.get("ask")
    if bid and ask and bid > 0 and ask > bid:
        mid = (bid + ask) / 2.0
        implied_bps = 10_000.0 * (ask - bid) / mid
        # sanity: stale or garbage yfinance quotes can show 50%+ spreads
        if implied_bps <= 300.0:
            # also sanity-check vs. recent close to detect stale snapshots
            h = _get_hist(ticker, days=5)
            if h.empty or "Close" not in h:
                return implied_bps
            recent_close = float(h["Close"].iloc[-1])
            if recent_close > 0 and 0.9 <= mid / recent_close <= 1.1:
                return implied_bps
    # heuristic fallback
    adv = estimate_adv_usd(ticker)
    for bucket_adv, bps in SPREAD_BPS_BY_ADV:
        if adv >= bucket_adv:
            return bps
    return 250.0


def in_tradeable_hours(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now()
    t = now.time()
    if now.weekday() >= 5:   # Sat/Sun
        return False
    return TOD_WINDOW[0] <= t <= TOD_WINDOW[1]


@dataclass
class GateVerdict:
    ticker: str
    position_notional_usd: float
    spread_bps: float
    adv_usd: float
    pct_of_adv: float
    in_hours: bool
    halt_suspected: bool
    spread_ok: bool
    adv_ok: bool
    tod_ok: bool
    tape_alive: bool
    verdict: str   # PASS | WARN | BLOCK
    reasons: list

    def to_dict(self) -> dict:
        return asdict(self)


def liquidity_gate_check(
    ticker: str,
    position_notional_usd: float,
    now: Optional[datetime] = None,
) -> GateVerdict:
    """Five-question pre-trade gate. PASS / WARN / BLOCK with diagnostics."""
    spread_bps = estimate_spread_bps(ticker)
    adv_usd = estimate_adv_usd(ticker)
    pct_of_adv = position_notional_usd / adv_usd if adv_usd else float("inf")
    in_hours = in_tradeable_hours(now)
    # halt heuristic: zero volume on last bar = suspect
    h = _get_hist(ticker, days=5)
    halt_suspected = bool(not h.empty and "Volume" in h and h["Volume"].iloc[-1] == 0)
    # tape alive heuristic: yfinance last quote within ~5 min during market hours
    # We approximate with "we got price data" since real-time isn't available.
    tape_alive = not h.empty

    spread_ok = spread_bps <= MAX_SPREAD_BPS
    adv_ok = pct_of_adv <= MAX_PCT_OF_ADV
    tod_ok = in_hours

    reasons = []
    if not spread_ok:
        reasons.append(f"spread {spread_bps:.0f}bps > {MAX_SPREAD_BPS:.0f}bps")
    if not adv_ok:
        reasons.append(f"size {pct_of_adv*100:.1f}% of ADV > {MAX_PCT_OF_ADV*100:.0f}%")
    if not tod_ok:
        reasons.append(f"outside tradeable hours {TOD_WINDOW[0]}-{TOD_WINDOW[1]} ET")
    if halt_suspected:
        reasons.append("zero-volume last bar (halt suspected)")
    if not tape_alive:
        reasons.append("no recent price data")

    # PASS = all green. WARN = ADV/spread soft fails. BLOCK = halt or no tape.
    if halt_suspected or not tape_alive:
        verdict = "BLOCK"
    elif spread_ok and adv_ok and tod_ok:
        verdict = "PASS"
    else:
        verdict = "WARN"

    return GateVerdict(
        ticker=ticker,
        position_notional_usd=position_notional_usd,
        spread_bps=round(spread_bps, 1),
        adv_usd=round(adv_usd, 0),
        pct_of_adv=round(pct_of_adv, 4),
        in_hours=in_hours,
        halt_suspected=halt_suspected,
        spread_ok=spread_ok,
        adv_ok=adv_ok,
        tod_ok=tod_ok,
        tape_alive=tape_alive,
        verdict=verdict,
        reasons=reasons,
    )


# ---------------------------------------------------------------------------
# Bundle 2 -- borrow-cost adjustment
# ---------------------------------------------------------------------------

def get_borrow_rate_annual(ticker: str) -> float:
    """Returns annual borrow rate as a decimal (e.g. 0.05 = 5%/yr)."""
    if ticker in BORROW_RATE_OVERRIDES:
        return BORROW_RATE_OVERRIDES[ticker]
    info = _get_info(ticker)
    mcap = info.get("marketCap") or 0
    for bucket_mcap, rate in BORROW_RATE_BUCKETS:
        if mcap >= bucket_mcap:
            return rate
    return 0.20


def compute_borrow_drag_usd(
    notional_usd: float,
    days_held: int,
    annual_borrow_rate: float,
) -> float:
    """Compounded borrow cost over the holding period."""
    if notional_usd <= 0 or days_held <= 0 or annual_borrow_rate <= 0:
        return 0.0
    daily_rate = annual_borrow_rate / 365.0
    factor = (1.0 + daily_rate) ** days_held - 1.0
    return notional_usd * factor


# ---------------------------------------------------------------------------
# Bundle 3 -- round-number-aware stop placement
# ---------------------------------------------------------------------------

def round_number_aware_stop(
    stop_price: float,
    side: str,
    dollar_threshold: float = 0.05,
    offset: float = 0.07,
) -> float:
    """Nudge stops away from round-dollar levels.

    For shorts (stop above entry): if stop is within `dollar_threshold` of a
    whole dollar, offset UPWARD by `offset` so the stop sits past the cluster
    instead of inside it. For longs (stop below entry): offset DOWNWARD.

    Examples:
        round_number_aware_stop(22.00, "SHORT")   -> 22.07
        round_number_aware_stop(22.04, "SHORT")   -> 22.07
        round_number_aware_stop(22.24, "SHORT")   -> 22.24  (already safe)
        round_number_aware_stop(50.02, "LONG")    -> 49.93
    """
    if stop_price <= 0:
        return stop_price
    nearest = round(stop_price)
    distance = abs(stop_price - nearest)
    if distance >= dollar_threshold:
        return stop_price
    if side.upper() == "SHORT":
        return nearest + offset
    return nearest - offset


# ---------------------------------------------------------------------------
# Bundle 4 -- TCA (Transaction Cost Analysis)
# ---------------------------------------------------------------------------

URGENCY_MULTIPLIER = {"low": 0.3, "normal": 0.5, "high": 0.8}


def simulate_fill(
    side: str,
    arrival_mid: float,
    spread_bps: float,
    urgency: str = "normal",
) -> float:
    """Realistic paper-trade fill price.

    For a SHORT entry (you sell), you cross the bid -- fill slightly below mid.
    For a LONG entry (you buy), you cross the ask -- fill slightly above mid.
    `urgency` scales how much of the half-spread you actually pay.
    """
    half_spread_pct = spread_bps / 10_000.0 / 2.0
    pay = half_spread_pct * URGENCY_MULTIPLIER.get(urgency, 0.5)
    if side.upper() == "SHORT":
        return arrival_mid * (1.0 - pay)
    return arrival_mid * (1.0 + pay)


@dataclass
class TCARecord:
    ts: str
    ticker: str
    side: str
    shares: int
    arrival_mid: float
    spread_bps: float
    simulated_fill: float
    slippage_bps: float

    def to_dict(self) -> dict:
        return asdict(self)


def tca_record(
    ticker: str,
    side: str,
    shares: int,
    arrival_mid: float,
    spread_bps: Optional[float] = None,
    urgency: str = "normal",
    now: Optional[datetime] = None,
) -> TCARecord:
    if spread_bps is None:
        spread_bps = estimate_spread_bps(ticker, arrival_mid)
    fill = simulate_fill(side, arrival_mid, spread_bps, urgency)
    if arrival_mid > 0:
        slip = abs(fill - arrival_mid) / arrival_mid * 10_000.0
    else:
        slip = 0.0
    return TCARecord(
        ts=(now or datetime.now()).isoformat(timespec="seconds"),
        ticker=ticker,
        side=side,
        shares=int(shares),
        arrival_mid=round(arrival_mid, 4),
        spread_bps=round(spread_bps, 1),
        simulated_fill=round(fill, 4),
        slippage_bps=round(slip, 1),
    )


# ---------------------------------------------------------------------------
# Bundle 4 -- VWAP execution algorithm (slice the order)
# ---------------------------------------------------------------------------

@dataclass
class VWAPFill:
    """Result of a VWAP slicing simulation."""
    ticker: str
    side: str
    shares_total: int
    n_slices: int
    avg_fill: float           # volume-weighted average fill price
    worst_slice_fill: float
    best_slice_fill: float
    duration_minutes: int
    slippage_bps: float       # avg_fill vs arrival_mid

    def to_dict(self) -> dict:
        return asdict(self)


URGENCY_PARTICIPATION = {
    "low":    (60, 12, 0.05),   # (duration_min, slices, half_spread_pay_per_slice)
    "normal": (30, 6,  0.30),
    "high":   (10, 3,  0.60),
}


def simulate_vwap_fill(
    ticker: str,
    side: str,
    shares_total: int,
    arrival_mid: float,
    *,
    urgency: str = "normal",
    realized_vol_pct: Optional[float] = None,
) -> VWAPFill:
    """Simulate a VWAP-style fill: shares sliced into N orders over a duration.

    Each slice is filled at a price drawn from (mid + noise) where noise reflects
    typical intraday drift over the window. The average is the volume-weighted
    fill the user would have realized. Compared to a market-on-arrival fill,
    VWAP captures realistic slippage including price drift during execution.
    """
    duration_min, n_slices, half_spread_factor = URGENCY_PARTICIPATION.get(
        urgency, URGENCY_PARTICIPATION["normal"]
    )
    spread_bps = estimate_spread_bps(ticker, arrival_mid)
    half_spread_pct = (spread_bps / 10_000.0) / 2.0
    base_pay = half_spread_factor * half_spread_pct

    # Drift model: assume realized 1-day vol of 2% (or override) -> sigma per slice
    vol_per_slice = (realized_vol_pct or 0.020) * math.sqrt(duration_min / (6.5 * 60))
    # deterministic micro-drift per slice; mid skews against the trade as it absorbs liquidity
    drift_per_slice = vol_per_slice / n_slices

    slice_fills: list[float] = []
    for i in range(n_slices):
        # accumulated drift moves against the side
        if side.upper() == "LONG":
            mid_i = arrival_mid * (1.0 + drift_per_slice * (i + 0.5))
            fill_i = mid_i * (1.0 + base_pay)
        else:
            mid_i = arrival_mid * (1.0 - drift_per_slice * (i + 0.5))
            fill_i = mid_i * (1.0 - base_pay)
        slice_fills.append(fill_i)

    avg_fill = sum(slice_fills) / len(slice_fills)
    slippage_bps = abs(avg_fill - arrival_mid) / arrival_mid * 10_000.0

    return VWAPFill(
        ticker=ticker,
        side=side.upper(),
        shares_total=int(shares_total),
        n_slices=n_slices,
        avg_fill=round(avg_fill, 4),
        worst_slice_fill=round(max(slice_fills) if side.upper() == "LONG"
                                else min(slice_fills), 4),
        best_slice_fill=round(min(slice_fills) if side.upper() == "LONG"
                               else max(slice_fills), 4),
        duration_minutes=duration_min,
        slippage_bps=round(slippage_bps, 1),
    )


# ---------------------------------------------------------------------------
# Bundle 6 -- adverse-selection awareness
# ---------------------------------------------------------------------------

# yfinance shortName / sharesShort / shortRatio coverage varies; these heuristic
# tiers fill the gap when fields are missing.
HARD_TO_BORROW_TICKERS = {
    "KLAR", "CURO", "RILY", "OPRT",   # IPOs, distressed, low float
}


@dataclass
class AdverseSelectionView:
    ticker: str
    short_interest_pct: Optional[float]   # SI / float
    days_to_cover: Optional[float]        # SI / avg_volume
    short_ratio_yf: Optional[float]
    is_htb: bool
    squeeze_risk: str                     # LOW / ELEVATED / HIGH
    size_multiplier: float                # recommended max-size scaler
    reasons: list

    def to_dict(self) -> dict:
        return asdict(self)


def adverse_selection_check(ticker: str, side: str = "SHORT") -> AdverseSelectionView:
    """Returns short-side adverse-selection diagnostics + recommended sizing scaler.

    The size_multiplier defaults to 1.0 (full size) and reduces as squeeze risk
    rises. WRLD with 25% SI and 8 days-to-cover => 0.5x. KLAR HTB => 0.4x.
    """
    info = _get_info(ticker)
    shares_short = info.get("sharesShort")
    float_shares = info.get("floatShares") or info.get("sharesOutstanding")
    avg_vol = info.get("averageVolume10days") or info.get("averageVolume") or 0
    short_ratio = info.get("shortRatio")

    si_pct = (shares_short / float_shares) if (shares_short and float_shares and float_shares > 0) else None
    days_to_cover = (shares_short / avg_vol) if (shares_short and avg_vol and avg_vol > 0) else None
    is_htb = ticker in HARD_TO_BORROW_TICKERS or get_borrow_rate_annual(ticker) > 0.10

    reasons: list[str] = []
    score = 0
    if si_pct is not None:
        if si_pct > 0.25:
            score += 2; reasons.append(f"SI {si_pct*100:.0f}% > 25%")
        elif si_pct > 0.10:
            score += 1; reasons.append(f"SI {si_pct*100:.0f}% > 10%")
    if days_to_cover is not None:
        if days_to_cover > 7:
            score += 2; reasons.append(f"days-to-cover {days_to_cover:.1f} > 7")
        elif days_to_cover > 3:
            score += 1; reasons.append(f"days-to-cover {days_to_cover:.1f} > 3")
    if is_htb:
        score += 1; reasons.append("hard-to-borrow")
    if short_ratio and short_ratio > 5:
        score += 1; reasons.append(f"yf short-ratio {short_ratio:.1f}")

    if score >= 4:
        squeeze_risk = "HIGH"; multiplier = 0.4
    elif score >= 2:
        squeeze_risk = "ELEVATED"; multiplier = 0.6
    elif score >= 1:
        squeeze_risk = "MODERATE"; multiplier = 0.8
    else:
        squeeze_risk = "LOW"; multiplier = 1.0

    # adverse-selection only meaningful for the short side
    if side.upper() == "LONG":
        return AdverseSelectionView(
            ticker=ticker, short_interest_pct=si_pct, days_to_cover=days_to_cover,
            short_ratio_yf=short_ratio, is_htb=is_htb,
            squeeze_risk="N/A (long)", size_multiplier=1.0,
            reasons=["adverse-selection check N/A for long positions"],
        )

    return AdverseSelectionView(
        ticker=ticker,
        short_interest_pct=round(si_pct, 4) if si_pct is not None else None,
        days_to_cover=round(days_to_cover, 2) if days_to_cover is not None else None,
        short_ratio_yf=round(short_ratio, 2) if short_ratio is not None else None,
        is_htb=is_htb,
        squeeze_risk=squeeze_risk,
        size_multiplier=multiplier,
        reasons=reasons,
    )


# ---------------------------------------------------------------------------
# CLI -- standalone sanity check
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("ticker")
    p.add_argument("--notional", type=float, default=10_000.0)
    p.add_argument("--shares", type=int, default=0)
    p.add_argument("--side", default="SHORT")
    p.add_argument("--days", type=int, default=30, help="days held (for borrow)")
    args = p.parse_args()

    print(f"\n=== Execution-gate dry-run: {args.ticker} ===\n")

    # Bundle 1
    g = liquidity_gate_check(args.ticker, args.notional)
    print(f"LIQUIDITY GATE: {g.verdict}")
    print(f"  spread:       {g.spread_bps:>6.1f} bps   (max {MAX_SPREAD_BPS})")
    print(f"  ADV (USD):    ${g.adv_usd:>15,.0f}")
    print(f"  position %ADV:{g.pct_of_adv*100:>6.2f}%        (max {MAX_PCT_OF_ADV*100}%)")
    print(f"  tradeable:    {g.tod_ok}   halt-suspected: {g.halt_suspected}   tape: {g.tape_alive}")
    if g.reasons:
        for r in g.reasons:
            print(f"    - {r}")

    # Bundle 2
    rate = get_borrow_rate_annual(args.ticker)
    drag = compute_borrow_drag_usd(args.notional, args.days, rate)
    print(f"\nBORROW COST")
    print(f"  annual rate:  {rate*100:.2f}%")
    print(f"  drag over {args.days}d on ${args.notional:,.0f}: ${drag:,.2f}")

    # Bundle 3 (demo)
    print(f"\nROUND-NUMBER STOP ADJUSTMENT (examples)")
    for px in [22.00, 22.04, 22.24, 50.02, 7.99]:
        adj = round_number_aware_stop(px, args.side)
        flag = "ADJUSTED" if adj != px else "OK"
        print(f"  raw ${px:>6.2f} -> ${adj:>6.2f}  [{flag}]")

    # Bundle 4
    h = _get_hist(args.ticker, days=2)
    if not h.empty:
        mid = float(h["Close"].iloc[-1])
        tca = tca_record(args.ticker, args.side, max(1, args.shares), mid)
        print(f"\nTCA")
        print(f"  arrival mid:  ${tca.arrival_mid}")
        print(f"  spread:       {tca.spread_bps} bps")
        print(f"  sim. fill:    ${tca.simulated_fill}")
        print(f"  slippage:     {tca.slippage_bps} bps")
    print()
