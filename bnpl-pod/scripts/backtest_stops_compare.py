"""
Strategy comparator: BSI signal with and without dynamic stop-loss machinery.
=============================================================================

For each fire-week in the 2025-04-15 to today deployment window, simulate
three trading strategies and compute Sharpe-adjusted statistics:

  A: RAW       -- short at fire-week, hold to fixed horizon, exit at horizon close
  B: STOPS     -- short at fire-week, exit at first of:
                    - Chandelier trailing stop (active after +20% MFE)
                    - Milestone partial at +50% and +100% MFE
                    - Time stop at archetype max holding period
                    - Horizon reached
  C: STOPS+RE  -- like B, but allow at-most-one re-entry post earnings or post
                  trail-firing IF BSI is still saturated (z >= 2.5)

Output:
  outputs/strategy_compare.csv     per-trade realized returns
  outputs/strategy_summary.csv     per-strategy aggregate stats
  outputs/strategy_summary.md      ready-to-paste markdown for §7

Reads existing conditional_fires.csv so the universe and fire-weeks match
the empirical study; reuses get_prices() from backtest_conditional.
"""
from __future__ import annotations

import math
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# reuse the existing infra
from backtest_conditional import get_prices

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INCEPTION = date(2025, 4, 15)
HORIZONS = [30, 90, 365]
ARCHETYPE_MAX_DAYS = {"BLITZ": 30, "SCOUT": 60, "GUARDIAN": 90, "ROBO": 120}
ARCHETYPE_K_BASE = {"BLITZ": 1.5, "SCOUT": 2.0, "GUARDIAN": 2.5}  # ROBO not in fires
TRAIL_ACTIVATION_MFE = 0.20
MFE_LOCK_PCT = 0.25
MILESTONES = [0.50, 1.00]
PARTIAL_FRACTION = 1.0 / 3.0
THESIS_COOL_Z = 1.0   # z dropping below this for re-entry block
REENTRY_Z_THRESHOLD = 2.5  # only re-enter if signal still saturated

FIRES_PATH = Path(__file__).resolve().parents[1] / "backtest" / "outputs" / "conditional" / "conditional_fires.csv"
OUT_DIR = Path(__file__).resolve().parents[1] / "backtest" / "outputs" / "strategy_compare"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Per-trade simulators
# ---------------------------------------------------------------------------

def realized_short_return(entry_px: float, exit_px: float) -> float:
    """Realized return for a short position as a percent."""
    if entry_px <= 0:
        return 0.0
    return 100.0 * (entry_px - exit_px) / entry_px


def simulate_raw(prices: pd.DataFrame, entry_date: date, horizon: int) -> dict:
    """Strategy A: hold to horizon, exit at horizon close."""
    after = prices.index[prices.index >= pd.Timestamp(entry_date)]
    if len(after) == 0:
        return {"realized_pct": None, "holding_days": None, "exit_reason": "no_entry_px"}
    entry_idx = after[0]
    entry_px = float(prices.loc[entry_idx, "Close"])
    target_ts = entry_idx + pd.Timedelta(days=horizon)
    on_or_before = prices.index[prices.index <= target_ts]
    if len(on_or_before) == 0:
        return {"realized_pct": None, "holding_days": None, "exit_reason": "no_horizon_px"}
    exit_idx = on_or_before[-1]
    if (exit_idx - entry_idx).days < horizon * 0.6:
        return {"realized_pct": None, "holding_days": None, "exit_reason": "horizon_too_short"}
    exit_px = float(prices.loc[exit_idx, "Close"])
    return {
        "realized_pct": realized_short_return(entry_px, exit_px),
        "holding_days": int((exit_idx - entry_idx).days),
        "exit_reason": "horizon",
    }


def simulate_stops(
    prices: pd.DataFrame,
    entry_date: date,
    horizon: int,
    archetype: str,
) -> dict:
    """Strategy B: trail + milestone partials + time stop + horizon.

    Returns a single realized return weighted across the 1/3 + 1/3 + 1/3 partial
    structure (final 1/3 rides the trail).
    """
    after = prices.index[prices.index >= pd.Timestamp(entry_date)]
    if len(after) == 0:
        return {"realized_pct": None, "holding_days": None, "exit_reason": "no_entry_px"}
    entry_idx = after[0]
    entry_px = float(prices.loc[entry_idx, "Close"])
    if entry_px <= 0:
        return {"realized_pct": None, "holding_days": None, "exit_reason": "bad_entry_px"}

    k_base = ARCHETYPE_K_BASE.get(archetype, 2.0)
    time_stop_days = ARCHETYPE_MAX_DAYS.get(archetype, 60)
    horizon_idx_max = entry_idx + pd.Timedelta(days=horizon)
    time_idx_max = entry_idx + pd.Timedelta(days=time_stop_days)
    sim_idx_max = min(horizon_idx_max, time_idx_max)

    # Path slice
    path = prices.loc[entry_idx:sim_idx_max].copy()
    if len(path) < 2:
        return {"realized_pct": None, "holding_days": None, "exit_reason": "no_path"}

    # Rolling sigma (20d trailing) for Chandelier band
    sigma_series = path["Close"].pct_change().rolling(20, min_periods=5).std().fillna(0.02)

    best_low = entry_px
    realized_legs: list[tuple[float, float]] = []  # (fraction, leg_return)
    milestone_fired = {m: False for m in MILESTONES}
    fraction_remaining = 1.0
    exit_reason = "horizon"
    days_held = 0

    for i, (ts, row) in enumerate(path.iterrows()):
        days_held = (ts - entry_idx).days
        px = float(row["Close"])
        if px < best_low:
            best_low = px
        mfe_pct = (entry_px - best_low) / entry_px  # for shorts, MFE is favorable downward move

        # Milestone partials (one-shot per level)
        for level in MILESTONES:
            if mfe_pct >= level and not milestone_fired[level] and fraction_remaining > 0:
                take_frac = min(PARTIAL_FRACTION, fraction_remaining)
                leg_ret = realized_short_return(entry_px, px)
                realized_legs.append((take_frac, leg_ret))
                fraction_remaining -= take_frac
                milestone_fired[level] = True

        # Chandelier trail (active after +20% MFE)
        if mfe_pct >= TRAIL_ACTIVATION_MFE and fraction_remaining > 0 and days_held >= 1:
            sigma = float(sigma_series.iloc[i]) if i < len(sigma_series) else 0.02
            sqrt_h = math.sqrt(max(1, days_held))
            chandelier_stop = best_low + k_base * sigma * px * sqrt_h
            mfe_lock_stop = entry_px * (1 - MFE_LOCK_PCT * mfe_pct)
            trail_stop = min(chandelier_stop, mfe_lock_stop)
            if px >= trail_stop:
                leg_ret = realized_short_return(entry_px, px)
                realized_legs.append((fraction_remaining, leg_ret))
                fraction_remaining = 0.0
                exit_reason = "trail"
                break

        # Time stop
        if days_held >= time_stop_days and fraction_remaining > 0:
            leg_ret = realized_short_return(entry_px, px)
            realized_legs.append((fraction_remaining, leg_ret))
            fraction_remaining = 0.0
            exit_reason = "time_stop"
            break

    # If still holding at horizon, mark out
    if fraction_remaining > 0:
        exit_px = float(path["Close"].iloc[-1])
        leg_ret = realized_short_return(entry_px, exit_px)
        realized_legs.append((fraction_remaining, leg_ret))
        fraction_remaining = 0.0

    weighted_return = sum(f * r for f, r in realized_legs)
    return {
        "realized_pct": weighted_return,
        "holding_days": days_held,
        "exit_reason": exit_reason,
        "n_partials": sum(1 for r in realized_legs if r[0] == PARTIAL_FRACTION),
    }


def simulate_stops_with_reentry(
    prices: pd.DataFrame,
    entry_date: date,
    horizon: int,
    archetype: str,
    panel: pd.DataFrame,
    ticker: str,
) -> dict:
    """Strategy C: after a trail-fire, re-enter if BSI z still >= GUARDIAN.

    Approximates the re-entry rule from the engine spec: only re-enter when
    the BSI signal is still saturated (z >= 2.5). Looks up the BSI z at the
    proposed re-entry week from the weekly panel.
    """
    # First leg = strategy B
    leg1 = simulate_stops(prices, entry_date, horizon, archetype)
    if leg1["realized_pct"] is None:
        return leg1
    # Only consider re-entry if leg1 ended on a trail-stop or time-stop
    if leg1["exit_reason"] not in ("trail", "time_stop"):
        return {**leg1, "reentry": False}

    # Re-entry date = exit date of leg1
    re_entry_date = entry_date + timedelta(days=int(leg1["holding_days"]))
    # Find the BSI z at the closest week
    pz = panel[(panel["ticker"] == ticker) &
               (pd.to_datetime(panel["week_end"]) <= pd.Timestamp(re_entry_date))]
    if pz.empty:
        return {**leg1, "reentry": False, "reentry_reason": "no_z_data"}
    current_z = float(pz.sort_values("week_end").iloc[-1]["z_score"])
    if current_z < REENTRY_Z_THRESHOLD:
        return {**leg1, "reentry": False, "reentry_reason": f"z {current_z:.2f} < {REENTRY_Z_THRESHOLD}"}

    # Run leg2 = strategy B again from the re-entry date with reduced horizon
    days_used = leg1["holding_days"]
    remaining_horizon = max(7, horizon - days_used)
    leg2 = simulate_stops(prices, re_entry_date, remaining_horizon, archetype)
    if leg2["realized_pct"] is None:
        return {**leg1, "reentry": False, "reentry_reason": "leg2_failed"}

    # Composite return = leg1 + leg2 compounded (approx -- treat as additive in % terms)
    composite = leg1["realized_pct"] + leg2["realized_pct"]
    return {
        "realized_pct": composite,
        "holding_days": int(leg1["holding_days"] + leg2["holding_days"]),
        "exit_reason": f"{leg1['exit_reason']}+re-{leg2['exit_reason']}",
        "n_partials": leg1.get("n_partials", 0) + leg2.get("n_partials", 0),
        "reentry": True,
    }


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def annualized_sharpe(returns_pct: pd.Series, periods_per_year: float) -> float:
    """Sharpe based on per-trade returns and an approximate periods-per-year scaler."""
    if len(returns_pct) < 3 or returns_pct.std() == 0:
        return float("nan")
    return float(returns_pct.mean() / returns_pct.std()) * math.sqrt(periods_per_year)


def max_drawdown(returns_pct: pd.Series) -> float:
    """Worst peak-to-trough percentage decline in the cumulative equity curve."""
    eq = (1 + returns_pct / 100).cumprod()
    peak = eq.cummax()
    dd = (eq - peak) / peak
    return float(dd.min() * 100)


def aggregate_stats(label: str, df: pd.DataFrame, horizon: int) -> dict:
    valid = df["realized_pct"].dropna()
    n = len(valid)
    if n == 0:
        return {"strategy": label, "horizon_d": horizon, "n": 0}
    mean = float(valid.mean())
    std = float(valid.std())
    hit = float((valid > 0).mean() * 100)  # for short returns, > 0 = profitable
    sharpe = annualized_sharpe(valid, periods_per_year=365.0 / max(1, horizon))
    mdd = max_drawdown(valid)
    avg_hold = float(df["holding_days"].mean()) if "holding_days" in df else None
    calmar = (mean / abs(mdd)) if mdd != 0 else float("nan")
    return {
        "strategy": label, "horizon_d": horizon, "n": n,
        "mean_pct": round(mean, 2),
        "std_pct": round(std, 2),
        "sharpe_ann": round(sharpe, 3) if not math.isnan(sharpe) else None,
        "hit_rate_pct": round(hit, 1),
        "max_dd_pct": round(mdd, 2),
        "calmar": round(calmar, 3) if not math.isnan(calmar) else None,
        "avg_hold_days": round(avg_hold, 1) if avg_hold else None,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    if not FIRES_PATH.exists():
        print(f"ERROR: {FIRES_PATH} not found. Run backtest_conditional.py first.", file=sys.stderr)
        return 1
    fires = pd.read_csv(FIRES_PATH)
    fires["week_end"] = pd.to_datetime(fires["week_end"])
    fires = fires[fires["week_end"] >= pd.Timestamp(INCEPTION)].copy()
    print(f"loaded {len(fires):,} fire-weeks since {INCEPTION}  ({fires['ticker'].nunique()} firms)")

    # Re-build z-panel for re-entry strategy
    from backtest_conditional import build_weekly_z_panel
    import duckdb
    WAREHOUSE = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    panel = build_weekly_z_panel(con)
    con.close()

    rows = []
    for horizon in HORIZONS:
        for _, fire in fires.iterrows():
            tk = fire["ticker"]
            entry = fire["week_end"].date()
            arch = fire["archetype"]
            prices = get_prices(tk, entry, entry + timedelta(days=horizon + 30))
            if prices.empty:
                continue
            a = simulate_raw(prices, entry, horizon)
            b = simulate_stops(prices, entry, horizon, arch)
            c = simulate_stops_with_reentry(prices, entry, horizon, arch, panel, tk)
            rows.append({"ticker": tk, "entry": entry.isoformat(), "horizon_d": horizon, "archetype": arch,
                         "strategy": "A_raw", **a})
            rows.append({"ticker": tk, "entry": entry.isoformat(), "horizon_d": horizon, "archetype": arch,
                         "strategy": "B_stops", **b})
            rows.append({"ticker": tk, "entry": entry.isoformat(), "horizon_d": horizon, "archetype": arch,
                         "strategy": "C_stops_re", **c})

    trades_df = pd.DataFrame(rows)
    trades_df.to_csv(OUT_DIR / "strategy_compare.csv", index=False)

    # Aggregate
    summary_rows = []
    for strategy in ["A_raw", "B_stops", "C_stops_re"]:
        for horizon in HORIZONS:
            sub = trades_df[(trades_df["strategy"] == strategy) & (trades_df["horizon_d"] == horizon)]
            stats = aggregate_stats(strategy, sub, horizon)
            summary_rows.append(stats)
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(OUT_DIR / "strategy_summary.csv", index=False)

    # Display
    print()
    print("=" * 110)
    print(f"  STRATEGY COMPARISON  (window: {INCEPTION} -> today, BSI shorts, {len(fires):,} fire-weeks per horizon)")
    print("=" * 110)
    label_map = {"A_raw": "A. Raw (no stops)", "B_stops": "B. With stops", "C_stops_re": "C. Stops + re-entry"}
    print(f"{'STRATEGY':<22} {'HORIZON':>8} {'N':>5} {'MEAN':>8} {'STD':>8} {'SHARPE':>8} {'HIT%':>7} "
          f"{'MAX DD':>9} {'CALMAR':>8} {'AVG HOLD':>9}")
    print("-" * 110)
    for r in summary_rows:
        lbl = label_map.get(r["strategy"], r["strategy"])
        n = r.get("n", 0)
        if n == 0:
            print(f"  {lbl:<20} {r['horizon_d']:>7}d {0:>5} (no data)")
            continue
        print(f"  {lbl:<20} {r['horizon_d']:>7}d {n:>5} "
              f"{r['mean_pct']:>+7.2f}% {r['std_pct']:>7.2f}% "
              f"{r['sharpe_ann'] if r['sharpe_ann'] is not None else 0:>+8.2f} "
              f"{r['hit_rate_pct']:>6.1f}% {r['max_dd_pct']:>+8.2f}% "
              f"{r['calmar'] if r['calmar'] is not None else 0:>+7.2f} {r['avg_hold_days'] or 0:>7.1f}d")
    print("=" * 110)

    # Markdown summary
    md = OUT_DIR / "strategy_summary.md"
    with md.open("w", encoding="utf-8") as f:
        f.write(f"# Strategy Comparison\n\n")
        f.write(f"Window: {INCEPTION} to today\n\n")
        f.write(f"| Strategy | Horizon | N | Mean | Std | Sharpe (ann) | Hit% | Max DD | Calmar |\n")
        f.write(f"|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            if r.get("n", 0) == 0:
                continue
            f.write(f"| {label_map.get(r['strategy'], r['strategy'])} | {r['horizon_d']}d | {r['n']} | "
                    f"{r['mean_pct']:+.2f}% | {r['std_pct']:.2f}% | "
                    f"{r['sharpe_ann'] if r['sharpe_ann'] is not None else '-'} | "
                    f"{r['hit_rate_pct']}% | {r['max_dd_pct']:+.2f}% | "
                    f"{r['calmar'] if r['calmar'] is not None else '-'} |\n")
    print(f"\nwrote: {OUT_DIR / 'strategy_compare.csv'}  ({len(trades_df):,} per-trade rows)")
    print(f"wrote: {OUT_DIR / 'strategy_summary.csv'}  ({len(summary_df)} rows)")
    print(f"wrote: {md}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
