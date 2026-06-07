"""
Conditional backtest -- BSI fire-week forward returns at scale.
================================================================

For each firm in UNIVERSE_KEYWORDS (filtered to firms with viable CFPB
coverage), compute the weekly z-score time series 2018-present, identify
every fire-week at BLITZ/SCOUT/GUARDIAN thresholds, and measure forward
returns at 30d/90d/365d horizons.

Outputs:
  outputs/conditional_fires.csv       Per-fire-week row (ticker, week, archetype, z, fwd30, fwd90, fwd365)
  outputs/conditional_summary.csv     Per-archetype-per-horizon aggregate (n, hit_rate, avg_fwd_ret)
  outputs/confusion_matrix.csv        Per-firm TP/FP/TN/FN classification

Statistical posture: this is the empirical bar-raiser. Previous analysis was
12 firms × 18 months = 341 fire-weeks (Wilson CI on precision [4%, 64%], vacuous).
This extension: 21 firms × 8 years -> ~5,000+ fire-weeks, CI tightens meaningfully.

Run:  python backtest_conditional.py
"""
from __future__ import annotations

import csv
import sys
import warnings
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# Reuse the universe + mapping helpers
from refresh_bsi_snapshot import (
    UNIVERSE_KEYWORDS,
    cfpb_delta_to_z,
    SETTLE_LAG_DAYS,
)

WAREHOUSE = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"
OUT_DIR = Path(__file__).resolve().parents[1] / "backtest" / "outputs" / "conditional"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Thresholds
ARCHETYPES = {"BLITZ": 1.5, "SCOUT": 2.0, "GUARDIAN": 2.5}
HORIZONS = [30, 90, 365]
HIT_THRESHOLD_PCT = -5.0           # "hit" = forward return <= -5% (short thesis succeeds)
WINDOW_START = date(2018, 1, 1)
MIN_COMPLAINTS_FOR_TRACKING = 50  # firms below this are dropped


# ---------------------------------------------------------------------------
# Build per-firm weekly z-score panel from CFPB warehouse
# ---------------------------------------------------------------------------

def build_weekly_z_panel(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Returns long DataFrame: (ticker, week_end, z_score)."""
    # 1. Pull all CFPB complaints with ticker tagging
    ticker_company = []
    for ticker, keywords in UNIVERSE_KEYWORDS.items():
        for kw in keywords:
            ticker_company.append((ticker, kw.upper()))
    df_map = pd.DataFrame(ticker_company, columns=["ticker", "keyword"])

    # 2. Get complaint counts per (company, week) for the whole warehouse
    cfpb = con.execute("""
        SELECT
            UPPER(company) AS company,
            DATE_TRUNC('week', received_at)::DATE AS week_end,
            COUNT(*) AS n
        FROM cfpb_complaints
        WHERE received_at >= '2017-01-01'
        GROUP BY UPPER(company), DATE_TRUNC('week', received_at)
        ORDER BY company, week_end
    """).fetchdf()

    # 3. Tag rows with ticker via keyword substring match
    per_ticker_weekly: dict[str, pd.DataFrame] = {}
    for ticker, group in df_map.groupby("ticker"):
        keywords = group["keyword"].tolist()
        mask = cfpb["company"].apply(
            lambda c: any(kw in c for kw in keywords)
        )
        sub = cfpb[mask][["week_end", "n"]].groupby("week_end", as_index=False).sum()
        if sub.empty or sub["n"].sum() < MIN_COMPLAINTS_FOR_TRACKING:
            continue
        sub["ticker"] = ticker
        per_ticker_weekly[ticker] = sub

    if not per_ticker_weekly:
        return pd.DataFrame(columns=["ticker", "week_end", "z_score"])

    out_rows = []
    for ticker, df in per_ticker_weekly.items():
        # Build daily rolling 90d windows -> weekly z via the same delta% logic as the live pod
        df = df.copy().sort_values("week_end").reset_index(drop=True)
        df["week_end"] = pd.to_datetime(df["week_end"])
        # Reindex to weekly continuous (fill missing weeks with 0)
        full_idx = pd.date_range(df["week_end"].min(), df["week_end"].max(), freq="W-MON")
        df = df.set_index("week_end").reindex(full_idx, fill_value=0).rename_axis("week_end").reset_index()
        df["n_recent_13w"] = df["n"].rolling(13, min_periods=1).sum().fillna(0)
        df["n_prior_13w"] = df["n"].shift(13).rolling(13, min_periods=1).sum().fillna(0)
        df["delta_pct"] = np.where(
            df["n_prior_13w"] > 0,
            100.0 * (df["n_recent_13w"] - df["n_prior_13w"]) / df["n_prior_13w"],
            np.where(df["n_recent_13w"] > 0, 100.0, 0.0),
        )
        df["z_score"] = df["delta_pct"].apply(cfpb_delta_to_z)
        for _, row in df.iterrows():
            out_rows.append((ticker, row["week_end"].date(), float(row["z_score"]),
                             int(row["n_recent_13w"]), int(row["n_prior_13w"])))
    return pd.DataFrame(out_rows, columns=["ticker", "week_end", "z_score",
                                            "n_recent_13w", "n_prior_13w"])


# ---------------------------------------------------------------------------
# Price history per ticker (yfinance) -- cached on disk for re-runs
# ---------------------------------------------------------------------------

_PX_CACHE_DIR = OUT_DIR.parent / "px_cache"
_PX_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_prices(ticker: str, start: date, end: date) -> pd.DataFrame:
    """Returns Close-price daily DataFrame indexed by date, or empty df."""
    cache_path = _PX_CACHE_DIR / f"{ticker}.csv"
    if cache_path.exists():
        df = pd.read_csv(cache_path, parse_dates=["Date"]).set_index("Date")
        if not df.empty and df.index.min().date() <= start and df.index.max().date() >= end - timedelta(days=10):
            return df
    try:
        df = yf.Ticker(ticker).history(start=start.isoformat(), end=end.isoformat(),
                                        auto_adjust=True)
        if df.empty:
            return pd.DataFrame()
        df = df[["Close"]].copy()
        df.index = df.index.tz_localize(None) if df.index.tz is not None else df.index
        df.to_csv(cache_path)
        return df
    except Exception as e:
        print(f"  yfinance fetch failed for {ticker}: {e}", file=sys.stderr)
        return pd.DataFrame()


def forward_return(prices: pd.DataFrame, anchor: date, horizon_days: int) -> float | None:
    """Return percent from anchor close to anchor+H close, or None if data missing."""
    anchor_ts = pd.Timestamp(anchor)
    target_ts = anchor_ts + pd.Timedelta(days=horizon_days)
    if prices.empty:
        return None
    # find closest price on/after anchor
    after_anchor = prices.index[prices.index >= anchor_ts]
    if len(after_anchor) == 0:
        return None
    p_anchor = prices.loc[after_anchor[0], "Close"]
    # find closest price on/before target (allow up to 5 trading days slack)
    on_or_before_target = prices.index[prices.index <= target_ts]
    if len(on_or_before_target) == 0:
        return None
    p_target = prices.loc[on_or_before_target[-1], "Close"]
    # ensure the gap is roughly the horizon
    if (on_or_before_target[-1] - after_anchor[0]).days < horizon_days * 0.6:
        return None
    return 100.0 * (p_target - p_anchor) / p_anchor


# ---------------------------------------------------------------------------
# Main backtest
# ---------------------------------------------------------------------------

def main() -> int:
    if not WAREHOUSE.exists():
        print(f"ERROR: warehouse not found at {WAREHOUSE}", file=sys.stderr)
        return 1

    print(f"== conditional backtest ==  window: {WINDOW_START} -> today  universe: {len(UNIVERSE_KEYWORDS)} candidates")
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    panel = build_weekly_z_panel(con)
    con.close()

    if panel.empty:
        print("no tickers survived the data check.", file=sys.stderr)
        return 1

    surviving = sorted(panel["ticker"].unique())
    print(f"surviving tickers ({len(surviving)}): {' '.join(surviving)}")
    print(f"weekly z-score rows: {len(panel):,}")

    # Identify all fire-weeks per archetype
    fire_rows = []
    confusion_rows = []
    print()
    print(f"{'TICKER':<7} {'WEEKS':>6} {'MAX_Z':>6} {'BLITZ':>6} {'SCOUT':>6} {'GUARD':>6} "
          f"{'WORST 30D':>10} {'WORST 90D':>10} {'WORST 365D':>11}")
    print("-" * 90)

    for ticker in surviving:
        sub = panel[panel["ticker"] == ticker].sort_values("week_end")
        sub = sub[sub["week_end"] >= WINDOW_START]
        if sub.empty:
            continue

        # Pull prices once for this ticker, covering the entire backtest window
        prices = get_prices(ticker, WINDOW_START, date.today())
        if prices.empty:
            print(f"{ticker:<7} (no price data)")
            continue

        # Fire-week extraction
        n_blitz = n_scout = n_guard = 0
        for _, row in sub.iterrows():
            z = row["z_score"]
            wk = row["week_end"]
            archetypes_fired = [name for name, thr in ARCHETYPES.items() if z >= thr]
            if not archetypes_fired:
                continue
            fwd_returns = {h: forward_return(prices, wk, h) for h in HORIZONS}
            for arch in archetypes_fired:
                if arch == "BLITZ": n_blitz += 1
                elif arch == "SCOUT": n_scout += 1
                elif arch == "GUARDIAN": n_guard += 1
                fire_rows.append({
                    "ticker": ticker, "week_end": wk.isoformat(), "archetype": arch,
                    "z_score": round(z, 3),
                    "fwd_30d_pct": round(fwd_returns[30], 2) if fwd_returns[30] is not None else None,
                    "fwd_90d_pct": round(fwd_returns[90], 2) if fwd_returns[90] is not None else None,
                    "fwd_365d_pct": round(fwd_returns[365], 2) if fwd_returns[365] is not None else None,
                })

        # Confusion matrix at FIRM-YEAR granularity (proper unit; 21 firms x 8 yrs ~ 168 cells)
        max_z = float(sub["z_score"].max())
        sub_dt = sub.copy()
        sub_dt["week_end"] = pd.to_datetime(sub_dt["week_end"])
        sub_dt["year"] = sub_dt["week_end"].dt.year
        first_year = max(WINDOW_START.year, int(sub_dt["year"].min()))
        last_year = min(date.today().year - 1, int(sub_dt["year"].max()))
        worst_30 = worst_90 = worst_365 = None
        for yr in range(first_year, last_year + 1):
            yr_rows = sub_dt[sub_dt["year"] == yr]
            yr_max_z = float(yr_rows["z_score"].max()) if len(yr_rows) else 0.0
            fired_this_yr = yr_max_z >= ARCHETYPES["BLITZ"]
            # measure forward 365d return at each weekly anchor IN year yr
            worst_yr_365 = None
            for _, wk in yr_rows.iterrows():
                r = forward_return(prices, wk["week_end"].date(), 365)
                if r is None:
                    continue
                if worst_yr_365 is None or r < worst_yr_365:
                    worst_yr_365 = r
            if worst_yr_365 is None:
                continue
            collapsed = worst_yr_365 <= -25.0  # 25%+ drawdown in next 12 months
            if fired_this_yr and collapsed:
                cls = "TP"
            elif fired_this_yr and not collapsed:
                cls = "FP"
            elif not fired_this_yr and collapsed:
                cls = "FN"
            else:
                cls = "TN"
            confusion_rows.append({
                "ticker": ticker, "year": yr, "class": cls,
                "max_z_in_year": round(yr_max_z, 2),
                "worst_fwd_365d": round(worst_yr_365, 1),
                "fired": fired_this_yr,
                "collapsed": collapsed,
            })
            # Track all-time worsts for display
            if worst_365 is None or worst_yr_365 < worst_365:
                worst_365 = worst_yr_365

        # Display row
        wr30 = wr90 = "-"
        try:
            anchors_30 = [forward_return(prices, w["week_end"].date(), 30)
                          for _, w in sub_dt.iterrows()]
            anchors_90 = [forward_return(prices, w["week_end"].date(), 90)
                          for _, w in sub_dt.iterrows()]
            a30 = [x for x in anchors_30 if x is not None]
            a90 = [x for x in anchors_90 if x is not None]
            if a30: worst_30 = min(a30); wr30 = f"{worst_30:.1f}%"
            if a90: worst_90 = min(a90); wr90 = f"{worst_90:.1f}%"
        except Exception:
            pass
        wr365 = f"{worst_365:.1f}%" if worst_365 is not None else "-"
        print(f"{ticker:<7} {len(sub):>6d} {max_z:>6.2f} {n_blitz:>6d} {n_scout:>6d} {n_guard:>6d} "
              f"{wr30:>10} {wr90:>10} {wr365:>11}")

    # ------- aggregate summary -------
    fire_df = pd.DataFrame(fire_rows)
    fire_df.to_csv(OUT_DIR / "conditional_fires.csv", index=False)

    summary_rows = []
    for arch in ARCHETYPES:
        for h in HORIZONS:
            sub = fire_df[fire_df["archetype"] == arch]
            col = f"fwd_{h}d_pct"
            valid = sub[sub[col].notna()]
            n = len(valid)
            if n == 0:
                summary_rows.append({"archetype": arch, "horizon_d": h, "n": 0,
                                     "hit_rate_pct": None, "avg_fwd_ret_pct": None,
                                     "median_fwd_ret_pct": None})
                continue
            hits = (valid[col] <= HIT_THRESHOLD_PCT).sum()
            summary_rows.append({
                "archetype": arch, "horizon_d": h, "n": n,
                "hit_rate_pct": round(100.0 * hits / n, 1),
                "avg_fwd_ret_pct": round(valid[col].mean(), 2),
                "median_fwd_ret_pct": round(valid[col].median(), 2),
            })
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(OUT_DIR / "conditional_summary.csv", index=False)

    confusion_df = pd.DataFrame(confusion_rows)
    confusion_df.to_csv(OUT_DIR / "confusion_matrix.csv", index=False)

    # ------- Wilson CI on confusion-matrix metrics -------
    def wilson_ci(k: int, n: int, conf: float = 0.95) -> tuple[float, float]:
        if n == 0:
            return (0.0, 0.0)
        from scipy.stats import norm
        z = norm.ppf(1 - (1 - conf) / 2)
        p = k / n
        denom = 1 + z**2 / n
        center = (p + z**2 / (2*n)) / denom
        half = z * ((p*(1-p)/n + z**2/(4*n**2))**0.5) / denom
        return (max(0.0, center - half), min(1.0, center + half))

    tp = (confusion_df["class"] == "TP").sum()
    fp = (confusion_df["class"] == "FP").sum()
    fn = (confusion_df["class"] == "FN").sum()
    tn = (confusion_df["class"] == "TN").sum()
    n = tp + fp + fn + tn
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0
    specificity = tn / (tn + fp) if (tn + fp) else 0
    accuracy = (tp + tn) / n if n else 0

    print()
    print("=" * 78)
    print(f"  SUMMARY: {n} firms tracked, {len(fire_df):,} fire-weeks across {len(surviving)} tickers")
    print("=" * 78)
    print(f"  TP / FP / FN / TN  =  {tp} / {fp} / {fn} / {tn}")
    p_lo, p_hi = wilson_ci(tp, tp + fp)
    r_lo, r_hi = wilson_ci(tp, tp + fn)
    s_lo, s_hi = wilson_ci(tn, tn + fp)
    print(f"  Precision = {100*precision:.0f}%   95% Wilson CI [{100*p_lo:.0f}%, {100*p_hi:.0f}%]")
    print(f"  Recall    = {100*recall:.0f}%   95% Wilson CI [{100*r_lo:.0f}%, {100*r_hi:.0f}%]")
    print(f"  Specificity= {100*specificity:.0f}%   95% Wilson CI [{100*s_lo:.0f}%, {100*s_hi:.0f}%]")
    print(f"  Accuracy  = {100*accuracy:.0f}%")
    print()
    print("=" * 78)
    print(f"  CONDITIONAL HIT RATES (BSI-fire-week -> forward returns)")
    print("=" * 78)
    print(f"  {'ARCH':<10} {'HORIZON':>8} {'N':>6} {'HIT %':>7} {'AVG RET':>9} {'MED RET':>9}")
    print("-" * 78)
    for r in summary_rows:
        hit = f"{r['hit_rate_pct']}%" if r['hit_rate_pct'] is not None else "-"
        avg = f"{r['avg_fwd_ret_pct']:+.1f}%" if r['avg_fwd_ret_pct'] is not None else "-"
        med = f"{r['median_fwd_ret_pct']:+.1f}%" if r['median_fwd_ret_pct'] is not None else "-"
        print(f"  {r['archetype']:<10} {r['horizon_d']:>7}d {r['n']:>6} {hit:>7} {avg:>9} {med:>9}")
    print("=" * 78)
    print()
    print(f"outputs written to: {OUT_DIR}")
    print(f"  conditional_fires.csv  ({len(fire_df):,} rows)")
    print(f"  conditional_summary.csv  ({len(summary_df):,} rows)")
    print(f"  confusion_matrix.csv  ({len(confusion_df):,} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
