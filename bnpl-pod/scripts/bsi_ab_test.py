"""
BSI A/B test -- correlate per-firm z-score with forward returns since testing began.
====================================================================================

For each firm in the tested universe, two complementary analyses:

  (1) Correlation   -- Pearson(z, forward_return) at each horizon. Negative
                       correlation = BSI predicts decline (good for short thesis).
                       Reports r, p-value, n.

  (2) A/B groups    -- Group A: weeks where z >= threshold (the BSI "fired").
                       Group B: weeks where z <  threshold (the BSI "did not fire").
                       Compares mean forward returns, t-stat, effect size (Cohen's d).

Window: defaults to inception (2025-04-15) -> today; can be overridden.

Outputs (under bnpl-pod/backtest/outputs/ab_test/):
  ab_per_firm_correlation.csv
  ab_per_firm_groups.csv
  ab_summary.csv
  ab_summary.md
"""
from __future__ import annotations

import argparse
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

from refresh_bsi_snapshot import UNIVERSE_KEYWORDS, cfpb_delta_to_z
from backtest_conditional import build_weekly_z_panel, get_prices, forward_return

WAREHOUSE = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"
OUT_DIR = Path(__file__).resolve().parents[1] / "backtest" / "outputs" / "ab_test"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INCEPTION_DEFAULT = date(2025, 4, 15)
HORIZONS = [30, 90, 365]
ARCHETYPES = {"BLITZ": 1.5, "SCOUT": 2.0, "GUARDIAN": 2.5}


# ---------------------------------------------------------------------------
# Pure stats (no scipy dependency for the simple cases)
# ---------------------------------------------------------------------------

def pearson_r(x: np.ndarray, y: np.ndarray) -> tuple[float, float, int]:
    """Returns (r, two-sided p-value, n) using t-approximation."""
    mask = (~np.isnan(x)) & (~np.isnan(y))
    x = x[mask]; y = y[mask]
    n = len(x)
    if n < 5:
        return (float("nan"), float("nan"), n)
    r = float(np.corrcoef(x, y)[0, 1])
    if not np.isfinite(r) or abs(r) >= 1.0:
        return (r, 0.0 if abs(r) >= 1 else float("nan"), n)
    # t = r * sqrt((n-2) / (1 - r^2))
    t = r * np.sqrt((n - 2) / max(1e-12, 1 - r * r))
    # two-sided p-value via student's t survival; use scipy if available, else norm approx
    try:
        from scipy.stats import t as student_t
        p = 2 * (1 - student_t.cdf(abs(t), df=n - 2))
    except Exception:
        # crude normal approx for large n
        from math import erf, sqrt
        p = 2 * (1 - 0.5 * (1 + erf(abs(t) / sqrt(2))))
    return (r, float(p), n)


def welch_ttest(a: np.ndarray, b: np.ndarray) -> tuple[float, float, float, int, int]:
    """Two-sample Welch t-test. Returns (mean_diff, t_stat, two-sided p, n_a, n_b)."""
    a = a[~np.isnan(a)]; b = b[~np.isnan(b)]
    n_a, n_b = len(a), len(b)
    if n_a < 5 or n_b < 5:
        return (float("nan"), float("nan"), float("nan"), n_a, n_b)
    ma, mb = float(a.mean()), float(b.mean())
    va, vb = float(a.var(ddof=1)), float(b.var(ddof=1))
    se = np.sqrt(va / n_a + vb / n_b) if (va > 0 or vb > 0) else 0.0
    if se == 0:
        return (ma - mb, float("nan"), float("nan"), n_a, n_b)
    t = (ma - mb) / se
    df = (va / n_a + vb / n_b) ** 2 / (
        (va / n_a) ** 2 / max(1, n_a - 1) + (vb / n_b) ** 2 / max(1, n_b - 1)
    )
    try:
        from scipy.stats import t as student_t
        p = 2 * (1 - student_t.cdf(abs(t), df=df))
    except Exception:
        from math import erf, sqrt
        p = 2 * (1 - 0.5 * (1 + erf(abs(t) / sqrt(2))))
    return (ma - mb, float(t), float(p), n_a, n_b)


def cohens_d(a: np.ndarray, b: np.ndarray) -> float:
    a = a[~np.isnan(a)]; b = b[~np.isnan(b)]
    if len(a) < 2 or len(b) < 2:
        return float("nan")
    s_pool = np.sqrt(
        ((len(a) - 1) * a.var(ddof=1) + (len(b) - 1) * b.var(ddof=1))
        / max(1, (len(a) + len(b) - 2))
    )
    if s_pool == 0:
        return float("nan")
    return float((a.mean() - b.mean()) / s_pool)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--inception", default=INCEPTION_DEFAULT.isoformat(),
                   help="window start; default 2025-04-15")
    p.add_argument("--threshold", type=float, default=ARCHETYPES["BLITZ"],
                   help=f"BSI z threshold for A/B split; default {ARCHETYPES['BLITZ']} (BLITZ)")
    args = p.parse_args(argv)

    inception = date.fromisoformat(args.inception)

    if not WAREHOUSE.exists():
        print(f"ERROR: warehouse not found at {WAREHOUSE}", file=sys.stderr)
        return 1

    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    print(f"== A/B test ==  inception: {inception}  threshold z>=:{args.threshold}")
    panel = build_weekly_z_panel(con)
    con.close()
    if panel.empty:
        print("no tickers survived data check", file=sys.stderr)
        return 1

    panel["week_end"] = pd.to_datetime(panel["week_end"])
    panel = panel[panel["week_end"] >= pd.Timestamp(inception)].copy()
    surviving = sorted(panel["ticker"].unique())
    print(f"firms in window: {len(surviving)}  weekly z-rows: {len(panel):,}")

    corr_rows = []
    group_rows = []
    all_fired_returns = {h: [] for h in HORIZONS}
    all_quiet_returns = {h: [] for h in HORIZONS}

    print()
    print(f"{'TICKER':<7} {'N WK':>5} {'r_30d':>8} {'p':>7} {'r_90d':>8} {'p':>7} "
          f"{'r_365d':>8} {'p':>7}  {'fired/quiet 30d':<22}")
    print("-" * 105)

    for ticker in surviving:
        sub = panel[panel["ticker"] == ticker].sort_values("week_end")
        if len(sub) < 10:
            continue
        prices = get_prices(ticker, inception, date.today())
        if prices.empty:
            continue

        # forward returns at each horizon for every weekly anchor
        rows = []
        for _, w in sub.iterrows():
            z = float(w["z_score"])
            anchor = w["week_end"].date()
            r30 = forward_return(prices, anchor, 30)
            r90 = forward_return(prices, anchor, 90)
            r365 = forward_return(prices, anchor, 365)
            rows.append((z, r30, r90, r365))
        df = pd.DataFrame(rows, columns=["z", "r30", "r90", "r365"])

        # (1) per-firm correlation
        cr = {}
        for h, col in [(30, "r30"), (90, "r90"), (365, "r365")]:
            r, p, n = pearson_r(df["z"].values, df[col].values)
            cr[h] = (r, p, n)
        corr_rows.append({
            "ticker": ticker,
            "n_weeks": int(len(df)),
            "r_30d":  round(cr[30][0], 3) if np.isfinite(cr[30][0]) else None,
            "p_30d":  round(cr[30][1], 4) if np.isfinite(cr[30][1]) else None,
            "r_90d":  round(cr[90][0], 3) if np.isfinite(cr[90][0]) else None,
            "p_90d":  round(cr[90][1], 4) if np.isfinite(cr[90][1]) else None,
            "r_365d": round(cr[365][0], 3) if np.isfinite(cr[365][0]) else None,
            "p_365d": round(cr[365][1], 4) if np.isfinite(cr[365][1]) else None,
        })

        # (2) A/B split at threshold
        fired = df[df["z"] >= args.threshold]
        quiet = df[df["z"] <  args.threshold]
        per_horizon = {}
        for h, col in [(30, "r30"), (90, "r90"), (365, "r365")]:
            md, t, p, n_a, n_b = welch_ttest(fired[col].values, quiet[col].values)
            d = cohens_d(fired[col].values, quiet[col].values)
            per_horizon[h] = (md, t, p, n_a, n_b, d)
            # accumulate for pooled report
            all_fired_returns[h].extend([x for x in fired[col].dropna().tolist()])
            all_quiet_returns[h].extend([x for x in quiet[col].dropna().tolist()])
        group_rows.append({
            "ticker": ticker,
            "n_fired_weeks": int(len(fired)),
            "n_quiet_weeks": int(len(quiet)),
            "mean_diff_30d":  round(per_horizon[30][0], 2) if np.isfinite(per_horizon[30][0]) else None,
            "t_30d":          round(per_horizon[30][1], 2) if np.isfinite(per_horizon[30][1]) else None,
            "p_30d":          round(per_horizon[30][2], 4) if np.isfinite(per_horizon[30][2]) else None,
            "d_30d":          round(per_horizon[30][5], 2) if np.isfinite(per_horizon[30][5]) else None,
            "mean_diff_90d":  round(per_horizon[90][0], 2) if np.isfinite(per_horizon[90][0]) else None,
            "t_90d":          round(per_horizon[90][1], 2) if np.isfinite(per_horizon[90][1]) else None,
            "p_90d":          round(per_horizon[90][2], 4) if np.isfinite(per_horizon[90][2]) else None,
            "mean_diff_365d": round(per_horizon[365][0], 2) if np.isfinite(per_horizon[365][0]) else None,
            "t_365d":         round(per_horizon[365][1], 2) if np.isfinite(per_horizon[365][1]) else None,
            "p_365d":         round(per_horizon[365][2], 4) if np.isfinite(per_horizon[365][2]) else None,
        })

        # display row
        fmt = lambda x: f"{x:>+8.3f}" if x is not None and np.isfinite(x) else f"{'-':>8}"
        fp  = lambda x: f"{x:>7.3f}" if x is not None and np.isfinite(x) else f"{'-':>7}"
        c30, c90, c365 = cr[30], cr[90], cr[365]
        ab_30 = f"{len(fired):>3}f/{len(quiet):>3}q  d={per_horizon[30][5]:+.2f}" \
                if np.isfinite(per_horizon[30][5]) else f"{len(fired):>3}f/{len(quiet):>3}q"
        print(f"{ticker:<7} {len(df):>5d} {fmt(c30[0])} {fp(c30[1])} "
              f"{fmt(c90[0])} {fp(c90[1])} {fmt(c365[0])} {fp(c365[1])}  {ab_30:<22}")

    # ------- pooled A/B summary across all firms -------
    print()
    print("=" * 86)
    print(f"  POOLED A/B TEST   threshold z >= {args.threshold}   inception {inception}")
    print("=" * 86)
    summary_rows = []
    for h in HORIZONS:
        a = np.array(all_fired_returns[h], dtype=float)
        b = np.array(all_quiet_returns[h], dtype=float)
        md, t, p, n_a, n_b = welch_ttest(a, b)
        d = cohens_d(a, b)
        sig = "***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < 0.05 else ""))
        mean_a = float(np.nanmean(a)) if len(a) else float("nan")
        mean_b = float(np.nanmean(b)) if len(b) else float("nan")
        summary_rows.append({
            "horizon_d": h,
            "n_fired": int(n_a), "n_quiet": int(n_b),
            "mean_fired_pct": round(mean_a, 2), "mean_quiet_pct": round(mean_b, 2),
            "mean_diff_pct": round(md, 2),
            "t_stat": round(t, 2) if np.isfinite(t) else None,
            "p_value": round(p, 4) if np.isfinite(p) else None,
            "cohens_d": round(d, 3) if np.isfinite(d) else None,
            "sig": sig,
        })
        print(f"  Horizon {h:>3}d:  mean(fired) = {mean_a:+.2f}%  vs  "
              f"mean(quiet) = {mean_b:+.2f}%   delta = {md:+.2f}%   "
              f"t={t:.2f}  p={p:.4f} {sig}   d={d:+.2f}   "
              f"N(A,B)=({n_a:,}, {n_b:,})")
    print("=" * 86)

    # -------- write outputs --------
    pd.DataFrame(corr_rows).to_csv(OUT_DIR / "ab_per_firm_correlation.csv", index=False)
    pd.DataFrame(group_rows).to_csv(OUT_DIR / "ab_per_firm_groups.csv", index=False)
    pd.DataFrame(summary_rows).to_csv(OUT_DIR / "ab_summary.csv", index=False)

    # markdown report -- ready to drop into the PDF builder or the paper
    md = OUT_DIR / "ab_summary.md"
    with md.open("w", encoding="utf-8") as f:
        f.write(f"# BSI A/B Test Summary\n\n")
        f.write(f"- Inception: **{inception}**\n")
        f.write(f"- Threshold: BSI z >= **{args.threshold}**  (BLITZ archetype)\n")
        f.write(f"- Universe: {len(surviving)} firms with viable CFPB coverage\n")
        f.write(f"- Total weekly observations: {len(panel):,}\n\n")
        f.write(f"## Pooled A/B (forward returns, fired vs quiet)\n\n")
        f.write(f"| Horizon | Mean(fired) | Mean(quiet) | Diff | t | p | Cohen's d | Sig |\n")
        f.write(f"|---|---:|---:|---:|---:|---:|---:|---|\n")
        for r in summary_rows:
            f.write(f"| {r['horizon_d']}d | {r['mean_fired_pct']:+.2f}% | "
                    f"{r['mean_quiet_pct']:+.2f}% | {r['mean_diff_pct']:+.2f}% | "
                    f"{r['t_stat']} | {r['p_value']} | {r['cohens_d']} | {r['sig']} |\n")
        f.write(f"\n*** p<0.001, ** p<0.01, * p<0.05\n\n")
        f.write(f"## Reading\n\n")
        # honest interpretation
        signs = [r["mean_diff_pct"] for r in summary_rows if r["mean_diff_pct"] is not None]
        if signs and all(s < 0 for s in signs):
            interp = ("BSI-fired weeks predict **lower** forward returns than quiet weeks at "
                      "every horizon. Direction is consistent with the short thesis.")
        elif signs and all(s > 0 for s in signs):
            interp = ("BSI-fired weeks predict **higher** forward returns than quiet weeks at "
                      "every horizon. Direction is opposite to the short thesis -- consistent "
                      "with momentum-crash literature (Daniel & Moskowitz 2016) and the paper's "
                      "alpha ~ 0 disclosure.")
        else:
            interp = ("Direction is mixed across horizons; see per-horizon t-stats above.")
        f.write(interp + "\n")
    print(f"\nwrote: {md}")
    print(f"wrote: ab_per_firm_correlation.csv ({len(corr_rows)} rows)")
    print(f"wrote: ab_per_firm_groups.csv      ({len(group_rows)} rows)")
    print(f"wrote: ab_summary.csv              ({len(summary_rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
