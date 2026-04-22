"""
Rolling Granger causality — BSI → trustee roll rate (or credit proxy).

This is the empirical centerpiece of the paper (MASTERPLAN v4.1 §7 #4).
We test whether lagged BSI values help predict a credit-stress target
beyond its own autoregressive history, at lags 4–8 weeks.

Target hierarchy (Sprint-H.d)
-----------------------------
The paper's original formulation was BSI → AFRMMT 60+ DPD roll rate, but
AFRMMT (Affirm's ABS trust) is a **144A private placement** — its trustee
reports never hit EDGAR. So we evaluate three targets in order and report
on the highest-tier one that has enough aligned observations:

  Tier 1.  **AFFIRM trustee roll_rate_60p** — the thesis target. Populates
           only if a private-data license is added to the pipeline.
  Tier 2.  **Subprime-auto composite roll_rate_60p** — the paper's public
           lookalike proxy (SDART / AMCAR / EART). Roll rates averaged
           across the three families per period. This is the real working
           target: it is the same product class (subprime consumer credit),
           filed publicly on EDGAR, and the paper frames BNPL as "Subprime
           2.0" precisely because the distress dynamics match.
  Tier 3.  **HYG credit-ETF negative log-return** — market-level credit
           stress proxy; used only if Tiers 1–2 are both starved. This is
           a weaker test (picks up all high-yield stress, not BNPL-specific)
           but keeps the pipeline productive during early ingest.

`target_label` in the returned results records which tier actually fired so
the paper's table can cite it unambiguously.

Design
------
- Pull BSI_t (weekly-resampled) and roll_rate_60p (monthly, forward-filled
  to weekly) from DuckDB.
- Align on weekly observation frequency.
- Run statsmodels.tsa.stattools.grangercausalitytests at each candidate lag.
- Report F-test p-value per lag; optionally run on rolling windows.

Outputs: a list of GrangerResult (lag_weeks, p_value, F, n) plus, if
persist=True, a write-back into the `granger_results` helper table
(auto-created on first run).

Run with:  python -m signals.granger
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable

import duckdb

from data.settings import settings

log = logging.getLogger(__name__)

DEFAULT_LAGS = (4, 5, 6, 7, 8)   # weeks


@dataclass
class GrangerResult:
    lag_weeks: int
    p_value: float
    f_stat: float
    n: int
    # Provenance of which tier in the three-tier target ladder fired for
    # this run (see run_granger docstring). Defaults keep backward compat
    # with callers / fixtures that construct GrangerResult by hand.
    tier: int = 1
    target_label: str = "AFFIRM_roll_rate_60p"


# Trust-family ILIKE patterns that define each tier's target universe.
# Tier 2 averages roll rates across sponsors per period_end — multiple SDART
# tranches report on the same date, so we aggregate to one observation per
# (period_end) before forward-filling into weekly buckets.
TIER1_AFFIRM_PATTERN = "%AFFIRM%"
TIER2_SUBPRIME_AUTO_PATTERNS = ("%SANTANDER%", "%AMERICREDIT%", "%EXETER%")


def _fetch_rr_rows(con: duckdb.DuckDBPyConnection,
                   patterns: tuple[str, ...]) -> list[tuple[date, float]]:
    """Pull (period_end, mean(roll_rate_60p)) for trust names matching any pattern.

    Averaging across tranches on the same date avoids biasing Granger toward
    whichever trust happens to have the most outstanding tranches in a given
    month. Each (period_end) contributes one observation.
    """
    placeholders = " OR ".join("trust_name ILIKE ?" for _ in patterns)
    rows = con.execute(
        f"""
        SELECT period_end, AVG(roll_rate_60p) AS rr
        FROM abs_tranche_metrics
        WHERE ({placeholders}) AND roll_rate_60p IS NOT NULL
        GROUP BY period_end
        ORDER BY period_end
        """,
        list(patterns),
    ).fetchall()
    return [(r[0], float(r[1])) for r in rows]


def _fetch_pairs(con: duckdb.DuckDBPyConnection,
                 patterns: tuple[str, ...] = (TIER1_AFFIRM_PATTERN,)
                 ) -> tuple[list[date], list[float], list[float]]:
    """Weekly-aligned (date, BSI, roll_rate_60p). Roll rate forward-filled weekly.

    `patterns` selects which trust-name family to target. Defaults to Tier 1
    (AFFIRM) for backwards-compatibility; callers should pass
    TIER2_SUBPRIME_AUTO_PATTERNS to force the public-lookalike target.
    """
    bsi_rows = con.execute(
        "SELECT observed_at, bsi FROM bsi_daily ORDER BY observed_at"
    ).fetchall()
    rr_rows = _fetch_rr_rows(con, patterns)
    if not bsi_rows or not rr_rows:
        return [], [], []

    # Weekly-bucket BSI (mean within ISO week).
    by_week: dict[tuple[int, int], list[float]] = {}
    for d, v in bsi_rows:
        iso = d.isocalendar()
        by_week.setdefault((iso[0], iso[1]), []).append(v)
    weekly_bsi = {k: sum(v) / len(v) for k, v in by_week.items()}

    # Roll rate forward-fill: for each week in the range, take the last
    # roll-rate observation whose period_end <= week_end.
    rr_sorted = sorted(rr_rows, key=lambda r: r[0])
    dates_sorted = sorted({d for d, _ in bsi_rows})
    weeks_sorted = sorted(weekly_bsi.keys())

    xs_date: list[date] = []
    xs_bsi: list[float] = []
    xs_rr: list[float] = []
    rr_i = 0
    last_rr: float | None = None

    for wk in weeks_sorted:
        year, weekno = wk
        # Monday-of-ISO-week date
        week_end = date.fromisocalendar(year, weekno, 7)
        while rr_i < len(rr_sorted) and rr_sorted[rr_i][0] <= week_end:
            last_rr = rr_sorted[rr_i][1]
            rr_i += 1
        if last_rr is None:
            continue
        xs_date.append(week_end)
        xs_bsi.append(weekly_bsi[wk])
        xs_rr.append(last_rr)
    return xs_date, xs_bsi, xs_rr


def _fetch_pairs_proxy(con: duckdb.DuckDBPyConnection,
                       target: str = "HYG") -> tuple[list[date], list[float], list[float]]:
    """
    Fallback target when abs_tranche_metrics is empty: BSI vs a credit-proxy
    FRED series (default HYG high-yield ETF). We use -1 × log-return so the
    series moves with credit stress (HYG down ≡ spreads wider).

    Returns weekly-aligned (dates, bsi, proxy_stress).
    """
    import math

    bsi_rows = con.execute(
        "SELECT observed_at, bsi FROM bsi_daily ORDER BY observed_at"
    ).fetchall()
    px_rows = con.execute(
        "SELECT observed_at, value FROM fred_series WHERE series_id = ? "
        "ORDER BY observed_at",
        [target],
    ).fetchall()
    if not bsi_rows or len(px_rows) < 2:
        return [], [], []

    # Daily negative log-returns (credit stress).
    stress_daily: dict[date, float] = {}
    prev_v: float | None = None
    for d, v in px_rows:
        if prev_v is not None and prev_v > 0 and v and v > 0:
            stress_daily[d] = -math.log(v / prev_v)
        prev_v = v

    # Weekly buckets — mean within ISO week for both series.
    bsi_week: dict[tuple[int, int], list[float]] = {}
    for d, v in bsi_rows:
        iso = d.isocalendar()
        bsi_week.setdefault((iso[0], iso[1]), []).append(v)
    px_week: dict[tuple[int, int], list[float]] = {}
    for d, v in stress_daily.items():
        iso = d.isocalendar()
        px_week.setdefault((iso[0], iso[1]), []).append(v)

    common = sorted(set(bsi_week) & set(px_week))
    xs_date: list[date] = []
    xs_bsi: list[float] = []
    xs_px: list[float] = []
    for wk in common:
        xs_date.append(date.fromisocalendar(wk[0], wk[1], 7))
        xs_bsi.append(sum(bsi_week[wk]) / len(bsi_week[wk]))
        xs_px.append(sum(px_week[wk]) / len(px_week[wk]))
    return xs_date, xs_bsi, xs_px


def _run_gc(rr: list[float], bsi_: list[float], lags: tuple[int, ...]) -> list[GrangerResult]:
    """statsmodels grangercausalitytests wrapper — returns per-lag results."""
    import numpy as np
    from statsmodels.tsa.stattools import grangercausalitytests

    results: list[GrangerResult] = []
    # Column order: [dependent (rr), predictor (bsi)] per statsmodels convention:
    # "tests whether the time series in the second column Granger-causes the first".
    data = np.column_stack([np.asarray(rr, dtype=float), np.asarray(bsi_, dtype=float)])
    if data.shape[0] < max(lags) + 5:
        return results
    try:
        gc = grangercausalitytests(data, maxlag=list(lags), verbose=False)
    except TypeError:
        # Older statsmodels signature.
        gc = grangercausalitytests(data, maxlag=max(lags), verbose=False)
    for lag in lags:
        if lag not in gc:
            continue
        test = gc[lag][0].get("ssr_ftest")
        if test is None:
            continue
        f, p, _df_num, _df_denom = test
        results.append(GrangerResult(lag_weeks=lag, p_value=float(p),
                                     f_stat=float(f), n=data.shape[0]))
    return results


def run_granger(lags: tuple[int, ...] = DEFAULT_LAGS,
                persist: bool = False,
                proxy_fallback: str = "HYG") -> list[GrangerResult]:
    """
    Three-tier target ladder (see module docstring):

      Tier 1 · AFFIRM trustee roll rate  (private — usually empty)
      Tier 2 · SDART + AMCAR + EART composite roll rate  (paper's working target)
      Tier 3 · HYG negative log-return  (market credit-stress proxy)

    We stop at the first tier with enough aligned weekly observations.
    """
    min_obs = max(lags) + 5
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        # Tier 1: AFFIRM.
        dates_, bsi_, rr_ = _fetch_pairs(con, (TIER1_AFFIRM_PATTERN,))
        target_label = "AFFIRM_roll_rate_60p"
        tier_used = 1

        if len(dates_) < min_obs:
            log.warning("granger | Tier-1 AFFIRM target unavailable (%d obs, "
                        "need %d); AFRMMT is 144A-private. Trying Tier-2 "
                        "subprime-auto composite.", len(dates_), min_obs)
            dates_, bsi_, rr_ = _fetch_pairs(con, TIER2_SUBPRIME_AUTO_PATTERNS)
            target_label = "subprime_auto_composite_roll_rate_60p"  # SDART+AMCAR+EART
            tier_used = 2

        if len(dates_) < min_obs:
            log.warning("granger | Tier-2 subprime-auto composite insufficient "
                        "(%d obs, need %d); falling back to Tier-3 proxy '%s'",
                        len(dates_), min_obs, proxy_fallback)
            dates_, bsi_, rr_ = _fetch_pairs_proxy(con, proxy_fallback)
            target_label = f"neg_log_return({proxy_fallback})"
            tier_used = 3

        if len(dates_) < min_obs:
            log.warning("granger | all three tiers insufficient (last n=%d); "
                        "aborting", len(dates_))
            return []

        log.info("granger | Tier-%d target=%s  n=%d weekly obs",
                 tier_used, target_label, len(dates_))
        results = _run_gc(rr_, bsi_, lags)
        # Stamp provenance on each result so downstream figure / paper code
        # can label which tier actually fired (the GrangerResult dataclass
        # carries tier + target_label for this purpose).
        for r in results:
            r.tier = tier_used
            r.target_label = target_label

        if persist and results:
            con.execute("""
                CREATE TABLE IF NOT EXISTS granger_results (
                    run_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tier         INTEGER,
                    target_label VARCHAR,
                    lag_weeks    INTEGER NOT NULL,
                    p_value      DOUBLE,
                    f_stat       DOUBLE,
                    n_obs        INTEGER
                )
            """)
            # Migrate any pre-Sprint-H.d table that lacks the two new columns.
            # ALTER TABLE ADD COLUMN IF NOT EXISTS is supported in DuckDB ≥0.9
            # and is a no-op when the column already exists.
            con.execute("ALTER TABLE granger_results ADD COLUMN IF NOT EXISTS tier INTEGER")
            con.execute("ALTER TABLE granger_results ADD COLUMN IF NOT EXISTS target_label VARCHAR")
            con.executemany(
                "INSERT INTO granger_results (tier, target_label, lag_weeks, p_value, f_stat, n_obs) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(tier_used, target_label, r.lag_weeks, r.p_value, r.f_stat, r.n)
                 for r in results],
            )
        for r in results:
            log.info("granger | tier=%d  target=%s  lag=%dw  p=%.4f  F=%.3f  n=%d",
                     tier_used, target_label, r.lag_weeks,
                     r.p_value, r.f_stat, r.n)
        return results
    finally:
        con.close()


def rolling_granger(window_weeks: int = 104,
                    step_weeks: int = 13,
                    lags: tuple[int, ...] = DEFAULT_LAGS
                    ) -> list[tuple[date, list[GrangerResult]]]:
    """Recompute Granger on rolling windows for robustness.

    Uses the same Tier-1 → Tier-2 ladder as :func:`run_granger` for the
    target; rolling proxy-fallback is not supported (HYG would dominate
    via market drift, biasing the stability test).
    """
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        # Try Tier 1, fall to Tier 2 if AFFIRM data is starved.
        dates_, bsi_, rr_ = _fetch_pairs(con, (TIER1_AFFIRM_PATTERN,))
        if len(dates_) < window_weeks:
            dates_, bsi_, rr_ = _fetch_pairs(con, TIER2_SUBPRIME_AUTO_PATTERNS)
    finally:
        con.close()
    if len(dates_) < window_weeks:
        return []
    out: list[tuple[date, list[GrangerResult]]] = []
    i = 0
    while i + window_weeks <= len(dates_):
        sub_rr = rr_[i : i + window_weeks]
        sub_bs = bsi_[i : i + window_weeks]
        res = _run_gc(sub_rr, sub_bs, lags)
        out.append((dates_[i + window_weeks - 1], res))
        i += step_weeks
    return out


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    res = run_granger(persist=True)
    print("\nGranger BSI -> credit-stress target:")
    print("(target tier auto-selected: 1=AFFIRM trustee, 2=subprime-auto "
          "composite, 3=HYG proxy; see log line above)")
    for r in res:
        flag = "**" if r.p_value < 0.05 else "  "
        print(f"  {flag} lag={r.lag_weeks}w  p={r.p_value:.4f}  "
              f"F={r.f_stat:.3f}  n={r.n}")
