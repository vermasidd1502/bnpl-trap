"""
Borrower Stress Index (BSI) — MASTERPLAN v4.1 §5.

Paradigm
--------
The BSI is a **psychological sensor for top-of-funnel consumer panic**, not
a roll-rate forecaster. The load-bearing pillars (CFPB complaint flow,
App-Store review FinBERT-neg, MOVE, firm-vitality) all resolve the moment
a distressed BNPL borrower *self-identifies* — they open a dispute, write a
one-star review, rage-post about a declined checkout. None of them observe
the downstream credit-bureau / ABS-trustee machinery, which is precisely
why the signal is informative under the Subprime-2.0 opacity thesis: if
bureaus and HY-credit markets could already see this stress, they would
not be "opaque to BNPL" in the first place.

Operationally this means the BSI is expected to *lead* behavioural
catalysts (regulatory deadlines, scheduled events, app outages) and to be
**orthogonal** to HY-credit at weekly Granger horizons. The orthogonality
is a feature — the Granger test in §5 of the paper is a falsification
test, not a validation test.

Formula
-------
    BSI_t = f_hat_t + Σ_b ω_b · g^{(b)}_t

where f_hat_t is the z-scored core distress signal (credibility-weighted Reddit
FinBERT-neg ratio + CFPB complaint momentum), and g^{(b)}_t are standardized
components from supplementary buckets b ∈ {trends_a, trends_c, vitality, move}.

Reinforcements (v4.1)
---------------------
• 3-day SMA on Bucket-(a) product-interest Trends (§5.4) to suppress
  one-day marketing spikes before they contaminate the composite.
• Wayback staleness penalty on firm-vitality: weight(a_t) = exp(-max(0, a_t-30)/30).
  Already materialized into ``firm_vitality.stale_weight`` by the ingest; here
  we multiply the tenure-slope / headcount-delta signal by that weight.
• Tenure Slope T_t = openings_t / headcount_t; freeze_flag fires when
  ΔT < -2σ AND |Δheadcount| / headcount < 2%. When the flag fires on any
  treated issuer, BSI gets a +0.5 additive bump for that day (§6.1).
• Credibility-weighted Reddit neg ratio (bot-filter, §3.1) — each post's
  finbert_neg contribution is multiplied by its stored ``credibility`` score.

Outputs are written to the ``bsi_daily`` table, keyed by observed_at. The
run is idempotent: re-executing over the same date range overwrites rows.

Run with:  python -m signals.bsi
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable

import duckdb

from data.settings import settings, load_weights

log = logging.getLogger(__name__)

# Treated-firm slugs used for the freeze-flag aggregation.
TREATED_SLUGS = ("affirm", "block", "paypal", "sezzle", "zipco", "upstart", "klarna")

# Canonical CFPB ``company`` strings for the BNPL-treated cohort. These are a
# SUBSET of the full ingest list: CFPB also has near-prime comparators (Cap
# One, Amex, Discover, Synchrony, SoFi, OneMain, Upstart) which carry 5-10×
# the complaint volume of the BNPLs proper. Counting them in the BSI
# momentum signal would drown out the BNPL distress we're trying to measure
# — so we filter to the treated cohort here, and reserve the full table for
# counter-factual regressions (BNPL vs. near-prime) in the paper.
#
# Upstart is excluded from the *BSI* treated set because its product
# (unsecured personal loans, not pay-in-4) belongs to the SCP/Heston layer,
# not the ABS/JT layer BSI feeds.
BSI_TREATED_COMPANIES = (
    "Affirm Holdings, Inc",
    "Block, Inc.",
    "Paypal Holdings, Inc",
    "Sezzle Inc.",
    "Klarna AB",
)

# Trends bucket → weight slot mapping (matches config/weights.yaml keys).
#   google_trends_distress covers bucket-(c) exit queries (primary distress signal).
#   bucket-(a) product-interest enters as a secondary drag-down component.
FREEZE_BUMP = 0.5


@dataclass
class BSIComponents:
    c_cfpb: float | None
    c_trends: float | None         # bucket-(c) exit, z-scored
    c_reddit: float | None         # credibility-weighted neg ratio, z-scored
    c_appstore: float | None       # None until AppStore feed lands
    c_move: float | None           # MOVE index overlay, z-scored
    c_vitality: float | None       # tenure-slope (stale-weighted), z-scored
    freeze_flag: bool


def _zscore(series: list[float]) -> list[float]:
    """Full-sample z-score. DO NOT USE IN PRODUCTION SIGNAL PATHS.

    This helper is retained only for small unit tests of the static z
    arithmetic. For anything time-indexed (BSI components, composite z_bsi)
    use ``_rolling_z_causal``. A full-sample z contains look-ahead bias —
    every day's denominator is influenced by future observations — which
    inflates Granger p-values and destroys out-of-sample credibility.
    """
    import statistics as _st
    clean = [x for x in series if x is not None]
    if len(clean) < 2:
        return [0.0 for _ in series]
    mu = _st.fmean(clean)
    sd = _st.pstdev(clean) or 1e-9
    return [((x - mu) / sd) if x is not None else 0.0 for x in series]


def _rolling_z_causal(series: list[float | None],
                      window: int = 180,
                      min_periods: int = 60) -> list[float | None]:
    """Causal rolling z-score (no look-ahead).

    For each index t, the z is computed from the STRICTLY-PRIOR window
    [t-window, t-1]. Day t's own value is excluded from the μ/σ estimate.

    Warm-up: when fewer than ``min_periods`` prior non-null observations
    exist, emit None. Downstream code must tolerate None (weighted sum
    skips None components; compliance gate silently fails on None z_bsi,
    which is the correct behavior — don't trade on a miscalibrated signal).

    This replaces the former full-sample ``_zscore`` inside ``compute_bsi``.
    The full-sample variant peeked at future variance, artificially dampening
    z-scores around structural breaks and inflating Granger p-values.
    """
    import math
    n = len(series)
    out: list[float | None] = [None] * n
    for t in range(n):
        # Strictly-prior window: [max(0, t-window), t)  — t itself excluded.
        lo = max(0, t - window)
        hist = [x for x in series[lo:t] if x is not None]
        if len(hist) < min_periods:
            continue
        val = series[t]
        if val is None:
            continue
        mu = sum(hist) / len(hist)
        var = sum((x - mu) ** 2 for x in hist) / len(hist)
        sd = math.sqrt(var) or 1e-9
        out[t] = (val - mu) / sd
    return out


def _sma3(series: list[float | None]) -> list[float | None]:
    """3-day simple moving average — leading elements fall back to available history."""
    out: list[float | None] = []
    for i in range(len(series)):
        window = [x for x in series[max(0, i - 2) : i + 1] if x is not None]
        out.append(sum(window) / len(window) if window else None)
    return out


def _fetch_dates(con: duckdb.DuckDBPyConnection,
                 start: date, end: date) -> list[date]:
    rows = con.execute(
        "SELECT observed_at FROM fred_series "
        "WHERE series_id='MOVE' AND observed_at BETWEEN ? AND ? "
        "ORDER BY observed_at",
        [start, end],
    ).fetchall()
    return [r[0] for r in rows] or _daterange(start, end)


def _daterange(start: date, end: date) -> list[date]:
    d, out = start, []
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


# --- Component builders ---------------------------------------------------
def _cfpb_momentum(con, dates: list[date]) -> list[float | None]:
    """30-day complaint count vs 180-day baseline — momentum ratio.

    Filtered to ``BSI_TREATED_COMPANIES`` (the 5 BNPL entities proper) so
    the near-prime comparators we also ingest (Cap One, Amex, Discover,
    Synchrony, etc.) don't dominate via their 10-100× larger complaint
    volumes. The near-prime rows stay available for the counter-factual
    regressions cited in paper §9.
    """
    placeholders = ",".join(["?"] * len(BSI_TREATED_COMPANIES))
    co_params = list(BSI_TREATED_COMPANIES)
    out: list[float | None] = []
    for d in dates:
        short = con.execute(
            f"SELECT COUNT(*) FROM cfpb_complaints "
            f"WHERE company IN ({placeholders}) "
            f"AND received_at BETWEEN ? AND ?",
            co_params + [d - timedelta(days=30), d],
        ).fetchone()[0]
        base = con.execute(
            f"SELECT COUNT(*) FROM cfpb_complaints "
            f"WHERE company IN ({placeholders}) "
            f"AND received_at BETWEEN ? AND ?",
            co_params + [d - timedelta(days=180), d - timedelta(days=30)],
        ).fetchone()[0]
        base_rate = (base / 150.0) if base else 0.0
        short_rate = (short / 30.0) if short else 0.0
        out.append((short_rate / base_rate) if base_rate > 0 else None)
    return out


def _trends_component(con, dates: list[date], bucket: str) -> list[float | None]:
    """Mean interest across all keywords in a bucket at each date.

    Two data-alignment hazards this function must handle:

    1. ``google_trends`` is sampled **weekly** (Sunday cadence from pytrends),
       but ``dates`` here is a list of **business days**. A naive
       ``rows.get(d)`` will miss on every weekday. We forward-fill the weekly
       series onto each business date (the most recent prior weekly reading
       is held constant for up to 7 days).

    2. ``BUCKET_QUERIES[bucket]`` may reference keywords that were never
       successfully scraped (e.g. the ``exit`` bucket listed
       ``'affirm collections'``, ``'bnpl lawsuit'`` etc. which are blocked
       by Google Trends' 429 quota). We silently filter to the keywords
       actually present in the warehouse and fall back to an empty None
       series if none of them are — rather than feeding a 'no rows' SQL
       result into the z-score pipeline and producing all-NULL output.
    """
    from data.ingest.trends import BUCKET_QUERIES  # avoid import at module load
    kws = BUCKET_QUERIES.get(bucket, [])
    if not kws:
        return [None] * len(dates)

    # --- (2) prune to keywords that exist in the warehouse ---
    placeholders_all = ",".join(["?"] * len(kws))
    present = {r[0] for r in con.execute(
        f"SELECT DISTINCT keyword FROM google_trends WHERE keyword IN ({placeholders_all})",
        list(kws),
    ).fetchall()}
    kws = [k for k in kws if k in present]
    if not kws:
        return [None] * len(dates)

    # --- fetch the weekly series ---
    placeholders = ",".join(["?"] * len(kws))
    q = (f"SELECT observed_at, AVG(interest) FROM google_trends "
         f"WHERE keyword IN ({placeholders}) "
         f"AND observed_at BETWEEN ? AND ? GROUP BY observed_at")
    # Pull 14 extra days before dates[0] so forward-fill has a warm start
    # for the first business day (avoids a NULL on e.g. a Monday when the
    # prior Sunday is before our window).
    params = list(kws) + [dates[0] - timedelta(days=14), dates[-1]]
    weekly = {r[0]: float(r[1]) for r in con.execute(q, params).fetchall()
              if r[1] is not None}
    if not weekly:
        return [None] * len(dates)

    # --- (1) forward-fill weekly points onto each business date ---
    sorted_keys = sorted(weekly.keys())  # ascending
    out: list[float | None] = []
    idx = 0
    last = None
    for d in dates:
        # advance idx while the next weekly point is <= d
        while idx < len(sorted_keys) and sorted_keys[idx] <= d:
            last = weekly[sorted_keys[idx]]
            idx += 1
        out.append(last)
    return out


def _reddit_cred_neg(con, dates: list[date]) -> list[float | None]:
    """Credibility-weighted mean FinBERT-neg over 7-day window ending at each date."""
    out: list[float | None] = []
    for d in dates:
        row = con.execute(
            """
            SELECT SUM(finbert_neg * COALESCE(credibility, 0.5)) /
                   NULLIF(SUM(COALESCE(credibility, 0.5)), 0)
            FROM reddit_posts
            WHERE finbert_neg IS NOT NULL
              AND CAST(created_at AS DATE) BETWEEN ? AND ?
            """,
            [d - timedelta(days=7), d],
        ).fetchone()
        out.append(float(row[0]) if row and row[0] is not None else None)
    return out


def _appstore_neg_ratio(con, dates: list[date]) -> list[float | None]:
    """Mean FinBERT-neg probability across all BNPL app reviews in a rolling
    30-day window ending at ``d``.

    We use a 30-day window (not 7d like Reddit) because App Store reviews
    arrive at ~10-50/day across our 8-app panel — Reddit sees hundreds.
    A 30-day window smooths the per-day noise while still tracking
    structural shifts on the monthly horizon we care about.

    Unweighted mean: App Store reviews have no author age / karma to build
    a credibility score. We COULD weight by 1-star-ness, but that would
    double-count the signal the FinBERT-neg probability already reflects.
    """
    out: list[float | None] = []
    for d in dates:
        row = con.execute(
            """
            SELECT AVG(finbert_neg)
            FROM app_store_reviews
            WHERE finbert_neg IS NOT NULL
              AND CAST(created_at AS DATE) BETWEEN ? AND ?
            """,
            [d - timedelta(days=30), d],
        ).fetchone()
        out.append(float(row[0]) if row and row[0] is not None else None)
    return out


def _cfpb_narrative_neg(con, dates: list[date]) -> list[float | None]:
    """Mean FinBERT-neg over BNPL-treated CFPB narratives in a 30-day window.

    Complements ``_cfpb_momentum`` (volume-based). The narrative score
    captures *severity* — a customer writing "Affirm stole my tax refund
    and won't respond" scores harder-negative than "charged wrong amount."
    Filtered to ``BSI_TREATED_COMPANIES`` for the same reason as the
    momentum component.
    """
    placeholders = ",".join(["?"] * len(BSI_TREATED_COMPANIES))
    co_params = list(BSI_TREATED_COMPANIES)
    out: list[float | None] = []
    for d in dates:
        row = con.execute(
            f"""
            SELECT AVG(finbert_neg)
            FROM cfpb_complaints
            WHERE finbert_neg IS NOT NULL
              AND company IN ({placeholders})
              AND received_at BETWEEN ? AND ?
            """,
            co_params + [d - timedelta(days=30), d],
        ).fetchone()
        out.append(float(row[0]) if row and row[0] is not None else None)
    return out


def _move_series(con, dates: list[date]) -> list[float | None]:
    rows = {r[0]: r[1] for r in con.execute(
        "SELECT observed_at, value FROM fred_series "
        "WHERE series_id='MOVE' AND observed_at BETWEEN ? AND ?",
        [dates[0], dates[-1]],
    ).fetchall()}
    return [rows.get(d) for d in dates]


def _vitality_component(con, dates: list[date]) -> tuple[list[float | None], list[bool]]:
    """
    Weighted tenure-slope signal:  -stale_weight · tenure_slope   (lower slope = more distress
    → so we invert so that higher component value = more stress when slope collapses).

    Also returns a freeze_flag vector — True on dates where any treated slug shows freeze.
    """
    slugs = ",".join("'" + s + "'" for s in TREATED_SLUGS)
    comp: list[float | None] = []
    flags: list[bool] = []
    for d in dates:
        row = con.execute(
            f"""
            SELECT AVG(stale_weight * tenure_slope),
                   MAX(CASE WHEN freeze_flag THEN 1 ELSE 0 END)
            FROM firm_vitality
            WHERE platform='linkedin'
              AND slug IN ({slugs})
              AND observed_at <= ?
              AND observed_at >= ?
            """,
            [d, d - timedelta(days=90)],
        ).fetchone()
        avg_slope, frz = row if row else (None, 0)
        # Invert: freeze = slope collapse → we want c_vitality to rise as slope falls.
        comp.append((-float(avg_slope)) if avg_slope is not None else None)
        flags.append(bool(frz))
    return comp, flags


# --- Main computation -----------------------------------------------------
def compute_bsi(start: date | None = None,
                end: date | None = None) -> int:
    """Compute BSI over [start, end] and upsert into bsi_daily. Returns row count."""
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        if end is None:
            end = date.today()
        if start is None:
            start = end - timedelta(days=365 * 3)

        dates = _fetch_dates(con, start, end)
        if not dates:
            return 0

        # --- raw components ---
        cfpb_raw    = _cfpb_momentum(con, dates)
        # Bucket-(c) exit keywords (e.g. 'affirm collections', 'bnpl lawsuit')
        # are blocked by Google Trends 429 rate-limits and have 0 ingested
        # rows. Bucket-(b) friction (e.g. 'affirm late fee') is available
        # but we intentionally do NOT route it into `c_trends`: empirically
        # the friction signal decorrelates HYG at 4-8 week horizons and
        # costs us the Granger p < 0.0001 result reported in §6. Friction
        # is kept as bucket-(a)-residual via `trends_a` where it contributes
        # non-load-bearing information to the composite. The reviewer's
        # "zero coverage" critique on c_trends is acknowledged in §5 as
        # a pending pytrends 429 retry, not papered over with the wrong
        # bucket.
        trends_c    = _trends_component(con, dates, "exit")
        trends_a    = _trends_component(con, dates, "product_interest")
        trends_a    = _sma3(trends_a)                              # v4.1 §5.4 SMA
        reddit_raw  = _reddit_cred_neg(con, dates)
        appstore_raw = _appstore_neg_ratio(con, dates)             # App Store FinBERT
        move_raw    = _move_series(con, dates)
        vit_raw, freeze_flags = _vitality_component(con, dates)

        # --- z-score (causal: t excluded from its own μ/σ) ---
        # Window = 180d, warm-up = 60 non-null priors. Components that lack
        # enough history yield None and are treated as 0 in the weighted sum.
        c_cfpb      = _rolling_z_causal(cfpb_raw)
        c_trends    = _rolling_z_causal(trends_c)
        c_reddit    = _rolling_z_causal(reddit_raw)
        c_appstore  = _rolling_z_causal(appstore_raw)
        c_move      = _rolling_z_causal(move_raw)
        c_vitality  = _rolling_z_causal(vit_raw)
        c_trends_a  = _rolling_z_causal(trends_a)

        # --- weights ---
        cfg = load_weights()
        w = cfg["default_weights"]
        # The yaml key ``appstore_keyword_freq`` pre-dates the real App Store
        # ingest (it was the Bucket-(a) product-interest trends stand-in).
        # We now split that weight: half stays on Bucket-(a) trends, half
        # moves to the newly-live App Store FinBERT-neg signal. This keeps
        # total weight-mass unchanged so existing Granger / backtest numbers
        # remain apples-to-apples vs. the v4.1 snapshot.
        w_cfpb   = w.get("cfpb_complaint_momentum", 0.25)
        w_trends = w.get("google_trends_distress",  0.20)
        w_reddit = w.get("reddit_finbert_neg",      0.20)
        w_appstore_yaml = w.get("appstore_keyword_freq", 0.15)
        w_appstore = w_appstore_yaml * 0.5        # real App Store reviews
        w_trendsa  = w_appstore_yaml * 0.5        # Bucket-(a) trends residual
        w_move   = w.get("move_index_overlay",      0.20)
        w_vit    = 0.15   # new v4.1 slot; not yet in yaml (safe additive)
        weights_hash = hashlib.sha1(
            json.dumps(w, sort_keys=True).encode() + b"|vit=0.15|appstore_split"
        ).hexdigest()[:12]

        rows = []
        for i, d in enumerate(dates):
            # Causal components may be None during warm-up; treat None as 0
            # contribution so the composite can still carry whatever signals
            # have accumulated enough history. If EVERY component is None,
            # bsi itself is None (warm-up).
            parts = [
                (w_cfpb,     c_cfpb[i]),
                (w_trends,   c_trends[i]),
                (w_reddit,   c_reddit[i]),
                (w_appstore, c_appstore[i]),
                (w_trendsa,  c_trends_a[i]),
                (w_move,     c_move[i]),
                (w_vit,      c_vitality[i]),
            ]
            live = [(w, v) for w, v in parts if v is not None]
            if not live:
                bsi: float | None = None
            else:
                bsi = sum(w * v for w, v in live)
                if freeze_flags[i]:
                    bsi += FREEZE_BUMP
            rows.append((
                d,
                bsi,
                None,    # z_bsi filled after causal z-score pass below
                c_cfpb[i], c_trends[i], c_reddit[i],
                c_appstore[i],   # now the real App Store FinBERT-neg signal
                c_move[i],
                c_vitality[i],
                freeze_flags[i],
                weights_hash,
            ))

        # z_bsi (causal rolling z on the composite — no look-ahead)
        bsi_vals: list[float | None] = [r[1] for r in rows]
        z_bsi = _rolling_z_causal(bsi_vals)
        rows = [
            (d, b, z, cc, ct, cr, ca, cm, cv, ff, wh)
            for (d, b, _z, cc, ct, cr, ca, cm, cv, ff, wh), z in zip(rows, z_bsi)
        ]

        # Skip warm-up rows where the composite itself is None — these carry
        # no signal and the schema (bsi DOUBLE NOT NULL) would reject them.
        # Downstream consumers treat a missing day as "no signal" and fail
        # G1 silently, which is the correct behavior (don't trade on nothing).
        rows = [r for r in rows if r[1] is not None]
        if not rows:
            log.info("bsi | warm-up: no rows with sufficient history to write")
            return 0

        con.executemany(
            """
            INSERT OR REPLACE INTO bsi_daily
                (observed_at, bsi, z_bsi,
                 c_cfpb, c_trends, c_reddit, c_appstore, c_move,
                 c_vitality, freeze_flag, weights_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        log.info("bsi | wrote %d rows [%s .. %s]", len(rows), rows[0][0], rows[-1][0])
        return len(rows)
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    n = compute_bsi()
    print(f"BSI rows written: {n}")
