"""
Google Trends ingest — the four-bucket taxonomy (MASTERPLAN_V4 §2 +
Sprint-H.d "Direct Admission" extension).

Per v4 §2.2, queries were originally split into three semantic buckets:
  (a) product-interest  — demand proxy, sign ambiguous for credit (powers
                          the Bucket-A sizing gate in §5.4, 3-day MA'd)
  (b) friction          — mid-funnel stress, feeds Trends overlay ω_b g_t
  (c) exit              — acute distress, fires the unsystematic jump
                          trigger (§2.4) with co-occurrence gate vs Reddit

Sprint-H.d adds a fourth bucket at the request of the research lead:
  (d) direct_admission  — first-person liquidity-wall queries
                          ("i am unable to pay for affirm",
                          "affirm hardship program"). Captures the moment a
                          borrower self-identifies as distressed, which
                          typically precedes the 60+DPD roll-rate print in
                          ABS trustee reports by several weeks. Feeds the
                          same Trends overlay as `friction` but with a
                          stronger prior — see `config/weights.yaml`.

The `google_trends` table stores one row per (keyword, observed_at); the
bucket label lives in BUCKET_QUERIES below and is joined at signal-build
time (not stored on the row, so the taxonomy stays editable without a
migration).

Two ingestion paths share the same warehouse table:

  * THIS MODULE (`data.ingest.trends`) — pytrends-based, unofficial scraper.
    Respect rate limits; retries are conservative. Useful for ad-hoc
    keyword discovery but hits 429s almost immediately at any real volume.

  * `data.ingest.trends_manual` — parses CSVs the user exported by hand
    from `https://trends.google.com/`. This is the preferred path for the
    paper (official data, no rate-limit war). See
    `data/manual_exports/google_trends/README.md` for the workflow.

Run with:  python -m data.ingest.trends
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb
from tenacity import retry, stop_after_attempt, wait_exponential

from data.settings import settings

log = logging.getLogger(__name__)

# --- Query taxonomy (v4 §2.2) ---------------------------------------------
BUCKET_QUERIES: dict[str, list[str]] = {
    "product_interest": [
        "affirm",
        "klarna",
        "afterpay",
        "buy now pay later",
        "pay in 4",
        "sezzle",
        "zip pay",
    ],
    "friction": [
        "affirm late fee",
        "klarna late fee",
        "affirm declined",
        "afterpay declined",
        "cant pay klarna",
        "bnpl denied",
    ],
    "exit": [
        "affirm collections",
        "klarna collections",
        "how to remove affirm from credit report",
        "bnpl lawsuit",
        "debt consolidation bnpl",
        "delete afterpay account",
        "cancel klarna",
    ],
    # Sprint-H.d: first-person self-identification of distress. These terms
    # are the behavioural analogue of a 60+DPD admission — the borrower has
    # already hit a liquidity wall and is looking for an exit path. Typically
    # lead roll-rate prints by several weeks, which is the whole point of the
    # BSI as a leading indicator over ABS trustee filings.
    "direct_admission": [
        "i am unable to pay for affirm",
        "affirm hardship program",
        "stop affirm automatic payments",
        "affirm debt collection help",
        "how to delete affirm account with balance",
    ],
}

# Flat list for ingestion loops; bucket membership resolved at signal time.
ALL_QUERIES: list[tuple[str, str]] = [
    (bucket, q) for bucket, qs in BUCKET_QUERIES.items() for q in qs
]

DEFAULT_TIMEFRAME = "today 5-y"   # pytrends: last 5 years, weekly granularity
REGION = "US"


@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=3, max=30))
def _fetch_series(keyword: str, timeframe: str = DEFAULT_TIMEFRAME,
                  geo: str = REGION) -> list[dict]:
    """Return a list of {observed_at, interest} rows for a single keyword."""
    from pytrends.request import TrendReq   # local import — heavy dep

    py = TrendReq(hl="en-US", tz=0, timeout=(10, 30))
    py.build_payload([keyword], timeframe=timeframe, geo=geo)
    df = py.interest_over_time()
    if df is None or df.empty:
        return []
    df = df.reset_index().rename(columns={"date": "observed_at", keyword: "interest"})
    return [
        {"observed_at": row["observed_at"].date(), "interest": float(row["interest"])}
        for _, row in df.iterrows()
    ]


def _upsert(con: duckdb.DuckDBPyConnection, keyword: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO google_trends (keyword, observed_at, interest)
        VALUES (?, ?, ?)
        """,
        [(keyword, r["observed_at"], r["interest"]) for r in rows],
    )
    return len(rows)


def ingest_keyword(keyword: str, *, timeframe: str = DEFAULT_TIMEFRAME) -> int:
    if settings.offline:
        log.info("offline mode; skipping trends %s", keyword)
        return 0
    rows = _fetch_series(keyword, timeframe=timeframe)
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, keyword, rows)
    finally:
        con.close()
    log.info("trends | %-40s | %s | %d weeks", keyword[:40], timeframe, n)
    return n


def ingest_all(*, timeframe: str = DEFAULT_TIMEFRAME,
               sleep_between: float = 2.0) -> dict[str, int]:
    """Pull every configured keyword with a polite delay between requests."""
    results: dict[str, int] = {}
    for bucket, kw in ALL_QUERIES:
        try:
            results[f"{bucket}:{kw}"] = ingest_keyword(kw, timeframe=timeframe)
            if sleep_between and not settings.offline:
                time.sleep(sleep_between)
        except Exception as e:   # noqa: BLE001
            log.error("trends | %s | FAILED: %s", kw, e)
            results[f"{bucket}:{kw}"] = -1
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    summary = ingest_all()
    print("\nGoogle Trends ingest summary:")
    for key, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {key:55s} {n:>5d} weeks")
