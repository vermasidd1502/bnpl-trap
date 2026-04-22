"""
CFPB consumer-complaints ingest.

Public API, no key: https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/
The complaint database is the *post-grievance* node in the temporal layering
table of MASTERPLAN_V4 §2.1 — one step past social-media post-event signal,
one step before 10-D bureau-reported delinquency. It feeds the dynamic
factor $\\hat f_t$ in the BSI composite (§2.3).

Per MASTERPLAN_V4 the BNPL/near-prime universe maps to the following
complaint-database `company` field values (CFPB uses uppercased legal
names). Unmatched companies are simply ignored — this list is tunable.

Run with:  python -m data.ingest.cfpb
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from data.settings import settings

log = logging.getLogger(__name__)

CFPB_API = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"

# Company name filters — CFPB's `company` field is free-form mixed case,
# legal-entity-specific, and changes over time (e.g. "Affirm, Inc." ->
# "Affirm Holdings, Inc" after the 2021 IPO). These are the *canonical*
# strings actually present in the CFPB complaint database circa 2023-2025
# as surveyed via the /search/ endpoint with `search_term`. Verify with:
#   GET /search/api/v1/?search_term=affirm -> distinct _source.company values.
# Unmatched companies filter to zero rows, not an error.
COMPANIES: list[str] = [
    # --- BNPL primary (treated) ---
    "Affirm Holdings, Inc",
    "Block, Inc.",               # parent of Cash App + Afterpay (acq. 2022)
    "Paypal Holdings, Inc",      # Pay in 4
    "Sezzle Inc.",
    "Klarna AB",                 # international legal entity; most US complaints file here
    # --- Near-prime / comparison set (C1 layer) ---
    "SYNCHRONY FINANCIAL",
    "SOFI TECHNOLOGIES, INC.",
    "OneMain Finance Corporation",
    "Upstart Holdings, Inc.",
    "CAPITAL ONE FINANCIAL CORPORATION",
    "AMERICAN EXPRESS COMPANY",
    "DISCOVER BANK",
]


@retry(reraise=True, stop=stop_after_attempt(4),
       wait=wait_exponential(multiplier=1.0, min=2, max=20))
def _fetch_all(company: str, date_from: str, date_to: str) -> list | dict:
    """GET *all* complaints for `company` in one request.

    Empirically (verified 2026-04-20) the CFPB /search/api/v1/ endpoint with
    `no_aggs=true` ignores the `size` / `from` parameters and returns the
    full filtered set as a flat list. Older docs describe Elasticsearch-style
    pagination, but the live API behavior is now flat-return. We therefore
    make one request per company and rely on server-side filtering.

    Response shape is still one of:
        (a) flat list[dict]                      (current behavior)
        (b) {"hits": {"hits": [...]}}            (historical)
    The caller normalizes via `_extract_hits`.
    """
    params = {
        "company":           company,
        "date_received_min": date_from,
        "date_received_max": date_to,
        "format":            "json",
        "no_aggs":           "true",
    }
    r = httpx.get(CFPB_API, params=params, timeout=120.0)
    r.raise_for_status()
    return r.json()


def _extract_hits(page: list | dict) -> list[dict]:
    """Normalize both response shapes to a list of hit dicts."""
    if isinstance(page, list):
        return [h for h in page if isinstance(h, dict)]
    if isinstance(page, dict):
        hits = (page.get("hits", {}) or {}).get("hits", []) or []
        return [h for h in hits if isinstance(h, dict)]
    return []


def _iter_complaints(company: str, date_from: str, date_to: str):
    """Yield `_source` dicts for every complaint in the date window.

    One HTTP request — the API ignores pagination parameters and returns the
    full filtered set. We de-duplicate defensively on complaint_id in case
    the upstream ever introduces retries that double-serve rows.
    """
    page = _fetch_all(company, date_from, date_to)
    hits = _extract_hits(page)
    seen: set[str] = set()
    for h in hits:
        src = h.get("_source", {}) or {}
        if not src:
            continue
        cid = str(src.get("complaint_id") or h.get("_id") or "")
        if cid and cid in seen:
            continue
        if cid:
            seen.add(cid)
        yield src


def _upsert(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO cfpb_complaints
            (complaint_id, received_at, product, sub_product, issue,
             company, narrative, tags, state)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(
            str(r.get("complaint_id") or r.get("_id") or ""),
            r.get("date_received"),
            r.get("product"),
            r.get("sub_product"),
            r.get("issue"),
            r.get("company"),
            r.get("complaint_what_happened") or None,
            r.get("tags"),
            r.get("state"),
        ) for r in rows if r.get("complaint_id") or r.get("_id")],
    )
    return len(rows)


def ingest_company(company: str, *, start: str = "2019-01-01",
                   end: str | None = None) -> int:
    if settings.offline:
        log.info("offline mode; skipping CFPB %s", company)
        return 0
    end = end or date.today().isoformat()
    rows = list(_iter_complaints(company, start, end))
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, rows)
    finally:
        con.close()
    log.info("cfpb | %-40s | %s..%s | %d complaints", company[:40], start, end, n)
    return n


def ingest_all(*, start: str = "2019-01-01",
               end: str | None = None) -> dict[str, int]:
    results: dict[str, int] = {}
    for c in COMPANIES:
        try:
            results[c] = ingest_company(c, start=start, end=end)
        except Exception as e:   # noqa: BLE001
            log.error("cfpb | %s | FAILED: %s", c, e)
            results[c] = -1
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    summary = ingest_all()
    print("\nCFPB complaint ingest summary:")
    for c, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {c:45s} {n:>6d}")
