"""
2005–2010 subprime-auto ABS backfill — crisis-regime calibration source.

Per MASTERPLAN_V3 §4 + §4.3 (with v3.1 duration scaler), this module pulls
the pre-crisis / Lehman-era filings for subprime-auto trust families. The
output feeds `quant/regime_transport.py`, which fits Λ_sys on the 2005–2018
auto hazard and extracts θ_sys^bad from the state-2 mean of an MS-CIR.

Scope limited to Λ_sys. Unsystematic dynamics do NOT transport — that layer
stays BNPL-calibrated. See v3 §4.3 for the transport-validity argument and
the φ_θ / φ_κ duration scalers applied downstream.

Why this lives in its own module (not just a function on sec_edgar.py):
  - 2005–2010 EDGAR filings are predominantly the per-deal issuing trust
    CIKs (e.g. "Santander Drive Auto Receivables Trust 2007-1"), not the
    sponsor CIK. edgartools full-text search resolves trust-family → list
    of per-deal CIKs; that discovery step is specific to archival work.
  - Form list is narrower: 10-D + ABS-15G only (8-K is too noisy at volume).
  - Output is written with a `source_tag='auto_abs_crisis_aux'` marker so
    downstream estimators can filter it cleanly from the main 2019+ panel.

Run with:  python -m data.ingest.auto_abs_historical
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import duckdb
from tenacity import retry, stop_after_attempt, wait_exponential

from data.ingest import sec_edgar
from data.settings import settings

log = logging.getLogger(__name__)

# Trust families we target for crisis-regime calibration. Each entry lists
# the public trust-family stem used by EDGAR full-text search; discovery
# returns a list of per-deal CIKs we then index.
CRISIS_TRUST_FAMILIES: list[dict] = [
    {"family": "SDART",  "search": "Santander Drive Auto Receivables Trust"},
    {"family": "AMCAR",  "search": "AmeriCredit Automobile Receivables Trust"},
    {"family": "CAAT",   "search": "Credit Acceptance Auto Loan Trust"},
    {"family": "CARMX",  "search": "CarMax Auto Owner Trust"},   # prime; control comparator
    {"family": "FORDO",  "search": "Ford Credit Auto Owner Trust"},  # prime; control comparator
]

CRISIS_START = "2005-01-01"
CRISIS_END   = "2010-12-31"
CRISIS_FORMS = ["10-D", "ABS-15G"]
SOURCE_TAG   = "auto_abs_crisis_aux"


@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=2, max=15))
def _discover_trust_ciks(search_stem: str) -> list[dict]:
    """Resolve a trust family stem to a list of {cik, name} for per-deal trusts.

    Uses edgartools' company search. Returns entities whose name matches the
    stem; callers further filter by filing date inside _fetch_filings.
    """
    sec_edgar._configure_edgar_identity()
    from edgar import find   # edgartools public search

    try:
        hits = find(search_stem)
    except Exception as e:  # noqa: BLE001
        log.warning("auto-abs-historical | search failed for %s: %s", search_stem, e)
        return []

    out: list[dict] = []
    # edgartools returns a lazy container; each item has .cik and .name.
    for h in getattr(hits, "entities", []) or []:
        cik = getattr(h, "cik", None)
        name = getattr(h, "name", None) or getattr(h, "company_name", None)
        if cik and name and search_stem.split()[0].lower() in name.lower():
            out.append({"cik": str(cik), "name": name})
    return out


def _upsert_tagged(con: duckdb.DuckDBPyConnection, rows: list[dict], tag: str) -> int:
    """Same as sec_edgar._upsert but stamps trust_name with a [tag] prefix so
    downstream SQL can partition crisis-aux rows from the 2019+ main panel
    without requiring a schema migration."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO sec_filings_index
            (accession_no, cik, trust_name, form_type, filed_at, period_end, url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(r["accession_no"], r["cik"],
          f"[{tag}] {r.get('trust_name') or ''}".strip(),
          r["form_type"], r["filed_at"], r["period_end"], r["url"]) for r in rows],
    )
    return len(rows)


def ingest_family(family: dict, *, start: str = CRISIS_START,
                  end: str = CRISIS_END) -> int:
    """Pull all 10-D / ABS-15G filings for every trust under a family stem."""
    if settings.offline:
        log.info("offline mode; skipping crisis-aux family %s", family["family"])
        return 0

    trusts = _discover_trust_ciks(family["search"])
    log.info("auto-abs-historical | %s | discovered %d per-deal trusts",
             family["family"], len(trusts))
    if not trusts:
        return 0

    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    total = 0
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        for trust in trusts:
            try:
                rows = sec_edgar._fetch_filings(trust["cik"], CRISIS_FORMS, start)
            except Exception as e:  # noqa: BLE001
                log.error("auto-abs-historical | %s | fetch failed: %s",
                          trust["name"], e)
                continue
            # Clip to crisis window end (sec_edgar only enforces start).
            rows = [r for r in rows if r["filed_at"].date() <= end_dt]
            # Stamp family on rows so the state-2 MS-CIR fit can group cleanly.
            for r in rows:
                r["trust_name"] = f"{family['family']}::{trust['name']}"
            n = _upsert_tagged(con, rows, SOURCE_TAG)
            total += n
            log.info("auto-abs-historical | %-10s | %-50s | %d filings",
                     family["family"], trust["name"][:50], n)
    finally:
        con.close()
    return total


def ingest_all(*, start: str = CRISIS_START, end: str = CRISIS_END) -> dict[str, int]:
    """Backfill every configured crisis-era trust family."""
    results: dict[str, int] = {}
    for fam in CRISIS_TRUST_FAMILIES:
        try:
            results[fam["family"]] = ingest_family(fam, start=start, end=end)
        except Exception as e:  # noqa: BLE001
            log.error("auto-abs-historical | %s | FAILED: %s", fam["family"], e)
            results[fam["family"]] = -1
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    log.info("auto-abs-historical | window = %s -> %s | forms = %s",
             CRISIS_START, CRISIS_END, ",".join(CRISIS_FORMS))
    summary = ingest_all()
    print("\nAuto-ABS crisis-aux ingest summary (2005-2010):")
    for fam, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {fam:10s} {n:>6d} filings")
    print(f"\nSource tag in DB: trust_name LIKE '[{SOURCE_TAG}]%'")
    print("Downstream consumer: quant/regime_transport.py (v3 §4.2)")
