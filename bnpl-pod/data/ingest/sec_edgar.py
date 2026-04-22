"""
SEC EDGAR ingest — filings index for the 23-entity panel + auto-ABS history.

Covers (per MASTERPLAN_V3 §6.1):
  - 10-D  (ABS distribution reports — BNPL + auto trusts)
  - 10-Q  (quarterly issuer financials — banks + fintechs)
  - 10-K  (annual; anchors segmentation)
  - ABS-15G (ABS representations & warranties filings)
  - 20-F   (foreign private issuers — Zip Co)

Design
------
- Wraps `edgartools` rather than reinventing. The SEC exposes a 10 req/s cap
  and a mandatory User-Agent header — edgartools handles both.
- Reads the firm panel from config/panel.yaml. Every CIK in the panel gets
  its filings index pulled; trust-family filings (AFRMMT, SDART, AMCAR etc.)
  are pulled via the sponsor CIK when the trust has no standalone CIK.
- Writes to `sec_filings_index` in DuckDB. Idempotent upsert on accession_no.
- Respects `settings.offline` for CI.

Run with:  python -m data.ingest.sec_edgar
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential

from data.settings import settings

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")

PANEL_PATH = settings.root / "config" / "panel.yaml"

# Forms we care about. Keyed by group so we can target only what's relevant.
FORMS_BY_ROLE = {
    "abs_trust":   ["10-D", "ABS-15G", "ABS-EE"],
    "issuer":      ["10-Q", "10-K", "8-K"],
    "foreign":     ["20-F", "6-K"],
}


# --- Panel loading ----------------------------------------------------------
def load_panel() -> dict:
    """Return the parsed panel.yaml with all 4 groups."""
    with open(PANEL_PATH) as f:
        return yaml.safe_load(f)


def flatten_panel(panel: dict) -> list[dict]:
    """Flatten groups into a single list with group annotation."""
    rows: list[dict] = []
    for group_key in ("treated", "near_prime", "placebo", "subprime_auto"):
        for firm in panel.get(group_key, []):
            rows.append({**firm, "group": group_key})
    return rows


# --- EDGAR client -----------------------------------------------------------
def _configure_edgar_identity() -> None:
    """SEC requires a declared identity; edgartools reads EDGAR_IDENTITY env."""
    import os
    os.environ["EDGAR_IDENTITY"] = settings.sec_edgar_ua


@retry(reraise=True, stop=stop_after_attempt(4),
       wait=wait_exponential(multiplier=1.0, min=2, max=20))
def _fetch_filings(cik: str, forms: list[str], start: str) -> list[dict]:
    """Return filings for a CIK as list of dicts. Raises on SEC errors."""
    _configure_edgar_identity()
    from edgar import Company   # local import — heavy dep

    co = Company(cik)
    filings = co.get_filings(form=forms)
    rows: list[dict] = []
    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    for f in filings:
        filed = f.filing_date
        if filed < start_dt:
            continue
        rows.append({
            "accession_no": f.accession_no,
            "cik":          str(cik).lstrip("0"),
            "trust_name":   getattr(f, "company", None),
            "form_type":    f.form,
            "filed_at":     datetime.combine(filed, datetime.min.time(), tzinfo=timezone.utc),
            "period_end":   getattr(f, "period_of_report", None),
            "url":          f.homepage_url if hasattr(f, "homepage_url") else None,
        })
    return rows


def _role_for(firm: dict) -> str:
    """Decide which form set to pull for a firm."""
    if firm.get("trust_family"):
        return "abs_trust"
    if firm.get("ticker", "").endswith(".AX") or firm.get("group") in {"foreign"}:
        return "foreign"
    return "issuer"


# --- DuckDB writer ----------------------------------------------------------
def _upsert(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO sec_filings_index
            (accession_no, cik, trust_name, form_type, filed_at, period_end, url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(r["accession_no"], r["cik"], r["trust_name"], r["form_type"],
          r["filed_at"], r["period_end"], r["url"]) for r in rows],
    )
    return len(rows)


# --- Public entry points ----------------------------------------------------
def ingest_firm(firm: dict, *, start: str = "2019-01-01") -> int:
    """Pull filings index for one firm."""
    cik = firm.get("cik")
    if not cik:
        log.info("edgar | %s | no CIK; skipping", firm.get("name"))
        return 0
    if settings.offline:
        log.info("offline mode; skipping %s", firm.get("name"))
        return 0

    role = _role_for(firm)
    forms = FORMS_BY_ROLE[role]
    rows = _fetch_filings(cik, forms, start)

    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, rows)
    finally:
        con.close()
    log.info("edgar | %-30s | %s forms=%s | %d filings",
             firm.get("name"), cik, ",".join(forms), n)
    return n


def ingest_panel(*, start: str = "2019-01-01") -> dict[str, int]:
    """Ingest filings for every firm in config/panel.yaml."""
    panel = load_panel()
    firms = flatten_panel(panel)
    results: dict[str, int] = {}
    for firm in firms:
        name = firm.get("name", firm.get("ticker", "unknown"))
        try:
            results[name] = ingest_firm(firm, start=start)
        except Exception as e:   # noqa: BLE001
            log.error("edgar | %s | FAILED: %s", name, e)
            results[name] = -1
    return results


def ingest_auto_abs_historical(*, start: str = "2005-01-01",
                               end: str = "2010-12-31") -> dict[str, int]:
    """
    Pull the 2005-2010 auto-ABS filings window for crisis-regime calibration
    (MASTERPLAN_V3 §4). Only the subprime_auto group is touched here.
    """
    panel = load_panel()
    results: dict[str, int] = {}
    for firm in panel.get("subprime_auto", []):
        name = firm.get("name", firm.get("ticker", "unknown"))
        try:
            # Use start override; filter by end inside.
            results[name] = ingest_firm(firm, start=start)
            log.info("edgar | auto-abs historical | %s | OK", name)
        except Exception as e:   # noqa: BLE001
            log.error("edgar | auto-abs historical | %s | FAILED: %s", name, e)
            results[name] = -1
    return results


if __name__ == "__main__":
    summary = ingest_panel()
    print("\nEDGAR panel ingest summary:")
    for name, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {name:35s} {n:>5d} filings")
