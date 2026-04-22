"""
ABS-trust filings index — fast path via SEC EDGAR JSON submissions API.

``sec_edgar.ingest_firm`` pulls filings by the SPONSOR CIK declared in
``config/panel.yaml``, which works for issuer-side filings (10-Q/10-K/8-K)
but returns zero 10-Ds for sponsors like Affirm: 10-Ds are filed by each
individual trust entity, each with its own CIK assigned at issuance.

There are two classes of trust sponsors to handle differently:

  1. **Public-ABS sponsors** (SDART, AMCAR, CARAT, EART, BCRST, CACC,
     OneMain, etc.). These register their trust entities on EDGAR; we
     discover the trust CIKs via EDGAR's company-name search, then pull
     each trust's filings via the fast ``submissions`` JSON endpoint.

  2. **144A private-placement sponsors** (Affirm, Afterpay, most BNPL
     issuers). These do NOT file 10-Ds publicly; the trustee reports are
     sent directly to accredited investors. No amount of scraping will
     recover this data — it is private by statute. We detect this case
     explicitly and emit a clear log message so downstream consumers
     know the gap is structural, not a bug.

The EDGAR submissions JSON endpoint returns every filing for a CIK in
a single request (~5kB-200kB depending on filing count), so pulling
50 trusts costs ~50 cheap JSON requests rather than ~10,000 lazy
per-filing Python objects via edgartools.

Run with:
    python -m data.ingest.abs_trust_index                    # full run
    python -m data.ingest.abs_trust_index --sponsors sdart    # one family
"""
from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime, timezone

import duckdb
import httpx
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential

from data.settings import settings

log = logging.getLogger(__name__)

BROWSE_URL      = "https://www.sec.gov/cgi-bin/browse-edgar"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

# Sponsor -> company-name search keywords on EDGAR. Multiple keywords are
# OR'd; results from each are deduped by CIK. Each match must also pass
# ``TRUST_FILTER`` below, which rejects obvious false-positives.
SPONSOR_KEYWORDS: dict[str, list[str]] = {
    "SDART":   ["santander drive auto"],
    "AMCAR":   ["americredit automobile receivables",
                "americredit auto receivables",
                "americredit master"],
    "CARAT":   ["capital auto receivables"],
    "DRIVE":   ["drive auto receivables"],
    "EART":    ["exeter automobile receivables"],
    "CACC":    ["credit acceptance auto loan"],
    "OMFIT":   ["onemain financial issuance"],
    "BCRST":   ["bridgecrest acceptance"],
}

# Every matched entity must pass this predicate (name LIKE '%trust%' or
# '%receivables%' etc.). Prevents "Affirm Holdings" kinds of matches from
# sneaking in when the keyword is too broad.
TRUST_FILTER = re.compile(r"(trust|receivables|securitization|issuance|LLC)", re.I)

FORMS_WANTED = ("10-D", "ABS-15G", "ABS-EE")

# BNPL sponsors we checked and found to be 144A-private. Kept here so the
# paper's §11 limitations section can cite the enumerated list without
# re-deriving it each time.
PRIVATE_144A_SPONSORS: dict[str, str] = {
    "AFRMMT": "Affirm Asset Securitization Trust — Affirm Holdings files S-1 and "
              "10-Q/10-K but never filed 10-D or ABS-15G on EDGAR as of 2026-04. "
              "Securitizations are 144A private placements.",
    "AFTR":   "Afterpay — no public ABS filings on EDGAR. Block, Inc. (CIK "
              "0001512673) carries Afterpay as a 10-Q operating segment only.",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _ua_headers() -> dict[str, str]:
    return {"User-Agent": settings.sec_edgar_ua}


@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=1, max=10))
def _edgar_browse(company: str, *, count: int = 100) -> list[tuple[str, str]]:
    """Return [(cik, name), ...] matching the company-name prefix search."""
    r = httpx.get(
        BROWSE_URL,
        params={
            "action": "getcompany",
            "company": company,
            "type": "",
            "dateb": "",
            "owner": "include",
            "count": str(count),
        },
        headers=_ua_headers(),
        timeout=30,
        follow_redirects=True,
    )
    r.raise_for_status()
    # EDGAR's HTML table: ...CIK=XXXX...>NNNN</a></td><td scope="row">NAME...
    pairs = re.findall(
        r"CIK=(\d+)[^>]*>\d+</a></td>\s*<td[^>]*>([^<]+?)(?:<|$)",
        r.text,
    )
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for cik, name in pairs:
        cik_s = cik.lstrip("0") or "0"
        if cik_s in seen:
            continue
        seen.add(cik_s)
        out.append((cik_s, name.strip()))
    return out


@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=1, max=10))
def _edgar_submissions(cik: str) -> dict:
    """Return the submissions JSON for one CIK."""
    cik_padded = cik.zfill(10)
    r = httpx.get(
        SUBMISSIONS_URL.format(cik=cik_padded),
        headers=_ua_headers(),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _discover_trust_ciks(sponsor: str) -> list[tuple[str, str]]:
    """For a sponsor code, return the list of matching trust (cik, name) pairs."""
    keywords = SPONSOR_KEYWORDS.get(sponsor, [])
    if not keywords:
        return []
    seen: dict[str, str] = {}
    for kw in keywords:
        for cik, name in _edgar_browse(kw, count=100):
            if not TRUST_FILTER.search(name):
                continue
            seen.setdefault(cik, name)
    out = sorted(seen.items(), key=lambda kv: int(kv[0]))
    log.info("abs_trust_index | sponsor=%s | discovered %d trust entities",
             sponsor, len(out))
    return out


def _filings_from_submissions(j: dict) -> list[dict]:
    """Flatten the recent+files arrays from a submissions JSON into row dicts."""
    company = j.get("name") or ""
    cik = str(j.get("cik") or "").lstrip("0") or "0"
    rows: list[dict] = []

    # The recent block is inline; older filings are split into files[].
    def _emit(recent: dict) -> None:
        forms = recent.get("form", []) or []
        accs  = recent.get("accessionNumber", []) or []
        dates = recent.get("filingDate", []) or []
        reps  = recent.get("reportDate", []) or []
        prims = recent.get("primaryDocument", []) or []
        n = min(len(forms), len(accs), len(dates))
        for i in range(n):
            if forms[i] not in FORMS_WANTED:
                continue
            filed = datetime.strptime(dates[i], "%Y-%m-%d").date()
            pd = None
            if i < len(reps) and reps[i]:
                try:
                    pd = datetime.strptime(reps[i], "%Y-%m-%d").date()
                except ValueError:
                    pd = None
            # Homepage URL convention.
            acc_nodash = accs[i].replace("-", "")
            primary = prims[i] if i < len(prims) else ""
            url = (
                f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
                f"{acc_nodash}/{primary}" if primary else
                f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}/"
            )
            rows.append({
                "accession_no": accs[i],
                "cik":          cik,
                "trust_name":   company,
                "form_type":    forms[i],
                "filed_at":     datetime.combine(
                    filed, datetime.min.time(), tzinfo=timezone.utc),
                "period_end":   pd,
                "url":          url,
            })

    _emit(j.get("filings", {}).get("recent", {}))
    # Older-filings pagination — fetched via separate URL in submissions JSON.
    for extra in j.get("filings", {}).get("files", []) or []:
        name = extra.get("name")
        if not name:
            continue
        try:
            r = httpx.get(
                f"https://data.sec.gov/submissions/{name}",
                headers=_ua_headers(),
                timeout=30,
            )
            r.raise_for_status()
            _emit(r.json())
        except Exception as e:   # noqa: BLE001
            log.warning("abs_trust_index | extra submissions file %s failed: %s",
                        name, e)
    return rows


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


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------
def ingest_sponsor(sponsor: str) -> int:
    """Discover + backfill all 10-D/ABS-EE/ABS-15G for one sponsor family."""
    if settings.offline:
        log.info("abs_trust_index | offline mode; skipping sponsor=%s", sponsor)
        return 0
    if sponsor in PRIVATE_144A_SPONSORS:
        log.info("abs_trust_index | sponsor=%s is 144A-private: %s",
                 sponsor, PRIVATE_144A_SPONSORS[sponsor])
        return 0
    trusts = _discover_trust_ciks(sponsor)
    if not trusts:
        log.warning("abs_trust_index | sponsor=%s | no public trust entities found",
                    sponsor)
        return 0

    con = duckdb.connect(str(settings.duckdb_path))
    try:
        total = 0
        for cik, name in trusts:
            try:
                j = _edgar_submissions(cik)
                rows = _filings_from_submissions(j)
                total += _upsert(con, rows)
                log.info("abs_trust_index | sponsor=%s | %s (CIK %s) | %d filings",
                         sponsor, name[:50], cik, len(rows))
            except Exception as e:   # noqa: BLE001
                log.error("abs_trust_index | sponsor=%s | CIK=%s | FAILED: %s",
                          sponsor, cik, e)
        return total
    finally:
        con.close()


def ingest_all() -> dict[str, int]:
    """Run `ingest_sponsor` across every public sponsor in the panel."""
    with open(settings.root / "config" / "panel.yaml") as f:
        panel = yaml.safe_load(f)
    want_sponsors: list[str] = []
    for g in ("treated", "near_prime", "placebo", "subprime_auto"):
        for firm in panel.get(g, []):
            tf = firm.get("trust_family")
            if tf and tf in SPONSOR_KEYWORDS and tf not in want_sponsors:
                want_sponsors.append(tf)
            elif tf and tf in PRIVATE_144A_SPONSORS and tf not in want_sponsors:
                want_sponsors.append(tf)
    results: dict[str, int] = {}
    for sp in want_sponsors:
        try:
            results[sp] = ingest_sponsor(sp)
        except Exception as e:   # noqa: BLE001
            log.error("abs_trust_index | sponsor=%s | FAILED: %s", sp, e)
            results[sp] = -1
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    parser = argparse.ArgumentParser(prog="python -m data.ingest.abs_trust_index")
    parser.add_argument("--sponsors", nargs="+", default=None,
                        help="Subset of sponsor codes to ingest; default = all.")
    args = parser.parse_args()
    if args.sponsors:
        results = {sp: ingest_sponsor(sp) for sp in args.sponsors}
    else:
        results = ingest_all()
    print("\nABS trust-index summary:")
    grand = 0
    for sp, n in results.items():
        if sp in PRIVATE_144A_SPONSORS:
            status = "PRIV"
        else:
            status = "OK  " if n >= 0 else "ERR "
        print(f"  {status}  {sp:10s} {n:>6d} filings")
        grand += max(0, n)
    print(f"  total: {grand}")
