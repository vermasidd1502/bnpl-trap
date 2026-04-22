"""
ABS trustee-report parser — roll rate / excess spread / CNL / senior enh.

Per MASTERPLAN_V4.1 this module is the load-bearing empirical input to:
  (1) the Shadow-Bureau-Gap diagnostic (§5.5) — the reported-delinquency
      leg of  SBG = d^alt - d^reported  comes from here
  (2) the two-factor JT Λ_sys calibration (§1.3) — the macro hazard proxy
      is the cross-sectional median of these roll-rate innovations
  (3) the 2008 crisis-transport fit (§4.1) — same four fields parsed
      from archival auto-ABS 10-Ds produce θ_sys^bad

Design
------
- Consumes rows from `sec_filings_index` where form_type='10-D'.
- Downloads the primary document via edgartools and runs a set of
  regex patterns tuned against published AFRMMT / SDART / AMCAR templates.
- Pure-function parsers (`_parse_*`) are unit-testable without network.
- Writes to `abs_tranche_metrics`. Upsert on accession_no.
- Missing-field handling: each of the four metrics is individually nullable;
  a filing with 3/4 parseable fields still lands a row with one NULL,
  downstream models use `COALESCE` or issuer-specific imputation.
- Idempotent.

Run with:  python -m data.ingest.abs_parser
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import duckdb
from tenacity import retry, stop_after_attempt, wait_exponential

from data.ingest import sec_edgar
from data.settings import settings

log = logging.getLogger(__name__)

# --- Regex library ---------------------------------------------------------
# Each pattern returns the first numeric capture group as a float (percent).
# Patterns are ordered most-specific first; first hit wins. All are case-
# insensitive and whitespace-tolerant. Keep them narrow — false positives
# corrupt downstream Λ_sys calibration.

# ``_PCT_STRICT`` requires a trailing % sign (optionally preceded by whitespace).
# This excludes SEC servicer reports' ``{NN}`` footnote tokens — plain bare
# numbers that would otherwise false-positive the parse. Every percent value
# in the servicer exhibits we observed (SDART / AMCAR / EART) is printed as
# e.g. ``0.83 %`` so the stricter form matches the real data but rejects
# footnote markers.
_PCT_STRICT = r"([0-9]+(?:\.[0-9]+)?)\s*%"
_PCT = _PCT_STRICT

ROLL_RATE_PATTERNS: list[re.Pattern] = [
    # AFRMT-style phrasing (hypothetical — BNPL trusts would use these verbs):
    re.compile(
        rf"60\+?[-\s]*(?:day|dpd)?[^%]{{0,80}}?roll[-\s]?rate[^%]{{0,60}}?{_PCT}",
        re.I | re.S),
    re.compile(
        rf"roll[-\s]?rate[^%]{{0,80}}?60\+?[^%]{{0,40}}?{_PCT}",
        re.I | re.S),
    re.compile(rf"roll\s*to\s*60\+?[^%]{{0,40}}?{_PCT}", re.I | re.S),
    # SDART / AMCAR / EART servicer-report phrasing (actual observed format).
    # The row immediately after "Aggregate Principal Balance of 60 Day
    # Delinquent Receivables" is the "Delinquency Percentage", which in the
    # SDART 2026-1 March 2026 servicer report reads:
    #   "... {84} 15,297,938.63 {85} Delinquency Percentage ... {85} 0.83 %"
    # so we must tolerate ``{NN}`` footnote markers between the anchor and
    # the percentage, and the trailing % gates out footnote numbers.
    re.compile(
        rf"Aggregate\s+Principal\s+Balance\s+of\s+60\s+Day\s+Delinquent"
        rf"[\s\S]{{0,400}}?Delinquency\s+Percentage[\s\S]{{0,120}}?{_PCT}",
        re.I),
    # Fallback: any "Delinquency Percentage" label (first hit).
    re.compile(rf"Delinquency\s+Percentage[\s\S]{{0,120}}?{_PCT}", re.I),
]

EXCESS_SPREAD_PATTERNS: list[re.Pattern] = [
    re.compile(rf"excess\s*spread[\s\S]{{0,80}}?{_PCT}", re.I),
    re.compile(rf"net\s*excess\s*spread[\s\S]{{0,80}}?{_PCT}", re.I),
    re.compile(rf"excess\s+collection[\s\S]{{0,80}}?{_PCT}", re.I),
]

CNL_PATTERNS: list[re.Pattern] = [
    re.compile(rf"cumulative\s+net\s+loss\s+ratio[\s\S]{{0,120}}?{_PCT}", re.I),
    re.compile(rf"cumulative\s+net\s+loss[\s\S]{{0,80}}?{_PCT}", re.I),
    re.compile(rf"\bCNL\b[\s\S]{{0,60}}?{_PCT}", re.I),
    # SDART "Current Period Net Loss Ratio" is a current-period reading;
    # tertiary fallback when true CNL is absent. Footnote text between the
    # label and the percent is common so we use [\s\S]*?.
    re.compile(rf"current\s+period\s+net\s+loss\s+ratio[\s\S]{{0,160}}?{_PCT}", re.I),
]

SENIOR_ENH_PATTERNS: list[re.Pattern] = [
    re.compile(rf"senior\s*credit\s*enhancement[\s\S]{{0,80}}?{_PCT}", re.I),
    re.compile(rf"senior\s*enhancement[\s\S]{{0,80}}?{_PCT}", re.I),
    re.compile(rf"overcollateralization[\s\S]{{0,80}}?{_PCT}", re.I),
]


@dataclass
class TrancheMetrics:
    roll_rate_60p: float | None
    excess_spread: float | None
    cnl: float | None
    senior_enh: float | None

    def nonnull(self) -> int:
        return sum(x is not None for x in
                   (self.roll_rate_60p, self.excess_spread, self.cnl, self.senior_enh))


# --- Pure parsers (unit-testable without network) --------------------------
def _first_match(text: str, patterns: list[re.Pattern]) -> float | None:
    for p in patterns:
        m = p.search(text)
        if m:
            try:
                v = float(m.group(1))
            except ValueError:
                continue
            # Sanity: percentages 0..100. Reject obvious parse corruption.
            if 0.0 <= v <= 100.0:
                return v
    return None


def parse_trustee_text(text: str) -> TrancheMetrics:
    """Extract the four metrics from a trustee-report text blob.

    Input is the full textual body of a 10-D filing (HTML stripped).
    Output fields are floats in percent units, or None if not found.
    """
    return TrancheMetrics(
        roll_rate_60p=_first_match(text, ROLL_RATE_PATTERNS),
        excess_spread=_first_match(text, EXCESS_SPREAD_PATTERNS),
        cnl=_first_match(text, CNL_PATTERNS),
        senior_enh=_first_match(text, SENIOR_ENH_PATTERNS),
    )


def _html_to_text(html: str) -> str:
    """Best-effort HTML → plain text. BS4 if available, else crude strip."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator=" ")
    except ImportError:
        return re.sub(r"<[^>]+>", " ", html)


# --- EDGAR fetch -----------------------------------------------------------
@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=2, max=15))
def _http_get_text(url: str) -> str:
    import httpx
    from data.settings import settings as _settings
    r = httpx.get(url, headers={"User-Agent": _settings.sec_edgar_ua}, timeout=60,
                  follow_redirects=True)
    r.raise_for_status()
    ct = r.headers.get("content-type", "").lower()
    raw = r.text
    if "html" in ct or raw.lstrip().lower().startswith(("<html", "<!doctype")):
        return _html_to_text(raw)
    return raw


def _servicer_exhibit_url(index_url: str) -> str | None:
    """Resolve the filing directory and pick the servicer-report exhibit.

    10-D filings on SDART / AMCAR / EART are structured as:

        dXXXd10d.htm     ~18kB   form-cover legal shell (no data)
        dXXXdex991.htm   150-250kB  servicer / trustee report (DATA)
        sdartNN_exXXX.xml   big   ABS-EE loan-level data (not used here)

    The `url` stored in ``sec_filings_index`` points at the cover file. We
    query the directory's ``index.json`` sibling, find the largest .htm
    file whose name contains ``ex99`` / ``ex-99`` / ``ex_99``, and return
    its fully-qualified URL.

    Returns None if no servicer exhibit is discoverable — the caller
    should fall back to parsing the cover file or mark the filing empty.
    """
    import httpx
    import os
    from data.settings import settings as _settings

    # Derive the directory URL from the filing URL.
    # e.g. https://www.sec.gov/Archives/edgar/data/2105961/000119312526155447/d125198d10d.htm
    #      -> https://www.sec.gov/Archives/edgar/data/2105961/000119312526155447/
    dir_url = index_url.rsplit("/", 1)[0] + "/"
    try:
        r = httpx.get(
            dir_url + "index.json",
            headers={"User-Agent": _settings.sec_edgar_ua},
            timeout=30,
        )
        r.raise_for_status()
        items = r.json().get("directory", {}).get("item", []) or []
    except Exception as e:   # noqa: BLE001
        log.warning("abs-parser | index.json for %s failed: %s", dir_url, e)
        return None

    # Rank servicer-exhibit candidates. Prefer .htm / .html files with
    # "ex99" / "ex-99" / "ex_99" / "ex991" in the filename; fall back to the
    # largest .htm in the directory that isn't the cover.
    def _is_ex99(name: str) -> bool:
        n = name.lower()
        return any(tag in n for tag in ("ex99", "ex-99", "ex_99", "ex991", "servicer"))

    def _ext_ok(name: str) -> bool:
        return name.lower().endswith((".htm", ".html"))

    candidates: list[tuple[int, bool, str]] = []
    for item in items:
        name = item.get("name") or ""
        if not _ext_ok(name) or name.lower().endswith("d10d.htm"):
            continue
        try:
            size = int(item.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        # Tag = (size, is_servicer_exhibit, name)
        candidates.append((size, _is_ex99(name), name))

    if not candidates:
        return None
    # Prefer ex99-tagged, then largest.
    candidates.sort(key=lambda t: (not t[1], -t[0]))
    best = candidates[0]
    return dir_url + best[2]


def _fetch_document_text(accession_no: str, *, url: str | None = None) -> str:
    """Pull the filing's trustee-report text.

    Strategy (in order):

      1. If ``url`` ends with ``d10d.htm`` (the cover-page convention), try
         to resolve the servicer exhibit in the same directory and fetch
         that instead. Servicer exhibits carry the actual delinquency /
         CNL / excess-spread tables.
      2. If resolution fails or the URL is not a cover-page URL, fetch the
         URL directly.

    The caller (``parse_all_unparsed``) must supply ``url``; edgartools 3.x
    does not accept accession-only ``Filing()`` construction, and every row
    ingested by ``data.ingest.abs_trust_index`` has a url populated.
    """
    if not url:
        raise RuntimeError(
            "abs_parser._fetch_document_text: url is required; "
            "supply url from sec_filings_index.url."
        )

    # Prefer the servicer exhibit when the stored URL is the cover file.
    if url.lower().endswith("d10d.htm") or url.lower().endswith("10d.htm"):
        exhibit = _servicer_exhibit_url(url)
        if exhibit:
            try:
                text = _http_get_text(exhibit)
                if text and len(text) > 5000:
                    return text
            except Exception as e:   # noqa: BLE001
                log.warning(
                    "abs-parser | servicer exhibit %s failed, falling back to cover: %s",
                    exhibit, e,
                )
    # Cover file fallback (or non-SDART-style URL).
    return _http_get_text(url)


# --- DuckDB writer ---------------------------------------------------------
def _upsert(con: duckdb.DuckDBPyConnection, accession_no: str,
            trust_name: str, period_end, metrics: TrancheMetrics) -> int:
    con.execute(
        """
        INSERT OR REPLACE INTO abs_tranche_metrics
            (accession_no, trust_name, period_end,
             roll_rate_60p, excess_spread, cnl, senior_enh)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (accession_no, trust_name, period_end,
         metrics.roll_rate_60p, metrics.excess_spread,
         metrics.cnl, metrics.senior_enh),
    )
    return 1


# --- Public entry points ---------------------------------------------------
def parse_filing(
    accession_no: str,
    trust_name: str,
    period_end,
    *,
    url: str | None = None,
) -> TrancheMetrics:
    """Fetch + parse one filing. Network-dependent.

    Pass ``url`` (the primary-document URL from ``sec_filings_index``) to
    use the fast httpx path. If ``url`` is None, raise — edgartools 3.x
    does not allow accession-only construction of a ``Filing`` object.
    """
    if settings.offline:
        log.info("offline mode; skipping parse of %s", accession_no)
        return TrancheMetrics(None, None, None, None)
    raw = _fetch_document_text(accession_no, url=url)
    return parse_trustee_text(raw)


def parse_all_unparsed(*, limit: int | None = None) -> dict[str, int]:
    """Parse every 10-D in sec_filings_index not yet in abs_tranche_metrics."""
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        q = """
        SELECT f.accession_no, f.trust_name, f.period_end, f.url
        FROM sec_filings_index f
        LEFT JOIN abs_tranche_metrics m USING (accession_no)
        WHERE f.form_type = '10-D' AND m.accession_no IS NULL
        ORDER BY f.filed_at DESC
        """
        if limit:
            q += f" LIMIT {int(limit)}"
        todo = con.execute(q).fetchall()
        log.info("abs-parser | %d unparsed 10-D filings", len(todo))

        counts = {"parsed": 0, "empty": 0, "failed": 0}
        for accession_no, trust_name, period_end, url in todo:
            try:
                m = parse_filing(accession_no, trust_name or "", period_end, url=url)
                _upsert(con, accession_no, trust_name or "", period_end, m)
                if m.nonnull() == 0:
                    counts["empty"] += 1
                else:
                    counts["parsed"] += 1
                log.info("abs-parser | %s | %s | fields=%d/4",
                         accession_no, (trust_name or "")[:40], m.nonnull())
            except Exception as e:  # noqa: BLE001
                log.error("abs-parser | %s | FAILED: %s", accession_no, e)
                counts["failed"] += 1
        return counts
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    summary = parse_all_unparsed()
    print("\nABS parser summary:")
    for k, v in summary.items():
        print(f"  {k:8s} {v:>6d}")
