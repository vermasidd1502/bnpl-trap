"""
Firm-vitality ingest — LinkedIn + X signals via the Wayback Machine.

Per MASTERPLAN_V4 §6.1, this module is the legally-defensible path to
LinkedIn and X data: we never hit linkedin.com or x.com directly. We
query the Internet Archive CDX API for cached snapshots of public
corporate pages and parse the archived HTML with BeautifulSoup.

Signals extracted
-----------------
LinkedIn (`linkedin.com/company/<slug>`)
    - headcount       (employee-count band midpoint)
    - openings        (job-postings count)
    - tenure_slope    = openings / headcount           (v4 §6.1 sub-signal)
    - freeze_flag     1 iff ΔTenureSlope < -2σ AND headcount flat
                      (the "hiring freeze while nobody leaves" indicator)

X / Twitter (`twitter.com/<handle>`)
    - followers       (follower count)

Staleness penalty (v4.1 §6.1)
-----------------------------
Wayback snapshots are not real-time. Each ingested row carries the age
(in days) of the snapshot and a corresponding exponential decay weight:

    stale_weight = exp( -max(0, age_d - 30) / tau ),  tau = 30

Downstream models (BSI dynamic factor) multiply feature contributions by
`stale_weight` so a three-month-old snapshot carries ~14% of a fresh one.

Run with:  python -m data.ingest.firm_vitality
"""
from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from data.settings import settings

log = logging.getLogger(__name__)

# Wayback CDX API — returns a list of snapshots matching a URL/pattern.
CDX_API = "https://web.archive.org/cdx/search/cdx"
WB_BASE = "https://web.archive.org/web"

# Target map: slug/handle per firm. Kept in-module (not panel.yaml) because
# LinkedIn slugs and X handles don't map 1:1 to tickers and are manually
# curated.
LINKEDIN_SLUGS: list[str] = [
    "affirm", "block", "paypal", "sezzle", "zipco", "upstart",
    "klarna",
    "capital-one", "synchrony-financial", "discover-financial-services",
    "american-express", "onemain-financial", "sofi", "lendingclub",
]

X_HANDLES: list[str] = [
    "Affirm", "Klarna", "Afterpay", "PayPal", "Sezzle", "Upstart", "ZipCo",
]

STALENESS_GRACE_DAYS = 30
STALENESS_TAU = 30.0


@dataclass
class Snapshot:
    url: str          # original URL
    ts: datetime      # Wayback snapshot timestamp
    wayback_url: str  # full archived URL we can fetch


def _stale_weight(age_d: int) -> float:
    """Exponential decay past the 30-day grace window (v4.1 §6.1)."""
    over = max(0, age_d - STALENESS_GRACE_DAYS)
    return math.exp(-over / STALENESS_TAU)


# --- Wayback CDX -----------------------------------------------------------
@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=2, max=15))
def _cdx_snapshots(url: str, *, from_date: str = "2019",
                   to_date: str | None = None,
                   collapse: str = "timestamp:8") -> list[Snapshot]:
    """Return list of snapshots for `url`. `collapse=timestamp:8` dedupes to
    roughly one snapshot per day."""
    params = {
        "url":      url,
        "output":   "json",
        "from":     from_date,
        "to":       to_date or datetime.now(timezone.utc).strftime("%Y%m%d"),
        "fl":       "timestamp,original,statuscode",
        "filter":   "statuscode:200",
        "collapse": collapse,
        "limit":    "5000",
    }
    r = httpx.get(CDX_API, params=params, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    if not data or len(data) < 2:
        return []
    # First row is header.
    out: list[Snapshot] = []
    for row in data[1:]:
        try:
            ts_str, orig, _ = row
            ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except Exception:  # noqa: BLE001
            continue
        out.append(Snapshot(
            url=orig,
            ts=ts,
            wayback_url=f"{WB_BASE}/{ts_str}/{orig}",
        ))
    return out


@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=2, max=15))
def _fetch_snapshot_html(snap: Snapshot) -> str:
    r = httpx.get(snap.wayback_url, timeout=30.0,
                  follow_redirects=True,
                  headers={"User-Agent": settings.sec_edgar_ua})
    r.raise_for_status()
    return r.text


# --- Parsers (pure functions — unit-testable offline) ----------------------
# LinkedIn's public company page historically exposes headcount as a band
# ("1,001-5,000 employees") and a "X open jobs" link; these selectors have
# changed multiple times, so we regex defensively.

_HEADCOUNT_BAND = re.compile(
    r"([0-9][0-9,]*)\s*[-\u2013]\s*([0-9][0-9,]*)\s*employees", re.I)
_HEADCOUNT_SINGLE = re.compile(
    r"([0-9][0-9,]{2,})\s*employees\s*(?:on LinkedIn|worldwide)?", re.I)
_OPENINGS = re.compile(r"([0-9][0-9,]*)\s*(?:open\s+jobs|jobs)\b", re.I)
_FOLLOWERS = re.compile(
    r"([0-9][0-9,.]*)\s*(?:K|M|B)?\s*Followers", re.I)


def _to_int(s: str) -> int:
    return int(s.replace(",", ""))


def parse_linkedin_html(html: str) -> dict:
    """Extract {headcount, openings} from archived LinkedIn HTML."""
    # Strip tags for regex robustness; BS4 gives cleaner text if available.
    try:
        from bs4 import BeautifulSoup
        text = BeautifulSoup(html, "html.parser").get_text(separator=" ")
    except ImportError:
        text = re.sub(r"<[^>]+>", " ", html)

    headcount: int | None = None
    m = _HEADCOUNT_BAND.search(text)
    if m:
        lo, hi = _to_int(m.group(1)), _to_int(m.group(2))
        headcount = (lo + hi) // 2
    else:
        m2 = _HEADCOUNT_SINGLE.search(text)
        if m2:
            headcount = _to_int(m2.group(1))

    openings: int | None = None
    m3 = _OPENINGS.search(text)
    if m3:
        openings = _to_int(m3.group(1))

    return {"headcount": headcount, "openings": openings}


def parse_x_html(html: str) -> dict:
    """Extract {followers} from archived X/Twitter HTML."""
    try:
        from bs4 import BeautifulSoup
        text = BeautifulSoup(html, "html.parser").get_text(separator=" ")
    except ImportError:
        text = re.sub(r"<[^>]+>", " ", html)

    m = _FOLLOWERS.search(text)
    if not m:
        return {"followers": None}
    token = m.group(1).upper()
    # Handle "12.3K" / "1.2M" suffix that regex captured as part of number.
    # Re-scan the token region for suffix.
    local = text[max(0, m.start() - 5): m.end() + 5].upper()
    suffix_mult = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    mult = 1
    for s, mv in suffix_mult.items():
        if s in local:
            mult = mv
            break
    try:
        followers = int(float(token.replace(",", "")) * mult)
    except ValueError:
        followers = None
    return {"followers": followers}


# --- Derived signals -------------------------------------------------------
def compute_tenure_slope(headcount: int | None, openings: int | None) -> float | None:
    if not headcount or headcount <= 0 or openings is None:
        return None
    return openings / headcount


def compute_freeze_flag(tenure_slope_series: list[float],
                        headcount_series: list[int],
                        lookback: int = 8) -> bool:
    """Freeze flag = ΔT < -2σ AND headcount change |Δ| < 2% over lookback."""
    if len(tenure_slope_series) < lookback + 1 or len(headcount_series) < 2:
        return False
    recent = tenure_slope_series[-lookback - 1:-1]
    if len(recent) < 2:
        return False
    mean_r = sum(recent) / len(recent)
    var_r = sum((x - mean_r) ** 2 for x in recent) / max(1, len(recent) - 1)
    sigma = var_r ** 0.5
    if sigma == 0:
        return False
    delta = tenure_slope_series[-1] - mean_r
    hc_prev, hc_now = headcount_series[-2], headcount_series[-1]
    hc_flat = hc_prev > 0 and abs(hc_now - hc_prev) / hc_prev < 0.02
    return delta < -2 * sigma and hc_flat


# --- Writer ----------------------------------------------------------------
def _upsert_row(con: duckdb.DuckDBPyConnection, *, slug: str, platform: str,
                observed_at: date, age_d: int,
                headcount: int | None, openings: int | None,
                followers: int | None, tenure_slope: float | None,
                freeze_flag: bool, wayback_url: str) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO firm_vitality
            (slug, platform, observed_at, snapshot_age_d,
             headcount, openings, followers, tenure_slope, freeze_flag,
             stale_weight, wayback_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (slug, platform, observed_at, age_d,
         headcount, openings, followers, tenure_slope, freeze_flag,
         _stale_weight(age_d), wayback_url),
    )


# --- Public entry points ---------------------------------------------------
def ingest_linkedin(slug: str, *, from_date: str = "2019") -> int:
    if settings.offline:
        log.info("offline mode; skipping linkedin %s", slug)
        return 0
    url = f"https://www.linkedin.com/company/{slug}"
    snaps = _cdx_snapshots(url, from_date=from_date)
    log.info("firm_vitality | linkedin:%s | %d snapshots", slug, len(snaps))

    con = duckdb.connect(str(settings.duckdb_path))
    n = 0
    today = datetime.now(timezone.utc).date()
    try:
        # Track history for freeze-flag computation.
        ts_series: list[float] = []
        hc_series: list[int] = []
        for snap in snaps:
            try:
                html = _fetch_snapshot_html(snap)
                parsed = parse_linkedin_html(html)
            except Exception as e:   # noqa: BLE001
                log.warning("firm_vitality | linkedin:%s | %s | fetch fail: %s",
                            slug, snap.ts.date(), e)
                continue
            hc = parsed["headcount"]
            op = parsed["openings"]
            ts = compute_tenure_slope(hc, op)
            if ts is not None:
                ts_series.append(ts)
            if hc is not None:
                hc_series.append(hc)
            freeze = compute_freeze_flag(ts_series, hc_series) if ts is not None else False
            age_d = (today - snap.ts.date()).days
            _upsert_row(
                con, slug=slug, platform="linkedin",
                observed_at=snap.ts.date(), age_d=age_d,
                headcount=hc, openings=op, followers=None,
                tenure_slope=ts, freeze_flag=freeze,
                wayback_url=snap.wayback_url,
            )
            n += 1
    finally:
        con.close()
    return n


def ingest_x(handle: str, *, from_date: str = "2019") -> int:
    if settings.offline:
        log.info("offline mode; skipping x %s", handle)
        return 0
    url = f"https://twitter.com/{handle}"
    snaps = _cdx_snapshots(url, from_date=from_date)
    log.info("firm_vitality | x:%s | %d snapshots", handle, len(snaps))

    con = duckdb.connect(str(settings.duckdb_path))
    n = 0
    today = datetime.now(timezone.utc).date()
    try:
        for snap in snaps:
            try:
                html = _fetch_snapshot_html(snap)
                parsed = parse_x_html(html)
            except Exception as e:   # noqa: BLE001
                log.warning("firm_vitality | x:%s | %s | fetch fail: %s",
                            handle, snap.ts.date(), e)
                continue
            age_d = (today - snap.ts.date()).days
            _upsert_row(
                con, slug=handle.lower(), platform="x",
                observed_at=snap.ts.date(), age_d=age_d,
                headcount=None, openings=None,
                followers=parsed["followers"],
                tenure_slope=None, freeze_flag=False,
                wayback_url=snap.wayback_url,
            )
            n += 1
    finally:
        con.close()
    return n


def ingest_all() -> dict[str, int]:
    results: dict[str, int] = {}
    for slug in LINKEDIN_SLUGS:
        try:
            results[f"linkedin:{slug}"] = ingest_linkedin(slug)
        except Exception as e:   # noqa: BLE001
            log.error("firm_vitality | linkedin:%s | FAILED: %s", slug, e)
            results[f"linkedin:{slug}"] = -1
    for h in X_HANDLES:
        try:
            results[f"x:{h}"] = ingest_x(h)
        except Exception as e:   # noqa: BLE001
            log.error("firm_vitality | x:%s | FAILED: %s", h, e)
            results[f"x:{h}"] = -1
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    summary = ingest_all()
    print("\nFirm-vitality ingest summary:")
    for key, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {key:40s} {n:>5d} snapshots")
