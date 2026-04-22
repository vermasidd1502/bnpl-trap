"""
Apple App Store customer-review ingest (free RSS, no auth).

Feeds into the `app_store_reviews` table, which acts as the retail-voice
pillar of the BSI composite now that Reddit PRAW is blocked on the
developer-registration form. App-store reviews are actually a *cleaner*
distress signal than Reddit for BNPL specifically — people review BNPL
apps when they're furious (charged twice, declined at checkout, sent to
collections) and the signal-to-noise ratio is much higher than generic
r/povertyfinance posts.

Apple exposes up to 500 reviews per app via their customer-reviews RSS
endpoint:
    https://itunes.apple.com/us/rss/customerreviews/id=<app_id>/page=<n>/json
Pages 1..10, 50 reviews per page = 500 most-recent max. No auth, no rate
limit that matters at our scale (the endpoint is cached heavily).

Run with:  python -m data.ingest.app_store_rss
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Iterable

import duckdb
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from data.settings import settings

log = logging.getLogger(__name__)

# Canonical Apple track_ids for the BNPL universe. Verified via the
# iTunes Search API (`itunes.apple.com/search?term=<app>&media=software`).
# The `app_name` slug is the identifier we use downstream — stable across
# iOS app-ID changes.
APPS: list[tuple[str, int]] = [
    ("affirm",   967040652),   # Affirm: Buy now, pay over time
    ("klarna",   1115120118),  # Klarna: Smarter everyday money
    ("afterpay", 1401019110),  # Afterpay: Pay over time
    ("sezzle",   1434922495),  # Sezzle: Buy Now, Pay Later
    ("zip",      1425045070),  # Zip (formerly Quadpay): Buy Now, Pay Later
    ("paypal",   283646709),   # PayPal - Pay, Send, Save  (owns Pay in 4)
    ("cashapp",  711923939),   # Cash App (Block-owned; Afterpay parent)
    ("upstart",  6450968733),  # Upstart: Personal Loans + More
]

_HEADERS = {
    # iTunes throttles requests that look botlike. A plain Mozilla UA is
    # sufficient; they don't fingerprint or enforce a stricter format here.
    "User-Agent": "Mozilla/5.0 (compatible; bnpl-pod-research/0.1)",
}


@retry(reraise=True, stop=stop_after_attempt(4),
       wait=wait_exponential(multiplier=1.0, min=2, max=15))
def _fetch_page(app_id: int, page: int) -> dict:
    """GET one page (50 reviews max) of the customer-reviews RSS feed."""
    url = (f"https://itunes.apple.com/us/rss/customerreviews/"
           f"id={app_id}/sortBy=mostRecent/page={page}/json")
    r = httpx.get(url, timeout=30.0, headers=_HEADERS)
    r.raise_for_status()
    return r.json()


def _parse_entry(e: dict, app_name: str, app_id: int) -> dict | None:
    """Normalize one RSS entry into the `app_store_reviews` row shape.

    Apple wraps every field in `{"label": "..."}`. Ratings come as strings.
    The outer `im:contentType.label` is *always* "Application" because it
    points at the app being reviewed, not at the entry type — we can't
    filter on it. The real marker of a review (vs. an app-metadata stub
    some feeds emit as the first entry) is the presence of `im:rating`.
    """
    try:
        rating_s = (e.get("im:rating", {}) or {}).get("label")
        if not rating_s:
            # No rating = not a review (typically the first-page app stub).
            return None

        review_id = (e.get("id", {}) or {}).get("label")
        if not review_id:
            return None

        author_blob = e.get("author", {}) or {}
        author = (author_blob.get("name", {}) or {}).get("label")
        title = (e.get("title", {}) or {}).get("label")
        body = (e.get("content", {}) or {}).get("label")
        version = (e.get("im:version", {}) or {}).get("label")
        updated = (e.get("updated", {}) or {}).get("label")

        rating = int(rating_s) if rating_s and rating_s.isdigit() else None
        # Updated is ISO-8601 with timezone offset, e.g. '2026-04-17T08:45:41-07:00'
        try:
            created_at = datetime.fromisoformat(updated) if updated else None
        except (ValueError, TypeError):
            created_at = None
        if created_at is None:
            return None

        return {
            "review_id": str(review_id),
            "app_id": str(app_id),
            "app_name": app_name,
            "platform": "ios",
            "author": author,
            "title": title,
            "body": body,
            "rating": rating,
            "version": version,
            "created_at": created_at,
        }
    except Exception as ex:  # noqa: BLE001
        log.warning("parse-skip | %s/%s | %s", app_name, app_id, ex)
        return None


def _iter_reviews(app_name: str, app_id: int,
                  max_pages: int = 10) -> Iterable[dict]:
    """Walk pages 1..max_pages and yield normalized review rows."""
    for page in range(1, max_pages + 1):
        try:
            doc = _fetch_page(app_id, page)
        except httpx.HTTPStatusError as e:
            # Some apps return 503/400 on deep pages — treat as end-of-feed.
            log.info("app_store | %s p%d | HTTP %s — stopping",
                     app_name, page, e.response.status_code)
            return
        except Exception as e:  # noqa: BLE001
            log.warning("app_store | %s p%d | fetch error: %s",
                        app_name, page, e)
            return

        entries = (doc.get("feed", {}) or {}).get("entry", []) or []
        if not entries:
            return
        yielded = 0
        for e in entries:
            row = _parse_entry(e, app_name, app_id)
            if row:
                yield row
                yielded += 1
        # If Apple gave us less than a full page, we've hit the tail.
        if yielded < 40:
            return
        # Gentle pacing — the CDN is fine with 1-2 req/sec but be polite.
        time.sleep(0.4)


def _upsert(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO app_store_reviews
            (review_id, app_id, app_name, platform, author,
             title, body, rating, version, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(
            r["review_id"], r["app_id"], r["app_name"], r["platform"],
            r.get("author"), r.get("title"), r.get("body"),
            r.get("rating"), r.get("version"), r["created_at"],
        ) for r in rows],
    )
    return len(rows)


def ingest_app(app_name: str, app_id: int) -> int:
    if settings.offline:
        log.info("offline mode; skipping app_store %s", app_name)
        return 0
    rows = list(_iter_reviews(app_name, app_id))
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, rows)
    finally:
        con.close()
    log.info("app_store | %-10s id=%-12d | %d reviews",
             app_name, app_id, n)
    return n


def ingest_all() -> dict[str, int]:
    out: dict[str, int] = {}
    for name, aid in APPS:
        try:
            out[name] = ingest_app(name, aid)
        except Exception as e:  # noqa: BLE001
            log.error("app_store | %s | FAILED: %s", name, e)
            out[name] = -1
    return out


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    summary = ingest_all()
    print("\nApp Store review ingest summary:")
    for name, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {name:10s}  {n:>6d}")
