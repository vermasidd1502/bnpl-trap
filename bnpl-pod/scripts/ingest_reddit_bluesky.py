"""
Reddit + Bluesky ingest using PUBLIC endpoints (no credentials required).

HARD CONSTRAINTS the script honestly reports:
  Reddit  — public JSON search returns only the most recent ~6-12 months per
            query. Pre-2024 historical data is NOT recoverable via public
            endpoints (Pushshift was deprecated in 2023). To pull from 2018-2022
            you would need either a paid vendor (Brandwatch / Talkwalker) or
            PRAW credentials with prior archive access.
  Bluesky — public AppView API works without auth. But Bluesky launched
            publicly in February 2024, so no posts exist before then by design.

What this script CAN do today, with no credentials:
  1. Pull recent matching Reddit posts via the public JSON search endpoint
  2. Pull all matching Bluesky posts since Bluesky's public launch
  3. Populate warehouse tables reddit_posts and bluesky_posts
  4. Report counts ingested per firm

USAGE:
  python ingest_reddit_bluesky.py
"""
from __future__ import annotations
import sys
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import requests
import duckdb

DB_CANDIDATES = [
    Path("C:/Users/siddh/Desktop/spring 2026/580/BNPL/bnpl-pod/data/warehouse.duckdb"),
    Path("C:/Users/siddh/Desktop/spring 2026/580/BNPL-experimental/bnpl-pod/data/warehouse.duckdb"),
]
DB = next((p for p in DB_CANDIDATES if p.exists()), None)
if DB is None: sys.exit("warehouse not found")

USER_AGENT = "BearWatch-research/1.0 (academic; siddharth@illinois.edu)"

# Per-firm search keywords. We bias toward distress-context terms because
# those are what BSI actually measures (negative consumer signal).
FIRM_KEYWORDS = {
    "AFRM": ["affirm", "affirm late fee", "affirm declined", "affirm scam"],
    "KLAR": ["klarna", "klarna late fee", "klarna scam"],
    "PYPL": ["paypal late", "paypal pay later", "paypal scam"],
    "SQ":   ["cash app pay", "afterpay block"],
    "SEZL": ["sezzle", "sezzle late"],
    "AFTR": ["afterpay", "afterpay late"],
    "UPST": ["upstart loan", "upstart denied"],
    "SOFI": ["sofi loan", "sofi denied"],
}


# ---------------------------------------------------------------------------
# REDDIT — public JSON endpoint (no auth)
# ---------------------------------------------------------------------------
def reddit_search(query: str, limit: int = 100, sort: str = "new",
                  time_filter: str = "year") -> list:
    """Hit Reddit's public search.json endpoint. No auth needed.

    Honest limitations:
      - Returns up to ~100 posts per call
      - 'time_filter=all' only goes back ~1 year in practice for non-popular queries
      - Rate limit is aggressive without auth — sleep 2s between calls
    """
    url = "https://www.reddit.com/search.json"
    params = {
        "q": query, "limit": limit, "sort": sort,
        "t": time_filter, "restrict_sr": False, "type": "link",
    }
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        if r.status_code == 429:
            print(f"    rate-limited on '{query}', backing off 30s")
            time.sleep(30)
            return []
        if r.status_code != 200:
            print(f"    HTTP {r.status_code} on '{query}'")
            return []
        data = r.json()
        return [c["data"] for c in data.get("data", {}).get("children", [])]
    except requests.Timeout:
        print(f"    timeout on '{query}'")
        return []
    except Exception as e:
        print(f"    error on '{query}': {e}")
        return []


def ingest_reddit(con) -> dict:
    """Pull matching Reddit posts per firm, dedupe, insert into reddit_posts."""
    print("\n=== REDDIT INGEST (public JSON, no auth) ===")
    counts = {}
    for firm, keywords in FIRM_KEYWORDS.items():
        firm_total = 0
        for kw in keywords:
            print(f"  {firm:6s} · '{kw}'", end=" ")
            posts = reddit_search(kw, limit=100, time_filter="year")
            inserted = 0
            for p in posts:
                pid = p.get("id")
                if not pid: continue
                created = datetime.fromtimestamp(p.get("created_utc", 0), tz=timezone.utc)
                try:
                    con.execute("""
                        INSERT OR IGNORE INTO reddit_posts
                        (post_id, subreddit, created_at, title, body, score, num_comments, url, issued_at, credibility)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """, (pid, p.get("subreddit", ""), created, p.get("title", "")[:500],
                          (p.get("selftext", "") or "")[:2000],
                          p.get("score", 0), p.get("num_comments", 0),
                          "https://reddit.com" + p.get("permalink", ""), 0.5))
                    inserted += 1
                except Exception as e:
                    if "already exists" not in str(e).lower(): pass  # silent dedupe
            firm_total += inserted
            print(f"→ {inserted} new")
            time.sleep(2.5)  # polite rate limit
        counts[firm] = firm_total
        print(f"  {firm:6s} total: {firm_total} new posts")
    return counts


# ---------------------------------------------------------------------------
# BLUESKY — public AppView API (no auth)
# ---------------------------------------------------------------------------
def bluesky_search(query: str, limit: int = 100, cursor: str = None) -> tuple[list, str]:
    """Hit Bluesky's public searchPosts endpoint. No auth needed.

    https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts
    """
    url = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
    params = {"q": query, "limit": min(limit, 100)}
    if cursor: params["cursor"] = cursor
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            print(f"    HTTP {r.status_code} on '{query}'")
            return [], None
        data = r.json()
        return data.get("posts", []), data.get("cursor")
    except Exception as e:
        print(f"    error on '{query}': {e}")
        return [], None


def ingest_bluesky(con) -> dict:
    """Pull matching Bluesky posts per firm, paginate, insert into bluesky_posts."""
    print("\n=== BLUESKY INGEST (public AT Protocol, no auth) ===")
    counts = {}
    for firm, keywords in FIRM_KEYWORDS.items():
        firm_total = 0
        for kw in keywords:
            print(f"  {firm:6s} · '{kw}'", end=" ")
            cursor = None
            kw_total = 0
            for page in range(5):  # up to 500 posts per keyword
                posts, cursor = bluesky_search(kw, limit=100, cursor=cursor)
                if not posts: break
                inserted = 0
                for p in posts:
                    uri = p.get("uri")
                    if not uri: continue
                    record = p.get("record", {}) or {}
                    author = p.get("author", {}) or {}
                    created_str = record.get("createdAt")
                    try:
                        created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                    except Exception:
                        continue
                    try:
                        con.execute("""
                            INSERT OR IGNORE INTO bluesky_posts
                            (post_uri, cid, author_handle, author_did, created_at, text,
                             like_count, repost_count, reply_count, matched_keyword, matched_firm)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (uri, p.get("cid", ""),
                              author.get("handle", ""), author.get("did", ""),
                              created, (record.get("text", "") or "")[:2000],
                              p.get("likeCount", 0), p.get("repostCount", 0),
                              p.get("replyCount", 0), kw, firm))
                        inserted += 1
                    except Exception:
                        pass
                kw_total += inserted
                if not cursor: break
                time.sleep(1)
            firm_total += kw_total
            print(f"→ {kw_total} new")
            time.sleep(0.5)
        counts[firm] = firm_total
        print(f"  {firm:6s} total: {firm_total} new posts")
    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 78)
    print("REDDIT + BLUESKY INGEST — public endpoints, no credentials needed")
    print("=" * 78)
    print(f"  Warehouse: {DB}")
    print(f"  Firms:     {len(FIRM_KEYWORDS)}")
    print(f"  Keywords:  {sum(len(v) for v in FIRM_KEYWORDS.values())}")

    con = duckdb.connect(str(DB))
    try:
        # Pre-existing counts
        n_reddit_before  = con.execute("SELECT COUNT(*) FROM reddit_posts").fetchone()[0]
        n_bluesky_before = con.execute("SELECT COUNT(*) FROM bluesky_posts").fetchone()[0]
        print(f"  Before: reddit={n_reddit_before}  bluesky={n_bluesky_before}")

        reddit_counts  = ingest_reddit(con)
        bluesky_counts = ingest_bluesky(con)

        n_reddit_after  = con.execute("SELECT COUNT(*) FROM reddit_posts").fetchone()[0]
        n_bluesky_after = con.execute("SELECT COUNT(*) FROM bluesky_posts").fetchone()[0]

        # Date-range coverage check
        try:
            r_range = con.execute("SELECT MIN(created_at), MAX(created_at) FROM reddit_posts").fetchone()
            b_range = con.execute("SELECT MIN(created_at), MAX(created_at) FROM bluesky_posts").fetchone()
        except Exception:
            r_range = b_range = (None, None)

        print()
        print("=" * 78)
        print("INGEST SUMMARY")
        print("=" * 78)
        print(f"  Reddit:   {n_reddit_before:>5} → {n_reddit_after:>5} (+{n_reddit_after - n_reddit_before})")
        print(f"            date range: {r_range[0]} → {r_range[1]}")
        print(f"  Bluesky:  {n_bluesky_before:>5} → {n_bluesky_after:>5} (+{n_bluesky_after - n_bluesky_before})")
        print(f"            date range: {b_range[0]} → {b_range[1]}")
        print()
        print("  Per-firm Reddit:")
        for f, c in reddit_counts.items(): print(f"    {f:6s}  {c:>4}")
        print("  Per-firm Bluesky:")
        for f, c in bluesky_counts.items(): print(f"    {f:6s}  {c:>4}")

        print()
        print("=" * 78)
        print("HONEST COVERAGE ASSESSMENT")
        print("=" * 78)
        if r_range[0] and r_range[0].year >= 2024:
            print("  ⚠ Reddit historical depth is < 2 years.")
            print("    Pre-2024 events (KLAR-2022-07, AFRM-2023-02, AFRM-2023-05) cannot be")
            print("    covered by Reddit using public endpoints. To get historical Reddit:")
            print("      - Register a Reddit app at https://www.reddit.com/prefs/apps")
            print("      - Use PRAW with credentials (still limited to ~1 year per query)")
            print("      - OR pay a vendor (Brandwatch / Talkwalker / Crimson Hexagon)")
        if b_range[0] and b_range[0].year >= 2024:
            print("  ⚠ Bluesky data only exists from Feb 2024 onward by design.")
            print("    Only TRICOLOR-2025-03 from our event universe is in Bluesky's window.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
