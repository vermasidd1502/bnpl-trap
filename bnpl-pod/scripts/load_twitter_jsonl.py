"""
Bulk-load Twitter JSONL files into the warehouse twitter_posts table.

Run AFTER the Reddit Common Crawl ingest releases the DB lock.
Reads all .jsonl files in data/twitter_jsonl/, dedupes by tweet_id,
inserts into twitter_posts.
"""
from __future__ import annotations
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb

DB_CANDIDATES = [
    Path("C:/Users/siddh/Desktop/spring 2026/580/BNPL/bnpl-pod/data/warehouse.duckdb"),
    Path("C:/Users/siddh/Desktop/spring 2026/580/BNPL-experimental/bnpl-pod/data/warehouse.duckdb"),
]
DB = next((p for p in DB_CANDIDATES if p.exists()), None)
JSONL_DIR = Path(__file__).parent.parent / "data" / "twitter_jsonl"


def parse_twitter_date(s: str) -> datetime | None:
    """Twitter API uses 'Mon Jul 11 12:34:56 +0000 2022' format."""
    if not s: return None
    try:
        return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
    except (ValueError, TypeError):
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None


def main():
    if DB is None: sys.exit("warehouse not found")
    if not JSONL_DIR.exists(): sys.exit(f"no JSONL dir at {JSONL_DIR}")

    files = sorted(JSONL_DIR.glob("*.jsonl"))
    if not files:
        sys.exit(f"no .jsonl files in {JSONL_DIR}")

    print(f"Loading {len(files)} JSONL files into {DB}")
    con = duckdb.connect(str(DB))

    # Ensure table exists
    con.execute("""CREATE TABLE IF NOT EXISTS twitter_posts (
        tweet_id            VARCHAR PRIMARY KEY,
        created_at          TIMESTAMP,
        user_screen_name    VARCHAR,
        user_followers      INTEGER,
        text                VARCHAR,
        lang                VARCHAR,
        favorite_count      INTEGER,
        retweet_count       INTEGER,
        reply_count         INTEGER,
        quote_count         INTEGER,
        is_retweet          BOOLEAN,
        matched_firm        VARCHAR,
        signal_tier         VARCHAR,
        source              VARCHAR,
        issued_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    n_before = con.execute("SELECT COUNT(*) FROM twitter_posts").fetchone()[0]
    print(f"twitter_posts before: {n_before}")

    total_inserted = 0
    for fp in files:
        per_file = 0
        with open(fp, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                created = parse_twitter_date(r.get("created_at", ""))
                try:
                    con.execute("""INSERT OR IGNORE INTO twitter_posts
                        (tweet_id, created_at, user_screen_name, user_followers, text, lang,
                         favorite_count, retweet_count, reply_count, quote_count, is_retweet,
                         matched_firm, signal_tier, source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (r["tweet_id"], created, r.get("user_screen_name", ""),
                         r.get("user_followers", 0), r.get("text", ""), r.get("lang", ""),
                         r.get("favorite_count", 0), r.get("retweet_count", 0),
                         r.get("reply_count", 0), r.get("quote_count", 0),
                         bool(r.get("is_retweet")), r.get("matched_firm"),
                         r.get("signal_tier"), r.get("source", "")))
                    per_file += 1
                except Exception:
                    pass
        total_inserted += per_file
        print(f"  {fp.name}  +{per_file}")

    n_after = con.execute("SELECT COUNT(*) FROM twitter_posts").fetchone()[0]
    print(f"\ntwitter_posts after:  {n_after}  (+{n_after - n_before})")
    print("Per-tier breakdown:")
    for r in con.execute("""SELECT signal_tier, COUNT(*) FROM twitter_posts
                            GROUP BY signal_tier ORDER BY 2 DESC""").fetchall():
        print(f"  {r[0] or 'null':24s}  {r[1]}")
    print("Per-firm hits (tier 1 + tier 2):")
    for r in con.execute("""SELECT matched_firm, COUNT(*) FROM twitter_posts
                            WHERE matched_firm IS NOT NULL
                            GROUP BY matched_firm ORDER BY 2 DESC""").fetchall():
        print(f"  {r[0]:8s}  {r[1]}")

    date_range = con.execute("SELECT MIN(created_at), MAX(created_at) FROM twitter_posts").fetchone()
    print(f"Date range: {date_range[0]} -> {date_range[1]}")
    con.close()


if __name__ == "__main__":
    main()
