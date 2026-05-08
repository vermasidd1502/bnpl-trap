"""
Phase 2M — Internet Archive Twitter Stream Grab ingest (KLARNA event day).

Streams ONE day of the IA Twitter Stream Grab (Jul 11, 2022 = Klarna $46B->$6.7B
markdown announcement day) without ever writing the full tar to disk.

Architecture:
  1. HTTP stream the tar file from archive.org (no full download to disk)
  2. tarfile in pipe mode -- iterate members on the fly
  3. Each member is a .json.bz2 with ~1 minute of 1% Twitter firehose
  4. bz2-decompress + parse JSONL streaming
  5. Filter each tweet against our 3-tier keyword regex (BNPL/auto/credit)
  6. Write matches to JSONL file (no DB lock contention with Reddit ingest)

Bulk-load to DuckDB twitter_posts table is a separate step (load_twitter_jsonl.py)
that runs after the Reddit ingest releases the DB lock.

DISK: <100 MB peak (only the JSONL output)
BANDWIDTH: ~2.5 GB streamed
TIME: ~30-45 minutes wall clock
"""
from __future__ import annotations
import argparse
import bz2
import io
import json
import re
import sys
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Reuse the 3-tier keyword strategy from the Reddit ingest
sys.path.insert(0, str(Path(__file__).parent))
from ingest_commoncrawl_reddit import (   # type: ignore
    FIRM_UNIQUE, FIRM_AMBIGUOUS, CATEGORY_TERMS,
    DISTRESS_CONTEXT, TIER1_REGEX, TIER2_REGEX, TIER3_REGEX,
    TIER1_TO_FIRM, TIER2_TO_FIRM,
)

OUT_DIR = Path(__file__).parent.parent / "data" / "twitter_jsonl"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def matches_universe(text: str) -> tuple[bool, str | None, str | None]:
    """Three-tier match. Returns (matched, firm, signal_kind)."""
    if not text: return False, None, None
    m = TIER1_REGEX.search(text)
    if m:
        return True, TIER1_TO_FIRM.get(m.group(1).lower()), "tier1"
    m = TIER2_REGEX.search(text)
    if m:
        idx = m.start()
        window = text[max(0, idx - 80): idx + 80 + len(m.group(0))]
        if DISTRESS_CONTEXT.search(window):
            return True, TIER2_TO_FIRM.get(m.group(1).lower()), "tier2_disambiguated"
    m = TIER3_REGEX.search(text)
    if m:
        return True, None, "tier3"
    return False, None, None


def stream_day(date_str: str, out_path: Path) -> dict:
    """Stream-process one daily tar file. Returns stats dict.

    date_str format: 'YYYYMMDD' (e.g. '20220711')
    """
    yyyymm = date_str[:6]
    yyyy_mm = f"{date_str[:4]}-{date_str[4:6]}"
    tar_url = (f"https://archive.org/download/archiveteam-twitter-stream-{yyyy_mm}"
               f"/twitter-stream-{date_str}.tar")

    print(f"  streaming {tar_url}")
    print(f"  output:   {out_path}")

    headers = {"User-Agent": "BearWatch-research/1.0 (academic; siddharth@illinois.edu)"}
    stats = {
        "tweets_seen":      0,
        "tweets_matched":   0,
        "tier1":            0,
        "tier2":            0,
        "tier3":            0,
        "members_processed": 0,
        "bytes_downloaded":  0,
        "errors":           0,
        "start_time":       datetime.now(timezone.utc).isoformat(),
    }

    try:
        # Stream the tar file -- no full disk write
        r = requests.get(tar_url, stream=True, headers=headers, timeout=120)
        if r.status_code != 200:
            stats["fatal"] = f"HTTP {r.status_code} on tar download"
            return stats
        # Wrap raw stream so tarfile can pull from it
        r.raw.decode_content = True

        with open(out_path, "w", encoding="utf-8") as out_f:
            with tarfile.open(fileobj=r.raw, mode="r|") as tar:
                for member in tar:
                    if not member.isfile():
                        continue
                    if not member.name.endswith(".json.bz2"):
                        continue
                    stats["members_processed"] += 1
                    try:
                        f = tar.extractfile(member)
                        if f is None: continue
                        compressed = f.read()
                        stats["bytes_downloaded"] += len(compressed)
                        # Decompress and parse JSONL
                        try:
                            decompressed = bz2.decompress(compressed)
                        except Exception:
                            stats["errors"] += 1
                            continue
                        for line in decompressed.split(b"\n"):
                            line = line.strip()
                            if not line: continue
                            stats["tweets_seen"] += 1
                            try:
                                tw = json.loads(line)
                            except Exception:
                                stats["errors"] += 1
                                continue
                            # Skip non-tweet objects (limit notices, deletes, etc.)
                            if "id_str" not in tw and "id" not in tw: continue
                            text = tw.get("text") or ""
                            # Handle extended_tweet for full text
                            ext = tw.get("extended_tweet") or {}
                            if ext.get("full_text"):
                                text = ext["full_text"]
                            # Also include retweeted_status text
                            if not text and tw.get("retweeted_status"):
                                rt = tw["retweeted_status"]
                                rt_ext = rt.get("extended_tweet") or {}
                                text = rt_ext.get("full_text") or rt.get("text", "")

                            matched, firm, tier = matches_universe(text)
                            if not matched: continue

                            stats["tweets_matched"] += 1
                            stats[tier.split("_")[0]] = stats.get(tier.split("_")[0], 0) + 1

                            user = tw.get("user") or {}
                            record = {
                                "tweet_id":         str(tw.get("id_str") or tw.get("id")),
                                "created_at":       tw.get("created_at"),
                                "user_screen_name": user.get("screen_name", ""),
                                "user_followers":   user.get("followers_count", 0),
                                "text":             text[:2000],
                                "lang":             tw.get("lang", ""),
                                "favorite_count":   tw.get("favorite_count", 0),
                                "retweet_count":    tw.get("retweet_count", 0),
                                "reply_count":      tw.get("reply_count", 0),
                                "quote_count":      tw.get("quote_count", 0),
                                "is_retweet":       bool(tw.get("retweeted_status")),
                                "matched_firm":     firm,
                                "signal_tier":      tier,
                                "source":           f"ia_stream_grab_{date_str}",
                            }
                            out_f.write(json.dumps(record) + "\n")

                    except Exception as e:
                        stats["errors"] += 1
                        continue

                    if stats["members_processed"] % 100 == 0:
                        elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(stats["start_time"])).total_seconds()
                        rate = stats["tweets_seen"] / max(elapsed, 1)
                        mb_dl = stats["bytes_downloaded"] / 1024**2
                        print(f"    [{stats['members_processed']:>5} members]  "
                              f"{stats['tweets_seen']:>9} seen  "
                              f"{stats['tweets_matched']:>5} matched  "
                              f"{mb_dl:>6.1f} MB  "
                              f"{rate:>5.0f} tw/sec")

    except requests.Timeout:
        stats["fatal"] = "timeout"
    except Exception as e:
        stats["fatal"] = f"{type(e).__name__}: {e}"

    stats["end_time"] = datetime.now(timezone.utc).isoformat()
    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="20220711",
                        help="Date YYYYMMDD; default 20220711 (Klarna markdown day)")
    args = parser.parse_args()

    print("=" * 78)
    print("PHASE 2M -- Internet Archive Twitter Stream Grab ingest")
    print("=" * 78)
    print(f"  Date:          {args.date}")
    print(f"  Output dir:    {OUT_DIR}")
    print(f"  Universe:      {len(FIRM_UNIQUE)} unique firms + {len(FIRM_AMBIGUOUS)} ambiguous + "
          f"{len(CATEGORY_TERMS)} categories")
    print()

    out_path = OUT_DIR / f"twitter_{args.date}.jsonl"
    t0 = time.time()
    stats = stream_day(args.date, out_path)
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print("STREAM COMPLETE")
    print("=" * 78)
    print(f"  Wall time:        {elapsed/60:.1f} min")
    print(f"  Tweets seen:      {stats.get('tweets_seen', 0):,}")
    print(f"  Tweets matched:   {stats.get('tweets_matched', 0):,}")
    print(f"  Members:          {stats.get('members_processed', 0):,}")
    print(f"  MB downloaded:    {stats.get('bytes_downloaded', 0)/1024**2:.1f}")
    print(f"  Errors:           {stats.get('errors', 0)}")
    if stats.get("fatal"):
        print(f"  FATAL:            {stats['fatal']}")
    print(f"  Tier 1 hits:      {stats.get('tier1', 0)}")
    print(f"  Tier 2 hits:      {stats.get('tier2', 0)}")
    print(f"  Tier 3 hits:      {stats.get('tier3', 0)}")
    print(f"  Output JSONL:     {out_path}  ({out_path.stat().st_size if out_path.exists() else 0} bytes)")
    print()
    print("Bulk-load to DuckDB with: python load_twitter_jsonl.py")


if __name__ == "__main__":
    main()
