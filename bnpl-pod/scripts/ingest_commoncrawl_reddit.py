"""
Phase 2L — Common Crawl historical Reddit ingest.

Pulls historical Reddit posts mentioning BearWatch's expanded universe of
distress events (BNPL + subprime auto + marketplace lending + credit cards)
from Common Crawl's quarterly archives covering 2022-2025.

Architecture:
  1. For each (target crawl, target subreddit) pair, query the Common Crawl
     CDX index (free, public) for matching reddit URLs.
  2. For each matching URL, fetch ONLY the bytes of that record from the
     WARC file via HTTP Range request (~10-50 KB per fetch).
  3. Parse HTML with BeautifulSoup, extract post metadata.
  4. Filter for distress-event keywords across the expanded firm universe.
  5. Insert into reddit_posts with source='common_crawl' for provenance.

Why this is legal: Common Crawl is a non-profit research crawl operating
under a research-use charter. We are consumers of their public archive,
not scrapers of Reddit directly. The CC TOS explicitly permits this.

Why the cost is ~$0:
  - CC index queries are free (rate-limited, polite ~1 req/sec)
  - WARC byte-range fetches from data.commoncrawl.org are free
  - We only download specific records, not whole WARC files
  - Estimated total bandwidth: ~250 MB across all crawls

Usage:
  python ingest_commoncrawl_reddit.py [--crawls=N]  # N=number of crawls to process
"""
from __future__ import annotations
import argparse
import gzip
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import requests
import duckdb
from bs4 import BeautifulSoup

DB_CANDIDATES = [
    Path("C:/Users/siddh/Desktop/spring 2026/580/BNPL/bnpl-pod/data/warehouse.duckdb"),
    Path("C:/Users/siddh/Desktop/spring 2026/580/BNPL-experimental/bnpl-pod/data/warehouse.duckdb"),
]
DB = next((p for p in DB_CANDIDATES if p.exists()), None)
if DB is None: sys.exit("warehouse not found")

USER_AGENT = "BearWatch-research/1.0 (academic project; siddharth@illinois.edu)"

# ============================================================================
# Target crawls — chosen to cover BearWatch named events 2022-2025
# ============================================================================
TARGET_CRAWLS = [
    # Each crawl ID is "CC-MAIN-{year}-{week}"; CC publishes ~one per month.
    # Selected to bracket key event windows.
    "CC-MAIN-2022-21",   # May 2022    — pre-Klarna markdown
    "CC-MAIN-2022-33",   # Aug 2022    — post-Klarna, pre-CVNA distress
    "CC-MAIN-2022-49",   # Dec 2022    — pre-Affirm layoffs
    "CC-MAIN-2023-06",   # Feb 2023    — Affirm layoff window
    "CC-MAIN-2023-14",   # Apr 2023    — pre-Affirm Q3 delinq
    "CC-MAIN-2023-23",   # Jun 2023    — Affirm Q3 delinq
    "CC-MAIN-2023-40",   # Oct 2023    — CURO buildup
    "CC-MAIN-2024-10",   # Mar 2024    — CURO bankruptcy
    "CC-MAIN-2024-26",   # Jul 2024    — mid-2024
    "CC-MAIN-2024-38",   # Sep 2024    — late 2024
    "CC-MAIN-2024-51",   # Dec 2024    — pre-TRICOLOR
    "CC-MAIN-2025-08",   # Feb 2025    — TRICOLOR window
]

# ============================================================================
# Universe — three-tier keyword strategy for higher historical recall
# ============================================================================
# TIER 1 — UNIQUE firm names (low false-positive rate; no disambiguation needed)
FIRM_UNIQUE = {
    "KLAR":     ["klarna"],
    "CVNA":     ["carvana"],
    "SEZL":     ["sezzle"],
    "AFTR":     ["afterpay"],
    "TRICOLOR": ["tricolor", "tricolor auto"],
    "CACC":     ["credit acceptance", "credit acceptance corp"],
    "KMX":      ["carmax"],
    "CURO":     ["speedy cash", "rapid cash", "curo group"],
    "ENVA":     ["netcredit", "cashnetusa", "enova"],
    "OMF":      ["onemain", "one main financial"],
    "WRLD":     ["world acceptance", "world finance"],
    "OPFI":     ["oppfi", "opploans"],
    "SYF":      ["synchrony bank", "care credit"],
    "BFH":      ["comenity", "bread financial"],
    "LC":       ["lendingclub", "lending club"],
    "UPST":     ["upstart loan", "upstart denied", "upstart bank"],
}

# TIER 2 — AMBIGUOUS firm names (need credit/loan/payment context — handled below)
FIRM_AMBIGUOUS = {
    "AFRM":     ["affirm"],   # also a verb
    "PYPL":     ["paypal credit", "paypal pay later", "paypal pay in 4"],
    "SQ":       ["cash app pay", "afterpay block"],
    "SOFI":     ["sofi", "social finance"],
    "ALLY":     ["ally bank", "ally financial", "ally auto"],
    "COF":      ["capital one", "cap one"],
    "ACA":      ["american credit acceptance"],
}

# TIER 3 — CATEGORY signal terms in CONSUMER VOCABULARY (not industry jargon)
# Reddit users don't say "BNPL" — they say "split payment", "pay over time",
# "klarna it", or just describe their experience with installment products.
CATEGORY_TERMS = {
    "INSTALLMENT_PRODUCT": [
        # How consumers actually describe BNPL
        "split payment", "split into 4", "split into four", "4 payments",
        "four payments", "pay over time", "pay later", "pay-later",
        "installment plan", "installment payment", "store credit plan",
        # Industry terms that DO occasionally show up
        "buy now pay later", "bnpl", "pay in 4", "pay-in-4", "pay in four",
    ],
    "SMALL_DOLLAR_LOAN": [
        # The actual terms people search/post
        "microloan", "micro loan", "micro-loan",
        "small dollar loan", "small-dollar loan",
        "payday loan", "payday advance", "cash advance",
        "title loan", "auto title loan", "car title loan",
        "high interest loan", "high-interest loan",
        "predatory loan", "loan shark", "loan-shark",
    ],
    "SUBPRIME_AUTO": [
        # How users describe subprime auto distress
        "subprime auto", "deep subprime",
        "buy here pay here", "buy-here-pay-here", "bhph",
        "tote the note", "lot car",
        "auto repo", "car repo", "voluntary repo", "voluntary surrender",
        "underwater car loan", "underwater on my car", "upside down on car",
        "self-financed dealer",
    ],
    "CONSUMER_DISTRESS": [
        # Distress-language WITHOUT a firm name
        "behind on payments", "missed a payment", "missed payments",
        "can't afford payment", "cant afford payment",
        "drowning in debt", "buried in debt", "stuck with debt",
        "ruined my credit", "tanked my credit", "destroyed my credit",
        "credit card debt", "minimum payment trap", "debt snowball",
        "delinquency", "delinquent loan", "charge-off", "charged off",
        "in collections", "sent to collections",
        "wage garnishment", "judgement against me", "judgment against me",
    ],
}

# Distress action verbs — when paired with any firm name, strong signal
DISTRESS_CONTEXT = re.compile(
    r"\b(scam|fraud|ripoff|rip[\s-]?off|hidden fee|won['’]?t refund|denied|"
    r"declined|reject|repo|repossess|default|delinquent|charged?[\s-]?off|"
    r"collection|lawsuit|class action|sued|bankrupt|chapter (7|11|13)|"
    r"predatory|usury|interest|late fee|missed payment|hardship|unable to pay|"
    r"behind on|struggling|debt|credit|loan|payment plan|installment|bnpl|"
    r"buy now pay later)\b", re.IGNORECASE)

TARGET_SUBREDDITS = [
    # Personal finance hubs
    "personalfinance", "povertyfinance", "CreditCards", "Bogleheads", "Frugal",
    "financialindependence", "personalloans", "DaveRamsey", "BorrowOrSpend",
    # Markets
    "wallstreetbets", "stocks", "investing", "options", "valueinvesting",
    # BNPL/lender-specific
    "Affirm", "Klarna", "Sezzle", "Afterpay", "Upstart", "SoFi",
    # Auto
    "Carvana", "CarvanaSucks", "UsedCars", "askcarsales", "AutoLoans", "cars",
    # Credit / banking
    "CashApp", "Banking", "CreditScore", "RebuildingCredit",
    # Legal/distress
    "legaladvice", "personalfinance_es", "studentloans", "debt", "debtfree",
]

# Compile fast filters
TIER1_REGEX = re.compile(r"\b(" + "|".join(re.escape(kw) for kws in FIRM_UNIQUE.values()
                                            for kw in kws) + r")\b", re.IGNORECASE)
TIER2_REGEX = re.compile(r"\b(" + "|".join(re.escape(kw) for kws in FIRM_AMBIGUOUS.values()
                                            for kw in kws) + r")\b", re.IGNORECASE)
TIER3_REGEX = re.compile(r"\b(" + "|".join(re.escape(t) for ts in CATEGORY_TERMS.values()
                                            for t in ts) + r")\b", re.IGNORECASE)

# Reverse lookups
TIER1_TO_FIRM = {kw.lower(): firm for firm, kws in FIRM_UNIQUE.items()    for kw in kws}
TIER2_TO_FIRM = {kw.lower(): firm for firm, kws in FIRM_AMBIGUOUS.items() for kw in kws}
TIER3_TO_CAT  = {t.lower(): cat   for cat, ts   in CATEGORY_TERMS.items() for t in ts}


# ============================================================================
# CDX index query (Common Crawl URL -> WARC location)
# ============================================================================
def query_cdx(crawl_id: str, url_pattern: str, limit: int = 1000) -> list[dict]:
    """Hit CC's CDX index for URLs matching the pattern. Free, polite."""
    base = f"https://index.commoncrawl.org/{crawl_id}-index"
    params = {"url": url_pattern, "output": "json", "limit": limit}
    try:
        r = requests.get(base, params=params,
                         headers={"User-Agent": USER_AGENT}, timeout=60)
        if r.status_code != 200:
            return []
        results = []
        for line in r.text.strip().split("\n"):
            line = line.strip()
            if not line: continue
            try:
                results.append(json.loads(line))
            except Exception:
                continue
        return results
    except Exception as e:
        print(f"      CDX error: {e}")
        return []


# ============================================================================
# WARC byte-range fetch (only the bytes for one record)
# ============================================================================
def fetch_warc_record(filename: str, offset: int, length: int) -> str | None:
    """Pull just the gzipped record from S3 via HTTP Range, decompress, return HTML."""
    url = f"https://data.commoncrawl.org/{filename}"
    headers = {
        "User-Agent": USER_AGENT,
        "Range": f"bytes={offset}-{offset + length - 1}",
    }
    try:
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code not in (200, 206):
            return None
        # Each record is gzip-compressed independently
        with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as f:
            data = f.read()
        # WARC structure: headers + \r\n\r\n + HTTP-headers + \r\n\r\n + body
        parts = data.split(b"\r\n\r\n", 2)
        if len(parts) < 3:
            return None
        body = parts[2]
        # Strip any trailing WARC record terminator
        body = body.rsplit(b"\r\n\r\n", 1)[0]
        return body.decode("utf-8", errors="replace")
    except Exception as e:
        return None


# ============================================================================
# Reddit HTML extraction (handles both old.reddit.com and new reddit.com)
# ============================================================================
def extract_reddit_post(html: str, url: str) -> dict | None:
    """Parse a Reddit comment-thread page, return post metadata or None."""
    if not html or len(html) < 500:
        return None
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    body = ""
    score = 0
    num_comments = 0
    created_utc = None
    subreddit = ""

    # Try OLD reddit format first (cleaner HTML)
    title_el = soup.find("a", class_="title")
    if title_el:
        title = title_el.get_text(strip=True)
        # Body
        body_el = soup.find("div", class_="usertext-body")
        if body_el:
            body = body_el.get_text(" ", strip=True)
        # Score
        score_el = soup.find("div", class_="score")
        if score_el and score_el.get("title"):
            try: score = int(score_el["title"])
            except (ValueError, TypeError): pass
        # Subreddit from URL
        m = re.search(r"reddit\.com/r/([^/]+)/", url)
        if m: subreddit = m.group(1)
        # Date: time element
        time_el = soup.find("time")
        if time_el and time_el.get("datetime"):
            try:
                created_utc = datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00"))
            except Exception: pass

    # Fallback: new reddit (less reliable, JS-heavy)
    if not title:
        title_el = soup.find("h1")
        if title_el:
            title = title_el.get_text(strip=True)
        # Try meta tags
        for m in soup.find_all("meta", attrs={"property": "og:title"}):
            if m.get("content"):
                title = m["content"]; break
        for m in soup.find_all("meta", attrs={"property": "og:description"}):
            if m.get("content"):
                body = m["content"]; break

    # Try JSON-LD for structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            j = json.loads(script.string or "{}")
            if isinstance(j, dict):
                if not title and j.get("headline"): title = j["headline"]
                if not body and j.get("articleBody"): body = j["articleBody"]
                date_str = j.get("datePublished") or j.get("dateCreated")
                if date_str and not created_utc:
                    try:
                        created_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    except Exception: pass
        except Exception:
            continue

    if not title:
        return None

    # Extract subreddit from URL if not found
    if not subreddit:
        m = re.search(r"reddit\.com/r/([^/]+)/", url)
        if m: subreddit = m.group(1)

    # Extract post_id from URL
    m = re.search(r"/comments/([a-z0-9]+)/", url)
    post_id = m.group(1) if m else url[-12:]

    return {
        "post_id":      post_id,
        "subreddit":    subreddit,
        "title":        title[:500],
        "body":         body[:2000],
        "score":        score,
        "num_comments": num_comments,
        "url":          url,
        "created_utc":  created_utc,
    }


def matches_universe(text: str) -> tuple[bool, str | None, str | None]:
    """Three-tier match. Returns (matched, firm, signal_kind).

    signal_kind: 'tier1' (high-conf firm match) | 'tier2_disambiguated'
                 (ambiguous firm + distress context) | 'tier3' (category only).
    Tier-2 matches require a distress-context word within 80 chars to count.
    """
    if not text: return False, None, None

    # TIER 1 — unambiguous firm names (always count)
    m = TIER1_REGEX.search(text)
    if m:
        return True, TIER1_TO_FIRM.get(m.group(1).lower()), "tier1"

    # TIER 2 — ambiguous firm names (require distress context)
    m = TIER2_REGEX.search(text)
    if m:
        # Check ±80-char window for a distress/credit context word
        idx = m.start()
        window = text[max(0, idx - 80): idx + 80 + len(m.group(0))]
        if DISTRESS_CONTEXT.search(window):
            return True, TIER2_TO_FIRM.get(m.group(1).lower()), "tier2_disambiguated"

    # TIER 3 — category signal (no firm)
    m = TIER3_REGEX.search(text)
    if m:
        return True, None, "tier3"

    return False, None, None


def matches_full_page(html: str) -> tuple[bool, str | None, str | None]:
    """Match against ALL page text (title + body + comments)."""
    if not html: return False, None, None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    return matches_universe(text)


# ============================================================================
# Main pipeline
# ============================================================================
def process_crawl(con, crawl_id: str, max_records_per_subreddit: int = 200) -> dict:
    """Process one crawl: query CDX for each subreddit, fetch + filter records."""
    stats = {"crawl": crawl_id, "queried": 0, "fetched": 0, "matched": 0, "inserted": 0}
    for sub in TARGET_SUBREDDITS:
        url_pattern = f"https://www.reddit.com/r/{sub}/comments/*"
        results = query_cdx(crawl_id, url_pattern, limit=max_records_per_subreddit)
        stats["queried"] += len(results)
        if not results:
            time.sleep(0.5)
            continue
        print(f"    r/{sub:25s}  CDX: {len(results)} URLs", end="", flush=True)
        local_matched = 0
        local_inserted = 0
        for rec in results:
            html = fetch_warc_record(rec["filename"], int(rec["offset"]), int(rec["length"]))
            stats["fetched"] += 1
            if not html: continue
            # Cheap filter first: does the FULL page (incl. comments) match any tier?
            matched, firm, tier = matches_full_page(html)
            if not matched: continue
            # Now do the more expensive structured-extraction
            post = extract_reddit_post(html, rec["url"])
            if not post: continue
            # Tag the post with tier + firm so the BSI pipeline can weight it
            post["signal_tier"]  = tier
            post["matched_firm"] = firm  # may be None for tier3 (generic distress)
            local_matched += 1
            stats["matched"] += 1
            # Insert
            try:
                created = post["created_utc"] or datetime.now(timezone.utc)
                # Encode tier into credibility so downstream filters can rank
                cred = {"tier1": 0.9, "tier2_disambiguated": 0.7, "tier3": 0.5}.get(post["signal_tier"], 0.5)
                con.execute("""
                    INSERT OR IGNORE INTO reddit_posts
                    (post_id, subreddit, created_at, title, body, score, num_comments, url, issued_at,
                     credibility, signal_tier, matched_firm)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)
                """, (post["post_id"], post["subreddit"], created,
                      post["title"], post["body"], post["score"], post["num_comments"],
                      post["url"], cred, post["signal_tier"], post["matched_firm"]))
                local_inserted += 1
                stats["inserted"] += 1
            except Exception:
                pass
            time.sleep(0.05)   # be polite to S3
        print(f"  -> {local_matched} matched, {local_inserted} new")
        time.sleep(1)   # CDX politeness
    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--crawls", type=int, default=len(TARGET_CRAWLS),
                        help="number of crawls to process (default: all)")
    parser.add_argument("--per-sub", type=int, default=200,
                        help="max records per subreddit per crawl (default 200)")
    args = parser.parse_args()

    print("=" * 78)
    print("PHASE 2L — Common Crawl historical Reddit ingest")
    print("=" * 78)
    print(f"  Warehouse:        {DB}")
    print(f"  Crawls:           {args.crawls} of {len(TARGET_CRAWLS)}")
    print(f"  Subreddits:       {len(TARGET_SUBREDDITS)}")
    print(f"  Tier 1 firms:     {len(FIRM_UNIQUE)}  ({sum(len(v) for v in FIRM_UNIQUE.values())} keywords)")
    print(f"  Tier 2 firms:     {len(FIRM_AMBIGUOUS)}  ({sum(len(v) for v in FIRM_AMBIGUOUS.values())} keywords, distress-context required)")
    print(f"  Tier 3 categories:{len(CATEGORY_TERMS)}  ({sum(len(v) for v in CATEGORY_TERMS.values())} terms, generic distress)")
    print(f"  Per-sub limit:    {args.per_sub}")

    con = duckdb.connect(str(DB))
    n_before = con.execute("SELECT COUNT(*) FROM reddit_posts").fetchone()[0]
    print(f"  reddit_posts before: {n_before}")
    print()

    all_stats = []
    for i, crawl in enumerate(TARGET_CRAWLS[:args.crawls], 1):
        print(f"[{i}/{args.crawls}] Processing {crawl}…")
        try:
            s = process_crawl(con, crawl, max_records_per_subreddit=args.per_sub)
            all_stats.append(s)
            print(f"  -> crawl {crawl}: queried={s['queried']} fetched={s['fetched']} "
                  f"matched={s['matched']} inserted={s['inserted']}")
        except KeyboardInterrupt:
            print("\n  Interrupted; saving progress…")
            break
        except Exception as e:
            print(f"  CRAWL FAILED: {e}")
            continue
        print()

    n_after = con.execute("SELECT COUNT(*) FROM reddit_posts").fetchone()[0]
    cc_count = con.execute("SELECT COUNT(*) FROM reddit_posts WHERE credibility = 0.7").fetchone()[0]
    date_range = con.execute("SELECT MIN(created_at), MAX(created_at) FROM reddit_posts WHERE credibility = 0.7").fetchone()

    print("=" * 78)
    print("INGEST SUMMARY")
    print("=" * 78)
    print(f"  reddit_posts:  {n_before} -> {n_after}  (+{n_after - n_before})")
    print(f"  Common-Crawl-sourced:  {cc_count}")
    print(f"  CC date range:  {date_range[0]}  ->  {date_range[1]}")
    print()
    print("  Per-crawl yield:")
    for s in all_stats:
        print(f"    {s['crawl']:18s}  matched {s['matched']:>4}  inserted {s['inserted']:>4}")
    print()
    print("  Per-tier breakdown of CC-sourced posts:")
    rows = con.execute("""
        SELECT signal_tier, COUNT(*) FROM reddit_posts
        WHERE credibility >= 0.5 AND signal_tier IS NOT NULL
        GROUP BY signal_tier ORDER BY 2 DESC
    """).fetchall()
    for tier, c in rows:
        print(f"    {tier:24s}  {c}")
    print()
    print("  Per-firm hits (tier 1 + tier 2 only):")
    rows = con.execute("""
        SELECT matched_firm, COUNT(*) FROM reddit_posts
        WHERE matched_firm IS NOT NULL
        GROUP BY matched_firm ORDER BY 2 DESC
    """).fetchall()
    for firm, c in rows:
        print(f"    {firm:8s}  {c}")

    con.close()
    print("\nPhase 2L complete.")


if __name__ == "__main__":
    main()
