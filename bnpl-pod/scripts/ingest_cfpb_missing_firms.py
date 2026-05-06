"""
Ingest CFPB complaints for the firms in UNIVERSE_25 that aren't yet in the
warehouse (CVNA, CACC, KMX, ACA, LC, ENVA, OPFI, WRLD, BFH, ALLY).

Uses the public CFPB Consumer Complaint Database search API:
  https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/

Pulls in 5,000-row pages, paginates with `frm` offset, inserts ON CONFLICT DO
NOTHING into cfpb_complaints. Idempotent — safe to re-run.

Usage:
    python ingest_cfpb_missing_firms.py
"""
import time, json, sys, urllib.parse
from pathlib import Path
import requests
import duckdb

DB_PATH = Path("C:/Users/siddh/Desktop/spring 2026/580/BNPL/bnpl-pod/data/warehouse.duckdb")
API = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
PAGE_SIZE = 5000  # CFPB API max
HEADERS = {"User-Agent": "BearWatch ingest 1.0", "Accept": "application/json"}

# Exact CFPB company-name strings (verified via aggregations endpoint).
# Maps ticker -> exact company string used in the CFPB database.
TARGETS = {
    "CVNA":  "Carvana Group, LLC",
    "CACC":  "CREDIT ACCEPTANCE CORPORATION",
    "KMX":   "CarMax, Inc.",
    "ACA":   "Americas Car-Mart, Inc.",
    "LC":    "Lending Club Corp",
    "ENVA":  "ENOVA INTERNATIONAL, INC.",
    "OPFI":  "Opportunity Financial, LLC",
    "WRLD":  "World Acceptance Corporation",
    "BFH":   "Bread Financial Holdings, Inc.",
    "ALLY":  "ALLY FINANCIAL INC.",
}


def fetch_page(company: str, frm: int, size: int = PAGE_SIZE):
    """Fetch one page of complaints for `company` starting at offset `frm`.
    The API returns a flat list of hits (not the nested ES envelope)."""
    params = {"company": company, "size": size, "frm": frm, "format": "json", "no_aggs": "true"}
    url = API + "?" + urllib.parse.urlencode(params)
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()  # list of {_id, _source: {...}}


def fetch_total(company: str) -> int:
    """One probe call with size=0&no_aggs=false returns aggregations including total count."""
    params = {"company": company, "size": 0, "no_aggs": "false", "field": "company"}
    url = API + "?" + urllib.parse.urlencode(params)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        return int(data.get("hits", {}).get("total", {}).get("value", 0))
    return 0


def normalise_row(hit: dict) -> dict | None:
    """Convert a single CFPB hit into the warehouse row schema."""
    src = hit.get("_source", {}) if isinstance(hit, dict) else {}
    cid = hit.get("_id") or src.get("complaint_id")
    received = src.get("date_received")
    if not cid or not received:
        return None
    # CFPB returns ISO timestamps like '2022-12-20T12:00:00-05:00' — slice to date
    received_date = received[:10] if isinstance(received, str) else received
    return {
        "complaint_id": str(cid),
        "received_at":  received_date,                         # YYYY-MM-DD
        "product":      src.get("product"),
        "sub_product":  src.get("sub_product"),
        "issue":        src.get("issue"),
        "company":      src.get("company"),
        "narrative":    src.get("complaint_what_happened"),
        "tags":         src.get("tags"),
        "state":        src.get("state"),
    }


def ingest_company(con, ticker: str, company: str) -> tuple[int, int]:
    """Pull all complaints for `company`, upsert into warehouse. Returns (fetched, inserted)."""
    print(f"\n[{ticker}] {company}")
    try:
        total = fetch_total(company)
        print(f"  CFPB reports {total:,} complaints for this company")
    except Exception as e:
        total = 0
        print(f"  (could not fetch total: {e})")

    n_fetched, n_inserted, frm = 0, 0, 0
    while True:
        try:
            page = fetch_page(company, frm)
        except Exception as e:
            print(f"  ! fetch error at frm={frm}: {e}")
            break
        if not isinstance(page, list) or len(page) == 0:
            break
        rows = [r for r in (normalise_row(h) for h in page) if r]
        n_fetched += len(rows)
        # Bulk insert with ON CONFLICT DO NOTHING (idempotent)
        for row in rows:
            try:
                res = con.execute("""
                    INSERT INTO cfpb_complaints
                        (complaint_id, received_at, product, sub_product, issue, company, narrative, tags, state)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (complaint_id) DO NOTHING
                """, (row["complaint_id"], row["received_at"], row["product"],
                      row["sub_product"], row["issue"], row["company"],
                      row["narrative"], row["tags"], row["state"]))
                n_inserted += 1
            except Exception:
                pass
        # Per-page progress
        print(f"  page frm={frm:>6}: fetched={len(page):>5}  total_so_far={n_fetched:>6}" + (f" / {total:,}" if total else ""))
        # Stop conditions: page smaller than PAGE_SIZE means we've reached the end
        if len(page) < PAGE_SIZE:
            break
        if total and (frm + PAGE_SIZE >= total):
            break
        frm += PAGE_SIZE
        time.sleep(0.3)  # be gentle to the API
    return n_fetched, n_inserted


def main():
    if not DB_PATH.exists():
        sys.exit(f"warehouse not found at {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))

    grand_fetched, grand_inserted = 0, 0
    for ticker, company in TARGETS.items():
        # Skip if already populated heavily (>500 rows)
        existing = con.execute(
            "SELECT COUNT(*) FROM cfpb_complaints WHERE UPPER(company) = UPPER(?)",
            (company,)).fetchone()[0]
        if existing >= 500:
            print(f"\n[{ticker}] {company} — already has {existing} rows, skipping")
            continue

        try:
            f, i = ingest_company(con, ticker, company)
            print(f"  -> {ticker}: fetched {f}, inserted {i}")
            grand_fetched += f
            grand_inserted += i
        except KeyboardInterrupt:
            print("\n!! interrupted by user")
            break
        except Exception as e:
            print(f"  !! {ticker} failed: {e}")

    print(f"\n=== TOTAL: fetched {grand_fetched}, inserted {grand_inserted} ===")
    print("\nFinal warehouse counts (>=200 rows, sorted):")
    df = con.execute("""
        SELECT company, COUNT(*) AS n
        FROM cfpb_complaints
        GROUP BY company
        HAVING COUNT(*) >= 200
        ORDER BY n DESC
    """).df()
    print(df.to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
