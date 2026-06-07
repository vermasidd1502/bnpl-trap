"""
Ingest CFPB complaints for the 18 expansion-universe tickers.
=============================================================

The original ingester (bnpl-pod/data/ingest/cfpb.py) was hardcoded to a
12-firm list. The empirical-expansion plan adds 18 more across three buckets
(pure-thesis, high-IG control, thematic adjacent). This script pulls their
CFPB complaint history from 2019 forward and writes into the same warehouse,
so the existing BSI snapshot and conditional backtest immediately pick them up.

Idempotent: re-running re-pulls the same complaints and INSERT OR REPLACE
de-dupes on the existing primary key.

Run:  python ingest_cfpb_new_universe.py
      python ingest_cfpb_new_universe.py --start 2019-01-01
      python ingest_cfpb_new_universe.py --ticker JPM   # just one
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Ensure we can import the existing cfpb module
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data.ingest.cfpb import ingest_company

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest_cfpb_new")


# CFPB `company` field strings as they appear in the database. Verified pattern:
# CFPB uses the legal-entity name in uppercase, sometimes with multiple variants
# for the same firm. Each ticker maps to a LIST of strings -- the script will
# ingest each variant; the warehouse deduplicates by complaint_id.
NEW_UNIVERSE_CFPB_NAMES = {
    # ---- Bucket A: pure-thesis (10 firms) ----
    "CURO": ["CURO GROUP HOLDINGS CORP", "SPEEDY CASH HOLDINGS CORP", "CURO INTERMEDIATE HOLDINGS CORP"],
    "RM":   ["REGIONAL MANAGEMENT CORP.", "REGIONAL FINANCE CORPORATION"],
    "FCFS": ["FIRSTCASH, INC.", "FIRSTCASH HOLDINGS, INC.", "FIRST CASH FINANCIAL SERVICES"],
    "EZPW": ["EZCORP, INC.", "EZCORP INC"],
    "CPSS": ["CONSUMER PORTFOLIO SERVICES, INC.", "CONSUMER PORTFOLIO SERVICES INC"],
    "SLM":  ["SLM CORPORATION", "SALLIE MAE BANK"],
    "NAVI": ["NAVIENT SOLUTIONS, LLC.", "NAVIENT CORPORATION", "NAVIENT, LLC"],
    "RILY": ["B. RILEY FINANCIAL, INC.", "B RILEY SECURITIES, INC."],
    "PROG": ["PROG HOLDINGS, INC.", "PROGRESSIVE LEASING, LLC", "AARON'S, LLC"],
    "OPRT": ["OPORTUN FINANCIAL CORPORATION", "OPORTUN INC", "PROGRESO FINANCIERO, LLC"],

    # ---- Bucket B: high-IG control (5 firms) ----
    "JPM":  ["JPMORGAN CHASE & CO.", "JPMORGAN CHASE BANK NA"],
    "BAC":  ["BANK OF AMERICA, NATIONAL ASSOCIATION", "BANK OF AMERICA CORPORATION"],
    "WFC":  ["WELLS FARGO & COMPANY", "WELLS FARGO BANK, NA"],
    "MA":   ["MASTERCARD INC", "MASTERCARD INTERNATIONAL"],
    "V":    ["VISA INC.", "VISA U.S.A. INC."],

    # ---- Bucket C: thematic adjacent (3 firms) ----
    "COIN": ["COINBASE, INC.", "COINBASE INC"],
    "HOOD": ["ROBINHOOD MARKETS, INC.", "ROBINHOOD FINANCIAL LLC", "ROBINHOOD SECURITIES, LLC"],
    "ABG":  ["ASBURY AUTOMOTIVE GROUP, INC.", "ASBURY AUTO GROUP, INC."],
}


def main(argv: list | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--start", default="2019-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--ticker", default=None,
                   help="single ticker to ingest (default: all 18)")
    p.add_argument("--dry-run", action="store_true",
                   help="show the plan without hitting the CFPB API")
    args = p.parse_args(argv)
    end = args.end or date.today().isoformat()

    targets = {args.ticker: NEW_UNIVERSE_CFPB_NAMES[args.ticker]} \
              if args.ticker else NEW_UNIVERSE_CFPB_NAMES
    if args.ticker and args.ticker not in NEW_UNIVERSE_CFPB_NAMES:
        log.error(f"unknown ticker {args.ticker}; choices: {list(NEW_UNIVERSE_CFPB_NAMES)}")
        return 1

    print("=" * 78)
    print(f"  CFPB NEW-UNIVERSE INGEST  |  window {args.start} -> {end}")
    print("=" * 78)
    total_complaints = 0
    summary: list[tuple[str, str, int]] = []
    for ticker, company_variants in targets.items():
        ticker_total = 0
        for company in company_variants:
            if args.dry_run:
                print(f"  [dry-run] {ticker:<6} <- '{company}'")
                continue
            try:
                n = ingest_company(company, start=args.start, end=end)
                ticker_total += n
                summary.append((ticker, company, n))
                print(f"  {ticker:<6} {company[:45]:<45}  {n:>7,} complaints")
            except Exception as e:
                log.error(f"  {ticker} <- {company}: FAILED: {e}")
                summary.append((ticker, company, -1))
        if not args.dry_run:
            print(f"  {'>>>':<6} {ticker} TOTAL: {ticker_total:>7,} complaints")
            print()
        total_complaints += ticker_total

    print("=" * 78)
    print(f"  GRAND TOTAL: {total_complaints:,} complaints across {len(targets)} tickers")
    print("=" * 78)
    if not args.dry_run:
        # quick coverage report
        import duckdb
        WAREHOUSE = ROOT / "data" / "warehouse.duckdb"
        con = duckdb.connect(str(WAREHOUSE), read_only=True)
        n_companies = con.execute("SELECT COUNT(DISTINCT UPPER(company)) FROM cfpb_complaints").fetchone()[0]
        n_total = con.execute("SELECT COUNT(*) FROM cfpb_complaints").fetchone()[0]
        con.close()
        print(f"  warehouse now: {n_companies} distinct companies, {n_total:,} total complaints")
    return 0


if __name__ == "__main__":
    sys.exit(main())
