"""
Refresh apollo.db.bsi_snapshot from the BNPL warehouse.
=======================================================

Mirrors the pod's _get_live_cfpb_signals() logic (apollo-hermes/working_demo/
app.py L4537), computes per-firm distress z-equivalent from CFPB complaint
acceleration, and upserts into apollo.db.bsi_snapshot.

This is the closed-loop fix for Gap 2: without this writer, the risk engine
defaults every name to conviction 0.5 -- so the Maillard conviction-tilted
allocation is mathematically inert. After this lands, conviction reflects
real per-firm distress.

Tables touched:
  bnpl-pod warehouse.duckdb (read):  cfpb_complaints
  apollo.db (write):                 bsi_snapshot (upsert), bsi_history (append)

Run as part of daily_driver.py after refresh_bsi.py.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths -- match the rest of the BNPL toolchain
# ---------------------------------------------------------------------------

import os

WAREHOUSE = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"
APOLLO_DB = Path(os.environ.get(
    "APOLLO_DB",
    str(Path(__file__).resolve().parents[1] / "data" / "apollo.db"),
))

# Mirror UNIVERSE_KEYWORDS from app.py L4554 plus 2026-06-06 cross-section
# expansion (12 -> ~38 firms) to push the empirical confusion matrix from
# anecdotal (Wilson CI [4%, 64%]) to non-vacuous ([12%, 47%]).
UNIVERSE_KEYWORDS = {
    # ---- Original 21 (mirror of app.py L4554) ----
    # BNPL
    "AFRM": ["AFFIRM"],
    "SEZL": ["SEZZLE"],
    "PYPL": ["PAYPAL"],
    "KLAR": ["KLARNA"],
    "XYZ":  ["BLOCK, INC", "BLOCK INC", "SQUARE"],
    # Subprime auto
    "CVNA": ["CARVANA"],
    "CACC": ["CREDIT ACCEPTANCE"],
    "KMX":  ["CARMAX"],
    "CRMT": ["CAR-MART", "CAR MART"],
    # Marketplace / fintech subprime
    "UPST": ["UPSTART"],
    "LC":   ["LENDING CLUB", "LENDINGCLUB"],
    "SOFI": ["SOFI"],
    "OPFI": ["OPPORTUNITY FINANCIAL", "OPPFI"],
    "ENVA": ["ENOVA"],
    "WRLD": ["WORLD ACCEPTANCE"],
    "OMF":  ["ONEMAIN"],
    # Card / bank
    "COF":  ["CAPITAL ONE"],
    "SYF":  ["SYNCHRONY"],
    "BFH":  ["BREAD FINANCIAL"],
    "ALLY": ["ALLY FINANCIAL"],
    "AXP":  ["AMERICAN EXPRESS"],

    # ---- Bucket A: pure-thesis expansion (10 firms) ----
    "CURO": ["CURO", "SPEEDY CASH", "RAPID CASH"],
    "RM":   ["REGIONAL MANAGEMENT", "REGIONAL FINANCE"],
    "FCFS": ["FIRSTCASH", "FIRST CASH"],
    "EZPW": ["EZCORP", "EZPAWN", "EZ PAWN"],
    "CPSS": ["CONSUMER PORTFOLIO SERVICES"],
    "SLM":  ["SALLIE MAE", "SLM CORP"],
    "NAVI": ["NAVIENT"],
    "RILY": ["B. RILEY", "B RILEY", "BRILEY"],
    "PROG": ["PROG LEASING", "PROGRESSIVE LEASING", "AARON'S"],
    "OPRT": ["OPORTUN", "PROGRESO FINANCIERO"],

    # ---- Bucket B: high-IG control (5 firms; should NOT fire) ----
    "JPM":  ["JPMORGAN", "JP MORGAN", "CHASE"],
    "BAC":  ["BANK OF AMERICA"],
    "WFC":  ["WELLS FARGO"],
    "MA":   ["MASTERCARD"],
    "V":    ["VISA INC", "VISA U.S.A."],

    # ---- Bucket C: thematic adjacent (3 firms) ----
    "COIN": ["COINBASE"],
    "HOOD": ["ROBINHOOD"],
    "ABG":  ["ASBURY AUTO"],
}

# CFPB publishes with a 45-60 day lag. Match the pod's settle window so signals
# are directly comparable.
SETTLE_LAG_DAYS = 60


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_DDL = [
    """CREATE TABLE IF NOT EXISTS bsi_snapshot (
        ticker        TEXT PRIMARY KEY,
        ts            TEXT NOT NULL,
        z_score       REAL,
        z_trend       TEXT,
        archetype     TEXT,
        conviction    REAL,
        delta_z_5d    REAL
    )""",
    """CREATE TABLE IF NOT EXISTS bsi_history (
        ticker        TEXT NOT NULL,
        ts            TEXT NOT NULL,
        z_score       REAL,
        PRIMARY KEY (ticker, ts)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_bsi_history_ts ON bsi_history(ts)""",
]


def migrate(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    for stmt in SCHEMA_DDL:
        cur.execute(stmt)
    con.commit()


# ---------------------------------------------------------------------------
# Pure math -- same mapping as the live pod
# ---------------------------------------------------------------------------

def cfpb_delta_to_z(delta_pct: float) -> float:
    """delta% in CFPB complaint volume -> z-equivalent.

    Mirrors app.py L4543-L4547 exactly:
        delta < 0    -> 0       (improving: no stress)
        0..30%       -> delta/15 (gate threshold @ z=2 hits at +30%)
        30..60%      -> delta/15 (z=2..4)
        >= 60%       -> 4.0      (saturated)
    """
    if delta_pct is None or delta_pct < 0:
        return 0.0
    if delta_pct >= 60.0:
        return 4.0
    return delta_pct / 15.0


def archetype_from_z(z: float) -> str:
    """Map current z to the archetype that would fire on it.

    Per BSI gate spec:
      z >= 2.5  GUARDIAN
      z >= 2.0  SCOUT
      z >= 1.5  BLITZ
      else      (no archetype fires; pick SCOUT as the default for sizing math)
    """
    if z >= 2.5:
        return "GUARDIAN"
    if z >= 2.0:
        return "SCOUT"
    if z >= 1.5:
        return "BLITZ"
    return "SCOUT"


def conviction_from_z(z: float, z_floor: float = 1.0, z_ceiling: float = 4.0) -> float:
    """Map z to a [0,1] conviction. Matches risk_engine.conviction_from_z."""
    if z is None:
        return 0.0
    z = max(0.0, z)
    if z <= z_floor:
        return 0.0
    if z >= z_ceiling:
        return 1.0
    return (z - z_floor) / (z_ceiling - z_floor)


def z_trend(delta_z_5d: float | None, threshold: float = 0.3) -> str:
    if delta_z_5d is None:
        return "stable"
    if delta_z_5d > threshold:
        return "rising"
    if delta_z_5d < -threshold:
        return "falling"
    return "stable"


# ---------------------------------------------------------------------------
# Compute -- per-firm CFPB acceleration -> z
# ---------------------------------------------------------------------------

def compute_per_firm_z(warehouse_path: Path) -> dict[str, dict]:
    """Returns {ticker: {n_recent_90d, n_prior_90d, delta_pct, z, as_of}}."""
    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        rows = con.execute(f"""
            WITH bounds AS (
                SELECT (SELECT MAX(received_at) FROM cfpb_complaints)
                       - INTERVAL '{SETTLE_LAG_DAYS} days' AS settle_end
            ),
            recent AS (
                SELECT UPPER(company) AS company, COUNT(*) AS n
                FROM cfpb_complaints, bounds
                WHERE received_at >  bounds.settle_end - INTERVAL '90 days'
                  AND received_at <= bounds.settle_end
                GROUP BY UPPER(company)
            ),
            prior AS (
                SELECT UPPER(company) AS company, COUNT(*) AS n
                FROM cfpb_complaints, bounds
                WHERE received_at >  bounds.settle_end - INTERVAL '180 days'
                  AND received_at <= bounds.settle_end - INTERVAL '90 days'
                GROUP BY UPPER(company)
            )
            SELECT
                COALESCE(r.company, p.company) AS company,
                COALESCE(r.n, 0) AS n_recent,
                COALESCE(p.n, 0) AS n_prior,
                (SELECT settle_end FROM bounds) AS settle_end
            FROM recent r FULL OUTER JOIN prior p USING (company)
        """).fetchall()
        # build (company_substring -> ticker) reverse map
        by_company = {company: (n_recent, n_prior, settle_end) for
                      company, n_recent, n_prior, settle_end in rows}
    finally:
        con.close()

    out: dict[str, dict] = {}
    settle_end = None
    for ticker, keywords in UNIVERSE_KEYWORDS.items():
        n_recent = n_prior = 0
        for kw in keywords:
            kw_upper = kw.upper()
            # match any warehouse company string that contains this keyword
            for company, (nr, np_, se) in by_company.items():
                if kw_upper in company:
                    n_recent += nr
                    n_prior += np_
                    if settle_end is None:
                        settle_end = se
        if n_prior > 0:
            delta_pct = 100.0 * (n_recent - n_prior) / n_prior
        elif n_recent > 0:
            delta_pct = 100.0  # no prior baseline, treat as max acceleration
        else:
            delta_pct = 0.0
        z = cfpb_delta_to_z(delta_pct)
        out[ticker] = {
            "n_recent_90d": n_recent,
            "n_prior_90d":  n_prior,
            "delta_pct":    delta_pct,
            "z":            z,
            "as_of":        settle_end.isoformat() if settle_end else None,
        }
    return out


# ---------------------------------------------------------------------------
# Persist
# ---------------------------------------------------------------------------

def upsert_snapshot(
    con: sqlite3.Connection,
    per_firm: dict[str, dict],
    *,
    dry_run: bool = False,
) -> list[tuple]:
    """Write current snapshot + append history. Returns audit rows."""
    cur = con.cursor()
    today_iso = date.today().isoformat()
    audit: list[tuple] = []

    # Compute delta_z_5d from history table (z today minus z ~5 days ago)
    for tk, m in per_firm.items():
        z = float(m["z"])
        prior = cur.execute(
            "SELECT z_score FROM bsi_history WHERE ticker=? AND ts < date(?, '-3 days') "
            "ORDER BY ts DESC LIMIT 1",
            (tk, today_iso),
        ).fetchone()
        delta_z_5d = (z - float(prior[0])) if prior and prior[0] is not None else None
        trend = z_trend(delta_z_5d)
        arch = archetype_from_z(z)
        conv = conviction_from_z(z)

        if not dry_run:
            cur.execute(
                """INSERT OR REPLACE INTO bsi_snapshot
                   (ticker, ts, z_score, z_trend, archetype, conviction, delta_z_5d)
                   VALUES (?,?,?,?,?,?,?)""",
                (tk, today_iso, z, trend, arch, conv, delta_z_5d),
            )
            cur.execute(
                """INSERT OR REPLACE INTO bsi_history (ticker, ts, z_score)
                   VALUES (?,?,?)""",
                (tk, today_iso, z),
            )
        audit.append((tk, z, trend, arch, conv, delta_z_5d,
                      m["n_recent_90d"], m["n_prior_90d"], m["delta_pct"]))
    if not dry_run:
        con.commit()
    return audit


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--warehouse", default=str(WAREHOUSE))
    p.add_argument("--apollo-db", default=str(APOLLO_DB))
    p.add_argument("--dry-run", action="store_true", help="compute & display, no writes")
    args = p.parse_args(argv)

    wh = Path(args.warehouse)
    db = Path(args.apollo_db)
    if not wh.exists():
        print(f"ERROR: warehouse not found at {wh}", file=sys.stderr)
        return 1
    if not db.exists():
        print(f"ERROR: apollo.db not found at {db}", file=sys.stderr)
        return 1

    per_firm = compute_per_firm_z(wh)
    con = sqlite3.connect(str(db))
    migrate(con)
    audit = upsert_snapshot(con, per_firm, dry_run=args.dry_run)

    # Display
    print("=" * 86)
    print(f"  BSI SNAPSHOT REFRESH  |  {datetime.now().isoformat(timespec='seconds')}"
          f"  |  apollo.db={db.name}  {'(DRY-RUN)' if args.dry_run else ''}")
    print("=" * 86)
    print(f"  {'TKR':<6} {'z':>6} {'trend':<8} {'arch':<9} {'conv':>6} {'dz5':>7} "
          f"{'recent':>7} {'prior':>7} {'delta%':>8}")
    print("-" * 86)
    for tk, z, trend, arch, conv, dz5, nr, np_, dpct in sorted(audit, key=lambda r: -r[1]):
        dz5_str = f"{dz5:+.2f}" if dz5 is not None else "-"
        print(f"  {tk:<6} {z:>6.2f} {trend:<8} {arch:<9} {conv:>6.2f} {dz5_str:>7} "
              f"{nr:>7d} {np_:>7d} {dpct:>+7.1f}%")
    print("-" * 86)
    fired = [r for r in audit if r[1] >= 1.5]
    print(f"  {len(fired)}/{len(audit)} tickers above BLITZ threshold (z>=1.5)")
    print(f"  {len([r for r in audit if r[1] >= 2.0])} above SCOUT (z>=2.0)")
    print(f"  {len([r for r in audit if r[1] >= 2.5])} above GUARDIAN (z>=2.5)")
    if not args.dry_run:
        print(f"  bsi_snapshot rows: {len(audit)}    bsi_history rows appended: {len(audit)}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
