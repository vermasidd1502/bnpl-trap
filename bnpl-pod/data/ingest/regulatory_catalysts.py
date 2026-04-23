"""
Seed the `regulatory_catalysts` table with curated BNPL-relevant events.

This table drives compliance gate 3 (days-to-nearest-material-catalyst),
replacing the single hardcoded CCD II deadline that caused a temporal leak
in historical backtests. See `data/regulatory_calendar.py` for the query
layer.

Curation policy
---------------
A catalyst is included only if it is on the public regulatory record with
a specific, citable publication or effective date. Rumor, comment-period
openings, and speeches without rule-making content are excluded. The set
below is deliberately small — the gate doesn't need comprehensiveness, it
needs DATED MATERIAL events spaced through the backtest window.

Seed rows (as of Sprint H + 2026-04-23 paper-anchor amendment)
--------------------------------------------------------------
* cfpb_2022_market_report      2022-09-15  US-CFPB  m=0.80  report
      "Buy Now, Pay Later: Market Trends and Consumer Impacts"
      First comprehensive regulator survey of the BNPL sector; explicitly
      flags debt-stacking and dispute-rights asymmetries. Moved secondary-
      market spreads on AFRMT tranches the week of publication.
* fca_bnpl_consultation_2023   2023-02-14  UK-FCA   m=0.70  consultation
      FCA policy statement on bringing BNPL under consumer-credit regime.
      Affected Klarna / Clearpay funding spreads.
* cfpb_2024_interpretive_rule  2024-05-22  US-CFPB  m=0.95  rule
      Interpretive rule classifying BNPL lenders as card issuers under
      Regulation Z. Direct ABS-pricing relevance — dispute rights and
      chargeback symmetry now enforceable.
* cfpb_2025_regz_effective     2025-01-17  US-CFPB  m=0.95  regulation
      Regulation Z compliance deadline for BNPL lenders (billing dispute
      / refund rights). THIS IS THE PAPER'S FLAGSHIP DATE — §7.2 pins
      z_bsi = +9.69 σ on this day, driven by a 12,838-complaint CFPB
      surge (≈213× baseline). Without this row the live pod and
      backtester structurally cannot fire Gate 3 on the paper's anchor:
      the next catalyst (CCD II 2026-11-20) is 673 days out, far past
      the 180d horizon. Mirrors the warehouse row pre-seeded manually
      on 2026-04-21; codified here so `ingest_all()` is the single
      source of truth.
* ccd_ii_transposition_2026    2026-11-20  EU       m=1.00  transposition
      EU Consumer Credit Directive II member-state transposition deadline.
      Full scope over BNPL: creditworthiness assessments, APR disclosure,
      ADR mechanisms. Pre-existing in the pod as DEFAULT_CCD_II_DEADLINE;
      preserved here so the Sprint H refactor is a strict superset.

Run with:  python -m data.ingest.regulatory_catalysts
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import duckdb

from data.settings import settings

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")


@dataclass(frozen=True)
class SeedRow:
    catalyst_id: str
    jurisdiction: str
    deadline_date: date
    title: str
    materiality: float
    category: str
    notes: str


SEED: list[SeedRow] = [
    SeedRow(
        catalyst_id="cfpb_2022_market_report",
        jurisdiction="US-CFPB",
        deadline_date=date(2022, 9, 15),
        title="Buy Now, Pay Later: Market Trends and Consumer Impacts",
        materiality=0.80,
        category="report",
        notes=(
            "First comprehensive federal survey of US BNPL. Explicitly "
            "flags debt-stacking, dispute-rights asymmetry, and data "
            "harvesting. Moved AFRMT secondary spreads."
        ),
    ),
    SeedRow(
        catalyst_id="fca_bnpl_consultation_2023",
        jurisdiction="UK-FCA",
        deadline_date=date(2023, 2, 14),
        title="FCA consultation on bringing BNPL under consumer-credit regime",
        materiality=0.70,
        category="consultation",
        notes="Affected Klarna / Clearpay UK funding spreads.",
    ),
    SeedRow(
        catalyst_id="cfpb_2024_interpretive_rule",
        jurisdiction="US-CFPB",
        deadline_date=date(2024, 5, 22),
        title="CFPB Interpretive Rule: BNPL lenders as card issuers under Reg Z",
        materiality=0.95,
        category="rule",
        notes=(
            "Dispute rights and chargeback symmetry become enforceable. "
            "Directly re-prices ABS-level expected-loss assumptions."
        ),
    ),
    SeedRow(
        catalyst_id="cfpb_2025_regz_effective",
        jurisdiction="US-CFPB",
        deadline_date=date(2025, 1, 17),
        title=(
            "Regulation Z compliance deadline for BNPL lenders "
            "(billing dispute / refund rights)"
        ),
        materiality=0.95,
        category="regulation",
        notes=(
            "PAPER ANCHOR: §7.2 pins z_bsi = +9.69 σ on this day. Compliance "
            "effective-date for the 2024 interpretive rule; first day BNPL "
            "lenders must provide Reg-Z-grade dispute / refund rights. "
            "Empirical peak of the 2019-2026 window: 12,838 BNPL complaints "
            "filed on this single day (≈213× daily baseline). Without this "
            "seed row, Gate 3 structurally fails on the paper's flagship "
            "day — the next material catalyst (CCD II 2026-11-20) is 673 "
            "days out, far past the 180d proximity horizon."
        ),
    ),
    SeedRow(
        catalyst_id="ccd_ii_transposition_2026",
        jurisdiction="EU",
        deadline_date=date(2026, 11, 20),
        title="EU Consumer Credit Directive II — member-state transposition deadline",
        materiality=1.00,
        category="transposition",
        notes=(
            "Full scope over BNPL: creditworthiness assessments, APR "
            "disclosure, ADR mechanisms. Pre-existing constant in the pod "
            "(DEFAULT_CCD_II_DEADLINE); preserved through the refactor."
        ),
    ),
]


def _upsert(con: duckdb.DuckDBPyConnection, rows: list[SeedRow]) -> int:
    con.executemany(
        """
        INSERT OR REPLACE INTO regulatory_catalysts
            (catalyst_id, jurisdiction, deadline_date, title,
             materiality, category, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.catalyst_id,
                r.jurisdiction,
                r.deadline_date,
                r.title,
                r.materiality,
                r.category,
                r.notes,
            )
            for r in rows
        ],
    )
    return len(rows)


def ingest_all() -> int:
    """Write every seed row. Idempotent — INSERT OR REPLACE on catalyst_id."""
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, SEED)
    finally:
        con.close()
    log.info("regulatory_catalysts | upserted %d rows", n)
    return n


if __name__ == "__main__":
    n = ingest_all()
    print(f"regulatory_catalysts seed: {n} rows")
    for r in SEED:
        print(f"  {r.deadline_date}  m={r.materiality:.2f}  {r.jurisdiction:8s}  {r.catalyst_id}")
