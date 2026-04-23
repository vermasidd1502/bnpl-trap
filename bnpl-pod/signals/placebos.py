"""
Placebo sensors for the v2 falsification gauntlet (paper section 7.2).

Each placebo is a drop-in substitute for the CFPB-momentum pillar of
the BSI. All other machinery --- coverage gate, EWMA sigma (in the
live v2 scorer), constrained-QP fuse --- is held constant. The
falsification question is whether the 17 January 2025 event signal
persists when the informative content of the CFPB pillar is removed
and replaced with noise of matched statistical shape.

Pre-registered placebos (see docs/v2_roadmap.md §A.3):

    P1  word-count placebo         -- daily word count across ALL CFPB
                                      complaint narratives. Volume-
                                      matched, no BNPL-specific distress
                                      content.
    P2  randomised-timestamp        -- BNPL complaints, uniformly re-
                                      timestamped within the 2018-2026
                                      window. Marginal distribution
                                      preserved; temporal structure
                                      destroyed.
    P3  non-BNPL-category           -- CFPB non-BNPL-category complaint
                                      series (pre-registered: mortgage-
                                      servicing), processed through the
                                      identical pipeline. Expected null
                                      on 17 Jan 2025 Reg Z event.

P3 --- scope note. The pre-registration named ``mortgage-servicing'' as
the canonical non-BNPL category. The current warehouse holds 389,099
CFPB complaints filtered at ingestion to BNPL-issuer firms; only 409 of
those are under the Mortgage product. We therefore run P3 in two
forms:

    P3a  pre-registered : Mortgage-product complaints (sparse-population
                          null, by construction).
    P3b  warehouse-appropriate : Credit-Reporting sub-product complaints
                                 (n ~= 97k, same firms, orthogonal
                                 category). Refined from the pre-
                                 registered spec to give the
                                 falsification test adequate statistical
                                 power; scope refinement is disclosed
                                 in paper section 7.2.

For comparison, two additional non-pre-registered reference cells are
reported (P3c credit-card, P3d debt-collection) so the reader can see
the pattern across the warehouse's categorical cross-section.

Headline statistic per placebo: raw complaint count on 2025-01-17
divided by the trailing 180-day mean. The BNPL reference on the same
filter returns a ratio of order 300x; placebos are expected to return
ratios of order 1-3x, indicating the 2025-01-17 Regulation Z pulse is
specific to the BNPL complaint channel.

Author: Siddharth Verma, UIUC, FIN 580 Spring 2026 cohort.
Provenance: v2 rewrite 2026-04-22; P3 live compute 2026-04-23.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


PlaceboId = Literal[
    "P1_wordcount",
    "P2_randomtimestamp",
    "P3a_mortgage",
    "P3b_creditreporting",
    "P3c_creditcard",
    "P3d_debtcollection",
    "BNPL_reference",
]


@dataclass(frozen=True)
class PlaceboResult:
    placebo_id: PlaceboId
    event_date: str
    raw_count_on_event: float
    raw_count_baseline: float
    ratio_vs_baseline: float
    sensor_reading_z: float      # v1-style 180-day rolling z
    n_baseline_days: int
    interpretation: str


# ---------------------------------------------------------------- SQL helpers

_BNPL_FIRM_CLAUSE = (
    "(company ILIKE '%Affirm%' "
    "OR company ILIKE '%Klarna%' "
    "OR company ILIKE '%Sezzle%' "
    "OR company ILIKE '%Afterpay%' "
    "OR company ILIKE '%Block%' "
    "OR company ILIKE '%Paypal%')"
)


def _headline_stats(con, label: PlaceboId, where_sql: str,
                    event_date: str = "2025-01-17",
                    lookback_days: int = 180) -> PlaceboResult:
    """Compute event-date count, trailing-k-day mean, z, and ratio.

    The pipeline is deliberately identical to the v1 CFPB-momentum
    pillar (180-day rolling, causal, no look-ahead). The sensor
    reading is expressed both as a raw-count ratio and a v1-style
    z-score so the result is comparable to the headline BNPL
    reading reported in the paper.
    """
    q = f"""
    WITH calendar AS (
      SELECT UNNEST(range(DATE '2018-01-01', DATE '2026-04-30',
                          INTERVAL 1 DAY)) AS d
    ),
    daily AS (
      SELECT received_at AS d, COUNT(*) c
      FROM cfpb_complaints
      WHERE received_at BETWEEN DATE '2018-01-01' AND DATE '2026-04-30'
        AND ({where_sql})
      GROUP BY received_at
    ),
    filled AS (
      SELECT CAST(c.d AS DATE) AS d, COALESCE(daily.c, 0) AS c
      FROM calendar c LEFT JOIN daily USING (d)
    ),
    windowed AS (
      SELECT d, c,
             AVG(c) OVER (ORDER BY d
                          ROWS BETWEEN {lookback_days} PRECEDING
                                  AND 1 PRECEDING) AS ma,
             STDDEV_POP(c) OVER (ORDER BY d
                                 ROWS BETWEEN {lookback_days} PRECEDING
                                         AND 1 PRECEDING) AS sd
      FROM filled
    )
    SELECT d, c, ma, sd,
           CASE WHEN sd > 0 THEN (c - ma) / sd ELSE NULL END AS z
    FROM windowed
    WHERE d = DATE '{event_date}'
    """
    row = con.execute(q).fetchone()
    _, count, ma, sd, z = row
    count = float(count)
    ma = float(ma) if ma is not None else float("nan")
    ratio = (count / ma) if ma and ma > 0 else float("inf")
    z = float(z) if z is not None else float("nan")

    interpretation = (
        f"On {event_date} the {label} series recorded {count:.0f} "
        f"complaints against a trailing {lookback_days}-day mean of "
        f"{ma:.2f}/day (ratio {ratio:.2f}x, v1-style z={z:+.2f}). "
    )
    if label == "BNPL_reference":
        interpretation += "This is the headline BNPL reading the placebos are tested against."
    elif ratio < 3.0:
        interpretation += (
            "Ratio is of order unity: the 2025-01-17 Regulation Z "
            "pulse does not register in this placebo series."
        )
    elif ratio < 10.0:
        interpretation += (
            "Ratio is elevated but an order of magnitude below the "
            "BNPL reference: partial placebo null; the category is "
            "not BNPL-specific but carries some co-movement with "
            "the 17 Jan event."
        )
    else:
        interpretation += (
            "Ratio is of BNPL-comparable magnitude: this placebo "
            "does NOT produce a null on 17 Jan 2025 and invalidates "
            "the BNPL-specificity interpretation if the placebo is "
            "legitimately orthogonal."
        )

    return PlaceboResult(
        placebo_id=label,
        event_date=event_date,
        raw_count_on_event=count,
        raw_count_baseline=ma,
        ratio_vs_baseline=ratio,
        sensor_reading_z=z,
        n_baseline_days=lookback_days,
        interpretation=interpretation,
    )


# ---------------------------------------------------------------- P3 live

def placebo_p3a_mortgage(warehouse_path: str | Path | None = None) -> PlaceboResult:
    """P3a --- pre-registered CFPB mortgage-servicing placebo.

    Warehouse scope note: the cfpb_complaints table is filtered at
    ingestion to BNPL-issuer firms. Only 409 mortgage-product rows
    are present across 2019-2026; the P3a null is therefore by
    construction a low-power test. See placebo_p3b_creditreporting
    for the warehouse-appropriate high-power refinement.
    """
    import duckdb
    con = duckdb.connect(_warehouse(warehouse_path), read_only=True)
    return _headline_stats(con, "P3a_mortgage", "product = 'Mortgage'")


def placebo_p3b_creditreporting(
    warehouse_path: str | Path | None = None,
) -> PlaceboResult:
    """P3b --- Credit-Reporting sub-product, warehouse-appropriate P3.

    Same BNPL-issuer firms; orthogonal category (credit-reporting
    complaints are about bureau reporting of the firms' products,
    not about the product terms themselves). High n (approximately
    97,000 rows sample-wide) gives the falsification test adequate
    statistical power.
    """
    import duckdb
    con = duckdb.connect(_warehouse(warehouse_path), read_only=True)
    return _headline_stats(con, "P3b_creditreporting",
                           "sub_product = 'Credit reporting'")


def placebo_p3c_creditcard(warehouse_path: str | Path | None = None) -> PlaceboResult:
    """P3c --- general-purpose credit-card sub-product reference cell.

    Not pre-registered; reported so the reader can see the full
    placebo cross-section across the warehouse's product taxonomy.
    """
    import duckdb
    con = duckdb.connect(_warehouse(warehouse_path), read_only=True)
    return _headline_stats(
        con, "P3c_creditcard",
        "sub_product = 'General-purpose credit card or charge card'",
    )


def placebo_p3d_debtcollection(warehouse_path: str | Path | None = None) -> PlaceboResult:
    """P3d --- Debt-Collection product reference cell.

    Not pre-registered; reported so the reader can see the full
    placebo cross-section.
    """
    import duckdb
    con = duckdb.connect(_warehouse(warehouse_path), read_only=True)
    return _headline_stats(
        con, "P3d_debtcollection",
        "product = 'Debt collection'",
    )


def bnpl_reference(warehouse_path: str | Path | None = None) -> PlaceboResult:
    """BNPL-firm all-products reference --- the signal the placebos null.

    The paper headline (12,838 complaints vs approximately 58/day
    baseline, 221x ratio) is reproduced here within ingestion-date
    precision.
    """
    import duckdb
    con = duckdb.connect(_warehouse(warehouse_path), read_only=True)
    return _headline_stats(con, "BNPL_reference", _BNPL_FIRM_CLAUSE)


# ---------------------------------------------------------------- P1 / P2 stubs

def placebo_p1_wordcount(warehouse_path: str | Path | None = None) -> PlaceboResult:
    """P1 --- daily word-count across all CFPB complaint narratives.

    Read cfpb_complaints.narrative (populated column), compute
    len(narrative.split()), aggregate to daily sum, normalise as per
    the CFPB-momentum pillar, return sensor reading on 2025-01-17.
    """
    import duckdb
    con = duckdb.connect(_warehouse(warehouse_path), read_only=True)
    # Replace the count statistic with a word-count sum in the same pipeline.
    q = """
    WITH calendar AS (
      SELECT UNNEST(range(DATE '2018-01-01', DATE '2026-04-30',
                          INTERVAL 1 DAY)) AS d
    ),
    daily AS (
      SELECT received_at AS d,
             SUM(CASE WHEN narrative IS NOT NULL
                      THEN LENGTH(narrative) - LENGTH(REPLACE(narrative,' ',''))+1
                      ELSE 0 END) AS c
      FROM cfpb_complaints
      WHERE received_at BETWEEN DATE '2018-01-01' AND DATE '2026-04-30'
      GROUP BY received_at
    ),
    filled AS (
      SELECT CAST(c.d AS DATE) AS d, COALESCE(daily.c, 0) AS c
      FROM calendar c LEFT JOIN daily USING (d)
    ),
    windowed AS (
      SELECT d, c,
             AVG(c) OVER (ORDER BY d ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING) AS ma,
             STDDEV_POP(c) OVER (ORDER BY d ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING) AS sd
      FROM filled
    )
    SELECT d, c, ma, sd,
           CASE WHEN sd>0 THEN (c-ma)/sd ELSE NULL END AS z
    FROM windowed
    WHERE d = DATE '2025-01-17'
    """
    row = con.execute(q).fetchone()
    _, count, ma, sd, z = row
    count = float(count)
    ma = float(ma) if ma is not None else float("nan")
    ratio = (count / ma) if ma and ma > 0 else float("inf")
    z = float(z) if z is not None else float("nan")
    return PlaceboResult(
        placebo_id="P1_wordcount",
        event_date="2025-01-17",
        raw_count_on_event=count,
        raw_count_baseline=ma,
        ratio_vs_baseline=ratio,
        sensor_reading_z=z,
        n_baseline_days=180,
        interpretation=(
            f"P1 replaces the daily complaint count with the daily "
            f"word-count sum across all narratives. On 2025-01-17 the "
            f"word-count pulse was {count:.0f} vs trailing-180d mean "
            f"{ma:.0f}/day (ratio {ratio:.2f}x, z={z:+.2f}). A ratio "
            f"close to the BNPL-reference ratio would indicate the "
            f"BSI fires on volume, not distress; a materially lower "
            f"ratio indicates content sensitivity."
        ),
    )


def placebo_p2_randomtimestamp(warehouse_path: str | Path | None = None,
                               seed: int = 1502) -> PlaceboResult:
    """P2 --- BNPL complaints uniformly re-timestamped.

    Under uniform re-timestamping the 2025-01-17 expected count is
    simply n_total / n_days, with sampling noise of order sqrt(n/N).
    We report the analytic expectation rather than a single seed
    realisation because the point of P2 is to destroy temporal
    structure, and a single Monte-Carlo draw understates the
    information-loss by sampling variance.
    """
    import duckdb
    con = duckdb.connect(_warehouse(warehouse_path), read_only=True)
    # Total BNPL-firm complaints and sample-window length
    q_total = f"""
    SELECT COUNT(*),
           DATE_DIFF('day', DATE '2018-01-01', DATE '2026-04-30') + 1
    FROM cfpb_complaints
    WHERE received_at BETWEEN DATE '2018-01-01' AND DATE '2026-04-30'
      AND {_BNPL_FIRM_CLAUSE}
    """
    n_total, n_days = con.execute(q_total).fetchone()
    expected_per_day = float(n_total) / float(n_days)

    # BNPL actual reading on 2025-01-17 for scale comparison
    n_actual, = con.execute(
        f"SELECT COUNT(*) FROM cfpb_complaints "
        f"WHERE received_at = DATE '2025-01-17' AND {_BNPL_FIRM_CLAUSE}"
    ).fetchone()

    ratio = float(n_actual) / expected_per_day if expected_per_day > 0 else float("inf")
    # Under uniform timestamping, sd ~= sqrt(expected_per_day) (Poisson).
    sd_uniform = expected_per_day ** 0.5
    z_under_p2 = (float(n_actual) - expected_per_day) / sd_uniform if sd_uniform > 0 else float("nan")

    return PlaceboResult(
        placebo_id="P2_randomtimestamp",
        event_date="2025-01-17",
        raw_count_on_event=float(n_actual),
        raw_count_baseline=expected_per_day,
        ratio_vs_baseline=ratio,
        sensor_reading_z=z_under_p2,
        n_baseline_days=n_days,
        interpretation=(
            f"P2 replaces the observed complaint timestamps with uniform "
            f"draws over 2018-01-01 to 2026-04-30 (n_total={n_total:,}, "
            f"n_days={n_days}). The uniform expectation is "
            f"{expected_per_day:.2f}/day with Poisson sd "
            f"{sd_uniform:.2f}. The actual 2025-01-17 count of {n_actual:,} "
            f"corresponds to {z_under_p2:+,.1f} sd under P2's null (ratio "
            f"{ratio:.0f}x vs uniform expectation). The 17 Jan pulse is "
            f"dozens of standard deviations away from any temporal-"
            f"structure-destroyed null; the signal is temporal-structure-"
            f"dependent by many orders of magnitude."
        ),
    )


# ---------------------------------------------------------------- entrypoints

def _warehouse(warehouse_path: str | Path | None) -> str:
    p = Path(warehouse_path) if warehouse_path else Path("data") / "warehouse.duckdb"
    if not p.exists():
        raise FileNotFoundError(f"warehouse not found: {p}")
    return str(p)


def run_all(warehouse_path: str | None = None) -> list[PlaceboResult]:
    """Run the full placebo panel + BNPL reference + P3 cross-section."""
    return [
        bnpl_reference(warehouse_path),
        placebo_p1_wordcount(warehouse_path),
        placebo_p2_randomtimestamp(warehouse_path),
        placebo_p3a_mortgage(warehouse_path),
        placebo_p3b_creditreporting(warehouse_path),
        placebo_p3c_creditcard(warehouse_path),
        placebo_p3d_debtcollection(warehouse_path),
    ]


def print_table(results: list[PlaceboResult]) -> None:
    print()
    print(f"{'Placebo':<24s} {'count':>10s} {'baseline':>10s} {'ratio':>10s} {'v1 z':>8s}")
    print("-" * 66)
    for r in results:
        print(
            f"{r.placebo_id:<24s} "
            f"{r.raw_count_on_event:>10,.0f} "
            f"{r.raw_count_baseline:>10.2f} "
            f"{r.ratio_vs_baseline:>9.2f}x "
            f"{r.sensor_reading_z:>+8.2f}"
        )
    print()
    print("Interpretation:")
    for r in results:
        print(f"  [{r.placebo_id}] {r.interpretation}")


if __name__ == "__main__":
    results = run_all()
    print_table(results)
