"""
Tests for the regulatory-catalyst calendar (Sprint H).

The calendar replaces the hardcoded `DEFAULT_CCD_II_DEADLINE = date(2026, 11, 20)`
constant that made `gate_ccd2` structurally un-firable on any pre-2026 event
window. These tests pin the semantics the compliance engine and event-study
driver rely on:

1. Future-only: past catalysts are ignored (a published rule has already
   re-priced the market — it is no longer a catalyst).
2. Materiality filter: catalysts below threshold are dropped so staff blog
   posts don't trigger compliance gate 3.
3. Nearest-first: the query returns the soonest qualifying catalyst.
4. Empty-record safety: no rows → None, and downstream gates fail closed.

The final two tests are integration probes that reproduce the Sprint H
empirical audit: run the compliance rule across the four canonical event
windows with the real seed calendar and verify every window now has
`gate_ccd2 = True` (the pre-Sprint-H world returned False on all four).
"""
from __future__ import annotations

from datetime import date

import pytest

from data.regulatory_calendar import (
    Catalyst,
    days_to_nearest,
    nearest_material_catalyst,
)


# --- Seed catalysts mirroring the warehouse -------------------------------
_CATALYSTS = [
    Catalyst(
        catalyst_id="cfpb_2022_market_report",
        jurisdiction="US-CFPB",
        deadline_date=date(2022, 9, 15),
        title="CFPB BNPL market report",
        materiality=0.80,
        category="report",
    ),
    Catalyst(
        catalyst_id="fca_bnpl_consultation_2023",
        jurisdiction="UK-FCA",
        deadline_date=date(2023, 2, 14),
        title="FCA BNPL consultation",
        materiality=0.70,
        category="consultation",
    ),
    Catalyst(
        catalyst_id="cfpb_2024_interpretive_rule",
        jurisdiction="US-CFPB",
        deadline_date=date(2024, 5, 22),
        title="CFPB Reg Z interpretive rule",
        materiality=0.95,
        category="rule",
    ),
    Catalyst(
        catalyst_id="ccd_ii_transposition_2026",
        jurisdiction="EU",
        deadline_date=date(2026, 11, 20),
        title="EU CCD II transposition",
        materiality=1.00,
        category="transposition",
    ),
    # A sub-threshold item we include explicitly to test the filter.
    Catalyst(
        catalyst_id="staff_speech_2022",
        jurisdiction="US-CFPB",
        deadline_date=date(2022, 7, 1),
        title="Staff speech — non-binding",
        materiality=0.20,
        category="speech",
    ),
]


# --- Unit tests ------------------------------------------------------------
def test_returns_soonest_future_catalyst():
    as_of = date(2022, 6, 1)
    c = nearest_material_catalyst(as_of, _CATALYSTS)
    # Sub-threshold staff speech at 2022-07-01 should be skipped; next
    # material catalyst is CFPB 2022 market report at 2022-09-15.
    assert c is not None
    assert c.catalyst_id == "cfpb_2022_market_report"


def test_past_catalysts_are_excluded():
    as_of = date(2023, 1, 1)  # past 2022-09-15, before 2023-02-14
    c = nearest_material_catalyst(as_of, _CATALYSTS)
    assert c is not None
    assert c.catalyst_id == "fca_bnpl_consultation_2023"


def test_returns_none_when_no_future_catalyst():
    """Past the last dated catalyst, the query must return None."""
    as_of = date(2030, 1, 1)
    assert nearest_material_catalyst(as_of, _CATALYSTS) is None
    assert days_to_nearest(as_of, _CATALYSTS) is None


def test_materiality_filter_honored():
    """At as_of 2022-06-01, the staff speech (m=0.2) is closer but filtered."""
    as_of = date(2022, 6, 1)
    # With the default 0.5 threshold, speech is dropped.
    high = nearest_material_catalyst(as_of, _CATALYSTS, min_materiality=0.5)
    assert high.catalyst_id == "cfpb_2022_market_report"
    # Lower the threshold to 0.1 and the speech becomes nearest.
    low = nearest_material_catalyst(as_of, _CATALYSTS, min_materiality=0.1)
    assert low.catalyst_id == "staff_speech_2022"


def test_as_of_equals_catalyst_day_counts_as_zero():
    """On the catalyst date itself, days_to == 0 (gate still fires)."""
    as_of = date(2022, 9, 15)
    days = days_to_nearest(as_of, _CATALYSTS)
    assert days == 0


def test_returns_none_on_empty_catalog():
    assert nearest_material_catalyst(date(2023, 1, 1), []) is None
    assert days_to_nearest(date(2023, 1, 1), []) is None


# --- Integration — the Sprint H empirical audit ---------------------------
# These are the four event-study windows pre-loaded in backtest/event_study.py.
# Pre-Sprint-H, all four computed days_to = 1593 / 1547 / 1380 / 912 (days
# to the single hardcoded 2026-11-20 deadline) → gate_ccd2 = False on every
# day of every window → approved = False. The calendar refactor must put
# every window within 180d of a DIFFERENT material catalyst.
_HISTORICAL_WINDOWS: list[tuple[str, date]] = [
    ("KLARNA_DOWNROUND",  date(2022, 7, 11)),
    ("AFFIRM_GUIDANCE_1", date(2022, 8, 26)),
    ("AFFIRM_GUIDANCE_2", date(2023, 2, 9)),
    ("CFPB_INTERP_RULE",  date(2024, 5, 22)),
]


@pytest.mark.parametrize("name,as_of", _HISTORICAL_WINDOWS)
def test_every_historical_window_now_has_material_catalyst_in_horizon(name, as_of):
    """Each of the four canonical windows must see a catalyst within 180d."""
    days = days_to_nearest(as_of, _CATALYSTS)
    assert days is not None, f"{name}: no material catalyst found"
    assert 0 <= days <= 180, (
        f"{name} at {as_of}: days_to_nearest={days} exceeds 180d horizon — "
        f"the temporal leak is NOT fixed. Expected <=180 after Sprint H."
    )


def test_empirical_audit_four_windows_all_approve_post_sprint_h():
    """Composite check matching the Risk Officer's empirical-proof directive.

    Pre-Sprint-H: all four windows returned approved=False even under the
    strongest possible BSI/MOVE configuration. Post-Sprint-H: with the
    calendar wired up, all four must approve given a passing BSI and MOVE.
    """
    from backtest.event_study import evaluate_three_gates

    for name, as_of in _HISTORICAL_WINDOWS:
        nearest = nearest_material_catalyst(as_of, _CATALYSTS)
        assert nearest is not None, f"{name}: calendar empty"
        gb, gm, gc, approved = evaluate_three_gates(
            bsi_z=2.0,
            move_ma30=135.0,
            as_of=as_of,
            nearest_catalyst_date=nearest.deadline_date,
        )
        assert approved, (
            f"{name} at {as_of} still fails post-Sprint-H: "
            f"gate_bsi={gb} gate_move={gm} gate_ccd2={gc} "
            f"nearest={nearest.catalyst_id}"
        )
