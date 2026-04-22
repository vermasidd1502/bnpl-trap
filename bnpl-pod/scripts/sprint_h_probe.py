"""
Sprint H empirical probe — the Risk Officer's "see the empty P&L" proof.

Before the Sprint H refactor, running the three-gate rule at the four
canonical event-study dates (Klarna down-round, AFRM guidance cuts,
CFPB interpretive rule) produced `approved = False` EVERYWHERE because
the single hardcoded `DEFAULT_CCD_II_DEADLINE = date(2026, 11, 20)` was
>180 days out on every date. That's a temporal leak: the gate cannot
distinguish "within 180 days of a material catalyst" from "a future
catalyst exists that was unknown at this as_of".

This script queries the live `regulatory_catalysts` warehouse table,
resolves the nearest material catalyst at each window's as_of, and
reports the difference between the pre-Sprint-H world (single deadline)
and the Sprint-H world (calendar). Run after `python -m data.ingest.regulatory_catalysts`
has seeded the warehouse.

Run with:  python -m scripts.sprint_h_probe
"""
from __future__ import annotations

from datetime import date

from backtest.event_study import evaluate_three_gates
from data.regulatory_calendar import load_catalysts, nearest_material_catalyst

WINDOWS: list[tuple[str, date]] = [
    ("KLARNA_DOWNROUND",  date(2022, 7, 11)),
    ("AFFIRM_GUIDANCE_1", date(2022, 8, 26)),
    ("AFFIRM_GUIDANCE_2", date(2023, 2, 9)),
    ("CFPB_INTERP_RULE",  date(2024, 5, 22)),
]

# Maximally-firing BSI + MOVE so only gate 3 is in question.
BSI_Z = 2.0
MOVE_MA30 = 135.0
OLD_HARDCODED_DEADLINE = date(2026, 11, 20)


def _run_old_world(as_of: date) -> tuple[bool, int]:
    days = (OLD_HARDCODED_DEADLINE - as_of).days
    _, _, gc, ok = evaluate_three_gates(
        bsi_z=BSI_Z, move_ma30=MOVE_MA30,
        as_of=as_of,
        nearest_catalyst_date=OLD_HARDCODED_DEADLINE,
    )
    return ok, days


def _run_new_world(as_of: date, catalysts):
    nearest = nearest_material_catalyst(as_of, catalysts)
    nd = nearest.deadline_date if nearest else None
    days = (nd - as_of).days if nd else None
    _, _, gc, ok = evaluate_three_gates(
        bsi_z=BSI_Z, move_ma30=MOVE_MA30,
        as_of=as_of,
        nearest_catalyst_date=nd,
    )
    return ok, days, (nearest.catalyst_id if nearest else None)


def main() -> int:
    catalysts = load_catalysts()
    print(f"Loaded {len(catalysts)} catalysts from warehouse:")
    for c in catalysts:
        print(f"  {c.deadline_date}  m={c.materiality:.2f}  {c.catalyst_id}")
    print()

    print(f"{'WINDOW':22s} {'AS_OF':12s} | {'OLD DAYS':>9s}  {'OLD OK':>6s} | "
          f"{'NEW DAYS':>9s}  {'NEW OK':>6s}  NEAREST")
    print("-" * 110)
    old_approvals = 0
    new_approvals = 0
    for name, as_of in WINDOWS:
        old_ok, old_days = _run_old_world(as_of)
        new_ok, new_days, nearest_id = _run_new_world(as_of, catalysts)
        old_approvals += int(old_ok)
        new_approvals += int(new_ok)
        new_days_str = f"{new_days}" if new_days is not None else "None"
        print(
            f"{name:22s} {str(as_of):12s} | "
            f"{old_days:>9d}  {str(old_ok):>6s} | "
            f"{new_days_str:>9s}  {str(new_ok):>6s}  {nearest_id or '—'}"
        )

    print()
    print(f"OLD (hardcoded 2026-11-20): approved on {old_approvals}/4 windows")
    print(f"NEW (calendar):             approved on {new_approvals}/4 windows")
    print()
    if new_approvals == 4 and old_approvals == 0:
        print("[PASS] Sprint H temporal leak fixed: zero -> four approvals on historical windows.")
        return 0
    print("[FAIL] Sprint H fix incomplete or calendar not seeded.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
