"""
Regulatory-catalyst calendar — kills the CCD II "time-travel" leak.

Why this module exists
----------------------
Prior to Sprint H, compliance gate 3 ("CCD II proximity") read a single
hardcoded deadline (`DEFAULT_CCD_II_DEADLINE = date(2026, 11, 20)` — the
EU Consumer Credit Directive II national-transposition date). On any
pre-2026 event window the difference `deadline - as_of` was > 180 days
by construction, so `gate_ccd2` returned False for the entire history
we backtest — which made the three-gate compliance rule structurally
un-firable on real data. That's a temporal leak: the gate is asking
whether a future event is imminent, but it cannot distinguish "we're
inside 180 days of the catalyst" from "the catalyst did not exist yet".

A BNPL trading pod doesn't care only about EU-CCD II. It cares about
any material regulatory publication that re-prices BNPL junior tranches:
CFPB market reports, UK FCA consultations, SEC no-action letters on
securitizer disclosure, state-AG settlements, and (yes) CCD II in 2026.
Each of these moves dealer inventory in the weeks around publication.
We model that by a time-varying table and ask the calendar — not a
magic constant — for the nearest catalyst at any `as_of`.

API contract
------------
- `load_catalysts(con=None) -> list[Catalyst]`:
      Pull every row from `regulatory_catalysts`. Sorted ascending by
      `deadline_date`. Opens its own DuckDB connection if none passed.
- `nearest_material_catalyst(as_of, catalysts=None, min_materiality=0.5) -> Catalyst | None`:
      Pure function. Returns the catalyst C such that
          C.deadline_date >= as_of  (future-or-same-day, not past)
          C.materiality    >= min_materiality
          (C.deadline_date - as_of).days is minimized.
      Returns None if no catalyst matches — callers treat that as
      "no material catalyst in the horizon → gate_ccd2 = False".
- `days_to_nearest(as_of, catalysts=None, min_materiality=0.5) -> int | None`:
      Thin wrapper; returns int(days) or None.

Materiality convention
----------------------
`materiality ∈ [0, 1]` is curated per row. Rule of thumb:
    1.00  binding rule / statutory deadline that re-caps BNPL liability
    0.80  major market-wide CFPB or FCA report with direct BNPL scope
    0.50  consultation paper or non-binding interpretive guidance
    0.20  speech, blog post, comment period without stated rule-making
Default gate threshold is 0.5 — below that the signal is too noisy
to be an actionable catalyst.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import duckdb

from data.settings import settings


@dataclass(frozen=True)
class Catalyst:
    catalyst_id: str
    jurisdiction: str
    deadline_date: date
    title: str
    materiality: float
    category: str | None = None
    notes: str | None = None


# --- Loaders ----------------------------------------------------------------
def load_catalysts(con: duckdb.DuckDBPyConnection | None = None) -> list[Catalyst]:
    """Read every row from `regulatory_catalysts`, sorted ascending by date."""
    owns = con is None
    if owns:
        con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT catalyst_id, jurisdiction, deadline_date, title,
                   materiality, category, notes
            FROM regulatory_catalysts
            ORDER BY deadline_date ASC
            """
        ).fetchall()
    finally:
        if owns:
            con.close()
    out: list[Catalyst] = []
    for cid, jur, dl, title, mat, cat, notes in rows:
        # DuckDB returns DATE as datetime.date already; defensive coerce.
        if isinstance(dl, datetime):
            dl = dl.date()
        out.append(
            Catalyst(
                catalyst_id=cid,
                jurisdiction=jur,
                deadline_date=dl,
                title=title,
                materiality=float(mat),
                category=cat,
                notes=notes,
            )
        )
    return out


# --- Queries ----------------------------------------------------------------
def nearest_material_catalyst(
    as_of: date,
    catalysts: Iterable[Catalyst] | None = None,
    *,
    min_materiality: float = 0.5,
) -> Catalyst | None:
    """Return the soonest future catalyst with materiality >= threshold.

    `as_of` itself counts as "future" (days_to == 0 is allowed — gate still
    fires as `0 <= days_to <= 180`). Past catalysts are excluded — a
    published rule no longer behaves as a catalyst; the re-pricing already
    happened.
    """
    if catalysts is None:
        catalysts = load_catalysts()

    best: Catalyst | None = None
    best_days: int | None = None
    for c in catalysts:
        if c.materiality < min_materiality:
            continue
        delta = (c.deadline_date - as_of).days
        if delta < 0:
            continue
        if best_days is None or delta < best_days:
            best = c
            best_days = delta
    return best


def days_to_nearest(
    as_of: date,
    catalysts: Iterable[Catalyst] | None = None,
    *,
    min_materiality: float = 0.5,
) -> int | None:
    c = nearest_material_catalyst(as_of, catalysts, min_materiality=min_materiality)
    if c is None:
        return None
    return (c.deadline_date - as_of).days
