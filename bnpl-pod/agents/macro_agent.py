"""
Macro Agent — reads warehouse state and emits the macro-layer inputs the
compliance engine needs for G1 (BSI) and G2 (MOVE). Gate numbering is
post-SCP-demotion (see paper §7): BSI=1, MOVE=2, CCD II catalyst=3.

The BSI is framed throughout as a **behavioural, top-of-funnel
psychological sensor** for BNPL-borrower panic, not as a roll-rate
forecaster. Agent advisories should surface that framing explicitly; in
particular, |BSI z| >= 10 triggers the SUPER-THRESHOLD BYPASS in which
the pod approves on behavioural evidence alone, accepting a documented
Type-I premium, because no macro gauge would have corroborated.

Responsibilities
----------------
- Pull the latest BSI row (bsi_daily). Compute z-score against a rolling
  180-day window (matches thresholds.yaml §gates.bsi specification).
- Pull MOVE series from fred_series; compute its 30-day moving average.
- Surface any freeze_flag from the vitality component.
- Optionally request an LLM advisory narrative on the macro state. The
  narrative is logged and shown on the dashboard; it NEVER feeds the
  compliance engine. Advisory is skipped (empty string) when no LLM key is
  configured or the caller passes `llm=None`.

Every field is produced deterministically. The LLM advisory is cosmetic.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone, timedelta
from typing import Optional

import duckdb

from agents.schemas import MacroReport
from data.settings import settings

log = logging.getLogger(__name__)

BSI_Z_WINDOW_DAYS = 180


def _rolling_z(series: list[float], target: float, window: int = BSI_Z_WINDOW_DAYS) -> float:
    """Causal rolling z-score of `target` against the PRIOR `window` observations.

    The target itself is NOT part of the μ/σ estimate — the fallback here
    must match the no-look-ahead contract in ``signals.bsi._rolling_z_causal``.
    Returns 0.0 if fewer than 5 prior observations are available (fail-safe:
    the BSI gate will not fire on an undertrained signal).
    """
    import statistics as st
    # series is full history in chronological order; target is the last row's
    # BSI value. Exclude the last observation (target's own day) before
    # building the window.
    prior = series[:-1] if series else []
    tail = [x for x in prior[-window:] if x is not None]
    if len(tail) < 5:
        return 0.0
    mu = st.fmean(tail)
    sd = st.pstdev(tail) or 1e-9
    return float((target - mu) / sd)


def _latest_bsi(con: duckdb.DuckDBPyConnection,
                as_of: datetime) -> tuple[float, float, bool]:
    rows = con.execute(
        """SELECT observed_at, bsi, z_bsi, freeze_flag
           FROM bsi_daily
           WHERE observed_at <= ?
           ORDER BY observed_at""",
        [as_of.date()],
    ).fetchall()
    if not rows:
        return 0.0, 0.0, False
    latest = rows[-1]
    bsi_raw = float(latest[1] or 0.0)
    # Prefer persisted z_bsi; fall back to rolling-z if missing.
    if latest[2] is not None:
        z = float(latest[2])
    else:
        hist = [float(r[1]) for r in rows if r[1] is not None]
        z = _rolling_z(hist, bsi_raw, BSI_Z_WINDOW_DAYS)
    freeze = bool(latest[3])
    return bsi_raw, z, freeze


def _move_ma30(con: duckdb.DuckDBPyConnection, as_of: datetime) -> float:
    rows = con.execute(
        """SELECT value FROM fred_series
           WHERE series_id='MOVE' AND observed_at <= ?
             AND value IS NOT NULL
           ORDER BY observed_at DESC LIMIT 30""",
        [as_of.date()],
    ).fetchall()
    if not rows:
        return 0.0
    vals = [float(r[0]) for r in rows]
    return float(sum(vals) / len(vals))


def _build_prompt(bsi: float, z: float, move_ma30: float, freeze: bool) -> tuple[str, str]:
    system = (
        "You are the Macro Agent in a BNPL credit-stress trading pod. "
        "Frame the BSI as a PSYCHOLOGICAL SENSOR for top-of-funnel "
        "consumer panic (CFPB complaints, App Store reviews, Google "
        "Trends friction queries) — NOT as a roll-rate proxy. The BSI "
        "is expected to be ORTHOGONAL to HY credit at weekly horizons; "
        "that orthogonality is a feature under the Subprime-2.0 opacity "
        "thesis, not a bug. Summarize the current macro-layer read in "
        "3-4 sentences. Be specific and quantitative. Do NOT make trade "
        "recommendations — your output is advisory only; a deterministic "
        "compliance engine decides whether any trade fires. If "
        "|BSI z| >= 10 (super-threshold bypass), explicitly flag that "
        "this is a Type-I-premium event — the pod is approving despite "
        "no macro-regime corroboration, on the basis of a single "
        "behavioural stress peak."
    )
    user = (
        f"Current inputs:\n"
        f"- BSI raw: {bsi:.3f}, z-score (180d): {z:+.2f}\n"
        f"- MOVE 30-day MA: {move_ma30:.1f}\n"
        f"- Any BNPL issuer freeze-flag active: {freeze}\n\n"
        "Give a compact macro read. Call out whether G1 (BSI z>1.5) and "
        "G2 (MOVE MA30>120) look likely to fire. If |z|>=10, state that "
        "the BSI-only super-threshold bypass would fire regardless of "
        "MOVE or catalyst — this is the behavioural-signal override. "
        "State explicitly this is advisory."
    )
    return system, user


def run(as_of: Optional[datetime] = None,
        llm=None) -> MacroReport:
    """Execute the macro agent. `llm` is an LLMClient or None."""
    as_of = as_of or datetime.now(timezone.utc)
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        bsi, z, freeze = _latest_bsi(con, as_of)
        move_ma30 = _move_ma30(con, as_of)
    finally:
        con.close()

    advisory = ""
    if llm is not None:
        try:
            system, user = _build_prompt(bsi, z, move_ma30, freeze)
            resp = llm.chat(system=system, user=user, tier="small", temperature=0.0, role="macro")
            advisory = resp.text.strip()
        except Exception as e:   # noqa: BLE001 — advisory is best-effort
            log.warning("macro_agent advisory failed: %s", e)

    log.info("macro | BSI=%.3f z=%+.2f MOVE_MA30=%.1f freeze=%s",
             bsi, z, move_ma30, freeze)
    return MacroReport(as_of=as_of, bsi=bsi, bsi_z=z,
                       move_ma30=move_ma30, freeze_flag=freeze,
                       advisory=advisory)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    report = run()
    print(f"BSI={report.bsi:.3f}  z={report.bsi_z:+.2f}  "
          f"MOVE_MA30={report.move_ma30:.1f}  freeze={report.freeze_flag}")
