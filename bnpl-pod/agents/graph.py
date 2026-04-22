"""
Pod orchestrator.

Pure-python LangGraph-style sequential state machine. LangGraph is not a hard
dependency; this module implements the same contract (typed state + pure
node functions) in ~80 lines so the pod is runnable with plain stdlib +
dataclasses.

Flow
----
    macro_agent  ->  quant_agent  ->  risk_manager  ->  compliance_engine
        |               |                |                     |
        v               v                v                     v
       MacroReport    QuantReport     RiskReport          ComplianceDecision
                            \\            |            /
                             \\-->  PodDecision  <--/

`compliance_engine.evaluate()` is the sole source of truth for `approved`.
LLM advisories from the three agents are concatenated into a single
`llm_advisory` field for the dashboard; they NEVER feed back into the engine.

Fix #2 note: the pod's only trade expression is `trs_junior_abs`. The
`expression` / `equity_tickers` parameters are gone. The macro-hedge
sleeve (HYG short / UST futures) is sized separately in portfolio.book.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Optional

from agents import macro_agent, quant_agent, risk_manager
from agents.compliance_engine import ComplianceEngine, GateInputs
from agents.schemas import PodDecision
from data.regulatory_calendar import Catalyst, nearest_material_catalyst

log = logging.getLogger(__name__)


def run_graph(
    as_of: Optional[datetime] = None,
    *,
    nearest_catalyst_date: Optional[date] = None,
    catalysts: Optional[list[Catalyst]] = None,
    llm=None,
    run_id: Optional[str] = None,
) -> PodDecision:
    """Execute one pod tick end-to-end and return the assembled PodDecision.

    Sprint H: the CCD II "time-travel" hardcode is gone. By default we query
    `data.regulatory_calendar.nearest_material_catalyst(as_of)` for the
    calendar-resolved catalyst date. Callers can override two ways:

    - `nearest_catalyst_date`: skip the calendar entirely and force the
      date fed into gate 3. Useful for scenario alt-testing ("what if
      CCD II transposition slips by six months?").
    - `catalysts`: inject a pre-loaded list (lets tests avoid the warehouse
      and lets the backtest pass a shared snapshot across many as_ofs).

    The function is read-only w.r.t. the warehouse. Persistence (writing the
    pod_decisions row) is the caller's responsibility — see agents.tick.
    """
    as_of = as_of or datetime.now(timezone.utc)
    run_id = run_id or uuid.uuid4().hex[:12]

    # Resolve the nearest material regulatory catalyst at `as_of`.
    # Explicit override > calendar query. Calendar lookup falls through
    # cleanly to `None` if the warehouse is empty or no catalyst is in
    # the horizon → the engine reports the gate failure with a dedicated
    # reason rather than silently defaulting to a future deadline.
    if nearest_catalyst_date is None:
        resolved = nearest_material_catalyst(as_of.date(), catalysts)
        nearest_catalyst_date = resolved.deadline_date if resolved is not None else None

    # --- Node 1: macro -----------------------------------------------------
    macro = macro_agent.run(as_of=as_of, llm=llm)
    # --- Node 2: quant -----------------------------------------------------
    quant = quant_agent.run(as_of=as_of, llm=llm)
    # --- Node 3: risk (squeeze telemetry only post-Fix #2) -----------------
    risk = risk_manager.run(as_of=as_of, llm=llm)

    # --- Node 4: deterministic compliance ----------------------------------
    inputs = GateInputs(
        as_of=as_of,
        bsi_z=macro.bsi_z,
        scp_by_ticker=quant.scp_by_ticker,   # telemetry input
        move_ma30=macro.move_ma30,
        nearest_catalyst_date=nearest_catalyst_date,
    )
    engine = ComplianceEngine()
    decision = engine.evaluate(inputs)

    # --- Assemble PodDecision ---------------------------------------------
    advisory = "\n\n".join(
        f"[{name}] {text}"
        for name, text in (
            ("macro", macro.advisory),
            ("quant", quant.advisory),
            ("risk", risk.advisory),
        )
        if text
    )

    trade_signal = {
        "run_id": run_id,
        "as_of": as_of.isoformat(),
        "expression": "trs_junior_abs",
        "equity_tickers": [],                       # dead field, kept for schema compat
        "approved": decision.approved,
        "gates": decision.gate_results,             # 3-gate dict
        "scp_telemetry_fires": decision.scp_telemetry_fires,
        "squeeze_veto": decision.squeeze_veto,      # always False
        "reasons": decision.reasons,
        "thresholds_version": decision.thresholds_version,
    }

    pod = PodDecision(
        run_id=run_id,
        as_of=as_of,
        macro=macro,
        quant=quant,
        risk=risk,
        expression="trs_junior_abs",
        equity_tickers=[],
        nearest_catalyst_date=nearest_catalyst_date,
        compliance_approved=decision.approved,
        gate_bsi=decision.gate_results.get("bsi", False),
        gate_scp=decision.scp_telemetry_fires,      # telemetry, not a gate
        gate_move=decision.gate_results.get("move", False),
        gate_ccd2=decision.gate_results.get("ccd2", False),
        squeeze_veto=decision.squeeze_veto,         # always False
        compliance_reasons=list(decision.reasons),
        thresholds_version=decision.thresholds_version,
        llm_advisory=advisory,
        trade_signal_json=json.dumps(trade_signal, default=str),
    )
    log.info(
        "pod | run=%s | approved=%s | gates=%s | scp_tele=%s",
        run_id, decision.approved, decision.gate_results, decision.scp_telemetry_fires,
    )
    return pod


def pod_to_dict(pod: PodDecision) -> dict:
    """Serializable view of the PodDecision — for JSON logs / dashboard."""
    return json.loads(json.dumps(asdict(pod), default=str))
