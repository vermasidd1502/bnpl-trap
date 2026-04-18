"""
Unit tests for the deterministic compliance engine.

These tests enforce the central architectural invariant: the compliance
engine is the SOLE source of trade-approval, is deterministic, and is
independent of any LLM. If these break, the whole pod's auditability fails.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from agents.compliance_engine import ComplianceEngine, GateInputs


@pytest.fixture
def engine() -> ComplianceEngine:
    return ComplianceEngine()


@pytest.fixture
def passing_inputs() -> GateInputs:
    """All four gates pass, no squeeze concerns, TRS expression."""
    return GateInputs(
        as_of=datetime(2026, 6, 1, 9, 30),
        bsi_z=2.3,
        scp_by_ticker={"AFRM": 4.1, "SQ": 3.2},
        move_ma30=135.0,
        ccd_ii_deadline=date(2026, 11, 20),
        squeeze_utilization={"AFRM": 0.70, "SQ": 0.55},
        squeeze_days_to_cover={"AFRM": 3.2, "SQ": 2.1},
        squeeze_skew_pctile={"AFRM": 0.60, "SQ": 0.40},
        expression="trs_junior_abs",
    )


def test_deterministic(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    """Same inputs -> bit-identical outputs, always."""
    r1 = engine.evaluate(passing_inputs)
    r2 = engine.evaluate(passing_inputs)
    assert r1.approved == r2.approved
    assert r1.gate_results == r2.gate_results
    assert r1.reasons == r2.reasons
    assert r1.thresholds_version == r2.thresholds_version


def test_all_four_gates_required(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    decision = engine.evaluate(passing_inputs)
    assert decision.approved is True
    assert all(decision.gate_results.values())


def test_bsi_below_threshold_vetoes(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    inputs = passing_inputs.__class__(**{**passing_inputs.__dict__, "bsi_z": 0.8})
    decision = engine.evaluate(inputs)
    assert decision.approved is False
    assert decision.gate_results["bsi"] is False
    assert any("Gate 1 (BSI) FAIL" in r for r in decision.reasons)


def test_move_below_threshold_vetoes(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    inputs = passing_inputs.__class__(**{**passing_inputs.__dict__, "move_ma30": 95.0})
    decision = engine.evaluate(inputs)
    assert decision.approved is False
    assert decision.gate_results["move"] is False


def test_ccd_ii_too_far_vetoes(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    inputs = passing_inputs.__class__(
        **{**passing_inputs.__dict__, "ccd_ii_deadline": date(2028, 1, 1)}
    )
    decision = engine.evaluate(inputs)
    assert decision.approved is False
    assert decision.gate_results["ccd2"] is False


def test_scp_empty_vetoes(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    inputs = passing_inputs.__class__(**{**passing_inputs.__dict__, "scp_by_ticker": {}})
    decision = engine.evaluate(inputs)
    assert decision.approved is False
    assert decision.gate_results["scp"] is False


def test_squeeze_veto_on_equity_expression(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    inputs = passing_inputs.__class__(**{
        **passing_inputs.__dict__,
        "expression": "equity_short",
        "equity_tickers": ["AFRM"],
        "squeeze_utilization": {"AFRM": 0.93},
    })
    decision = engine.evaluate(inputs)
    assert decision.approved is False
    assert decision.squeeze_veto is True
    assert any("SQUEEZE VETO" in r for r in decision.reasons)


def test_squeeze_bypassed_for_trs(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    """Structured-credit TRS expression should bypass squeeze defense by design."""
    inputs = passing_inputs.__class__(**{
        **passing_inputs.__dict__,
        "expression": "trs_junior_abs",
        "squeeze_utilization": {"AFRM": 0.99},   # nuclear-level squeeze
    })
    decision = engine.evaluate(inputs)
    assert decision.approved is True
    assert decision.squeeze_veto is False


def test_thresholds_version_present(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    """Every decision carries a hash of the active thresholds YAML for audit."""
    decision = engine.evaluate(passing_inputs)
    assert decision.thresholds_version
    assert len(decision.thresholds_version) == 12


def test_engine_has_no_network_dependencies() -> None:
    """Structural test: the engine module must not import any HTTP/LLM client."""
    import ast
    import pathlib
    src = pathlib.Path(__file__).resolve().parent.parent / "agents" / "compliance_engine.py"
    tree = ast.parse(src.read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names: imported.add(a.name)
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    forbidden = {"openai", "anthropic", "httpx", "requests", "urllib.request", "langchain", "langgraph"}
    leaks = imported & forbidden
    assert not leaks, f"compliance_engine.py must stay LLM/network-free, but imports: {leaks}"
