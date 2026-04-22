"""
Unit tests for the deterministic compliance engine.

These tests enforce the central architectural invariant: the compliance
engine is the SOLE source of trade-approval, is deterministic, and is
independent of any LLM. If these break, the whole pod's auditability fails.

Fix #2 (post-critique): the engine is now a 3-gate AND (BSI × MOVE × CCD2).
SCP is demoted from a gate to TELEMETRY (reported, not gating). The
squeeze-defense veto is REMOVED — the pod no longer evaluates an
`equity_short` expression. See agents/compliance_engine.py for the
rationale.
"""
from __future__ import annotations

from datetime import date, datetime

import pytest

from agents.compliance_engine import ComplianceEngine, GateInputs


@pytest.fixture
def engine() -> ComplianceEngine:
    return ComplianceEngine()


@pytest.fixture
def passing_inputs() -> GateInputs:
    """All three gates pass; SCP telemetry also fires (non-gating)."""
    return GateInputs(
        as_of=datetime(2026, 6, 1, 9, 30),
        bsi_z=2.3,
        scp_by_ticker={"AFRM": 4.1, "SQ": 3.2},
        move_ma30=135.0,
        nearest_catalyst_date=date(2026, 11, 20),
    )


def test_deterministic(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    """Same inputs -> bit-identical outputs, always."""
    r1 = engine.evaluate(passing_inputs)
    r2 = engine.evaluate(passing_inputs)
    assert r1.approved == r2.approved
    assert r1.gate_results == r2.gate_results
    assert r1.reasons == r2.reasons
    assert r1.thresholds_version == r2.thresholds_version


def test_three_gates_required(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    """Approval is BSI × MOVE × CCD2. Exactly three gates, all must fire."""
    decision = engine.evaluate(passing_inputs)
    assert decision.approved is True
    assert set(decision.gate_results.keys()) == {"bsi", "move", "ccd2"}
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
        **{**passing_inputs.__dict__, "nearest_catalyst_date": date(2028, 1, 1)}
    )
    decision = engine.evaluate(inputs)
    assert decision.approved is False
    assert decision.gate_results["ccd2"] is False


def test_ccd2_none_catalyst_vetoes(
    engine: ComplianceEngine, passing_inputs: GateInputs,
) -> None:
    """Sprint H: `nearest_catalyst_date=None` => gate 3 fails with a dedicated reason.

    This is the calendar-aware replacement for the old "hardcoded future
    deadline always passes" behavior. If the calendar query returns no
    material catalyst at this as_of, the engine must refuse approval
    rather than silently defaulting to some future date.
    """
    inputs = passing_inputs.__class__(
        **{**passing_inputs.__dict__, "nearest_catalyst_date": None}
    )
    decision = engine.evaluate(inputs)
    assert decision.approved is False
    assert decision.gate_results["ccd2"] is False
    assert any("no material catalyst" in r for r in decision.reasons)


def test_scp_is_telemetry_only_does_not_block_approval(
    engine: ComplianceEngine, passing_inputs: GateInputs,
) -> None:
    """Fix #2: SCP below threshold (or absent) must NOT fail approval.

    This is the architecturally-critical test. Pre-Fix #2, SCP was a hard
    gate; the credit thesis was coupled to an equity-vol signal. Post-fix,
    SCP is reported but non-gating.
    """
    # Absent SCP input
    inputs_empty = passing_inputs.__class__(**{**passing_inputs.__dict__,
                                                "scp_by_ticker": {}})
    decision = engine.evaluate(inputs_empty)
    assert decision.approved is True
    assert decision.scp_telemetry_fires is False

    # SCP below threshold
    inputs_low = passing_inputs.__class__(**{**passing_inputs.__dict__,
                                              "scp_by_ticker": {"AFRM": 0.1}})
    decision = engine.evaluate(inputs_low)
    assert decision.approved is True
    assert decision.scp_telemetry_fires is False


def test_scp_telemetry_fires_when_over_threshold(
    engine: ComplianceEngine, passing_inputs: GateInputs,
) -> None:
    """The telemetry flag must still fire when max(SCP) ≥ threshold."""
    decision = engine.evaluate(passing_inputs)
    assert decision.scp_telemetry_fires is True


def test_squeeze_veto_field_is_always_false_post_fix_2(
    engine: ComplianceEngine, passing_inputs: GateInputs,
) -> None:
    """The squeeze_veto field is retained for DB-schema compat but is DEAD.

    The pod's only expression is TRS; there is no equity-short leg for a
    squeeze veto to protect against. Keep this guard so any future code
    that tries to re-enable the veto trips a visible test.
    """
    decision = engine.evaluate(passing_inputs)
    assert decision.squeeze_veto is False


def test_thresholds_version_present(engine: ComplianceEngine, passing_inputs: GateInputs) -> None:
    """Every decision carries a hash of the active thresholds YAML for audit."""
    decision = engine.evaluate(passing_inputs)
    assert decision.thresholds_version
    assert len(decision.thresholds_version) == 12


def test_bsi_bypass_fires_when_z_super_threshold(
    engine: ComplianceEngine, passing_inputs: GateInputs,
) -> None:
    """Sprint Q (post-review, 2026-04-22): |z|>=bypass_z approves on BSI alone.

    Setup: force MOVE below threshold and no catalyst. Strict-gate would
    reject. A |bsi_z| above the bypass threshold must flip approval to True
    and carry a Type-I-premium reason + the `bypass_fired` flag.
    """
    inputs = passing_inputs.__class__(
        **{
            **passing_inputs.__dict__,
            "bsi_z": 27.4,                   # Reg Z deadline peak (2025-01-17)
            "move_ma30": 95.0,               # below 120
            "nearest_catalyst_date": None,   # no catalyst in record
        }
    )
    decision = engine.evaluate(inputs)
    assert decision.approved is True
    assert decision.bypass_fired is True
    # Per-gate flags still reflect the honest evaluation (MOVE + catalyst fail).
    assert decision.gate_results["move"] is False
    assert decision.gate_results["ccd2"] is False
    # Audit trail must carry the explicit bypass disclosure.
    assert any("BSI-only bypass FIRED" in r for r in decision.reasons)
    assert any("Type-I premium" in r for r in decision.reasons)


def test_bsi_bypass_does_not_fire_below_super_threshold(
    engine: ComplianceEngine, passing_inputs: GateInputs,
) -> None:
    """z=5 is above the 1.5 gate threshold but below the 10 bypass."""
    inputs = passing_inputs.__class__(
        **{
            **passing_inputs.__dict__,
            "bsi_z": 5.0,
            "move_ma30": 95.0,
            "nearest_catalyst_date": None,
        }
    )
    decision = engine.evaluate(inputs)
    assert decision.approved is False
    assert decision.bypass_fired is False
    assert decision.gate_results["bsi"] is True     # BSI DOES fire at 5σ
    assert decision.gate_results["move"] is False   # MOVE still blocks


def test_bsi_bypass_does_not_flag_normal_approvals(
    engine: ComplianceEngine, passing_inputs: GateInputs,
) -> None:
    """When all three gates already pass, `bypass_fired` stays False.

    The flag must accurately identify decisions that would have been
    rejected under the strict conjunction. A normal approval at bsi_z=2.3
    with favorable MOVE/catalyst must carry `bypass_fired=False` so the
    audit trail distinguishes routine approvals from Type-I-premium ones.
    """
    decision = engine.evaluate(passing_inputs)
    assert decision.approved is True
    assert decision.bypass_fired is False
    assert all(decision.gate_results.values())


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
