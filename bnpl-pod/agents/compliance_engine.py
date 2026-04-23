"""
Deterministic compliance engine.

This module is the SOLE source of trade-approval decisions in the pod.
It contains NO LLM calls, NO probabilistic logic, and NO external I/O
beyond reading the thresholds YAML.

Design rationale
----------------
LLMs (including the risk_manager agent) are probabilistic: they can produce
different outputs for identical inputs. For a trading system this is a
compliance hazard — regulators auditing a bad trade cannot accept "an AI
approved it based on a Reddit post" as a defense.

Therefore we separate concerns:

    LLM risk_manager.py   ->  ADVISORY reasoning, narrative, hypothesis.
    compliance_engine.py  ->  DETERMINISTIC approval/veto.
    Human-in-the-loop     ->  final sign-off. Required, non-optional.

Every trade signal is a 3-tuple:

    (advisory_reasoning: str, compliance_decision: ComplianceDecision, human_approval: bool | None)

compliance_engine.py is the only layer that can return compliance_decision.approved = True.

Fix #2 (post-critique) — 3-gate AND, not 4
------------------------------------------
MASTERPLAN v4.1 originally framed approval as a four-gate AND:
    BSI × SCP × MOVE × CCD2
with a fifth squeeze-defense veto active for the equity_short expression.

The critique: SCP is a microstructure (equity-vol) signal, not a macro
thesis signal. Keeping it as a hard gate coupled the ABS-TRS book to the
equity layer, which then forced the squeeze-defense veto to exist — and
that veto only bites on equity shorts, a leg the thesis never endorsed.

Post-Fix #2:
  * The pod's only expression is `trs_junior_abs`.
  * Approval requires BSI × MOVE × CCD2 (three gates). SCP is still
    computed and written to the audit trail as diagnostic telemetry
    (`scp_telemetry_fires`), but it does NOT block approval.
  * Squeeze-defense logic is GONE from this engine. Squeeze metrics are
    still surfaced on the dashboard via risk_manager — advisory only.
  * Market-neutral hedging lives in a STATIC macro-hedge sleeve
    (HYG short / 2Y UST futures), sized in portfolio/book.py OUTSIDE
    the Mean-CVaR LP. See agents/schemas.py::MacroHedgeSpec.

Sprint H — calendar-driven CCD2 gate
------------------------------------
Field `ccd_ii_deadline: date` is gone. It hardcoded a future EU deadline
and made `gate_ccd2` structurally un-firable on any pre-2026 event window
(see data/regulatory_calendar.py for the full diagnosis). The engine now
receives `nearest_catalyst_date: date | None` — the soonest material
regulatory catalyst at the as_of moment, as resolved from the
`regulatory_catalysts` warehouse table by the caller. `None` means "no
catalyst in the horizon" and the gate fails with a dedicated reason.
The caller (agents/graph.py for live ticks, backtest/event_study.py for
historical windows) owns the calendar query so the engine stays pure.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

from data.settings import load_thresholds

# Only the three gates that gate approval.
Gate = Literal["bsi", "move", "ccd2"]


@dataclass(frozen=True)
class GateInputs:
    """Strict schema for the compliance engine. Fill every field explicitly.

    Fix #2: `expression`, `equity_tickers`, and the three `squeeze_*` dicts
    are gone. SCP input is preserved but is now telemetry-only.

    Sprint H: `ccd_ii_deadline` → `nearest_catalyst_date`. The caller
    resolves the regulatory-catalyst calendar via
    `data.regulatory_calendar.nearest_material_catalyst(as_of)` and passes
    the resulting date (or None if no material catalyst is within the
    record) to the engine. The engine no longer owns the calendar — it
    owns the rule.
    """
    as_of: datetime
    # Gate 1 — BSI
    bsi_z: float
    # Telemetry — SCP (per-ticker dollar gap between Heston ES and GBM EL,
    # $/100 notional). Computed and surfaced, but does NOT gate approval.
    scp_by_ticker: dict[str, float]
    # Gate 2 — MOVE 30-day moving average
    move_ma30: float
    # Gate 3 — Nearest material regulatory catalyst (calendar-resolved).
    # None => caller found no catalyst with materiality >= threshold whose
    # deadline is >= as_of; the engine treats this as a gate failure with
    # a dedicated reason string.
    nearest_catalyst_date: date | None


@dataclass
class ComplianceDecision:
    approved: bool
    gate_results: dict[Gate, bool]
    scp_telemetry_fires: bool           # diagnostic only
    squeeze_veto: bool                  # dead — always False post-Fix #2
    reasons: list[str]
    # Checksum of the active thresholds YAML — kept with every decision for audit
    thresholds_version: str
    # Sprint Q (post-review 2026-04-22): set True when approval was granted
    # via the |z|>bypass_z super-threshold and MOVE / catalyst gates would
    # otherwise have rejected the trade. Carries an explicit Type-I premium
    # disclosure — downstream dashboards and audit trails MUST surface this
    # flag on every bypassed decision.
    bypass_fired: bool = False


class ComplianceEngine:
    """Pure-python rule evaluator. No network. No LLM."""

    def __init__(self, thresholds: dict | None = None) -> None:
        self.thresholds = thresholds or load_thresholds()
        self._version = _stable_hash(self.thresholds)

    def evaluate(self, inputs: GateInputs) -> ComplianceDecision:
        reasons: list[str] = []
        gates: dict[Gate, bool] = {}
        th = self.thresholds

        # --- Gate 1: BSI z-score -----------------------------------------
        # Paper v2.0.1 carry-over: z_threshold was calibrated against the v1
        # 180-day rolling σ implementation; mechanical carry-over to the
        # post-v2 EWMA σ is disclosed in paper_formal.tex §6 and in
        # config/thresholds.yaml. Every audit reason emitted below carries a
        # `(v1-calibrated carry-over; see paper §6)` marker so a downstream
        # reader cannot mistake Gate 1 for a re-fit rule.
        z_req = float(th["gates"]["bsi"]["z_threshold"])
        gates["bsi"] = inputs.bsi_z >= z_req
        if not gates["bsi"]:
            reasons.append(
                f"Gate 1 (BSI) FAIL: z={inputs.bsi_z:.3f} < {z_req:.3f}"
                " (v1-calibrated carry-over; see paper §6)"
            )
        else:
            reasons.append(
                f"Gate 1 (BSI) PASS: z={inputs.bsi_z:.3f} >= {z_req:.3f}"
                " (v1-calibrated carry-over; see paper §6)"
            )

        # --- Gate 2: MOVE 30-day MA --------------------------------------
        move_req = float(th["gates"]["move"]["ma30_threshold"])
        gates["move"] = inputs.move_ma30 >= move_req
        if not gates["move"]:
            reasons.append(f"Gate 2 (MOVE) FAIL: MA30={inputs.move_ma30:.1f} < {move_req:.1f}")

        # --- Gate 3: Nearest material regulatory catalyst ----------------
        # Calendar-resolved by the caller. `None` == no material catalyst
        # found inside the record at this as_of.
        max_days = int(th["gates"]["ccd_ii"]["max_days_to_deadline"])
        if inputs.nearest_catalyst_date is None:
            gates["ccd2"] = False
            reasons.append(
                f"Gate 3 (Reg catalyst) FAIL: no material catalyst found within [0, {max_days}d] horizon"
            )
        else:
            days_remaining = (inputs.nearest_catalyst_date - inputs.as_of.date()).days
            gates["ccd2"] = 0 <= days_remaining <= max_days
            if not gates["ccd2"]:
                reasons.append(
                    f"Gate 3 (Reg catalyst) FAIL: days_remaining={days_remaining} not in [0, {max_days}]"
                )

        # --- Telemetry: SCP (NOT gating) ---------------------------------
        # Kept for the audit trail + paper §9 event studies. A post-Fix #2
        # auditor who wants a 4-gate view can re-impose this condition
        # off the recorded value; approval here is insensitive to it.
        scp_req = float(th["gates"]["scp"]["min_scp_equity_layer"])
        if inputs.scp_by_ticker:
            scp_fires = max(inputs.scp_by_ticker.values()) >= scp_req
        else:
            scp_fires = False

        # --- Approval: 3-gate AND, with |z| super-threshold bypass ------
        require_all = bool(th["compliance"]["require_all_four_gates"])
        gates_pass = all(gates.values()) if require_all else any(gates.values())

        # Sprint Q bypass (post-review, 2026-04-22). When bsi_z >= a super-
        # threshold (default 10σ, see thresholds.yaml :: gates.bsi.bypass_z),
        # the trade approves on BSI alone. The paper's §8.5 finding: every
        # public macro regime gauge was at/below its long-run median on the
        # only day BSI crossed +10σ in the entire sample (Reg Z deadline,
        # 17-Jan-2025). A strict conjunction with MOVE or regulatory
        # catalyst is therefore structurally unable to approve the paper's
        # flagship trade; the bypass is the architectural response that
        # accepts an explicit Type-I premium in exchange for capturing
        # idiosyncratic BNPL signals that macro gauges will not corroborate.
        bypass_z_req = float(th["gates"]["bsi"].get("bypass_z_threshold", float("inf")))
        bypass_fired = False
        if math.isfinite(inputs.bsi_z) and abs(inputs.bsi_z) >= bypass_z_req and not gates_pass:
            bypass_fired = True
            reasons.insert(
                0,
                f"BSI-only bypass FIRED: |z|={abs(inputs.bsi_z):.2f} >= "
                f"bypass_z_threshold={bypass_z_req:.4f}. Behavioural "
                f"top-of-funnel consumer-panic signal has dominated the "
                f"macro regime; trade approved on BSI alone and macro-"
                f"corroboration gates (MOVE, catalyst) would otherwise "
                f"have rejected. Type-I premium ACCEPTED per paper §8.5 "
                f"architectural recommendation (Subprime-2.0 blind-spot "
                f"signature: no public macro gauge saw this event).",
            )
            approved = True
        else:
            approved = gates_pass

        squeeze_veto = False   # dead post-Fix #2

        # Consolidated approval line is emitted on every approved decision
        # (conjunction or bypass). Earlier versions gated this on
        # `not reasons`, but Gate 1 now appends a carry-over provenance
        # marker on both PASS and FAIL by construction (paper v2.0.1), so
        # the reason list is never empty and the headline would otherwise
        # be suppressed in every real decision path.
        if approved and not bypass_fired:
            reasons.append(
                "All three gates passed (BSI, MOVE, Reg catalyst); TRS-only expression; "
                "deterministic rules satisfied."
            )

        return ComplianceDecision(
            approved=approved,
            gate_results=gates,
            scp_telemetry_fires=scp_fires,
            squeeze_veto=squeeze_veto,
            reasons=reasons,
            thresholds_version=self._version,
            bypass_fired=bypass_fired,
        )


def _stable_hash(obj) -> str:
    """Deterministic short hash for a YAML-loaded dict. For audit provenance."""
    import hashlib
    import json

    payload = json.dumps(obj, sort_keys=True, default=str).encode()
    return hashlib.sha256(payload).hexdigest()[:12]
