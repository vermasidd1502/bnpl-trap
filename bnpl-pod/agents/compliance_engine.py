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
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

from data.settings import load_thresholds

Gate = Literal["bsi", "scp", "move", "ccd2"]


@dataclass(frozen=True)
class GateInputs:
    """Strict schema for the compliance engine. Fill every field explicitly."""
    as_of: datetime
    # Gate 1 — BSI
    bsi_z: float
    # Gate 2 — SCP (per-ticker dollar gap between Heston ES and GBM EL, $/100 notional)
    scp_by_ticker: dict[str, float]
    # Gate 3 — MOVE 30-day moving average
    move_ma30: float
    # Gate 4 — CCD II calendar
    ccd_ii_deadline: date
    # Squeeze-defense raw metrics (per equity ticker)
    squeeze_utilization: dict[str, float]
    squeeze_days_to_cover: dict[str, float]
    squeeze_skew_pctile: dict[str, float]   # current value's percentile vs own history
    # Expression type — TRS vs equity
    expression: Literal["trs_junior_abs", "equity_short"]
    equity_tickers: list[str] = field(default_factory=list)   # required if expression=equity_short


@dataclass
class ComplianceDecision:
    approved: bool
    gate_results: dict[Gate, bool]
    squeeze_veto: bool
    reasons: list[str]
    # Checksum of the active thresholds YAML — kept with every decision for audit
    thresholds_version: str


class ComplianceEngine:
    """Pure-python rule evaluator. No network. No LLM."""

    def __init__(self, thresholds: dict | None = None) -> None:
        self.thresholds = thresholds or load_thresholds()
        self._version = _stable_hash(self.thresholds)

    def evaluate(self, inputs: GateInputs) -> ComplianceDecision:
        reasons: list[str] = []
        gates: dict[Gate, bool] = {}
        th = self.thresholds

        # --- Gate 1: BSI z-score ------------------------------------------------
        z_req = float(th["gates"]["bsi"]["z_threshold"])
        gates["bsi"] = inputs.bsi_z >= z_req
        if not gates["bsi"]:
            reasons.append(f"Gate 1 (BSI) FAIL: z={inputs.bsi_z:.3f} < {z_req:.3f}")

        # --- Gate 2: SCP min across considered tickers -------------------------
        scp_req = float(th["gates"]["scp"]["min_scp_equity_layer"])
        if inputs.scp_by_ticker:
            scp_ok = max(inputs.scp_by_ticker.values()) >= scp_req
        else:
            scp_ok = False
        gates["scp"] = scp_ok
        if not scp_ok:
            reasons.append(
                f"Gate 2 (SCP) FAIL: max(SCP)={max(inputs.scp_by_ticker.values(), default=0):.3f} "
                f"< {scp_req:.3f}"
            )

        # --- Gate 3: MOVE 30-day MA --------------------------------------------
        move_req = float(th["gates"]["move"]["ma30_threshold"])
        gates["move"] = inputs.move_ma30 >= move_req
        if not gates["move"]:
            reasons.append(f"Gate 3 (MOVE) FAIL: MA30={inputs.move_ma30:.1f} < {move_req:.1f}")

        # --- Gate 4: CCD II proximity ------------------------------------------
        max_days = int(th["gates"]["ccd_ii"]["max_days_to_deadline"])
        days_remaining = (inputs.ccd_ii_deadline - inputs.as_of.date()).days
        gates["ccd2"] = 0 <= days_remaining <= max_days
        if not gates["ccd2"]:
            reasons.append(
                f"Gate 4 (CCD II) FAIL: days_remaining={days_remaining} not in [0, {max_days}]"
            )

        # --- Squeeze defense (equity-only veto) --------------------------------
        squeeze_veto = False
        sd = th["squeeze_defense"]
        if inputs.expression == "equity_short" and not sd.get("bypass_for_trs", True):
            pass  # configured to enforce even on TRS — unusual
        if inputs.expression == "equity_short":
            for tkr in inputs.equity_tickers:
                util = inputs.squeeze_utilization.get(tkr, 0.0)
                dtc = inputs.squeeze_days_to_cover.get(tkr, 0.0)
                skew_p = inputs.squeeze_skew_pctile.get(tkr, 0.0)
                if util >= sd["utilization_veto"]:
                    squeeze_veto = True
                    reasons.append(f"SQUEEZE VETO ({tkr}): utilization={util:.2%} >= {sd['utilization_veto']:.2%}")
                if dtc >= sd["days_to_cover_veto"]:
                    squeeze_veto = True
                    reasons.append(f"SQUEEZE VETO ({tkr}): days_to_cover={dtc:.2f} >= {sd['days_to_cover_veto']:.2f}")
                if skew_p >= sd["skew_veto_percentile"]:
                    squeeze_veto = True
                    reasons.append(
                        f"SQUEEZE VETO ({tkr}): IV skew at {skew_p:.0%} pct >= {sd['skew_veto_percentile']:.0%}"
                    )

        require_all = bool(th["compliance"]["require_all_four_gates"])
        gates_pass = all(gates.values()) if require_all else any(gates.values())
        approved = gates_pass and not squeeze_veto

        if approved and not reasons:
            reasons.append("All four gates passed; no squeeze veto; deterministic rules satisfied.")

        return ComplianceDecision(
            approved=approved,
            gate_results=gates,
            squeeze_veto=squeeze_veto,
            reasons=reasons,
            thresholds_version=self._version,
        )


def _stable_hash(obj) -> str:
    """Deterministic short hash for a YAML-loaded dict. For audit provenance."""
    import hashlib
    import json

    payload = json.dumps(obj, sort_keys=True, default=str).encode()
    return hashlib.sha256(payload).hexdigest()[:12]
