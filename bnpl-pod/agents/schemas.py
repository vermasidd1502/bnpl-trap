"""Typed schemas shared by every agent in the pod.

Keeping them in one module means the graph orchestrator, tick runner, and
downstream auditors all speak the same vocabulary. Pydantic would work but
introduces a hard dependency — dataclasses are sufficient and serialize
cleanly to JSON via `json.dumps(asdict(obj), default=str)`.

Fix #2 (MASTERPLAN v4.1, post-critique): the equity_short expression has
been RETIRED. The pod's only trade expression is `trs_junior_abs`; any
market-neutral hedging is carried by a static macro-hedge sleeve
(HYG short or 2Y UST futures) sized separately from the Mean-CVaR LP.
The `MacroHedgeSpec` dataclass below is the on-wire representation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal


@dataclass
class MacroReport:
    as_of: datetime
    bsi: float                 # latest raw BSI
    bsi_z: float               # latest z-scored BSI (vs rolling 180d)
    move_ma30: float           # MOVE 30-day MA
    freeze_flag: bool          # any treated firm in freeze state
    advisory: str = ""         # LLM narrative (optional)


@dataclass
class QuantReport:
    as_of: datetime
    scp_by_ticker: dict[str, float]         # latest per-ticker SCP z-score (telemetry)
    scp_gate_fires: dict[str, bool]         # per-ticker SCP-above-threshold (telemetry)
    lambda_total_by_issuer: dict[str, float]  # latest λ_total
    advisory: str = ""


@dataclass
class RiskReport:
    """Squeeze telemetry — retained for dashboard/paper. Post-Fix #2 the
    squeeze layer no longer vetoes any trade because equity_short is gone.
    Risk_manager populates these fields for transparency only.
    """
    as_of: datetime
    squeeze_utilization: dict[str, float]
    squeeze_days_to_cover: dict[str, float]
    squeeze_skew_pctile: dict[str, float]
    squeeze_score_by_ticker: dict[str, float]
    squeeze_veto_candidate: bool            # TRUE if any ticker's own module flagged veto
    advisory: str = ""


@dataclass
class MacroHedgeSpec:
    """Static macro-hedge sleeve, sized OUTSIDE the Mean-CVaR LP.

    Fix #2 rationale: mixing static-sized equity shorts with dynamic LP
    sizing contaminates the optimizer's risk budget and re-introduces the
    squeeze exposure the thesis explicitly avoids. A clean separation —
    LP solves the TRS book; this sleeve hedges credit-beta to index — is
    the institutionally-defensible structure.

    Fields
    ------
    instrument    HYG_SHORT (credit-beta hedge) or ZT_FUT (rates hedge)
    sizing_rule   beta_credit (|hedge| = β·TRS_gross) or dv01_neutral
                  (size so ΔHedge_DV01 ≈ ΔTRS_DV01)
    notional      Signed dollar notional. Negative for short positions.
    hedge_ratio   The β_credit or DV01 multiplier actually applied.
    trs_gross     Aggregate TRS abs-notional this sleeve is hedging.
                  Kept with the spec for audit reproducibility.
    rationale     Short free-text — e.g., "β_credit=0.60 per thresholds.yaml".
    """
    instrument: Literal["HYG_SHORT", "ZT_FUT"]
    sizing_rule: Literal["beta_credit", "dv01_neutral"]
    notional: float
    hedge_ratio: float
    trs_gross: float
    rationale: str = ""


@dataclass
class PodDecision:
    run_id: str
    as_of: datetime
    macro: MacroReport
    quant: QuantReport
    risk: RiskReport
    # Fix #2: the only live expression is TRS on junior ABS tranches. The
    # Literal is kept single-valued (not stripped) so JSON-schema consumers
    # still see a discriminator and any forward-compat additions slot in
    # here rather than through a new field.
    expression: Literal["trs_junior_abs"] = "trs_junior_abs"
    equity_tickers: list[str] = field(default_factory=list)
    # Sprint H: renamed from `ccd_ii_deadline`. This is the date of the
    # nearest material regulatory catalyst at `as_of`, resolved via
    # data.regulatory_calendar. `None` means no material catalyst was
    # inside the compliance horizon — gate 3 fails with a dedicated reason.
    nearest_catalyst_date: date | None = None

    # Populated by the compliance engine after the agents run.
    compliance_approved: bool = False
    gate_bsi: bool = False
    gate_scp: bool = False       # telemetry post-Fix #2 (not in approval AND)
    gate_move: bool = False
    gate_ccd2: bool = False
    squeeze_veto: bool = False   # telemetry post-Fix #2 (dead — always False)
    compliance_reasons: list[str] = field(default_factory=list)
    thresholds_version: str = ""
    llm_advisory: str = ""
    trade_signal_json: str = ""
