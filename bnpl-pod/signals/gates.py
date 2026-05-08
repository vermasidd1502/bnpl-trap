"""
signals/gates.py — 5-gate archetype-aware trade-firing logic.

Architecture (paper §10):
  G1  BSI               consumer-distress sentiment  (CFPB / Reddit / AppStore / Bluesky / GTrends / 8-K / vitality)
  G2  SCP               firm-specific market stress  (single-counterparty pillar — ABS spread, equity vol)
  G3  MOVE              macro rates-vol regime
  G4  CCD               cross-firm contagion / network effects
  G5  FDS               fundamentals distress score  (NCO ↑, provisions ↑, originations ↓, ABS CNL breach)

Each MASCOT archetype has its own gate-count threshold AND a list of mandatory
gates that cannot be skipped. All archetypes share the constraint that G1 BSI
is mandatory (no thesis without the leading-indicator anchor) and that they
cannot skip BOTH G2 SCP and G5 FDS (must have either market-implied OR
accounting confirmation, never pure-sentiment).

Position size is monotonically decreasing in aggressiveness:
  BLITZ    (3/5)  →  0.5% of equity   — high recall, low precision
  SCOUT    (4/5)  →  1.0% of equity   — balanced
  GUARDIAN (5/5)  →  2.0% of equity   — high precision, low recall
  ROBO     (cont) →  size ∝ score      — algorithmic, continuous

The framework is pre-registered: thresholds, mandatory sets, and size caps
are constants here, NOT learned from data, to prevent HARKing.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal, Mapping

GateName    = Literal["G1_BSI", "G2_SCP", "G3_MOVE", "G4_CCD", "G5_FDS"]
GateStatus  = Literal["PASS", "FAIL", "UNKNOWN"]
Archetype   = Literal["BLITZ", "SCOUT", "GUARDIAN", "ROBO"]

ALL_GATES: tuple[GateName, ...] = ("G1_BSI", "G2_SCP", "G3_MOVE", "G4_CCD", "G5_FDS")

# ---------------------------------------------------------------------------
# Pre-registered archetype rules — paper §10 Table 10.1
# ---------------------------------------------------------------------------

GATE_REQUIRED_COUNT: dict[Archetype, int] = {
    "BLITZ":    3,   # any 3 of 5 (with mandatory subset satisfied)
    "SCOUT":    4,   # any 4 of 5
    "GUARDIAN": 5,   # all 5 must fire
    "ROBO":     0,   # not a count-based archetype; uses weighted score (see ROBO_THRESHOLD)
}

# Mandatory gates per archetype — these MUST be PASS regardless of total count
GATE_MANDATORY: dict[Archetype, tuple[GateName, ...]] = {
    "BLITZ":    ("G1_BSI",),                           # only BSI is non-negotiable
    "SCOUT":    ("G1_BSI", "G5_FDS"),                  # sentiment + accounting both required
    "GUARDIAN": ("G1_BSI", "G2_SCP", "G3_MOVE",
                 "G4_CCD", "G5_FDS"),                  # all five
    "ROBO":     ("G1_BSI",),                           # at minimum BSI; rest absorbed in score
}

# Position-size cap per archetype, expressed as fraction of total equity
POSITION_SIZE_PCT: dict[Archetype, float] = {
    "BLITZ":    0.005,   # 0.5%
    "SCOUT":    0.010,   # 1.0%
    "GUARDIAN": 0.020,   # 2.0%
    "ROBO":     0.010,   # baseline; scales with score in evaluate_robo()
}

# Hard-stop discipline (one-sided, applies to short side; flip sign for long)
STOP_LOSS_PCT: dict[Archetype, float] = {
    "BLITZ":    0.15,    # tight — high false-positive risk justifies fast exit
    "SCOUT":    0.20,    # normal
    "GUARDIAN": 0.25,    # wider — high conviction earns the right to weather noise
    "ROBO":     0.20,
}

# ROBO scoring (continuous; not gate-count based) ----------------------------
# Weights sum to 1.0; a gate's contribution is its weight * 1[status==PASS]
ROBO_GATE_WEIGHTS: dict[GateName, float] = {
    "G1_BSI":  0.35,
    "G2_SCP":  0.20,
    "G3_MOVE": 0.10,
    "G4_CCD":  0.15,
    "G5_FDS":  0.20,
}
ROBO_THRESHOLD: float = 0.55   # composite score must clear this for ROBO to fire

# Cross-archetype invariant: cannot skip BOTH G2 SCP and G5 FDS
# (no pure-sentiment shorts; must have market OR accounting confirmation)
NO_PURE_SENTIMENT_GATES: tuple[GateName, GateName] = ("G2_SCP", "G5_FDS")


# ---------------------------------------------------------------------------
# Evaluation result shape
# ---------------------------------------------------------------------------

@dataclass
class ArchetypeFireResult:
    archetype: Archetype
    fires: bool
    n_passing: int
    n_required: int
    missing_required: list[GateName] = field(default_factory=list)
    missing_optional: list[GateName] = field(default_factory=list)
    failed_invariant: str | None = None     # e.g. "NO_PURE_SENTIMENT" if both G2+G5 missing
    position_size_pct: float = 0.0
    stop_loss_pct: float = 0.0
    robo_score: float | None = None         # only set for ROBO
    rationale: str = ""

    def to_dict(self) -> dict:
        return {
            "archetype":         self.archetype,
            "fires":             self.fires,
            "n_passing":         self.n_passing,
            "n_required":        self.n_required,
            "missing_required":  list(self.missing_required),
            "missing_optional":  list(self.missing_optional),
            "failed_invariant":  self.failed_invariant,
            "position_size_pct": self.position_size_pct,
            "stop_loss_pct":     self.stop_loss_pct,
            "robo_score":        self.robo_score,
            "rationale":         self.rationale,
        }


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------

def _check_invariants(gate_states: Mapping[GateName, GateStatus]) -> str | None:
    """Returns the name of any failed cross-archetype invariant, or None."""
    g2, g5 = gate_states.get("G2_SCP", "UNKNOWN"), gate_states.get("G5_FDS", "UNKNOWN")
    if g2 != "PASS" and g5 != "PASS":
        return "NO_PURE_SENTIMENT"  # neither market nor accounting confirms — block all archetypes
    return None


def evaluate_archetype(archetype: Archetype,
                       gate_states: Mapping[GateName, GateStatus]) -> ArchetypeFireResult:
    """
    Evaluate whether a single archetype fires given the current 5-gate state.

    gate_states must contain all 5 gate names with status in {PASS, FAIL, UNKNOWN}.
    UNKNOWN counts as FAIL for firing logic (conservative — don't fire on unknowns).
    """
    if archetype == "ROBO":
        return _evaluate_robo(gate_states)

    invariant = _check_invariants(gate_states)
    mandatory = GATE_MANDATORY[archetype]
    required_count = GATE_REQUIRED_COUNT[archetype]

    passing = [g for g in ALL_GATES if gate_states.get(g) == "PASS"]
    n_passing = len(passing)

    missing_required = [g for g in mandatory if gate_states.get(g) != "PASS"]
    # Optional gates that didn't pass — informational only (display purposes)
    optional = [g for g in ALL_GATES if g not in mandatory]
    missing_optional = [g for g in optional if gate_states.get(g) != "PASS"]

    fires = (
        invariant is None
        and len(missing_required) == 0
        and n_passing >= required_count
    )

    pos = POSITION_SIZE_PCT[archetype] if fires else 0.0
    stop = STOP_LOSS_PCT[archetype] if fires else 0.0

    if fires:
        rationale = (f"{archetype} fires — {n_passing}/5 gates pass, all mandatory "
                     f"({', '.join(mandatory)}) confirmed.")
    elif invariant == "NO_PURE_SENTIMENT":
        rationale = ("Blocked by cross-archetype invariant: both G2 SCP (market) and "
                     "G5 FDS (accounting) must not be FAIL/UNKNOWN. Pure-sentiment "
                     "shorts are not permitted regardless of archetype.")
    elif missing_required:
        rationale = f"{archetype} blocked — missing mandatory gate(s): {', '.join(missing_required)}."
    else:
        short = required_count - n_passing
        rationale = f"{archetype} blocked — {n_passing}/5 passing; needs {short} more."

    return ArchetypeFireResult(
        archetype=archetype,
        fires=fires,
        n_passing=n_passing,
        n_required=required_count,
        missing_required=missing_required,
        missing_optional=missing_optional,
        failed_invariant=invariant,
        position_size_pct=pos,
        stop_loss_pct=stop,
        rationale=rationale,
    )


def _evaluate_robo(gate_states: Mapping[GateName, GateStatus]) -> ArchetypeFireResult:
    """ROBO uses a continuous weighted score, not a hard gate count."""
    invariant = _check_invariants(gate_states)
    score = sum(
        w for g, w in ROBO_GATE_WEIGHTS.items()
        if gate_states.get(g) == "PASS"
    )
    n_passing = sum(1 for g in ALL_GATES if gate_states.get(g) == "PASS")
    missing_required = ["G1_BSI"] if gate_states.get("G1_BSI") != "PASS" else []
    fires = (
        invariant is None
        and not missing_required
        and score >= ROBO_THRESHOLD
    )

    # Size scales with score above threshold; clipped to 1.5x baseline
    if fires:
        pos = POSITION_SIZE_PCT["ROBO"] * min(1.5, max(1.0, score / ROBO_THRESHOLD))
    else:
        pos = 0.0
    stop = STOP_LOSS_PCT["ROBO"] if fires else 0.0

    if fires:
        rationale = (f"ROBO fires — weighted score {score:.2f} ≥ threshold "
                     f"{ROBO_THRESHOLD:.2f}; G1 BSI mandatory confirmed.")
    elif invariant == "NO_PURE_SENTIMENT":
        rationale = "ROBO blocked — pure-sentiment invariant (need G2 SCP or G5 FDS)."
    elif missing_required:
        rationale = "ROBO blocked — G1 BSI mandatory not passing."
    else:
        rationale = f"ROBO blocked — score {score:.2f} below threshold {ROBO_THRESHOLD:.2f}."

    return ArchetypeFireResult(
        archetype="ROBO",
        fires=fires,
        n_passing=n_passing,
        n_required=0,
        missing_required=missing_required,
        missing_optional=[g for g in ALL_GATES if gate_states.get(g) != "PASS" and g != "G1_BSI"],
        failed_invariant=invariant,
        position_size_pct=pos,
        stop_loss_pct=stop,
        robo_score=round(score, 3),
        rationale=rationale,
    )


def evaluate_all_archetypes(gate_states: Mapping[GateName, GateStatus]
                           ) -> dict[Archetype, ArchetypeFireResult]:
    """Convenience: evaluate every archetype in one call."""
    return {a: evaluate_archetype(a, gate_states) for a in ("BLITZ", "SCOUT", "GUARDIAN", "ROBO")}


# ---------------------------------------------------------------------------
# Helper to convert raw signal values into PASS/FAIL/UNKNOWN gate states
# ---------------------------------------------------------------------------

# Pre-registered gate thresholds (paper §10 footnotes) -----------------------
GATE_THRESHOLDS = {
    "G1_BSI":   2.0,    # |z| ≥ 2.0 → PASS (legacy from SCOUT default)
    "G2_SCP":   1.5,    # firm-specific spread/equity-vol z ≥ 1.5
    "G3_MOVE":  120.0,  # MOVE ≥ 120 → rates vol regime supportive of credit-stress trades
    "G4_CCD":   0.30,   # cross-firm contagion index ≥ 0.30
    "G5_FDS":   1.5,    # fundamentals distress z ≥ 1.5 (PENDING — see TODO below)
}


def gate_states_from_signals(*, bsi_z: float | None = None,
                             scp_z: float | None = None,
                             move_level: float | None = None,
                             ccd_index: float | None = None,
                             fds_z: float | None = None) -> dict[GateName, GateStatus]:
    """
    Convert raw signal values into PASS/FAIL/UNKNOWN per pre-registered thresholds.

    Pass None for any signal that is not (yet) computable for the current ticker;
    that gate becomes UNKNOWN (conservative — counts as not-passing for firing).
    """
    def _grade(v, thresh):
        if v is None: return "UNKNOWN"
        return "PASS" if v >= thresh else "FAIL"

    return {
        "G1_BSI":   _grade(bsi_z,      GATE_THRESHOLDS["G1_BSI"]),
        "G2_SCP":   _grade(scp_z,      GATE_THRESHOLDS["G2_SCP"]),
        "G3_MOVE":  _grade(move_level, GATE_THRESHOLDS["G3_MOVE"]),
        "G4_CCD":   _grade(ccd_index,  GATE_THRESHOLDS["G4_CCD"]),
        # G5 FDS is the new gate — pending the EDGAR XBRL extension (NCO + provisions
        # + DPD migration + ABS CNL). Until shipped, callers pass fds_z=None and G5
        # resolves to UNKNOWN, which blocks SCOUT and GUARDIAN until the data lands.
        # That is the intended behavior — better to under-fire than over-fire.
        "G5_FDS":   _grade(fds_z,      GATE_THRESHOLDS["G5_FDS"]),
    }


__all__ = [
    "ALL_GATES", "GATE_REQUIRED_COUNT", "GATE_MANDATORY",
    "POSITION_SIZE_PCT", "STOP_LOSS_PCT", "ROBO_GATE_WEIGHTS",
    "ROBO_THRESHOLD", "GATE_THRESHOLDS",
    "ArchetypeFireResult",
    "evaluate_archetype", "evaluate_all_archetypes",
    "gate_states_from_signals",
]
