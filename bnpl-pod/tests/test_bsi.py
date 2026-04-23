"""
Canonical BSI regression suite (paper v2.0.1).

Pins the paper's empirical claims to the code forever:

  * Paper §7.2 headline, 17 January 2025 (Reg Z deadline):
        z_bsi = +9.6870 σ   (±1e-3, tighter bound would chase numerics)
        bsi   =  7.4062     (level, for completeness)
        γ_cfpb = γ_move = γ_appstore = 1  (every load-bearing pillar online)

  * Sprint Q bypass wiring (compliance_engine + thresholds.yaml):
        |z_bsi| >= bypass_z_threshold (10.0 σ) AND conjunction gates fail
            =>  bypass_fired = True, approved = True
        |z_bsi| <  bypass_z_threshold
            =>  bypass_fired = False, approved = False (when conjunction
                fails), AND bypass_fired = False on every clean PASS path.

  * Paper v2.0.1 carry-over disclosure (compliance_engine Gate 1):
        Every Gate-1 PASS or FAIL reason string MUST contain the literal
        substring "carry-over" so a downstream auditor cannot confuse the
        v1-calibrated +1.5 σ threshold for a re-fit-on-EWMA rule.

The paper-headline regression is warehouse-gated: it runs only when the
default DuckDB warehouse is present and carries a 2025-01-17 row. That
guard prevents false-red on fresh clones that have not yet run the data
pipeline.

The Equation-(1)-shape and coverage-gate tests are purely synthetic and
run unconditionally — they guard the scorer's mathematical contract
independently of the warehouse state.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from agents.compliance_engine import ComplianceEngine, GateInputs
from data.settings import settings
from signals import bsi
from signals.bsi import (
    ALL_PILLARS,
    BSISpec,
    compute_bsi,
    compute_bsi_from_warehouse,
    load_spec,
)


# ---------------------------------------------------------------------------
# Warehouse guard — skip the headline regression pin if the DuckDB file is
# absent or the Reg-Z date is missing (fresh clone, pipeline not yet run).
# ---------------------------------------------------------------------------

REG_Z_DEADLINE = pd.Timestamp("2025-01-17")


def _warehouse_has_reg_z_row() -> bool:
    db_path = Path(settings.duckdb_path)
    if not db_path.exists():
        return False
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        (cnt,) = con.execute(
            "SELECT COUNT(*) FROM bsi_daily WHERE observed_at = ?",
            [REG_Z_DEADLINE.date()],
        ).fetchone()
        con.close()
    except Exception:  # noqa: BLE001 — duckdb.BinderException, IOError, etc.
        return False
    return cnt >= 1


_warehouse_ok = _warehouse_has_reg_z_row()


# ===========================================================================
# 1.  Paper headline regression pin  (§7.2, 17 January 2025)
# ===========================================================================

@pytest.mark.skipif(
    not _warehouse_ok,
    reason="Warehouse missing or 2025-01-17 row absent — run the data "
           "ingest pipeline to arm the paper-headline regression pin.",
)
def test_bsi_paper_headline_2025_01_17():
    """
    The load-bearing regression assertion of the whole project.

    paper_formal.tex §7.2 quotes z_bsi = +9.69 σ on 2025-01-17 — the
    single day BSI crossed +10 σ was also the Reg Z deadline pulse the
    paper frames as the Subprime-2.0 blind-spot signature. If this
    number drifts, the paper's empirical centerpiece no longer matches
    the shipped code.
    """
    df = compute_bsi_from_warehouse()
    assert REG_Z_DEADLINE in df.index, (
        "2025-01-17 missing from canonical bsi frame — warehouse may be stale."
    )
    row = df.loc[REG_Z_DEADLINE]
    assert row["z_bsi"] == pytest.approx(9.6870, abs=1e-3), (
        f"paper v2.0.1 §7.2 headline drifted: expected z_bsi ≈ +9.6870 σ, "
        f"got {row['z_bsi']:.6f}. If this is an intentional recalibration, "
        f"update the paper in the SAME commit."
    )
    assert row["bsi"] == pytest.approx(7.4062, abs=1e-2)
    # Every load-bearing pillar was online on the Reg-Z date — the paper
    # relies on this for the Subprime-2.0 blind-spot argument.
    assert row["gamma_cfpb"] == 1.0
    assert row["gamma_move"] == 1.0
    assert row["gamma_appstore"] == 1.0


# ===========================================================================
# 2.  Equation (1) mathematical-shape tests  (synthetic, warehouse-free)
# ===========================================================================

def _two_pillar_panel(n: int = 600) -> pd.DataFrame:
    """
    Deterministic two-pillar panel: `cfpb` on a 600-day ramp, `move`
    steady around 100. Other pillars all-NaN so coverage gate zeros
    them out. 600 days comfortably clears the 250-day EWMA halflife.
    """
    idx = pd.date_range("2023-01-01", periods=n, freq="D")
    cfpb_series = np.linspace(50.0, 110.0, n)
    # move — bounded oscillation around 100 so EWMA σ is non-zero.
    move_series = 100.0 + 3.0 * np.sin(np.linspace(0.0, 20 * np.pi, n))
    panel = pd.DataFrame(
        {p: np.full(n, np.nan) for p in ALL_PILLARS},
        index=idx,
    )
    panel["cfpb"] = cfpb_series
    panel["move"] = move_series
    return panel


def test_bsi_spec_weights_must_sum_to_one():
    bad = BSISpec(weights={p: 0.2 for p in ALL_PILLARS})  # 7 × 0.2 = 1.4
    with pytest.raises(ValueError, match="weights must sum to 1"):
        bad.validate()


def test_bsi_spec_rejects_missing_pillar_key():
    incomplete = {p: 1.0 / 6 for p in ALL_PILLARS[:-1]}   # drop `macro`
    bad = BSISpec(weights=incomplete)
    with pytest.raises(ValueError, match="missing pillar"):
        bad.validate()


def test_compute_bsi_returns_expected_columns():
    out = compute_bsi(_two_pillar_panel())
    assert "bsi" in out.columns and "z_bsi" in out.columns
    for p in ALL_PILLARS:
        assert f"z_{p}" in out.columns
        assert f"gamma_{p}" in out.columns


def test_coverage_gate_zeros_empty_pillars():
    """All-NaN pillars must receive gamma=0 and contribute 0 to BSI."""
    out = compute_bsi(_two_pillar_panel())
    # Inspect the final row — by day 600 every pillar's coverage has
    # stabilised. NaN pillars (trends/reddit/appstore/vitality/macro)
    # must all carry gamma = 0.
    last = out.iloc[-1]
    for p in ("trends", "reddit", "appstore", "vitality", "macro"):
        assert last[f"gamma_{p}"] == 0.0, f"pillar {p} should be gated off"
    assert last["gamma_cfpb"] == 1.0
    assert last["gamma_move"] == 1.0


def test_compute_bsi_level_positive_on_ramp_pillar():
    """
    A monotone-rising CFPB pillar must produce a positive BSI level at
    the tail (the pillar is well above its own EWMA mean by construction).
    """
    out = compute_bsi(_two_pillar_panel())
    tail_bsi = out["bsi"].dropna().iloc[-1]
    assert tail_bsi > 0.0


def test_compute_bsi_is_deterministic():
    """Running the scorer twice on identical input yields identical output."""
    panel = _two_pillar_panel()
    a = compute_bsi(panel)
    b = compute_bsi(panel)
    pd.testing.assert_frame_equal(a, b)


def test_load_spec_from_yaml_validates():
    """YAML-driven spec must satisfy validate() by construction."""
    spec = load_spec()
    spec.validate()
    # EWMA halflife is the paper's canonical 250 trading days.
    assert spec.ewma_halflife_days == 250


# ===========================================================================
# 3.  Compliance-engine bypass wiring  (Sprint Q, thresholds.yaml)
# ===========================================================================

def test_bypass_fires_when_z_exceeds_super_threshold_and_other_gates_fail():
    """
    Core Sprint Q contract: |z_bsi| >= bypass_z_threshold must approve
    on BSI alone when MOVE and the reg-catalyst gate would otherwise
    reject. Uses the YAML-loaded default threshold (10.0 σ per
    config/thresholds.yaml).
    """
    engine = ComplianceEngine()
    inputs = GateInputs(
        as_of=datetime(2025, 1, 17),
        bsi_z=12.5,                     # well above 10 σ
        scp_by_ticker={},
        move_ma30=50.0,                 # fails Gate 2
        nearest_catalyst_date=None,     # fails Gate 3
    )
    decision = engine.evaluate(inputs)
    assert decision.bypass_fired is True
    assert decision.approved is True
    # Bypass reason must be inserted at position 0 per engine contract.
    assert "BSI-only bypass FIRED" in decision.reasons[0]


def test_bypass_does_not_fire_below_super_threshold():
    """
    A +9.69 σ reading — paper's flagship — is BELOW the 10 σ bypass.
    With every other gate failing, the trade must reject cleanly and
    NOT mark bypass_fired.
    """
    engine = ComplianceEngine()
    inputs = GateInputs(
        as_of=datetime(2025, 1, 17),
        bsi_z=9.687,                    # paper headline, sub-bypass
        scp_by_ticker={},
        move_ma30=50.0,
        nearest_catalyst_date=None,
    )
    decision = engine.evaluate(inputs)
    assert decision.bypass_fired is False
    assert decision.approved is False


def test_clean_three_gate_pass_does_not_fire_bypass():
    """
    When every conjunction gate passes independently, approval must come
    through the conjunction path — bypass_fired must stay False and the
    consolidated 'All three gates passed' headline must emit.
    """
    from datetime import date
    engine = ComplianceEngine()
    inputs = GateInputs(
        as_of=datetime(2025, 1, 17),
        bsi_z=5.0,                              # passes Gate 1 (>= 1.5)
        scp_by_ticker={"AFRM": 3.0},
        move_ma30=150.0,                        # passes Gate 2 (>= 120)
        nearest_catalyst_date=date(2025, 2, 1), # passes Gate 3 (within 180d)
    )
    decision = engine.evaluate(inputs)
    assert decision.approved is True
    assert decision.bypass_fired is False
    assert any("All three gates passed" in r for r in decision.reasons), (
        "Consolidated approval headline was suppressed — the post-merge "
        "fix to emit on `approved and not bypass_fired` has regressed."
    )


# ===========================================================================
# 4.  Paper v2.0.1 Gate-1 carry-over disclosure marker  (audit invariant)
# ===========================================================================

def test_gate1_pass_reason_carries_v1_carry_over_marker():
    engine = ComplianceEngine()
    from datetime import date
    inputs = GateInputs(
        as_of=datetime(2025, 1, 17),
        bsi_z=5.0,
        scp_by_ticker={},
        move_ma30=150.0,
        nearest_catalyst_date=date(2025, 2, 1),
    )
    decision = engine.evaluate(inputs)
    gate1 = [r for r in decision.reasons if r.startswith("Gate 1 (BSI) PASS")]
    assert gate1, "Gate 1 PASS reason missing"
    assert "carry-over" in gate1[0], (
        "Gate-1 PASS reason must carry the v1-calibrated carry-over "
        "provenance marker (paper v2.0.1 §6)."
    )


def test_gate1_fail_reason_carries_v1_carry_over_marker():
    engine = ComplianceEngine()
    inputs = GateInputs(
        as_of=datetime(2025, 1, 17),
        bsi_z=0.5,                       # below +1.5 σ
        scp_by_ticker={},
        move_ma30=150.0,
        nearest_catalyst_date=None,
    )
    decision = engine.evaluate(inputs)
    gate1 = [r for r in decision.reasons if r.startswith("Gate 1 (BSI) FAIL")]
    assert gate1, "Gate 1 FAIL reason missing"
    assert "carry-over" in gate1[0]
