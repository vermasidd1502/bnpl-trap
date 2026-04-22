"""Offline tests for quant.jarrow_turnbull."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pytest

from data.schema import DDL
from quant import jarrow_turnbull as jt


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(jt.settings, "duckdb_path", db)
    return db


# --- CIR params -----------------------------------------------------------
def test_feller_enforced():
    bad = jt.CIRParams(kappa=0.2, theta=0.005, sigma=0.5)   # 2kθ = 0.002 < σ² = 0.25
    assert not bad.feller_ok()
    fixed = bad.enforce_feller()
    assert fixed.feller_ok()
    assert fixed.sigma < bad.sigma


def test_feller_noop_when_ok():
    good = jt.CIRParams(kappa=0.8, theta=0.04, sigma=0.05)
    assert good.feller_ok()
    fixed = good.enforce_feller()
    assert fixed.sigma == pytest.approx(good.sigma, rel=1e-3)


# --- affine link ----------------------------------------------------------
def test_affine_hazard_monotone_in_bsi():
    h0 = jt.affine_hazard(0.0, move=90.0)
    h1 = jt.affine_hazard(2.0, move=90.0)
    h2 = jt.affine_hazard(4.0, move=90.0)
    assert h0 < h1 < h2


def test_affine_hazard_respects_move_threshold():
    low = jt.affine_hazard(1.0, move=70.0)    # below 80 bps threshold
    high = jt.affine_hazard(1.0, move=140.0)
    assert high > low


def test_affine_hazard_clamps_negative_bsi():
    """Negative BSI shouldn't reduce hazard below alpha floor."""
    h_neg = jt.affine_hazard(-3.0, move=None)
    h_zero = jt.affine_hazard(0.0, move=None)
    assert h_neg == h_zero


# --- EWMA + cap -----------------------------------------------------------
def test_ewma_smoothes_spike():
    spike = [0.01, 0.01, 0.01, 0.20, 0.01]
    out = jt.ewma(spike, halflife=5.0)
    # spike day reduced, tail elevated but still below peak
    assert out[3] < spike[3]
    assert out[4] > spike[4]


def test_apply_cap():
    out = jt.apply_cap([0.01, 0.08, -0.02, 0.04], cap=0.05)
    assert out == [0.01, 0.05, 0.0, 0.04]


# --- CIR sim --------------------------------------------------------------
def test_simulate_cir_mean_converges_to_theta():
    params = jt.CIRParams(kappa=2.0, theta=0.03, sigma=0.05).enforce_feller()
    paths = jt.simulate_cir(params, lambda_0=0.03, horizon_days=252,
                            n_paths=2000, seed=7)
    terminal_mean = paths[:, -1].mean()
    assert terminal_mean == pytest.approx(params.theta, abs=0.01)
    assert (paths >= 0.0).all()    # full-truncation keeps it non-negative


def test_survival_probability_in_unit_interval():
    params = jt.CIRParams(kappa=0.5, theta=0.02, sigma=0.05).enforce_feller()
    paths = jt.simulate_cir(params, lambda_0=0.02, horizon_days=252,
                            n_paths=500, seed=11)
    S = jt.survival_probability(paths)
    assert ((0.0 < S) & (S <= 1.0)).all()
    # 1-year default prob around 2% → S ~ 0.98
    assert 0.9 < S.mean() < 1.0


# --- Pricing --------------------------------------------------------------
def test_price_tranche_higher_hazard_kills_tranche():
    """Junior tranche loss increases as hazard rises."""
    params = jt.CIRParams(kappa=0.5, theta=0.02, sigma=0.05).enforce_feller()
    low  = jt.simulate_cir(params, lambda_0=0.01, horizon_days=252, n_paths=1000, seed=3)
    high = jt.simulate_cir(
        jt.CIRParams(0.5, 0.04, 0.05).enforce_feller(),
        lambda_0=0.04, horizon_days=252, n_paths=1000, seed=3,
    )
    p_low  = jt.price_tranche(100.0, attach=0.00, detach=0.05,
                              spread_bps=700, ttm_days=252, lambda_total_path=low)
    p_high = jt.price_tranche(100.0, attach=0.00, detach=0.05,
                              spread_bps=700, ttm_days=252, lambda_total_path=high)
    assert p_high["tranche_loss_mean"] > p_low["tranche_loss_mean"]
    assert p_high["survival_mean"]     < p_low["survival_mean"]


def test_price_tranche_sigma_zero_limit():
    """With sigma≈0, survival should be deterministic exp(-lambda*T)."""
    params = jt.CIRParams(kappa=0.5, theta=0.02, sigma=1e-5).enforce_feller()
    paths = jt.simulate_cir(params, lambda_0=0.02, horizon_days=252,
                            n_paths=200, seed=1)
    S = jt.survival_probability(paths)
    # CIR with sigma->0 and lambda_0=theta stays near 0.02 → S ≈ exp(-0.02) ≈ 0.9802
    assert abs(S.mean() - np.exp(-0.02)) < 0.005


# --- Warehouse integration ------------------------------------------------
def test_build_issuer_hazard_writes_and_caps(tmp_duckdb):
    db = tmp_duckdb
    d0 = date(2024, 6, 3)
    con = duckdb.connect(str(db))
    # 10 BSI days, some very high so the cap kicks in
    for i in range(10):
        con.execute(
            "INSERT INTO bsi_daily (observed_at, bsi) VALUES (?, ?)",
            [d0 + timedelta(days=i), (20.0 if i == 5 else 1.0)],
        )
        con.execute(
            "INSERT INTO fred_series (series_id, observed_at, value) VALUES ('MOVE', ?, ?)",
            [d0 + timedelta(days=i), 120.0],
        )
    con.close()

    n = jt.build_issuer_hazard("AFRM", start=d0, end=d0 + timedelta(days=9))
    assert n == 10

    con = duckdb.connect(str(db))
    vals = [r[0] for r in con.execute(
        "SELECT lambda_total FROM jt_lambda WHERE issuer='AFRM' ORDER BY observed_at"
    ).fetchall()]
    con.close()
    # cap honored
    assert max(vals) <= jt.J_MAX + 1e-12
    # spike day (i=5) EWMA-smoothed → non-trivially elevated but below cap
    assert vals[5] > vals[0]


def test_build_issuer_hazard_idempotent(tmp_duckdb):
    db = tmp_duckdb
    d0 = date(2024, 6, 3)
    con = duckdb.connect(str(db))
    for i in range(5):
        con.execute(
            "INSERT INTO bsi_daily (observed_at, bsi) VALUES (?, ?)",
            [d0 + timedelta(days=i), 1.0],
        )
    con.close()
    jt.build_issuer_hazard("AFRM", start=d0, end=d0 + timedelta(days=4))
    jt.build_issuer_hazard("AFRM", start=d0, end=d0 + timedelta(days=4))
    con = duckdb.connect(str(db))
    (cnt,) = con.execute("SELECT COUNT(*) FROM jt_lambda").fetchone()
    con.close()
    assert cnt == 5
