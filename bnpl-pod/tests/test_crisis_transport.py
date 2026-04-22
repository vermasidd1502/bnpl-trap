"""Offline tests for quant.crisis_transport."""
from __future__ import annotations

import numpy as np
import pytest

from quant import crisis_transport as ct


# --- transport() ----------------------------------------------------------
def test_transport_unit_scalers_near_baseline():
    r = ct.transport(1.0, 1.0)
    assert r.params.theta == pytest.approx(ct.AUTO_BAD_THETA)
    assert r.params.kappa == pytest.approx(ct.AUTO_BAD_KAPPA)


def test_transport_phi_theta_raises_theta():
    r_low  = ct.transport(1.0, 1.0)
    r_high = ct.transport(1.8, 1.0)
    assert r_high.params.theta > r_low.params.theta


def test_transport_phi_kappa_slows_reversion():
    """Higher phi_kappa → SMALLER kappa (slower mean reversion = more persistence)."""
    r_low  = ct.transport(1.0, 1.0)
    r_high = ct.transport(1.0, 11.0)
    assert r_high.params.kappa < r_low.params.kappa


def test_transport_enforces_feller():
    r = ct.transport(1.8, 11.0)
    assert r.params.feller_ok()


# --- strategy_sharpe ------------------------------------------------------
def test_strategy_sharpe_monotone_in_severity():
    s_low  = ct.strategy_sharpe(1.2, 8.0, n_paths=500, seed=11)
    s_high = ct.strategy_sharpe(1.8, 8.0, n_paths=500, seed=11)
    assert s_high > s_low


def test_strategy_sharpe_is_finite_number():
    s = ct.strategy_sharpe(1.5, 8.0, n_paths=300, seed=7)
    assert np.isfinite(s)


# --- Joint 3×3 grid -------------------------------------------------------
def test_sensitivity_grid_shape_and_finiteness():
    pt = [1.2, 1.5, 1.8]
    pk = [5.0, 8.0, 11.0]
    g = ct.sensitivity_grid(pt, pk, n_paths=250)
    assert g.shape == (3, 3)
    assert np.isfinite(g).all()


def test_sensitivity_grid_severity_monotone_within_column():
    """At fixed phi_kappa, rising phi_theta should raise Sharpe."""
    pt = [1.2, 1.5, 1.8]
    pk = [8.0]
    g = ct.sensitivity_grid(pt, pk, n_paths=500)
    col = g[:, 0]
    assert col[0] < col[2]


def test_sharpe_zero_contour_exposed():
    # Just confirm re-export works.
    from quant.crisis_transport import sharpe_zero_contour
    assert callable(sharpe_zero_contour)
