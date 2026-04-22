"""Offline tests for signals.sensitivity + dashboard.sbg_dashboard helpers."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from signals import sensitivity as sens
from dashboard import sbg_dashboard as dash


# --- sensitivity grid -----------------------------------------------------
def test_sensitivity_grid_shape():
    pt = [1.2, 1.5, 1.8]
    pk = [5.0, 8.0, 11.0]
    g = sens.sensitivity_grid(pt, pk)
    assert g.shape == (3, 3)
    assert np.isfinite(g).all()


def test_sensitivity_monotone_in_severity():
    """Higher phi_theta → higher Sharpe at fixed phi_kappa (strategy benefits from
    wider stressed spreads)."""
    pk = [8.0]
    g = sens.sensitivity_grid([1.2, 1.5, 1.8], pk)
    col = g[:, 0]
    assert col[0] < col[1] < col[2]


def test_sharpe_zero_contour_detected_somewhere():
    pt = np.linspace(1.0, 1.8, 9)
    pk = np.linspace(4.0, 12.0, 9)
    g = sens.sensitivity_grid(pt, pk)
    pts = sens.sharpe_zero_contour(pt, pk, g)
    assert len(pts) >= 1   # contour crosses at least one row
    for (t, k) in pts:
        assert pt.min() <= t <= pt.max()
        assert pk.min() <= k <= pk.max()


# --- alert classifier -----------------------------------------------------
def test_alert_red_on_freeze_flag():
    row = pd.Series({"z_bsi": 0.3, "freeze_flag": True})
    assert dash.classify_alert(row, co_occur_pctile=0.5) == "RED"


def test_alert_red_on_extreme_z():
    row = pd.Series({"z_bsi": 2.4, "freeze_flag": False})
    assert dash.classify_alert(row, co_occur_pctile=0.0) == "RED"


def test_alert_yellow_on_elevated_z():
    row = pd.Series({"z_bsi": 1.2, "freeze_flag": False})
    assert dash.classify_alert(row, co_occur_pctile=0.1) == "YELLOW"


def test_alert_yellow_on_co_occurrence():
    row = pd.Series({"z_bsi": 0.0, "freeze_flag": False})
    assert dash.classify_alert(row, co_occur_pctile=0.85) == "YELLOW"


def test_alert_green_when_calm():
    row = pd.Series({"z_bsi": -0.3, "freeze_flag": False})
    assert dash.classify_alert(row, co_occur_pctile=0.4) == "GREEN"


# --- co-occurrence percentile --------------------------------------------
def test_co_occurrence_returns_unit_when_today_is_peak():
    df = pd.DataFrame({
        "c_reddit":  [-1.0, -0.5, 0.0, 0.5, 2.0],
        "c_trends":  [-1.0, -0.5, 0.0, 0.5, 2.0],
    })
    pct = dash.co_occurrence_percentile(df)
    assert pct == pytest.approx(1.0)


def test_co_occurrence_low_when_today_is_calm():
    df = pd.DataFrame({
        "c_reddit":  [1.5, 1.0, 0.5, 0.0, -1.5],
        "c_trends":  [1.5, 1.0, 0.5, 0.0, -1.5],
    })
    pct = dash.co_occurrence_percentile(df)
    assert pct <= 0.25
