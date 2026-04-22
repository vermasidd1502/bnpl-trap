"""Offline tests for quant.heston_scp — no QuantLib dependency required."""
from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from data.schema import DDL
from quant import heston_scp as hs


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(hs.settings, "duckdb_path", db)
    return db


# --- ATM-IV picker --------------------------------------------------------
def test_atm_iv_selects_closest_call_in_dte_band():
    opts = [
        {"option_type": "C", "strike": 90,  "iv": 0.70, "dte": 30},
        {"option_type": "C", "strike": 100, "iv": 0.62, "dte": 30},
        {"option_type": "C", "strike": 110, "iv": 0.68, "dte": 30},
        {"option_type": "P", "strike": 100, "iv": 0.80, "dte": 30},   # skip puts
        {"option_type": "C", "strike": 100, "iv": 0.95, "dte": 5},    # outside DTE band
    ]
    iv = hs._atm_iv_from_chain(opts, spot=100.0)
    assert iv == pytest.approx(0.62)


def test_atm_iv_none_when_no_call_near_spot():
    opts = [{"option_type": "C", "strike": 200, "iv": 0.5, "dte": 30}]
    assert hs._atm_iv_from_chain(opts, spot=100.0) is None


def test_atm_iv_handles_empty():
    assert hs._atm_iv_from_chain([], spot=100.0) is None
    assert hs._atm_iv_from_chain([{"option_type": "C"}], spot=0) is None


# --- realized vol ---------------------------------------------------------
def test_realized_vol_matches_constant_drift():
    # constant multiplicative path → zero vol
    prices = [100.0 * (1.01 ** i) for i in range(25)]
    hv = hs.realized_vol(prices, window=20)
    assert hv is not None and hv < 1e-6


def test_realized_vol_positive_on_random_walk():
    import numpy as np
    rng = np.random.default_rng(0)
    log_prices = np.cumsum(rng.normal(0, 0.02, 40))
    prices = (100 * np.exp(log_prices)).tolist()
    hv = hs.realized_vol(prices, window=20)
    assert hv is not None and hv > 0.1


def test_realized_vol_none_on_short_history():
    assert hs.realized_vol([100.0, 101.0], window=20) is None


# --- SCP + z-score --------------------------------------------------------
def test_scp_value_passthrough_and_missing():
    assert hs.scp_value(0.60, 0.45) == pytest.approx(0.15)
    assert hs.scp_value(None, 0.45) is None
    assert hs.scp_value(0.60, None) is None


def test_rolling_zscore_picks_outlier():
    series = [0.10] * 30 + [0.40]
    z = hs.rolling_zscore(series, window=252)
    assert z[-1] is not None and z[-1] > 3.0
    # early elements (< 20 obs) should be None
    assert z[0] is None


def test_gate_fires_at_90th_pctile():
    assert hs.gate_fires(1.5) is True
    assert hs.gate_fires(1.0) is False
    assert hs.gate_fires(None) is False


# --- End-to-end DB integration -------------------------------------------
def _seed_chain(db: Path, ticker: str = "AFRM",
                n_days: int = 60, spike_day: int = 55,
                spot_start: float = 50.0):
    con = duckdb.connect(str(db))
    d0 = date(2024, 5, 1)
    for i in range(n_days):
        d = d0 + timedelta(days=i)
        spot = spot_start + 0.1 * i
        atm_iv = 0.60 if i != spike_day else 1.20   # vol spike
        # one expiry 30 days out
        exp = d + timedelta(days=30)
        con.execute(
            """INSERT INTO options_chain (ticker, observed_at, expiry, strike,
               option_type, iv, underlying_price)
               VALUES (?, ?, ?, ?, 'C', ?, ?)""",
            [ticker, d, exp, spot, atm_iv, spot],
        )
        # a put far OTM — parser should skip for ATM-IV
        con.execute(
            """INSERT INTO options_chain (ticker, observed_at, expiry, strike,
               option_type, iv, underlying_price)
               VALUES (?, ?, ?, ?, 'P', 0.90, ?)""",
            [ticker, d, exp, spot * 0.7, spot],
        )
    con.close()


def test_compute_scp_writes_and_gate_lights_on_spike(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(hs, "_calibrate_ql", lambda *a, **kw: None)  # disable QL
    _seed_chain(tmp_duckdb, n_days=60, spike_day=55)
    n = hs.compute_scp_for("AFRM", calibrate=False)
    assert n >= 40

    con = duckdb.connect(str(tmp_duckdb))
    rows = con.execute(
        "SELECT observed_at, scp, z_scp FROM scp_daily "
        "WHERE ticker='AFRM' ORDER BY observed_at"
    ).fetchall()
    con.close()
    assert any(r[2] is not None and r[2] > 1.28 for r in rows)


def test_compute_scp_is_idempotent(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(hs, "_calibrate_ql", lambda *a, **kw: None)
    _seed_chain(tmp_duckdb)
    hs.compute_scp_for("AFRM", calibrate=False)
    hs.compute_scp_for("AFRM", calibrate=False)
    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM scp_daily").fetchone()
    (dist,) = con.execute("SELECT COUNT(DISTINCT observed_at) FROM scp_daily").fetchone()
    con.close()
    assert cnt == dist   # no dup rows


def test_heston_params_stored_when_calibrator_returns(monkeypatch, tmp_duckdb):
    fake = hs.HestonParams(kappa=1.5, theta=0.04, sigma=0.3, rho=-0.5,
                           v0=0.04, rmse=0.002)
    monkeypatch.setattr(hs, "_calibrate_ql", lambda *a, **kw: fake)
    _seed_chain(tmp_duckdb, n_days=5)
    hs.compute_scp_for("AFRM", calibrate=True)
    con = duckdb.connect(str(tmp_duckdb))
    row = con.execute(
        "SELECT kappa, theta, sigma, rho, v0 FROM scp_daily WHERE ticker='AFRM' LIMIT 1"
    ).fetchone()
    con.close()
    assert row[0] == pytest.approx(1.5)
    assert row[1] == pytest.approx(0.04)
