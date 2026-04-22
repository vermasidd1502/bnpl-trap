"""Offline tests for quant.squeeze_defense."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from data.schema import DDL
from quant import squeeze_defense as sd


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(sd.settings, "duckdb_path", db)
    return db


# --- OTM call share -------------------------------------------------------
def test_otm_call_share_weighted_by_oi():
    opts = [
        {"option_type": "C", "strike": 100, "open_interest": 1000},   # ATM → not OTM
        {"option_type": "C", "strike": 115, "open_interest": 500},    # OTM
        {"option_type": "C", "strike": 130, "open_interest": 500},    # OTM
        {"option_type": "P", "strike": 90,  "open_interest": 9999},   # ignored
    ]
    share = sd.otm_call_share(opts, spot=100.0)
    assert share == pytest.approx(1000 / 2000)


def test_otm_call_share_none_when_no_oi():
    assert sd.otm_call_share([], 100.0) is None
    assert sd.otm_call_share(
        [{"option_type": "C", "strike": 100, "open_interest": 0}], 100.0
    ) is None


# --- IV skew -------------------------------------------------------------
def test_iv_skew_positive_when_puts_richer():
    opts = [
        {"option_type": "P", "strike": 90,  "iv": 0.85, "dte": 30},
        {"option_type": "C", "strike": 110, "iv": 0.55, "dte": 30},
    ]
    sk = sd.iv_skew_proxy(opts, spot=100.0)
    assert sk == pytest.approx(0.30)


def test_iv_skew_none_on_missing_side():
    opts = [{"option_type": "P", "strike": 90, "iv": 0.85, "dte": 30}]
    assert sd.iv_skew_proxy(opts, spot=100.0) is None


# --- rank percentile -----------------------------------------------------
def test_rank_pctile_handles_ties_and_nones():
    out = sd.rank_pctile([1.0, 2.0, 3.0, None, 2.0, 5.0])
    assert out[0] == pytest.approx(0.0)        # smallest
    assert out[-1] == pytest.approx(4 / 5)     # largest among 5 cleans
    assert out[3] is None


def test_rank_pctile_too_short():
    out = sd.rank_pctile([1.0, 2.0])
    assert all(x is None for x in out)


# --- combine_score -------------------------------------------------------
def test_combine_score_monotone_in_each_input():
    base = sd.combine_score(0.2, 0.2, 0.2, 0.2)
    high_otm = sd.combine_score(0.9, 0.2, 0.2, 0.2)
    high_util = sd.combine_score(0.2, 0.9, 0.2, 0.2)
    assert high_otm > base
    assert high_util > base


def test_combine_score_tolerates_missing():
    score = sd.combine_score(None, 0.8, 0.8, None)
    assert 0.0 <= score <= 1.0


# --- End-to-end DB integration -------------------------------------------
def _seed(db: Path, ticker: str = "AFRM", n_days: int = 30,
          spike_day: int = 25):
    """High OTM-call OI + high utilization on `spike_day`."""
    con = duckdb.connect(str(db))
    d0 = date(2024, 5, 1)
    for i in range(n_days):
        d = d0 + timedelta(days=i)
        exp = d + timedelta(days=30)
        spot = 50.0
        if i == spike_day:
            # heavy OTM call OI + elevated put skew
            con.executemany(
                """INSERT INTO options_chain (ticker, observed_at, expiry, strike,
                   option_type, iv, open_interest, underlying_price)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (ticker, d, exp, 50.0, "C", 0.55,  500, spot),
                    (ticker, d, exp, 60.0, "C", 0.60, 8000, spot),   # OTM
                    (ticker, d, exp, 70.0, "C", 0.65, 4000, spot),   # OTM
                    (ticker, d, exp, 45.0, "P", 0.95,  500, spot),
                    (ticker, d, exp, 55.0, "C", 0.45,  500, spot),
                ],
            )
            si_util = 0.40
            si_dtc  = 8.0
        else:
            con.executemany(
                """INSERT INTO options_chain (ticker, observed_at, expiry, strike,
                   option_type, iv, open_interest, underlying_price)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (ticker, d, exp, 50.0, "C", 0.55, 1000, spot),
                    (ticker, d, exp, 60.0, "C", 0.58,  200, spot),
                    (ticker, d, exp, 45.0, "P", 0.65,  600, spot),
                    (ticker, d, exp, 55.0, "C", 0.55,  400, spot),
                ],
            )
            si_util = 0.10
            si_dtc  = 2.0
        con.execute(
            """INSERT INTO short_interest
               (ticker, observed_at, utilization, days_to_cover)
               VALUES (?, ?, ?, ?)""",
            [ticker, d, si_util, si_dtc],
        )
    con.close()


def test_compute_for_ticker_lights_veto_on_spike(tmp_duckdb):
    _seed(tmp_duckdb, spike_day=25)
    n = sd.compute_for_ticker("AFRM")
    assert n >= 20
    con = duckdb.connect(str(tmp_duckdb))
    rows = con.execute(
        "SELECT observed_at, squeeze_score, veto FROM squeeze_defense "
        "WHERE ticker='AFRM' ORDER BY observed_at"
    ).fetchall()
    con.close()
    # at least one day must fire the veto (the spike day by construction)
    assert any(r[2] for r in rows)
    # and veto days must have higher mean squeeze_score than non-veto days
    veto_scores    = [r[1] for r in rows if r[2] and r[1] is not None]
    nonveto_scores = [r[1] for r in rows if (not r[2]) and r[1] is not None]
    assert min(veto_scores) > max(nonveto_scores or [-1])


def test_compute_is_idempotent(tmp_duckdb):
    _seed(tmp_duckdb, n_days=10)
    sd.compute_for_ticker("AFRM")
    sd.compute_for_ticker("AFRM")
    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM squeeze_defense").fetchone()
    (dist,) = con.execute(
        "SELECT COUNT(DISTINCT observed_at) FROM squeeze_defense"
    ).fetchone()
    con.close()
    assert cnt == dist
