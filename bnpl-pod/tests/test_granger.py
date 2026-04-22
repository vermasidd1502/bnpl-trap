"""Offline tests for signals.granger."""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from data.schema import DDL
from signals import granger


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(granger.settings, "duckdb_path", db)
    return db


def _seed_causal(db: Path, n_weeks: int = 200, lag_weeks: int = 6):
    """
    Build a toy series where BSI Granger-causes roll rate with lag=`lag_weeks`:
        rr_t = 0.6 * rr_{t-1} + 0.8 * bsi_{t-lag} + noise
    Writes daily BSI (Mon-Fri) and monthly roll-rate observations.
    """
    import random
    random.seed(42)
    start = date(2019, 1, 7)   # Monday
    bsi_weekly = [random.gauss(0.0, 1.0) for _ in range(n_weeks)]
    rr_weekly = [0.0] * n_weeks
    for t in range(1, n_weeks):
        prev = rr_weekly[t - 1]
        driver = bsi_weekly[t - lag_weeks] if t >= lag_weeks else 0.0
        rr_weekly[t] = 0.6 * prev + 0.8 * driver + random.gauss(0.0, 0.3)
    # Push mean/scale into a roll-rate-like range: 2% .. 6%
    rr_weekly = [2.0 + 0.5 * x for x in rr_weekly]

    con = duckdb.connect(str(db))
    # Daily BSI: repeat weekly value across Mon-Fri of that week.
    for wk, b in enumerate(bsi_weekly):
        wk_start = start + timedelta(weeks=wk)
        for dow in range(5):
            d = wk_start + timedelta(days=dow)
            con.execute(
                "INSERT OR REPLACE INTO bsi_daily (observed_at, bsi) VALUES (?, ?)",
                [d, b],
            )
    # Monthly roll-rate: take every 4th week's value, period_end = that Sunday.
    for wk in range(0, n_weeks, 4):
        pend = start + timedelta(weeks=wk, days=6)
        accn = f"0000000000-AFRM-{wk:04d}"
        con.execute(
            """INSERT INTO abs_tranche_metrics
               (accession_no, trust_name, period_end, roll_rate_60p)
               VALUES (?, 'AFFIRM ASSET SECURITIZATION TRUST 2024-B', ?, ?)""",
            [accn, pend, rr_weekly[wk]],
        )
    con.close()


def test_fetch_pairs_aligns_weekly(tmp_duckdb):
    _seed_causal(tmp_duckdb, n_weeks=60, lag_weeks=6)
    con = duckdb.connect(str(tmp_duckdb))
    dates_, bsi_, rr_ = granger._fetch_pairs(con)
    con.close()
    assert len(dates_) == len(bsi_) == len(rr_)
    assert len(dates_) > 40   # most weeks aligned after rr forward-fill


def test_granger_detects_causal_at_true_lag(tmp_duckdb):
    _seed_causal(tmp_duckdb, n_weeks=200, lag_weeks=6)
    res = granger.run_granger(lags=(4, 5, 6, 7, 8), persist=True)
    assert res, "expected at least one lag result"
    by_lag = {r.lag_weeks: r for r in res}
    # At the true lag (6), p-value should be small (strong rejection of H0).
    assert by_lag[6].p_value < 0.05
    # Persisted table should now hold rows.
    con = duckdb.connect(str(tmp_duckdb))
    (n,) = con.execute("SELECT COUNT(*) FROM granger_results").fetchone()
    con.close()
    assert n == len(res)


def test_granger_empty_when_insufficient_data(tmp_duckdb):
    # No data seeded.
    res = granger.run_granger(lags=(4, 6, 8))
    assert res == []


def test_rolling_granger_returns_windows(tmp_duckdb):
    _seed_causal(tmp_duckdb, n_weeks=250, lag_weeks=6)
    windows = granger.rolling_granger(window_weeks=100, step_weeks=25,
                                      lags=(4, 6, 8))
    assert len(windows) >= 3
    for end_date, res in windows:
        assert isinstance(end_date, date)
        # each window yields up to 3 lag results
        assert 0 < len(res) <= 3
