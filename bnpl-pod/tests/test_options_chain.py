"""Offline tests for data.ingest.options_chain."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from data.ingest import options_chain as oc
from data.schema import DDL


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(oc.settings, "duckdb_path", db)
    return db


def _fake_rows(ticker: str) -> list[dict]:
    return [
        {
            "ticker": ticker,
            "observed_at": date(2024, 6, 3),
            "expiry": date(2024, 7, 19),
            "strike": 50.0,
            "option_type": "C",
            "bid": 1.2, "ask": 1.3, "last_price": 1.25,
            "volume": 100, "open_interest": 500,
            "iv": 0.65, "underlying_price": 48.0,
        },
        {
            "ticker": ticker,
            "observed_at": date(2024, 6, 3),
            "expiry": date(2024, 7, 19),
            "strike": 50.0,
            "option_type": "P",
            "bid": 2.8, "ask": 2.9, "last_price": 2.85,
            "volume": 80, "open_interest": 300,
            "iv": 0.70, "underlying_price": 48.0,
        },
    ]


def test_option_tickers_nonempty():
    assert "AFRM" in oc.OPTION_TICKERS
    assert len(oc.OPTION_TICKERS) >= 10


def test_coerce_helpers():
    assert oc._f(None) is None
    assert oc._f("1.5") == 1.5
    assert oc._f("bad") is None
    assert oc._i(None) is None
    assert oc._i(3.7) == 3
    assert oc._i("bad") is None


def test_ingest_ticker_writes(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(oc.settings, "offline", False)
    monkeypatch.setattr(oc, "_fetch_chain", lambda t: _fake_rows(t))

    n = oc.ingest_ticker("AFRM")
    assert n == 2

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute(
        "SELECT COUNT(*) FROM options_chain WHERE ticker='AFRM'"
    ).fetchone()
    con.close()
    assert cnt == 2


def test_ingest_is_idempotent(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(oc.settings, "offline", False)
    monkeypatch.setattr(oc, "_fetch_chain", lambda t: _fake_rows(t))
    oc.ingest_ticker("AFRM")
    oc.ingest_ticker("AFRM")

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM options_chain").fetchone()
    con.close()
    assert cnt == 2


def test_offline_is_noop(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(oc.settings, "offline", True)
    monkeypatch.setattr(oc, "_fetch_chain",
                        lambda t: pytest.fail("called in offline mode"))
    assert oc.ingest_ticker("AFRM") == 0
