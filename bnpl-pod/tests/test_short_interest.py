"""Offline tests for data.ingest.short_interest."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from data.ingest import short_interest as si
from data.schema import DDL


FINRA_SAMPLE = (
    "settlementDate|symbolCode|currentShortPositionQuantity|"
    "previousShortPositionQuantity|averageDailyVolumeQuantity|daysToCoverQuantity\n"
    "20240615|AFRM|45000000|44000000|9000000|5.0\n"
    "20240615|SQ|30000000|28000000|10000000|3.0\n"
    "20240615|NOTINPANEL|1|1|1|1.0\n"
)


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(si.settings, "duckdb_path", db)
    return db


def test_read_finra_file_filters_panel(tmp_path: Path):
    p = tmp_path / "si.txt"
    p.write_text(FINRA_SAMPLE, encoding="utf-8")
    rows = si._read_finra_file(p)
    tickers = {r["ticker"] for r in rows}
    assert tickers == {"AFRM", "SQ"}
    afrm = next(r for r in rows if r["ticker"] == "AFRM")
    assert afrm["shares_short"] == 45_000_000
    assert afrm["avg_daily_vol"] == 9_000_000
    assert afrm["days_to_cover"] == pytest.approx(5.0)


def test_ingest_finra_file_writes(monkeypatch, tmp_duckdb, tmp_path):
    monkeypatch.setattr(si.settings, "offline", False)
    p = tmp_path / "si.txt"
    p.write_text(FINRA_SAMPLE, encoding="utf-8")

    n = si.ingest_finra_file(str(p))
    assert n == 2

    con = duckdb.connect(str(tmp_duckdb))
    rows = con.execute(
        "SELECT ticker, shares_short, days_to_cover FROM short_interest ORDER BY ticker"
    ).fetchall()
    con.close()
    assert rows[0] == ("AFRM", 45_000_000, pytest.approx(5.0))
    assert rows[1][0] == "SQ"


def test_finra_idempotent(monkeypatch, tmp_duckdb, tmp_path):
    monkeypatch.setattr(si.settings, "offline", False)
    p = tmp_path / "si.txt"
    p.write_text(FINRA_SAMPLE, encoding="utf-8")
    si.ingest_finra_file(str(p))
    si.ingest_finra_file(str(p))
    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM short_interest").fetchone()
    con.close()
    assert cnt == 2


def test_yf_proxy_writes(monkeypatch, tmp_duckdb):
    from datetime import date
    monkeypatch.setattr(si.settings, "offline", False)
    fake = {
        "ticker": "AFRM",
        "observed_at": date(2024, 6, 15),
        "shares_short": 45_000_000,
        "free_float": 300_000_000,
        "utilization": 0.15,
        "avg_daily_vol": 9_000_000,
        "days_to_cover": 5.0,
    }
    monkeypatch.setattr(si, "_yf_proxy", lambda t: fake)
    n = si.ingest_yf_proxy("AFRM")
    assert n == 1


def test_offline_is_noop(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(si.settings, "offline", True)
    monkeypatch.setattr(si, "_yf_proxy",
                        lambda t: pytest.fail("called in offline mode"))
    assert si.ingest_yf_proxy("AFRM") == 0
    assert si.ingest_finra_file("/nonexistent") == 0
