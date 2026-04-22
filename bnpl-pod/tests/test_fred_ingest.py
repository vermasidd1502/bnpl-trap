"""Unit tests for the FRED ingest module.

These run fully offline — they exercise the parser and the DuckDB upsert
path by monkeypatching the HTTP call. A separate integration test (marked
`@pytest.mark.live`) hits the real FRED API and is opt-in.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from data.ingest import fred
from data.schema import DDL


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    """Fresh DuckDB file + initialized schema, scoped to the test."""
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(fred.settings, "duckdb_path", db)
    return db


def test_parser_handles_missing_values(monkeypatch, tmp_duckdb):
    fake_payload = [
        (date(2024, 1, 2), 101.5),
        (date(2024, 1, 3), None),      # FRED '.' turned into None
        (date(2024, 1, 4), 99.0),
    ]
    monkeypatch.setattr(fred, "_fetch_series", lambda *a, **kw: fake_payload)
    monkeypatch.setattr(fred.settings, "offline", False)
    monkeypatch.setattr(fred.settings, "fred_api_key", "test-key")

    n = fred.ingest_series("MOVE", start="2024-01-01")
    assert n == 3

    con = duckdb.connect(str(tmp_duckdb))
    rows = con.execute(
        "SELECT observed_at, value FROM fred_series WHERE series_id='MOVE' ORDER BY observed_at"
    ).fetchall()
    con.close()

    assert rows[0] == (date(2024, 1, 2), 101.5)
    assert rows[1][1] is None
    assert rows[2] == (date(2024, 1, 4), 99.0)


def test_idempotent_upsert(monkeypatch, tmp_duckdb):
    payload = [(date(2024, 1, 2), 10.0)]
    monkeypatch.setattr(fred, "_fetch_series", lambda *a, **kw: payload)
    monkeypatch.setattr(fred.settings, "offline", False)
    monkeypatch.setattr(fred.settings, "fred_api_key", "test-key")

    fred.ingest_series("MOVE")
    fred.ingest_series("MOVE")   # run twice

    con = duckdb.connect(str(tmp_duckdb))
    (count,) = con.execute(
        "SELECT COUNT(*) FROM fred_series WHERE series_id='MOVE'"
    ).fetchone()
    con.close()
    assert count == 1, "upsert must be idempotent on (series_id, observed_at)"


def test_offline_mode_is_no_op(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(fred.settings, "offline", True)
    # Should NOT call _fetch_series at all.
    monkeypatch.setattr(fred, "_fetch_series",
                        lambda *a, **kw: pytest.fail("network call in offline mode"))
    assert fred.ingest_series("MOVE") == 0
