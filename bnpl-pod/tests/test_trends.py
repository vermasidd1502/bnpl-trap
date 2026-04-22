"""Offline tests for data.ingest.trends."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from data.ingest import trends
from data.schema import DDL


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(trends.settings, "duckdb_path", db)
    return db


def _fake_fetch(keyword, timeframe=None, geo=None):
    return [
        {"observed_at": date(2024, 6,  2), "interest": 34.0},
        {"observed_at": date(2024, 6,  9), "interest": 41.0},
        {"observed_at": date(2024, 6, 16), "interest": 55.0},
    ]


def test_taxonomy_covers_four_buckets():
    # Sprint-H.d promoted the taxonomy to four buckets (added `direct_admission`
    # for first-person distress queries). If this assert ever fires, update the
    # paper's Table 2 alongside the code change.
    assert set(trends.BUCKET_QUERIES.keys()) == {
        "product_interest", "friction", "exit", "direct_admission",
    }
    # Every query is non-empty.
    for qs in trends.BUCKET_QUERIES.values():
        assert qs and all(isinstance(q, str) and q for q in qs)


def test_ingest_keyword_writes(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(trends.settings, "offline", False)
    monkeypatch.setattr(trends, "_fetch_series", _fake_fetch)

    n = trends.ingest_keyword("affirm collections")
    assert n == 3

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute(
        "SELECT COUNT(*) FROM google_trends WHERE keyword='affirm collections'"
    ).fetchone()
    con.close()
    assert cnt == 3


def test_ingest_is_idempotent(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(trends.settings, "offline", False)
    monkeypatch.setattr(trends, "_fetch_series", _fake_fetch)
    trends.ingest_keyword("affirm collections")
    trends.ingest_keyword("affirm collections")

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM google_trends").fetchone()
    con.close()
    assert cnt == 3


def test_offline_is_noop(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(trends.settings, "offline", True)
    monkeypatch.setattr(trends, "_fetch_series",
                        lambda *a, **kw: pytest.fail("called in offline mode"))
    assert trends.ingest_keyword("affirm") == 0
