"""Offline tests for data.ingest.auto_abs_historical.

Live SEC full-text search is stubbed. An opt-in live test can be added
under `pytest -m live` once the crisis backfill is executed in anger.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from data.ingest import auto_abs_historical as aah
from data.ingest import sec_edgar
from data.schema import DDL


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(sec_edgar.settings, "duckdb_path", db)
    monkeypatch.setattr(aah.settings, "duckdb_path", db)
    return db


def _fake_discover(stem: str):
    # Two synthetic per-deal trusts per family.
    return [
        {"cik": "1111111", "name": f"{stem} 2007-1"},
        {"cik": "2222222", "name": f"{stem} 2008-2"},
    ]


def _fake_filings(cik, forms, start):
    return [
        {
            "accession_no": f"000{cik}-08-000001",
            "cik":          str(cik),
            "trust_name":   "synthetic",
            "form_type":    "10-D",
            "filed_at":     datetime(2008, 10, 1, tzinfo=timezone.utc),
            "period_end":   None,
            "url":          None,
        },
        {
            "accession_no": f"000{cik}-11-000001",
            "cik":          str(cik),
            "trust_name":   "synthetic",
            "form_type":    "10-D",
            # Outside the crisis window — must be filtered out.
            "filed_at":     datetime(2011, 5, 1, tzinfo=timezone.utc),
            "period_end":   None,
            "url":          None,
        },
    ]


def test_ingest_family_writes_crisis_window_only(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(aah.settings, "offline", False)
    monkeypatch.setattr(aah, "_discover_trust_ciks", _fake_discover)
    monkeypatch.setattr(sec_edgar, "_fetch_filings", _fake_filings)

    fam = {"family": "SDART", "search": "Santander Drive Auto Receivables Trust"}
    n = aah.ingest_family(fam)
    # 2 trusts × 1 in-window filing each = 2, post-2010 filing dropped.
    assert n == 2

    con = duckdb.connect(str(tmp_duckdb))
    rows = con.execute(
        "SELECT trust_name, form_type FROM sec_filings_index "
        "WHERE trust_name LIKE '[auto_abs_crisis_aux]%'"
    ).fetchall()
    con.close()
    assert len(rows) == 2
    assert all(r[1] == "10-D" for r in rows)
    assert all("SDART::" in r[0] for r in rows)


def test_offline_is_noop(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(aah.settings, "offline", True)
    monkeypatch.setattr(aah, "_discover_trust_ciks",
                        lambda s: pytest.fail("called in offline mode"))
    fam = {"family": "SDART", "search": "x"}
    assert aah.ingest_family(fam) == 0


def test_ingest_all_aggregates(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(aah.settings, "offline", False)
    monkeypatch.setattr(aah, "_discover_trust_ciks", _fake_discover)
    monkeypatch.setattr(sec_edgar, "_fetch_filings", _fake_filings)

    summary = aah.ingest_all()
    assert set(summary.keys()) == {f["family"] for f in aah.CRISIS_TRUST_FAMILIES}
    assert all(v == 2 for v in summary.values())
