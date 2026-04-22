"""Offline tests for data.ingest.cfpb."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from data.ingest import cfpb
from data.schema import DDL


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(cfpb.settings, "duckdb_path", db)
    return db


def _fake_iter(company, date_from, date_to, page_size=1000):
    yield from [
        {
            "complaint_id":  "1111111",
            "date_received": "2024-06-01",
            "product":       "Credit card or prepaid card",
            "sub_product":   "General-purpose credit card",
            "issue":         "Struggling to pay your bill",
            "company":       company,
            "complaint_what_happened": "I couldn't afford the payment...",
            "tags":          None,
            "state":         "IL",
        },
        {
            "complaint_id":  "2222222",
            "date_received": "2024-06-03",
            "product":       "Money transfer, virtual currency, or money service",
            "sub_product":   None,
            "issue":         "Fraud or scam",
            "company":       company,
            "complaint_what_happened": None,
            "tags":          "Older American",
            "state":         "CA",
        },
    ]


def test_ingest_company_writes(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(cfpb.settings, "offline", False)
    monkeypatch.setattr(cfpb, "_iter_complaints", _fake_iter)

    n = cfpb.ingest_company("AFFIRM, INC.", start="2024-01-01", end="2024-12-31")
    assert n == 2

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM cfpb_complaints").fetchone()
    con.close()
    assert cnt == 2


def test_ingest_is_idempotent(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(cfpb.settings, "offline", False)
    monkeypatch.setattr(cfpb, "_iter_complaints", _fake_iter)
    cfpb.ingest_company("AFFIRM, INC.")
    cfpb.ingest_company("AFFIRM, INC.")

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM cfpb_complaints").fetchone()
    con.close()
    assert cnt == 2


def test_offline_is_noop(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(cfpb.settings, "offline", True)
    monkeypatch.setattr(cfpb, "_iter_complaints",
                        lambda *a, **kw: pytest.fail("called in offline mode"))
    assert cfpb.ingest_company("x") == 0
