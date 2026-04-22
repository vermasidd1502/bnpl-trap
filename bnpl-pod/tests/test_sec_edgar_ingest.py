"""Offline tests for data.ingest.sec_edgar.

All network calls are stubbed. A separate live integration test
(`pytest -m live`) hits the real SEC API and is opt-in.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

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
    return db


def _fake_filings(*_args, **_kwargs):
    return [
        {
            "accession_no": "0001234567-24-000001",
            "cik":          "1820953",
            "trust_name":   "Affirm Asset Securitization Trust 2024-B",
            "form_type":    "10-D",
            "filed_at":     datetime(2024, 7, 15, tzinfo=timezone.utc),
            "period_end":   None,
            "url":          "https://www.sec.gov/Archives/...",
        },
        {
            "accession_no": "0001234567-24-000002",
            "cik":          "1820953",
            "trust_name":   "Affirm Holdings",
            "form_type":    "10-Q",
            "filed_at":     datetime(2024, 8, 7, tzinfo=timezone.utc),
            "period_end":   None,
            "url":          None,
        },
    ]


def test_panel_loads_and_flattens():
    panel = sec_edgar.load_panel()
    firms = sec_edgar.flatten_panel(panel)
    # 6 BNPL + 7 near-prime + 4 placebo + 6 subprime-auto = 23
    assert len(firms) == 23
    groups = {f["group"] for f in firms}
    assert groups == {"treated", "near_prime", "placebo", "subprime_auto"}


def test_role_routing():
    assert sec_edgar._role_for({"trust_family": "AFRMMT"}) == "abs_trust"
    assert sec_edgar._role_for({"ticker": "ZIP.AX"}) == "foreign"
    assert sec_edgar._role_for({"ticker": "AFRM"}) == "issuer"


def test_ingest_firm_writes_rows(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(sec_edgar, "_fetch_filings", _fake_filings)
    monkeypatch.setattr(sec_edgar.settings, "offline", False)

    n = sec_edgar.ingest_firm(
        {"name": "Affirm Holdings", "cik": "0001820953", "group": "treated"}
    )
    assert n == 2

    con = duckdb.connect(str(tmp_duckdb))
    (count,) = con.execute(
        "SELECT COUNT(*) FROM sec_filings_index WHERE cik='1820953'"
    ).fetchone()
    con.close()
    assert count == 2


def test_ingest_is_idempotent(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(sec_edgar, "_fetch_filings", _fake_filings)
    monkeypatch.setattr(sec_edgar.settings, "offline", False)
    firm = {"name": "Affirm Holdings", "cik": "0001820953", "group": "treated"}
    sec_edgar.ingest_firm(firm)
    sec_edgar.ingest_firm(firm)

    con = duckdb.connect(str(tmp_duckdb))
    (count,) = con.execute(
        "SELECT COUNT(*) FROM sec_filings_index WHERE cik='1820953'"
    ).fetchone()
    con.close()
    assert count == 2


def test_offline_is_noop(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(sec_edgar.settings, "offline", True)
    monkeypatch.setattr(sec_edgar, "_fetch_filings",
                        lambda *a, **kw: pytest.fail("called in offline mode"))
    assert sec_edgar.ingest_firm({"name": "x", "cik": "123"}) == 0


def test_skip_when_no_cik(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(sec_edgar.settings, "offline", False)
    monkeypatch.setattr(sec_edgar, "_fetch_filings",
                        lambda *a, **kw: pytest.fail("called when cik is None"))
    assert sec_edgar.ingest_firm({"name": "No-CIK Entity", "cik": None}) == 0
