"""Offline tests for data.ingest.abs_parser.

Parsers are pure-string functions; we exercise them against synthetic
trustee-report blurbs modelled on real AFRMMT / SDART / AMCAR language.
Network is not touched.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from data.ingest import abs_parser as ap
from data.schema import DDL


# --- Pure-parser tests -----------------------------------------------------
AFRMMT_BLURB = """
... As of the Distribution Date, the 60+ day delinquency roll-rate was
2.47% for the reporting period (prior period: 2.35%). The Excess Spread
for the current period stands at 8.12%, annualized. Cumulative Net Loss
on the pool is 3.84% of the original pool balance. Senior Credit
Enhancement remains at 28.50%. ...
"""

SDART_BLURB = """
Pool performance summary. 60-day DPD roll rate: 6.20%. CNL: 11.43%.
Excess spread (net) 4.87%. Senior enhancement: 35.00%.
"""

PARTIAL_BLURB = """
Excess spread 7.1%. Other pool-level metrics are presented in Exhibit A.
"""

GARBAGE = "The quick brown fox jumps over 12345 lazy dogs."


def test_parse_afrmmt_full():
    m = ap.parse_trustee_text(AFRMMT_BLURB)
    assert m.roll_rate_60p == pytest.approx(2.47)
    assert m.excess_spread == pytest.approx(8.12)
    assert m.cnl == pytest.approx(3.84)
    assert m.senior_enh == pytest.approx(28.50)
    assert m.nonnull() == 4


def test_parse_sdart_variant_wording():
    m = ap.parse_trustee_text(SDART_BLURB)
    assert m.roll_rate_60p == pytest.approx(6.20)
    assert m.excess_spread == pytest.approx(4.87)
    assert m.cnl == pytest.approx(11.43)
    assert m.senior_enh == pytest.approx(35.00)


def test_parse_partial_fields():
    m = ap.parse_trustee_text(PARTIAL_BLURB)
    assert m.excess_spread == pytest.approx(7.1)
    assert m.roll_rate_60p is None
    assert m.cnl is None
    assert m.senior_enh is None
    assert m.nonnull() == 1


def test_parse_garbage_yields_empty():
    m = ap.parse_trustee_text(GARBAGE)
    assert m.nonnull() == 0


def test_pct_sanity_rejects_out_of_range():
    # A stray 9999 shouldn't get absorbed as a percentage.
    bad = "excess spread 9999 (in basis-point column -- do not use)"
    m = ap.parse_trustee_text(bad)
    assert m.excess_spread is None


# --- Offline DB integration (parser fn monkeypatched) ----------------------
@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    # Seed one 10-D filing to be parsed.
    con.execute(
        """
        INSERT INTO sec_filings_index
            (accession_no, cik, trust_name, form_type, filed_at, period_end, url)
        VALUES
            ('0001820953-24-000001', '1820953',
             'Affirm Asset Securitization Trust 2024-B',
             '10-D', TIMESTAMP '2024-07-15 00:00:00', DATE '2024-06-30', NULL)
        """
    )
    con.close()
    monkeypatch.setattr(ap.settings, "duckdb_path", db)
    return db


def test_parse_all_unparsed_writes_row(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(ap.settings, "offline", False)
    monkeypatch.setattr(
        ap, "_fetch_document_text", lambda acc, *, url=None: AFRMMT_BLURB
    )

    counts = ap.parse_all_unparsed()
    assert counts["parsed"] == 1
    assert counts["empty"] == 0
    assert counts["failed"] == 0

    con = duckdb.connect(str(tmp_duckdb))
    row = con.execute(
        "SELECT roll_rate_60p, excess_spread, cnl, senior_enh "
        "FROM abs_tranche_metrics WHERE accession_no='0001820953-24-000001'"
    ).fetchone()
    con.close()
    assert row == pytest.approx((2.47, 8.12, 3.84, 28.50))


def test_parse_all_skips_already_parsed(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(ap.settings, "offline", False)
    monkeypatch.setattr(
        ap, "_fetch_document_text", lambda acc, *, url=None: AFRMMT_BLURB
    )

    ap.parse_all_unparsed()
    # Second run must find nothing to do.
    counts = ap.parse_all_unparsed()
    assert counts == {"parsed": 0, "empty": 0, "failed": 0}


def test_offline_is_noop_on_parse_filing(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(ap.settings, "offline", True)
    monkeypatch.setattr(
        ap,
        "_fetch_document_text",
        lambda acc, *, url=None: pytest.fail("called in offline mode"),
    )
    m = ap.parse_filing("x", "y", None)
    assert m.nonnull() == 0
