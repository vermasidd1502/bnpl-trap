"""Offline tests for data.ingest.trends_manual (manual-CSV Trends ingest)."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from textwrap import dedent

import duckdb
import pytest

from data.ingest import trends_manual
from data.schema import DDL


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(trends_manual.settings, "duckdb_path", db)
    return db


def _write_csv(path: Path, body: str) -> Path:
    path.write_text(dedent(body).lstrip("\n"), encoding="utf-8")
    return path


# ---------- single-keyword, weekly ----------------------------------------

def test_parse_single_keyword_weekly(tmp_path):
    csv = _write_csv(tmp_path / "single.csv", """
        Category: All categories

        Week,affirm late fee: (United States)
        2024-01-07,41
        2024-01-14,55
        2024-01-21,<1
    """)

    parsed = trends_manual.parse_csv(csv)
    assert parsed.keywords == ["affirm late fee"]
    assert len(parsed.rows) == 3
    # The '<1' sentinel coerces to 0.5 — documented parser contract.
    assert parsed.rows.loc[parsed.rows["observed_at"] == date(2024, 1, 21),
                          "interest"].iloc[0] == 0.5
    assert parsed.rows["interest"].max() == 55.0


# ---------- multi-keyword compare (up to 5 per CSV) -----------------------

def test_parse_multi_keyword_compare(tmp_path):
    csv = _write_csv(tmp_path / "multi.csv", """
        Category: All categories

        Week,affirm late fee: (United States),cant pay klarna: (United States),bnpl lawsuit: (United States)
        2024-01-07,41,12,3
        2024-01-14,55,18,4
    """)

    parsed = trends_manual.parse_csv(csv)
    assert set(parsed.keywords) == {"affirm late fee", "cant pay klarna", "bnpl lawsuit"}
    assert len(parsed.rows) == 6   # 3 kw × 2 weeks
    klarna = parsed.rows[parsed.rows["keyword"] == "cant pay klarna"]
    assert klarna["interest"].tolist() == [12.0, 18.0]


# ---------- daily granularity (short-timeframe exports) -------------------

def test_parse_daily_granularity(tmp_path):
    csv = _write_csv(tmp_path / "daily.csv", """
        Category: All categories

        Day,affirm hardship program: (United States)
        2025-03-01,10
        2025-03-02,14
        2025-03-03,22
    """)

    parsed = trends_manual.parse_csv(csv)
    assert parsed.keywords == ["affirm hardship program"]
    assert parsed.rows["observed_at"].tolist() == [
        date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3),
    ]


# ---------- keyword normalisation -----------------------------------------

def test_keyword_is_lowercased_and_stripped(tmp_path):
    csv = _write_csv(tmp_path / "mixed_case.csv", """
        Category: All categories

        Week,Affirm Late Fee: (United States)
        2024-01-07,41
    """)
    parsed = trends_manual.parse_csv(csv)
    assert parsed.keywords == ["affirm late fee"]


# ---------- end-to-end ingest into DuckDB ---------------------------------

def test_ingest_directory_writes_rows(tmp_path, tmp_duckdb):
    _write_csv(tmp_path / "panel.csv", """
        Category: All categories

        Week,affirm late fee: (United States),bnpl lawsuit: (United States)
        2024-01-07,41,3
        2024-01-14,55,4
    """)

    summary = trends_manual.ingest_directory(tmp_path)
    assert summary == {"affirm late fee": 2, "bnpl lawsuit": 2}

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM google_trends").fetchone()
    con.close()
    assert cnt == 4


def test_ingest_is_idempotent(tmp_path, tmp_duckdb):
    _write_csv(tmp_path / "panel.csv", """
        Category: All categories

        Week,affirm late fee: (United States)
        2024-01-07,41
        2024-01-14,55
    """)
    trends_manual.ingest_directory(tmp_path)
    trends_manual.ingest_directory(tmp_path)   # second pass must not dup-insert

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM google_trends").fetchone()
    con.close()
    assert cnt == 2


def test_empty_dir_is_noop(tmp_path, tmp_duckdb):
    summary = trends_manual.ingest_directory(tmp_path)
    assert summary == {}
    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM google_trends").fetchone()
    con.close()
    assert cnt == 0


def test_dry_run_does_not_write(tmp_path, tmp_duckdb):
    _write_csv(tmp_path / "panel.csv", """
        Category: All categories

        Week,affirm late fee: (United States)
        2024-01-07,41
    """)
    summary = trends_manual.ingest_directory(tmp_path, dry_run=True)
    assert summary == {"affirm late fee": 1}

    con = duckdb.connect(str(tmp_duckdb))
    (cnt,) = con.execute("SELECT COUNT(*) FROM google_trends").fetchone()
    con.close()
    assert cnt == 0


# ---------- malformed input -----------------------------------------------

def test_missing_header_row_raises(tmp_path):
    bad = _write_csv(tmp_path / "bad.csv", """
        garbage,only
        1,2
    """)
    with pytest.raises(ValueError):
        trends_manual.parse_csv(bad)
