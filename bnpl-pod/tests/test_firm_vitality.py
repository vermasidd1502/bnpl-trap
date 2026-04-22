"""Offline tests for data.ingest.firm_vitality."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from data.ingest import firm_vitality as fv
from data.schema import DDL


LI_HTML = """
<html><body>
<section>
    <h1>Affirm</h1>
    <p>Financial Services</p>
    <div class="top-card-layout__cta-modifier">1,001-5,000 employees</div>
    <a href="/jobs/affirm-jobs">142 open jobs</a>
</section>
</body></html>
"""

X_HTML_K = '<html><body><div>Affirm</div><span>123.4K Followers</span></body></html>'
X_HTML_M = '<html><body><div>Klarna</div><span>1.2M Followers</span></body></html>'


# --- Parsers ---------------------------------------------------------------
def test_parse_linkedin_extracts_headcount_and_openings():
    out = fv.parse_linkedin_html(LI_HTML)
    assert out["headcount"] == 3000     # midpoint of 1001-5000
    assert out["openings"] == 142


def test_parse_linkedin_handles_no_match():
    out = fv.parse_linkedin_html("<html><body>Nothing here</body></html>")
    assert out == {"headcount": None, "openings": None}


def test_parse_x_followers_k_suffix():
    out = fv.parse_x_html(X_HTML_K)
    assert out["followers"] == 123_400


def test_parse_x_followers_m_suffix():
    out = fv.parse_x_html(X_HTML_M)
    assert out["followers"] == 1_200_000


# --- Derived signals -------------------------------------------------------
def test_tenure_slope_basic():
    assert fv.compute_tenure_slope(1000, 50) == pytest.approx(0.05)
    assert fv.compute_tenure_slope(None, 10) is None
    assert fv.compute_tenure_slope(0, 10) is None


def test_freeze_flag_fires_on_freeze_with_flat_headcount():
    # 8 stable slopes around 0.05, then a crash.
    ts = [0.050, 0.051, 0.049, 0.052, 0.050, 0.051, 0.049, 0.050, 0.005]
    hc = [3000, 3002]
    assert fv.compute_freeze_flag(ts, hc) is True


def test_freeze_flag_doesnt_fire_when_headcount_crashes_too():
    ts = [0.050, 0.051, 0.049, 0.052, 0.050, 0.051, 0.049, 0.050, 0.005]
    hc = [3000, 2000]  # 33% layoff — not a freeze, this is a shrink.
    assert fv.compute_freeze_flag(ts, hc) is False


# --- Staleness weight ------------------------------------------------------
def test_stale_weight_full_inside_grace_window():
    assert fv._stale_weight(0) == 1.0
    assert fv._stale_weight(30) == 1.0


def test_stale_weight_decays_past_grace():
    w60 = fv._stale_weight(60)
    w90 = fv._stale_weight(90)
    assert w60 == pytest.approx(math.exp(-1))          # ~0.368
    assert w90 == pytest.approx(math.exp(-2))          # ~0.135
    assert w60 > w90


# --- DB integration --------------------------------------------------------
@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(fv.settings, "duckdb_path", db)
    return db


def test_ingest_linkedin_writes_rows(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(fv.settings, "offline", False)
    snaps = [
        fv.Snapshot(url="https://www.linkedin.com/company/affirm",
                    ts=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    wayback_url="https://web.archive.org/web/20240115/x"),
        fv.Snapshot(url="https://www.linkedin.com/company/affirm",
                    ts=datetime(2024, 3, 15, tzinfo=timezone.utc),
                    wayback_url="https://web.archive.org/web/20240315/x"),
    ]
    monkeypatch.setattr(fv, "_cdx_snapshots",
                        lambda url, from_date="2019", to_date=None, collapse="timestamp:8": snaps)
    monkeypatch.setattr(fv, "_fetch_snapshot_html", lambda snap: LI_HTML)

    n = fv.ingest_linkedin("affirm")
    assert n == 2

    con = duckdb.connect(str(tmp_duckdb))
    rows = con.execute(
        "SELECT headcount, openings, tenure_slope, stale_weight "
        "FROM firm_vitality WHERE slug='affirm' AND platform='linkedin' "
        "ORDER BY observed_at"
    ).fetchall()
    con.close()
    assert len(rows) == 2
    for hc, op, ts, sw in rows:
        assert hc == 3000
        assert op == 142
        assert ts == pytest.approx(142 / 3000)
        assert 0.0 < sw <= 1.0


def test_offline_is_noop(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(fv.settings, "offline", True)
    monkeypatch.setattr(fv, "_cdx_snapshots",
                        lambda *a, **kw: pytest.fail("called in offline mode"))
    assert fv.ingest_linkedin("affirm") == 0
    assert fv.ingest_x("Affirm") == 0
