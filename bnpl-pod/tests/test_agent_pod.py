"""End-to-end tests for the agent pod: schemas, 3 agents, graph, tick.

Every test is offline. No LLM calls, no network. Agents are called with
`llm=None` — their advisory path is skipped and the deterministic outputs
flow straight into the compliance engine.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from agents import graph, macro_agent, quant_agent, risk_manager, tick
from agents.schemas import MacroReport, PodDecision, QuantReport, RiskReport
from data.schema import DDL
from data.settings import settings


# --- Fixtures --------------------------------------------------------------
@pytest.fixture()
def tmp_warehouse(tmp_path: Path, monkeypatch):
    """Fresh DuckDB with full schema + seeded regulatory catalyst.

    Sprint H: the live graph queries `regulatory_catalysts` for gate 3.
    An empty table would make `gate_ccd2` fail for ALL as_ofs — which would
    silently kill every graph test. Seed the EU CCD II row so the graph
    behaves as it does in production (which runs `data.ingest.regulatory_catalysts`
    at build time).
    """
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.execute(
        """INSERT INTO regulatory_catalysts
              (catalyst_id, jurisdiction, deadline_date, title, materiality, category, notes)
           VALUES ('ccd_ii_transposition_2026', 'EU', DATE '2026-11-20',
                   'EU CCD II transposition', 1.0, 'transposition', 'test seed')"""
    )
    con.close()
    # Every module resolves its path via data.settings.settings.duckdb_path.
    monkeypatch.setattr(settings, "duckdb_path", db)
    return db


def _seed_macro(db: Path, as_of: date, *, bsi_z: float = 2.0,
                move_ma30: float = 130.0, freeze: bool = False):
    """Seed bsi_daily (60 rows, latest matching bsi_z) and fred_series MOVE."""
    con = duckdb.connect(str(db))
    d0 = as_of - timedelta(days=60)
    for i in range(60):
        d = d0 + timedelta(days=i)
        # Flat history then a latest z-consistent value on the last day.
        bsi = 0.0 if i < 59 else float(bsi_z)
        z = 0.0 if i < 59 else float(bsi_z)
        con.execute(
            """INSERT INTO bsi_daily (observed_at, bsi, z_bsi, freeze_flag,
                                      weights_hash)
               VALUES (?, ?, ?, ?, 'test')""",
            [d, bsi, z, bool(freeze and i == 59)],
        )
    # MOVE — 30 constant values landing at the desired MA30
    for i in range(30):
        d = as_of - timedelta(days=29 - i)
        con.execute(
            "INSERT INTO fred_series (series_id, observed_at, value) "
            "VALUES ('MOVE', ?, ?)",
            [d, float(move_ma30)],
        )
    con.close()


def _seed_quant(db: Path, as_of: date, *,
                scp_by_ticker: dict[str, float] | None = None,
                lambdas: dict[str, float] | None = None):
    scp_by_ticker = scp_by_ticker or {"AFRM": 3.0, "SQ": -0.5}
    lambdas = lambdas or {"AFRM": 0.012, "SQ": 0.008}
    con = duckdb.connect(str(db))
    for t, z in scp_by_ticker.items():
        con.execute(
            """INSERT INTO scp_daily (ticker, observed_at, scp, z_scp)
               VALUES (?, ?, ?, ?)""",
            [t, as_of, float(z), float(z)],
        )
    for iss, lam in lambdas.items():
        con.execute(
            """INSERT INTO jt_lambda (issuer, observed_at, lambda_sys,
                                      lambda_unsys, lambda_total, kappa,
                                      theta, sigma, j_max)
               VALUES (?, ?, ?, ?, ?, 0.5, 0.03, 0.08, 0.05)""",
            [iss, as_of, 0.003, float(lam) - 0.003, float(lam)],
        )
    con.close()


def _seed_risk(db: Path, as_of: date, tickers=("AFRM", "SQ"),
               *, veto: bool = False):
    con = duckdb.connect(str(db))
    d0 = as_of - timedelta(days=40)
    for t in tickers:
        for i in range(40):
            d = d0 + timedelta(days=i)
            skew = 0.02 + 0.0001 * i   # monotonic so latest has rank ≈ 1.0
            con.execute(
                """INSERT INTO squeeze_defense (ticker, observed_at,
                       otm_call_pct, utilization, days_to_cover,
                       iv_skew_25d, squeeze_score, veto)
                   VALUES (?, ?, 0.3, 0.4, 2.5, ?, 0.2, ?)""",
                [t, d, skew, bool(veto and i == 39)],
            )
    con.close()


# --- schemas ---------------------------------------------------------------
def test_schemas_dataclasses_default_fields():
    m = MacroReport(as_of=datetime(2026, 4, 1, tzinfo=timezone.utc),
                    bsi=0.1, bsi_z=0.5, move_ma30=100.0, freeze_flag=False)
    assert m.advisory == ""
    q = QuantReport(as_of=m.as_of, scp_by_ticker={}, scp_gate_fires={},
                    lambda_total_by_issuer={})
    assert q.advisory == ""
    r = RiskReport(as_of=m.as_of, squeeze_utilization={}, squeeze_days_to_cover={},
                   squeeze_skew_pctile={}, squeeze_score_by_ticker={},
                   squeeze_veto_candidate=False)
    assert r.advisory == ""
    pod = PodDecision(run_id="x", as_of=m.as_of, macro=m, quant=q, risk=r)
    assert pod.expression == "trs_junior_abs"
    assert pod.compliance_approved is False
    assert pod.equity_tickers == []


# --- macro_agent -----------------------------------------------------------
def test_macro_agent_reads_latest_z_and_move(tmp_warehouse):
    as_of = date(2026, 4, 15)
    _seed_macro(tmp_warehouse, as_of, bsi_z=2.3, move_ma30=135.0, freeze=True)
    rep = macro_agent.run(
        as_of=datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc),
        llm=None,
    )
    assert rep.bsi_z == pytest.approx(2.3)
    assert rep.move_ma30 == pytest.approx(135.0)
    assert rep.freeze_flag is True
    assert rep.advisory == ""


def test_macro_agent_handles_empty_warehouse(tmp_warehouse):
    rep = macro_agent.run(
        as_of=datetime(2026, 4, 15, tzinfo=timezone.utc),
        llm=None,
    )
    assert rep.bsi == 0.0 and rep.bsi_z == 0.0
    assert rep.move_ma30 == 0.0


# --- quant_agent -----------------------------------------------------------
def test_quant_agent_flags_g2_on_high_z(tmp_warehouse):
    as_of = date(2026, 4, 15)
    _seed_quant(tmp_warehouse, as_of,
                scp_by_ticker={"AFRM": 1.6, "SQ": 0.5},
                lambdas={"AFRM": 0.015})
    rep = quant_agent.run(
        as_of=datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc),
        tickers=("AFRM", "SQ"),
        llm=None,
    )
    assert rep.scp_by_ticker["AFRM"] == pytest.approx(1.6)
    assert rep.scp_gate_fires["AFRM"] is True
    assert rep.scp_gate_fires["SQ"] is False
    assert rep.lambda_total_by_issuer["AFRM"] == pytest.approx(0.015)


# --- risk_manager ----------------------------------------------------------
def test_risk_manager_ranks_skew_and_surfaces_veto(tmp_warehouse):
    as_of = date(2026, 4, 15)
    _seed_risk(tmp_warehouse, as_of, tickers=("AFRM",), veto=True)
    rep = risk_manager.run(
        as_of=datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc),
        tickers=("AFRM",),
        llm=None,
    )
    assert rep.squeeze_utilization["AFRM"] == pytest.approx(0.4)
    assert rep.squeeze_days_to_cover["AFRM"] == pytest.approx(2.5)
    # Monotonic-increasing skew → latest is at rank ≈ 1.0 (all 39 priors below)
    assert rep.squeeze_skew_pctile["AFRM"] > 0.95
    assert rep.squeeze_veto_candidate is True


def test_risk_manager_skips_tickers_with_no_rows(tmp_warehouse):
    rep = risk_manager.run(
        as_of=datetime(2026, 4, 15, tzinfo=timezone.utc),
        tickers=("AFRM",),
        llm=None,
    )
    assert "AFRM" not in rep.squeeze_utilization
    assert rep.squeeze_veto_candidate is False


# --- graph end-to-end ------------------------------------------------------
def test_graph_approves_trs_when_all_gates_pass(tmp_warehouse):
    """Post-Fix #2: approval requires BSI × MOVE × CCD2 (three gates).
    SCP is still surfaced via pod.gate_scp but is telemetry, not gating."""
    as_of = date(2026, 10, 1)   # within 180d of DEFAULT_CCD_II_DEADLINE (2026-11-20)
    _seed_macro(tmp_warehouse, as_of, bsi_z=2.0, move_ma30=130.0)
    _seed_quant(tmp_warehouse, as_of,
                scp_by_ticker={"AFRM": 3.0}, lambdas={"AFRM": 0.01})
    _seed_risk(tmp_warehouse, as_of, tickers=("AFRM",))

    pod = graph.run_graph(
        as_of=datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc),
        llm=None,
    )
    assert pod.gate_bsi and pod.gate_move and pod.gate_ccd2
    assert pod.gate_scp is True               # telemetry fires
    assert pod.squeeze_veto is False          # dead field, always False
    assert pod.compliance_approved is True
    assert pod.thresholds_version             # non-empty audit hash
    # Trade signal JSON is well-formed and always carries TRS expression.
    import json
    payload = json.loads(pod.trade_signal_json)
    assert payload["approved"] is True
    assert payload["expression"] == "trs_junior_abs"


def test_graph_rejects_when_bsi_below_threshold(tmp_warehouse):
    as_of = date(2026, 10, 1)
    _seed_macro(tmp_warehouse, as_of, bsi_z=0.5, move_ma30=130.0)   # G1 fail
    _seed_quant(tmp_warehouse, as_of,
                scp_by_ticker={"AFRM": 3.0}, lambdas={"AFRM": 0.01})
    _seed_risk(tmp_warehouse, as_of, tickers=("AFRM",))
    pod = graph.run_graph(
        as_of=datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc),
        llm=None,
    )
    assert pod.gate_bsi is False
    assert pod.compliance_approved is False
    assert any("Gate 1 (BSI)" in r for r in pod.compliance_reasons)


def test_graph_approves_even_when_scp_below_threshold(tmp_warehouse):
    """Fix #2 regression test: low SCP must NOT block approval.

    Mirrors test_scp_is_telemetry_only_does_not_block_approval in
    test_compliance_engine but exercises the full graph path.
    """
    as_of = date(2026, 10, 1)
    _seed_macro(tmp_warehouse, as_of, bsi_z=2.0, move_ma30=130.0)
    _seed_quant(tmp_warehouse, as_of,
                scp_by_ticker={"AFRM": 0.1}, lambdas={"AFRM": 0.01})   # SCP FAR below 2.5
    _seed_risk(tmp_warehouse, as_of, tickers=("AFRM",))
    pod = graph.run_graph(
        as_of=datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc),
        llm=None,
    )
    assert pod.compliance_approved is True
    assert pod.gate_scp is False   # telemetry off, approval on


# --- tick runner -----------------------------------------------------------
def test_tick_dry_run_does_not_persist(tmp_warehouse):
    as_of = date(2026, 10, 1)
    _seed_macro(tmp_warehouse, as_of)
    _seed_quant(tmp_warehouse, as_of)
    _seed_risk(tmp_warehouse, as_of)
    pod = tick.run_pod_tick(
        as_of=datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc),
        persist=False, llm=None,
    )
    con = duckdb.connect(str(tmp_warehouse))
    (n,) = con.execute("SELECT COUNT(*) FROM pod_decisions").fetchone()
    con.close()
    assert n == 0
    assert pod.run_id


def test_tick_persist_writes_one_row(tmp_warehouse):
    as_of = date(2026, 10, 1)
    _seed_macro(tmp_warehouse, as_of)
    _seed_quant(tmp_warehouse, as_of)
    _seed_risk(tmp_warehouse, as_of)
    pod = tick.run_pod_tick(
        as_of=datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc),
        persist=True, llm=None,
    )
    con = duckdb.connect(str(tmp_warehouse))
    rows = con.execute(
        "SELECT run_id, compliance_ok FROM pod_decisions"
    ).fetchall()
    con.close()
    assert len(rows) == 1
    assert rows[0][0] == pod.run_id
    assert rows[0][1] == pod.compliance_approved


def test_tick_persist_idempotent_on_run_id(tmp_warehouse):
    """Re-running the same run_id should not duplicate rows."""
    as_of = date(2026, 10, 1)
    _seed_macro(tmp_warehouse, as_of)
    _seed_quant(tmp_warehouse, as_of)
    _seed_risk(tmp_warehouse, as_of)
    as_of_dt = datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc)
    # Two runs with graph.run_graph forcing same run_id via re-persist
    pod = graph.run_graph(as_of=as_of_dt, run_id="fixed-run-1", llm=None)
    tick._persist(pod)
    tick._persist(pod)   # second write, same PK
    con = duckdb.connect(str(tmp_warehouse))
    (n,) = con.execute("SELECT COUNT(*) FROM pod_decisions").fetchone()
    con.close()
    assert n == 1
