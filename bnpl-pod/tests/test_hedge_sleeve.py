"""Offline tests for the macro-hedge sleeve (Fix #2).

The sleeve is sized statically AFTER the Mean-CVaR LP clears, so these
tests exercise:
  * the sizing math (beta_credit and dv01_neutral paths)
  * persistence round-trip to portfolio_hedges
  * end-to-end book.build populates both portfolio_weights and portfolio_hedges
  * unknown sizing_rule raises
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from agents.schemas import MacroHedgeSpec
from data.schema import DDL
from data.settings import settings
from portfolio import book


@pytest.fixture()
def tmp_warehouse(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(settings, "duckdb_path", db)
    return db


# --- Sizing math ----------------------------------------------------------
def test_beta_credit_sizing_signs_and_magnitude():
    """|hedge_notional| = beta * trs_gross, and sign is negative (short)."""
    cfg = {"instrument": "HYG_SHORT", "sizing_rule": "beta_credit",
           "beta_credit": 0.60}
    h = book._size_hedge_sleeve(cfg, trs_gross=2.5)
    assert h.instrument == "HYG_SHORT"
    assert h.sizing_rule == "beta_credit"
    assert h.trs_gross == pytest.approx(2.5)
    assert h.hedge_ratio == pytest.approx(0.60)
    # |hedge| = 0.60 * 2.5 = 1.50, signed short (negative).
    assert h.notional == pytest.approx(-1.5)
    assert "beta_credit" in h.rationale


def test_beta_credit_zero_trs_produces_zero_hedge():
    """If the LP refuses to open a book (|w|=0), the sleeve is zero too."""
    cfg = {"sizing_rule": "beta_credit", "beta_credit": 0.60,
           "instrument": "HYG_SHORT"}
    h = book._size_hedge_sleeve(cfg, trs_gross=0.0)
    assert h.notional == pytest.approx(0.0)
    assert h.trs_gross == pytest.approx(0.0)


def test_dv01_neutral_placeholder_path():
    cfg = {"instrument": "ZT_FUT", "sizing_rule": "dv01_neutral",
           "dv01_target": 1.25}
    h = book._size_hedge_sleeve(cfg, trs_gross=2.0)
    assert h.instrument == "ZT_FUT"
    assert h.sizing_rule == "dv01_neutral"
    assert h.hedge_ratio == pytest.approx(1.25)
    assert h.notional == pytest.approx(-2.5)
    assert "Sprint G" in h.rationale or "placeholder" in h.rationale


def test_unknown_sizing_rule_raises():
    cfg = {"instrument": "HYG_SHORT", "sizing_rule": "black_scholes_vibes"}
    with pytest.raises(ValueError, match="unknown hedge sizing_rule"):
        book._size_hedge_sleeve(cfg, trs_gross=1.0)


# --- Persistence round-trip ----------------------------------------------
def test_persist_hedge_writes_row_and_is_idempotent(tmp_warehouse):
    con = duckdb.connect(str(tmp_warehouse))
    try:
        spec = MacroHedgeSpec(
            instrument="HYG_SHORT", sizing_rule="beta_credit",
            notional=-1.5, hedge_ratio=0.6, trs_gross=2.5,
            rationale="beta_credit sleeve test",
        )
        book._persist_hedge(con, "run-1", spec)
        book._persist_hedge(con, "run-1", spec)   # second write, same PK
        rows = con.execute(
            "SELECT instrument, notional, hedge_ratio, trs_gross "
            "FROM portfolio_hedges WHERE run_id='run-1'"
        ).fetchall()
    finally:
        con.close()
    assert len(rows) == 1   # PK (run_id, instrument) de-dupes
    instr, notional, ratio, gross = rows[0]
    assert instr == "HYG_SHORT"
    assert notional == pytest.approx(-1.5)
    assert ratio == pytest.approx(0.6)
    assert gross == pytest.approx(2.5)


# --- End-to-end: book.build populates both tables -------------------------
def _seed_pod_decision(db: Path, run_id: str, as_of: date,
                        *, approved: bool = True) -> None:
    import json
    con = duckdb.connect(str(db))
    signal = {"expression": "trs_junior_abs", "approved": approved,
              "equity_tickers": [], "reasons": []}
    con.execute(
        """INSERT OR REPLACE INTO pod_decisions
             (run_id, as_of, bsi, move_ma30, scp_by_ticker_json,
              gate_bsi, gate_scp, gate_move, gate_ccd2,
              squeeze_veto, compliance_ok, compliance_reasons,
              llm_advisory, trade_signal_json)
           VALUES (?, ?, 0.5, 130.0, '{}', ?, ?, ?, ?, FALSE, ?, '[]', '',
                   ?)""",
        [run_id, datetime.combine(as_of, datetime.min.time()),
         approved, approved, approved, approved, approved,
         json.dumps(signal)],
    )
    con.close()


def _seed_jt_lambda(db: Path, as_of: date, issuers=("AFRM", "SQ")) -> None:
    con = duckdb.connect(str(db))
    for iss in issuers:
        con.execute(
            """INSERT OR REPLACE INTO jt_lambda
                 (issuer, observed_at, lambda_sys, lambda_unsys, lambda_total,
                  kappa, theta, sigma, j_max)
               VALUES (?, ?, 0.003, 0.017, 0.02, 0.5, 0.025, 0.08, 0.05)""",
            [iss, as_of],
        )
    con.close()


def _seed_sofr(db: Path, as_of: date, value_pct: float = 1.0) -> None:
    con = duckdb.connect(str(db))
    con.execute(
        "INSERT OR REPLACE INTO fred_series (series_id, observed_at, value) "
        "VALUES ('SOFR', ?, ?)",
        [as_of, value_pct],
    )
    con.close()


def test_book_build_writes_hedge_row(tmp_warehouse, monkeypatch):
    """After an approved TRS run, portfolio_hedges has exactly one sleeve row."""
    as_of = date(2026, 10, 1)
    _seed_pod_decision(tmp_warehouse, "trs-hedge-1", as_of, approved=True)
    _seed_jt_lambda(tmp_warehouse, as_of, issuers=("AFRM", "SQ"))
    _seed_sofr(tmp_warehouse, as_of, value_pct=1.0)

    # Shrink scenario count + gamma so the LP opens a book.
    from data import settings as settings_mod
    th_real = settings_mod.load_thresholds()
    th_real["portfolio"]["n_scenarios"] = 400
    th_real["portfolio"]["gamma_risk_aversion"] = 1.0
    # Ensure hedge config is present (thresholds.yaml already has it, but
    # tighten beta so the test math is obvious).
    th_real["portfolio"]["hedge"] = {
        "instrument": "HYG_SHORT", "sizing_rule": "beta_credit",
        "beta_credit": 0.60, "dv01_target": 1.0,
    }
    monkeypatch.setattr(book, "load_thresholds", lambda: th_real)

    result = book.build("trs-hedge-1", persist=True, seed=7)
    assert result is not None
    assert result.hedge is not None
    # Hedge proportional to TRS gross leverage.
    import numpy as np
    trs_gross = float(np.abs(result.weights).sum())
    assert result.hedge.trs_gross == pytest.approx(trs_gross)
    assert result.hedge.notional == pytest.approx(-0.60 * trs_gross)

    con = duckdb.connect(str(tmp_warehouse))
    rows = con.execute(
        "SELECT instrument, sizing_rule, notional, hedge_ratio, trs_gross "
        "FROM portfolio_hedges WHERE run_id='trs-hedge-1'"
    ).fetchall()
    con.close()
    assert len(rows) == 1
    instr, rule, notional, ratio, gross = rows[0]
    assert instr == "HYG_SHORT"
    assert rule == "beta_credit"
    assert notional == pytest.approx(-0.60 * trs_gross)
    assert ratio == pytest.approx(0.60)
    assert gross == pytest.approx(trs_gross)
