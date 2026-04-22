"""
Offline tests for the Sprint F portfolio layer.

Covers:
  * scenario generator shape + determinism + stress blend
  * Mean-CVaR LP sign convention, constraint binding, gamma effect
  * 3 "break the LP" stress tests:
      - gamma=0 risk-neutral knapsack fills every per-leg cap
      - SOFR carry > expected return → zero allocation
      - tail-risk injection + high gamma → deleverage
  * book.build pipeline: TRS-only filter, unapproved skip, persistence,
    INSERT OR REPLACE idempotency, and --optimize flag end-to-end

Scenario matrices are kept small (T <= 500, n <= 12) so the whole file runs
well inside the 40-second contract.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import numpy as np
import pytest

from agents import tick
from data.schema import DDL
from data.settings import settings
from portfolio import book, mean_cvar
from portfolio.scenario_generator import IssuerSpec, generate_loss_matrix


# --- Fixtures --------------------------------------------------------------
@pytest.fixture()
def tmp_warehouse(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    # Sprint H: the live graph resolves gate 3 from regulatory_catalysts.
    # Seed the EU CCD II row so end-to-end tick tests mirror production.
    con.execute(
        """INSERT INTO regulatory_catalysts
              (catalyst_id, jurisdiction, deadline_date, title, materiality, category, notes)
           VALUES ('ccd_ii_transposition_2026', 'EU', DATE '2026-11-20',
                   'EU CCD II transposition', 1.0, 'transposition', 'test seed')"""
    )
    con.close()
    monkeypatch.setattr(settings, "duckdb_path", db)
    return db


# --- Scenario generator ----------------------------------------------------
def _three_issuer_specs(horizon_days: int = 64) -> list[IssuerSpec]:
    return [
        IssuerSpec("AFRM", kappa=0.5, theta=0.03, sigma=0.08,
                   lambda_0=0.02, ttm_days=horizon_days),
        IssuerSpec("SQ",   kappa=0.4, theta=0.02, sigma=0.06,
                   lambda_0=0.015, ttm_days=horizon_days),
        IssuerSpec("PYPL", kappa=0.6, theta=0.025, sigma=0.07,
                   lambda_0=0.018, ttm_days=horizon_days),
    ]


def test_scenario_matrix_shape_and_nonneg():
    specs = _three_issuer_specs()
    L = generate_loss_matrix(specs, n_scenarios=400, horizon_days=64, seed=7)
    assert L.shape == (400, 3)
    assert np.all(L >= 0.0)
    assert np.all(L <= 1.0)


def test_scenario_generator_reproducible_under_seed():
    specs = _three_issuer_specs()
    L1 = generate_loss_matrix(specs, n_scenarios=300, horizon_days=48, seed=123)
    L2 = generate_loss_matrix(specs, n_scenarios=300, horizon_days=48, seed=123)
    assert np.allclose(L1, L2)


def test_stress_blend_increases_tail_losses():
    specs = _three_issuer_specs()
    L_no_stress = generate_loss_matrix(specs, n_scenarios=400, horizon_days=64,
                                        stress_blend_weight=0.0, seed=11)
    L_stress = generate_loss_matrix(specs, n_scenarios=400, horizon_days=64,
                                     stress_blend_weight=0.8, seed=11)
    p95_no = np.percentile(L_no_stress.flatten(), 95)
    p95_st = np.percentile(L_stress.flatten(), 95)
    assert p95_st > p95_no


# --- Mean-CVaR LP: sign + caps + gamma ------------------------------------
def _profitable_inputs(rng, *, n=3, T=500,
                       mu_gross=0.08, carry=0.02, loss_scale=0.20):
    """A setup where mu is net positive and CVaR is moderate."""
    mu = np.full(n, mu_gross - carry)
    # Scenario tranche losses drawn around a mean > carry so short is profitable
    L = rng.uniform(low=mu_gross * 0.5, high=loss_scale, size=(T, n))
    return mu, L, carry


def test_cvar_lp_binds_leverage_under_low_gamma():
    """At gamma=0.5 with profitable mu, each leg fills to the per-leg cap."""
    rng = np.random.default_rng(0)
    mu, L, carry = _profitable_inputs(rng, n=3)
    sol = mean_cvar.solve(mu=mu, L=L, carry=carry, gamma=0.5,
                          max_gross_leverage=3.0, max_single_weight=0.25)
    # All shorts (≤ 0), per-leg cap binding, leverage = 3 * 0.25 = 0.75.
    assert np.all(sol.weights <= 1e-7)
    assert np.all(np.abs(sol.weights) <= 0.25 + 1e-6)
    assert np.isclose(np.abs(sol.weights).sum(), 0.75, atol=1e-3)


def test_cvar_lp_prefers_higher_mu_leg_under_skewed_mu():
    """With heterogenous mu and moderate gamma, richest leg gets largest |w|."""
    rng = np.random.default_rng(1)
    n, T = 3, 800
    mu = np.array([0.10, 0.01, 0.04])   # AFRM richest, SQ thinnest
    L = rng.uniform(0.02, 0.15, size=(T, n))
    carry = 0.02
    sol = mean_cvar.solve(mu=mu, L=L, carry=carry, gamma=2.0,
                          max_gross_leverage=3.0, max_single_weight=0.25)
    abs_w = np.abs(sol.weights)
    # Richest leg must weakly dominate every other leg.
    assert abs_w[0] >= abs_w[1] - 1e-6
    assert abs_w[0] >= abs_w[2] - 1e-6
    # Thinnest leg must not dominate the richest.
    assert abs_w[1] <= abs_w[0] + 1e-6


def test_cvar_lp_high_gamma_shrinks_leverage():
    rng = np.random.default_rng(2)
    mu, L, carry = _profitable_inputs(rng, n=3, mu_gross=0.06, loss_scale=0.25)
    lev_low = float(np.abs(mean_cvar.solve(
        mu=mu, L=L, carry=carry, gamma=0.5,
        max_gross_leverage=3.0, max_single_weight=0.25).weights).sum())
    lev_high = float(np.abs(mean_cvar.solve(
        mu=mu, L=L, carry=carry, gamma=50.0,
        max_gross_leverage=3.0, max_single_weight=0.25).weights).sum())
    assert lev_high <= lev_low + 1e-6


def test_cvar_lp_solver_reports_optimal():
    rng = np.random.default_rng(3)
    mu, L, carry = _profitable_inputs(rng, n=2, T=200)
    sol = mean_cvar.solve(mu=mu, L=L, carry=carry, gamma=5.0)
    assert sol.status.lower() in {"optimal", "optimal_inaccurate"}


# --- The 3 "break the LP" stress tests ------------------------------------
def test_stress_risk_neutral_knapsack_fills_all_legs_to_cap():
    """gamma=0.0 with profitable mu must max every leg to max_single_weight.

    With 12 legs and per-leg cap 0.25, gross leverage lands at exactly 3.0
    (the global cap). Confirms caps + LP alignment.
    """
    rng = np.random.default_rng(42)
    n, T = 12, 300
    mu = np.full(n, 0.08)     # every leg identically profitable
    L = rng.uniform(0.05, 0.20, size=(T, n))
    carry = 0.02
    sol = mean_cvar.solve(
        mu=mu, L=L, carry=carry, gamma=0.0,
        max_gross_leverage=3.0, max_single_weight=0.25,
    )
    abs_w = np.abs(sol.weights)
    # Every leg at cap.
    assert np.allclose(abs_w, 0.25, atol=1e-3)
    # Gross leverage = 12 * 0.25 = 3.0 exactly.
    assert np.isclose(abs_w.sum(), 3.0, atol=1e-3)


def test_stress_sofr_carry_exceeds_yield_gives_zero_allocation():
    """mu <= 0 (negative carry scenario) must produce |w| = 0 everywhere.

    SOFR at 15% on a 1-year horizon dwarfs the baseline tranche-loss yield;
    the LP should refuse to take any TRS short because every leg loses money
    in expectation.
    """
    n, T = 5, 300
    mu = np.full(n, -0.05)    # deeply negative net return
    rng = np.random.default_rng(7)
    L = rng.uniform(0.01, 0.05, size=(T, n))    # small realized losses
    carry = 0.15              # SOFR spike → 15% cost
    sol = mean_cvar.solve(
        mu=mu, L=L, carry=carry, gamma=5.0,
        max_gross_leverage=3.0, max_single_weight=0.25,
    )
    assert np.allclose(sol.weights, 0.0, atol=1e-6)
    assert sol.expected_return == pytest.approx(0.0, abs=1e-9)


def test_stress_doomsday_tail_with_high_gamma_deleverages():
    """Inject zero-loss scenarios + positive carry (the short's real tail risk)
    and set gamma very high. The LP must aggressively deleverage vs a baseline
    without the injected tail.

    Note: for a TRS SHORT book, the doomsday scenario is NOT `L=1.0` (which
    pays the short maximally) — it is `L≈0` combined with positive carry,
    which means the short pays SOFR for protection on a pool that never
    defaults. The tail of the book-loss distribution sits at L=0.
    """
    rng = np.random.default_rng(11)
    n, T = 3, 400
    mu = np.full(n, 0.04)     # profitable in baseline
    L_base = rng.uniform(0.08, 0.20, size=(T, n))
    carry = 0.02

    # Baseline book — no doomsday.
    lev_base = float(np.abs(mean_cvar.solve(
        mu=mu, L=L_base, carry=carry, gamma=5.0,
        max_gross_leverage=3.0, max_single_weight=0.25).weights).sum())

    # Injected tail: 20 scenarios (5% of T=400) with L=0.
    L_doom = L_base.copy()
    L_doom[:20, :] = 0.0
    lev_doom = float(np.abs(mean_cvar.solve(
        mu=mu, L=L_doom, carry=carry, gamma=100.0,
        max_gross_leverage=3.0, max_single_weight=0.25).weights).sum())

    # Deleveraging must be material, not just a rounding move.
    assert lev_doom < lev_base - 0.05, f"lev_doom={lev_doom}, lev_base={lev_base}"


# --- Book builder end-to-end ----------------------------------------------
def _seed_pod_decision(db: Path, run_id: str, as_of: date, *,
                       approved: bool = True,
                       expression: str = "trs_junior_abs") -> None:
    import json
    con = duckdb.connect(str(db))
    signal = {"expression": expression, "approved": approved,
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


def _seed_sofr(db: Path, as_of: date, value_pct: float = 5.33) -> None:
    con = duckdb.connect(str(db))
    con.execute(
        "INSERT OR REPLACE INTO fred_series (series_id, observed_at, value) "
        "VALUES ('SOFR', ?, ?)",
        [as_of, value_pct],
    )
    con.close()


def test_book_filters_out_equity_short_runs(tmp_warehouse):
    as_of = date(2026, 10, 1)
    _seed_pod_decision(tmp_warehouse, "eq-run-1", as_of,
                       approved=True, expression="equity_short")
    _seed_jt_lambda(tmp_warehouse, as_of)
    _seed_sofr(tmp_warehouse, as_of)
    result = book.build("eq-run-1", persist=True)
    assert result is None

    con = duckdb.connect(str(tmp_warehouse))
    (n,) = con.execute("SELECT COUNT(*) FROM portfolio_weights").fetchone()
    con.close()
    assert n == 0


def test_book_skips_unapproved_runs(tmp_warehouse):
    as_of = date(2026, 10, 1)
    _seed_pod_decision(tmp_warehouse, "trs-bad", as_of, approved=False)
    _seed_jt_lambda(tmp_warehouse, as_of)
    result = book.build("trs-bad", persist=True)
    assert result is None


def test_book_builds_and_persists_weights(tmp_warehouse, monkeypatch):
    as_of = date(2026, 10, 1)
    _seed_pod_decision(tmp_warehouse, "trs-ok-1", as_of, approved=True)
    _seed_jt_lambda(tmp_warehouse, as_of, issuers=("AFRM", "SQ"))
    _seed_sofr(tmp_warehouse, as_of, value_pct=1.0)   # low carry → profitable

    # Shrink scenario count for test-speed.
    from data import settings as settings_mod
    th_real = settings_mod.load_thresholds()
    th_real["portfolio"]["n_scenarios"] = 400
    # Lower gamma so the LP actually opens a book at these modest mu values.
    th_real["portfolio"]["gamma_risk_aversion"] = 1.0
    monkeypatch.setattr(book, "load_thresholds", lambda: th_real)

    result = book.build("trs-ok-1", persist=True, seed=7)
    assert result is not None
    assert result.gamma == pytest.approx(1.0)
    assert result.solver_status.lower() in {"optimal", "optimal_inaccurate"}
    assert np.abs(result.weights).sum() <= 3.0 + 1e-6
    assert np.all(np.abs(result.weights) <= 0.25 + 1e-6)
    assert np.all(result.weights <= 1e-7)

    con = duckdb.connect(str(tmp_warehouse))
    rows = con.execute(
        "SELECT issuer, weight, gamma, solver_status "
        "FROM portfolio_weights WHERE run_id='trs-ok-1' ORDER BY issuer"
    ).fetchall()
    con.close()
    assert len(rows) == len(result.issuers)
    for _iss, _w, gamma_v, status in rows:
        assert gamma_v == pytest.approx(1.0)
        assert status.lower() in {"optimal", "optimal_inaccurate"}


def test_book_insert_or_replace_is_idempotent(tmp_warehouse, monkeypatch):
    as_of = date(2026, 10, 1)
    _seed_pod_decision(tmp_warehouse, "trs-idem", as_of, approved=True)
    _seed_jt_lambda(tmp_warehouse, as_of, issuers=("AFRM", "SQ"))
    _seed_sofr(tmp_warehouse, as_of, value_pct=1.0)

    from data import settings as settings_mod
    th_real = settings_mod.load_thresholds()
    th_real["portfolio"]["n_scenarios"] = 300
    th_real["portfolio"]["gamma_risk_aversion"] = 1.0
    monkeypatch.setattr(book, "load_thresholds", lambda: th_real)

    book.build("trs-idem", persist=True, seed=7)
    book.build("trs-idem", persist=True, seed=7)   # second write, same PK
    con = duckdb.connect(str(tmp_warehouse))
    (n,) = con.execute(
        "SELECT COUNT(*) FROM portfolio_weights WHERE run_id='trs-idem'"
    ).fetchone()
    con.close()
    # Same PK (run_id, issuer) must NOT duplicate.
    assert n <= 5   # at most one row per issuer in TICKER_TO_ISSUER


def test_tick_optimize_flag_runs_end_to_end(tmp_warehouse, monkeypatch):
    """--optimize after approve writes both pod_decisions AND portfolio_weights."""
    as_of = date(2026, 10, 1)
    as_of_dt = datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc)
    con = duckdb.connect(str(tmp_warehouse))
    for i in range(30):
        d = as_of - timedelta(days=29 - i)
        con.execute(
            "INSERT INTO bsi_daily (observed_at, bsi, z_bsi, freeze_flag, "
            "weights_hash) VALUES (?, ?, ?, FALSE, 'test')",
            [d, 2.0, 2.0],
        )
        con.execute(
            "INSERT INTO fred_series (series_id, observed_at, value) "
            "VALUES ('MOVE', ?, 135.0)",
            [d],
        )
    con.execute(
        "INSERT INTO fred_series (series_id, observed_at, value) "
        "VALUES ('SOFR', ?, 1.0)",
        [as_of],
    )
    for iss in ("AFRM", "SQ"):
        con.execute(
            "INSERT INTO scp_daily (ticker, observed_at, scp, z_scp) "
            "VALUES (?, ?, 3.0, 3.0)",
            [iss, as_of],
        )
        con.execute(
            "INSERT INTO jt_lambda (issuer, observed_at, lambda_sys, lambda_unsys, "
            "lambda_total, kappa, theta, sigma, j_max) "
            "VALUES (?, ?, 0.003, 0.017, 0.02, 0.5, 0.025, 0.08, 0.05)",
            [iss, as_of],
        )
    con.close()

    from data import settings as settings_mod
    th_real = settings_mod.load_thresholds()
    th_real["portfolio"]["n_scenarios"] = 300
    th_real["portfolio"]["gamma_risk_aversion"] = 1.0
    monkeypatch.setattr(book, "load_thresholds", lambda: th_real)

    pod = tick.run_pod_tick(
        as_of=as_of_dt, persist=True, optimize=True, llm=None,
    )
    assert pod.compliance_approved is True

    con = duckdb.connect(str(tmp_warehouse))
    (n_pod,) = con.execute(
        "SELECT COUNT(*) FROM pod_decisions WHERE run_id=?", [pod.run_id],
    ).fetchone()
    (n_book,) = con.execute(
        "SELECT COUNT(*) FROM portfolio_weights WHERE run_id=?", [pod.run_id],
    ).fetchone()
    con.close()
    assert n_pod == 1
    assert n_book >= 1
