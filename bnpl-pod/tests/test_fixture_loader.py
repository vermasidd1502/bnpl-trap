"""
Sprint H.b — `load_window_from_warehouse` smoke + contract tests.

The bridge is the only place in the repo where "what's in DuckDB" meets
"what the backtest consumes." If these assertions pass, the event study
can be re-run against real historical data end-to-end.

The tests never touch the live warehouse. Each test seeds an in-memory
DuckDB with a deterministic fixture, points the loader at that handle,
and checks the `WindowFixture` shape / numerics.
"""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import numpy as np
import pandas as pd
import pytest

from backtest.event_study import (
    WINDOWS,
    load_window_from_warehouse,
)
from data.schema import DDL


# --------------------------------------------------------------------------
# Warehouse seed helpers
# --------------------------------------------------------------------------
def _seed_con(catalyst: date,
              *,
              lookback: int = 120,
              lookahead: int = 120,
              move_level: float = 130.0,
              sofr_pct: float = 4.0,
              hyg_start: float = 80.0,
              afrm_start: float = 25.0,
              bsi_z: float = 1.9,
              include_excess_spread: bool = False,
              drop_bsi_after: date | None = None) -> duckdb.DuckDBPyConnection:
    """Build an in-memory DuckDB with the pod schema and synthetic daily series.

    `catalyst` defines the window center; we insert a generous span either
    side so the loader's ffill + MA30 warmup never touches the edges.
    """
    con = duckdb.connect(":memory:")
    for stmt in DDL:
        con.execute(stmt)

    # Business-day span comfortably wider than (lookback+lookahead) business
    # days plus the MOVE MA30 warmup padding inside the loader.
    start = catalyst - timedelta(days=lookback + 120)
    end   = catalyst + timedelta(days=lookahead + 30)
    cal = pd.bdate_range(start=start, end=end)

    # Deterministic ticker paths — smooth enough that pct_change is non-zero
    # but finite, so the loader's validation passes.
    rng = np.random.default_rng(seed=42)
    hyg  = hyg_start  * np.cumprod(1 + 0.0002 * rng.standard_normal(len(cal)))
    afrm = afrm_start * np.cumprod(1 + 0.0015 * rng.standard_normal(len(cal)))

    def _ins_series(series_id: str, vals):
        rows = [(series_id, d.date(), float(v)) for d, v in zip(cal, vals)]
        con.executemany(
            "INSERT OR REPLACE INTO fred_series (series_id, observed_at, value) "
            "VALUES (?, ?, ?)",
            rows,
        )

    _ins_series("MOVE", np.full(len(cal), move_level))
    _ins_series("SOFR", np.full(len(cal), sofr_pct))
    _ins_series("HYG",  hyg)
    _ins_series("AFRM", afrm)

    # BSI — truncate optionally to model a warehouse that is missing the
    # recent tail (we expect the loader to raise in that case).
    bsi_rows = []
    for d in cal:
        if drop_bsi_after is not None and d.date() > drop_bsi_after:
            continue
        bsi_rows.append((d.date(), float(bsi_z), float(bsi_z), "{}"))
    con.executemany(
        "INSERT OR REPLACE INTO bsi_daily (observed_at, bsi, z_bsi, weights_hash) "
        "VALUES (?, ?, ?, ?)",
        bsi_rows,
    )

    if include_excess_spread:
        # One quarterly AFRMT print per ~90 days, below the catalyst.
        d = start
        while d <= end:
            con.execute(
                "INSERT OR REPLACE INTO abs_tranche_metrics "
                "(accession_no, trust_name, period_end, roll_rate_60p, "
                " excess_spread, cnl) VALUES (?, ?, ?, ?, ?, ?)",
                [f"acc-{d.isoformat()}", "AFRMT 2022-A", d, 5.0, 8.5, 2.1],
            )
            d = d + timedelta(days=90)

    return con


# --------------------------------------------------------------------------
# Contract tests
# --------------------------------------------------------------------------
def test_unknown_window_raises():
    con = duckdb.connect(":memory:")
    for s in DDL:
        con.execute(s)
    with pytest.raises(KeyError):
        load_window_from_warehouse("NOT_A_WINDOW", con=con)


def test_klarna_window_reconstructs_full_fixture():
    w = WINDOWS["KLARNA_DOWNROUND"]
    con = _seed_con(w.catalyst_date)
    fx = load_window_from_warehouse("KLARNA_DOWNROUND", con=con)

    # Shape: business-day aligned, length = lookback+lookahead+1.
    expected_T = w.lookback_days + w.lookahead_days + 1
    assert len(fx.dates) == expected_T
    assert fx.move_level.shape == (expected_T,)
    assert fx.move_ma30.shape == (expected_T,)
    assert fx.sofr_annual.shape == (expected_T,)
    assert fx.bsi_z.shape == (expected_T,)
    assert fx.tranche_book_returns.shape == (expected_T,)
    assert fx.hyg_returns.shape == (expected_T,)
    assert fx.afrm_returns.shape == (expected_T,)

    # Numeric sanity.
    assert np.all(np.isfinite(fx.move_level))
    assert np.all(np.isfinite(fx.move_ma30))
    assert np.all(np.isfinite(fx.sofr_annual))
    assert np.all(np.isfinite(fx.bsi_z))
    assert np.all(np.isfinite(fx.tranche_book_returns))

    # SOFR converted percent→decimal.
    assert 0.0 < fx.sofr_annual.mean() < 0.15   # 4% → 0.04

    # BSI z stable at seeded value after ffill.
    assert np.isclose(fx.bsi_z.mean(), 1.9, atol=1e-6)

    # MA30 matches level when level is constant.
    assert np.allclose(fx.move_ma30, fx.move_level, atol=1e-6)

    # Catalyst day must sit inside the window.
    assert fx.dates[0] <= w.catalyst_date <= fx.dates[-1]


def test_missing_bsi_raises_with_actionable_message():
    """Empty bsi_daily must raise a ValueError pointing at signals.bsi."""
    w = WINDOWS["AFFIRM_GUIDANCE_1"]
    con = duckdb.connect(":memory:")
    for s in DDL:
        con.execute(s)
    # Seed fred_series but leave bsi_daily empty — this is the real-world
    # failure mode when a user runs ingest but forgets `python -m signals.bsi`.
    cal = pd.bdate_range(
        start=w.catalyst_date - timedelta(days=300),
        end=w.catalyst_date + timedelta(days=300),
    )
    for sid, level in [("MOVE", 130.0), ("SOFR", 4.0), ("HYG", 80.0), ("AFRM", 25.0)]:
        rows = [(sid, d.date(), level) for d in cal]
        con.executemany(
            "INSERT OR REPLACE INTO fred_series (series_id, observed_at, value) VALUES (?, ?, ?)",
            rows,
        )
    with pytest.raises(ValueError, match="no bsi_daily rows"):
        load_window_from_warehouse("AFFIRM_GUIDANCE_1", con=con)


def test_partial_bsi_truncation_still_ffills_tail():
    """Ffill bridges a late-publishing signal: truncating BSI 30 days before
    the catalyst must NOT raise — the last observation carries forward.
    This is the intentional contract (real FRED signals publish late)."""
    w = WINDOWS["AFFIRM_GUIDANCE_1"]
    con = _seed_con(w.catalyst_date,
                    drop_bsi_after=w.catalyst_date - timedelta(days=30))
    fx = load_window_from_warehouse("AFFIRM_GUIDANCE_1", con=con)
    assert np.all(np.isfinite(fx.bsi_z))


def test_missing_move_raises_pointing_to_yahoo_macro():
    w = WINDOWS["KLARNA_DOWNROUND"]
    con = duckdb.connect(":memory:")
    for s in DDL:
        con.execute(s)
    # Leave fred_series completely empty.
    with pytest.raises(ValueError, match="yahoo_macro"):
        load_window_from_warehouse("KLARNA_DOWNROUND", con=con)


def test_excess_spread_carry_boosts_tranche_return():
    """When abs_tranche_metrics has rows, the tranche return gets a carry overlay."""
    w = WINDOWS["AFFIRM_GUIDANCE_2"]
    con_no  = _seed_con(w.catalyst_date, include_excess_spread=False)
    con_yes = _seed_con(w.catalyst_date, include_excess_spread=True)
    fx_no  = load_window_from_warehouse("AFFIRM_GUIDANCE_2", con=con_no)
    fx_yes = load_window_from_warehouse("AFFIRM_GUIDANCE_2", con=con_yes)
    # Carry is positive (8.5 %/252 ≈ 3.4 bps/day); mean should lift.
    assert fx_yes.tranche_book_returns.mean() > fx_no.tranche_book_returns.mean()


def test_naive_companion_always_populated_and_is_spread_only():
    """Sprint H.c: the loader emits `tranche_book_returns_naive` on every call
    so the three-panel NAIVE arm can quantify the duration-blind P&L lie."""
    w = WINDOWS["KLARNA_DOWNROUND"]
    con = _seed_con(w.catalyst_date)
    fx = load_window_from_warehouse("KLARNA_DOWNROUND", con=con)
    assert fx.tranche_book_returns_naive is not None
    assert fx.tranche_book_returns_naive.shape == fx.tranche_book_returns.shape
    # With constant SOFR the duration hit is zero → the two series are
    # identical; with a step-up they must differ.
    assert np.allclose(fx.tranche_book_returns, fx.tranche_book_returns_naive)

    # Now inject a rate step-up and re-load: canonical ≠ naive.
    con.execute(
        "UPDATE fred_series SET value = 6.0 "
        "WHERE series_id = 'SOFR' AND observed_at >= ?",
        [w.catalyst_date],
    )
    fx2 = load_window_from_warehouse("KLARNA_DOWNROUND", con=con)
    assert fx2.tranche_book_returns_naive is not None
    # Duration-adjusted (canonical) must be LOWER on average than spread-only.
    assert fx2.tranche_book_returns.sum() < fx2.tranche_book_returns_naive.sum()
    # The spread-only companion matches the legacy spread-only loader output.
    fx_legacy = load_window_from_warehouse(
        "KLARNA_DOWNROUND", con=con, apply_duration_adjustment=False,
    )
    assert np.allclose(fx_legacy.tranche_book_returns, fx2.tranche_book_returns_naive)


def test_run_window_routes_naive_tranche_by_mode():
    """PnLMode.NAIVE/FIX3_ONLY consume tranche_book_returns_naive when set;
    INSTITUTIONAL always consumes the canonical duration-adjusted series."""
    from backtest.event_study import PnLMode, run_window

    w = WINDOWS["AFFIRM_GUIDANCE_2"]
    con = _seed_con(w.catalyst_date)
    # Step-up SOFR across the catalyst so the two series diverge measurably.
    con.execute(
        "UPDATE fred_series SET value = 6.5 "
        "WHERE series_id = 'SOFR' AND observed_at >= ?",
        [w.catalyst_date],
    )
    fx = load_window_from_warehouse("AFFIRM_GUIDANCE_2", con=con)

    inst = run_window(fx, mode=PnLMode.INSTITUTIONAL)
    naive = run_window(fx, mode=PnLMode.NAIVE)
    # Different tranche series → different TRS-arm cumulative P&L.
    # (Short TRS book profits from spread widening and LOSES from the
    # duration leg when rates rise. INSTITUTIONAL includes the duration
    # hit in the LONG-tranche reference path, so the SHORT realizes more
    # gain — i.e. INSTITUTIONAL TRS total_return >= NAIVE TRS total_return.)
    assert inst.trs_stats.total_return != naive.trs_stats.total_return


def test_duration_adjustment_dominates_when_rates_move():
    """Phase 2: a synthetic SOFR step-up produces a measurable MTM drag."""
    w = WINDOWS["AFFIRM_GUIDANCE_1"]
    con = _seed_con(w.catalyst_date)
    # Overwrite half the SOFR series to simulate a +200bp hike around the
    # window — the duration-adjusted fixture must be materially lower than
    # the spread-only one.
    half_cut = w.catalyst_date
    con.execute(
        "UPDATE fred_series SET value = 6.0 "
        "WHERE series_id = 'SOFR' AND observed_at >= ?",
        [half_cut],
    )
    fx_spread = load_window_from_warehouse(
        "AFFIRM_GUIDANCE_1", con=con, apply_duration_adjustment=False,
    )
    fx_dur = load_window_from_warehouse(
        "AFFIRM_GUIDANCE_1", con=con, apply_duration_adjustment=True,
        tranche_wal_years=3.0,
    )
    # Duration-adjusted cumulative return should be LOWER (rates rose).
    assert fx_dur.tranche_book_returns.sum() < fx_spread.tranche_book_returns.sum()
    # And the delta should be of roughly the right magnitude:
    # -WAL * ΔSOFR = -3.0 * 0.02 = -0.06 over the step-up day.
    delta = fx_spread.tranche_book_returns - fx_dur.tranche_book_returns
    # Sum of the positive deltas should be near 0.06 (the one-day step).
    assert 0.04 <= delta.sum() <= 0.08, f"duration hit sum was {delta.sum():.4f}"
    # Same assertion via the spread-only companion on the duration-on fixture.
    assert np.allclose(fx_spread.tranche_book_returns,
                       fx_dur.tranche_book_returns_naive)


def test_afrm_returns_are_returns_not_levels():
    """Sanity: AFRM returns cluster around 0, not around $25."""
    w = WINDOWS["CFPB_INTERP_RULE"]
    con = _seed_con(w.catalyst_date)
    fx = load_window_from_warehouse("CFPB_INTERP_RULE", con=con)
    assert abs(fx.afrm_returns.mean()) < 0.01      # not centered on $25
    assert abs(fx.hyg_returns.mean())  < 0.01
    assert fx.afrm_returns.std() > 0.0             # non-degenerate


def test_load_all_windows_from_warehouse_round_trip():
    """End-to-end: all five windows reconstruct from one warehouse snapshot."""
    from backtest.event_study import load_all_windows_from_warehouse

    # One warehouse that covers the full 2022–2025 span. Must be wide enough
    # for BOTH the earliest window (KLARNA 2022-07-11) and the latest
    # (REGZ_EFFECTIVE 2025-01-17, added when the paper anchor landed).
    con = duckdb.connect(":memory:")
    for s in DDL:
        con.execute(s)
    # Reuse helper by inserting a combined span manually. `lookahead=700` puts
    # the right edge well past 2025-04-06 (the loader's pad-right horizon for
    # REGZ_EFFECTIVE: 40 trading-day lookahead * 1.6 + 15d pad).
    combined = _seed_con(date(2023, 6, 1),
                         lookback=400, lookahead=700,
                         include_excess_spread=True)
    # Move the seeded rows into `con` by re-emitting via INSERT.
    # Simpler: just pass `combined` directly to the loader — it already holds
    # the full DDL + seeded rows.
    fixtures = load_all_windows_from_warehouse(con=combined)
    assert set(fixtures) == set(WINDOWS)
    for k, fx in fixtures.items():
        w = WINDOWS[k]
        assert len(fx.dates) == w.lookback_days + w.lookahead_days + 1
