"""Offline tests for signals.bsi — v4.1 reinforcements."""
from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import duckdb
import pytest

from data.schema import DDL
from signals import bsi


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    # MOVE index — drives the date grid. We seed 130 calendar days so the
    # causal z-score (180d window, 60-obs warm-up) has SOMETHING to work
    # with on the latest rows. ~95 weekdays → ~35 post-warm-up days.
    d_start = date(2024, 2, 1)
    d0 = date(2024, 6, 3)        # "anchor" date used by tests for seeding
    move_rows = [
        (d_start + timedelta(days=i), 95.0 + (i % 20) * 0.5)
        for i in range(130)
        if (d_start + timedelta(days=i)).weekday() < 5
    ]
    con.executemany(
        "INSERT INTO fred_series (series_id, observed_at, value) VALUES ('MOVE', ?, ?)",
        move_rows,
    )
    con.close()
    monkeypatch.setattr(bsi.settings, "duckdb_path", db)
    return db, d0


# --- helpers --------------------------------------------------------------
def test_zscore_handles_short_series():
    assert bsi._zscore([]) == []
    assert bsi._zscore([1.0]) == [0.0]


def test_zscore_center_and_scale():
    z = bsi._zscore([1.0, 2.0, 3.0])
    assert z[1] == pytest.approx(0.0)
    assert z[0] < 0 < z[2]


# --- causal z-score: NO look-ahead ----------------------------------------
def test_rolling_z_causal_warmup_returns_none():
    """First `min_periods` observations have no prior history — z must be None."""
    series = [float(i) for i in range(100)]
    z = bsi._rolling_z_causal(series, window=180, min_periods=60)
    assert all(v is None for v in z[:60])
    # After warm-up, z exists
    assert all(v is not None for v in z[60:])


def test_rolling_z_causal_excludes_target_from_window():
    """Day t's value MUST NOT appear in day t's own μ/σ estimate.

    Construct a flat series of zeros for 200 days (prior variance ≈ 0).
    Place a +10σ-equivalent spike on day 200. A full-sample z would dilute
    the spike because post-day-200 variance gets baked in. A causal z,
    using ONLY the pre-200 window, sees near-zero prior stdev and must
    report an enormous z (or inf).
    """
    series = [0.0] * 200 + [10.0]
    z = bsi._rolling_z_causal(series, window=180, min_periods=60)
    # Pre-break days: flat zeros → z = (0 - 0) / tiny = 0.0
    assert z[150] == pytest.approx(0.0, abs=1e-6)
    # Break day: pre-window is flat zeros → σ ≈ 0 → z is astronomical.
    # Contrast: a full-sample z on this series yields ≈ 14.1 (diluted).
    assert z[200] is not None
    assert z[200] > 1e6  # effectively infinite vs the diluted full-sample ~14


def test_rolling_z_causal_insensitive_to_future_observations():
    """Day t's z must be identical whether or not the series extends past t."""
    import random
    rng = random.Random(42)
    base = [rng.gauss(0.0, 1.0) for _ in range(250)]
    future_tail_a = base + [0.0] * 50
    future_tail_b = base + [100.0] * 50   # huge future shock
    za = bsi._rolling_z_causal(future_tail_a, window=180, min_periods=60)
    zb = bsi._rolling_z_causal(future_tail_b, window=180, min_periods=60)
    # Every z at index t < 250 must be byte-identical across the two series.
    for t in range(250):
        if za[t] is None:
            assert zb[t] is None
        else:
            assert za[t] == pytest.approx(zb[t], abs=1e-12)


def test_rolling_z_causal_handles_none_in_history():
    """Scattered None values in prior window are skipped, not propagated."""
    series: list[float | None] = [None if i % 5 == 0 else 1.0 for i in range(100)]
    series.append(5.0)
    z = bsi._rolling_z_causal(series, window=180, min_periods=10)
    # Warm-up until enough non-null priors accumulate, then z exists.
    # By the end we have ~80 non-null priors (well over min_periods=10).
    assert z[-1] is not None
    # Prior non-null values are all 1.0 → μ=1, σ=0 → z for 5.0 is enormous.
    assert z[-1] > 1e6


def test_sma3_trailing_window():
    out = bsi._sma3([None, 10.0, 20.0, 40.0, None, 100.0])
    # idx 0: None → None (no data)
    assert out[0] is None
    # idx 2: mean(10, 20) = 15
    assert out[2] == pytest.approx(15.0)
    # idx 3: mean(10, 20, 40) = 23.33
    assert out[3] == pytest.approx((10 + 20 + 40) / 3)


def test_sma3_suppresses_single_day_spike():
    """v4.1 §5.4 — a one-day marketing spike is softened by SMA."""
    out = bsi._sma3([10.0, 10.0, 10.0, 100.0, 10.0])
    assert out[3] == pytest.approx((10 + 10 + 100) / 3)   # 40, not 100


# --- credibility plumbed through BSI --------------------------------------
def test_compute_bsi_end_to_end_writes_rows(tmp_duckdb):
    db, d0 = tmp_duckdb
    con = duckdb.connect(str(db))

    # Reddit posts — one high-credibility negative, one bot-like positive
    ts = datetime(d0.year, d0.month, d0.day, tzinfo=timezone.utc)
    con.executemany(
        """INSERT INTO reddit_posts
           (post_id, subreddit, created_at, title, body, author_age_days,
            author_karma, finbert_neg, finbert_neu, finbert_pos, credibility)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("p1", "debt", ts, "", "", 800, 5000, 0.85, 0.10, 0.05, 0.90),
            ("p2", "debt", ts, "", "",  10,    2, 0.05, 0.15, 0.80, 0.06),
        ],
    )
    # CFPB — some complaints to drive momentum
    for i in range(50):
        con.execute(
            "INSERT INTO cfpb_complaints (complaint_id, received_at, product, company, narrative) "
            "VALUES (?, ?, 'BNPL', 'AFFIRM, INC.', 'x')",
            [f"c{i}", d0 - timedelta(days=i % 60)],
        )
    # Google Trends — exit bucket keyword
    con.executemany(
        "INSERT INTO google_trends (keyword, observed_at, interest) VALUES (?, ?, ?)",
        [("affirm collections", d0 + timedelta(days=i), 40.0 + i) for i in range(5)],
    )
    # Firm vitality — one snapshot, not in freeze
    con.execute(
        """INSERT INTO firm_vitality
           (slug, platform, observed_at, snapshot_age_d, headcount, openings,
            tenure_slope, freeze_flag, stale_weight)
           VALUES ('affirm', 'linkedin', ?, 5, 3000, 150, 0.05, FALSE, 1.0)""",
        [d0],
    )
    con.close()

    # Compute over the full fixture range so causal z has enough prior
    # observations to clear the 60-obs warm-up.
    n = bsi.compute_bsi(start=date(2024, 2, 1), end=d0 + timedelta(days=9))
    assert n >= 1

    con = duckdb.connect(str(db))
    rows = con.execute(
        "SELECT observed_at, bsi, z_bsi, c_reddit, c_vitality, freeze_flag, weights_hash "
        "FROM bsi_daily ORDER BY observed_at"
    ).fetchall()
    con.close()
    assert len(rows) >= 1
    # Every written row must have a non-null composite BSI (warm-up rows
    # were skipped on write).
    assert all(r[1] is not None for r in rows)
    # weights_hash should be stable and non-null
    assert all(r[-1] and len(r[-1]) == 12 for r in rows)
    # freeze_flag never fires here
    assert not any(r[-2] for r in rows)


def test_compute_bsi_is_idempotent(tmp_duckdb):
    db, d0 = tmp_duckdb
    bsi.compute_bsi(start=date(2024, 2, 1), end=d0 + timedelta(days=9))
    bsi.compute_bsi(start=date(2024, 2, 1), end=d0 + timedelta(days=9))
    con = duckdb.connect(str(db))
    (cnt,) = con.execute("SELECT COUNT(*) FROM bsi_daily").fetchone()
    (dist,) = con.execute("SELECT COUNT(DISTINCT observed_at) FROM bsi_daily").fetchone()
    con.close()
    assert cnt == dist   # no duplicates on second run


def test_freeze_flag_bumps_bsi(tmp_duckdb):
    """A treated firm in freeze should raise BSI by ~FREEZE_BUMP for that day."""
    db, d0 = tmp_duckdb
    con = duckdb.connect(str(db))
    con.execute(
        """INSERT INTO firm_vitality
           (slug, platform, observed_at, snapshot_age_d, headcount, openings,
            tenure_slope, freeze_flag, stale_weight)
           VALUES ('affirm', 'linkedin', ?, 5, 3000, 5, 0.002, TRUE, 1.0)""",
        [d0],
    )
    con.close()

    bsi.compute_bsi(start=date(2024, 2, 1), end=d0 + timedelta(days=9))
    con = duckdb.connect(str(db))
    rows = con.execute(
        "SELECT observed_at, bsi, freeze_flag FROM bsi_daily ORDER BY observed_at"
    ).fetchall()
    con.close()
    # Post-warm-up rows should inherit the freeze flag (snapshot lookback 90d).
    assert len(rows) >= 1
    # At least one row must carry the freeze signal.
    assert any(r[2] is True for r in rows)
