"""
Phase B.1 tests --- cross-channel decoupling module.

Exercises `signals/decoupling.py` on deterministic synthetic data so that:
    * `rolling_correlation` returns the expected correlation shape.
    * `consumer_subindex` / `market_subindex` weight gated pillars correctly.
    * `event_window_correlations` applies the pre-registered one-sided
      H_1 tests correctly.

Network and warehouse never touched. All fixtures are in-memory.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backtest.event_study import EventWindow
from signals.decoupling import (
    CONSUMER_PILLARS,
    MARKET_PILLARS,
    MIN_EVENTS_FOR_H1,
    consumer_subindex,
    event_window_correlations,
    market_subindex,
    rolling_correlation,
    _write_tex_table,
)


# --------------------------------------------------------------------------
# Deterministic synthetic `compute_bsi` output.
# --------------------------------------------------------------------------
def _synthetic_bsi_out(n: int = 600, rng_seed: int = 42,
                       consumer_market_corr: float = 0.8) -> pd.DataFrame:
    """Build a two-subindex DataFrame that mimics `compute_bsi` output
    columns: z_<pillar>, gamma_<pillar> for every pillar the aggregator
    touches.

    `consumer_market_corr` controls the injected correlation between the
    consumer and market drivers; tests check the rolling correlation
    recovers it within tolerance.
    """
    rng = np.random.default_rng(rng_seed)
    idx = pd.date_range("2022-01-01", periods=n, freq="D")

    # Shared latent driver; consumer pillars load correlated, market pillars
    # load at `consumer_market_corr`.
    latent = rng.standard_normal(n)
    consumer_driver = latent + 0.1 * rng.standard_normal(n)
    noise = rng.standard_normal(n)
    market_driver = (
        consumer_market_corr * consumer_driver
        + np.sqrt(max(0.0, 1.0 - consumer_market_corr ** 2)) * noise
    )

    out = pd.DataFrame(index=idx)
    for p in CONSUMER_PILLARS:
        out[f"z_{p}"] = consumer_driver + 0.3 * rng.standard_normal(n)
        out[f"gamma_{p}"] = 1.0
    for p in MARKET_PILLARS:
        out[f"z_{p}"] = market_driver + 0.3 * rng.standard_normal(n)
        out[f"gamma_{p}"] = 1.0
    return out


# --------------------------------------------------------------------------
# Subindex unit tests
# --------------------------------------------------------------------------
def test_consumer_subindex_drops_gated_off_pillars():
    bsi = _synthetic_bsi_out()
    # Zero the gate on `trends` --- it must not contribute.
    bsi["gamma_trends"] = 0.0
    cons = consumer_subindex(bsi)
    # Reconstruct by hand from the remaining three pillars.
    remaining = [p for p in CONSUMER_PILLARS if p != "trends"]
    z = bsi[[f"z_{p}" for p in remaining]].mean(axis=1)
    assert np.allclose(cons.values, z.values, atol=1e-9)


def test_market_subindex_averages_gated_pillars():
    bsi = _synthetic_bsi_out()
    mkt = market_subindex(bsi)
    z = bsi[[f"z_{p}" for p in MARKET_PILLARS]].mean(axis=1)
    assert np.allclose(mkt.values, z.values, atol=1e-9)


def test_subindex_is_nan_when_all_pillars_gated_off():
    bsi = _synthetic_bsi_out(n=60)
    for p in CONSUMER_PILLARS:
        bsi[f"gamma_{p}"] = 0.0
    cons = consumer_subindex(bsi)
    assert cons.isna().all()


# --------------------------------------------------------------------------
# Rolling correlation
# --------------------------------------------------------------------------
def test_rolling_correlation_recovers_injected_correlation():
    bsi = _synthetic_bsi_out(n=800, consumer_market_corr=0.8)
    cons = consumer_subindex(bsi)
    mkt = market_subindex(bsi)
    rho = rolling_correlation(cons, mkt, window=60)
    # The mean rolling-rho should land in [0.5, 0.95] for consumer_market_corr=0.8
    # (noise on each pillar attenuates the observed correlation).
    mean_rho = float(rho.dropna().mean())
    assert 0.5 < mean_rho < 0.95, f"recovered rho was {mean_rho:.3f}"


def test_rolling_correlation_deterministic_under_fixed_seed():
    bsi1 = _synthetic_bsi_out(n=200, rng_seed=7)
    bsi2 = _synthetic_bsi_out(n=200, rng_seed=7)
    r1 = rolling_correlation(consumer_subindex(bsi1), market_subindex(bsi1))
    r2 = rolling_correlation(consumer_subindex(bsi2), market_subindex(bsi2))
    pd.testing.assert_series_equal(r1, r2)


def test_rolling_correlation_handles_orthogonal_series():
    """With independent signals the rolling correlation mean should be ~0."""
    bsi = _synthetic_bsi_out(n=800, consumer_market_corr=0.0, rng_seed=11)
    rho = rolling_correlation(consumer_subindex(bsi), market_subindex(bsi),
                              window=60)
    mean_rho = float(rho.dropna().mean())
    assert abs(mean_rho) < 0.15, f"orthogonal series produced rho={mean_rho:.3f}"


# --------------------------------------------------------------------------
# Event-window pre-registered test
# --------------------------------------------------------------------------
def _mk_rho_with_on_spike(
    n_pre: int = 95, n_on: int = 11, n_post: int = 95,
    pre_rho: float = 0.10, on_rho: float = 0.80, post_rho: float = 0.10,
    pre_sd: float = 0.03, post_sd: float = 0.03,
) -> pd.Series:
    """Construct a rho time series with a known pre/on/post structure so the
    H_1 test must evaluate True on the single synthetic event."""
    rng = np.random.default_rng(0)
    # Build so that the catalyst sits exactly at the center.
    pre_vals = pre_rho + pre_sd * rng.standard_normal(n_pre)
    on_vals = np.full(n_on, on_rho)
    post_vals = post_rho + post_sd * rng.standard_normal(n_post)
    # Calendar: pre covers catalyst-7 back to catalyst-90 + 7 buffer.
    n = n_pre + n_on + n_post
    catalyst = pd.Timestamp("2024-01-15")
    # align so pre goes [-97..-7], on goes [-5..+5], post goes [+7..+97]
    pre_idx = pd.date_range(catalyst - pd.Timedelta(days=97), periods=n_pre, freq="D")
    on_idx = pd.date_range(catalyst - pd.Timedelta(days=5), periods=n_on, freq="D")
    post_idx = pd.date_range(catalyst + pd.Timedelta(days=7), periods=n_post, freq="D")
    idx = pre_idx.append(on_idx).append(post_idx)
    vals = np.concatenate([pre_vals, on_vals, post_vals])
    return pd.Series(vals, index=idx, name="rho_30d")


def test_event_window_correlations_detects_on_spike():
    rho = _mk_rho_with_on_spike()
    events = {"SYNTH": EventWindow("SYNTH", date(2024, 1, 15))}
    df = event_window_correlations(rho, events=events)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["rho_on"] > row["rho_pre"] + 2.0 * row["se_pre"]
    assert row["rho_on"] > row["rho_post"] + 2.0 * row["se_post"]
    assert bool(row["h1_pre_pass"]) and bool(row["h1_post_pass"])
    assert bool(row["h1_both_pass"])


def test_event_window_correlations_rejects_flat_series():
    """A rho series with NO on-catalyst spike must fail both H_1 flags."""
    rho = _mk_rho_with_on_spike(on_rho=0.10, pre_rho=0.10, post_rho=0.10)
    events = {"SYNTH_NULL": EventWindow("SYNTH_NULL", date(2024, 1, 15))}
    df = event_window_correlations(rho, events=events)
    row = df.iloc[0]
    # Either pre or post must fail (or both). h1_both_pass must be False.
    assert not bool(row["h1_both_pass"])


# --------------------------------------------------------------------------
# TeX output smoke test
# --------------------------------------------------------------------------
def test_write_tex_table_respects_H1_verdict(tmp_path: Path):
    # Craft a fake event-table that satisfies H_1 on all 5 events.
    df = pd.DataFrame({
        "event_name": [f"EVENT_{i}" for i in range(5)],
        "catalyst_date": [date(2024, 1, 15)] * 5,
        "rho_pre":  [0.1] * 5,
        "rho_on":   [0.8] * 5,
        "rho_post": [0.1] * 5,
        "se_pre":   [0.01] * 5,
        "se_post":  [0.01] * 5,
        "h1_pre_pass": [True] * 5,
        "h1_post_pass": [True] * 5,
        "h1_both_pass": [True] * 5,
    })
    out_path = tmp_path / "decoupling_table.tex"
    _write_tex_table(df, out_path)
    text = out_path.read_text(encoding="utf-8")
    assert r"\begin{tabular}" in text
    assert r"\bottomrule" in text
    assert "SUPPORTED" in text
    assert f"{MIN_EVENTS_FOR_H1}/5" in text

    # And the opposite: zero events pass --> NOT SUPPORTED.
    df2 = df.copy()
    df2["h1_pre_pass"] = False
    df2["h1_post_pass"] = False
    df2["h1_both_pass"] = False
    out_path2 = tmp_path / "decoupling_table2.tex"
    _write_tex_table(df2, out_path2)
    text2 = out_path2.read_text(encoding="utf-8")
    assert "NOT SUPPORTED" in text2
