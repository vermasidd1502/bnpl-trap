"""
Phase B.3 tests --- permutation-null module.

Exercises `signals/permutation_null.py` on deterministic synthetic z_bsi
series so that:
    * A series with a known injected spike recovers the spike at a very
      high percentile of the shuffled-catalyst null.
    * The null distribution is deterministic under a fixed RNG seed.
    * The one-sided p-value is monotonic in the observed magnitude.

Warehouse never touched. All fixtures are in-memory.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from signals.permutation_null import (
    H1_PERCENTILE_THRESHOLD,
    WINDOW_HALFWIDTH,
    percentile_of_observed,
    permutation_test,
    rolling_max_abs_z,
    shuffle_catalysts,
)


# --------------------------------------------------------------------------
# Synthetic z_bsi with a known spike
# --------------------------------------------------------------------------
SPIKE_POS = 1500


def _mk_z_series(n: int = 2000, spike_value: float = 10.0,
                 rng_seed: int = 1, inject_spike: bool = True) -> pd.Series:
    """Mean-zero unit-sigma Gaussian background with an optional single
    large spike at a known date (row `SPIKE_POS`)."""
    rng = np.random.default_rng(rng_seed)
    vals = rng.standard_normal(n)
    if inject_spike and n > SPIKE_POS:
        vals[SPIKE_POS] = spike_value
    idx = pd.bdate_range("2018-01-01", periods=n)
    return pd.Series(vals, index=idx, name="z_bsi")


def _spike_date(n: int = 2000) -> pd.Timestamp:
    return pd.bdate_range("2018-01-01", periods=n)[SPIKE_POS]


# --------------------------------------------------------------------------
# rolling_max_abs_z
# --------------------------------------------------------------------------
def test_rolling_max_abs_z_recovers_spike():
    z = _mk_z_series(spike_value=10.0)
    rmax = rolling_max_abs_z(z, halfwidth=WINDOW_HALFWIDTH)
    # The spike at position 1500 must show up in windows centered on any of
    # positions 1495 through 1505.
    window_center_dates = z.index[1495:1506]
    for d in window_center_dates:
        assert rmax.loc[d] >= 10.0 - 1e-9


def test_rolling_max_abs_z_returns_same_index():
    z = _mk_z_series(n=100, inject_spike=False)
    rmax = rolling_max_abs_z(z)
    assert list(rmax.index) == list(z.index)


# --------------------------------------------------------------------------
# shuffle_catalysts determinism + distribution shape
# --------------------------------------------------------------------------
def test_shuffle_catalysts_determinism_under_fixed_seed():
    z = _mk_z_series(spike_value=9.0, rng_seed=0)
    n1 = shuffle_catalysts(z, n_perms=200, rng_seed=77, exclude_around=None)
    n2 = shuffle_catalysts(z, n_perms=200, rng_seed=77, exclude_around=None)
    assert np.allclose(n1, n2)


def test_shuffle_catalysts_null_is_subGaussian_on_background():
    """With Gaussian background and no spike, the null of max|z| over an
    11-day window should sit mostly below 4 sigma."""
    rng = np.random.default_rng(3)
    n = 2000
    vals = rng.standard_normal(n)
    z = pd.Series(vals, index=pd.bdate_range("2018-01-01", periods=n))
    null = shuffle_catalysts(z, n_perms=500, rng_seed=13, exclude_around=None)
    assert float(np.median(null)) < 4.0
    # Very high extremes should be rare in 500 draws.
    assert float(np.max(null)) < 6.0


# --------------------------------------------------------------------------
# percentile_of_observed
# --------------------------------------------------------------------------
def test_percentile_of_observed_monotonic():
    null = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    assert percentile_of_observed(0.5, null) == 0.0
    assert percentile_of_observed(3.0, null) == 60.0
    assert percentile_of_observed(5.0, null) == 100.0
    assert percentile_of_observed(100.0, null) == 100.0


# --------------------------------------------------------------------------
# permutation_test end-to-end
# --------------------------------------------------------------------------
def test_permutation_test_detects_injected_spike():
    """A 10-sigma spike should sit above the 99th percentile of the shuffled
    null even with a modest number of perms."""
    z = _mk_z_series(spike_value=10.0, rng_seed=5)
    # 1500 is our spike position. Use a custom exclude_around=None so the
    # null sampling can pull from anywhere; but note the observed is the
    # INJECTED value, not re-measured from the series itself.
    r = permutation_test(z, observed=10.0, n_perms=1000,
                         halfwidth=WINDOW_HALFWIDTH, rng_seed=5)
    # 10 sigma in a draw of ~1000 centered-11d-max of Gaussians is extreme
    # enough to land at the 99th percentile or higher.
    assert r.percentile >= 99.0, f"percentile was {r.percentile:.2f}"
    assert r.p_value <= 0.01


def test_permutation_test_null_distribution_sane():
    z = _mk_z_series(rng_seed=2, inject_spike=False)
    r = permutation_test(z, observed=2.0, n_perms=500)
    assert r.n_perms > 0
    assert np.isfinite(r.null_p999)
    # Null max is a max-of-11 Gaussians -> 99.9% tail somewhere in (3, 7).
    assert 2.5 < r.null_p999 < 8.0


def test_permutation_test_below_threshold_reports_faithfully():
    """A 2-sigma "observed" is not an extreme draw -> H_1 must NOT be
    supported, and the percentile must land well below 99.9."""
    z = _mk_z_series(rng_seed=4, inject_spike=False)
    r = permutation_test(z, observed=2.0, n_perms=500)
    assert r.percentile < H1_PERCENTILE_THRESHOLD
