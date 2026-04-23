"""
Permutation null for the 9.6870 sigma Regulation-Z headline (paper §13.X).

Phase B.3 of the non-hallucinating alpha pivot. This module replaces the
round-number 10 sigma Sprint-Q bypass threshold with an empirical percentile
of the null distribution of max|z_bsi| in random 11-day windows.

Pre-registered hypothesis
-------------------------
H_0: the observed max(|z_bsi|) in any 11-day window around 2025-01-17 is
     drawn from the empirical distribution of max(|z_bsi|) in 11-day windows
     centered on random dates in the 2019-2026 period.
H_1: percentile of observed 9.6870 sigma in the shuffled null >= 99.9.
Rejection region: observed percentile >= 99.9.

Outputs
-------
`paper_formal/figures/permutation_null.pdf`  -- histogram of null distribution
                                                with the observed 9.6870 annot.
stdout                                        -- the percentile number
                                                regardless of whether H_1 holds.

Honest-disclosure rule (hard-coded)
-----------------------------------
`run()` prints the percentile regardless of whether H_1 is supported. If the
9.6870 sigma pulse sits below the 99.9th percentile, the paper §13.X text
MUST report that number verbatim --- no "try another window" loop.

Threshold-recalibration follow-on
---------------------------------
IF the observed percentile is >= 99.9, the 99.9-th percentile value of the
null distribution becomes the principled replacement for the Sprint-Q bypass
threshold (currently hard-coded 10.0 in config/thresholds.yaml). This is a
downstream edit and is NOT performed by this module --- it is a conscious
decision by the author after reading the `run()` output.

Author: Siddharth Verma, UIUC, FIN 580 Spring 2026 cohort.
Provenance: Phase B.3 of v2.1 non-hallucinating alpha pivot, 2026-04-23.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Sequence

import duckdb
import numpy as np
import pandas as pd

from data.settings import settings
from signals.bsi import compute_bsi_from_warehouse


# Pre-registered constants
WINDOW_HALFWIDTH: int = 5          # 11-day total window
N_PERMS_DEFAULT: int = 10_000
RNG_SEED: int = 42
OBSERVED_ABS_Z: float = 9.6870     # the headline Sprint-Q bypass candidate
CATALYST_DATE: date = date(2025, 1, 17)
H1_PERCENTILE_THRESHOLD: float = 99.9


# ---------------------------------------------------------------------------
# Sliding-window max absolute z extractor
# ---------------------------------------------------------------------------

def rolling_max_abs_z(
    z_series: pd.Series,
    halfwidth: int = WINDOW_HALFWIDTH,
) -> pd.Series:
    """For each date t, compute max |z_bsi| in the (t - halfwidth, t + halfwidth)
    inclusive window. Returns a same-index series."""
    w = 2 * halfwidth + 1
    abs_z = z_series.abs()
    # centered rolling max
    out = abs_z.rolling(window=w, center=True, min_periods=1).max()
    out.name = "max_abs_z_11d"
    return out


# ---------------------------------------------------------------------------
# Shuffle-catalyst permutation null
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PermutationResult:
    observed: float
    null_distribution: np.ndarray
    percentile: float
    p_value: float          # one-sided tail probability
    null_p999: float        # 99.9th percentile of the null distribution
    n_perms: int


def shuffle_catalysts(
    z_series: pd.Series,
    n_perms: int = N_PERMS_DEFAULT,
    halfwidth: int = WINDOW_HALFWIDTH,
    rng_seed: int = RNG_SEED,
    exclude_around: date | None = CATALYST_DATE,
    exclusion_halfwidth: int = 30,
) -> np.ndarray:
    """Sample `n_perms` random dates from the z_series index, compute the
    max |z| in the (t - halfwidth, t + halfwidth) window around each, return
    the empirical distribution.

    `exclude_around` lets us drop a calendar band around the observed catalyst
    so that the null draws do not include the observed pulse itself. 30-day
    exclusion is pre-registered here.
    """
    rng = np.random.default_rng(rng_seed)
    idx = z_series.index
    # Exclude dates within `exclusion_halfwidth` days of the observed catalyst.
    if exclude_around is not None:
        excl_lo = pd.Timestamp(exclude_around) - pd.Timedelta(days=exclusion_halfwidth)
        excl_hi = pd.Timestamp(exclude_around) + pd.Timedelta(days=exclusion_halfwidth)
        eligible = idx[(idx < excl_lo) | (idx > excl_hi)]
    else:
        eligible = idx

    # Buffer at edges so the window fits.
    eligible = eligible[(eligible >= idx[halfwidth])
                        & (eligible <= idx[-halfwidth - 1])]
    if len(eligible) == 0:
        raise ValueError("no eligible permutation dates; sample too short")

    abs_z = z_series.abs().values
    pos_map = {t: i for i, t in enumerate(idx)}
    nulls = np.empty(n_perms, dtype=float)
    drawn = rng.choice(eligible, size=n_perms, replace=True)
    for j, t in enumerate(drawn):
        i = pos_map[t]
        lo, hi = i - halfwidth, i + halfwidth + 1
        nulls[j] = float(np.nanmax(abs_z[lo:hi]))
    return nulls


def percentile_of_observed(
    observed: float,
    null_dist: np.ndarray,
) -> float:
    """Fraction of the null distribution <= `observed`, expressed as a
    percentile in [0, 100]."""
    arr = null_dist[np.isfinite(null_dist)]
    if arr.size == 0:
        return float("nan")
    return float(100.0 * np.mean(arr <= observed))


def permutation_test(
    z_series: pd.Series,
    observed: float = OBSERVED_ABS_Z,
    n_perms: int = N_PERMS_DEFAULT,
    halfwidth: int = WINDOW_HALFWIDTH,
    rng_seed: int = RNG_SEED,
) -> PermutationResult:
    nulls = shuffle_catalysts(
        z_series, n_perms=n_perms, halfwidth=halfwidth, rng_seed=rng_seed,
    )
    pct = percentile_of_observed(observed, nulls)
    null_clean = nulls[np.isfinite(nulls)]
    p_value = float(np.mean(null_clean >= observed))
    null_p999 = float(np.nanpercentile(null_clean, H1_PERCENTILE_THRESHOLD))
    return PermutationResult(
        observed=float(observed),
        null_distribution=null_clean,
        percentile=pct,
        p_value=p_value,
        null_p999=null_p999,
        n_perms=int(null_clean.size),
    )


# ---------------------------------------------------------------------------
# Output: PDF figure
# ---------------------------------------------------------------------------

def _write_pdf_figure(result: PermutationResult, path: Path) -> None:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    mpl.rcParams.update({
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "font.family": "serif",
        "font.size": 9.5,
        "axes.titlesize": 10.5,
        "axes.labelsize": 9.5,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "grid.linestyle": "--",
        "grid.linewidth": 0.5,
        "legend.frameon": False,
    })

    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    bins = np.linspace(0, max(result.observed * 1.1, float(result.null_distribution.max()) * 1.05),
                       80)
    ax.hist(result.null_distribution, bins=bins, color="#22d3ee", alpha=0.55,
            edgecolor="#0b3d91", linewidth=0.4,
            label=f"null distribution (B={result.n_perms:,})")
    ax.axvline(result.observed, color="#e11d48", linewidth=1.6,
               linestyle="--",
               label=f"observed = {result.observed:.3f}$\\sigma$")
    ax.axvline(result.null_p999, color="#f59e0b", linewidth=1.2,
               linestyle=":",
               label=f"null 99.9 percentile = {result.null_p999:.3f}$\\sigma$")
    ax.set_xlabel(r"max $|z_{\mathrm{BSI}}|$ over 11-day window")
    ax.set_ylabel("count")
    verdict = (
        "$H_1$ SUPPORTED" if result.percentile >= H1_PERCENTILE_THRESHOLD
        else "$H_1$ NOT SUPPORTED"
    )
    ax.set_title(
        f"Shuffle-catalyst permutation null: observed sits at "
        f"percentile {result.percentile:.2f}  ({verdict})"
    )
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------------

def load_z_series(con: duckdb.DuckDBPyConnection | None = None) -> pd.Series:
    """Pull z_bsi from the warehouse via the canonical scorer (NOT from the
    `bsi_daily` table directly, because that table's `z_bsi` column was
    computed under the v1 180-day rolling sigma; the canonical scorer uses
    the paper's Equation (1) EWMA sigma).
    """
    out = compute_bsi_from_warehouse(conn=con)
    s = out["z_bsi"].dropna()
    s.name = "z_bsi"
    return s


def run(
    out_dir: Path | None = None,
    n_perms: int = N_PERMS_DEFAULT,
    observed: float = OBSERVED_ABS_Z,
) -> PermutationResult:
    """CLI entry: compute, write PDF, print percentile regardless of outcome."""
    if out_dir is None:
        out_dir = Path(__file__).resolve().parent.parent / "paper_formal" / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)

    z = load_z_series()
    result = permutation_test(z, observed=observed, n_perms=n_perms)
    _write_pdf_figure(result, out_dir / "permutation_null.pdf")

    print("permutation null for the Reg-Z sigma headline")
    print(f"  observed |z_bsi|      = {result.observed:.4f} sigma")
    print(f"  B (perms completed)    = {result.n_perms:,}")
    print(f"  null median            = {np.median(result.null_distribution):.4f}")
    print(f"  null 99.0 pct          = {np.percentile(result.null_distribution, 99.0):.4f}")
    print(f"  null 99.9 pct          = {result.null_p999:.4f}")
    print(f"  percentile of observed = {result.percentile:.3f}")
    print(f"  one-sided p-value      = {result.p_value:.5f}")
    if result.percentile >= H1_PERCENTILE_THRESHOLD:
        print(f"  VERDICT: pre-registered H_1 SUPPORTED "
              f"(percentile >= {H1_PERCENTILE_THRESHOLD}).")
        print(f"  Principled bypass threshold = {result.null_p999:.4f} sigma "
              f"(99.9-th pct of null).")
    else:
        print(f"  VERDICT: pre-registered H_1 NOT SUPPORTED. "
              f"Paper must report percentile={result.percentile:.3f} verbatim.")
    return result


if __name__ == "__main__":
    run()
