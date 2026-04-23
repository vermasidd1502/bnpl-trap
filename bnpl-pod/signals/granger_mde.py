"""
Minimum Detectable Effect (MDE) for the Granger F-test used in section 6.

For the nested-linear-model F-test with k restrictions (the null is that all
k lagged BSI coefficients are zero), with sample size n and unrestricted
regression using 2k + 1 parameters (k own-lags, k BSI-lags, intercept),
the degrees of freedom are df1 = k, df2 = n - 2k - 1.

MDE answers: given alpha = 0.05 and a target statistical power (by default
0.80), what is the smallest incremental R^2 (Delta-R^2) that the test could
reliably reject? Any true effect smaller than that is, by construction, not
distinguishable from the null given the sample.

Numerical method: use scipy.stats.ncf (non-central F) to find the
noncentrality parameter `lambda` at which the power equals the target, then
convert to Cohen's f^2 = lambda / (df1 + df2 + 1), then to Delta-R^2 via
Delta-R^2 = f^2 * (1 - R^2_full). The conversion carries a (modest)
dependence on the R^2 of the unrestricted model; we report at R^2 = 0.15
(the default; a defensible middle ground for Granger regressions at weekly
frequency) and also expose a sensitivity across R^2 in {0.05, 0.10, 0.15,
0.20, 0.30}.

Reference v2 setup (paper section 6):

    n = 399
    k in {4, 5, 6, 7, 8}
    alpha = 0.05
    power = 0.80
    R^2_full = 0.15  (disclosed; see sensitivity table)

Result at R^2_full = 0.15 (headline number in section 6):

    Delta-R^2 ranges from approximately 0.026 (k=4) to 0.033 (k=8).

Interpretation: a true population R^2 increment below ~2.6-3.3% would fall
below the detection threshold of this test at n=399. The joint non-
rejection reported in Tables tab:granger-t2 and tab:granger is therefore
consistent with true incremental predictive content anywhere in
[0, ~3%], not with zero. We disclose this explicitly rather than claim
orthogonality.

Author: Siddharth Verma, UIUC, FIN 580 Spring 2026 cohort.
Provenance: v2 rewrite, 2026-04-22.
"""

from __future__ import annotations

from dataclasses import dataclass

from scipy.stats import ncf, f as f_dist


@dataclass(frozen=True)
class MDEResult:
    lag: int
    df_num: int
    df_den: int
    f_crit: float
    noncentrality: float
    cohens_f2: float
    delta_r2: float


def _find_noncentrality(df_num: int, df_den: int, f_crit: float,
                        power: float) -> float:
    """Bisection: smallest lambda s.t. 1 - NCF.cdf(f_crit) >= power."""
    lo, hi = 0.0, 500.0
    for _ in range(100):
        mid = 0.5 * (lo + hi)
        pw = 1.0 - ncf.cdf(f_crit, df_num, df_den, mid)
        if pw < power:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def mde_for_granger(n: int = 399, lag: int = 6, alpha: float = 0.05,
                    power: float = 0.80, r2_full: float = 0.15) -> MDEResult:
    """Minimum detectable Delta-R^2 for the nested-VAR Granger F-test."""
    df_num = lag
    df_den = n - 2 * lag - 1
    f_crit = float(f_dist.ppf(1.0 - alpha, df_num, df_den))
    lam = _find_noncentrality(df_num, df_den, f_crit, power)
    f2 = lam / (df_num + df_den + 1)
    delta_r2 = f2 * (1.0 - r2_full)
    return MDEResult(lag=lag, df_num=df_num, df_den=df_den, f_crit=f_crit,
                     noncentrality=lam, cohens_f2=f2, delta_r2=delta_r2)


def paper_headline_range(n: int = 399, alpha: float = 0.05,
                         power: float = 0.80, r2_full: float = 0.15,
                         lags: tuple[int, ...] = (4, 5, 6, 7, 8)) -> list[MDEResult]:
    """Reproduce the MDE range disclosed in section 6 of the paper."""
    return [mde_for_granger(n=n, lag=k, alpha=alpha, power=power,
                            r2_full=r2_full) for k in lags]


def sensitivity_table(n: int = 399, alpha: float = 0.05, power: float = 0.80,
                      lags: tuple[int, ...] = (4, 5, 6, 7, 8),
                      r2_grid: tuple[float, ...] = (0.05, 0.10, 0.15, 0.20, 0.30)
                      ) -> dict[int, dict[float, float]]:
    """Return {lag: {r2_full: delta_r2}} sensitivity grid."""
    out: dict[int, dict[float, float]] = {}
    for k in lags:
        out[k] = {r2: mde_for_granger(n=n, lag=k, alpha=alpha, power=power,
                                      r2_full=r2).delta_r2
                  for r2 in r2_grid}
    return out


if __name__ == "__main__":
    print("Granger MDE at paper defaults (n=399, alpha=0.05, power=0.80, "
          "R^2_full=0.15):")
    print(f"{'lag':>4} {'df_num':>7} {'df_den':>7} {'F_crit':>8} "
          f"{'lambda':>8} {'f^2':>8} {'Delta_R2':>10}")
    for r in paper_headline_range():
        print(f"{r.lag:>4} {r.df_num:>7} {r.df_den:>7} {r.f_crit:>8.4f} "
              f"{r.noncentrality:>8.3f} {r.cohens_f2:>8.5f} {r.delta_r2:>10.5f}")
    print()
    print("Sensitivity of Delta-R^2 to R^2_full:")
    grid = sensitivity_table()
    r2_grid = (0.05, 0.10, 0.15, 0.20, 0.30)
    hdr = f"{'lag':>4} " + " ".join(f"R2={x:.2f}".rjust(10) for x in r2_grid)
    print(hdr)
    for k in sorted(grid.keys()):
        row = f"{k:>4} " + " ".join(f"{grid[k][r]:>10.5f}" for r in r2_grid)
        print(row)
