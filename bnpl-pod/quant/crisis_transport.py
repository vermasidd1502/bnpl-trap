"""
Crisis-regime transport from 2005–2010 subprime auto to 2024–2026 BNPL.

MASTERPLAN v4.1 §4.3 — the duration scaler pair (phi_theta, phi_kappa) maps
the calibrated "bad-regime" CIR parameters from the 2008 auto-ABS crisis
onto the forward-looking BNPL simulator:

    theta_bnpl_bad  = phi_theta  * theta_sys_bad_auto
    kappa_bnpl_bad  = kappa_sys_bad_auto / phi_kappa     (longer half-life)

The JT pricer (quant.jarrow_turnbull) then consumes these params to simulate
Lambda_sys under stress, run Monte-Carlo tranche pricing, and compute the
strategy Sharpe ratio. The joint 3×3 grid on (phi_theta, phi_kappa) feeds
the dashboard heatmap and the paper's §7 #9 stress-test chart.

This module REPLACES the closed-form stand-in in signals/sensitivity.py with
a JT-driven simulator. signals.sensitivity retains its public API; callers
that import `sensitivity_grid` keep working unchanged.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Iterable

import numpy as np

from quant import jarrow_turnbull as jt

log = logging.getLogger(__name__)

# Baseline 2005–2010 calibrated systemic CIR (set conservatively for dashboard
# until auto_abs_historical has enough parsed metrics — swap these when the
# 2008 calibration is final).
AUTO_BAD_KAPPA = 0.4
AUTO_BAD_THETA = 0.035
AUTO_BAD_SIGMA = 0.09


@dataclass
class TransportedRegime:
    phi_theta: float
    phi_kappa: float
    params: jt.CIRParams

    @property
    def stressed_theta(self) -> float:
        return self.params.theta

    @property
    def stressed_kappa(self) -> float:
        return self.params.kappa


def transport(phi_theta: float, phi_kappa: float,
              base_kappa: float = AUTO_BAD_KAPPA,
              base_theta: float = AUTO_BAD_THETA,
              base_sigma: float = AUTO_BAD_SIGMA) -> TransportedRegime:
    """Apply (phi_theta, phi_kappa) duration scalers to the auto-ABS base."""
    stressed = jt.CIRParams(
        kappa=base_kappa / max(phi_kappa, 1e-3),   # higher phi_kappa → slower reversion
        theta=base_theta * phi_theta,              # higher phi_theta → worse long-run
        sigma=base_sigma,
    ).enforce_feller()
    return TransportedRegime(phi_theta=phi_theta, phi_kappa=phi_kappa, params=stressed)


def strategy_sharpe(phi_theta: float, phi_kappa: float,
                    horizon_days: int = 252,
                    n_paths: int = 800,
                    spread_bps: float = 700.0,
                    attach: float = 0.00, detach: float = 0.05,
                    seed: int = 42) -> float:
    """
    Simulate the TRS-short P&L under the transported crisis regime and return
    an annualized Sharpe. We short the junior tranche, so P&L = -ΔPV ≈ tranche
    loss realized over the horizon (positive when defaults exceed the priced-in
    expectation).

    This is a one-factor simulation — enough for dashboard heatmap and for the
    paper's §7 #9 chart. Sprint G replaces it with full waterfall backtest.
    """
    regime = transport(phi_theta, phi_kappa)
    lambda_0 = regime.stressed_theta
    paths = jt.simulate_cir(regime.params, lambda_0=lambda_0,
                            horizon_days=horizon_days,
                            n_paths=n_paths, seed=seed)
    S = jt.survival_probability(paths)
    pool_loss = (1.0 - S) * 0.55                   # LGD 55%
    tranche_loss = np.clip(pool_loss - attach, 0.0, detach - attach) / (detach - attach)

    # TRS short P&L per unit notional per year:
    #   receive floating (spread) * survival share, pay realized tranche loss.
    ttm_yrs = horizon_days / 252.0
    short_pnl = tranche_loss - (spread_bps * 1e-4) * ttm_yrs * (1.0 - tranche_loss)
    mean = float(short_pnl.mean())
    std  = float(short_pnl.std(ddof=1) or 1e-9)
    # Annualize
    ann_factor = math.sqrt(252.0 / horizon_days)
    return (mean / std) * ann_factor


def sensitivity_grid(phi_theta: Iterable[float],
                     phi_kappa: Iterable[float],
                     horizon_days: int = 252,
                     n_paths: int = 800) -> np.ndarray:
    """JT-driven replacement for signals.sensitivity.sensitivity_grid."""
    pt = np.asarray(list(phi_theta), dtype=float)
    pk = np.asarray(list(phi_kappa), dtype=float)
    out = np.zeros((len(pt), len(pk)), dtype=float)
    for i, t in enumerate(pt):
        for j, k in enumerate(pk):
            out[i, j] = strategy_sharpe(float(t), float(k),
                                        horizon_days=horizon_days,
                                        n_paths=n_paths)
    return out


# Re-export the contour finder from signals.sensitivity so downstream imports
# have one canonical location.
from signals.sensitivity import sharpe_zero_contour  # noqa: E402,F401


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    pt = [1.2, 1.5, 1.8]
    pk = [5.0, 8.0, 11.0]
    grid = sensitivity_grid(pt, pk, n_paths=400)
    print("Joint 3x3 sensitivity (rows=phi_theta, cols=phi_kappa):")
    print(grid)
