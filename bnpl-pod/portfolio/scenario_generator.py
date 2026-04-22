"""
Vectorized Monte-Carlo scenario generator for the Mean-CVaR LP.

Produces a loss matrix L ∈ R^{T × n} where rows are scenarios and columns are
issuers. L[s, i] is the tranche-loss fraction realized on issuer i under
scenario s over the planning horizon (default 252 days).

Design
------
1) SYSTEMIC PATH CACHING — the Λ_sys(t) CIR draws are shared across all
   issuers in a given scenario (by the two-factor decomposition λ_i = Λ_sys +
   λ_unsys,i). We simulate Λ_sys ONCE per regime and re-use it for every
   issuer, so the marginal cost of an extra issuer is only one idiosyncratic
   CIR draw — not a fresh systemic draw.

2) VECTORIZED CIR — full-truncation Euler scheme in pure NumPy. No Python
   loop over paths and no Python loop over issuers. There is one explicit
   Python loop over TIME STEPS because CIR is a Markov recursion — this is
   unavoidable and intrinsic. Existing `quant.jarrow_turnbull.simulate_cir`
   already takes this form; we keep the same contract.

3) STRESS BLEND — scenarios split into two tranches:
       (1 - stress_blend_weight) fraction drawn from the issuer's current
       calibrated regime  (reads jt_lambda.kappa/theta/sigma)
       stress_blend_weight fraction drawn from `quant.crisis_transport.transport()`
       at the chosen (phi_theta, phi_kappa) point — default (1.5, 8.0), the
       centre of the 3×3 sensitivity grid.

Speed budget
------------
For T=2000 paths, H=252, n_issuers=5:
  1 systemic CIR sim : (2000, 253) float  -> ~8 ms
  5 idiosyn. CIR sim : 5 * (2000, 253)    -> ~40 ms
  Loss evaluation    : vector ops          -> ~2 ms
Total ~50 ms per regime → ~100 ms for blended (baseline + stressed). Well
inside the 40s test budget.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np

from data.settings import load_thresholds
from quant import crisis_transport as ct
from quant import jarrow_turnbull as jt

log = logging.getLogger(__name__)

# Defaults — shared with JT. Kept here so callers can override without mutating
# the pricer module globals.
DEFAULT_LGD = 0.55
DEFAULT_SYS_PARAMS = jt.CIRParams(kappa=0.5, theta=0.01, sigma=0.08)


@dataclass(frozen=True)
class IssuerSpec:
    """Per-issuer Monte-Carlo inputs. One row per approved TRS signal."""
    issuer: str
    kappa: float
    theta: float
    sigma: float
    lambda_0: float
    # Tranche geometry — junior-ABS defaults, overridable per-issuer.
    attach: float = 0.00
    detach: float = 0.05
    ttm_days: int = 252


def _vectorized_cir(kappa: float, theta: float, sigma: float,
                    lambda_0: float, horizon_days: int, n_paths: int,
                    rng: np.random.Generator,
                    dt_days: float = 1.0) -> np.ndarray:
    """Fully vectorized full-truncation Euler (Lord et al.).

    Returns (n_paths, n_steps+1). The time-axis loop is intrinsic to the CIR
    Markov recursion; there is no loop across paths.
    """
    # Enforce Feller the same way jt.CIRParams does — critical for stability
    # once kappa/theta come from calibration and may be close to the boundary.
    limit = math.sqrt(max(0.0, 2.0 * kappa * theta))
    sigma_safe = min(sigma, 0.999 * limit) if limit > 0 else sigma
    sigma_safe = max(sigma_safe, 1e-6)

    n_steps = int(horizon_days / dt_days)
    dt = dt_days / 252.0
    sqrt_dt = math.sqrt(dt)

    paths = np.empty((n_paths, n_steps + 1), dtype=float)
    paths[:, 0] = lambda_0
    # Pre-draw ALL Gaussians at once — vectorized and deterministic under rng.
    z = rng.standard_normal(size=(n_paths, n_steps))
    for t in range(n_steps):
        x = np.maximum(paths[:, t], 0.0)
        paths[:, t + 1] = (
            x
            + kappa * (theta - x) * dt
            + sigma_safe * np.sqrt(x) * sqrt_dt * z[:, t]
        )
    return np.maximum(paths, 0.0)


def _tranche_loss_from_lambda(lambda_path: np.ndarray,
                               attach: float, detach: float,
                               lgd: float = DEFAULT_LGD) -> np.ndarray:
    """Vectorized tranche-loss fraction per scenario. Returns (n_paths,)."""
    S = jt.survival_probability(lambda_path)
    pool_loss = (1.0 - S) * lgd
    width = detach - attach
    return np.clip(pool_loss - attach, 0.0, width) / width


def generate_loss_matrix(
    issuers: list[IssuerSpec],
    *,
    n_scenarios: Optional[int] = None,
    stress_blend_weight: Optional[float] = None,
    horizon_days: Optional[int] = None,
    phi_theta: float = 1.5,
    phi_kappa: float = 8.0,
    sys_params: Optional[jt.CIRParams] = None,
    seed: int = 42,
) -> np.ndarray:
    """Return L ∈ R^{T × n_issuers} of tranche-loss fractions.

    The systemic λ_sys path is simulated ONCE per regime and added to every
    issuer's idiosyncratic path — this encodes the common shock the v4.1
    two-factor decomposition was designed for and saves ~n_issuers× compute.
    """
    th = load_thresholds()["portfolio"]
    T = int(n_scenarios if n_scenarios is not None else th["n_scenarios"])
    blend = float(stress_blend_weight if stress_blend_weight is not None
                  else th["stress_blend_weight"])
    H = int(horizon_days if horizon_days is not None else th["horizon_days"])
    n = len(issuers)
    assert 0 <= blend <= 1.0, "stress_blend_weight must be in [0, 1]"
    assert n > 0, "at least one issuer required"

    n_stressed = int(round(T * blend))
    n_baseline = T - n_stressed

    rng = np.random.default_rng(seed)
    sys_p = (sys_params or DEFAULT_SYS_PARAMS).enforce_feller()

    # --- Cached systemic paths -------------------------------------------
    sys_base = (
        _vectorized_cir(sys_p.kappa, sys_p.theta, sys_p.sigma,
                        lambda_0=sys_p.theta,
                        horizon_days=H, n_paths=n_baseline, rng=rng)
        if n_baseline else np.empty((0, H + 1))
    )
    if n_stressed:
        stressed_regime = ct.transport(phi_theta, phi_kappa)
        sp = stressed_regime.params
        sys_stress = _vectorized_cir(
            sp.kappa, sp.theta, sp.sigma,
            lambda_0=sp.theta,
            horizon_days=H, n_paths=n_stressed, rng=rng,
        )
    else:
        sys_stress = np.empty((0, H + 1))

    # --- Per-issuer idiosyncratic + loss column ---------------------------
    L = np.empty((T, n), dtype=float)
    for i, spec in enumerate(issuers):
        # Idiosyncratic CIR draws — independent per issuer. `rng` is advanced
        # deterministically so re-runs are reproducible.
        idio_base = (
            _vectorized_cir(spec.kappa, spec.theta, spec.sigma,
                            lambda_0=spec.lambda_0,
                            horizon_days=H, n_paths=n_baseline, rng=rng)
            if n_baseline else np.empty((0, H + 1))
        )
        idio_stress = (
            _vectorized_cir(spec.kappa, spec.theta, spec.sigma,
                            lambda_0=spec.lambda_0,
                            horizon_days=H, n_paths=n_stressed, rng=rng)
            if n_stressed else np.empty((0, H + 1))
        )
        # λ_total = Λ_sys + λ_unsys, cap at J_MAX — all vectorized.
        lam_base = np.minimum(sys_base + idio_base, jt.J_MAX) if n_baseline else np.empty((0, H + 1))
        lam_stress = np.minimum(sys_stress + idio_stress, jt.J_MAX) if n_stressed else np.empty((0, H + 1))

        loss_base = (_tranche_loss_from_lambda(lam_base, spec.attach, spec.detach)
                     if n_baseline else np.empty((0,)))
        loss_stress = (_tranche_loss_from_lambda(lam_stress, spec.attach, spec.detach)
                       if n_stressed else np.empty((0,)))

        L[:n_baseline, i] = loss_base
        L[n_baseline:, i] = loss_stress

    log.info(
        "scenarios | T=%d (base=%d, stress=%d) | n_issuers=%d | H=%d | "
        "stress (φθ=%.2f, φκ=%.2f)",
        T, n_baseline, n_stressed, n, H, phi_theta, phi_kappa,
    )
    return L


def baseline_loss_mean(L: np.ndarray) -> np.ndarray:
    """Column-wise mean of L — informal sanity check. Returns shape (n,)."""
    return L.mean(axis=0)
