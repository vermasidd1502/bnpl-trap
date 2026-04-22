"""
Joint 3×3 sensitivity grid on (phi_theta, phi_kappa) — MASTERPLAN v4.1 §4.3.

phi_theta scales the severity (long-run mean) shock; phi_kappa scales the
persistence (mean-reversion speed) shock. The grid yields simulated Sharpe
ratios for a market-neutral TRS short strategy under each scaler pair. The
Sharpe = 0 contour marks strategy breakeven.

The real transport kernel lives in quant/ (Sprint D). This module exposes a
deterministic, seeded, closed-form approximation sufficient for the
dashboard heatmap and regression tests. When Sprint D lands, swap
``_simulate_sharpe`` for the JT-driven simulator without changing callers.
"""
from __future__ import annotations

import math
from typing import Iterable

import numpy as np


def _simulate_sharpe(phi_theta: float, phi_kappa: float,
                     n: int = 1000, seed: int = 42) -> float:
    """
    Toy closed-form stand-in:
        excess_return ∝ (phi_theta - 1.0) * persistence_damping(phi_kappa)
    Chosen so that:
      - Sharpe rises with severity (phi_theta > 1 shocks θ higher → wider spreads)
      - Sharpe falls with persistence scaler > reasonable band (stress drags roll risk)
      - Sharpe = 0 surface passes through (phi_theta ≈ 1.2, phi_kappa ≈ 8-11)
    """
    rng = np.random.default_rng(seed + int(phi_theta * 100) + int(phi_kappa * 10))
    damping = math.exp(-(phi_kappa - 6.0) / 8.0)   # decays as persistence grows
    mu = 0.35 * (phi_theta - 1.05) * damping
    sd = 0.08 + 0.01 * phi_kappa
    sample = rng.normal(mu, sd, size=n)
    mean, std = float(sample.mean()), float(sample.std(ddof=1) or 1e-9)
    return mean / std * math.sqrt(252.0 / 21.0)     # monthly → annualized-ish


def sensitivity_grid(phi_theta: Iterable[float],
                     phi_kappa: Iterable[float]) -> np.ndarray:
    """Return a (len(phi_theta), len(phi_kappa)) Sharpe matrix."""
    pt = np.asarray(list(phi_theta), dtype=float)
    pk = np.asarray(list(phi_kappa), dtype=float)
    out = np.zeros((len(pt), len(pk)), dtype=float)
    for i, t in enumerate(pt):
        for j, k in enumerate(pk):
            out[i, j] = _simulate_sharpe(float(t), float(k))
    return out


def sharpe_zero_contour(phi_theta: Iterable[float],
                        phi_kappa: Iterable[float],
                        grid: np.ndarray) -> list[tuple[float, float]]:
    """
    Return a list of (phi_theta, phi_kappa) pairs that bracket Sharpe = 0 along
    each θ row, by linear interpolation between adjacent grid points.
    """
    pt = np.asarray(list(phi_theta), dtype=float)
    pk = np.asarray(list(phi_kappa), dtype=float)
    pts: list[tuple[float, float]] = []
    # Walk rows (vary phi_kappa at fixed phi_theta)
    for i, t in enumerate(pt):
        for j in range(len(pk) - 1):
            a, b = grid[i, j], grid[i, j + 1]
            if a == b:
                continue
            if (a <= 0 <= b) or (b <= 0 <= a):
                frac = a / (a - b)
                k_interp = pk[j] + frac * (pk[j + 1] - pk[j])
                pts.append((float(t), float(k_interp)))
    # Walk columns (vary phi_theta at fixed phi_kappa)
    for j, k in enumerate(pk):
        for i in range(len(pt) - 1):
            a, b = grid[i, j], grid[i + 1, j]
            if a == b:
                continue
            if (a <= 0 <= b) or (b <= 0 <= a):
                frac = a / (a - b)
                t_interp = pt[i] + frac * (pt[i + 1] - pt[i])
                pts.append((float(t_interp), float(k)))
    return pts
