"""
Jarrow-Turnbull reduced-form ABS tranche pricer with two-factor CIR hazard.

MASTERPLAN v4.1 §4 + §7.

Hazard decomposition
--------------------
    lambda_i(t) = Lambda_sys(t) + lambda_unsys_i(t),     capped at J_max = 5%

Both factors follow CIR dynamics:

    d lambda = kappa (theta - lambda) dt + sigma sqrt(lambda) dW,
    Feller:  2 kappa theta >= sigma^2        (enforced at construction)

lambda_unsys is driven contemporaneously by BSI + MOVE via an affine link so
calibrating on (BSI_t, roll_rate_60p_{t+k}) ties the sentiment signal to a
realized hazard surface.

Pricing
-------
Junior-tranche fair value is a discounted expectation over the pool loss
distribution. We use the standard one-factor copula approximation with
Monte-Carlo survival-time simulation:

    S_i(T) = exp(-∫_0^T lambda_i(u) du)
    L(T)   = Σ_i LGD_i * 1{tau_i <= T}
    PV     = Σ_cash-flow DF(t) * expected waterfall payment given L(t)

For Sprint D the pricer supports the two quantities the paper actually cites
in §7: **survival probability** and **expected tranche loss**. Full waterfall
goes to Sprint G (backtest P&L).

EWMA smoothing (v4.1 §4.1)
--------------------------
Raw lambda_unsys_i from affine link gets EWMA-smoothed with halflife=5d to
stabilize the sim; the cap is applied AFTER smoothing.

Run with:  python -m quant.jarrow_turnbull
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

import duckdb

from data.settings import settings

log = logging.getLogger(__name__)

J_MAX = 0.05           # 5% total-hazard cap
EWMA_HALFLIFE = 5.0    # days


# --- CIR parameter object -------------------------------------------------
@dataclass
class CIRParams:
    kappa: float
    theta: float
    sigma: float

    def feller_ok(self) -> bool:
        return 2.0 * self.kappa * self.theta >= self.sigma ** 2 - 1e-12

    def enforce_feller(self) -> "CIRParams":
        """Clip sigma so the Feller condition holds strictly."""
        limit = math.sqrt(max(0.0, 2.0 * self.kappa * self.theta))
        sigma_ok = min(self.sigma, limit * 0.999)
        return CIRParams(self.kappa, self.theta, max(sigma_ok, 1e-6))


# --- Affine link: BSI/MOVE -> lambda_unsys --------------------------------
def affine_hazard(bsi: float, move: float | None,
                  alpha: float = 0.008,
                  beta_bsi: float = 0.004,
                  beta_move: float = 0.00015) -> float:
    """
    Baseline link. alpha is the idiosyncratic floor, beta_bsi captures
    sentiment → hazard sensitivity, beta_move captures macro-rates-vol drag.
    Returns lambda_unsys BEFORE capping and EWMA smoothing.
    """
    h = alpha + beta_bsi * max(bsi, 0.0)
    if move is not None:
        h += beta_move * max(float(move) - 80.0, 0.0)   # only above 80 bps
    return max(h, 0.0)


def ewma(series: list[float], halflife: float = EWMA_HALFLIFE) -> list[float]:
    if not series:
        return []
    alpha = 1.0 - math.exp(-math.log(2.0) / max(halflife, 1e-9))
    out = [series[0]]
    for x in series[1:]:
        out.append(alpha * x + (1.0 - alpha) * out[-1])
    return out


def apply_cap(series: Iterable[float], cap: float = J_MAX) -> list[float]:
    return [min(max(x, 0.0), cap) for x in series]


# --- CIR simulation -------------------------------------------------------
def simulate_cir(params: CIRParams, lambda_0: float, horizon_days: int,
                 n_paths: int = 2000, dt_days: float = 1.0,
                 seed: int = 42) -> "np.ndarray":
    """Full-truncation Euler scheme for CIR (Lord et al.), returns (n_paths, n_steps+1)."""
    import numpy as np
    params = params.enforce_feller()
    rng = np.random.default_rng(seed)
    n_steps = int(horizon_days / dt_days)
    dt = dt_days / 252.0    # annualize
    paths = np.empty((n_paths, n_steps + 1), dtype=float)
    paths[:, 0] = lambda_0
    sqrt_dt = math.sqrt(dt)
    for t in range(n_steps):
        x = np.maximum(paths[:, t], 0.0)
        z = rng.standard_normal(n_paths)
        paths[:, t + 1] = (
            x + params.kappa * (params.theta - x) * dt
              + params.sigma * np.sqrt(x) * sqrt_dt * z
        )
    return np.maximum(paths, 0.0)


def survival_probability(lambda_path: "np.ndarray", dt_days: float = 1.0) -> "np.ndarray":
    """Path-wise S(T) = exp(-∫ lambda du). Integrates with trapezoidal rule."""
    import numpy as np
    dt = dt_days / 252.0
    # trapezoid
    integral = 0.5 * dt * (lambda_path[:, 0] + lambda_path[:, -1]) \
               + dt * lambda_path[:, 1:-1].sum(axis=1)
    return np.exp(-integral)


# --- Tranche pricing ------------------------------------------------------
def price_tranche(notional: float,
                  attach: float, detach: float,
                  spread_bps: float,
                  ttm_days: int,
                  lambda_total_path: "np.ndarray",
                  lgd: float = 0.55,
                  discount_rate: float = 0.045) -> dict:
    """
    One-factor copula-free approximation: assume issuer pool homogeneity so the
    cumulative loss ratio at horizon is (1 - S(T)) * LGD. Tranche loss is the
    excess above the attachment point clipped at (detach - attach).

    Returns: dict(survival_mean, pool_loss_mean, tranche_loss_mean, fair_value)
    """
    import numpy as np
    assert 0.0 <= attach < detach <= 1.0
    S = survival_probability(lambda_total_path)      # (n_paths,)
    pool_loss = (1.0 - S) * lgd                      # fraction of pool lost
    tranche_width = detach - attach
    tranche_loss_frac = np.clip(pool_loss - attach, 0.0, tranche_width) / tranche_width
    exp_tranche_loss = float(tranche_loss_frac.mean())

    ttm_yrs = ttm_days / 252.0
    df = math.exp(-discount_rate * ttm_yrs)
    # Fair value = PV(coupon leg) - PV(loss leg). Simple quarterly accrual.
    coupon_leg = (spread_bps * 1e-4) * ttm_yrs * (1.0 - exp_tranche_loss) * df
    loss_leg   = exp_tranche_loss * df
    fair_value = notional * tranche_width * (coupon_leg - loss_leg)
    return {
        "survival_mean":      float(S.mean()),
        "pool_loss_mean":     float(pool_loss.mean()),
        "tranche_loss_mean":  exp_tranche_loss,
        "fair_value":         fair_value,
    }


# --- Warehouse write ------------------------------------------------------
def write_lambda_path(issuer: str, dates: list[date],
                      lambda_sys: list[float], lambda_unsys: list[float],
                      params: CIRParams) -> int:
    lambda_total = apply_cap([s + u for s, u in zip(lambda_sys, lambda_unsys)])
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        con.executemany(
            """
            INSERT OR REPLACE INTO jt_lambda
                (issuer, observed_at, lambda_sys, lambda_unsys, lambda_total,
                 kappa, theta, sigma, j_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [(issuer, d, s, u, t, params.kappa, params.theta, params.sigma, J_MAX)
             for d, s, u, t in zip(dates, lambda_sys, lambda_unsys, lambda_total)],
        )
        return len(dates)
    finally:
        con.close()


def build_issuer_hazard(issuer: str,
                        start: date | None = None,
                        end: date | None = None,
                        sys_params: CIRParams | None = None) -> int:
    """Pull BSI + MOVE, compute lambda_unsys via affine link, EWMA-smooth, cap, write."""
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        if end is None:
            end = date.today()
        if start is None:
            start = end - timedelta(days=365 * 3)
        rows = con.execute(
            """
            SELECT b.observed_at, b.bsi, m.value AS move
            FROM bsi_daily b
            LEFT JOIN fred_series m
              ON m.observed_at = b.observed_at AND m.series_id = 'MOVE'
            WHERE b.observed_at BETWEEN ? AND ?
            ORDER BY b.observed_at
            """,
            [start, end],
        ).fetchall()
    finally:
        con.close()
    if not rows:
        log.warning("jt | %s | no BSI rows", issuer)
        return 0

    dates    = [r[0] for r in rows]
    raw_h    = [affine_hazard(r[1] or 0.0, r[2]) for r in rows]
    smoothed = ewma(raw_h)
    # Cap AFTER smoothing.
    capped   = apply_cap(smoothed, J_MAX)

    # Systemic component: use a constant baseline (sys_params.theta) until
    # crisis_transport.py supplies a time-varying Lambda_sys path. Safe default.
    params = sys_params or CIRParams(kappa=0.5, theta=0.01, sigma=0.08).enforce_feller()
    lambda_sys = [params.theta] * len(dates)

    return write_lambda_path(issuer, dates, lambda_sys, capped, params)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    for iss in ("AFRM", "SQ", "PYPL", "SEZL", "UPST"):
        n = build_issuer_hazard(iss)
        print(f"  {iss:6s} {n:>5d} hazard rows")
