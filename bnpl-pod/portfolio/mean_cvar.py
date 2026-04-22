"""
Mean-CVaR portfolio optimizer — Rockafellar-Uryasev Linear Program.

MASTERPLAN v4.1 §8. Sprint F.

Sign convention
---------------
A TRS SHORT position pays floating SOFR (carry) and RECEIVES the realized
tranche-loss fraction. Per unit notional `|w_i|`, the scenario P&L is

    return_s,i  =  L_{s,i}  -  carry_i
    loss_s,i    = -return_s,i  =  carry_i  -  L_{s,i}

With a notional (non-negative) variable `abs_w`, scenario book loss is

    book_loss_s =  sum_i ( carry_i - L_{s,i} ) * abs_w_i
                 =  carry^T @ abs_w  -  L_s @ abs_w           (T x n matrix L)

Expected return of the book is

    E[book_return] = sum_i mu_i * abs_w_i     where  mu_i = E[L_{·,i}] - carry_i

`mu` is already net of carry at the call site (see `portfolio/book.py` where it
is built as `baseline_loss - SOFR_carry`), so the optimizer does NOT subtract
carry from mu again — it only needs carry for the scenario CVaR term.

Formulation
-----------
Decision variables:
    abs_w in R^n      >= 0, notional per leg (signed weight w_i = -abs_w_i)
    alpha in R        R-U auxiliary VaR at (1 - cvar_alpha) tail
    u in R^T          >= 0, R-U tail slack

R-U CVaR identity:
    CVaR_{1-q}(X)  =  min_alpha  alpha + (1/((1-q)T)) * sum max(X_s - alpha, 0)

So CVaR of book_loss is the LP expression:
    alpha + (1/((1-q)T)) * sum(u),     u_s >= book_loss_s - alpha,  u_s >= 0

Joint objective:
    maximize  mu^T @ abs_w  -  gamma * ( alpha + (1/((1-q)T)) sum(u) )

Constraints:
    sum(abs_w) <= max_gross_leverage
    abs_w_i    <= max_single_weight
    abs_w      >= 0

gamma = 5.0 override (locked in config/thresholds.yaml §portfolio)
-----------------------------------------------------------------
Per v4.1 §8.2, we deliberately set gamma above institutional default (~2.0)
to force the optimizer to respect the BNPL debt-stacking tail rather than
chase near-term spread.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Union

import cvxpy as cp
import numpy as np

log = logging.getLogger(__name__)

# CLARABEL: fast + robust for LP/QP; HIGHS: pure-LP; SCS: universal fallback.
_SOLVER_PREFERENCE = ("CLARABEL", "HIGHS", "SCS")

CarryLike = Union[float, np.ndarray]


@dataclass
class CVaRSolution:
    """Solver output + diagnostics.

    `weights` carries the SIGNED weight returned to the caller (negative for
    TRS short legs). Internal `abs_w` is stored for audit.
    """
    weights: np.ndarray            # (n,) signed; TRS short -> negative
    abs_notional: np.ndarray       # (n,) non-negative |w|
    alpha_var: float
    cvar_value: float              # CVaR of book_loss at (1 - cvar_alpha) tail
    cvar_contributions: np.ndarray # (n,)
    expected_return: float         # mu @ abs_w
    status: str


def _pick_solver() -> str:
    installed = set(cp.installed_solvers())
    for s in _SOLVER_PREFERENCE:
        if s in installed:
            return s
    return next(iter(installed))


def solve(
    mu: np.ndarray,
    L: np.ndarray,
    *,
    carry: CarryLike = 0.0,
    gamma: float = 5.0,
    cvar_alpha: float = 0.95,
    max_gross_leverage: float = 3.0,
    max_single_weight: float = 0.25,
    short_only: bool = True,
    solver: Optional[str] = None,
) -> CVaRSolution:
    """Solve the Mean-CVaR LP for a TRS-short book.

    Parameters
    ----------
    mu : shape (n,) expected NET return per unit notional. Already includes
         the SOFR carry deduction (built in `portfolio/book.py`).
    L  : shape (T, n) scenario GROSS tranche-loss fractions (in [0, 1]).
    carry : scalar or shape (n,) SOFR cost per unit notional over the horizon.
            Used ONLY inside the CVaR term so scenario-level loss accounts for
            the carry floor. Should match the value already subtracted from mu.
    gamma : risk-aversion penalty on CVaR. Default 5.0 (v4.1 §8.2).
    cvar_alpha : CVaR confidence level (0.95 → worst 5%).
    max_gross_leverage : sum |w_i| cap.
    max_single_weight : per-leg |w_i| cap.
    short_only : if True, all w_i <= 0 (TRS short convention). If False, the
                 returned weights equal +abs_w — currently unused but kept for
                 forward-compat with long TRS-receive-protection legs.
    """
    mu = np.asarray(mu, dtype=float).ravel()
    L = np.asarray(L, dtype=float)
    T, n = L.shape
    assert mu.shape == (n,), f"mu shape {mu.shape} vs n_cols {n}"
    assert 0.0 < cvar_alpha < 1.0
    assert gamma >= 0.0

    # Broadcast carry to (n,)
    carry_vec = np.full(n, float(carry), dtype=float) \
        if np.isscalar(carry) else np.asarray(carry, dtype=float).ravel()
    assert carry_vec.shape == (n,), f"carry shape {carry_vec.shape} vs n {n}"

    # --- Variables --------------------------------------------------------
    abs_w = cp.Variable(n, nonneg=True, name="abs_w")
    alpha = cp.Variable(name="alpha_var")
    u = cp.Variable(T, nonneg=True, name="u")

    # --- Objective --------------------------------------------------------
    # Per-scenario book loss (positive = bad for the short):
    #   loss_s = carry^T @ abs_w  -  L_s @ abs_w
    scenario_loss = carry_vec @ abs_w - L @ abs_w            # (T,)
    cvar_expr = alpha + (1.0 / ((1.0 - cvar_alpha) * T)) * cp.sum(u)
    expected_ret = mu @ abs_w

    objective = cp.Maximize(expected_ret - gamma * cvar_expr)

    # --- Constraints ------------------------------------------------------
    cons = [
        u >= scenario_loss - alpha,
        cp.sum(abs_w) <= max_gross_leverage,
        abs_w <= max_single_weight,
    ]

    prob = cp.Problem(objective, cons)
    chosen = solver or _pick_solver()
    try:
        prob.solve(solver=chosen)
    except Exception as e:   # noqa: BLE001
        log.warning("solver %s failed (%s); retrying with SCS", chosen, e)
        prob.solve(solver="SCS")

    if abs_w.value is None:
        raise RuntimeError(f"CVaR LP returned no solution; status={prob.status}")

    abs_w_val = np.maximum(np.asarray(abs_w.value).ravel(), 0.0)
    weights = -abs_w_val if short_only else abs_w_val
    alpha_val = float(alpha.value) if alpha.value is not None else 0.0
    u_val = np.asarray(u.value).ravel() if u.value is not None else np.zeros(T)
    cvar_val = float(alpha_val + u_val.sum() / ((1.0 - cvar_alpha) * T))

    # Marginal CVaR contributions — take the tail scenarios (u > eps) and
    # decompose the mean loss across legs. Rescaled so sum ≈ cvar_val.
    eps = 1e-9
    tail = u_val > eps
    if tail.any():
        per_leg_loss = (carry_vec - L[tail, :]).mean(axis=0) * abs_w_val
        total = per_leg_loss.sum()
        contribs = (per_leg_loss * (cvar_val / total)
                    if abs(total) > eps else per_leg_loss)
    else:
        contribs = np.zeros(n, dtype=float)

    log.info(
        "cvar-lp | status=%s gamma=%.2f alpha=%.2f | E[r]=%.5f CVaR=%.5f "
        "|w|_1=%.3f",
        prob.status, gamma, cvar_alpha,
        float(mu @ abs_w_val), cvar_val, float(abs_w_val.sum()),
    )
    return CVaRSolution(
        weights=weights,
        abs_notional=abs_w_val,
        alpha_var=alpha_val,
        cvar_value=cvar_val,
        cvar_contributions=contribs,
        expected_return=float(mu @ abs_w_val),
        status=str(prob.status),
    )
