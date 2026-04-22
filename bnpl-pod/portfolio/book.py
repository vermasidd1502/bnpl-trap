"""
Portfolio book builder.

Pipeline
--------
    pod_decisions (approved, TRS-only)
          |
          +--> per-issuer latest jt_lambda         (quant inputs)
          |
          +--> SOFR cost-of-carry (fred_series)    (floating leg)
          |
          v
       IssuerSpec list  +  mu vector (R^n)
          |
          v
       scenario_generator.generate_loss_matrix  ->  L (T x n)
          |
          v
       mean_cvar.solve(mu, L, gamma=5.0)         ->  weights, cvar, status
          |
          v
       portfolio_weights  (INSERT OR REPLACE)
          |
          v
       macro-hedge sleeve sizer (Fix #2)          ->  MacroHedgeSpec
          |
          v
       portfolio_hedges    (INSERT OR REPLACE)

Expected-return calc (v4.1 §8, Sprint F directive)
--------------------------------------------------
    mu_i = spread_tightening_i  -  SOFR_carry_i

where

    spread_tightening_i   =   max(0, baseline_loss_i - model_loss_i) * LGD
    SOFR_carry_i          =   SOFR_1y * (horizon_days / 252)

The TRS receive-leg pays floating SOFR — that's a cost to the shorter. We
subtract it from the gross mean-reversion gain so the LP is solving for
net-of-funding return, not theoretical convergence.

TRS-only filter
---------------
Only pod_decisions rows with `expression='trs_junior_abs'` AND
`compliance_ok=TRUE` are forwarded. Post-Fix #2 the only legitimate pod
expression is TRS; the explicit filter remains to reject any legacy row
still carrying the (retired) `equity_short` expression string.

Macro-hedge sleeve (Fix #2)
---------------------------
The Mean-CVaR LP does NOT take a hedge instrument as a decision variable.
Mixing a static equity/credit-ETF hedge with a dynamic LP contaminates the
risk budget. Instead, after the LP clears, ``_size_hedge_sleeve`` computes
an HYG-short (or UST-futures) notional proportional to the aggregate TRS
abs-weight sum, and persists it to `portfolio_hedges`. This cleanly
decomposes: LP optimizes ISSUER SELECTION; sleeve optimizes INDEX BETA.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import duckdb
import numpy as np

from agents.schemas import MacroHedgeSpec
from data.settings import load_thresholds, settings
from portfolio.scenario_generator import IssuerSpec, generate_loss_matrix
from portfolio import mean_cvar

log = logging.getLogger(__name__)

# Map equity ticker -> ABS issuer. For now 1:1 since we only treat the five
# treated tickers. Extend when multi-trust-per-issuer support lands (Sprint G).
TICKER_TO_ISSUER = {
    "AFRM": "AFRM", "SQ": "SQ", "PYPL": "PYPL",
    "SEZL": "SEZL", "UPST": "UPST",
}


@dataclass
class BookResult:
    run_id: str
    issuers: list[str]
    mu: np.ndarray                    # (n,)
    weights: np.ndarray               # (n,)
    cvar_value: float
    cvar_contributions: np.ndarray    # (n,)
    gross_leverage: float
    gamma: float
    solver_status: str
    hedge: Optional[MacroHedgeSpec] = None   # Fix #2 sleeve, None if not sized


def _latest_jt_lambda(con: duckdb.DuckDBPyConnection,
                      as_of: datetime,
                      issuer: str) -> Optional[dict]:
    row = con.execute(
        """SELECT lambda_total, kappa, theta, sigma
           FROM jt_lambda
           WHERE issuer=? AND observed_at<=?
           ORDER BY observed_at DESC LIMIT 1""",
        [issuer, as_of.date()],
    ).fetchone()
    if not row:
        return None
    return {
        "lambda_total": float(row[0]) if row[0] is not None else 0.01,
        "kappa":        float(row[1]) if row[1] is not None else 0.5,
        "theta":        float(row[2]) if row[2] is not None else 0.03,
        "sigma":        float(row[3]) if row[3] is not None else 0.08,
    }


def _latest_sofr(con: duckdb.DuckDBPyConnection, as_of: datetime) -> float:
    """Most recent SOFR in `fred_series`. Returns annualized decimal (e.g. 0.0533).

    Falls back to 0.045 (the module-wide default discount rate used by JT)
    if the SOFR series is absent from the warehouse — pod stays runnable
    on partial data.
    """
    row = con.execute(
        """SELECT value FROM fred_series
           WHERE series_id='SOFR' AND observed_at<=?
             AND value IS NOT NULL
           ORDER BY observed_at DESC LIMIT 1""",
        [as_of.date()],
    ).fetchone()
    if row and row[0] is not None:
        v = float(row[0])
        # FRED SOFR is in percent (e.g. 5.33). Normalize.
        return v / 100.0 if v > 1.0 else v
    return 0.045


def _baseline_loss(lam: dict, horizon_days: int) -> float:
    """
    Naive baseline expected loss — what the market "prices in" at observed
    lambda. E[L] ≈ (1 - exp(-λ·T)) · LGD. We subtract this from the
    model-expected loss to get the tightening picked up by the LP as μ_gross.
    """
    import math
    T_yr = horizon_days / 252.0
    S = math.exp(-lam["lambda_total"] * T_yr)
    return (1.0 - S) * 0.55


def build_specs_and_mu(
    con: duckdb.DuckDBPyConnection,
    as_of: datetime,
    approved_issuers: list[str],
    *,
    horizon_days: int,
) -> tuple[list[IssuerSpec], np.ndarray, float, list[str]]:
    """Returns (specs, mu_vector, sofr_carry, kept_issuers).

    Issuers with no JT row are dropped. `sofr_carry` is the per-unit-notional
    cost that the LP subtracts inside its CVaR term (mu already has it netted).
    """
    sofr = _latest_sofr(con, as_of)
    sofr_carry = sofr * (horizon_days / 252.0)

    specs: list[IssuerSpec] = []
    mu_list: list[float] = []
    kept: list[str] = []

    for iss in approved_issuers:
        lam = _latest_jt_lambda(con, as_of, iss)
        if lam is None:
            log.warning("book | no JT lambda for %s — skipping", iss)
            continue
        specs.append(IssuerSpec(
            issuer=iss,
            kappa=lam["kappa"], theta=lam["theta"], sigma=lam["sigma"],
            lambda_0=lam["lambda_total"],
            ttm_days=horizon_days,
        ))
        # mu_i = E[tranche_loss_i] - SOFR_carry. Short earns the loss fraction,
        # pays SOFR on the leg notional. The LP uses mu directly as expected
        # NET return per unit notional; carry is passed separately so scenario
        # CVaR can include the carry floor (scenarios with L_s < carry hurt the
        # short book — that's where the tail risk lives, not in high-loss paths).
        gross = _baseline_loss(lam, horizon_days)
        mu_list.append(gross - sofr_carry)
        kept.append(iss)

    mu = np.asarray(mu_list, dtype=float) if mu_list else np.zeros(0)
    log.info("book | as_of=%s | SOFR=%.4f carry=%.4f | issuers=%s",
             as_of.date(), sofr, sofr_carry, kept)
    return specs, mu, sofr_carry, kept


def _approved_issuers_for_run(con: duckdb.DuckDBPyConnection,
                              run_id: str) -> tuple[list[str], datetime]:
    """Extract the issuer list from pod_decisions.trade_signal_json (if TRS+approved)."""
    row = con.execute(
        """SELECT as_of, compliance_ok, trade_signal_json
           FROM pod_decisions WHERE run_id=?""",
        [run_id],
    ).fetchone()
    if not row:
        raise ValueError(f"unknown run_id={run_id}")
    as_of, ok, signal_json = row
    if not ok:
        return [], as_of
    payload = json.loads(signal_json or "{}")
    if payload.get("expression") != "trs_junior_abs":
        # TRS-only filter — equity_short hedges do NOT enter the LP.
        return [], as_of
    # For TRS, the approved-issuer universe is every treated ticker with a live
    # lambda row; we don't require equity_tickers to be set on TRS expressions.
    return list(TICKER_TO_ISSUER.values()), as_of


def _persist_weights(con: duckdb.DuckDBPyConnection,
                     run_id: str, result: BookResult) -> None:
    rows = [
        (run_id, iss, float(w), float(mu), float(c),
         float(result.gross_leverage), float(result.cvar_value),
         float(result.gamma), result.solver_status)
        for iss, w, mu, c in zip(
            result.issuers, result.weights, result.mu, result.cvar_contributions,
        )
    ]
    con.executemany(
        """INSERT OR REPLACE INTO portfolio_weights
             (run_id, issuer, weight, expected_return, cvar_contribution,
              gross_leverage, cvar_value, gamma, solver_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )


def _size_hedge_sleeve(hedge_cfg: dict, trs_gross: float) -> MacroHedgeSpec:
    """Compute the static macro-hedge sleeve notional from the TRS book size.

    Parameters
    ----------
    hedge_cfg : dict
        thresholds.yaml["portfolio"]["hedge"] — carries instrument,
        sizing_rule, beta_credit, dv01_target.
    trs_gross : float
        Aggregate TRS absolute notional from the LP (= Σ|w_i|).

    Returns
    -------
    MacroHedgeSpec with signed `notional` (negative for shorts) and a
    `rationale` string suitable for audit logs.
    """
    instrument = str(hedge_cfg.get("instrument", "HYG_SHORT"))
    sizing_rule = str(hedge_cfg.get("sizing_rule", "beta_credit"))

    if sizing_rule == "beta_credit":
        beta = float(hedge_cfg.get("beta_credit", 0.60))
        # HYG_SHORT is negative notional; ZT_FUT under beta_credit mirrors
        # the same sign convention (short rates exposure to offset spread).
        notional = -beta * trs_gross
        hedge_ratio = beta
        rationale = (
            f"beta_credit sleeve: |hedge|={beta:.2f} × TRS_gross={trs_gross:.4f} "
            f"→ notional={notional:.4f} ({instrument})"
        )
    elif sizing_rule == "dv01_neutral":
        # Placeholder until ABS WAL flows through the pipe in Sprint G.
        # Use dv01_target as a direct multiplier so the field is at least
        # wired end-to-end; sizing will be recalibrated when the DV01
        # attribution lands.
        dv01_target = float(hedge_cfg.get("dv01_target", 1.0))
        notional = -dv01_target * trs_gross
        hedge_ratio = dv01_target
        rationale = (
            f"dv01_neutral sleeve (placeholder): dv01_target={dv01_target:.2f} × "
            f"TRS_gross={trs_gross:.4f} → notional={notional:.4f} ({instrument}); "
            "full DV01 attribution lands in Sprint G"
        )
    else:
        raise ValueError(f"unknown hedge sizing_rule: {sizing_rule!r}")

    return MacroHedgeSpec(
        instrument=instrument,            # type: ignore[arg-type]
        sizing_rule=sizing_rule,          # type: ignore[arg-type]
        notional=float(notional),
        hedge_ratio=float(hedge_ratio),
        trs_gross=float(trs_gross),
        rationale=rationale,
    )


def _persist_hedge(con: duckdb.DuckDBPyConnection,
                   run_id: str, hedge: MacroHedgeSpec) -> None:
    con.execute(
        """INSERT OR REPLACE INTO portfolio_hedges
             (run_id, instrument, sizing_rule, notional, hedge_ratio,
              trs_gross, rationale)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [run_id, hedge.instrument, hedge.sizing_rule,
         hedge.notional, hedge.hedge_ratio, hedge.trs_gross, hedge.rationale],
    )


def build(run_id: str,
          *, persist: bool = True,
          seed: int = 42) -> Optional[BookResult]:
    """Entry point invoked from agents.tick with --optimize.

    Returns None if the run is not TRS or not approved.
    """
    th = load_thresholds()["portfolio"]
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        issuers, as_of = _approved_issuers_for_run(con, run_id)
        if not issuers:
            log.info("book | run=%s | no TRS book to size (not approved / not TRS)", run_id)
            return None
        if isinstance(as_of, str):
            as_of = datetime.fromisoformat(as_of)

        specs, mu, sofr_carry, kept = build_specs_and_mu(
            con, as_of, issuers, horizon_days=int(th["horizon_days"]),
        )
        if not specs:
            log.warning("book | run=%s | no issuers with JT lambda available", run_id)
            return None

        L = generate_loss_matrix(specs, seed=seed)

        sol = mean_cvar.solve(
            mu=mu, L=L,
            carry=sofr_carry,
            gamma=float(th["gamma_risk_aversion"]),
            cvar_alpha=float(th["cvar_alpha"]),
            max_gross_leverage=float(th["max_gross_leverage"]),
            max_single_weight=float(th["max_single_trust_weight"]),
        )
        gross_leverage = float(np.abs(sol.weights).sum())
        hedge_cfg = th.get("hedge") or {}
        hedge = _size_hedge_sleeve(hedge_cfg, gross_leverage) if hedge_cfg else None

        result = BookResult(
            run_id=run_id,
            issuers=kept,
            mu=mu,
            weights=sol.weights,
            cvar_value=sol.cvar_value,
            cvar_contributions=sol.cvar_contributions,
            gross_leverage=gross_leverage,
            gamma=float(th["gamma_risk_aversion"]),
            solver_status=sol.status,
            hedge=hedge,
        )
        if persist:
            _persist_weights(con, run_id, result)
            if hedge is not None:
                _persist_hedge(con, run_id, hedge)
            log.info(
                "book | run=%s | persisted %d TRS legs + hedge=%s",
                run_id, len(kept),
                f"{hedge.instrument}:{hedge.notional:.4f}" if hedge else "none",
            )
        return result
    finally:
        con.close()
