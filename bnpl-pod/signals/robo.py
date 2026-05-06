"""
signals/robo.py — ROBO meta-archetype population-aggregation layer.

This module is the bridge between the Phase 2D Monte Carlo validation results
and the live pod. It exposes a single function `compute_robo_signal(...)` that
the trade-firing engine calls; that function transparently transitions from
synthetic agents (cold-start) to real users as the user base grows.

DEPLOYMENT PROTOCOL (paper §10.6):
  N_real ∈ [0, 50)         → 100% synthetic (Monte Carlo prior)
  N_real ∈ [50, K_target)  → linear blend, w_real = (N_real - 50) / (K_target - 50)
  N_real ≥ K_target        → 100% real users

K_target is read from out/v2/robo_montecarlo_summary_v21.json. The Monte Carlo
tells us at what user count the population layer's accuracy (under
PnL-weighted reputation) saturates within 5% of asymptote.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Literal

# Locate the Monte Carlo summary written by run_robo_montecarlo.py
_MC_CANDIDATES = [
    Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL_v9_FINAL\01_paper\empirics_v2\out\v2\robo_montecarlo_summary_v21.json"),
]
_MC_PATH = next((p for p in _MC_CANDIDATES if p.exists()), None)


def _load_mc() -> dict:
    if _MC_PATH is None:
        return {
            "converged_K": 500, "headline_pnl": {}, "headline_accuracy_K500": {},
            "deployment_protocol": "Monte Carlo summary not yet generated.",
            "honest_caveats": [], "named_cases": [],
            "adversarial_robustness_K500": {},
        }
    with open(_MC_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


_MC = _load_mc()
# Effective transition target: clamp converged_K into a sane operational range.
# The strict std<0.05 criterion can produce K=5000 which is unrealistic for a
# launch-phase product. We cap at 500 because Monte Carlo shows 96% accuracy
# already at K=500 under PnL-weighted reputation.
_K_TARGET = min(_MC.get("converged_K", 500), 500)
_K_FLOOR  = 50    # below this we are 100% synthetic


def deployment_status(n_real_users: int) -> dict:
    """Return the synthetic↔real blend status for a given real-user count."""
    if n_real_users < _K_FLOOR:
        w_real = 0.0
        phase = "SYNTHETIC_ONLY"
    elif n_real_users >= _K_TARGET:
        w_real = 1.0
        phase = "REAL_ONLY"
    else:
        w_real = (n_real_users - _K_FLOOR) / (_K_TARGET - _K_FLOOR)
        phase = "TRANSITIONING"
    return {
        "n_real_users":   n_real_users,
        "K_floor":        _K_FLOOR,
        "K_target":       _K_TARGET,
        "weight_real":    round(w_real, 3),
        "weight_synth":   round(1 - w_real, 3),
        "phase":          phase,
        "next_milestone": (
            f"At N_real = {_K_FLOOR}, real-user signal begins blending in." if n_real_users < _K_FLOOR else
            f"At N_real = {_K_TARGET}, synthetic agents are fully retired." if n_real_users < _K_TARGET else
            "Synthetic layer fully retired; ROBO_population is 100% live users."
        ),
    }


def get_montecarlo_summary() -> dict:
    """Expose the full MC summary for the /robo page."""
    return _MC


def compute_robo_signal(ticker: str, *,
                        n_real_users: int = 0,
                        regime: Literal["uniform", "pnl_weighted", "kelly_weighted"] = "pnl_weighted",
                       ) -> dict:
    """Compute the ROBO_population signal for a ticker under the current
    deployment status (synthetic / blended / real).

    Currently returns the Monte Carlo prior value because we have zero real
    users. As real user trades accumulate in the journal table, this function
    will progressively weight real-user signal in via deployment_status().
    """
    status = deployment_status(n_real_users)
    pnl_table = _MC.get("headline_pnl", {})
    acc_table = _MC.get("headline_accuracy_K500", {})

    synthetic_signal = {
        # The Monte Carlo headline expectation under the chosen regime
        "expected_pnl_pct":     pnl_table.get(f"ROBO_population_{regime}", 0.0),
        "expected_accuracy":    acc_table.get(regime, 0.0),
        "regime":               regime,
        "source":               "monte_carlo_synthetic",
    }

    # Real-user component is a stub for now (no real user data); when the
    # journal table accumulates per-user trade history the live aggregation
    # plugs in here.
    real_signal = None

    return {
        "ticker":            ticker,
        "deployment_status": status,
        "synthetic":         synthetic_signal,
        "real":              real_signal,
        "blended_pnl_pct":   round(
            status["weight_synth"] * synthetic_signal["expected_pnl_pct"], 2
        ),
        "regime":            regime,
    }


__all__ = [
    "deployment_status", "compute_robo_signal", "get_montecarlo_summary",
]
