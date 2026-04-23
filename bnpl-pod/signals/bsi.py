"""
Canonical BNPL Stress Index (BSI) scorer — Equation (1) of paper §6.

This module is the single, frozen reference implementation of the CFPB--MOVE
composite documented in ``paper_formal/paper_formal.tex`` §6 "Data Architecture
and the CFPB--MOVE Composite". It exists so that the paper's mathematical
specification and the live pod agree byte-for-byte — not by convention, but by
one file that both cite.

Paper ↔ code crosswalk
----------------------

The paper gives the scorer as::

    BSI_t  =  Σ_i  gamma_{i,t} · w_i · z^EWMA(X_{i,t}; μ_{i,t}, σ_{i,t})

    z^EWMA(x; μ, σ)  =  (x − μ) / max{σ, σ_floor_i}

    μ_{i,t}, σ_{i,t}  =  EWMA mean and standard deviation of the X_{i,·}
                        series, computed with half-life 250 trading days
                        (λ = 1 − 2^(−1/250) ≈ 0.00277).

    gamma_{i,t}  ∈  {0, 1}  =  per-pillar coverage gate; gamma_{i,t} = 1 iff the
                           trailing coverage-window density for pillar i
                           exceeds its pre-registered minimum threshold.

Implementation of each symbol in this file:

    ==============  ==========================================================
    paper symbol    code
    ==============  ==========================================================
    X_{i,t}         ``panel[pillar]`` (one column per pillar)
    w_i             ``spec.weights[pillar]`` from ``config/weights.yaml``
    σ_floor_i       ``spec.sigma_floor[pillar]`` from ``config/weights.yaml``
    half-life       ``spec.ewma_halflife_days``
    gamma_{i,t}         ``_coverage_gate(panel, spec)`` indicator per (pillar,
                    date) cell
    BSI_t           output column ``bsi``
    z_BSI_t         output column ``z_bsi`` (EWMA z of the BSI level itself)
    ==============  ==========================================================

This module deliberately does NOT implement the origination residualised
variant; that lives in ``signals/bsi_residual.py`` and is invoked explicitly
as an alternative scorer when Phase B origination data is live. The
pre-registered decision rule governing which scorer the paper reports is
reproduced in ``signals/bsi_residual.py``.

Version-1 carry-over
--------------------

Gate 1 of the compliance engine uses a +1.5 σ threshold that was
calibrated against the v1 180-day rolling-σ estimator. The EWMA σ implemented
here is tighter than the v1 rolling σ on the 17 January 2025 pulse, so
+1.5 σ is a weakly conservative carry-over — it fires here if it fired in
v1. The paper discloses this carry-over explicitly at the Gate-1 description;
re-thresholding on realised EWMA σ is a Phase C deliverable and is
deliberately deferred to avoid post-hoc tuning.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

import duckdb
import numpy as np
import pandas as pd

from data.settings import load_weights, settings

# ---------------------------------------------------------------------------
# Canonical pillar names. Paper §6 Table "Pillar Inventory" uses these
# identifiers; the warehouse `bsi_daily` table uses `c_<pillar>` columns.
# ---------------------------------------------------------------------------

LOAD_BEARING: tuple[str, ...] = ("cfpb", "move")
COVERAGE_GATED: tuple[str, ...] = ("trends", "reddit", "appstore")
AUXILIARY: tuple[str, ...] = ("vitality", "macro")
ALL_PILLARS: tuple[str, ...] = LOAD_BEARING + COVERAGE_GATED + AUXILIARY

_WAREHOUSE_COLUMN: Mapping[str, str] = {
    "cfpb": "c_cfpb",
    "trends": "c_trends",
    "reddit": "c_reddit",
    "appstore": "c_appstore",
    "move": "c_move",
    "vitality": "c_vitality",
    # `macro` pillar has no bsi_daily column in the current warehouse; it is
    # retained for config completeness and will surface when FRED-driven
    # factors are folded into the QP fuse. _load_panel_from_warehouse will
    # return an all-null series for it.
    "macro": "c_macro",
}


@dataclass(frozen=True)
class BSISpec:
    """
    Frozen specification of the canonical BSI scorer (Equation (1) of §6).

    Values here must match the paper prose exactly. Any change to any field
    is a specification change and should be accompanied by a paper edit.
    """

    # EWMA window. Paper §6: "EWMA with half-life 250 trading days".
    ewma_halflife_days: int = 250

    # Pillar weights (paper §6 Table, not yet QP-refit). Must sum to 1 and
    # each be in [0, 1]. Load-bearing pillars carry higher priors.
    weights: Mapping[str, float] = field(default_factory=lambda: {
        "cfpb": 0.30,
        "move": 0.30,
        "trends": 0.10,
        "reddit": 0.10,
        "appstore": 0.10,
        "vitality": 0.05,
        "macro": 0.05,
    })

    # Per-pillar σ floor. Prevents a sleepy pillar (tiny EWMA σ) from
    # producing arithmetic-artefact z-scores when it moves by a single point.
    # Units: same as X_{i,t}.
    sigma_floor: Mapping[str, float] = field(default_factory=lambda: {
        "cfpb": 0.25,
        "move": 2.50,
        "trends": 0.50,
        "reddit": 0.50,
        "appstore": 0.25,
        "vitality": 0.10,
        "macro": 0.10,
    })

    # Per-pillar coverage-gate threshold: gamma_{i,t} = 1 iff the fraction of
    # non-null observations in the trailing `coverage_window_days` window
    # is at least this much. Paper §6 "Coverage gate" — conservative so
    # thinly populated pillars drop out rather than spuriously firing.
    coverage_min: Mapping[str, float] = field(default_factory=lambda: {
        "cfpb": 0.80,
        "move": 0.95,
        "trends": 0.50,
        "reddit": 0.50,
        "appstore": 0.50,
        "vitality": 0.30,
        "macro": 0.30,
    })
    coverage_window_days: int = 180

    # Numerical guardrails.
    min_observations_for_ewma: int = 30

    @property
    def lam(self) -> float:
        """EWMA decay factor λ = 1 − 2^(−1/half-life)."""
        return 1.0 - 2.0 ** (-1.0 / float(self.ewma_halflife_days))

    def validate(self) -> None:
        weight_sum = sum(self.weights.values())
        if abs(weight_sum - 1.0) > 1e-6:
            raise ValueError(
                f"BSISpec weights must sum to 1.0; got {weight_sum:.6f}"
            )
        for p in ALL_PILLARS:
            if p not in self.weights:
                raise ValueError(f"BSISpec.weights missing pillar {p!r}")
            if p not in self.sigma_floor:
                raise ValueError(f"BSISpec.sigma_floor missing pillar {p!r}")
            if p not in self.coverage_min:
                raise ValueError(f"BSISpec.coverage_min missing pillar {p!r}")


# ---------------------------------------------------------------------------
# Factory: read YAML and instantiate
# ---------------------------------------------------------------------------

def load_spec() -> BSISpec:
    """
    Build a `BSISpec` from ``config/weights.yaml``.

    The YAML must carry these top-level keys (see paper §6):

        default_weights: {<pillar>: <weight>, ...}
        sigma_floor:     {<pillar>: <float>, ...}
        coverage_min:    {<pillar>: <float>, ...}
        ewma:
          halflife_days:        int
          coverage_window_days: int
    """
    cfg = load_weights()
    weights_raw = cfg.get("default_weights") or {}
    sigma_floor = cfg.get("sigma_floor") or {}
    coverage_min = cfg.get("coverage_min") or {}
    ewma = cfg.get("ewma") or {}

    spec = BSISpec(
        ewma_halflife_days=int(ewma.get("halflife_days", 250)),
        coverage_window_days=int(ewma.get("coverage_window_days", 180)),
        weights=dict(weights_raw),
        sigma_floor=dict(sigma_floor),
        coverage_min=dict(coverage_min),
    )
    spec.validate()
    return spec


# ---------------------------------------------------------------------------
# Panel loader — either from an explicit DataFrame or from the warehouse
# ---------------------------------------------------------------------------

def _load_panel_from_warehouse(
    conn: duckdb.DuckDBPyConnection,
    pillars: Sequence[str] = ALL_PILLARS,
) -> pd.DataFrame:
    cols = ["observed_at"]
    for p in pillars:
        wcol = _WAREHOUSE_COLUMN.get(p)
        if wcol is None:
            continue
        cols.append(f"{wcol} AS {p}")
    sql = (
        "SELECT "
        + ", ".join(cols)
        + " FROM bsi_daily ORDER BY observed_at"
    )
    try:
        df = conn.execute(sql).fetch_df()
    except duckdb.BinderException:
        # `c_macro` column absent on this warehouse; retry without it.
        cols_present = [c for c in cols if not c.startswith("c_macro")]
        missing = [p for p in pillars if _WAREHOUSE_COLUMN.get(p) not in
                   [c.split()[0] for c in cols_present[1:]]]
        sql = (
            "SELECT "
            + ", ".join(cols_present)
            + " FROM bsi_daily ORDER BY observed_at"
        )
        df = conn.execute(sql).fetch_df()
        for p in missing:
            df[p] = np.nan
    df["observed_at"] = pd.to_datetime(df["observed_at"])
    df = df.set_index("observed_at")
    # Re-order columns canonically
    for p in pillars:
        if p not in df.columns:
            df[p] = np.nan
    return df[list(pillars)]


# ---------------------------------------------------------------------------
# EWMA primitives
# ---------------------------------------------------------------------------

def _ewma_mean_std(series: pd.Series, halflife_days: int) -> tuple[pd.Series, pd.Series]:
    """
    EWMA mean and (biased, EWMA-squared) std over a Series, NaN-safe.

    NaNs are skipped: the EWMA state is held constant through missing days.
    This matches the paper's statement that coverage-gated pillars contribute
    zero to BSI on days where they are absent, rather than injecting noise.
    """
    ewm = series.ewm(halflife=halflife_days, adjust=False, ignore_na=True)
    mu = ewm.mean()
    sd = ewm.std()
    return mu, sd


def _coverage_gate(
    panel: pd.DataFrame,
    spec: BSISpec,
) -> pd.DataFrame:
    """
    Per-pillar coverage-gate indicator gamma_{i,t} ∈ {0, 1}.

    gamma_{i,t} = 1 iff trailing-`coverage_window_days` non-null density on pillar
    i is ≥ spec.coverage_min[i]. Returns a DataFrame with the same shape as
    `panel`, dtype float (0.0 or 1.0).
    """
    density = panel.notna().rolling(
        window=spec.coverage_window_days,
        min_periods=1,
    ).mean()
    gate = pd.DataFrame(0.0, index=panel.index, columns=panel.columns)
    for p in panel.columns:
        thr = float(spec.coverage_min.get(p, 0.5))
        gate[p] = (density[p] >= thr).astype(float)
    return gate


# ---------------------------------------------------------------------------
# The scorer itself — Equation (1)
# ---------------------------------------------------------------------------

def compute_bsi(
    panel: pd.DataFrame,
    spec: BSISpec | None = None,
) -> pd.DataFrame:
    """
    Apply Equation (1) to a pillar panel and return a DataFrame with columns:

        bsi, z_bsi, <one column per pillar's z-score>, gamma_<pillar>

    `panel` is a DataFrame indexed by date with one column per pillar in
    `ALL_PILLARS`. Missing columns are treated as all-null (coverage gate
    will zero them out).

    The returned `bsi` column is BSI_t in the paper. The `z_bsi` column is
    its own EWMA z — the quantity Gate 1 consumes.
    """
    if spec is None:
        spec = load_spec()
    spec.validate()

    # Ensure all configured pillars are present (fill absent with NaN).
    for p in ALL_PILLARS:
        if p not in panel.columns:
            panel = panel.assign(**{p: np.nan})
    panel = panel[list(ALL_PILLARS)].copy()

    out = pd.DataFrame(index=panel.index)
    gate = _coverage_gate(panel, spec)

    # Per-pillar gated EWMA z-scores
    weighted_sum = pd.Series(0.0, index=panel.index)
    weight_used = pd.Series(0.0, index=panel.index)
    for p in ALL_PILLARS:
        x = panel[p]
        mu, sd = _ewma_mean_std(x, spec.ewma_halflife_days)
        sd_floored = sd.where(
            sd >= spec.sigma_floor[p], other=spec.sigma_floor[p]
        )
        z = (x - mu) / sd_floored
        # gate drops contribution on thin-coverage cells
        z_gated = z * gate[p]
        out[f"z_{p}"] = z_gated
        out[f"gamma_{p}"] = gate[p]
        w = float(spec.weights[p])
        weighted_sum = weighted_sum.add(w * z_gated.fillna(0.0), fill_value=0.0)
        weight_used = weight_used.add(w * gate[p], fill_value=0.0)

    # Renormalise by the effective gate-weighted total so that a day where
    # only the 100%-coverage MOVE pillar survives is not penalised against a
    # day where every pillar is live. This preserves the paper's "coverage-
    # gated weighted z-score" wording.
    bsi_level = np.where(weight_used > 0, weighted_sum / weight_used, np.nan)
    out["bsi"] = pd.Series(bsi_level, index=panel.index)

    bsi_mu, bsi_sd = _ewma_mean_std(out["bsi"], spec.ewma_halflife_days)
    bsi_sd_floored = bsi_sd.where(bsi_sd >= 1e-6, other=1e-6)
    out["z_bsi"] = (out["bsi"] - bsi_mu) / bsi_sd_floored

    return out


def compute_bsi_from_warehouse(
    conn: duckdb.DuckDBPyConnection | None = None,
    spec: BSISpec | None = None,
) -> pd.DataFrame:
    """Convenience: open the default warehouse and run `compute_bsi`."""
    owns_conn = False
    if conn is None:
        conn = duckdb.connect(str(settings.duckdb_path), read_only=True)
        owns_conn = True
    try:
        panel = _load_panel_from_warehouse(conn)
        return compute_bsi(panel, spec=spec)
    finally:
        if owns_conn:
            conn.close()


# ---------------------------------------------------------------------------
# CLI smoke test: rebuild `bsi_daily`-style output from the warehouse and
# print the headline 17 January 2025 pulse.
# ---------------------------------------------------------------------------

def _cli() -> int:
    spec = load_spec()
    print(
        "BSISpec: halflife={hl}d, lambda={lam:.6f}, coverage_window={cw}d".format(
            hl=spec.ewma_halflife_days, lam=spec.lam, cw=spec.coverage_window_days
        )
    )
    print("weights:", dict(spec.weights))
    print("sigma_floor:", dict(spec.sigma_floor))
    print()

    try:
        out = compute_bsi_from_warehouse()
    except Exception as exc:  # noqa: BLE001
        print("compute_bsi_from_warehouse failed:", exc)
        return 2

    print(f"rows: {len(out)}  date range: {out.index.min().date()} -> {out.index.max().date()}")
    event = pd.Timestamp("2025-01-17")
    if event in out.index:
        row = out.loc[event]
        print()
        print("17 January 2025 snapshot:")
        print(f"  bsi   = {row['bsi']:.4f}")
        print(f"  z_bsi = {row['z_bsi']:.4f}")
        for p in ALL_PILLARS:
            gname = f"gamma_{p}"
            zname = f"z_{p}"
            if gname in row.index:
                print(
                    f"  pillar {p:<9}: gamma={row[gname]:.0f}, z={row[zname]:.4f}"
                    if not math.isnan(row[zname])
                    else f"  pillar {p:<9}: gamma={row[gname]:.0f}, z=nan"
                )
    else:
        print("2025-01-17 not present in bsi_daily panel.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_cli())
