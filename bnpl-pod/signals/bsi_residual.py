"""
Origination-residual BSI scorer (Phase C.2 of v2_roadmap.md).

The v1 CFPB-momentum pillar regresses a complaint-count time series
against nothing; its level is mechanically a function of the BNPL
origination stock, which was growing ~40% YoY across the sample window.
The 17 January 2025 filing-deadline pulse therefore lives on top of a
secular growth trend that the v1 scorer did not subtract. Referees will
(correctly) worry that the +27.4σ v1 reading on that date is partly
mis-specification rather than distress.

This module implements the pre-registered fix: regress daily complaint
momentum on log(interpolated daily originations), take the residual as
the CFPB-pillar input, and run the rest of the BSI machinery unchanged
(EWMA σ, per-pillar coverage-gate, QP fuse).

The pre-registered decision criterion that gates the paper's framing
is reproduced below, from docs/v2_roadmap.md §C.3, so that a reader
who lands here before reading the roadmap still sees the rule:

    If ≥ 4 of 5 canonical events fire the 4-gate AND under the
        residualised BSI:            v2 retains the behavioral-sensor
                                     framing (paper body unchanged).
    If ≤ 2 of 5 events fire:         v2 swaps to the construct-
                                     validity-only framing sealed in
                                     docs/alt_abstract_sealed.md.
    If exactly 3 of 5 fire:          author decides; paper discloses
                                     the 3/5 result in abstract either
                                     way.

Pre-registered specification
----------------------------

Let c_t be the CFPB daily complaint count for BNPL-tagged complaints
(cfpb_complaints filtered by product ILIKE '%BNPL%'). Let g_t be the
interpolated daily composite-issuer originations in USD from
`signals.originations_interp.interpolate_daily()`.

Define the momentum pillar as the residual from

    m_t  :=  c_t / MA_28(c_t)     (v1 momentum; scale-free)
    r_t  :=  m_t  -  α  -  β · log(g_t)

where (α, β) are estimated OLS on the training window 2019-07-01 to
2025-06-30 (pre-registered in v2_roadmap.md §A.5). The held-out window
(2025-07-01 onward) uses the trained coefficients to compute r_t out-
of-sample. No coefficient refit after 2025-06-30.

The residual r_t replaces m_t as the CFPB pillar input to the EWMA-σ /
QP-fuse stack; all downstream machinery (σ half-life = 250d, floor,
per-pillar coverage-gate, constrained QP) is held constant versus v1.

Data contract (inputs)
----------------------

complaints : DataFrame[date, bnpl_complaint_count]
    Daily BNPL complaint count from `cfpb_complaints`.
originations_daily : DataFrame[date, gmv_daily_usd, coverage_mask]
    Output of `signals.originations_interp.interpolate_daily()`.

Data contract (outputs)
-----------------------

DataFrame[date, m_raw, g_daily, r_residual, r_zscore, coverage_mask]
    The `r_zscore` column is the EWMA-σ standardised residual, the
    input that the BSI QP fuse expects under scorer="residual".

Implementation status
---------------------

**STAGED, NOT LIVE.** Prerequisites (all data-gated):

    1. `signals.originations_interp` requires `data/10q/*.parquet` which
       requires Phase B.1–B.3 EDGAR/IR-deck pulls. Blocker: AFRM has
       zero EDGAR filings (144A); SQ segment-parse pending; PYPL
       Pay-in-4 parse pending.
    2. The v1 BSI scorer module itself (`signals/bsi.py`) is not in
       this working copy — the paper prose describes it but the
       canonical implementation lives in an earlier sprint. When it
       lands, the `run_residualised_event_study()` entrypoint below
       should dispatch to it via the `scorer="residual"` parameter.
    3. The event windows frozen in `backtest/event_windows/` are
       referenced but not present in this working copy either.

This module exposes the specification and the interface so that, the
moment the Phase B data pulls and the v1 scorer land, the residualised
event study is one function call away.

Author: Siddharth Verma, UIUC, FIN 580 Spring 2026 cohort.
Provenance: v2 scorer-surgery staging, 2026-04-23.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


ScorerVariant = Literal["raw", "residual"]


@dataclass(frozen=True)
class ResidualisationSpec:
    """Pre-registered residualisation parameters. Freeze before any fit."""

    momentum_window_days: int = 28           # MA_28 in m_t := c_t / MA_28(c_t)
    training_start: str = "2019-07-01"
    training_end: str = "2025-06-30"
    holdout_start: str = "2025-07-01"
    log_base: Literal["e", "10"] = "e"
    intercept: bool = True
    refit_oos: bool = False                  # MUST stay False


@dataclass(frozen=True)
class ResidualisationFit:
    """OLS coefficients from the training window."""

    alpha: float
    beta: float
    n_train: int
    r2: float
    residual_std_train: float


@dataclass(frozen=True)
class EventSurvivalRow:
    """One row of the scorer-surgery comparison table."""

    event_date: str            # ISO date
    event_label: str           # e.g. "Reg Z BNPL 2025"
    bsi_z_raw: float           # v1 scorer reading
    bsi_z_residual: float      # v2 residualised reading
    fires_4gate_raw: bool
    fires_4gate_residual: bool

    @property
    def survives(self) -> bool:
        return self.fires_4gate_residual


def fit_residualisation(
    complaints,
    originations_daily,
    spec: ResidualisationSpec | None = None,
) -> ResidualisationFit:
    """Fit α + β · log(g_t) on the training window.

    Refuses to fit if `spec.refit_oos` is True (guard against the one
    post-hoc choice we can make by accident).
    """
    import numpy as np
    import pandas as pd

    spec = spec or ResidualisationSpec()
    if spec.refit_oos:
        raise ValueError(
            "refit_oos=True is explicitly forbidden by v2 pre-registration. "
            "See docs/v2_roadmap.md §A.5."
        )

    df = pd.merge(complaints, originations_daily, on="date", how="inner")
    df["date"] = pd.to_datetime(df["date"])
    train = df[
        (df["date"] >= spec.training_start) & (df["date"] <= spec.training_end)
    ].copy()

    # momentum: c_t / MA_window(c_t)
    ma = train["bnpl_complaint_count"].rolling(
        spec.momentum_window_days, min_periods=spec.momentum_window_days
    ).mean()
    m = train["bnpl_complaint_count"] / ma
    logg = (
        np.log(train["gmv_daily_usd"].clip(lower=1.0))
        if spec.log_base == "e"
        else np.log10(train["gmv_daily_usd"].clip(lower=1.0))
    )

    mask = m.notna() & logg.notna() & (train["coverage_mask"] > 0)
    y = m[mask].values
    x = logg[mask].values
    n = int(mask.sum())
    if n < 100:
        raise ValueError(
            f"Too few usable training observations: {n}. "
            "Check coverage_mask and MA warm-up."
        )

    X = np.column_stack([np.ones_like(x), x]) if spec.intercept else x[:, None]
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    yhat = X @ coef
    resid = y - yhat
    ss_res = float((resid ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    alpha = float(coef[0]) if spec.intercept else 0.0
    beta = float(coef[-1])
    return ResidualisationFit(
        alpha=alpha,
        beta=beta,
        n_train=n,
        r2=r2,
        residual_std_train=float(resid.std(ddof=2)),
    )


def apply_residualisation(
    complaints,
    originations_daily,
    fit: ResidualisationFit,
    spec: ResidualisationSpec | None = None,
):
    """Apply trained coefficients across the full sample (train + holdout).

    Returns a DataFrame matching the output contract in the module
    docstring.
    """
    import numpy as np
    import pandas as pd

    spec = spec or ResidualisationSpec()
    df = pd.merge(complaints, originations_daily, on="date", how="inner")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    ma = df["bnpl_complaint_count"].rolling(
        spec.momentum_window_days, min_periods=spec.momentum_window_days
    ).mean()
    df["m_raw"] = df["bnpl_complaint_count"] / ma
    df["g_daily"] = df["gmv_daily_usd"]
    logg = (
        np.log(df["g_daily"].clip(lower=1.0))
        if spec.log_base == "e"
        else np.log10(df["g_daily"].clip(lower=1.0))
    )
    df["r_residual"] = df["m_raw"] - (fit.alpha + fit.beta * logg)

    # EWMA-σ standardisation is delegated to the v1 EWMA module when it
    # lands; here we emit the raw residual and a simple placeholder
    # z-score so the downstream interface compiles.
    ewma_sigma = df["r_residual"].ewm(halflife=250, min_periods=250).std()
    df["r_zscore"] = df["r_residual"] / ewma_sigma

    return df[
        [
            "date",
            "m_raw",
            "g_daily",
            "r_residual",
            "r_zscore",
            "coverage_mask",
        ]
    ]


def run_residualised_event_study(
    event_windows_path: str | Path | None = None,
    warehouse_path: str | Path | None = None,
    spec: ResidualisationSpec | None = None,
) -> list[EventSurvivalRow]:
    """Run the residualised BSI across the 5 frozen event windows.

    Orchestrates:
        1. load_quarterly_originations -> interpolate_daily
        2. load daily BNPL complaint counts
        3. fit_residualisation on training window
        4. apply_residualisation full sample
        5. dispatch to v1 4-gate compliance engine with scorer="residual"
        6. produce the comparison table (raw vs residual survival)

    Returns the comparison rows. Raises FileNotFoundError with a clear
    message if any prerequisite (originations parquet, v1 scorer, event
    windows) is absent. See docs/scorer_surgery_result.md for current
    gating state.
    """
    raise NotImplementedError(
        "run_residualised_event_study() is staged but data-gated. "
        "See docs/scorer_surgery_result.md for current prerequisites "
        "(Phase B.1-B.3 10-Q pulls + v1 BSI scorer module)."
    )


def format_comparison_table(rows: list[EventSurvivalRow]) -> str:
    """Render the raw-vs-residual survival table as Markdown.

    Used by docs/scorer_surgery_result.md once Phase B data lands.
    """
    header = (
        "| Event date | Label | v1 z | v2 z (residual) | v1 fires | v2 fires |\n"
        "|---|---|---:|---:|:---:|:---:|\n"
    )
    body = "\n".join(
        f"| {r.event_date} | {r.event_label} "
        f"| {r.bsi_z_raw:+.2f} | {r.bsi_z_residual:+.2f} "
        f"| {'Y' if r.fires_4gate_raw else 'N'} "
        f"| {'Y' if r.fires_4gate_residual else 'N'} |"
        for r in rows
    )
    survived = sum(r.fires_4gate_residual for r in rows)
    n_total = len(rows)
    decision = _decide_framing(survived, n_total)
    footer = (
        f"\n\n**Residual-scorer survivors:** {survived} / {n_total}. "
        f"**Pre-registered decision:** {decision}."
    )
    return header + body + footer


def _decide_framing(survived: int, n_total: int) -> str:
    """Apply the v2_roadmap §C.3 decision rule verbatim."""
    if n_total != 5:
        return (
            f"decision rule is defined on the 5 frozen canonical events; "
            f"received n={n_total}, author must re-examine."
        )
    if survived >= 4:
        return "retain behavioral-sensor framing (paper body unchanged)"
    if survived <= 2:
        return (
            "swap to construct-validity-only framing "
            "(docs/alt_abstract_sealed.md becomes v2 abstract verbatim)"
        )
    return (
        "3/5 boundary case: author decides; abstract discloses 3/5 either way"
    )


def _cli_disclose() -> None:
    """Print the Phase B gating disclosure and exit non-zero."""
    import sys

    print(
        "bsi_residual.py is STAGED but not LIVE.\n"
        "\n"
        "Prerequisites (all data-gated):\n"
        "    1. signals/originations_interp.py outputs (Phase B.1-B.3)\n"
        "    2. v1 BSI scorer module (signals/bsi.py — not in this\n"
        "       working copy as of 2026-04-23)\n"
        "    3. backtest/event_windows/ frozen event set (not in this\n"
        "       working copy as of 2026-04-23)\n"
        "\n"
        "Pre-registered decision rule (from docs/v2_roadmap.md §C.3):\n"
        "    >= 4 of 5 survive : behavioral-sensor framing stays\n"
        "    <= 2 of 5 survive : swap to sealed alt abstract\n"
        "     = 3 of 5 survive : author decides, disclose in abstract\n"
        "\n"
        "See docs/scorer_surgery_result.md for the current gating state\n"
        "and the path to LIVE.\n"
    )
    sys.exit(2)


if __name__ == "__main__":
    _cli_disclose()
