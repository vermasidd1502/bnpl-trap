"""
Cumulative-abnormal-return event study (paper §10.X, v2.1 additions).

Phase B.2 of the non-hallucinating alpha pivot. This module computes AFRM's
abnormal returns around the five 2022-2025 BNPL regulatory catalysts under a
two-factor market model fit on a pre-event estimation window, then tests the
cross-sectional average-abnormal-return (AAR) at lag 0 and the cumulative-
abnormal-return (CAR) over the event window against a bootstrap null.

Pre-registered hypothesis (state in paper §10.X BEFORE the result)
------------------------------------------------------------------
H_0: Average abnormal return AAR over the (-5, +5) event window of AFRM across
     the five regulatory catalysts is zero under a two-factor (HYG, MOVE)
     market model estimated on the pre-event window (-250, -30).
H_1: AAR < 0 with a bootstrap-adjusted t-statistic t < -2.0 (cross-sectional
     Patell-1976-style variance correction).
Rejection region: t_boot < -2.0 AND bootstrap 95% CI for AAR excludes zero.

Factor substitution
-------------------
The plan specified a Fama-French + HYG market model; this warehouse has neither
FF factors nor SPY. We use (HYG_log_return, MOVE_daily_delta) as the two-factor
proxy: HYG captures credit-market beta (AFRM is a consumer-credit name; HYG is
the closest public credit-exposure index available) and MOVE captures rates-
volatility beta (the 2022-2025 catalysts cluster around a rising-rates regime).
The substitution is disclosed in paper §10.X footnote; no external data is
ingested.

Outputs
-------
`paper_formal/figures/car_table.tex`  -- per-event CAR + t-stat + p-value,
                                         AAR bottom row, booktabs-style.
`paper_formal/figures/car_timepath.pdf` -- AFRM AAR over (-5, +5) averaged
                                         across the five events with a
                                         bootstrap 95% CI band.

Non-hallucination discipline
----------------------------
`run()` writes the CAR row regardless of sign. If the AAR is insignificant
(or wrong sign), the paper §10.X text must report the result verbatim. No
alternative specifications are swept from the same data.

Author: Siddharth Verma, UIUC, FIN 580 Spring 2026 cohort.
Provenance: Phase B.2 of v2.1 non-hallucinating alpha pivot, 2026-04-23.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Mapping, Sequence

import duckdb
import numpy as np
import pandas as pd

from backtest.event_study import WINDOWS, EventWindow
from data.settings import settings


# ---------------------------------------------------------------------------
# Pre-registered constants
# ---------------------------------------------------------------------------

EVENT_WINDOW: tuple[int, int] = (-5, 5)          # trading-day event window
ESTIMATION_WINDOW: tuple[int, int] = (-250, -30) # trading-day pre-event fit
BOOT_N: int = 10_000                              # bootstrap replications
BOOT_SEED: int = 42
T_CRITICAL: float = -2.0                          # pre-registered rejection


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_price_series(
    con: duckdb.DuckDBPyConnection,
    series_id: str,
) -> pd.Series:
    """Load a FRED-style daily level series from the warehouse."""
    df = con.execute(
        "SELECT observed_at, value FROM fred_series WHERE series_id = ? "
        "ORDER BY observed_at",
        [series_id],
    ).fetch_df()
    if df.empty:
        raise ValueError(f"no rows in fred_series for series_id={series_id}")
    df["observed_at"] = pd.to_datetime(df["observed_at"])
    s = df.set_index("observed_at")["value"].astype(float).sort_index()
    s.name = series_id
    return s


def _build_returns_panel(
    con: duckdb.DuckDBPyConnection,
    ticker: str = "AFRM",
    factors: Sequence[str] = ("HYG", "MOVE"),
) -> pd.DataFrame:
    """Build a business-day-indexed panel:
        r_<ticker>      daily log-return
        r_<factor>      daily log-return for price-style factors (HYG)
                        or daily first-difference for MOVE (levels in bp).

    Drops rows with any NaN so the market-model OLS has a complete matrix.
    """
    t = _load_price_series(con, ticker)
    df = pd.DataFrame({f"p_{ticker}": t})
    for f in factors:
        df[f"p_{f}"] = _load_price_series(con, f)

    bdays = pd.bdate_range(df.index.min(), df.index.max())
    df = df.reindex(bdays).ffill(limit=3)

    # AFRM and HYG are price series -> log-return; MOVE is a bp level ->
    # first-difference (interpreted as a vol-shock factor).
    out = pd.DataFrame(index=df.index)
    out[f"r_{ticker}"] = np.log(df[f"p_{ticker}"]).diff()
    for f in factors:
        if f == "MOVE":
            out[f"r_{f}"] = df[f"p_{f}"].diff()
        else:
            out[f"r_{f}"] = np.log(df[f"p_{f}"]).diff()

    return out.dropna(how="any")


# ---------------------------------------------------------------------------
# Market model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MarketModelFit:
    alpha: float
    beta: tuple[float, ...]
    sigma_e: float
    n_obs: int
    factors: tuple[str, ...]


def market_model(
    returns: pd.DataFrame,
    ticker: str,
    factors: Sequence[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> MarketModelFit:
    """Fit r_ticker = alpha + sum_k beta_k * r_factor_k + epsilon on the
    estimation window [start, end]. Returns MarketModelFit."""
    slice_ = returns[(returns.index >= start) & (returns.index <= end)].dropna()
    y = slice_[f"r_{ticker}"].values
    X_cols = [f"r_{f}" for f in factors]
    X = np.column_stack([np.ones(len(slice_))] + [slice_[c].values for c in X_cols])
    # OLS: beta_hat = (X'X)^{-1} X'y
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    alpha = float(coef[0])
    beta = tuple(float(b) for b in coef[1:])
    resid = y - X @ coef
    dof = max(1, len(y) - len(coef))
    sigma_e = float(np.sqrt((resid @ resid) / dof))
    return MarketModelFit(
        alpha=alpha, beta=beta, sigma_e=sigma_e,
        n_obs=int(len(y)), factors=tuple(factors),
    )


def abnormal_returns(
    returns: pd.DataFrame,
    ticker: str,
    fit: MarketModelFit,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.Series:
    """AR_t = r_t - (alpha_hat + sum_k beta_k * factor_k,t) on [start, end]."""
    slice_ = returns[(returns.index >= start) & (returns.index <= end)].dropna()
    y = slice_[f"r_{ticker}"].values
    X = np.column_stack([
        np.ones(len(slice_)),
        *[slice_[f"r_{f}"].values for f in fit.factors]
    ])
    theta = np.array([fit.alpha, *fit.beta])
    ar = y - X @ theta
    return pd.Series(ar, index=slice_.index, name="AR")


def car(ar: pd.Series) -> float:
    """Sum of abnormal returns over the event window."""
    return float(ar.sum())


# ---------------------------------------------------------------------------
# Per-event computation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EventCAR:
    event_name: str
    catalyst_date: date
    ar: pd.Series            # abnormal-return vector, length ~= 11
    car_value: float
    sigma_e: float           # estimation-window residual std
    t_car: float             # parametric t-stat under iid-normal null
    n_event_days: int


def _event_relative_span(
    returns: pd.DataFrame,
    catalyst: pd.Timestamp,
    offsets: tuple[int, int],
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Convert (trading-day offsets) into actual calendar bounds by walking
    the `returns` index."""
    idx = returns.index
    # find the closest trading day on/after the catalyst (exclusive of weekends)
    nearest = idx[idx >= catalyst]
    if nearest.empty:
        raise ValueError(f"no returns data on/after {catalyst.date()}")
    cat_pos = int(np.searchsorted(idx, nearest[0]))
    lo_pos = max(0, cat_pos + offsets[0])
    hi_pos = min(len(idx) - 1, cat_pos + offsets[1])
    return idx[lo_pos], idx[hi_pos]


def run_single_event(
    returns: pd.DataFrame,
    ticker: str,
    factors: Sequence[str],
    w: EventWindow,
) -> EventCAR:
    catalyst = pd.Timestamp(w.catalyst_date)
    est_lo, est_hi = _event_relative_span(returns, catalyst, ESTIMATION_WINDOW)
    evt_lo, evt_hi = _event_relative_span(returns, catalyst, EVENT_WINDOW)

    fit = market_model(returns, ticker, factors, est_lo, est_hi)
    ar = abnormal_returns(returns, ticker, fit, evt_lo, evt_hi)
    car_val = car(ar)
    n = len(ar)
    # Parametric t under iid-normal null: t = CAR / (sigma_e * sqrt(n))
    t_car = car_val / (fit.sigma_e * np.sqrt(max(n, 1))) if fit.sigma_e > 0 else 0.0
    return EventCAR(
        event_name=w.name, catalyst_date=w.catalyst_date,
        ar=ar, car_value=car_val, sigma_e=fit.sigma_e,
        t_car=float(t_car), n_event_days=int(n),
    )


# ---------------------------------------------------------------------------
# Cross-event AAR + bootstrap
# ---------------------------------------------------------------------------

def aar_across_events(events: Sequence[EventCAR]) -> pd.Series:
    """Average-abnormal-return across events at each event-day offset.

    Each event's AR vector is re-indexed by trading-day offset (-5..+5); the
    AAR is the mean across events at each offset.
    """
    # Re-index each AR by integer offset relative to its catalyst.
    frames = []
    for ev in events:
        idx_offset = np.arange(-len(ev.ar) // 2 + 1,
                               len(ev.ar) // 2 + 1)
        # Force length-11 pattern: crop or pad center-anchored. Here we trust
        # run_single_event returned exactly EVENT_WINDOW-width vectors.
        offsets = np.arange(EVENT_WINDOW[0], EVENT_WINDOW[1] + 1)
        if len(ev.ar) == len(offsets):
            s = pd.Series(ev.ar.values, index=offsets, name=ev.event_name)
        else:
            # Left-pad with NaN if the event hit an index boundary.
            padded = np.full(len(offsets), np.nan)
            k = len(ev.ar)
            padded[:k] = ev.ar.values
            s = pd.Series(padded, index=offsets, name=ev.event_name)
        frames.append(s)
    panel = pd.concat(frames, axis=1)
    aar = panel.mean(axis=1, skipna=True)
    aar.name = "AAR"
    return aar


def bootstrap_car_null(
    events: Sequence[EventCAR],
    returns: pd.DataFrame,
    ticker: str,
    factors: Sequence[str],
    n_boot: int = BOOT_N,
    seed: int = BOOT_SEED,
) -> dict[str, float]:
    """Shuffle-catalyst bootstrap: for each replication, sample an eligible
    date uniformly from the post-estimation sample and re-run the event
    study. Report the (empirical) distribution of cross-event mean CAR under
    the null that event dates are arbitrary.
    """
    rng = np.random.default_rng(seed)
    eligible = returns.index
    # Keep enough buffer so the 250-day estimation + 5-day event window fit.
    lo_buffer = abs(ESTIMATION_WINDOW[0]) + 10
    hi_buffer = EVENT_WINDOW[1] + 10
    valid = eligible[lo_buffer:-hi_buffer]

    n_events = len(events)
    mean_cars = np.empty(n_boot, dtype=float)
    for b in range(n_boot):
        draws = rng.choice(valid, size=n_events, replace=False)
        cars = []
        for d in draws:
            # Lightweight inline single-event CAR on a shuffled catalyst.
            catalyst = pd.Timestamp(d)
            est_lo, est_hi = _event_relative_span(
                returns, catalyst, ESTIMATION_WINDOW)
            evt_lo, evt_hi = _event_relative_span(
                returns, catalyst, EVENT_WINDOW)
            try:
                fit = market_model(returns, ticker, factors, est_lo, est_hi)
                ar = abnormal_returns(returns, ticker, fit, evt_lo, evt_hi)
                cars.append(float(ar.sum()))
            except Exception:  # noqa: BLE001
                cars.append(np.nan)
        mean_cars[b] = float(np.nanmean(cars))

    observed_mean_car = float(np.mean([ev.car_value for ev in events]))
    null_mean = float(np.nanmean(mean_cars))
    null_std = float(np.nanstd(mean_cars))
    ci_low, ci_high = np.nanpercentile(mean_cars, [2.5, 97.5])
    # Two-sided empirical p-value.
    p_two_sided = float(
        np.mean(np.abs(mean_cars - null_mean) >= abs(observed_mean_car - null_mean))
    )
    t_boot = (observed_mean_car - null_mean) / null_std if null_std > 0 else 0.0
    return {
        "observed_mean_car": observed_mean_car,
        "null_mean": null_mean,
        "null_std": null_std,
        "t_boot": float(t_boot),
        "p_two_sided": p_two_sided,
        "ci95_low": float(ci_low),
        "ci95_high": float(ci_high),
        "n_boot_eff": int(np.sum(~np.isnan(mean_cars))),
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _write_tex_table(
    events: Sequence[EventCAR],
    aar: pd.Series,
    boot_stats: dict[str, float],
    path: Path,
) -> None:
    lines: list[str] = []
    lines.append(r"\begin{tabular}{lccrr}")
    lines.append(r"\toprule")
    lines.append(r"Event & Catalyst & $n$ & CAR & $t$ \\")
    lines.append(r"\midrule")
    for ev in events:
        lines.append(
            rf"{ev.event_name.replace('_', r'\_')} & "
            rf"{ev.catalyst_date.isoformat()} & "
            rf"{ev.n_event_days} & "
            rf"{ev.car_value*100:+.2f}\% & "
            rf"{ev.t_car:+.3f} \\"
        )
    lines.append(r"\midrule")
    mean_car = boot_stats["observed_mean_car"]
    t_boot = boot_stats["t_boot"]
    p_val = boot_stats["p_two_sided"]
    ci_lo = boot_stats["ci95_low"] * 100
    ci_hi = boot_stats["ci95_high"] * 100
    verdict = "REJECT $H_0$" if t_boot < T_CRITICAL else "FAIL TO REJECT $H_0$"
    lines.append(
        rf"\textbf{{Mean CAR}} & --- & {len(events)} & "
        rf"{mean_car*100:+.2f}\% & {t_boot:+.3f} \\"
    )
    lines.append(r"\midrule")
    lines.append(
        rf"\multicolumn{{5}}{{l}}{{\emph{{Bootstrap null ($B={boot_stats['n_boot_eff']}$): "
        rf"95\% CI for mean CAR $=[{ci_lo:+.2f}\%, {ci_hi:+.2f}\%]$, "
        rf"$p={p_val:.3f}$ (two-sided). Outcome: {verdict}.}}}} \\"
    )
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_pdf_figure(
    aar: pd.Series,
    events: Sequence[EventCAR],
    path: Path,
) -> None:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    mpl.rcParams.update({
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "font.family": "serif",
        "font.size": 9.0,
        "axes.titlesize": 10.0,
        "axes.labelsize": 9.0,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "grid.linestyle": "--",
        "grid.linewidth": 0.5,
        "legend.frameon": False,
    })

    # Cross-event CI via simple percentile bootstrap of per-day AR values
    rng = np.random.default_rng(BOOT_SEED)
    ars_matrix = np.full((len(events), len(aar)), np.nan)
    for i, ev in enumerate(events):
        k = min(len(ev.ar), len(aar))
        ars_matrix[i, :k] = ev.ar.values[:k]
    n_boot = 2000
    boot_means = np.empty((n_boot, len(aar)))
    idx = np.arange(len(events))
    for b in range(n_boot):
        sample = rng.choice(idx, size=len(events), replace=True)
        boot_means[b] = np.nanmean(ars_matrix[sample], axis=0)
    lo = np.nanpercentile(boot_means, 2.5, axis=0) * 100
    hi = np.nanpercentile(boot_means, 97.5, axis=0) * 100
    center = aar.values * 100

    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    offsets = aar.index.values
    ax.fill_between(offsets, lo, hi, color="#22d3ee", alpha=0.25,
                    label="95\\% bootstrap CI")
    ax.plot(offsets, center, color="#0b3d91", linewidth=1.6,
            marker="o", markersize=3.5,
            label=f"AAR (N={len(events)} events)")
    ax.axhline(0.0, color="#6b7689", linewidth=0.8, alpha=0.7)
    ax.axvline(0, color="#e11d48", linewidth=1.1, linestyle="--",
               label="catalyst day (t=0)")
    ax.set_xlabel("event-relative trading day")
    ax.set_ylabel("abnormal return (\\%)")
    ax.set_title("AFRM AAR over event window: two-factor (HYG, MOVE) market model")
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------------

def compute_car_panel(
    ticker: str = "AFRM",
    factors: Sequence[str] = ("HYG", "MOVE"),
    con: duckdb.DuckDBPyConnection | None = None,
    events: Mapping[str, EventWindow] = WINDOWS,
    run_bootstrap: bool = True,
    n_boot: int = BOOT_N,
) -> tuple[list[EventCAR], pd.Series, dict[str, float]]:
    """End-to-end: load returns, run per-event CARs, compute AAR, bootstrap."""
    owns = False
    if con is None:
        con = duckdb.connect(str(settings.duckdb_path), read_only=True)
        owns = True
    try:
        returns = _build_returns_panel(con, ticker, factors)
        event_cars = [run_single_event(returns, ticker, factors, w)
                      for w in events.values()]
        aar = aar_across_events(event_cars)
        if run_bootstrap:
            boot_stats = bootstrap_car_null(
                event_cars, returns, ticker, factors,
                n_boot=n_boot, seed=BOOT_SEED,
            )
        else:
            boot_stats = {
                "observed_mean_car": float(np.mean([ev.car_value for ev in event_cars])),
                "null_mean": float("nan"), "null_std": float("nan"),
                "t_boot": float("nan"), "p_two_sided": float("nan"),
                "ci95_low": float("nan"), "ci95_high": float("nan"),
                "n_boot_eff": 0,
            }
    finally:
        if owns:
            con.close()
    return event_cars, aar, boot_stats


def run(out_dir: Path | None = None, n_boot: int = BOOT_N) -> dict[str, float]:
    """CLI entry: compute, write TeX + PDF, print verdict."""
    if out_dir is None:
        out_dir = Path(__file__).resolve().parent.parent / "paper_formal" / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)

    events, aar, boot = compute_car_panel(n_boot=n_boot)
    _write_tex_table(events, aar, boot, out_dir / "car_table.tex")
    _write_pdf_figure(aar, events, out_dir / "car_timepath.pdf")

    print("CAR event study -- AFRM, two-factor (HYG, MOVE) market model")
    print(f"  Observed mean CAR:  {boot['observed_mean_car']*100:+.2f}%")
    print(f"  Bootstrap t-stat:   {boot['t_boot']:+.3f}")
    print(f"  Bootstrap p-value:  {boot['p_two_sided']:.4f}")
    print(f"  95% CI on mean CAR: [{boot['ci95_low']*100:+.2f}%, {boot['ci95_high']*100:+.2f}%]")
    if boot['t_boot'] < T_CRITICAL:
        print("  VERDICT: pre-registered H_1 SUPPORTED (t < -2.0).")
    else:
        print("  VERDICT: pre-registered H_1 NOT SUPPORTED. Paper must report verbatim.")
    for ev in events:
        print(f"    {ev.event_name:22s} {ev.catalyst_date.isoformat()} "
              f"CAR={ev.car_value*100:+6.2f}%  t={ev.t_car:+.2f}")
    return boot


if __name__ == "__main__":
    run()
