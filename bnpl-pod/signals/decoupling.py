"""
Cross-channel decoupling analysis (paper §7.X, v2.1 additions).

Phase B.1 of the non-hallucinating alpha pivot. This module tests whether the
consumer-channel stress signal decouples from the market-channel stress signal
AROUND the five 2022-2025 BNPL regulatory catalysts --- a direct falsification
test of the Hong-Stein (1999) information-diffusion hypothesis stated in
paper §4 Literature Review.

Pre-registered hypothesis
-------------------------
H_0: rolling 30-day correlation rho(consumer-BSI, market-BSI) is identical in
     three disjoint event-relative windows:
        pre-catalyst  (t - 90, t - 7)
        on-catalyst   (t -  5, t + 5)
        post-catalyst (t +  7, t + 90)
     across all five `backtest.event_study.WINDOWS` catalysts.
H_1: rho_on > rho_pre + 2 * SE(rho_pre) AND rho_on > rho_post + 2 * SE(rho_post)
     across at least 4 of 5 events.
Rejection region (pre-registered): fewer than 4 of 5 events satisfy the pair of
     one-sided 2*SE tests above.

Pillar split
------------
Consumer-side subindex: cfpb, trends, appstore, vitality
   (Reddit is dropped --- 0 rows in warehouse as of 2026-04. Disclosed in
   paper footnote.)
Market-side subindex:   move, macro

Each subindex re-uses the canonical EWMA-z pillar series from
`signals.bsi.compute_bsi_from_warehouse`, renormalised within the subindex so
the correlation operates on two comparable z-scaled composites rather than
scale-mismatched levels.

Outputs
-------
`paper_formal/figures/decoupling.pdf`    -- 5 small-multiple panels, one per
                                            event, showing rolling-30d rho with
                                            pre/on/post window shading.
`paper_formal/figures/decoupling_table.tex` -- 5 rows (events) x 3 columns
                                            (pre/on/post mean rho) + bottom row
                                            pre-registered-test-outcome summary.

Non-hallucination discipline
----------------------------
`run()` writes the table REGARDLESS of which direction the result goes. If the
pre-registered H_1 fails, the paper §7.X text must report the failure
verbatim. No "try another window" loop is hard-coded.

Author: Siddharth Verma, UIUC, FIN 580 Spring 2026 cohort.
Provenance: Phase B.1 of v2.1 non-hallucinating alpha pivot, 2026-04-23.
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
from signals.bsi import ALL_PILLARS, compute_bsi_from_warehouse

# ---------------------------------------------------------------------------
# Pre-registered pillar split --- frozen for the paper.
# ---------------------------------------------------------------------------

CONSUMER_PILLARS: tuple[str, ...] = ("cfpb", "trends", "appstore", "vitality")
MARKET_PILLARS: tuple[str, ...] = ("move", "macro")

# Reddit is dropped from CONSUMER_PILLARS: 0 rows in bsi_daily.c_reddit across
# the 2022-2025 sample. Drop is disclosed in paper §7.X footnote, not swept
# under the rug.
DROPPED_PILLAR = "reddit"

# Pre-registered event windows (trading-day offsets).
PRE_WINDOW = (-90, -7)
ON_WINDOW = (-5, 5)
POST_WINDOW = (7, 90)

# Pre-registered rolling window for the correlation.
ROLLING_DAYS = 30

# Pre-registered rejection threshold for H_1.
MIN_EVENTS_FOR_H1 = 4


# ---------------------------------------------------------------------------
# Subindex aggregation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DecouplingSubindices:
    """Daily two-series panel: consumer-side composite z and market-side z."""
    dates: pd.DatetimeIndex
    consumer: pd.Series
    market: pd.Series


def _subindex_z(
    bsi_out: pd.DataFrame,
    pillars: Sequence[str],
) -> pd.Series:
    """Aggregate per-pillar z-scores into a single subindex z-series.

    Strategy: mean of available per-pillar gated z-scores, weighted by the
    pillar's gamma (so a pillar with gamma=0 does not contribute). If NO
    pillars in the subindex carry gamma>0 on a given day, the subindex is NaN
    that day --- the correlation will skip it.
    """
    z_cols = [f"z_{p}" for p in pillars if f"z_{p}" in bsi_out.columns]
    g_cols = [f"gamma_{p}" for p in pillars if f"gamma_{p}" in bsi_out.columns]
    if not z_cols:
        raise ValueError(f"no z_<pillar> columns present for {pillars}")

    z = bsi_out[z_cols].fillna(0.0)
    g = bsi_out[g_cols].fillna(0.0)
    # Re-label g columns to match z columns so alignment is by pillar.
    g.columns = z_cols
    weighted = (z * g).sum(axis=1)
    total_weight = g.sum(axis=1)
    out = weighted / total_weight.replace(0.0, np.nan)
    out.name = "subindex_z"
    return out


def consumer_subindex(bsi_out: pd.DataFrame) -> pd.Series:
    """Consumer-side composite z-score (cfpb, trends, appstore, vitality)."""
    return _subindex_z(bsi_out, CONSUMER_PILLARS)


def market_subindex(bsi_out: pd.DataFrame) -> pd.Series:
    """Market-side composite z-score (move, macro)."""
    return _subindex_z(bsi_out, MARKET_PILLARS)


# ---------------------------------------------------------------------------
# Rolling correlation
# ---------------------------------------------------------------------------

def rolling_correlation(
    consumer: pd.Series,
    market: pd.Series,
    window: int = ROLLING_DAYS,
) -> pd.Series:
    """Rolling Pearson correlation between the two subindices.

    min_periods = window // 2 so the output begins once half the window has
    observations. NaN-preserving: if either series has a NaN on day t, the
    rolling window simply excludes that day from its count.
    """
    df = pd.concat({"c": consumer, "m": market}, axis=1).dropna(how="any")
    rho = df["c"].rolling(window=window, min_periods=window // 2).corr(df["m"])
    rho.name = f"rho_{window}d"
    return rho


# ---------------------------------------------------------------------------
# Event-relative window aggregation + H_1 test
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EventCorrelationRow:
    event_name: str
    catalyst_date: date
    rho_pre: float
    rho_on: float
    rho_post: float
    se_pre: float
    se_post: float
    h1_pre_pass: bool
    h1_post_pass: bool

    @property
    def h1_both_pass(self) -> bool:
        return self.h1_pre_pass and self.h1_post_pass


def _window_stats(rho: pd.Series, catalyst: pd.Timestamp,
                  offset_lo: int, offset_hi: int) -> tuple[float, float, int]:
    """Return (mean, standard error, n_obs) of rho on the calendar-day
    window [catalyst + offset_lo, catalyst + offset_hi]."""
    lo = catalyst + pd.Timedelta(days=offset_lo)
    hi = catalyst + pd.Timedelta(days=offset_hi)
    slice_ = rho[(rho.index >= lo) & (rho.index <= hi)].dropna()
    n = int(len(slice_))
    if n == 0:
        return float("nan"), float("nan"), 0
    mu = float(slice_.mean())
    # Standard error of the mean; floor n at 1 to avoid division-by-zero.
    se = float(slice_.std(ddof=1) / np.sqrt(max(n, 1))) if n > 1 else float("nan")
    return mu, se, n


def event_window_correlations(
    rho: pd.Series,
    events: Mapping[str, EventWindow] = WINDOWS,
    pre: tuple[int, int] = PRE_WINDOW,
    on: tuple[int, int] = ON_WINDOW,
    post: tuple[int, int] = POST_WINDOW,
) -> pd.DataFrame:
    """Compute pre/on/post mean-rho per event + the two pre-registered
    one-sided H_1 pass/fail flags.

    Returns a DataFrame with one row per event, columns:
        rho_pre, rho_on, rho_post, se_pre, se_post,
        h1_pre_pass, h1_post_pass, h1_both_pass
    """
    rows: list[EventCorrelationRow] = []
    for name, w in events.items():
        cat = pd.Timestamp(w.catalyst_date)
        rho_pre, se_pre, _ = _window_stats(rho, cat, *pre)
        rho_on, _, _ = _window_stats(rho, cat, *on)
        rho_post, se_post, _ = _window_stats(rho, cat, *post)

        h1_pre = (
            not np.isnan(rho_on)
            and not np.isnan(rho_pre)
            and not np.isnan(se_pre)
            and (rho_on > rho_pre + 2.0 * se_pre)
        )
        h1_post = (
            not np.isnan(rho_on)
            and not np.isnan(rho_post)
            and not np.isnan(se_post)
            and (rho_on > rho_post + 2.0 * se_post)
        )

        rows.append(EventCorrelationRow(
            event_name=name,
            catalyst_date=w.catalyst_date,
            rho_pre=rho_pre, rho_on=rho_on, rho_post=rho_post,
            se_pre=se_pre, se_post=se_post,
            h1_pre_pass=h1_pre, h1_post_pass=h1_post,
        ))

    df = pd.DataFrame([r.__dict__ for r in rows])
    df["h1_both_pass"] = df["h1_pre_pass"] & df["h1_post_pass"]
    return df


# ---------------------------------------------------------------------------
# Output: TeX table + PDF figure
# ---------------------------------------------------------------------------

def _write_tex_table(df: pd.DataFrame, path: Path) -> None:
    """Emit a booktabs-style LaTeX tabular body (no \\begin{table} wrapper;
    that is placed by the paper so the caller controls floats)."""
    n_pass = int(df["h1_both_pass"].sum())
    verdict = (
        f"pre-registered H$_1$ SUPPORTED at $\\ge${MIN_EVENTS_FOR_H1}/5"
        if n_pass >= MIN_EVENTS_FOR_H1
        else f"pre-registered H$_1$ NOT SUPPORTED: {n_pass}/5 events satisfy both tests"
    )

    lines: list[str] = []
    lines.append(r"\begin{tabular}{lcccc}")
    lines.append(r"\toprule")
    lines.append(r"Event & Catalyst & $\bar{\rho}_{\mathrm{pre}}$ & "
                 r"$\bar{\rho}_{\mathrm{on}}$ & $\bar{\rho}_{\mathrm{post}}$ \\")
    lines.append(r"\midrule")
    for _, row in df.iterrows():
        lines.append(
            rf"{row['event_name'].replace('_', r'\_')} & "
            rf"{row['catalyst_date'].isoformat()} & "
            rf"{row['rho_pre']:+.3f} & "
            rf"{row['rho_on']:+.3f} & "
            rf"{row['rho_post']:+.3f} \\"
        )
    lines.append(r"\midrule")
    lines.append(rf"\multicolumn{{5}}{{l}}{{\emph{{Outcome: {verdict}}}}} \\")
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_pdf_figure(
    rho: pd.Series,
    df: pd.DataFrame,
    path: Path,
    events: Mapping[str, EventWindow] = WINDOWS,
) -> None:
    """5-panel small-multiple: one subplot per event, rolling rho with
    pre/on/post window shading."""
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

    names = list(events.keys())
    n = len(names)
    fig, axes = plt.subplots(n, 1, figsize=(9.0, 2.1 * n), sharey=True)
    if n == 1:
        axes = [axes]

    for ax, name in zip(axes, names):
        w = events[name]
        cat = pd.Timestamp(w.catalyst_date)
        lo = cat + pd.Timedelta(days=PRE_WINDOW[0] - 15)
        hi = cat + pd.Timedelta(days=POST_WINDOW[1] + 15)
        sub = rho[(rho.index >= lo) & (rho.index <= hi)]
        ax.plot(sub.index, sub.values, color="#0b3d91", linewidth=1.3,
                label=f"rolling {ROLLING_DAYS}d $\\rho$")
        ax.axhline(0.0, color="#6b7689", linewidth=0.8, alpha=0.6)
        # Shade the three event-relative windows.
        for (lo_off, hi_off), color, alpha in [
            (PRE_WINDOW, "#6b7689", 0.10),
            (ON_WINDOW, "#e11d48", 0.18),
            (POST_WINDOW, "#22d3ee", 0.10),
        ]:
            ax.axvspan(cat + pd.Timedelta(days=lo_off),
                       cat + pd.Timedelta(days=hi_off),
                       color=color, alpha=alpha, linewidth=0)
        ax.axvline(cat, color="#e11d48", linewidth=1.1, linestyle="--")
        ax.set_title(f"{name}  (catalyst {w.catalyst_date.isoformat()})")
        ax.set_ylabel(r"$\rho$")
        ax.set_ylim(-1.05, 1.05)

    axes[-1].set_xlabel("date")
    fig.suptitle(
        f"Cross-channel decoupling: consumer-side vs market-side subindex "
        f"(rolling {ROLLING_DAYS}d Pearson $\\rho$)",
        y=1.002, fontsize=11.0,
    )
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------------

def compute_decoupling(
    con: duckdb.DuckDBPyConnection | None = None,
) -> tuple[pd.Series, pd.DataFrame]:
    """Return (rolling_rho_series, event_table)."""
    bsi_out = compute_bsi_from_warehouse(conn=con)
    cons = consumer_subindex(bsi_out)
    mkt = market_subindex(bsi_out)
    rho = rolling_correlation(cons, mkt)
    df = event_window_correlations(rho)
    return rho, df


def run(out_dir: Path | None = None) -> pd.DataFrame:
    """CLI entry: compute, write PDF + TeX, print pre-registered verdict."""
    if out_dir is None:
        out_dir = Path(__file__).resolve().parent.parent / "paper_formal" / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)

    rho, df = compute_decoupling()
    _write_tex_table(df, out_dir / "decoupling_table.tex")
    _write_pdf_figure(rho, df, out_dir / "decoupling.pdf")

    n_pass = int(df["h1_both_pass"].sum())
    print(f"decoupling: {n_pass}/{len(df)} events satisfy both pre-registered "
          f"one-sided tests (threshold for H_1: >= {MIN_EVENTS_FOR_H1}).")
    if n_pass >= MIN_EVENTS_FOR_H1:
        print("  VERDICT: pre-registered H_1 SUPPORTED.")
    else:
        print("  VERDICT: pre-registered H_1 NOT SUPPORTED. Paper must report "
              "this outcome verbatim per non-hallucination discipline.")
    print(df.to_string(index=False, float_format=lambda x: f"{x:+.4f}"))
    return df


if __name__ == "__main__":
    run()
