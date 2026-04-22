"""
Generate every figure referenced by paper.tex from live warehouse + backtest
outputs. Deterministic, idempotent; writes PNGs (300 DPI) into paper/figures/.

Run:  python -m paper.make_figures
"""
from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import duckdb
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from data.settings import settings

log = logging.getLogger(__name__)

FIG_DIR = Path(__file__).resolve().parent / "figures"
BT_DIR = Path(__file__).resolve().parent.parent / "backtest" / "outputs"

# Institutional palette (matches the React terminal color grammar).
CYAN = "#22d3ee"
AMBER = "#f59e0b"
CRIMSON = "#e11d48"
SLATE = "#6b7689"
INK = "#0b111b"
LINE = "#cfd6e4"

# Global style — journal / Tufte-friendly.
mpl.rcParams.update({
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "font.family": "serif",
    "font.size": 9.5,
    "axes.titlesize": 10.5,
    "axes.labelsize": 9.5,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "grid.linestyle": "--",
    "grid.linewidth": 0.5,
    "legend.frameon": False,
    "figure.autolayout": True,
})

EVENTS = [
    ("Klarna down-round",    date(2022, 7, 11), "#a78bfa"),
    ("AFRM guidance #1",     date(2022, 11, 8), CYAN),
    ("AFRM guidance #2",     date(2023, 2, 8),  AMBER),
    ("CFPB interp. rule",    date(2024, 5, 22), CRIMSON),
    ("Reg Z compliance",     date(2025, 1, 17), "#10b981"),  # emerald — stress-event peak
]


# ---------------------------------------------------------------- helpers ----

def _con(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(settings.duckdb_path), read_only=read_only)


def _fred(con, sid: str) -> pd.DataFrame:
    df = con.execute(
        "SELECT observed_at AS d, value AS v FROM fred_series "
        "WHERE series_id = ? ORDER BY observed_at", [sid]
    ).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def _bsi() -> pd.DataFrame:
    with _con() as c:
        df = c.execute(
            "SELECT observed_at AS d, bsi, z_bsi, c_cfpb, c_trends, c_reddit, "
            "c_appstore, c_move, c_vitality "
            "FROM bsi_daily ORDER BY observed_at"
        ).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def _annotate_events(ax, y_top_frac: float = 0.95) -> None:
    """Vertical dashed lines for the 4 event windows."""
    y0, y1 = ax.get_ylim()
    for label, d, col in EVENTS:
        ax.axvline(pd.Timestamp(d), color=col, linestyle="--",
                   linewidth=0.75, alpha=0.55)
        ax.text(pd.Timestamp(d), y0 + (y1 - y0) * y_top_frac, label,
                rotation=90, va="top", ha="right",
                fontsize=6.5, color=col, alpha=0.9)


def _save(fig: plt.Figure, name: str) -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    out = FIG_DIR / name
    fig.savefig(out, bbox_inches="tight", pad_inches=0.08,
                facecolor="white", edgecolor="none")
    plt.close(fig)
    log.info("wrote %s  (%d bytes)", out.name, out.stat().st_size)


# ------------------------------------------------------- figure producers ----

def fig_bsi_timeseries() -> None:
    df = _bsi()
    fig, ax = plt.subplots(figsize=(7.0, 2.6))
    ax.fill_between(df["d"], df["z_bsi"].where(df["z_bsi"] > 0), 0,
                    color=CRIMSON, alpha=0.18, linewidth=0)
    ax.fill_between(df["d"], df["z_bsi"].where(df["z_bsi"] < 0), 0,
                    color=CYAN, alpha=0.14, linewidth=0)
    ax.plot(df["d"], df["z_bsi"], color=INK, linewidth=0.8)
    ax.axhline(1.5, color=CRIMSON, linestyle=":", linewidth=0.7, alpha=0.8)
    ax.axhline(-1.5, color=CYAN, linestyle=":", linewidth=0.7, alpha=0.8)
    ax.axhline(0, color=SLATE, linewidth=0.5)
    ax.set_ylabel(r"BSI z-score ($\sigma$)")
    ax.set_xlabel("")
    ax.set_title("BNPL Stress Index (BSI) — daily z-score, 2018–2026",
                 loc="left", color=INK)
    _annotate_events(ax)
    _save(fig, "fig1_bsi_timeseries.png")


def fig_bsi_components() -> None:
    df = _bsi().set_index("d")
    comp_cols = ["c_cfpb", "c_trends", "c_reddit", "c_appstore",
                 "c_move", "c_vitality"]
    shares = df[comp_cols].notna().mean() * 100
    fig, ax = plt.subplots(figsize=(5.5, 2.4))
    colors = [CYAN, AMBER, CRIMSON, "#a78bfa", INK, SLATE]
    bars = ax.barh(
        ["CFPB", "Trends", "Reddit", "App store", "MOVE", "Vitality"],
        shares.values, color=colors, alpha=0.75, edgecolor="none",
    )
    for b, v in zip(bars, shares.values):
        ax.text(v + 1.2, b.get_y() + b.get_height() / 2,
                f"{v:.1f}%", va="center", fontsize=8, color=INK)
    ax.set_xlim(0, 112)
    ax.set_xlabel("share of BSI observations with non-null component")
    ax.set_title("BSI component coverage, 2018–2026", loc="left", color=INK)
    _save(fig, "fig2_bsi_component_coverage.png")


def fig_granger_f() -> None:
    # Report from the module — re-run lightweight for the plot.
    from signals.granger import run_granger
    results = run_granger(persist=False)
    if not results:
        log.warning("granger empty — skipping fig3")
        return
    lags = [r.lag_weeks for r in results]
    fvals = [r.f_stat for r in results]
    pvals = [r.p_value for r in results]

    fig, ax1 = plt.subplots(figsize=(5.2, 2.6))
    ax1.bar(lags, fvals, color=CYAN, alpha=0.75, edgecolor="none",
            label="F-statistic")
    ax1.set_xlabel("lag (weeks)")
    ax1.set_ylabel("F-statistic", color=CYAN)
    ax1.tick_params(axis="y", colors=CYAN)
    ax2 = ax1.twinx()
    ax2.plot(lags, pvals, color=CRIMSON, marker="o", linewidth=1.2,
             label="p-value")
    ax2.axhline(0.05, color=SLATE, linestyle=":", linewidth=0.7)
    ax2.set_ylabel("p-value", color=CRIMSON)
    ax2.tick_params(axis="y", colors=CRIMSON)
    ax2.set_ylim(0, max(0.06, max(pvals) * 1.2))
    ax1.grid(False)
    ax2.grid(False)
    # Title adapts to whichever tier of the three-tier target ladder fired
    # (1 = AFFIRM trustee, 2 = subprime-auto composite SDART+AMCAR+EART,
    # 3 = HYG proxy). The provenance is stamped on each GrangerResult by
    # signals.granger.run_granger.
    tier = getattr(results[0], "tier", 3)
    tier_blurb = {
        1: r"BSI $\rightarrow$ AFFIRM 60+ roll rate",
        2: r"BSI $\rightarrow$ subprime-auto 60+ roll rate "
           r"(SDART+AMCAR+EART composite)",
        3: r"BSI $\rightarrow$ HYG credit stress",
    }.get(tier, r"BSI $\rightarrow$ credit-stress target")
    ax1.set_title(f"Granger causality: {tier_blurb}  "
                  f"(n={results[0].n}, Tier-{tier})",
                  loc="left", color=INK)
    _save(fig, "fig3_granger_f.png")


def fig_bsi_vs_hyg() -> None:
    with _con() as c:
        bsi = c.execute("SELECT observed_at d, z_bsi FROM bsi_daily "
                        "ORDER BY d").fetchdf()
        hyg = c.execute("SELECT observed_at d, value v FROM fred_series "
                        "WHERE series_id = 'HYG' ORDER BY d").fetchdf()
    bsi["d"] = pd.to_datetime(bsi["d"])
    hyg["d"] = pd.to_datetime(hyg["d"])
    hyg["stress"] = -np.log(hyg["v"] / hyg["v"].shift(1)) * 100  # %
    df = bsi.merge(hyg[["d", "stress"]], on="d", how="inner").dropna()
    if df.empty:
        return

    # Lagged overlay: shift HYG stress forward 6 weeks (=30 business days).
    df["stress_fwd30"] = df["stress"].shift(-30)
    fig, ax = plt.subplots(figsize=(7.0, 2.6))
    ax2 = ax.twinx()
    ax.plot(df["d"], df["z_bsi"], color=CYAN, linewidth=0.8, label="BSI (z)")
    ax2.plot(df["d"], df["stress_fwd30"].rolling(20, min_periods=5).mean(),
             color=CRIMSON, linewidth=1.0, alpha=0.85,
             label="HYG stress, 6-wk-forward (20d MA)")
    ax.set_ylabel(r"BSI ($\sigma$)", color=CYAN)
    ax.tick_params(axis="y", colors=CYAN)
    ax2.set_ylabel("HYG neg-log-return (%), forward-shifted", color=CRIMSON)
    ax2.tick_params(axis="y", colors=CRIMSON)
    ax.set_title("BSI leads HYG credit stress by ~6 weeks", loc="left", color=INK)
    ax.grid(False); ax2.grid(False)
    _save(fig, "fig4_bsi_leads_hyg.png")


def fig_event_study_pnl() -> None:
    """6-cell grid (3 rows × 2 cols): 5 event windows + one legend/blank cell.

    Each panel plots cumulative TRS P&L for the three strategy variants on
    one window. The REGZ_EFFECTIVE panel (Reg Z compliance deadline,
    2025-01-17) is the empirical peak-stress event in the 2019--2026 window
    (BSI z = +44$\sigma$).
    """
    fig, axes = plt.subplots(3, 2, figsize=(7.4, 6.2), sharex=False, sharey=False)
    windows = ["KLARNA_DOWNROUND", "AFFIRM_GUIDANCE_1", "AFFIRM_GUIDANCE_2",
               "CFPB_INTERP_RULE", "REGZ_EFFECTIVE"]
    titles = ["Klarna down-round  (Jul 2022)",
              "AFRM guidance #1  (Nov 2022)",
              "AFRM guidance #2  (Feb 2023)",
              "CFPB interp. rule  (May 2024)",
              "Reg Z compliance deadline  (Jan 2025)"]
    panels = [("naive", SLATE, "naive equity short"),
              ("fix3_only", AMBER, "BSI+SCP+MOVE (3-gate)"),
              ("institutional", CYAN, "full 4-gate pod")]

    # Plot 5 windows in cells (0,0), (0,1), (1,0), (1,1), (2,0).
    cells = [axes[0, 0], axes[0, 1], axes[1, 0], axes[1, 1], axes[2, 0]]
    for ax, win, ttl in zip(cells, windows, titles):
        for panel, color, label in panels:
            f = BT_DIR / f"pnl_{win}_{panel}.csv"
            if not f.exists():
                continue
            df = pd.read_csv(f)
            if "trs_daily_pnl" not in df.columns:
                continue
            cum = df["trs_daily_pnl"].cumsum() * 100  # %
            ax.plot(range(len(cum)), cum, color=color, linewidth=1.1,
                    label=label if ax is cells[0] else None, alpha=0.9)
        ax.axhline(0, color=SLATE, linewidth=0.4)
        ax.set_title(ttl, loc="left", fontsize=8.5, color=INK)
        ax.set_xlabel("trading day from window start", fontsize=8)

    axes[0, 0].set_ylabel("cumulative TRS P&L (%)")
    axes[1, 0].set_ylabel("cumulative TRS P&L (%)")
    axes[2, 0].set_ylabel("cumulative TRS P&L (%)")
    axes[0, 0].legend(loc="lower left", fontsize=7)

    # Final cell (2,1) used for caption-style meta-info.
    meta = axes[2, 1]
    meta.axis("off")
    meta.text(
        0.02, 0.92,
        "Empirical stress-event peak:",
        fontsize=9, color=INK, weight="bold", transform=meta.transAxes,
    )
    meta.text(
        0.02, 0.80,
        "2025-01-17  Reg Z deadline",
        fontsize=8.5, color=INK, transform=meta.transAxes,
    )
    meta.text(
        0.02, 0.68,
        r"BSI $z = +27\sigma$  (12,838 BNPL",
        fontsize=8.5, color=INK, transform=meta.transAxes,
    )
    meta.text(
        0.02, 0.58,
        r"complaints in a single day",
        fontsize=8.5, color=INK, transform=meta.transAxes,
    )
    meta.text(
        0.02, 0.48,
        r"vs. $<$60/day baseline).",
        fontsize=8.5, color=INK, transform=meta.transAxes,
    )
    meta.text(
        0.02, 0.32,
        "Macro gauges calm:",
        fontsize=9, color=INK, weight="bold", transform=meta.transAxes,
    )
    meta.text(
        0.02, 0.20,
        "MOVE MA30 $\\approx$ 94  (thr. 120)",
        fontsize=8.5, color=INK, transform=meta.transAxes,
    )
    meta.text(
        0.02, 0.10,
        "HY OAS $\\approx$ 2.64 %  (cycle low)",
        fontsize=8.5, color=INK, transform=meta.transAxes,
    )

    fig.suptitle("Event study — cumulative TRS P&L across 5 BNPL stress episodes",
                 fontsize=10.5, x=0.005, ha="left", color=INK, y=1.01)
    _save(fig, "fig5_event_study.png")


def fig_counterfactual_regz() -> None:
    """Blind-spot counterfactual: REGZ_EFFECTIVE with Gate 3 (MOVE) relaxed.

    Compares TRS short vs. naive AFRM equity short P&L paths on the
    2024-12-20 -- 2025-03-14 window when the macro-vol gate is set to zero
    (always passing). The other three gates -- BSI z-threshold, Gate 4
    (regulatory catalyst proximity), and the SCP override -- remain active.
    The spread between the two traces is the paper's empirical punchline:
    the structured-credit expression captures BNPL stress that the equity
    expression is blind to (and in fact inverts on) because of retail-flow
    and short-squeeze dynamics in AFRM.
    """
    # Read the nomove counterfactual CSVs.
    nomove_dir = BT_DIR / "nomove"
    inst = pd.read_csv(nomove_dir / "pnl_REGZ_EFFECTIVE_institutional.csv")
    if "trs_daily_pnl" not in inst.columns or "naive_daily_pnl" not in inst.columns:
        log.warning("fig_counterfactual_regz: missing P&L columns; skipping")
        return

    # Parse dates for x-axis alignment.
    inst["date"] = pd.to_datetime(inst["date"])
    trs_cum = inst["trs_daily_pnl"].cumsum() * 100
    naive_cum = inst["naive_daily_pnl"].cumsum() * 100

    fig, ax = plt.subplots(figsize=(7.2, 3.2))
    ax.plot(inst["date"], trs_cum, color=CYAN, linewidth=1.6,
            label=f"TRS short (junior ABS): {trs_cum.iloc[-1]:+.2f}%")
    ax.plot(inst["date"], naive_cum, color=CRIMSON, linewidth=1.6,
            label=f"naive AFRM equity short: {naive_cum.iloc[-1]:+.2f}%")
    ax.axhline(0, color=SLATE, linewidth=0.5)

    # Annotate the Reg Z deadline.
    regz = pd.Timestamp("2025-01-17")
    ax.axvline(regz, color="#10b981", linestyle=":", linewidth=0.9, alpha=0.7)
    y0, y1 = ax.get_ylim()
    ax.text(regz, y1 * 0.95, " Reg Z deadline\n BSI $z = +44\\sigma$",
            fontsize=7.5, color=INK, va="top")

    # Annotate the spread at terminal.
    spread = trs_cum.iloc[-1] - naive_cum.iloc[-1]
    ax.text(
        inst["date"].iloc[-2], (trs_cum.iloc[-1] + naive_cum.iloc[-1]) / 2,
        f"  spread: {spread:+.2f} pp",
        fontsize=8, color=INK, weight="bold", va="center",
    )

    ax.set_ylabel("cumulative P&L (%)")
    ax.set_title(
        "Blind-spot counterfactual: Gate 3 (MOVE) relaxed, REGZ_EFFECTIVE window",
        loc="left", color=INK, fontsize=10,
    )
    ax.legend(loc="lower left", fontsize=8)
    _save(fig, "fig9_counterfactual_regz.png")


def fig_move_vs_bsi() -> None:
    with _con() as c:
        bsi = c.execute("SELECT observed_at d, z_bsi FROM bsi_daily "
                        "ORDER BY d").fetchdf()
        mv = c.execute("SELECT observed_at d, value v FROM fred_series "
                       "WHERE series_id = 'MOVE' ORDER BY d").fetchdf()
    bsi["d"] = pd.to_datetime(bsi["d"])
    mv["d"] = pd.to_datetime(mv["d"])

    fig, ax = plt.subplots(figsize=(7.0, 2.6))
    ax2 = ax.twinx()
    ax.plot(bsi["d"], bsi["z_bsi"], color=CYAN, linewidth=0.7, alpha=0.8)
    ax2.plot(mv["d"], mv["v"], color=CRIMSON, linewidth=0.7, alpha=0.7)
    ax2.axhline(120, color=CRIMSON, linestyle=":", linewidth=0.7)
    ax.set_ylabel(r"BSI z-score ($\sigma$)", color=CYAN)
    ax.tick_params(axis="y", colors=CYAN)
    ax2.set_ylabel("MOVE index", color=CRIMSON)
    ax2.tick_params(axis="y", colors=CRIMSON)
    ax.set_title("Rates-vol regime (MOVE) vs. BSI, 2018–2026",
                 loc="left", color=INK)
    ax.grid(False); ax2.grid(False)
    _save(fig, "fig6_move_vs_bsi.png")


def fig_gate_fire_heatmap() -> None:
    """Per-window, per-gate fire rate under absolute and dynamic Gate-3
    thresholds. Reads the per-window CSVs directly so we can decompose
    approvals into their per-gate components (BSI / MOVE / CCD-II) rather
    than just the conjunctive total."""
    order_win = ["KLARNA_DOWNROUND", "AFFIRM_GUIDANCE_1",
                 "AFFIRM_GUIDANCE_2", "CFPB_INTERP_RULE"]
    row_lbl  = ["Klarna\n(Jul '22)", "AFRM #1\n(Nov '22)",
                "AFRM #2\n(Feb '23)", "CFPB\n(May '24)"]

    rows: list[dict] = []
    for mode in ("absolute", "dynamic"):
        for win in order_win:
            f = BT_DIR / mode / f"pnl_{win}_institutional.csv"
            if not f.exists():
                continue
            d = pd.read_csv(f)
            rows.append({
                "mode": mode, "window": win,
                "g_bsi":  int(d["gate_bsi"].sum()),
                "g_move": int(d["gate_move"].sum()),
                "g_ccd2": int(d["gate_ccd2"].sum()),
                "approved": int(d["approved"].sum()),
            })
    if not rows:
        return
    dfm = pd.DataFrame(rows)

    fig, axes = plt.subplots(1, 2, figsize=(7.4, 2.6), sharey=True)
    for ax, mode in zip(axes, ("absolute", "dynamic")):
        sub = dfm[dfm["mode"] == mode].set_index("window").reindex(order_win)
        mat = sub[["g_bsi", "g_move", "g_ccd2", "approved"]].values
        im = ax.imshow(mat, cmap="magma_r", aspect="auto", vmin=0, vmax=61)
        for i in range(mat.shape[0]):
            for j in range(mat.shape[1]):
                v = mat[i, j]
                ax.text(j, i, f"{int(v)}", ha="center", va="center",
                        fontsize=8, color="white" if v > 25 else INK)
        ax.set_xticks(range(4))
        ax.set_xticklabels(["G1 BSI", "G2 MOVE", "G3 CCD-II", "approved"],
                           fontsize=7.5)
        ax.set_title(f"Gate 3 = {mode}", loc="left", fontsize=9, color=INK)
        ax.grid(False)
        if ax is axes[0]:
            ax.set_yticks(range(4))
            ax.set_yticklabels(row_lbl, fontsize=7.5)
    fig.colorbar(im, ax=axes, shrink=0.75, label="days fired (of 61)")
    fig.suptitle("Gate decomposition across event windows: absolute vs. "
                 "dynamic Gate-3 threshold",
                 fontsize=10.5, x=0.005, ha="left", color=INK, y=1.02)
    _save(fig, "fig7_gate_heatmap.png")


def fig_dynamic_threshold_trajectory() -> None:
    """Overlay MOVE MA30, absolute threshold (120), and dynamic
    85th-percentile threshold from 2020–2026 to show the regime-adaptive
    behaviour."""
    import numpy as np
    with _con() as c:
        mv = c.execute(
            "SELECT observed_at, value FROM fred_series "
            "WHERE series_id = 'MOVE' ORDER BY observed_at"
        ).fetchdf()
    mv["d"] = pd.to_datetime(mv["observed_at"])
    s = mv.set_index("d")["value"].astype(float).asfreq("B").ffill()
    ma30 = s.rolling(30, min_periods=8).mean()
    thr = ma30.rolling(504, min_periods=60).quantile(0.85).shift(1)

    # Mark event dates.
    mask = ma30.index >= pd.Timestamp("2020-01-01")
    fig, ax = plt.subplots(figsize=(7.0, 2.8))
    ax.plot(ma30.index[mask], ma30[mask], color=INK, linewidth=0.9,
            label="MOVE MA30")
    ax.axhline(120, color=CRIMSON, linestyle="--", linewidth=0.9,
               label="absolute threshold (120)")
    ax.plot(thr.index[mask], thr[mask], color=AMBER, linewidth=1.2,
            label="dynamic threshold (trailing 85th pct, 504d)")
    for label, d, col in EVENTS:
        ax.axvline(pd.Timestamp(d), color=col, linestyle=":", linewidth=0.7, alpha=0.6)
    ax.set_ylabel("MOVE MA30 (level)")
    ax.set_title("Regime-adaptive Gate-3 threshold, 2020--2026",
                 loc="left", color=INK)
    ax.legend(loc="upper right", fontsize=7.5, frameon=False)
    _save(fig, "fig8_dynamic_threshold.png")


# ------------------------------------------------------------------- main ----

def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    log.info("generating paper figures -> %s", FIG_DIR)
    fig_bsi_timeseries()
    fig_bsi_components()
    fig_granger_f()
    fig_bsi_vs_hyg()
    fig_event_study_pnl()
    fig_move_vs_bsi()
    fig_gate_fire_heatmap()
    fig_dynamic_threshold_trajectory()
    fig_counterfactual_regz()
    log.info("done")


if __name__ == "__main__":
    main()
