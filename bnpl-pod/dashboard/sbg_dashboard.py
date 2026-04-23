"""
Institutional BNPL Pod Terminal — Sprint H.d deliverable.

Layout  (Artifact split-screen, [1 : 2] columns)
------------------------------------------------
Left  pane — Control & Telemetry
    * Global controls: as-of date selector, PnL mode selector
    * Macro snapshot at selected as-of
    * Agent Debate Log: scrolling container reading
      logs/agent_decisions/YYYY-MM-DD.jsonl (role-tagged: macro / quant / risk)

Right pane — Artifact Canvas
    * Top row of st.metric cards: Sharpe · Max DD · Gross Leverage
      (with delta arrows vs. NAIVE baseline)
    * Visual 1: Three-Panel Cumulative P&L (Plotly)
                NAIVE vs FIX3_ONLY vs INSTITUTIONAL for one window
    * Visual 2: Macro Radar (Plotly polar/Scatterpolar)
                BSI_z · MOVE · AFRMMT Excess Spread (normalized 0-1)

Design contract
---------------
- Page config must be the FIRST Streamlit call (st.set_page_config).
- Custom CSS hides the hamburger + footer; enforces a dark terminal feel.
- The terminal degrades gracefully: if the warehouse is missing BSI, the
  three-panel chart is replaced with an explanatory note (no crash).

Launch:   streamlit run dashboard/sbg_dashboard.py
"""
from __future__ import annotations

import json
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

# Streamlit launches the script with the script's directory as sys.path[0],
# not the repo root — so `import backtest.event_study` fails unless we put
# the project root on sys.path explicitly. This is the one-liner that lets
# `streamlit run dashboard/sbg_dashboard.py` work from any cwd.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Design tokens + Plotly institutional template — importing registers
# the template and sets it as the Plotly default for this process.
from dashboard.design_tokens import C as TOK_C, FONT as TOK_FONT
from dashboard import plotly_theme as _plotly_theme  # noqa: F401  (side-effect)

from backtest.event_study import (
    PnLMode,
    WINDOWS,
    load_window_from_warehouse,
    run_three_panel_comparison,
)
from data.regulatory_calendar import load_catalysts
from data.settings import settings
# Layer-2 upgrade (Phase C): Granger proof + JT simulation + extracted
# agent log renderer all read off existing public APIs — no warehouse
# schema changes, no new data ingest paths.
from quant.jarrow_turnbull import (
    CIRParams,
    affine_hazard,
    simulate_cir,
    survival_probability,
)


# =========================================================================
# Pure SBG helpers (kept at module level for `test_sensitivity_and_sbg.py`)
# These existed in the pre-H.d dashboard; preserving them as pure functions
# so the institutional-terminal rewrite doesn't break the SBG test contract.
# =========================================================================
def classify_alert(row: pd.Series, co_occur_pctile: float) -> str:
    """Three-level Shadow-Bureau-Gap alert classifier.

        RED    — freeze_flag fired OR z_bsi in the extreme tail (|z| >= 2).
        YELLOW — elevated stress (z_bsi >= 1.0) OR a notable co-occurrence
                 spike (>= 0.8 percentile of historical Reddit × Trends).
        GREEN  — everything else.

    `row` must carry keys `z_bsi` (float) and `freeze_flag` (bool). The
    `co_occur_pctile` argument is precomputed externally via
    `co_occurrence_percentile(df)`.
    """
    if bool(row.get("freeze_flag", False)):
        return "RED"
    z = float(row.get("z_bsi", 0.0))
    if z >= 2.0:
        return "RED"
    if z >= 1.0 or co_occur_pctile >= 0.8:
        return "YELLOW"
    return "GREEN"


def co_occurrence_percentile(df: pd.DataFrame) -> float:
    """Percentile rank of TODAY's Reddit × Trends co-stress vs. the history.

    Score per row = min(c_reddit, c_trends) — both need to be elevated for a
    co-occurrence to count. Using the minimum kills the degenerate case of
    one pillar spiking in isolation (which is a single-signal event, not a
    "two sources agree the consumer is cracking" event).

    Returns a float in [0, 1]; 1.0 means today is at or above every prior
    observation in the frame. Empty frames return 0.0.
    """
    if df is None or df.empty:
        return 0.0
    if not {"c_reddit", "c_trends"}.issubset(df.columns):
        return 0.0
    scores = df[["c_reddit", "c_trends"]].min(axis=1).astype(float)
    today = float(scores.iloc[-1])
    return float((scores <= today).mean())

# -------------------------------------------------------------------------
# PAGE CONFIG — must be the first Streamlit call in the script
# -------------------------------------------------------------------------
st.set_page_config(
    page_title="BNPL Pod · Institutional Terminal",
    page_icon="■",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Terminal CSS: unified dark palette matching the React tear-sheet so the
# two layers feel like one product. Kills default chrome, flattens padding,
# gives metrics/tables a tight institutional feel.
st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

        /* ── design tokens (lockstep with web/design_tokens.ts AND
         * dashboard/design_tokens.py). Edit all three together. ──── */
        :root {
            --bg:          #0F172A;  /* slate-900 */
            --card:        #1E293B;  /* slate-800 */
            --cardAlt:     #273449;  /* chip lift */
            --border:      #334155;  /* slate-700 */
            --borderHi:    #475569;  /* slate-600 */
            --borderMuted: #1F2937;
            --text:        #F8FAFC;  /* slate-50  */
            --dim:         #64748B;  /* slate-500 */
            --muted:       #94A3B8;  /* slate-400 */
            --accent:      #38BDF8;  /* sky-blue  — primary / calm / pass */
            --warn:        #FBBF24;  /* amber — thresholds */
            --critical:    #EF4444;  /* red — breach */
            --violet:      #8B5CF6;  /* QUANT agent */
            /* legacy aliases — keep so existing className refs keep resolving
             * without per-site edits. All three alias the new palette. */
            --cyan:        #38BDF8;
            --amber:       #FBBF24;
            --crimson:     #EF4444;
            --green:       #38BDF8;
        }

        /* hide default chrome */
        #MainMenu        { visibility: hidden; }
        footer           { visibility: hidden; }
        header           { visibility: hidden; }

        /* global dark surface */
        [data-testid="stAppViewContainer"] {
            background-color: var(--bg);
            color: var(--text);
        }
        [data-testid="stHeader"] { background-color: transparent; }
        section.main > div.block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
            max-width: 100%;
        }

        /* typography — Inter for UI chrome, JetBrains Mono for numbers */
        html, body { font-family: 'Inter', system-ui, sans-serif; }
        .stMarkdown, .stDataFrame, [class*="css"] { font-family: inherit; }
        [data-testid="stMetricValue"],
        [data-testid="stMetricDelta"],
        code, pre, .mono {
            font-family: 'JetBrains Mono', ui-monospace, 'Consolas', monospace !important;
            font-variant-numeric: tabular-nums;
        }

        /* metric cards — tighter bezel, cyan rail, clean value */
        [data-testid="stMetric"] {
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 12px 16px 10px 16px;
            position: relative;
        }
        [data-testid="stMetric"]::before {
            content: "";
            position: absolute; left: 0; top: 8px; bottom: 8px;
            width: 2px; background: var(--borderHi); border-radius: 0 2px 2px 0;
        }
        [data-testid="stMetricLabel"] p {
            color: var(--dim) !important;
            font-size: 0.7rem !important;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            font-weight: 500;
        }
        [data-testid="stMetricValue"] {
            color: var(--text) !important;
            font-size: 1.5rem !important;
            font-weight: 500 !important;
            letter-spacing: -0.01em;
        }
        [data-testid="stMetricDelta"] {
            font-size: 0.7rem !important;
            letter-spacing: 0.05em;
        }

        /* tabs — flatter, cyan underline on active */
        [data-testid="stTabs"] [role="tablist"] {
            gap: 0;
            border-bottom: 1px solid var(--border);
        }
        [data-testid="stTabs"] [role="tab"] {
            background: transparent;
            color: var(--dim);
            font-size: 0.78rem;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            padding: 8px 18px;
            border-bottom: 2px solid transparent;
            margin-bottom: -1px;
        }
        [data-testid="stTabs"] [role="tab"][aria-selected="true"] {
            color: var(--cyan);
            border-bottom-color: var(--cyan);
        }

        /* section headers inside tabs */
        .panel-title {
            color: var(--muted);
            font-size: 0.72rem;
            letter-spacing: 0.22em;
            text-transform: uppercase;
            margin: 20px 0 8px 0;
            padding-bottom: 6px;
            border-bottom: 1px solid var(--border);
            display: flex; align-items: center; gap: 8px;
        }
        .panel-title::before {
            content: ""; width: 6px; height: 6px; border-radius: 50%;
            background: var(--cyan);
            box-shadow: 0 0 6px rgba(34,211,238,0.55);
        }

        /* status bar (always-visible decision strip) */
        .status-bar {
            display: grid;
            grid-template-columns: auto repeat(4, 1fr) auto;
            align-items: center;
            gap: 18px;
            padding: 10px 14px;
            margin: 0 0 14px 0;
            background: linear-gradient(180deg, var(--card) 0%, var(--cardAlt) 100%);
            border: 1px solid var(--border);
            border-radius: 8px;
        }
        .status-bar .wordmark {
            display: flex; align-items: center; gap: 10px;
            font-size: 0.82rem; letter-spacing: 0.22em; text-transform: uppercase;
            color: var(--text); font-weight: 600;
        }
        .status-bar .wordmark .dot {
            width: 8px; height: 8px; border-radius: 50%;
            background: var(--cyan); box-shadow: 0 0 8px rgba(34,211,238,0.7);
        }
        .status-bar .sub { color: var(--dim); font-size: 0.66rem; letter-spacing: 0.1em; font-weight: 400; }
        .status-bar .cell { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
        .status-bar .label { font-size: 0.62rem; letter-spacing: 0.22em; text-transform: uppercase; color: var(--dim); }
        /* Values are single-line tabular figures. Mid-word wrapping (e.g.
         * "Credit Dire\nctive") looked broken in earlier passes — ellipsize
         * instead. .dim is the default / neutral state; .ok/.warn/.fire
         * light up only when a threshold is crossed. */
        .status-bar .value {
            font-family: "JetBrains Mono", monospace;
            font-size: 0.95rem;
            font-variant-numeric: tabular-nums;
            color: var(--text);
            letter-spacing: -0.01em;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .status-bar .value.dim    { color: var(--muted); }
        .status-bar .value.ok     { color: var(--cyan); }
        .status-bar .value.warn   { color: var(--amber); }
        .status-bar .value.fire   { color: var(--crimson); }
        .status-bar .state-pill {
            padding: 8px 16px; border-radius: 6px;
            font-family: "JetBrains Mono", monospace;
            font-size: 0.82rem; letter-spacing: 0.18em; font-weight: 600;
            /* Default (STAND-DOWN) = calm / neutral. No sky-blue glow,
             * otherwise STAND-DOWN looks like a clickable primary button. */
            background: rgba(148,163,184,0.06);
            color: var(--muted);
            border: 1px solid var(--border);
        }
        .status-bar .state-pill.fire {
            color: var(--crimson);
            background: rgba(239,68,68,0.08);
            border-color: rgba(239,68,68,0.3);
        }
        .status-bar .state-pill.bypass {
            color: var(--amber);
            background: rgba(251,191,36,0.08);
            border-color: rgba(251,191,36,0.3);
        }

        /* Compact Streamlit st.metric cards — the default has 24+ px of
         * internal padding which makes a 4-up row sprawl and separates the
         * label from its value. Tighten to match the institutional density
         * of the Layer 1 bento cards. */
        [data-testid="stMetric"] {
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 10px 14px;
        }
        [data-testid="stMetricLabel"] {
            font-size: 0.62rem !important;
            letter-spacing: 0.22em;
            text-transform: uppercase;
            color: var(--muted) !important;
        }
        [data-testid="stMetricLabel"] p {
            font-size: 0.62rem !important;
            letter-spacing: 0.22em;
            color: var(--muted) !important;
        }
        [data-testid="stMetricValue"] {
            font-family: "JetBrains Mono", monospace !important;
            font-size: 1.35rem !important;
            font-variant-numeric: tabular-nums;
            color: var(--text) !important;
            padding-top: 2px;
        }

        /* tab onramp — one-line inline caption, not a collapsed expander */
        .tab-onramp {
            background: var(--card);
            border: 1px solid var(--border);
            border-left: 2px solid var(--cyan);
            border-radius: 4px;
            padding: 8px 14px;
            margin-bottom: 16px;
            font-size: 0.78rem;
            color: var(--muted);
            line-height: 1.5;
        }
        .tab-onramp .paper-ref {
            color: var(--amber);
            font-family: "JetBrains Mono", monospace;
            font-size: 0.72rem;
        }

        /* agent-row styling (Audit tab) */
        .agent-row {
            background: var(--card);
            border: 1px solid var(--border);
            border-left: 3px solid var(--borderHi);
            border-radius: 4px;
            padding: 8px 12px;
            margin-bottom: 5px;
            font-size: 0.78rem;
            color: var(--text);
        }
        .agent-row.macro { border-left-color: var(--cyan); }
        .agent-row.quant { border-left-color: var(--violet); }
        .agent-row.risk  { border-left-color: var(--amber); }
        .agent-row .who  { color: var(--muted); font-weight: 600; letter-spacing: 0.1em; }
        .agent-row .prov { color: var(--green); font-weight: 500; }

        /* dataframe polish */
        [data-testid="stDataFrame"] {
            border: 1px solid var(--border);
            border-radius: 6px;
        }

        /* scrollbars */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: var(--bg); }
        ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--borderHi); }

        /* selection */
        ::selection { background: rgba(34,211,238,0.3); }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------------------------------------------------------
# LOADERS — shared DuckDB handle, cached per-session
# -------------------------------------------------------------------------
LOG_DIR = Path(__file__).resolve().parent.parent / "logs" / "agent_decisions"


@st.cache_resource(show_spinner=False)
def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(settings.duckdb_path), read_only=True)


@st.cache_data(ttl=60, show_spinner=False)
def _bsi_frame() -> pd.DataFrame:
    try:
        df = _con().execute(
            "SELECT observed_at, bsi, z_bsi, c_move, c_reddit, c_cfpb, "
            "       c_vitality, freeze_flag "
            "FROM bsi_daily ORDER BY observed_at"
        ).df()
        if not df.empty:
            df["observed_at"] = pd.to_datetime(df["observed_at"])
        return df
    except Exception:
        return pd.DataFrame(columns=["observed_at", "bsi", "z_bsi"])


@st.cache_data(ttl=60, show_spinner=False)
def _fred_frame(series_id: str) -> pd.DataFrame:
    df = _con().execute(
        "SELECT observed_at, value FROM fred_series "
        "WHERE series_id = ? ORDER BY observed_at",
        [series_id],
    ).df()
    if not df.empty:
        df["observed_at"] = pd.to_datetime(df["observed_at"])
    return df


@st.cache_data(ttl=60, show_spinner=False)
def _abs_tranche_frame() -> pd.DataFrame:
    df = _con().execute(
        "SELECT period_end, excess_spread, roll_rate_60p, cnl "
        "FROM abs_tranche_metrics ORDER BY period_end"
    ).df()
    if not df.empty:
        df["period_end"] = pd.to_datetime(df["period_end"])
    return df


@st.cache_data(ttl=15, show_spinner=False)
def _agent_log_rows(n_days: int = 3) -> list[dict]:
    """Read the last `n_days` of role-tagged JSONL emitted by llm_client."""
    rows: list[dict] = []
    if not LOG_DIR.exists():
        return rows
    cutoff = date.today() - timedelta(days=n_days - 1)
    for p in sorted(LOG_DIR.glob("*.jsonl")):
        try:
            day = datetime.strptime(p.stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        if day < cutoff:
            continue
        for ln in p.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except json.JSONDecodeError:
                continue
    # newest first
    rows.sort(key=lambda r: r.get("ts", ""), reverse=True)
    return rows


@st.cache_data(ttl=120, show_spinner="Running 3-panel backtest…")
def _three_panel_comparison(window_key: str):
    """Load window fixture from warehouse + run NAIVE/FIX3/INSTITUTIONAL."""
    fx = load_window_from_warehouse(window_key)
    catalysts = load_catalysts()
    return fx, run_three_panel_comparison(fx, catalysts=catalysts)


# -------------------------------------------------------------------------
# Layer-2 Phase C helpers — Granger proof + JT simulation
# Pure-function wrappers so the tabs below stay layout-only.
# -------------------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner=False)
def _granger_results_frame() -> pd.DataFrame:
    """Latest run per tier from `granger_results`. Empty frame if table absent."""
    try:
        df = _con().execute(
            """
            WITH latest AS (
                SELECT tier, MAX(run_at) AS run_at
                FROM granger_results
                GROUP BY tier
            )
            SELECT g.run_at, g.tier, g.target_label, g.lag_weeks,
                   g.p_value, g.f_stat, g.n_obs
            FROM granger_results g
            JOIN latest l USING (tier, run_at)
            ORDER BY g.tier, g.lag_weeks
            """
        ).df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame(
            columns=["run_at", "tier", "target_label", "lag_weeks",
                     "p_value", "f_stat", "n_obs"]
        )
    return df


@st.cache_data(ttl=60, show_spinner="Simulating CIR hazard paths…")
def _jt_simulate(
    bsi: float,
    move: float,
    alpha: float,
    beta_bsi: float,
    beta_move: float,
    kappa: float,
    theta: float,
    sigma: float,
    horizon_days: int,
    n_paths: int,
) -> dict:
    """Run CIR sim live. Returns {paths, surv, lambda0}."""
    lambda0 = affine_hazard(bsi, move, alpha=alpha,
                            beta_bsi=beta_bsi, beta_move=beta_move)
    params = CIRParams(kappa=kappa, theta=theta, sigma=sigma)
    paths = simulate_cir(params, lambda_0=lambda0,
                         horizon_days=horizon_days, n_paths=n_paths, seed=42)
    surv = survival_probability(paths, dt_days=1.0)
    return {
        "paths": paths,
        "surv": surv,
        "lambda0": lambda0,
        "surv_p05": float(np.percentile(surv, 5)),
        "surv_p50": float(np.percentile(surv, 50)),
        "surv_p95": float(np.percentile(surv, 95)),
    }


def _render_agent_debate_log(*, n_days: int = 3, height_px: int = 520,
                             role_filter: set[str] | None = None) -> None:
    """Render the agent-decisions JSONL tail as role-coloured chat bubbles.

    Uses ``dashboard.chat_renderer`` so the HTML shape is byte-identical to
    the Layer 1 React ``AgentBubble`` — the two layers read as one product.
    """
    from dashboard.chat_renderer import render_agent_log

    rows = _agent_log_rows(n_days=n_days)
    if not rows:
        st.caption("No `logs/agent_decisions/*.jsonl` rows yet. "
                   "Run `python -m agents.tick` to populate.")
        return

    # The chat renderer expects upper-case role tags to match the
    # AGENT_COLORS keys (MACRO/QUANT/RISK). Normalise the optional
    # role_filter up-front.
    norm_filter = (
        {r.upper() for r in role_filter} if role_filter else None
    )

    html_block = render_agent_log(
        rows[:200],
        max_height_px=height_px,
        role_filter=norm_filter,
        truncate_chars=400,
    )
    st.markdown(html_block, unsafe_allow_html=True)


def _what_is_this(*, what: str, how: str, values: str, section: str) -> None:
    """Compact tab onramp — a one-line lede + collapsible Method/Values.

    Earlier pass rendered WHAT/HOW/VALUES/PAPER as four stacked labelled
    rows. That read as a wall of text on every tab and pushed the first
    real chart below the fold. This version keeps the single most useful
    sentence (WHAT) visible always, surfaces the paper reference inline
    on the right, and hides the denser prose (HOW / VALUES) behind an
    ``st.expander`` for readers who want it.
    """
    st.markdown(
        f"""
        <div class="tab-onramp">
            <div style="display:flex;align-items:baseline;gap:14px;
                        flex-wrap:wrap;justify-content:space-between;">
                <div style="flex:1 1 auto;min-width:0;
                            color:var(--text);line-height:1.45;">
                    {what}
                </div>
                <div style="flex:0 0 auto;display:flex;align-items:center;
                            gap:8px;white-space:nowrap;">
                    <span style="color:var(--dim);font-size:0.62rem;
                                 letter-spacing:0.22em;text-transform:uppercase;">
                        Paper
                    </span>
                    <span class="paper-ref">{section}</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("Method · Values", expanded=False):
        st.markdown(
            f"""
            <div style="display:grid;grid-template-columns:auto 1fr;
                        gap:8px 16px;line-height:1.5;">
                <div style="color:var(--cyan);font-size:0.62rem;
                            letter-spacing:0.22em;text-transform:uppercase;">
                    Method
                </div>
                <div style="color:var(--muted);">{how}</div>
                <div style="color:var(--cyan);font-size:0.62rem;
                            letter-spacing:0.22em;text-transform:uppercase;">
                    Values
                </div>
                <div style="font-family:'JetBrains Mono',monospace;
                            font-size:0.78rem;color:var(--text);">
                    {values}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# -------------------------------------------------------------------------
# PLOT HELPERS
# -------------------------------------------------------------------------
_MODE_COLOR = {
    # Institutional token palette — red = breach, amber = partial, sky = pass.
    PnLMode.NAIVE: TOK_C["critical"],
    PnLMode.FIX3_ONLY: TOK_C["warn"],
    PnLMode.INSTITUTIONAL: TOK_C["accent"],
}


def _three_panel_chart(fx, cmp) -> go.Figure:
    """Cumulative TRS-arm P&L by mode, shared x-axis."""
    fig = go.Figure()
    dates = pd.to_datetime(fx.dates)
    for mode, panel in cmp.panels.items():
        cum = np.cumsum(panel.trs_daily_pnl)
        fig.add_trace(go.Scatter(
            x=dates, y=cum,
            mode="lines",
            name=mode.value.upper(),
            line=dict(color=_MODE_COLOR[mode], width=2.2),
            hovertemplate="%{x|%Y-%m-%d}<br>cum = %{y:+.4f}<extra>"
                          + mode.value.upper() + "</extra>",
        ))
    fig.update_layout(
        # Institutional Plotly template (paper/plot bg, Inter/JetBrains Mono,
        # slate gridlines) is applied as default on import of
        # ``dashboard.plotly_theme``; we only patch per-chart knobs here.
        template="institutional",
        margin=dict(l=40, r=20, t=30, b=30),
        height=340,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.12, x=0, bgcolor="rgba(0,0,0,0)"),
        yaxis=dict(tickformat="+.3f", title="Cumulative TRS P&L"),
    )
    return fig


def _macro_radar(bsi_z: float, move_level: float, excess_spread: float) -> go.Figure:
    """Radar of BSI / MOVE / AFRMMT excess spread at the current as-of.

    Normalizations (plausible envelopes so the shape is interpretable):
        BSI_z in [-3, 3]               → 0..1 via (z + 3) / 6
        MOVE  in [70, 200]             → 0..1 via (MOVE - 70) / 130
        Excess spread (%) in [0, 15]   → 0..1 via es / 15
    """
    def _norm_bsi(z):   return float(np.clip((z + 3) / 6.0, 0, 1))
    def _norm_move(m):  return float(np.clip((m - 70) / 130.0, 0, 1))
    def _norm_es(es):   return float(np.clip(es / 15.0, 0, 1))

    categories = ["BSI z", "MOVE level", "Excess spread"]
    values = [_norm_bsi(bsi_z), _norm_move(move_level), _norm_es(excess_spread)]
    # Close the polygon.
    values_closed = values + [values[0]]
    categories_closed = categories + [categories[0]]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values_closed, theta=categories_closed,
        fill="toself", name="Current",
        line=dict(color=TOK_C["accent"], width=2),
        # Sky-blue fill at 22% opacity — matches Layer 1 BSI AreaChart gradient.
        fillcolor="rgba(56,189,248,0.22)",
        hovertemplate="%{theta}: %{r:.2f}<extra></extra>",
    ))
    # Threshold ring — BSI>=1.5 maps to norm ≈0.75, MOVE>=120 maps to ≈0.38,
    # ES ≥ 8% maps to ≈0.53. Draw a single reference polygon so viewers see
    # which axes are currently "firing" relative to the rule-of-thumb envelope.
    ref = [
        _norm_bsi(1.5),
        _norm_move(120.0),
        _norm_es(8.0),
    ]
    ref_closed = ref + [ref[0]]
    fig.add_trace(go.Scatterpolar(
        r=ref_closed, theta=categories_closed,
        name="Gate threshold",
        # Dashed amber — same semantic as the Layer 1 +1.5σ ReferenceLine.
        line=dict(color=TOK_C["warn"], width=1, dash="dash"),
        hovertemplate="threshold %{theta}: %{r:.2f}<extra></extra>",
    ))
    fig.update_layout(
        template="institutional",
        margin=dict(l=20, r=20, t=30, b=20),
        height=320,
        polar=dict(
            bgcolor=TOK_C["card"],
            radialaxis=dict(
                visible=True, range=[0, 1],
                gridcolor=TOK_C["chartGrid"],
                tickfont=dict(color=TOK_C["textMuted"]),
            ),
            angularaxis=dict(
                gridcolor=TOK_C["chartGrid"],
                tickfont=dict(color=TOK_C["textSecondary"]),
            ),
        ),
        legend=dict(orientation="h", y=-0.1, x=0, bgcolor="rgba(0,0,0,0)"),
    )
    return fig


# -------------------------------------------------------------------------
# STATUS BAR — always-visible decision strip ("is the trade on?" in one line)
# Reads live BSI z / MOVE / gate state / next catalyst so the PM doesn't have
# to click into Backtest to see current state.
# -------------------------------------------------------------------------
def _render_status_bar() -> None:
    """One-line decision banner above the tabs. Reads live from warehouse."""
    bsi_df_s = _bsi_frame()
    bsi_z_s = (float(bsi_df_s["z_bsi"].dropna().iloc[-1])
               if not bsi_df_s.empty and bsi_df_s["z_bsi"].dropna().size else None)
    move_df_s = _fred_frame("MOVE")
    move_s = (float(move_df_s["value"].dropna().iloc[-1])
              if not move_df_s.empty and move_df_s["value"].dropna().size else None)
    try:
        catalysts_s = load_catalysts()
        future_s = [c for c in catalysts_s if c.deadline_date >= date.today()]
        next_cat = min(future_s, key=lambda c: c.deadline_date) if future_s else None
        cat_days = (next_cat.deadline_date - date.today()).days if next_cat else None
        # Word-boundary ellipsis so "EU Consumer Credit Directive" doesn't
        # chop mid-word into "Credit Direc". 18 chars is roughly one
        # narrow-viewport cell width at the current .status-bar grid ratios.
        if next_cat:
            raw_title = next_cat.title.strip()
            if len(raw_title) <= 18:
                cat_name = raw_title
            else:
                # Cut at last whitespace before char 18, append ellipsis.
                cut = raw_title[:18].rsplit(" ", 1)[0]
                cat_name = (cut if len(cut) >= 4 else raw_title[:17]) + "…"
        else:
            cat_name = "—"
    except Exception:
        cat_days = None
        cat_name = "—"

    # Compute gate state (same logic the compliance engine uses).
    g1 = (bsi_z_s is not None and bsi_z_s >= 1.5)
    g2 = (move_s is not None and move_s >= 120.0)
    g3 = (cat_days is not None and cat_days <= 30)
    bypass = (bsi_z_s is not None and abs(bsi_z_s) >= 10.0)
    gates_pass = sum([g1, g2, g3])
    if bypass:
        state_cls, state_txt = "bypass", "BYPASS"
    elif g1 and g2 and g3:
        state_cls, state_txt = "fire", "FIRING"
    else:
        # Explicit empty class — .state-pill default is now neutral/muted,
        # not the old sky-blue which made STAND-DOWN look like a CTA button.
        state_cls, state_txt = "", "STAND-DOWN"

    # Color classes on individual cells. The default is now ``"dim"`` so a
    # resting, non-firing value sits at secondary-text color rather than
    # primary-white. Only threshold crossings light up the bright tokens.
    bsi_cls = (
        "fire" if (bsi_z_s is not None and bsi_z_s >= 1.5)
        else "dim"
    )
    move_cls = (
        "fire" if (move_s is not None and move_s >= 120)
        else ("warn" if (move_s is not None and move_s >= 100) else "dim")
    )
    cat_cls = "warn" if (cat_days is not None and cat_days <= 30) else "dim"
    gate_cls = (
        "fire" if gates_pass == 3
        else ("warn" if gates_pass == 2 else "dim")
    )

    bsi_txt = f"{bsi_z_s:+.2f}σ" if bsi_z_s is not None else "—"
    move_txt = f"{move_s:.1f}" if move_s is not None else "—"
    cat_txt = f"{cat_days}d" if cat_days is not None else "—"
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M") + "z"

    st.markdown(
        f"""
        <div class="status-bar">
            <div class="wordmark">
                <span class="dot"></span>
                <div>BNPL · POD
                    <div class="sub">Layer 2 · Quant Risk Engine</div>
                </div>
            </div>
            <div class="cell"><span class="label">BSI z-score</span>
                <span class="value {bsi_cls}">{bsi_txt}</span></div>
            <div class="cell"><span class="label">MOVE (index)</span>
                <span class="value {move_cls}">{move_txt}</span></div>
            <div class="cell"><span class="label">Gates pass</span>
                <span class="value {gate_cls}">{gates_pass} / 3</span></div>
            <div class="cell"><span class="label">Next catalyst</span>
                <span class="value {cat_cls}">{cat_txt} · {cat_name}</span></div>
            <div class="state-pill {state_cls}">{state_txt}</div>
        </div>
        <div style="color: var(--dim); font-size: 0.68rem; letter-spacing: 0.14em;
                    text-transform: uppercase; margin: -6px 0 14px 2px;
                    font-family: 'JetBrains Mono', monospace;">
            warehouse · {settings.duckdb_path.name} &nbsp;·&nbsp; as-of {ts} &nbsp;·&nbsp;
            3-gate AND + |z|≥10 bypass &nbsp;·&nbsp;
            <span style="color: var(--amber);">paper-trade only</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


_render_status_bar()
# -------------------------------------------------------------------------
# TABBED LAYOUT — 5 tabs following the paper's reading order.
#   Proof    — Granger null that falsifies "BSI is just a subprime gauge"
#   Funnel   — alt-data pillar coverage + ingest status
#   Math     — Jarrow-Turnbull CIR hazard + survival sim
#   Audit    — LLM agent-debate JSONL tail (advisory only; never gates)
#   Backtest — Sprint H.d 3-panel event-study + macro radar + metrics
# -------------------------------------------------------------------------
tab_proof, tab_funnel, tab_math, tab_audit, tab_backtest = st.tabs(
    ["Proof", "Coverage", "Pricing", "Audit", "Backtest"]
)


# =========================================================================
# TAB 1 — PROOF · GRANGER (the falsification headline)
# =========================================================================
with tab_proof:
    _what_is_this(
        what=("Granger-causality test: does BSI lead a broader subprime-credit "
              "stress target? Null-rejection at any lag would suggest BSI is "
              "just a general-subprime gauge."),
        how=("OLS Granger F-test across lags 4–8 weeks. Tier ladder: "
             "Tier-1 AFFIRM 144A trustee (usually empty), Tier-2 SDART+"
             "AMCAR+EART composite, Tier-3 HYG proxy. Persisted to "
             "`granger_results`."),
        values=("p-value per lag per tier. p ≥ 0.95 at every lag = strong "
                "failure to reject the null = BSI signal is IDIOSYNCRATIC "
                "(our A+ result)."),
        section="§5 — Granger causality · §8.2 — falsification framing",
    )

    gdf = _granger_results_frame()
    if gdf.empty:
        st.info(
            "No rows in `granger_results` yet. Run\n\n"
            "```\npython -m signals.granger --persist\n```\n\n"
            "This populates the table and this tab reads it live."
        )
    else:
        latest_ts = pd.to_datetime(gdf["run_at"]).max()
        tiers_available = sorted(gdf["tier"].dropna().astype(int).unique().tolist())
        tier_labels = {
            1: "Tier-1 · AFFIRM 10-D trustee roll-rate",
            2: "Tier-2 · subprime-auto composite (SDART+AMCAR+EART)",
            3: "Tier-3 · HYG negative log-return (proxy)",
        }
        sel = st.radio(
            "Target tier",
            tiers_available,
            format_func=lambda t: tier_labels.get(int(t), f"Tier-{t}"),
            horizontal=True,
            help="Tier ladder: BSI is tested against progressively coarser "
                 "targets. A null result at every tier is the strongest "
                 "falsification of the 'general-subprime gauge' alternative.",
        )
        sub = gdf[gdf["tier"] == sel].sort_values("lag_weeks")

        # Adaptive metric strip: lag-range label rather than redundant min/max
        # when sub has a single row (min_p == max_p, F is a scalar).
        lags_arr = sub["lag_weeks"].astype(int).tolist()
        if len(lags_arr) == 1:
            lag_display = f"{lags_arr[0]}w"
        else:
            lag_display = f"{min(lags_arr)}w – {max(lags_arr)}w ({len(lags_arr)})"
        # KPI strip — the onramp's Method/Values expander already explains
        # what each figure means; the per-metric ⓘ icons added visual noise
        # for no information gain, so they're gone.
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("n (weekly obs)", f"{int(sub['n_obs'].iloc[0])}")
        c2.metric("Lags tested", lag_display)
        c3.metric("p-value (tightest)", f"{sub['p_value'].min():.3f}")
        c4.metric("Last run", latest_ts.strftime("%Y-%m-%d %H:%M"))

        # ─────────────────────────────────────────────────────────────
        # Granger Heatmap · lag (x) × tier (y) × p-value (z).
        # Shows ALL tiers at once so the "p > 0.95 everywhere" result
        # reads in a single glance.
        #
        # Color semantic (inverted from the usual "red = high"):
        #   p < 0.05  → critical-red   — null rejected, BSI behaves like a
        #                                generic subprime gauge → our
        #                                falsification claim would FAIL.
        #   p ~ 0.5   → muted-slate    — inconclusive.
        #   p > 0.95  → accent-sky     — null not rejected, BSI is
        #                                idiosyncratic → falsification
        #                                PASSES (the A+ result).
        # Rationale: in every other panel of the app, red = breach / bad.
        # Using red for "our thesis survives" would fight that language —
        # a reviewer would misread the block as an alarm. Sky-blue for
        # "pass" lines up with the Layer 1 gate-PASS chip color.
        # ─────────────────────────────────────────────────────────────
        all_tiers = sorted(gdf["tier"].dropna().astype(int).unique().tolist())
        all_lags = sorted(gdf["lag_weeks"].dropna().astype(int).unique().tolist())
        n_cells = max(1, len(all_tiers)) * max(1, len(all_lags))

        # Flag — set when the degenerate-case verdict card renders so we can
        # skip the redundant "Falsification · pass" callout further down.
        verdict_rendered = False

        if n_cells <= 2:
            # ── Degenerate case: not enough cells for a heatmap to read ──
            # A single p-value stretched across the full panel width reads
            # as an alarm block, not a visualization. Instead, render a
            # focused "verdict card" — big p-value, accent-colored chip,
            # plain-English one-liner. Same information, far clearer.
            t0 = all_tiers[0]
            lw0 = all_lags[0]
            cell = gdf[(gdf["tier"] == t0) & (gdf["lag_weeks"] == lw0)]
            pv = float(cell["p_value"].iloc[0]) if not cell.empty else float("nan")
            fstat = float(cell["f_stat"].iloc[0]) if not cell.empty else float("nan")
            tier_name = tier_labels.get(int(t0), f"Tier-{t0}").split("·")[0].strip()
            if pv >= 0.95:
                verdict = "FALSIFICATION · PASS"
                verdict_color = TOK_C["accent"]
                verdict_bg = "rgba(56,189,248,0.10)"
                verdict_detail = (
                    "Null <em>is not rejected</em> — BSI does <strong>not</strong> "
                    "Granger-cause this tier's subprime-credit stress target. "
                    "The signal is idiosyncratic to BNPL, not a general-subprime gauge."
                )
            elif pv < 0.05:
                verdict = "FALSIFICATION · FAIL"
                verdict_color = TOK_C["critical"]
                verdict_bg = "rgba(239,68,68,0.10)"
                verdict_detail = (
                    "Null <em>is rejected</em> at α = 0.05 — BSI Granger-causes "
                    "this target. The thesis that BSI is idiosyncratic to BNPL "
                    "would be weakened at this tier/lag."
                )
            else:
                verdict = "FALSIFICATION · INCONCLUSIVE"
                verdict_color = TOK_C["warn"]
                verdict_bg = "rgba(251,191,36,0.10)"
                verdict_detail = (
                    "p lies between 0.05 and 0.95. Neither a confident reject nor "
                    "a confident fail-to-reject — more lags or a wider tier sweep "
                    "would tighten the test."
                )

            verdict_html = (
                "<div style='"
                f"background:{verdict_bg};"
                f"border:1px solid {TOK_C['border']};"
                f"border-left:3px solid {verdict_color};"
                "border-radius:4px;padding:18px 22px;"
                "height:100%;box-sizing:border-box;'>"
                f"<div style='color:{verdict_color};"
                f"font-family:{TOK_FONT['mono']};font-size:0.72rem;"
                "letter-spacing:0.14em;text-transform:uppercase;"
                "margin-bottom:10px;'>"
                f"{verdict}</div>"
                "<div style='display:flex;align-items:baseline;gap:18px;"
                "margin-bottom:10px;flex-wrap:wrap;'>"
                f"<div style='font-family:{TOK_FONT['mono']};"
                f"font-size:2.2rem;font-weight:600;color:{verdict_color};"
                "line-height:1;'>"
                f"p = {pv:.4f}</div>"
                f"<div style='font-family:{TOK_FONT['mono']};"
                f"font-size:0.82rem;color:{TOK_C['textSecondary']};'>"
                f"F = {fstat:.3f} · {tier_name} · lag {lw0}w · n = "
                f"{int(cell['n_obs'].iloc[0]) if not cell.empty else 0}</div>"
                "</div>"
                f"<div style='color:{TOK_C['textPrimary']};"
                "font-size:0.88rem;line-height:1.55;'>"
                f"{verdict_detail}</div>"
                "</div>"
            )

            # Two-column layout: verdict card (left) + F-stats mini-table
            # (right). A lonely 1-row dataframe stacked under the verdict
            # wasted vertical space; side-by-side reads faster and fills
            # the viewport.
            col_v, col_t = st.columns([3, 2])
            with col_v:
                st.markdown(verdict_html, unsafe_allow_html=True)
            with col_t:
                st.markdown(
                    f"<div style='color:{TOK_C['textSecondary']};"
                    "font-size:0.62rem;letter-spacing:0.22em;"
                    "text-transform:uppercase;margin:2px 0 6px 2px;'>"
                    "F-statistics · raw"
                    "</div>",
                    unsafe_allow_html=True,
                )
                tbl = sub[["lag_weeks", "p_value", "f_stat", "n_obs"]].copy()
                tbl.columns = ["lag", "p", "F", "n"]
                st.dataframe(
                    tbl,
                    hide_index=True,
                    width="stretch",
                    column_config={
                        "lag": st.column_config.NumberColumn(format="%d w", width="small"),
                        "p":   st.column_config.NumberColumn(format="%.4f", width="small"),
                        "F":   st.column_config.NumberColumn(format="%.4f", width="small"),
                        "n":   st.column_config.NumberColumn(format="%d",   width="small"),
                    },
                )

            st.caption(
                "Single-cell sample — the full lag × tier heatmap needs at least "
                "3 lags or 2 tiers of cached Granger runs. Run "
                "`python -m signals.granger` with a wider lag sweep to populate it."
            )
            verdict_rendered = True
        else:
            # ── Normal case: full heatmap over a lag × tier grid ──
            z = []
            hover = []
            tier_row_labels = []
            for t in all_tiers:
                row = []
                hrow = []
                for lw in all_lags:
                    cell = gdf[(gdf["tier"] == t) & (gdf["lag_weeks"] == lw)]
                    pv = float(cell["p_value"].iloc[0]) if not cell.empty else float("nan")
                    row.append(pv)
                    fstat = float(cell["f_stat"].iloc[0]) if not cell.empty else float("nan")
                    hrow.append(
                        f"tier {t} · lag {lw}w<br>p = {pv:.4f}<br>F = {fstat:.3f}"
                    )
                z.append(row)
                hover.append(hrow)
                tier_row_labels.append(
                    tier_labels.get(int(t), f"Tier-{t}").split("·")[0].strip()
                )

            heat = go.Figure(data=go.Heatmap(
                z=z,
                x=[f"lag {lw}w" for lw in all_lags],
                y=tier_row_labels,
                zmin=0.0, zmax=1.0,
                # Inverted gradient: red at p<0.05 (falsification fails),
                # accent-sky at p>0.95 (falsification passes).
                colorscale=[
                    [0.00, TOK_C["critical"]],       # p = 0   — reject null
                    [0.05, TOK_C["warn"]],           # p = .05 — borderline
                    [0.50, TOK_C["textMuted"]],      # p = .5  — inconclusive
                    [0.90, TOK_C["borderMuted"]],    # dark muted shoulder
                    [0.95, TOK_C["accent"]],         # threshold — pass glow
                    [1.00, TOK_C["accent"]],         # p = 1 — strong pass
                ],
                colorbar=dict(
                    title=dict(text="p-value",
                               font=dict(color=TOK_C["textSecondary"])),
                    tickvals=[0, 0.05, 0.5, 0.95, 1.0],
                    ticktext=["0", "0.05", "0.5", "0.95", "1"],
                    tickfont=dict(color=TOK_C["textSecondary"],
                                  family=TOK_FONT["mono"]),
                    outlinewidth=0,
                ),
                customdata=hover,
                hovertemplate="%{customdata}<extra></extra>",
                xgap=2, ygap=4,
            ))
            heat.update_layout(
                template="institutional",
                height=max(220, 78 * max(1, len(all_tiers)) + 80),
                margin=dict(l=140, r=40, t=40, b=60),
                xaxis=dict(
                    title=dict(text="weekly lag (BSI → target)",
                               font=dict(color=TOK_C["textSecondary"])),
                    side="bottom",
                    tickfont=dict(family=TOK_FONT["mono"],
                                  color=TOK_C["textSecondary"]),
                ),
                yaxis=dict(
                    tickfont=dict(family=TOK_FONT["sans"],
                                  color=TOK_C["textPrimary"]),
                    autorange="reversed",
                ),
                annotations=[dict(
                    x=0.5, y=1.08, xref="paper", yref="paper",
                    text=("<span style='color:" + TOK_C["accent"] + "'>sky</span>"
                          " = null not rejected (pass) · "
                          "<span style='color:" + TOK_C["critical"] + "'>red</span>"
                          " = null rejected (fail)"),
                    showarrow=False,
                    font=dict(color=TOK_C["textSecondary"],
                              family=TOK_FONT["sans"], size=11),
                    xanchor="center",
                )],
            )
            st.plotly_chart(heat, width="stretch")

        # Dense F-stat table — full-width (with `target` column) only renders
        # in the heatmap branch. In the degenerate branch the compact table
        # already sits beside the verdict card, so we skip it here to avoid
        # a duplicate render.
        if not verdict_rendered:
            st.markdown(
                f"<div style='color:{TOK_C['textSecondary']};font-size:0.78rem;"
                "letter-spacing:0.04em;text-transform:uppercase;"
                "margin:0.6rem 0 0.3rem 0;'>"
                "F-statistics · raw"
                "</div>",
                unsafe_allow_html=True,
            )
            show = sub[["lag_weeks", "p_value", "f_stat", "n_obs", "target_label"]].copy()
            show.columns = ["lag", "p-value", "F", "n", "target"]
            st.dataframe(
                show,
                hide_index=True,
                width="stretch",
                column_config={
                    "lag": st.column_config.NumberColumn(format="%d w", width="small"),
                    "p-value": st.column_config.NumberColumn(format="%.4f", width="small"),
                    "F": st.column_config.NumberColumn(format="%.4f", width="small"),
                    "n": st.column_config.NumberColumn(format="%d", width="small"),
                    "target": st.column_config.TextColumn(width="large"),
                },
            )

        # Palette-matching headline callout — only render in the heatmap
        # branch. The degenerate branch already shipped a full verdict card;
        # rendering this too would be double-speak.
        if not verdict_rendered and sub["p_value"].min() >= 0.95:
            st.markdown(
                "<div style='"
                # Sky-blue gradient wash instead of green — "pass" in the
                # institutional palette is accent/calm, never neon emerald.
                "background:linear-gradient(90deg,"
                "rgba(56,189,248,0.10), rgba(56,189,248,0.02));"
                f"border:1px solid {TOK_C['border']};"
                f"border-left:2px solid {TOK_C['accent']};"
                "padding:0.85rem 1rem;"
                "border-radius:4px;"
                "margin:0.75rem 0 0.25rem 0;'>"
                f"<div style='color:{TOK_C['accent']};"
                f"font-family:{TOK_FONT['mono']};"
                "font-size:0.72rem;letter-spacing:0.08em;"
                "text-transform:uppercase;margin-bottom:0.25rem;'>"
                "Falsification · pass</div>"
                f"<div style='color:{TOK_C['textPrimary']};"
                "font-size:0.88rem;line-height:1.5;'>"
                f"All {len(lags_arr)} lag{'s' if len(lags_arr) != 1 else ''} ≥ 0.95. "
                "The null <em>is not rejected</em> — BSI does "
                "<strong>not</strong> Granger-cause this tier's subprime-credit "
                "stress target. The BNPL signal is idiosyncratic to BNPL, not "
                "a restatement of general subprime stress."
                "</div></div>",
                unsafe_allow_html=True,
            )


# =========================================================================
# TAB 5 — BACKTEST (existing Sprint H.d split-screen Terminal view)
# This block preserves the old `tab1` body verbatim, minus the agent-log
# section which now lives in its own Audit tab.
# =========================================================================
with tab_backtest:
    _what_is_this(
        what=("Out-of-sample event-study P&L on real BNPL catalyst windows. "
              "Compares three strategies: NAIVE, FIX3_ONLY, and INSTITUTIONAL."),
        how=("For each catalyst window, run the TRS arm (junior-tranche "
             "short via total-return swap) under the selected P&L mode, "
             "then plot cumulative P&L daily. Top metrics are Sharpe / "
             "MaxDD / Gross-leverage vs. NAIVE baseline."),
        values=("Sharpe (annualised 252), MaxDD (negative pct), Gross "
                "leverage (× starting capital). Higher Sharpe + less "
                "negative MDD = better."),
        section="§7 — Event study",
    )

    # -------------------------------------------------------------------------
    # SPLIT-SCREEN LAYOUT — [1 : 2]
    # -------------------------------------------------------------------------
    col_left, col_right = st.columns([1, 2], gap="large")


    # =========================================================================
    # LEFT PANE — control & telemetry
    # =========================================================================
    with col_left:
        st.markdown('<div class="panel-title">Control</div>', unsafe_allow_html=True)

        # Date selector: default to most recent BSI observation, or today.
        bsi_df = _bsi_frame()
        if not bsi_df.empty:
            default_date = bsi_df["observed_at"].max().date()
            min_date = bsi_df["observed_at"].min().date()
        else:
            default_date = date.today()
            min_date = date(2018, 1, 1)

        as_of = st.date_input(
            "As-of date",
            value=default_date,
            min_value=min_date,
            max_value=date.today(),
            help="The date the pod treats as 'now'. BSI z, MOVE, and excess-spread "
                 "reads are forward-filled to this day.",
        )

        mode_labels = {
            PnLMode.NAIVE: "NAIVE  (all fixes OFF · look-ahead BSI · spread-only tranche)",
            PnLMode.FIX3_ONLY: "FIX3_ONLY  (causal BSI only · still no sim-layer fixes)",
            PnLMode.INSTITUTIONAL: "INSTITUTIONAL  (all fixes · duration-adjusted)",
        }
        mode = st.radio(
            "P&L mode",
            list(PnLMode),
            index=2,
            format_func=lambda m: mode_labels[m],
        )

        window_key = st.selectbox(
            "Event window",
            list(WINDOWS),
            index=0,
            help="Choose which historical catalyst window drives the three-panel "
                 "backtest on the right.",
        )

        st.markdown('<div class="panel-title">Macro snapshot</div>', unsafe_allow_html=True)

        # Build the radar inputs at as_of.
        def _latest_at(frame: pd.DataFrame, date_col: str, val_col: str) -> float | None:
            if frame.empty:
                return None
            mask = frame[date_col] <= pd.Timestamp(as_of)
            row = frame.loc[mask].tail(1)
            if row.empty:
                return None
            v = row[val_col].iloc[0]
            return None if pd.isna(v) else float(v)

        bsi_z_now = _latest_at(bsi_df, "observed_at", "z_bsi") if not bsi_df.empty else None
        move_now = _latest_at(_fred_frame("MOVE"), "observed_at", "value")
        abs_df = _abs_tranche_frame()
        es_now = _latest_at(abs_df, "period_end", "excess_spread")

        # Small inline metrics.
        m1, m2, m3 = st.columns(3)
        # Behavioural super-threshold bypass (paper §8.5): |BSI z| ≥ 10σ
        # approves on BSI alone, accepting a documented Type-I premium.
        bypass_fired = bsi_z_now is not None and abs(bsi_z_now) >= 10.0
        bypass_label = "BYPASS FIRED" if bypass_fired else "armed"
        m1.metric("BSI z", f"{bsi_z_now:+.2f}" if bsi_z_now is not None else "—",
                  delta=bypass_label,
                  delta_color=("off" if not bypass_fired else "inverse"),
                  help="Causal 180-day rolling z of BSI. Gate 1 fires at ≥ 1.5σ. "
                       "Super-threshold bypass fires at |z| ≥ 10σ (behavioural "
                       "top-of-funnel panic override; paper §8.5).")
        m2.metric("MOVE", f"{move_now:.1f}" if move_now is not None else "—",
                  help="ICE BofA MOVE Index (bond vol). Gate 2 fires on MA30 ≥ 120.")
        m3.metric("AFRMT ES%", f"{es_now:.2f}" if es_now is not None else "—",
                  help="Most recent AFRMT trustee excess spread (annualized %). "
                       "Note: SCP is no longer a gate — it is non-gating "
                       "telemetry retained for audit (paper §7).")

        # Agent Debate Log moved to its own tab (Audit) in the 5-tab refactor.
        # Not repeated here — the Backtest tab stays focused on P&L + metrics.


    # =========================================================================
    # RIGHT PANE — artifact canvas
    # =========================================================================
    with col_right:
        st.markdown('<div class="panel-title">Backtest artifact</div>',
                    unsafe_allow_html=True)

        # Attempt the three-panel load. If BSI / HYG / AFRM isn't in the
        # warehouse yet, catch and render an explanatory banner so the screen
        # doesn't crash — the user can still use the radar + log panes.
        cmp = None
        fx = None
        bt_err: str | None = None
        try:
            fx, cmp = _three_panel_comparison(window_key)
        except Exception as e:  # noqa: BLE001
            bt_err = f"{type(e).__name__}: {e}"

        # ---- Top row metrics: Sharpe, MDD, Gross Leverage -----------------
        if cmp is not None:
            sel_panel = cmp.panels[mode]
            naive_panel = cmp.panels[PnLMode.NAIVE]

            sharpe = sel_panel.trs_stats.sharpe
            sharpe_delta = sharpe - naive_panel.trs_stats.sharpe

            mdd = sel_panel.trs_stats.max_drawdown
            mdd_delta = mdd - naive_panel.trs_stats.max_drawdown

            # Gross leverage for the TRS arm — extract from the final state's
            # absolute TRS notional / starting capital. Fall back to 0 if missing.
            try:
                gross = abs(sel_panel.trs_final_state.trs_notional) + \
                        abs(sel_panel.trs_final_state.hedge_notional)
            except Exception:
                gross = 0.0

            k1, k2, k3 = st.columns(3)
            k1.metric(
                "Sharpe (TRS arm)",
                f"{sharpe:+.2f}",
                f"{sharpe_delta:+.2f} vs NAIVE",
                delta_color="normal",
            )
            k2.metric(
                "Max Drawdown",
                f"{mdd:+.4f}",
                f"{mdd_delta:+.4f} vs NAIVE",
                # MDD is negative; smaller |MDD| is better — inverse color.
                delta_color="inverse",
            )
            _days_on = int(sel_panel.gate_approved.sum())
            _days_total = int(sel_panel.gate_approved.size)
            k3.metric(
                "Gross Leverage",
                f"{gross:.2f}×",
                help=f"{_days_on}/{_days_total} days the 3-gate AND approved a TRS position in this window.",
            )
            st.caption(
                f"Gate approvals: {_days_on}/{_days_total} days in this window. "
                f"Mode = {mode.value.upper()}."
            )
        else:
            # Placeholder cards so the layout doesn't collapse.
            k1, k2, k3 = st.columns(3)
            k1.metric("Sharpe (TRS arm)", "—")
            k2.metric("Max Drawdown", "—")
            k3.metric("Gross Leverage", "—")

        # ---- Visual 1: Three-Panel Backtest Chart -------------------------
        if cmp is not None and fx is not None:
            st.plotly_chart(_three_panel_chart(fx, cmp), width="stretch")
            st.caption(
                f"Window **{window_key}** · catalyst {WINDOWS[window_key].catalyst_date.isoformat()} · "
                f"T = {len(fx.dates)} business days · "
                f"duration adjustment is **on** for INSTITUTIONAL, **off** for NAIVE / FIX3_ONLY. "
                f"Positive slope = TRS-short profits."
            )
        else:
            st.error(
                "Three-panel backtest unavailable — the warehouse is missing one "
                "of MOVE / SOFR / HYG / AFRM / BSI for this window.\n\n"
                "Recovery path:\n"
                "  • `python -m data.ingest.fred`\n"
                "  • `python -m data.ingest.yahoo_macro`\n"
                "  • `python -m data.ingest.regulatory_catalysts`\n"
                "  • `python -m signals.bsi`\n\n"
                f"Detail: `{bt_err}`"
            )

        # ---- Visual 2: Macro Radar ---------------------------------------
        st.markdown('<div class="panel-title">Macro radar · current regime</div>',
                    unsafe_allow_html=True)
        radar_bsi = bsi_z_now if bsi_z_now is not None else 0.0
        radar_move = move_now if move_now is not None else 100.0
        radar_es = es_now if es_now is not None else 6.0
        st.plotly_chart(
            _macro_radar(radar_bsi, radar_move, radar_es),
            width="stretch",
        )
        st.caption(
            "Blue polygon = current state (normalized to 0-1). Dashed ring = reference "
            "stress envelope (BSI ≥ 1.5σ, MOVE ≥ 120, AFRMT ES ≥ 8 %). Post-SCP-demotion "
            "(paper §7) approval is BSI × MOVE × CCD-II catalyst; the AFRMT excess-spread "
            "axis on this radar is diagnostic, not a gate. A |BSI z| ≥ 10σ reading trips "
            "the behavioural super-threshold bypass regardless of the other axes."
        )

    # -------------------------------------------------------------------------
    # FOOTER
    # -------------------------------------------------------------------------
    st.markdown(
        f"<div style='color:{TOK_C['textMuted']};font-size:0.7rem;"
        "margin-top:24px;text-align:right;'>"
        f"warehouse = <code>{settings.duckdb_path.name}</code> · "
        f"as-of = {as_of.isoformat()} · "
        f"mode = {mode.value} · window = {window_key}"
        f"</div>",
        unsafe_allow_html=True,
    )


# =========================================================================
# TAB 3 — MATH · JT PRICING (Jarrow-Turnbull hazard + survival)
# =========================================================================
with tab_math:
    _what_is_this(
        what=("Jarrow-Turnbull default-intensity simulation. Turns current "
              "BSI + MOVE into a hazard rate, then Monte-Carlos CIR hazard "
              "paths and reports survival quantiles for the AFRMT junior "
              "tranche."),
        how=("λ₀ = α + β_bsi·max(BSI, 0) + β_move·max(MOVE-80, 0) (paper "
             "defaults from `affine_hazard`). CIR: dλ = κ(θ-λ)dt + σ√λ dW, "
             "full-truncation Euler, n_paths user-set. S(T) = exp(-∫ λ du)."),
        values=("λ₀ in [0.002, 0.25] range per thresholds.yaml. Survival S(T)"
                " ∈ [0,1], reported as 5/50/95 percentiles. Lower S(T) = "
                "more default risk priced in."),
        section="§4.6 — Jarrow-Turnbull · §9 — Pricing",
    )

    jt_left, jt_right = st.columns([1, 3], gap="large")
    with jt_left:
        st.markdown('<div class="panel-title">Live inputs</div>',
                    unsafe_allow_html=True)
        bsi_df_jt = _bsi_frame()
        bsi_now_jt = (float(bsi_df_jt["z_bsi"].dropna().iloc[-1])
                      if not bsi_df_jt.empty and bsi_df_jt["z_bsi"].dropna().size
                      else 0.0)
        move_series = _fred_frame("MOVE")
        move_now_jt = (float(move_series["value"].dropna().iloc[-1])
                       if not move_series.empty and move_series["value"].dropna().size
                       else 100.0)
        st.metric("BSI z (live)", f"{bsi_now_jt:+.2f} σ",
                  help="Most recent `bsi_daily.z_bsi` in the warehouse.")
        st.metric("MOVE (live)", f"{move_now_jt:.1f}",
                  help="Most recent `fred_series` MOVE row.")

        st.markdown('<div class="panel-title">Simulation</div>',
                    unsafe_allow_html=True)
        horizon = st.slider("Horizon (days)", 30, 720, 365, 30,
                            help="Simulation horizon in calendar days.")
        n_paths = st.select_slider("Paths", options=[100, 200, 500, 1000, 2000],
                                   value=200,
                                   help="Monte-Carlo paths. 200 is usually enough "
                                        "for visible band width at 1-year horizon.")

        with st.expander("Affine-link coefficients (paper defaults)", expanded=False):
            alpha = st.slider("α (idiosyncratic floor)", 0.001, 0.030,
                              0.008, step=0.001, format="%.3f",
                              help="Baseline hazard when BSI=0 and MOVE≤80.")
            beta_bsi = st.slider("β_BSI (sentiment sensitivity)", 0.001, 0.020,
                                 0.004, step=0.001, format="%.3f",
                                 help="Hazard added per σ of positive BSI.")
            beta_move = st.slider("β_MOVE (macro-vol drag)", 0.00005, 0.00050,
                                  0.00015, step=0.00005, format="%.5f",
                                  help="Hazard added per bp of MOVE above 80.")

        with st.expander("CIR dynamics (advanced)", expanded=False):
            kappa = st.slider("κ (mean-reversion)", 0.1, 3.0, 0.8, 0.1,
                              help="Speed of hazard reversion to θ.")
            theta = st.slider("θ (long-run mean)", 0.005, 0.100, 0.020, 0.005,
                              help="Long-run equilibrium hazard.")
            sigma = st.slider("σ (hazard vol)", 0.01, 0.40, 0.10, 0.01,
                              help="Diffusion coefficient; clipped to respect Feller.")

    with jt_right:
        sim = _jt_simulate(
            bsi=bsi_now_jt, move=move_now_jt,
            alpha=alpha, beta_bsi=beta_bsi, beta_move=beta_move,
            kappa=kappa, theta=theta, sigma=sigma,
            horizon_days=int(horizon), n_paths=int(n_paths),
        )

        _horizon_lbl = f"{int(horizon)}d"
        # Default-probability at the median gives a more intuitive headline
        # than four survival quantiles, while the band [P5, P95] shows dispersion.
        _pd50 = 1.0 - sim["surv_p50"]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(
            "λ₀ (today)",
            f"{sim['lambda0'] * 10000:.0f} bp",
            help="Affine-hazard implied by current BSI/MOVE, expressed in "
                 "basis points of instantaneous default intensity.",
        )
        m2.metric(
            f"PD · median ({_horizon_lbl})",
            f"{_pd50 * 100:.2f}%",
            help="1 − S(T) at the 50th percentile path. Higher = more "
                 "default risk priced into the AFRMT junior tranche.",
        )
        m3.metric(
            f"S(T) · P5 ({_horizon_lbl})",
            f"{sim['surv_p05']:.3f}",
            help="5th-percentile survival — worst-case tail, stressed-regime floor.",
        )
        m4.metric(
            f"S(T) · P95 ({_horizon_lbl})",
            f"{sim['surv_p95']:.3f}",
            help="95th-percentile survival — best-case tail, benign-regime ceiling.",
        )

        # Dual-regime overlay: in addition to the live STRESS sim, run a
        # benign-baseline NORMAL sim (BSI=0, MOVE=100) so the two hazard
        # curves can be read against each other under a single unified-x
        # tooltip. Smooth-interpolated (spline) because linear step
        # interpolation on hazard paths looks jagged and we want the eye
        # to follow trend-of-median, not per-sample noise.
        sim_normal = _jt_simulate(
            bsi=0.0, move=100.0,
            alpha=alpha, beta_bsi=beta_bsi, beta_move=beta_move,
            kappa=kappa, theta=theta, sigma=sigma,
            horizon_days=int(horizon), n_paths=int(n_paths),
        )

        paths = sim["paths"]
        t_axis = np.arange(paths.shape[1])
        stress_p05 = np.percentile(paths, 5,  axis=0)
        stress_p50 = np.percentile(paths, 50, axis=0)
        stress_p95 = np.percentile(paths, 95, axis=0)
        normal_p50 = np.percentile(sim_normal["paths"], 50, axis=0)

        fig_jt = go.Figure()
        # Stress 5–95 percentile band (tonexty pairs with the preceding trace)
        fig_jt.add_scatter(
            x=t_axis, y=stress_p95, mode="lines",
            line=dict(color=TOK_C["critical"], width=0, shape="spline"),
            hoverinfo="skip", showlegend=False,
            name="stress · P95",
        )
        fig_jt.add_scatter(
            x=t_axis, y=stress_p05, mode="lines",
            line=dict(color=TOK_C["critical"], width=0, shape="spline"),
            fill="tonexty", fillcolor="rgba(239,68,68,0.14)",
            hoverinfo="skip", showlegend=False,
            name="stress · P5",
        )
        # Stress regime median
        fig_jt.add_scatter(
            x=t_axis, y=stress_p50, mode="lines",
            line=dict(color=TOK_C["critical"], width=2, shape="spline"),
            name="Stress Regime · median λ(t)",
            hovertemplate="t=%{x}d<br>λ stress = %{y:.4f}<extra></extra>",
        )
        # Normal regime median (benign baseline)
        fig_jt.add_scatter(
            x=t_axis, y=normal_p50, mode="lines",
            line=dict(color=TOK_C["accent"], width=2, shape="spline",
                      dash="solid"),
            name="Normal Regime · median λ(t)",
            hovertemplate="t=%{x}d<br>λ normal = %{y:.4f}<extra></extra>",
        )
        fig_jt.add_hline(
            y=sim["lambda0"], line_dash="dot", line_color=TOK_C["warn"],
            annotation_text=f" λ₀ = {sim['lambda0']:.4f}",
            annotation_position="top left",
            annotation_font_color=TOK_C["warn"],
        )
        fig_jt.update_layout(
            template="institutional",
            height=380,
            hovermode="x unified",
            xaxis=dict(title=dict(text="Days ahead",
                                  font=dict(color=TOK_C["textSecondary"])),
                       tickfont=dict(family=TOK_FONT["mono"], size=10)),
            yaxis=dict(title=dict(text="Hazard rate λ(t)",
                                  font=dict(color=TOK_C["textSecondary"])),
                       tickfont=dict(family=TOK_FONT["mono"], size=10)),
            margin=dict(l=50, r=20, t=20, b=40),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                        bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig_jt, width="stretch")

        # Survival histogram — kept for diagnostic (path-level dispersion),
        # now re-themed with token palette and JetBrains Mono tickfonts.
        fig_s = go.Figure()
        fig_s.add_histogram(
            x=sim["surv"], nbinsx=40,
            marker=dict(color=TOK_C["accent"], line=dict(color=TOK_C["card"], width=1)),
            name="S(T)",
        )
        for pct, color, label in (("surv_p05", TOK_C["critical"], "P5"),
                                  ("surv_p50", TOK_C["accent"],   "P50"),
                                  ("surv_p95", TOK_C["warn"],     "P95")):
            fig_s.add_vline(
                x=sim[pct], line_dash="dash", line_color=color,
                annotation_text=label, annotation_font_color=color,
            )
        fig_s.update_layout(
            template="institutional",
            height=240,
            xaxis=dict(title=dict(text=f"Survival S({int(horizon)}d)",
                                  font=dict(color=TOK_C["textSecondary"])),
                       range=[0, 1],
                       tickfont=dict(family=TOK_FONT["mono"], size=10)),
            yaxis=dict(title=dict(text="Path count",
                                  font=dict(color=TOK_C["textSecondary"])),
                       tickfont=dict(family=TOK_FONT["mono"], size=10)),
            margin=dict(l=50, r=20, t=10, b=40),
            showlegend=False,
        )
        st.plotly_chart(fig_s, width="stretch")


# =========================================================================
# TAB 4 — AUDIT · AGENTS (LLM debate log — advisory only)
# =========================================================================
with tab_audit:
    _what_is_this(
        what=("Live tail of LLM agent decisions. Macro, Quant, and Risk "
              "agents emit JSON reasoning each tick; these rows are the "
              "audit trail, never the approval authority."),
        how=("Reads `logs/agent_decisions/YYYY-MM-DD.jsonl` from the last "
             "N days. Each row: ts · role · provider · model · latency · "
             "token count · error (if any)."),
        values=("Role-filter chips: MACRO (cyan), QUANT (violet), RISK "
                "(amber). Latency in ms. Tokens include both prompt + "
                "completion."),
        section="§3 — LangGraph pod · §4.4 — compliance boundary",
    )

    audit_cols = st.columns([1, 3, 1])
    with audit_cols[0]:
        n_days_audit = st.selectbox("Days", [1, 3, 7, 14], index=1,
                                    help="How many days of JSONL to tail.")
    with audit_cols[1]:
        role_chips = st.multiselect(
            "Roles",
            ["macro", "quant", "risk"],
            default=["macro", "quant", "risk"],
            help="Show only these agent roles. Clear to see all.",
        )
    with audit_cols[2]:
        height_audit = st.selectbox("Height (px)", [320, 520, 720], index=1)

    _render_agent_debate_log(
        n_days=int(n_days_audit),
        height_px=int(height_audit),
        role_filter=set(role_chips) if role_chips else None,
    )


# =========================================================================
# TAB 2 — FUNNEL · ALT-DATA COVERAGE
# Descriptive stats over every warehouse table + trend plots for populated
# tables. Empty tables are greyed out with a remediation hint so the reader
# sees honestly which signals are driving BSI today.
# =========================================================================
with tab_funnel:
    _what_is_this(
        what=("Ingest-coverage audit: which BSI pillars and warehouse tables "
              "are actually populated, and which are waiting on backfill."),
        how=("Per-table row counts + date-range scan + trend chart when the "
             "table carries a time column. Empty tables list their ingest "
             "command so nothing is hidden."),
        values=("Coverage = non-null rows / total days. Populated columns "
                "(c_cfpb, c_trends, c_reddit, c_move, c_vitality) feed BSI. "
                "c_appstore deferred to Sprint B."),
        section="§4.2 — BSI construction · §10 — data limitations",
    )

    # =====================================================================
    # STAGE COLUMN · four-stage BSI-construction pipeline at a glance.
    # Row-wise: Raw Ingest → FinBERT Sentiment → Pillar Weights + Freeze
    # → Residual z_bsi. Chosen over a Sankey because rigor > decoration:
    # we care about the _rigor of each stage_ — counts, distributions,
    # weight sanity, today's residual — not the flow.
    # =====================================================================
    st.markdown('<div class="panel-title">BSI pipeline · stage view</div>',
                unsafe_allow_html=True)

    @st.cache_data(ttl=120, show_spinner=False)
    def _pillar_counts() -> dict:
        con = _con()
        cols = ["c_cfpb", "c_trends", "c_reddit", "c_appstore",
                "c_move", "c_vitality"]
        out: dict = {}
        for c in cols:
            try:
                n = con.execute(
                    f'SELECT COUNT(*) FROM bsi_daily WHERE {c} IS NOT NULL'
                ).fetchone()[0]
            except Exception:  # noqa: BLE001
                n = 0
            out[c] = int(n)
        return out

    @st.cache_data(ttl=120, show_spinner=False)
    def _finbert_scores() -> pd.DataFrame:
        """Pulls sentiment scores from the complaint-level tables. Falls back
        to empty frame if tables are absent or the `sentiment` column is
        missing (pre-FinBERT backfill)."""
        con = _con()
        rows: list[pd.DataFrame] = []
        for q in (
            "SELECT 'cfpb' AS source, sentiment FROM cfpb_complaints "
            "WHERE sentiment IS NOT NULL",
            "SELECT 'appstore' AS source, sentiment FROM appstore_reviews "
            "WHERE sentiment IS NOT NULL",
        ):
            try:
                df = con.execute(q).df()
                if not df.empty:
                    rows.append(df)
            except Exception:  # noqa: BLE001
                continue
        if not rows:
            return pd.DataFrame(columns=["source", "sentiment"])
        return pd.concat(rows, ignore_index=True)

    @st.cache_data(ttl=120, show_spinner=False)
    def _pillar_weights() -> pd.DataFrame:
        path = Path(__file__).resolve().parent.parent / "config" / "weights.yaml"
        defaults: dict = {}
        try:
            import yaml  # lazy — avoid a hard dep if config/ missing
            defaults = (yaml.safe_load(path.read_text())
                          .get("default_weights") or {})
        except Exception:  # noqa: BLE001
            defaults = {
                "cfpb_complaint_momentum": 0.25,
                "google_trends_distress":  0.20,
                "reddit_finbert_neg":      0.20,
                "appstore_keyword_freq":   0.15,
                "move_index_overlay":      0.20,
            }
        return pd.DataFrame(
            [{"pillar": k, "weight": float(v)} for k, v in defaults.items()]
        )

    @st.cache_data(ttl=120, show_spinner=False)
    def _freeze_count_180d() -> int:
        try:
            n = _con().execute(
                "SELECT COUNT(*) FROM bsi_daily "
                "WHERE freeze_flag = TRUE "
                "  AND observed_at >= CURRENT_DATE - INTERVAL 180 DAY"
            ).fetchone()[0]
            return int(n)
        except Exception:  # noqa: BLE001
            return 0

    @st.cache_data(ttl=120, show_spinner=False)
    def _z_bsi_180d() -> pd.DataFrame:
        try:
            df = _con().execute(
                "SELECT observed_at, z_bsi FROM bsi_daily "
                "WHERE observed_at >= CURRENT_DATE - INTERVAL 180 DAY "
                "  AND z_bsi IS NOT NULL "
                "ORDER BY observed_at"
            ).df()
            if not df.empty:
                df["observed_at"] = pd.to_datetime(df["observed_at"])
            return df
        except Exception:  # noqa: BLE001
            return pd.DataFrame(columns=["observed_at", "z_bsi"])

    col_raw, col_finb, col_w, col_z = st.columns([1, 1, 1, 1], gap="small")

    # ─── Stage 1 · Raw Ingest ───────────────────────────────────────────
    with col_raw:
        st.markdown(
            f"<div style='font-size:0.72rem;letter-spacing:0.22em;"
            f"text-transform:uppercase;color:{TOK_C['textSecondary']};"
            f"margin-bottom:6px;'>1 · Raw Ingest</div>",
            unsafe_allow_html=True,
        )
        pc = _pillar_counts()
        pc_df = pd.DataFrame(
            [{"pillar": k.replace("c_", ""), "rows": v} for k, v in pc.items()]
        ).sort_values("rows", ascending=True)
        fig_raw = go.Figure(go.Bar(
            y=pc_df["pillar"], x=pc_df["rows"],
            orientation="h",
            marker=dict(color=TOK_C["accent"]),
            text=pc_df["rows"].map(lambda n: f"{n:,}"),
            textposition="outside",
            textfont=dict(family=TOK_FONT["mono"], color=TOK_C["textSecondary"], size=10),
            hovertemplate="%{y}: %{x:,}<extra></extra>",
        ))
        fig_raw.update_layout(
            template="institutional",
            height=180,
            margin=dict(l=70, r=40, t=10, b=20),
            xaxis=dict(showgrid=False, visible=False),
            yaxis=dict(tickfont=dict(family=TOK_FONT["mono"], size=10)),
            showlegend=False,
        )
        st.plotly_chart(fig_raw, width="stretch",
                        config={"displayModeBar": False})
        with st.expander("ℹ What is this stage?"):
            st.markdown(
                "**What** · row counts for each of the six BSI pillar columns "
                "in `bsi_daily`.\n\n"
                "**How** · `SELECT COUNT(*) WHERE c_<pillar> IS NOT NULL`.\n\n"
                "**Values** · thousands = backfilled; zero = deferred / "
                "awaiting ingest.\n\n"
                "**§** · §4.2 — BSI construction"
            )

    # ─── Stage 2 · FinBERT Sentiment ────────────────────────────────────
    with col_finb:
        st.markdown(
            f"<div style='font-size:0.72rem;letter-spacing:0.22em;"
            f"text-transform:uppercase;color:{TOK_C['textSecondary']};"
            f"margin-bottom:6px;'>2 · FinBERT Sentiment</div>",
            unsafe_allow_html=True,
        )
        fdf = _finbert_scores()
        if fdf.empty:
            st.caption("No FinBERT-scored rows yet. Run complaint/review "
                       "ingest + sentiment backfill.")
        else:
            fig_fb = go.Figure()
            for src in fdf["source"].unique():
                fig_fb.add_trace(go.Histogram(
                    x=fdf[fdf["source"] == src]["sentiment"],
                    name=str(src),
                    opacity=0.75,
                    marker=dict(color=(TOK_C["accent"] if src == "cfpb"
                                       else TOK_C["violet"])),
                    nbinsx=24,
                ))
            fig_fb.update_layout(
                template="institutional",
                height=180, barmode="overlay",
                margin=dict(l=30, r=10, t=10, b=20),
                legend=dict(orientation="h", y=1.22, x=0,
                            bgcolor="rgba(0,0,0,0)",
                            font=dict(size=9)),
                xaxis=dict(title=dict(text="sentiment",
                                      font=dict(size=10)),
                           tickfont=dict(family=TOK_FONT["mono"], size=9)),
                yaxis=dict(showgrid=False, visible=False),
            )
            st.plotly_chart(fig_fb, width="stretch",
                            config={"displayModeBar": False})
        with st.expander("ℹ What is this stage?"):
            st.markdown(
                "**What** · distribution of FinBERT negative-sentiment scores "
                "feeding the reddit/appstore pillars.\n\n"
                "**How** · joined from `cfpb_complaints.sentiment` + "
                "`appstore_reviews.sentiment`.\n\n"
                "**Values** · scores on [-1, +1]; mass above 0 = negative.\n\n"
                "**§** · §4.2 — BSI construction"
            )

    # ─── Stage 3 · Pillar Weights & Freeze ──────────────────────────────
    with col_w:
        st.markdown(
            f"<div style='font-size:0.72rem;letter-spacing:0.22em;"
            f"text-transform:uppercase;color:{TOK_C['textSecondary']};"
            f"margin-bottom:6px;'>3 · Weights &amp; Freeze</div>",
            unsafe_allow_html=True,
        )
        wdf = _pillar_weights()
        st.dataframe(
            wdf.style.format({"weight": "{:.2f}"}),
            width="stretch", hide_index=True,
        )
        freezes = _freeze_count_180d()
        st.markdown(
            f"<div style='margin-top:6px;font-family:{TOK_FONT['mono']};"
            f"font-size:0.85rem;color:{TOK_C['textPrimary']};'>"
            f"freeze_flag · 180d · <b>{freezes}</b>"
            f"</div>",
            unsafe_allow_html=True,
        )
        with st.expander("ℹ What is this stage?"):
            st.markdown(
                "**What** · current default pillar weights (priors) + count "
                "of days the firm-vitality freeze fired over the last 180d.\n\n"
                "**How** · weights from `config/weights.yaml`; freeze count "
                "from `bsi_daily.freeze_flag = TRUE`.\n\n"
                "**Values** · weights sum to 1.0; freeze rare (≤2/y in normal "
                "regime).\n\n"
                "**§** · §4.2 — BSI construction · §10 — frozen weights"
            )

    # ─── Stage 4 · Residual z_bsi ───────────────────────────────────────
    with col_z:
        st.markdown(
            f"<div style='font-size:0.72rem;letter-spacing:0.22em;"
            f"text-transform:uppercase;color:{TOK_C['textSecondary']};"
            f"margin-bottom:6px;'>4 · Residual z_bsi · 180d</div>",
            unsafe_allow_html=True,
        )
        zdf = _z_bsi_180d()
        if zdf.empty:
            st.caption("No z_bsi rows in the last 180d. Populate `bsi_daily`.")
        else:
            last = float(zdf["z_bsi"].iloc[-1])
            is_red = last >= 1.5
            line_c = TOK_C["critical"] if is_red else TOK_C["accent"]
            fig_z = go.Figure()
            fig_z.add_trace(go.Scatter(
                x=zdf["observed_at"], y=zdf["z_bsi"],
                mode="lines",
                line=dict(shape="spline", color=line_c, width=1.5),
                fill="tozeroy",
                fillcolor=(f"rgba(56,189,248,0.12)" if not is_red
                           else f"rgba(239,68,68,0.14)"),
                hovertemplate="%{x|%Y-%m-%d}<br>z = %{y:.2f}<extra></extra>",
            ))
            fig_z.add_hline(
                y=1.5, line_dash="dash",
                line_color=TOK_C["warn"], line_width=1,
                annotation=dict(text="+1.5σ",
                                font=dict(color=TOK_C["warn"], size=9),
                                xanchor="left"),
            )
            fig_z.update_layout(
                template="institutional",
                height=150,
                margin=dict(l=30, r=10, t=10, b=20),
                xaxis=dict(showgrid=False,
                           tickfont=dict(family=TOK_FONT["mono"], size=9)),
                yaxis=dict(tickfont=dict(family=TOK_FONT["mono"], size=9)),
                showlegend=False,
            )
            st.plotly_chart(fig_z, width="stretch",
                            config={"displayModeBar": False})
            st.markdown(
                f"<div style='font-family:{TOK_FONT['mono']};"
                f"font-size:1rem;color:{line_c};'>"
                f"today · {last:+.2f}σ"
                f"</div>",
                unsafe_allow_html=True,
            )
        with st.expander("ℹ What is this stage?"):
            st.markdown(
                "**What** · the output: residual z-score `z_bsi`, 180d "
                "trajectory with the Gate-1 threshold.\n\n"
                "**How** · weighted pillar aggregate → 180d causal residual "
                "z. Dashed amber = +1.5σ (Gate 1 fires above).\n\n"
                "**Values** · typical band −3 to +5. Line flips red when "
                "z ≥ +1.5σ.\n\n"
                "**§** · §4.2 — BSI construction"
            )

    st.markdown("---")

    # --------- shared loaders for this tab (cached separately from tab1) ---
    @st.cache_data(ttl=120, show_spinner=False)
    def _table_inventory() -> pd.DataFrame:
        """One row per warehouse table: rows, date-column coverage, status."""
        con = _con()
        # (table_name, date_col or None) — None for tables with no natural
        # time column (granger_results uses run_at, regulatory_catalysts uses
        # deadline_date).
        specs = [
            ("bsi_daily",            "observed_at"),
            ("fred_series",          "observed_at"),
            ("google_trends",        "observed_at"),
            ("options_chain",        "observed_at"),
            ("short_interest",       "observed_at"),
            ("regulatory_catalysts", "deadline_date"),
            ("granger_results",      "run_at"),
            ("abs_tranche_metrics",  "period_end"),
            ("cfpb_complaints",      "received_at"),
            ("reddit_posts",         "created_at"),
            ("firm_vitality",        "observed_at"),
            ("scp_daily",            "observed_at"),
            ("jt_lambda",            "observed_at"),
            ("sec_filings_index",    "filed_at"),
            ("pod_decisions",        "as_of"),
            ("portfolio_weights",    "issued_at"),
            ("portfolio_hedges",     "issued_at"),
            ("squeeze_defense",      "observed_at"),
        ]
        out = []
        for t, dc in specs:
            try:
                n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            except Exception:
                out.append({"table": t, "rows": 0, "min_date": None,
                            "max_date": None, "status": "MISSING"})
                continue
            mn, mx = None, None
            if dc and n > 0:
                try:
                    r = con.execute(
                        f'SELECT MIN({dc}), MAX({dc}) FROM "{t}"'
                    ).fetchone()
                    mn, mx = r[0], r[1]
                except Exception:
                    pass
            status = "POPULATED" if n > 0 else "EMPTY"
            out.append({"table": t, "rows": int(n),
                        "min_date": mn, "max_date": mx, "status": status})
        return pd.DataFrame(out)

    @st.cache_data(ttl=120, show_spinner=False)
    def _fred_all() -> pd.DataFrame:
        df = _con().execute(
            "SELECT series_id, observed_at, value FROM fred_series "
            "ORDER BY series_id, observed_at"
        ).df()
        if not df.empty:
            df["observed_at"] = pd.to_datetime(df["observed_at"])
        return df

    @st.cache_data(ttl=120, show_spinner=False)
    def _trends_all() -> pd.DataFrame:
        df = _con().execute(
            "SELECT keyword, observed_at, interest FROM google_trends "
            "ORDER BY keyword, observed_at"
        ).df()
        if not df.empty:
            df["observed_at"] = pd.to_datetime(df["observed_at"])
        return df

    @st.cache_data(ttl=120, show_spinner=False)
    def _options_all() -> pd.DataFrame:
        df = _con().execute(
            "SELECT ticker, expiry, strike, option_type, iv, "
            "       underlying_price, observed_at "
            "FROM options_chain WHERE iv IS NOT NULL "
            "ORDER BY ticker, expiry, strike"
        ).df()
        return df

    @st.cache_data(ttl=120, show_spinner=False)
    def _short_int_all() -> pd.DataFrame:
        return _con().execute(
            "SELECT ticker, observed_at, shares_short, free_float, "
            "       utilization, days_to_cover "
            "FROM short_interest ORDER BY ticker, observed_at"
        ).df()

    @st.cache_data(ttl=120, show_spinner=False)
    def _catalysts_all() -> pd.DataFrame:
        return _con().execute(
            "SELECT jurisdiction, deadline_date, title, materiality, category "
            "FROM regulatory_catalysts ORDER BY deadline_date"
        ).df()

    @st.cache_data(ttl=120, show_spinner=False)
    def _granger_all() -> pd.DataFrame:
        return _con().execute(
            "SELECT run_at, lag_weeks, f_stat, p_value, n_obs "
            "FROM granger_results ORDER BY run_at DESC, lag_weeks"
        ).df()

    # --------- Palette reused across plots -------------------------------
    # Palette sourced from the institutional token set so Tab 5 backtest
    # charts read identically to the rest of the app.
    PLOT_BG = TOK_C["card"]
    GRID = TOK_C["chartGrid"]
    TEXT = TOK_C["textPrimary"]
    LINE_PALETTE = [
        TOK_C["accent"], TOK_C["critical"], TOK_C["violet"],
        TOK_C["warn"], TOK_C["textSecondary"],
    ]

    def _style(fig: go.Figure, height: int = 320) -> go.Figure:
        """Thin wrapper — applies the registered ``institutional`` template
        plus the per-chart margin/height/legend overrides this tab needs.
        The institutional template is set as Plotly default on import of
        :mod:`dashboard.plotly_theme`, so all figures already inherit the
        institutional background + font; we only patch the layout knobs
        that vary per-call."""
        fig.update_layout(
            template="institutional",
            margin=dict(l=40, r=20, t=30, b=30),
            height=height,
            hovermode="x unified",
            legend=dict(orientation="h", y=1.12, x=0,
                        bgcolor="rgba(0,0,0,0)"),
        )
        return fig

    # =====================================================================
    # SECTION 1 · Warehouse inventory
    # =====================================================================
    st.markdown('<div class="panel-title">Warehouse inventory</div>',
                unsafe_allow_html=True)

    inv = _table_inventory()
    n_pop = int((inv["status"] == "POPULATED").sum())
    n_emp = int((inv["status"] == "EMPTY").sum())
    total_rows = int(inv["rows"].sum())

    iv1, iv2, iv3, iv4 = st.columns(4)
    iv1.metric("Tables total", len(inv))
    iv2.metric("Populated", f"{n_pop} / {len(inv)}")
    iv3.metric("Empty", f"{n_emp} / {len(inv)}")
    iv4.metric("Total rows", f"{total_rows:,}")

    # Present populated first, empty second — with a visible status color.
    inv_display = inv.copy()
    inv_display["min_date"] = inv_display["min_date"].astype(str).replace("None", "—")
    inv_display["max_date"] = inv_display["max_date"].astype(str).replace("None", "—")
    inv_display = inv_display.sort_values(
        by=["status", "rows"], ascending=[True, False]
    )

    def _row_style(row: pd.Series) -> list[str]:
        # Populated = card-tone; Empty = dim-muted (not alarmist red).
        # Token-sourced so it tracks the rest of the app's palette.
        color = TOK_C["card"] if row["status"] == "POPULATED" else TOK_C["borderMuted"]
        text = TOK_C["textPrimary"] if row["status"] == "POPULATED" else TOK_C["textMuted"]
        return [f"background-color:{color};color:{text};"] * len(row)

    st.dataframe(
        inv_display.style.apply(_row_style, axis=1),
        width="stretch", hide_index=True,
    )

    # =====================================================================
    # SECTION 2 · FRED macro series (primary populated signal source)
    # =====================================================================
    st.markdown('<div class="panel-title">FRED macro series</div>',
                unsafe_allow_html=True)

    fred_df = _fred_all()
    if fred_df.empty:
        st.caption("fred_series is empty. Run `python -m data.ingest.fred`.")
    else:
        series_ids = sorted(fred_df["series_id"].unique())
        # Per-series stats table.
        stats_rows = []
        for sid in series_ids:
            sdf = fred_df[fred_df["series_id"] == sid]
            stats_rows.append({
                "series": sid,
                "n": len(sdf),
                "from": sdf["observed_at"].min().date().isoformat(),
                "to":   sdf["observed_at"].max().date().isoformat(),
                "last": float(sdf["value"].iloc[-1]),
                "mean": float(sdf["value"].mean()),
                "std":  float(sdf["value"].std()),
            })
        st.dataframe(pd.DataFrame(stats_rows), width="stretch",
                     hide_index=True)

        # Two charts: (a) big-panel normalized overlay, (b) faceted raw
        # levels so scale-dominant series (DGS10 vs MOVE vs ICSA) don't
        # crush each other. Normalization = (x - mean) / std per series.
        fig_norm = go.Figure()
        for i, sid in enumerate(series_ids):
            sdf = fred_df[fred_df["series_id"] == sid].sort_values("observed_at")
            if len(sdf) < 2 or sdf["value"].std() == 0:
                continue
            z = (sdf["value"] - sdf["value"].mean()) / sdf["value"].std()
            fig_norm.add_trace(go.Scatter(
                x=sdf["observed_at"], y=z, mode="lines", name=sid,
                line=dict(color=LINE_PALETTE[i % len(LINE_PALETTE)], width=1.3),
                hovertemplate="%{x|%Y-%m-%d}<br>z = %{y:+.2f}"
                              f"<extra>{sid}</extra>",
            ))
        fig_norm.update_layout(
            yaxis=dict(title="z-score (per-series, full-history)",
                       gridcolor=GRID, zerolinecolor=GRID),
            xaxis=dict(title="observed_at",
                       gridcolor=GRID, zerolinecolor=GRID),
        )
        st.plotly_chart(_style(fig_norm, height=360), width="stretch")
        st.caption(
            "All 11 FRED series normalized to full-history z-score so the "
            "cross-series regime is visible on one axis. Levels in the "
            "table above."
        )

    # =====================================================================
    # SECTION 3 · Google Trends (distress-query panel)
    # =====================================================================
    st.markdown('<div class="panel-title">Google Trends — distress queries</div>',
                unsafe_allow_html=True)

    gt_df = _trends_all()
    if gt_df.empty:
        st.caption("google_trends is empty. Run `python -m data.ingest.trends`.")
    else:
        # Table of per-keyword stats.
        kw_rows = []
        for kw in sorted(gt_df["keyword"].unique()):
            sdf = gt_df[gt_df["keyword"] == kw]
            kw_rows.append({
                "keyword": kw,
                "n": len(sdf),
                "from": sdf["observed_at"].min().date().isoformat(),
                "to":   sdf["observed_at"].max().date().isoformat(),
                "mean_interest": float(sdf["interest"].mean()),
                "max_interest":  float(sdf["interest"].max()),
            })
        st.dataframe(pd.DataFrame(kw_rows), width="stretch", hide_index=True)

        fig_gt = go.Figure()
        for i, kw in enumerate(sorted(gt_df["keyword"].unique())):
            sdf = gt_df[gt_df["keyword"] == kw].sort_values("observed_at")
            fig_gt.add_trace(go.Scatter(
                x=sdf["observed_at"], y=sdf["interest"], mode="lines",
                name=kw,
                line=dict(color=LINE_PALETTE[i % len(LINE_PALETTE)], width=1.3),
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:.0f}"
                              f"<extra>{kw}</extra>",
            ))
        fig_gt.update_layout(
            yaxis=dict(title="Google Trends interest (0–100)",
                       gridcolor=GRID, zerolinecolor=GRID),
            xaxis=dict(title="observed_at",
                       gridcolor=GRID, zerolinecolor=GRID),
        )
        st.plotly_chart(_style(fig_gt, height=360), width="stretch")
        st.caption(
            "All populated BNPL distress/product keywords. Note: the `exit` "
            "bucket queries referenced in `signals/bsi.py` (e.g. 'affirm "
            "collections') are NOT present in this feed, which is why "
            "bsi_daily.c_trends is NULL — a known bug tracked under `bsi.py`."
        )

    # =====================================================================
    # SECTION 4 · BSI and components
    # =====================================================================
    st.markdown('<div class="panel-title">BSI — composite and components</div>',
                unsafe_allow_html=True)

    bsi_df_full = _bsi_frame()
    if bsi_df_full.empty:
        st.caption("bsi_daily is empty. Run `python -m signals.bsi`.")
    else:
        # Coverage bar: which components have non-null values at all?
        comps = ["c_cfpb", "c_reddit", "c_trends", "c_vitality",
                 "c_move", "c_appstore"]
        present_cols = [c for c in comps if c in bsi_df_full.columns]
        cov = {
            c: int(bsi_df_full[c].notna().sum()) for c in present_cols
        }
        total = len(bsi_df_full)
        cov_df = pd.DataFrame({
            "component": list(cov.keys()),
            "populated_days": list(cov.values()),
            "coverage_pct": [v / total * 100 for v in cov.values()],
        })
        st.dataframe(cov_df, width="stretch", hide_index=True)

        fig_bsi = go.Figure()
        fig_bsi.add_trace(go.Scatter(
            x=bsi_df_full["observed_at"], y=bsi_df_full["z_bsi"],
            mode="lines", name="z_bsi",
            # Primary composite — sky-blue, the calm/pass accent.
            line=dict(color=TOK_C["accent"], width=2.2, shape="spline"),
            hovertemplate="%{x|%Y-%m-%d}<br>z_bsi = %{y:+.2f}<extra></extra>",
        ))
        # Overlay each component that has ANY non-null values.
        # Palette is token-sourced: critical for complaints/vitality stress,
        # warn for attention signals, violet for demand, secondary-text for
        # the rest. Colors mirror the Layer-1 gate ladder semantics so a
        # reviewer can pattern-match across layers.
        comp_palette = {
            "c_cfpb":     TOK_C["critical"],        # complaints spike → red
            "c_reddit":   TOK_C["warn"],            # chatter → amber
            "c_trends":   TOK_C["violet"],          # search demand
            "c_vitality": TOK_C["textSecondary"],   # macro proxy
            "c_move":     TOK_C["warn"],            # vol — amber like gate 2
            "c_appstore": TOK_C["textMuted"],       # review cadence
        }
        for c in present_cols:
            if cov[c] == 0:
                continue
            fig_bsi.add_trace(go.Scatter(
                x=bsi_df_full["observed_at"], y=bsi_df_full[c],
                mode="lines", name=c, opacity=0.55,
                line=dict(
                    color=comp_palette.get(c, TOK_C["textSecondary"]),
                    width=1.1, shape="spline",
                ),
                hovertemplate=f"%{{x|%Y-%m-%d}}<br>{c} = %{{y:+.2f}}<extra></extra>",
            ))
        fig_bsi.add_hline(
            y=1.5,
            line=dict(color=TOK_C["warn"], dash="dash"),
            annotation_text="gate threshold z = 1.5",
            annotation_position="top left",
        )
        fig_bsi.update_layout(
            yaxis=dict(title="z-score", gridcolor=GRID, zerolinecolor=GRID),
            xaxis=dict(title="observed_at", gridcolor=GRID, zerolinecolor=GRID),
        )
        st.plotly_chart(_style(fig_bsi, height=380), width="stretch")
        populated = [c for c, n in cov.items() if n > 0]
        missing = [c for c, n in cov.items() if n == 0]
        st.caption(
            "z_bsi is the composite the 3-gate rule reads (BSI × MOVE × CCD II, "
            "with a |z|≥10σ super-threshold bypass; see paper §7). Components with "
            "at least one non-null day: " +
            ", ".join(f"`{c}` ({cov[c]}/{total})" for c in populated) +
            (". Components currently zeroed (never populated): "
             + ", ".join(f"`{c}`" for c in missing)
             if missing else "") +
            ". Backfilling the zeroed components is the #1 data priority — "
            "see §8 of the paper."
        )

    # =====================================================================
    # SECTION 5 · Options chain (single snapshot, IV skew)
    # =====================================================================
    st.markdown('<div class="panel-title">Options chain — IV skew snapshot</div>',
                unsafe_allow_html=True)

    opt_df = _options_all()
    if opt_df.empty:
        st.caption("options_chain has no IV rows yet.")
    else:
        # Pivot: for each ticker, plot IV vs. strike for the nearest-dated
        # expiry only (cleanest single-curve visual). Calls + puts both.
        tickers = sorted(opt_df["ticker"].unique())
        fig_opt = go.Figure()
        for i, tk in enumerate(tickers):
            sub = opt_df[opt_df["ticker"] == tk].copy()
            # nearest expiry for that ticker
            nearest = sub["expiry"].min()
            sub = sub[sub["expiry"] == nearest]
            # moneyness = strike / underlying
            sub["moneyness"] = (
                sub["strike"] / sub["underlying_price"]
            ).astype(float)
            sub = sub.sort_values("moneyness")
            fig_opt.add_trace(go.Scatter(
                x=sub["moneyness"], y=sub["iv"], mode="markers+lines",
                name=f"{tk} @ {nearest}",
                marker=dict(size=6),
                line=dict(color=LINE_PALETTE[i % len(LINE_PALETTE)], width=1.3),
                hovertemplate="moneyness=%{x:.2f}<br>IV=%{y:.3f}"
                              f"<extra>{tk}</extra>",
            ))
        fig_opt.update_layout(
            yaxis=dict(title="Implied vol", tickformat=".0%",
                       gridcolor=GRID, zerolinecolor=GRID),
            xaxis=dict(title="Moneyness (strike / underlying)",
                       gridcolor=GRID, zerolinecolor=GRID),
        )
        st.plotly_chart(_style(fig_opt, height=340), width="stretch")

        snap_stats = (
            opt_df.groupby("ticker")
                  .agg(n=("iv", "size"), mean_iv=("iv", "mean"),
                       min_iv=("iv", "min"), max_iv=("iv", "max"),
                       expiries=("expiry", "nunique"))
                  .reset_index()
        )
        st.dataframe(snap_stats, width="stretch", hide_index=True)
        st.caption(
            "Single-snapshot option chain pulled from yfinance. For the SCP "
            "Heston calibration §7.2 needs multi-day history — backfill is "
            "queued but not yet wired into the nightly job."
        )

    # =====================================================================
    # SECTION 6 · Short interest
    # =====================================================================
    st.markdown('<div class="panel-title">Short interest snapshot</div>',
                unsafe_allow_html=True)

    si_df = _short_int_all()
    if si_df.empty:
        st.caption("short_interest is empty.")
    else:
        st.dataframe(si_df, width="stretch", hide_index=True)
        # If more than one observation per ticker, show a trend line.
        if si_df.groupby("ticker").size().max() > 1:
            fig_si = go.Figure()
            for i, tk in enumerate(sorted(si_df["ticker"].unique())):
                sub = si_df[si_df["ticker"] == tk].sort_values("observed_at")
                fig_si.add_trace(go.Scatter(
                    x=sub["observed_at"], y=sub["utilization"],
                    mode="lines+markers", name=tk,
                    line=dict(color=LINE_PALETTE[i % len(LINE_PALETTE)],
                              width=1.5),
                ))
            fig_si.update_layout(
                yaxis=dict(title="Utilization (0–1)",
                           gridcolor=GRID, zerolinecolor=GRID),
                xaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
            )
            st.plotly_chart(_style(fig_si, height=280), width="stretch")

    # =====================================================================
    # SECTION 7 · Regulatory catalysts
    # =====================================================================
    st.markdown('<div class="panel-title">Regulatory catalyst timeline</div>',
                unsafe_allow_html=True)

    cat_df = _catalysts_all()
    if cat_df.empty:
        st.caption("regulatory_catalysts is empty.")
    else:
        st.dataframe(cat_df, width="stretch", hide_index=True)
        # Gantt-like scatter of deadlines.
        cat_plot = cat_df.copy()
        cat_plot["deadline_date"] = pd.to_datetime(cat_plot["deadline_date"])
        # Materiality → token palette: HIGH=critical, MEDIUM=warn, LOW=accent.
        # Same semantic as the compliance-engine gate states, so a reviewer
        # reads the gantt through the same red/amber/sky lens.
        mat_color = {
            "HIGH":   TOK_C["critical"],
            "MEDIUM": TOK_C["warn"],
            "LOW":    TOK_C["accent"],
        }
        fig_cat = go.Figure()
        for i, row in cat_plot.iterrows():
            fig_cat.add_trace(go.Scatter(
                x=[row["deadline_date"]], y=[row["jurisdiction"]],
                mode="markers+text",
                marker=dict(
                    size=16,
                    color=mat_color.get(
                        str(row.get("materiality", "")).upper(),
                        TOK_C["textSecondary"],
                    ),
                ),
                text=[row["title"][:30]], textposition="middle right",
                showlegend=False,
                hovertemplate=f"<b>{row['title']}</b><br>"
                              f"{row['jurisdiction']} — {row['category']}"
                              "<extra></extra>",
            ))
        fig_cat.update_layout(
            xaxis=dict(gridcolor=GRID, zerolinecolor=GRID,
                       title="deadline"),
            yaxis=dict(gridcolor=GRID, zerolinecolor=GRID,
                       title="jurisdiction"),
        )
        st.plotly_chart(_style(fig_cat, height=260), width="stretch")

    # =====================================================================
    # SECTION 8 · Granger causality history
    # =====================================================================
    st.markdown('<div class="panel-title">Granger causality — cached runs</div>',
                unsafe_allow_html=True)

    g_df = _granger_all()
    if g_df.empty:
        st.caption("granger_results is empty. Run `python -m signals.granger`.")
    else:
        st.dataframe(g_df, width="stretch", hide_index=True)
        # Bar chart of p-value by lag (most recent run only).
        latest_ts = g_df["run_at"].max()
        latest = g_df[g_df["run_at"] == latest_ts].sort_values("lag_weeks")
        fig_g = go.Figure()
        fig_g.add_trace(go.Bar(
            x=latest["lag_weeks"], y=latest["p_value"],
            marker=dict(color=TOK_C["accent"]),
            hovertemplate="lag=%{x}w<br>p=%{y:.4f}<extra></extra>",
            name="p-value",
        ))
        fig_g.add_hline(
            y=0.05, line=dict(color=TOK_C["warn"], dash="dash"),
            annotation_text="α = 0.05",
            annotation_position="top left",
        )
        fig_g.update_layout(
            yaxis=dict(title="p-value", type="log", gridcolor=GRID,
                       zerolinecolor=GRID),
            xaxis=dict(title="lag (weeks)", gridcolor=GRID,
                       zerolinecolor=GRID),
        )
        st.plotly_chart(_style(fig_g, height=260), width="stretch")
        st.caption(
            f"Latest run at {str(latest_ts)[:19]}. Target = HYG negative "
            f"log-return (proxy fallback; abs_tranche_metrics empty)."
        )

    # =====================================================================
    # SECTION 9 · Empty tables · remediation hints
    # =====================================================================
    st.markdown('<div class="panel-title">Empty tables — ingest required</div>',
                unsafe_allow_html=True)

    empty_hints = {
        "cfpb_complaints":      "python -m data.ingest.cfpb",
        "reddit_posts":         "python -m data.ingest.reddit_praw",
        "abs_tranche_metrics":  "python -m data.ingest.sec_edgar",
        "firm_vitality":        "python -m data.ingest.linkedin_scraper",
        "scp_daily":            "python -m quant.heston_scp",
        "pod_decisions":        "python -m agents.tick",
        "short_interest":       "python -m data.ingest.short_interest  (partial — 8 rows)",
        "squeeze_defense":      "python -m quant.squeeze_defense",
        "sec_filings_index":    "python -m data.ingest.sec_edgar",
        "jt_lambda":            "python -m quant.jarrow_turnbull",
        "portfolio_weights":    "python -m portfolio.mean_cvar",
        "portfolio_hedges":     "python -m portfolio.mean_cvar",
    }
    empty_rows = []
    for _, r in inv.iterrows():
        if r["status"] == "EMPTY":
            empty_rows.append({
                "table": r["table"],
                "ingest_command": empty_hints.get(
                    r["table"], "(no default ingest script yet)"
                ),
            })
    if not empty_rows:
        st.caption("All warehouse tables are populated.")
    else:
        st.dataframe(pd.DataFrame(empty_rows), width="stretch",
                     hide_index=True)
        st.caption(
            "Each empty table is a known, accepted gap documented in §11 "
            "(Risks and Limitations) of the paper. The four that matter "
            "most for the thesis are `cfpb_complaints`, `reddit_posts`, "
            "`abs_tranche_metrics`, and `firm_vitality` — they populate "
            "four of the six BSI sub-components."
        )
