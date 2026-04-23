"""
BNPL Pod dashboard — v2.0.1 viewing surface.

Single-file Streamlit app that opens the pod against the live warehouse and
renders five tabs:

    Overview      — hero cards + BSI trajectory + compliance decision anchor
    BSI Internals — per-pillar panel, coverage heatmap, distribution,
                    gate-1-fire history
    Compliance    — 4 gate cards with live readings, what-if slider,
                    regulatory catalyst calendar
    Falsification — placebo panel + Granger results + residualisation
                    status + Gate-1 carry-over disclosure
    Warehouse     — ingestion summary + CFPB firm/product breakdown +
                    ABS tranche metrics + FRED series inventory

Design language (locked):
    * Slate-900 background, slate-800 cards, sky-blue accent.
    * No drop-shadows. No neon. Inter + JetBrains Mono via Google Fonts.
    * Numbers in JetBrains Mono for column alignment.

Run:
    make dashboard
    # or:
    streamlit run dashboard/streamlit_app.py
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

# --- Project-root path shim --------------------------------------------------
# When launched via `streamlit run dashboard/streamlit_app.py`, the script's
# containing directory (dashboard/) is on sys.path but the project root is
# not, so `from signals import bsi` fails with ModuleNotFoundError unless
# the project is editable-installed (`uv pip install -e .`). Prepend the
# project root unconditionally so the dashboard works from a fresh clone.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
# -----------------------------------------------------------------------------

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.settings import settings

# ---------------------------------------------------------------------------
# Page config + design tokens
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="BNPL Pod — v2.0.1",
    layout="wide",
    initial_sidebar_state="collapsed",
)

_BG = "#0F172A"
_CARD = "#1E293B"
_BORDER = "#334155"
_ACCENT = "#38BDF8"
_WARN = "#FBBF24"
_CRITICAL = "#EF4444"
_TEXT_P = "#F8FAFC"
_TEXT_S = "#94A3B8"
_MUTED = "#475569"

_INSTITUTIONAL_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

html, body, [class*="css"] {{
    background-color: {_BG} !important;
    color: {_TEXT_P} !important;
    font-family: 'Inter', system-ui, sans-serif !important;
}}
.stApp {{ background-color: {_BG}; }}
h1, h2, h3, h4 {{ color: {_TEXT_P}; font-family: 'Inter', sans-serif; font-weight: 600; }}
.mono, code, .stCode, [data-testid="stMetricValue"] {{
    font-family: 'JetBrains Mono', monospace !important;
}}
.pod-card {{
    background: {_CARD};
    border: 1px solid {_BORDER};
    border-radius: 0.375rem;
    padding: 1rem 1.25rem;
    margin-bottom: 1rem;
}}
.pod-card-title {{
    color: {_TEXT_S};
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    margin-bottom: 0.5rem;
}}
.pod-hero {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 2.25rem;
    font-weight: 600;
    color: {_TEXT_P};
    line-height: 1;
}}
.pod-hero-sub {{ color: {_TEXT_S}; font-size: 0.875rem; margin-top: 0.25rem; }}
.mini-stat {{
    display: flex; flex-direction: column;
    padding: 0.5rem 0.75rem;
    background: {_CARD}; border: 1px solid {_BORDER};
    border-radius: 0.375rem;
}}
.mini-stat-label {{
    color: {_TEXT_S}; font-size: 0.65rem; text-transform: uppercase;
    letter-spacing: 0.05em; font-weight: 600;
}}
.mini-stat-value {{
    color: {_TEXT_P}; font-family: 'JetBrains Mono', monospace;
    font-size: 1.1rem; font-weight: 600; margin-top: 0.15rem;
}}
.mini-stat-sub {{
    color: {_TEXT_S}; font-size: 0.7rem; margin-top: 0.15rem;
}}
.chip {{
    display: inline-block;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    padding: 0.125rem 0.5rem;
    border-radius: 0.25rem;
    margin-right: 0.5rem;
}}
.chip-pass {{ background: {_ACCENT}; color: {_BG}; }}
.chip-fail {{ background: {_CRITICAL}; color: {_TEXT_P}; }}
.chip-warn {{ background: {_WARN}; color: {_BG}; }}
.chip-neutral {{ background: {_BORDER}; color: {_TEXT_P}; }}
.chip-muted {{ background: {_MUTED}; color: {_TEXT_S}; }}
.table-inst {{ width: 100%; border-collapse: collapse; font-size: 0.875rem; }}
.table-inst th {{
    text-align: left; color: {_TEXT_S}; font-weight: 500;
    padding: 0.5rem 0.75rem; border-bottom: 1px solid {_BORDER};
    font-size: 0.7rem; letter-spacing: 0.04em; text-transform: uppercase;
}}
.table-inst td {{ padding: 0.5rem 0.75rem; border-bottom: 1px solid {_BORDER}; }}
.table-inst td.num {{ font-family: 'JetBrains Mono', monospace; text-align: right; }}
.footer-text {{ color: {_TEXT_S}; font-size: 0.75rem; margin-top: 2rem; }}
[data-testid="stHeader"] {{ background: {_BG}; }}
[data-testid="stSidebar"] {{ background: {_CARD}; }}
[data-testid="stTabs"] [data-baseweb="tab-list"] {{
    gap: 4px; background: {_CARD}; padding: 4px; border-radius: 0.375rem;
    border: 1px solid {_BORDER};
}}
[data-testid="stTabs"] [data-baseweb="tab"] {{
    background: transparent; color: {_TEXT_S};
    border-radius: 0.25rem; padding: 0.4rem 1rem;
    font-weight: 500; font-size: 0.875rem;
}}
[data-testid="stTabs"] [aria-selected="true"] {{
    background: {_ACCENT} !important; color: {_BG} !important;
}}
.stAlert {{ background: {_CARD}; border: 1px solid {_BORDER}; }}
.stMarkdown p {{ color: {_TEXT_P}; }}
.heat-cell {{
    display: inline-block; width: 100%;
    padding: 0.2rem 0.35rem; font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem; text-align: center;
}}
</style>
"""

st.markdown(_INSTITUTIONAL_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Plotly institutional theme — every chart on the pod goes through this.
# ---------------------------------------------------------------------------

def _style_fig(
    fig: go.Figure,
    *,
    height: int = 260,
    margin: dict | None = None,
    showlegend: bool = True,
    ytitle: str | None = None,
    xtitle: str | None = None,
    yaxis_type: str | None = None,
) -> go.Figure:
    """Apply the slate-900/sky-blue institutional theme to a plotly figure."""
    fig.update_layout(
        paper_bgcolor=_BG,
        plot_bgcolor=_CARD,
        font=dict(family="Inter, system-ui, sans-serif", color=_TEXT_P, size=12),
        height=height,
        margin=margin or dict(l=40, r=20, t=10, b=36),
        showlegend=showlegend,
        legend=dict(
            bgcolor=_CARD, bordercolor=_BORDER, borderwidth=1,
            font=dict(color=_TEXT_P, size=11),
        ),
        hoverlabel=dict(
            bgcolor=_CARD, bordercolor=_BORDER,
            font=dict(family="JetBrains Mono, monospace",
                      color=_TEXT_P, size=11),
        ),
    )
    fig.update_xaxes(
        showgrid=True, gridcolor=_BORDER, zeroline=False,
        linecolor=_BORDER, tickcolor=_BORDER,
        tickfont=dict(color=_TEXT_S, family="JetBrains Mono, monospace", size=10),
        title=xtitle, title_font=dict(color=_TEXT_S, size=11),
    )
    y_kwargs = dict(
        showgrid=True, gridcolor=_BORDER,
        zeroline=True, zerolinecolor=_BORDER,
        linecolor=_BORDER, tickcolor=_BORDER,
        tickfont=dict(color=_TEXT_S, family="JetBrains Mono, monospace", size=10),
        title=ytitle, title_font=dict(color=_TEXT_S, size=11),
    )
    if yaxis_type is not None:
        y_kwargs["type"] = yaxis_type
    fig.update_yaxes(**y_kwargs)
    return fig


# ---------------------------------------------------------------------------
# Data loaders — cached so the dashboard is snappy on rerun
# ---------------------------------------------------------------------------

def _conn(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(settings.duckdb_path), read_only=read_only)


@st.cache_data(ttl=300)
def load_bsi_panel() -> pd.DataFrame:
    from signals import bsi

    out = bsi.compute_bsi_from_warehouse()
    return out


@st.cache_data(ttl=300)
def load_spec_summary() -> dict:
    from signals import bsi

    spec = bsi.load_spec()
    return {
        "halflife_days": spec.ewma_halflife_days,
        "lambda": spec.lam,
        "coverage_window_days": spec.coverage_window_days,
        "weights": dict(spec.weights),
        "sigma_floor": dict(spec.sigma_floor),
        "coverage_min": dict(spec.coverage_min),
    }


@st.cache_data(ttl=300)
def load_placebos() -> pd.DataFrame:
    from signals import placebos

    results = placebos.run_all()
    return pd.DataFrame([{
        "placebo_id": r.placebo_id,
        "event_count": r.raw_count_on_event,
        "baseline": r.raw_count_baseline,
        "ratio": r.ratio_vs_baseline,
        "z_v1": r.sensor_reading_z,
        "interpretation": r.interpretation,
    } for r in results])


@st.cache_data(ttl=300)
def load_warehouse_inventory() -> pd.DataFrame:
    tables = [
        "cfpb_complaints", "app_store_reviews", "reddit_posts",
        "google_trends", "firm_vitality", "fred_series",
        "granger_results", "jt_lambda", "scp_daily",
        "short_interest", "options_chain", "regulatory_catalysts",
        "abs_tranche_metrics", "pod_decisions", "sec_filings_index",
        "portfolio_hedges", "portfolio_weights", "squeeze_defense",
        "bsi_daily",
    ]
    time_col_candidates = [
        "observed_at", "received_at", "filed_at", "created_at",
        "issued_at", "period_end", "deadline_date", "as_of",
    ]
    rows = []
    with _conn() as c:
        for t in tables:
            try:
                n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                cols = [r[0] for r in c.execute(f"DESCRIBE {t}").fetchall()]
                tcol = next((x for x in time_col_candidates if x in cols), None)
                if tcol and n > 0:
                    mn, mx = c.execute(
                        f"SELECT MIN({tcol}), MAX({tcol}) FROM {t}"
                    ).fetchone()
                else:
                    mn, mx = None, None
                rows.append({
                    "table": t, "rows": int(n),
                    "time_col": tcol, "earliest": mn, "latest": mx,
                    "status": "live" if n > 0 else "empty",
                })
            except Exception as exc:  # noqa: BLE001
                rows.append({
                    "table": t, "rows": 0, "time_col": None,
                    "earliest": None, "latest": None,
                    "status": f"err: {exc}",
                })
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_cfpb_firm_breakdown(top_n: int = 12) -> pd.DataFrame:
    sql = """
        SELECT company, COUNT(*) AS n_complaints,
               MIN(received_at) AS earliest,
               MAX(received_at) AS latest
        FROM cfpb_complaints
        GROUP BY company
        ORDER BY n_complaints DESC
        LIMIT ?
    """
    with _conn() as c:
        return c.execute(sql, [top_n]).fetch_df()


@st.cache_data(ttl=300)
def load_cfpb_product_breakdown() -> pd.DataFrame:
    sql = """
        SELECT product, COUNT(*) AS n_complaints
        FROM cfpb_complaints
        GROUP BY product
        ORDER BY n_complaints DESC
    """
    with _conn() as c:
        return c.execute(sql).fetch_df()


@st.cache_data(ttl=300)
def load_cfpb_daily_count() -> pd.DataFrame:
    sql = """
        SELECT CAST(received_at AS DATE) AS day,
               COUNT(*) AS n_complaints
        FROM cfpb_complaints
        WHERE (company ILIKE '%Affirm%' OR company ILIKE '%Klarna%'
            OR company ILIKE '%Sezzle%' OR company ILIKE '%Afterpay%'
            OR company ILIKE '%Block%' OR company ILIKE '%Paypal%')
        GROUP BY CAST(received_at AS DATE)
        ORDER BY day
    """
    with _conn() as c:
        df = c.execute(sql).fetch_df()
    df["day"] = pd.to_datetime(df["day"])
    return df.set_index("day")


@st.cache_data(ttl=300)
def load_granger_results() -> pd.DataFrame:
    with _conn() as c:
        return c.execute(
            "SELECT * FROM granger_results ORDER BY target_label, lag_weeks"
        ).fetch_df()


@st.cache_data(ttl=300)
def load_regulatory_catalysts() -> pd.DataFrame:
    with _conn() as c:
        df = c.execute(
            "SELECT jurisdiction, deadline_date, title, materiality, category, notes "
            "FROM regulatory_catalysts ORDER BY deadline_date"
        ).fetch_df()
    df["deadline_date"] = pd.to_datetime(df["deadline_date"]).dt.date
    today = date.today()
    df["days_to_deadline"] = df["deadline_date"].apply(
        lambda d: (d - today).days if pd.notna(d) else None
    )
    return df


@st.cache_data(ttl=300)
def load_abs_tranche_summary() -> pd.DataFrame:
    sql = """
        SELECT trust_name,
               COUNT(*) AS n_reports,
               MIN(period_end) AS earliest,
               MAX(period_end) AS latest,
               AVG(roll_rate_60p) AS avg_roll_60p,
               MAX(roll_rate_60p) AS max_roll_60p,
               AVG(excess_spread) AS avg_excess_spread,
               AVG(cnl) AS avg_cnl
        FROM abs_tranche_metrics
        GROUP BY trust_name
        ORDER BY n_reports DESC
        LIMIT 20
    """
    with _conn() as c:
        return c.execute(sql).fetch_df()


@st.cache_data(ttl=300)
def load_fred_inventory() -> pd.DataFrame:
    sql = """
        SELECT series_id, COUNT(*) AS n_obs,
               MIN(observed_at) AS earliest,
               MAX(observed_at) AS latest,
               LAST(value ORDER BY observed_at) AS last_value
        FROM fred_series
        GROUP BY series_id
        ORDER BY n_obs DESC
    """
    with _conn() as c:
        return c.execute(sql).fetch_df()


@st.cache_data(ttl=300)
def load_move_series() -> pd.Series:
    sql = """
        SELECT observed_at, value
        FROM fred_series WHERE series_id = 'MOVE'
        ORDER BY observed_at
    """
    with _conn() as c:
        df = c.execute(sql).fetch_df()
    df["observed_at"] = pd.to_datetime(df["observed_at"])
    return df.set_index("observed_at")["value"]


def _run_compliance(event_date: date, bsi_z: float,
                    move_ma30: float, scp_afrm: float,
                    days_to_ccd2: int) -> dict:
    from agents.compliance_engine import ComplianceEngine, GateInputs

    eng = ComplianceEngine()
    from datetime import timedelta
    inp = GateInputs(
        as_of=datetime.combine(event_date, datetime.min.time()),
        bsi_z=bsi_z,
        scp_by_ticker={"AFRM": scp_afrm, "SQ": max(0.0, scp_afrm - 0.9)},
        move_ma30=move_ma30,
        ccd_ii_deadline=event_date + timedelta(days=days_to_ccd2),
        squeeze_utilization={},
        squeeze_days_to_cover={},
        squeeze_skew_pctile={},
        expression="trs_junior_abs",
    )
    dec = eng.evaluate(inp)
    return {
        "approved": dec.approved,
        "gates": dec.gate_results,
        "squeeze_veto": dec.squeeze_veto,
        "reasons": dec.reasons,
        "thresholds_version": dec.thresholds_version,
    }


def _heat_cell_color(gamma: float, z: float | None) -> str:
    """Color a coverage-gate heatmap cell."""
    if gamma < 0.5:
        return _MUTED
    if z is None or pd.isna(z):
        return _BORDER
    if z >= 1.5:
        return _CRITICAL
    if z >= 0.5:
        return _WARN
    if z <= -1.5:
        return _ACCENT
    return _BORDER


# ---------------------------------------------------------------------------
# Global load
# ---------------------------------------------------------------------------

try:
    panel = load_bsi_panel()
    spec_summary = load_spec_summary()
    inventory = load_warehouse_inventory()
except Exception as exc:  # noqa: BLE001
    st.error(f"Failed to load BSI panel from warehouse: {exc}")
    st.stop()

panel_nn = panel.dropna(subset=["z_bsi"])
latest = panel_nn.iloc[-1]
latest_date = panel_nn.index[-1].date()

event_ts = pd.Timestamp("2025-01-17")
event_row = panel.loc[event_ts] if event_ts in panel.index else None

# ---------------------------------------------------------------------------
# Top banner + summary strip
# ---------------------------------------------------------------------------

n_rows = len(panel_nn)
first_date = panel_nn.index[0].date()
pillar_active_today = int(sum(
    float(latest.get(f"gamma_{p}", 0)) > 0.5 for p in spec_summary["weights"]
))
fires_30d = int((panel_nn["z_bsi"].tail(30) >= 1.5).sum())
fires_90d = int((panel_nn["z_bsi"].tail(90) >= 1.5).sum())
fires_365d = int((panel_nn["z_bsi"].tail(365) >= 1.5).sum())
fired_idx = panel_nn.index[panel_nn["z_bsi"] >= 1.5]
days_since_fire = (panel_nn.index[-1] - fired_idx[-1]).days if len(fired_idx) else None
bsi_pctile = float((panel_nn["z_bsi"] <= latest["z_bsi"]).mean() * 100)

st.markdown(
    f"""
    <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:0.75rem;">
      <div>
        <div style="color:{_TEXT_S};font-size:0.75rem;letter-spacing:0.1em;text-transform:uppercase;">
          BNPL Pod
        </div>
        <h1 style="margin:0;font-weight:600;">v2.0.1 &mdash; CFPB&ndash;MOVE Composite, EWMA &sigma;</h1>
        <div style="color:{_TEXT_S};font-size:0.875rem;margin-top:0.25rem;">
          Equation (1) of paper &sect;6 &middot; coverage-gated weighted z-score &middot;
          halflife <span class="mono">{spec_summary['halflife_days']}d</span> &middot;
          &lambda;&nbsp;<span class="mono">{spec_summary['lambda']:.6f}</span>
        </div>
      </div>
      <div class="mono" style="color:{_TEXT_S};font-size:0.875rem;">
        as of {datetime.now().strftime('%Y-%m-%d %H:%M')}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

summary_cols = st.columns(6)
_STATS = [
    ("Panel days", f"{n_rows:,}", f"{first_date} &rarr; {latest_date}"),
    ("Pillars live today", f"{pillar_active_today}/7",
     f"{int(latest.get('gamma_cfpb',0))}|{int(latest.get('gamma_move',0))}|{int(latest.get('gamma_appstore',0))} cfpb|move|app"),
    ("Gate-1 fires 30d", f"{fires_30d}", f"90d: {fires_90d} &middot; 365d: {fires_365d}"),
    ("Days since last fire",
     f"{days_since_fire if days_since_fire is not None else '—'}",
     f"last: {fired_idx[-1].date() if len(fired_idx) else '—'}"),
    ("Current z percentile", f"{bsi_pctile:.1f}&percnt;",
     f"vs {len(panel_nn):,}-day history"),
    ("Warehouse tables",
     f"{int((inventory['rows'] > 0).sum())}/{len(inventory)}",
     f"total rows: {int(inventory['rows'].sum()):,}"),
]
for col, (lbl, val, sub) in zip(summary_cols, _STATS):
    with col:
        st.markdown(
            f"""<div class="mini-stat">
                <div class="mini-stat-label">{lbl}</div>
                <div class="mini-stat-value">{val}</div>
                <div class="mini-stat-sub">{sub}</div>
            </div>""",
            unsafe_allow_html=True,
        )

st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_overview, tab_internals, tab_compliance, tab_falsify, tab_warehouse = st.tabs(
    ["Overview", "BSI Internals", "Compliance", "Falsification", "Warehouse"]
)

# =========================================================================
# TAB 1 — OVERVIEW
# =========================================================================

with tab_overview:
    col_hero, col_event, col_spec = st.columns([1, 1, 1])
    with col_hero:
        st.markdown(
            f"""
            <div class="pod-card">
              <div class="pod-card-title">Latest BSI</div>
              <div class="pod-hero">{latest['z_bsi']:+.2f}&sigma;</div>
              <div class="pod-hero-sub">
                BSI level <span class="mono">{latest['bsi']:+.3f}</span> &middot;
                as of <span class="mono">{latest_date}</span>
              </div>
              <div class="pod-hero-sub" style="margin-top:0.5rem;">
                {"<span class='chip chip-fail'>GATE 1 FIRED</span>" if latest['z_bsi'] >= 1.5 else "<span class='chip chip-pass'>GATE 1 QUIET</span>"}
                <span class="chip chip-muted">
                  {bsi_pctile:.0f}th pct
                </span>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col_event:
        if event_row is not None and pd.notna(event_row["z_bsi"]):
            chip_cls = "chip-fail" if event_row["z_bsi"] >= 1.5 else "chip-pass"
            chip_txt = "GATE 1 FIRED" if event_row["z_bsi"] >= 1.5 else "GATE 1 QUIET"
            event_pctile = float((panel_nn["z_bsi"] <= event_row["z_bsi"]).mean() * 100)
            st.markdown(
                f"""
                <div class="pod-card">
                  <div class="pod-card-title">17 Jan 2025 anchor</div>
                  <div class="pod-hero">{event_row['z_bsi']:+.2f}&sigma;</div>
                  <div class="pod-hero-sub">
                    Headline pulse. BSI level <span class="mono">{event_row['bsi']:+.3f}</span>.
                  </div>
                  <div class="pod-hero-sub" style="margin-top:0.5rem;">
                    <span class="chip {chip_cls}">{chip_txt}</span>
                    <span class="chip chip-muted">{event_pctile:.1f}th pct</span>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"""<div class="pod-card">
                  <div class="pod-card-title">17 Jan 2025 anchor</div>
                  <div class="pod-hero-sub">Not in warehouse window.</div>
                </div>""",
                unsafe_allow_html=True,
            )

    with col_spec:
        weights_html = "<br>".join(
            f"<span class='mono'>{k:<9}</span> {v:.3f}"
            + (" <span class='chip chip-pass' style='font-size:0.6rem;padding:0 0.35rem;'>live</span>"
               if int(latest.get(f"gamma_{k}", 0) or 0) else
               " <span class='chip chip-muted' style='font-size:0.6rem;padding:0 0.35rem;'>gated</span>")
            for k, v in spec_summary["weights"].items()
        )
        st.markdown(
            f"""
            <div class="pod-card">
              <div class="pod-card-title">Scorer spec</div>
              <div class="pod-hero-sub" style="line-height:1.5;">
                halflife <span class="mono">{spec_summary['halflife_days']}d</span> &middot;
                &lambda; <span class="mono">{spec_summary['lambda']:.6f}</span><br>
                coverage window <span class="mono">{spec_summary['coverage_window_days']}d</span>
              </div>
              <div style="height:0.5rem;"></div>
              <div class="pod-card-title" style="font-size:0.65rem;">
                pillar weights &middot; gate state today
              </div>
              <div class="pod-hero-sub">{weights_html}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("#### BSI trajectory")
    tail_opt = st.radio(
        "Window",
        options=["1M", "60D", "3M", "6M", "1Y", "ALL"],
        index=2, horizontal=True, label_visibility="collapsed",
        key="overview_tail",
    )
    _TAIL_DAYS = {"1M": 22, "60D": 60, "3M": 66, "6M": 132, "1Y": 260, "ALL": len(panel_nn)}
    slice_df = panel_nn.tail(_TAIL_DAYS[tail_opt])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=slice_df.index, y=slice_df["z_bsi"],
        mode="lines", name="z_bsi",
        line=dict(color=_ACCENT, width=2, shape="spline"),
        fill="tozeroy", fillcolor="rgba(56,189,248,0.12)",
        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>z_bsi = %{y:+.2f}σ<extra></extra>",
    ))
    # Gate-1 threshold
    fig.add_hline(
        y=1.5, line=dict(color=_WARN, width=1.25, dash="dash"),
        annotation_text="Gate 1 threshold +1.5σ",
        annotation_position="top right",
        annotation_font=dict(color=_WARN, size=10, family="JetBrains Mono, monospace"),
    )
    # Mark any gate-1 fires in the slice as red markers
    fires = slice_df[slice_df["z_bsi"] >= 1.5]
    if len(fires):
        fig.add_trace(go.Scatter(
            x=fires.index, y=fires["z_bsi"],
            mode="markers", name="fires (z≥1.5)",
            marker=dict(color=_CRITICAL, size=7, symbol="circle",
                        line=dict(color=_TEXT_P, width=1)),
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>z_bsi = %{y:+.2f}σ<br>GATE 1 FIRED<extra></extra>",
        ))
    _style_fig(fig, height=260, ytitle="z_bsi (σ)", showlegend=len(fires) > 0)
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

    st.caption(
        f"Window: {slice_df.index[0].date()} → {slice_df.index[-1].date()} "
        f"({len(slice_df):,} days). "
        f"Min z {slice_df['z_bsi'].min():+.2f} / max z {slice_df['z_bsi'].max():+.2f}. "
        f"Fires in window: {len(fires):,}."
    )

    # Per-pillar contributions on 17-Jan-2025
    if event_row is not None:
        st.markdown("#### Per-pillar contributions &mdash; 17 Jan 2025")
        rows_html = []
        pillars = ["cfpb", "move", "trends", "reddit", "appstore", "vitality", "macro"]
        total_contrib = 0.0
        for p in pillars:
            gamma_col = f"gamma_{p}"
            z_col = f"z_{p}"
            gamma = event_row.get(gamma_col)
            z = event_row.get(z_col)
            w = spec_summary["weights"].get(p, 0.0)
            if gamma is None or pd.isna(gamma):
                gamma = 0.0
            if pd.isna(z):
                z_disp = "&mdash;"
                contrib_disp = "&mdash;"
                contrib_val = 0.0
            else:
                z_disp = f"{z:+.3f}"
                contrib_val = w * z * float(gamma)
                contrib_disp = f"{contrib_val:+.3f}"
                total_contrib += contrib_val
            gate_chip = (
                f"<span class='chip chip-pass'>&gamma;=1</span>"
                if float(gamma) > 0.5
                else f"<span class='chip chip-muted'>&gamma;=0</span>"
            )
            rows_html.append(
                f"<tr>"
                f"<td><span class='mono'>{p:<9}</span></td>"
                f"<td class='num'>{w:.3f}</td>"
                f"<td>{gate_chip}</td>"
                f"<td class='num'>{z_disp}</td>"
                f"<td class='num'>{contrib_disp}</td>"
                f"</tr>"
            )
        st.markdown(
            f"""
            <div class="pod-card">
              <table class="table-inst">
                <thead>
                  <tr>
                    <th>Pillar</th><th style="text-align:right;">Weight</th>
                    <th>Gate</th><th style="text-align:right;">z</th>
                    <th style="text-align:right;">w&middot;&gamma;&middot;z</th>
                  </tr>
                </thead>
                <tbody>{''.join(rows_html)}</tbody>
                <tfoot>
                  <tr>
                    <td colspan="4" style="text-align:right;color:{_TEXT_S};font-size:0.75rem;">
                      Sum of gate-normalised contributions &rarr;
                    </td>
                    <td class="num" style="font-weight:600;">
                      <span class='mono'>{event_row['bsi']:+.3f}</span>
                    </td>
                  </tr>
                </tfoot>
              </table>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Compliance decision on the anchor
        st.markdown("#### Compliance engine decision &mdash; 17 Jan 2025")
        dec = _run_compliance(
            event_date=date(2025, 1, 17),
            bsi_z=float(event_row["z_bsi"]),
            move_ma30=125.0,
            scp_afrm=3.50,
            days_to_ccd2=(date(2025, 5, 20) - date(2025, 1, 17)).days,
        )
        g_rows = []
        for g, passed in dec["gates"].items():
            cls = "chip-pass" if passed else "chip-fail"
            label = "PASS" if passed else "FAIL"
            g_rows.append(f"<span class='chip {cls}'>{g.upper()} {label}</span>")
        reasons_html = "<br>".join(
            f"<span class='mono' style='font-size:0.8rem;color:{_TEXT_S};'>&middot; {r}</span>"
            for r in dec["reasons"]
        )
        verdict_chip = (
            "<span class='chip chip-pass'>APPROVED</span>" if dec["approved"]
            else "<span class='chip chip-fail'>VETOED</span>"
        )
        st.markdown(
            f"""
            <div class="pod-card">
              <div style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:center;">
                {verdict_chip}{''.join(g_rows)}
              </div>
              <div style="height:0.75rem;"></div>
              {reasons_html}
              <div style="height:0.75rem;"></div>
              <div class="mono" style="font-size:0.75rem;color:{_TEXT_S};">
                thresholds_version = {dec['thresholds_version']}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# =========================================================================
# TAB 2 — BSI INTERNALS
# =========================================================================

with tab_internals:
    st.markdown("#### Per-pillar z-score trajectory (tail 90 days)")
    pillars = ["cfpb", "move", "trends", "reddit", "appstore", "vitality", "macro"]
    z_cols = [f"z_{p}" for p in pillars if f"z_{p}" in panel.columns]
    z_tail = panel[z_cols].tail(90).copy()
    z_tail.columns = [c.replace("z_", "") for c in z_tail.columns]
    st.line_chart(z_tail, height=260)
    live_pillars = [p for p in pillars if float(latest.get(f"gamma_{p}", 0)) > 0.5]
    gated_pillars = [p for p in pillars if p not in live_pillars]
    st.caption(
        f"Live today (γ=1): {', '.join(live_pillars) if live_pillars else 'none'}. "
        f"Gated (γ=0): {', '.join(gated_pillars) if gated_pillars else 'none'}. "
        f"A γ=0 pillar's line sits at exactly 0 because it contributes no weighted z to the BSI."
    )

    st.markdown("#### Coverage-gate heatmap (γ over last 60 days)")
    gamma_cols = [f"gamma_{p}" for p in pillars if f"gamma_{p}" in panel.columns]
    gamma_tail = panel[gamma_cols].tail(60).copy()
    gamma_tail.columns = [c.replace("gamma_", "") for c in gamma_tail.columns]
    # Render as a compact HTML grid
    heat_rows = []
    for p in gamma_tail.columns:
        cells = []
        for d in gamma_tail.index:
            g = gamma_tail.loc[d, p]
            color = _ACCENT if g >= 0.5 else _MUTED
            cells.append(f"<td style='padding:0;'>"
                         f"<div style='background:{color};height:14px;width:10px;'></div></td>")
        heat_rows.append(
            f"<tr><td style='color:{_TEXT_S};font-family:\"JetBrains Mono\",monospace;"
            f"font-size:0.75rem;padding-right:0.5rem;'>{p}</td>{''.join(cells)}</tr>"
        )
    st.markdown(
        f"""
        <div class="pod-card">
          <table style="border-collapse:collapse;">{''.join(heat_rows)}</table>
          <div style="color:{_TEXT_S};font-size:0.7rem;margin-top:0.5rem;">
            Sky-blue cell = γ=1 (pillar contributing). Muted cell = γ=0 (pillar gated out).
            Each column = one trading day.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("#### BSI z-score distribution")
    col_hist, col_stats = st.columns([2, 1])
    with col_hist:
        zvals = panel_nn["z_bsi"].values
        bins = np.linspace(np.floor(zvals.min()), np.ceil(zvals.max()), 50)
        counts, edges = np.histogram(zvals, bins=bins)
        centers = 0.5 * (edges[:-1] + edges[1:])
        hist_df = pd.DataFrame({"z": centers, "count": counts}).set_index("z")
        st.bar_chart(hist_df, height=240, color=_ACCENT)
    with col_stats:
        zs = panel_nn["z_bsi"]
        st.markdown(
            f"""
            <div class="pod-card">
              <div class="pod-card-title">Distribution stats</div>
              <div class="pod-hero-sub">
                <span class="mono">N          </span> {len(zs):,}<br>
                <span class="mono">mean       </span> {zs.mean():+.3f}<br>
                <span class="mono">median     </span> {zs.median():+.3f}<br>
                <span class="mono">std        </span> {zs.std():.3f}<br>
                <span class="mono">min        </span> {zs.min():+.3f}<br>
                <span class="mono">max        </span> {zs.max():+.3f}<br>
                <span class="mono">p05 / p95  </span> {zs.quantile(0.05):+.2f} / {zs.quantile(0.95):+.2f}<br>
                <span class="mono">p99        </span> {zs.quantile(0.99):+.2f}<br>
                <span class="mono">today      </span> {latest['z_bsi']:+.3f} ({bsi_pctile:.1f}th pct)
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("#### Gate-1 fires by calendar year")
    fire_df = panel_nn.assign(fired=(panel_nn["z_bsi"] >= 1.5).astype(int)).copy()
    fire_df["year"] = fire_df.index.year
    fires_by_year = fire_df.groupby("year")["fired"].sum().astype(int)
    days_by_year = fire_df.groupby("year")["fired"].count().astype(int)
    rate_by_year = (100.0 * fires_by_year / days_by_year).round(2)
    fire_table = pd.DataFrame({
        "days": days_by_year, "fires": fires_by_year,
        "fire_rate_%": rate_by_year,
    })
    st.bar_chart(fire_table[["fires"]], height=200, color=_WARN)
    st.dataframe(fire_table, use_container_width=True)

# =========================================================================
# TAB 3 — COMPLIANCE
# =========================================================================

with tab_compliance:
    st.markdown("#### Four-gate status (current readings)")

    # Load thresholds live
    from data.settings import load_thresholds
    th = load_thresholds()
    z_req = float(th["gates"]["bsi"]["z_threshold"])
    move_req = float(th["gates"]["move"]["ma30_threshold"])
    scp_req = float(th["gates"]["scp"]["min_scp_equity_layer"])
    ccd_max_days = int(th["gates"]["ccd_ii"]["max_days_to_deadline"])

    # Derive current readings
    move_s = None
    try:
        move_s = load_move_series()
    except Exception:  # noqa: BLE001
        pass
    if move_s is not None and len(move_s) > 0:
        move_ma30_now = float(move_s.rolling(30).mean().dropna().iloc[-1])
    else:
        move_ma30_now = float("nan")

    try:
        catalysts = load_regulatory_catalysts()
        future_cat = catalysts[catalysts["days_to_deadline"] >= 0]
        nearest = future_cat.iloc[0] if len(future_cat) else None
    except Exception:  # noqa: BLE001
        nearest = None

    gate_cards = []
    # Gate 1 — BSI
    gate_cards.append({
        "name": "Gate 1 — BSI",
        "value": f"{latest['z_bsi']:+.2f}σ",
        "thr": f"≥ {z_req:.2f}σ",
        "pass": latest['z_bsi'] >= z_req,
        "sub": f"current; percentile {bsi_pctile:.0f} of {len(panel_nn):,}-day history",
        "prov": "v1-calibrated carry-over; see paper §6",
    })
    # Gate 2 — SCP (stubbed: scp_daily empty)
    gate_cards.append({
        "name": "Gate 2 — SCP",
        "value": "n/a",
        "thr": f"≥ {scp_req:.2f} $/100",
        "pass": False,
        "sub": "scp_daily warehouse table is empty (Phase C equity apparatus)",
        "prov": "Heston apparatus demoted to Appendix B; non-gating",
    })
    # Gate 3 — MOVE
    gate_cards.append({
        "name": "Gate 3 — MOVE",
        "value": f"{move_ma30_now:.1f}" if not np.isnan(move_ma30_now) else "n/a",
        "thr": f"≥ {move_req:.1f}",
        "pass": (not np.isnan(move_ma30_now)) and move_ma30_now >= move_req,
        "sub": "ICE BofA MOVE 30-day MA from FRED",
        "prov": "load-bearing pillar #2; 100% coverage in warehouse",
    })
    # Gate 4 — CCD II
    if nearest is not None:
        dtd = int(nearest["days_to_deadline"])
        gate_cards.append({
            "name": "Gate 4 — CCD II",
            "value": f"{dtd}d",
            "thr": f"0 ≤ d ≤ {ccd_max_days}",
            "pass": 0 <= dtd <= ccd_max_days,
            "sub": f"nearest: {nearest['title']} ({nearest['deadline_date']})",
            "prov": "EU Consumer Credit Directive II calendar",
        })
    else:
        gate_cards.append({
            "name": "Gate 4 — CCD II",
            "value": "n/a",
            "thr": f"0 ≤ d ≤ {ccd_max_days}",
            "pass": False,
            "sub": "no future catalysts in warehouse",
            "prov": "EU Consumer Credit Directive II calendar",
        })

    cols = st.columns(4)
    for i, (col, g) in enumerate(zip(cols, gate_cards)):
        chip = ("<span class='chip chip-pass'>PASS</span>" if g["pass"]
                else "<span class='chip chip-fail'>FAIL</span>")
        col.markdown(
            f"""
            <div class="pod-card">
              <div class="pod-card-title">{g['name']}</div>
              <div class="pod-hero" style="font-size:1.75rem;">{g['value']}</div>
              <div class="pod-hero-sub">
                {chip}&nbsp;<span class="mono">threshold {g['thr']}</span>
              </div>
              <div class="pod-hero-sub" style="margin-top:0.5rem;font-size:0.75rem;">
                {g['sub']}
              </div>
              <div class="pod-hero-sub" style="font-size:0.7rem;color:{_TEXT_S};font-style:italic;">
                {g['prov']}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        # Attach a sparkline inside the card where data exists.
        with col:
            if g["name"].startswith("Gate 1") and len(panel_nn) > 0:
                tail = panel_nn["z_bsi"].tail(180)
                sp = go.Figure()
                sp.add_trace(go.Scatter(
                    x=tail.index, y=tail.values,
                    mode="lines", line=dict(color=_ACCENT, width=1.5, shape="spline"),
                    showlegend=False, hoverinfo="skip",
                ))
                sp.add_hline(y=z_req, line=dict(color=_WARN, width=1, dash="dash"))
                _style_fig(
                    sp, height=90, showlegend=False,
                    margin=dict(l=0, r=0, t=4, b=4),
                )
                sp.update_xaxes(visible=False); sp.update_yaxes(visible=False)
                st.plotly_chart(sp, use_container_width=True,
                                config={"displayModeBar": False})
            elif g["name"].startswith("Gate 3") and move_s is not None and len(move_s) > 0:
                ma30 = move_s.rolling(30).mean().dropna().tail(180)
                sp = go.Figure()
                sp.add_trace(go.Scatter(
                    x=ma30.index, y=ma30.values,
                    mode="lines", line=dict(color=_ACCENT, width=1.5, shape="spline"),
                    showlegend=False, hoverinfo="skip",
                ))
                sp.add_hline(y=move_req, line=dict(color=_WARN, width=1, dash="dash"))
                _style_fig(
                    sp, height=90, showlegend=False,
                    margin=dict(l=0, r=0, t=4, b=4),
                )
                sp.update_xaxes(visible=False); sp.update_yaxes(visible=False)
                st.plotly_chart(sp, use_container_width=True,
                                config={"displayModeBar": False})

    st.markdown("#### What-if — probe a hypothetical compliance decision")
    col_slide, col_out = st.columns([1, 1])
    with col_slide:
        whatif_z = st.slider("BSI z-score", min_value=-3.0, max_value=15.0,
                             value=float(latest["z_bsi"]), step=0.1)
        whatif_move = st.slider("MOVE 30d MA", min_value=50.0, max_value=200.0,
                                value=float(move_ma30_now) if not np.isnan(move_ma30_now) else 120.0,
                                step=1.0)
        whatif_scp = st.slider("SCP AFRM ($/100)", min_value=0.0, max_value=8.0,
                               value=3.5, step=0.1)
        whatif_ccd = st.slider("Days to CCD II deadline", min_value=-30, max_value=365,
                               value=int(nearest["days_to_deadline"]) if nearest is not None else 120,
                               step=1)

    with col_out:
        dec_w = _run_compliance(
            event_date=date.today(), bsi_z=whatif_z,
            move_ma30=whatif_move, scp_afrm=whatif_scp, days_to_ccd2=whatif_ccd,
        )
        verdict_chip = (
            "<span class='chip chip-pass'>APPROVED</span>" if dec_w["approved"]
            else "<span class='chip chip-fail'>VETOED</span>"
        )
        g_rows = []
        for g, passed in dec_w["gates"].items():
            cls = "chip-pass" if passed else "chip-fail"
            label = "PASS" if passed else "FAIL"
            g_rows.append(f"<span class='chip {cls}'>{g.upper()} {label}</span>")
        reasons_html = "<br>".join(
            f"<span class='mono' style='font-size:0.75rem;color:{_TEXT_S};'>&middot; {r}</span>"
            for r in dec_w["reasons"]
        )
        st.markdown(
            f"""
            <div class="pod-card">
              <div class="pod-card-title">What-if decision</div>
              <div style="display:flex;gap:0.4rem;flex-wrap:wrap;align-items:center;">
                {verdict_chip}{''.join(g_rows)}
              </div>
              <div style="height:0.75rem;"></div>
              {reasons_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("#### Regulatory catalyst calendar")
    try:
        cat_df = load_regulatory_catalysts()
        rows_html = []
        for _, r in cat_df.iterrows():
            dtd = r["days_to_deadline"]
            if dtd is None:
                chip = "<span class='chip chip-muted'>unknown</span>"
            elif dtd < 0:
                chip = f"<span class='chip chip-muted'>passed {abs(dtd)}d ago</span>"
            elif dtd <= 30:
                chip = f"<span class='chip chip-fail'>{dtd}d</span>"
            elif dtd <= ccd_max_days:
                chip = f"<span class='chip chip-warn'>{dtd}d</span>"
            else:
                chip = f"<span class='chip chip-neutral'>{dtd}d</span>"
            rows_html.append(
                f"<tr>"
                f"<td><span class='mono'>{r['deadline_date']}</span></td>"
                f"<td>{chip}</td>"
                f"<td><span class='mono'>{r['jurisdiction']}</span></td>"
                f"<td>{r['title']}</td>"
                f"<td><span class='mono'>{r['materiality']}</span></td>"
                f"<td><span class='mono'>{r['category']}</span></td>"
                f"</tr>"
            )
        st.markdown(
            f"""
            <div class="pod-card">
              <table class="table-inst">
                <thead><tr>
                  <th>Deadline</th><th>Countdown</th><th>Jurisdiction</th>
                  <th>Title</th><th>Materiality</th><th>Category</th>
                </tr></thead>
                <tbody>{''.join(rows_html)}</tbody>
              </table>
            </div>
            """,
            unsafe_allow_html=True,
        )
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Catalyst calendar unavailable: {exc}")

# =========================================================================
# TAB 4 — FALSIFICATION
# =========================================================================

with tab_falsify:
    st.markdown("#### Placebo panel &mdash; Table <code>tab:placebos-live</code> (paper &sect;7.2)")
    try:
        placebos_df = load_placebos()

        # --- Chart: placebo ratio bar (log scale) -----------------------------
        pb_sorted = placebos_df.copy()
        # Guard log10 on zero-ratio P3a (mortgage is null-by-construction).
        pb_sorted["ratio_display"] = pb_sorted["ratio"].clip(lower=1e-3)
        pb_sorted = pb_sorted.sort_values("ratio_display", ascending=True)
        bar_colors = []
        for pid, ratio in zip(pb_sorted["placebo_id"], pb_sorted["ratio"]):
            if pid == "BNPL_reference":
                bar_colors.append(_WARN)
            elif ratio >= 10:
                # P1/P2 numerator placebos — fire too, by design
                bar_colors.append(_CRITICAL)
            else:
                bar_colors.append(_ACCENT)
        fig_pb = go.Figure()
        fig_pb.add_trace(go.Bar(
            x=pb_sorted["ratio_display"],
            y=pb_sorted["placebo_id"],
            orientation="h",
            marker=dict(color=bar_colors, line=dict(color=_BORDER, width=1)),
            text=[
                (f"{r:.2f}×" if r >= 0.01 else "0.00× (null)")
                for r in pb_sorted["ratio"]
            ],
            textposition="outside",
            textfont=dict(family="JetBrains Mono, monospace",
                          color=_TEXT_P, size=11),
            hovertemplate="<b>%{y}</b><br>ratio = %{text}<extra></extra>",
            showlegend=False,
        ))
        fig_pb.add_vline(
            x=10.0, line=dict(color=_WARN, width=1, dash="dash"),
            annotation_text="10× (paper §7.2 fires-threshold)",
            annotation_position="top",
            annotation_font=dict(color=_WARN, size=10,
                                 family="JetBrains Mono, monospace"),
        )
        _style_fig(
            fig_pb, height=300, showlegend=False,
            xtitle="event-window count ÷ 180d baseline  (log scale)",
            yaxis_type=None,
        )
        fig_pb.update_xaxes(type="log")
        st.plotly_chart(fig_pb, use_container_width=True,
                        config={"displaylogo": False})
        st.caption(
            "Amber = BNPL reference (304.74×). Red = numerator placebos (P1/P2) "
            "that fire by construction — a sensor that discriminates on signal "
            "rather than absolute count will suppress these. Sky-blue = "
            "P3a-d sibling-product placebos, all ≤ 2.29×. Log x-axis; "
            "zero-ratio P3a (mortgage, null-by-construction) clipped to 1e-3."
        )

        rows_html = []
        for _, r in placebos_df.iterrows():
            pid = r["placebo_id"]
            if pid == "BNPL_reference":
                cls, label = "chip-warn", "REFERENCE"
            elif r["ratio"] >= 10:
                cls, label = "chip-fail", "FIRES"
            else:
                cls, label = "chip-pass", "NULL"
            rows_html.append(
                f"<tr>"
                f"<td><span class='mono'>{pid}</span></td>"
                f"<td class='num'>{int(r['event_count']):,}</td>"
                f"<td class='num'>{r['baseline']:.2f}</td>"
                f"<td class='num'>{r['ratio']:.2f}&times;</td>"
                f"<td class='num'>{r['z_v1']:+.2f}</td>"
                f"<td><span class='chip {cls}'>{label}</span></td>"
                f"</tr>"
            )
        st.markdown(
            f"""
            <div class="pod-card">
              <table class="table-inst">
                <thead><tr>
                  <th>Sensor</th>
                  <th style="text-align:right;">Event count</th>
                  <th style="text-align:right;">180d baseline</th>
                  <th style="text-align:right;">Ratio</th>
                  <th style="text-align:right;">v1-style z</th>
                  <th>Verdict</th>
                </tr></thead>
                <tbody>{''.join(rows_html)}</tbody>
              </table>
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.expander("Per-placebo interpretation", expanded=False):
            for _, r in placebos_df.iterrows():
                st.markdown(
                    f"<div class='pod-card' style='margin-bottom:0.5rem;'>"
                    f"<b class='mono'>{r['placebo_id']}</b> &mdash; "
                    f"<span style='color:{_TEXT_S};font-size:0.85rem;'>{r['interpretation']}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Placebo panel failed to load: {exc}")

    st.markdown("#### Granger tests &mdash; warehouse results")
    try:
        g_df_all = load_granger_results()
        if len(g_df_all):
            # The warehouse holds two runs: an untagged 2026-04-19 legacy run
            # (target_label IS NULL) and the 2026-04-22 canonical run against
            # subprime-auto roll rate. Tag each run explicitly so the charts
            # and table draw them side-by-side without "None" labels.
            g_df = g_df_all.copy()
            g_df["run_label"] = g_df["target_label"].where(
                g_df["target_label"].notna(), "legacy (2026-04-19, untagged)"
            )
            runs = list(dict.fromkeys(g_df["run_label"].tolist()))
            run_color = {
                r: (_ACCENT if "subprime" in r.lower() else _TEXT_S)
                for r in runs
            }

            # --- Chart 1: F-stat × lag, one line per run ----------------------
            fig_f = go.Figure()
            for r in runs:
                sub = g_df[g_df["run_label"] == r].sort_values("lag_weeks")
                fig_f.add_trace(go.Scatter(
                    x=sub["lag_weeks"], y=sub["f_stat"],
                    mode="lines+markers", name=r,
                    line=dict(color=run_color[r], width=2, shape="spline"),
                    marker=dict(size=8, color=run_color[r],
                                line=dict(color=_TEXT_P, width=1)),
                    hovertemplate=(
                        "<b>"+r+"</b><br>"
                        "lag = %{x} weeks<br>"
                        "F-stat = %{y:.3f}<extra></extra>"
                    ),
                ))
            # F_crit for α=0.05 at df_num=lag, df_den=399-lag-1 ≈ 2.0 across this range
            fig_f.add_hline(
                y=2.0, line=dict(color=_WARN, width=1, dash="dash"),
                annotation_text="F_crit ≈ 2.0 (α=0.05)",
                annotation_position="top right",
                annotation_font=dict(color=_WARN, size=10,
                                     family="JetBrains Mono, monospace"),
            )
            _style_fig(
                fig_f, height=260, xtitle="Granger lag (weeks)",
                ytitle="F-statistic",
            )

            # --- Chart 2: p-value × lag, log-y, threshold lines ---------------
            fig_p = go.Figure()
            for r in runs:
                sub = g_df[g_df["run_label"] == r].sort_values("lag_weeks")
                # Clip to a floor so log-scale doesn't blow up on p ~ 1e-6
                y = sub["p_value"].clip(lower=1e-6)
                fig_p.add_trace(go.Scatter(
                    x=sub["lag_weeks"], y=y,
                    mode="lines+markers", name=r,
                    line=dict(color=run_color[r], width=2, shape="spline"),
                    marker=dict(size=8, color=run_color[r],
                                line=dict(color=_TEXT_P, width=1)),
                    customdata=sub["p_value"],
                    hovertemplate=(
                        "<b>"+r+"</b><br>"
                        "lag = %{x} weeks<br>"
                        "p-value = %{customdata:.4f}<extra></extra>"
                    ),
                ))
            fig_p.add_hline(
                y=0.05, line=dict(color=_CRITICAL, width=1, dash="dash"),
                annotation_text="α = 0.05", annotation_position="bottom right",
                annotation_font=dict(color=_CRITICAL, size=10,
                                     family="JetBrains Mono, monospace"),
            )
            fig_p.add_hline(
                y=0.10, line=dict(color=_WARN, width=1, dash="dot"),
                annotation_text="α = 0.10 (marginal)",
                annotation_position="top right",
                annotation_font=dict(color=_WARN, size=10,
                                     family="JetBrains Mono, monospace"),
            )
            _style_fig(
                fig_p, height=260, xtitle="Granger lag (weeks)",
                ytitle="p-value (log)", yaxis_type="log",
            )

            # --- Chart 3: MDE ΔR² × lag from paper_headline_range() -----------
            try:
                from signals import granger_mde
                mde_rows = granger_mde.paper_headline_range()
                mde_df = pd.DataFrame([{
                    "lag": m.lag, "delta_r2": m.delta_r2,
                    "cohens_f2": m.cohens_f2, "f_crit": m.f_crit,
                } for m in mde_rows])
                fig_m = go.Figure()
                fig_m.add_trace(go.Scatter(
                    x=mde_df["lag"], y=100.0 * mde_df["delta_r2"],
                    mode="lines+markers", name="MDE ΔR² (%)",
                    line=dict(color=_ACCENT, width=2, shape="spline"),
                    marker=dict(size=8, color=_ACCENT,
                                line=dict(color=_TEXT_P, width=1)),
                    fill="tozeroy", fillcolor="rgba(56,189,248,0.10)",
                    hovertemplate=(
                        "lag = %{x} weeks<br>"
                        "ΔR² = %{y:.2f}%<extra></extra>"
                    ),
                ))
                _style_fig(
                    fig_m, height=220, showlegend=False,
                    xtitle="Granger lag (weeks)",
                    ytitle="minimum detectable ΔR² (%)",
                )
            except Exception as mde_exc:  # noqa: BLE001
                fig_m = None
                mde_err = str(mde_exc)

            # --- Render the two chart columns ---------------------------------
            cc1, cc2 = st.columns(2)
            with cc1:
                st.markdown("##### F-statistic by lag")
                st.plotly_chart(fig_f, use_container_width=True,
                                config={"displaylogo": False})
            with cc2:
                st.markdown("##### p-value by lag (log scale)")
                st.plotly_chart(fig_p, use_container_width=True,
                                config={"displaylogo": False})

            st.markdown("##### Minimum-detectable ΔR² &mdash; paper §7.1 power analysis")
            if fig_m is not None:
                st.plotly_chart(fig_m, use_container_width=True,
                                config={"displaylogo": False})
                st.caption(
                    "Non-central F, α=0.05, power=0.80, N=399 weekly obs. "
                    "At lag 4 we can detect ΔR² as small as "
                    f"{100*mde_df['delta_r2'].iloc[0]:.2f}%; at lag 8, "
                    f"{100*mde_df['delta_r2'].iloc[-1]:.2f}%. "
                    "This bounds how small an effect could still be hiding "
                    "beneath the non-rejection headline."
                )
            else:
                st.info(f"MDE curve unavailable: {mde_err}")

            # --- Table (only labeled rows, legacy rows in expander) -----------
            def _row_html(r) -> str:
                p = r["p_value"]
                if p <= 0.05:
                    chip = "<span class='chip chip-fail'>REJECTS null</span>"
                elif p <= 0.10:
                    chip = "<span class='chip chip-warn'>marginal</span>"
                else:
                    chip = "<span class='chip chip-pass'>non-reject</span>"
                tier_disp = (f"<span class='mono'>{int(r['tier'])}</span>"
                             if pd.notna(r["tier"]) else "&mdash;")
                return (
                    f"<tr>"
                    f"<td><span class='mono'>{r['run_label']}</span></td>"
                    f"<td>{tier_disp}</td>"
                    f"<td class='num'>{int(r['lag_weeks'])}</td>"
                    f"<td class='num'>{r['f_stat']:.3f}</td>"
                    f"<td class='num'>{r['p_value']:.4f}</td>"
                    f"<td class='num'>{int(r['n_obs'])}</td>"
                    f"<td>{chip}</td>"
                    f"</tr>"
                )

            labeled = g_df[g_df["target_label"].notna()].sort_values("lag_weeks")
            legacy = g_df[g_df["target_label"].isna()].sort_values("lag_weeks")
            rows_labeled = "".join(_row_html(r) for _, r in labeled.iterrows())

            st.markdown(
                f"""
                <div class="pod-card">
                  <table class="table-inst">
                    <thead><tr>
                      <th>Target</th><th>Tier</th>
                      <th style="text-align:right;">Lag (wk)</th>
                      <th style="text-align:right;">F-stat</th>
                      <th style="text-align:right;">p-value</th>
                      <th style="text-align:right;">N</th>
                      <th>Verdict</th>
                    </tr></thead>
                    <tbody>{rows_labeled}</tbody>
                  </table>
                  <div style="color:{_TEXT_S};font-size:0.75rem;margin-top:0.5rem;">
                    Paper §7.1: non-rejection dominates — the Granger gauntlet is
                    disclosed as a precondition check, not as an affirmative result.
                    MDE at n≈399 weekly, lags 4–8, α=0.05, power=0.80 → ΔR² ≈ 2.6–3.3%.
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            if len(legacy):
                with st.expander(
                    f"Show {len(legacy)} legacy untagged rows "
                    "(2026-04-19 run — kept for audit only)",
                    expanded=False,
                ):
                    rows_legacy = "".join(_row_html(r) for _, r in legacy.iterrows())
                    st.markdown(
                        f"""<div class="pod-card"><table class="table-inst">
                        <thead><tr>
                          <th>Target</th><th>Tier</th>
                          <th style="text-align:right;">Lag (wk)</th>
                          <th style="text-align:right;">F-stat</th>
                          <th style="text-align:right;">p-value</th>
                          <th style="text-align:right;">N</th>
                          <th>Verdict</th>
                        </tr></thead>
                        <tbody>{rows_legacy}</tbody></table>
                        <div style="color:{_TEXT_S};font-size:0.75rem;margin-top:0.5rem;">
                          target_label was NULL on this earlier run; F-stats look
                          superficially significant, but without a labelled
                          outcome the direction of causation is unidentified.
                          The canonical paper result is the subprime-auto run above.
                        </div></div>""",
                        unsafe_allow_html=True,
                    )
        else:
            st.info("granger_results table is empty.")
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Granger results unavailable: {exc}")

    st.markdown("#### Paper §6 disclosures")
    col_r, col_c = st.columns(2)
    with col_r:
        st.markdown(
            f"""
            <div class="pod-card">
              <div class="pod-card-title">Residualisation status</div>
              <div style="color:{_TEXT_P};font-size:0.875rem;">
                Origination-residual BSI scorer is specified and staged in
                <span class="mono">signals/bsi_residual.py</span> but
                <b>not yet populating numerical results</b>. Phase B 10-Q pulls
                (AFRM / SQ / PYPL) are the blocker.<br><br>
                Pre-registered decision rule (v2_roadmap §C.3):
                <br>&middot; ≥4/5 events fire under residualised scorer &rarr; retain framing.
                <br>&middot; ≤2/5 &rarr; swap to sealed alt abstract.
                <br>&middot; 3/5 &rarr; author decides.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col_c:
        st.markdown(
            f"""
            <div class="pod-card">
              <div class="pod-card-title">Gate-1 carry-over disclosure</div>
              <div style="color:{_TEXT_P};font-size:0.875rem;">
                The +1.5σ threshold was <b>calibrated against the v1
                180-day rolling-σ estimator</b> that §6 has retired in favour
                of the EWMA-σ form of Equation (1).<br><br>
                EWMA σ is tighter than v1 rolling σ on the 17 Jan 2025 pulse,
                so +1.5σ is weakly conservative: it fires under EWMA iff it
                fired under v1. Re-thresholding on realised EWMA σ is a
                <b>Phase C deliverable</b>, deliberately deferred to avoid
                post-hoc tuning.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# =========================================================================
# TAB 5 — WAREHOUSE
# =========================================================================

with tab_warehouse:
    st.markdown("#### Ingestion inventory")

    rows_html = []
    for _, r in inventory.iterrows():
        if r["status"] == "live":
            chip = f"<span class='chip chip-pass'>live</span>"
        elif r["status"] == "empty":
            chip = f"<span class='chip chip-muted'>empty</span>"
        else:
            chip = f"<span class='chip chip-fail'>err</span>"
        earliest = r["earliest"] or "—"
        latest_v = r["latest"] or "—"
        rows_html.append(
            f"<tr>"
            f"<td><span class='mono'>{r['table']}</span></td>"
            f"<td class='num'>{int(r['rows']):,}</td>"
            f"<td>{chip}</td>"
            f"<td><span class='mono' style='font-size:0.75rem;'>{r['time_col'] or '—'}</span></td>"
            f"<td><span class='mono' style='font-size:0.75rem;'>{earliest}</span></td>"
            f"<td><span class='mono' style='font-size:0.75rem;'>{latest_v}</span></td>"
            f"</tr>"
        )
    st.markdown(
        f"""
        <div class="pod-card">
          <table class="table-inst">
            <thead><tr>
              <th>Table</th>
              <th style="text-align:right;">Rows</th>
              <th>Status</th><th>Time col</th>
              <th>Earliest</th><th>Latest</th>
            </tr></thead>
            <tbody>{''.join(rows_html)}</tbody>
          </table>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col_firm, col_prod = st.columns(2)
    with col_firm:
        st.markdown("#### CFPB — top companies (all products)")
        try:
            df = load_cfpb_firm_breakdown(top_n=12)
            df["earliest"] = pd.to_datetime(df["earliest"]).dt.date
            df["latest"] = pd.to_datetime(df["latest"]).dt.date
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as exc:  # noqa: BLE001
            st.warning(f"firm breakdown: {exc}")

    with col_prod:
        st.markdown("#### CFPB — product breakdown")
        try:
            df = load_cfpb_product_breakdown()
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as exc:  # noqa: BLE001
            st.warning(f"product breakdown: {exc}")

    st.markdown("#### CFPB BNPL-issuer daily complaint count (full history)")
    try:
        daily = load_cfpb_daily_count()
        st.line_chart(daily, height=220, color=_ACCENT)
        st.caption(
            f"All BNPL-issuer complaint filings: {int(daily['n_complaints'].sum()):,} total rows "
            f"across {len(daily):,} filing days. Max single-day count: "
            f"{int(daily['n_complaints'].max()):,} on {daily['n_complaints'].idxmax().date()}."
        )
    except Exception as exc:  # noqa: BLE001
        st.warning(f"CFPB daily: {exc}")

    st.markdown("#### ABS tranche metrics &mdash; per-trust summary")
    try:
        df = load_abs_tranche_summary()
        for c in ("earliest", "latest"):
            df[c] = pd.to_datetime(df[c]).dt.date
        for c in ("avg_roll_60p", "max_roll_60p", "avg_excess_spread", "avg_cnl"):
            df[c] = df[c].round(3)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(
            f"{len(df):,} trusts shown (top by report count). "
            "roll_rate_60p = rolling-60+ day delinquency roll; "
            "excess_spread = weighted coupon − WAC − servicing fees; "
            "cnl = cumulative net loss."
        )
    except Exception as exc:  # noqa: BLE001
        st.warning(f"ABS summary: {exc}")

    st.markdown("#### FRED series inventory")
    try:
        df = load_fred_inventory()
        for c in ("earliest", "latest"):
            df[c] = pd.to_datetime(df[c]).dt.date
        df["last_value"] = df["last_value"].round(3)
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"FRED inventory: {exc}")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown(
    f"""
    <div class="footer-text">
    BNPL Pod v2.0.1 &middot; paper at
    <span class='mono'>paper_formal/paper_formal.pdf</span>
    (43 pages) &middot; canonical scorer at
    <span class='mono'>signals/bsi.py</span> &middot; compliance rules at
    <span class='mono'>agents/compliance_engine.py</span> &middot; placebo pipeline at
    <span class='mono'>signals/placebos.py</span>.
    </div>
    """,
    unsafe_allow_html=True,
)
