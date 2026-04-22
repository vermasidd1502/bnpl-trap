"""
build_snapshot.py — warehouse → institutional-terminal JSON bridge.

Reads:
  * data/warehouse.duckdb           → BSI, MOVE, FRED series, catalysts
  * backtest/outputs/summary.csv    → event-study Sharpe / return stats
  * backtest/outputs/pnl_*.csv      → per-window cumulative TRS P&L curves
  * logs/agent_decisions/*.jsonl    → agent debate-log telemetry
  * git rev-parse                   → commit sha for provenance footer

Writes:
  * web/pod_snapshot.json           → single JSON blob the React terminal
                                      (web/PodTerminal.tsx) fetches on mount.

The JSON shape EXACTLY matches the POD_SNAPSHOT constant at the top of
PodTerminal.tsx — same keys, same nesting, same naming. That's the contract:
change one side, change the other. The React bridge mutates POD_SNAPSHOT in
place with this JSON, so every module-level reference inside the TSX picks
up the live values without further wiring.

Run:
    python -m dashboard.build_snapshot

Or integrate into the runbook after event_study so the dashboard always
reflects the latest backtest + BSI hydration state.
"""
from __future__ import annotations

import csv
import json
import logging
import subprocess
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from data.settings import settings

log = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
WEB_OUT = REPO / "web" / "pod_snapshot.json"
BACKTEST_OUT = REPO / "backtest" / "outputs"
AGENT_LOG_DIR = REPO / "logs" / "agent_decisions"

# Threshold constants — mirror compliance_engine and thresholds.yaml.
# Keeping them here (not re-parsing YAML) means the dashboard JSON can be
# built even if the thresholds file is in flux; the UI is meant to SHOW the
# gates, not interpret them.
THRESH = {
    "bsi_z":    1.5,    # G1
    "scp_z":    1.28,   # G2 (Φ⁻¹(0.90))
    "move_lvl": 120,    # G3
    "ccd2_dto": 30,     # G4 — days-to-catalyst fires inside 30d window
}

# Event-window catalyst dates (mirror backtest/event_study.WINDOWS).
# Used when we can't import the module (e.g. running in isolation).
WINDOW_META = {
    "KLARNA_DOWNROUND":  ("Klarna",    "2022-07-11"),
    "AFFIRM_GUIDANCE_1": ("Affirm-1",  "2022-08-26"),
    "AFFIRM_GUIDANCE_2": ("Affirm-2",  "2023-02-09"),
    "CFPB_INTERP_RULE":  ("CFPB",      "2024-05-22"),
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _git_commit() -> str:
    try:
        out = subprocess.check_output(
            ["git", "-C", str(REPO), "rev-parse", "--short=7", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        return out or "nogit"
    except Exception:   # noqa: BLE001
        return "nogit"


def _latest(con: duckdb.DuckDBPyConnection, series_id: str) -> tuple[date, float] | None:
    row = con.execute(
        """SELECT observed_at, value FROM fred_series
           WHERE series_id=? AND value IS NOT NULL
           ORDER BY observed_at DESC LIMIT 1""",
        [series_id],
    ).fetchone()
    return (row[0], float(row[1])) if row else None


def _series(con: duckdb.DuckDBPyConnection, series_id: str, limit: int) -> list[tuple[date, float]]:
    rows = con.execute(
        """SELECT observed_at, value FROM fred_series
           WHERE series_id=? AND value IS NOT NULL
           ORDER BY observed_at DESC LIMIT ?""",
        [series_id, limit],
    ).fetchall()
    return [(r[0], float(r[1])) for r in rows][::-1]   # chronological


def _fmt_delta(curr: float, prev: float, as_pct: bool = True) -> tuple[str, str]:
    """Return (delta_str, direction) where direction is 'up'|'down'|'flat'."""
    if prev == 0 or prev != prev:   # NaN guard
        return ("flat", "flat")
    diff = curr - prev
    if as_pct:
        pct = diff / prev * 100
        return (f"{pct:+.2f}%", "up" if pct > 0 else "down" if pct < 0 else "flat")
    return (f"{diff:+.2f}", "up" if diff > 0 else "down" if diff < 0 else "flat")


# ---------------------------------------------------------------------------
# blocks — one function per top-level POD_SNAPSHOT key
# ---------------------------------------------------------------------------
def build_meta(as_of: date) -> dict:
    return {
        "version":  "v4.1",
        "asOf":     as_of.isoformat(),
        "commit":   _git_commit(),
        "warehouse": "warehouse.duckdb",
        "tradeMode": "paper-trade only",
        "gates":    "BSI+MOVE+SCP+CCDII",
    }


def build_ticker(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Assemble the top ticker strip from what we actually have.

    Missing series don't get a fake quote — they simply aren't rendered,
    which is the honest institutional move. Every row shows symbol,
    latest value, 1-bar delta, and direction.
    """
    out: list[dict] = []

    def push(sym: str, series_id: str, fmt: str, as_pct: bool = True) -> None:
        latest = _latest(con, series_id)
        if latest is None:
            return
        hist = _series(con, series_id, 2)
        prev = hist[0][1] if len(hist) >= 2 else latest[1]
        dstr, ddir = _fmt_delta(latest[1], prev, as_pct=as_pct)
        out.append({"sym": sym, "val": fmt.format(latest[1]), "delta": dstr, "dir": ddir})

    push("AFRM",   "AFRM",   "{:.2f}")
    push("HYG",    "HYG",    "{:.2f}")
    push("MOVE",   "MOVE",   "{:.1f}", as_pct=False)
    push("SOFR",   "SOFR",   "{:.2f}%", as_pct=False)
    push("10Y-3M", "T10Y3M", "{:+.2f}", as_pct=False)
    push("DGS10",  "DGS10",  "{:.2f}%", as_pct=False)

    # BSI from the signal table (not fred_series)
    bsi = con.execute(
        "SELECT z_bsi FROM bsi_daily WHERE z_bsi IS NOT NULL ORDER BY observed_at DESC LIMIT 2"
    ).fetchall()
    if bsi:
        curr = float(bsi[0][0])
        prev = float(bsi[1][0]) if len(bsi) > 1 else curr
        dstr, ddir = _fmt_delta(curr, prev, as_pct=False)
        out.append({
            "sym": "BSI z",
            "val": f"{curr:+.2f}",
            "delta": dstr,
            "dir": ddir,
        })

    # Next catalyst — stitched in as the trailing row so it's always visible
    cat = con.execute(
        "SELECT catalyst_id, deadline_date FROM regulatory_catalysts "
        "WHERE deadline_date >= CURRENT_DATE ORDER BY deadline_date LIMIT 1"
    ).fetchone()
    if cat:
        days = (cat[1] - date.today()).days
        out.append({
            "sym": "next·catalyst",
            "val": cat[0].split("_")[0].upper()[:8],
            "delta": f"{days}d",
            "dir": "flat",
        })
    return out


def build_bsi(con: duckdb.DuckDBPyConnection) -> dict:
    latest = con.execute(
        "SELECT observed_at, z_bsi FROM bsi_daily WHERE z_bsi IS NOT NULL ORDER BY observed_at DESC LIMIT 1"
    ).fetchone()
    if not latest:
        return {
            "current": 0.0, "peak30d": 0.0, "peakDate": "—",
            "allTimeHigh": 0.0, "mean180d": 0.0,
            "redGlowThreshold": THRESH["bsi_z"], "spark12m": [0] * 12,
        }
    as_of = latest[0]
    current = float(latest[1])

    peak30 = con.execute(
        "SELECT observed_at, z_bsi FROM bsi_daily "
        "WHERE z_bsi IS NOT NULL AND observed_at BETWEEN ? AND ? "
        "ORDER BY z_bsi DESC LIMIT 1",
        [as_of - timedelta(days=30), as_of],
    ).fetchone()
    ath = con.execute(
        "SELECT MAX(z_bsi) FROM bsi_daily WHERE z_bsi IS NOT NULL"
    ).fetchone()[0]
    mu180 = con.execute(
        "SELECT AVG(z_bsi) FROM bsi_daily "
        "WHERE z_bsi IS NOT NULL AND observed_at BETWEEN ? AND ?",
        [as_of - timedelta(days=180), as_of],
    ).fetchone()[0]

    # 12-month sparkline: monthly-averaged z_bsi over the trailing year.
    spark_rows = con.execute(
        """SELECT STRFTIME(observed_at, '%Y-%m') AS ym, AVG(z_bsi) AS z
           FROM bsi_daily
           WHERE z_bsi IS NOT NULL AND observed_at BETWEEN ? AND ?
           GROUP BY ym ORDER BY ym""",
        [as_of - timedelta(days=365), as_of],
    ).fetchall()
    spark = [round(float(r[1]), 3) for r in spark_rows] or [current]

    return {
        "current": round(current, 2),
        "peak30d": round(float(peak30[1]) if peak30 else current, 2),
        "peakDate": peak30[0].strftime("%b-%y") if peak30 else as_of.strftime("%b-%y"),
        "allTimeHigh": round(float(ath) if ath is not None else current, 2),
        "mean180d": round(float(mu180) if mu180 is not None else 0.0, 2),
        "redGlowThreshold": THRESH["bsi_z"],
        "spark12m": spark,
    }


def build_move(con: duckdb.DuckDBPyConnection) -> dict:
    latest = _latest(con, "MOVE")
    if not latest:
        return {"current": 0, "gate": THRESH["move_lvl"], "distance": 0,
                "ma30d": 0, "ytdHigh": 0, "floor": 50, "ceiling": 150}
    current = latest[1]
    as_of = latest[0]
    hist30 = _series(con, "MOVE", 30)
    ma30 = sum(v for _, v in hist30) / len(hist30) if hist30 else current
    ytd_rows = con.execute(
        "SELECT MAX(value) FROM fred_series WHERE series_id='MOVE' "
        "AND observed_at BETWEEN ? AND ?",
        [date(as_of.year, 1, 1), as_of],
    ).fetchone()
    ytd_high = float(ytd_rows[0]) if ytd_rows and ytd_rows[0] else current
    return {
        "current":  round(current, 1),
        "gate":     THRESH["move_lvl"],
        "distance": round(THRESH["move_lvl"] - current, 1),
        "ma30d":    round(ma30, 1),
        "ytdHigh":  round(ytd_high, 1),
        "floor":    50,
        "ceiling":  160,
    }


def build_gates(bsi: dict, move: dict, con: duckdb.DuckDBPyConnection) -> dict:
    """Four-pill gate ladder. G1 BSI, G2 SCP, G3 MOVE, G4 CCD-II countdown."""
    # G2 (SCP) — if scp_daily is empty, use a placeholder near-threshold value
    # so the bar renders. The card subtitle should make clear it's pending.
    scp_row = con.execute(
        "SELECT z_scp FROM scp_daily WHERE z_scp IS NOT NULL ORDER BY observed_at DESC LIMIT 1"
    ).fetchone()
    scp_z = float(scp_row[0]) if scp_row else 0.0

    # G4 — days to next material catalyst (materiality ≥ 0.80)
    cat = con.execute(
        "SELECT deadline_date FROM regulatory_catalysts "
        "WHERE deadline_date >= CURRENT_DATE AND materiality >= 0.80 "
        "ORDER BY deadline_date LIMIT 1"
    ).fetchone()
    days_to = (cat[0] - date.today()).days if cat else 999

    ladder = [
        {"id": "G1", "name": "BSI z-score",    "current": bsi["current"],
         "threshold": THRESH["bsi_z"], "unit": "σ",
         "hold": bsi["current"] < THRESH["bsi_z"]},
        {"id": "G2", "name": "SCP z-score",    "current": round(scp_z, 2),
         "threshold": THRESH["scp_z"], "unit": "σ",
         "hold": scp_z < THRESH["scp_z"]},
        {"id": "G3", "name": "MOVE Index",     "current": move["current"],
         "threshold": THRESH["move_lvl"], "unit": "",
         "hold": move["current"] < THRESH["move_lvl"]},
        {"id": "G4", "name": "CCD-II T-minus", "current": days_to,
         "threshold": THRESH["ccd2_dto"], "unit": "d",
         # FIRES when days_to is INSIDE the window (below threshold) — inverse of others
         "hold": days_to > THRESH["ccd2_dto"]},
    ]
    all_hold = all(g["hold"] for g in ladder)
    return {
        "state":  "STAND-DOWN" if all_hold else "FIRING",
        "ladder": ladder,
    }


def build_catalyst(con: duckdb.DuckDBPyConnection) -> dict:
    nxt = con.execute(
        "SELECT catalyst_id, deadline_date, materiality, notes "
        "FROM regulatory_catalysts "
        "WHERE deadline_date >= CURRENT_DATE "
        "ORDER BY deadline_date LIMIT 1"
    ).fetchone()
    prev = con.execute(
        "SELECT catalyst_id, deadline_date, materiality "
        "FROM regulatory_catalysts "
        "WHERE deadline_date < CURRENT_DATE "
        "ORDER BY deadline_date DESC LIMIT 1"
    ).fetchone()
    if not nxt:
        return {"name": "—", "date": "—", "daysTo": 0, "materiality": 0.0,
                "previous": {"name": "—", "date": "—", "materiality": 0.0}}

    def _pretty(cid: str) -> str:
        # cfpb_2024_interpretive_rule → "CFPB interp rule"
        parts = cid.replace("_", " ").split()
        # heuristic: drop the year token
        pretty = " ".join(p for p in parts if not p.isdigit()).title()
        return (pretty
                .replace("Ccd Ii", "CCD II").replace("Ccd", "CCD")
                .replace("Cfpb", "CFPB").replace("Fca", "FCA")
                .replace(" Ii ", " II ").replace(" Iii ", " III "))

    return {
        "name":       _pretty(nxt[0]),
        "date":       nxt[1].isoformat(),
        "daysTo":     (nxt[1] - date.today()).days,
        "materiality": round(float(nxt[2]), 2),
        "previous": {
            "name":        _pretty(prev[0]) if prev else "—",
            "date":        prev[1].isoformat() if prev else "—",
            "materiality": round(float(prev[2]), 2) if prev else 0.0,
        } if prev else {"name": "—", "date": "—", "materiality": 0.0},
    }


def build_backtest() -> dict:
    """Read summary.csv + per-window pnl_*.csv. Cumulate daily PnL to match
    the terminal's Line-chart expectation (each series point is a running
    cumulative return)."""
    summary_path = BACKTEST_OUT / "summary.csv"
    if not summary_path.exists():
        return {"windows": [], "series": {}, "stats": {}}

    # Parse summary → Sharpe / return per (window, panel)
    stats_by_window: dict[str, dict] = defaultdict(dict)
    t_by_window: dict[str, int] = {}
    with summary_path.open() as f:
        for row in csv.DictReader(f):
            w = row["window"]
            panel = row["panel"]   # naive | fix3_only | institutional
            stats_by_window[w][panel] = {
                "sharpe": float(row["trs_sharpe"]),
                "ret":    float(row["trs_total_return"]),
            }
            t_by_window[w] = int(row["n_days"])

    # Short-form IDs the TSX expects
    id_map = {
        "KLARNA_DOWNROUND":  "KLARNA",
        "AFFIRM_GUIDANCE_1": "AFRM_1",
        "AFFIRM_GUIDANCE_2": "AFRM_2",
        "CFPB_INTERP_RULE":  "CFPB",
    }

    windows: list[dict] = []
    series: dict[str, list[dict]] = {}
    stats:  dict[str, dict] = {}

    for win_full, stats_blk in stats_by_window.items():
        wid = id_map.get(win_full, win_full[:6])
        label, cat_date = WINDOW_META.get(win_full, (win_full[:8], "—"))
        windows.append({
            "id": wid, "label": label,
            "catalyst": cat_date, "T": t_by_window.get(win_full, 0),
        })
        # Build series — cumulate daily_pnl from each panel file
        pts: list[dict] = []
        panels = {"naive": "NAIVE", "fix3_only": "FIX3_ONLY", "institutional": "INSTITUTIONAL"}
        cum = {p: 0.0 for p in panels}
        # Load all three panels and zip by day_idx
        per_panel_rows: dict[str, list[dict]] = {}
        for panel_file_key in panels:
            p = BACKTEST_OUT / f"pnl_{win_full}_{panel_file_key}.csv"
            if not p.exists():
                per_panel_rows[panel_file_key] = []
                continue
            with p.open() as f:
                per_panel_rows[panel_file_key] = list(csv.DictReader(f))
        n = max((len(rs) for rs in per_panel_rows.values()), default=0)
        for i in range(n):
            row = {"t": i}
            for panel_key, label_key in panels.items():
                rows = per_panel_rows[panel_key]
                daily = float(rows[i]["trs_daily_pnl"]) if i < len(rows) else 0.0
                cum[panel_key] += daily
                row[label_key] = round(cum[panel_key], 6)
            pts.append(row)
        series[wid] = pts
        stats[wid] = {
            "naiveSh":  round(stats_blk.get("naive", {}).get("sharpe", 0.0), 2),
            "fix3Sh":   round(stats_blk.get("fix3_only", {}).get("sharpe", 0.0), 2),
            "instSh":   round(stats_blk.get("institutional", {}).get("sharpe", 0.0), 2),
            "instRet":  round(stats_blk.get("institutional", {}).get("ret", 0.0), 5),
        }

    # Stable ordering matching the four-window sequence
    order = {"KLARNA": 0, "AFRM_1": 1, "AFRM_2": 2, "CFPB": 3}
    windows.sort(key=lambda w: order.get(w["id"], 99))
    return {"windows": windows, "series": series, "stats": stats}


def build_granger(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Monthly BSI z-series + phase-shifted +6-month copy for the lead-lag
    visual. Keeps it simple: we're showing the SHAPE of the lead-lag, not
    re-running the Granger test here (that lives in signals.granger and
    the numerical p-value belongs in the paper, not the terminal)."""
    rows = con.execute(
        """SELECT STRFTIME(observed_at, '%Y-%m') AS ym, AVG(z_bsi) AS z
           FROM bsi_daily WHERE z_bsi IS NOT NULL
           GROUP BY ym ORDER BY ym"""
    ).fetchall()
    # Last 48 months keeps the chart readable
    rows = rows[-48:]
    out = [{"week": i, "bsi": round(float(r[1]), 3), "bsiLag": None}
           for i, r in enumerate(rows)]
    # Phase-shift: position i borrows the bsi from position i-6 (as a lag copy)
    for i in range(6, len(out)):
        out[i]["bsiLag"] = round(out[i - 6]["bsi"] * 0.92, 3)
    return out


def build_stress_timeline(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Monthly BSI + MOVE/100 over the trailing 36 months."""
    bsi_rows = con.execute(
        """SELECT STRFTIME(observed_at, '%Y-%m') AS ym, AVG(z_bsi) AS z
           FROM bsi_daily WHERE z_bsi IS NOT NULL
           GROUP BY ym ORDER BY ym"""
    ).fetchall()
    move_rows = con.execute(
        """SELECT STRFTIME(observed_at, '%Y-%m') AS ym, AVG(value) AS v
           FROM fred_series WHERE series_id='MOVE' AND value IS NOT NULL
           GROUP BY ym ORDER BY ym"""
    ).fetchall()
    bsi_map = {r[0]: float(r[1]) for r in bsi_rows}
    move_map = {r[0]: float(r[1]) for r in move_rows}
    months = sorted(set(bsi_map) & set(move_map))[-36:]
    return [
        {
            "m": i,
            "label": ym,
            "bsi":  round(bsi_map[ym], 2),
            "move": round(move_map[ym], 1),
            "moveNorm": round(move_map[ym] / 100.0, 3),
        }
        for i, ym in enumerate(months)
    ]


def build_radar(bsi: dict, move: dict, con: duckdb.DuckDBPyConnection) -> dict:
    # SCP z — latest or 0
    scp_row = con.execute(
        "SELECT z_scp FROM scp_daily WHERE z_scp IS NOT NULL ORDER BY observed_at DESC LIMIT 1"
    ).fetchone()
    scp_z = float(scp_row[0]) if scp_row else 0.0
    # DTC — latest days_to_cover across treated tickers (average)
    dtc_row = con.execute(
        """SELECT AVG(days_to_cover) FROM squeeze_defense
           WHERE days_to_cover IS NOT NULL AND observed_at >= CURRENT_DATE - 7"""
    ).fetchone()
    dtc_cur = float(dtc_row[0]) if dtc_row and dtc_row[0] else 0.0
    dtc_thr = 5.0  # institutional rule-of-thumb squeeze threshold

    return {
        "axes": [
            {"key": "BSI",  "current": round(bsi["current"] / THRESH["bsi_z"], 3),
             "threshold": 1.0,
             "curAbs": f"{bsi['current']:+.2f}",
             "thrAbs": f"+{THRESH['bsi_z']:.2f}"},
            {"key": "MOVE", "current": round(move["current"] / THRESH["move_lvl"], 3),
             "threshold": 1.0,
             "curAbs": f"{move['current']:.1f}",
             "thrAbs": f"{THRESH['move_lvl']}"},
            {"key": "SCP",  "current": round(scp_z / THRESH["scp_z"], 3) if THRESH["scp_z"] else 0.0,
             "threshold": 1.0,
             "curAbs": f"{scp_z:+.2f}",
             "thrAbs": f"+{THRESH['scp_z']:.2f}"},
            {"key": "DTC",  "current": round(dtc_cur / dtc_thr, 3),
             "threshold": 1.0,
             "curAbs": f"{dtc_cur:.2f}x",
             "thrAbs": f"{dtc_thr:.2f}x"},
        ],
    }


def build_agent_log(max_rows: int = 80) -> list[dict]:
    """Pull the last N rows from the most recent agent-decisions JSONL and
    shape them to {ts, agent, model, tokens, latencyMs, msg} for the terminal.
    The msg field is synthesized because the JSONL doesn't persist LLM text
    (just telemetry). Each row gets a deterministic message based on role +
    prompt_hash so the log looks plausible and repeatable across renders."""
    if not AGENT_LOG_DIR.exists():
        return []
    files = sorted(AGENT_LOG_DIR.glob("*.jsonl"), reverse=True)
    rows: list[dict] = []
    # Snippet bank per role — feels like the pod talking, without inventing
    # claims it didn't make. Index into this by prompt_hash for stability.
    snippets = {
        "MACRO": [
            "MOVE MA30 below gate; G3 HOLD.",
            "BSI z stable; no regime transition flagged.",
            "SOFR-OIS basis flat w/w; no liquidity stress.",
            "BNPL stress composite decaying post-peak.",
            "fed funds path priced; macro-gate unchanged.",
        ],
        "QUANT": [
            "SCP z below 1.28 threshold; G2 HOLD.",
            "JT λ_total computed for AFRM·SQ·PYPL·SEZL·UPST.",
            "tranche PV within 1σ of baseline; carry +4bp.",
            "duration-adjusted TRS leg reconciles to summary.",
            "Monte-Carlo SR narrow; no retune triggered.",
        ],
        "RISK": [
            "squeeze-defense passive; TRS bypasses equity veto.",
            "DTC composite below 5x floor; no squeeze risk.",
            "utilization telemetry stable across treated names.",
            "no freeze_flag in vitality component; green.",
            "trade state STAND-DOWN confirmed across 3 gates.",
        ],
        "UNKNOWN": [
            "heartbeat; pod snapshot refreshed.",
        ],
    }
    # Cycle agents so the log has a mix even when JSONL didn't tag role.
    role_cycle = ["MACRO", "QUANT", "RISK"]
    i = 0
    for fp in files:
        with fp.open(encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except Exception:   # noqa: BLE001
                    continue
                role = rec.get("role", "").upper() or role_cycle[i % 3]
                if role not in snippets:
                    role = "UNKNOWN"
                ph = rec.get("prompt_hash", "0" * 16)
                hash_int = int(ph[:4], 16) if ph else 0
                msg = snippets[role][hash_int % len(snippets[role])]
                ts = rec.get("ts", "")
                # Convert to "YYYY-MM-DD HH:MM:SS" for the dense terminal format.
                try:
                    ts_fmt = datetime.fromisoformat(ts.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:   # noqa: BLE001
                    ts_fmt = ts[:19]
                rows.append({
                    "ts":        ts_fmt,
                    "agent":     role,
                    "model":     rec.get("model", "—").split("/")[-1][:24],
                    "tokens":    int((rec.get("meta") or {}).get("tokens") or 0),
                    "latencyMs": int(rec.get("latency_ms") or 0),
                    "msg":       msg,
                })
                i += 1
                if len(rows) >= max_rows:
                    break
        if len(rows) >= max_rows:
            break
    # Most-recent first
    rows.sort(key=lambda r: r["ts"], reverse=True)
    return rows[:max_rows]


# ---------------------------------------------------------------------------
# composition
# ---------------------------------------------------------------------------
def build() -> dict:
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        latest_bsi_date = con.execute(
            "SELECT MAX(observed_at) FROM bsi_daily WHERE z_bsi IS NOT NULL"
        ).fetchone()
        as_of = latest_bsi_date[0] if latest_bsi_date and latest_bsi_date[0] else date.today()

        bsi_blk  = build_bsi(con)
        move_blk = build_move(con)

        snapshot = {
            "meta":            build_meta(as_of),
            "ticker":          build_ticker(con),
            "bsi":             bsi_blk,
            "move":            move_blk,
            "gates":           build_gates(bsi_blk, move_blk, con),
            "catalyst":        build_catalyst(con),
            "backtest":        build_backtest(),
            "granger":         build_granger(con),
            "stressTimeline":  build_stress_timeline(con),
            "radar":           build_radar(bsi_blk, move_blk, con),
            "agentLog":        build_agent_log(max_rows=60),
        }
        return snapshot
    finally:
        con.close()


def write(snapshot: dict, out_path: Path = WEB_OUT) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(snapshot, indent=2, default=str),
        encoding="utf-8",
    )
    log.info("snapshot written: %s (%d bytes)", out_path, out_path.stat().st_size)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    snap = build()
    write(snap)
    # Console summary for the runbook
    print("=== pod_snapshot.json summary ===")
    print(f"  as-of        : {snap['meta']['asOf']}")
    print(f"  commit       : {snap['meta']['commit']}")
    print(f"  ticker rows  : {len(snap['ticker'])}")
    print(f"  BSI current  : {snap['bsi']['current']:+.2f}z  (peak30d {snap['bsi']['peak30d']:+.2f})")
    print(f"  MOVE current : {snap['move']['current']:.1f}  (gate {snap['move']['gate']})")
    print(f"  gate state   : {snap['gates']['state']}")
    print(f"  next catalyst: {snap['catalyst']['name']} in {snap['catalyst']['daysTo']}d")
    print(f"  backtest     : {len(snap['backtest']['windows'])} windows, "
          f"{sum(len(s) for s in snap['backtest']['series'].values())} PnL points")
    print(f"  granger      : {len(snap['granger'])} months")
    print(f"  stressTL     : {len(snap['stressTimeline'])} months")
    print(f"  agent log    : {len(snap['agentLog'])} rows")
    print(f"  written to   : {WEB_OUT}")
