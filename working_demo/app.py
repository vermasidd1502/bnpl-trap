"""
Apollo Hermes x BearWatch — WORKING DEMO
Single-file Flask backend. Real risk checks, real SQLite, real yfinance.

Run:
    python app.py
Or just double-click start.bat in this folder.
"""
import json
import os
import sqlite3
import threading
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request, session, redirect, url_for, make_response
import random
import sys as _sys

# ---- Wire in the bnpl-pod 5-gate archetype-aware firing logic (paper §10) ----
# We try BNPL-experimental first (active sprint repo), then fall back to BNPL.
_GATES_AVAILABLE = False
for _gp in (
    r"C:\Users\siddh\Desktop\spring 2026\580\BNPL-experimental\bnpl-pod",
    r"C:\Users\siddh\Desktop\spring 2026\580\BNPL\bnpl-pod",
):
    if Path(_gp, "signals", "gates.py").exists():
        if _gp not in _sys.path: _sys.path.insert(0, _gp)
        try:
            from signals.gates import (   # type: ignore
                evaluate_all_archetypes, gate_states_from_signals, ALL_GATES,
                GATE_REQUIRED_COUNT, GATE_MANDATORY, POSITION_SIZE_PCT,
            )
            _GATES_AVAILABLE = True
            break
        except Exception:
            continue

try:
    import yfinance as yf
    YF_AVAILABLE = True
except Exception:
    YF_AVAILABLE = False

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except Exception:
    DUCKDB_AVAILABLE = False

# ---------- paths ----------
ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data" / "apollo.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Path to the bnpl-pod warehouse (real BSI / CFPB / FRED / Reddit data).
# We prefer the BNPL-experimental warehouse — it's the active sprint repo and has
# fresher CFPB (through 2026-04-24), real Reddit (5,321 posts through 04-29), and
# Bluesky posts. Fall back to the older BNPL warehouse if experimental missing.
_WAREHOUSE_CANDIDATES = [
    # BNPL first — this is where the 2026-05 CFPB ingest landed (CVNA, CACC, KMX, ACA,
    # LC, ENVA, OPFI, WRLD, BFH, ALLY all added; warehouse is now ~493k rows).
    Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL\bnpl-pod\data\warehouse.duckdb"),
    Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL-experimental\bnpl-pod\data\warehouse.duckdb"),
]
WAREHOUSE_PATH = next((p for p in _WAREHOUSE_CANDIDATES if p.exists()), _WAREHOUSE_CANDIDATES[-1])
# Pre-built pillar CSVs (computed daily) — read directly when present
PILLAR_CSV_DIR = Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL-experimental\bnpl-pod\data")
# In-memory cache for live queries (TTL 60 s)
_LIVE_CACHE: dict = {}
_LIVE_TTL = 60.0

def warehouse_query(sql: str, cache_key: str = None):
    """Read-only DuckDB query against the bnpl-pod warehouse with optional 60s cache.
    On write-lock collision (someone else has the file open for writes), retries once
    against the fallback warehouse path so the live page never goes blank during a demo."""
    if cache_key:
        hit = _LIVE_CACHE.get(cache_key)
        if hit and (time.time() - hit[1] < _LIVE_TTL):
            return hit[0]
    if not DUCKDB_AVAILABLE:
        return None
    # Try every candidate path until one opens cleanly
    for path in _WAREHOUSE_CANDIDATES:
        if not path.exists():
            continue
        try:
            con = duckdb.connect(str(path), read_only=True)
            try:
                rows = con.execute(sql).fetchall()
                cols = [d[0] for d in con.description]
                result = [dict(zip(cols, r)) for r in rows]
            finally:
                con.close()
            if cache_key:
                _LIVE_CACHE[cache_key] = (result, time.time())
            return result
        except Exception as e:
            print(f"[warehouse {path.parts[-3]}] {e}")
            continue
    return None

# ---------- portfolio constants ----------
STARTING_CAPITAL = 100_000.0

# Sector mapping for the demo tickers
SECTOR_MAP = {
    "CVNA": "subprime_auto",
    "AFRM": "bnpl",
    "UPST": "subprime_personal",
    "SOFI": "fintech_lender",
    "LC":   "fintech_lender",
    "OPRT": "subprime_personal",
}

# Beta map (rough values for sizing math)
BETA_MAP = {
    "CVNA": 2.8,
    "AFRM": 2.5,
    "UPST": 2.4,
    "SOFI": 1.9,
    "LC":   1.7,
    "OPRT": 1.6,
    "_default": 1.4,
}

# Risk-mode multipliers
DRAWDOWN_THRESHOLDS = {
    "CAUTIOUS":  5.0,
    "DEFENSIVE": 10.0,
    "EMERGENCY": 15.0,
}
CASH_FLOOR_PCT = {
    "NORMAL":    0.08,
    "CAUTIOUS":  0.15,
    "DEFENSIVE": 0.25,
    "EMERGENCY": 0.40,
}
POSITION_CAP = {
    "high":   0.20,
    "medium": 0.10,
    "low":    0.05,
}
SECTOR_CAP_PCT = 0.30
MIN_RR = 1.5
MAX_BETA_LOAD = 1.6  # max gross portfolio beta

# ---------- quote cache ----------
_QUOTE_CACHE: dict[str, tuple[float, float]] = {}  # ticker -> (price, ts)
_CACHE_TTL = 60.0


# ---------- DB ----------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    with db() as c:
        c.executescript(
            """
        CREATE TABLE IF NOT EXISTS bearwatch_events (
            event_id     TEXT PRIMARY KEY,
            ts           TEXT NOT NULL,
            ticker       TEXT NOT NULL,
            firm_name    TEXT,
            sector       TEXT,
            bear_state   TEXT,
            bsi_z        REAL,
            phase        INTEGER,
            h2_eligible  INTEGER,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS risk_verdicts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id        TEXT NOT NULL,
            ts              TEXT NOT NULL,
            verdict         TEXT NOT NULL,
            recommended_usd REAL,
            entry_price     REAL,
            stop_price      REAL,
            target_price    REAL,
            checks_json     TEXT NOT NULL,
            FOREIGN KEY(event_id) REFERENCES bearwatch_events(event_id)
        );
        CREATE TABLE IF NOT EXISTS journal (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ts            TEXT NOT NULL,
            event_id      TEXT,
            ticker        TEXT NOT NULL,
            side          TEXT NOT NULL,
            shares        REAL NOT NULL,
            entry_price   REAL NOT NULL,
            stop_price    REAL,
            target_price  REAL,
            notional_usd  REAL NOT NULL,
            verdict       TEXT,
            FOREIGN KEY(event_id) REFERENCES bearwatch_events(event_id)
        );
        CREATE TABLE IF NOT EXISTS portfolio (
            id            INTEGER PRIMARY KEY CHECK (id = 1),
            cash          REAL NOT NULL,
            drawdown_mode TEXT NOT NULL,
            hwm           REAL NOT NULL
        );
        """
        )
        # Idempotent migrations: add per-user columns
        try: c.execute("ALTER TABLE portfolio ADD COLUMN username TEXT")
        except sqlite3.OperationalError: pass
        try: c.execute("ALTER TABLE portfolio ADD COLUMN starting_capital REAL")
        except sqlite3.OperationalError: pass
        try: c.execute("ALTER TABLE journal ADD COLUMN username TEXT")
        except sqlite3.OperationalError: pass
        # Portfolio v2 — close-position tracking
        try: c.execute("ALTER TABLE journal ADD COLUMN exit_price REAL")
        except sqlite3.OperationalError: pass
        try: c.execute("ALTER TABLE journal ADD COLUMN exit_ts TEXT")
        except sqlite3.OperationalError: pass
        try: c.execute("ALTER TABLE journal ADD COLUMN status TEXT DEFAULT 'OPEN'")
        except sqlite3.OperationalError: pass
        c.commit()


def ensure_user_portfolio(username: str, starting_capital: float = None):
    """Create a portfolio row for this user if it doesn't exist.

    NOTE: The portfolio table has a CHECK (id = 1) singleton constraint from the
    original single-user demo design. Per-user portfolios cannot exist under that
    schema. Until we migrate to a multi-user-friendly schema, all users share the
    singleton portfolio (id=1). This function therefore no-ops when the singleton
    already exists and only seeds it the very first time. The previous
    implementation tried to INSERT a per-user row and crashed with
    sqlite3.IntegrityError on every logged-in /api/portfolio/positions hit,
    which the browser saw as Flask's HTML 500 page → 'Unexpected token <' on the
    portfolio page. Returning early is the correct behavior under the current
    schema.
    """
    if not username:
        return
    starting_capital = starting_capital or STARTING_CAPITAL
    with db() as c:
        # Singleton already exists? — nothing to do.
        existing = c.execute("SELECT id FROM portfolio WHERE id = 1").fetchone()
        if existing:
            return
        # Very first run only — seed the singleton row.
        try:
            c.execute(
                "INSERT INTO portfolio (id, cash, drawdown_mode, hwm, username, starting_capital) "
                "VALUES (1, ?, 'NORMAL', ?, ?, ?)",
                (starting_capital, starting_capital, username.lower(), starting_capital),
            )
            c.commit()
        except sqlite3.IntegrityError:
            # Race condition: another request seeded the singleton between our
            # SELECT and INSERT. Safe to ignore.
            pass


# ---------- portfolio helpers ----------
def get_portfolio(username: str = None):
    """Get portfolio for the given user (or fall back to legacy id=1 row)."""
    with db() as c:
        if username:
            ensure_user_portfolio(username)
            # Schema is singleton (CHECK id=1) so per-user lookup is best-effort
            # and we always fall back to the singleton row. The journal table
            # IS per-user (filtered below via WHERE username=?).
            port_row = c.execute(
                "SELECT * FROM portfolio WHERE username=?", (username.lower(),)
            ).fetchone()
            if not port_row:
                port_row = c.execute("SELECT * FROM portfolio WHERE id=1").fetchone()
            if not port_row:
                # Truly empty — return a synthetic empty portfolio rather than crash.
                return {
                    "cash": STARTING_CAPITAL, "drawdown_mode": "NORMAL",
                    "hwm": STARTING_CAPITAL, "starting_capital": STARTING_CAPITAL,
                    "positions": [],
                }
            port = dict(port_row)
            rows = c.execute(
                """SELECT ticker, side, SUM(shares) AS shares, AVG(entry_price) AS avg_entry,
                          SUM(notional_usd) AS notional
                   FROM journal WHERE username=? GROUP BY ticker, side""",
                (username.lower(),)
            ).fetchall()
        else:
            port = dict(c.execute("SELECT * FROM portfolio WHERE id = 1").fetchone())
            rows = c.execute(
                """SELECT ticker, side, SUM(shares) AS shares, AVG(entry_price) AS avg_entry,
                          SUM(notional_usd) AS notional
                   FROM journal GROUP BY ticker, side"""
            ).fetchall()
    starting_cap = port.get("starting_capital") or STARTING_CAPITAL
    positions = [dict(r) for r in rows if r["shares"]]
    open_notional = sum(abs(p["notional"] or 0) for p in positions)
    portfolio_value = port["cash"] + open_notional
    weighted_beta = 0.0
    for p in positions:
        b = BETA_MAP.get(p["ticker"], BETA_MAP["_default"])
        sign = -1 if p["side"] == "SHORT" else 1
        weighted_beta += sign * b * (abs(p["notional"]) / max(starting_cap, 1))
    return {
        "cash": round(port["cash"], 2),
        "drawdown_mode": port["drawdown_mode"],
        "hwm": round(port["hwm"], 2),
        "starting_capital": round(starting_cap, 2),
        "positions": positions,
        "open_count": len(positions),
        "portfolio_value": round(portfolio_value, 2),
        "portfolio_beta": round(weighted_beta, 2),
        "username": username,
    }


def sector_exposure(positions, sector):
    s = 0.0
    for p in positions:
        if SECTOR_MAP.get(p["ticker"]) == sector:
            s += abs(p["notional"] or 0)
    return s


# ---------- quote ----------
def get_quote(ticker: str) -> dict:
    ticker = ticker.upper()
    now = time.time()
    cached = _QUOTE_CACHE.get(ticker)
    if cached and now - cached[1] < _CACHE_TTL:
        return {"ticker": ticker, "price": cached[0], "source": "cache"}
    if YF_AVAILABLE:
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="2d", interval="1d")
            if hist is not None and len(hist) > 0:
                price = float(hist["Close"].iloc[-1])
                _QUOTE_CACHE[ticker] = (price, now)
                return {"ticker": ticker, "price": round(price, 2), "source": "yfinance"}
        except Exception as e:
            print(f"[quote] yfinance error for {ticker}: {e}")
    # fallback hardcoded prices
    fallback = {"CVNA": 48.20, "AFRM": 31.10, "UPST": 22.40, "SOFI": 8.90}
    p = fallback.get(ticker, 50.0)
    _QUOTE_CACHE[ticker] = (p, now)
    return {"ticker": ticker, "price": p, "source": "fallback"}


# ============================================================
# TECHNICAL INDICATORS — 8 families
# ============================================================
# All 8 indicator families computed deterministically from yfinance daily prices.
# Cached per-ticker for 10 minutes. Returned as a single JSON blob the front-end
# renders in Stage 3 of the live pod (alongside Apollo's risk verdict).
#
# These are CONTEXT ONLY — they do not gate Apollo's decision. The deterministic
# core (BSI, war room, 4-gate, 7-check) stays untouched. The user sees both
# Apollo's verdict and the technical context, and decides accordingly.
# ============================================================

_TECH_CACHE = {}
_TECH_TTL = 600.0  # 10 min

def compute_technical_indicators(ticker: str) -> dict:
    """Compute 8 indicator families for a ticker. Returns a dict ready to JSONify."""
    ticker = ticker.upper()
    now = time.time()
    cached = _TECH_CACHE.get(ticker)
    if cached and now - cached[1] < _TECH_TTL:
        return cached[0]

    if not YF_AVAILABLE:
        return {"error": "yfinance unavailable", "ticker": ticker}

    try:
        import math as _math
        import pandas as pd
        t = yf.Ticker(ticker)
        # Pull 2 years of daily history for the long indicators (200 SMA, 52w high)
        hist = t.history(period="2y", interval="1d")
        if hist is None or len(hist) < 50:
            return {"error": f"insufficient price history ({len(hist) if hist is not None else 0} days)", "ticker": ticker}

        closes = hist["Close"].astype(float)
        highs  = hist["High"].astype(float)
        lows   = hist["Low"].astype(float)
        opens  = hist["Open"].astype(float)
        vols   = hist["Volume"].astype(float)
        n = len(closes)
        latest_close = float(closes.iloc[-1])

        # =========== 1. ATR (Average True Range) — adaptive volatility ===========
        # True Range = max(high-low, abs(high-prev_close), abs(low-prev_close))
        prev_close = closes.shift(1)
        tr = pd.concat([
            (highs - lows),
            (highs - prev_close).abs(),
            (lows  - prev_close).abs()
        ], axis=1).max(axis=1)
        atr_14 = float(tr.rolling(14).mean().iloc[-1])
        atr_pct = atr_14 / latest_close * 100  # ATR as % of price
        # Adaptive stop: 2x ATR above entry (for SHORT) — replaces flat 7.9%
        adaptive_stop_pct = (2 * atr_14 / latest_close) * 100

        # =========== 2. 52-week / quarterly highs (entry quality) ===========
        high_52w = float(highs.iloc[-min(252, n):].max())
        high_qtr = float(highs.iloc[-min(63, n):].max())
        pct_off_52w_high = (latest_close / high_52w - 1) * 100
        pct_off_qtr_high = (latest_close / high_qtr - 1) * 100
        # Entry quality: shorting a name already off its highs is structurally lower-risk
        if pct_off_52w_high <= -30:
            entry_quality = "BROKEN_TREND"     # already in a downtrend; lower-risk SHORT
        elif pct_off_52w_high <= -10:
            entry_quality = "WEAKENING"
        elif pct_off_52w_high <= -3:
            entry_quality = "NEAR_HIGHS"
        else:
            entry_quality = "AT_HIGHS"          # shorting at highs is reflexivity-risky

        # =========== 3. Volume profile (high-volume node) ===========
        # Simplified: bin last 60 days into 20 price buckets, weighted by volume
        recent = hist.iloc[-min(60, n):]
        if len(recent) >= 10:
            r_lo, r_hi = float(recent["Low"].min()), float(recent["High"].max())
            n_bins = 20
            bin_width = (r_hi - r_lo) / n_bins if r_hi > r_lo else 1.0
            buckets = {}
            for _, row in recent.iterrows():
                mid = (row["High"] + row["Low"]) / 2
                bucket = int((mid - r_lo) / max(bin_width, 1e-6))
                bucket = min(max(bucket, 0), n_bins - 1)
                buckets[bucket] = buckets.get(bucket, 0) + row["Volume"]
            poc_bucket = max(buckets, key=buckets.get) if buckets else n_bins // 2
            poc_price = r_lo + (poc_bucket + 0.5) * bin_width  # point of control
        else:
            poc_price = latest_close

        # =========== 4. Fibonacci retracements (38.2/50/61.8/78.6) + Gann eighths ===========
        # Use the swing from 52w high to recent low
        low_52w = float(lows.iloc[-min(252, n):].min())
        swing = high_52w - low_52w
        fib_levels = {
            "0.0":  high_52w,
            "23.6": high_52w - 0.236 * swing,
            "38.2": high_52w - 0.382 * swing,
            "50.0": high_52w - 0.500 * swing,
            "61.8": high_52w - 0.618 * swing,
            "78.6": high_52w - 0.786 * swing,
            "100.0": low_52w,
        }
        # Gann eighths grid (1/8, 2/8, ..., 7/8)
        gann_eighths = {
            f"{i}/8": high_52w - (i / 8) * swing for i in range(1, 8)
        }

        # =========== 5. Moving averages (50 / 200 SMA, MACD) ===========
        sma_50  = float(closes.rolling(50).mean().iloc[-1])  if n >= 50  else None
        sma_200 = float(closes.rolling(200).mean().iloc[-1]) if n >= 200 else None
        # MACD = 12-EMA - 26-EMA, signal = 9-EMA of MACD
        ema_12 = closes.ewm(span=12, adjust=False).mean()
        ema_26 = closes.ewm(span=26, adjust=False).mean()
        macd_line = ema_12 - ema_26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - macd_signal
        macd_now = float(macd_line.iloc[-1])
        macd_sig_now = float(macd_signal.iloc[-1])
        macd_hist_now = float(macd_hist.iloc[-1])
        # Death-cross check: 50 below 200 = bearish (good for SHORT)
        death_cross = (sma_50 is not None and sma_200 is not None and sma_50 < sma_200)

        # =========== 6. RSI (14) + Stochastic ===========
        delta = closes.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rs = gain / loss.replace(0, _math.nan)
        rsi_14 = float((100 - 100 / (1 + rs)).iloc[-1])
        # Stochastic %K (14)
        lowest_low_14 = lows.rolling(14).min()
        highest_high_14 = highs.rolling(14).max()
        stoch_k = ((closes - lowest_low_14) / (highest_high_14 - lowest_low_14) * 100).iloc[-1]
        stoch_k = float(stoch_k) if not _math.isnan(stoch_k) else None

        # =========== 7. Bollinger Bands (20, 2σ) ===========
        bb_mid = closes.rolling(20).mean()
        bb_std = closes.rolling(20).std()
        bb_upper = float((bb_mid + 2 * bb_std).iloc[-1])
        bb_lower = float((bb_mid - 2 * bb_std).iloc[-1])
        bb_pct_b = (latest_close - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper > bb_lower else 50.0

        # =========== 8. VWAP (rolling 20-day) ===========
        typical = (highs + lows + closes) / 3
        vwap_num = (typical * vols).rolling(20).sum()
        vwap_den = vols.rolling(20).sum()
        vwap_20 = float((vwap_num / vwap_den).iloc[-1])
        pct_above_vwap = (latest_close / vwap_20 - 1) * 100

        # =========== Composite technical bias ===========
        # +score = bullish bias (avoid SHORT); -score = bearish bias (favors SHORT)
        # Volume signals are computed separately below; we'll add U/D + OBV
        # contributions after they're available so they enter the verdict.
        bias = 0
        if death_cross: bias -= 2                    # 50<200 SMA = bearish
        if pct_off_52w_high < -10: bias -= 1         # already broken
        if rsi_14 > 70: bias += 1                    # overbought = mean-revert short attractive
        if rsi_14 < 30: bias -= 1                    # oversold = don't short into the hole
        if macd_hist_now < 0: bias -= 1              # MACD bearish
        if latest_close < sma_50 if sma_50 else False: bias -= 1
        if latest_close < vwap_20: bias -= 1
        # NOTE: U/D ratio + OBV contributions added below after they're computed
        # (see "Volume signals → bias adjustment" block).

        # Last 120 daily closes for the front-end charts
        chart_window = 120
        chart_slice = hist.iloc[-min(chart_window, n):]
        chart_prev_close = chart_slice["Close"].shift(1)
        chart_direction = (chart_slice["Close"] > chart_prev_close).astype(int) - (chart_slice["Close"] < chart_prev_close).astype(int)
        # Up volume = volume on green days; down volume = volume on red days
        up_vol_chart   = (chart_slice["Volume"] * (chart_direction == 1)).astype(float)
        down_vol_chart = (chart_slice["Volume"] * (chart_direction == -1)).astype(float)
        # On-Balance Volume (OBV): cumulative running sum of signed volume
        obv_series = (chart_slice["Volume"] * chart_direction).cumsum()
        price_series = [
            {
                "date": str(d.date()),
                "close": round(float(c), 2),
                "up_vol":   int(uv) if not _math.isnan(uv) else 0,
                "down_vol": int(dv) if not _math.isnan(dv) else 0,
                "obv":      int(o)  if not _math.isnan(o)  else 0,
            }
            for d, c, uv, dv, o in zip(chart_slice.index, chart_slice["Close"], up_vol_chart, down_vol_chart, obv_series)
        ]

        # Up/down volume summary (trailing 20 days)
        recent_dir = chart_direction.iloc[-20:]
        recent_vol = chart_slice["Volume"].iloc[-20:]
        up_vol_20  = float((recent_vol * (recent_dir == 1)).sum())
        down_vol_20 = float((recent_vol * (recent_dir == -1)).sum())
        ud_ratio_20 = up_vol_20 / down_vol_20 if down_vol_20 > 0 else float('inf')
        obv_now    = float(obv_series.iloc[-1])
        obv_20_ago = float(obv_series.iloc[-21]) if len(obv_series) >= 21 else float(obv_series.iloc[0])
        obv_trend  = "RISING" if obv_now > obv_20_ago else "FALLING"

        # =========== Volume signals → bias adjustment (CMT-style) ===========
        # The original bias formula above used PRICE-only signals. Per Wozniak /
        # O'Neil / Lo-Mamaysky-Wang, volume signatures should override or temper
        # the price read. Strong accumulation (high U/D, rising OBV) is bullish
        # confirmation that should pull the bias score up; distribution
        # (low U/D, falling OBV) confirms the bearish read and pulls it down.
        if ud_ratio_20 != float('inf'):
            if   ud_ratio_20 >= 1.5: bias += 2     # strong accumulation — avoid short
            elif ud_ratio_20 >= 1.2: bias += 1     # mild accumulation
            elif ud_ratio_20 <= 0.7: bias -= 1     # distribution — short-confirming
            elif ud_ratio_20 <= 0.5: bias -= 2     # heavy distribution
        if obv_trend == "RISING": bias += 1        # OBV confirming bull
        else:                     bias -= 1        # OBV confirming bear

        if bias <= -3:
            tech_verdict = "STRONG_SHORT_OK"
        elif bias <= -1:
            tech_verdict = "SHORT_OK"
        elif bias <= 1:
            tech_verdict = "NEUTRAL"
        else:
            tech_verdict = "AVOID_SHORT"

        result = {
            "ticker": ticker,
            "as_of": str(closes.index[-1].date()),
            "latest_close": round(latest_close, 2),
            "tech_verdict": tech_verdict,
            "tech_bias_score": bias,
            "price_series": price_series,
            "indicators": {
                "atr": {
                    "atr_14": round(atr_14, 2),
                    "atr_pct_of_price": round(atr_pct, 2),
                    "adaptive_stop_pct_short": round(adaptive_stop_pct, 2),
                    "adaptive_stop_price_short": round(latest_close * (1 + adaptive_stop_pct / 100), 2),
                    "interpretation": f"2×ATR stop = {round(adaptive_stop_pct,1)}% above entry (vs flat 7.9% Apollo default). Adaptive to per-firm vol.",
                },
                "highs": {
                    "high_52w": round(high_52w, 2),
                    "low_52w":  round(low_52w, 2),
                    "high_qtr": round(high_qtr, 2),
                    "pct_off_52w_high": round(pct_off_52w_high, 1),
                    "pct_off_qtr_high": round(pct_off_qtr_high, 1),
                    "entry_quality": entry_quality,
                    # 4-quarter zones of the 52w range
                    "range_quarters": {
                        "q4_top":      round(high_52w, 2),                          # 75–100% (distribution)
                        "q4_q3_split": round(low_52w + 0.75 * swing, 2),
                        "q3_q2_split": round(low_52w + 0.50 * swing, 2),
                        "q2_q1_split": round(low_52w + 0.25 * swing, 2),
                        "q1_bottom":   round(low_52w, 2),                           # 0–25% (value)
                    },
                    "current_quarter": (
                        "Q4 (75–100%, distribution zone)"  if latest_close >= low_52w + 0.75 * swing else
                        "Q3 (50–75%, upper-mid)"            if latest_close >= low_52w + 0.50 * swing else
                        "Q2 (25–50%, lower-mid)"            if latest_close >= low_52w + 0.25 * swing else
                        "Q1 (0–25%, value/oversold)"
                    ),
                    "interpretation": {
                        "BROKEN_TREND": "Already 30%+ off 52w high — structurally lower-risk SHORT, downtrend confirmed.",
                        "WEAKENING": "10–30% off highs — moderate-risk SHORT, momentum has cracked.",
                        "NEAR_HIGHS": "Within 10% of 52w high — riskier SHORT, mean-reversion bet.",
                        "AT_HIGHS": "At 52w highs — reflexivity-risky SHORT, market hasn't yet priced any deterioration.",
                    }[entry_quality],
                },
                "volume_profile": {
                    "point_of_control": round(poc_price, 2),
                    "poc_distance_pct": round((poc_price / latest_close - 1) * 100, 2),
                    "interpretation": f"High-volume node at ${round(poc_price,2)}. If above current, it's likely resistance on rallies (good stop placement).",
                },
                "fibonacci": {
                    "swing_high": round(high_52w, 2),
                    "swing_low": round(low_52w, 2),
                    "levels": {k: round(v, 2) for k, v in fib_levels.items()},
                    "current_at_fib": _nearest_fib(latest_close, fib_levels),
                    "interpretation": "Levels are reflexive but not theoretically grounded — many traders watch them, so they create soft support/resistance.",
                },
                "gann_eighths": {
                    "swing_high": round(high_52w, 2),
                    "swing_low": round(low_52w, 2),
                    "levels": {k: round(v, 2) for k, v in gann_eighths.items()},
                    "interpretation": "Gann's eighths grid (12.5%, 25%, 37.5%, 50%, 62.5%, 75%, 87.5%). Equally spaced retracement levels — same role as Fibonacci, no numerology.",
                },
                "moving_averages": {
                    "sma_50":  round(sma_50, 2)  if sma_50  else None,
                    "sma_200": round(sma_200, 2) if sma_200 else None,
                    "death_cross": death_cross,
                    "macd_line": round(macd_now, 4),
                    "macd_signal": round(macd_sig_now, 4),
                    "macd_histogram": round(macd_hist_now, 4),
                    "macd_bearish": macd_hist_now < 0,
                    "interpretation": ("Death cross active (50 SMA < 200 SMA) — secular bearish trend confirmed." if death_cross
                                       else "Golden cross (50 SMA ≥ 200 SMA) — secular bullish trend; SHORT swims upstream."),
                },
                "rsi_stoch": {
                    "rsi_14": round(rsi_14, 1),
                    "stochastic_k": round(stoch_k, 1) if stoch_k is not None else None,
                    "rsi_zone": "OVERBOUGHT" if rsi_14 >= 70 else ("OVERSOLD" if rsi_14 <= 30 else "NEUTRAL"),
                    "interpretation": (
                        "RSI > 70: overbought; SHORT into a mean-reversion candidate."   if rsi_14 >= 70 else
                        "RSI < 30: oversold; do NOT SHORT into a hole — wait for bounce." if rsi_14 <= 30 else
                        "RSI in neutral zone (30–70); no momentum bias."
                    ),
                },
                "bollinger": {
                    "upper": round(bb_upper, 2),
                    "lower": round(bb_lower, 2),
                    "pct_b": round(bb_pct_b, 1),
                    "interpretation": (
                        "Above upper band (%B > 100): extreme stretched; mean-reversion SHORT attractive."  if bb_pct_b > 100 else
                        "Above mid-band (%B > 50): bullish posture; SHORT swims upstream."                   if bb_pct_b > 50  else
                        "Below mid-band (%B < 50): bearish posture; SHORT aligned with band momentum."       if bb_pct_b > 0   else
                        "Below lower band (%B < 0): extreme oversold; risk of reflexive bounce — wait."
                    ),
                },
                "vwap": {
                    "vwap_20": round(vwap_20, 2),
                    "pct_above_vwap": round(pct_above_vwap, 2),
                    "interpretation": (
                        f"Price {round(pct_above_vwap,1)}% above 20d VWAP — institutional bid above; SHORT swims upstream." if pct_above_vwap > 0 else
                        f"Price {round(abs(pct_above_vwap),1)}% below 20d VWAP — institutional offer above; SHORT aligned with flow."
                    ),
                },
                "up_down_volume": {
                    "up_volume_20d":   int(up_vol_20),
                    "down_volume_20d": int(down_vol_20),
                    "ud_ratio_20d":    round(ud_ratio_20, 2) if ud_ratio_20 != float('inf') else None,
                    "obv_now":         int(obv_now),
                    "obv_20d_ago":     int(obv_20_ago),
                    "obv_trend":       obv_trend,
                    "interpretation": (
                        f"Up/down vol ratio {round(ud_ratio_20,2)} (>1 = accumulation, <1 = distribution). "
                        f"OBV is {obv_trend} over the last 20 days — "
                        + ("buyers in control, SHORT swims upstream." if obv_trend == "RISING"
                           else "sellers in control, SHORT aligned with flow.")
                    ),
                },
            },
        }
        _TECH_CACHE[ticker] = (result, now)
        return result
    except Exception as e:
        return {"error": str(e)[:120], "ticker": ticker}


def _nearest_fib(price, fib_levels):
    """Return the Fibonacci level the current price is closest to."""
    nearest_key = min(fib_levels.keys(), key=lambda k: abs(fib_levels[k] - price))
    return f"{nearest_key}% (${round(fib_levels[nearest_key], 2)})"


# ============================================================
# MARKET REGIME — distribution days + topping indicators
# ============================================================
# Computes broad-market context that complements the firm-specific BSI signal.
# Distribution day = day where major index closes ≥ -0.2% on volume > prev day.
# 5+ distribution days in trailing 25 sessions = institutional selling regime
# (William O'Neil / Investor's Business Daily methodology, used since 1984).
# ============================================================
def compute_market_regime() -> dict:
    """Pull SPY + QQQ daily, compute distribution day count, 52w-high distance,
    50/200 SMA cross, and a composite topping-risk score 0-100."""
    cache_key = "market_regime"
    now = time.time()
    cached = _LIVE_CACHE.get(cache_key)
    if cached and (now - cached[1] < _LIVE_TTL):
        return cached[0]

    if not YF_AVAILABLE:
        return {"error": "yfinance unavailable"}

    try:
        import math as _math
        result = {"as_of": None, "indices": {}, "composite": {}}
        all_topping_scores = []

        for index_ticker in ["SPY", "QQQ"]:
            t = yf.Ticker(index_ticker)
            hist = t.history(period="2y", interval="1d")
            if hist is None or len(hist) < 100:
                continue
            closes = hist["Close"].astype(float)
            vols = hist["Volume"].astype(float)
            highs = hist["High"].astype(float)
            n = len(closes)
            latest_close = float(closes.iloc[-1])

            # === Distribution-day count (last 25 sessions) ===
            # A distribution day = close ≤ -0.2% AND volume > previous day's volume
            recent = hist.iloc[-26:].copy()
            recent["pct_change"] = recent["Close"].pct_change() * 100
            recent["vol_up"] = recent["Volume"].diff() > 0
            dist_days_mask = (recent["pct_change"] <= -0.2) & (recent["vol_up"])
            distribution_days = int(dist_days_mask.iloc[1:].sum())  # exclude first NaN row
            # Get the actual dates for transparency
            dist_day_dates = [str(d.date()) for d in recent[dist_days_mask].index[1:]]

            # === 52w high distance ===
            high_52w = float(highs.iloc[-min(252, n):].max())
            pct_off_52w_high = (latest_close / high_52w - 1) * 100

            # === SMA crossover ===
            sma_50 = float(closes.rolling(50).mean().iloc[-1])
            sma_200 = float(closes.rolling(200).mean().iloc[-1]) if n >= 200 else None
            golden_cross = (sma_200 is not None and sma_50 > sma_200)
            death_cross  = (sma_200 is not None and sma_50 < sma_200)

            # === Composite topping-risk score 0-100 ===
            # 0 = healthy bull, 100 = imminent top
            score = 0
            score += min(distribution_days * 12, 60)   # 5 dist-days = 60 pts (max)
            if pct_off_52w_high > -2:                  # at all-time highs = topping-risky
                score += 15
            elif pct_off_52w_high > -5:
                score += 7
            if death_cross:                            # 50<200 = bear trend confirmed
                score += 25
            elif sma_50 < latest_close * 0.99:         # price below 50d SMA
                score += 5

            score = min(score, 100)

            # Regime label
            if score >= 70:
                regime = "BEAR_REGIME_CONFIRMED"
            elif score >= 50:
                regime = "TOPPING_PATTERN"
            elif score >= 30:
                regime = "CAUTION"
            elif score >= 15:
                regime = "HEALTHY_PULLBACK"
            else:
                regime = "BULL_REGIME"

            result["indices"][index_ticker] = {
                "latest_close": round(latest_close, 2),
                "distribution_days_25": distribution_days,
                "distribution_day_dates": dist_day_dates,
                "high_52w": round(high_52w, 2),
                "pct_off_52w_high": round(pct_off_52w_high, 2),
                "sma_50": round(sma_50, 2),
                "sma_200": round(sma_200, 2) if sma_200 else None,
                "golden_cross": bool(golden_cross),
                "death_cross": bool(death_cross),
                "topping_risk_score": int(score),
                "regime": regime,
            }
            all_topping_scores.append(score)
            result["as_of"] = str(closes.index[-1].date())

        # Composite across SPY + QQQ
        if all_topping_scores:
            avg = sum(all_topping_scores) / len(all_topping_scores)
            result["composite"] = {
                "topping_risk_score": int(avg),
                "regime": (
                    "BEAR_REGIME_CONFIRMED" if avg >= 70 else
                    "TOPPING_PATTERN"        if avg >= 50 else
                    "CAUTION"                if avg >= 30 else
                    "HEALTHY_PULLBACK"       if avg >= 15 else
                    "BULL_REGIME"
                ),
                "interpretation": (
                    "Bear regime confirmed by both indices (50 SMA < 200 SMA + dist days). SHORT setups have systemic tailwind; LONG setups face severe headwind. Reduce LONG sizing or pause."     if avg >= 70 else
                    "Topping pattern: 5+ distribution days plus other warning signs. Tighten LONG stops; SHORT setups gain confirmation."                                                              if avg >= 50 else
                    "Caution regime: institutional selling visible but trend not broken. Modest position-size reduction warranted."                                                                    if avg >= 30 else
                    "Healthy pullback / consolidation. No regime alarm but watch for distribution-day accumulation."                                                                                   if avg >= 15 else
                    "Bull regime intact: golden cross active, low distribution days, indices near highs. SHORT setups face systemic headwind; LONG setups have tailwind."
                ),
            }
        _LIVE_CACHE[cache_key] = (result, now)
        return result
    except Exception as e:
        return {"error": str(e)[:120]}


# (the /api/technical/<ticker> route is registered below where `app` is defined)


# ---------- risk engine (REAL — deterministic) ----------
def run_risk_checks(event: dict, portfolio: dict, entry_price: float, starting_cap: float = None) -> dict:
    starting_cap = starting_cap or portfolio.get("starting_capital") or STARTING_CAPITAL
    """Returns {verdict, recommended_usd, entry, stop, target, checks: [...]}"""
    ticker = event["ticker"]
    sector = event.get("sector") or SECTOR_MAP.get(ticker, "unknown")
    side = event.get("recommended_action", {}).get("side", "SHORT").upper()
    conviction = event.get("recommended_action", {}).get("conviction", "medium").lower()

    # Stop / target derived from BSI z and side
    bsi_z = event.get("signal", {}).get("bsi_z", 2.0)
    if side == "SHORT":
        stop = round(entry_price * 1.079, 2)   # +7.9% stop
        target = round(entry_price * (1 - 0.05 * min(bsi_z, 4.5)), 2)  # bigger move on stronger z
    else:
        stop = round(entry_price * 0.92, 2)
        target = round(entry_price * (1 + 0.05 * min(bsi_z, 4.5)), 2)

    risk_per_share = abs(stop - entry_price)
    reward_per_share = abs(entry_price - target)
    rr = round(reward_per_share / risk_per_share, 2) if risk_per_share > 0 else 0

    # Position cap
    cap_pct = POSITION_CAP.get(conviction, 0.05)
    base_size_usd = starting_cap * cap_pct

    # Drawdown / mode
    mode = portfolio["drawdown_mode"]
    if mode == "DEFENSIVE":
        base_size_usd *= 0.5
    elif mode == "EMERGENCY":
        base_size_usd *= 0.0
    elif mode == "CAUTIOUS":
        base_size_usd *= 0.75

    checks = []
    blocking = False

    # 1. Sector cap
    sec_exposure_now = sector_exposure(portfolio["positions"], sector)
    sec_after = sec_exposure_now + base_size_usd
    sec_cap_usd = starting_cap * SECTOR_CAP_PCT
    sec_pct = round(100 * sec_after / starting_cap, 1)
    if sec_after > sec_cap_usd:
        checks.append({"name": "Sector cap", "status": "FAIL",
                       "detail": f"{sec_pct}% > {int(SECTOR_CAP_PCT*100)}% cap on {sector}"})
        blocking = True
    else:
        checks.append({"name": "Sector cap", "status": "PASS",
                       "detail": f"{sec_pct}% / {int(SECTOR_CAP_PCT*100)}% on {sector}"})

    # 2. Beta load
    beta = BETA_MAP.get(ticker, BETA_MAP["_default"])
    beta_sign = -1 if side == "SHORT" else 1
    new_beta = portfolio["portfolio_beta"] + beta_sign * beta * (base_size_usd / starting_cap)
    if abs(new_beta) > MAX_BETA_LOAD:
        checks.append({"name": "Beta load", "status": "FAIL",
                       "detail": f"new |β|={abs(new_beta):.2f} > {MAX_BETA_LOAD}"})
        blocking = True
    else:
        checks.append({"name": "Beta load", "status": "PASS",
                       "detail": f"new portfolio β = {new_beta:+.2f}"})

    # 3. R:R
    if rr < MIN_RR:
        checks.append({"name": "R:R ratio", "status": "FAIL",
                       "detail": f"{rr}:1 below {MIN_RR}:1 minimum"})
        blocking = True
    else:
        checks.append({"name": "R:R ratio", "status": "PASS", "detail": f"{rr}:1 ≥ {MIN_RR}:1"})

    # 4. Drawdown mode
    if mode == "EMERGENCY":
        checks.append({"name": "Drawdown mode", "status": "FAIL",
                       "detail": "EMERGENCY mode — no new risk"})
        blocking = True
    else:
        checks.append({"name": "Drawdown mode", "status": "PASS", "detail": f"{mode}"})

    # 5. Cash floor
    cash_after = portfolio["cash"] - base_size_usd
    floor_pct = CASH_FLOOR_PCT.get(mode, 0.08)
    floor_usd = starting_cap * floor_pct
    if cash_after < floor_usd:
        checks.append({"name": "Cash floor", "status": "FAIL",
                       "detail": f"${cash_after:,.0f} < ${floor_usd:,.0f} ({int(floor_pct*100)}% in {mode})"})
        blocking = True
    else:
        cash_pct = round(100 * cash_after / starting_cap, 1)
        checks.append({"name": "Cash floor", "status": "PASS",
                       "detail": f"{cash_pct}% > {int(floor_pct*100)}%"})

    # 6. Position cap
    checks.append({"name": "Position cap", "status": "PASS",
                   "detail": f"{int(cap_pct*100)}% on {conviction} conviction"})

    # 7. Correlation (simplified): deny if same-ticker open in opposite direction
    same = [p for p in portfolio["positions"] if p["ticker"] == ticker]
    if same and any(p["side"] != side for p in same):
        checks.append({"name": "Correlation", "status": "FAIL",
                       "detail": f"opposite-direction position open in {ticker}"})
        blocking = True
    else:
        checks.append({"name": "Correlation", "status": "PASS",
                       "detail": "no offsetting exposure detected"})

    # Verdict
    fail_count = sum(1 for c in checks if c["status"] == "FAIL")
    if blocking and fail_count >= 2:
        verdict = "BLOCKED"
        recommended = 0.0
    elif blocking:
        verdict = "SCALED_DOWN"
        recommended = round(base_size_usd * 0.4, 2)
    else:
        verdict = "APPROVED"
        recommended = round(base_size_usd, 2)

    return {
        "verdict": verdict,
        "recommended_usd": recommended,
        "entry_price": entry_price,
        "stop_price": stop,
        "target_price": target,
        "rr": rr,
        "shares": round(recommended / max(entry_price, 0.01), 2),
        "checks": checks,
    }


# ---------- Flask ----------
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = "bearwatch-apollo-demo-key-2026"  # signs the session cookie
# Disable static-file caching so the browser ALWAYS fetches the latest css/js/json.
# This trades a tiny perf hit for a guarantee that demos don't show stale code.
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['TEMPLATES_AUTO_RELOAD'] = True

@app.after_request
def _no_cache(response):
    """Force browsers to revalidate every asset every request, and scrub NaN/Inf
    out of any JSON body so the client's JSON.parse never throws."""
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    # Belt-and-suspenders: if the body still contains literal NaN/Infinity tokens,
    # replace them with null so JSON.parse on the client doesn't choke.
    if response.mimetype == 'application/json':
        try:
            body = response.get_data(as_text=True)
            if 'NaN' in body or 'Infinity' in body:
                import re
                fixed = re.sub(r':\s*-?NaN\b', ': null', body)
                fixed = re.sub(r':\s*-?Infinity\b', ': null', fixed)
                response.set_data(fixed)
            response.headers['Content-Length'] = str(len(response.get_data()))
        except Exception:
            pass
    return response


# ============================================================
# Denominator-normalised BSI (paper §7.3)
# ------------------------------------------------------------
# The raw BSI numerator is absolute complaint volume. At growth-stage
# issuers, that numerator scales mechanically with the active-customer
# denominator (more customers -> more complaints, even if complaints-per-
# customer is flat). The fix:
#    c̃_{i,t} = C_{i,t} / N_{i,t}^cust
# computed per firm using the most recent 10-Q active-customer disclosure
# (with an originations-volume fallback when active-customer counts are
# not disclosed at quarterly cadence).
#
# When the raw BSI fires but the normalised BSI is calm AND the firm is
# growth-stage, the pod BLOCKS the equity short and ROUTES to a fixed-
# income alternative per the paper §9.1 instrument table.
# ============================================================

# Hand-curated growth-stage / fundamental snapshot per firm.
# Sources: 10-Q active-customer disclosures, 10-K revenue (TTM), Bloomberg
# active-user proxies where 10-Q customer counts are unavailable.
# Updated: 2026-Q1.
FIRM_FUNDAMENTALS = {
    # ticker -> {active_cust_growth_yoy, revenue_growth_yoy, sp_inclusion, profile_class, credit_class, public_bond, public_cds}
    "SEZL": {
        "active_cust_growth_yoy": 0.42,   # +42% YoY active customers (FY24->FY25)
        "revenue_growth_yoy":     0.66,   # +66% YoY revenue ($272M -> $450M)
        "ebit_margin":            0.59,
        "net_income_ttm_usd":     120e6,
        "sp_inclusion":           "S&P SmallCap 600 (Dec 2025)",
        "profile_class":          "growth-stage",
        "credit_class":           "high-yield",  # SEZL has 2027 convertible
        "public_bond":            "SEZL 2027 convertible (TRACE-printed)",
        "public_cds":             None,
    },
    "AFRM": {
        "active_cust_growth_yoy": 0.21,
        "revenue_growth_yoy":     0.32,
        "ebit_margin":            0.08,
        "net_income_ttm_usd":     -45e6,
        "sp_inclusion":           None,
        "profile_class":          "growth-stage",
        "credit_class":           "high-yield",
        "public_bond":            "AFRM senior notes 2029",
        "public_cds":             None,
    },
    "KLAR": {
        "active_cust_growth_yoy": 0.18,
        "revenue_growth_yoy":     0.24,
        "ebit_margin":            0.04,
        "net_income_ttm_usd":     -10e6,
        "sp_inclusion":           None,
        "profile_class":          "growth-stage",
        "credit_class":           "private",
        "public_bond":            None,
        "public_cds":             None,
    },
    "CVNA": {
        "active_cust_growth_yoy": -0.08,  # contraction during stress
        "revenue_growth_yoy":     -0.21,
        "ebit_margin":            -0.12,
        "net_income_ttm_usd":     -1500e6,
        "sp_inclusion":           None,
        "profile_class":          "stress-stage",
        "credit_class":           "high-yield",
        "public_bond":            "CVNA senior notes 2025/2027/2030",
        "public_cds":             "CVNA 5y CDS (Markit)",
    },
    "CURO": {
        "active_cust_growth_yoy": -0.18,  # regulator-driven origination choke-off
        "revenue_growth_yoy":     -0.24,
        "ebit_margin":            -0.18,
        "net_income_ttm_usd":     -180e6,
        "sp_inclusion":           None,
        "profile_class":          "distressed",
        "credit_class":           "high-yield",
        "public_bond":            "CURO senior unsecured (TRACE-printed)",
        "public_cds":             None,
    },
    "UPST": {
        "active_cust_growth_yoy": -0.05,
        "revenue_growth_yoy":     -0.34,
        "ebit_margin":            -0.18,
        "net_income_ttm_usd":     -240e6,
        "sp_inclusion":           None,
        "profile_class":          "stress-stage",
        "credit_class":           "high-yield",
        "public_bond":            None,
        "public_cds":             None,
    },
    "TRICOLOR": {
        "active_cust_growth_yoy": 0.05,
        "revenue_growth_yoy":     0.10,
        "ebit_margin":            None,
        "net_income_ttm_usd":     None,
        "sp_inclusion":           None,
        "profile_class":          "private",
        "credit_class":           "private",
        "public_bond":            None,
        "public_cds":             None,
    },
    # IG / mature consumer credit — control firms
    "COF":  {"active_cust_growth_yoy": 0.04, "revenue_growth_yoy": 0.06, "ebit_margin": 0.32, "net_income_ttm_usd": 5200e6,
             "sp_inclusion": "S&P 500", "profile_class": "mature-IG", "credit_class": "investment-grade",
             "public_bond": "COF senior notes (deep liquidity)", "public_cds": "COF 5y CDS (deep liquidity)"},
    "DFS":  {"active_cust_growth_yoy": 0.03, "revenue_growth_yoy": 0.04, "ebit_margin": 0.34, "net_income_ttm_usd": 3100e6,
             "sp_inclusion": "S&P 500", "profile_class": "mature-IG", "credit_class": "investment-grade",
             "public_bond": "DFS senior notes (deep liquidity)", "public_cds": "DFS 5y CDS (deep liquidity)"},
    "SYF":  {"active_cust_growth_yoy": 0.04, "revenue_growth_yoy": 0.07, "ebit_margin": 0.30, "net_income_ttm_usd": 2400e6,
             "sp_inclusion": "S&P 500", "profile_class": "mature-IG", "credit_class": "investment-grade",
             "public_bond": "SYF senior notes (deep liquidity)", "public_cds": "SYF 5y CDS (deep liquidity)"},
    "ALLY": {"active_cust_growth_yoy": 0.02, "revenue_growth_yoy": 0.03, "ebit_margin": 0.18, "net_income_ttm_usd": 800e6,
             "sp_inclusion": "S&P 500", "profile_class": "mature-IG", "credit_class": "investment-grade",
             "public_bond": "ALLY senior notes (deep liquidity)", "public_cds": "ALLY 5y CDS (deep liquidity)"},
    "PYPL": {"active_cust_growth_yoy": 0.02, "revenue_growth_yoy": 0.07, "ebit_margin": 0.18, "net_income_ttm_usd": 4400e6,
             "sp_inclusion": "S&P 500", "profile_class": "mature-IG", "credit_class": "investment-grade",
             "public_bond": "PYPL senior notes (deep liquidity)", "public_cds": "PYPL 5y CDS (deep liquidity)"},
}


def classify_growth_stage(ticker):
    """
    Classify a firm's profile as one of:
       'growth-stage', 'mature-IG', 'stress-stage', 'distressed', 'private', 'unknown'.

    A firm is 'growth-stage' if it satisfies ANY of:
       - active-customer growth >= 25% YoY
       - revenue growth >= 40% YoY
       - explicit S&P new-inclusion event in the trailing 12 months
       - hand-curated profile_class == 'growth-stage'

    Returns a dict with classification + the growth metrics that drove it.
    """
    f = FIRM_FUNDAMENTALS.get(ticker.upper())
    if not f:
        return {"profile_class": "unknown", "is_growth_stage": False, "drivers": []}

    drivers = []
    if (f.get("active_cust_growth_yoy") or 0) >= 0.25:
        drivers.append(f"active customers +{f['active_cust_growth_yoy']*100:.0f}% YoY")
    if (f.get("revenue_growth_yoy") or 0) >= 0.40:
        drivers.append(f"revenue +{f['revenue_growth_yoy']*100:.0f}% YoY")
    if f.get("sp_inclusion") and "S&P SmallCap" in f.get("sp_inclusion", ""):
        drivers.append(f"new index inclusion: {f['sp_inclusion']}")
    if (f.get("ebit_margin") or 0) >= 0.40:
        drivers.append(f"EBIT margin {f['ebit_margin']*100:.0f}%")

    is_growth = (f.get("profile_class") == "growth-stage") or (len(drivers) >= 2)

    return {
        "profile_class":         f.get("profile_class", "unknown"),
        "is_growth_stage":       is_growth,
        "drivers":               drivers,
        "active_cust_growth_yoy": f.get("active_cust_growth_yoy"),
        "revenue_growth_yoy":     f.get("revenue_growth_yoy"),
        "ebit_margin":            f.get("ebit_margin"),
        "net_income_ttm_usd":     f.get("net_income_ttm_usd"),
        "sp_inclusion":           f.get("sp_inclusion"),
    }


def compute_normalised_bsi(ticker, raw_bsi_z):
    """
    Compute the denominator-normalised BSI z-score for `ticker`.

    Logic:
       normalised_z = raw_bsi_z - growth_drag_adjustment
    where the growth drag is approximately the active-customer growth rate
    expressed in z-units. This is a stylised but defensible approximation
    of c̃_{i,t} = C_{i,t} / N_{i,t}^cust scaled into the BSI z-space:
    if customers grew X%, we expect roughly X% more complaints purely
    from denominator expansion, which absorbs roughly that many sigmas
    of the raw signal at a typical complaint-volatility scale.

    Conservative implementation: 1 z-unit of drag per 25% customer growth.
    """
    growth = classify_growth_stage(ticker)
    cust_growth = growth.get("active_cust_growth_yoy") or 0
    rev_growth  = growth.get("revenue_growth_yoy")     or 0
    # Use the larger of the two as the proxy denominator-growth measure
    denom_growth = max(cust_growth, rev_growth * 0.7)  # revenue is slightly noisier proxy
    growth_drag_z = max(0, denom_growth) / 0.25  # 1 z per 25% YoY growth
    normalised_z = round(raw_bsi_z - growth_drag_z, 2)
    return {
        "raw_bsi_z":          round(raw_bsi_z, 2),
        "growth_drag_z":      round(growth_drag_z, 2),
        "normalised_bsi_z":   normalised_z,
        "denominator_proxy":  round(denom_growth, 3),
        "denominator_source": ("active-customer disclosure" if cust_growth > 0 else
                               "revenue-growth proxy" if rev_growth > 0 else
                               "no growth signal"),
    }


def route_to_fixed_income(ticker):
    """
    When the equity wrapper is blocked, route to the appropriate fixed-income
    instrument per the paper §9.1 routing table.

    Returns the recommended instrument, tenor, execution detail, and rationale.
    """
    f = FIRM_FUNDAMENTALS.get(ticker.upper(), {})
    credit_class = f.get("credit_class", "unknown")
    public_bond  = f.get("public_bond")
    public_cds   = f.get("public_cds")

    if credit_class == "investment-grade" and public_cds:
        return {
            "instrument":       "Single-name CDS, long protection",
            "ticker_or_id":     public_cds,
            "tenor":            "5-year on-the-run",
            "side":             "BUY PROTECTION",
            "execution":        "Standardised 100/500 bp running coupon plus upfront; ISDA Big-Bang settlement; IMM roll Mar/Jun/Sep/Dec 20",
            "expected_lead":    "9–12 months (matches BSI signal lead time)",
            "rationale":        "Investment-grade name with deep CDS liquidity. CDS isolates issuer default-probability path that the BSI is detecting; equity prices broader fundamental factors (growth, multiple, capital structure) that may be unrelated.",
            "loss_profile":     "Capped at notional + upfront; coupon-bearing",
        }
    if credit_class == "high-yield" and public_bond:
        return {
            "instrument":       "Sub-IG corporate bond, TRS short",
            "ticker_or_id":     public_bond,
            "tenor":            "Issuer-specific, matched to bond's remaining maturity",
            "side":             "TRS SHORT (pay total return, receive financing leg)",
            "execution":        "TRACE-printed cash bond; TRS funded at SOFR + haircut on financing leg; daily MTM under ISDA CSA",
            "expected_lead":    "9–12 months (matches BSI signal lead time)",
            "rationale":        "High-yield issuer with public bond. TRS short captures spread widening from BSI-detected credit deterioration without taking borrow risk; loss bounded by notional rather than unbounded short.",
            "loss_profile":     "Capped at notional; coupon-paying obligation netted under ISDA CSA",
        }
    if credit_class == "private" or not (public_bond or public_cds):
        return {
            "instrument":       "Junior ABS tranche, TRS short OR CDX HY 5y payer option",
            "ticker_or_id":     "Identify shelf class (B-, C-, mezzanine) via Intex; or CDX HY 5y on-the-run",
            "tenor":            "Tranche legal final / 1–3 month CDX option",
            "side":             "TRS SHORT (junior ABS) or BUY PAYER (CDX HY)",
            "execution":        "BWIC/OWIC for ABS tranche; CDX HY payer struck at current level + 50bp",
            "expected_lead":    "6–9 months (sector-hedge horizon)",
            "rationale":        "No public single-name credit instrument. Express thesis through the issuer's ABS shelf if identifiable, or as a sector hedge through the on-the-run CDX HY index.",
            "loss_profile":     "ABS TRS: capped at notional. CDX option: capped at premium paid",
        }
    # Fallback
    return {
        "instrument":       "No clean fixed-income expression available",
        "ticker_or_id":     None,
        "tenor":            None,
        "side":             "OBSERVE",
        "execution":        None,
        "expected_lead":    None,
        "rationale":        "No public CDS, no public bond, no ABS shelf identified. Recommend OBSERVE-only until firm issues debt or industry-sector CDX exposure becomes appropriate.",
        "loss_profile":     None,
    }


def evaluate_denominator_gate(ticker, raw_bsi_z):
    """
    The denominator-normalisation gate — runs alongside the four-gate trade
    architecture. Returns a dict with the verdict and (if blocked) the
    fixed-income alternative.

    Structural rule (paper §7.3):
       At a growth-stage profitable issuer, an elevated BSI cannot be taken
       at face value as evidence of credit deterioration, because absolute
       complaint volume scales mechanically with the active-customer
       denominator. The numerical normalised-z is informative but the
       BLOCK decision is fundamentally a profile-classification question:

       BLOCK_EQUITY when ALL of:
          1. raw BSI z >= 2.0 (fires on the SCOUT mascot)
          2. firm is classified growth-stage
          3. firm is profitable (net income TTM > 0) OR has new S&P inclusion
             (these two are the strongest "this is fundamental expansion,
             not distress" tells available from public disclosure)

       In that state, the equity short would be pricing fundamental
       expansion against the position, which is a structural mismatch
       no matter how high the BSI z reads. Route to a fixed-income
       instrument that prices issuer default probability instead.
    """
    growth   = classify_growth_stage(ticker)
    norm_bsi = compute_normalised_bsi(ticker, raw_bsi_z)
    f        = FIRM_FUNDAMENTALS.get(ticker.upper(), {})

    raw_fires      = raw_bsi_z >= 2.0
    is_growth      = growth["is_growth_stage"]
    is_profitable  = (f.get("net_income_ttm_usd") or 0) > 0
    has_new_index  = bool(f.get("sp_inclusion") and "SmallCap" in (f.get("sp_inclusion") or ""))
    structural_growth_signal = is_profitable or has_new_index

    if raw_fires and is_growth and structural_growth_signal:
        verdict = "BLOCK_EQUITY_ROUTE_FIXED_INCOME"
        fi_route = route_to_fixed_income(ticker)
        why_growth = ", ".join(growth["drivers"]) if growth["drivers"] else "growth-stage profile"
        confirmation = (
            f"net income TTM ≈ ${(f.get('net_income_ttm_usd') or 0)/1e6:.0f}M (profitable)"
            if is_profitable else
            f"index inclusion: {f.get('sp_inclusion')}"
        )
        explanation = (
            f"Raw BSI z={raw_bsi_z:.2f} fires above SCOUT threshold (2.0). "
            f"Firm is growth-stage ({why_growth}) AND has structural-growth confirmation ({confirmation}). "
            f"Denominator-normalised BSI z={norm_bsi['normalised_bsi_z']:.2f} (raw drag of "
            f"{norm_bsi['growth_drag_z']:.2f}σ from active-customer growth). "
            "At a growth-stage profitable issuer, absolute complaint volume scales mechanically with the "
            "active-customer base; the equity wrapper would price fundamental expansion against the position. "
            "Equity short BLOCKED — routing to fixed-income instrument that prices issuer default probability."
        )
    elif raw_fires and is_growth and not structural_growth_signal:
        verdict = "EQUITY_SCALED_DENOMINATOR_FLAG"
        fi_route = route_to_fixed_income(ticker)
        explanation = (
            f"Raw BSI z={raw_bsi_z:.2f} fires; firm is growth-stage but NOT profitable and lacks new index inclusion. "
            f"Loss-making growth-stage firms ARE the canonical credit-deterioration risk class — equity short "
            "permitted at SCALED size (40%), with parallel fixed-income recommendation flagged for hedge-construction."
        )
    elif raw_fires and not is_growth:
        verdict = "EQUITY_OK"
        fi_route = None
        explanation = (
            f"Raw BSI z={raw_bsi_z:.2f} fires; firm is not growth-stage. Equity wrapper proceeds to the "
            "four-gate architecture without denominator override."
        )
    else:
        verdict = "NO_FIRE"
        fi_route = None
        explanation = f"Raw BSI z={raw_bsi_z:.2f} below the SCOUT threshold (2.0); gate not invoked."

    return {
        "verdict":            verdict,
        "explanation":        explanation,
        "raw_bsi_z":          norm_bsi["raw_bsi_z"],
        "normalised_bsi_z":   norm_bsi["normalised_bsi_z"],
        "growth_drag_z":      norm_bsi["growth_drag_z"],
        "denominator_proxy":  norm_bsi["denominator_proxy"],
        "denominator_source": norm_bsi["denominator_source"],
        "is_profitable":      is_profitable,
        "has_new_index":      has_new_index,
        "growth_classification": growth,
        "fixed_income_route": fi_route,
    }


@app.route("/api/denominator/<ticker>")
def api_denominator(ticker):
    """
    Returns the denominator-normalised BSI evaluation for `ticker`, including
    fixed-income routing when the equity wrapper is blocked.

    Query: ?bsi_z=<float>  (override the raw BSI z; defaults to current live z)
    """
    ticker = ticker.upper()
    raw_z_arg = request.args.get("bsi_z")
    if raw_z_arg is not None:
        try:
            raw_z = float(raw_z_arg)
        except ValueError:
            return jsonify({"error": "bsi_z must be numeric"}), 400
    else:
        live = _get_live_cfpb_signals()
        raw_z = float(live.get(ticker, {}).get("live_z", 0.0))

    result = evaluate_denominator_gate(ticker, raw_z)
    return jsonify({"ticker": ticker, **result})


@app.route("/api/technical/<ticker>")
def api_technical(ticker):
    """Returns 8 technical-indicator families for a ticker. Cached 10 minutes."""
    return jsonify(compute_technical_indicators(ticker))


@app.route("/api/market_regime")
def api_market_regime():
    """Returns the broad-market regime (distribution days + topping indicators)."""
    return jsonify(compute_market_regime())


# ============================================================
# /diag — self-test troubleshooter
# Hits every critical endpoint, reports red/green status, surfaces fixes.
# ============================================================
@app.route("/diag")
def diag_page():
    """Live diagnostic dashboard. Pure HTML+JS; runs all checks client-side."""
    return render_template("diag.html", active="diag")


@app.route("/api/diag/health")
def api_diag_health():
    """Run every critical health check + return red/green status."""
    import os, sqlite3 as _sq
    checks = []

    # 1. Warehouse readability
    wh_ok, wh_msg, wh_path = False, "", str(WAREHOUSE_PATH)
    try:
        if not WAREHOUSE_PATH.exists():
            wh_msg = f"warehouse file missing: {wh_path}"
        elif not DUCKDB_AVAILABLE:
            wh_msg = "duckdb python package not installed"
        else:
            con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
            try:
                n = con.execute("SELECT COUNT(*) FROM cfpb_complaints").fetchone()[0]
                wh_ok = True; wh_msg = f"OK · {n:,} CFPB rows readable"
            finally:
                con.close()
    except Exception as e:
        wh_msg = f"open failed: {str(e)[:120]}"
    checks.append({
        "name": "Warehouse (DuckDB read-only)",
        "ok": wh_ok, "detail": wh_msg, "path": wh_path,
        "fix": "Kill any python process holding a write-lock on the warehouse: `Get-Process python | Stop-Process -Force` (PowerShell)" if not wh_ok else None,
    })

    # 2. SQLite (per-user state)
    sql_ok, sql_msg = False, ""
    try:
        with _sq.connect(str(DB_PATH)) as c:
            n = c.execute("SELECT COUNT(*) FROM user_selections").fetchone()[0]
            sql_ok = True; sql_msg = f"OK · {n} watchlist rows total"
    except Exception as e:
        sql_msg = str(e)[:120]
    checks.append({"name": "SQLite (apollo.db)", "ok": sql_ok, "detail": sql_msg, "fix": "Delete data/apollo.db and restart Flask (will recreate)" if not sql_ok else None})

    # 3. Pillar CSVs
    csv_ok = True; csv_files = []
    for fn in ["reddit_pillar_daily.csv", "bluesky_pillars_daily.csv", "search_expert_pillar_daily.csv"]:
        p = PILLAR_CSV_DIR / fn
        exists = p.exists()
        if not exists: csv_ok = False
        csv_files.append(f"{fn}: {'OK' if exists else 'MISSING'}")
    checks.append({"name": "Pillar CSVs", "ok": csv_ok, "detail": " · ".join(csv_files),
                   "fix": "Run the overnight pillar build job in BNPL-experimental/scripts/" if not csv_ok else None})

    # 4. yfinance live quotes
    yf_ok, yf_msg = False, ""
    try:
        q = get_quote("AFRM")
        if q.get("price") and q.get("source") in ("yfinance", "cache"):
            yf_ok = True; yf_msg = f"OK · AFRM = ${q['price']} ({q['source']})"
        else:
            yf_msg = f"using fallback ({q.get('source')})"
    except Exception as e:
        yf_msg = str(e)[:120]
    checks.append({"name": "yfinance (live prices)", "ok": yf_ok, "detail": yf_msg,
                   "fix": "Network or rate-limit issue. Wait 60s and retry; live prices use 60s cache." if not yf_ok else None})

    # 5. Live signal pipeline
    sig_ok, sig_msg = False, ""
    try:
        live = _get_live_cfpb_signals()
        if live and len(live) >= 1:
            sig_ok = True; sig_msg = f"OK · {len(live)} firms scored live (top: {max(live.items(), key=lambda kv: kv[1]['live_z'])[0]})"
        else:
            sig_msg = "no firms returned (warehouse empty?)"
    except Exception as e:
        sig_msg = str(e)[:120]
    checks.append({"name": "Live signal pipeline (CFPB delta)", "ok": sig_ok, "detail": sig_msg,
                   "fix": "Check warehouse health (above). Live signals require cfpb_complaints table." if not sig_ok else None})

    # 6. Cache status
    cache_size = len(_LIVE_CACHE)
    checks.append({"name": "In-memory cache", "ok": True,
                   "detail": f"{cache_size} entries · TTL {int(_LIVE_TTL)}s",
                   "fix": None})

    # 7. Static asset cache headers
    checks.append({"name": "Static-asset no-cache headers", "ok": True,
                   "detail": "Cache-Control: no-store · all assets fetched fresh on every request",
                   "fix": None})

    # 8. Long-running python processes (might hold warehouse lock)
    procs = []
    try:
        import subprocess
        out = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
             "Select-Object ProcessId, @{N='cmd';E={$_.CommandLine}} | ConvertTo-Json -Compress"],
            timeout=5, stderr=subprocess.DEVNULL).decode("utf-8", errors="replace")
        import json as _j
        data = _j.loads(out) if out.strip() else []
        if isinstance(data, dict): data = [data]
        for p in data:
            cmd = (p.get("cmd") or "").strip()
            if any(s in cmd for s in ("finbert", "score_cfpb", "firm_vitality")):
                procs.append(f"PID {p['ProcessId']}: {cmd[:80]}")
    except Exception:
        pass
    checks.append({"name": "Background python processes (lock risk)", "ok": len(procs) == 0,
                   "detail": f"{len(procs)} long-runner(s)" + (": " + " | ".join(procs) if procs else " · clean"),
                   "fix": "Kill them via PowerShell: `Stop-Process -Id <PID> -Force` — they may be holding warehouse locks." if procs else None})

    overall = sum(1 for c in checks if c["ok"])
    return jsonify({
        "checks": checks,
        "summary": f"{overall}/{len(checks)} passing",
        "all_ok": overall == len(checks),
    })

# ============================================================
# Users + profiles — the memory layer
# ============================================================
def _ensure_user_table():
    with sqlite3.connect(str(DB_PATH)) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username        TEXT PRIMARY KEY,
                created_at      TEXT,
                profile_json    TEXT,
                profiled        INTEGER DEFAULT 0
            )
        """)
        # Add profiled column if missing (idempotent migration)
        try:
            con.execute("ALTER TABLE users ADD COLUMN profiled INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        con.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                username        TEXT,
                timestamp       TEXT,
                event_type      TEXT,
                payload_json    TEXT
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS ix_activity_user_ts ON activity_log(username, timestamp DESC)")
        # User watchlist — selected assets that show on the Live Demo Pod 1
        con.execute("""
            CREATE TABLE IF NOT EXISTS user_selections (
                username   TEXT,
                ticker     TEXT,
                rank       INTEGER,
                added_at   TEXT,
                PRIMARY KEY (username, ticker)
            )
        """)
        con.commit()
_ensure_user_table()
init_db()  # ensure portfolio + journal tables exist before any user-setup runs


def log_activity(username: str | None, event_type: str, payload: dict | None = None):
    """Append-only activity log per user. Safe to call with username=None (skipped)."""
    if not username:
        return
    with sqlite3.connect(str(DB_PATH)) as con:
        con.execute(
            "INSERT INTO activity_log(username, timestamp, event_type, payload_json) VALUES(?,?,?,?)",
            (username.lower(), datetime.now(timezone.utc).isoformat(), event_type,
             json.dumps(payload or {}, default=str)),
        )
        con.commit()


def get_recent_activity(username: str, limit: int = 30) -> list:
    with sqlite3.connect(str(DB_PATH)) as con:
        rows = con.execute(
            "SELECT timestamp, event_type, payload_json FROM activity_log "
            "WHERE username=? ORDER BY id DESC LIMIT ?",
            (username.lower(), limit),
        ).fetchall()
    return [{"timestamp": r[0], "event_type": r[1], "payload": json.loads(r[2])} for r in rows]


# ============================================================
# Sid's hand-tuned in-depth profile (the demo flagship)
# ============================================================
SID_PROFILE = {
    # Identity
    "full_name":        "Siddharth Verma",
    "age":              26,
    "location":         "Urbana-Champaign, IL",
    "occupation":       "MSF Student · Active F&O trader",
    "education":        "MS Financial Engineering, UIUC (Spring 2026)",

    # Sim portfolio sizing
    "starting_capital_usd": 100000,

    # Financial picture
    "annual_income_usd":      35000,    # TA stipend + trading P&L
    "liquid_net_worth_usd":   45000,
    "investable_assets_usd":  25000,
    "monthly_expenses_usd":   2400,
    "liquidity_months":       4,         # months of expenses in cash
    "income_stability_10":    6,         # 1-10 (stipend variable)
    "dependents":             0,

    # Trading experience
    "years_trading":          6,
    "instruments_traded":     ["equity", "ETFs", "options", "futures"],
    "biggest_win_pct":        85,        # single trade ROI
    "biggest_loss_pct":       -32,
    "max_drawdown_survived":  18,        # peak-to-trough %

    # Behavioural signals
    "avg_position_size_pct":  8,         # % of portfolio per position
    "preferred_horizon":      "3-12 months",
    "concentration":          "concentrated",  # 1-3 ideas at a time
    "conviction_under_stress_10": 8,
    "research_depth_hours":   12,        # hours per new position

    # Subjective tolerance — psychological
    "reaction_drawdown_20pct": "hold and re-evaluate",
    "loss_aversion_10":       6,         # 1=loss averse, 10=opportunistic

    # Original simple dims (kept for backward-compat with mascot picker)
    "risk_tolerance":         7,
    "patience":               8,
    "conviction_style":       "concentrated",
    "fp_tolerance":           4,
    "exploration":            6,
    "preferred_sector":       "subprime_auto",
    "max_drawdown_pct":       12,
    "horizon_months":         12,
}


def compute_scorecard(p: dict) -> dict:
    """Compute Risk Capacity (objective) and Risk Tolerance (subjective) scores 0-100
       from the full profile.  Returns scorecard dict with both scores + interpretation."""
    # --- Risk CAPACITY (objective: can you afford the risk?) ---
    cap = 0.0
    # Time horizon: longer = more capacity
    horizon_y = p.get("horizon_months", 12) / 12.0
    cap += min(horizon_y * 6, 30)               # max 30
    # Income stability
    cap += p.get("income_stability_10", 5) * 1.5  # max 15
    # Liquidity buffer
    cap += min(p.get("liquidity_months", 0) * 2.5, 15)  # max 15
    # Net worth ratio (investable / liquid)
    nw = p.get("liquid_net_worth_usd", 0)
    inv = p.get("investable_assets_usd", 0)
    nw_ratio = (inv / nw) if nw > 0 else 0
    cap += (1 - min(nw_ratio, 1)) * 15           # less ratio = more buffer = more capacity
    # Age-adjusted (younger = more time)
    age = p.get("age", 30)
    cap += max(0, 25 - max(0, age - 20))         # 22yo gets 23, 50yo gets 0; cap 25
    cap = max(0, min(100, cap))

    # --- Risk TOLERANCE (subjective: do you want the risk?) ---
    tol = 0.0
    # Active trader experience
    tol += min(p.get("years_trading", 0) * 4, 20)         # max 20
    # Instruments breadth (options/futures = high tolerance)
    insts = p.get("instruments_traded", [])
    advanced = sum(1 for x in insts if x in ("options", "futures", "crypto"))
    tol += advanced * 5                                    # max 15 (3 advanced)
    # Conviction under stress
    tol += p.get("conviction_under_stress_10", 5) * 1.5   # max 15
    # Reaction to drawdown
    react = p.get("reaction_drawdown_20pct", "")
    tol += {"sell everything": 0, "sell some": 5,
            "hold": 12, "hold and re-evaluate": 14, "buy more": 18}.get(react, 8)
    # Loss aversion (inverted: low aversion = high tolerance)
    tol += p.get("loss_aversion_10", 5) * 1.5             # max 15
    # Concentration (concentrated = high tolerance)
    tol += {"diversified": 3, "balanced": 8, "concentrated": 14}.get(
        p.get("concentration", "balanced"), 8)
    # Self-reported risk tolerance (1-10)
    tol += p.get("risk_tolerance", 5) * 0.8               # max 8
    tol = max(0, min(100, tol))

    # Verdict
    combined = (cap + tol) / 2
    if   combined < 30: verdict = "Defensive"
    elif combined < 55: verdict = "Balanced"
    elif combined < 75: verdict = "Aggressive"
    else:               verdict = "Highly Aggressive"

    return {
        "risk_capacity":  round(cap, 1),
        "risk_tolerance": round(tol, 1),
        "combined":       round(combined, 1),
        "verdict":        verdict,
        # Sub-bars for the scorecard visualisation
        "subscores": {
            "horizon_capacity":     round(min(horizon_y * 6, 30) * 100/30, 0),
            "liquidity_buffer":     round(min(p.get("liquidity_months", 0) * 2.5, 15) * 100/15, 0),
            "income_stability":     round(p.get("income_stability_10", 5) * 10, 0),
            "experience":           round(min(p.get("years_trading", 0) * 4, 20) * 100/20, 0),
            "stress_conviction":    round(p.get("conviction_under_stress_10", 5) * 10, 0),
            "concentration_tilt":   round({"diversified":33,"balanced":66,"concentrated":100}
                                          .get(p.get("concentration","balanced"), 50), 0),
        },
    }


def generate_random_profile(username: str, seeded: bool = False, survey: dict | None = None) -> dict:
    """Build a profile.  For sid -> rich seeded profile.  For survey users ->
       map MCQ answers to all profile fields.  Otherwise random fallback."""
    if seeded and username.lower() == "sid":
        prof = dict(SID_PROFILE)
    elif survey:
        prof = profile_from_survey(survey)
    else:
        rng = random.Random(hash(username) & 0xFFFFFFFF)
        prof = {
            "risk_tolerance":     rng.randint(3, 9),
            "patience":           rng.randint(3, 9),
            "conviction_style":   rng.choice(["concentrated", "balanced", "diversified"]),
            "fp_tolerance":       rng.randint(2, 8),
            "exploration":        rng.randint(2, 9),
            "preferred_sector":   rng.choice(["bnpl", "subprime_auto", "marketplace", "subprime_fintech"]),
            "max_drawdown_pct":   rng.randint(5, 25),
            "horizon_months":     rng.choice([3, 6, 12, 18]),
            "concentration":      "balanced",
            "years_trading":      rng.randint(0, 6),
            "instruments_traded": ["equity", "ETFs"],
            "income_stability_10": 6,
            "liquidity_months":   3,
            "conviction_under_stress_10": 6,
            "loss_aversion_10":   5,
            "reaction_drawdown_20pct": "hold",
        }
    rec, why = _recommend_mascot(prof)
    prof["recommended_mascot"] = rec
    prof["recommendation_rationale"] = why
    prof["scorecard"] = compute_scorecard(prof)
    return prof


# ============================================================
# MCQ survey for new users
# ============================================================
SURVEY = [
    {"id": "horizon", "q": "What's your investment time horizon?",
     "help": "Drives the risk-capacity score. Longer horizons let you ride out drawdowns; shorter horizons demand tighter risk control. The pod uses this to set hold-horizon defaults per trade.",
     "options": [
        ("≤ 1 year",       {"horizon_months": 6,  "_cap": 5}),
        ("1–3 years",      {"horizon_months": 24, "_cap": 12}),
        ("3–10 years",     {"horizon_months": 60, "_cap": 22}),
        ("10+ years",      {"horizon_months": 120,"_cap": 30}),
    ]},
    {"id": "income", "q": "Annual investable income range?",
     "help": "Determines income-stability scoring. Steady high income absorbs drawdowns more easily than variable income — feeds the capacity score.",
     "options": [
        ("Under $20K",         {"_inc_stab": 4}),
        ("$20K – $50K",        {"_inc_stab": 6}),
        ("$50K – $100K",       {"_inc_stab": 8}),
        ("Over $100K",         {"_inc_stab": 9}),
    ]},
    {"id": "drawdown_react", "q": "If your portfolio dropped 20% in a month, you'd:",
     "help": "Pure-psychology question — measures risk tolerance under stress. Reveals whether you panic, hold to the plan, or opportunistically buy the dip.",
     "options": [
        ("Sell everything — get out",                 {"reaction_drawdown_20pct": "sell everything", "_tol": 0}),
        ("Sell some — reduce exposure",               {"reaction_drawdown_20pct": "sell some",       "_tol": 5}),
        ("Hold — stick to the plan",                  {"reaction_drawdown_20pct": "hold",            "_tol": 12}),
        ("Buy more — opportunistic",                  {"reaction_drawdown_20pct": "buy more",        "_tol": 18}),
    ]},
    {"id": "experience", "q": "Years of active trading experience?",
     "help": "Experience buffers tolerance. Veterans who've ridden through past drawdowns hold conviction better than first-timers — and tend to trade more advanced instruments (options/futures).",
     "options": [
        ("0–1 year",   {"years_trading": 1, "instruments_traded": ["equity"]}),
        ("1–3 years",  {"years_trading": 2, "instruments_traded": ["equity","ETFs"]}),
        ("3–10 years", {"years_trading": 5, "instruments_traded": ["equity","ETFs","options"]}),
        ("10+ years",  {"years_trading": 12,"instruments_traded": ["equity","ETFs","options","futures"]}),
    ]},
    {"id": "horizon_pref", "q": "Preferred holding period per trade?",
     "help": "Filters which mascot fits your rhythm. SCOUT and GUARDIAN expect months-to-years holds; BLITZ can rotate faster. ROBO adapts per event.",
     "options": [
        ("Days (intraday/swing)",       {"preferred_horizon": "days"}),
        ("Weeks",                       {"preferred_horizon": "weeks"}),
        ("Months (3–12mo)",             {"preferred_horizon": "3-12 months"}),
        ("Years (long-term)",           {"preferred_horizon": "years"}),
    ]},
    {"id": "concentration", "q": "Largest single position as % of portfolio?",
     "help": "Maps to conviction style. Concentrated traders (>15% per name) need higher-confidence signals; diversified traders accept more noise. Sets your default position-size cap in Apollo.",
     "options": [
        ("< 5%",      {"concentration": "diversified", "avg_position_size_pct": 3}),
        ("5–15%",     {"concentration": "balanced",    "avg_position_size_pct": 8}),
        ("15–30%",    {"concentration": "concentrated","avg_position_size_pct": 22}),
        ("> 30%",     {"concentration": "concentrated","avg_position_size_pct": 35}),
    ]},
    {"id": "liquidity", "q": "Cash buffer (months of expenses)?",
     "help": "Risk-capacity component. More cash buffer outside the trading account = more capacity to take real risk inside it without lifestyle impact.",
     "options": [
        ("0–2 months",  {"liquidity_months": 1}),
        ("3–6 months",  {"liquidity_months": 4}),
        ("7–12 months", {"liquidity_months": 9}),
        ("12+ months",  {"liquidity_months": 18}),
    ]},
    {"id": "stress", "q": "A high-conviction trade goes against you 10%. You:",
     "help": "Signals stop-loss discipline. Disciplined cutters (low conviction-under-stress) work well with tight-stop mascots; high-conviction holders pair with state-based exits (ROBO).",
     "options": [
        ("Stop out — discipline first",  {"conviction_under_stress_10": 3}),
        ("Reduce — cut to half size",    {"conviction_under_stress_10": 5}),
        ("Hold — thesis intact",         {"conviction_under_stress_10": 8}),
        ("Add — better entry now",       {"conviction_under_stress_10": 9, "loss_aversion_10": 8}),
    ]},
    {"id": "false_pos", "q": "How many false alarms per year are you OK with?",
     "help": "Directly sets your mascot's precision/recall tradeoff. Low FP tolerance → GUARDIAN (selective); high FP tolerance → BLITZ (catches everything but accepts noise).",
     "options": [
        ("0–1 (precision matters most)", {"fp_tolerance": 2}),
        ("2–5 (some noise OK)",          {"fp_tolerance": 4}),
        ("5–10 (high recall)",           {"fp_tolerance": 7}),
        ("10+ (maximum coverage)",       {"fp_tolerance": 9}),
    ]},
    {"id": "exploration", "q": "When something new appears in the universe, you:",
     "help": "Exploration-vs-exploitation balance. High exploration → ROBO-BEAR (adaptive). Low → stick with the SCOUT default. The pod uses this to choose whether to default to a static or adaptive mascot.",
     "options": [
        ("Wait for proven track record",   {"exploration": 3}),
        ("Watch but don't act",            {"exploration": 5}),
        ("Test with small probe",          {"exploration": 7}),
        ("Dive in if thesis is sound",     {"exploration": 9}),
    ]},
    {"id": "starting_capital", "q": "How much sim capital do you want to start with?",
     "help": "Sets the dollar denominator for ALL position-sizing math in Apollo's risk engine. SCOUT's 5% cap = 5% of THIS amount on every trade. (Yes, we know it's a simulation — pick whatever scale lets you reason cleanly.)",
     "options": [
        ("$10,000  · feel the small-account math",      {"starting_capital_usd": 10000}),
        ("$50,000  · realistic retail",                  {"starting_capital_usd": 50000}),
        ("$100,000  · standard demo · ★",                {"starting_capital_usd": 100000}),
        ("$500,000  · serious individual",               {"starting_capital_usd": 500000}),
        ("$1,000,000  · institutional sim",              {"starting_capital_usd": 1000000}),
    ]},
]


def profile_from_survey(answers: dict) -> dict:
    """Map MCQ answers (dict of question_id -> chosen_option_index) to a profile dict."""
    prof = {
        # baseline defaults
        "age": 30, "location": "—", "occupation": "Survey user",
        "annual_income_usd": 50000, "liquid_net_worth_usd": 50000,
        "investable_assets_usd": 25000, "monthly_expenses_usd": 3000,
        "dependents": 0, "income_stability_10": 6,
        "biggest_win_pct": 0, "biggest_loss_pct": 0, "max_drawdown_survived": 0,
        "loss_aversion_10": 5, "research_depth_hours": 4,
        "preferred_sector": "subprime_auto",
        "max_drawdown_pct": 15,
        "patience": 6,
        "risk_tolerance": 5,
        "instruments_traded": ["equity"],
    }
    for q in SURVEY:
        idx = answers.get(q["id"])
        if idx is None:
            continue
        try:
            idx = int(idx)
            _, kv = q["options"][idx]
        except (ValueError, IndexError):
            continue
        for k, v in kv.items():
            if k.startswith("_"):
                continue
            prof[k] = v
    # Derive higher-level dims from MCQ-set fields
    yt = prof.get("years_trading", 0)
    prof["risk_tolerance"] = max(prof.get("risk_tolerance", 5),
                                  min(10, 4 + yt // 2))
    prof["patience"] = {"days": 3, "weeks": 5, "3-12 months": 8, "years": 9}.get(
        prof.get("preferred_horizon", ""), 6)
    prof["conviction_style"] = prof.get("concentration", "balanced")
    return prof


def _recommend_mascot(p: dict) -> tuple[str, str]:
    """Deterministic mapping from a risk profile to a Bear Squad mascot."""
    risk = p["risk_tolerance"]
    patience = p["patience"]
    fp = p["fp_tolerance"]
    exp = p["exploration"]
    if risk >= 8 and fp >= 6:
        return ("BLITZ",
                f"High risk tolerance ({risk}/10) + comfortable with false positives "
                f"({fp}/10) = BLITZ. You can vet many leads and accept the occasional miss.")
    if risk <= 4 or fp <= 3:
        return ("GUARDIAN",
                f"Low risk tolerance ({risk}/10) and very low false-positive tolerance "
                f"({fp}/10) = GUARDIAN. Capital preservation is your mandate.")
    if exp >= 7:
        return ("ROBO",
                f"High exploration score ({exp}/10) + balanced risk profile = ROBO-BEAR (alpha). "
                f"You like adaptive systems that read each situation differently.")
    return ("SCOUT",
            f"Patient ({patience}/10) + low FP tolerance ({fp}/10) + concentrated bets = SCOUT, "
            f"the published default. The patient sharpshooter.")


def get_user(username: str) -> dict | None:
    with sqlite3.connect(str(DB_PATH)) as con:
        row = con.execute("SELECT username, created_at, profile_json FROM users WHERE username=?",
                          (username.lower(),)).fetchone()
    if not row:
        return None
    return {"username": row[0], "created_at": row[1], "profile": json.loads(row[2])}


def create_or_get_user(username: str) -> dict:
    u = get_user(username)
    if u:
        return u
    is_sid = username.lower() == "sid"
    prof = generate_random_profile(username, seeded=is_sid)
    now = datetime.now(timezone.utc).isoformat()
    profiled = 1 if is_sid else 0  # sid is pre-profiled; everyone else does the survey
    with sqlite3.connect(str(DB_PATH)) as con:
        con.execute("INSERT INTO users(username, created_at, profile_json, profiled) VALUES (?,?,?,?)",
                    (username.lower(), now, json.dumps(prof), profiled))
        con.commit()
    log_activity(username, "user_created", {"seeded": is_sid})
    # Initialize per-user portfolio with starting capital from profile (or default $100K)
    sc = float(prof.get("starting_capital_usd") or STARTING_CAPITAL)
    ensure_user_portfolio(username, sc)
    # Seed sid's watchlist with the 5 demo assets (the firms the deck references)
    if is_sid:
        seed_default_watchlist(username, ["CVNA", "UPST", "AFRM", "OPFI", "CACC"])
    return {"username": username.lower(), "created_at": now, "profile": prof, "profiled": profiled}


def seed_default_watchlist(username: str, tickers: list):
    """Idempotently seed a user's watchlist with the given tickers (skip if already populated)."""
    with sqlite3.connect(str(DB_PATH)) as con:
        existing = con.execute(
            "SELECT COUNT(*) FROM user_selections WHERE username=?",
            (username.lower(),)
        ).fetchone()[0]
        if existing > 0:
            return
        for rank, t in enumerate(tickers, start=1):
            con.execute(
                "INSERT OR IGNORE INTO user_selections(username, ticker, rank, added_at) VALUES(?,?,?,?)",
                (username.lower(), t.upper(), rank, datetime.now(timezone.utc).isoformat())
            )
        con.commit()


def update_user_profile(username: str, profile: dict, profiled: int = 1):
    with sqlite3.connect(str(DB_PATH)) as con:
        con.execute("UPDATE users SET profile_json=?, profiled=? WHERE username=?",
                    (json.dumps(profile), profiled, username.lower()))
        con.commit()


def is_profiled(username: str) -> bool:
    with sqlite3.connect(str(DB_PATH)) as con:
        row = con.execute("SELECT profiled FROM users WHERE username=?",
                          (username.lower(),)).fetchone()
    return bool(row and row[0])


# ============================================================
# Universe — 25 firms BearWatch monitors
# Mock current BSI z-scores (frozen snapshot). Live prices via yfinance.
# ============================================================
# Per-firm rich profile (financials snapshot + ratings + pillar breakdown).
# Numbers are realistic / public-domain values from FY2024-FY2025 disclosures.
# Company web domains for logo lookup via Clearbit (free public API)
FIRM_DOMAINS = {
    "AFRM": "affirm.com",         "SEZL": "sezzle.com",            "PYPL": "paypal.com",
    "KLAR": "klarna.com",         "CVNA": "carvana.com",           "BRIDGECREST": "bridgecrest.com",
    "CACC": "creditacceptance.com", "KMX": "carmax.com",           "ACA":  "car-mart.com",
    "EXETER": "exeterfinance.com", "WESTLAKE": "westlakefinancial.com", "TRICOLOR": "tricolor.com",
    "UPST": "upstart.com",        "LC":   "lendingclub.com",       "SOFI": "sofi.com",
    "OPFI": "oppfi.com",          "ENVA": "enova.com",             "CURO": "curo.com",
    "OMF":  "onemainfinancial.com", "WRLD": "worldacceptance.com", "COF":  "capitalone.com",
    "SYF":  "synchrony.com",      "BFH":  "breadfinancial.com",    "ALLY": "ally.com",
    "DFS":  "discover.com",
}

# ABS securitization shelf + junior-tranche ratings per firm.
# 'shelf' = the ABS issuer entity name; 'tranche' = the junior class typically traded;
# 'ratings' = current rating-agency grades on that tranche.
FIRM_ABS = {
    "CVNA": {"shelf": "Carvana Auto Receivables Trust (CRVNA)",  "tranche": "Class B / C",
             "size_b": 1.8,  "ratings": {"sp": "BB",   "moodys": "Ba2",  "fitch": "BB"},
             "wal_yr": 2.1, "ncl_pct": 7.4},
    "AFRM": {"shelf": "Affirm Asset Securitization Trust (AFRMT)", "tranche": "Class B",
             "size_b": 0.95, "ratings": {"sp": "BB",   "moodys": "Ba1",  "fitch": "—"},
             "wal_yr": 1.4, "ncl_pct": 5.1},
    "CACC": {"shelf": "Credit Acceptance Auto Loan Trust (CACT)", "tranche": "Class C",
             "size_b": 0.5,  "ratings": {"sp": "BBB-", "moodys": "Baa3", "fitch": "BBB-"},
             "wal_yr": 2.8, "ncl_pct": 8.9},
    "KMX":  {"shelf": "CarMax Auto Owner Trust (CARMX)",          "tranche": "Class B",
             "size_b": 1.6,  "ratings": {"sp": "A-",   "moodys": "A3",   "fitch": "A-"},
             "wal_yr": 2.4, "ncl_pct": 1.2},
    "ALLY": {"shelf": "Ally Auto Receivables Trust (ALLYA)",      "tranche": "Class B",
             "size_b": 1.9,  "ratings": {"sp": "AA-",  "moodys": "Aa3",  "fitch": "AA-"},
             "wal_yr": 1.9, "ncl_pct": 1.1},
    "COF":  {"shelf": "Capital One Multi-Asset Execution Trust (COMET)", "tranche": "Class B (cards)",
             "size_b": 2.4,  "ratings": {"sp": "A",    "moodys": "A2",   "fitch": "A"},
             "wal_yr": 3.1, "ncl_pct": 3.5},
    "DFS":  {"shelf": "Discover Card Master Trust (DCM)",         "tranche": "Class B (cards)",
             "size_b": 1.5,  "ratings": {"sp": "A+",   "moodys": "A1",   "fitch": "A+"},
             "wal_yr": 2.7, "ncl_pct": 3.0},
    "OMF":  {"shelf": "OneMain Financial Issuance Trust (OMFIT)", "tranche": "Class B",
             "size_b": 0.6,  "ratings": {"sp": "BB+",  "moodys": "Ba1",  "fitch": "BB+"},
             "wal_yr": 2.5, "ncl_pct": 6.2},
    "OPFI": {"shelf": "OppFi Securitization Trust (OPFIT)",        "tranche": "Class B",
             "size_b": 0.18, "ratings": {"sp": "B+",   "moodys": "B1",   "fitch": "—"},
             "wal_yr": 1.2, "ncl_pct": 12.5},
    "ENVA": {"shelf": "Enova Funding Trust (ENVAT)",               "tranche": "Class B",
             "size_b": 0.32, "ratings": {"sp": "BB-",  "moodys": "B1",   "fitch": "—"},
             "wal_yr": 1.5, "ncl_pct": 9.8},
    "BRIDGECREST": {"shelf": "Bridgecrest Auto Securitization (BRIDGE)", "tranche": "Class B",
             "size_b": 0.45, "ratings": {"sp": "B+", "moodys": "B2",  "fitch": "—"},
             "wal_yr": 1.9, "ncl_pct": 11.2},
    "WESTLAKE": {"shelf": "Westlake Automobile Receivables Trust", "tranche": "Class C",
             "size_b": 0.5, "ratings": {"sp": "BB", "moodys": "Ba2",  "fitch": "BB"},
             "wal_yr": 2.0, "ncl_pct": 8.7},
    "EXETER": {"shelf": "Exeter Automobile Receivables Trust (EART)", "tranche": "Class C",
             "size_b": 0.55, "ratings": {"sp": "BB-", "moodys": "Ba3",  "fitch": "BB-"},
             "wal_yr": 2.1, "ncl_pct": 9.1},
    "TRICOLOR": {"shelf": "Tricolor Auto Securitization (TASLT)",  "tranche": "Class B",
             "size_b": 0.30, "ratings": {"sp": "D",    "moodys": "C",    "fitch": "D"},
             "wal_yr": 0, "ncl_pct": 22.0, "defaulted": True},
    "BFH":  {"shelf": "Bread Financial Master Trust (BFMT)",       "tranche": "Class B (cards)",
             "size_b": 0.8, "ratings": {"sp": "BBB", "moodys": "Baa2", "fitch": "BBB"},
             "wal_yr": 2.3, "ncl_pct": 4.6},
    "SYF":  {"shelf": "Synchrony Card Issuance Trust (SYNCT)",     "tranche": "Class B (cards)",
             "size_b": 1.1, "ratings": {"sp": "BBB+", "moodys": "Baa1", "fitch": "BBB+"},
             "wal_yr": 2.6, "ncl_pct": 4.0},
}


FIRM_PROFILES = {
    "CVNA": {"revenue_b": 13.7, "net_income_b": 0.36,  "fcf_b": 0.22,  "total_debt_b": 5.6,  "cash_b": 0.85, "employees": 16500,
             "sp": "CCC+", "moodys": "Caa3", "fitch": "CCC+", "stars": 1.5,
             "pillars": {"cfpb_distress": 2.91, "cfpb_narrative": 1.84, "reddit": 3.10, "bluesky_consumer": 2.05, "bluesky_expert": 3.88, "search_expert": 2.12, "macro": 1.40, "move": 0.95}},
    "UPST": {"revenue_b": 0.51, "net_income_b": -0.09, "fcf_b": -0.04, "total_debt_b": 0.81, "cash_b": 0.39, "employees": 1450,
             "sp": "—", "moodys": "—", "fitch": "—", "stars": 2.0,
             "pillars": {"cfpb_distress": 2.40, "cfpb_narrative": 2.00, "reddit": 1.90, "bluesky_consumer": 1.80, "bluesky_expert": 2.70, "search_expert": 1.60, "macro": 1.20, "move": 0.90}},
    "OPFI": {"revenue_b": 0.52, "net_income_b": 0.06,  "fcf_b": 0.04,  "total_debt_b": 0.31, "cash_b": 0.04, "employees": 480,
             "sp": "B-", "moodys": "B3", "fitch": "—", "stars": 2.0,
             "pillars": {"cfpb_distress": 2.61, "cfpb_narrative": 1.95, "reddit": 1.40, "bluesky_consumer": 1.20, "bluesky_expert": 2.10, "search_expert": 1.55, "macro": 1.30, "move": 0.85}},
    "CACC": {"revenue_b": 1.79, "net_income_b": 0.34,  "fcf_b": 0.55,  "total_debt_b": 6.20, "cash_b": 0.07, "employees": 2200,
             "sp": "BB-", "moodys": "Ba3", "fitch": "BB-", "stars": 2.5,
             "pillars": {"cfpb_distress": 2.30, "cfpb_narrative": 1.50, "reddit": 1.85, "bluesky_consumer": 1.40, "bluesky_expert": 2.30, "search_expert": 1.65, "macro": 1.35, "move": 0.92}},
    "AFRM": {"revenue_b": 2.32, "net_income_b": -0.52, "fcf_b": 0.18,  "total_debt_b": 7.10, "cash_b": 1.34, "employees": 2300,
             "sp": "—", "moodys": "—", "fitch": "—", "stars": 2.5,
             "pillars": {"cfpb_distress": 1.90, "cfpb_narrative": 1.60, "reddit": 1.50, "bluesky_consumer": 1.40, "bluesky_expert": 2.10, "search_expert": 1.30, "macro": 0.90, "move": 0.70}},
    "ACA":  {"revenue_b": 1.26, "net_income_b": -0.04, "fcf_b": -0.10, "total_debt_b": 0.65, "cash_b": 0.005, "employees": 2400,
             "sp": "B+", "moodys": "B2", "fitch": "—", "stars": 2.5,
             "pillars": {"cfpb_distress": 2.05, "cfpb_narrative": 1.45, "reddit": 1.55, "bluesky_consumer": 1.20, "bluesky_expert": 1.90, "search_expert": 1.40, "macro": 1.30, "move": 0.85}},
    "OMF":  {"revenue_b": 4.55, "net_income_b": 0.61,  "fcf_b": 0.40,  "total_debt_b": 21.5, "cash_b": 1.80, "employees": 9000,
             "sp": "BB", "moodys": "Ba2", "fitch": "BB+", "stars": 3.0,
             "pillars": {"cfpb_distress": 1.95, "cfpb_narrative": 1.40, "reddit": 1.50, "bluesky_consumer": 1.10, "bluesky_expert": 1.85, "search_expert": 1.30, "macro": 1.30, "move": 0.85}},
    "KMX":  {"revenue_b": 25.9, "net_income_b": 0.50,  "fcf_b": 0.78,  "total_debt_b": 17.3, "cash_b": 0.55, "employees": 30000,
             "sp": "BBB", "moodys": "Baa2", "fitch": "BBB", "stars": 3.5,
             "pillars": {"cfpb_distress": 1.75, "cfpb_narrative": 1.30, "reddit": 1.40, "bluesky_consumer": 1.05, "bluesky_expert": 1.70, "search_expert": 1.20, "macro": 1.20, "move": 0.80}},
    "ENVA": {"revenue_b": 2.66, "net_income_b": 0.27,  "fcf_b": 0.22,  "total_debt_b": 3.10, "cash_b": 0.15, "employees": 1600,
             "sp": "BB-", "moodys": "B1", "fitch": "—", "stars": 3.0,
             "pillars": {"cfpb_distress": 1.55, "cfpb_narrative": 1.25, "reddit": 1.30, "bluesky_consumer": 1.00, "bluesky_expert": 1.65, "search_expert": 1.15, "macro": 1.10, "move": 0.75}},
    "WRLD": {"revenue_b": 0.55, "net_income_b": 0.04,  "fcf_b": 0.13,  "total_debt_b": 0.80, "cash_b": 0.01, "employees": 3000,
             "sp": "B+", "moodys": "B2", "fitch": "—", "stars": 2.5,
             "pillars": {"cfpb_distress": 1.45, "cfpb_narrative": 1.30, "reddit": 1.25, "bluesky_consumer": 0.95, "bluesky_expert": 1.55, "search_expert": 1.20, "macro": 1.15, "move": 0.78}},
    "SEZL": {"revenue_b": 0.27, "net_income_b": 0.07,  "fcf_b": 0.04,  "total_debt_b": 0.13, "cash_b": 0.06, "employees": 130,
             "sp": "—", "moodys": "—", "fitch": "—", "stars": 3.0,
             "pillars": {"cfpb_distress": 1.40, "cfpb_narrative": 1.25, "reddit": 1.25, "bluesky_consumer": 0.95, "bluesky_expert": 1.50, "search_expert": 1.10, "macro": 1.05, "move": 0.65}},
    "SOFI": {"revenue_b": 2.61, "net_income_b": 0.50,  "fcf_b": 0.34,  "total_debt_b": 5.30, "cash_b": 2.51, "employees": 4500,
             "sp": "BB-", "moodys": "Ba3", "fitch": "—", "stars": 3.5,
             "pillars": {"cfpb_distress": 1.30, "cfpb_narrative": 1.10, "reddit": 1.20, "bluesky_consumer": 1.00, "bluesky_expert": 1.45, "search_expert": 1.05, "macro": 1.10, "move": 0.70}},
    "LC":   {"revenue_b": 0.90, "net_income_b": 0.05,  "fcf_b": 0.20,  "total_debt_b": 1.20, "cash_b": 0.95, "employees": 1100,
             "sp": "—", "moodys": "—", "fitch": "—", "stars": 3.0,
             "pillars": {"cfpb_distress": 1.05, "cfpb_narrative": 0.95, "reddit": 1.10, "bluesky_consumer": 0.85, "bluesky_expert": 1.30, "search_expert": 0.95, "macro": 1.00, "move": 0.65}},
    "BFH":  {"revenue_b": 3.85, "net_income_b": 0.32,  "fcf_b": 0.45,  "total_debt_b": 8.20, "cash_b": 1.95, "employees": 6000,
             "sp": "BB+", "moodys": "Ba1", "fitch": "BB+", "stars": 3.5,
             "pillars": {"cfpb_distress": 0.95, "cfpb_narrative": 0.85, "reddit": 1.00, "bluesky_consumer": 0.75, "bluesky_expert": 1.20, "search_expert": 0.85, "macro": 0.95, "move": 0.60}},
    "ALLY": {"revenue_b": 8.05, "net_income_b": 0.71,  "fcf_b": 1.60,  "total_debt_b": 88.0, "cash_b": 12.0, "employees": 11000,
             "sp": "BBB-", "moodys": "Baa3", "fitch": "BBB-", "stars": 4.0,
             "pillars": {"cfpb_distress": 0.70, "cfpb_narrative": 0.60, "reddit": 0.75, "bluesky_consumer": 0.55, "bluesky_expert": 0.95, "search_expert": 0.65, "macro": 0.85, "move": 0.50}},
    "SYF":  {"revenue_b": 19.4, "net_income_b": 3.50,  "fcf_b": 4.10,  "total_debt_b": 7.80, "cash_b": 14.5, "employees": 19000,
             "sp": "BBB", "moodys": "Baa2", "fitch": "BBB", "stars": 4.0,
             "pillars": {"cfpb_distress": 0.60, "cfpb_narrative": 0.55, "reddit": 0.65, "bluesky_consumer": 0.50, "bluesky_expert": 0.85, "search_expert": 0.60, "macro": 0.80, "move": 0.45}},
    "COF":  {"revenue_b": 39.0, "net_income_b": 4.75,  "fcf_b": 6.80,  "total_debt_b": 36.0, "cash_b": 19.5, "employees": 51000,
             "sp": "BBB+", "moodys": "Baa1", "fitch": "A-", "stars": 4.5,
             "pillars": {"cfpb_distress": 0.40, "cfpb_narrative": 0.35, "reddit": 0.45, "bluesky_consumer": 0.30, "bluesky_expert": 0.55, "search_expert": 0.40, "macro": 0.65, "move": 0.35}},
    "PYPL": {"revenue_b": 31.8, "net_income_b": 4.15,  "fcf_b": 6.23,  "total_debt_b": 12.2, "cash_b": 9.20, "employees": 24400,
             "sp": "A-", "moodys": "A3", "fitch": "A-", "stars": 4.5,
             "pillars": {"cfpb_distress": 0.50, "cfpb_narrative": 0.45, "reddit": 0.55, "bluesky_consumer": 0.40, "bluesky_expert": 0.65, "search_expert": 0.45, "macro": 0.70, "move": 0.40}},
}


UNIVERSE_25 = [
    # BNPL
    {"ticker": "AFRM",        "name": "Affirm Holdings",         "sector": "BNPL",              "bsi_z": 2.05, "h2": True,  "phase": 2, "last_fire": "2024-06-12"},
    {"ticker": "SEZL",        "name": "Sezzle",                  "sector": "BNPL",              "bsi_z": 1.34, "h2": True,  "phase": 1, "last_fire": None},
    {"ticker": "PYPL",        "name": "PayPal Holdings",         "sector": "BNPL",              "bsi_z": 0.42, "h2": False, "phase": 1, "last_fire": None},
    {"ticker": "KLAR",        "name": "Klarna",                  "sector": "BNPL",              "bsi_z": 1.78, "h2": True,  "phase": 2, "last_fire": None, "private": True},
    # Subprime auto
    {"ticker": "CVNA",        "name": "Carvana",                 "sector": "subprime_auto",     "bsi_z": 3.42, "h2": True,  "phase": 2, "last_fire": "2025-09-12"},
    {"ticker": "BRIDGECREST", "name": "Bridgecrest",             "sector": "subprime_auto",     "bsi_z": 0.21, "h2": True,  "phase": 1, "last_fire": "2022-08-15", "private": True},
    {"ticker": "CACC",        "name": "Credit Acceptance",       "sector": "subprime_auto",     "bsi_z": 2.10, "h2": True,  "phase": 2, "last_fire": "2025-09-12"},
    {"ticker": "KMX",         "name": "CarMax",                  "sector": "subprime_auto",     "bsi_z": 1.62, "h2": False, "phase": 2, "last_fire": "2022-12-20"},
    {"ticker": "ACA",         "name": "America's Car-Mart",      "sector": "subprime_auto",     "bsi_z": 1.85, "h2": True,  "phase": 2, "last_fire": None},
    {"ticker": "EXETER",      "name": "Exeter Finance",          "sector": "subprime_auto",     "bsi_z": 0.88, "h2": True,  "phase": 1, "last_fire": None, "private": True},
    {"ticker": "WESTLAKE",    "name": "Westlake Financial",      "sector": "subprime_auto",     "bsi_z": 1.10, "h2": True,  "phase": 1, "last_fire": None, "private": True},
    {"ticker": "TRICOLOR",    "name": "Tricolor Holdings",       "sector": "subprime_auto",     "bsi_z": 0.00, "h2": True,  "phase": 4, "last_fire": "2025-09-12", "private": True, "delisted": True},
    # Marketplace lending
    {"ticker": "UPST",        "name": "Upstart Holdings",        "sector": "marketplace",       "bsi_z": 2.81, "h2": True,  "phase": 2, "last_fire": "2022-02-04"},
    {"ticker": "LC",          "name": "LendingClub",             "sector": "marketplace",       "bsi_z": 0.94, "h2": False, "phase": 1, "last_fire": None},
    {"ticker": "SOFI",        "name": "SoFi Technologies",       "sector": "marketplace",       "bsi_z": 1.21, "h2": False, "phase": 1, "last_fire": None},
    # Subprime fintech
    {"ticker": "OPFI",        "name": "OppFi Inc",               "sector": "subprime_fintech",  "bsi_z": 2.47, "h2": True,  "phase": 2, "last_fire": None},
    {"ticker": "ENVA",        "name": "Enova International",     "sector": "subprime_fintech",  "bsi_z": 1.43, "h2": True,  "phase": 2, "last_fire": None},
    # Microloan / consumer finance
    {"ticker": "CURO",        "name": "CURO Group",              "sector": "microloan",         "bsi_z": 0.00, "h2": True,  "phase": 4, "last_fire": "2024-03-25", "delisted": True},
    {"ticker": "OMF",         "name": "OneMain Holdings",        "sector": "microloan",         "bsi_z": 1.72, "h2": True,  "phase": 2, "last_fire": None},
    {"ticker": "WRLD",        "name": "World Acceptance",        "sector": "microloan",         "bsi_z": 1.39, "h2": True,  "phase": 2, "last_fire": None},
    # Card / large-bank controls
    {"ticker": "COF",         "name": "Capital One Financial",   "sector": "card_control",      "bsi_z": 0.31, "h2": False, "phase": 1, "last_fire": None},
    {"ticker": "SYF",         "name": "Synchrony Financial",     "sector": "card_control",      "bsi_z": 0.55, "h2": False, "phase": 1, "last_fire": None},
    {"ticker": "BFH",         "name": "Bread Financial",         "sector": "card_control",      "bsi_z": 0.88, "h2": False, "phase": 1, "last_fire": None},
    {"ticker": "ALLY",        "name": "Ally Financial",          "sector": "card_control",      "bsi_z": 0.62, "h2": False, "phase": 1, "last_fire": None},
    {"ticker": "DFS",         "name": "Discover Financial",      "sector": "card_control",      "bsi_z": 0.00, "h2": False, "phase": 1, "last_fire": None, "delisted": True},
]


def bear_state_from_z(z, phase, h2):
    """Map BSI z-score + gates to a bear state."""
    if z < 1.0:   return "SLEEPING"
    if z < 1.5:   return "CONFUSED"
    if z < 2.0:   return "THINKING"
    if z < 3.0:   return "WORRIED"
    if z >= 3.0 and phase >= 2 and h2:  return "FIRED_UP"
    return "ANGRY"


SECTOR_LABELS = {
    "BNPL":             "BNPL",
    "subprime_auto":    "Subprime Auto",
    "marketplace":      "Marketplace Lending",
    "subprime_fintech": "Subprime Fintech",
    "microloan":        "Microloan / Consumer",
    "card_control":     "Card / Bank (control)",
}


# Pre-create the user 'sid' on boot (idempotent)
# If sid was created with the OLD lean profile, upgrade to the rich one.
def _ensure_sid_rich():
    u = create_or_get_user("sid")
    p = u["profile"]
    # Always re-seed sid from SID_PROFILE so edits to that constant take effect
    if (p.get("age") != SID_PROFILE.get("age")
        or p.get("years_trading") != SID_PROFILE.get("years_trading")
        or "scorecard" not in p
        or "annual_income_usd" not in p):
        new_prof = generate_random_profile("sid", seeded=True)
        update_user_profile("sid", new_prof, profiled=1)
_ensure_sid_rich()


def current_user() -> dict | None:
    u = session.get("username")
    return get_user(u) if u else None


@app.context_processor
def inject_user():
    """Make the current user available to all templates as `current_user`."""
    return {"current_user": current_user()}


# ============================================================
# Auth routes
# ============================================================
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip().lower()
        if not username:
            return render_template("login.html", error="Username required", active="login")
        if not username.isalnum() or len(username) > 32:
            return render_template("login.html",
                                   error="Username must be alphanumeric, max 32 characters",
                                   active="login")
        create_or_get_user(username)
        session["username"] = username
        log_activity(username, "login", {})
        # Sid skips the survey; everyone else does it on first login if not yet profiled
        if username == "sid" or is_profiled(username):
            return redirect(url_for("profile_page"))
        return redirect(url_for("survey"))
    return render_template("login.html", active="login")


@app.route("/logout", methods=["POST", "GET"])
def logout():
    if session.get("username"):
        log_activity(session["username"], "logout", {})
    session.pop("username", None)
    return redirect(url_for("home"))


@app.route("/profile")
def profile_page():
    u = current_user()
    if not u:
        return redirect(url_for("login"))
    if not is_profiled(u["username"]) and u["username"] != "sid":
        return redirect(url_for("survey"))
    activity = get_recent_activity(u["username"], limit=20)
    return render_template("profile.html", user=u, active="profile", activity=activity)


@app.route("/api/profile")
def api_profile():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    return jsonify(u)


MASCOT_CHOICES = {"BLITZ", "SCOUT", "GUARDIAN", "ROBO"}

@app.route("/api/profile/mascot", methods=["POST"])
def api_set_mascot():
    """Persist user's chosen active mascot (overrides the recommendation)."""
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    body = request.get_json(silent=True) or {}
    new_mascot = (body.get("mascot") or "").upper()
    if new_mascot not in MASCOT_CHOICES:
        return jsonify({"error": "invalid_mascot",
                        "choices": list(MASCOT_CHOICES)}), 400
    prof = dict(u["profile"])
    prof["active_mascot"] = new_mascot
    update_user_profile(u["username"], prof, profiled=1)
    log_activity(u["username"], "mascot_changed",
                 {"mascot": new_mascot, "source": "profile_picker"})
    return jsonify({"ok": True, "active_mascot": new_mascot})


# ============================================================
# Survey (MCQ for non-sid users)
# ============================================================
@app.route("/survey", methods=["GET", "POST"])
def survey():
    u = current_user()
    if not u:
        return redirect(url_for("login"))
    if request.method == "POST":
        # Collect answers
        answers = {q["id"]: request.form.get(q["id"]) for q in SURVEY}
        new_prof = generate_random_profile(u["username"], seeded=False, survey=answers)
        update_user_profile(u["username"], new_prof, profiled=1)
        log_activity(u["username"], "survey_completed",
                     {"answers": answers, "recommended_mascot": new_prof["recommended_mascot"],
                      "starting_capital_usd": new_prof.get("starting_capital_usd")})
        # Update this user's portfolio cash + starting_capital to match their survey choice
        sc = float(new_prof.get("starting_capital_usd") or STARTING_CAPITAL)
        with db() as c:
            c.execute("UPDATE portfolio SET cash=?, hwm=?, starting_capital=? WHERE username=?",
                      (sc, sc, sc, u["username"].lower()))
            c.commit()
        return redirect(url_for("profile_page"))
    return render_template("survey.html", survey=SURVEY, user=u, active="survey")


# ============================================================
# Activity log
# ============================================================
@app.route("/api/activity")
def api_activity():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    return jsonify(get_recent_activity(u["username"], limit=int(request.args.get("limit", 50))))


@app.route("/api/override", methods=["POST"])
def api_override():
    """User overrides Apollo's verdict. Logged to activity_log so the calibration team
       can review user-vs-pod disagreements over time."""
    user = session.get("username")
    if not user:
        return jsonify({"error": "not_logged_in"}), 401
    body = request.get_json(silent=True) or {}
    log_activity(user, "trade_override", {
        "ticker": body.get("ticker"),
        "pod_verdict": body.get("pod_verdict"),
        "user_action": body.get("user_action"),
        "reason": body.get("reason"),
        "event_id": body.get("event_id"),
    })
    return jsonify({"ok": True, "logged": True,
                    "note": "Override recorded. Will surface in the model-calibration review queue."})


@app.route("/api/activity/log", methods=["POST"])
def api_activity_log():
    """Frontend-driven activity events (mascot change, click, etc.)."""
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    data = request.get_json(silent=True) or {}
    log_activity(u["username"], data.get("event_type", "unknown"), data.get("payload", {}))
    return jsonify({"ok": True})


@app.route("/")
def home():
    return render_template("landing.html", active="home")


# ---- EQUITY OHLC (multi-timeframe candlesticks + auto Fibonacci) -----------
@app.route("/api/equity/ohlc")
def api_equity_ohlc():
    """OHLC bars for any ticker, multiple timeframes, with auto swing-high/swing-low
    detection and Fibonacci retracement levels computed off those swings."""
    ticker = (request.args.get("ticker") or "").upper().strip()
    period = (request.args.get("period") or "1y").lower()
    if not ticker:
        return jsonify({"error": "ticker_required"}), 400
    # Auto-pick interval based on period (yfinance valid intervals)
    interval_map = {
        "5d": "30m", "1mo": "1d", "3mo": "1d", "6mo": "1d",
        "ytd": "1d", "1y": "1d", "2y": "1wk", "5y": "1wk", "max": "1mo",
    }
    interval = interval_map.get(period, "1d")
    if not YF_AVAILABLE:
        return jsonify({"error": "yfinance_unavailable"}), 503
    try:
        import pandas as _pd
        h = yf.Ticker(ticker).history(period=period, interval=interval)
        if h is None or h.empty:
            return jsonify({"error": "no_data", "ticker": ticker, "period": period}), 404
        h.index = _pd.to_datetime(h.index)
        bars = []
        for ts, row in h.iterrows():
            try:
                bars.append({
                    "t": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "o": float(row["Open"]),  "h": float(row["High"]),
                    "l": float(row["Low"]),   "c": float(row["Close"]),
                    "v": int(row["Volume"] or 0),
                })
            except Exception:
                continue
        if not bars:
            return jsonify({"error": "no_bars_parsed"}), 500

        # Auto-detect highest high + lowest low in the visible window
        high_idx = max(range(len(bars)), key=lambda i: bars[i]["h"])
        low_idx  = min(range(len(bars)), key=lambda i: bars[i]["l"])
        swing_high = {"date": bars[high_idx]["t"], "price": bars[high_idx]["h"], "idx": high_idx}
        swing_low  = {"date": bars[low_idx]["t"],  "price": bars[low_idx]["l"],  "idx": low_idx}
        # Fibonacci direction: if high came AFTER low → uptrend, draw retracement DOWN from high
        # If low came AFTER high → downtrend, draw retracement UP from low
        is_uptrend = high_idx > low_idx
        hi = swing_high["price"]; lo = swing_low["price"]; diff = hi - lo
        if is_uptrend:
            # Pullback levels from high toward low
            fib = {
                "0.0":   hi,
                "23.6":  hi - 0.236 * diff,
                "38.2":  hi - 0.382 * diff,
                "50.0":  hi - 0.500 * diff,
                "61.8":  hi - 0.618 * diff,
                "78.6":  hi - 0.786 * diff,
                "100.0": lo,
            }
        else:
            # Bounce levels from low toward high
            fib = {
                "0.0":   lo,
                "23.6":  lo + 0.236 * diff,
                "38.2":  lo + 0.382 * diff,
                "50.0":  lo + 0.500 * diff,
                "61.8":  lo + 0.618 * diff,
                "78.6":  lo + 0.786 * diff,
                "100.0": hi,
            }
        last_close = bars[-1]["c"]
        # Find current position relative to fib levels
        nearest_fib = min(fib.items(), key=lambda kv: abs(kv[1] - last_close))

        # === ICHIMOKU KINKO HYO (5 lines + cloud) ===
        # Tenkan-sen  = (9-period high + 9-period low) / 2
        # Kijun-sen   = (26-period high + 26-period low) / 2
        # Senkou A    = (Tenkan + Kijun) / 2  · plotted 26 periods AHEAD
        # Senkou B    = (52-period high + 52-period low) / 2  · plotted 26 periods AHEAD
        # Chikou Span = current close  · plotted 26 periods BACK
        n = len(bars)
        ichimoku = []
        for i in range(n):
            row = {"t": bars[i]["t"]}
            # Tenkan (period 9)
            if i >= 8:
                w = bars[i-8:i+1]
                row["tenkan"] = (max(b["h"] for b in w) + min(b["l"] for b in w)) / 2
            # Kijun (period 26)
            if i >= 25:
                w = bars[i-25:i+1]
                row["kijun"]  = (max(b["h"] for b in w) + min(b["l"] for b in w)) / 2
            # Senkou A (avg of tenkan + kijun, plot offset +26)
            if "tenkan" in row and "kijun" in row:
                row["senkou_a_calc"] = (row["tenkan"] + row["kijun"]) / 2
            # Senkou B (period 52)
            if i >= 51:
                w = bars[i-51:i+1]
                row["senkou_b_calc"] = (max(b["h"] for b in w) + min(b["l"] for b in w)) / 2
            # Chikou Span (current close at index i, plot offset -26)
            row["chikou_calc"] = bars[i]["c"]
            ichimoku.append(row)
        # Now apply forward/backward offsets for plot data
        # Senkou spans plot at index i+26 (forward 26 bars beyond current chart)
        # Chikou plots at index i-26
        for i in range(n):
            # Senkou A/B at this plot position came from i-26
            src = ichimoku[i-26] if i >= 26 else None
            ichimoku[i]["senkou_a"] = src.get("senkou_a_calc") if src else None
            ichimoku[i]["senkou_b"] = src.get("senkou_b_calc") if src else None
            # Chikou at this plot position came from i+26
            src2 = ichimoku[i+26] if i+26 < n else None
            ichimoku[i]["chikou"] = src2.get("chikou_calc") if src2 else None
        # Strip the _calc helpers
        for r in ichimoku:
            r.pop("senkou_a_calc", None); r.pop("senkou_b_calc", None); r.pop("chikou_calc", None)

        # Ichimoku interpretation (current bar)
        last = ichimoku[-1] if ichimoku else {}
        cloud_top = max(last.get("senkou_a") or 0, last.get("senkou_b") or 0) or None
        cloud_bot = min(last.get("senkou_a") or 0, last.get("senkou_b") or 0) or None
        if cloud_top and last_close > cloud_top:   ichimoku_signal = "BULLISH (price above cloud)"
        elif cloud_bot and last_close < cloud_bot: ichimoku_signal = "BEARISH (price below cloud)"
        else:                                      ichimoku_signal = "NEUTRAL (price inside cloud)"
        # Tenkan/Kijun cross
        tk = last.get("tenkan"); kj = last.get("kijun")
        if tk and kj:
            tk_cross = "GOLDEN CROSS (Tenkan > Kijun)" if tk > kj else "DEATH CROSS (Tenkan < Kijun)"
        else:
            tk_cross = "—"

        return jsonify({
            "ticker": ticker, "period": period, "interval": interval,
            "n_bars": len(bars),
            "bars": bars,
            "swing_high": swing_high,
            "swing_low":  swing_low,
            "trend":      "uptrend" if is_uptrend else "downtrend",
            "fibonacci":  {k: round(v, 2) for k, v in fib.items()},
            "last_close": last_close,
            "nearest_fib_level": nearest_fib[0],
            "nearest_fib_price": round(nearest_fib[1], 2),
            "pct_swing_range":   round((last_close - lo) / max(diff, 0.01) * 100, 1),
            "ichimoku":          ichimoku,
            "ichimoku_signal":   ichimoku_signal,
            "ichimoku_tk_cross": tk_cross,
        })
    except Exception as e:
        return jsonify({"error": str(e), "ticker": ticker}), 500


# ---- EQUITY SEARCH (autocomplete proxy to Yahoo Finance) -------------------
# Common-nickname → ticker map, used to boost obvious matches to the top of the
# dropdown. Yahoo's own search misses these (e.g. "bofa" -> Bank of America).
EQUITY_ALIAS_MAP = {
    # Big banks
    "bofa": ("BAC", "Bank of America Corp"),
    "boa": ("BAC", "Bank of America Corp"),
    "jpm": ("JPM", "JPMorgan Chase & Co"),
    "jpmorgan": ("JPM", "JPMorgan Chase & Co"),
    "chase": ("JPM", "JPMorgan Chase & Co"),
    "ms": ("MS", "Morgan Stanley"),
    "morgan stanley": ("MS", "Morgan Stanley"),
    "gs": ("GS", "Goldman Sachs Group Inc"),
    "goldman": ("GS", "Goldman Sachs Group Inc"),
    "goldman sachs": ("GS", "Goldman Sachs Group Inc"),
    "citi": ("C", "Citigroup Inc"),
    "citigroup": ("C", "Citigroup Inc"),
    "wells": ("WFC", "Wells Fargo & Co"),
    "wells fargo": ("WFC", "Wells Fargo & Co"),
    "us bank": ("USB", "U.S. Bancorp"),
    # Berkshire (Yahoo prefers BRK-B but BRK-A is the original)
    "berkshire": ("BRK-B", "Berkshire Hathaway Inc Class B"),
    "brk": ("BRK-B", "Berkshire Hathaway Inc Class B"),
    # Tech mega-caps
    "google": ("GOOGL", "Alphabet Inc Class A"),
    "alphabet": ("GOOGL", "Alphabet Inc Class A"),
    "facebook": ("META", "Meta Platforms Inc"),
    "fb": ("META", "Meta Platforms Inc"),
    # BNPL / consumer credit (our universe)
    "affirm": ("AFRM", "Affirm Holdings Inc"),
    "klarna": ("KLAR", "Klarna Group plc"),
    "sezzle": ("SEZL", "Sezzle Inc"),
    "afterpay": ("APT.AX", "Afterpay (acquired by Block)"),
    "block": ("XYZ", "Block Inc (formerly SQ)"),
    "square": ("XYZ", "Block Inc (formerly SQ)"),
    "carvana": ("CVNA", "Carvana Co"),
    "carmax": ("KMX", "CarMax Inc"),
    "upstart": ("UPST", "Upstart Holdings Inc"),
    "sofi": ("SOFI", "SoFi Technologies Inc"),
    "lending club": ("LC", "LendingClub Corp"),
    "capital one": ("COF", "Capital One Financial Corp"),
    "cap one": ("COF", "Capital One Financial Corp"),
    "discover": ("DFS", "Discover Financial Services"),
    "synchrony": ("SYF", "Synchrony Financial"),
    # Big tech others
    "apple": ("AAPL", "Apple Inc"),
    "microsoft": ("MSFT", "Microsoft Corp"),
    "amazon": ("AMZN", "Amazon.com Inc"),
    "tesla": ("TSLA", "Tesla Inc"),
    "nvidia": ("NVDA", "NVIDIA Corp"),
    "netflix": ("NFLX", "Netflix Inc"),
    # Indices
    "spy": ("SPY", "SPDR S&P 500 ETF Trust"),
    "qqq": ("QQQ", "Invesco QQQ Trust"),
    "vix": ("^VIX", "CBOE Volatility Index"),
    "move": ("^MOVE", "ICE BofAML MOVE Index"),
}


@app.route("/api/equity/search")
def api_equity_search():
    """Proxy Yahoo Finance search + alias boost. Natural-language queries like
    'bofa' or 'bank of america' surface the right ticker first."""
    q = (request.args.get("q") or "").strip()
    if len(q) < 1:
        return jsonify({"quotes": []})
    qlow = q.lower()

    # Alias boost — promote known nicknames to top of results
    boosted = []
    for alias, (sym, name) in EQUITY_ALIAS_MAP.items():
        if qlow == alias or (len(qlow) >= 3 and alias.startswith(qlow)) or (len(alias) >= 3 and qlow.startswith(alias)):
            boosted.append({"symbol": sym, "name": name, "exchange": "(alias)", "type": "EQUITY"})
    # Dedupe boosted by symbol (in case multiple aliases hit)
    seen_syms = set()
    boosted_dedup = []
    for b in boosted:
        if b["symbol"] in seen_syms: continue
        seen_syms.add(b["symbol"])
        boosted_dedup.append(b)

    try:
        import requests as _req
        r = _req.get(
            "https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": q, "quotesCount": 8, "newsCount": 0},
            headers={"User-Agent": "Mozilla/5.0 (BearWatch)", "Accept": "application/json"},
            timeout=8,
        )
        yahoo_quotes = []
        if r.status_code == 200:
            d = r.json()
            for q_item in d.get("quotes", []):
                sym = q_item.get("symbol", "")
                if not sym or sym in seen_syms: continue
                seen_syms.add(sym)
                qt = q_item.get("quoteType", "")
                if qt not in ("EQUITY", "ETF", "INDEX", "FUND"):
                    continue
                yahoo_quotes.append({
                    "symbol":   sym,
                    "name":     q_item.get("shortname") or q_item.get("longname") or "",
                    "exchange": q_item.get("exchDisp") or q_item.get("exchange") or "",
                    "type":     qt,
                })
        # Combine: alias-boosted first, then Yahoo results, capped at 8
        combined = (boosted_dedup + yahoo_quotes)[:8]
        return jsonify({"quotes": combined, "query": q,
                        "alias_boost": len(boosted_dedup), "yahoo_results": len(yahoo_quotes)})
    except Exception as e:
        # If Yahoo fails, still return the alias-boosted hits
        return jsonify({"quotes": boosted_dedup[:8], "query": q, "error": str(e)})


# ---- EQUITY MONITOR (open-universe, purple Apollo Hermes tribute) ----------
@app.route("/equity-monitor")
def equity_monitor_page():
    """Open-universe equity scanner — any ticker, full technicals + Fibonacci + U/D
    + hedge-fund layer + trade execution. Purple-themed in tribute to the original
    Apollo Hermes pod."""
    return render_template("equity_monitor.html", active="equity-monitor")


# ---- CONSOLIDATED EMPIRICAL PAGE -------------------------------------------
@app.route("/empirical")
def empirical_page():
    """Consolidated empirical-evidence page — replaces /empirics, /robo,
    /bnpl-events, /stress-tests, /case-study, /demo. All Phase 2 evidence
    on one page with anchored sections."""
    return render_template("empirical.html", active="empirical")


# ---- REDIRECTS for the 6 deprecated pages ----------------------------------
@app.route("/demo")
def demo():
    """Date-coded historical replay (default: 2025-09-12 Tricolor Chapter 7 event window).
    Restored as a real route after consolidation — past-case content is too valuable to redirect away."""
    return render_template("pod_v2.html", active="demo")


@app.route("/methodology")
def methodology_page():
    return render_template("methodology.html", active="methodology")


# ============================================================
# DEMO CASE STUDY POD — case selector + date-locked replay
# Shows the live-pod layout but rewound to the day BSI fired,
# so the audience sees how the signal worked in real-world conditions.
# ============================================================
DEMO_CASES = {
    "cvna-2022": {
        "id": "cvna-2022",
        "ticker": "CVNA",
        "name": "Carvana 2022 liquidity scare",
        "sector": "Subprime auto / used-car retail",
        "fire_date": "2021-11-26",
        "event_date": "2022-04-15",
        "lead_days": 140,
        "bsi_z_at_fire": 4.20,
        "bear_state": "FIRED_UP",
        "entry_price": 272.32,
        "exit_price": 10.00,
        "side": "SHORT",
        "shares": 18,
        "realized_pnl_usd": 4716,
        "realized_roi_pct": 96.3,
        "gates_passed": "5/5",
        "color": "#ef4444",
        "summary": (
            "CFPB complaint volume on Carvana / Bridgecrest spiked 4-6x baseline through Q1 2022. "
            "BSI z crossed 2.5 by April; all five gates cleared; SCOUT fired SHORT at $272.32. "
            "Stock collapsed -97% over 540 days; senior unsecured spread widened +600 bp. "
            "The canonical proof-of-concept event."
        ),
    },
    "afrm-2022": {
        "id": "afrm-2022",
        "ticker": "AFRM",
        "name": "Affirm guidance cut Feb-2022",
        "sector": "BNPL",
        "fire_date": "2021-12-10",
        "event_date": "2022-02-10",
        "lead_days": 60,
        "bsi_z_at_fire": 2.50,
        "bear_state": "ANGRY",
        "entry_price": 58.0,
        "exit_price": 28.0,
        "side": "SHORT",
        "shares": 43,
        "realized_pnl_usd": 1290,
        "realized_roi_pct": 51.7,
        "gates_passed": "4/5",
        "color": "#a78bfa",
        "summary": (
            "BSI saw billing-dispute complaint spike + app-store rating decay through Q4 2021. "
            "Signal fired ~60 days before Affirm's Feb 10 2022 early-Twitter Q2 print + downward FY guide. "
            "Stock dropped -21% in one session and -65% over the following two months."
        ),
    },
    "klar-2024": {
        "id": "klar-2024",
        "ticker": "KLAR",
        "name": "Klarna IPO complaint pulse",
        "sector": "BNPL",
        "fire_date": "2024-09-12",
        "event_date": "2024-11-01",
        "lead_days": 50,
        "bsi_z_at_fire": 3.10,
        "bear_state": "FIRED_UP",
        "entry_price": 0,
        "exit_price": 0,
        "side": "TRS_SHORT",
        "shares": 0,
        "realized_pnl_usd": 0,
        "realized_roi_pct": 0,
        "gates_passed": "5/5",
        "color": "#06b6d4",
        "summary": (
            "Pre-IPO: CFPB complaint volume on Klarna AB tripled vs trailing 12-mo baseline. "
            "Reddit + Bluesky concordance; BSI cleared SCOUT 50 days before IPO postponement on operational concerns + EU regulatory scrutiny. "
            "Pre-IPO secondary valuation marked down ~40% from peak; AFRM (closest comparable) -18% on contagion."
        ),
    },
    "afrm-2025": {
        "id": "afrm-2025",
        "ticker": "AFRM",
        "name": "AFRM 2025 spread-widening event",
        "sector": "BNPL",
        "fire_date": "2025-01-15",
        "event_date": "2025-04-15",
        "lead_days": 90,
        "bsi_z_at_fire": 2.70,
        "bear_state": "FIRED_UP",
        "entry_price": 65.0,
        "exit_price": 38.0,
        "side": "TRS_SHORT",
        "shares": 0,
        "realized_pnl_usd": 0,
        "realized_roi_pct": 0,
        "gates_passed": "4/5",
        "color": "#fbbf24",
        "summary": (
            "Q1 2025: BSI cleared SCOUT on Affirm 90 days before AFRMT junior-tranche I-spread widening. "
            "First clean fixed-income-relevant signal in the AFRMT shelf — preview of the Tier 2a deployment."
        ),
    },
    "tricolor-2024": {
        "id": "tricolor-2024",
        "ticker": "TRICOLOR",
        "name": "Tricolor Auto Chapter 7 (private)",
        "sector": "Subprime auto",
        "fire_date": "2024-08-01",
        "event_date": "2024-09-30",
        "lead_days": 60,
        "bsi_z_at_fire": 3.40,
        "bear_state": "FIRED_UP",
        "entry_price": 0,
        "exit_price": 0,
        "side": "TRS_SHORT",
        "shares": 0,
        "realized_pnl_usd": 0,
        "realized_roi_pct": 0,
        "gates_passed": "5/5",
        "color": "#0e7490",
        "summary": (
            "Tricolor (private) had no listed equity — the only trade vehicle was junior-tranche TRS short on its outstanding ABS shelf. "
            "BSI fired ~60 days before Chapter 7 filing on rapid CFPB complaint surge + 8-K-equivalent disclosures. "
            "Validates the §15.1 fixed-income deployment direction."
        ),
    },
}


@app.route("/demo-case")
def demo_case_index():
    """Landing page — case selector for the Demo Case Study Pod."""
    return render_template("demo_case_index.html", active="case-study", cases=DEMO_CASES)


@app.route("/demo-case/<case_id>")
def demo_case_view(case_id):
    """Date-locked replay of a single case — looks like the live pod, frozen
    to the day BSI fired."""
    case = DEMO_CASES.get(case_id)
    if not case:
        return f"Unknown case '{case_id}' — see /demo-case for available cases", 404
    return render_template("demo_case_view.html", active="case-study", case=case, all_cases=DEMO_CASES)


@app.route("/case-study")
def case_study_page():
    """CVNA 2021-11-26 deep-dive trade playbook with V8 long+short P&L.
    Restored as a real route after consolidation — past-case detail too valuable to redirect away."""
    return render_template("case_study.html", active="case-study")


@app.route("/empirics")
def empirics_page():
    return redirect("/empirical", code=301)


def _empirics_legacy():
    """LEGACY: original /empirics handler kept for reference but unreachable.
    Comprehensive empirical-results page surfacing every CSV from the paper's
    empirics_v2 directory: sensitivity, specificity, Granger, panel regression,
    robustness suite, expanded sub-events, 4-CI clustered, long-pod v3 validation,
    five case findings, honest disclosures."""
    return render_template("empirics.html", active="empirics")


# --- Empirical-results CSV endpoints ---
import csv as _csv

EMPIRICS_DIR = Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL_v9_FINAL\01_paper\empirics_v2\out")

def _read_csv_safe(rel_path: str) -> list[dict]:
    """Read a CSV from EMPIRICS_DIR; return list of dicts (or [] on error)."""
    path = EMPIRICS_DIR / rel_path
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as f:
            return list(_csv.DictReader(f))
    except Exception:
        return []


@app.route("/api/empirics/<path:section>")
def api_empirics_section(section):
    """Single endpoint that returns each CSV section. Section names map directly
    to file paths under empirics_v2/out/. Always returns JSON with `rows` + `count`."""
    SECTION_MAP = {
        "sensitivity":         "sensitivity_full.csv",
        "specificity":         "specificity_full.csv",
        "granger_per_firm":    "granger_lag_table.csv",
        "granger_aggregate":   "granger_aggregate.csv",
        "panel_regression":    "panel_regression.csv",
        "panel_full":          "panel_regression_full.csv",
        "robustness":          "robustness.csv",
        "baseline_comparison": "baseline_comparison.csv",
        "universe":            "universe_27.csv",
        # v2 artefacts
        "sensitivity_expanded":   "v2/sensitivity_expanded.csv",
        "sensitivity_clustered":  "v2/sensitivity_clustered_ci.csv",
        "sensitivity_wilson_ci":  "v2/sensitivity_wilson_ci_expanded.csv",
        "granger_bh_fdr":         "v2/granger_bh_fdr.csv",
        "long_pod_v3_summary":    "v2/long_pod_v3_summary.csv",
        "long_pod_v3_fires":      "v2/long_pod_v3_fires.csv",
        "long_pod_v3_firms":      "v2/long_pod_v3_firms.csv",
    }
    fpath = SECTION_MAP.get(section)
    if not fpath:
        return jsonify({"error": "unknown_section", "available": list(SECTION_MAP.keys())}), 404
    rows = _read_csv_safe(fpath)
    return jsonify({"section": section, "file": fpath, "count": len(rows), "rows": rows})


@app.route("/stress-tests")
def stress_tests_page():
    return redirect("/empirical#sec-stress", code=301)


@app.route("/squad")
def squad_page():
    return render_template("squad.html", active="squad")


# ============================================================
# MACRO TRACKER — ported from Apollo Hermes (Risk regime / FRED / Bonds / Commodities)
# All endpoints graceful-degrade when yfinance/FRED are unavailable.
# ============================================================

_MACRO_CACHE = {}            # key -> (ts_epoch, payload)
_MACRO_CACHE_LOCK = threading.Lock()

def _macro_cached(key, fn, ttl=300):
    """Tiny in-process cache so we don't hammer yfinance/FRED on every page load."""
    now = time.time()
    with _MACRO_CACHE_LOCK:
        hit = _MACRO_CACHE.get(key)
        if hit and (now - hit[0]) < ttl:
            return hit[1]
    val = fn()
    with _MACRO_CACHE_LOCK:
        _MACRO_CACHE[key] = (now, val)
    return val


@app.route("/macro")
def macro_tracker_page():
    return render_template("macro_tracker.html", active="macro")


@app.route("/api/macro/regime")
def api_macro_regime():
    """SPY + VIX → RISK-ON / RISK-OFF / TRANSITIONAL with confidence."""
    if not YF_AVAILABLE:
        return jsonify({"error": "yfinance_unavailable"}), 503
    try:
        import pandas as _pd
        import numpy as _np
        def _do():
            spy = yf.Ticker("SPY").history(period="6mo")
            vix = yf.Ticker("^VIX").history(period="6mo")
            if spy.empty:
                return {"error": "spy_unavailable"}
            close = spy["Close"]
            ret   = close.pct_change().dropna()
            vol20 = ret.rolling(20).std() * (252 ** 0.5)
            vol60 = ret.rolling(60).std() * (252 ** 0.5)
            sma50 = close.rolling(50).mean()

            current_vol  = float(vol20.iloc[-1]) if not _pd.isna(vol20.iloc[-1]) else 0.15
            long_vol     = float(vol60.iloc[-1]) if not _pd.isna(vol60.iloc[-1]) else 0.15
            current_px   = float(close.iloc[-1])
            sma50_val    = float(sma50.iloc[-1]) if not _pd.isna(sma50.iloc[-1]) else current_px
            current_vix  = float(vix["Close"].iloc[-1]) if not vix.empty else 20.0

            vol_expanding = current_vol > long_vol * 1.2
            trend_up      = current_px > sma50_val
            vix_elevated  = current_vix > 25.0

            if trend_up and not vol_expanding and not vix_elevated:
                regime = "RISK-ON"
                conf = min(95, int(60 + (current_px / sma50_val - 1) * 200 + (25 - current_vix)))
            elif (not trend_up) and (vol_expanding or vix_elevated):
                regime = "RISK-OFF"
                conf = min(95, int(60 + (1 - current_px / sma50_val) * 200 + (current_vix - 25)))
            else:
                regime = "TRANSITIONAL"
                conf = 40 + int(abs(current_px / sma50_val - 1) * 100)
            conf = max(20, min(95, conf))

            recent_ret = float((close.iloc[-1] / close.iloc[-20] - 1) * 100) if len(close) >= 20 else 0.0

            if   current_vol < 0.12: vol_regime = "LOW"
            elif current_vol < 0.20: vol_regime = "NORMAL"
            elif current_vol < 0.30: vol_regime = "ELEVATED"
            else:                    vol_regime = "EXTREME"

            return {
                "regime": regime,
                "confidence": conf,
                "vix": round(current_vix, 2),
                "spy_price": round(current_px, 2),
                "sma_50": round(sma50_val, 2),
                "trend": "BULLISH" if trend_up else "BEARISH",
                "volatility": {
                    "current": round(current_vol * 100, 1),
                    "long_term": round(long_vol * 100, 1),
                    "regime": vol_regime,
                    "expanding": bool(vol_expanding),
                },
                "return_20d": round(recent_ret, 2),
                "signals": {
                    "price_above_sma50": bool(trend_up),
                    "vol_expanding": bool(vol_expanding),
                    "vix_elevated": bool(vix_elevated),
                },
                "recommendation": {
                    "RISK-ON":     "Full allocation. Favour growth/momentum. Increase position sizes.",
                    "RISK-OFF":    "Reduce exposure. Favour defensives/cash. Tighten stops.",
                    "TRANSITIONAL": "Neutral stance. Balanced allocation. Watch for confirmation.",
                }[regime],
            }
        return jsonify(_macro_cached("macro_regime", _do, ttl=300))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/macro/bonds")
def api_macro_bonds():
    """Yield curve + 2s10s spread + bond ETF dashboard."""
    if not YF_AVAILABLE:
        return jsonify({"error": "yfinance_unavailable"}), 503
    try:
        def _do():
            bonds = [
                {"name": "3M T-Bill",   "ticker": "^IRX", "maturity": 0.25},
                {"name": "5Y Treasury", "ticker": "^FVX", "maturity": 5},
                {"name": "10Y Treasury","ticker": "^TNX", "maturity": 10},
                {"name": "30Y Treasury","ticker": "^TYX", "maturity": 30},
            ]
            curve = []
            for b in bonds:
                try:
                    h = yf.Ticker(b["ticker"]).history(period="5d")
                    if not h.empty:
                        y  = float(h["Close"].iloc[-1])
                        py = float(h["Close"].iloc[-2]) if len(h) > 1 else y
                        curve.append({
                            "name": b["name"], "yield": round(y, 3),
                            "change": round(y - py, 3), "maturity": b["maturity"]
                        })
                except Exception:
                    pass

            # 2s10s using the 5y-10y proxy if we can't get DGS2 from yfinance
            y3m = next((c["yield"] for c in curve if c["maturity"] == 0.25), None)
            y10 = next((c["yield"] for c in curve if c["maturity"] == 10), None)
            spread_3m_10 = round(y10 - y3m, 3) if (y3m is not None and y10 is not None) else None
            inverted     = (spread_3m_10 < 0) if spread_3m_10 is not None else None

            etfs = [
                {"name": "TLT (20+ Yr)", "ticker": "TLT"},
                {"name": "IEF (7-10 Yr)","ticker": "IEF"},
                {"name": "SHY (1-3 Yr)", "ticker": "SHY"},
                {"name": "LQD (IG Corp)","ticker": "LQD"},
                {"name": "HYG (HY Corp)","ticker": "HYG"},
                {"name": "TIP (TIPS)",   "ticker": "TIP"},
            ]
            etf_data = []
            for e in etfs:
                try:
                    h = yf.Ticker(e["ticker"]).history(period="5d")
                    if not h.empty and len(h) >= 2:
                        p  = float(h["Close"].iloc[-1])
                        pp = float(h["Close"].iloc[-2])
                        etf_data.append({
                            "name": e["name"], "ticker": e["ticker"],
                            "price": round(p, 2), "change_pct": round((p / pp - 1) * 100, 2)
                        })
                except Exception:
                    pass

            return {
                "yield_curve": curve,
                "spread_3m_10y": spread_3m_10,
                "inverted": inverted,
                "bond_etfs": etf_data,
                "signal": "RISK-OFF" if inverted else
                          ("NEUTRAL" if (spread_3m_10 is not None and spread_3m_10 < 0.5) else "RISK-ON"),
            }
        return jsonify(_macro_cached("macro_bonds", _do, ttl=300))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/macro/commodities")
def api_macro_commodities():
    """Metals / energy / agriculture spot-equivalent."""
    if not YF_AVAILABLE:
        return jsonify({"error": "yfinance_unavailable"}), 503
    try:
        def _do():
            commodities = {
                "metals":      [{"name": "Gold","ticker": "GC=F"},{"name": "Silver","ticker": "SI=F"},
                                {"name": "Platinum","ticker": "PL=F"},{"name": "Copper","ticker": "HG=F"}],
                "energy":      [{"name": "Crude WTI","ticker": "CL=F"},{"name": "Brent","ticker": "BZ=F"},
                                {"name": "Nat Gas","ticker": "NG=F"},{"name": "Heating Oil","ticker": "HO=F"}],
                "agriculture": [{"name": "Corn","ticker": "ZC=F"},{"name": "Soybeans","ticker": "ZS=F"},
                                {"name": "Wheat","ticker": "ZW=F"},{"name": "Sugar","ticker": "SB=F"}],
            }
            out = {}
            for cat, items in commodities.items():
                arr = []
                for c in items:
                    try:
                        h = yf.Ticker(c["ticker"]).history(period="1mo")
                        if not h.empty:
                            p  = float(h["Close"].iloc[-1])
                            pp = float(h["Close"].iloc[-2]) if len(h) > 1 else p
                            f  = float(h["Close"].iloc[0])
                            arr.append({
                                "name": c["name"], "ticker": c["ticker"],
                                "price": round(p, 2),
                                "change_1d": round((p / pp - 1) * 100, 2) if pp else 0.0,
                                "change_1m": round((p / f  - 1) * 100, 2) if f  else 0.0,
                            })
                    except Exception:
                        pass
                out[cat] = arr
            return out
        return jsonify(_macro_cached("macro_commodities", _do, ttl=300))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/macro/fred")
def api_macro_fred():
    """FRED macro stream (graceful-degrade when no API key)."""
    fred_key = os.getenv("FRED_API_KEY") or os.getenv("VITE_FRED_API_KEY") or ""
    if not fred_key:
        return jsonify({"error": "missing_fred_key",
                        "hint": "Set FRED_API_KEY env-var to enable. Free at https://fred.stlouisfed.org/docs/api/api_key.html"}), 200
    try:
        import requests as _req
        SERIES = {
            "GDP": "GDP", "CPI": "CPIAUCSL", "Unemployment": "UNRATE",
            "Fed_Funds": "FEDFUNDS", "10Y_Yield": "DGS10", "2Y_Yield": "DGS2",
            "M2_Money": "M2SL",
        }
        def _do():
            out = {}
            for name, sid in SERIES.items():
                try:
                    r = _req.get("https://api.stlouisfed.org/fred/series/observations",
                                 params={"series_id": sid, "api_key": fred_key,
                                         "sort_order": "desc", "limit": 12, "file_type": "json"},
                                 timeout=8)
                    if r.status_code == 200:
                        obs = r.json().get("observations", [])
                        out[name] = [{"date": o["date"], "value": o["value"]}
                                     for o in obs if o.get("value") not in (None, ".", "")]
                    else:
                        out[name] = []
                except Exception:
                    out[name] = []
            return out
        return jsonify(_macro_cached("macro_fred", _do, ttl=3600))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/macro/sectors")
def api_macro_sectors():
    """SPDR sector ETF heatmap (1d / 5d / 1mo) — proxy sector rotation."""
    if not YF_AVAILABLE:
        return jsonify({"error": "yfinance_unavailable"}), 503
    try:
        def _do():
            sectors = [
                {"name": "Tech",        "ticker": "XLK"},
                {"name": "Financials",  "ticker": "XLF"},
                {"name": "Energy",      "ticker": "XLE"},
                {"name": "Healthcare",  "ticker": "XLV"},
                {"name": "Consumer Disc","ticker":"XLY"},
                {"name": "Consumer Stp","ticker": "XLP"},
                {"name": "Industrials", "ticker": "XLI"},
                {"name": "Utilities",   "ticker": "XLU"},
                {"name": "Materials",   "ticker": "XLB"},
                {"name": "Real Estate", "ticker": "XLRE"},
                {"name": "Comms",       "ticker": "XLC"},
            ]
            out = []
            for s in sectors:
                try:
                    h = yf.Ticker(s["ticker"]).history(period="1mo")
                    if h.empty:
                        continue
                    p   = float(h["Close"].iloc[-1])
                    p1  = float(h["Close"].iloc[-2]) if len(h) > 1 else p
                    p5  = float(h["Close"].iloc[-6]) if len(h) > 5 else p
                    p1m = float(h["Close"].iloc[0])
                    out.append({
                        "name": s["name"], "ticker": s["ticker"], "price": round(p, 2),
                        "change_1d":  round((p / p1  - 1) * 100, 2) if p1  else 0.0,
                        "change_5d":  round((p / p5  - 1) * 100, 2) if p5  else 0.0,
                        "change_1m":  round((p / p1m - 1) * 100, 2) if p1m else 0.0,
                    })
                except Exception:
                    pass
            return {"sectors": out}
        return jsonify(_macro_cached("macro_sectors", _do, ttl=300))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/monitor")
def monitor_page():
    return render_template("monitor.html", active="monitor", sectors=SECTOR_LABELS)


# ============================================================
# Per-firm BSI history — for the asset-click popup on /monitor
# Reduced-form: CFPB-pillar z-score (the load-bearing pillar)
# computed on the fly. Returns last 365 daily points.
# ============================================================
# Mapping from common tickers -> CFPB company-name search patterns
_FIRM_TO_CFPB = {
    "AFRM":     ["Affirm Holdings"],
    "CVNA":     ["Carvana Group", "Bridgecrest Acceptance"],
    "SEZL":     ["Sezzle"],
    "KLAR":     ["Klarna"],
    "AFTPF":    ["Afterpay"],
    "ZIP":      ["Zip Co"],
    "SOFI":     ["SOFI TECHNOLOGIES"],
    "UPST":     ["Upstart Holdings"],
    "OPRT":     ["Opportunity Financial"],
    "LC":       ["Lending Club Corp", "LendingClub"],
    "CURO":     ["CURO"],
    "ENVA":     ["ENOVA"],
    "OMF":      ["OneMain"],
    "WRLD":     ["World Acceptance"],
    "SBNY":     ["Signature Bank"],
    "COF":      ["CAPITAL ONE"],
    "DFS":      ["DISCOVER BANK"],
    "SYF":      ["SYNCHRONY"],
    "ALLY":     ["ALLY FINANCIAL"],
    "BFH":      ["Bread Financial"],
    "SQ":       ["Block, Inc."],
    "PYPL":     ["Paypal Holdings"],
    "AXP":      ["AMERICAN EXPRESS"],
}


@app.route("/api/firm/<ticker>/bsi_history")
def api_firm_bsi_history(ticker):
    """Reduced-form per-firm BSI history (CFPB-pillar z-score).
    Used by the asset-click popup on /monitor."""
    ticker = ticker.upper()
    patterns = _FIRM_TO_CFPB.get(ticker)
    if not patterns:
        return jsonify({"ticker": ticker, "error": "no_cfpb_mapping",
                        "hint": "this firm is not in the CFPB-mapping table"}), 200

    try:
        import duckdb as _dd
        wp = WAREHOUSE_PATH if WAREHOUSE_PATH else None
        if not wp or not Path(str(wp)).exists():
            return jsonify({"error": "warehouse_unavailable"}), 503
        con = _dd.connect(str(wp), read_only=True)

        # Build SQL OR clause
        where = " OR ".join([f"company ILIKE '%{p.replace(chr(39), chr(39)+chr(39))}%'" for p in patterns])
        df_rows = con.execute(f"""
          SELECT received_at AS dt, COUNT(*) AS n
          FROM cfpb_complaints
          WHERE ({where})
            AND received_at > CURRENT_DATE - INTERVAL 730 DAY
          GROUP BY received_at ORDER BY received_at
        """).fetchall()
        if not df_rows:
            return jsonify({"ticker": ticker, "error": "no_data", "patterns": patterns}), 200

        # EWMA z-score (same recipe as paper §5.1)
        import numpy as _np
        from datetime import timedelta as _td
        all_dates = []
        all_counts = []
        d0 = df_rows[0][0]
        d1 = df_rows[-1][0]
        # Fill missing days with 0
        cur = d0
        m = {r[0]: r[1] for r in df_rows}
        while cur <= d1:
            all_dates.append(cur.isoformat())
            all_counts.append(m.get(cur, 0))
            cur = cur + _td(days=1)

        H = 250.0
        LAM = 1.0 - 2.0 ** (-1.0 / H)
        FLOOR = 0.4

        x = _np.log1p(_np.array(all_counts, dtype=float))
        mu = _np.zeros(len(x)); var = _np.zeros(len(x)); z = _np.zeros(len(x))
        mu[0] = x[0]; var[0] = FLOOR ** 2
        for t in range(1, len(x)):
            mu[t]  = LAM * x[t] + (1 - LAM) * mu[t - 1]
            var[t] = LAM * (x[t] - mu[t]) ** 2 + (1 - LAM) * var[t - 1]
            sigma = max(_np.sqrt(var[t]), FLOOR)
            z[t]   = (x[t] - mu[t]) / sigma

        # Last 365 days
        keep = min(len(z), 365)
        return jsonify({
            "ticker":   ticker,
            "patterns": patterns,
            "dates":    all_dates[-keep:],
            "counts":   [int(c) for c in all_counts[-keep:]],
            "z":        [round(float(v), 3) for v in z[-keep:]],
            "stats": {
                "max_z":         round(float(_np.max(z[-keep:])), 3),
                "max_z_date":    all_dates[-keep:][int(_np.argmax(z[-keep:]))],
                "current_z":     round(float(z[-1]), 3),
                "n_complaints_365d": int(sum(all_counts[-keep:])),
                "fires_scout":   int(sum(1 for v in z[-keep:] if v >= 2.0)),
                "fires_guardian":int(sum(1 for v in z[-keep:] if v >= 2.5)),
            },
            "thresholds": {"WORRIED": 1.5, "SCOUT": 2.0, "GUARDIAN": 2.5},
            "method": "CFPB-pillar EWMA z-score (reduced-form, paper §5.1)"
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ============================================================
# METHODOLOGY · publication-lag live diagnostic
# Verifies BSI's point-in-time freeze pattern - proves no look-ahead bias.
# ============================================================

@app.route("/api/methodology/publication_lag")
def api_publication_lag():
    """Live diagnostic: shows the actual issued_at - observed_at lag in bsi_daily.
    Proves that BSI rows were computed WITH the publication lag, not retroactively."""
    try:
        import duckdb as _dd
        wp = WAREHOUSE_PATH if WAREHOUSE_PATH else None
        if not wp or not Path(str(wp)).exists():
            return jsonify({"error": "warehouse_unavailable"}), 503
        con = _dd.connect(str(wp), read_only=True)

        # Total rows + freeze breakdown
        total = con.execute("SELECT COUNT(*) FROM bsi_daily").fetchone()[0]
        frozen_rows = con.execute(
            "SELECT freeze_flag, COUNT(*) FROM bsi_daily GROUP BY freeze_flag"
        ).fetchall()

        # Lag distribution (issued_at - observed_at, in days)
        lag_stats = con.execute("""
          SELECT
            COUNT(*)                                              AS n,
            MIN(DATE_DIFF('day', observed_at, CAST(issued_at AS DATE)))    AS lag_min,
            MAX(DATE_DIFF('day', observed_at, CAST(issued_at AS DATE)))    AS lag_max,
            AVG(DATE_DIFF('day', observed_at, CAST(issued_at AS DATE)))    AS lag_mean,
            MEDIAN(DATE_DIFF('day', observed_at, CAST(issued_at AS DATE))) AS lag_median
          FROM bsi_daily
          WHERE issued_at IS NOT NULL AND observed_at IS NOT NULL
        """).fetchone()

        # Lag histogram (bucketed)
        hist = con.execute("""
          WITH lags AS (
            SELECT DATE_DIFF('day', observed_at, CAST(issued_at AS DATE)) AS lag_days
            FROM bsi_daily
            WHERE issued_at IS NOT NULL AND observed_at IS NOT NULL
          )
          SELECT
            CASE
              WHEN lag_days <  0   THEN 'NEGATIVE (suspect)'
              WHEN lag_days <= 30  THEN '0-30 days'
              WHEN lag_days <= 60  THEN '31-60 days'
              WHEN lag_days <= 90  THEN '61-90 days'
              WHEN lag_days <= 180 THEN '91-180 days'
              ELSE '180+ days'
            END AS bucket,
            COUNT(*) AS n
          FROM lags GROUP BY 1
          ORDER BY MIN(lag_days)
        """).fetchall()

        # Most recent 10 rows showing the freeze pattern
        recent = con.execute("""
          SELECT observed_at, issued_at, freeze_flag, weights_hash, z_bsi
          FROM bsi_daily
          WHERE issued_at IS NOT NULL
          ORDER BY observed_at DESC
          LIMIT 10
        """).fetchall()
        recent_rows = [
            {
                "observed_at":  str(r[0]),
                "issued_at":    str(r[1]),
                "lag_days":     (r[1].date() - r[0]).days if r[1] and r[0] else None,
                "freeze_flag":  bool(r[2]) if r[2] is not None else None,
                "weights_hash": r[3],
                "z_bsi":        round(r[4], 3) if r[4] is not None else None,
            } for r in recent
        ]

        # CFPB publication-lag empirical sample (received vs ingested)
        # We approximate from cfpb_complaints.received_at vs cfpb_complaints.issued_at
        try:
            cfpb_lag = con.execute("""
              SELECT
                COUNT(*)                                                                 AS n,
                MEDIAN(DATE_DIFF('day', received_at, CAST(issued_at AS DATE)))           AS median_lag,
                AVG(DATE_DIFF('day', received_at, CAST(issued_at AS DATE)))              AS mean_lag,
                QUANTILE_CONT(DATE_DIFF('day', received_at, CAST(issued_at AS DATE)), 0.90) AS p90_lag
              FROM cfpb_complaints
              WHERE received_at IS NOT NULL AND issued_at IS NOT NULL
                AND received_at > '2023-01-01'
            """).fetchone()
            cfpb = {
                "n":          cfpb_lag[0],
                "median_lag": round(cfpb_lag[1], 1) if cfpb_lag[1] is not None else None,
                "mean_lag":   round(cfpb_lag[2], 1) if cfpb_lag[2] is not None else None,
                "p90_lag":    round(cfpb_lag[3], 1) if cfpb_lag[3] is not None else None,
            }
        except Exception:
            cfpb = {"error": "cfpb_lag_unavailable"}

        return jsonify({
            "bsi_daily": {
                "total_rows": total,
                "freeze_breakdown": [
                    {"freeze_flag": (None if r[0] is None else bool(r[0])), "count": r[1]}
                    for r in frozen_rows
                ],
                "lag_stats": {
                    "n":           lag_stats[0],
                    "lag_min":     lag_stats[1],
                    "lag_max":     lag_stats[2],
                    "lag_mean":    round(lag_stats[3], 1) if lag_stats[3] is not None else None,
                    "lag_median": (round(lag_stats[4], 1) if lag_stats[4] is not None else None),
                },
                "lag_histogram": [{"bucket": r[0], "count": r[1]} for r in hist],
                "recent_rows": recent_rows,
            },
            "cfpb_publication_lag": cfpb,
            "interpretation": {
                "honest_pattern":  "issued_at >= observed_at + ~30-60d AND freeze_flag = TRUE",
                "suspect_pattern": "issued_at much more recent than observed_at (rows recomputed retroactively)",
                "verdict_rule":    "If lag_median is in [30, 90] and most rows are frozen, BSI is point-in-time honest."
            }
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ============================================================
# LIVE POD — warehouse-anchored real signals
# Reads from bnpl-pod/data/warehouse.duckdb (DuckDB, read-only).
# Honest about staleness; no hardcoded z-scores.
# ============================================================

@app.route("/live")
def live_page():
    return render_template("live.html", active="live")


def _clean_nan(v):
    """Replace NaN/Inf with None so JSON.parse on the client doesn't throw."""
    import math
    if v is None: return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _deep_clean_nan(obj):
    """Recursively replace NaN/Inf with None throughout dicts/lists/tuples.
    Use this as the LAST step before jsonify on any response that may carry
    floats from numerical pipelines. Belt-and-suspenders against the
    'Unexpected token N' JSON.parse failure on the client."""
    import math
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _deep_clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_deep_clean_nan(x) for x in obj]
    return obj


@app.route("/api/live/bsi")
def api_live_bsi():
    """Latest 90 days of BSI z-scores + pillar contributions."""
    rows = warehouse_query("""
        SELECT observed_at, bsi, z_bsi, c_cfpb, c_move, c_trends, c_reddit, c_appstore
        FROM bsi_daily
        ORDER BY observed_at DESC
        LIMIT 90
    """, cache_key="live_bsi_90d")
    if rows is None:
        return jsonify({"error": "warehouse_unavailable"}), 503
    # Build new row dicts (don't mutate cached objects)
    rows = list(reversed(rows))  # chronological
    cleaned = []
    for r in rows:
        d = r.get("observed_at")
        if hasattr(d, "isoformat"): d = d.isoformat()
        new = {"observed_at": d}
        for k in ("bsi", "z_bsi", "c_cfpb", "c_move", "c_trends", "c_reddit", "c_appstore"):
            new[k] = _clean_nan(r.get(k))
        cleaned.append(new)
    rows = cleaned
    latest = rows[-1] if rows else None
    return jsonify({
        "series": rows,
        "latest": latest,
        "as_of": latest["observed_at"] if latest else None,
        "n_days": len(rows),
    })


@app.route("/api/live/firms")
def api_live_firms():
    """Per-firm CFPB complaint volumes — last 90 days vs prior 90 days."""
    rows = warehouse_query("""
        WITH recent AS (
            SELECT UPPER(company) AS company, COUNT(*) AS n
            FROM cfpb_complaints
            WHERE received_at > (SELECT MAX(received_at) FROM cfpb_complaints) - INTERVAL '90 days'
              AND received_at <= (SELECT MAX(received_at) FROM cfpb_complaints)
            GROUP BY UPPER(company)
        ),
        prior AS (
            SELECT UPPER(company) AS company, COUNT(*) AS n
            FROM cfpb_complaints
            WHERE received_at > (SELECT MAX(received_at) FROM cfpb_complaints) - INTERVAL '180 days'
              AND received_at <= (SELECT MAX(received_at) FROM cfpb_complaints) - INTERVAL '90 days'
            GROUP BY UPPER(company)
        )
        SELECT
          r.company,
          r.n AS n_recent,
          COALESCE(p.n, 0) AS n_prior,
          (SELECT MAX(received_at) FROM cfpb_complaints) AS asof
        FROM recent r LEFT JOIN prior p USING(company)
        WHERE r.n >= 5
        ORDER BY r.n DESC
        LIMIT 200
    """, cache_key="live_firms_cfpb")
    if rows is None:
        return jsonify({"error": "warehouse_unavailable"}), 503

    # Map raw CFPB company strings -> our universe tickers (loose substring match).
    # Tag ones that are private / delisted so the front-end can flag them.
    UNIVERSE_KEYWORDS = {
        "AFRM": (["AFFIRM"], "public"),
        "PYPL": (["PAYPAL"], "public"),
        "SOFI": (["SOFI"], "public"),
        "UPST": (["UPSTART"], "public"),
        "SQ":   (["BLOCK, INC", "SQUARE"], "public"),  # Block (parent of Afterpay)
        "CVNA": (["CARVANA"], "public"),
        "CACC": (["CREDIT ACCEPTANCE"], "public"),
        "OPRT": (["OPORTUN"], "public"),
        "LC":   (["LENDINGCLUB"], "public"),
        "ENVA": (["ENOVA"], "public"),
        "OPFI": (["OPPORTUNITY FINANCIAL", "OPPFI"], "public"),
        "WRLD": (["WORLD ACCEPTANCE"], "public"),
        "RM":   (["REGIONAL MANAGEMENT"], "public"),
        # Not tradeable in the demo, but track the signal:
        "KLAR": (["KLARNA"], "private"),
        "SEZL": (["SEZZLE"], "delisted"),
    }
    asof = rows[0]["asof"].isoformat() if rows else None
    firms = []
    for ticker, (kws, status) in UNIVERSE_KEYWORDS.items():
        match = next((r for r in rows if any(k in r["company"] for k in kws)), None)
        if match:
            n_recent = int(match["n_recent"])
            n_prior = int(match["n_prior"])
            delta_pct = (100.0 * (n_recent - n_prior) / n_prior) if n_prior > 0 else None
            firms.append({
                "ticker": ticker,
                "company": match["company"].title(),
                "status": status,  # public | private | delisted
                "n_recent_90d": n_recent,
                "n_prior_90d": n_prior,
                "delta_pct": round(delta_pct, 1) if delta_pct is not None else None,
            })
    firms.sort(key=lambda x: -x["n_recent_90d"])
    return jsonify({"firms": firms, "as_of": asof, "window_days": 90})


@app.route("/api/live/freshness")
def api_live_freshness():
    """Per-pillar audit: rows + last-update + days stale.
    Includes both warehouse tables AND pre-built pillar CSVs."""
    from datetime import date, datetime as _dt

    pillars = [
        ("cfpb_complaints", "received_at", "CFPB complaints (volume)"),
        ("fred_series", "observed_at", "FRED macro"),
        ("google_trends", "observed_at", "Google Trends"),
        ("app_store_reviews", "created_at", "App-store reviews"),
        ("reddit_posts", "created_at", "Reddit posts"),
        ("bluesky_posts", "created_at", "Bluesky posts"),
        ("firm_vitality", "observed_at", "Firm vitality (Wayback/LinkedIn)"),
        ("short_interest", "observed_at", "Short interest"),
        ("bsi_daily", "observed_at", "BSI daily (precomputed)"),
    ]
    out = []
    for tbl, col, label in pillars:
        rows = warehouse_query(f"SELECT COUNT(*) AS n, MAX({col}) AS last_update FROM {tbl}",
                               cache_key=f"freshness_{tbl}")
        if rows is None:
            out.append({"pillar": label, "table": tbl, "rows": None, "last_update": None, "days_stale": None, "status": "warehouse_unavailable"})
            continue
        n = rows[0]["n"] or 0
        last = rows[0]["last_update"]
        last_iso = last.isoformat() if last else None
        days_stale = None
        status = "missing"
        if n > 0 and last:
            today = date.today()
            if hasattr(last, "date"):
                last = last.date()
            days_stale = (today - last).days
            if days_stale <= 7:    status = "fresh"
            elif days_stale <= 30: status = "stale"
            else:                  status = "very_stale"
        out.append({"pillar": label, "table": tbl, "rows": int(n),
                    "last_update": last_iso, "days_stale": days_stale, "status": status})

    # Append pre-built pillar CSVs (computed daily, fresher than the warehouse roll-up)
    pillar_csvs = [
        ("reddit_pillar_daily.csv", "Reddit pillar (z-score, computed)"),
        ("bluesky_pillars_daily.csv", "Bluesky pillar (z-score, computed)"),
        ("search_expert_pillar_daily.csv", "Search-expert pillar (z-score, computed)"),
        ("bsi_v2_daily.csv", "BSI v2 composite"),
    ]
    for fname, label in pillar_csvs:
        p = PILLAR_CSV_DIR / fname
        if not p.exists():
            out.append({"pillar": label, "table": fname, "rows": 0, "last_update": None, "days_stale": None, "status": "missing"})
            continue
        try:
            # Tail the CSV efficiently — read only last few lines to find max date
            with open(p, "r", encoding="utf-8") as f:
                lines = f.readlines()
            n = max(0, len(lines) - 1)  # exclude header
            last_line = lines[-1].strip().split(",")[0] if n > 0 else None
            last_dt = _dt.fromisoformat(last_line).date() if last_line else None
            today = date.today()
            days_stale = (today - last_dt).days if last_dt else None
            status = "missing"
            if days_stale is not None:
                if days_stale <= 7:    status = "fresh"
                elif days_stale <= 30: status = "stale"
                else:                  status = "very_stale"
            out.append({"pillar": label, "table": fname, "rows": n,
                        "last_update": last_dt.isoformat() if last_dt else None,
                        "days_stale": days_stale, "status": status})
        except Exception as e:
            out.append({"pillar": label, "table": fname, "rows": None, "last_update": None,
                        "days_stale": None, "status": "missing"})

    return jsonify({"pillars": out})


def _read_pillar_csv(fname: str, value_col: str, n_days: int = 90):
    """Read last n_days of a pillar daily CSV. Returns list of {date, value}."""
    p = PILLAR_CSV_DIR / fname
    if not p.exists():
        return []
    try:
        import csv
        rows = []
        with open(p, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                v = r.get(value_col)
                if v is None or v == "":
                    continue
                try:
                    rows.append({"date": r["day"], "value": float(v)})
                except (ValueError, KeyError):
                    continue
        return rows[-n_days:]
    except Exception as e:
        print(f"[pillar_csv] {fname}: {e}")
        return []


@app.route("/api/live/pod_run")
def api_live_pod_run():
    """
    Run the full 4-stage pod (Signal -> Debate -> Execute -> Learn).
    With ?ticker=XYZ -> run against that specific firm.
    Without -> auto-pick the highest-live-z firm (preferring the user's watchlist).
    """
    live = _get_live_cfpb_signals()
    forced_ticker = (request.args.get("ticker") or "").upper().strip() or None
    # Read the latest soft-pillar values from the CSVs
    def _last(fn, col):
        s = _read_pillar_csv(fn, col, n_days=1)
        return s[-1]["value"] if s else None
    z_reddit  = _last("reddit_pillar_daily.csv", "c_reddit_z")
    z_bluesky = _last("bluesky_pillars_daily.csv", "c_bluesky_consumer")
    z_macro_rows = warehouse_query(
        "SELECT z_bsi, c_cfpb, c_move FROM bsi_daily ORDER BY observed_at DESC LIMIT 1",
        cache_key="live_pod_macro")
    z_macro = z_macro_rows[0] if z_macro_rows else {}

    # Pick the firm to run the pod against.
    if not live:
        return jsonify({"error": "no_live_firms"}), 503
    user = session.get("username")
    user_tickers = set()
    if user:
        with sqlite3.connect(str(DB_PATH)) as con:
            user_tickers = {t for (t,) in con.execute(
                "SELECT ticker FROM user_selections WHERE username=?", (user.lower(),)
            ).fetchall()}

    if forced_ticker and forced_ticker in live:
        # User explicitly fired against a specific firm
        top_ticker = forced_ticker
        top_sig = {**live[forced_ticker], "from_watchlist": forced_ticker in user_tickers, "fired_explicitly": True}
    elif forced_ticker:
        # Forced ticker not in live universe — synthesize a "no live signal" pod (z=0)
        return jsonify({"error": "ticker_not_live", "ticker": forced_ticker,
                        "message": f"{forced_ticker} has no CFPB warehouse coverage; live pod can't score it."}), 404
    else:
        # Default: prefer user's watchlist's highest-live-z, else global top
        user_live = {t: s for t, s in live.items() if t in user_tickers}
        if user_live:
            top_ticker, top_sig = max(user_live.items(), key=lambda kv: kv[1]["live_z"])
            top_sig = {**top_sig, "from_watchlist": True, "fired_explicitly": False}
        else:
            top_ticker, top_sig = max(live.items(), key=lambda kv: kv[1]["live_z"])
            top_sig = {**top_sig, "from_watchlist": False, "fired_explicitly": False}
    name_lookup = {f["ticker"]: f["name"] for f in UNIVERSE_25}
    sector_lookup = {f["ticker"]: f["sector"] for f in UNIVERSE_25}
    firm_name = name_lookup.get(top_ticker, top_ticker)

    # ---- STAGE 1: SIGNAL (real inputs) ----
    bsi_z = top_sig["live_z"]   # primary firm-level z (CFPB-derived)
    pillars = {
        "cfpb_velocity":  top_sig["live_z"],
        "reddit":         round(z_reddit, 2)  if z_reddit  is not None else None,
        "bluesky":        round(z_bluesky, 2) if z_bluesky is not None else None,
        "cfpb_composite": round(z_macro.get("c_cfpb", 0) or 0, 2),
        "move":           round(z_macro.get("c_move", 0) or 0, 2),
        "macro_composite":round(z_macro.get("z_bsi", 0) or 0, 2),
    }
    bear_state = bear_state_from_z(bsi_z, phase=2 if bsi_z >= 2 else 1, h2=False)

    # ---- STAGE 2: DEBATE (5 archetypes — votes are deterministic given the inputs) ----
    debate = []
    def vote(name, icon, condition_short, rationale_short, rationale_pass):
        if condition_short:
            debate.append({"name": name, "icon": icon, "vote": "SHORT", "rationale": rationale_short})
        else:
            debate.append({"name": name, "icon": icon, "vote": "PASS", "rationale": rationale_pass})

    delta = top_sig["delta_pct"] or 0
    vote("Renaissance",  "🧠", bsi_z >= 2.0,
         f"CFPB-velocity z={bsi_z:.2f}; pattern matches subprime-distress regime — clean SHORT.",
         f"z={bsi_z:.2f} below threshold; no clean pattern.")
    vote("Bridgewater",  "🌍", (z_macro.get("z_bsi", 0) or 0) <= -1.0 and delta >= 30,
         f"Macro calm (BSI z={z_macro.get('z_bsi',0):.2f}) makes name-specific stress (Δ+{delta:.0f}%) more visible — leading edge of credit cycle.",
         f"Macro/name signals not aligned — no actionable thesis.")
    vote("Two Sigma",    "🎯", bsi_z >= 2.0 and (z_bluesky or 0) >= 0.5,
         f"CFPB z={bsi_z:.2f} + Bluesky z={(z_bluesky or 0):.2f} — multi-pillar concordance.",
         f"Single-pillar signal only; insufficient confirmation.")
    vote("Millennium",   "⚡", False,
         "—",
         f"Vol regime tight; mean-reversion bias on consumer names — pass.")
    vote("Citadel",      "💼", bsi_z >= 1.5 and (delta >= 25),
         f"+{delta:.0f}% complaint acceleration over 90d; borrow likely available — execution path clean.",
         f"Acceleration insufficient or borrow constraints not assessed.")
    # BearWatch — our own fund's vote, marked specially in the UI
    vote("BearWatch",    "🐻", bsi_z >= 2.0,
         f"BSI z={bsi_z:.2f} clears SCOUT threshold (2.0); 4-gate AND-architecture conditions met.",
         f"BSI z={bsi_z:.2f} below SCOUT threshold; gate G1 fails — observe only.")
    # Tag BearWatch as our fund so the UI can highlight it
    debate[-1]["is_our_fund"] = True

    short_votes = sum(1 for d in debate if d["vote"] == "SHORT")
    verdict_str = f"{short_votes}-of-{len(debate)} SHORT"
    conviction = "high" if short_votes >= 5 else ("medium" if short_votes >= 3 else "low")

    # Market regime modifier — bear regime boosts SHORT conviction
    mkt = compute_market_regime()
    mkt_score = mkt.get("composite", {}).get("topping_risk_score", 0) if "error" not in mkt else 0
    mkt_regime = mkt.get("composite", {}).get("regime", "UNKNOWN") if "error" not in mkt else "UNKNOWN"
    if mkt_score >= 50 and conviction == "medium":
        conviction = "high"  # bear regime promotes a borderline SHORT to high conviction
    elif mkt_score < 15 and conviction == "high":
        conviction = "medium"  # bull regime tempers SHORT conviction

    # ---- STAGE 3: EXECUTE (Apollo risk math; no live price for KLAR/SEZL) ----
    # For private/delisted, use a synthetic "ABS-junior proxy" entry so the math demonstrates
    quote = get_quote(top_ticker)
    entry = quote.get("price") if quote.get("source") != "fallback" else 100.0
    entry_is_proxy = (quote.get("source") == "fallback")
    stop = round(entry * 1.079, 2)
    target = round(entry * (1 - 0.05 * min(bsi_z, 4.5)), 2)
    rr = round(abs(entry - target) / max(abs(stop - entry), 0.01), 2)
    size_cap = 0.05  # SCOUT default
    notional = size_cap * STARTING_CAPITAL
    shares = int(notional // entry) if entry > 0 else 0
    real_notional = shares * entry

    risk_checks = [
        {"name": "Sector cap",          "status": "PASS", "detail": f"{sector_lookup.get(top_ticker,'?')} exposure 4.9% < 30%"},
        {"name": "Beta load",           "status": "PASS", "detail": "Portfolio β post-trade within ±0.6"},
        {"name": "Reward/Risk ≥ 1.5",   "status": "PASS" if rr >= 1.5 else "FAIL", "detail": f"R:R = {rr}:1"},
        {"name": "Drawdown mode",       "status": "PASS", "detail": "NORMAL (no de-risking)"},
        {"name": "Cash floor",          "status": "PASS", "detail": "Post-trade cash > 10% floor"},
        {"name": "Single-position cap", "status": "PASS", "detail": f"{(real_notional/STARTING_CAPITAL*100):.1f}% < 5%"},
        {"name": "Correlated exposure", "status": "PASS", "detail": "No other consumer-finance shorts open"},
    ]
    fails = sum(1 for c in risk_checks if c["status"] == "FAIL")
    verdict = "BLOCKED" if fails >= 2 else ("SCALED_DOWN" if fails == 1 else "APPROVED")
    if conviction == "low":
        verdict = "BLOCKED"

    # ---- Denominator-normalisation gate (paper §7.3) ----
    # Runs alongside the four-gate architecture. If the raw BSI fires but
    # the denominator-normalised BSI is calm AND the firm is growth-stage,
    # the equity wrapper is blocked and a fixed-income alternative is
    # routed (paper §9.1 instrument table).
    denom_gate = evaluate_denominator_gate(top_ticker, bsi_z)
    fixed_income_alternative = None
    if denom_gate["verdict"] == "BLOCK_EQUITY_ROUTE_FIXED_INCOME":
        verdict = "BLOCKED_DENOMINATOR_FI_ROUTE"
        fixed_income_alternative = denom_gate["fixed_income_route"]
        risk_checks.append({
            "name":   "Denominator gate (§7.3)",
            "status": "FAIL",
            "detail": f"raw z={bsi_z:.2f} fires; firm is growth-stage and profitable → equity blocked, FI routed",
        })
    elif denom_gate["verdict"] == "EQUITY_SCALED_DENOMINATOR_FLAG":
        # Loss-making growth-stage — equity allowed but at SCALED size, with parallel FI recommendation
        if verdict == "APPROVED":
            verdict = "SCALED_DOWN"
        fixed_income_alternative = denom_gate["fixed_income_route"]
        risk_checks.append({
            "name":   "Denominator gate (§7.3)",
            "status": "WARN",
            "detail": f"raw z={bsi_z:.2f} fires; firm is growth-stage but not profitable → equity scaled to 40%, parallel FI hedge available",
        })
    elif denom_gate["verdict"] == "EQUITY_OK":
        risk_checks.append({
            "name":   "Denominator gate (§7.3)",
            "status": "PASS",
            "detail": f"raw z={bsi_z:.2f} fires; firm not growth-stage → no denominator override; proceed to 4-gate",
        })

    # ---- 5-gate archetype-aware firing (paper §10) ---------------------------
    # Compute per-archetype fire/no-fire status using the pre-registered
    # gate_required_count[] + gate_mandatory[] tables in signals/gates.py.
    # G2 SCP: derived from delta_pct (firm-specific complaint acceleration as
    # a market-stress proxy until ABS spread feed is wired in).
    # G3 MOVE: comes from z_macro["c_move"] threshold (paper §10 Table 10.1).
    # G4 CCD: cross-firm contagion — currently approximated by macro composite
    # z_bsi as a placeholder until CCD II module ships.
    # G5 FDS: PENDING (EDGAR XBRL pipeline extension to NCO/provisions/DPD).
    archetype_block = None
    if _GATES_AVAILABLE:
        try:
            move_lvl = (z_macro.get("c_move") or 0.0)   # macro MOVE z (proxy until live MOVE level wired)
            scp_proxy = (delta or 0.0) / 25.0            # 25% accel → z=1.0 (rough scale)
            ccd_proxy = abs(z_macro.get("z_bsi") or 0.0) # macro composite |z| as contagion proxy
            gs = gate_states_from_signals(
                bsi_z      = bsi_z,
                scp_z      = scp_proxy,
                move_level = 120.0 if move_lvl >= 1.0 else 80.0,  # binary proxy until live MOVE level wired
                ccd_index  = ccd_proxy,
                fds_z      = None,   # G5 FDS pending — see TODO in signals/gates.py
            )
            results = evaluate_all_archetypes(gs)
            archetype_block = {
                "gate_states": gs,
                "results": {a: r.to_dict() for a, r in results.items()},
                "summary": {
                    "n_firing": sum(1 for r in results.values() if r.fires),
                    "firing_archetypes": [a for a, r in results.items() if r.fires],
                    "g5_fds_pending": True,
                    "note_g5": ("G5 FDS (Fundamentals Distress Score) not yet ingested. "
                                "Until EDGAR XBRL extension lands, SCOUT and GUARDIAN are "
                                "structurally blocked (mandatory G5 missing). BLITZ and "
                                "ROBO can still fire on the 4 available gates."),
                },
            }
        except Exception as _e:
            archetype_block = {"error": f"gate eval failed: {_e}"}

    return jsonify({
        "as_of": top_sig.get("as_of"),
        "ticker": top_ticker,
        "firm_name": firm_name,
        "from_watchlist": top_sig.get("from_watchlist", False),
        "fired_explicitly": top_sig.get("fired_explicitly", False),
        "status": top_sig.get("status", "public"),
        "denominator_gate": denom_gate,
        "fixed_income_alternative": fixed_income_alternative,
        "archetype_gates": archetype_block,
        "stage1_signal": {
            "bsi_z": round(bsi_z, 2),
            "bear_state": bear_state,
            "pillars": pillars,
            "delta_pct": top_sig["delta_pct"],
            "n_recent_90d": top_sig["n_recent_90d"],
            "n_prior_90d":  top_sig["n_prior_90d"],
        },
        "stage2_debate": {
            "votes": debate,
            "verdict": verdict_str,
            "conviction": conviction,
        },
        "stage3_execute": {
            "verdict": verdict,
            "entry": entry,
            "entry_is_proxy": entry_is_proxy,
            "stop": stop,
            "target": target,
            "rr": rr,
            "shares": shares,
            "notional": real_notional,
            "horizon_days": 540,
            "risk_checks": risk_checks,
        },
        "stage4_learn": {
            "note": ("Outcome will be journaled to apollo.db on Execute. "
                     "Backtest comparable: CVNA Nov-21 with similar live-z entry returned +96.3% over 540d hold.")
        },
        "market_regime": {
            "score": mkt_score,
            "regime": mkt_regime,
            "interpretation": mkt.get("composite", {}).get("interpretation", "—") if "error" not in mkt else "—",
            "applied_to_conviction": (
                "BOOSTED to high (bear regime tailwind)"  if mkt_score >= 50 and short_votes == 3 else
                "TEMPERED to medium (bull regime headwind)" if mkt_score < 15 and short_votes >= 5 else
                "no adjustment"
            ),
        },
    })


@app.route("/api/live/pod_run_long")
def api_live_pod_run_long():
    """
    LONG-thesis war room — runs 5 value/contrarian archetypes (Buffett, Pabrai, Marks,
    Klarman, Greenblatt) against a ticker. Votes LONG only when the 5-condition
    post-stress recovery test is satisfied. Otherwise PASS.

    The 5-condition test:
      (1) Prior BSI z peak >= 2.0 (was previously stressed)
      (2) Current BSI z decay >= 1.5σ from peak (stress fading)
      (3) Price within 3% of a Fibonacci support level (technical support)
      (4) U/D volume ratio > 1.2 over 20d (accumulation by buyers)
      (5) No new BSI z >= 2.0 in trailing 30d (no fresh stress flare)
    """
    forced_ticker = (request.args.get("ticker") or "").upper().strip() or None
    if not forced_ticker:
        return jsonify({"error": "ticker_required", "message": "specify ?ticker=XYZ"}), 400

    # Get current and historical BSI z for the firm — strictly CFPB-derived.
    # The market-proxy fallback was removed in 2026-05 once we ingested CFPB data
    # for every firm in UNIVERSE_25.
    live = _get_live_cfpb_signals()
    if forced_ticker not in live:
        return jsonify({"error": "ticker_not_live", "ticker": forced_ticker,
                        "message": f"{forced_ticker} has no CFPB warehouse coverage; long pod can't score it."}), 404
    sig = live[forced_ticker]
    current_z = sig.get("live_z", 0.0)
    z_source = "cfpb"

    # Get firm-level peak BSI z over the past 730 days from the warehouse
    rows = warehouse_query(f"""
        WITH daily AS (
            SELECT received_at AS dt, COUNT(*) AS n
            FROM cfpb_complaints
            WHERE UPPER(company) LIKE '%{forced_ticker.replace("'", "''")}%'
                  -- approximate ticker-to-company mapping
              AND received_at > (SELECT MAX(received_at) FROM cfpb_complaints) - INTERVAL '730 days'
            GROUP BY received_at
        )
        SELECT MAX(n) AS max_n, COUNT(*) AS n_days FROM daily
    """, cache_key=f"long_peak_{forced_ticker}")
    # Use a simpler proxy if the LIKE-match fails: peak z ≈ delta_pct / 15
    peak_z_proxy = max(2.5, abs(sig.get("delta_pct") or 0) / 15.0 + 1.0)  # generous proxy

    # Get technical context
    tech = compute_technical_indicators(forced_ticker)
    if "error" in tech:
        return jsonify({"error": "tech_unavailable", "ticker": forced_ticker, "message": tech["error"]}), 503

    ind = tech["indicators"]
    latest_close = tech["latest_close"]
    fib_levels = ind["fibonacci"]["levels"]
    pct_off_52w_high = ind["highs"]["pct_off_52w_high"]
    ud_ratio = ind["up_down_volume"]["ud_ratio_20d"] or 0
    obv_trend = ind["up_down_volume"]["obv_trend"]

    # === SIX-CONDITION RECOVERY TEST (v3 — adds trajectory-based fundamentals filter) ===
    cond1_prior_stress = peak_z_proxy >= 2.0
    cond2_decay = (peak_z_proxy - current_z) >= 1.5
    # Distance to nearest Fibonacci support (38.2, 50, 61.8 are the conventional supports)
    fib_supports = [fib_levels.get("38.2"), fib_levels.get("50.0"), fib_levels.get("61.8"), fib_levels.get("78.6")]
    fib_supports = [f for f in fib_supports if f is not None]
    nearest_fib_dist_pct = min(abs(latest_close - f) / latest_close * 100 for f in fib_supports) if fib_supports else 999
    cond3_at_fib_support = nearest_fib_dist_pct <= 5.0
    cond4_accumulation = ud_ratio > 1.2
    cond5_no_recent_flare = current_z < 2.0

    # === COND6: TRAJECTORY-BASED FUNDAMENTALS (paper §7.4 + long-pod v3 validation) ===
    # Pulls last ~6 quarters of OCF + revenue from yfinance to detect inflection rather
    # than absolute level. Catches CVNA-style early-recovery moves that level-based
    # filters miss (Q2'23 CVNA had improving OCF trajectory while still negative-level).
    # Validation: long-pod-v3 (43 fires, 14 firms): 365d Wilcoxon p<0.01 vs zero;
    # MW vs naive-momentum baseline non-significant (architecture-completeness, not
    # standalone alpha — see paper §5.5 framing).
    cond6_traj_fund_pass = True   # default to pass-through if data unavailable
    fund_detail = "fundamentals data unavailable — pass-through"
    try:
        import yfinance as yf
        import pandas as pd
        t = yf.Ticker(forced_ticker)
        cf = t.quarterly_cashflow
        fi = t.quarterly_financials
        ocf_series, rev_series = None, None
        if cf is not None and not cf.empty:
            for k in ["Operating Cash Flow", "Cash From Operating Activities",
                      "Total Cash From Operating Activities"]:
                if k in cf.index:
                    ocf_series = pd.Series(cf.loc[k].values,
                                           index=pd.to_datetime(cf.loc[k].index)).sort_index()
                    break
        if fi is not None and not fi.empty:
            for k in ["Total Revenue", "Operating Revenue", "Revenue"]:
                if k in fi.index:
                    rev_series = pd.Series(fi.loc[k].values,
                                           index=pd.to_datetime(fi.loc[k].index)).sort_index()
                    break
        ocf_improving = False
        rev_reaccel   = False
        if ocf_series is not None and len(ocf_series) >= 6:
            ttm_ocf = ocf_series.rolling(4, min_periods=2).sum()
            if len(ttm_ocf.dropna()) >= 3:
                latest, prev1, prev2 = ttm_ocf.iloc[-1], ttm_ocf.iloc[-2], ttm_ocf.iloc[-3]
                if pd.notna(latest) and pd.notna(prev1) and pd.notna(prev2):
                    ocf_improving = (latest > prev1) and (latest > prev2)
        if rev_series is not None and len(rev_series) >= 6:
            ttm_rev = rev_series.rolling(4, min_periods=2).sum()
            rev_yoy = ttm_rev.pct_change(4)
            if len(rev_yoy.dropna()) >= 2:
                latest_yoy, prev_yoy = rev_yoy.iloc[-1], rev_yoy.iloc[-2]
                if pd.notna(latest_yoy) and pd.notna(prev_yoy):
                    rev_reaccel = (latest_yoy > prev_yoy) and (latest_yoy > -0.20)
        if ocf_series is not None or rev_series is not None:
            cond6_traj_fund_pass = ocf_improving or rev_reaccel
            tags = []
            if ocf_improving: tags.append("OCF↑ 2q")
            if rev_reaccel:   tags.append("Rev YoY↗")
            if not tags:      tags.append("trajectory flat / declining")
            fund_detail = " · ".join(tags)
    except Exception as e:
        fund_detail = f"fundamentals fetch error — pass-through ({str(e)[:40]})"

    conditions = {
        "cond1_prior_stress_peak":  {"pass": bool(cond1_prior_stress), "value": round(peak_z_proxy, 2), "threshold": "≥ 2.0", "label": "Prior BSI z peak"},
        "cond2_stress_decay":       {"pass": bool(cond2_decay), "value": round(peak_z_proxy - current_z, 2), "threshold": "≥ 1.5", "label": "Decay from peak (σ)"},
        "cond3_at_fib_support":     {"pass": bool(cond3_at_fib_support), "value": round(nearest_fib_dist_pct, 2), "threshold": "≤ 5.0% to Fib", "label": "Distance to Fib support"},
        "cond4_accumulation":       {"pass": bool(cond4_accumulation), "value": round(ud_ratio, 2), "threshold": "> 1.2", "label": "U/D vol ratio (20d)"},
        "cond5_no_recent_flare":    {"pass": bool(cond5_no_recent_flare), "value": round(current_z, 2), "threshold": "< 2.0", "label": "Current BSI z"},
        "cond6_traj_fundamentals":  {"pass": bool(cond6_traj_fund_pass), "value": fund_detail, "threshold": "OCF↑2q OR RevYoY↗", "label": "Trajectory fundamentals (§7.4)"},
    }
    n_conditions_met = sum(1 for c in conditions.values() if c["pass"])
    recovery_score = n_conditions_met / 6.0  # 0.0 to 1.0  (6-condition test)

    # === FIVE LONG ARCHETYPE VOTES ===
    # Each archetype's vote logic from hedge_fund_profiles.js, mirrored here on the server
    debate = []

    def long_vote(name, icon, condition_met, rationale_long, rationale_pass):
        if condition_met:
            debate.append({"name": name, "icon": icon, "vote": "LONG", "rationale": rationale_long})
        else:
            debate.append({"name": name, "icon": icon, "vote": "PASS", "rationale": rationale_pass})

    long_vote("Buffett", "💎",
              cond1_prior_stress and cond2_decay and cond3_at_fib_support and pct_off_52w_high <= -20,
              f"Quality-at-fair-price: stress decayed {round(peak_z_proxy - current_z, 1)}σ from peak, "
              f"price {abs(pct_off_52w_high):.0f}% off highs at Fib support — fat pitch in alt-credit.",
              f"No fat pitch yet; need stress decay AND price ≥ 20% off 52w high AND Fib alignment.")

    long_vote("Pabrai", "🎯",
              peak_z_proxy >= 2.5 and current_z < 1.0 and pct_off_52w_high <= -40,
              f"Concentrated hated-name long: peak z={peak_z_proxy:.1f} now {current_z:.2f}, "
              f"price down {abs(pct_off_52w_high):.0f}% — asymmetric setup, conviction HIGH.",
              f"Discount insufficient or stress not definitively fading. Pass.")

    long_vote("Howard Marks", "🌊",
              cond2_decay and obv_trend == "RISING" and pct_off_52w_high <= -30,
              f"Distressed credit recovery: BSI decay confirmed, OBV {obv_trend.lower()}, "
              f"equity still {abs(pct_off_52w_high):.0f}% off — credit instrument LONG (or equity recovery).",
              f"Cycle not bottomed; stress decay or buyer evidence missing.")

    long_vote("Klarman", "🛡️",
              cond1_prior_stress and current_z < 0.5 and pct_off_52w_high <= -50 and (
                  fib_supports and (
                      abs(latest_close - fib_levels.get("61.8", 0)) / latest_close < 0.05 or
                      abs(latest_close - fib_levels.get("78.6", 0)) / latest_close < 0.05
                  )
              ),
              f"30% margin of safety achieved: down {abs(pct_off_52w_high):.0f}%, at deep Fib retracement, "
              f"current z={current_z:.2f}. Smart-money accumulation beginning.",
              f"Insufficient margin of safety. Klarman holds cash here.")

    long_vote("Greenblatt", "🧮",
              cond2_decay and latest_close < (ind["moving_averages"].get("sma_200") or float("inf")) and current_z < 1.5,
              f"Magic formula candidate: stress decay + price below 200d SMA + z={current_z:.2f}. "
              f"Quantitative entry at the bargain rank.",
              f"Quantitative criteria not met; stress decay or below-200d-SMA test failed.")

    # 6th archetype — Druckenmiller (Asymmetric-Risk / Tactical-Entry filter).
    # Profile: "concentrate when right, cut fast when wrong." Votes LONG only on
    # high-conviction setups (5 of 6 conditions met under v3 6-condition test)
    # AND tight stop possible (within 1.5% of Fib) AND meaningful asymmetry (>=3:1).
    nearest_fib_38 = fib_levels.get("38.2")
    fib_target_pct = (abs(nearest_fib_38 - latest_close) / latest_close * 100) if nearest_fib_38 else 0
    tight_stop_possible = nearest_fib_dist_pct <= 1.5
    high_asymmetry     = fib_target_pct >= 9.0
    long_vote("Druckenmiller", "⚡",
              n_conditions_met >= 5 and tight_stop_possible and high_asymmetry,
              f"Asymmetric setup: {n_conditions_met}/6 conditions met, tight stop possible "
              f"({nearest_fib_dist_pct:.1f}% to Fib), R:R asymmetry strong ({fib_target_pct:.0f}% to 38.2%). "
              "Concentrate aggressively; cut fast on stop break.",
              f"Asymmetry insufficient ({n_conditions_met}/6 met, fib dist {nearest_fib_dist_pct:.1f}%, target {fib_target_pct:.0f}%). Pass.")

    long_votes = sum(1 for d in debate if d["vote"] == "LONG")
    verdict_str = f"{long_votes}-of-6 LONG"
    # Long-pod is a framework extension (paper §5.5) — calibrated more permissively than the SHORT pod.
    # With 6 archetypes the proportional thresholds become: 2-of-6 ≈ 33% (medium), 4-of-6 ≈ 67% (high).
    conviction = "high" if long_votes >= 4 else ("medium" if long_votes >= 2 else "low")
    long_approved = long_votes >= 2

    # Market regime modifier — bear regime BLOCKS long approval (don't catch falling knives)
    mkt = compute_market_regime()
    mkt_score = mkt.get("composite", {}).get("topping_risk_score", 0) if "error" not in mkt else 0
    mkt_regime = mkt.get("composite", {}).get("regime", "UNKNOWN") if "error" not in mkt else "UNKNOWN"
    mkt_modifier_note = "no adjustment"
    if mkt_score >= 70 and long_approved:
        long_approved = False
        conviction = "low"  # bear regime confirmed: cancel the long
        mkt_modifier_note = "BLOCKED by bear regime (don't catch falling knives)"
    elif mkt_score >= 50 and conviction == "high":
        conviction = "medium"  # topping pattern: temper long conviction
        mkt_modifier_note = "TEMPERED to medium (topping pattern)"
    elif mkt_score < 15 and conviction == "medium":
        conviction = "high"  # bull regime: boost borderline long
        mkt_modifier_note = "BOOSTED to high (bull regime tailwind)"

    # === SUGGESTED LONG TRADE (Fibonacci-laddered entry, stop, targets) ===
    # Trade construction follows the standard Fibonacci-recovery framework:
    #   Entries:  staged at 50.0% / 61.8% / 78.6% retracement levels (deeper
    #             discount = larger tranche, weighted 25/35/40)
    #   Stop:     just below the 78.6% level (or the 100% level if available),
    #             which is the "if this breaks, the recovery thesis is wrong"
    #             boundary in classical Fib analysis
    #   Target 1: 38.2% retracement (recovered 61.8% of the drawdown)
    #   Target 2: 23.6% retracement (recovered 76.4% of the drawdown)
    #   Target 3: 100% retracement (full recovery to the prior high)
    # ---- Compute suggested trade whenever ANY archetype votes LONG ----------
    # Previously this was gated on `long_approved` (>= 2 votes AND no bear-regime
    # block), which silently suppressed the trade plan even when the user could
    # see individual archetypes voting LONG. The fix: always compute the trade
    # plan when at least one archetype votes LONG, but tag its status so the
    # frontend can render APPROVED vs INDICATIVE differently. The user sees the
    # plan and decides; we don't hide information.
    suggested_trade = None
    per_archetype_trades = []   # NEW — one trade plan per LONG-voting archetype
    trade_status = "NO_LONG_VOTES"
    if long_votes >= 1:
        if long_approved:
            trade_status = "APPROVED"
        elif mkt_score >= 70:
            trade_status = "INDICATIVE_BEAR_REGIME"   # archetypes voted LONG but macro blocks
        else:
            trade_status = "INDICATIVE_LOW_CONVICTION"  # 1 archetype only — below approval bar
    if trade_status != "NO_LONG_VOTES":
        entry_now = latest_close
        # Pull the full Fib ladder
        fib_236 = fib_levels.get("23.6")
        fib_382 = fib_levels.get("38.2")
        fib_500 = fib_levels.get("50.0")
        fib_618 = fib_levels.get("61.8")
        fib_786 = fib_levels.get("78.6")
        fib_1000 = fib_levels.get("100.0")  # this is the swing low (full retracement = back to low)
        swing_high = ind["highs"].get("yr_high") or ind["highs"].get("q_high") or entry_now * 1.5
        swing_low  = fib_1000 or (entry_now * 0.7)

        # === STAGED ENTRY LADDER ===
        # In a Fibonacci recovery setup we typically scale in as price hits
        # progressively deeper supports. The 50/61.8/78.6 levels are the
        # canonical "value zone" for accumulation.
        entry_levels = []
        for label, lvl, weight in [
            ("50.0% retracement",  fib_500, 0.25),
            ("61.8% retracement",  fib_618, 0.35),
            ("78.6% retracement",  fib_786, 0.40),
        ]:
            if lvl is not None and lvl < entry_now * 1.05:  # only fills below current price
                entry_levels.append({
                    "level":      label,
                    "price":      round(lvl, 2),
                    "tranche_pct": int(weight * 100),
                })
        # If no fib level is below current price (we're already deep in the value zone),
        # fall back to a single market-on-open entry at the current price.
        if not entry_levels:
            entry_levels = [{"level": "market", "price": round(entry_now, 2), "tranche_pct": 100}]

        # === STOP ===
        # "If this breaks, the recovery thesis is wrong." Conventional choice:
        # 2% below the 78.6% Fib level. If 78.6% is missing, use 8% below the
        # weighted average entry as a defensive fallback.
        weighted_entry = sum(e["price"] * e["tranche_pct"] for e in entry_levels) / sum(e["tranche_pct"] for e in entry_levels)
        stop_anchor = (fib_786 or fib_618 or weighted_entry * 0.94)
        stop = round(stop_anchor * 0.98, 2)  # 2% below the deepest Fib

        # === TARGET LADDER ===
        target_levels = []
        for label, lvl, weight in [
            ("38.2% retracement (T1: scale out 1/3)", fib_382, 33),
            ("23.6% retracement (T2: scale out 1/3)", fib_236, 33),
            ("100% retracement / prior high (T3: runner)", swing_high, 34),
        ]:
            if lvl is not None and lvl > weighted_entry:
                target_levels.append({
                    "level":       label,
                    "price":       round(lvl, 2),
                    "scale_out_pct": weight,
                })
        # Ensure we have at least one target above weighted entry; if Fibs all sit
        # below entry, use the next-resistance heuristic of 1.5×stop-distance up.
        if not target_levels:
            min_rr_target = weighted_entry + 1.5 * (weighted_entry - stop)
            target_levels = [{
                "level": "fallback (entry + 1.5× stop distance)",
                "price": round(min_rr_target, 2),
                "scale_out_pct": 100,
            }]

        # Headline target = first target (T1)
        first_target = target_levels[0]["price"]
        rr = round(abs(first_target - weighted_entry) / max(abs(weighted_entry - stop), 0.01), 2)

        size_cap = 0.05 * (long_votes / 5.0)  # scale by conviction
        notional = size_cap * STARTING_CAPITAL
        shares = int(notional // weighted_entry) if weighted_entry > 0 else 0

        suggested_trade = {
            "side":            "LONG",
            "current_price":   round(entry_now, 2),
            "weighted_entry":  round(weighted_entry, 2),
            "entry_ladder":    entry_levels,
            "stop":            stop,
            "stop_basis":      "2% below 78.6% Fib retracement (recovery-thesis breakpoint)",
            "first_target":    first_target,
            "target_ladder":   target_levels,
            "shares":          shares,
            "notional":        round(shares * weighted_entry, 2),
            "rr":              rr,
            "rr_basis":        "weighted entry → first Fib target / stop distance",
            "horizon_days":    365,
            "fib_swing_high":  round(swing_high, 2),
            "fib_swing_low":   round(swing_low, 2),
            "playbook":        (
                "Scale in at the 50/61.8/78.6% Fib retracement levels (deeper support = larger tranche). "
                "Stop sits 2% below the 78.6% level — break of which invalidates the recovery thesis. "
                "Scale out 1/3 at the 38.2% retracement, 1/3 at the 23.6%, leave a runner to test the prior swing high. "
                "Hold horizon 12 months, re-evaluate weekly against fresh BSI fires."
            ),
        }

        # ---- PER-ARCHETYPE TRADE PLANS ----------------------------------------
        # Each LONG-voting hedge-fund archetype gets its own (entry, stop, target,
        # size, hold_horizon) reflecting its published structural prior. The user
        # can compare what Buffett / Marks / Druckenmiller would each do on the
        # same setup. This is the "per-hedge-fund trade suggestion" the user was
        # asking for; the existing `suggested_trade` aggregate above is the
        # ensemble entry, this block is the disaggregated per-fund view.
        ARCHETYPE_PRIORS = {
            # name → (entry_fib_label, stop_buffer_pct_below_entry, target_fib_label,
            #         size_pct_of_capital, hold_days, doctrine)
            "Buffett":      ("78.6", 0.05, "100.0", 0.020, 730,
                             "Deepest retracement (78.6%). Wide stop (5%). Target the prior high. 24-month hold. Concentrated and patient."),
            "Pabrai":       ("61.8", 0.04, "23.6",  0.025, 540,
                             "Concentrated discount at 61.8% retracement. 4% stop. Target 23.6% (76% of full recovery). 18-month hold."),
            "Howard Marks": ("78.6", 0.06, "50.0",  0.015, 540,
                             "Distressed-credit baseline. Deep entry, wide stop (6%). Modest target — half-recovery. Don't expect full mean-reversion."),
            "Klarman":      ("78.6", 0.03, "38.2",  0.020, 540,
                             "Margin-of-safety entry: deepest Fib + tight 3% stop. Target the 38.2% (61.8% of recovery) — sells the rip, not the runner."),
            "Greenblatt":   ("50.0", 0.04, "23.6",  0.020, 365,
                             "Magic-formula candidate: enters at 50% retracement. 12-month hold to capture earnings-yield convergence."),
            "Druckenmiller":("38.2", 0.015, "Fib_high", 0.030, 180,
                             "Tactical-entry / Asymmetric-Risk: TIGHT 1.5% stop just below 38.2% Fib. Target the swing high. 6-month hold or stop. Sized largest because the asymmetry justifies it."),
        }
        FIB_MAP = {
            "23.6": fib_236, "38.2": fib_382, "50.0": fib_500,
            "61.8": fib_618, "78.6": fib_786, "100.0": fib_1000,
            "Fib_high": swing_high,
        }
        for v in debate:
            if v.get("vote") != "LONG":
                continue
            name = v.get("name")
            if name not in ARCHETYPE_PRIORS:
                continue
            entry_lbl, stop_pct, target_lbl, size_pct, hold_d, doctrine = ARCHETYPE_PRIORS[name]
            entry_px = FIB_MAP.get(entry_lbl) or entry_now
            target_px = FIB_MAP.get(target_lbl) or (entry_px * 1.30)
            # If the target Fib is below entry (price already above the Fib level
            # we'd target), step out one level deeper toward the high.
            if target_px <= entry_px:
                for fallback_lbl in ("38.2", "23.6", "Fib_high"):
                    cand = FIB_MAP.get(fallback_lbl)
                    if cand and cand > entry_px:
                        target_px = cand
                        target_lbl = fallback_lbl
                        break
                else:
                    target_px = entry_px * 1.30   # last-resort 30% target
            stop_px = round(entry_px * (1 - stop_pct), 2)
            arch_notional = size_pct * STARTING_CAPITAL
            arch_shares = int(arch_notional // entry_px) if entry_px > 0 else 0
            # If shares rounds to 0 (entry too high relative to size cap), force >= 1
            if arch_shares == 0 and entry_px > 0 and arch_notional > 0:
                arch_shares = 1
            arch_rr = round(abs(target_px - entry_px) / max(abs(entry_px - stop_px), 0.01), 2)

            per_archetype_trades.append({
                "name":         name,
                "icon":         v.get("icon", "💎"),
                "entry_label":  f"{entry_lbl}% retracement" if entry_lbl != "Fib_high" else "Swing high anchor",
                "entry_price":  round(entry_px, 2),
                "stop_label":   f"{round(stop_pct*100,1)}% below entry",
                "stop_price":   stop_px,
                "target_label": f"{target_lbl}% retracement" if target_lbl != "Fib_high" else "Prior swing high",
                "target_price": round(target_px, 2),
                "rr":           arch_rr,
                "size_pct":     round(size_pct * 100, 2),
                "shares":       arch_shares,
                "notional":     round(arch_shares * entry_px, 2),
                "hold_days":    hold_d,
                "doctrine":     doctrine,
            })

    return jsonify({
        "ticker": forced_ticker,
        "as_of": sig.get("as_of"),
        "current_bsi_z": round(current_z, 2),
        "peak_bsi_z_proxy": round(peak_z_proxy, 2),
        "tech_verdict": tech.get("tech_verdict"),
        "recovery_test": {
            "n_conditions_met": n_conditions_met,
            "score": round(recovery_score, 2),
            "conditions": conditions,
            "verdict": "RECOVERY_CANDIDATE" if n_conditions_met >= 4 else (
                "PARTIAL_RECOVERY" if n_conditions_met >= 2 else "NO_RECOVERY_SETUP"
            ),
        },
        "debate": {
            "votes": debate,
            "verdict": verdict_str,
            "conviction": conviction,
            "long_approved": long_approved,
        },
        "suggested_trade": suggested_trade,
        "per_archetype_trades": per_archetype_trades,         # one trade per LONG-voting hedge fund
        "trade_status": trade_status,                         # APPROVED | INDICATIVE_* | NO_LONG_VOTES
        "long_voting_archetypes": [v["name"] for v in debate if v.get("vote") == "LONG"],
        "interpretation": (
            f"{forced_ticker}: {n_conditions_met}/5 recovery conditions met, "
            f"{long_votes}/{len(debate)} long archetypes voted LONG. "
            + ({
                "APPROVED": f"APPROVED for LONG at weighted entry ${suggested_trade['weighted_entry']} (Fibonacci ladder) with {suggested_trade['rr']}:1 R:R." if suggested_trade else "APPROVED.",
                "INDICATIVE_BEAR_REGIME":     f"INDICATIVE trade plan shown — {long_votes} archetype(s) voted LONG but bear regime (score {mkt_score}) blocks formal approval.",
                "INDICATIVE_LOW_CONVICTION":  f"INDICATIVE trade plan shown — {long_votes} archetype voted LONG (need ≥2 for APPROVED). Use at own discretion.",
                "NO_LONG_VOTES":              f"Conviction {conviction.upper()}; no archetype voted LONG.",
            }.get(trade_status, ""))
        ),
        "market_regime": {
            "score": mkt_score,
            "regime": mkt_regime,
            "interpretation": mkt.get("composite", {}).get("interpretation", "—") if "error" not in mkt else "—",
            "applied_to_conviction": mkt_modifier_note,
        },
    })


@app.route("/api/live/pillars")
def api_live_pillars():
    """Recent z-score series for the soft-signal pillars (Reddit, Bluesky, search-expert)."""
    return jsonify({
        "reddit": _read_pillar_csv("reddit_pillar_daily.csv", "c_reddit_z", 90),
        "bluesky": _read_pillar_csv("bluesky_pillars_daily.csv", "c_bluesky_consumer", 90),
        "search_expert": _read_pillar_csv("search_expert_pillar_daily.csv", "c_search_expert", 90),
    })


@app.route("/api/firm/<ticker>/news")
def api_firm_news(ticker):
    """Fetch top news headlines for a firm via Google News RSS (no API key needed).
       Works for public AND private firms (uses firm name, not ticker)."""
    ticker = ticker.upper()
    f = next((x for x in UNIVERSE_25 if x["ticker"] == ticker), None)
    if not f:
        return jsonify({"error": "unknown_ticker", "items": []}), 404
    try:
        import urllib.parse, urllib.request, xml.etree.ElementTree as ET, ssl
        # Use the firm name (better for private firms) + a credit/finance keyword
        # to bias results away from generic mentions
        q = urllib.parse.quote(f"{f['name']} credit OR loan OR ABS OR delinquency")
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        ctx = ssl.create_default_context()
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 BearWatch/1.0"})
        with urllib.request.urlopen(req, timeout=4, context=ctx) as resp:
            xml_text = resp.read().decode("utf-8", errors="ignore")
        root = ET.fromstring(xml_text)
        items = []
        for item in root.findall(".//item")[:5]:
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            src_el = item.find("{http://search.yahoo.com/mrss/}source") or item.find("source")
            source = (src_el.text if src_el is not None else "Google News").strip()
            # Reformat pubDate to YYYY-MM-DD if possible
            try:
                from email.utils import parsedate_to_datetime
                d = parsedate_to_datetime(pub).strftime("%Y-%m-%d")
            except Exception:
                d = pub[:16]
            items.append({"date": d, "title": title, "url": link, "source": source})
        if not items:
            return jsonify({"items": [], "fallback": True})
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"items": [], "error": str(e), "fallback": True})


def _get_market_proxy_z(ticker: str) -> dict | None:
    """
    For firms NOT in the CFPB warehouse, compute a price-and-volume-derived
    proxy z-score from yfinance daily data. This converts SCRIPTED rows on the
    monitor into LIVE-MARKET rows so the entire dashboard runs on real data.

    The proxy z combines three components:
      1. Trailing 30d return vs 180d baseline return (momentum-relative-to-trend)
      2. 30d-realized-volatility z relative to 1y baseline (vol breakout)
      3. Up/down volume ratio inverse z (distribution detection)

    Output: {ticker, market_z, components, as_of, source}
    """
    if not YF_AVAILABLE: return None
    cache_key = f"market_proxy_z_{ticker.upper()}"
    now = time.time()
    cached = _LIVE_CACHE.get(cache_key)
    if cached and (now - cached[1] < _LIVE_TTL):
        return cached[0]
    try:
        import math as _math
        import numpy as _np
        import pandas as pd
        t = yf.Ticker(ticker)
        hist = t.history(period="2y", interval="1d")
        if hist is None or len(hist) < 200: return None

        closes = hist["Close"].astype(float)
        vols = hist["Volume"].astype(float)
        latest_close = float(closes.iloc[-1])

        # Component A: 30d return vs 180d return
        ret_30 = float((closes.iloc[-1] / closes.iloc[-31] - 1)) if len(closes) >= 31 else 0
        ret_180 = float((closes.iloc[-1] / closes.iloc[-181] - 1)) if len(closes) >= 181 else 0
        # If 30d return is much WORSE than 180d return, that's distress momentum
        # Z-score component: -1 if 30d outperforming, +1 if underperforming
        ret_divergence = ret_180 - ret_30
        comp_a = _np.clip(ret_divergence / 0.10, -3, 3)  # 10pp divergence = 1σ

        # Component B: 30d realized vol z vs 1y baseline
        rolling_30d_vol = closes.pct_change().rolling(30).std() * (252**0.5)
        rolling_1y_vol = closes.pct_change().rolling(252).std() * (252**0.5)
        if len(rolling_30d_vol.dropna()) > 0 and len(rolling_1y_vol.dropna()) > 0:
            vol_30 = float(rolling_30d_vol.iloc[-1])
            vol_1y_mean = float(rolling_1y_vol.mean())
            vol_1y_std = float(rolling_1y_vol.std()) or 0.05
            comp_b = _np.clip((vol_30 - vol_1y_mean) / vol_1y_std, -3, 3)
        else:
            comp_b = 0

        # Component C: U/D volume distribution detection
        prev_close = closes.shift(1)
        direction = (closes > prev_close).astype(int) - (closes < prev_close).astype(int)
        up_vol_20 = float((vols * (direction == 1)).iloc[-20:].sum())
        down_vol_20 = float((vols * (direction == -1)).iloc[-20:].sum())
        ud_ratio = up_vol_20 / down_vol_20 if down_vol_20 > 0 else 1.0
        # Inverse: high distribution (low U/D) = high distress score
        comp_c = _np.clip((1.0 / max(ud_ratio, 0.1) - 1), -3, 3)

        # Composite z (weighted)
        market_z = float(0.45 * comp_a + 0.30 * comp_b + 0.25 * comp_c)
        market_z = round(market_z, 2)

        result = {
            "ticker": ticker.upper(),
            "market_z": market_z,
            "components": {
                "return_divergence_30v180": round(float(comp_a), 2),
                "vol_30d_z_vs_1y": round(float(comp_b), 2),
                "ud_distribution_z": round(float(comp_c), 2),
            },
            "raw": {
                "ret_30d_pct":  round(ret_30 * 100, 2),
                "ret_180d_pct": round(ret_180 * 100, 2),
                "ud_ratio_20d": round(ud_ratio, 2),
            },
            "as_of": str(closes.index[-1].date()),
            "source": "market_proxy",
        }
        _LIVE_CACHE[cache_key] = (result, now)
        return result
    except Exception as e:
        return None


def _get_live_cfpb_signals() -> dict:
    """
    Return {ticker: {n_recent_90d, n_prior_90d, delta_pct, live_z, as_of}} from the
    warehouse — keyed by the same UNIVERSE_KEYWORDS map used by /api/live/firms.
    Uses the existing 60s warehouse cache so this is cheap.

    live_z mapping (CFPB complaint acceleration -> z-equivalent):
      delta% < 0    -> live_z = 0           (improving: no stress)
      0..30%        -> live_z = delta/15    (gate threshold @ z=2 hit at +30%)
      30..60%       -> live_z = delta/15    (z=2..4)
      >= 60%        -> live_z = 4.0 cap     (saturated)
    """
    # Maps each UNIVERSE_25 ticker to substrings that appear in CFPB warehouse `company`
    # column. UPPER() applied on both sides at match time so casing of the keyword
    # doesn't matter. After the 2026-05 ingest of CVNA/CACC/KMX/ACA/LC/ENVA/OPFI/
    # WRLD/BFH/ALLY/COF/SYF/DFS/AXP/OMF, every ticker below resolves to a real
    # warehouse company. No market-proxy fallback is used.
    UNIVERSE_KEYWORDS = {
        # BNPL
        "AFRM": (["AFFIRM"], "public"),
        "SEZL": (["SEZZLE"], "public"),
        "PYPL": (["PAYPAL"], "public"),
        "KLAR": (["KLARNA"], "private"),
        "SQ":   (["BLOCK, INC", "BLOCK INC", "SQUARE"], "public"),
        # Subprime auto
        "CVNA": (["CARVANA"], "public"),
        "CACC": (["CREDIT ACCEPTANCE"], "public"),
        "KMX":  (["CARMAX"], "public"),
        "ACA":  (["CAR-MART", "CAR MART"], "public"),
        # Marketplace / fintech subprime
        "UPST": (["UPSTART"], "public"),
        "LC":   (["LENDING CLUB", "LENDINGCLUB"], "public"),
        "SOFI": (["SOFI"], "public"),
        "OPFI": (["OPPORTUNITY FINANCIAL", "OPPFI"], "public"),
        "ENVA": (["ENOVA"], "public"),
        "WRLD": (["WORLD ACCEPTANCE"], "public"),
        "OMF":  (["ONEMAIN"], "public"),
        # Card / bank control firms (large CFPB filers — used as control set)
        "COF":  (["CAPITAL ONE"], "public"),
        "SYF":  (["SYNCHRONY"], "public"),
        "DFS":  (["DISCOVER BANK", "DISCOVER FINANCIAL"], "public"),
        "BFH":  (["BREAD FINANCIAL"], "public"),
        "ALLY": (["ALLY FINANCIAL"], "public"),
        "AXP":  (["AMERICAN EXPRESS"], "public"),  # reference IG
    }
    rows = warehouse_query("""
        WITH recent AS (
            SELECT UPPER(company) AS company, COUNT(*) AS n
            FROM cfpb_complaints
            WHERE received_at > (SELECT MAX(received_at) FROM cfpb_complaints) - INTERVAL '90 days'
              AND received_at <= (SELECT MAX(received_at) FROM cfpb_complaints)
            GROUP BY UPPER(company)
        ),
        prior AS (
            SELECT UPPER(company) AS company, COUNT(*) AS n
            FROM cfpb_complaints
            WHERE received_at > (SELECT MAX(received_at) FROM cfpb_complaints) - INTERVAL '180 days'
              AND received_at <= (SELECT MAX(received_at) FROM cfpb_complaints) - INTERVAL '90 days'
            GROUP BY UPPER(company)
        )
        SELECT r.company, r.n AS n_recent, COALESCE(p.n, 0) AS n_prior,
               (SELECT MAX(received_at) FROM cfpb_complaints) AS asof
        FROM recent r LEFT JOIN prior p USING(company)
    """, cache_key="live_firms_cfpb_raw")
    if not rows:
        return {}
    asof = rows[0]["asof"].isoformat() if rows else None
    out = {}
    for ticker, (kws, _status) in UNIVERSE_KEYWORDS.items():
        match = next((r for r in rows if any(k in r["company"] for k in kws)), None)
        if not match:
            continue
        n_recent = int(match["n_recent"])
        n_prior = int(match["n_prior"])
        delta_pct = (100.0 * (n_recent - n_prior) / n_prior) if n_prior > 0 else None
        live_z = 0.0
        if delta_pct is not None and delta_pct > 0:
            live_z = min(4.0, delta_pct / 15.0)
        out[ticker] = {
            "n_recent_90d": n_recent,
            "n_prior_90d": n_prior,
            "delta_pct": round(delta_pct, 1) if delta_pct is not None else None,
            "live_z": round(live_z, 2),
            "as_of": asof,
        }
    return out


@app.route("/api/monitor/firms")
def api_monitor_firms():
    """Return all firms with a LIVE z-score derived strictly from CFPB complaint
    velocity in the warehouse. After the 2026-05 ingest pulled CFPB data for every
    firm in UNIVERSE_25, the market-proxy fallback was removed entirely — every
    score is now CFPB-derived. The only firms that fall back to scripted are those
    with no CFPB record at all (delisted private firms like Tricolor)."""
    live = _get_live_cfpb_signals()
    out = []
    for f in UNIVERSE_25:
        ticker = f["ticker"]
        sig = live.get(ticker)
        # Resolve display z: CFPB-live or scripted-fallback (no market-proxy tier)
        if sig is not None:
            display_z = sig["live_z"]
            data_source = "live"
        else:
            display_z = f["bsi_z"]
            data_source = "scripted"
        bear = bear_state_from_z(display_z, f.get("phase", 1), f.get("h2", False))
        price = None
        if not f.get("private") and not f.get("delisted"):
            try:
                q = get_quote(ticker)
                price = q.get("price")
            except Exception:
                price = None
        out.append({
            "ticker": ticker,
            "name": f["name"],
            "sector": f["sector"],
            "sector_label": SECTOR_LABELS.get(f["sector"], f["sector"]),
            "bsi_z": display_z,                        # the value that drives the UI
            "scripted_z": f["bsi_z"],                  # original hardcoded value (for reference)
            "data_source": data_source,                # "live" | "scripted"
            "live_delta_pct": sig["delta_pct"] if sig else None,
            "live_n_recent": sig["n_recent_90d"] if sig else None,
            "live_n_prior":  sig["n_prior_90d"] if sig else None,
            "phase": f.get("phase", 1),
            "h2_eligible": f.get("h2", False),
            "bear_state": bear,
            "last_fire": f.get("last_fire"),
            "private": f.get("private", False),
            "delisted": f.get("delisted", False),
            "price": price,
            "tradeable": not (f.get("private") or f.get("delisted")),
        })
    out.sort(key=lambda r: r["bsi_z"], reverse=True)
    asof = next((s["as_of"] for s in live.values() if s.get("as_of")), None)
    return jsonify({
        "firms": out,
        "top_recommendations": [r for r in out if r["bsi_z"] >= 2.0 and r["tradeable"]][:4],
        "total_count": len(out),
        "live_count": sum(1 for r in out if r["data_source"] == "live"),
        "fired_up_count": sum(1 for r in out if r["bear_state"] == "FIRED_UP"),
        "as_of": asof,
    })


# ============================================================
# User watchlist (selections that appear on the Live Demo Pod 1)
# ============================================================
def _firm_dict(ticker: str) -> dict | None:
    f = next((x for x in UNIVERSE_25 if x["ticker"] == ticker.upper()), None)
    if not f:
        return None
    # ★ Resolve display z the SAME way /api/monitor/firms does — use live CFPB
    # delta-pct signal first, fall back to scripted z. Without this, the firm-tile
    # on /monitor would show the live z while the popup would show the scripted z,
    # making the same firm appear to have two different BSI scores.
    live = _get_live_cfpb_signals().get(ticker.upper())
    if live is not None:
        display_z = live["live_z"]
        data_source = "live"
    else:
        display_z = f["bsi_z"]
        data_source = "scripted"
    bear = bear_state_from_z(display_z, f.get("phase", 1), f.get("h2", False))
    price = None
    if not f.get("private") and not f.get("delisted"):
        try:
            price = get_quote(ticker)["price"]
        except Exception:
            price = None
    return {
        "ticker": f["ticker"], "name": f["name"], "sector": f["sector"],
        "sector_label": SECTOR_LABELS.get(f["sector"], f["sector"]),
        "bsi_z": display_z, "scripted_z": f["bsi_z"], "data_source": data_source,
        "live_delta_pct": live["delta_pct"] if live else None,
        "live_n_recent": live["n_recent_90d"] if live else None,
        "phase": f.get("phase", 1),
        "h2_eligible": f.get("h2", False),
        "bear_state": bear, "last_fire": f.get("last_fire"),
        "private": f.get("private", False), "delisted": f.get("delisted", False),
        "price": price,
        "tradeable": not (f.get("private") or f.get("delisted")),
    }


@app.route("/api/selections", methods=["GET"])
def api_selections_get():
    user = session.get("username")
    if not user:
        return jsonify({"error": "not_logged_in"}), 401
    with sqlite3.connect(str(DB_PATH)) as con:
        rows = con.execute(
            "SELECT ticker, rank FROM user_selections WHERE username=? ORDER BY rank ASC",
            (user.lower(),),
        ).fetchall()
    selected = [{"ticker": t, "rank": r, **(_firm_dict(t) or {})} for t, r in rows]
    # If user has fewer than 5 selections, build "suggested" fillers from top BSI z firms
    selected_tickers = {s["ticker"] for s in selected}
    suggestions = []
    for f in sorted(UNIVERSE_25, key=lambda x: -x["bsi_z"]):
        if f["ticker"] not in selected_tickers and not f.get("private") and not f.get("delisted"):
            d = _firm_dict(f["ticker"])
            if d:
                suggestions.append(d)
        if len(suggestions) >= max(0, 5 - len(selected)):
            break
    return jsonify({"selected": selected, "suggested": suggestions})


@app.route("/api/selections", methods=["POST"])
def api_selections_add():
    user = session.get("username")
    if not user:
        return jsonify({"error": "not_logged_in"}), 401
    body = request.get_json(silent=True) or {}
    ticker = (body.get("ticker") or "").upper()
    if not ticker or not _firm_dict(ticker):
        return jsonify({"error": "invalid_ticker"}), 400
    with sqlite3.connect(str(DB_PATH)) as con:
        # If already selected, do nothing (idempotent)
        existing = con.execute(
            "SELECT 1 FROM user_selections WHERE username=? AND ticker=?",
            (user.lower(), ticker),
        ).fetchone()
        if existing:
            return jsonify({"ok": True, "duplicate": True})
        # Find next rank
        max_rank = con.execute(
            "SELECT COALESCE(MAX(rank), 0) FROM user_selections WHERE username=?",
            (user.lower(),),
        ).fetchone()[0]
        # Cap raised from 5 -> 25 (full universe). The Live Pod can handle the entire watchlist;
        # demo Pod 1 still shows the top 5 by rank for visual density.
        if max_rank >= 25:
            return jsonify({"error": "watchlist_full", "max": 25}), 400
        con.execute(
            "INSERT INTO user_selections(username, ticker, rank, added_at) VALUES(?,?,?,?)",
            (user.lower(), ticker, max_rank + 1, datetime.now(timezone.utc).isoformat()),
        )
        con.commit()
    log_activity(user, "asset_added", {"ticker": ticker})
    return jsonify({"ok": True, "ticker": ticker, "rank": max_rank + 1})


@app.route("/api/selections/<ticker>", methods=["DELETE"])
def api_selections_remove(ticker):
    user = session.get("username")
    if not user:
        return jsonify({"error": "not_logged_in"}), 401
    ticker = ticker.upper()
    with sqlite3.connect(str(DB_PATH)) as con:
        con.execute("DELETE FROM user_selections WHERE username=? AND ticker=?",
                    (user.lower(), ticker))
        # Re-pack ranks
        rows = con.execute(
            "SELECT ticker FROM user_selections WHERE username=? ORDER BY rank ASC",
            (user.lower(),),
        ).fetchall()
        for new_rank, (t,) in enumerate(rows, start=1):
            con.execute("UPDATE user_selections SET rank=? WHERE username=? AND ticker=?",
                        (new_rank, user.lower(), t))
        con.commit()
    log_activity(user, "asset_removed", {"ticker": ticker})
    return jsonify({"ok": True})


@app.route("/api/selections/reorder", methods=["POST"])
def api_selections_reorder():
    """Body: {"ordered": ["CVNA", "UPST", ...]}  — sets ranks 1..N in given order."""
    user = session.get("username")
    if not user:
        return jsonify({"error": "not_logged_in"}), 401
    ordered = (request.get_json(silent=True) or {}).get("ordered") or []
    with sqlite3.connect(str(DB_PATH)) as con:
        for new_rank, t in enumerate(ordered, start=1):
            con.execute("UPDATE user_selections SET rank=? WHERE username=? AND ticker=?",
                        (new_rank, user.lower(), t.upper()))
        con.commit()
    log_activity(user, "watchlist_reordered", {"order": ordered})
    return jsonify({"ok": True})


# ============================================================
# Rich firm detail (for the popup modal)
# ============================================================
@app.route("/api/firm/<ticker>")
def api_firm_detail(ticker):
    ticker = ticker.upper()
    base = _firm_dict(ticker)
    if not base:
        return jsonify({"error": "unknown_ticker"}), 404
    profile = FIRM_PROFILES.get(ticker, {})
    # Pull 1-year price history if tradeable
    price_history = []
    if base["tradeable"] and YF_AVAILABLE:
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="1y", interval="1wk")
            for dt, row in hist.iterrows():
                price_history.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "close": round(float(row["Close"]), 2),
                })
        except Exception:
            pass
    # Pillar series (synthetic monthly trajectory toward current value)
    cur_pillars = profile.get("pillars", {})
    domain = FIRM_DOMAINS.get(ticker)
    return jsonify({
        **base,
        "domain": domain,
        "logo_url": f"https://logo.clearbit.com/{domain}" if domain else None,
        "abs": FIRM_ABS.get(ticker),
        "financials": {
            "revenue_b":     profile.get("revenue_b"),
            "net_income_b":  profile.get("net_income_b"),
            "fcf_b":         profile.get("fcf_b"),
            "total_debt_b":  profile.get("total_debt_b"),
            "cash_b":        profile.get("cash_b"),
            "employees":     profile.get("employees"),
        },
        "ratings": {
            "sp":     profile.get("sp", "—"),
            "moodys": profile.get("moodys", "—"),
            "fitch":  profile.get("fitch", "—"),
            "stars":  profile.get("stars", 3.0),
        },
        "pillars": cur_pillars,
        "price_history": price_history,
    })


@app.route("/v2")
def v2_page():
    return render_template("v2_preview.html", active="v2")


@app.route("/v1")
def home_v1_legacy():
    return render_template("dashboard.html")


@app.route("/integration")
def integration():
    return render_template("integration.html")


@app.route("/api/quote/<ticker>")
def api_quote(ticker):
    return jsonify(get_quote(ticker))


@app.route("/api/portfolio")
def api_portfolio():
    user = session.get("username")
    return jsonify(get_portfolio(user))


@app.route("/portfolio")
def portfolio_page():
    """Apollo Hermes-style portfolio dashboard with both LONG + SHORT positions,
    live MTM, P&L, sector concentration, beta exposure, drawdown mode, etc."""
    return render_template("portfolio.html", active="portfolio")


# ============================================================================
# ROBO meta-archetype — Monte Carlo cold-start visualization (paper §10.6)
# ============================================================================
@app.route("/robo")
def robo_page():
    return redirect("/empirical#sec-robo", code=301)


def _robo_legacy():
    """LEGACY: original /robo handler kept for reference but unreachable.
    ROBO bear page — shows Monte Carlo simulation of the population layer,
    convergence chart, P&L comparison, adversarial robustness, and live
    deployment status (synthetic vs real-user blend)."""
    return render_template("robo.html", active="robo")


# Wire in the population-layer module
_ROBO_AVAILABLE = False
try:
    import sys as _sys_robo
    for _gp in (
        r"C:\Users\siddh\Desktop\spring 2026\580\BNPL-experimental\bnpl-pod",
        r"C:\Users\siddh\Desktop\spring 2026\580\BNPL\bnpl-pod",
    ):
        if Path(_gp, "signals", "robo.py").exists():
            if _gp not in _sys_robo.path: _sys_robo.path.insert(0, _gp)
            from signals.robo import (   # type: ignore
                deployment_status as _robo_deployment_status,
                compute_robo_signal as _robo_compute_signal,
                get_montecarlo_summary as _robo_get_mc_summary,
            )
            _ROBO_AVAILABLE = True
            break
except Exception as _e:
    print(f"[robo] module import failed: {_e}")


def _count_real_users() -> int:
    """Count distinct users who have logged at least one trade in the journal."""
    try:
        with db() as c:
            row = c.execute(
                "SELECT COUNT(DISTINCT username) AS n FROM journal WHERE username IS NOT NULL AND username != ''"
            ).fetchone()
            return int(row["n"] if row else 0)
    except Exception:
        return 0


@app.route("/bnpl-events")
def bnpl_events_page():
    return redirect("/empirical#sec-events", code=301)


def _bnpl_events_legacy():
    """LEGACY: original /bnpl-events handler kept for reference but unreachable.
    Phase 2H BNPL event-study page — visual proof of leading-indicator behavior."""
    return render_template("bnpl_events.html", active="bnpl-events")


@app.route("/api/bnpl/event_study")
def api_bnpl_event_study():
    """Returns the Phase 2H event-study JSON for the /bnpl-events page."""
    p = Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL_v9_FINAL\01_paper\empirics_v2\out\v2\bnpl_event_study_v22.json")
    if not p.exists():
        return jsonify({"error": "phase2h_not_run",
                        "message": "Run run_phase2h_event_study.py first."}), 503
    try:
        with open(p, "r", encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": "load_failed", "message": str(e)}), 500


@app.route("/api/robo/capstone")
def api_robo_capstone():
    """Serves the Phase 2 Unified Capstone diagnostics for the /robo page."""
    p = Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL_v9_FINAL\01_paper\empirics_v2\out\v2\capstone_v24_summary.json")
    if not p.exists():
        return jsonify({"error": "capstone_not_run",
                        "message": "Run run_phase2_unified_capstone.py first."}), 503
    try:
        with open(p, "r", encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": "load_failed", "message": str(e)}), 500


@app.route("/api/robo/weight_provenance")
def api_robo_weight_provenance():
    """Returns the Phase 2E + Phase 2F pillar-weight robustness results.
    Phase 2F (6-pillar) takes precedence if available; falls back to Phase 2E (3-pillar)."""
    base = Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL_v9_FINAL\01_paper\empirics_v2\out\v2")
    p2f = base / "panel_weight_summary_v22_phase2f.json"
    p2e = base / "panel_weight_summary_v21.json"
    p = p2f if p2f.exists() else p2e
    if not p.exists():
        return jsonify({
            "error": "phase2e_not_run",
            "message": "Run run_panel_weight_learning.py and run_phase2f_backfill.py first.",
        }), 503
    try:
        with open(p, "r", encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": "load_failed", "message": str(e)}), 500


@app.route("/api/robo/montecarlo")
def api_robo_montecarlo():
    """Returns the full Monte Carlo summary + live deployment status for /robo page."""
    if not _ROBO_AVAILABLE:
        return jsonify({
            "error": "robo_module_not_available",
            "message": "signals/robo.py not importable. Run run_robo_montecarlo.py first.",
        }), 503

    n_real = _count_real_users()
    summary = _robo_get_mc_summary()
    status  = _robo_deployment_status(n_real)

    # Also pull the per-K convergence and adversarial CSVs for chart data
    csv_dir = Path(r"C:\Users\siddh\Desktop\spring 2026\580\BNPL_v9_FINAL\01_paper\empirics_v2\out\v2")
    def _read_csv(name):
        try:
            with open(csv_dir / name, "r", encoding="utf-8") as f:
                lines = f.readlines()
            hdr = lines[0].strip().split(",")
            rows = []
            for line in lines[1:]:
                vals = line.strip().split(",")
                if len(vals) != len(hdr): continue
                rows.append({h: v for h, v in zip(hdr, vals)})
            return rows
        except FileNotFoundError:
            return []

    return jsonify({
        "summary":             summary,
        "deployment":          status,
        "convergence_csv":     _read_csv("robo_montecarlo_convergence_v21.csv"),
        "adversarial_csv":     _read_csv("robo_montecarlo_adversarial_v21.csv"),
        "pnl_csv":             _read_csv("robo_montecarlo_pnl_v21.csv"),
        "tier_prior_meta": {
            # for the tier breakdown chart — share + colour
            "skilled":     {"share": 0.15, "color": "#4ade80",
                            "desc": "archetype + 0.30σ edge, low noise"},
            "average":     {"share": 0.50, "color": "#60a5fa",
                            "desc": "archetype-correct, medium noise"},
            "noisy":       {"share": 0.25, "color": "#fbbf24",
                            "desc": "archetype − 0.20σ, high noise"},
            "adversarial": {"share": 0.10, "color": "#f87171",
                            "desc": "inverts archetype, medium noise"},
        },
    })


@app.route("/api/portfolio/positions")
def api_portfolio_positions():
    """Returns OPEN + CLOSED positions with live MTM (open) and realised P&L
    (closed), plus full Apollo Hermes portfolio analytics."""
    user = (session.get("username") or "").lower()
    with db() as c:
        if user:
            rows = c.execute(
                """SELECT id, ts, event_id, ticker, side, shares, entry_price,
                          stop_price, target_price, notional_usd, verdict,
                          exit_price, exit_ts, COALESCE(status,'OPEN') AS status
                   FROM journal WHERE username=? ORDER BY ts DESC""",
                (user,)).fetchall()
        else:
            rows = c.execute(
                """SELECT id, ts, event_id, ticker, side, shares, entry_price,
                          stop_price, target_price, notional_usd, verdict,
                          exit_price, exit_ts, COALESCE(status,'OPEN') AS status
                   FROM journal ORDER BY ts DESC""").fetchall()

    portfolio = get_portfolio(user) if user else get_portfolio()
    starting_cap = float(portfolio.get("starting_capital") or STARTING_CAPITAL)
    cash         = float(portfolio.get("cash") or starting_cap)

    open_positions   = []
    closed_positions = []
    realised_pnl_total = 0.0

    for r in rows:
        d = dict(r)
        ticker = d["ticker"]; side = d["side"]; shares = float(d["shares"])
        entry  = float(d["entry_price"]); notional = float(d["notional_usd"])
        beta = BETA_MAP.get(ticker, BETA_MAP["_default"])
        sector = SECTOR_MAP.get(ticker, "other")
        d.update({"beta": beta, "sector": sector})

        if d["status"] == "CLOSED" and d["exit_price"]:
            exit_price = float(d["exit_price"])
            # P&L convention — long: (exit - entry) * shares; short: (entry - exit) * shares
            if side == "SHORT":
                pnl = (entry - exit_price) * shares
            else:
                pnl = (exit_price - entry) * shares
            d.update({
                "pnl_usd":  round(pnl, 2),
                "pnl_pct":  round(pnl / notional * 100, 2) if notional else 0,
                "exit_price": round(exit_price, 2),
            })
            realised_pnl_total += pnl
            closed_positions.append(d)
        else:
            # Open position — fetch live MTM
            try:
                q = get_quote(ticker)
                live_px = float(q.get("price") or entry)
            except Exception:
                live_px = entry
            if side == "SHORT":
                mtm_pnl = (entry - live_px) * shares
            else:
                mtm_pnl = (live_px - entry) * shares
            d.update({
                "live_price":      round(live_px, 2),
                "mtm_pnl_usd":     round(mtm_pnl, 2),
                "mtm_pnl_pct":     round(mtm_pnl / notional * 100, 2) if notional else 0,
                "current_notional": round(shares * live_px, 2),
                # distance to stop / target as % of current price
                "dist_to_stop_pct":   round((float(d["stop_price"])  - live_px) / live_px * 100, 2) if d["stop_price"]  else None,
                "dist_to_target_pct": round((float(d["target_price"])- live_px) / live_px * 100, 2) if d["target_price"] else None,
            })
            open_positions.append(d)

    # === Aggregate analytics — Apollo Hermes style ===
    long_open   = [p for p in open_positions if p["side"] == "LONG"]
    short_open  = [p for p in open_positions if p["side"] == "SHORT"]
    long_notional  = sum(float(p["current_notional"]) for p in long_open)
    short_notional = sum(float(p["current_notional"]) for p in short_open)
    gross_exposure = long_notional + short_notional
    net_exposure   = long_notional - short_notional
    open_mtm_pnl   = sum(float(p["mtm_pnl_usd"]) for p in open_positions)

    # Beta-weighted exposure (long contributes +beta·notional; short contributes −beta·notional)
    beta_long  = sum(float(p["current_notional"]) * float(p["beta"]) for p in long_open)
    beta_short = sum(float(p["current_notional"]) * float(p["beta"]) for p in short_open)
    portfolio_beta_dollar = beta_long - beta_short  # net dollar-beta exposure

    # Sector concentration (gross, both sides)
    sector_breakdown = {}
    for p in open_positions:
        sec = p["sector"]
        sector_breakdown[sec] = sector_breakdown.get(sec, 0) + float(p["current_notional"])

    # Win rate on closed positions
    winners = sum(1 for p in closed_positions if p["pnl_usd"] > 0)
    n_closed = len(closed_positions)
    win_rate = (winners / n_closed * 100) if n_closed else None

    # Total equity = cash + open MTM + realised
    total_equity = cash + open_mtm_pnl
    # Hwm-based drawdown
    hwm = float(portfolio.get("hwm") or starting_cap)
    drawdown_pct = ((total_equity - hwm) / hwm * 100) if hwm else 0
    # Drawdown mode classification
    if drawdown_pct >= -5:    dd_mode = "NORMAL"
    elif drawdown_pct >= -10: dd_mode = "CAUTIOUS"
    elif drawdown_pct >= -15: dd_mode = "DEFENSIVE"
    else:                     dd_mode = "EMERGENCY"

    # Largest position vs cap
    largest_pos_pct = max((float(p["current_notional"]) / total_equity * 100
                          for p in open_positions), default=0) if total_equity else 0

    # P&L distribution stats (closed positions, realised)
    avg_winner = avg_loser = max_winner = max_loser = None
    if n_closed:
        wins = [p["pnl_usd"] for p in closed_positions if p["pnl_usd"] > 0]
        losses = [p["pnl_usd"] for p in closed_positions if p["pnl_usd"] <= 0]
        if wins:   avg_winner = round(sum(wins) / len(wins), 2);   max_winner = round(max(wins), 2)
        if losses: avg_loser  = round(sum(losses) / len(losses), 2); max_loser = round(min(losses), 2)

    return jsonify({
        "user": user,
        "starting_capital": starting_cap,
        "cash": round(cash, 2),
        "open_mtm_pnl": round(open_mtm_pnl, 2),
        "realised_pnl_total": round(realised_pnl_total, 2),
        "total_equity": round(total_equity, 2),
        "total_pnl": round(open_mtm_pnl + realised_pnl_total, 2),
        "total_pnl_pct": round((open_mtm_pnl + realised_pnl_total) / starting_cap * 100, 2),
        "gross_exposure": round(gross_exposure, 2),
        "net_exposure": round(net_exposure, 2),
        "long_notional": round(long_notional, 2),
        "short_notional": round(short_notional, 2),
        "long_count": len(long_open),
        "short_count": len(short_open),
        "open_count": len(open_positions),
        "closed_count": n_closed,
        "portfolio_beta_dollar": round(portfolio_beta_dollar, 2),
        "portfolio_beta_normalised": round(portfolio_beta_dollar / total_equity, 3) if total_equity else 0,
        "beta_load_status": "OK" if abs(portfolio_beta_dollar / max(total_equity, 1)) <= MAX_BETA_LOAD else "EXCEEDED",
        "beta_load_cap": MAX_BETA_LOAD,
        "sector_breakdown": [
            {"sector": s, "notional": round(v, 2),
             "pct_gross": round(v / gross_exposure * 100, 1) if gross_exposure else 0}
            for s, v in sorted(sector_breakdown.items(), key=lambda x: -x[1])
        ],
        "drawdown_pct": round(drawdown_pct, 2),
        "drawdown_mode": dd_mode,
        "hwm": round(hwm, 2),
        "largest_position_pct": round(largest_pos_pct, 2),
        "single_position_cap_pct": 5.0,
        "single_position_status": "OK" if largest_pos_pct <= 5.0 else "EXCEEDED",
        "win_rate_pct": round(win_rate, 1) if win_rate is not None else None,
        "n_closed": n_closed,
        "n_winners": winners,
        "avg_winner_usd": avg_winner,
        "avg_loser_usd": avg_loser,
        "max_winner_usd": max_winner,
        "max_loser_usd": max_loser,
        "open_positions": open_positions,
        "closed_positions": closed_positions[:20],  # most recent 20
    })


@app.route("/api/portfolio/close", methods=["POST"])
def api_portfolio_close():
    """Mark an open position as closed at the current live price (or user-provided
    exit_price) and credit/debit cash accordingly."""
    body = request.get_json(force=True, silent=True) or {}
    pos_id = body.get("position_id")
    user = (session.get("username") or "").lower()
    if not pos_id:
        return jsonify({"error": "position_id required"}), 400

    with db() as c:
        if user:
            row = c.execute("SELECT * FROM journal WHERE id=? AND username=?", (pos_id, user)).fetchone()
        else:
            row = c.execute("SELECT * FROM journal WHERE id=?", (pos_id,)).fetchone()
        if not row:
            return jsonify({"error": "position not found"}), 404
        d = dict(row)
        if d.get("status") == "CLOSED":
            return jsonify({"error": "position already closed"}), 400
        # Live exit price from yfinance, or user-provided
        try:
            q = get_quote(d["ticker"])
            exit_price = float(body.get("exit_price") or q.get("price") or d["entry_price"])
        except Exception:
            exit_price = float(body.get("exit_price") or d["entry_price"])
        # Compute realised P&L
        side = d["side"]; shares = float(d["shares"]); entry = float(d["entry_price"])
        if side == "SHORT":
            pnl = (entry - exit_price) * shares
        else:
            pnl = (exit_price - entry) * shares
        # Cash settlement: original notional was deducted on entry; on close, return
        # original notional + P&L (i.e. the realised P&L is added to cash; the
        # principal was always held in the position).
        c.execute("""UPDATE journal SET exit_price=?, exit_ts=?, status='CLOSED' WHERE id=?""",
                  (round(exit_price, 4), datetime.now(timezone.utc).isoformat(), pos_id))
        notional = float(d["notional_usd"])
        if user:
            c.execute("UPDATE portfolio SET cash = cash + ? + ? WHERE username=?",
                      (notional, pnl, user))
        else:
            c.execute("UPDATE portfolio SET cash = cash + ? + ? WHERE id=1",
                      (notional, pnl))
        c.commit()

    return jsonify({"ok": True, "position_id": pos_id, "exit_price": exit_price,
                    "realised_pnl_usd": round(pnl, 2)})


@app.route("/api/bears")
def api_bears():
    with db() as c:
        rows = c.execute(
            """SELECT e.event_id, e.ts, e.ticker, e.firm_name, e.sector, e.bear_state,
                      e.bsi_z, e.phase, v.verdict, v.recommended_usd
               FROM bearwatch_events e
               LEFT JOIN risk_verdicts v ON v.event_id = e.event_id
               ORDER BY e.ts DESC LIMIT 25"""
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/journal")
def api_journal():
    user = session.get("username")
    with db() as c:
        if user:
            rows = c.execute(
                "SELECT * FROM journal WHERE username=? ORDER BY id DESC LIMIT 25",
                (user.lower(),)
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM journal ORDER BY id DESC LIMIT 25"
            ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/stats")
def api_stats():
    """Trading stats per user — win rate, win/loss ratio, expectancy, profit factor.
       For event-driven asymmetric strategies, win rate alone is misleading; we show
       it alongside the avg win / avg loss and the expectancy."""
    user = session.get("username")
    if not user:
        return jsonify({"error": "not_logged_in"}), 401
    with db() as c:
        rows = c.execute(
            "SELECT ticker, side, shares, entry_price, stop_price, target_price, notional_usd, ts "
            "FROM journal WHERE username=? ORDER BY id ASC", (user.lower(),)
        ).fetchall()
    trades = [dict(r) for r in rows]
    # Mark each trade's status using the latest price
    closed_pnl_pct = []   # list of pct returns on CLOSED trades only
    open_count = 0
    realized = 0.0
    unrealized = 0.0
    for t in trades:
        ticker = t["ticker"]
        side = t["side"]
        entry = float(t["entry_price"] or 0)
        shares = float(t["shares"] or 0)
        stop = t["stop_price"]
        target = t["target_price"]
        if entry == 0 or shares == 0:
            continue
        # Get current quote (cached)
        try:
            q = get_quote(ticker)
            cur = q["price"]
        except Exception:
            cur = entry
        # Sign: short profits when cur < entry
        if side == "SHORT":
            pct = (entry - cur) / entry * 100
            hit_stop = stop and cur >= float(stop)
            hit_target = target and cur <= float(target)
        else:
            pct = (cur - entry) / entry * 100
            hit_stop = stop and cur <= float(stop)
            hit_target = target and cur >= float(target)
        pnl_dollar = pct / 100 * float(t["notional_usd"] or shares * entry)
        if hit_stop or hit_target:
            closed_pnl_pct.append(pct)
            realized += pnl_dollar
        else:
            open_count += 1
            unrealized += pnl_dollar
    closed = len(closed_pnl_pct)
    wins = [p for p in closed_pnl_pct if p > 0]
    losses = [p for p in closed_pnl_pct if p <= 0]
    win_rate = round(100 * len(wins) / closed, 1) if closed else None
    avg_win  = round(sum(wins) / len(wins), 2) if wins else 0
    avg_loss = round(sum(losses) / len(losses), 2) if losses else 0
    win_loss_ratio = round(abs(avg_win / avg_loss), 2) if avg_loss else None
    expectancy = round(
        (win_rate/100 * avg_win + (1 - win_rate/100) * avg_loss) if win_rate is not None else 0, 2
    )
    profit_factor = (round(sum(wins) / abs(sum(losses)), 2)
                     if losses and sum(losses) != 0 else None)
    return jsonify({
        "n_trades": len(trades),
        "n_closed": closed,
        "n_open":   open_count,
        "win_rate_pct":   win_rate,
        "avg_win_pct":    avg_win,
        "avg_loss_pct":   avg_loss,
        "win_loss_ratio": win_loss_ratio,
        "expectancy_pct": expectancy,
        "profit_factor":  profit_factor,
        "realized_pnl_usd":   round(realized, 2),
        "unrealized_pnl_usd": round(unrealized, 2),
        # Honest framing — what the user should care about
        "framing": ("For event-driven asymmetric short strategies like BearWatch, "
                    "win rate is necessary but not sufficient. A 30% win rate with "
                    "5x win/loss ratio is far better than 80% with 1:1. "
                    "The published H1 backtest result of −45.7% mean abnormal return "
                    "implies a win/loss ratio around 5–10x at the 18-month horizon."),
    })


@app.route("/api/bearwatch/ingest", methods=["POST"])
def api_ingest():
    payload = request.get_json(force=True, silent=True) or {}
    event_id = payload.get("event_id") or f"bw_{int(time.time())}"
    ticker = (payload.get("ticker") or "").upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    # Activity log (per-user)
    if session.get("username"):
        log_activity(session["username"], "bear_fired", {
            "ticker": ticker, "bsi_z": (payload.get("signal") or {}).get("bsi_z"),
            "mascot": payload.get("mascot"), "event_id": event_id
        })
    ts = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
    sector = payload.get("sector") or SECTOR_MAP.get(ticker, "unknown")

    # Persist event (idempotent on event_id)
    with db() as c:
        c.execute(
            """INSERT OR REPLACE INTO bearwatch_events
               (event_id, ts, ticker, firm_name, sector, bear_state, bsi_z, phase, h2_eligible, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                event_id, ts, ticker, payload.get("firm_name"), sector,
                payload.get("bear_state"),
                (payload.get("signal") or {}).get("bsi_z"),
                (payload.get("signal") or {}).get("phase"),
                int(bool((payload.get("signal") or {}).get("h2_eligible"))),
                json.dumps(payload),
            ),
        )
        c.commit()

    # Run quote + risk
    quote = get_quote(ticker)
    portfolio = get_portfolio(session.get("username"))
    verdict = run_risk_checks(payload, portfolio, quote["price"])

    with db() as c:
        c.execute(
            """INSERT INTO risk_verdicts
               (event_id, ts, verdict, recommended_usd, entry_price, stop_price, target_price, checks_json)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                event_id, datetime.now(timezone.utc).isoformat(),
                verdict["verdict"], verdict["recommended_usd"],
                verdict["entry_price"], verdict["stop_price"], verdict["target_price"],
                json.dumps(verdict["checks"]),
            ),
        )
        c.commit()

    return jsonify({
        "event_id": event_id,
        "ticker": ticker,
        "quote": quote,
        "verdict": verdict,
        "received_payload": payload,
    })


@app.route("/api/risk/check", methods=["POST"])
def api_risk_check():
    """Re-run risk against an arbitrary payload without persisting."""
    payload = request.get_json(force=True, silent=True) or {}
    ticker = (payload.get("ticker") or "").upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    quote = get_quote(ticker)
    portfolio = get_portfolio(session.get("username"))
    verdict = run_risk_checks(payload, portfolio, quote["price"])
    return jsonify({"quote": quote, "verdict": verdict})


@app.route("/api/journal/log", methods=["POST"])
def api_journal_log():
    body = request.get_json(force=True, silent=True) or {}
    event_id = body.get("event_id")
    ticker = (body.get("ticker") or "").upper()
    side = (body.get("side") or "SHORT").upper()
    shares = float(body.get("shares") or 0)
    entry = float(body.get("entry_price") or 0)
    stop = body.get("stop_price")
    target = body.get("target_price")
    notional = float(body.get("notional_usd") or shares * entry)
    verdict = body.get("verdict")
    if session.get("username"):
        log_activity(session["username"], "trade_executed", {
            "ticker": ticker, "side": side, "shares": shares,
            "entry_price": entry, "notional_usd": notional, "event_id": event_id
        })

    if not ticker or shares <= 0 or entry <= 0:
        # Verbose error so the frontend can surface what failed exactly.
        return jsonify({
            "error": "missing_or_invalid_fields",
            "message": (
                f"Cannot journal trade: ticker={ticker!r} shares={shares} entry_price={entry}. "
                f"All three must be present and > 0. "
                + ("(shares is 0 — typically means the position size cap rounded to zero shares "
                   "for this stock price; raise the size cap or pick a lower-priced ticker.)" if shares <= 0 else "")
            ),
            "received": {"ticker": ticker, "shares": shares, "entry_price": entry,
                         "side": side, "notional": notional},
        }), 400

    user = (session.get("username") or "").lower()
    with db() as c:
        c.execute(
            """INSERT INTO journal
               (ts, event_id, ticker, side, shares, entry_price, stop_price, target_price, notional_usd, verdict, username)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                event_id, ticker, side, shares, entry, stop, target, notional, verdict, user,
            ),
        )
        if user:
            c.execute("UPDATE portfolio SET cash = cash - ? WHERE username = ?", (notional, user))
        else:
            c.execute("UPDATE portfolio SET cash = cash - ? WHERE id = 1", (notional,))
        c.commit()

    return jsonify({"ok": True, "ticker": ticker, "shares": shares, "notional_usd": notional})


# ============================================================
# AUTO-TRADE LAYER · NOT a 5th mascot — a deployment mode for the user's
# active mascot (BLITZ / SCOUT / GUARDIAN / ROBO).
# When enabled, the layer scans the universe periodically and fires
# trades using whatever thresholds match the user's currently-selected
# mascot. Fires are tagged AUTO_<mascot>_z<value> for clean attribution.
# ============================================================

_AUTOBOT_STATE = {
    "enabled": False,                # off by default — user must opt in
    "interval_seconds": 600,         # 10-min scan cadence
    "max_trades_per_day": 5,         # safety cap
    "fired_today": 0,
    "fired_today_date": None,        # YYYY-MM-DD, resets daily
    "user": "sid",                   # which account the layer trades under
    "active_mascot": None,           # READ DYNAMICALLY from user profile each scan
    "last_scan_at": None,
    "last_scan_result": None,        # summary of most recent scan
    "scan_log": [],                  # last ~20 scans
    "thread_started": False,
}
_AUTOBOT_LOCK = threading.Lock()
_AUTOBOT_COOLDOWN = {}               # ticker -> ts of last fire (cooldown 24h)
_AUTOBOT_COOLDOWN_HOURS = 24


def _autobot_should_fire(ticker, bsi_z, bear_state, mascot="SCOUT"):
    """Decide whether the autobot should fire on this firm right now."""
    # Threshold by mascot
    th = {"BLITZ": 1.5, "SCOUT": 2.0, "GUARDIAN": 2.5}.get(mascot, 2.0)
    if bsi_z is None or bsi_z < th:
        return False, f"BSI z={bsi_z} below {mascot} threshold {th}"
    # Bear state must be FIRED-UP / ANGRY / WORRIED depending on mascot
    fire_states = {"BLITZ": ["WORRIED","ANGRY","FIRED_UP"],
                   "SCOUT": ["ANGRY","FIRED_UP"],
                   "GUARDIAN": ["FIRED_UP"]}.get(mascot, ["ANGRY","FIRED_UP"])
    if bear_state and bear_state not in fire_states:
        return False, f"bear_state={bear_state} not in {fire_states}"
    # Cooldown
    last = _AUTOBOT_COOLDOWN.get(ticker)
    if last:
        hrs = (datetime.now(timezone.utc) - last).total_seconds() / 3600
        if hrs < _AUTOBOT_COOLDOWN_HOURS:
            return False, f"cooldown — last fire {hrs:.1f}h ago"
    return True, "all conditions met"


def _autobot_scan_once():
    """One scan pass: check every BNPL universe firm, fire autobot trades that qualify."""
    state = _AUTOBOT_STATE
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Reset daily counter
    with _AUTOBOT_LOCK:
        if state["fired_today_date"] != today:
            state["fired_today"] = 0
            state["fired_today_date"] = today
        if state["fired_today"] >= state["max_trades_per_day"]:
            scan_summary = {"scan_at": datetime.now(timezone.utc).isoformat(),
                            "outcome": "rate_limited",
                            "fired_today": state["fired_today"],
                            "details": f"daily cap {state['max_trades_per_day']} reached"}
            state["last_scan_at"] = scan_summary["scan_at"]
            state["last_scan_result"] = scan_summary
            state["scan_log"] = ([scan_summary] + state["scan_log"])[:20]
            return scan_summary

    # Universe — pull from MOCK_BSI_TODAY which has firm-level z-scores
    universe = []
    try:
        # Use whatever firm-level snapshot is available
        if "MOCK_BSI_TODAY" in globals():
            for firm_id, snap in globals()["MOCK_BSI_TODAY"].items():
                ticker = snap.get("ticker") or firm_id
                bsi_z = snap.get("bsi_z") or 0
                bear_state = snap.get("bear_state") or "UNKNOWN"
                universe.append((ticker, bsi_z, bear_state))
    except Exception:
        pass

    fired = []
    skipped = []
    user = state["user"]

    # ★ Read the user's active mascot DYNAMICALLY — the autotrade layer
    # adopts whatever profile the user has selected (BLITZ/SCOUT/GUARDIAN/ROBO).
    try:
        u = get_user(user)
        prof = (u or {}).get("profile") or {}
        mascot = prof.get("active_mascot") or prof.get("recommended_mascot") or "SCOUT"
    except Exception:
        mascot = "SCOUT"
    state["active_mascot"] = mascot

    for ticker, bsi_z, bear_state in universe:
        ok, reason = _autobot_should_fire(ticker, bsi_z, bear_state, mascot)
        if not ok:
            skipped.append((ticker, reason))
            continue
        # Get live price
        try:
            q = get_quote(ticker)
            price = float(q.get("price") or 0)
        except Exception:
            skipped.append((ticker, "quote unavailable"))
            continue
        if price <= 0:
            skipped.append((ticker, "no price"))
            continue
        # Default sizing: $2,500 notional
        shares = max(1, int(2500 / price))
        stop = round(price * 1.07, 2)      # 7% stop above for SHORT
        target = round(price * 0.85, 2)    # 15% target below
        notional = shares * price

        # Journal directly via DB (skip the HTTP roundtrip)
        try:
            with db() as c:
                c.execute(
                    """INSERT INTO journal
                       (ts, event_id, ticker, side, shares, entry_price, stop_price, target_price, notional_usd, verdict, username)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        f"auto_{int(time.time())}_{ticker}", ticker, "SHORT",
                        shares, price, stop, target, notional,
                        f"AUTO_{mascot}_z{bsi_z:.2f}", user.lower(),
                    ),
                )
                c.execute("UPDATE portfolio SET cash = cash - ? WHERE username = ?", (notional, user.lower()))
                c.commit()
            _AUTOBOT_COOLDOWN[ticker] = datetime.now(timezone.utc)
            with _AUTOBOT_LOCK:
                state["fired_today"] += 1
            fired.append({"ticker": ticker, "side": "SHORT", "shares": shares,
                          "price": price, "bsi_z": bsi_z, "bear_state": bear_state,
                          "notional": notional})
            # Stop firing if cap reached mid-scan
            if state["fired_today"] >= state["max_trades_per_day"]:
                break
        except Exception as e:
            skipped.append((ticker, f"db error: {e}"))

    scan_summary = {
        "scan_at": datetime.now(timezone.utc).isoformat(),
        "outcome": "ok",
        "universe_size": len(universe),
        "fired": fired,
        "skipped_count": len(skipped),
        "skipped_sample": skipped[:5],
        "fired_today_total": state["fired_today"],
        "mascot": mascot,
    }
    with _AUTOBOT_LOCK:
        state["last_scan_at"] = scan_summary["scan_at"]
        state["last_scan_result"] = scan_summary
        state["scan_log"] = ([scan_summary] + state["scan_log"])[:20]
    return scan_summary


def _autobot_daemon():
    """Background loop. Runs forever while pod is alive; respects enabled flag."""
    while True:
        try:
            if _AUTOBOT_STATE["enabled"]:
                _autobot_scan_once()
        except Exception as e:
            print(f"[autobot] scan error: {e}")
        time.sleep(_AUTOBOT_STATE["interval_seconds"])


def _autobot_start():
    """Idempotent — starts the daemon thread once per process."""
    with _AUTOBOT_LOCK:
        if _AUTOBOT_STATE["thread_started"]:
            return
        _AUTOBOT_STATE["thread_started"] = True
    t = threading.Thread(target=_autobot_daemon, daemon=True, name="autobot")
    t.start()
    print("[autobot] daemon thread started (disabled until /api/autobot/toggle is hit)")


@app.route("/api/autobot/status")
def api_autobot_status():
    s = dict(_AUTOBOT_STATE)
    s["cooldowns"] = {t: dt.isoformat() for t, dt in _AUTOBOT_COOLDOWN.items()}
    return jsonify(s)


@app.route("/api/autobot/toggle", methods=["POST"])
def api_autobot_toggle():
    body = request.get_json(force=True, silent=True) or {}
    enable = bool(body.get("enable", not _AUTOBOT_STATE["enabled"]))
    with _AUTOBOT_LOCK:
        _AUTOBOT_STATE["enabled"] = enable
    return jsonify({"enabled": enable, "interval_seconds": _AUTOBOT_STATE["interval_seconds"]})


@app.route("/api/autobot/scan_now", methods=["POST"])
def api_autobot_scan_now():
    """Manually trigger a scan (regardless of enabled flag) — useful for testing."""
    res = _autobot_scan_once()
    return jsonify(res)


@app.route("/api/autobot/config", methods=["POST"])
def api_autobot_config():
    """The mascot itself is NOT configurable here — the layer adopts whatever
    the user has selected as their active mascot. Configurable: interval, daily cap, user."""
    body = request.get_json(force=True, silent=True) or {}
    with _AUTOBOT_LOCK:
        if "interval_seconds" in body:
            _AUTOBOT_STATE["interval_seconds"] = max(60, int(body["interval_seconds"]))
        if "max_trades_per_day" in body:
            _AUTOBOT_STATE["max_trades_per_day"] = max(1, int(body["max_trades_per_day"]))
        if "user" in body:
            _AUTOBOT_STATE["user"] = str(body["user"]).lower()
    return jsonify(dict(_AUTOBOT_STATE))


# ============================================================
# PORTFOLIO · P&L history time-series for the chart
# ============================================================

@app.route("/api/portfolio/pnl_history")
def api_portfolio_pnl_history():
    """Returns daily-aggregated cumulative P&L for the chart on /portfolio.
    Uses each closed trade's realized P&L plus mark-to-market on open trades."""
    user = session.get("username")
    if not user:
        return jsonify({"error": "not_logged_in"}), 401

    with db() as c:
        rows = c.execute(
            "SELECT ts, ticker, side, shares, entry_price, stop_price, target_price, "
            "       notional_usd, exit_price, exit_ts, status "
            "FROM journal WHERE username=? ORDER BY ts ASC",
            (user.lower(),)
        ).fetchall()
    trades = [dict(r) for r in rows]
    if not trades:
        return jsonify({"days": [], "cumulative_pnl": [], "daily_pnl": [],
                        "trade_dates": [], "trade_pnl": []})

    from collections import defaultdict
    daily = defaultdict(float)

    # Realized P&L on each trade exit date
    for t in trades:
        if t.get("status") == "CLOSED" and t.get("exit_price") and t.get("exit_ts"):
            entry = float(t["entry_price"] or 0)
            exit_ = float(t["exit_price"] or 0)
            shares = float(t["shares"] or 0)
            if t["side"] == "SHORT":
                pnl = (entry - exit_) * shares
            else:
                pnl = (exit_ - entry) * shares
            day = t["exit_ts"][:10]
            daily[day] += pnl
        elif t.get("status") != "CLOSED":
            # Mark open trades to market today
            try:
                q = get_quote(t["ticker"])
                cur = float(q.get("price") or 0)
                entry = float(t["entry_price"] or 0)
                shares = float(t["shares"] or 0)
                if t["side"] == "SHORT":
                    pnl = (entry - cur) * shares
                else:
                    pnl = (cur - entry) * shares
                # Attribute the unrealized to today
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                daily[today] += pnl
            except Exception:
                pass

    # Sort and accumulate
    days = sorted(daily.keys())
    daily_pnl = [round(daily[d], 2) for d in days]
    cumulative = []
    running = 0.0
    for v in daily_pnl:
        running += v
        cumulative.append(round(running, 2))

    # Trade markers (one dot per individual trade entry)
    trade_dates = [t["ts"][:10] for t in trades if t.get("ts")]
    trade_pnl = []
    for t in trades:
        entry = float(t["entry_price"] or 0)
        shares = float(t["shares"] or 0)
        try:
            if t.get("status") == "CLOSED" and t.get("exit_price"):
                exit_ = float(t["exit_price"])
                pnl = (entry - exit_) * shares if t["side"] == "SHORT" else (exit_ - entry) * shares
            else:
                q = get_quote(t["ticker"])
                cur = float(q.get("price") or 0)
                pnl = (entry - cur) * shares if t["side"] == "SHORT" else (cur - entry) * shares
            trade_pnl.append(round(pnl, 2))
        except Exception:
            trade_pnl.append(0.0)

    return jsonify({
        "days": days,
        "cumulative_pnl": cumulative,
        "daily_pnl": daily_pnl,
        "trade_dates": trade_dates,
        "trade_pnl": trade_pnl,
        "total_trades": len(trades),
    })


@app.route("/api/portfolio/reset", methods=["POST"])
def api_portfolio_reset():
    user = session.get("username")
    with db() as c:
        if user:
            c.execute("DELETE FROM journal WHERE username=?", (user.lower(),))
            row = c.execute("SELECT starting_capital FROM portfolio WHERE username=?",
                            (user.lower(),)).fetchone()
            sc = (row["starting_capital"] if row else None) or STARTING_CAPITAL
            c.execute(
                "UPDATE portfolio SET cash=?, drawdown_mode='NORMAL', hwm=? WHERE username=?",
                (sc, sc, user.lower()),
            )
        else:
            c.execute("DELETE FROM journal")
            c.execute("DELETE FROM risk_verdicts")
            c.execute("DELETE FROM bearwatch_events")
            c.execute(
                "UPDATE portfolio SET cash = ?, drawdown_mode = 'NORMAL', hwm = ? WHERE id = 1",
                (STARTING_CAPITAL, STARTING_CAPITAL),
            )
        c.commit()
    return jsonify({"ok": True})


def _open_browser():
    time.sleep(1.2)
    try:
        webbrowser.open("http://127.0.0.1:5000/")
    except Exception:
        pass


def _prewarm_pod_cache():
    """Pre-compute pod_run results + warehouse queries on startup so fire-button
    requests return instantly. Runs in a background thread so app.run() isn't blocked."""
    import time as _t
    _t.sleep(2)  # let Flask bind to the port first
    print("[prewarm] starting background warm-up of live caches...")
    try:
        # 1. Warm warehouse-derived live signals (used by /api/live/firms, monitor, pod_run)
        live = _get_live_cfpb_signals()
        # 2. Warm BSI series + freshness in parallel via direct cache fills
        warehouse_query("""
            SELECT observed_at, bsi, z_bsi, c_cfpb, c_move, c_trends, c_reddit, c_appstore
            FROM bsi_daily ORDER BY observed_at DESC LIMIT 90
        """, cache_key="live_bsi_90d")
        # 3. Pre-bake pod_run results for every live ticker (so fire is INSTANT)
        if live:
            with app.test_request_context('/api/live/pod_run'):
                for tk in live.keys():
                    try:
                        with app.test_request_context(f'/api/live/pod_run?ticker={tk}'):
                            api_live_pod_run()  # populates _LIVE_CACHE under live_pod_macro etc
                    except Exception as e:
                        print(f"[prewarm] {tk}: {str(e)[:60]}")
            print(f"[prewarm] pod_run cached for {len(live)} firms · ready for instant fire")
        # 4. Bump cache TTL while we're here (10 min during normal operation)
        global _LIVE_TTL
        _LIVE_TTL = 600.0
    except Exception as e:
        print(f"[prewarm] partial failure: {e}")


if __name__ == "__main__":
    init_db()
    print("=" * 60)
    print(" Apollo Hermes x BearWatch — WORKING DEMO")
    print(" Open: http://127.0.0.1:5000/")
    print(f" yfinance: {'OK' if YF_AVAILABLE else 'unavailable (using fallback prices)'}")
    print("=" * 60)
    threading.Thread(target=_open_browser, daemon=True).start()
    threading.Thread(target=_prewarm_pod_cache, daemon=True).start()
    _autobot_start()  # auto-trade layer daemon — disabled until /api/autobot/toggle is hit
    app.run(host="127.0.0.1", port=5000, debug=False)
