"""
Corporate-actions + news layer (Investopedia-simulator style).

Handles two things automatically for any OPEN journal position:

  1. SPLITS — when a ticker splits N:M, we adjust:
        new_shares      = old_shares      * (N / M)
        new_entry_price = old_entry_price * (M / N)
        new_stop        = old_stop        * (M / N)
        new_target      = old_target      * (M / N)
        notional_usd    = unchanged
     Audit-logged to corporate_actions table + activity_log.

  2. DIVIDENDS — cash dividend $X/share:
        LONG  → cash += shares * X       (you receive)
        SHORT → cash -= shares * X       (you pay — short owes the divvie)
     Stock dividends are treated as fractional splits.

Detector pulls from yfinance's `.actions` DataFrame (splits + dividends)
which is free + reliable. Anything detected lands in `corporate_actions`
with `applied_at = NULL`; the applier walks unapplied rows and mutates
journal + portfolio inside one transaction.

The `news_events` table is the lightweight news-layer companion — every
detected corporate action also gets a row there so the live pod can show
"News:  CVNA executed 5-for-1 split on 2026-05-08" cards.

Public API:
    init_tables(con)              — idempotent schema setup
    scan_and_apply(con, tickers)  — full pass (detect + apply); returns summary dict
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Iterable

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
DDL = [
    """
    CREATE TABLE IF NOT EXISTS corporate_actions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT NOT NULL,
        action_type     TEXT NOT NULL,           -- 'SPLIT' | 'CASH_DIVIDEND' | 'STOCK_DIVIDEND'
        ratio           REAL,                    -- split ratio (5.0 = 5-for-1); NULL for dividends
        dividend_per_share REAL,                 -- $/share; NULL for splits
        ex_date         TEXT NOT NULL,           -- ISO date the action took effect
        detected_at     TEXT NOT NULL,
        applied_at      TEXT,                    -- NULL until applier runs
        source          TEXT DEFAULT 'yfinance',
        notes           TEXT,
        UNIQUE(ticker, action_type, ex_date)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS news_events (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT NOT NULL,
        event_type      TEXT NOT NULL,           -- 'CORPORATE_ACTION' | 'EARNINGS' | 'RATING_CHANGE' | ...
        headline        TEXT NOT NULL,
        ts              TEXT NOT NULL,           -- when the event occurred
        severity        TEXT DEFAULT 'INFO',     -- 'INFO' | 'WARN' | 'CRITICAL'
        url             TEXT,
        captured_at     TEXT NOT NULL
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_ca_unapplied ON corporate_actions(applied_at) WHERE applied_at IS NULL;",
    "CREATE INDEX IF NOT EXISTS idx_news_ticker_ts ON news_events(ticker, ts DESC);",
]


def init_tables(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    con.commit()


# --------------------------------------------------------------------------- #
# Detection (yfinance pull)
# --------------------------------------------------------------------------- #
def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def detect_actions(tickers: Iterable[str], lookback_days: int = 365) -> list[dict]:
    """Return list of {ticker, action_type, ratio, dividend_per_share, ex_date, source}.

    Pulls from yfinance Ticker.actions, which is a DataFrame with index=date
    and columns ['Dividends', 'Stock Splits']. Filters to lookback_days.
    """
    try:
        import yfinance as yf
    except ImportError:
        log.warning("yfinance not installed; skipping CA detection")
        return []

    import pandas as pd

    cutoff = pd.Timestamp.utcnow().tz_convert(None) - pd.Timedelta(days=lookback_days)
    out: list[dict] = []

    for tk in {t.upper() for t in tickers if t}:
        try:
            t = yf.Ticker(tk)
            df = t.actions  # DataFrame; may be empty
            if df is None or df.empty:
                continue
            # Index is tz-aware; strip for comparison
            df = df.copy()
            df.index = pd.to_datetime(df.index).tz_localize(None)
            df = df[df.index >= cutoff]
            for ts, row in df.iterrows():
                ex_date = ts.strftime("%Y-%m-%d")
                divv = float(row.get("Dividends", 0.0) or 0.0)
                split = float(row.get("Stock Splits", 0.0) or 0.0)
                if split and split != 1.0:
                    out.append({
                        "ticker": tk,
                        "action_type": "SPLIT",
                        "ratio": split,            # yfinance reports e.g. 5.0 for 5-for-1
                        "dividend_per_share": None,
                        "ex_date": ex_date,
                        "source": "yfinance",
                    })
                if divv > 0:
                    out.append({
                        "ticker": tk,
                        "action_type": "CASH_DIVIDEND",
                        "ratio": None,
                        "dividend_per_share": divv,
                        "ex_date": ex_date,
                        "source": "yfinance",
                    })
        except Exception as e:
            log.warning("yfinance actions lookup failed for %s: %s", tk, e)
            continue
    return out


def upsert_actions(con: sqlite3.Connection, actions: list[dict]) -> int:
    """Insert detected actions; UNIQUE(ticker, action_type, ex_date) prevents dupes."""
    cur = con.cursor()
    n = 0
    for a in actions:
        try:
            cur.execute(
                """INSERT INTO corporate_actions
                   (ticker, action_type, ratio, dividend_per_share, ex_date,
                    detected_at, applied_at, source, notes)
                   VALUES (?, ?, ?, ?, ?, ?, NULL, ?, NULL)""",
                (a["ticker"], a["action_type"], a.get("ratio"),
                 a.get("dividend_per_share"), a["ex_date"], _utcnow(), a["source"])
            )
            n += 1
        except sqlite3.IntegrityError:
            pass  # already known
    con.commit()
    return n


# --------------------------------------------------------------------------- #
# Application — mutate journal + portfolio
# --------------------------------------------------------------------------- #
def _log_activity(con: sqlite3.Connection, username: str, action: str, detail: str) -> None:
    """Best-effort write to activity_log; ignore if schema differs."""
    try:
        con.execute(
            "INSERT INTO activity_log (ts, username, action, detail) VALUES (?, ?, ?, ?)",
            (_utcnow(), username, action, detail)
        )
    except sqlite3.OperationalError:
        pass


def _emit_news(con: sqlite3.Connection, ticker: str, headline: str, ts: str, severity: str = "INFO") -> None:
    con.execute(
        """INSERT INTO news_events (ticker, event_type, headline, ts, severity, captured_at)
           VALUES (?, 'CORPORATE_ACTION', ?, ?, ?, ?)""",
        (ticker, headline, ts, severity, _utcnow())
    )


def apply_pending(con: sqlite3.Connection) -> dict:
    """Walk unapplied corporate_actions and mutate matching OPEN journal positions.

    Returns summary: {applied: N, splits_processed: N, dividends_processed: N,
                      positions_touched: N, cash_delta_by_user: {...}}
    """
    cur = con.cursor()
    pending = cur.execute(
        """SELECT id, ticker, action_type, ratio, dividend_per_share, ex_date
           FROM corporate_actions WHERE applied_at IS NULL ORDER BY ex_date ASC"""
    ).fetchall()

    summary = {"applied": 0, "splits_processed": 0, "dividends_processed": 0,
               "positions_touched": 0, "cash_delta_by_user": {}}

    for ca_id, ticker, action_type, ratio, dpsh, ex_date in pending:
        # Find OPEN positions for this ticker that were entered ON OR BEFORE ex_date
        positions = cur.execute(
            """SELECT id, username, side, shares, entry_price, stop_price, target_price, ts
               FROM journal
               WHERE ticker = ? AND status = 'OPEN' AND DATE(ts) <= DATE(?)""",
            (ticker, ex_date)
        ).fetchall()

        if action_type == "SPLIT":
            r = float(ratio or 1.0)
            if r <= 0 or r == 1.0:
                cur.execute("UPDATE corporate_actions SET applied_at=? WHERE id=?",
                            (_utcnow(), ca_id))
                continue
            for pid, user, side, sh, ep, sp, tp, ts in positions:
                new_sh = sh * r
                new_ep = ep / r
                new_sp = (sp / r) if sp else None
                new_tp = (tp / r) if tp else None
                cur.execute(
                    """UPDATE journal
                       SET shares=?, entry_price=?, stop_price=?, target_price=?
                       WHERE id=?""",
                    (new_sh, new_ep, new_sp, new_tp, pid)
                )
                _log_activity(con, user, "CA_SPLIT_APPLIED",
                              f"{ticker} {r:g}-for-1 split on {ex_date}: "
                              f"shares {sh:g}->{new_sh:g}, entry ${ep:.4f}->${new_ep:.4f}")
                summary["positions_touched"] += 1
            _emit_news(con, ticker,
                       f"{ticker} executed a {r:g}-for-1 stock split on {ex_date}. "
                       f"All open positions auto-adjusted.",
                       ts=f"{ex_date}T00:00:00+00:00", severity="INFO")
            summary["splits_processed"] += 1

        elif action_type == "CASH_DIVIDEND":
            d = float(dpsh or 0.0)
            if d <= 0:
                cur.execute("UPDATE corporate_actions SET applied_at=? WHERE id=?",
                            (_utcnow(), ca_id))
                continue
            user_deltas: dict[str, float] = {}
            for pid, user, side, sh, ep, sp, tp, ts in positions:
                amt = sh * d
                # LONG receives, SHORT pays
                delta = amt if side == "LONG" else -amt
                user_deltas[user] = user_deltas.get(user, 0.0) + delta
                _log_activity(con, user, "CA_DIVIDEND_APPLIED",
                              f"{ticker} ${d:.4f}/sh dividend on {ex_date}: "
                              f"{side} {sh:g} sh -> ${delta:+,.2f} cash")
                summary["positions_touched"] += 1
            for user, delta in user_deltas.items():
                cur.execute("UPDATE portfolio SET cash = cash + ? WHERE username = ?",
                            (delta, user))
                summary["cash_delta_by_user"][user] = (
                    summary["cash_delta_by_user"].get(user, 0.0) + delta
                )
            _emit_news(con, ticker,
                       f"{ticker} paid a ${d:.4f}/share cash dividend on {ex_date}. "
                       f"LONG positions credited; SHORT positions debited.",
                       ts=f"{ex_date}T00:00:00+00:00", severity="INFO")
            summary["dividends_processed"] += 1

        elif action_type == "STOCK_DIVIDEND":
            # Treat as fractional split: 1 + d new shares per old
            d = float(dpsh or 0.0)
            r = 1.0 + d
            if r > 1.0:
                for pid, user, side, sh, ep, sp, tp, ts in positions:
                    new_sh = sh * r
                    new_ep = ep / r
                    cur.execute("UPDATE journal SET shares=?, entry_price=? WHERE id=?",
                                (new_sh, new_ep, pid))
                    summary["positions_touched"] += 1
                summary["splits_processed"] += 1

        cur.execute("UPDATE corporate_actions SET applied_at=? WHERE id=?",
                    (_utcnow(), ca_id))
        summary["applied"] += 1

    con.commit()
    return summary


# --------------------------------------------------------------------------- #
# Top-level convenience
# --------------------------------------------------------------------------- #
def scan_and_apply(con: sqlite3.Connection, tickers: Iterable[str] | None = None,
                   lookback_days: int = 365) -> dict:
    """Detect new actions for given tickers (or all OPEN-position tickers if None),
    persist them, then apply any unapplied ones."""
    init_tables(con)

    if tickers is None:
        rows = con.execute(
            "SELECT DISTINCT UPPER(ticker) FROM journal WHERE status='OPEN'"
        ).fetchall()
        tickers = [r[0] for r in rows]

    detected = detect_actions(tickers, lookback_days=lookback_days)
    n_new = upsert_actions(con, detected)
    summary = apply_pending(con)
    summary["detected_in_scan"] = len(detected)
    summary["new_to_db"] = n_new
    summary["tickers_scanned"] = list(tickers)
    return summary
