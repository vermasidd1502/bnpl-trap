"""
Options-chain ingest via yfinance — input to Heston SCP (§7 of paper) and
IV-skew component of the Squeeze Defense Layer (§10).

yfinance is free but rate-limited and intermittently flaky; retries are
conservative. Only current/forward-expiry chains are pullable — there is
no yfinance historical-chain backfill. Historical chains come from a
one-time OptionMetrics / CBOE pull at paper-write time; this module
maintains the live series.

Run with:  python -m data.ingest.options_chain
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
from tenacity import retry, stop_after_attempt, wait_exponential

from data.settings import settings

log = logging.getLogger(__name__)

# Tickers with options chains relevant to the thesis (equity-layer signal).
OPTION_TICKERS: list[str] = [
    "AFRM", "SQ", "PYPL", "SEZL", "UPST",                # treated
    "COF", "SYF", "DFS", "AXP", "SOFI", "LC",            # near-prime
    "V", "MA", "JPM", "BRK-B",                           # placebo
    "CACC", "ALLY", "SC",                                # subprime-auto listed
]


@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=2, max=20))
def _fetch_chain(ticker: str) -> list[dict]:
    """Return a flat list of {ticker, observed_at, expiry, strike, option_type,
    bid, ask, last_price, volume, open_interest, iv, underlying_price}."""
    import yfinance as yf   # local import

    t = yf.Ticker(ticker)
    expiries = t.options or []
    if not expiries:
        return []
    info_price = None
    try:
        hist = t.history(period="1d", auto_adjust=False)
        if not hist.empty:
            info_price = float(hist["Close"].iloc[-1])
    except Exception:   # noqa: BLE001
        info_price = None

    today = datetime.now(timezone.utc).date()
    rows: list[dict] = []
    for exp_str in expiries:
        try:
            chain = t.option_chain(exp_str)
        except Exception as e:   # noqa: BLE001
            log.warning("options | %s | expiry %s fetch fail: %s", ticker, exp_str, e)
            continue
        exp_d = datetime.strptime(exp_str, "%Y-%m-%d").date()
        for df, typ in ((chain.calls, "C"), (chain.puts, "P")):
            if df is None or df.empty:
                continue
            for _, r in df.iterrows():
                rows.append({
                    "ticker":            ticker,
                    "observed_at":       today,
                    "expiry":            exp_d,
                    "strike":            float(r.get("strike") or 0.0),
                    "option_type":       typ,
                    "bid":               _f(r.get("bid")),
                    "ask":               _f(r.get("ask")),
                    "last_price":        _f(r.get("lastPrice")),
                    "volume":            _i(r.get("volume")),
                    "open_interest":     _i(r.get("openInterest")),
                    "iv":                _f(r.get("impliedVolatility")),
                    "underlying_price":  info_price,
                })
    return rows


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:   # noqa: BLE001
        return None


def _i(x):
    try:
        return int(x) if x is not None else None
    except Exception:   # noqa: BLE001
        return None


def _upsert(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO options_chain
            (ticker, observed_at, expiry, strike, option_type,
             bid, ask, last_price, volume, open_interest, iv, underlying_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(r["ticker"], r["observed_at"], r["expiry"], r["strike"], r["option_type"],
          r["bid"], r["ask"], r["last_price"], r["volume"],
          r["open_interest"], r["iv"], r["underlying_price"]) for r in rows],
    )
    return len(rows)


def ingest_ticker(ticker: str) -> int:
    if settings.offline:
        log.info("offline mode; skipping options %s", ticker)
        return 0
    rows = _fetch_chain(ticker)
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, rows)
    finally:
        con.close()
    log.info("options | %-6s | %d contracts", ticker, n)
    return n


def ingest_all(*, sleep_between: float = 1.0) -> dict[str, int]:
    results: dict[str, int] = {}
    for t in OPTION_TICKERS:
        try:
            results[t] = ingest_ticker(t)
            if sleep_between and not settings.offline:
                time.sleep(sleep_between)
        except Exception as e:   # noqa: BLE001
            log.error("options | %s | FAILED: %s", t, e)
            results[t] = -1
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    summary = ingest_all()
    print("\nOptions-chain ingest summary:")
    for t, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {t:6s} {n:>6d} contracts")
