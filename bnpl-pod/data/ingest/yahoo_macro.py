"""
Yahoo-sourced macro series — FRED-fallback for tickers FRED does not host.

Why this module exists
----------------------
FRED does not host the ICE BofA MOVE Index (`^MOVE`) — it is proprietary and
requesting it returns 400 "series does not exist". But the four-gate thesis
specifically requires MOVE MA30 as one of its compliance gates (see
`config/thresholds.yaml::gates.move.ma30_threshold`), and Fix #1's
regime-scaled bid/ask haircut multiplies by MOVE/MOVE_median. Without MOVE in
the warehouse, the pod cannot run a real-data backtest.

This module pulls `^MOVE` (and other FRED-missing tickers) from Yahoo Finance
via `yfinance` and writes them into the SAME `fred_series` table with the
same `series_id` label the pod expects. Downstream code (BSI builder,
compliance engine, event study) never needs to know where the data came from.

Design
------
- Same idempotency contract as `data/ingest/fred.py`: INSERT OR REPLACE on
  (series_id, observed_at), so reruns are safe.
- Uses `Close` price as the canonical value. For index series this is the
  closing level; yfinance normalizes across dividends / splits.
- Timezone-strips the DatetimeIndex — DuckDB's TIMESTAMP without TIME ZONE
  silently converts tz-aware stamps to local time (see invariant #5 in
  SPRINT_REPORT.md).
- Skips holes in the history (`Close` NaN) by inserting NULL to preserve
  the calendar alignment downstream.
- Respects `settings.offline` so CI doesn't touch the network.

Run with:  python -m data.ingest.yahoo_macro
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import duckdb

from data.settings import settings

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")


# --- Series catalog ---------------------------------------------------------
@dataclass(frozen=True)
class YahooSeries:
    series_id: str      # canonical label written to fred_series.series_id
    yahoo_symbol: str   # what yfinance resolves
    name: str
    purpose: str


SERIES: list[YahooSeries] = [
    YahooSeries(
        series_id="MOVE",
        yahoo_symbol="^MOVE",
        name="ICE BofA MOVE Index",
        purpose=(
            "Bond-vol macro gate (3-gate AND: gate_move requires MA30 > 120). "
            "Also drives Fix #1 regime-scaled B/A: "
            "ba_bps_t = base + stress * max(0, MOVE_t / MOVE_median - 1). "
            "Not hosted by FRED — pulled from Yahoo as a FRED-schema-compatible fallback."
        ),
    ),
    # --- Sprint H.b additions: instrument closes required by the event-study
    # warehouse→WindowFixture bridge in `backtest/event_study.py`. These are
    # NOT macro series in the strict FRED sense; we store them in `fred_series`
    # because the warehouse treats that table as the generic (series_id,
    # observed_at, value) daily-timeseries home and downstream code already
    # knows how to read it. Returns are computed by the loader from Close.
    YahooSeries(
        series_id="HYG",
        yahoo_symbol="HYG",
        name="iShares iBoxx $ High Yield Corporate Bond ETF",
        purpose=(
            "High-yield credit-beta proxy. Drives hyg_returns in WindowFixture "
            "AND is the MTM proxy the event-study uses to synthesize junior "
            "ABS tranche returns when abs_tranche_metrics is sparse (the TRS "
            "book tracks BB/BBB junior spreads, for which HYG is the listed "
            "replica). The macro-hedge sleeve is also sized against HYG."
        ),
    ),
    YahooSeries(
        series_id="AFRM",
        yahoo_symbol="AFRM",
        name="Affirm Holdings common stock",
        purpose=(
            "Drives afrm_returns in WindowFixture — the naive AFRM-equity-short "
            "comparison arm in the Sprint G three-panel backtest. The NAIVE "
            "panel shorts AFRM directly at the same gross notional as the TRS "
            "arm; the -alpha differential quantifies the 'thesis stapled to "
            "a squeeze-prone instrument' tax."
        ),
    ),
]


# --- yfinance fetch ---------------------------------------------------------
def _fetch_series(yahoo_symbol: str, *, start: str = "2018-01-01") -> list[tuple[date, float | None]]:
    """Pull daily closes for `yahoo_symbol`. Returns (date, close_or_None)."""
    # Local import — yfinance is a heavy dep; don't pay the cost unless called.
    import yfinance as yf   # noqa: PLC0415

    ticker = yf.Ticker(yahoo_symbol)
    # auto_adjust=False so `Close` is the unadjusted closing level. For an
    # index like ^MOVE there are no dividends or splits anyway, but we set
    # it explicitly so the column name and semantics are stable if we later
    # add equity tickers here.
    hist = ticker.history(start=start, auto_adjust=False)

    if hist.empty:
        return []

    rows: list[tuple[date, float | None]] = []
    for ts, row in hist.iterrows():
        # Strip timezone to UTC-naive (invariant #5).
        d = ts.tz_localize(None).date() if getattr(ts, "tzinfo", None) else ts.date()
        close = row.get("Close")
        try:
            v: float | None = float(close) if close is not None and not _isnan(close) else None
        except (TypeError, ValueError):
            v = None
        rows.append((d, v))
    return rows


def _isnan(x: object) -> bool:
    try:
        return x != x   # NaN self-inequality
    except Exception:
        return False


# --- DuckDB writer ----------------------------------------------------------
def _upsert(
    con: duckdb.DuckDBPyConnection,
    series_id: str,
    rows: Iterable[tuple[date, float | None]],
) -> int:
    rows = list(rows)
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO fred_series (series_id, observed_at, value)
        VALUES (?, ?, ?)
        """,
        [(series_id, d, v) for d, v in rows],
    )
    return len(rows)


# --- Public entry points ----------------------------------------------------
def ingest_series(s: YahooSeries, *, start: str = "2018-01-01") -> int:
    """Backfill one Yahoo-sourced series. Returns row count written."""
    if settings.offline:
        log.info("offline mode; skipping %s", s.series_id)
        return 0
    rows = _fetch_series(s.yahoo_symbol, start=start)
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, s.series_id, rows)
    finally:
        con.close()
    log.info(
        "yahoo_macro | %-8s (%s) | upserted %d rows",
        s.series_id, s.yahoo_symbol, n,
    )
    return n


def ingest_all(*, start: str = "2018-01-01") -> dict[str, int]:
    """Backfill every configured Yahoo series. Returns {series_id: row_count}."""
    results: dict[str, int] = {}
    for s in SERIES:
        try:
            results[s.series_id] = ingest_series(s, start=start)
        except Exception as e:   # noqa: BLE001 — one bad series shouldn't kill the run
            log.error("yahoo_macro | %s | FAILED: %s", s.series_id, e)
            results[s.series_id] = -1
    return results


if __name__ == "__main__":
    summary = ingest_all()
    print("Yahoo-macro ingest summary:")
    for sid, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {sid:10s} {n:>6d} rows")
