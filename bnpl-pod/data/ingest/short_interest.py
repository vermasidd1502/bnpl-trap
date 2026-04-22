"""
Short-interest ingest — feeds Squeeze Defense Layer (§10, G-SDL gate).

Two sources:
1. FINRA bi-monthly short-interest file (free, official, lagged ~8 trading days).
   https://www.finra.org/finra-data/short-sale-volume-data/daily-short-sale-volume-files
   The semi-monthly aggregate is published as downloadable TXT. For robustness
   we accept either a user-supplied CSV/TXT (env FINRA_SI_FILE) or the live HTTP
   endpoint when available.
2. yfinance fallback for avg_daily_vol and last-known shares-short (for
   days-to-cover computation between FINRA releases).

Run with:  python -m data.ingest.short_interest
"""
from __future__ import annotations

import csv
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
from tenacity import retry, stop_after_attempt, wait_exponential

from data.settings import settings

log = logging.getLogger(__name__)

SI_TICKERS: list[str] = [
    "AFRM", "SQ", "PYPL", "SEZL", "UPST",
    "COF", "SYF", "DFS", "AXP", "SOFI", "LC",
    "CACC", "ALLY",
]


@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1.0, min=2, max=20))
def _yf_proxy(ticker: str) -> dict | None:
    """Best-effort: use yfinance .get_info() for sharesShort / avgVolume."""
    import yfinance as yf  # local
    t = yf.Ticker(ticker)
    info = {}
    try:
        info = t.get_info() or {}
    except Exception:   # noqa: BLE001
        try:
            info = t.info or {}
        except Exception:   # noqa: BLE001
            return None
    shares_short = info.get("sharesShort")
    float_shares = info.get("floatShares") or info.get("sharesOutstanding")
    adv = info.get("averageVolume10days") or info.get("averageVolume")
    if shares_short is None and adv is None:
        return None

    util = None
    if shares_short and float_shares:
        try:
            util = float(shares_short) / float(float_shares)
        except Exception:   # noqa: BLE001
            util = None
    dtc = None
    if shares_short and adv:
        try:
            dtc = float(shares_short) / float(adv)
        except Exception:   # noqa: BLE001
            dtc = None
    return {
        "ticker":         ticker,
        "observed_at":    datetime.now(timezone.utc).date(),
        "shares_short":   int(shares_short) if shares_short else None,
        "free_float":     int(float_shares) if float_shares else None,
        "utilization":    util,
        "avg_daily_vol":  int(adv) if adv else None,
        "days_to_cover":  dtc,
    }


def _read_finra_file(path: Path) -> list[dict]:
    """Parse a locally-cached FINRA short-interest CSV/TXT.

    Expected columns (case-insensitive, | or , separator):
      settlementDate, symbolCode, currentShortPositionQuantity,
      previousShortPositionQuantity, averageDailyVolumeQuantity, daysToCoverQuantity
    """
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(4096)
        f.seek(0)
        sep = "|" if sample.count("|") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=sep)
        for r in reader:
            low = {k.lower().strip(): (v.strip() if v else v) for k, v in r.items()}
            sym = low.get("symbolcode") or low.get("symbol")
            if not sym or sym.upper() not in SI_TICKERS:
                continue
            try:
                d = datetime.strptime(low.get("settlementdate", ""), "%Y%m%d").date()
            except Exception:   # noqa: BLE001
                try:
                    d = datetime.strptime(low.get("settlementdate", ""), "%Y-%m-%d").date()
                except Exception:   # noqa: BLE001
                    continue
            ss = _to_int(low.get("currentshortpositionquantity"))
            adv = _to_int(low.get("averagedailyvolumequantity"))
            dtc = _to_float(low.get("daystocoverquantity"))
            rows.append({
                "ticker":        sym.upper(),
                "observed_at":   d,
                "shares_short":  ss,
                "free_float":    None,
                "utilization":   None,
                "avg_daily_vol": adv,
                "days_to_cover": dtc if dtc is not None else (
                    (ss / adv) if (ss and adv) else None
                ),
            })
    return rows


def _to_int(x):
    try:
        return int(float(x)) if x not in (None, "") else None
    except Exception:   # noqa: BLE001
        return None


def _to_float(x):
    try:
        return float(x) if x not in (None, "") else None
    except Exception:   # noqa: BLE001
        return None


def _upsert(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO short_interest
            (ticker, observed_at, shares_short, free_float,
             utilization, avg_daily_vol, days_to_cover)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(r["ticker"], r["observed_at"], r["shares_short"], r["free_float"],
          r["utilization"], r["avg_daily_vol"], r["days_to_cover"]) for r in rows],
    )
    return len(rows)


def ingest_finra_file(path: str | None = None) -> int:
    if settings.offline:
        log.info("offline mode; skipping FINRA short-interest")
        return 0
    p = Path(path or os.environ.get("FINRA_SI_FILE", "data/raw/finra_si_latest.txt"))
    rows = _read_finra_file(p)
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, rows)
    finally:
        con.close()
    log.info("short_interest | FINRA file %s | %d rows", p, n)
    return n


def ingest_yf_proxy(ticker: str) -> int:
    if settings.offline:
        log.info("offline mode; skipping short_interest %s", ticker)
        return 0
    r = _yf_proxy(ticker)
    if not r:
        return 0
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, [r])
    finally:
        con.close()
    log.info("short_interest | %-6s | ss=%s dtc=%s", ticker,
             r.get("shares_short"), r.get("days_to_cover"))
    return n


def ingest_all() -> dict[str, int]:
    results: dict[str, int] = {}
    results["_finra_file"] = ingest_finra_file()
    for t in SI_TICKERS:
        try:
            results[t] = ingest_yf_proxy(t)
        except Exception as e:   # noqa: BLE001
            log.error("short_interest | %s | FAILED: %s", t, e)
            results[t] = -1
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    summary = ingest_all()
    print("\nShort-interest ingest summary:")
    for t, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {t:12s} {n:>4d}")
