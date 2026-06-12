"""
Marle-G — DATASTORE. One synchronized, persisted market-data layer (DuckDB).

Why: every backtest/bot was re-downloading yfinance on each run — slow, flaky, and
non-reproducible (two runs can see different data). This store makes the data:
  SYNCHRONIZED — all modules read the SAME table;
  INCREMENTAL  — sync() fetches only missing days (first run pulls full history once);
  AUDITABLE    — snapshots persist on disk (marleg_market.duckdb), so any result can be
                 reproduced against the exact data that produced it.

  import marleg_datastore as ds
  ds.sync()                      # top-up to the latest session (cheap after first run)
  C = ds.panel("close")          # DataFrame: dates x symbols
  V = ds.panel("volume")
  n = ds.series("^NSEI")         # one symbol's close series
"""
import os
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "marleg_market.duckdb")
PERIOD_FULL = "5y"

UNIV = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "ITC", "LT", "SBIN", "AXISBANK",
        "KOTAKBANK", "BHARTIARTL", "BAJFINANCE", "HINDUNILVR", "MARUTI", "SUNPHARMA",
        "EICHERMOT", "TATASTEEL", "M&M", "NTPC", "TITAN", "ASIANPAINT", "ULTRACEMCO",
        "WIPRO", "ADANIPORTS", "JSWSTEEL", "COALINDIA", "ONGC", "GRASIM", "HCLTECH", "CIPLA",
        "POWERGRID", "BAJAJFINSV", "TECHM", "NESTLEIND", "TEJASNET"]
INDEXES = ["^NSEI", "^INDIAVIX"]


def _con():
    con = duckdb.connect(DB)
    con.execute("""CREATE TABLE IF NOT EXISTS prices(
        symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
        close DOUBLE, volume DOUBLE, PRIMARY KEY(symbol, date))""")
    return con


def _yf_symbol(s):
    return s if s.startswith("^") else s + ".NS"


def _insert(con, sym, df):
    if df is None or df.empty:
        return 0
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna(subset=["Close"])
    rows = pd.DataFrame({
        "symbol": sym, "date": pd.to_datetime(df.index).date,
        "open": df["Open"].values, "high": df["High"].values, "low": df["Low"].values,
        "close": df["Close"].values, "volume": df.get("Volume", pd.Series(0, index=df.index)).values})
    con.execute("INSERT OR REPLACE INTO prices SELECT * FROM rows")
    return len(rows)


def sync(symbols=None, verbose=True):
    """Top-up the store. New symbols: full history; known symbols: only missing days."""
    symbols = symbols or (UNIV + INDEXES)
    con = _con()
    last = dict(con.execute("SELECT symbol, max(date) FROM prices GROUP BY symbol").fetchall())
    today = datetime.now().date()
    full, top_up = [], []
    for s in symbols:
        ld = last.get(s)
        if ld is None:
            full.append(s)
        elif (today - ld).days >= 1:
            top_up.append((s, ld))
    n = 0
    def _pick(data, s):
        # yfinance returns ticker-keyed MultiIndex even for a single symbol with group_by
        if isinstance(data.columns, pd.MultiIndex) and _yf_symbol(s) in data.columns.get_level_values(0):
            return data[_yf_symbol(s)]
        return data

    if full:
        data = yf.download([_yf_symbol(s) for s in full], period=PERIOD_FULL, interval="1d",
                           group_by="ticker", auto_adjust=False, progress=False, threads=True)
        for s in full:
            try:
                n += _insert(con, s, _pick(data, s))
            except Exception:
                pass
    if top_up:
        start = min(ld for _, ld in top_up) - timedelta(days=4)
        data = yf.download([_yf_symbol(s) for s, _ in top_up], start=str(start), interval="1d",
                           group_by="ticker", auto_adjust=False, progress=False, threads=True)
        for s, _ in top_up:
            try:
                n += _insert(con, s, _pick(data, s))
            except Exception:
                pass
    total, syms = con.execute("SELECT count(*), count(DISTINCT symbol) FROM prices").fetchone()
    con.close()
    if verbose:
        print(f"[datastore] upserted {n} rows · store now {total} rows / {syms} symbols")
    return n


def panel(field="close", symbols=None):
    """dates x symbols DataFrame for one field, equities only by default."""
    symbols = symbols or UNIV
    con = _con()
    df = con.execute(
        f"SELECT date, symbol, {field} FROM prices WHERE symbol IN ({','.join('?' * len(symbols))}) "
        "ORDER BY date", symbols).df()
    con.close()
    out = df.pivot(index="date", columns="symbol", values=field)
    out.index = pd.to_datetime(out.index)
    return out


def series(symbol, field="close"):
    con = _con()
    df = con.execute(f"SELECT date, {field} FROM prices WHERE symbol=? ORDER BY date",
                     [symbol]).df()
    con.close()
    s = pd.Series(df[field].values, index=pd.to_datetime(df["date"]), name=symbol)
    return s


def backfill(period="10y"):
    """One-off deep-history pull (idempotent INSERT OR REPLACE). Use to extend the store
    so frozen strategy rules can be tested on years they were never designed on."""
    con = _con()
    syms = UNIV + INDEXES
    data = yf.download([_yf_symbol(s) for s in syms], period=period, interval="1d",
                       group_by="ticker", auto_adjust=False, progress=False, threads=True)
    n = 0
    for s in syms:
        try:
            n += _insert(con, s, data[_yf_symbol(s)])
        except Exception:
            pass
    total = con.execute("SELECT count(*) FROM prices").fetchone()[0]
    con.close()
    print(f"[datastore] backfill({period}): upserted {n} rows · store now {total} rows")
    return n


def status():
    con = _con()
    rows = con.execute("SELECT symbol, min(date), max(date), count(*) FROM prices "
                       "GROUP BY symbol ORDER BY symbol").fetchall()
    con.close()
    return {s: {"from": str(a), "to": str(b), "rows": c} for s, a, b, c in rows}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if "--backfill" in sys.argv:
        backfill()
        raise SystemExit
    sync()
    st = status()
    eq = [s for s in st if not s.startswith("^")]
    print(f"symbols: {len(st)} ({len(eq)} equities + {len(st)-len(eq)} indexes)")
    any_sym = eq[0]
    print(f"sample {any_sym}: {st[any_sym]}")
