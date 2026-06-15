"""
marleg_panel_build.py — build the ONE canonical, reproducible daily price panel for all backtests.

Why: every rigorous study must run on the *same* clean, gap-free, point-in-time dataset — not on
whatever a throttled yfinance call happened to return. This pulls 5 years of daily OHLCV for the full
liquid taxonomy universe via Groww (broker-grade, IST-exact, no throttle gaps), in parallel, and
caches it to marleg_panel_cache.pkl. Backtests then `from marleg_panel_build import load` — instant,
identical, reproducible every run.

  python marleg_panel_build.py            # build (5y, parallel) + cache
  python marleg_panel_build.py --years 3  # shorter window
"""
import os
import sys
import time
import concurrent.futures as cf
from datetime import datetime, timezone, timedelta

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "marleg_panel_cache.pkl")
WORKERS = 6
MIN_BARS = 250


def _syms():
    import json
    tax = json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json"), encoding="utf-8"))
    return list(tax.get("by_symbol", {}).keys())


def build(years=5):
    import groww_client as gc
    g = gc.GrowwClient(); g.token()                       # pre-warm auth so threads don't race the refresh
    days = int(years * 366)
    syms = _syms()
    print(f"building canonical panel: {len(syms)} names x {years}y daily via Groww ({WORKERS} workers)...")

    def fetch(s):
        try:
            df = g.candles(s, interval_min=1440, days=days)
            if df is not None and len(df) > MIN_BARS:
                df = df.copy()
                df.index = df.index.normalize()           # align all symbols on the trading DATE
                df = df[~df.index.duplicated(keep="last")]
                return s, df[["open", "high", "low", "close", "volume"]]
        except Exception:
            pass
        return s, None

    results, done, t0 = {}, 0, time.time()
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch, s): s for s in syms}
        for fu in cf.as_completed(futs):
            s, df = fu.result(); done += 1
            if df is not None:
                results[s] = df
            if done % 100 == 0:
                print(f"  {done}/{len(syms)} fetched · {len(results)} ok · {time.time()-t0:.0f}s", flush=True)

    if not results:
        print("FAILED — no data fetched."); return None
    close = pd.DataFrame({s: d["close"] for s, d in results.items()}).sort_index()
    panel = {
        "close": close,
        "open": pd.DataFrame({s: d["open"] for s, d in results.items()}).reindex(close.index),
        "high": pd.DataFrame({s: d["high"] for s, d in results.items()}).reindex(close.index),
        "low": pd.DataFrame({s: d["low"] for s, d in results.items()}).reindex(close.index),
        "volume": pd.DataFrame({s: d["volume"] for s, d in results.items()}).reindex(close.index),
        "built": datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST"),
        "source": "groww", "years": years, "n": close.shape[1],
    }
    pd.to_pickle(panel, CACHE)
    print(f"\nDONE: {close.shape[1]} names x {close.shape[0]} days "
          f"({close.index[0].date()} -> {close.index[-1].date()}) in {time.time()-t0:.0f}s")
    print(f"cached -> {CACHE}")
    return panel


def load():
    """Canonical panel {open,high,low,close,volume,...} from cache, or None if not built yet."""
    try:
        return pd.read_pickle(CACHE)
    except Exception:
        return None


def info():
    p = load()
    if not p:
        return {"built": False}
    return {"built": p.get("built"), "names": p.get("n"), "days": int(p["close"].shape[0]),
            "from": str(p["close"].index[0].date()), "to": str(p["close"].index[-1].date()), "source": p.get("source")}


if __name__ == "__main__":
    yrs = 5
    if "--years" in sys.argv:
        yrs = int(sys.argv[sys.argv.index("--years") + 1])
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    build(yrs)
