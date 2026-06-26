"""
marleg_data.py — one market-data layer that switches smoothly between Groww and yfinance.

The problem this solves: yfinance BULK downloads throttle hard (the gated scan kept collapsing to
~56 names mid-session), while Groww is rock-solid but per-symbol. So we route each need to the
source that's good at it, and fall back automatically:

  candles(tk, interval_min, days)   per-symbol OHLCV  -> GROWW first (throttle-proof, IST), yfinance fallback
  daily_panel(symbols, period)      bulk daily panel  -> yfinance BULK first (one fast call); if it comes
                                                         back THIN (throttled), fall back to Groww per-symbol
                                                         for a liquidity-capped subset so the scan still
                                                         produces a real result instead of 56 names
  health()                          {groww, yfinance} up/down (for the diag pod)

Override the policy with env MARLEG_DATA_SOURCE = auto | groww | yfinance.
Read-only throughout — never places, modifies or cancels an order.
"""
import os
import time
import threading

import numpy as np
import pandas as pd

_SOURCE = (os.environ.get("MARLEG_DATA_SOURCE") or "groww").lower()   # Groww-only by default (no yfinance)
_YF_TF = {1: "1m", 5: "5m", 15: "15m", 30: "30m", 60: "60m", 1440: "1d"}

# THREAD-LOCAL Groww client. requests.Session is NOT thread-safe, and groww_client mutates self._token on a
# 401 — so a single shared client races when the chart page fires candles+levels+events+pivots at once (and
# when a background bulk scan runs alongside chart loads). Giving each thread its own client+Session removes
# the race AND the bottleneck a global lock would create (the background scan can't block a chart load). The
# auth TOKEN is disk-cached, so a per-thread client is cheap — it never re-mints, just reads the cached token.
_tls = threading.local()


def _groww():
    c = getattr(_tls, "client", None)
    if c is None:
        try:
            import groww_client as gc
            c = gc.GrowwClient(); c.token()
        except Exception:
            c = False
        _tls.client = c
    return c or None


# ---------------------------------------------------------------- per-symbol candles
def _pull_candles(client, tk, interval_min, days):
    for ex in ("NSE", "BSE"):                      # BSE so SENSEX/BANKEX (and BSE-only names) resolve
        try:
            df = client.candles(tk.upper(), interval_min=interval_min, days=days, exchange=ex)
        except Exception:
            df = None
        if df is None or df.empty or len(df) < 2:
            continue
        if interval_min >= 1440 and days > 800 and len(df) < days / 4:        # index breaks on huge ranges
            try:
                d2 = client.candles(tk.upper(), interval_min=interval_min, days=750, exchange=ex)
                if d2 is not None and len(d2) > len(df):
                    df = d2
            except Exception:
                pass
        out = df[["open", "high", "low", "close", "volume"]].copy()
        out["volume"] = out["volume"].fillna(0.0)      # INDICES carry NaN volume — fill, don't let dropna nuke rows
        return out.dropna(subset=["open", "high", "low", "close"])   # only a missing PRICE invalidates a bar
    return None


def _is_thin(df, interval_min, days):
    if df is None or len(df) < 2:
        return True
    # Groww's INDEX daily-candle endpoint intermittently returns a 1-2 row blip (one row all-NaN -> 1 after
    # dropna) for a few seconds, then recovers. Equities never do this. So treat a daily result far below the
    # expected bar count as thin and worth a retry. (~0.65 trading days per calendar day.)
    return interval_min >= 1440 and days >= 120 and len(df) < min(40, int(days * 0.4))


def _groww_candles(tk, interval_min, days):
    """Groww OHLCV. One cheap retry on the SAME memoized client if the first pull comes back empty/thin
    (a genuine occasional transient). We deliberately do NOT mint fresh clients here: every GrowwClient()
    spins up a new auth session, and doing that under load multiplies sessions and trips Groww's throttle."""
    g = _groww()
    if not g:
        return None
    best = _pull_candles(g, tk, interval_min, days)
    if _is_thin(best, interval_min, days):
        df = _pull_candles(g, tk, interval_min, days)
        if df is not None and (best is None or len(df) > len(best)):
            best = df
    return best


def _yf_candles(tk, interval_min, days):
    try:
        import yfinance as yf
        tf = _YF_TF.get(interval_min, "1d")
        period = f"{max(1, days)}d" if interval_min < 1440 else (f"{days}d" if days <= 60 else f"{max(1, days // 365 + 1)}y")
        df = yf.Ticker(tk.upper() + ".NS").history(period=period, interval=tf, auto_adjust=False)
        if df is not None and not df.empty:
            df = df.rename(columns=str.lower)[["open", "high", "low", "close", "volume"]].dropna()
            df.index = df.index.tz_convert("Asia/Kolkata") if df.index.tz else df.index
            return df
    except Exception:
        pass
    return None


def candles(tk, interval_min=1440, days=250):
    """Per-symbol OHLCV (IST-indexed). Groww-first (reliable), yfinance fallback."""
    order = (["yfinance", "groww"] if _SOURCE == "yfinance" else ["groww", "yfinance"])
    if _SOURCE == "groww":
        order = ["groww"]
    for src in order:
        df = _groww_candles(tk, interval_min, days) if src == "groww" else _yf_candles(tk, interval_min, days)
        if df is not None and len(df):
            df.attrs["source"] = src
            return df
    return None


# ---------------------------------------------------------------- bulk daily panel
def _yf_bulk(symbols, period):
    import yfinance as yf
    data = yf.download([s + ".NS" for s in symbols], period=period, interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume, high, low = {}, {}, {}, {}
    for s in symbols:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 130:
                close[s] = c; volume[s] = data[t]["Volume"]; high[s] = data[t]["High"]; low[s] = data[t]["Low"]
        except Exception:
            pass
    return _frame(close, volume, high, low)


def _groww_bulk(symbols, days, pause=0.0):
    close, volume, high, low = {}, {}, {}, {}
    g = _groww()
    if not g:
        return _frame(close, volume, high, low)
    for s in symbols:
        df = _groww_candles(s, 1440, days)
        if df is not None and len(df) > 130:
            close[s] = df["close"]; volume[s] = df["volume"]; high[s] = df["high"]; low[s] = df["low"]
        if pause:
            time.sleep(pause)
    return _frame(close, volume, high, low)


def _frame(close, volume, high, low):
    # drop phantom trailing rows. yfinance appends a NaN/near-empty "today" row pre-market (before the
    # bar prints), making close.iloc[-1] NaN -> breadth 0 and EVERY gate collapses to 0 picks.
    # dropna(how="all") only catches FULLY empty rows; a partial phantom (a few stray names) survives, so
    # we also trim any trailing row whose coverage is far below the recent norm — relative, so it works for
    # both the ~750 liquid panel and the ~2700 full universe without a brittle absolute threshold.
    c = pd.DataFrame(close).sort_index().dropna(how="all")
    if c.shape[0] > 12 and c.shape[1] >= 20:
        cnt = c.notna().sum(axis=1)
        typical = float(cnt.iloc[-12:-2].median())
        while c.shape[0] > 1 and typical > 0 and float(cnt.iloc[-1]) < 0.5 * typical:
            c = c.iloc[:-1]; cnt = cnt.iloc[:-1]
    return {"close": c,
            "volume": pd.DataFrame(volume).reindex(c.index),
            "high": pd.DataFrame(high).reindex(c.index),
            "low": pd.DataFrame(low).reindex(c.index),
            "n": c.shape[1]}


def daily_panel(symbols, period="1y", groww_days=260, groww_cap=550):
    """Bulk daily panel {close,volume,high,low}. yfinance bulk first; Groww per-symbol fallback
    (capped to the first `groww_cap` symbols — pass a liquidity-ordered list) only when yfinance comes
    back CLEARLY throttled (< groww_cap, so Groww can actually do better — no wasted fallback otherwise)."""
    if _SOURCE == "groww":
        p = _groww_bulk(symbols[:groww_cap], groww_days); p["source"] = "groww"; return p
    p = _yf_bulk(symbols, period)
    n = p.get("n", 0)
    if _SOURCE != "yfinance" and n < groww_cap:          # only fall back when Groww (capped) could beat it
        gp = _groww_bulk(symbols[:groww_cap], groww_days)
        if gp["n"] > n:
            gp["source"] = f"groww-fallback ({gp['n']}/{min(len(symbols), groww_cap)} cap · yfinance throttled at {n})"
            return gp
    p["source"] = "yfinance"
    return p


# ---------------------------------------------------------------- health
def health():
    out = {"preference": _SOURCE}
    g = _groww()
    out["groww"] = bool(g)
    if _SOURCE == "groww":
        out["yfinance"] = "n/a (groww-only)"     # don't probe yfinance in Groww-only mode — we never call it
        return out
    try:
        import yfinance as yf
        df = yf.Ticker("RELIANCE.NS").history(period="5d", interval="1d")
        out["yfinance"] = bool(df is not None and not df.empty)
    except Exception:
        out["yfinance"] = False
    return out


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print("health:", json.dumps(health()))
    df = candles("RELIANCE", 1440, 60)
    print("candles RELIANCE:", None if df is None else f"{len(df)} bars via {df.attrs.get('source')}")
