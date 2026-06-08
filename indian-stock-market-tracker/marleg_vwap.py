"""
Marle-G VWAP analytics — swing + positional volume-weighted price reads.

Institutional cost-basis lens for the India pod. Everything is built on the
*typical price* tp = (H + L + C) / 3, weighted by daily volume, matching the
VWAP convention already used inline in marleg_server.equity_analysis().

What this computes
------------------
- Rolling daily VWAP over N sessions (N in {20, 50}): a trailing institutional
  average-fill estimate that resets each day's window.
- ANCHORED VWAP from two pivots inside the last 120 sessions:
    * anchored_low  — cumulative VWAP from the 120-day swing LOW to now
      (cost basis of buyers who stepped in at the bottom; a support shelf).
    * anchored_high — cumulative VWAP from the 120-day swing HIGH to now
      (cost basis of buyers trapped at the top; an overhead supply shelf).
- VWAP bands: +/-1σ and +/-2σ around the rolling 20d VWAP, where σ is the
  volume-weighted standard deviation of (price - vwap) over the window.
- Distance of current price from each VWAP (in %), plus plain-English reads
  (above/below = bullish/bearish; near a band edge = stretched).

Public API
----------
    vwap_analysis(ticker) -> dict
        {vwap20, vwap50, anchored_low, anchored_high,
         bands:{u1,u2,l1,l2}, current, dist{...}, reads:[...],
         anchors{...}, series:[{time, vwap20, vwap50}, ...], asof}

Designed to be served behind a Flask route using marleg_server.cached(...) +
_hist(...). It does NOT import marleg_server (avoids a circular import); it
pulls its own history with the identical yfinance call so it can also be run
stand-alone for validation:

    python marleg_vwap.py                 # validates RELIANCE + TCS
    python marleg_vwap.py INFY HDFCBANK   # validate any tickers
"""
import sys
import numpy as np
import pandas as pd
import yfinance as yf


# --------------------------------------------------------------- data access
def _yftk(sym):
    """Mirror of marleg_server.yftk — NSE suffix unless already qualified."""
    sym = sym.upper().strip()
    if sym.startswith("^") or "=" in sym or sym.endswith(".NS") or sym.endswith(".BO"):
        return sym
    return sym + ".NS"


def _load_hist(sym, period="1y", interval="1d"):
    """Stand-alone history loader matching marleg_server._hist exactly.

    When wired into the Flask app, prefer passing the server's own _hist(...)
    result into vwap_analysis(..., df=...) so we reuse the cached download.
    """
    df = yf.Ticker(_yftk(sym)).history(period=period, interval=interval, auto_adjust=False)
    return df if df is not None and len(df) else None


# --------------------------------------------------------------- VWAP math
def _typical_price(df):
    return (df["High"] + df["Low"] + df["Close"]) / 3.0


def _rolling_vwap(tp, vol, n):
    """Trailing N-session VWAP: sum(tp*vol)/sum(vol) over a rolling window."""
    pv = (tp * vol).rolling(n, min_periods=max(2, n // 2)).sum()
    vv = vol.rolling(n, min_periods=max(2, n // 2)).sum()
    return pv / vv.replace(0, np.nan)


def _anchored_vwap(tp, vol, start_idx):
    """Cumulative VWAP from start_idx (positional) to the end of the series.

    Returns a Series aligned to tp.index covering [start_idx:].
    """
    seg_tp = tp.iloc[start_idx:]
    seg_vol = vol.iloc[start_idx:]
    cum_pv = (seg_tp * seg_vol).cumsum()
    cum_v = seg_vol.cumsum().replace(0, np.nan)
    return cum_pv / cum_v


def _vw_std(tp, vol, vwap_series, n):
    """Volume-weighted stdev of (tp - vwap) over the trailing N window.

    sigma_t = sqrt( sum_w[(tp - vwap)^2 * vol] / sum_w[vol] ).
    """
    dev2 = (tp - vwap_series) ** 2
    num = (dev2 * vol).rolling(n, min_periods=max(2, n // 2)).sum()
    den = vol.rolling(n, min_periods=max(2, n // 2)).sum().replace(0, np.nan)
    return np.sqrt(num / den)


def _r2(x):
    return round(float(x), 2) if x is not None and np.isfinite(x) else None


# --------------------------------------------------------------- public API
def vwap_analysis(ticker, df=None, period="1y", lookback=120):
    """Full VWAP read for one ticker.

    Parameters
    ----------
    ticker : str   NSE symbol (e.g. "RELIANCE"); ".NS" added automatically.
    df     : DataFrame, optional  pre-fetched OHLCV (pass marleg_server._hist
             result to reuse the cached download). If None, fetched here.
    period : str   yfinance period when df is fetched here.
    lookback : int swing window for the anchored-VWAP pivots (default 120d).
    """
    tk = ticker.upper().strip()
    if df is None:
        df = _load_hist(tk, period)
    if df is None or len(df) < 60:
        return {"error": f"no data for {tk}", "tk": tk}

    df = df.dropna(subset=["High", "Low", "Close", "Volume"])
    high, low, close, vol = df["High"], df["Low"], df["Close"], df["Volume"]
    tp = _typical_price(df)
    ltp = float(close.iloc[-1])

    # rolling VWAPs
    v20 = _rolling_vwap(tp, vol, 20)
    v50 = _rolling_vwap(tp, vol, 50)
    vwap20 = float(v20.iloc[-1])
    vwap50 = float(v50.iloc[-1])

    # anchored VWAPs from the 120-day swing pivots
    win_n = min(lookback, len(df))
    win = df.iloc[-win_n:]
    # positional index (into the full df) of the swing low / high
    lo_pos = len(df) - win_n + int(np.argmin(win["Low"].values))
    hi_pos = len(df) - win_n + int(np.argmax(win["High"].values))
    av_low = _anchored_vwap(tp, vol, lo_pos)
    av_high = _anchored_vwap(tp, vol, hi_pos)
    anchored_low = float(av_low.iloc[-1])
    anchored_high = float(av_high.iloc[-1])

    # bands around the 20d VWAP (volume-weighted sigma)
    sig20 = _vw_std(tp, vol, v20, 20)
    sigma = float(sig20.iloc[-1])
    bands = {"u2": _r2(vwap20 + 2 * sigma), "u1": _r2(vwap20 + sigma),
             "l1": _r2(vwap20 - sigma), "l2": _r2(vwap20 - 2 * sigma)}

    # distance of current price from each VWAP (%)
    def _dpct(v):
        return round((ltp / v - 1) * 100, 2) if v and np.isfinite(v) and v != 0 else None
    dist = {"vwap20": _dpct(vwap20), "vwap50": _dpct(vwap50),
            "anchored_low": _dpct(anchored_low), "anchored_high": _dpct(anchored_high)}

    # how many sigma stretched from the 20d VWAP
    z = (ltp - vwap20) / sigma if sigma and np.isfinite(sigma) and sigma > 0 else 0.0

    # --------------------------------------------------- textual reads
    reads = []
    reads.append(
        f"Price {('above' if ltp >= vwap20 else 'below')} 20d VWAP "
        f"({dist['vwap20']:+}%) - short-term bias {'bullish' if ltp >= vwap20 else 'bearish'}."
    )
    reads.append(
        f"Price {('above' if ltp >= vwap50 else 'below')} 50d VWAP "
        f"({dist['vwap50']:+}%) - positional bias {'bullish' if ltp >= vwap50 else 'bearish'}."
    )
    # band stretch (ASCII-safe text so the served JSON never trips a console codec)
    if z >= 2:
        reads.append(f"Stretched +{z:.1f}sd above VWAP (>= upper 2sd band {bands['u2']}) - mean-reversion risk to the downside.")
    elif z <= -2:
        reads.append(f"Stretched {z:.1f}sd below VWAP (<= lower 2sd band {bands['l2']}) - oversold vs cost basis, snap-back risk up.")
    elif z >= 1:
        reads.append(f"Extended +{z:.1f}sd above VWAP (past upper 1sd {bands['u1']}) - momentum strong but nearing stretch.")
    elif z <= -1:
        reads.append(f"Extended {z:.1f}sd below VWAP (past lower 1sd {bands['l1']}) - weak, watch for capitulation.")
    else:
        reads.append(f"Within +/-1sd of VWAP ({z:+.1f}sd) - balanced, no edge from band stretch.")
    # anchored pivots
    reads.append(
        f"Swing-low anchored VWAP {round(anchored_low, 2)} ({dist['anchored_low']:+}% away) - "
        f"{'holding above' if ltp >= anchored_low else 'lost'} bottom-buyers' cost basis (support)."
    )
    reads.append(
        f"Swing-high anchored VWAP {round(anchored_high, 2)} ({dist['anchored_high']:+}% away) - "
        f"{'reclaimed' if ltp >= anchored_high else 'still below'} top-buyers' cost basis (overhead supply)."
    )

    # --------------------------------------------------- charting series (last 120)
    n_series = min(120, len(df))
    sv20 = v20.iloc[-n_series:]
    sv50 = v50.iloc[-n_series:]
    idx = df.index[-n_series:]
    series = []
    for d, a, b in zip(idx, sv20.values, sv50.values):
        series.append({
            "time": str(pd.Timestamp(d).date()),
            "vwap20": _r2(a) if np.isfinite(a) else None,
            "vwap50": _r2(b) if np.isfinite(b) else None,
        })

    # anchored-low series for charting (from the pivot to now)
    al = av_low
    anch_low_series = [{"time": str(pd.Timestamp(d).date()), "value": _r2(v)}
                       for d, v in zip(al.index, al.values) if np.isfinite(v)]

    return {
        "tk": tk,
        "current": round(ltp, 2),
        "vwap20": _r2(vwap20),
        "vwap50": _r2(vwap50),
        "anchored_low": _r2(anchored_low),
        "anchored_high": _r2(anchored_high),
        "bands": bands,
        "sigma": _r2(sigma),
        "z": round(float(z), 2),
        "dist": dist,
        "anchors": {
            "low": {"date": str(pd.Timestamp(df.index[lo_pos]).date()),
                    "price": round(float(low.iloc[lo_pos]), 2)},
            "high": {"date": str(pd.Timestamp(df.index[hi_pos]).date()),
                     "price": round(float(high.iloc[hi_pos]), 2)},
            "lookback": win_n,
        },
        "reads": reads,
        "series": series,
        "anchored_low_series": anch_low_series,
        "asof": str(pd.Timestamp(df.index[-1]).date()),
    }


# --------------------------------------------------------------- CLI validation
def _validate(tickers):
    try:  # make stand-alone printing robust on a cp1252 Windows console
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for t in tickers:
        print("=" * 70)
        print(f"VWAP ANALYSIS - {t}")
        print("=" * 70)
        r = vwap_analysis(t)
        if r.get("error"):
            print("  ERROR:", r["error"])
            continue
        print(f"  asof            : {r['asof']}")
        print(f"  current         : {r['current']}")
        print(f"  vwap20 / vwap50 : {r['vwap20']} / {r['vwap50']}")
        print(f"  anchored_low    : {r['anchored_low']}  (anchor {r['anchors']['low']['date']} @ {r['anchors']['low']['price']})")
        print(f"  anchored_high   : {r['anchored_high']}  (anchor {r['anchors']['high']['date']} @ {r['anchors']['high']['price']})")
        print(f"  bands u2/u1/l1/l2: {r['bands']['u2']} / {r['bands']['u1']} / {r['bands']['l1']} / {r['bands']['l2']}  (sigma {r['sigma']}, z {r['z']})")
        print(f"  dist %          : {r['dist']}")
        print(f"  series points   : {len(r['series'])}  (first {r['series'][0]['time']} -> last {r['series'][-1]['time']})")
        print(f"  anchored_low pts: {len(r['anchored_low_series'])}")
        print("  reads:")
        for line in r["reads"]:
            print("    -", line)
        print()


if __name__ == "__main__":
    syms = [s.upper() for s in sys.argv[1:]] or ["RELIANCE", "TCS"]
    _validate(syms)
