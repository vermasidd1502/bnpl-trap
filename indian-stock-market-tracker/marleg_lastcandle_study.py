"""
marleg_lastcandle_study.py — does a FINAL-10-MIN volume spike add edge to the fib x Ichimoku x U/D confluence?

The user's question. Tested on the OVERLAP of yfinance 5m intraday (last ~60 sessions, for the closing-volume
spike) x the canonical daily panel (for the confluence: fib position, Ichimoku, U/D ratio) x NEXT-day return.
HONEST: intraday history is shallow (~40-60 sessions), so this is INDICATIVE (small samples), NOT 5y-grade.
Prior finding: closing-pressure / last-candle MEAN-REVERTS at the micro-horizon — so we expect the spike to
add little or hurt. We report whatever the data says.

Signal components (evaluated at session close t):
  last_spike : final 10-min volume >= 2.5x the session's average 5-min volume (a closing surge)
  fib        : close > 0.618 of its 120d range
  ichi       : close above the Ichimoku cloud AND above the Kijun (26)
  ud         : 20d up/down volume ratio > its 50d MA
Forward = next-day close-to-close, net of cost. Win ratio + mean for each component & the full confluence.

  python marleg_lastcandle_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_lastcandle_study.json")
COST = 0.30
MAXN = 70


def _ud(close, volume):
    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    return ud, ud.rolling(50).mean()


def _ichi_bull(h, l, c):
    ten = (h.rolling(9).max() + l.rolling(9).min()) / 2
    kij = (h.rolling(26).max() + l.rolling(26).min()) / 2
    spanA = ((ten + kij) / 2).shift(26)
    spanB = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
    cloud_top = pd.concat([spanA, spanB], axis=1).max(axis=1)
    return (c > cloud_top) & (c > kij)


def last_spike_by_session(tk):
    """yfinance 5m (60d) -> {date: bool} is the final 10-min a >=2.5x volume surge vs the session's 5m avg?"""
    try:
        import yfinance as yf
        df = yf.Ticker(tk + ".NS").history(period="60d", interval="5m")
    except Exception:
        return None
    if df is None or df.empty:
        return None
    v = df["Volume"]
    if getattr(v.index, "tz", None) is not None:
        v.index = v.index.tz_localize(None)
    out, bydate = {}, {}
    for ts, vol in v.items():
        bydate.setdefault(ts.normalize(), []).append(float(vol))
    for d, vols in bydate.items():
        vols = [x for x in vols if x == x]
        if len(vols) < 10:
            continue
        avg = float(np.mean(vols))
        last10 = float(np.mean(vols[-2:]))
        out[d] = bool(avg > 0 and last10 >= 2.5 * avg)
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    panel = pb.load()
    if not panel:
        print("no panel"); return
    close = panel["close"].copy()
    if getattr(close.index, "tz", None) is not None:
        close.index = close.index.tz_localize(None)
    high = panel["high"].reindex(close.index); low = panel["low"].reindex(close.index); vol = panel["volume"].reindex(close.index)
    import marleg_options_monitor as mom
    U = [s for s in sorted(mom.FNO_UNDERLYINGS) if s in close.columns][:MAXN]
    print(f"last-candle study on {len(U)} liquid F&O names (yfinance 5m 60d x panel daily)...")
    keys = ["baseline", "last_spike", "confluence_daily", "full", "spike_x_fib", "spike_x_ichi"]
    res = {k: [] for k in keys}
    fwd_all = close.pct_change().shift(-1)
    kept = 0
    for s in U:
        c = close[s].dropna()
        if len(c) < 150:
            continue
        h = high[s].reindex(c.index); l = low[s].reindex(c.index); v = vol[s].reindex(c.index)
        hh = c.rolling(120).max(); ll = c.rolling(120).min(); fib = (c - ll) / (hh - ll)
        ichi = _ichi_bull(h, l, c)
        ud, udma = _ud(c, v)
        fwd = fwd_all[s].reindex(c.index)
        spikes = last_spike_by_session(s)
        if not spikes:
            continue
        kept += 1
        for d, isspike in spikes.items():
            if d not in c.index:
                continue
            f = fwd.get(d)
            if f is None or f != f:
                continue
            fibok = bool(fib.get(d, 0) > 0.618)
            ichiok = bool(ichi.get(d, False))
            udok = bool(ud.get(d, np.nan) > udma.get(d, np.nan))
            res["baseline"].append(f)
            if isspike:
                res["last_spike"].append(f)
            if fibok and ichiok and udok:
                res["confluence_daily"].append(f)
            if fibok and ichiok and udok and isspike:
                res["full"].append(f)
            if isspike and fibok:
                res["spike_x_fib"].append(f)
            if isspike and ichiok:
                res["spike_x_ichi"].append(f)

    def agg(a):
        x = np.array(a, float)
        return None if len(x) < 30 else {"n": int(len(x)), "win": round(float((x > 0).mean()) * 100, 1),
                                         "net": round(float(x.mean()) * 100 - COST, 3)}
    out = {"cost": COST, "kept": kept, "results": {k: agg(res[k]) for k in keys},
           "caveat": "Indicative only — intraday history ~40-60 sessions, small samples (n<30 shows as null). "
                     "Closing-pressure mean-reverts at the micro-horizon (prior finding), so a closing spike "
                     "is not expected to add much on the long side."}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\nkept {kept} names. NEXT-DAY win ratio + net (close-to-close, net of {COST}% cost):")
    print(f"  {'signal':<18}{'n':>7}{'win%':>8}{'net%':>9}")
    for k in keys:
        r = out["results"][k]
        if r:
            print(f"  {k:<18}{r['n']:>7}{r['win']:>8}{r['net']:>9}")
        else:
            print(f"  {k:<18}{'(n<30)':>7}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
