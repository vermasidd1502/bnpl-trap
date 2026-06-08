"""
Live gated-longs screen (O'Neil/CAN-SLIM top-down confluence). A stock qualifies
only when ALL gates fire at the latest bar:
  1. SECTOR/GROUP : its sector is top-40% by 20-day relative strength (leading group)
  2. VOLUME       : U/D ratio > its own 50-day MA  AND  rising (vs 10d ago)
  3. FIBONACCI    : price above the 0.618 retracement of its 120-day range
Writes marleg_gated_cache.json (served by /api/gated).
"""
import json, os, time
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_volume_scan as mvs

HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
OUT = os.path.join(HERE, "marleg_gated_cache.json")
U = mvs.SEED


def main():
    print(f"downloading {len(U)} symbols (1y)...")
    data = yf.download([s + ".NS" for s in U], period="1y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume, high, low = {}, {}, {}, {}
    for s in U:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 130:
                close[s] = c; volume[s] = data[t]["Volume"]; high[s] = data[t]["High"]; low[s] = data[t]["Low"]
        except Exception:
            pass
    close = pd.DataFrame(close)
    volume = pd.DataFrame(volume).reindex(close.index)
    print(f"universe with data: {close.shape[1]} stocks")

    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    members = {}
    for s, sec in secmap.items():
        members.setdefault(sec, []).append(s)
    ret20 = close.pct_change(20).iloc[-1]
    sec_ret = pd.Series({sec: ret20[m].mean() for sec, m in members.items()})
    sec_rank = sec_ret.rank(ascending=False, pct=True)

    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    ud_now, ud_ma, ud_10 = ud.iloc[-1], ud.rolling(50).mean().iloc[-1], ud.iloc[-11]
    hh = close.rolling(120).max().iloc[-1]; ll = close.rolling(120).min().iloc[-1]
    fibpos = (close.iloc[-1] - ll) / (hh - ll).replace(0, np.nan)

    picks = []
    for s in close.columns:
        sec = secmap[s]
        if not (sec_rank.get(sec, 1) <= 0.40):
            continue
        if not (ud_now[s] > ud_ma[s] and ud_now[s] > ud_10[s]):
            continue
        if not (fibpos[s] > 0.618):
            continue
        price = float(close[s].iloc[-1])
        h, l, c = high[s], low[s], close[s]
        pc = c.shift(1)
        tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])
        picks.append({"s": s, "n": NAMES.get(s, s), "sector": sec,
                      "ud": round(float(ud_now[s]), 2), "ud_ma": round(float(ud_ma[s]), 2),
                      "fib": round(float(fibpos[s]), 2), "sec_rank": round(float(sec_rank.get(sec, 1)) * 100),
                      "price": round(price, 2), "target": round(price + 2 * atr, 1),
                      "tgtpct": round(2 * atr / price * 100, 1), "stop": round(price - atr, 1)})
    picks.sort(key=lambda x: -x["ud"])
    json.dump({"asof": time.strftime("%Y-%m-%d %H:%M IST"), "n": len(picks),
               "universe": close.shape[1], "picks": picks}, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"gated longs: {len(picks)} / {close.shape[1]}  ->  {OUT}")
    for p in picks[:12]:
        print(f"  {p['s']:<12} {p['sector']:<26} U/D {p['ud']}>{p['ud_ma']}  fib {p['fib']}  -> tgt {p['target']} (+{p['tgtpct']}%)")


if __name__ == "__main__":
    main()
