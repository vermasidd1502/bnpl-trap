"""
Marle-G next-session WATCHLIST (from today's close, EOD data).

Tiers:
  TRIGGERED LONG : full accumulation gate fires now -> ready, with ATR stop/target.
  WATCH LONG     : accumulation building, price approaching the 0.618 breakout
                   (Fib 0.50-0.618) in a leading sector -> watch for the break tomorrow.
  AVOID / SHORT  : distribution in a lagging sector below 0.382 Fib.
RSI entry-timing tagged (EARLY<45 best, DEAD-ZONE 45-60 skipped, EXTENDED/CHASING late).

  python marleg_watchlist.py
"""
import os, json
import numpy as np, pandas as pd, yfinance as yf
import marleg_volume_scan as v

HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))


def rsi_tag(r):
    return "EARLY" if r < 45 else "DEAD-ZONE" if r < 60 else "EXTENDED" if r < 70 else "CHASING"


def main():
    U = v.SEED
    data = yf.download([s + ".NS" for s in U] + ["^NSEI"], period="1y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume, high, low = {}, {}, {}, {}
    for s in U:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 200:
                close[s] = c; volume[s] = data[t]["Volume"]; high[s] = data[t]["High"]; low[s] = data[t]["Low"]
        except Exception:
            pass
    close = pd.DataFrame(close); volume = pd.DataFrame(volume).reindex(close.index)
    high = pd.DataFrame(high).reindex(close.index); low = pd.DataFrame(low).reindex(close.index)
    t = close.index[-1]

    # NIFTY regime
    try:
        ns = data["^NSEI"]["Close"].dropna()
        nlast = float(ns.iloc[-1]); nsma = float(ns.rolling(50).mean().iloc[-1]); n5 = (nlast/float(ns.iloc[-6])-1)*100
        regime = f"NIFTY {nlast:,.0f} ({n5:+.1f}% 5d), {'above' if nlast>nsma else 'below'} 50-DMA -> {'risk-on lean' if nlast>nsma else 'cautious / below trend'}"
    except Exception:
        regime = "regime n/a"

    ret20 = close.pct_change(20, fill_method=None)
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    members = {}
    for s in close.columns: members.setdefault(secmap[s], []).append(s)
    sec_ret = pd.DataFrame({sec: ret20[m].mean(axis=1) for sec, m in members.items()})
    srank = pd.DataFrame({s: sec_ret.rank(axis=1, ascending=False, pct=True)[secmap[s]] for s in close.columns})
    d = np.sign(close.diff()); upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    ud_ma = ud.rolling(50).mean()
    hh = close.rolling(120).max(); ll = close.rolling(120).min(); fib = (close - ll) / (hh - ll).replace(0, np.nan)
    delta = close.diff(); gain = delta.clip(lower=0).rolling(14).mean(); loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    def row(s, withlevels=False):
        r = float(rsi.loc[t, s]); o = {"s": s, "sector": secmap[s], "ud": round(float(ud.loc[t, s]), 2),
              "udma": round(float(ud_ma.loc[t, s]), 2), "fib": round(float(fib.loc[t, s]), 2),
              "rsi": round(r, 1), "tag": rsi_tag(r), "secRS": round(float(srank.loc[t, s]) * 100),
              "price": round(float(close.loc[t, s]), 1)}
        if withlevels:
            c, h, l = close[s], high[s], low[s]; pc = c.shift(1)
            tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
            a = float(tr.rolling(14).mean().iloc[-1]); e = o["price"]
            o["stop"] = round(e - 2 * a, 1); o["target"] = round(e + 4 * a, 1)
        return o

    acc = (ud > ud_ma) & (ud > ud.shift(10))
    triggered = [row(s, True) for s in close.columns if (srank.loc[t, s] <= 0.40) and acc.loc[t, s] and fib.loc[t, s] > 0.618]
    watch = [row(s) for s in close.columns if (srank.loc[t, s] <= 0.45) and acc.loc[t, s]
             and 0.50 < fib.loc[t, s] <= 0.618 and not (45 <= float(rsi.loc[t, s]) < 60)]
    avoid = [row(s) for s in close.columns if (srank.loc[t, s] >= 0.60) and (ud.loc[t, s] < ud_ma.loc[t, s])
             and (ud.loc[t, s] < ud.shift(10).loc[t, s]) and fib.loc[t, s] < 0.382]
    order = {"EARLY": 0, "EXTENDED": 1, "CHASING": 2, "DEAD-ZONE": 3}
    triggered.sort(key=lambda x: (order[x["tag"]], -x["ud"]))
    watch.sort(key=lambda x: (-x["ud"]))
    avoid.sort(key=lambda x: x["ud"])

    print(f"MARLE-G WATCHLIST  |  as of {t.date()} close  |  {regime}\n")
    print(f"TRIGGERED LONG ({len(triggered)}) — accumulation gate fires now (entry / 2xATR stop / 2:1 target):")
    for o in triggered:
        print(f"   {o['s']:<12} {o['tag']:<9} U/D {o['ud']}>{o['udma']}  Fib {o['fib']}  RSI {o['rsi']}  secRS {o['secRS']}%  "
              f"Rs{o['price']}  stop {o['stop']}  tgt {o['target']}  [{o['sector'][:20]}]")
    print(f"\nWATCH LONG ({len(watch)}) — accumulation building, nearing the 0.618 breakout (watch for the break):")
    for o in watch[:12]:
        print(f"   {o['s']:<12} {o['tag']:<9} U/D {o['ud']}>{o['udma']}  Fib {o['fib']} (->0.618)  RSI {o['rsi']}  secRS {o['secRS']}%  Rs{o['price']}  [{o['sector'][:20]}]")
    print(f"\nAVOID / DISTRIBUTION ({len(avoid)}) — lagging sector, U/D falling, below 0.382 Fib:")
    for o in avoid[:12]:
        print(f"   {o['s']:<12} U/D {o['ud']}<{o['udma']}  Fib {o['fib']}  RSI {o['rsi']}  secRS {o['secRS']}%  Rs{o['price']}  [{o['sector'][:20]}]")

    json.dump({"asof": str(t.date()), "regime": regime, "triggered": triggered, "watch": watch, "avoid": avoid},
              open(os.path.join(HERE, "marleg_watchlist.json"), "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
