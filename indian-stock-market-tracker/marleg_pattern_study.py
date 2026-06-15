"""
marleg_pattern_study.py — does each technical pattern actually work in the Indian market?

Detects every historical occurrence of every pattern (marleg_patterns.signals) across the
F&O-liquid universe over ~3y, then measures the forward return at H=5/10/20 days and the hit-rate.
Bias-aware: long-bias patterns are scored as longs (net = fwd - long cost), short-bias as shorts
(net = -fwd - short cost). The pod then GATES each pattern's verdict on this — folklore patterns
with no edge get flagged, edge patterns get trusted.

Universe = NSE F&O (liquid, cleaner patterns, fewer requests). Caveat: survivorship lifts longs.

  python marleg_pattern_study.py
"""
import json, os, sys, time
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_patterns as mp
import marleg_options_monitor as mom

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_pattern_study.json")
COST_L, COST_S = 0.30, 0.50
HORIZONS = [5, 10, 20]


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    U = sorted(mom.FNO_UNDERLYINGS)
    print(f"downloading {len(U)} F&O symbols (3y daily) for pattern reliability...")
    acc = {name: {h: [] for h in HORIZONS} for name in mp.META}
    CH, kept = 60, 0
    for i in range(0, len(U), CH):
        chunk = U[i:i + CH]
        for attempt in range(3):
            try:
                data = yf.download([s + ".NS" for s in chunk], period="3y", interval="1d",
                                   group_by="ticker", progress=False, threads=True)
                break
            except Exception as e:
                print(f"  retry {attempt+1} ({str(e)[:50]})"); time.sleep(20)
        else:
            continue
        for s in chunk:
            try:
                df = data[s + ".NS"][["Open", "High", "Low", "Close"]].dropna().rename(columns=str.lower)
            except Exception:
                continue
            if len(df) < 120:
                continue
            kept += 1
            c = df["close"]
            fwd = {h: (c.shift(-h) / c - 1.0) for h in HORIZONS}
            for name, d in mp.signals(df).items():
                sig = d["sig"]
                for h in HORIZONS:
                    x = fwd[h][sig & fwd[h].notna()]
                    if len(x):
                        acc[name][h].append(x.values)
        print(f"  {min(i + CH, len(U))}/{len(U)} (kept {kept})")
        time.sleep(5)

    result = {"cost_long": COST_L, "cost_short": COST_S, "universe": "F&O", "kept": kept, "patterns": {}}
    for name in mp.META:
        bias = mp.META[name][0]
        row = {"bias": bias, "h": {}}
        for h in HORIZONS:
            arr = acc[name][h]
            if not arr:
                continue
            x = np.concatenate(arr)
            if len(x) < 50:
                continue
            fwd_mean = float(np.mean(x)) * 100
            if bias == "short":
                net = -fwd_mean - COST_S
                hit = float(np.mean(x < 0)) * 100
            else:
                net = fwd_mean - COST_L
                hit = float(np.mean(x > 0)) * 100
            row["h"][h] = {"n": int(len(x)), "fwd_mean": round(fwd_mean, 3),
                           "net": round(net, 3), "hit": round(hit, 1)}
        if row["h"]:
            result["patterns"][name] = row

    json.dump(result, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    # rank by 10-day net
    rankable = [(n, r["h"].get(10, {}).get("net", -99)) for n, r in result["patterns"].items()]
    rankable.sort(key=lambda z: -z[1])
    print(f"\n================ PATTERN RELIABILITY (India F&O, {kept} stocks) — ranked by 10-day net ================")
    print(f"  {'pattern':<22}{'bias':<7}{'n':>8}{'fwd%':>8}{'net%':>8}{'hit%':>7}")
    for n, _ in rankable:
        r = result["patterns"][n]; hh = r["h"].get(10) or {}
        print(f"  {n:<22}{r['bias']:<7}{hh.get('n',0):>8,}{hh.get('fwd_mean',0):>8}{hh.get('net',0):>8}{hh.get('hit',0):>7}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
