"""
marleg_mood_study.py — does the mood needle actually predict forward returns?

Uses the SAME scoring as the live meter (marleg_mood._frame), so this validates exactly what
the needle shows. For every stock/day we take the mood score and the forward return at
H=1/3/5 days, pool across the liquid universe (3y), bucket by score, and report:
  - LONG net (fwd - 0.3% cost) : does a strong reading pay if you go long?
  - SHORT net (-fwd - 0.5% cost): is a weak reading shortable (expected: NO)?
  - win% (stock up)

Caveat: yfinance survivors -> mild upward bias; the relative ordering across buckets is the signal.

  python marleg_mood_study.py
"""
import json, os, sys
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_volume_scan as mvs
import marleg_mood as mm

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_mood_study.json")
LONG_COST, SHORT_COST = 0.30, 0.50
HORIZONS = [1, 3, 5]
BUCKETS = [("<=-50 (strong weak)", -1e9, -50), ("-50..-20 (weak)", -50, -20),
           ("-20..20 (neutral)", -20, 20), ("20..50 (long-lean)", 20, 50),
           (">=50 (strong long)", 50, 1e9)]


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    U = mvs.load_universe()
    print(f"downloading {len(U)} symbols (3y daily) for mood backtest...")
    SC = {h: [] for h in HORIZONS}; FW = {h: [] for h in HORIZONS}
    CH, kept = 200, 0
    for i in range(0, len(U), CH):
        chunk = U[i:i + CH]
        try:
            data = yf.download([s + ".NS" for s in chunk], period="3y", interval="1d",
                               group_by="ticker", progress=False, threads=True)
        except Exception:
            continue
        for s in chunk:
            try:
                sub = data[s + ".NS"][["Open", "High", "Low", "Close", "Volume"]].dropna()
            except Exception:
                continue
            if len(sub) < 60:
                continue
            kept += 1
            df = sub.rename(columns=str.lower)
            score = mm._frame(df)["score"]
            c = df["close"]
            for h in HORIZONS:
                fwd = c.shift(-h) / c - 1.0
                m = score.notna() & fwd.notna()
                SC[h].append(score[m].values); FW[h].append(fwd[m].values)
        print(f"  {min(i + CH, len(U))}/{len(U)} (kept {kept})")

    result = {"long_cost": LONG_COST, "short_cost": SHORT_COST, "horizons": HORIZONS, "kept": kept, "rows": {}}
    for h in HORIZONS:
        sc = np.concatenate(SC[h]); fw = np.concatenate(FW[h])
        result["rows"][h] = []
        for name, lo, hi in BUCKETS:
            sel = (sc >= lo) & (sc < hi)
            x = fw[sel]
            if len(x) < 30:
                continue
            mean = float(np.mean(x)) * 100
            result["rows"][h].append({"bucket": name, "n": int(len(x)),
                                      "fwd_mean": round(mean, 3),
                                      "long_net": round(mean - LONG_COST, 3),
                                      "short_net": round(-mean - SHORT_COST, 3),
                                      "win": round(float(np.mean(x > 0)) * 100, 1)})

    json.dump(result, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n================ MOOD SCORE -> forward return ({kept} stocks) ================")
    for h in HORIZONS:
        print(f"\n  forward {h} day(s):")
        print(f"    {'mood bucket':<24}{'n':>10}{'fwd%':>8}{'LONG net%':>11}{'SHORT net%':>12}{'win%':>7}")
        for r in result["rows"][h]:
            print(f"    {r['bucket']:<24}{r['n']:>10,}{r['fwd_mean']:>8}{r['long_net']:>11}{r['short_net']:>12}{r['win']:>7}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
