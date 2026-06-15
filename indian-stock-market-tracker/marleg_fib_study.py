"""
marleg_fib_study.py — does longing BELOW the 120d high (fib<1, "room") beat longing
AT/ABOVE it (breakout)? The empirical test of the don't-chase thesis.

Two metrics — the gated scan's `fib` = position in the rolling-120d range, capped at 1.0
(=new high), so it can't exceed 1:
  A) fib_pos = (close - 120d_low) / (120d_high - 120d_low)  ∈ [0,1]   (the gate's own metric)
  B) ext     = close / prior_120d_high (excluding the last 5 bars)     (>1 = real breakout)

For every stock/day with enough history we bucket the entry by fib_pos / ext and record the
forward return at H = 5/10/20 trading days, then report mean / median / win% per bucket,
GROSS and NET of a round-trip cost. Pooled across the liquid universe over ~3y (single pass).

Caveat: yfinance = currently-listed names -> mild survivorship bias (lifts every bucket about
equally, so the *relative* comparison between buckets stays fair).

  python marleg_fib_study.py
"""
import json, os, sys
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_volume_scan as mvs

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_fib_study.json")
COST = 0.30          # % round-trip cost assumption (swing/delivery long)
HORIZONS = [5, 10, 20]
WIN = 120            # range lookback (matches the gated scan)

FIB_BUCKETS = [("<0.618 (deep in range)", -1, 0.618), ("0.618-0.80", 0.618, 0.80),
               ("0.80-0.95", 0.80, 0.95), ("0.95-<1.0 (near high)", 0.95, 0.99999),
               ("=1.0 (at 120d high)", 0.99999, 1.0001)]
EXT_BUCKETS = [("<0.90 (well below high)", -1, 0.90), ("0.90-0.98", 0.90, 0.98),
               ("0.98-1.00 (just below)", 0.98, 1.0), ("1.00-1.05 (fresh breakout)", 1.0, 1.05),
               (">1.05 (extended breakout)", 1.05, 9e9)]


def _stats(x):
    x = x[~np.isnan(x)]
    if len(x) < 30:
        return None
    return {"n": int(len(x)), "mean": round(float(np.mean(x)) * 100, 3),
            "median": round(float(np.median(x)) * 100, 3),
            "win": round(float(np.mean(x > 0)) * 100, 1)}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    U = mvs.load_universe()
    print(f"downloading {len(U)} symbols (3y daily, single pass)...")
    FIB = {h: [] for h in HORIZONS}; EXT = {h: [] for h in HORIZONS}; FWD = {h: [] for h in HORIZONS}
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
                c = data[s + ".NS"]["Close"].dropna()
            except Exception:
                continue
            if len(c) < WIN + max(HORIZONS) + 30:
                continue
            kept += 1
            hh = c.rolling(WIN).max(); ll = c.rolling(WIN).min()
            fib = (c - ll) / (hh - ll).replace(0, np.nan)
            ext = c / c.shift(5).rolling(WIN).max()
            for h in HORIZONS:
                fwd = c.shift(-h) / c - 1.0
                df = pd.DataFrame({"fib": fib, "ext": ext, "fwd": fwd}).dropna()
                FIB[h].append(df["fib"].values); EXT[h].append(df["ext"].values); FWD[h].append(df["fwd"].values)
        print(f"  {min(i + CH, len(U))}/{len(U)} (kept {kept})")

    result = {"cost_pct": COST, "horizons": HORIZONS, "kept_stocks": kept, "fib_pos": {}, "ext": {}}
    for h in HORIZONS:
        fb = np.concatenate(FIB[h]); ex = np.concatenate(EXT[h]); fw = np.concatenate(FWD[h])
        for key, buckets, arr in [("fib_pos", FIB_BUCKETS, fb), ("ext", EXT_BUCKETS, ex)]:
            result[key][h] = []
            for name, lo, hi in buckets:
                st = _stats(fw[(arr >= lo) & (arr < hi)])
                if st:
                    st["bucket"] = name; st["net_mean"] = round(st["mean"] - COST, 3)
                    result[key][h].append(st)

    json.dump(result, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    for metric, label in [("fib_pos", "FIB POSITION in 120d range (gate's metric, <=1)"),
                          ("ext", "EXTENSION = price / prior-120d-high (>1 = breakout)")]:
        print(f"\n================ {label} ================")
        for h in HORIZONS:
            print(f"\n  forward {h} trading days (net of {COST}% round-trip):")
            print(f"    {'bucket':<28}{'n':>10}{'mean%':>9}{'net%':>9}{'win%':>8}")
            for st in result[metric][h]:
                print(f"    {st['bucket']:<28}{st['n']:>10,}{st['mean']:>9}{st['net_mean']:>9}{st['win']:>8}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
