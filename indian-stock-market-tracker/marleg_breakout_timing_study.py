"""
marleg_breakout_timing_study.py — does "fight then burst" beat raw breakouts, and what's the best entry?

The thesis: the fib gate (price > 0.618 of range) is stronger when the breakout resolves a TIGHT
CONSOLIDATION — a "fight" where buyers and sellers reached balance (range/volatility contraction) —
rather than firing mid-rally. We test two things across the F&O universe (~3y):

1) SETUP QUALITY — forward returns (5/10/20d, net of cost) of:
     fib_gate      : price > 0.618 of its 120d range (the current gate, baseline)
     raw_breakout  : close breaks above the prior 20d high  (no base required)
     fight_burst   : breakout that resolves a TIGHT 20d base (range in the stock's own bottom tercile)
     tight_x_vol   : fight_burst + breakout-day volume > 1.5x average (volume confirmation)

2) ENTRY TIMING (for fight_burst events) — where to actually long:
     breakout_close : buy the breakout day's close
     pullback_retest: wait for a pullback to the box top within 5 days, buy the retest (note FILL rate)
     delay_2d       : buy 2 days after the breakout (chasing)

  python marleg_breakout_timing_study.py
"""
import json, os, sys, time
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_options_monitor as mom

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_breakout_timing_study.json")
COST = 0.30
HORIZONS = [5, 10, 20]


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    U = sorted(mom.FNO_UNDERLYINGS)
    print(f"downloading {len(U)} F&O symbols (3y daily) for breakout-timing study...")
    setups = {k: {h: [] for h in HORIZONS} for k in ["fib_gate", "raw_breakout", "fight_burst", "tight_x_vol"]}
    timing = {k: {"ret": [], "fills": 0, "events": 0} for k in ["breakout_close", "pullback_retest", "delay_2d"]}
    CH, kept = 60, 0
    for i in range(0, len(U), CH):
        chunk = U[i:i + CH]
        for attempt in range(3):
            try:
                data = yf.download([s + ".NS" for s in chunk], period="3y", interval="1d",
                                   group_by="ticker", progress=False, threads=True)
                break
            except Exception as e:
                print(f"  retry {attempt+1} ({str(e)[:40]})"); time.sleep(20)
        else:
            continue
        for s in chunk:
            try:
                df = data[s + ".NS"][["High", "Low", "Close", "Volume"]].dropna()
            except Exception:
                continue
            if len(df) < 180:
                continue
            kept += 1
            h, l, c, v = (df[x].values for x in ["High", "Low", "Close", "Volume"])
            n = len(c)
            hh20 = pd.Series(h).rolling(20).max().shift(1).values     # prior 20d box top
            ll20 = pd.Series(l).rolling(20).min().shift(1).values
            rng20 = (hh20 - ll20) / ll20
            tight_thr = np.nanpercentile(rng20[~np.isnan(rng20)], 33) if np.isfinite(rng20).any() else np.nan
            hh120 = pd.Series(h).rolling(120).max().shift(1).values
            ll120 = pd.Series(l).rolling(120).min().shift(1).values
            fibpos = (c - ll120) / (hh120 - ll120)
            vavg = pd.Series(v).rolling(20).mean().shift(1).values
            fwd = {hz: np.concatenate([c[hz:] / c[:-hz] - 1, np.full(hz, np.nan)]) for hz in HORIZONS}

            for t in range(121, n - 1):
                if np.isnan(hh20[t]) or np.isnan(fibpos[t]):
                    continue
                brk = c[t] > hh20[t] and c[t - 1] <= hh20[t - 1]
                tight = (not np.isnan(tight_thr)) and rng20[t] <= tight_thr
                volok = vavg[t] and v[t] > 1.5 * vavg[t]
                if fibpos[t] > 0.618:
                    for hz in HORIZONS:
                        if not np.isnan(fwd[hz][t]): setups["fib_gate"][hz].append(fwd[hz][t])
                if brk:
                    tag = "fight_burst" if tight else "raw_breakout"
                    for hz in HORIZONS:
                        if not np.isnan(fwd[hz][t]):
                            setups[tag][hz].append(fwd[hz][t])
                            if tight and volok: setups["tight_x_vol"][hz].append(fwd[hz][t])
                    if tight:    # entry-timing test on fight_burst events (10-day horizon)
                        hz = 10
                        timing["breakout_close"]["events"] += 1
                        if t + hz < n:
                            timing["breakout_close"]["ret"].append(c[t + hz] / c[t] - 1); timing["breakout_close"]["fills"] += 1
                        if t + 2 + hz < n:
                            timing["delay_2d"]["events"] += 1; timing["delay_2d"]["fills"] += 1
                            timing["delay_2d"]["ret"].append(c[t + 2 + hz] / c[t + 2] - 1)
                        box = hh20[t]; filled = False     # pullback to the box top within 5 days
                        timing["pullback_retest"]["events"] += 1
                        for j in range(t + 1, min(t + 6, n)):
                            if l[j] <= box and j + hz < n:
                                timing["pullback_retest"]["ret"].append(c[j + hz] / box - 1)
                                timing["pullback_retest"]["fills"] += 1; filled = True; break

        print(f"  {min(i + CH, len(U))}/{len(U)} (kept {kept})")
        time.sleep(5)

    def agg(arr):
        x = np.array(arr)
        return None if len(x) < 40 else {"n": int(len(x)), "mean": round(float(x.mean()) * 100, 3),
                                         "net": round(float(x.mean()) * 100 - COST, 3),
                                         "win": round(float((x > 0).mean()) * 100, 1)}
    res = {"cost": COST, "setups": {}, "timing": {}}
    for k in setups:
        res["setups"][k] = {hz: agg(setups[k][hz]) for hz in HORIZONS}
    for k in timing:
        a = agg(timing[k]["ret"]);
        if a: a["fill_rate"] = round(timing[k]["fills"] / max(1, timing[k]["events"]) * 100, 1)
        res["timing"][k] = a
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print("\n========= SETUP QUALITY (10-day forward, net of cost) =========")
    print(f"  {'setup':<16}{'n':>9}{'mean%':>8}{'net%':>8}{'win%':>7}")
    for k in ["fib_gate", "raw_breakout", "fight_burst", "tight_x_vol"]:
        r = res["setups"][k].get(10)
        if r: print(f"  {k:<16}{r['n']:>9,}{r['mean']:>8}{r['net']:>8}{r['win']:>7}")
    print("\n========= ENTRY TIMING for fight_burst (10-day) =========")
    print(f"  {'timing':<18}{'n':>8}{'net%':>8}{'win%':>7}{'fill%':>8}")
    for k in ["breakout_close", "pullback_retest", "delay_2d"]:
        r = res["timing"].get(k)
        if r: print(f"  {k:<18}{r['n']:>8,}{r['net']:>8}{r['win']:>7}{r.get('fill_rate','—'):>8}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
