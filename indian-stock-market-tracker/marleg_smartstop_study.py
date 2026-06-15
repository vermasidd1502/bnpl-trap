"""
marleg_smartstop_study.py — does the smart stop beat a fixed/plain stop, and cut shakeouts?

Same long entry for every method (close crosses above the 20-EMA), then exit each trade under:
  fixed_intrabar : fixed -6%, triggered by the intraday LOW (the naive stop that gets wicked out)
  fixed_close    : fixed -6%, triggered only on the CLOSE
  chand_close    : 3×ATR Chandelier trail, CLOSE-based
  chand_gated    : 3×ATR Chandelier, CLOSE-based, + 20-EMA thesis gate (exit only if also below EMA)

Per method: avg net return, win%, avg bars held, and the SHAKEOUT rate (stopped out, yet price was
higher 5 bars later — "lost a winner to a wiggle"). F&O universe, ~2y. Net of 0.4% cost.

  python marleg_smartstop_study.py
"""
import json, os, sys, time
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_options_monitor as mom

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_smartstop_study.json")
HORIZON, COST = 25, 0.4
METHODS = ["fixed_intrabar", "fixed_close", "chand_close", "chand_gated"]


def _atr(h, l, c, n=22):
    tr = np.maximum(h - l, np.maximum(np.abs(h - np.roll(c, 1)), np.abs(l - np.roll(c, 1))))
    return pd.Series(tr).rolling(n).mean().values


def sim_entry(o, h, l, c, ema, atr, ei):
    n = len(c); entry = c[ei]; res = {}
    for m in METHODS:
        hh = h[ei]; stop0 = entry * 0.94; exit_px = None; bars = HORIZON; stopped = False
        for k in range(1, HORIZON + 1):
            t = ei + k
            if t >= n:
                exit_px = c[n - 1]; bars = k; break
            if m.startswith("chand"):
                hh = max(hh, h[t]); stop = hh - 3 * atr[t]
            else:
                stop = stop0
            if m == "fixed_intrabar" and l[t] <= stop:
                exit_px = stop if o[t] > stop else o[t]; bars = k; stopped = True; break
            if m == "fixed_close" and c[t] <= stop:
                exit_px = c[t]; bars = k; stopped = True; break
            if m == "chand_close" and c[t] <= stop:
                exit_px = c[t]; bars = k; stopped = True; break
            if m == "chand_gated" and c[t] <= stop and c[t] < ema[t]:
                exit_px = c[t]; bars = k; stopped = True; break
        if exit_px is None:
            t = min(ei + HORIZON, n - 1); exit_px = c[t]; bars = t - ei
        ret = (exit_px / entry - 1) * 100 - COST
        ft = min(ei + bars + 5, n - 1)
        recovered = bool(stopped and c[ft] > exit_px)
        res[m] = {"ret": ret, "bars": bars, "stopped": stopped, "recovered": recovered}
    return res


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    U = sorted(mom.FNO_UNDERLYINGS)
    print(f"downloading {len(U)} F&O symbols (2y daily) for stop backtest...")
    acc = {m: {"ret": [], "bars": [], "stopped": 0, "recovered": 0, "n": 0} for m in METHODS}
    CH = 60
    for i in range(0, len(U), CH):
        chunk = U[i:i + CH]
        for attempt in range(3):
            try:
                data = yf.download([s + ".NS" for s in chunk], period="2y", interval="1d",
                                   group_by="ticker", progress=False, threads=True)
                break
            except Exception as e:
                print(f"  retry {attempt+1} ({str(e)[:40]})"); time.sleep(20)
        else:
            continue
        for s in chunk:
            try:
                df = data[s + ".NS"][["Open", "High", "Low", "Close"]].dropna()
            except Exception:
                continue
            if len(df) < 80:
                continue
            o, h, l, c = (df[x].values for x in ["Open", "High", "Low", "Close"])
            ema = pd.Series(c).ewm(span=20).mean().values
            atr = _atr(h, l, c, 22)
            cross = (c > ema) & (np.roll(c, 1) <= np.roll(ema, 1))
            for ei in np.where(cross)[0]:
                if ei < 25 or ei >= len(c) - 2:
                    continue
                r = sim_entry(o, h, l, c, ema, atr, int(ei))
                for m in METHODS:
                    a = acc[m]; a["ret"].append(r[m]["ret"]); a["bars"].append(r[m]["bars"])
                    a["n"] += 1; a["stopped"] += int(r[m]["stopped"]); a["recovered"] += int(r[m]["recovered"])
        print(f"  {min(i + CH, len(U))}/{len(U)}  (trades {acc['fixed_close']['n']:,})")
        time.sleep(5)

    result = {"horizon": HORIZON, "cost": COST, "methods": {}}
    for m in METHODS:
        a = acc[m]
        if not a["n"]:
            continue
        ret = np.array(a["ret"])
        result["methods"][m] = {"trades": a["n"], "avg_ret": round(float(ret.mean()), 3),
                                "win": round(float((ret > 0).mean()) * 100, 1),
                                "avg_bars": round(float(np.mean(a["bars"])), 1),
                                "stopped_pct": round(a["stopped"] / a["n"] * 100, 1),
                                "shakeout_pct": round(a["recovered"] / a["n"] * 100, 1)}
    json.dump(result, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n================ STOP METHOD COMPARISON ({acc['fixed_close']['n']:,} trades, 20-EMA-cross entries) ================")
    print(f"  {'method':<16}{'avg net%':>10}{'win%':>8}{'avg bars':>10}{'stopped%':>10}{'shakeout%':>11}")
    for m in METHODS:
        r = result["methods"].get(m)
        if r:
            print(f"  {m:<16}{r['avg_ret']:>10}{r['win']:>8}{r['avg_bars']:>10}{r['stopped_pct']:>10}{r['shakeout_pct']:>11}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
