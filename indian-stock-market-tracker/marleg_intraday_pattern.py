"""
marleg_intraday_pattern.py — tests the user's exact intraday thesis:

  "On a BULLISH day (uptrend), price RISES until ~2pm, then bears take over and push it to a daily LOW,
   then it REBOUNDS into the close."

For each day we measure: morning move (open→2pm), whether the day's LOW forms in the afternoon (post-2pm),
the recovery off that afternoon low into the close, and whether the close finishes back above the 2pm price.
We then compare BULLISH + morning-rise days (the setup) to ALL days, to see if the dip-and-recover is real
and *conditional* — not just what every day does.

Index proxy = NIFTYBEES (NIFTY has no intraday volume; the ETF tracks it). + a liquid basket for robustness.
Honest: Groww serves ~40d of intraday, so the filtered sample is SMALL (~15-25 days). Directional, not gospel.
"""
from collections import defaultdict

import numpy as np
import pandas as pd

import marleg_data as md

NAMES = ["NIFTYBEES", "BANKBEES", "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "SBIN", "LT", "ITC"]


def _trend(sym):
    d = md.candles(sym, 1440, 120)
    if d is None or len(d) < 30:
        return {}
    c = d["close"].astype(float)
    ma = c.rolling(20).mean()
    up = (c > ma)
    return {k.strftime("%Y-%m-%d"): bool(v) for k, v in up.items()}


def study(sym):
    trend = _trend(sym)
    df = md.candles(sym, 15, 40)
    if df is None or len(df) < 60:
        return None
    df = df.dropna(subset=["open", "high", "low", "close"])
    g = pd.DataFrame({"t": df.index.strftime("%H:%M"), "day": df.index.strftime("%Y-%m-%d"),
                      "open": df["open"].astype(float).values, "high": df["high"].astype(float).values,
                      "low": df["low"].astype(float).values, "close": df["close"].astype(float).values})
    rows_set, rows_all = [], []
    for d, gd in g.groupby("day"):
        gd = gd.sort_values("t")
        if len(gd) < 10:
            continue
        o = float(gd["open"].iloc[0]); c = float(gd["close"].iloc[-1])
        ref2 = gd[gd["t"] <= "14:00"]
        if ref2.empty:
            continue
        p2 = float(ref2["close"].iloc[-1])                       # price ~2pm
        pm = gd[gd["t"] >= "14:00"]
        if len(pm) < 2:
            continue
        pm_low = float(pm["low"].min())
        low_t = gd.loc[gd["low"].idxmin(), "t"]                  # time-of-day of the WHOLE-day low
        rec = {"morning_up": p2 > o, "low_in_pm": low_t >= "14:00", "recov_off_low": (c / pm_low - 1) * 100,
               "close_above_2pm": c > p2, "day_up": c > o}
        rows_all.append(rec)
        if trend.get(d, False) and p2 > o:                      # SETUP: uptrend + rose into 2pm
            rows_set.append(rec)
    return {"setup": rows_set, "all": rows_all}


def _agg(rows):
    if not rows:
        return None
    return {"n": len(rows),
            "low_in_pm_pct": round(np.mean([r["low_in_pm"] for r in rows]) * 100, 0),
            "recovered_past_2pm_pct": round(np.mean([r["close_above_2pm"] for r in rows]) * 100, 0),
            "mean_recov_off_low_pct": round(float(np.mean([r["recov_off_low"] for r in rows])), 2),
            "closed_up_pct": round(np.mean([r["day_up"] for r in rows]) * 100, 0)}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    SET, ALL = [], []
    nif = None
    for nm in NAMES:
        s = study(nm)
        if not s:
            continue
        SET += s["setup"]; ALL += s["all"]
        if nm == "NIFTYBEES":
            nif = s
    print("\n═══ INTRADAY THESIS: bullish + rise-to-2pm → afternoon low → rebound into close ═══")

    def show(title, rows):
        a = _agg(rows)
        if not a:
            print(f"  {title}: no days"); return
        print(f"  {title}  (n={a['n']})")
        print(f"     daily LOW formed after 2pm ......... {a['low_in_pm_pct']:.0f}%")
        print(f"     closed back ABOVE the 2pm price .... {a['recovered_past_2pm_pct']:.0f}%   ← the 'rebound' claim")
        print(f"     mean recovery off the pm low ....... {a['mean_recov_off_low_pct']:+.2f}%")
        print(f"     finished the day UP ................ {a['closed_up_pct']:.0f}%")
    if nif:
        show("NIFTY proxy · SETUP days (uptrend + 2pm rise)", nif["setup"])
        show("NIFTY proxy · ALL days (baseline)", nif["all"])
    print()
    show("BASKET · SETUP days (uptrend + 2pm rise)", SET)
    show("BASKET · ALL days (baseline)", ALL)
    print("\n  Read: if 'closed back above 2pm' on SETUP days is well above 50% AND above the ALL-days baseline, the "
          "dip-and-recover is real and conditional. If it's ~50% or ≈ baseline, the pattern is hindsight — the dips "
          "that recovered are the ones you remember; the ones that kept falling you don't.")
