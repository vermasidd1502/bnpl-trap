"""
marleg_intraday_tod.py — does the Indian market MOVE from ~2:30pm (the last hour)?

Buckets 15-min bars by IST time-of-day across a liquid basket (+ NIFTYBEES for the index read) over the
~40 days Groww serves intraday, and measures:
  • VOLATILITY curve — mean |bar return| per time slot → is 14:30-15:30 bigger than midday? (the classic
    U-shape: open + close are the most active.)
  • LAST-HOUR DRIFT — per day, the 14:30→close return: is the last hour systematically up/down, or just noisy?

Small sample (Groww caps intraday history ~40d) → directional, not gospel. Honest: a louder last hour is
real microstructure (positioning into the close); a *directional* last-hour edge is the kind of thing our
other intraday tests have found to be thin/folklore — read the numbers, don't assume an edge.
"""
from collections import defaultdict

import numpy as np

import marleg_data as md

BASKET = ["NIFTYBEES", "BANKBEES", "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "SBIN", "LT", "ITC"]
SLOTS = ["09:15", "09:30", "09:45", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30",
         "13:00", "13:30", "14:00", "14:30", "14:45", "15:00", "15:15"]


def study():
    buck = defaultdict(list)
    lasthour, firsthour, midday = [], [], []
    names = 0
    for sym in BASKET:
        df = md.candles(sym, 15, 40)
        if df is None or len(df) < 60:
            continue
        df = df.dropna(subset=["open", "close"])
        names += 1
        t = df.index.strftime("%H:%M")
        day = df.index.strftime("%Y-%m-%d")
        br = (df["close"].astype(float) / df["open"].astype(float) - 1) * 100
        for ti, bi in zip(t, br):
            buck[ti].append(abs(float(bi)))
        import pandas as pd
        g = pd.DataFrame({"t": t, "day": day, "open": df["open"].astype(float).values,
                          "close": df["close"].astype(float).values})
        for d, gd in g.groupby("day"):
            lh = gd[gd["t"] >= "14:30"]
            if len(lh) >= 2:
                lasthour.append((lh["close"].iloc[-1] / lh["open"].iloc[0] - 1) * 100)
            fh = gd[gd["t"] <= "10:15"]
            if len(fh) >= 2:
                firsthour.append((fh["close"].iloc[-1] / fh["open"].iloc[0] - 1) * 100)
            mid = gd[(gd["t"] >= "11:30") & (gd["t"] <= "13:30")]
            if len(mid) >= 2:
                midday.append((mid["close"].iloc[-1] / mid["open"].iloc[0] - 1) * 100)
    return buck, lasthour, firsthour, midday, names


def _stat(a):
    a = np.array(a, dtype=float)
    return {"n": len(a), "mean": round(float(np.mean(a)), 3), "absmean": round(float(np.mean(np.abs(a))), 3),
            "up": round(float(np.mean(a > 0)) * 100, 0)}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    buck, lh, fh, mid, names = study()
    print(f"\n═══ INTRADAY TIME-OF-DAY · {names} names · ~40d ═══")
    print("  VOLATILITY by slot (mean |15m return| %) — bigger = more movement:")
    mx = max((np.mean(v) for v in buck.values() if v), default=1)
    for s in SLOTS:
        if s in buck and buck[s]:
            m = float(np.mean(buck[s])); bar = "█" * int(round(m / mx * 30))
            tag = "  ← last hour" if s >= "14:30" else ("  ← open" if s <= "09:30" else "")
            print(f"    {s}  {m:5.3f}  {bar}{tag}")
    print("\n  DIRECTIONAL drift by window (mean return %, % up):")
    for nm, a in (("first hour (9:15-10:15)", fh), ("midday (11:30-13:30)", mid), ("LAST HOUR (14:30-close)", lh)):
        s = _stat(a)
        print(f"    {nm:<26} mean {s['mean']:+.3f}%   up {s['up']:.0f}%   (n={s['n']})")
    print("\n  Read: a taller last-hour VOL bar = real (positioning into the close). A last-hour MEAN far from 0 with "
          "up% far from 50 would be a directional edge — small sample, treat skeptically (our other intraday edges were thin).")
