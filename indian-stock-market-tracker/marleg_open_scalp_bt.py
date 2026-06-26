"""
marleg_open_scalp_bt.py — tests: "buy at the OPEN on a bullish day, sell after the day HIGH" (intraday scalp).

"Sell at the high" needs hindsight, so we bracket it:
  • OPEN→HIGH  = the DREAM (perfect exit, look-ahead) — the absolute ceiling of the idea.
  • OPEN→CLOSE = just hold the whole day.
  • OPEN→TRAIL = the REALIZABLE version — exit on the first pullback of `trail%` from the running high
    (this IS "sell after it makes a high and turns").
Plus: WHEN does the day high form? (if it's late, "sell after the high" = holding nearly all day.)

Bullish regime = prior-day close > its 20DMA (knowable at the open — no look-ahead). Compared to ALL days.
Measured on the UNDERLYING (NIFTYBEES proxy + basket) — the clean directional signal. The OPTION overlay
(leverage helps, but intraday bid/ask spread + theta hurt) is discussed separately; this shows whether the
DIRECTION is even there to capture before costs eat it.
"""
from collections import defaultdict

import numpy as np
import pandas as pd

import marleg_data as md

NAMES = ["NIFTYBEES", "BANKBEES", "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "SBIN", "LT", "ITC"]
TRAIL = 0.4   # % pullback from the running high that triggers the realizable exit


def _trend(sym):
    d = md.candles(sym, 1440, 120)
    if d is None or len(d) < 30:
        return {}
    c = d["close"].astype(float); ma = c.rolling(20).mean()
    return {k.strftime("%Y-%m-%d"): bool(c.loc[k] > ma.loc[k]) for k in c.index if ma.loc[k] == ma.loc[k]}


def study(sym):
    trend = _trend(sym)
    df = md.candles(sym, 15, 40)
    if df is None or len(df) < 60:
        return None
    df = df.dropna(subset=["open", "high", "low", "close"])
    g = pd.DataFrame({"t": df.index.strftime("%H:%M"), "day": df.index.strftime("%Y-%m-%d"),
                      "open": df["open"].astype(float).values, "high": df["high"].astype(float).values,
                      "low": df["low"].astype(float).values, "close": df["close"].astype(float).values})
    out = {"bull": [], "all": []}
    for d, gd in g.groupby("day"):
        gd = gd.sort_values("t").reset_index(drop=True)
        if len(gd) < 10:
            continue
        o = float(gd["open"].iloc[0]); hi = float(gd["high"].max()); cl = float(gd["close"].iloc[-1])
        hi_t = gd.loc[gd["high"].idxmax(), "t"]
        # realizable trailing exit: ride from open, exit on first `TRAIL`% drop from the running high
        run_hi = o; exit_px = cl
        for _, b in gd.iterrows():
            run_hi = max(run_hi, float(b["high"]))
            stop = run_hi * (1 - TRAIL / 100)
            if float(b["low"]) <= stop:
                exit_px = stop; break
        rec = {"o2h": (hi / o - 1) * 100, "o2c": (cl / o - 1) * 100, "o2trail": (exit_px / o - 1) * 100,
               "hi_late": hi_t >= "13:00"}
        out["all"].append(rec)
        if trend.get(d, False):
            out["bull"].append(rec)
    return out


def _agg(rows):
    if not rows:
        return None
    return {"n": len(rows),
            "open_to_high": round(float(np.mean([r["o2h"] for r in rows])), 2),
            "open_to_close": round(float(np.mean([r["o2c"] for r in rows])), 2),
            "open_to_trail": round(float(np.mean([r["o2trail"] for r in rows])), 2),
            "trail_win_pct": round(float(np.mean([r["o2trail"] > 0 for r in rows])) * 100, 0),
            "high_after_1pm_pct": round(float(np.mean([r["hi_late"] for r in rows])) * 100, 0)}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    BULL, ALL = [], []
    nif = None
    for nm in NAMES:
        s = study(nm)
        if not s:
            continue
        BULL += s["bull"]; ALL += s["all"]
        if nm == "NIFTYBEES":
            nif = s
    print(f"\n═══ BUY-OPEN → SELL-NEAR-HIGH scalp · trail {TRAIL}% · ~40d ═══")
    print("  (underlying %; an ATM option ≈ this × ~10-20 leverage, MINUS bid/ask spread + theta)")

    def show(t, rows):
        a = _agg(rows)
        if not a:
            print(f"  {t}: none"); return
        print(f"\n  {t}  (n={a['n']})")
        print(f"     OPEN→HIGH  (dream/look-ahead) ... {a['open_to_high']:+.2f}%")
        print(f"     OPEN→CLOSE (hold the day) ....... {a['open_to_close']:+.2f}%")
        print(f"     OPEN→TRAIL (realizable scalp) ... {a['open_to_trail']:+.2f}%   win {a['trail_win_pct']:.0f}%")
        print(f"     day high formed after 1pm ....... {a['high_after_1pm_pct']:.0f}%")
    if nif:
        show("NIFTY proxy · BULLISH days", nif["bull"])
        show("NIFTY proxy · ALL days", nif["all"])
    show("BASKET · BULLISH days", BULL)
    show("BASKET · ALL days", ALL)
    print("\n  Read: OPEN→HIGH is the ceiling you can NEVER fully get. OPEN→TRAIL is honest. If TRAIL is small/negative "
          "on the UNDERLYING, the option version is worse (spread+theta) — the idea fails. If TRAIL is solidly positive "
          "AND > all-days baseline, the bullish-open momentum is real — then it's a question of beating option costs.")
