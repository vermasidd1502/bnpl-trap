"""
marleg_overbought.py — today's most OVERBOUGHT / extended names, with the HONEST verdict attached.

The user's idea: short the most-overbought of the day. The factor backtest (marleg_factors) says that is NOT
a validated edge in India: RSI / distance-from-50DMA / 5d-reversal all carry only a weak, statistically
INSIGNIFICANT negative IC (|t|<1.6), while "near the 52-week high" carries a POSITIVE IC — extended winners
keep winning. So shorting the most-overbought is, at best, a coin-flip that loses to costs + India's
overnight up-drift.

So this screen is framed for what the data DOES support: a TRIM / take-profit / don't-chase list for longs.
If you still want to short one, the rules are (a) intraday only — flat by close (overnight drift is +0.37%),
(b) only a name that's ALSO breaking down (below its pivot), not one riding its highs. Read-only.

  python marleg_overbought.py
"""
import sys
import numpy as np
import pandas as pd


def _rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def screen(top=25):
    import marleg_panel_build as pb
    import marleg_options_monitor as mom
    import json
    import os
    HERE = os.path.dirname(os.path.abspath(__file__))
    try:
        NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
    except Exception:
        NAMES = {}
    P = pb.load()
    C = P["close"]
    rsi = _rsi(C).iloc[-1]
    sma50 = C.rolling(50).mean().iloc[-1]
    hi52 = C.rolling(252, min_periods=60).max().iloc[-1]
    ret5 = (C.iloc[-1] / C.iloc[-6] - 1) * 100
    px = C.iloc[-1]
    dist50 = (px / sma50 - 1) * 100
    dist52 = (px / hi52 - 1) * 100
    fno = set(mom.FNO_UNDERLYINGS) | set(mom.INDEX_STEP)
    df = pd.DataFrame({"rsi": rsi, "dist50": dist50, "ret5": ret5, "dist52": dist52, "px": px}).dropna()
    # overbought composite = mean percentile-rank of RSI, distance-above-50DMA, recent 5d run
    for col in ("rsi", "dist50", "ret5"):
        df[col + "_p"] = df[col].rank(pct=True)
    df["ob"] = (df["rsi_p"] + df["dist50_p"] + df["ret5_p"]) / 3 * 100
    df = df.sort_values("ob", ascending=False).head(top)
    rows = []
    for s, r in df.iterrows():
        near_high = r["dist52"] >= -3                    # within 3% of the 52w high = riding strength (don't short)
        rows.append({"s": s, "n": NAMES.get(s, s), "fno": bool(s in fno), "price": round(float(r["px"]), 2),
                     "rsi": round(float(r["rsi"]), 1), "dist_50dma_pct": round(float(r["dist50"]), 1),
                     "ret5_pct": round(float(r["ret5"]), 1), "dist_52w_high_pct": round(float(r["dist52"]), 1),
                     "ob_score": round(float(r["ob"]), 0), "near_high": bool(near_high),
                     "read": ("RIDING HIGHS — momentum factor is POSITIVE here; do NOT short, consider trimming a long"
                              if near_high else "extended but off its high — IF shorting, intraday-only + needs a pivot break")})
    return {"ok": True, "n": len(rows), "names": rows,
            "verdict": "Shorting the most-overbought is NOT a backtested edge in India (RSI/over-extension IC is "
                       "weak & insignificant; 'near 52w-high' IC is POSITIVE — winners keep winning). Use this as a "
                       "TRIM / don't-chase list for longs. To short at all: intraday-only (overnight drift +0.37%), "
                       "and only a name ALSO breaking below its pivot — never one riding its highs.",
            "caveat": "Decision-support, not advice — I'm not a licensed advisor."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = screen()
    print(f"\n  MOST OVERBOUGHT / EXTENDED TODAY ({r['n']})  —  trim/don't-chase list (NOT a validated short)")
    print(f"  {'name':<12}{'OB':>5}{'RSI':>6}{'>50DMA':>8}{'ret5':>7}{'vs52wH':>8}  read")
    for x in r["names"][:18]:
        tag = "F&O" if x["fno"] else "cash"
        print(f"  {x['s']:<12}{x['ob_score']:>5.0f}{x['rsi']:>6.0f}{x['dist_50dma_pct']:>7.1f}%{x['ret5_pct']:>6.1f}%{x['dist_52w_high_pct']:>7.1f}%  [{tag}] {'★HIGHS' if x['near_high'] else ''}")
    print(f"\n  ➤ {r['verdict']}")
