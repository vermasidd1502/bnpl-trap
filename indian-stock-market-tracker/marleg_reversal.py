"""
marleg_reversal.py — REVERSAL-TO-LONG radar: stocks turning back UP after a pullback (the re-entry trigger).

Grounded in our backtests: BULLISH reversal candles work in India (Hammer ~55% / +0.71%/10d, Morning Star
+0.64%, Piercing +0.6%, Bullish Engulfing) — every BEARISH pattern was folklore, so this is long-only. The
HIGHEST-conviction reversal = one that fires DURING an uptrend (above 50DMA) in a leading/gated name = the
classic "booked out, pulled back, bulls retake control, re-enter" setup (your TEJAS situation).

Detects at the latest bar + tags context (uptrend? gated leader? news-clean?) + the backtested win/net + the
entry/stop. Read-only. Pairs with the War Room alerts feed (+ optional Slack push).
"""
import json
import os
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_reversal.json")
# backtested win% / net%/10d per signal (marleg_pattern_study + gate_pullback)
BT = {"Hammer": (55, 0.71), "Morning Star": (54, 0.64), "Piercing": (53, 0.6),
      "Bullish Engulfing": (53, 0.5), "Inverted Hammer": (52, 0.4), "Pullback turn-up": (58, 0.9)}


def detect(o, h, l, c):
    c = c.dropna()
    if len(c) < 60:
        return None
    idx = c.index
    o = o.reindex(idx); h = h.reindex(idx); l = l.reindex(idx)
    O, H, L, C = o.values, h.values, l.values, c.values
    t = len(C) - 1
    o1, h1, l1, c1 = O[t], H[t], L[t], C[t]
    o2, c2 = O[t - 1], C[t - 1]
    o3, c3 = O[t - 2], C[t - 2]
    body = abs(c1 - o1)
    lower = min(o1, c1) - l1
    upper = h1 - max(o1, c1)
    down_prior = C[t - 1] < C[t - 4]                       # pulled back into the signal
    green = c1 > o1
    sigs = []
    if down_prior and body > 0:
        if lower >= 2 * body and upper <= body:
            sigs.append("Hammer")
        if upper >= 2 * body and lower <= body:
            sigs.append("Inverted Hammer")
    if down_prior and green and c2 < o2 and c1 >= o2 and o1 <= c2:
        sigs.append("Bullish Engulfing")
    if down_prior and green and c2 < o2 and o1 < c2 and (o2 + c2) / 2 < c1 < o2:
        sigs.append("Piercing")
    if down_prior and green and c3 < o3 and abs(c2 - o2) < abs(c3 - o3) * 0.5 and c1 > (o3 + c3) / 2:
        sigs.append("Morning Star")
    # pullback-turn-up: was down over the last 3-5 sessions, today turns up & reclaims the prior day's high
    if green and c1 > H[t - 1] and (C[t - 1] / C[t - 5] - 1) < 0:
        sigs.append("Pullback turn-up")
    if not sigs:
        return None
    dma50 = float(pd.Series(C).rolling(50).mean().iloc[-1])
    above50 = bool(c1 > dma50)
    stop = round(float(l1), 1)
    return {"signals": sigs, "above50": above50, "price": round(float(c1), 2),
            "stop": stop, "stop_pct": round((c1 - stop) / c1 * 100, 1),
            "best_win": max(BT[s][0] for s in sigs), "best_net": max(BT[s][1] for s in sigs)}


def build():
    import marleg_panel_build as pb
    panel = pb.load()
    if not panel:
        return {"ok": False, "error": "panel not built", "signals": []}
    close = panel["close"]
    o, h, l = panel["open"], panel["high"], panel["low"]   # keep tz consistent with close; detect() aligns internally
    NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    try:
        g = json.load(open(os.path.join(HERE, "marleg_gated_cache.json"), encoding="utf-8"))
        gated = {p["s"]: p for p in g.get("picks", [])}
        leadnames = {str(x.get("group") or "").lower() for x in g.get("leading_industries", [])}
    except Exception:
        gated, leadnames = {}, set()
    rows = []
    for s in close.columns:
        try:
            d = detect(o[s], h[s], l[s], close[s])
        except Exception:
            d = None
        if not d:
            continue
        gp = gated.get(s)
        sec = SECT.get(s, {})
        ind = sec.get("industry")
        leader = bool(ind and str(ind).lower() in leadnames) or bool(gp)
        clean = (gp.get("clean") if gp else None)
        # conviction: reversal in an UPTREND in a LEADER (clean) = the prime re-entry
        conv = (40 if d["above50"] else 0) + (35 if leader else 0) + (15 if clean else 0) + min(d["best_win"] - 50, 10)
        tag = "PRIME re-entry" if (d["above50"] and leader) else "in uptrend" if d["above50"] else "downtrend bounce (risky)"
        rows.append({"s": s, "n": NAMES.get(s, s), "sector": sec.get("sector"), "industry": ind,
                     "signals": d["signals"], "price": d["price"], "stop": d["stop"], "stop_pct": d["stop_pct"],
                     "above50": d["above50"], "leader": leader, "clean": clean, "gated": bool(gp),
                     "win": d["best_win"], "net": d["best_net"], "conv": round(conv), "tag": tag})
    rows.sort(key=lambda x: (-x["conv"], -x["win"]))
    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    prime = [r for r in rows if r["above50"] and r["leader"]]
    return {"ok": True, "asof": ist, "n": len(rows), "n_prime": len(prime), "signals": rows[:50],
            "note": "Bullish reversal-to-long only (bearish patterns are folklore in India). PRIME = reversal in "
                    "an uptrend in a leader = re-enter; downtrend bounces are risky (falling-knife). Confirm + tight stop below the signal bar's low."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = build()
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    json.dump(r, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)   # persist for the EOD scan + autopilot consolidator
    print(f"REVERSAL-TO-LONG {r['asof']} — {r['n']} signals ({r['n_prime']} PRIME re-entries)")
    for x in r["signals"][:14]:
        print(f"  {x['s']:<11} conv {x['conv']:>3} {x['tag']:<22} {'/'.join(x['signals'])[:28]:<28} ₹{x['price']} stop ₹{x['stop']} (~{x['win']}% win)")
