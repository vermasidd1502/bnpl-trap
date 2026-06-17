"""
marleg_conscience.py — NIFTY bull/bear CONSCIENCE + next-day lean from global cues.

Two questions in one gauge:
  1. WHERE ARE WE? — NIFTY trend (vs 20/50/200 DMA) + market BREADTH (% of the universe above its 50DMA) +
     VIX level & direction → a BULL / NEUTRAL / BEAR bias with a 0-100 score.
  2. WHAT'S TOMORROW'S LEAN? — overnight global cues, WEIGHTED BY WHAT ACTUALLY PREDICTS (3y backtest):
     the US close leads next-day NIFTY (S&P 57% sign-hit; after S&P <-1%, NIFTY is down ~72%); Nikkei/Japan
     does NOT (corr 0.02 — a coin flip), so it's shown for context but NOT trusted as a signal.

Read-only, decision-support — a conscience, not a crystal ball (the cue edge is modest, ~57%).

  python marleg_conscience.py
"""
import os
import sys
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

IST = timezone(timedelta(hours=5, minutes=30))
# backtested predictive weight of each overnight cue on next-day NIFTY (3y sign-hit / corr)
CUE_EDGE = {"S&P500": 0.57, "Nasdaq": 0.56, "Nikkei": 0.50, "HangSeng": 0.50}


def _nifty_vix():
    import yfinance as yf
    out = {}
    try:
        h = yf.Ticker("^NSEI").history(period="1y")["Close"].dropna()
        c = float(h.iloc[-1])
        out["nifty"] = {"price": round(c, 1),
                        "vs20": round((c / h.rolling(20).mean().iloc[-1] - 1) * 100, 2),
                        "vs50": round((c / h.rolling(50).mean().iloc[-1] - 1) * 100, 2),
                        "vs200": round((c / h.rolling(200).mean().iloc[-1] - 1) * 100, 2),
                        "ret5": round((c / h.iloc[-6] - 1) * 100, 2), "ret20": round((c / h.iloc[-21] - 1) * 100, 2)}
    except Exception:
        out["nifty"] = None
    try:
        v = yf.Ticker("^INDIAVIX").history(period="1mo")["Close"].dropna()
        lvl = float(v.iloc[-1]); chg5 = (lvl / float(v.iloc[-6]) - 1) * 100 if len(v) > 6 else 0.0
        reg = "calm" if lvl < 13 else "normal" if lvl < 17 else "elevated" if lvl < 22 else "stress"
        out["vix"] = {"level": round(lvl, 2), "chg5d": round(chg5, 1), "regime": reg, "falling": chg5 < 0}
    except Exception:
        out["vix"] = None
    return out


def _breadth():
    try:
        import marleg_panel_build as pb
        P = pb.load(); C = P["close"]
        last = C.iloc[-1]; s50 = C.rolling(50).mean().iloc[-1]
        ok = (last > s50)
        n = int(ok.notna().sum() if hasattr(ok, "notna") else 0)
        valid = (~last.isna()) & (~s50.isna())
        pct = float((last[valid] > s50[valid]).mean()) * 100
        return {"pct_above_50dma": round(pct, 1), "n": int(valid.sum())}
    except Exception:
        return None


def build():
    nv = _nifty_vix(); br = _breadth()
    nf = nv.get("nifty"); vx = nv.get("vix")
    try:
        import marleg_lookahead
        la = marleg_lookahead.build()
    except Exception:
        la = {}

    # ---- bias (where are we) ----
    score = 50; reasons = []
    if nf:
        for k, w, lab in [("vs20", 8, "20DMA"), ("vs50", 10, "50DMA"), ("vs200", 12, "200DMA")]:
            if nf[k] is not None:
                if nf[k] > 0:
                    score += w; reasons.append(f"above {lab} ({nf[k]:+.1f}%)")
                else:
                    score -= w; reasons.append(f"below {lab} ({nf[k]:+.1f}%)")
    if br and br["pct_above_50dma"] is not None:
        p = br["pct_above_50dma"]
        if p >= 55:
            score += 10; reasons.append(f"broad breadth ({p:.0f}% > 50DMA)")
        elif p <= 45:
            score -= 10; reasons.append(f"weak breadth ({p:.0f}% > 50DMA)")
        else:
            reasons.append(f"mixed breadth ({p:.0f}% > 50DMA)")
    if vx:
        if vx["regime"] in ("elevated", "stress"):
            score -= 8; reasons.append(f"VIX {vx['regime']} ({vx['level']})")
        elif vx["regime"] == "calm" and vx["falling"]:
            score += 6; reasons.append(f"VIX calm & falling ({vx['level']}, {vx['chg5d']:+.0f}%/5d)")
    score = int(max(0, min(100, score)))
    bias = "BULL" if score >= 62 else "BEAR" if score <= 38 else "NEUTRAL"

    # ---- next-day lean (what's tomorrow) ----
    composite = la.get("composite"); verdict = la.get("verdict")
    cues = la.get("cues", {})
    sp = (cues.get("sp500") or {}).get("chg"); nq = (cues.get("nasdaq") or {}).get("chg")
    us = np.mean([x for x in (sp, nq) if x is not None]) if (sp is not None or nq is not None) else None
    if us is None:
        lean = "UNKNOWN (no US close yet)"
    elif us > 1:
        lean = "LEAN UP (strong US — 56% hit next day)"
    elif us < -1:
        lean = "LEAN DOWN (US <-1% → NIFTY down ~72% next day)"
    elif us > 0.2:
        lean = "mild up"
    elif us < -0.2:
        lean = "mild down"
    else:
        lean = "flat"

    return {"ok": True, "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
            "nifty": nf, "vix": vx, "breadth": br,
            "bias": {"label": bias, "score": score, "reasons": reasons},
            "next_day": {"lean": lean, "us_overnight_pct": round(float(us), 2) if us is not None else None,
                         "composite": composite, "verdict": verdict, "drivers": la.get("drivers", []),
                         "note": "US close is the real overnight tell (S&P 57% sign-hit, asymmetric on big down "
                                 "days). Japan/Nikkei does NOT predict (corr 0.02) — shown for context only."},
            "evidence": {"sp_hit": 57, "nasdaq_hit": 56, "nikkei_hit": 50,
                         "after_sp_down1pct": "NIFTY -0.48% next day, 28% up (n69, 3y)"},
            "note": "Conscience, not a crystal ball — bias = where we are (trend+breadth+VIX); lean = tomorrow's "
                    "open tilt from the US close (modest ~57% edge). Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = build()
    b = r["bias"]; nd = r["next_day"]
    print(f"\nNIFTY CONSCIENCE — {r['asof']}")
    print(f"  BIAS: {b['label']}  ({b['score']}/100)")
    print(f"    {' · '.join(b['reasons'])}")
    if r["nifty"]:
        print(f"  NIFTY ₹{r['nifty']['price']} · vs50 {r['nifty']['vs50']:+}% · vs200 {r['nifty']['vs200']:+}% · ret20 {r['nifty']['ret20']:+}%")
    if r["vix"]:
        print(f"  VIX {r['vix']['level']} ({r['vix']['regime']}, {r['vix']['chg5d']:+}%/5d)")
    if r["breadth"]:
        print(f"  BREADTH {r['breadth']['pct_above_50dma']}% of {r['breadth']['n']} names above 50DMA")
    print(f"\n  NEXT-DAY LEAN: {nd['lean']}  (US overnight {nd['us_overnight_pct']}%)")
    print(f"    {nd['note']}")
