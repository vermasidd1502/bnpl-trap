"""
marleg_closing.py — NSE session-phase clock + closing-behaviour read for the intraday pod.

Backtested (marleg_closing_pressure_study, 1200 stock-days / 30 liquid names / 40 sessions, Groww
5-min) — and the result REVERSED the folk intuition:
  • The 15:00->close window drifts mildly UP (+0.036% / 53%), even on crowded-long days. The MIS
    square-off does NOT systematically dump the crowded side. So there is NO "exit before 15:15" edge
    — holding into the close is mildly +EV (+0.05% / 56% over the last 20 min). We do NOT ship that rule.
  • The real, clean edge: a stock that SELLS OFF into the close (15:00->close < -0.2%) BOUNCES at the
    next open (+0.133% / 61%). Late-day weakness is mechanical square-off and it reverts. So a no-news
    close dip is a better next-morning fill, not a panic exit.
  • A strong close does NOT carry to the next open (-0.01% / 47%).

This module turns that into (a) a session-phase clock for context and (b) an honest closing-behaviour
verdict per stock — never a "sell now because everyone sells at close" rule (that's folklore here).
"""
import os
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))

# evidence surfaced in the UI so the read is auditable, not asserted
EVIDENCE = {
    "sample": "1200 stock-days · 30 liquid names · 40 sessions · Groww 5-min",
    "close_window_15to_close": {"avg_pct": 0.036, "pos_pct": 52.7},
    "last20_1510_to_close": {"avg_pct": 0.05, "pos_pct": 56.4},
    "soldoff_close_to_nextopen": {"avg_pct": 0.133, "pos_pct": 61.2},
    "strong_close_to_nextopen": {"avg_pct": -0.012, "pos_pct": 46.8},
}


def ist_now():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))


def _mins(t):
    return t.hour * 60 + t.minute


def phase(now=None):
    """Which NSE session phase are we in, with minutes to the square-off (15:15) and close (15:30)."""
    now = now or ist_now()
    m = _mins(now)
    wd = now.weekday()
    open_m, sq_m, close_m = 9 * 60 + 15, 15 * 60 + 15, 15 * 60 + 30
    weekend = wd >= 5
    if weekend or m < 9 * 60 or m >= 16 * 60:
        name, label, guide = "closed", "market closed", "NSE is closed. Daily-gate reads apply; intraday timing resumes at 09:15 IST."
    elif m < open_m:
        name, label, guide = "preopen", "pre-open auction (09:00–09:15)", "Pre-open call auction — gap discovery. No clean intraday signal yet."
    elif m < 9 * 60 + 30:
        name, label, guide = "open", "opening 15 min (09:15–09:30)", "High-vol gap digestion. Wide stops; avoid acting on the very first bar."
    elif m < 11 * 60 + 30:
        name, label, guide = "morning", "morning trend (09:30–11:30)", "Cleanest trend of the day — highest-conviction window for entries."
    elif m < 13 * 60 + 30:
        name, label, guide = "midday", "midday lull (11:30–13:30)", "Thin liquidity, chop — breakouts here fail more often. Be patient."
    elif m < 15 * 60:
        name, label, guide = "afternoon", "afternoon (13:30–15:00)", "Trend can resume. Last clean window for fresh intraday entries."
    elif m < sq_m:
        name, label, guide = "preclose", "pre square-off (15:00–15:15)", "MIS unwind building. Backtested: the close window drifts mildly UP (+0.04%/53%) — holding is fine; don't open fresh conviction."
    else:
        name, label, guide = "squareoff", "square-off + close (15:15–15:30)", "MIS auto-square-off. A no-news dip here is mechanical and tends to bounce next open (+0.13%/61%) — better fill than a panic exit."
    return {"phase": name, "label": label, "guide": guide,
            "mins_to_squareoff": max(0, sq_m - m) if (not weekend and m < sq_m) else 0,
            "mins_to_close": max(0, close_m - m) if (not weekend and m < close_m) else 0,
            "asof": now.strftime("%Y-%m-%d %H:%M IST")}


def _today_bars(tk):
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        df = g.candles(tk.upper(), interval_min=5, days=3)
        if df is None or df.empty:
            return None
        last_day = df.index.normalize().max()
        return df[df.index.normalize() == last_day].sort_index()
    except Exception:
        return None


def read(tk):
    """Per-stock closing-behaviour read: phase + day positioning + the VALIDATED close signals."""
    ph = phase()
    d = _today_bars(tk)
    out = {"tk": tk.upper(), **ph, "evidence": EVIDENCE}
    if d is None or len(d) < 3:
        out["note"] = "no intraday data (market may be pre-open / closed)."
        return out
    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    vwap = (tp * d["volume"]).cumsum() / d["volume"].cumsum()
    o = float(d["open"].iloc[0]); last = float(d["close"].iloc[-1])
    hi = float(d["high"].max()); lo = float(d["low"].min())
    vw = float(vwap.iloc[-1])
    dret = last / o - 1
    abv = last > vw
    clv = (last - lo) / (hi - lo) if hi > lo else 0.5
    crowd = "long" if (dret > 0.005 and abv) else "short" if (dret < -0.005 and not abv) else "balanced"

    # close-window move (only meaningful once past 15:00)
    tmap = {t.strftime("%H:%M"): i for i, t in enumerate(d.index)}
    p1500 = float(d["close"].iloc[tmap["15:00"]]) if "15:00" in tmap else None
    cw_ret = (last / p1500 - 1) if p1500 else None

    msgs = []
    msgs.append(f"Day so far: {dret*100:+.2f}% · {'above' if abv else 'below'} VWAP · CLV {clv:.2f} → crowded **{crowd}**.")
    if ph["phase"] in ("preclose", "squareoff") and cw_ret is not None:
        if cw_ret < -0.002:
            msgs.append(f"🟢 Selling off into the close ({cw_ret*100:+.2f}% since 15:00). Backtested as **mechanical square-off** — "
                        f"reverts **+0.13% / 61%** at the next open. This is a *better next-morning fill*, NOT a panic exit.")
            verdict = "CLOSE-DIP → likely next-open bounce (don't panic-exit)"
        elif cw_ret > 0.002:
            msgs.append(f"🟡 Closing strong ({cw_ret*100:+.2f}% since 15:00). Late strength historically does **not** carry "
                        f"(next open ~flat, 47%) — don't chase the close.")
            verdict = "STRONG CLOSE → doesn't carry overnight (don't chase)"
        else:
            msgs.append("Close window flat. It drifts mildly up on average (+0.04%/53%) — holding into the close is fine.")
            verdict = "NEUTRAL close — holding is fine"
    elif ph["phase"] in ("morning", "afternoon", "open"):
        verdict = f"{ph['label']} — fresh intraday entries OK" if ph["phase"] != "open" else "opening — let it settle"
    elif ph["phase"] == "midday":
        verdict = "midday chop — breakouts fail more; be patient"
    else:
        verdict = ph["label"]

    out.update({"day_ret": round(dret * 100, 2), "above_vwap": bool(abv), "clv": round(clv, 2),
                "crowd": crowd, "vwap": round(vw, 2), "last": round(last, 2),
                "cw_ret": round(cw_ret * 100, 2) if cw_ret is not None else None,
                "verdict": verdict, "messages": msgs})
    return out


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(json.dumps(read(sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"), indent=2, default=str))
