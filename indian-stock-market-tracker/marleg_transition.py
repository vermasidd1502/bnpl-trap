"""
marleg_transition.py — "is my long still a long?" transition watch.

For each held long (and any extra watchlist names) it builds ONE composite bias from the engines
we've validated — mood (same-day + regime), Ichimoku (above/below the cloud), U/D accumulation vs
distribution, and RSI direction — and maps it to LONG / NEUTRAL / WEAKENING / SHORT-LEAN.

A daily snapshot is stored so we can detect the TRANSITION: a name that was LONG and is now
WEAKENING/SHORT is the big signal — your thesis is rolling over, time to tighten/exit. One Groww
daily pull per name (so it works even when Yahoo is throttled).
"""
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import marleg_mood as mm
import marleg_ichimoku as ich

HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(HERE, "marleg_transition_history.json")


def _ist_date():
    return (datetime.now(timezone(timedelta(hours=5, minutes=30)))).strftime("%Y-%m-%d")


def _rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def _bars(tk):
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        df = g.candles(tk.upper(), interval_min=1440, days=220)
        if df is not None and not df.empty:
            return df[["open", "high", "low", "close", "volume"]].dropna()
    except Exception:
        pass
    return None


def signal(tk):
    """Composite long/short bias for one name from mood + Ichimoku + U/D + RSI."""
    df = _bars(tk)
    if df is None or len(df) < 60:
        return {"tk": tk.upper(), "error": "no data"}
    mood = mm._frame(df)["score"]
    mood_now = None if mood.empty or pd.isna(mood.iloc[-1]) else int(mood.iloc[-1])
    ist = ich.state(df)
    above_cloud = ist.get("above_cloud"); below_cloud = ist.get("below_cloud", False)
    bull_stack = ist.get("bull_stack")
    dN = df["close"].diff()
    uv = float(df["volume"].iloc[-40:][dN.iloc[-40:] > 0].sum())
    dv = float(df["volume"].iloc[-40:][dN.iloc[-40:] < 0].sum())
    ud = round(uv / dv, 2) if dv else None
    rsi = float(_rsi(df["close"]).iloc[-1])

    score = 0
    if mood_now is not None:
        score += 1 if mood_now >= 20 else -1 if mood_now <= -20 else 0
    if above_cloud:
        score += 1
    elif below_cloud:
        score -= 1
    if ud is not None:
        score += 1 if ud >= 1.1 else -1 if ud <= 0.9 else 0
    score += 1 if 45 <= rsi <= 72 else (-1 if rsi < 42 else 0)

    state = ("LONG" if score >= 2 else "SHORT-LEAN" if score <= -2 else
             "WEAKENING" if score < 0 else "NEUTRAL")
    return {"tk": tk.upper(), "score": score, "state": state, "mood": mood_now,
            "above_cloud": bool(above_cloud), "below_cloud": bool(below_cloud),
            "bull_stack": bull_stack, "ud": ud, "rsi": round(rsi, 0),
            "components": {"mood": mood_now, "ichimoku": ("above cloud" if above_cloud else "below cloud" if below_cloud else "in cloud"),
                           "ud": ("accum" if (ud or 1) >= 1.1 else "distrib" if (ud or 1) <= 0.9 else "balanced"),
                           "rsi": ("breaking down" if rsi < 42 else "overbought" if rsi > 72 else "ok")}}


def _load():
    try:
        with open(HIST, encoding="utf-8") as f:
            d = json.load(f); d.setdefault("days", {}); return d
    except Exception:
        return {"days": {}}


def _save(d):
    tmp = HIST + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False)
    os.replace(tmp, HIST)


def _transition(prev, cur):
    """Classify the move from a prior state to the current one — the 'big signal' is rolling over."""
    if prev is None:
        return {"flag": "new", "label": "baseline set", "big": False}
    p, c = prev.get("score"), cur["score"]
    if p is None:
        return {"flag": "new", "label": "baseline set", "big": False}
    if p >= 2 and c <= -2:
        return {"flag": "flip_short", "label": "🔴 FLIPPED LONG→SHORT — exit/avoid", "big": True}
    if p >= 2 and c <= 0:
        return {"flag": "rolling_over", "label": "⚠ ROLLING OVER (long→weak) — tighten stop", "big": True}
    if p <= -1 and c >= 2:
        return {"flag": "turning_up", "label": "🟢 turning back up (short→long)", "big": False}
    if c > p:
        return {"flag": "strengthening", "label": "↑ strengthening", "big": False}
    if c < p:
        return {"flag": "softening", "label": "↘ softening", "big": False}
    return {"flag": "stable", "label": "stable", "big": False}


def watch(extra=None):
    """Composite + transition for held longs (+ optional watchlist), and snapshot today's state."""
    syms = []
    try:
        import marleg_var
        book, _ = marleg_var._book()
        syms = [s for s, d in book.items() if d.get("val", 0) >= 500]
    except Exception:
        pass
    for s in (extra or []):
        if s.upper() not in syms:
            syms.append(s.upper())
    hist = _load()
    today = _ist_date()
    prior_dates = sorted([d for d in hist["days"] if d < today])
    prior = hist["days"].get(prior_dates[-1]) if prior_dates else {}
    rows = []
    snap = {}
    for s in syms:
        sig = signal(s)
        if "error" in sig:
            continue
        tr = _transition(prior.get(s), sig)
        sig["transition"] = tr
        rows.append(sig)
        snap[s] = {"score": sig["score"], "state": sig["state"]}
    hist["days"][today] = snap
    for old in sorted(hist["days"])[:-120]:
        hist["days"].pop(old, None)
    _save(hist)
    # big signals first, then weakest
    order = {"flip_short": 0, "rolling_over": 1, "softening": 2, "stable": 3, "strengthening": 4, "turning_up": 5, "new": 6}
    rows.sort(key=lambda r: (order.get(r["transition"]["flag"], 9), r["score"]))
    return {"asof": today + " IST", "n": len(rows), "rows": rows,
            "baseline_from": prior_dates[-1] if prior_dates else None}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(json.dumps(watch(sys.argv[1:]), indent=2, default=str))
