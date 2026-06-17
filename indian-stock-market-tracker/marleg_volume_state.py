"""
marleg_volume_state.py — live STATE + two-gate WATCHLIST for the volume pod.

Turns the gated list into a disciplined, self-updating board:

STATE (drives the animated shader behind each name):
  • GOLD  — live price has reached the target (book / trail).
  • GREEN — gates holding & it's CLIMBING toward target (clean, disciplined U/D, above the EOD close).
  • RED   — movement is FALSE: falling back, U/D gone weak, news-dirty, OR a blow-off U/D (>=6).

TWO GATES (don't auto-promote raw signals):
  • Gate 2 — WATCHLIST: a name that just qualified (tenure day-1), or whose U/D is a blow-off, or that's
    going red. It is NOT pushed to the main list yet — it has to prove itself.
  • Gate 1 — MAIN: promoted only once it CONFIRMS — held the gate >=2 sessions, news-clean, U/D in the
    disciplined band, and not red.

DISCIPLINED U/D (your rule — a high U/D can spiral us): weak <1.3 · disciplined 1.3-4 · heating 4-6 ·
blow-off >=6. The main list prefers DISCIPLINED; blow-off names are held in the watchlist, never chased.

Live prices come from Groww (batch quote_table); falls back to the EOD close (no gold) if Groww is down.

  python marleg_volume_state.py
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def _streaks():
    """Consecutive sessions each name has been on the gated list (from gated_history) — the tenure that
    promotes a watchlist name to the main list."""
    days = (_load("marleg_gated_history.json") or {}).get("days", {})
    if not days:
        return {}
    dts = sorted(days.keys())
    streak = {}
    for s in days.get(dts[-1], []):
        n = 0
        for d in reversed(dts):
            if s in days.get(d, []):
                n += 1
            else:
                break
        streak[s] = n
    return streak


def _live(syms):
    """Batch real-time prices via Groww, CHUNKED (the ltp query caps out on long symbol lists → a single
    52-name call silently returns nothing)."""
    out = {}
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        for i in range(0, len(syms), 10):
            try:
                q = g.quote_table(syms[i:i + 10])
                for k, v in q.items():
                    if isinstance(v, dict) and v.get("price"):
                        out[k.upper()] = v["price"]
            except Exception:
                continue
    except Exception:
        pass
    return out, ("groww-live" if out else "eod-close")


def ud_band(ud):
    if ud is None:
        return "weak"
    return "blowoff" if ud >= 6 else "heating" if ud >= 4 else "disciplined" if ud >= 1.3 else "weak"


def build():
    g = _load("marleg_gated_cache.json")
    picks = [p for p in g.get("picks", []) if p.get("price") and p.get("target")]
    streaks = _streaks()
    live, src = _live([p["s"] for p in picks]) if picks else ({}, "eod-close")
    rows = []
    for p in picks:
        s = p["s"]; ref = float(p["price"]); tgt = float(p["target"])
        ud = p.get("ud"); band = ud_band(ud)
        rising = bool(p.get("ud_ma") is not None and ud is not None and ud > p["ud_ma"])
        clean = p.get("clean") is not False
        now = float(live.get(s) or ref)
        chg = round((now / ref - 1) * 100, 2)
        to_tgt = round((tgt / now - 1) * 100, 2)
        streak = streaks.get(s, 1)
        # STATE
        if now >= tgt:
            state = "gold"
        elif (not clean) or (ud is None or ud < 1.3) or band == "blowoff":
            state = "red"
        elif now < ref * 0.985:
            state = "red"
        else:
            state = "green"
        # TWO-GATE tier
        confirmed = (streak >= 2) and clean and (band == "disciplined") and state != "red"
        tier = "main" if confirmed else "watch"
        why_watch = None
        if tier == "watch":
            why_watch = ("blow-off U/D — wait, don't chase" if band == "blowoff" else
                         "going red — unproven" if state == "red" else
                         "news/event flagged" if not clean else
                         "new (day 1) — needs to hold the gate" if streak < 2 else
                         "U/D not in the disciplined band")
        rows.append({"s": s, "n": p.get("n", s), "sector": p.get("sector"), "industry": p.get("industry"),
                     "ud": ud, "ud_band": band, "ud_rising": rising, "clean": clean,
                     "price": round(ref, 1), "now": round(now, 1), "chg": chg, "target": round(tgt, 1),
                     "to_target_pct": to_tgt, "tgtpct": p.get("tgtpct"), "stop": p.get("stop"),
                     "streak": streak, "state": state, "tier": tier, "why_watch": why_watch,
                     "entry": p.get("entry")})
    main = sorted([r for r in rows if r["tier"] == "main"], key=lambda x: (x["state"] != "gold", -(x["ud"] or 0)))
    watch = sorted([r for r in rows if r["tier"] == "watch"], key=lambda x: -(x["ud"] or 0))
    return {"ok": True, "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"), "price_source": src,
            "n_main": len(main), "n_watch": len(watch), "main": main, "watch": watch,
            "counts": {"gold": sum(1 for r in rows if r["state"] == "gold"),
                       "green": sum(1 for r in rows if r["state"] == "green"),
                       "red": sum(1 for r in rows if r["state"] == "red"),
                       "blowoff": sum(1 for r in rows if r["ud_band"] == "blowoff")},
            "note": "GOLD = target hit · GREEN = climbing, gates holding, disciplined U/D · RED = false move / "
                    "weak U/D / blow-off / news-dirty. Watchlist = unproven (day-1 / blow-off / red); promotes "
                    "to MAIN once it holds the gate >=2 sessions with a disciplined U/D. Live via " + src + "."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = build()
    c = r["counts"]
    print(f"\nVOLUME STATE — {r['asof']} · {r['price_source']}  (🟡{c['gold']} 🟢{c['green']} 🔴{c['red']} · blow-off {c['blowoff']})")
    print(f"\n  MAIN — promoted ({r['n_main']}):")
    for x in r["main"][:14]:
        print(f"   {({'gold':'🟡','green':'🟢','red':'🔴'}[x['state']])} {x['s']:<11} U/D {x['ud']} [{x['ud_band']}] · ₹{x['now']} ({x['chg']:+}%) → ₹{x['target']} ({x['to_target_pct']:+}%) · {x['streak']}d")
    print(f"\n  WATCHLIST — provisional, gate-2 ({r['n_watch']}):")
    for x in r["watch"][:14]:
        print(f"   {({'gold':'🟡','green':'🟢','red':'🔴'}[x['state']])} {x['s']:<11} U/D {x['ud']} [{x['ud_band']}] · ₹{x['now']} ({x['chg']:+}%) · {x['why_watch']}")
