"""
marleg_call_setups.py — where is a CALL actually worth buying? (clean, optionable, with runway)

Buying calls usually loses in India (positive vol-risk-premium + theta), so this is deliberately strict —
it returns only names that clear ALL of:
  • F&O-optionable          (you can actually buy a call)
  • a confirmed directional setup (cup-handle CONFIRMED / reversal PRIME / gated leader; conviction >= 65)
  • real AMPLITUDE          (ATR% >= 2.5 — a call needs the underlying to MOVE to pay)
  • NOT at resistance       (fib: room to the prior-high target, so the call has runway — not a ceiling)
  • news-clean              (no earnings/event contamination)
plus the disciplined structure note (slightly-ITM, NEXT expiry not today's, size small).

  python marleg_call_setups.py
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
MIN_CONV = 65
MIN_ATRP = 2.5


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def setups(top=12):
    import marleg_autopilot as ap
    try:
        import marleg_options_monitor as mom
        FNO = set(mom.FNO_UNDERLYINGS)
    except Exception:
        FNO = set()
    movers = {m["s"]: m for m in _load("marleg_movers.json").get("movers", [])}
    gated = {p["s"]: p for p in _load("marleg_gated_cache.json").get("picks", [])}
    base = ap.signals()
    rows, rejected = [], {"not_fno": 0, "low_conv": 0, "at_resistance": 0, "low_amp": 0, "dirty": 0}
    for sg in base:
        s = sg["s"]
        if FNO and s not in FNO:
            rejected["not_fno"] += 1; continue
        if (sg.get("conv") or 0) < MIN_CONV:
            rejected["low_conv"] += 1; continue
        gp = gated.get(s, {})
        if gp.get("at_resistance"):
            rejected["at_resistance"] += 1; continue        # at the ceiling = no runway for a call
        mv = movers.get(s, {})
        atrp = mv.get("atrp")
        if atrp is None or atrp < MIN_ATRP:
            rejected["low_amp"] += 1; continue                # calls need amplitude
        if sg.get("clean") is False or mv.get("clean") is False:
            rejected["dirty"] += 1; continue
        target = gp.get("target") or sg.get("target")
        rows.append({"s": s, "conv": sg.get("conv"), "sources": sg.get("sources", []), "thesis": sg.get("thesis"),
                     "entry": sg.get("entry"), "target": target, "payout_pct": sg.get("payout_pct"),
                     "atrp": atrp, "amp": mv.get("tag"), "fib": gp.get("fib"),
                     "resistance": gp.get("resistance"), "ext_1272": gp.get("ext_1272")})
    rows.sort(key=lambda x: (0 if x["amp"] == "HIGH" else 1, -(x["conv"] or 0)))
    cal = {}
    try:
        import marleg_expiry as ex
        c = ex.calendar_pos()
        cal = {"weekly": c["weekly_expiry"], "days_to_weekly": c["days_to_weekly"],
               "monthly": c["monthly_expiry"], "is_expiry_today": c["is_weekly_today"]}
    except Exception:
        pass
    vix = None
    try:
        import yfinance as yf
        v = yf.Ticker("^INDIAVIX").history(period="2d")["Close"].dropna()
        vix = round(float(v.iloc[-1]), 1)
    except Exception:
        pass
    return {"ok": True, "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
            "n": len(rows), "setups": rows[:top], "rejected": rejected, "expiry": cal, "india_vix": vix,
            "structure": ("Disciplined call: slightly-ITM (delta ~0.6, less theta) · use the "
                          + ("NEXT expiry (today is weekly expiry — don't buy into the decay)" if cal.get("is_expiry_today")
                             else "near-month") + " · size small (define max loss = the premium) · exit fast if the "
                          "move doesn't come in a few sessions."),
            "note": "Strict by design — calls bleed in India's positive-VRP/low-VIX tape, so this only lists "
                    "F&O names with a CONFIRMED setup + real amplitude + runway to the resistance. Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = setups()
    print(f"\nCLEAN CALL SETUPS — {r['asof']} · India VIX {r['india_vix']} · expiry {r['expiry'].get('weekly')} ({r['expiry'].get('days_to_weekly')}d)")
    if not r["setups"]:
        print("  NONE clear the bar right now —", r["rejected"])
    for x in r["setups"]:
        print(f"  {x['s']:<11} conv {x['conv']} [{'+'.join(x['sources'])}] amp {x['amp']} ATR%{x['atrp']} fib {x['fib']} → ₹{x['target']} (+{x['payout_pct']}%)")
        print(f"       {x['thesis']}")
    print(f"\n  rejected: {r['rejected']}")
    print(f"  STRUCTURE: {r['structure']}")
