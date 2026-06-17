"""
marleg_intraday_trigger.py — LIVE intraday trigger watcher for a name you're stalking.

Answers "tell me the MOMENT the bulls actually take over" with objective, backtest-grounded triggers
instead of a feeling. For a ticker it builds the intraday scoreboard (VWAP, opening-range high,
support) and stages the triggers in the order control actually flips:

  BELOW VWAP        sellers from the morning still have the edge — wait
  VWAP_RECLAIM      a 5m close back ABOVE VWAP -> control flipping to buyers (early, lower-conviction)
  ORH_BREAK         a 5m close breaks the opening-range high -> the real "bulls take over"
                    (our cup-handle backtest: buy the HELD break = 62-67% win, not the raw bar = 51%)
  SUPPORT_LOSS      a 5m close below support -> thesis invalidated, stand down

Each state carries the disciplined entry / stop / target the pod's framework would mark, and watch()
pushes a self-contained Slack card on every FRESH trigger during market hours.

Read-only, decision-support — never places an order. Bars via marleg_intraday.gbars (live yfinance +
zero-lag tick-recorder); price can lag a minute or two.

  python marleg_intraday_trigger.py TEJASNET            # one-shot pulse
  python marleg_intraday_trigger.py TEJASNET --watch    # loop + Slack until close
"""
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
CT = timezone(timedelta(hours=-5))
ORH_MIN = 12        # opening-range = first 12 five-min bars (~60 min)


def _clocks():
    return (datetime.now(IST).strftime("%a %d %b %H:%M IST") + " · " +
            datetime.now(CT).strftime("%H:%M US-CT"))


def _today_bars(tk, interval):
    """Today's intraday OHLCV, lower-cased columns, IST-filtered to the live session. yfinance 5m carries
    today's NSE session near-real-time and (unlike the tick-recorder merge) keeps the full morning incl. the
    opening spike — which is exactly the pivot the trader is watching."""
    import yfinance as yf
    h = yf.Ticker(tk.upper() + ".NS").history(period="1d", interval=f"{interval}m").dropna()
    if h is None or h.empty:
        return None
    h = h.rename(columns={c: c.lower() for c in h.columns})
    return h[["open", "high", "low", "close", "volume"]]


def pulse(tk, interval=5, support=None, pivot=None):
    df = _today_bars(tk, interval)
    if df is None or len(df) < 4:
        return {"ok": False, "error": f"no live intraday bars for {tk} yet", "tk": tk}
    o = float(df["open"].iloc[0]); hi = float(df["high"].max()); lo = float(df["low"].min())
    cur = float(df["close"].iloc[-1]); prev = float(df["close"].iloc[-2])
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    vwap_s = (tp * df["volume"]).cumsum() / df["volume"].cumsum().replace(0, np.nan)
    vwap = float(vwap_s.iloc[-1]); vwap_prev = float(vwap_s.iloc[-2])
    orh = float(df["high"].iloc[:ORH_MIN].max())          # opening-range high (the breakout pivot)
    piv = float(pivot) if pivot else orh
    sup = float(support) if support else round(lo, 1)      # default support = day low
    # state
    above = cur > vwap
    reclaim = (cur > vwap) and (prev <= vwap_prev)         # fresh cross up through VWAP
    brk = cur > piv
    brk_held = brk and (prev > piv)                        # the HELD breakout (our edge)
    sup_loss = cur < sup
    if sup_loss:
        status, light = "SUPPORT LOST — stand down", "red"
    elif brk_held:
        status, light = "BREAKOUT confirmed — bulls in control", "green"
    elif brk:
        status, light = "BREAKOUT (unconfirmed — needs a 2nd bar to hold)", "amber"
    elif above:
        status, light = "ABOVE VWAP — control flipping to buyers", "amber"
    else:
        status, light = "BELOW VWAP — sellers still have the edge", "dim"
    # entry plans (two models). breakout = our backtested edge; vwap-reclaim = earlier/aggressive.
    bo_entry = round(piv, 1)
    bo_stop = round(max(sup, vwap), 1)                     # back inside the range = invalidation
    if bo_stop >= bo_entry:
        bo_stop = round(min(sup, piv * 0.985), 1)
    bo_tgt = round(piv + (piv - sup), 1)                  # measured move (range height projected up)
    bo_rr = round((bo_tgt - bo_entry) / (bo_entry - bo_stop), 2) if bo_entry > bo_stop else None
    vw_entry = round(vwap, 1); vw_stop = round(sup, 1); vw_tgt = round(piv, 1)
    vw_rr = round((vw_tgt - vw_entry) / (vw_entry - vw_stop), 2) if vw_entry > vw_stop else None
    return {"ok": True, "tk": tk, "asof": _clocks(), "price": round(cur, 1), "light": light, "status": status,
            "vwap": round(vwap, 1), "above_vwap": above, "vwap_reclaim": reclaim,
            "day_high": round(hi, 1), "day_low": round(lo, 1), "open": round(o, 1),
            "pivot": round(piv, 1), "support": sup, "orh": round(orh, 1),
            "breakout": brk, "breakout_held": brk_held, "support_loss": sup_loss,
            "to_vwap_pct": round((vwap / cur - 1) * 100, 2), "to_pivot_pct": round((piv / cur - 1) * 100, 2),
            "cushion_pct": round((cur / sup - 1) * 100, 2),
            "plan_breakout": {"label": "Breakout (confirmed) — higher-conviction", "entry": bo_entry,
                              "stop": bo_stop, "target": bo_tgt, "rr": bo_rr,
                              "risk_pct": round((bo_entry - bo_stop) / bo_entry * 100, 2)},
            "plan_vwap": {"label": "VWAP reclaim — earlier / aggressive", "entry": vw_entry,
                          "stop": vw_stop, "target": vw_tgt, "rr": vw_rr,
                          "risk_pct": round((vw_entry - vw_stop) / vw_entry * 100, 2)},
            "note": "Triggers are objective; sizing is yours — size so (entry-stop) × qty ≤ your per-trade risk cap "
                    "(War Room loss-limit). F&O: a ~2-3% underlying move ≈ 6-8% on a near-month future/ATM option, "
                    "so you don't need the full cash move. Monitor-only — not advice."}


def _slack(p, kind):
    import marleg_slack as sk
    tk = p["tk"]
    if kind == "armed":
        sk.card(f"🎯 *Trigger watch ARMED — {tk}* @ ₹{p['price']}",
                lines=[f"now: {p['status']}",
                       f"VWAP ₹{p['vwap']} · pivot ₹{p['pivot']} (day high) · support ₹{p['support']}",
                       f"breakout plan: enter ₹{p['plan_breakout']['entry']} · stop ₹{p['plan_breakout']['stop']} · target ₹{p['plan_breakout']['target']} (R:R {p['plan_breakout']['rr']})"],
                action=f"watching {tk} 5m — I'll ping you the moment it reclaims VWAP, breaks the pivot, or loses support.")
        return
    if kind == "vwap":
        sk.card(f"🟡 *{tk} reclaimed VWAP* @ ₹{p['price']}",
                lines=[f"₹{p['price']} closed back above VWAP ₹{p['vwap']} — intraday control flipping to buyers.",
                       f"this is the EARLY tell; the high-conviction trigger is a break of ₹{p['pivot']}.",
                       f"aggressive entry ₹{p['plan_vwap']['entry']} · stop ₹{p['plan_vwap']['stop']} · target ₹{p['plan_vwap']['target']} (R:R {p['plan_vwap']['rr']})"],
                action=f"OPTIONAL early add. Wait for the ₹{p['pivot']} break-and-hold for the real entry. Stop ₹{p['plan_vwap']['stop']}.")
    elif kind == "breakout":
        sk.card(f"🟢 *{tk} BREAKOUT — bulls took over* @ ₹{p['price']}",
                lines=[f"5m close broke & HELD the pivot ₹{p['pivot']} (the backtested 62-67% entry, not the raw bar).",
                       f"enter ₹{p['plan_breakout']['entry']} · stop ₹{p['plan_breakout']['stop']} · target ₹{p['plan_breakout']['target']} (R:R {p['plan_breakout']['rr']}, risk {p['plan_breakout']['risk_pct']}%)",
                       "daily is extended (+22% over 50DMA) — take a first scale at target and TRAIL, don't marry it."],
                action=f"Long trigger LIVE. Size to your loss-limit. Hard stop ₹{p['plan_breakout']['stop']} (a 5m close below VWAP voids it).")
    elif kind == "support":
        sk.card(f"🔴 *{tk} lost support* @ ₹{p['price']}",
                lines=[f"5m close below support ₹{p['support']} — the base failed; the bull thesis is invalidated for now.",
                       "no long here — this is the falling-knife zone our backtest says not to catch."],
                action="STAND DOWN on the long. Re-arm only if it reclaims VWAP again.")


def watch(tk, interval_s=180, support=None, pivot=None, max_iters=200):
    last = None
    fired = set()
    print(f"watching {tk} — pulse every {interval_s}s until ~15:35 IST. {_clocks()}")
    for _ in range(max_iters):
        now = datetime.now(IST)
        if now.hour > 15 or (now.hour == 15 and now.minute >= 35):
            print("market closed — stopping watch."); break
        if now.hour < 9 or (now.hour == 9 and now.minute < 15):
            time.sleep(interval_s); continue
        p = pulse(tk, support=support, pivot=pivot)
        if not p.get("ok"):
            print(p.get("error")); time.sleep(interval_s); continue
        st = p["status"]
        print(f"  {p['asof']}  ₹{p['price']}  {st}  (VWAP {p['vwap']} pivot {p['pivot']} sup {p['support']})")
        if last is None:
            _slack(p, "armed")
        if p["support_loss"] and "support" not in fired:
            _slack(p, "support"); fired.add("support")
        elif p["breakout_held"] and "breakout" not in fired:
            _slack(p, "breakout"); fired.add("breakout")
        elif p["above_vwap"] and not p["breakout"] and "vwap" not in fired:
            _slack(p, "vwap"); fired.add("vwap")
        last = st
        time.sleep(interval_s)
    print("watch ended.")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = (sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "TEJASNET").upper()
    sup = None; piv = None
    for a in sys.argv:
        if a.startswith("--support="):
            sup = float(a.split("=")[1])
        if a.startswith("--pivot="):
            piv = float(a.split("=")[1])
    if "--watch" in sys.argv:
        watch(tk, support=sup, pivot=piv)
    else:
        p = pulse(tk, support=sup, pivot=piv)
        if not p.get("ok"):
            print(p.get("error")); sys.exit()
        print(f"\n{tk}  {p['asof']}   ₹{p['price']}   [{p['status']}]")
        print(f"  VWAP ₹{p['vwap']} ({p['to_vwap_pct']:+}% away) · pivot ₹{p['pivot']} ({p['to_pivot_pct']:+}% away) · support ₹{p['support']} (cushion {p['cushion_pct']:+}%)")
        for k in ("plan_breakout", "plan_vwap"):
            x = p[k]
            print(f"  {x['label']:<38} entry ₹{x['entry']}  stop ₹{x['stop']}  target ₹{x['target']}  R:R {x['rr']}  (risk {x['risk_pct']}%)")
        print(f"  {p['note']}")
