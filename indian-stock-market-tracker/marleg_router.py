"""
marleg_router.py — THE TRADE-ROUTE ROUTER: one engine that decides WHICH strategy a name belongs to.

You have many gates (buy-hold, swing-long, intraday-amplitude, avoid/pairs). They compete, complement, and
counter. This routes each name to the CORRECT trade route via a cascade whose weights come from the pod's
OWN backtests — not a fitted black box (that would overfit). Honest hierarchy, in order of validated power:

  REGIME  (master)         bull -> longs enabled ; bear/shock -> defensive only      [the risk-adjusted edge]
  ROUTE   (score each)     BUY-HOLD | SWING-LONG | INTRADAY-WATCH | AVOID/PAIRS
  RESOLVE (compete/etc.)   regime > don't-chase-extended > confluence > setup
        -> agree = complement (high conviction) ; clash = dominant gate wins (e.g. quality-but-LATE = HOLD-don't-add)

Returns the primary route + conviction + the runner-up + WHY it resolved that way. Read-only decision-support.

  python marleg_router.py RELIANCE
  python marleg_router.py --scan RELIANCE BHEL TEJASNET SBIN GAIL
"""
import sys


def _compounder(sym):
    try:
        import marleg_buyhold as bh
        c = bh.compounder_score(sym)
        if isinstance(c, dict) and (c.get("above200") is not None or c.get("cagr3_pct") is not None):
            sc = (40 if c.get("above200") else 0) + min(max((c.get("cagr3_pct") or 0) / 3, 0), 40) \
                 + (15 if (c.get("from_52wh_pct") or -99) > -15 else 0)
            return round(min(sc, 100)), c
    except Exception:
        pass
    # fallback: a durable-trend proxy from price (so the BUY-HOLD lane isn't blind when fundamentals don't hydrate)
    try:
        import marleg_vol as mv
        import yfinance as yf
        h = yf.Ticker(mv._yf_symbol(sym)).history(period="1y")["Close"].dropna()
        if len(h) > 200:
            above200 = bool(h.iloc[-1] > h.rolling(200).mean().iloc[-1])
            ret6 = float(h.iloc[-1] / h.iloc[-126] - 1)
            sc = (40 if above200 else 0) + min(max(ret6 * 100, 0), 45)
            return round(min(sc, 100)), {"proxy": True, "above200": above200, "ret6m_pct": round(ret6 * 100, 1)}
    except Exception:
        pass
    return 0, {}


def route(sym):
    import marleg_target as tg
    try:
        import marleg_overnight_gate as og
        bull = og._regime_bull()
    except Exception:
        bull = True
    sym = sym.upper()
    t = tg.target(sym)
    if not t.get("ok"):
        return {"ok": False, "sym": sym, "error": t.get("error", "no read")}
    spot = t.get("spot"); status = t.get("status"); conv = t.get("conviction", 0)
    mom, room, relvol, atr = t.get("mom20_pct"), t.get("room_pct"), t.get("relvol"), (t.get("atr") or 0)
    atrp = round(atr / spot * 100, 2) if spot else 0
    extended = status == "LATE"

    # ---- score each route (weights reflect the validated edges) ----
    bh_score, bh = _compounder(sym)
    legs = []
    if (mom or 0) > 0: legs.append("momentum↑")
    if (relvol or 0) > 1.2: legs.append("volume↑")
    if (room or 0) > 3: legs.append("room↑")
    if status in ("ARMED", "FIRED"): legs.append("at-trigger")
    swing_score = len(legs) * 20 if (bull and not extended and status not in ("STOPPED",)) else 0
    intraday_score = min(100, max(0, (atrp - 1.5) * 30)) if (atrp >= 2 and (relvol or 0) >= 0.8) else 0
    avoid_score = (50 if (mom or 0) < -3 else 0) + (40 if status == "STOPPED" else 0) + (25 if not bull else 0)

    scores = {"BUY-HOLD": bh_score, "SWING-LONG": swing_score, "INTRADAY-WATCH": intraday_score, "AVOID/PAIRS": avoid_score}
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    primary, pscore = ranked[0]
    runner, rscore = ranked[1]

    # ---- resolve by the hierarchy ----
    if not bull:
        primary, why = "AVOID/PAIRS", "bear/▼ regime — longs turn negative; cash, hedge, or market-neutral pairs only"
        pscore = max(avoid_score, 40)
    elif extended and primary in ("SWING-LONG", "BUY-HOLD"):
        if bh_score >= 55:
            primary, why = "HOLD (don't add)", f"quality compounder ({bh_score}/100) BUT extended/LATE short-term — hold, don't chase the add"
        else:
            primary, why = "STAND-ASIDE", "already ran (LATE) and not a durable compounder — wait for a fresh base"
    elif primary == "AVOID/PAIRS" and bh_score >= 55:
        primary, pscore = "BUY-HOLD (buy the dip)", bh_score
        why = f"quality compounder ({bh_score}/100) pulling back (mom {mom}%) — the validated 'buy the leader on a DIP', NOT a laggard breakdown; accumulate in tranches"
    elif pscore < 30:
        primary, why = "STAND-ASIDE", f"no route clears the bar (best {primary} {pscore}) — thin; pass"
    elif primary == "BUY-HOLD" and swing_score >= 60:
        why = f"COMPLEMENT — durable compounder ({bh_score}) AND a live swing setup ({', '.join(legs)}) → accumulate"
    elif primary == "SWING-LONG":
        why = f"{len(legs)}/4 confluence ({', '.join(legs)}), bull regime, not extended"
    elif primary == "BUY-HOLD":
        why = f"compounder {bh_score}/100 (above-200DMA, durable CAGR) — own it, add on dips not chases"
    elif primary == "INTRADAY-WATCH":
        why = f"high amplitude (ATR {atrp}%/day) — a mover for intraday, NOT a hold; pair with a hard loss-limit"
    else:
        why = f"relative laggard (mom {mom}%) — avoid / exit; short leg only market-neutral vs a leader"

    return {"ok": True, "sym": sym, "spot": spot, "regime_bull": bull, "status": status,
            "route": primary, "conviction": pscore, "runner_up": runner, "runner_score": rscore,
            "scores": scores, "atrp": atrp, "compounder": bh_score, "swing_legs": legs, "why": why,
            "note": "Validated-edge-weighted cascade (not an overfit regression). Regime > don't-chase > "
                    "confluence > setup. Read-only decision-support — the desk gives the plan for the route."}


def scan(names):
    out = [route(n) for n in names]
    out = [x for x in out if x.get("ok")]
    rank = {"SWING-LONG": 0, "COMPLEMENT": 0, "BUY-HOLD": 1, "HOLD (don't add)": 2, "INTRADAY-WATCH": 3,
            "AVOID/PAIRS": 4, "STAND-ASIDE": 5}
    out.sort(key=lambda x: (rank.get(x["route"], 9), -x["conviction"]))
    buckets = {}
    for x in out:
        buckets.setdefault(x["route"], []).append(x["sym"])
    return {"ok": True, "n": len(out), "rows": out, "buckets": buckets,
            "note": "Auto-scan: every name routed to its correct strategy lane, conflicts resolved."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    a = sys.argv[1:]
    if a and a[0] == "--scan":
        r = scan(a[1:] or ["RELIANCE", "BHEL", "TEJASNET", "SBIN", "GAIL"])
        print(f"\n  regime: {'BULL' if (r['rows'] and r['rows'][0]['regime_bull']) else 'BEAR/▼'}")
        for x in r["rows"]:
            print(f"  {x['sym']:<11} -> {x['route']:<18} conv {x['conviction']:>3} · {x['why']}")
        print("\n  buckets:", {k: v for k, v in r["buckets"].items()})
    else:
        sym = a[0] if a else "RELIANCE"
        x = route(sym)
        if not x.get("ok"):
            print(x.get("error")); sys.exit()
        print(f"\n  {x['sym']}  ₹{x['spot']}  ·  regime {'BULL' if x['regime_bull'] else 'BEAR/▼'} · status {x['status']}")
        print(f"  route scores: {x['scores']}")
        print(f"\n  ➤ ROUTE: {x['route']}  (conviction {x['conviction']}, runner-up {x['runner_up']} {x['runner_score']})")
        print(f"  WHY: {x['why']}")
        print(f"\n  {x['note']}")
