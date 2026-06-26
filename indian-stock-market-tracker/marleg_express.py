"""
marleg_express.py — the "HOW to express it" conscientious meter: once a LONG (or short) is plausible,
should you take it as MTF (margin on the stock) or as an OPTION (call/put)?

The honest trade-offs (backtested + first-principles):
  • MTF = LINEAR exposure with borrow interest (~15%/yr ≈ 0.04%/day), NO theta, NO expiry. Good for SLOW or
    timing-uncertain moves, steadier names, lower conviction on WHEN. It just costs carry while you wait.
  • OPTION = CONVEX, but you pay THETA and face EXPIRY. Good ONLY for FAST / BIG / high-conviction moves with
    a clear runway and non-rich IV. On a slow grind, theta eats you alive ([[user_otm_call_style]] caveat).

So the meter (−100 = MTF … +100 = OPTION) weighs: move SIZE, conviction, daily amplitude, IV rich/cheap —
AND it is GATED on the option side: if there's no liquid strike, or the hold needed runs past the expiry /
into the theta cliff, the needle is FORCED back toward MTF and the failing check is named. "Options is a go"
only when the expiry/theta/liquidity checks pass — exactly the user's rule.

express(sym, side, ...) → meter + label + the signed factors + the option-checks + the target ETA.
Read-only decision-support — you place the trade on Groww.
"""
import datetime as dt

import marleg_cockpit as ck_mod
import marleg_vol as mv


def _nearest_expiry_dte(th):
    live = th.get("live") if isinstance(th, dict) else None
    if live and live.get("dte"):
        return int(live["dte"])
    today = dt.date.today()
    y, m = today.year, today.month
    e = mv.monthly_expiry(y, m)
    if e < today:
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        e = mv.monthly_expiry(y, m)
    return max((e - today).days, 1)


def _clearing_expiry(hold_cal):
    """The first monthly expiry whose tenor comfortably clears the hold (hold + ~5d buffer) — i.e. the
    expiry you'd ACTUALLY buy, not the near-month that would die first. Returns (dte, 'DD Mon') or (None, None)."""
    today = dt.date.today()
    y, m = today.year, today.month
    for _ in range(5):                                   # scan up to ~5 monthly expiries out
        e = mv.monthly_expiry(y, m)
        d = (e - today).days
        if d >= hold_cal + 5:
            return d, e.strftime("%d %b")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return None, None


def express(sym, side="LONG", capital=100000.0, risk_pct=1.0, profile="normal", horizon="swing"):
    sym = (sym or "").upper().strip()
    side = (side or "LONG").upper()
    horizon = (horizon or "swing").lower()
    ck = ck_mod.cockpit(sym, side, None, capital, risk_pct, profile=profile)
    if not ck.get("ok"):
        return {"ok": False, "sym": sym, "error": ck.get("error", "no plan")}
    entry, atr, spot = ck["entry"], ck["atr"], ck["spot"]
    final_tp = ck["tps"][-1]["px"]
    payout = abs(final_tp - entry) / entry * 100 if entry else 0
    atr_pct = atr / spot * 100 if spot else 0
    eta = (ck.get("target_eta") or {}).get("sessions") or 10
    hold_cal = max(int(eta * 1.4) + 1, 2)                      # sessions → ~calendar days

    # conviction from the horizon engine (best-effort)
    rating = None
    try:
        import marleg_horizon as hz
        r = hz.rate(sym)
        if r.get("ok"):
            rating = r.get("rating")
            if r.get("ideal_payout_pct"):
                payout = r["ideal_payout_pct"]
            hd = r.get("ideal_hold_days")
            if hd:
                hold_cal = max(int((sum(hd) / 2) * 1.4) + 1, 2)
    except Exception:
        pass

    # theta / IV / expiry context
    th = {}
    try:
        import marleg_theta_surface as ts
        th = ts.surface(sym)
    except Exception:
        pass
    near_dte = _nearest_expiry_dte(th)
    cliff = (th or {}).get("cliff_dte") or 14
    live = (th or {}).get("live")
    iv_verdict = (live or {}).get("verdict")
    liquid = live is not None
    chosen_dte, chosen_date = _clearing_expiry(hold_cal)       # the expiry you'd actually buy
    remaining_at_exit = (chosen_dte - hold_cal) if chosen_dte else None   # option life left when you plan to exit

    now_ist = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")

    # ─── INTRADAY (scalp) branch — a same-day trade is a DIFFERENT game: near-expiry options are GOOD
    #     (max gamma, you exit by close so theta/expiry-cycle don't matter); amplitude + momentum rule,
    #     delivery%/long-term gates matter less. This is where AMBER-type day-high spikes are the play.
    if horizon == "intraday":
        FI = []

        def addi(n, v, why):
            FI.append({"name": n, "v": round(v), "why": why})

        if atr_pct >= 3:
            addi("day amplitude", 25, f"{atr_pct:.1f}% ATR/day — big range, option gamma pays on a scalp")
        elif atr_pct >= 1.5:
            addi("day amplitude", 8, f"{atr_pct:.1f}% ATR/day — tradeable intraday range")
        else:
            addi("day amplitude", -16, f"{atr_pct:.1f}% ATR/day — too quiet to scalp with an option")
        if rating is None:
            addi("intraday signal", 0, "no setup read")
        elif rating >= 70:
            addi("intraday signal", 16, f"STRONG setup ({rating}/100) — gates aligned → the option is the efficient bet")
        elif rating >= 55:
            addi("intraday signal", 4, f"ok setup ({rating}/100)")
        else:
            addi("intraday signal", -12, f"weak setup ({rating}/100) — use leverage (MTF/MIS), don't pay option premium")
        if iv_verdict == "RICH":
            addi("IV", -10, "ATM is RICH — you pay the spread, but it's a 1-day hold so crush risk is small")
        elif iv_verdict == "CHEAP":
            addi("IV", 12, "ATM is CHEAP — cheap gamma for the scalp")
        gate_fail_i = None
        if liquid:
            addi("expiry/gamma", 12, f"near expiry ({near_dte}d) = MAX gamma for a same-day scalp; one day of theta is tiny — near-expiry is GOOD here, not bad")
        else:
            addi("liquidity", -40, "no liquid strike — scalp via MTF/MIS or futures instead")
            gate_fail_i = "no liquid strike"
        meter_i = max(-100, min(100, sum(f["v"] for f in FI)))
        if gate_fail_i:
            meter_i = min(meter_i, -25)
        if meter_i >= 25:
            label_i, rec_i = "OPTION · gamma scalp", "Scalp it with a near-expiry option — max gamma, exit by close. Strong setup + range justify the premium."
        elif meter_i >= -15:
            label_i, rec_i = "EITHER", "Marginal — intraday MTF/MIS (or futures) is the cheaper vehicle unless the setup is strong."
        else:
            label_i, rec_i = "MTF / FUTURES", "Scalp via intraday MTF/MIS or futures — linear, no premium to pay; the option edge isn't there today."
        return {
            "ok": True, "sym": sym, "side": side, "horizon": "intraday", "meter": meter_i, "label": label_i,
            "recommendation": rec_i, "factors": sorted(FI, key=lambda x: -abs(x["v"])),
            "checks": {"liquid": liquid, "near_expiry_dte": near_dte, "cliff_dte": cliff, "iv": iv_verdict},
            "gate_fail": gate_fail_i, "entry": entry, "final_target": ck["tps"][0]["px"], "payout_pct": round(payout, 1),
            "atr_pct": round(atr_pct, 2), "rating": rating, "iv_verdict": iv_verdict,
            "target_eta": {"sessions": "same day", "date": "by close", "note": "intraday — exit by close, no overnight hold."},
            "option_timing": {"verdict": ("BUY NOW · scalp" if liquid else "MTF / FUTURES"), "tone": ("pos" if liquid else "amb"),
                              "why": ("Intraday: a near-expiry option is MAX gamma and you're out by close, so the theta cliff is irrelevant. "
                                      "Buy when your intraday trigger fires — do NOT carry it overnight." if liquid
                                      else "No liquid strike — scalp the move with intraday MTF/MIS or futures.")},
            "short_note": ("SHORT intraday = buy puts / short futures, flat by close (India's drift is overnight)." if side == "SHORT" else None),
            "asof": now_ist,
            "note": "INTRADAY mode — a same-day scalp. High-amplitude movers (the AMBER-type day-high spikes) are the play; "
                    "theta/expiry-cycle don't matter (out by close); delivery% and long-term gates matter less than today's range + momentum. "
                    "Read-only; you place it on Groww.",
        }

    F = []                                                      # signed factors: + = OPTION, − = MTF (positional / swing)

    def add(name, v, why):
        F.append({"name": name, "v": round(v), "why": why})

    # 1) move size
    if payout >= 8:
        add("move size", 28, f"~{payout:.0f}% target — big enough for convexity to pay")
    elif payout >= 5:
        add("move size", 12, f"~{payout:.0f}% target — decent for an option")
    elif payout >= 3:
        add("move size", -8, f"~{payout:.0f}% target — modest; theta starts to outweigh convexity")
    else:
        add("move size", -22, f"only ~{payout:.0f}% target — too small; theta would eat the option")
    # 2) conviction
    if rating is None:
        add("conviction", 0, "no conviction read")
    elif rating >= 78:
        add("conviction", 16, f"high conviction ({rating}/100) — worth paying theta")
    elif rating >= 65:
        add("conviction", 6, f"decent conviction ({rating}/100)")
    else:
        add("conviction", -10, f"low conviction ({rating}/100) — don't pay theta on a maybe")
    # 3) daily amplitude
    if atr_pct >= 4:
        add("amplitude", 10, f"{atr_pct:.1f}% ATR/day — moves fast, convex payoff")
    elif atr_pct < 2:
        add("amplitude", -6, f"{atr_pct:.1f}% ATR/day — slow mover, theta grinds you")
    else:
        add("amplitude", 2, f"{atr_pct:.1f}% ATR/day")
    # 4) IV rich/cheap
    if iv_verdict == "RICH":
        add("IV", -18, "live ATM is RICH vs model — you'd overpay + risk IV-crush → MTF")
    elif iv_verdict == "CHEAP":
        add("IV", 14, "live ATM is CHEAP — good time to buy optionality")
    elif iv_verdict == "FAIR":
        add("IV", 2, "IV fair vs model")
    # 5) MTF carry on a long hold
    if hold_cal > 20:
        add("hold length", 6, f"~{hold_cal}d hold — MTF interest (~15%/yr) adds up over that long")

    # 6) THE EXPIRY / LIQUIDITY GATE (decides whether the option side may light up) — judged against the
    #    expiry you'd ACTUALLY buy (the one that clears the hold), not the near-month that would die first.
    checks = {"liquid": liquid, "near_expiry_dte": near_dte, "buy_expiry_dte": chosen_dte, "buy_expiry": chosen_date,
              "cliff_dte": cliff, "hold_cal": hold_cal, "iv": iv_verdict, "remaining_at_exit": remaining_at_exit}
    gate_fail = None
    if not liquid:
        add("liquidity", -40, "no liquid ATM strike resolved — you can't reliably enter/exit an option")
        gate_fail = "no liquid strike"
    elif chosen_dte is None:
        add("expiry", -45, f"~{hold_cal}d hold is too long — no listed monthly clears it without huge theta")
        gate_fail = "hold too long for a clean expiry"
    elif remaining_at_exit >= cliff:
        add("expiry", 14, f"buy the {chosen_date} expiry ({chosen_dte}d) — exits with ~{remaining_at_exit}d left, clear of the {cliff}d cliff")
    elif remaining_at_exit >= cliff * 0.5:
        add("expiry", 4, f"buy the {chosen_date} expiry ({chosen_dte}d) — workable, but exit (~{remaining_at_exit}d left) lands in mild cliff")
    else:
        add("expiry", -12, f"even the {chosen_date} expiry exits deep in the {cliff}d cliff (~{remaining_at_exit}d left) — option timing is tight")

    meter = max(-100, min(100, sum(f["v"] for f in F)))
    if gate_fail:                                             # options can't light up if a hard check failed
        meter = min(meter, -25)

    if meter <= -50:
        label, rec = "STRONG MTF", "Take it as MTF (or cash) — the option side fails on cost/timing."
    elif meter <= -15:
        label, rec = "MTF", "Lean MTF — linear exposure, no theta clock; let the move come on its own time."
    elif meter < 15:
        label, rec = "EITHER", "Marginal — MTF is the safer default; only go option if you have a hard catalyst."
    elif meter < 50:
        label, rec = "OPTION", "An option (call) expresses this well — fast/big enough, runway OK."
    else:
        label, rec = "STRONG OPTION", "High-conviction, fast, cheap-IV with runway — the option is the efficient bet."

    short_note = None
    if side == "SHORT":
        short_note = ("SHORT = buying PUTS (or MTF-short). Note: shorting is a structural ANTI-edge in India "
                      "(market drifts up, weak names bounce) — treat this as a hedge/confirmed-downtrend play only.")

    return {
        "ok": True, "sym": sym, "side": side, "horizon": horizon, "meter": meter, "label": label, "recommendation": rec,
        "factors": sorted(F, key=lambda x: -abs(x["v"])), "checks": checks, "gate_fail": gate_fail,
        "target_eta": ck.get("target_eta"), "entry": entry, "final_target": final_tp, "payout_pct": round(payout, 1),
        "atr_pct": round(atr_pct, 2), "rating": rating, "iv_verdict": iv_verdict, "short_note": short_note,
        "option_timing": (th or {}).get("timing"),
        "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
        "note": "MTF = linear + carry, no expiry (slow/uncertain-timing moves). OPTION = convex but theta + expiry "
                "(fast/big/high-conviction with cheap IV + runway). The option side is GATED on liquidity + expiry "
                "+ theta-cliff — it only lights up when those pass. Read-only; you place it on Groww.",
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for u in (sys.argv[1:] or ["LT"]):
        e = express(u)
        if not e.get("ok"):
            print("  " + u + ": " + e.get("error", "?")); continue
        print(f"\n═══ {e['sym']} {e['side']} · EXPRESS: {e['label']} (meter {e['meter']:+d}) ═══")
        print(f"  {e['recommendation']}")
        et = e.get("target_eta") or {}
        print(f"  target {e['final_target']} (~{e['payout_pct']}%) · ETA ~{et.get('sessions')} sessions (~by {et.get('date')})")
        c = e["checks"]
        print(f"  checks: liquid={c['liquid']} buy-expiry={c['buy_expiry']}({c['buy_expiry_dte']}d) cliff={c['cliff_dte']}d hold≈{c['hold_cal']}d IV={e['iv_verdict']}")
        for f in e["factors"]:
            print(f"    {f['v']:+4d}  {f['name']:<12} {f['why']}")
