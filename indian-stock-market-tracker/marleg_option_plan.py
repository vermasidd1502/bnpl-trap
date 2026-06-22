"""
marleg_option_plan.py — the OPTION TRADE DECK: is a long plausible, on what timeline, to what targets,
and (if optionable) which call to buy + WHEN to buy and sell it.

Synthesis, not new signals:
  • marleg_horizon.rate()        → direction / conviction / horizon / ideal hold / target / stop / R:R
                                   (already folds in trend, gated volume, cup-handle, industry leadership)
  • marleg_opt_value.optimize_otm_calls() → which OTM call, for the "ride into ATM" style
  • here                         → the theta-aware BUY / SELL CALENDAR (concrete dates)

Timing logic for the option leg:
  - pick the nearest monthly expiry that comfortably covers the hold PLUS a ~12-day buffer, so you exit
    BEFORE the theta cliff (the last ~2 weeks where decay accelerates).
  - BUY now (this/next session). SELL on target/touch, else a TIME-STOP at the end of the ideal window
    (don't let an OTM call rot), and a HARD exit ≈ expiry − 10 sessions regardless.

Read-only PLANNING — proposes dates, never places/schedules a real order on the Groww account.
"""
import datetime as dt
import math

import marleg_vol as mv

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def _add_sessions(start, n):
    """Add n trading sessions (skip Sat/Sun) to a date → calendar date. (Holidays not modelled.)"""
    d, added = start, 0
    while added < n:
        d += dt.timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d


def _sessions_until(d):
    """Approx trading sessions from today until date d (skip weekends)."""
    t, n = dt.date.today(), 0
    while t < d:
        t += dt.timedelta(days=1)
        if t.weekday() < 5:
            n += 1
    return n


def _monthly_expiries(n=5):
    today = dt.date.today()
    out, y, m = [], today.year, today.month
    while len(out) < n:
        lt = mv.monthly_expiry(y, m)
        if lt >= today:
            out.append(lt)
        y, m = (y + (m == 12)), (1 if m == 12 else m + 1)
    return out


def _option_detail(tk, strike, kind, exp, und_chg):
    """Rich live read on ONE option: liquidity verdict + OI-buildup signal + the raw book numbers."""
    import marleg_options_monitor as mom
    try:
        q = mom.option_quote(mom.build_symbol(tk, strike, kind, exp))
        if not isinstance(q, dict) or "error" in q or not q.get("ltp"):
            return None
        return {"ltp": q.get("ltp"), "bid": q.get("bid"), "ask": q.get("ask"), "spread_pct": q.get("spread_pct"),
                "oi": q.get("oi"), "oi_change": q.get("oi_change"), "volume": q.get("volume"),
                "liquidity": mom._liquidity_verdict(q),
                "oi_signal": mom._oi_signal(q.get("oi_change"), und_chg, kind)}
    except Exception:
        return None


def deck(tk, expiry=None):
    """One-look option-trade verdict for a stock."""
    import marleg_horizon as hz
    import marleg_options_monitor as mom
    tk = tk.upper().strip()
    r = hz.rate(tk)
    if not r.get("ok"):
        return {"ok": False, "error": r.get("error"), "tk": tk}
    price = r["price"]
    horizon = r["horizon"]
    plausible = not horizon.startswith("AVOID")
    conv = r["rating"]
    is_index = r.get("is_index", False)
    out = {"ok": True, "tk": tk, "name": r.get("name", tk), "price": price, "industry": r.get("industry"),
           "long_plausible": plausible, "conviction": conv, "horizon": horizon, "is_index": is_index,
           "route": r.get("route"), "rationale": r.get("rationale", []), "signals": r.get("signals", {}),
           "verdict": (f"INDEX — the hold decision is WHICH EXPIRY (theta), not buy-&-hold · trend {conv}/100 → see the Expiry Matrix"
                       if is_index else f"LONG PLAUSIBLE — {horizon} ({conv}/100)" if plausible
                       else f"LONG NOT ADVISED — {horizon} ({conv}/100)"),
           "asof": dt.datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")}

    # ---- cross-engine ALL-SIDES read (the evidence: every engine's read + the conflicts made explicit) ----
    try:
        import marleg_analyze as az
        a = az.analyze(tk)
        if a.get("ok"):
            out["net"] = a.get("net")
            out["bull"] = a.get("bull", [])
            out["caution"] = a.get("caution", [])
            out["tensions"] = a.get("tensions", [])
            out["engines"] = a.get("engines", {})
            out["chg"] = a.get("chg")
            if a.get("price"):
                out["price"] = a["price"]                       # prefer live price over the panel close
    except Exception:
        pass

    # ---- targets + timeline + the cash/equity buy-sell calendar ----
    if r.get("ideal_hold_days"):
        lo, hi = r["ideal_hold_days"]
        today = dt.date.today()
        review = _add_sessions(today, lo)
        sell_by = _add_sessions(today, hi)
        out.update({"ideal_hold_days": [lo, hi], "ideal_hold_label": r.get("ideal_hold_label"),
                    "target": r.get("target"), "payout_pct": r.get("ideal_payout_pct"),
                    "stop": r.get("stop"), "stop_pct": r.get("stop_pct"), "rr": r.get("rr"),
                    "plan": {
                        "buy_by": today.isoformat(),
                        "review_from": review.isoformat(),
                        "sell_by": sell_by.isoformat(),
                        "hold_sessions": [lo, hi],
                        "sell_rules": [f"book if it hits the target ₹{r.get('target')} (or touches your strike)",
                                       f"cut if it breaks the stop ₹{r.get('stop')} (-{r.get('stop_pct')}%)",
                                       f"time-stop: if neither by {sell_by.isoformat()}, exit anyway — don't let it rot"]}})

    # ---- option leg (only if F&O-optionable) ----
    if mom.has_options(tk):
        import marleg_opt_value as ov
        hold_hi = (r.get("ideal_hold_days") or [5, 10])[1]
        hold_cal = math.ceil(hold_hi * 1.4)                        # sessions → ~calendar days
        exps = _monthly_expiries(5)
        if expiry:
            chosen = dt.date.fromisoformat(expiry)
        else:
            # first expiry that covers the hold + a 12-day theta buffer; else the longest available
            chosen = next((e for e in exps if (e - dt.date.today()).days >= hold_cal + 12), exps[-1])
        d2e = (chosen - dt.date.today()).days
        tgt = r.get("target") if plausible else None
        opt = ov.optimize_otm_calls(tk, expiry=chosen.isoformat(), target=tgt)
        rec = opt.get("recommended") if isinstance(opt, dict) else None
        # the option can NEVER be sold after it expires — clamp the hold-based exit to the option's life.
        eq_sell = out.get("plan", {}).get("sell_by")
        eq_sell_d = dt.date.fromisoformat(eq_sell) if eq_sell else chosen
        hard_exit = _add_sessions(dt.date.today(), max(_sessions_until(chosen) - 10, 1))   # ~10 sessions pre-expiry
        if hard_exit >= chosen:                                    # safety: keep it before expiry
            hard_exit = max(chosen - dt.timedelta(days=2), dt.date.today())
        opt_sell = min(eq_sell_d, hard_exit)                       # sell by the EARLIER of hold-exit / theta-cliff
        too_short = chosen <= eq_sell_d                            # expiry lands on/before the planned exit
        if not plausible:
            note = "Direction isn't plausible right now — no call to time. Revisit when the setup turns."
        elif too_short:
            note = (f"⚠ The {chosen.isoformat()} expiry ({d2e}d) is SHORTER than your ~{hold_cal}-day hold — this "
                    f"option expires BEFORE the swing completes (and bleeds theta first). Roll to the next monthly. "
                    f"If you trade it anyway you MUST be out by {hard_exit.isoformat()}, not your equity exit.")
        else:
            note = (f"Use the {chosen.isoformat()} expiry ({d2e}d) — it clears your ~{hold_cal}-day hold with a theta "
                    f"buffer. Sell on the target/touch, else by {opt_sell.isoformat()}; be OUT by "
                    f"{hard_exit.isoformat()} (≈10 sessions pre-expiry) to dodge the theta cliff.")
        out["option"] = {
            "optionable": True, "expiry": chosen.isoformat(), "days_to_expiry": d2e,
            "expiries": [e.isoformat() for e in exps],
            "recommended": rec, "view": opt.get("view") if isinstance(opt, dict) else None,
            "source": opt.get("source") if isinstance(opt, dict) else None,
            "buy_call": (f"{int(rec['strike'])} CE @ ~₹{rec['cost']} (P→ATM {rec['p_touch']}%, "
                         f"{rec['leverage']}× leverage)" if rec else None),
            "option_plan": {
                "buy_by": dt.date.today().isoformat(),
                "sell_by": opt_sell.isoformat(),
                "hard_exit_by": hard_exit.isoformat(),
                "too_short": too_short,
                "note": note},
        }
        if rec and plausible:
            det = _option_detail(tk, rec["strike"], "C", chosen, out.get("chg"))
            if det:
                out["option"]["detail"] = det
            rvv = opt.get("realized_vol")
            if rvv and rec.get("mkt_iv"):
                gap = rec["mkt_iv"] - rvv
                out["option"]["vol_read"] = {"iv": rec["mkt_iv"], "realized": rvv,
                    "verdict": ("RICH — IV>RV, you're paying up for vol" if gap > 4
                                else "CHEAP — IV<RV, vol on sale" if gap < -4 else "FAIR — IV≈RV")}
        if not plausible:
            out["option"]["recommended"] = None
    else:
        out["option"] = {"optionable": False,
                         "note": f"{tk} has no F&O — cash equity only; use the buy/sell plan above."}
    return out


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "MANAPPURAM"
    exp = sys.argv[2] if len(sys.argv) > 2 else None
    d = deck(tk, exp)
    if not d.get("ok"):
        print(d.get("error")); sys.exit()
    print(f"\n═══ OPTION TRADE DECK · {d['name']} ({d['tk']}) ₹{d['price']} · {d['industry']} ═══")
    net = d.get("net") or {}
    print(f"  {d['verdict']}   ·   all-sides: {net.get('label', '—')}   [{d['asof']}]")
    s = d.get("signals") or {}
    if s:
        print(f"  signals: {s.get('trend')} · fib {s.get('fib120')} · RSI {s.get('rsi')} · ATR% {s.get('atr_pct')} · "
              f"U/D {s.get('volume_ud')} · industry {'LEADING' if s.get('industry_leading') else 'not leading'}"
              f"{(' β' + str(s.get('beta'))) if s.get('beta') else ''} · {'GATED' if s.get('gated') else 'not gated'}")
    for x in d.get("bull", []):
        print(f"    ✅ {x}")
    for x in d.get("caution", []):
        print(f"    ⚠  {x}")
    for x in d.get("tensions", []):
        print(f"    ⚡ {x}")
    if d.get("target"):
        print(f"\n  TIMELINE  {d['ideal_hold_label']}")
        print(f"  TARGETS   target ₹{d['target']} (+{d['payout_pct']}%) · stop ₹{d['stop']} (-{d['stop_pct']}%) · R:R {d['rr']}")
        p = d["plan"]
        print(f"  CALENDAR  buy by {p['buy_by']} → review from {p['review_from']} → sell by {p['sell_by']}")
        for s in p["sell_rules"]:
            print(f"            – {s}")
    o = d.get("option", {})
    if o.get("optionable"):
        print(f"\n  OPTION    expiry {o['expiry']} ({o['days_to_expiry']}d)")
        if o.get("buy_call"):
            print(f"            BUY  {o['buy_call']}")
            det = o.get("detail")
            if det:
                print(f"            book: {det['liquidity']}")
                print(f"            OI:   {det['oi_signal']}")
            vr = o.get("vol_read")
            if vr:
                print(f"            vol:  {vr['verdict']} (IV {vr['iv']}% vs RV {vr['realized']}%)")
            op = o["option_plan"]
            print(f"            SELL by {op['sell_by']} · HARD EXIT by {op['hard_exit_by']}")
            print(f"            {op['note']}")
        else:
            print(f"            {o['option_plan']['note']}")
    else:
        print(f"\n  OPTION    {o.get('note')}")
    print("\n  Read-only planning — proposes dates; never places an order. Decision-support, not advice.")
