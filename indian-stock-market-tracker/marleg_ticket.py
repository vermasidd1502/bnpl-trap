"""
marleg_ticket.py — the full-TICKET / suggestion brain every notification card + dashboard tile renders.

Per the agreed spec:
  • horizon-TAGGED per signal — which of F&O / SWING / POSITIONAL it fits (primary + all that apply)
  • FULL ticket — entry / stop / targets / qty sized at a fixed risk % (default 2%, your 1-3% band) / R:R / levels
  • BOTH instruments — the underlying (cash/MTF) AND a best-effort liquid option — plus the SYSTEM'S pick
  • a CONVICTION score (noise-adjusted, from the notify engine)

It composes existing engines (cockpit = the underlying ticket, option_levels = the option's sell-target).
READ-ONLY: it hands you tickets; you place them on Groww. Real data only.
"""
import time
import datetime as dt

import marleg_cockpit as cp
import marleg_notify_engine as ne
import marleg_panel_build as pb
import marleg_option_levels as ol
import marleg_vol as mv

_U = {"t": 0, "s": None}


def _ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")


def _ustats():
    if _U["s"] and time.time() - _U["t"] < 3600:
        return _U["s"]
    P = pb.load()
    close, vol = P["close"], P["volume"]
    rets = close.pct_change()
    s = {"close": close, "vol": vol, "vol20": rets.tail(20).std(),
         "liq_rank": (close * vol).tail(60).median().rank(pct=True),
         "vol_rank": rets.tail(20).std().rank(pct=True),
         "sma50": close.tail(50).mean(), "sma200": close.tail(200).mean()}
    _U.update({"t": time.time(), "s": s})
    return s


def _conviction(sym):
    try:
        st = _ustats()
        ser = st["close"][sym].dropna()
        if len(ser) < 6:
            return None
        m1 = ser.iloc[-1] / ser.iloc[-2] - 1
        vser = st["vol"][sym]
        vs = float(vser.iloc[-1] / (vser.tail(20).mean() or 1))
        sc, tier, factors, nm = ne.conviction(
            m1, float(st["vol20"].get(sym) or 0.02), vs,
            bool(ser.iloc[-1] > (st["sma50"].get(sym) or ser.iloc[-1])),
            float(st["liq_rank"].get(sym) or 0.5), float(st["vol_rank"].get(sym) or 0.5))
        return {"score": sc, "tier": tier, "factors": factors, "move1d_pct": round(m1 * 100, 1)}
    except Exception:
        return None


def _horizon(sym, ck):
    try:
        st = _ustats()
        ser = st["close"][sym].dropna()
        last = float(ser.iloc[-1])
        s50, s200 = st["sma50"].get(sym), st["sma200"].get(sym)
        atr_pct = ck["atr"] / last * 100
        fits = []
        if s200 and last > s200 and s50 and last > s50:
            fits.append("POSITIONAL")        # established uptrend = holdable
        fits.append("SWING")                  # the validated default vehicle
        if atr_pct >= 3.0 and float(st["liq_rank"].get(sym) or 0) > 0.6:
            fits.append("F&O")                # volatile + liquid = tradable short-term
        return {"primary": "SWING" if "SWING" in fits else fits[0], "fits": fits, "atr_pct": round(atr_pct, 1)}
    except Exception:
        return {"primary": "SWING", "fits": ["SWING"], "atr_pct": None}


def _next_monthly():
    today = dt.date.today()
    exp = mv.monthly_expiry(today.year, today.month)
    if (exp - today).days < 12:               # too little runway → roll to next month
        y, m = (today.year + 1, 1) if today.month == 12 else (today.year, today.month + 1)
        exp = mv.monthly_expiry(y, m)
    return exp


def _option_side(sym, ck, capital, risk_pct):
    try:
        spot = ck["spot"]; long = ck.get("side", "long").lower() == "long"
        step = 10 if spot < 500 else 20 if spot < 1000 else 50 if spot < 2500 else 100
        strike = int(round(spot / step) * step)
        exp = _next_monthly()
        osym = f"{sym}{exp.strftime('%y')}{exp.strftime('%b').upper()}{strike}{'CE' if long else 'PE'}"
        sig = ol.signal(osym)
        if not sig.get("ok"):
            return {"available": False, "note": "no liquid ATM monthly quote for this name"}
        prem = sig["premium"]; tgt = sig.get("sell_target") or {}
        budget = capital * risk_pct / 100.0
        return {"available": True, "symbol": osym, "strike": strike, "expiry": exp.isoformat(),
                "premium": prem, "iv": sig.get("iv"), "sell_target": tgt,
                "max_premium_budget": round(budget),
                "defined_risk_note": f"max loss = premium paid; keep total premium ≤ ₹{round(budget):,} "
                f"({risk_pct}% of capital). Exit on the level-high via the guardian, never to expiry."}
    except Exception as e:
        return {"available": False, "note": "option side unavailable: " + str(e)[:70]}


def _recommend(ck, conv, opt, hz):
    if conv and conv["tier"] == "HIGH" and "F&O" in hz["fits"] and opt.get("available"):
        return {"pick": "OPTION", "why": "high-conviction + fast + liquid option → defined-risk convexity; "
                "small size, exit on the level-high. The rare case where the option beats the underlying."}
    return {"pick": "UNDERLYING", "why": "swing setup → cash/MTF is linear (no theta, no expiry, no round-trip-to-zero) "
            "— the validated vehicle. An option here just adds decay risk for no edge."}


def suggest(sym, capital=100000.0, risk_pct=2.0):
    sym = (sym or "").upper().strip()
    ck = cp.cockpit(sym, capital=capital, risk_pct=risk_pct)
    if not ck.get("ok"):
        return {"ok": False, "sym": sym, "error": ck.get("error", "no cockpit data")}
    conv = _conviction(sym)
    hz = _horizon(sym, ck)
    opt = _option_side(sym, ck, capital, risk_pct)
    rec = _recommend(ck, conv, opt, hz)
    return {
        "ok": True, "sym": sym, "asof": _ist(), "spot": ck["spot"], "capital": capital, "risk_pct": risk_pct,
        "conviction": conv, "horizon": hz,
        "underlying": {"entry": ck["entry"], "stop": ck["stop"], "qty": ck["qty"], "risk_rupees": ck["risk_rupees"],
                       "blended_rr": ck["blended_rr"], "targets": ck["tps"], "tickets": ck["tickets"],
                       "levels": ck.get("levels"), "be_rule": ck.get("be_rule")},
        "option": opt, "recommendation": rec,
        "caveat": "READ-ONLY full ticket sized at " + str(risk_pct) + "% risk; underlying qty scales with your capital "
                  "(shown for ₹" + format(int(capital), ",") + "). You place every order on Groww. Real data only.",
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for s in (sys.argv[1:] or ["RELIANCE"]):
        r = suggest(s)
        if not r.get("ok"):
            print(f"  {s}: {r.get('error')}"); continue
        c = r["conviction"] or {}
        print(f"\n═══ {r['sym']}  spot ₹{r['spot']}  ·  conviction {c.get('score','?')} ({c.get('tier','?')})  "
              f"·  horizon {r['horizon']['primary']} {r['horizon']['fits']} ═══")
        u = r["underlying"]
        print(f"  UNDERLYING: entry {u['entry']} · stop {u['stop']} · qty {u['qty']} (risk ₹{u['risk_rupees']:,}) · R:R {u['blended_rr']}")
        print(f"     targets: " + " | ".join(f"TP{t['n']} {t['px']} ({t['R']}R)" for t in u["targets"]))
        o = r["option"]
        if o.get("available"):
            t = o["sell_target"] or {}
            print(f"  OPTION: {o['symbol']} ~₹{o['premium']} (IV {o['iv']}%) · budget ≤ ₹{o['max_premium_budget']:,} · "
                  f"sell-tgt {t.get('which','?')} → ₹{t.get('opt_value','?')} ({t.get('opt_ret_pct','?')}%)")
        else:
            print(f"  OPTION: {o.get('note')}")
        print(f"  ➤ SYSTEM PICK: {r['recommendation']['pick']} — {r['recommendation']['why']}")
