"""
marleg_cockpit.py — the intraday TRADE COCKPIT: turn an idea into a fully-managed plan.

Given a symbol (+ side / capital / risk%), it computes:
  • SMART STOP — structure (just past the nearest support/resistance) or ATR, whichever is the sane tighter level
  • RISK-based POSITION SIZE — capital x risk% / per-unit risk (so every trade risks the same rupees)
  • a SCALED take-profit LADDER — TP1/TP2/TP3 at R-multiples, each anchored to the nearest real level, with %-off
  • the LIQUIDITY map — prior-day H/L, pivots, 20-day swing H/L (where stops pool / price reacts)
  • GTT-ready ORDER TICKETS + ALERT lines for every trigger, and the "after TP1 -> stop to breakeven" rule

READ-ONLY / decision-support. The pod NEVER places, modifies or cancels an order — it hands you the ticket;
YOU place it on Groww (or load it as a GTT). The cockpit is the brain; you are the trigger.

  python marleg_cockpit.py DRREDDY LONG
"""
import datetime as dt
import pandas as pd

import marleg_data as md


def _live(sym):
    out = {}
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        p = (g.quote(sym, segment="CASH", exchange="NSE").json().get("payload") or {})
        lp = p.get("last_price"); oh = p.get("ohlc") if isinstance(p.get("ohlc"), dict) else {}
        if lp is not None:
            out["last"] = round(float(lp), 2)
        for k in ("open", "high", "low"):
            if oh.get(k) is not None:
                out[k] = round(float(oh[k]), 2)
        out["prev_close"] = oh.get("close")
    except Exception:
        pass
    return out


def _round_tick(x):
    return round(float(x), 1)


def cockpit(sym, side="LONG", entry=None, capital=100000.0, risk_pct=1.0, tp_split=(40, 30, 30), profile="normal"):
    sym = (sym or "").upper().strip(); side = (side or "LONG").upper()
    daily = md.candles(sym, 1440, 60)
    if daily is None or len(daily) < 20:
        return {"ok": False, "sym": sym, "error": "insufficient history"}
    c = daily["close"].dropna(); h = daily["high"]; l = daily["low"]
    live = _live(sym)
    spot = live.get("last") or float(c.iloc[-1])
    entry = float(entry) if entry else spot

    # ATR(14)
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(14).mean().iloc[-1])

    # levels (the liquidity map)
    prior = daily.iloc[-2]
    ph, pl, pc = float(prior["high"]), float(prior["low"]), float(prior["close"])
    PP = (ph + pl + pc) / 3.0
    R1 = 2 * PP - pl; R2 = PP + (ph - pl); S1 = 2 * PP - ph; S2 = PP - (ph - pl)
    R3 = ph + 2 * (PP - pl); S3 = pl - 2 * (ph - PP)                 # Classic R3/S3 for the full S&R ladder
    sw_hi = float(h.iloc[-20:].max()); sw_lo = float(l.iloc[-20:].min())
    levels = {"PP": _round_tick(PP), "R1": _round_tick(R1), "R2": _round_tick(R2), "R3": _round_tick(R3),
              "S1": _round_tick(S1), "S2": _round_tick(S2), "S3": _round_tick(S3),
              "PDH": _round_tick(ph), "PDL": _round_tick(pl),
              "swing_hi": _round_tick(sw_hi), "swing_lo": _round_tick(sw_lo)}

    long = side == "LONG"
    # STOP PROFILE — backtested: expectancy peaks ~1.5 ATR; tighter than 1 ATR whipsaws (0.5 = NEGATIVE).
    PROF = {"tight": 1.0, "normal": 1.5, "wide": 2.2, "trail": 2.0}
    profile = profile if profile in PROF else "normal"
    m = PROF[profile]
    trail = profile == "trail"
    # stop distance = the profile's m×ATR; then NUDGE to just beyond a structural level near that distance
    # (so it isn't the obvious round-number stop), but the profile is what drives how tight/wide it is.
    if long:
        atr_stop = entry - m * atr
        near = [x for x in (S1, pl, sw_lo, live.get("low")) if x and (atr_stop - 0.5 * atr) <= x <= (atr_stop + 0.3 * atr) and x < entry]
        stop = (min(near) - 0.1 * atr) if near else atr_stop
    else:
        atr_stop = entry + m * atr
        near = [x for x in (R1, ph, sw_hi, live.get("high")) if x and (atr_stop - 0.3 * atr) <= x <= (atr_stop + 0.5 * atr) and x > entry]
        stop = (max(near) + 0.1 * atr) if near else atr_stop
    stop = _round_tick(stop)
    risk_per = abs(entry - stop)
    if risk_per <= 0:
        return {"ok": False, "sym": sym, "error": "degenerate stop"}

    qty = int((capital * risk_pct / 100.0) / risk_per)
    # SCALED TP ladder — R-multiples, each tagged with the nearest real level
    mults = (1.5, 2.5, 4.0)
    above = sorted(x for x in (R1, R2, ph, sw_hi) if x > entry)
    below = sorted((x for x in (S1, S2, pl, sw_lo) if x < entry), reverse=True)
    pool = above if long else below

    def near_level(px):
        if not pool:
            return None
        best = min(pool, key=lambda x: abs(x - px))
        return best if abs(best - px) <= 0.6 * risk_per else None

    # ETA to target — moves chop, so net progress ~0.6 ATR/session (a planning horizon, not a deadline)
    pace = 0.6 * atr

    def _eta(dist):
        if pace <= 0:
            return None
        return max(1, int(dist / pace) + (1 if (dist % pace) > 1e-6 else 0))

    def _add_bdays(n):
        d = dt.date.today(); added = 0
        while n and added < n:
            d += dt.timedelta(days=1)
            if d.weekday() < 5:
                added += 1
        return d

    tps = []
    for i, m in enumerate(mults):
        px = entry + m * risk_per if long else entry - m * risk_per
        lvl = near_level(px)
        eta = _eta(m * risk_per)
        tps.append({"n": i + 1, "px": _round_tick(px), "R": m, "pct": tp_split[i],
                    "near": _round_tick(lvl) if lvl else None,
                    "eta": eta, "eta_date": _add_bdays(eta).strftime("%d %b") if eta else None})
    blended_R = round(sum(t["R"] * t["pct"] for t in tps) / sum(tp_split), 2)
    target_eta = {"sessions": tps[-1]["eta"], "date": tps[-1]["eta_date"], "tp1_sessions": tps[0]["eta"],
                  "tp1_date": tps[0]["eta_date"], "atr_per_day": round(atr, 2),
                  "note": "ETA assumes ~0.6 ATR/session net progress — moves chop, so treat as a planning horizon, not a deadline."}

    # order tickets (GTT-ready; the pod NEVER fires these)
    tt = "BUY" if long else "SELL"
    xt = "SELL" if long else "BUY"
    tickets = [f"{tt} {qty} {sym} @ {entry} (entry)",
               f"{xt} {qty} {sym} SL-trigger {stop}  (stop — risk ₹{round(risk_per*qty):,})"]
    for t in tps:
        tickets.append(f"{xt} {int(qty*t['pct']/100)} {sym} @ {t['px']}  (TP{t['n']} · {t['R']}R · {t['pct']}%)")

    alerts = [f"⛔ STOP hit {stop} → exit all, −1R (₹{round(risk_per*qty):,})",
              f"🎯 TP1 {tps[0]['px']} → trim {tps[0]['pct']}% AND pull stop to breakeven ({entry})",
              f"🎯 TP2 {tps[1]['px']} → trim {tps[1]['pct']}%, trail stop under last swing",
              f"🎯 TP3 {tps[2]['px']} → trim final {tps[2]['pct']}% / let a runner ride"]

    return {
        "ok": True, "sym": sym, "side": side,
        "spot": spot, "entry": _round_tick(entry), "stop": stop, "qty": qty,
        "atr": round(atr, 2), "risk_per_unit": round(risk_per, 2), "risk_rupees": round(risk_per * qty),
        "capital": capital, "risk_pct": risk_pct, "profile": profile, "trail": trail,
        "tps": tps, "blended_rr": blended_R, "tp_split": list(tp_split), "target_eta": target_eta,
        "levels": levels, "tickets": tickets, "alerts": alerts,
        "be_rule": f"after TP1 ({tps[0]['px']}) → move stop to breakeven {_round_tick(entry)} (free trade from here)",
        "trail_note": (f"CHANDELIER trail: ratchet stop to (highest price since entry) − {m}×ATR (~₹{round(m*atr,1)}); "
                       "moves up only, never down — locks gains as it runs." if trail else None),
        "stop_note": f"profile «{profile}» = {m}×ATR. Backtested (intraday momentum, 2h hold): expectancy PEAKS ~1.5 ATR; "
                     "tighter than 1 ATR whipsaws (0.5 ATR = NEGATIVE, 67% stopped). Wider lifts win% but caps expectancy.",
        "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
        "note": "Smart-stop = a hair beyond the nearest support/resistance (bounded 0.8–2.5 ATR); size risks a "
                "fixed % of capital; TPs scale out at R-multiples. Liquidity = the levels where stops pool.",
        "caveat": "READ-ONLY decision-support — the pod hands you the tickets; YOU place them on Groww (or as GTT). "
                  "It never auto-executes. Entry edge is your call; this manages the trade once you're in.",
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sym = sys.argv[1] if len(sys.argv) > 1 else "DRREDDY"
    side = sys.argv[2] if len(sys.argv) > 2 else "LONG"
    r = cockpit(sym, side)
    if not r.get("ok"):
        print(r); raise SystemExit
    print(f"\n  {r['sym']} {r['side']} · spot {r['spot']} · ATR {r['atr']}")
    print(f"  ENTRY {r['entry']} · STOP {r['stop']} (risk {r['risk_per_unit']}/u) · QTY {r['qty']} · risk ₹{r['risk_rupees']:,} ({r['risk_pct']}% of ₹{int(r['capital']):,})")
    for t in r["tps"]:
        print(f"   TP{t['n']}: {t['px']} ({t['R']}R, take {t['pct']}%)" + (f" ~near {t['near']}" if t["near"] else ""))
    print(f"  blended R:R ≈ {r['blended_rr']} · {r['be_rule']}")
    print("  TICKETS:");  [print("    " + x) for x in r["tickets"]]
