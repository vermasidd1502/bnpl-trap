"""
marleg_option_suggest.py — LIQUID-ONLY option suggestions.

The user trades options only on highly-liquid F&O (+ the indices). MANAPPURAM showed why: its chain
prints exist but the strikes are near-untraded (OI 15-35) — you can't get filled. So this scanner:

  1. restricts the universe to the most option-liquid NSE F&O underlyings (HIGHLY_LIQUID) + NIFTY,
  2. finds the LONG-plausible ones cheaply (horizon.rate — already folds in trend/gated/cup/leadership),
  3. for the top few, prices the OTM chain and picks the best REACHABLE strike with REAL OI (≥ min_oi)
     and a tight enough spread — i.e. an option you can actually enter and exit.

A name with a great setup but no liquid strike is reported as "no liquid strike" rather than suggested.
Read-only — decision-support, not advice.

  python marleg_option_suggest.py            # scan, top 4
  python marleg_option_suggest.py 6 300      # top 6, require OI >= 300
"""
import sys

# The most option-liquid NSE F&O underlyings (by typical option OI / turnover). Indices added separately.
HIGHLY_LIQUID = {
    "RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "INFY", "TCS", "AXISBANK", "KOTAKBANK", "BHARTIARTL",
    "ITC", "LT", "BAJFINANCE", "TATAMOTORS", "TATASTEEL", "HINDALCO", "MARUTI", "SUNPHARMA", "ADANIENT",
    "ADANIPORTS", "TITAN", "HCLTECH", "WIPRO", "ONGC", "NTPC", "POWERGRID", "COALINDIA", "JSWSTEEL",
    "M&M", "BAJAJFINSV", "VEDL", "DLF", "TRENT", "BEL", "HAL",
}


INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY"]    # "the main ones" — NSE index options
_INDEX_YF = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS"}


def _index_dir(idx):
    """Quick directional read on an index: price vs 20/50-DMA + 10-day momentum → BULL/BEAR/NEUTRAL."""
    try:
        import yfinance as yf
        h = yf.Ticker(_INDEX_YF.get(idx, "^NSEI")).history(period="4mo")["Close"].dropna()
    except Exception:
        h = None
    if h is None or len(h) < 50:
        return {"dir": "NEUTRAL", "score": 0, "reasons": ["no history"]}
    price = float(h.iloc[-1]); s20 = float(h.rolling(20).mean().iloc[-1]); s50 = float(h.rolling(50).mean().iloc[-1])
    mom10 = float(h.iloc[-1] / h.iloc[-11] - 1) * 100 if len(h) > 11 else 0.0
    score = (1 if price > s20 else -1) + (1 if price > s50 else -1) + (1 if mom10 > 0 else -1)
    reasons = [f"{'>' if price > s20 else '<'}20DMA", f"{'>' if price > s50 else '<'}50DMA", f"10d {mom10:+.1f}%"]
    if idx == "NIFTY":                          # fold in the breadth/VIX conscience for NIFTY
        try:
            import marleg_conscience as cs
            b = (cs.build() or {}).get("bias", {})
            if b.get("label"):
                reasons.append(f"conscience {b['label']}")
                score += 1 if "BULL" in b["label"].upper() else -1 if "BEAR" in b["label"].upper() else 0
        except Exception:
            pass
    d = "BULLISH" if score >= 2 else "BEARISH" if score <= -2 else "NEUTRAL"
    return {"dir": d, "score": score, "reasons": reasons}


INDEX_LABEL = {"NIFTY": "NIFTY 50", "BANKNIFTY": "BANK NIFTY", "FINNIFTY": "FIN NIFTY"}


def index_suggest(min_oi=50):
    """The MAIN options — NIFTY 50 / BANK NIFTY / FIN NIFTY. Read direction, then pick the nearest liquid
    OTM CALL (bullish) or PUT (bearish) at near-month. Buying a directional index option needs a direction."""
    import datetime as dt
    import marleg_options_monitor as mom
    import marleg_vol as mv
    g = mom._g()
    exp = mom.nearest_monthly_expiry()
    days = max((exp - dt.date.today()).days, 1)
    T = days / 365.0
    # PASS 1: resolve every index's spot/direction/vol FIRST (light calls) so the option-quote burst
    # below doesn't throttle the later indices' spot fetch (that was dropping FINNIFTY).
    metas = []
    for idx in INDICES:
        metas.append((idx, mv.underlying_ltp(idx, g), _index_dir(idx), mv.realized_vol(idx, 20) or 0.15))
    out = []
    for idx, spot, dd, rv in metas:                    # PASS 2: quote strikes for the directional ones
        if not spot:
            continue
        step = mom.INDEX_STEP.get(idx, 50)
        atm = round(spot / step) * step
        sigma_move = spot * rv * (T ** 0.5)
        smp = round(sigma_move / spot * 100, 1)
        kind = "CALL" if dd["dir"] == "BULLISH" else "PUT" if dd["dir"] == "BEARISH" else None
        pick = None
        if kind:
            kc = "C" if kind == "CALL" else "P"
            cands = [atm + step * i for i in (1, 2, 3)] if kind == "CALL" else [atm - step * i for i in (1, 2, 3)]
            best = None
            for K in cands:                            # quote only the nearest ~3 OTM strikes (throttle-safe)
                if (kind == "CALL" and K <= spot) or (kind == "PUT" and K >= spot):
                    continue
                q = mom.option_quote(mom.build_symbol(idx, K, kc, exp))
                if not (isinstance(q, dict) and "error" not in q and q.get("ltp")):
                    continue
                ivv = q.get("iv") or rv
                gk = mv.greeks(spot, K, T, mv.R_FREE, ivv, kc)
                ltp = q["ltp"]; be = (K + ltp) if kind == "CALL" else (K - ltp)
                tgt_spot = spot + sigma_move if kind == "CALL" else spot - sigma_move      # ~1σ directional target
                tgt_val = mv.bs_price(tgt_spot, K, T / 2, mv.R_FREE, ivv, kc)               # option value there, ~half time
                rsk = _opt_risk(abs(gk["delta"]) * 100, abs(gk["theta"]) / ltp, (be / spot - 1) * 100,
                                sigma_move / spot * 100, days, q.get("oi"), q.get("spread_pct"))
                cp = {"kind": kind, "strike": K, "premium": ltp, "oi": q.get("oi"), "risk": rsk,
                      "iv": round(ivv * 100, 1), "delta": round(gk["delta"], 3), "theta": round(gk["theta"], 2),
                      "be": round(be, 1), "be_move_pct": round((be / spot - 1) * 100, 1),
                      "target": round(tgt_val, 2), "target_pct": round((tgt_val / ltp - 1) * 100, 0),
                      "target_spot": round(tgt_spot, 0), "src": "live"}
                if best is None:
                    best = cp
                if (q.get("oi") or 0) >= min_oi:
                    best = cp; break
            pick = best
        vol_read = None
        if pick:
            gap = pick["iv"] - rv * 100
            vol_read = {"iv": pick["iv"], "realized": round(rv * 100, 1),
                        "verdict": "RICH" if gap > 3 else "CHEAP" if gap < -3 else "FAIR"}
        reachable = (pick is not None and abs(pick["be_move_pct"]) <= smp)
        out.append({"idx": idx, "label": INDEX_LABEL.get(idx, idx), "spot": round(spot, 1),
                    "direction": dd["dir"], "dir_reasons": dd["reasons"], "expiry": exp.isoformat(), "days": days,
                    "sigma_move_pct": smp, "iv": round(rv * 100, 1), "pick": pick, "vol_read": vol_read,
                    "reachable": reachable})
    return out


def _opt_risk(p_profit_pct, theta_drag_frac, be_move_pct, exp_move_pct, days, oi, spread_pct):
    """Risk meter for an option BUYER (0-100, higher = riskier). Speculation is fine, but you should SEE
    the risk: chance it expires worthless, theta bleed, how big a move is needed, time left, liquidity."""
    c = lambda x: max(0.0, min(1.0, x))
    loss = c(1 - (p_profit_pct or 0) / 100.0)                                  # P(doesn't reach target/ITM)
    decay = c((theta_drag_frac or 0) / 0.04)                                    # 4%/day decay = maxed
    sig = abs(be_move_pct) / exp_move_pct if (exp_move_pct and exp_move_pct > 0) else 1.5
    money = c(sig / 2.5)                                                        # breakeven 2.5σ away = maxed
    time = c((15 - days) / 15.0) if days < 15 else 0.0                          # <15d ramps the theta-cliff risk
    liq = 1.0 if (oi or 0) < 300 else 0.5 if (oi or 0) < 1000 else 0.1
    if spread_pct:
        liq = max(liq, c(spread_pct / 8.0))
    score = round(100 * (0.34 * loss + 0.24 * decay + 0.16 * money + 0.12 * time + 0.14 * liq))
    drivers = {"worthless-risk": round(loss * 100), "theta-bleed": round(decay * 100),
               "move-needed": round(money * 100), "time-left": round(time * 100), "liquidity": round(liq * 100)}
    main = max(drivers, key=drivers.get)
    main_lbl = {"worthless-risk": "high chance it expires worthless", "theta-bleed": "fast theta bleed",
                "move-needed": "needs a big move to pay", "time-left": "little time left (theta cliff)",
                "liquidity": "thin OI — hard to exit"}[main]
    level = "EXTREME" if score >= 78 else "HIGH" if score >= 58 else "MODERATE" if score >= 35 else "LOW"
    return {"score": score, "level": level, "main": main_lbl, "drivers": drivers}


def _stock_pick(tk, spot, target, exp, days, min_oi, max_spread):
    """Directly quote the reachable OTM call strikes (just-OTM up to target) for ONE stock — ~3 Groww
    calls, NOT a full 34-strike chain — and return the nearest LIQUID one with ride metrics. This is what
    makes the multi-name scan throttle-safe (a full chain per stock was choking the later names)."""
    import math
    import marleg_options_monitor as mom
    import marleg_vol as mv
    import marleg_opt_value as ov
    T = max(days, 1) / 365.0
    r = mv.R_FREE
    step = mom._strike_step(spot)
    atm = round(spot / step) * step
    mu = math.log(max(target, spot * 1.001) / spot) / T          # drift implied by the (scaled) target
    rv = mv.realized_vol(tk, 20) or 0.30
    cands, k = [], (atm if atm > spot else atm + step)
    while k <= target * 1.0001 and len(cands) < 5:               # reachable OTM strikes only
        if k > spot:
            cands.append(k)
        k += step
    if not cands:
        cands = [atm + step]
    liquid = []
    for K in cands:                                              # quote each reachable OTM strike
        try:
            q = mom.option_quote(mom.build_symbol(tk, K, "C", exp))
        except Exception:
            continue
        if not (isinstance(q, dict) and "error" not in q and q.get("ltp")):
            continue
        ltp = q["ltp"]; oi = q.get("oi"); sp = q.get("spread_pct")
        if (oi or 0) < min_oi or (sp is not None and sp > max_spread):
            continue                                             # keep only strikes you can actually trade
        iv = q.get("iv") or rv
        gk = mv.greeks(spot, K, T, r, iv, "C")
        atm_val = mv.bs_price(K, K, T / 2, r, iv, "C")           # the call's value when spot touches K (½ time left)
        p_touch = ov.prob_touch(spot, K, T, iv, mu)
        rsk = _opt_risk(p_touch * 100, -gk["theta"] / ltp, ((K + ltp) / spot - 1) * 100,
                        (target / spot - 1) * 100, days, oi, sp)
        liquid.append({"strike": K, "cost": round(ltp, 2), "oi": oi, "spread_pct": sp, "mkt_iv": round(iv * 100, 1),
                       "risk": rsk, "delta": round(gk["delta"], 3), "p_touch": round(p_touch * 100, 1),
                       "target": round(atm_val, 2), "target_pct": round((atm_val / ltp - 1) * 100, 0),
                       "target_spot": round(K, 1),                  # sell the call when spot reaches the strike (it's ATM)
                       "ret_at_touch": round((atm_val / ltp - 1) * 100, 0),
                       "ev_ride": round((p_touch * atm_val / ltp - 1) * 100, 0),       # ride-or-bust EV
                       "be_move_pct": round(((K + ltp) / spot - 1) * 100, 1),
                       "leverage": round(gk["delta"] * spot / ltp, 1),
                       "theta_drag": round(-gk["theta"] / ltp * 100, 2),
                       "edge": round(mv.bs_price(spot, K, T, r, rv, "C") - ltp, 2), "src": q.get("src", "live")})
    best = max(liquid, key=lambda c: c["ev_ride"]) if liquid else None
    # only suggest if the ride actually pays — a barely-OTM strike whose ATM value < premium (negative
    # ev_ride) isn't worth buying even if liquid; that's a "cash only", not an option trade.
    return best if (best and best["ev_ride"] > 0) else None


def suggest(top=4, min_oi=200, min_rating=62, max_spread=6.0):
    import marleg_horizon as hz
    import marleg_opt_value as ov
    import marleg_options_monitor as mom

    idx_sug = index_suggest()                    # THE MAIN ONES first, on a fresh (un-throttled) connection

    # 1) cheap plausibility pass over the liquid universe (panel-cached) -> rank by conviction
    cand = []
    for tk in sorted(HIGHLY_LIQUID):
        if not mom.has_options(tk):
            continue
        try:
            r = hz.rate(tk)
        except Exception:
            continue
        if not r.get("ok") or r["horizon"].startswith("AVOID") or not r.get("target"):
            continue
        if (r.get("rating") or 0) < min_rating:
            continue
        cand.append(r)
    cand.sort(key=lambda r: -(r.get("rating") or 0))
    shortlist = cand[:top]

    # 2) for the shortlist, price the chain at an expiry that actually CLEARS the hold + a theta buffer.
    #    A 2-4wk swing (LT, AXISBANK…) must NOT be bought on a 12-day near-month option — it expires
    #    before the thesis plays out and dies in the theta cliff. So a swing → next-month, not near-month.
    import datetime as dt
    import math
    import marleg_vol as mv
    today = dt.date.today()
    exps, yy, mm = [], today.year, today.month
    while len(exps) < 4:
        e = mv.monthly_expiry(yy, mm)
        if e >= today:
            exps.append(e)
        yy, mm = (yy + (mm == 12)), (1 if mm == 12 else mm + 1)
    out = []
    for r in shortlist:
        tk = r["tk"]
        hold = r.get("ideal_hold_days") or [10, 20]
        hold_cal = max(math.ceil((sum(hold) / 2) * 1.4), 1)          # sessions → ~calendar days
        exp = next((e for e in exps if (e - today).days >= hold_cal + 10), exps[-1])   # hold + theta buffer
        d2e = max((exp - today).days, 1)
        # target time-scaled to the chosen tenor (keeps P→ATM honest for long-horizon names)
        frac = min(1.0, d2e / hold_cal)
        eff_target = round(r["price"] * (1 + (r.get("ideal_payout_pct") or 5) / 100 * frac), 1)
        pick = _stock_pick(tk, r["price"], eff_target, exp, d2e, min_oi, max_spread)
        out.append({
            "tk": tk, "name": r.get("name", tk), "industry": r.get("industry"),
            "horizon": r["horizon"], "rating": r["rating"], "price": r["price"],
            "target": r["target"], "payout_pct": r.get("ideal_payout_pct"),
            "hold": r.get("ideal_hold_label"), "expiry": exp.isoformat(), "days_to_expiry": d2e,
            "pick": pick, "liquid": bool(pick),
        })
    out.sort(key=lambda x: (0 if x["liquid"] else 1, -x["rating"]))
    return {"ok": True, "universe": "highly-liquid F&O", "scanned": len(cand), "shortlisted": len(shortlist),
            "n": len(out), "min_oi": min_oi, "indices": idx_sug, "suggestions": out,
            "note": f"Restricted to the most option-liquid F&O names; each pick is a REACHABLE strike with "
                    f"OI ≥ {min_oi} and spread ≤ {max_spread}% — an option you can actually enter/exit. "
                    f"Near-month expiry (most liquid). Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    top = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    moi = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    r = suggest(top=top, min_oi=moi)
    print(f"\n═══ LIQUID-ONLY OPTION SUGGESTIONS · scanned {r['scanned']} plausible, top {r['shortlisted']} ═══")
    print(f"  {r['note']}\n")
    print("  ── THE MAIN ONES (indices) ──")
    for ix in r.get("indices", []):
        p = ix.get("pick")
        line = f"  {ix['idx']:<10} {ix['direction']:<8} ₹{ix['spot']} · ~1σ ±{ix['sigma_move_pct']}% by {ix['expiry']} · [{', '.join(ix['dir_reasons'])}]"
        print(line)
        if p:
            print(f"       ✅ BUY {int(p['strike'])} {p['kind']} @ ₹{p['premium']} · OI {int(p['oi'])} · IV {p['iv']}% · Δ{p['delta']} · BE {p['be_move_pct']:+.1f}%")
        elif ix["direction"] == "NEUTRAL":
            print("       ⚪ no directional edge — skip a directional buy (don't pay theta for a flat view)")
        else:
            print(f"       ⚪ no liquid OTM strike ≥ OI threshold")
    print()
    for x in r["suggestions"]:
        head = f"  {x['tk']:<11} {x['horizon']:<11} conv {x['rating']}/100 · ₹{x['price']} → ₹{x['target']} (+{x['payout_pct']}%) · {x['hold']}"
        print(head)
        p = x["pick"]
        if p:
            print(f"       ✅ BUY {int(p['strike'])} CE @ ₹{p['cost']} (exp {x['expiry']}, {x['days_to_expiry']}d) · "
                  f"P→ATM {p['p_touch']}% · ret@ATM {p['ret_at_touch']:+.0f}% · OI {int(p['oi'])} · {p['mkt_iv']}% IV")
        else:
            print(f"       ⚪ plausible, but no liquid reachable strike (OI ≥ {r['min_oi']}) — skip the option, cash only")
    if not r["suggestions"]:
        print("  none of the liquid names are long-plausible right now — stand down.")
