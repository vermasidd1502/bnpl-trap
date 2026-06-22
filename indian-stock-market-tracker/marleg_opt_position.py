"""
marleg_opt_position.py — VIABILITY + SCOPE scorecard for an option position you actually hold.

Given a held option (symbol + your avg + qty), this answers two questions with hard numbers:

  IS IT VIABLE?  — does this contract have a real chance to make money FROM HERE, net of the time
                   decay you're fighting? A call can be green on screen and still be a losing hold:
                   the green is time value that converges to zero by expiry. We separate intrinsic
                   (won't decay) from extrinsic (will), price P(finish above YOUR breakeven), and
                   weigh odds × reachability × runway × decay-drag × liquidity × vol into a 0–100 score.

  WHAT'S THE SCOPE? — the realistic upside if your view works: the option's value at +1σ / +2σ moves
                   and at a target, the upside multiple, vs the downside if it just sits flat (the
                   decay floor). Asymmetry = is the reward worth the bleed?

Honest core (FIN 513): under risk-neutral pricing a fairly-priced option's expected return is just rf —
so the headline odds use mu=r (NO assumed edge). A drift only enters if YOU supply a view. Implied vol
is SOLVED from the live premium (Newton), greeks are finite-difference. Read-only — never trades.
Decision-support, not investment advice.

  python marleg_opt_position.py NIFTY26JUN24000CE --avg 202.95 --qty 130
  python marleg_opt_position.py --book          # pull live Groww F&O option positions and score each
"""
import math
import datetime as dt

import marleg_vol as mv
import marleg_options_monitor as mom
import marleg_opt_value as ov

# index realized-vol fallback (mv.realized_vol uses <UND>.NS, which doesn't resolve for indices)
_INDEX_YF = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS",
             "MIDCPNIFTY": "^NSEMDCP50", "NIFTYNXT50": "^NSEI"}


def _realized(und, window=20):
    """20d realized vol — GROWW candles first (no yfinance rate-limits), then index ticker fallback."""
    try:
        import marleg_expiry_matrix as _em
        rv = _em._realized(und, window)              # Groww daily candles
        if rv:
            return rv
    except Exception:
        pass
    rv = mv.realized_vol(und, window)
    if rv:
        return rv
    yfsym = _INDEX_YF.get((und or "").upper())
    if not yfsym:
        return None
    try:
        import yfinance as yf
        import statistics
        h = yf.Ticker(yfsym).history(period="4mo", interval="1d")["Close"].dropna()
        rets = (h / h.shift(1)).apply(lambda x: math.log(x) if x > 0 else 0).dropna()
        if len(rets) < window + 1:
            return None
        return statistics.pstdev(list(rets.iloc[-window:])) * math.sqrt(252)
    except Exception:
        return None


def _trading_days_left(today, expiry):
    """NSE sessions remaining incl. expiry day (drops weekends + known holidays)."""
    n, d = 0, today + dt.timedelta(days=1)
    while d <= expiry:
        if mv._is_trading_day(d):
            n += 1
        d += dt.timedelta(days=1)
    return n


def analyze_position(sym, avg=None, qty=None, view_target=None):
    """Full viability + scope scorecard for one held option. avg = your entry premium, qty = units held."""
    info = mv.parse_option_any(sym)
    if not info:
        return {"ok": False, "symbol": sym, "error": f"could not parse option symbol '{sym}'"}
    und, K, kind, expiry, right = info["underlying"], info["strike"], info["kind"], info["expiry"], info["right"]
    is_call = kind == "C"
    today = dt.date.today()
    dte = (expiry - today).days
    tdl = _trading_days_left(today, expiry)
    T = max(dte, 1) / 365.0
    r = mv.R_FREE

    g = mom._g()
    S = mv.underlying_ltp(und, g)
    if not S:
        return {"ok": False, "symbol": sym, "error": f"no spot for {und}"}
    q = mom.option_quote(sym)
    qerr = q.get("error") if isinstance(q, dict) else "no quote"
    prem = (q.get("ltp") if isinstance(q, dict) else None)
    if not prem and isinstance(q, dict):
        prem = q.get("bid") and q.get("ask") and (q["bid"] + q["ask"]) / 2
    model_px = mv.bs_price(S, K, T, r, (_realized(und) or 0.25), kind)
    premium = prem or model_px
    px_src = "live" if prem else "model"

    # implied vol: prefer Groww's, else SOLVE from the live premium, else realized
    iv = (q.get("iv") if isinstance(q, dict) else None)
    if not iv and prem:
        iv, _diag = mv.implied_vol_newton(prem, S, K, T, r, kind)
    rv = _realized(und)
    iv = iv or rv or 0.25
    vix = mv.india_vix()

    gk = mv.fd_greeks(S, K, T, r, iv, kind)
    delta, theta, vega = gk["delta"], gk["theta"], gk["vega"]

    # ---- moneyness: intrinsic never decays, extrinsic (time value) bleeds to zero by expiry ----
    intrinsic = max(S - K, 0.0) if is_call else max(K - S, 0.0)
    extrinsic = max(premium - intrinsic, 0.0)
    extrinsic_pct = (extrinsic / premium) if premium else 1.0
    if is_call:
        state = "ITM" if S > K * 1.001 else ("ATM" if abs(S - K) <= K * 0.01 else "OTM")
        dist_pct = (K - S) / S * 100        # +ve = OTM (how far up to the strike)
    else:
        state = "ITM" if S < K * 0.999 else ("ATM" if abs(S - K) <= K * 0.01 else "OTM")
        dist_pct = (S - K) / S * 100

    # ---- breakeven (vs YOUR entry, held to expiry) ----
    be = (K + avg) if (avg and is_call) else (K - avg) if (avg and not is_call) else (K + premium if is_call else K - premium)
    be_move_pct = (be / S - 1) * 100
    sigma_move = S * iv * math.sqrt(T)       # 1σ underlying move by expiry (price units)
    sigma_pct = sigma_move / S * 100
    be_within_1sig = abs(be - S) <= sigma_move
    be_sigmas = abs(be - S) / sigma_move if sigma_move else 99

    # ---- odds (risk-neutral, no assumed edge) ----
    if is_call:
        p_touch = ov.prob_touch(S, K, T, iv, r) if S < K else 1.0      # P(reach the strike before expiry)
        p_itm = ov.prob_itm(S, K, T, r, iv)                            # P(finish ITM)
        p_profit = ov.prob_itm(S, be, T, r, iv)                        # P(finish above YOUR breakeven)
    else:
        p_touch = None
        p_itm = 1 - ov.prob_itm(S, K, T, r, iv)
        p_profit = 1 - ov.prob_itm(S, be, T, r, iv)

    # ---- P&L now ----
    pnl_pct = ((premium - avg) / avg * 100) if avg else None
    pnl_abs = ((premium - avg) * qty) if (avg and qty) else None

    # ---- decay floor: if the underlying sits FLAT, the option → intrinsic by expiry ----
    decay_to_expiry_pct = ((intrinsic - premium) / premium * 100) if premium else None   # what flat costs from here
    decay_vs_avg_pct = ((intrinsic - avg) / avg * 100) if avg else None                   # ...vs your entry
    theta_rupees_day = theta * (qty or 0)
    theta_drag_pct = (-theta / premium * 100) if premium else None

    # ---- SCOPE: near-term reprice at ±σ moves (same DTE — "if it gets there soon, then you sell") ----
    def opt_at(spot, t=T):
        return mv.bs_price(spot, K, t, r, iv, kind)
    scen = []
    for lbl, mult in (("−1σ", -1), ("flat", 0), ("+1σ", 1), ("+2σ", 2)):
        sp = S + mult * sigma_move
        val = opt_at(sp)
        scen.append({"label": lbl, "spot": round(sp, 1),
                     "move_pct": round((sp / S - 1) * 100, 2), "opt_value": round(val, 2),
                     "ret_vs_avg_pct": round((val / avg - 1) * 100, 0) if avg else None})
    # value if it expires flat (the decay floor) + a target/view scenario
    floor = {"label": "flat→expiry", "spot": round(S, 1), "opt_value": round(intrinsic, 2),
             "ret_vs_avg_pct": round((intrinsic / avg - 1) * 100, 0) if avg else None}
    target_row = None
    if view_target:
        tv = opt_at(float(view_target))
        target_row = {"label": f"→{view_target}", "spot": float(view_target),
                      "move_pct": round((float(view_target) / S - 1) * 100, 2), "opt_value": round(tv, 2),
                      "ret_vs_avg_pct": round((tv / avg - 1) * 100, 0) if avg else None}
    up_2sig_x = (opt_at(S + 2 * sigma_move) / avg) if avg else None
    down_flat_pct = floor["ret_vs_avg_pct"]

    # ---------------------------------------------------------------- viability score (0–100)
    c = {}
    # odds (0–40): P(finish above your breakeven)
    c["odds"] = round(min(40.0, max(0.0, (p_profit - 0.10) / (0.55 - 0.10) * 40))) if p_profit is not None else 0
    # reachability (0–15): is breakeven within ~1σ?
    c["reach"] = 15 if be_sigmas <= 1 else 9 if be_sigmas <= 1.5 else 4 if be_sigmas <= 2 else 0
    # runway (0–15): sessions left — short-dated is theta-punished
    c["runway"] = 15 if tdl >= 15 else 10 if tdl >= 10 else 6 if tdl >= 6 else 3 if tdl >= 3 else 0
    # decay-drag (0–15): how much of the premium is time value that must bleed
    c["decay"] = 15 if extrinsic_pct <= 0.2 else 10 if extrinsic_pct <= 0.4 else 6 if extrinsic_pct <= 0.6 else 2 if extrinsic_pct <= 0.85 else 0
    # liquidity (0–10): can you actually exit?
    sp_pct = q.get("spread_pct") if isinstance(q, dict) else None
    oi = (q.get("oi") if isinstance(q, dict) else None) or 0
    if sp_pct is not None and sp_pct <= 1.5 and oi >= 1000:
        c["liq"] = 10
    elif sp_pct is not None and sp_pct <= 3:
        c["liq"] = 6
    elif oi >= 500:
        c["liq"] = 4
    else:
        c["liq"] = 1
    # vol (0–5): are you overpaying for vol (vega risk)?
    vrp = (iv - rv) if rv else None
    c["vol"] = (5 if (vrp is None or vrp <= 0.0) else 3 if vrp <= 0.04 else 1) if iv else 3
    score = int(sum(c.values()))

    if tdl <= 2 and state == "OTM" and (p_touch or 0) < 0.10:
        band, score = "DEAD", min(score, 18)
    elif score >= 64:
        band = "VIABLE"
    elif score >= 45:
        band = "MARGINAL"
    elif score >= 25:
        band = "POOR"
    else:
        band = "DEAD"

    reasons = []
    if p_profit is not None:
        reasons.append(("+" if p_profit >= 0.45 else "−") +
                       f" {p_profit*100:.0f}% chance to finish above your breakeven ₹{be:.0f} "
                       f"({'+' if be_move_pct>=0 else ''}{be_move_pct:.1f}% from spot; 1σ by expiry ≈ ±{sigma_pct:.1f}%)")
    reasons.append(("−" if extrinsic_pct > 0.5 else "+") +
                   f" {extrinsic_pct*100:.0f}% of the ₹{premium:.1f} premium is time value — it converges to "
                   f"intrinsic (₹{intrinsic:.0f}) by {expiry.isoformat()} if {und} sits flat")
    reasons.append(("−" if tdl <= 6 else "+") + f" {tdl} trading sessions left (theta ≈ ₹{theta:.1f}/day per unit"
                   + (f", ₹{theta_rupees_day:.0f}/day on your {qty}" if qty else "") + ")")
    if state == "ITM":
        reasons.append(f"• already ITM by ₹{intrinsic:.0f} ({-dist_pct:.1f}%) — intrinsic is a floor, but {extrinsic_pct*100:.0f}% time value still bleeds")
    else:
        reasons.append(f"• {dist_pct:.1f}% OTM" + (f"; P(reach strike before expiry) ≈ {p_touch*100:.0f}%" if p_touch is not None else ""))
    reasons.append(("+" if c["liq"] >= 6 else "−") + f" liquidity: OI {oi:,.0f}, spread {sp_pct if sp_pct is not None else '—'}%")
    if vrp is not None:
        reasons.append(("−" if vrp > 0.04 else "+") + f" IV {iv*100:.0f}% vs realized {rv*100:.0f}% "
                       f"({'rich — overpaying for vol' if vrp>0.04 else 'cheap' if vrp< -0.04 else 'fair'})")

    return {
        "ok": True, "symbol": sym, "underlying": und, "right": right, "strike": K,
        "expiry": expiry.isoformat(), "dte": dte, "trading_days_left": tdl,
        "spot": round(S, 2), "premium": round(premium, 2), "premium_src": px_src,
        "avg": avg, "qty": qty, "quote_error": qerr,
        "moneyness": {"state": state, "dist_pct": round(dist_pct, 2), "intrinsic": round(intrinsic, 2),
                      "extrinsic": round(extrinsic, 2), "extrinsic_pct": round(extrinsic_pct * 100, 1)},
        "pnl": {"pct": round(pnl_pct, 1) if pnl_pct is not None else None,
                "abs": round(pnl_abs, 0) if pnl_abs is not None else None},
        "vol": {"iv_pct": round(iv * 100, 1), "rv_pct": round(rv * 100, 1) if rv else None,
                "iv_vs_rv_pct": round(vrp * 100, 1) if vrp is not None else None,
                "india_vix": round(vix, 1) if vix else None, "vega": round(vega, 3)},
        "greeks": {"delta": round(delta, 3), "gamma": round(gk["gamma"], 5), "theta": round(theta, 2),
                   "vega": round(vega, 3), "theta_rupees_day": round(theta_rupees_day, 0),
                   "theta_drag_pct": round(theta_drag_pct, 2) if theta_drag_pct is not None else None,
                   "pos_delta": round(delta * (qty or 0), 1)},
        "odds": {"p_touch_atm": round(p_touch * 100, 1) if p_touch is not None else None,
                 "p_itm_expiry": round(p_itm * 100, 1), "p_profit_vs_entry": round(p_profit * 100, 1) if p_profit is not None else None,
                 "breakeven": round(be, 1), "breakeven_move_pct": round(be_move_pct, 2),
                 "sigma_move_pts": round(sigma_move, 1), "sigma_move_pct": round(sigma_pct, 2),
                 "be_within_1sigma": be_within_1sig, "be_sigmas": round(be_sigmas, 2)},
        "decay": {"value_if_flat_at_expiry": round(intrinsic, 2),
                  "decay_to_expiry_pct": round(decay_to_expiry_pct, 1) if decay_to_expiry_pct is not None else None,
                  "decay_vs_avg_pct": round(decay_vs_avg_pct, 1) if decay_vs_avg_pct is not None else None},
        "scope": {"scenarios": scen, "floor": floor, "target": target_row,
                  "up_2sigma_x": round(up_2sig_x, 2) if up_2sig_x else None,
                  "down_if_flat_pct": down_flat_pct},
        "liquidity": {"oi": oi, "volume": q.get("volume") if isinstance(q, dict) else None,
                      "spread_pct": sp_pct, "verdict": mom._liquidity_verdict(q) if isinstance(q, dict) and "error" not in q else "⚪ no live quote"},
        "viability": {"score": score, "band": band, "components": c, "reasons": reasons},
        "note": "Odds are risk-neutral (mu=r) — NO assumed edge; a target/view would shift them. IV solved from "
                "the live premium. Intrinsic never decays; extrinsic (time value) goes to zero at expiry. "
                "Scope reprices the SAME contract at ±σ moves (a near-term 'if it gets there, you sell' read).",
        "caveat": "Decision-support, not investment advice — I'm not a licensed advisor. Read-only on your account.",
    }


def _is_option(p):
    ts = (p.get("trading_symbol") or "")
    return (p.get("segment") == "FNO") and (ts.endswith("CE") or ts.endswith("PE")) and mv.parse_option(ts) is not None


def book():
    """Pull live Groww positions, keep the F&O OPTION legs, score each. avg = net_price, qty = quantity."""
    try:
        import groww_client
        g = groww_client.GrowwClient(); g.token()
        pos = (g.positions_data() or {}).get("positions") or []
    except Exception as e:
        return {"ok": False, "error": f"groww positions unavailable: {str(e)[:120]}"}
    def _qty(p):
        return p.get("quantity") or p.get("net_carry_forward_quantity") or 0
    opts = [p for p in pos if _is_option(p) and _qty(p) != 0]   # CURRENT holdings only (drop closed-today)
    closed = sum(1 for p in pos if _is_option(p) and _qty(p) == 0)
    out = []
    for p in opts:
        ts = p["trading_symbol"]
        avg = p.get("net_price") or p.get("credit_price") or None
        qty = _qty(p)
        r = analyze_position(ts, avg=avg, qty=qty)
        r["active"] = True
        r["product"] = p.get("product")
        out.append(r)
    out.sort(key=lambda x: x.get("dte", 999))   # nearest expiry first
    return {"ok": True, "n": len(out), "closed_today": closed, "positions": out,
            "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
            "caveat": "Decision-support, not investment advice — I'm not a licensed advisor. Read-only."}


if __name__ == "__main__":
    import sys, json, argparse
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol", nargs="?")
    ap.add_argument("--avg", type=float)
    ap.add_argument("--qty", type=float)
    ap.add_argument("--target", type=float)
    ap.add_argument("--book", action="store_true")
    a = ap.parse_args()

    def show(r):
        if not r.get("ok"):
            print("  ERR:", r.get("error")); return
        m, o, d, v = r["moneyness"], r["odds"], r["decay"], r["viability"]
        print(f"\n  {r['symbol']}  —  {r['underlying']} spot ₹{r['spot']}  ·  {r['expiry']} ({r['trading_days_left']} sessions)")
        print(f"    {m['state']}  strike ₹{r['strike']:.0f}  ·  premium ₹{r['premium']} ({r['premium_src']})  ·  avg ₹{r['avg']}  ·  "
              f"P&L {('+' if (r['pnl']['pct'] or 0)>=0 else '')}{r['pnl']['pct']}% (₹{r['pnl']['abs']})")
        print(f"    intrinsic ₹{m['intrinsic']} · time-value ₹{m['extrinsic']} ({m['extrinsic_pct']}% of premium)")
        print(f"    breakeven ₹{o['breakeven']} ({'+' if o['breakeven_move_pct']>=0 else ''}{o['breakeven_move_pct']}%)  ·  "
              f"1σ by expiry ±{o['sigma_move_pct']}%  ·  P(beat entry) {o['p_profit_vs_entry']}%  ·  P(finish ITM) {o['p_itm_expiry']}%")
        print(f"    if flat → expiry: ₹{d['value_if_flat_at_expiry']} ({d['decay_vs_avg_pct']}% vs your avg)   [the decay floor]")
        print(f"    SCOPE:  " + "  ".join(f"{s['label']} ₹{s['opt_value']}({'+' if (s['ret_vs_avg_pct'] or 0)>=0 else ''}{s['ret_vs_avg_pct']}%)" for s in r["scope"]["scenarios"]))
        print(f"    IV {r['vol']['iv_pct']}% vs RV {r['vol']['rv_pct']}%  ·  Δ{r['greeks']['delta']} Θ{r['greeks']['theta']}/d (₹{r['greeks']['theta_rupees_day']}/d)  ·  {r['liquidity']['verdict']}")
        print(f"\n    ▶ VIABILITY {v['score']}/100  →  {v['band']}   {v['components']}")
        for ln in v["reasons"]:
            print(f"        {ln}")
        print(f"\n    {r['caveat']}")

    if a.book:
        b = book()
        if not b.get("ok"):
            print("ERR:", b["error"]); sys.exit()
        print(f"\n  ═══ HELD OPTION POSITIONS ({b['n']}) — {b['asof']} ═══")
        for r in b["positions"]:
            tag = "" if r.get("active", True) else "  [inactive/closed?]"
            print(tag, end="")
            show(r)
    elif a.symbol:
        show(analyze_position(a.symbol, avg=a.avg, qty=a.qty, view_target=a.target))
    else:
        print("usage: marleg_opt_position.py SYMBOL --avg X --qty N   |   --book")
