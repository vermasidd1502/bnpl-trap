"""
marleg_opt_value.py — OTM-call VALUE LAB: fair value vs the live order book + a strike optimizer
for the "buy OTM, ride it into ATM" style.

The honest core (FIN 513): under pure risk-neutral pricing there is NO edge — a fairly-priced option's
expected return is just the risk-free rate. Positive expectancy for a call BUYER comes from exactly two
places, and this module keeps them separate instead of blurring them into one "score":

  1. MISPRICING — the market price ≠ model fair value at a sane vol anchor.  -> value_ladder() tally
  2. A VIEW      — you think spot drifts up faster than risk-free.            -> optimize_otm_calls(view)

Implied vol is SOLVED, not observed: we invert Black-Scholes on the order-book mid (Newton → secant(FD)
→ bisection, in marleg_vol.implied_vol_newton). Greeks are finite-difference (marleg_vol.fd_greeks).

Probabilities for "gets into ATM":
  • prob_itm   = N(d2)             — terminal P(S_T ≥ K) at expiry.
  • prob_touch = first-passage     — P(spot reaches K at ANY time before expiry) via the reflection
                                     principle. This is the relevant one for "moves into ATM".

  python marleg_opt_value.py MANAPPURAM --target 360
  python marleg_opt_value.py RELIANCE --move 6
"""
import math
import datetime as dt

import marleg_vol as mv
import marleg_options_monitor as mom

_ncdf = mv._ncdf


# ------------------------------------------------------------------ probabilities
def prob_itm(S, K, T, mu, sigma):
    """P(S_T ≥ K) under lognormal drift mu (use mu=r for the market's risk-neutral odds)."""
    if T <= 0 or sigma <= 0:
        return 1.0 if S >= K else 0.0
    d2 = (math.log(S / K) + (mu - 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return _ncdf(d2)


def prob_touch(S, K, T, sigma, mu):
    """
    P(spot REACHES the upper barrier K before T) for a GBM with drift mu — "gets into ATM".
    First-passage (reflection principle): with b=ln(K/S)>0, ν=mu−½σ², s=σ√T,
        P = N((−b+νT)/s) + exp(2νb/σ²)·N((−b−νT)/s)
    Always ≥ prob_itm (touching is easier than finishing above).
    """
    if K <= S:
        return 1.0
    if T <= 0 or sigma <= 0:
        return 0.0
    b = math.log(K / S)
    nu = mu - 0.5 * sigma * sigma
    s = sigma * math.sqrt(T)
    try:
        p = _ncdf((-b + nu * T) / s) + math.exp(2 * nu * b / (sigma * sigma)) * _ncdf((-b - nu * T) / s)
    except OverflowError:
        p = 1.0
    return max(0.0, min(1.0, p))


def expected_call_value(S, K, T, mu, sigma):
    """
    Physical expectation E[max(S_T − K, 0)] under lognormal drift mu (UNDISCOUNTED — this is the
    expected rupee value of the call at expiry, not a no-arb price):
        S·e^{muT}·N(d1) − K·N(d2),  d1=(ln(S/K)+(mu+½σ²)T)/(σ√T), d2=d1−σ√T
    Under mu=r this returns e^{rT}·(BS call), so expected_return ≈ risk-free — the no-free-lunch check.
    """
    if T <= 0 or sigma <= 0:
        return max(0.0, S - K)
    d1 = (math.log(S / K) + (mu + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return S * math.exp(mu * T) * _ncdf(d1) - K * _ncdf(d2)


# ------------------------------------------------------------------ order-book tally
def _mid(o):
    """Best fair read of an option's price: bid/ask mid if both present (LTP can be stale), else LTP."""
    b, a = o.get("bid"), o.get("ask")
    if b and a and a >= b:
        return (a + b) / 2.0, "mid"
    if o.get("ltp"):
        return o["ltp"], "ltp"
    return None, None


def value_ladder(und, expiry=None, n=10):
    """
    Per-strike TALLY of model fair value vs the live order book. Two honest lenses:
      • vs REALIZED vol (the model anchor) → is the premium rich/cheap vs how the stock actually moves
        (the vol-risk-premium / level edge).
      • vs ATM IV                          → is THIS strike rich/cheap on the smile (skew edge).
    """
    ch = mom.chain(und, n=n, expiry=expiry)
    if "error" in ch:
        return ch
    S, days = ch["spot"], ch["days_to_expiry"]
    T = max(days, 1) / 365.0
    r = mv.R_FREE
    rv = ch["iv_used"] / 100.0                                   # realized-vol anchor (model sigma)
    atm_iv = None
    for row in ch["rows"]:
        if row["is_atm"]:
            atm_iv = (row["ce"].get("iv") or row["pe"].get("iv"))
            atm_iv = atm_iv / 100.0 if atm_iv else None
    out = []
    for row in ch["rows"]:
        K = row["strike"]
        rec = {"strike": K, "is_atm": row["is_atm"]}
        for side, kind in (("ce", "C"), ("pe", "P")):
            o = row[side]
            px, pxsrc = _mid(o)
            fair = mv.bs_price(S, K, T, r, rv, kind)            # what it'd cost at realized vol
            miv, diag = (mv.implied_vol_newton(px, S, K, T, r, kind) if (o.get("src") == "live" and px)
                         else (None, None))
            price_edge = (fair - px) if px else None             # +ve = market cheaper than fair = underpriced
            vol_edge = (rv - miv) if miv else None               # +ve = market IV below realized = cheap vol
            skew_edge = (atm_iv - miv) if (miv and atm_iv) else None
            flag = "—"
            if vol_edge is not None:
                flag = ("CHEAP" if vol_edge > 0.03 else "RICH" if vol_edge < -0.03 else "FAIR")
            rec[side] = {"px": round(px, 2) if px else None, "px_src": pxsrc,
                         "fair": round(fair, 2), "mkt_iv": round(miv * 100, 1) if miv else None,
                         "price_edge": round(price_edge, 2) if price_edge is not None else None,
                         "vol_edge": round(vol_edge * 100, 1) if vol_edge is not None else None,
                         "skew_edge": round(skew_edge * 100, 1) if skew_edge is not None else None,
                         "flag": flag, "oi": o.get("oi"), "spread_pct": o.get("spread_pct"),
                         "iv_method": (diag or {}).get("method"), "src": o.get("src")}
        out.append(rec)
    return {"underlying": und, "spot": S, "expiry": ch["expiry"], "days": days,
            "realized_vol": round(rv * 100, 1), "atm_iv": round(atm_iv * 100, 1) if atm_iv else None,
            "india_vix": ch.get("india_vix"), "source": ch.get("source"), "rows": out,
            "note": "Fair value priced at the stock's 20d REALIZED vol; CHEAP/RICH is the live IV vs that "
                    "realized anchor (the vol-risk-premium). price_edge>0 means the market is asking LESS "
                    "than the model — underpriced. Mid uses bid/ask (LTP is often stale on thin strikes)."}


# ------------------------------------------------------------------ OTM-call optimizer
def optimize_otm_calls(und, expiry=None, target=None, move_pct=None, drift_pct=None, n=12):
    """
    Rank OTM call strikes for the "buy OTM → ride into ATM" style. We resolve a single drift mu (your
    VIEW) and price every metric under it, so the no-free-lunch logic is explicit:
        target given      -> mu = ln(target/S)/T        (drift needed to reach your target by expiry)
        move_pct given    -> mu = ln(1+move/100)/T
        drift_pct given   -> mu = drift/100             (annualised)
        nothing (no view) -> mu = r                     (risk-neutral; expected return collapses to ~rf)
    """
    ch = mom.chain(und, n=n, expiry=expiry)
    if "error" in ch:
        return ch
    S, days = ch["spot"], ch["days_to_expiry"]
    T = max(days, 1) / 365.0
    r = mv.R_FREE
    rv = ch["iv_used"] / 100.0

    if target:
        mu = math.log(float(target) / S) / T
        view = f"reach ₹{target} by expiry"
    elif move_pct:
        mu = math.log(1 + float(move_pct) / 100.0) / T
        view = f"+{move_pct}% by expiry"
    elif drift_pct is not None:
        mu = float(drift_pct) / 100.0
        view = f"{drift_pct}%/yr drift"
    else:
        mu = r
        view = "no view (risk-neutral)"
    has_view = mu > r + 1e-9

    target_px = float(target) if target else (S * (1 + float(move_pct) / 100.0) if move_pct else None)
    rows = []
    for row in ch["rows"]:
        K = row["strike"]
        if K <= S:                                              # OTM calls only
            continue
        o = row["ce"]
        px, _src = _mid(o)
        cost = (o.get("ask") or px or mv.bs_price(S, K, T, r, rv, kind="C"))  # what you actually pay
        if not cost or cost <= 0:
            continue
        miv, _d = (mv.implied_vol_newton(px, S, K, T, r, "C") if (o.get("src") == "live" and px) else (None, None))
        iv = miv or rv                                          # price probabilities at the market's own vol
        fair = mv.bs_price(S, K, T, r, rv, "C")
        gk = mv.fd_greeks(S, K, T, r, iv, "C")
        be = K + cost
        p_touch = prob_touch(S, K, T, iv, mu)
        p_itm_view = prob_itm(S, K, T, mu, iv)
        p_itm_rn = prob_itm(S, K, T, r, iv)
        exp_val = expected_call_value(S, K, T, mu, iv)
        exp_ret = (exp_val / cost - 1) * 100
        lev = gk["delta"] * S / cost
        theta_drag = (-gk["theta"] / cost) * 100                # daily decay as % of premium
        ret_if_hit = ((max(target_px - K, 0) - cost) / cost * 100) if target_px else None
        # the OTM→ATM RIDE: when spot reaches K the call is ATM. Value it with ~half the time left
        # (mid-hold touch — earlier touch is worth more, later worth less) and assume you SELL there.
        atm_value = mv.bs_price(K, K, T / 2, r, iv, "C")
        ret_at_touch = (atm_value / cost - 1) * 100
        ev_ride = (p_touch * atm_value / cost - 1) * 100        # P(touch)·(ATM value) else 0 — ride-or-bust EV
        rows.append({"strike": K, "cost": round(cost, 2), "fair": round(fair, 2),
                     "edge": round(fair - cost, 2), "mkt_iv": round(iv * 100, 1),
                     "reachable": (target_px is None or K <= target_px * 1.005),
                     "be": round(be, 1), "be_move_pct": round((be / S - 1) * 100, 1),
                     "p_touch": round(p_touch * 100, 1), "p_itm": round(p_itm_view * 100, 1),
                     "p_itm_rn": round(p_itm_rn * 100, 1), "delta": round(gk["delta"], 3),
                     "leverage": round(lev, 1), "theta_drag": round(theta_drag, 2),
                     "atm_value": round(atm_value, 2), "ret_at_touch": round(ret_at_touch, 1),
                     "ev_ride": round(ev_ride, 1), "exp_ret": round(exp_ret, 1),
                     "ret_if_hit": round(ret_if_hit, 1) if ret_if_hit is not None else None,
                     "oi": o.get("oi"), "spread_pct": o.get("spread_pct"), "src": o.get("src")})
    # rank for the OTM→ATM ride: expected return of touch-and-sell (ev_ride). Without a view, mu=r so
    # ev_ride uses the market's own (risk-neutral) touch odds — still the right shape, just no edge.
    rows.sort(key=lambda x: -x["ev_ride"])
    rank_by = ("touch-and-sell expected return (your view)" if has_view
               else "touch-and-sell EV at risk-neutral odds (no view given)")
    # recommend a FILLABLE strike (real OI) that's actually REACHABLE under the target (a strike above
    # your target never gets into ATM, so it can't be the pick) — cheap far-OTM prints are often untraded
    fillable = [x for x in rows if (x.get("oi") or 0) > 0]
    reach = [x for x in fillable if x["reachable"]]
    rec = (reach or fillable or rows)[0] if rows else None
    return {"underlying": und, "spot": S, "expiry": ch["expiry"], "days": days,
            "realized_vol": round(rv * 100, 1), "mu_annual_pct": round(mu * 100, 1), "view": view,
            "has_view": has_view, "rank_by": rank_by, "target": target_px, "source": ch.get("source"),
            "n": len(rows), "rows": rows, "recommended": rec,
            "note": ("Ranked by the OTM→ATM RIDE: ret@ATM = the call's value WHEN spot touches the strike "
                     "(it becomes ATM and you sell), assuming the touch lands mid-hold (~half the time left — "
                     "an earlier touch is worth more, later less). EV-ride = P→ATM × that, i.e. ride-or-bust "
                     "expected return. " +
                     ("These odds use YOUR view's drift — sanity-check it; with no view they'd collapse to ~rf. "
                      if has_view else "No view supplied, so odds are risk-neutral (≈no edge) — add a target/move "
                      "to price your direction. ") +
                     "ret-if-hit shows the trap: a strike AT/ABOVE your target expires worthless if spot lands "
                     "exactly there. Thin OI = the cheap print may not be fillable. Decision-support, not advice.")}


def value_at(und, strike, expiry=None, vol=None):
    """'Move the strike' single-point estimator: model fair value + Greeks at an arbitrary strike."""
    g = mom._g()
    S = mv.underlying_ltp(und, g)
    if not S:
        return {"error": f"no spot for {und}"}
    exp = dt.date.fromisoformat(expiry) if expiry else mom.nearest_monthly_expiry()
    T = max((exp - dt.date.today()).days, 1) / 365.0
    sigma = (float(vol) / 100.0) if vol else (mv.realized_vol(und, 20) or 0.3)
    out = {"underlying": und, "spot": round(S, 2), "strike": float(strike), "expiry": exp.isoformat(),
           "days": (exp - dt.date.today()).days, "vol_used": round(sigma * 100, 1)}
    for kind, key in (("C", "call"), ("P", "put")):
        price = mv.bs_price(S, float(strike), T, mv.R_FREE, sigma, kind)
        gk = mv.fd_greeks(S, float(strike), T, mv.R_FREE, sigma, kind)
        be = (float(strike) + price) if kind == "C" else (float(strike) - price)
        out[key] = {"fair": round(price, 2), "delta": round(gk["delta"], 3), "theta": round(gk["theta"], 3),
                    "vega": round(gk["vega"], 3), "be": round(be, 1)}
    return out


if __name__ == "__main__":
    import sys, json, argparse
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("underlying")
    ap.add_argument("--expiry")
    ap.add_argument("--target", type=float)
    ap.add_argument("--move", type=float, dest="move_pct")
    ap.add_argument("--drift", type=float, dest="drift_pct")
    ap.add_argument("--ladder", action="store_true")
    a = ap.parse_args()
    if a.ladder:
        print(json.dumps(value_ladder(a.underlying, a.expiry), indent=2, default=str))
    else:
        r = optimize_otm_calls(a.underlying, a.expiry, a.target, a.move_pct, a.drift_pct)
        if "error" in r:
            print(r["error"]); sys.exit()
        print(f"\nOTM-CALL OPTIMIZER — {r['underlying']} spot ₹{r['spot']} · exp {r['expiry']} ({r['days']}d) "
              f"· RV {r['realized_vol']}% · source {r['source']}")
        print(f"  VIEW: {r['view']}  (drift {r['mu_annual_pct']}%/yr) — ranked by {r['rank_by']}")
        print(f"  {'strike':>7} {'cost':>7} {'edge':>6} {'IV':>5} {'BE%':>6} {'P→ATM':>6} "
              f"{'ret@ATM':>8} {'EV-ride':>8} {'lev':>5} {'θ/d%':>5} {'ifHit%':>7} {'OI':>6}")
        for x in r["rows"][:12]:
            print(f"  {x['strike']:>7.0f} {x['cost']:>7.2f} {x['edge']:>+6.2f} "
                  f"{x['mkt_iv']:>5.1f} {x['be_move_pct']:>+6.1f} {x['p_touch']:>6.1f} "
                  f"{x['ret_at_touch']:>+8.0f} {x['ev_ride']:>+8.0f} {x['leverage']:>5.1f} {x['theta_drag']:>5.2f} "
                  f"{(str(x['ret_if_hit']) if x['ret_if_hit'] is not None else '-'):>7} {str(x['oi'] or '-'):>6}")
        if r["recommended"]:
            rc = r["recommended"]
            print(f"\n  ➤ TOP (fillable): {int(rc['strike'])}CE @ ₹{rc['cost']} — BE +{rc['be_move_pct']}%, "
                  f"P→ATM {rc['p_touch']}%, ret@ATM {rc['ret_at_touch']:+.0f}%, EV-ride {rc['ev_ride']:+.0f}%, lev {rc['leverage']}×")
        print(f"\n  {r['note']}")
