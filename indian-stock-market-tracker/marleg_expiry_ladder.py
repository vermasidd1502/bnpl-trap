"""
marleg_expiry_ladder.py — short vs long expiry, side by side, for the SAME directional view.

The suggestion engine picks ONE tenor (the monthly that clears your hold + a theta buffer). This instead
prices the SAME just-OTM call across the next 3 monthly expiries so you can SEE the trade-off and pick:
  • NEAR  = cheapest premium, but a brutal theta cliff — only if the move is imminent.
  • FAR   = more premium, but lower theta-drag and more time, so a higher chance of touching the strike.
Ranked by ev_ride = P(touch the strike) x (value-when-it-touches / premium) - 1, liquidity-gated to strikes
you can actually fill. Target is held FIXED across tenors (the expected move), so ONLY time varies.

  python marleg_expiry_ladder.py RELIANCE
"""
import sys
import math
import datetime as dt


def ladder(tk, spot=None, target_pct=None, kind="C", min_oi=100, max_spread=8.0):
    import marleg_options_monitor as mom
    import marleg_vol as mv
    import marleg_opt_value as ov
    tk = (tk or "").upper().strip()
    kind = "C" if kind.upper().startswith("C") else "P"
    if not mom.has_options(tk):
        return {"ok": False, "error": f"{tk} is not in the NSE F&O universe — no options to compare"}
    if spot is None:
        try:
            spot = mv.underlying_ltp(tk, mom._g())
        except Exception:
            spot = None
    if not spot:
        return {"ok": False, "error": f"no live spot for {tk}"}
    spot = float(spot)
    r = mv.R_FREE
    rv = mv.realized_vol(tk, 20) or 0.30
    step = mom.INDEX_STEP.get(tk) or mom._strike_step(spot)
    atm = round(spot / step) * step

    today = dt.date.today()
    exps, yy, mm = [], today.year, today.month
    while len(exps) < 3:
        e = mv.monthly_expiry(yy, mm)
        if e >= today:
            exps.append(e)
        yy, mm = (yy + (mm == 12)), (1 if mm == 12 else mm + 1)
    t_near = max((exps[0] - today).days, 1) / 365.0

    # strike held FIXED across tenors so only TIME varies. Use a reachable ~0.8σ-OTM strike (over the near
    # month) where "ride to ATM" actually pays — a barely-OTM strike is almost all time-value, so touching it
    # just loses to theta. Or honor an explicit % target.
    if target_pct is not None:
        target = spot * (1 + target_pct / 100.0) if kind == "C" else spot * (1 - target_pct / 100.0)
        K = round(target / step) * step
    else:
        otm = spot * rv * math.sqrt(t_near) * 0.8
        K = round((spot + otm) / step) * step if kind == "C" else round((spot - otm) / step) * step
        target = float(K)
    if kind == "C" and K <= spot:
        K = atm + step; target = float(K)
    if kind == "P" and K >= spot:
        K = atm - step; target = float(K)

    rows = []
    for e in exps:
        d2e = max((e - today).days, 1)
        T = d2e / 365.0
        mu = 0.0   # driftless touch probability — honest vol-implied odds, no directional assumption baked in
        try:
            q = mom.option_quote(mom.build_symbol(tk, K, kind, e))
        except Exception:
            q = None
        live = bool(isinstance(q, dict) and "error" not in q and q.get("ltp"))
        iv = (q.get("iv") if live and q.get("iv") else rv) or rv
        ltp = float(q["ltp"]) if live else mv.bs_price(spot, K, T, r, iv, kind)
        gk = mv.greeks(spot, K, T, r, iv, kind)
        atm_val = mv.bs_price(K, K, T / 2, r, iv, kind)              # value when spot touches the strike (½ time left)
        p_touch = ov.prob_touch(spot, K, T, iv, mu)
        oi = q.get("oi") if live else None
        sp = q.get("spread_pct") if live else None
        ev_ride = (p_touch * atm_val / ltp - 1) * 100 if ltp else None
        be = ((K + ltp) / spot - 1) * 100 if kind == "C" else (1 - (K - ltp) / spot) * 100
        rows.append({"expiry": e.isoformat(), "days": d2e, "strike": round(K, 1), "kind": kind,
                     "cost": round(ltp, 2), "iv": round(iv * 100, 1), "delta": round(gk["delta"], 3),
                     "theta_drag_pct": round(-gk["theta"] / ltp * 100, 2) if ltp else None,
                     "be_move_pct": round(be, 1), "p_touch": round(p_touch * 100, 1),
                     "ret_at_touch": round((atm_val / ltp - 1) * 100, 0) if ltp else None,
                     "ev_ride": round(ev_ride, 0) if ev_ride is not None else None,
                     "target_spot": round(target, 1), "oi": oi, "spread_pct": sp, "live": live,
                     "fillable": bool(live and (oi or 0) >= min_oi and (sp is None or sp <= max_spread))})

    pool = [x for x in rows if x["fillable"]] or rows
    best = max(pool, key=lambda x: (x["ev_ride"] if x["ev_ride"] is not None else -1e9))
    bi = rows.index(best)
    near, far = rows[0], rows[-1]
    label = {0: "NEAR", len(rows) - 1: "FAR"}.get(bi, "MID")
    why = ("Same {} strike, only time changes. NEAR ({}d) is cheapest (₹{}) but bleeds {}%/day theta; "
           "FAR ({}d) costs ₹{} with {}%/day decay and a {}% chance of touching the strike vs NEAR's {}%. "
           "Best risk-adjusted = {} ({}d) on EV-ride {:+.0f}%.").format(
        round(K, 1), near["days"], near["cost"], near["theta_drag_pct"], far["days"], far["cost"],
        far["theta_drag_pct"], far["p_touch"], near["p_touch"], label, best["days"], best["ev_ride"] or 0)
    return {"ok": True, "tk": tk, "spot": round(spot, 2), "strike": round(K, 1), "kind": kind,
            "rows": rows, "best_idx": bi, "best_label": label, "why": why,
            "note": "Same strike across the next 3 monthlies (only TIME varies). ev_ride = P(touch strike) x "
                    "value-at-touch / premium - 1. Longer tenor = more premium but lower theta-drag + higher "
                    "touch odds. Liquidity-gated. Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    import json
    tk = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    print(json.dumps(ladder(tk), indent=2))
