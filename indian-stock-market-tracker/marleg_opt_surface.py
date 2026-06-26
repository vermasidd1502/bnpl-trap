"""
marleg_opt_surface.py — the WHOLE-ASSET option picture: every strike x every expiry as one decision
surface, plus a ranked shortlist of the best contracts to express a view. Groww-only (no yfinance).

Two views off one fetch:
  surface(und, kind)  -> a 2-D grid (rows = strikes ATM±n, cols = live expiries). Each cell carries the
                         BUY/OK/CAUTION/AVOID stance (the expiry-matrix logic), the breakeven move %, the
                         drift-free P(beat breakeven), theta bleed over the hold, EV, liquidity. The map.
  rank(und, kind, mode) -> the same cells, gated on liquidity + stance!=AVOID, scored by a chosen objective
                         and sorted. The shortlist you act on.

The honest core: a ranking only means something RELATIVE TO A MOVE. We anchor it to a target (pivot R1 for
calls / S1 for puts — or an override) and rank the STRUCTURE that best expresses that view. It does NOT
claim the view is right; if your direction is wrong, the #1 pick still loses. P(beat BE) uses a ZERO-drift
(no-edge) assumption on purpose — no free lunch baked in.

Reuses the pod's option core: marleg_vol (BS price / Newton IV / FD greeks / normal CDF), marleg_instruments
(live strikes+expiries from Groww's master), marleg_options_monitor (live quotes), marleg_pivots (targets).

Read-only. Decision-support, not investment advice — I'm not a licensed advisor.

  python marleg_opt_surface.py NIFTY
  python marleg_opt_surface.py NIFTY --put --mode ride
"""
import math
import datetime as dt
import concurrent.futures as cf

import marleg_vol as mv
import marleg_options_monitor as mom
import marleg_pivots as pv
import marleg_instruments as inst
from marleg_expiry_matrix import _realized, _spot, _stance

R = mv.R_FREE
MODES = ("rr", "ride", "conv", "bleed")
MODE_LABEL = {"rr": "best risk:reward", "ride": "cheap ride", "conv": "conviction", "bleed": "low bleed"}


def _reprice(iv, K, dte_after, S, kind):
    """Option value `dte_after` days from now at underlying S (intrinsic at/after expiry)."""
    if dte_after <= 0:
        return max(S - K, 0.0) if kind == "C" else max(K - S, 0.0)
    return mv.bs_price(S, K, dte_after / 365.0, R, iv, kind)


def _prob_past(level, spot, iv, dte, kind):
    """Drift-free P(S_T finishes past `level` in the option's favour) — call: P(S>level), put: P(S<level).
    Zero drift = no directional edge assumed (no free lunch). Lognormal."""
    T = max(dte, 0.5) / 365.0
    vol = max(iv, 1e-4) * math.sqrt(T)
    if level <= 0 or spot <= 0:
        return 0.99 if (kind == "C") == (level <= spot) else 0.01
    z = math.log(level / spot) / vol
    p = mv._ncdf(-z) if kind == "C" else mv._ncdf(z)
    return max(0.01, min(0.99, p))


def _liquidity(oi, spread_pct):
    """0–100 tradeability from open interest (log) minus a bid/ask-spread penalty."""
    oi = oi or 0
    base = 18.0 * math.log10(max(oi, 1))               # 200->~41, 2k->~59, 20k->~77, 200k->~95
    pen = (spread_pct if spread_pct is not None else 4.0) * 3.0
    return max(0.0, min(100.0, base - pen))


def _cell(und, spot, K, kind, e_iso, sym, q, rv, levels, hold):
    """One (strike, expiry) decision: stance, breakeven, probabilities, theta, EV, liquidity."""
    today = dt.date.today()
    e = dt.date.fromisoformat(e_iso)
    dte = (e - today).days
    if dte <= 0:
        return None
    prem = q.get("ltp")
    if not prem or prem <= 0:
        return None
    iv = q.get("iv")
    if not iv:
        iv, _ = mv.implied_vol_newton(prem, spot, K, max(dte, 1) / 365.0, R, kind)
    iv = iv or rv or 0.12
    gk = mv.fd_greeks(spot, K, max(dte, 1) / 365.0, R, iv, kind)

    call = kind == "C"
    intrinsic = max(0.0, spot - K) if call else max(0.0, K - spot)
    be = K + prem if call else K - prem
    be_move_pct = (be / spot - 1) * 100                # +ve = needs spot up (call), -ve = down (put)
    p_beat = _prob_past(be, spot, iv, dte, kind)

    target = levels["tgt_up"] if call else levels["tgt_dn"]      # ≈1σ expected move over the hold
    stop = levels["stop_dn"] if call else levels["stop_up"]      # nearest pivot the other way

    sigma_hold = spot * rv * math.sqrt(hold / 365.0)
    dte_after = dte - hold
    expired = dte_after <= 0
    flat = _reprice(iv, K, dte_after, spot, kind)
    theta_flat_pct = round((flat / prem - 1) * 100)
    fav = spot + (sigma_hold if call else -sigma_hold)
    up1_pct = round((_reprice(iv, K, dte_after, fav, kind) / prem - 1) * 100)

    tgt_val = _reprice(iv, K, dte_after, target, kind) if target else None
    stop_val = _reprice(iv, K, dte_after, stop, kind) if stop else None
    target_gain_pct = round((tgt_val / prem - 1) * 100) if tgt_val is not None else None
    stop_loss_pct = round((stop_val / prem - 1) * 100) if stop_val is not None else None
    rr = round(abs(target_gain_pct) / abs(stop_loss_pct), 2) if (target_gain_pct and stop_loss_pct) else None

    p_target = _prob_past(target, spot, iv, dte, kind) if target else p_beat
    ride_ret = (tgt_val / prem - 1) if tgt_val is not None else 0.0          # return if it tags the target
    ev = (p_target * (tgt_val if tgt_val is not None else prem)
          + (1 - p_target) * flat - prem)              # reach-target vs sit-and-bleed, minus what you paid
    ev_per_rs = ev / prem

    oi = q.get("oi")
    liq = _liquidity(oi, q.get("spread_pct"))
    stance, footnote = _stance(expired, theta_flat_pct, up1_pct, oi, hold)

    return {"strike": K, "expiry": e_iso, "symbol": sym, "dte": dte,
            "monthly": e == mv.monthly_expiry(e.year, e.month),
            "premium": round(prem, 2), "iv_pct": round(iv * 100, 1), "delta": round(gk["delta"], 3),
            "theta_day": round(gk["theta"], 2), "intrinsic": round(intrinsic, 2),
            "breakeven": round(be, 1), "be_move_pct": round(be_move_pct, 2), "p_beat_be": round(p_beat, 3),
            "theta_flat_pct": theta_flat_pct, "up1_pct": up1_pct,
            "target": target, "stop": stop, "target_gain_pct": target_gain_pct,
            "stop_loss_pct": stop_loss_pct, "rr": rr, "p_target": round(p_target, 3),
            "ride_ret": round(ride_ret, 3), "ev": round(ev, 2), "ev_per_rs": round(ev_per_rs, 3),
            "oi": oi, "spread_pct": q.get("spread_pct"), "liquidity": round(liq),
            "stance": stance, "footnote": footnote}


def _scaffold(und, kind, n_strikes, n_expiries, hold):
    """Spot, realized vol, target/stop levels, the strike band and live expiries — shared by surface()/rank()."""
    und = (und or "NIFTY").upper().strip()
    if not inst.has_options(und):
        return None, {"ok": False, "error": f"{und} has no listed options in the Groww master"}
    exchange = inst.exchange_of(und)
    spot = _spot(und, exchange)
    if not spot:
        return None, {"ok": False, "error": f"no spot for {und}"}
    rv = _realized(und, exchange=exchange) or 0.12
    piv = pv.pivots(und, spot=spot)
    cl = piv["methods"]["classic"] if piv.get("ok") else {}
    R1, R2, S1, S2 = cl.get("R1"), cl.get("R2"), cl.get("S1"), cl.get("S2")
    # TARGET = the expected move over the hold (≈1σ) — a real horizon-appropriate move, not a 0.1% daily pivot;
    # if a pivot resistance/support sits FURTHER than 1σ, use that (price tends to stall at it). STOP = the
    # nearest pivot level the other way (a tight, structural exit).
    sig = spot * rv * math.sqrt(hold / 365.0)
    near_res = next((x for x in sorted([v for v in (R1, R2) if v and v > spot])), None)
    near_sup = next((x for x in sorted([v for v in (S1, S2) if v and v < spot], reverse=True)), None)
    levels = {"tgt_up": round(max(spot + sig, near_res or 0), 1),
              "tgt_dn": round(min(spot - sig, near_sup if near_sup is not None else 1e18), 1),
              "stop_dn": round(near_sup if near_sup is not None else spot - 1.3 * sig, 1),
              "stop_up": round(near_res if near_res is not None else spot + 1.3 * sig, 1)}
    exps = inst.expiries(und, within_days=140)[:n_expiries]
    front = exps[0] if exps else None
    grid = inst.strikes(und, front) if front else []
    if not grid:
        step = mom.INDEX_STEP.get(und) or mom._strike_step(spot)
        atm = round(spot / step) * step
        grid = [atm + i * step for i in range(-n_strikes, n_strikes + 1)]
    grid = sorted(set(grid))
    atm = min(grid, key=lambda x: abs(x - spot))
    ai = grid.index(atm)
    band = grid[max(0, ai - n_strikes): ai + n_strikes + 1]
    ctx = {"und": und, "exchange": exchange, "spot": spot, "rv": rv,
           "pivots": {"S1": S1, "S2": S2, "R1": R1, "R2": R2, "P": cl.get("P")},
           "levels": levels, "sigma_hold": round(sig, 1),
           "expiries": exps, "strikes": sorted(band, reverse=True)}
    return ctx, None


def _fetch_cells(ctx, kind, hold):
    """Parallel live-quote every (strike, expiry) in the band and build its cell."""
    und, exchange, spot, rv = ctx["und"], ctx["exchange"], ctx["spot"], ctx["rv"]
    levels = ctx["levels"]
    jobs = []
    for K in ctx["strikes"]:
        for e in ctx["expiries"]:
            c = inst.contract(und, e, K, kind)
            if c:
                jobs.append((K, e, c))

    def _one(job):
        K, e, c = job
        try:
            q = mom.option_quote(c["symbol"], exchange=c.get("exchange") or exchange)
        except Exception:
            return None
        if not isinstance(q, dict) or "error" in q or (q.get("ltp") or 0) <= 0:
            return None
        return _cell(und, spot, K, kind, e, c["symbol"], q, rv, levels, hold)

    cells, failed = [], []
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        for job, r in zip(jobs, ex.map(_one, jobs)):
            (cells.append(r) if r else failed.append(job))
    for job in failed:                       # one sequential retry — fills transient concurrent-fetch drops
        r = _one(job)
        if r:
            cells.append(r)
    return cells


def _asof():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")


def surface(und, kind="C", n_strikes=4, n_expiries=6, hold=10):
    """The strikes x expiries decision grid for one underlying + right."""
    ctx, err = _scaffold(und, kind, n_strikes, n_expiries, hold)
    if err:
        return err
    cells = _fetch_cells(ctx, kind, hold)
    if not cells:
        return {"ok": False, "error": f"no live {ctx['und']} {('CE' if kind=='C' else 'PE')} contracts quoted"}
    grid = {}
    for c in cells:
        grid.setdefault(str(int(c["strike"])), {})[c["expiry"]] = c
    today = dt.date.today()
    cols = [{"expiry": e, "dte": (dt.date.fromisoformat(e) - today).days,
             "monthly": dt.date.fromisoformat(e) == mv.monthly_expiry(*map(int, e.split("-")[:2]))}
            for e in ctx["expiries"]]
    return {"ok": True, "underlying": ctx["und"], "spot": round(ctx["spot"], 2),
            "kind": kind, "right": "CE" if kind == "C" else "PE", "exchange": ctx["exchange"],
            "lot": inst.lot_size(ctx["und"]), "realized_vol_pct": round(ctx["rv"] * 100, 1),
            "hold_sessions": hold, "pivots": ctx["pivots"], "sigma_hold": ctx["sigma_hold"],
            "target_up": ctx["levels"]["tgt_up"], "target_dn": ctx["levels"]["tgt_dn"],
            "strikes": [int(s) for s in ctx["strikes"]], "expiries": cols, "grid": grid,
            "asof": _asof(),
            "note": "Whole-asset option surface: stance per (strike, expiry) over a ~2-week hold; number = % "
                    "move spot needs to break even. Live Groww premiums + realized vol + pivots.",
            "caveat": "Decision-support, not investment advice — I'm not a licensed advisor. Read-only."}


def _score(c, mode):
    if mode == "rr":
        return c["ev_per_rs"]
    if mode == "ride":
        return c["ride_ret"] * c["p_target"]
    if mode == "conv":
        return c["p_beat_be"]
    return -(c["theta_flat_pct"])                       # low bleed = least negative theta-if-flat


def _reason(c, mode):
    h = "R1" if c["target"] and c["stance"] else "target"
    if mode == "rr":
        return (f"best EV per rupee — {round(c['p_target']*100)}% to tag the target, "
                f"theta bleed only {c['theta_flat_pct']}% if it sits")
    if mode == "ride":
        return (f"cheap convex ride — +{c['target_gain_pct']}% if it reaches the target "
                f"(P {round(c['p_target']*100)}%)")
    if mode == "conv":
        return f"highest P(beat breakeven) — {round(c['p_beat_be']*100)}%, the safest structure here"
    return f"lowest bleed — {c['theta_flat_pct']}% if flat over the hold; survives sideways chop"


def rank(und, kind="C", mode="rr", n=6, n_strikes=5, n_expiries=6, hold=10, min_liq=35, surf=None):
    """Liquidity-gated, stance-filtered top-N contracts scored by `mode`, anchored to the pivot target."""
    mode = mode if mode in MODES else "rr"
    if surf is None:
        surf = surface(und, kind, n_strikes=n_strikes, n_expiries=n_expiries, hold=hold)
    if not surf.get("ok"):
        return surf
    cells = [c for row in surf["grid"].values() for c in row.values()]
    elig = [c for c in cells if c["stance"] != "AVOID" and (c["liquidity"] or 0) >= min_liq]
    if not elig:
        elig = [c for c in cells if c["stance"] != "AVOID"]            # relax liquidity if nothing clears
    elig.sort(key=lambda c: _score(c, mode), reverse=True)
    picks = []
    for i, c in enumerate(elig[:n]):
        p = dict(c)
        p["rank"] = i + 1
        p["score"] = round(_score(c, mode), 4)
        p["reason"] = _reason(c, mode)
        picks.append(p)
    target = surf.get("target_up") if kind == "C" else surf.get("target_dn")
    return {"ok": True, "underlying": surf["underlying"], "spot": surf["spot"], "kind": kind,
            "right": surf["right"], "mode": mode, "mode_label": MODE_LABEL[mode], "modes": list(MODES),
            "target": target, "target_kind": "≈ +1σ expected move" if kind == "C" else "≈ −1σ expected move",
            "hold_sessions": hold, "n": len(picks), "picks": picks, "asof": surf["asof"],
            "note": f"Ranked to express a {('+1σ up' if kind=='C' else '−1σ down')} move (the expected move over "
                    f"~{hold} sessions) by '{MODE_LABEL[mode]}'. Gated on liquidity (your only-liquid rule) + stance.",
            "caveat": "Ranks the STRUCTURE for a target you set — not whether the move happens. No free lunch: "
                      "the chain prices most of this in. Decision-support, not advice; I'm not a licensed advisor."}


if __name__ == "__main__":
    import sys, argparse, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("underlying", nargs="?", default="NIFTY")
    ap.add_argument("--put", action="store_true")
    ap.add_argument("--mode", default="rr", choices=MODES)
    a = ap.parse_args()
    k = "P" if a.put else "C"
    s = surface(a.underlying, k)
    if not s.get("ok"):
        print(s.get("error")); sys.exit()
    print(f"\n  {s['underlying']} spot ₹{s['spot']} · {s['right']} · RV {s['realized_vol_pct']}% · "
          f"S1 {s['pivots']['S1']} / R1 {s['pivots']['R1']} · {s['asof']}")
    hdr = "  {:>7}".format("strike") + "".join("{:>9}".format(c["expiry"][5:]) for c in s["expiries"])
    print(hdr)
    for K in s["strikes"]:
        row = s["grid"].get(str(K), {})
        line = "  {:>7}".format(K)
        for c in s["expiries"]:
            cell = row.get(c["expiry"])
            line += "{:>9}".format(("%s %+.1f" % (cell["stance"][:1], cell["be_move_pct"])) if cell else "·")
        print(line)
    r = rank(a.underlying, k, a.mode, surf=s)
    print(f"\n  top {r['right']} by {r['mode_label']} → target ₹{r['target']} ({r['target_kind']}):")
    for p in r["picks"]:
        print(f"    #{p['rank']} {int(p['strike'])}{r['right']} {p['expiry'][5:]} ({p['dte']}d) "
              f"[{p['stance']}] liq {p['liquidity']} · {p['reason']}")
    print(f"\n  {r['caveat']}")
