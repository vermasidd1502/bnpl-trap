"""
marleg_daymap.py — "map your day": where price sits vs its key levels, a Monte-Carlo of the REST of
today's session, the odds of BREAKING resistance vs rejecting, and which scenario the day is tracking.

Hitting resistance is normal — supply just concentrates there. The question is break-or-reject, and that's
quantifiable. This stitches three things the pod already has:
  • LEVELS   (marleg_levels)      -> the resistance/support ladder
  • VOL      (marleg_vol)         -> realized vol -> the size of the remaining-session move
  • MONTE-CARLO                   -> simulate the rest of the day from the live spot -> P(touch/break each
                                     level by the bell), the expected-close cone, and a bull/base/bear fan
Plus PATH-MATCHING: given where price is right now vs the open, which percentile band is the day tracking?

Read-only, live-spot decision-support. NEVER trades.

  python marleg_daymap.py NIFTY
"""
import sys
import math
import datetime as dt
import numpy as np


def _ist_now():
    return dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)   # machine is US-CT; IST = UTC+5:30


def _session():
    """NSE session = 09:15-15:30 IST (375 min). Returns (elapsed_frac, remaining_frac, state)."""
    now = _ist_now()
    op = now.replace(hour=9, minute=15, second=0, microsecond=0)
    cl = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now < op:
        return 0.0, 1.0, "pre-open"
    if now > cl:
        return 1.0, 0.0, "closed"
    el = (now - op).total_seconds() / 60.0
    return el / 375.0, (375.0 - el) / 375.0, "live"


def daymap(tk="NIFTY", spot=None, n_paths=4000, n_steps=26):
    import marleg_vol as mv
    import marleg_levels as lv
    import marleg_options_monitor as mom
    tk = (tk or "NIFTY").upper()
    if spot is None:
        try:
            spot = mv.underlying_ltp(tk, mom._g())
        except Exception:
            spot = None
    o = h = l = None
    try:
        import yfinance as yf
        d1 = yf.Ticker(mv._yf_symbol(tk)).history(period="1d", interval="5m")
        if len(d1):
            o, h, l = float(d1["Open"].iloc[0]), float(d1["High"].max()), float(d1["Low"].min())
            if spot is None:
                spot = float(d1["Close"].iloc[-1])
    except Exception:
        pass
    if not spot:
        return {"ok": False, "error": f"no live spot for {tk}"}
    spot = float(spot)
    o = o or spot; h = max(h or spot, spot); l = min(l or spot, spot)

    L = lv.levels(tk)
    res = [x for x in (L.get("resistances") or []) if x["price"] > spot][:4] if L.get("ok") else []
    sup = [x for x in (L.get("supports") or []) if x["price"] < spot][:4] if L.get("ok") else []
    nearest_r = res[0] if res else None

    sig_ann = mv.realized_vol(tk, 20) or ((mv.india_vix() or 14) / 100.0)
    sig_day = sig_ann / math.sqrt(252.0)
    el, rem, state = _session()
    if rem < 1e-6:                                   # closed -> project the NEXT full session
        rem, state = 1.0, "next-session"
    sig_rem = sig_day * math.sqrt(max(rem, 1e-6))    # log-vol over the remaining session

    # ---- Monte-Carlo the rest of the day (driftless GBM) ----
    rng = np.random.default_rng(7)
    step_sd = sig_rem / math.sqrt(n_steps)
    incr = rng.normal(-0.5 * step_sd ** 2, step_sd, size=(n_paths, n_steps))
    paths = spot * np.exp(np.cumsum(incr, axis=1))   # (n_paths, n_steps) -> price path to the close
    closeP = paths[:, -1]
    pmax, pmin = paths.max(axis=1), paths.min(axis=1)
    pc = lambda q: round(float(np.percentile(closeP, q)), 2)
    cone = {"p5": pc(5), "p25": pc(25), "p50": pc(50), "p75": pc(75), "p95": pc(95)}

    res_out = [{**r, "p_touch": round(float((pmax >= r["price"]).mean()) * 100),
                "p_break_close": round(float((closeP >= r["price"]).mean()) * 100)} for r in res]
    sup_out = [{**s, "p_touch": round(float((pmin <= s["price"]).mean()) * 100),
                "p_lose_close": round(float((closeP <= s["price"]).mean()) * 100)} for s in sup]

    order = np.argsort(closeP)
    fan = {k: [round(float(x), 2) for x in paths[order[int(q / 100 * (n_paths - 1))]]]
           for k, q in (("bear", 15), ("base", 50), ("bull", 85))}

    # ---- resistance-pressure metric (0-100) + state ----
    day_ret = spot / o - 1
    rng_pos = (spot - l) / (h - l) if h > l else 0.5
    pressure, rstate, pbreak = 0, "no resistance overhead", None
    if nearest_r:
        dist = nearest_r["price"] / spot - 1
        closeness = max(0.0, 1 - dist / max(sig_rem, 1e-6))         # 1 at the level, 0 if >1 remaining-σ away
        strength = nearest_r["strength"] / 100.0
        mom_up = 1.0 if day_ret > 0 else 0.45
        pressure = round(100 * closeness * (0.45 + 0.55 * strength) * (0.7 + 0.3 * mom_up))
        pbreak = res_out[0]["p_break_close"]
        if h >= nearest_r["price"] * 0.999 and spot < nearest_r["price"] * 0.997:
            rstate = "REJECTED — tagged the level and fell back (supply winning so far)"
        elif closeness > 0.6 and day_ret >= 0:
            rstate = "COILING under resistance — testing it (break or fade decides the day)"
        elif closeness > 0.6:
            rstate = "pinned under resistance on a soft tape"
        else:
            rstate = "approaching — room before the level"

    # ---- path-matching: which band is TODAY tracking? ----
    if state == "live" and el > 0.03 and o:
        sig_el = sig_day * math.sqrt(el)
        z = math.log(spot / o) / max(sig_el, 1e-6)
        pctile = round(0.5 * (1 + math.erf(z / math.sqrt(2))) * 100)
        track = ("tracking the BULL band (upper third)" if pctile > 70
                 else "tracking the BEAR band (lower third)" if pctile < 30
                 else "tracking the BASE / median path")
    else:
        pctile, track = 50, ("session not open yet" if state == "pre-open" else
                             "session closed — this maps the NEXT session" if state in ("closed", "next-session")
                             else "just opened")

    exp_close, lo95, hi95 = cone["p50"], cone["p5"], cone["p95"]
    verdict = (f"Seeing resistance is normal — it's where supply sits. Right now: {rstate}. "
               f"Monte-Carlo of the rest of the session ({round(rem*100)}% left): expected close ~{exp_close} "
               f"(range {lo95}–{hi95}). " +
               (f"P(break {nearest_r['price']} by close) = {pbreak}%, P(just touch it) = {res_out[0]['p_touch']}%. "
                if nearest_r else "") +
               f"The day is {track}.")
    return {"ok": True, "tk": tk, "spot": round(spot, 2), "open": round(o, 2), "high": round(h, 2),
            "low": round(l, 2), "day_ret_pct": round(day_ret * 100, 2), "range_pos_pct": round(rng_pos * 100),
            "session_state": state, "session_elapsed_pct": round(el * 100), "session_left_pct": round(rem * 100),
            "sigma_remaining_pct": round(sig_rem * 100, 2), "resistance_pressure": pressure, "resistance_state": rstate,
            "p_break_nearest_res": pbreak, "track_pctile": pctile, "tracking": track,
            "resistances": res_out, "supports": sup_out, "cone": cone, "fan": fan,
            "expected_close": exp_close, "verdict": verdict,
            "note": "Driftless Monte-Carlo of the remaining NSE session from the live spot; vol = 20d realized "
                    "scaled to time left. P(touch) uses the path max/min, P(break) the simulated close. "
                    "Path-matching = where spot sits vs the open-anchored distribution. Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    import json
    tk = sys.argv[1] if len(sys.argv) > 1 else "NIFTY"
    r = daymap(tk)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n{r['tk']}  spot {r['spot']}  ({'+' if r['day_ret_pct']>=0 else ''}{r['day_ret_pct']}% day, "
          f"{r['range_pos_pct']}% up range)  ·  session {r['session_state']} {r['session_left_pct']}% left")
    print(f"  resistance pressure {r['resistance_pressure']}/100 — {r['resistance_state']}")
    print(f"  expected close ~{r['expected_close']}  ·  cone {r['cone']}")
    print("  RESISTANCES:  " + " | ".join(f"{x['price']} P(touch {x['p_touch']}%, break {x['p_break_close']}%)" for x in r["resistances"]))
    print("  SUPPORTS:     " + " | ".join(f"{x['price']} P(touch {x['p_touch']}%)" for x in r["supports"]))
    print(f"  DAY IS {r['tracking']} (pctile {r['track_pctile']})")
    print(f"\n  {r['verdict']}")
