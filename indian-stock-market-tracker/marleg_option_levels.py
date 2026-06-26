"""
marleg_option_levels.py — "sell the option at the top, buy at support" — done correctly.

An option has no meaningful 52w-high / fib of its OWN (it's a derivative that expires). So we trigger off the
UNDERLYING's levels and PROJECT the option premium there:

  • build the underlying's resistance ladder (R1-R3, swing-high, 52w-high, fib retracement levels above spot)
    and support ladder (S1-S3, swing-low, 52w-low, fib below),
  • find the NEAREST resistance above + nearest support below the live spot,
  • Black-Scholes PROJECT the option's premium AT those levels (using the IV solved from the live premium) →
    so "sell at resistance" becomes a concrete ₹ target and % gain, and "support" is the downside reference,
  • emit TRIM/SELL (call at resistance — move stalling, premium near a local peak), ADD/BUY (call at support),
    or HOLD (mid-range), inverted for puts.

The live trailing-stop on the premium's running high is the companion piece (needs live tracking — the
stop-guardian holds that). This engine is the structural map: where to sell, where to add, and what the
option is worth there. Read-only decision-support — you place it on Groww.
"""
import datetime as dt

import marleg_data as md
import marleg_vol as mv
import marleg_cockpit as cp


def _fib_52(sym):
    df = md.candles(sym, 1440, 260)
    if df is None or len(df) < 40:
        return {}
    c = df["close"].astype(float); h = df["high"].astype(float); l = df["low"].astype(float)
    seg = c.iloc[-60:]
    lo = float(seg.min()); hi = float(seg.max()); rng = hi - lo or 1.0
    fib = {f"fib{int(f*1000)}": round(hi - rng * f, 1) for f in (0.236, 0.382, 0.5, 0.618, 0.786)}
    return {"fib": fib, "wk52h": round(float(h.iloc[-252:].max()), 1), "wk52l": round(float(l.iloc[-252:].min()), 1)}


def signal(option_sym, premium=None):
    info = mv.parse_option_any(option_sym)
    if not info:
        return {"ok": False, "error": "could not parse option symbol " + str(option_sym)}
    u = info["underlying"]; K = float(info["strike"]); kind = info["kind"]; exp = info["expiry"]
    is_call = kind == "C"
    days = max((exp - dt.date.today()).days, 0)
    T = max(days, 0.5) / 365.0
    ck = cp.cockpit(u)
    if not ck.get("ok"):
        return {"ok": False, "error": "no underlying data for " + u}
    spot = ck["spot"]; atr = ck["atr"] or (spot * 0.01); L = ck["levels"]
    fz = _fib_52(u)
    iv = None
    if premium:
        try:
            iv, _ = mv.implied_vol_newton(float(premium), spot, K, T, mv.R_FREE, kind)
        except Exception:
            iv = None
    if not iv or iv <= 0:
        iv = mv.realized_vol(u, 20) or 0.30
    prem0 = float(premium) if premium else mv.bs_price(spot, K, T, mv.R_FREE, iv, kind)

    res = {"R1": L.get("R1"), "R2": L.get("R2"), "R3": L.get("R3"), "swing_hi": L.get("swing_hi"), "52w_high": fz.get("wk52h")}
    sup = {"S1": L.get("S1"), "S2": L.get("S2"), "S3": L.get("S3"), "swing_lo": L.get("swing_lo"), "52w_low": fz.get("wk52l")}
    for k, v in (fz.get("fib") or {}).items():
        (res if v > spot else sup)[k] = v
    R = sorted(((v, k) for k, v in res.items() if v and v > spot))
    S = sorted(((v, k) for k, v in sup.items() if v and v < spot), reverse=True)
    nr, ns = (R[0] if R else None), (S[0] if S else None)

    r = mv.R_FREE

    def proj(level_price):     # value the option AT a level, with time-to-reach scaled by distance (~1.5 sessions/ATR)
        if level_price is None:
            return None
        reach = max(0.5, min(days * 0.7, abs(level_price - spot) / atr * 1.5))
        Tp = max((days - reach) / 365.0, 0.3 / 365.0)
        val = mv.bs_price(level_price, K, Tp, r, iv, kind)
        return {"u_level": round(level_price, 1), "opt_value": round(val, 2),
                "opt_ret_pct": round((val / prem0 - 1) * 100, 0) if prem0 > 0 else None}

    at_r = proj(nr[0]) if nr else None
    if at_r:
        at_r["which"] = nr[1]
    at_s = proj(ns[0]) if ns else None
    if at_s:
        at_s["which"] = ns[1]
    d_res = (nr[0] - spot) / atr if nr else 99
    d_sup = (spot - ns[0]) / atr if ns else 99
    NEAR = 0.5

    if is_call:
        if d_res <= NEAR:
            sig, tone = "TRIM / SELL", "sell"
            why = f"underlying at resistance {nr[1]} {nr[0]:.0f} — the move is stalling, premium near a local peak. Bank it."
        elif d_sup <= NEAR:
            sig, tone = "ADD / BUY", "buy"
            why = f"underlying at support {ns[1]} {ns[0]:.0f} — bounce zone; add only if the trend gate is still green."
        else:
            sig, tone = "HOLD", "hold"
            why = f"mid-range — {d_res:.1f} ATR to resistance ({nr[1] if nr else '—'}), {d_sup:.1f} ATR to support ({ns[1] if ns else '—'})."
    else:
        if d_sup <= NEAR:
            sig, tone = "TRIM / SELL", "sell"
            why = f"underlying at support {ns[1]} {ns[0]:.0f} — put move stalling, bank it."
        elif d_res <= NEAR:
            sig, tone = "ADD / BUY", "buy"
            why = f"underlying at resistance {nr[1]} {nr[0]:.0f} — put entry zone (if bearish gate confirms)."
        else:
            sig, tone = "HOLD", "hold"
            why = f"mid-range — {d_sup:.1f} ATR to support, {d_res:.1f} ATR to resistance."

    return {
        "ok": True, "sym": option_sym, "underlying": u, "strike": K, "kind": info["right"], "expiry": exp.isoformat(),
        "days_to_expiry": days, "spot": round(spot, 1), "atr": round(atr, 1), "iv": round(iv * 100, 1),
        "premium": round(prem0, 2), "signal": sig, "tone": tone, "why": why,
        "sell_target": at_r, "support_ref": at_s,      # for a call: sell_target = value at resistance
        "all_resistance": [{"level": round(v, 1), "which": k} for v, k in R[:5]],
        "all_support": [{"level": round(v, 1), "which": k} for v, k in S[:5]],
        "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
        "caveat": "Triggers off the UNDERLYING's levels (the option mirrors it via delta); option premium at each "
                  "level is Black-Scholes-PROJECTED at the solved IV (no spread/IV-shift). Pair with a trailing stop "
                  "on the premium's running high. Read-only — you place it on Groww.",
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for s in (sys.argv[1:] or ["LT26JUL4400CE"]):
        r = signal(s)
        if not r.get("ok"):
            print("  " + s + ": " + r.get("error", "?")); continue
        print(f"\n═══ {r['sym']}  ({r['underlying']} {r['kind']} {r['strike']}, {r['days_to_expiry']}d) ═══")
        print(f"  spot {r['spot']}  premium ~₹{r['premium']}  IV {r['iv']}%  → SIGNAL: {r['signal']}")
        print(f"  {r['why']}")
        t = r["sell_target"]
        if t:
            print(f"  SELL target: at {t['which']} {t['u_level']} → option ≈ ₹{t['opt_value']} ({t['opt_ret_pct']:+.0f}%)")
        sp = r["support_ref"]
        if sp:
            print(f"  support ref: at {sp['which']} {sp['u_level']} → option ≈ ₹{sp['opt_value']} ({sp['opt_ret_pct']:+.0f}%)")
