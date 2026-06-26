"""
marleg_theta_surface.py — the ATM theta-decay SURFACE: what an ATM option "usually" costs as a function
of time-to-expiry (and IV), so you can SEE when initiating a buy is cheap vs a theta-trap.

The "usual price" of an ATM option is not folklore — it's Black-Scholes. An ATM option's value ≈
0.4 · S · σ · √T, so premium scales with √(time-left): it falls as expiry nears, and the *rate* of decay
(theta, as % of premium per day) ACCELERATES into the last ~2 weeks (the theta cliff). This engine builds:

  • a SURFACE: ATM premium (% of spot) over a grid of (days-to-expiry × implied vol) — the 3D picture.
  • the DECAY CURVE: premium% vs DTE at the stock's own realized vol (a slice of the surface).
  • the CLIFF CURVE: theta as %/day-of-premium vs DTE — where holding starts to bleed fast.
  • the LIVE ATM point (best-effort from the chain): today's actual ATM premium vs the theoretical curve
    → RICH (you're overpaying vs the model / IV elevated) or CHEAP.

It is a MODEL surface (BS at the stock's realized vol) — the "fair/usual" cost. The live point shows where
the market sits vs it. For a BUYER the read is simple and backtested-consistent: don't buy rich IV deep in
the cliff; pay for time when your thesis needs room. Read-only decision-support, not advice.
"""
import math

import marleg_vol as mv

DTE_GRID = [1, 2, 3, 4, 5, 7, 10, 14, 18, 21, 25, 30, 37, 45]


def _spot_rv(sym):
    spot = None
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        p = (g.quote(sym, segment="CASH", exchange="NSE").json().get("payload") or {})
        if p.get("last_price") is not None:
            spot = float(p["last_price"])
    except Exception:
        pass
    if spot is None:
        try:
            import groww_client as gc
            spot = mv.underlying_ltp(sym, gc.GrowwClient())
        except Exception:
            pass
    rv = None
    try:
        rv = mv.realized_vol(sym, 20)
    except Exception:
        pass
    return spot, (rv or 0.30)


def _live_atm(sym, spot, rv):
    """Best-effort: today's nearest-expiry ATM call premium + IV from the live chain."""
    try:
        import datetime as dt
        import marleg_options_monitor as mom
        exp = mom.nearest_monthly_expiry()
        dte = max((exp - dt.date.today()).days, 1)
        step = mom._strike_step(spot)
        K = round(spot / step) * step
        q = mom.option_quote(mom.build_symbol(sym, K, "C", exp))
        if isinstance(q, dict) and "error" not in q and q.get("ltp"):
            ltp = float(q["ltp"]); iv = q.get("iv") or rv
            theo = mv.bs_price(spot, K, dte / 365.0, mv.R_FREE, rv, "C")     # theory at REALIZED vol
            return {"dte": dte, "strike": K, "premium": round(ltp, 2), "premium_pct": round(ltp / spot * 100, 2),
                    "iv": round(iv * 100, 1), "theo_pct": round(theo / spot * 100, 2),
                    "verdict": "RICH" if ltp > theo * 1.06 else "CHEAP" if ltp < theo * 0.94 else "FAIR",
                    "iv_vs_rv": round((iv - rv) * 100, 1), "expiry": exp.isoformat()}
    except Exception:
        pass
    return None


def _timing(live, cliff):
    """BUY-NOW vs WAIT vs NEXT-EXPIRY — the explicit option-entry call from IV (rich/cheap) × the expiry
    cycle (is the near series inside the theta cliff?)."""
    if not live:
        return {"verdict": "N/A", "tone": "mut", "wait_for": None,
                "why": "no liquid ATM strike to price — can't judge buy-now vs wait."}
    dte = live["dte"]; v = live["verdict"]; ivd = live.get("iv_vs_rv", 0)
    in_cliff = dte <= cliff
    sgn = (f" ({'+' if ivd >= 0 else ''}{ivd} IV vs realized)" if abs(ivd or 0) >= 1 else " (premium vs model)")
    if v == "RICH":
        if dte <= 7:
            return {"verdict": "WAIT", "tone": "neg", "wait_for": "this series to expire → fresh, calmer IV",
                    "why": f"IV is RICH{sgn} AND the near series expires in {dte}d — you'd "
                           f"overpay into expiry and risk a vol-crush. Let it roll off; the next series usually opens cheaper."}
        return {"verdict": "WAIT / LATER EXPIRY", "tone": "amb", "wait_for": "IV to cool, or use a later expiry",
                "why": f"IV is RICH{sgn} — you'd overpay now and a vol-crush would hurt. "
                       f"Wait for IV to settle, or buy a LATER expiry (calmer IV, more runway)."}
    if v == "CHEAP":
        if in_cliff:
            return {"verdict": "BUY NOW · NEXT EXPIRY", "tone": "pos", "wait_for": None,
                    "why": f"IV is CHEAP — good to buy — but the near expiry ({dte}d) is inside the {cliff}d theta "
                           f"cliff, so take the NEXT expiry for clean runway."}
        return {"verdict": "BUY NOW", "tone": "pos", "wait_for": None,
                "why": f"IV is CHEAP{sgn} and the near expiry ({dte}d) clears the {cliff}d cliff — "
                       f"this is a good entry window. Don't wait."}
    if in_cliff:
        return {"verdict": "OK · NEXT EXPIRY", "tone": "amb", "wait_for": None,
                "why": f"IV is fair, but the near expiry ({dte}d) sits in the {cliff}d cliff — if you buy, use the next expiry."}
    return {"verdict": "OK TO BUY", "tone": "amb", "wait_for": None,
            "why": f"IV is fair and the near expiry ({dte}d) clears the cliff — no urgency, no penalty; buy when your entry triggers."}


def surface(sym):
    sym = (sym or "").upper().strip()
    spot, rv = _spot_rv(sym)
    if not spot:
        return {"ok": False, "sym": sym, "error": "no live spot for " + sym}
    r = mv.R_FREE
    ivs = [round(rv * m, 4) for m in (0.7, 0.85, 1.0, 1.15, 1.3)]
    # SURFACE z[iv][dte] = ATM call premium as % of spot
    z = []
    for iv in ivs:
        row = [round(mv.bs_price(spot, spot, d / 365.0, r, iv, "C") / spot * 100, 3) for d in DTE_GRID]
        z.append(row)
    # decay + cliff curves at the stock's OWN realized vol
    curve, cliff = [], []
    for d in DTE_GRID:
        T = d / 365.0
        prem = mv.bs_price(spot, spot, T, r, rv, "C")
        th = mv.greeks(spot, spot, T, r, rv, "C")["theta"]                  # per day, negative
        curve.append(round(prem / spot * 100, 3))
        cliff.append(round(-th / prem * 100, 2) if prem > 0 else None)      # %/day of premium
    # where the cliff bites: the LARGEST DTE whose daily decay >= 2.5% of premium (decay rises as expiry nears,
    # so this is the threshold below which you start bleeding fast)
    cliff_dte = next((DTE_GRID[i] for i in range(len(DTE_GRID) - 1, -1, -1) if (cliff[i] or 0) >= 2.5), DTE_GRID[-1])
    live = _live_atm(sym, spot, rv)
    # straddle (ATM call+put) cost ~= 2x a single ATM leg — the "expected move" the market is pricing
    return {
        "ok": True, "sym": sym, "spot": round(spot, 2), "rv": round(rv * 100, 1),
        "dte": DTE_GRID, "iv": [round(x * 100, 1) for x in ivs], "z": z,
        "curve": curve, "cliff": cliff, "cliff_dte": cliff_dte, "live": live, "timing": _timing(live, cliff_dte),
        "guidance": {
            "law": "ATM premium ≈ 0.4 · S · σ · √T → it scales with √(time-left): halving the days does NOT "
                   "halve the cost, but it roughly DOUBLES the theta/day.",
            "cliff": f"Theta crosses ~2.5%/day around {cliff_dte} DTE — inside that you bleed fast unless the "
                     f"move comes immediately. As a BUYER, prefer MORE time than that unless you're explicitly "
                     f"buying gamma for a same-session move.",
            "iv": "Premium scales ~linearly with IV (vega): buying when IV is elevated (live point RICH) means "
                  "you pay up AND risk IV-crush. Cheapest entries = lower IV + enough DTE.",
            "expected_move": f"At {DTE_GRID[-1]}d the market prices roughly ±{round(2*curve[-1],1)}% (ATM straddle) "
                             f"by expiry — your target must clear that to win as a buyer.",
        },
        "note": "MODEL surface (Black-Scholes at the stock's realized vol) = the 'usual/fair' ATM cost by "
                "time-to-expiry. The live ATM point shows today's market vs the model (RICH/CHEAP). "
                "Read-only decision-support — buying options is ride-or-bust; this tells you WHEN it's least "
                "stacked against you.",
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for u in (sys.argv[1:] or ["RELIANCE"]):
        s = surface(u)
        if not s.get("ok"):
            print("  " + u + ": " + s.get("error", "?")); continue
        print(f"\n═══ {s['sym']} ATM theta surface · spot {s['spot']} · RV {s['rv']}% ═══")
        print("  DTE :", "  ".join(f"{d:>5}" for d in s["dte"]))
        print("  prem%:", "  ".join(f"{c:>5.2f}" for c in s["curve"]))
        print("  θ/day%:", "  ".join(f"{(c if c is not None else 0):>5.1f}" for c in s["cliff"]))
        print(f"  theta cliff ~ {s['cliff_dte']} DTE")
        if s["live"]:
            L = s["live"]
            print(f"  LIVE ATM {L['strike']} @ ₹{L['premium']} ({L['premium_pct']}% of spot) vs theory {L['theo_pct']}% → {L['verdict']} · IV {L['iv']}% (RV{'+' if L['iv_vs_rv']>=0 else ''}{L['iv_vs_rv']})")
        print("  " + s["guidance"]["cliff"])
