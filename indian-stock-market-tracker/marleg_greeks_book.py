"""
marleg_greeks_book.py — NET Greeks exposure (Δ/Γ/Θ/V) across a book.

Per-option Greeks already live in marleg_vol; this aggregates them into the number that actually matters
for risk: what is my WHOLE book's exposure? Works on the live Groww F&O positions (read-only) OR on a
hypothetical list of legs (a position builder). Equity legs count as pure delta (Δ=qty, no Γ/Θ/V).

  net delta  — share-equivalent directional exposure (+ long / − short); also ₹ P&L per +1% in the underlier
  net gamma  — how fast delta changes per ₹1 move (convexity)
  net theta  — ₹/day time decay (− = you PAY decay, + = you COLLECT it)
  net vega   — ₹ P&L per +1 vol-point (IV) move

Read-only — it only reads quotes/positions. Never places an order.
"""
import datetime as dt
import marleg_vol as mv
import marleg_options_monitor as mom


def _leg_greeks(sym, qty, g=None):
    """Greeks for one signed leg (qty>0 long, <0 short). Option → full Greeks; non-option → pure delta."""
    info = mv.parse_option(sym)
    g = g or mom._g()
    if not info:                                            # treat as equity/underlier: pure delta
        S = mv.underlying_ltp(sym, g)
        if not S:
            return None
        return {"symbol": sym, "underlying": sym, "kind": "EQ", "qty": qty, "spot": round(S, 2), "iv": None,
                "delta": float(qty), "gamma": 0.0, "theta": 0.0, "vega": 0.0, "per_share": {"delta": 1.0}}
    und, K, kind = info["underlying"], info["strike"], info["kind"]
    S = mv.underlying_ltp(und, g)
    if not S:
        return None
    T = max((info["expiry"] - dt.date.today()).days, 0) / 365.0
    iv = None
    try:
        q = mom.option_quote(sym)
        if isinstance(q, dict) and "error" not in q:
            if q.get("iv"):
                iv = q["iv"]
            elif q.get("ltp"):
                iv, _ = mv.implied_vol_newton(q["ltp"], S, K, T, mv.R_FREE, kind)
    except Exception:
        pass
    if not iv:
        iv = mv.realized_vol(und) or 0.30                   # fallback when the option doesn't quote
    gk = mv.greeks(S, K, T, mv.R_FREE, iv, kind)
    return {"symbol": sym, "underlying": und, "strike": K, "kind": ("CE" if kind == "C" else "PE"),
            "qty": qty, "spot": round(S, 2), "iv": round(iv * 100, 1),
            "delta": qty * gk["delta"], "gamma": qty * gk["gamma"], "theta": qty * gk["theta"], "vega": qty * gk["vega"],
            "per_share": {k: round(v, 5) for k, v in gk.items()}}


def _aggregate(legs):
    nd = sum(l["delta"] for l in legs)
    ng = sum(l["gamma"] for l in legs)
    nt = sum(l["theta"] for l in legs)
    nv = sum(l["vega"] for l in legs)
    rupee_per_1pct = sum(l["delta"] * l["spot"] for l in legs) * 0.01    # ₹ P&L for a +1% underlier move
    lean = "LONG" if nd > 1e-6 else "SHORT" if nd < -1e-6 else "FLAT"
    reads = [
        f"Directional: net {lean} {nd:+.0f} share-equiv (≈ ₹{rupee_per_1pct:+,.0f} per +1% move).",
        (f"Decay: you {'COLLECT' if nt > 0 else 'PAY'} ₹{abs(nt):,.0f}/day (theta {nt:+.0f})."),
        (f"Vol: {'LONG' if nv > 0 else 'SHORT'} vega — ₹{nv:+,.0f} per +1 IV-point."),
        (f"Convexity: gamma {ng:+.2f} — delta moves {ng:+.2f} per ₹1."),
    ]
    return {"ok": True, "n_legs": len(legs),
            "net": {"delta": round(nd, 2), "gamma": round(ng, 4), "theta": round(nt, 1), "vega": round(nv, 1),
                    "rupee_per_1pct": round(rupee_per_1pct, 0)},
            "legs": [{k: (round(v, 3) if isinstance(v, float) else v) for k, v in l.items() if k != "per_share"}
                     for l in legs],
            "reads": reads, "asof": dt.datetime.now(mom.dt.timezone(mom.dt.timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")}


def book(legs=None):
    """legs = [{'symbol','qty'(signed)}] for a builder; or None → read the live Groww book."""
    g = mom._g()
    out_src = "hypothetical legs"
    if legs is None:
        if not g:
            return {"ok": False, "error": "Groww unavailable (and no legs supplied)"}
        try:
            data = g.positions_data() or {}
        except Exception as e:
            return {"ok": False, "error": f"positions read failed: {str(e)[:100]}"}
        rows = data.get("positions", data) if isinstance(data, dict) else data
        net = {}
        for p in (rows or []):
            s = p.get("trading_symbol")
            if not s:
                continue
            net[s] = net.get(s, 0) + (p.get("credit_quantity", 0) - p.get("debit_quantity", 0))
        legs = [{"symbol": s, "qty": q} for s, q in net.items() if q != 0]
        out_src = "live Groww positions"
        if not legs:
            return {"ok": True, "empty": True, "source": out_src, "n_legs": 0,
                    "note": "No open F&O/equity positions in the live book right now. Use the builder to model exposure."}
    built = []
    for lg in legs:
        r = _leg_greeks(lg["symbol"], lg["qty"], g)
        if r:
            built.append(r)
    if not built:
        return {"ok": True, "empty": True, "source": out_src, "n_legs": 0,
                "note": "No priceable option/equity legs found."}
    return _aggregate(built) | {"source": out_src}


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if len(sys.argv) > 1:                       # e.g. python marleg_greeks_book.py MANAPPURAM26JUN320CE:300 RELIANCE26JUN1300PE:-250
        legs = []
        for a in sys.argv[1:]:
            sym, q = a.rsplit(":", 1)
            legs.append({"symbol": sym, "qty": int(q)})
        r = book(legs)
    else:
        r = book()                              # live book
    print(json.dumps(r, indent=2, default=str))
