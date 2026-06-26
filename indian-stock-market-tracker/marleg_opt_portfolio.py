"""
marleg_opt_portfolio.py — PORTFOLIO analysis of your LIVE held option positions, with explicit per-position
guidance: ACTION (drop / hold / book) · TARGET · STOP · DROP-BY date. Groww-only, READ-ONLY.

Builds on marleg_opt_position.book() (which already pulls the live Groww F&O option legs and scores each for
viability + scope + odds + greeks + decay). This layer adds the two things that scorecard didn't:
  1. a PORTFOLIO roll-up — net P&L, total theta bleed/day, net delta (directional exposure), capital at risk,
     concentration by underlying, the count in each viability band, and which positions are flagged to exit.
  2. an explicit per-position PLAY derived from those metrics:
       • ACTION  DROP / TRIM / HOLD / BOOK-PARTIAL / WATCH  (viability band × P&L × theta-cliff)
       • TARGET  the +1σ near-term reprice — what it's worth if your view works soon (option ₹ + underlying level)
       • STOP    a mechanical −40% ref floored at intrinsic, with the −1σ reprice and the decay floor as anchors
       • DROP-BY ~4 sessions before expiry — the cyclical theta-cliff exit (median −20% vs −73% if held through)

Honest: this is decision-support, not advice — it computes the exact numbers; YOU place any order. Odds are
risk-neutral (no assumed edge). READ-ONLY on the account — it never places, modifies or cancels anything.

  python marleg_opt_portfolio.py
"""
import datetime as dt

import marleg_opt_position as op
import marleg_opt_timing as ot


def _num(x, d=0.0):
    return x if isinstance(x, (int, float)) else d


def _guidance(p):
    """ACTION / TARGET / STOP / DROP-BY for one analyzed position."""
    if not p.get("ok"):
        return None
    band = p["viability"]["band"]
    pnl = _num(p["pnl"]["pct"])
    tdl = p["trading_days_left"]
    prem = _num(p["premium"])
    avg = _num(p.get("avg")) or prem
    intrinsic = _num(p["moneyness"]["intrinsic"])
    scen = {s["label"]: s for s in p["scope"]["scenarios"]}
    try:
        drop_by = ot._back_sessions(dt.date.fromisoformat(p["expiry"]), ot.EXIT_EDGE["exit_days"]).isoformat()
    except Exception:
        drop_by = None

    cliff = tdl <= ot.EXIT_EDGE["exit_days"]
    if band == "DEAD":
        action, tone, why = "DROP", "fear", "viability DEAD — odds + decay don't justify the hold; cut it"
    elif cliff:
        action, tone, why = "DROP", "warn", f"theta cliff ({tdl} sessions) — exit before terminal decay, regardless of view"
    elif band == "POOR":
        action, tone, why = "TRIM / DROP", "warn", "viability POOR — the math is against it; reduce or exit"
    elif band == "VIABLE" and pnl >= 50:
        action, tone, why = "BOOK PARTIAL", "good", f"viable and +{round(pnl)}% — book some, trail the rest to target"
    elif band == "VIABLE":
        action, tone, why = "HOLD", "good", "viable — odds + runway support holding toward the target"
    else:
        action, tone, why = "WATCH", "normal", "marginal — hold only with a tight stop + a near catalyst, else cut"

    tgt = scen.get("+1σ")
    target = ({"opt": tgt["opt_value"], "ret_pct": tgt["ret_vs_avg_pct"], "spot": tgt["spot"], "move_pct": tgt["move_pct"]}
              if tgt else None)
    stop_opt = round(max(prem * 0.60, intrinsic), 1)            # mechanical −40% ref, never below intrinsic
    dn = scen.get("−1σ")
    stop = {"opt": stop_opt, "ret_pct": round((stop_opt / avg - 1) * 100) if avg else None,
            "ref_1sigma_down": dn["opt_value"] if dn else None,
            "decay_floor": _num(p["decay"]["value_if_flat_at_expiry"])}
    return {"action": action, "tone": tone, "why": why, "drop_by": drop_by, "target": target, "stop": stop}


def portfolio():
    b = op.book()
    if not b.get("ok"):
        return b
    pos = b.get("positions") or []
    for p in pos:
        p["guidance"] = _guidance(p)

    net_pnl = sum(_num(p["pnl"]["abs"]) for p in pos)
    theta_day = sum(_num(p["greeks"]["theta_rupees_day"]) for p in pos)
    pos_delta = sum(_num(p["greeks"]["pos_delta"]) for p in pos)
    capital = sum(_num(p.get("avg")) * _num(p.get("qty")) for p in pos)
    by_und, bands = {}, {}
    for p in pos:
        by_und[p["underlying"]] = round(by_und.get(p["underlying"], 0) + _num(p["pnl"]["abs"]))
        bands[p["viability"]["band"]] = bands.get(p["viability"]["band"], 0) + 1
    drop_now = [{"symbol": p["symbol"], "action": p["guidance"]["action"], "why": p["guidance"]["why"]}
                for p in pos if p.get("guidance") and p["guidance"]["action"].startswith(("DROP", "TRIM"))]

    bias = "long" if pos_delta > 0.5 else "short" if pos_delta < -0.5 else "flat"
    read = (f"{len(pos)} option position(s), net {'+' if net_pnl >= 0 else ''}₹{round(net_pnl):,} · bleeding "
            f"₹{abs(round(theta_day)):,}/day to theta · net delta {round(pos_delta, 1)} ({bias} bias). "
            + (f"{len(drop_now)} flagged to drop/trim." if drop_now else "none flagged for immediate exit."))

    return {"ok": True, "n": len(pos), "net_pnl": round(net_pnl), "theta_day": round(theta_day),
            "pos_delta": round(pos_delta, 1), "capital_at_risk": round(capital),
            "by_underlying": by_und, "bands": bands, "drop_now": drop_now, "read": read,
            "positions": pos, "asof": b.get("asof"),
            "note": "Per-position DROP/HOLD/BOOK/TARGET/STOP from the viability + scope metrics; DROP-BY = ~4 "
                    "sessions before expiry (the cyclical theta-cliff exit, median −20% vs −73% held-through). "
                    "Aggregates greeks across the whole book.",
            "caveat": "Decision-support, not investment advice — I'm not a licensed advisor. READ-ONLY on your "
                      "Groww account: it computes the target/stop, YOU place any order. Odds are risk-neutral."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = portfolio()
    if not r.get("ok"):
        print("ERR:", r.get("error")); sys.exit()
    print(f"\n  ═══ OPTION PORTFOLIO — {r['asof']} ═══")
    print(f"  {r['read']}")
    print(f"  capital ₹{r['capital_at_risk']:,} · bands {r['bands']} · by-underlying {r['by_underlying']}")
    for p in r["positions"]:
        g = p.get("guidance") or {}
        t, s = g.get("target") or {}, g.get("stop") or {}
        print(f"\n  {p['symbol']}  [{p['viability']['band']}]  P&L {p['pnl']['pct']}%  ·  {p['trading_days_left']} sessions")
        print(f"    ▶ {g.get('action')} — {g.get('why')}")
        print(f"      TARGET ₹{t.get('opt')} ({'+' if _num(t.get('ret_pct'))>=0 else ''}{t.get('ret_pct')}% · spot {t.get('spot')})  ·  "
              f"STOP ₹{s.get('opt')} ({s.get('ret_pct')}%)  ·  DROP-BY {g.get('drop_by')}")
    print(f"\n  {r['caveat']}")
