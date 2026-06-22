"""
marleg_desk.py — THE UNIFIED TRADE DESK: every validated edge in the pod, assembled into one call per name.

  GATE     should I even be in this?   -> bull-regime × setup confluence (the validated factors), not LATE
  PLAN     the numbers                  -> trigger / stop / target / R:R + position size from your risk budget
  EXECUTE  how to get in/out clean      -> scale-in (climb) + scale-out (offload) ladders, POV-sized
  MANAGE   the live lifecycle           -> WAIT/ENTER/HOLD/TRAIL/TARGET/STOP + exit overlays

Built on the session's backtested truths: bull-regime gate (the risk-adjusted edge), buy STRENGTH not the
bounce (higher-lows + accumulation + RSI>60, NOT recovering-from-bear), don't chase the EXTENDED, Supertrend
+EMA is an EXIT/confluence leg (not a standalone engine), scale into resistance. Read-only — you place the
orders; this NEVER auto-executes.

  python marleg_desk.py RELIANCE 40000           # candidate, plan a 40k campaign
  python marleg_desk.py TEJASNET 100 560         # held 100 @ 560 -> manage it
"""
import sys


def desk(sym, qty=None, avg=None, risk_rs=None):
    import marleg_target as tg
    import marleg_trade_manager as tm
    try:
        import marleg_overnight_gate as og
        bull = og._regime_bull()
    except Exception:
        bull = True
    sym = sym.upper()
    t = tg.target(sym)
    if not t.get("ok"):
        return {"ok": False, "sym": sym, "error": t.get("error", "no read")}
    try:
        import marleg_pullback as pbk
        pb = pbk.signal(sym)                      # the VALIDATED entry: dip in an uptrend
    except Exception:
        pb = {}
    status, conv = t.get("status"), t.get("conviction", 0)
    mom, room, relvol = t.get("mom20_pct"), t.get("room_pct"), t.get("relvol")
    held = avg is not None

    # --- GATE: regime × setup confluence (the validated legs) ---
    legs = []
    if (mom or 0) > 0: legs.append("momentum↑")
    if (relvol or 0) > 1.2: legs.append("volume↑")
    if (room or 0) > 3: legs.append("room↑")
    if status in ("ARMED", "FIRED"): legs.append("at-trigger")
    if bull: legs.append("bull-regime")
    conf = len(legs)
    if not bull:
        gate, why = "STAND-ASIDE", "bear/▼ regime — the long edge turns negative here; hold cash or manage exits only"
    elif status == "LATE":
        gate, why = "STAND-ASIDE", "already ran past the trigger (extended) — do NOT chase; wait for a fresh base"
    elif status == "STOPPED":
        gate, why = "STAND-ASIDE", "below support — avoid"
    elif conf >= 3:
        gate, why = "TRADE", f"{conf}/5 confluence ({', '.join(legs)})"
    else:
        gate, why = "WEAK", f"only {conf}/5 confluence ({', '.join(legs) or 'none'}) — thin; size down or pass"

    # --- PLAN + MANAGE: the numbers and the live action ---
    m = tm.manage_one(sym, qty=qty, avg=avg, risk_rs=risk_rs)
    plan = {k: m.get(k) for k in ("entry", "stop", "target", "rr", "per_share_risk", "qty")} if m.get("ok") else {}
    state = m.get("state") if m.get("ok") else None
    action = m.get("action") if m.get("ok") else m.get("error")

    # --- EXECUTE: scaling ladders (when there's a real size to work) ---
    camp = None
    if qty and not held and gate in ("TRADE", "WEAK") and state in ("ENTER", "WAIT"):
        try:
            import marleg_scaling as sc
            c = sc.campaign(sym, total_qty=int(qty))
            if c.get("ok"):
                camp = {"pct_of_adv": c["pct_of_adv"], "climb_in": c["climb_in"], "offload_out": c["offload_out"],
                        "slippage_saved_rs": c["impact"]["slippage_saved_rs"]}
        except Exception:
            pass

    # --- headline: fuse gate + lifecycle into the single next move ---
    if gate == "STAND-ASIDE" and not held:
        headline = f"STAND ASIDE — {why}"
    elif held:
        headline = action  # the manager owns held names (TRAIL/TARGET/STOP/HOLD)
    elif not pb.get("trend_up", True):
        headline = f"WAIT — not a clean uptrend ({pb.get('state', '—')}); the validated buy is a DIP in an UPTREND, not this"
    elif pb.get("state") == "DIP-READY":
        headline = (f"✅ DIP-BUY: buy {sym} now ~₹{m.get('price')} — VALIDATED pullback-in-uptrend · "
                    f"SL ₹{plan.get('stop')} · TGT ₹{plan.get('target')} · R:R {plan.get('rr')}")
    elif pb.get("state") in ("EXTENDED", "TRENDING"):
        headline = f"WAIT for the dip — {pb.get('note')} (don't chase ₹{m.get('price')})"
    else:
        headline = action

    return {"ok": True, "sym": sym, "price": m.get("price"), "held": held,
            "gate": gate, "gate_why": why, "confluence": conf, "legs": legs, "conviction": conv,
            "regime_bull": bull, "status": status, "state": state, "plan": plan,
            "pullback": pb.get("state"), "buy_zone": pb.get("buy_zone"),
            "scaling": camp, "headline": headline, "action": action,
            "note": "Unified desk: gate (regime×confluence) → plan (numbers+size) → execute (scale in/out) → "
                    "manage (lifecycle). Decision-support; you place the orders. Never auto-executes."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    a = sys.argv[1:] or ["RELIANCE", "40000"]
    sym = a[0]
    qty = int(a[1]) if len(a) > 1 else None
    avg = float(a[2]) if len(a) > 2 else None
    r = desk(sym, qty=qty, avg=avg)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    p = r["plan"]
    print(f"\n  {r['sym']}  ₹{r['price']}  ·  GATE: {r['gate']} ({r['gate_why']})")
    print(f"  regime {'BULL' if r['regime_bull'] else 'BEAR/▼'} · status {r['status']} · confluence {r['confluence']}/5 · conviction {r['conviction']}")
    print(f"  PLAN: entry ₹{p.get('entry')} · stop ₹{p.get('stop')} · target ₹{p.get('target')} · R:R {p.get('rr')}")
    if r["scaling"]:
        s = r["scaling"]; print(f"  EXECUTE: {s['pct_of_adv']}% of ADV · climb {len(s['climb_in'])} tranches · offload {len(s['offload_out'])} tranches · ~₹{s['slippage_saved_rs']:,} saved vs dumping")
    print(f"\n  ➤ {r['headline']}")
    print(f"\n  {r['note']}")
