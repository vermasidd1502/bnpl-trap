"""
marleg_trade_manager.py — a script that MANAGES your orders to a strategy.

It does all the thinking — when the entry triggers, where the stop sits, when to TRAIL it, when to take profit —
and hands you the EXACT order to place at each step. It does NOT place, modify, or cancel a real order: your
account is read-only here and you tap the order on Groww yourself. (A bug in an auto-firer = real money gone,
irreversibly — so the trigger stays in your hand. Everything up to that, the script owns.)

Per name it runs a state machine off the strategy's trigger / stop / target (pulled from the target engine
when you don't pass your own), sized to your risk budget:
  WAIT    price below trigger        -> stage a buy at the trigger
  ENTER   price just cleared trigger  -> "BUY n @ ~px · SL x · TGT y"   (place it)
  LATE    price ran past the trigger  -> don't chase
  HOLD    in the trade                -> between stop & target
  TRAIL   +1R in profit               -> "move SL to z"                 (lock it in)
  TARGET  hit the target              -> "SELL n @ ~px  (+R)"
  STOP    hit the stop                -> "SELL n @ ~px  (-R)"

  python marleg_trade_manager.py RELIANCE         # candidate (manage an entry)
  python marleg_trade_manager.py TEJASNET 100 620 # held: 100 sh @ avg 620 (manage the exit)
"""
import sys
import math


def _live(sym):
    import marleg_vol as mv
    import marleg_options_monitor as mom
    try:
        p = mv.underlying_ltp(sym.upper(), mom._g())
        if p:
            return float(p)
    except Exception:
        pass
    try:
        import yfinance as yf
        h = yf.Ticker(mv._yf_symbol(sym.upper())).history(period="1d")["Close"].dropna()
        return float(h.iloc[-1]) if len(h) else None
    except Exception:
        return None


def manage_one(sym, qty=None, avg=None, entry=None, stop=None, target=None, risk_rs=None, trail="R"):
    """avg!=None => you HOLD it (manage the exit). avg==None => candidate (manage the entry)."""
    sym = sym.upper()
    px = _live(sym)
    if not px:
        return {"ok": False, "sym": sym, "error": "no live price"}
    # fill the strategy levels (+ status, atr) from the target engine if not supplied
    tg_status, atr = None, None
    if entry is None or stop is None or target is None:
        try:
            import marleg_target as tg
            t = tg.target(sym)
            if t.get("ok"):
                entry = entry if entry is not None else (t.get("trigger") or px)
                stop = stop if stop is not None else t.get("stop")
                target = target if target is not None else t.get("target")
                tg_status, atr = t.get("status"), t.get("atr")
        except Exception:
            pass
    entry = entry or px
    held = avg is not None
    if not target:
        return {"ok": False, "sym": sym, "error": "no target (pass one or ensure the target engine has levels)"}
    # the stop MUST sit on the loss side of your anchor (below avg if held, below entry if buying)
    anchor = avg if held else entry
    if not stop or stop >= anchor:
        atr = atr or anchor * 0.02
        stop = round(anchor - 1.5 * atr, 1)
    per_share_risk = max(anchor - stop, 0.01)
    if qty is None and risk_rs:
        qty = int(risk_rs // per_share_risk)

    state, action, new_stop = "HOLD", "", None
    if held:
        rmult = (px - avg) / per_share_risk
        if px <= stop:
            state, action = "STOP", f"SELL {qty or ''} {sym} @ ~₹{px:.1f} — stop hit ({rmult:+.1f}R). Exit; no averaging down."
        elif px >= target:
            state, action = "TARGET", f"SELL {qty or ''} {sym} @ ~₹{px:.1f} — target hit ({rmult:+.1f}R). Bank it or trail a runner."
        elif rmult >= 2 and stop < avg + per_share_risk:
            new_stop = round(avg + per_share_risk, 1); state = "TRAIL"
            action = f"MOVE STOP to ₹{new_stop} (+1R locked) — {sym} is {rmult:.1f}R up."
        elif rmult >= 1 and stop < avg:
            new_stop = round(avg, 1); state = "TRAIL"
            action = f"MOVE STOP to breakeven ₹{new_stop} — {sym} is {rmult:.1f}R up; the trade is now free."
        else:
            state, action = "HOLD", f"HOLD {sym} — {rmult:+.1f}R, between stop ₹{stop} and target ₹{round(target,1)}. Do nothing."
    else:
        if tg_status == "LATE":
            state, action = "LATE", f"LATE — the target engine flags {sym} extended (already ran). Don't chase; wait for a fresh base/pullback toward ₹{round(entry,1)}."
        elif px < entry * 0.999:
            state, action = "WAIT", (f"WAIT — stage a buy of {qty or '<n>'} {sym} at the trigger ₹{round(entry,1)} "
                                     f"(SL ₹{stop}, TGT ₹{round(target,1)}). {((entry/px-1)*100):+.1f}% away.")
        elif px <= entry * 1.02:
            state, action = "ENTER", (f"ENTER NOW: BUY {qty or '<n>'} {sym} @ ~₹{px:.1f} · SL ₹{stop} · TGT ₹{round(target,1)} "
                                      f"· risk ₹{per_share_risk:.1f}/sh · R:R {((target-entry)/per_share_risk):.1f}")
        else:
            state, action = "LATE", f"LATE — {sym} is {((px/entry-1)*100):+.1f}% past the trigger ₹{round(entry,1)}. Don't chase; wait for a pullback."
    rr = round((target - entry) / per_share_risk, 2) if per_share_risk else None
    return {"ok": True, "sym": sym, "price": round(px, 2), "held": held, "qty": qty, "avg": avg,
            "entry": round(entry, 1), "stop": round(stop, 1), "target": round(target, 1),
            "new_stop": new_stop, "per_share_risk": round(per_share_risk, 2), "rr": rr,
            "state": state, "action": action,
            "note": "Read-only: this computes the order; YOU place it on Groww. Never auto-executes."}


def manage(orders, risk_rs=None):
    """orders: list of dicts {sym, qty?, avg?, entry?, stop?, target?}."""
    out = []
    for o in orders:
        out.append(manage_one(o.get("sym"), qty=o.get("qty"), avg=o.get("avg"), entry=o.get("entry"),
                              stop=o.get("stop"), target=o.get("target"), risk_rs=o.get("risk_rs", risk_rs)))
    out = [x for x in out if x.get("ok")]
    rank = {"STOP": 0, "TARGET": 1, "ENTER": 2, "TRAIL": 3, "WAIT": 4, "HOLD": 5, "LATE": 6}
    out.sort(key=lambda x: rank.get(x["state"], 9))
    actionable = [x for x in out if x["state"] in ("STOP", "TARGET", "ENTER", "TRAIL")]
    return {"ok": True, "n": len(out), "n_actionable": len(actionable), "orders": out,
            "note": "Trade manager: states sorted by urgency. STOP/TARGET/ENTER/TRAIL need an action from you; "
                    "HOLD/WAIT/LATE do not. It NEVER places an order — you execute on Groww."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    a = sys.argv[1:]
    if not a:
        a = ["RELIANCE"]
    sym = a[0]
    qty = int(a[1]) if len(a) > 1 else None
    avg = float(a[2]) if len(a) > 2 else None
    r = manage_one(sym, qty=qty, avg=avg)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n  {r['sym']}  ₹{r['price']}  [{r['state']}]{'  (HELD '+str(r['qty'])+' @ '+str(r['avg'])+')' if r['held'] else ''}")
    print(f"  entry ₹{r['entry']} · stop ₹{r['stop']} · target ₹{r['target']} · R:R {r['rr']}")
    print(f"\n  ➤ {r['action']}")
    print(f"\n  {r['note']}")
