"""
Pure, side-effect-free decision engine for the Groww Cloud execution script.

ANTI-"KNIGHT CAPITAL" DESIGN: this module makes NO API calls. decide() takes the
current broker state and returns a list of INTENDED actions (dicts). The live Cloud
script (and the simulator/tests) execute them separately. Because the logic is pure
and deterministic, it can be exhaustively backtested. Guardrails baked in:
  - IDEMPOTENT  : actions are a function of current state (positions + live SL trigger);
                  re-running the same state yields the same actions, no duplicates.
  - RATCHET-ONLY: a stop is only ever tightened, never loosened.
  - RUNAWAY CAP : if a cycle would emit more than max_actions, it HALTS instead of flooding.
  - KILL-SWITCH : daily loss beyond a limit flattens everything and halts.
  - HALT-STICKY : once halted, zero new orders until a human resets.
  - SANITY      : no orders on missing/zero data or nonsensical (wrong-side) stops.
Stdlib only -> safe to run inside Groww Cloud.
"""

DEFAULTS = dict(k_atr=2.5, be_at=1.0, max_actions=8, daily_loss_pct=3.0, eod_min=1520, min_move=0.0015)


def _act(kind, symbol="", side="", qty=0, trigger=0.0, reason=""):
    return {"kind": kind, "symbol": symbol, "side": side, "qty": int(qty),
            "trigger": round(float(trigger), 1), "reason": reason}


def decide_position(p, now_min, cfg):
    """One position -> list of intended actions. p needs: symbol, side('long'/'short'),
    qty, entry, price, atr, day_high, day_low, sl_trigger(float|None), product."""
    price, atr, qty = p.get("price"), p.get("atr"), p.get("qty", 0)
    if not price or price <= 0 or not atr or atr <= 0 or qty <= 0:      # SANITY: bad/missing data
        return []
    side, entry = p["side"], p["entry"]
    exit_side = "SELL" if side == "long" else "BUY"

    if p.get("product") == "MIS" and now_min >= cfg["eod_min"]:          # EOD square-off
        return [_act("EXIT_MKT", p["symbol"], exit_side, qty, reason="EOD square-off")]

    if side == "long":
        stop = p["day_high"] - cfg["k_atr"] * atr
        if price >= entry + cfg["be_at"] * atr:                         # BREAK-EVEN lock
            stop = max(stop, entry)
        breached, valid = price <= stop, stop < price
    else:
        stop = p["day_low"] + cfg["k_atr"] * atr
        if price <= entry - cfg["be_at"] * atr:
            stop = min(stop, entry)
        breached, valid = price >= stop, stop > price
    stop = round(stop, 1)

    if breached:                                                        # backup market exit
        return [_act("EXIT_MKT", p["symbol"], exit_side, qty, reason="stop breached")]
    if not valid:                                                       # SANITY: wrong-side stop
        return []

    cur = p.get("sl_trigger")
    if cur is None:
        return [_act("PLACE_SL", p["symbol"], exit_side, qty, stop, "initial SL")]
    better = (stop > cur) if side == "long" else (stop < cur)           # RATCHET-ONLY
    material = abs(stop - cur) / price >= cfg["min_move"]               # IDEMPOTENT: ignore tiny moves
    if better and material:
        return [_act("MODIFY_SL", p["symbol"], exit_side, qty, stop, "trail %.1f->%.1f" % (cur, stop))]
    return []


def decide(positions, now_min, day_pnl_pct, halted, cfg=None):
    """Top level. Returns (actions, new_halted)."""
    cfg = {**DEFAULTS, **(cfg or {})}
    if halted:                                                          # HALT-STICKY
        return [], True
    if day_pnl_pct <= -abs(cfg["daily_loss_pct"]):                      # KILL-SWITCH
        acts = [_act("EXIT_MKT", p["symbol"], "SELL" if p["side"] == "long" else "BUY",
                     p["qty"], reason="KILL-SWITCH daily loss") for p in positions if p.get("qty", 0) > 0]
        return acts[:cfg["max_actions"]] + [_act("HALT", reason="daily loss limit hit")], True
    acts = []
    for p in positions:
        acts += decide_position(p, now_min, cfg)
    if len(acts) > cfg["max_actions"]:                                  # RUNAWAY CAP
        return [_act("HALT", reason="action flood %d>%d -> halt for safety" % (len(acts), cfg["max_actions"]))], True
    return acts, False
