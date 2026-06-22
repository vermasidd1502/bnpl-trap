"""
marleg_target.py — treat every stock as NUMBERS, not a story. The systematic targeting engine.

Reduce any stock to four numbers + a status, so you act mechanically (no chasing, no emotion):
  TRIGGER  = entry number    — the level price must CLEAR for the move to be on (nearest resistance / pivot)
  TARGET   = objective       — the next resistance above the trigger (a measured move)
  STOP     = risk number     — invalidation (nearest support, or trigger minus a buffer)
  STATUS   = where price sits vs its trigger:
       FAR       >4% below trigger          — nothing to do
       WATCH     1.5-4% below               — on the radar
       ARMED     within 1.5% below          — about to fire; stage the order
       FIRED     just cleared, still near it — the actionable window
       LATE      >2% above the trigger      — the move already ran -> DO NOT CHASE (this is TEJAS now)
       BLUE-SKY  new highs, no overhead R    — momentum, but no defined entry/risk
       STOPPED   below the stop/support      — out / avoid
Plus R:R and a 0-100 conviction (momentum x room x volume). board() scans a list and floats the ACTIONABLE
names (ARMED / FIRED) to the top. Read-only decision-support — you place the orders; this never trades.

  python marleg_target.py TEJASNET RELIANCE BHEL
"""
import sys
import numpy as np

STATUS_RANK = {"FIRED": 0, "ARMED": 1, "WATCH": 2, "BLUE-SKY": 3, "FAR": 4, "LATE": 5, "STOPPED": 6, "NA": 9}


def _bars(tk, period="1y"):
    import marleg_vol as mv
    import yfinance as yf
    h = yf.Ticker(mv._yf_symbol(tk.upper())).history(period=period)
    return h[["Open", "High", "Low", "Close", "Volume"]].dropna() if len(h) else None


def target(tk):
    import marleg_levels as lv
    L = lv.levels(tk)
    if not L.get("ok"):
        return {"ok": False, "tk": tk.upper(), "error": L.get("error", "no levels")}
    spot, atr = L["spot"], (L.get("atr") or L["spot"] * 0.02)
    R = L.get("nearest_resistance"); S = L.get("nearest_support")
    res = L.get("resistances") or []
    trigger = R["price"] if R else None
    stop = S["price"] if S else round(spot - 1.5 * atr, 1)

    # target = next resistance above the trigger; else project the range (trigger + (trigger-stop))
    above = [x["price"] for x in res if trigger and x["price"] > trigger + 0.2 * atr]
    if above:
        tgt = above[0]
    elif trigger:
        tgt = round(trigger + (trigger - stop), 1)
    else:
        tgt = round(spot + 2 * atr, 1)

    # momentum / room / volume FIRST — the run + room feed the "extended -> don't chase" overlay
    h = _bars(tk)
    conv, mom20, room, relvol, extended = 0, None, None, None, False
    if h is not None and len(h) > 60:
        C, V, Hi, Lo = h["Close"].values, h["Volume"].values, h["High"].values, h["Low"].values
        mom20 = round(float(C[-1] / C[-21] - 1) * 100, 1)
        hi120 = float(Hi[-120:].max()); lo120 = float(Lo[-120:].min())
        room = round((hi120 / spot - 1) * 100, 1)                          # headroom to the 120d high
        off_low = (spot / lo120 - 1) * 100 if lo120 > 0 else 0             # how far it's run off the 120d low
        relvol = round(float(V[-1]) / (float(V[-21:-1].mean()) or V[-1]), 2)
        extended = bool((mom20 > 12 and room < 6) or (room < 3 and off_low > 30))   # ran far, little room left
        s_mom = max(0, min(40, mom20 * 2)); s_room = max(0, min(35, room * 2.5))
        s_vol = max(0, min(25, (relvol - 0.8) * 35)); conv = round(s_mom + s_room + s_vol)

    # status from price vs trigger, with the EXTENDED overlay (the anti-chase — e.g. TEJAS after its run)
    if spot < stop:
        status = "STOPPED"
    elif trigger is None:
        status = "BLUE-SKY"
    else:
        d = (trigger / spot - 1) * 100                      # % above spot the trigger sits (+ = below trigger)
        if spot > trigger * 1.02:
            status = "LATE"
        elif spot >= trigger:
            status = "FIRED"
        elif d <= 1.5:
            status = "ARMED"
        elif d <= 4.0:
            status = "WATCH"
        else:
            status = "FAR"
    if extended and status in ("FIRED", "ARMED", "WATCH", "BLUE-SKY"):
        status = "LATE"                                     # ran too far off its base — don't chase the move
    dist_trig = round((trigger / spot - 1) * 100, 2) if trigger else None
    rr = round((tgt - trigger) / (trigger - stop), 2) if (trigger and trigger > stop and tgt > trigger) else None

    note = {"FIRED": "cleared the trigger and still near it — the actionable window",
            "ARMED": "within 1.5% of the trigger — stage the order",
            "WATCH": "approaching the trigger — radar",
            "LATE": "already ran past the trigger — DO NOT CHASE",
            "BLUE-SKY": "new highs, no resistance overhead — momentum but no defined risk",
            "STOPPED": "below support/stop — out / avoid",
            "FAR": "well below the trigger — nothing to do"}.get(status, "")
    return {"ok": True, "tk": tk.upper(), "spot": spot, "trigger": trigger, "target": tgt, "stop": stop,
            "status": status, "status_note": note, "dist_to_trigger_pct": dist_trig, "rr": rr,
            "atr": atr, "mom20_pct": round(mom20, 1) if mom20 is not None else None,
            "room_pct": room, "relvol": relvol, "conviction": conv}


def board(names=None, top=None):
    if not names:
        names = ["RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "INFY", "TCS", "TATASTEEL", "HINDALCO",
                 "BHEL", "TATAMOTORS", "AXISBANK", "LT", "TITAN", "TEJASNET", "BEL", "HAL", "DIXON", "TRENT"]
    out = [target(t) for t in names]
    out = [x for x in out if x.get("ok")]
    out.sort(key=lambda x: (STATUS_RANK.get(x["status"], 9), -(x.get("conviction") or 0)))
    if top:
        out = out[:top]
    return {"ok": True, "n": len(out), "rows": out,
            "note": "Every stock as numbers: clear the TRIGGER -> ride to TARGET, invalid below STOP. Act on "
                    "STATUS (ARMED/FIRED), never on a story. LATE = the move already ran; don't chase. "
                    "Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    names = sys.argv[1:] or None
    b = board(names)
    print(f"\n  {'STOCK':<11} {'STATUS':<9} {'SPOT':>8} {'TRIGGER':>8} {'TARGET':>8} {'STOP':>8} {'R:R':>5} {'CONV':>4}  note")
    for x in b["rows"]:
        print(f"  {x['tk']:<11} {x['status']:<9} {x['spot']:>8} {str(x['trigger'] or '-'):>8} {str(x['target']):>8} "
              f"{str(x['stop']):>8} {str(x['rr'] or '-'):>5} {str(x['conviction']):>4}  {x['status_note']}")
