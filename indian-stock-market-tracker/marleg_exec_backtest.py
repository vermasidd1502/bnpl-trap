"""
Backtest the LOGIC (not the alpha) of the execution engine. These are the
anti-Knight-Capital safety properties — each must PASS before live deployment.
Run: python marleg_exec_backtest.py
"""
import marleg_exec_logic as E

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    PASS += cond; FAIL += (not cond)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (("  -> " + str(detail)) if (detail and not cond) else ""))

def pos(**kw):
    b = dict(symbol="X", side="long", qty=10, entry=100.0, price=100.0, atr=4.0,
             day_high=100.0, day_low=100.0, sl_trigger=None, product="CNC")
    b.update(kw); return b

print("EXECUTION-LOGIC SAFETY TESTS")
print("-" * 70)

# 1 — initial SL placed on the protective side
a, _ = E.decide([pos(price=110, day_high=110)], 1000, 0, False)
check("places initial SL below price (long)", len(a) == 1 and a[0]["kind"] == "PLACE_SL" and a[0]["trigger"] < 110, a)

# 2 — ratchet UP when favorable; never loosen
a, _ = E.decide([pos(price=120, day_high=120, sl_trigger=100)], 1000, 0, False)
check("trails stop UP when favorable", a and a[0]["kind"] == "MODIFY_SL" and a[0]["trigger"] > 100, a)
a, _ = E.decide([pos(price=112, day_high=115, sl_trigger=110)], 1000, 0, False)   # computed 105 < cur 110
check("NEVER loosens the stop", a == [], a)

# 3 — idempotency: same state twice -> identical; sub-threshold move -> no modify
s = pos(price=120, day_high=120, sl_trigger=109.9)   # computed 110, better by 0.1 (<0.15% of 120)
a1, _ = E.decide([s], 1000, 0, False); a2, _ = E.decide([s], 1000, 0, False)
check("idempotent (same state -> same actions)", a1 == a2)
check("no modify on sub-threshold move (anti-spam)", a1 == [], a1)

# 4 — exit on breach
a, _ = E.decide([pos(price=95, day_high=120, sl_trigger=110)], 1000, 0, False)
check("EXIT_MKT on stop breach", bool(a) and a[0]["kind"] == "EXIT_MKT", a)

# 5 — kill-switch flattens all + halts, and stays halted
a, h = E.decide([pos(qty=10), pos(symbol="Y", qty=5)], 1000, -3.5, False)
check("kill-switch: flatten ALL + HALT", sum(x["kind"] == "EXIT_MKT" for x in a) == 2 and any(x["kind"] == "HALT" for x in a) and h, a)
a2, h2 = E.decide([pos()], 1000, 0, True)
check("HALT is sticky (no orders after halt)", a2 == [] and h2, a2)

# 6 — runaway cap: many wanted orders -> HALT, not a flood
many = [pos(symbol="S%d" % i, price=120, day_high=120, sl_trigger=100) for i in range(20)]
a, h = E.decide(many, 1000, 0, False)
check("runaway cap -> single HALT (no flood)", len(a) == 1 and a[0]["kind"] == "HALT" and h, len(a))

# 7 — bad/missing data -> no orders
bad_any = any(E.decide([b], 1000, 0, False)[0] for b in [pos(price=None), pos(price=0), pos(atr=0), pos(qty=0)])
check("bad/missing data -> zero orders", not bad_any)

# 8 — break-even lock (stop >= entry once +1 ATR in profit)
a, _ = E.decide([pos(price=106, day_high=106, entry=100, sl_trigger=None)], 1000, 0, False)
check("break-even lock (stop >= entry)", bool(a) and a[0]["trigger"] >= 100, a)

# 9 — EOD square-off only for MIS
a, _ = E.decide([pos(product="MIS")], 1520, 0, False)
b, _ = E.decide([pos(product="CNC")], 1520, 0, False)
check("EOD square-off for MIS, not CNC", a and a[0]["kind"] == "EXIT_MKT" and (not b or b[0]["kind"] != "EXIT_MKT"), (a, b))

# 10 — short side mirrors (ratchet DOWN, exit on rise through stop)
a, _ = E.decide([pos(side="short", price=80, day_low=80, entry=100, sl_trigger=95)], 1000, 0, False)
check("short: trails stop DOWN", a and a[0]["kind"] == "MODIFY_SL" and a[0]["trigger"] < 95, a)

# ---- real-data trail simulation (does it behave on an actual price path?) ----
print("\nREAL-DATA TRAIL SIMULATION")
print("-" * 70)
try:
    import yfinance as yf
    df = yf.Ticker("RELIANCE.NS").history(period="1y")
    close, high, low = df["Close"].values, df["High"].values, df["Low"].values
    entry = float(close[0]); atr = float((df["High"] - df["Low"]).rolling(14).mean().iloc[20]) or entry * 0.02
    hwm, sl, exitpx, trig = entry, None, None, []
    for i in range(1, len(close)):
        px = float(close[i]); hwm = max(hwm, float(high[i]))
        p = pos(symbol="REL", entry=entry, price=px, atr=atr, day_high=hwm, sl_trigger=sl)
        acts, _ = E.decide([p], 1000, 0, False)
        for x in acts:
            if x["kind"] in ("PLACE_SL", "MODIFY_SL"): sl = x["trigger"]; trig.append(sl)
            if x["kind"] == "EXIT_MKT": exitpx = px
        if exitpx: break
    mono = all(trig[k] >= trig[k - 1] for k in range(1, len(trig)))
    check("real path: SL strictly non-decreasing (no loosening)", mono)
    pnl = ((exitpx or float(close[-1])) / entry - 1) * 100
    print(f"  RELIANCE 1y long: entry {entry:.0f} -> {'EXIT @'+str(round(exitpx)) if exitpx else 'open @'+str(round(close[-1]))} | P&L {pnl:+.1f}% | {len(trig)} stop-raises | final SL {sl}")
except Exception as e:
    print("  (skipped real-data sim:", str(e)[:60], ")")

print("\n" + "=" * 70)
print(f"RESULT: {PASS} passed, {FAIL} failed  ->  {'SAFE TO PROCEED (logic verified)' if FAIL == 0 else 'DO NOT DEPLOY — fix failures first'}")
