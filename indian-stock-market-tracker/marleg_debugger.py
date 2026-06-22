"""
marleg_debugger.py — the TRADE-DISCIPLINE DEBUGGER: when a trade goes wrong, what RULE did you break?

It judges the DECISION (process), not the P&L (outcome) — a clean trade can still lose (variance) and a
sloppy one can win (luck). It reconstructs the entry context and checks your trade against the pod's
VALIDATED rules, flagging each violation with the fix + the backtest behind it. Read-only coach; it never
trades.

  python marleg_debugger.py GAIL LONG
  python marleg_debugger.py SBIN LONG --entry 1040 --qty 200 --capital 500000 --stop 1060 --outcome -3
"""
import sys


def review(sym, side="LONG", entry=None, qty=None, capital=None, stop=None, outcome_pct=None):
    import marleg_target as tg
    import marleg_pullback as pbk
    try:
        import marleg_overnight_gate as og
        bull = og._regime_bull()
    except Exception:
        bull = True
    sym, side = sym.upper(), side.upper()
    t = tg.target(sym)
    pb = pbk.signal(sym)
    spot = (t.get("spot") if t.get("ok") else None) or (pb.get("spot") if pb.get("ok") else None)
    entry = entry if entry is not None else spot
    v = []

    def add(sev, what, fix, why):
        v.append({"severity": sev, "what": what, "fix": fix, "evidence": why})

    if side in ("SHORT", "SELL"):
        add("HIGH", "You went SHORT.", "Long-or-cash; for downside use market-neutral PAIRS or an index hedge.",
            "Every short category tested negative net (−1% to −2.6%/10d) — India drifts up; shorting never paid.")
    if not bull:
        add("HIGH", "Bought in a BEAR/▼ regime.", "Stand aside or hedge until NIFTY > 50DMA.",
            "Bull-gating the book ~halves drawdown; the long edge is negative in bear.")
    if (t.get("ok") and t.get("status") == "LATE") or pb.get("extended"):
        add("HIGH", "Chased an EXTENDED / LATE name (it had already run).",
            "Wait for a pullback to the buy-zone; never buy the chase.",
            "Extended entries underperform; the edge is the DIP in an uptrend (+1.75%/10d), not the chase.")
    if pb.get("ok") and not pb.get("trend_up"):
        add("MED", "Bought OUTSIDE an uptrend (below / falling 50-EMA).",
            "Only buy dips when price is above a rising 50-EMA.",
            "Dips in flat/down regimes LOSE (+0.9% flat / +0.4% down vs +1.75% in an uptrend).")
    if t.get("ok") and (t.get("room_pct") if t.get("room_pct") is not None else 9) < 2:
        add("MED", "Bought INTO resistance (little room to a target).", "Buy with room; book/trail at the prior high.",
            "A target set past the prior high is reached only ~52% of the time.")
    if t.get("ok") and (t.get("conviction") or 0) < 35:
        add("LOW", "Thin setup (low conviction / confluence).", "Require 3+ confluence legs before risking capital.",
            "Confluence pays and saturates at 3 legs; the 0–1-leg zone is negative.")
    if stop is None:
        add("MED", "No stop defined.", "Define the invalidation BEFORE entry.", "Undefined risk is the #1 account-killer.")
    if capital and stop and entry and qty:
        rp = abs(entry - stop) * qty / capital * 100
        if rp > 2:
            add("HIGH", f"Over-sized: risked {rp:.1f}% of capital on one trade.",
                "Size from risk: qty = 1% × capital ÷ (entry − stop).",
                "The 1% rule caps any single stop-out from hurting the account.")

    hi = sum(x["severity"] == "HIGH" for x in v)
    grade = "F" if hi >= 2 else "D" if hi == 1 else "C" if len(v) >= 2 else "B" if v else "A"
    if outcome_pct is not None:
        if v and outcome_pct < 0:
            lesson = "LOST and broke rules → a REPEATABLE mistake. Fix the process, this will keep happening."
        elif v and outcome_pct >= 0:
            lesson = "WON, but broke rules → that was LUCK, not skill. Don't let the win reinforce the bad habit."
        elif not v and outcome_pct < 0:
            lesson = "Clean process, still LOST → that's VARIANCE, not a mistake. Do NOT change the process over one loss."
        else:
            lesson = "Clean process AND won → this is exactly what to repeat."
    else:
        lesson = "Clean — no rule violations at entry." if not v else f"{len(v)} issue(s) at entry — see the fixes."
    return {"ok": True, "sym": sym, "side": side, "entry": round(entry, 2) if entry else None, "regime_bull": bull,
            "grade": grade, "n": len(v), "violations": v, "lesson": lesson,
            "note": "Judges the DECISION (process) against the pod's validated rules — NOT the P&L. "
                    "A clean trade can still lose; a sloppy one can win. Read-only coach."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    a = sys.argv[1:]
    sym = a[0] if a else "GAIL"
    side = a[1] if len(a) > 1 and not a[1].startswith("--") else "LONG"
    kw = {}
    for i, x in enumerate(a):
        if x in ("--entry", "--qty", "--capital", "--stop", "--outcome") and i + 1 < len(a):
            kw[x[2:]] = float(a[i + 1])
    r = review(sym, side, entry=kw.get("entry"), qty=kw.get("qty"), capital=kw.get("capital"),
               stop=kw.get("stop"), outcome_pct=kw.get("outcome"))
    icon = {"HIGH": "🔴", "MED": "🟠", "LOW": "🟡"}
    print(f"\n  TRADE REVIEW · {r['sym']} {r['side']}  ·  PROCESS GRADE: {r['grade']}  ({r['n']} issue(s))")
    print(f"  ➤ {r['lesson']}\n")
    for x in r["violations"]:
        print(f"  {icon.get(x['severity'],'·')} {x['what']}")
        print(f"      fix:  {x['fix']}")
        print(f"      why:  {x['evidence']}\n")
    if not r["violations"]:
        print("  ✅ No rule violations at entry — the process was clean.\n")
    print(f"  {r['note']}")
