"""
marleg_suggest.py — SUGGESTION BOX for the Stock Lab / simulator.

"What's even worth simulating?" → a ranked list of candidates, each carrying TWO stamps:
  • GATED PASS    — which validated gates it clears right now (industry leading · disciplined U/D ·
                    news-clean · fib strength), as a checklist (e.g. 4/4).
  • BACKTESTED PASS — the historical edge of its SETUP type from our 5y backtests (cup-handle confirmed
                    62-67% · bullish-reversal candle ~55% · gated leader pullback) — honest win%/net.

Base candidates come from the auto-pilot's consolidator (cup-handle CONFIRMED + reversal PRIME + gated
leaders), so the box and the auto-pilot agree on what's high-conviction. One click adds to the simulator.
Read-only, cache-driven (fast). Not advice.

  python marleg_suggest.py
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))

# backtested edge per setup type (from marleg_pattern_study / cuphandle_study / strat-lab — the validated edges)
BT = {
    "cup-handle": {"win": 65, "net": 4.4, "hold": "2-4 wks", "label": "confirmed cup-handle (5y; raw breakout 51% → HELD breakout 62-67%)"},
    "reversal":   {"win": 55, "net": 0.71, "hold": "5-10 d", "label": "bullish reversal candle (5y; Hammer 55%/+0.71%, all bearish folklore)"},
    "gated":      {"win": 54, "net": 0.95, "hold": "2-4 wks", "label": "gated leader on a 5d pullback (gate_pullback; the validated swing entry)"},
}


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def suggest(top=15):
    import marleg_autopilot as ap
    base = ap.signals()
    gc = _load("marleg_gated_cache.json")
    picks = {p.get("s"): p for p in gc.get("picks", [])}
    leadset = {str(x.get("group") or "").lower() for x in gc.get("leading_industries", [])}
    SECT = _load("marleg_sectors.json")
    out = []
    for sg in base[:30]:
        s = sg["s"]; gp = picks.get(s, {})
        ind = gp.get("industry") or (SECT.get(s, {}) or {}).get("industry")
        ud = gp.get("ud"); fib = gp.get("fib")
        clean = (sg.get("clean") is not False) and (gp.get("clean") is not False)
        gates = [
            {"k": "industry leading", "ok": bool(ind and str(ind).lower() in leadset)},
            {"k": "U/D disciplined (1.3-4)", "ok": bool(ud is not None and 1.3 <= ud < 4)},
            {"k": "news-clean", "ok": bool(clean)},
            {"k": "fib strength (≥0.6)", "ok": bool(fib is not None and fib >= 0.6)},
        ]
        src = (sg.get("sources") or ["gated"])[0]
        bt = BT.get(src, BT["gated"])
        passed = sum(1 for g in gates if g["ok"])
        out.append({"s": s, "conv": sg.get("conv"), "thesis": sg.get("thesis"), "sources": sg.get("sources", []),
                    "entry": sg.get("entry"), "target": sg.get("target"), "stop": sg.get("stop"),
                    "payout_pct": sg.get("payout_pct"), "horizon": sg.get("horizon"), "industry": ind,
                    "gates": gates, "gates_passed": passed, "gates_total": len(gates), "backtest": bt})
    # rank: gate-pass count first, then conviction
    out.sort(key=lambda x: (-x["gates_passed"], -(x["conv"] or 0)))
    return {"ok": True, "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"), "n": len(out),
            "suggestions": out[:top],
            "note": "Each candidate clears live GATES (checklist) and carries its SETUP's BACKTESTED edge. "
                    "Backtest = the setup's historical odds, not a promise on this name. ➕ adds to your simulator. Not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = suggest()
    print(f"\nSUGGESTION BOX — {r['asof']} ({r['n']} candidates)\n")
    for x in r["suggestions"]:
        g = "".join("✓" if gg["ok"] else "·" for gg in x["gates"])
        print(f"  {x['s']:<11} conv {x['conv']:>3} gates {x['gates_passed']}/4 [{g}] · {'+'.join(x['sources'])} · "
              f"bt ~{x['backtest']['win']}% · entry ₹{x['entry']}→₹{x['target']} (+{x['payout_pct']}%)")
        print(f"       {x['thesis']}")
