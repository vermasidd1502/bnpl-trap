"""
marleg_simulator.py — YOUR paper TRACKER / simulator (manual picks).

You add a stock from anywhere (a pod row, the dossier, the Stock Lab). It opens a paper position at the
live price and tracks it live — entry / now / P&L — and re-runs the cross-engine all-sides verdict on each
view, so you watch YOUR picks the way the auto-pilot watches its own signals, with every angle reconciled.

This is distinct from the auto-pilot (which trades signals on its own); the simulator holds what YOU chose.
Paper only — marks a local book, never sends an order.

  add(tk) / remove(tk) / view()
"""
import json
import os
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
BOOK = os.path.join(HERE, "marleg_simulator_book.json")
START = 100000.0
ALLOC = 0.10


def _book():
    try:
        return json.load(open(BOOK, encoding="utf-8"))
    except Exception:
        return {"start": START, "positions": [], "closed": []}


def _save(b):
    try:
        tmp = BOOK + ".tmp"; json.dump(b, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        os.replace(tmp, BOOK)
    except Exception:
        pass


def _live(tk):
    try:
        import marleg_live
        lp = marleg_live.price(tk)
        return lp.get("price") if lp.get("ok") else None
    except Exception:
        return None


def add(tk, note=None):
    tk = tk.upper()
    b = _book()
    if any(p["s"] == tk for p in b["positions"]):
        return {"ok": False, "msg": f"{tk} already in the simulator"}
    px = _live(tk)
    if not px:
        return {"ok": False, "msg": f"no live price for {tk}"}
    qty = int((b.get("equity", b["start"]) * ALLOC) // px) or 1
    b["positions"].append({"s": tk, "entry": round(px, 1), "qty": qty, "note": note or "",
                           "opened": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")})
    _save(b)
    return {"ok": True, "msg": f"added {tk} @ ₹{round(px,1)} ({qty} sh, paper)"}


def remove(tk):
    tk = tk.upper()
    b = _book()
    n = len(b["positions"])
    b["positions"] = [p for p in b["positions"] if p["s"] != tk]
    _save(b)
    return {"ok": len(b["positions"]) < n, "msg": f"removed {tk}" if len(b["positions"]) < n else f"{tk} not in simulator"}


def view():
    import marleg_analyze
    b = _book()
    rows, upnl = [], 0.0
    for p in b["positions"]:
        a = {}
        try:
            a = marleg_analyze.analyze(p["s"])
        except Exception:
            pass
        now = a.get("price") or _live(p["s"]) or p["entry"]
        pnl = round((now - p["entry"]) * p["qty"], 1)
        upnl += pnl
        net = (a.get("net") or {})
        rows.append({"s": p["s"], "entry": p["entry"], "qty": p["qty"], "now": round(now, 1),
                     "pnl": pnl, "ret_pct": round((now / p["entry"] - 1) * 100, 2), "note": p.get("note"),
                     "opened": p.get("opened"), "verdict": net.get("label"),
                     "n_bull": net.get("n_bull", 0), "n_caution": net.get("n_caution", 0),
                     "n_tension": net.get("n_tension", 0), "tensions": a.get("tensions", [])})
    rows.sort(key=lambda x: -x["pnl"])
    eq = round(b["start"] + upnl, 1)
    return {"ok": True, "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
            "n": len(rows), "equity": eq, "ret_pct": round((eq / b["start"] - 1) * 100, 2),
            "upnl": round(upnl, 1), "positions": rows, "start": b["start"],
            "note": "Your manual paper picks, tracked live + re-analyzed across every engine. Paper only — not advice."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if len(sys.argv) > 2 and sys.argv[1] == "add":
        print(add(sys.argv[2]))
    elif len(sys.argv) > 2 and sys.argv[1] == "remove":
        print(remove(sys.argv[2]))
    v = view()
    print(f"\nSIMULATOR — {v['asof']} · equity ₹{v['equity']:,} ({v['ret_pct']:+}%) · {v['n']} positions")
    for p in v["positions"]:
        print(f"  {p['s']:<11} {p['qty']}@₹{p['entry']} now ₹{p['now']} P&L ₹{p['pnl']:>8} ({p['ret_pct']:+}%) · {p['verdict']}")
