"""
marleg_winners.py — live WINNERS vs LOSERS board.

Two different questions, both answered per name:
  1. Is the STOCK up or down today?   (LTP vs prev close)
  2. Is MY POSITION winning or losing? (LTP vs my blended cost — across delivery + intraday + MTF)

These diverge: a stock can be flat today while your position bleeds because you bought
above the current price (e.g. QUESS). The board shows both, marks each name, and adds
watchlist movers for context. Read-only — marks to live price, never trades.

  python marleg_winners.py
"""
import os, json, sys
from datetime import datetime, timezone, timedelta
import groww_client as gc

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
try:
    NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
except Exception:
    NAMES = {}


def _f(x):
    try:
        return float(x)
    except Exception:
        return 0.0


def _q(qt, sym):
    return qt.get(sym.upper(), {}) or {}


def _watchlist_movers(g, n=8):
    """Top up/down movers among the volume-pod watchlist today."""
    syms = []
    try:
        d = json.load(open(os.path.join(HERE, "marleg_volume_cache.json"), encoding="utf-8"))
        scored = []
        for sec in d.get("sectors", []):
            for x in sec.get("stocks", []):
                scored.append((x.get("ud") or 0, x["s"]))
        scored.sort(key=lambda z: -z[0])
        syms = [s for _, s in scored[:45]]
    except Exception:
        pass
    if not syms:
        return {"up": [], "down": []}
    qt = g.quote_table(syms)
    rows = [{"sym": s, "name": NAMES.get(s, s), "ltp": _q(qt, s).get("price"), "chg": _q(qt, s).get("chg")}
            for s in syms if _q(qt, s).get("chg") is not None]
    rows.sort(key=lambda r: -(r["chg"] or 0))
    return {"up": rows[:n], "down": list(reversed(rows[-n:]))}


def board(g=None, watch=True):
    g = g or gc.GrowwClient()
    g.token()
    h = g.holdings_data() or {}
    p = g.positions_data() or {}
    holds = h.get("holdings") or []
    poss = p.get("positions") or []

    syms = set()
    for x in holds:
        if _f(x.get("quantity")):
            syms.add((x.get("trading_symbol") or "").upper())
    for x in poss:
        syms.add((x.get("trading_symbol") or "").upper())
    syms.discard("")
    qt = g.quote_table(sorted(syms)) if syms else {}

    agg = {}
    def slot(sym):
        return agg.setdefault(sym, {"sym": sym, "name": NAMES.get(sym, sym), "deliv_qty": 0.0, "deliv_cost": 0.0,
                                    "net_qty": 0.0, "cost_basis": 0.0, "unreal": 0.0, "realised": 0.0,
                                    "buckets": {"delivery": 0.0, "intraday": 0.0, "mtf": 0.0}})

    # delivery (demat holdings) — clean qty x avg
    for x in holds:
        sym = (x.get("trading_symbol") or "").upper()
        qty = _f(x.get("quantity")); avg = _f(x.get("average_price"))
        if not sym or not qty:
            continue
        s = slot(sym); ltp = _q(qt, sym).get("price")
        s["deliv_qty"] += qty; s["deliv_cost"] += qty * avg; s["net_qty"] += qty; s["cost_basis"] += qty * avg
        if ltp is not None and avg:
            pnl = (ltp - avg) * qty
            s["unreal"] += pnl; s["buckets"]["delivery"] += pnl

    # positions (intraday MIS / MTF) — credit=BOUGHT, debit=SOLD.
    # held = carry-forward qty if carried (MTF), else today's net (MIS). Groww reports the same
    # carried lot in BOTH `quantity` and `net_carry_forward_quantity`, so never add them.
    for x in poss:
        sym = (x.get("trading_symbol") or "").upper()
        if not sym:
            continue
        netq = _f(x.get("quantity")); netpx = _f(x.get("net_price"))
        ncf = _f(x.get("net_carry_forward_quantity"))
        prod = (x.get("product") or "").upper(); rpnl = _f(x.get("realised_pnl"))
        held = ncf if ncf else netq
        s = slot(sym); ltp = _q(qt, sym).get("price")
        s["realised"] += rpnl
        if not held:
            continue
        s["net_qty"] += held; s["cost_basis"] += held * netpx     # signed
        if ltp is not None and netpx:
            tot = held * (ltp - netpx)
            s["unreal"] += tot
            s["buckets"]["mtf" if prod == "MTF" else "intraday"] += tot

    rows = []
    for sym, s in agg.items():
        q = _q(qt, sym); ltp = q.get("price"); prev = q.get("prev"); chg = q.get("chg")
        netq = s["net_qty"]
        avg = (s["cost_basis"] / abs(netq)) if netq else None
        day_pnl = (ltp - prev) * netq if (ltp is not None and prev) else None
        unreal_pct = (s["unreal"] / abs(s["cost_basis"]) * 100) if s["cost_basis"] else None
        tag = "winner" if s["unreal"] > 1 else "loser" if s["unreal"] < -1 else "flat"
        rows.append({"sym": sym, "name": s["name"], "ltp": ltp, "prev": prev, "day_chg": chg,
                     "net_qty": round(netq), "avg": round(avg, 2) if avg else None,
                     "unreal": round(s["unreal"]), "unreal_pct": round(unreal_pct, 2) if unreal_pct is not None else None,
                     "realised": round(s["realised"]), "day_pnl": round(day_pnl) if day_pnl is not None else None,
                     "buckets": {k: round(v) for k, v in s["buckets"].items() if abs(v) >= 1},
                     "day_tag": "up" if (chg or 0) > 0.05 else "down" if (chg or 0) < -0.05 else "flat",
                     "tag": tag})
    rows.sort(key=lambda r: -(r["unreal"] or 0))
    totals = {"unreal": round(sum(r["unreal"] for r in rows)),
              "realised": round(sum(r["realised"] for r in rows)),
              "day_pnl": round(sum((r["day_pnl"] or 0) for r in rows)),
              "winners": sum(1 for r in rows if r["tag"] == "winner"),
              "losers": sum(1 for r in rows if r["tag"] == "loser"),
              "n": len(rows)}
    out = {"asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"), "book": rows, "totals": totals}
    if watch:
        try:
            out["movers"] = _watchlist_movers(g)
        except Exception:
            out["movers"] = {"up": [], "down": []}
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = board()
    t = r["totals"]
    print(f"\nWINNERS / LOSERS — {r['asof']}")
    print(f"  book: {t['winners']} winners · {t['losers']} losers · unrealized Rs {t['unreal']:,} · "
          f"realised today Rs {t['realised']:,} · day P&L Rs {t['day_pnl']:,}\n")
    print(f"  {'SYM':<12}{'qty':>6}{'avg':>9}{'ltp':>9}{'day%':>7}{'unreal':>11}{'u%':>7}  buckets")
    for x in r["book"]:
        b = " ".join(f"{k}:{v:,}" for k, v in x["buckets"].items())
        print(f"  {x['sym']:<12}{x['net_qty']:>6}{(x['avg'] or 0):>9}{(x['ltp'] or 0):>9}"
              f"{(x['day_chg'] or 0):>6}%{x['unreal']:>11,}{(x['unreal_pct'] or 0):>6}%  {b}")
    m = r.get("movers", {})
    if m.get("up") or m.get("down"):
        print("\n  watchlist movers:")
        print("   up  :", ", ".join(f"{z['sym']} +{z['chg']}%" for z in m["up"][:6]))
        print("   down:", ", ".join(f"{z['sym']} {z['chg']}%" for z in m["down"][:6]))


if __name__ == "__main__":
    main()
