"""
marleg_rotation.py — industry LEADERSHIP board (gate-stacked) + a mechanical rotation PAPER-TRACKER.

Two things:
  1. board()  — dense industry list, each with the GATE STACK: RS · leading? · β (risk) · persistence
     (weeks it's led) · 🌡 overheat · and the COUNT of fully-gated, news-clean setups INSIDE it (with the
     names). Leadership = the pond; the gated setups = the fish. (The validated use.)
  2. tracker  — a live PAPER book that mechanically rotates into the top-N leading industries' gated stocks
     and benchmarks itself against NIFTY buy-hold, so you can WATCH whether rotation actually beats holding
     (our walk-forward said it LAGS — this lets you see it forward, honestly). Paper only, never an order.

  python marleg_rotation.py
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
BOOK = os.path.join(HERE, "marleg_rotation_book.json")
START = 100000.0
TOPN_IND = 3          # rotate into the top-3 leading industries
PER_IND = 2           # up to 2 gated stocks per industry


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def _live(syms):
    try:
        import marleg_volume_state as vs
        d, _ = vs._live(syms)
        return d
    except Exception:
        return {}


def _nifty():
    try:
        import yfinance as yf
        h = yf.Ticker("^NSEI").history(period="5d")["Close"].dropna()
        return float(h.iloc[-1]) if len(h) else None
    except Exception:
        return None


def board():
    # base = the GRANULAR industry-RS engine (same scheme as the gated picks); persistence is a different
    # (broader) taxonomy, so it's only a best-effort overlay for beta/persistence.
    try:
        import marleg_industry_rs as irs
        d = irs.load_cache()
        if not (d and d.get("groups")):
            d = irs.run("6mo")
        groups = (d or {}).get("groups", [])
    except Exception:
        groups = []
    gc = _load("marleg_gated_cache.json")
    leadset = {str(x.get("group") or "").lower() for x in gc.get("leading_industries", [])}
    picks = [p for p in gc.get("picks", []) if p.get("clean") is not False and p.get("industry")]
    by_ind = {}
    for p in picks:
        by_ind.setdefault(str(p["industry"]).lower(), []).append(
            {"s": p["s"], "entry": p.get("entry"), "target": p.get("target"), "tgtpct": p.get("tgtpct"), "ud": p.get("ud")})
    per = {str(x.get("industry")).lower(): x for x in _load("marleg_industry_persistence.json").get("industries", [])}
    rows = []
    for g in groups:
        ind = g.get("group")
        if not ind:
            continue
        key = str(ind).lower()
        setups = sorted(by_ind.get(key, []), key=lambda z: -(z.get("ud") or 0))
        pp = per.get(key, {})
        rows.append({"industry": ind, "rs_week": round(g.get("ret20", 0), 1) if g.get("ret20") is not None else None,
                     "breadth": g.get("breadth"), "leading": bool(g.get("leading")) or key in leadset,
                     "beta": pp.get("beta"), "persist_weeks": pp.get("median_lead_weeks"),
                     "heat": pp.get("heat"), "heat_class": pp.get("heat_class"),
                     "n_setups": len(setups), "setups": setups[:6]})
    rows.sort(key=lambda r: (not r["leading"], -(r["rs_week"] if r["rs_week"] is not None else -99)))
    return rows


def _target_basket(b):
    leaders = [g for g in b if g["leading"] and g["setups"]][:TOPN_IND]
    basket = []
    for g in leaders:
        for p in g["setups"][:PER_IND]:
            basket.append({"s": p["s"], "ind": g["industry"]})
    return basket


def _book():
    try:
        return json.load(open(BOOK, encoding="utf-8"))
    except Exception:
        return {"start": START, "positions": [], "closed": [], "inception_date": None, "inception_nifty": None}


def _save(bk):
    try:
        tmp = BOOK + ".tmp"; json.dump(bk, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        os.replace(tmp, BOOK)
    except Exception:
        pass


def tracker(brd):
    bk = _book(); today = datetime.now(IST).strftime("%Y-%m-%d")
    nif = _nifty()
    if not bk.get("inception_nifty") and nif:
        bk["inception_nifty"] = nif; bk["inception_date"] = today
    target = _target_basket(brd); tsyms = {x["s"] for x in target}
    held = {p["s"] for p in bk["positions"]}
    live = _live(list(held | tsyms))
    # rebalance: drop industries no longer in the top-N
    for p in list(bk["positions"]):
        if p["s"] not in tsyms:
            px = live.get(p["s"], p["entry"]); pnl = (px - p["entry"]) * p["qty"]
            bk["closed"].append({**p, "exit": round(px, 1), "pnl": round(pnl, 1), "exit_day": today})
            bk["positions"].remove(p)
    per_pos = bk["start"] / max(1, TOPN_IND * PER_IND)
    for x in target:
        if x["s"] not in held:
            px = live.get(x["s"])
            if not px:
                continue
            qty = int(per_pos // px) or 1
            bk["positions"].append({"s": x["s"], "ind": x["ind"], "entry": round(px, 1), "qty": qty, "since": today})
    upnl = 0.0
    for p in bk["positions"]:
        px = live.get(p["s"], p["entry"]); p["now"] = round(px, 1)
        p["upnl"] = round((px - p["entry"]) * p["qty"], 1); upnl += p["upnl"]
    realized = round(sum(c["pnl"] for c in bk["closed"]), 1)
    eq = round(bk["start"] + realized + upnl, 1)
    ret = round((eq / bk["start"] - 1) * 100, 2)
    nif_ret = round((nif / bk["inception_nifty"] - 1) * 100, 2) if (nif and bk.get("inception_nifty")) else 0.0
    bk["equity"] = eq; bk["ret_pct"] = ret; bk["asof"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
    _save(bk)
    alpha = round(ret - nif_ret, 2)
    verdict = (f"rotation {ret:+}% vs NIFTY {nif_ret:+}% since {bk.get('inception_date')} → "
               + ("BEATING buy-hold (so far)" if alpha > 0 else "LAGGING buy-hold" if alpha < 0 else "tied")
               + " · backtest says rotation lags long-run, so watch the alpha, not one print.")
    return {"equity": eq, "ret_pct": ret, "nifty_ret_pct": nif_ret, "alpha": alpha,
            "positions": sorted(bk["positions"], key=lambda z: -(z.get("upnl") or 0)),
            "n_closed": len(bk["closed"]), "inception_date": bk.get("inception_date"), "verdict": verdict}


def view():
    brd = board()
    return {"ok": True, "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
            "n_industries": len(brd), "n_leading": sum(1 for g in brd if g["leading"]),
            "board": brd, "tracker": tracker(brd),
            "note": "Leadership board (β · persistence · overheat · gated setups inside) + a mechanical rotation "
                    "paper-tracker vs NIFTY buy-hold. Rotation-as-churn underperformed in our walk-forward — the "
                    "tracker lets you watch that live; use the board as a FILTER (trade the gated setups inside a "
                    "leader), not a churn signal. Paper only — not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    v = view()
    t = v["tracker"]
    print(f"\nROTATION — {v['asof']} · {v['n_leading']}/{v['n_industries']} industries leading")
    print(f"  TRACKER (paper): {t['verdict']}")
    print(f"    holding {len(t['positions'])}: " + ", ".join(f"{p['s']}({p['ind'][:12]})" for p in t["positions"]))
    print(f"\n  LEADERS (gate-stacked):")
    for g in [g for g in v["board"] if g["leading"]][:10]:
        hot = "🌡" if g["heat_class"] in ("OVERBLOWN", "HEATING") else " "
        print(f"   {hot} {g['industry'][:26]:<26} RS {g['rs_week']:>6} β{g['beta']} {g['persist_weeks']}w · {g['n_setups']} gated: "
              + ", ".join(p["s"] for p in g["setups"][:4]))
