"""
marleg_alerts.py — STRATEGY ALERTS + the CAN SLIM staged-gate funnel + a timed CATALYST (earnings) monitor.

The user's funnel, mapped to O'Neil/CAN SLIM, run in ORDER (a stock must clear each to advance):
  ① FUNDAMENTALS  (C+A) : quality / earnings score sound          (fundamentals_cache q-score)
  ② FAIR VALUE    (val) : not absurdly priced                     (PE in a sane band, growth not collapsing)
  ③ VOLUME-MOMENTUM (S+N): leading industry + U/D rising + fib     (it's in the gated list = this fired)
  ④ ENTRY         (entry): healthy entry — pullback/coiled, news-clean, NOT chasing / pre-earnings
A name that clears all four = a full ALERT. We also show how far each candidate got (the gate ladder), and a
timed CATALYST monitor (upcoming earnings = trade AROUND not through; recent results = drift window).

Assembled from caches (gated scan + fundamentals_cache + movers) + earnings proximity (yfinance, cached) +
the live Groww book (read-only) for the diversification flag. Cache-served. Never places an order.

The "as the day moves" intraday cadence is the EOD/periodic rebuild today; a market-hours loop is the next step.
"""
import json
import os
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def _held():
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        out = set()
        for getter in ("holdings_data", "positions_data"):
            try:
                data = getattr(g, getter)()
            except Exception:
                continue
            rows = data if isinstance(data, list) else (data.get("positions") or data.get("holdings") or data.get("data") or []) if isinstance(data, dict) else []
            for r in rows:
                if isinstance(r, dict):
                    sym = (r.get("trading_symbol") or r.get("symbol") or r.get("tradingsymbol") or "").upper()
                    if sym:
                        out.add(sym)
        return out
    except Exception:
        return set()


def _catalyst(s):
    """Timed earnings catalyst for one name (best-effort, cached)."""
    import marleg_eventfilter as ef
    try:
        e = ef.earnings_proximity(s)
    except Exception:
        return None
    ind, ago = e.get("in_days"), e.get("ago_days")
    if ind is not None and 0 <= ind <= 21:
        advice = ("⚠ results imminent — do NOT open a short-term trade; binary event" if ind <= 2
                  else f"earnings in {ind}d — plan around it; don't hold a swing through results")
        return {"s": s, "kind": "UPCOMING", "days": ind, "date": e.get("next"), "advice": advice}
    if ago is not None and 0 <= ago <= 10:
        advice = ("just reported — post-earnings DRIFT window (direction often persists a few days)" if ago <= 3
                  else f"reported {ago}d ago — drift likely fading")
        return {"s": s, "kind": "RECENT", "days": ago, "date": e.get("last"), "advice": advice}
    return None


def build(catalysts_for=28):
    g = _load("marleg_gated_cache.json")
    funda = _load("marleg_fundamentals_cache.json")
    mv = _load("marleg_movers.json")
    picks = g.get("picks", [])
    held = _held()
    SECT = _load("marleg_sectors.json")
    held_sectors = {SECT.get(s, {}).get("sector") for s in held if SECT.get(s, {}).get("sector")}

    ladder = []
    for p in picks:
        s = p["s"]
        f = funda.get(s, {})
        q, pe, growth = f.get("q"), f.get("pe"), f.get("growth")
        g1 = (q is not None and q >= 55); g1_state = "pass" if g1 else ("fail" if q is not None else "unknown")
        g2 = (pe is not None and 0 < pe <= 60 and (growth is None or growth > -8)); g2_state = "pass" if g2 else ("fail" if pe is not None else "unknown")
        g1ok = g1 or g1_state == "unknown"        # sparse fundamentals: UNKNOWN = soft-pass (flagged), not a hard block
        g2ok = g2 or g2_state == "unknown"
        entry = p.get("entry")
        g4 = bool((p.get("clean") and not p.get("pre_earn_runup") and entry in ("PULLBACK", "OK"))
                  or (p.get("clean") and p.get("coiled") and not p.get("pre_earn_runup")))
        gates = {"g1_fundamentals": g1_state, "g2_fair_value": g2_state,
                 "g3_volume_momentum": "pass", "g4_entry": "pass" if g4 else "fail"}
        reached = 0
        if g1ok: reached = 1
        if g1ok and g2ok: reached = 3             # g3 (volume-momentum + industry) is automatic for a gated name
        if g1ok and g2ok and g4: reached = 4
        cleared = reached == 4
        funda_verified = (g1_state == "pass" and g2_state == "pass")
        ladder.append({"s": s, "n": p.get("n"), "industry": p.get("industry"), "ind_rank": p.get("ind_rank"),
                       "entry": entry, "clean": p.get("clean"), "coiled": p.get("coiled"),
                       "price": p.get("price"), "stop": p.get("stop"), "target": p.get("target"), "tgtpct": p.get("tgtpct"),
                       "q": q, "pe": pe, "gates": gates, "reached": reached, "cleared": cleared,
                       "funda_verified": funda_verified,
                       "same_sector_held": (SECT.get(s, {}).get("sector") in held_sectors),
                       "sector": SECT.get(s, {}).get("sector")})
    ladder.sort(key=lambda x: (-x["reached"], 0 if x["clean"] else 1, x.get("ind_rank") or 99))

    alerts = [x for x in ladder if x["cleared"]]
    watch = [x for x in ladder if x["reached"] == 3 and not x["cleared"]]

    # catalyst monitor over gated + top movers + held
    def _eq(s):                              # equities only — skip option/future symbols (digits) + ETFs
        return bool(s) and not any(ch.isdigit() for ch in s) and len(s) <= 14 and not s.endswith("ETF")
    uni = [p["s"] for p in picks[:30]] + [m["s"] for m in mv.get("movers", [])[:25]] + [s for s in held if _eq(s)]
    seen, cats = set(), []
    for s in uni:
        if s in seen or not _eq(s):
            continue
        seen.add(s)
        c = _catalyst(s)
        if c:
            c["held"] = s in held
            cats.append(c)
    upcoming = sorted([c for c in cats if c["kind"] == "UPCOMING"], key=lambda x: x["days"])
    recent = sorted([c for c in cats if c["kind"] == "RECENT"], key=lambda x: x["days"])

    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    return {"asof": ist, "n_ladder": len(ladder), "n_alerts": len(alerts),
            "alerts": alerts[:25], "watch": watch[:15], "ladder": ladder[:40],
            "catalysts": {"upcoming": upcoming, "recent": recent, "n": len(cats)},
            "held": sorted(held)}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = build()
    print(f"ALERTS {r['asof']} — {r['n_alerts']} cleared all 4 gates of {r['n_ladder']} gated; {len(r['watch'])} at the gate")
    for a in r["alerts"][:10]:
        print(f"  ✓ {a['s']:<11} #{a['ind_rank']} {a['entry']:<9} q={a['q']} pe={a['pe']}  ₹{a['price']} -> +{a['tgtpct']}%"
              + ("  ⭐ same-sector held" if a["same_sector_held"] else ""))
    up = r["catalysts"]["upcoming"]
    print(f"\nUPCOMING earnings ({len(up)}):")
    for c in up[:8]:
        print(f"  {c['s']:<11} in {c['days']}d — {c['advice']}")
