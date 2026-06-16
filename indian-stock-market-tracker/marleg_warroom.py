"""
marleg_warroom.py — the WAR ROOM: the one screen you run a session from.

It does NOT recompute anything — it ASSEMBLES the validated engines from their caches (fast, cache-served):
  • regime / macro gate      (gated cache's regime block; the page also shows the /api/shock badge on top)
  • leading "winner" sectors  (gated cache leading_industries — you PIN the few that drive the tape)
  • strict, news-clean watchlist (the gated picks: clean, with ⚡ coiled-breakout + ⚠ event flags)
  • per-name READ             : holding-period classification + entry/exit triggers (what move confirms)
  • position sizing           : risk-per-share, so the page sizes qty from YOUR capital + risk-%
  • daily P&L discipline       : your goal / loss-limit (config) vs the live book

Everything here is decision-SUPPORT you parameterize — not advice, and never an order (read-only account).
Grounded in the backtests on the canonical 5y panel:
  • gate_pullback (buy the leading-industry name on a 5d DIP) is the prime swing entry.
  • ⚡ coiled breakout (break from an ATR-contracted base) is the bear-PROOF subset — buy the break, don't
    wait for a retest (the retest underperforms and only fills ~70%).
  • HOT (RSI>70) / EXTENDED = chasing a rallied name — the TEJAS mistake — so the War Room says STAND DOWN.
  • volume accumulation is NOT standalone alpha (≈ baseline), so it's shown as context, never a reason alone.
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
GATED = os.path.join(HERE, "marleg_gated_cache.json")
CFG = os.path.join(HERE, "marleg_warroom_config.json")

DEFAULT_CFG = {
    "capital": 100000.0,         # ₹ tradeable capital for the session (yours to set)
    "risk_pct": 1.0,             # % of capital risked per trade (the stop distance)
    "daily_goal": 2000.0,        # ₹ profit target for the day -> consider banking it
    "daily_loss_limit": 2000.0,  # ₹ max loss for the day -> step out, stop trading
    "max_positions": 4,          # how many concurrent ideas you'll hold
    "pinned_sectors": [],        # the few leading industries you choose to fish in ([] = all leaders)
}


def config():
    try:
        c = json.load(open(CFG, encoding="utf-8"))
    except Exception:
        c = {}
    return {**DEFAULT_CFG, **(c if isinstance(c, dict) else {})}


def set_config(d):
    c = config()
    d = d or {}
    for k in DEFAULT_CFG:
        if k in d:
            c[k] = d[k]
    for k in ("capital", "risk_pct", "daily_goal", "daily_loss_limit"):
        try:
            c[k] = float(c[k])
        except Exception:
            c[k] = DEFAULT_CFG[k]
    try:
        c["max_positions"] = max(1, int(c["max_positions"]))
    except Exception:
        c["max_positions"] = DEFAULT_CFG["max_positions"]
    if not isinstance(c.get("pinned_sectors"), list):
        c["pinned_sectors"] = []
    tmp = CFG + ".tmp"
    json.dump(c, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
    os.replace(tmp, CFG)
    return c


def _hold(p):
    """Holding-period + setup verdict for a gated pick, grounded in the backtests."""
    entry = p.get("entry")
    if p.get("pre_earn_runup"):
        return {"setup": "STAND DOWN", "horizon": "—", "tradeable": False,
                "why": "pre-earnings run-up — binary event risk; this is the trap that burned us"}
    if not p.get("clean", True):
        flags = ", ".join(p.get("event_flags") or []) or "event-driven"
        return {"setup": "VERIFY FIRST", "horizon": "—", "tradeable": False,
                "why": "event-contaminated (" + flags + ") — not organic; confirm the why before trusting it"}
    if entry == "HOT":
        return {"setup": "STAND DOWN", "horizon": "—", "tradeable": False,
                "why": "RSI>70 — chasing an over-heated name (the TEJAS mistake); wait for a pullback"}
    if entry == "EXTENDED":
        return {"setup": "STAND DOWN", "horizon": "—", "tradeable": False,
                "why": "extended at the high — no room left; let it come back to you"}
    if p.get("coiled"):
        return {"setup": "COILED BREAKOUT", "horizon": "swing 5-10d", "tradeable": True, "coiled": True,
                "why": "breaking out of a tight base — bear-proof subset; BUY THE BREAK (don't wait for a retest)"}
    if entry == "PULLBACK":
        return {"setup": "PULLBACK BUY", "horizon": "swing 3-5d", "tradeable": True,
                "why": "leader on a dip — the prime, best-backtested entry; buy as it turns up off the dip"}
    return {"setup": "WATCH", "horizon": "swing 3-5d", "tradeable": True,
            "why": "gated leader, fairly priced — wait for a dip or a clean base breakout to enter"}


def _entry_exit(p):
    price, stop, tgt = p.get("price"), p.get("stop"), p.get("target")
    if p.get("coiled"):
        trig = f"BUY THE BREAK — a close holding above the base near ₹{price}; don't chase if it's already run well past it"
    elif p.get("entry") == "PULLBACK":
        trig = f"BUY THE TURN — a green bar reclaiming the prior day's high after the dip, around ₹{price}; never while it's still falling"
    else:
        trig = f"WAIT — for a dip toward support, or a clean base breakout above ₹{price}, before entering"
    rps = None
    try:
        if price and stop and price > stop:
            rps = round(float(price) - float(stop), 2)
    except Exception:
        pass
    return {
        "trigger": trig,
        "confirm": f"industry rank #{p.get('ind_rank')} (leading) · U/D {p.get('ud')} > {p.get('ud_ma')} (accumulating) · fib {p.get('fib')} (above 0.618)",
        "stop": stop, "target": tgt, "tgtpct": p.get("tgtpct"), "risk_per_share": rps,
        "exit": f"hard stop ₹{stop} (~1 ATR below); first target ₹{tgt} (+{p.get('tgtpct')}%). Once +1 ATR in profit, trail with the smart-stop and let a winner run.",
        "invalidate": "get out if the industry drops out of the leading 40%, the U/D rolls back under its average, or it closes back below the 0.618 fib / the base.",
    }


def _move_potential(p):
    """Amplitude tag (the 3-8%/day filter). ATR% ~= tgtpct/2 (target = price + 2*ATR). HIGH = can do 3-8%
    routinely; LOW = won't without a catalyst. Amplitude, not a direction call — pair with the loss-limit."""
    tp = p.get("tgtpct")
    atrp = round(tp / 2, 2) if tp else None
    a = atrp or 0
    return {"atrp": atrp, "tag": "HIGH" if a >= 3 else "MED" if a >= 1.8 else "LOW",
            "fno_note": "F&O leverage turns a ~2-3% underlying move into your 6-8%"}


def build(top=40):
    try:
        g = json.load(open(GATED, encoding="utf-8"))
    except Exception:
        return {"ok": False, "error": "gated cache not built yet — run marleg_gated_scan.py first"}
    cfg = config()
    pinned = [str(s).lower() for s in cfg.get("pinned_sectors", [])]
    wl = []
    for p in g.get("picks", []):
        if pinned and str(p.get("industry", "")).lower() not in pinned:
            continue
        wl.append({**p, "hold": _hold(p), "plan": _entry_exit(p), "mp": _move_potential(p)})
    _rank = {"COILED BREAKOUT": 0, "PULLBACK BUY": 1, "WATCH": 2, "VERIFY FIRST": 3, "STAND DOWN": 4}
    wl.sort(key=lambda x: (0 if x["hold"]["tradeable"] else 1,
                           _rank.get(x["hold"]["setup"], 5), x.get("ind_rank", 99)))
    wl = wl[:top]
    return {"ok": True, "asof": g.get("asof"), "regime": g.get("regime", {}),
            "n_strict": g.get("n_strict"), "n": g.get("n"), "universe": g.get("universe"),
            "leaders": g.get("leading_industries", []), "pinned": cfg.get("pinned_sectors", []),
            "config": cfg, "watchlist": wl,
            "tradeable_count": sum(1 for x in wl if x["hold"]["tradeable"])}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = build()
    if not r.get("ok"):
        print(r.get("error"))
    else:
        print(f"WAR ROOM · {r['asof']} · regime: {r['regime'].get('verdict')}")
        print(f"watchlist {len(r['watchlist'])} (tradeable {r['tradeable_count']}) · strict {r['n_strict']}/{r['n']} · leaders {len(r['leaders'])}")
        for w in r["watchlist"][:12]:
            h = w["hold"]
            print(f"  {w['s']:<12} {h['setup']:<16} {str(w.get('industry'))[:22]:<22} #{w.get('ind_rank')}  ₹{w.get('price')}  stop ₹{w.get('stop')}  {'TRADE' if h['tradeable'] else 'hold'}")
