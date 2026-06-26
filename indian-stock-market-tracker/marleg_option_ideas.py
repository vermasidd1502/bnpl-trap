"""
marleg_option_ideas.py — the DAILY OPTION-IDEAS list ("ride the wave").

Same architecture as the volume/gated pod: a fresh list each trading day, persisted to disk, with
"on-list since" tenure — but for OPTIONS instead of cash names. The pipeline:

  1. marleg_option_suggest.suggest()  →  gated, liquid-only candidates. Each already passed: a long-plausible
     setup (horizon.rate folds trend/gated/cup/leadership), a REACHABLE strike with real OI and a tight
     spread (an option you can actually enter/exit), and a positive ride EV (ev_ride > 0).
  2. CORP-EVENTS GATE (marleg_corp_events) — drop anything on ASM/GSM surveillance or with a fresh distress
     filing (you don't buy calls on a manipulated/distressed name); tag fresh deals/orders as a catalyst.
  3. WAVE score — is the move actually being RIDDEN? rvol (today vs 20d) + 20-day up/down-volume. The user's
     thesis: ride names where volume is surging, not just where the chart looks nice. A pick with a great
     setup but no volume behind it scores lower.
  4. Rank by a blended "ride" score (conviction × ride-EV × wave), cache to marleg_option_ideas_cache.json,
     and stamp tenure from marleg_option_ideas_history.json.

ideas() / daily() return the list for /api/option_ideas. Read-only decision-support — you place the order
on Groww. Honest: buying options is a ride-or-bust bet (theta works against you); the gates raise the hit
rate, they don't remove the risk. Each row carries its own risk meter + P→ATM so you SEE the bet.
"""
import os
import json
import datetime as dt

_DIR = os.path.dirname(os.path.abspath(__file__))
_CACHE = os.path.join(_DIR, "marleg_option_ideas_cache.json")
_HIST = os.path.join(_DIR, "marleg_option_ideas_history.json")


def _today_ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).date().isoformat()


def _now_ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")


def _wave(sym):
    """Is the move being ridden? rvol + 20d up/down-volume → a 0-100 wave score + a label."""
    try:
        import marleg_move as mvm
        vs = mvm._vol_stats(sym)
    except Exception:
        vs = None
    if not vs:
        return {"score": 0, "label": "no data", "rvol": None, "ud": None, "chg": None}
    rvol = vs.get("rvol") or 0
    ud = vs.get("ud") or 0
    chg = vs.get("chg") or 0
    # rvol contributes up to 55, U/D up to 30, a green last bar up to 15
    s = min(55, max(0, (rvol - 0.8) / (2.5 - 0.8) * 55)) + min(30, max(0, (ud - 0.9) / (1.8 - 0.9) * 30)) + (15 if chg > 0 else 0)
    s = round(min(100, s))
    label = "surging" if s >= 70 else "building" if s >= 45 else "quiet" if s >= 20 else "flat"
    return {"score": s, "label": label, "rvol": rvol, "ud": ud, "chg": chg}


def _record(syms, date=None):
    date = date or _today_ist()
    try:
        hist = json.load(open(_HIST, encoding="utf-8")) if os.path.exists(_HIST) else {"days": {}}
    except Exception:
        hist = {"days": {}}
    hist.setdefault("days", {})[date] = sorted(set(syms))
    # keep last 120 trading days
    days = sorted(hist["days"].keys())
    for d in days[:-120]:
        hist["days"].pop(d, None)
    try:
        json.dump(hist, open(_HIST, "w", encoding="utf-8"))
    except Exception:
        pass
    return hist


def _tenure(syms, date=None):
    date = date or _today_ist()
    try:
        hist = json.load(open(_HIST, encoding="utf-8")) if os.path.exists(_HIST) else {"days": {}}
    except Exception:
        hist = {"days": {}}
    days = sorted([d for d in hist.get("days", {}) if d <= date])
    out = {}
    for sym in syms:
        streak, since, total, first = 0, None, 0, None
        for d in days:
            if sym in hist["days"].get(d, []):
                total += 1
                first = first or d
        for d in reversed(days):                     # consecutive run ending today
            if sym in hist["days"].get(d, []):
                streak += 1; since = d
            else:
                break
        out[sym] = {"streak": streak, "since": since, "total": total, "first": first,
                    "new": streak <= 1}
    return out


def _fmt_since(iso):
    try:
        return dt.date.fromisoformat(iso).strftime("%d %b")
    except Exception:
        return iso or "—"


def build(top=8, min_oi=200):
    """Heavy: runs the liquid-only option scan + corp gate + wave score. ~45-60s cold."""
    import marleg_option_suggest as osg
    raw = osg.suggest(top=top, min_oi=min_oi)

    try:
        import marleg_corp_events as cev
    except Exception:
        cev = None

    actionable, blocked = [], []
    for x in raw.get("suggestions", []):
        if not x.get("liquid") or not x.get("pick"):
            continue                                       # only rows with a real, reachable, liquid strike
        sym = x["tk"]
        ce = None
        if cev:
            try:
                ce = cev.gate(sym)
            except Exception:
                ce = None
        x["corp"] = ce
        x["wave"] = _wave(sym)
        if ce and ce.get("score", 0) <= -2:               # ASM/GSM/distress → don't buy calls on it
            x["block_reason"] = ce.get("verdict")
            blocked.append(x)
            continue
        p = x["pick"]
        conv = (x.get("rating") or 0) / 100.0
        ev = max(0.0, (p.get("ev_ride") or 0)) / 100.0
        catalyst_bonus = 0.10 if (ce and ce.get("score", 0) > 0) else 0.0
        x["ride_score"] = round(100 * (0.45 * conv + 0.30 * (x["wave"]["score"] / 100.0) + 0.20 * min(1.0, ev) + catalyst_bonus), 1)
        actionable.append(x)

    actionable.sort(key=lambda r: -r["ride_score"])
    date = _today_ist()
    syms = [x["tk"] for x in actionable]
    _record(syms, date)
    ten = _tenure(syms, date)
    for x in actionable:
        t = ten.get(x["tk"], {})
        x["days_on_list"] = t.get("streak"); x["on_since"] = _fmt_since(t.get("since")); x["new_today"] = t.get("new")

    out = {"ok": True, "asof": _now_ist(), "date": date, "n": len(actionable), "n_blocked": len(blocked),
           "universe": raw.get("universe"), "scanned": raw.get("scanned"),
           "indices": raw.get("indices", []), "ideas": actionable, "blocked": blocked,
           "note": "Daily, gate-passed option ideas — ride-the-wave ranking (conviction × volume-surge × ride-EV). "
                   "Each is a REACHABLE liquid strike (OI ≥ %d) past the corp-events gate (no ASM/GSM/distress). "
                   "Buying options is ride-or-bust — theta bleeds; the risk meter + P→ATM show the bet. "
                   "Read-only; you place the order on Groww." % min_oi}
    try:
        json.dump(out, open(_CACHE, "w", encoding="utf-8"))
    except Exception:
        pass
    return out


def daily(top=8, min_oi=200, refresh=False):
    """Serve today's cached list; (re)build once per trading day or on refresh."""
    if not refresh and os.path.exists(_CACHE):
        try:
            c = json.load(open(_CACHE, encoding="utf-8"))
            if c.get("date") == _today_ist():
                return c
        except Exception:
            pass
    return build(top=top, min_oi=min_oi)


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    top = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    r = build(top=top)
    print(f"\n═══ DAILY OPTION IDEAS · {r['asof']} · {r['n']} actionable ({r['n_blocked']} blocked) ═══")
    for ix in r.get("indices", []):
        p = ix.get("pick")
        if p:
            print(f"  [{ix['idx']}] {ix['direction']} → BUY {int(p['strike'])} {p['kind']} @ ₹{p['premium']} · BE {p['be_move_pct']:+.1f}%")
    print()
    for x in r["ideas"]:
        p = x["pick"]; w = x["wave"]
        on = f"🔥day{x['days_on_list']}" if not x.get("new_today") else "🆕new"
        print(f"  {x['tk']:<11} ride {x['ride_score']:<5} {on:<7} wave:{w['label']:<8} conv {x['rating']} · "
              f"BUY {int(p['strike'])}CE @ ₹{p['cost']} exp {x['expiry']} · P→ATM {p['p_touch']}% · ret {p['ret_at_touch']:+.0f}% · risk {p['risk']['level']}")
    for x in r.get("blocked", []):
        print(f"  ⛔ {x['tk']:<11} blocked — {x.get('block_reason')}")
