"""
marleg_watch.py — the WATCH LOOP you sit in front of. Read-only; it tells, you tap.

Each cycle it runs the unified desk over your watchlist + held book and prints a board:
  🔫 FIRE     a validated entry is live (DIP-BUY in an uptrend) — place the order
  🛠 MANAGE   a held name needs an action (TRAIL / TARGET / STOP)
  👀 WATCH    in an uptrend but no dip yet — wait for the pullback (shows the buy-zone)
  ⛔ ASIDE    no trade (bear regime / extended / no-trend)
It NEVER places an order — you execute on Groww. Run it once, or on a loop during NSE hours.

  python marleg_watch.py                       # one scan of the default watchlist
  python marleg_watch.py --held TEJASNET:100:560 RELIANCE SBIN GAIL   # held + candidates
  python marleg_watch.py --loop 300            # rescan every 300s (run detached during the session)
"""
import sys
import time
import datetime as dt


def _ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")


def scan(watch, held):
    import marleg_desk as dk
    rows = []
    for h in held:                                   # held = list of (sym, qty, avg)
        rows.append(dk.desk(h[0], qty=h[1], avg=h[2]))
    for s in watch:
        rows.append(dk.desk(s))
    rows = [r for r in rows if r.get("ok")]

    def lane(r):
        hl = r.get("headline", "")
        if r.get("held"):
            return "MANAGE" if r.get("state") in ("TRAIL", "TARGET", "STOP") else "HOLD"
        if hl.startswith("✅ DIP-BUY"):
            return "FIRE"
        if hl.startswith("STAND ASIDE") or "not a clean uptrend" in hl:
            return "ASIDE"
        return "WATCH"
    for r in rows:
        r["_lane"] = lane(r)
    order = {"FIRE": 0, "MANAGE": 1, "WATCH": 2, "HOLD": 3, "ASIDE": 4}
    rows.sort(key=lambda r: order.get(r["_lane"], 9))
    return rows


def render(rows):
    icon = {"FIRE": "🔫 FIRE  ", "MANAGE": "🛠 MANAGE", "WATCH": "👀 WATCH ", "HOLD": "·  hold  ", "ASIDE": "⛔ aside "}
    out = [f"\n  ┌─ MARLE·G WATCH · {_ist()} ─ regime {'BULL' if (rows and rows[0].get('regime_bull')) else '—'} ─"]
    for r in rows:
        out.append(f"  {icon.get(r['_lane'], r['_lane']):<9} {r['sym']:<11} ₹{str(r.get('price')):<9} {r.get('headline', '')}")
    fires = sum(r["_lane"] == "FIRE" for r in rows); mng = sum(r["_lane"] == "MANAGE" for r in rows)
    out.append(f"  └─ {fires} to FIRE · {mng} to MANAGE · read-only, you place the order ─")
    return "\n".join(out)


def _parse_held(items):
    held = []
    for it in items:
        p = it.split(":")
        held.append((p[0].upper(), int(p[1]) if len(p) > 1 else None, float(p[2]) if len(p) > 2 else None))
    return held


_SENT = set()


def slack_push(rows, dedup=True):
    """Push only the ACTIONABLE lanes (FIRE / MANAGE) to Slack. Dedups so it doesn't re-ping the same call."""
    act = [r for r in rows if r["_lane"] in ("FIRE", "MANAGE")]
    if not act:
        return 0
    try:
        import marleg_slack as sl
    except Exception:
        return 0
    pushed = 0
    for r in act:
        key = r["sym"] + "|" + (r.get("headline") or "")[:40]
        if dedup and key in _SENT:
            continue
        _SENT.add(key)
        head = ("🔫 FIRE · " if r["_lane"] == "FIRE" else "🛠 MANAGE · ") + f"{r['sym']} ₹{r.get('price')}"
        try:
            sl.card(head, lines=[r.get("headline", "")])
            pushed += 1
        except Exception:
            pass
    return pushed


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    a = sys.argv[1:]
    held, watch, loop, slack = [], [], 0, False
    i = 0
    while i < len(a):
        if a[i] == "--held":
            j = i + 1
            while j < len(a) and not a[j].startswith("--"):
                j += 1
            held = _parse_held(a[i + 1:j]); i = j
        elif a[i] == "--loop":
            loop = int(a[i + 1]); i += 2
        elif a[i] == "--slack":
            slack = True; i += 1
        else:
            watch.append(a[i].upper()); i += 1
    if not watch and not held:
        watch = ["RELIANCE", "SBIN", "TEJASNET", "BHEL", "GAIL", "HINDALCO", "TATASTEEL", "INFY"]
    while True:
        rows = scan(watch, held)
        print(render(rows))
        if slack:
            print(f"  (slack: pushed {slack_push(rows)} actionable)")
        if loop <= 0:
            break
        time.sleep(loop)
