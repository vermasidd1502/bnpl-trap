"""
Marle-G Slack WATCHER — pings your phone when a stock flips to BUYING / FADING.

The live monitor (browser) can't alert you when the tab is closed; this runs headless
and posts to Slack on the EVENTS that matter:
  - a watched name flips to BUYING (up + real volume) or FADING (down + volume)
  - a fresh name enters the gated-longs screen
It tracks last state per symbol (marleg_slack_state.json) so it alerts on the FLIP,
not every poll, with a per-symbol cooldown so it never spams. Market-hours aware.

  python marleg_slack_watch.py            # loop every 60s (keep it running in the session)
  python marleg_slack_watch.py --once     # single pass (for testing)
  python marleg_slack_watch.py --interval 90
"""
import os, sys, json, time, argparse, urllib.request, urllib.parse, datetime as dt
import marleg_slack as slack

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.environ.get("MARLEG_BASE", "http://127.0.0.1:8777")
STATE = os.path.join(HERE, "marleg_slack_state.json")
COOLDOWN = 1800        # don't re-alert the same symbol+tag within 30 min
DEFAULTS = ["RELIANCE", "HDFCBANK", "ICICIBANK", "TCS", "INFY", "SBIN", "TMPV", "ITC",
            "BHARTIARTL", "LT", "JSWENERGY", "ADANIENT", "ADANIENSOL", "MARUTI", "AXISBANK"]


def _get(path):
    try:
        with urllib.request.urlopen(BASE + path, timeout=45) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def live(syms):
    out = {}
    for i in range(0, len(syms), 20):
        d = _get("/api/live?syms=" + urllib.parse.quote(",".join(syms[i:i + 20]))) or {}
        for s in syms[i:i + 20]:
            if isinstance(d.get(s), dict) and d[s].get("price") is not None:
                out[s] = d[s]
    return out


def market_open():
    ist = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)
    if ist.weekday() >= 5:
        return False
    mins = ist.hour * 60 + ist.minute
    return 9 * 60 + 15 <= mins <= 15 * 60 + 30


def load_state():
    try:
        return json.load(open(STATE))
    except Exception:
        return {}


def watchlist():
    syms = set(DEFAULTS)
    g = _get("/api/gated") or {}
    for p in (g.get("picks") or [])[:15]:                # cap gated adds so alerts stay tasteful
        if p.get("s"):
            syms.add(p["s"].upper())
    return sorted(syms)


def pass_once(seed_only=False):
    """seed_only=True records current tags WITHOUT alerting (baseline). Otherwise
    alerts only on a genuine flip into BUYING/FADING, respecting the cooldown."""
    syms = watchlist()
    q = live(syms)
    state = load_state()
    now = time.time()
    sent = 0
    for s, d in q.items():
        tag = d.get("tag")
        if tag not in ("BUYING", "FADING"):
            state[s] = {"tag": tag, "t": now}            # track, never alert FLAT/QUIET
            continue
        prev = state.get(s, {})
        flipped = prev.get("tag") != tag
        cooled = (now - prev.get("t", 0)) > COOLDOWN
        if flipped and cooled and not seed_only:
            emoji = "🟢" if tag == "BUYING" else "🔴"
            if slack.notify(
                    f"{emoji} *{s}* flipped to *{tag}*",
                    fields={"price": f"₹{d['price']}", "change": f"{d['chg']:+.2f}%",
                            "vol vs avg": f"{d.get('volr')}x", "monitor": "marle_g_live"}):
                sent += 1
            state[s] = {"tag": tag, "t": now}
        elif flipped:                                    # seed, or flipped-but-not-cooled
            state[s] = {"tag": tag, "t": (now if seed_only else prev.get("t", now))}
    json.dump(state, open(STATE, "w"), indent=1)
    return len(q), sent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval", type=int, default=60)
    a = ap.parse_args()
    if not slack.enabled():
        print("MARLEG_SLACK_WEBHOOK not set in this shell — alerts would be no-ops. "
              "Open a NEW terminal after setx, or set it for this process.")
    print(f"Slack watcher started | webhook {'LIVE' if slack.enabled() else 'OFF'} | "
          f"poll {a.interval}s | cooldown {COOLDOWN//60}min")
    if not os.path.exists(STATE):                        # first run -> seed silently, no alert blast
        n, _ = pass_once(seed_only=True)
        print(f"first run — seeded {n} names silently; will alert only on future flips.")
    while True:
        if market_open() or a.once:
            n, sent = pass_once()
            print(f"{dt.datetime.now().strftime('%H:%M:%S')}  checked {n} names, {sent} alert(s) sent")
        else:
            print(f"{dt.datetime.now().strftime('%H:%M:%S')}  market closed — idle")
        if a.once:
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
