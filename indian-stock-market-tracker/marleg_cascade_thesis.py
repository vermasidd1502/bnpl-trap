"""
Marle-G Cascade THESIS — turn a cascade "painting" into a volume-confirmed basket.

The cascade graph (marleg_cascade.py) says WHICH industries should move and HOW
(long/short, by tier). This layer asks the market: is the move ACTUALLY happening,
with volume? Only legs whose member stocks confirm the predicted direction on real
volume make the final basket. That is the user's rule: don't initiate a move, go
ALONG with one that volume is already confirming.

Confirmation uses the live monitor endpoint (/api/live: Groww real-time price +
pace-adjusted volume + BUYING/FADING tag). Server must be running.

  python marleg_cascade_thesis.py --event oil_shock_up
  python marleg_cascade_thesis.py --event war_escalation --min-vol 0.9 --topk 4
"""
import os, sys, json, time, argparse, urllib.request, urllib.parse, collections
import marleg_cascade as casc
import marleg_slack

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.environ.get("MARLEG_BASE", "http://127.0.0.1:8777")
PER_LEG_FETCH = 12          # cap members fetched per leg (liquidity-agnostic, bounds calls)


def live(syms):
    """Call /api/live in chunks of 20 (with one retry); return {sym: {...}}."""
    out = {}
    syms = list(dict.fromkeys(syms))
    for i in range(0, len(syms), 20):
        chunk = syms[i:i + 20]
        url = BASE + "/api/live?syms=" + urllib.parse.quote(",".join(chunk))
        for attempt in (1, 2):
            try:
                with urllib.request.urlopen(url, timeout=50) as r:
                    d = json.loads(r.read().decode())
                for s in chunk:
                    if s in d and isinstance(d[s], dict) and "price" in d[s]:
                        out[s] = d[s]
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  [warn] live fetch failed for chunk {i//20}: {str(e)[:60]}")
                else:
                    time.sleep(1.5)
    return out


def confirm_member(side, q):
    """Does this stock confirm the leg's predicted direction, on volume?
    Returns (confirmed: bool, strength: float)."""
    chg = q.get("chg") or 0.0
    volr = q.get("volr")
    if volr is None:
        return False, 0.0
    if side == "LONG":
        ok = chg > 0.2 and volr > 0.0
    else:
        ok = chg < -0.2 and volr > 0.0
    # strength = move size * volume confirmation (capped), only when aligned
    strength = abs(chg) * min(volr, 3.0) if ok else 0.0
    return ok, round(strength, 2)


def build(event, min_vol, topk):
    cascade = casc.build_cascade(event)
    # gather members to test
    pool = []
    for lg in cascade["legs"]:
        pool += lg["members"][:PER_LEG_FETCH]
    print(f"confirming {len(set(pool))} member stocks against live volume ...")
    q = live(pool)

    basket = []
    for lg in cascade["legs"]:
        confirmed = []
        for s in lg["members"][:PER_LEG_FETCH]:
            if s not in q:
                continue
            ok, strength = confirm_member(lg["side"], q[s])
            if ok and (q[s].get("volr") or 0) >= min_vol:
                confirmed.append({"sym": s, "chg": q[s]["chg"], "volr": q[s]["volr"],
                                  "tag": q[s].get("tag"), "strength": strength})
        confirmed.sort(key=lambda x: -x["strength"])
        tested = sum(1 for s in lg["members"][:PER_LEG_FETCH] if s in q)
        conf_frac = round(len(confirmed) / tested, 2) if tested else 0.0
        # leg conviction = cascade structural impact * market confirmation breadth
        conviction = round(abs(lg["impact"]) * (0.3 + 0.7 * conf_frac), 3)
        basket.append({**lg, "confirmed": confirmed[:topk], "tested": tested,
                       "conf_frac": conf_frac, "conviction": conviction})
    # keep only legs the market is confirming
    fired = [b for b in basket if b["confirmed"]]
    fired.sort(key=lambda b: -b["conviction"])
    return cascade, basket, fired


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", default="oil_shock_up")
    ap.add_argument("--min-vol", type=float, default=0.8, help="min vol-vs-avg to count as confirmed")
    ap.add_argument("--topk", type=int, default=4, help="top confirmed names per leg")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--alert", action="store_true", help="post the confirmed basket to Slack")
    a = ap.parse_args()
    if a.event not in casc.EVENTS:
        print(f"unknown event. options: {', '.join(casc.EVENTS)}"); return
    cascade, basket, fired = build(a.event, a.min_vol, a.topk)
    if a.json:
        print(json.dumps({"event": a.event, "fired": fired}, indent=1)); return

    print(f"\nCASCADE THESIS: {a.event}")
    print(f"  {cascade['label']}")
    print(f"  {len(fired)}/{len(basket)} legs confirmed by live volume (min vol {a.min_vol}x)\n")
    print(f"{'CONV':>6} {'TIER':<5}{'SIDE':<6}{'INDUSTRY':<36} confirmed names (chg% / vol)")
    print("-" * 108)
    for b in fired:
        names = ", ".join(f"{c['sym']}({c['chg']:+.1f}/{c['volr']}x)" for c in b["confirmed"])
        print(f"{b['conviction']:>6} T{b['tier']:<4}{b['side']:<6}{b['industry']:<36} {names}")
    longs = [b for b in fired if b["side"] == "LONG"]
    shorts = [b for b in fired if b["side"] == "SHORT"]
    print("-" * 108)
    print(f"BASKET: {sum(len(b['confirmed']) for b in longs)} long names / "
          f"{sum(len(b['confirmed']) for b in shorts)} short names, "
          f"{len(longs)} long legs vs {len(shorts)} short legs.")
    unconf = [b["industry"] for b in basket if not b["confirmed"]]
    if unconf:
        print(f"NOT confirmed (cascade says move, volume doesn't yet): {', '.join(unconf[:8])}"
              + (" ..." if len(unconf) > 8 else ""))
    if a.alert and fired:
        body = "\n".join(f"{b['side']} {b['industry']} ({', '.join(c['sym'] for c in b['confirmed'][:3])})"
                         for b in fired[:6])
        ok = marleg_slack.notify(f"🎨 Cascade fired: {a.event} — {len(fired)} volume-confirmed legs",
                                 fields={"basket": body, "long/short legs": f"{len(longs)}/{len(shorts)}"})
        print("posted cascade basket to Slack." if ok else "(Slack off — set MARLEG_SLACK_WEBHOOK)")
    out_path = os.path.join(HERE, f"marleg_cascade_{a.event}.json")
    json.dump({"event": a.event, "label": cascade["label"], "fired": fired},
              open(out_path, "w"), indent=1)
    print(f"\nsaved {out_path}  (monitor-only thesis; no orders)")


if __name__ == "__main__":
    main()
