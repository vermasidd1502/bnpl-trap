"""
marleg_gated_snapshot.py — guaranteed daily snapshot of the gated long list.

Reads the latest gated scan (marleg_gated_cache.json) and records its members into the
gated-list history (marleg_gated_history.py). Run this once per trading day AFTER the EOD
gated scan regenerates the cache, so the daily record is captured even if nobody opens the pod.

  python marleg_gated_snapshot.py

(The server also auto-snapshots on every /api/gated compute during the session, so this is
the belt-and-suspenders path for unattended days. Schedule via Windows Task Scheduler, e.g.
after MarleG-EOD, or append a call to the EOD pipeline.)
"""
import os
import json
import sys
import marleg_gated_history as gh

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    path = os.path.join(HERE, "marleg_gated_cache.json")
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except Exception as e:
        print(f"no gated cache to snapshot: {e}")
        return
    syms = [x.get("s") for x in d.get("picks", []) if x.get("s")]
    if not syms:
        print("gated cache had no picks — nothing to snapshot")
        return
    gh.record(syms)
    ten = gh.tenure(syms)
    print(f"snapshotted {len(syms)} gated names for today.")
    veterans = sorted(((s, ten[s.upper()]["streak"]) for s in syms),
                      key=lambda z: -z[1])[:8]
    for s, st in veterans:
        t = ten[s.upper()]
        tag = "NEW" if t["new"] else f"day {st} (since {t['since_fmt']})"
        print(f"  {s:<12} {tag}")


if __name__ == "__main__":
    main()
