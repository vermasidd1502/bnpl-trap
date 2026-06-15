"""
marleg_fundamentals_scan.py — batch-precompute compact fundamentals for the whole universe.

Per-stock fundamentals (marleg_fundamentals.fundamentals) fetch financial STATEMENTS from Yahoo
(seconds each, rate-limited), so they can't be computed live for ~2,700 names in the volume pod.
This batch iterates the universe and caches a compact {q, pe, roe, growth, cov} per symbol to
marleg_fundamentals_cache.json — RESUMABLE (skips already-cached, saves every 20) so it fills the
universe over multiple runs / overnight. The volume pod reads the cache (shows "—" until a name lands).

  python marleg_fundamentals_scan.py [max_per_run]
"""
import json, os, sys, time
import marleg_fundamentals as mf
from marleg_volume_scan import load_universe

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "marleg_fundamentals_cache.json")


def _load():
    try:
        with open(CACHE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save(d):
    tmp = CACHE + ".tmp"                       # atomic write (batch + cache-on-view share this file)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False)
    os.replace(tmp, CACHE)


def _compact(f):
    h = f.get("health", {}) or {}
    return {"q": f.get("qscore"), "pe": h.get("P/E"), "roe": h.get("ROE"),
            "growth": h.get("Rev growth"), "cov": f.get("coverage")}


def cache_one(tk, f):
    """Upsert one stock's compact fundamentals into the cache (cache-on-view from the server)."""
    cache = _load()
    cache[tk.upper()] = _compact(f)
    _save(cache)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    U = load_universe()
    cache = _load()
    todo = [s for s in U if s not in cache]
    cap = int(sys.argv[1]) if len(sys.argv) > 1 else len(todo)
    print(f"universe {len(U)} · cached {len(cache)} · to-do {len(todo)} · this run cap {cap}")
    fails = 0
    for i, s in enumerate(todo[:cap]):
        try:
            f = mf.fundamentals(s)
            h = f.get("health", {}) or {}
            real = ("error" not in f) and (f.get("qscore") is not None or h.get("P/E") is not None or h.get("ROE") is not None)
            if real:
                cache[s] = _compact(f)          # cache ONLY real data; empty (throttle/sparse) stays to-do -> retried next run
                fails = 0
            else:
                fails += 1                       # empty result usually = Yahoo throttle; back off, don't poison the cache
                if fails >= 6:
                    print("  bailing — repeated empty results (Yahoo likely throttling); rerun later"); break
                time.sleep(10); continue
        except Exception as e:
            fails += 1
            if fails >= 6:                       # likely rate-limited hard — persist + bail
                print(f"  bailing after repeated failures ({str(e)[:50]})"); break
            time.sleep(15)
            continue
        if (i + 1) % 20 == 0:
            _save(cache); print(f"  {i+1}/{min(cap,len(todo))} (total cached {len(cache)})")
        time.sleep(0.2)
    _save(cache)
    real = sum(1 for v in cache.values() if "q" in v)
    print(f"done — cache holds {len(cache)} symbols ({real} with fundamentals) -> {CACHE}")


if __name__ == "__main__":
    main()
