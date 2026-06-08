"""
Classify NSE symbols by sector + (granular) industry via yfinance, cached & resumable.
Rate-limit aware: throttles requests and backs off on HTTP 429.

  python marleg_sectors.py --liquid    # (re)classify only the stocks that have volume
                                        #  data AND are still "Others"  <- the useful set
  python marleg_sectors.py --all       # classify the full 3910 universe (slow)
  python marleg_sectors.py             # classify the volume-scan seed list

Writes marleg_sectors.json: {symbol: {"sector": ..., "industry": ...}}.
"""
import json, os, sys, time
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
SYMS = os.path.join(HERE, "marleg_symbols.json")
VOLC = os.path.join(HERE, "marleg_volume_cache.json")
OUT = os.path.join(HERE, "marleg_sectors.json")


def load(p, d):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return d


def save(cache):
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


def classify_one(sym, tries=4):
    for _ in range(tries):
        try:
            info = yf.Ticker(sym + ".NS").info
            sec = info.get("sector")
            return {"sector": sec or "Others", "industry": info.get("industry") or "Others"}
        except Exception as e:
            if "Too Many" in str(e) or "429" in str(e) or "Rate" in str(e):
                time.sleep(45)            # rate-limited -> back off and retry
            else:
                return {"sector": "Others", "industry": "Others"}
    return None                            # exhausted retries -> leave as-is for next run


def main():
    cache = load(OUT, {})
    if "--liquid" in sys.argv:
        vc = load(VOLC, {})
        universe = [st["s"] for sec in vc.get("sectors", []) for st in sec.get("stocks", [])]
        todo = [s for s in universe if cache.get(s, {}).get("sector", "Others") == "Others"]
    elif "--all" in sys.argv:
        universe = [r["s"] for r in load(SYMS, [])]
        todo = [s for s in universe if s not in cache]
    else:
        import marleg_volume_scan as v
        todo = [s for s in v.SEED if s not in cache]
    if "--limit" in sys.argv:
        todo = todo[:int(sys.argv[sys.argv.index("--limit") + 1])]

    print(f"have {len(cache)} | to (re)classify {len(todo)} (throttled)")
    done = 0
    for i, s in enumerate(todo, 1):
        r = classify_one(s)
        if r:
            cache[s] = r
            done += 1
        time.sleep(1.2)                    # throttle
        if i % 10 == 0:
            save(cache)
            print(f"  {i}/{len(todo)}  (classified {done})")
    save(cache)
    real = sum(1 for v in cache.values() if v.get("sector") != "Others")
    print(f"done — {real} real / {len(cache)} total classified -> {OUT}")


if __name__ == "__main__":
    main()
