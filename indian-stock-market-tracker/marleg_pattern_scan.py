"""
marleg_pattern_scan.py — universe scan that bins stocks by the pattern they're printing.

Detects patterns firing on the last ~2 bars across the F&O-liquid universe and groups them
(which names show a Hammer today, which a Bullish Engulfing, etc.), each tagged with the
backtested grade (edge / weak / anti-folklore). Writes marleg_pattern_scan.json for /api/patterns_scan.

  python marleg_pattern_scan.py
"""
import json, os, sys, time
import yfinance as yf
import marleg_patterns as mp
import marleg_options_monitor as mom

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_pattern_scan.json")
NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    U = sorted(mom.FNO_UNDERLYINGS)
    print(f"scanning {len(U)} F&O names for live patterns...")
    bins = {name: [] for name in mp.META}
    CH, kept = 60, 0
    for i in range(0, len(U), CH):
        chunk = U[i:i + CH]
        for attempt in range(3):
            try:
                data = yf.download([s + ".NS" for s in chunk], period="1y", interval="1d",
                                   group_by="ticker", progress=False, threads=True)
                break
            except Exception as e:
                print(f"  retry {attempt+1} ({str(e)[:40]})"); time.sleep(20)
        else:
            continue
        for s in chunk:
            try:
                df = data[s + ".NS"][["Open", "High", "Low", "Close", "Volume"]].dropna().rename(columns=str.lower)
            except Exception:
                continue
            if len(df) < 60:
                continue
            kept += 1
            for d in mp.detect_last(df, within=2):
                bins[d["name"]].append({"s": s, "n": NAMES.get(s, s), "bias": d["bias"],
                                        "bars_ago": d["bars_ago"], "close": round(float(df["close"].iloc[-1]), 2)})
        print(f"  {min(i + CH, len(U))}/{len(U)} (kept {kept})")
        time.sleep(5)

    from datetime import datetime, timezone, timedelta
    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    groups = []
    for name in mp.META:
        if bins[name]:
            v = mp.verdict(name)
            groups.append({"name": name, "bias": mp.META[name][0], "desc": mp.META[name][1],
                           "grade": v["grade"], "note": v["note"], "n": len(bins[name]),
                           "stocks": sorted(bins[name], key=lambda x: x["bars_ago"])})
    grank = {"edge": 0, "weak": 1, "untested": 2, "anti": 3}
    groups.sort(key=lambda g: (grank.get(g["grade"], 4), -g["n"]))
    json.dump({"asof": ist, "scanned": kept, "groups": groups},
              open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\nscanned {kept} names -> {OUT}")
    for g in groups[:12]:
        print(f"  {g['name']:<22} [{g['grade']:<8}] {g['n']} names")


if __name__ == "__main__":
    main()
