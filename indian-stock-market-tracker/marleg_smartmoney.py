"""
Marle-G — SMART-MONEY / institutional-flow layer (India's answer to 13F).

India has no 13F, so "follow the smart money" = quarterly SHAREHOLDING PATTERN deltas:
  - Promoters (insider conviction; promoter BUYING is a strong signal)
  - FIIs / FPIs (foreign institutions)
  - DIIs (domestic MFs + insurance — the structural India bid)
  - Public / retail (the dumb-money counterweight)
Rising FII + DII (and promoter buying) into a name = institutions accumulating.

Source: screener.in quarterly shareholding table (12 quarters), reachable from any IP
(NSE bulk-deals are Akamai-blocked from datacenter VPNs). yfinance gives a current
institutional/insider % as a cross-check. Cached 12h (changes only quarterly).

  python marleg_smartmoney.py TITAN
  python marleg_smartmoney.py --screen        # rank a universe by institutional inflow
"""
import sys, re, json, os, time
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
MONTHS = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}"
_CACHE = {}


def fetch_shareholding(sym):
    """Quarterly shareholding % from screener.in. Returns dict or None."""
    sym = sym.upper()
    hit = _CACHE.get(sym)
    if hit and time.time() - hit[0] < 43200:
        return hit[1]
    for path in ("consolidated/", ""):
        try:
            t = requests.get(f"https://www.screener.in/company/{sym}/{path}", headers=UA, timeout=15).text
        except Exception:
            continue
        i = t.find('id="shareholding"')
        if i < 0:
            continue
        seg = re.sub(r"<script.*?</script>", "", t[i:i + 5000], flags=re.S)
        seg = re.sub(r"<[^>]+>", " ", seg); seg = re.sub(r"\s+", " ", seg)
        dates = re.findall(MONTHS, seg)
        if not dates:
            continue
        n = len(dates)
        out = {"symbol": sym, "quarters": dates}
        for key, label in [("promoter", "Promoters"), ("fii", "FIIs"), ("dii", "DIIs"),
                           ("government", "Government"), ("public", "Public")]:
            j = seg.find(label)
            vals = re.findall(r"(\d+\.\d+)%", seg[j:]) if j >= 0 else []
            out[key] = [float(x) for x in vals[:n]]
        _CACHE[sym] = (time.time(), out)
        return out
    _CACHE[sym] = (time.time(), None)
    return None


def flow(sym):
    sh = fetch_shareholding(sym)
    if not sh or not sh.get("quarters"):
        return {"error": "no shareholding data for " + sym.upper()}

    def d(arr, k):
        return round(arr[-1] - arr[-1 - k], 2) if (arr and len(arr) > k) else None
    res = {"symbol": sym.upper(), "asof": sh["quarters"][-1], "quarters": sh["quarters"]}
    for key in ("promoter", "fii", "dii", "public"):
        a = sh.get(key) or []
        res[key] = {"now": (a[-1] if a else None), "d1q": d(a, 1), "d4q": d(a, 4), "series": a}
    fii1 = res["fii"]["d1q"] or 0; dii1 = res["dii"]["d1q"] or 0; pro1 = res["promoter"]["d1q"] or 0
    inst1 = round(fii1 + dii1, 2)
    res["inst_delta_1q"] = inst1
    res["inst_delta_4q"] = round((res["fii"]["d4q"] or 0) + (res["dii"]["d4q"] or 0), 2)
    res["verdict"] = "ACCUMULATING" if inst1 > 0.3 else "DISTRIBUTING" if inst1 < -0.3 else "STABLE"
    res["promoter_action"] = "BUYING" if pro1 > 0.1 else "SELLING" if pro1 < -0.1 else "FLAT"
    # yfinance cross-check (current institutional + insider %)
    try:
        import yfinance as yf
        i = yf.Ticker(sym.upper() + ".NS").info
        res["yf_institutions_pct"] = round((i.get("heldPercentInstitutions") or 0) * 100, 1)
        res["yf_insiders_pct"] = round((i.get("heldPercentInsiders") or 0) * 100, 1)
    except Exception:
        pass
    return res


def screen(universe, limit=40):
    """Rank a universe by 1-quarter institutional inflow (FII+DII delta)."""
    out = []
    for s in universe[:limit]:
        f = flow(s)
        if not f.get("error") and f.get("inst_delta_1q") is not None:
            out.append({"sym": s, "inst_1q": f["inst_delta_1q"], "inst_4q": f["inst_delta_4q"],
                        "fii_1q": f["fii"]["d1q"], "dii_1q": f["dii"]["d1q"],
                        "promoter": f["promoter_action"], "verdict": f["verdict"]})
        time.sleep(0.4)            # be polite to screener.in
    out.sort(key=lambda x: -(x["inst_1q"] or -99))
    return out


def main():
    if "--screen" in sys.argv:
        import marleg_volume_scan as v
        rows = screen(v.SEED[:30])
        print(f"{'SYM':<14}{'inst d1q':>9}{'inst d4q':>9}{'FII d1q':>9}{'DII d1q':>9}  promoter   verdict")
        print("-" * 78)
        for r in rows:
            print(f"{r['sym']:<14}{r['inst_1q']:>+9.2f}{r['inst_4q']:>+9.2f}{(r['fii_1q'] or 0):>+9.2f}{(r['dii_1q'] or 0):>+9.2f}  {r['promoter']:<10} {r['verdict']}")
        return
    sym = sys.argv[1] if len(sys.argv) > 1 else "TITAN"
    f = flow(sym)
    if f.get("error"):
        print(f["error"]); return
    print(f"\n{sym.upper()} — institutional flow (as of {f['asof']})")
    print(f"  {'holder':<12}{'now %':>8}{'d 1Q':>8}{'d 4Q':>8}")
    for k, lbl in [("promoter", "Promoters"), ("fii", "FIIs"), ("dii", "DIIs"), ("public", "Public")]:
        x = f[k]
        print(f"  {lbl:<12}{(x['now'] if x['now'] is not None else 0):>7.2f}%{(x['d1q'] or 0):>+8.2f}{(x['d4q'] or 0):>+8.2f}")
    print(f"\n  institutional d: {f['inst_delta_1q']:+.2f}% (1Q)  {f['inst_delta_4q']:+.2f}% (4Q)  -> {f['verdict']}")
    print(f"  promoters: {f['promoter_action']}")
    if "yf_institutions_pct" in f:
        print(f"  yfinance cross-check: institutions {f['yf_institutions_pct']}% · insiders/promoters {f['yf_insiders_pct']}%")


if __name__ == "__main__":
    main()
