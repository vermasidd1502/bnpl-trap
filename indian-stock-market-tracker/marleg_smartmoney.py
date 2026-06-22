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


_LBL = {"promoter": "promoter", "fii": "FII", "dii": "DII", "public": "retail"}


def timeline(sym):
    """Quarter-by-quarter PARTICIPANT timeline — who accumulated vs distributed each quarter, oldest→newest.
    This is the 'what kind of investor is buying/selling' tape, built from the shareholding-pattern series."""
    sh = fetch_shareholding(sym)
    if not sh or not sh.get("quarters"):
        return {"error": "no shareholding data for " + sym.upper()}
    qs = sh["quarters"]
    events = []
    for i in range(1, len(qs)):
        chg = {}
        for key in ("promoter", "fii", "dii", "public"):
            a = sh.get(key) or []
            if len(a) > i and a[i] is not None and a[i - 1] is not None:
                chg[key] = {"pct": a[i], "delta": round(a[i] - a[i - 1], 2)}
        buyers = [_LBL[k] for k, v in chg.items() if v["delta"] >= 0.2]
        sellers = [_LBL[k] for k, v in chg.items() if v["delta"] <= -0.2]
        events.append({"quarter": qs[i], "changes": chg, "buyers": buyers, "sellers": sellers,
                       "story": " · ".join(f"{_LBL[k]} {v['delta']:+.1f}" for k, v in chg.items() if abs(v["delta"]) >= 0.2) or "no notable change"})
    # window trend (FII/DII/promoter over the whole series)
    def trend(key):
        a = sh.get(key) or []
        return round(a[-1] - a[0], 2) if len(a) >= 2 else None
    fii_t, dii_t, pro_t = trend("fii"), trend("dii"), trend("promoter")
    reads = []
    if fii_t is not None:
        reads.append(f"FII over {len(qs)}q: {fii_t:+.1f}% ({'accumulating' if fii_t > 0.3 else 'distributing' if fii_t < -0.3 else 'flat'})")
    if dii_t is not None:
        reads.append(f"DII: {dii_t:+.1f}%")
    if pro_t is not None:
        reads.append(f"Promoter: {pro_t:+.1f}% ({'buying' if pro_t > 0.1 else 'selling' if pro_t < -0.1 else 'flat'})")
    return {"symbol": sym.upper(), "quarters": qs, "events": events,
            "trend": {"fii": fii_t, "dii": dii_t, "promoter": pro_t}, "reads": reads,
            "note": "Quarterly shareholding deltas — the legal, public 'who'. Lagged (filed within 21 days of "
                    "quarter-end). For trade-level named activity use bulk_deals(). Decision-support."}


def _nse_session():
    import requests
    s = requests.Session()
    s.headers.update({"User-Agent": UA["User-Agent"], "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9",
                      "Referer": "https://www.nseindia.com/"})
    try:
        s.get("https://www.nseindia.com", timeout=8)          # prime Akamai cookies
    except Exception:
        pass
    return s


def bulk_deals(sym=None, days=30):
    """NSE bulk deals — NAMED buyer/seller at the trade level. Works from a RESIDENTIAL IP; datacenter
    IPs are Akamai-blocked (so this runs best on the user's own machine, not the cloud guardian)."""
    import datetime as dt
    s = _nse_session()
    end = dt.date.today(); start = end - dt.timedelta(days=days)
    url = f"https://www.nseindia.com/api/historical/bulk-deals?from={start:%d-%m-%Y}&to={end:%d-%m-%Y}"
    if sym:
        url += f"&symbol={sym.upper()}"
    try:
        data = s.get(url, timeout=12).json()
    except Exception as e:
        return {"error": f"NSE bulk-deals unreachable (likely datacenter-IP/Akamai block): {str(e)[:80]}"}
    rows = data.get("data", data) if isinstance(data, dict) else data
    out = []
    for d in (rows or []):
        out.append({"date": d.get("mTIMESTAMP") or d.get("date") or d.get("BD_DT_DATE"),
                    "symbol": d.get("BD_SYMBOL") or d.get("symbol"),
                    "client": d.get("BD_CLIENT_NAME") or d.get("clientName"),
                    "side": d.get("BD_BUY_SELL") or d.get("buySell"),
                    "qty": d.get("BD_QTY_TRD") or d.get("quantity"),
                    "price": d.get("BD_TP_WATP") or d.get("price")})
    return {"ok": True, "n": len(out), "deals": out[:60]}


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
