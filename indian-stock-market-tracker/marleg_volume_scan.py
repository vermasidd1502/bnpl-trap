"""
Volume Pod scan — up/down volume ratio per stock, grouped by sector + industry.

U/D ratio = up-day volume / down-day volume over the last 20 sessions.
  > 1.5 = accumulation (green) ; < 0.7 = distribution (red).
Also computes RVOL (today's vol / 20d avg) and day change.

  python marleg_volume_scan.py            # scan the liquid SEED universe (fast)
  python marleg_volume_scan.py --all      # scan everything classified in marleg_sectors.json

Writes marleg_volume_cache.json (served by /api/volume_pod). Sectors sorted by
median U/D so the most-accumulated sectors float to the top at the open.
"""
import json, os, sys, time
from collections import defaultdict
import numpy as np
import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
SECT = os.path.join(HERE, "marleg_sectors.json")
SYMS = os.path.join(HERE, "marleg_symbols.json")
OUT = os.path.join(HERE, "marleg_volume_cache.json")

# Liquid universe spanning every sector (seed). --all expands to the full classified set.
def load_universe():
    """Full NSE-equity universe from marleg_universe.json (built by marleg_universe_build.py);
    falls back to SEED if the file isn't there."""
    try:
        import json as _j
        d = _j.load(open(os.path.join(HERE, "marleg_universe.json"), encoding="utf-8"))
        u = [x["s"] for x in d.get("stocks", [])]
        if u:
            return sorted(set(u) | set(SEED))     # SEED guarantees the liquid core is in
    except Exception:
        pass
    return list(dict.fromkeys(SEED))


SEED = [
    # Energy / power / oil-gas
    "RELIANCE","ONGC","IOC","BPCL","HINDPETRO","GAIL","OIL","NTPC","POWERGRID","TATAPOWER",
    "ADANIPOWER","JSWENERGY","NHPC","SJVN","COALINDIA","ADANIGREEN","ADANIENSOL",
    # IT
    "TCS","INFY","WIPRO","HCLTECH","TECHM","LTIM","PERSISTENT","COFORGE","MPHASIS","LTTS","OFSS",
    # Banks / financials
    "HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK","INDUSINDBK","BANKBARODA","PNB",
    "IDFCFIRSTB","AUBANK","FEDERALBNK","BAJFINANCE","BAJAJFINSV","SBICARD","CHOLAFIN","SHRIRAMFIN",
    "HDFCLIFE","SBILIFE","ICICIPRULI","ICICIGI","LICI","PFC","RECLTD","IRFC","MUTHOOTFIN","JIOFIN",
    # Auto
    "MARUTI","M&M","EICHERMOT","BAJAJ-AUTO","HEROMOTOCO","TVSMOTOR","ASHOKLEY","BOSCHLTD",
    "MOTHERSON","BALKRISIND","TIINDIA","MRF",
    # Metals / mining
    "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","JINDALSTEL","NMDC","SAIL","NATIONALUM","HINDZINC","APLAPOLLO",
    # Pharma / healthcare
    "SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","AUROPHARMA","LUPIN","ALKEM","TORNTPHARM","BIOCON",
    "ZYDUSLIFE","MANKIND","APOLLOHOSP","MAXHEALTH","FORTIS",
    # FMCG / consumer defensive
    "HINDUNILVR","ITC","NESTLEIND","BRITANNIA","DABUR","MARICO","GODREJCP","COLPAL","TATACONSUM",
    "VBL","UNITDSPR","RADICO",
    # Consumer cyclical / retail
    "TITAN","TRENT","DMART","JUBLFOOD","PAGEIND","ZOMATO","NYKAA","PVRINOX","INDHOTEL","ABFRL","BATAINDIA",
    # Cement / infra / realty
    "ULTRACEMCO","GRASIM","SHREECEM","AMBUJACEM","ACC","LT","DLF","GODREJPROP","OBEROIRLTY","LODHA","PHOENIXLTD",
    # Telecom / media
    "BHARTIARTL","IDEA","INDUSTOWER","SUNTV",
    # Chemicals / fertilisers
    "PIDILITIND","SRF","AARTIIND","DEEPAKNTR","TATACHEM","UPL","PIIND","COROMANDEL","DEEPAKFERT",
    # Capital goods / defence
    "BEL","HAL","BHEL","SIEMENS","ABB","CGPOWER","BDL","MAZDOCK","CUMMINSIND","ABCAPITAL",
    # Logistics / others
    "ADANIENT","ADANIPORTS","IRCTC","CONCOR","INDIGO","DELHIVERY","BSE","CDSL","ANGELONE",
]


def load(p, d):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return d


def metrics(df):
    close = df["Close"].dropna()
    if len(close) < 25:
        return None
    high = df["High"].reindex(close.index); low = df["Low"].reindex(close.index)
    vol = df["Volume"].reindex(close.index).fillna(0)
    d = np.sign(close.diff()).fillna(0)
    def udw(a, b):
        seg = slice(a, b)
        up = float(vol[seg][d[seg] > 0].sum()); dn = float(vol[seg][d[seg] < 0].sum())
        return round(up / dn, 2) if dn > 0 else 3.0
    ud = udw(-20, None)
    ud_prev = udw(-40, -20) if len(close) >= 40 else ud
    rising = ud >= ud_prev
    avg = float(vol[-20:].mean())
    rvol = round(float(vol.iloc[-1]) / avg, 2) if avg > 0 else None
    p, pp = float(close.iloc[-1]), float(close.iloc[-2])
    chg = round((p / pp - 1) * 100, 2)
    pc = close.shift(1)
    tr = pd.concat([(high - low), (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(14).mean().iloc[-1])
    # momentum verdict: volume (U/D) + its direction + price
    if ud >= 1.3 and rising and chg > -2: verdict = "LONG"
    elif ud <= 0.8 and not rising and chg < 2: verdict = "SHORT"
    elif ud >= 1.6: verdict = "LONG"
    elif ud <= 0.6: verdict = "SHORT"
    else: verdict = "FLAT"
    if verdict == "LONG" and ud >= 2.6: verdict = "FLAT"   # over-extended ceiling
    if verdict == "LONG": target, stop = round(p + 2 * atr, 1), round(p - atr, 1)
    elif verdict == "SHORT": target, stop = round(p - 2 * atr, 1), round(p + atr, 1)
    else: target = stop = None
    return {"ud": ud, "rvol": rvol, "price": round(p, 2), "chg": chg,
            "dir": "rising" if rising else "falling", "verdict": verdict,
            "target": target, "stop": stop,
            "tgtpct": round((target / p - 1) * 100, 1) if target else None,
            "control": "bulls" if ud >= 1.0 else "bears"}


def _write(rows, sect, names):
    for m in rows.values():
        if "verdict" not in m:   # fallback for legacy cache rows (no full re-scan yet)
            ud, chg = m.get("ud", 1), m.get("chg", 0)
            m["verdict"] = "LONG" if (ud >= 1.3 and chg > -2) else "SHORT" if (ud <= 0.8 and chg < 2) else "FLAT"
            m["control"] = "bulls" if ud >= 1.0 else "bears"
    bys = defaultdict(list)
    for s, m in rows.items():
        si = sect.get(s, {})
        bys[si.get("sector") or "Others"].append({"s": s, "n": names.get(s, s),
                                                   "industry": si.get("industry") or "Others", **m})
    sectors = []
    for sector, stocks in bys.items():
        uds = [x["ud"] for x in stocks]
        stocks.sort(key=lambda x: -x["ud"])
        longs = sum(1 for x in stocks if x.get("verdict") == "LONG")
        shorts = sum(1 for x in stocks if x.get("verdict") == "SHORT")
        cons = "LONG" if longs > shorts * 1.3 else "SHORT" if shorts > longs * 1.3 else "MIXED"
        sectors.append({"sector": sector, "ud": round(float(np.median(uds)), 2),
                        "n": len(stocks), "strong": sum(1 for u in uds if u >= 1.5),
                        "longs": longs, "shorts": shorts, "consensus": cons, "stocks": stocks})
    sectors.sort(key=lambda x: -x["ud"])
    from datetime import datetime as _dtm, timezone as _tz, timedelta as _td
    _ist = _dtm.now(_tz(_td(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")  # true IST (machine is US-CT)
    out = {"asof": _ist, "universe": len(rows), "sectors": sectors}
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"wrote {len(rows)} stocks across {len(sectors)} sectors -> {OUT}")


def main():
    sect = load(SECT, {})
    names = {r["s"]: r["n"] for r in load(SYMS, [])}
    if "--regroup" in sys.argv:   # re-bucket the cached stocks with refreshed sectors (no re-download)
        flat = [st for sec in load(OUT, {}).get("sectors", []) for st in sec.get("stocks", [])]
        rows = {st["s"]: {k: v for k, v in st.items() if k not in ("s", "n", "industry")} for st in flat}
        print(f"regrouping {len(rows)} cached stocks with refreshed sectors...")
        _write(rows, sect, names)
        return
    if "--universe" in sys.argv:                  # full Groww NSE-equity universe (~3000+)
        universe = load_universe()
    elif "--all" in sys.argv:
        universe = sorted(set(sect) | set(SEED))
    else:
        universe = list(dict.fromkeys(SEED))
    print(f"scanning {len(universe)} symbols...")
    rows = {}
    CH = 80
    for i in range(0, len(universe), CH):
        chunk = universe[i:i + CH]
        try:
            d = yf.download([s + ".NS" for s in chunk], period="2mo", interval="1d",
                            group_by="ticker", progress=False, threads=True)
        except Exception as e:
            print("  chunk fail", str(e)[:60]); continue
        for s in chunk:
            try:
                sub = d[s + ".NS"] if len(chunk) > 1 else d
                m = metrics(sub)
                if m:
                    rows[s] = m
            except Exception:
                pass
        print(f"  {min(i + CH, len(universe))}/{len(universe)}  (kept {len(rows)})")

    _write(rows, sect, names)


if __name__ == "__main__":
    main()
