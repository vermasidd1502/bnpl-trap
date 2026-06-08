"""Merge NSE official sector classification into marleg_sectors.json.
   sector  = NSE 'Industry' (Indian 22-sector taxonomy)   [authoritative, broad coverage]
   industry= granular yfinance sub-sector if we have it, else the NSE sector
"""
import csv, json, os
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
nse = {}
for f in ("nse_ntm.csv", "nse_n500.csv"):
    p = os.path.join(HERE, f)
    if not os.path.exists(p):
        continue
    with open(p, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            sym = (r.get("Symbol") or "").strip()
            ind = (r.get("Industry") or "").strip()
            if sym and ind:
                nse[sym] = ind

cache = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
upd = 0
for sym, sec in nse.items():
    yf_ind = cache.get(sym, {}).get("industry")
    sub = yf_ind if (yf_ind and yf_ind != "Others") else sec
    cache[sym] = {"sector": sec, "industry": sub}
    upd += 1

# Unify leftover yfinance/GICS sectors into the NSE 22-sector taxonomy
GICS2NSE = {"Technology": "Information Technology", "Industrials": "Capital Goods",
            "Energy": "Oil Gas & Consumable Fuels", "Real Estate": "Realty",
            "Consumer Defensive": "Fast Moving Consumer Goods",
            "Communication Services": "Telecommunication", "Utilities": "Power"}
def normalize(sector, industry):
    if sector in GICS2NSE:
        return GICS2NSE[sector]
    i = (industry or "").lower()
    if sector == "Basic Materials":
        if "chemical" in i or "agric" in i: return "Chemicals"
        if "building" in i or "cement" in i: return "Construction Materials"
        if "paper" in i or "lumber" in i or "forest" in i: return "Forest Materials"
        return "Metals & Mining"
    if sector == "Consumer Cyclical":
        if "auto" in i: return "Automobile and Auto Components"
        if "textile" in i or "apparel" in i or "footwear" in i: return "Textiles"
        if "furnish" in i or "appliance" in i or "durable" in i or "luxury" in i: return "Consumer Durables"
        return "Consumer Services"
    return sector
for v in cache.values():
    if v.get("sector") and v["sector"] != "Others":
        v["sector"] = normalize(v["sector"], v.get("industry"))
json.dump(cache, open(os.path.join(HERE, "marleg_sectors.json"), "w", encoding="utf-8"), ensure_ascii=False)

real = sum(1 for v in cache.values() if v["sector"] != "Others")
print(f"NSE symbols merged: {upd} | real-sector now: {real}/{len(cache)} (was 417)")
for s, c in Counter(v["sector"] for v in cache.values() if v["sector"] != "Others").most_common():
    print(f"  {s:<32} {c}")
