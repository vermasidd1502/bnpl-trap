"""
marleg_mf.py — mutual-fund universe: search + category/sector classification.

Data source: api.mfapi.in (free AMFI NAV mirror).
  - /mf            -> master list of every scheme [{schemeCode, schemeName}]  (~11k rows)
  - /mf/<code>     -> NAV history + meta (used by the server's existing /api/mf route)

There is no free category field in the master list, so we classify each scheme by
keyword on its NAME into:
  - cap / style buckets  (Large / Mid / Small / Flexi / Multi / ELSS / Index / Value / …)
  - sector / industry buckets  (Banking, Pharma, IT, Infra, Consumption, Energy, Auto, …)
  - Debt, Hybrid, International, Commodity, Other

For BROWSING we keep only Direct-Growth plans (one row per fund instead of the 4
Regular/Direct × Growth/IDCW variants). SEARCH spans everything but ranks Direct-Growth first.
Classification is name-based and therefore approximate — good enough to navigate ~3k funds,
and honest about it.
"""
import re
import time
import requests

# ---- module-level master cache (the list is ~1MB; refetch at most every few hours) ----
_MASTER = {"t": 0.0, "data": None}
_TTL = 6 * 3600


def _master():
    if _MASTER["data"] is not None and (time.time() - _MASTER["t"]) < _TTL:
        return _MASTER["data"]
    try:
        r = requests.get("https://api.mfapi.in/mf", timeout=25)
        data = r.json() if r.status_code == 200 else []
    except Exception:
        data = _MASTER["data"] or []
    if data:
        _MASTER["data"] = data
        _MASTER["t"] = time.time()
    return data


# ---- classification rules (checked in this order) ----------------------------
# DEBT first (so "Banking & PSU Debt" doesn't get tagged as the Banking sector),
# then HYBRID, then SECTOR/THEMATIC, then CAP/STYLE, then INTERNATIONAL/COMMODITY, else Other.
_DEBT = re.compile(r"\b(DEBT|BOND|GILT|LIQUID|OVERNIGHT|MONEY MARKET|DURATION|"
                   r"BANKING & PSU|BANKING AND PSU|CREDIT RISK|CORPORATE BOND|FLOATER|"
                   r"FLOATING RATE|G-?SEC|GOVERNMENT SECURIT|INCOME FUND|SHORT TERM|"
                   r"DYNAMIC BOND|FIXED MATURITY|FMP)\b", re.I)
_HYBRID = re.compile(r"\b(HYBRID|BALANCED|ARBITRAGE|EQUITY SAVINGS|MULTI ASSET|MULTI-ASSET|"
                     r"ASSET ALLOCAT|AGGRESSIVE|CONSERVATIVE|DYNAMIC ASSET|RETIREMENT|"
                     r"CHILDREN|CHILD)\b", re.I)
# sector / industry -> bucket label
_SECTORS = [
    (re.compile(r"\b(PHARMA|HEALTH|HEALTHCARE)\b", re.I), "Sector · Pharma & Healthcare"),
    (re.compile(r"\b(TECHNOLOGY|TECHNOLOG|INFOTECH|DIGITAL|TECH FUND)\b", re.I), "Sector · Technology / IT"),
    (re.compile(r"\b(BANK|BANKING|FINANCIAL|FIN SERV|FINANCIAL SERVICES)\b", re.I), "Sector · Banking & Financial"),
    (re.compile(r"\b(INFRA|INFRASTRUCTURE)\b", re.I), "Sector · Infrastructure"),
    (re.compile(r"\b(CONSUM|FMCG|MNC)\b", re.I), "Sector · Consumption / FMCG"),
    (re.compile(r"\b(ENERGY|POWER|OIL|GAS|UTILIT)\b", re.I), "Sector · Energy & Power"),
    (re.compile(r"\b(AUTO|AUTOMOBILE|MOBILITY)\b", re.I), "Sector · Auto"),
    (re.compile(r"\b(MANUFACTUR|INDUSTRIAL)\b", re.I), "Sector · Manufacturing"),
    (re.compile(r"\b(REALTY|REAL ESTATE|HOUSING)\b", re.I), "Sector · Realty / Housing"),
    (re.compile(r"\b(METAL|RESOURCES|COMMODITIES)\b", re.I), "Sector · Metals & Resources"),
    (re.compile(r"\b(MEDIA|ENTERTAINMENT|TELECOM)\b", re.I), "Sector · Media & Telecom"),
    (re.compile(r"\b(DEFENCE|DEFENSE)\b", re.I), "Sector · Defence"),
    (re.compile(r"\b(TRANSPORT|LOGISTIC)\b", re.I), "Sector · Transport & Logistics"),
    (re.compile(r"\bESG\b", re.I), "Sector · ESG"),
    (re.compile(r"\b(PSU|CPSE)\b", re.I), "Sector · PSU"),
    (re.compile(r"\b(BUSINESS CYCLE|SPECIAL OPPORTUNIT|SPECIAL SITUATION|THEMATIC|SECTORAL|OPPORTUNITIES)\b", re.I), "Sector · Thematic (other)"),
]
# cap / style -> bucket label
_CAPS = [
    (re.compile(r"\b(LARGE & MID|LARGE AND MID|LARGE\s*&\s*MIDCAP)\b", re.I), "Large & Mid Cap"),
    (re.compile(r"\b(MID\s*CAP|MIDCAP)\b", re.I), "Mid Cap"),
    (re.compile(r"\b(SMALL\s*CAP|SMALLCAP)\b", re.I), "Small Cap"),
    (re.compile(r"\b(LARGE\s*CAP|LARGECAP|BLUECHIP|BLUE CHIP|TOP 100|TOP 50)\b", re.I), "Large Cap"),
    (re.compile(r"\b(FLEXI\s*CAP|FLEXICAP)\b", re.I), "Flexi Cap"),
    (re.compile(r"\b(MULTI\s*CAP|MULTICAP)\b", re.I), "Multi Cap"),
    (re.compile(r"\b(ELSS|TAX SAVER|TAX SAVING|LONG TERM EQUITY|TAXSHIELD)\b", re.I), "ELSS (Tax Saver)"),
    (re.compile(r"\b(FOCUSED|FOCUSSED)\b", re.I), "Focused"),
    (re.compile(r"\b(VALUE|CONTRA)\b", re.I), "Value / Contra"),
    (re.compile(r"\b(DIVIDEND YIELD)\b", re.I), "Dividend Yield"),
    (re.compile(r"\b(INDEX|NIFTY|SENSEX|BSE|NASDAQ 100 INDEX)\b", re.I), "Index / ETF"),
]
_INTL = re.compile(r"\b(INTERNATIONAL|GLOBAL|US EQUIT|U\.S\.|NASDAQ|GREATER CHINA|EMERGING|"
                   r"WORLDWIDE|OVERSEAS|EUROPE|JAPAN|FANG)\b", re.I)
_COMMOD = re.compile(r"\b(GOLD|SILVER|COMMODITY)\b", re.I)


def classify(name):
    """Return (group, bucket). group ∈ {cap, sector, debt, hybrid, intl, commodity, other}."""
    n = name or ""
    if _DEBT.search(n):
        return ("debt", "Debt")
    if _HYBRID.search(n):
        return ("hybrid", "Hybrid")
    if _COMMOD.search(n):
        return ("commodity", "Commodity (Gold/Silver)")
    for rx, label in _SECTORS:
        if rx.search(n):
            return ("sector", label)
    for rx, label in _CAPS:
        if rx.search(n):
            # Index funds that are international
            if label == "Index / ETF" and _INTL.search(n):
                return ("intl", "International")
            return ("cap", label)
    if _INTL.search(n):
        return ("intl", "International")
    return ("other", "Other / Diversified")


def _is_direct_growth(name):
    n = (name or "").upper()
    return ("DIRECT" in n) and ("GROWTH" in n) and ("IDCW" not in n) and ("DIVIDEND" not in n)


# group display order
_GROUP_ORDER = ["cap", "sector", "hybrid", "debt", "intl", "commodity", "other"]
_GROUP_LABEL = {"cap": "By market cap / style", "sector": "By sector / industry",
                "hybrid": "Hybrid", "debt": "Debt", "intl": "International",
                "commodity": "Commodity", "other": "Other"}


def directory():
    """Bucket counts grouped for the browser (Direct-Growth plans only)."""
    data = _master()
    groups = {}          # group -> {bucket: count}
    total = 0
    for s in data:
        nm = s.get("schemeName") or ""
        if not _is_direct_growth(nm):
            continue
        g, b = classify(nm)
        groups.setdefault(g, {}).setdefault(b, 0)
        groups[g][b] += 1
        total += 1
    out = []
    for g in _GROUP_ORDER:
        if g not in groups:
            continue
        buckets = sorted(groups[g].items(), key=lambda kv: -kv[1])
        out.append({"group": g, "label": _GROUP_LABEL.get(g, g),
                    "buckets": [{"bucket": b, "count": c} for b, c in buckets]})
    return {"total_direct_growth": total, "groups": out}


def category(bucket, limit=400):
    """All Direct-Growth funds whose classified bucket == `bucket`."""
    data = _master()
    out = []
    for s in data:
        nm = s.get("schemeName") or ""
        if not _is_direct_growth(nm):
            continue
        if classify(nm)[1] == bucket:
            out.append({"code": str(s.get("schemeCode")), "name": nm})
    out.sort(key=lambda x: x["name"])
    return {"bucket": bucket, "count": len(out), "funds": out[:limit]}


def search(q, limit=40):
    """Substring search over all schemes; Direct-Growth ranked first, shorter names first."""
    q = (q or "").strip().upper()
    if len(q) < 2:
        return {"q": q, "results": []}
    data = _master()
    hits = []
    for s in data:
        nm = s.get("schemeName") or ""
        if q in nm.upper():
            hits.append({"code": str(s.get("schemeCode")), "name": nm,
                         "dg": _is_direct_growth(nm), "cat": classify(nm)[1]})
    hits.sort(key=lambda x: (0 if x["dg"] else 1, len(x["name"])))
    return {"q": q, "count": len(hits), "results": hits[:limit]}


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    d = directory()
    print(f"Direct-Growth universe: {d['total_direct_growth']} funds")
    for grp in d["groups"]:
        print(f"\n[{grp['label']}]")
        for b in grp["buckets"]:
            print(f"  {b['bucket']:<34} {b['count']}")
