"""
marleg_index_universe.py — the NIFTY POD: every index, sector and commodity you can actually trade, in one
grouped live board. Groww-only (no yfinance — yfinance can't reliably serve India sectoral indices anyway).

The honest design: Groww doesn't quote the sectoral INDICES (Nifty IT / Pharma / Metal …), but it DOES serve
their tracking ETFs natively — and the ETF is the thing you'd buy, so we track the ETF. Each row's % change IS
the sector/commodity move. F&O indices (Nifty / Bank / Fin / Midcap-Select / Next-50 / Sensex) come from the
live quote; everything else from the ETF candle.

Groups: BROAD (the headline indices, → options) · SECTORS (every liquid sector ETF) · COMMODITIES
(gold / silver / basket) · GLOBAL (Nasdaq / FANG / Hang Seng — for pairing). Plus COMMODITY↔NIFTY correlation
so you can pair them (gold as a hedge, metal/energy as a tailwind to their sectors).

  python marleg_index_universe.py
"""
import datetime as dt
import concurrent.futures as cf

import marleg_options_monitor as mom
import marleg_instruments as inst
import marleg_data as md

# (symbol, display, kind)  kind: "idx" = F&O index via live quote · "etf" = ETF via candle
GROUPS = {
    "broad": [("NIFTY", "Nifty 50", "idx"), ("BANKNIFTY", "Bank Nifty", "idx"), ("FINNIFTY", "Fin Nifty", "idx"),
              ("MIDCPNIFTY", "Nifty Midcap Select", "idx"), ("NIFTYNXT50", "Nifty Next 50", "idx"),
              ("SENSEX", "Sensex", "idx"), ("MOM100", "Nifty Midcap 100", "etf")],
    "sectors": [("BANKBEES", "Bank", "etf"), ("ITBEES", "IT", "etf"), ("PHARMABEES", "Pharma", "etf"),
                ("AUTOBEES", "Auto", "etf"), ("FMCGIETF", "FMCG", "etf"), ("METALIETF", "Metal", "etf"),
                ("PSUBNKBEES", "PSU Bank", "etf"), ("PVTBANIETF", "Private Bank", "etf"),
                ("CONSUMBEES", "Consumer", "etf"), ("HEALTHIETF", "Healthcare", "etf"),
                ("INFRABEES", "Infra", "etf"), ("CPSEETF", "CPSE / PSU", "etf")],
    "commodities": [("GOLDBEES", "Gold", "etf"), ("SILVERBEES", "Silver", "etf"), ("COMMOIETF", "Commodities", "etf")],
    "global": [("MON100", "Nasdaq 100", "etf"), ("MAFANG", "US FANG+", "etf"), ("HNGSNGBEES", "Hang Seng", "etf")],
}
_EX = {"SENSEX": "BSE", "BANKEX": "BSE"}


def _quote_idx(sym):
    try:
        g = mom._g()
        r = g.quote(sym, segment="CASH", exchange=_EX.get(sym, "NSE"))
        p = (r.json().get("payload") or {})
        return p.get("last_price") or p.get("ltp"), p.get("day_change_perc")
    except Exception:
        return None, None


def _quote_etf(sym):
    df = md.candles(sym, 1440, 6)
    if df is None or len(df) < 2:
        return None, None, None
    c = df["close"].dropna()
    px = float(c.iloc[-1])
    v = float(df["volume"].iloc[-1])
    return round(px, 2), round((px / float(c.iloc[-2]) - 1) * 100, 2), (int(v) if v == v else None)


def _one(item, group):
    sym, name, kind = item
    if kind == "idx":
        px, chg = _quote_idx(sym)
        vol = None
        opts = True
    else:
        px, chg, vol = _quote_etf(sym)
        opts = False
    return {"symbol": sym, "name": name, "group": group, "kind": kind,
            "price": round(px, 2) if px else None, "chg_pct": round(chg, 2) if chg is not None else None,
            "volume": vol, "has_options": opts}


def _corr(a_rets, b_rets):
    import statistics
    n = min(len(a_rets), len(b_rets))
    if n < 15:
        return None
    a, b = a_rets[-n:], b_rets[-n:]
    ma, mb = statistics.mean(a), statistics.mean(b)
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    da = sum((x - ma) ** 2 for x in a) ** 0.5
    db = sum((x - mb) ** 2 for x in b) ** 0.5
    return round(cov / (da * db), 2) if da and db else None


def _rets(sym, days=70):
    df = md.candles(sym, 1440, days)
    if df is None or len(df) < 20:
        return []
    c = df["close"].dropna().values
    return [(c[i] / c[i - 1] - 1) for i in range(1, len(c))]


def universe():
    try:
        inst.expiries("NIFTY")                       # pre-warm master
    except Exception:
        pass
    jobs = [(it, g) for g, items in GROUPS.items() for it in items]
    with cf.ThreadPoolExecutor(max_workers=10) as ex:
        rows = list(ex.map(lambda t: _one(*t), jobs))
    rows = [r for r in rows if r["price"]]
    out = {g: [r for r in rows if r["group"] == g] for g in GROUPS}

    # commodity <-> nifty pairing (daily-return correlation over ~70d)
    pairs = []
    try:
        nr = _rets("NIFTYBEES")
        for sym, nm in [("GOLDBEES", "Gold"), ("SILVERBEES", "Silver"), ("COMMOIETF", "Commodities")]:
            cr = _rets(sym)
            corr = _corr(nr, cr)
            if corr is not None:
                tag = ("hedge (moves opposite Nifty)" if corr <= -0.15 else
                       "decoupled (own driver)" if abs(corr) < 0.15 else "rides with Nifty")
                pairs.append({"commodity": nm, "symbol": sym, "corr_nifty": corr, "read": tag})
    except Exception:
        pass

    asof = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")
    return {"ok": True, "n": len(rows), "groups": out, "pairs": pairs, "asof": asof,
            "note": "Live index/sector/commodity board — all Groww-native. Sectors tracked via their ETF (the "
                    "tradeable basket; % change = the sector move). F&O indices via live quote (→ options).",
            "caveat": "Read-only, decision-support. Sector ETFs ≈ their index (tiny tracking error / expense). "
                      "Broad index-only flavours without an ETF are omitted. Not investment advice."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    u = universe()
    print(f"\n  NIFTY POD — index · sector · commodity — {u['asof']}")
    for g in GROUPS:
        print(f"\n  {g.upper()}")
        for r in sorted(u["groups"][g], key=lambda x: -(x["chg_pct"] or -99)):
            print(f"    {r['name']:<20} ₹{r['price']:<11} {('+' if (r['chg_pct'] or 0) >= 0 else '')}{r['chg_pct']}%"
                  + ("  → options" if r["has_options"] else ""))
    print("\n  COMMODITY ↔ NIFTY")
    for p in u["pairs"]:
        print(f"    {p['commodity']:<12} corr {p['corr_nifty']:+}  — {p['read']}")
