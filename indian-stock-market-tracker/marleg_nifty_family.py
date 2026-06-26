"""
marleg_nifty_family.py — the NIFTY pod: every index OPTION product (the "flavours of Nifty"), live price +
day change, so you pick the flavour and jump straight into its options. Groww-only, read-only.

Only the flavours that actually HAVE options are listed — that's the whole point (each gets a "→ options"
button). Broad index-only flavours (Nifty Midcap 100 / Smallcap 100/250) are deliberately omitted: Groww
doesn't quote them cleanly AND they have no options to trade, so they'd be dead weight on a trading board.

  python marleg_nifty_family.py
"""
import datetime as dt
import concurrent.futures as cf

import marleg_options_monitor as mom
import marleg_instruments as inst

# index -> its liquid tracking ETF (symbol, name, is_proxy). The ETF is the no-decay way to hold the basket:
# buy/sell it like a stock, no theta, no expiry — the clean lane vs the leveraged option lane.
ETF = {
    "NIFTY": ("NIFTYBEES", "Nifty 50 ETF", False),
    "BANKNIFTY": ("BANKBEES", "Nifty Bank ETF", False),
    "NIFTYNXT50": ("JUNIORBEES", "Nifty Next 50 ETF", False),
    "MIDCPNIFTY": ("MID150BEES", "Nifty Midcap 150 ETF", True),   # proxy — tracks Midcap 150, not the 25-stock Select
}

# (symbol, display name, exchange) — every NSE/BSE index with listed options
PRODUCTS = [
    ("NIFTY", "Nifty 50", "NSE"),
    ("BANKNIFTY", "Bank Nifty", "NSE"),
    ("FINNIFTY", "Fin Nifty", "NSE"),
    ("MIDCPNIFTY", "Nifty Midcap Select", "NSE"),
    ("NIFTYNXT50", "Nifty Next 50", "NSE"),
    ("SENSEX", "Sensex", "BSE"),
    ("BANKEX", "Bankex", "BSE"),
]


def _one(item):
    sym, name, ex = item
    price = chg = None
    try:
        g = mom._g()
        r = g.quote(sym, segment="CASH", exchange=ex)
        p = (r.json().get("payload") or {})
        price = p.get("last_price") or p.get("ltp")
        chg = p.get("day_change_perc")
    except Exception:
        pass
    exps = inst.expiries(sym, within_days=220)
    if not exps:                                          # master read flakes under the parallel burst — retry once
        try:
            exps = inst.expiries(sym, within_days=220)
        except Exception:
            exps = []
    has_weekly = any((dt.date.fromisoformat(exps[i + 1]) - dt.date.fromisoformat(exps[i])).days <= 12
                     for i in range(min(2, len(exps) - 1))) if len(exps) >= 2 else False
    etf = None
    em = ETF.get(sym)
    if em:
        try:
            import marleg_data as md
            d = md.candles(em[0], 1440, 6)
            if d is not None and len(d) >= 2:
                cc = d["close"].dropna()
                epx = float(cc.iloc[-1]); ev = float(d["volume"].iloc[-1])
                etf = {"symbol": em[0], "name": em[1], "proxy": em[2], "price": round(epx, 2),
                       "chg_pct": round((epx / float(cc.iloc[-2]) - 1) * 100, 2),
                       "volume": int(ev) if ev == ev else None}
        except Exception:
            pass
    return {"symbol": sym, "name": name, "exchange": ex,
            "price": round(price, 2) if price else None,
            "chg_pct": round(chg, 2) if chg is not None else None,
            "has_options": True, "n_expiries": len(exps),   # every PRODUCT is an option product by construction
            "next_expiry": exps[0] if exps else None,
            "cadence": ("weekly+monthly" if has_weekly else "monthly") if exps else "options", "etf": etf}


def family():
    try:
        inst.expiries("NIFTY")            # pre-warm the instruments master so the parallel has_options reads hit cache
    except Exception:
        pass
    with cf.ThreadPoolExecutor(max_workers=7) as ex:
        rows = list(ex.map(_one, PRODUCTS))
    rows = [r for r in rows if r["price"]]
    rows.sort(key=lambda r: (r["exchange"] != "NSE", PRODUCTS.index(next(p for p in PRODUCTS if p[0] == r["symbol"]))))
    return {"ok": True, "n": len(rows), "family": rows,
            "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
            "note": "Index option products (the Nifty flavours) — live price + day change; every one has options, "
                    "so each gets a → options jump.",
            "caveat": "Read-only. Broad index-only flavours (Midcap 100 / Smallcap) are omitted — no options + "
                      "Groww doesn't quote them cleanly."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = family()
    print(f"\n  NIFTY POD — {r['asof']}")
    for x in r["family"]:
        ch = x["chg_pct"]
        print(f"    {x['name']:<22} {x['exchange']}  ₹{x['price']:<11} {('+' if (ch or 0) >= 0 else '')}{ch}%   "
              f"{x['cadence']:<14} → options")
