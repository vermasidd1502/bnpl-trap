"""
marleg_options_monitor.py — live options monitor off the Groww quote payload.

Groww's /v1/live-data/quote (segment=FNO) returns, per option instrument:
  last_price · implied_volatility · open_interest + oi_day_change(+pct) · previous_open_interest
  · full 5-level depth {buy:[{price,quantity,orderCount}], sell:[...]} · total_buy/sell_quantity
  · bid/offer price+qty · volume · ohlc · circuit limits.

We turn that raw payload into a tradeable read:
  - LIQUIDITY    bid-ask spread %, depth size, OI, volume        -> can you actually get filled?
  - OI SIGNAL    oi_day_change × underlying move                 -> long/short buildup vs covering/unwinding
  - VOL          Groww IV (direct) vs realized vol + India VIX   -> rich / fair / cheap
  - GREEKS       delta/gamma/vega/theta (reused from marleg_vol)
  - DEPTH IMBAL  Σ buy qty vs Σ sell qty across the ladder       -> bid-heavy / ask-heavy

Plus a constructed ATM±N chain (nearest monthly expiry) and an F&O-universe check
for the "📊 options" button. Read-only — never places an order.
"""
import datetime as dt
import marleg_vol as mv          # parse_option, last_thursday, greeks, realized_vol, india_vix, R_FREE, MONTHS

# month number -> 3-letter NSE code (reverse of mv.MONTHS)
_MONNUM = {v: k for k, v in mv.MONTHS.items()}

# ---- NSE F&O underlyings (maintained list; ~the liquid stock-derivatives set) ----
# Used only to decide "does this stock have options" for the button. Not exhaustive law;
# update as NSE adds/removes names. The user's small/mid-caps (TEJASNET, DLINKINDIA,
# AEGISVOPAK, …) are intentionally absent -> they correctly report "no options".
FNO_UNDERLYINGS = {
    "AARTIIND","ABB","ABBOTINDIA","ABCAPITAL","ABFRL","ACC","ADANIENT","ADANIPORTS","ALKEM",
    "AMBUJACEM","APOLLOHOSP","APOLLOTYRE","ASHOKLEY","ASIANPAINT","ASTRAL","ATGL","AUBANK",
    "AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BALKRISIND","BANDHANBNK",
    "BANKBARODA","BATAINDIA","BEL","BERGEPAINT","BHARATFORG","BHARTIARTL","BHEL","BIOCON",
    "BOSCHLTD","BPCL","BRITANNIA","BSOFT","CANBK","CANFINHOME","CHAMBLFERT","CHOLAFIN","CIPLA",
    "COALINDIA","COFORGE","COLPAL","CONCOR","COROMANDEL","CROMPTON","CUMMINSIND","DABUR",
    "DALBHARAT","DEEPAKNTR","DELHIVERY","DIVISLAB","DIXON","DLF","DRREDDY","EICHERMOT","ESCORTS",
    "EXIDEIND","FEDERALBNK","GAIL","GLENMARK","GMRINFRA","GNFC","GODREJCP","GODREJPROP","GRANULES",
    "GRASIM","GUJGASLTD","HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO",
    "HINDALCO","HINDCOPPER","HINDPETRO","HINDUNILVR","ICICIBANK","ICICIGI","ICICIPRULI","IDEA",
    "IDFCFIRSTB","IEX","IGL","INDHOTEL","INDIAMART","INDIGO","INDUSINDBK","INDUSTOWER","INFY",
    "IOC","IPCALAB","IRCTC","ITC","JINDALSTEL","JKCEMENT","JSWSTEEL","JUBLFOOD","KOTAKBANK",
    "LALPATHLAB","LAURUSLABS","LICHSGFIN","LT","LTF","LTIM","LTTS","LUPIN","M&M","M&MFIN",
    "MANAPPURAM","MARICO","MARUTI","MCX","METROPOLIS","MFSL","MGL","MOTHERSON","MPHASIS",
    "MRF","MUTHOOTFIN","NATIONALUM","NAUKRI","NAVINFLUOR","NESTLEIND","NMDC","NTPC","OBEROIRLTY",
    "OFSS","ONGC","PAGEIND","PEL","PERSISTENT","PETRONET","PFC","PIDILITIND","PIIND","PNB",
    "POLYCAB","POWERGRID","PVRINOX","RAMCOCEM","RBLBANK","RECLTD","RELIANCE","SAIL","SBICARD",
    "SBILIFE","SBIN","SHREECEM","SHRIRAMFIN","SIEMENS","SRF","SUNPHARMA","SUNTV","SYNGENE",
    "TATACHEM","TATACOMM","TATACONSUM","TATAMOTORS","TATAPOWER","TATASTEEL","TCS","TECHM",
    "TIINDIA","TITAN","TORNTPHARM","TRENT","TVSMOTOR","UBL","ULTRACEMCO","UNITDSPR","UPL",
    "VEDL","VOLTAS","WIPRO","ZYDUSLIFE",
}


def has_options(tk):
    return (tk or "").upper().strip() in FNO_UNDERLYINGS


def _g():
    try:
        import groww_client
        g = groww_client.GrowwClient(); g.token()
        return g
    except Exception:
        return None


def _strike_step(spot):
    """Heuristic NSE strike interval by price band — used to construct a synthetic chain."""
    if spot is None:
        return 50
    if spot < 100:
        return 2.5
    if spot < 250:
        return 5
    if spot < 500:
        return 10
    if spot < 1000:
        return 20
    if spot < 2500:
        return 50
    return 100


def nearest_monthly_expiry(today=None):
    today = today or dt.date.today()
    e = mv.last_thursday(today.year, today.month)
    if today > e:                                   # this month's expiry passed -> next month
        y, m = (today.year + (today.month == 12)), (1 if today.month == 12 else today.month + 1)
        e = mv.last_thursday(y, m)
    return e


def build_symbol(und, strike, kind, expiry):
    """NSE monthly option trading symbol: UND + YY + MON + STRIKE + CE/PE (e.g. RELIANCE26JUL3000CE)."""
    yy = expiry.strftime("%y")
    mon = _MONNUM[expiry.month]
    k = int(round(strike))
    return f"{und.upper()}{yy}{mon}{k}{'CE' if kind in ('C', 'CE') else 'PE'}"


def _norm_iv(iv):
    """Groww IV may come as percent (28.0) or decimal (0.28). Normalize to decimal."""
    if iv is None:
        return None
    iv = float(iv)
    return iv / 100.0 if iv > 3 else iv


def _depth_summary(depth):
    """5-level ladder -> top bid/ask + Σ qty each side + imbalance."""
    buy = [d for d in (depth.get("buy") or []) if (d.get("price") or 0) > 0]
    sell = [d for d in (depth.get("sell") or []) if (d.get("price") or 0) > 0]
    bq = sum(d.get("quantity") or 0 for d in buy)
    sq = sum(d.get("quantity") or 0 for d in sell)
    top_bid = buy[0]["price"] if buy else None
    top_ask = sell[0]["price"] if sell else None
    tot = bq + sq
    imb = round((bq - sq) / tot, 2) if tot else 0.0     # +1 all bids, -1 all asks
    return {"buy": buy, "sell": sell, "bid_qty": bq, "ask_qty": sq, "imbalance": imb,
            "top_bid": top_bid, "top_ask": top_ask}


def option_quote(sym):
    """Live parsed quote for one option symbol via Groww FNO. None on failure."""
    g = _g()
    if not g:
        return {"error": "groww unavailable"}
    try:
        r = g.quote(sym, segment="FNO")
        if r.status_code != 200:
            return {"error": f"quote HTTP {r.status_code}"}
        p = (r.json() or {}).get("payload") or {}
    except Exception as e:
        return {"error": str(e)[:120]}
    if not p:
        return {"error": "empty payload (symbol may not exist / not listed)"}
    dep = _depth_summary(p.get("depth") or {})
    ltp = p.get("last_price")
    bid = p.get("bid_price") or dep["top_bid"]
    ask = p.get("offer_price") or dep["top_ask"]
    spread = (ask - bid) if (bid and ask) else None
    mid = ((ask + bid) / 2) if (bid and ask) else ltp
    spread_pct = round(spread / mid * 100, 2) if (spread and mid) else None
    return {
        "symbol": sym, "ltp": ltp, "iv_raw": p.get("implied_volatility"),
        "iv": _norm_iv(p.get("implied_volatility")),
        "oi": p.get("open_interest"), "prev_oi": p.get("previous_open_interest"),
        "oi_change": p.get("oi_day_change"), "oi_change_pct": p.get("oi_day_change_percentage"),
        "volume": p.get("volume"), "bid": bid, "ask": ask, "spread": spread, "spread_pct": spread_pct,
        "total_buy_qty": p.get("total_buy_quantity"), "total_sell_qty": p.get("total_sell_quantity"),
        "depth": dep, "day_change_pct": p.get("day_change_perc"),
    }


def _liquidity_verdict(q):
    sp = q.get("spread_pct"); oi = q.get("oi") or 0; vol = q.get("volume") or 0
    if sp is None and not oi and not vol:
        return "⚪ NO DATA — market closed or illiquid"
    if (oi or 0) < 100 and (vol or 0) < 50:
        return "🔴 ILLIQUID — almost no OI/volume; you'll struggle to enter or exit"
    if sp is not None and sp > 5:
        return "🟠 WIDE — bid-ask >5% of premium; slippage will eat the trade"
    if sp is not None and sp <= 1.5:
        return "🟢 TIGHT — bid-ask ≤1.5%; clean to trade"
    return "🟡 OK — tradeable, watch the spread"


def _oi_signal(oi_change, und_day_change, kind):
    """Classic F&O OI-price matrix (on the OPTION's OI vs the UNDERLYING's move)."""
    if oi_change is None or und_day_change is None:
        return "—"
    up = und_day_change > 0
    add = oi_change > 0
    if up and add:
        return "📈 LONG BUILDUP — price up + OI up: fresh longs, trend has conviction"
    if up and not add:
        return "🟢 SHORT COVERING — price up + OI down: shorts bailing, bounce may be less durable"
    if (not up) and add:
        return "📉 SHORT BUILDUP — price down + OI up: fresh shorts, bearish pressure"
    return "🟡 LONG UNWINDING — price down + OI down: longs exiting, weak hands leaving"


def analyze(sym, side="long", qty=0):
    """Full monitor read for one option: liquidity + OI signal + vol(rich/cheap) + Greeks + depth."""
    q = option_quote(sym)
    if "error" in q:
        return q
    base = mv.analyze_option(sym, side=side, qty=qty)   # parse + spot + premium + IV + greeks + iv_vs_rv
    iv = q.get("iv") or base.get("iv")
    rv20 = base.get("rv20")
    vrp = (iv - rv20) if (iv and rv20) else None
    vol_verdict = base.get("vol_verdict")
    if vrp is not None and not vol_verdict:
        vol_verdict = ("RICH (IV≫RV → favour selling vol / spreads)" if vrp > 0.04 else
                       "CHEAP (IV≪RV → favour buying vol)" if vrp < -0.04 else "FAIR (IV≈RV)")
    return {
        "symbol": sym, "underlying": base.get("underlying"), "right": base.get("right"),
        "strike": base.get("strike"), "days_to_expiry": base.get("days_to_expiry"),
        "spot": base.get("spot"), "premium": q.get("ltp") or base.get("premium"),
        "iv": round(iv, 4) if iv else None, "rv20": rv20, "rv60": base.get("rv60"),
        "iv_vs_rv": round(vrp, 4) if vrp is not None else None, "vol_verdict": vol_verdict,
        "india_vix": base.get("india_vix"),
        "oi": q.get("oi"), "oi_change": q.get("oi_change"), "oi_change_pct": q.get("oi_change_pct"),
        "volume": q.get("volume"), "bid": q.get("bid"), "ask": q.get("ask"),
        "spread": q.get("spread"), "spread_pct": q.get("spread_pct"),
        "liquidity": _liquidity_verdict(q),
        "oi_signal": _oi_signal(q.get("oi_change"), q.get("day_change_pct"), base.get("right")),
        "depth": q.get("depth"),
        "greeks_per_share": base.get("greeks_per_share"),
        "position_greeks": base.get("position_greeks"),
    }


def chain(underlying, n=5, expiry=None):
    """Constructed ATM±n strikes for the nearest monthly expiry. Best-effort — strikes that
    aren't listed simply return empty and are skipped. Returns spot + per-strike CE/PE rows."""
    und = (underlying or "").upper().strip()
    if not has_options(und):
        return {"error": f"{und} is not in the NSE F&O universe — no options to chain"}
    g = _g()
    spot = mv.underlying_ltp(und, g)
    if not spot:
        return {"error": f"no spot price for {und}"}
    exp = expiry or nearest_monthly_expiry()
    T = max((exp - dt.date.today()).days, 0) / 365.0
    step = _strike_step(spot)
    atm = round(spot / step) * step
    strikes = [atm + i * step for i in range(-n, n + 1)]
    rows = []
    for k in strikes:
        ce_sym, pe_sym = build_symbol(und, k, "C", exp), build_symbol(und, k, "P", exp)
        ce, pe = option_quote(ce_sym), option_quote(pe_sym)
        row = {"strike": k, "is_atm": abs(k - atm) < step / 2}
        for tag, oq, kind, osym in (("ce", ce, "C", ce_sym), ("pe", pe, "P", pe_sym)):
            if "error" not in oq and (oq.get("ltp") or oq.get("oi")):
                iv = oq.get("iv")
                if iv is None and oq.get("ltp") and spot and T > 0:   # Groww IV null (e.g. closed) -> invert BS
                    try:
                        iv = mv.implied_vol(oq["ltp"], spot, k, T, mv.R_FREE, kind)
                    except Exception:
                        iv = None
                row[tag] = {"sym": osym, "ltp": oq.get("ltp"), "iv": round(iv, 4) if iv else None,
                            "oi": oq.get("oi"), "oi_change": oq.get("oi_change"),
                            "volume": oq.get("volume"), "spread_pct": oq.get("spread_pct")}
        if "ce" in row or "pe" in row:
            rows.append(row)
    # crude PCR from listed OI
    coi = sum((r.get("ce") or {}).get("oi") or 0 for r in rows)
    poi = sum((r.get("pe") or {}).get("oi") or 0 for r in rows)
    return {"underlying": und, "spot": spot, "atm": atm, "expiry": exp.isoformat(),
            "days_to_expiry": (exp - dt.date.today()).days, "step": step,
            "pcr_oi": round(poi / coi, 2) if coi else None,
            "india_vix": mv.india_vix(), "rv20": mv.realized_vol(und, 20), "rows": rows}


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    arg = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    print(json.dumps(chain(arg) if has_options(arg) else analyze(arg), indent=2, default=str))
