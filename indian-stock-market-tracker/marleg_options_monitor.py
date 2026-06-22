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


# Index option underlyings + their NSE strike intervals (the price-band heuristic gets these wrong:
# NIFTY strikes are every 50, not the 100 a >2500 price implies). These are "the main" options.
INDEX_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25, "NIFTYNXT50": 100,
              "SENSEX": 100, "BANKEX": 100, "SENSEX50": 100}     # SENSEX/BANKEX are BSE (Thursday weeklies)


def has_options(tk):
    t = (tk or "").upper().strip()
    return t in FNO_UNDERLYINGS or t in INDEX_STEP


_GCLIENT = None


def _g():
    """Memoized Groww client — constructing + token() on every option_quote (42×/chain) was the
    bottleneck. The client self-renews its token internally, so one instance is reused."""
    global _GCLIENT
    if _GCLIENT is not None:
        return _GCLIENT
    try:
        import groww_client
        g = groww_client.GrowwClient(); g.token()
        _GCLIENT = g
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
    e = mv.monthly_expiry(today.year, today.month)
    if today > e:                                   # this month's expiry passed -> next month
        y, m = (today.year + (today.month == 12)), (1 if today.month == 12 else today.month + 1)
        e = mv.monthly_expiry(y, m)
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


def option_quote(sym, exchange="NSE"):
    """Live parsed quote for one option symbol via Groww FNO (exchange NSE or BSE). None on failure."""
    g = _g()
    if not g:
        return {"error": "groww unavailable"}
    try:
        r = g.quote(sym, segment="FNO", exchange=exchange)
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


def chain(underlying, n=8, expiry=None):
    """ATM±n option chain. Built from BLACK-SCHOLES (real spot + 20d realized-vol IV → premiums + Greeks +
    breakevens) so it ALWAYS works; overlays live Groww option data per strike IF that feed responds (it
    currently returns GA001 for constructed F&O symbols, so the chain is model-only until that's fixed)."""
    und = (underlying or "").upper().strip()
    if not has_options(und):
        return {"error": f"{und} is not in the NSE F&O universe — no options to chain"}
    g = _g()
    spot = mv.underlying_ltp(und, g)
    if not spot:
        try:
            import yfinance as yf
            h = yf.Ticker(und + ".NS").history(period="1d")["Close"].dropna()
            spot = float(h.iloc[-1]) if len(h) else None
        except Exception:
            pass
    if not spot:
        return {"error": f"no spot price for {und}"}
    exp = expiry or nearest_monthly_expiry()
    if isinstance(exp, str):
        exp = dt.date.fromisoformat(exp)
    days = max((exp - dt.date.today()).days, 0)
    T = max(days, 1) / 365.0
    exp_iso = exp.isoformat()
    try:
        import marleg_instruments as _inst
    except Exception:
        _inst = None

    def _sym(k, kd):                                # exact contract from the master (weeklies/BSE), else built
        if _inst:
            c = _inst.contract(und, exp_iso, k, kd)
            if c and c.get("symbol"):
                return c["symbol"], (c.get("exchange") or "NSE")
        return build_symbol(und, k, kd, exp), "NSE"
    sigma = mv.realized_vol(und, 20); iv_src = "20d realized vol"
    if not sigma or sigma <= 0:
        vix = mv.india_vix(); sigma = (vix / 100.0 if vix else 0.30); iv_src = "India VIX proxy"
    step = INDEX_STEP.get(und) or _strike_step(spot)
    atm = round(spot / step) * step
    strikes = [atm + i * step for i in range(-n, n + 1)]
    # single live-probe: only attempt the Groww overlay if the ATM call actually quotes
    live_ok = False
    try:
        _ps, _pe = _sym(atm, "C"); pr = option_quote(_ps, exchange=_pe)
        live_ok = isinstance(pr, dict) and "error" not in pr and bool(pr.get("ltp"))
    except Exception:
        pass

    # if live, fetch all 2n+1 strikes × {C,P} IN PARALLEL (sequential was 42 HTTP calls -> >60s timeout).
    # Groww throttles a 42-call burst, so keep concurrency modest and do one retry pass for the strikes
    # that got rate-limited (otherwise ~1/3 of rows silently drop to the model).
    live_map = {}
    if live_ok:
        from concurrent.futures import ThreadPoolExecutor

        def _fetch(job):
            k, kd = job
            try:
                _s, _e = _sym(k, kd); return job, option_quote(_s, exchange=_e)
            except Exception:
                return job, None

        def _run(jobs, workers):
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for job, q in ex.map(_fetch, jobs):
                    if isinstance(q, dict) and "error" not in q and q.get("ltp"):
                        live_map[job] = q

        all_jobs = [(k, kd) for k in strikes for kd in ("C", "P")]
        _run(all_jobs, 6)
        missing = [j for j in all_jobs if j not in live_map]   # recover throttled strikes
        if missing:
            _run(missing, 4)

    def mk(k, kind):
        price = mv.bs_price(spot, k, T, mv.R_FREE, sigma, kind)
        gk = mv.greeks(spot, k, T, mv.R_FREE, sigma, kind)
        be = (k + price) if kind == "C" else (k - price)
        row = {"ltp": round(price, 2), "iv": round(sigma * 100, 1), "delta": round(gk["delta"], 3),
               "gamma": round(gk["gamma"], 5), "theta": round(gk["theta"], 2), "vega": round(gk["vega"], 3),
               "be": round(be, 1), "be_pct": round((be / spot - 1) * 100, 1), "oi": None, "src": "model"}
        lq = live_map.get((k, kind))
        if lq:
            iv = lq.get("iv") or mv.implied_vol(lq["ltp"], spot, k, T, mv.R_FREE, kind)
            row.update({"ltp": lq["ltp"], "iv": round((iv or sigma) * 100, 1), "oi": lq.get("oi"),
                        "oi_change": lq.get("oi_change"), "volume": lq.get("volume"),
                        "bid": lq.get("bid"), "ask": lq.get("ask"), "spread_pct": lq.get("spread_pct"),
                        "src": "live"})
        return row

    rows = [{"strike": k, "is_atm": abs(k - atm) < step / 2, "ce": mk(k, "C"), "pe": mk(k, "P")} for k in strikes]
    ce_oi = sum((r["ce"].get("oi") or 0) for r in rows); pe_oi = sum((r["pe"].get("oi") or 0) for r in rows)
    pcr = round(pe_oi / ce_oi, 2) if ce_oi else None        # local PCR across the shown strikes (live OI only)
    vix = mv.india_vix()
    today = dt.date.today()
    exps = (_inst.expiries(und, within_days=160) if _inst else None) or []   # ALL real expiries (weeklies+monthlies)
    if not exps:                                                              # fallback: monthlies only
        yy, mm = today.year, today.month
        while len(exps) < 4:
            lt = mv.monthly_expiry(yy, mm)
            if lt >= today:
                exps.append(lt.isoformat())
            yy, mm = (yy + (mm == 12)), (1 if mm == 12 else mm + 1)
    return {"underlying": und, "spot": round(spot, 2), "atm": atm, "expiry": exp.isoformat(), "expiries": exps,
            "days_to_expiry": days, "step": step, "iv_used": round(sigma * 100, 1), "iv_source": iv_src,
            "pcr": pcr, "pcr_basis": f"ATM±{n} strikes (local)",
            "india_vix": vix, "source": ("live" if live_ok else "model"),
            "note": ("BLACK-SCHOLES modelled — Groww didn't quote this name/expiry (likely off-hours or thin "
                     "strikes). Premiums, Greeks (Δ/Θ/Vega) and breakevens are theoretical, from spot + 20d "
                     "realized vol. OI / PCR are omitted (no fabricated numbers). Verify premiums on your broker."
                     if not live_ok else "LIVE Groww option data (premiums · IV · OI · volume) where it quotes; "
                     "Black-Scholes fallback per strike that doesn't."),
            "rows": rows}


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    arg = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    print(json.dumps(chain(arg) if has_options(arg) else analyze(arg), indent=2, default=str))
