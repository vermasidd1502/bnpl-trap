"""
Marle-G surveillance backend — real Indian-market data.

Serves the existing static pages AND live JSON endpoints computed from real
data (yfinance for equities/ETF/indices/FX, mfapi.in for mutual funds).
TTL-cached so we poll upstream politely (surveillance, not hammering).

Run:  python marleg_server.py        ->  http://127.0.0.1:8777/
Endpoints:
  /api/quote?symbols=RELIANCE,HDFCBANK     batch LTP + change%
  /api/equity/<ticker>                     full volume-primary analysis
  /api/macro                               NIFTY / India VIX / USDINR / sectors / regime
  /api/mf/<scheme_code>                    NAV history + returns
  /api/options/<underlying>                NSE option chain (best-effort)
  /api/health
"""
import os, re, time, threading, math, json
from flask import Flask, jsonify, request, send_from_directory
import pandas as pd, numpy as np
import yfinance as yf
import requests

app = Flask(__name__, static_folder=None)
HERE = __file__.rsplit("\\", 1)[0] if "\\" in __file__ else "."

# ----------------------------------------------------------------- Groww broker (real-time + account)
import groww_client
import marleg_vwap as mvwap   # VWAP analytics (rolling + anchored + sigma bands)
import marleg_cascade         # event -> 5-tier industry cascade ("the painting")
import marleg_vol             # options vol layer: IV (BS) vs realized + Greeks
import marleg_option_structures as mstruct   # straddle/butterfly/condor compare + backtest
import marleg_projection        # accounting trends + multi-horizon price cone
import marleg_fundamentals       # robust fundamentals (statement-computed ratios + coverage)
import marleg_winners            # live winners/losers board (book marked to live price)
import marleg_overextension      # don't-chase guard (room vs ceiling, 52w-high golden number)
import marleg_weekend            # weekend-carry edge (Friday-momentum -> Monday) + live scan
import marleg_var                # portfolio VaR + Monte-Carlo + CAPM beta (FIN-537 risk engine)
import marleg_options_monitor as mom   # live options monitor: depth/OI/IV/Greeks + constructed chain
import marleg_mf                        # mutual-fund universe: search + category/sector classification
import marleg_buyhold                    # Buy & Hold pod: compounder score (quality+valuation+durability) + screen
import marleg_patterns                    # technical pattern detection (gated on India reliability backtest)
import marleg_regime            # dispersion/correlation regime dial (scenario-alpha gate)
import marleg_thesis            # structural grey-swan scenario book (Thesis Ledger)
import marleg_smartmoney         # institutional-flow / smart-money (shareholding deltas)
import marleg_business           # firm-level: moat / Porter 5-forces / SWOT / sector-override
import marleg_mindhive           # the local scenario brain: synthesis + KG + deterministic chat
import marleg_intraday           # FIN537 realized-vol (RV+HAR) + Bollinger/Keltner squeeze
import marleg_nifty_sim          # Nifty options strategy tournament (BS-priced multi-year backtest)
import marleg_volume_book        # daily volume intraday "thrill" book (intraday vs overnight legs)
import marleg_signal_quality     # winners vs misleading spikes + shorts (bad ud / 0.618) + dated targets + Pareto
import marleg_robust_bt          # cost / DSR / PSR / bootstrap robustness battery (honest scorecard)
import marleg_strategies         # trading-strategies pod: playbooks + honest backtests + try-on-a-stock paper trade
_GROWW, _GROWW_ERR = None, None
def groww():
    """Lazy singleton. Returns a GrowwClient, or None if creds are unavailable."""
    global _GROWW, _GROWW_ERR
    if _GROWW is None and _GROWW_ERR is None:
        try:
            _GROWW = groww_client.GrowwClient()
        except Exception as e:
            _GROWW_ERR = str(e)
    return _GROWW
# Live order placement is OFF unless explicitly enabled (real money safety).
ALLOW_LIVE_ORDERS = os.environ.get("MARLEG_ALLOW_LIVE_ORDERS") == "1"

# ----------------------------------------------------------------- cache
_CACHE, _LOCK = {}, threading.Lock()
def cached(key, fn, ttl):
    now = time.time()
    with _LOCK:
        hit = _CACHE.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = fn()
    with _LOCK:
        _CACHE[key] = (now, val)
    return val

NAMES = {
 "RELIANCE":"Reliance Industries","HDFCBANK":"HDFC Bank","TCS":"Tata Consultancy","INFY":"Infosys",
 "ICICIBANK":"ICICI Bank","SBIN":"State Bank of India","TMPV":"Tata Motors PV","TMCV":"Tata Motors CV","ITC":"ITC",
 "BHARTIARTL":"Bharti Airtel","MARUTI":"Maruti Suzuki","HINDUNILVR":"Hindustan Unilever","LT":"Larsen & Toubro",
 "AXISBANK":"Axis Bank","KOTAKBANK":"Kotak Mahindra Bank","SUNPHARMA":"Sun Pharma","WIPRO":"Wipro",
 "HCLTECH":"HCL Tech","BAJFINANCE":"Bajaj Finance","TATASTEEL":"Tata Steel","NTPC":"NTPC",
}
def yftk(sym):
    sym = sym.upper().strip()
    if sym.startswith("^") or "=" in sym or sym.endswith(".NS") or sym.endswith(".BO"):
        return sym
    return sym + ".NS"

# ----------------------------------------------------------------- helpers
def _hist(sym, period="1y", interval="1d"):
    df = yf.Ticker(yftk(sym)).history(period=period, interval=interval, auto_adjust=False)
    return df if df is not None and len(df) else None

def _rsi(close, n=14):
    d = close.diff(); up = d.clip(lower=0).rolling(n).mean(); dn = (-d.clip(upper=0)).rolling(n).mean()
    rs = up / dn.replace(0, np.nan); return float((100 - 100/(1+rs)).iloc[-1])

# ----------------------------------------------------------------- equity engine (REAL volume)
def equity_analysis(ticker):
    tk = ticker.upper().strip()
    df = _hist(tk, "1y")
    if df is None or len(df) < 60:
        return {"error": f"no data for {tk}", "tk": tk}
    df = df.dropna(subset=[c for c in ["Close", "High", "Low", "Volume"] if c in df.columns])
    if len(df) < 60:
        return {"error": f"insufficient clean data for {tk}", "tk": tk}
    close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]
    ltp = float(close.iloc[-1]); prev = float(close.iloc[-2]); chg = round((ltp/prev-1)*100, 2)
    direction = np.sign(close.diff()).fillna(0)
    # up/down volume (20d)
    up20 = float((vol[-20:][direction[-20:] > 0]).sum()); dn20 = float((vol[-20:][direction[-20:] < 0]).sum())
    ud = round(up20/dn20, 2) if dn20 > 0 else 3.0
    # OBV + slope (normalised)
    obv = (vol * direction).cumsum()
    obv_chg = float(obv.iloc[-1] - obv.iloc[-21]) if len(obv) > 21 else 0.0
    obv_norm = obv_chg / (float(vol[-20:].mean()) * 20 + 1e-9)
    obvSlope = round(max(-1.5, min(1.5, obv_norm)), 2)
    # A/D line (Chaikin) slope
    clv = ((close - low) - (high - close)) / (high - low).replace(0, np.nan)
    adl = (clv.fillna(0) * vol).cumsum()
    adl_norm = float(adl.iloc[-1] - adl.iloc[-21]) / (float(vol[-20:].mean()) * 20 + 1e-9) if len(adl) > 21 else 0.0
    adSlope = round(max(-1.5, min(1.5, adl_norm)), 2)
    # CMF (21)
    mfv = clv.fillna(0) * vol
    cmf = round(float(mfv[-21:].sum() / (vol[-21:].sum() + 1e-9)), 2)
    # MFI (14)
    tp = (high + low + close) / 3; rmf = tp * vol
    pos = float(rmf[-14:][tp.diff()[-14:] > 0].sum()); neg = float(rmf[-14:][tp.diff()[-14:] < 0].sum())
    mfi = round(100 - 100/(1 + pos/(neg+1e-9)))
    # RVOL
    rvol = round(float(vol.iloc[-1] / vol[-20:].mean()), 2)
    # contributions
    C = []
    C.append([f"U/D ratio {ud}x", 2 if ud >= 1.5 else 1 if ud >= 1.2 else -2 if ud <= 0.7 else -1 if ud <= 0.9 else 0])
    C.append(["OBV slope", 2 if obvSlope > 0.2 else 1 if obvSlope > 0 else -2 if obvSlope < -0.2 else -1])
    C.append(["A/D line (Chaikin)", 1 if adSlope > 0.1 else -1 if adSlope < -0.1 else 0])
    C.append([f"CMF {cmf}", 1 if cmf > 0.1 else -1 if cmf < -0.1 else 0])
    C.append([f"MFI {mfi}", -1 if mfi > 80 else 1 if mfi < 20 else 1 if mfi > 55 else -1 if mfi < 45 else 0])
    C.append([f"RVOL {rvol}x (conviction)", (1 if obvSlope > 0 else -1) if rvol > 1.5 else 0])
    # effort vs result divergence
    if obvSlope > 0.2 and chg < 0: C.append(["Effort/result: OBV rising while price falls — bullish accumulation divergence", 2])
    elif obvSlope < -0.2 and chg > 0: C.append(["Effort/result: OBV falling while price rises — bearish distribution divergence", -2])
    vcs = sum(c[1] for c in C)
    verdict = "LONG" if vcs >= 3 else "SHORT" if vcs <= -2 else "FLAT"
    conv = min(95, 40 + abs(vcs) * 9)
    # VSA
    vsa = []
    if rvol > 2 and abs(chg) > 2.5: vsa.append("CLIMAX - " + ("buying" if chg > 0 else "selling") + " exhaustion")
    if rvol > 1.6 and abs(chg) < 0.6: vsa.append("ABSORPTION - effort>>result")
    if rvol < 0.8 and chg > 0: vsa.append("NO-DEMAND - weak up-bar")
    if rvol < 0.8 and chg < 0: vsa.append("NO-SUPPLY - weak down-bar")
    top = sorted(C, key=lambda c: -abs(c[1]))[:3]
    driver = f"{verdict} - {conv} - driven by " + " - ".join(c[0].lower() for c in top) + "."
    # price TA confirmation
    sma50 = float(close.rolling(50).mean().iloc[-1]); sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else sma50
    ema12 = close.ewm(span=12).mean(); ema26 = close.ewm(span=26).mean(); macd = float((ema12-ema26).iloc[-1])
    bbm = close.rolling(20).mean(); bbs = close.rolling(20).std(); pctb = float((ltp - (bbm-2*bbs).iloc[-1])/(((bbm+2*bbs)-(bbm-2*bbs)).iloc[-1]+1e-9)*100)
    if math.isnan(pctb): pctb = 50.0
    vwap = float((tp*vol)[-20:].sum()/vol[-20:].sum()); rsi = round(_rsi(close), 1)
    hi52 = float(high[-252:].max()); off52 = round((ltp/hi52-1)*100, 1)
    # entry timing from RSI — backtested: low-RSI accumulation = best risk-adjusted entry;
    # RSI 45-60 is the dead zone (lost in/out of sample); high RSI still works as momentum.
    if verdict == "LONG" or vcs > 0:
        entry = ("EARLY · oversold accumulation" if rsi < 45 else "DEAD-ZONE · RSI 45-60" if rsi < 60
                 else "EXTENDED · RSI 60-70" if rsi < 70 else "CHASING · overbought")
        if rsi < 45: conv = min(95, conv + 10)
        elif rsi < 60: conv = max(20, conv - 12)
        driver = f"{verdict} - {conv} - driven by " + " - ".join(c[0].lower() for c in top) + "."
    elif verdict == "SHORT" or vcs < 0:
        entry = ("OVERSOLD · bounce risk" if rsi < 30 else "distribution" if rsi < 70 else "still overbought")
    else:
        entry = "neutral"
    dir_ = 1 if verdict == "LONG" else -1 if verdict == "SHORT" else 0
    def conf(b): return "NEUTRAL" if dir_ == 0 else ("CONFIRMS" if b == dir_ else "CONFLICTS" if b != 0 else "NEUTRAL")
    # Ichimoku Kinko Hyo — cloud position + Tenkan/Kijun cross
    def _mid(n): return (high.rolling(n).max() + low.rolling(n).min()) / 2
    tenkan = float(_mid(9).iloc[-1]); kijun = float(_mid(26).iloc[-1])
    spanA_s = (_mid(9) + _mid(26)) / 2; spanB_s = _mid(52)
    spanA_now = float(spanA_s.iloc[-26]) if len(spanA_s) > 26 else float(spanA_s.iloc[-1])
    spanB_now = float(spanB_s.iloc[-26]) if len(spanB_s) > 26 else float(spanB_s.iloc[-1])
    if math.isnan(spanA_now) or math.isnan(spanB_now):
        ichi_dir, cloud_txt = 0, "n/a"
    else:
        ctop, cbot = max(spanA_now, spanB_now), min(spanA_now, spanB_now)
        ichi_dir, cloud_txt = (1, "above cloud") if ltp > ctop else (-1, "below cloud") if ltp < cbot else (0, "in cloud")
    tk_sig = 1 if tenkan > kijun else -1 if tenkan < kijun else 0
    ichimoku = {"tenkan": round(tenkan, 2), "kijun": round(kijun, 2),
                "spanA": round(spanA_now, 2), "spanB": round(spanB_now, 2), "cloud": cloud_txt,
                "tk": "Tenkan>Kijun" if tk_sig > 0 else "Tenkan<Kijun" if tk_sig < 0 else "Tenkan=Kijun",
                "dir": ichi_dir}
    ta = [
        [f"RSI {rsi}", conf(1 if rsi > 55 else -1 if rsi < 45 else 0)],
        ["MACD", conf(1 if macd > 0 else -1)],
        ["50/200 SMA", conf(1 if sma50 >= sma200 else -1)],
        [f"Boll %B {round(pctb)}", conf(1 if pctb > 50 else -1)],
        ["VWAP", conf(1 if ltp > vwap else -1)],
        [f"52w {off52}%", conf(1 if off52 > -10 else -1)],
        [f"Ichimoku · {cloud_txt}", conf(ichi_dir)],
        [f"TK cross {'bullish' if tk_sig > 0 else 'bearish' if tk_sig < 0 else 'flat'}", conf(tk_sig)],
    ]
    # hedge fund archetypes (from real signals)
    arch = [
        ["Momentum","trend follower", "LONG" if chg>0 and obvSlope>0 else "SHORT" if chg<0 and obvSlope<0 else "PASS",
         f"Price {('up' if chg>0 else 'down')} + OBV {('rising' if obvSlope>0 else 'falling')}."],
        ["Value","mean reversion", "LONG" if mfi<35 else "SHORT" if mfi>75 else "PASS",
         ("Oversold money-flow." if mfi<35 else "Overbought." if mfi>75 else "No valuation edge.")],
        ["Quant","volume factor", verdict, f"Volume-conviction score {vcs}."],
        ["Macro","top-down", "LONG" if ltp>sma200 else "PASS", ("Above 200-DMA." if ltp>sma200 else "Below 200-DMA.")],
        ["Activist","catalyst", "LONG" if ud>1.5 else "PASS", ("Accumulation footprint." if ud>1.5 else "No footprint.")],
        ["Contrarian","fade the crowd", "SHORT" if rvol>2 and chg>0 else "LONG" if rvol<0.8 and chg<0 else "PASS",
         ("Climactic volume - fade." if rvol>2 else "Crowd not stretched.")],
    ]
    longs = sum(1 for a in arch if a[2] == "LONG"); shorts = sum(1 for a in arch if a[2] == "SHORT")
    consensus = f"CONSENSUS: LONG ({longs}/6)" if longs>shorts else f"CONSENSUS: SHORT ({shorts}/6)" if shorts>longs else "SPLIT - no consensus"
    # chart series (last 120)
    cs = df.iloc[-120:]
    series = [{"d": str(d.date()), "c": round(float(c), 2), "v": float(v),
               "up": bool(cs["Close"].iloc[i] > cs["Close"].iloc[i-1]) if i>0 else True}
              for i, (d, c, v) in enumerate(zip(cs.index, cs["Close"], cs["Volume"]))]
    # --- target & stop from ATR (2:1 R:R), tied to the verdict direction ---
    _pc = close.shift(1)
    _tr = pd.concat([(high - low), (high - _pc).abs(), (low - _pc).abs()], axis=1).max(axis=1)
    atr14 = float(_tr.rolling(14).mean().iloc[-1])
    ret_sigma = float(close.pct_change().iloc[-60:].std() * 100)
    if verdict == "LONG":
        target = round(ltp + 2 * atr14, 1); stopL = round(ltp - atr14, 1)
    elif verdict == "SHORT":
        target = round(ltp - 2 * atr14, 1); stopL = round(ltp + atr14, 1)
    else:
        target = stopL = None
    tgtpct = round((target / ltp - 1) * 100, 1) if target else None
    return {"tk": tk, "name": NAMES.get(tk, tk), "ltp": round(ltp, 2), "chg": chg,
            "target": target, "stopL": stopL, "tgtpct": tgtpct, "rr": (2.0 if target else None),
            "atr": round(atr14, 2), "ret_sigma": round(ret_sigma, 2),
            "ud": ud, "obvSlope": obvSlope, "adSlope": adSlope, "cmf": cmf, "mfi": mfi, "rvol": rvol,
            "deliv": None, "entry": entry, "rsi": rsi, "C": C, "vcs": vcs, "verdict": verdict, "conv": conv, "vsa": vsa, "driver": driver,
            "ta": ta, "ichimoku": ichimoku, "arch": arch, "consensus": consensus, "longs": longs, "shorts": shorts, "series": series,
            "asof": str(close.index[-1].date())}

# ----------------------------------------------------------------- routes
@app.route("/api/health")
def health(): return jsonify({"ok": True, "ts": time.time()})

# ----------------------------------------------------------------- symbol master (autocomplete)
_SYMBOLS = None
def _load_symbols():
    global _SYMBOLS
    if _SYMBOLS is None:
        path = os.path.join(HERE, "marleg_symbols.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                _SYMBOLS = json.load(f)
        except Exception:
            try:  # build from Groww instruments master, cache to disk
                import csv, io
                txt = requests.get("https://growwapi-assets.groww.in/instruments/instrument.csv", timeout=90).text
                rows = [{"s": x["trading_symbol"], "n": x["name"] or x["trading_symbol"]}
                        for x in csv.DictReader(io.StringIO(txt))
                        if x["exchange"] == "NSE" and x["instrument_type"] == "EQ" and x["segment"] == "CASH"]
                rows.sort(key=lambda d: d["s"])
                _SYMBOLS = rows
                try:
                    with open(path, "w", encoding="utf-8") as f:
                        json.dump(rows, f, ensure_ascii=False)
                except Exception:
                    pass
            except Exception:
                _SYMBOLS = []
    return _SYMBOLS

@app.route("/api/symbols")
def api_symbols():
    q = request.args.get("q", "").strip().upper()
    lim = min(int(request.args.get("limit", 20) or 20), 50)
    syms = _load_symbols()
    if not q:
        return jsonify(syms[:lim])
    pre = [r for r in syms if r["s"].startswith(q)]
    sub = [r for r in syms if (q in r["s"] or q in r["n"].upper()) and not r["s"].startswith(q)]
    return jsonify((pre + sub)[:lim])

def _volume_pod_live():
    """Cache (nightly scan) + LIVE price overlay. The cached `price` is yesterday's
    close (midnight scan), so live chg = live/cached - 1. Refreshed every 5 min."""
    with open(os.path.join(HERE, "marleg_volume_cache.json"), encoding="utf-8") as f:
        data = json.load(f)
    try:
        import yfinance as yf
        import pandas as _pd
        # cap live refresh to the top-conviction names (a 3000-name universe can't refresh
        # every 5 min) — the rest keep their scan-time price.
        allst = [x for sec in data.get("sectors", []) for x in sec.get("stocks", [])]
        allst.sort(key=lambda x: -(x.get("ud") or 0))
        syms = list(dict.fromkeys(x["s"] for x in allst[:250]))
        d = yf.download([s + ".NS" for s in syms], period="1d", interval="5m",
                        group_by="ticker", progress=False, threads=True)
        n = 0
        for sec in data.get("sectors", []):
            for x in sec.get("stocks", []):
                try:
                    c = d[x["s"] + ".NS"]["Close"].dropna()
                    if not len(c):
                        continue
                    live = float(c.iloc[-1])
                    prev = x.get("price")
                    if prev:
                        x["chg"] = round((live / prev - 1) * 100, 2)
                    x["price"] = round(live, 1)
                    n += 1
                except Exception:
                    pass
        from datetime import datetime, timezone, timedelta
        ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        data["live_overlay"] = {"updated": ist.strftime("%H:%M IST"), "names": n}
        data["asof"] = (data.get("asof") or "") + "  ·  live px " + ist.strftime("%H:%M")
    except Exception:
        pass                                         # overlay is best-effort; cache still serves
    return data


@app.route("/api/volume_pod")
def api_volume_pod():
    try:
        return jsonify(cached("volume_pod_live", _volume_pod_live, 300))
    except Exception:
        return jsonify({"error": "no volume cache yet — run: python marleg_volume_scan.py", "sectors": []})

def _gated_live(fname="marleg_gated_cache.json", record_tenure=True):
    """Gated-longs scan cache + LIVE price overlay (target% recomputed vs live).
    fname picks the daily (structural) vs hourly (intraday) cache; tenure is daily-only."""
    with open(os.path.join(HERE, fname), encoding="utf-8") as f:
        d = json.load(f)
    try:
        import yfinance as yf
        syms = [x["s"] for x in d.get("picks", [])]
        if syms:
            dd = yf.download([s + ".NS" for s in syms], period="1d", interval="5m",
                             group_by="ticker", progress=False, threads=True)
            for x in d.get("picks", []):
                try:
                    c = dd[x["s"] + ".NS"]["Close"].dropna()
                    if not len(c):
                        continue
                    live = float(c.iloc[-1])
                    x["price"] = round(live, 2)
                    if x.get("target"):
                        x["tgtpct"] = round((x["target"] / live - 1) * 100, 1)
                except Exception:
                    pass
            from datetime import datetime, timezone, timedelta
            ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
            d["asof"] = (d.get("asof") or "") + "  ·  live px " + ist.strftime("%H:%M")
    except Exception:
        pass
    # daily-persist the gated list + stamp each pick with its tenure (streak / on-since) — daily list only
    if record_tenure:
        try:
            import marleg_gated_history as gh
            gh.annotate(d.get("picks", []), sym_key="s")
        except Exception:
            pass
    return d


@app.route("/api/gated")
def api_gated():
    mode = (request.args.get("mode") or "daily").lower()
    fname = "marleg_gated_hourly.json" if mode == "hourly" else "marleg_gated_cache.json"
    key = "gated_hourly" if mode == "hourly" else "gated_live"
    try:
        return jsonify(cached(key, lambda: _gated_live(fname, record_tenure=(mode != "hourly")), 300))
    except Exception:
        pass
    try:
        with open(os.path.join(HERE, fname), encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify({"error": f"no {mode} gated screen yet — run: python marleg_gated_scan.py", "picks": []})

@app.route("/api/industry_rs")
def api_industry_rs():
    """Granular industry relative-strength / breadth — leading->lagging rotation (heatmap).
    Served from the cache the gated scan writes; cold-fallback pulls only the taxonomy universe."""
    import marleg_industry_rs as mir
    def load():
        d = mir.load_cache()
        if d and d.get("groups"):
            return d
        return mir.run("6mo")
    try:
        return jsonify(cached("industry_rs", load, 900))
    except Exception as e:
        return jsonify({"error": str(e), "groups": []})


@app.route("/api/warroom")
def api_warroom():
    """The WAR ROOM payload — regime + leading sectors + strict news-clean watchlist with per-name
    setup / holding-period / entry-exit plan + risk-per-share. Pure cache assembly (fast)."""
    import marleg_warroom
    return jsonify(cached("warroom", lambda: marleg_warroom.build(), 60))


@app.route("/api/warroom/config", methods=["GET", "POST"])
def api_warroom_config():
    """User's own session settings (capital / risk-% / daily goal / loss-limit / pinned sectors).
    Local preference state — never an account or order action."""
    import marleg_warroom
    if request.method == "POST":
        cfg = marleg_warroom.set_config(request.get_json(force=True, silent=True) or {})
        with _LOCK:
            _CACHE.pop("warroom", None)        # bust so the watchlist re-filters to the new pinned sectors
        return jsonify(cfg)
    return jsonify(marleg_warroom.config())


def _read_json(fname, err="not built yet"):
    try:
        with open(os.path.join(HERE, fname), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"error": err}


@app.route("/api/movers")
def api_movers():
    """High-mover / squeeze radar — move-potential (the 3-8%/day amplitude filter) + abnormal-up + F&O short-covering."""
    import marleg_movers
    return jsonify(cached("movers", lambda: marleg_movers.build(), 120))


@app.route("/api/vix")
def api_vix():
    """VIX conscience (sector behavior in calm vs volatile) + driver attribution (why VIX is moving)."""
    return jsonify(cached("vix_study", lambda: _read_json("marleg_vix_study.json", "VIX study not built — run marleg_vix_study.py"), 300))


@app.route("/api/tiers")
def api_tiers():
    """5-tier industry volatility ladder + the tier1->tier2 lead-lag result."""
    return jsonify(cached("tier_study", lambda: _read_json("marleg_tier_study.json", "tier study not built — run marleg_tier_study.py"), 600))


@app.route("/api/lastcandle")
def api_lastcandle():
    """Final-10-min volume-spike x fib x Ichimoku x U/D -> next-day win ratio (indicative; shallow intraday)."""
    return jsonify(cached("lastcandle", lambda: _read_json("marleg_lastcandle_study.json", "last-candle study not built — run marleg_lastcandle_study.py"), 600))


@app.route("/api/dossier/<ticker>")
def api_dossier(ticker):
    """Per-stock dossier: conviction + squeeze-ceiling + move-potential + Monte Carlo cone + levels +
    suggested trade + read-only same-sector overlap. Drives the click-to-expand chart on the Movers pod."""
    import marleg_dossier
    return jsonify(cached("dossier:" + ticker.upper(), lambda: marleg_dossier.dossier(ticker), 240))


@app.route("/api/alerts")
def api_alerts():
    """Strategy alerts: the CAN SLIM staged-gate funnel (fundamentals->fair-value->volume->entry) + a timed
    earnings/catalyst monitor + the gate ladder. Read-only assembly of the gated/fundamentals/movers caches."""
    import marleg_alerts
    return jsonify(cached("alerts", lambda: marleg_alerts.build(), 180))


@app.route("/api/cuphandle")
def api_cuphandle():
    """Live cup-with-handle radar with the STAGE (forming/handle/breakout/confirmed/failed) + the backtested
    win/net per stage. The strategy: buy the CONFIRMED breakout, never the raw bar."""
    import marleg_cuphandle
    return jsonify(cached("cuphandle", lambda: marleg_cuphandle.build(), 180))


@app.route("/api/industry_persistence")
def api_industry_persistence():
    """Per-industry BETA + vol + this-week RS + how LONG leadership typically lasts (the rotation hold horizon)."""
    return jsonify(cached("ind_persist", lambda: _read_json("marleg_industry_persistence.json", "industry persistence not built — run marleg_industry_persistence.py"), 600))


@app.route("/api/lookahead")
def api_lookahead():
    """Pre-open look-ahead: live global cues (US close / US VIX / Asia / Brent / USDINR) + a composite gap
    read + the prior Nifty close (so the page turns a typed GIFT Nifty level into an implied gap)."""
    import marleg_lookahead
    return jsonify(cached("lookahead", lambda: marleg_lookahead.build(), 300))


@app.route("/api/reversal")
def api_reversal():
    """Reversal-to-long radar: validated bullish reversal signals (Hammer/Morning Star/Piercing/Engulfing +
    pullback-turn) ranked by conviction — PRIME = reversal in an uptrend in a leader (the re-entry trigger)."""
    import marleg_reversal
    return jsonify(cached("reversal", lambda: marleg_reversal.build(), 180))


@app.route("/api/intraday_trigger/<tk>")
def api_intraday_trigger(tk):
    """Live intraday trigger scoreboard for one name: VWAP / opening-range pivot / support + the staged
    'bulls take over' triggers (VWAP reclaim -> breakout) and the disciplined entry/stop/target. Read-only."""
    import marleg_intraday_trigger as itr
    sup = request.args.get("support", type=float)
    piv = request.args.get("pivot", type=float)
    return jsonify(cached(f"itrig:{tk.upper()}:{sup}:{piv}", lambda: itr.pulse(tk.upper(), support=sup, pivot=piv), 45))


@app.route("/api/bearish")
def api_bearish():
    """Honest bearish/defensive pod: avoid/exit relative laggards, market-neutral pairs, regime-gated index
    hedge. Outright shorting backtested negative in India (all 6 categories) — this only does what pays."""
    import marleg_bearish
    return jsonify(cached("bearish", lambda: marleg_bearish.build(), 600))


@app.route("/api/horizon/<tk>")
def api_horizon(tk):
    """Holding-horizon rating for one stock: LONG-TERM / SWING / SHORT-TERM / AVOID + ideal hold & payout,
    fusing volume (U/D, gated) + rotation (industry leading/beta/persistence) + the validated edges."""
    import marleg_horizon
    return jsonify(cached(f"horizon:{tk.upper()}", lambda: marleg_horizon.rate(tk.upper()), 300))


@app.route("/api/live/<tk>")
def api_live_price(tk):
    """Real-time last price (Groww LTP → yfinance fallback) for tight milestone re-checks — so the UI can
    flag TARGET HIT / STOP HIT / entry-passed live, independent of the EOD-built analysis."""
    import marleg_live
    tk = tk.upper()
    p = cached(f"live:{tk}", lambda: marleg_live.price(tk), 5)   # 5s cache: fresh but Groww-friendly
    if p.get("ok"):
        tgt = request.args.get("target", type=float)
        stp = request.args.get("stop", type=float)
        ent = request.args.get("entry", type=float)
        if tgt or stp or ent:
            p = {**p, "milestone": marleg_live.milestone(p["price"], ent, tgt, stp)}
    return jsonify(p)


@app.route("/api/conscience")
def api_conscience():
    """NIFTY bull/bear conscience (trend + breadth + VIX) + next-day lean from the US close
    (backtest-weighted; Japan doesn't predict). The 'where are we + what's tomorrow' gauge."""
    import marleg_conscience
    return jsonify(cached("conscience", lambda: marleg_conscience.build(), 300))


@app.route("/api/autopilot")
def api_autopilot():
    """Conviction consolidator + autonomous paper-trader: open paper positions (live P&L + conviction +
    thesis), the fresh high-conviction queue, and the track record by engine. Paper only — never an order."""
    import marleg_autopilot
    return jsonify(cached("autopilot", lambda: marleg_autopilot.view(), 120))


@app.route("/api/volume_state")
def api_volume_state():
    """Live state board for the gated list: GOLD (target hit) / GREEN (climbing, disciplined U/D) / RED
    (false move / blow-off), split into MAIN (promoted) vs WATCHLIST (provisional, gate-2). Read-only."""
    import marleg_volume_state
    return jsonify(cached("volume_state", lambda: marleg_volume_state.build(), 45))


@app.route("/api/allsides/<tk>")
def api_allsides(tk):
    """Cross-engine all-sides read for one stock: every engine's verdict + the tensions (conflicts) + a
    net label. The homogeneous analyzer behind the Stock Lab and the dossier."""
    import marleg_analyze
    return jsonify(cached(f"allsides:{tk.upper()}", lambda: marleg_analyze.analyze(tk.upper()), 60))


@app.route("/api/simulator")
def api_simulator():
    """Your manual paper tracker — holdings tracked live + re-analyzed across every engine."""
    import marleg_simulator
    return jsonify(cached("simulator", lambda: marleg_simulator.view(), 30))


@app.route("/api/simulator/add", methods=["POST"])
def api_simulator_add():
    import marleg_simulator
    tk = ((request.json or {}).get("tk") or "").strip().upper()
    r = marleg_simulator.add(tk) if tk else {"ok": False, "msg": "no ticker"}
    _CACHE.pop("simulator", None)
    return jsonify(r)


@app.route("/api/simulator/remove", methods=["POST"])
def api_simulator_remove():
    import marleg_simulator
    tk = ((request.json or {}).get("tk") or "").strip().upper()
    r = marleg_simulator.remove(tk) if tk else {"ok": False, "msg": "no ticker"}
    _CACHE.pop("simulator", None)
    return jsonify(r)


@app.route("/api/regime_gate")
def api_regime_gate():
    """Tiny market bull/bear read (the durable macro gate) for the nav badge on every pod.
    Distinct from /api/regime (the dispersion Regime Dial) — this is the NIFTY>50DMA deploy/cash gate."""
    def load():
        try:
            d = json.load(open(os.path.join(HERE, "marleg_gated_cache.json"), encoding="utf-8"))
            r = d.get("regime") or {}
            return {"bull": r.get("bull"), "breadth": r.get("breadth"),
                    "verdict": r.get("verdict"), "asof": d.get("asof")}
        except Exception:
            return {"bull": None, "verdict": "no scan yet"}
    return jsonify(cached("regime_badge", load, 300))


@app.route("/api/shock")
def api_shock():
    """Fast macro-shock / regime-break read (VIX + NIFTY + breadth + correlation) — the trade-gate
    overlay. NORMAL -> defer to bull/bear; ELEVATED -> cut size; SHOCK -> refrain, long-term hold only."""
    import marleg_shock
    return jsonify(cached("shock", marleg_shock.read, 180))


@app.route("/api/industry_members")
def api_industry_members():
    """Static taxonomy-backed member lists (symbol+name) per industry and per macro sector —
    always complete + throttle-proof, the drill-down's fallback when the live RS cache is thin."""
    def load():
        tax = json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json"), encoding="utf-8"))
        by_ind, by_macro = {}, {}
        for s, m in tax.get("by_symbol", {}).items():
            ind = m.get("industry") or "Others"
            macro = m.get("macro") or "Others"
            nm = m.get("name") or s
            by_ind.setdefault(ind, []).append({"s": s, "n": nm})
            by_macro.setdefault(macro, []).append({"s": s, "n": nm, "industry": ind})
        for d in (by_ind, by_macro):
            for k in d:
                d[k].sort(key=lambda x: x["s"])
        return {"by_industry": by_ind, "by_macro": by_macro,
                "industries": sorted(by_ind), "macros": sorted(by_macro)}
    return jsonify(cached("industry_members", load, 3600))


@app.route("/api/depth/<tk>")
def api_depth(tk):
    """Live equity order book (5-level ladder + buy/sell order % + walls), Groww read-only."""
    import marleg_depth
    return jsonify(cached("depth:" + tk.upper(), lambda: marleg_depth.read(tk), 8))   # short TTL — live book


@app.route("/api/closing/<tk>")
def api_closing(tk):
    """Session-phase clock + backtested closing-behaviour read for one stock."""
    import marleg_closing
    return jsonify(cached("closing:" + tk.upper(), lambda: marleg_closing.read(tk), 60))


@app.route("/api/volume_pod/add", methods=["POST"])
def api_volume_pod_add():
    """Add or re-classify a stock in the Volume Pod. Creates new sectors/industries freely."""
    import marleg_volume_scan as mvs
    p = request.get_json(force=True, silent=True) or {}
    sym = (p.get("symbol") or "").strip().upper()
    sector = (p.get("sector") or "Others").strip() or "Others"
    industry = (p.get("industry") or sector).strip() or sector
    if not sym:
        return jsonify({"error": "symbol required"}), 400
    # 1) persist classification
    sp = os.path.join(HERE, "marleg_sectors.json")
    sect = mvs.load(sp, {})
    sect[sym] = {"sector": sector, "industry": industry}
    try:
        with open(sp, "w", encoding="utf-8") as f:
            json.dump(sect, f, ensure_ascii=False)
    except Exception:
        pass
    # 2) compute volume metrics for the symbol
    m = None
    try:
        d = yf.download(sym + ".NS", period="2mo", interval="1d", progress=False)
        if d is not None and len(d):
            m = mvs.metrics(d)
    except Exception:
        m = None
    if not m:
        return jsonify({"ok": True, "symbol": sym, "sector": sector, "industry": industry,
                        "warn": "classified, but no market data yet (won't show until it has volume)"})
    # 3) merge into the volume cache and regroup (no full re-download)
    names = {r["s"]: r["n"] for r in mvs.load(os.path.join(HERE, "marleg_symbols.json"), [])}
    cache = mvs.load(mvs.OUT, {})
    flat = [st for sec in cache.get("sectors", []) for st in sec.get("stocks", [])]
    rows = {st["s"]: {"ud": st["ud"], "rvol": st.get("rvol"), "price": st.get("price"), "chg": st.get("chg")} for st in flat}
    rows[sym] = m
    mvs._write(rows, sect, names)
    return jsonify({"ok": True, "symbol": sym, "sector": sector, "industry": industry, **m})

# ----------------------------------------------------------------- U/D quadrant ("where does it stand")
# 8 quadrants = 4 zones (by U/D level, golden-ratio ladder) x 2 directions (U/D rising/falling).
_QUAD = {
    ("DISTRIBUTION", True):  ("Bottoming · turning up", "watch", "Earliest long signal — selling is drying up. Wait for follow-through into Balance before sizing."),
    ("DISTRIBUTION", False): ("Heavy distribution", "avoid", "Active selling, no bid. Avoid longs — short/exit territory."),
    ("BALANCE", True):       ("Accumulation building", "long", "Favourable long entry — money rotating in from neutral, lots of room to the ceiling."),
    ("BALANCE", False):      ("Losing steam", "wait", "Drifting weaker, no edge — wait for direction."),
    ("ACCUMULATION", True):  ("Strong & trending", "long-trail", "Trend long but already extended — ride with a trailing stop, don't chase fresh size."),
    ("ACCUMULATION", False): ("Accumulation fading", "trim", "Buyers stepping back — take profit / tighten; don't initiate."),
    ("CEILING", True):       ("Blow-off / euphoria", "no-long", "At the ceiling — do NOT initiate longs (poor R/R, mean-reversion risk). If long, trail tight."),
    ("CEILING", False):      ("Rolling over from the top", "short", "Distribution from the top — exit longs; short setup."),
}

def _volume_position(df):
    close, vol = df["Close"], df["Volume"]
    d = np.sign(close.diff()).fillna(0)
    up20 = vol.where(d > 0, 0.0).rolling(20).sum()
    dn20 = vol.where(d < 0, 0.0).rolling(20).sum()
    ud = (up20 / dn20.replace(0, np.nan)).dropna()
    if len(ud) < 25:
        return None
    cur = float(ud.iloc[-1])
    prev = float(ud.iloc[-11]) if len(ud) > 11 else float(ud.iloc[0])
    rising = cur >= prev
    hist = ud.iloc[-252:]
    pctile = int(round(float((hist <= cur).mean()) * 100))
    level = ("DISTRIBUTION" if cur < 0.7 else "BALANCE" if cur < 1.3
             else "ACCUMULATION" if cur < 2.618 else "CEILING")
    ceiling = (pctile >= 85) or (cur >= 2.618)
    quad, bias, note = _QUAD[(level, rising)]
    if ceiling and bias in ("long", "long-trail"):
        note = "At its own 1-yr ceiling — " + note + " R/R is poor here; wait for a reset."
        bias = "no-long"
    sma50 = round(float(ud.rolling(50).mean().iloc[-1]), 2) if len(ud) >= 50 else None
    sma250 = round(float(ud.rolling(250).mean().iloc[-1]), 2) if len(ud) >= 250 else None
    ctrl = "bulls" if cur >= 1.0 else "bears"
    if ctrl == "bulls":
        ctrl_txt = "🐂 Bulls in control" + (" — strengthening" if rising else " — but fading")
    else:
        ctrl_txt = "🐻 Bears in control" + (" — strengthening" if not rising else " — but easing")
    return {"ud": round(cur, 2), "ud_prev": round(prev, 2), "direction": "rising" if rising else "falling",
            "level": level, "percentile": pctile, "ceiling": ceiling, "ud_sma50": sma50, "ud_sma250": sma250,
            "quadrant": quad, "bias": bias, "note": note, "control": ctrl, "control_txt": ctrl_txt}

@app.route("/api/volume_position/<ticker>")
def api_volume_position(ticker):
    def do():
        df = _hist(ticker, "2y")  # 2y so the 250d U/D SMA has data; percentile still uses last ~1y
        if df is None or len(df) < 50:
            return {"error": "insufficient history"}
        return _volume_position(df) or {"error": "could not compute"}
    return jsonify(cached("vp:" + ticker.upper(), do, 300))

@app.route("/api/candles/<ticker>")
def api_candles(ticker):
    period = request.args.get("period", "2y")
    if period not in ("1y", "2y", "5y", "10y", "max"):
        period = "2y"
    def do():
        df = _hist(ticker, period)
        if df is None or len(df) < 30:
            return {"error": "no data"}
        df = df.dropna(subset=["Open", "High", "Low", "Close"])   # NaN bars poison browser JSON
        candles = [{"time": str(idx.date()), "open": round(float(r["Open"]), 2),
                    "high": round(float(r["High"]), 2), "low": round(float(r["Low"]), 2),
                    "close": round(float(r["Close"]), 2),
                    "vol": float(r["Volume"]) if r["Volume"] == r["Volume"] else 0.0}
                   for idx, r in df.iterrows()]
        return {"symbol": ticker.upper(), "period": period, "candles": candles}
    return jsonify(cached("candles:" + ticker.upper() + ":" + period, do, 300))

@app.route("/api/ichimoku/<ticker>")
def api_ichimoku(ticker):
    """Ichimoku 5 lines (chart-ready, cloud projected forward) + the bull-stack state."""
    import marleg_ichimoku
    period = request.args.get("period", "2y")
    if period not in ("1y", "2y", "5y", "10y", "max"):
        period = "2y"
    def do():
        df = _hist(ticker, period)
        if df is None or len(df) < 80:
            return {"error": "no data / need >=80 bars for Ichimoku"}
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        return {"symbol": ticker.upper(), **marleg_ichimoku.compute(df), "state": marleg_ichimoku.state(df)}
    return jsonify(cached("ichimoku:" + ticker.upper() + ":" + period, do, 300))

# ----------------------------------------------------------------- VWAP analytics (rolling + anchored)
@app.route("/api/vwap/<ticker>")
def api_vwap(ticker):
    period = request.args.get("period", "1y")
    if period not in ("6mo", "1y", "2y", "5y"):
        period = "1y"
    def do():
        df = _hist(ticker, period)          # reuse the cached download
        if df is None or len(df) < 60:
            return {"error": "no data"}
        return mvwap.vwap_analysis(ticker, df=df)
    return jsonify(cached("vwap:" + ticker.upper() + ":" + period, do, 300))

# ----------------------------------------------------------------- combined signal + news sentiment
_POS = set("surge surges surged jump jumped jumps rally rallies soar soars gain gains rise rises rose "
           "beat beats record profit profits growth upgrade upgrades outperform strong bullish buy deal "
           "deals order orders win wins bonus dividend expansion approval approves boost rallied".split())
_NEG = set("fall falls fell drop drops dropped slump slumps plunge plunges crash decline declines loss "
           "losses miss misses cut cuts downgrade downgrades weak bearish sell probe fraud fine penalty "
           "lawsuit resign resigns layoff layoffs slowdown warning warns concern hit halt halts ban bans".split())
def _sent_score(title):
    ws = re.findall(r"[a-z']+", title.lower())
    return sum(1 for w in ws if w in _POS) - sum(1 for w in ws if w in _NEG)

@app.route("/api/stock_score/<ticker>")
def api_stock_score(ticker):
    def do():
        a = equity_analysis(ticker)
        if "error" in a:
            return a
        news = _news((a.get("name") or ticker) + " share price", 7)
        scored, net = [], 0
        for it in news:
            s = _sent_score(it["title"]); net += s
            scored.append({**it, "s": s})
        nsent = max(-1.0, min(1.0, net / max(3, len(news)))) if news else 0.0
        z_vol = max(-3, min(3, a.get("vcs", 0) / 3.0))
        dirn = 1 if a["verdict"] == "LONG" else -1 if a["verdict"] == "SHORT" else 0
        conf = sum(1 for t in a.get("ta", []) if t[1] == "CONFIRMS")
        confl = sum(1 for t in a.get("ta", []) if t[1] == "CONFLICTS")
        z_tech = max(-3, min(3, dirn * (conf - confl) / 2.0))
        z_news = max(-3, min(3, nsent * 2.5))
        combined = round(0.45 * z_vol + 0.35 * z_tech + 0.20 * z_news, 2)
        mag = abs(combined)
        label = (("STRONG " if mag >= 1.5 else "WEAK " if mag < 0.6 else "")
                 + ("BULLISH" if combined > 0 else "BEARISH" if combined < 0 else "NEUTRAL"))
        sigma = a.get("ret_sigma") or 1.5
        spike_z = round(a["chg"] / sigma, 1) if sigma else 0
        spike = None
        if abs(spike_z) >= 2.5 or (a.get("rvol") or 0) >= 2.5:
            spike = {"chg": a["chg"], "rvol": a.get("rvol"), "z": spike_z,
                     "explain": scored[0]["title"] if scored else "no clear news catalyst — possible block deal / index-flow / derivatives"}
        return {"tk": a["tk"], "name": a.get("name"), "ltp": a["ltp"], "verdict": a["verdict"],
                "combined_z": combined, "label": label, "z_vol": round(z_vol, 2), "z_tech": round(z_tech, 2),
                "z_news": round(z_news, 2), "news_sentiment": round(nsent, 2), "news": scored,
                "spike": spike, "target": a.get("target"), "tgtpct": a.get("tgtpct")}
    return jsonify(cached("score:" + ticker.upper(), do, 180))

def _quote_yf(syms):
    tickers = " ".join(yftk(s) for s in syms)
    d = yf.download(tickers, period="5d", interval="1d", progress=False, group_by="ticker", threads=True)
    out = {}
    for s in syms:
        t = yftk(s)
        try:
            cl = (d[t]["Close"] if len(syms) > 1 else d["Close"]).dropna()
            p, pp = float(cl.iloc[-1]), float(cl.iloc[-2])
            out[s.upper()] = {"price": round(p, 2), "prev": round(pp, 2),
                              "chg": round((p / pp - 1) * 100, 2), "src": "yfinance"}
        except Exception:
            out[s.upper()] = {"error": "no data"}
    return out

@app.route("/api/quote")
def api_quote():
    syms = [s for s in request.args.get("symbols", "").split(",") if s.strip()]
    if not syms: return jsonify({})
    def do():
        g = groww()
        if g is not None:
            try:
                out = g.quote_table(syms)
                if any("price" in v for v in out.values()):   # got at least some live data
                    for v in out.values():
                        if "price" in v: v["src"] = "groww"
                    return out
            except Exception:
                pass
        return _quote_yf(syms)   # fallback: delayed daily
    return jsonify(cached("q:" + ",".join(sorted(syms)), do, 5))

# ----------------------------------------------------------------- broker (Groww account, real-time)
@app.route("/api/groww/health")
def api_groww_health():
    g = groww()
    if g is None:
        return jsonify({"connected": False, "error": _GROWW_ERR or "no client"})
    try:
        return jsonify({"connected": bool(g.token()), "src": g.src,
                        "live_orders": ALLOW_LIVE_ORDERS})
    except Exception as e:
        return jsonify({"connected": False, "error": str(e)[:200]})

def _broker(fn, key, ttl):
    g = groww()
    if g is None:
        return jsonify({"error": _GROWW_ERR or "no groww client"}), 503
    def do():
        try:                                   # token can lapse (approval flow) -> degrade, don't 500
            return fn(g) or {"error": "no data"}
        except Exception as e:
            return {"error": "Groww unavailable (auth lapsed?): " + str(e)[:140]}
    return jsonify(cached(key, do, ttl))

@app.route("/api/holdings")
def api_holdings():  return _broker(lambda g: g.holdings_data(),  "groww:holdings",  30)

@app.route("/api/positions")
def api_positions(): return _broker(lambda g: g.positions_data(), "groww:positions", 10)

@app.route("/api/winners")
def api_winners():   return _broker(lambda g: marleg_winners.board(g), "groww:winners", 20)

@app.route("/api/orders")
def api_orders():    return _broker(lambda g: g.orders_data(),    "groww:orders",    10)

@app.route("/api/margin")
def api_margin():    return _broker(lambda g: g.margin_data(),    "groww:margin",    15)

@app.route("/api/order", methods=["POST"])
def api_order():
    g = groww()
    if g is None:
        return jsonify({"error": _GROWW_ERR or "no groww client"}), 503
    p = request.get_json(force=True, silent=True) or {}
    if not all(p.get(k) for k in ("trading_symbol", "transaction_type", "quantity")):
        return jsonify({"error": "need trading_symbol, transaction_type, quantity"}), 400
    confirm = bool(p.get("confirm")) and ALLOW_LIVE_ORDERS
    try:
        res = g.place_order(
            p["trading_symbol"], p["transaction_type"], p["quantity"],
            exchange=p.get("exchange", "NSE"), segment=p.get("segment", "CASH"),
            product=p.get("product", "CNC"), order_type=p.get("order_type", "MARKET"),
            price=p.get("price", 0), trigger_price=p.get("trigger_price", 0),
            validity=p.get("validity", "DAY"), confirm=confirm)
        if p.get("confirm") and not ALLOW_LIVE_ORDERS:
            res["note"] = "LIVE ORDERS DISABLED. Start server with MARLEG_ALLOW_LIVE_ORDERS=1 to enable."
        return jsonify(res)
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 400

@app.route("/api/equity/<ticker>")
def api_equity(ticker):
    return jsonify(cached("eq:" + ticker.upper(), lambda: equity_analysis(ticker), 120))

@app.route("/api/macro")
def api_macro():
    def do():
        idx = {"NIFTY": "^NSEI", "VIX": "^INDIAVIX", "USDINR": "INR=X", "SENSEX": "^BSESN"}
        sectors = {"Nifty Bank": "^NSEBANK", "Nifty IT": "^CNXIT", "Nifty Auto": "^CNXAUTO",
                   "Nifty Pharma": "^CNXPHARMA", "Nifty FMCG": "^CNXFMCG", "Nifty Metal": "^CNXMETAL",
                   "Nifty Realty": "^CNXREALTY", "Nifty PSU Bank": "^CNXPSUBANK", "Nifty Energy": "^CNXENERGY",
                   "Nifty Infra": "^CNXINFRA", "Nifty Media": "^CNXMEDIA", "Nifty FinServ": "NIFTY_FIN_SERVICE.NS"}
        allt = {**idx, **{k: v for k, v in sectors.items()}}
        d = yf.download(" ".join(allt.values()), period="3mo", interval="1d", progress=False, group_by="ticker", threads=True)
        def ser(t):
            try: return (d[t]["Close"]).dropna()
            except Exception: return None
        out = {"indices": {}, "sectors": []}
        for k, t in idx.items():
            s = ser(t)
            if s is not None and len(s) > 1:
                out["indices"][k] = {"v": round(float(s.iloc[-1]), 2), "chg": round((float(s.iloc[-1])/float(s.iloc[-2])-1)*100, 2)}
        for nm, t in sectors.items():
            s = ser(t)
            if s is None or len(s) < 22: continue
            p = float(s.iloc[-1]); c1 = (p/float(s.iloc[-2])-1)*100
            c5 = (p/float(s.iloc[-6])-1)*100 if len(s) > 6 else 0
            c1m = (p/float(s.iloc[-22])-1)*100
            out["sectors"].append({"nm": nm, "tk": t, "px": round(p, 2), "c1": round(c1, 2), "c5": round(c5, 2), "c1m": round(c1m, 2)})
        # regime from NIFTY + India VIX
        ns = ser("^NSEI"); vix = out["indices"].get("VIX", {}).get("v", 15)
        if ns is not None and len(ns) > 50:
            ret = ns.pct_change().dropna(); vol20 = float(ret[-20:].std()*math.sqrt(252)*100); vol60 = float(ret[-60:].std()*math.sqrt(252)*100)
            sma50 = float(ns.rolling(50).mean().iloc[-1]); px = float(ns.iloc[-1])
            up = px > sma50; vexp = vol20 > vol60*1.2; vhi = vix > 18
            regime = "RISK-ON" if (up and not vexp and not vhi) else "RISK-OFF" if ((not up) and (vexp or vhi)) else "TRANSITIONAL"
            out["regime"] = {"regime": regime, "vol20": round(vol20, 1), "vol60": round(vol60, 1),
                             "sma50": round(sma50, 2), "nifty": round(px, 2), "vix": vix,
                             "trend": "BULLISH" if up else "BEARISH", "ret20": round((px/float(ns.iloc[-21])-1)*100, 2) if len(ns) > 21 else 0}
        return out
    return jsonify(cached("macro", do, 120))

@app.route("/api/fundamentals_cache")
def api_fundamentals_cache():
    """Compact precomputed fundamentals {sym: {q,pe,roe,growth,cov}} for the volume pod (batch-built)."""
    def do():
        try:
            with open(os.path.join(HERE, "marleg_fundamentals_cache.json"), encoding="utf-8") as f:
                c = json.load(f)
        except Exception:
            c = {}
        try:                                   # manual = FALLBACK only (don't clobber real data once the feed has it)
            import marleg_manual_fundamentals as mmf
            for k, v in mmf.compacts().items():
                c.setdefault(k, v)
        except Exception:
            pass
        return c
    return jsonify(cached("fund_cache", do, 600))

@app.route("/api/mf_search")
def api_mf_search():
    q = request.args.get("q") or ""
    return jsonify(cached("mf_search:" + q.lower(), lambda: marleg_mf.search(q), 300))

@app.route("/api/mf_directory")
def api_mf_directory():
    return jsonify(cached("mf_directory", marleg_mf.directory, 21600))   # classify whole universe -> cache 6h

@app.route("/api/mf_category")
def api_mf_category():
    bucket = request.args.get("bucket") or ""
    return jsonify(cached("mf_cat:" + bucket, lambda: marleg_mf.category(bucket), 21600))

@app.route("/api/mf/<code>")
def api_mf(code):
    def do():
        r = requests.get(f"https://api.mfapi.in/mf/{code}", timeout=12); j = r.json()
        data = j.get("data", []); meta = j.get("meta", {})
        if not data: return {"error": "no nav"}
        navs = [(d["date"], float(d["nav"])) for d in data if d.get("nav") not in (None, "", "0")]
        latest = navs[0][1]
        def cagr(years):
            target = navs[min(len(navs)-1, int(252*years))]
            yrs = years
            return round(((latest/target[1]) ** (1/yrs) - 1) * 100, 2) if target[1] > 0 else None
        series = [{"d": d, "nav": n} for d, n in navs[:520][::-1]]
        return {"meta": meta, "latest": round(latest, 4), "asof": navs[0][0],
                "cagr1": cagr(1), "cagr3": cagr(3), "cagr5": cagr(5), "series": series}
    return jsonify(cached("mf:" + code, do, 1800))

@app.route("/api/options/<und>")
def api_options(und):
    sym = und.upper()
    sym = "NIFTY" if sym in ("NIFTY", "NIFTY50") else sym
    def do():
        try:
            import nsepython
            d = nsepython.nse_optionchain_scrapper(sym)
            if isinstance(d, dict) and d.get("records", {}).get("data"):
                return d
            return {"error": "NSE returned empty — datacenter/VPN IP blocked by Akamai. NSE needs a RESIDENTIAL India IP (or a relay on a home India connection)."}
        except Exception as e:
            return {"error": "NSE fetch failed: " + str(e)[:140]}
    return jsonify(cached("opt:" + sym, do, 90))

# ----------------------------------------------------------------- news + next-day outlook
def _news(query, n=8):
    try:
        import urllib.parse, xml.etree.ElementTree as ET
        from email.utils import parsedate_to_datetime
        from datetime import datetime, timezone
        url = "https://news.google.com/rss/search?q=" + urllib.parse.quote(query) + "&hl=en-IN&gl=IN&ceid=IN:en"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        root = ET.fromstring(r.content)
        now = datetime.now(timezone.utc)
        items = []
        for it in root.findall(".//item"):
            title = (it.findtext("title") or "").strip()
            src = ""
            se = it.find("source")
            if se is not None and se.text:
                src = se.text.strip()
            elif " - " in title:
                title, src = title.rsplit(" - ", 1)
            pub = (it.findtext("pubDate") or "").strip()
            ts, ago = 0.0, ""
            try:
                dt = parsedate_to_datetime(pub)
                ts = dt.timestamp()
                hrs = (now - dt).total_seconds() / 3600
                ago = f"{int(hrs)}h ago" if hrs < 48 else f"{int(hrs / 24)}d ago"
            except Exception:
                pass
            items.append({"title": title.strip(), "source": src.strip(), "pub": pub, "ts": ts, "ago": ago})
        items.sort(key=lambda x: -x["ts"])               # NEWEST first — RSS order is by relevance, not date
        return items[:n]
    except Exception:
        return []

@app.route("/api/news")
def api_news():
    q = request.args.get("q", "Nifty 50 Sensex Indian stock market")
    return jsonify(cached("news:" + q, lambda: _news(q, 8), 300))

@app.route("/api/outlook")
def api_outlook():
    def do():
        tickers = "^NSEI ^BSESN INR=X ^INDIAVIX ^NSEBANK ^CNXIT ^CNXAUTO ^CNXPHARMA ^CNXFMCG ^CNXMETAL ^CNXREALTY ^CNXENERGY"
        d = yf.download(tickers, period="3mo", interval="1d", progress=False, group_by="ticker", threads=True)
        def ser(t):
            try: return d[t]["Close"].dropna()
            except Exception: return None
        ns = ser("^NSEI")
        if ns is None or len(ns) < 25:
            return {"error": "no NIFTY data"}
        px = float(ns.iloc[-1]); chg1 = (px/float(ns.iloc[-2])-1)*100; chg5 = (px/float(ns.iloc[-6])-1)*100
        sma20 = float(ns.rolling(20).mean().iloc[-1]); sma50 = float(ns.rolling(50).mean().iloc[-1])
        hi20 = float(ns[-20:].max()); lo20 = float(ns[-20:].min())
        ret = ns.pct_change().dropna(); vol20 = float(ret[-20:].std()*math.sqrt(252)*100); vol60 = float(ret[-60:].std()*math.sqrt(252)*100)
        vs = ser("^INDIAVIX"); vix = float(vs.iloc[-1]) if (vs is not None and len(vs)) else None
        ir = ser("INR=X"); usdinr = float(ir.iloc[-1]) if (ir is not None and len(ir)) else None
        inr5 = ((usdinr/float(ir.iloc[-6])-1)*100) if (ir is not None and len(ir) > 6) else 0.0
        sxs = ser("^BSESN"); sx = float(sxs.iloc[-1]) if (sxs is not None and len(sxs)) else None
        secmap = {"Bank":"^NSEBANK","IT":"^CNXIT","Auto":"^CNXAUTO","Pharma":"^CNXPHARMA","FMCG":"^CNXFMCG","Metal":"^CNXMETAL","Realty":"^CNXREALTY","Energy":"^CNXENERGY"}
        secs = []
        for nm, t in secmap.items():
            s = ser(t)
            if s is not None and len(s) > 6:
                secs.append({"nm": nm, "c5": (float(s.iloc[-1])/float(s.iloc[-6])-1)*100, "c1": (float(s.iloc[-1])/float(s.iloc[-2])-1)*100})
        secs.sort(key=lambda x: -x["c5"])
        leaders = secs[:3]; laggards = secs[-3:][::-1]; green = sum(1 for s in secs if s["c1"] > 0)
        up = px > sma50; vexp = vol20 > vol60*1.2; vhi = (vix or 15) > 18
        regime = "RISK-ON" if (up and not vexp and not vhi) else "RISK-OFF" if ((not up) and (vexp or vhi)) else "TRANSITIONAL"
        trend = "BULLISH" if up else "BEARISH"
        news = _news("Nifty 50 Sensex Indian stock market", 7)

        def sgn(x): return ("up %.2f%%" % x) if x >= 0 else ("down %.2f%%" % abs(x))
        sx_str = (", with the Sensex at %s" % format(sx, ",.0f")) if sx else ""
        vix_str = (" India VIX sits at %.1f%s." % (vix, " — elevated; the market is paying up for protection" if vix > 18 else " — subdued, no panic bid in protection")) if vix else ""
        N = []
        N.append({"h": "Where we stand", "p":
            ("The Nifty 50 closed at %s, %s on the day and %s over the past five sessions%s. "
             "It is trading %s its 50-day average (%s) and %s its 20-day (%s), so the medium-term trend reads %s.%s "
             "Realised 20-day volatility is %.0f%% versus %.0f%% over 60 days, so volatility is %s. Net, the regime is %s.")
            % (format(px, ",.0f"), sgn(chg1), sgn(chg5), sx_str,
               "above" if px > sma50 else "below", format(sma50, ",.0f"),
               "above" if px > sma20 else "below", format(sma20, ",.0f"), trend.lower(), vix_str,
               vol20, vol60, "expanding" if vexp else "contained", regime)})
        if leaders and laggards:
            cyc = any(l["nm"] in ("Auto", "Metal", "Realty", "Bank") for l in leaders)
            N.append({"h": "Beneath the surface", "p":
                ("Over the last five sessions, leadership sat in %s, while %s lagged. %d of %d tracked sectors closed green. %s "
                 "Breadth that confirms the index move is healthy; a rising index on narrow breadth is the classic warning sign.")
                % (", ".join("%s (%s)" % (l["nm"], sgn(l["c5"])) for l in leaders),
                   ", ".join("%s (%s)" % (l["nm"], sgn(l["c5"])) for l in laggards),
                   green, len(secs),
                   "Cyclical/high-beta leadership over defensives points to genuine risk appetite." if cyc
                   else "Defensive leadership (FMCG/Pharma/IT) over cyclicals is the textbook risk-off rotation — money hiding, not chasing.")})
        if usdinr:
            N.append({"h": "Cross-asset & the rupee", "p":
                ("USD/INR is at %.2f, %s over five sessions. %s")
                % (usdinr, sgn(inr5),
                   "A weakening rupee is a headwind — it pressures the import bill and often coincides with FII selling; exporters (IT/pharma) cushion it while importers (oil, capital goods) wear it." if inr5 > 0.2
                   else "A firming rupee is a tailwind for foreign flows and eases the inflation/import path — supportive for domestic cyclicals and banks." if inr5 < -0.2
                   else "The rupee is broadly stable — neutral for cross-border flows right now.")})
        if news:
            N.append({"h": "What the tape is reading", "p":
                ("Headlines in rotation over the weekend and recent sessions: %s. Read these as the catalyst backdrop — the stories that explain where volume is concentrating. "
                 "When a leading sector moves on visible news the move has a narrative; when it moves on no news, that silent volume is often the more telling signal.")
                % ("; ".join(n["title"] for n in news[:4]))})
        sup = max(lo20, sma20) if px > sma20 else lo20
        res = min(hi20, sma50) if px < sma50 else hi20
        bias = ("constructive — dips toward support are likely bought while the index holds its averages" if (up and chg5 > 0)
                else "cautious — rallies into resistance are likely sold while the index sits below its averages" if (not up)
                else "two-way and headline-driven — neither side has clear control")
        N.append({"h": "Next-day setup", "p":
            ("Into the next session the bias is %s. Key levels: support around %s (20-day low / 20-DMA zone) and resistance near %s (recent high / 50-DMA). "
             "A decisive open-and-hold above %s on expanding volume confirms risk-on and puts the five-day leaders in play for continuation; a break and hold below %s flips the tape defensive. "
             "With VIX at %s, expect a %s opening range — assess the first 15 minutes before committing. This is context, not advice; confirm with the volume and sector gates before acting.")
            % (bias, format(sup, ",.0f"), format(res, ",.0f"), format(res, ",.0f"), format(sup, ",.0f"),
               ("%.1f" % vix) if vix else "n/a", "wider" if (vix and vix > 16) else "tighter")})
        return {"asof": str(ns.index[-1].date()), "regime": regime, "trend": trend, "nifty": round(px, 2),
                "chg1": round(chg1, 2), "chg5": round(chg5, 2), "vix": round(vix, 2) if vix else None,
                "levels": {"support": round(sup), "resistance": round(res)},
                "narrative": N, "news": news}
    return jsonify(cached("outlook", do, 600))

# ----------------------------------------------------------------- fundamentals (accounting / valuation)
def _pct(x): return round(x*100, 1) if isinstance(x, (int, float)) else None
def _f(x):   return ("%.2f" % x) if isinstance(x, (int, float)) else "n/a"

def _piotroski(t):
    try:
        fin = t.financials; bs = t.balance_sheet; cf = t.cashflow
        def row(df, *names):
            for n in names:
                if df is not None and n in df.index:
                    return df.loc[n].dropna()
            return None
        def v(s, i): return float(s.iloc[i]) if (s is not None and len(s) > i) else None
        ni = row(fin, "Net Income", "Net Income Common Stockholders")
        ta = row(bs, "Total Assets"); cfo = row(cf, "Operating Cash Flow", "Total Cash From Operating Activities")
        ltd = row(bs, "Long Term Debt")
        ca = row(bs, "Current Assets", "Total Current Assets"); cl = row(bs, "Current Liabilities", "Total Current Liabilities")
        rev = row(fin, "Total Revenue"); gp = row(fin, "Gross Profit"); sh = row(bs, "Share Issued", "Ordinary Shares Number")
        score = 0; checks = 0
        def chk(c):
            nonlocal score, checks
            if c is None: return
            checks += 1
            if c: score += 1
        ni0, ta0, ta1 = v(ni, 0), v(ta, 0), v(ta, 1)
        roa0 = (ni0/ta0) if (ni0 is not None and ta0) else None
        roa1 = (v(ni, 1)/ta1) if (v(ni, 1) is not None and ta1) else None
        chk(ni0 is not None and ni0 > 0)
        chk(v(cfo, 0) is not None and v(cfo, 0) > 0)
        chk(roa0 is not None and roa1 is not None and roa0 > roa1)
        chk(v(cfo, 0) is not None and ni0 is not None and v(cfo, 0) > ni0)
        chk(v(ltd, 0) is not None and v(ltd, 1) is not None and v(ltd, 0) < v(ltd, 1))
        cur0 = (v(ca, 0)/v(cl, 0)) if (v(ca, 0) and v(cl, 0)) else None
        cur1 = (v(ca, 1)/v(cl, 1)) if (v(ca, 1) and v(cl, 1)) else None
        chk(cur0 is not None and cur1 is not None and cur0 > cur1)
        chk(v(sh, 0) is not None and v(sh, 1) is not None and v(sh, 0) <= v(sh, 1))
        gm0 = (v(gp, 0)/v(rev, 0)) if (v(gp, 0) and v(rev, 0)) else None
        gm1 = (v(gp, 1)/v(rev, 1)) if (v(gp, 1) and v(rev, 1)) else None
        chk(gm0 is not None and gm1 is not None and gm0 > gm1)
        at0 = (v(rev, 0)/ta0) if (v(rev, 0) and ta0) else None
        at1 = (v(rev, 1)/ta1) if (v(rev, 1) and ta1) else None
        chk(at0 is not None and at1 is not None and at0 > at1)
        return {"score": score, "of": checks}
    except Exception:
        return None

@app.route("/api/fundamentals/<ticker>")
def api_fundamentals(ticker):
    tk = ticker.upper()
    # Robust provider: ratios computed from the financial statements (TTM), .info only as
    # fallback, with sanity + formatting guards and honest coverage. See marleg_fundamentals.py.
    def do():
        f = marleg_fundamentals.fundamentals(tk, NAMES)
        if "error" in f or f.get("qscore") is None:    # auto-feed empty (recent SME / throttle) -> manual override
            try:
                import marleg_manual_fundamentals as mmf
                m = mmf.record(tk)
                if m:
                    return m
            except Exception:
                pass
        try:                                   # cache-on-view: warm the volume-pod cache, but only with REAL data
            h = f.get("health") or {}
            if "error" not in f and (f.get("qscore") is not None or h.get("P/E") is not None or h.get("ROE") is not None):
                import marleg_fundamentals_scan as fsc
                fsc.cache_one(tk, f)
                _CACHE.pop("fund_cache", None)
        except Exception:
            pass
        try:                                   # surface manual flags (e.g. cash-flow warnings the auto-feed can't see)
            import marleg_manual_fundamentals as mmf
            m = mmf.record(tk)
            if m and "error" not in f:
                f["narrative"] = (m.get("narrative") or []) + (f.get("narrative") or [])
                if m.get("manual_flags"):
                    f["manual_flags"] = m["manual_flags"]
        except Exception:
            pass
        return f
    return jsonify(cached("fund:" + tk, do, 1800))

# ----------------------------------------------------------------- UNIVERSAL analysis (fuse all pillars)
@app.route("/api/analyze/<ticker>")
def api_analyze(ticker):
    tk = ticker.upper()
    def do():
        tech = equity_analysis(tk)
        base = "http://127.0.0.1:8777"
        def getj(path, **kw):
            try: return requests.get(base + path, timeout=kw.pop("timeout", 45), **kw).json()
            except Exception: return {}
        fund = getj("/api/fundamentals/" + tk)
        macro = getj("/api/macro")
        news = getj("/api/news", params={"q": tk + " stock NSE India"}, timeout=15)
        if not isinstance(news, list): news = []
        # value axis [-1..1]
        val = 0.0
        if fund and not fund.get("error"):
            fv = fund.get("verdict")
            val += 1 if fv == "UNDERVALUED" else -1 if fv == "OVERVALUED" else 0
            up = fund.get("upside") or 0; val += 0.5 if up > 10 else -0.5 if up < -10 else 0
            q = fund.get("qscore") or 0; val += 0.5 if q >= 65 else -0.5 if q < 40 else 0
        val = max(-2.0, min(2.0, val)) / 2.0
        # timing axis [-1..1]
        tim = 0.0
        if tech and not tech.get("error"):
            bt = 1 if tech.get("verdict") == "LONG" else -1 if tech.get("verdict") == "SHORT" else 0
            tim = bt * (tech.get("conv", 50) / 100.0)
        regime = (macro.get("regime") or {}).get("regime") if isinstance(macro, dict) else None
        trend = (macro.get("regime") or {}).get("trend") if isinstance(macro, dict) else None
        combined = (val + tim) / 2.0
        cheap, rich = val > 0.15, val < -0.15
        accum, distrib = tim > 0.15, tim < -0.15
        conflicts = []
        if cheap and accum: quad, verdict = "Undervalued + accumulating", "HIGH-CONVICTION LONG"
        elif rich and distrib: quad, verdict = "Overvalued + distributing", "HIGH-CONVICTION SHORT"
        elif cheap and distrib:
            quad, verdict = "Cheap but distributing", "VALUE TRAP — WAIT"
            conflicts.append("Fundamentally cheap but volume is distributing — wait for accumulation to confirm.")
        elif rich and accum:
            quad, verdict = "Expensive but accumulating", "MOMENTUM ONLY"
            conflicts.append("Volume bullish but fundamentally rich — a momentum trade, not an investment; tighten stops.")
        elif accum: quad, verdict = "Accumulating · neutral value", "LONG · timing"
        elif distrib: quad, verdict = "Distributing · neutral value", "SHORT · timing"
        elif cheap: quad, verdict = "Undervalued · quiet volume", "LONG · value"
        elif rich: quad, verdict = "Overvalued · quiet volume", "SHORT · value"
        else: quad, verdict = "Balanced", "NEUTRAL"
        conviction = round(min(95, 40 + abs(combined) * 55 + (8 if news else 0)))
        parts = []
        if regime: parts.append("Market regime reads %s%s." % (regime, (" with a %s tape" % trend.lower()) if trend else ""))
        if fund and not fund.get("error"):
            pio = ("%s/%s" % (fund["piotroski"]["score"], fund["piotroski"]["of"])) if fund.get("piotroski") else "n/a"
            parts.append("Fundamentally it screens %s — fair value ~Rs %s, Street 1-yr target Rs %s (%s%% %s), quality %s/100, Piotroski %s." % (
                (fund.get("verdict") or "n/a").lower(), fund.get("fair"), fund.get("target"),
                abs(fund.get("upside")) if fund.get("upside") is not None else "—",
                "upside" if (fund.get("upside") or 0) >= 0 else "downside", fund.get("qscore"), pio))
        if tech and not tech.get("error"):
            parts.append("On volume the read is %s (conviction %s, VCS %s)." % (tech.get("verdict"), tech.get("conv"), tech.get("vcs")))
        if news: parts.append("A live news catalyst is in the flow.")
        parts.append("Net call: %s — %s." % (quad, verdict))
        return {"tk": tk, "name": (tech or {}).get("name", tk), "price": (tech or {}).get("ltp"),
                "pillars": {
                    "technical": {"verdict": (tech or {}).get("verdict"), "conviction": (tech or {}).get("conv"), "vcs": (tech or {}).get("vcs")},
                    "fundamental": {"verdict": fund.get("verdict"), "fair": fund.get("fair"), "target": fund.get("target"),
                                    "upside": fund.get("upside"), "qscore": fund.get("qscore"), "piotroski": fund.get("piotroski")},
                    "macro": {"regime": regime, "trend": trend},
                    "catalyst": {"count": len(news), "top": news[:3]}},
                "synthesis": {"quadrant": quad, "verdict": verdict, "conviction": conviction,
                              "value_score": round(val, 2), "timing_score": round(tim, 2),
                              "rationale": " ".join(parts), "conflicts": conflicts}}
    return jsonify(cached("analyze:" + tk, do, 120))

# ----------------------------------------------------------------- live monitor (auto-refresh, no page reload)
_AVG_LAST, _VOL_LAST = {}, {}   # last-good per-symbol fallbacks (yfinance is flaky on bulk pulls)

def _avg_prev(syms):
    """20-day avg volume + prev close per symbol — changes only daily, cache 1h.
    Validates each symbol and back-fills from last-good, so a partial/garbage
    yfinance pull can't poison the cache with missing or absurd values."""
    def do():
        out = {}
        try:
            d = yf.download([s + ".NS" for s in syms], period="1mo", interval="1d",
                            progress=False, group_by="ticker", threads=True)
            for s in syms:
                try:
                    dc = (d[s + ".NS"] if len(syms) > 1 else d)
                    cl = dc["Close"].dropna(); vv = dc["Volume"].dropna()
                    if len(vv) >= 10 and len(cl) >= 2:                 # need a real history
                        avg = float(vv.iloc[-21:-1].mean())
                        if avg > 0:
                            out[s] = [avg, float(cl.iloc[-2])]
                            _AVG_LAST[s] = out[s]
                except Exception:
                    pass
        except Exception:
            pass
        for s in syms:                                                # back-fill gaps
            if s not in out and s in _AVG_LAST:
                out[s] = _AVG_LAST[s]
        return out
    return cached("avgprev:" + ",".join(sorted(syms)), do, 3600)


def _intraday_vol(syms):
    """Today's cumulative volume + bar count + last intraday price per symbol
    (yfinance 1m), cache 30s. Volume pace moves slowly, so a 30s cache cuts
    yfinance load (less throttling = fewer partial failures); gaps use last-good."""
    def do():
        out = {}
        try:
            d = yf.download([s + ".NS" for s in syms], period="1d", interval="1m",
                            progress=False, group_by="ticker", threads=True)
        except Exception:
            d = None
        for s in syms:
            try:
                cm = (d[s + ".NS"] if len(syms) > 1 else d)
                v = cm["Volume"].dropna(); c = cm["Close"].dropna()
                if len(v) >= 1 and float(v.sum()) > 0:
                    out[s] = [float(v.sum()), len(v), float(c.iloc[-1]) if len(c) else None]
                    _VOL_LAST[s] = out[s]
            except Exception:
                pass
        for s in syms:
            if s not in out and s in _VOL_LAST:
                out[s] = _VOL_LAST[s]
        return out
    return cached("livevol:" + ",".join(sorted(syms)), do, 30)

@app.route("/api/live")
def api_live():
    syms = [s.strip().upper() for s in request.args.get("syms", "").split(",") if s.strip()][:30]
    if not syms:
        return jsonify({})
    def do():
        ap = _avg_prev(syms)            # 1h-cached daily avg vol + prev close (validated)
        vol = _intraday_vol(syms)       # 30s-cached today cumulative volume (+ fallback price)
        gq, g = {}, groww()
        if g is not None:
            try:
                gq = g.quote_table(syms)   # real-time price / prev / chg (batch ltp + ohlc)
            except Exception:
                gq = {}
        out, live = {}, False
        for s in syms:
            try:
                gv = gq.get(s) or {}
                avg, dprev = (ap.get(s) or [None, None])
                tv = vol.get(s)
                yp = tv[2] if (tv and len(tv) > 2) else None
                if gv.get("price") is not None:                       # real-time (Groww)
                    price, src, live = float(gv["price"]), "groww", True
                    prev = gv.get("prev") or dprev
                    chg = gv.get("chg") if gv.get("chg") is not None else \
                        (round((price / prev - 1) * 100, 2) if prev else 0.0)
                elif yp is not None:                                  # delayed (yfinance) fallback
                    price, src, prev = yp, "yfinance", dprev
                    chg = round((price / prev - 1) * 100, 2) if prev else 0.0
                else:
                    out[s] = {"error": 1}; continue
                # pace-adjusted volume ratio — validated; drop if implausible (poisoned avg)
                volr = None
                if tv and avg:
                    todayvol, nbars = tv[0], tv[1]
                    frac = min(1.0, max(0.05, nbars / 375.0)) if nbars else 1.0
                    vr = todayvol / (avg * frac)
                    volr = round(vr, 2) if vr < 30 else None
                vconf = volr is not None and volr > 0.9                # volume confirms the move
                tag = ("BUYING" if (chg > 0.3 and vconf) else "FADING" if (chg < -0.3 and vconf)
                       else "QUIET" if (volr is not None and volr < 0.5) else "FLAT")
                out[s] = {"price": round(price, 2), "chg": chg, "volr": volr, "tag": tag, "src": src}
            except Exception:
                out[s] = {"error": 1}
        out["_ts"] = str(pd.Timestamp.now(tz="Asia/Kolkata"))[:19] if live else None
        out["_src"] = "groww" if live else "yfinance"
        return out
    return jsonify(cached("live:" + ",".join(sorted(syms)), do, 5))

# ----------------------------------------------------------------- event cascade ("the painting")
@app.route("/api/cascade/events")
def api_cascade_events():
    return jsonify([{"key": k, "label": v["label"]} for k, v in marleg_cascade.EVENTS.items()])

@app.route("/api/cascade")
def api_cascade():
    ev = request.args.get("event", "oil_shock_up")
    if ev not in marleg_cascade.EVENTS:
        return jsonify({"error": "unknown event", "events": list(marleg_cascade.EVENTS)})
    return jsonify(cached("cascade:" + ev, lambda: marleg_cascade.build_cascade(ev), 600))

@app.route("/api/vol")
def api_vol():
    opt = request.args.get("opt", "")
    side = request.args.get("side", "long")
    qty = int(request.args.get("qty", "0") or 0)
    if not opt:
        return jsonify({"error": "pass ?opt=<SYMBOL>&side=&qty="})
    return jsonify(cached(f"vol:{opt}:{side}:{qty}", lambda: marleg_vol.analyze_option(opt, side, qty), 60))

@app.route("/api/structures/compare")
def api_structures_compare():
    return jsonify(cached("struct:compare", lambda: mstruct.compare_data(), 60))

@app.route("/api/structures/backtest")
def api_structures_backtest():
    yrs = int(request.args.get("years", "5") or 5)
    return jsonify(cached(f"struct:bt:{yrs}", lambda: mstruct.backtest_data(yrs), 3600))

@app.route("/api/projection/<ticker>")
def api_projection(ticker):
    tk = ticker.upper()
    return jsonify(cached("proj:" + tk, lambda: marleg_projection.project(tk), 1800))

@app.route("/api/regime")
def api_regime():
    return jsonify(cached("regime", marleg_regime.compute, 3600))   # heavy 1y pull -> cache 1h

@app.route("/api/thesis")
def api_thesis():
    return jsonify(cached("thesis:ledger", lambda: marleg_thesis.ledger(), 1800))

@app.route("/api/thesis/<key>")
def api_thesis_one(key):
    return jsonify(cached("thesis:" + key, lambda: marleg_thesis.analyze(key), 1800))

@app.route("/api/smartmoney/<ticker>")
def api_smartmoney(ticker):
    tk = ticker.upper()
    return jsonify(cached("smart:" + tk, lambda: marleg_smartmoney.flow(tk), 43200))

@app.route("/api/smartmoney_screen")
def api_smartmoney_screen():
    # rank a liquid universe by 1Q institutional inflow; quarterly data -> cache 12h
    def _run():
        try:
            import marleg_volume_scan as mvs
            uni = mvs.SEED[:24]
        except Exception:
            uni = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "ITC", "LT", "SBIN",
                   "AXISBANK", "BHARTIARTL", "TITAN", "MARUTI", "SUNPHARMA", "NTPC",
                   "TATASTEEL", "ULTRACEMCO", "ASIANPAINT", "BAJFINANCE", "M&M", "WIPRO"]
        return {"rows": marleg_smartmoney.screen(uni), "n": len(uni)}
    return jsonify(cached("smartmoney_screen", _run, 43200))

@app.route("/api/business/<ticker>")
def api_business(ticker):
    tk = ticker.upper()
    return jsonify(cached("biz:" + tk, lambda: marleg_business.analyze(tk), 43200))   # heavy peer pull -> cache 12h

@app.route("/api/mindhive")
def api_mindhive():
    return jsonify(cached("mindhive:state", marleg_mindhive.synthesize, 1800))   # self-disk-caches 3h too

@app.route("/api/mindhive/ask", methods=["GET", "POST"])
def api_mindhive_ask():
    q = ((request.get_json(force=True, silent=True) or {}).get("q") if request.method == "POST"
         else request.args.get("q", ""))
    q = (q or "").strip()
    if not q:
        return jsonify({"intent": "help", "answer": "Ask me something — e.g. \"what's the story\"."})
    st = cached("mindhive:state", marleg_mindhive.synthesize, 1800)
    try:
        return jsonify(marleg_mindhive.ask(q, st))
    except Exception as e:
        return jsonify({"intent": "error", "answer": f"Mindhive error: {e}"})

@app.route("/api/intraday/<ticker>")
def api_intraday(ticker):
    return jsonify(cached("intraday:" + ticker.upper(), lambda: marleg_intraday.analyze(ticker), 600))

@app.route("/api/intraday/<ticker>/signature")
def api_intraday_sig(ticker):
    return jsonify(cached("intraday:sig:" + ticker.upper(), lambda: marleg_intraday.signature(ticker), 3600))

@app.route("/api/intraday/<ticker>/live")
def api_intraday_live(ticker):
    return jsonify(cached("intraday:live:" + ticker.upper(), lambda: marleg_intraday.live(ticker), 45))

@app.route("/api/intraday/<ticker>/position")
def api_intraday_position(ticker):
    iv = request.args.get("interval", 5, type=int)
    return jsonify(cached(f"intraday:pos:{ticker.upper()}:{iv}", lambda: marleg_intraday.position(ticker, iv), 20))

@app.route("/api/intraday/<ticker>/dd")
def api_intraday_dd(ticker):
    iv = request.args.get("interval", 5, type=int)
    return jsonify(cached(f"intraday:dd:{ticker.upper()}:{iv}", lambda: marleg_intraday.due_diligence(ticker, iv), 60))

@app.route("/api/intraday/<ticker>/intervals")
def api_intraday_intervals(ticker):
    return jsonify(cached("intraday:iv:" + ticker.upper(), lambda: marleg_intraday.interval_advisor(ticker), 1800))

@app.route("/api/intraday/<ticker>/sparklines")
def api_intraday_sparklines(ticker):
    return jsonify(cached("intraday:spk:" + ticker.upper(), lambda: marleg_intraday.sparklines(ticker), 1800))

@app.route("/api/intraday/<ticker>/rsi")
def api_intraday_rsi(ticker):
    iv = request.args.get("interval", 5, type=int)
    return jsonify(cached(f"intraday:rsi:{ticker.upper()}:{iv}", lambda: marleg_intraday.rsi_engine(ticker, iv), 30))

@app.route("/api/intraday/<ticker>/mood")
def api_intraday_mood(ticker):
    import marleg_mood
    return jsonify(cached("mood:" + ticker.upper(), lambda: marleg_mood.mood(ticker), 120))

@app.route("/api/intraday/<ticker>/smartstop")
def api_smartstop(ticker):
    import marleg_smartstop
    entry = request.args.get("entry", type=float)
    return jsonify(cached(f"smartstop:{ticker.upper()}:{entry}", lambda: marleg_smartstop.smart_stop(ticker, entry), 300))

@app.route("/api/buyhold/<tk>")
def api_buyhold(tk):
    return jsonify(cached("buyhold:" + tk.upper(), lambda: marleg_buyhold.compounder_score(tk), 3600))

@app.route("/api/buyhold_screen")
def api_buyhold_screen():
    n = int(request.args.get("n") or 40)
    return jsonify(cached("buyhold_screen:" + str(n), lambda: marleg_buyhold.screen(n), 1800))

@app.route("/api/patterns/<tk>")
def api_patterns(tk):
    return jsonify(cached("patterns:" + tk.upper(), lambda: marleg_patterns.analyze(tk), 600))

@app.route("/api/pattern_reliability")
def api_pattern_reliability():
    return jsonify(cached("pattern_reliability", marleg_patterns.reliability, 3600))

@app.route("/api/patterns_scan")
def api_patterns_scan():
    def do():
        try:
            with open(os.path.join(HERE, "marleg_pattern_scan.json"), encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"error": "no pattern scan yet — run: python marleg_pattern_scan.py", "groups": []}
    return jsonify(cached("patterns_scan", do, 600))

@app.route("/api/overextension/<ticker>")
def api_overextension(ticker):
    return jsonify(cached("overext:" + ticker.upper(), lambda: marleg_overextension.chase_check(ticker), 1800))

@app.route("/api/weekend_carry")
def api_weekend_carry():
    return jsonify(cached("weekend_carry", marleg_weekend.carry_scan, 120))   # live; refresh through the session

@app.route("/api/weekend_board")
def api_weekend_board():
    mode = request.args.get("mode", "friday")
    if mode not in ("friday", "monday"):
        mode = "friday"
    return jsonify(cached("weekend_board:" + mode, lambda: marleg_weekend.weekend_board(mode), 600))

@app.route("/api/weekend_stock/<ticker>")
def api_weekend_stock(ticker):
    return jsonify(cached("weekend_stock:" + ticker.upper(), lambda: marleg_weekend.weekend_stock(ticker), 600))

@app.route("/api/portfolio_var")
def api_portfolio_var():
    return jsonify(cached("portfolio_var", lambda: marleg_var.portfolio_risk(), 300))

@app.route("/api/transition")
def api_transition():
    """Held longs' composite bias + transition flags (long->short rolling-over = the big signal)."""
    import marleg_transition
    extra = [x for x in (request.args.get("watch", "").upper().split(",")) if x]
    return jsonify(cached("transition:" + ",".join(sorted(extra)), lambda: marleg_transition.watch(extra), 300))

@app.route("/api/has_options/<tk>")
def api_has_options(tk):
    return jsonify({"sym": tk.upper(), "has_options": mom.has_options(tk)})

@app.route("/api/option_chain/<underlying>")
def api_option_chain(underlying):
    n = int(request.args.get("n") or 5)
    key = f"opt_chain:{underlying.upper()}:{n}"
    return jsonify(cached(key, lambda: mom.chain(underlying, n=n), 30))

@app.route("/api/option_monitor/<sym>")
def api_option_monitor(sym):
    side = (request.args.get("side") or "long").lower()
    qty = int(request.args.get("qty") or 0)
    key = f"opt_mon:{sym.upper()}:{side}:{qty}"
    return jsonify(cached(key, lambda: mom.analyze(sym, side=side, qty=qty), 20))

@app.route("/api/whatif")
def api_whatif():
    """Scenario tool: add/trim a position -> recomputed beta, weekend VaR, correlation, diversification."""
    tk = (request.args.get("tk") or "").upper().strip()
    qty = request.args.get("qty") or "0"
    side = (request.args.get("side") or "buy").lower()
    key = f"whatif:{tk}:{qty}:{side}"
    return jsonify(cached(key, lambda: marleg_var.whatif(tk, qty, side), 120))

@app.route("/api/refresh", methods=["GET", "POST"])
def api_refresh():
    """Force-refresh: drop cached responses so every pod re-pulls live on next load.
    ?key=<prefix> clears only matching keys (e.g. ?key=volume); no arg clears everything."""
    prefix = request.args.get("key")
    with _LOCK:
        if prefix:
            ks = [k for k in _CACHE if k.startswith(prefix)]
            for k in ks:
                _CACHE.pop(k, None)
            n = len(ks)
        else:
            n = len(_CACHE)
            _CACHE.clear()
    return jsonify({"cleared": n, "scope": prefix or "all", "ts": time.strftime("%H:%M:%S")})

@app.route("/api/diag")
def api_diag():
    """System self-diagnosis — auth, feeds, processes, freshness, modules, routes, tasks."""
    import marleg_diag
    def self_test(path):
        try:
            r = app.test_client().get(path)
            return {"code": r.status_code, "ok": r.status_code == 200,
                    "snippet": r.get_data(as_text=True)[:200]}
        except Exception as e:
            return {"code": 0, "ok": False, "snippet": str(e)[:200]}
    # short TTL so the page reflects "now", but repeated clicks don't hammer feeds
    return jsonify(cached("diag", lambda: marleg_diag.diagnose(
        groww_getter=groww, cache=_CACHE, self_test=self_test), 20))

@app.route("/api/nifty_sim")
def api_nifty_sim():
    return jsonify(cached("nifty_sim", marleg_nifty_sim.run_tournament, 21600))   # heavy multi-year backtest -> cache 6h

@app.route("/api/expiry")
def api_expiry():
    """Options-expiry tracker: where NIFTY is in the weekly/monthly expiry cycle + the backtested
    day-of-week / expiry-day effect (expiry = chop/pin, not trend) + a plain live read."""
    import marleg_expiry
    return jsonify(cached("expiry", lambda: marleg_expiry.build(), 3600))

@app.route("/api/sectoral")
def api_sectoral():
    return jsonify(cached("sectoral", marleg_nifty_sim.sector_compare, 21600))   # 4 tournaments -> cache 6h

@app.route("/api/volume_book")
def api_volume_book():
    return jsonify(cached("volume_book", marleg_volume_book.book, 3600))   # universe fetch + backtest -> cache 1h

@app.route("/api/fibmap/<symbol>")
def api_fibmap(symbol):
    import marleg_fibmap
    sym = symbol.upper()
    return jsonify(cached("fib:" + sym, lambda: marleg_fibmap.fibmap(sym), 3600))


@app.route("/api/bias_dial")
def api_bias_dial():
    import marleg_bias_dial
    return jsonify(cached("bias_dial", marleg_bias_dial.run, 1800))


@app.route("/api/short_radar")
def api_short_radar():
    # eclipse watch: overbought + extended + spike, fade-history aware; validation from disk
    import marleg_short_radar as sr
    def _run():
        res = sr.scan()
        try:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "marleg_short_radar.json")) as f:
                res["validation"] = json.load(f).get("validation")
        except Exception:
            pass
        return res
    return jsonify(cached("short_radar", _run, 600))


@app.route("/api/volume_ledger")
def api_volume_ledger():
    # suggestion memory: record today (idempotent) + streaks + the streak-age study
    import marleg_volume_ledger as vl
    def _run():
        try:
            vl.record_today()
        except Exception:
            pass
        return vl.summary()
    return jsonify(cached("volume_ledger", _run, 600))   # 10 min — keeps 'now' price fresh in-session


@app.route("/api/builder/templates")
def api_builder_templates():
    import marleg_script_builder as msb
    return jsonify({"templates": msb.TEMPLATES, "limits": msb.LIMITS})

@app.route("/api/builder/backtest", methods=["POST"])
def api_builder_backtest():
    import marleg_script_builder as msb
    try:
        return jsonify(msb.simulate(request.get_json(force=True, silent=True) or {}))
    except Exception as e:
        return jsonify({"error": str(e)[:200]})

@app.route("/api/builder/build", methods=["POST"])
def api_builder_build():
    import marleg_script_builder as msb
    try:
        return jsonify(msb.build(request.get_json(force=True, silent=True) or {}))
    except Exception as e:
        return jsonify({"error": str(e)[:200]})


@app.route("/api/engine")
def api_engine():
    # strategy-engine meta-backtest + current allocation posture
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "marleg_engine.json")) as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": "run: python marleg_engine.py", "detail": str(e)[:80]})


@app.route("/api/india_rules")
def api_india_rules():
    import marleg_india_rules
    return jsonify(cached("india_rules", marleg_india_rules.get_rules, 21600))


@app.route("/api/gatebot")
def api_gatebot():
    # GateBot paper book + the horizon-grid backtest behind its config
    out = {}
    for key, fn in [("book", "marleg_gatebot_book.json"), ("backtest", "marleg_gate_backtest.json")]:
        try:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), fn)) as f:
                out[key] = json.load(f)
        except Exception:
            out[key] = None
    return jsonify(out)


@app.route("/api/robust_bt")
def api_robust_bt():
    # serve the precomputed honest scorecard (instant); recompute on demand if missing
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "marleg_robust_bt.json")
    try:
        with open(p) as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify(cached("robust_bt", marleg_robust_bt.run, 21600))


@app.route("/api/strategies")
def api_strategies():
    return jsonify(cached("strategies_catalog", marleg_strategies.catalog, 1800))   # playbooks + cached backtest stats

@app.route("/api/strategies/paper")
def api_strategies_paper():
    return jsonify(marleg_strategies.paper_book())                                  # live-marked paper book

@app.route("/api/strategies/try", methods=["POST"])
def api_strategies_try():
    d = request.get_json(force=True, silent=True) or {}
    return jsonify(marleg_strategies.paper_trade(d.get("id", ""), d.get("ticker", "")))   # evaluate + paper-trade if it fires

@app.route("/api/strategies/<sid>")
def api_strategy_detail(sid):
    return jsonify(marleg_strategies.detail(sid))                                   # full playbook

@app.route("/api/autotrader")
def api_autotrader():
    import glob
    order = ["conservative", "balanced", "aggressive", "adaptive", "sid"]
    profs = {}
    for f in glob.glob(os.path.join(HERE, "marleg_at_*.json")):
        try:
            name = os.path.basename(f)[len("marleg_at_"):-len(".json")]
            with open(f, encoding="utf-8") as fh:
                profs[name] = json.load(fh)          # live books written by the bot (separate process)
        except Exception:
            pass
    if not profs:
        return jsonify({"error": "overnight auto-trader not started yet — run: python marleg_autotrader.py"})
    return jsonify({"mode": "overnight", "profiles": {k: profs[k] for k in order if k in profs}})

@app.route("/api/signal_quality")
def api_signal_quality():
    return jsonify(cached("signal_quality", marleg_signal_quality.scan, 3600))   # universe fetch -> cache 1h

# ----------------------------------------------------------------- paper-book status
@app.route("/api/paper")
def api_paper():
    def do():
        names = ["conservative", "balanced", "aggressive", "adaptive"]
        books, allsyms = {}, set()
        for n in names:
            try:
                b = json.load(open(os.path.join(HERE, f"marleg_paper_{n}.json")))
                books[n] = b
                for p in b.get("positions", []):
                    allsyms.add(p["sym"])
            except Exception:
                pass
        px, ts = {}, None
        if allsyms:
            try:                                              # intraday 1-min = freshest while market is open
                d = yf.download(" ".join(s + ".NS" for s in allsyms), period="1d", interval="1m",
                                progress=False, group_by="ticker", threads=True)
                for s in allsyms:
                    try:
                        c = (d[s + ".NS"]["Close"] if len(allsyms) > 1 else d["Close"]).dropna()
                        if len(c):
                            px[s] = round(float(c.iloc[-1]), 2)
                            ts = str(c.index[-1].tz_convert("Asia/Kolkata")) if c.index.tz is not None else str(c.index[-1])
                    except Exception:
                        pass
            except Exception:
                pass
            miss = [s for s in allsyms if s not in px]          # fallback to daily close (market closed / illiquid)
            if miss:
                try:
                    d2 = yf.download(" ".join(s + ".NS" for s in miss), period="5d", interval="1d",
                                     progress=False, group_by="ticker", threads=True)
                    for s in miss:
                        try:
                            c = (d2[s + ".NS"]["Close"] if len(miss) > 1 else d2["Close"]).dropna()
                            if len(c):
                                px[s] = round(float(c.iloc[-1]), 2)
                                if ts is None:
                                    ts = str(c.index[-1])
                        except Exception:
                            pass
                except Exception:
                    pass
        out = {}
        for n, b in books.items():
            pos, mv, upnl = [], 0.0, 0.0
            for p in b.get("positions", []):
                now = px.get(p["sym"], p["entry"]); v = p["qty"] * now; up = p["qty"] * (now - p["entry"])
                mv += v; upnl += up
                pos.append({"sym": p["sym"], "qty": p["qty"], "entry": p["entry"], "now": now,
                            "stop": p["stop"], "target": p["target"], "tag": p.get("tag"), "held": p.get("held", 0),
                            "mv": round(v), "upnl": round(up),
                            "dist_stop": round((now - p["stop"]) / now * 100, 1) if now else None,
                            "dist_tgt": round((p["target"] - now) / now * 100, 1) if now else None})
            equity = b["cash"] + mv
            out[n] = {"equity": round(equity), "start": b.get("start", 100000),
                      "ret": round((equity / b.get("start", 100000) - 1) * 100, 2), "cash": round(b["cash"]),
                      "upnl": round(upnl), "realized": sum(x.get("pnl", 0) for x in b.get("closed", [])),
                      "open": len(pos), "closed": len(b.get("closed", [])), "regime": b.get("regime"),
                      "cfg": b.get("cfg"), "asof": b.get("asof"), "positions": pos,
                      "recent_closed": b.get("closed", [])[-6:]}
        out["price_ts"] = ts
        return out
    return jsonify(cached("paper", do, 20))

# ----------------------------------------------------------------- static (front-end served from web/)
WEB = os.path.join(HERE, "web")
@app.route("/")
def root(): return send_from_directory(WEB, "marle_g_pod.html")

@app.route("/<path:p>")
def static_file(p):
    # web/ first, then fall back to repo root / docs (sector-map + stray assets) so nothing 404s
    for base in (WEB, HERE, os.path.join(HERE, "docs")):
        if os.path.isfile(os.path.join(base, p)):
            return send_from_directory(base, p)
    return ("not found", 404)

def _gated_cache_stale():
    """True if the gated cache wasn't regenerated today (IST) — i.e. the daily scan didn't run."""
    import datetime as _dt
    p = os.path.join(HERE, "marleg_gated_cache.json")
    if not os.path.exists(p):
        return True
    ist_today = (_dt.datetime.utcnow() + _dt.timedelta(hours=5, minutes=30)).date()
    m_ist = (_dt.datetime.utcfromtimestamp(os.path.getmtime(p)) + _dt.timedelta(hours=5, minutes=30)).date()
    return m_ist < ist_today


def _daily_scan_refresh_loop():
    """Keep the gated + volume lists fresh. If the gated cache is from a prior IST day, re-run the
    EOD pipeline (full volume scan + gated scan) in the background, then bust their caches. Checked
    on startup and every 3h — so the lists never silently freeze because a scheduled task didn't fire.
    Weekdays only (NSE shut Sat/Sun; Monday picks up Friday's close). A lockfile prevents overlap."""
    import datetime as _dt
    import subprocess as _sp
    lock = os.path.join(HERE, "marleg_eod_running.lock")
    while True:
        try:
            ist = _dt.datetime.utcnow() + _dt.timedelta(hours=5, minutes=30)
            recent_lock = os.path.exists(lock) and (time.time() - os.path.getmtime(lock) < 1800)
            if _gated_cache_stale() and ist.weekday() < 5 and not recent_lock:
                print("[daily-refresh] gated/volume cache is stale -> running EOD scans in background...")
                with open(lock, "w") as f:
                    f.write(str(time.time()))
                try:
                    _sp.run([sys.executable, os.path.join(HERE, "marleg_eod.py")], cwd=HERE, timeout=3600)
                finally:
                    try:
                        os.remove(lock)
                    except Exception:
                        pass
                with _LOCK:
                    for k in [k for k in _CACHE if k.startswith("gated") or k.startswith("volume")]:
                        _CACHE.pop(k, None)
                print("[daily-refresh] EOD scans done; gated/volume caches refreshed.")
        except Exception as e:
            print("[daily-refresh] error:", e)
        time.sleep(3 * 3600)


def _gated_hourly_refresh_loop():
    """Full gated scan into a SEPARATE hourly cache (marleg_gated_hourly.json), at most once an
    hour and only during NSE hours. The user asked for the full scan at both cadences; keeping it
    off the daily cache means the daily list's tenure (a day-over-day concept) stays clean."""
    import datetime as _dt, subprocess as _sp, sys as _sys
    last = 0.0
    while True:
        try:
            ist = _dt.datetime.utcnow() + _dt.timedelta(hours=5, minutes=30)
            mkt = ist.weekday() < 5 and 555 <= ist.hour * 60 + ist.minute <= 930   # 09:15..15:30 IST
            if mkt and (time.time() - last) >= 3600:
                print("[hourly-gated] running full gated scan -> marleg_gated_hourly.json ...")
                _sp.run([_sys.executable, os.path.join(HERE, "marleg_gated_scan.py"), "--out", "marleg_gated_hourly.json"],
                        cwd=HERE, timeout=1800)
                last = time.time()
                with _LOCK:
                    _CACHE.pop("gated_hourly", None)
                print("[hourly-gated] done.")
        except Exception as e:
            print("[hourly-gated] error:", e)
        time.sleep(600)   # check every 10 min; fires at most hourly during market hours


if __name__ == "__main__":
    # MARLEG_HOST/MARLEG_PORT let the Pod Suite launcher (and anything else) relocate us
    _host = os.environ.get("MARLEG_HOST", "127.0.0.1")
    _port = int(os.environ.get("MARLEG_PORT", "8777"))
    import sys
    threading.Thread(target=_daily_scan_refresh_loop, daemon=True).start()    # auto-keep lists fresh daily
    threading.Thread(target=_gated_hourly_refresh_loop, daemon=True).start()  # full gated scan hourly (market hours)
    print(f"Marle-G surveillance backend -> http://{_host}:{_port}/")
    app.run(host=_host, port=_port, threaded=True)
