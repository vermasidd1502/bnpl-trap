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
 "ICICIBANK":"ICICI Bank","SBIN":"State Bank of India","TATAMOTORS":"Tata Motors","ITC":"ITC",
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
    vwap = float((tp*vol)[-20:].sum()/vol[-20:].sum()); rsi = round(_rsi(close), 1)
    hi52 = float(high[-252:].max()); off52 = round((ltp/hi52-1)*100, 1)
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
            "deliv": None, "C": C, "vcs": vcs, "verdict": verdict, "conv": conv, "vsa": vsa, "driver": driver,
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

@app.route("/api/volume_pod")
def api_volume_pod():
    try:
        with open(os.path.join(HERE, "marleg_volume_cache.json"), encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify({"error": "no volume cache yet — run: python marleg_volume_scan.py", "sectors": []})

@app.route("/api/gated")
def api_gated():
    try:
        with open(os.path.join(HERE, "marleg_gated_cache.json"), encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify({"error": "no gated screen yet — run: python marleg_gated_scan.py", "picks": []})

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
        candles = [{"time": str(idx.date()), "open": round(float(r["Open"]), 2),
                    "high": round(float(r["High"]), 2), "low": round(float(r["Low"]), 2),
                    "close": round(float(r["Close"]), 2), "vol": float(r["Volume"])}
                   for idx, r in df.iterrows()]
        return {"symbol": ticker.upper(), "period": period, "candles": candles}
    return jsonify(cached("candles:" + ticker.upper() + ":" + period, do, 300))

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
    return jsonify(cached(key, lambda: fn(g) or {"error": "request failed"}, ttl))

@app.route("/api/holdings")
def api_holdings():  return _broker(lambda g: g.holdings_data(),  "groww:holdings",  30)

@app.route("/api/positions")
def api_positions(): return _broker(lambda g: g.positions_data(), "groww:positions", 10)

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
        url = "https://news.google.com/rss/search?q=" + urllib.parse.quote(query) + "&hl=en-IN&gl=IN&ceid=IN:en"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        root = ET.fromstring(r.content)
        out = []
        for it in root.findall(".//item")[:n]:
            title = (it.findtext("title") or "").strip()
            src = ""
            se = it.find("source")
            if se is not None and se.text:
                src = se.text.strip()
            elif " - " in title:
                title, src = title.rsplit(" - ", 1)
            out.append({"title": title.strip(), "source": src.strip(), "pub": (it.findtext("pubDate") or "").strip()})
        return out
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
    def do():
        t = yf.Ticker(yftk(tk)); info = t.info or {}
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        if not price: return {"error": "no fundamentals for " + tk}
        eps = info.get("trailingEps"); bvps = info.get("bookValue")
        shares = (info.get("marketCap")/price) if (info.get("marketCap") and price) else None
        graham = (22.5*eps*bvps)**0.5 if (eps and bvps and eps > 0 and bvps > 0) else None
        dcf = None; fcf = info.get("freeCashflow")
        g = max(0.05 if (eps and eps > 0) else 0.0, min((info.get("earningsGrowth") or info.get("revenueGrowth") or 0.08), 0.15))
        if fcf and fcf > 0 and shares:
            r = 0.115; gt = 0.045; f = fcf; pv = 0.0
            for yrn in range(1, 6):
                f *= (1+g); pv += f/((1+r)**yrn)
            pv += (f*(1+gt)/(r-gt))/((1+r)**5)
            dcf = (pv + (info.get("totalCash") or 0) - (info.get("totalDebt") or 0))/shares
        tgt = info.get("targetMeanPrice"); rec = info.get("recommendationKey"); na = info.get("numberOfAnalystOpinions")
        intrinsic = [x for x in [graham, dcf] if x]
        fair = sum(intrinsic)/len(intrinsic) if intrinsic else None
        gap = ((fair/price-1)*100) if fair else None
        verdict = "N/A" if gap is None else "UNDERVALUED" if gap > 15 else "OVERVALUED" if gap < -15 else "FAIRLY VALUED"
        upside = ((tgt/price-1)*100) if tgt else None
        de = info.get("debtToEquity")
        H = {"ROE": _pct(info.get("returnOnEquity")), "ROA": _pct(info.get("returnOnAssets")),
             "Net margin": _pct(info.get("profitMargins")), "Op margin": _pct(info.get("operatingMargins")),
             "Rev growth": _pct(info.get("revenueGrowth")), "EPS growth": _pct(info.get("earningsGrowth")),
             "D/E (x)": round(de/100, 2) if isinstance(de, (int, float)) else None, "Current ratio": info.get("currentRatio"),
             "P/E": info.get("trailingPE"), "Fwd P/E": info.get("forwardPE"), "P/B": info.get("priceToBook"),
             "PEG": info.get("pegRatio"), "EV/EBITDA": info.get("enterpriseToEbitda"), "Div yield %": info.get("dividendYield")}
        roe = info.get("returnOnEquity") or 0; pm = info.get("profitMargins") or 0
        rg = info.get("revenueGrowth") or 0; eg = info.get("earningsGrowth") or 0; cr = info.get("currentRatio") or 0
        sc = 0; mx = 0
        for cond, w in [(roe > 0.15, 20), (pm > 0.10, 15), (rg > 0.08, 15), (eg > 0, 15),
                        (isinstance(de, (int, float)) and de < 100, 15), (cr > 1.2, 10), (bool(fcf and fcf > 0), 10)]:
            mx += w
            if cond: sc += w
        qscore = round(sc/mx*100) if mx else None
        pio = _piotroski(t)
        def s2(x): return format(x, ",.0f") if isinstance(x, (int, float)) else "n/a"
        N = []
        if fair:
            div_note = ""
            if upside is not None and gap is not None:
                if gap < -10 and upside > 10:
                    div_note = (" Note the divergence: intrinsic models (current cash flow / book) screen expensive, yet analysts see ~%.0f%% upside — that gap is the growth premium the market assigns to future earnings a current-FCF DCF can't yet capture." % upside)
                elif gap > 10 and upside < -10:
                    div_note = " Note the divergence: it screens cheap on assets/cash flow but analysts see downside — often a value trap or a deteriorating outlook."
            N.append({"h": "What it's worth", "p":
                ("On the numbers, intrinsic estimates span Graham %s and DCF %s against the market price of Rs %s — on current cash flow and book value the stock screens %s. "
                 "Read these as a range, not a point: Graham is a conservative floor; the DCF assumes ~%d%% cash-flow growth at an 11.5%% discount, so it understates capex-heavy, growth-optionality names. "
                 "Trading multiples: P/E %s, forward P/E %s, P/B %s, EV/EBITDA %s.%s")
                % (("Rs "+s2(graham)) if graham else "n/a", ("Rs "+s2(dcf)) if dcf else "n/a", s2(price), verdict.lower(),
                   int(g*100), _f(info.get("trailingPE")), _f(info.get("forwardPE")), _f(info.get("priceToBook")),
                   _f(info.get("enterpriseToEbitda")), div_note)})
        N.append({"h": "How they're doing", "p":
            ("Return on equity is %s%% and net margin %s%%, on revenue growth of %s%% and EPS growth of %s%%. "
             "Balance sheet: debt/equity %s, current ratio %s. %s Quality score %s/100%s.")
            % (_f(H["ROE"]), _f(H["Net margin"]), _f(H["Rev growth"]), _f(H["EPS growth"]),
               (str(H["D/E (x)"])+"x") if H["D/E (x)"] is not None else "n/a", _f(H["Current ratio"]),
               ("Piotroski F-score %d/%d (accounting health, 9 is best). " % (pio["score"], pio["of"])) if pio else "",
               qscore if qscore is not None else "—",
               (" — financially " + ("strong" if (qscore or 0) >= 70 else "mixed" if (qscore or 0) >= 45 else "weak")) if qscore is not None else "")})
        if tgt:
            N.append({"h": "The Street's 1-year view", "p":
                ("Analyst consensus 1-year target is Rs %s — %s%% %s today's Rs %s, from %s analysts, consensus rating %s. "
                 "Range Rs %s (low) to Rs %s (high). The Street's forward number is a projection; pair it with the volume/technical read for timing.")
                % (s2(tgt), ("%.0f" % abs(upside)) if upside is not None else "—", "above" if (upside or 0) > 0 else "below",
                   s2(price), na or "?", (rec or "n/a").replace("_", " "),
                   s2(info.get("targetLowPrice") or 0), s2(info.get("targetHighPrice") or 0))})
        return {"tk": tk, "name": info.get("shortName") or NAMES.get(tk, tk), "price": round(price, 2),
                "sector": info.get("sector"), "industry": info.get("industry"),
                "graham": round(graham, 1) if graham else None, "dcf": round(dcf, 1) if dcf else None,
                "fair": round(fair, 1) if fair else None, "gap": round(gap, 1) if gap is not None else None,
                "verdict": verdict, "target": round(tgt, 1) if tgt else None, "upside": round(upside, 1) if upside is not None else None,
                "rec": rec, "n_analysts": na, "health": H, "qscore": qscore, "piotroski": pio, "narrative": N}
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

if __name__ == "__main__":
    print("Marle-G surveillance backend -> http://127.0.0.1:8777/")
    app.run(host="127.0.0.1", port=8777, threaded=True)
