"""
Marle-G — INTRADAY TRACKER: FIN 537 Realized Volatility + HAR forecast, with the Bollinger squeeze.

Implements the intraday-volatility measure from FIN 537 (N. Pearson, UIUC) lectures 16-17,
"Intraday Volatility Modelling" (Christoffersen, Elements of Financial Risk Management, Ch.5),
then layers the Bollinger/Keltner squeeze breakout signal on top.

THE MEASURE (FIN 537)
  Realized Variance  RV_t = sum of squared intraday log returns within a day.  Stylized fact #1:
                     RV is a far more precise gauge of daily variance than the daily squared return.
  Sparse sampling    we sample on a 5-min grid, NOT 1-min.  The 1-min "All RV" is biased UP by
                     bid-ask bounce / microstructure noise.  The volatility-signature plot (average
                     RV vs sampling interval s) shows where the bias dies out -> pick the smallest
                     stable s (see signature()).
  Overnight gap      NSE is closed overnight, so the market-open RV is scaled to a full day (method 2):
                     RV_24H = ( ln(open_t / close_{t-1}) )^2  +  RV_open_t.
  HAR forecast       Corsi's Heterogeneous AR captures RV's long memory parsimoniously:
                     ln RV_{t+1} = b0 + bD ln RV_D + bW ln RV_W(5d) + bM ln RV_M(21d) + bR R_t + e
                     OLS in logs (stylized fact #3: log RV ~ normal), with a leverage term bR and a
                     Jensen correction exp(sigma^2/2) when undoing the log for the level forecast.
  Check              standardized returns R_t / sqrt(RV_t) are ~ i.i.d. N(0,1)  (stylized fact #4).

THE SIGNAL (squeeze)
  Bollinger bandwidth contraction + the TTM squeeze (Bollinger Bands inside the Keltner Channels)
  flags volatility COILING -> a range expansion / breakout typically follows; the momentum sign
  gives the likely direction.

  python marleg_intraday.py RELIANCE
  python marleg_intraday.py NIFTY          # ^NSEI index
"""
import os, sys, json, time
import numpy as np, pandas as pd, yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
try:
    BY_SYM = json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json")))["by_symbol"]
except Exception:
    BY_SYM = {}
IST = "Asia/Kolkata"
SESS_START, SESS_END = "09:15", "15:30"
ANN = 252                                   # trading days per year (annualization)
INDEX = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS",
         "NIFTYIT": "^CNXIT", "SENSEX": "^BSESN", "VIX": "^INDIAVIX"}


def _yf(tk):
    tk = tk.upper()
    if tk in INDEX:
        return INDEX[tk]
    return tk if (tk.endswith(".NS") or tk.startswith("^")) else tk + ".NS"


def bars(tk, interval="5m", period="60d"):
    """Intraday OHLCV, localized to IST and filtered to the NSE regular session."""
    df = None
    for _attempt in range(3):                       # yfinance intraday can return transient empties
        try:
            df = yf.download(_yf(tk), interval=interval, period=period, progress=False, auto_adjust=False)
        except Exception:
            df = None
        if df is not None and not df.empty:
            break
        time.sleep(1.5)
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    idx = df.index
    df.index = idx.tz_localize("UTC").tz_convert(IST) if idx.tz is None else idx.tz_convert(IST)
    keep = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[keep].dropna(subset=[c for c in ["Close"] if c in keep])
    t = df.index.strftime("%H:%M")
    return df[(t >= SESS_START) & (t <= SESS_END)]


# --------------------------------------------------- FIN 537: Realized Variance
def daily_rv(df, min_bars=8):
    """RV_open per day = sum of squared intraday log returns (decimal variance). Drops thin days."""
    c = np.log(df["Close"])
    day = df.index.normalize()

    def _rv(x):
        v = x.values
        return float(np.square(np.diff(v)).sum()) if len(v) >= min_bars else np.nan
    rv = c.groupby(day).apply(_rv).dropna()
    rv.index = pd.to_datetime(rv.index)
    return rv[rv > 0]


def overnight_adjusted_rv(df, min_bars=8):
    """RV_24H = (ln(open_t/close_{t-1}))^2 + RV_open_t  (FIN 537 overnight adjustment, method 2)."""
    rv_open = daily_rv(df, min_bars)
    day = df.index.normalize()
    op = df["Open"].groupby(day).first() if "Open" in df else df["Close"].groupby(day).first()
    cl = df["Close"].groupby(day).last()
    op.index = pd.to_datetime(op.index); cl.index = pd.to_datetime(cl.index)
    overnight = (np.log(op / cl.shift(1))) ** 2
    overnight = overnight.reindex(rv_open.index).fillna(0.0)
    rv24 = (rv_open + overnight).dropna()
    return rv24, rv_open, overnight


def daily_returns(df):
    cl = df["Close"].groupby(df.index.normalize()).last()
    cl.index = pd.to_datetime(cl.index)
    return np.log(cl / cl.shift(1))


# --------------------------------------------------- FIN 537: HAR forecast (Corsi, log + leverage)
def har(rv, daily_ret):
    rv = rv.dropna()
    if len(rv) < 30:
        return {"error": f"need >=30 daily RV obs for HAR (have {len(rv)}); extend history via Groww"}
    lrv = np.log(rv)
    D, W, M = lrv, np.log(rv.rolling(5).mean()), np.log(rv.rolling(21).mean())
    R = daily_ret.reindex(rv.index).fillna(0.0)
    y = lrv.shift(-1)
    d = pd.concat([y.rename("y"), D.rename("D"), W.rename("W"), M.rename("M"), R.rename("R")], axis=1).dropna()
    if len(d) < 20:
        return {"error": "insufficient overlap for HAR fit"}
    X = np.column_stack([np.ones(len(d)), d["D"], d["W"], d["M"], d["R"]])
    beta, *_ = np.linalg.lstsq(X, d["y"].values, rcond=None)
    resid = d["y"].values - X @ beta
    k = len(beta)
    sigma2 = float(np.sum(resid ** 2) / max(1, len(d) - k))
    ss_tot = float(np.sum((d["y"].values - d["y"].values.mean()) ** 2))
    r2 = 1 - float(np.sum(resid ** 2)) / ss_tot if ss_tot else 0.0
    xd, xw, xm = np.log(rv.iloc[-1]), np.log(rv.tail(5).mean()), np.log(rv.tail(21).mean())
    xr = float(R.iloc[-1])
    ln_fc = float(beta @ np.array([1, xd, xw, xm, xr]))
    rv_fc = float(np.exp(ln_fc + sigma2 / 2))                       # Jensen correction
    return {"coef": {"const": round(beta[0], 3), "daily": round(beta[1], 3), "weekly": round(beta[2], 3),
                     "monthly": round(beta[3], 3), "leverage": round(beta[4], 4)},
            "r2": round(r2, 3), "n_obs": int(len(d)),
            "rv_forecast": rv_fc,
            "vol_forecast_daily_pct": round(float(np.sqrt(rv_fc)) * 100, 2),
            "vol_forecast_ann_pct": round(float(np.sqrt(rv_fc * ANN)) * 100, 1),
            "persistence": round(float(beta[1] + beta[2] + beta[3]), 3)}


def stylized_facts(rv24, daily_ret):
    z = (daily_ret.reindex(rv24.index) / np.sqrt(rv24)).dropna()
    lrv = np.log(rv24.dropna())
    return {"std_standardized_return": round(float(z.std()), 2) if len(z) > 2 else None,   # ~1 if RV captures vol
            "mean_standardized_return": round(float(z.mean()), 2) if len(z) > 2 else None,
            "logRV_acf1": round(float(lrv.autocorr(1)), 2) if len(lrv) > 3 else None,       # persistence
            "n_days": int(len(rv24))}


# --------------------------------------------------- FIN 537: volatility signature plot
def signature(tk):
    """Average RV (annualized vol %) across sampling intervals over a common 7-day window.
    Downward-sloping at small s => microstructure bias; pick the smallest s where it stabilizes."""
    out = []
    for s in (1, 2, 5, 15, 30):
        try:
            df = bars(tk, interval=f"{s}m", period="7d")
            if df.empty:
                continue
            rv = daily_rv(df, min_bars=max(3, int(30 / s)))
            if len(rv):
                out.append({"interval_min": s, "avg_vol_ann_pct": round(float(np.sqrt(rv.mean() * ANN)) * 100, 1),
                            "days": int(len(rv))})
        except Exception:
            pass
    # recommended s: smallest where avg vol is within 5% of the next-coarser estimate
    rec = 5
    for i in range(len(out) - 1):
        a, b = out[i]["avg_vol_ann_pct"], out[i + 1]["avg_vol_ann_pct"]
        if a and abs(a - b) / a < 0.05:
            rec = out[i]["interval_min"]; break
    return {"curve": out, "recommended_min": rec}


# --------------------------------------------------- the squeeze signal (Bollinger + Keltner TTM)
def squeeze(df, n=20, k=2.0, mult=1.5):
    c, h, l = df["Close"], df["High"], df["Low"]
    mid = c.rolling(n).mean(); sd = c.rolling(n).std(ddof=0)
    up, lo = mid + k * sd, mid - k * sd
    bw = (up - lo) / mid
    ema = c.ewm(span=n, adjust=False).mean()
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr = tr.rolling(n).mean()
    kup, klo = ema + mult * atr, ema - mult * atr
    on = (lo > klo) & (up < kup)                       # Bollinger inside Keltner = squeeze ON
    bars_in = 0
    for v in reversed(on.fillna(False).tolist()):
        if v:
            bars_in += 1
        else:
            break
    fired = bool(len(on) > 2 and (not on.iloc[-1]) and on.iloc[-2])
    bwp = float(bw.rank(pct=True).iloc[-1] * 100) if bw.notna().sum() > 5 else 50.0
    basis = ((h.rolling(n).max() + l.rolling(n).min()) / 2 + c.rolling(n).mean()) / 2
    yv = (c - basis).dropna()
    if len(yv) >= n:
        b = np.polyfit(np.arange(n), yv.values[-n:], 1); mom = float(b[0] * (n - 1) + b[1])
    else:
        mom = 0.0
    direction = "UP" if mom > 0 else "DOWN" if mom < 0 else "flat"
    if bool(on.iloc[-1]):
        verdict = f"SQUEEZE ON — {bars_in} bars coiled (bandwidth {round(bwp)}th pctile). Expansion building; watch the break."
    elif fired:
        verdict = f"SQUEEZE FIRED — bands released, momentum {direction}. The move may be starting."
    elif bwp < 20:
        verdict = f"Tightening — bandwidth {round(bwp)}th pctile (pre-squeeze coil)."
    elif bwp > 80:
        verdict = f"Expanded — bandwidth {round(bwp)}th pctile (already moving / late)."
    else:
        verdict = f"Neutral — bandwidth {round(bwp)}th pctile."
    return {"on": bool(on.iloc[-1]), "bars_in_squeeze": bars_in, "fired": fired,
            "bandwidth_pctile": round(bwp), "momentum": round(mom, 3), "direction": direction,
            "upper": round(float(up.iloc[-1]), 2), "mid": round(float(mid.iloc[-1]), 2),
            "lower": round(float(lo.iloc[-1]), 2), "verdict": verdict}


# --------------------------------------------------- Pearson intraday U-shape (seasonality)
def ushape(df):
    c = np.log(df["Close"]); r2 = c.diff() ** 2
    day = df.index.normalize()
    same = day.values == np.roll(day.values, 1)        # drop the first (overnight) bar per day
    r2 = r2[same]; tod = pd.Index(df.index.strftime("%H:%M"))[same]
    g = pd.DataFrame({"tod": tod, "r2": r2.values}).dropna().groupby("tod")["r2"].mean().sort_index()
    tot = float(g.sum())
    if tot <= 0:
        return []
    return [{"t": t, "share_pct": round(float(v / tot * 100), 2), "vol_bp": round(float(np.sqrt(v)) * 1e4, 1)}
            for t, v in g.items()]


# --------------------------------------------------- live snapshot (frequently pollable)
def live(tk):
    """Fast snapshot for continuous polling: latest price, today's RV-so-far, the squeeze. One 5-day fetch."""
    df = bars(tk, "5m", "5d")
    if df.empty or len(df) < 25:
        return {"error": f"no live data for {tk.upper()}"}
    sq = squeeze(df)
    rv_open = daily_rv(df, min_bars=5)
    today = float(rv_open.iloc[-1]) if len(rv_open) else None
    last = float(df["Close"].iloc[-1])
    rng = (sq["upper"] - sq["lower"]) or 1.0
    return {"tk": tk.upper(), "name": (BY_SYM.get(tk.upper()) or {}).get("name", tk.upper()),
            "asof": str(df.index[-1]), "last": round(last, 2),
            "today_vol_ann_pct": round(float(np.sqrt(today * ANN)) * 100, 1) if today else None,
            "pctb": round((last - sq["lower"]) / rng, 2), "squeeze": sq}


# --------------------------------------------------- orchestration
def analyze(tk):
    tk = tk.upper()
    fine = bars(tk, "5m", "60d")                       # fine grid: today's RV, squeeze, U-shape
    if fine.empty or len(fine) < 60:
        return {"error": f"no/low intraday data for {tk}"}
    coarse = bars(tk, "60m", "730d")                   # long grid: many daily RVs -> stable HAR fit

    rv24_f, rv_open_f, overnight_f = overnight_adjusted_rv(fine, min_bars=10)
    today_rv = float(rv24_f.iloc[-1])
    on_share = round(float(overnight_f.iloc[-1] / today_rv * 100), 1) if today_rv else None

    if not coarse.empty and len(coarse) > 200:
        rv24_l, _, _ = overnight_adjusted_rv(coarse, min_bars=4)
        dret_l = daily_returns(coarse)
        har_res = har(rv24_l, dret_l)
        sf = stylized_facts(rv24_l, dret_l)
        rv_hist = rv24_l
    else:
        har_res = har(rv24_f, daily_returns(fine))
        sf = stylized_facts(rv24_f, daily_returns(fine))
        rv_hist = rv24_f

    sq = squeeze(fine)
    us = ushape(fine)

    # charts: daily realized-vol history (annualized %) + recent intraday price w/ Bollinger bands
    rv_series = [{"d": str(d.date()), "vol": round(float(np.sqrt(v * ANN)) * 100, 1)}
                 for d, v in rv_hist.tail(60).items()]
    c = fine["Close"]; mid = c.rolling(20).mean(); sd = c.rolling(20).std(ddof=0)
    up, lo = mid + 2 * sd, mid - 2 * sd
    tail = fine.tail(100)
    price_series = [{"t": ts.strftime("%m-%d %H:%M"), "c": round(float(c.loc[ts]), 2),
                     "u": round(float(up.loc[ts]), 2) if pd.notna(up.loc[ts]) else None,
                     "l": round(float(lo.loc[ts]), 2) if pd.notna(lo.loc[ts]) else None}
                    for ts in tail.index]

    parts = []
    if not har_res.get("error"):
        parts.append(f"HAR forecasts next-day vol ~{har_res['vol_forecast_daily_pct']}% "
                     f"({har_res['vol_forecast_ann_pct']}% annualized).")
    parts.append(sq["verdict"])
    return {
        "tk": tk, "name": (BY_SYM.get(tk) or {}).get("name", tk), "asof": str(fine.index[-1]),
        "last": round(float(fine["Close"].iloc[-1]), 2), "n_days_rv": int(len(rv_hist)),
        "rv": {"today_vol_pct": round(float(np.sqrt(today_rv)) * 100, 2),
               "today_vol_ann_pct": round(float(np.sqrt(today_rv * ANN)) * 100, 1),
               "overnight_share_pct": on_share, "grid": "5-min (sparse, microstructure-aware)"},
        "har": har_res, "stylized_facts": sf, "squeeze": sq, "ushape": us,
        "rv_series": rv_series, "price_series": price_series,
        "verdict": " ".join(p for p in parts if p),
    }


# =================================================================== LIVE (Groww) — position + double-DD
import groww_client as _gcmod
_GCLIENT = None
NATIVE_INTERVALS = {1, 5, 10, 15, 60}
STD_INTERVALS = [1, 3, 5, 10, 15, 30, 60]


def _client():
    global _GCLIENT
    if _GCLIENT is None:
        _GCLIENT = _gcmod.GrowwClient(); _GCLIENT.token()
    return _GCLIENT


def _resample(df, interval):
    if df is None or df.empty:
        return pd.DataFrame()
    cols = {c.lower(): c for c in df.columns}
    agg = {cols["open"]: "first", cols["high"]: "max", cols["low"]: "min", cols["close"]: "last"}
    if "volume" in cols:
        agg[cols["volume"]] = "sum"
    return df.resample(f"{interval}min", label="left", closed="left").agg(agg).dropna(subset=[cols["close"]])


# NOTE on data source: Groww's historical candle-range endpoint lags a full session (no live
# intraday bars), so the LIVE layer uses yfinance intraday (which carries today's session,
# near-real-time for NSE) and overlays the authoritative Groww live LTP for the headline price.
YF_NATIVE = {1, 2, 5, 15, 30, 60}


def gbars(tk, interval=5, days=None):
    """Today-session intraday at `interval` minutes (yfinance; resampled for 3/10/non-native),
    IST-indexed, session-filtered. Live enough for the intraday-position read."""
    if days is None:
        days = max(1, min(20, int(np.ceil(45 * interval / 375))))   # enough bars for RSI14/EMA21
    if interval in YF_NATIVE:
        per = f"{min(days, 7)}d" if interval <= 2 else f"{days}d"
        df = bars(tk, f"{interval}m", per)
    elif interval == 10:
        df = _resample(bars(tk, "5m", f"{days}d"), 10)
    else:                                                            # 3m and any other custom
        df = _resample(bars(tk, "1m", "7d"), interval)
    df = df.rename(columns={c: c.lower() for c in df.columns}) if (df is not None and not df.empty) else pd.DataFrame()
    rec = _recorder_resampled(tk, interval)                          # zero-lag tick-recorder tail
    if rec is not None and len(rec):
        df = pd.concat([df[~df.index.isin(rec.index)], rec]).sort_index() if len(df) else rec
    return df


def _recorder_resampled(tk, interval):
    """Today's bars from the local tick-recorder (zero-lag), resampled to the interval. None if unrun."""
    try:
        import marleg_tick_recorder as tr
        rows = tr.load_bars(tk)
        if not rows or len(rows) < 2:
            return None
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST)
        df = df.set_index("dt")[["open", "high", "low", "close", "volume"]]
        t = df.index.strftime("%H:%M")
        df = df[(t >= SESS_START) & (t <= SESS_END)]
        return df if interval == 1 else _resample(df, interval)
    except Exception:
        return None


def _rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def _momentum(vwap_dist, ema_state, rsi_now, cons, last3, vol_confirm):
    s = 0.0
    if vwap_dist is not None:
        s += 25 if vwap_dist > 0 else -25
    s += 20 if ema_state == "bull" else -20
    if rsi_now is not None:
        s += max(-40, min(40, (rsi_now - 50) * 0.8))
    s += max(-15, min(15, cons * 4))
    s += 20 if last3 > 0 else -20 if last3 < 0 else 0
    if vol_confirm:
        s += 10
    s = max(-100, min(100, s))
    label = ("🟢 Strong momentum up — accelerating" if s >= 55 else "🟢 Grinding higher" if s >= 20 else
             "⚪ Choppy / balanced — no clear edge" if s > -20 else
             "🔴 Rolling over / weakening" if s > -55 else "🔴 Strong momentum down")
    return {"score": round(s), "label": label}


def _book_for(sym):
    """Blended entry + held qty for sym across the live book (delivery + intraday + MTF), or None."""
    try:
        g = _client(); qty = 0.0; cost = 0.0
        for h in (g.holdings_data() or {}).get("holdings") or []:
            if (h.get("trading_symbol") or "").upper() == sym and (h.get("quantity") or 0):
                q = float(h["quantity"]); qty += q; cost += q * float(h.get("average_price") or 0)
        for p in (g.positions_data() or {}).get("positions") or []:
            if (p.get("trading_symbol") or "").upper() != sym:
                continue
            ncf = float(p.get("net_carry_forward_quantity") or 0); net = float(p.get("quantity") or 0)
            held = ncf if ncf else net
            if held:
                qty += held; cost += held * float(p.get("net_price") or 0)
        return {"qty": round(qty), "entry": round(cost / qty, 2)} if qty > 0 else None
    except Exception:
        return None


def position(tk, interval=5):
    """Live 'where is the stock right now' at the chosen interval + minute-by-minute bar table."""
    tk = tk.upper()
    if interval not in STD_INTERVALS:
        interval = 5
    try:
        import marleg_tick_recorder as _tr; _tr.note_focus(tk)   # so the recorder captures viewed tickers
    except Exception:
        pass
    df = gbars(tk, interval)
    if df.empty or len(df) < 5:
        return {"error": f"no Groww intraday data for {tk} @ {interval}m"}
    today = df.index[-1].normalize()                 # tz-aware (IST), matches the index
    norm = df.index.normalize()
    tdf = df[norm == today]
    prev = df[norm < today]
    if len(tdf) < 1:
        return {"error": f"no bars for today on {tk}"}
    prev_close = float(prev["close"].iloc[-1]) if len(prev) else float(tdf["open"].iloc[0])
    o = float(tdf["open"].iloc[0]); ltp = float(tdf["close"].iloc[-1])
    hi = float(tdf["high"].max()); lo = float(tdf["low"].min())
    live = False
    try:                                              # Groww live LTP is authoritative for the headline
        q = _client().quote_table([tk]).get(tk, {})
        if q.get("price"):
            ltp = float(q["price"]); live = True
        if q.get("prev"):
            prev_close = float(q["prev"])
    except Exception:
        pass
    hi = max(hi, ltp); lo = min(lo, ltp); rng = (hi - lo) or 1e-9
    gap = (o / prev_close - 1) * 100; chg_open = (ltp / o - 1) * 100; chg_day = (ltp / prev_close - 1) * 100

    # VWAP anchored at the open
    tp = (tdf["high"] + tdf["low"] + tdf["close"]) / 3
    cvol = tdf["volume"].cumsum()
    vwap_series = (tp * tdf["volume"]).cumsum() / cvol.replace(0, np.nan)
    vwap = float(vwap_series.iloc[-1]) if cvol.iloc[-1] else None
    vwap_dist = round((ltp / vwap - 1) * 100, 2) if vwap else None

    # indicators on the interval (warm up on full df)
    c = df["close"]
    ema9 = c.ewm(span=9, adjust=False).mean(); ema21 = c.ewm(span=21, adjust=False).mean()
    ema_state = "bull" if ema9.iloc[-1] >= ema21.iloc[-1] else "bear"
    rsi = _rsi(c); rsi_now = round(float(rsi.iloc[-1]), 1) if rsi.notna().any() else None

    # consecutive up/down bars (today)
    seq = [1 if x > 0 else -1 if x < 0 else 0 for x in tdf["close"].diff().dropna()]
    cons = 0
    if seq and seq[-1] != 0:
        for x in reversed(seq):
            if x == seq[-1]:
                cons += 1
            else:
                break
        cons *= seq[-1]
    last3 = float(tdf["close"].iloc[-1] - tdf["close"].iloc[-min(3, len(tdf))])

    # volume read
    vols = tdf["volume"]
    avg_bar_vol = float(vols.iloc[:-1].mean()) if len(vols) > 1 else float(vols.iloc[-1])
    last_vol = float(vols.iloc[-1]); vol_surge = round(last_vol / avg_bar_vol, 2) if avg_bar_vol else None
    up_vol = float(vols[tdf["close"].diff() > 0].sum()); dn_vol = float(vols[tdf["close"].diff() < 0].sum())
    ud_intraday = round(up_vol / dn_vol, 2) if dn_vol else None
    vol_confirm = bool(vol_surge and vol_surge > 1.3 and (tdf["close"].diff().iloc[-1] > 0))

    # ATR + realized vol so far today
    pc = tdf["close"].shift(1)
    tr = pd.concat([tdf["high"] - tdf["low"], (tdf["high"] - pc).abs(), (tdf["low"] - pc).abs()], axis=1).max(axis=1)
    atr = round(float(tr.tail(14).mean()), 2) if len(tr) else None
    rv_today = float(np.square(np.diff(np.log(tdf["close"].values))).sum())
    rvol_today = round(float(np.sqrt(rv_today)) * 100, 2)

    mom = _momentum(vwap_dist, ema_state, rsi_now, cons, last3, vol_confirm)

    # day-range meter context + a concrete day STOP and TARGET (so you have a book level)
    pr = df[norm < today]
    ranges = []
    for _d, gg in pr.groupby(pr.index.normalize()):
        if len(gg):
            ranges.append(float(gg["high"].max() - gg["low"].min()))
    avg_range = float(np.mean(ranges[-10:])) if ranges else (hi - lo)
    day_target = round(o + avg_range, 1)                              # where a trending day typically reaches
    stretch_target = round(o + 1.5 * avg_range, 1)
    target_hit = bool(hi >= day_target)
    day_stop = round((vwap * 0.997) if (vwap and ltp > vwap) else lo * 0.998, 1)   # below VWAP, else below day-low
    book = _book_for(tk)
    levels = {"day_target": day_target, "stretch_target": stretch_target, "target_hit": target_hit,
              "day_stop": day_stop, "avg_daily_range": round(avg_range, 1),
              "to_target_pct": round((day_target / ltp - 1) * 100, 1),
              "to_stop_pct": round((day_stop / ltp - 1) * 100, 1)}
    if book:
        book["pnl_pct"] = round((ltp / book["entry"] - 1) * 100, 2)
        book["pnl_rs"] = round((ltp - book["entry"]) * book["qty"])
        book["entry_in_range"] = round(max(0, min(100, (book["entry"] - lo) / ((hi - lo) or 1e-9) * 100)))

    # minute-by-minute bar table (today)
    rsi_t = _rsi(c).reindex(tdf.index)
    bars_out = []
    base = o
    for ts in tdf.index[-min(120, len(tdf)):]:
        cc = float(tdf["close"].loc[ts])
        bars_out.append({"t": ts.strftime("%H:%M"), "ts": int(ts.timestamp()), "o": round(float(tdf["open"].loc[ts]), 2),
                         "h": round(float(tdf["high"].loc[ts]), 2), "l": round(float(tdf["low"].loc[ts]), 2),
                         "c": round(cc, 2), "v": int(tdf["volume"].loc[ts]),
                         "cum": round((cc / base - 1) * 100, 2),
                         "vwap": round(float(vwap_series.loc[ts]), 2) if pd.notna(vwap_series.loc[ts]) else None,
                         "av": bool(vwap_series.loc[ts] and cc >= vwap_series.loc[ts]),
                         "rsi": round(float(rsi_t.loc[ts]), 1) if pd.notna(rsi_t.loc[ts]) else None})

    return {"tk": tk, "name": (BY_SYM.get(tk) or {}).get("name", tk), "interval": interval,
            "asof": str(tdf.index[-1]), "live": live, "ltp": round(ltp, 2), "open": round(o, 2), "prev_close": round(prev_close, 2),
            "day_high": round(hi, 2), "day_low": round(lo, 2), "gap_pct": round(gap, 2),
            "chg_open_pct": round(chg_open, 2), "chg_day_pct": round(chg_day, 2),
            "pos_in_range": round((ltp - lo) / rng * 100), "vwap": round(vwap, 2) if vwap else None, "vwap_dist": vwap_dist,
            "ema_state": ema_state, "rsi": rsi_now, "consecutive": cons, "atr": atr, "rvol_today_pct": rvol_today,
            "vol_surge": vol_surge, "ud_intraday": ud_intraday, "cum_volume": int(vols.sum()),
            "momentum": mom, "levels": levels, "book": book, "bars": bars_out,
            "verdict": f"{mom['label']} · {('above' if (vwap_dist or 0) >= 0 else 'below')} VWAP ({vwap_dist}%) · "
                       f"RSI {rsi_now} · {round((ltp - lo) / rng * 100)}% up the day's range · EMA {ema_state}"}


def _hist(tk, iv):
    """Enough intraday history per interval for a multi-session backtest (yfinance limits aware)."""
    if iv in (1, 2):
        df = bars(tk, f"{iv}m", "7d")
    elif iv == 3:
        df = _resample(bars(tk, "1m", "7d"), 3)
    elif iv == 10:
        df = _resample(bars(tk, "5m", "60d"), 10)
    elif iv in (5, 15, 30, 60):
        df = bars(tk, f"{iv}m", "60d")
    else:
        df = _resample(bars(tk, "1m", "7d"), iv)
    return df.rename(columns={c: c.lower() for c in df.columns}) if (df is not None and not df.empty) else pd.DataFrame()


def _intraday_bt_long(df, cost_rt):
    """Long-or-flat EMA9/21 cross, reset each session (true intraday), net of round-trip cost %."""
    trades = []
    for _d, g in df.groupby(df.index.normalize()):
        c = g["close"].values
        if len(c) < 12:
            continue
        e9 = pd.Series(c).ewm(span=9, adjust=False).mean().values
        e21 = pd.Series(c).ewm(span=21, adjust=False).mean().values
        long = e9 > e21
        inpos = False; entry = 0.0
        for i in range(9, len(c)):
            if long[i] and not inpos:
                inpos = True; entry = c[i]
            elif not long[i] and inpos:
                trades.append((c[i] / entry - 1) * 100 - cost_rt); inpos = False
        if inpos:                                            # force EOD exit (no overnight)
            trades.append((c[-1] / entry - 1) * 100 - cost_rt)
    if not trades:
        return None
    t = np.array(trades); wins = t[t > 0]; losses = t[t < 0]
    return {"n": len(t), "win_rate": round(float((t > 0).mean()) * 100),
            "avg": round(float(t.mean()), 3), "total": round(float(t.sum()), 2),
            "pf": round(float(wins.sum() / abs(losses.sum())), 2) if len(losses) and losses.sum() else None}


def interval_advisor(tk, intervals=(3, 5, 10, 15, 30), cost_rt=0.12):
    """Which timeframe suits THIS stock? Trendiness (Kaufman efficiency ratio), momentum
    persistence (return autocorrelation), whipsaw rate, and a cost-aware long-only EMA backtest."""
    from itertools import groupby
    tk = tk.upper()
    rows = []
    for iv in intervals:
        df = _hist(tk, iv)
        if df.empty or len(df) < 30:
            continue
        c = df["close"]
        lr = np.log(c).diff().dropna()
        autocorr = round(float(lr.autocorr(1)), 3) if len(lr) > 10 else None
        ers = []
        for _d, g in c.groupby(c.index.normalize()):
            if len(g) < 5:
                continue
            path = float(g.diff().abs().sum())
            ers.append(abs(float(g.iloc[-1] - g.iloc[0])) / path if path else 0.0)
        er = round(float(np.mean(ers)), 3) if ers else None
        er_today = round(float(ers[-1]), 3) if ers else None
        e9 = c.ewm(span=9, adjust=False).mean(); e21 = c.ewm(span=21, adjust=False).mean()
        nday = max(1, len(ers))
        whip = round(int((np.sign(e9 - e21).diff().fillna(0) != 0).sum()) / nday, 1)
        signs = [int(x) for x in np.sign(c.diff().dropna()) if x != 0]
        runs = [len(list(g)) for _, g in groupby(signs)]
        run_len = round(float(np.mean(runs)), 2) if runs else None
        bt = _intraday_bt_long(df, cost_rt)
        bars_day = round(len(c) / nday)
        rows.append({"interval": iv, "sessions": nday, "bars_per_day": bars_day,
                     "efficiency_ratio": er, "er_today": er_today, "autocorr": autocorr,
                     "whipsaws_per_day": whip, "avg_run_len": run_len, "bt": bt})
    if not rows:
        return {"tk": tk, "error": "no intraday history"}

    # rank: net backtest edge first (needs enough trades), efficiency ratio breaks ties
    def key(r):
        b = r["bt"] or {}
        edge = b.get("total", -99) if (b.get("n", 0) >= 8) else -99
        return (edge, r["efficiency_ratio"] or 0)
    best = max(rows, key=key)
    by_iv = {r["interval"]: r for r in rows}
    five, fifteen = by_iv.get(5), by_iv.get(15)
    cmp = None
    if five and fifteen and five["bt"] and fifteen["bt"]:
        win = 5 if five["bt"]["total"] >= fifteen["bt"]["total"] else 15
        cmp = (f"5m vs 15m: 5m netted {five['bt']['total']}% over {five['bt']['n']} trades "
               f"(win {five['bt']['win_rate']}%, {five['whipsaws_per_day']} whipsaws/day); "
               f"15m netted {fifteen['bt']['total']}% over {fifteen['bt']['n']} trades "
               f"(win {fifteen['bt']['win_rate']}%, {fifteen['whipsaws_per_day']} whipsaws/day). "
               f"→ {win}m has the better net intraday edge on recent history.")
    today_regime = None
    if best.get("er_today") is not None:
        today_regime = ("trending — finer intervals (5m) are tradeable today" if best["er_today"] >= 0.4
                        else "choppy — lean coarser (15m+) or sit out today")
    bb = best["bt"] or {}
    if bb.get("n", 0) >= 8 and bb.get("total", 0) > 0:
        verdict = (f"Best fit: {best['interval']}m — net +{bb['total']}% over {bb['n']} trades "
                   f"(win {bb['win_rate']}%, PF {bb.get('pf')}), efficiency ratio {best['efficiency_ratio']}.")
    else:
        verdict = ("No interval shows a positive net intraday edge after cost on recent history — "
                   "this stock is better as a positional hold than an intraday trade; if you do trade it, go coarser to cut whipsaw.")
    return {"tk": tk, "name": (BY_SYM.get(tk) or {}).get("name", tk), "cost_rt_pct": cost_rt,
            "rows": rows, "recommended": best["interval"], "compare_5_15": cmp,
            "today_regime": today_regime, "verdict": verdict}


def _rsi_state(v):
    if v is None:
        return ("?", "na")
    if v >= 80:
        return ("extreme overbought", "ob2")
    if v >= 70:
        return ("overbought", "ob")
    if v <= 20:
        return ("extreme oversold", "os2")
    if v <= 30:
        return ("oversold", "os")
    if v >= 55:
        return ("bullish", "mid")
    if v <= 45:
        return ("bearish", "mid")
    return ("neutral", "mid")


def _divergence(price, rsi, win=30):
    """Regular divergence over the last `win` bars: price higher-high w/ RSI lower-high = bearish;
    price lower-low w/ RSI higher-low = bullish. Pragmatic two-half-window heuristic."""
    try:
        p = price.tail(win).values; r = rsi.tail(win).values
        if len(p) < win or np.isnan(r).all():
            return None
        h = win // 2
        p1, p2, r1, r2 = p[:h], p[h:], r[:h], r[h:]
        if p2.max() > p1.max() and r2[int(p2.argmax())] < r1[int(p1.argmax())]:
            return "bearish"
        if p2.min() < p1.min() and r2[int(p2.argmin())] > r1[int(p1.argmin())]:
            return "bullish"
    except Exception:
        pass
    return None


def rsi_engine(tk, interval=5):
    """Active multi-timeframe RSI: 5m/15m/1h/1D zones + divergence + an entry/exit read."""
    tk = tk.upper()
    base = gbars(tk, 5, days=5)                       # 5 sessions so 15m/1h have enough bars to resample
    tfs = []
    def add(label, closes):
        if closes is None or len(closes) < 20:
            return
        r = _rsi(closes)
        if r.dropna().empty:
            return
        v = float(r.iloc[-1]); pv = float(r.iloc[-2]) if len(r) > 1 else v
        state, cls = _rsi_state(v)
        trig = ("↑ crossed above 30 — bullish trigger" if pv < 30 <= v else
                "↓ crossed below 70 — bearish trigger" if pv > 70 >= v else
                "turning down from overbought" if (v >= 68 and v < pv) else
                "turning up from oversold" if (v <= 32 and v > pv) else None)
        tfs.append({"tf": label, "rsi": round(v, 1), "state": state, "cls": cls, "trig": trig})
    if not base.empty:
        add("5m", base["close"])
        r15 = _resample(base, 15); add("15m", r15["close"] if len(r15) else None)
        r60 = _resample(base, 60); add("1h", r60["close"] if len(r60) else None)
    daily = None
    try:
        d = yf.download(_yf(tk), period="6mo", interval="1d", progress=False, auto_adjust=False)
        if isinstance(d.columns, pd.MultiIndex):
            d.columns = d.columns.get_level_values(0)
        daily = d["Close"].dropna()
        add("1D", daily)
    except Exception:
        pass
    # divergence: intraday (15m) + daily
    div_intra = _divergence(r15["close"], _rsi(r15["close"])) if (not base.empty and len(r15) > 31) else None
    div_daily = _divergence(daily, _rsi(daily)) if (daily is not None and len(daily) > 31) else None
    ob = sum(1 for t in tfs if t["cls"] in ("ob", "ob2"))
    os_ = sum(1 for t in tfs if t["cls"] in ("os", "os2"))
    daily_rsi = next((t for t in tfs if t["tf"] == "1D"), None)
    daily_ob = bool(daily_rsi and daily_rsi["cls"] in ("ob", "ob2"))
    daily_os = bool(daily_rsi and daily_rsi["cls"] in ("os", "os2"))
    turning_up = any(t["trig"] and "up" in t["trig"] or (t["trig"] and "above 30" in t["trig"]) for t in tfs)
    turning_dn = any(t["trig"] and ("down" in t["trig"] or "below 70" in t["trig"]) for t in tfs)

    if div_daily == "bearish" or (daily_ob and div_intra == "bearish"):
        verdict = "🔴 EXIT/TRIM — overbought with bearish divergence (price up, momentum not). Book into strength; don't add."
        signal = "exit"
    elif ob >= 2 or daily_ob:
        verdict = "🟠 OVERBOUGHT — extended; trim or trail, wait for a pullback to add. Not an entry."
        signal = "trim"
    elif div_intra == "bullish" or (os_ >= 1 and turning_up):
        verdict = "🟢 OVERSOLD BOUNCE — momentum turning up from oversold; a dip-entry setup (confirm with price > VWAP)."
        signal = "entry"
    elif os_ >= 2 or daily_os:
        verdict = "🟡 OVERSOLD — stretched down; watch for a turn-up trigger before entering (don't catch a falling knife)."
        signal = "watch"
    else:
        verdict = "⚪ NEUTRAL — RSI mid-range, no overbought/oversold edge right now."
        signal = "neutral"
    return {"tk": tk, "timeframes": tfs, "divergence_intraday": div_intra, "divergence_daily": div_daily,
            "signal": signal, "verdict": verdict}


def sparklines(tk):
    """Multi-timeframe 'overall move' mini-series (1M/3M/6M/1Y daily) for the sparkline strip."""
    tk = tk.upper()
    try:
        d = yf.download(_yf(tk), period="1y", interval="1d", progress=False, auto_adjust=False)
        if isinstance(d.columns, pd.MultiIndex):
            d.columns = d.columns.get_level_values(0)
        c = d["Close"].dropna()
    except Exception:
        return {"tk": tk, "error": "no data", "periods": []}
    if len(c) < 25:
        return {"tk": tk, "error": "thin", "periods": []}
    out = []
    for label, n in [("1M", 21), ("3M", 63), ("6M", 126), ("1Y", 252)]:
        s = c.tail(n)
        if len(s) < 2:
            continue
        chg = (float(s.iloc[-1]) / float(s.iloc[0]) - 1) * 100
        step = max(1, len(s) // 40)
        pts = [round(float(x), 2) for x in s.iloc[::step]]
        if pts[-1] != round(float(s.iloc[-1]), 2):
            pts.append(round(float(s.iloc[-1]), 2))
        out.append({"label": label, "chg": round(chg, 1), "pts": pts,
                    "lo": round(float(s.min()), 1), "hi": round(float(s.max()), 1)})
    return {"tk": tk, "periods": out}


def due_diligence(tk, interval=5):
    """Double due diligence for a volume-pod / gated name: gate ✕ fundamentals ✕ live intraday."""
    tk = tk.upper()
    checks = []
    gate = None
    try:
        gd = json.load(open(os.path.join(HERE, "marleg_gated_cache.json"), encoding="utf-8"))
        gate = next((p for p in gd.get("picks", []) if p["s"] == tk), None)
    except Exception:
        pass
    gated_ok = bool(gate)
    if gate:
        checks.append({"k": "Gate · leading sector", "ok": gate.get("sec_rank", 100) <= 40, "detail": f"sector {gate.get('sec_rank')}th pctile"})
        checks.append({"k": "Gate · U/D rising > MA", "ok": gate.get("ud", 0) > gate.get("ud_ma", 0), "detail": f"U/D {gate.get('ud')} vs MA {gate.get('ud_ma')}"})
        checks.append({"k": "Gate · above 0.618 fib", "ok": gate.get("fib", 0) > 0.618, "detail": f"fib pos {gate.get('fib')}"})
    else:
        checks.append({"k": "Gate", "ok": None, "detail": "not gate-qualified in latest scan"})

    f = {}
    try:
        import marleg_fundamentals as mf
        f = mf.fundamentals(tk)
        q = f.get("qscore")
        checks.append({"k": "Fundamentals · quality", "ok": (q or 0) >= 55, "detail": f"quality {q}/100 · {f.get('verdict')}"})
        pio = f.get("piotroski")
        if pio:
            checks.append({"k": "Fundamentals · Piotroski", "ok": pio["score"] >= 6, "detail": f"{pio['score']}/{pio['of']} accounting health"})
    except Exception:
        checks.append({"k": "Fundamentals", "ok": None, "detail": "unavailable"})

    p = position(tk, interval)
    if not p.get("error"):
        checks.append({"k": "Intraday · above VWAP", "ok": (p.get("vwap_dist") or 0) >= 0, "detail": f"{p.get('vwap_dist')}% vs VWAP"})
        checks.append({"k": "Intraday · momentum", "ok": p["momentum"]["score"] > 20, "detail": p["momentum"]["label"]})
        checks.append({"k": "Intraday · trend (EMA)", "ok": p.get("ema_state") == "bull", "detail": f"EMA9/21 {p.get('ema_state')}"})

    chase = None                                          # the DON'T-CHASE guard (room vs ceiling)
    try:
        import marleg_overextension as oe
        chase = oe.chase_check(tk)
        if not chase.get("error"):
            checks.append({"k": "Not over-extended (room to run)", "ok": not chase["at_ceiling"],
                           "detail": f"{chase['structural'].lower()} · {chase['from_52w_high_pct']}% from 52w-high · RSI {chase['rsi']:.0f}"})
    except Exception:
        chase = None

    real = [c for c in checks if c["ok"] is not None]
    passed = sum(1 for c in real if c["ok"]); n = len(real)
    score = round(passed / n * 100) if n else 0
    if gated_ok and score >= 70:
        verdict = "✅ GO — gate, fundamentals and intraday all align (double-confirmed)"
    elif score >= 70:
        verdict = "🟢 STRONG — but not gate-qualified in the latest scan"
    elif score >= 45:
        verdict = "🟡 WATCH — mixed; wait for more confirmation"
    else:
        verdict = "🔴 AVOID — too many red flags"
    # over-extension VETO — a name parked at its 52w high doesn't get a green light, gates or not
    if chase and not chase.get("error"):
        if chase["at_ceiling"]:
            verdict = "🔴 DON'T CHASE — extended near the 52w high; the gates may pass but upside is capped"
        elif chase["hot"] and verdict.startswith(("✅", "🟢")):
            verdict = ("🟡 WAIT FOR A PULLBACK — setup is right but it's hot now" +
                       (f"; target a dip toward ~{chase['pullback_entry']}" if chase.get("pullback_entry") else ""))
    return {"tk": tk, "name": (BY_SYM.get(tk) or {}).get("name", tk), "gated": gated_ok, "interval": interval,
            "score": score, "passed": passed, "n": n, "checks": checks, "verdict": verdict, "chase": chase,
            "fundamentals": {"qscore": f.get("qscore"), "verdict": f.get("verdict"), "piotroski": f.get("piotroski"),
                             "fair": f.get("fair"), "target": f.get("target")},
            "intraday": p if not p.get("error") else None, "asof": p.get("asof")}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    if "--position" in sys.argv:
        print(json.dumps(position(tk, 5), indent=1, default=str)); return
    if "--dd" in sys.argv:
        r = due_diligence(tk, 5)
        print(f"\nDOUBLE DUE DILIGENCE — {r['name']} ({r['tk']})  ·  {r['verdict']}  ({r['passed']}/{r['n']})")
        for c in r["checks"]:
            mark = "✓" if c["ok"] else "·" if c["ok"] is None else "✗"
            print(f"   [{mark}] {c['k']:<26} {c['detail']}")
        return
    if "--signature" in sys.argv:
        print(json.dumps(signature(tk), indent=2)); return
    r = analyze(tk)
    if r.get("error"):
        print(r["error"]); return
    print(f"\nINTRADAY TRACKER — {r['name']} ({r['tk']})   {r['asof']}")
    print(f"  last {r['last']} · RV grid {r['rv']['grid']} · {r['n_days_rv']} daily RVs\n")
    print(f"  REALIZED VOL (today): {r['rv']['today_vol_pct']}%/day  ({r['rv']['today_vol_ann_pct']}% annualized)"
          f"  · overnight share {r['rv']['overnight_share_pct']}%")
    h = r["har"]
    if h.get("error"):
        print(f"  HAR: {h['error']}")
    else:
        print(f"  HAR FORECAST (next day): {h['vol_forecast_daily_pct']}%/day  ({h['vol_forecast_ann_pct']}% ann.)"
              f"  · R2 {h['r2']} · persistence {h['persistence']} · n={h['n_obs']}")
        print(f"      coef: daily {h['coef']['daily']} · weekly {h['coef']['weekly']} · monthly {h['coef']['monthly']} · leverage {h['coef']['leverage']}")
    s = r["stylized_facts"]
    print(f"  STYLIZED FACTS: std(R/sqrt(RV)) {s['std_standardized_return']} (~1 good) · logRV ACF(1) {s['logRV_acf1']} (persistence)")
    sq = r["squeeze"]
    print(f"\n  SQUEEZE: {sq['verdict']}")
    print(f"      Bollinger {sq['lower']} / {sq['mid']} / {sq['upper']} · momentum {sq['momentum']} ({sq['direction']})")
    if r["ushape"]:
        peak = max(r["ushape"], key=lambda x: x["share_pct"]); low = min(r["ushape"], key=lambda x: x["share_pct"])
        print(f"  U-SHAPE: variance peaks at {peak['t']} ({peak['share_pct']}% of day) · lull at {low['t']} ({low['share_pct']}%)")
    print(f"\n  -> {r['verdict']}\n")


if __name__ == "__main__":
    main()
