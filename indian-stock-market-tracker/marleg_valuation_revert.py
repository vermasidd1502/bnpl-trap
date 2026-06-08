"""
Valuation mean-reversion probe — "when a stock is over/undervalued, how often
does price correct, and how fast?"

HONEST CAVEAT: free point-in-time fundamentals don't exist, so this is NOT a
P/E or DCF model. Fair value = a STATISTICAL trend proxy: a rolling long
regression of log(close) on time (window ~250d). Deviation = log(close) - trend;
z = deviation standardised over a rolling window. z >= +2 = overvalued (rich vs
trend), z <= -2 = undervalued (cheap vs trend). This measures reversion-to-trend,
not reversion-to-intrinsic-value.

Episodes: each time |z| crosses 2 we open an episode and track until z returns to
within +-0.5 of 0 (reverted) or 60 trading days elapse (failed). Per episode we
record reverted?, days-to-revert, and correction % (price move back toward fair
value). Per stock we also fit an AR(1) on z (z_t = a + phi*z_{t-1}) and report the
deviation half-life = -ln(2)/ln(phi).

  python marleg_valuation_revert.py                 # sample universe report
  python marleg_valuation_revert.py RELIANCE TCS    # specific tickers

API: valuation_revert(ticker) -> dict  (see bottom of file for shape).
stdlib + pandas + numpy + yfinance only. Tickers are NSE (".NS" appended).
"""
import sys
import numpy as np
import pandas as pd
import yfinance as yf

TREND_WIN = 250    # rolling regression / DMA window for the fair-value trend
Z_WIN = 250        # rolling window to standardise the deviation into a z-score
ENTER = 2.0        # |z| threshold to flag over/undervalued
REVERT = 0.5       # |z| band that counts as "back to fair value"
MAX_DAYS = 60      # trading days allowed for an episode to revert
SAMPLE = ["RELIANCE", "TCS", "HDFCBANK", "OIL", "ITC", "SUNPHARMA", "TATASTEEL", "DLF"]


def _fetch(ticker, period="5y"):
    df = yf.download(ticker + ".NS", period=period, interval="1d",
                     progress=False, auto_adjust=True, threads=False)
    if df is None or df.empty:
        return None
    close = df["Close"]
    if isinstance(close, pd.DataFrame):   # yfinance sometimes returns a 1-col frame
        close = close.iloc[:, 0]
    return close.dropna()


def _trend_logfv(logp):
    """Fair value = rolling OLS of log(close) on time index, fitted value at t.
    Slope*t + intercept evaluated at the window's end => a trend that bends with
    the stock instead of a single static line. Falls back to a rolling mean
    (200-DMA of log price) for the warm-up region."""
    n = len(logp)
    fv = np.full(n, np.nan)
    y = logp.values
    x = np.arange(TREND_WIN, dtype=float)
    xc = x - x.mean()
    denom = float((xc * xc).sum())
    for i in range(TREND_WIN - 1, n):
        seg = y[i - TREND_WIN + 1:i + 1]
        b = float((xc * (seg - seg.mean())).sum()) / denom    # slope
        a = float(seg.mean()) - b * x.mean()                  # intercept
        fv[i] = a + b * x[-1]                                 # fitted at window end
    return pd.Series(fv, index=logp.index)


def _ar1_halflife(z):
    """Fit z_t = a + phi*z_{t-1} (OLS). half-life = -ln(2)/ln(phi) in trading days.
    Returns (phi, half_life) or (None, None) if not mean-reverting (phi outside (0,1))."""
    z = z.dropna()
    if len(z) < 30:
        return None, None
    y = z.values[1:]
    x = z.values[:-1]
    xm, ym = x.mean(), y.mean()
    var = float(((x - xm) ** 2).sum())
    if var == 0:
        return None, None
    phi = float(((x - xm) * (y - ym)).sum()) / var
    if not (0 < phi < 1):
        return round(phi, 3), None    # not stationary-mean-reverting
    hl = -np.log(2) / np.log(phi)
    return round(phi, 3), round(float(hl), 1)


def _episodes(z, price, fv_price, side):
    """Walk z and carve out over/undervalued episodes for one side.
    side = +1 (overvalued, z>=ENTER) or -1 (undervalued, z<=-ENTER).
    Returns list of dicts: reverted, days, correction_pct."""
    zv = z.values
    pv = price.values
    fvv = fv_price.values
    n = len(zv)
    out = []
    i = 0
    while i < n:
        crossed = (side > 0 and zv[i] >= ENTER) or (side < 0 and zv[i] <= -ENTER)
        if not (np.isfinite(zv[i]) and crossed):
            i += 1
            continue
        start = i
        p0, fv0 = pv[start], fvv[start]
        reverted, days = False, None
        j = start + 1
        end = min(n, start + MAX_DAYS + 1)
        while j < end:
            if np.isfinite(zv[j]) and abs(zv[j]) <= REVERT:
                reverted, days = True, j - start
                break
            j += 1
        end_idx = j if reverted else min(end, n) - 1
        p1 = pv[end_idx]
        # correction % = price move TOWARD fair value, signed positive when it corrected
        gap0 = p0 - fv0                         # +ve if rich, -ve if cheap
        move = p1 - p0
        corr_pct = (-move / p0 * 100.0) if side > 0 else (move / p0 * 100.0)
        out.append({"start": str(z.index[start].date()),
                    "reverted": bool(reverted),
                    "days": int(days) if days is not None else None,
                    "correction_pct": round(float(corr_pct), 2),
                    "z0": round(float(zv[start]), 2)})
        # skip past this episode (resume after it returns inside +-ENTER or after window)
        k = end_idx
        while k < n and ((side > 0 and zv[k] >= ENTER) or (side < 0 and zv[k] <= -ENTER)):
            k += 1
        i = max(k, start + 1)
    return out


def _agg(eps):
    if not eps:
        return {"n": 0, "pct_reverted": None, "median_days": None,
                "mean_correction_pct": None}
    rev = [e for e in eps if e["reverted"]]
    days = [e["days"] for e in rev if e["days"] is not None]
    corr = [e["correction_pct"] for e in eps]
    return {"n": len(eps),
            "pct_reverted": round(100.0 * len(rev) / len(eps), 1),
            "median_days": round(float(np.median(days)), 1) if days else None,
            "mean_correction_pct": round(float(np.mean(corr)), 2) if corr else None}


def valuation_revert(ticker):
    """Mean-reversion-to-trend profile for one NSE ticker. See module docstring.
    Returns a dict (or {'ticker':..,'error':..} on failure)."""
    ticker = ticker.upper().replace(".NS", "")
    close = _fetch(ticker)
    if close is None or len(close) < TREND_WIN + Z_WIN // 2:
        return {"ticker": ticker, "error": "insufficient data"}
    logp = np.log(close)
    fv_log = _trend_logfv(logp)
    dev = logp - fv_log                                       # log-deviation from trend
    mu = dev.rolling(Z_WIN, min_periods=Z_WIN // 2).mean()
    sd = dev.rolling(Z_WIN, min_periods=Z_WIN // 2).std()
    z = ((dev - mu) / sd).replace([np.inf, -np.inf], np.nan)
    fv_price = np.exp(fv_log)                                 # fair value in price terms

    valid = z.dropna().index
    z_v, price_v, fvp_v = z.loc[valid], close.loc[valid], fv_price.loc[valid]

    over = _episodes(z_v, price_v, fvp_v, side=+1)
    under = _episodes(z_v, price_v, fvp_v, side=-1)
    phi, half_life = _ar1_halflife(z_v)

    cur_z = float(z_v.iloc[-1]) if len(z_v) else float("nan")
    state = ("OVERVALUED" if cur_z >= ENTER else "UNDERVALUED" if cur_z <= -ENTER
             else "rich" if cur_z >= 1 else "cheap" if cur_z <= -1 else "fair")
    cur_fv = float(fvp_v.iloc[-1])
    cur_px = float(price_v.iloc[-1])

    return {"ticker": ticker,
            "asof": str(valid[-1].date()),
            "n_days": int(len(z_v)),
            "current": {"price": round(cur_px, 2),
                        "fair_value": round(cur_fv, 2),
                        "z": round(cur_z, 2),
                        "premium_pct": round((cur_px / cur_fv - 1) * 100, 1),
                        "state": state},
            "ar1": {"phi": phi, "half_life_days": half_life},
            "overvalued": _agg(over),
            "undervalued": _agg(under),
            "params": {"trend_win": TREND_WIN, "z_win": Z_WIN, "enter": ENTER,
                       "revert_band": REVERT, "max_days": MAX_DAYS}}


def _print_report(r):
    if "error" in r:
        print(f"  {r['ticker']:<12} ERROR: {r['error']}")
        return
    c = r["current"]
    ar = r["ar1"]
    hl = f"{ar['half_life_days']}d" if ar["half_life_days"] else "n/a"
    print(f"\n=== {r['ticker']}  ({r['asof']}, {r['n_days']} z-days) ===")
    print(f"  now: px {c['price']}  fair {c['fair_value']}  "
          f"premium {c['premium_pct']:+.1f}%  z {c['z']:+.2f}  -> {c['state']}")
    print(f"  AR(1) phi={ar['phi']}  deviation half-life={hl}")
    for lbl, key in (("OVERVALUED (z>=+2)", "overvalued"), ("UNDERVALUED (z<=-2)", "undervalued")):
        a = r[key]
        if a["n"] == 0:
            print(f"  {lbl:<22} no episodes")
            continue
        md = f"{a['median_days']}d" if a["median_days"] is not None else "n/a"
        print(f"  {lbl:<22} {a['n']:>2} eps | reverted {a['pct_reverted']}% | "
              f"median {md} | mean correction {a['mean_correction_pct']:+.1f}%")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    universe = args if args else SAMPLE
    print(f"valuation mean-reversion probe — {len(universe)} tickers "
          f"(trend={TREND_WIN}d, z-win={Z_WIN}d, enter=+-{ENTER}, revert<={REVERT}, "
          f"window={MAX_DAYS}d)")
    results = []
    for tk in universe:
        try:
            r = valuation_revert(tk)
        except Exception as e:
            r = {"ticker": tk.upper(), "error": str(e)[:80]}
        results.append(r)
        _print_report(r)

    ok = [r for r in results if "error" not in r]
    if ok:
        hls = [r["ar1"]["half_life_days"] for r in ok if r["ar1"]["half_life_days"]]
        ov = [r["overvalued"]["pct_reverted"] for r in ok if r["overvalued"]["pct_reverted"] is not None]
        un = [r["undervalued"]["pct_reverted"] for r in ok if r["undervalued"]["pct_reverted"] is not None]
        print("\n--- pooled ---")
        if hls:
            print(f"  median deviation half-life: {np.median(hls):.1f}d  (n={len(hls)})")
        if ov:
            print(f"  overvalued  reversion rate: {np.mean(ov):.0f}%  (n={len(ov)})")
        if un:
            print(f"  undervalued reversion rate: {np.mean(un):.0f}%  (n={len(un)})")


if __name__ == "__main__":
    main()
