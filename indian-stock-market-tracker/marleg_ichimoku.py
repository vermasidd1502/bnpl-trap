"""
marleg_ichimoku.py — Ichimoku Kinko Hyo: the 5 lines + the bull-stack state.

compute(df)  -> chart-ready line series (time as 'YYYY-MM-DD' to match /api/candles), with the
                cloud (Senkou A/B) projected 26 bars FORWARD and Chikou shifted 26 BACK — the real
                Ichimoku geometry.
state(df)    -> the honest "are bulls in control" read: price vs the cloud (the strong filter),
                Tenkan>Kijun, price>Kijun, Chikou>price, and a 0-4 bull-stack count.

This also powers the (optional) Ichimoku reliability backtest — does 'above the cloud' actually
predict in India, vs 'above just the Kijun' (the red line)?
"""
import numpy as np
import pandas as pd
from datetime import timedelta

T, K, B, SHIFT = 9, 26, 52, 26


def _mid(h, l, n):
    return (h.rolling(n).max() + l.rolling(n).min()) / 2


def _lower(df):
    return df.rename(columns={c: c.lower() for c in df.columns})


def _future_bdays(last, n):
    out, d = [], last
    while len(out) < n:
        d = d + timedelta(days=1)
        if d.weekday() < 5:
            out.append(d)
    return out


def compute(df):
    df = _lower(df)
    h, l, c = df["high"], df["low"], df["close"]
    tenkan, kijun = _mid(h, l, T), _mid(h, l, K)
    span_a, span_b = (tenkan + kijun) / 2, _mid(h, l, B)
    dates = [ix.date() for ix in df.index]
    n = len(dates)
    fut = _future_bdays(df.index[-1].date() if hasattr(df.index[-1], "date") else df.index[-1], SHIFT)

    def ser(series, shift=0):
        v = series.values
        out = []
        for i in range(n):
            if np.isnan(v[i]):
                continue
            if shift == 0:
                t = dates[i]
            elif shift < 0:                       # chikou: plotted in the past
                j = i + shift
                if j < 0:
                    continue
                t = dates[j]
            else:                                  # senkou: plotted in the future
                j = i + shift
                t = dates[j] if j < n else fut[j - n]
            out.append({"time": str(t), "value": round(float(v[i]), 2)})
        return out

    return {"tenkan": ser(tenkan), "kijun": ser(kijun),
            "senkou_a": ser(span_a, SHIFT), "senkou_b": ser(span_b, SHIFT),
            "chikou": ser(c, -SHIFT)}


def state(df):
    df = _lower(df)
    h, l, c = df["high"], df["low"], df["close"]
    if len(c) < B + SHIFT + 1:
        return {"error": "need more history for Ichimoku"}
    tenkan, kijun = _mid(h, l, T).iloc[-1], _mid(h, l, K).iloc[-1]
    span_a = ((_mid(h, l, T) + _mid(h, l, K)) / 2)
    span_b = _mid(h, l, B)
    # the cloud at 'now' was computed SHIFT bars ago (cloud is plotted forward)
    sa, sb = float(span_a.iloc[-1 - SHIFT]), float(span_b.iloc[-1 - SHIFT])
    price = float(c.iloc[-1])
    ctop, cbot = max(sa, sb), min(sa, sb)
    above_cloud, below_cloud = bool(price > ctop), bool(price < cbot)
    tk = bool(tenkan > kijun)
    above_kijun = bool(price > kijun)
    chikou_above = bool(price > float(c.iloc[-1 - SHIFT]))     # today's close (the chikou) vs price 26 ago
    stack = int(above_cloud) + int(tk) + int(above_kijun) + int(chikou_above)
    if above_cloud and tk and chikou_above and above_kijun:
        verdict = "🟢 FULL BULL STACK — above cloud + Tenkan>Kijun + Chikou clear (high-conviction)"
    elif above_cloud:
        verdict = "🟢 above the cloud — bullish trend (the strong filter)"
    elif below_cloud:
        verdict = "🔴 below the cloud — bearish; longs are counter-trend here"
    else:
        verdict = "🟡 inside the cloud — no trend / chop (the red-line cross alone isn't control)"
    return {"price": round(price, 2), "tenkan": round(float(tenkan), 2), "kijun": round(float(kijun), 2),
            "cloud_top": round(ctop, 2), "cloud_bottom": round(cbot, 2), "cloud_green": bool(sa > sb),
            "above_cloud": above_cloud, "above_kijun": above_kijun, "tenkan_gt_kijun": bool(tk),
            "chikou_above_price": chikou_above, "bull_stack": stack, "verdict": verdict}


if __name__ == "__main__":
    import sys, json, yfinance as yf
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "TEJASNET"
    df = yf.Ticker(tk + ".NS").history(period="1y", interval="1d", auto_adjust=False)
    print(json.dumps(state(df), indent=2))
