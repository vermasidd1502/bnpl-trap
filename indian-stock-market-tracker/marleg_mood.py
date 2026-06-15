"""
marleg_mood.py — per-stock LONG/SHORT "mood" meter (−100 weak ↔ +100 strong).

Blends the stock's OWN recent tape into one bias needle:
  SAME-DAY  : close-location-value, the day's % move, the opening gap, all confirmed by RVOL
  YESTERDAY : where it closed in yesterday's range + yesterday's return (follow-through)
  REGIME    : distance from the 5- and 20-DMA (is it in an up- or down-state)

The score formula lives in _frame(df) so the LIVE meter (last bar) and the BACKTEST
(whole history) are guaranteed identical. All inputs are daily-bar-derivable, so the meter
is fully backtestable (marleg_mood_study.py).

IMPORTANT (validated): a NEGATIVE/weak reading is a "avoid / exit longs" state, NOT a short
signal — shorting weak names loses net of cost (see marleg_short_gate_study). The needle is a
LONG timing tool: lean in when it turns strong, step aside when it turns weak.
"""
import numpy as np
import pandas as pd

# component weights (within each block, then across blocks)
W_SAMEDAY, W_YEST, W_REGIME = 0.50, 0.20, 0.30


def _tanh(x, scale):
    return np.tanh(x / scale)


def _frame(df):
    """Given daily OHLCV (columns open/high/low/close/volume), return a DataFrame with the mood
    `score` (−100..+100) plus its sub-scores and raw components, one row per bar."""
    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    rng = (h - l).replace(0, np.nan)
    clv = ((c - l) / rng).clip(0, 1)                       # close location in the day's range
    day_ret = c.pct_change()
    gap = (o - c.shift(1)) / c.shift(1)
    rvol = v / v.rolling(20).mean()
    ma5, ma20 = c.rolling(5).mean(), c.rolling(20).mean()
    dist5, dist20 = c / ma5 - 1, c / ma20 - 1

    s_clv = (clv - 0.5) * 2                                # −1..+1
    s_day = _tanh(day_ret, 0.03)                           # ±3% ~ saturates
    s_gap = _tanh(gap, 0.02)
    s_pclv = (clv.shift(1) - 0.5) * 2
    s_pret = _tanh(day_ret.shift(1), 0.03)
    s_d20 = _tanh(dist20, 0.10)
    s_d5 = _tanh(dist5, 0.05)
    conf = (rvol.clip(0.4, 3.0) / 1.3).clip(0.5, 1.6)     # volume confirmation multiplier (~1 at avg vol)

    sameday = (0.40 * s_clv + 0.40 * s_day + 0.20 * s_gap) * conf
    yest = 0.5 * s_pclv + 0.5 * s_pret
    regime = 0.6 * s_d20 + 0.4 * s_d5
    raw = (W_SAMEDAY * sameday + W_YEST * yest + W_REGIME * regime).clip(-1.2, 1.2)
    score = (raw / 1.2 * 100).round(0)

    return pd.DataFrame({"score": score, "sameday": (sameday * 100).round(0),
                         "yest": (yest * 100).round(0), "regime": (regime * 100).round(0),
                         "clv": clv.round(2), "day_ret": (day_ret * 100).round(2),
                         "gap": (gap * 100).round(2), "rvol": rvol.round(2),
                         "dist5": (dist5 * 100).round(2), "dist20": (dist20 * 100).round(2)},
                        index=df.index)


def verdict(score):
    # U-shaped edge (validated, marleg_mood_study): BOTH extremes pay, the middle is the dead zone.
    # >=50 momentum-long (+0.4% net); <=-50 mean-reversion BOUNCE (contrarian long, survivorship-
    # inflated/high-variance) — NOT a short (shorting lost in every bucket). Middle = no edge.
    if score is None or (isinstance(score, float) and np.isnan(score)):
        return "—"
    if score >= 50:
        return "🟢 STRONG — momentum long"
    if score >= 20:
        return "🟢 long-lean (weak edge)"
    if score > -20:
        return "⚪ neutral — no edge / chop"
    if score > -50:
        return "🟠 weak — no clean edge"
    return "🔵 capitulation — bounce candidate (contrarian, NOT short)"


def _daily(tk):
    import yfinance as yf
    df = yf.Ticker(tk.upper() + ".NS").history(period="6mo", interval="1d", auto_adjust=False)
    if df is None or df.empty:
        return None
    df = df.rename(columns=str.lower)[["open", "high", "low", "close", "volume"]].dropna()
    return df if len(df) > 25 else None


def mood(tk):
    """Live mood for one stock from the latest daily bar (includes today's forming bar in-session)."""
    df = _daily(tk)
    if df is None:
        return {"tk": tk.upper(), "error": "no data"}
    f = _frame(df)
    last = f.iloc[-1]
    sc = None if pd.isna(last["score"]) else int(last["score"])
    return {"tk": tk.upper(), "score": sc, "verdict": verdict(sc),
            "asof": str(df.index[-1].date()),
            "blocks": {"same_day": None if pd.isna(last["sameday"]) else int(last["sameday"]),
                       "yesterday": None if pd.isna(last["yest"]) else int(last["yest"]),
                       "regime": None if pd.isna(last["regime"]) else int(last["regime"])},
            "components": {"close_location": last["clv"], "day_change_pct": last["day_ret"],
                           "gap_pct": last["gap"], "rvol": last["rvol"],
                           "dist_ma5_pct": last["dist5"], "dist_ma20_pct": last["dist20"]}}


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(json.dumps(mood(sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"), indent=2, default=str))
