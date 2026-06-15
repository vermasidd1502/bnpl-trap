"""
marleg_patterns.py — technical pattern detection (shared by the live pod + the reliability backtest).

signals(df) returns, for each pattern, a vectorized boolean Series (True at the bar where the
pattern completes/triggers) + its conventional bias. The SAME function feeds:
  - the India reliability backtest (marleg_pattern_study) — forward returns at every True bar
  - the live Patterns pod — patterns firing on the most recent bar

Deterministic set (candlesticks + breakouts) is what we backtest rigorously — that's the folklore
worth testing (does a "shooting star" actually mean anything in India?). Heuristic multi-week
chart patterns (cup-handle, H&S, double top/bottom, VCP) are added for the live view separately
(detect_chart), labelled approximate.

Bias is CONVENTIONAL (what the textbook says) — the pod only TRUSTS it if the backtest agrees.
"""
import numpy as np
import pandas as pd

# conventional bias + one-line description (the comprehensive list the UI renders)
META = {
    "Hammer": ("long", "small body, long lower wick, in a downtrend — rejection of lows"),
    "Inverted Hammer": ("long", "long upper wick after a decline — potential bottom"),
    "Hanging Man": ("short", "hammer shape but in an uptrend — distribution warning"),
    "Shooting Star": ("short", "small body, long upper wick after a rise — rejection of highs"),
    "Doji": ("neutral", "open≈close — indecision / potential turn"),
    "Bullish Engulfing": ("long", "green bar fully engulfs prior red — demand"),
    "Bearish Engulfing": ("short", "red bar fully engulfs prior green — supply"),
    "Piercing Line": ("long", "gap down then close back above prior mid-body"),
    "Dark Cloud Cover": ("short", "gap up then close back below prior mid-body"),
    "Bullish Harami": ("long", "small green body inside prior large red — momentum stalling down"),
    "Bearish Harami": ("short", "small red body inside prior large green"),
    "Morning Star": ("long", "3-bar bottom reversal (down, doji, up)"),
    "Evening Star": ("short", "3-bar top reversal (up, doji, down)"),
    "Three White Soldiers": ("long", "3 strong up-closes in a row"),
    "Three Black Crows": ("short", "3 strong down-closes in a row"),
    "Bullish Marubozu": ("long", "full-body green, no wicks — conviction"),
    "Bearish Marubozu": ("short", "full-body red, no wicks"),
    "52w-High Breakout": ("long", "fresh break above the 52-week high"),
    "20d Box Breakout": ("long", "break above a 20-day consolidation high (Darvas)"),
    "20d Breakdown": ("short", "break below a 20-day consolidation low"),
}


def signals(df):
    """dict {pattern: {'sig': boolean Series, 'bias': str}} — vectorized over the daily OHLC frame."""
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    body = c - o
    rng = (h - l).replace(0, np.nan)
    ab = body.abs()
    upper = h - pd.concat([o, c], axis=1).max(axis=1)
    lower = pd.concat([o, c], axis=1).min(axis=1) - l
    sma5 = c.rolling(5).mean()
    up = c.shift(1) > sma5.shift(1)          # prior bar in an up-state
    dn = c.shift(1) < sma5.shift(1)          # prior bar in a down-state
    bull, bear = body > 0, body < 0
    pbull, pbear = body.shift(1) > 0, body.shift(1) < 0
    mid1 = (o.shift(1) + c.shift(1)) / 2

    S = {}
    def add(name, sig):
        S[name] = {"sig": sig.fillna(False).astype(bool), "bias": META[name][0]}

    add("Hammer", dn & (lower >= 2 * ab) & (upper <= ab) & (ab <= 0.4 * rng))
    add("Inverted Hammer", dn & (upper >= 2 * ab) & (lower <= ab) & (ab <= 0.4 * rng))
    add("Hanging Man", up & (lower >= 2 * ab) & (upper <= ab) & (ab <= 0.4 * rng))
    add("Shooting Star", up & (upper >= 2 * ab) & (lower <= ab) & (ab <= 0.4 * rng))
    add("Doji", ab <= 0.1 * rng)
    add("Bullish Engulfing", bull & pbear & (c >= o.shift(1)) & (o <= c.shift(1)))
    add("Bearish Engulfing", bear & pbull & (o >= c.shift(1)) & (c <= o.shift(1)))
    add("Piercing Line", pbear & bull & (o < l.shift(1)) & (c > mid1) & (c < o.shift(1)))
    add("Dark Cloud Cover", pbull & bear & (o > h.shift(1)) & (c < mid1) & (c > o.shift(1)))
    add("Bullish Harami", pbear & (ab.shift(1) > 2 * ab) & (o > c.shift(1)) & (c < o.shift(1)) & bull)
    add("Bearish Harami", pbull & (ab.shift(1) > 2 * ab) & (o < c.shift(1)) & (c > o.shift(1)) & bear)
    add("Morning Star", (body.shift(2) < 0) & (ab.shift(2) > 0.5 * rng.shift(2)) &
        (ab.shift(1) <= 0.3 * rng.shift(1)) & bull & (c > (o.shift(2) + c.shift(2)) / 2))
    add("Evening Star", (body.shift(2) > 0) & (ab.shift(2) > 0.5 * rng.shift(2)) &
        (ab.shift(1) <= 0.3 * rng.shift(1)) & bear & (c < (o.shift(2) + c.shift(2)) / 2))
    add("Three White Soldiers", bull & (body.shift(1) > 0) & (body.shift(2) > 0) &
        (c > c.shift(1)) & (c.shift(1) > c.shift(2)) & (ab > 0.5 * rng))
    add("Three Black Crows", bear & (body.shift(1) < 0) & (body.shift(2) < 0) &
        (c < c.shift(1)) & (c.shift(1) < c.shift(2)) & (ab > 0.5 * rng))
    add("Bullish Marubozu", bull & (ab >= 0.9 * rng))
    add("Bearish Marubozu", bear & (ab >= 0.9 * rng))
    hi252 = c.rolling(252).max()
    add("52w-High Breakout", (c >= hi252) & (c.shift(1) < hi252.shift(1)))
    boxhi = h.rolling(20).max().shift(1)
    boxlo = l.rolling(20).min().shift(1)
    add("20d Box Breakout", (c > boxhi) & (c.shift(1) <= boxhi))
    add("20d Breakdown", (c < boxlo) & (c.shift(1) >= boxlo))
    return S


def detect_last(df, within=3):
    """Patterns firing on the last `within` bars — for the live per-stock view."""
    S = signals(df)
    out = []
    for name, d in S.items():
        tail = d["sig"].iloc[-within:]
        if bool(tail.any()):
            ago = int(np.argmax(tail.values[::-1]))          # bars ago of the latest True (0 = today)
            out.append({"name": name, "bias": d["bias"], "desc": META[name][1], "bars_ago": ago})
    out.sort(key=lambda x: x["bars_ago"])
    return out


if __name__ == "__main__":
    import sys, yfinance as yf
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "TEJASNET"
    df = yf.Ticker(tk + ".NS").history(period="6mo", interval="1d", auto_adjust=False).rename(columns=str.lower)
    for p in detect_last(df, within=2):
        print(f"  {p['name']:<22} {p['bias']:<7} {p['desc']}")
