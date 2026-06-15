"""
marleg_smartstop.py — dynamic, noise-resistant, thesis-aware stop (fixes "wicked out of a winner").

Three stacked ideas so a spike-down wiggle doesn't shake you out of a trade that's still working:
  1. CLOSE-based   — only a *close* below the level counts; intraday wicks are ignored
  2. ATR-Chandelier — stop = (highest high, last N bars) − mult×ATR; volatility-scaled, ratchets up only
  3. THESIS-GATE   — stay long while the setup holds (close > 20-EMA, RSI not rolling over);
                     a close below the ATR stop while the thesis is intact = SOFT BREACH (don't knee-jerk),
                     a close below it AND a broken thesis = hard EXIT.

smart_stop(tk, entry=None) returns the levels + a HOLD / SOFT-BREACH / EXIT verdict for the live pod.
Validated separately by marleg_smartstop_study (vs fixed-% and plain trailing).
"""
import numpy as np
import pandas as pd


def _atr(df, n=22):
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def _rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def _bars(tk):
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        df = g.candles(tk.upper(), interval_min=1440, days=120)
        if df is not None and not df.empty:
            return df[["open", "high", "low", "close", "volume"]].dropna()
    except Exception:
        pass
    try:
        import yfinance as yf
        return yf.Ticker(tk.upper() + ".NS").history(period="6mo", interval="1d", auto_adjust=False)\
            .rename(columns=str.lower)[["open", "high", "low", "close", "volume"]].dropna()
    except Exception:
        return None


def smart_stop(tk, entry=None, atr_mult=3.0, lookback=22):
    tk = tk.upper()
    df = _bars(tk)
    if df is None or len(df) < 30:
        return {"tk": tk, "error": "no daily data"}
    c, h, l = df["close"], df["high"], df["low"]
    last = float(c.iloc[-1])
    atr = float(_atr(df, 22).iloc[-1])
    ema20 = float(c.ewm(span=20).mean().iloc[-1])
    rsi = float(_rsi(c).iloc[-1])
    chandelier = float(h.tail(lookback).max() - atr_mult * atr)     # vol-scaled trailing
    swing_low = float(l.tail(10).min())                              # last ~higher-low
    stop = min(last * 0.999, max(chandelier, swing_low))            # noise-resistant floor
    thesis_intact = (last > ema20) and (rsi > 45)
    below = last <= stop

    if not below and thesis_intact:
        grade, verdict = "hold", "🟢 HOLD — above the ATR stop, trend intact (close > 20-EMA, RSI ok)"
    elif not below and not thesis_intact:
        grade, verdict = "watch", "🟡 HOLD / WATCH — stop intact but 20-EMA/RSI weakening; tighten if it loses the EMA on a close"
    elif below and thesis_intact:
        grade, verdict = "soft", "🟡 SOFT BREACH — price slipped under the ATR stop, but the thesis holds (close > 20-EMA, RSI ok). Close-based + gate says don't knee-jerk exit on the wiggle; exit only on a CLOSE below that also breaks the 20-EMA."
    else:
        grade, verdict = "exit", "🔴 EXIT — close below the ATR stop AND the thesis is broken (lost 20-EMA / RSI rolling over)"

    out = {"tk": tk, "asof": str(df.index[-1].date()), "last": round(last, 2),
           "atr": round(atr, 2), "atr_pct": round(atr / last * 100, 2),
           "chandelier_stop": round(chandelier, 2), "swing_low_stop": round(swing_low, 2),
           "recommended_stop": round(stop, 2), "dist_to_stop_pct": round((last / stop - 1) * 100, 1),
           "ema20": round(ema20, 2), "rsi": round(rsi, 0), "thesis_intact": thesis_intact,
           "grade": grade, "verdict": verdict,
           "note": f"close-based · {atr_mult}×ATR Chandelier · trail up only · exit gated on 20-EMA/RSI"}
    if entry:
        entry = float(entry)
        risk = entry - stop
        out["entry"] = round(entry, 2)
        out["r_multiple"] = round((last - entry) / risk, 2) if risk > 0 else None
        out["pnl_pct"] = round((last / entry - 1) * 100, 1)
        if last > entry + (entry - chandelier):                      # up ~1R
            out["breakeven_note"] = "up ~1R+ — raise the stop to at least breakeven (lock the trade risk-free)"
    return out


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "TEJASNET"
    entry = float(sys.argv[2]) if len(sys.argv) > 2 else None
    print(json.dumps(smart_stop(tk, entry), indent=2, default=str))
