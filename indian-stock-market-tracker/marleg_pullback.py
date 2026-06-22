"""
marleg_pullback.py — the VALIDATED entry: BUY THE DIP IN AN UPTREND (trend-gated).

Backtest truth (this session + the panel): a pullback in an UPTREND (above a rising 50-EMA) is the pod's
best-fit entry — generic 10d-low dip in uptrend = +1.75%/10d, beating breakouts AND beating SMC/POI taps.
Dips in FLAT or DOWNtrends LOSE. So this fires ONLY trend-gated. POI/FVG geometry is decoration; THIS is
the real signal. States:
  NO-TREND   below / under a falling 50-EMA      -> don't buy dips here (they lose)
  DIP-READY  uptrend + pulled back               -> ✅ the validated buy
  EXTENDED   uptrend but stretched (RSI hot)     -> wait for a dip toward the 9/50-EMA
  TRENDING   uptrend, no dip yet                 -> wait for the pullback

  python marleg_pullback.py RELIANCE
"""
import sys


def signal(sym):
    import marleg_vol as mv
    import yfinance as yf
    h = yf.Ticker(mv._yf_symbol(sym.upper())).history(period="1y")[["High", "Low", "Close"]].dropna()
    if len(h) < 60:
        return {"ok": False, "sym": sym.upper(), "error": "short history"}
    C = h["Close"]
    spot = float(C.iloc[-1])
    ema50 = C.ewm(span=50).mean()
    ema9 = C.ewm(span=9).mean()
    slope = float(ema50.iloc[-1] - ema50.iloc[-11])
    trend_up = bool(spot > ema50.iloc[-1] and slope > 0)
    d = C.diff()
    up = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-d).clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = float((100 - 100 / (1 + up / dn)).iloc[-1])
    lo6 = float(h["Low"].iloc[-6:].min())
    e9 = float(ema9.iloc[-1]); e50 = float(ema50.iloc[-1])
    dip = bool(spot <= lo6 * 1.01 or spot < e9 or rsi < 48)
    extended = bool(rsi > 68 or spot > e9 * 1.06)

    if not trend_up:
        state, note = "NO-TREND", "below/under a falling 50-EMA — don't buy dips here (dips outside an uptrend lose)"
    elif dip:
        state, note = "DIP-READY", "✅ pullback in an uptrend — the VALIDATED buy (dip-in-uptrend +1.75%/10d)"
    elif extended:
        state, note = "EXTENDED", f"uptrend but stretched (RSI {rsi:.0f}) — wait for a dip toward the 9/50-EMA (₹{e9:.0f}/₹{e50:.0f})"
    else:
        state, note = "TRENDING", f"uptrend, no dip yet — wait for a pullback toward the 9-EMA ₹{e9:.0f}"
    return {"ok": True, "sym": sym.upper(), "spot": round(spot, 2), "trend_up": trend_up, "dip": dip,
            "extended": extended, "rsi": round(rsi, 1), "ema9": round(e9, 1), "ema50": round(e50, 1),
            "buy_zone": round(e9, 1), "state": state, "note": note}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for s in (sys.argv[1:] or ["RELIANCE"]):
        r = signal(s)
        if r.get("ok"):
            print(f"  {r['sym']:<11} ₹{r['spot']:<9} [{r['state']:<9}] RSI {r['rsi']} · 9EMA ₹{r['ema9']} · 50EMA ₹{r['ema50']} — {r['note']}")
        else:
            print(f"  {s}: {r.get('error')}")
