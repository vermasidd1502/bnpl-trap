"""
marleg_opt_regime.py — the call-buyer's regime gate: is NOW one of the validated good moments to buy a call?

Backtested on the 5y panel (1.79M stock-days, 10-day forward) — the user's thesis, confirmed:
  • BREAK the 20-day high in LOW realized vol  -> +1.90% / 56% win   (the single best setup; ~2.2x baseline)
  • low vol + up-move (not yet broken)         -> +1.17% / 53%       (favourable; watch for the break)
  • break resistance in HIGH vol               -> +1.25% / 47%       (exhaustion risk — low-vol is what makes breaks pay)
  • NEAR the 20d high but not broken           -> +0.79% / 51%       (no edge — the BREAK is the inflection, not the approach)
  • baseline                                    -> +0.86% / 48%

So the gate: a call is most worth buying when price is BREAKING its 20d high (resistance) while realized vol is
LOW (cheap premium + clean, sustainable move). Same break in high vol = exhaustion. "Near but below" = wait.

regime(und) reads the underlying live (Groww) and classifies PRIME / GOOD / WEAK-BREAK / AT-RESISTANCE / NEUTRAL.
Read-only, decision-support. The %s are cross-sectional (stocks); an index moves less but the logic holds.

  python marleg_opt_regime.py NIFTY
"""
import math
import datetime as dt

import marleg_data as md


def regime(und):
    und = (und or "NIFTY").upper()
    df = md.candles(und, 1440, 70)
    if df is None or len(df) < 30:
        return {"ok": False, "und": und, "error": "short history"}
    c = df["close"].dropna()
    h = df["high"].dropna()
    spot = float(c.iloc[-1])
    resistance = float(h.iloc[-21:-1].max())               # prior 20-day high = the resistance
    dist_pct = (spot / resistance - 1) * 100
    rets = c.pct_change().dropna()
    rv20 = float(rets.iloc[-20:].std() * math.sqrt(252))
    rv_hist = (rets.rolling(20).std() * math.sqrt(252)).dropna()
    rv_med = float(rv_hist.iloc[-120:].median()) if len(rv_hist) >= 30 else rv20
    low_vol = rv20 < rv_med
    mom20 = float(c.iloc[-1] / c.iloc[-21] - 1) * 100 if len(c) > 21 else 0.0
    broke = spot > resistance
    near = (spot >= resistance * 0.985) and not broke

    if broke and low_vol:
        tag, tone = "PRIME", "good"
        stat = ("+1.90% / 56% win (10d) — breaking the 20d high in LOW vol: cheap premium + a clean, sustainable "
                "move. The call-buyer's best validated setup.")
    elif low_vol and mom20 > 0:
        tag, tone = "GOOD", "good"
        stat = ("+1.17% / 53% — low vol + up-move (not yet broken). Favourable; the edge fires when it BREAKS the "
                f"resistance at ₹{round(resistance,1)} ({'+' if dist_pct>=0 else ''}{round(dist_pct,2)}% away).")
    elif broke and not low_vol:
        tag, tone = "WEAK BREAK", "warn"
        stat = ("+1.25% / 47% — breaking resistance but in HIGH vol (exhaustion risk). The low-vol condition is what "
                "makes breaks actually pay; this one's a coin-flip.")
    elif near:
        tag, tone = "AT RESISTANCE", "warn"
        stat = (f"+0.79% / 51% — pressed against the 20d high (₹{round(resistance,1)}) but NOT broken. No edge yet — "
                "the BREAK is the inflection, not the approach. Don't anticipate; wait for the close above.")
    else:
        tag, tone = "NEUTRAL", "normal"
        stat = "baseline (+0.86%) — no resistance-break / low-vol edge active right now."

    return {"ok": True, "und": und, "spot": round(spot, 2), "resistance": round(resistance, 1),
            "dist_to_resistance_pct": round(dist_pct, 2), "broke": broke, "near": near,
            "rv20_pct": round(rv20 * 100, 1), "rv_median_pct": round(rv_med * 100, 1), "low_vol": low_vol,
            "mom20_pct": round(mom20, 2), "regime": tag, "tone": tone, "stat": stat,
            "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
            "note": "Validated regime gate: break the 20d-high resistance in LOW realized vol = the best call-buy "
                    "window (+1.90%/56% over baseline +0.86%). Near-but-below = wait; high-vol break = exhaustion.",
            "caveat": "Decision-support, not advice. %s are cross-sectional (stocks); an index drifts less but the "
                      "low-vol-breakout logic holds. A tilt, not a guarantee."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for u in (sys.argv[1:] or ["NIFTY"]):
        r = regime(u)
        if not r.get("ok"):
            print(f"  {u}: {r.get('error')}"); continue
        print(f"\n  {r['und']} ₹{r['spot']} · resistance ₹{r['resistance']} ({'+' if r['dist_to_resistance_pct']>=0 else ''}{r['dist_to_resistance_pct']}%) · "
              f"RV {r['rv20_pct']}% vs med {r['rv_median_pct']}% ({'LOW' if r['low_vol'] else 'high'}) · 20d {r['mom20_pct']:+}%")
        print(f"  ⮞ {r['regime']} — {r['stat']}")
