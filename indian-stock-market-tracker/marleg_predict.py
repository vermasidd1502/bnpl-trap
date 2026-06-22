"""
marleg_predict.py — an HONEST index forecaster for NIFTY & the indices.

Not a crystal ball — short-term index DIRECTION is near-random. What IS real (6y NIFTY backtest) is a small,
regime-conditioned TILT + a volatility CONE:
  BULL    (above a rising 50DMA)   P(up tomorrow) ~57%, low vol — the up-drift is cleanest here
  NEUTRAL                          ~56%
  BEAR    (below a falling 50DMA)  ~49% (direction is a coin-flip) + HIGHER vol
India drifts up regardless (~55% of days, ~57% of weeks). So we report: the regime lean, P(up), the expected
close, and the 1σ/2σ CONE — and we say plainly that the cone is the real story, not the point. Use it for
risk / option-strike selection, not as a directional bet. Read-only, decision-support.

  python marleg_predict.py NIFTY BANKNIFTY
"""
import sys
import math

# NIFTY-calibrated, regime-conditioned (6y, ^NSEI). p_up in %, mean in % (1d = per-day, 5d = cumulative).
CAL = {"BULL":    {"p_up_1d": 57, "mean_1d": 0.07, "p_up_5d": 58, "mean_5d": 0.32, "vol_1d": 0.81},
       "NEUTRAL": {"p_up_1d": 56, "mean_1d": 0.09, "p_up_5d": 56, "mean_5d": 0.39, "vol_1d": 1.02},
       "BEAR":    {"p_up_1d": 49, "mean_1d": 0.03, "p_up_5d": 57, "mean_5d": 0.24, "vol_1d": 1.06}}


def predict(idx="NIFTY"):
    import marleg_vol as mv
    import yfinance as yf
    idx = idx.upper()
    h = yf.Ticker(mv._yf_symbol(idx)).history(period="1y")["Close"].dropna()
    if len(h) < 60:
        return {"ok": False, "idx": idx, "error": "short history"}
    spot = float(h.iloc[-1])
    ema50 = h.ewm(span=50).mean()
    e50 = float(ema50.iloc[-1]); slope = float(ema50.iloc[-1] - ema50.iloc[-11])
    dma200 = float(h.rolling(200).mean().iloc[-1]) if len(h) > 200 else None
    bull = spot > e50 and slope > 0
    bear = spot < e50 and slope < 0
    regime = "BULL" if bull else "BEAR" if bear else "NEUTRAL"
    c = CAL[regime]
    rv = mv.realized_vol(idx, 20)                          # annualized; fall back to the calibrated daily vol
    vol1d = (rv / math.sqrt(252) * 100) if rv else c["vol_1d"]
    vix = mv.india_vix()
    vix = round(float(vix), 2) if vix else None
    mom20 = (spot / float(h.iloc[-21]) - 1) * 100 if len(h) > 21 else None

    def horizon(days, p_up, mean_pct):
        v = vol1d * math.sqrt(days)
        exp = spot * (1 + mean_pct / 100.0)
        return {"p_up": p_up, "expected_close": round(exp, 1), "exp_move_pct": round(mean_pct, 2),
                "sigma_pct": round(v, 2),
                "lo_1sigma": round(spot * (1 - v / 100), 1), "hi_1sigma": round(spot * (1 + v / 100), 1),
                "lo_2sigma": round(spot * (1 - 2 * v / 100), 1), "hi_2sigma": round(spot * (1 + 2 * v / 100), 1)}

    nd = horizon(1, c["p_up_1d"], c["mean_1d"])
    nw = horizon(5, c["p_up_5d"], c["mean_5d"])
    lean = ("mild UP lean" if c["p_up_1d"] >= 56 else "no directional edge (coin-flip)" if c["p_up_1d"] <= 51 else "slight up")
    drivers = [f"regime {regime} ({'+' if spot>=e50 else ''}{(spot/e50-1)*100:.1f}% vs 50DMA, slope {'up' if slope>0 else 'down'})"]
    if dma200:
        drivers.append(f"{'above' if spot>=dma200 else 'below'} 200DMA ({(spot/dma200-1)*100:+.1f}%)")
    if mom20 is not None:
        drivers.append(f"20d momentum {mom20:+.1f}%")
    if vix:
        drivers.append(f"India VIX {vix}")
    verdict = (f"{regime}: ~{c['p_up_1d']}% chance NIFTY-style up tomorrow ({lean}), expected ~{nd['expected_close']}; "
               f"most-likely range (1σ) {nd['lo_1sigma']}–{nd['hi_1sigma']}. Next week (5d): ~{c['p_up_5d']}% up, "
               f"range {nw['lo_1sigma']}–{nw['hi_1sigma']}. The CONE is the real signal — the directional tilt is small.")
    return {"ok": True, "idx": idx, "spot": round(spot, 1), "regime": regime, "lean": lean,
            "dma50": round(e50, 1), "dma200": round(dma200, 1) if dma200 else None, "vol_1d_pct": round(vol1d, 2),
            "india_vix": vix, "mom20_pct": round(mom20, 1) if mom20 is not None else None,
            "next_day": nd, "next_week": nw, "drivers": drivers, "verdict": verdict,
            "note": "Regime-conditioned probability + vol cone (NIFTY-calibrated, this index's own realized vol). "
                    "NOT a point prediction — short-term direction is near-random. Use the cone for risk / "
                    "strike selection, not as a bet. Decision-support, not advice."}


def multi(idxs=None):
    idxs = idxs or ["NIFTY", "BANKNIFTY", "FINNIFTY"]
    return {"ok": True, "indices": [p for p in (predict(i) for i in idxs) if p.get("ok")]}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for i in (sys.argv[1:] or ["NIFTY", "BANKNIFTY"]):
        r = predict(i)
        if not r.get("ok"):
            print(f"  {i}: {r.get('error')}"); continue
        print(f"\n  {r['idx']}  {r['spot']}  [{r['regime']}]  vol {r['vol_1d_pct']}%/day")
        nd, nw = r["next_day"], r["next_week"]
        print(f"    tomorrow:  P(up) {nd['p_up']}% · exp {nd['expected_close']} · 1σ {nd['lo_1sigma']}–{nd['hi_1sigma']} · 2σ {nd['lo_2sigma']}–{nd['hi_2sigma']}")
        print(f"    next week: P(up) {nw['p_up']}% · exp {nw['expected_close']} · 1σ {nw['lo_1sigma']}–{nw['hi_1sigma']}")
        print(f"    drivers: {' · '.join(r['drivers'])}")
        print(f"    ➤ {r['verdict']}")
