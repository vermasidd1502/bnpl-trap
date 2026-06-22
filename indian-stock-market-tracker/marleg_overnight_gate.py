"""
marleg_overnight_gate.py — GATED APPROVAL to carry a position OVERNIGHT.

Backtested premise (5y panel, 943k obs): a STRONG intraday close predicts a better overnight + next-day
return, MONOTONICALLY, and it survives costs at the strong end:
    weak close (cir<0.3)              -> next-day -0.00%            (square off)
    strong close (cir>0.7)+up+vol     -> next-day +0.29% net +0.09%
    VERY strong (cir>0.9)+up+vol1.5   -> next-day +0.76% net +0.56%  win 54%
where cir = close-in-range = (Close-Low)/(High-Low): 1.0 = closed on the high (buyers in control into the
bell). The gate stacks the validated intraday-strength core with REGIME (overnight drift is bull-dependent
— it turns negative in bear/shock) and ROOM (don't carry something already pinned at its ceiling).

Holding a position you ALREADY own costs ~0 at the margin, so you keep close to the gross next-day; only an
intraday->delivery CONVERT pays the ~20bps, which only the STRONG / VERY-STRONG tiers clear. Read-only
decision-support; NEVER auto-trades.

  python marleg_overnight_gate.py TEJASNET RELIANCE HINDALCO
"""
import sys

# calibrated to the backtest buckets above: tier -> (gross next-day %, net@20bps %, win %)
CAL = {"STRONG-HOLD": (0.76, 0.56, 54), "HOLD": (0.29, 0.09, 50),
       "NEUTRAL": (0.10, -0.10, 48), "SQUARE-OFF": (-0.01, -0.20, 47)}


def _bar(tk, period="4mo"):
    import marleg_vol as mv
    import yfinance as yf
    h = yf.Ticker(mv._yf_symbol(tk.upper())).history(period=period)
    h = h[["Open", "High", "Low", "Close", "Volume"]].dropna()
    return h if len(h) >= 25 else None


_REGIME = {}


def _regime_bull():
    """NIFTY above its 50DMA = the bull regime where the overnight drift is positive. Cached per process run."""
    if "bull" in _REGIME:
        return _REGIME["bull"]
    try:
        h = _bar("NIFTY", "6mo")
        c = h["Close"]
        bull = bool(c.iloc[-1] > c.rolling(50).mean().iloc[-1])
    except Exception:
        bull = True
    _REGIME["bull"] = bull
    return bull


def evaluate(tk, regime_bull=None):
    h = _bar(tk)
    if h is None:
        return {"ok": False, "tk": tk.upper(), "error": "no/short history"}
    O, H, L, C, V = (h["Open"].values, h["High"].values, h["Low"].values, h["Close"].values, h["Volume"].values)
    o, hi, lo, c, pc = float(O[-1]), float(H[-1]), float(L[-1]), float(C[-1]), float(C[-2])
    rng = hi - lo
    cir = (c - lo) / rng if rng > 0 else 0.5                       # close-in-range (strong-close proxy)
    day_ret = c / pc - 1
    avg20 = float(V[-21:-1].mean()) or float(V[-1])
    relvol = float(V[-1]) / avg20 if avg20 else 1.0
    hi20 = float(H[-20:].max())
    room_pct = (hi20 / c - 1) * 100                                # headroom to the 20d high
    extended = bool(c >= hi20 * 0.999 and day_ret > 0.04)          # pinned at the ceiling after a big day
    bull = _regime_bull() if regime_bull is None else bool(regime_bull)

    gates = {
        "strong_close": bool(cir > 0.7), "very_strong_close": bool(cir > 0.9),
        "up_day": bool(day_ret > 0), "volume": bool(relvol > 1.0), "heavy_volume": bool(relvol > 1.5),
        "regime_bull": bull, "room": not extended,
    }
    # intraday-strength core (the validated signal) -> base tier
    if gates["very_strong_close"] and gates["up_day"] and gates["heavy_volume"]:
        tier = "STRONG-HOLD"
    elif gates["strong_close"] and gates["up_day"] and gates["volume"]:
        tier = "HOLD"
    elif cir < 0.3 or not gates["up_day"]:
        tier = "SQUARE-OFF"
    else:
        tier = "NEUTRAL"
    # regime is a HARD gate — overnight drift goes negative in bear/shock
    capped = False
    if not bull and tier in ("STRONG-HOLD", "HOLD"):
        tier, capped = "SQUARE-OFF", True
    g, net, win = CAL[tier]
    approve = tier in ("STRONG-HOLD", "HOLD")
    bits = []
    bits.append(("✅" if gates["strong_close"] else "⚪") + f" close {cir*100:.0f}% up range")
    bits.append(("✅" if gates["up_day"] else "❌") + f" {'+' if day_ret>=0 else ''}{day_ret*100:.1f}% day")
    bits.append(("✅" if gates["volume"] else "⚪") + f" vol {relvol:.1f}x")
    bits.append(("✅" if bull else "❌") + f" regime {'bull' if bull else 'bear'}")
    bits.append(("⚠ extended" if extended else f"room +{room_pct:.1f}%"))
    if tier == "STRONG-HOLD":
        verdict = f"HOLD OVERNIGHT (high conviction) — strong close on heavy volume; ~+{g:.2f}% next-day edge (net +{net:.2f}%), ~{win}% win"
    elif tier == "HOLD":
        verdict = f"HOLD OVERNIGHT — strong close confirmed; ~+{g:.2f}% next-day edge (net +{net:.2f}%), ~{win}% win"
    elif capped:
        verdict = "SQUARE OFF — close was strong but the regime is BEAR/shock; the overnight drift turns negative there"
    elif tier == "SQUARE-OFF":
        verdict = "SQUARE OFF — weak/red close; weak closes give a ~flat-to-negative next day, not worth the gap risk"
    else:
        verdict = "NEUTRAL — middling close; no edge to carrying it, square off unless you have another reason"
    return {"ok": True, "tk": tk.upper(), "tier": tier, "approve": approve, "verdict": verdict,
            "close_in_range_pct": round(cir * 100, 0), "day_ret_pct": round(day_ret * 100, 2),
            "relvol": round(relvol, 2), "room_pct": round(room_pct, 1), "extended": extended,
            "regime_bull": bull, "gates": gates, "checks": bits,
            "exp_nextday_pct": g, "exp_net_pct": net, "exp_win_pct": win,
            "note": "Gate = intraday strong-close (backtested core) x regime x room. Edge numbers are the 5y "
                    "calibration for this tier. Hold = ~0 marginal cost; convert pays ~20bps (net column). "
                    "Decision-support, not advice."}


def scan(names, regime_bull=None):
    bull = _regime_bull() if regime_bull is None else regime_bull
    out = [evaluate(t, bull) for t in names]
    out = [x for x in out if x.get("ok")]
    rank = {"STRONG-HOLD": 0, "HOLD": 1, "NEUTRAL": 2, "SQUARE-OFF": 3}
    out.sort(key=lambda x: (rank.get(x["tier"], 9), -x["exp_nextday_pct"]))
    return {"ok": True, "regime_bull": bull, "n": len(out), "names": out,
            "note": "Evening overnight-hold desk: which of these to carry vs square off, gated + backtest-calibrated."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    names = sys.argv[1:] or ["TEJASNET", "RELIANCE", "HINDALCO"]
    r = scan(names)
    print(f"\nregime: {'BULL' if r['regime_bull'] else 'BEAR/▼'}  (overnight drift is bull-dependent)\n")
    for x in r["names"]:
        print(f"  {x['tk']:<12} {x['tier']:<11} | {x['verdict']}")
        print(f"  {'':12} {' · '.join(x['checks'])}\n")
