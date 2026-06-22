"""
marleg_liquidity.py — map the LIQUIDITY POOLS (where stops cluster) so you can ANTICIPATE a stop-hunt.

Smart-money idea: retail stops sit in predictable places —
  BSL (buy-side liquidity, ABOVE price): above swing highs, prior-day/week highs, round numbers — short stops
                                         + breakout-buy stops. Price spikes UP to grab them ("high sweep").
  SSL (sell-side liquidity, BELOW price): the mirror — long stops + breakdown stops. Price dips to grab them.
  EQUAL highs/lows = the STRONGEST magnets (an obvious cluster everyone can see).
A RESTING pool (untested) is a draw; a SWEPT pool (price already ran it and came back) is spent.

HONEST: we can't see real stop orders (the book is anonymous) — this is a CLUSTERING HYPOTHESIS, not
certainty. And the pod's own ICT sweep backtest was NEGATIVE net of costs. So use this to UNDERSTAND the
landscape (where price may be drawn, where your own stop is exposed), NOT as a buy/sell trigger.

  python marleg_liquidity.py NIFTY
"""
import sys
import numpy as np
import pandas as pd


def _hist(tk, period="6mo"):
    import marleg_vol as mv
    import yfinance as yf
    h = yf.Ticker(mv._yf_symbol(tk.upper())).history(period=period)
    return h[["Open", "High", "Low", "Close"]].dropna() if len(h) else None


def _rounds(spot):
    mag = 10 ** (len(str(int(spot))) - 2) if spot >= 100 else 10
    step = mag * 5 if spot / mag > 20 else mag
    base = round(spot / step) * step
    return sorted({base + i * step for i in (-2, -1, 0, 1, 2)})


def liquidity(tk, period="6mo", k=3):
    h = _hist(tk, period)
    if h is None or len(h) < 40:
        return {"ok": False, "error": f"no history for {tk}"}
    H, L, C = h["High"].values, h["Low"].values, h["Close"].values
    spot, n = float(C[-1]), len(C)
    atr = float(pd.concat([h["High"] - h["Low"], (h["High"] - h["Close"].shift()).abs(),
                           (h["Low"] - h["Close"].shift()).abs()], axis=1).max(axis=1).rolling(14).mean().iloc[-1])
    tol = max(0.004 * spot, 0.5 * atr)

    def cluster(vals):
        out = []
        for p in sorted(vals):
            if out and abs(p - out[-1]["price"]) <= tol:
                m = out[-1]; m["price"] = (m["price"] * m["touch"] + p) / (m["touch"] + 1); m["touch"] += 1
            else:
                out.append({"price": float(p), "touch": 1})
        return out

    sh = cluster([H[i] for i in range(k, n - k) if H[i] == max(H[i - k:i + k + 1])])
    sl = cluster([L[i] for i in range(k, n - k) if L[i] == min(L[i - k:i + k + 1])])
    pdh, pdl, pwh, pwl = float(H[-2]), float(L[-2]), float(H[-6:-1].max()), float(L[-6:-1].min())

    pools = []

    def add(price, side, typ, touch=1):
        if price <= 0:
            return
        swept = False
        for j in range(max(0, n - 8), n):
            if side == "BSL" and H[j] > price and C[j] < price:
                swept = True; break
            if side == "SSL" and L[j] < price and C[j] > price:
                swept = True; break
        mag = min(100, 28 + touch * 22 + (16 if typ.startswith("prior") else 0) + (12 if typ.startswith("equal") else 0))
        pools.append({"price": round(price, 1), "side": side, "type": typ, "touches": touch, "magnetism": mag,
                      "dist_pct": round((price / spot - 1) * 100, 2), "state": "swept" if swept else "resting"})

    for c in sh:
        if c["price"] > spot:
            add(c["price"], "BSL", "equal-highs" if c["touch"] >= 2 else "swing-high", c["touch"])
    for c in sl:
        if c["price"] < spot:
            add(c["price"], "SSL", "equal-lows" if c["touch"] >= 2 else "swing-low", c["touch"])
    if pdh > spot: add(pdh, "BSL", "prior-day-high")
    if pwh > spot: add(pwh, "BSL", "prior-week-high")
    if pdl < spot: add(pdl, "SSL", "prior-day-low")
    if pwl < spot: add(pwl, "SSL", "prior-week-low")
    for r in _rounds(spot):
        if r > spot: add(r, "BSL", "round-number")
        elif r < spot: add(r, "SSL", "round-number")

    # dedup near-equal levels, keep the highest-magnetism label
    pools.sort(key=lambda x: (x["price"], -x["magnetism"]))
    dedup = []
    for p in pools:
        if dedup and abs(p["price"] - dedup[-1]["price"]) <= tol and p["side"] == dedup[-1]["side"]:
            if p["magnetism"] > dedup[-1]["magnetism"]:
                dedup[-1] = p
        else:
            dedup.append(p)

    bsl = sorted([p for p in dedup if p["side"] == "BSL"], key=lambda x: x["price"])
    ssl = sorted([p for p in dedup if p["side"] == "SSL"], key=lambda x: -x["price"])
    draw_up = next((p for p in bsl if p["state"] == "resting"), None)
    draw_dn = next((p for p in ssl if p["state"] == "resting"), None)
    return {"ok": True, "tk": tk.upper(), "spot": round(spot, 2), "atr": round(atr, 1),
            "bsl_above": bsl[:5], "ssl_below": ssl[:5], "draw_up": draw_up, "draw_dn": draw_dn,
            "note": "Liquidity = where stops cluster (clustering hypothesis, not real orders). RESTING = a draw / "
                    "magnet; SWEPT = already taken. Equal highs/lows + prior-day/week levels are the strongest. "
                    "Your OWN stop just under an SSL pool is exposed to a sweep. Context, not a signal."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "NIFTY"
    r = liquidity(tk)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n  {r['tk']}  spot ₹{r['spot']}  (ATR {r['atr']})")
    print("\n  BUY-SIDE liquidity ABOVE (price may spike up to grab, then maybe reverse):")
    for p in r["bsl_above"]:
        print(f"    ₹{p['price']:<9} {p['type']:<14} mag {p['magnetism']:>3} · {p['dist_pct']:+}% · {p['state']}")
    print("  SELL-SIDE liquidity BELOW (price may dip to grab; YOUR long stop is exposed here):")
    for p in r["ssl_below"]:
        print(f"    ₹{p['price']:<9} {p['type']:<14} mag {p['magnetism']:>3} · {p['dist_pct']:+}% · {p['state']}")
    du, dd = r["draw_up"], r["draw_dn"]
    print(f"\n  nearest RESTING draw:  ↑ {('₹'+str(du['price'])+' ('+du['type']+')') if du else '—'}   ↓ {('₹'+str(dd['price'])+' ('+dd['type']+')') if dd else '—'}")
    print(f"\n  {r['note']}")
