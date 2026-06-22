"""
marleg_poi.py — track POINTS OF INTEREST (where institutional orders sit) with a lifecycle state.

Liquidity (marleg_liquidity) = where STOPS cluster. POIs = where ORDERS were left:
  ORDER BLOCK (OB) — the last opposite candle before a displacement that broke structure. Bullish OB
                     (last down-candle before a strong up-break) = a DEMAND zone; bearish OB = SUPPLY.
  FAIR-VALUE GAP (FVG) — a 3-candle imbalance (gap). Bullish FVG = support, bearish FVG = resistance.
Each POI is "well tracked" by STATE so you see its lifecycle:
  fresh      — untouched since it formed (a valid, unmitigated zone)
  mitigated  — price has tapped INTO the zone (partially used — weaker now)
  broken     — price closed clean THROUGH it (invalidated — ignore)

HONEST: SMC/ICT is popular but our own sweep backtest came out negative net of costs. So POIs are a
STRUCTURAL MAP (where price may react, where to anchor a stop/target), NOT a validated buy/sell signal.

  python marleg_poi.py RELIANCE
"""
import sys
import numpy as np
import pandas as pd


def _hist(tk, period="6mo"):
    import marleg_vol as mv
    import yfinance as yf
    h = yf.Ticker(mv._yf_symbol(tk.upper())).history(period=period)
    return h[["Open", "High", "Low", "Close"]].dropna() if len(h) else None


def pois(tk, period="6mo"):
    h = _hist(tk, period)
    if h is None or len(h) < 40:
        return {"ok": False, "error": f"no history for {tk}"}
    O, H, L, C = h["Open"].values, h["High"].values, h["Low"].values, h["Close"].values
    dts = [d.strftime("%Y-%m-%d") for d in h.index]
    spot, n = float(C[-1]), len(C)
    atr = float(pd.concat([h["High"] - h["Low"], (h["High"] - h["Close"].shift()).abs(),
                           (h["Low"] - h["Close"].shift()).abs()], axis=1).max(axis=1).rolling(14).mean().iloc[-1])
    out = []

    def _state_demand(lo, hi, frm):                         # a zone below price reacts when price taps back DOWN into it
        st = "fresh"
        for j in range(frm + 1, n):
            if L[j] <= hi:
                st = "mitigated" if L[j] > lo else "broken"; break
        return st

    def _state_supply(lo, hi, frm):                         # a zone above price reacts when price taps back UP into it
        st = "fresh"
        for j in range(frm + 1, n):
            if H[j] >= lo:
                st = "mitigated" if H[j] < hi else "broken"; break
        return st

    # ---- Fair-value gaps (3-candle imbalance), last ~70 bars ----
    for i in range(max(2, n - 70), n):
        if L[i] > H[i - 2]:                                 # bullish FVG (gap up) = demand
            lo, hi = float(H[i - 2]), float(L[i])
            out.append({"kind": "bull-FVG", "side": "demand", "lo": round(lo, 1), "hi": round(hi, 1),
                        "i": i, "date": dts[i], "state": _state_demand(lo, hi, i)})
        if H[i] < L[i - 2]:                                 # bearish FVG (gap down) = supply
            lo, hi = float(H[i]), float(L[i - 2])
            out.append({"kind": "bear-FVG", "side": "supply", "lo": round(lo, 1), "hi": round(hi, 1),
                        "i": i, "date": dts[i], "state": _state_supply(lo, hi, i)})

    # ---- Order blocks: last opposite candle before a displacement that breaks structure ----
    for i in range(5, n - 1):
        up_disp = (C[i] - C[i - 1]) > 1.2 * atr and H[i] > max(H[i - 5:i])
        dn_disp = (C[i - 1] - C[i]) > 1.2 * atr and L[i] < min(L[i - 5:i])
        if up_disp:
            for ob in range(i - 1, max(-1, i - 6), -1):
                if C[ob] < O[ob]:                           # last down candle = bullish OB (demand)
                    lo, hi = float(L[ob]), float(H[ob])
                    out.append({"kind": "bull-OB", "side": "demand", "lo": round(lo, 1), "hi": round(hi, 1),
                                "i": ob, "date": dts[ob], "state": _state_demand(lo, hi, i)})
                    break
        if dn_disp:
            for ob in range(i - 1, max(-1, i - 6), -1):
                if C[ob] > O[ob]:                           # last up candle = bearish OB (supply)
                    lo, hi = float(L[ob]), float(H[ob])
                    out.append({"kind": "bear-OB", "side": "supply", "lo": round(lo, 1), "hi": round(hi, 1),
                                "i": ob, "date": dts[ob], "state": _state_supply(lo, hi, i)})
                    break

    # annotate distance, dedup overlapping same-side zones (keep the freshest/most-recent)
    for p in out:
        mid = (p["lo"] + p["hi"]) / 2
        p["dist_pct"] = round((mid / spot - 1) * 100, 2)
    out.sort(key=lambda p: (p["side"], (p["lo"] + p["hi"]) / 2, -p["i"]))
    dd, tol = [], 0.4 * atr
    for p in out:
        if dd and p["side"] == dd[-1]["side"] and abs((p["lo"] + p["hi"]) / 2 - (dd[-1]["lo"] + dd[-1]["hi"]) / 2) <= tol:
            if p["i"] > dd[-1]["i"]:
                dd[-1] = p
        else:
            dd.append(p)

    demand = sorted([p for p in dd if p["side"] == "demand" and p["hi"] < spot], key=lambda x: -x["hi"])
    supply = sorted([p for p in dd if p["side"] == "supply" and p["lo"] > spot], key=lambda x: x["lo"])
    fresh_dem = next((p for p in demand if p["state"] == "fresh"), None)
    fresh_sup = next((p for p in supply if p["state"] == "fresh"), None)
    return {"ok": True, "tk": tk.upper(), "spot": round(spot, 2), "atr": round(atr, 1),
            "demand_below": demand[:5], "supply_above": supply[:5],
            "nearest_fresh_demand": fresh_dem, "nearest_fresh_supply": fresh_sup,
            "note": "POIs = order blocks + FVGs with lifecycle state (fresh/mitigated/broken). Fresh demand "
                    "below = where a long may react; fresh supply above = where a rally may stall. Structural "
                    "context, NOT a validated signal (SMC sweep tested negative net of costs)."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    r = pois(tk)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n  {r['tk']}  spot ₹{r['spot']}  (ATR {r['atr']})")
    print("\n  SUPPLY zones ABOVE (rally may stall / offload):")
    for p in r["supply_above"]:
        print(f"    ₹{p['lo']}–{p['hi']:<9} {p['kind']:<9} {p['dist_pct']:+}% · {p['state']:<9} (since {p['date']})")
    print("  DEMAND zones BELOW (long may react / anchor a stop just under a FRESH one):")
    for p in r["demand_below"]:
        print(f"    ₹{p['lo']}–{p['hi']:<9} {p['kind']:<9} {p['dist_pct']:+}% · {p['state']:<9} (since {p['date']})")
    fd, fs = r["nearest_fresh_demand"], r["nearest_fresh_supply"]
    print(f"\n  nearest FRESH:  demand ↓ {('₹'+str(fd['lo'])+'–'+str(fd['hi'])) if fd else '—'}   supply ↑ {('₹'+str(fs['lo'])+'–'+str(fs['hi'])) if fs else '—'}")
    print(f"\n  {r['note']}")
