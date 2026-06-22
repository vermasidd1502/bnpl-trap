"""
marleg_levels.py — KEY support/resistance levels for a stock, the honest way.

A "level" is where a lot of trading actually happened (so it acts as a magnet / barrier), or where price
repeatedly turned. We find them three ways and merge:
  1. VOLUME PROFILE   — bucket the price range, spread each day's volume across its H-L, sum per bucket.
                        High-Volume Nodes (HVN) = strong S/R; the Point of Control (POC) = the magnet.
  2. SWING PIVOTS     — local highs (resistance) / lows (support) that stood out over a ±k window, by how
                        many times price turned there (more touches = stronger).
  3. STRUCTURE        — prior 52w / 120d high (the fib resistance), recent range high/low, round numbers.

Each level is classed support (below spot) or resistance (above), scored by volume + touches, with the
distance from spot. Read-only, pure compute from daily OHLCV.

  python marleg_levels.py RELIANCE
"""
import sys
import numpy as np
import pandas as pd


def _hist(tk, period="1y"):
    import marleg_vol as mv
    import yfinance as yf
    yfsym = mv._yf_symbol(tk.upper())
    h = yf.Ticker(yfsym).history(period=period)
    return h[["Open", "High", "Low", "Close", "Volume"]].dropna() if len(h) else None


def levels(tk, period="1y", bins=48, pivot_k=5, top=8):
    h = _hist(tk, period)
    if h is None or len(h) < 60:
        return {"ok": False, "error": f"no/short history for {tk}"}
    H, L, C, V = h["High"].values, h["Low"].values, h["Close"].values, h["Volume"].values
    spot = float(C[-1])
    lo, hi = float(L.min()), float(H.max())
    atr = float(pd.concat([h["High"] - h["Low"], (h["High"] - h["Close"].shift()).abs(),
                           (h["Low"] - h["Close"].shift()).abs()], axis=1).max(axis=1).rolling(14).mean().iloc[-1])

    # ---- 1) volume profile: spread each day's volume across the buckets its [low,high] spans ----
    edges = np.linspace(lo, hi, bins + 1)
    centers = (edges[:-1] + edges[1:]) / 2
    vbin = np.zeros(bins)
    for i in range(len(H)):
        a = np.searchsorted(edges, L[i], "right") - 1
        b = np.searchsorted(edges, H[i], "right") - 1
        a, b = max(0, a), min(bins - 1, b)
        if b >= a:
            vbin[a:b + 1] += V[i] / (b - a + 1)
    poc = float(centers[int(vbin.argmax())])                       # point of control = highest-volume price
    vmax = vbin.max() or 1.0
    hvn_idx = [i for i in np.argsort(vbin)[::-1][:top] if vbin[i] > 0.45 * vmax]   # high-volume nodes

    # ---- 2) swing pivots (touches) ----
    piv = []
    k = pivot_k
    for i in range(k, len(H) - k):
        if H[i] == max(H[i - k:i + k + 1]):
            piv.append(("R", float(H[i])))
        if L[i] == min(L[i - k:i + k + 1]):
            piv.append(("S", float(L[i])))

    # ---- 3) structure ----
    struct = [("52w high", float(H.max())), ("52w low", float(L.min())),
              ("120d high", float(H[-120:].max())), ("120d low", float(L[-120:].min())),
              ("20d high", float(H[-20:].max())), ("20d low", float(L[-20:].min()))]
    rnd = round(spot / (10 ** (len(str(int(spot))) - 2))) * (10 ** (len(str(int(spot))) - 2)) if spot > 50 else None

    # ---- merge into clustered levels (within ~0.6 ATR) ----
    cand = []
    for c in (centers[i] for i in hvn_idx):
        cand.append({"price": float(c), "vol": float(vbin[int(np.argmin(abs(centers - c)))] / vmax), "touch": 0, "src": "HVN"})
    for typ, p in piv:
        cand.append({"price": p, "vol": 0.0, "touch": 1, "src": "pivot"})
    for nm, p in struct:
        cand.append({"price": p, "vol": 0.0, "touch": 1, "src": nm})
    if rnd:
        cand.append({"price": float(rnd), "vol": 0.0, "touch": 0, "src": "round"})

    tol = max(0.6 * atr, spot * 0.006)
    cand.sort(key=lambda x: x["price"])
    merged = []
    for c in cand:
        if merged and abs(c["price"] - merged[-1]["price"]) <= tol:
            m = merged[-1]
            m["price"] = (m["price"] * m["w"] + c["price"]) / (m["w"] + 1)
            m["w"] += 1; m["vol"] = max(m["vol"], c["vol"]); m["touch"] += c["touch"]
            m["src"] = m["src"] if m["src"] not in ("pivot",) else c["src"]
        else:
            merged.append({**c, "w": 1})

    out = []
    for m in merged:
        strength = round(min(100, m["vol"] * 55 + m["touch"] * 14 + (m["w"] - 1) * 8))
        out.append({"price": round(m["price"], 1), "type": "resistance" if m["price"] > spot else "support",
                    "strength": strength, "touches": m["touch"], "near_poc": abs(m["price"] - poc) <= tol,
                    "dist_pct": round((m["price"] / spot - 1) * 100, 1), "src": m["src"]})
    out = [x for x in out if x["strength"] >= 18]
    out.sort(key=lambda x: -x["strength"])
    res = sorted([x for x in out if x["type"] == "resistance"], key=lambda x: x["price"])
    sup = sorted([x for x in out if x["type"] == "support"], key=lambda x: -x["price"])
    return {"ok": True, "tk": tk.upper(), "spot": round(spot, 1), "atr": round(atr, 1), "poc": round(poc, 1),
            "nearest_resistance": res[0] if res else None, "nearest_support": sup[0] if sup else None,
            "resistances": res[:6], "supports": sup[:6], "levels": out[:top + 4],
            "note": "Volume-profile HVN + swing pivots + structure, clustered within ~0.6 ATR. POC = the "
                    "highest-traded price (a magnet). Strength = volume-node weight + touches. Decision-support."}


def chart_data(tk, period="1y"):
    """Candles + the S/R levels + a TIME-CODED log of past breaks (close decisively crossing a strong
    level), for an inline chart so you never have to open a separate chart pod."""
    h = _hist(tk, period)
    if h is None or len(h) < 60:
        return {"ok": False, "error": f"no/short history for {tk}"}
    lv = levels(tk, period)
    if not lv.get("ok"):
        return lv
    O, H, L, C, V = h["Open"].values, h["High"].values, h["Low"].values, h["Close"].values, h["Volume"].values
    idx = h.index
    candles = [{"time": idx[i].strftime("%Y-%m-%d"), "open": round(float(O[i]), 2), "high": round(float(H[i]), 2),
                "low": round(float(L[i]), 2), "close": round(float(C[i]), 2), "vol": int(V[i])} for i in range(len(C))]
    atr = lv.get("atr") or float(C[-1]) * 0.02
    strong = [x for x in lv.get("levels", []) if x["strength"] >= 40]      # only break STRONG levels
    events, last = [], {}
    for x in strong:
        Lp = x["price"]
        for i in range(3, len(C)):
            up = C[i - 1] <= Lp and C[i] > Lp + 0.25 * atr and float(min(C[i - 3:i])) < Lp
            dn = C[i - 1] >= Lp and C[i] < Lp - 0.25 * atr and float(max(C[i - 3:i])) > Lp
            if not (up or dn):
                continue
            d = "up" if up else "down"
            key = (round(Lp), d)
            if key in last and i - last[key] < 12:                        # dedup within ~12 sessions
                continue
            last[key] = i
            events.append({"time": idx[i].strftime("%Y-%m-%d"), "price": round(Lp, 1), "dir": d,
                           "label": ("broke ₹{:g} ↑".format(round(Lp)) if up else "lost ₹{:g} ↓".format(round(Lp))),
                           "close": round(float(C[i]), 1)})
    events.sort(key=lambda e: e["time"])

    # ---- overlays: Ichimoku cloud (future-trend) + Bollinger bands (vol envelope / squeeze) ----
    ichi, ichi_state = None, None
    try:
        import marleg_ichimoku as ic
        ichi = ic.compute(h)
        ichi_state = ic.state(h)
    except Exception:
        pass
    boll = None
    try:
        cser = h["Close"]
        mid = cser.rolling(20).mean()
        sd = cser.rolling(20).std(ddof=0)
        up, dn = mid + 2 * sd, mid - 2 * sd

        def _bl(s):
            return [{"time": idx[i].strftime("%Y-%m-%d"), "value": round(float(s.iloc[i]), 2)}
                    for i in range(len(s)) if not np.isnan(s.iloc[i])]
        bwser = ((up - dn) / mid * 100)
        bw = float(bwser.iloc[-1]) if not np.isnan(bwser.iloc[-1]) else None
        recent = bwser.dropna().values[-120:]
        squeeze = bool(bw is not None and len(recent) > 20 and bw <= np.percentile(recent, 20))
        boll = {"upper": _bl(up), "mid": _bl(mid), "lower": _bl(dn),
                "bandwidth_pct": round(bw, 1) if bw is not None else None, "squeeze": squeeze}
    except Exception:
        pass

    return {"ok": True, "tk": tk.upper(), "spot": lv["spot"], "poc": lv["poc"], "atr": round(atr, 1),
            "candles": candles, "levels": lv.get("levels", [])[:10], "events": events[-16:],
            "nearest_resistance": lv.get("nearest_resistance"), "nearest_support": lv.get("nearest_support"),
            "ichimoku": ichi, "ichimoku_state": ichi_state, "bollinger": boll,
            "note": "Time-coded: a 'break' = the close crossing a strong S/R level by >0.25 ATR. "
                    "Ichimoku cloud projects the trend forward; Bollinger shows the vol envelope. Decision-support."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    r = levels(tk)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n{r['tk']}  spot ₹{r['spot']}  ·  POC ₹{r['poc']}  ·  ATR {r['atr']}")
    print(f"  nearest resistance: {r['nearest_resistance']}")
    print(f"  nearest support   : {r['nearest_support']}")
    print("  RESISTANCES:")
    for x in r["resistances"]:
        print(f"    ₹{x['price']:<9} str {x['strength']:>3}  {('+' if x['dist_pct']>=0 else '')}{x['dist_pct']}%  [{x['src']}]{'  ←POC' if x['near_poc'] else ''}")
    print("  SUPPORTS:")
    for x in r["supports"]:
        print(f"    ₹{x['price']:<9} str {x['strength']:>3}  {x['dist_pct']}%  [{x['src']}]{'  ←POC' if x['near_poc'] else ''}")
