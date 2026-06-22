"""
marleg_pivots.py — daily PIVOT POINTS (the Groww-style Pivot + 3 supports + 3 resistances) for any
stock / equity / index, computed from the PRIOR completed session's OHLC.

Three textbook methods (Groww shows Classic by default and offers the other two — we return all three):

  CLASSIC (floor-trader)        P = (H+L+C)/3
      R1 = 2P−L   S1 = 2P−H
      R2 = P+(H−L)   S2 = P−(H−L)
      R3 = H+2(P−L)  S3 = L−2(H−P)

  FIBONACCI                     P = (H+L+C)/3,  range R = H−L
      R1/2/3 = P + {.382,.618,1.0}·R     S1/2/3 = P − {.382,.618,1.0}·R

  CAMARILLA                     P = (H+L+C)/3,  range R = H−L
      R1..R4 = C + R·1.1·{1/12,1/6,1/4,1/2}    S1..S4 = C − R·1.1·{…}
      (R3/S3 = reversal band, R4/S4 = breakout band)

Pivots are a SELF-FULFILLING level map (everyone's platform draws the same lines, so they get watched and
defended) — useful as objective intraday support/resistance, NOT a predictive edge on their own. Read-only.

  python marleg_pivots.py RELIANCE
  python marleg_pivots.py NIFTY
"""
import datetime as dt

# index yfinance tickers (<UND>.NS doesn't resolve for indices)
_INDEX_YF = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS",
             "MIDCPNIFTY": "^NSEMDCP50", "NIFTYNXT50": "^NSEI", "SENSEX": "^BSESN"}


def _coerce(df):
    """(basis_date, O,H,L,C, spot) from any daily OHLC DataFrame — uses the last COMPLETED session
    (steps back one bar if the last row is today's still-forming candle); spot = latest close."""
    cols = {c.lower(): c for c in df.columns}
    if not all(k in cols for k in ("open", "high", "low", "close")):
        return None
    d = df.dropna(subset=[cols["high"], cols["low"], cols["close"]])
    if len(d) < 2:
        return None
    today = dt.date.today()
    ld = d.index[-1]
    ld = ld.date() if hasattr(ld, "date") else ld
    idx = -2 if ld == today else -1
    r = d.iloc[idx]
    bd = d.index[idx]
    bd = bd.date() if hasattr(bd, "date") else bd
    spot = float(d[cols["close"]].iloc[-1])
    return (bd, float(r[cols["open"]]), float(r[cols["high"]]), float(r[cols["low"]]), float(r[cols["close"]]), spot)


def _fetch_df(tk, days=240):
    """Daily OHLC, robust to yfinance rate-limits: Groww candles (works for stocks AND indices) →
    canonical 5y panel → yfinance. `days` of history (default 240 — enough for the bias lookback)."""
    tk = tk.upper()
    try:                                            # 1) Groww daily candles (no yfinance dependency)
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        for ex in ("NSE", "BSE"):                    # BSE so SENSEX/BANKEX resolve
            try:
                df = g.candles(tk, interval_min=1440, days=days, segment="CASH", exchange=ex)
                if df is not None and len(df):
                    return df
            except Exception:
                pass
    except Exception:
        pass
    try:                                            # 2) canonical in-memory panel
        import marleg_panel_build as pb
        import pandas as pd
        P = pb.load()
        if tk in P["close"].columns:
            return pd.DataFrame({"open": P["open"][tk], "high": P["high"][tk],
                                 "low": P["low"][tk], "close": P["close"][tk]}).dropna()
    except Exception:
        pass
    try:                                            # 3) yfinance fallback
        import marleg_vol as mv
        import yfinance as yf
        sym = _INDEX_YF.get(tk) or mv._yf_symbol(tk)
        df = yf.Ticker(sym).history(period="1y")
        if df is not None and len(df):
            return df
    except Exception:
        pass
    return None


def _ohlc(tk, df=None):
    df = df if df is not None else _fetch_df(tk)
    if df is None or not len(df):
        return None
    return _coerce(df)


def _classic(H, L, C):
    P = (H + L + C) / 3.0
    rng = H - L
    return {"P": P, "R1": 2 * P - L, "S1": 2 * P - H, "R2": P + rng, "S2": P - rng,
            "R3": H + 2 * (P - L), "S3": L - 2 * (H - P)}


def _fibonacci(H, L, C):
    P = (H + L + C) / 3.0
    rng = H - L
    return {"P": P, "R1": P + 0.382 * rng, "R2": P + 0.618 * rng, "R3": P + 1.0 * rng,
            "S1": P - 0.382 * rng, "S2": P - 0.618 * rng, "S3": P - 1.0 * rng}


def _camarilla(H, L, C):
    P = (H + L + C) / 3.0
    rng = H - L
    k = 1.1
    return {"P": P, "R1": C + rng * k / 12, "R2": C + rng * k / 6, "R3": C + rng * k / 4, "R4": C + rng * k / 2,
            "S1": C - rng * k / 12, "S2": C - rng * k / 6, "S3": C - rng * k / 4, "S4": C - rng * k / 2}


def _round(d):
    return {k: round(v, 2) for k, v in d.items()}


def _position(spot, c):
    """Where the live spot sits within the Classic ladder."""
    ladder = [("S3", c["S3"]), ("S2", c["S2"]), ("S1", c["S1"]), ("P", c["P"]),
              ("R1", c["R1"]), ("R2", c["R2"]), ("R3", c["R3"])]
    above = [x for x in ladder if x[1] <= spot]
    below = [x for x in ladder if x[1] > spot]
    lo = above[-1] if above else None
    hi = below[0] if below else None
    zone = (f"between {lo[0]} (₹{lo[1]:.1f}) and {hi[0]} (₹{hi[1]:.1f})" if lo and hi
            else f"above R3 (₹{c['R3']:.1f}) — extended up" if not hi
            else f"below S3 (₹{c['S3']:.1f}) — extended down")
    bias = "bullish (above pivot)" if spot >= c["P"] else "bearish (below pivot)"
    return {"zone": zone, "bias": bias,
            "nearest_support": {"name": lo[0], "price": round(lo[1], 2), "dist_pct": round((lo[1] / spot - 1) * 100, 2)} if lo else None,
            "nearest_resistance": {"name": hi[0], "price": round(hi[1], 2), "dist_pct": round((hi[1] / spot - 1) * 100, 2)} if hi else None}


def pivots(tk, df=None, spot=None):
    tk = (tk or "").upper().strip()
    d = _ohlc(tk, df)
    if not d:
        return {"ok": False, "tk": tk, "error": f"no daily OHLC for {tk}"}
    basis_date, O, H, L, C, last_close = d
    spot = float(spot) if spot else last_close
    classic = _classic(H, L, C)
    out = {"ok": True, "tk": tk, "basis_date": basis_date.isoformat(),
           "basis": {"open": round(O, 2), "high": round(H, 2), "low": round(L, 2), "close": round(C, 2),
                     "range": round(H - L, 2)},
           "spot": round(spot, 2),
           "methods": {"classic": _round(classic), "fibonacci": _round(_fibonacci(H, L, C)),
                       "camarilla": _round(_camarilla(H, L, C))},
           "position": _position(spot, classic),
           "note": "Pivots use the prior completed session's High/Low/Close — these are the levels for the NEXT "
                   "session (matches what Groww plots). Classic is the default; Fibonacci weights the range by "
                   "fib ratios; Camarilla R3/S3 are reversal bands and R4/S4 breakout bands. A widely-watched "
                   "level map, not a standalone edge."}
    return out


def _runs(states):
    """[(state, run_length), …] for a sequence of booleans."""
    out, cur, n = [], None, 0
    for v in states:
        if v == cur:
            n += 1
        else:
            if cur is not None:
                out.append((cur, n))
            cur, n = v, 1
    if cur is not None:
        out.append((cur, n))
    return out


def bias(tk, lookback=120):
    """BULL/BEAR by PIVOT ACCEPTANCE: how often, and for how long, price holds above the daily pivot (and
    above resistance / below support). Each day is scored vs THAT day's pivot (from the prior session)."""
    import pandas as pd
    tk = (tk or "").upper().strip()
    df = _fetch_df(tk)
    if df is None or len(df) < 40:
        return {"ok": False, "tk": tk, "error": f"no daily data for {tk}"}
    cols = {c.lower(): c for c in df.columns}
    H, L, C = df[cols["high"]].astype(float), df[cols["low"]].astype(float), df[cols["close"]].astype(float)
    Hp, Lp, Cp = H.shift(1), L.shift(1), C.shift(1)
    P = (Hp + Lp + Cp) / 3.0
    rng = Hp - Lp
    R1, S1, R2, S2 = 2 * P - Lp, 2 * P - Hp, P + rng, P - rng
    d = pd.DataFrame({"C": C, "P": P, "R1": R1, "S1": S1, "R2": R2, "S2": S2}).dropna()
    if len(d) < 30:
        return {"ok": False, "tk": tk, "error": "too little history"}
    above = d["C"] > d["P"]
    runs = _runs(list(above))
    above_runs = [n for s, n in runs if s]
    below_runs = [n for s, n in runs if not s]
    cur_state, cur_streak = bool(above.iloc[-1]), runs[-1][1]

    def zone(r):
        if r["C"] >= r["R2"]:
            return ">R2"
        if r["C"] >= r["R1"]:
            return "R1–R2"
        if r["C"] >= r["P"]:
            return "P–R1"
        if r["C"] >= r["S1"]:
            return "S1–P"
        if r["C"] >= r["S2"]:
            return "S2–S1"
        return "<S2"
    win = d.tail(lookback)
    zlabels = ["<S2", "S2–S1", "S1–P", "P–R1", "R1–R2", ">R2"]
    zc = {z: 0 for z in zlabels}
    for _, r in win.iterrows():
        zc[zone(r)] += 1
    nz = len(win)
    zdist = {z: round(zc[z] / nz * 100, 1) for z in zlabels}
    pa = lambda n: round(float(above.tail(n).mean()) * 100, 1)
    pct20, pct60, pctL = pa(20), pa(60), round(float(above.tail(lookback).mean()) * 100, 1)
    avg_above = round(sum(above_runs) / len(above_runs), 1) if above_runs else 0
    avg_below = round(sum(below_runs) / len(below_runs), 1) if below_runs else 0
    above_r1 = round(float((win["C"] >= win["R1"]).mean()) * 100, 1)
    below_s1 = round(float((win["C"] <= win["S1"]).mean()) * 100, 1)
    verdict = ("BULL — price lives above the pivot" if pctL >= 60 else
               "BEAR — price lives below the pivot" if pctL <= 40 else
               "CHOP — straddling the pivot, no acceptance")
    return {"ok": True, "tk": tk, "lookback": int(nz), "spot": round(float(C.iloc[-1]), 2),
            "pct_above_pivot": {"d20": pct20, "d60": pct60, "dL": pctL},
            "current": {"side": "ABOVE" if cur_state else "BELOW", "streak_sessions": int(cur_streak)},
            "persistence": {"avg_run_above": avg_above, "avg_run_below": avg_below,
                            "longest_above": max(above_runs) if above_runs else 0,
                            "longest_below": max(below_runs) if below_runs else 0},
            "pct_above_R1": above_r1, "pct_below_S1": below_s1, "zone_dist": zdist, "verdict": verdict,
            "note": "Each session scored vs that day's CLASSIC pivot (from the prior session). %above-pivot is the "
                    "bull/bear tilt; avg-run = how many sessions it typically holds one side before flipping; "
                    "zone_dist = share of time in each pivot band. Acceptance, not a signal — context for bias."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = pivots(sys.argv[1] if len(sys.argv) > 1 else "RELIANCE")
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    b = r["basis"]
    print(f"\n  {r['tk']}  spot ₹{r['spot']}   (pivots from {r['basis_date']}: O{b['open']} H{b['high']} L{b['low']} C{b['close']})")
    cl, fb, cm = r["methods"]["classic"], r["methods"]["fibonacci"], r["methods"]["camarilla"]
    print(f"\n  {'':<5}{'CLASSIC':>11}{'FIBONACCI':>12}{'CAMARILLA':>12}")
    for lvl in ("R4", "R3", "R2", "R1", "P", "S1", "S2", "S3", "S4"):
        print(f"  {lvl:<5}{('₹'+format(cl[lvl],'.1f')) if lvl in cl else '·':>11}"
              f"{('₹'+format(fb[lvl],'.1f')) if lvl in fb else '·':>12}"
              f"{('₹'+format(cm[lvl],'.1f')) if lvl in cm else '·':>12}")
    p = r["position"]
    print(f"\n  position: {p['zone']}  ·  {p['bias']}")
    if p["nearest_resistance"]:
        print(f"  nearest resistance: {p['nearest_resistance']['name']} ₹{p['nearest_resistance']['price']} ({p['nearest_resistance']['dist_pct']:+}%)")
    if p["nearest_support"]:
        print(f"  nearest support:    {p['nearest_support']['name']} ₹{p['nearest_support']['price']} ({p['nearest_support']['dist_pct']:+}%)")
