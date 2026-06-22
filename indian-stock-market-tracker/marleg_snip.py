"""
marleg_snip.py — "snip" a slice of the chart and analyze ONLY that window.

You brush a start→end range on the chart; this pulls daily OHLCV, slices to JUST that stretch, and
reports what happened in it: net move + annualized, the path (max run-up / max drawdown inside the
window), realized vol, up/down-day mix, volume vs the run-up before it, the trend slope, the high/low
with dates, and which S/R levels sit inside the band. Read-only daily compute. Decision-support, not advice.

  python marleg_snip.py RELIANCE 2026-03-01 2026-05-01
"""
import sys
import numpy as np
import pandas as pd


def _hist(tk, period="2y"):
    import marleg_vol as mv
    import yfinance as yf
    yfsym = mv._yf_symbol(tk.upper())
    h = yf.Ticker(yfsym).history(period=period)
    return h[["Open", "High", "Low", "Close", "Volume"]].dropna() if h is not None and len(h) else None


def analyze(tk, start, end):
    h = _hist(tk)
    if h is None or len(h) < 20:
        return {"ok": False, "error": f"no history for {tk}"}
    h = h.copy()
    h["d"] = [d.strftime("%Y-%m-%d") for d in h.index]
    s, e = sorted([str(start), str(end)])
    win = h[(h["d"] >= s) & (h["d"] <= e)]
    if len(win) < 2:
        return {"ok": False, "error": "window too short — snip a wider range (need ≥2 sessions)"}
    O, H, L, C, V = (win["Open"].values, win["High"].values, win["Low"].values,
                     win["Close"].values, win["Volume"].values)
    n = len(win)
    c0, c1 = float(C[0]), float(C[-1])
    ret = (c1 / c0 - 1) * 100
    ann = ((c1 / c0) ** (252.0 / max(n, 1)) - 1) * 100
    hi, lo = float(H.max()), float(L.min())
    hi_d = str(win["d"].values[int(H.argmax())]); lo_d = str(win["d"].values[int(L.argmin())])
    rng = (hi / lo - 1) * 100
    # path WITHIN the window (on close): deepest drawdown and biggest run-up
    run = np.maximum.accumulate(C); maxdd = float((C / run - 1).min()) * 100
    trough = np.minimum.accumulate(C); maxrun = float((C / trough - 1).max()) * 100
    r = np.diff(C) / C[:-1]
    up_days, dn_days = int((r > 0).sum()), int((r < 0).sum())
    rvol = float(np.std(r, ddof=1) * np.sqrt(252) * 100) if len(r) > 1 else 0.0
    # volume vs the 60 sessions BEFORE the window
    before = h[h["d"] < s].tail(60)
    vbase = float(before["Volume"].mean()) if len(before) else float(V.mean())
    vrat = (float(V.mean()) / vbase) if vbase else 1.0
    # trend = OLS slope of close, as %/session
    slope = float(np.polyfit(np.arange(n), C, 1)[0])
    slope_pct = (slope / c0) * 100
    # S/R levels that fall inside the snipped band
    levels_in = []
    try:
        import marleg_levels as lv
        L_ = lv.levels(tk)
        if L_.get("ok"):
            for x in L_.get("levels", []):
                if lo <= x["price"] <= hi:
                    levels_in.append({"price": x["price"], "type": x["type"], "strength": x["strength"]})
            levels_in.sort(key=lambda z: -z["strength"])
    except Exception:
        pass
    direction = "UP" if ret > 0.5 else "DOWN" if ret < -0.5 else "FLAT"
    verdict = ("{:+.1f}% over {} sessions ({} up / {} down), trend {:+.2f}%/session; deepest drawdown "
               "inside the stretch {:.1f}%, biggest run-up +{:.1f}%, realized vol {:.0f}% ann."
               ).format(ret, n, up_days, dn_days, slope_pct, maxdd, maxrun, rvol)
    return {"ok": True, "tk": tk.upper(), "start": s, "end": e, "sessions": n,
            "start_close": round(c0, 2), "end_close": round(c1, 2),
            "ret_pct": round(ret, 2), "ann_pct": round(ann, 1), "direction": direction,
            "high": round(hi, 2), "high_date": hi_d, "low": round(lo, 2), "low_date": lo_d, "range_pct": round(rng, 1),
            "max_drawdown_pct": round(maxdd, 1), "max_runup_pct": round(maxrun, 1),
            "up_days": up_days, "down_days": dn_days, "realized_vol_ann": round(rvol, 1),
            "vol_vs_prior": round(vrat, 2), "trend_slope_pct": round(slope_pct, 3),
            "levels_in_band": levels_in[:6], "verdict": verdict,
            "note": "Stats on ONLY the snipped window (daily closes). Volume vs the 60 sessions before it. "
                    "Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    import json
    tk = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    a = sys.argv[2] if len(sys.argv) > 2 else "2026-03-01"
    b = sys.argv[3] if len(sys.argv) > 3 else "2026-05-01"
    print(json.dumps(analyze(tk, a, b), indent=2))
