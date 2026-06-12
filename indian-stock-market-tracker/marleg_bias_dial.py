"""
Marle-G — BIAS DIAL: one number, 0 (short-favored) → 50 (neutral) → 100 (long-favored),
built from breadth + volume tilt + trend, and BACKTESTED to answer the real question:
when the dial is low, do stocks actually FALL enough to short profitably — or do they
just dip-and-recover (in which case the dial means long-or-CASH, not long-or-short)?

Components (each 0-100, blended):
  breadth50   % of universe above its 50d MA
  breadth200  % above 200d MA
  ud_tilt     % of names with ud(20d) > 1 (accumulation breadth)
  nifty_trend NIFTY vs its 100d MA (scaled)
  nh_nl       (20d-new-highs − 20d-new-lows) / N, scaled

TEST: bucket every day by dial, measure forward 10d return of the equal-weight universe
(long) and what a short would net after ~12bps intraday cost. Short "works" only if low
buckets show forward returns negative enough to clear costs.

  python marleg_bias_dial.py
"""
import sys, json
import numpy as np
import pandas as pd
import marleg_datastore as ds

SHORT_COST = 12 / 1e4    # intraday short round-trip (best case); puts cost ~5x more


def _frame():
    ds.sync(verbose=False)
    C = ds.panel("close").ffill()
    keep = [c for c in C.columns if C[c].dropna().shape[0] > 400]
    C = C[keep]
    V = ds.panel("volume")[keep].reindex(C.index)
    rc = C.pct_change()
    above50 = C > C.rolling(50).mean()
    above200 = C > C.rolling(200).mean()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    hi20 = C.rolling(20).max()
    lo20 = C.rolling(20).min()
    nh = (C >= hi20 * 0.999)
    nl = (C <= lo20 * 1.001)
    try:
        nifty = ds.series("^NSEI").reindex(C.index).ffill()
    except Exception:
        nifty = C.mean(axis=1)
    n100 = nifty.rolling(100).mean()
    return C, ud, above50, above200, nh, nl, nifty, n100


def dial_series():
    C, ud, above50, above200, nh, nl, nifty, n100 = _frame()
    breadth50 = above50.mean(axis=1) * 100
    breadth200 = above200.mean(axis=1) * 100
    ud_tilt = (ud > 1).mean(axis=1) * 100
    trend = (50 + np.clip((nifty / n100 - 1) * 100 * 8, -50, 50))
    nh_nl = 50 + np.clip((nh.sum(axis=1) - nl.sum(axis=1)) / C.shape[1] * 100 * 2.5, -50, 50)
    comp = pd.DataFrame({"breadth50": breadth50, "breadth200": breadth200,
                         "ud_tilt": ud_tilt, "trend": trend, "nh_nl": nh_nl})
    dial = comp.mean(axis=1)
    return dial, comp, C


def run():
    dial, comp, C = dial_series()
    ewret = C.pct_change().mean(axis=1)                       # equal-weight universe daily
    fwd10 = (C.pct_change(10).shift(-10)).mean(axis=1) * 100  # fwd 10d EW return
    df = pd.DataFrame({"dial": dial, "fwd10": fwd10}).dropna()
    df = df.iloc[220:]                                        # warmup
    buckets = [(0, 20, "0-20 deep-short"), (20, 40, "20-40 lean-short"),
               (40, 60, "40-60 neutral"), (60, 80, "60-80 lean-long"),
               (80, 101, "80-100 strong-long")]
    res = []
    for lo, hi, name in buckets:
        seg = df[(df.dial >= lo) & (df.dial < hi)]
        if len(seg) < 20:
            res.append({"bucket": name, "n": len(seg), "note": "thin"}); continue
        f = seg.fwd10
        res.append({"bucket": name, "n": len(f),
                    "long_fwd10_med": round(float(f.median()), 2),
                    "long_fwd10_avg": round(float(f.mean()), 2),
                    "long_win_pct": round(float((f > 0).mean() * 100), 1),
                    "short_net_med": round(float(-f.median() - SHORT_COST * 100), 2),
                    "short_win_pct": round(float((f < 0).mean() * 100), 1)})
    live = float(dial.iloc[-1])
    livec = {k: round(float(comp[k].iloc[-1]), 0) for k in comp.columns}
    reading = ("STRONG LONG" if live >= 70 else "LEAN LONG" if live >= 55 else
               "NEUTRAL" if live >= 45 else "LEAN SHORT/CASH" if live >= 30 else "DEEP SHORT/CASH")
    # verdict: does shorting the low buckets actually pay net of cost?
    deep = next((r for r in res if r["bucket"].startswith("0-20")), None)
    lean = next((r for r in res if r["bucket"].startswith("20-40")), None)
    short_works = any(r.get("short_net_med", -9) > 0.3 and r.get("short_win_pct", 0) > 52
                      for r in (deep, lean) if r and "long_fwd10_med" in r)
    return {"live_dial": round(live, 1), "reading": reading, "components": livec,
            "buckets": res, "short_logic_works": short_works,
            "verdict": ("Shorting the low-dial regime clears costs — worth a tested short stream."
                        if short_works else
                        "Shorting does NOT pay even in low-dial regimes (drift/dip-recovery dominates) "
                        "-> the dial means LONG vs CASH, not LONG vs SHORT."),
            "note": "Forward 10d EW-universe returns by dial bucket; short net of 12bps (best-case "
                    "intraday; puts cost ~5x). No lookahead (dial from data through day t)."}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = run()
    import os
    json.dump(r, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "marleg_bias_dial.json"), "w"), indent=1)
    print(f"\nBIAS DIAL = {r['live_dial']}/100  ->  {r['reading']}")
    print("components:", r["components"])
    print()
    print(f"{'bucket':<20}{'n':>6}{'LONG fwd10':>12}{'long win':>10}{'SHORT net':>11}{'short win':>11}")
    for b in r["buckets"]:
        if "long_fwd10_med" not in b:
            print(f"{b['bucket']:<20}{b['n']:>6}  (thin)"); continue
        print(f"{b['bucket']:<20}{b['n']:>6}{b['long_fwd10_med']:>11}%{b['long_win_pct']:>9}%"
              f"{b['short_net_med']:>10}%{b['short_win_pct']:>10}%")
    print("\nSHORT LOGIC WORKS:", r["short_logic_works"])
    print("VERDICT:", r["verdict"])


if __name__ == "__main__":
    main()
