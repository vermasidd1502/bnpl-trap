"""
marleg_expiry_window_bt.py — does the NEAR-month expiry hurt a held NEXT-month option?

The worry: you hold a JULY call; the JUNE monthly expiry is next week — will it damage the July option?

Your July option does NOT expire in June, so it loses no time value to June's expiry directly. Two indirect
channels DO exist, and this tests the part we can measure on the underlying:
  • DELTA — if the underlying DRIFTS after the near expiry, a held call gains/loses via delta. So: is there a
    systematic post-expiry drift in NIFTY / the name?
  • GAMMA — is the expiry DAY itself abnormally volatile (dealer hedging / pin)? Measured as |return| on T0.
  • VEGA (inferred) — if REALIZED vol calms after expiry, IMPLIED vol usually does too → the next-month option
    loses a little premium to a vol-reset even if the stock is flat. We proxy it with realized vol pre vs post.

We can't see IMPLIED vol history for free, so the vega channel is INFERRED from realized vol — labelled as such.
Monthly expiry ≈ last Thursday of the month (the rule for most of the lookback; NSE moved to last Tuesday in
late-2025 — a caveat on the most recent prints).
"""
import datetime as dt

import numpy as np
import pandas as pd

import marleg_data as md

NAMES = ["NIFTY", "RELIANCE", "HDFCBANK", "LT", "INFY", "TCS", "ICICIBANK", "SBIN"]


def _monthly_expiries(idx):
    """Last Thursday (weekday 3) trading day of each month present in the index."""
    df = pd.DataFrame({"d": idx})
    df["ym"] = df["d"].dt.to_period("M")
    df["thu"] = df["d"].dt.weekday == 3
    exps = []
    for ym, g in df.groupby("ym"):
        thu = g[g["thu"]]
        if len(thu):
            exps.append(thu["d"].iloc[-1])
    return exps


def study(name):
    df = md.candles(name, 1440, 800)
    if df is None or len(df) < 120:
        return None
    df = df.dropna(subset=["close"])
    close = df["close"].astype(float)
    ret = close.pct_change() * 100
    idx = list(df.index)
    pos = {d: i for i, d in enumerate(idx)}
    exps = _monthly_expiries(pd.DatetimeIndex(idx))
    offs = {o: [] for o in range(-2, 4)}
    vol_pre, vol_post, t0_abs = [], [], []
    for e in exps:
        i = pos.get(e)
        if i is None or i < 6 or i > len(idx) - 6:
            continue
        for o in offs:
            offs[o].append(float(ret.iloc[i + o]))
        t0_abs.append(abs(float(ret.iloc[i])))
        vol_pre.append(float(ret.iloc[i - 5:i].abs().mean()))
        vol_post.append(float(ret.iloc[i + 1:i + 6].abs().mean()))
    if not t0_abs:
        return None
    base_abs = float(ret.abs().mean())
    return {
        "name": name, "n_expiries": len(t0_abs),
        "offset_mean_ret": {o: round(float(np.mean(v)), 2) for o, v in offs.items()},
        "offset_pct_up": {o: round(float(np.mean(np.array(v) > 0)) * 100, 0) for o, v in offs.items()},
        "expiry_day_abs": round(float(np.mean(t0_abs)), 2), "baseline_abs": round(base_abs, 2),
        "vol_pre5": round(float(np.mean(vol_pre)), 2), "vol_post5": round(float(np.mean(vol_post)), 2),
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print("\n═══ BEHAVIOUR AROUND MONTHLY EXPIRY (does June expiry move the underlying?) ═══")
    print(f"  {'name':<11}{'#exp':>5}{'  T-1':>7}{'  T0':>7}{'  T+1':>7}{'  T+2':>7}{'  T+3':>7}{'  | T0|vs base':>14}{'  vol pre→post':>16}")
    post_drift = []
    for nm in NAMES:
        s = study(nm)
        if not s:
            print(f"  {nm:<11} (no data)"); continue
        om = s["offset_mean_ret"]
        post = round(om[1] + om[2] + om[3], 2)
        post_drift.append(post)
        print(f"  {s['name']:<11}{s['n_expiries']:>5}{om[-1]:>7}{om[0]:>7}{om[1]:>7}{om[2]:>7}{om[3]:>7}"
              f"{(str(s['expiry_day_abs'])+'/'+str(s['baseline_abs'])):>14}{(str(s['vol_pre5'])+'→'+str(s['vol_post5'])):>16}")
    if post_drift:
        print(f"\n  avg T+1..T+3 drift across names: {round(float(np.mean(post_drift)),2)}%  "
              f"(>0 = underlying tends to RISE after expiry → helps a held CALL; <0 = hurts it)")
    print("  NOTE: delta+gamma channels measured on the underlying; the vega/IV-reset channel is INFERRED from "
          "realized vol pre→post (we have no free IMPLIED-vol history). Expiry≈last Thursday (NSE→last Tuesday late-2025).")
