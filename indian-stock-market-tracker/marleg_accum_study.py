"""
marleg_accum_study.py — is explicit volume ACCUMULATION an edge beyond our U/D gate?

User Q: "Can volume accumulation be key for our trading strategy?" Our gate #2 already uses the U/D
ratio (up-volume vs down-volume, above its 50d MA and rising). This tests whether fancier accumulation
metrics add forward edge OVER that gate, on the canonical 5y panel:
  ud_gate    : the current gate (U/D > 50d MA AND rising vs 10d ago)
  obv_up     : On-Balance-Volume higher than 20d ago (steady net buying)
  ad_up      : Accumulation/Distribution (CLV-weighted volume) line rising over 20d
  upvol_hi   : 20d up-volume share in the top tercile of its OWN trailing-252d history (look-ahead-safe)
  ud_and_obv : the U/D gate AND OBV rising  (does layering accumulation on the gate help?)
  ud_and_ad  : the U/D gate AND A/D rising
Honest metric: bootstrap 5th-pct lower bound of net mean (10d), plus bull/bear split.

  python marleg_accum_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_accum_study.json")
COST = 0.30
HORIZONS = [5, 10, 20]
RNG = np.random.default_rng(0)
SET = ["baseline", "ud_gate", "obv_up", "ad_up", "upvol_hi", "ud_and_obv", "ud_and_ad"]
REGIME_KEYS = ["ud_gate", "obv_up", "ad_up", "upvol_hi", "ud_and_obv", "ud_and_ad"]


def _boot_lb(arr, n_boot=800, pct=5, cap=20000):
    x = np.asarray(arr, float)
    n = len(x)
    if n < 40:
        return None
    if n > cap:
        x = RNG.choice(x, cap, replace=False)
        n = cap
    means = np.empty(n_boot)
    for b in range(n_boot):
        means[b] = x[RNG.integers(0, n, n)].mean()
    return round(float(np.percentile(means, pct)) * 100 - COST, 3)


def _agg(rets, bulls=None, regime=None):
    r = np.asarray(rets, float)
    if regime and bulls is not None:
        b = np.asarray(bulls, bool)
        r = r[b] if regime == "bull" else r[~b]
    if len(r) < 40:
        return None
    return {"n": int(len(r)), "mean": round(float(r.mean()) * 100, 3),
            "net": round(float(r.mean()) * 100 - COST, 3),
            "win": round(float((r > 0).mean()) * 100, 1), "lb": _boot_lb(r)}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    panel = pb.load()
    if not panel:
        print("no canonical panel — run:  python marleg_panel_build.py")
        return
    close, high, low, vol = panel["close"], panel["high"], panel["low"], panel["volume"]
    print(f"panel: {close.shape[1]} names x {close.shape[0]} days")
    mkt = (1 + close.pct_change().mean(axis=1)).cumprod()
    bull_by_date = (mkt > mkt.rolling(50).mean())

    store = {k: {h: {"r": [], "b": []} for h in HORIZONS} for k in SET}

    for s in close.columns:
        c_s = close[s].dropna()
        if len(c_s) < 300:
            continue
        idx = c_s.index
        c = c_s.values
        h = high[s].reindex(idx).values
        l = low[s].reindex(idx).values
        v = vol[s].reindex(idx).values
        bull = bull_by_date.reindex(idx).fillna(False).values
        cs = pd.Series(c)

        sgn = np.sign(cs.diff()).fillna(0.0).values
        upv = pd.Series(np.where(sgn > 0, v, 0.0))
        dnv = pd.Series(np.where(sgn < 0, v, 0.0))
        ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
        ud_ma = ud.rolling(50).mean()
        ud_gate = ((ud > ud_ma) & (ud > ud.shift(10))).values

        obv = pd.Series(sgn * v).cumsum()
        obv_up = (obv > obv.shift(20)).values

        rng_hl = (h - l)
        clv = np.where(rng_hl > 0, ((c - l) - (h - c)) / np.where(rng_hl > 0, rng_hl, np.nan), 0.0)
        ad = pd.Series(clv * v).cumsum()
        ad_up = (ad > ad.shift(20)).values

        share = upv.rolling(20).sum() / pd.Series(v).rolling(20).sum()
        share_q = share.rolling(252, min_periods=60).quantile(0.66).shift(1)
        upvol_hi = (share > share_q).fillna(False).values

        fwd = {hz: np.concatenate([c[hz:] / c[:-hz] - 1, np.full(hz, np.nan)]) for hz in HORIZONS}
        valid = ~np.isnan(ud_ma.values)

        def add(tag, mask):
            for hz in HORIZONS:
                f = fwd[hz]
                m = mask & ~np.isnan(f)
                if m.any():
                    store[tag][hz]["r"].extend(f[m].tolist())
                    store[tag][hz]["b"].extend(bull[m].tolist())

        add("baseline", valid)
        add("ud_gate", valid & ud_gate)
        add("obv_up", valid & obv_up)
        add("ad_up", valid & ad_up)
        add("upvol_hi", valid & upvol_hi)
        add("ud_and_obv", valid & ud_gate & obv_up)
        add("ud_and_ad", valid & ud_gate & ad_up)

    res = {"cost": COST, "panel": pb.info(), "setups": {}, "regime": {}}
    for k in SET:
        res["setups"][k] = {hz: _agg(store[k][hz]["r"]) for hz in HORIZONS}
    for k in REGIME_KEYS:
        res["regime"][k] = {"bull": _agg(store[k][10]["r"], store[k][10]["b"], "bull"),
                            "bear": _agg(store[k][10]["r"], store[k][10]["b"], "bear")}
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print("\n10-day, ranked by bootstrap lower-bound (net%):")
    print(f"  {'setup':<14}{'n':>10}{'mean%':>9}{'net%':>9}{'win%':>8}{'lb%':>9}")
    order = sorted(SET, key=lambda k: (res["setups"][k].get(10) or {}).get("lb", -99), reverse=True)
    for k in order:
        r = res["setups"][k].get(10)
        if r:
            print(f"  {k:<14}{r['n']:>10,}{r['mean']:>9}{r['net']:>9}{r['win']:>8}{str(r['lb']):>9}")
    print("\n10-day BULL vs BEAR (net%):")
    for k in REGIME_KEYS:
        rb, rr = res["regime"][k]["bull"], res["regime"][k]["bear"]
        print(f"  {k:<14} bull {(rb['net'] if rb else '—'):>7} (n={rb['n'] if rb else 0:,})   "
              f"bear {(rr['net'] if rr else '—'):>7} (n={rr['n'] if rr else 0:,})")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
