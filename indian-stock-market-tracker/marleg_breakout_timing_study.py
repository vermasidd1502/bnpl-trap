"""
marleg_breakout_timing_study.py — does "fight then burst" beat raw breakouts? (canonical-panel, look-ahead-safe)

The user's theory: a breakout is stronger when it RESOLVES a tight-range consolidation — a "fight"
between buyers and sellers (volatility/range contraction) — than when it fires mid-rally. We test it
RIGOROUSLY on the canonical 5y panel (marleg_panel_build.load(), ~750 liquid names, Groww, no throttle).

CONSOLIDATION DEFINITIONS (each LOOK-AHEAD-SAFE: the "is this tight?" threshold uses only PAST data — a
trailing rolling quantile shifted by 1. The OLD version used a full-history percentile = peeking at the
future, which is exactly the flaw we're fixing):
  range_tight : 20d box width in the bottom tercile of its OWN trailing-252d history
  bb_squeeze  : Bollinger band-width (4*std/ma, 20d) in the bottom quintile of trailing-252d history
  atr_contract: ATR(14)/price in the bottom quartile of trailing-126d history
  retstd_low  : 20d realized vol (std of returns) in the bottom quartile of trailing-252d history

SETUPS compared (forward 5/10/20d, close-to-close, net of COST% round-trip):
  baseline    : every stock-day (the unconditional drift — the bar to beat)
  fib_gate    : price > 0.618 of 120d range (our current gate)
  raw_brk     : close breaks above prior-20d high, NO base required
  fb_range/bb/atr/std : raw_brk that resolves consolidation <def>  (the "fight then burst")
  coil_only   : in a tight base but NOT breaking out yet (does coiling alone predict up?)
  breakdown   : tight base then breaks DOWN below the box (control — India weak names tend to bounce)
  fb_range_vol: fb_range + breakout-day volume > 1.5x its 20d average (volume confirmation)

Each setup reports n, mean%, net%, win%, and a BOOTSTRAP 5th-percentile lower bound of net mean
(the "best-fit, not most-outlandish" metric). We also split bull/bear (market proxy vs its 50DMA).
A definition only earns a gate if its net lower-bound clears raw_brk AND fib_gate by a real margin.

ENTRY TIMING (on fb_range events, 10d): breakout_close vs pullback_retest (with FILL rate) vs delay_2d.

  python marleg_breakout_timing_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_breakout_timing_study.json")
COST = 0.30
HORIZONS = [5, 10, 20]
RNG = np.random.default_rng(0)
SET = ["baseline", "fib_gate", "raw_brk", "fb_range", "fb_bb", "fb_atr", "fb_std",
       "coil_only", "breakdown", "fb_range_vol"]
REGIME_KEYS = ["fib_gate", "raw_brk", "fb_range", "fb_bb", "fb_atr", "fb_std", "fb_range_vol"]


def _boot_lb(arr, n_boot=800, pct=5, cap=20000):
    """5th-percentile bootstrap lower bound of the NET mean return (%, after COST). The honest,
    not-outlandish metric: an edge we can defend even on a bad draw of the sample."""
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
        print("no canonical panel — run first:  python marleg_panel_build.py")
        return
    close, high, low, vol = panel["close"], panel["high"], panel["low"], panel["volume"]
    print(f"panel: {close.shape[1]} names x {close.shape[0]} days ({close.index[0].date()} -> {close.index[-1].date()})")

    # market regime: equal-weight index vs its 50DMA (bull = risk-on)
    mkt = (1 + close.pct_change().mean(axis=1)).cumprod()
    bull_by_date = (mkt > mkt.rolling(50).mean())

    store = {k: {h: {"r": [], "b": []} for h in HORIZONS} for k in SET}
    timing = {k: {"ret": [], "fills": 0, "events": 0} for k in ["breakout_close", "pullback_retest", "delay_2d"]}

    kept = 0
    for s in close.columns:
        c_s = close[s].dropna()
        if len(c_s) < 300:
            continue
        kept += 1
        idx = c_s.index
        c = c_s.values
        h = high[s].reindex(idx).values
        l = low[s].reindex(idx).values
        v = vol[s].reindex(idx).values
        bull = bull_by_date.reindex(idx).fillna(False).values
        n = len(c)

        # prior-20d box from HIGH/LOW (look-ahead-safe via shift(1))
        hh20 = pd.Series(h).rolling(20).max().shift(1).values
        ll20 = pd.Series(l).rolling(20).min().shift(1).values
        rng20 = (hh20 - ll20) / np.where(ll20 == 0, np.nan, ll20)
        rng_q = pd.Series(rng20).rolling(252, min_periods=60).quantile(0.33).shift(1).values

        cs = pd.Series(c)
        ma20 = cs.rolling(20).mean()
        sd20 = cs.rolling(20).std()
        bbw = (4 * sd20 / ma20)
        bbw_v = bbw.shift(1).values
        bbw_q = bbw.rolling(252, min_periods=60).quantile(0.20).shift(2).values

        pc = np.concatenate([[np.nan], c[:-1]])
        tr = np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])
        atrp = pd.Series(tr).rolling(14).mean().values / c
        atrp_v = pd.Series(atrp).shift(1).values
        atrp_q = pd.Series(atrp).rolling(126, min_periods=40).quantile(0.25).shift(2).values

        ret = cs.pct_change()
        std20 = ret.rolling(20).std()
        std_v = std20.shift(1).values
        std_q = std20.rolling(252, min_periods=60).quantile(0.25).shift(2).values

        hh120 = pd.Series(h).rolling(120).max().shift(1).values
        ll120 = pd.Series(l).rolling(120).min().shift(1).values
        fibpos = (c - ll120) / (hh120 - ll120)
        vavg = pd.Series(v).rolling(20).mean().shift(1).values

        fwd = {hz: np.concatenate([c[hz:] / c[:-hz] - 1, np.full(hz, np.nan)]) for hz in HORIZONS}

        prev_le = np.concatenate([[False], c[:-1] <= hh20[:-1]])
        prev_ge = np.concatenate([[False], c[:-1] >= ll20[:-1]])
        brk = (c > hh20) & prev_le
        brkdn = (c < ll20) & prev_ge
        tight_range = rng20 <= rng_q
        tight_bb = bbw_v <= bbw_q
        tight_atr = atrp_v <= atrp_q
        tight_std = std_v <= std_q
        volok = (vavg > 0) & (v > 1.5 * vavg)
        valid = ~np.isnan(hh20) & np.isfinite(fibpos)

        def add(tag, mask):
            for hz in HORIZONS:
                f = fwd[hz]
                m = mask & ~np.isnan(f)
                if m.any():
                    store[tag][hz]["r"].extend(f[m].tolist())
                    store[tag][hz]["b"].extend(bull[m].tolist())

        add("baseline", valid)
        add("fib_gate", valid & (fibpos > 0.618))
        add("raw_brk", valid & brk)
        add("fb_range", valid & brk & tight_range)
        add("fb_bb", valid & brk & tight_bb)
        add("fb_atr", valid & brk & tight_atr)
        add("fb_std", valid & brk & tight_std)
        add("coil_only", valid & tight_range & ~brk)
        add("breakdown", valid & brkdn & tight_range)
        add("fb_range_vol", valid & brk & tight_range & volok)

        # ENTRY TIMING on fb_range events (10d horizon)
        hz = 10
        for t in np.where(valid & brk & tight_range)[0]:
            if t + hz < n:
                timing["breakout_close"]["events"] += 1
                timing["breakout_close"]["ret"].append(c[t + hz] / c[t] - 1)
                timing["breakout_close"]["fills"] += 1
            if t + 2 + hz < n:
                timing["delay_2d"]["events"] += 1
                timing["delay_2d"]["ret"].append(c[t + 2 + hz] / c[t + 2] - 1)
                timing["delay_2d"]["fills"] += 1
            box = hh20[t]
            timing["pullback_retest"]["events"] += 1
            for j in range(t + 1, min(t + 6, n)):
                if l[j] <= box and j + hz < n:
                    timing["pullback_retest"]["ret"].append(c[j + hz] / box - 1)
                    timing["pullback_retest"]["fills"] += 1
                    break

    res = {"cost": COST, "panel": pb.info(), "kept": kept, "setups": {}, "regime": {}, "timing": {}}
    for k in SET:
        res["setups"][k] = {hz: _agg(store[k][hz]["r"]) for hz in HORIZONS}
    for k in REGIME_KEYS:
        res["regime"][k] = {"bull": _agg(store[k][10]["r"], store[k][10]["b"], "bull"),
                            "bear": _agg(store[k][10]["r"], store[k][10]["b"], "bear")}
    for k in timing:
        a = _agg(timing[k]["ret"])
        if a:
            a["fill_rate"] = round(timing[k]["fills"] / max(1, timing[k]["events"]) * 100, 1)
        res["timing"][k] = a
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    def row(name, r):
        if not r:
            return f"  {name:<14}{'(n<40)':>50}"
        return f"  {name:<14}{r['n']:>9,}{r['mean']:>9}{r['net']:>9}{r['win']:>8}{str(r['lb']):>9}"

    print(f"\nkept {kept} names. ranked by 10-day bootstrap lower-bound (net%, the honest edge):")
    print(f"  {'setup':<14}{'n':>9}{'mean%':>9}{'net%':>9}{'win%':>8}{'lb%':>9}")
    order = sorted(SET, key=lambda k: (res["setups"][k].get(10) or {}).get("lb", -99), reverse=True)
    for k in order:
        print(row(k, res["setups"][k].get(10)))

    print("\n10-day, BULL vs BEAR regime (net%):")
    print(f"  {'setup':<14}{'bull n':>8}{'bull net':>10}{'bear n':>8}{'bear net':>10}")
    for k in REGIME_KEYS:
        rb, rr = res["regime"][k]["bull"], res["regime"][k]["bear"]
        print(f"  {k:<14}{(rb['n'] if rb else 0):>8,}{(rb['net'] if rb else '—'):>10}"
              f"{(rr['n'] if rr else 0):>8,}{(rr['net'] if rr else '—'):>10}")

    print("\nENTRY TIMING on fb_range breakouts (10-day, net%):")
    print(f"  {'timing':<18}{'n':>8}{'net%':>9}{'win%':>8}{'fill%':>8}")
    for k in ["breakout_close", "pullback_retest", "delay_2d"]:
        r = res["timing"].get(k)
        if r:
            print(f"  {k:<18}{r['n']:>8,}{r['net']:>9}{r['win']:>8}{r.get('fill_rate', '—'):>8}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
