"""
marleg_cuphandle_study.py — does O'Neil's CUP-WITH-HANDLE ("pan then handle") pay in India, and is U/D 2-4 the sweet spot?

User's pattern: a CUP (price falls from a prior high, rounds a bottom, recovers back near that high) then a
HANDLE (a short, shallow pullback near the rim — the "fight" / shakeout), bought on the BREAKOUT above the
handle pivot. We approximate O'Neil's discretionary pattern algorithmically and backtest forward returns on
the canonical 5y panel, then bucket by the 20d U/D ratio (<2, 2-4, >4) — and check whether the U/D PERIOD
(10 / 20 / 40d) changes the read.

HONEST: this is an algorithmic approximation of a chart pattern O'Neil drew by eye; treat the win rate as
indicative of the EDGE, not a literal trading rule. Net of cost. Bootstrap lower bound = the honest metric.

  python marleg_cuphandle_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_cuphandle_study.json")
COST = 0.30
HZ = [5, 10, 20]
RNG = np.random.default_rng(0)


def _boot_lb(a, nb=600, pct=5, cap=8000):
    x = np.asarray(a, float)
    if len(x) < 25:
        return None
    if len(x) > cap:
        x = RNG.choice(x, cap, replace=False)
    m = np.array([x[RNG.integers(0, len(x), len(x))].mean() for _ in range(nb)])
    return round(float(np.percentile(m, pct)) * 100 - COST, 3)


def _ud(close, volume, win):
    d = np.sign(close.diff())
    up = volume.where(d > 0, 0.0).rolling(win).sum()
    dn = volume.where(d < 0, 0.0).rolling(win).sum().replace(0, np.nan)
    return up / dn


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    panel = pb.load()
    if not panel:
        print("no panel"); return
    close = panel["close"].copy()
    if getattr(close.index, "tz", None) is not None:
        close.index = close.index.tz_localize(None)
    vol = panel["volume"].reindex(close.index)

    buckets = {k: {h: [] for h in HZ} for k in ["baseline", "cup_handle", "ch_ud_lt2", "ch_ud_2_4", "ch_ud_gt4"]}
    udperiod = {p: {h: [] for h in HZ} for p in [10, 20, 40]}    # cup_handle triggers, U/D(p) in 2-4
    n_events = 0

    for s in close.columns:
        c = close[s].dropna()
        if len(c) < 320:
            continue
        v = vol[s].reindex(c.index)
        cv = c.values
        ud20 = _ud(c, v, 20)
        uds = {p: _ud(c, v, p) for p in [10, 20, 40]}
        fwd = {h: np.concatenate([cv[h:] / cv[:-h] - 1, np.full(h, np.nan)]) for h in HZ}
        n = len(cv)

        # baseline sample (every ~3rd day to bound size)
        for t in range(160, n - 21, 3):
            for h in HZ:
                if not np.isnan(fwd[h][t]):
                    buckets["baseline"][h].append(fwd[h][t])

        last_trig = -99
        for t in range(160, n - 21):
            if t - last_trig < 12:
                continue
            ph = float(cv[t - 160:t - 35].max())            # left-rim / prior high
            i_ph = t - 160 + int(cv[t - 160:t - 35].argmax())
            if i_ph >= t - 12:
                continue
            bottom = float(cv[i_ph:t - 8].min())            # cup bottom after the left rim
            depth = (ph - bottom) / ph if ph else 0
            if not (0.15 <= depth <= 0.45):                 # O'Neil cup depth ~12-33%, we allow 15-45%
                continue
            handle = cv[t - 8:t + 1]                          # the handle / pivot zone (last ~8d)
            hh = float(handle.max()); hl = float(handle.min())
            if hh < ph * 0.90:                               # right side must recover near the rim
                continue
            if (hh - hl) / hh > 0.12:                        # handle must be SHALLOW (tight fight)
                continue
            if not (cv[t] >= hh * 0.999 and cv[t] >= cv[t - 1]):   # breakout above the handle pivot today
                continue
            last_trig = t
            n_events += 1
            u = ud20.iloc[t]
            for h in HZ:
                f = fwd[h][t]
                if np.isnan(f):
                    continue
                buckets["cup_handle"][h].append(f)
                if u == u:
                    key = "ch_ud_lt2" if u < 2 else "ch_ud_2_4" if u <= 4 else "ch_ud_gt4"
                    buckets[key][h].append(f)
                for p in [10, 20, 40]:
                    up = uds[p].iloc[t]
                    if up == up and 2 <= up <= 4:
                        udperiod[p][h].append(f)

    def agg(a):
        x = np.array(a, float)
        return None if len(x) < 25 else {"n": int(len(x)), "win": round(float((x > 0).mean()) * 100, 1),
                                         "net": round(float(x.mean()) * 100 - COST, 3), "lb": _boot_lb(x)}
    res = {"cost": COST, "n_events": n_events,
           "setups": {k: {h: agg(buckets[k][h]) for h in HZ} for k in buckets},
           "ud_period_2to4": {p: {h: agg(udperiod[p][h]) for h in HZ} for p in [10, 20, 40]}}
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"cup-with-handle triggers detected: {n_events}\n")
    print(f"  {'setup':<14}{'n(10d)':>8}{'win%':>8}{'net%':>9}{'lb%':>9}   (10-day forward)")
    for k in ["baseline", "cup_handle", "ch_ud_lt2", "ch_ud_2_4", "ch_ud_gt4"]:
        r = res["setups"][k].get(10)
        print(f"  {k:<14}" + (f"{r['n']:>8}{r['win']:>8}{r['net']:>9}{str(r['lb']):>9}" if r else f"{'(n<25)':>8}"))
    print("\n  U/D 2-4 by PERIOD (cup-handle triggers, 10d): does the lookback matter?")
    for p in [10, 20, 40]:
        r = res["ud_period_2to4"][p].get(10)
        print(f"    U/D({p}d) in 2-4 " + (f"n={r['n']:>5}  win {r['win']}%  net {r['net']}%" if r else "(n<25)"))
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
