"""
marleg_param_stability.py — the VALIDATION LAB: parameter-stability heatmap + Monte-Carlo per cell.

Overfit vs robust, made visible. Sweep a strategy's TWO key parameters on a grid; at each cell, backtest
the net edge on the canonical panel AND bootstrap the trades for a robust lower bound (5th percentile).
  • ROBUST  strategy → a broad PLATEAU of good cells (nearby params all work).
  • OVERFIT strategy → a lone SPIKE (one magic cell, neighbours dead).
We score plateau-vs-spike explicitly so it isn't eyeballing.

This is also the substrate for BACKTESTABLE AGENT LOGIC: an agent's rule is just a parameterised strategy;
run it through here and you know if its thresholds sit on a plateau (trust it) or a spike (don't).

  python marleg_param_stability.py
"""
import sys
import numpy as np
import pandas as pd
import marleg_panel_build as pb

COST = 0.0025
FIB_THRS = [0.40, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]      # gate-3 threshold
HOLDS = [3, 5, 8, 10, 13, 16, 21, 25]                            # holding horizon (sessions)
BOOT = 300


def _prep():
    P = pb.load()
    close, high, vol = P["close"], P["high"], P["volume"]
    turn = (close * vol).median()
    keep = turn[turn >= turn.quantile(0.40)].index
    close, vol = close[keep], vol[keep]
    rc = close.pct_change()
    upv = vol.where(rc > 0, 0.0).rolling(20).sum()
    dnv = vol.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    gate_vol = (ud > ud.rolling(50).mean()) & (ud > ud.shift(10))    # U/D above 50dMA and rising (fixed)
    hi120, lo120 = close.rolling(120).max(), close.rolling(120).min()
    fib = (close - lo120) / (hi120 - lo120)
    fwd = {h: (close.shift(-h) / close - 1) for h in HOLDS}          # fwd return per hold (precomputed once)
    return gate_vol.fillna(False), fib, fwd


def sweep():
    gate_vol, fib, fwd = _prep()
    sh = np.full((len(FIB_THRS), len(HOLDS)), np.nan)               # annualised Sharpe per cell
    lb = np.full_like(sh, np.nan)                                    # MC 5th-pct of mean (robust)
    mean = np.full_like(sh, np.nan)                                  # net mean return per trade
    narr = np.zeros_like(sh, dtype=int)
    rng = np.random.default_rng(7)
    for i, ft in enumerate(FIB_THRS):
        gate = (gate_vol & (fib > ft)).values
        for j, hd in enumerate(HOLDS):
            r = fwd[hd].values[gate]
            r = r[~np.isnan(r)] - COST
            if len(r) < 200:
                continue
            narr[i, j] = len(r)
            mu, sd = r.mean(), r.std(ddof=1)
            sh[i, j] = (mu / sd * np.sqrt(252.0 / hd)) if sd > 0 else 0.0
            mean[i, j] = mu * 100
            boot_means = rng.choice(r, size=(BOOT, min(len(r), 4000)), replace=True).mean(axis=1)
            lb[i, j] = np.percentile(boot_means, 5) * 100           # robust lower bound on mean return
    # ---- plateau vs spike (on the robust lower-bound surface) ----
    flat = lb.copy()
    bi, bj = np.unravel_index(np.nanargmax(flat), flat.shape)
    best = flat[bi, bj]
    nb = [flat[a, b] for a in range(bi - 1, bi + 2) for b in range(bj - 1, bj + 2)
          if 0 <= a < flat.shape[0] and 0 <= b < flat.shape[1] and not (a == bi and b == bj) and not np.isnan(flat[a, b])]
    nb_mean = float(np.mean(nb)) if nb else 0.0
    plateau_ratio = round(nb_mean / best, 2) if best > 0 else 0.0
    pos_frac = round(float(np.nanmean(lb > 0)) * 100, 0)            # % of cells robustly positive
    verdict = ("ROBUST — best params sit on a plateau; neighbours work too" if plateau_ratio >= 0.6 and pos_frac >= 50
               else "FRAGILE/THIN — edge is small or patchy across params" if pos_frac >= 25
               else "OVERFIT-RISK — good cells are isolated spikes; neighbours fail")
    return {"fib_thrs": FIB_THRS, "holds": HOLDS, "sharpe": sh.tolist(), "lb_mean": lb.tolist(),
            "mean_ret": mean.tolist(), "n": narr.tolist(),
            "best": {"fib_thr": FIB_THRS[bi], "hold": HOLDS[bj], "lb_mean": round(best, 3), "sharpe": round(sh[bi, bj], 2)},
            "plateau_ratio": plateau_ratio, "pos_frac_pct": pos_frac, "verdict": verdict,
            "note": "Heatmap = robust lower-bound (MC 5th-pct) of net mean return per cell. Plateau ratio = "
                    "neighbours ÷ best (→1 robust, →0 overfit spike). Decision-support; the U/D+fib gate core only."}


def _heat(grid, rows, cols, title):
    ramp = " .:-=+*#%@"
    vals = np.array(grid, float)
    lo, hi = np.nanmin(vals), np.nanmax(vals)
    print(f"\n{title}   (rows=fib_thr, cols=hold; darker=better)")
    print("        " + "".join(f"{c:>6}" for c in cols))
    for i, rv in enumerate(rows):
        cells = []
        for j in range(len(cols)):
            v = vals[i, j]
            if np.isnan(v):
                cells.append("   .  ")
            else:
                k = 0 if hi == lo else int((v - lo) / (hi - lo) * (len(ramp) - 1))
                cells.append(f" {ramp[k]}{v:+4.2f}")
        print(f"  {rv:>5} " + "".join(cells))


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = sweep()
    _heat(r["lb_mean"], r["fib_thrs"], r["holds"], "ROBUST LOWER-BOUND of net mean return % (MC 5th-pct)")
    print(f"\n  best cell: fib>{r['best']['fib_thr']} × hold {r['best']['hold']}d  →  robust LB {r['best']['lb_mean']}%  (Sharpe {r['best']['sharpe']})")
    print(f"  plateau ratio: {r['plateau_ratio']}   ·   {r['pos_frac_pct']}% of cells robustly positive")
    print(f"  VERDICT: {r['verdict']}\n  {r['note']}")
