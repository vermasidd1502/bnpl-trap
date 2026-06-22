"""
marleg_gated_tenure_bt.py — does the volume-pod gated list actually pay, and does TENURE (days in the
list) matter? Point-in-time reconstruction over the canonical 5y panel.

The live gate is 3 conditions (industry-RS top-40% + U/D>50dMA&rising + price>0.618 fib) + event-clean.
The U/D + fib CORE is vectorizable and daily (so tenure = consecutive gated days); industry-RS and
event-clean can't be replayed historically — so the live list is a STRICTER subset and its real win rate
should be >= what this measures. This is the honest floor, not the ceiling.

No look-ahead: gate at close[t] uses only data <= t; forward return measured close[t] -> close[t+h].
Costs: round-trip deducted. Baseline: unconditional liquid-universe drift (separates alpha from beta).
Research artifact (gitignored).
"""
import sys
import numpy as np
import pandas as pd
import marleg_panel_build as pb

COST = 0.0025                 # ~0.25% round-trip (STT+brokerage+slippage, liquid names)
HORIZONS = [5, 10, 21]
TGT, TGT_H = 0.05, 10         # "reached target" = +5% high within 10 sessions
LIQ_Q = 0.40                  # keep top 60% by median turnover


def run():
    P = pb.load()
    close, high, vol = P["close"], P["high"], P["volume"]
    # liquidity filter
    turn = (close * vol).median()
    keep = turn[turn >= turn.quantile(LIQ_Q)].index
    close, high, vol = close[keep], high[keep], vol[keep]
    n_names, n_days = close.shape[1], close.shape[0]

    # ---- reconstruct the gate (U/D + fib core), point-in-time ----
    rc = close.pct_change()
    upv = vol.where(rc > 0, 0.0).rolling(20).sum()
    dnv = vol.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    gate_vol = (ud > ud.rolling(50).mean()) & (ud > ud.shift(10))      # above 50d-MA and rising
    hi120, lo120 = close.rolling(120).max(), close.rolling(120).min()
    fib = (close - lo120) / (hi120 - lo120)
    gate_fib = fib > 0.618                                             # above the 0.618 retracement
    gated = (gate_vol & gate_fib).fillna(False)

    # ---- tenure = consecutive days gated ending at t (1 = first day on the list) ----
    c = gated.cumsum()
    tenure = (c - c.where(~gated).ffill().fillna(0)).astype(int)

    # ---- forward returns + target-hit (no look-ahead) ----
    fwd = {h: close.shift(-h) / close - 1 for h in HORIZONS}
    fwd_high = high.shift(-TGT_H).rolling(TGT_H).max()                 # max high over next TGT_H sessions
    tgt_hit = (fwd_high >= close * (1 + TGT))

    # market regime (equal-weight universe index vs its 200d-MA), broadcast across columns
    idx = close.mean(axis=1)
    bull_s = (idx > idx.rolling(200).mean())
    bull = pd.DataFrame(np.repeat(bull_s.values.reshape(-1, 1), close.shape[1], axis=1),
                        index=close.index, columns=close.columns)

    def stats(mask, h):
        r = fwd[h].values[mask.values]
        r = r[~np.isnan(r)]
        if len(r) == 0:
            return None
        net = r - COST
        return {"n": len(r), "hit": float((net > 0).mean()) * 100, "mean": float(net.mean()) * 100,
                "med": float(np.median(net)) * 100}

    base = {h: float((fwd[h].values[~np.isnan(fwd[h].values)]).mean() - COST) * 100 for h in HORIZONS}

    print(f"\n=== GATED VOLUME-CORE BACKTEST (U/D + fib, point-in-time) ===")
    print(f"universe {n_names} liquid names x {n_days} days | gated stock-days: {int(gated.values.sum())}")
    print(f"costs: {COST*100:.2f}% round-trip | target = +{int(TGT*100)}% high within {TGT_H}d\n")

    print(f"BASELINE (any liquid name, unconditional drift), net %:  " +
          "  ".join(f"{h}d {base[h]:+.2f}" for h in HORIZONS))

    print("\nALL GATED:")
    for h in HORIZONS:
        s = stats(gated, h)
        print(f"  {h:>2}d: n={s['n']:>6}  hit {s['hit']:4.1f}%  mean {s['mean']:+.2f}%  "
              f"(excess vs drift {s['mean']-base[h]:+.2f}%)  median {s['med']:+.2f}%")
    th = tgt_hit.values[gated.values]; th = th[~pd.isna(th)]
    print(f"  target +{int(TGT*100)}% within {TGT_H}d:  {th.mean()*100:4.1f}% hit")

    print("\nBY TENURE (days already on the list at entry):")
    buckets = [("day 1", tenure == 1), ("days 2-4", (tenure >= 2) & (tenure <= 4)), ("days 5+", tenure >= 5)]
    for label, tb in buckets:
        m = gated & tb
        row = []
        for h in HORIZONS:
            s = stats(m, h)
            row.append(f"{h}d hit {s['hit']:4.1f}% mean {s['mean']:+.2f}%" if s else f"{h}d n/a")
        thb = tgt_hit.values[m.values]; thb = thb[~pd.isna(thb)]
        tgtpct = f"{thb.mean()*100:4.1f}%" if len(thb) else "n/a"
        n1 = stats(m, HORIZONS[0])
        print(f"  {label:<9} (n={n1['n']:>6}):  " + "  ".join(row) + f"  | tgt+{int(TGT*100)}% {tgtpct}")

    print("\nBY REGIME (10d horizon):")
    for label, rb in [("BULL (idx>200dMA)", bull), ("BEAR (idx<200dMA)", ~bull)]:
        s = stats(gated & rb, 10)
        if s:
            print(f"  {label:<20} n={s['n']:>6}  hit {s['hit']:4.1f}%  mean {s['mean']:+.2f}%  "
                  f"(excess {s['mean']-base[10]:+.2f}%)")
    print()


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    run()
