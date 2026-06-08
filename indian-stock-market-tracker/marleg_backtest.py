"""
Swing, VOLUME-CENTRIC backtest. Thesis: a rising U/D volume ratio (accumulation
building) PREDICTS the coming up-move/spike — the level alone just mean-reverts.

Compares several pure-volume signals, cross-sectionally ranked, rebalanced every
HOLD days: long top quintile / short bottom quintile (and long-only).

  python marleg_backtest.py          # 5-day swing hold
  python marleg_backtest.py 10
"""
import sys
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_volume_scan as v

HOLD = int(sys.argv[1]) if len(sys.argv) > 1 else 5
QTILE = 0.2
U = v.SEED


def zrow(df):
    return df.sub(df.mean(axis=1), axis=0).div(df.std(axis=1).replace(0, np.nan), axis=0)


def main():
    print(f"downloading {len(U)} symbols (2y)...")
    data = yf.download([s + ".NS" for s in U], period="2y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume = {}, {}
    for s in U:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 250:
                close[s] = c; volume[s] = data[t]["Volume"]
        except Exception:
            pass
    close = pd.DataFrame(close); volume = pd.DataFrame(volume).reindex(close.index)
    print(f"universe with data: {close.shape[1]} stocks, {close.shape[0]} days\n")

    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    ud_mom = ud - ud.shift(10)                                  # rising U/D
    obv = (volume * d).cumsum()
    avgvol = volume.rolling(20).mean()
    obv_slope = (obv - obv.shift(20)) / (avgvol * 20)           # accumulation slope
    rvol = volume / avgvol

    signals = {
        "U/D level": zrow(np.log(ud.clip(lower=0.05))),
        "U/D momentum (rising)": zrow(ud_mom),
        "OBV slope": zrow(obv_slope),
        "Volume combo": 0.5 * zrow(ud_mom) + 0.3 * zrow(obv_slope) + 0.2 * zrow(rvol),
    }
    fwd = close.shift(-HOLD) / close - 1.0
    # forward MAX up-move over the next HOLD days -> "spike" capture
    fwd_high = close.shift(-1)
    for k in range(2, HOLD + 1):
        fwd_high = np.maximum(fwd_high, close.shift(-k))
    fwd_max = fwd_high / close - 1.0
    SPIKE = 0.06
    spike = fwd_max > SPIKE
    rebal = close.index[60:-HOLD:HOLD]

    def run(z):
        ls, lo, bench = [], [], []
        for t in rebal:
            zt = z.loc[t].dropna(); ft = fwd.loc[t]
            if len(zt) < 10:
                continue
            k = max(2, int(len(zt) * QTILE))
            top, bot = zt.nlargest(k).index, zt.nsmallest(k).index
            rtop, rbot, rall = ft[top].mean(), ft[bot].mean(), ft[zt.index].mean()
            if np.isnan(rtop) or np.isnan(rbot):
                continue
            ls.append(rtop - rbot); lo.append(rtop); bench.append(rall)
        return np.array(ls), np.array(lo), np.array(bench)

    def m(r):
        if len(r) == 0:
            return (float("nan"),) * 3
        tot = np.prod(1 + r) - 1
        yrs = len(r) * HOLD / 252
        cagr = ((1 + tot) ** (1 / yrs) - 1) if (yrs > 0 and 1 + tot > 0) else float("nan")
        sh = (r.mean() / r.std() * np.sqrt(252 / HOLD)) if r.std() > 0 else 0
        return cagr * 100, sh, (r > 0).mean() * 100

    print(f"VOLUME-CENTRIC swing backtest | hold {HOLD}d | top/bottom {int(QTILE*100)}% | ~2y")
    print("=" * 94)
    print(f"{'signal':<24}{'L-S CAGR':>10}{'L-S Sharpe':>12}{'L-S hit':>9}   |{'  LongOnly CAGR':>16}{'Sharpe':>9}{'hit':>7}")
    print("-" * 94)
    _, _, b = run(list(signals.values())[0]); bc, bs, bh = m(b)
    for name, z in signals.items():
        ls, lo, _ = run(z)
        lc, lsh, lh = m(ls); oc, osh, oh = m(lo)
        print(f"{name:<24}{lc:>9.1f}%{lsh:>12.2f}{lh:>8.1f}%   |{oc:>15.1f}%{osh:>9.2f}{oh:>6.1f}%")
    print("-" * 94)
    print(f"{'Benchmark (equal-wt)':<24}{'':>10}{'':>12}{'':>9}   |{bc:>15.1f}%{bs:>9.2f}{bh:>6.1f}%")

    print(f"\nSPIKE PREDICTION — does the signal concentrate forward >+{int(SPIKE*100)}% moves within {HOLD}d?")
    print("-" * 70)
    print(f"{'signal':<24}{'base spike rate':>16}{'P(spike|top)':>15}{'lift':>9}")
    for name, z in signals.items():
        bn = bk = cn = ck = 0
        for t in rebal:
            zt = z.loc[t].dropna(); st = spike.loc[t]
            valid = [i for i in zt.index if i in st.index and not pd.isna(fwd_max.loc[t, i])]
            if len(valid) < 10:
                continue
            bk += int(st[valid].sum()); bn += len(valid)
            k = max(2, int(len(valid) * QTILE))
            top = zt[valid].nlargest(k).index
            ck += int(st[top].sum()); cn += len(top)
        if bn and cn:
            base, cond = bk / bn, ck / cn
            print(f"{name:<24}{base*100:>15.1f}%{cond*100:>14.1f}%{(cond/base if base else 0):>8.2f}x")


if __name__ == "__main__":
    main()
