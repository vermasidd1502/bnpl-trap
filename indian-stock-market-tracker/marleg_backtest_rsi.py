"""
Does RSI position matter for the volume-pod LONG signal?
Hypothesis (user): among accumulating names (volume turning up), prefer LOW RSI —
you're early (Wyckoff accumulation) rather than chasing an extended move.

Take the SAME long gate (sector top40 RS + U/D>50dMA & rising + Fib>0.618) and split
the picks into RSI buckets, then compare forward returns TRAIN vs TEST (out-of-sample).

  python marleg_backtest_rsi.py        # 10-day swing
"""
import sys, os, json
import numpy as np, pandas as pd, yfinance as yf
import marleg_volume_scan as v

HOLD = int(sys.argv[1]) if len(sys.argv) > 1 else 10
U = v.SEED
HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))


def stat(r, n):
    r = np.asarray(r)
    if len(r) == 0: return f"  {n:<26} (no picks)"
    yrs = max(1e-9, len(r) * HOLD / 252); tot = np.prod(1 + r) - 1
    cagr = ((1 + tot) ** (1 / yrs) - 1) if 1 + tot > 0 else float("nan")
    sh = (r.mean() / r.std() * np.sqrt(252 / HOLD)) if r.std() > 0 else 0.0
    return f"  {n:<26} avg/reb {r.mean()*100:+5.2f}%   Sharpe {sh:5.2f}   hit {(r>0).mean()*100:4.1f}%   CAGR {cagr*100:6.1f}%   n_obs {len(r)}"


def main():
    print(f"downloading {len(U)} symbols (2y) ...")
    data = yf.download([s + ".NS" for s in U], period="2y", interval="1d", group_by="ticker", progress=False, threads=True)
    close, volume = {}, {}
    for s in U:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 300: close[s] = c; volume[s] = data[t]["Volume"]
        except Exception: pass
    close = pd.DataFrame(close); volume = pd.DataFrame(volume).reindex(close.index)
    print(f"universe: {close.shape[1]} stocks\n")

    ret20 = close.pct_change(20, fill_method=None)
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    members = {}
    for s, sec in secmap.items(): members.setdefault(sec, []).append(s)
    sec_ret = pd.DataFrame({sec: ret20[m].mean(axis=1) for sec, m in members.items()})
    srank = pd.DataFrame({s: sec_ret.rank(axis=1, ascending=False, pct=True)[secmap[s]] for s in close.columns})

    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    hh = close.rolling(120).max(); ll = close.rolling(120).min()
    fib = (close - ll) / (hh - ll).replace(0, np.nan)
    longsig = (srank <= 0.40) & (ud > ud.rolling(50).mean()) & (ud > ud.shift(10)) & (fib > 0.618)

    # RSI(14) vectorised
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean(); loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    fwd = close.shift(-HOLD) / close - 1.0
    rebals = list(close.index[120:-HOLD:HOLD]); split = int(len(rebals) * 0.6)
    segs = {"TRAIN": rebals[:split], "TEST (out-of-sample)": rebals[split:]}
    print(f"hold {HOLD}d | {len(rebals)} rebalances | TRAIN {split} / TEST {len(rebals)-split}\n")

    # buckets within the accumulation long signal
    buckets = [("all longs (any RSI)", lambda R: R == R),
               ("LOW  RSI < 45  (early)", lambda R: R < 45),
               ("MID  RSI 45-60", lambda R: (R >= 45) & (R < 60)),
               ("HIGH RSI 60-70", lambda R: (R >= 60) & (R < 70)),
               ("OVERBOUGHT RSI >= 70", lambda R: R >= 70)]

    for seg, dates in segs.items():
        print(f"=== {seg} ({len(dates)} rebalances) ===")
        # benchmark
        bench = np.array([fwd.loc[t].dropna().mean() for t in dates])
        print(stat(bench, "Benchmark (EW all)"))
        for label, cond in buckets:
            rets = []
            for t in dates:
                sel = longsig.loc[t] & cond(rsi.loc[t])
                names = [s for s in close.columns if sel.get(s) and not pd.isna(fwd.loc[t, s])]
                if names: rets.append(fwd.loc[t, names].mean())
            print(stat(rets, label))
        print()

    # current accumulation longs by RSI
    t = close.index[-1]
    cur = [(s, round(float(rsi.loc[t, s]), 1), round(float(ud.loc[t, s]), 2), round(float(fib.loc[t, s]), 2), round(float(close.loc[t, s]), 1))
           for s in close.columns if longsig.loc[t, s]]
    cur.sort(key=lambda x: x[1])  # lowest RSI first = earliest
    print(f"CURRENT accumulation longs ranked by RSI (lowest = earliest entry), as of {t.date()}:")
    for s, r, u, f, p in cur:
        tag = "EARLY" if r < 45 else "building" if r < 60 else "extended" if r < 70 else "OVERBOUGHT/chasing"
        print(f"   {s:<12} RSI {r:>5}  [{tag}]   U/D {u}  Fib {f}  Rs{p}")


if __name__ == "__main__":
    main()
