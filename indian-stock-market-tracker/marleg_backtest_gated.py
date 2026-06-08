"""
Gated TOP-DOWN swing backtest (AIQ / CAN SLIM style). Buy a stock only when ALL
gates fire together (confluence); hold HOLD days; equal-weight; cash when none fire.

Gates (all backtestable from price/volume):
  1. SECTOR/GROUP : stock's sector in the top 40% by 20-day relative strength
  2. VOLUME       : U/D ratio ABOVE its own 50-day average AND rising (vs 10d ago)
  3. FIBONACCI    : price above the 0.618 retracement of its 120-day range
Earnings (3rd CAN SLIM factor) needs point-in-time fundamentals not freely
available historically -> it stays a LIVE gate, not in this backtest.

  python marleg_backtest_gated.py        # 10-day swing
  python marleg_backtest_gated.py 5
"""
import sys, json, os
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_volume_scan as v

HOLD = int(sys.argv[1]) if len(sys.argv) > 1 else 10
U = v.SEED
HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))


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
    print(f"universe with data: {close.shape[1]} stocks\n")

    # Gate 1 — sector/group relative strength
    ret20 = close.pct_change(20)
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    members = {}
    for s, sec in secmap.items():
        members.setdefault(sec, []).append(s)
    sec_ret = pd.DataFrame({sec: ret20[m].mean(axis=1) for sec, m in members.items()})
    sec_rank = sec_ret.rank(axis=1, ascending=False, pct=True)            # 0 = leading
    stock_secrank = pd.DataFrame({s: sec_rank[secmap[s]] for s in close.columns})
    gate_sector = stock_secrank <= 0.40

    # Gate 2 — volume: U/D above its 50d MA and rising
    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    gate_vol = (ud > ud.rolling(50).mean()) & (ud > ud.shift(10))

    # Gate 3 — Fibonacci position: above the 0.618 retracement of the 120d range
    hh = close.rolling(120).max(); ll = close.rolling(120).min()
    fibpos = (close - ll) / (hh - ll).replace(0, np.nan)
    gate_fib = fibpos > 0.618

    longsig = gate_sector & gate_vol & gate_fib
    fwd = close.shift(-HOLD) / close - 1.0
    all_reb = list(close.index[120:-HOLD:HOLD])
    cutoff = close.index[-(252 + HOLD)] if len(close) > 252 + HOLD else close.index[120]
    rebal = [t for t in all_reb if t >= cutoff]          # score only the PAST ~1 YEAR

    gret, bench, npos = [], [], []
    for t in rebal:
        sel = longsig.loc[t]
        chosen = [s for s in sel.index if sel.get(s) and not pd.isna(fwd.loc[t, s])]
        bench.append(fwd.loc[t].dropna().mean())
        gret.append(fwd.loc[t, chosen].mean() if chosen else 0.0)   # no confluence -> cash
        npos.append(len(chosen))
    gret, bench, npos = np.array(gret), np.array(bench), np.array(npos)

    def stat(r, name):
        tot = np.prod(1 + r) - 1
        yrs = len(r) * HOLD / 252
        cagr = ((1 + tot) ** (1 / yrs) - 1) if (yrs > 0 and 1 + tot > 0) else float("nan")
        sh = (r.mean() / r.std() * np.sqrt(252 / HOLD)) if r.std() > 0 else 0
        print(f"  {name:<20} total {tot*100:6.1f}%   CAGR {cagr*100:6.1f}%   Sharpe {sh:5.2f}   "
              f"hit {(r>0).mean()*100:4.1f}%   avg/reb {r.mean()*100:5.2f}%")

    COST = 0.002                                          # ~0.1%/side round-trip on the rebalanced book
    net = np.where(npos > 0, gret - COST, gret)
    invested = (npos > 0).mean() * 100
    print(f"Gated top-down swing — PAST 1 YEAR | hold {HOLD}d | gates: sectorRS + U/D>MA&rising + Fib>0.618")
    print(f"{len(rebal)} rebalances | avg {npos.mean():.1f} positions/reb | invested {invested:.0f}% of the time | cost {COST*100:.1f}%/reb")
    print("-" * 86)
    stat(gret, "Gated (gross)"); stat(net, "Gated (net of cost)"); stat(bench, "Benchmark (EW)")
    # when actually invested, how do the gated picks do vs benchmark same day?
    inv = npos > 0
    if inv.sum() > 3:
        g, b = gret[inv], bench[inv]
        print(f"\n  When invested ({inv.sum()} rebs): gated avg {g.mean()*100:+.2f}%/reb  vs  benchmark {b.mean()*100:+.2f}%/reb  "
              f"-> edge {(g.mean()-b.mean())*100:+.2f}%")


if __name__ == "__main__":
    main()
