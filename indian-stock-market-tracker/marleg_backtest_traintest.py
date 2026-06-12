"""
Train / test (in-sample / out-of-sample) backtest of the volume-pod LONG & SHORT
signals — the honest validation: does the edge found in the TRAIN window survive,
untouched, in the held-out TEST window? (If it only works in-sample it was luck —
Bailey & Lopez de Prado, backtest overfitting.)

Rule is FIXED and economically motivated (no per-run tuning), evaluated on a
chronological split. Both sides come from the same volume-pod gates:

  LONG  : sector in top 40% by 20d relative strength
          AND U/D ratio above its 50d MA AND rising (vs 10d ago)        [accumulation]
          AND price above the 0.618 Fib of its 120d range              [strong position]
  SHORT : sector in bottom 40% by 20d relative strength
          AND U/D ratio below its 50d MA AND falling (vs 10d ago)       [distribution]
          AND price below the 0.382 Fib of its 120d range              [weak position]

  python marleg_backtest_traintest.py        # 10-day swing
  python marleg_backtest_traintest.py 5      # 5-day swing
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
COST = 0.002  # ~0.1%/side round-trip


def seg_stat(r, npos, name, hold):
    r = np.asarray(r); inv = np.asarray(npos) > 0
    tot = np.prod(1 + r) - 1
    yrs = max(1e-9, len(r) * hold / 252)
    cagr = ((1 + tot) ** (1 / yrs) - 1) if 1 + tot > 0 else float("nan")
    sh = (r.mean() / r.std() * np.sqrt(252 / hold)) if r.std() > 0 else 0.0
    print(f"  {name:<22} CAGR {cagr*100:6.1f}%   Sharpe {sh:5.2f}   hit {(r>0).mean()*100:4.1f}%   "
          f"avg/reb {r.mean()*100:+5.2f}%   invested {inv.mean()*100:3.0f}%")
    return {"cagr": cagr, "sharpe": sh, "hit": float((r > 0).mean()), "avg": float(r.mean())}


def main():
    print(f"downloading {len(U)} symbols (2y) ...")
    data = yf.download([s + ".NS" for s in U], period="2y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume = {}, {}
    for s in U:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 300:
                close[s] = c; volume[s] = data[t]["Volume"]
        except Exception:
            pass
    close = pd.DataFrame(close); volume = pd.DataFrame(volume).reindex(close.index)
    print(f"universe with clean data: {close.shape[1]} stocks over {close.shape[0]} sessions\n")

    # ---- gates (computed once over full history; split happens on rebalance dates) ----
    ret20 = close.pct_change(20)
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    members = {}
    for s, sec in secmap.items():
        members.setdefault(sec, []).append(s)
    sec_ret = pd.DataFrame({sec: ret20[m].mean(axis=1) for sec, m in members.items()})
    sec_rank = sec_ret.rank(axis=1, ascending=False, pct=True)               # 0 = leading sector
    srank = pd.DataFrame({s: sec_rank[secmap[s]] for s in close.columns})

    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    ud_ma = ud.rolling(50).mean()

    hh = close.rolling(120).max(); ll = close.rolling(120).min()
    fib = (close - ll) / (hh - ll).replace(0, np.nan)

    longsig  = (srank <= 0.40) & (ud > ud_ma) & (ud > ud.shift(10)) & (fib > 0.618)
    shortsig = (srank >= 0.60) & (ud < ud_ma) & (ud < ud.shift(10)) & (fib < 0.382)

    fwd = close.shift(-HOLD) / close - 1.0
    rebals = list(close.index[120:-HOLD:HOLD])
    split = int(len(rebals) * 0.6)
    segs = {"TRAIN (in-sample)": rebals[:split], "TEST (out-of-sample)": rebals[split:]}
    print(f"hold {HOLD}d | {len(rebals)} rebalances | TRAIN {split} / TEST {len(rebals)-split} "
          f"| split {rebals[split].date()}\n")

    summary = {}
    for seg, dates in segs.items():
        L, S, LS, B, nL, nS = [], [], [], [], [], []
        for t in dates:
            f = fwd.loc[t]
            lo = [s for s in close.columns if longsig.loc[t, s] and not pd.isna(f[s])]
            sh = [s for s in close.columns if shortsig.loc[t, s] and not pd.isna(f[s])]
            lret = f[lo].mean() if lo else 0.0          # cash when no confluence
            sret = -f[sh].mean() if sh else 0.0         # short book: profit when names fall
            L.append(lret); S.append(sret); LS.append(lret + sret)
            B.append(f.dropna().mean()); nL.append(len(lo)); nS.append(len(sh))
        nLa, nSa = np.array(nL), np.array(nS)
        Ln = np.where(nLa > 0, np.array(L) - COST, 0.0)
        Sn = np.where(nSa > 0, np.array(S) - COST, 0.0)
        print(f"=== {seg} | {len(dates)} rebalances | avg {nLa.mean():.1f} longs / {nSa.mean():.1f} shorts per reb ===")
        sl = seg_stat(Ln, nLa, "LONG basket (net)", HOLD)
        ss = seg_stat(Sn, nSa, "SHORT basket (net)", HOLD)
        seg_stat(np.array(LS), (nLa + nSa), "LONG-SHORT (gross)", HOLD)
        sb = seg_stat(np.array(B), np.ones_like(B), "Benchmark (EW)", HOLD)
        edge_l = (np.array(L)[nLa > 0].mean() - np.array(B)[nLa > 0].mean()) * 100 if (nLa > 0).sum() else float("nan")
        print(f"  -> LONG edge vs benchmark when invested: {edge_l:+.2f}%/reb\n")
        summary[seg] = {"long": sl, "short": ss, "bench": sb, "edge_long": edge_l}

    # ---- verdict: does the edge survive out-of-sample? ----
    tr, te = summary["TRAIN (in-sample)"], summary["TEST (out-of-sample)"]
    print("=" * 70)
    print("OUT-OF-SAMPLE CHECK (the only number that matters):")
    print(f"  LONG  edge   train {tr['edge_long']:+.2f}%/reb  ->  test {te['edge_long']:+.2f}%/reb")
    print(f"  LONG  Sharpe train {tr['long']['sharpe']:.2f}      ->  test {te['long']['sharpe']:.2f}")
    print(f"  SHORT Sharpe train {tr['short']['sharpe']:.2f}      ->  test {te['short']['sharpe']:.2f}")
    holds = te["long"]["sharpe"] > 0.3 and te["edge_long"] > 0
    print(f"  VERDICT: long edge {'SURVIVES out-of-sample (plausible)' if holds else 'does NOT clearly survive — treat as weak/overfit'}")

    # ---- current plausible long & short interest from the volume pod ----
    t = close.index[-1]
    def picks(sig, side):
        rows = []
        for s in close.columns:
            if sig.loc[t, s]:
                rows.append({"s": s, "name": SECT.get(s, {}).get("name", s),
                             "sector": secmap[s], "ud": round(float(ud.loc[t, s]), 2),
                             "ud_ma": round(float(ud_ma.loc[t, s]), 2), "fib": round(float(fib.loc[t, s]), 2),
                             "sec_rank_pct": round(float(srank.loc[t, s]) * 100), "price": round(float(close.loc[t, s]), 1)})
        rows.sort(key=lambda r: (-r["ud"]) if side == "LONG" else (r["ud"]))
        return rows
    longs, shorts = picks(longsig, "LONG"), picks(shortsig, "SHORT")
    print("\n" + "=" * 70)
    print(f"CURRENT VOLUME-POD INTEREST (as of {t.date()})")
    print(f"\nLONG interest ({len(longs)}) — accumulation in leading sectors, above 0.618 Fib:")
    for r in longs[:12]:
        print(f"   {r['s']:<12} {r['sector'][:22]:<22} U/D {r['ud']:>4} (MA {r['ud_ma']:>4})  Fib {r['fib']:>4}  secRS {r['sec_rank_pct']}%  Rs{r['price']}")
    print(f"\nSHORT interest ({len(shorts)}) — distribution in lagging sectors, below 0.382 Fib:")
    for r in shorts[:12]:
        print(f"   {r['s']:<12} {r['sector'][:22]:<22} U/D {r['ud']:>4} (MA {r['ud_ma']:>4})  Fib {r['fib']:>4}  secRS {r['sec_rank_pct']}%  Rs{r['price']}")

    json.dump({"asof": str(t.date()), "hold": HOLD, "summary": summary,
               "longs": longs, "shorts": shorts},
              open(os.path.join(HERE, "marleg_traintest_cache.json"), "w"), default=str, indent=1)


if __name__ == "__main__":
    main()
