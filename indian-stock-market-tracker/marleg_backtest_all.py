"""
Marle-G — UNIFIED 5-year strategy scorecard. Backtests every strategy that has
real multi-year data on ONE common daily panel, head-to-head vs buy-&-hold, with
a proper scorecard (CAGR / Sharpe / max-drawdown / hit-rate / % time invested).

Equity strategies (daily, ~5y, non-overlapping HOLD-day rebalances, net of cost):
  - Buy & Hold (EW)            benchmark
  - Gated confluence           sectorRS top-40% AND U/D>50dMA&rising AND price>0.618 Fib  (the MTF-swing core)
  - Volume momentum            U/D>50dMA & rising, top decile  (the naive volume signal)
  - Low-RSI accumulation       RSI<45 AND price>50dMA AND U/D rising  (the validated entry barbell)
  - Price momentum             top-decile 60-day return
Options:
  - NIFTY vol structures       reuses marleg_option_structures (short-vol / VRP, 5y monthly)
  - Debit call spread on gated  1-month ATM spread opened on each gated-long signal (BS)

INTRADAY is NOT here: yfinance serves only ~60 days of 1-minute history, so a 5-year
intraday backtest is impossible on free data (would need a paid tick archive).

  python marleg_backtest_all.py            # ~5y, 10-day swing
  python marleg_backtest_all.py 5          # 5-day hold
"""
import sys, os, json
import numpy as np, pandas as pd, yfinance as yf
import marleg_volume_scan as v
import marleg_vol as mv
try:
    import marleg_option_structures as mstruct
except Exception:
    mstruct = None

HOLD = int(sys.argv[1]) if len(sys.argv) > 1 else 10
COST = 0.002
HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
U = v.SEED


def rsi(df, n=14):
    d = df.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def stat(r):
    r = np.asarray(r, float)
    tot = np.prod(1 + r) - 1
    yrs = len(r) * HOLD / 252
    cagr = ((1 + tot) ** (1 / yrs) - 1) if (yrs > 0 and 1 + tot > 0) else float("nan")
    sh = (r.mean() / r.std() * np.sqrt(252 / HOLD)) if r.std() > 0 else 0.0
    curve = np.cumprod(1 + r)
    dd = float((curve / np.maximum.accumulate(curve) - 1).min()) if len(curve) else 0.0
    return dict(cagr=cagr * 100, sharpe=sh, maxdd=dd * 100, hit=(r > 0).mean() * 100, tot=tot * 100)


def main():
    print(f"downloading {len(U)} symbols (5y daily)...")
    data = yf.download([s + ".NS" for s in U], period="5y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume = {}, {}
    for s in U:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 600:
                close[s] = c; volume[s] = data[t]["Volume"]
        except Exception:
            pass
    close = pd.DataFrame(close); volume = pd.DataFrame(volume).reindex(close.index)
    n_years = len(close) / 252
    print(f"universe with 5y data: {close.shape[1]} stocks | {len(close)} days (~{n_years:.1f}y)\n")

    ret20 = close.pct_change(20)
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    members = {}
    for s, sec in secmap.items():
        members.setdefault(sec, []).append(s)
    sec_ret = pd.DataFrame({sec: ret20[m].mean(axis=1) for sec, m in members.items()})
    sec_rank = sec_ret.rank(axis=1, ascending=False, pct=True)
    stock_secrank = pd.DataFrame({s: sec_rank[secmap[s]] for s in close.columns})
    gate_sector = stock_secrank <= 0.40

    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    gate_vol = (ud > ud.rolling(50).mean()) & (ud > ud.shift(10))

    hh = close.rolling(120).max(); ll = close.rolling(120).min()
    gate_fib = ((close - ll) / (hh - ll).replace(0, np.nan)) > 0.618

    R = rsi(close); sma50 = close.rolling(50).mean(); mom60 = close.pct_change(60)

    sig = {
        "Gated confluence":     gate_sector & gate_vol & gate_fib,
        "Volume momentum":      gate_vol & (ud.rank(axis=1, ascending=False, pct=True) <= 0.10),
        "Low-RSI accumulation": (R < 45) & (close > sma50) & (ud > ud.shift(10)),
        "Price momentum":       mom60.rank(axis=1, ascending=False, pct=True) <= 0.10,
    }
    fwd = close.shift(-HOLD) / close - 1.0
    rebal = list(close.index[120:-HOLD:HOLD])

    print(f"5-YEAR EQUITY SCORECARD | hold {HOLD}d | {len(rebal)} rebalances | net {COST*100:.1f}%/reb | EW, no slippage")
    print("-" * 96)
    print(f"  {'strategy':<24}{'CAGR':>8}{'Sharpe':>8}{'maxDD':>8}{'hit':>7}{'invested':>10}{'tot ret':>9}")
    # benchmark
    bench = np.array([fwd.loc[t].dropna().mean() for t in rebal])
    bs = stat(bench)
    print(f"  {'Buy & Hold (EW)':<24}{bs['cagr']:>7.1f}%{bs['sharpe']:>8.2f}{bs['maxdd']:>7.1f}%{bs['hit']:>6.0f}%{'100%':>10}{bs['tot']:>8.0f}%")
    results = {"Buy & Hold (EW)": bs}
    for name, mask in sig.items():
        rr, inv = [], 0
        for t in rebal:
            sel = mask.loc[t]
            chosen = [s for s in sel.index if sel.get(s) and not pd.isna(fwd.loc[t, s])]
            if chosen:
                rr.append(fwd.loc[t, chosen].mean() - COST); inv += 1
            else:
                rr.append(0.0)
        st = stat(rr); results[name] = st
        print(f"  {name:<24}{st['cagr']:>7.1f}%{st['sharpe']:>8.2f}{st['maxdd']:>7.1f}%{st['hit']:>6.0f}%{inv/len(rebal)*100:>9.0f}%{st['tot']:>8.0f}%")

    # ---- options: debit call spread opened on each gated-long signal (1-month BS sim) ----
    print("\n5-YEAR OPTIONS SCORECARD")
    print("-" * 96)
    H21 = 21
    fwd21 = close.shift(-H21) / close - 1.0
    rv = close.pct_change().rolling(20).std() * np.sqrt(252)
    gmask = sig["Gated confluence"]
    spread_r = []
    for t in close.index[120:-H21:H21]:
        names = [s for s in gmask.columns if gmask.loc[t].get(s) and not pd.isna(fwd21.loc[t, s])]
        if not names:
            continue
        per = []
        for s in names:
            S0 = close.loc[t, s]; sig0 = rv.loc[t, s]
            if not (S0 > 0 and sig0 > 0):
                continue
            T = H21 / 252.0; em = S0 * sig0 * np.sqrt(T)
            kL, kS = S0, S0 + em
            debit = mv.bs_price(S0, kL, T, mv.R_FREE, sig0, "C") - mv.bs_price(S0, kS, T, mv.R_FREE, sig0, "C")
            if debit <= 0.01:
                continue
            S1 = close.loc[t, s] * (1 + fwd21.loc[t, s])
            settle = max(0.0, min(S1, kS) - kL)
            per.append((settle - debit) / debit)           # return on premium
        if per:
            spread_r.append(float(np.mean(per)))
    if spread_r:
        ss = np.array(spread_r)
        yrs = len(ss) * H21 / 252
        cagr = ((np.prod(1 + ss)) ** (1 / yrs) - 1) * 100 if yrs > 0 and np.prod(1 + ss) > 0 else float("nan")
        print(f"  {'Debit spread on gated':<24}{cagr:>7.1f}%{'':>8}{'':>8}{(ss>0).mean()*100:>6.0f}%{'':>10}  avg {ss.mean()*100:+.1f}%/mo on premium ({len(ss)} cycles)")
    if mstruct:
        bt = mstruct.backtest_data(5)
        if not bt.get("error"):
            for r in bt["rows"]:
                if r["structure"] in ("iron_butterfly", "short_straddle", "long_straddle"):
                    print(f"  NIFTY {r['structure']:<18}{'':>8}{'':>8}{'':>8}{r['win']:>6.0f}%{'':>10}  avg Rs{r['avg']:+.0f}/unit/mo (VRP {bt['vrp']*100:+.1f}%)")

    print("\nINTRADAY: not backtestable 5y on free data (yfinance gives ~60d of 1-min bars). Paper-traded live instead.")
    print("Caveats: liquid-150 universe, EW, flat 0.2% cost (no slippage/impact), survivorship (current SEED names),")
    print("earnings gate omitted (live-only), options sim = ATM 1sd debit spread settled at expiry intrinsic.")


if __name__ == "__main__":
    main()
