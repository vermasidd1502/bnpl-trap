"""
marleg_short_gate_study.py — does a GATED SHORT (the inverse confluence) actually pay?

Long gate  (current): leading sector (top-40% 20d RS) + U/D > 50d-MA & rising + fib > 0.618
Short gate (inverse) : lagging sector (bottom-40% 20d RS) + U/D < 50d-MA & falling + fib < 0.382

For every stock/day that fires the short gate we record the forward return at H=5/10/20.
A short PROFITS only if the stock falls more than the (higher) short cost, so we report the
SHORT net P&L = -fwd_return - SHORT_COST. The long gate is run alongside as a sanity baseline
(it should be clearly positive — validating the method). Pooled across the liquid universe, 3y.

Caveat: yfinance = survivors -> survivorship LIFTS returns, which is *adverse* to a short
thesis (real delisted names fell hard and are missing). So if shorts look bad here, they're
even worse live; if they look good, treat with caution.

  python marleg_short_gate_study.py
"""
import json, os, sys
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_volume_scan as mvs

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_short_gate_study.json")
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
SHORT_COST = 0.50      # % round-trip for an intraday/MIS short (brokerage+STT+slippage, higher than long)
LONG_COST = 0.30
HORIZONS = [5, 10, 20]


def _stats(x, cost, side):
    x = x[~np.isnan(x)]
    if len(x) < 30:
        return None
    fwd = float(np.mean(x)) * 100
    win_long = float(np.mean(x > 0)) * 100
    if side == "short":
        net = -fwd - cost                       # short gains when the stock falls
        win = float(np.mean(x < 0)) * 100       # a short "wins" when fwd return < 0
    else:
        net = fwd - cost
        win = win_long
    return {"n": int(len(x)), "fwd_mean_pct": round(fwd, 3),
            "net_pnl_pct": round(net, 3), "win_pct": round(win, 1)}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    U = mvs.load_universe()
    print(f"downloading {len(U)} symbols (3y daily) for short-gate backtest...")
    close, volume = {}, {}
    CH = 200
    for i in range(0, len(U), CH):
        chunk = U[i:i + CH]
        try:
            data = yf.download([s + ".NS" for s in chunk], period="3y", interval="1d",
                               group_by="ticker", progress=False, threads=True)
        except Exception:
            continue
        for s in chunk:
            try:
                c = data[s + ".NS"]["Close"].dropna()
                v = data[s + ".NS"]["Volume"]
            except Exception:
                continue
            if len(c) > 200:
                close[s] = c; volume[s] = v
        print(f"  {min(i + CH, len(U))}/{len(U)} (kept {len(close)})")

    close = pd.DataFrame(close)
    volume = pd.DataFrame(volume).reindex(close.index)
    print(f"panel: {close.shape[1]} stocks x {close.shape[0]} days")

    # sector relative-strength rank per date (cross-sectional)
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    members = {}
    for s, sec in secmap.items():
        members.setdefault(sec, []).append(s)
    ret20 = close.pct_change(20)
    sec_ret = pd.DataFrame({sec: ret20[m].mean(axis=1) for sec, m in members.items()})
    sec_rank = sec_ret.rank(axis=1, ascending=False, pct=True)        # dates x sectors (0=best)
    stock_sec_rank = pd.DataFrame({s: sec_rank[secmap[s]] for s in close.columns})

    # U/D ratio + MA + trend
    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    ud_ma = ud.rolling(50).mean(); ud_10 = ud.shift(10)
    hh = close.rolling(120).max(); ll = close.rolling(120).min()
    fib = (close - ll) / (hh - ll).replace(0, np.nan)

    lead = stock_sec_rank <= 0.40
    lag = stock_sec_rank >= 0.60
    strong_vol = (ud > ud_ma) & (ud > ud_10)
    weak_vol = (ud < ud_ma) & (ud < ud_10)
    long_gate = lead & strong_vol & (fib > 0.618)
    short_gate = lag & weak_vol & (fib < 0.382)

    result = {"short_cost_pct": SHORT_COST, "long_cost_pct": LONG_COST, "horizons": HORIZONS,
              "panel_stocks": int(close.shape[1]), "short_gate": {}, "long_gate_baseline": {},
              "all_stocks_baseline": {}}
    for h in HORIZONS:
        fwd = close.shift(-h) / close - 1.0
        sg = fwd.values[short_gate.values]; lg = fwd.values[long_gate.values]; al = fwd.values.ravel()
        result["short_gate"][h] = _stats(sg, SHORT_COST, "short")
        result["long_gate_baseline"][h] = _stats(lg, LONG_COST, "long")
        result["all_stocks_baseline"][h] = _stats(al, SHORT_COST, "short")

    json.dump(result, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("\n================ GATED SHORT vs baselines ================")
    print(f"(short net = -forward_return - {SHORT_COST}% cost; a short WINS when the stock falls)\n")
    for h in HORIZONS:
        s = result["short_gate"][h]; l = result["long_gate_baseline"][h]
        print(f"  --- forward {h} days ---")
        if s:
            print(f"    SHORT-GATE   n={s['n']:>7,}  stock fwd {s['fwd_mean_pct']:+}%  -> SHORT net {s['net_pnl_pct']:+}%  win {s['win_pct']}%")
        if l:
            print(f"    LONG-GATE    n={l['n']:>7,}  stock fwd {l['fwd_mean_pct']:+}%  -> LONG  net {l['net_pnl_pct']:+}%  win {l['win_pct']}%  (sanity)")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
