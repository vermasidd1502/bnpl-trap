"""
marleg_rotation_bt.py — does CYCLICAL SECTOR ROTATION beat just holding the market (and is it better than
picking leaders)? Tests the user's idea directly on the canonical 5y panel.

Strategy: every ~month, rank the broad sectors by trailing 63-day relative strength and HOLD the top-K
(equal-weight), rebalance monthly. Compared to the equal-weight market (hold everything), a bull-gated
version (only deploy when the market is above its 50DMA, else cash), and an anti-momentum control
(hold the WORST sectors — to check it's momentum, not just being invested).

  python marleg_rotation_bt.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_rotation_bt.json")


def _metrics(daily):
    d = daily.dropna()
    if len(d) < 100:
        return None
    eq = (1 + d).cumprod()
    yrs = len(d) / 252
    cagr = float(eq.iloc[-1] ** (1 / yrs) - 1)
    vol = float(d.std() * np.sqrt(252))
    sharpe = float((d.mean() * 252) / vol) if vol else None
    dd = float((eq / eq.cummax() - 1).min())
    return {"cagr": round(cagr * 100, 1), "vol": round(vol * 100, 1),
            "sharpe": round(sharpe, 2) if sharpe else None, "maxdd": round(dd * 100, 1)}


def _run(secret, top=True, K=3, bull_gate=False, reb=21, look=63):
    idx = secret.index
    mkt_eq = (1 + secret.mean(axis=1)).cumprod()
    dma = mkt_eq.rolling(50).mean()
    port = pd.Series(np.nan, index=idx)
    hold = []
    for i, date in enumerate(idx):
        if i >= look and i % reb == 0:
            mom = (1 + secret.iloc[i - look:i]).prod() - 1
            mom = mom.dropna().sort_values(ascending=not top)
            hold = list(mom.index[:K])
        if not hold or i < look:
            continue
        if bull_gate and not (mkt_eq.iloc[i] > dma.iloc[i] if dma.iloc[i] == dma.iloc[i] else True):
            port.iloc[i] = 0.0                       # cash when market below 50DMA
        else:
            port.iloc[i] = float(secret.iloc[i][hold].mean())
    return port


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
    rets = close.pct_change()
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    bysec = {}
    for s in close.columns:
        sec = SECT.get(s, {}).get("sector") or "Other"
        bysec.setdefault(sec, []).append(s)
    bysec = {k: v for k, v in bysec.items() if len(v) >= 4}
    secret = pd.DataFrame({k: rets[v].mean(axis=1) for k, v in bysec.items()}).dropna(how="all")

    mkt = secret.mean(axis=1)
    res = {"n_sectors": secret.shape[1], "days": int(secret.shape[0]),
           "market_equalweight": _metrics(mkt),
           "rotate_top3": _metrics(_run(secret, top=True, K=3)),
           "rotate_top5": _metrics(_run(secret, top=True, K=5)),
           "rotate_top3_bullgated": _metrics(_run(secret, top=True, K=3, bull_gate=True)),
           "rotate_BOTTOM3 (control)": _metrics(_run(secret, top=False, K=3))}
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"sector-rotation backtest — {res['n_sectors']} sectors, {res['days']} days (~5y)\n")
    print(f"  {'strategy':<28}{'CAGR%':>8}{'vol%':>8}{'Sharpe':>8}{'maxDD%':>9}")
    for k in ["market_equalweight", "rotate_top3", "rotate_top5", "rotate_top3_bullgated", "rotate_BOTTOM3 (control)"]:
        m = res[k]
        if m:
            print(f"  {k:<28}{m['cagr']:>8}{m['vol']:>8}{str(m['sharpe']):>8}{m['maxdd']:>9}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
