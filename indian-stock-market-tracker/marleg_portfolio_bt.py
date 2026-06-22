"""
marleg_portfolio_bt.py — cross-sectional PORTFOLIO backtests: LONG / SHORT / LONG-SHORT for every factor +
multi-factor composites, net of realistic Indian costs, vs an equal-weight benchmark. Complements
marleg_strat_lab.py (which tests long-only signal models) by adding the SIDE dimension the user asked for.

Method: at each rebalance date rank the universe by the signal, equal-weight the top quintile (long) and/or
bottom quintile (short), hold `horizon` sessions, record the portfolio's forward return; chain → equity →
annualised return, Sharpe, max-DD, win-rate. Costs = ~0.15%/leg per rebalance (brokerage+STT+slippage proxy),
so weekly rebalances carry far more drag than monthly. Survivorship: long-leg mildly optimistic on the
current panel; the long-SHORT (market-neutral) and short reads are the more trustworthy. Read-only.

  python marleg_portfolio_bt.py
"""
import sys
import json
import os
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_portfolio_bt.json")


def _zx(df):
    return df.sub(df.mean(axis=1), axis=0).div(df.std(axis=1) + 1e-9, axis=0)


def _signals():
    import marleg_factors as fa
    C, f = fa._factor_panels()
    f["composite_lowvol_strength_liq"] = _zx(f["low_vol"]) + _zx(f["dist_52w_high"]) + _zx(f["liquidity"])
    f["composite_mom_lowvol"] = _zx(f["momentum_6m"]) + _zx(f["low_vol"])
    return C, f


def _metrics(rets, ppy):
    if not rets:
        return None
    a = np.array(rets, float)
    eq = np.cumprod(1 + a)
    dd = eq / np.maximum.accumulate(eq) - 1
    ann = (float(eq[-1]) ** (ppy / len(a)) - 1) if eq[-1] > 0 else -1.0
    sharpe = float(a.mean() / (a.std() + 1e-9) * np.sqrt(ppy))
    return {"ann_ret_pct": round(ann * 100, 1), "sharpe": round(sharpe, 2),
            "win_pct": round(float((a > 0).mean()) * 100, 1), "maxdd_pct": round(float(dd.min()) * 100, 1),
            "mean_per_pct": round(float(a.mean()) * 100, 2), "n_periods": len(a)}


def _bt(C, sig, side, horizon, step, q=5, cost=0.0015, window=520):
    n = len(C.index)
    start = max(252, n - window)
    rets = []
    for i in range(start, n - horizon, step):
        fwd = C.shift(-horizon).iloc[i] / C.iloc[i] - 1
        if side == "bench":
            v = fwd.dropna()
            if len(v) >= 50:
                rets.append(float(v.mean()))
            continue
        d = pd.concat([sig.iloc[i], fwd], axis=1).dropna()
        d.columns = ["f", "r"]
        if len(d) < 50:
            continue
        try:
            qq = pd.qcut(d["f"], q, labels=False, duplicates="drop")
        except Exception:
            continue
        top, bot = d["r"][qq == qq.max()].mean(), d["r"][qq == 0].mean()
        if side == "long":
            r, legs = top, 1
        elif side == "short":
            r, legs = -bot, 1
        else:
            r, legs = top - bot, 2
        rets.append(float(r) - cost * legs)
    return rets


def run(window=520):
    C, sigs = _signals()
    horizons = [(21, 21, 12, "monthly"), (5, 5, 50, "weekly")]
    rows = []
    for h, step, ppy, lbl in horizons:
        m = _metrics(_bt(C, None, "bench", h, step, window=window), ppy)
        if m:
            rows.append({"strategy": "BENCHMARK equal-weight", "signal": "—", "side": "long", "horizon": lbl, **m})
    for name, sig in sigs.items():
        for h, step, ppy, lbl in horizons:
            for side in ("long", "short", "ls"):
                m = _metrics(_bt(C, sig, side, h, step, window=window), ppy)
                if m:
                    rows.append({"strategy": f"{name} · {('long-short' if side=='ls' else side)}",
                                 "signal": name, "side": side, "horizon": lbl, **m})
    rows.sort(key=lambda x: -x["sharpe"])
    from datetime import datetime, timezone, timedelta
    ist = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")
    out = {"ok": True, "asof": ist, "window_days": window, "n_strategies": len(rows), "leaderboard": rows,
           "note": "Net of ~0.15%/leg per rebalance (Indian cost proxy). Sharpe annualised. Survivorship: long-leg "
                   "mildly optimistic; the long-SHORT (market-neutral) + short reads are the more trustworthy. "
                   "Decision-support, not advice."}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    return out


def _market_regime(C):
    """Bull/bear at each date = sign of the market's trailing 50d return (market = mean cross-sectional)."""
    ret = C.pct_change(fill_method=None).mean(axis=1)
    mkt = (1 + ret).cumprod()
    return mkt / mkt.shift(50) - 1


def run_regime(window=520):
    """Top strategies split by NIFTY-ish bull/bear regime — does the edge survive both?"""
    C, sigs = _signals()
    reg = _market_regime(C)
    h, step, ppy = 21, 21, 12
    picks = [("dist_52w_high", "long"), ("composite_mom_lowvol", "ls"), ("low_vol", "ls"),
             ("composite_lowvol_strength_liq", "ls"), ("momentum_6m", "long"), ("reversal_5d", "short")]
    n = len(C.index); start = max(252, n - window)
    out = []
    for name, side in picks:
        sig = sigs[name]; bull, bear = [], []
        for i in range(start, n - h, step):
            fwd = C.shift(-h).iloc[i] / C.iloc[i] - 1
            d = pd.concat([sig.iloc[i], fwd], axis=1).dropna(); d.columns = ["f", "r"]
            if len(d) < 50:
                continue
            try:
                qq = pd.qcut(d["f"], 5, labels=False, duplicates="drop")
            except Exception:
                continue
            top, bot = d["r"][qq == qq.max()].mean(), d["r"][qq == 0].mean()
            r = top if side == "long" else (-bot if side == "short" else top - bot)
            r -= 0.0015 * (2 if side == "ls" else 1)
            rr = reg.iloc[i]
            (bull if (pd.notna(rr) and rr > 0) else bear).append(float(r))
        out.append({"strategy": f"{name} · {('long-short' if side=='ls' else side)}",
                    "bull": _metrics(bull, ppy), "bear": _metrics(bear, ppy)})
    return out


def run_intraday(window=520, cost=0.0008):
    """Intraday (open→close) long-strong / short-weak, ranked on yesterday's return (known at the open).
    Daily rebalance → heavy turnover; result is indicative (illiquid names inflate gross). """
    import marleg_panel_build as pb
    P = pb.load(); O, C = P["open"], P["close"]
    ret1 = C.pct_change(fill_method=None)
    n = len(C.index); start = max(60, n - window)
    res = {}
    for side in ("long_strong_intraday", "short_weak_intraday"):
        rets = []
        for i in range(start, n):
            sig = ret1.iloc[i - 1]
            idr = C.iloc[i] / O.iloc[i] - 1
            d = pd.concat([sig, idr], axis=1).dropna(); d.columns = ["f", "r"]
            if len(d) < 50:
                continue
            try:
                qq = pd.qcut(d["f"], 5, labels=False, duplicates="drop")
            except Exception:
                continue
            top, bot = d["r"][qq == qq.max()].mean(), d["r"][qq == 0].mean()
            r = (top if side == "long_strong_intraday" else -bot) - cost
            rets.append(float(r))
        res[side] = _metrics(rets, 250)
    return res


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = run()
    print(f"\n  PORTFOLIO BACKTEST LAB — {r['n_strategies']} strategies · net of costs · {r['asof']}")
    print(f"  {'#':>2} {'strategy':<44}{'horizon':>9}{'Sharpe':>8}{'annRet':>8}{'maxDD':>8}{'win':>6}")
    for i, x in enumerate(r["leaderboard"][:22], 1):
        print(f"  {i:>2} {x['strategy']:<44}{x['horizon']:>9}{x['sharpe']:>8.2f}{x['ann_ret_pct']:>7.1f}%{x['maxdd_pct']:>7.1f}%{x['win_pct']:>5.0f}%")
    print("\n  worst 6:")
    for x in r["leaderboard"][-6:]:
        print(f"     {x['strategy']:<44}{x['horizon']:>9}{x['sharpe']:>8.2f}{x['ann_ret_pct']:>7.1f}%")
    print(f"\n  {r['note']}")
