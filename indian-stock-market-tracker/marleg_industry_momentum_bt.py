"""
marleg_industry_momentum_bt.py — does INDUSTRY-MOMENTUM ROTATION pay in India?

Academic basis:
  • Moskowitz & Grinblatt (1999, JF) "Do Industries Explain Momentum?" — buying past-winning
    industries (and the diversified basket within them) captures most of stock momentum with far
    less idiosyncratic / volatility risk than chasing individual winners.
  • Jegadeesh & Titman (1993) — momentum with a SKIP month (rank on t-12..t-2, skip t-1 to dodge
    the 1-month reversal). We use 6-mo formation, skip 1-mo.
  • Moreira & Muir (2017, JF) "Volatility-Managed Portfolios" — scale exposure down when recent
    realized vol is high → higher Sharpe. Tested as a variant (the fix for "volatile names hurt us").

Method (walk-forward, long-only — India shorting never pays in our edges): build an equal-weight
daily index per industry (>=3 members with data) from the taxonomy, then on each rebalance rank
industries by 6-mo skip-1mo return, hold the top-K equal-weight until the next rebalance, net of
turnover cost. Benchmark = equal-weight ALL industries (own-everything). Reports CAGR/vol/Sharpe/maxDD.

Writes marleg_industry_momentum_bt.json. Research artifact (gitignored).
"""
import os
import sys
import json
import numpy as np
import pandas as pd
import marleg_data as md
import marleg_industry_rs as mir

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_industry_momentum_bt.json")
FORM, SKIP = 126, 21          # 6-mo formation, skip the most recent 1-mo (Jegadeesh-Titman)
MIN_MEM = 3
TARGET_VOL = 0.15             # 15% annualized, for the vol-managed variant
COST = 0.0010                 # 10 bps per unit of rebalance turnover


def metrics(r):
    r = r.dropna()
    if len(r) < 60:
        return {"n": len(r)}
    cum = float((1 + r).prod())
    yrs = len(r) / 252.0
    cagr = cum ** (1 / yrs) - 1 if cum > 0 else -1
    vol = float(r.std() * np.sqrt(252))
    sharpe = float(r.mean() * 252 / vol) if vol else 0.0
    eq = (1 + r).cumprod()
    dd = float((eq / eq.cummax() - 1).min())
    return {"n": len(r), "cagr_pct": round(cagr * 100, 1), "vol_pct": round(vol * 100, 1),
            "sharpe": round(sharpe, 2), "maxdd_pct": round(dd * 100, 1)}


def main():
    tax = mir._tax()
    syms = list(tax["by_symbol"].keys())
    print(f"loading {len(syms)} taxonomy names (3y) via marleg_data...")
    panel = md.daily_panel(syms, period="3y", groww_days=600, groww_cap=450)
    close = panel["close"]
    print(f"panel: {close.shape[1]} names x {close.shape[0]} days (source: {panel.get('source')})")
    if close.shape[0] < 220 or close.shape[1] < 80:
        print("insufficient history for a rotation backtest (need ~1y+ and enough names)."); return

    rets = close.pct_change().fillna(0.0)
    ind_of = {s: (tax["by_symbol"].get(s) or {}).get("industry") for s in close.columns}
    members = {}
    for s, ind in ind_of.items():
        if ind:
            members.setdefault(ind, []).append(s)
    members = {k: v for k, v in members.items() if len(v) >= MIN_MEM}
    ind_ret = pd.DataFrame({k: rets[v].mean(axis=1) for k, v in members.items()})
    ind_cum = (1 + ind_ret).cumprod()
    print(f"industries with >={MIN_MEM} members: {ind_ret.shape[1]}")

    def run(cadence, K, volm=False):
        idx = list(range(FORM + SKIP, len(ind_ret), cadence))
        strat = pd.Series(0.0, index=ind_ret.index)
        prev = set()
        for a in range(len(idx)):
            t = idx[a]
            t2 = idx[a + 1] if a + 1 < len(idx) else len(ind_ret)
            form = ind_cum.iloc[t - SKIP] / ind_cum.iloc[t - FORM - SKIP] - 1
            top = set(form.dropna().nlargest(K).index)
            if not top:
                continue
            seg = ind_ret[list(top)].iloc[t:t2].mean(axis=1)
            strat.iloc[t:t2] = seg.values
            turn = (len(top.symmetric_difference(prev)) / (2 * K)) if prev else 1.0
            strat.iloc[t] -= COST * turn
            prev = top
        if volm:
            rv = strat.rolling(21).std() * np.sqrt(252)
            scale = (TARGET_VOL / rv).clip(0, 1.5).shift(1).fillna(1.0)
            strat = strat * scale
        return strat.iloc[FORM + SKIP:]

    bench = ind_ret.mean(axis=1).iloc[FORM + SKIP:]
    variants = {
        "benchmark_EW_all_industries": bench,
        "rotate_monthly_top6": run(21, 6),
        "rotate_monthly_top8": run(21, 8),
        "rotate_weekly_top6": run(5, 6),
        "rotate_monthly_top6_VOLMANAGED": run(21, 6, volm=True),
        "rotate_weekly_top6_VOLMANAGED": run(5, 6, volm=True),
    }
    res = {k: metrics(v) for k, v in variants.items()}
    payload = {"panel_names": close.shape[1], "panel_days": close.shape[0], "source": panel.get("source"),
               "industries": ind_ret.shape[1], "form": FORM, "skip": SKIP, "cost_bps": COST * 1e4,
               "results": res}
    json.dump(payload, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"\n{'variant':<36} {'CAGR':>7} {'vol':>6} {'Sharpe':>7} {'maxDD':>7}")
    for k, m in res.items():
        if m.get("sharpe") is not None and "cagr_pct" in m:
            print(f"  {k:<34} {str(m['cagr_pct'])+'%':>7} {str(m['vol_pct'])+'%':>6} {str(m['sharpe']):>7} {str(m['maxdd_pct'])+'%':>7}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
