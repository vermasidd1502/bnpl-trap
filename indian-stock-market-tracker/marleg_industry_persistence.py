"""
marleg_industry_persistence.py — answers: (1) how LONG does an industry's leadership usually last? and
(2) how RISKY is each industry (beta + vol)? — on the canonical 5y panel.

PERSISTENCE: each week, rank granular industries by trailing-63d relative strength; a "leader" = top-40%.
We measure how many consecutive weeks an industry STAYS a leader once it gets there = the natural hold
horizon for rotation (rebalance around it, don't churn weekly).

RISK: per-industry BETA to the equal-weight market (sensitivity) + annualized VOL. High beta = amplifies the
market both ways (size smaller); low beta = defensive.

Writes marleg_industry_persistence.json (per-industry beta/vol/this-week-RS/median-lead-weeks) + the overall
typical leadership duration.

  python marleg_industry_persistence.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_industry_persistence.json")


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
    byind = {}
    for s in close.columns:
        ind = SECT.get(s, {}).get("industry")
        if ind:
            byind.setdefault(ind, []).append(s)
    byind = {k: v for k, v in byind.items() if len(v) >= 4}
    indret = pd.DataFrame({k: rets[v].mean(axis=1) for k, v in byind.items()}).dropna(how="all")
    mkt = indret.mean(axis=1)

    # weekly RS rank -> leader (top-40%) -> consecutive-week run lengths
    wk = list(range(63, len(indret), 5))
    lead_hist = {k: [] for k in indret.columns}      # 1/0 leader each week
    for i in wk:
        mom = (1 + indret.iloc[i - 63:i]).prod() - 1
        thr = mom.quantile(0.60)                     # top 40%
        for k in indret.columns:
            lead_hist[k].append(1 if mom.get(k, -9) >= thr else 0)

    def runs(seq):
        out, c = [], 0
        for x in seq:
            if x:
                c += 1
            elif c:
                out.append(c); c = 0
        if c:
            out.append(c)
        return out

    all_runs = []
    rows = []
    mkt_var = float(np.var(mkt.dropna().values))
    for k in indret.columns:
        r = runs(lead_hist[k])
        all_runs += r
        d = pd.concat([indret[k], mkt], axis=1).dropna()
        beta = round(float(np.cov(d.iloc[:, 0], d.iloc[:, 1])[0, 1] / mkt_var), 2) if mkt_var else None
        vol = round(float(indret[k].std() * np.sqrt(252)) * 100, 1)
        mom_now = float((1 + indret[k].iloc[-63:]).prod() - 1) * 100
        wk5 = float((1 + indret[k].iloc[-5:]).prod() - 1) * 100
        rows.append({"industry": k, "beta": beta, "vol": vol,
                     "rs_week": round(wk5, 1), "rs_63d": round(mom_now, 1),
                     "median_lead_weeks": round(float(np.median(r)), 1) if r else 0,
                     "max_lead_weeks": max(r) if r else 0, "stints": len(r)})
    rows.sort(key=lambda x: x["rs_week"], reverse=True)
    overall_med = round(float(np.median(all_runs)), 1) if all_runs else None
    overall_mean = round(float(np.mean(all_runs)), 1) if all_runs else None
    res = {"n_industries": len(rows), "overall_median_lead_weeks": overall_med,
           "overall_mean_lead_weeks": overall_mean, "industries": rows}
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"industry leadership PERSISTENCE & RISK ({len(rows)} granular industries, 5y)\n")
    print(f"  once an industry becomes a leader (top-40%), it STAYS one for a median of "
          f"{overall_med} weeks (~{round((overall_med or 0)*5)} trading days); mean {overall_mean} wks.")
    print(f"\n  bullish THIS WEEK (top by 5d RS) + risk:")
    print(f"  {'industry':<30}{'wk RS%':>8}{'63d%':>8}{'beta':>7}{'vol%':>7}{'stays(wks)':>11}")
    for x in rows[:12]:
        print(f"  {x['industry'][:28]:<30}{x['rs_week']:>8}{x['rs_63d']:>8}{str(x['beta']):>7}{x['vol']:>7}{x['median_lead_weeks']:>11}")
    hi = sorted([x for x in rows if x["beta"] is not None], key=lambda x: x["beta"], reverse=True)
    print(f"\n  HIGHEST beta (riskiest, amplify the market): " + ", ".join(f"{x['industry'][:20]}({x['beta']})" for x in hi[:4]))
    print(f"  LOWEST beta (defensive): " + ", ".join(f"{x['industry'][:20]}({x['beta']})" for x in hi[-4:][::-1]))
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
