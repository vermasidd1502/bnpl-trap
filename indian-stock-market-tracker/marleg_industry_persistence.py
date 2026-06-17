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

    def _rsi(c, n=14):
        dd = c.diff(); up = dd.clip(lower=0).rolling(n).mean(); dn = (-dd.clip(upper=0)).rolling(n).mean()
        v = (100 - 100 / (1 + up / dn.replace(0, np.nan))).iloc[-1]
        return float(v) if v == v else 50.0

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
        rs2 = float((1 + indret[k].iloc[-2:]).prod() - 1) * 100               # last 2 trading days
        # OVERHEAT / "overblown" — extension vs own history + RSI + stretch above 50DMA + breadth euphoria/divergence
        idx = (1 + indret[k]).cumprod()
        ret20h = float(idx.iloc[-1] / idx.iloc[-21] - 1) if len(idx) > 21 else 0.0
        roll20 = idx.pct_change(20).dropna()
        pct20 = float((roll20 <= ret20h).mean()) if len(roll20) > 60 else 0.5
        rsi = _rsi(idx)
        dma = idx.rolling(50).mean().iloc[-1]
        stretch = float(idx.iloc[-1] / dma - 1) * 100 if dma == dma and dma else 0.0
        syms = byind[k]
        ab = [bool(close[s].iloc[-1] > close[s].rolling(50).mean().iloc[-1]) for s in syms
              if close[s].notna().iloc[-1] and close[s].rolling(50).mean().notna().iloc[-1]]
        breadth = float(np.mean(ab)) * 100 if ab else 0.0
        ab20 = [bool(close[s].iloc[-21] > close[s].rolling(50).mean().iloc[-21]) for s in syms if len(close[s].dropna()) > 71]
        breadth20 = float(np.mean(ab20)) * 100 if ab20 else breadth
        diverge = breadth < breadth20 - 8 and ret20h > 0                      # price up but breadth rolling over = topping
        heat = (30 if pct20 >= 0.90 else 18 if pct20 >= 0.80 else 8 if pct20 >= 0.70 else 0)
        heat += (25 if rsi >= 75 else 15 if rsi >= 70 else 7 if rsi >= 65 else 0)
        heat += (20 if stretch >= 12 else 12 if stretch >= 8 else 6 if stretch >= 5 else 0)
        heat += (15 if breadth >= 92 else 0) + (10 if diverge else 0)
        heat = min(heat, 100)
        hcls = "OVERBLOWN" if heat >= 60 else "HEATING" if heat >= 35 else "COOLING" if ret20h < -2 else "NORMAL"
        rows.append({"industry": k, "beta": beta, "vol": vol,
                     "rs_week": round(wk5, 1), "rs_2d": round(rs2, 1), "rs_63d": round(mom_now, 1),
                     "median_lead_weeks": round(float(np.median(r)), 1) if r else 0,
                     "max_lead_weeks": max(r) if r else 0, "stints": len(r),
                     "rsi": round(rsi), "stretch": round(stretch, 1), "breadth": round(breadth),
                     "heat": heat, "heat_class": hcls, "diverge": bool(diverge)})
    rows.sort(key=lambda x: x["rs_week"], reverse=True)
    overall_med = round(float(np.median(all_runs)), 1) if all_runs else None
    overall_mean = round(float(np.mean(all_runs)), 1) if all_runs else None
    overblown = sorted([x for x in rows if x["heat_class"] == "OVERBLOWN"], key=lambda x: -x["heat"])
    heating = [x for x in rows if x["heat_class"] == "HEATING"]
    hot2 = sorted(rows, key=lambda x: x["rs_2d"], reverse=True)[:6]
    res = {"n_industries": len(rows), "overall_median_lead_weeks": overall_med,
           "overall_mean_lead_weeks": overall_mean, "n_overblown": len(overblown),
           "hot_2d": [{"industry": x["industry"], "rs_2d": x["rs_2d"]} for x in hot2], "industries": rows}
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("\n  OVERBLOWN (overheated → mean-reversion risk; don't chase / trim, NOT a short): "
          + (", ".join(f"{x['industry'][:22]}(heat {x['heat']})" for x in overblown[:6]) or "none"))
    print("  HEATING (extended → buy pullbacks only): " + (", ".join(x["industry"][:20] for x in heating[:6]) or "none"))
    print("  HOT last 2 trading days: " + ", ".join(f"{x['industry'][:20]} {x['rs_2d']:+}%" for x in hot2))

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
