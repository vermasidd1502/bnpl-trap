"""
marleg_tier_study.py — 5-TIER industry volatility ladder + does tier-2 FOLLOW tier-1? (lead-lag test)

User hypothesis: rank industries into 5 tiers by volatility (tier1 = most volatile); tier-1 leads and
tier-2 "structurally follows" tier-1 -> trade tier-2 reactively on tier-1's move. Tested honestly on the
canonical 5y panel:
  • contemporaneous vs LAGGED correlation (only a LAGGED/predictive lead-lag is tradeable — you can't buy
    yesterday's move),
  • a trade test: long tier-2 the day AFTER a tier-1 up-day / big-up-day, forward 1d & 3d, net of cost,
    vs tier-2's unconditional drift.
Efficient-market prior: contemporaneous corr is high (they co-move), but lead-lag is usually tiny in
liquid markets. We report the truth either way.

  python marleg_tier_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_tier_study.json")
COST = 0.30


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
    volume = panel["volume"].reindex(close.index)
    rets = close.pct_change()

    import marleg_industry_rs as mir
    _, eff_group, _, _ = mir.leadership(close, volume)      # canonical symbol -> industry group
    byind = {}
    for s in close.columns:
        g = eff_group.get(s)
        if g:
            byind.setdefault(g, []).append(s)
    byind = {g: c for g, c in byind.items() if len(c) >= 4}
    indret = pd.DataFrame({g: rets[c].mean(axis=1) for g, c in byind.items()})
    vol = (indret.std() * np.sqrt(252)).sort_values(ascending=False)

    tiers = [list(a) for a in np.array_split(vol.index.tolist(), 5)]   # tier1 (highest vol) .. tier5
    tierret = pd.DataFrame({f"T{i+1}": indret[grp].mean(axis=1) for i, grp in enumerate(tiers)})

    out = {"tiers": {}, "leadlag": {}, "trade": {}}
    for i, grp in enumerate(tiers):
        out["tiers"][f"T{i+1}"] = {"vol_pct": round(float(vol[grp].mean()) * 100, 1), "n": len(grp),
                                   "examples": grp[:7]}

    # lead-lag: does T1 YESTERDAY predict Tk TODAY? (lag1/lag2 = tradeable; contemp = just co-movement)
    t1 = tierret["T1"]
    for k in range(1, 6):
        tk = tierret[f"T{k}"]
        out["leadlag"][f"T1->T{k}"] = {
            "contemp": round(float(t1.corr(tk)), 3),
            "lag1": round(float(t1.shift(1).corr(tk)), 3),
            "lag2": round(float(t1.shift(2).corr(tk)), 3)}
    # reverse sanity: does T2 lead T1?
    out["leadlag"]["T2->T1_lag1"] = round(float(tierret["T2"].shift(1).corr(t1)), 3)

    # trade test — long T2 the day AFTER a T1 up-day, forward 1d & 3d, net of cost
    f1 = tierret["T2"].shift(-1)
    f3 = tierret["T2"].shift(-1) + tierret["T2"].shift(-2) + tierret["T2"].shift(-3)
    up = tierret["T1"] > 0
    big = tierret["T1"] > tierret["T1"].quantile(0.80)

    def stat(series, mask=None):
        x = series[mask].dropna() if mask is not None else series.dropna()
        if len(x) < 40:
            return None
        return {"n": int(len(x)), "net": round(float(x.mean()) * 100 - COST, 3), "win": round(float((x > 0).mean()) * 100, 1)}

    out["trade"] = {
        "T2_next1d_uncond": stat(f1),
        "T2_next1d_after_T1up": stat(f1, up),
        "T2_next1d_after_T1bigup": stat(f1, big),
        "T2_next3d_uncond": stat(f3),
        "T2_next3d_after_T1bigup": stat(f3, big),
    }
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print("5-TIER VOLATILITY LADDER (tier1 = most volatile):")
    for i, grp in enumerate(tiers):
        t = out["tiers"][f"T{i+1}"]
        print(f"  T{i+1}  vol~{t['vol_pct']}%  ({t['n']} industries)  e.g. {', '.join(t['examples'][:5])}")
    print("\nLEAD-LAG  (contemp = co-move; lag1/lag2 = T1 YESTERDAY predicting Tk — the tradeable kind):")
    print(f"  {'pair':<10}{'contemp':>9}{'lag1':>8}{'lag2':>8}")
    for k in range(1, 6):
        r = out["leadlag"][f"T1->T{k}"]
        print(f"  T1->T{k:<6}{r['contemp']:>9}{r['lag1']:>8}{r['lag2']:>8}")
    print(f"  (reverse) T2->T1 lag1: {out['leadlag']['T2->T1_lag1']}")
    print("\nTRADE TEST — long Tier-2 the day AFTER a Tier-1 move (net of cost):")
    for k, v in out["trade"].items():
        if v:
            print(f"  {k:<26} n={v['n']:>5}  net {v['net']:>7}%  win {v['win']}%")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
