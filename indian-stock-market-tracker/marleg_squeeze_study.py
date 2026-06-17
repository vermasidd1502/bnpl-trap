"""
marleg_squeeze_study.py — WHICH INDUSTRIES ACTUALLY SQUEEZE? (and do their squeezes pay?)

The user's thesis: only hunt short-squeezes in industries that are *known* to squeeze, and prove it
with a backtest before trusting it. India publishes NO daily equity short-interest, so a literal
squeeze isn't observable historically. What we CAN measure on the 4-5y panel is the squeeze
FOOTPRINT (price+volume): a name beaten down, then snapping UP hard on a volume blast.

For every symbol we flag a "squeeze event" at day t:
    (20d return as of YESTERDAY <= DOWN)      # it was beaten down  (no look-ahead)
    AND (today's 1d return >= UP)             # sharp snap up
    AND (today's RVOL >= RVOL_MIN)            # on a volume blast
Then we measure the FORWARD return (1/3/5/10d) from today's close — could you have bought the snap
and made money, or does it fade? — and aggregate per granular industry:
    • frequency  (events per 1000 stock-days)         = how "dirty"/squeezy the industry is
    • fwd win% / net% vs the industry's own baseline   = does the squeeze actually PAY (tradeable)
                                                          or fade (a trap to avoid)?
Output: marleg_squeeze_study.json with a per-industry table + a backtest-blessed whitelist
(dirty AND tradeable) that the live squeeze radar then focuses on.

    python marleg_squeeze_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_squeeze_study.json")

DOWN = -0.08        # beaten down: 20d return <= -8% as of yesterday
UP = 0.04           # snap: today +4% or more
RVOL_MIN = 1.5      # on >=1.5x average volume
HORIZONS = [1, 3, 5, 10]
MIN_EVENTS = 30     # need this many events for a per-industry verdict


def study():
    import marleg_panel_build as pb
    panel = pb.load()
    if not panel:
        return {"ok": False, "error": "panel not built"}
    close, vol = panel["close"], panel["volume"]
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))

    ev_ind, ev_fwd = [], {h: [] for h in HORIZONS}      # per-event industry + forward returns
    base_ind, base_fwd5 = [], []                        # baseline (all days) for fwd5 comparison
    days_by_ind = {}                                    # stock-days per industry (for frequency)

    for s in close.columns:
        c = close[s].dropna()
        if len(c) < 150:
            continue
        ind = (SECT.get(s) or {}).get("industry") or (SECT.get(s) or {}).get("sector")
        if not ind:
            continue
        r1 = c.pct_change()
        r20 = c.pct_change(20)
        v = vol[s].reindex(c.index)
        rvol = v / v.rolling(21).mean().shift(1)
        mask = (r20.shift(1) <= DOWN) & (r1 >= UP) & (rvol >= RVOL_MIN)
        cv = c.values
        n = len(cv)
        days_by_ind[ind] = days_by_ind.get(ind, 0) + n
        # baseline: a random-ish but full sample of fwd5 for this industry (every 5th day, has +5 ahead)
        for t in range(0, n - 5, 5):
            base_ind.append(ind); base_fwd5.append(cv[t + 5] / cv[t] - 1)
        idx = np.where(mask.values)[0]
        for t in idx:
            ev_ind.append(ind)
            for h in HORIZONS:
                ev_fwd[h].append((cv[t + h] / cv[t] - 1) if t + h < n else np.nan)

    if not ev_ind:
        return {"ok": False, "error": "no squeeze events found"}

    E = pd.DataFrame({"ind": ev_ind, **{f"f{h}": ev_fwd[h] for h in HORIZONS}})
    B = pd.DataFrame({"ind": base_ind, "f5": base_fwd5})
    base5_by_ind = B.groupby("ind")["f5"].mean().to_dict()
    pooled = {f"f{h}": {"n": int(E[f"f{h}"].notna().sum()),
                        "win": round(float((E[f"f{h}"] > 0).mean()) * 100, 1),
                        "net": round(float(E[f"f{h}"].mean()) * 100, 2)} for h in HORIZONS}
    pooled_base5 = round(float(B["f5"].mean()) * 100, 2)

    rows = []
    for ind, g in E.groupby("ind"):
        ne = len(g)
        if ne < MIN_EVENTS:
            continue
        kday = days_by_ind.get(ind, 0) / 1000.0
        rec = {"industry": ind, "n_events": ne,
               "freq_per_kday": round(ne / kday, 2) if kday else None,
               "base_net5": round(base5_by_ind.get(ind, 0) * 100, 2)}
        for h in HORIZONS:
            col = g[f"f{h}"].dropna()
            rec[f"win{h}"] = round(float((col > 0).mean()) * 100, 1) if len(col) else None
            rec[f"net{h}"] = round(float(col.mean()) * 100, 2) if len(col) else None
        rec["edge5"] = round((rec["net5"] or 0) - rec["base_net5"], 2)     # squeeze net5 minus just-holding-the-industry
        rows.append(rec)

    rows.sort(key=lambda x: x["freq_per_kday"] or 0, reverse=True)
    # blessed = DIRTY (above-median frequency) AND TRADEABLE (fwd5 wins >=52% AND beats its own baseline)
    freqs = sorted([r["freq_per_kday"] for r in rows if r["freq_per_kday"]])
    med_freq = freqs[len(freqs) // 2] if freqs else 0
    for r in rows:
        dirty = (r["freq_per_kday"] or 0) >= med_freq
        tradeable = (r["win5"] or 0) >= 52 and r["edge5"] > 0 and (r["net5"] or 0) > 0
        r["dirty"] = bool(dirty)
        r["tradeable"] = bool(tradeable)
        r["verdict"] = ("FOCUS (dirty + pays)" if dirty and tradeable else
                        "trap (squeezy but fades)" if dirty and not tradeable else
                        "tradeable (rare)" if tradeable else "skip")
    whitelist = [r["industry"] for r in rows if r["dirty"] and r["tradeable"]]
    traps = [r["industry"] for r in rows if r["dirty"] and not r["tradeable"]]

    out = {"ok": True, "params": {"down20": DOWN, "snap1d": UP, "rvol_min": RVOL_MIN,
                                  "horizons": HORIZONS, "min_events": MIN_EVENTS},
           "pooled": pooled, "pooled_baseline_net5": pooled_base5,
           "median_freq_per_kday": round(med_freq, 2),
           "n_industries": len(rows), "industries": rows,
           "whitelist": whitelist, "traps": traps,
           "note": "Squeeze = beaten-down (20d<=-8%) then a >=+4% snap on >=1.5x volume (the India footprint; "
                   "no equity short-interest exists). edge5 = squeeze net5 MINUS the industry's own baseline net5 "
                   "(does the squeeze add anything over just holding the group?). FOCUS = dirty (freq>=median) AND "
                   "tradeable (win5>=52%, edge5>0). 'trap' = squeezy but the snap fades — don't chase."}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    return out


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = study()
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    p = r["pooled"]
    print(f"POOLED squeeze edge (all industries): "
          + "  ".join(f"f{h} {p['f'+str(h)]['win']}%win/{p['f'+str(h)]['net']}net (n{p['f'+str(h)]['n']})" for h in HORIZONS))
    print(f"  vs baseline net5 = {r['pooled_baseline_net5']}%   -> squeeze {'ADDS' if p['f5']['net']>r['pooled_baseline_net5'] else 'does NOT add'} edge on average")
    print(f"\nDIRTIEST / most squeeze-prone industries (events per 1000 stock-days), with forward edge:")
    print(f"  {'industry':<30} {'freq':>5} {'n':>5} {'win5':>5} {'net5':>6} {'edge5':>6}  verdict")
    for x in r["industries"][:22]:
        print(f"  {x['industry'][:30]:<30} {x['freq_per_kday']:>5} {x['n_events']:>5} {x['win5']:>5} {x['net5']:>6} {x['edge5']:>6}  {x['verdict']}")
    print(f"\nFOCUS whitelist (dirty + pays, {len(r['whitelist'])}): " + ", ".join(r["whitelist"]))
    print(f"TRAP industries (squeezy but fade, {len(r['traps'])}): " + ", ".join(r["traps"][:12]))
