"""
marleg_confluence_study.py — DOES MULTI-ENGINE AGREEMENT ACTUALLY PAY? (the Stock-Lab premise, tested)

The Lab's whole pitch is that when MORE engines agree on a name, conviction is higher. This tests that on
the 5y panel by reconstructing the Lab's signal components historically and bucketing forward returns by
how many AGREE.

Base candidate (a long setup at all): above 50 & 200 DMA AND not RSI-extended (<72) — the don't-chase gate.
Among those, count how many of these 4 also fire (the "confluence"):
   • accum    — disciplined U/D (1.3-4)            (volume pod)
   • leading  — industry in the top-40% by 20d RS  (rotation pod)
   • strength — 120d range position >= 0.6 (fib)   (fib gate)
   • reversal — pulled back 3d then turned up green (reversal radar)
Then measure forward 10d & 20d return + win-rate by confluence count (0..4), NET of cost. If win/net rises
monotonically with agreement, the Lab's premise holds; if flat, agreement is just redundancy.

  python marleg_confluence_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_confluence_study.json")
COST = 0.3          # round-trip cost %, charged against net
FWD = (10, 20)


def _rsi(C, n=14):
    d = C.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def study():
    import marleg_panel_build as pb
    P = pb.load()
    if not P:
        return {"ok": False, "error": "panel not built"}
    C, O, V = P["close"], P["open"], P["volume"]
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))

    s50 = C.rolling(50).mean(); s200 = C.rolling(200).mean()
    trend = (C > s50) & (C > s200)
    rsi = _rsi(C)
    not_ext = rsi < 72
    ret20 = C.pct_change(20)
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    accum = (ud >= 1.3) & (ud < 4)
    lo120 = C.rolling(120).min(); hi120 = C.rolling(120).max()
    fib = (C - lo120) / (hi120 - lo120)
    strength = fib >= 0.6
    o = O.reindex(C.index)
    green = C > o
    down3 = (C / C.shift(3) - 1) < 0
    reversal = down3 & green & (C > C.shift(1))

    # industry leading: industry mean ret20 per day, top-40% of industries, broadcast back to stocks
    ind_of = pd.Series({s: (SECT.get(s, {}) or {}).get("industry") for s in C.columns})
    valid_ind = ind_of.dropna()
    ind_ret = ret20[valid_ind.index].T.groupby(valid_ind).mean().T          # days x industry
    ind_rank = ind_ret.rank(axis=1, pct=True)
    lead_ind = ind_rank >= 0.6
    leading = lead_ind.reindex(columns=[ind_of.get(s) for s in C.columns])
    leading.columns = C.columns
    leading = leading.fillna(False)

    conf = (accum.fillna(False).astype(int) + leading.astype(int)
            + strength.fillna(False).astype(int) + reversal.fillna(False).astype(int))
    cand = (trend.fillna(False) & not_ext.fillna(False))

    fwd = {h: (C.shift(-h) / C - 1) * 100 for h in FWD}
    base = {h: float(fwd[h].values[np.isfinite(fwd[h].values)].mean()) for h in FWD}

    rows = []
    cm = cand.values; confv = conf.values
    for k in range(0, 5):
        mask = cm & (confv == k)
        rec = {"confluence": k, "n": int(mask.sum())}
        for h in FWD:
            fv = fwd[h].values[mask]
            fv = fv[np.isfinite(fv)]
            if len(fv):
                rec[f"win{h}"] = round(float((fv > COST).mean()) * 100, 1)
                rec[f"net{h}"] = round(float(fv.mean()) - COST, 2)
        rows.append(rec)

    # individual single-signal forward (single-engine baseline) at 10d
    singles = {}
    for nm, sig in [("accum", accum), ("leading", leading), ("strength", strength), ("reversal", reversal)]:
        m = (cand & sig.fillna(False)).values
        fv = fwd[10].values[m]; fv = fv[np.isfinite(fv)]
        if len(fv):
            singles[nm] = {"n": int(m.sum()), "win10": round(float((fv > COST).mean()) * 100, 1),
                           "net10": round(float(fv.mean()) - COST, 2)}

    # verdict: does net10 rise with confluence?
    valid = [r for r in rows if r.get("n", 0) >= 200 and "net10" in r]
    mono = all(valid[i]["net10"] <= valid[i + 1]["net10"] for i in range(len(valid) - 1)) if len(valid) >= 2 else None
    lo_k = valid[0] if valid else None; hi_k = valid[-1] if valid else None
    verdict = ""
    if lo_k and hi_k and lo_k is not hi_k:
        lift = round(hi_k["net10"] - lo_k["net10"], 2)
        verdict = (f"Confluence {hi_k['confluence']} vs {lo_k['confluence']}: net10 {hi_k['net10']:+}% vs {lo_k['net10']:+}% "
                   f"(lift {lift:+}%), win {hi_k.get('win10')}% vs {lo_k.get('win10')}%. "
                   + ("Agreement PAYS — monotone-ish: more engines → better." if (lift > 0.3) else
                      "Agreement adds little — the lift is small/noisy; treat the Lab as a FILTER, not a multiplier."))
    return {"ok": True, "cost_pct": COST, "baseline_net": {h: round(base[h] - COST, 2) for h in FWD},
            "by_confluence": rows, "singles": singles, "monotone": mono, "verdict": verdict,
            "note": "Confluence = # of {accum, leading, strength, reversal} firing on a name that's already "
                    "in an uptrend & not RSI-extended. Forward % net of " + str(COST) + "% cost. Survivorship "
                    "lifts all buckets equally, so the CROSS-bucket comparison is the fair read."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = study()
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    json.dump(r, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\nCONFLUENCE STUDY — does multi-engine agreement pay? (net of {r['cost_pct']}% cost)")
    print(f"baseline net: 10d {r['baseline_net'][10]}% · 20d {r['baseline_net'][20]}%\n")
    print(f"  {'agree':<6}{'n':>9}{'win10':>8}{'net10':>8}{'win20':>8}{'net20':>8}")
    for x in r["by_confluence"]:
        print(f"  {x['confluence']:<6}{x['n']:>9}{x.get('win10','-'):>8}{x.get('net10','-'):>8}{x.get('win20','-'):>8}{x.get('net20','-'):>8}")
    print("\nsingle-engine (10d net of cost):")
    for k, v in r["singles"].items():
        print(f"  {k:<10} n{v['n']:>7}  win {v['win10']}%  net {v['net10']}%")
    print("\nVERDICT:", r["verdict"])
