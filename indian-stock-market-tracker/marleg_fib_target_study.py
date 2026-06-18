"""
marleg_fib_target_study.py — should the target CHASE past fib=1, or sit AT the resistance?

The pod's current target = price + 2*ATR, recomputed every scan (it chases through the prior high). This
tests the user's intuition: fib=1 (the 120d high) is a market-watched resistance, so a target set BEYOND it
is often not reached — price stalls at the high. For gated-style entries with room below the high we compare,
over the next 20 sessions:
   • reach A = does price reach entry + 2*ATR        (the current "chase" target)
   • reach B = does price reach the prior 120d high   (the resistance — the proposed first target)
and, on the subset where the chase target sits BEYOND the resistance (tgtA > prior high), which is hit more
reliably. Plus the payoff of EXTENDING only after the high is reached (confirmation).

  python marleg_fib_target_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_fib_target_study.json")
FWD = 20


def study():
    import marleg_panel_build as pb
    P = pb.load()
    if not P:
        return {"ok": False, "error": "panel not built"}
    C, H, L, V = P["close"], P["high"], P["low"], P["volume"]
    reachA = []; reachB = []; beyond = []; then_ext = []; fwd20 = []
    for s in C.columns:
        c = C[s].dropna()
        if len(c) < 200:
            continue
        h = H[s].reindex(c.index); l = L[s].reindex(c.index); v = V[s].reindex(c.index)
        s50 = c.rolling(50).mean()
        ph = h.rolling(120).max().shift(1)                       # prior 120d high (the fib-1 resistance)
        pc = c.shift(1)
        tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        rc = c.pct_change()
        upv = v.where(rc > 0, 0.0).rolling(20).sum(); dnv = v.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
        ud = upv / dnv
        # forward 20d max high (excl today) + forward 20d close return
        fhigh = h.shift(-1)[::-1].rolling(FWD, min_periods=1).max()[::-1]
        fret = c.shift(-FWD) / c - 1
        cand = (c > s50) & (ud > 1.3) & (c < ph) & atr.notna() & ph.notna() & fhigh.notna()
        idx = np.where(cand.values)[0]
        cv = c.values; av = atr.values; pv = ph.values; fv = fhigh.values; rv = fret.values
        for t in idx:
            entry = cv[t]; tgtA = entry + 2 * av[t]; tgtB = pv[t]; hh = fv[t]
            reachA.append(hh >= tgtA); reachB.append(hh >= tgtB)
            b = tgtA > tgtB; beyond.append(b)
            if b:                                                # chase target sits past the resistance
                then_ext.append((hh >= tgtB, hh >= tgtA))        # (reached high?, reached chase target?)
            if np.isfinite(rv[t]):
                fwd20.append(rv[t] * 100)

    n = len(reachA)
    if not n:
        return {"ok": False, "error": "no candidates"}
    rA = np.array(reachA); rB = np.array(reachB); bey = np.array(beyond)
    res = {"ok": True, "n": n, "fwd_days": FWD,
           "reach_chase2atr_pct": round(float(rA.mean()) * 100, 1),
           "reach_priorhigh_pct": round(float(rB.mean()) * 100, 1),
           "pct_chase_beyond_resistance": round(float(bey.mean()) * 100, 1),
           "fwd20_mean_pct": round(float(np.mean(fwd20)), 2) if fwd20 else None}
    # the key subset: chase target sits BEYOND the prior high
    if then_ext:
        te = np.array(then_ext)
        res["beyond_subset"] = {"n": len(te),
                                "reach_resistance_pct": round(float(te[:, 0].mean()) * 100, 1),
                                "reach_chase_pct": round(float(te[:, 1].mean()) * 100, 1),
                                "extend_after_high_pct": round(float(te[te[:, 0], 1].mean()) * 100, 1) if te[:, 0].any() else None}
    bs = res.get("beyond_subset", {})
    res["verdict"] = (
        f"On the {res['pct_chase_beyond_resistance']}% of setups whose +2ATR target sits BEYOND the prior high: "
        f"price reaches the RESISTANCE (prior high) {bs.get('reach_resistance_pct')}% of the time but the "
        f"CHASE target only {bs.get('reach_chase_pct')}% — and once the high IS reached, it goes on to the chase "
        f"target {bs.get('extend_after_high_pct')}% of the time. So: target the RESISTANCE first; EXTEND only "
        f"after it's reached/held. Chasing 2ATR past fib=1 books a target that's hit far less often.")
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    return res


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = study()
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\nFIB-TARGET STUDY (n={r['n']:,}, fwd {r['fwd_days']}d)")
    print(f"  reach +2ATR (chase) target: {r['reach_chase2atr_pct']}%")
    print(f"  reach prior-high (resistance): {r['reach_priorhigh_pct']}%")
    print(f"  setups whose +2ATR sits BEYOND the prior high: {r['pct_chase_beyond_resistance']}%")
    b = r.get("beyond_subset", {})
    print(f"  └ in that subset: reach resistance {b.get('reach_resistance_pct')}% · reach chase {b.get('reach_chase_pct')}% · extend-after-high {b.get('extend_after_high_pct')}%")
    print(f"\nVERDICT: {r['verdict']}")
