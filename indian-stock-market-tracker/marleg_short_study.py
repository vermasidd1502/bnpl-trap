"""
marleg_short_study.py — DOES ANY SHORT SETUP ACTUALLY PAY IN INDIA? (category by category)

Our standing finding is blunt: shorting cash equity in India has been a money-loser (weak names bounce,
the market drifts up, every bearish pattern was folklore). But "shorting never pays" is too coarse —
this tests SHORT-ENTRY CATEGORIES separately so the short pod only ever suggests the survivors (if any).

A SHORT profits when the forward return is NEGATIVE. So per category we report:
  • und_net5/10/21 = mean forward UNDERLYING return (what the stock did)
  • short_net10    = -mean(fwd10)  (the short's P&L before costs; POSITIVE = short made money)
  • short_win10    = % of events where fwd10 < 0 (the short was right)
and compare to BASELINE (all stock-days) — because India's drift means the average stock RISES,
so a short has to overcome a structural headwind just to break even. Net of ~0.2-0.3%/leg costs too.

Entry = at the close of the signal day, hold H days, short.

  python marleg_short_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_short_study.json")
FWD = (5, 10, 21)
COST = 0.25            # round-trip cost assumption, % (brokerage+impact+borrow), charged against the short


def _rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def study():
    import marleg_panel_build as pb
    panel = pb.load()
    if not panel:
        return {"ok": False, "error": "panel not built"}
    C, O, H, L, V = panel["close"], panel["open"], panel["high"], panel["low"], panel["volume"]
    cats = {k: {h: [] for h in FWD} for k in
            ("parabolic", "breakdown", "gated_short", "hi_reject", "gap_down", "bear_engulf")}
    base = {h: [] for h in FWD}

    for s in C.columns:
        c = C[s].dropna()
        if len(c) < 300:
            continue
        o = O[s].reindex(c.index); h = H[s].reindex(c.index); l = L[s].reindex(c.index)
        v = V[s].reindex(c.index)
        rsi = _rsi(c)
        s50 = c.rolling(50).mean(); s200 = c.rolling(200).mean()
        ret20 = c.pct_change(20)
        hi250 = c.rolling(250).max()
        rc = c.pct_change()
        upv = v.where(rc > 0, 0.0).rolling(20).sum()
        dnv = v.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
        ud = upv / dnv
        gap = o / c.shift(1) - 1
        # forward returns
        fwd = {hh: (c.shift(-hh) / c - 1) for hh in FWD}
        cv = {hh: fwd[hh].values for hh in FWD}
        # masks (entry at close of day t)
        m = {
            "parabolic":  (rsi > 78) & (c > s50 * 1.15),
            "breakdown":  (c < s50) & (c < s200) & (ret20 < -0.08),
            "gated_short": (ud < 0.7) & (c < s50),
            "hi_reject":  (c.shift(1) > hi250.shift(1) * 0.97) & (rc < -0.03),
            "gap_down":   (gap < -0.03),
            # bearish engulfing at a high (control): today red, engulfs yesterday's green body, after an up-run
            "bear_engulf": (c < o) & (c.shift(1) > o.shift(1)) & (o >= c.shift(1)) & (c <= o.shift(1)) & (rsi > 55),
        }
        # baseline: every 5th day
        bn = np.zeros(len(c), bool); bn[::5] = True
        for hh in FWD:
            base[hh].extend(cv[hh][bn & np.isfinite(cv[hh])])
        for k, mask in m.items():
            mv = mask.fillna(False).values
            for hh in FWD:
                vals = cv[hh][mv & np.isfinite(cv[hh])]
                cats[k][hh].extend(vals.tolist())

    def agg(d):
        out = {"n": len(d[10])}
        for hh in FWD:
            a = np.array(d[hh], float)
            if len(a):
                out[f"und_net{hh}"] = round(float(a.mean()) * 100, 2)
                out[f"short_win{hh}"] = round(float((a < 0).mean()) * 100, 1)
        out["short_net10"] = round(-out.get("und_net10", 0) - COST, 2)   # short P&L net of costs
        return out

    base_a = {hh: np.array(base[hh], float) for hh in FWD}
    baseline = {"n": len(base[10]),
                **{f"und_net{hh}": round(float(base_a[hh].mean()) * 100, 2) for hh in FWD}}
    rows = {k: agg(v) for k, v in cats.items()}
    # verdict: a short category "works" if short_net10 > 0 AND short_win10 >= 50
    winners = [k for k, r in rows.items()
               if r.get("short_net10", -9) > 0 and r.get("short_win10", 0) >= 50]
    out = {"ok": True, "cost_pct": COST, "fwd": list(FWD), "baseline": baseline,
           "categories": rows, "short_winners": winners,
           "labels": {"parabolic": "parabolic / overextended (RSI>78 & >15% over 50DMA)",
                      "breakdown": "breakdown (below 50&200DMA, 20d<-8%)",
                      "gated_short": "gated-short control (U/D<0.7 & <50DMA)",
                      "hi_reject": "52w-high rejection (poke high then close -3%)",
                      "gap_down": "gap-down continuation (open <-3%)",
                      "bear_engulf": "bearish engulfing at a high (control)"},
           "note": "SHORT P&L = -(forward underlying return) - costs. India's baseline drift is POSITIVE, so "
                   "the average stock rises — a short fights that. short_net10>0 AND short_win10>=50 = a real "
                   "short edge. Anything else = the long-only / hedge-only verdict stands."}
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
    b = r["baseline"]
    print(f"BASELINE drift (all stock-days): +{b['und_net5']}%/5d  +{b['und_net10']}%/10d  +{b['und_net21']}%/21d  (n={b['n']:,})")
    print(f"  -> the average India stock RISES; a short must overcome this + {r['cost_pct']}% costs.\n")
    print(f"{'category':<42}{'n':>8}{'und10%':>8}{'shortNet10':>11}{'shortWin10':>11}")
    for k, lab in r["labels"].items():
        x = r["categories"][k]
        print(f"{lab:<42}{x['n']:>8}{x.get('und_net10', 0):>8}{x.get('short_net10', 0):>11}{x.get('short_win10', 0):>10}%")
    print(f"\nSHORT CATEGORIES THAT ACTUALLY PAY (net, win>=50%): {r['short_winners'] or 'NONE — long-only/hedge verdict stands'}")
