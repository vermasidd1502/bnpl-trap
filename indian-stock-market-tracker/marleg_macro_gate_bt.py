"""
marleg_macro_gate_bt.py — does the long edge depend on the MACRO STATE? (the micro->macro bridge)

Runs on the canonical 5y panel. For each as-of date we tag the market state — trend (NIFTY-proxy vs
its 50DMA), breadth (% of names above their 50DMA), market volatility (median 20d realized vol), and
cross-sectional DISPERSION (std of 21d returns; the Regime-Dial's stock-picker signal) — then measure
forward returns for the gated-long book vs the do-nothing universe (drift), bucketed by each state.

The attribution we're after: alpha = gate - drift, per macro bucket. If the long edge is concentrated
in (say) high-dispersion / low-vol / improving-breadth states, that's a macro theory we can act on
(only deploy the book when the macro gate is open) and backtest further. Tercile cutoffs use the full
sample (attribution, not a tradeable signal — noted).

Writes marleg_macro_gate_bt.json. Research artifact (gitignored).
"""
import os
import sys
import json
import numpy as np
import pandas as pd
import marleg_panel_build as pb
import marleg_industry_rs as mir

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_macro_gate_bt.json")
HS = [10, 21]
STEP = 3
COST = 0.25


def rank_groups(r20, r50, abv, members):
    rows = []
    for g, mem in members.items():
        mm = [s for s in mem if pd.notna(r20.get(s))]
        if not mm:
            continue
        rows.append((g, np.nanmean([r20[s] for s in mm]),
                     np.nanmean([r50[s] for s in mm if pd.notna(r50.get(s))]) if any(pd.notna(r50.get(s)) for s in mm) else np.nan,
                     np.mean([1.0 if bool(abv.get(s, False)) else 0.0 for s in mm])))
    if not rows:
        return {}
    df = pd.DataFrame(rows, columns=["g", "a", "b", "c"])
    sc = 0.4 * df["a"].rank(pct=True) + 0.25 * df["b"].rank(pct=True).fillna(df["a"].rank(pct=True)) + 0.35 * df["c"].rank(pct=True)
    return dict(zip(df["g"], sc.rank(ascending=False, pct=True)))


def mean(a):
    a = np.asarray(a, float) * 100.0
    return {"n": int(len(a)), "mean": round(float(np.nanmean(a)), 3) if len(a) else None,
            "hit": round(float(np.nanmean(a > 0)) * 100, 1) if len(a) else None}


def main():
    panel = pb.load()
    if not panel:
        print("panel not built — run marleg_panel_build.py first."); return
    close, volume = panel["close"], panel["volume"]
    print(f"panel: {close.shape[1]} x {close.shape[0]} ({close.index[0].date()}->{close.index[-1].date()})")

    rets = close.pct_change()
    RET20, RET50, r21 = close.pct_change(20), close.pct_change(50), close.pct_change(21)
    abv50 = close > close.rolling(50).mean()
    dsg = np.sign(close.diff())
    UD = volume.where(dsg > 0, 0.0).rolling(20).sum() / volume.where(dsg < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    UDMA = UD.rolling(50).mean()
    FIB = (close - close.rolling(120).min()) / (close.rolling(120).max() - close.rolling(120).min()).replace(0, np.nan)
    RVOL = rets.rolling(20).std() * np.sqrt(252)
    TURN = (close * volume).rolling(20).mean()
    FWD = {h: close.shift(-h) / close - 1 for h in HS}

    eff, _, _ = mir.effective_groups(list(close.columns))
    eff_s = pd.Series(eff); members = {}
    for s, g in eff.items():
        members.setdefault(g, []).append(s)

    # macro-state series
    mkt = (1 + rets.mean(axis=1)).cumprod()
    bull = (mkt > mkt.rolling(50).mean())
    breadth = abv50.mean(axis=1)
    mvol = RVOL.median(axis=1)
    disp = r21.std(axis=1)

    def terc(series, i):
        s = series.dropna()
        lo, hi = s.quantile(0.33), s.quantile(0.66)
        v = series.iloc[i]
        return "lo" if v <= lo else "hi" if v >= hi else "mid"

    # buckets: dim -> bucket -> {gate:[], drift:[]} per horizon
    dims = ["trend", "breadth", "vol", "disp"]
    bag = {h: {d: {} for d in dims} for h in HS}
    n = close.shape[0]; dates = 0
    for i in range(232, n - max(HS) - 1, STEP):
        ind_rank = rank_groups(RET20.iloc[i], RET50.iloc[i], abv50.iloc[i], members)
        if not ind_rank:
            continue
        dates += 1
        ir = eff_s.map(ind_rank).reindex(close.columns).fillna(1.0)
        L = (TURN.iloc[i].rank(pct=True) >= 0.40).fillna(False)
        gate = (L & (ir <= 0.40) & (UD.iloc[i] > UDMA.iloc[i]) & (UD.iloc[i] > UD.iloc[i - 10]) & (FIB.iloc[i] > 0.618)).fillna(False)
        gcols = close.columns[gate.values]; dcols = close.columns[L.values]
        states = {"trend": "bull" if bool(bull.iloc[i]) else "bear",
                  "breadth": terc(breadth, i), "vol": terc(mvol, i), "disp": terc(disp, i)}
        for h in HS:
            gv = FWD[h].iloc[i][gcols].values; dv = FWD[h].iloc[i][dcols].values
            for d, b in states.items():
                slot = bag[h][d].setdefault(b, {"gate": [], "drift": []})
                slot["gate"].extend(gv.tolist()); slot["drift"].extend(dv.tolist())

    res = {}
    for h in HS:
        res[f"h{h}"] = {}
        for d in dims:
            res[f"h{h}"][d] = {}
            for b, slot in bag[h][d].items():
                g, dr = mean(slot["gate"]), mean(slot["drift"])
                alpha = round((g["mean"] - dr["mean"]), 3) if (g["mean"] is not None and dr["mean"] is not None) else None
                res[f"h{h}"][d][b] = {"gate": g, "drift": dr, "alpha_gate_minus_drift": alpha}
    payload = {"as_of_dates": dates, "cost_pct": COST, "from": str(close.index[0].date()),
               "to": str(close.index[-1].date()), "results": res}
    json.dump(payload, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    H = 10
    print(f"\n=== gated-long ALPHA (gate - drift) by MACRO STATE @ {H}d ({dates} dates) ===")
    order = {"trend": ["bull", "bear"], "breadth": ["lo", "mid", "hi"], "vol": ["lo", "mid", "hi"], "disp": ["lo", "mid", "hi"]}
    for d in ["trend", "breadth", "vol", "disp"]:
        print(f"  {d}:")
        for b in order[d]:
            r = res[f"h{H}"][d].get(b)
            if r and r["gate"]["n"] > 30:
                print(f"    {b:<5} gate {str(r['gate']['mean']):>7}%  drift {str(r['drift']['mean']):>7}%  "
                      f"ALPHA {str(r['alpha_gate_minus_drift']):>7}%  (n={r['gate']['n']})")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
