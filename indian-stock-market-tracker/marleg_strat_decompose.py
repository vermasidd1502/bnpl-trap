"""
marleg_strat_decompose.py — reverse-engineer WHERE the gated-long edge lives (cross-sectional).

macro_gate answers WHEN (which market regime). This answers WHERE: decompose the gated-long forward
returns by
  • SECTOR (macro) — is it one or two sectors carrying it, or broad?
  • LIQUIDITY tercile — CRITICAL realism check: if the edge is concentrated in the LEAST-liquid names,
    it's a microstructure/illiquidity artifact, not a tradeable edge.
  • realized-VOL tercile — is it a low-vol or high-vol phenomenon?
  • industry-rank bucket — does a stronger industry (top-10% vs 10-40%) actually matter within the gate?

This is the "manually fit / reverse-engineer the structure" step — it tells us the mechanism (broad
momentum? a sector bet? an illiquidity premium? a vol effect?) so we know what we're really trading.

Runs on the canonical panel. Writes marleg_strat_decompose.json. Research artifact (gitignored).
"""
import os
import sys
import json
import numpy as np
import pandas as pd
import marleg_panel_build as pb
import marleg_industry_rs as mir

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_strat_decompose.json")
HS = [10, 21]
STEP = 3


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


def agg(d):
    out = {}
    for k, v in d.items():
        a = np.asarray(v, float) * 100.0
        if len(a) >= 20:
            out[k] = {"n": int(len(a)), "mean": round(float(a.mean()), 3), "hit": round(float((a > 0).mean()) * 100, 1)}
    return out


def main():
    panel = pb.load()
    if not panel:
        print("panel not built — run marleg_panel_build.py first."); return
    close, volume = panel["close"], panel["volume"]
    tax = mir._tax()
    macro_of = {s: (tax["by_symbol"].get(s) or {}).get("macro") or "?" for s in close.columns}
    print(f"panel: {close.shape[1]} x {close.shape[0]} ({close.index[0].date()}->{close.index[-1].date()})")

    rets = close.pct_change()
    RET20, RET50 = close.pct_change(20), close.pct_change(50)
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

    bag = {h: {"sector": {}, "liq": {}, "vol": {}, "indbucket": {}} for h in HS}
    n = close.shape[0]; dates = 0
    for i in range(232, n - max(HS) - 1, STEP):
        ind_rank = rank_groups(RET20.iloc[i], RET50.iloc[i], abv50.iloc[i], members)
        if not ind_rank:
            continue
        dates += 1
        ir = eff_s.map(ind_rank).reindex(close.columns).fillna(1.0)
        turn = TURN.iloc[i]; rv = RVOL.iloc[i]
        L = (turn.rank(pct=True) >= 0.40).fillna(False)
        gate = (L & (ir <= 0.40) & (UD.iloc[i] > UDMA.iloc[i]) & (UD.iloc[i] > UD.iloc[i - 10]) & (FIB.iloc[i] > 0.618)).fillna(False)
        cols = close.columns[gate.values]
        if not len(cols):
            continue
        liq_r = turn.rank(pct=True); vol_r = rv.rank(pct=True)
        for s in cols:
            fwds = {h: FWD[h].iloc[i].get(s) for h in HS}
            sec = macro_of.get(s, "?")
            lq = "illiquid" if liq_r.get(s, 1) < 0.6 else "mid" if liq_r.get(s, 1) < 0.8 else "liquid"
            vb = "lo" if vol_r.get(s, 0.5) <= 0.33 else "hi" if vol_r.get(s, 0.5) >= 0.66 else "mid"
            ib = "top10" if ir.get(s, 1) <= 0.10 else "10-40"
            for h in HS:
                fv = fwds[h]
                if fv is None or fv != fv:
                    continue
                bag[h]["sector"].setdefault(sec, []).append(fv)
                bag[h]["liq"].setdefault(lq, []).append(fv)
                bag[h]["vol"].setdefault(vb, []).append(fv)
                bag[h]["indbucket"].setdefault(ib, []).append(fv)

    res = {f"h{h}": {dim: agg(bag[h][dim]) for dim in bag[h]} for h in HS}
    json.dump({"as_of_dates": dates, "from": str(close.index[0].date()), "to": str(close.index[-1].date()),
               "results": res}, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    H = 10
    print(f"\n=== WHERE the gated-long edge lives @ {H}d ({dates} dates) ===")
    for dim, order in [("liq", ["illiquid", "mid", "liquid"]), ("vol", ["lo", "mid", "hi"]), ("indbucket", ["top10", "10-40"])]:
        print(f"  by {dim}:")
        for b in order:
            r = res[f"h{H}"][dim].get(b)
            if r:
                print(f"    {b:<9} mean {r['mean']:>7}%  hit {r['hit']:>5}%  (n={r['n']})")
    print("  top sectors (by mean, n>=40):")
    secs = [(k, v) for k, v in res[f"h{H}"]["sector"].items() if v["n"] >= 40]
    for k, v in sorted(secs, key=lambda kv: -kv[1]["mean"])[:8]:
        print(f"    {k:<32} mean {v['mean']:>7}%  hit {v['hit']:>5}%  (n={v['n']})")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
