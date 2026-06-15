"""
marleg_macro_overlay_bt.py — does MACRO-GATING the long book actually pay? (the capstone test)

The attribution (marleg_macro_gate_bt) said the gated-long alpha lives in bull + low-vol + high-dispersion
regimes and turns negative in bear. This turns that into a tradeable OVERLAY and measures whether it
helps: build the actual equity curve of a long book (non-overlapping 10-day blocks, EW, net of cost) under
three rules and compare CAGR / Sharpe / maxDD:

  always_on       hold the basket every block
  bull_gated      hold only when NIFTY-proxy > its 50DMA, else cash
  calmbull_gated  hold only when bull AND market vol is not in its top tier (bull + risk-on)

If macro-gating raises Sharpe and (especially) cuts max drawdown, the macro theory is real and tradeable.
Runs on the canonical panel. Writes marleg_macro_overlay_bt.json. Research artifact (gitignored).
"""
import os
import sys
import json
import numpy as np
import pandas as pd
import marleg_panel_build as pb
import marleg_industry_rs as mir

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_macro_overlay_bt.json")
HOLD = 10
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


def metrics(blockrets, blocks_per_year):
    r = np.asarray(blockrets, float)
    if len(r) < 10:
        return {"n": len(r)}
    eq = np.cumprod(1 + r)
    yrs = len(r) / blocks_per_year
    cagr = eq[-1] ** (1 / yrs) - 1 if eq[-1] > 0 else -1
    vol = r.std() * np.sqrt(blocks_per_year)
    sharpe = (r.mean() * blocks_per_year) / vol if vol else 0
    dd = float((eq / np.maximum.accumulate(eq) - 1).min())
    inv = float(np.mean(r != 0))                       # fraction of blocks invested (not cash)
    return {"n": len(r), "cagr_pct": round(cagr * 100, 1), "vol_pct": round(vol * 100, 1),
            "sharpe": round(sharpe, 2), "maxdd_pct": round(dd * 100, 1), "invested_pct": round(inv * 100)}


def main():
    panel = pb.load()
    if not panel:
        print("panel not built."); return
    close, volume = panel["close"], panel["volume"]
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

    eff, _, _ = mir.effective_groups(list(close.columns))
    eff_s = pd.Series(eff); members = {}
    for s, g in eff.items():
        members.setdefault(g, []).append(s)

    mkt = (1 + rets.mean(axis=1)).cumprod()
    bull = mkt > mkt.rolling(50).mean()
    mvol = RVOL.median(axis=1)
    voltop = mvol >= mvol.quantile(0.66)

    n = close.shape[0]
    bpy = 252 / HOLD
    strategies = ["ind_mom", "gate_pullback"]
    rules = ["always_on", "bull_gated", "calmbull_gated"]
    series = {s: {r: [] for r in rules} for s in strategies}

    for t in range(232, n - HOLD - 1, HOLD):                 # non-overlapping blocks
        ind_rank = rank_groups(RET20.iloc[t], RET50.iloc[t], abv50.iloc[t], members)
        if not ind_rank:
            continue
        ir = eff_s.map(ind_rank).reindex(close.columns).fillna(1.0)
        L = (TURN.iloc[t].rank(pct=True) >= 0.40).fillna(False)
        gate = (L & (ir <= 0.40) & (UD.iloc[t] > UDMA.iloc[t]) & (UD.iloc[t] > UD.iloc[t - 10]) & (FIB.iloc[t] > 0.618)).fillna(False)
        sel = {
            "ind_mom": L & (ir <= 0.20),
            "gate_pullback": gate & (close.pct_change(5).iloc[t] < 0),
        }
        fwd = close.shift(-HOLD).iloc[t] / close.iloc[t] - 1
        on_bull = bool(bull.iloc[t]); calm = on_bull and not bool(voltop.iloc[t])
        for s, mask in sel.items():
            cols = close.columns[mask.values]
            br = float(np.nanmean(fwd[cols].values)) - COST / 100 if len(cols) else 0.0
            series[s]["always_on"].append(br)
            series[s]["bull_gated"].append(br if on_bull else 0.0)
            series[s]["calmbull_gated"].append(br if calm else 0.0)

    res = {s: {r: metrics(series[s][r], bpy) for r in rules} for s in strategies}
    # market buy-hold benchmark (the universe)
    mkt_blocks = [float(mkt.iloc[t + HOLD] / mkt.iloc[t] - 1) for t in range(232, n - HOLD - 1, HOLD)]
    res["_MARKET_buyhold"] = metrics(mkt_blocks, bpy)
    # OOS robustness: does bull-gating help in BOTH halves of the sample (not period-luck)?
    oos = {}
    for s in strategies:
        oos[s] = {}
        for r in ["always_on", "bull_gated"]:
            seq = series[s][r]; mid = len(seq) // 2
            oos[s][r] = {"H1": metrics(seq[:mid], bpy), "H2": metrics(seq[mid:], bpy)}
    res["_OOS_halfsplit"] = oos
    json.dump({"hold_days": HOLD, "cost_pct": COST, "from": str(close.index[0].date()),
               "to": str(close.index[-1].date()), "results": res}, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"\n=== MACRO OVERLAY · {HOLD}d blocks · net {COST}% · ({close.index[0].date()}->{close.index[-1].date()}) ===")
    print(f"{'strategy / rule':<28}{'CAGR':>7}{'vol':>6}{'Sharpe':>7}{'maxDD':>7}{'invested':>9}")
    bh = res["_MARKET_buyhold"]
    print(f"{'MARKET buy-hold':<28}{str(bh['cagr_pct'])+'%':>7}{str(bh['vol_pct'])+'%':>6}{str(bh['sharpe']):>7}{str(bh['maxdd_pct'])+'%':>7}{'100%':>9}")
    for s in strategies:
        for r in rules:
            m = res[s][r]
            if m.get("sharpe") is not None:
                print(f"{(s+' / '+r):<28}{str(m['cagr_pct'])+'%':>7}{str(m['vol_pct'])+'%':>6}{str(m['sharpe']):>7}{str(m['maxdd_pct'])+'%':>7}{str(m['invested_pct'])+'%':>9}")
    print(f"\n--- OOS robustness (first half H1 vs second half H2) ---")
    for s in strategies:
        for r in ["always_on", "bull_gated"]:
            h1, h2 = res["_OOS_halfsplit"][s][r]["H1"], res["_OOS_halfsplit"][s][r]["H2"]
            if h1.get("sharpe") is not None and h2.get("sharpe") is not None:
                print(f"  {(s+' / '+r):<28} H1 Sharpe {h1['sharpe']:>5} maxDD {str(h1['maxdd_pct'])+'%':>7}  |  H2 Sharpe {h2['sharpe']:>5} maxDD {str(h2['maxdd_pct'])+'%':>7}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
