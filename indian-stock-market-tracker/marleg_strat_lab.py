"""
marleg_strat_lab.py — the meticulous strategy laboratory (runs on the canonical 5y panel).

Tests a LIBRARY of long-only models (India shorting doesn't pay) + combinations, walk-forward, at
3 horizons, SPLIT BY MARKET REGIME (bull/bear), each with a bootstrap CI — so we rank by the robust
lower bound across regimes, not the lucky point estimate. Liquidity-filtered (top-60% by turnover) so
results are tradeable. Reproducible: reads marleg_panel_build.load() — identical every run.

Outputs marleg_strat_lab.json + a printed leaderboard. Research artifact (gitignored).
"""
import os
import sys
import json
import numpy as np
import pandas as pd
import marleg_panel_build as pb
import marleg_industry_rs as mir

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_strat_lab.json")
HORIZONS = [5, 10, 21]
STEP = 3
COST = 0.25
BOOT = 400
CAP = 45000          # max samples kept per (model,horizon,regime) bucket (memory + bootstrap bound)


def rsi(c, n=14):
    d = c.diff(); up = d.clip(lower=0).rolling(n).mean(); dn = (-d.clip(upper=0)).rolling(n).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


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


def cap_extend(lst, vals):
    for v in vals:
        if v != v:           # NaN
            continue
        if len(lst) < CAP:
            lst.append(v)
        else:
            j = np.random.randint(0, len(lst))      # reservoir-ish
            lst[j] = v


def summ(a):
    a = np.asarray(a, float) * 100.0
    if len(a) < 15:
        return {"n": int(len(a))}
    boot = np.array([np.random.choice(a, len(a), replace=True).mean() for _ in range(BOOT)])
    return {"n": int(len(a)), "mean": round(float(a.mean()), 3), "hit": round(float((a > 0).mean()) * 100, 1),
            "std": round(float(a.std()), 2), "rv": round(float(a.mean() / a.std()), 3) if a.std() else None,
            "net": round(float(a.mean()) - COST, 3), "lo": round(float(np.percentile(boot, 5)), 3),
            "hi": round(float(np.percentile(boot, 95)), 3)}


def main():
    np.random.seed(11)
    panel = pb.load()
    if not panel:
        print("canonical panel not built yet — run marleg_panel_build.py first."); return
    close, volume = panel["close"], panel["volume"]
    print(f"panel: {close.shape[1]} names x {close.shape[0]} days ({close.index[0].date()}->{close.index[-1].date()}) src={panel.get('source')}")

    rets = close.pct_change()
    r5, r21, r63, r126 = close.pct_change(5), close.pct_change(21), close.pct_change(63), close.pct_change(126)
    RET20, RET50 = close.pct_change(20), close.pct_change(50)
    sma20, sma50 = close.rolling(20).mean(), close.rolling(50).mean()
    d20, d50 = close / sma20 - 1, close / sma50 - 1
    abv50 = close > sma50
    RSI = close.apply(rsi)
    dsg = np.sign(close.diff())
    UD = volume.where(dsg > 0, 0.0).rolling(20).sum() / volume.where(dsg < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    UDMA = UD.rolling(50).mean()
    FIB = (close - close.rolling(120).min()) / (close.rolling(120).max() - close.rolling(120).min()).replace(0, np.nan)
    RVOL = rets.rolling(20).std() * np.sqrt(252)
    TURN = (close * volume).rolling(20).mean()
    FWD = {h: close.shift(-h) / close - 1 for h in HORIZONS}

    eff, _, _ = mir.effective_groups(list(close.columns))
    eff_s = pd.Series(eff)
    members = {}
    for s, g in eff.items():
        members.setdefault(g, []).append(s)

    # market regime per date
    mkt = (1 + rets.mean(axis=1)).cumprod()
    bull_series = mkt > mkt.rolling(50).mean()

    MODELS = ["drift", "ind_mom", "mom63", "mom126", "rev5", "lowvol", "hi52",
              "gate_strict", "gate_cool", "gate_hot", "qmom", "mom_nothot",
              "gate_indmom", "confluence_z", "gate_pullback"]
    bag = {m: {h: {"all": [], "bull": [], "bear": []} for h in HORIZONS} for m in MODELS}
    n = close.shape[0]; dates = 0

    for i in range(232, n - max(HORIZONS) - 1, STEP):
        ind_rank = rank_groups(RET20.iloc[i], RET50.iloc[i], abv50.iloc[i], members)   # industry RS (20d/50d + breadth)
        if not ind_rank:
            continue
        dates += 1
        regime = "bull" if bool(bull_series.iloc[i]) else "bear"
        ir = eff_s.map(ind_rank).reindex(close.columns).fillna(1.0)
        turn = TURN.iloc[i]
        liquid = turn.rank(pct=True) >= 0.40                      # tradeable universe this date
        L = liquid.fillna(False)

        c5, c21, c63, c126 = r5.iloc[i], r21.iloc[i], r63.iloc[i], r126.iloc[i]
        rs, fb, ud, udma, ud10, rv = RSI.iloc[i], FIB.iloc[i], UD.iloc[i], UDMA.iloc[i], UD.iloc[i - 10], RVOL.iloc[i]

        def qr(s):                                                # cross-sectional rank within liquid names
            return s.where(L).rank(pct=True)
        rk126, rk63, rk5, rkrv = qr(c126), qr(c63), qr(c5), qr(rv)
        gate = (ir <= 0.40) & (ud > udma) & (ud > ud10) & (fb > 0.618)

        def z(s):
            x = s.where(L); return (x - x.mean()) / x.std()
        conf = z(c126).fillna(0) + z(-rv).fillna(0) + z(1 - ir).fillna(0) + z(fb).fillna(0)

        masks = {
            "drift": L,
            "ind_mom": L & (ir <= 0.20),
            "mom63": L & (rk63 >= 0.80),
            "mom126": L & (rk126 >= 0.80),
            "rev5": L & (rk5 <= 0.20),
            "lowvol": L & (rkrv <= 0.20),
            "hi52": L & (fb >= 0.95),
            "gate_strict": L & gate,
            "gate_cool": L & gate & (rs <= 70),
            "gate_hot": L & gate & (rs > 70),
            "qmom": L & (rk126 >= 0.70) & (rkrv <= 0.50),
            "mom_nothot": L & (rk126 >= 0.80) & (rs < 70),
            "gate_indmom": L & gate & (ir <= 0.20),
            "confluence_z": L & (conf.rank(pct=True) >= 0.80),
            "gate_pullback": L & gate & (c5 < 0),
        }
        for m, mask in masks.items():
            cols = close.columns[mask.fillna(False).values]
            if not len(cols):
                continue
            for h in HORIZONS:
                v = FWD[h].iloc[i][cols].values
                cap_extend(bag[m][h]["all"], v)
                cap_extend(bag[m][h][regime], v)

    res = {m: {f"h{h}": {"all": summ(bag[m][h]["all"]), "bull": summ(bag[m][h]["bull"]), "bear": summ(bag[m][h]["bear"])}
               for h in HORIZONS} for m in MODELS}
    payload = {"panel_names": close.shape[1], "panel_days": close.shape[0], "as_of_dates": dates,
               "cost_pct": COST, "horizons": HORIZONS,
               "from": str(close.index[0].date()), "to": str(close.index[-1].date()), "results": res}
    json.dump(payload, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    H = 10
    print(f"\n=== strategy leaderboard @ {H}d · net {COST}% · {dates} as-of dates · ranked by ALL-regime boot_lo ===")
    print(f"{'model':<15}{'n':>7}{'mean':>7}{'hit':>6}{'rv':>7}{'net':>7}{'lo':>7}{'hi':>7}  | {'bull_mean':>9}{'bear_mean':>10}")
    rows = sorted(res.items(), key=lambda kv: -(kv[1][f'h{H}']['all'].get('lo') or -9))
    for m, r in rows:
        a = r[f"h{H}"]["all"]; bu = r[f"h{H}"]["bull"]; be = r[f"h{H}"]["bear"]
        if a.get("n", 0) < 15:
            continue
        print(f"{m:<15}{a['n']:>7}{a['mean']:>7}{a['hit']:>6}{str(a['rv']):>7}{a['net']:>7}{a['lo']:>7}{a['hi']:>7}  | "
              f"{str(bu.get('mean')):>9}{str(be.get('mean')):>10}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
