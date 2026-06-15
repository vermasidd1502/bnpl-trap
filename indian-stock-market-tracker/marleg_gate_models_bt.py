"""
marleg_gate_models_bt.py — compare a FAMILY of gated-long entry models, by distribution.

The question (yours): instead of only entering when the swing gate fully fires, what about names
APPROACHING the gate (one condition away), or names that just CROSSED, or ones that cleared but are
OVER-HEATED (chase now vs wait for the pullback)? And — don't crown one model; look at the DISTRIBUTION
of fits across the whole family and find the robust cluster (the "best-fitting", not the most outlandish).

Gate (the strict baseline): industry-RS top-40% × U/D > 50d-MA & rising × fib > 0.618.

Models (entry signal at the as-of bar; long; forward returns at 3/5/10d, net of cost):
  drift                  every stock with data (the do-nothing baseline)
  strict                 all three gates fire (confirmed)
  strict_cool            strict & RSI <= 70   (not over-heated)
  strict_hot             strict & RSI > 70    (chase the hot ones)
  strict_hot_pullback    strict & RSI > 70, but ENTER on the next-day dip (wait-for-pullback)
  fresh_cross            newly strict vs the prior as-of date (caught at the cross)
  approach_fib           lead & U/D-pass & fib in [0.50,0.618]   (anticipate the fib)
  approach_ud            lead & fib-pass & U/D in [0.9*MA, MA] & rising  (anticipate the U/D cross)
  approach_ind           U/D-pass & fib-pass & industry rank in (40%,50%]  (anticipate the group)
  one_away               exactly one condition "near", the other two pass

For each model x horizon: n, mean, median, hit%, std, return/vol, net (mean-cost), and a BOOTSTRAP
90% CI on the mean — so we judge by the robust lower bound (boot_lo), not the lucky point estimate.

Writes marleg_gate_models_bt.json. Research artifact (gitignored).
"""
import os
import sys
import json
import numpy as np
import pandas as pd
import marleg_data as md
import marleg_industry_rs as mir

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_gate_models_bt.json")
HORIZONS = [3, 5, 10]
STEP = 3
COST = 0.25          # swing round-trip %, delivery
BOOT = 600


def rsi(close, n=14):
    d = close.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def rank_groups(ret20_row, ret50_row, above_row, members):
    rows = []
    for g, mem in members.items():
        mm = [s for s in mem if pd.notna(ret20_row.get(s))]
        if not mm:
            continue
        r20 = np.nanmean([ret20_row[s] for s in mm])
        r50v = [ret50_row[s] for s in mm if pd.notna(ret50_row.get(s))]
        r50 = np.nanmean(r50v) if r50v else np.nan
        brd = np.mean([1.0 if bool(above_row.get(s, False)) else 0.0 for s in mm])
        rows.append((g, r20, r50, brd))
    if not rows:
        return {}
    df = pd.DataFrame(rows, columns=["g", "r20", "r50", "brd"])
    r20r = df["r20"].rank(pct=True); r50r = df["r50"].rank(pct=True).fillna(r20r); brdr = df["brd"].rank(pct=True)
    df["score"] = 0.40 * r20r + 0.25 * r50r + 0.35 * brdr
    df["rank_pct"] = df["score"].rank(ascending=False, pct=True)
    return dict(zip(df["g"], df["rank_pct"]))


def summarize(rets):
    a = np.array([x for x in rets if x is not None and not np.isnan(x)]) * 100.0
    if len(a) < 12:
        return {"n": int(len(a))}
    boot = np.array([np.random.choice(a, len(a), replace=True).mean() for _ in range(BOOT)])
    return {"n": int(len(a)), "mean": round(float(a.mean()), 3), "median": round(float(np.median(a)), 3),
            "hit": round(float((a > 0).mean()) * 100, 1), "std": round(float(a.std()), 2),
            "ret_vol": round(float(a.mean() / a.std()), 3) if a.std() else None,
            "net": round(float(a.mean()) - COST, 3),
            "boot_lo": round(float(np.percentile(boot, 5)), 3), "boot_hi": round(float(np.percentile(boot, 95)), 3),
            "p25": round(float(np.percentile(a, 25)), 2), "p75": round(float(np.percentile(a, 75)), 2)}


def main():
    np.random.seed(7)
    tax = mir._tax()
    syms = list(tax["by_symbol"].keys())
    print(f"loading {len(syms)} taxonomy names (2y) via marleg_data...")
    panel = md.daily_panel(syms, period="2y", groww_days=520, groww_cap=450)
    close = panel["close"]
    volume = panel["volume"]
    print(f"panel: {close.shape[1]} names x {close.shape[0]} days (source: {panel.get('source')})")
    if close.shape[0] < 220 or close.shape[1] < 80:
        print("insufficient history."); return

    RET20 = close.pct_change(20); RET50 = close.pct_change(50)
    ABOVE = close > close.rolling(50).mean()
    d = np.sign(close.diff())
    UD = volume.where(d > 0, 0.0).rolling(20).sum() / volume.where(d < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    UDMA = UD.rolling(50).mean()
    HH = close.rolling(120).max(); LL = close.rolling(120).min()
    FIB = (close - LL) / (HH - LL).replace(0, np.nan)
    RSI = close.apply(rsi)
    FWD = {h: close.shift(-h) / close - 1 for h in HORIZONS}
    FWD1 = {h: close.shift(-(h + 1)) / close.shift(-1) - 1 for h in HORIZONS}   # entry delayed 1 bar (pullback)

    eff, _, _ = mir.effective_groups(list(close.columns))
    eff_series = pd.Series(eff)
    members = {}
    for s, g in eff.items():
        members.setdefault(g, []).append(s)

    MODELS = ["drift", "strict", "strict_cool", "strict_hot", "strict_hot_pullback", "fresh_cross",
              "approach_fib", "approach_ud", "approach_ind", "one_away"]
    bag = {m: {h: [] for h in HORIZONS} for m in MODELS}
    n = close.shape[0]
    prev_strict = None
    dates = 0
    for i in range(130, n - max(HORIZONS) - 1, STEP):
        ind_rank = rank_groups(RET20.iloc[i], RET50.iloc[i], ABOVE.iloc[i], members)
        if not ind_rank:
            continue
        dates += 1
        ir = eff_series.map(ind_rank).reindex(close.columns).fillna(1.0)
        ud, udma, ud10, fib, r = UD.iloc[i], UDMA.iloc[i], UD.iloc[i - 10], FIB.iloc[i], RSI.iloc[i]
        lead = ir <= 0.40
        near_ind = (ir > 0.40) & (ir <= 0.50)
        ud_pass = (ud > udma) & (ud > ud10)
        ud_near = (ud > 0.9 * udma) & (ud <= udma) & (ud > ud10)
        fib_pass = fib > 0.618
        fib_near = (fib > 0.50) & (fib <= 0.618)
        hot = r > 70
        strict = (lead & ud_pass & fib_pass).fillna(False)
        masks = {
            "drift": pd.Series(True, index=close.columns),
            "strict": strict,
            "strict_cool": strict & (r <= 70),
            "strict_hot": strict & hot,
            "fresh_cross": (strict & (~prev_strict)) if prev_strict is not None else pd.Series(False, index=close.columns),
            "approach_fib": (lead & ud_pass & fib_near).fillna(False),
            "approach_ud": (lead & ud_near & fib_pass).fillna(False),
            "approach_ind": (near_ind & ud_pass & fib_pass).fillna(False),
        }
        # exactly one "near", the other two pass
        near_cnt = near_ind.astype(int) + ud_near.astype(int) + fib_near.astype(int)
        pass_ok = (lead | near_ind) & (ud_pass | ud_near) & (fib_pass | fib_near)
        masks["one_away"] = (pass_ok & (near_cnt == 1)).fillna(False)
        for m, mask in masks.items():
            cols = close.columns[mask.values]
            for h in HORIZONS:
                bag[m][h].extend(FWD[h].iloc[i][cols].tolist())
        # over-heated pullback: strict & hot, enter next bar only if it dipped
        ph = strict & hot & (close.iloc[i + 1] < close.iloc[i])
        cols = close.columns[ph.fillna(False).values]
        for h in HORIZONS:
            bag["strict_hot_pullback"][h].extend(FWD1[h].iloc[i][cols].tolist())
        prev_strict = strict

    res = {m: {f"h{h}": summarize(bag[m][h]) for h in HORIZONS} for m in MODELS}
    payload = {"panel_names": close.shape[1], "panel_days": close.shape[0], "source": panel.get("source"),
               "as_of_dates": dates, "cost_pct": COST, "horizons": HORIZONS, "results": res}
    json.dump(payload, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    H = 5
    print(f"\n=== entry-model distribution @ {H}d (net cost {COST}%, {dates} as-of dates) ===")
    print(f"{'model':<22}{'n':>7}{'mean':>7}{'med':>7}{'hit%':>6}{'ret/vol':>8}{'net':>7}{'boot[lo,hi]':>16}")
    rows = sorted(res.items(), key=lambda kv: -(kv[1][f'h{H}'].get('boot_lo') or -9))
    for m, r in rows:
        b = r[f"h{H}"]
        if b.get("n", 0) < 12:
            print(f"{m:<22}{b.get('n', 0):>7}   (too few)"); continue
        print(f"{m:<22}{b['n']:>7}{b['mean']:>7}{b['median']:>7}{b['hit']:>6}{str(b['ret_vol']):>8}{b['net']:>7}{('['+str(b['boot_lo'])+','+str(b['boot_hi'])+']'):>16}")
    print(f"\nrank = robust lower bound (boot_lo), not the lucky mean. wrote {OUT}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
