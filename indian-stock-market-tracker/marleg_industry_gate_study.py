"""
marleg_industry_gate_study.py — does INDUSTRY confirmation beat SECTOR confirmation?

The live screener requires (gate 2) U/D > its 50d MA & rising, (gate 3) price above the 0.618
fib of its 120d range, and (gate 1) a *leading group*. Today gate 1 is the coarse ~25-bucket
macro sector. The question: if we make gate 1 the granular industry instead, do the resulting
longs actually do better over the next ~2 weeks — net of India's natural upward drift?

Method (walk-forward, no look-ahead): over a 2y panel of the taxonomy universe, on a grid of
as-of dates we find the names already passing gates 2 & 3, then bucket each by whether its
*industry* and/or its *sector* was top-40% leading AS OF that date (same composite scoring for
both — only the grouping granularity differs). We compare forward returns:

   base        passes gates 2 & 3 only
   +sector     ... and its sector is leading        (today's gate)
   +industry   ... and its industry is leading       (proposed gate)
   +both       ... and both are leading
   drift       every stock with data (the do-nothing baseline)

Writes marleg_industry_gate_study.json. Pure research artifact (gitignored).
"""
import os
import json
import sys
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_industry_rs as mir

HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
OUT = os.path.join(HERE, "marleg_industry_gate_study.json")
FWDS = [5, 10, 21]          # forward holding windows (trading days)
STEP = 5                    # as-of date stride
COST = 0.25                 # round-trip cost assumption, % (STT+brokerage+slippage, delivery)


def rank_groups(ret20_row, ret50_row, above_row, members):
    """Composite leadership rank_pct per group (0=leading..1=lagging), mirroring the live engine."""
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
    r20r = df["r20"].rank(pct=True)
    r50r = df["r50"].rank(pct=True).fillna(r20r)
    brdr = df["brd"].rank(pct=True)
    df["score"] = 0.40 * r20r + 0.25 * r50r + 0.35 * brdr
    df["rank_pct"] = df["score"].rank(ascending=False, pct=True)
    return dict(zip(df["g"], df["rank_pct"]))


def summarize(rets):
    if not rets:
        return {"n": 0, "avg": None, "win": None, "net": None}
    a = np.array(rets) * 100.0
    return {"n": len(a), "avg": round(float(a.mean()), 2),
            "win": round(float((a > 0).mean()) * 100, 1),
            "net": round(float(a.mean()) - COST, 2)}


def main():
    syms = list(mir._tax()["by_symbol"].keys())
    print(f"downloading {len(syms)} taxonomy names (2y)...")
    data = yf.download([s + ".NS" for s in syms], period="2y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume = {}, {}
    for s in syms:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 300:
                close[s] = c
                volume[s] = data[t]["Volume"]
        except Exception:
            pass
    close = pd.DataFrame(close).sort_index()
    volume = pd.DataFrame(volume).reindex(close.index)
    print(f"panel: {close.shape[1]} names x {close.shape[0]} days")

    # precompute the rolling features once (vectorised), then slice per as-of date
    RET20 = close.pct_change(20)
    RET50 = close.pct_change(50)
    ABOVE = close > close.rolling(50).mean()
    d = np.sign(close.diff())
    UD = volume.where(d > 0, 0.0).rolling(20).sum() / volume.where(d < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    UDMA = UD.rolling(50).mean()
    HH = close.rolling(120).max()
    LL = close.rolling(120).min()
    FIB = (close - LL) / (HH - LL).replace(0, np.nan)

    eff, kind, _ = mir.effective_groups(list(close.columns))
    ind_members = {}
    for s, g in eff.items():
        ind_members.setdefault(g, []).append(s)
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    sec_members = {}
    for s, sec in secmap.items():
        sec_members.setdefault(sec, []).append(s)

    n = close.shape[0]
    results = {}
    for fwd in FWDS:
        buckets = {k: [] for k in ("base", "sector", "industry", "both", "drift")}
        dates_used = 0
        for i in range(130, n - fwd, STEP):
            ret20_row, ret50_row, above_row = RET20.iloc[i], RET50.iloc[i], ABOVE.iloc[i]
            ind_rank = rank_groups(ret20_row, ret50_row, above_row, ind_members)
            sec_rank = rank_groups(ret20_row, ret50_row, above_row, sec_members)
            if not ind_rank:
                continue
            dates_used += 1
            ud_i, udma_i, ud10_i, fib_i = UD.iloc[i], UDMA.iloc[i], UD.iloc[i - 10], FIB.iloc[i]
            fwd_ret = close.iloc[i + fwd] / close.iloc[i] - 1.0
            for s in close.columns:
                fr = fwd_ret.get(s)
                if pd.isna(fr):
                    continue
                buckets["drift"].append(fr)
                # gates 2 & 3 (volume + fib) — the non-group gates
                if not (pd.notna(ud_i.get(s)) and pd.notna(udma_i.get(s)) and ud_i[s] > udma_i[s] and ud_i[s] > ud10_i.get(s, np.inf)):
                    continue
                if not (pd.notna(fib_i.get(s)) and fib_i[s] > 0.618):
                    continue
                buckets["base"].append(fr)
                lead_ind = ind_rank.get(eff[s], 1) <= mir.LEAD_PCT
                lead_sec = sec_rank.get(secmap[s], 1) <= mir.LEAD_PCT
                if lead_sec:
                    buckets["sector"].append(fr)
                if lead_ind:
                    buckets["industry"].append(fr)
                if lead_ind and lead_sec:
                    buckets["both"].append(fr)
        results[f"fwd{fwd}"] = {k: summarize(v) for k, v in buckets.items()}
        results[f"fwd{fwd}"]["dates"] = dates_used

    payload = {"universe": close.shape[1], "days": close.shape[0], "step": STEP,
               "cost_pct": COST, "lead_pct": mir.LEAD_PCT, "min_members": mir.MIN_MEMBERS,
               "results": results}
    json.dump(payload, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"\n{'window':>7} {'bucket':<10} {'n':>6} {'avg%':>7} {'win%':>6} {'net%':>7}")
    for fwd in FWDS:
        r = results[f"fwd{fwd}"]
        print(f"  -- fwd {fwd}d  ({r['dates']} as-of dates) " + "-" * 30)
        for k in ("drift", "base", "sector", "industry", "both"):
            b = r[k]
            print(f"{'':7} {k:<10} {b['n']:>6} {str(b['avg']):>7} {str(b['win']):>6} {str(b['net']):>7}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
