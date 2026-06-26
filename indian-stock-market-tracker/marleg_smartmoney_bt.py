"""
marleg_smartmoney_bt.py — DOES INSTITUTIONAL ACCUMULATION PREDICT FORWARD RETURNS IN INDIA?

The empirical spine the whole "follow the smart money" thesis rests on. Cross-sectional, look-ahead-safe.

For each stock × quarter we take the change in ownership (the legal, public shareholding pattern):
    dFII, dDII, dPromoter, dInst (=dFII+dDII), dInst+Promoter, dRetail
…and measure the FORWARD return, entered 30 days AFTER quarter-end (SEBI filing is within 21 days, so this is
strictly post-public — no look-ahead), held 1 quarter (and 2 quarters as a second horizon).

Then, the way a quant actually judges a signal:
  • cross-sectional Spearman rank-IC per quarter  → mean IC, IC t-stat, hit-rate (% quarters IC>0)
  • quintile spread (top-minus-bottom forward return) + Q1..Q5 monotonicity
  • component decomposition (is it FII? DII? promoter? is retail-buying a NEGATIVE signal?)
  • regime split (does it work in up-quarters vs down-quarters?)
  • a tradable portfolio: long top-quintile, equal-weight, quarterly rebal — vs equal-weight-all market

Prices: canonical Groww panel (marleg_panel_build). Signal: screener.in shareholding (cached to disk here so
re-runs are instant). Honest caveats (few quarters, survivorship, rounding) printed at the end.

    python marleg_smartmoney_bt.py            # ~250 most-liquid names
    python marleg_smartmoney_bt.py --n 400    # wider
"""
import os
import sys
import json
import time
import datetime as dt

import numpy as np
import pandas as pd

import marleg_panel_build as pb
import marleg_smartmoney as sm

HERE = os.path.dirname(os.path.abspath(__file__))
SHCACHE = os.path.join(HERE, "marleg_smartmoney_bt_shcache.json")
OUT = os.path.join(HERE, "marleg_smartmoney_bt.json")
QEND = {"Mar": (3, 31), "Jun": (6, 30), "Sep": (9, 30), "Dec": (12, 31)}
SIGS = ["dfii", "ddii", "dpro", "dinst", "dinst_pro", "dretail"]
SIG_LBL = {"dfii": "ΔFII", "ddii": "ΔDII", "dpro": "ΔPromoter", "dinst": "ΔInst (FII+DII)",
           "dinst_pro": "ΔInst+Promoter", "dretail": "ΔRetail (public)"}


def _qend(label):
    try:
        mon, yr = label.split()
        m, d = QEND[mon[:3]]
        return dt.date(int(yr), m, d)
    except Exception:
        return None


def _load_shcache():
    try:
        return json.load(open(SHCACHE, encoding="utf-8"))
    except Exception:
        return {}


def _px_after(ser, d, beyond_ok=False):
    ts = pd.Timestamp(d)
    i = ser.index.searchsorted(ts)
    if i >= len(ser):
        return None
    return float(ser.iloc[i])


def gather(n_universe=250, lag_days=30):
    P = pb.load()
    if not P or "close" not in P:
        print("no canonical panel — run:  python marleg_panel_build.py"); return None
    close, vol = P["close"], P["volume"]
    turnover = (close * vol).median().sort_values(ascending=False)
    liquid = [s for s in turnover.head(n_universe).index]
    if getattr(close.index, "tz", None) is not None:      # panel index is tz-aware (IST); our lookups are naive
        close = close.copy(); close.index = close.index.tz_localize(None)
    cache = _load_shcache()
    rows = []
    fetched = 0
    for k, sym in enumerate(liquid):
        if sym not in close.columns:
            continue
        if sym in cache:
            sh = cache[sym]
        else:
            try:
                sh = sm.fetch_shareholding(sym)
            except Exception:
                sh = None
            cache[sym] = sh
            fetched += 1
            time.sleep(0.25)
            if fetched % 25 == 0:
                json.dump(cache, open(SHCACHE, "w", encoding="utf-8"))
                print(f"  …fetched {fetched} new ({k+1}/{len(liquid)})")
        if not sh or not sh.get("quarters"):
            continue
        qs = sh["quarters"]
        fii, dii, pro, pub = sh.get("fii") or [], sh.get("dii") or [], sh.get("promoter") or [], sh.get("public") or []
        ser = close[sym].dropna()
        if len(ser) < 60:
            continue
        for i in range(1, len(qs)):
            def delta(a):
                return (a[i] - a[i - 1]) if (len(a) > i and a[i] is not None and a[i - 1] is not None) else None
            dfii, ddii, dpro, dpub = delta(fii), delta(dii), delta(pro), delta(pub)
            if dfii is None and ddii is None:
                continue
            qd = _qend(qs[i])
            if not qd:
                continue
            entry = qd + dt.timedelta(days=lag_days)
            p0 = _px_after(ser, entry)
            p1 = _px_after(ser, entry + dt.timedelta(days=90))
            p2 = _px_after(ser, entry + dt.timedelta(days=180))
            if not p0 or p0 <= 0:
                continue
            rows.append({
                "q": qd.isoformat(), "sym": sym,
                "dfii": dfii, "ddii": ddii, "dpro": dpro, "dretail": dpub,
                "dinst": (dfii or 0) + (ddii or 0), "dinst_pro": (dfii or 0) + (ddii or 0) + (dpro or 0),
                "fwd1": (p1 / p0 - 1) if p1 else None, "fwd2": (p2 / p0 - 1) if p2 else None})
    json.dump(cache, open(SHCACHE, "w", encoding="utf-8"))
    return pd.DataFrame(rows)


def _ic_block(df, sig, fwd, min_names=20):
    ics, spreads, perq, q5 = [], [], [], [[] for _ in range(5)]
    for q, g in df.groupby("q"):
        g = g.dropna(subset=[sig, fwd])
        if len(g) < min_names or g[sig].nunique() < 5:
            continue
        ic = g[sig].corr(g[fwd], method="spearman")
        if pd.isna(ic):
            continue
        g = g.sort_values(sig)
        k = len(g) // 5
        if k < 1:
            continue
        spreads.append(g[fwd].iloc[-k:].mean() - g[fwd].iloc[:k].mean())
        for b in range(5):
            seg = g[fwd].iloc[b * k:(b + 1) * k]
            if len(seg):
                q5[b].append(seg.mean())
        ics.append(ic)
        perq.append({"q": q, "n": int(len(g)), "ic": round(ic, 3)})
    ics = np.array(ics)
    if len(ics) == 0:
        return None
    t = float(ics.mean() / ics.std() * np.sqrt(len(ics))) if (len(ics) > 1 and ics.std() > 0) else float("nan")
    return {"mean_ic": round(float(ics.mean()), 4), "t": round(t, 2), "hit": round(float((ics > 0).mean()), 2),
            "n_q": int(len(ics)), "mean_qspread_pct": round(float(np.mean(spreads)) * 100, 2),
            "quintiles_pct": [round(float(np.mean(b)) * 100, 2) if b else None for b in q5], "per_q": perq}


def portfolio(df, sig, fwd="fwd1", min_names=20):
    qs = sorted(df["q"].unique())
    eq_top, eq_ls, eq_mkt = 1.0, 1.0, 1.0
    n = 0
    for q in qs:
        g = df[df["q"] == q].dropna(subset=[sig, fwd])
        if len(g) < min_names or g[sig].nunique() < 5:
            continue
        g = g.sort_values(sig)
        k = len(g) // 5
        if k < 1:
            continue
        top = g[fwd].iloc[-k:].mean(); bot = g[fwd].iloc[:k].mean(); mkt = g[fwd].mean()
        eq_top *= (1 + top); eq_ls *= (1 + (top - bot)); eq_mkt *= (1 + mkt)
        n += 1
    if n == 0:
        return None
    ann = 4.0 / n
    return {"n_q": n, "top_total_pct": round((eq_top - 1) * 100, 1), "top_cagr_pct": round((eq_top ** ann - 1) * 100, 1),
            "ls_total_pct": round((eq_ls - 1) * 100, 1), "ls_cagr_pct": round((eq_ls ** ann - 1) * 100, 1),
            "mkt_total_pct": round((eq_mkt - 1) * 100, 1), "mkt_cagr_pct": round((eq_mkt ** ann - 1) * 100, 1)}


def regime_split(df, sig, fwd="fwd1", min_names=20):
    mkt = df.dropna(subset=[fwd]).groupby("q")[fwd].mean()
    up_q = set(mkt[mkt > 0].index); dn_q = set(mkt[mkt <= 0].index)
    out = {}
    for name, qset in [("up", up_q), ("down", dn_q)]:
        sub = df[df["q"].isin(qset)]
        b = _ic_block(sub, sig, fwd, min_names=max(15, min_names - 5))
        out[name] = {"mean_ic": b["mean_ic"], "t": b["t"], "n_q": b["n_q"]} if b else None
    return out


def run(n_universe=250):
    print(f"gathering shareholding × forward returns ({n_universe} most-liquid names)…")
    df = gather(n_universe)
    if df is None or df.empty:
        print("no observations"); return None
    res = {"asof": dt.datetime.now().strftime("%Y-%m-%d"), "n_universe": n_universe,
           "n_names": int(df["sym"].nunique()), "n_obs": int(len(df)), "n_quarters": int(df["q"].nunique()),
           "span": [min(df["q"]), max(df["q"])], "signals": {}}
    for sig in SIGS:
        for fwd in ("fwd1", "fwd2"):
            b = _ic_block(df, sig, fwd)
            if b:
                res["signals"][f"{sig}|{fwd}"] = b
    head = "dinst_pro"
    res["portfolio"] = portfolio(df, head)
    res["regime"] = regime_split(df, head)
    res["headline_signal"] = head
    json.dump(res, open(OUT, "w", encoding="utf-8"))
    return res


def _fmt(b):
    star = "***" if abs(b["t"]) >= 3 else "**" if abs(b["t"]) >= 2 else "*" if abs(b["t"]) >= 1.5 else ""
    return f"IC {b['mean_ic']:+.4f}  t={b['t']:+.2f}{star}  hit {b['hit']*100:.0f}%  spread {b['mean_qspread_pct']:+.2f}%  ({b['n_q']}q)"


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 250
    r = run(n)
    if not r:
        raise SystemExit
    print(f"\n{'='*78}\n DOES INSTITUTIONAL ACCUMULATION PREDICT FORWARD RETURNS? (India, cross-sectional)\n{'='*78}")
    print(f" {r['n_names']} names · {r['n_obs']} stock-quarters · {r['n_quarters']} quarters · {r['span'][0]} → {r['span'][1]}")
    print(f"\n 1-QUARTER FORWARD (entry = quarter-end + 30d, look-ahead-safe):")
    for sig in SIGS:
        b = r["signals"].get(f"{sig}|fwd1")
        if b:
            print(f"   {SIG_LBL[sig]:<22} {_fmt(b)}")
    print(f"\n 2-QUARTER FORWARD:")
    for sig in SIGS:
        b = r["signals"].get(f"{sig}|fwd2")
        if b:
            print(f"   {SIG_LBL[sig]:<22} {_fmt(b)}")
    head = r["headline_signal"]; hb = r["signals"].get(f"{head}|fwd1")
    if hb:
        print(f"\n {SIG_LBL[head]} quintiles (Q1 sell→Q5 buy), 1q fwd %:  " +
              "  ".join(f"Q{i+1} {v:+.2f}" for i, v in enumerate(hb["quintiles_pct"]) if v is not None))
    pf = r.get("portfolio")
    if pf:
        print(f"\n PORTFOLIO ({SIG_LBL[head]}, long top-quintile, quarterly rebal, {pf['n_q']}q):")
        print(f"   top-quintile  {pf['top_cagr_pct']:+.1f}%/yr   |   market (eq-wt)  {pf['mkt_cagr_pct']:+.1f}%/yr   |   long-short  {pf['ls_cagr_pct']:+.1f}%/yr")
    rg = r.get("regime")
    if rg and rg.get("up") and rg.get("down"):
        print(f"\n REGIME ({SIG_LBL[head]}):  up-quarters IC {rg['up']['mean_ic']:+.4f} (t={rg['up']['t']:+.2f})   |   down-quarters IC {rg['down']['mean_ic']:+.4f} (t={rg['down']['t']:+.2f})")
    print(f"\n CAVEATS: quarterly data → only ~{r['n_quarters']} time periods (low power, wide error bars); "
          f"panel = today's liquid names (survivorship → optimistic); screener.in % rounded to 0.01 (small Δ noisy); "
          f"no costs (quarterly rebal is cheap). t≥2 = real-ish, t≥3 = strong.")
