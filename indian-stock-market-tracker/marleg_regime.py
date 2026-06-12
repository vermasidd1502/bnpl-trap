"""
Marle-G — REGIME / DISPERSION DIAL: the scenario-alpha gate.

The Narrative Scenario Engine only has edge when the market is DISPERSED (stocks
moving idiosyncratically, on their own stories). When it's COHERENT (everything moving
together on macro risk-on/off), single-name theses are dormant — don't deploy.

This makes the user's thesis measurable ("normal vol -> stocks move alike; different
regime -> incoherent moves"). Grounded in the cross-sectional-dispersion literature:
dispersion + VIX forecast the alpha available to stock-pickers.

  cross-sectional dispersion = std across stocks of each day's return (high = idiosyncratic)
  avg pairwise correlation    = how together names move (high = macro-driven)
  scenario gauge 0-100        = high -> stock-picker's market (engine LIVE)

Per-sector dispersion shows WHERE the incoherence is = where a thesis may be activating.

  python marleg_regime.py
"""
import os, json, sys, time
import numpy as np, pandas as pd, yfinance as yf
import marleg_volume_scan as v

HERE = os.path.dirname(os.path.abspath(__file__))
U = v.SEED
CACHE_FILE = os.path.join(HERE, "marleg_regime_cache.json")
DISK_TTL = 6 * 3600          # regime moves slowly; recompute at most every 6h, survive restarts
try:
    SECT = json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json")))["by_symbol"]
    def macro_of(s): return (SECT.get(s) or {}).get("macro", "Other")
except Exception:
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json")))
    def macro_of(s): return (SECT.get(s) or {}).get("sector", "Other")


def _pctile(series, value):
    s = series.dropna()
    return float((s < value).mean() * 100) if len(s) else 50.0


def compute(force=False):
    if not force:                                  # disk cache -> instant serve, survives restarts
        try:
            d = json.load(open(CACHE_FILE))
            if time.time() - d.get("_ts", 0) < DISK_TTL:
                return d
        except Exception:
            pass
    px = yf.download([s + ".NS" for s in U], period="1y", interval="1d",
                     group_by="ticker", progress=False, threads=True)
    closes = {}
    for s in U:
        try:
            c = px[s + ".NS"]["Close"].dropna()
            if len(c) > 120:
                closes[s] = c
        except Exception:
            pass
    R = pd.DataFrame(closes).pct_change().dropna(how="all")
    n = R.shape[1]
    # cross-sectional dispersion: std across names each day (daily series)
    disp = R.std(axis=1)
    disp_now = float(disp.tail(20).mean())
    disp_pctile = _pctile(disp.rolling(20).mean(), disp_now)
    # average pairwise correlation over trailing 60d
    corr = R.tail(60).corr().values
    iu = np.triu_indices_from(corr, k=1)
    avg_corr = float(np.nanmean(corr[iu]))
    # rolling avg-corr distribution (weekly windows) for a percentile
    corr_hist = []
    idx = R.index
    for i in range(60, len(R), 5):
        c = R.iloc[i - 60:i].corr().values
        corr_hist.append(np.nanmean(c[np.triu_indices_from(c, k=1)]))
    corr_pctile = _pctile(pd.Series(corr_hist), avg_corr)
    # India VIX
    vix_now = vix_pct = None
    try:
        vx = yf.Ticker("^INDIAVIX").history(period="1y")["Close"].dropna()
        vix_now = float(vx.iloc[-1]); vix_pct = _pctile(vx, vix_now)
    except Exception:
        pass
    # scenario-alpha gauge: high dispersion + low correlation = stock-picker's market
    gauge = round(0.55 * disp_pctile + 0.45 * (100 - corr_pctile))
    regime = ("DISPERSED — stock-picker's market (scenario-alpha LIVE)" if gauge >= 60 else
              "COHERENT — macro-driven, names move together (scenario-alpha DORMANT)" if gauge <= 35 else
              "TRANSITIONAL — mixed; thesis edge partial")
    # per-sector dispersion (where the incoherence is -> where a thesis may be activating)
    sect_disp = {}
    by_sec = {}
    for s in R.columns:
        by_sec.setdefault(macro_of(s), []).append(s)
    for sec, members in by_sec.items():
        if len(members) >= 3:
            sd = R[members].tail(20).std(axis=1).mean()
            sect_disp[sec] = round(float(sd) * 100, 3)
    hot = sorted(sect_disp.items(), key=lambda kv: -kv[1])
    out = {
        "universe": n, "asof": str(R.index[-1].date()),
        "dispersion_now": round(disp_now * 100, 3), "dispersion_pctile": round(disp_pctile),
        "avg_correlation": round(avg_corr, 3), "corr_pctile": round(corr_pctile),
        "vix": round(vix_now, 2) if vix_now else None, "vix_pctile": round(vix_pct) if vix_pct else None,
        "gauge": gauge, "regime": regime,
        "sector_dispersion": [[k, v] for k, v in hot],   # list of pairs (immune to JSON key-sorting)
    }
    out["_ts"] = time.time()
    try:
        json.dump(out, open(CACHE_FILE, "w"))
    except Exception:
        pass
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = compute()
    print(f"\nMARLE-G REGIME DIAL — {r['asof']} | {r['universe']} names\n")
    print(f"  SCENARIO-ALPHA GAUGE: {r['gauge']}/100   ->  {r['regime']}\n")
    print(f"  cross-sectional dispersion : {r['dispersion_now']}%  ({r['dispersion_pctile']}th pctile vs 1y)  [high = idiosyncratic]")
    print(f"  avg pairwise correlation   : {r['avg_correlation']}  ({r['corr_pctile']}th pctile)            [high = macro-driven]")
    print(f"  India VIX                  : {r['vix']}  ({r['vix_pctile']}th pctile)")
    print(f"\n  WHERE THE INCOHERENCE IS (top-dispersion sectors — where a thesis may be activating):")
    for sec, d in r["sector_dispersion"][:8]:
        print(f"    {sec:<34}{d}%")
    print("\n  Read: gauge HIGH = stocks moving on their own stories -> deploy the scenario engine.")
    print("  gauge LOW = everything moving on macro -> single-name theses are dormant; wait.")


if __name__ == "__main__":
    main()
