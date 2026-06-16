"""
marleg_vix_study.py — the VIX CONSCIENCE: how does each sector behave when VIX is calm vs volatile?

Conditions sector daily returns on the India VIX regime (canonical 5y panel + ^INDIAVIX):
  CALM     : VIX in the bottom third of its trailing-1y range  (low fear, stable)
  ELEVATED : middle third
  STRESS   : top third OR spiking (>= +8% over 3 sessions)     (high fear / volatile)
Plus VIX-UP days (VIX rose that day) — what actually holds up when fear jumps.

Per sector: mean DAILY return in each regime, hit-rate, and VIX-BETA (slope of the sector's daily return
on VIX's daily % change). Negative VIX-beta + positive STRESS return = a DEFENSIVE (rises when fear rises);
large positive VIX-beta = high-beta (gets hit when VIX spikes — but also the big mover when VIX falls).

  python marleg_vix_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_vix_study.json")


def vix_series(index):
    try:
        import yfinance as yf
        v = yf.Ticker("^INDIAVIX").history(period="6y")["Close"]
        if getattr(v.index, "tz", None) is not None:
            v.index = v.index.tz_localize(None)
        v.index = v.index.normalize()
        v = v[~v.index.duplicated(keep="last")]
        return v.reindex(index).ffill()
    except Exception as e:
        print("VIX fetch failed:", e)
        return None


def _close(tk, period="2y"):
    try:
        import yfinance as yf
        s = yf.Ticker(tk).history(period=period)["Close"].dropna()
        if getattr(s.index, "tz", None) is not None:
            s.index = s.index.tz_localize(None)
        s.index = s.index.normalize()
        return s[~s.index.duplicated(keep="last")]
    except Exception:
        return None


DRIVER_IMPACT = {
    "US_VIX": "global risk-off — high-beta & midcaps hit hardest; IT/pharma exporters relatively cushioned",
    "Brent": "crude UP → OMCs / aviation / paints / tyres / logistics HURT (input cost); upstream (ONGC, Oil India) BENEFIT",
    "Gold": "flight to safety (risk-off); broad caution; jewellers' margins mixed",
    "USDINR": "weak rupee → IT & pharma EXPORTERS benefit; oil importers, capital goods, airlines HURT",
    "NIFTY": "local sell-off — broad risk-off; defensives (FMCG, pharma) hold up best",
    "SP500": "global equity direction — risk-on/off spills into high-beta here",
}


def drivers():
    """Why is India VIX moving? Attribute its recent change to global/commodity drivers (quantitative;
    news stays best-effort/manual). Correlates India-VIX daily change to US VIX, Brent, gold, USDINR,
    NIFTY, S&P; flags which driver's recent move is most CONSISTENT with the VIX move + the industry read."""
    iv = _close("^INDIAVIX")
    if iv is None or iv.notna().sum() < 150:
        return None
    cols = {"IndiaVIX": iv}
    for k, t in {"US_VIX": "^VIX", "Brent": "BZ=F", "Gold": "GC=F", "USDINR": "INR=X",
                 "NIFTY": "^NSEI", "SP500": "^GSPC"}.items():
        s = _close(t)
        if s is not None:
            cols[k] = s
    df = pd.DataFrame(cols).sort_index()
    chg = df.pct_change()
    ivc = chg["IndiaVIX"]
    ivs = df["IndiaVIX"].dropna()
    ivchg5 = float(ivs.iloc[-1] / ivs.iloc[-6] - 1) * 100 if len(ivs) > 6 else 0.0
    contrib = []
    for k in cols:
        if k == "IndiaVIX":
            continue
        d = pd.concat([ivc, chg[k]], axis=1).dropna()
        if len(d) < 120:
            continue
        corr = float(d.iloc[:, 0].corr(d.iloc[:, 1]))
        s = df[k].dropna()
        chg5 = float(s.iloc[-1] / s.iloc[-6] - 1) * 100 if len(s) > 6 else None
        consistent = ivchg5 != 0 and (np.sign(corr) * np.sign(chg5 or 0)) == np.sign(ivchg5)
        contrib.append({"driver": k, "corr": round(corr, 2),
                        "chg5d": round(chg5, 1) if chg5 is not None else None,
                        "consistent": bool(consistent), "score": round(abs(corr) * abs(chg5 or 0), 1),
                        "impact": DRIVER_IMPACT.get(k, "")})
    contrib.sort(key=lambda x: x["score"], reverse=True)
    lead = [c for c in contrib if c["consistent"]][:2]
    direction = "RISING" if ivchg5 > 1 else "FALLING" if ivchg5 < -1 else "FLAT"
    txt = f"India VIX {direction} ({ivchg5:+.1f}% over 5d). " + (
        "Most consistent driver(s): " + "; ".join(f"{c['driver']} ({c['chg5d']:+.1f}% 5d, ρ={c['corr']})" for c in lead)
        if lead else "no single global/commodity driver dominates — likely local/idiosyncratic.")
    return {"vix_chg5d": round(ivchg5, 1), "direction": direction, "contributions": contrib,
            "attribution": txt, "lead": [c["driver"] for c in lead]}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    panel = pb.load()
    if not panel:
        print("no canonical panel — run marleg_panel_build.py")
        return
    close = panel["close"].copy()
    if getattr(close.index, "tz", None) is not None:        # Groww panel is tz-aware (Asia/Kolkata); strip
        close.index = close.index.tz_localize(None)          # so external (yfinance, naive) series align by date
    close.index = close.index.normalize()
    rets = close.pct_change()
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    secof = {s: (SECT.get(s, {}).get("sector") or "Other") for s in close.columns}
    bysec = {}
    for s, sec in secof.items():
        bysec.setdefault(sec, []).append(s)
    bysec = {sec: cols for sec, cols in bysec.items() if len(cols) >= 4}
    secret = pd.DataFrame({sec: rets[cols].mean(axis=1) for sec, cols in bysec.items()})

    vix = vix_series(close.index)
    if vix is None or vix.notna().sum() < 250:
        print("insufficient VIX history"); return
    vchg = vix.pct_change()
    qlo = vix.rolling(252, min_periods=120).quantile(0.33)
    qhi = vix.rolling(252, min_periods=120).quantile(0.67)
    spike = (vix / vix.shift(3) - 1) >= 0.08
    regime = pd.Series("ELEVATED", index=vix.index, dtype=object)
    regime[vix <= qlo] = "CALM"
    regime[(vix >= qhi) | spike] = "STRESS"
    vixup = vchg > 0

    rows = []
    for sec in secret.columns:
        r = secret[sec]
        row = {"sector": sec, "n": int(r.notna().sum())}
        for reg in ["CALM", "ELEVATED", "STRESS"]:
            x = r[(regime == reg) & r.notna()]
            row[reg] = round(float(x.mean()) * 100, 3) if len(x) > 20 else None
            row[reg + "_win"] = round(float((x > 0).mean()) * 100, 1) if len(x) > 20 else None
        xu = r[vixup & r.notna()]
        row["vix_up"] = round(float(xu.mean()) * 100, 3) if len(xu) > 20 else None
        d = pd.concat([r, vchg], axis=1).dropna()
        if len(d) > 100 and np.var(d.iloc[:, 1].values) > 0:
            row["vix_beta"] = round(float(np.cov(d.iloc[:, 0], d.iloc[:, 1])[0, 1] / np.var(d.iloc[:, 1].values)), 3)
        else:
            row["vix_beta"] = None
        rows.append(row)

    vnow = float(vix.iloc[-1])
    abslvl = "CALM" if vnow < 13 else "NORMAL" if vnow < 17 else "ELEVATED" if vnow < 22 else "STRESS"
    cur = {"vix": round(vnow, 2), "regime": abslvl, "regime_trailing": str(regime.iloc[-1]),
           "chg5d": round(float(vix.iloc[-1] / vix.iloc[-6] - 1) * 100, 1) if len(vix) > 6 else None,
           "trailing_lo": round(float(qlo.iloc[-1]), 1) if qlo.iloc[-1] == qlo.iloc[-1] else None,
           "trailing_hi": round(float(qhi.iloc[-1]), 1) if qhi.iloc[-1] == qhi.iloc[-1] else None}
    rows.sort(key=lambda x: (x["STRESS"] if x["STRESS"] is not None else -99), reverse=True)
    dr = drivers()
    json.dump({"asof_vix": cur, "sectors": rows, "drivers": dr}, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    if dr:
        print("\nVIX DRIVERS:", dr["attribution"])
        for c in dr["contributions"][:4]:
            print(f"  {c['driver']:<8} ρ={c['corr']:>5}  5d {str(c['chg5d']):>6}%  {'<= consistent' if c['consistent'] else ''}")

    print(f"India VIX now {cur['vix']} — {cur['regime']} ({cur['chg5d']:+}% 5d; 1y band {cur['trailing_lo']}-{cur['trailing_hi']})")
    print(f"\n  {'sector':<24}{'CALM':>8}{'ELEV':>8}{'STRESS':>8}{'VIXup':>8}{'β-VIX':>8}   (mean daily %)")
    for x in rows:
        print(f"  {x['sector'][:23]:<24}{str(x['CALM']):>8}{str(x['ELEVATED']):>8}{str(x['STRESS']):>8}{str(x['vix_up']):>8}{str(x['vix_beta']):>8}")
    bystress = sorted([x for x in rows if x["STRESS"] is not None], key=lambda x: x["STRESS"], reverse=True)
    bycalm = sorted([x for x in rows if x["CALM"] is not None], key=lambda x: x["CALM"], reverse=True)
    print("\nDEFENSIVE — hold up best when VIX spikes (highest STRESS-day return):", ", ".join(x["sector"] for x in bystress[:4]))
    print("HIGH-BETA — hit hardest in stress, but the biggest UP-movers when VIX is calm:", ", ".join(x["sector"] for x in bycalm[:4]))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
