"""
marleg_cuphandle_study.py — cup-with-handle in India: the EDGE + a RETROSPECTION of what actually won.

Detects the pattern (cup = fall from a prior high, round a bottom, recover; handle = a shallow pullback near
the rim; trigger = breakout above the handle pivot) on the canonical 5y panel, then answers the user's
questions head-on:
  • PREMISE OF JUDGMENT: a "win" = the next-10-session close-to-close return is positive NET of 0.30% cost.
    HORIZON = 10 trading days (~2 weeks); 5d and 20d also reported. Entry = the breakout bar's close.
  • WHAT WON: winners vs losers compared on sector, volatility (ATR%), cup depth, prior run-up, market regime.
  • CONFIRMATION FILTERS (Kirkpatrick / Fidelity deck — to cut FALSE/FAILED breakouts, i.e. the "pattern
    morphs into a trap"): does requiring close >= pivot+1%, or a volume breakout, improve the win rate?

  python marleg_cuphandle_study.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_cuphandle_study.json")
COST = 0.30
RNG = np.random.default_rng(0)


def _stats(arr):
    x = np.asarray(arr, float)
    if len(x) < 25:
        return None
    nb = 500; xs = x if len(x) <= 8000 else RNG.choice(x, 8000, replace=False)
    lb = float(np.percentile([xs[RNG.integers(0, len(xs), len(xs))].mean() for _ in range(nb)], 5)) * 100 - COST
    return {"n": int(len(x)), "win": round(float((x > 0).mean()) * 100, 1),
            "net": round(float(x.mean()) * 100 - COST, 3), "lb": round(lb, 3)}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    panel = pb.load()
    if not panel:
        print("no panel"); return
    close = panel["close"].copy()
    if getattr(close.index, "tz", None) is not None:
        close.index = close.index.tz_localize(None)
    high = panel["high"].reindex(close.index); low = panel["low"].reindex(close.index); vol = panel["volume"].reindex(close.index)
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    mkt = (1 + close.pct_change().mean(axis=1)).cumprod()
    bull_by_date = (mkt > mkt.rolling(50).mean())

    rows = []
    for s in close.columns:
        c = close[s].dropna()
        if len(c) < 320:
            continue
        cv = c.values; hv = high[s].reindex(c.index).values; lv = low[s].reindex(c.index).values; vv = vol[s].reindex(c.index).values
        bull = bull_by_date.reindex(c.index).fillna(False).values
        vavg = pd.Series(vv).rolling(20).mean().shift(1).values
        pc = np.concatenate([[np.nan], cv[:-1]])
        tr = np.maximum.reduce([hv - lv, np.abs(hv - pc), np.abs(lv - pc)])
        atrp = pd.Series(tr).rolling(14).mean().values / cv * 100
        f5 = np.concatenate([cv[5:] / cv[:-5] - 1, np.full(5, np.nan)])
        f10 = np.concatenate([cv[10:] / cv[:-10] - 1, np.full(10, np.nan)])
        f20 = np.concatenate([cv[20:] / cv[:-20] - 1, np.full(20, np.nan)])
        n = len(cv); last = -99
        for t in range(160, n - 21):
            if t - last < 12:
                continue
            ph = float(cv[t - 160:t - 35].max()); i_ph = t - 160 + int(cv[t - 160:t - 35].argmax())
            if i_ph >= t - 12:
                continue
            bottom = float(cv[i_ph:t - 8].min()); depth = (ph - bottom) / ph if ph else 0
            if not (0.15 <= depth <= 0.45):
                continue
            handle = cv[t - 8:t + 1]; hh = float(handle.max()); hl = float(handle.min())
            if hh < ph * 0.90 or (hh - hl) / hh > 0.12:
                continue
            if not (cv[t] >= hh * 0.999 and cv[t] >= cv[t - 1]):
                continue
            last = t
            if f10[t] != f10[t]:
                continue
            atrp_t = float(np.nanmean(tr[max(0, t - 14):t]) / cv[t] * 100) if cv[t] else None
            vb = float(np.nanmean(vv[max(0, t - 20):t]))
            rvol = float(vv[t] / vb) if (vb == vb and vb > 0) else 1.0
            held = bool(t + 1 < n and cv[t + 1] >= hh)            # breakout HELD the next day = not a false breakout
            rows.append({"s": s, "sec": SECT.get(s, {}).get("sector") or "Other",
                         "depth": depth, "atrp": atrp_t if atrp_t == atrp_t else None,
                         "prior_run": float(cv[i_ph] / cv[max(0, i_ph - 60)] - 1), "bull": bool(bull[t]), "rvol": rvol,
                         "f5": f5[t], "f10": f10[t], "f20": f20[t],
                         "conf_hold": held, "conf_pct": bool(t + 1 < n and cv[t + 1] >= hh * 1.01), "conf_vol": bool(rvol >= 1.5)})

    f10 = [r["f10"] for r in rows]
    win = [r for r in rows if r["f10"] * 100 - COST > 0]
    loss = [r for r in rows if r["f10"] * 100 - COST <= 0]

    def avg(rs, k):
        v = [r[k] for r in rows if r in rs and r[k] is not None] if False else [r[k] for r in rs if r.get(k) is not None]
        return round(float(np.mean(v)), 3) if v else None

    res = {"cost": COST, "n": len(rows),
           "overall": {"5d": _stats([r["f5"] for r in rows]), "10d": _stats(f10), "20d": _stats([r["f20"] for r in rows])},
           "winners_vs_losers": {
               "winners": {"n": len(win), "depth": avg(win, "depth"), "atrp": avg(win, "atrp"),
                           "prior_run": avg(win, "prior_run"), "bull_rate": round(np.mean([r["bull"] for r in win]) * 100, 1) if win else None,
                           "rvol": avg(win, "rvol")},
               "losers": {"n": len(loss), "depth": avg(loss, "depth"), "atrp": avg(loss, "atrp"),
                          "prior_run": avg(loss, "prior_run"), "bull_rate": round(np.mean([r["bull"] for r in loss]) * 100, 1) if loss else None,
                          "rvol": avg(loss, "rvol")}},
           "by_regime": {"bull": _stats([r["f10"] for r in rows if r["bull"]]),
                         "bear": _stats([r["f10"] for r in rows if not r["bull"]])},
           "confirmation_filters_10d": {
               "all_triggers": _stats(f10),
               "held_next_day(not_false_brk)": _stats([r["f10"] for r in rows if r["conf_hold"]]),
               "next_close_>=_pivot+1%": _stats([r["f10"] for r in rows if r["conf_pct"]]),
               "breakout_on_volume(RVOL>=1.5)": _stats([r["f10"] for r in rows if r["conf_vol"]]),
               "held_AND_bull": _stats([r["f10"] for r in rows if r["conf_hold"] and r["bull"]])}}
    # win-rate by sector (>=40 events)
    secs = {}
    for r in rows:
        secs.setdefault(r["sec"], []).append(r["f10"])
    sec_tbl = sorted([{"sec": k, "n": len(v), "win": round(float((np.array(v) > 0).mean()) * 100, 1),
                       "net": round(float(np.mean(v)) * 100 - COST, 3)} for k, v in secs.items() if len(v) >= 40],
                     key=lambda x: x["net"], reverse=True)
    res["by_sector"] = sec_tbl
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    o = res["overall"]["10d"]; w = res["winners_vs_losers"]
    print(f"cup-with-handle triggers: {res['n']}  |  PREMISE: win = next-10d return net>0  |  HORIZON: 10 trading days")
    print(f"  overall 10d: win {o['win']}%  net {o['net']}%  lb {o['lb']}%   (5d net {res['overall']['5d']['net']}, 20d net {res['overall']['20d']['net']})")
    print(f"\nWHAT WON (winners {w['winners']['n']} vs losers {w['losers']['n']}):")
    print(f"  {'metric':<14}{'winners':>10}{'losers':>10}")
    for k, lbl in [("depth", "cup depth"), ("atrp", "ATR% (vol)"), ("prior_run", "prior run-up"), ("bull_rate", "% in bull"), ("rvol", "breakout RVOL")]:
        print(f"  {lbl:<14}{str(w['winners'][k]):>10}{str(w['losers'][k]):>10}")
    print(f"\nBY REGIME (10d net): bull {res['by_regime']['bull']['net']}% (n={res['by_regime']['bull']['n']})  |  bear {res['by_regime']['bear']['net']}% (n={res['by_regime']['bear']['n']})")
    print("\nCONFIRMATION FILTERS (10d) — do they cut false breakouts?")
    for k, v in res["confirmation_filters_10d"].items():
        if v:
            print(f"  {k:<32} n={v['n']:>5}  win {v['win']}%  net {v['net']}%  lb {v['lb']}%")
    print("\nTOP SECTORS by net (>=40 events):")
    for x in sec_tbl[:6]:
        print(f"  {x['sec'][:26]:<28} n={x['n']:>4}  win {x['win']}%  net {x['net']}%")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
