"""
marleg_movers.py — HIGH-MOVER radar + move-potential (the 3-8%/day amplitude engine).

The user wants 3-8% daily moves. India has NO daily equity short-interest, so we proxy a squeeze /
abnormal mover from price+volume(+F&O OI):
  • move_potential : ATR% (typical daily range) + historical P(|1d move|>=5%) + expected 1-day band.
                     HIGH = ATR% >= 3 (can do 3-8% routinely); LOW = ATR% < 1.8 (can't without a catalyst).
  • abnormal_up    : a sharp up-move (event footprint) — often after a downtrend, on high RVOL.
  • short_covering : F&O names (live Groww) price UP + OI DOWN = shorts bailing = the India squeeze tell.
Ranks by move-potential × heat so you only take 3-8% shots in names that CAN move that much.
F&O leverage: a 2-3% underlying move on a future/option is already your 6-8%.
HONEST: amplitude + a covering proxy, NOT US-style short-interest; every shot pairs with the daily loss-limit.

  python marleg_movers.py            # scan F&O universe (Groww, fresh) -> marleg_movers.json
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_movers.json")


def move_potential(c, h=None, l=None, lookback=120):
    c = c.dropna()
    if len(c) < 50:
        return None
    tail = c.tail(lookback + 1)
    ret = tail.pct_change().dropna()
    if h is not None and l is not None:
        h = h.reindex(c.index).tail(lookback + 1); l = l.reindex(c.index).tail(lookback + 1)
        pc = tail.shift(1)
        tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atrp = float((tr.rolling(14).mean() / tail).iloc[-1]) * 100
    else:
        atrp = float(ret.abs().mean()) * 100
    if not np.isfinite(atrp):
        atrp = float(ret.abs().mean()) * 100
    p5 = float((ret.abs() >= 0.05).mean()) * 100
    p3 = float((ret.abs() >= 0.03).mean()) * 100
    tag = "HIGH" if atrp >= 3 else "MED" if atrp >= 1.8 else "LOW"
    px = float(c.iloc[-1])
    return {"atrp": round(atrp, 2), "p5": round(p5, 1), "p3": round(p3, 1), "tag": tag,
            "exp_lo": round(px * (1 - atrp / 100), 1), "exp_hi": round(px * (1 + atrp / 100), 1),
            "fno_2pct_leveraged": "a 2% underlying move ~ 6-8% on a near-month future/ATM option"}


def _squeeze_industries():
    """The backtest-blessed squeeze whitelist/traps (marleg_squeeze_study.py): which industries' squeezes PAY
    vs FADE. Pooled, a squeeze is a 46%-win lottery; the industry filter is what makes it tradeable."""
    try:
        d = json.load(open(os.path.join(HERE, "marleg_squeeze_study.json"), encoding="utf-8"))
        return set(d.get("whitelist", [])), set(d.get("traps", [])), d
    except Exception:
        return set(), set(), None


def scan(oi_top=14):
    import marleg_data as md
    import marleg_options_monitor as mom
    import marleg_eventfilter as ef
    NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    FOCUS, TRAPS, SQ = _squeeze_industries()
    U = sorted(mom.FNO_UNDERLYINGS)
    print(f"scanning {len(U)} F&O underlyings for high-movers (move-potential + abnormal-up + squeeze)...")
    panel = md.daily_panel(U, period="1y")
    close, volume, high, low = panel["close"], panel["volume"], panel["high"], panel["low"]
    if close.shape[1] < 30:
        print(f"[thin] only {close.shape[1]} names — keeping prior radar."); return None
    rows = []
    for s in close.columns:
        c = close[s].dropna()
        if len(c) < 60:
            continue
        mp = move_potential(c, high[s], low[s])
        if not mp:
            continue
        px = float(c.iloc[-1])
        ret1 = float(c.iloc[-1] / c.iloc[-2] - 1) * 100
        ret5 = float(c.iloc[-1] / c.iloc[-6] - 1) * 100 if len(c) > 6 else 0.0
        trend20 = float(c.iloc[-1] / c.iloc[-21] - 1) * 100 if len(c) > 21 else 0.0
        v = volume[s].reindex(c.index)
        rvol = float(v.iloc[-1] / v.tail(21).iloc[:-1].mean()) if v.tail(21).iloc[:-1].mean() else 1.0
        fp = ef.footprint(c, v)
        abnormal_up = (ret1 >= 4) or (not fp["clean"] and ret1 > 0)
        bounce = abnormal_up and trend20 < -3            # sharp up after a downtrend = the squeeze shape
        ind = (SECT.get(s) or {}).get("industry") or (SECT.get(s) or {}).get("sector")
        sq_class = "focus" if ind in FOCUS else "trap" if ind in TRAPS else "neutral"
        heat = abs(ret5) * max(1.0, rvol) * (mp["atrp"] / 2)
        if bounce and sq_class == "focus":               # backtest says this industry's squeezes PAY — boost it
            heat *= 1.5
        elif bounce and sq_class == "trap":              # squeezy but the snap fades — demote, don't chase
            heat *= 0.5
        rows.append({"s": s, "n": NAMES.get(s, s), "price": round(px, 2), "industry": ind, "sq_class": sq_class,
                     "ret1": round(ret1, 1), "ret5": round(ret5, 1), "trend20": round(trend20, 1),
                     "rvol": round(rvol, 1), **{k: mp[k] for k in ("atrp", "p5", "tag", "exp_lo", "exp_hi")},
                     "abnormal_up": bool(abnormal_up), "bounce_shape": bool(bounce),
                     "clean": fp["clean"], "event_flags": list(fp["flags"]), "heat": round(heat, 1)})
    rows.sort(key=lambda x: x["heat"], reverse=True)

    # enrich the hottest names with live F&O OI -> short-covering (the India squeeze tell). Best-effort.
    for r in rows[:oi_top]:
        try:
            q = mom.option_quote(r["s"])
            if isinstance(q, dict) and "error" not in q:
                oc, dc = q.get("oi_change"), q.get("day_change_pct")
                r["oi_change"] = oc; r["oi_change_pct"] = q.get("oi_change_pct")
                if oc is not None and dc is not None:
                    r["short_covering"] = bool(dc > 0 and oc < 0)
                    r["oi_signal"] = ("short covering (price up, OI down)" if (dc > 0 and oc < 0) else
                                      "long buildup (price up, OI up)" if (dc > 0 and oc > 0) else
                                      "short buildup (price down, OI up)" if (dc < 0 and oc > 0) else
                                      "long unwinding (price down, OI down)")
        except Exception:
            pass
        try:                                                       # NEWS/EARNINGS GATE — confirm the mover isn't an event spike
            e = ef.earnings_proximity(r["s"])
            r["earnings_in"], r["earnings_ago"] = e.get("in_days"), e.get("ago_days")
            ru = ef.pre_earnings_runup(r.get("ret5"), e.get("in_days"))
            if e.get("ago_days") is not None and e["ago_days"] <= 10:
                r["event_flags"].append(f"reported {e['ago_days']}d ago"); r["clean"] = False
            if ru:
                r["event_flags"].append(ru); r["clean"] = False; r["pre_earn_runup"] = True
            elif e.get("in_days") is not None and 0 <= e["in_days"] <= 7:
                r["event_flags"].append(f"earnings in {e['in_days']}d"); r["clean"] = False
        except Exception:
            pass

    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    hi = [r for r in rows if r["tag"] == "HIGH"]
    n_focus_sq = len([r for r in rows if r.get("bounce_shape") and r.get("sq_class") == "focus"])
    sq_summary = None
    if SQ:
        sq_summary = {"whitelist": sorted(FOCUS), "traps": sorted(TRAPS),
                      "pooled": SQ.get("pooled"), "base5": SQ.get("pooled_baseline_net5"),
                      "top": [{k: x.get(k) for k in ("industry", "n_events", "freq_per_kday", "win5", "net5", "edge5", "verdict")}
                              for x in (SQ.get("industries") or [])[:14]]}
    out = {"asof": ist, "n": len(rows), "n_high": len(hi), "universe": close.shape[1],
           "n_focus_squeeze": n_focus_sq, "squeeze_study": sq_summary, "movers": rows[:60],
           "note": "Move-potential = amplitude (can it do 3-8%?), NOT a direction call. India has no equity "
                   "short-interest; short_covering is an F&O OI proxy. High amplitude = high RISK — size to "
                   "the daily loss-limit. F&O leverage turns a 2-3% underlying move into 6-8%. Squeezes are now "
                   "graded by industry: backtest-blessed FOCUS industries' snaps pay (~52-71% win); TRAP "
                   "industries' snaps fade (don't chase)."}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"high-movers: {len(rows)} (HIGH move-potential {len(hi)}) -> {OUT}")
    for r in rows[:12]:
        sc = " · SHORT-COVERING" if r.get("short_covering") else ""
        print(f"  {r['s']:<12} ATR%{r['atrp']:>5}  {r['tag']:<4} ret5 {r['ret5']:>5}%  RVOL {r['rvol']:>4}  P(>=5%) {r['p5']:>4}%{sc}")
    return out


def build():
    try:
        return json.load(open(OUT, encoding="utf-8"))
    except Exception:
        return {"ok": False, "error": "movers radar not built yet — run marleg_movers.py", "movers": []}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    scan()
