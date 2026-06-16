"""
marleg_eventfilter.py — don't get fooled by earnings / sudden news.

A stock can be "gated" or "leading" only because it gapped on a one-off catalyst (results, news) — that
is NOT sustainable accumulation, and chasing it is how you get burned. We can't read news reliably from
a US IP, but we detect its FOOTPRINT and flag the name as event-CONTAMINATED so the strict list can drop it:

  • abnormal single day   — one day's close-to-close move is huge (>=9%)
  • spike dominance        — a single up-day is most of the recent gain (organic moves grind, news jumps)
  • volume blast           — the big day had RVOL >> normal (a reaction, not steady demand)
  • earnings proximity     — reported in the last ~10 sessions, OR reports within ~7 sessions (don't enter
                             a short-term trade in front of a binary event) — yfinance dates, cached
  • pre-earnings run-up    — up sharply (>=6%) INTO an upcoming results date: the "green into results,
                             round-trips overnight" trap. Hard-flagged, never on the strict list.

clean = organic/sustained -> keep on the strict list.  flagged = event-driven -> drop / ⚠.
"""
import os
import json
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ECACHE = os.path.join(HERE, "marleg_earnings_cache.json")


def footprint(close, volume, lookback=20):
    """Event-footprint from price/volume only (fast, whole-universe, no extra data)."""
    c = close.dropna().tail(lookback + 1)
    if len(c) < 10:
        return {"clean": True, "flags": []}
    rets = c.pct_change().dropna()
    if rets.empty:
        return {"clean": True, "flags": []}
    v = volume.reindex(c.index)
    avgvol = float(v.iloc[:-1].mean()) if len(v.dropna()) > 1 else None
    flags = []
    mx = float(rets.abs().max())
    mxday = rets.abs().idxmax()
    if mx >= 0.09:
        flags.append(f"abnormal {rets.loc[mxday] * 100:+.0f}% day")
    up = rets[rets > 0]
    if up.sum() > 0:
        dom = float(up.max() / up.sum())
        if dom >= 0.55 and up.max() >= 0.05:
            flags.append(f"1 day = {dom * 100:.0f}% of the gain")
    if avgvol and pd.notna(v.get(mxday)) and float(v.loc[mxday]) >= 3 * avgvol and mx >= 0.05:
        flags.append(f"vol blast {float(v.loc[mxday]) / avgvol:.0f}x")
    return {"clean": len(flags) == 0, "flags": flags, "max_day_pct": round(mx * 100, 1)}


def coiled_breakout(high, low, close, box=20, atr_n=14, look=126, q=0.25):
    """Is the LATEST bar a breakout out of an ATR-CONTRACTED base — the user's 'fight then burst'?
    Backtested (marleg_breakout_timing_study, 5y canonical panel) as the highest-conviction breakout
    subset: net ~1.2%/10d and, unusually, still POSITIVE in bear tapes (+0.62%) where raw breakouts
    LOSE (-0.25%). It's a ⚡ conviction tag, not a hard gate (it only ties our fib gate overall, but
    it's what protects you when the market is weak). Entry rule: buy the breakout CLOSE — the pullback
    retest underperforms and only fills ~70% of the time. The coil ALONE predicts nothing; the edge is
    the resolution UP, so we require the actual breakout."""
    c = close.dropna()
    if len(c) < look + atr_n + 5:
        return False
    idx = c.index
    h = high.reindex(idx)
    l = low.reindex(idx)
    hh20 = h.rolling(box).max().shift(1)
    brk = bool(c.iloc[-1] > hh20.iloc[-1]) and bool(c.iloc[-2] <= hh20.iloc[-2])
    if not brk:
        return False
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atrp = (tr.rolling(atr_n).mean() / c).shift(1)            # base measured through yesterday
    thr = atrp.rolling(look, min_periods=40).quantile(q).iloc[-1]
    return bool(atrp.iloc[-1] <= thr) if thr == thr else False


def _load_ecache():
    try:
        return json.load(open(ECACHE, encoding="utf-8"))
    except Exception:
        return {}


def _save_ecache(d):
    try:
        tmp = ECACHE + ".tmp"
        json.dump(d, open(tmp, "w", encoding="utf-8"))
        os.replace(tmp, ECACHE)
    except Exception:
        pass


def earnings_proximity(tk, refresh_days=5):
    """Best-effort days-to-next / days-since-last earnings via yfinance, cached (refreshes weekly)."""
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    cache = _load_ecache()
    hit = cache.get(tk.upper())
    if hit and hit.get("d") and (today - datetime.fromisoformat(hit["d"]).date()).days < refresh_days:
        return {k: hit.get(k) for k in ("in_days", "ago_days", "next", "last")}
    out = {"in_days": None, "ago_days": None, "next": None, "last": None}
    try:
        import yfinance as yf
        t = yf.Ticker(tk.upper() + ".NS")
        nxt = None
        cal = t.calendar
        if isinstance(cal, dict):
            ed = cal.get("Earnings Date")
            if ed:
                nxt = ed[0] if isinstance(ed, (list, tuple)) else ed
        last = None
        try:
            ds = t.get_earnings_dates(limit=8)
            if ds is not None and len(ds):
                past = [d.date() for d in ds.index.tz_localize(None) if d.tz_localize(None).date() <= today] if hasattr(ds.index, "tz_localize") else []
                past = sorted([d for d in (x.date() for x in ds.index.to_pydatetime()) if d <= today])
                if past:
                    last = past[-1]
        except Exception:
            pass
        if nxt:
            out["next"] = str(nxt); out["in_days"] = (nxt - today).days if hasattr(nxt, "year") else None
        if last:
            out["last"] = str(last); out["ago_days"] = (today - last).days
    except Exception:
        pass
    cache[tk.upper()] = {"d": str(today), **out}
    _save_ecache(cache)
    return out


def pre_earnings_runup(ret_recent, earnings_in, runup_thresh=6.0, window=7):
    """The trap that round-trips a green trade overnight: a sharp run-up INTO a known upcoming
    results date. If the name is up >= runup_thresh% (recent) AND earnings land within `window`
    sessions, return a flag string; else None. Holding a short-term trade through a binary event
    is uncompensated risk — strike it. (This is the move that burned us.)"""
    if earnings_in is None or not (0 <= earnings_in <= window):
        return None
    if ret_recent is not None and ret_recent >= runup_thresh:
        return f"pre-earnings run-up +{ret_recent:.0f}% (results in {earnings_in}d)"
    return None


def classify(close, volume, tk=None, with_earnings=False):
    """Combined verdict for a candidate: footprint (+ optional earnings proximity / pre-earnings run-up)."""
    fp = footprint(close, volume)
    flags = list(fp["flags"])
    earn = {}
    if with_earnings and tk:
        earn = earnings_proximity(tk)
        if earn.get("ago_days") is not None and earn["ago_days"] <= 10:
            flags.append(f"reported {earn['ago_days']}d ago")
        if earn.get("in_days") is not None and 0 <= earn["in_days"] <= 7:
            c = close.dropna()
            ret5 = (float(c.iloc[-1] / c.iloc[-6] - 1) * 100) if len(c) > 6 else None
            flags.append(pre_earnings_runup(ret5, earn["in_days"]) or f"earnings in {earn['in_days']}d")
    return {"clean": len(flags) == 0, "flags": flags, "max_day_pct": fp.get("max_day_pct"),
            "earnings_in": earn.get("in_days"), "earnings_ago": earn.get("ago_days")}
