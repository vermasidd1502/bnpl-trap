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


def classify(close, volume, tk=None, with_earnings=False):
    """Combined verdict for a candidate: footprint (+ optional earnings proximity)."""
    fp = footprint(close, volume)
    flags = list(fp["flags"])
    earn = {}
    if with_earnings and tk:
        earn = earnings_proximity(tk)
        if earn.get("ago_days") is not None and earn["ago_days"] <= 10:
            flags.append(f"reported {earn['ago_days']}d ago")
        if earn.get("in_days") is not None and 0 <= earn["in_days"] <= 7:
            flags.append(f"⚠ earnings in {earn['in_days']}d")
    return {"clean": len(flags) == 0, "flags": flags, "max_day_pct": fp.get("max_day_pct"),
            "earnings_in": earn.get("in_days"), "earnings_ago": earn.get("ago_days")}
