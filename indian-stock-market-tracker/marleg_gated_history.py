"""
marleg_gated_history.py — daily persistence + tenure for the gated long list.

The gated long list is the heart of the volume pod. Each trading day its members are
snapshotted (last-write-wins per IST date) to marleg_gated_history.json. tenure() then
reports, per current name, how long it has been on the list:
  - streak   : consecutive snapshot-days it has appeared (including today)
  - since    : the date the current streak began ("on the list since …")
  - total    : how many days it appears in the retained window
  - first_seen: the very first day it ever appeared

A name that re-qualifies day after day is a stronger, more persistent signal than a
one-day flash — that's what this surfaces. Snapshots are keyed by *trading day* (one row
per day), so intraday churn doesn't matter; the latest compute of the day wins.
"""
import os
import json
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
STORE = os.path.join(HERE, "marleg_gated_history.json")
KEEP_DAYS = 180


def _ist_date():
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")


def _load():
    try:
        with open(STORE, encoding="utf-8") as f:
            d = json.load(f)
            d.setdefault("days", {})
            return d
    except Exception:
        return {"days": {}}


def _save(d):
    try:
        with open(STORE, "w", encoding="utf-8") as f:
            json.dump(d, f)
    except Exception:
        pass


def record(symbols, date=None):
    """Snapshot today's gated symbols (overwrites today's row; last-write-wins). Prunes old days."""
    date = date or _ist_date()
    syms = sorted({(s or "").upper() for s in symbols if s})
    d = _load()
    d["days"][date] = syms
    for old in sorted(d["days"])[:-KEEP_DAYS]:      # keep only the most recent KEEP_DAYS
        d["days"].pop(old, None)
    _save(d)
    return d


def _fmt(dstr):
    try:
        return datetime.strptime(dstr, "%Y-%m-%d").strftime("%d %b")
    except Exception:
        return dstr


def tenure(symbols, date=None):
    """Per-symbol tenure on the gated list, computed from the snapshot history."""
    d = _load()
    days_map = d.get("days", {})
    dates = sorted(days_map)                          # ascending trading-day snapshots
    out = {}
    for s in symbols:
        s = (s or "").upper()
        present = [dt for dt in dates if s in days_map.get(dt, [])]
        if not present:
            out[s] = {"streak": 0, "since": None, "since_fmt": None, "total": 0,
                      "first_seen": None, "new": True}
            continue
        streak, since = 0, None
        for dt in reversed(dates):                    # count consecutive days back from latest
            if s in days_map.get(dt, []):
                streak += 1
                since = dt
            else:
                break
        out[s] = {"streak": streak, "since": since, "since_fmt": _fmt(since),
                  "total": len(present), "first_seen": present[0], "new": streak <= 1}
    return out


def annotate(picks, sym_key="s"):
    """Convenience: record the picks' symbols for today, then stamp tenure fields onto each pick.
    Mutates and returns picks. Safe to call on every gated compute (idempotent per day)."""
    syms = [p.get(sym_key) for p in picks if p.get(sym_key)]
    if not syms:
        return picks
    record(syms)
    ten = tenure(syms)
    for p in picks:
        t = ten.get((p.get(sym_key) or "").upper(), {})
        p["days_on_list"] = t.get("streak")
        p["on_since"] = t.get("since_fmt")
        p["total_days"] = t.get("total")
        p["new_today"] = t.get("new")
    return picks


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    d = _load()
    print(f"gated history: {len(d.get('days', {}))} day(s) recorded -> {STORE}")
    for dt in sorted(d.get("days", {}))[-10:]:
        print(f"  {dt}: {len(d['days'][dt])} names")
