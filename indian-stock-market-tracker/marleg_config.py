"""
marleg_config.py — the ONE place the knobs live. Dynamic > hardcoded; the harness backbone.

Every engine should read its universe, thresholds, cache TTLs, weights and FRESHNESS tier from here instead
of hardcoding constants. Two payoffs:
  • organic, not brittle — change a weight/threshold/universe in one place, the whole system follows.
  • latency honesty — each served payload is STAMPED with its freshness tier, so the UI never implies a
    live tick feed when it's actually EOD-daily (the delay you correctly sensed).

Freshness tiers — what data of this kind is allowed to be, and what we tell the user:
  LIVE     held positions, hard-exits, a watched name's quote  → Groww real-time, ~15s
  INTRADAY "what's moving now" mover/feed scan                 → live 1-5min bars, ~3m   (replaces the 3h EOD feed)
  EOD      cross-sectional ranks, conviction calibration       → daily panel, fine to be a day old
  STATIC   taxonomies, fundamentals                            → ~daily
"""
import os
import datetime as dt

DATA_SOURCE = os.environ.get("MARLEG_DATA_SOURCE", "groww")

FRESHNESS = {
    "live":     {"ttl": 15,    "label": "LIVE"},
    "intraday": {"ttl": 180,   "label": "~3m"},
    "eod":      {"ttl": 10800,  "label": "EOD daily"},
    "static":   {"ttl": 86400,  "label": "daily"},
}

# notify-engine conviction meter — was hardcoded inside the engine; now tunable in one place
CONVICTION = {
    "signal_max": 60.0, "vol2x": 18.0, "volup": 9.0, "trend": 14.0,
    "noise_floor": 0.45, "noise_span": 0.55, "erratic_pct": 0.90, "erratic_mult": 0.70,
    "high": 65, "med": 40,
}

# mover-scan thresholds
MOVERS = {"move1_thresh": 0.03, "move5_thresh": 0.06, "top": 40}

# risk budget per trade (the user's 1-3% band)
RISK = {"default_pct": 2.0, "min_pct": 1.0, "max_pct": 3.0}


def ist_now():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30))


def market_open(now=None):
    """NSE cash session 09:15-15:30 IST, Mon-Fri, excluding holidays (marleg_vol.NSE_HOLIDAYS)."""
    now = now or ist_now()
    try:
        import marleg_vol as mv
        if now.date().isoformat() in mv.NSE_HOLIDAYS:
            return False
    except Exception:
        pass
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return 9 * 60 + 15 <= t <= 15 * 60 + 30


def freshness(tier, built=None):
    """Stamp a payload with how fresh it really is — the latency-honesty contract."""
    f = FRESHNESS.get(tier, FRESHNESS["eod"])
    live = tier == "live" or (tier == "intraday" and market_open())
    return {"tier": tier, "label": f["label"], "ttl": f["ttl"],
            "as_of": ist_now().strftime("%Y-%m-%d %H:%M IST"), "source_built": built,
            "is_live": live,
            "note": "live tick path" if live else f"{f['label']} — NOT a live tick feed (data may lag the tape)"}


def universe():
    """The tradable universe — DERIVED dynamically from the canonical classified panel, not a hardcoded list.
    Returns [] if the panel isn't built (caller falls back)."""
    try:
        import marleg_panel_build as pb
        P = pb.load()
        if P and "close" in P:
            return list(P["close"].columns)
    except Exception:
        pass
    return []


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print("DATA_SOURCE:", DATA_SOURCE)
    print("market_open now:", market_open())
    u = universe()
    print(f"dynamic universe: {len(u)} names (from panel)" + (f" e.g. {u[:6]}" if u else " — panel not built"))
    for t in ("live", "intraday", "eod"):
        print(" ", t, "->", freshness(t))
