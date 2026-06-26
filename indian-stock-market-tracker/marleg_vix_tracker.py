"""
marleg_vix_tracker.py — India VIX as an OPTION-BUYER'S weather report, plus "what's the story": which part
of the market is under stress right now and why. Groww-only.

VIX = the market's expected 30-day NIFTY volatility priced into options. It rises with fear/uncertainty, so
a VIX move IS the market pricing a story. For an option BUYER it's the price tag on convexity:
  • LOW VIX (calm)  -> premiums are CHEAP. Good entry for buyers; risk is a vol SPIKE against a short-vol view.
  • HIGH VIX (fear) -> premiums are RICH + IV-CRUSH risk: you can be right on direction and still lose as IV
                      collapses after the event. Buyers overpay; sellers are compensated.

tracker() returns:
  • vix      level, 1y percentile + range, regime band, 5-day change (rising/falling)
  • weather  what this regime means for someone BUYING options right now
  • suffering the sectors under stress today (from the sector-bias engine — most names below their pivot),
              with the weakest names — i.e. "which market is hurting"
  • story    a plain-English line tying VIX + the stressed sectors together (news is added by the endpoint)

Read-only. Decision-support, not investment advice — I'm not a licensed advisor.

  python marleg_vix_tracker.py
"""
import datetime as dt

import marleg_vol as mv


def _ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")


def regime(v):
    """India-VIX bands (its lived range is ~9–28). Returns (label, tone, one-line read)."""
    if v is None:
        return "unknown", "dim", "no VIX read"
    if v < 12:
        return "very calm", "calm", "complacent — premiums cheap, but a vol spike can arrive without warning"
    if v < 15:
        return "calm", "calm", "low fear — option premiums are cheap; favourable for buyers, small moves"
    if v < 19:
        return "normal", "normal", "ordinary two-way risk — premiums fair"
    if v < 25:
        return "elevated", "warn", "fear rising — premiums getting rich, IV-crush risk after the event grows"
    return "high", "fear", "stress/panic — premiums expensive; buyers overpay, IV collapse can sink a right call"


def _vix_context():
    """Live India VIX + 1y percentile/range + 5-day change, from Groww INDIAVIX candles."""
    level = mv.india_vix()
    out = {"level": round(level, 2) if level else None}
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        df = g.candles("INDIAVIX", interval_min=1440, days=400, exchange="NSE")
        c = df["close"].dropna()
        lo, hi = float(c.min()), float(c.max())
        out["lo_1y"], out["hi_1y"] = round(lo, 1), round(hi, 1)
        if level and hi > lo:
            out["percentile_1y"] = round((level - lo) / (hi - lo) * 100)
        if len(c) > 6 and c.iloc[-6] > 0:
            ref = level or float(c.iloc[-1])
            out["chg5d_pct"] = round((ref / float(c.iloc[-6]) - 1) * 100, 1)
            out["falling"] = out["chg5d_pct"] < 0
    except Exception:
        pass
    return out


def tracker():
    vix = _vix_context()
    lvl = vix.get("level")
    label, tone, read = regime(lvl)
    vix["regime"], vix["tone"], vix["read"] = label, tone, read

    # what this means for an option BUYER right now
    pct = vix.get("percentile_1y")
    if lvl is None:
        weather = "No VIX read."
    elif lvl < 15:
        weather = (f"Premiums are CHEAP ({pct}th-percentile VIX) — a buyer pays little for convexity. The catch: "
                   f"calm means small moves (theta still bites), and the asymmetric risk is a vol SPIKE, not a crush.")
    elif lvl < 19:
        weather = f"Premiums are FAIR ({pct}th-percentile VIX). Neither a gift nor a trap for buyers."
    else:
        weather = (f"Premiums are RICH ({pct}th-percentile VIX) — buyers overpay and face IV-CRUSH: be right on "
                   f"direction and still lose as IV collapses post-event. This regime pays SELLERS, not buyers.")

    # which market is suffering — from the sector-bias engine (most names below pivot = under stress)
    suffering, drivers = [], []
    try:
        import marleg_sector_bias as sb
        secs = (sb.build() or {}).get("sectors", [])
        bear = [s for s in secs if s.get("bearish_now") and s.get("n", 0) >= 3]
        bear.sort(key=lambda s: -(s.get("frac_below_now") or 0))
        def _nm(n):
            if isinstance(n, dict):
                return n.get("tk") or n.get("s") or n.get("symbol") or n.get("name") or n.get("ticker")
            return n
        for s in bear[:5]:
            weak = (s.get("names") or [])
            weakest = [x for x in (_nm(n) for n in weak) if x][:3]
            suffering.append({"sector": s.get("sector"), "frac_below_pct": round((s.get("frac_below_now") or 0) * 100),
                              "persistent_bears": s.get("n_persistent_bear"), "verdict": s.get("verdict"),
                              "weakest": weakest})
            drivers.append(s.get("sector"))
    except Exception:
        pass

    if not suffering:
        story = f"India VIX {lvl} ({label}) — no broad stress; the tape is orderly. {read.capitalize()}."
    else:
        ds = ", ".join(d for d in drivers[:3] if d)
        story = (f"India VIX {lvl} ({label}, {'falling' if vix.get('falling') else 'rising'}) — "
                 f"{'no broad panic, but' if (lvl or 0) < 16 else 'stress concentrated in'} the soft spots are "
                 f"{ds}: most names there sit below their pivot today. That's where the put interest / hedging is.")

    return {"ok": True, "vix": vix, "weather": weather, "suffering": suffering, "drivers": drivers,
            "story": story, "asof": _ist(),
            "note": "VIX = expected 30-day NIFTY vol (the option-premium weather). 'Suffering' = sectors with most "
                    "names below their pivot today (the stress map). News is attached per stressed sector.",
            "caveat": "Decision-support, not investment advice — I'm not a licensed advisor. VIX tells you the "
                      "PRICE of options, not the direction of the market. Read-only."}


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = tracker()
    v = r["vix"]
    print(f"\n  India VIX {v.get('level')} · {v.get('regime')} ({v.get('percentile_1y')}th pctile of {v.get('lo_1y')}–{v.get('hi_1y')}) "
          f"· 5d {v.get('chg5d_pct')}% · {v.get('read')}")
    print(f"\n  buyer's weather: {r['weather']}")
    print(f"\n  suffering now:")
    for s in r["suffering"]:
        print(f"    {s['sector']:<22} {s['frac_below_pct']}% below pivot · weakest: {', '.join(s['weakest'])}")
    print(f"\n  story: {r['story']}")
    print(f"\n  {r['caveat']}")
