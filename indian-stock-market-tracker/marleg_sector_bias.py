"""
marleg_sector_bias.py — SECTORAL bull/bear monitor by pivot acceptance, with put candidates for the
persistently-bearish names (e.g. Information Technology).

For every F&O name it computes the same pivot-acceptance bias as marleg_pivots.bias (how often price holds
above the daily pivot), then aggregates by NSE sector. Sectors are ranked most-bearish → most-bull. Names
that have lived BELOW the pivot for a long stretch (low %above over 120d AND currently below) are flagged
as PUT candidates — but with the honest caveat baked in.

THE HONEST CAVEAT (this pod's own backtests, marleg_validated_edges): India drifts UP and shorting/bearish
signals have been ANTI-signals (weak names bounce); and a long put bleeds THETA on a slow drift. So treat a
put as a HEDGE against long exposure, or only on a strongly CONFIRMED downtrend — NOT as a standalone edge.

Computed off the in-memory canonical panel (fast, no per-name network calls). Read-only, decision-support.
"""
import os
import json

HERE = os.path.dirname(os.path.abspath(__file__))


def _name_bias(h, l, c, lb60=60, lb120=120):
    Hp, Lp, Cp = h.shift(1), l.shift(1), c.shift(1)
    P = (Hp + Lp + Cp) / 3.0
    above = (c > P).dropna()
    if len(above) < 40:
        return None
    vals = list(above.astype(bool))
    pa60 = round(sum(vals[-lb60:]) / len(vals[-lb60:]) * 100, 1)
    pa120 = round(sum(vals[-lb120:]) / len(vals[-lb120:]) * 100, 1)
    cur = vals[-1]; st = 0
    for v in reversed(vals):
        if v == cur:
            st += 1
        else:
            break
    runs, c0, n = [], None, 0
    for v in vals:
        if v == c0:
            n += 1
        else:
            if c0 is not None:
                runs.append((c0, n))
            c0, n = v, 1
    runs.append((c0, n))
    below_runs = [x for s, x in runs if not s]
    return {"pct_above60": pa60, "pct_above120": pa120, "side": "ABOVE" if cur else "BELOW",
            "streak": int(st), "avg_below_run": round(sum(below_runs) / len(below_runs), 1) if below_runs else 0,
            "price": round(float(c.iloc[-1]), 2)}


def build(lookback=60):
    import marleg_panel_build as pb
    import marleg_options_monitor as mom
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    try:
        NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
    except Exception:
        NAMES = {}
    P = pb.load()
    close, high, low = P["close"], P["high"], P["low"]
    fno = set(mom.FNO_UNDERLYINGS) | set(mom.INDEX_STEP)
    secs = {}
    for s in close.columns:
        if s not in fno:
            continue
        sec = (SECT.get(s) or {}).get("sector")
        if not sec:
            continue
        c = close[s].dropna()
        b = _name_bias(high[s].reindex(c.index), low[s].reindex(c.index), c)
        if not b:
            continue
        # persistent bear = lived below the pivot a long time AND still below
        b["persistent_bear"] = bool(b["pct_above120"] < 38 and b["side"] == "BELOW")
        step = mom.INDEX_STEP.get(s) or mom._strike_step(b["price"])
        b["atm"] = round(b["price"] / step) * step
        secs.setdefault(sec, []).append({"s": s, "n": NAMES.get(s, s), **b})
    out = []
    for sec, names in secs.items():
        names.sort(key=lambda x: x["pct_above60"])      # most-bearish first
        mean60 = round(sum(n["pct_above60"] for n in names) / len(names), 1)
        mean120 = round(sum(n["pct_above120"] for n in names) / len(names), 1)
        nbear = sum(1 for n in names if n["side"] == "BELOW")
        npb = sum(1 for n in names if n["persistent_bear"])
        verdict = ("BULL" if mean60 >= 58 else "BEAR" if mean60 <= 42 else "CHOP")
        frac = round(nbear / len(names), 2)
        bearish_now = bool(frac >= 0.7)              # most constituents below the pivot RIGHT NOW (fresh weakness)
        if bearish_now:
            strat = ("⚡ BEARISH NOW — most names sit below the pivot today. The data says INTRADAY-short the "
                     "weakest (exit by close); do NOT hold puts/shorts overnight — India's drift is overnight "
                     "(+0.37%/night) and intraday is the only short-friendly window (theta also bleeds a held put).")
        elif npb >= 1 or verdict == "BEAR":
            strat = "persistent weakness — intraday-short or hedge; puts only on a confirmed downside break (overnight drift + theta fight a held short)."
        else:
            strat = "no bearish edge — sector isn't weak enough to short; stand aside (India is long-biased)."
        out.append({"sector": sec, "n": len(names), "mean_above60": mean60, "mean_above120": mean120,
                    "n_below_now": nbear, "frac_below_now": frac, "bearish_now": bearish_now,
                    "n_persistent_bear": npb, "verdict": verdict, "strategy": strat, "names": names})
    # most-bearish-NOW first (fresh weakness), then by sustained acceptance
    out.sort(key=lambda x: (-x["frac_below_now"], x["mean_above60"]))
    from datetime import datetime, timezone, timedelta
    ist = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")
    return {"ok": True, "asof": ist, "sectors": out,
            "caveat": "Bias = pivot acceptance (how long price holds above/below the daily pivot). PUT candidates "
                      "= persistently below the pivot (120d). BUT: India drifts up and shorting/bearish signals "
                      "backtested as ANTI-signals here, and a long put bleeds theta on a slow drift — treat puts "
                      "as a HEDGE or a confirmed-downtrend play, not a standalone edge. Decision-support, not advice."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = build()
    print(f"\n  SECTORAL BIAS (pivot acceptance) — {r['asof']}  ·  most-bearish first")
    print(f"  {'sector':<34}{'verdict':>8}{'%>piv 60d':>11}{'120d':>7}{'below now':>11}{'persist-bear':>13}")
    for x in r["sectors"]:
        print(f"  {x['sector'][:33]:<34}{x['verdict']:>8}{x['mean_above60']:>10}%{x['mean_above120']:>6}%{str(x['n_below_now'])+'/'+str(x['n']):>11}{x['n_persistent_bear']:>13}")
    bear = [x for x in r["sectors"] if x["verdict"] == "BEAR"] or r["sectors"][:2]
    for x in bear[:2]:
        print(f"\n  {x['sector']} — most bearish names (put candidates if confirmed):")
        for nm in x["names"][:6]:
            tag = " ⚑PUT-cand" if nm["persistent_bear"] else ""
            print(f"    {nm['s']:<12} %>piv60 {nm['pct_above60']:>5}  120d {nm['pct_above120']:>5}  now {nm['side']:<5} {nm['streak']}s  ATM {nm['atm']:.0f}{tag}")
    print(f"\n  {r['caveat']}")
