"""
marleg_analyze.py — CROSS-ENGINE "all-sides" analyzer for one stock.

The pods each see a stock through one lens. This reconciles ALL of them at once — cup-handle, reversal,
squeeze, volume/gated, rotation, horizon, fundamentals, live price — into one read, and crucially surfaces
the TENSIONS (e.g. "cup-handle bullish BUT the reversal radar tags it a falling-knife", or "accumulating
but the industry is overheated"). That's the homogeneous platform: every angle, conflicts made explicit,
one net verdict.

  analyze(tk) -> dict      (engines{} + bull[] + caution[] + tensions[] + net{})
  python marleg_analyze.py TEJASNET
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def _find(rows, tk, key="s"):
    for r in rows or []:
        if str(r.get(key, "")).upper() == tk:
            return r
    return None


def analyze(tk):
    tk = tk.upper()
    eng = {}
    bull, caution, tensions = [], [], []

    # ---- live price ----
    price = chg = None
    try:
        import marleg_live
        lp = marleg_live.price(tk)
        if lp.get("ok"):
            price, chg = lp.get("price"), lp.get("chg")
    except Exception:
        pass

    # ---- cup-with-handle ----
    cup = _find(_load("marleg_cuphandle.json").get("setups", []), tk)
    if cup:
        eng["cuphandle"] = {"stage": cup.get("stage"), "pivot": cup.get("pivot"), "target": cup.get("target"),
                            "bt_win": cup.get("bt_win"), "sector_edge": cup.get("sector_edge")}
        st = str(cup.get("stage", ""))
        if st.startswith("CONFIRMED"):
            bull.append(f"cup-with-handle {st} (≈{cup.get('bt_win', 62)}% backtested) → ₹{cup.get('target')}")
        elif st in ("BREAKOUT", "IN_HANDLE", "FORMING"):
            caution.append(f"cup-handle only {st} — not yet a confirmed buy (raw breakout = coin-flip)")
        elif st == "FAILED":
            caution.append("cup-handle FAILED (false breakout) — trap")

    # ---- reversal-to-long ----
    rev = _find(_load("marleg_reversal.json").get("signals", []), tk)
    if rev:
        eng["reversal"] = {"tag": rev.get("tag"), "conv": rev.get("conv"), "signals": rev.get("signals"), "stop": rev.get("stop")}
        if rev.get("tag") == "PRIME re-entry":
            bull.append(f"reversal radar: PRIME re-entry (conv {rev.get('conv')}) — {'/'.join(rev.get('signals', []))}")
        elif rev.get("tag") == "in uptrend":
            bull.append(f"reversal radar: bullish candle in an uptrend ({'/'.join(rev.get('signals', []))})")
        else:
            caution.append("reversal radar: downtrend bounce — falling-knife risk (catching a knife rarely pays)")

    # ---- squeeze (movers) ----
    mv = _find(_load("marleg_movers.json").get("movers", []), tk)
    if mv:
        sq = bool(mv.get("short_covering") or mv.get("bounce_shape") or mv.get("abnormal_up"))
        eng["squeeze"] = {"active": sq, "short_covering": mv.get("short_covering"), "bounce": mv.get("bounce_shape"),
                          "sq_class": mv.get("sq_class"), "amp": mv.get("tag"), "atrp": mv.get("atrp")}
        if sq and mv.get("sq_class") == "focus":
            bull.append("squeeze active in a backtest-blessed industry (snaps pay here)")
        elif sq and mv.get("sq_class") == "trap":
            caution.append("squeeze active but in a TRAP industry (these snaps fade — don't chase)")
        elif sq:
            caution.append("squeeze/abnormal move — burst to scalp with a tight stop, not a trend")

    # ---- volume / gated ----
    gc = _load("marleg_gated_cache.json")
    gp = _find(gc.get("picks", []), tk)
    leadset = {str(x.get("group") or "").lower() for x in gc.get("leading_industries", [])}
    if gp:
        ud = gp.get("ud")
        band = "blowoff" if (ud or 0) >= 6 else "heating" if (ud or 0) >= 4 else "disciplined" if (ud or 0) >= 1.3 else "weak"
        eng["volume"] = {"ud": ud, "ud_band": band, "clean": gp.get("clean"), "gated": True,
                         "entry": gp.get("entry"), "target": gp.get("target"), "industry": gp.get("industry")}
        if gp.get("clean") is not False and band == "disciplined":
            bull.append(f"gated volume leader — disciplined U/D {ud} (healthy accumulation)")
        elif band == "blowoff":
            caution.append(f"U/D {ud} is a BLOW-OFF (≥6) — exhaustion risk, not accumulation")
        elif band == "heating":
            caution.append(f"U/D {ud} heating (4-6) — getting frothy")
        if gp.get("clean") is False:
            caution.append("event/news-contaminated — gate ① not clean")

    # ---- rotation (industry) ----
    industry = (eng.get("volume") or {}).get("industry") or (eng.get("squeeze") and mv.get("industry")) \
        or (_load("marleg_sectors.json").get(tk, {}) or {}).get("industry")
    per = _find(_load("marleg_industry_persistence.json").get("industries", []), industry, key="industry") if industry else None
    if industry:
        leading = str(industry).lower() in leadset
        rec = {"industry": industry, "leading": leading}
        if per:
            rec.update({"beta": per.get("beta"), "persist_weeks": per.get("median_lead_weeks"), "heat_class": per.get("heat_class")})
        eng["rotation"] = rec
        if leading:
            bull.append(f"industry '{industry}' is LEADING this week" + (f" (β{per.get('beta')}, ~{per.get('median_lead_weeks')}w)" if per else ""))
        if per and per.get("heat_class") in ("OVERBLOWN", "HEATING"):
            caution.append(f"industry '{industry}' is {per.get('heat_class')} — rollback risk, don't chase the group")

    # ---- horizon ----
    try:
        import marleg_horizon
        hz = marleg_horizon.rate(tk)
        if hz.get("ok"):
            eng["horizon"] = {"class": hz.get("horizon"), "rating": hz.get("rating"),
                              "ideal_payout": hz.get("ideal_payout_pct"), "target": hz.get("target"),
                              "stop": hz.get("stop"), "hold": hz.get("ideal_hold_label")}
            if hz.get("horizon", "").startswith("AVOID"):
                caution.append(f"horizon rater: {hz.get('horizon')} — {(hz.get('rationale') or [''])[0]}")
            elif hz.get("rating", 0) >= 65:
                bull.append(f"horizon: {hz.get('horizon')} {hz.get('rating')}/100 → +{hz.get('ideal_payout_pct')}% over {hz.get('ideal_hold_label')}")
    except Exception:
        pass

    # ---- fundamentals ----
    fc = _load("marleg_fundamentals_cache.json").get(tk)
    if fc and isinstance(fc, dict):
        eng["fundamentals"] = {"verdict": fc.get("verdict"), "gap": fc.get("gap"), "pe": fc.get("pe"), "q": fc.get("q")}
        if fc.get("verdict") == "OVERVALUED":
            caution.append(f"fundamentals: OVERVALUED ({fc.get('gap')}% below fair) — rich on cash-flow/book")
        elif fc.get("verdict") == "UNDERVALUED":
            bull.append(f"fundamentals: UNDERVALUED (+{fc.get('gap')}% to fair)")

    # ---- TENSIONS (explicit cross-engine conflicts) ----
    has_cup = (eng.get("cuphandle") or {}).get("stage", "").startswith("CONFIRMED")
    rev_risky = (eng.get("reversal") or {}).get("tag") not in (None, "PRIME re-entry", "in uptrend")
    if has_cup and rev_risky:
        tensions.append("Bullish cup-handle base, BUT the reversal radar reads it as a risky downtrend bounce — two pods disagree; wait for confirmation.")
    if (eng.get("volume") or {}).get("ud_band") == "disciplined" and (eng.get("rotation") or {}).get("heat_class") in ("OVERBLOWN", "HEATING"):
        tensions.append("Healthy accumulation, BUT into an overheated industry — the group rollback can drag a good stock.")
    if bull and (eng.get("fundamentals") or {}).get("verdict") == "OVERVALUED":
        tensions.append("Technically strong, BUT fundamentally rich — momentum trade, not a value hold; keep the stop tight.")
    if (eng.get("squeeze") or {}).get("active") and (eng.get("volume") or {}).get("ud_band") == "blowoff":
        tensions.append("Squeeze + blow-off U/D together = late/exhaustion zone — highest false-move risk.")

    # ---- net verdict ----
    score = len(bull) * 10 - len(caution) * 8 - len(tensions) * 6
    if not bull and not caution:
        label = "NO SIGNAL — not on any engine right now"
    elif tensions and len(bull) and len(caution):
        label = "CONFLICTED — engines disagree (read the tensions)"
    elif len(bull) >= 3 and not tensions:
        label = f"ALIGNED LONG — {len(bull)} engines agree"
    elif len(bull) > len(caution):
        label = "LEAN LONG (with caveats)"
    elif len(caution) > len(bull):
        label = "AVOID / STAND DOWN"
    else:
        label = "MIXED"

    return {"ok": True, "tk": tk, "name": (gp or cup or rev or mv or {}).get("n", tk),
            "price": price, "chg": chg, "industry": industry,
            "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
            "engines": eng, "bull": bull, "caution": caution, "tensions": tensions,
            "net": {"label": label, "score": score, "n_bull": len(bull), "n_caution": len(caution), "n_tension": len(tensions)},
            "note": "Every engine's read on one stock + the conflicts made explicit. Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = analyze(sys.argv[1] if len(sys.argv) > 1 else "TEJASNET")
    print(f"\n{r['tk']} ({r.get('industry')})  ₹{r.get('price')}  →  {r['net']['label']}")
    print(f"  engines firing: {', '.join(r['engines'].keys()) or 'none'}")
    if r["bull"]:
        print("  ✅ BULL:"); [print("     +", x) for x in r["bull"]]
    if r["caution"]:
        print("  ⚠ CAUTION:"); [print("     -", x) for x in r["caution"]]
    if r["tensions"]:
        print("  ⚡ TENSIONS:"); [print("     !", x) for x in r["tensions"]]
