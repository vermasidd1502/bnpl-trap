"""
marleg_horizon.py — RATE a stock's ideal holding HORIZON (+ ideal period & payout for short-term).

Synthesises every validated edge into one verdict per name:
  • LONG-TERM (buy & hold)  — steady compounder above the 200DMA, positive 1y trend, contained drawdown.
                              Hold months+; trail, no fixed target.
  • SWING (2-4 weeks)       — gated/leading-industry leader in an uptrend, or a confirmed cup-handle.
                              (our edges: cup-handle confirmed grows to +2.5%/20d; industry leadership
                              persists a median ~3 weeks → 2-4wk is the natural hold.)
  • SHORT-TERM (days)       — high-amplitude / pullback-bounce / momentum name. Hold ~1 week.
  • AVOID                   — below both MAs & falling, OR RSI-fried/extended (don't-chase), OR event-dirty.

Ideal payout = a vol-scaled expected favorable move over the ideal hold (ATR% × √days × capture), floored
by the relevant validated base-rate. Pulls VOLUME-pod logic (U/D gate, gated status) + ROTATION-pod logic
(industry leading?, beta, leadership-persistence weeks). Read-only, decision-support — not advice.

  python marleg_horizon.py TEJASNET
"""
import json
import math
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def _rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean().replace(0, np.nan)
    return float((100 - 100 / (1 + up / dn)).iloc[-1])


def _index_rate(tk):
    """Indices aren't in the equity panel and aren't a buy-&-hold equity — for an index OPTION the real
    decision is WHICH EXPIRY (theta/time structure). Give a clean Groww-based trend read that routes to
    the Expiry Matrix, instead of the panel error."""
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        df = g.candles(tk, interval_min=1440, days=400, segment="CASH")
        c = df["close"].dropna()
        price = float(c.iloc[-1])
        s50 = float(c.rolling(50).mean().iloc[-1])
        s200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else None
        rsi = _rsi(c)
        ret20 = float(c.iloc[-1] / c.iloc[-21] - 1) * 100 if len(c) > 21 else None
        a50 = price > s50
        a200 = (s200 is not None and price > s200)
        trend = f"{'>50DMA' if a50 else '<50DMA'}" + (f" · {'>200DMA' if a200 else '<200DMA'}" if s200 else "")
        rating = 62 if (a50 and a200) else 48 if a50 else 34
        return {"ok": True, "tk": tk, "name": tk, "is_index": True, "industry": "Index",
                "price": round(price, 2), "horizon": "INDEX-OPTION", "rating": rating,
                "signals": {"trend": trend, "rsi": round(rsi, 0), "ret20": round(ret20, 1) if ret20 else None},
                "route": "marle_g_options.html?u=" + tk,
                "rationale": [
                    "Index — not a buy-&-hold equity. For an index OPTION the P&L is dominated by THETA, so the "
                    "real call is which EXPIRY to hold, not just direction.",
                    f"Trend {trend}, RSI {rsi:.0f}" + (f", 20d {ret20:+.1f}%" if ret20 else ""),
                    "➤ Use the Expiry Matrix to compare every live expiry for a 1–2 week hold (theta-aware)."],
                "note": "Index options: time structure (theta) drives it — compare expiries in the matrix."}
    except Exception as e:
        return {"ok": False, "error": f"{tk}: index read failed ({str(e)[:70]})", "tk": tk}


def rate(tk):
    import marleg_panel_build as pb
    tk = tk.upper()
    try:
        import marleg_options_monitor as _mom
        if tk in _mom.INDEX_STEP:
            return _index_rate(tk)
    except Exception:
        pass
    P = pb.load()
    if not P or tk not in P["close"].columns:
        return {"ok": False, "error": f"{tk} not in the panel", "tk": tk}
    c = P["close"][tk].dropna()
    if len(c) < 220:
        return {"ok": False, "error": f"{tk}: too little history to rate", "tk": tk}
    h = P["high"][tk].reindex(c.index); l = P["low"][tk].reindex(c.index)
    v = P["volume"][tk].reindex(c.index)
    price = float(c.iloc[-1])
    s50 = float(c.rolling(50).mean().iloc[-1]); s200 = float(c.rolling(200).mean().iloc[-1])
    ret = lambda n: float(c.iloc[-1] / c.iloc[-1 - n] - 1) * 100 if len(c) > n else None
    ret20, ret60, ret250 = ret(20), ret(60), ret(250)
    rsi = _rsi(c)
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atrp = float((tr.rolling(14).mean() / c).iloc[-1]) * 100
    lo120, hi120 = float(c.iloc[-120:].min()), float(c.iloc[-120:].max())
    fib = (price - lo120) / (hi120 - lo120) if hi120 > lo120 else 0.5
    rc = c.pct_change()
    upv = v.where(rc > 0, 0.0).rolling(20).sum(); dnv = v.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = float((upv / dnv).iloc[-1])
    dd1y = float((c.iloc[-250:] / c.iloc[-250:].cummax() - 1).min()) * 100
    above50, above200 = price > s50, price > s200
    extended = rsi > 72 or price > s50 * 1.18

    # ---- volume + rotation pod context ----
    SECT = _load("marleg_sectors.json"); NAMES = {}
    syms = _load("marleg_symbols.json")
    if isinstance(syms, list):
        NAMES = {r["s"]: r["n"] for r in syms}
    sec = SECT.get(tk, {}); industry = sec.get("industry") or sec.get("sector")
    g = _load("marleg_gated_cache.json")
    gated = any(p.get("s") == tk for p in g.get("picks", []))
    leadnames = {str(x.get("group") or "").lower() for x in g.get("leading_industries", [])}
    ip = _load("marleg_industry_persistence.json")
    irow = next((x for x in ip.get("industries", []) if str(x.get("industry", "")).lower() == str(industry or "").lower()), {})
    ind_leading = bool(industry and str(industry).lower() in leadnames)
    beta = irow.get("beta"); persist_w = irow.get("median_lead_weeks")
    cup = _load("marleg_cuphandle.json")
    cuprow = next((x for x in cup.get("setups", []) if x.get("s") == tk), None)
    cup_confirmed = bool(cuprow and str(cuprow.get("stage", "")).startswith("CONFIRMED"))

    # ---- classify horizon ----
    reasons, signals = [], {}
    signals["trend"] = f"{'>50DMA' if above50 else '<50DMA'} · {'>200DMA' if above200 else '<200DMA'}"
    signals["volume_ud"] = round(ud, 2)
    signals["fib120"] = round(fib, 2)
    signals["rsi"] = round(rsi, 0)
    signals["atr_pct"] = round(atrp, 2)
    signals["industry"] = industry
    signals["industry_leading"] = ind_leading
    signals["beta"] = beta
    signals["gated"] = gated

    broken = (not above50 and not above200 and (ret60 or 0) < 0)
    steady_compounder = above200 and (ret250 or 0) > 0 and dd1y > -35 and atrp < 4
    if broken:
        cls, hold_lo, hold_hi, rating = "AVOID", None, None, 20
        reasons.append("below both 50 & 200 DMA and falling — no trend to buy")
    elif extended:
        cls, hold_lo, hold_hi, rating = "AVOID (extended)", None, None, 35
        reasons.append(f"extended (RSI {rsi:.0f}{', far over 50DMA' if price>s50*1.18 else ''}) — don't chase; wait for a pullback")
    elif cup_confirmed or (gated and ind_leading and above50):
        cls, hold_lo, hold_hi, rating = "SWING", 10, 20, 78
        if cup_confirmed: reasons.append("confirmed cup-with-handle — the validated 2-4wk base (edge grows to +2.5%/20d)")
        if gated: reasons.append("on the gated volume list (U/D + above MAs)")
        if ind_leading: reasons.append(f"industry leading{f' (persists ~{persist_w}w)' if persist_w else ''}")
    elif above50 and fib >= 0.6 and (ret20 or 0) > 0:
        cls, hold_lo, hold_hi, rating = "SWING", 10, 20, 66
        reasons.append("uptrend with strength (fib≥0.6, above 50DMA) — swing the leg")
        if ind_leading: reasons.append("in a leading industry")
    elif steady_compounder and atrp < 3 and fib >= 0.5:
        cls, hold_lo, hold_hi, rating = "LONG-TERM", 60, 120, 72
        reasons.append(f"steady compounder: above 200DMA, +{ret250:.0f}%/1y, max DD {dd1y:.0f}% — hold for the trend")
    elif above50 and atrp >= 3:
        cls, hold_lo, hold_hi, rating = "SHORT-TERM", 4, 8, 58
        reasons.append(f"high-amplitude mover (ATR% {atrp:.1f}) above 50DMA — a days-not-weeks trade")
    elif above50:
        cls, hold_lo, hold_hi, rating = "SHORT-TERM", 5, 10, 52
        reasons.append("mild uptrend, no standout setup — short leash only")
    else:
        cls, hold_lo, hold_hi, rating = "AVOID", None, None, 30
        reasons.append("no clean long setup right now")

    # nudges
    if ud < 0.8 and not cls.startswith("AVOID"):
        rating -= 8; reasons.append(f"U/D {ud:.2f} soft (distribution) — knock conviction down")
    if cls == "LONG-TERM" and not steady_compounder:
        pass
    rating = int(max(0, min(100, rating)))

    out = {"ok": True, "tk": tk, "name": NAMES.get(tk, tk), "price": round(price, 2),
           "industry": industry, "rating": rating, "horizon": cls,
           "signals": signals, "rationale": reasons}
    if hold_lo:
        mid = (hold_lo + hold_hi) / 2
        # ideal payout = vol-scaled expected favourable move over the hold, floored by validated base-rates
        payout = atrp * math.sqrt(mid) * 0.6
        if cls == "SWING":
            payout = max(payout, 6.0)               # cup-handle/leadership base ≈ +6-12% over 2-4wk
        elif cls == "SHORT-TERM":
            payout = max(payout, 3.0)               # amplitude base ≈ 3-6% over a few days
        elif cls == "LONG-TERM":
            payout = max(payout, 15.0)              # trend hold, trail (indicative only)
        payout = round(payout, 1)
        stop_atr = float((tr.rolling(14).mean()).iloc[-1])
        stop = round(price - 1.5 * stop_atr, 1)
        out.update({"ideal_hold_days": [hold_lo, hold_hi], "ideal_hold_label": _hold_label(cls, hold_lo, hold_hi),
                    "ideal_payout_pct": payout, "target": round(price * (1 + payout / 100), 1),
                    "stop": stop, "stop_pct": round((price - stop) / price * 100, 1),
                    "rr": round(payout / max(0.1, (price - stop) / price * 100), 1)})
    return out


def _hold_label(cls, lo, hi):
    if cls == "LONG-TERM":
        return "months (trail; trim into strength, no fixed exit)"
    if cls == "SWING":
        return f"{lo}-{hi} sessions (≈2-4 weeks)"
    return f"{lo}-{hi} sessions (days)"


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "TEJASNET"
    r = rate(tk)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n{r['tk']} ({r['industry']})  ₹{r['price']}   →  {r['horizon']}   rating {r['rating']}/100")
    if r.get("ideal_hold_days"):
        print(f"  ideal hold : {r['ideal_hold_label']}")
        print(f"  ideal payout: +{r['ideal_payout_pct']}%  → target ₹{r['target']}  · stop ₹{r['stop']} (-{r['stop_pct']}%)  · R:R {r['rr']}")
    print("  why:")
    for x in r["rationale"]:
        print(f"    • {x}")
    s = r["signals"]
    bstr = f" (β{s['beta']})" if s.get("beta") else ""
    lead = "LEADING" if s["industry_leading"] else "not leading"
    print(f"  signals: {s['trend']} · fib {s['fib120']} · RSI {s['rsi']:.0f} · ATR% {s['atr_pct']} · U/D {s['volume_ud']} · "
          f"industry {lead}{bstr} · {'GATED' if s['gated'] else 'not gated'}")
