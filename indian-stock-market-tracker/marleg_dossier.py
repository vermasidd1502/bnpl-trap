"""
marleg_dossier.py — the per-stock DOSSIER behind a click on the Movers pod.

Turns a ticker into the numbers you need to decide, and makes SQUEEZE vs MOVE-POTENTIAL distinct:
  • move_potential : statistical AMPLITUDE — ATR% (typical daily range) + P(|1d|>=5%). "How much can it move?"
  • squeeze_ceiling: DIRECTIONAL room — nearest overhead resistance + % room + a probabilistic upper bound.
                     "How high can this run before it hits a wall?"  (a squeeze exhausts at supply.)
  • montecarlo     : bootstrap the stock's OWN recent daily returns into N forward paths -> a p5..p95 cone
                     + P(+5%), P(+8%), P(-5%). A DISTRIBUTION of outcomes from current vol — NOT a forecast.
  • conviction     : 0-100 from the VALIDATED signals (regime, trend, news-clean, leading industry, amplitude,
                     room-not-chasing). Low conviction + you fire = fooling yourself. Drives a suggested trade.
  • levels         : support / resistance / 50DMA / entry / stop / target — for the chart.
  • overlap        : reads your live Groww book (READ-ONLY) and flags if you're already exposed to this sector.

Read-only throughout; never places/modifies an order.
"""
import json
import os

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
_RNG = np.random.default_rng(0)


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def _rsi(c, n=14):
    d = c.diff(); up = d.clip(lower=0).rolling(n).mean(); dn = (-d.clip(upper=0)).rolling(n).mean()
    v = (100 - 100 / (1 + up / dn.replace(0, np.nan))).iloc[-1]
    return float(v) if v == v else 50.0


def _atr_pct(h, l, c):
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    a = (tr.rolling(14).mean() / c).iloc[-1]
    return float(a) * 100 if a == a else None


def _montecarlo(close, price, H=10, N=2000):
    rets = close.pct_change().dropna().tail(180).values
    if len(rets) < 40 or not price:
        return None
    samp = _RNG.choice(rets, size=(N, H), replace=True)
    paths = price * np.cumprod(1 + samp, axis=1)
    qs = [5, 25, 50, 75, 95]
    cone = [{"day": h + 1, **{f"p{q}": round(float(np.percentile(paths[:, h], q)), 1) for q in qs}} for h in range(H)]
    term = paths[:, -1]
    return {"H": H, "cone": cone,
            "p_up5": round(float((term >= price * 1.05).mean()) * 100, 1),
            "p_up8": round(float((term >= price * 1.08).mean()) * 100, 1),
            "p_dn5": round(float((term <= price * 0.95).mean()) * 100, 1),
            "exp": round(float(np.median(term)), 1),
            "p95": round(float(np.percentile(term, 95)), 1),
            "p05": round(float(np.percentile(term, 5)), 1)}


def dossier(tk):
    import marleg_data as md
    import marleg_eventfilter as ef
    tk = (tk or "").upper().strip()
    df = md.candles(tk, interval_min=1440, days=300)
    if df is None or len(df) < 120:
        return {"ok": False, "error": f"no daily data for {tk}"}
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    price = float(c.iloc[-1])
    dma50 = float(c.rolling(50).mean().iloc[-1]); dma200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else None
    atrp = _atr_pct(h, l, c)
    atr = price * (atrp / 100) if atrp else price * 0.02
    rsi = round(_rsi(c))
    ret5 = round(float(c.iloc[-1] / c.iloc[-6] - 1) * 100, 1)
    ret20 = round(float(c.iloc[-1] / c.iloc[-21] - 1) * 100, 1)
    rets = c.pct_change().dropna().tail(180)
    p5 = round(float((rets.abs() >= 0.05).mean()) * 100, 1)
    hh120 = float(h.rolling(120).max().iloc[-1]); ll120 = float(l.rolling(120).min().iloc[-1])
    fib = round((price - ll120) / (hh120 - ll120), 2) if hh120 > ll120 else None

    # support / resistance
    sup = float(l.rolling(20).min().iloc[-1])
    res_window = float(h.iloc[-60:].max())
    resistance = res_window if res_window > price * 1.005 else round(price + 2 * atr, 1)   # if at highs, project a measured move
    room = round((resistance - price) / price * 100, 1)
    support = round(max(sup, dma50 * 0.99), 1)

    mc = _montecarlo(c, price)

    # context for conviction
    fp = ef.footprint(c, v)
    clean = fp["clean"]
    sect = _load("marleg_sectors.json").get(tk, {})
    sector, industry = sect.get("sector"), sect.get("industry")
    gated = _load("marleg_gated_cache.json")
    bull = bool((gated.get("regime") or {}).get("bull"))
    lead_names = {str(g.get("group") or g.get("industry") or g.get("name") or "").lower()
                  for g in gated.get("leading_industries", [])}
    industry_leading = bool(industry and str(industry).lower() in lead_names)

    # ichimoku bull (above cloud + Kijun)
    ten = (h.rolling(9).max() + l.rolling(9).min()) / 2
    kij = (h.rolling(26).max() + l.rolling(26).min()) / 2
    spanA = ((ten + kij) / 2).shift(26); spanB = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
    cloud_top = pd.concat([spanA, spanB], axis=1).max(axis=1).iloc[-1]
    ichi_bull = bool(price > cloud_top and price > kij.iloc[-1]) if cloud_top == cloud_top else None

    # CONVICTION (0-100) from validated signals — a discipline gauge, not a guarantee
    comp = []
    def add(label, ok, pts, why):
        comp.append({"k": label, "ok": bool(ok), "pts": pts if ok else 0, "max": pts, "why": why})
    add("regime open", bull, 15, "market above 50DMA (long edge lives here)")
    add("above 50DMA", price > dma50, 14, "primary uptrend intact")
    add("above Ichimoku cloud", ichi_bull, 8, "trend structure bullish")
    add("leading industry", industry_leading, 14, "a leader in a leading group")
    add("news-clean", clean, 15, "organic move, not an earnings/news spike")
    add("amplitude (can do 3-8%)", (atrp or 0) >= 3, 12, f"ATR% {round(atrp,1) if atrp else '—'}")
    add("room to run", room >= 3, 12, f"{room}% to resistance — not chasing")
    add("not over-heated", rsi < 72, 10, f"RSI {rsi}")
    score = sum(x["pts"] for x in comp)
    cap_reason = None
    if not clean:                          # an earnings/news SPIKE is not organic conviction — cap it hard
        score = min(score, 45); cap_reason = "event-contaminated — capped (the 'don't get fooled' guard)"
    if any("pre-earnings" in str(f) for f in fp["flags"]):
        score = min(score, 32); cap_reason = "pre-earnings run-up — capped (binary event risk)"
    if rsi >= 78:                          # parabolic — chasing
        score = min(score, 48); cap_reason = cap_reason or f"RSI {rsi} — over-heated, capped"
    conv = "HIGH" if score >= 70 else "MEDIUM" if score >= 50 else "LOW"

    # suggested trade from conviction + setup
    stop = round(price - atr, 1); target = round(min(resistance, price + 2 * atr), 1)
    tgtpct = round((target - price) / price * 100, 1)
    if score >= 70:
        action = "LONG"
        sugg = (f"Long {tk} — conviction {score}/100. Enter near ₹{price} (or a small dip), "
                f"stop ₹{stop} (~1 ATR), target ₹{target} (+{tgtpct}%). Swing 3-5d; trail once +1 ATR.")
    elif score >= 50:
        action = "WATCH"
        sugg = (f"Half-size or WATCH — conviction {score}/100. Wait for a pullback to ₹{support} or a clean "
                f"break above ₹{round(resistance,1)} before committing. Don't chase.")
    else:
        action = "STAND DOWN"
        miss = cap_reason or ("failing: " + ", ".join(x["k"] for x in comp if not x["ok"]))
        sugg = (f"STAND DOWN — conviction only {score}/100 ({miss}). Firing here is fooling yourself; "
                f"wait for a cleaner setup.")

    # direction verdict
    higher = bool(c.iloc[-1] > c.iloc[-6] and c.iloc[-6] > c.iloc[-11])
    direction = ("✓ heading up — higher highs, above key averages" if (price > dma50 and higher)
                 else "→ sideways / mixed — not a clean uptrend" if price > dma50
                 else "✗ below 50DMA — downtrend, long edge negative here")

    # position overlap (read-only book)
    overlap = _overlap(sector, tk)

    # chart history (last 70 closes, downsampled to keep payload small)
    hist = [{"d": str(idx.date()) if hasattr(idx, "date") else str(idx), "c": round(float(x), 1)}
            for idx, x in c.tail(70).items()]

    return {"ok": True, "s": tk, "name": sect.get("n") or tk, "sector": sector, "industry": industry,
            "price": round(price, 2), "ret5": ret5, "ret20": ret20, "rsi": rsi, "fib": fib,
            "dma50": round(dma50, 1), "dma200": round(dma200, 1) if dma200 else None,
            "move_potential": {"atrp": round(atrp, 2) if atrp else None, "p5": p5,
                               "tag": "HIGH" if (atrp or 0) >= 3 else "MED" if (atrp or 0) >= 1.8 else "LOW",
                               "exp_lo": round(price * (1 - (atrp or 2) / 100), 1), "exp_hi": round(price * (1 + (atrp or 2) / 100), 1),
                               "note": "amplitude — how much it can move in a day (symmetric; pair with the loss-limit)"},
            "squeeze_ceiling": {"resistance": round(resistance, 1), "room_pct": room,
                                "mc_p95_10d": mc["p95"] if mc else None,
                                "note": "directional room — nearest overhead supply where a squeeze tends to exhaust"},
            "montecarlo": mc, "levels": {"price": round(price, 2), "support": support, "resistance": round(resistance, 1),
                                         "dma50": round(dma50, 1), "stop": stop, "target": target},
            "conviction": {"score": score, "label": conv, "capped": cap_reason, "components": comp},
            "direction": direction, "clean": clean, "event_flags": fp["flags"],
            "suggested": {"action": action, "text": sugg, "stop": stop, "target": target, "tgtpct": tgtpct},
            "overlap": overlap, "hist": hist}


def _overlap(sector, tk):
    """Best-effort: read the live Groww book (holdings + positions) and flag same-sector concentration."""
    if not sector:
        return None
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        held = {}
        for getter in ("holdings_data", "positions_data"):
            try:
                data = getattr(g, getter)()
            except Exception:
                continue
            rows = data if isinstance(data, list) else (data.get("positions") or data.get("holdings") or data.get("data") or []) if isinstance(data, dict) else []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                sym = (r.get("trading_symbol") or r.get("symbol") or r.get("tradingsymbol") or r.get("nse_symbol") or "").upper()
                if sym:
                    held[sym] = True
        if not held:
            return None
        SECT = _load("marleg_sectors.json")
        same = [s for s in held if s != tk and (SECT.get(s, {}).get("sector") == sector)]
        if same:
            return {"warn": True, "sector": sector, "held": same[:6],
                    "msg": f"⭐ You already hold {', '.join(same[:4])} in {sector} — adding {tk} concentrates the bet. Diversify or size down."}
        return {"warn": False, "sector": sector, "msg": f"No existing {sector} position — adds diversification."}
    except Exception:
        return None
