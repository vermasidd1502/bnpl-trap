"""
marleg_buyhold.py — the INVESTING counterweight to the trading pods.

A "compounder score" (0-100) for holding a stock for years, not trading it for days:
  QUALITY    : fundamentals qscore + Piotroski (marleg_fundamentals)
  VALUATION  : PEG / PE (cheaper = better; neutral when unavailable)
  DURABILITY : 3y CAGR, risk-adjusted (Sharpe-like), max drawdown, above-200-DMA  (from price)

compounder_score(tk) is the per-stock analyzer (used by /api/buyhold/<tk>).
screen() reads the bulk price-durability cache (marleg_buyhold_scan.py) — the universe list of
the steadiest long-term compounders. (Quality layer is per-stock because universe-wide statement
fetches are too slow for a live screen.)

NOTE: low-churn by design — this is buy-and-hold, the opposite of the gated/intraday machinery.
"""
import os
import json
import numpy as np
import marleg_fundamentals

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "marleg_buyhold_cache.json")


def _durability(c):
    """Long-horizon price quality from a daily close series."""
    c = c.dropna()
    n = len(c)
    if n < 220:
        return None
    yrs3 = min(n - 1, 756)
    base3 = c.iloc[-1 - yrs3]
    cagr3 = float((c.iloc[-1] / base3) ** (252.0 / yrs3) - 1) if base3 > 0 else None   # native float (jsonify-safe)
    yrs1 = min(n - 1, 252)
    cagr1 = c.iloc[-1] / c.iloc[-1 - yrs1] - 1
    rets = c.pct_change().dropna()
    annvol = float(rets.std() * np.sqrt(252)) if len(rets) else None
    sharpe = (cagr3 / annvol) if (cagr3 is not None and annvol) else None
    dd = float((c / c.cummax() - 1).min())
    ma200 = c.rolling(200).mean().iloc[-1]
    above200 = bool(c.iloc[-1] > ma200)
    hi52 = c.iloc[-252:].max() if n >= 252 else c.max()
    return {"cagr3": cagr3, "cagr1": float(cagr1), "ann_vol": annvol, "sharpe": sharpe,
            "maxdd": dd, "above200": above200, "dist_52wh": float(c.iloc[-1] / hi52 - 1)}


def dur_score(d):
    if not d:
        return None
    s = 50.0
    if d.get("cagr3") is not None:
        s += float(np.clip(d["cagr3"] * 100 * 0.9, -25, 30))     # reward compounding track record
    if d.get("sharpe") is not None:
        s += float(np.clip(d["sharpe"] * 12, -10, 15))           # risk-adjusted
    s += 8 if d.get("above200") else -12                         # multi-year trend intact
    s += float(np.clip((d["maxdd"] + 0.35) * 40, -15, 5))        # penalize deep (> -35%) drawdowns
    return int(np.clip(round(s), 0, 100))


def _val_score(peg, pe):
    if peg and peg > 0:
        return (100 if peg < 1 else 70 if peg < 2 else 42 if peg < 3 else 18), f"PEG {round(peg, 2)}"
    if pe and pe > 0:
        return (80 if pe < 15 else 60 if pe < 25 else 42 if pe < 40 else 22), f"PE {round(pe, 1)}"
    return 50, "valuation n/a"


def _verdict(total):
    if total is None:
        return "—"
    if total >= 70:
        return "🟢 COMPOUNDER — accumulate / hold"
    if total >= 55:
        return "🟢 quality hold"
    if total >= 40:
        return "🟡 mixed — watch / partial"
    return "🔴 not a long-term hold"


def compounder_score(tk):
    import yfinance as yf
    tk = tk.upper()
    f = marleg_fundamentals.fundamentals(tk)
    qscore = f.get("qscore")
    pio = f.get("piotroski") or {}
    quality = None
    if qscore is not None:
        quality = 0.6 * qscore + (0.4 * (pio["score"] / pio["of"] * 100) if pio.get("of") else 0.4 * qscore)
        quality = int(round(quality))
    info = {}
    try:
        info = yf.Ticker(tk + ".NS").info or {}
    except Exception:
        pass
    valuation, val_label = _val_score(info.get("pegRatio") or info.get("trailingPegRatio"), info.get("trailingPE"))
    try:
        c = yf.Ticker(tk + ".NS").history(period="3y", interval="1d", auto_adjust=False)["Close"]
    except Exception:
        c = None
    dur = _durability(c) if c is not None and len(c) else None
    ds = dur_score(dur)

    parts = []
    if quality is not None:
        parts.append((0.40, quality))
    parts.append((0.20, valuation))
    if ds is not None:
        parts.append((0.40, ds))
    wsum = sum(w for w, _ in parts)
    total = int(round(sum(w * v for w, v in parts) / wsum)) if wsum else None

    proj = None
    if dur and dur.get("cagr3") is not None:
        g = dur["cagr3"]
        proj = {"cagr3_pct": round(g * 100, 1),
                "lakh_in_3y": round(1e5 * (1 + g) ** 3),
                "lakh_in_5y": round(1e5 * (1 + g) ** 5)}
    return {"tk": tk, "name": f.get("name", tk), "total": total, "verdict": _verdict(total),
            "blocks": {"quality": quality, "valuation": valuation, "durability": ds},
            "val_label": val_label, "qcoverage": f.get("coverage"),
            "piotroski": (f"{pio['score']}/{pio['of']}" if pio.get("of") else None),
            "durability": (None if not dur else {k: (round(v * 100, 1) if k in ("cagr3", "cagr1", "ann_vol", "maxdd", "dist_52wh") and v is not None else (round(v, 2) if isinstance(v, float) else v)) for k, v in dur.items()}),
            "projection": proj}


def screen(limit=40):
    """Universe list of steadiest long-term compounders (price-durability), from the bulk cache."""
    try:
        with open(CACHE, encoding="utf-8") as fp:
            d = json.load(fp)
        d["rows"] = d.get("rows", [])[:limit]
        return d
    except Exception:
        return {"error": "no buy-hold screen yet — run: python marleg_buyhold_scan.py", "rows": []}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(json.dumps(compounder_score(sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"), indent=2, default=str))
