"""
marleg_shock.py — fast MACRO-SHOCK / regime-break detector + trade-gate overlay.

Why: the regime gate (NIFTY>50DMA) is SLOW — a war/news shock would blindside it (the index can crash
for days before the 50DMA flips). This watches the signals that move in HOURS and confirms a regime
BREAK fast, then OVERRIDES the gate:

  • India VIX           level + 1-day jump
  • NIFTY               1-day drop / gap
  • breadth             % of names above their 50DMA collapsing
  • correlation         cross-sectional avg pairwise corr spiking (everything moves as one = alpha dormant)

State: NORMAL -> defer to the bull/bear gate; ELEVATED -> cut size/tighten; SHOCK -> refrain from
intraday/swing, long-term hold only. It does NOT predict shocks — it CONFIRMS them early so you de-risk
before the slow gate would. Thresholds calibrated to 3y history (10 worst NIFTY days: VIX ~22.5, VIX
1d jump ~+21%, NIFTY ~-2.8%).
"""
import os
import json

HERE = os.path.dirname(os.path.abspath(__file__))


def _gated_regime():
    try:
        return (json.load(open(os.path.join(HERE, "marleg_gated_cache.json"), encoding="utf-8")).get("regime") or {})
    except Exception:
        return {}


def _dial_corr():
    try:
        d = json.load(open(os.path.join(HERE, "marleg_regime_cache.json"), encoding="utf-8"))
        return d.get("corr_pctile") if isinstance(d, dict) else None
    except Exception:
        return None


def _idx(sym):
    """Last valid close series for an index via yfinance (drops the phantom NaN pre-market row)."""
    try:
        import yfinance as yf
        s = yf.Ticker(sym).history(period="1y")["Close"].dropna()
        return s if len(s) >= 5 else None
    except Exception:
        return None


def read():
    reg = _gated_regime()
    bull, breadth = reg.get("bull"), reg.get("breadth")
    vix = _idx("^INDIAVIX")
    nif = _idx("^NSEI")
    vix_now = vix_chg = vix_pct = None
    if vix is not None:
        vix_now = round(float(vix.iloc[-1]), 1)
        vix_chg = round(float(vix.iloc[-1] / vix.iloc[-2] - 1) * 100, 1)
        vix_pct = round(float((vix <= vix.iloc[-1]).mean()) * 100)
    nif_chg = round(float(nif.iloc[-1] / nif.iloc[-2] - 1) * 100, 2) if nif is not None else None
    corr_pct = _dial_corr()

    reasons, sev = [], 0
    if vix_now is not None:
        if vix_now >= 25 or (vix_chg or 0) >= 20:
            sev += 2; reasons.append(f"India VIX {vix_now} ({vix_chg:+}%) — spiking")
        elif vix_now >= 18 or (vix_chg or 0) >= 12:
            sev += 1; reasons.append(f"India VIX {vix_now} ({vix_chg:+}%) — elevated")
    if nif_chg is not None:
        if nif_chg <= -2.5:
            sev += 2; reasons.append(f"NIFTY {nif_chg:+}% — hard down day")
        elif nif_chg <= -1.2:
            sev += 1; reasons.append(f"NIFTY {nif_chg:+}% — weak")
    if breadth is not None and breadth <= 20:
        sev += 1; reasons.append(f"breadth {breadth}% above 50DMA — collapsing")
    if corr_pct is not None and corr_pct >= 85:
        sev += 1; reasons.append(f"correlation {corr_pct}th pctile — moving as one (stock-picking alpha dormant)")

    state = "SHOCK" if sev >= 2 else "ELEVATED" if sev >= 1 else "NORMAL"
    if state == "SHOCK":
        badge = "SHOCK"
        verdict = "MACRO SHOCK confirmed — refrain from intraday/swing. Long-term buy & hold only; let it settle before re-deploying."
    elif state == "ELEVATED":
        badge = "STRESS"
        verdict = "Macro stress building — cut size, tighten stops, no fresh aggressive longs."
    else:
        badge = "BULL" if bull else "BEAR" if bull is False else "—"
        verdict = reg.get("verdict") or ("bull regime — deploy" if bull else "bear regime — caution")

    return {"state": state, "badge": badge, "trade_ok": state == "NORMAL",
            "bull": bull, "breadth": breadth, "vix": vix_now, "vix_chg": vix_chg, "vix_pctile": vix_pct,
            "nifty_chg": nif_chg, "corr_pctile": corr_pct, "reasons": reasons, "verdict": verdict}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(json.dumps(read(), indent=2, ensure_ascii=False))
