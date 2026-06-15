"""
marleg_manual_fundamentals.py — hand-verified fundamentals for names the auto-feed can't reach.

Recent SME/IPO listings (e.g. Anlon Healthcare, listed Sep-2025) have NO yfinance statements, so
the volume-pod cache + /api/fundamentals come back empty. This module holds curated, source-attributed
fundamentals for such names so they show in the pods. Each entry is marked source="manual" + asof so
it's never confused with the statement-computed data.

record(tk)   -> a fundamentals-shaped dict (drop-in for /api/fundamentals fallback)
compacts()   -> {sym: {q,pe,roe,growth,cov,src}} to merge into the volume-pod fundamentals cache

IMPORTANT: numbers are transcribed from public sources (Screener.in etc.) on the noted date — verify
against the latest filing before relying on them; update asof when refreshed.
"""

MANUAL = {
    "AHCL": {
        "name": "Anlon Healthcare",
        "source": "manual · Screener.in (FY26)", "asof": "2026-06",
        # quality 52: strong headline (ROE 19 / ROCE 22 / margins / 36% PAT CAGR) but materially
        # docked for poor earnings quality — negative operating cash flow + stretched working capital.
        "qscore": 52, "q_coverage": 100, "coverage": 90, "piotroski": None,
        "health": {"ROE": 19.2, "ROCE": 22.4, "Net margin": 15.9, "Op margin": 26.0,
                   "Rev growth": 47.0, "D/E (x)": 0.87, "P/E": 29.6, "P/B": 3.92, "Div yield %": 0.0},
        "narrative": [
            {"h": "⚠ Earnings quality is the catch", "p": "FY26 net profit was ₹28 cr but OPERATING CASH FLOW was −₹46 cr; debtor days 215, working-capital cycle ~321 days. The P&L glows, cash conversion does not — accrual profit is stuck in receivables. This is the thing to watch, not the ROE."},
            {"h": "Headline (genuinely strong-looking)", "p": "Revenue 66→120→176 cr (FY24→26), PAT 10→21→28 cr (~36% CAGR); ROE 19% / ROCE 22%; net margin ~16%; D/E 0.87; promoter 52.7%. API / pharma-intermediates maker (incorporated 2013, listed Sep-2025)."},
            {"h": "Profile risk", "p": "Small-cap (~₹824 cr mcap), <1yr public track record, no dividend. High reward / high risk — size accordingly."},
        ],
        "manual_flags": ["negative operating cash flow FY26", "debtor days 215", "WC cycle ~321d", "no dividend", "listed Sep-2025"],
        "annual": [{"y": "FY24", "rev": 66, "pat": 10}, {"y": "FY25", "rev": 120, "pat": 21}, {"y": "FY26", "rev": 176, "pat": 28}],
    },
}


def record(tk):
    """Fundamentals-shaped dict for a manual name, or None."""
    m = MANUAL.get((tk or "").upper())
    if not m:
        return None
    return {"tk": tk.upper(), "ysym": tk.upper() + ".NS", "name": m["name"], "source": m["source"],
            "asof": m["asof"], "qscore": m["qscore"], "q_coverage": m["q_coverage"],
            "coverage": m["coverage"], "piotroski": m["piotroski"], "health": m["health"],
            "narrative": m["narrative"], "manual_flags": m.get("manual_flags", []),
            "annual": m.get("annual", []), "manual": True}


def compacts():
    """{sym: compact} to merge into the volume-pod fundamentals cache."""
    out = {}
    for s, m in MANUAL.items():
        h = m["health"]
        out[s] = {"q": m["qscore"], "pe": h.get("P/E"), "roe": h.get("ROE"),
                  "growth": h.get("Rev growth"), "cov": m["coverage"], "src": "manual"}
    return out


if __name__ == "__main__":
    import json, sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(json.dumps(record(sys.argv[1] if len(sys.argv) > 1 else "AHCL"), indent=2, ensure_ascii=False))
