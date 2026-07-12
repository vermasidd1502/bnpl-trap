"""
marleg_asialead.py — the PRE-OPEN ASIAN LEAD tracker (check ~8:30–9:00 IST before the 9:15 NSE open).

The markets that trade BEFORE NSE and that NIFTY follows most — validated same-day follow-rates when the
market is down >1% (marleg backtest, 2015-26): Singapore 82%, KOSPI 76%, Taiwan 76%, Nikkei 70%, HangSeng
67% (Shanghai 57% = decoupled → down-weighted). Blends them into a NIFTY-FOLLOWS-DOWN probability for the
open + a macro strip (crude/gold/US-fut/VIX).

HONEST LIMITS: there is NO foreign order-book / options feed (only index levels via yfinance) — so no
put/call walls for these markets. The transmission is FII: global risk-off → FIIs sell EM → India follows.
India's OWN option walls are where it lands. Read-only.
"""
from __future__ import annotations

# name: (yfinance ticker, validated follow-down-rate %, reliability weight, open IST)
LEADERS = {
    "Singapore": ("^STI", 82, 0.95, "6:30"),
    "KOSPI":     ("^KS11", 76, 1.00, "5:30"),
    "Taiwan":    ("^TWII", 76, 1.00, "7:00"),
    "Nikkei":    ("^N225", 70, 0.80, "5:30"),
    "HangSeng":  ("^HSI", 67, 0.70, "7:00"),
    "Shanghai":  ("000001.SS", 57, 0.30, "7:00"),  # decoupled → low weight
}


def _chg(tk):
    import yfinance as yf
    try:
        c = yf.Ticker(tk).history(period="5d")["Close"].dropna()
        if len(c) >= 2:
            v = (c.iloc[-1] / c.iloc[-2] - 1) * 100
            if abs(v) < 20:                     # glitch guard (yfinance Asia can spike)
                return round(float(v), 2)
    except Exception:
        pass
    return None


def macro():
    out = {}
    for k, tk in [("crude", "CL=F"), ("gold", "GC=F"), ("us_fut", "ES=F"), ("nasdaq_fut", "NQ=F"), ("vix", "^VIX")]:
        v = _chg(tk)
        if v is not None:
            out[k] = v
    return out


def read():
    leaders = []
    for name, (tk, fol, w, op) in LEADERS.items():
        leaders.append({"name": name, "chg_pct": _chg(tk), "follow_rate": fol, "weight": w, "opens_ist": op})
    valid = [l for l in leaders if l["chg_pct"] is not None]
    wsum = sum(l["weight"] for l in valid)
    cons = round(sum(l["chg_pct"] * l["weight"] for l in valid) / wsum, 2) if wsum else None

    # NIFTY-follows-down probability, driven by the strongest tells (STI / KOSPI / Taiwan)
    strong = [l for l in valid if l["name"] in ("Singapore", "KOSPI", "Taiwan") and l["chg_pct"] is not None]
    strong_dn = [l for l in strong if l["chg_pct"] <= -1.0]
    if cons is None:
        prob, label = None, "n/a"
    elif strong_dn:                              # a strong tell is down >1% → its validated follow-rate
        prob = round(sum(l["follow_rate"] for l in strong_dn) / len(strong_dn))
        label = "NIFTY LIKELY DOWN"
    elif cons <= -0.5:
        prob, label = 66, "NIFTY leans DOWN"
    elif cons <= -0.15:
        prob, label = 58, "mild down lean"
    elif cons >= 0.4:
        prob, label = None, "risk-ON / NIFTY leans up"
    else:
        prob, label = None, "MIXED / flat"

    return {
        "ok": True, "consensus_pct": cons, "follow_down_prob": prob, "label": label,
        "leaders": sorted(leaders, key=lambda x: x["follow_rate"], reverse=True),
        "macro": macro(),
        "fii_channel": ("FII is the transmission belt: global risk-off → FIIs sell EM (incl. India) → India "
                        "follows. FII flow is LAGGED regime context (EOD data), not a same-day trigger — the "
                        "Asian tape is the fast tell, FII confirms the regime."),
        "limits": "No foreign order-book / options feed exists — these are index levels only. India's own "
                  "put/call walls are where the move lands.",
        "when": "check ~8:30–9:00 IST, before the 9:15 NSE open. Follow-rates are for the market down >1%.",
    }


if __name__ == "__main__":
    import sys, json
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
    print(json.dumps(read(), indent=1, default=str))
