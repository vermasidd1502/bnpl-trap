"""
marleg_lookahead.py — pre-open LOOK-AHEAD for the NSE day.

GIFT Nifty is the real gap tell (it trades while NSE is shut), but it has NO free live feed from here
(NSE-IX/GIFT City; yfinance/Groww don't carry it). So this gives:
  1. the global cues that DRIVE the gap — US close (S&P/Nasdaq/Dow), US VIX, Asia (Nikkei/Hang Seng),
     Brent (inverse for India), USD/INR, gold — fetched live,
  2. a composite "implied open" heuristic (US-weighted, the dominant overnight driver),
  3. the prior Nifty close, so the pod can turn the GIFT level YOU type into an implied gap %.
Honest: the composite is a heuristic; GIFT (manual) + the US close are the real tells.
"""
import os


def _cues():
    import yfinance as yf
    out = {}
    tks = {"nifty_prev": "^NSEI", "sp500": "^GSPC", "nasdaq": "^IXIC", "dow": "^DJI", "us_vix": "^VIX",
           "nikkei": "^N225", "hangseng": "^HSI", "brent": "BZ=F", "usdinr": "INR=X", "gold": "GC=F"}
    for k, t in tks.items():
        try:
            h = yf.Ticker(t).history(period="5d")["Close"].dropna()
            if len(h) >= 2:
                out[k] = {"last": round(float(h.iloc[-1]), 2), "chg": round(float(h.iloc[-1] / h.iloc[-2] - 1) * 100, 2)}
        except Exception:
            pass
    return out


def build(gift=None):
    c = _cues()

    def ch(k):
        v = c.get(k)
        return v["chg"] if v else None

    us = [x for x in (ch("sp500"), ch("nasdaq"), ch("dow")) if x is not None]
    asia = [x for x in (ch("nikkei"), ch("hangseng")) if x is not None]
    us_avg = sum(us) / len(us) if us else 0.0
    asia_avg = sum(asia) / len(asia) if asia else 0.0
    crude = ch("brent") or 0.0
    vix = ch("us_vix") or 0.0
    score = us_avg * 0.6 + asia_avg * 0.3 - crude * 0.04 - vix * 0.04
    verdict = "GAP UP likely" if score > 0.3 else "GAP DOWN likely" if score < -0.3 else "FLAT-ish open"

    drivers = []
    if us:
        drivers.append(f"US {us_avg:+.2f}% (S&P {ch('sp500'):+.2f}, Nasdaq {ch('nasdaq'):+.2f}, Dow {ch('dow'):+.2f})")
    if asia:
        drivers.append(f"Asia {asia_avg:+.2f}%")
    if ch("brent") is not None:
        drivers.append(f"Brent {ch('brent'):+.1f}% ({'tailwind' if crude < 0 else 'headwind'} for India)")
    if ch("usdinr") is not None:
        drivers.append(f"USDINR {c['usdinr']['last']} ({ch('usdinr'):+.2f}%)")
    if ch("us_vix") is not None:
        drivers.append(f"US VIX {c['us_vix']['last']} ({ch('us_vix'):+.1f}%)")

    nprev = (c.get("nifty_prev") or {}).get("last")
    out = {"cues": c, "composite": round(score, 2), "verdict": verdict, "drivers": drivers,
           "nifty_prev": nprev,
           "note": "GIFT Nifty has no free live feed here — type the level you see for the exact implied gap. "
                   "The composite is a heuristic (US-weighted); GIFT + the US close are the real tells."}
    if gift and nprev:
        out["implied_gap_pct"] = round((float(gift) - nprev) / nprev * 100, 2)
    return out


if __name__ == "__main__":
    import json, sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    g = sys.argv[1] if len(sys.argv) > 1 else None
    r = build(g)
    print("verdict:", r["verdict"], "| composite", r["composite"], "| nifty prev", r["nifty_prev"])
    for d in r["drivers"]:
        print("  ", d)
    if r.get("implied_gap_pct") is not None:
        print(f"  GIFT {g} -> implied gap {r['implied_gap_pct']:+}%")
