"""
marleg_live.py — REAL-TIME last price for one name (Groww LTP → yfinance fallback).

The analysis pods (dossier, horizon, gated) are built off the EOD daily panel, so their price/target/stop
are yesterday's close. That's fine for the SETUP but stale for "did it hit target?" — which is why DIXON
could run past target intraday while the dossier still said LONG. This gives a fast, real-time price so the
UI can re-check milestones (TARGET HIT / STOP HIT / entry already passed) on a tight poll, without
re-running the heavy daily analysis every few seconds.

Groww LTP is true real-time (the account is live-data enabled); yfinance 5m is the fallback (~15min lag).

  python marleg_live.py DIXON
"""
import os
import sys
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))


def _now():
    return datetime.now(IST).strftime("%H:%M:%S IST")


def price(tk):
    tk = tk.upper()
    # 1) Groww real-time LTP
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        q = g.quote_table([tk])
        r = q.get(tk) or {}
        if r.get("price"):
            return {"ok": True, "tk": tk, "price": r["price"], "prev": r.get("prev"),
                    "chg": r.get("chg"), "source": "groww-live", "ts": _now()}
    except Exception:
        pass
    # 2) yfinance 5m fallback (~15min delayed)
    try:
        import yfinance as yf
        t = yf.Ticker(tk + ".NS")
        h = t.history(period="1d", interval="5m")["Close"].dropna()
        d5 = t.history(period="5d")["Close"].dropna()
        if len(h):
            p = round(float(h.iloc[-1]), 2)
            prev = round(float(d5.iloc[-2]), 2) if len(d5) >= 2 else None
            return {"ok": True, "tk": tk, "price": p, "prev": prev,
                    "chg": round((p / prev - 1) * 100, 2) if prev else None,
                    "source": "yfinance-5m (~15m delay)", "ts": _now()}
    except Exception:
        pass
    return {"ok": False, "tk": tk, "error": "no live price available"}


def milestone(p, entry=None, target=None, stop=None):
    """Re-evaluate a setup against the LIVE price. Returns status + distances."""
    out = {"price": p}
    if target and p >= target:
        out["status"] = "TARGET_HIT"
    elif stop and p <= stop:
        out["status"] = "STOP_HIT"
    else:
        out["status"] = "IN_PROGRESS"
    if target:
        out["to_target_pct"] = round((target / p - 1) * 100, 2)
    if stop:
        out["to_stop_pct"] = round((p / stop - 1) * 100, 2)
    if entry:
        out["entry_passed"] = p > entry * 1.005
        out["since_entry_pct"] = round((p / entry - 1) * 100, 2)
    return out


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "DIXON"
    r = price(tk)
    if r.get("ok"):
        print(f"{r['tk']}  ₹{r['price']}  ({r.get('chg')}% day)  [{r['source']}]  {r['ts']}")
    else:
        print(r.get("error"))
