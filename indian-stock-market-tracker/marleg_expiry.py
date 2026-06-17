"""
marleg_expiry.py — is NIFTY being driven by OPTIONS EXPIRY? (calendar + backtested effect)

Tells you WHERE you are in the expiry cycle and what NIFTY historically does there, so you don't
confuse expiry chop with a real trend. Backtest (4y ^NSEI) is clear:
  • Expiry day (Thursday) is the WEAKEST, CHOPPIEST weekday — flat/slightly negative, widest range
    (pinning to max-pain + hedge unwinding), NOT a trend day.
  • Monthly expiry (last Thursday) is mildly NEGATIVE (~-0.09%, 45% win).
  • The day AFTER monthly expiry tends to BOUNCE (+0.2%, 55%) once the series rolls.
So: don't hold fresh directional longs INTO expiry expecting a move; expiry is mean-reverting/pinning.

NOTE: NSE changed the weekly-expiry weekday during 2024-25 (SEBI one-weekly-per-exchange). Default here is
Thursday (true for most of the backtest window); verify the current day with your broker.

  python marleg_expiry.py
"""
import calendar as _cal
import os
import sys
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
WEEKLY_DOW = 3        # Thursday (NSE NIFTY weekly; configurable — changed in 2024-25)
DOW_NAME = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def _last_thu(y, m):
    d = date(y, m, _cal.monthrange(y, m)[1])
    while d.weekday() != WEEKLY_DOW:
        d -= timedelta(days=1)
    return d


def _next_weekly(today):
    delta = (WEEKLY_DOW - today.weekday()) % 7
    return today + timedelta(days=delta)         # == today if today is the expiry weekday


def _next_monthly(today):
    lt = _last_thu(today.year, today.month)
    if lt < today:
        ny, nm = (today.year + (today.month == 12)), (today.month % 12 + 1)
        lt = _last_thu(ny, nm)
    return lt


def calendar_pos(today=None):
    today = today or datetime.now(IST).date()
    wk = _next_weekly(today); mo = _next_monthly(today)
    d2w = (wk - today).days; d2m = (mo - today).days
    is_wk_today = d2w == 0; is_mo_today = d2m == 0
    # last monthly expiry (to detect "day after")
    prev_lt = _last_thu(today.year, today.month)
    if prev_lt >= today:
        py, pm = (today.year - (today.month == 1)), (12 if today.month == 1 else today.month - 1)
        prev_lt = _last_thu(py, pm)
    day_after_monthly = (today - prev_lt).days == 1
    if is_mo_today:
        pos = "MONTHLY EXPIRY today — choppiest, mildly negative historically"
    elif is_wk_today:
        pos = "WEEKLY EXPIRY today — pinning/chop, flat-to-negative historically"
    elif d2w == 1:
        pos = "day BEFORE weekly expiry — positioning day"
    elif day_after_monthly:
        pos = "day AFTER monthly expiry — relief-bounce window historically"
    else:
        pos = f"mid-cycle ({d2w}d to weekly, {d2m}d to monthly)"
    return {"today": str(today), "today_dow": DOW_NAME[today.weekday()],
            "weekly_expiry": str(wk), "days_to_weekly": d2w, "is_weekly_today": is_wk_today,
            "monthly_expiry": str(mo), "days_to_monthly": d2m, "is_monthly_today": is_mo_today,
            "day_after_monthly": day_after_monthly, "cycle_position": pos}


def profile():
    """Backtested NIFTY behaviour by weekday + monthly-expiry (4y ^NSEI)."""
    import yfinance as yf
    h = yf.Ticker("^NSEI").history(period="4y", interval="1d").dropna()
    if h.empty:
        return None
    h.index = h.index.tz_localize(None)
    r = h["Close"].pct_change() * 100
    rng = (h["High"] - h["Low"]) / h["Close"] * 100
    dow = h.index.dayofweek
    by = []
    for d in range(5):
        m = dow == d
        if m.sum() < 20:
            continue
        by.append({"dow": DOW_NAME[d], "n": int(m.sum()), "mean_ret": round(float(r[m].mean()), 3),
                   "win": round(float((r[m] > 0).mean()) * 100, 0), "range": round(float(rng[m].mean()), 2),
                   "expiry": d == WEEKLY_DOW})
    thu = h[h.index.dayofweek == WEEKLY_DOW]
    ym = pd.Series(h.index.to_period("M"), index=h.index)
    last = thu.groupby(ym.loc[thu.index]).apply(lambda x: x.index.max())
    exp = set(pd.to_datetime(last.values))
    isexp = pd.Series(h.index.isin(exp), index=h.index)
    aft = h.index.isin(pd.to_datetime(pd.Series(h.index).shift(-1)[isexp.values].dropna().values))
    return {"by_dow": by,
            "monthly_expiry": {"n": int(isexp.sum()), "mean_ret": round(float(r[isexp].mean()), 3),
                               "win": round(float((r[isexp] > 0).mean()) * 100, 0), "range": round(float(rng[isexp].mean()), 2)},
            "non_expiry": {"mean_ret": round(float(r[~isexp].mean()), 3), "range": round(float(rng[~isexp].mean()), 2)},
            "day_after_monthly": {"mean_ret": round(float(r[aft].mean()), 3), "win": round(float((r[aft] > 0).mean()) * 100, 0)}}


def build():
    cal = calendar_pos()
    prof = profile()
    verdict = ""
    if prof:
        thu = next((x for x in prof["by_dow"] if x["expiry"]), None)
        wed = next((x for x in prof["by_dow"] if x["dow"] == "Wed"), None)
        me = prof["monthly_expiry"]; da = prof["day_after_monthly"]
        bits = []
        if thu:
            bits.append(f"expiry day (Thu) historically {thu['mean_ret']:+.2f}%/{thu['win']:.0f}% win, widest range {thu['range']}% — chop, not trend")
        bits.append(f"monthly expiry {me['mean_ret']:+.2f}%/{me['win']:.0f}% (vs non-expiry {prof['non_expiry']['mean_ret']:+.2f}%)")
        bits.append(f"day-after bounce {da['mean_ret']:+.2f}%/{da['win']:.0f}%")
        verdict = " · ".join(bits)
    return {"ok": True, "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
            "calendar": cal, "profile": prof, "verdict": verdict,
            "read": _read(cal, prof),
            "note": "Expiry is mean-reverting/pinning, not directional. NSE moved the weekly-expiry weekday "
                    "in 2024-25 — default Thursday (true for most of the 4y window); confirm with your broker. "
                    "Decision-support, not advice."}


def _read(cal, prof):
    """One-liner: is expiry the driver today, and what to do."""
    if cal["is_monthly_today"]:
        return "Today IS monthly expiry — expect pinning/chop near max-pain, mild negative bias. Don't read today's move as trend; favour range/theta over fresh directional longs."
    if cal["is_weekly_today"]:
        return "Today IS weekly expiry — pinning/chop likely, flat-to-negative bias. Today's move is expiry mechanics, not a trend signal."
    if cal["days_to_weekly"] == 1:
        return f"Expiry is TOMORROW ({cal['weekly_expiry']}). Today's tape includes pre-expiry positioning; tomorrow tends to chop/pin — avoid holding fresh directional longs into it."
    if cal["day_after_monthly"]:
        return "Day AFTER monthly expiry — the historical relief-bounce window (series rolled, hedges reset)."
    return f"Mid-cycle — expiry isn't the driver today ({cal['days_to_weekly']}d to weekly expiry). Read the tape on its own merits."


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = build()
    c = r["calendar"]
    print(f"\nEXPIRY TRACKER — {r['asof']}")
    print(f"  today {c['today']} ({c['today_dow']}) · {c['cycle_position']}")
    print(f"  next weekly {c['weekly_expiry']} ({c['days_to_weekly']}d) · next monthly {c['monthly_expiry']} ({c['days_to_monthly']}d)")
    print(f"\n  READ: {r['read']}")
    if r["profile"]:
        print(f"\n  {'day':<5}{'n':>5}{'ret%':>8}{'win':>6}{'range%':>8}")
        for x in r["profile"]["by_dow"]:
            print(f"  {x['dow']:<5}{x['n']:>5}{x['mean_ret']:>8.3f}{x['win']:>5.0f}%{x['range']:>7.2f}%{'  ◄ expiry' if x['expiry'] else ''}")
        print(f"\n  VERDICT: {r['verdict']}")
