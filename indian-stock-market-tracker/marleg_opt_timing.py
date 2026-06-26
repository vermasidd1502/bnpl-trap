"""
marleg_opt_timing.py — WHEN to buy the option, not just which one. The entry-timing + cyclical layer for the
option suggestion / surface pods. Groww-only, MASTER-DRIVEN (works for any listed underlying).

The question: "is now the right time to buy, or wait for the contract to roll?" For an option BUYER two
things move with the calendar:
  1. THETA RUNWAY — a fresh cycle gives the most days, so the least %/day decay for a 1–2 week hold. Buying a
     near-expiry contract for a 2-week view is a theta cliff; the answer is "wait for / buy the next series".
  2. A real SEASONAL tilt (Marle-G backtest, NIFTY, 948 sessions): FRESH-cycle first session (right after
     monthly expiry) +0.24% / 67% win vs +0.037% base; early week (Mon–Wed) up-drifts, Thu/Fri fade, expiry
     day pins to max-pain; the WEEK BEFORE monthly expiry is the weakest window AND the theta cliff.

The CYCLICAL play ("dump before the mayhem, re-buy fresh") — backtested (48 monthly cycles, fresh ATM call):
held-to-expiry the TYPICAL (median) call ends −72.9% vs exit-4d-early −20.5% (same mean upside). So dumping
the OPTION ~4 sessions before its expiry is THETA HYGIENE — it more than halves the typical bleed. Do NOT
dump the UNDERLYING (NIFTY's expiry week is mildly positive; sitting it out lost +52%→+27%).

Expiry dates come from Groww's instruments master, so NSE-Tuesday / monthly-only-midcap (MIDCPNIFTY,
FINNIFTY, NIFTYNXT50) / BSE-Thursday (SENSEX, BANKEX) are all handled correctly — no hard-coded calendar.
The seasonal %s are NIFTY-measured; the cycle/theta LOGIC is universal, so for other indices they're a labelled
proxy (the structure holds; the exact number is NIFTY's).

Read-only. Decision-support, not investment advice — I'm not a licensed advisor.

  python marleg_opt_timing.py NIFTY
  python marleg_opt_timing.py MIDCPNIFTY --hold 10
"""
import datetime as dt
import calendar as _cal

import marleg_vol as mv

FRESH_EDGE = {"ret": 0.24, "win": 67, "base": 0.037}
EXIT_EDGE = {"exit_days": 4, "exit_median": -20.5, "hold_median": -72.9}
WEEKDAY_RET = {0: 0.073, 1: 0.037, 2: 0.118, 3: -0.032, 4: -0.003}
WEEKDAY = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _today_ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).date()


def _is_trading(d):
    try:
        return mv._is_trading_day(d)
    except Exception:
        return d.weekday() < 5


def _roll_back(d):
    for _ in range(6):
        if _is_trading(d):
            return d
        d -= dt.timedelta(days=1)
    return d


def _back_sessions(d, n):
    cnt = 0
    while cnt < n and d:
        d -= dt.timedelta(days=1)
        if _is_trading(d):
            cnt += 1
    return d


def _expiries(und):
    """Real future option expiries from Groww's instruments master (sorted dates)."""
    try:
        import marleg_instruments as inst
        return [dt.date.fromisoformat(e) for e in (inst.expiries(und, within_days=200) or [])]
    except Exception:
        return []


def _monthlies(exps):
    """The month-end expiry of each month = the last listed expiry whose next one is a different month."""
    out = []
    for i, e in enumerate(exps):
        if i == len(exps) - 1 or (exps[i + 1].year, exps[i + 1].month) != (e.year, e.month):
            out.append(e)
    return out


def _last_wd_prev_month(anchor, wd):
    """Last weekday `wd` of the month BEFORE anchor's month (holiday-rolled) — the prior monthly expiry."""
    y, m = (anchor.year - 1, 12) if anchor.month == 1 else (anchor.year, anchor.month - 1)
    try:
        e = mv.last_weekday(y, m, wd)
    except Exception:
        e = dt.date(y, m, _cal.monthrange(y, m)[1])
        while e.weekday() != wd:
            e -= dt.timedelta(days=1)
    return _roll_back(e)


def timing(und="NIFTY", hold=10):
    und = (und or "NIFTY").upper()
    today = _today_ist()
    wd = today.weekday()
    exps = _expiries(und)
    if not exps:
        return {"ok": False, "underlying": und, "error": f"{und} has no listed option expiries in the Groww master"}

    months = _monthlies(exps)
    next_monthly = next((e for e in months if e >= today), months[-1])
    expiry_wd = next_monthly.weekday()                       # Tue (NSE) or Thu (BSE) — read from the master
    prev_monthly = _last_wd_prev_month(next_monthly, expiry_wd)
    next_expiry = exps[0]
    has_weeklies = any(e not in months for e in exps)
    next_weekly = next((e for e in exps if e not in months and e >= today), None) if has_weeklies else None

    dte_next = (next_expiry - today).days
    dte_monthly = (next_monthly - today).days
    dte_weekly = (next_weekly - today).days if next_weekly else None
    cycle_day = (today - prev_monthly).days
    fresh_entry = prev_monthly + dt.timedelta(days=1)
    next_fresh = next_monthly + dt.timedelta(days=1)
    seasonal_proxy = und != "NIFTY"
    px = " (NIFTY-measured proxy — the structure holds for this index, the exact % is NIFTY's)" if seasonal_proxy else ""

    # ---- verdict ----------------------------------------------------------------
    fresh = cycle_day <= 4
    pre_expiry = 1 <= dte_monthly <= 9
    expiry_day = dte_next == 0
    early_week = wd <= 2
    if fresh:
        label, tone = "PRIME", "good"
        why = (f"fresh monthly cycle (day {cycle_day}) — max theta runway AND the seasonal pop "
               f"(+{FRESH_EDGE['ret']}% / {FRESH_EDGE['win']}% right after the roll vs {FRESH_EDGE['base']}% base{px}). "
               f"Best window to open a directional CALL.")
    elif expiry_day:
        label, tone = "WAIT", "warn"
        why = (f"expiry day ({WEEKDAY[expiry_wd]} series) — the index pins to max-pain and chops, not trends. "
               f"Don't open a fresh directional buy today; let it expire and buy the new series.")
    elif pre_expiry:
        label, tone = "WAIT", "warn"
        why = (f"week before monthly expiry ({dte_monthly}d left) — historically the weakest window AND a theta "
               f"cliff{px}. Prefer waiting for the roll: fresh entry ≈ {next_fresh.isoformat()}.")
    elif early_week:
        label, tone = "GOOD", "good"
        why = (f"early week ({WEEKDAY[wd]}, hist +{WEEKDAY_RET[wd]:.2f}%/day{px}) with {dte_monthly}d of monthly "
               f"runway — a fine time to open. Use an expiry with ≥{hold + 5}d.")
    else:
        label, tone = "OK", "normal"
        why = (f"mid-to-late week ({WEEKDAY[wd]}, hist {WEEKDAY_RET[wd]:+.2f}%/day{px}) — neutral. Workable, but the "
               f"fresh-cycle and early-week windows are statistically better entries.")

    # ---- theta-runway ----------------------------------------------------------
    if has_weeklies:
        runway = (f"For a ~{hold}-session ({round(hold / 5)}wk) hold pick an expiry ≥ {hold + 5} DTE so theta isn't the "
                  f"trade. Next weekly {next_weekly.isoformat()} ({dte_weekly}d — too soon if < {hold}); this monthly "
                  f"{next_monthly.isoformat()} ({dte_monthly}d).")
    else:
        runway = (f"{und} is MONTHLY-ONLY (no weeklies). Your contract is the {next_monthly.isoformat()} monthly "
                  f"({dte_monthly}d) — for a ~{hold}-session hold that's {'enough' if dte_monthly >= hold + 3 else 'tight; consider the next monthly'}.")

    # ---- the COHERENT cyclical play: ONE contract, forward-ordered (BUY < DUMP < RE-BUY) -------------
    # The series you actually hold = the first monthly with enough runway for the hold (skip the near one if
    # it's already in the cliff). DUMP/RE-BUY are computed from THAT series' expiry, so the dates can't invert.
    idx = months.index(next_monthly) if next_monthly in months else 0
    trade_exp = next_monthly
    while (trade_exp - today).days < hold + EXIT_EDGE["exit_days"] + 1 and idx + 1 < len(months):
        idx += 1
        trade_exp = months[idx]
    buy_date = today if trade_exp == next_monthly else next_fresh   # buy now if the front month has runway, else after the roll
    exit_by = _back_sessions(trade_exp, EXIT_EDGE["exit_days"])
    rebuy = trade_exp + dt.timedelta(days=1)
    trade_dte = (trade_exp - today).days
    exit_rule = (f"Dump a directional option ~{EXIT_EDGE['exit_days']} sessions BEFORE its expiry — don't hold into "
                 f"the final theta cliff. Backtest (48 NIFTY monthly cycles, fresh ATM call): held to expiry the "
                 f"TYPICAL (median) outcome is {EXIT_EDGE['hold_median']}%; exited {EXIT_EDGE['exit_days']}d early it's "
                 f"{EXIT_EDGE['exit_median']}% — same mean upside, far less bleed. Theta hygiene, not mayhem-alpha.")
    cycle_plan = [{"step": "BUY", "when": buy_date.isoformat(),
                   "what": f"the {trade_exp.isoformat()} series ({trade_dte}d runway)" + ("" if buy_date == today else " — after the roll")},
                  {"step": "DUMP", "when": exit_by.isoformat(),
                   "what": f"~{EXIT_EDGE['exit_days']} sessions before {trade_exp.isoformat()}, before the theta cliff"},
                  {"step": "RE-BUY", "when": rebuy.isoformat(), "what": "roll into the next fresh series — repeat"}]
    next_fresh_entry = buy_date
    # if you ALREADY hold the near (about-to-expire) series, here's when to get out of THAT one
    near_exit = _back_sessions(next_monthly, EXIT_EDGE["exit_days"])
    holding_note = (f"Already holding the near {next_monthly.isoformat()} series? It's in/near the theta cliff — "
                    f"exit it by ~{near_exit.isoformat()} (this is a SEPARATE position from the fresh buy above)."
                    if dte_monthly <= hold + EXIT_EDGE["exit_days"] else None)
    underlying_warning = ("Applies to the OPTION only. Do NOT dump the underlying index before expiry — the expiry "
                          "week is mildly POSITIVE; sitting it out lost +52%→+27% in the NIFTY backtest.")

    return {"ok": True, "underlying": und, "exchange": _ex(und),
            "today": today.isoformat(), "weekday": WEEKDAY[wd], "monthly_only": not has_weeklies,
            "monthly_expiry": next_monthly.isoformat(), "dte_monthly": dte_monthly,
            "next_weekly_expiry": next_weekly.isoformat() if next_weekly else None, "dte_weekly": dte_weekly,
            "next_expiry": next_expiry.isoformat(), "dte_next": dte_next,
            "cycle_day": cycle_day, "last_expiry": prev_monthly.isoformat(),
            "next_fresh_entry": next_fresh_entry.isoformat(), "verdict": label, "tone": tone, "why": why,
            "runway": runway, "exit_by": exit_by.isoformat(), "exit_rule": exit_rule, "cycle_plan": cycle_plan,
            "holding_note": holding_note, "underlying_warning": underlying_warning, "seasonal_proxy": seasonal_proxy,
            "basis": {"fresh_edge": FRESH_EDGE, "weekday_ret": WEEKDAY_RET, "exit_edge": EXIT_EDGE},
            "asof": today.isoformat(),
            "note": "Entry-timing for option BUYERS: theta runway (fresh cycle = most days) + a validated NIFTY "
                    "seasonal tilt + the cyclical dump-before-expiry rule. Expiries read live from Groww's master.",
            "caveat": "A timing TILT, not a guarantee — it sets the odds, your direction call still dominates. "
                      "CALL/up edge (India drifts up); never a reason to be short. Not investment advice."}


def _ex(und):
    try:
        import marleg_instruments as inst
        return inst.exchange_of(und)
    except Exception:
        return None


if __name__ == "__main__":
    import sys, argparse
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("underlying", nargs="?", default="NIFTY")
    ap.add_argument("--hold", type=int, default=10)
    a = ap.parse_args()
    r = timing(a.underlying, a.hold)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n  {r['underlying']} ({r['exchange']}) · {r['today']} ({r['weekday']}) · {'MONTHLY-ONLY' if r['monthly_only'] else 'has weeklies'}")
    print(f"  monthly {r['monthly_expiry']} ({r['dte_monthly']}d) · next expiry {r['next_expiry']} ({r['dte_next']}d) · cycle day {r['cycle_day']}")
    print(f"\n  ⏰ {r['verdict']} — {r['why']}")
    print(f"\n  cyclical: BUY {r['cycle_plan'][0]['when']} → DUMP {r['exit_by']} → RE-BUY {r['next_fresh_entry']}")
    print(f"  {r['runway']}")
    print(f"\n  {r['caveat']}")
