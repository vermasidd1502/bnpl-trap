"""
marleg_option_exit_bt.py — BACKTEST the "scalp the option, EXIT before the theta cliff" rule.

The claim (from the theta surface): a long option BUYER should exit before the last ~2 weeks, because theta
(decay/day) accelerates into expiry. This tests it on REAL underlying paths.

Method (honest about what it is): for a basket of liquid F&O names over ~3y of daily data, at rolling entry
dates we BUY an ATM call and re-price it each day with Black-Scholes along the REAL price path (σ = the name's
trailing realized vol at entry, held fixed). We then compare three exit rules:
  • HOLD  — carry to expiry (intrinsic at T=0)
  • CLIFF — exit when the option reaches the theta cliff (`cliff_dte` days left)
  • HALF  — exit at half the tenor (a fixed-time scalp)

This is a MODEL backtest (BS-priced options on real paths, fixed σ, no bid/ask cost, ATM only). It cannot see
the vol-risk-premium or IV-crush; it CAN see the theta/gamma cost of carrying into expiry — which is exactly the
cliff question. Read the tail (p10) as much as the mean: the "wrath of expiry" shows up as the left tail of HOLD.
"""
import math

import numpy as np

import marleg_data as md
import marleg_vol as mv

BASKET = ["RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "INFY", "TCS", "LT", "AXISBANK",
          "TATAMOTORS", "TATASTEEL", "MARUTI", "SUNPHARMA", "BHARTIARTL", "BAJFINANCE", "TITAN", "TRENT"]


def _rolling_rv(close, win=20):
    r = np.diff(np.log(close))
    rv = np.full(len(close), np.nan)
    for i in range(win, len(close)):
        rv[i] = np.std(r[i - win:i]) * math.sqrt(252)
    return rv


def _sim(close, rv, i, entry_dte, cliff_dte, r):
    S0 = close[i]; K = S0
    sig = rv[i]
    if not (sig and sig > 0.02):
        return None
    prem0 = mv.bs_price(S0, K, entry_dte / 365.0, r, sig, "C")
    if prem0 <= 0:
        return None
    j_exp = max(1, round(entry_dte / 1.4))                 # trading days to expiry (~1.4 cal/trading)
    if i + j_exp >= len(close):
        return None
    j_cliff = max(1, min(round((entry_dte - cliff_dte) / 1.4), j_exp))
    j_half = max(1, min(round(j_exp / 2), j_exp))

    def val(j):
        cal_left = max(entry_dte - j * 1.4, 0.0)
        S = close[i + j]
        if cal_left <= 0.5:
            return max(S - K, 0.0)
        return mv.bs_price(S, K, cal_left / 365.0, r, sig, "C")

    return {"HOLD": val(j_exp) / prem0 - 1, "CLIFF": val(j_cliff) / prem0 - 1, "HALF": val(j_half) / prem0 - 1}


def _agg(rets):
    a = np.array(rets, dtype=float)
    return {"n": len(a), "mean_pct": round(float(np.mean(a)) * 100, 1), "median_pct": round(float(np.median(a)) * 100, 1),
            "win_pct": round(float(np.mean(a > 0)) * 100, 1), "p10_pct": round(float(np.percentile(a, 10)) * 100, 1),
            "p90_pct": round(float(np.percentile(a, 90)) * 100, 1)}


def run(entry_dte=30, cliff_dte=14, step=5, gated=True):
    r = mv.R_FREE
    out = {"HOLD": [], "CLIFF": [], "HALF": []}
    out_g = {"HOLD": [], "CLIFF": [], "HALF": []}
    names = 0
    for sym in BASKET:
        df = md.candles(sym, 1440, 800)
        if df is None or len(df) < 200:
            continue
        close = df["close"].astype(float).values
        rv = _rolling_rv(close, 20)
        names += 1
        for i in range(30, len(close) - round(entry_dte / 1.4) - 1, step):
            s = _sim(close, rv, i, entry_dte, cliff_dte, r)
            if not s:
                continue
            for k in out:
                out[k].append(s[k])
            if gated and i >= 20 and close[i] > close[i - 20]:        # momentum gate: 20d uptrend at entry
                for k in out_g:
                    out_g[k].append(s[k])
    res = {"params": {"entry_dte": entry_dte, "cliff_dte": cliff_dte, "names": names, "step": step},
           "all": {k: _agg(v) for k, v in out.items() if v},
           "gated_uptrend": {k: _agg(v) for k, v in out_g.items() if v} if gated else None,
           "note": "MODEL backtest: BS-priced ATM calls on real paths, σ fixed at entry's realized vol, no costs. "
                   "Captures theta/gamma (the cliff), NOT vol-risk-premium / IV-crush / spread. CLIFF beating HOLD "
                   "on mean AND p10 = exiting before the cliff avoids the worst of the theta bleed."}
    return res


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for (ed, cd) in [(30, 14), (45, 21)]:
        r = run(entry_dte=ed, cliff_dte=cd)
        print(f"\n═══ ENTER at {ed}d DTE · EXIT at {cd}d cliff · {r['params']['names']} names ═══")
        for scope in ("all", "gated_uptrend"):
            g = r.get(scope)
            if not g:
                continue
            print(f"  ── {scope} ──   (n={g['HOLD']['n']})")
            print(f"     {'rule':<7}{'mean%':>8}{'median%':>9}{'win%':>7}{'p10%(tail)':>12}{'p90%':>8}")
            for k in ("HOLD", "CLIFF", "HALF"):
                a = g[k]
                print(f"     {k:<7}{a['mean_pct']:>8}{a['median_pct']:>9}{a['win_pct']:>7}{a['p10_pct']:>12}{a['p90_pct']:>8}")
    print("\n  " + r["note"])
