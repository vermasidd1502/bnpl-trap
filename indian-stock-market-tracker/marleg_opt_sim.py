"""
marleg_opt_sim.py — what happens AFTER you click a suggestion: a forward Monte-Carlo of the trade, and a
historical reality-check on the style. Groww-only.

simulate(...)  FORWARD: simulate thousands of GBM paths of the underlying over the hold (vol = the option's
               IV), reprice the option on each, and report the OUTCOME DISTRIBUTION — P(profit), expected
               return, the 10/50/90th-percentile P&L, P(it tags your target), P(it hits your stop), and the
               terminal payoff histogram. Zero drift (no edge assumed) — the honest base case.

backtest(...)  HISTORICAL: replay the STYLE over Groww's underlying history — "every few sessions, buy a
               {mode} {call/put} on this name and hold ~2 weeks" — pricing each option with Black-Scholes on
               the trailing realized vol. Reports win rate, average/median return, Sharpe, max drawdown and
               an equity curve. HONEST LIMITS: Groww has no deep option-premium history, so this is SYNTHETIC
               — BS-priced at trailing realized vol, no bid/ask, no IV crush, no slippage. It reality-checks
               the style ("does repeatedly buying this kind of option on this name pay?"), it is NOT a live
               P&L promise. Net of real costs, most retail option-buying styles bleed — let the curve show it.

Reuses marleg_vol (BS / IV / normal CDF), marleg_data (Groww history), marleg_expiry_matrix (_spot/_realized).
Read-only. Decision-support, not investment advice — I'm not a licensed advisor.

  python marleg_opt_sim.py NIFTY 24200 2026-07-28 C
"""
import math
import datetime as dt

import numpy as np

import marleg_vol as mv
import marleg_options_monitor as mom
import marleg_data as md
from marleg_expiry_matrix import _spot, _realized

R = mv.R_FREE


def _pct(a, q):
    return float(np.percentile(a, q))


def simulate(und, strike, expiry, kind="C", premium=None, iv=None, hold=10,
             target=None, stop=None, n_paths=6000, drift=0.0):
    """Forward Monte-Carlo of one option over the hold. Returns the P&L distribution + hit probabilities."""
    und = (und or "NIFTY").upper()
    kind = "C" if str(kind).upper().startswith("C") else "P"
    K = float(strike)
    today = dt.date.today()
    try:
        e = dt.date.fromisoformat(str(expiry)[:10])
    except Exception:
        return {"ok": False, "error": f"bad expiry {expiry}"}
    dte = (e - today).days
    if dte <= 0:
        return {"ok": False, "error": "already expired"}
    spot = _spot(und)
    if not spot:
        return {"ok": False, "error": f"no spot for {und}"}
    rv = _realized(und) or 0.12
    if premium is None or iv is None:                  # fetch live if the caller didn't pass them
        try:
            c = __import__("marleg_instruments").contract(und, e.isoformat(), K, kind)
            q = mom.option_quote(c["symbol"]) if c else {}
            premium = premium or q.get("ltp")
            iv = iv or q.get("iv")
        except Exception:
            pass
    if not premium or premium <= 0:
        return {"ok": False, "error": "no live premium"}
    if not iv:
        iv, _ = mv.implied_vol_newton(premium, spot, K, dte / 365.0, R, kind)
    iv = iv or rv

    h = min(hold, dte)
    dt_step = 1.0 / 252.0
    vol = max(iv, 1e-4)
    rng = np.random.default_rng(7)                     # fixed seed -> reproducible (Math.random would not be)
    z = rng.standard_normal((n_paths, h))
    incr = (drift - 0.5 * vol * vol) * dt_step + vol * math.sqrt(dt_step) * z
    logpath = np.cumsum(incr, axis=1)
    paths = spot * np.exp(logpath)                     # (n_paths, h) daily closes
    S_T = paths[:, -1]
    pmax = np.maximum(paths.max(axis=1), spot)
    pmin = np.minimum(paths.min(axis=1), spot)

    dte_after = dte - h
    T_after = max(dte_after, 0) / 365.0
    if dte_after <= 0:
        val = np.maximum(S_T - K, 0.0) if kind == "C" else np.maximum(K - S_T, 0.0)
    else:                                              # vectorised BS reprice at the hold horizon
        val = _bs_vec(S_T, K, T_after, R, iv, kind)
    ret = val / premium - 1.0

    out = {"ok": True, "underlying": und, "strike": K, "expiry": e.isoformat(), "kind": kind,
           "right": "CE" if kind == "C" else "PE", "spot": round(spot, 2), "premium": round(premium, 2),
           "iv_pct": round(iv * 100, 1), "rv_pct": round(rv * 100, 1), "hold_sessions": h, "dte": dte,
           "n_paths": n_paths,
           "p_profit": round(float((val > premium).mean()), 3),
           "p_total_loss": round(float((val < 0.2 * premium).mean()), 3),
           "exp_return_pct": round(float(ret.mean()) * 100, 1),
           "median_return_pct": round(float(np.median(ret)) * 100, 1),
           "ret_p10_pct": round(_pct(ret, 10) * 100, 0), "ret_p25_pct": round(_pct(ret, 25) * 100, 0),
           "ret_p75_pct": round(_pct(ret, 75) * 100, 0), "ret_p90_pct": round(_pct(ret, 90) * 100, 0),
           "exp_value": round(float(val.mean()), 2),
           "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")}
    if target:
        out["target"] = round(float(target), 1)
        out["p_hit_target"] = round(float((pmax >= target).mean() if kind == "C" else (pmin <= target).mean()), 3)
    if stop:
        out["stop"] = round(float(stop), 1)
        out["p_hit_stop"] = round(float((pmin <= stop).mean() if kind == "C" else (pmax >= stop).mean()), 3)
    # terminal P&L histogram (return %), 11 bins clipped to [-100, +250]
    rc = np.clip(ret * 100, -100, 250)
    hist, edges = np.histogram(rc, bins=11, range=(-100, 250))
    out["hist"] = [{"lo": round(float(edges[i])), "hi": round(float(edges[i + 1])), "n": int(hist[i])}
                   for i in range(len(hist))]
    out["caveat"] = ("Forward Monte-Carlo at the option's IV, zero drift (no edge assumed). A base-case shape, "
                     "not a forecast — real IV moves and your direction call dominate. Not investment advice.")
    return out


def _ncdf_vec(x):
    """Vectorised standard-normal CDF (Abramowitz-Stegun 7.1.26) — works elementwise on a numpy array, no scipy."""
    t = 1.0 / (1.0 + 0.2316419 * np.abs(x))
    d = 0.3989423 * np.exp(-x * x / 2.0)
    p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))))
    return np.where(x > 0, 1.0 - p, p)


def _bs_vec(S, K, T, r, sig, kind):
    """Vectorised Black-Scholes over an array of spots."""
    S = np.asarray(S, dtype=float)
    if T <= 0:
        return np.maximum(S - K, 0.0) if kind == "C" else np.maximum(K - S, 0.0)
    d1 = (np.log(S / K) + (r + 0.5 * sig * sig) * T) / (sig * math.sqrt(T))
    d2 = d1 - sig * math.sqrt(T)
    if kind == "C":
        return S * _ncdf_vec(d1) - K * math.exp(-r * T) * _ncdf_vec(d2)
    return K * math.exp(-r * T) * _ncdf_vec(-d2) - S * _ncdf_vec(-d1)


_MODE_OFFSET = {"conv": 0.0, "rr": 0.004, "ride": 0.013, "bleed": 0.0}     # entry moneyness by ranking mode
_MODE_DTE = {"bleed": 35}                                                   # extra time for the low-bleed style


def backtest(und, kind="C", mode="rr", hold=10, lookback=820, step=5):
    """Synthetic historical replay of the '{mode} {right}' buying style. BS-priced on trailing realized vol."""
    und = (und or "NIFTY").upper()
    kind = "C" if str(kind).upper().startswith("C") else "P"
    df = md.candles(und, 1440, lookback + 60)
    if df is None or len(df) < 120:
        return {"ok": False, "error": f"not enough history for {und}"}
    c = df["close"].dropna().values.astype(float)
    n = len(c)
    off = _MODE_OFFSET.get(mode, 0.004)
    dte0 = hold + _MODE_DTE.get(mode, 15)
    step_strike = mom.INDEX_STEP.get(und) or mom._strike_step(c[-1])

    rets, eq, curve = [], 1.0, []
    i = 25
    while i < n - hold - 1:
        win = c[i - 20:i]
        lr = np.diff(np.log(win))
        rv = float(np.std(lr) * math.sqrt(252)) if len(lr) > 5 else 0.12
        rv = min(max(rv, 0.05), 0.60)
        spot0 = c[i]
        K = round((spot0 * (1 + off if kind == "C" else 1 - off)) / step_strike) * step_strike
        prem0 = mv.bs_price(spot0, K, dte0 / 365.0, R, rv, kind)
        if prem0 <= 0.5:
            i += step; continue
        spotH = c[i + hold]
        valH = mv.bs_price(spotH, K, max(dte0 - hold, 0) / 365.0, R, rv, kind)
        ret = valH / prem0 - 1.0
        rets.append(ret)
        eq *= (1 + 0.15 * ret)                          # 15%-of-capital sizing per trade (so a -100% can't ruin)
        curve.append({"i": int(i), "date": str(df.index[i].date()) if hasattr(df.index[i], "date") else i,
                      "ret_pct": round(ret * 100, 1), "equity": round(eq, 3)})
        i += step

    if not rets:
        return {"ok": False, "error": "no trades generated"}
    a = np.array(rets)
    trades_per_yr = 252.0 / step
    sharpe = float(a.mean() / a.std() * math.sqrt(trades_per_yr)) if a.std() > 1e-9 else 0.0
    peak, mdd = 1.0, 0.0
    for pt in curve:
        peak = max(peak, pt["equity"]); mdd = min(mdd, pt["equity"] / peak - 1)
    return {"ok": True, "underlying": und, "kind": kind, "right": "CE" if kind == "C" else "PE",
            "mode": mode, "hold_sessions": hold, "entry_dte": dte0, "step_sessions": step,
            "n_trades": len(rets), "win_pct": round(float((a > 0).mean()) * 100, 1),
            "avg_return_pct": round(float(a.mean()) * 100, 1), "median_return_pct": round(float(np.median(a)) * 100, 1),
            "best_pct": round(float(a.max()) * 100, 0), "worst_pct": round(float(a.min()) * 100, 0),
            "sharpe": round(sharpe, 2), "max_drawdown_pct": round(mdd * 100, 1),
            "final_equity": round(eq, 3), "curve": curve[-160:],
            "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
            "caveat": ("SYNTHETIC: Groww has no historical option premiums, so each trade is Black-Scholes-priced "
                       "at the trailing realized vol — no bid/ask, no IV crush, no slippage. A reality-check on the "
                       "STYLE, not a live track record. Real costs make most option-buying styles worse than shown. "
                       "Not investment advice; I'm not a licensed advisor.")}


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    und = sys.argv[1] if len(sys.argv) > 1 else "NIFTY"
    K = float(sys.argv[2]) if len(sys.argv) > 2 else 24200
    exp = sys.argv[3] if len(sys.argv) > 3 else (dt.date.today() + dt.timedelta(days=35)).isoformat()
    knd = sys.argv[4] if len(sys.argv) > 4 else "C"
    s = simulate(und, K, exp, knd)
    print("\n  FORWARD SIM:", json.dumps({k: s[k] for k in s if k not in ("hist",)}, default=str)[:600] if s.get("ok") else s.get("error"))
    b = backtest(und, knd, "ride")
    if b.get("ok"):
        print(f"\n  HISTORICAL (ride {b['right']}): {b['n_trades']} trades · win {b['win_pct']}% · "
              f"avg {b['avg_return_pct']}% · Sharpe {b['sharpe']} · maxDD {b['max_drawdown_pct']}% · "
              f"final eq {b['final_equity']}x")
        print(f"  {b['caveat']}")
