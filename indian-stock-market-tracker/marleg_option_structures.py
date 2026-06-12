"""
Marle-G — option STRUCTURES: build, compare, and backtest (Trading Volatility).

Builds the structures the user asked to compare:
  long_straddle / long_strangle  -> LONG vol  (+gamma, +vega, -theta) : bet on a big move
  iron_butterfly / iron_condor   -> SHORT vol (-gamma, -vega, +theta) : bet vol stays range-bound
  short_straddle                 -> SHORT vol (naked, undefined risk)
  bull_call_spread               -> directional debit, defined risk

Each is priced off Black-Scholes (marleg_vol) at a given spot/IV/T: net premium, max
profit / max loss, breakevens, and NET Greeks. The right structure is regime-dependent:
cheap IV -> buy vol (straddle); rich IV -> sell vol (iron butterfly/condor).

BACKTEST (NIFTY, real data): monthly entries, IV = India VIX at entry, settle at the
actual index level ~1m later. Splits results by regime (VIX > trailing realized vol =
"rich" vs "cheap") to show WHICH structure wins WHEN. This is the vol-risk-premium test
(Bennett; IV is on average > RV, so selling vol earns a premium but carries tail risk).

  python marleg_option_structures.py --compare           # snapshot table (NIFTY)
  python marleg_option_structures.py --backtest --years 5
"""
import os, sys, math, argparse, datetime as dt
import numpy as np, pandas as pd, yfinance as yf
import marleg_vol as mv

R = mv.R_FREE


def _legs(structure, S, sigma, T):
    """Return list of (kind, strike, sign) for an ATM-centred structure.
    Wing width = 1 expected-move stdev (S*sigma*sqrt(T))."""
    em = S * sigma * math.sqrt(max(T, 1e-6))
    K = round(S)                                   # ATM
    if structure == "long_straddle":
        return [("C", K, +1), ("P", K, +1)]
    if structure == "short_straddle":
        return [("C", K, -1), ("P", K, -1)]
    if structure == "long_strangle":
        return [("C", K + em, +1), ("P", K - em, +1)]
    if structure == "iron_butterfly":            # short ATM straddle + long 1-sd wings
        return [("C", K, -1), ("P", K, -1), ("C", K + em, +1), ("P", K - em, +1)]
    if structure == "iron_condor":               # short 1-sd, long 2-sd
        return [("C", K + em, -1), ("P", K - em, -1), ("C", K + 2 * em, +1), ("P", K - 2 * em, +1)]
    if structure == "bull_call_spread":          # long ATM call, short 1-sd call
        return [("C", K, +1), ("C", K + em, -1)]
    raise ValueError(structure)


def price_structure(structure, S, sigma, T):
    legs = _legs(structure, S, sigma, T)
    net = 0.0
    g = {"delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0}
    detail = []
    for kind, K, sign in legs:
        px = mv.bs_price(S, K, T, R, sigma, kind)
        gk = mv.greeks(S, K, T, R, sigma, kind)
        net += sign * px
        for k in g:
            g[k] += sign * gk[k]
        detail.append((kind, round(K, 1), sign, round(px, 2)))
    # payoff at expiry across a +-3sd grid
    em = S * sigma * math.sqrt(max(T, 1e-6))
    grid = np.linspace(S - 3 * em, S + 3 * em, 121)
    payoff = []
    for ST in grid:
        val = sum(sign * max(0.0, (ST - K) if kind == "C" else (K - ST)) for kind, K, sign in legs)
        payoff.append(val - net)                  # minus net premium paid (net<0 = credit)
    payoff = np.array(payoff)
    return {
        "structure": structure, "legs": detail,
        "net_premium": round(net, 2),             # >0 debit (pay), <0 credit (receive)
        "max_profit": round(float(payoff.max()), 1),
        "max_loss": round(float(payoff.min()), 1),
        "greeks": {k: round(v, 4) for k, v in g.items()},
        "vol_stance": "LONG vol" if g["vega"] > 0 else "SHORT vol",
        "grid": [round(float(x), 1) for x in grid], "payoff": [round(float(x), 1) for x in payoff],
    }


STRUCTS = ["long_straddle", "long_strangle", "iron_butterfly", "iron_condor", "short_straddle", "bull_call_spread"]


def compare_data(S=None, sigma=None, T=None):
    vix = mv.india_vix()
    if S is None:
        try:
            S = float(yf.Ticker("^NSEI").history(period="1d")["Close"].dropna().iloc[-1])
        except Exception:
            S = 24000.0
    sigma = sigma or (vix / 100.0 if vix else 0.14)
    T = T or 30 / 365.0
    return {"spot": round(S, 1), "iv": round(sigma, 4), "vix": vix, "days": int(T * 365),
            "em": round(S * sigma * math.sqrt(T), 1),
            "structures": [price_structure(st, S, sigma, T) for st in STRUCTS]}


def compare(S=None, sigma=None, T=None):
    d = compare_data(S, sigma, T)
    S, sigma, T = d["spot"], d["iv"], d["days"] / 365.0
    print(f"NIFTY snapshot: spot {S:.0f}  IV(VIX) {sigma*100:.1f}%  T {int(T*365)}d  (1sd move = {d['em']:.0f})\n")
    print(f"{'STRUCTURE':<17}{'stance':<10}{'net prem':>9}{'maxP':>9}{'maxL':>9}  {'delta':>7}{'vega':>8}{'theta':>8}")
    print("-" * 84)
    for st in STRUCTS:
        r = price_structure(st, S, sigma, T)
        g = r["greeks"]
        kind = "credit" if r["net_premium"] < 0 else "debit"
        print(f"{st:<17}{r['vol_stance']:<10}{r['net_premium']:>8}{kind[0]} {r['max_profit']:>8}{r['max_loss']:>9}  "
              f"{g['delta']:>7.2f}{g['vega']:>8.1f}{g['theta']:>8.2f}")
    print("\ncheap IV -> LONG vol (straddle/strangle) ; rich IV -> SHORT vol (iron butterfly/condor).")


def backtest_data(years=5, hold_days=21):
    """NIFTY monthly: enter at IV=VIX, settle at actual index ~1m later. Split by regime."""
    end = dt.date.today()
    start = end - dt.timedelta(days=int(years * 365) + 60)
    nf = yf.Ticker("^NSEI").history(start=start.isoformat(), end=end.isoformat())["Close"].dropna()
    vx = yf.Ticker("^INDIAVIX").history(start=start.isoformat(), end=end.isoformat())["Close"].dropna()
    if len(nf) < 100 or len(vx) < 100:
        return {"error": "insufficient history"}
    logret = np.log(nf / nf.shift(1)).dropna()
    rows = []
    i, idx = 30, nf.index
    while i + hold_days < len(nf):
        t0 = idx[i]; S0 = float(nf.iloc[i])
        try:
            vix0 = float(vx.reindex([t0], method="ffill").iloc[0]) / 100.0
        except Exception:
            i += hold_days; continue
        rv = float(np.std(logret.iloc[i - 20:i]) * math.sqrt(252))
        T = hold_days / 252.0
        S1 = float(nf.iloc[i + hold_days])
        rec = {"date": t0.date().isoformat(), "vix": vix0, "rv": rv,
               "regime": "rich" if vix0 > rv else "cheap"}
        for st in STRUCTS:
            legs = _legs(st, S0, vix0, T)
            entry = sum(sign * mv.bs_price(S0, K, T, R, vix0, kind) for kind, K, sign in legs)
            settle = sum(sign * max(0.0, (S1 - K) if kind == "C" else (K - S1)) for kind, K, sign in legs)
            rec[st] = settle - entry
        rows.append(rec); i += hold_days
    df = pd.DataFrame(rows)
    rich, cheap = df[df.regime == "rich"], df[df.regime == "cheap"]
    return {
        "cycles": len(df), "hold_days": hold_days, "years": years,
        "avg_vix": round(float(df.vix.mean()), 4), "avg_rv": round(float(df.rv.mean()), 4),
        "vrp": round(float(df.vix.mean() - df.rv.mean()), 4),
        "n_rich": len(rich), "n_cheap": len(cheap),
        "rows": [{"structure": st, "avg": round(float(df[st].mean()), 1),
                  "win": round(float((df[st] > 0).mean()) * 100, 0),
                  "rich": round(float(rich[st].mean()), 1) if len(rich) else None,
                  "cheap": round(float(cheap[st].mean()), 1) if len(cheap) else None} for st in STRUCTS],
    }


def backtest(years=5, hold_days=21):
    d = backtest_data(years, hold_days)
    if d.get("error"):
        print(d["error"]); return
    n = d["cycles"]
    print(f"NIFTY structure backtest: {n} monthly cycles over ~{years}y, hold {hold_days}d, IV=VIX, settle=actual.\n")
    print(f"{'STRUCTURE':<17}{'avg P&L':>10}{'win%':>7}{'  | rich-IV avg':>16}{'  cheap-IV avg':>15}")
    print("-" * 70)
    for r in d["rows"]:
        print(f"{r['structure']:<17}{r['avg']:>10.1f}{r['win']:>6.0f}%{(r['rich'] or 0):>16.1f}{(r['cheap'] or 0):>15.1f}")
    print("-" * 70)
    print(f"regimes: {d['n_rich']} rich (VIX>RV), {d['n_cheap']} cheap (VIX<RV).  "
          f"avg VIX {d['avg_vix']*100:.1f}% vs avg RV {d['avg_rv']*100:.1f}%  "
          f"-> vol-risk-premium = {d['vrp']*100:+.1f}%")
    print("P&L is per 1 index unit (scale by lot for rupees). No costs/slippage; ATM 1sd wings; monthly.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compare", action="store_true")
    ap.add_argument("--backtest", action="store_true")
    ap.add_argument("--years", type=int, default=5)
    a = ap.parse_args()
    if a.backtest:
        backtest(a.years)
    else:
        compare()


if __name__ == "__main__":
    main()
