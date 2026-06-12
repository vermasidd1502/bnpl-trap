"""
Marle-G — EXPRESSION BACKTEST: the same volume-long signal, three ways to express it.

Question (Sid's): "can we backtest our logic of volume based longs on MTF or options?"
Signal: 20d up/down-volume conviction (ud), top-N basket, rebalanced every 15 trading days
(the positional horizon Pareto said carries the edge — daily versions die on costs).

Expressions, per 15-day window, all NET:
  CASH   basket return - delivery costs on turnover (~33 bps round-trip)
  MTF 2x 2 * basket return - MTF interest (~16%/yr on the borrowed half) - 2x costs
  CALLS  buy 1-month ATM calls on the basket names, hold the window, revalue:
         premium priced via Black-Scholes at IV = 1.15 * realized vol (the VRP markup
         you PAY as an option BUYER), exit value at remaining life. Notional-matched.

Output: per-expression net CAGR / Sharpe / win% / maxDD + verdict.
  python marleg_expression_bt.py
"""
import math, os, sys, json
import numpy as np
import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
HOLD = 15                      # trading days per window
TOPN = 8
LOOK = 20                      # ud lookback
COST_RT = 33.0 / 1e4           # delivery round-trip
MTF_LEV = 2.0
MTF_RATE = 0.16                # broker MTF interest /yr on the borrowed leg
VRP_MARKUP = 1.15              # IV you pay vs realized vol when BUYING options
WINDOWS_PER_YR = 252 / HOLD

UNIV = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "ITC", "LT", "SBIN", "AXISBANK",
        "KOTAKBANK", "BHARTIARTL", "BAJFINANCE", "HINDUNILVR", "MARUTI", "SUNPHARMA",
        "EICHERMOT", "TATASTEEL", "M&M", "NTPC", "TITAN", "ASIANPAINT", "ULTRACEMCO",
        "WIPRO", "ADANIPORTS", "JSWSTEEL", "COALINDIA", "ONGC", "GRASIM", "HCLTECH", "CIPLA"]


def _bs_call(S, K, T, sigma, r=0.065):
    if T <= 0:
        return max(0.0, S - K)
    d1 = (math.log(S / K) + (r + sigma * sigma / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    N = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))
    return S * N(d1) - K * math.exp(-r * T) * N(d2)


def run(period="3y"):
    df = yf.download([s + ".NS" for s in UNIV], period=period, interval="1d",
                     group_by="ticker", auto_adjust=False, progress=False, threads=True)
    C, V = {}, {}
    for s in UNIV:
        try:
            d = df[s + ".NS"]
            if len(d["Close"].dropna()) > 300:
                C[s], V[s] = d["Close"], d["Volume"]
        except Exception:
            pass
    C, V = pd.DataFrame(C), pd.DataFrame(V)
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(LOOK).sum()
    dnv = V.where(rc < 0, 0.0).rolling(LOOK).sum().replace(0, np.nan)
    ud = upv / dnv
    rv20 = rc.rolling(20).std() * math.sqrt(252)          # realized vol per name

    cash_w, mtf_w, call_w, dates = [], [], [], []
    prev_basket = []
    for i in range(LOOK + 21, len(C) - HOLD, HOLD):
        sig = ud.iloc[i].dropna()
        sig = sig[sig > 1.3]                               # conviction filter (the volume-long logic)
        if len(sig) < 3:
            continue
        basket = list(sig.sort_values(ascending=False).head(TOPN).index)
        turn = 1.0 if not prev_basket else len(set(basket) ^ set(prev_basket)) / (2.0 * max(len(basket), 1))
        prev_basket = basket
        p0 = C[basket].iloc[i]
        p1 = C[basket].iloc[i + HOLD]
        r = float((p1 / p0 - 1).mean())                    # basket window return
        # CASH
        cash_w.append(r - turn * COST_RT)
        # MTF 2x: levered return - interest on borrowed half for the window - levered costs
        mtf_w.append(MTF_LEV * r - MTF_RATE * (MTF_LEV - 1) * (HOLD / 252) - MTF_LEV * turn * COST_RT)
        # CALLS: 1M ATM, notional-matched; premium at VRP-marked IV, revalue at window end
        prem_pnl = []
        for s in basket:
            sgm = float(rv20[s].iloc[i])
            if not (sgm > 0):
                continue
            iv = sgm * VRP_MARKUP
            c0 = _bs_call(1.0, 1.0, 21 / 252, iv)          # buy: 21 trading days to expiry
            sT = float(p1[s] / p0[s])
            c1 = _bs_call(sT, 1.0, max(21 - HOLD, 1) / 252, iv)
            prem_pnl.append(c1 - c0)                       # pnl per unit notional
        if prem_pnl:
            call_w.append(float(np.mean(prem_pnl)) - turn * COST_RT * 0.5)
        else:
            call_w.append(0.0)
        dates.append(str(C.index[i].date()))

    def stats(w):
        w = np.asarray(w, float)
        if len(w) < 8:
            return None
        eq = np.cumprod(1 + w)
        yrs = len(w) / WINDOWS_PER_YR
        cagr = (eq[-1] ** (1 / yrs) - 1) * 100 if eq[-1] > 0 else -100.0
        sh = w.mean() / (w.std(ddof=1) + 1e-12) * math.sqrt(WINDOWS_PER_YR)
        peak = np.maximum.accumulate(eq)
        mdd = float(((eq - peak) / peak).min() * 100)
        return {"cagr_pct": round(cagr, 1), "sharpe": round(float(sh), 2),
                "win_pct": round(float((w > 0).mean() * 100), 1), "maxdd_pct": round(mdd, 1),
                "n_windows": len(w), "avg_window_pct": round(float(w.mean() * 100), 2)}

    out = {"signal": f"ud>1.3 top-{TOPN}, {HOLD}d windows", "from": dates[0] if dates else None,
           "to": dates[-1] if dates else None,
           "cash": stats(cash_w), "mtf_2x": stats(mtf_w), "atm_calls": stats(call_w),
           "note": ("Net: delivery costs on turnover; MTF charges 16%/yr on the borrowed half; "
                    "calls priced at IV=1.15x realized (the VRP an option BUYER pays). "
                    "Same signal, same windows — only the expression differs.")}
    json.dump(out, open(os.path.join(HERE, "marleg_expression_bt.json"), "w"), indent=1)
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = run()
    print(f"\nEXPRESSION BACKTEST — volume longs ({r['signal']})  {r['from']} -> {r['to']}\n")
    print(f"{'expression':<12}{'net CAGR%':>10}{'Sharpe':>8}{'win%':>7}{'maxDD%':>8}{'avg/window':>11}")
    print("-" * 58)
    for k, lbl in [("cash", "CASH"), ("mtf_2x", "MTF 2x"), ("atm_calls", "ATM CALLS")]:
        s = r[k]
        if not s:
            print(f"{lbl:<12}  insufficient data"); continue
        print(f"{lbl:<12}{s['cagr_pct']:>10}{s['sharpe']:>8}{s['win_pct']:>7}{s['maxdd_pct']:>8}{s['avg_window_pct']:>10}%")
    print("\n" + r["note"])


if __name__ == "__main__":
    main()
