"""
Marle-G — GATE x VOLUME BACKTEST with a time-horizon grid.

Question: does the pre-trade gate improve the volume-buying signal, and WHAT HOLDING
HORIZON should the live bot use?

Design (no lookahead):
  signal day i (close)  ->  enter next day's OPEN
  VOLUME-ONLY entry : ud(20d) > 1.3                       (the volume pod's buy logic)
  VOLUME+GATE entry : ud > 1.3  AND  px > 50d & 200d MA   (the gate's hard trend filter)
  sizing (both)     : risk 1% of capital per trade, stop = 2.5*ATR(14) at entry
                      qty = risk / stop-distance, notional capped at 15% of capital
  exits             : STOP  — daily close below entry-stop (close-basis, like the guardian)
                      TIME  — calendar exit after N trading days  (N = 5/10/15/21/30/none)
  costs             : 33 bps round-trip on notional. Max 8 concurrent names, 1 per symbol.

Smart-money / Kelly / jump gates are LIVE-ONLY overlays (quarterly data, leverage policy);
this backtest isolates what is cleanly testable: trend gate + vol-aware sizing + horizon.

  python marleg_gate_backtest.py
"""
import os, sys, json, math
import numpy as np
import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
CAP = 100000.0
RISK = 0.01
K_ATR = 2.5
COST = 33 / 1e4
MAXPOS = 8
NOTIONAL_CAP = 0.15
UNIV = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "ITC", "LT", "SBIN", "AXISBANK",
        "KOTAKBANK", "BHARTIARTL", "BAJFINANCE", "HINDUNILVR", "MARUTI", "SUNPHARMA",
        "EICHERMOT", "TATASTEEL", "M&M", "NTPC", "TITAN", "ASIANPAINT", "ULTRACEMCO",
        "WIPRO", "ADANIPORTS", "JSWSTEEL", "COALINDIA", "ONGC", "GRASIM", "HCLTECH", "CIPLA",
        "POWERGRID", "BAJAJFINSV", "TECHM", "NESTLEIND"]


def load(period="3y"):
    df = yf.download([s + ".NS" for s in UNIV], period=period, interval="1d",
                     group_by="ticker", auto_adjust=False, progress=False, threads=True)
    O, C, H, L, V = {}, {}, {}, {}, {}
    for s in UNIV:
        try:
            d = df[s + ".NS"].dropna()
            if len(d) > 300:
                O[s], C[s], H[s], L[s], V[s] = d["Open"], d["Close"], d["High"], d["Low"], d["Volume"]
        except Exception:
            pass
    return (pd.DataFrame(O), pd.DataFrame(C), pd.DataFrame(H), pd.DataFrame(L), pd.DataFrame(V))


def precompute(O, C, H, L, V):
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    sma50 = C.rolling(50).mean()
    sma200 = C.rolling(200).mean()
    pc = C.shift(1)
    tr = pd.concat([(H - L).stack(), (H - pc).abs().stack(), (L - pc).abs().stack()], axis=1).max(axis=1).unstack()
    atr = tr.rolling(14).mean()
    return ud, sma50, sma200, atr


def simulate(O, C, ud, sma50, sma200, atr, horizon, gated, daily_top=None):
    """Daily event loop. horizon=None -> stop-only (60d hard cap). daily_top=N -> only the
    N strongest-ud candidates per day (selectivity). Returns summary stats."""
    n = len(C.index)
    cash = CAP
    open_pos = {}                                  # sym -> dict
    closed = []
    equity = []
    for i in range(210, n - 1):
        # ---- manage exits on today's close (day i) ----
        for sym in list(open_pos):
            p = open_pos[sym]
            px = C[sym].iloc[i]
            if not np.isfinite(px):
                continue
            days_held = i - p["i0"]
            reason = None
            if px <= p["stop"]:
                reason = "stop"
            elif horizon is not None and days_held >= horizon:
                reason = "time"
            elif days_held >= 60:
                reason = "maxhold"
            if reason:
                pnl = (px - p["entry"]) * p["qty"] - COST * p["entry"] * p["qty"]
                cash += p["qty"] * px - COST * p["entry"] * p["qty"] * 0   # cost charged at entry
                closed.append({"sym": sym, "pnl": pnl, "days": days_held, "reason": reason,
                               "ret_pct": (px / p["entry"] - 1) * 100})
                del open_pos[sym]
        # ---- entries decided on close i, filled at open i+1 ----
        if len(open_pos) < MAXPOS:
            sig = ud.iloc[i].dropna()
            sig = sig[sig > 1.3]
            cands = sig.sort_values(ascending=False).index
            if daily_top:
                cands = cands[:daily_top]
            for sym in cands:
                if len(open_pos) >= MAXPOS:
                    break
                if sym in open_pos:
                    continue
                px_c = C[sym].iloc[i]
                s50, s200, a = sma50[sym].iloc[i], sma200[sym].iloc[i], atr[sym].iloc[i]
                if not all(np.isfinite(x) for x in (px_c, s50, s200, a)) or a <= 0:
                    continue
                if gated and not (px_c > s50 and px_c > s200):
                    continue
                entry = O[sym].iloc[i + 1]
                if not np.isfinite(entry) or entry <= 0:
                    continue
                stop_dist = K_ATR * a
                qty = int((CAP * RISK) // stop_dist)
                if qty < 1:
                    continue
                notional = qty * entry
                if notional > CAP * NOTIONAL_CAP:
                    qty = int(CAP * NOTIONAL_CAP // entry)
                    notional = qty * entry
                if qty < 1 or notional > cash:
                    continue
                cash -= notional + COST * notional          # full round-trip cost at entry
                open_pos[sym] = {"entry": float(entry), "qty": qty,
                                 "stop": float(entry - stop_dist), "i0": i + 1}
        # ---- mark ----
        mtm = sum(p["qty"] * (C[s].iloc[i] if np.isfinite(C[s].iloc[i]) else p["entry"])
                  for s, p in open_pos.items())
        equity.append(cash + mtm)
    eq = np.array(equity)
    r = np.diff(eq) / eq[:-1]
    yrs = len(r) / 252
    out = {
        "horizon": horizon if horizon is not None else "stop-only",
        "trades": len(closed),
        "win_pct": round(float(np.mean([1 if t["pnl"] > 0 else 0 for t in closed]) * 100), 1) if closed else 0,
        "avg_days": round(float(np.mean([t["days"] for t in closed])), 1) if closed else 0,
        "stop_exits_pct": round(100 * sum(1 for t in closed if t["reason"] == "stop") / max(len(closed), 1)),
        "net_ret_pct": round((eq[-1] / CAP - 1) * 100, 1),
        "cagr_pct": round(((eq[-1] / CAP) ** (1 / yrs) - 1) * 100, 1) if yrs > 0 else 0,
        "sharpe": round(float(r.mean() / (r.std(ddof=1) + 1e-12) * math.sqrt(252)), 2),
        "maxdd_pct": round(float(((eq - np.maximum.accumulate(eq)) / np.maximum.accumulate(eq)).min() * 100), 1),
        "pf": round(sum(t["pnl"] for t in closed if t["pnl"] > 0) /
                    max(1e-9, -sum(t["pnl"] for t in closed if t["pnl"] < 0)), 2) if closed else 0,
    }
    return out


def run():
    O, C, H, L, V = load()
    ud, sma50, sma200, atr = precompute(O, C, H, L, V)
    grid = [5, 10, 15, 21, 30, None]
    res = {"volume_only": [], "volume_gate": [], "gate_top2": []}
    for hz in grid:
        res["volume_only"].append(simulate(O, C, ud, sma50, sma200, atr, hz, gated=False))
        res["volume_gate"].append(simulate(O, C, ud, sma50, sma200, atr, hz, gated=True))
        res["gate_top2"].append(simulate(O, C, ud, sma50, sma200, atr, hz, gated=True, daily_top=2))
    best = max(res["volume_gate"] + res["gate_top2"], key=lambda x: x["sharpe"])
    out = {"from": str(C.index[210].date()), "to": str(C.index[-1].date()),
           "universe": C.shape[1], "capital": CAP, "risk_per_trade_pct": RISK * 100,
           "k_atr": K_ATR, "max_pos": MAXPOS, "results": res,
           "best_gate_horizon": best["horizon"], "best_gate_sharpe": best["sharpe"],
           "note": ("Signal at close -> next-open entry; stop on closes (close-basis like the "
                    "guardian); 33bps RT; max 8 names; 1%-risk ATR sizing both variants — the "
                    "ONLY difference between panels is the gate's trend filter. Smart-money/"
                    "Kelly/jump gates are live-only overlays. Single-path, gross-of-slippage.")}
    json.dump(out, open(os.path.join(HERE, "marleg_gate_backtest.json"), "w"), indent=1)
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = run()
    print(f"\nGATE x VOLUME BACKTEST — {r['from']} -> {r['to']} · {r['universe']} names · "
          f"₹{int(r['capital']):,} · {r['risk_per_trade_pct']}% risk/trade · stop {r['k_atr']}xATR\n")
    hdr = f"{'horizon':<10}{'trades':>7}{'win%':>6}{'avgD':>6}{'stop%':>6}{'net%':>7}{'CAGR%':>7}{'Sharpe':>8}{'maxDD':>7}{'PF':>6}"
    for panel, lbl in [("volume_only", "VOLUME ONLY (ud>1.3)"), ("volume_gate", "VOLUME + GATE (ud>1.3 & >50d & >200d)"),
                       ("gate_top2", "GATE + SELECTIVE (top-2 strongest ud per day)")]:
        print(lbl); print(hdr); print("-" * len(hdr))
        for x in r["results"][panel]:
            print(f"{str(x['horizon']):<10}{x['trades']:>7}{x['win_pct']:>6}{x['avg_days']:>6}"
                  f"{x['stop_exits_pct']:>6}{x['net_ret_pct']:>7}{x['cagr_pct']:>7}{x['sharpe']:>8}{x['maxdd_pct']:>7}{x['pf']:>6}")
        print()
    print(f"BEST (gate panel): horizon = {r['best_gate_horizon']} (Sharpe {r['best_gate_sharpe']})")
    print(r["note"])


if __name__ == "__main__":
    main()
