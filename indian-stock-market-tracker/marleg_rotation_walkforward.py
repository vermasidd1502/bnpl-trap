"""
marleg_rotation_walkforward.py — WALK-FORWARD industry rotation: what would the pod have picked, point-in-time,
and what was the real P&L? No look-ahead in the SIGNAL (ranking uses only trailing data; returns realized
forward). Logs the actual industries + stocks picked and how much they rose.

Method (every HOLD days):
  1. rank granular industries by trailing-63d momentum using ONLY data up to that day,
  2. pick the top-K, hold for HOLD days (≈ the ~3-week leadership duration we measured),
  3. (bull-gate) sit in cash when the market is below its 50DMA,
  4. record the picks, each industry's realized hold-return, and the top constituent stocks.

HONEST CAVEATS (stated, not buried):
  • No look-ahead in the signal — but the UNIVERSE is today's survivors (survivorship bias) → the ABSOLUTE
    P&L is OPTIMISTIC (names that delisted/blew up are missing). The RELATIVE result (vs buy-and-hold the same
    universe) is the fair read.
  • Costs: a flat {COST}% per rebalance is subtracted. Slippage/impact not modelled. Equal-weight, idealized.

  python marleg_rotation_walkforward.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

import marleg_panel_build as pb

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_rotation_walkforward.json")
COST = 0.20
HOLD = 15
K = 3
LOOK = 63


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    panel = pb.load()
    if not panel:
        print("no panel"); return
    close = panel["close"].copy()
    if getattr(close.index, "tz", None) is not None:
        close.index = close.index.tz_localize(None)
    rets = close.pct_change()
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
    byind = {}
    for s in close.columns:
        ind = SECT.get(s, {}).get("industry")
        if ind:
            byind.setdefault(ind, []).append(s)
    byind = {k: v for k, v in byind.items() if len(v) >= 4}
    indret = pd.DataFrame({k: rets[v].mean(axis=1) for k, v in byind.items()}).dropna(how="all")
    idx = indret.index
    mkt_eq = (1 + indret.mean(axis=1)).cumprod()
    dma = mkt_eq.rolling(50).mean()

    def run(bull_gate):
        equity = 1.0
        log, contrib, yearly = [], {}, {}
        for t in range(LOOK, len(idx) - HOLD, HOLD):
            mom = (1 + indret.iloc[t - LOOK:t]).prod() - 1          # trailing momentum (past only)
            picks = list(mom.sort_values(ascending=False).index[:K])
            gated = bull_gate and not (mkt_eq.iloc[t] > dma.iloc[t] if dma.iloc[t] == dma.iloc[t] else True)
            date = str(idx[t].date())
            if gated:
                pr = 0.0
                entry = {"date": date, "cash": True, "ret": 0.0, "picks": []}
            else:
                rows, irs = [], []
                for ind in picks:
                    ir = float((1 + indret[ind].iloc[t:t + HOLD]).prod() - 1)
                    irs.append(ir)
                    # top constituent stocks of this industry over the hold
                    sh = {s: float((1 + rets[s].iloc[t:t + HOLD]).prod() - 1) for s in byind[ind]
                          if rets[s].iloc[t:t + HOLD].notna().any()}
                    for s, v in sh.items():
                        contrib[s] = contrib.get(s, 0.0) + v
                    top = sorted(sh.items(), key=lambda x: x[1], reverse=True)[:2]
                    rows.append({"industry": ind, "ret": round(ir * 100, 1),
                                 "stocks": [{"s": s, "n": NAMES.get(s, s), "ret": round(v * 100, 1)} for s, v in top]})
                pr = float(np.mean(irs)) - COST / 100
                entry = {"date": date, "cash": False, "ret": round(pr * 100, 2), "picks": rows}
            equity *= (1 + pr)
            yearly[date[:4]] = yearly.get(date[:4], 1.0) * (1 + pr)
            entry["equity"] = round(equity, 3)
            log.append(entry)
        return equity, log, contrib, yearly

    eq, log, contrib, yearly = run(bull_gate=True)
    eq_ng, _, _, _ = run(bull_gate=False)
    # buy-and-hold the same universe over the same span
    span = indret.iloc[LOOK:]
    bh = float((1 + span.mean(axis=1)).prod())
    yrs = len(span) / 252
    res = {"hold_days": HOLD, "top_k": K, "look": LOOK, "cost_per_reb": COST, "span_years": round(yrs, 1),
           "no_lookahead": "signal uses trailing data only; survivorship bias remains in the universe",
           "rotation_bullgated": {"total_return_pct": round((eq - 1) * 100, 1), "cagr_pct": round((eq ** (1 / yrs) - 1) * 100, 1)},
           "rotation_always_in": {"total_return_pct": round((eq_ng - 1) * 100, 1), "cagr_pct": round((eq_ng ** (1 / yrs) - 1) * 100, 1)},
           "buyhold_same_universe": {"total_return_pct": round((bh - 1) * 100, 1), "cagr_pct": round((bh ** (1 / yrs) - 1) * 100, 1)},
           "by_year": {y: round((v - 1) * 100, 1) for y, v in sorted(yearly.items())},
           "trade_log_recent": log[-10:],
           "top_stock_contributors": sorted(
               [{"s": s, "n": NAMES.get(s, s), "sum_ret_pct": round(v * 100, 1)} for s, v in contrib.items()],
               key=lambda x: x["sum_ret_pct"], reverse=True)[:15]}
    json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"WALK-FORWARD rotation — top-{K} momentum industries, {HOLD}d hold, {res['span_years']}y, no look-ahead in signal\n")
    print(f"  rotation (bull-gated):  total {res['rotation_bullgated']['total_return_pct']}%   CAGR {res['rotation_bullgated']['cagr_pct']}%")
    print(f"  rotation (always-in):   total {res['rotation_always_in']['total_return_pct']}%   CAGR {res['rotation_always_in']['cagr_pct']}%")
    print(f"  buy & hold same univ:   total {res['buyhold_same_universe']['total_return_pct']}%   CAGR {res['buyhold_same_universe']['cagr_pct']}%")
    print(f"\n  by year: " + "  ".join(f"{y} {v:+}%" for y, v in res["by_year"].items()))
    print(f"\n  recent picks (point-in-time) + realized {HOLD}d returns:")
    for e in log[-6:]:
        if e["cash"]:
            print(f"   {e['date']}  CASH (bear gate)")
        else:
            pk = " | ".join(f"{p['industry'][:18]} {p['ret']:+}% (top {p['stocks'][0]['s']} {p['stocks'][0]['ret']:+}%)" for p in e["picks"])
            print(f"   {e['date']}  {e['ret']:+}%:  {pk}")
    print(f"\n  biggest stock contributors over the years (sum of hold returns while picked):")
    for c in res["top_stock_contributors"][:8]:
        print(f"   {c['s']:<12} {str(c['n'])[:30]:<32} {c['sum_ret_pct']:+}%")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
