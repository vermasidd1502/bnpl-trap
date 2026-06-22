"""
marleg_factor_screen.py — the LIVE, actionable payoff of the factor research (marleg_factors /
marleg_portfolio_bt): today's TOP LONG picks by the factors that actually backtested (low-vol + strength
near the 52-week high + liquidity), the regime-robust long edge (Sharpe ~1.0 in BOTH bull and bear).

This is the constructive flip of "short the overbought" — the data says India's edge is on the LONG side:
own low-vol, strong, liquid names; don't short. F&O names are flagged (you can express them with calls).
Read-only, decision-support — not advice.

  python marleg_factor_screen.py
"""
import sys
import os
import json
import numpy as np
import pandas as pd


def screen(top=25):
    import marleg_panel_build as pb
    import marleg_options_monitor as mom
    HERE = os.path.dirname(os.path.abspath(__file__))
    try:
        NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
        SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    except Exception:
        NAMES, SECT = {}, {}
    P = pb.load()
    C, V = P["close"], P["volume"]
    ret = C.pct_change(fill_method=None)
    lowvol = -(ret.rolling(20).std() * np.sqrt(252)).iloc[-1]
    hi52 = C.rolling(252, min_periods=60).max().iloc[-1]
    dist52 = (C.iloc[-1] / hi52 - 1)
    liq = np.log((C * V).rolling(20).mean().iloc[-1].replace(0, np.nan))
    rsi = (lambda c, n=14: (lambda d: 100 - 100 / (1 + d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean() /
           (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean().replace(0, np.nan)))(c.diff()))(C).iloc[-1]
    df = pd.DataFrame({"lowvol": lowvol, "dist52": dist52, "liq": liq, "rsi": rsi, "px": C.iloc[-1]}).dropna()
    # drop cash-like funds / ETFs (near-zero vol, monotonic) — they game the low-vol factor but aren't stocks
    df = df[df["lowvol"] <= -0.05]                      # realized vol >= ~5% annualised
    df = df[~df.index.to_series().str.contains("BEES|ETF|LIQUID|GOLD|SILVER|GSEC|GILT|CASH|NIFTY", case=False, regex=True)]
    for col in ("lowvol", "dist52", "liq"):
        df[col + "_z"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)
    df["score"] = df["lowvol_z"] + df["dist52_z"] + df["liq_z"]
    df = df.sort_values("score", ascending=False).head(top)
    fno = set(mom.FNO_UNDERLYINGS) | set(mom.INDEX_STEP)
    rows = []
    for s, r in df.iterrows():
        rows.append({"s": s, "n": NAMES.get(s, s), "fno": bool(s in fno),
                     "sector": (SECT.get(s) or {}).get("sector"), "price": round(float(r["px"]), 2),
                     "vs_52w_high_pct": round(float(r["dist52"]) * 100, 1), "rsi": round(float(r["rsi"]), 0),
                     "ann_vol_pct": round(float(-r["lowvol"]) * 100, 1), "score": round(float(r["score"]), 2)})
    return {"ok": True, "n": len(rows), "names": rows,
            "note": "Top names by the backtested LONG composite: low realized vol + strength near the 52-week high + "
                    "liquidity (Sharpe ~1.0, robust in bull AND bear). F&O names = expressible with calls. NOT a "
                    "short list — India's edge is long. Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = screen()
    print(f"\n  WINNING-FACTOR LONG SCREEN ({r['n']}) — low-vol + strength + liquidity")
    print(f"  {'name':<12}{'score':>6}{'vs52wH':>8}{'annVol':>8}{'RSI':>5}  sector")
    for x in r["names"]:
        print(f"  {x['s']:<12}{x['score']:>6.2f}{x['vs_52w_high_pct']:>7.1f}%{x['ann_vol_pct']:>7.1f}%{x['rsi']:>5.0f}  [{'F&O' if x['fno'] else 'cash'}] {str(x['sector'] or '')[:22]}")
    print(f"\n  {r['note']}")
