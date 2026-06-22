"""
marleg_factors.py — cross-sectional FACTOR DISCOVERY on the Indian universe (which signals actually predict
forward returns, and on what horizon).

At monthly rebalance dates over the panel, each factor is computed cross-sectionally (a value per stock),
stocks are ranked, and we measure:
  • RANK IC  — Spearman corr(factor rank, forward return). Sign = direction; |IC| = strength.
  • IC t-stat — IC mean / IC std × √n_rebalances. |t|>~2 = a stable, real relationship.
  • IC hit%  — fraction of rebalances the IC kept its sign (consistency).
  • L-S spread — mean (top-quintile − bottom-quintile) forward return = the tradeable long-short edge.
Run for a SHORT (5d) and a MEDIUM (21d) horizon, because the same factor can flip sign with horizon —
e.g. high RSI / over-extension MEAN-REVERTS short-term (a short edge) but momentum PERSISTS medium-term.

This is how you decide "short the most overbought" sensibly: only if RSI / over-extension carry a real
NEGATIVE IC at the horizon you'll hold. Pure compute on the canonical panel; survivorship-aware caveat below.
  python marleg_factors.py
"""
import sys
import numpy as np
import pandas as pd


def _rsi(close, n=14):
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def _factor_panels():
    import marleg_panel_build as pb
    P = pb.load()
    C, V = P["close"], P["volume"]
    ret = C.pct_change(fill_method=None)
    sma50, sma200 = C.rolling(50).mean(), C.rolling(200).mean()
    hi52 = C.rolling(252, min_periods=60).max()
    factors = {
        "momentum_3m": C / C.shift(63) - 1,
        "momentum_6m": C / C.shift(126) - 1,
        "reversal_5d": C / C.shift(5) - 1,                 # high = recent winner (test if it reverses)
        "rsi_14": _rsi(C),                                 # high = overbought
        "dist_50dma": C / sma50 - 1,                       # over-extension above the 50DMA
        "dist_200dma": C / sma200 - 1,
        "dist_52w_high": C / hi52 - 1,                     # 0 = at the high (most extended)
        "low_vol": -(ret.rolling(20).std() * np.sqrt(252)),  # negated → high = LOW vol (low-vol anomaly)
        "liquidity": np.log((C * V).rolling(20).mean().replace(0, np.nan)),
    }
    return C, factors


def discover(horizons=(5, 21), step=21, window=520, q=5):
    C, factors = _factor_panels()
    n = len(C.index)
    start = max(252, n - window)
    out = {}
    for h in horizons:
        fwd = C.shift(-h) / C - 1
        rb = list(range(start, n - h, step))
        rows = []
        for name, fp in factors.items():
            ics, ls = [], []
            for i in rb:
                df = pd.concat([fp.iloc[i], fwd.iloc[i]], axis=1).dropna()
                if len(df) < 50:
                    continue
                df.columns = ["f", "r"]
                ic = df["f"].corr(df["r"], method="spearman")
                if pd.notna(ic):
                    ics.append(float(ic))
                try:
                    qq = pd.qcut(df["f"], q, labels=False, duplicates="drop")
                    top, bot = df["r"][qq == qq.max()].mean(), df["r"][qq == 0].mean()
                    if pd.notna(top) and pd.notna(bot):
                        ls.append(float(top - bot))
                except Exception:
                    pass
            if not ics:
                continue
            a = np.array(ics)
            t = float(a.mean() / (a.std() + 1e-9) * np.sqrt(len(a)))
            lsm = float(np.mean(ls)) * 100 if ls else 0.0
            rows.append({"factor": name, "ic": round(float(a.mean()), 4), "ic_t": round(t, 2),
                         "ic_hit": round(float((a > 0).mean()) * 100, 1), "ls_spread_pct": round(lsm, 2),
                         "n_rebal": len(ics),
                         "read": ("predicts UP (long high)" if a.mean() > 0 else "predicts DOWN (SHORT high / long low)")})
        rows.sort(key=lambda x: -abs(x["ic_t"]))
        out[str(h)] = rows
    return {"ok": True, "horizons": list(horizons), "rebalance_step": step, "factors": out,
            "note": "Rank IC = Spearman(factor, forward return) at monthly rebalances; sign = direction, "
                    "|t|>2 ≈ real & stable. A NEGATIVE IC on rsi_14 / dist_50dma means over-extension MEAN-REVERTS "
                    "(shorting the most-overbought has an edge at that horizon). Survivorship caveat: the panel is "
                    "current constituents, so long-leg returns are mildly optimistic — the SHORT/spread read is the "
                    "more robust takeaway. Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = discover()
    for h in r["horizons"]:
        print(f"\n  ═══ FORWARD HORIZON {h}d ═══   (IC = rank corr with fwd return; |t|>2 ≈ real)")
        print(f"  {'factor':<16}{'IC':>8}{'IC t':>7}{'hit%':>7}{'L-S %':>8}   read")
        for x in r["factors"][str(h)]:
            print(f"  {x['factor']:<16}{x['ic']:>8.4f}{x['ic_t']:>7.2f}{x['ic_hit']:>6.1f}%{x['ls_spread_pct']:>7.2f}   {x['read']}")
    print(f"\n  {r['note']}")
