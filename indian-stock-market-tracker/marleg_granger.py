"""
Marle-G — GRANGER PREDICTABILITY TEST for the volume signal.

The question backtests can't answer: does ud(20d up/down volume) LEAD returns, or does
it merely follow them? A profitable backtest on known history can be design hindsight;
Granger tests temporal PRECEDENCE: do lags of the signal improve the prediction of
returns beyond what returns' own lags already give?

Both directions are tested — if returns->ud is significant but ud->returns is not, the
"signal" is reactive (volume chases price) and any backtest profit is suspect.

Honesty notes: Granger = predictive precedence, NOT economic causality; linear VAR form;
p-values across 34 stocks need Bonferroni; predictability != profitability (costs) and
vice versa. ud is log-transformed (heavy right skew).

  python marleg_granger.py
"""
import sys
import numpy as np
import pandas as pd
from scipy import stats
import marleg_datastore as ds

LAGS_D = 5      # daily test
LAGS_B = 4      # 5-day block test


def _granger_pair(y, x, p):
    """F-test: do lags 1..p of x help predict y beyond lags of y? -> (F, pval, n)"""
    d = pd.DataFrame({"y": y, "x": x})
    for i in range(1, p + 1):
        d[f"yl{i}"] = d["y"].shift(i)
        d[f"xl{i}"] = d["x"].shift(i)
    d = d.dropna()
    if len(d) < 40 * p:
        return None
    Y = d["y"].values
    Xr = np.column_stack([np.ones(len(d))] + [d[f"yl{i}"].values for i in range(1, p + 1)])
    Xu = np.column_stack([Xr] + [d[f"xl{i}"].values for i in range(1, p + 1)])

    def ssr(X):
        beta, *_ = np.linalg.lstsq(X, Y, rcond=None)
        e = Y - X @ beta
        return float(e @ e)

    ssr_r, ssr_u = ssr(Xr), ssr(Xu)
    n, k, q = len(Y), Xu.shape[1], p
    if ssr_u <= 0 or n <= k:
        return None
    F = ((ssr_r - ssr_u) / q) / (ssr_u / (n - k))
    return F, float(stats.f.sf(F, q, n - k)), n


def _blocks(r, s, width=5):
    g = np.arange(len(r)) // width
    rb = r.groupby(g).sum()
    sb = s.groupby(g).last()
    return rb, sb


def run():
    ds.sync(verbose=False)
    C = ds.panel("close").ffill()
    V = ds.panel("volume").reindex(C.index)
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = (upv / dnv).clip(lower=0.05, upper=20.0)
    lud = np.log(ud)

    out = {}
    for label, p, blocky in [("daily", LAGS_D, False), ("5d-blocks", LAGS_B, True)]:
        fwd, rev = [], []
        pooled_y, pooled_xr, pooled_xu = [], [], []
        for sym in C.columns:
            r, s = rc[sym].dropna(), lud[sym].dropna()
            both = pd.concat([r, s], axis=1, keys=["r", "s"]).dropna()
            if len(both) < 300:
                continue
            r, s = both["r"], both["s"]
            if blocky:
                r, s = _blocks(r, s)
            a = _granger_pair(r, s, p)          # signal -> returns (the claim)
            b = _granger_pair(s, r, p)          # returns -> signal (the reactivity check)
            if a:
                fwd.append((sym, a[1]))
            if b:
                rev.append((sym, b[1]))
        nf = len(fwd)
        bon = 0.05 / max(nf, 1)
        out[label] = {
            "n_stocks": nf,
            "fwd_sig_5pct": sum(1 for _, pv in fwd if pv < 0.05),
            "fwd_sig_bonferroni": sum(1 for _, pv in fwd if pv < bon),
            "fwd_median_p": round(float(np.median([pv for _, pv in fwd])), 4) if fwd else None,
            "rev_sig_5pct": sum(1 for _, pv in rev if pv < 0.05),
            "rev_sig_bonferroni": sum(1 for _, pv in rev if pv < bon),
            "rev_median_p": round(float(np.median([pv for _, pv in rev])), 4) if rev else None,
            "top_predictable": sorted(fwd, key=lambda t: t[1])[:5],
        }
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = run()
    print("\nGRANGER PREDICTABILITY — ud(20d volume) vs returns, both directions\n")
    for label, o in r.items():
        print(f"[{label}]  {o['n_stocks']} stocks, lags tested")
        print(f"  ud -> RETURNS : {o['fwd_sig_5pct']}/{o['n_stocks']} sig at 5% · "
              f"{o['fwd_sig_bonferroni']} after Bonferroni · median p {o['fwd_median_p']}")
        print(f"  returns -> UD : {o['rev_sig_5pct']}/{o['n_stocks']} sig at 5% · "
              f"{o['rev_sig_bonferroni']} after Bonferroni · median p {o['rev_median_p']}")
        print("  most predictable:", ", ".join(f"{s}(p={pv:.3f})" for s, pv in o["top_predictable"]))
        print()
    print("Read: if returns->ud dominates, volume CHASES price (reactive signal); "
          "if ud->returns holds after Bonferroni, the signal genuinely leads.")


if __name__ == "__main__":
    main()
