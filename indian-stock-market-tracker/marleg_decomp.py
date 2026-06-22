"""
marleg_decomp.py — was the move the STOCK, or just the MARKET?

Single-stock return attribution against NIFTY (the market), the honest market-model way:

    r_stock = alpha + beta * r_market + epsilon          (OLS over a trailing window)

    beta   = market sensitivity            (cov / var)
    R^2    = how much of the stock's variance the market explains  → "market-driven %"
    1-R^2  = the idiosyncratic fraction    → "its own story %"

For ANY single day we split the move into the part the index dragged and the residual:

    market_part = beta * r_market_today          (what the tape did to it)
    idio_part   = r_stock_today - market_part     (stock-specific: news, flows, its own bid)

Plus a LIVE rolling correlation to the market so you can see, right now, whether the name is
trading WITH the tape or off on its own. Read-only daily-close compute. Decision-support, not advice.

  python marleg_decomp.py RELIANCE
"""
import sys
import numpy as np
import pandas as pd

BENCH_DEFAULT = "NIFTY"


def _closes(tk, period="1y"):
    import marleg_vol as mv
    import yfinance as yf
    sym = mv._yf_symbol(tk.upper())
    h = yf.Ticker(sym).history(period=period)
    return h["Close"].dropna() if h is not None and len(h) else None


def decompose(tk, bench=BENCH_DEFAULT, window=90, roll=20, period="1y"):
    cs, cb = _closes(tk, period), _closes(bench, period)
    if cs is None or cb is None or len(cs) < 40 or len(cb) < 40:
        return {"ok": False, "error": f"no/short history for {tk} or {bench}"}
    df = pd.DataFrame({"s": cs, "m": cb}).dropna()
    rs, rm = df["s"].pct_change(), df["m"].pct_change()
    both = pd.DataFrame({"s": rs, "m": rm}).dropna()
    if len(both) < 40:
        return {"ok": False, "error": "not enough overlapping history"}
    rs, rm = both["s"], both["m"]

    # ---- market model over the trailing window ----
    w = int(min(window, len(rs)))
    xs, xm = rs.iloc[-w:].values, rm.iloc[-w:].values
    var_m = float(xm.var(ddof=1))
    beta = float(np.cov(xs, xm, ddof=1)[0, 1] / var_m) if var_m > 0 else 0.0
    alpha = float(xs.mean() - beta * xm.mean())
    corr = float(np.corrcoef(xs, xm)[0, 1]) if xs.std() > 0 and xm.std() > 0 else 0.0
    r2 = corr * corr                                   # single factor → R^2 = corr^2
    resid = xs - (alpha + beta * xm)
    idio_vol = float(resid.std(ddof=1) * np.sqrt(252) * 100)
    tot_vol = float(xs.std(ddof=1) * np.sqrt(252) * 100)

    # ---- today's move, attributed ----
    rs_t, rm_t = float(rs.iloc[-1]), float(rm.iloc[-1])
    mkt_part = beta * rm_t
    idio_part = rs_t - mkt_part
    denom = abs(mkt_part) + abs(idio_part)
    idio_share = float(abs(idio_part) / denom) if denom > 1e-9 else 0.5

    # ---- live rolling correlation to the market ----
    rc = rs.rolling(roll).corr(rm).dropna()
    roll_corr = [{"time": str(t.date()), "value": round(float(v), 3)} for t, v in rc.items()][-160:]
    corr_now = float(rc.iloc[-1]) if len(rc) else corr

    driven = ("mostly MARKET-driven — it mostly does what the index does" if r2 >= 0.50
              else "mostly its OWN story — the market barely explains it" if r2 < 0.25
              else "a MIX — part market, part its own")
    if denom < 1e-9:
        today_v = "flat today — nothing to attribute"
    elif idio_share >= 0.60:
        today_v = ("today was STOCK-SPECIFIC: {:.0f}% of the move was its own "
                   "(residual {:+.2f}% vs market pull {:+.2f}%)").format(idio_share * 100, idio_part * 100, mkt_part * 100)
    elif idio_share <= 0.40:
        today_v = ("today it just FOLLOWED THE TAPE: {:.0f}% of the move was the index "
                   "(market {:+.2f}% vs own {:+.2f}%)").format((1 - idio_share) * 100, mkt_part * 100, idio_part * 100)
    else:
        today_v = "today was a MIX: market {:+.2f}% + its own {:+.2f}%".format(mkt_part * 100, idio_part * 100)

    return {"ok": True, "tk": tk.upper(), "bench": bench.upper(), "window": w, "roll": roll,
            "beta": round(beta, 2), "alpha_ann_pct": round(alpha * 252 * 100, 1), "corr": round(corr, 2),
            "r2": round(r2, 3), "market_driven_pct": round(r2 * 100), "idio_pct": round((1 - r2) * 100),
            "idio_vol_ann": round(idio_vol, 1), "tot_vol_ann": round(tot_vol, 1),
            "today": {"stock_ret_pct": round(rs_t * 100, 2), "mkt_ret_pct": round(rm_t * 100, 2),
                      "market_part_pct": round(mkt_part * 100, 2), "idio_part_pct": round(idio_part * 100, 2),
                      "idio_share_pct": round(idio_share * 100)},
            "corr_now": round(corr_now, 2), "roll_corr": roll_corr,
            "driven": driven, "today_verdict": today_v,
            "note": ("Market model r_stock = alpha + beta*r_{} + e over {}d. R^2 = market-driven %, 1-R^2 = "
                     "idiosyncratic. Today's split: market = beta x index move, residual = stock-specific. "
                     "Daily returns; decision-support, not advice.").format(bench.upper(), w)}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    import json
    tk = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    bench = sys.argv[2] if len(sys.argv) > 2 else BENCH_DEFAULT
    r = decompose(tk, bench)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n{r['tk']}  vs  {r['bench']}   (window {r['window']}d)")
    print(f"  beta {r['beta']}   ·   corr {r['corr']}   ·   R^2 {r['r2']}  ->  {r['market_driven_pct']}% market-driven / {r['idio_pct']}% its own")
    print(f"  alpha {r['alpha_ann_pct']}%/yr   ·   total vol {r['tot_vol_ann']}%   ·   idiosyncratic vol {r['idio_vol_ann']}%")
    print(f"  DRIVEN BY: {r['driven']}")
    t = r["today"]
    print(f"\n  TODAY: stock {t['stock_ret_pct']:+}%  =  market {t['market_part_pct']:+}%  +  own {t['idio_part_pct']:+}%   ({t['idio_share_pct']}% idiosyncratic)")
    print(f"  -> {r['today_verdict']}")
    print(f"  live {r['roll']}d correlation to {r['bench']}: {r['corr_now']}")
    print(f"\n  {r['note']}")
