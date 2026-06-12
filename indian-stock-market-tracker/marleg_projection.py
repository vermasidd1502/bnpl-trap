"""
Marle-G — accounting layer + honest multi-horizon price PROJECTION.

Two layers:
  1. ACCOUNTING: latest financial statements + trends (revenue / profit / margins /
     ROE / debt / FCF, with multi-year CAGRs) -> a "performance picture".
  2. PROJECTION: what the price could be at 1m / 3m / 6m / 1y. Done honestly:
       - FUNDAMENTALS anchor the 1-year fair value (DCF + Graham + analyst target).
       - VOLATILITY sets a lognormal CONE that widens with sqrt(time).
     Short horizons are vol-dominated RANGES (not point forecasts); the 1y horizon
     carries the fundamental drift. Output per horizon: expected level + 68%/90% bands
     + implied return. This is a distribution, not a false-precision single number.

  python marleg_projection.py TITAN
"""
import sys, math
import yfinance as yf
import marleg_vol as mv

R, GT, DISC = mv.R_FREE, 0.045, 0.115        # terminal growth, DCF discount (match /api/fundamentals)


def _yf(tk):
    return "^NSEI" if tk.startswith("^") else tk + ".NS"


def statements(t):
    """Up to 4y of revenue / net income / operating margin + CAGRs (newest-first cols)."""
    try:
        fin = t.income_stmt
        if fin is None or fin.empty:
            return None

        def row(*names):
            for n in names:
                if n in fin.index:
                    return [float(x) if x == x else None for x in fin.loc[n].tolist()]
            return None
        rev = row("Total Revenue"); ni = row("Net Income", "Net Income Common Stockholders")
        op = row("Operating Income", "Operating Income Or Loss"); gp = row("Gross Profit")
        years = [str(c.year) for c in fin.columns]
        n = min(4, len(years))

        def cagr(series):
            if not series or len(series) < 2:
                return None
            new, old = series[0], series[min(len(series) - 1, 3)]
            yrs = min(len(series) - 1, 3)
            if not (new and old) or old <= 0 or yrs <= 0:
                return None
            return (new / old) ** (1 / yrs) - 1
        op_margin = [round(op[i] / rev[i] * 100, 1) if (op and rev and op[i] and rev[i]) else None for i in range(len(years))]
        net_margin = [round(ni[i] / rev[i] * 100, 1) if (ni and rev and ni[i] and rev[i]) else None for i in range(len(years))]
        return {
            "years": years[:n],
            "revenue": (rev or [])[:n], "net_income": (ni or [])[:n],
            "op_margin": op_margin[:n], "net_margin": net_margin[:n],
            "rev_cagr": round(cagr(rev) * 100, 1) if cagr(rev) is not None else None,
            "ni_cagr": round(cagr(ni) * 100, 1) if cagr(ni) is not None else None,
        }
    except Exception:
        return None


def fair_value(info, price):
    eps = info.get("trailingEps"); bvps = info.get("bookValue")
    shares = (info.get("marketCap") / price) if (info.get("marketCap") and price) else None
    graham = (22.5 * eps * bvps) ** 0.5 if (eps and bvps and eps > 0 and bvps > 0) else None
    dcf = None; fcf = info.get("freeCashflow")
    g = max(0.05 if (eps and eps > 0) else 0.0,
            min((info.get("earningsGrowth") or info.get("revenueGrowth") or 0.08), 0.15))
    if fcf and fcf > 0 and shares:
        f, pv = fcf, 0.0
        for yrn in range(1, 6):
            f *= (1 + g); pv += f / ((1 + DISC) ** yrn)
        pv += (f * (1 + GT) / (DISC - GT)) / ((1 + DISC) ** 5)
        dcf = (pv + (info.get("totalCash") or 0) - (info.get("totalDebt") or 0)) / shares
    intrinsic = [x for x in [graham, dcf] if x]
    fair = sum(intrinsic) / len(intrinsic) if intrinsic else None
    return {"graham": round(graham, 1) if graham else None,
            "dcf": round(dcf, 1) if dcf else None,
            "fair": round(fair, 1) if fair else None,
            "analyst_target": info.get("targetMeanPrice"),
            "analyst_low": info.get("targetLowPrice"), "analyst_high": info.get("targetHighPrice"),
            "n_analysts": info.get("numberOfAnalystOpinions"), "growth": round(g, 3)}


def project(tk):
    tk = tk.upper()
    t = yf.Ticker(_yf(tk)); info = t.info or {}
    S = info.get("currentPrice") or info.get("regularMarketPrice")
    if not S:
        return {"error": "no data for " + tk}
    fv = fair_value(info, S)
    # 1-year anchor: blend only SANE intrinsic estimates + a haircut analyst target.
    # Graham collapses for asset-light, high-ROCE compounders (book << earning power),
    # so include it only when it isn't absurdly below price; same sanity guard on DCF.
    used = []
    if fv["dcf"] and 0.4 * S < fv["dcf"] < 3 * S:
        used.append(("DCF", fv["dcf"]))
    if fv["graham"] and fv["graham"] > 0.6 * S:
        used.append(("Graham", fv["graham"]))
    if fv["analyst_target"]:
        used.append(("analyst x0.92", fv["analyst_target"] * 0.92))   # sell-side optimism haircut
    central_1y = sum(v for _, v in used) / len(used) if used else S
    exp_ret_1y = max(-0.5, min(0.75, central_1y / S - 1.0))           # cap to sane band
    fv["anchors_used"] = [u[0] for u in used] or ["none (drift=0)"]
    fv["graham_excluded"] = bool(fv["graham"] and fv["graham"] <= 0.6 * S)
    mu = exp_ret_1y                                                   # annual drift
    sigma = mv.realized_vol(tk, 60) or mv.realized_vol(tk, 20) or 0.30
    horizons = [("1 month", 1 / 12.0), ("3 months", 0.25), ("6 months", 0.5), ("1 year", 1.0)]
    rows = []
    for label, T in horizons:
        drift_log = (mu - 0.5 * sigma * sigma) * T
        sd = sigma * math.sqrt(T)
        mean = S * math.exp(mu * T)                                  # expected level
        band = lambda z: (S * math.exp(drift_log - z * sd), S * math.exp(drift_log + z * sd))
        lo68, hi68 = band(1.0); lo90, hi90 = band(1.645)
        rows.append({"label": label, "days": int(T * 365), "center": round(mean, 1),
                     "lo68": round(lo68, 1), "hi68": round(hi68, 1),
                     "lo90": round(lo90, 1), "hi90": round(hi90, 1),
                     "ret": round((mean / S - 1) * 100, 1),
                     "vol_share": round(sd / (abs(mu * T) + sd) * 100)})   # % of move that's noise
    return {"tk": tk, "name": info.get("shortName") or tk, "price": round(S, 2),
            "sigma": round(sigma, 4), "iv_vix": mv.india_vix(),
            **fv, "central_1y": round(central_1y, 1), "exp_return_1y": round(exp_ret_1y * 100, 1),
            "statements": statements(t), "horizons": rows}


def main():
    tk = sys.argv[1] if len(sys.argv) > 1 else "TITAN"
    r = project(tk)
    if r.get("error"):
        print(r["error"]); return
    print(f"\n{r['name']} ({tk})  spot Rs {r['price']}   realized vol {r['sigma']*100:.0f}%   India VIX {r.get('iv_vix')}")
    st = r.get("statements")
    if st:
        print("\nACCOUNTING (newest first):")
        print("  years      :", "  ".join(f"{y:>10}" for y in st["years"]))
        print("  revenue    :", "  ".join(f"{(v/1e7):>9.0f}Cr" if v else f"{'n/a':>10}" for v in st["revenue"]))
        print("  net income :", "  ".join(f"{(v/1e7):>9.0f}Cr" if v else f"{'n/a':>10}" for v in st["net_income"]))
        print("  op margin% :", "  ".join(f"{v:>10}" for v in st["op_margin"]))
        print(f"  3y CAGR    : revenue {st['rev_cagr']}%   net income {st['ni_cagr']}%")
    print(f"\nVALUATION: Graham Rs {r['graham']}{' (EXCLUDED - asset-light compounder)' if r.get('graham_excluded') else ''}  "
          f"DCF Rs {r['dcf']}  analyst Rs {r['analyst_target']} (Rs {r['analyst_low']}-{r['analyst_high']}, {r['n_analysts']} analysts)")
    print(f"  1-year anchor Rs {r['central_1y']} (from {', '.join(r['anchors_used'])})  ->  expected 1y return {r['exp_return_1y']:+.1f}%\n")
    print(f"PROJECTION (lognormal cone; expected level + 68% and 90% range):")
    print(f"  {'horizon':<10}{'expected':>10}{'ret%':>7}{'   68% range':>22}{'   90% range':>24}{'  noise%':>9}")
    for h in r["horizons"]:
        print(f"  {h['label']:<10}{h['center']:>10}{h['ret']:>+7.1f}   Rs {h['lo68']:>7}-{h['hi68']:<8}   "
              f"Rs {h['lo90']:>7}-{h['hi90']:<8}{h['vol_share']:>7}%")
    print("\nRead: short-horizon 'expected' ~ today (drift tiny); the RANGE is the real story (vol-driven).")
    print("Only the 1y carries a fundamental anchor. Projections are model output, not advice.")


if __name__ == "__main__":
    main()
