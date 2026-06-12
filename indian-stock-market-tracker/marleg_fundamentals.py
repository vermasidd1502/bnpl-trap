"""
marleg_fundamentals.py — ROBUST fundamentals provider.

Why this exists: the pod used to read yahoo's pre-baked `.info` ratios (trailingPE,
profitMargins, pegRatio, ...). For Indian names those are frequently MISSING (pegRatio
absent for ~all NSE stocks; earningsGrowth / forwardPE / targets absent for small-caps)
and sometimes simply WRONG (e.g. TEJASNET .info profitMargins = -82% vs the real
TTM-from-statements -25.4%). Meanwhile the financial statements are available for ~100%
of names and go unused.

This module computes every ratio from the statements (TTM from quarterlies, fallback to
latest annual), uses `.info` / `fast_info` only as a fallback, applies sanity + formatting
guards, distinguishes "not reported" from "bad", and reports DATA COVERAGE so we can see
exactly how complete each name is.

  fundamentals(tk, names_map=None) -> dict  (same contract the /api/fundamentals card expects)
  python marleg_fundamentals.py TEJASNET     # CLI inspect
"""
import sys, math, warnings
warnings.filterwarnings("ignore")
import yfinance as yf

HEALTH_KEYS = ["ROE", "ROA", "Net margin", "Op margin", "Rev growth", "EPS growth",
               "D/E (x)", "Current ratio", "P/E", "Fwd P/E", "P/B", "PEG", "EV/EBITDA", "Div yield %"]


# ---------- statement helpers ----------
def _safe(fn):
    try:
        df = fn()
        return df if (df is not None and not df.empty) else None
    except Exception:
        return None


def _num(x):
    try:
        x = float(x)
        return None if math.isnan(x) else x
    except Exception:
        return None


def _row(df, *names):
    if df is None:
        return None
    for n in names:
        if n in df.index:
            s = df.loc[n].dropna()
            if len(s):
                return s
    return None


def _flow(qdf, adf, *names):
    """(ttm, prior_ttm) for a flow line. Prefer trailing-4-quarter sum; fallback latest annual."""
    q = _row(qdf, *names)
    if q is not None and len(q) >= 4:
        ttm = float(q.iloc[:4].sum())
        prior = float(q.iloc[4:8].sum()) if len(q) >= 8 else None
        return ttm, prior
    a = _row(adf, *names)
    if a is not None and len(a):
        return float(a.iloc[0]), (float(a.iloc[1]) if len(a) >= 2 else None)
    return None, None


def _point(bdf, *names):
    """latest balance-sheet value (point-in-time)."""
    s = _row(bdf, *names)
    return float(s.iloc[0]) if (s is not None and len(s)) else None


def _clip(x, lo, hi):
    if x is None:
        return None
    return None if (x < lo or x > hi) else x          # outside sane band -> treat as unreliable


def _pctchg(a, b):
    return ((a / b - 1) * 100) if (a is not None and b not in (None, 0) and b > 0) else None


def _growth_calc(ttm, prior, adf, *names):
    """Growth %, robust: trailing-4Q vs prior-4Q first, then annual YoY, rejecting absurd
    values (tiny-base blow-ups like +1285%). Returns the first estimate inside a sane band."""
    cands = []
    g1 = _pctchg(ttm, prior)
    if g1 is not None:
        cands.append(g1)
    a = _row(adf, *names)
    if a is not None and len(a) >= 2:
        g2 = _pctchg(float(a.iloc[0]), float(a.iloc[1]))
        if g2 is not None:
            cands.append(g2)
    for g in cands:
        if -95 <= g <= 400:
            return g
    return None


def _piotroski(ai, bs, cf):
    try:
        def v(s, i):
            return float(s.iloc[i]) if (s is not None and len(s) > i) else None
        ni = _row(ai, "Net Income", "Net Income Common Stockholders")
        ta = _row(bs, "Total Assets"); cfo = _row(cf, "Operating Cash Flow", "Total Cash From Operating Activities")
        ltd = _row(bs, "Long Term Debt")
        ca = _row(bs, "Current Assets", "Total Current Assets"); cl = _row(bs, "Current Liabilities", "Total Current Liabilities")
        rev = _row(ai, "Total Revenue"); gp = _row(ai, "Gross Profit"); sh = _row(bs, "Share Issued", "Ordinary Shares Number")
        score = 0; checks = 0
        def chk(c):
            nonlocal score, checks
            if c is None:
                return
            checks += 1
            if c:
                score += 1
        ni0, ta0, ta1 = v(ni, 0), v(ta, 0), v(ta, 1)
        roa0 = (ni0 / ta0) if (ni0 is not None and ta0) else None
        roa1 = (v(ni, 1) / ta1) if (v(ni, 1) is not None and ta1) else None
        chk(ni0 is not None and ni0 > 0)
        chk(v(cfo, 0) is not None and v(cfo, 0) > 0)
        chk(roa0 is not None and roa1 is not None and roa0 > roa1)
        chk(v(cfo, 0) is not None and ni0 is not None and v(cfo, 0) > ni0)
        chk(v(ltd, 0) is not None and v(ltd, 1) is not None and v(ltd, 0) < v(ltd, 1))
        cur0 = (v(ca, 0) / v(cl, 0)) if (v(ca, 0) and v(cl, 0)) else None
        cur1 = (v(ca, 1) / v(cl, 1)) if (v(ca, 1) and v(cl, 1)) else None
        chk(cur0 is not None and cur1 is not None and cur0 > cur1)
        chk(v(sh, 0) is not None and v(sh, 1) is not None and v(sh, 0) <= v(sh, 1))
        gm0 = (v(gp, 0) / v(rev, 0)) if (v(gp, 0) and v(rev, 0)) else None
        gm1 = (v(gp, 1) / v(rev, 1)) if (v(gp, 1) and v(rev, 1)) else None
        chk(gm0 is not None and gm1 is not None and gm0 > gm1)
        at0 = (v(rev, 0) / ta0) if (v(rev, 0) and ta0) else None
        at1 = (v(rev, 1) / ta1) if (v(rev, 1) and ta1) else None
        chk(at0 is not None and at1 is not None and at0 > at1)
        return {"score": score, "of": checks} if checks else None
    except Exception:
        return None


# ---------- main ----------
def _ticker(tk):
    """Try NSE; if statements are empty, try BSE."""
    t = yf.Ticker(tk + ".NS")
    if _safe(lambda: t.quarterly_income_stmt) is not None or _safe(lambda: t.balance_sheet) is not None:
        return t, tk + ".NS"
    tb = yf.Ticker(tk + ".BO")
    return (tb, tk + ".BO") if _safe(lambda: tb.balance_sheet) is not None else (t, tk + ".NS")


def fundamentals(tk, names_map=None):
    tk = tk.upper()
    t, ysym = _ticker(tk)
    info = {}
    try:
        info = t.info or {}
    except Exception:
        info = {}
    qi, ai = _safe(lambda: t.quarterly_income_stmt), _safe(lambda: t.income_stmt)
    bs = _safe(lambda: t.balance_sheet)
    qcf, cf = _safe(lambda: t.quarterly_cashflow), _safe(lambda: t.cashflow)
    fi = {}
    try:
        f = t.fast_info
        fi = {"price": _num(f.get("lastPrice")), "mcap": _num(f.get("marketCap")), "shares": _num(f.get("shares"))}
    except Exception:
        pass

    price = fi.get("price") or _num(info.get("currentPrice")) or _num(info.get("regularMarketPrice"))
    if not price:
        return {"error": "no data for " + tk, "tk": tk}
    shares = fi.get("shares") or _point(bs, "Share Issued", "Ordinary Shares Number") or _num(info.get("sharesOutstanding"))
    mcap = fi.get("mcap") or (price * shares if shares else _num(info.get("marketCap")))

    # flows (TTM)
    rev, rev0 = _flow(qi, ai, "Total Revenue", "Operating Revenue")
    ni, ni0 = _flow(qi, ai, "Net Income", "Net Income Common Stockholders")
    oi, _ = _flow(qi, ai, "Operating Income", "Total Operating Income As Reported")
    gp, _ = _flow(qi, ai, "Gross Profit")
    ebitda, _ = _flow(qi, ai, "EBITDA", "Normalized EBITDA")
    cfo, _ = _flow(qcf, cf, "Operating Cash Flow", "Total Cash From Operating Activities")
    capex, _ = _flow(qcf, cf, "Capital Expenditure")
    # balance (point in time)
    equity = _point(bs, "Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity")
    assets = _point(bs, "Total Assets")
    debt = _point(bs, "Total Debt")
    if debt is None:
        ltd = _point(bs, "Long Term Debt") or 0
        std = _point(bs, "Current Debt", "Short Term Debt", "Current Debt And Capital Lease Obligation") or 0
        debt = (ltd + std) or None
    cash = _point(bs, "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments")
    ca = _point(bs, "Current Assets", "Total Current Assets")
    cl = _point(bs, "Current Liabilities", "Total Current Liabilities")

    def margin(num, den):
        return _clip(num / den * 100, -300, 300) if (num is not None and den) else None

    src = {}
    def pick(computed, yahoo_key, transform=lambda x: x):
        """prefer statement-computed; fall back to a sane yahoo value."""
        if computed is not None:
            return computed, "computed"
        y = _num(info.get(yahoo_key))
        return (transform(y), "yahoo") if y is not None else (None, "n/a")

    net_margin, src["Net margin"] = pick(margin(ni, rev), "profitMargins", lambda x: x * 100)
    op_margin, src["Op margin"] = pick(margin(oi, rev), "operatingMargins", lambda x: x * 100)
    rev_growth, src["Rev growth"] = pick(_growth_calc(rev, rev0, ai, "Total Revenue", "Operating Revenue"), "revenueGrowth", lambda x: x * 100)
    eps_growth, src["EPS growth"] = pick(_growth_calc(ni, ni0, ai, "Net Income", "Net Income Common Stockholders"), "earningsGrowth", lambda x: x * 100)
    roe, src["ROE"] = pick(margin(ni, equity), "returnOnEquity", lambda x: x * 100)
    roa, src["ROA"] = pick(margin(ni, assets), "returnOnAssets", lambda x: x * 100)

    eps_ttm = (ni / shares) if (ni is not None and shares) else _num(info.get("trailingEps"))
    pe_c = (price / eps_ttm) if (eps_ttm and eps_ttm > 0) else None
    pe, src["P/E"] = pick(pe_c, "trailingPE")
    bvps = (equity / shares) if (equity and shares) else _num(info.get("bookValue"))
    pb_c = (price / bvps) if (bvps and bvps > 0) else None
    pb, src["P/B"] = pick(pb_c, "priceToBook")
    ev = (mcap + (debt or 0) - (cash or 0)) if mcap else None
    evebitda_c = (ev / ebitda) if (ev and ebitda and ebitda > 0) else None
    ev_ebitda, src["EV/EBITDA"] = pick(evebitda_c, "enterpriseToEbitda")
    current_ratio, src["Current ratio"] = pick((ca / cl) if (ca and cl) else None, "currentRatio")
    de_c = (debt / equity) if (debt is not None and equity and equity > 0) else None
    de, src["D/E (x)"] = pick(de_c, "debtToEquity", lambda x: x / 100)
    fcf = (cfo + capex) if (cfo is not None and capex is not None) else _num(info.get("freeCashflow"))
    peg, src["PEG"] = pick((pe / eps_growth) if (pe and eps_growth and eps_growth > 0) else None, "pegRatio")
    fwd_pe, src["Fwd P/E"] = pick(None, "forwardPE")                 # needs forward estimates -> yahoo-only
    # dividend yield: prefer rate/price; yahoo's field flips between fraction/percent across versions
    dr = _num(info.get("dividendRate")) or _num(info.get("trailingAnnualDividendRate"))
    dy = (dr / price * 100) if dr else None
    if dy is None:
        y = _num(info.get("dividendYield"))
        dy = (y if 0 <= y < 25 else (y * 100 if y and y < 1 else None)) if y is not None else None
    div_yield = dy

    # P/E that is technically positive but absurd is "not meaningful"
    pe_nm = (pe is not None and pe > 300)

    def r1(x):
        return round(x, 1) if isinstance(x, (int, float)) else None
    def r2(x):
        return round(x, 2) if isinstance(x, (int, float)) else None

    health = {"ROE": r1(roe), "ROA": r1(roa), "Net margin": r1(net_margin), "Op margin": r1(op_margin),
              "Rev growth": r1(rev_growth), "EPS growth": r1(eps_growth),
              "D/E (x)": r2(de), "Current ratio": r2(current_ratio),
              "P/E": ("n.m." if pe_nm else r2(pe)), "Fwd P/E": r2(fwd_pe), "P/B": r2(pb),
              "PEG": r2(peg), "EV/EBITDA": r2(ev_ebitda), "Div yield %": r1(div_yield)}

    # ---- valuation: Graham + DCF ----
    graham = math.sqrt(22.5 * eps_ttm * bvps) if (eps_ttm and bvps and eps_ttm > 0 and bvps > 0) else None
    g = max(0.05 if (eps_ttm and eps_ttm > 0) else 0.0,
            min((eps_growth / 100 if eps_growth else (rev_growth / 100 if rev_growth else 0.08)), 0.15))
    dcf = None
    if fcf and fcf > 0 and shares:
        r, gt, fc, pv = 0.115, 0.045, fcf, 0.0
        for yrn in range(1, 6):
            fc *= (1 + g); pv += fc / ((1 + r) ** yrn)
        pv += (fc * (1 + gt) / (r - gt)) / ((1 + r) ** 5)
        dcf = (pv + (cash or _num(info.get("totalCash")) or 0) - (debt or _num(info.get("totalDebt")) or 0)) / shares
    intrinsic = [x for x in [graham, dcf] if x and x > 0]
    fair = sum(intrinsic) / len(intrinsic) if intrinsic else None
    gap = ((fair / price - 1) * 100) if fair else None
    verdict = "N/A" if gap is None else "UNDERVALUED" if gap > 15 else "OVERVALUED" if gap < -15 else "FAIRLY VALUED"
    tgt = _num(info.get("targetMeanPrice")); rec = info.get("recommendationKey"); na = info.get("numberOfAnalystOpinions")
    upside = ((tgt / price - 1) * 100) if tgt else None

    # ---- quality score (denominator = AVAILABLE checks; "missing" != "weak") ----
    raw = [(roe, roe is not None and roe > 15, 20), (net_margin, net_margin is not None and net_margin > 10, 15),
           (rev_growth, rev_growth is not None and rev_growth > 8, 15), (eps_growth, eps_growth is not None and eps_growth > 0, 15),
           (de, de is not None and de < 1.0, 15), (current_ratio, current_ratio is not None and current_ratio > 1.2, 10),
           (fcf, fcf is not None and fcf > 0, 10)]
    avail = [(ok, w) for val, ok, w in raw if val is not None]
    mx = sum(w for _, w in avail); sc = sum(w for ok, w in avail if ok)
    qscore = round(sc / mx * 100) if mx else None
    q_cov = round(len(avail) / len(raw) * 100)
    pio = _piotroski(ai, bs, cf)

    # data coverage across the headline grid
    filled = sum(1 for k in HEALTH_KEYS if health.get(k) not in (None, "n.m."))
    coverage = round(filled / len(HEALTH_KEYS) * 100)
    computed_n = sum(1 for k in src if src[k] == "computed")

    # ---- narrative ----
    def s2(x):
        return format(x, ",.0f") if isinstance(x, (int, float)) else "n/a"
    def f2(x):
        return ("%.2f" % x) if isinstance(x, (int, float)) else "n/a"
    N = []
    if fair:
        div_note = ""
        if upside is not None and gap is not None:
            if gap < -10 and upside > 10:
                div_note = (" Divergence: intrinsic models (current cash flow / book) screen rich, yet analysts see ~%.0f%% upside — that gap is the growth premium a current-FCF DCF can't capture." % upside)
            elif gap > 10 and upside < -10:
                div_note = " Divergence: cheap on assets/cash flow but analysts see downside — often a value trap or deteriorating outlook."
        N.append({"h": "What it's worth", "p":
            ("Intrinsic estimates span Graham %s and DCF %s vs the market price of Rs %s — on current cash flow and book value it screens %s. "
             "Read as a range: Graham is a conservative floor; the DCF assumes ~%d%% cash-flow growth at 11.5%% discount. "
             "Multiples: P/E %s, P/B %s, EV/EBITDA %s.%s")
            % (("Rs " + s2(graham)) if graham else "n/a", ("Rs " + s2(dcf)) if dcf else "n/a", s2(price), verdict.lower(),
               int(g * 100), (health["P/E"] if health["P/E"] not in (None,) else "n/a"), f2(pb), f2(ev_ebitda), div_note)})
    loss = (eps_ttm is not None and eps_ttm <= 0)
    N.append({"h": "How they're doing", "p":
        ("Return on equity is %s%% and net margin %s%%, on revenue growth of %s%% and EPS growth of %s%%. "
         "Balance sheet: debt/equity %s, current ratio %s. %s%s%s")
        % (f2(roe), f2(net_margin), f2(rev_growth), f2(eps_growth),
           (f2(de) + "x") if de is not None else "n/a", f2(current_ratio),
           ("Piotroski %d/%d (accounting health, 9 best). " % (pio["score"], pio["of"])) if pio else "",
           ("Loss-making on a trailing basis, so P/E isn't meaningful. " if loss else ""),
           (("Quality %s/100 — %s." % (qscore, "strong" if qscore >= 70 else "mixed" if qscore >= 45 else "weak")
             + (" (only %d%% of quality inputs reported.)" % q_cov if q_cov < 70 else "")) if qscore is not None else "Quality: insufficient data reported."))})
    if tgt:
        N.append({"h": "The Street's 1-year view", "p":
            ("Analyst consensus target is Rs %s — %s%% %s today's Rs %s, from %s analysts (%s). Range Rs %s to Rs %s."
             % (s2(tgt), ("%.0f" % abs(upside)) if upside is not None else "—", "above" if (upside or 0) > 0 else "below",
                s2(price), na or "?", (rec or "n/a").replace("_", " "),
                s2(_num(info.get("targetLowPrice")) or 0), s2(_num(info.get("targetHighPrice")) or 0)))})
    else:
        N.append({"h": "The Street's 1-year view", "p": "No sell-side analyst coverage reported for this name, so there's no consensus target — lean on intrinsic value + the volume/technical read."})

    return {"tk": tk, "ysym": ysym, "name": info.get("shortName") or (names_map or {}).get(tk, tk),
            "price": round(price, 2), "sector": info.get("sector"), "industry": info.get("industry"),
            "graham": round(graham, 1) if graham else None, "dcf": round(dcf, 1) if dcf else None,
            "fair": round(fair, 1) if fair else None, "gap": round(gap, 1) if gap is not None else None,
            "verdict": verdict, "target": round(tgt, 1) if tgt else None,
            "upside": round(upside, 1) if upside is not None else None, "rec": rec, "n_analysts": na,
            "health": health, "qscore": qscore, "q_coverage": q_cov, "piotroski": pio,
            "coverage": coverage, "computed_fields": computed_n, "sources": src, "narrative": N}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for tk in (sys.argv[1:] or ["TEJASNET"]):
        r = fundamentals(tk)
        if r.get("error"):
            print(tk, r["error"]); continue
        print(f"\n{r['name']} ({r['tk']} -> {r['ysym']}) Rs {r['price']} · {r.get('sector')}/{r.get('industry')}")
        print(f"  coverage {r['coverage']}% of grid · {r['computed_fields']} fields computed-from-statements · "
              f"quality {r['qscore']}/100 (inputs {r['q_coverage']}%) · Piotroski {r['piotroski']['score'] if r['piotroski'] else '?'}/{r['piotroski']['of'] if r['piotroski'] else '?'}")
        h = r["health"]
        print("  " + " · ".join(f"{k} {h[k]}" for k in HEALTH_KEYS if h[k] is not None))
        miss = [k for k in HEALTH_KEYS if h[k] is None]
        if miss:
            print("  still n/a:", miss)
        print("  valuation:", r["verdict"], "Graham", r["graham"], "DCF", r["dcf"], "fair", r["fair"], "target", r["target"])


if __name__ == "__main__":
    main()
