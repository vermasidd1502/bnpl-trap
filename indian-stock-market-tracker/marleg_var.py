"""
marleg_var.py — portfolio Value-at-Risk, FIN-537 style.

Follows the F537 risk framework:
  - conditional volatility via EWMA / RiskMetrics (λ=0.94)  (GARCH-lite σ_t)
  - FAT TAILS: Student-t VaR alongside Normal (the course's central lesson — Normal
    underestimates the left tail) + Historical Simulation (no distributional assumption)
  - Expected Shortfall (CVaR) — average loss beyond VaR
  - portfolio aggregation via the sample correlation matrix
  - Component / marginal VaR — which position drives the risk
  - multi-day (weekend) scaling + leverage context (VaR vs equity)

Pulls the live LONG book from Groww (demat, positive qty only — drops MTF churn artifacts).

  python marleg_var.py
"""
import sys, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, yfinance as yf
from scipy import stats

LAMBDA = 0.94                      # RiskMetrics EWMA decay
ANN = 252


def _book():
    import groww_client as gc
    g = gc.GrowwClient(); g.token()
    h = g.holdings_data() or {}
    bk = {}
    for x in (h.get("holdings") or []):
        s = (x.get("trading_symbol") or "").upper(); q = float(x.get("quantity") or 0)
        if q > 0:
            bk[s] = q
    qt = g.quote_table(list(bk))
    out = {}
    for s, q in bk.items():
        L = (qt.get(s, {}) or {}).get("price")
        if L:
            out[s] = {"qty": q, "ltp": float(L), "val": q * float(L)}
    cash = float((g.margin_data() or {}).get("clear_cash") or 0)
    return out, cash


def _ewma_vol(r):
    """RiskMetrics EWMA daily vol forecast (conditional σ_{t+1})."""
    v = r.var()
    for x in r.values:
        v = LAMBDA * v + (1 - LAMBDA) * x * x
    return float(np.sqrt(v))


def run():
    book, cash = _book()
    book = {s: d for s, d in book.items() if d["val"] >= 500}      # drop dust
    syms = list(book)
    V = sum(d["val"] for d in book.values())
    w = np.array([book[s]["val"] / V for s in syms])

    px = yf.download([s + ".NS" for s in syms], period="2y", interval="1d",
                     progress=False, group_by="ticker", auto_adjust=False)
    rets = {}
    for s in syms:
        try:
            c = px[s + ".NS"]["Close"].dropna()
        except Exception:
            c = pd.Series(dtype=float)
        rets[s] = np.log(c / c.shift(1)).dropna()
    R = pd.DataFrame(rets).dropna()
    # per-asset vols
    samp = R.std().values
    ewma = np.array([_ewma_vol(R[s]) for s in syms])
    corr = R.corr().values
    # portfolio daily return series (current weights)
    port = (R[syms].values @ w)
    sig_p_samp = float(np.sqrt(w @ (np.outer(samp, samp) * corr) @ w))
    sig_p_ewma = float(np.sqrt(w @ (np.outer(ewma, ewma) * corr) @ w))
    # fat tails
    exk = float(pd.Series(port).kurtosis())                       # excess kurtosis
    d = max(4.5, 6.0 / exk + 4) if exk > 0.1 else 30              # Student-t dof (cheatsheet)
    z95, z99 = 1.645, 2.326
    t95 = stats.t.ppf(0.95, d) * np.sqrt((d - 2) / d)
    t99 = stats.t.ppf(0.99, d) * np.sqrt((d - 2) / d)

    def report(sig, label):
        out = {"sigma_day_pct": round(sig * 100, 2)}
        out["normal95"] = z95 * sig * V; out["normal99"] = z99 * sig * V
        out["t95"] = t95 * sig * V; out["t99"] = t99 * sig * V
        return out
    rs = report(sig_p_samp, "sample"); re = report(sig_p_ewma, "ewma")
    # historical simulation + ES (on actual portfolio returns)
    hs95 = -np.percentile(port, 5) * V; hs99 = -np.percentile(port, 1) * V
    es95 = -port[port <= np.percentile(port, 5)].mean() * V
    es99 = -port[port <= np.percentile(port, 1)].mean() * V
    # component VaR (marginal contribution to Normal-95 using EWMA cov)
    cov = np.outer(ewma, ewma) * corr
    mvar = (cov @ w) / sig_p_ewma                                 # marginal vol
    comp = w * mvar / sig_p_ewma                                  # fractional contribution (sums to 1)

    print(f"\n=== PORTFOLIO VaR (FIN-537) — exposure Rs {V:,.0f} · cash Rs {cash:,.0f} ===")
    print(f"positions: " + " · ".join(f"{s} {round(book[s]['val']/V*100)}%" for s in syms))
    print(f"\nportfolio daily vol: sample {sig_p_samp*100:.2f}% · EWMA(λ.94) {sig_p_ewma*100:.2f}%  | annualized ~{sig_p_ewma*np.sqrt(ANN)*100:.0f}%")
    print(f"fat-tail: excess kurtosis {exk:.1f} -> Student-t d≈{d:.1f} (lower=fatter)")
    print(f"\n1-DAY VaR (rupees you could lose):")
    print(f"  {'method':<22}{'95%':>12}{'99%':>12}")
    print(f"  {'Normal (EWMA)':<22}{re['normal95']:>12,.0f}{re['normal99']:>12,.0f}")
    print(f"  {'Student-t (EWMA)':<22}{re['t95']:>12,.0f}{re['t99']:>12,.0f}   <- fat-tail (bigger)")
    print(f"  {'Historical sim':<22}{hs95:>12,.0f}{hs99:>12,.0f}")
    print(f"  {'Expected Shortfall':<22}{es95:>12,.0f}{es99:>12,.0f}   <- avg loss BEYOND VaR")
    h = 3
    print(f"\nWEEKEND / {h}-day VaR (√{h} scaling; gap risk makes the true tail worse):")
    print(f"  Normal-99 ~Rs {re['normal99']*np.sqrt(h):,.0f} · Student-t 99 ~Rs {re['t99']*np.sqrt(h):,.0f} · ES-99 ~Rs {es99*np.sqrt(h):,.0f}")
    print(f"\nRISK CONTRIBUTION (who drives the risk):")
    for s, cc, vv in sorted(zip(syms, comp, [book[s]['val'] for s in syms]), key=lambda z: -z[1]):
        print(f"  {s:<12} weight {vv/V*100:>4.0f}%   risk-share {cc*100:>4.0f}%   asset vol {R[s].std()*100:.1f}%/day")
    print(f"\nLEVERAGE CONTEXT: 1-day Student-t 99% VaR = Rs {re['t99']:,.0f} = {re['t99']/cash*100:.0f}% of your free cash (Rs {cash:,.0f})")
    print(f"  weekend Student-t 99% ~Rs {re['t99']*np.sqrt(h):,.0f} = {re['t99']*np.sqrt(h)/cash*100:.0f}% of free cash.")
    return {"V": V, "cash": cash}


def montecarlo(N=50000, horizons=(1, 3), seed=7):
    """Correlated Monte Carlo (Cholesky) with Student-t fat tails + Normal, per FIN-537.
    Returns the probability of losing more than various rupee thresholds, 1-day and weekend."""
    book, cash = _book()
    book = {s: d for s, d in book.items() if d["val"] >= 500}
    syms = list(book); V = sum(d["val"] for d in book.values())
    w = np.array([book[s]["val"] / V for s in syms]); n = len(syms)
    px = yf.download([s + ".NS" for s in syms], period="2y", interval="1d",
                     progress=False, group_by="ticker", auto_adjust=False)
    R = pd.DataFrame({s: np.log(px[s + ".NS"]["Close"].dropna()).diff().dropna() for s in syms}).dropna()
    ewma = np.array([_ewma_vol(R[s]) for s in syms])
    corr = R.corr().values
    Lc = np.linalg.cholesky(corr + np.eye(n) * 1e-10)
    exk = max(0.2, float(pd.Series(R[syms].values @ w).kurtosis())); d = max(4.5, 6.0 / exk + 4)
    rng = np.random.default_rng(seed)
    sims = {}
    for h in horizons:
        for dist in ("normal", "t"):
            pnl = np.zeros(N)
            for _ in range(h):
                z = rng.standard_normal((N, n)) @ Lc.T            # correlated std-normal
                if dist == "t":
                    wq = rng.chisquare(d, size=(N, 1))
                    z = z * np.sqrt(d / wq) * np.sqrt((d - 2) / d)  # standardized multivariate-t
                pnl += (z * ewma) @ w                              # portfolio daily return
            sims[(h, dist)] = pnl * V
    print(f"\n=== MONTE CARLO ({N:,} sims, Cholesky-correlated, zero-drift) — book Rs {V:,.0f}, cash Rs {cash:,.0f} ===")
    print(f"fat-tail draw: Student-t d≈{d:.1f}\n")
    thr = [5000, 10000, 20000, 30000, 37000, 50000, 75000]
    for h, label in [(1, "1-DAY"), (3, "WEEKEND (3-day)")]:
        for dist in ("t", "normal"):
            pnl = sims[(h, dist)]
            tag = "Student-t (realistic)" if dist == "t" else "Normal (thin-tail)"
            print(f"{label} · {tag}:  P(profit) {np.mean(pnl > 0)*100:.0f}% · median Rs {np.median(pnl):,.0f} · worst 0.1% Rs {np.percentile(pnl,0.1):,.0f}")
            if dist == "t":
                probs = "  ".join(f"P(lose>{t//1000}k) {np.mean(pnl < -t)*100:.1f}%" for t in thr)
                print(f"   {probs}")
        print()
    # key answers under t
    pt1 = sims[(1, "t")]; pt3 = sims[(3, "t")]
    print(f"KEY ODDS (fat-tail): lose >Rs 20k in a day {np.mean(pt1 < -20000)*100:.1f}% · over the weekend {np.mean(pt3 < -20000)*100:.0f}%")
    print(f"            lose >Rs 37k (your weekend 99% VaR) over the weekend: {np.mean(pt3 < -37000)*100:.1f}%")
    print(f"            lose >30% of cash (Rs {cash*0.3:,.0f}) over the weekend: {np.mean(pt3 < -cash*0.3)*100:.1f}%")
    return sims


def portfolio_risk(N=30000, seed=7):
    """Serious portfolio risk: CAPM beta + systematic/idiosyncratic split, VaR (Normal/t/HS/ES),
    Monte-Carlo loss distribution (histogram + probabilities), component VaR, diversification,
    and beta-aware suggestions. Returns one JSON-able dict for the pod."""
    book, cash = _book()
    book = {s: d for s, d in book.items() if d["val"] >= 500}
    syms = list(book); V = sum(d["val"] for d in book.values())
    if not syms or V <= 0:
        return {"error": "no positions"}
    w = np.array([book[s]["val"] / V for s in syms]); n = len(syms)
    px = yf.download([s + ".NS" for s in syms] + ["^NSEI"], period="2y", interval="1d",
                     progress=False, group_by="ticker", auto_adjust=False)
    R = pd.DataFrame({s: np.log(px[s + ".NS"]["Close"].dropna()).diff() for s in syms})
    R["MKT"] = np.log(px["^NSEI"]["Close"].dropna()).diff()
    R = R.dropna()
    mkt = R["MKT"]; var_m = float(mkt.var())
    betas = {s: float(np.cov(R[s], mkt)[0, 1] / var_m) for s in syms}
    mcorr = {s: float(R[s].corr(mkt)) for s in syms}
    beta_p = float(sum(w[i] * betas[syms[i]] for i in range(n)))
    Rp = R[syms].values @ w
    sig_p = float(np.std(Rp))
    sys_var = beta_p ** 2 * var_m
    pct_sys = float(min(1.0, sys_var / (sig_p ** 2)))
    ewma = np.array([_ewma_vol(R[s]) for s in syms]); vols = R[syms].std().values
    corr = R[syms].corr().values
    off = corr[np.triu_indices(n, 1)]
    avg_corr = float(off.mean()) if len(off) else 0.0
    div_ratio = float((w @ vols) / sig_p)                       # >1 = diversification benefit; ~1 = none
    # VaR
    exk = max(0.2, float(pd.Series(Rp).kurtosis())); d = max(4.5, 6.0 / exk + 4)
    t99 = stats.t.ppf(0.99, d) * np.sqrt((d - 2) / d)
    var = {"normal99": 2.326 * sig_p * V, "t99": t99 * sig_p * V,
           "hist99": -np.percentile(Rp, 1) * V, "es99": -Rp[Rp <= np.percentile(Rp, 1)].mean() * V}
    # Monte Carlo (t), 1-day + weekend
    Lc = np.linalg.cholesky(corr + np.eye(n) * 1e-10); rng = np.random.default_rng(seed)
    def mc(h):
        pnl = np.zeros(N)
        for _ in range(h):
            z = rng.standard_normal((N, n)) @ Lc.T
            wq = rng.chisquare(d, size=(N, 1)); z = z * np.sqrt(d / wq) * np.sqrt((d - 2) / d)
            pnl += (z * ewma) @ w
        return pnl * V
    pnl1, pnl3 = mc(1), mc(3)
    cnt, edges = np.histogram(pnl3, bins=41)
    thr = [10000, 20000, 30000, 37000, 50000]
    # component VaR
    cov = np.outer(ewma, ewma) * corr; comp = w * ((cov @ w) / sig_p) / sig_p
    # suggestions
    sug = []
    if beta_p > 1.2:
        sug.append(f"High portfolio beta ({beta_p:.2f}) — you're geared ~{beta_p:.1f}× to Nifty; a market drop hits you hard. Add a low-beta/defensive name to dampen.")
    elif beta_p < 0.8:
        sug.append(f"Low beta ({beta_p:.2f}) — defensive, less market-sensitive.")
    if avg_corr > 0.45:
        sug.append(f"Holdings are highly correlated with each other (avg {avg_corr:.2f}) — little diversification; in a sell-off they fall together. Add an UNCORRELATED name.")
    if pct_sys > 0.55:
        sug.append(f"{pct_sys*100:.0f}% of your risk is *market* (systematic) — it's basically a leveraged Nifty bet. Diversifying stocks won't help; only lower beta or hedge will.")
    top = syms[int(np.argmax(comp))]
    sug.append(f"{top} drives {comp.max()*100:.0f}% of portfolio risk — trimming it cuts the most risk per rupee.")
    if var["t99"] > 0.25 * cash:
        sug.append(f"1-day 99% VaR (₹{var['t99']:,.0f}) is {var['t99']/cash*100:.0f}% of free cash — high for the capital; reduce gross or hold more cash.")
    return {"asof_note": "live book · 2y daily · EWMA vol · Student-t MC", "V": round(V), "cash": round(cash),
            "positions": [{"sym": s, "weight": round(book[s]["val"] / V * 100, 1), "beta": round(betas[s], 2),
                           "mkt_corr": round(mcorr[s], 2), "vol_pct": round(float(R[s].std()) * 100, 1),
                           "risk_share": round(float(comp[i]) * 100)} for i, s in enumerate(syms)],
            "beta": round(beta_p, 2), "vol_day_pct": round(sig_p * 100, 2), "vol_ann_pct": round(sig_p * np.sqrt(ANN) * 100),
            "pct_systematic": round(pct_sys * 100), "pct_idiosyncratic": round((1 - pct_sys) * 100),
            "avg_pair_corr": round(avg_corr, 2), "diversification_ratio": round(div_ratio, 2), "tdof": round(d, 1),
            "var": {k: round(v) for k, v in var.items()},
            "mc": {"hist_counts": cnt.tolist(), "hist_edges": [round(e) for e in edges],
                   "prob_1d": {str(t): round(float(np.mean(pnl1 < -t)) * 100, 1) for t in thr},
                   "prob_wk": {str(t): round(float(np.mean(pnl3 < -t)) * 100, 1) for t in thr},
                   "worst_wk_01pct": round(float(np.percentile(pnl3, 0.1))), "p_profit_wk": round(float(np.mean(pnl3 > 0)) * 100)},
            "suggestions": sug}


def _metrics(book, R, mkt, var_m):
    """All portfolio risk metrics for a given book dict {sym:{val}} against return frame R.
    Returns a flat dict — used to diff a hypothetical book against the current one."""
    syms = list(book); V = sum(book[s]["val"] for s in syms); n = len(syms)
    w = np.array([book[s]["val"] / V for s in syms])
    sub = R[syms]
    betas = {s: float(np.cov(sub[s], mkt)[0, 1] / var_m) for s in syms}
    beta_p = float(w @ np.array([betas[s] for s in syms]))
    ewma = np.array([_ewma_vol(sub[s]) for s in syms])
    corr = sub.corr().values
    cov = np.outer(ewma, ewma) * corr
    sig_p = float(np.sqrt(max(w @ cov @ w, 1e-12)))               # EWMA conditional daily vol
    Rp = sub.values @ w
    exk = max(0.2, float(pd.Series(Rp).kurtosis())); d = max(4.5, 6.0 / exk + 4)
    t99 = stats.t.ppf(0.99, d) * np.sqrt((d - 2) / d)
    var1 = t99 * sig_p * V                                        # 1-day Student-t 99% VaR
    sys_var = beta_p ** 2 * var_m
    pct_sys = float(min(1.0, sys_var / (sig_p ** 2)))
    off = corr[np.triu_indices(n, 1)]
    avg_corr = float(off.mean()) if len(off) else 0.0
    vols = sub.std().values
    div_ratio = float((w @ vols) / sig_p) if sig_p else 1.0
    comp = w * ((cov @ w) / sig_p) / sig_p                        # risk-share per name
    return {"V": V, "beta": beta_p, "vol_day_pct": sig_p * 100, "var99_1d": var1,
            "var99_wk": var1 * np.sqrt(3), "pct_sys": pct_sys * 100, "avg_corr": avg_corr,
            "div_ratio": div_ratio, "comp": {syms[i]: float(comp[i]) for i in range(n)}}


def whatif(ticker, qty, side="buy"):
    """Scenario: add (buy) or trim (sell) `qty` of `ticker` and show how portfolio risk shifts.
    Returns before/after metrics + the candidate's standalone profile (beta, vol, correlation to
    the existing book, post-trade risk-share). No Monte-Carlo — parametric, so it's instant."""
    tk = (ticker or "").upper().strip()
    qty = float(qty or 0)
    if not tk or qty <= 0:
        return {"error": "need a ticker and a positive quantity"}
    book, cash = _book()
    book = {s: d for s, d in book.items() if d["val"] >= 500}
    if not book:
        return {"error": "no live positions to compare against"}
    cur_syms = list(book)
    need = sorted(set(cur_syms + [tk]))
    px = yf.download([s + ".NS" for s in need] + ["^NSEI"], period="1y", interval="1d",
                     progress=False, group_by="ticker", auto_adjust=False)

    def close(s):
        try:
            return px[s + ".NS"]["Close"].dropna()
        except Exception:
            return pd.Series(dtype=float)
    cand_close = close(tk)
    if cand_close.empty:
        return {"error": f"no price data for {tk} (check the symbol)"}
    cand_ltp = float(cand_close.iloc[-1])
    cand_val = qty * cand_ltp

    R = pd.DataFrame({s: np.log(close(s)).diff() for s in need})
    R["MKT"] = np.log(close("^NSEI") if not close("^NSEI").empty else px["^NSEI"]["Close"].dropna()).diff()
    R = R.dropna()
    mkt = R["MKT"]; var_m = float(mkt.var())

    # before = current book; after = current ± candidate
    before = {s: {"val": book[s]["val"]} for s in cur_syms}
    after = {s: {"val": book[s]["val"]} for s in cur_syms}
    if side == "sell":
        held_val = book.get(tk, {}).get("val", 0.0)
        new_val = max(0.0, held_val - cand_val)
        if new_val <= 500:
            after.pop(tk, None)
        else:
            after[tk] = {"val": new_val}
        if tk not in book:
            return {"error": f"{tk} isn't in your book — nothing to trim"}
    else:  # buy / add
        after[tk] = {"val": book.get(tk, {}).get("val", 0.0) + cand_val}

    m_before = _metrics(before, R, mkt, var_m)
    m_after = _metrics(after, R, mkt, var_m)

    # candidate standalone profile
    cand_beta = float(np.cov(R[tk], mkt)[0, 1] / var_m)
    cand_vol = float(R[tk].std() * 100)
    # correlation of candidate to the *existing* book's return stream
    w_b = np.array([before[s]["val"] for s in cur_syms]); w_b = w_b / w_b.sum()
    book_ret = R[cur_syms].values @ w_b
    cand_corr_book = float(np.corrcoef(R[tk].values, book_ret)[0, 1])
    cand_share_after = m_after["comp"].get(tk, 0.0) * 100

    dW = m_after["V"] - m_before["V"]
    dBeta = m_after["beta"] - m_before["beta"]
    dVaRwk = m_after["var99_wk"] - m_before["var99_wk"]
    dDiv = m_after["div_ratio"] - m_before["div_ratio"]
    dSys = m_after["pct_sys"] - m_before["pct_sys"]

    # verdict
    notes = []
    if cand_corr_book < 0.3:
        notes.append(f"Low correlation to your book ({cand_corr_book:.2f}) — it genuinely diversifies; it won't fall in lockstep with what you already hold.")
    elif cand_corr_book > 0.6:
        notes.append(f"Highly correlated to your book ({cand_corr_book:.2f}) — it moves with your existing names, so it adds exposure more than diversification.")
    else:
        notes.append(f"Moderate correlation to your book ({cand_corr_book:.2f}).")
    if dBeta < -0.02:
        notes.append(f"Pulls portfolio beta DOWN {m_before['beta']:.2f} → {m_after['beta']:.2f} — less market-geared (good if you're over-levered to Nifty).")
    elif dBeta > 0.02:
        notes.append(f"Pushes portfolio beta UP {m_before['beta']:.2f} → {m_after['beta']:.2f} — more market-geared.")
    if dDiv > 0.03:
        notes.append("Improves the diversification ratio — risk per rupee falls.")
    elif dDiv < -0.03:
        notes.append("Lowers the diversification ratio — concentrates risk.")
    if side == "buy":
        notes.append(f"Weekend 99% VaR { 'rises' if dVaRwk>0 else 'falls'} ₹{abs(dVaRwk):,.0f} → ₹{m_after['var99_wk']:,.0f} ({m_after['var99_wk']/cash*100:.0f}% of free cash ₹{cash:,.0f}).")
        if cand_val > cash:
            notes.append(f"⚠ Position size ₹{cand_val:,.0f} exceeds your free cash ₹{cash:,.0f} — only doable on MTF leverage.")

    diversifier = cand_corr_book < 0.35 and dDiv >= 0 and dBeta <= 0.05
    if diversifier:
        verdict = "DIVERSIFIER — lowers risk per rupee"
    elif dBeta > 0.05:
        verdict = "RISK-ADDER — raises portfolio beta"
    elif cand_corr_book > 0.6:
        verdict = "RISK-ADDER — correlated, adds exposure"
    else:
        verdict = "NEUTRAL — modest effect"

    return {
        "candidate": {"sym": tk, "qty": qty, "ltp": round(cand_ltp, 2), "value": round(cand_val),
                      "side": side, "beta": round(cand_beta, 2), "vol_pct": round(cand_vol, 1),
                      "corr_to_book": round(cand_corr_book, 2), "risk_share_after": round(cand_share_after)},
        "cash": round(cash), "verdict": verdict, "notes": notes,
        "before": {"exposure": round(m_before["V"]), "beta": round(m_before["beta"], 2),
                   "vol_day_pct": round(m_before["vol_day_pct"], 2), "var99_1d": round(m_before["var99_1d"]),
                   "var99_wk": round(m_before["var99_wk"]), "pct_sys": round(m_before["pct_sys"]),
                   "avg_corr": round(m_before["avg_corr"], 2), "div_ratio": round(m_before["div_ratio"], 2)},
        "after": {"exposure": round(m_after["V"]), "beta": round(m_after["beta"], 2),
                  "vol_day_pct": round(m_after["vol_day_pct"], 2), "var99_1d": round(m_after["var99_1d"]),
                  "var99_wk": round(m_after["var99_wk"]), "pct_sys": round(m_after["pct_sys"]),
                  "avg_corr": round(m_after["avg_corr"], 2), "div_ratio": round(m_after["div_ratio"], 2)},
        "delta": {"exposure": round(dW), "beta": round(dBeta, 2), "var99_wk": round(dVaRwk),
                  "div_ratio": round(dDiv, 2), "pct_sys": round(dSys)},
    }


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    run()


if __name__ == "__main__":
    main()
