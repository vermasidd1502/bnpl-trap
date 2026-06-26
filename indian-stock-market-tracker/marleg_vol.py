"""
Marle-G Options VOL layer (Trading Volatility / Bennett, applied to NSE options).

Trade vol as its own asset along three axes — LEVEL (implied vs realized), and (later)
SKEW + TERM STRUCTURE. Core signal: if an option's IV >> the underlying's realized-vol
forecast, vol is RICH (favour selling/structures that are short vega + positive theta,
e.g. iron butterfly/condor or short straddle); if IV << RV, vol is CHEAP (favour buying:
long straddle / OTM debit spread).

No QuantLib / Groww-IV (it returns null on illiquid strikes) — we Black-Scholes it:
  - implied_vol() inverts BS on the live premium (Groww/NSE).
  - greeks() returns delta/gamma/vega/theta (per-share and per-position).
  - realized_vol() = annualised close-to-close vol from yfinance history.
  - India VIX = the market-wide IV regime benchmark.

Position Greeks honour SIGN: a SHORT put (the user's TCS26JUL2000PE) is +theta / -vega /
-gamma / +delta — the classic "sell rich vol, collect decay" book.

  python marleg_vol.py --opt TCS26JUL2000PE --side short --qty 450
  python marleg_vol.py --underlying RELIANCE        # just IV-regime context
"""
import os, re, sys, math, argparse, datetime as dt
import yfinance as yf

R_FREE = 0.065          # ~India 1y T-bill / MIBOR proxy
TRADING_DAYS = 252

# ------------------------------------------------------------------ Black-Scholes
def _npdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def _ncdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_price(S, K, T, r, sigma, kind):
    if T <= 0 or sigma <= 0:
        intrinsic = max(0.0, (S - K) if kind == "C" else (K - S))
        return intrinsic
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if kind == "C":
        return S * _ncdf(d1) - K * math.exp(-r * T) * _ncdf(d2)
    return K * math.exp(-r * T) * _ncdf(-d2) - S * _ncdf(-d1)


def implied_vol(price, S, K, T, r, kind):
    """Bisection IV solve. Returns None if the price is below intrinsic / no solution."""
    intrinsic = max(0.0, (S - K) if kind == "C" else (K - S)) * math.exp(0)  # undiscounted floor
    if price < max(0.0, (S - K) if kind == "C" else (K - S)) * math.exp(-r * T) - 1e-6:
        return None
    lo, hi = 1e-4, 5.0
    for _ in range(100):
        mid = 0.5 * (lo + hi)
        if bs_price(S, K, T, r, mid, kind) > price:
            hi = mid
        else:
            lo = mid
        if hi - lo < 1e-6:
            break
    iv = 0.5 * (lo + hi)
    return iv if 1e-3 < iv < 4.99 else None


def greeks(S, K, T, r, sigma, kind):
    if T <= 0 or sigma <= 0:
        return {"delta": 0, "gamma": 0, "vega": 0, "theta": 0}
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    delta = _ncdf(d1) if kind == "C" else _ncdf(d1) - 1
    gamma = _npdf(d1) / (S * sigma * math.sqrt(T))
    vega = S * _npdf(d1) * math.sqrt(T) / 100.0                       # per 1 vol point (1%)
    if kind == "C":
        theta = (-(S * _npdf(d1) * sigma) / (2 * math.sqrt(T)) - r * K * math.exp(-r * T) * _ncdf(d2)) / 365.0
    else:
        theta = (-(S * _npdf(d1) * sigma) / (2 * math.sqrt(T)) + r * K * math.exp(-r * T) * _ncdf(-d2)) / 365.0
    return {"delta": delta, "gamma": gamma, "vega": vega, "theta": theta}  # per share


def _vega_raw(S, K, T, r, sigma):
    """dPrice/dσ per 1.0 of vol (NOT per 1%) — the derivative Newton needs to invert the price."""
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return S * _npdf(d1) * math.sqrt(T)


def implied_vol_newton(price, S, K, T, r, kind, tol=1e-6, max_iter=60):
    """
    THE implied vol: the σ that makes Black-Scholes reproduce the option's market price. You can't
    observe IV — you invert the pricing formula on the traded price (the mid of bid/ask, ideally).

    Optimisation ladder, fast → robust:
      1. NEWTON-RAPHSON on f(σ)=BS(σ)−price with f'(σ)=vega (analytic). Quadratic convergence, ~3-5
         iters — but vega→0 for deep OTM/ITM, so Newton can diverge there.
      2. SECANT (finite-difference Newton): same root-find but the derivative is approximated from two
         price points — no vega needed. This is the finite-difference fallback.
      3. BISECTION backstop (bracketed, guaranteed to converge).

    Returns (iv, diag) where iv is a decimal (0.31 = 31%) or None, and diag records which method
    converged + iteration count (so the page can show the numerics).
    """
    if price is None or price <= 0:
        return None, {"ok": False, "why": "no price"}
    intrinsic = max(0.0, (S - K) if kind == "C" else (K - S)) * math.exp(-r * T)
    if price < intrinsic - 1e-6:
        return None, {"ok": False, "why": "price below discounted intrinsic — no real IV"}
    ceiling = S if kind == "C" else K * math.exp(-r * T)             # no-arb upper bound on the premium
    if price >= ceiling - 1e-9:
        return None, {"ok": False, "why": "price at/above the no-arbitrage ceiling"}
    # 1) Newton with analytic vega
    sigma = 0.30
    for i in range(max_iter):
        diff = bs_price(S, K, T, r, sigma, kind) - price
        if abs(diff) < tol:
            return sigma, {"ok": True, "method": "newton", "iters": i + 1}
        v = _vega_raw(S, K, T, r, sigma)
        if v < 1e-8:
            break                                                   # vega collapsed → Newton unstable
        sigma -= diff / v
        if sigma <= 0 or sigma > 9.99:
            break
    # 2) secant (FD-derivative Newton)
    a, b = 0.01, 3.0
    fa = bs_price(S, K, T, r, a, kind) - price
    fb = bs_price(S, K, T, r, b, kind) - price
    for i in range(max_iter):
        if abs(fb - fa) < 1e-12:
            break
        c = b - fb * (b - a) / (fb - fa)                            # FD slope = (fb−fa)/(b−a)
        if not (0 < c < 10):
            break
        fc = bs_price(S, K, T, r, c, kind) - price
        if abs(fc) < tol:
            return c, {"ok": True, "method": "secant(FD)", "iters": i + 1}
        a, fa, b, fb = b, fb, c, fc
    # 3) bisection backstop
    iv = implied_vol(price, S, K, T, r, kind)
    if iv:
        return iv, {"ok": True, "method": "bisection"}
    return None, {"ok": False, "why": "no convergence"}


def fd_greeks(S, K, T, r, sigma, kind, pricer=None, h_rel=1e-4):
    """
    Greeks by FINITE DIFFERENCE (central differences) on a pricer function — model-agnostic, so the
    SAME machinery gives Greeks for Black-Scholes today and a binomial/Heston pricer tomorrow. Also a
    clean numerical cross-check of the closed-form greeks().

        delta = [V(S+h) − V(S−h)] / 2h
        gamma = [V(S+h) − 2V(S) + V(S−h)] / h²
        vega  = [V(σ+dσ) − V(σ−dσ)] / 2dσ      (reported per 1 vol-point, i.e. /100)
        theta = [V(T−dt) − V(T)]               (one calendar day of decay; sign is naturally negative)
        rho   = [V(r+dr) − V(r−dr)] / 2dr       (per 1% rate, /100)
    """
    P = pricer or bs_price
    hS = max(S * h_rel, 1e-6)
    v0 = P(S, K, T, r, sigma, kind)
    vSp, vSm = P(S + hS, K, T, r, sigma, kind), P(S - hS, K, T, r, sigma, kind)
    delta = (vSp - vSm) / (2 * hS)
    gamma = (vSp - 2 * v0 + vSm) / (hS * hS)
    dsig = 1e-4
    vega = (P(S, K, T, r, sigma + dsig, kind) - P(S, K, T, r, sigma - dsig, kind)) / (2 * dsig) / 100.0
    dt_ = 1.0 / 365.0
    theta = P(S, K, max(T - dt_, 1e-9), r, sigma, kind) - v0
    dr = 1e-4
    rho = (P(S, K, T, r + dr, sigma, kind) - P(S, K, T, r - dr, sigma, kind)) / (2 * dr) / 100.0
    return {"delta": delta, "gamma": gamma, "vega": vega, "theta": theta, "rho": rho}


# ------------------------------------------------------------------ realized vol
# Indices resolve to Yahoo's ^-tickers, NOT "<sym>.NS" (which 404s for NIFTY/BANKNIFTY).
INDEX_YAHOO = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS",
               "MIDCPNIFTY": "^NSEMDCP50", "NIFTYNXT50": "^NSEI"}


def _yf_symbol(sym):
    return INDEX_YAHOO.get((sym or "").upper(), (sym or "") + ".NS")


def realized_vol(symbol, window=20):
    try:
        import marleg_data as md
        import statistics
        df = md.candles(symbol, 1440, max(window + 25, 120))     # Groww-only
        c = df["close"].dropna() if df is not None and "close" in df.columns else None
        if c is None or len(c) < window + 1:
            return None
        rets = [math.log(c.iloc[i] / c.iloc[i - 1]) for i in range(1, len(c)) if c.iloc[i - 1] > 0]
        return statistics.pstdev(rets[-window:]) * math.sqrt(TRADING_DAYS) if len(rets) >= window else None
    except Exception:
        return None


def india_vix():
    try:                                                         # Groww INDIAVIX quote (no yfinance)
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        p = (g.ltp("INDIAVIX", exchange="NSE").json().get("payload") or {})
        for v in p.values():
            if v:
                return float(v)
    except Exception:
        pass
    return None


# ------------------------------------------------------------------ symbol parsing
MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}


# NSE trading holidays — roll a monthly expiry BACK to the prior session if it lands on one.
# Update yearly from the official NSE circular. (2026 list verified vs NSE/Groww calendar.)
NSE_HOLIDAYS = {
    "2026-01-15", "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03",
    "2026-04-14", "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14", "2026-10-02",
    "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25",
}


def _is_trading_day(d):
    return d.weekday() < 5 and d.isoformat() not in NSE_HOLIDAYS


def last_thursday(year, month):
    """Kept for back-compat (pre-Sep-2025 expiry rule). Use monthly_expiry() for current contracts."""
    d = dt.date(year, 12, 31) if month == 12 else dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    while d.weekday() != 3:        # 3 = Thursday
        d -= dt.timedelta(days=1)
    return d


def last_weekday(year, month, weekday):
    d = dt.date(year, 12, 31) if month == 12 else dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    while d.weekday() != weekday:
        d -= dt.timedelta(days=1)
    return d


def monthly_expiry(year, month):
    """NSE monthly F&O expiry = the LAST TUESDAY of the month (SEBI single-expiry-day rule, effective
    Sep-2025 — replaced the old last-Thursday), rolled back to the prior trading day if that Tuesday is
    an NSE holiday. (e.g. Jun-2026 → Tue Jun 30; the old rule wrongly gave Thu Jun 25.)"""
    e = last_weekday(year, month, 1)          # 1 = Tuesday
    while not _is_trading_day(e):
        e -= dt.timedelta(days=1)
    return e


def parse_option(sym):
    """NSE monthly format UND + YY + MON + STRIKE + CE/PE  (e.g. TCS26JUL2000PE)."""
    m = re.match(r"^([A-Z&\-]+?)(\d{2})([A-Z]{3})(\d+)(CE|PE)$", sym.upper())
    if not m:
        return None
    und, yy, mon, strike, cp = m.groups()
    if mon not in MONTHS:
        return None
    expiry = monthly_expiry(2000 + int(yy), MONTHS[mon])
    return {"underlying": und, "strike": float(strike), "kind": "C" if cp == "CE" else "P",
            "expiry": expiry, "right": cp}


def parse_option_any(sym):
    """Parse MONTHLY (UND+YY+MMM+STRIKE+CE/PE) OR WEEKLY (UND+YY+M+DD+STRIKE+CE/PE, M = 1-9 or O/N/D for
    Oct/Nov/Dec) NSE option symbols. Returns the same dict shape as parse_option (+ weekly flag)."""
    info = parse_option(sym)
    if info:
        return info
    m = re.match(r"^([A-Z&\-]+?)(\d{2})([1-9OND])(\d{2})(\d+)(CE|PE)$", (sym or "").upper().strip())
    if not m:
        return None
    und, yy, mc, dd, strike, cp = m.groups()
    month = {"O": 10, "N": 11, "D": 12}.get(mc, int(mc) if mc.isdigit() else 0)
    if not (1 <= month <= 12):
        return None
    try:
        expiry = dt.date(2000 + int(yy), month, int(dd))
    except ValueError:
        return None
    return {"underlying": und, "strike": float(strike), "kind": "C" if cp == "CE" else "P",
            "expiry": expiry, "right": cp, "weekly": True}


# ------------------------------------------------------------------ live prices
def _groww():
    try:
        import groww_client
        return groww_client.GrowwClient()
    except Exception:
        return None


def underlying_ltp(und, g):
    if g:
        for ex in ("NSE", "BSE"):                      # BSE so indices like SENSEX/BANKEX resolve too
            try:
                r = g.ltp(und, exchange=ex); p = (r.json().get("payload") or {})
                v = p.get(g.sym(und, ex)) if hasattr(g, "sym") else None
                if v:
                    return float(v)
                for vv in p.values():                  # payload key is EXCH_SYMBOL (e.g. BSE_SENSEX)
                    if vv:
                        return float(vv)
            except Exception:
                pass
    try:
        import marleg_data as md
        df = md.candles(und, 1440, 6)
        return float(df["close"].iloc[-1]) if df is not None and len(df) else None
    except Exception:
        return None


def option_ltp(sym, g):
    if not g:
        return None
    try:
        r = g.ltp(sym, segment="FNO"); p = (r.json().get("payload") or {})
        v = p.get(g.sym(sym))
        return float(v) if v else None
    except Exception:
        return None


def analyze_option(sym, side="long", qty=0, today=None):
    info = parse_option(sym)
    if not info:
        return {"error": f"could not parse option symbol '{sym}'"}
    g = _groww()
    today = today or dt.date.today()
    T = max((info["expiry"] - today).days, 0) / 365.0
    S = underlying_ltp(info["underlying"], g)
    prem = option_ltp(sym, g)
    rv20 = realized_vol(info["underlying"], 20)
    rv60 = realized_vol(info["underlying"], 60)
    out = {"symbol": sym, **{k: (v.isoformat() if isinstance(v, dt.date) else v) for k, v in info.items()},
           "days_to_expiry": (info["expiry"] - today).days, "spot": S, "premium": prem,
           "rv20": rv20, "rv60": rv60, "india_vix": india_vix()}
    if S and prem and T > 0:
        iv = implied_vol(prem, S, info["strike"], T, R_FREE, info["kind"])
        out["iv"] = iv
        if iv:
            gk = greeks(S, info["strike"], T, R_FREE, iv, info["kind"])
            sign = -1 if side == "short" else 1
            out["greeks_per_share"] = {k: round(v, 5) for k, v in gk.items()}
            out["position_greeks"] = {k: round(sign * v * (qty or 0), 2) for k, v in gk.items()}
            if rv20:
                vrp = iv - rv20                          # variance/vol risk premium
                out["iv_vs_rv20"] = round(vrp, 4)
                out["vol_verdict"] = ("RICH (IV>>RV -> favour SHORT vol / butterflies)" if vrp > 0.04
                                      else "CHEAP (IV<<RV -> favour LONG vol / straddle)" if vrp < -0.04
                                      else "FAIR (IV~RV)")
    return out


def _fmt_pct(x):
    return f"{x*100:.1f}%" if isinstance(x, (int, float)) else "—"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--opt", help="option trading symbol e.g. TCS26JUL2000PE")
    ap.add_argument("--side", default="long", choices=["long", "short"])
    ap.add_argument("--qty", type=int, default=0)
    ap.add_argument("--underlying", help="just show IV-regime context for an underlying")
    a = ap.parse_args()
    vix = india_vix()
    print(f"India VIX (market IV regime): {vix:.2f}" if vix else "India VIX: n/a")
    if a.underlying:
        for w in (20, 60):
            print(f"  {a.underlying} realized vol {w}d: {_fmt_pct(realized_vol(a.underlying, w))}")
        return
    if not a.opt:
        print("pass --opt <SYMBOL> [--side short --qty 450] or --underlying <SYM>"); return
    r = analyze_option(a.opt, a.side, a.qty)
    if r.get("error"):
        print(r["error"]); return
    print(f"\n{a.opt}  ({a.side} {a.qty})   {r['underlying']} {r['right']} {int(r['strike'])}  exp {r['expiry']} ({r['days_to_expiry']}d)")
    print(f"  spot {r['spot']}   premium {r['premium']}")
    print(f"  IV {_fmt_pct(r.get('iv'))}   RV20 {_fmt_pct(r['rv20'])}   RV60 {_fmt_pct(r['rv60'])}   "
          f"IV-RV20 {(_fmt_pct(r.get('iv_vs_rv20')))}")
    if r.get("vol_verdict"):
        print(f"  VERDICT: {r['vol_verdict']}")
    if r.get("position_greeks"):
        g = r["position_greeks"]
        print(f"  position greeks ({a.side} {a.qty}):  delta {g['delta']:+.1f}  "
              f"gamma {g['gamma']:+.3f}  vega {g['vega']:+.1f}/volpt  theta {g['theta']:+.1f}/day")
        gs = r["greeks_per_share"]
        print(f"  per-share:  delta {gs['delta']:+.3f}  vega {gs['vega']:+.4f}  theta {gs['theta']:+.4f}")


if __name__ == "__main__":
    main()
