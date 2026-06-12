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


# ------------------------------------------------------------------ realized vol
def realized_vol(symbol, window=20):
    try:
        h = yf.Ticker(symbol + ".NS").history(period="4mo", interval="1d")["Close"].dropna()
        rets = (h / h.shift(1)).apply(lambda x: math.log(x) if x > 0 else 0).dropna()
        if len(rets) < window + 1:
            return None
        import statistics
        sd = statistics.pstdev(list(rets.iloc[-window:]))
        return sd * math.sqrt(TRADING_DAYS)
    except Exception:
        return None


def india_vix():
    try:
        v = yf.Ticker("^INDIAVIX").history(period="5d")["Close"].dropna()
        return float(v.iloc[-1]) if len(v) else None
    except Exception:
        return None


# ------------------------------------------------------------------ symbol parsing
MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}


def last_thursday(year, month):
    d = dt.date(year, 12, 31) if month == 12 else dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    while d.weekday() != 3:        # 3 = Thursday
        d -= dt.timedelta(days=1)
    return d


def parse_option(sym):
    """NSE monthly format UND + YY + MON + STRIKE + CE/PE  (e.g. TCS26JUL2000PE)."""
    m = re.match(r"^([A-Z&\-]+?)(\d{2})([A-Z]{3})(\d+)(CE|PE)$", sym.upper())
    if not m:
        return None
    und, yy, mon, strike, cp = m.groups()
    if mon not in MONTHS:
        return None
    expiry = last_thursday(2000 + int(yy), MONTHS[mon])
    return {"underlying": und, "strike": float(strike), "kind": "C" if cp == "CE" else "P",
            "expiry": expiry, "right": cp}


# ------------------------------------------------------------------ live prices
def _groww():
    try:
        import groww_client
        return groww_client.GrowwClient()
    except Exception:
        return None


def underlying_ltp(und, g):
    if g:
        try:
            r = g.ltp(und); p = (r.json().get("payload") or {})
            v = p.get(g.sym(und))
            if v:
                return float(v)
        except Exception:
            pass
    try:
        c = yf.Ticker(und + ".NS").history(period="1d")["Close"].dropna()
        return float(c.iloc[-1]) if len(c) else None
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
