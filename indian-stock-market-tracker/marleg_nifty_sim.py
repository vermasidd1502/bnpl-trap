"""
Marle-G — NIFTY OPTIONS STRATEGY TOURNAMENT.

Single-instrument discipline: backtest a roster of Nifty options strategies over years,
tally one scorecard, and rank + letter-grade them. Each strategy is run on WEEKLY cycles
and priced with Black-Scholes using Nifty spot (^NSEI) + India VIX (^INDIAVIX) as the IV.

ROSTER
  premium-selling : short straddle · short strangle · iron condor · iron butterfly
                    · short straddle (+stop/target) · short strangle (+stop) · VRP-filtered strangle
  directional     : squeeze-gated debit spread (compression + momentum)
  long-vol / tail : long straddle · long strangle
  hybrid          : regime-switched (sell premium when calm, long vol when stressed)
  benchmark       : buy & hold Nifty (the bar to beat)
Each option strategy is run at BOTH entry timings — at the close (EOD, bet overnight) and
at the open — so the EOD-vs-open question is answered by the data, not asserted.

SCORECARD  CAGR · Sharpe · Sortino · max drawdown · Calmar · win-rate · avg win/loss
           · expectancy · profit factor · worst week · % time in market -> A-F grade, ranked.

HONEST CAVEAT (shown in the UI): options are BS-priced with VIX as the implied vol — no
volatility skew/smile, no real bid-ask spread, and VIX is a 30-day measure used for weekly
options. So absolute P&L is APPROXIMATE; the RELATIVE RANKING across strategies is the
trustworthy output. Returns are per-cycle P&L as a fraction of spot (unleveraged notional).

  python marleg_nifty_sim.py
"""
import os, sys, json, math
import numpy as np, pandas as pd, yfinance as yf

R = 0.065          # India risk-free (approx); dividends ignored (minor, cancels in ranking)
STEP = 5           # trading days per weekly cycle
WPY = 52           # weeks/year (annualization)


# ---------------------------------------------------------------- Black-Scholes
def _N(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs(S, K, T, sig, cp):
    if T <= 0 or sig <= 0 or S <= 0 or K <= 0:
        return max(0.0, (S - K) if cp == "C" else (K - S))
    d1 = (math.log(S / K) + (R + 0.5 * sig * sig) * T) / (sig * math.sqrt(T))
    d2 = d1 - sig * math.sqrt(T)
    if cp == "C":
        return S * _N(d1) - K * math.exp(-R * T) * _N(d2)
    return K * math.exp(-R * T) * _N(-d2) - S * _N(-d1)


# ---------------------------------------------------------------- strike helpers
def _atm(S):
    return round(S / 50.0) * 50


def _otm(S, vix, T, ksig):
    pts = S * vix * math.sqrt(max(T, 1e-6))      # 1 SD move over the cycle
    return round((S + ksig * pts) / 50.0) * 50


# ---------------------------------------------------------------- strategy roster
def _sqz(ctx, i, S, v, T):
    bw, mom = ctx["bw_pctile"][i], ctx["mom10"][i]
    if np.isnan(bw) or np.isnan(mom):
        return None
    if bw < 0.30 and mom > 0.01:                 # compressed + up -> bull call debit spread
        return [("C", _atm(S), +1), ("C", _otm(S, v, T, 1), -1)]
    if bw < 0.30 and mom < -0.01:                # compressed + down -> bear put debit spread
        return [("P", _atm(S), +1), ("P", _otm(S, v, T, -1), -1)]
    return None                                   # else stay flat


def _regime(ctx, i, S, v, T):
    vp = ctx["vix_pctile"][i]
    if np.isnan(vp):
        return None
    if vp < 0.40:                                 # calm -> harvest premium
        return [("C", _otm(S, v, T, 1), -1), ("P", _otm(S, v, T, -1), -1)]
    if vp > 0.75:                                 # stressed -> own vol
        return [("C", _atm(S), +1), ("P", _atm(S), +1)]
    return [("C", _atm(S), -1), ("P", _atm(S), -1)]   # mixed -> short straddle


def _vrp(ctx, i, S, v, T):
    rv = ctx["rv20"][i]
    if np.isnan(rv) or v <= rv * 1.05:           # only sell when IV richer than realized
        return None
    return [("C", _otm(S, v, T, 1), -1), ("P", _otm(S, v, T, -1), -1)]


STRATS = [
    {"name": "Short Straddle", "family": "sell", "kind": "option",
     "legs": lambda ctx, i, S, v, T: [("C", _atm(S), -1), ("P", _atm(S), -1)]},
    {"name": "Short Strangle", "family": "sell", "kind": "option",
     "legs": lambda ctx, i, S, v, T: [("C", _otm(S, v, T, 1), -1), ("P", _otm(S, v, T, -1), -1)]},
    {"name": "Iron Condor", "family": "sell", "kind": "option",
     "legs": lambda ctx, i, S, v, T: [("C", _otm(S, v, T, 1), -1), ("P", _otm(S, v, T, -1), -1),
                                      ("C", _otm(S, v, T, 2), +1), ("P", _otm(S, v, T, -2), +1)]},
    {"name": "Iron Butterfly", "family": "sell", "kind": "option",
     "legs": lambda ctx, i, S, v, T: [("C", _atm(S), -1), ("P", _atm(S), -1),
                                      ("C", _otm(S, v, T, 1), +1), ("P", _otm(S, v, T, -1), +1)]},
    {"name": "Short Straddle +stop", "family": "sell", "kind": "option", "target": 0.5, "stop": 1.5,
     "legs": lambda ctx, i, S, v, T: [("C", _atm(S), -1), ("P", _atm(S), -1)]},
    {"name": "Short Strangle +stop", "family": "sell", "kind": "option", "target": 0.5, "stop": 2.0,
     "legs": lambda ctx, i, S, v, T: [("C", _otm(S, v, T, 1), -1), ("P", _otm(S, v, T, -1), -1)]},
    {"name": "VRP-filtered Strangle", "family": "sell", "kind": "option", "legs": _vrp},
    {"name": "Squeeze Directional", "family": "directional", "kind": "option", "legs": _sqz},
    {"name": "Long Straddle", "family": "buy", "kind": "option",
     "legs": lambda ctx, i, S, v, T: [("C", _atm(S), +1), ("P", _atm(S), +1)]},
    {"name": "Long Strangle (tail)", "family": "buy", "kind": "option",
     "legs": lambda ctx, i, S, v, T: [("C", _otm(S, v, T, 1), +1), ("P", _otm(S, v, T, -1), +1)]},
    {"name": "Regime-Switched", "family": "hybrid", "kind": "option", "legs": _regime},
    {"name": "Buy & Hold Nifty", "family": "benchmark", "kind": "benchmark", "legs": lambda *a: None},
]


# ---------------------------------------------------------------- data + context
INDEX_SYM = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS", "NIFTYIT": "^CNXIT"}
VRP_SCALAR = 1.15      # sector IV proxy = 30d realized vol x typical vol-risk-premium (sectors have no VIX)


def _iv_label(u):
    return "India VIX" if u.upper() == "NIFTY" else "realized-vol x VRP proxy"


def _flat(x):
    if isinstance(x.columns, pd.MultiIndex):
        x.columns = x.columns.get_level_values(0)
    return x


def _load(underlying="NIFTY", period="6y"):
    sym = INDEX_SYM.get(underlying.upper(), "^NSEI")
    px = yf.download(sym, period=period, interval="1d", progress=False, auto_adjust=False)
    if px is None or px.empty:
        return pd.DataFrame()
    px = _flat(px)
    df = pd.DataFrame({"open": px["Open"], "close": px["Close"]}).dropna()
    if underlying.upper() == "NIFTY":
        vx = yf.download("^INDIAVIX", period=period, interval="1d", progress=False, auto_adjust=False)
        if vx is None or vx.empty:
            return pd.DataFrame()
        df["vix"] = _flat(vx)["Close"]                                       # real implied vol
    else:
        realized = df["close"].pct_change().rolling(30).std() * np.sqrt(252) * 100   # ann %, VIX-scale
        df["vix"] = realized * VRP_SCALAR                                    # IV proxy (no sector VIX)
    return df.dropna()


def _context(df):
    c = df["close"]
    rv20 = c.pct_change().rolling(20).std() * np.sqrt(252)
    mom10 = c / c.shift(10) - 1
    ma20, sd20 = c.rolling(20).mean(), c.rolling(20).std()
    bw = sd20 / ma20
    bw_pctile = bw.rolling(252, min_periods=60).apply(lambda s: (s <= s.iloc[-1]).mean(), raw=False)
    vix_pctile = df["vix"].rolling(252, min_periods=60).apply(lambda s: (s <= s.iloc[-1]).mean(), raw=False)
    return {"rv20": rv20.values, "mom10": mom10.values,
            "bw_pctile": bw_pctile.values, "vix_pctile": vix_pctile.values}


# ---------------------------------------------------------------- per-strategy simulation
def _simulate(c, o, v, dates, ctx, strat, timing):
    rets, entered = [], 0
    n = len(c)
    i = max(60, STEP)
    while i + STEP < n:
        ent, exp = i, i + STEP
        S0 = c[ent] if timing == "close" else o[ent]
        if strat["kind"] == "benchmark":
            rets.append(c[exp] / S0 - 1.0); entered += 1; i += STEP; continue
        vix0 = v[ent]
        T0 = max((dates[exp] - dates[ent]).days, 1) / 365.0
        legs = strat["legs"](ctx, ent, S0, vix0, T0)
        if not legs:
            i += STEP; continue
        entered += 1
        entry_cost = sum(q * bs(S0, K, T0, vix0, cp) for (cp, K, q) in legs)
        credit = abs(entry_cost) or 1e-9
        tgt, stp = strat.get("target"), strat.get("stop")
        pnl = None
        for d in range(ent + 1, exp + 1):
            S, vix = c[d], v[d]
            T = max((dates[exp] - dates[d]).days, 0) / 365.0
            if T <= 0:
                val = sum(q * max(0.0, (S - K) if cp == "C" else (K - S)) for (cp, K, q) in legs)
            else:
                val = sum(q * bs(S, K, T, vix, cp) for (cp, K, q) in legs)
            p = val - entry_cost
            if tgt and p >= tgt * credit:
                pnl = p; break
            if stp and p <= -stp * credit:
                pnl = p; break
        if pnl is None:
            pnl = p
        rets.append(pnl / S0)
        i += STEP
    return rets, entered


def _grade(sharpe, maxdd):
    if sharpe >= 1.5 and maxdd > -0.25:
        return "A"
    if sharpe >= 1.0:
        return "B"
    if sharpe >= 0.5:
        return "C"
    if sharpe >= 0.0:
        return "D"
    return "F"


def _score(rets, entered, total):
    a = np.array(rets, dtype=float)
    if len(a) < 5:
        return None
    mean, sd = a.mean(), (a.std(ddof=1) or 1e-9)
    downside = a[a < 0].std(ddof=1) if (a < 0).sum() > 1 else 1e-9
    sharpe = mean / sd * math.sqrt(WPY)
    sortino = mean / (downside or 1e-9) * math.sqrt(WPY)
    eq = np.cumprod(1 + a)
    cagr = eq[-1] ** (WPY / len(a)) - 1 if eq[-1] > 0 else -1
    peak = np.maximum.accumulate(eq)
    maxdd = float((eq / peak - 1).min())
    wins, losses = a[a > 0], a[a <= 0]
    pf = wins.sum() / abs(losses.sum()) if losses.sum() != 0 else float("inf")
    return {"cycles": len(a), "cagr": round(cagr * 100, 1), "sharpe": round(sharpe, 2),
            "sortino": round(sortino, 2), "maxdd": round(maxdd * 100, 1),
            "calmar": round(cagr / abs(maxdd), 2) if maxdd < 0 else None,
            "winrate": round(len(wins) / len(a) * 100), "avgwin": round(float(wins.mean()) * 100, 2) if len(wins) else 0,
            "avgloss": round(float(losses.mean()) * 100, 2) if len(losses) else 0,
            "pf": round(pf, 2) if pf != float("inf") else None,
            "expectancy": round(mean * 100, 3), "worst": round(float(a.min()) * 100, 1),
            "time_in_mkt": round(entered / total * 100) if total else 100,
            "grade": _grade(sharpe, maxdd),
            "equity": [round(float(x), 4) for x in eq[:: max(1, len(eq) // 80)]]}


# ---------------------------------------------------------------- tournament
def run_tournament(underlying="NIFTY", period="6y", timings=("close", "open")):
    df = _load(underlying, period)
    if df.empty or len(df) < 300:
        return {"error": f"insufficient {underlying.upper()} history (need a published index + IV source)"}
    ctx = _context(df)
    c, o, v, dates = df["close"].values, df["open"].values, df["vix"].values / 100.0, df.index
    total = len(range(max(60, STEP), len(c) - STEP, STEP))
    board = []
    for st in STRATS:
        tms = timings if st["kind"] == "option" else ("close",)
        for tm in tms:
            rets, entered = _simulate(c, o, v, dates, ctx, st, tm)
            sc = _score(rets, entered, total)
            if not sc:
                continue
            tag = f" ({tm[0].upper()})" if st["kind"] == "option" else ""
            board.append({"name": st["name"] + tag, "base": st["name"], "family": st["family"],
                          "timing": tm, **sc})
    board.sort(key=lambda x: -x["sharpe"])
    for rank, b in enumerate(board, 1):
        b["rank"] = rank
    # EOD-vs-open verdict (avg Sharpe across option strategies by timing)
    eod = [b["sharpe"] for b in board if b["timing"] == "close" and b["family"] != "benchmark"]
    opn = [b["sharpe"] for b in board if b["timing"] == "open"]
    timing_edge = None
    if eod and opn:
        de, do = float(np.mean(eod)), float(np.mean(opn))
        timing_edge = {"eod_avg_sharpe": round(de, 2), "open_avg_sharpe": round(do, 2),
                       "winner": "EOD (enter at close)" if de >= do else "Open (enter at open)"}
    return {"asof": str(dates[-1].date()), "period": period, "n_cycles": total,
            "underlying": underlying.upper(), "iv_source": _iv_label(underlying),
            "desc": f"{underlying.upper()} · weekly cycles · BS-priced from {_iv_label(underlying)}",
            "timing_edge": timing_edge, "leaderboard": board}


def current_position(underlying, strat_name, timing="close"):
    """The CURRENT open weekly position for a strategy on an underlying, marked to the latest price."""
    df = _load(underlying)
    if df.empty or len(df) < 300:
        return None
    ctx = _context(df)
    c, o, v, dates = df["close"].values, df["open"].values, df["vix"].values / 100.0, df.index
    strat = next((s for s in STRATS if s["name"] == strat_name and s["kind"] == "option"), None)
    if not strat:
        return None
    n = len(c); start = max(60, STEP)
    ent = start + ((n - 1 - start) // STEP) * STEP          # latest cycle-entry grid point
    exp = min(ent + STEP, n - 1)
    S0 = c[ent] if timing == "close" else o[ent]
    vix0 = v[ent]; T0 = max((dates[exp] - dates[ent]).days, 1) / 365.0
    legs = strat["legs"](ctx, ent, S0, vix0, T0)
    if not legs:
        return {"strategy": strat_name, "status": "flat — signal says stay out this cycle"}
    entry_cost = sum(q * bs(S0, K, T0, vix0, cp) for cp, K, q in legs)
    Snow, vnow = c[-1], v[-1]
    Tnow = max((dates[exp] - dates[-1]).days, 0) / 365.0
    if Tnow <= 0:
        valnow = sum(q * max(0.0, (Snow - K) if cp == "C" else (K - Snow)) for cp, K, q in legs)
    else:
        valnow = sum(q * bs(Snow, K, Tnow, vnow, cp) for cp, K, q in legs)
    pnl = valnow - entry_cost
    return {"strategy": strat_name, "timing": timing, "entry_date": str(dates[ent].date()),
            "entry_spot": round(float(S0), 1), "spot_now": round(float(Snow), 1),
            "premium": round(float(-entry_cost), 1),               # +ve = net credit received
            "legs": [{"opt": cp, "strike": int(K), "side": "SELL" if q < 0 else "BUY", "qty": abs(q)} for cp, K, q in legs],
            "mtm_pnl": round(float(pnl), 1), "mtm_pnl_pct_spot": round(float(pnl / S0 * 100), 2),
            "dte_days": max((dates[exp] - dates[-1]).days, 0)}


def sector_compare(sectors=("NIFTY", "BANKNIFTY", "FINNIFTY", "NIFTYIT")):
    """Tournament per sector index, pick the best option strategy, show this week's paper position."""
    out = []
    for u in sectors:
        t = run_tournament(underlying=u, timings=("close",))      # close-only for speed (timing ~ a tie)
        if t.get("error"):
            out.append({"underlying": u, "error": t["error"]}); continue
        lb = t["leaderboard"]
        best = next((b for b in lb if b["family"] != "benchmark"), lb[0])
        bench = next((b for b in lb if b["family"] == "benchmark"), None)
        out.append({"underlying": u, "iv_source": t.get("iv_source"), "asof": t["asof"],
                    "n_cycles": t["n_cycles"], "best": best, "benchmark": bench, "top3": lb[:3],
                    "current_position": current_position(u, best["base"], best["timing"])})
    return {"sectors": out, "note": "Sectors priced with a realized-vol VRP proxy (no sector VIX); relative ranking is the signal."}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if "--sectoral" in sys.argv:
        for s in sector_compare()["sectors"]:
            if s.get("error"):
                print(f"\n{s['underlying']}: {s['error']}"); continue
            b = s["best"]; p = s.get("current_position") or {}
            print(f"\n{s['underlying']:<10} best: {b['name']:<24} [{b['grade']}] Sharpe {b['sharpe']} CAGR {b['cagr']}%  ({s['iv_source']}, {s['n_cycles']} cyc)")
            if p.get("legs"):
                legs = ", ".join(f"{l['side']} {l['strike']}{l['opt']}" for l in p["legs"])
                print(f"           this week: {legs} · premium {p['premium']} · MTM {p['mtm_pnl']} ({p['mtm_pnl_pct_spot']}% spot) · DTE {p['dte_days']}d")
            elif p.get("status"):
                print(f"           this week: {p['status']}")
        return
    underlying = sys.argv[1] if (len(sys.argv) > 1 and not sys.argv[1].startswith("-")) else "NIFTY"
    r = run_tournament(underlying=underlying)
    if r.get("error"):
        print(r["error"]); return
    print(f"\n{r['underlying']} OPTIONS STRATEGY TOURNAMENT — {r['asof']} · {r['n_cycles']} weekly cycles ({r['period']})")
    print(f"  {r['desc']}\n")
    print(f"  {'#':>2}  {'STRATEGY':<26}{'GR':>3}{'SHARPE':>8}{'CAGR%':>8}{'MAXDD%':>8}{'WIN%':>6}{'IN%':>6}")
    print("  " + "-" * 70)
    for b in r["leaderboard"]:
        print(f"  {b['rank']:>2}  {b['name']:<26}{b['grade']:>3}{b['sharpe']:>8}{b['cagr']:>8}{b['maxdd']:>8}{b['winrate']:>6}{b['time_in_mkt']:>6}")
    if r.get("timing_edge"):
        te = r["timing_edge"]
        print(f"\n  ENTRY TIMING: EOD avg Sharpe {te['eod_avg_sharpe']} vs Open {te['open_avg_sharpe']}  ->  {te['winner']}")
    print("\n  BS-priced from VIX (no skew/spread) — relative ranking is the signal, not absolute P&L. Monitor-only; not advice.")


if __name__ == "__main__":
    main()
