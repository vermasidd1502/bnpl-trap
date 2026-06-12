"""
Marle-G — PRE-TRADE GATE. Run this BEFORE any order. It is the discipline, mechanized.

Seven checks, each mapped to the academic concept it enforces:
  1. TREND      above 50d & 200d MA              (cross-sectional momentum, Jegadeesh-Titman)
  2. VOLUME     ud(20d) > 1.3 accumulation        (volume precedes price; Wyckoff/O'Neil)
  3. SMART $    institutions not DISTRIBUTING     (informed vs noise traders, Grossman-Stiglitz)
  4. VOL CLASS  realized vol -> stop & SIZE       (vol targeting; stop outside daily noise)
  5. KELLY      leverage sanity: f* ~ mu/sigma^2  (over-betting makes growth negative)
  6. JUMP RISK  big single-day moves -> gaps      (Merton jumps: stops are not insurance, size is)
  7. EXPRESSION cash vs MTF vs options            (expression backtest: cash wins; MTF halves Sharpe)

The GATE is universal — same checks for every stock. The OUTPUT is per-stock — each name
gets its own stop distance (its ATR), its own max size (its vol), its own leverage permission
(its Kelly), its own holding style (its smart-money read). Long-side only.

  python marleg_pretrade.py TEJASNET                 # one name, full detail
  python marleg_pretrade.py --book                   # gate EVERY open position (live/file)
  python marleg_pretrade.py RELIANCE --capital 500000 --risk-pct 1.0
Monitor/education only — never places orders.
"""
import sys, math, argparse
import numpy as np
import pandas as pd
import yfinance as yf

GREEN, RED, YEL, END = "\033[92m", "\033[91m", "\033[93m", "\033[0m"


def _flag(ok, warn=False):
    return f"{YEL}CAUTION{END}" if warn else (f"{GREEN}PASS{END}" if ok else f"{RED}FAIL{END}")


def gate_data(tk, capital=200000.0, risk_pct=1.0, k_atr=2.5):
    """Compute every gate for one ticker. Returns a dict (or {'error': ...})."""
    tk = tk.upper().replace(".NS", "")
    try:
        d = yf.download(tk + ".NS", period="2y", interval="1d", progress=False, auto_adjust=False)
        if isinstance(d.columns, pd.MultiIndex):
            d.columns = d.columns.get_level_values(0)
        d = d.dropna()
    except Exception as e:
        return {"tk": tk, "error": str(e)[:80]}
    if len(d) < 220:
        return {"tk": tk, "error": "not enough history (delisted/renamed?)"}
    c, v, h, l = d["Close"], d["Volume"], d["High"], d["Low"]
    px = float(c.iloc[-1])
    sma50, sma200 = float(c.rolling(50).mean().iloc[-1]), float(c.rolling(200).mean().iloc[-1])
    r = c.pct_change()
    ud = float((v.where(r > 0, 0.0).rolling(20).sum() / v.where(r < 0, 0.0).rolling(20).sum().replace(0, np.nan)).iloc[-1])
    rv = float(r.tail(20).std() * math.sqrt(252) * 100)             # %/yr
    pc = c.shift(1)
    atr = float(pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1).rolling(14).mean().iloc[-1])
    atr_pct = atr / px * 100
    jumps = int((r.tail(252).abs() > 0.06).sum())
    mom6 = float(c.iloc[-1] / c.iloc[-126] - 1) * 100

    sm_verdict, sm_line = None, "unavailable (screener offline?)"
    try:
        import marleg_smartmoney as sm
        f = sm.flow(tk)
        if not f.get("error"):
            sm_verdict = f["verdict"]
            sm_line = (f"{f['verdict']} — FII Δ1Q {f['fii']['d1q']:+}%, DII Δ1Q {f['dii']['d1q']:+}%, "
                       f"promoters {f['promoter_action']} (as of {f['asof']})")
    except Exception:
        pass

    stop_dist = k_atr * atr
    stop_px = round(px - stop_dist, 1)
    risk_rs = capital * risk_pct / 100
    qty = int(risk_rs // stop_dist)
    g1 = px > sma50 and px > sma200
    g2 = ud > 1.3
    mu = 0.25 if (g1 and g2) else 0.10
    f_star = mu / ((rv / 100) ** 2) if rv > 0 else 0.0
    g3 = (sm_verdict != "DISTRIBUTING") if sm_verdict else None
    g4 = atr_pct < 4.0
    g6 = jumps <= 5
    surv, surv_note = "unknown", "surveillance list unreachable"
    try:
        import marleg_india_rules as ir
        surv, surv_note = ir.surveillance_check(tk)
    except Exception:
        pass

    hard_fails = [n for n, g in [("TREND", g1), ("VOLUME", g2)] if not g]
    if surv == "GSM":
        hard_fails.append("SURVEILLANCE(GSM)")
    cautions = []
    if surv == "ASM":
        cautions.append("ASM surveillance: 100% margin, tight bands — halve size, no leverage")
    # v1.2 ud zone (decade-ratified): 2.0-3.6 = go-to; decay past 3.6; >=6 event-suspect
    if np.isfinite(ud):
        if ud >= 6.0:
            cautions.append(f"ud {ud:.1f} EXTREME — event-suspect volume (10+ band: negative medians); not conviction")
        elif ud > 3.6:
            cautions.append(f"ud {ud:.1f} above go-to zone (2.0-3.6) — edge decays past 3.6")
    if g3 is False:
        cautions.append("smart$ distributing -> rental: calendar exit ~20d")
    if not g4:
        cautions.append(f"high vol -> stop {stop_dist/px*100:.0f}% wide, size shrinks")
    if f_star < 1.0:
        cautions.append("Kelly<1 -> NEVER leverage")
    if not g6:
        cautions.append(f"{jumps} jump-days -> gaps beat stops; size is the insurance")
    verdict = "NO TRADE" if hard_fails else ("REDUCED" if cautions else "TRADEABLE")
    return {"tk": tk, "px": px, "sma50": sma50, "sma200": sma200, "mom6": mom6, "ud": ud,
            "rv": rv, "sig_d": rv / math.sqrt(252), "atr": atr, "atr_pct": atr_pct,
            "jumps": jumps, "sm_verdict": sm_verdict, "sm_line": sm_line,
            "surv": surv, "surv_note": surv_note,
            "g1": g1, "g2": g2, "g3": g3, "g4": g4, "f_star": f_star, "g6": g6,
            "stop_px": stop_px, "stop_dist": stop_dist, "risk_rs": risk_rs, "qty": qty,
            "notional": qty * px, "hard_fails": hard_fails, "cautions": cautions,
            "verdict": verdict, "capital": capital, "risk_pct": risk_pct, "k_atr": k_atr}


def gate(tk, capital=200000.0, risk_pct=1.0, k_atr=2.5):
    g = gate_data(tk, capital, risk_pct, k_atr)
    if g.get("error"):
        print(f"{g['tk']}: {g['error']}"); return g
    print(f"\nPRE-TRADE GATE — {g['tk']}  @ {g['px']:.1f}   (capital ₹{capital:,.0f}, risk {risk_pct}%/trade)")
    print("-" * 76)
    print(f" 1 TREND      {_flag(g['g1'])}   vs 50d {g['px']/g['sma50']-1:+.1%} · vs 200d {g['px']/g['sma200']-1:+.1%} · 6m {g['mom6']:+.0f}%")
    print(f" 2 VOLUME     {_flag(g['g2'])}   ud(20d) {g['ud']:.2f}  (>1.3 = accumulation)")
    print(f" 3 SMART $    {_flag(bool(g['g3'])) if g['g3'] is not None else 'n/a    '}   {g['sm_line']}")
    print(f" 4 VOL CLASS  {_flag(g['g4'], warn=not g['g4'])}   RV {g['rv']:.0f}%/yr · daily σ {g['sig_d']:.1f}% · ATR {g['atr_pct']:.1f}%/day")
    print(f" 5 KELLY      {_flag(g['f_star'] >= 1.0, warn=g['f_star'] < 1.0)}   f* ≈ {g['f_star']:.2f}x -> "
          f"{'cash ok, NO leverage' if g['f_star'] < 1.5 else 'leverage tolerable'} (MTF 2x needs f*>1.5)")
    print(f" 6 JUMP RISK  {_flag(g['g6'], warn=not g['g6'])}   {g['jumps']} days >|6%| in the last year")
    print(f" 7 EXPRESSION cash delivery (expression backtest: cash Sharpe 1.11 > MTF 0.54 > calls -0.11)")
    sflag = _flag(False) if g['surv'] == 'GSM' else (_flag(True, warn=True) if g['surv'] == 'ASM'
            else ('n/a    ' if g['surv'] == 'unknown' else _flag(True)))
    print(f" 8 SURVEILL.  {sflag}   {g['surv_note']}")
    print("-" * 76)
    print(f" SIZE: stop {g['stop_px']} ({k_atr}xATR) · risk ₹{g['risk_rs']:,.0f} -> qty {g['qty']} "
          f"(~₹{g['notional']:,.0f}, {g['notional']/capital*100:.0f}% of capital)")
    if g["verdict"] == "NO TRADE":
        print(f" VERDICT: {RED}NO TRADE{END} — hard gate failed: {', '.join(g['hard_fails'])}")
    elif g["verdict"] == "REDUCED":
        print(f" VERDICT: {YEL}REDUCED/CONDITIONAL{END} — " + " | ".join(g["cautions"]))
    else:
        print(f" VERDICT: {GREEN}TRADEABLE{END} — cash, GTT stop at {g['stop_px']}, guardian watching")
    print(" (Research/education only — not advice. Never run on MTF what fails gate 5.)")
    return g


def book(capital=200000.0, risk_pct=1.0, k_atr=2.5):
    """Gate EVERY open position (live Groww read-only; my_positions.json fallback)."""
    from marleg_check_stops import load_positions
    positions, src = load_positions()
    eq = [p for p in positions if p.get("type") in ("EQ", "MTF") and (p.get("qty") or 0) > 0]
    print(f"\nBOOK GATE — {len(eq)} equity/MTF positions  |  source: {src}")
    if not eq:
        print("no equity/MTF positions found"); return
    mark = lambda x: ("?" if x is None else ("+" if x else "x"))
    print(f"{'SYMBOL':<14}{'verdict':<11}{'T V S':<7}{'ATR%':>5}{'f*':>6}{'jmp':>4}{'stop':>9}{'held':>6}{'gate qty':>9}  shape")
    print("-" * 96)
    for p in eq:
        g = gate_data(p.get("underlying") or p["symbol"], capital, risk_pct, k_atr)
        if g.get("error"):
            print(f"{p['symbol']:<14}{'NO DATA':<11}{'':<7}{'':>5}{'':>6}{'':>4}{'':>9}{(p.get('qty') or 0):>6}{'':>9}  {g['error']}")
            continue
        flags = f"{mark(g['g1'])} {mark(g['g2'])} {mark(g['g3'])}"
        col = RED if g["verdict"] == "NO TRADE" else (YEL if g["verdict"] == "REDUCED" else GREEN)
        held = p.get("qty") or 0
        oversized = held > g["qty"] > 0
        shape = "; ".join(g["cautions"][:2]) if g["cautions"] else ("hard fail: " + ",".join(g["hard_fails"]) if g["hard_fails"] else "clean")
        if p.get("type") == "MTF" and g["f_star"] < 1.5:
            shape = "MTF but Kelly bans leverage! " + shape
        if oversized:
            shape = f"held {held} > gate size {g['qty']}! " + shape
        print(f"{g['tk']:<14}{col}{g['verdict']:<11}{END}{flags:<7}{g['atr_pct']:>5.1f}{g['f_star']:>6.2f}{g['jumps']:>4}"
              f"{g['stop_px']:>9}{held:>6}{g['qty']:>9}  {shape}")
    print("-" * 96)
    print(" T V S = Trend / Volume / Smart-money (+ pass, x fail, ? n/a) · gate qty at "
          f"{risk_pct}% risk on ₹{capital:,.0f} · stop = {k_atr}xATR")
    print(" The gate is the same for every name; the SHAPE it demands is not. Not advice.")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker", nargs="?", default=None)
    ap.add_argument("--book", action="store_true", help="gate every open position")
    ap.add_argument("--capital", type=float, default=200000)
    ap.add_argument("--risk-pct", type=float, default=1.0)
    ap.add_argument("--k", type=float, default=2.5)
    a = ap.parse_args()
    if a.book or not a.ticker:
        book(a.capital, a.risk_pct, a.k)
    else:
        gate(a.ticker, a.capital, a.risk_pct, a.k)


if __name__ == "__main__":
    main()
