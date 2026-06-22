"""
marleg_expiry_matrix.py — compare the SAME option idea across EVERY live expiry, for a fixed HOLDING
PERIOD, as one P&L matrix with the best stop. Groww-only (no yfinance).

The problem it solves: the suggestion engine re-picks minute by minute. This anchors the decision to a
HORIZON (1 week / 2 weeks) and asks the real question: for a NIFTY 24200 CE view, which EXPIRY should I
hold — the cheap near weekly (more gamma, brutal theta) or a further one (steadier, costs more)? And
where's the stop?

For each live expiry at a chosen strike it computes, at the END of a 1wk and 2wk hold:
  • the option's P&L under spot scenarios (−2σ … +2σ, σ scaled to the hold from realized vol)
  • the theta cost if the underlying just sits flat (the bleed you pay to wait)
  • a structural STOP = the underlying at pivot S1 (support) → the option's value there → max loss
  • a TARGET = pivot R1/R2 (resistance) → gain → reward:risk
Then it ranks expiries for each horizon and recommends one (near enough for convexity, far enough to dodge
the theta cliff). Live expiries are DISCOVERED from Groww (weeklies = Tuesdays, monthlies = 3-letter),
probed for a real quote — no hard-coded calendar.

Read-only. Decision-support, not investment advice.

  python marleg_expiry_matrix.py NIFTY
  python marleg_expiry_matrix.py NIFTY --strike 24200
"""
import math
import datetime as dt

import marleg_vol as mv
import marleg_options_monitor as mom
import marleg_pivots as pv
import marleg_instruments as inst


def _realized(und, window=20, exchange="NSE"):
    """20d realized vol from GROWW daily candles (no yfinance; exchange-aware for BSE indices)."""
    try:
        import groww_client as gc
        import statistics
        g = gc.GrowwClient(); g.token()
        df = g.candles(und.upper(), interval_min=1440, days=window + 18, segment="CASH", exchange=exchange)
        c = df["close"].dropna() if df is not None and "close" in df.columns else None
        if c is None or len(c) < window + 1:
            return None
        rets = [math.log(c.iloc[i] / c.iloc[i - 1]) for i in range(1, len(c)) if c.iloc[i - 1] > 0]
        return statistics.pstdev(rets[-window:]) * math.sqrt(252) if len(rets) >= window else None
    except Exception:
        return None


def _spot(und, exchange="NSE"):
    """Live underlying/index spot, exchange-aware (BSE_SENSEX etc.); yfinance fallback inside underlying_ltp."""
    g = mom._g()
    if g:
        tried = []
        for ex in (exchange, "NSE", "BSE"):
            if ex in tried:
                continue
            tried.append(ex)
            try:
                rr = g.ltp(und, exchange=ex)
                p = (rr.json().get("payload") or {})
                for v in p.values():
                    if v:
                        return float(v)
            except Exception:
                pass
    try:
        return mv.underlying_ltp(und, g)
    except Exception:
        return None


def _live_expiries(und, strike, kind, exchange="NSE", within_days=160):
    """Every LIVE expiry from Groww's instruments master (no guessing) whose contract actually quotes."""
    found = []
    for e in inst.expiries(und, within_days=within_days):
        c = inst.contract(und, e, strike, kind)
        if not c:
            continue
        q = mom.option_quote(c["symbol"], exchange=c.get("exchange") or exchange)
        if isinstance(q, dict) and "error" not in q and (q.get("ltp") or 0) > 0:
            found.append((dt.date.fromisoformat(e), c["symbol"], q))
    return found


def _stance(expired, theta_flat, up1, oi, hold):
    """BUY / OK / CAUTION / AVOID for this expiry over the hold — judged on TIME COMPLEXITY (theta), with a
    footnote explaining the call."""
    if expired:
        return "AVOID", (f"Expires during a {hold}-session hold — a theta cliff: it decays to intrinsic mid-hold, "
                         "so a slow move loses even when you're right on direction.")
    if (oi or 0) < 200:
        return "AVOID", "Too thin (OI < 200) — hard to exit cleanly; slippage eats the edge."
    if theta_flat <= -50:
        return "AVOID", (f"Bleeds {theta_flat:.0f}% if it goes nowhere over the hold — time complexity too high; "
                         "the move must be both fast and big just to break even.")
    if theta_flat <= -28:
        return "CAUTION", f"Survivable but theta-heavy ({theta_flat:.0f}% if flat) — only if you expect the move soon."
    if up1 >= 35:
        return "BUY", (f"Right time structure for a {hold}-session hold: strong convexity (+{up1:.0f}% on a +1σ "
                       f"move) with a tolerable {theta_flat:.0f}% flat-bleed.")
    return "OK", f"Balanced (+{up1:.0f}% on +1σ, {theta_flat:.0f}% if flat) — fine but not standout for {hold} sessions."


def matrix(underlying, strike=None, kind="C", holds=(5, 10), n_sigma=2):
    und = (underlying or "NIFTY").upper().strip()
    if not inst.has_options(und):
        return {"ok": False, "error": f"{und} has no listed options in the Groww master"}
    exchange = inst.exchange_of(und)
    spot = _spot(und, exchange)
    if not spot:
        return {"ok": False, "error": f"no spot for {und}"}
    # canonical strike = a REAL listed strike nearest spot (from the master), so it exists across expiries
    front = (inst.expiries(und, max_n=1) or [None])[0]
    grid = inst.strikes(und, front) if front else []
    if strike:
        strike = float(strike)
    elif grid:
        strike = min(grid, key=lambda x: abs(x - spot))
    else:
        step = mom.INDEX_STEP.get(und) or mom._strike_step(spot)
        strike = round(spot / step) * step
    r = mv.R_FREE
    rv = _realized(und, exchange=exchange) or 0.12
    piv = pv.pivots(und, spot=spot)
    cl = piv["methods"]["classic"] if piv.get("ok") else {}
    stop_lvl = cl.get("S1")                       # structural stop = pivot S1 (nearest support)
    tgt1, tgt2 = cl.get("R1"), cl.get("R2")       # targets = pivot R1 / R2 (resistance)

    lives = _live_expiries(und, strike, kind, exchange=exchange)
    if not lives:
        return {"ok": False, "error": f"no live {und} {int(strike)}{('CE' if kind=='C' else 'PE')} contracts found"}

    today = dt.date.today()
    # scenario labels per hold (σ over the hold)
    sigma_by_hold = {h: spot * rv * math.sqrt(h / 365.0) for h in holds}

    def reprice(premium_iv, K, dte_after, S):
        if dte_after <= 0:
            return max(S - K, 0.0) if kind == "C" else max(K - S, 0.0)
        return mv.bs_price(S, K, dte_after / 365.0, r, premium_iv, kind)

    rows = []
    for e, sym, q in lives:
        dte = (e - today).days
        prem = q.get("ltp")
        iv = q.get("iv")
        if not iv and prem:
            iv, _ = mv.implied_vol_newton(prem, spot, strike, max(dte, 1) / 365.0, r, kind)
        iv = iv or rv
        gk = mv.fd_greeks(spot, strike, max(dte, 1) / 365.0, r, iv, kind)
        row = {"expiry": e.isoformat(), "symbol": sym, "dte": dte,
               "is_monthly": e == mv.monthly_expiry(e.year, e.month),
               "premium": round(prem, 2), "iv_pct": round(iv * 100, 1),
               "delta": round(gk["delta"], 3), "theta_day": round(gk["theta"], 2),
               "oi": q.get("oi"), "spread_pct": q.get("spread_pct"), "holds": {}}
        for h in holds:
            sg = sigma_by_hold[h]
            dte_after = dte - h
            expired = dte_after <= 0
            scen = []
            for mult in range(-n_sigma, n_sigma + 1):
                S = spot + mult * sg
                val = reprice(iv, strike, dte_after, S)
                scen.append({"sigma": mult, "spot": round(S, 1), "move_pct": round((S / spot - 1) * 100, 2),
                             "opt": round(val, 2), "pnl_pct": round((val / prem - 1) * 100, 0)})
            flat = reprice(iv, strike, dte_after, spot)
            theta_cost_pct = round((flat / prem - 1) * 100, 0)                # bleed if flat over the hold
            # structural stop @ pivot S1 (priced at end of hold), target @ pivot R1
            stop_opt = reprice(iv, strike, dte_after, stop_lvl) if stop_lvl else None
            stop_loss_pct = round((stop_opt / prem - 1) * 100, 0) if stop_opt is not None else None
            tgt_opt = reprice(iv, strike, dte_after, tgt1) if tgt1 else None
            tgt_gain_pct = round((tgt_opt / prem - 1) * 100, 0) if tgt_opt is not None else None
            rr = round(abs(tgt_gain_pct) / abs(stop_loss_pct), 2) if (tgt_gain_pct and stop_loss_pct) else None
            up1 = next((s["pnl_pct"] for s in scen if s["sigma"] == 1), 0)
            stance, footnote = _stance(expired, theta_cost_pct, up1, row.get("oi"), h)
            row["holds"][str(h)] = {"expired_in_hold": expired, "scenarios": scen,
                                    "theta_cost_flat_pct": theta_cost_pct, "stance": stance, "footnote": footnote,
                                    "stop": {"level": stop_lvl, "opt": round(stop_opt, 2) if stop_opt is not None else None,
                                             "loss_pct": stop_loss_pct},
                                    "target": {"level": tgt1, "opt": round(tgt_opt, 2) if tgt_opt is not None else None,
                                               "gain_pct": tgt_gain_pct},
                                    "rr": rr}
        rows.append(row)

    # recommendation per horizon: best (P&L at +1σ) − (theta bleed if flat), among expiries that DON'T
    # expire inside the hold and have real OI. Captures convexity-vs-cliff trade-off.
    rec = {}
    for h in holds:
        def _up1(hh):
            return next((s["pnl_pct"] for s in hh["scenarios"] if s["sigma"] == 1), 0)
        tradeable = [(row, row["holds"][str(h)]) for row in rows
                     if not row["holds"][str(h)]["expired_in_hold"] and (row.get("oi") or 0) >= 200]
        # prefer expiries that SURVIVE a flat tape (don't bleed worse than ~-35% if it goes nowhere) — that's
        # the point of choosing a hold; among those, take the best upside on a +1σ move. Fall back if none.
        survivable = [(row, hh) for row, hh in tradeable if hh["theta_cost_flat_pct"] >= -35]
        pool = survivable or tradeable
        if pool:
            pool.sort(key=lambda x: -_up1(x[1]))
            row, hh = pool[0]
            rec[str(h)] = {"expiry": row["expiry"], "symbol": row["symbol"], "dte": row["dte"],
                           "pnl_at_+1sigma_pct": _up1(hh), "theta_if_flat_pct": hh["theta_cost_flat_pct"],
                           "stop_loss_pct": hh["stop"]["loss_pct"], "rr": hh["rr"],
                           "why": f"best upside on a +1σ move among expiries that survive a flat tape "
                                  f"(≤35% bleed) over {h} sessions — dodges the near-expiry theta cliff"}
    horizon_verdict = ("For a 1–2 week hold the math favours expiries ~3–5 weeks out: nearly the same direction "
                       "payoff, but a fraction of the theta bleed if the move takes time. The current weekly is a "
                       "high-convexity lottery — great on an immediate move, near-total loss if it stalls.")

    return {"ok": True, "underlying": und, "spot": round(spot, 2), "strike": strike, "kind": kind,
            "right": "CE" if kind == "C" else "PE", "realized_vol_pct": round(rv * 100, 1),
            "holds": list(holds), "sigma_by_hold": {str(h): round(v, 1) for h, v in sigma_by_hold.items()},
            "pivots": {"S1": stop_lvl, "R1": tgt1, "R2": tgt2, "P": cl.get("P")},
            "exchange": exchange, "lot": inst.lot_size(und),
            "n_expiries": len(rows), "matrix": rows, "recommendation": rec, "horizon_verdict": horizon_verdict,
            "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
            "note": "Same strike across every LIVE expiry, P&L at the END of a 1wk/2wk hold (σ scaled to the hold "
                    "from Groww realized vol; each expiry repriced with its own IV held constant). Stop = pivot S1, "
                    "target = pivot R1. Near weeklies give more % on a move but bleed faster if flat.",
            "caveat": "Decision-support, not investment advice — I'm not a licensed advisor. Read-only."}


if __name__ == "__main__":
    import sys, argparse
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("underlying", nargs="?", default="NIFTY")
    ap.add_argument("--strike", type=float)
    ap.add_argument("--put", action="store_true")
    a = ap.parse_args()
    r = matrix(a.underlying, a.strike, kind="P" if a.put else "C")
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n  {r['underlying']} spot ₹{r['spot']} · {int(r['strike'])}{r['right']} · RV {r['realized_vol_pct']}% · "
          f"pivots S1 ₹{r['pivots']['S1']} / R1 ₹{r['pivots']['R1']} · {r['asof']}")
    for h in r["holds"]:
        sg = r["sigma_by_hold"][str(h)]
        print(f"\n  ══ {h}-session hold (±1σ ≈ ±{sg} pts) ══")
        hdr = "  {:>11} {:>5} {:>7} {:>6}".format("expiry", "dte", "prem", "θflat")
        labels = ["-2σ", "-1σ", "flat", "+1σ", "+2σ"]
        print(hdr + "".join("{:>8}".format(l) for l in labels) + "   stop  R:R")
        for row in r["matrix"]:
            hh = row["holds"][str(h)]
            pnls = [s["pnl_pct"] for s in hh["scenarios"]]
            tag = "M" if row["is_monthly"] else "w"
            line = "  {:>9}{} {:>5} {:>7} {:>5}%".format(row["expiry"][5:], tag, row["dte"], row["premium"], hh["theta_cost_flat_pct"])
            line += "".join("{:>7}%".format(p) for p in pnls)
            line += "  {:>4}% {:>4}".format(hh["stop"]["loss_pct"], hh["rr"] if hh["rr"] else "-")
            print(line)
        rc = r["recommendation"].get(str(h))
        if rc:
            print(f"   ➤ best for {h}d: {rc['symbol']}  (+1σ {rc['pnl_at_+1sigma_pct']}% · flat {rc['theta_if_flat_pct']}% · stop {rc['stop_loss_pct']}% · R:R {rc['rr']})")
    print(f"\n  {r['caveat']}")
