"""
marleg_execution.py — buy/sell in BULK harmoniously, without moving the market against yourself
("not losing big on big lots"). This is the optimal-execution / order-slicing problem.

The one number that decides everything is Q / ADV — your order size vs the stock's average daily volume:
  • small (<5% ADV)   -> you're invisible; slice over the day at a low participation rate, ~spread-only cost.
  • large (>20% ADV)  -> you WILL move the price; spread it over several days or you eat the book.
We model market impact with the square-root law (Almgren et al.):  impact ≈ η · σ · sqrt(participation),
and compare DUMP-IT-NOW vs WORK-IT-SLICED so you can see the rupees saved. We emit a TWAP/VWAP child-order
schedule and an Almgren-Chriss front-load knob for urgency.

MODELLING ONLY. This NEVER places, modifies, or cancels an order — it hands YOU the schedule to follow.

  python marleg_execution.py RELIANCE 50000 SELL
"""
import sys
import math

ETA = 0.6                 # square-root market-impact coefficient (empirical ~0.3-1.0)
DAY_MIN = 375.0           # NSE session minutes (09:15-15:30)
DUMP_FRAC = 0.05          # "dump it now" ≈ consuming one ~5%-of-day liquidity slug immediately
SAFE_POV = 0.10           # a child-order participation that stays ~invisible

# typical NSE intraday volume U-curve over 13 half-hour buckets (open & close heavy) — for VWAP slicing
VWAP_CURVE = [0.128, 0.092, 0.073, 0.062, 0.058, 0.055, 0.052, 0.055, 0.060, 0.068, 0.080, 0.097, 0.120]


def _liq(tk):
    import marleg_vol as mv
    import yfinance as yf
    h = yf.Ticker(mv._yf_symbol(tk.upper())).history(period="3mo")
    h = h[["Close", "Volume"]].dropna()
    if len(h) < 20:
        return None
    spot = float(h["Close"].iloc[-1])
    adv = float(h["Volume"].iloc[-20:].mean())
    sig = mv.realized_vol(tk, 20) or 0.30
    return spot, adv, sig / math.sqrt(252.0)        # spot, ADV shares, daily sigma (fraction)


def _spread_bps(turnover_cr):
    return max(1.0, min(60.0, 1.5 + 40.0 / math.sqrt(max(turnover_cr, 0.05))))


def plan(tk, qty=None, notional=None, side="SELL", horizon_min=DAY_MIN, pov=SAFE_POV, urgency=0.5):
    tk = (tk or "").upper()
    liq = _liq(tk)
    if not liq:
        return {"ok": False, "error": f"no liquidity data for {tk}"}
    spot, adv, sig_day = liq
    if qty is None and notional:
        qty = int(notional / spot)
    if not qty or qty <= 0:
        return {"ok": False, "error": "give a share qty or a notional"}
    qty = int(qty)
    notional = qty * spot
    turnover_cr = adv * spot / 1e7
    X = qty / adv                                    # order as a fraction of ADV (the key number)
    half_sp = _spread_bps(turnover_cr) / 2.0
    sig_bps = sig_day * 1e4

    def impact_bps(participation, full_spread=False):
        sp = (half_sp * 2 if full_spread else half_sp)
        return sp + ETA * sig_bps * math.sqrt(max(participation, 1e-6))

    # DUMP now vs WORK it over the horizon
    part_dump = X / DUMP_FRAC
    t_frac = max(min(horizon_min / DAY_MIN, 1.0), 0.02)
    part_work = X / t_frac
    dump_bps = impact_bps(part_dump, full_spread=True)
    work_bps = impact_bps(part_work, full_spread=False)
    dump_rs = dump_bps / 1e4 * notional
    work_rs = work_bps / 1e4 * notional
    saved_rs = max(dump_rs - work_rs, 0)

    # liquidity verdict: days to unwind at a safe participation
    days_safe = X / pov                              # at `pov` of each day's volume
    if days_safe <= (horizon_min / DAY_MIN) * 1.05:
        liq_verdict = f"comfortable — {X*100:.1f}% of ADV; exits inside the {round(horizon_min)}-min window at ≤{round(pov*100)}% of volume"
    elif days_safe <= 1.0:
        liq_verdict = f"OK in one session — {X*100:.1f}% of ADV; work it the full day at ~{round(pov*100)}% of volume"
    else:
        liq_verdict = f"BIG — {X*100:.1f}% of ADV; spread over ~{math.ceil(days_safe)} sessions at ≤{round(pov*100)}% of volume or you'll move it"

    # child-order schedule across the horizon (cap a single session)
    n = max(2, min(13, round(horizon_min / 30)))
    today_qty = int(min(qty, pov * adv * t_frac)) if days_safe > 1.0 else qty
    twap = today_qty / n
    cur = VWAP_CURVE[:n]; cs = sum(cur) or 1.0
    sched = []
    for i in range(n):
        sched.append({"slice": i + 1, "twap_qty": int(round(twap)),
                      "vwap_qty": int(round(today_qty * cur[i] / cs)),
                      "minute": int((i + 0.5) * horizon_min / n)})
    # Almgren-Chriss urgency: front-load when you fear an adverse move (or are big & want out)
    kappa = 0.6 + 2.4 * max(0.0, min(urgency, 1.0))
    w = [math.exp(-kappa * (i / max(n - 1, 1))) for i in range(n)]
    ws = sum(w)
    for i, s in enumerate(sched):
        s["urgent_qty"] = int(round(today_qty * w[i] / ws))

    return {"ok": True, "tk": tk, "side": side.upper(), "spot": round(spot, 2), "qty": qty,
            "notional": round(notional), "adv_shares": int(adv), "adv_turnover_cr": round(turnover_cr, 1),
            "pct_of_adv": round(X * 100, 1), "sigma_day_pct": round(sig_day * 100, 2),
            "spread_bps_est": round(half_sp * 2, 1), "horizon_min": round(horizon_min),
            "participation_if_worked_pct": round(min(part_work, 5) * 100), "target_pov_pct": round(pov * 100),
            "days_to_unwind": round(days_safe, 2), "liquidity": liq_verdict,
            "cost_dump_bps": round(dump_bps), "cost_dump_rs": round(dump_rs),
            "cost_worked_bps": round(work_bps), "cost_worked_rs": round(work_rs), "slippage_saved_rs": round(saved_rs),
            "today_qty": today_qty, "n_slices": n, "schedule": sched,
            "verdict": (f"{side.upper()} {qty:,} {tk} (₹{round(notional):,}) = {X*100:.1f}% of ADV. Dumping it ≈ "
                        f"{round(dump_bps)} bps (₹{round(dump_rs):,}); working it sliced ≈ {round(work_bps)} bps "
                        f"(₹{round(work_rs):,}) — about ₹{round(saved_rs):,} saved. {liq_verdict}."),
            "note": "Square-root impact model (impact ≈ η·σ·√participation); estimate, not a guarantee. TWAP = "
                    "even, VWAP = follow the volume curve, urgent = Almgren-Chriss front-load. MODELLING ONLY — "
                    "place the child orders yourself; this never trades."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    qty = int(sys.argv[2]) if len(sys.argv) > 2 else 50000
    side = sys.argv[3] if len(sys.argv) > 3 else "SELL"
    r = plan(tk, qty=qty, side=side)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n{r['side']} {r['qty']:,} {r['tk']}  @ ~₹{r['spot']}  (₹{r['notional']:,})")
    print(f"  ADV {r['adv_shares']:,} sh (₹{r['adv_turnover_cr']}cr/day) · this order = {r['pct_of_adv']}% of ADV · σ {r['sigma_day_pct']}%/day · spread ~{r['spread_bps_est']}bps")
    print(f"  LIQUIDITY: {r['liquidity']}")
    print(f"  DUMP now  ≈ {r['cost_dump_bps']} bps  (₹{r['cost_dump_rs']:,})")
    print(f"  WORK it   ≈ {r['cost_worked_bps']} bps  (₹{r['cost_worked_rs']:,})   ->  ~₹{r['slippage_saved_rs']:,} saved")
    print(f"  schedule ({r['n_slices']} slices of {r['today_qty']:,} sh today):")
    for s in r["schedule"]:
        print(f"    +{s['minute']:>3}min   TWAP {s['twap_qty']:>7,}   VWAP {s['vwap_qty']:>7,}   urgent {s['urgent_qty']:>7,}")
    print(f"\n  {r['verdict']}")
