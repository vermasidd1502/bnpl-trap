"""
marleg_scaling.py — climb IN and offload OUT systematically, without moving the price against yourself.

Two problems, two tools (people conflate them):
  IMPACT   — your order moves the price → solved by SLICING (keep each clip under ~POV% of live volume).
  CAMPAIGN — where to add / trim       → solved by a price LADDER (pyramid in on confirmation; scale OUT in
             tranches INTO resistance, where buyers cluster — never one dump into thin bids).
A trailing stop is a RISK tool, not an execution tool — it can't fix a fill.

This plans the whole campaign: an accumulation ladder (climb) + a distribution ladder (offload), each tranche
sized to liquidity, with the dump-vs-laddered slippage you save. MODEL ONLY — you place each tranche on Groww.

  python marleg_scaling.py RELIANCE 40000          # plan a 40k-share campaign
"""
import sys


def campaign(sym, total_qty=None, notional=None, entry=None, stop=None, target=None, pov=0.08):
    import marleg_execution as ex
    import marleg_levels as lv
    sym = sym.upper()
    liq = ex._liq(sym)
    if not liq:
        return {"ok": False, "error": f"no liquidity data for {sym}"}
    spot, adv, sig_day = liq
    atr = max(spot * sig_day, spot * 0.005)                      # ~1-day move unit
    L = lv.levels(sym)
    if entry is None or stop is None or target is None:
        if L.get("ok"):
            entry = entry or (L.get("nearest_resistance") or {}).get("price") or spot
            stop = stop or (L.get("nearest_support") or {}).get("price") or round(spot - 1.5 * atr, 1)
            target = target or round(spot + 3 * atr, 1)
    entry = entry or spot
    stop = stop if (stop and stop < entry) else round(entry - 1.5 * atr, 1)
    risk = max(entry - stop, atr)
    target = target or round(entry + 3 * risk, 1)
    res_above = sorted([x["price"] for x in (L.get("resistances") or []) if x["price"] > spot]) if L.get("ok") else []

    Q = int(total_qty) if total_qty else (int(notional / spot) if notional else None)
    if not Q or Q <= 0:
        return {"ok": False, "error": "give total_qty or notional"}
    frac_adv = Q / adv
    # a clean "clip" = pov of a ~5-min bar's volume (ADV/75). If a tranche exceeds it, it must be worked over minutes.
    clip5 = max(int(pov * adv / 75), 1)
    notes_for = lambda q: ("1 clip — fills clean" if q <= clip5 else
                           f"work over ~{int((q / max(clip5,1)) * 5)} min at ≤{int(pov*100)}% of volume ({q//max(clip5,1)} clips)")

    # ---- CLIMB IN (pyramid on confirmation; commit full size only once it's working) ----
    cin = [(0.40, round(spot, 1), "starter — now"),
           (0.35, round(max(entry, spot) + 0.5 * atr, 1), "add on the push / hold above the trigger"),
           (0.25, round(max(entry, spot) + 1.3 * atr, 1), "add on confirmation (it's working)")]
    climb = [{"pct": int(p * 100), "qty": int(round(Q * p)), "price": px, "when": w, "fill": notes_for(int(round(Q * p)))}
             for p, px, w in cin]

    # ---- OFFLOAD OUT (scale into resistance + target; trail the runner) ----
    pts = []
    pts.append((round(entry + 1.0 * risk, 1), "+1R — take the first slice off"))
    for r in res_above[:2]:
        if r > entry + 0.3 * risk:
            pts.append((round(r, 1), "into resistance ₹%g (sellers cluster here)" % round(r)))
    pts.append((round(target, 1), "target"))
    seen, upts = set(), []
    for px, w in pts:
        if px not in seen and px > spot:
            seen.add(px); upts.append((px, w))
    upts = sorted(upts)[:3]
    outpcts = [0.30, 0.30, 0.25][:len(upts)]
    offload = [{"pct": int(op * 100), "qty": int(round(Q * op)), "price": px, "when": w, "fill": notes_for(int(round(Q * op)))}
               for (px, w), op in zip(upts, outpcts)]
    trail_pct = 100 - sum(o["pct"] for o in offload)
    if trail_pct > 0:
        offload.append({"pct": trail_pct, "qty": int(round(Q * trail_pct / 100)), "price": None,
                        "when": "trail the runner with the stop — let the last piece ride", "fill": "exit when the trail hits"})

    # impact: dump it all vs work it
    plan = ex.plan(sym, qty=Q, side="SELL", pov=pov)
    return {"ok": True, "sym": sym, "spot": round(spot, 2), "total_qty": Q,
            "pct_of_adv": round(frac_adv * 100, 1), "adv_turnover_cr": round(adv * spot / 1e7, 1),
            "entry": round(entry, 1), "stop": round(stop, 1), "target": round(target, 1), "atr": round(atr, 1),
            "clip_5min": clip5, "pov_pct": int(pov * 100), "climb_in": climb, "offload_out": offload,
            "impact": {"dump_bps": plan.get("cost_dump_bps"), "worked_bps": plan.get("cost_worked_bps"),
                       "slippage_saved_rs": plan.get("slippage_saved_rs"), "liquidity": plan.get("liquidity")},
            "note": "Climb: pyramid on confirmation (don't commit full size until it works). Offload: scale OUT "
                    "INTO resistance/strength where bids absorb you — never dump into weakness. Each tranche fed "
                    "at <=POV% of live volume. MODEL ONLY — you place each clip on Groww."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sym = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    qty = int(sys.argv[2]) if len(sys.argv) > 2 else 40000
    r = campaign(sym, total_qty=qty)
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n  {r['sym']}  ₹{r['spot']}  ·  campaign {r['total_qty']:,} sh = {r['pct_of_adv']}% of ADV (₹{r['adv_turnover_cr']}cr/day)")
    print(f"  entry ₹{r['entry']} · stop ₹{r['stop']} · target ₹{r['target']} · clean clip ≈ {r['clip_5min']:,} sh / 5min at {r['pov_pct']}% POV")
    print(f"\n  CLIMB IN (accumulate):")
    for c in r["climb_in"]:
        print(f"    {c['pct']:>3}%  {c['qty']:>7,} sh @ ₹{c['price']:<9} {c['when']:<42} [{c['fill']}]")
    print(f"\n  OFFLOAD OUT (distribute):")
    for o in r["offload_out"]:
        print(f"    {o['pct']:>3}%  {o['qty']:>7,} sh @ {('₹'+str(o['price'])) if o['price'] else 'trail   ':<9} {o['when']:<42} [{o['fill']}]")
    im = r["impact"]
    print(f"\n  IMPACT: dump-it-all ≈ {im['dump_bps']}bps vs worked ≈ {im['worked_bps']}bps  ->  ~₹{im['slippage_saved_rs']:,} saved · {im['liquidity']}")
