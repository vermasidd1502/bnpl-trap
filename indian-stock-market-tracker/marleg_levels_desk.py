"""
marleg_levels_desk.py — the LEVELS & SMART-MONEY DESK for any stock / equity / index.

One call returns the full "where is resistance / support / where do the stops sit" picture, blending FOUR
independent level sources so you can read price like the desk does:

  1. PIVOTS (marleg_pivots)        — Classic/Fib/Camarilla Pivot + R1-R3 + S1-S3 from the prior session.
                                     The universally-watched, self-fulfilling level map (what Groww plots).
  2. VOLUME PROFILE (marleg_levels)— HVN / POC + swing pivots + structure: where trade actually happened
                                     (acceptance = strong S/R; the POC is the magnet).
  3. LIQUIDITY POOLS (marleg_liquidity) — BSL/SSL: swing H/L, prior day/week, equal H/L, round numbers.
                                     Where retail STOPS cluster = where price gets drawn to sweep them.
  4. ORDER BOOK L1/L2 (marleg_depth / Groww FNO) — the live 5-level bid/ask ladder: bid wall = support
                                     shelf / absorption, ask wall = overhead supply. Index has no cash book,
                                     so we read its near-month FUTURE's depth (the tradeable proxy).

SYNTHESIS merges all four into one overhead/below ladder + the single nearest resistance & support.

HONEST: the order book is resting liquidity (spoofable, warps at open/close), and this pod's own ICT
liquidity-sweep edge backtested NEGATIVE net of costs — so this is a MAP of the battlefield (where you'll
meet sellers, where your stop is exposed), NOT a buy/sell signal. Read-only. Decision-support, not advice.

  python marleg_levels_desk.py RELIANCE
  python marleg_levels_desk.py NIFTY
"""
import marleg_pivots as pv
import marleg_liquidity as lq
import marleg_levels as vpmod
import marleg_options_monitor as mom
import marleg_vol as mv


def _wall(levels):
    qs = [d.get("quantity") or 0 for d in levels]
    tot = sum(qs)
    if len(qs) >= 2 and tot and max(qs) / tot > 0.45:
        d = levels[qs.index(max(qs))]
        return {"price": d.get("price"), "quantity": d.get("quantity")}
    return None


def _orderbook(tk):
    """Live L1/L2: equity via CASH depth; index via its near-month FUTURE (indices have no cash book)."""
    tk = tk.upper()
    if tk in mom.INDEX_STEP:
        try:
            exp = mom.nearest_monthly_expiry()
            fut = f"{tk}{exp.strftime('%y')}{mom._MONNUM[exp.month]}FUT"
            q = mom.option_quote(fut)
            if not isinstance(q, dict) or "error" in q:
                return {"error": (q or {}).get("error", "no quote"), "instrument": fut, "kind": "future"}
            d = q.get("depth", {}) or {}
            buy, sell = d.get("buy", []), d.get("sell", [])
            tbq = q.get("total_buy_qty") or sum(x.get("quantity") or 0 for x in buy)
            tsq = q.get("total_sell_qty") or sum(x.get("quantity") or 0 for x in sell)
            tot = tbq + tsq
            return {"instrument": fut, "kind": "future", "ltp": q.get("ltp"),
                    "bid": d.get("top_bid"), "ask": d.get("top_ask"), "spread_pct": q.get("spread_pct"),
                    "buy_pct": round(tbq / tot * 100, 1) if tot else None,
                    "sell_pct": round(tsq / tot * 100, 1) if tot else None,
                    "imbalance": d.get("imbalance"), "bid_wall": _wall(buy), "ask_wall": _wall(sell),
                    "buy": buy, "sell": sell, "maxq": max([x.get("quantity") or 0 for x in (buy + sell)] or [1]),
                    "note": "Index has no cash order book — this is the near-month FUTURE's 5-level depth (the proxy)."}
        except Exception as e:
            return {"error": str(e)[:120], "kind": "future"}
    try:
        import marleg_depth as dp
        r = dp.read(tk)
        if isinstance(r, dict):
            r["instrument"] = tk
            r["kind"] = "equity"
        return r
    except Exception as e:
        return {"error": str(e)[:120], "kind": "equity"}


def _synthesize(spot, piv, vp, liq, ob):
    res, sup = [], []

    def push(price, src, kind, **kw):
        if not price or price <= 0:
            return
        (res if price > spot else sup).append({"price": price, "src": src, "kind": kind, **kw})

    if piv.get("ok"):
        cl = piv["methods"]["classic"]
        for nm in ("P", "R1", "R2", "R3", "S1", "S2", "S3"):
            push(cl[nm], f"pivot {nm}", "pivot")
    if vp and vp.get("ok"):
        for x in vp.get("resistances", []):
            push(x["price"], x.get("src", "HVN"), "volume", strength=x.get("strength"), near_poc=x.get("near_poc"))
        for x in vp.get("supports", []):
            push(x["price"], x.get("src", "HVN"), "volume", strength=x.get("strength"), near_poc=x.get("near_poc"))
    if liq.get("ok"):
        for p in liq.get("bsl_above", []):
            push(p["price"], p["type"], "stops", mag=p.get("magnetism"), state=p.get("state"))
        for p in liq.get("ssl_below", []):
            push(p["price"], p["type"], "stops", mag=p.get("magnetism"), state=p.get("state"))
    # only merge walls when the book is the SAME instrument (equity cash). An index's book is its FUTURE,
    # which trades at a basis to the index spot — those walls live in the order_book panel, not this ladder.
    if ob and "error" not in ob and ob.get("kind") == "equity":
        if ob.get("ask_wall"):
            push(ob["ask_wall"]["price"], f"ask wall {ob['ask_wall']['quantity']:,}", "wall")
        if ob.get("bid_wall"):
            push(ob["bid_wall"]["price"], f"bid wall {ob['bid_wall']['quantity']:,}", "wall")
    res = sorted([r for r in res if r["price"] > spot], key=lambda x: x["price"])
    sup = sorted([s for s in sup if s["price"] < spot], key=lambda x: -x["price"])
    for r in res:
        r["price"] = round(r["price"], 2); r["dist_pct"] = round((r["price"] / spot - 1) * 100, 2)
    for s in sup:
        s["price"] = round(s["price"], 2); s["dist_pct"] = round((s["price"] / spot - 1) * 100, 2)
    return {"resistance_above": res[:8], "support_below": sup[:8],
            "nearest_resistance": res[0] if res else None, "nearest_support": sup[0] if sup else None}


def desk(tk, spot=None):
    tk = (tk or "").upper().strip()
    g = mom._g()
    try:
        live = mv.underlying_ltp(tk, g)
    except Exception:
        live = None
    spot = float(spot) if spot else live
    piv = pv.pivots(tk, spot=spot)
    spot = spot or (piv.get("spot") if piv.get("ok") else None)
    try:
        vp = vpmod.levels(tk)
    except Exception:
        vp = {"ok": False}
    if not spot and vp.get("ok"):
        spot = vp.get("spot")
    liq = lq.liquidity(tk)
    if not spot and liq.get("ok"):
        spot = liq.get("spot")
    ob = _orderbook(tk)
    if not spot and ob and ob.get("ltp"):
        spot = ob.get("ltp")
    if not spot:
        return {"ok": False, "tk": tk, "error": f"no spot/levels for {tk}"}
    syn = _synthesize(spot, piv, vp, liq, ob)
    return {"ok": True, "tk": tk, "spot": round(spot, 2),
            "poc": vp.get("poc") if vp.get("ok") else None,
            "pivots": piv, "volume_levels": vp, "liquidity": liq, "order_book": ob, "synthesis": syn,
            "note": "Four level sources merged: pivots (watched map) · volume profile (HVN/POC = acceptance) · "
                    "liquidity pools (where stops cluster — a draw, not a signal; sweep edge backtested negative) · "
                    "order-book walls (live resting supply/support — spoofable, warps at open/close).",
            "caveat": "Decision-support, not investment advice — I'm not a licensed advisor. Read-only."}


def order_ladder(sym):
    """Live L1/L2 order ladder for ANY instrument: an OPTION symbol, an INDEX (→ its near future), or EQUITY.
    Unified shape for the UI bars."""
    sym = (sym or "").upper().strip()
    try:
        if mv.parse_option_any(sym):                       # it's an option contract
            q = mom.option_quote(sym)
            if not isinstance(q, dict) or "error" in q:
                return {"ok": False, "error": (q or {}).get("error", "no quote"), "instrument": sym, "kind": "option"}
            d = q.get("depth", {}) or {}
            buy, sell = d.get("buy", []), d.get("sell", [])
            tbq = q.get("total_buy_qty") or sum(x.get("quantity") or 0 for x in buy)
            tsq = q.get("total_sell_qty") or sum(x.get("quantity") or 0 for x in sell)
            tot = tbq + tsq
            return {"ok": True, "instrument": sym, "kind": "option", "ltp": q.get("ltp"),
                    "bid": d.get("top_bid"), "ask": d.get("top_ask"), "spread_pct": q.get("spread_pct"),
                    "buy_pct": round(tbq / tot * 100, 1) if tot else None,
                    "sell_pct": round(tsq / tot * 100, 1) if tot else None,
                    "imbalance": d.get("imbalance"), "bid_wall": _wall(buy), "ask_wall": _wall(sell),
                    "buy": buy, "sell": sell, "maxq": max([x.get("quantity") or 0 for x in (buy + sell)] or [1]),
                    "oi": q.get("oi"), "volume": q.get("volume")}
    except Exception:
        pass
    ob = _orderbook(sym)                                    # index → future, else equity cash
    if isinstance(ob, dict):
        ob.setdefault("ok", "error" not in ob)
    return ob


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = desk(sys.argv[1] if len(sys.argv) > 1 else "RELIANCE")
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n  {r['tk']}  spot ₹{r['spot']}" + (f"  ·  POC ₹{r['poc']}" if r.get("poc") else ""))
    s = r["synthesis"]
    print("\n  OVERHEAD — resistance (where you meet sellers):")
    for x in s["resistance_above"]:
        extra = (f"  str{x['strength']}" if x.get("strength") else "") + (f"  [{x.get('state')}]" if x.get("state") else "")
        print(f"    ₹{x['price']:<10} {x['dist_pct']:+6.2f}%  {x['src']:<16}{extra}")
    print("  BELOW — support (and where your long stop is exposed to a sweep):")
    for x in s["support_below"]:
        extra = (f"  str{x['strength']}" if x.get("strength") else "") + (f"  [{x.get('state')}]" if x.get("state") else "")
        print(f"    ₹{x['price']:<10} {x['dist_pct']:+6.2f}%  {x['src']:<16}{extra}")
    ob = r["order_book"]
    if ob and "error" not in ob:
        print(f"\n  ORDER BOOK ({ob.get('kind')} {ob.get('instrument')}): buy {ob.get('buy_pct')}% / sell {ob.get('sell_pct')}%"
              + (f"  · 🟢 bid wall {ob['bid_wall']['quantity']:,}@₹{ob['bid_wall']['price']}" if ob.get("bid_wall") else "")
              + (f"  · 🔴 ask wall {ob['ask_wall']['quantity']:,}@₹{ob['ask_wall']['price']}" if ob.get("ask_wall") else ""))
    else:
        print(f"\n  ORDER BOOK: {(ob or {}).get('error','n/a')}")
    nr, ns = s["nearest_resistance"], s["nearest_support"]
    print(f"\n  ➤ nearest resistance: {('₹'+str(nr['price'])+' ('+nr['src']+', '+format(nr['dist_pct'],'+')+'%)') if nr else '—'}")
    print(f"  ➤ nearest support:    {('₹'+str(ns['price'])+' ('+ns['src']+', '+format(ns['dist_pct'],'+')+'%)') if ns else '—'}")
    print(f"\n  {r['caveat']}")
