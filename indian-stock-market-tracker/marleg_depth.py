"""
marleg_depth.py — live equity ORDER BOOK (market depth) read for the intraday pod.

Groww quote() (read-only) returns the 5-level bid/ask ladder plus total_buy_quantity /
total_sell_quantity — the *whole* pending order book. This turns that into a clean buy-vs-sell
pressure read, and is deliberately HONEST about what the book does and does not tell you:

  • It is RESTING LIMIT-order liquidity — a snapshot of orders WAITING, not trades that EXECUTED.
  • "more buy orders" does NOT mean the price will rise. A big bid can be genuine support OR an
    institution quietly absorbing supply; books are spoofed, pulled, and badly distorted at the
    open/close. The % flips second to second.
  • Its real use is EXECUTION (how wide is the spread, where are the walls, can I get filled) plus
    a weak, short-horizon imbalance tell.
  • For DIRECTION, EXECUTED-volume signals — U/D ratio, VWAP position, delivery%, CMF — are far more
    reliable, so the pod always shows those next to the book and never the book alone.

Read-only: this only ever reads quotes. It never places, modifies, or cancels an order.
"""


def _g():
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        return g
    except Exception:
        return None


def _wall(levels):
    """A single level holding >50% of that side's visible qty = a 'wall' (barrier)."""
    qs = [d.get("quantity") or 0 for d in levels]
    tot = sum(qs)
    if len(qs) >= 2 and tot and max(qs) / tot > 0.5:
        d = levels[qs.index(max(qs))]
        return {"price": d.get("price"), "quantity": d.get("quantity")}
    return None


def read(tk):
    g = _g()
    if not g:
        return {"tk": tk.upper(), "error": "groww unavailable"}
    try:
        r = g.quote(tk.upper(), segment="CASH")
        if r.status_code != 200:
            return {"tk": tk.upper(), "error": f"quote HTTP {r.status_code}"}
        p = (r.json() or {}).get("payload") or {}
    except Exception as e:
        return {"tk": tk.upper(), "error": str(e)[:120]}
    if not p:
        return {"tk": tk.upper(), "error": "empty payload (symbol may not exist / market closed)"}

    depth = p.get("depth") or {}
    buy = [d for d in (depth.get("buy") or []) if (d.get("price") or 0) > 0]
    sell = [d for d in (depth.get("sell") or []) if (d.get("price") or 0) > 0]
    vis_bid = sum(d.get("quantity") or 0 for d in buy)       # 5-level visible
    vis_ask = sum(d.get("quantity") or 0 for d in sell)

    # the WHOLE pending book (authoritative buy/sell order totals) — this is the "buy/sell order %"
    tbq = p.get("total_buy_quantity") or 0
    tsq = p.get("total_sell_quantity") or 0
    tot = tbq + tsq
    buy_pct = round(tbq / tot * 100, 1) if tot else None
    sell_pct = round(tsq / tot * 100, 1) if tot else None
    imb = round((tbq - tsq) / tot, 2) if tot else 0.0        # +1 = all bids, -1 = all asks

    ltp = p.get("last_price")
    bid = p.get("bid_price") or (buy[0]["price"] if buy else None)
    ask = p.get("offer_price") or (sell[0]["price"] if sell else None)
    spread = round(ask - bid, 2) if (bid and ask) else None
    mid = ((ask + bid) / 2) if (bid and ask) else ltp
    spread_pct = round(spread / mid * 100, 2) if (spread and mid) else None

    lean = ("buy-heavy" if (buy_pct or 50) >= 58 else "sell-heavy" if (buy_pct or 50) <= 42 else "balanced")
    bid_wall, ask_wall = _wall(buy), _wall(sell)

    # max ladder qty for UI bar-scaling
    maxq = max([d.get("quantity") or 0 for d in (buy + sell)] or [1]) or 1

    notes = []
    if buy_pct is not None:
        notes.append(f"Pending book is {buy_pct}% buy / {sell_pct}% sell orders → {lean}. "
                     f"This is RESTING liquidity (orders waiting), NOT trades that executed.")
    if spread_pct is not None:
        q = "tight/liquid" if spread_pct < 0.1 else "wide/thin — slippage risk" if spread_pct > 0.4 else "normal"
        notes.append(f"Spread ₹{spread} ({spread_pct}%) — {q}.")
    if bid_wall:
        notes.append(f"🟢 Bid wall {bid_wall['quantity']:,} @ ₹{bid_wall['price']} — a support / absorption shelf (until it's pulled or eaten).")
    if ask_wall:
        notes.append(f"🔴 Ask wall {ask_wall['quantity']:,} @ ₹{ask_wall['price']} — overhead supply / a ceiling to clear.")
    notes.append("⚠ The book is spoofable and warps near the open/close. For DIRECTION trust executed volume "
                 "(U/D, VWAP, delivery%) — not this snapshot.")

    return {"tk": tk.upper(), "ltp": ltp, "bid": bid, "ask": ask, "spread": spread, "spread_pct": spread_pct,
            "buy_pct": buy_pct, "sell_pct": sell_pct, "imbalance": imb, "lean": lean,
            "total_buy_qty": tbq, "total_sell_qty": tsq, "vis_bid_qty": vis_bid, "vis_ask_qty": vis_ask,
            "buy": buy, "sell": sell, "bid_wall": bid_wall, "ask_wall": ask_wall, "maxq": maxq,
            "volume": p.get("volume"), "day_change_pct": p.get("day_change_perc"), "notes": notes}


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(json.dumps(read(sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"), indent=2, default=str))
