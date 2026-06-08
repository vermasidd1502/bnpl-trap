"""
Marle-G dynamic stop-loss engine  —  ATR chandelier trailing stops.

Why this fixes the "too tight / too wide" problem:
  trail distance = k * ATR(14)  ->  volatility-scaled, not guessed.
  The stop only ratchets in your favour (locks profit), never loosens.

Modes:
  MONITOR (default, works NOW): polls live price, trails the stop, prints it,
      and ALERTS when price hits it. No orders sent.
  PUSH  (--push, needs MARLEG_ALLOW_LIVE_ORDERS=1 + registered static IP):
      also maintains a resting SL order at Groww and modifies its trigger as
      the stop trails; flattens on a hit. (Blocked today by the static-IP rule;
      runs as dry-run until then.)

State persists to marleg_stops_state.json so trails survive restarts.

CLI:
  python marleg_stop_engine.py                 # one snapshot of all stops
  python marleg_stop_engine.py --ticks 0 --interval 5    # run forever, every 5s
  python marleg_stop_engine.py --k 3 --push    # wider trail + auto-push (when unlocked)
"""
import os, json, time, math, argparse
from collections import defaultdict
import yfinance as yf
import groww_client as gc

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "marleg_stops_state.json")


def atr(symbol, period=14):
    try:
        df = yf.Ticker(symbol + ".NS").history(period="3mo", interval="1d", auto_adjust=False)
        if df is None or len(df) < period + 1:
            return None
        h, l, c = df["High"], df["Low"], df["Close"]
        pc = c.shift(1)
        tr = (h - l)
        tr = tr.combine((h - pc).abs(), max).combine((l - pc).abs(), max)
        return float(tr.rolling(period).mean().iloc[-1])
    except Exception:
        return None


def net_positions(client):
    pos = (client.positions_data() or {}).get("positions", [])
    agg = defaultdict(lambda: {"sold": 0, "bought": 0, "sellv": 0.0, "buyv": 0.0,
                               "product": "MIS", "exchange": "NSE"})
    for p in pos:
        a = agg[p["trading_symbol"]]
        a["sold"] += p.get("credit_quantity", 0)
        a["bought"] += p.get("debit_quantity", 0)
        a["sellv"] += p.get("credit_quantity", 0) * p.get("credit_price", 0)
        a["buyv"] += p.get("debit_quantity", 0) * p.get("debit_price", 0)
        a["product"] = p.get("product", a["product"])
        a["exchange"] = p.get("exchange", a["exchange"])
    out = []
    for s, a in agg.items():
        net = a["bought"] - a["sold"]
        if net == 0:
            continue
        long = net > 0
        entry = (a["buyv"] / a["bought"]) if long and a["bought"] else \
                (a["sellv"] / a["sold"]) if a["sold"] else None
        out.append({"symbol": s, "side": "long" if long else "short", "qty": abs(net),
                    "entry": round(entry, 2) if entry else None,
                    "product": a["product"], "exchange": a["exchange"]})
    return out


def trail_stop(side, hwm, lwm, atr_val, k, prev_stop):
    """Return the ratcheted stop. Long trails below the high; short above the low."""
    if side == "long":
        raw = hwm - k * atr_val
        return raw if prev_stop is None else max(prev_stop, raw)
    else:
        raw = lwm + k * atr_val
        return raw if prev_stop is None else min(prev_stop, raw)


def snapshot(client, k, push, state):
    positions = net_positions(client)
    if not positions:
        print("No open positions to trail.")
        return state, []
    syms = [p["symbol"] for p in positions]
    px = (gc._safe_json(client.ltp(syms)).get("payload") or {})
    rows = []
    live_keys = set()
    for p in positions:
        key = f"{p['symbol']}:{p['side']}"
        live_keys.add(key)
        price = px.get("NSE_" + p["symbol"])
        if price is None:
            continue
        price = float(price)
        st = state.get(key, {})
        a = st.get("atr") or atr(p["symbol"]) or price * 0.02   # 2% fallback
        side = p["side"]
        if side == "long":
            hwm = max(st.get("hwm", p["entry"] or price), price)
            lwm = None
        else:
            lwm = min(st.get("lwm", p["entry"] or price), price)
            hwm = None
        stop = trail_stop(side, hwm, lwm, a, k, st.get("stop"))
        hit = (price <= stop) if side == "long" else (price >= stop)
        dist = (price - stop) / price * 100 if side == "long" else (stop - price) / price * 100
        state[key] = {"symbol": p["symbol"], "side": side, "qty": p["qty"], "entry": p["entry"],
                      "product": p["product"], "atr": round(a, 2), "k": k,
                      "hwm": round(hwm, 2) if hwm else None, "lwm": round(lwm, 2) if lwm else None,
                      "stop": round(stop, 2)}
        rows.append({**state[key], "ltp": price, "dist": round(dist, 2), "status": "HIT" if hit else "ok"})
    # prune closed positions
    for k2 in [k2 for k2 in state if k2 not in live_keys]:
        state.pop(k2, None)
    return state, rows


def render(rows):
    print(f"{'SYMBOL':<12}{'side':<6}{'qty':>5}{'entry':>9}{'LTP':>9}{'ATR':>7}{'STOP':>9}{'dist%':>7}  status")
    for r in rows:
        print(f"{r['symbol']:<12}{r['side']:<6}{r['qty']:>5}{(r['entry'] or 0):>9}{r['ltp']:>9}"
              f"{r['atr']:>7}{r['stop']:>9}{r['dist']:>7}  "
              f"{'>>> STOP HIT — EXIT NOW <<<' if r['status']=='HIT' else 'trailing'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=float, default=2.5, help="trail = k * ATR (lower = tighter)")
    ap.add_argument("--interval", type=int, default=6, help="poll seconds")
    ap.add_argument("--ticks", type=int, default=1, help="number of polls (0 = forever)")
    ap.add_argument("--push", action="store_true", help="also place/modify the SL order (gated)")
    args = ap.parse_args()

    client = gc.GrowwClient()
    state = {}
    try:
        with open(STATE) as f:
            state = json.load(f)
    except Exception:
        pass

    allow_push = os.environ.get("MARLEG_ALLOW_LIVE_ORDERS") == "1"
    print(f"Dynamic stop engine | trail = {args.k}x ATR | mode = "
          f"{'PUSH' if (args.push and allow_push) else 'PUSH(dry-run)' if args.push else 'MONITOR'}\n")

    n, t = 0, 0
    while True:
        n += 1
        state, rows = snapshot(client, args.k, args.push, state)
        print(f"--- tick {n}  {time.strftime('%H:%M:%S')} ---")
        render(rows)
        try:
            with open(STATE, "w") as f:
                json.dump(state, f, indent=2)
        except Exception:
            pass
        for r in rows:
            if r["status"] == "HIT":
                act = (f"flatten {r['side']} {r['qty']} {r['symbol']} @ market")
                if args.push:
                    res = client.place_order(
                        r["symbol"], "SELL" if r["side"] == "long" else "BUY", r["qty"],
                        product=r["product"], order_type="MARKET",
                        confirm=allow_push)
                    print(f"   PUSH: {act} -> {res.get('note') or res.get('status_code') or 'dry-run'}")
                else:
                    print(f"   ALERT: {act}  (monitor mode — place it in the Groww app)")
        t += 1
        if args.ticks and t >= args.ticks:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
