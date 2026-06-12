"""
Marle-G stop-loss CHECK for your CURRENT trades — equity / MTF / options.
MONITOR ONLY: computes the protective stop for each open position and the exact
SL-M trigger to set. Sends NO orders.

Position source (auto):
  1. LIVE Groww (positions + MTF holdings) — works on Groww Cloud or a whitelisted
     static India IP. From a normal US/VPN IP the token mint returns 403, so it
     falls back to:
  2. my_positions.json  — you fill this once; lets you check stops anywhere.

Stop logic:
  EQUITY / MTF : chandelier ATR stop = max( recent-20d-high - K*ATR(14),  entry - K*ATR ).
                 Trails up, never loosens. MTF flagged — leverage amplifies the loss,
                 so the same % move hurts more; consider a tighter K.
  OPTION       : premium stop. long -> entry*(1-OPT%);  short -> entry*(1+OPT%).
                 The SL-M is placed on the OPTION symbol at that premium trigger,
                 because an option's risk is its premium, not the underlying's ATR.

  python marleg_check_stops.py
  python marleg_check_stops.py --k 2 --opt-stop 25
"""
import os, json, argparse
import pandas as pd, numpy as np, yfinance as yf
import marleg_slack

HERE = os.path.dirname(os.path.abspath(__file__))
POS_FILE = os.path.join(HERE, "my_positions.json")

TEMPLATE = [
    {"symbol": "RELIANCE", "type": "MTF", "side": "long", "qty": 50, "entry": 1290, "underlying": "RELIANCE"},
    {"symbol": "TMPV", "type": "MTF", "side": "long", "qty": 100, "entry": 389, "underlying": "TMPV"},
    {"symbol": "NIFTY25JUN24000CE", "type": "OPT", "side": "long", "qty": 75, "entry": 120, "ltp": 104, "underlying": "^NSEI"}
]


def atr_price_hi(underlying):
    try:
        tk = underlying if (underlying.startswith("^") or "." in underlying) else underlying + ".NS"
        df = yf.Ticker(tk).history(period="3mo", interval="1d", auto_adjust=False).dropna()
        if len(df) < 16:
            return None, None, None
        h, l, c = df["High"], df["Low"], df["Close"]; pc = c.shift(1)
        tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        return float(tr.rolling(14).mean().iloc[-1]), float(c.iloc[-1]), float(h.iloc[-20:].max())
    except Exception:
        return None, None, None


def _opt_ltps(c, syms):
    """Live option premiums (FNO LTP) keyed by trading symbol; {} on failure."""
    out = {}
    try:
        r = c.ltp(syms, segment="FNO")
        pay = (r.json().get("payload") or {}) if r.status_code == 200 else {}
        for s in syms:
            es = c.sym(s)            # NSE_<symbol>
            if es in pay and pay[es] is not None:
                out[s] = float(pay[es])
    except Exception:
        pass
    return out


def load_positions():
    """Real open book from Groww, correctly signed:
      holdings -> long equity (flagged MTF if a matching MTF position exists),
      FNO -> option long/short by credit(sold) vs debit(bought),
      MTF positions not yet in holdings -> long. Flat intraday legs ignored."""
    try:
        import groww_client as gc
        c = gc.GrowwClient(); c.token()                          # 403 here -> falls through
        hold = {}
        for h in ((c.holdings_data() or {}).get("holdings") or []):
            q = h.get("quantity") or 0
            if q <= 0:
                continue
            s = h.get("trading_symbol")
            hold[s] = {"symbol": s, "type": "EQ", "side": "long", "qty": q,
                       "entry": h.get("average_price"), "underlying": s, "live": True}
        opts = []
        for p in ((c.positions_data() or {}).get("positions") or []):
            s = p.get("trading_symbol"); seg = (p.get("segment") or "CASH").upper()
            prod = (p.get("product") or "").upper()
            cq = p.get("credit_quantity", 0) or 0; dq = p.get("debit_quantity", 0) or 0
            q = p.get("quantity", 0) or 0; ncf = p.get("net_carry_forward_quantity", 0) or 0
            if seg == "FNO":
                net_long = cq - dq          # DEMAT semantics: credit=BOUGHT (shares in), debit=SOLD
                if net_long == 0:
                    continue
                side = "long" if net_long > 0 else "short"
                entry = (p.get("credit_price") if side == "long" else p.get("debit_price")) or p.get("net_price")
                opts.append({"symbol": s, "type": "OPT", "side": side, "qty": abs(net_long),
                             "entry": entry, "underlying": s, "live": True})
            elif prod == "MIS":
                net = cq - dq               # DEMAT: credit=BOUGHT — intraday net
                if net > 0:                  # MIS longs: guardian watches (delayed-data alerts)
                    key = s + "?MIS"
                    e = p.get("credit_price") or p.get("net_price") or 0
                    cur = hold.get(key)
                    if cur:                  # multiple MIS legs on one symbol -> blend
                        tot = cur["qty"] + net
                        if cur.get("entry") and e:
                            cur["entry"] = round((cur["entry"] * cur["qty"] + e * net) / tot, 2)
                        cur["qty"] = tot
                    else:
                        hold[key] = {"symbol": s, "type": "MIS", "side": "long", "qty": net,
                                     "entry": e or None, "underlying": s, "live": True}
            elif prod == "MTF":
                if s in hold:
                    hold[s]["type"] = "MTF"                       # same shares, just MTF-funded
                else:
                    size = q or ncf
                    if size > 0:
                        hold[s] = {"symbol": s, "type": "MTF", "side": "long", "qty": size,
                                   "entry": p.get("net_price") or p.get("credit_price"),
                                   "underlying": s, "live": True}
            # MIS/CNC intraday legs that net flat carry no overnight risk -> skip
        if opts:
            lt = _opt_ltps(c, [o["symbol"] for o in opts])
            for o in opts:
                o["ltp"] = lt.get(o["symbol"])
        out = list(hold.values()) + opts
        if out:
            return out, "LIVE Groww"
        return [], "LIVE Groww (no open positions)"
    except Exception as e:
        if os.path.exists(POS_FILE):
            return json.load(open(POS_FILE)), f"my_positions.json  (live unavailable: {str(e)[:55]})"
        json.dump(TEMPLATE, open(POS_FILE, "w"), indent=1)
        return TEMPLATE, f"my_positions.json TEMPLATE just created (live unavailable: {str(e)[:45]})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=float, default=2.5, help="equity/MTF stop = recentHigh - K*ATR")
    ap.add_argument("--opt-stop", type=float, default=30.0, help="option premium stop %%")
    ap.add_argument("--alert", action="store_true", help="post below-trail / hit positions to Slack")
    a = ap.parse_args()
    positions, src = load_positions()
    print(f"Marle-G STOP CHECK  |  source: {src}")
    print(f"equity/MTF stop = {a.k}x ATR chandelier   |   option stop = {a.opt_stop}% of premium   |   MONITOR ONLY (no orders)\n")
    if not positions:
        print("No positions. Fill my_positions.json or run on Groww Cloud for live.")
        return
    print(f"{'SYMBOL':<22}{'type':<5}{'side':<6}{'qty':>6}{'entry':>9}{'now':>9}{'STOP':>9}{'dist%':>7}{'risk Rs':>10}  status")
    print("-" * 100)
    total_risk = 0
    alerts = []
    for p in positions:
        typ = p.get("type", "EQ").upper(); side = p.get("side", "long"); qty = p.get("qty") or 0; entry = p.get("entry")
        if entry is None:
            print(f"{str(p.get('symbol')):<22}{typ:<5} (no entry price)"); continue
        if typ == "OPT":
            stop = round(entry * (1 - a.opt_stop / 100), 1) if side == "long" else round(entry * (1 + a.opt_stop / 100), 1)
            now = p.get("ltp")
            dist = (((now - stop) / now * 100) if side == "long" else ((stop - now) / now * 100)) if now else None
            risk = round(qty * abs(entry - stop))
            hit = now is not None and ((side == "long" and now <= stop) or (side == "short" and now >= stop))
            status = "HIT - EXIT" if hit else ("ok" if now else "place SL")
            now_s = str(now) if now is not None else "?"
            dist_s = (str(round(dist, 1)) if dist is not None else "?")
            print(f"{p['symbol']:<22}{'OPT':<5}{side:<6}{qty:>6}{entry:>9}{now_s:>9}{stop:>9}{dist_s:>7}{risk:>10}  {status}")
            if hit:
                alerts.append(f"{p['symbol']} OPT {side} {status} (now {now_s}, stop {stop})")
        else:
            atr, price, hi = atr_price_hi(p.get("underlying") or p["symbol"])
            if atr is None:
                print(f"{p['symbol']:<22}{typ:<5} no underlying data (check symbol / yfinance)"); continue
            if side == "long":
                trail = hi - a.k * atr                      # chandelier off the 20-day high
                below = price <= trail                       # already past the trail -> exit signal
                stop = round(price - a.k * atr if below else trail, 1)
                if stop >= price:                            # protective stop must sit below market
                    stop = round(price - a.k * atr, 1)
                dist = (price - stop) / price * 100
                status = "BELOW TRAIL - exit/raise" if below else "ok - trailing"
                risk = round(qty * (entry - stop))
            else:
                stop = round(price + a.k * atr, 1)
                dist = (stop - price) / price * 100
                status = "ok - trailing"
                risk = round(qty * (stop - entry))
            risk_s = f"{risk:,}" if risk >= 0 else f"({abs(risk):,})lock"
            note = "  <- MTF: leverage, watch margin" if typ == "MTF" else ""
            print(f"{p['symbol']:<22}{typ:<5}{side:<6}{qty:>6}{entry:>9}{round(price,1):>9}{stop:>9}{round(dist,1):>7}{risk_s:>10}  {status}{note}")
            if status.startswith("BELOW"):
                alerts.append(f"{p['symbol']} {typ} {status} (now {round(price,1)}, stop {stop})")
        total_risk += max(0, risk)
    print("-" * 100)
    print(f"Total open risk if every stop hits: Rs {total_risk:,.0f}")
    print("\nMONITOR ONLY - nothing sent. Place each STOP as an SL-M on the symbol, or deploy the")
    print("Groww Cloud trailing-SL (covers equity/MTF/options) to maintain these automatically.")
    if a.alert and alerts:
        ok = marleg_slack.notify(f"⚠️ Marle-G stop check — {len(alerts)} position(s) need attention",
                                 fields={"positions": "\n".join(alerts), "total risk": f"Rs {total_risk:,.0f}"})
        print(f"{'posted to Slack' if ok else 'Slack off (set MARLEG_SLACK_WEBHOOK)'} — {len(alerts)} alert(s)")
    json.dump({"source": src, "stops": positions}, open(os.path.join(HERE, "marleg_stop_check.json"), "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
