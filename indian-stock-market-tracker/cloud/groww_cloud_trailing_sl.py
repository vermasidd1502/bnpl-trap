# =====================================================================
# Groww Cloud — Automatic Trailing Stop-Loss for INTRADAY (MIS) positions
# ---------------------------------------------------------------------
# Paste this into Groww Cloud  ->  "Add script".
# Schedule: run every 1 minute, market hours (~09:20–15:10 IST).
# Runs on Groww's servers, so the SEBI static-IP rule does NOT apply.
#
# Each run (stateless — safe for a scheduled job):
#   1. read your open MIS positions
#   2. compute an ATR "chandelier" stop  (stop = day-extreme  -/+  K*ATR)
#   3. place the SL order if missing, or MODIFY it only when the new stop
#      is MORE favourable  -> it trails in your favour, never loosens.
#   The ratchet "memory" is the live order's trigger price, so nothing
#   needs to persist between scheduled runs.
#
# SAFETY: DRY_RUN = True by default -> it only LOGS what it would do.
#         Watch one or two runs, confirm the logs look right, THEN set
#         DRY_RUN = False to let it touch real orders.
# =====================================================================
from growwapi import GrowwAPI
from datetime import datetime, timedelta

# ----------------------------- CONFIG --------------------------------
K        = 2.0      # trail distance = K * ATR   (lower = tighter stop)
ATR_MIN  = 15       # candle size (minutes) used for ATR
ATR_LEN  = 14
DRY_RUN  = True     # True = log only.  Set False to place/modify real orders.

# ----------------------------- AUTH ----------------------------------
# If Groww Cloud injects an authenticated `groww` for you, DELETE the next
# two lines. Otherwise provide your access token (or key+secret per Cloud's
# auth panel). Verify this against the Cloud editor's auth section.
ACCESS_TOKEN = "PASTE_ACCESS_TOKEN_HERE"
groww = GrowwAPI(ACCESS_TOKEN)

SEG = groww.SEGMENT_CASH
EXCH = groww.EXCHANGE_NSE
LIVE_STATUS = {"OPEN", "ACKED", "TRIGGER_PENDING", "PENDING", "NEW", "MODIFIED", "APPROVED"}


def log(*a):
    print("[trail-sl]", datetime.now().strftime("%H:%M:%S"), *a)


def net_positions():
    """Net each symbol across legs: net = bought(debit) - sold(credit). MIS only."""
    res = groww.get_positions_for_user(segment=SEG)
    rows = res.get("positions", res) if isinstance(res, dict) else res
    agg = {}
    for p in (rows or []):
        s = p.get("trading_symbol")
        if not s:
            continue
        a = agg.setdefault(s, {"bought": 0, "sold": 0, "product": p.get("product", "MIS")})
        a["bought"] += p.get("credit_quantity", 0)   # DEMAT semantics: credit=BOUGHT
        a["sold"] += p.get("debit_quantity", 0)
        a["product"] = p.get("product", a["product"])
    out = []
    for s, a in agg.items():
        net = a["bought"] - a["sold"]
        if net != 0 and str(a["product"]).upper() == "MIS":
            out.append({"symbol": s, "side": "long" if net > 0 else "short", "qty": abs(net)})
    return out


def ltp(symbol):
    r = groww.get_ltp(segment=SEG, exchange_trading_symbols=("NSE_" + symbol,))
    return (r or {}).get("NSE_" + symbol)


def day_extreme(symbol):
    o = groww.get_ohlc(segment=SEG, exchange_trading_symbols=("NSE_" + symbol,))
    d = (o or {}).get("NSE_" + symbol, {}) or {}
    return d.get("high"), d.get("low")


def atr(symbol, price):
    """ATR from intraday candles; fall back to 1.2% of price."""
    try:
        end = datetime.now()
        start = end - timedelta(days=4)
        c = groww.get_historical_candle_data(
            trading_symbol=symbol, exchange=EXCH, segment=SEG,
            start_time=start.strftime("%Y-%m-%d %H:%M:%S"),
            end_time=end.strftime("%Y-%m-%d %H:%M:%S"),
            interval_in_minutes=str(ATR_MIN))
        candles = c.get("candles", c) if isinstance(c, dict) else c
        if candles and len(candles) > ATR_LEN + 1:
            trs = []
            for i in range(1, len(candles)):
                h, l, pc = candles[i][2], candles[i][3], candles[i - 1][4]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            return sum(trs[-ATR_LEN:]) / ATR_LEN
    except Exception as e:
        log("atr fallback for", symbol, "-", str(e)[:60])
    return price * 0.012 if price else None


def existing_sl(symbol, exit_side):
    try:
        ol = groww.get_order_list(segment=SEG)
        orders = ol.get("order_list", ol) if isinstance(ol, dict) else ol
    except Exception:
        return None
    for o in (orders or []):
        if o.get("trading_symbol") != symbol:
            continue
        if str(o.get("order_type", "")).upper().replace("-", "_") not in ("SL", "SL_M", "SLM"):
            continue
        if str(o.get("transaction_type", "")).upper() != exit_side:
            continue
        if str(o.get("order_status", "")).upper() not in LIVE_STATUS:
            continue
        return o
    return None


def market_exit(symbol, exit_side, qty):
    groww.place_order(trading_symbol=symbol, quantity=qty, validity=groww.VALIDITY_DAY,
                      exchange=EXCH, segment=SEG, product=groww.PRODUCT_MIS,
                      order_type=groww.ORDER_TYPE_MARKET,
                      transaction_type=getattr(groww, "TRANSACTION_TYPE_" + exit_side))


def place_sl(symbol, exit_side, qty, trigger):
    groww.place_order(trading_symbol=symbol, quantity=qty, validity=groww.VALIDITY_DAY,
                      exchange=EXCH, segment=SEG, product=groww.PRODUCT_MIS,
                      order_type=groww.ORDER_TYPE_SL_M,
                      transaction_type=getattr(groww, "TRANSACTION_TYPE_" + exit_side),
                      trigger_price=trigger)


def modify_sl(order_id, qty, trigger):
    groww.modify_order(groww_order_id=order_id, quantity=qty,
                       order_type=groww.ORDER_TYPE_SL_M, segment=SEG, trigger_price=trigger)


def run():
    positions = net_positions()
    if not positions:
        log("no open MIS positions"); return
    for p in positions:
        s, side, qty = p["symbol"], p["side"], p["qty"]
        price = ltp(s)
        hi, lo = day_extreme(s)
        a = atr(s, price)
        if price is None or a is None or hi is None or lo is None:
            log("skip", s, "- missing data"); continue
        if side == "long":
            stop = round(hi - K * a, 1); exit_side = "SELL"
            breached = price <= stop
        else:
            stop = round(lo + K * a, 1); exit_side = "BUY"
            breached = price >= stop

        if breached:
            log(f"EXIT-NOW  {exit_side} {qty} {s}  price {price} breached stop {stop}")
            if not DRY_RUN:
                market_exit(s, exit_side, qty)
            continue

        cur = existing_sl(s, exit_side)
        if cur is None:
            log(f"PLACE SL  {exit_side} {qty} {s}  trigger={stop}  (price {price}, ATR {round(a,2)})")
            if not DRY_RUN:
                place_sl(s, exit_side, qty, stop)
        else:
            cur_trig = float(cur.get("trigger_price") or 0)
            better = (stop > cur_trig) if side == "long" else (stop < cur_trig)
            if cur_trig > 0 and better:
                log(f"TRAIL     {s}  {cur_trig} -> {stop}  (price {price})")
                if not DRY_RUN:
                    modify_sl(cur.get("groww_order_id"), qty, stop)
            else:
                log(f"hold      {s}  stop {cur_trig} (computed {stop}, price {price})")


run()
