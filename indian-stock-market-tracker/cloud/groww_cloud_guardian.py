# =====================================================================
# Groww Cloud — POSITIONAL STOP GUARDIAN (holdings + MTF + F&O options)
# ---------------------------------------------------------------------
# The cloud twin of marleg_stop_guardian.py, replay-validated logic:
#   EQUITY / MTF (long):   entry-anchored chandelier. stop = anchor - K*ATR,
#       anchor = max(avg_price, LTP). The live SL order's trigger is the
#       ratchet memory: each run may only RAISE it -> trails up, never loosens.
#   ADAPTIVE DEFENSE:      if today's range > 1.5*ATR (vol spike), K tightens
#       2.5 -> 1.5; and if the position is up >= 1.5*ATR, the stop floors at
#       breakeven + 0.1*ATR (a winner is not allowed to become a loser).
#   OPTIONS (from net credit/debit):
#       SHORT option -> BUY-back SL at entry*1.30; trails DOWN with ltp*1.30.
#       LONG  option -> SELL SL at entry*0.70;     trails UP   with ltp*0.70.
#
# Paste into Groww Cloud -> "Add script". Schedule: every 1-2 min, 09:20-15:25 IST.
# Runs on Groww's India servers -> no laptop, no US-IP problem, SL re-placed
# fresh each morning if missing (DAY validity).
#
# SAFETY: DRY_RUN = True by default — it only LOGS what it would do.
#         Watch a few runs in the Cloud log. Flipping DRY_RUN = False is YOUR
#         decision and YOUR action; it will then place/modify real SL orders.
# =====================================================================
from growwapi import GrowwAPI
from datetime import datetime, timedelta
import time as _time

# ----------------------------- CONFIG --------------------------------
K          = 2.5     # positional chandelier distance (x daily ATR) — outside noise
K_TIGHT    = 1.5     # defense distance during a vol spike
RANGE_X    = 1.5     # spike = today's range > 1.5 * ATR
PROFIT_ATR = 1.5     # up >= 1.5*ATR in defense -> floor at breakeven + 0.1*ATR
OPT_STOP   = 0.30    # option premium stop: 30% beyond entry premium
# ---- intraday (MIS) layer — for intraday shorts/longs ----
K_INTRA      = 1.5   # intraday trail = 1.5 x ATR(15-min candles)
INTRA_LOCK   = 1.0   # in profit >= 1*ATR_intra -> stop floors at breakeven -/+ 0.1*ATR
SQUARE_OFF   = (15, 5)   # 15:05 IST: exit all MIS yourself, before broker auto square-off
DRY_RUN    = True    # True = log only. YOU flip this to False, consciously.

# ----------------------------- AUTH ----------------------------------
# If Groww Cloud injects an authenticated `groww`, DELETE these two lines.
ACCESS_TOKEN = "PASTE_ACCESS_TOKEN_HERE"
groww = GrowwAPI(ACCESS_TOKEN)

CASH, FNO = groww.SEGMENT_CASH, groww.SEGMENT_FNO
EXCH = groww.EXCHANGE_NSE
LIVE = {"OPEN", "ACKED", "TRIGGER_PENDING", "PENDING", "NEW", "MODIFIED", "APPROVED"}


def log(*a):
    print("[guardian]", datetime.now().strftime("%H:%M:%S"), *a)


def ltp(symbol, seg=CASH):
    r = groww.get_ltp(segment=seg, exchange_trading_symbols=("NSE_" + symbol,))
    return (r or {}).get("NSE_" + symbol)


def ohlc(symbol, seg=CASH):
    r = groww.get_ohlc(segment=seg, exchange_trading_symbols=("NSE_" + symbol,))
    return (r or {}).get("NSE_" + symbol, {}) or {}


def atr14(symbol):
    """Daily ATR(14); fallback 2% of price."""
    try:
        end = datetime.now()
        c = groww.get_historical_candle_data(
            trading_symbol=symbol, exchange=EXCH, segment=CASH,
            start_time=(end - timedelta(days=40)).strftime("%Y-%m-%d %H:%M:%S"),
            end_time=end.strftime("%Y-%m-%d %H:%M:%S"), interval_in_minutes="1440")
        rows = c.get("candles", c) if isinstance(c, dict) else c
        if rows and len(rows) > 15:
            trs = []
            for i in range(1, len(rows)):
                h, l, pc = rows[i][2], rows[i][3], rows[i - 1][4]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            return sum(trs[-14:]) / 14.0
    except Exception as e:
        log("atr fallback", symbol, str(e)[:50])
    p = ltp(symbol)
    return p * 0.02 if p else None


def holdings_book():
    """Long CNC/MTF equity: symbol, qty, avg."""
    out = []
    try:
        res = groww.get_holdings_for_user()
        rows = res.get("holdings", res) if isinstance(res, dict) else res
        for h in (rows or []):
            q = h.get("quantity") or 0
            if q > 0 and h.get("trading_symbol"):
                out.append({"symbol": h["trading_symbol"], "qty": q,
                            "avg": h.get("average_price") or 0})
    except Exception as e:
        log("holdings error", str(e)[:60])
    return out


def option_book():
    """Net F&O option legs: symbol, side, qty, entry premium."""
    out = []
    try:
        res = groww.get_positions_for_user(segment=FNO)
        rows = res.get("positions", res) if isinstance(res, dict) else res
        for p in (rows or []):
            s = p.get("trading_symbol")
            cq, dq = p.get("credit_quantity", 0) or 0, p.get("debit_quantity", 0) or 0
            net = cq - dq                   # DEMAT semantics: credit=BOUGHT, debit=SOLD
            if not s or net == 0:
                continue
            side = "long" if net > 0 else "short"
            entry = (p.get("credit_price") if side == "long" else p.get("debit_price")) or p.get("net_price")
            if entry:
                out.append({"symbol": s, "side": side, "qty": abs(net), "entry": float(entry)})
    except Exception as e:
        log("fno error", str(e)[:60])
    return out


def existing_sl(symbol, exit_side, seg):
    try:
        ol = groww.get_order_list(segment=seg)
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
        if str(o.get("order_status", "")).upper() not in LIVE:
            continue
        return o
    return None


def upsert_sl(symbol, exit_side, qty, trigger, seg, product, better):
    """Place SL-M if missing; modify only in the favourable direction."""
    cur = existing_sl(symbol, exit_side, seg)
    if cur is None:
        log(f"PLACE SL  {exit_side} {qty} {symbol} trigger={trigger}")
        if not DRY_RUN:
            groww.place_order(trading_symbol=symbol, quantity=qty, validity=groww.VALIDITY_DAY,
                              exchange=EXCH, segment=seg, product=product,
                              order_type=groww.ORDER_TYPE_SL_M,
                              transaction_type=getattr(groww, "TRANSACTION_TYPE_" + exit_side),
                              trigger_price=trigger)
        return
    cur_trig = float(cur.get("trigger_price") or 0)
    if cur_trig > 0 and better(trigger, cur_trig):
        log(f"TRAIL     {symbol} {cur_trig} -> {trigger}")
        if not DRY_RUN:
            groww.modify_order(groww_order_id=cur.get("groww_order_id"), quantity=qty,
                               order_type=groww.ORDER_TYPE_SL_M, segment=seg,
                               trigger_price=trigger)
    else:
        log(f"hold      {symbol} stop {cur_trig} (computed {trigger})")


def atr_intra(symbol, price):
    """ATR(14) on 15-min candles — the intraday noise unit. Fallback 0.5% of price."""
    try:
        end = datetime.now()
        c = groww.get_historical_candle_data(
            trading_symbol=symbol, exchange=EXCH, segment=CASH,
            start_time=(end - timedelta(days=4)).strftime("%Y-%m-%d %H:%M:%S"),
            end_time=end.strftime("%Y-%m-%d %H:%M:%S"), interval_in_minutes="15")
        rows = c.get("candles", c) if isinstance(c, dict) else c
        if rows and len(rows) > 15:
            trs = []
            for i in range(1, len(rows)):
                h, l, pc = rows[i][2], rows[i][3], rows[i - 1][4]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            return sum(trs[-14:]) / 14.0
    except Exception:
        pass
    return price * 0.005 if price else None


def mis_book():
    """Net intraday (MIS) legs with avg entry: symbol, side, qty, entry."""
    out = []
    try:
        res = groww.get_positions_for_user(segment=CASH)
        rows = res.get("positions", res) if isinstance(res, dict) else res
        for p in (rows or []):
            s = p.get("trading_symbol")
            if not s or str(p.get("product", "")).upper() != "MIS":
                continue
            bought, sold = p.get("credit_quantity", 0) or 0, p.get("debit_quantity", 0) or 0
            net = bought - sold             # DEMAT semantics: credit=BOUGHT, debit=SOLD
            if net == 0:
                continue
            side = "long" if net > 0 else "short"
            entry = (p.get("credit_price") if side == "long" else p.get("debit_price")) or p.get("net_price")
            if entry:
                out.append({"symbol": s, "side": side, "qty": abs(net), "entry": float(entry)})
    except Exception as e:
        log("mis error", str(e)[:60])
    return out


def manage_intraday():
    """Intraday MIS legs — entry-anchored trail in INTRADAY ATR units, both sides.
    SHORT: anchor = min(entry, LTP); stop = anchor + K_INTRA*ATRi; trigger only ever FALLS.
    LONG : anchor = max(entry, LTP); stop = anchor - K_INTRA*ATRi; trigger only ever RISES.
    Profit lock at breakeven once up 1*ATRi. Hard time exit at 15:05 IST."""
    legs = mis_book()
    if not legs:
        return
    now = datetime.now()
    squareoff = (now.hour, now.minute) >= SQUARE_OFF
    for p in legs:
        s, side, qty, entry = p["symbol"], p["side"], p["qty"], p["entry"]
        px = ltp(s)
        ai = atr_intra(s, px)
        if not px or not ai:
            log("skip MIS", s, "no data"); continue
        if squareoff:
            exit_side = "SELL" if side == "long" else "BUY"
            log(f"TIME EXIT {exit_side} {qty} {s} — 15:05 square-off (MIS)")
            if not DRY_RUN:
                groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                                  exchange=EXCH, segment=CASH, product=groww.PRODUCT_MIS,
                                  order_type=groww.ORDER_TYPE_MARKET,
                                  transaction_type=getattr(groww, "TRANSACTION_TYPE_" + exit_side))
            continue
        if side == "short":
            anchor = min(entry, px)
            stop = anchor + K_INTRA * ai
            if entry - px >= INTRA_LOCK * ai:                 # short in profit: lock breakeven
                stop = min(stop, entry - 0.1 * ai)
            stop = round(stop, 1)
            if px >= stop:
                log(f"EXIT-NOW  BUY {qty} {s} — {px} >= stop {stop} (intraday short)")
                if not DRY_RUN:
                    groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                                      exchange=EXCH, segment=CASH, product=groww.PRODUCT_MIS,
                                      order_type=groww.ORDER_TYPE_MARKET,
                                      transaction_type=groww.TRANSACTION_TYPE_BUY)
                continue
            upsert_sl(s, "BUY", qty, stop, CASH, groww.PRODUCT_MIS,
                      better=lambda new, old: new < old)      # short: trigger only falls
        else:
            anchor = max(entry, px)
            stop = anchor - K_INTRA * ai
            if px - entry >= INTRA_LOCK * ai:
                stop = max(stop, entry + 0.1 * ai)
            stop = round(stop, 1)
            if px <= stop:
                log(f"EXIT-NOW  SELL {qty} {s} — {px} <= stop {stop} (intraday long)")
                if not DRY_RUN:
                    groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                                      exchange=EXCH, segment=CASH, product=groww.PRODUCT_MIS,
                                      order_type=groww.ORDER_TYPE_MARKET,
                                      transaction_type=groww.TRANSACTION_TYPE_SELL)
                continue
            upsert_sl(s, "SELL", qty, stop, CASH, groww.PRODUCT_MIS,
                      better=lambda new, old: new > old)


def run():
    manage_intraday()                                          # MIS first — fastest clock
    # ---- long equity (CNC holdings; MTF shares appear here once delivered) ----
    for h in holdings_book():
        s, qty, avg = h["symbol"], int(h["qty"]), float(h["avg"])
        px = ltp(s)
        a = atr14(s)
        if not px or not a:
            log("skip", s, "no data"); continue
        o = ohlc(s)
        day_range = (o.get("high") or px) - (o.get("low") or px)
        spike = day_range > RANGE_X * a
        k_eff = K_TIGHT if spike else K
        anchor = max(avg, px)
        stop = anchor - k_eff * a
        if spike and avg > 0 and px >= avg + PROFIT_ATR * a:
            stop = max(stop, avg + 0.1 * a)               # profit lock
        stop = round(stop, 1)
        if px <= stop:
            log(f"EXIT-NOW  SELL {qty} {s} — price {px} at/below stop {stop}"
                + (" [DEFENSE]" if spike else ""))
            if not DRY_RUN:
                groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                                  exchange=EXCH, segment=CASH, product=groww.PRODUCT_CNC,
                                  order_type=groww.ORDER_TYPE_MARKET,
                                  transaction_type=groww.TRANSACTION_TYPE_SELL)
            continue
        upsert_sl(s, "SELL", qty, stop, CASH, groww.PRODUCT_CNC,
                  better=lambda new, old: new > old)      # long: trigger only ever rises
        if spike:
            log(f"          {s} DEFENSE active (range {round(day_range,1)} > {RANGE_X}xATR {round(a,1)})")

    # ---- options (side-aware premium stops) ----
    for p in option_book():
        s, side, qty, entry = p["symbol"], p["side"], p["qty"], p["entry"]
        prem = ltp(s, FNO)
        if not prem:
            log("skip", s, "no premium"); continue
        if side == "short":
            stop = round(min(entry, prem) * (1 + OPT_STOP), 1)     # buy-back; trails DOWN
            if prem >= stop:
                log(f"EXIT-NOW  BUY {qty} {s} — premium {prem} >= stop {stop} (short)")
                if not DRY_RUN:
                    groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                                      exchange=EXCH, segment=FNO, product=groww.PRODUCT_NRML,
                                      order_type=groww.ORDER_TYPE_MARKET,
                                      transaction_type=groww.TRANSACTION_TYPE_BUY)
                continue
            upsert_sl(s, "BUY", qty, stop, FNO, groww.PRODUCT_NRML,
                      better=lambda new, old: new < old)  # short: trigger only ever falls
        else:
            stop = round(max(entry, prem) * (1 - OPT_STOP), 1)     # sell; trails UP
            if prem <= stop:
                log(f"EXIT-NOW  SELL {qty} {s} — premium {prem} <= stop {stop} (long)")
                if not DRY_RUN:
                    groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                                      exchange=EXCH, segment=FNO, product=groww.PRODUCT_NRML,
                                      order_type=groww.ORDER_TYPE_MARKET,
                                      transaction_type=groww.TRANSACTION_TYPE_SELL)
                continue
            upsert_sl(s, "SELL", qty, stop, FNO, groww.PRODUCT_NRML,
                      better=lambda new, old: new > old)


# ---------------------------------------------------------------------
# Groww Cloud schedules a RUN WINDOW (start–end), not an interval — the
# platform starts this script once at the window's start. So we loop
# ourselves: one pass every LOOP_SECONDS until END_IST (or until the
# platform kills the process at the window's end). Passes are stateless —
# the live order's trigger is the only memory — so this is restart-safe.
LOOP_SECONDS = 60
END_IST = (15, 25)

log("guardian started — looping every", LOOP_SECONDS, "s until",
    "%02d:%02d IST" % END_IST, "| DRY_RUN =", DRY_RUN)
while True:
    _now = datetime.now()
    if (_now.hour, _now.minute) >= END_IST:
        log("window over — exiting")
        break
    try:
        run()
    except Exception as _e:
        log("cycle error:", str(_e)[:90])
    _time.sleep(LOOP_SECONDS)
