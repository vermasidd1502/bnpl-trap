# =====================================================================
# Groww Cloud — MTF SWING ENTRY  (paste into Groww Cloud -> "Add script")
# ---------------------------------------------------------------------
# Places an MTF BUY + a resting SL-M protective stop for each REVIEWED pick
# from the Marle-G screen. The brain (local) screens + sizes; you paste
# today's picks here; the Cloud hands execute. A human reviews PICKS each
# day -> no blind auto-entry.
#
# Schedule: once daily, ~09:20-09:30 IST (after the open).
# Runs on Groww's servers -> the SEBI static-IP rule does NOT apply.
#
# SAFETY: DRY_RUN = True -> logs only, sends nothing. Watch one run, confirm,
#         THEN set DRY_RUN = False. Caps new entries (MAX_NEW), skips names you
#         already hold, and rejects nonsensical stops.
# =====================================================================
from growwapi import GrowwAPI
from datetime import datetime

# ----------------------------- CONFIG --------------------------------
DRY_RUN  = True     # True = log only. False = place real MTF orders.
MAX_NEW  = 3        # max new entries this run (anti-runaway)

# --- TODAY'S PICKS — paste from:  python marleg_momentum_buy.py  /  paper engine ---
# qty is risk-sized by the local screen; stop is the protective SL-M trigger.
PICKS = [
    {"symbol": "JSWENERGY",  "qty": 271, "stop": 540.4},
    {"symbol": "ADANIENT",   "qty": 60,  "stop": 2804.7},
    {"symbol": "ADANIENSOL", "qty": 83,  "stop": 1466.1},
]

# ----------------------------- AUTH ----------------------------------
# If Groww Cloud injects an authenticated `groww`, DELETE the next two lines.
ACCESS_TOKEN = "PASTE_ACCESS_TOKEN_HERE"
groww = GrowwAPI(ACCESS_TOKEN)

SEG, EXCH = groww.SEGMENT_CASH, groww.EXCHANGE_NSE
PROD_MTF  = getattr(groww, "PRODUCT_MTF", "MTF")


def log(*a):
    print("[mtf-entry]", datetime.now().strftime("%H:%M:%S"), *a)


def held_symbols():
    """Names you already hold or have an open position in -> never double-enter."""
    s = set()
    try:
        h = groww.get_holdings_for_user() or {}
        for x in (h.get("holdings", h) if isinstance(h, dict) else h) or []:
            if x.get("trading_symbol"):
                s.add(x["trading_symbol"])
    except Exception as e:
        log("holdings read failed:", str(e)[:60])
    try:
        p = groww.get_positions_for_user(segment=SEG) or {}
        for x in (p.get("positions", p) if isinstance(p, dict) else p) or []:
            if (x.get("debit_quantity", 0) - x.get("credit_quantity", 0)) != 0:
                s.add(x.get("trading_symbol"))
    except Exception:
        pass
    return s


def ltp(sym):
    r = groww.get_ltp(segment=SEG, exchange_trading_symbols=("NSE_" + sym,))
    return (r or {}).get("NSE_" + sym)


def run():
    held = held_symbols()
    placed = 0
    for p in PICKS:
        s, qty, stop = p["symbol"], int(p["qty"]), float(p["stop"])
        if placed >= MAX_NEW:
            log("MAX_NEW reached — halting for safety"); break
        if s in held:
            log("skip", s, "- already held / open position"); continue
        price = ltp(s)
        if not price or qty <= 0 or stop <= 0 or stop >= float(price):   # SANITY
            log("skip", s, f"- sanity (price={price}, qty={qty}, stop={stop})"); continue
        log(f"ENTER  MTF BUY {qty} {s} @ market (~{price})  + SL-M sell trigger {stop}")
        if not DRY_RUN:
            groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                              exchange=EXCH, segment=SEG, product=PROD_MTF,
                              order_type=groww.ORDER_TYPE_MARKET,
                              transaction_type=groww.TRANSACTION_TYPE_BUY)
            groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                              exchange=EXCH, segment=SEG, product=PROD_MTF,
                              order_type=groww.ORDER_TYPE_SL_M,
                              transaction_type=groww.TRANSACTION_TYPE_SELL,
                              trigger_price=stop)
        placed += 1
    log(f"done — {placed} {'ORDERS PLACED' if not DRY_RUN else 'would place (DRY-RUN)'}.")
    log("after fills, let groww_cloud_trailing_sl.py trail the stops.")


run()
