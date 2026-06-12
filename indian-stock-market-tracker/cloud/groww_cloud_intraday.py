# =====================================================================
# Groww Cloud — INTRADAY (MIS) ENTRY   (paste into Groww Cloud -> "Add script")
# ---------------------------------------------------------------------
# Places an intraday MIS BUY + a resting SL-M stop for each reviewed BUYING
# pick from the Marle-G live monitor / intraday paper engine. The brain
# (local) finds the live BUYING signals + sizes; you paste today's picks; the
# Cloud hands execute. A human reviews PICKS -> no blind auto-entry.
#
# Schedule: every ~15 min, ~09:30-14:45 IST. MIS auto-squares ~15:20, so do
# NOT enter late. Runs on Groww's servers -> SEBI static-IP rule does not apply.
#
# SAFETY: DRY_RUN = True -> logs only. Watch a run, confirm, THEN set False.
#         MAX_NEW caps entries; skips names already open; rejects bad stops.
# =====================================================================
from growwapi import GrowwAPI
from datetime import datetime

# ----------------------------- CONFIG --------------------------------
DRY_RUN = True       # True = log only. False = place real MIS orders.
MAX_NEW = 3          # max new intraday entries per run (anti-runaway)
NO_ENTRY_AFTER = "14:45"   # don't open new MIS positions after this (IST)

# --- TODAY'S PICKS — paste from:  python marleg_paper_intraday.py  (BUY lines) ---
PICKS = [
    {"symbol": "SBIN",      "qty": 75, "stop": 989.4},
    {"symbol": "ICICIBANK", "qty": 58, "stop": 1257.8},
]

# ----------------------------- AUTH ----------------------------------
# If Groww Cloud injects an authenticated `groww`, DELETE the next two lines.
ACCESS_TOKEN = "PASTE_ACCESS_TOKEN_HERE"
groww = GrowwAPI(ACCESS_TOKEN)

SEG, EXCH = groww.SEGMENT_CASH, groww.EXCHANGE_NSE
PROD_MIS = getattr(groww, "PRODUCT_MIS", "MIS")


def log(*a):
    print("[intraday]", datetime.now().strftime("%H:%M:%S"), *a)


def open_symbols():
    s = set()
    try:
        p = groww.get_positions_for_user(segment=SEG) or {}
        for x in (p.get("positions", p) if isinstance(p, dict) else p) or []:
            if (x.get("debit_quantity", 0) - x.get("credit_quantity", 0)) != 0:
                s.add(x.get("trading_symbol"))
    except Exception as e:
        log("positions read failed:", str(e)[:60])
    return s


def ltp(sym):
    r = groww.get_ltp(segment=SEG, exchange_trading_symbols=("NSE_" + sym,))
    return (r or {}).get("NSE_" + sym)


def run():
    now = datetime.now().strftime("%H:%M")
    if now >= NO_ENTRY_AFTER:
        log(f"after {NO_ENTRY_AFTER} IST — no new MIS entries (square-off window). exiting."); return
    held = open_symbols()
    placed = 0
    for p in PICKS:
        s, qty, stop = p["symbol"], int(p["qty"]), float(p["stop"])
        if placed >= MAX_NEW:
            log("MAX_NEW reached — halting"); break
        if s in held:
            log("skip", s, "- already an open position"); continue
        price = ltp(s)
        if not price or qty <= 0 or stop <= 0 or stop >= float(price):   # SANITY
            log("skip", s, f"- sanity (price={price}, qty={qty}, stop={stop})"); continue
        log(f"ENTER  MIS BUY {qty} {s} @ market (~{price})  + SL-M sell trigger {stop}")
        if not DRY_RUN:
            groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                              exchange=EXCH, segment=SEG, product=PROD_MIS,
                              order_type=groww.ORDER_TYPE_MARKET,
                              transaction_type=groww.TRANSACTION_TYPE_BUY)
            groww.place_order(trading_symbol=s, quantity=qty, validity=groww.VALIDITY_DAY,
                              exchange=EXCH, segment=SEG, product=PROD_MIS,
                              order_type=groww.ORDER_TYPE_SL_M,
                              transaction_type=groww.TRANSACTION_TYPE_SELL,
                              trigger_price=stop)
        placed += 1
    log(f"done — {placed} {'ORDERS PLACED' if not DRY_RUN else 'would place (DRY-RUN)'}.")


run()
