# =====================================================================
# Groww Cloud — OPTIONS debit CALL SPREAD entry  (Groww Cloud -> "Add script")
# ---------------------------------------------------------------------
# Opens a defined-risk debit call spread (BUY lower-strike call + SELL higher-
# strike call) for each reviewed pick from the Marle-G options paper engine.
# Long + short legs are placed together so risk stays capped. The brain (local)
# picks gated-long names + builds the spread; you paste the FNO symbols; the
# Cloud hands execute. Human-reviewed PICKS -> no blind auto-entry.
#
# Schedule: once daily after the open (debit spreads are a ~1-month hold).
# Runs on Groww's servers -> SEBI static-IP rule does not apply.
#
# SAFETY: DRY_RUN = True -> logs only. Confirm a run, THEN set False. Places the
#         LONG leg first; if the SHORT leg fails the spread is incomplete -> the
#         log flags it so you can fix manually (never leaves a naked short).
#
# Build the FNO trading symbols from the options engine output, format:
#   <UNDERLYING><YY><MON><STRIKE><CE>   e.g.  ADANIENT26JUL2950CE
#   long  = lower strike (k_long) call   |   short = higher strike (k_short) call
#   qty   = contracts * lot size (must be a multiple of the F&O lot)
# =====================================================================
from growwapi import GrowwAPI
from datetime import datetime

# ----------------------------- CONFIG --------------------------------
DRY_RUN = True       # True = log only. False = place real F&O orders.
MAX_NEW = 3          # max new spreads per run (anti-runaway)

# --- TODAY'S PICKS — from: python marleg_paper_options.py  (OPEN lines) ---
# Provide the two FNO trading symbols + qty (= contracts * lot). limit prices
# optional (else MARKET). One dict per spread.
PICKS = [
    {"long": "ADANIENT26JUL2950CE", "short": "ADANIENT26JUL3350CE", "qty": 300},
    {"long": "OIL26JUL480CE",       "short": "OIL26JUL540CE",       "qty": 9500},
]

# ----------------------------- AUTH ----------------------------------
# If Groww Cloud injects an authenticated `groww`, DELETE the next two lines.
ACCESS_TOKEN = "PASTE_ACCESS_TOKEN_HERE"
groww = GrowwAPI(ACCESS_TOKEN)

SEG_FNO = getattr(groww, "SEGMENT_FNO", "FNO")
EXCH = groww.EXCHANGE_NSE
PROD_NRML = getattr(groww, "PRODUCT_NRML", "NRML")


def log(*a):
    print("[opt-spread]", datetime.now().strftime("%H:%M:%S"), *a)


def open_fno():
    s = set()
    try:
        p = groww.get_positions_for_user(segment=SEG_FNO) or {}
        for x in (p.get("positions", p) if isinstance(p, dict) else p) or []:
            if (x.get("credit_quantity", 0) or 0) or (x.get("debit_quantity", 0) or 0):
                s.add(x.get("trading_symbol"))
    except Exception as e:
        log("positions read failed:", str(e)[:60])
    return s


def place(sym, side, qty):
    return groww.place_order(trading_symbol=sym, quantity=int(qty), validity=groww.VALIDITY_DAY,
                             exchange=EXCH, segment=SEG_FNO, product=PROD_NRML,
                             order_type=groww.ORDER_TYPE_MARKET,
                             transaction_type=(groww.TRANSACTION_TYPE_BUY if side == "BUY"
                                               else groww.TRANSACTION_TYPE_SELL))


def run():
    held = open_fno()
    placed = 0
    for p in PICKS:
        lo, sh, qty = p["long"], p["short"], int(p["qty"])
        if placed >= MAX_NEW:
            log("MAX_NEW reached — halting"); break
        if lo in held or sh in held:
            log("skip", lo, "- a leg is already open"); continue
        if qty <= 0:
            log("skip", lo, "- bad qty"); continue
        log(f"OPEN SPREAD  BUY {qty} {lo}  +  SELL {qty} {sh}")
        if not DRY_RUN:
            try:
                place(lo, "BUY", qty)                       # long leg FIRST (defines max risk)
            except Exception as e:
                log("LONG leg failed, aborting this spread:", str(e)[:80]); continue
            try:
                place(sh, "SELL", qty)
            except Exception as e:
                log("!! SHORT leg failed — spread INCOMPLETE (long-only). FIX MANUALLY:", lo, str(e)[:60])
        placed += 1
    log(f"done — {placed} {'SPREADS PLACED' if not DRY_RUN else 'would place (DRY-RUN)'}.")


run()
