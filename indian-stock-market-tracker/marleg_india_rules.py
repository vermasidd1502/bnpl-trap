"""
Marle-G — INDIA MARKET RULES. The Indian-market "book", encoded as code.

Why a module and not a book: 2024-26 rewrote the rulebook —
  * Budget 2026 (eff. 2026-04-01): STT futures 0.05% sell, options 0.15% of premium on
    sell AND 0.15% if exercised. Delivery 0.1%/side, intraday 0.025% sell unchanged.
  * SEBI F&O reform: ONE weekly index per exchange — NIFTY weekly on NSE (TUESDAY expiry
    since 2025-09); SENSEX weekly on BSE (Thursday). BankNifty/FinNifty/Midcap weeklies
    DISCONTINUED -> monthly only. Single-stock options were always monthly.
  * Lot sizes (min contract ~Rs 15L): NIFTY 75, BANKNIFTY 35, FINNIFTY 65, SENSEX 30.
  * Option buyers pay FULL premium upfront (2025-02); no calendar-spread margin benefit
    on expiry day.
  * Taxes: STCG 20% (<12m), LTCG 12.5% above Rs 1.25L/yr (>12m); intraday & F&O =
    business income at slab. T+1 settlement everywhere.
  * Surveillance: ASM (4 stages; 100% margin, bands tighten) / GSM (trade-to-trade, 5%
    band, 100% ASD) / ESM small-caps. Surveillance names are STOP-LOSS TRAPS: T2T means
    no intraday exit, a 5% band means gap-locks straight through your stop.

Every engine should consult THIS module instead of hard-coding assumptions.
  python marleg_india_rules.py        # print the live rule summary
"""
import os, json, time
from datetime import date, datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
HERE = os.path.dirname(os.path.abspath(__file__))

# ----------------------------------------------------------------- sessions (IST minutes)
SESSIONS = {
    "pre_open":      ("09:00", "09:08"),   # order entry 9:00-9:07:59, matching to 9:08
    "normal":        ("09:15", "15:30"),
    "closing":       ("15:40", "16:00"),   # close-price session
    "block_morning": ("08:45", "09:00"),
    "block_noon":    ("14:05", "14:20"),
}

# BEST-EFFORT NSE trading holidays 2026 (weekday closures). Verify against the exchange
# calendar at the start of each quarter; bots also fall back to data-availability.
HOLIDAYS_2026 = [
    "2026-01-26",  # Republic Day (Mon)
    "2026-03-04",  # Holi (Wed)
    "2026-03-26",  # Shri Ram Navami (Thu)  [monthly expiry shifts a day early that week]
    "2026-03-31",  # Mahavir Jayanti (Tue)
    "2026-04-03",  # Good Friday (Fri)
    "2026-04-14",  # Dr. Ambedkar Jayanti (Tue)
    "2026-05-01",  # Maharashtra Day (Fri)
    "2026-05-28",  # Bakri Id (Thu)
    "2026-06-26",  # Muharram (Fri) — date approximate, lunar
    "2026-09-14",  # Ganesh Chaturthi (Mon)
    "2026-10-02",  # Gandhi Jayanti (Fri)
    "2026-10-20",  # Dussehra (Tue)
    "2026-11-09",  # Balipratipada (Mon)
    "2026-11-24",  # Guru Nanak Jayanti (Tue)
    "2026-12-25",  # Christmas (Fri)
]
MUHURAT_2026 = "2026-11-08"               # special ~1h Diwali session (Sunday)


def is_trading_day(d=None):
    d = d or datetime.now(IST).date()
    if isinstance(d, datetime):
        d = d.date()
    return d.weekday() < 5 and d.isoformat() not in HOLIDAYS_2026


def next_trading_day(d=None):
    d = (d or datetime.now(IST).date()) + timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return d


# ----------------------------------------------------------------- costs (bps) — current 2026-04-01
STT = {  # security transaction tax
    "delivery_buy": 10.0, "delivery_sell": 10.0,       # 0.1% each side
    "intraday_sell": 2.5,                              # 0.025% sell only
    "futures_sell": 5.0,                               # 0.05% sell  (Budget 2026 hike)
    "option_premium_sell": 15.0,                       # 0.15% of premium on sell (hike)
    "option_exercised": 15.0,                          # 0.15% of intrinsic if exercised
}
OTHER = {
    "stamp_delivery_buy": 1.5, "stamp_intraday_buy": 0.3, "stamp_futures_buy": 0.2,
    "stamp_option_buy": 0.3,
    "nse_txn_equity": 0.297 * 2,                       # ~0.00297%/side
    "nse_txn_futures": 0.173 * 2, "nse_txn_option_premium": 3.503 * 2,
    "sebi_fee": 0.01 * 2, "dp_sell_rs": 15.0, "gst_pct_on_charges": 18.0,
}
SLIPPAGE_BPS_DEFAULT = 10.0


def cost_bps(segment="delivery", slippage_bps=SLIPPAGE_BPS_DEFAULT):
    """Round-trip cost in bps of notional (options: bps of PREMIUM), incl. slippage."""
    if segment == "delivery":
        core = STT["delivery_buy"] + STT["delivery_sell"] + OTHER["stamp_delivery_buy"] + OTHER["nse_txn_equity"] + OTHER["sebi_fee"]
    elif segment == "intraday":
        core = STT["intraday_sell"] + OTHER["stamp_intraday_buy"] + OTHER["nse_txn_equity"] + OTHER["sebi_fee"]
    elif segment == "futures":
        core = STT["futures_sell"] + OTHER["stamp_futures_buy"] + OTHER["nse_txn_futures"] + OTHER["sebi_fee"]
    elif segment == "options_premium":
        core = STT["option_premium_sell"] + OTHER["stamp_option_buy"] + OTHER["nse_txn_option_premium"] + OTHER["sebi_fee"]
        return round(core + 4 * slippage_bps, 1)      # option spreads >> equity slippage
    else:
        core = 33.0
    return round(core + slippage_bps, 1)


# ----------------------------------------------------------------- F&O structure (post-reform)
FO = {
    "weekly": {"NIFTY": {"exchange": "NSE", "expiry_weekday": 1, "lot": 75},     # Tuesday
               "SENSEX": {"exchange": "BSE", "expiry_weekday": 3, "lot": 30}},   # Thursday
    "monthly_only": ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "ALL_SINGLE_STOCKS"],
    "lots": {"NIFTY": 75, "BANKNIFTY": 35, "FINNIFTY": 65, "MIDCPNIFTY": 120, "SENSEX": 30},
    "min_contract_value_rs": 1500000,
    "nse_monthly_expiry": "last Tuesday of the month (holiday -> previous trading day)",
    "buyer_pays_full_premium_upfront": True,
    "no_calendar_margin_benefit_on_expiry_day": True,
    "mtf_interest_pct_typical": 16.0,
}


def next_weekly_expiry(symbol="NIFTY", d=None):
    cfg = FO["weekly"].get(symbol.upper())
    if not cfg:
        return None                                    # monthly-only underlying
    d = d or datetime.now(IST).date()
    x = d + timedelta(days=(cfg["expiry_weekday"] - d.weekday()) % 7)
    while not is_trading_day(x):
        x -= timedelta(days=1)
    return x if x >= d else x + timedelta(days=7)


def next_monthly_expiry(d=None):
    d = d or datetime.now(IST).date()
    for probe in (d.replace(day=28), (d.replace(day=1) + timedelta(days=58)).replace(day=28)):
        x = probe
        while x.weekday() != 1:                         # walk back to the last Tuesday
            x -= timedelta(days=1)
        while not is_trading_day(x):
            x -= timedelta(days=1)
        if x >= d:
            return x
    return None


# ----------------------------------------------------------------- circuits & surveillance
CIRCUITS = {
    "index_halt": {"10%": "halt 45m (before 13:00) / 15m (13:00-14:30) / none (after 14:30)",
                   "15%": "halt 1h45 / 45m / rest of day", "20%": "halt for the day"},
    "stock_bands_pct": [2, 5, 10, 20],
    "fo_stocks": "no daily band; 10% dynamic band, flexed in 5% steps",
}
SURVEILLANCE = {
    "ASM": "Additional Surveillance: 4 stages; 100% margin; bands tighten (20%->10%->5%)",
    "GSM": "Graded Surveillance: trade-to-trade (NO intraday exit), 5% band, stage2+ needs 100% ASD",
    "ESM": "Enhanced Surveillance for small-caps (mcap < ~Rs 1000cr): T2T, 2-5% bands",
    "trader_meaning": "Surveillance names are stop-loss traps: T2T blocks intraday exits and "
                      "narrow bands gap-lock through stops. The gate hard-fails GSM/ESM, cautions ASM.",
}
_SURV_CACHE = {"t": 0, "asm": None, "gsm": None}


def fetch_surveillance():
    """Best-effort NSE ASM/GSM lists (Akamai may block non-India IPs). Cached 12h; None = unknown."""
    if time.time() - _SURV_CACHE["t"] < 43200 and _SURV_CACHE["asm"] is not None:
        return _SURV_CACHE["asm"], _SURV_CACHE["gsm"]
    try:
        import requests
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                          "Accept": "application/json", "Referer": "https://www.nseindia.com/"})
        s.get("https://www.nseindia.com", timeout=8)
        asm = {x.get("symbol") for x in (s.get("https://www.nseindia.com/api/reportASM",
                                               timeout=8).json() or {}).get("longterm", [])}
        gsm = {x.get("symbol") for x in (s.get("https://www.nseindia.com/api/reportGSM",
                                               timeout=8).json() or {}).get("data", [])}
        _SURV_CACHE.update({"t": time.time(), "asm": asm, "gsm": gsm})
        return asm, gsm
    except Exception:
        _SURV_CACHE["t"] = time.time()
        return None, None


def surveillance_check(symbol):
    """-> ('GSM'|'ASM'|'clear'|'unknown', note)"""
    asm, gsm = fetch_surveillance()
    if asm is None:
        return "unknown", "NSE surveillance list unreachable from this IP — check manually"
    s = symbol.upper()
    if gsm and s in gsm:
        return "GSM", "GSM: trade-to-trade, 5% band — exits gap-lock; do not trade"
    if asm and s in asm:
        return "ASM", "ASM: 100% margin, tightened bands — reduce size, expect lock days"
    return "clear", "not under exchange surveillance"


# ----------------------------------------------------------------- taxes & settlement
TAX = {
    "stcg_pct": 20.0, "stcg_holding": "< 12 months (listed equity / equity MF)",
    "ltcg_pct": 12.5, "ltcg_exempt_rs": 125000, "ltcg_holding": ">= 12 months",
    "intraday": "speculative business income — slab rates",
    "fno": "non-speculative business income — slab rates",
    "buyback": "taxed as dividend in shareholder hands (since 2024-10)",
    "settlement": "T+1 (T+0 optional beta on top names)",
}

MICROSTRUCTURE_FACTS = [
    "First-hour realized vol runs ~2-3x midday (measured 2.6x on TEJASNET) — stops evaluated 09:15-10:15 get noise-harvested",
    "Index vol peaks at the CLOSE; single-stock vol peaks at the OPEN (measured in the intraday pod)",
    "Expiry-day (Tue NSE / Thu BSE) pinning + gamma flows distort index moves",
    "FII/DII daily cash flows publish ~18:00 IST; quarterly shareholding ~21 days after quarter-end",
    "DII SIP bid (~monthly inflows) is the structural support under dips",
    "Delivery round-trip ~%s bps + slippage; intraday ~%s bps — positional edges survive, intraday edges rarely do" % (
        cost_bps('delivery', 0), cost_bps('intraday', 0)),
]


def get_rules():
    return {"sessions_ist": SESSIONS, "holidays_2026": HOLIDAYS_2026, "muhurat": MUHURAT_2026,
            "is_trading_day_today": is_trading_day(),
            "costs_bps": {k: cost_bps(k) for k in ("delivery", "intraday", "futures", "options_premium")},
            "stt": STT, "fo": FO,
            "next_expiries": {"NIFTY_weekly": str(next_weekly_expiry("NIFTY")),
                              "SENSEX_weekly": str(next_weekly_expiry("SENSEX")),
                              "NSE_monthly": str(next_monthly_expiry())},
            "circuits": CIRCUITS, "surveillance": SURVEILLANCE, "tax": TAX,
            "microstructure": MICROSTRUCTURE_FACTS,
            "asof_rules": "2026-06 (post Budget-2026 STT, post SEBI F&O reform)"}


def main():
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = get_rules()
    print("\nINDIA MARKET RULES — as of", r["asof_rules"])
    print("trading day today:", r["is_trading_day_today"])
    print("costs (bps, incl. slippage): delivery RT %(delivery)s · intraday %(intraday)s · "
          "futures %(futures)s · options %(options_premium)s (of premium)" % r["costs_bps"])
    print("next expiries:", r["next_expiries"])
    print("weekly options: NIFTY (NSE, Tue) + SENSEX (BSE, Thu) ONLY — everything else monthly")
    print("lots:", FO["lots"], "· min contract ~Rs 15L")
    print("tax: STCG %s%% · LTCG %s%% over Rs %s · F&O = business income" % (
        TAX["stcg_pct"], TAX["ltcg_pct"], TAX["ltcg_exempt_rs"]))
    asm, gsm = fetch_surveillance()
    print("surveillance lists:", ("ASM %d / GSM %d names" % (len(asm), len(gsm))) if asm is not None
          else "unreachable from this IP (US) — gate returns 'unknown'")


if __name__ == "__main__":
    main()
