"""
Quick bearish (intraday-short) screen over a liquid NSE F&O universe.
Reuses Marle-G's volume/VSA engine (equity_analysis) and ranks the most
bearish setups. Data is daily history (yfinance) -> validate vs live tape.

Run:  python marleg_scan.py
"""
import sys, json
from marleg_server import equity_analysis, NAMES  # noqa: E402

UNIVERSE = [
    "RELIANCE","HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK","BAJFINANCE","BAJAJFINSV",
    "INFY","TCS","WIPRO","HCLTECH","TECHM","BHARTIARTL","ITC","HINDUNILVR","LT","MARUTI",
    "TMPV","TMCV","TATASTEEL","JSWSTEEL","HINDALCO","COALINDIA","ONGC","NTPC","POWERGRID",
    "ADANIENT","ADANIPORTS","SUNPHARMA","ULTRACEMCO","TITAN","DLF",
]

rows = []
for tk in UNIVERSE:
    try:
        r = equity_analysis(tk)
        if isinstance(r, dict) and "error" not in r:
            rows.append(r)
    except Exception as e:
        print(f"  skip {tk}: {str(e)[:60]}", file=sys.stderr)

# bearish = engine says SHORT, or volume-conviction score is clearly negative
shorts = [r for r in rows if r.get("verdict") == "SHORT" or r.get("vcs", 0) <= -2]
# most bearish first; tie-break by higher RVOL (more conviction / tradeable)
shorts.sort(key=lambda r: (r.get("vcs", 0), -r.get("rvol", 0)))

print(f"\nScanned {len(rows)}/{len(UNIVERSE)} | bearish candidates: {len(shorts)}\n")
print(f"{'TICKER':<12}{'LTP':>9}{'CHG%':>7}{'VCS':>5}{'RVOL':>6}{'MFI':>5}{'OBVslp':>8}  SIGNAL")
for r in shorts[:8]:
    vsa = "; ".join(r.get("vsa", [])) or "-"
    print(f"{r['tk']:<12}{r['ltp']:>9}{r['chg']:>7}{r['vcs']:>5}{r['rvol']:>6}{r['mfi']:>5}{r['obvSlope']:>8}  "
          f"{r['verdict']} | {r.get('consensus','')} | {vsa}")
print()
# detail for top 5
for r in shorts[:5]:
    print(f"--- {r['tk']} ({NAMES.get(r['tk'], r['tk'])}) ---")
    print(f"    driver: {r.get('driver','')}")
    confs = [t for t in r.get("ta", []) if t[1] == "CONFIRMS"]
    print(f"    TA confirming short: {', '.join(t[0] for t in confs) or 'none'}")
    print()
