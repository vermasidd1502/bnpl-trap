"""
Swing-trade screen: rank the liquid NSE universe by multi-pillar alignment
(volume conviction + TA confirmation incl. Ichimoku + hedge-fund archetype consensus).
Daily-bar engine -> swing horizon (days-weeks). Validate vs live tape; use stops.
"""
import re
from marleg_server import equity_analysis, NAMES

U = ["RELIANCE","HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK","BAJFINANCE","INFY","TCS",
     "WIPRO","HCLTECH","LT","BHARTIARTL","ITC","HINDUNILVR","NESTLEIND","ASIANPAINT","TITAN",
     "MARUTI","TATASTEEL","JSWSTEEL","HINDALCO","VEDL","COALINDIA","ONGC","NTPC","POWERGRID",
     "ADANIENT","ADANIPORTS","ADANIPOWER","DLF","SUNPHARMA","CIPLA","ULTRACEMCO","BEL","HAL"]

rows = []
for tk in U:
    try:
        r = equity_analysis(tk)
        if not isinstance(r, dict) or "error" in r:
            continue
        ta = r.get("ta", [])
        confirms = sum(1 for t in ta if t[1] == "CONFIRMS")
        conflicts = sum(1 for t in ta if t[1] == "CONFLICTS")
        ichi = r.get("ichimoku", {}) or {}
        m = re.search(r"RSI ([\d.]+)", ta[0][0]) if ta else None
        rsi = float(m.group(1)) if m else None
        rows.append({"tk": tk, "v": r["verdict"], "conv": r["conv"], "vcs": r["vcs"], "rsi": rsi,
                     "mfi": r["mfi"], "ud": r["ud"], "rvol": r["rvol"], "cloud": ichi.get("cloud", "?"),
                     "idir": ichi.get("dir", 0), "cf": confirms, "cx": conflicts,
                     "L": r["longs"], "S": r["shorts"]})
    except Exception as e:
        print("skip", tk, str(e)[:50])

def sc(x):
    return x["conv"] + 6*x["cf"] - 4*x["cx"] + 8*(1 if x["idir"] > 0 else -1 if x["idir"] < 0 else 0) + 3*(x["L"] - x["S"])

longs = sorted([x for x in rows if x["v"] == "LONG" and x["idir"] >= 0], key=lambda x: -sc(x))
shorts = sorted([x for x in rows if x["v"] == "SHORT" and x["idir"] <= 0], key=lambda x: sc(x))

def show(title, lst, short=False):
    print(f"\n=== {title} ({len(lst)}) ===")
    print(f"{'TKR':<12}{'conv':>5}{'vcs':>5}{'RSI':>6}{'MFI':>5}{'U/D':>6}{'TAok':>6}{'cloud':>13}{'cons':>10}  flag")
    for x in lst[:8]:
        cons = f"{x['L']}L/{x['S']}S"
        flag = ""
        if short and x["rsi"] and x["rsi"] < 32: flag = "OVERSOLD-squeeze risk"
        if (not short) and x["rsi"] and x["rsi"] > 72: flag = "EXTENDED-overbought"
        print(f"{x['tk']:<12}{x['conv']:>5}{x['vcs']:>5}{(x['rsi'] or 0):>6}{x['mfi']:>5}{x['ud']:>6}"
              f"{str(x['cf'])+'/'+str(x['cf']+x['cx']):>6}{x['cloud']:>13}{cons:>10}  {flag}")

print(f"scanned {len(rows)}/{len(U)}")
show("SWING LONGS (pillars aligned bullish)", longs)
show("SWING SHORTS (pillars aligned bearish)", shorts, short=True)
