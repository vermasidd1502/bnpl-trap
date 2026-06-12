"""Settle the TEJASNET anchor question: official Groww/NSE candles vs Yahoo."""
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from groww_client import GrowwClient

c = GrowwClient()
r = c._get("/v1/historical/candle/range", {
    "exchange": "NSE", "segment": "CASH", "trading_symbol": "TEJASNET",
    "start_time": "2026-01-01 09:15:00", "end_time": "2026-06-11 15:30:00",
    "interval_in_minutes": "1440"})
print("groww status:", r.status_code)
j = r.json()
payload = j.get("payload", j)
candles = payload.get("candles") or []
print("bars:", len(candles))
rows = []
for k in candles:
    ts = datetime.fromtimestamp(k[0])
    rows.append((ts.date(), float(k[1]), float(k[2]), float(k[3]), float(k[4])))

if rows:
    lo = min(rows, key=lambda x: x[3])
    hi = max(rows, key=lambda x: x[2])
    print(f"\nGROWW/NSE official:")
    print(f"  lowest low   : {lo[3]:8.2f}  on {lo[0]}")
    print(f"  highest high : {hi[2]:8.2f}  on {hi[0]}")
    print(f"\n  daily bars, May 28 - Jun 11:")
    for d, o, h, l, cl in rows:
        if d >= datetime(2026, 5, 28).date():
            print(f"    {d}  O {o:7.2f}  H {h:7.2f}  L {l:7.2f}  C {cl:7.2f}")

import pandas as pd
import yfinance as yf
df = yf.download("TEJASNET.NS", start="2026-05-28", end="2026-06-12", interval="1d",
                 progress=False, auto_adjust=False)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)
print("\nYAHOO dailies, same window:")
for d, row in df.iterrows():
    print(f"    {d.date()}  O {row['Open']:7.2f}  H {row['High']:7.2f}  L {row['Low']:7.2f}  C {row['Close']:7.2f}")
