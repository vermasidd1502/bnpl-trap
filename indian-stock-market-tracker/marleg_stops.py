"""
Net the live Groww intraday positions by symbol and propose intraday stop-losses.
net qty = total bought (debit) - total sold (credit).  +long / -short / 0 flat.
Stops: (a) % cap ~1.2% of LTP (bounded MIS risk) and (b) structural = day extreme.
"""
import groww_client as gc
from collections import defaultdict

c = gc.GrowwClient()
pos = (c.positions_data() or {}).get("positions", [])

agg = defaultdict(lambda: {"sold": 0, "bought": 0, "sellv": 0.0, "buyv": 0.0, "real": 0.0})
for p in pos:
    a = agg[p["trading_symbol"]]
    a["sold"] += p.get("credit_quantity", 0)
    a["bought"] += p.get("debit_quantity", 0)
    a["sellv"] += p.get("credit_quantity", 0) * p.get("credit_price", 0)
    a["buyv"] += p.get("debit_quantity", 0) * p.get("debit_price", 0)
    a["real"] += p.get("realised_pnl", 0) / 2.0  # realised is duplicated across the 2 legs

opens, flats = {}, []
for s, a in agg.items():
    net = a["bought"] - a["sold"]
    (opens.__setitem__(s, {**a, "net": net}) if net != 0 else flats.append(s))

print("FLAT / netted (no open risk):", ", ".join(flats) or "none")
print(f"OPEN intraday positions: {len(opens)}\n")

syms = list(opens)
ltp = (gc._safe_json(c.ltp(syms)).get("payload") or {}) if syms else {}
oh = (gc._safe_json(c.ohlc(syms)).get("payload") or {}) if syms else {}

hdr = f"{'SYM':<12}{'DIR':<6}{'QTY':>5}{'ENTRY':>9}{'LTP':>9}{'uP&L':>9}{'dayLo':>8}{'dayHi':>8}{'SL 1.2%':>9}{'risk%':>8}{'SL struct':>10}{'riskS':>8}"
print(hdr); print("-" * len(hdr))
for s, d in sorted(opens.items()):
    es = "NSE_" + s
    px = ltp.get(es)
    o = oh.get(es, {}) or {}
    dayH, dayL = o.get("high"), o.get("low")
    net = d["net"]; long = net > 0; qty = abs(net)
    entry = (d["buyv"] / d["bought"]) if long and d["bought"] else \
            (d["sellv"] / d["sold"]) if (not long) and d["sold"] else None
    if px is None:
        print(f"{s:<12}{'LONG' if long else 'SHORT':<6}{qty:>5}  (no live price)")
        continue
    upl = (px - entry) * qty if long and entry else (entry - px) * qty if entry else 0
    if long:
        sl_pct = round(px * 0.988, 2); sl_str = round((dayL or px) * 0.999, 2)
    else:
        sl_pct = round(px * 1.012, 2); sl_str = round((dayH or px) * 1.001, 2)
    risk_pct = round(abs(px - sl_pct) * qty)
    risk_str = round(abs(px - sl_str) * qty)
    print(f"{s:<12}{'LONG' if long else 'SHORT':<6}{qty:>5}{(round(entry,2) if entry else 0):>9}"
          f"{px:>9}{round(upl):>9}{(dayL or 0):>8}{(dayH or 0):>8}{sl_pct:>9}{risk_pct:>8}{sl_str:>10}{risk_str:>8}")
