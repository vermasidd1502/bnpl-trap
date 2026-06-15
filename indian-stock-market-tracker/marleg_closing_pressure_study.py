"""
marleg_closing_pressure_study.py — does the MIS square-off actually create a tradeable
closing-pressure edge on the NSE intraday tape?

Three questions (walk-forward, 5-min Groww candles, ~40 days, ~30 liquid names — throttle-proof):

  Q1  CLOSE-WINDOW DRIFT   — what does 15:00 -> close (15:25/30) do, split by how the day is
                            positioned at 14:00? If "crowded long" days (up + above VWAP) fade
                            into the close and "crowded short" days pop, the square-off reverses
                            the crowded side (the whole thesis).
  Q2  COST OF HOLDING THE  — for an intraday long, is the last 15-20 min (15:10/15:15 -> close)
      LAST 20 MIN           +EV or -EV? If negative on crowded-long days, exit before 15:15.
  Q3  IS THE DIP MECHANICAL — after a sold-off close, does the stock bounce at the NEXT open?
                            (close[d] -> open[d+1]). If yes, the close dip is a *better fill*,
                            not a sell signal.
  +   CLV -> next-open: does closing in the top of the day's range predict next-day strength?

Writes marleg_closing_pressure_study.json. Research artifact (gitignored).
"""
import os
import sys
import json
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_closing_pressure_study.json")
COST = 0.07            # MIS round-trip %, ~ brokerage*2 + STT(sell) + slippage

UNIVERSE = ["RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK", "INDUSINDBK",
            "INFY", "TCS", "HCLTECH", "ITC", "HINDUNILVR", "LT", "BHARTIARTL", "BAJFINANCE",
            "MARUTI", "TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "ADANIENT", "ADANIPORTS",
            "SAIL", "PNB", "BANKBARODA", "TEJASNET", "QUESS", "AVANTEL", "DLINKINDIA", "RBLBANK"]


def day_records(sym, df):
    """One record per trading day with the timing-point prices + day classification."""
    recs = []
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    for day, d in df.groupby(df.index.normalize()):
        d = d.sort_index()
        if len(d) < 60:                          # need a near-complete session
            continue
        vwap = (tp.loc[d.index] * d["volume"]).cumsum() / d["volume"].cumsum()
        tmap = {t.strftime("%H:%M"): i for i, t in enumerate(d.index)}

        def cl(hhmm):
            i = tmap.get(hhmm)
            return float(d["close"].iloc[i]) if i is not None else np.nan

        def vw(hhmm):
            i = tmap.get(hhmm)
            return float(vwap.iloc[i]) if i is not None else np.nan

        o = float(d["open"].iloc[0])
        close = float(d["close"].iloc[-1])
        last_open = float(d["open"].iloc[-1])               # the final 5-min candle (15:25 bar)
        last_hi, last_lo = float(d["high"].iloc[-1]), float(d["low"].iloc[-1])
        hi, lo = float(d["high"].max()), float(d["low"].min())
        p1400, p1500, p1510, p1515 = cl("14:00"), cl("15:00"), cl("15:10"), cl("15:15")
        if any(np.isnan(x) for x in (o, p1400, p1500, p1510, close)) or o <= 0:
            continue
        dret14 = p1400 / o - 1
        abv14 = p1400 > vw("14:00")
        crowd = "long" if (dret14 > 0.005 and abv14) else "short" if (dret14 < -0.005 and not abv14) else "neutral"
        recs.append({"sym": sym, "day": str(day.date()), "open": o, "close": close,
                     "cw_ret": close / p1500 - 1,                 # 15:00 -> close
                     "l20_ret": close / p1510 - 1,                # 15:10 -> close (the square-off)
                     "l15_ret": (close / p1515 - 1) if not np.isnan(p1515) else np.nan,  # 15:15 -> close
                     "clv": (close - lo) / (hi - lo) if hi > lo else 0.5,
                     # the LAST 5-min candle itself: its body direction + where it closed within its own range
                     "last_candle": (close / last_open - 1) if last_open > 0 else 0.0,
                     "last_clv": (close - last_lo) / (last_hi - last_lo) if last_hi > last_lo else 0.5,
                     "crowd": crowd})
    # link next-day open + next-day INTRADAY (open->close) within this symbol
    for i in range(len(recs) - 1):
        recs[i]["nextopen_ret"] = recs[i + 1]["open"] / recs[i]["close"] - 1
        recs[i]["nextday_intra"] = recs[i + 1]["close"] / recs[i + 1]["open"] - 1   # does the close predict tomorrow's intraday?
    return recs


def agg(rows, key):
    vals = np.array([r[key] for r in rows if r.get(key) is not None and not (isinstance(r.get(key), float) and np.isnan(r[key]))]) * 100
    if not len(vals):
        return {"n": 0}
    return {"n": len(vals), "avg": round(float(vals.mean()), 3), "med": round(float(np.median(vals)), 3),
            "pos%": round(float((vals > 0).mean()) * 100, 1)}


def main():
    import groww_client as gc
    g = gc.GrowwClient(); g.token()
    allrecs = []
    got = 0
    for sym in UNIVERSE:
        try:
            df = g.candles(sym, interval_min=5, days=60)
            if df is None or df.empty:
                print(f"  {sym}: no data"); continue
            r = day_records(sym, df)
            allrecs += r; got += 1
            print(f"  {sym}: {len(r)} days")
        except Exception as e:
            print(f"  {sym}: {e}")
    print(f"\n{got}/{len(UNIVERSE)} names · {len(allrecs)} stock-days\n")
    if not allrecs:
        print("no data"); return

    byc = {c: [r for r in allrecs if r["crowd"] == c] for c in ("long", "short", "neutral")}
    res = {
        "Q1_close_window_15to_close": {c: agg(byc[c], "cw_ret") for c in byc} | {"ALL": agg(allrecs, "cw_ret")},
        "Q2_last20_1510_to_close": {c: agg(byc[c], "l20_ret") for c in byc} | {"ALL": agg(allrecs, "l20_ret")},
        "Q2b_last15_1515_to_close": {c: agg(byc[c], "l15_ret") for c in byc} | {"ALL": agg(allrecs, "l15_ret")},
    }
    # Q3: after a sold-off close (cw_ret < -0.2%) vs strong close (> +0.2%), next-day open gap
    sold = [r for r in allrecs if r.get("nextopen_ret") is not None and r["cw_ret"] < -0.002]
    strong = [r for r in allrecs if r.get("nextopen_ret") is not None and r["cw_ret"] > 0.002]
    res["Q3_nextopen_after_close"] = {"after_soldoff_close": agg(sold, "nextopen_ret"),
                                      "after_strong_close": agg(strong, "nextopen_ret")}
    # CLV bucket -> next-open
    hicl = [r for r in allrecs if r.get("nextopen_ret") is not None and r["clv"] > 0.7]
    locl = [r for r in allrecs if r.get("nextopen_ret") is not None and r["clv"] < 0.3]
    res["CLV_to_nextopen"] = {"closed_top30%": agg(hicl, "nextopen_ret"), "closed_bottom30%": agg(locl, "nextopen_ret")}
    # Q4: does the last-5-min/close predict the NEXT DAY's INTRADAY (open->close)?
    res["Q4_close_to_nextday_intraday"] = {
        "CLV_closed_top30%": agg(hicl, "nextday_intra"), "CLV_closed_bottom30%": agg(locl, "nextday_intra"),
        "after_soldoff_close": agg(sold, "nextday_intra"), "after_strong_close": agg(strong, "nextday_intra")}
    # Q5: the LAST 5-min candle's OWN body direction (the "last candlestick of day" theory)
    lc = [r for r in allrecs if r.get("nextday_intra") is not None]
    green = [r for r in lc if r["last_candle"] > 0.0005]      # green final candle (closed up)
    red = [r for r in lc if r["last_candle"] < -0.0005]       # red final candle (closed down)
    strongclose_candle = [r for r in lc if r["last_clv"] > 0.7]   # closed in top of its own last-candle range
    res["Q5_last_candle_direction"] = {
        "green_final_candle__nextopen": agg(green, "nextopen_ret"),
        "red_final_candle__nextopen": agg(red, "nextopen_ret"),
        "green_final_candle__nextintraday": agg(green, "nextday_intra"),
        "red_final_candle__nextintraday": agg(red, "nextday_intra"),
        "final_candle_closed_strong__nextintraday": agg(strongclose_candle, "nextday_intra")}

    payload = {"universe": got, "stock_days": len(allrecs), "cost_pct": COST, "results": res}
    json.dump(payload, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    def show(title, block):
        print(title)
        for k, v in block.items():
            if v.get("n"):
                print(f"    {k:<22} n={v['n']:<5} avg {v['avg']:>7}%  med {v['med']:>7}%  pos {v['pos%']:>5}%")
            else:
                print(f"    {k:<22} (no data)")
    print("=" * 64)
    show("Q1 · close-window 15:00->close (square-off reverses crowded side?)", res["Q1_close_window_15to_close"])
    show("Q2 · last 20 min 15:10->close (cost of holding into square-off?)", res["Q2_last20_1510_to_close"])
    show("Q2b· last 15 min 15:15->close", res["Q2b_last15_1515_to_close"])
    show("Q3 · next-day OPEN after close (is the dip mechanical?)", res["Q3_nextopen_after_close"])
    show("CLV· close-location -> next-day open", res["CLV_to_nextopen"])
    show("Q4 · close -> NEXT-DAY INTRADAY (open->close) — does the close predict tomorrow's session?", res["Q4_close_to_nextday_intraday"])
    show("Q5 · the LAST 5-min CANDLE itself (green/red body) -> next day", res["Q5_last_candle_direction"])
    print(f"\n(cost ~{COST}% round-trip MIS; close-window moves are small — read the SIGN + pos%, not just size)")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
