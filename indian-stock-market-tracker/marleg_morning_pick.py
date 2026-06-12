"""
Marle-G — MORNING PICK. At ~09:20 IST (after the first 5-min candle), find the 1-2 stocks
showing the strongest OPENING-MOMENTUM ignition — the ones "closest to move" — from the
volume-pod universe we already trust. Opening-Range-Breakout (ORB) style: ride the day's
drive with a trail, exit by 15:05. NOT a scalp (scalping loses to cost — tested).

Score per candidate (long bias — we're long-only):
  gap      today open vs prev close            (a catalyst gapped it)
  range1   first 5-min candle range / 5m-ATR   (energy in the open)
  vol1     first-candle volume / its avg        (real participation)
  trend    above 50d & 200d MA                  (go with the tide)
  udzone   already in the 2.0-3.6 volume go-to zone (aligned with the positional edge)

Candidates = gated longs + volume-pod leaders + your holdings (liquid, fast).
Outputs the ORB plan (entry on first-candle-high break, stop at first-candle low, ride+trail)
to marleg_morning_pick.json and Slack. PAPER/decision-support — never places an order.

  python marleg_morning_pick.py
"""
import sys, os, json
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
OUT = os.path.join(HERE, "marleg_morning_pick.json")


def _candidates():
    s = set()
    for f, key, sub in [("marleg_gated_cache.json", "picks", "s"),
                        ("marleg_volume_cache.json", None, None)]:
        try:
            d = json.load(open(os.path.join(HERE, f), encoding="utf-8"))
            if f.startswith("marleg_gated"):
                s |= {p["s"] for p in d.get("picks", [])}
            else:
                for sec in d.get("sectors", []):
                    for x in sec.get("stocks", []):
                        if (x.get("ud") or 0) > 1.3:
                            s.add(x["s"])
        except Exception:
            pass
    # plus current holdings
    try:
        import groww_client as gc
        c = gc.GrowwClient(); c.token()
        for h in (c.holdings_data() or {}).get("holdings") or []:
            if (h.get("quantity") or 0) > 0:
                s.add(h.get("trading_symbol"))
    except Exception:
        pass
    return sorted(x for x in s if x)[:60]


def _wait_for_open_candle(c, today, tries=18, gap=20):
    """Groww's historical-candle feed lags the just-closed bar by a few minutes.
    Poll a always-liquid proxy until today's first 5-min bar appears (or give up)."""
    import time
    for _ in range(tries):
        try:
            df = c.candles("RELIANCE", 5, 3)
            if df is not None and len(df) and df.index[-1].date() == today:
                return True
        except Exception:
            pass
        time.sleep(gap)
    return False


def run():
    import groww_client as gc
    import marleg_datastore as ds
    c = gc.GrowwClient(); c.token()
    cands = _candidates()
    today = datetime.now(IST).date()
    now = datetime.now(IST)
    # only wait during the morning window (don't stall an after-hours manual run)
    if (now.hour, now.minute) >= (9, 15) and now.hour < 12 and now.weekday() < 5:
        if not _wait_for_open_candle(c, today):
            return {"asof": now.strftime("%Y-%m-%d %H:%M IST"), "n_scanned": 0, "pre_open": True,
                    "picks": [], "ranked": [], "plan": "", "note": "today's first candle not published yet"}
    rows = []; stale = 0
    for sym in cands:
        try:
            df = c.candles(sym, 5, 10)               # ~10 sessions of 5-min, official
            if df is None or len(df) < 50:
                continue
            df = df.copy(); df["d"] = [t.date() for t in df.index]
            if df["d"].iloc[-1] != today:            # today's first candle not printed yet -> pre-open
                stale += 1
                continue
            today_bars = df[df["d"] == today]
            c1 = today_bars.iloc[0]                   # first 5-min candle (09:15-09:20)
            prev = df[df["d"] < today]
            if prev.empty:
                continue
            prev_close = float(prev["close"].iloc[-1])
            op = float(c1["open"]); hi1 = float(c1["high"]); lo1 = float(c1["low"]); vol1 = float(c1["volume"])
            gap = (op / prev_close - 1) * 100
            range1 = (hi1 - lo1)
            # baselines: this open vs the stock's OWN typical opening candle (apples-to-apples)
            firsts = df[df["d"] < today].groupby("d").first().tail(10)
            avg_first_vol = float(firsts["volume"].mean()) or vol1
            avg_first_range = float((firsts["high"] - firsts["low"]).mean()) or range1
            volx = vol1 / avg_first_vol if avg_first_vol else 1
            range_x = range1 / avg_first_range if avg_first_range else 1
            atr5 = avg_first_range
            # daily trend + ud
            trend = 0; ud = None
            try:
                dd = ds.series(sym).dropna()
                if len(dd) > 200:
                    s50 = dd.rolling(50).mean().iloc[-1]; s200 = dd.rolling(200).mean().iloc[-1]
                    trend = 1 if (op > s50 and op > s200) else (-1 if op < s50 and op < s200 else 0)
            except Exception:
                pass
            # score (long bias): reward up-gap, big first candle, volume surge, uptrend
            score = (np.clip(gap, -3, 6) * 1.0
                     + np.clip(range_x, 0, 4) * 1.5
                     + np.clip(volx, 0, 5) * 1.2
                     + trend * 2.0)
            rows.append({"sym": sym, "prev_close": round(prev_close, 1), "open": round(op, 1),
                         "ltp": round(float(today_bars["close"].iloc[-1]), 1),
                         "gap_pct": round(gap, 2), "range_x": round(range_x, 2),
                         "vol_x": round(volx, 2), "trend": trend, "atr5": round(atr5, 2),
                         "entry_trigger": round(hi1, 1), "stop": round(lo1, 1),
                         "target": round(hi1 + 2 * (hi1 - lo1), 1), "score": round(float(score), 2)})
        except Exception:
            continue
    rows.sort(key=lambda x: -x["score"])
    picks = [r for r in rows if r["trend"] >= 0 and r["gap_pct"] > -1][:2]   # long-only, not gapping down
    pre_open = (len(rows) == 0 and stale > 0)
    out = {"asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"), "n_scanned": len(rows),
           "pre_open": pre_open,
           "picks": picks, "ranked": rows[:8],
           "plan": ("ORB long: enter on a break above the first-candle high (entry_trigger), "
                    "stop at first-candle low, ride with a trailing stop, EXIT 15:05 IST. "
                    "Size by 1% risk (entry-stop). Paper-first; not advice."),
           "note": "Opening-momentum ignition from the volume-pod universe. Intraday is cost-heavy "
                   "— this is the active/thrill book, ride-with-trail, never a scalp."}
    json.dump(out, open(OUT, "w"), indent=1)
    try:
        import marleg_slack as sl
        if picks:
            lines = [f"*{p['sym']}*: gap {p['gap_pct']:+}%, vol {p['vol_x']}× · enter > {p['entry_trigger']}, "
                     f"stop {p['stop']}, target {p['target']} (2:1)" for p in picks]
            lines.append("Ride with a trailing stop; exit by 15:05 IST. Paper-first until it proves out.")
            sl.card(f"☀ *MORNING PICKS* — opening-momentum (ORB) longs, {out['asof']}",
                    lines=lines,
                    action="Enter ONLY on a break above the trigger with volume; size by 1% risk to the stop. "
                           "Skip if the name is flagged extended (don't-chase).")
    except Exception:
        pass
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = run()
    print(f"\nMORNING PICK — {r['asof']} · scanned {r['n_scanned']}")
    if r.get("pre_open"):
        print("  PRE-OPEN — today's first 5-min candle hasn't printed yet. Run at/after 09:20 IST.")
        return
    print("TOP PICKS (ORB long, ride+trail, exit 15:05):")
    for p in r["picks"]:
        print(f"  {p['sym']:<12} gap {p['gap_pct']:+5}% range {p['range_x']}x vol {p['vol_x']}x trend {p['trend']} "
              f"-> enter>{p['entry_trigger']} stop {p['stop']} tgt {p['target']} (score {p['score']})")
    if not r["picks"]:
        print("  (nothing igniting cleanly — sit out, that's a valid call)")
    print("\nnext ranked:")
    for p in r["ranked"][:5]:
        print(f"  {p['sym']:<12} score {p['score']} gap {p['gap_pct']:+}% vol {p['vol_x']}x trend {p['trend']}")


if __name__ == "__main__":
    main()
