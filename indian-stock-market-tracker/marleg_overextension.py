"""
marleg_overextension.py — the DON'T-CHASE guard.

The rule (user's insight, data-confirmed): real momentum is a beaten-down stock RECOVERING
with room to run (TEJASNET: -18% from its 52w high, +53% off its low) — NOT a name that has
already rallied into its ceiling and is 'hot' (GRASIM 89%ile of range, -2% from high;
APOLLOHOSP at the 52w high, 98%ile). A stock parked at its 'golden number' — the 52w / prior
swing high, where fib extensions cluster — has capped upside and high reversal risk, so we
DON'T chase it even when the gate or the broker shows green.

Two timescales, kept separate on purpose:
  STRUCTURAL   distance below the 52w high + position in the 52w range  -> room vs ceiling
  SHORT-TERM   stretch above the 20/50-DMA + daily RSI                  -> chase-the-spike risk

A name can be structurally fresh yet short-term hot (QUESS, TEJAS on a spike) — that reads
"right thesis, wait for a pullback", not "buy now".

  chase_check(tk) -> dict (structural, short-term, freshness 0-100, verdict)
  python marleg_overextension.py GRASIM APOLLOHOSP TEJASNET QUESS
"""
import sys, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, yfinance as yf


def _rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def chase_check(tk):
    tk = tk.upper()
    df = yf.download(tk + ".NS", period="1y", interval="1d", progress=False, auto_adjust=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    c = df["Close"].dropna()
    if len(c) < 120:
        return {"tk": tk, "error": "insufficient daily history"}
    px = float(c.iloc[-1])
    ma20 = float(c.rolling(20).mean().iloc[-1]); ma50 = float(c.rolling(50).mean().iloc[-1])
    ma200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else None
    hi52 = float(c.tail(252).max()); lo52 = float(c.tail(252).min())
    rsi = float(_rsi(c).iloc[-1])
    room_pct = round((1 - px / hi52) * 100, 1)                    # how far below the 52w high (room)
    range_pos = round((px - lo52) / (hi52 - lo52) * 100) if hi52 > lo52 else 50
    stretch20 = round((px / ma20 - 1) * 100, 1); stretch50 = round((px / ma50 - 1) * 100, 1)
    ret5 = round((px / float(c.iloc[-6]) - 1) * 100, 1); ret20 = round((px / float(c.iloc[-21]) - 1) * 100, 1)
    ret60 = round((px / float(c.iloc[-61]) - 1) * 100, 1)
    lo60 = float(c.tail(60).min()); recovery60 = round((px / lo60 - 1) * 100, 1)

    at_ceiling = range_pos >= 85 or room_pct <= 5
    hot = rsi >= 70 or stretch50 >= 15
    recovering = px > ma50 and ret20 > 3
    has_room = room_pct >= 12

    # freshness score: high = clean beaten-down recovery with room; low = chasing a topped-out name
    score = 50.0
    score += min(25.0, max(0.0, room_pct))                       # room below the ceiling
    score -= max(0.0, range_pos - 80) * 2.0                      # parked near the 52w high
    score -= max(0.0, rsi - 70) * 1.5                            # short-term overbought
    score -= max(0.0, stretch50 - 12) * 1.0                      # stretched from the 50-DMA
    score += 10.0 if recovering else -10.0                       # confirmed turning up vs drifting/falling
    score = int(max(0, min(100, round(score))))

    if at_ceiling:
        structural = "AT CEILING"
        verdict = "🔴 DON'T CHASE — extended near the 52w high; upside capped, reversal risk high"
    elif has_room and recovering and not hot:
        structural = "ROOM TO RUN"
        verdict = "🟢 REAL MOMENTUM — beaten down and recovering with room to run"
    elif has_room and recovering and hot:
        structural = "ROOM TO RUN"
        verdict = "🟡 RIGHT THESIS, BUT HOT — room to run, yet overbought now; wait for a pullback (toward 20-DMA / VWAP) to enter"
    elif hot:
        structural = "MID-RANGE"
        verdict = "🟡 HOT — stretched short-term; don't chase the spike"
    else:
        structural = "MID-RANGE"
        verdict = "⚪ NEUTRAL — neither extended nor a clean recovery setup"

    short_term = "OVERBOUGHT" if hot else ("OVERSOLD" if rsi < 35 else "NORMAL")
    pullback_to = round(ma20, 1) if hot else None                # where to wait for a hot name
    return {"tk": tk, "px": round(px, 2), "score": score, "structural": structural, "short_term": short_term,
            "at_ceiling": at_ceiling, "hot": hot, "has_room": has_room, "recovering": recovering,
            "room_to_52w_high_pct": room_pct, "range_pos_pct": range_pos, "from_52w_high_pct": -room_pct,
            "rsi": round(rsi, 0), "stretch_20dma_pct": stretch20, "stretch_50dma_pct": stretch50,
            "ret5_pct": ret5, "ret20_pct": ret20, "ret60_pct": ret60, "recovery_off_60d_low_pct": recovery60,
            "golden_number": round(hi52, 1), "pullback_entry": pullback_to, "verdict": verdict}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for tk in (sys.argv[1:] or ["GRASIM", "APOLLOHOSP", "TEJASNET", "QUESS"]):
        r = chase_check(tk)
        if r.get("error"):
            print(f"{tk}: {r['error']}"); continue
        print(f"\n{r['tk']}  px {r['px']}  freshness {r['score']}/100  [{r['structural']} · {r['short_term']}]")
        print(f"   {r['from_52w_high_pct']}% from 52w-high (room {r['room_to_52w_high_pct']}%, {r['range_pos_pct']}%ile) · "
              f"RSI {r['rsi']} · +{r['stretch_50dma_pct']}% vs 50-DMA · 20d {r['ret20_pct']}% · off-low {r['recovery_off_60d_low_pct']}%")
        print(f"   golden number (52w high) {r['golden_number']}" + (f" · wait for pullback ~{r['pullback_entry']}" if r['pullback_entry'] else ""))
        print(f"   ➤ {r['verdict']}")


if __name__ == "__main__":
    main()
