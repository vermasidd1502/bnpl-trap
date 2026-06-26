"""
marleg_events.py — dated EVENT BUBBLES for the charts (TradingView-style) + a recent-news panel.

For a stock/index, returns dated events to drop on the chart as markers and list in a "what happened" log:
  • earnings (best-effort)        • gap days (open vs prev close ≥ 2.5%)
  • dividends / splits            • 52-week high / low touches
  • volume spikes (≥2.5× 20d avg) • F&O monthly expiries (if it's an F&O name)
Each event carries a lightweight-charts marker spec (shape/color/position/text) + a detail line. Plus a
RECENT-NEWS list (yfinance headlines) for the "Latest updates" panel — honestly *recent*, not a dated
bubble on every candle (we don't have a historical dated-news feed).

Read-only, pure compute. Decision-support.

  python marleg_events.py RELIANCE
"""
import sys


def events(tk, period="1y"):
    import marleg_vol as mv
    import marleg_data as md
    import numpy as np
    import datetime as dt
    tk = tk.upper()
    days = {"6mo": 190, "1y": 380, "2y": 760}.get(period, 380)
    df = md.candles(tk, 1440, days)          # Groww-only (no yfinance)
    if df is None or len(df) < 30:
        return {"ok": False, "tk": tk, "error": "short history"}
    h = df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close",
                           "volume": "Volume"})[["Open", "High", "Low", "Close", "Volume"]].dropna()
    idx = h.index
    O, H, L, C, V = h["Open"].values, h["High"].values, h["Low"].values, h["Close"].values, h["Volume"].values
    first = idx[0].date()
    ev = []

    def add(d, typ, label, detail, shape, color, pos, text):
        ev.append({"time": d, "type": typ, "label": label, "detail": detail,
                   "shape": shape, "color": color, "position": pos, "text": text})

    # gaps
    for i in range(1, len(C)):
        g = (O[i] / C[i - 1] - 1) * 100
        if abs(g) >= 2.5:
            up = g > 0
            add(idx[i].strftime("%Y-%m-%d"), "gap", f"Gap {'+' if up else ''}{g:.1f}%",
                f"Opened {'+' if up else ''}{g:.1f}% vs the prior close", "arrowUp" if up else "arrowDown",
                "#22c55e" if up else "#ef4444", "belowBar" if up else "aboveBar", "gap")
    # 52w high / low touches (dedup within 10 sessions)
    hi = h["High"].rolling(252, min_periods=60).max().values
    lo = h["Low"].rolling(252, min_periods=60).min().values
    lastH = lastL = -99
    for i in range(60, len(C)):
        if not np.isnan(hi[i]) and H[i] >= hi[i] * 0.999 and i - lastH > 10:
            lastH = i; add(idx[i].strftime("%Y-%m-%d"), "52wH", "52-week high", f"New 52w high ₹{round(float(H[i]),1)}", "circle", "#22c55e", "aboveBar", "52wH")
        if not np.isnan(lo[i]) and L[i] <= lo[i] * 1.001 and i - lastL > 10:
            lastL = i; add(idx[i].strftime("%Y-%m-%d"), "52wL", "52-week low", f"New 52w low ₹{round(float(L[i]),1)}", "circle", "#ef4444", "belowBar", "52wL")
    # volume spikes
    av = h["Volume"].rolling(20).mean().values
    lastVS = -99
    for i in range(20, len(C)):
        if av[i] and V[i] > 2.5 * av[i] and i - lastVS > 4:
            lastVS = i; mult = V[i] / av[i]
            add(idx[i].strftime("%Y-%m-%d"), "vol", f"Volume {mult:.1f}×", f"Volume {mult:.1f}× the 20-day average", "square", "#3b82f6", "belowBar", "V")
    # (dividends / splits / earnings dates dropped — those were yfinance-only; Groww has no corporate-action feed.
    #  Price-derived bubbles below stay. Earnings still show via the F&O-expiry + news panels.)
    # F&O expiries (if it's an F&O name)
    try:
        import marleg_options_monitor as mom
        if mom.has_options(tk):
            y, m = first.year, first.month
            today = dt.date.today()
            while dt.date(y, m, 1) <= today:
                e = mv.monthly_expiry(y, m)
                if first <= e <= today:
                    add(e.strftime("%Y-%m-%d"), "expiry", "F&O expiry", "Monthly F&O expiry (last Tue)", "square", "#646c7a", "aboveBar", "X")
                y, m = (y + (m == 12)), (1 if m == 12 else m + 1)
    except Exception:
        pass

    ev.sort(key=lambda x: x["time"])
    news = []      # news now comes from the pod's Google-News endpoint (/api/news), not yfinance

    return {"ok": True, "tk": tk, "n": len(ev), "events": ev[-40:], "news": news,
            "note": "Dated event bubbles (gaps / 52-week / volume-spike / F&O-expiry) derived from Groww price "
                    "history. Recent news is on the options page's news panel (Google News)."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = events(sys.argv[1] if len(sys.argv) > 1 else "RELIANCE")
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"\n  {r['tk']} — {r['n']} events:")
    for e in r["events"][-18:]:
        print(f"    {e['time']}  [{e['type']:<6}] {e['label']:<18} {e['detail']}")
    print(f"\n  recent news ({len(r['news'])}):")
    for n in r["news"][:5]:
        print(f"    {n.get('source')}: {n.get('title')}")
