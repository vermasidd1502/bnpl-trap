"""
marleg_indexcompare.py — multi-index OVERLAY. Rebase every index to 100 and lay them on ONE timeline so you
can SEE whether NIFTY follows the Asian lead or diverges. Asia trades before NSE (opens 5:30–7:00 IST), so on
each Indian trading day Asia's bar is already known — that's the 'ahead of India' edge. For the visible window
it reports same-day + next-day follow-correlation and co-direction %, next to the long-run validated follow-rate.

yfinance daily closes, aligned onto NIFTY's trading dates (India = the reference axis). Read-only, no Groww.
"""
from __future__ import annotations

# name, yfinance ticker, line color, default-visible, long-run down>1% follow-rate
INDICES = [
    ("NIFTY",     "^NSEI",     "#ff9933", True,  None),   # the reference line — 'does it follow?'
    ("Singapore", "^STI",      "#60a5fa", True,  82),
    ("KOSPI",     "^KS11",     "#a78bfa", True,  76),
    ("Nikkei",    "^N225",     "#22c55e", True,  70),
    ("Taiwan",    "^TWII",     "#f472b6", False, 76),
    ("HangSeng",  "^HSI",      "#fbbf24", False, 67),
    ("Shanghai",  "000001.SS", "#94a3b8", False, 57),
]
_PERIOD = {"1mo": "1mo", "3mo": "3mo", "6mo": "6mo", "1y": "1y", "2y": "2y"}


def compare(window="6mo"):
    import yfinance as yf, pandas as pd, numpy as np
    period = _PERIOD.get(window, "6mo")

    raw = {}
    for name, tk, _c, _on, _f in INDICES:
        try:
            s = yf.Ticker(tk).history(period=period)["Close"].dropna()
            if len(s) > 5:
                s.index = s.index.tz_localize(None).normalize()
                raw[name] = s[~s.index.duplicated(keep="last")]
        except Exception:
            pass
    if "NIFTY" not in raw:
        return {"ok": False, "error": "NIFTY data unavailable (yfinance)"}

    axis = raw["NIFTY"].index                       # India's trading dates = the reference axis
    nifty_ret = raw["NIFTY"].pct_change()
    series, stats = {}, []
    for name, tk, color, on, follow in INDICES:
        if name not in raw:
            continue
        s = raw[name].reindex(axis).ffill().bfill()  # carry each market's latest close onto India's dates
        base = float(s.iloc[0]) or 1.0
        reb = s / base * 100.0
        series[name] = [{"time": d.strftime("%Y-%m-%d"), "value": round(float(v), 2)}
                        for d, v in reb.items() if pd.notna(v)]
        st = {"name": name, "color": color, "default_on": on, "follow_validated": follow,
              "window_pct": round(float(reb.iloc[-1] - 100.0), 2)}
        if name != "NIFTY":
            aret = s.pct_change()
            df = pd.DataFrame({"n": nifty_ret, "a": aret}).dropna()
            if len(df) > 10:
                st["corr_sameday"] = round(float(df["n"].corr(df["a"])), 2)
                st["codir_pct"] = int(round(float((np.sign(df["n"]) == np.sign(df["a"])).mean() * 100)))
                df2 = pd.DataFrame({"n": nifty_ret, "a": aret.shift(1)}).dropna()
                st["corr_nextday"] = round(float(df2["n"].corr(df2["a"])), 2) if len(df2) > 10 else None
        stats.append(st)

    # order the stats table by same-day correlation (strongest tell on top), NIFTY pinned first
    body = sorted([s for s in stats if s["name"] != "NIFTY"], key=lambda x: x.get("corr_sameday", -9), reverse=True)
    stats = [s for s in stats if s["name"] == "NIFTY"] + body

    return {
        "ok": True, "window": window,
        "start": axis[0].strftime("%Y-%m-%d"), "end": axis[-1].strftime("%Y-%m-%d"), "days": len(axis),
        "series": series, "stats": stats,
        "note": ("rebased to 100 at window start. same-day corr = Asia's morning move vs NIFTY the SAME day "
                 "(Asia leads intraday, so it's the tell); next-day = does NIFTY follow Asia's PREVIOUS close; "
                 "co-dir = % of days NIFTY closed the same direction as that market."),
        "limit": "daily closes only — no foreign order-book / option OI feed exists (same wall as GIFT).",
    }


if __name__ == "__main__":
    import sys, json
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
    w = sys.argv[1] if len(sys.argv) > 1 else "6mo"
    d = compare(w)
    if d.get("ok"):
        print("window %s · %s→%s · %d days" % (d["window"], d["start"], d["end"], d["days"]))
        for s in d["stats"]:
            print(" %-10s win%+7.2f%%  same %s  next %s  co-dir %s%%  follow %s" % (
                s["name"], s["window_pct"], s.get("corr_sameday", "  —"),
                s.get("corr_nextday", "  —"), s.get("codir_pct", " —"), s.get("follow_validated", "—")))
    else:
        print(json.dumps(d))
