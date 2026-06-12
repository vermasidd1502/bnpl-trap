"""
Marle-G — VOLUME INTRADAY BOOK (the daily "thrill" book, paper).

Each session, trade the names where VOLUME is shouting: high 20-day up/down-volume ratio
(ud) + short-term momentum. The rule, kept deliberately simple and testable:

  signal (close of day T)  : ud = sum(up-day vol)/sum(down-day vol) over 20d ; pick the
                             top-N names with ud >= 1.3 AND 3-day momentum > 0
  trade (day T+1)          : buy the basket at the OPEN, sell at the CLOSE — pure intraday
  edge tested              : the basket's daily intraday return, compounded over ~1 year,
                             with win-rate, Sharpe, drawdown — so the "fun" book is honest.

It reuses the Volume Pod's universe + ud signal. Long-only v1. Paper only — no real orders.

  python marleg_volume_book.py
"""
import os, sys, json
import numpy as np, pandas as pd, yfinance as yf
import marleg_volume_scan as vs

HERE = os.path.dirname(os.path.abspath(__file__))
ANN = 252
try:
    NAMES = {k: (val.get("name") or k) for k, val in
             json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json")))["by_symbol"].items()}
except Exception:
    NAMES = {}


def _stats(a, eq):
    mean, sd = a.mean(), (a.std(ddof=1) or 1e-9)
    peak = np.maximum.accumulate(eq)
    maxdd = float((eq / peak - 1).min())
    return {"days": int(len(a)), "avg_daily_pct": round(float(mean) * 100, 3),
            "win_rate": round(float((a > 0).mean()) * 100), "sharpe": round(float(mean / sd) * np.sqrt(ANN), 2),
            "cum_return_pct": round((float(eq[-1]) - 1) * 100, 1), "maxdd_pct": round(maxdd * 100, 1),
            "best_day_pct": round(float(a.max()) * 100, 2), "worst_day_pct": round(float(a.min()) * 100, 2)}


def book(period="1y", topn=5, universe_n=100):
    uni = vs.SEED[:universe_n]
    data = yf.download([s + ".NS" for s in uni], period=period, interval="1d",
                       group_by="ticker", progress=False, threads=True)
    if data is None or data.empty:
        return {"error": "no data for the volume universe"}
    SIG, CLO, OPN, last = {}, {}, {}, {}
    for s in uni:
        try:
            d = data[s + ".NS"][["Open", "Close", "Volume"]].dropna()
        except Exception:
            continue
        if len(d) < 40:
            continue
        r = d["Close"].pct_change()
        upv = d["Volume"].where(r > 0, 0.0)
        dnv = d["Volume"].where(r < 0, 0.0)
        ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
        rvol = d["Volume"] / d["Volume"].rolling(20).mean()
        mom3 = d["Close"].pct_change(3)
        SIG[s] = ((ud >= 1.3) & (mom3 > 0)).astype(float) * ud.fillna(0)
        CLO[s], OPN[s] = d["Close"], d["Open"]
        last[s] = {"name": NAMES.get(s, s), "close": round(float(d["Close"].iloc[-1]), 2),
                   "chg": round(float(r.iloc[-1]) * 100, 2),
                   "ud": round(float(ud.iloc[-1]), 2) if pd.notna(ud.iloc[-1]) else None,
                   "rvol": round(float(rvol.iloc[-1]), 2) if pd.notna(rvol.iloc[-1]) else None}
    if not SIG:
        return {"error": "no volume signals computed"}
    S, C, O = pd.DataFrame(SIG), pd.DataFrame(CLO), pd.DataFrame(OPN)
    idx = S.index

    def _m(x):
        x = x.dropna()
        return float(x.mean()) if len(x) else 0.0

    intr, ovn, full = [], [], []
    for i in range(len(idx) - 1):
        row = S.iloc[i]
        row = row[row > 0]
        if not len(row):
            intr.append(0.0); ovn.append(0.0); full.append(0.0); continue
        picks = row.sort_values(ascending=False).head(topn).index
        o1, c1, c0 = O.iloc[i + 1].reindex(picks), C.iloc[i + 1].reindex(picks), C.iloc[i].reindex(picks)
        intr.append(_m((c1 - o1) / o1))           # open -> close (pure intraday)
        ovn.append(_m((o1 - c0) / c0))            # prev close -> open (overnight gap)
        full.append(_m((c1 - c0) / c0))           # close -> close (hold)

    def _trk(lst):
        a = np.nan_to_num(np.array(lst, dtype=float))
        eq = np.cumprod(1 + a)
        return {**_stats(a, eq), "equity": [round(float(x), 4) for x in eq[:: max(1, len(eq) // 90)]]}

    lastrow = S.iloc[-1]
    lastrow = lastrow[lastrow > 0].sort_values(ascending=False).head(topn)
    picks = [{"sym": s, **last.get(s, {})} for s in lastrow.index]
    return {"asof": str(idx[-1].date()), "topn": topn, "universe": len(SIG), "picks": picks,
            "intraday": _trk(intr), "overnight": _trk(ovn), "full": _trk(full),
            "note": "Signal: 20d up/down-volume (ud≥1.3) + 3d momentum, top-N. INTRADAY = open→close (what you'd day-trade); OVERNIGHT = prev close→open (the gap); FULL = close→close. Paper only — no real orders."}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = book()
    if r.get("error"):
        print(r["error"]); return
    print(f"\nVOLUME INTRADAY BOOK — {r['asof']} · universe {r['universe']} · top-{r['topn']}\n")
    print(f"  {'LEG':<11}{'CUM%':>8}{'SHARPE':>8}{'WIN%':>7}{'AVG/DAY':>9}{'MAXDD%':>8}")
    for leg in ("intraday", "overnight", "full"):
        t = r[leg]
        print(f"  {leg:<11}{t['cum_return_pct']:>8}{t['sharpe']:>8}{t['win_rate']:>7}{t['avg_daily_pct']:>9}{t['maxdd_pct']:>8}")
    print(f"\n  TODAY'S PICKS (top volume-conviction, trade next session):")
    for p in r["picks"]:
        print(f"    {p['sym']:<12}{(p.get('name') or '')[:24]:<26} ud {p.get('ud')}  rvol {p.get('rvol')}  chg {p.get('chg')}%")
    if not r["picks"]:
        print("    (no names pass the volume+momentum filter today)")
    print("\n  " + r["note"])


if __name__ == "__main__":
    main()
