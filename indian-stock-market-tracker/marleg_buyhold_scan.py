"""
marleg_buyhold_scan.py — bulk universe screen for the Buy & Hold pod.

Ranks the whole liquid universe by long-horizon price DURABILITY (3y CAGR, risk-adjusted,
drawdown resilience, above-200-DMA) — the steadiest compounders. Writes marleg_buyhold_cache.json
(served by /api/buyhold_screen). The per-stock view layers fundamentals on top; the screen is
price-only so it can cover ~2000 names quickly. Low-churn by design — run daily/weekly.

  python marleg_buyhold_scan.py
"""
import json, os, sys
import pandas as pd
import yfinance as yf
import marleg_volume_scan as mvs
import marleg_buyhold as bh

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_buyhold_cache.json")
NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    import marleg_options_monitor as mom
    U = sorted(mom.FNO_UNDERLYINGS)     # quality/liquid universe (established names) — right set for buy-and-hold
    print(f"downloading {len(U)} F&O-quality symbols (3y daily) for buy-hold durability screen...")
    rows = []
    CH = 200
    for i in range(0, len(U), CH):
        chunk = U[i:i + CH]
        try:
            data = yf.download([s + ".NS" for s in chunk], period="3y", interval="1d",
                               group_by="ticker", progress=False, threads=True)
        except Exception:
            continue
        for s in chunk:
            try:
                c = data[s + ".NS"]["Close"].dropna()
            except Exception:
                continue
            if len(c) < 600:               # need ~2.4y of real history (drops recent-IPO CAGR mirages)
                continue
            d = bh._durability(c)
            sc = bh.dur_score(d)
            if d is None or sc is None or d.get("cagr3") is None:
                continue
            if d["maxdd"] < -0.55:          # a buy-and-hold name shouldn't more-than-halve; drop catastrophic DD
                continue
            rows.append({"s": s, "n": NAMES.get(s, s), "score": sc,
                         "cagr3_pct": round(d["cagr3"] * 100, 1), "cagr1_pct": round(d["cagr1"] * 100, 1),
                         "sharpe": round(d["sharpe"], 2) if d["sharpe"] is not None else None,
                         "maxdd_pct": round(d["maxdd"] * 100, 1), "above200": d["above200"],
                         "from_52wh_pct": round(d["dist_52wh"] * 100, 1),
                         "price": round(float(c.iloc[-1]), 2)})
        print(f"  {min(i + CH, len(U))}/{len(U)} (kept {len(rows)})")

    # De-saturate + buy-hold-weight: percentile composite (Sharpe-led, CAGR-capped, drawdown-aware)
    # so the ranking spreads 0-100 and rewards steady compounders over volatile rockets.
    def _pr(vals):
        s = pd.Series(vals, dtype="float64")
        return s.rank(pct=True).fillna(0.5).values
    if rows:
        sh = _pr([r["sharpe"] if r["sharpe"] is not None else 0 for r in rows])
        dd = _pr([-(r["maxdd_pct"]) for r in rows])              # shallower drawdown ranks higher (holdability)
        cg = _pr([min(r["cagr3_pct"], 40) for r in rows])        # cap CAGR hard so rockets don't dominate
        for i, r in enumerate(rows):
            comp = 0.40 * dd[i] + 0.35 * sh[i] + 0.15 * cg[i] + \
                   0.10 * ((0.5 if r["above200"] else 0) + (0.5 if r["cagr1_pct"] > 0 else 0))
            r["score"] = int(round(comp * 100))
    rows.sort(key=lambda x: -x["score"])
    from datetime import datetime, timezone, timedelta
    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    json.dump({"asof": ist, "universe": len(rows), "rows": rows},
              open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\nbuy-hold screen: {len(rows)} ranked -> {OUT}")
    for r in rows[:15]:
        print(f"  {r['s']:<12} score {r['score']:>3}  3yCAGR {r['cagr3_pct']:>6}%  Sharpe {r['sharpe']}  maxDD {r['maxdd_pct']}%")


if __name__ == "__main__":
    main()
