"""
Live gated-longs screen (O'Neil/CAN-SLIM top-down confluence). A stock qualifies
only when ALL gates fire at the latest bar:
  1. INDUSTRY GROUP : its granular industry is top-40% by leadership (momentum + breadth).
                      Falls back to the macro sector for thin industries (<3 names).
                      Backtested upgrade: marleg_industry_gate_study showed the old COARSE
                      sector gate was value-destroying at 10-21d (-0.27/-0.50% vs base),
                      while the granular industry gate adds value (-0.06/-0.17%) — strictly
                      better at every horizon. "Leader in a leading group", done right.
  2. VOLUME         : U/D ratio > its own 50-day MA  AND  rising (vs 10d ago)
  3. FIBONACCI      : price above the 0.618 retracement of its 120-day range
Writes marleg_gated_cache.json (served by /api/gated) + marleg_industry_rs_cache.json.
"""
import json, os, sys, time
import numpy as np
import pandas as pd
import yfinance as yf
import marleg_volume_scan as mvs

HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
OUT = os.path.join(HERE, "marleg_gated_cache.json")
U = mvs.load_universe()                             # full Groww NSE-equity universe (~3000+)


def main():
    out_path = os.path.join(HERE, sys.argv[sys.argv.index("--out") + 1]) if "--out" in sys.argv else OUT
    print(f"downloading {len(U)} symbols (1y)...")
    data = yf.download([s + ".NS" for s in U], period="1y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume, high, low = {}, {}, {}, {}
    for s in U:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 130:
                close[s] = c; volume[s] = data[t]["Volume"]; high[s] = data[t]["High"]; low[s] = data[t]["Low"]
        except Exception:
            pass
    close = pd.DataFrame(close)
    volume = pd.DataFrame(volume).reindex(close.index)
    print(f"universe with data: {close.shape[1]} stocks")
    if close.shape[1] < 200:        # yfinance throttled this run — do NOT clobber good caches with thin data
        print(f"[throttled] only {close.shape[1]} names with data. Keeping prior caches, exiting without overwrite.")
        return

    # GATE 1 — granular INDUSTRY relative strength (replaces the old coarse sector gate).
    import marleg_industry_rs as mir
    ind_rank, eff_group, ind_table, ind_grow = mir.leadership(close, volume)
    # keep the coarse sector rank too — surfaced as a secondary "sector also leading" flag
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    smembers = {}
    for s, sec in secmap.items():
        smembers.setdefault(sec, []).append(s)
    ret20 = close.pct_change(20).iloc[-1]
    sec_ret = pd.Series({sec: ret20[m].mean() for sec, m in smembers.items()})
    sec_rank = sec_ret.rank(ascending=False, pct=True)

    d = np.sign(close.diff())
    upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    ud_now, ud_ma, ud_10 = ud.iloc[-1], ud.rolling(50).mean().iloc[-1], ud.iloc[-11]
    hh = close.rolling(120).max().iloc[-1]; ll = close.rolling(120).min().iloc[-1]
    fibpos = (close.iloc[-1] - ll) / (hh - ll).replace(0, np.nan)

    picks = []
    for s in close.columns:
        sec = secmap[s]
        grp = eff_group.get(s, sec)
        if not (ind_rank.get(grp, 1) <= mir.LEAD_PCT):          # GATE 1: industry/group leading
            continue
        if not (ud_now[s] > ud_ma[s] and ud_now[s] > ud_10[s]):
            continue
        if not (fibpos[s] > 0.618):
            continue
        price = float(close[s].iloc[-1])
        h, l, c = high[s], low[s], close[s]
        pc = c.shift(1)
        tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])
        gr = ind_grow.get(grp, {})
        picks.append({"s": s, "n": NAMES.get(s, s), "sector": sec,
                      "industry": grp, "ind_kind": gr.get("kind"),
                      "ind_rank": round(float(ind_rank.get(grp, 1)) * 100),
                      "ind_ret20": gr.get("ret20"), "ind_breadth": gr.get("breadth"),
                      "sector_leading": bool(sec_rank.get(sec, 1) <= 0.40),
                      "ud": round(float(ud_now[s]), 2), "ud_ma": round(float(ud_ma[s]), 2),
                      "fib": round(float(fibpos[s]), 2), "sec_rank": round(float(sec_rank.get(sec, 1)) * 100),
                      "price": round(price, 2), "target": round(price + 2 * atr, 1),
                      "tgtpct": round(2 * atr / price * 100, 1), "stop": round(price - atr, 1)})
    # leaders in a leading group first (lowest industry rank), then strongest volume
    picks.sort(key=lambda x: (x["ind_rank"], -x["ud"]))
    # WEAKEST CONFLUENCE (inverse gate): lagging sector + U/D < MA & falling + price < 0.382 fib.
    # Backtested (marleg_short_gate_study) as NO short edge — these mean-revert UP, shorting them
    # loses ~0.8-1.1% net of cost. Surfaced as AVOID-on-longs / hedge reference, NOT a short signal.
    shorts = []
    for s in close.columns:
        sec = secmap[s]
        grp = eff_group.get(s, sec)
        if not (ind_rank.get(grp, 1) >= 1 - mir.LEAD_PCT):      # lagging group (bottom 40%)
            continue
        if not (ud_now[s] < ud_ma[s] and ud_now[s] < ud_10[s]):
            continue
        if not (fibpos[s] < 0.382):
            continue
        shorts.append({"s": s, "n": NAMES.get(s, s), "sector": sec,
                       "industry": grp, "ind_rank": round(float(ind_rank.get(grp, 1)) * 100),
                       "ud": round(float(ud_now[s]), 2), "ud_ma": round(float(ud_ma[s]), 2),
                       "fib": round(float(fibpos[s]), 2),
                       "sec_rank": round(float(sec_rank.get(sec, 1)) * 100),
                       "price": round(float(close[s].iloc[-1]), 2)})
    shorts.sort(key=lambda x: x["ud"])        # weakest (lowest U/D) first
    from datetime import datetime, timezone, timedelta
    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")  # true IST (machine is US-CT)
    # persist the full industry-RS table so /api/industry_rs (rotation heatmap) needs no re-download
    mir.save_cache(ind_table, ist, universe=close.shape[1])
    leading = [g for g in ind_table if g.get("leading")][:14]
    json.dump({"asof": ist, "n": len(picks), "n_weak": len(shorts), "universe": close.shape[1],
               "leading_industries": leading, "picks": picks, "shorts": shorts},
              open(out_path, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"gated longs: {len(picks)} / {close.shape[1]}  ->  {out_path}")
    print(f"weakest/avoid: {len(shorts)} names | leading groups: {len(leading)}")
    for p in picks[:12]:
        print(f"  {p['s']:<12} {str(p['industry'])[:24]:<24} ind#{p['ind_rank']:<3} U/D {p['ud']}>{p['ud_ma']}  fib {p['fib']}  -> +{p['tgtpct']}%")


if __name__ == "__main__":
    main()
