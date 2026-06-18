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
import marleg_eventfilter as ef

HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
OUT = os.path.join(HERE, "marleg_gated_cache.json")
U = mvs.load_universe()                             # full Groww NSE-equity universe (~3000+)


def main():
    out_path = os.path.join(HERE, sys.argv[sys.argv.index("--out") + 1]) if "--out" in sys.argv else OUT
    import marleg_data as md
    # liquidity-bias the universe so the Groww fallback (capped) picks liquid names first
    try:
        tax = set(json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json"), encoding="utf-8")).get("by_symbol", {}))
        U2 = [s for s in U if s in tax] + [s for s in U if s not in tax]
    except Exception:
        U2 = U
    print(f"loading {len(U2)} symbols (1y) via marleg_data (groww/yfinance auto-switch)...")
    panel = md.daily_panel(U2, period="1y")
    close, volume, high, low = panel["close"], panel["volume"], panel["high"], panel["low"]
    print(f"universe with data: {close.shape[1]} stocks (source: {panel['source']})")
    if close.shape[1] < 60:         # even the Groww fallback failed — don't clobber good caches
        print(f"[no data] only {close.shape[1]} names. Keeping prior caches, exiting without overwrite.")
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

    def _rsi(c, n=14):
        dd = c.diff(); up = dd.clip(lower=0).rolling(n).mean(); dn = (-dd.clip(upper=0)).rolling(n).mean()
        v = (100 - 100 / (1 + up / dn.replace(0, np.nan))).iloc[-1]
        return float(v) if v == v else 50.0

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
        import marleg_fib_target as ftgt
        ft = ftgt.target(price, float(hh[s]), float(ll[s]), atr)   # fib-aware: target the resistance, extend on the break (backtested)
        gr = ind_grow.get(grp, {})
        fp = ef.footprint(c, volume[s])            # event-footprint: earnings/news spike vs organic accumulation
        coiled = ef.coiled_breakout(high[s], low[s], c)   # ⚡ breakout out of an ATR-contracted base (bear-proof subset)
        ret5 = round(float(c.iloc[-1] / c.iloc[-6] - 1) * 100, 1) if c.dropna().shape[0] > 6 else None
        rsi = round(_rsi(c))
        # entry quality (backtested: gate_pullback — buying the gated leader ON A 5d DIP — is the best entry;
        # RSI>70 hot = bull-only; near-high + ripping = chasing). PULLBACK is the prime setup.
        entry = ("PULLBACK" if (ret5 is not None and ret5 < 0) else
                 "HOT" if rsi > 70 else
                 "EXTENDED" if (fibpos[s] >= 0.98 and (ret5 or 0) > 3) else "OK")
        picks.append({"s": s, "n": NAMES.get(s, s), "sector": sec,
                      "industry": grp, "ind_kind": gr.get("kind"),
                      "ind_rank": round(float(ind_rank.get(grp, 1)) * 100),
                      "ind_ret20": gr.get("ret20"), "ind_breadth": gr.get("breadth"),
                      "sector_leading": bool(sec_rank.get(sec, 1) <= 0.40),
                      "ud": round(float(ud_now[s]), 2), "ud_ma": round(float(ud_ma[s]), 2),
                      "fib": round(float(fibpos[s]), 2), "sec_rank": round(float(sec_rank.get(sec, 1)) * 100),
                      "ret5": ret5, "rsi": rsi, "entry": entry,
                      "clean": fp["clean"], "event_flags": fp["flags"], "coiled": coiled,
                      "price": round(price, 2), "target": ft["first_target"], "tgtpct": ft["tgtpct"],
                      "stop": ft["stop"], "resistance": ft["resistance"], "tgt_status": ft["status"],
                      "ext_1272": ft["ext_1272"], "ext_1618": ft["ext_1618"],
                      "at_resistance": ft["at_resistance"], "tgt_note": ft["note"]})
    # leader-in-leading-group first; within that, CLEAN (organic) before event-driven, then PULLBACK, then volume
    _ep = {"PULLBACK": 0, "OK": 1, "EXTENDED": 2, "HOT": 3}
    picks.sort(key=lambda x: (x["ind_rank"], 0 if x.get("clean") else 1, _ep.get(x["entry"], 9), -x["ud"]))
    # earnings proximity for the top picks (best-effort, cached) — refine the event flags so the strict list drops them
    for p in picks[:25]:
        try:
            e = ef.earnings_proximity(p["s"])
        except Exception:
            e = {}
        p["earnings_in"], p["earnings_ago"] = e.get("in_days"), e.get("ago_days")
        if e.get("ago_days") is not None and e["ago_days"] <= 10:
            p["event_flags"] = p["event_flags"] + [f"reported {e['ago_days']}d ago"]; p["clean"] = False
        if e.get("in_days") is not None and 0 <= e["in_days"] <= 7:
            ru = ef.pre_earnings_runup(p.get("ret5"), e["in_days"])     # hot INTO results = the trap that burned us
            p["event_flags"] = p["event_flags"] + [ru or f"earnings in {e['in_days']}d"]
            p["clean"] = False
            if ru:
                p["pre_earn_runup"] = True
    n_strict = sum(1 for p in picks if p.get("clean"))
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
    if len(picks) == 0 and close.shape[1] >= 200:    # 0 picks on a big universe = stale/pre-market panel (last bar NaN)
        print(f"[no picks] 0 gated of {close.shape[1]} names — keeping prior caches, not overwriting.")
        return
    from datetime import datetime, timezone, timedelta
    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")  # true IST (machine is US-CT)
    # persist the full industry-RS table so /api/industry_rs (rotation heatmap) needs no re-download
    mir.save_cache(ind_table, ist, universe=close.shape[1])
    leading = [g for g in ind_table if g.get("leading")][:14]
    # MACRO REGIME GATE (the durable, backtested edge): NIFTY-proxy vs 50DMA + breadth -> deploy or cash.
    mkt = (1 + close.pct_change().mean(axis=1)).cumprod()
    bull = bool(mkt.iloc[-1] > mkt.rolling(50).mean().iloc[-1])
    breadth = round(float((close.iloc[-1] > close.rolling(50).mean().iloc[-1]).mean()) * 100)
    regime = {"bull": bull, "breadth": breadth,
              "verdict": "DEPLOY — bull regime, gate OPEN" if bull else "CASH / CAUTION — bear regime, gate SHUT",
              "note": ("Backtested: bull-gating the long book (mkt>50DMA) ~halves drawdown + lifts Sharpe above the market. "
                       "In bear, the long edge turns negative — stand aside or run mean-reversion only.")}
    json.dump({"asof": ist, "n": len(picks), "n_strict": n_strict, "n_weak": len(shorts), "universe": close.shape[1],
               "regime": regime, "leading_industries": leading, "picks": picks, "shorts": shorts},
              open(out_path, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"gated longs: {len(picks)} / {close.shape[1]}  ->  {out_path}")
    print(f"weakest/avoid: {len(shorts)} names | leading groups: {len(leading)}")
    for p in picks[:12]:
        print(f"  {p['s']:<12} {str(p['industry'])[:24]:<24} ind#{p['ind_rank']:<3} U/D {p['ud']}>{p['ud_ma']}  fib {p['fib']}  -> +{p['tgtpct']}%")


if __name__ == "__main__":
    main()
