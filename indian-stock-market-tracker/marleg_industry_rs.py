"""
marleg_industry_rs.py — granular INDUSTRY relative-strength, breadth & rotation engine.

For each industry in marleg_industry_taxonomy.json we build an equal-weight basket from its
members and measure:
  • ret20 / ret50   — short- and medium-term momentum (mean member return)
  • breadth         — % of members trading above their own 50-DMA
  • rs              — excess 20d return vs the universe median ("the market")
  • udtilt          — 20d up-volume / down-volume across the basket (accumulation tilt)
A composite leadership score (momentum + breadth) ranks every group leading → lagging.

Thin industries (< MIN_MEMBERS names with data) collapse to their MACRO sector, so a one-stock
"industry" can never become a circular self-gate. This is O'Neil's "L" done at the right
granularity — leader in a leading *industry group*, not just a leading 11-bucket sector.

Used by:
  • marleg_gated_scan.py  — gate #1 (is the stock's industry top-tier RS?) + writes the cache
  • /api/industry_rs      — the rotation heatmap

The engine takes a price panel (so the screener reuses its existing 1y download — no extra
network); run() is a standalone fallback that pulls only the ~1.2k taxonomy names.
"""
import os
import json
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
TAXP = os.path.join(HERE, "marleg_industry_taxonomy.json")
CACHE = os.path.join(HERE, "marleg_industry_rs_cache.json")
MIN_MEMBERS = 3            # below this, an industry falls back to its macro sector
LEAD_PCT = 0.40            # top 40% of groups by composite score = "leading"


def _tax():
    return json.load(open(TAXP, encoding="utf-8"))


def effective_groups(symbols, tax=None):
    """Each symbol -> its effective RS group: granular industry if that industry has
    >= MIN_MEMBERS names in the panel, else its macro sector. Returns (eff, kind, meta)."""
    tax = tax or _tax()
    bysym = tax["by_symbol"]
    ind_count, meta = {}, {}
    for s in symbols:
        m = bysym.get(s) or {}
        ind, macro = m.get("industry"), (m.get("macro") or "Others")
        meta[s] = (ind, macro)
        if ind:
            ind_count[ind] = ind_count.get(ind, 0) + 1
    eff, kind = {}, {}
    for s, (ind, macro) in meta.items():
        if ind and ind_count.get(ind, 0) >= MIN_MEMBERS:
            eff[s] = ind
            kind[ind] = "industry"
        else:
            eff[s] = macro
            kind.setdefault(macro, "sector")
    return eff, kind, meta


def industry_table(close, volume=None):
    """Per-group RS/breadth/momentum table (DataFrame) + {symbol: effective group}."""
    eff, kind, _ = effective_groups(list(close.columns))
    bysym = _tax()["by_symbol"]
    group_macro = {}
    for s, g in eff.items():                       # macro-sector parent (for sector grouping in the UI)
        if g not in group_macro:
            group_macro[g] = (bysym.get(s) or {}).get("macro") or g
    ret20 = close.pct_change(20).iloc[-1]
    ret50 = close.pct_change(50).iloc[-1]
    last = close.iloc[-1]
    above = last > close.rolling(50).mean().iloc[-1]
    mkt20 = float(np.nanmedian(ret20.values))                  # market proxy = universe median 20d
    upv = dnv = None
    if volume is not None:
        d = np.sign(close.diff())
        upv = volume.where(d > 0, 0.0).rolling(20).sum().iloc[-1]
        dnv = volume.where(d < 0, 0.0).rolling(20).sum().iloc[-1]

    groups = {}
    for s, g in eff.items():
        groups.setdefault(g, []).append(s)

    rows = []
    for g, mem in groups.items():
        mm = [s for s in mem if pd.notna(ret20.get(s))]
        if not mm:
            continue
        r20 = float(np.nanmean([ret20[s] for s in mm]))
        r50v = [ret50[s] for s in mm if pd.notna(ret50.get(s))]
        r50 = float(np.nanmean(r50v)) if r50v else None
        brd = float(np.mean([1.0 if bool(above.get(s, False)) else 0.0 for s in mm]))
        tilt = None
        if volume is not None:
            uu = float(np.nansum([upv.get(s, 0.0) for s in mm]))
            dd = float(np.nansum([dnv.get(s, 0.0) for s in mm]))
            tilt = round(uu / dd, 2) if dd else None
        # per-stock member detail for the drill-down (symbol, name, 20d, U/D, above 50DMA)
        md = []
        for s in mm:
            ud_s = None
            if volume is not None:
                dv = float(dnv.get(s, 0.0))
                ud_s = round(float(upv.get(s, 0.0)) / dv, 2) if dv else None
            md.append({"s": s, "n": (bysym.get(s) or {}).get("name") or s,
                       "ret20": round(float(ret20[s]) * 100, 1), "ud": ud_s, "up": bool(above.get(s, False))})
        md.sort(key=lambda x: -(x["ret20"] if x["ret20"] is not None else -999))
        rows.append({"group": g, "macro": group_macro.get(g, g), "kind": kind.get(g, "?"), "n": len(mm),
                     "ret20": round(r20 * 100, 2), "ret50": round(r50 * 100, 2) if r50 is not None else None,
                     "breadth": round(brd * 100), "rs": round((r20 - mkt20) * 100, 2), "udtilt": tilt,
                     "members": md})

    df = pd.DataFrame(rows)
    if df.empty:
        return df, eff
    # composite leadership = short + medium momentum + breadth (all cross-sectional ranks)
    r20r = df["ret20"].rank(pct=True)
    r50r = df["ret50"].rank(pct=True).fillna(r20r)
    brdr = df["breadth"].rank(pct=True)
    df["score"] = (0.40 * r20r + 0.25 * r50r + 0.35 * brdr)
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df["lead_rank"] = np.arange(1, len(df) + 1)
    df["rank_pct"] = df["score"].rank(ascending=False, pct=True)     # 0=leading .. 1=lagging
    df["leading"] = df["rank_pct"] <= LEAD_PCT
    return df, eff


def leadership(close, volume=None):
    """For the screener gate: ({group: rank_pct}, {symbol: group}, table[list], {group: row})."""
    df, eff = industry_table(close, volume)
    if df.empty:
        return {}, eff, [], {}
    rp = dict(zip(df["group"], df["rank_pct"]))
    grow = {r["group"]: r for r in df.to_dict("records")}
    return rp, eff, df.to_dict("records"), grow


def _ist():
    from datetime import datetime, timezone, timedelta
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")


def save_cache(table, asof, universe=0):
    payload = {"asof": asof, "n": len(table), "universe": universe,
               "min_members": MIN_MEMBERS, "lead_pct": LEAD_PCT, "groups": table}
    tmp = CACHE + ".tmp"
    json.dump(payload, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
    os.replace(tmp, CACHE)
    return payload


def load_cache():
    try:
        return json.load(open(CACHE, encoding="utf-8"))
    except Exception:
        return None


def run(period="6mo"):
    """Standalone: pull only the taxonomy universe (~1.2k), build the table, cache it."""
    import yfinance as yf
    syms = list(_tax()["by_symbol"].keys())
    data = yf.download([s + ".NS" for s in syms], period=period, interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume = {}, {}
    for s in syms:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 60:
                close[s] = c
                volume[s] = data[t]["Volume"]
        except Exception:
            pass
    close = pd.DataFrame(close)
    if close.shape[1] < 50:                    # throttled / no data — never clobber a good cache
        existing = load_cache()
        if existing and existing.get("universe", 0) >= close.shape[1]:
            return existing
    volume = pd.DataFrame(volume).reindex(close.index)
    df, _ = industry_table(close, volume)
    return save_cache(df.to_dict("records"), _ist(), universe=close.shape[1])


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    out = run("6mo" if "--full" not in sys.argv else "1y")
    print(f"industry RS — {out['universe']} names -> {out['n']} groups  @ {out['asof']}")
    lead = [g for g in out["groups"] if g["leading"]]
    print(f"\nLEADING groups ({len(lead)}):")
    for g in out["groups"][:18]:
        tag = "★" if g["leading"] else " "
        print(f"  {tag} {g['lead_rank']:>3}. {g['group']:<34} {g['kind']:<9} n={g['n']:<3} "
              f"ret20 {g['ret20']:>6}%  breadth {g['breadth']:>3}%  rs {g['rs']:>6}")
