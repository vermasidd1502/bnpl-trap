"""
Marle-G — VOLUME SUGGESTION LEDGER. The pod remembers its own volume picks.

What it does:
  1. RECORDS the daily volume suggestion list automatically (GateBot calls it every
     session; the endpoint also records on first hit of the day). Stored in the same
     DuckDB as prices -> synchronized, auditable memory.
  2. STREAKS — how many consecutive sessions a name has been on the list ("day 3 on
     the chart"), shown live on the day-book page.
  3. THE STUDY — a decade BACKFILL replays the same rule over history and measures
     forward returns BY STREAK AGE (is the upside on day 1 or day 5?) and what happens
     after a name DROPS OFF the list (the downside catch).

The ledger rule (identical for backfill and live — otherwise the stats don't transfer):
  ud(20d) > 1.3  AND  px > 50dma  AND  px > 200dma  ->  top 8 by ud.

  python marleg_volume_ledger.py --backfill     # one-off: replay the decade
  python marleg_volume_ledger.py                # record today + show streaks + study
"""
import os, sys, math
import numpy as np
import pandas as pd
import duckdb
import marleg_datastore as ds

UD_MIN, TOP_N = 1.3, 8
FWD = (1, 5, 10, 21)
BUCKETS = [(1, 1, "day 1 (new)"), (2, 3, "days 2-3"), (4, 7, "days 4-7"),
           (8, 15, "days 8-15"), (16, 999, "day 16+")]


def _con():
    con = duckdb.connect(ds.DB)
    con.execute("""CREATE TABLE IF NOT EXISTS vol_suggestions(
        date DATE, symbol VARCHAR, ud DOUBLE, rank INTEGER, streak INTEGER,
        source VARCHAR, PRIMARY KEY(date, symbol))""")
    return con


def _signals():
    C = ds.panel("close").ffill()
    keep = [c for c in C.columns if C[c].dropna().shape[0] > 400]
    C = C[keep]
    V = ds.panel("volume")[keep].reindex(C.index)
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    s50 = C.rolling(50).mean()
    s200 = C.rolling(200).mean()
    return C, ud, s50, s200


def _list_for_day(C, ud, s50, s200, i):
    row = ud.iloc[i]
    ok = row[(row > UD_MIN) & (C.iloc[i] > s50.iloc[i]) & (C.iloc[i] > s200.iloc[i])].dropna()
    return list(ok.sort_values(ascending=False).head(TOP_N).items())


def backfill():
    """Replay the rule over the whole store; rebuild the ledger with true streaks."""
    C, ud, s50, s200 = _signals()
    con = _con()
    con.execute("DELETE FROM vol_suggestions WHERE source='backfill'")
    streak = {}
    rows = []
    for i in range(210, len(C)):
        d = C.index[i].date()
        lst = _list_for_day(C, ud, s50, s200, i)
        today = {s for s, _ in lst}
        streak = {s: (streak.get(s, 0) + 1) for s in today}
        for rank, (s, u) in enumerate(lst, 1):
            rows.append((d, s, float(u), rank, streak[s], "backfill"))
    if rows:
        con.executemany("INSERT OR REPLACE INTO vol_suggestions VALUES (?,?,?,?,?,?)", rows)
    n = con.execute("SELECT count(*) FROM vol_suggestions").fetchone()[0]
    con.close()
    print(f"[ledger] backfilled {len(rows)} suggestion-rows · table now {n}")
    return len(rows)


def record_today():
    """Record the latest session's list (idempotent). Called by GateBot every cycle."""
    ds.sync(verbose=False)
    C, ud, s50, s200 = _signals()
    i = len(C.index) - 1
    d = C.index[i].date()
    lst = _list_for_day(C, ud, s50, s200, i)
    con = _con()
    prev = dict(con.execute(
        "SELECT symbol, streak FROM vol_suggestions WHERE date = "
        "(SELECT max(date) FROM vol_suggestions WHERE date < ?)", [d]).fetchall())
    rows = [(d, s, float(u), rank, prev.get(s, 0) + 1, "live")
            for rank, (s, u) in enumerate(lst, 1)]
    if rows:
        con.executemany("INSERT OR REPLACE INTO vol_suggestions VALUES (?,?,?,?,?,?)", rows)
    con.close()
    return rows


def _live_px(syms):
    """Live-ish last price per symbol (5m bars, ~15min delayed). {} on failure."""
    out = {}
    if not syms:
        return out
    try:
        import yfinance as yf
        d = yf.download([s + ".NS" for s in syms], period="1d", interval="5m",
                        group_by="ticker", progress=False, threads=True)
        for s in syms:
            try:
                c = (d[s + ".NS"]["Close"] if len(syms) > 1 else d["Close"]).dropna()
                if len(c):
                    out[s] = float(c.iloc[-1])
            except Exception:
                pass
    except Exception:
        pass
    return out


def current():
    """Today's list with streaks + THE THREE PRICES per name:
    trigger (close on the streak's first day), live now, target (trigger + 2*ATR)."""
    con = _con()
    last = con.execute("SELECT max(date) FROM vol_suggestions").fetchone()[0]
    if last is None:
        con.close()
        return {"error": "ledger empty — run --backfill"}
    cur = con.execute("SELECT symbol, ud, rank, streak FROM vol_suggestions "
                      "WHERE date=? ORDER BY rank", [last]).fetchall()
    firsts = {}
    for s, _u, _r, k in cur:
        rowz = con.execute("SELECT date FROM vol_suggestions WHERE symbol=? AND date<=? "
                           "ORDER BY date DESC LIMIT ?", [s, last, int(k)]).fetchall()
        if rowz:
            firsts[s] = rowz[-1][0]
    prevd = con.execute("SELECT max(date) FROM vol_suggestions WHERE date < ?", [last]).fetchone()[0]
    dropped = []
    if prevd:
        dropped = [{"sym": s, "had_streak": k} for s, k in con.execute(
            "SELECT symbol, streak FROM vol_suggestions WHERE date=? AND symbol NOT IN "
            "(SELECT symbol FROM vol_suggestions WHERE date=?) ORDER BY streak DESC",
            [prevd, last]).fetchall()]
    con.close()
    syms = [s for s, *_ in cur]
    live = _live_px(syms)
    out_list = []
    for s, u, r, k in cur:
        trig_d = firsts.get(s)
        trig = tgt = None
        try:
            d = ds.series(s).dropna()
            h = ds.series(s, "high").dropna()
            l = ds.series(s, "low").dropna()
            pc = d.shift(1)
            tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
            atr = tr.rolling(14).mean()
            ts = pd.Timestamp(trig_d)
            if ts in d.index:
                trig = float(d.loc[ts])
                a = float(atr.loc[ts]) if ts in atr.index and np.isfinite(atr.loc[ts]) else None
                if a:
                    tgt = round(trig + 2 * a, 1)          # the pod's 2:1 convention
        except Exception:
            pass
        now = live.get(s) or (float(ds.series(s).dropna().iloc[-1]) if trig else None)
        prog = None
        if trig and tgt and now and tgt > trig:
            prog = round(100 * (now - trig) / (tgt - trig), 0)
        zone = ("go-to" if 2.0 <= u < 3.6 else "ok" if u < 2.0 else
                "hot" if u < 6.0 else "extreme")
        out_list.append({"sym": s, "ud": round(u, 2), "rank": r, "days_on_list": k,
                         "zone": zone,
                         "badge": "NEW" if k == 1 else f"{k}d",
                         "trigger_date": str(trig_d) if trig_d else None,
                         "trigger": round(trig, 2) if trig else None,
                         "now": round(now, 2) if now else None,
                         "chg_pct": round((now / trig - 1) * 100, 2) if (now and trig) else None,
                         "target": tgt, "progress_pct": prog})
    return {"asof": str(last), "list": out_list, "dropped_since_prev": dropped}


def study():
    """Forward returns by streak age + the drop-off downside. Backfill rows only."""
    con = _con()
    df = con.execute("SELECT date, symbol, streak FROM vol_suggestions "
                     "WHERE source='backfill' ORDER BY date").df()
    con.close()
    if df.empty:
        return {"error": "run --backfill first"}
    df["date"] = pd.to_datetime(df["date"]).dt.date
    C = ds.panel("close").ffill()
    pos = {d: i for i, d in enumerate(C.index.date)}
    px = C.values
    cols = {s: j for j, s in enumerate(C.columns)}
    n = len(C.index)

    def fwd(i, j, k):
        if i + k >= n:
            return np.nan
        a, b = px[i, j], px[i + k, j]
        return (b / a - 1) * 100 if (np.isfinite(a) and np.isfinite(b) and a > 0) else np.nan

    recs = []
    for d, s, k in df.itertuples(index=False):
        i, j = pos.get(d), cols.get(s)
        if i is None or j is None:
            continue
        recs.append((k, [fwd(i, j, h) for h in FWD]))
    by = []
    for lo, hi, label in BUCKETS:
        sel = [f for k, f in recs if lo <= k <= hi]
        if len(sel) < 30:
            continue
        arr = np.array(sel, float)
        med = np.nanmedian(arr, axis=0)
        win10 = float(np.nanmean(arr[:, 2] > 0) * 100)
        by.append({"bucket": label, "n": len(sel),
                   **{f"fwd{h}_med_pct": round(float(m), 2) for h, m in zip(FWD, med)},
                   "win10_pct": round(win10, 1)})
    # drop-off events: streak >= 4 on day d, absent on the next recorded day
    dates_sorted = sorted(df["date"].unique())
    nxt = {a: b for a, b in zip(dates_sorted, dates_sorted[1:])}
    present_set = set(zip(df["date"], df["symbol"]))
    drops = []
    for d, s, k in df[df["streak"] >= 4][["date", "symbol", "streak"]].itertuples(index=False):
        nd = nxt.get(d)
        if nd is None:
            continue
        if (nd, s) not in present_set:
            i, j = pos.get(nd), cols.get(s)
            if i is not None and j is not None:
                drops.append([fwd(i, j, h) for h in FWD])
    drop_stats = None
    if len(drops) >= 30:
        arr = np.array(drops, float)
        med = np.nanmedian(arr, axis=0)
        drop_stats = {"n": len(drops),
                      **{f"fwd{h}_med_pct": round(float(m), 2) for h, m in zip(FWD, med)},
                      "win10_pct": round(float(np.nanmean(arr[:, 2] > 0) * 100), 1)}
    # auto-verdict
    verdict = ""
    if by:
        best = max(by, key=lambda x: x["fwd10_med_pct"])
        worst = min(by, key=lambda x: x["fwd10_med_pct"])
        verdict = (f"10d edge peaks at {best['bucket']} (median {best['fwd10_med_pct']:+}%) "
                   f"and is weakest at {worst['bucket']} ({worst['fwd10_med_pct']:+}%).")
        if drop_stats:
            verdict += (f" After a 4d+ name DROPS off the list: 10d median "
                        f"{drop_stats['fwd10_med_pct']:+}% (n={drop_stats['n']}) — "
                        + ("downside follows; treat drop-off as an exit signal."
                           if drop_stats["fwd10_med_pct"] < 0 else "no clean downside; drop-off is neutral."))
    return {"by_streak": by, "dropoff": drop_stats, "verdict": verdict,
            "rule": f"ud>{UD_MIN} & >50dma & >200dma, top {TOP_N} by ud",
            "note": "Backfilled decade replay; medians, net of nothing (raw forward returns)."}


def summary():
    return {"current": current(), "study": study()}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if "--backfill" in sys.argv:
        backfill()
        return
    record_today()
    cur = current()
    print(f"\nVOLUME LEDGER — {cur['asof']}")
    for r in cur["list"]:
        print(f"  {r['badge']:>5}  {r['sym']:<14} ud {r['ud']:>6} rank {r['rank']}")
    if cur["dropped_since_prev"]:
        print("  dropped:", ", ".join(f"{d['sym']}({d['had_streak']}d)" for d in cur["dropped_since_prev"]))
    st = study()
    if "by_streak" in st:
        print(f"\nSTUDY — forward returns by streak age ({st['rule']})")
        print(f"{'bucket':<14}{'n':>7}{'fwd1':>7}{'fwd5':>7}{'fwd10':>7}{'fwd21':>7}{'win10':>7}")
        for b in st["by_streak"]:
            print(f"{b['bucket']:<14}{b['n']:>7}{b['fwd1_med_pct']:>7}{b['fwd5_med_pct']:>7}"
                  f"{b['fwd10_med_pct']:>7}{b['fwd21_med_pct']:>7}{b['win10_pct']:>6}%")
        if st["dropoff"]:
            d = st["dropoff"]
            print(f"{'DROP-OFF(4d+)':<14}{d['n']:>7}{d['fwd1_med_pct']:>7}{d['fwd5_med_pct']:>7}"
                  f"{d['fwd10_med_pct']:>7}{d['fwd21_med_pct']:>7}{d['win10_pct']:>6}%")
        print("\nVERDICT:", st["verdict"])


if __name__ == "__main__":
    main()
