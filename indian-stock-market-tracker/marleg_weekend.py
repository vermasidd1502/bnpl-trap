"""
marleg_weekend.py — is holding over the weekend an EDGE or just gap risk?

Tests, per name + Nifty, over multi-year daily history:
  - day-of-week average return (the classic Monday/weekend effect)
  - WEEKEND HOLD: buy Friday close -> sell next session (Mon) close, unconditional
  - CONDITIONAL on Friday MOMENTUM (up day, closed in the top 40% of its range, above the
    20-DMA, volume > average) — i.e. does a strong Friday carry into Monday?
  - GAP RISK: the Monday-open gap distribution (how often it gaps down hard over the weekend)

  python marleg_weekend.py
"""
import sys, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, yfinance as yf
import groww_client as gc
import marleg_winners as mw     # live book for the Monday board + Friday candidate seeding (was only imported locally -> NameError -> empty board)


def _load(tk, years=4):
    sym = tk if tk.startswith("^") else tk + ".NS"
    df = yf.download(sym, period=f"{years}y", interval="1d", progress=False, auto_adjust=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df.dropna()


def analyze(tk, years=4, cost=0.10):
    df = _load(tk, years)
    if len(df) < 250:
        return {"tk": tk, "error": "thin history"}
    c, o, h, l, v = df["Close"], df["Open"], df["High"], df["Low"], df["Volume"]
    ret = c.pct_change() * 100
    wd = pd.Series(df.index.dayofweek, index=df.index)             # 0=Mon … 4=Fri
    by_wd = {int(d): round(float(ret[wd == d].mean()), 3) for d in range(5)}
    fwd_ret = (c.shift(-1) / c - 1) * 100                          # next-session close return
    gap = (o.shift(-1) / c - 1) * 100                             # next-session open gap
    ma20 = c.rolling(20).mean()
    rng = (h - l).replace(0, np.nan)
    closepos = (c - l) / rng                                       # where the day closed in its range
    rvol = v / v.rolling(20).mean()
    fri = wd == 4
    strong = fri & (ret > 0) & (closepos > 0.6) & (c > ma20) & (rvol > 1.1)

    def stats(mask):
        r = fwd_ret[mask].dropna(); g = gap[mask].dropna()
        if len(r) < 5:
            return None
        return {"n": int(len(r)), "avg": round(float(r.mean()), 3),
                "net": round(float(r.mean()) - cost, 3), "win": round(float((r > 0).mean()) * 100),
                "gap_avg": round(float(g.mean()), 3), "gapdn2_pct": round(float((g < -2).mean()) * 100),
                "worst": round(float(r.min()), 1)}
    return {"tk": tk, "weekday_ret": by_wd, "all_fri": stats(fri), "strong_fri": stats(strong),
            "n_strong": int(strong.sum())}


_HIST = {}
def _hist(tk):
    if tk not in _HIST:
        _HIST[tk] = analyze(tk)
    return _HIST[tk]


def carry_scan(names=None):
    """Live weekend-carry read: historical strong-Friday edge ✕ today's momentum confirmation
    ✕ don't-chase. Watch it firm up through the session; decide at the close."""
    import json, os
    import marleg_intraday as mi, marleg_overextension as oe, marleg_winners as mw, groww_client as gc
    HERE = os.path.dirname(os.path.abspath(__file__))
    g = gc.GrowwClient(); g.token()
    if names is None:
        names = set()
        try:
            for r in mw.board(g, watch=False).get("book", []):
                names.add(r["sym"])
        except Exception:
            pass
        try:
            gd = json.load(open(os.path.join(HERE, "marleg_gated_cache.json"), encoding="utf-8"))
            for p in gd.get("picks", [])[:8]:
                names.add(p["s"])
        except Exception:
            pass
        names = sorted(names)
    rows = []
    for tk in names:
        h = _hist(tk); s = h.get("strong_fri")
        has_edge = bool(s and s["net"] > 0 and s["win"] >= 55)
        low_gap = bool(s and s["gapdn2_pct"] <= 10)
        pos = mi.position(tk, 15); ch = oe.chase_check(tk)
        live_ok = pos and not pos.get("error")
        up = bool(live_ok and pos["chg_day_pct"] > 0)
        upper = bool(live_ok and pos.get("pos_in_range", 0) >= 55)
        above_vwap = bool(live_ok and (pos.get("vwap_dist") or -9) >= 0)
        extended = bool(ch and not ch.get("error") and ch.get("at_ceiling"))
        confirm = sum([up, upper, above_vwap, not extended])
        if has_edge and confirm >= 3 and not extended:
            verdict = "✅ CARRY" + ("" if low_gap else " (small — gappy)")
        elif extended:
            verdict = "🔴 FLAT — extended"
        elif confirm >= 2:
            verdict = "🟡 WATCH — forming"
        else:
            verdict = "⚪ FLAT — no setup"
        rows.append({"tk": tk, "name": (pos or {}).get("name", tk),
                     "edge_net": (s["net"] if s else None), "edge_win": (s["win"] if s else None),
                     "gapdn2": (s["gapdn2_pct"] if s else None), "n_strong": h.get("n_strong"),
                     "chg_day": (pos.get("chg_day_pct") if live_ok else None),
                     "pos_in_range": (pos.get("pos_in_range") if live_ok else None),
                     "vwap_dist": (pos.get("vwap_dist") if live_ok else None),
                     "structural": (ch.get("structural") if ch and not ch.get("error") else None),
                     "confirm": confirm, "verdict": verdict})
    order = {"✅": 0, "🟡": 1, "⚪": 2, "🔴": 3}
    rows.sort(key=lambda r: order.get(r["verdict"][0], 9))
    return {"asof_note": "watch through the close (15:30 IST); decide near 15:25", "rows": rows}


def accumulation(tk):
    """Is the stock being ACCUMULATED or DISTRIBUTED? Chaikin A/D + OBV trend + O'Neil
    distribution/accumulation-day counts + up/down volume + close-location."""
    tk = tk.upper()
    try:
        d = yf.download(tk + ".NS", period="4mo", interval="1d", progress=False, auto_adjust=False)
        if isinstance(d.columns, pd.MultiIndex):
            d.columns = d.columns.get_level_values(0)
        c, h, l, v = d["Close"].dropna(), d["High"], d["Low"], d["Volume"]
    except Exception:
        return {"error": "no data", "score": 50, "label": "?"}
    if len(c) < 30:
        return {"error": "thin", "score": 50, "label": "?"}
    rng = (h - l).replace(0, np.nan)
    clv = ((c - l) - (h - c)) / rng                         # Chaikin money-flow multiplier
    adl = (clv * v).cumsum()
    obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
    def trend(s, n=20):
        return 1 if (len(s) > n and s.iloc[-1] > s.iloc[-n]) else -1
    ad_tr, obv_tr = trend(adl), trend(obv)
    chg = c.pct_change() * 100; volup = v > v.shift(1)
    dist = int(((chg <= -0.2) & volup).tail(25).sum())       # O'Neil distribution days
    acc = int(((chg >= 0.2) & volup).tail(25).sum())         # accumulation days
    upv = float(v.where(chg > 0, 0).tail(20).sum()); dnv = float(v.where(chg < 0, 0).tail(20).sum())
    ud20 = round(upv / dnv, 2) if dnv else None
    clv_recent = round(float(clv.tail(10).mean()), 2)
    score = 50 + (acc - dist) * 4 + clv_recent * 18 + (8 if ad_tr > 0 else -8) + (8 if obv_tr > 0 else -8)
    score = int(max(0, min(100, round(score))))
    label = "Under accumulation" if score >= 62 else "Under distribution" if score <= 38 else "Neutral / mixed"
    return {"score": score, "label": label, "ad_trend": "rising" if ad_tr > 0 else "falling",
            "obv_trend": "rising" if obv_tr > 0 else "falling", "acc_days": acc, "dist_days": dist,
            "ud20": ud20, "close_location": clv_recent}


def _candidates(g, n=14):
    import json, os
    HERE = os.path.dirname(os.path.abspath(__file__))
    syms = []
    try:
        d = json.load(open(os.path.join(HERE, "marleg_volume_cache.json"), encoding="utf-8"))
        sc = []
        for sec in d.get("sectors", []):
            for x in sec.get("stocks", []):
                sc.append((x.get("ud") or 0, x["s"]))
        sc.sort(key=lambda z: -z[0]); syms = [s for _, s in sc[:n]]
    except Exception:
        pass
    s = set(syms)
    try:
        for r in mw.board(g, watch=False).get("book", []):
            s.add(r["sym"])
    except Exception:
        pass
    return sorted(s)[:n + 5]


def weekend_board(mode="friday"):
    import marleg_overextension as oe, marleg_intraday as mi
    g = gc.GrowwClient(); g.token()
    if mode == "monday":
        return _monday_board(g)
    rows = []
    for tk in _candidates(g):
        try:
            acc = accumulation(tk); ch = oe.chase_check(tk); h = _hist(tk); s = h.get("strong_fri")
            pos = mi.position(tk, 15); live = pos and not pos.get("error")
            edge_ok = bool(s and s["net"] > 0 and s["win"] >= 55)
            low_gap = bool(s and s["gapdn2_pct"] <= 10)
            extended = bool(ch and not ch.get("error") and ch.get("at_ceiling"))
            fri_strong = bool(live and pos["chg_day_pct"] > 0 and pos.get("pos_in_range", 0) >= 55 and (pos.get("vwap_dist") or -9) >= 0)
            fav = 50 + (acc["score"] - 50) * 0.6
            fav += 14 if edge_ok else -4
            fav += -18 if extended else 10
            fav += 8 if fri_strong else -4
            fav += 5 if low_gap else -3
            fav += -12 if acc["label"] == "Under distribution" else (8 if acc["label"] == "Under accumulation" else 0)
            fav = int(max(0, min(100, round(fav))))
            if extended or acc["label"] == "Under distribution":
                verdict = "🔴 AVOID"
            elif fav >= 65:
                verdict = "✅ FAVORABLE" + ("" if low_gap else " (small — gappy)")
            elif fav >= 50:
                verdict = "🟡 WATCH"
            else:
                verdict = "⚪ PASS"
            rows.append({"tk": tk, "name": (pos or {}).get("name", tk), "fav": fav, "verdict": verdict,
                         "accum": acc, "structural": (ch.get("structural") if ch and not ch.get("error") else None),
                         "edge_net": (s["net"] if s else None), "edge_win": (s["win"] if s else None),
                         "gapdn2": (s["gapdn2_pct"] if s else None),
                         "chg_day": (pos.get("chg_day_pct") if live else None),
                         "pos_in_range": (pos.get("pos_in_range") if live else None)})
        except Exception:
            continue
    rows.sort(key=lambda r: -r["fav"])
    return {"mode": "friday", "asof_note": "Accumulation ✕ Friday strength ✕ room-to-run ✕ weekend edge. Decide near the 15:15 close.",
            "rows": rows}


def _monday_board(g):
    """Manage what you carried: each held name's historical Monday gap/return + a hold/exit lean."""
    rows = []
    try:
        book = mw.board(g, watch=False).get("book", [])
    except Exception:
        book = []
    for r in book:
        tk = r["sym"]; h = _hist(tk); s = h.get("strong_fri"); a = h.get("all_fri")
        acc = accumulation(tk)
        lean = ("hold — accumulation + edge intact" if (acc.get("label") == "Under accumulation" and s and s["net"] > 0)
                else "exit/trim — distribution" if acc.get("label") == "Under distribution"
                else "manage on the open")
        rows.append({"tk": tk, "name": r.get("name", tk), "qty": r.get("net_qty"), "avg": r.get("avg"),
                     "unreal": r.get("unreal"), "accum_label": acc.get("label"), "accum_score": acc.get("score"),
                     "mon_ret": (s["avg"] if s else (a["avg"] if a else None)),
                     "mon_win": (s["win"] if s else (a["win"] if a else None)),
                     "gapdn2": (s["gapdn2_pct"] if s else None), "lean": lean})
    return {"mode": "monday", "asof_note": "Monday playbook for your carried book — gap odds + accumulation + a hold/exit lean.",
            "rows": rows}


def friday_volume(tk):
    """Deep Friday volume-accumulation study: is Friday volume ACCUMULATION (on up-bars,
    closes high) or DISTRIBUTION (on down-bars, fades into the close on heavy volume)?
    Compares Fridays vs other days on intraday up/down volume, close-location, and the
    late-session (last hour) direction — the weekend de-risk window."""
    import marleg_intraday as mi
    tk = tk.upper()
    df = mi.gbars(tk, 15, days=60)
    if df is None or df.empty:
        return {"tk": tk, "error": "no intraday data"}
    df = df.copy(); df["d"] = df.index.normalize(); df["t"] = df.index.strftime("%H:%M"); df["wd"] = df.index.dayofweek
    fri, oth = [], []
    for _d, g in df.groupby("d"):
        if len(g) < 8 or g["t"].iloc[-1] < "15:00":      # skip thin / incomplete (incl. today's live) sessions
            continue
        c, o, h, l, v = g["close"], g["open"], g["high"], g["low"], g["volume"]
        up = c > o
        upv, dnv = float(v[up].sum()), float(v[~up].sum())
        clv = (float(c.iloc[-1]) - float(l.min())) / ((float(h.max()) - float(l.min())) or np.nan)
        morn = float(v[g["t"] < "12:00"].sum()); aft = float(v[g["t"] >= "12:00"].sum())
        late = g[g["t"] >= "14:30"]
        lup = float(late["volume"][late["close"] > late["open"]].sum())
        ldn = float(late["volume"][late["close"] <= late["open"]].sum())
        rec = {"udv": (upv / dnv if dnv else None), "clv": clv,
               "aft_share": (aft / (morn + aft) if (morn + aft) else None),
               "late_sell": bool(ldn > lup), "vol": float(v.sum())}
        (fri if int(g["wd"].iloc[0]) == 4 else oth).append(rec)

    def agg(rows):
        if not rows:
            return {}
        f = lambda k: [r[k] for r in rows if r.get(k) is not None]
        return {"n": len(rows), "udv": round(float(np.nanmean(f("udv"))), 2),
                "clv": round(float(np.nanmean(f("clv"))), 2),
                "aft_share": round(float(np.nanmean(f("aft_share"))) * 100),
                "late_sell_pct": round(float(np.mean([1 if r["late_sell"] else 0 for r in rows])) * 100),
                "avg_vol": float(np.mean(f("vol")))}
    F, O = agg(fri), agg(oth)
    ad = accumulation(tk)                                        # daily A/D context
    vol_vs = round(F.get("avg_vol", 0) / O["avg_vol"], 2) if O.get("avg_vol") else None
    # verdict — distinguish HEAVY distribution (sold on rising vol) from light DRIFT (buyers step aside)
    udv = F.get("udv", 1) or 1; clv = F.get("clv", 0.5) or 0.5; late = F.get("late_sell_pct", 0) or 0
    weak = clv < 0.42 or udv < 0.95
    heavy = (vol_vs or 1) >= 1.3
    if weak and heavy:
        tag = "distribution"
        verdict = ("🔴 HEAVY Friday DISTRIBUTION — being sold into the weekend on rising volume "
                   "(weak/low closes, late-session selling). Avoid carrying, especially leveraged.")
    elif udv > 1.05 and clv > 0.55:
        tag = "accumulation"
        verdict = ("🟢 Friday ACCUMULATION — up-bar volume dominates and it closes strong. "
                   "Friday strength is being bought, not faded — a better weekend-carry profile.")
    elif weak or late >= 60:
        tag = "drift"
        verdict = ("🟠 Friday DRIFT — fades to a weak close on LIGHT volume (buyers step aside, not heavy "
                   "selling). Owned delivery is fine; just don't add or leverage into the close.")
    else:
        tag = "mixed"
        verdict = "🟡 Friday MIXED — no consistent accumulation/distribution edge into the weekend."
    return {"tk": tk, "friday": F, "other": O, "tag": tag,
            "daily_ad": {"label": ad.get("label"), "score": ad.get("score"),
                         "dist_days": ad.get("dist_days"), "acc_days": ad.get("acc_days")},
            "friday_vol_vs_other": vol_vs, "verdict": verdict}


def weekend_stock(tk):
    """Full weekend report for ONE stock: accumulation ✕ Friday-volume ✕ don't-chase ✕ edge."""
    import marleg_overextension as oe, marleg_intraday as mi
    tk = tk.upper()
    acc = accumulation(tk); fv = friday_volume(tk); ch = oe.chase_check(tk)
    h = _hist(tk); s = h.get("strong_fri"); allf = h.get("all_fri")
    pos = mi.position(tk, 15); live = pos and not pos.get("error")
    edge_ok = bool(s and s["net"] > 0 and s["win"] >= 55)
    low_gap = bool(s and s["gapdn2_pct"] <= 10)
    extended = bool(ch and not ch.get("error") and ch.get("at_ceiling"))
    fri_strong = bool(live and pos["chg_day_pct"] > 0 and pos.get("pos_in_range", 0) >= 55 and (pos.get("vwap_dist") or -9) >= 0)
    ftag = fv.get("tag")                                    # accumulation / drift / distribution / mixed
    fri_dist = ftag == "distribution"; fri_drift = ftag == "drift"; fri_acc = ftag == "accumulation"
    fav = 50 + (acc.get("score", 50) - 50) * 0.6
    fav += 14 if edge_ok else -4
    fav += -18 if extended else 10
    fav += 8 if fri_strong else -4
    fav += 5 if low_gap else -3
    fav += -12 if acc.get("label") == "Under distribution" else (8 if acc.get("label") == "Under accumulation" else 0)
    fav += -10 if fri_dist else (-4 if fri_drift else (6 if fri_acc else 0))
    fav = int(max(0, min(100, round(fav))))
    if extended or acc.get("label") == "Under distribution" or fri_dist:
        verdict = "🔴 NOT a clean weekend carry"
    elif fav >= 65 and fri_drift:
        verdict = "🟢 OK as DELIVERY only — drifts Fridays, don't leverage"
    elif fav >= 65:
        verdict = "✅ FAVORABLE weekend long" + ("" if low_gap else " (small — gappy)")
    elif fav >= 50:
        verdict = "🟡 WATCH"
    else:
        verdict = "⚪ PASS"
    return {"tk": tk, "name": (pos or {}).get("name", tk) if live else tk, "fav": fav, "verdict": verdict,
            "accum": acc, "friday_volume": fv,
            "chase": ({"structural": ch.get("structural"), "at_ceiling": ch.get("at_ceiling"),
                       "room_pct": ch.get("room_to_52w_high_pct"), "rsi": ch.get("rsi"),
                       "verdict": ch.get("verdict")} if ch and not ch.get("error") else None),
            "edge": ({"strong_net": (s["net"] if s else None), "strong_win": (s["win"] if s else None),
                      "strong_n": (s["n"] if s else None), "gapdn2": (s["gapdn2_pct"] if s else None),
                      "all_net": (allf["net"] if allf else None)} ),
            "today": ({"chg_day": pos.get("chg_day_pct"), "pos_in_range": pos.get("pos_in_range"),
                       "vwap_dist": pos.get("vwap_dist"), "rsi": pos.get("rsi")} if live else None)}


def friday_logic_backtest(universe=None, years=3, cost=0.30):
    """Cross-sectional validation of the Friday-carry logic. For every (stock, Friday) over
    `years`, compute each signal component AS-OF the Friday close, then the realized forward
    return (Mon, and next-Fri). Reports the marginal edge of each component + the combined
    bucket, so we can see what actually predicts and re-weight accordingly. Daily data only."""
    import json, os
    HERE = os.path.dirname(os.path.abspath(__file__))
    if universe is None:
        u = set()
        try:
            d = json.load(open(os.path.join(HERE, "marleg_volume_cache.json"), encoding="utf-8"))
            for sec in d.get("sectors", []):
                for x in sec.get("stocks", [])[:6]:
                    u.add(x["s"])
        except Exception:
            pass
        try:
            g = json.load(open(os.path.join(HERE, "marleg_gated_cache.json"), encoding="utf-8"))
            for p in g.get("picks", [])[:40]:
                u.add(p["s"])
        except Exception:
            pass
        universe = sorted(u)[:60]
    data = yf.download([s + ".NS" for s in universe], period=f"{years}y", interval="1d",
                       group_by="ticker", progress=False, threads=True, auto_adjust=False)
    obs = []
    for s in universe:
        try:
            df = data[s + ".NS"].dropna()
        except Exception:
            continue
        if len(df) < 260:
            continue
        c, h, l, v = df["Close"], df["High"], df["Low"], df["Volume"]
        ma20 = c.rolling(20).mean(); hi252 = c.rolling(252).max()
        rng = (h - l).replace(0, np.nan); clv = (c - l) / rng
        adl = (((c - l) - (h - c)) / rng * v).cumsum(); obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
        chg = c.pct_change() * 100; volup = v > v.shift(1)
        accday = ((chg >= 0.2) & volup).rolling(25).sum(); distday = ((chg <= -0.2) & volup).rolling(25).sum()
        wd = pd.Series(df.index.dayofweek, index=df.index)
        idx = list(df.index)
        for i in range(252, len(df) - 6):
            if wd.iloc[i] != 4:
                continue
            px = float(c.iloc[i])
            accum = bool(adl.iloc[i] > adl.iloc[i - 20] and obv.iloc[i] > obv.iloc[i - 20] and accday.iloc[i] >= distday.iloc[i])
            fri_strong = bool(chg.iloc[i] > 0 and clv.iloc[i] > 0.6 and px > ma20.iloc[i])
            room = bool((1 - px / hi252.iloc[i]) * 100 >= 12)
            extended = bool((1 - px / hi252.iloc[i]) * 100 <= 3)
            fwd_mon = (float(c.iloc[i + 1]) / px - 1) * 100 - cost
            fwd_wk = (float(c.iloc[i + 5]) / px - 1) * 100 - cost
            obs.append({"accum": accum, "fri_strong": fri_strong, "room": room, "extended": extended,
                        "fav": int(accum) + int(fri_strong) + int(room), "mon": fwd_mon, "wk": fwd_wk})
    if not obs:
        return {"error": "no observations"}
    A = pd.DataFrame(obs)
    def stat(mask, col):
        x = A[mask][col]
        return {"n": int(len(x)), "avg": round(float(x.mean()), 3), "win": round(float((x > 0).mean()) * 100)} if len(x) else None
    out = {"n_obs": len(A), "universe": len(universe), "years": years, "cost_pct": cost, "horizons": {}}
    for col in ("mon", "wk"):
        out["horizons"][col] = {
            "baseline": stat(A.index >= 0, col),
            "accum": {"yes": stat(A["accum"], col), "no": stat(~A["accum"], col)},
            "fri_strong": {"yes": stat(A["fri_strong"], col), "no": stat(~A["fri_strong"], col)},
            "room": {"yes": stat(A["room"], col), "no": stat(~A["room"], col)},
            "extended": {"yes": stat(A["extended"], col), "no": stat(~A["extended"], col)},
            "favorable(all 3)": stat(A["accum"] & A["fri_strong"] & A["room"], col),
            "avoid(extended)": stat(A["extended"], col),
            "by_fav_score": {str(k): stat(A["fav"] == k, col) for k in range(4)},
        }
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    names = sys.argv[1:] or ["^NSEI", "TEJASNET", "QUESS", "APOLLOHOSP", "SYRMA", "MSTCLTD", "EMMVEE", "PARAS", "IGIL", "JBMA"]
    print(f"{'name':<12}{'Mon':>7}{'Fri':>7} | {'allFri Mon-ret/win':>22} | {'STRONG-Fri Mon-ret/win/n':>26} | gap<-2%")
    agg_all, agg_strong = [], []
    for tk in names:
        r = analyze(tk)
        if r.get("error"):
            print(f"{tk:<12} {r['error']}"); continue
        wdr = r["weekday_ret"]; a = r["all_fri"]; s = r["strong_fri"]
        astr = f"{a['net']:+.2f}% / {a['win']}%" if a else "—"
        sstr = f"{s['net']:+.2f}% / {s['win']}% / {s['n']}" if s else "—"
        gap = f"{s['gapdn2_pct']}%" if s else "—"
        print(f"{tk:<12}{wdr.get(0,0):>7}{wdr.get(4,0):>7} | {astr:>22} | {sstr:>26} | {gap}")
        if a: agg_all.append(a["net"])
        if s: agg_strong.append(s["net"])
    if agg_all:
        print(f"\nAVG net Mon-return  ·  all Fridays {np.mean(agg_all):+.2f}%  ·  STRONG Fridays {np.mean(agg_strong):+.2f}%"
              f"  (edge from momentum filter: {np.mean(agg_strong)-np.mean(agg_all):+.2f}%)")


if __name__ == "__main__":
    main()
