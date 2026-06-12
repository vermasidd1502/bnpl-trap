"""
Marle-G — GUARDIAN REPLAY: how would the stop-loss script behave in REAL LIFE?

Takes real 5-minute bars (last ~60 sessions) and replays three stop policies on the
same trades, for EVERY possible entry day (buy at that morning's first 5-min close,
hold max 21 sessions):

  NAIVE   -3% fixed stop, tick-evaluated (any 5m low touches -> filled). What retail does.
  CLOSE3  -3% fixed stop, evaluated only on daily closes. The lazy improvement.
  GUARDIAN  the live script's exact rules:
            chandelier = 20d-high - 2.5*ATR(14)  (computed from data through the PREVIOUS
            day - no lookahead; ratchets up, never loosens)
            first-hour guard: 09:15-10:15 IST breach must exceed stop by 0.35*ATR
            persistence: TWO consecutive 5m closes below trigger before exit

Reported per policy: median/mean P&L, % of entries stopped, % NOISE stops (price back
above the exit within 10 sessions — the stop-trap), avg hold. Plus a narrated timeline
of the specific trade that hurt (e.g. TEJASNET bought into the 2026-05-07 pop).

  python marleg_guardian_replay.py TEJASNET ADANIPORTS RELIANCE
"""
import sys, math, json, os
import numpy as np
import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
K_ATR = 2.5
FIRST_EXTRA = 0.35
CONFIRM_N = 2
NAIVE_STOP = 0.03
MAX_HOLD_SESS = 21


def load_bars(sym):
    b5 = yf.download(sym + ".NS", period="60d", interval="5m", progress=False, auto_adjust=False)
    if isinstance(b5.columns, pd.MultiIndex):
        b5.columns = b5.columns.get_level_values(0)
    b5 = b5.dropna()
    b5.index = b5.index.tz_convert("Asia/Kolkata")
    d = yf.download(sym + ".NS", period="1y", interval="1d", progress=False, auto_adjust=False)
    if isinstance(d.columns, pd.MultiIndex):
        d.columns = d.columns.get_level_values(0)
    d = d.dropna()
    pc = d["Close"].shift(1)
    tr = pd.concat([d["High"] - d["Low"], (d["High"] - pc).abs(), (d["Low"] - pc).abs()], axis=1).max(axis=1)
    d["atr"] = tr.rolling(14).mean()
    d["hi20"] = d["High"].rolling(20).max()
    return b5, d


def replay_one(b5, daily, entry_day, sessions):
    """Replay all three policies for one entry day. Returns dict per policy."""
    days = [s for s in sessions if s >= entry_day][:MAX_HOLD_SESS]
    if len(days) < 3:
        return None
    first = b5[b5.index.date == entry_day]
    if len(first) < 5:
        return None
    entry = float(first["Close"].iloc[0])
    naive_stop = entry * (1 - NAIVE_STOP)
    res = {}
    # --- NAIVE: tick-evaluated fixed stop
    exit_px, exit_d, hold = None, None, 0
    for k, day in enumerate(days):
        bars = b5[b5.index.date == day]
        lows = bars["Low"].values
        if (lows <= naive_stop).any():
            exit_px, exit_d, hold = naive_stop, day, k
            break
    if exit_px is None:
        exit_px, exit_d, hold = float(b5[b5.index.date == days[-1]]["Close"].iloc[-1]), days[-1], len(days) - 1
        stopped = False
    else:
        stopped = True
    res["NAIVE"] = {"entry": entry, "exit": exit_px, "ret": exit_px / entry - 1,
                    "stopped": stopped, "hold": hold, "exit_day": exit_d}
    # --- CLOSE3: daily-close fixed stop
    exit_px = exit_d = None
    for k, day in enumerate(days):
        c = float(b5[b5.index.date == day]["Close"].iloc[-1])
        if c <= naive_stop:
            exit_px, exit_d, hold = c, day, k
            break
    if exit_px is None:
        exit_px, exit_d, hold = float(b5[b5.index.date == days[-1]]["Close"].iloc[-1]), days[-1], len(days) - 1
        stopped = False
    else:
        stopped = True
    res["CLOSE3"] = {"entry": entry, "exit": exit_px, "ret": exit_px / entry - 1,
                     "stopped": stopped, "hold": hold, "exit_day": exit_d}
    # --- GUARDIAN: chandelier + first-hour guard + persistence (no lookahead)
    didx = list(daily.index.date)
    stop = None
    breach = 0
    exit_px = exit_d = None
    events = []
    for k, day in enumerate(days):
        try:
            i = didx.index(day)
        except ValueError:
            continue
        if i < 1 or not np.isfinite(daily["atr"].iloc[i - 1]):
            continue
        atr = float(daily["atr"].iloc[i - 1])
        base = float(daily["hi20"].iloc[i - 1]) - K_ATR * atr      # data through PREV day
        stop = base if stop is None else max(stop, base)
        bars = b5[b5.index.date == day]
        for ts, c in bars["Close"].items():
            hm = ts.hour * 60 + ts.minute
            trigger = stop - (FIRST_EXTRA * atr if hm <= 615 else 0.0)
            if c <= trigger:
                breach += 1
                if breach == 1:
                    events.append(f"{ts.strftime('%m-%d %H:%M')} breach 1/2 (px {c:.1f} vs trigger {trigger:.1f})")
                if breach >= CONFIRM_N:
                    exit_px, exit_d, hold = float(c), day, k
                    events.append(f"{ts.strftime('%m-%d %H:%M')} CONFIRMED EXIT @ {c:.1f}")
                    break
            else:
                breach = 0
        if exit_px is not None:
            break
        events.append(f"{day} stop ratchet -> {stop:.1f}")
    if exit_px is None:
        exit_px, exit_d, hold = float(b5[b5.index.date == days[-1]]["Close"].iloc[-1]), days[-1], len(days) - 1
        stopped = False
    else:
        stopped = True
    res["GUARDIAN"] = {"entry": entry, "exit": exit_px, "ret": exit_px / entry - 1,
                       "stopped": stopped, "hold": hold, "exit_day": exit_d, "events": events}
    # --- GUARDIAN2: ENTRY-ANCHORED chandelier (highest price SINCE entry, never
    #     pre-entry highs) + the same first-hour guard and persistence. The fix the
    #     replay demanded: your trade's risk anchors to YOUR entry, not last month's spike.
    hi_since = entry
    breach = 0
    exit_px = exit_d = None
    events2 = []
    for k, day in enumerate(days):
        try:
            i = didx.index(day)
        except ValueError:
            continue
        if i < 1 or not np.isfinite(daily["atr"].iloc[i - 1]):
            continue
        atr = float(daily["atr"].iloc[i - 1])
        bars = b5[b5.index.date == day]
        for ts, c in bars["Close"].items():
            hi_since = max(hi_since, float(c))
            stop2 = hi_since - K_ATR * atr
            hm = ts.hour * 60 + ts.minute
            trigger = stop2 - (FIRST_EXTRA * atr if hm <= 615 else 0.0)
            if c <= trigger:
                breach += 1
                if breach >= CONFIRM_N:
                    exit_px, exit_d, hold = float(c), day, k
                    events2.append(f"{ts.strftime('%m-%d %H:%M')} CONFIRMED EXIT @ {c:.1f} (stop {stop2:.1f})")
                    break
            else:
                breach = 0
        if exit_px is not None:
            break
        events2.append(f"{day} trail -> {hi_since - K_ATR * atr:.1f} (hi {hi_since:.1f})")
    if exit_px is None:
        exit_px, exit_d, hold = float(b5[b5.index.date == days[-1]]["Close"].iloc[-1]), days[-1], len(days) - 1
        stopped = False
    else:
        stopped = True
    res["GUARDIAN2"] = {"entry": entry, "exit": exit_px, "ret": exit_px / entry - 1,
                        "stopped": stopped, "hold": hold, "exit_day": exit_d, "events": events2}
    # --- DEFENSE: entry-anchored chandelier that TIGHTENS (k 2.5 -> 1.5) while the tape
    #     is hostile: vol spike (rv5/rv20 > 1.4 from prior day, or today's range > 1.5*ATR)
    #     or adverse persistence (3 consecutive falling 5m closes below the day's open).
    #     Transient: evaluation-only tightening. Profit-lock: if up >= 1.5*ATR when defense
    #     fires, stop floors at breakeven+0.1*ATR. First-hour guard still applies.
    hi_since = entry
    breach = 0
    exit_px = exit_d = None
    events3 = []
    rv_ratio_series = (daily["Close"].pct_change().rolling(5).std()
                       / daily["Close"].pct_change().rolling(20).std())
    for k, day in enumerate(days):
        try:
            i = didx.index(day)
        except ValueError:
            continue
        if i < 1 or not np.isfinite(daily["atr"].iloc[i - 1]):
            continue
        atr = float(daily["atr"].iloc[i - 1])
        rvr = float(rv_ratio_series.iloc[i - 1]) if np.isfinite(rv_ratio_series.iloc[i - 1]) else 1.0
        bars = b5[b5.index.date == day]
        if not len(bars):
            continue
        day_open = float(bars["Close"].iloc[0])
        day_hi = day_lo = day_open
        last3 = []
        for ts, c in bars["Close"].items():
            c = float(c)
            hi_since = max(hi_since, c)
            day_hi, day_lo = max(day_hi, c), min(day_lo, c)
            last3 = (last3 + [c])[-3:]
            spike = rvr > 1.4 or (day_hi - day_lo) > 1.5 * atr
            adverse = len(last3) == 3 and last3[0] > last3[1] > last3[2] and c < day_open
            defense = spike or adverse
            k_eff = 1.5 if defense else K_ATR
            stop_eval = hi_since - k_eff * atr
            if defense and hi_since >= entry + 1.5 * atr:
                stop_eval = max(stop_eval, entry + 0.1 * atr)       # never give back a winner
            hm = ts.hour * 60 + ts.minute
            trigger = stop_eval - (FIRST_EXTRA * atr if hm <= 615 else 0.0)
            if c <= trigger:
                breach += 1
                if breach >= CONFIRM_N:
                    exit_px, exit_d, hold = c, day, k
                    events3.append(f"{ts.strftime('%m-%d %H:%M')} DEFENSE EXIT @ {c:.1f} "
                                   f"(k={k_eff}, spike={spike}, adverse={adverse})")
                    break
            else:
                breach = 0
        if exit_px is not None:
            break
    if exit_px is None:
        exit_px, exit_d, hold = float(b5[b5.index.date == days[-1]]["Close"].iloc[-1]), days[-1], len(days) - 1
        stopped = False
    else:
        stopped = True
    res["DEFENSE"] = {"entry": entry, "exit": exit_px, "ret": exit_px / entry - 1,
                      "stopped": stopped, "hold": hold, "exit_day": exit_d, "events": events3}
    # --- reference: no stop, hold the window
    final = float(b5[b5.index.date == days[-1]]["Close"].iloc[-1])
    res["NOSTOP"] = {"entry": entry, "exit": final, "ret": final / entry - 1,
                     "stopped": False, "hold": len(days) - 1, "exit_day": days[-1]}
    return res


def noise_stop(b5, sessions, exit_day, exit_px):
    """Stopped, but price recovered above the exit within 10 sessions -> a stop-trap."""
    after = [s for s in sessions if s > exit_day][:10]
    if not after:
        return False
    mx = max(float(b5[b5.index.date == d]["Close"].max()) for d in after if len(b5[b5.index.date == d]))
    return mx >= exit_px * 1.02


def run(sym):
    b5, daily = load_bars(sym)
    if len(b5) < 500:
        return None
    sessions = sorted(set(b5.index.date))
    agg = {p: [] for p in ("NAIVE", "CLOSE3", "GUARDIAN", "GUARDIAN2", "DEFENSE", "NOSTOP")}
    for ed in sessions[:-3]:
        r = replay_one(b5, daily, ed, sessions)
        if not r:
            continue
        for p, v in r.items():
            v = dict(v)
            v["noise"] = v["stopped"] and noise_stop(b5, sessions, v["exit_day"], v["exit"])
            agg[p].append(v)
    out = {"symbol": sym, "entries_tested": len(agg["NAIVE"]),
           "window": f"{sessions[0]} -> {sessions[-1]}", "policies": {}}
    for p, rows in agg.items():
        if not rows:
            continue
        rets = np.array([r["ret"] for r in rows]) * 100
        stopped = [r for r in rows if r["stopped"]]
        noise = [r for r in stopped if r["noise"]]
        out["policies"][p] = {
            "median_ret_pct": round(float(np.median(rets)), 2),
            "mean_ret_pct": round(float(np.mean(rets)), 2),
            "worst_pct": round(float(rets.min()), 2),
            "stopped_pct": round(100 * len(stopped) / len(rows), 1),
            "noise_stop_pct_of_stops": round(100 * len(noise) / max(len(stopped), 1), 1),
            "avg_hold_sessions": round(float(np.mean([r["hold"] for r in rows])), 1),
        }
    return out, b5, daily, sessions


def timeline(sym, b5, daily, sessions, entry_day):
    r = replay_one(b5, daily, entry_day, sessions)
    if not r:
        print(f"  (no data for entry {entry_day})")
        return
    print(f"\n  TIMELINE — {sym} bought {entry_day} @ {r['NAIVE']['entry']:.1f}")
    for p in ("NAIVE", "CLOSE3", "GUARDIAN", "GUARDIAN2", "DEFENSE", "NOSTOP"):
        v = r[p]
        tag = "stopped" if v["stopped"] else "survived"
        print(f"    {p:<9} exit {v['exit']:>8.1f} on {v['exit_day']}  ret {v['ret']*100:>+6.1f}%  ({tag}, {v['hold']}d)")
    ev = r["GUARDIAN"].get("events", [])
    for e in ev[-6:]:
        print(f"      guardian: {e}")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    syms = [s.upper() for s in sys.argv[1:] if not s.startswith("-")] or ["TEJASNET", "ADANIPORTS", "RELIANCE"]
    results = {}
    for sym in syms:
        got = run(sym)
        if not got:
            print(sym, "— insufficient intraday data")
            continue
        out, b5, daily, sessions = got
        results[sym] = out
        print(f"\n{'='*78}\n{sym} — {out['entries_tested']} entry days replayed ({out['window']}), hold ≤ {MAX_HOLD_SESS} sessions")
        hdr = f"{'policy':<10}{'median%':>9}{'mean%':>8}{'worst%':>8}{'stopped':>9}{'noise-stops':>12}{'avg hold':>10}"
        print(hdr); print("-" * len(hdr))
        for p in ("NAIVE", "CLOSE3", "GUARDIAN", "GUARDIAN2", "DEFENSE", "NOSTOP"):
            s = out["policies"].get(p)
            if not s:
                continue
            print(f"{p:<10}{s['median_ret_pct']:>9}{s['mean_ret_pct']:>8}{s['worst_pct']:>8}"
                  f"{s['stopped_pct']:>8}%{s['noise_stop_pct_of_stops']:>11}%{s['avg_hold_sessions']:>9}d")
        if sym == "TEJASNET":
            for ed in sessions:
                if str(ed) in ("2026-05-07", "2026-05-08"):
                    timeline(sym, b5, daily, sessions, ed)
    json.dump(results, open(os.path.join(HERE, "marleg_guardian_replay.json"), "w"), indent=1)
    print(f"\n[wrote marleg_guardian_replay.json]")


if __name__ == "__main__":
    main()
