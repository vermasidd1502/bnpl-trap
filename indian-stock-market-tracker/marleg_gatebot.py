"""
Marle-G — GATEBOT. The synchronized live system: volume signal -> pre-trade gate ->
PAPER entry -> stop + calendar exit, managed automatically through IST market hours.

Backtest-derived configuration (marleg_gate_backtest.py, 2024-04 -> 2026-06):
    entries  : top-2 strongest ud(20d)>1.3 per day, AND price > 50d & 200d MA (the gate)
    sizing   : 1% of paper capital at risk, stop = 2.5*ATR(14); notional cap 15%
    horizon  : 21 trading days (the best cell: +4.1% net, Sharpe 0.24, PF 1.10) — the
               TIME EXIT is set AT ENTRY; smart-money DISTRIBUTING names get 10d (rental)
    exits    : stop breach (2 consecutive checks, extra first-hour margin — the guardian's
               noise filters) OR horizon reached. Whichever first.
    regime   : new entries only when the regime gauge >= 45 (live overlay; exits always run)

HONESTY: the backtested edge is modest and single-path (Sharpe 0.24; neighboring horizon
cells are negative — it would not clear a DSR test). This bot exists to learn the pipeline
end-to-end with discipline, not to print money.

PAPER ONLY — marks a local book (marleg_gatebot_book.json). It NEVER sends a real order.

  python marleg_gatebot.py            # live loop (leave running through IST hours)
  python marleg_gatebot.py --test     # one forced scan cycle now
"""
import os, sys, json, time, math, argparse
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import yfinance as yf
import requests
import marleg_slack as slack
from marleg_stop_guardian import phase, _live_px, now_ist

HERE = os.path.dirname(os.path.abspath(__file__))
BOOK = os.path.join(HERE, "marleg_gatebot_book.json")
CAP = 100000.0
RISK = 0.01
K_ATR = 2.5
HORIZON_TD = 21          # trading days (backtest best cell)
HORIZON_RENTAL = 10      # smart-money DISTRIBUTING -> rental horizon
MAXPOS = 8
DAILY_NEW = 2            # top-2 strongest per day (selectivity was the edge)
NOTIONAL_CAP = 0.15
REGIME_MIN = 45
FIRST_HOUR_EXTRA = 0.35  # extra ATRs of breach required 09:15-10:15 IST
CONFIRM_N = 2
UNIV = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "ITC", "LT", "SBIN", "AXISBANK",
        "KOTAKBANK", "BHARTIARTL", "BAJFINANCE", "HINDUNILVR", "MARUTI", "SUNPHARMA",
        "EICHERMOT", "TATASTEEL", "M&M", "NTPC", "TITAN", "ASIANPAINT", "ULTRACEMCO",
        "WIPRO", "ADANIPORTS", "JSWSTEEL", "COALINDIA", "ONGC", "GRASIM", "HCLTECH", "CIPLA",
        "POWERGRID", "BAJAJFINSV", "TECHM", "NESTLEIND"]

_DAILY = {"date": None, "ud": None, "sma50": None, "sma200": None, "atr": None}


def _book():
    try:
        return json.load(open(BOOK))
    except Exception:
        return {"start": CAP, "cash": CAP, "positions": [], "closed": [], "log": [],
                "entered": {}, "breach": {}}


def _save(b):
    try:
        json.dump(b, open(BOOK + ".tmp", "w"), indent=1)
        os.replace(BOOK + ".tmp", BOOK)
    except Exception:
        pass


def _log(b, msg):
    line = f"{now_ist().strftime('%m-%d %H:%M')} {msg}"
    b["log"] = (b.get("log", []) + [line])[-100:]
    print(line, flush=True)


def refresh_daily():
    """Once per IST day: 1y batch -> ud / SMAs / ATR for the whole universe."""
    today = now_ist().strftime("%Y-%m-%d")
    if _DAILY["date"] == today and _DAILY["ud"] is not None:
        return True
    df = yf.download([s + ".NS" for s in UNIV], period="1y", interval="1d",
                     group_by="ticker", auto_adjust=False, progress=False, threads=True)
    C, H, L, V = {}, {}, {}, {}
    for s in UNIV:
        try:
            d = df[s + ".NS"].dropna()
            if len(d) > 210:
                C[s], H[s], L[s], V[s] = d["Close"], d["High"], d["Low"], d["Volume"]
        except Exception:
            pass
    C, H, L, V = pd.DataFrame(C), pd.DataFrame(H), pd.DataFrame(L), pd.DataFrame(V)
    if C.shape[1] < 10:
        return False
    rc = C.pct_change()
    udf = (V.where(rc > 0, 0.0).rolling(20).sum() /
           V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan))
    ud = udf.iloc[-1]
    # v1.3: the gate's "ud > MA & rising" element — best 10d filter (+0.50% med, 54.4% win)
    _DAILY["ud_rising"] = (udf.iloc[-1] > udf.rolling(10).mean().iloc[-1]) & (udf.iloc[-1] > udf.iloc[-4])
    pc = C.shift(1)
    tr = pd.concat([(H - L).stack(), (H - pc).abs().stack(), (L - pc).abs().stack()],
                   axis=1).max(axis=1).unstack()
    _DAILY.update({"date": today, "ud": ud, "sma50": C.rolling(50).mean().iloc[-1],
                   "sma200": C.rolling(200).mean().iloc[-1], "atr": tr.rolling(14).mean().iloc[-1]})
    try:                                   # auto-record today's volume suggestions (once/day)
        import marleg_volume_ledger as vl
        vl.record_today()
    except Exception:
        pass
    return True


def regime_gauge():
    try:
        r = requests.get("http://localhost:8777/api/regime", timeout=6).json()
        g = r.get("gauge")
        return float(g) if g is not None else None
    except Exception:
        return None


def smart_verdict(sym):
    try:
        import marleg_smartmoney as sm
        f = sm.flow(sym)
        return f.get("verdict")
    except Exception:
        return None


def scan_entries(b, force=False):
    """Top-DAILY_NEW gate-passing candidates -> open PAPER positions."""
    if not refresh_daily():
        _log(b, "universe refresh failed — no entries this cycle")
        return
    today = now_ist().strftime("%Y-%m-%d")
    entered_today = b.get("entered", {}).get(today, 0)
    if entered_today >= DAILY_NEW or len(b["positions"]) >= MAXPOS:
        return
    g = regime_gauge()
    if not force and g is not None and g < REGIME_MIN:
        _log(b, f"regime gauge {g:.0f} < {REGIME_MIN} — risk-off, no new entries")
        return
    held = {p["sym"] for p in b["positions"]}
    ud, s50, s200, atr = _DAILY["ud"], _DAILY["sma50"], _DAILY["sma200"], _DAILY["atr"]
    sig = ud.dropna()
    # v1.2 GO-TO ZONE (decade-ratified): 2.0-3.6 best band (fwd21 med +0.88%, win 54.4%);
    # edge decays past 3.6; ud>=6 = event-suspect prints (10+ band median NEGATIVE) -> excluded.
    sig = sig[(sig > 1.3) & (sig < 6.0)]
    rising = _DAILY.get("ud_rising")
    if rising is not None:                       # v1.3: require ud > its 10d MA and rising
        sig = sig[[s for s in sig.index if bool(rising.get(s, False))]]
    _prio = lambda u: 0 if 2.0 <= u < 3.6 else (1 if u < 2.0 else 2)
    order = [s for s, _ in sorted(sig.items(), key=lambda kv: (_prio(kv[1]), -kv[1]))]
    px_map = _live_px([s for s in order[:10] if s not in held])
    for sym in order:
        if entered_today >= DAILY_NEW or len(b["positions"]) >= MAXPOS:
            break
        if sym in held:
            continue
        px = px_map.get(sym)
        a = atr.get(sym)
        if px is None or a is None or not np.isfinite(a) or a <= 0:
            continue
        if not (px > s50.get(sym, np.inf) and px > s200.get(sym, np.inf)):
            continue
        stop_dist = K_ATR * float(a)
        qty = int((CAP * RISK) // stop_dist)
        notional = qty * px
        if notional > CAP * NOTIONAL_CAP:
            qty = int(CAP * NOTIONAL_CAP // px)
            notional = qty * px
        if qty < 1 or notional > b["cash"]:
            continue
        sv = smart_verdict(sym)
        horizon = HORIZON_RENTAL if sv == "DISTRIBUTING" else HORIZON_TD
        uval = float(sig.get(sym, 0))
        pos = {"sym": sym, "qty": qty, "entry": round(px, 2),
               "stop": round(px - stop_dist, 2), "atr": round(float(a), 2),
               "horizon_td": horizon, "days_held": 0, "entry_date": today,
               "last_session": today, "smart": sv or "n/a",
               "why": f"ud {uval:.2f}{' (go-to zone)' if 2.0 <= uval < 3.6 else ''}, gate pass"}
        b["cash"] -= notional
        b["positions"].append(pos)
        b.setdefault("entered", {})[today] = entered_today = entered_today + 1
        _log(b, f"PAPER BUY {qty} {sym} @ {pos['entry']} stop {pos['stop']} "
                f"horizon {horizon}td (smart$ {pos['smart']})")
        slack.notify(f"🟢 *GateBot PAPER BUY* {qty} {sym} @ {pos['entry']}",
                     fields={"stop": str(pos["stop"]), "horizon": f"{horizon} td",
                             "smart$": pos["smart"], "why": pos["why"]})


def manage_exits(b):
    if not b["positions"]:
        return
    today = now_ist().strftime("%Y-%m-%d")
    ph = phase()
    px_map = _live_px([p["sym"] for p in b["positions"]])
    for p in list(b["positions"]):
        px = px_map.get(p["sym"])
        if px is None:
            continue
        if p.get("last_session") != today:
            p["days_held"] = p.get("days_held", 0) + 1
            p["last_session"] = today
        p["now"] = round(px, 2)
        p["upnl"] = round((px - p["entry"]) * p["qty"], 1)
        trigger = p["stop"] - (FIRST_HOUR_EXTRA * p["atr"] if ph == "first_hour" else 0.0)
        reason = None
        if px <= trigger:
            n = b.setdefault("breach", {}).get(p["sym"], 0) + 1
            b["breach"][p["sym"]] = n
            if n >= CONFIRM_N:
                reason = "stop (confirmed)"
        else:
            b.setdefault("breach", {})[p["sym"]] = 0
            if p["days_held"] >= p["horizon_td"]:
                reason = f"time ({p['horizon_td']}td horizon)"
        if reason:
            pnl = (px - p["entry"]) * p["qty"]
            b["cash"] += p["qty"] * px
            b["closed"].append({**p, "exit": round(px, 2), "pnl": round(pnl, 1),
                                "reason": reason, "exit_ts": now_ist().strftime("%m-%d %H:%M")})
            b["positions"].remove(p)
            b["breach"].pop(p["sym"], None)
            emoji = "🔴" if pnl < 0 else "🟢"
            _log(b, f"PAPER EXIT {p['sym']} @ {round(px,2)} P&L {round(pnl,1)} — {reason}")
            slack.notify(f"{emoji} *GateBot PAPER EXIT* {p['sym']} @ {round(px,2)} "
                         f"P&L ₹{round(pnl,1)} — {reason}")


def mark(b):
    inv = sum(p["qty"] * p.get("now", p["entry"]) for p in b["positions"])
    b["equity"] = round(b["cash"] + inv, 1)
    b["ret_pct"] = round((b["equity"] / b["start"] - 1) * 100, 2)
    b["realized"] = round(sum(c["pnl"] for c in b["closed"]), 1)
    b["asof"] = now_ist().strftime("%Y-%m-%d %H:%M IST")
    b["config"] = {"horizon_td": HORIZON_TD, "rental_td": HORIZON_RENTAL, "k_atr": K_ATR,
                   "risk_pct": RISK * 100, "daily_new": DAILY_NEW, "max_pos": MAXPOS,
                   "regime_min": REGIME_MIN, "paper": True}


def cycle(force=False):
    b = _book()
    ph = phase()
    if ph != "closed" or force:
        manage_exits(b)
        if force or ph in ("open", "last_hour"):       # entries after the first hour only
            scan_entries(b, force=force)
    mark(b)
    _save(b)
    return b


def run():
    print("GATEBOT running — PAPER ONLY. Entries 10:15-15:25 IST (top-2/day, gate+regime), "
          "exits managed all session. Hourly P&L digest to Slack. Never sends a real order.", flush=True)
    last_digest = 0.0
    while True:
        try:
            ph = phase()
            if ph == "closed":
                time.sleep(900)
            else:
                b = cycle()
                if time.time() - last_digest > 3600:
                    last_digest = time.time()
                    pos = " · ".join(f"{p['sym']} {p.get('upnl', 0):+.0f}"
                                     for p in b.get("positions", [])) or "flat"
                    slack.notify(f"📊 *GateBot paper P&L* — equity ₹{b.get('equity', 0):,.0f} "
                                 f"({b.get('ret_pct', 0):+}%) · open: {pos}")
                    print(f"[digest] equity {b.get('equity')} ret {b.get('ret_pct')}% · {pos}", flush=True)
                time.sleep(300)
        except KeyboardInterrupt:
            return
        except Exception as e:
            print("gatebot cycle error:", str(e)[:120], flush=True)
            time.sleep(300)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="one forced scan cycle now")
    a = ap.parse_args()
    if a.test:
        b = cycle(force=True)
        print(f"\nequity {b['equity']} · ret {b['ret_pct']}% · open {len(b['positions'])} · "
              f"closed {len(b['closed'])} · cash {round(b['cash'],1)}")
        for p in b["positions"]:
            print(f"  {p['sym']:<12} {p['qty']:>4} @ {p['entry']:>8} stop {p['stop']:>8} "
                  f"hz {p['horizon_td']}td held {p['days_held']}d smart$ {p['smart']}")
        print(f"Slack: {'ON' if slack.enabled() else 'OFF'}")
        return
    run()


if __name__ == "__main__":
    main()
