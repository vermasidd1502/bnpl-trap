"""
Marle-G — OVERNIGHT AUTO-TRADER (paper, multi-profile).

Captures the volume signal's OVERNIGHT edge — the backtest (Volume Day-Book) showed the
overnight leg at ~+3.2 Sharpe while the intraday leg LOSES (~-1.8). So this bot:
    ENTER  in the last 30 min of the NSE session (15:00-15:30 IST)
    HOLD   overnight
    EXIT   at the next session's open (09:15-09:50 IST)
Picks are quality-filtered (REAL ud winners only; fade-prone "suspect" spikes are skipped).

Profiles: conservative / balanced / aggressive / adaptive / sid (custom: long + short).
PAPER ONLY — it marks local cash books; it never sends a real order.

  python marleg_autotrader.py          # live session loop (keep machine on, IST hours)
  python marleg_autotrader.py --test   # force one entry cycle now (for verification)
"""
import os, sys, json, time
from datetime import datetime, timezone, timedelta
import numpy as np, pandas as pd, yfinance as yf
import marleg_signal_quality as sq

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
START = 100000.0
ENTRY_WIN = (15 * 60 + 0, 15 * 60 + 30)     # 15:00-15:30 IST: enter the overnight book
EXIT_WIN = (9 * 60 + 15, 9 * 60 + 50)       # 09:15-09:50 IST: exit at the open
PROFILES = {
    "conservative": {"longs": 3, "shorts": 0, "alloc": 0.10, "suspect": False},
    "balanced":     {"longs": 5, "shorts": 0, "alloc": 0.18, "suspect": False},
    "aggressive":   {"longs": 8, "shorts": 0, "alloc": 0.30, "suspect": True},
    "adaptive":     {"longs": 5, "shorts": 0, "alloc": 0.18, "suspect": False},
    "sid":          {"longs": 5, "shorts": 3, "alloc": 0.20, "suspect": False},   # custom: long/short
}


def now_ist():
    return datetime.now(IST)


def _hm(t):
    return t.hour * 60 + t.minute


def _bf(p):
    return os.path.join(HERE, f"marleg_at_{p}.json")


def _load(p):
    try:
        return json.load(open(_bf(p)))
    except Exception:
        return {"profile": p, "start": START, "positions": [], "closed": [], "log": [],
                "entered_day": None, "mode": "overnight"}


def _save(p, b):
    try:
        tmp = _bf(p) + ".tmp"
        json.dump(b, open(tmp, "w"), indent=1)
        os.replace(tmp, _bf(p))
    except Exception:
        pass


def _log(b, msg):
    b["log"] = (b.get("log", []) + [f"{now_ist().strftime('%m-%d %H:%M')} {msg}"])[-60:]
    print(f"[{b['profile']}] {msg}", flush=True)


def _prices(syms):
    if not syms:
        return {}
    d = yf.download([s + ".NS" for s in syms], period="1d", interval="5m",
                    group_by="ticker", progress=False, threads=True)
    out = {}
    for s in syms:
        try:
            c = d[s + ".NS"]["Close"].dropna()
            if len(c):
                out[s] = float(c.iloc[-1])
        except Exception:
            try:
                c = d["Close"].dropna()
                if len(c):
                    out[s] = float(c.iloc[-1])
            except Exception:
                pass
    return out


def _enter(b, longs, shorts):
    cfg = PROFILES[b["profile"]]
    today = now_ist().strftime("%Y-%m-%d")
    per = b["start"] * cfg["alloc"]
    for w in longs[:cfg["longs"]]:
        p = w.get("price")
        if not p:
            continue
        qty = int(per // p)
        if qty < 1:
            continue
        b["positions"].append({"sym": w["sym"], "side": "LONG", "qty": qty, "entry": round(p, 2),
                               "stop": w.get("stop"), "target": w.get("target"),
                               "entry_day": today, "why": "REAL ud winner"})
        _log(b, f"LONG {qty} {w['sym']} @ {round(p,2)} (overnight)")
    for s in shorts[:cfg["shorts"]]:
        p = s.get("price")
        if not p:
            continue
        qty = int(per // p)
        if qty < 1:
            continue
        b["positions"].append({"sym": s["sym"], "side": "SHORT", "qty": qty, "entry": round(p, 2),
                               "stop": s.get("swing_high") or round(p * 1.03, 2), "target": s.get("target_down"),
                               "entry_day": today, "why": "; ".join(s.get("reasons", []))})
        _log(b, f"SHORT {qty} {s['sym']} @ {round(p,2)} (overnight)")
    b["entered_day"] = today


def _exit_all(b, px, reason):
    for pos in list(b["positions"]):
        pr = px.get(pos["sym"], pos["entry"])
        pnl = (pr - pos["entry"]) * pos["qty"] if pos["side"] == "LONG" else (pos["entry"] - pr) * pos["qty"]
        b["closed"].append({**pos, "exit": round(pr, 2), "pnl": round(pnl, 1),
                            "reason": reason, "exit_ts": now_ist().strftime("%m-%d %H:%M")})
        _log(b, f"EXIT {pos['side']} {pos['qty']} {pos['sym']} @ {round(pr,2)} · P&L {round(pnl,1)} · {reason}")
    b["positions"] = []


def _mark(b, px):
    up = 0.0
    for pos in b["positions"]:
        pr = px.get(pos["sym"], pos["entry"])
        pos["now"] = round(pr, 2)
        pos["upnl"] = round((pr - pos["entry"]) * pos["qty"] if pos["side"] == "LONG"
                            else (pos["entry"] - pr) * pos["qty"], 1)
        up += pos["upnl"]
    b["realized"] = round(sum(c["pnl"] for c in b["closed"]), 1)
    b["equity"] = round(b["start"] + b["realized"] + up, 1)
    b["ret_pct"] = round((b["equity"] / b["start"] - 1) * 100, 2)
    b["open"], b["n_closed"] = len(b["positions"]), len(b["closed"])
    b["asof"] = now_ist().strftime("%Y-%m-%d %H:%M IST")


def cycle(force_entry=False):
    t = now_ist(); today = t.strftime("%Y-%m-%d"); hm = _hm(t); weekday = t.weekday() < 5
    in_entry = force_entry or (weekday and ENTRY_WIN[0] <= hm <= ENTRY_WIN[1])
    in_exit = weekday and EXIT_WIN[0] <= hm <= EXIT_WIN[1]
    books = {p: _load(p) for p in PROFILES}
    # EXIT overnight holds at the open
    if in_exit:
        held = set()
        for b in books.values():
            held.update(pos["sym"] for pos in b["positions"])
        px = _prices(list(held)) if held else {}
        for p, b in books.items():
            if b["positions"] and any(pos["entry_day"] != today for pos in b["positions"]):
                _exit_all(b, px, "open square-off (overnight)")
    # ENTRY in the last 30 min — one shared quality scan
    if in_entry and any((not b["positions"] and b.get("entered_day") != today) for b in books.values()):
        scan = sq.scan()
        longs = scan.get("winners", [])
        longs_all = longs + scan.get("suspects", [])
        shorts = scan.get("shorts", [])
        for p, b in books.items():
            if not b["positions"] and b.get("entered_day") != today:
                _enter(b, longs_all if PROFILES[p]["suspect"] else longs, shorts)
    # MARK everything
    allsyms = set()
    for b in books.values():
        allsyms.update(pos["sym"] for pos in b["positions"])
    px = _prices(list(allsyms)) if allsyms else {}
    for p, b in books.items():
        _mark(b, px); _save(p, b)
    return books


def run(loop_min=10):
    print("OVERNIGHT auto-trader (paper, multi-profile) started — windows 15:00-15:30 enter / 09:15 exit IST", flush=True)
    while True:
        try:
            cycle()
        except Exception as e:
            print("cycle error:", e, flush=True)
        time.sleep(loop_min * 60)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if "--test" in sys.argv:
        books = cycle(force_entry=True)
        for p, b in books.items():
            print(f"\n[{p}] equity {b.get('equity')} · ret {b.get('ret_pct')}% · open {b.get('open')} · closed {b.get('n_closed')}")
            for pos in b["positions"]:
                print(f"   {pos['side']} {pos['qty']} {pos['sym']} @ {pos['entry']}  ({pos['why']})")
        return
    if "--once" in sys.argv:
        cycle(); return
    run()


if __name__ == "__main__":
    main()
