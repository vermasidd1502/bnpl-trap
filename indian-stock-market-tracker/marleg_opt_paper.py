"""
marleg_opt_paper.py — MANUAL single-leg option PAPER trades you open from the matrix / position desk.

Distinct from marleg_paper_options.py (that auto-trades debit spreads off the gated screen). This one lets
you paper-trade a SPECIFIC contract you picked — entry marked to the live Groww premium, then tracked to a
holding-period target with live P&L, theta, and DTE. PAPER ONLY — it never places a real order and never
touches your Groww account (read-only quotes). Book: marleg_opt_paper_book.json (runtime, git-ignored).
"""
import os
import json
import datetime as dt

import marleg_vol as mv
import marleg_options_monitor as mom

HERE = os.path.dirname(os.path.abspath(__file__))
BOOK = os.path.join(HERE, "marleg_opt_paper_book.json")


def _ist():
    return dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)


def _load():
    try:
        return json.load(open(BOOK, encoding="utf-8"))
    except Exception:
        return {"trades": [], "seq": 0}


def _save(b):
    json.dump(b, open(BOOK, "w", encoding="utf-8"), indent=1, default=str)


def _add_trading_days(start, n):
    d, added = start, 0
    while added < n:
        d += dt.timedelta(days=1)
        if mv._is_trading_day(d):
            added += 1
    return d


def open_trade(symbol, qty, hold_days=10, note="", source="matrix"):
    symbol = (symbol or "").upper().strip()
    info = mv.parse_option_any(symbol)
    if not info:
        return {"ok": False, "error": f"could not parse '{symbol}'"}
    q = mom.option_quote(symbol)
    if not isinstance(q, dict) or "error" in q:
        return {"ok": False, "error": f"no live quote for {symbol} ({(q or {}).get('error')})"}
    prem = q.get("ltp") or q.get("ask")
    if not prem:
        return {"ok": False, "error": f"no premium for {symbol}"}
    g = mom._g()
    spot = mv.underlying_ltp(info["underlying"], g)
    today = _ist().date()
    b = _load()
    b["seq"] = b.get("seq", 0) + 1
    t = {"id": b["seq"], "symbol": symbol, "underlying": info["underlying"], "right": info["right"],
         "strike": info["strike"], "expiry": info["expiry"].isoformat(),
         "qty": float(qty), "entry_prem": round(float(prem), 2), "entry_spot": round(spot, 2) if spot else None,
         "entry_iv_pct": round((q.get("iv") or 0) * 100, 1) if q.get("iv") else None,
         "opened": _ist().strftime("%Y-%m-%d %H:%M IST"), "opened_date": today.isoformat(),
         "hold_days": int(hold_days), "target_exit_date": _add_trading_days(today, int(hold_days)).isoformat(),
         "source": source, "note": note, "status": "open"}
    b.setdefault("trades", []).append(t)
    _save(b)
    return {"ok": True, "opened": t, "msg": f"PAPER opened {symbol} x{qty} @ ₹{t['entry_prem']}"}


def _mark(t):
    today = _ist().date()
    exp = dt.date.fromisoformat(t["expiry"])
    dte = (exp - today).days
    q = mom.option_quote(t["symbol"])
    now = (q.get("ltp") if isinstance(q, dict) and "error" not in q else None)
    if not now and dte < 0 and t.get("entry_spot") is not None:
        now = max(0.0, (t["entry_spot"] - t["strike"]) if t["right"] == "CE" else (t["strike"] - t["entry_spot"]))
    now = now if now is not None else t["entry_prem"]
    pnl_abs = (now - t["entry_prem"]) * t["qty"]
    pnl_pct = (now / t["entry_prem"] - 1) * 100 if t["entry_prem"] else 0
    held = (today - dt.date.fromisoformat(t["opened_date"])).days
    to_tgt = (dt.date.fromisoformat(t["target_exit_date"]) - today).days
    return {**t, "now_prem": round(now, 2), "pnl_abs": round(pnl_abs, 0), "pnl_pct": round(pnl_pct, 1),
            "dte": dte, "days_held": held, "sessions_to_target": to_tgt,
            "due": to_tgt <= 0, "oi": q.get("oi") if isinstance(q, dict) else None}


def close_trade(tid):
    b = _load()
    tid = int(tid)
    for t in b.get("trades", []):
        if t["id"] == tid and t["status"] == "open":
            m = _mark(t)
            t["status"] = "closed"; t["exit_prem"] = m["now_prem"]; t["exit_pnl"] = m["pnl_abs"]
            t["closed"] = _ist().strftime("%Y-%m-%d %H:%M IST")
            _save(b)
            return {"ok": True, "closed": t}
    return {"ok": False, "error": f"no open trade #{tid}"}


def book():
    b = _load()
    open_t = [_mark(t) for t in b.get("trades", []) if t["status"] == "open"]
    closed_t = [t for t in b.get("trades", []) if t["status"] == "closed"]
    open_t.sort(key=lambda x: x["dte"])
    tot = round(sum(t["pnl_abs"] for t in open_t), 0)
    real = round(sum(t.get("exit_pnl", 0) for t in closed_t), 0)
    return {"ok": True, "open": open_t, "closed": closed_t[-12:],
            "n_open": len(open_t), "unrealized": tot, "realized": real,
            "asof": _ist().strftime("%Y-%m-%d %H:%M IST"),
            "caveat": "PAPER trades only — no real order is ever placed; your Groww account is read-only."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if len(sys.argv) > 1 and sys.argv[1] == "--open":
        print(json.dumps(open_trade(sys.argv[2], float(sys.argv[3]) if len(sys.argv) > 3 else 75,
                                    int(sys.argv[4]) if len(sys.argv) > 4 else 10), indent=2, default=str))
    elif len(sys.argv) > 1 and sys.argv[1] == "--close":
        print(json.dumps(close_trade(sys.argv[2]), indent=2, default=str))
    else:
        r = book()
        print(f"\n  PAPER option book — {r['asof']}  ·  open {r['n_open']}  ·  unrealized ₹{r['unrealized']}  ·  realized ₹{r['realized']}")
        for t in r["open"]:
            print(f"    #{t['id']} {t['symbol']} x{t['qty']:g} @ ₹{t['entry_prem']} → ₹{t['now_prem']}  "
                  f"{t['pnl_pct']:+}% (₹{t['pnl_abs']:g})  · {t['days_held']}d held, {t['sessions_to_target']} to target, {t['dte']} DTE"
                  + ("  ⏰ DUE" if t['due'] else ""))
        print(f"\n  {r['caveat']}")
