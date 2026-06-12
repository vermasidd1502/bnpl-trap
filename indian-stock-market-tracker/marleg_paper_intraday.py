"""
Marle-G PAPER engine #2 — INTRADAY (live, signal-driven).

Rides the live monitor's BUYING signals (price up + pace-adjusted real volume = the
user's "go along with the move"), enters paper longs with a TIGHT intraday stop, takes
2:1, and force-flats at the close. Steps live (call it every few minutes while the
session is open); it marks open positions against Groww real-time price each step.

Multi-profile, same shape as the MTF engine:
  conservative / balanced / aggressive / adaptive.

  python marleg_paper_intraday.py --all --reset --capital 100000   # start fresh
  python marleg_paper_intraday.py --all                            # one live step
Books: marleg_intraday_<profile>.json   |   view alongside /marle_g_paper.html
"""
import os, json, argparse, urllib.request, urllib.parse, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.environ.get("MARLEG_BASE", "http://127.0.0.1:8777")
STOP_PCT, RR = 0.012, 2.0                 # 1.2% intraday stop (structural cap), 2:1 target
DEFAULT_UNIVERSE = ["RELIANCE", "HDFCBANK", "ICICIBANK", "TCS", "INFY", "SBIN", "TMPV", "ITC",
                    "BHARTIARTL", "LT", "JSWENERGY", "ADANIENT", "ADANIENSOL", "MARUTI", "AXISBANK",
                    "TATASTEEL", "HINDALCO", "SUNPHARMA", "TITAN", "BAJFINANCE"]

PROFILES = {
    "conservative": dict(risk=0.3, maxpos=2),
    "balanced":     dict(risk=0.6, maxpos=4),
    "aggressive":   dict(risk=1.2, maxpos=6),
}


def live(syms):
    out = {}
    for i in range(0, len(syms), 20):
        chunk = syms[i:i + 20]
        try:
            with urllib.request.urlopen(BASE + "/api/live?syms=" + urllib.parse.quote(",".join(chunk)), timeout=45) as r:
                d = json.loads(r.read().decode())
            for s in chunk:
                if isinstance(d.get(s), dict) and d[s].get("price") is not None:
                    out[s] = d[s]
            out["_src"] = d.get("_src")
        except Exception:
            pass
    return out


def regime():
    """Adaptive sizing off NIFTY vs VIX via /api/macro (fallback neutral)."""
    try:
        with urllib.request.urlopen(BASE + "/api/macro", timeout=20) as r:
            m = json.loads(r.read().decode())
        vix = (m.get("vix") or {}).get("value") or m.get("india_vix") or 15
        return ("RISK-ON", dict(risk=0.9, maxpos=5)) if vix < 16 else \
               ("CAUTIOUS", dict(risk=0.4, maxpos=2)) if vix > 20 else \
               ("NEUTRAL", dict(risk=0.6, maxpos=4))
    except Exception:
        return "unknown", dict(risk=0.5, maxpos=3)


def book_path(name): return os.path.join(HERE, f"marleg_intraday_{name}.json")


def _now_ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).replace(tzinfo=None)


def step_book(name, cfg, capital, q, reg_label=None):
    path = book_path(name)
    try:
        book = json.load(open(path))
    except Exception:
        book = {"profile": name, "cash": capital, "start": capital, "positions": [], "closed": [], "steps": 0}
    book["steps"] += 1; book["cfg"] = cfg
    if reg_label:
        book["regime"] = reg_label
    ist = _now_ist(); eod = ist.hour > 15 or (ist.hour == 15 and ist.minute >= 20)
    acts = []
    # mark + exit
    still = []
    for p in book["positions"]:
        d = q.get(p["sym"]); price = d["price"] if d else p.get("now", p["entry"])
        p["now"] = price
        ex = ("STOP", p["stop"]) if price <= p["stop"] else ("TARGET", p["target"]) if price >= p["target"] \
            else ("EOD", price) if eod else None
        if ex:
            reason, xp = ex
            book["cash"] += p["qty"] * xp
            book["closed"].append({**p, "exit": xp, "reason": reason, "pnl": round(p["qty"] * (xp - p["entry"]))})
            acts.append(f"EXIT {p['sym']} {reason} @ {xp}")
        else:
            still.append(p)
    book["positions"] = still

    def equity():
        return book["cash"] + sum(p["qty"] * q.get(p["sym"], {}).get("price", p["entry"]) for p in book["positions"])

    # enter on fresh BUYING (only intraday, not after EOD)
    if not eod:
        held = {p["sym"] for p in book["positions"]}
        buys = sorted([(s, d) for s, d in q.items() if isinstance(d, dict) and d.get("tag") == "BUYING" and s not in held],
                      key=lambda x: -(abs(x[1]["chg"]) * (x[1].get("volr") or 1)))
        for s, d in buys:
            if len(book["positions"]) >= cfg["maxpos"]:
                break
            entry = d["price"]; stop = round(entry * (1 - STOP_PCT), 1)
            eq = equity(); qty = int((eq * cfg["risk"] / 100.0) // (entry - stop))
            if qty <= 0 or book["cash"] < qty * entry:
                continue
            book["cash"] -= qty * entry
            book["positions"].append({"sym": s, "qty": qty, "entry": entry, "now": entry, "stop": stop,
                                      "target": round(entry + RR * (entry - stop), 1),
                                      "tag": "BUYING", "volr": d.get("volr"), "t": str(ist)[:16]})
            acts.append(f"BUY {qty} {s} @ {entry} (vol {d.get('volr')}x)")
    book["equity"] = round(equity()); book["last_actions"] = acts; book["asof"] = str(ist)[:16]
    book["eod"] = eod
    json.dump(book, open(path, "w"), indent=1, default=str)
    return book, acts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=100000)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--profile", choices=list(PROFILES) + ["adaptive"])
    ap.add_argument("--reset", action="store_true")
    a = ap.parse_args()
    names = list(PROFILES) + ["adaptive"] if a.all else ([a.profile] if a.profile else ["adaptive"])
    if a.reset:
        for n in names:
            if os.path.exists(book_path(n)):
                os.remove(book_path(n))
    # universe = defaults + gated buying names
    uni = set(DEFAULT_UNIVERSE)
    try:
        with urllib.request.urlopen(BASE + "/api/gated", timeout=20) as r:
            for p in (json.loads(r.read().decode()).get("picks") or []):
                uni.add((p.get("s") or "").upper())
    except Exception:
        pass
    for n in names:
        try:
            uni |= {p["sym"] for p in json.load(open(book_path(n)))["positions"]}
        except Exception:
            pass
    q = live(sorted(x for x in uni if x))
    reg_label, reg_cfg = regime()
    nbuy = sum(1 for d in q.values() if isinstance(d, dict) and d.get("tag") == "BUYING")
    print(f"INTRADAY live step | src {q.get('_src')} | {nbuy} BUYING signals | regime {reg_label}\n")
    for n in names:
        cfg = dict(reg_cfg) if n == "adaptive" else dict(PROFILES[n])
        book, acts = step_book(n, cfg, a.capital, q, reg_label if n == "adaptive" else None)
        ret = (book["equity"] / book["start"] - 1) * 100
        realized = sum(x["pnl"] for x in book["closed"])
        print(f"[{n:<12}] equity Rs{book['equity']:>9,} ({ret:+5.2f}%)  open {len(book['positions'])}  "
              f"closed {len(book['closed'])}  realized Rs{realized:>7,}{'  [EOD-flat]' if book['eod'] else ''}")
        for x in acts[:6]:
            print(f"     - {x}")
    print("\nStep this every few minutes while the session is open. Books: marleg_intraday_<profile>.json")


if __name__ == "__main__":
    main()
