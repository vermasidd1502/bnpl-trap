"""
Marle-G PAPER engine — MTF swing strategy (validated long signal), multi-profile.

Simulates the strategy forward with realistic MTF mechanics (leverage cap + daily
interest). Four risk profiles run as independent paper books on the same signal:
  conservative / balanced / aggressive / adaptive (adaptive sizes to the regime).
Runs anywhere (yfinance EOD data). Step once per trading day after the close.

  python marleg_paper_mtf.py --all --reset --capital 100000   # start all 4 fresh
  python marleg_paper_mtf.py --all                             # step all 4 a day
  python marleg_paper_mtf.py --profile adaptive               # step just one
"""
import os, json, argparse
import yfinance as yf
import marleg_momentum_buy as mb

HERE = os.path.dirname(os.path.abspath(__file__))
RR, MAX_HOLD, DAILY_INT, MTF_LEV = 2.0, 10, 0.0004, 4.0

PROFILES = {
    "conservative": dict(risk=0.5, maxpos=3, deploy=90,  k=2.5),
    "balanced":     dict(risk=1.0, maxpos=5, deploy=180, k=2.0),
    "aggressive":   dict(risk=2.0, maxpos=8, deploy=350, k=2.0),
}


def regime():
    """NIFTY vs 50-DMA + India VIX -> (label, params) for the adaptive profile."""
    try:
        d = yf.download("^NSEI ^INDIAVIX", period="6mo", interval="1d", group_by="ticker", progress=False, threads=True)
        ns = d["^NSEI"]["Close"].dropna(); vix = float(d["^INDIAVIX"]["Close"].dropna().iloc[-1])
        px = float(ns.iloc[-1]); sma = float(ns.rolling(50).mean().iloc[-1])
        up = px > sma
        if up and vix < 18:
            return "RISK-ON", dict(risk=1.5, maxpos=6, deploy=250, k=2.0)
        if up:
            return "NEUTRAL", dict(risk=1.0, maxpos=5, deploy=180, k=2.0)
        return "CAUTIOUS (below trend)", dict(risk=0.5, maxpos=2, deploy=60, k=2.5)
    except Exception:
        return "unknown", dict(risk=0.75, maxpos=4, deploy=120, k=2.2)


def book_path(name): return os.path.join(HERE, f"marleg_paper_{name}.json")


def cur_prices(syms):
    if not syms:
        return {}
    d = yf.download([s + ".NS" for s in syms], period="5d", interval="1d", group_by="ticker", progress=False, threads=True)
    out = {}
    for s in syms:
        try:
            c = (d[s + ".NS"]["Close"] if len(syms) > 1 else d["Close"]).dropna()
            out[s] = float(c.iloc[-1])
        except Exception:
            pass
    return out


def step_book(name, cfg, capital, cands, asof, px, reg_label=None):
    path = book_path(name)
    try:
        book = json.load(open(path))
    except Exception:
        book = {"profile": name, "cash": capital, "start": capital, "positions": [], "closed": [], "steps": 0, "asof": None}
    new_day = asof != book.get("asof")
    book["steps"] += 1
    book["cfg"] = cfg
    if reg_label:
        book["regime"] = reg_label
    cand_by = {c["s"]: c for c in cands}
    actions = []
    # MTF interest on borrowed (cash<0)
    interest = max(0.0, -book["cash"]) * DAILY_INT
    if interest:
        book["cash"] -= interest; actions.append(f"interest -Rs{interest:,.0f}")
    # manage positions
    still = []
    for p in book["positions"]:
        p["held"] = p.get("held", 0) + 1
        price = px.get(p["sym"])
        if price is None:
            still.append(p); continue
        c = cand_by.get(p["sym"])
        if c:
            p["stop"] = max(p["stop"], round(price - cfg["k"] * c["atr"], 1))
        ex = ("STOP", p["stop"]) if price <= p["stop"] else ("TARGET", p["target"]) if price >= p["target"] \
            else ("TIME", price) if p["held"] >= MAX_HOLD else None
        if ex:
            reason, xp = ex
            book["cash"] += p["qty"] * xp
            book["closed"].append({**p, "exit": xp, "exit_reason": reason, "pnl": round(p["qty"] * (xp - p["entry"]))})
            actions.append(f"EXIT {p['sym']} {reason} @ {xp}")
        else:
            still.append(p)
    book["positions"] = still

    def equity():
        return book["cash"] + sum(p["qty"] * px.get(p["sym"], p["entry"]) for p in book["positions"])

    if new_day:
        for c in cands:
            if len(book["positions"]) >= cfg["maxpos"]:
                break
            if c["s"] in [p["sym"] for p in book["positions"]]:
                continue
            entry = c["entry"]; stop = round(entry - cfg["k"] * c["atr"], 1)
            if stop >= entry:
                continue
            eq = equity(); qty = int((eq * cfg["risk"] / 100.0) // (entry - stop))
            if qty <= 0:
                continue
            dep = sum(p["qty"] * px.get(p["sym"], p["entry"]) for p in book["positions"]) + qty * entry
            if dep > eq * cfg["deploy"] / 100.0 or dep > eq * MTF_LEV:
                continue
            book["cash"] -= qty * entry
            book["positions"].append({"sym": c["s"], "qty": qty, "entry": entry, "stop": stop,
                                      "target": round(entry + RR * (entry - stop), 1), "tag": c["tag"], "held": 0, "entry_date": asof})
            actions.append(f"BUY {qty} {c['s']} @ {entry}")
    book["asof"] = asof
    book["equity"] = round(equity())
    book["last_actions"] = actions
    json.dump(book, open(path, "w"), indent=1, default=str)
    return book, actions


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

    print("screening (validated MTF long signal)...")
    cands, asof = mb.screen()
    reg_label, reg_cfg = regime()
    print(f"asof {asof} | {len(cands)} candidates | regime: {reg_label}\n")
    syms = set(c["s"] for c in cands)
    for n in names:
        try:
            syms |= set(p["sym"] for p in json.load(open(book_path(n)))["positions"])
        except Exception:
            pass
    px = cur_prices(list(syms))

    for n in names:
        cfg = dict(reg_cfg) if n == "adaptive" else dict(PROFILES[n])
        book, acts = step_book(n, cfg, a.capital, cands, asof, px, reg_label if n == "adaptive" else None)
        ret = (book["equity"] / book["start"] - 1) * 100
        realized = sum(x["pnl"] for x in book["closed"])
        extra = f" | regime={reg_label}" if n == "adaptive" else ""
        print(f"[{n:<12}] equity Rs{book['equity']:>9,}  ({ret:+5.2f}%)  open {len(book['positions'])}  "
              f"closed {len(book['closed'])}  realized Rs{realized:>7,}  risk {cfg['risk']}%/max {cfg['maxpos']}{extra}")
        for x in acts:
            print(f"     - {x}")
    print("\nPaper books saved. View live status in the pod: /marle_g_paper.html")


if __name__ == "__main__":
    main()
