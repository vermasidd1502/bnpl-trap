"""
Marle-G PAPER engine #3 — OPTIONS (defined-risk debit call spreads).

On a gated-LONG name (the confluence screen the user's gating validated), it opens a
~1-month ATM debit CALL SPREAD: long ATM call + short ~1-sigma OTM call. That is the
"cheaper OTM, less risk, leveraged upside" idea done RIGHT — a spread caps the premium
bleed and the cost, vs a naked far-OTM lottery ticket. Priced + marked via Black-Scholes
(marleg_vol); IV proxied by the name's realized vol. Exits on target / stop / expiry.

Multi-profile sizing (conservative/balanced/aggressive/adaptive) by % capital at risk
(max loss = net debit). Paper-notional: 1 "contract" = 1 share-equivalent.

  python marleg_paper_options.py --all --reset --capital 100000   # start fresh
  python marleg_paper_options.py --all                            # one step
Books: marleg_options_<profile>.json
"""
import os, json, argparse, urllib.request, datetime as dt
import marleg_vol as mv

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.environ.get("MARLEG_BASE", "http://127.0.0.1:8777")
TGT, STOPL = 0.80, -0.55          # take 80% of max spread value; cut at -55% of debit

PROFILES = {
    "conservative": dict(risk=0.5, maxpos=2),
    "balanced":     dict(risk=1.0, maxpos=4),
    "aggressive":   dict(risk=2.0, maxpos=6),
}


def _strike_step(s):
    return 10 if s < 1000 else 20 if s < 2500 else 50 if s < 6000 else 100


def next_month_expiry(today):
    y, m = today.year, today.month + 1
    if m > 12:
        y, m = y + 1, 1
    return mv.monthly_expiry(y, m)


def build_spread(sym, spot, today):
    sigma = mv.realized_vol(sym, 30) or (mv.india_vix() or 15) / 100.0 or 0.30
    exp = next_month_expiry(today); T = max((exp - today).days, 1) / 365.0
    step = _strike_step(spot)
    k_long = round(spot / step) * step
    em = spot * sigma * (T ** 0.5)
    k_short = k_long + max(step, round(em / step) * step)
    long_p = mv.bs_price(spot, k_long, T, mv.R_FREE, sigma, "C")
    short_p = mv.bs_price(spot, k_short, T, mv.R_FREE, sigma, "C")
    debit = long_p - short_p
    return {"k_long": k_long, "k_short": k_short, "expiry": exp.isoformat(), "sigma": round(sigma, 3),
            "debit": round(debit, 2), "max_val": round(k_short - k_long, 2)}


def value_spread(sp, spot, today):
    exp = dt.date.fromisoformat(sp["expiry"]); T = max((exp - today).days, 0) / 365.0
    lp = mv.bs_price(spot, sp["k_long"], T, mv.R_FREE, sp["sigma"], "C")
    spp = mv.bs_price(spot, sp["k_short"], T, mv.R_FREE, sp["sigma"], "C")
    return lp - spp


def gated_longs():
    try:
        with urllib.request.urlopen(BASE + "/api/gated", timeout=25) as r:
            return [(p.get("s") or "").upper() for p in (json.loads(r.read().decode()).get("picks") or []) if p.get("s")]
    except Exception:
        return []


def spot_of(syms):
    out = {}
    try:
        import urllib.parse
        with urllib.request.urlopen(BASE + "/api/quote?symbols=" + urllib.parse.quote(",".join(syms)), timeout=30) as r:
            d = json.loads(r.read().decode())
        for s in syms:
            if isinstance(d.get(s), dict) and d[s].get("price"):
                out[s] = d[s]["price"]
    except Exception:
        pass
    return out


def book_path(name): return os.path.join(HERE, f"marleg_options_{name}.json")


def step_book(name, cfg, capital, cands, spots, today, reg=None):
    path = book_path(name)
    try:
        book = json.load(open(path))
    except Exception:
        book = {"profile": name, "cash": capital, "start": capital, "positions": [], "closed": [], "steps": 0}
    book["steps"] += 1; book["cfg"] = cfg
    if reg:
        book["regime"] = reg
    acts = []
    still = []
    for p in book["positions"]:
        spot = spots.get(p["sym"]);
        if spot is None:
            still.append(p); continue
        val = value_spread(p["spread"], spot, today)
        p["now"] = round(val, 2); pnl_frac = (val - p["spread"]["debit"]) / p["spread"]["debit"] if p["spread"]["debit"] else 0
        exp = dt.date.fromisoformat(p["spread"]["expiry"])
        ex = ("TARGET", val) if val >= TGT * p["spread"]["max_val"] else \
             ("STOP", val) if pnl_frac <= STOPL else \
             ("EXPIRY", max(0.0, min(spot, p["spread"]["k_short"]) - p["spread"]["k_long"])) if today >= exp else None
        if ex:
            reason, xv = ex
            book["cash"] += p["contracts"] * xv
            book["closed"].append({**p, "exit": round(xv, 2), "reason": reason,
                                   "pnl": round(p["contracts"] * (xv - p["spread"]["debit"]))})
            acts.append(f"EXIT {p['sym']} {reason} val {round(xv,2)}")
        else:
            still.append(p)
    book["positions"] = still

    def equity():
        return book["cash"] + sum(p["contracts"] * (value_spread(p["spread"], spots.get(p["sym"]), today)
                                  if spots.get(p["sym"]) else p["spread"]["debit"]) for p in book["positions"])

    held = {p["sym"] for p in book["positions"]}
    for s in cands:
        if len(book["positions"]) >= cfg["maxpos"]:
            break
        if s in held or s not in spots:
            continue
        sp = build_spread(s, spots[s], today)
        if sp["debit"] <= 0.05:
            continue
        eq = equity(); contracts = int((eq * cfg["risk"] / 100.0) // sp["debit"])
        if contracts <= 0 or book["cash"] < contracts * sp["debit"]:
            continue
        book["cash"] -= contracts * sp["debit"]
        book["positions"].append({"sym": s, "contracts": contracts, "spread": sp, "now": sp["debit"],
                                  "entry_spot": spots[s], "t": today.isoformat()})
        acts.append(f"OPEN {s} {sp['k_long']:.0f}/{sp['k_short']:.0f}C x{contracts} debit {sp['debit']}")
    book["equity"] = round(equity()); book["last_actions"] = acts; book["asof"] = today.isoformat()
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
    today = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).date()
    cands = gated_longs()
    syms = set(cands)
    for n in names:
        try:
            syms |= {p["sym"] for p in json.load(open(book_path(n)))["positions"]}
        except Exception:
            pass
    spots = spot_of(sorted(syms)) if syms else {}
    vix = mv.india_vix() or 15
    reg = "RISK-ON" if vix < 16 else "CAUTIOUS" if vix > 20 else "NEUTRAL"
    reg_cfg = dict(risk=1.2, maxpos=5) if vix < 16 else dict(risk=0.5, maxpos=2) if vix > 20 else dict(risk=0.8, maxpos=4)
    print(f"OPTIONS step | {len(cands)} gated longs | India VIX {vix:.1f} | regime {reg}\n")
    for n in names:
        cfg = dict(reg_cfg) if n == "adaptive" else dict(PROFILES[n])
        book, acts = step_book(n, cfg, a.capital, cands, spots, today, reg if n == "adaptive" else None)
        ret = (book["equity"] / book["start"] - 1) * 100
        realized = sum(x["pnl"] for x in book["closed"])
        print(f"[{n:<12}] equity Rs{book['equity']:>9,} ({ret:+5.2f}%)  open {len(book['positions'])}  "
              f"closed {len(book['closed'])}  realized Rs{realized:>7,}")
        for x in acts[:6]:
            print(f"     - {x}")
    print("\nStep daily (debit spreads are a ~1-month hold). Books: marleg_options_<profile>.json")


if __name__ == "__main__":
    main()
