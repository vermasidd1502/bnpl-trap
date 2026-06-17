"""
marleg_autopilot.py — the CONVICTION CONSOLIDATOR + autonomous paper-trader.

The problem you flagged: signals scatter across ~10 pods (gated, reversal, cup-handle, horizon, movers)
and get overlooked. This pulls the BEST of them into ONE ranked feed, PAPER-TRADES the high-conviction
ones on its own (entry/target/stop taken from each signal, live-tracked via Groww LTP), and keeps a
TRACK RECORD — so you can see WHICH ideas work, WHY (thesis + conviction), and mirror the ones you like
in your real account while you trade alongside it.

How it decides (transparent, not a black box):
  • candidates = today's signals from the validated engines:
      - cup-with-handle CONFIRMED   (conv 68-75; backtested 62-67% win)
      - reversal-to-long PRIME      (conv = the radar's own score; bulls retaking in an uptrend leader)
      - gated U/D leader, news-clean (conv 65; the volume-pod core)
  • TAKE a paper trade when conviction >= 65, news-clean, and the name isn't already open / on cooldown.
  • EXIT (book it) when live price hits the target (WIN) or stop (LOSS), or the hold horizon expires (TIME).
  • size = ~12% of equity per position (paper; START Rs 1,00,000).

PAPER ONLY — marks a local cash book, never sends an order. Decision-support, not advice.

  python marleg_autopilot.py            # advance one step (open/mark/close) + print the book
"""
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
# exclude ETFs / index funds / InvITs / REITs — the auto-trader takes equities only
_FUND_RE = re.compile(r"(ETF|IETF|BEES|LOWVOL|INVIT|REIT|MOMENTUM|GILT|LIQUID|GSEC|SDL|NIFTY|SENSEX|MIDCAP|SMALLCAP|NEXT50|PSUBANK|BANKBEES)|((VALUE|GOLD|SILVER)$)", re.I)
IST = timezone(timedelta(hours=5, minutes=30))
BOOK = os.path.join(HERE, "marleg_autopilot_book.json")
START = 100000.0
ALLOC = 0.12            # paper capital fraction per position
MIN_CONV = 65           # take only high-conviction signals
MAX_OPEN = 8
COOLDOWN_D = 4          # don't re-open a name within N days of closing it
HOLD_D = {"SWING": 20, "SHORT": 8, "LONG-TERM": 60}   # time-stop by horizon


def _load_json(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def _book():
    try:
        return json.load(open(BOOK, encoding="utf-8"))
    except Exception:
        return {"start": START, "positions": [], "closed": [], "log": []}


def _save(b):
    try:
        tmp = BOOK + ".tmp"; json.dump(b, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        os.replace(tmp, BOOK)
    except Exception:
        pass


def _today():
    return datetime.now(IST).strftime("%Y-%m-%d")


def signals():
    """Consolidate today's high-conviction signals from the engines → one ranked, deduped feed."""
    cand = {}                                   # sym -> signal dict (highest conviction kept, sources merged)

    def add(s, conv, thesis, entry, target, stop, horizon, source, clean=True):
        if not s or _FUND_RE.search(s):                 # equities only — no ETFs/funds/InvITs
            return
        if not entry or not target or not stop or target <= entry or stop >= entry:
            return
        cur = cand.get(s)
        rec = {"s": s, "conv": int(conv), "thesis": thesis, "entry": round(float(entry), 1),
               "target": round(float(target), 1), "stop": round(float(stop), 1),
               "horizon": horizon, "sources": [source], "clean": clean,
               "payout_pct": round((target / entry - 1) * 100, 1), "risk_pct": round((1 - stop / entry) * 100, 1)}
        if cur:
            cur["sources"] = sorted(set(cur["sources"] + [source]))
            if conv > cur["conv"]:
                cur.update({k: rec[k] for k in ("conv", "thesis", "entry", "target", "stop", "horizon", "payout_pct", "risk_pct")})
            cur["sources"] = sorted(set(cur["sources"]))
        else:
            cand[s] = rec

    # 1) cup-with-handle CONFIRMED
    cup = _load_json("marleg_cuphandle.json")
    for x in cup.get("setups", []):
        st = str(x.get("stage", ""))
        if st.startswith("CONFIRMED") and x.get("pivot") and x.get("target"):
            conv = 75 if st == "CONFIRMED+" else 68
            stop = x.get("stop") or round(x["pivot"] * 0.96, 1)
            add(x["s"], conv, f"confirmed cup-handle ({st}) — {x.get('bt_win', 62)}% backtested",
                x["pivot"], x["target"], stop, "SWING", "cup-handle")

    # 2) reversal-to-long PRIME / uptrend
    rev = _load_json("marleg_reversal.json")
    for x in rev.get("signals", []):
        if x.get("tag") == "PRIME re-entry" and x.get("price") and x.get("stop"):
            tgt = round(x["price"] * (1 + max(0.06, x.get("net", 1) / 100 * 4)), 1)
            add(x["s"], x.get("conv", 70), f"PRIME reversal — {'/'.join(x.get('signals', []))[:30]} (bulls retaking, leader)",
                x["price"], tgt, x["stop"], "SHORT", "reversal", clean=(x.get("clean") is not False))

    # 3) gated U/D leader (news-clean only)
    g = _load_json("marleg_gated_cache.json")
    for p in g.get("picks", []):
        if p.get("clean") is not False and p.get("entry") and p.get("target") and p.get("s"):
            try:
                entry = float(str(p["entry"]).replace("₹", "").split("-")[0])
            except Exception:
                entry = None
            if entry:
                add(p["s"], 66, "gated U/D leader (volume-pod core), news-clean",
                    entry, p["target"], round(entry * 0.97, 1), "SWING", "gated")

    rows = sorted(cand.values(), key=lambda x: -x["conv"])
    return rows


def step():
    """One autonomous cycle: open fresh high-conviction signals, mark live, book target/stop/time exits."""
    import marleg_live
    b = _book(); today = _today()
    open_syms = {p["s"] for p in b["positions"]}
    recent = {c["s"]: c.get("exit_day") for c in b["closed"][-40:]}
    sigs = signals()
    sig_by = {s["s"]: s for s in sigs}

    # OPEN new
    equity_est = b.get("equity", b["start"])
    for sg in sigs:
        if len(b["positions"]) >= MAX_OPEN:
            break
        s = sg["s"]
        if s in open_syms or sg["conv"] < MIN_CONV or not sg["clean"]:
            continue
        rd = recent.get(s)
        if rd:
            try:
                if (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(rd, "%Y-%m-%d")).days < COOLDOWN_D:
                    continue
            except Exception:
                pass
        qty = int((equity_est * ALLOC) // sg["entry"])
        if qty < 1:
            continue
        b["positions"].append({"s": s, "qty": qty, "entry": sg["entry"], "target": sg["target"], "stop": sg["stop"],
                               "conv": sg["conv"], "thesis": sg["thesis"], "sources": sg["sources"],
                               "horizon": sg["horizon"], "opened": today, "now": sg["entry"], "upnl": 0.0, "status": "OPEN"})
        open_syms.add(s)
        b["log"] = (b.get("log", []) + [f"{today} OPEN {qty} {s} @ {sg['entry']} (conv {sg['conv']} · {sg['thesis'][:40]})"])[-80:]

    # MARK + EXIT
    upnl = 0.0
    for pos in list(b["positions"]):
        lp = marleg_live.price(pos["s"])
        px = lp.get("price") if lp.get("ok") else pos["now"]
        pos["now"] = round(px, 1)
        held = 0
        try:
            held = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(pos["opened"], "%Y-%m-%d")).days
        except Exception:
            pass
        exit_reason = None
        if px >= pos["target"]:
            exit_reason = "TARGET"
        elif px <= pos["stop"]:
            exit_reason = "STOP"
        elif held >= HOLD_D.get(pos["horizon"], 20):
            exit_reason = "TIME"
        if exit_reason:
            pnl = (px - pos["entry"]) * pos["qty"]
            b["closed"].append({**pos, "exit": round(px, 1), "pnl": round(pnl, 1), "reason": exit_reason,
                                "exit_day": today, "ret_pct": round((px / pos["entry"] - 1) * 100, 1)})
            b["log"] = (b.get("log", []) + [f"{today} CLOSE {pos['s']} @ {round(px,1)} · {exit_reason} · P&L {round(pnl,1)}"])[-80:]
            b["positions"].remove(pos)
        else:
            pos["upnl"] = round((px - pos["entry"]) * pos["qty"], 1)
            pos["status"] = "▲ in profit" if px > pos["entry"] else "▼ underwater"
            upnl += pos["upnl"]

    realized = round(sum(c["pnl"] for c in b["closed"]), 1)
    b["realized"] = realized
    b["equity"] = round(b["start"] + realized + upnl, 1)
    b["ret_pct"] = round((b["equity"] / b["start"] - 1) * 100, 2)
    wins = [c for c in b["closed"] if c["pnl"] > 0]
    b["win_rate"] = round(len(wins) / len(b["closed"]) * 100, 0) if b["closed"] else None
    b["n_open"], b["n_closed"] = len(b["positions"]), len(b["closed"])
    b["asof"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
    # per-source track record
    per = {}
    for c in b["closed"]:
        for src in c.get("sources", ["?"]):
            d = per.setdefault(src, {"n": 0, "wins": 0, "pnl": 0.0})
            d["n"] += 1; d["wins"] += 1 if c["pnl"] > 0 else 0; d["pnl"] += c["pnl"]
    b["by_source"] = {k: {"n": v["n"], "win": round(v["wins"] / v["n"] * 100, 0), "pnl": round(v["pnl"], 1)} for k, v in per.items()}
    _save(b)
    return b


def view():
    """Read the book + attach the fresh QUEUE (high-conviction signals not yet taken)."""
    b = step()
    open_syms = {p["s"] for p in b["positions"]}
    queue = [s for s in signals() if s["s"] not in open_syms and s["conv"] >= MIN_CONV and s["clean"]][:12]
    return {"ok": True, "asof": b.get("asof"), "equity": b.get("equity"), "ret_pct": b.get("ret_pct"),
            "realized": b.get("realized"), "win_rate": b.get("win_rate"), "n_open": b.get("n_open"),
            "n_closed": b.get("n_closed"), "start": b.get("start"),
            "positions": sorted(b["positions"], key=lambda x: -x["conv"]),
            "closed": list(reversed(b["closed"]))[:15], "queue": queue, "by_source": b.get("by_source", {}),
            "log": list(reversed(b.get("log", [])))[:12],
            "note": "Paper auto-trader over the validated engines' high-conviction signals. It books a real "
                    "track record so you can trust (or distrust) the suggestions before mirroring them. "
                    "Paper only — never sends an order. Not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    v = view()
    print(f"\nAUTO-PILOT (paper) — {v['asof']}")
    print(f"  equity ₹{v['equity']:,} ({v['ret_pct']:+}%) · realized ₹{v['realized']:,} · win {v['win_rate']}% · open {v['n_open']} · closed {v['n_closed']}")
    print(f"\n  OPEN ({v['n_open']}):")
    for p in v["positions"]:
        print(f"   {p['s']:<11} {p['qty']}@₹{p['entry']} now ₹{p['now']} P&L ₹{p['upnl']:>8} conv {p['conv']} [{'+'.join(p['sources'])}] — {p['thesis'][:46]}")
    print(f"\n  QUEUE — high-conviction, not yet taken ({len(v['queue'])}):")
    for s in v["queue"][:10]:
        print(f"   {s['s']:<11} conv {s['conv']} {s['horizon']:<6} entry ₹{s['entry']} → ₹{s['target']} (+{s['payout_pct']}%) stop ₹{s['stop']} [{'+'.join(s['sources'])}]")
    if v["by_source"]:
        print("\n  TRACK RECORD by engine:")
        for k, d in v["by_source"].items():
            print(f"   {k:<12} {d['n']} trades · {d['win']}% win · ₹{d['pnl']:,}")
