"""
Marle-G — STOP GUARDIAN. The night-watch for your real positions.

The problem it solves: IST market hours are the middle of the night in the US
(close 15:30 IST ~ 4-5am CT). Tight fixed stops get harvested by open/close vol
swings (TEJASNET's first hour runs ~2.6x midday vol; daily ATR ~6%) while you
sleep — the "stoploss trap".

What it does, every few minutes during IST market hours:
  1. Loads your REAL positions (live Groww read-only; falls back to my_positions.json).
  2. Computes a VOL-AWARE dynamic stop per position:
       chandelier = 20d-high - K*ATR(14)   (trails UP only — ratchets, never loosens)
       + FIRST-HOUR GUARD: 09:15-10:15 IST requires a deeper breach (extra ATR margin)
       + PERSISTENCE: breach must hold for N consecutive checks before it's "real"
  3. Alerts your phone via Slack ONLY when it matters:
       ⚠ APPROACH  — price within 0.25*ATR of the stop (once per day per name)
       🔴 EXIT     — confirmed persistent breach (the moment you'd actually act)
  4. After the close (and at startup if closed): writes the GTT PLAN — the exact
     broker-side stop triggers to place on Groww in a 5-minute evening ritual, so
     protection sits AT THE BROKER while you sleep and no machine needs to stay up.

NEVER places, modifies, or cancels an order. Monitor + alert + plan only.

  python marleg_stop_guardian.py            # the watch loop (leave running)
  python marleg_stop_guardian.py --once     # one check + GTT plan now
  python marleg_stop_guardian.py --k 2.0    # tighter chandelier
"""
import os, sys, json, time, argparse
from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
import marleg_slack as slack
from marleg_check_stops import load_positions, atr_price_hi

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
STATE_FILE = os.path.join(HERE, "marleg_guardian_state.json")
PLAN_FILE = os.path.join(HERE, "marleg_gtt_plan.json")

K_DEFAULT = 2.5            # chandelier distance in ATRs (>= 2 keeps you outside daily noise)
FIRST_HOUR_EXTRA = 0.35    # extra ATRs of breach required during 09:15-10:15 IST (2.6x vol window)
APPROACH_ATR = 0.25        # "approaching" warning band above the stop, in ATRs
CONFIRM_N = 2              # consecutive breached checks (~5 min apart) before EXIT alert
OPT_STOP_PCT = 30.0        # option positions: premium stop, % of entry
# --- ADAPTIVE DEFENSE (replay-validated): tighten k 2.5 -> 1.5 while the tape is hostile.
# ARMED only on names where it earns its keep (high-vol class) — on calm trends it clips
# winners (ADANIPORTS replay: 87% noise stops). Transient: evaluation-only tightening.
K_TIGHT = 1.5
DEF_ARM_ATRPCT = 3.0       # arm when daily ATR >= 3% of price...
DEF_ARM_RVR = 1.4          # ...or 5d/20d realized-vol ratio >= 1.4
DEF_RANGE_X = 1.5          # intraday spike: day range so far > 1.5*ATR
PROFIT_LOCK_ATR = 1.5      # in defense, if up >= 1.5*ATR -> stop floors at breakeven+0.1*ATR


def now_ist():
    return datetime.now(IST)


def phase(t=None):
    """closed | first_hour | open | last_hour  (IST session phases, holiday-aware)"""
    t = t or now_ist()
    try:
        import marleg_india_rules as ir
        if not ir.is_trading_day(t.date()):
            return "closed"
    except Exception:
        if t.weekday() >= 5:
            return "closed"
    hm = t.hour * 60 + t.minute
    if hm < 555 or hm > 930:          # before 09:15 or after 15:30
        return "closed"
    if hm <= 615:                      # 09:15-10:15
        return "first_hour"
    if hm >= 870:                      # 14:30-15:30
        return "last_hour"
    return "open"


def _load_state():
    try:
        return json.load(open(STATE_FILE))
    except Exception:
        return {"trail": {}, "breach_count": {}, "alerted": {}, "plan_day": None}


def _save_state(s):
    try:
        json.dump(s, open(STATE_FILE + ".tmp", "w"), indent=1)
        os.replace(STATE_FILE + ".tmp", STATE_FILE)
    except Exception:
        pass


def _live_px(unders):
    """Last 5-min close per underlying (delayed ~15min — fine for a stop watch)."""
    out = {}
    if not unders:
        return out
    syms = [u if (u.startswith("^") or "." in u) else u + ".NS" for u in unders]
    try:
        d = yf.download(syms, period="1d", interval="5m", group_by="ticker",
                        progress=False, threads=True)
        for u, s in zip(unders, syms):
            try:
                c = (d[s]["Close"] if len(syms) > 1 else d["Close"]).dropna()
                if len(c):
                    out[u] = float(c.iloc[-1])
            except Exception:
                pass
    except Exception:
        pass
    return out


def live_sl_coverage():
    """Read-only: map trading_symbol -> live protective SL order qty on Groww."""
    out = {}
    try:
        import groww_client as gc
        c = gc.GrowwClient(); c.token()
        ACTIVE = {"OPEN", "TRIGGER_PENDING", "PENDING", "NEW", "ACKED", "APPROVED", "MODIFIED"}
        for seg in ("CASH", "FNO"):
            try:
                resp = c._get("/v1/order/list", params={"segment": seg, "page": 0, "page_size": 100})
                rows = ((resp.json().get("payload", {}) or {}).get("order_list", []) or []) if resp.status_code == 200 else []
            except Exception:
                rows = []
            for o in rows:
                if "SL" not in str(o.get("order_type", "")).upper():
                    continue
                if str(o.get("order_status", "")).upper() not in ACTIVE:
                    continue
                s = o.get("trading_symbol")
                out[s] = out.get(s, 0) + (o.get("quantity") or 0)
    except Exception:
        pass
    return out


def protection_report(rows, state):
    """For every open position: is there a live SL order covering it? Loud-alert the naked."""
    cover = live_sl_coverage()
    naked, covered = [], []
    for r in rows:
        if not r or r.get("type") == "OPT":
            continue
        sym, qty = r["symbol"], (r.get("qty") or 0)
        have = cover.get(sym, 0)
        if have >= qty * 0.9:
            covered.append(sym)
        else:
            naked.append({"sym": sym, "qty": qty, "have": have,
                          "suggest_stop": r.get("eval_stop") or r.get("stop"),
                          "ltp": r.get("price"), "entry": r.get("entry"), "ptype": r.get("type", "EQ")})
    for n in naked:
        ltp, stop, short = n.get("ltp"), n.get("suggest_stop"), n["qty"] - n["have"]
        risk = f" · ~{round((ltp - stop) / ltp * 100, 1)}% away if it trips" if (ltp and stop) else ""
        pl = f" (entry {n['entry']}, {round((ltp / n['entry'] - 1) * 100, 1)}%)" if (ltp and n.get("entry")) else ""
        _alert_once(state, n["sym"], "naked",
            slack.build(
                f"⛔ *UNPROTECTED · {n['sym']}* — {short} share(s) with NO stop at the broker",
                lines=[f"Live ~{ltp}{pl}",
                       (f"Suggested stop *{stop}*{risk}" if stop else "No stop level computed yet"),
                       f"Covered {n['have']} of {n['qty']} → *{short} naked* · product {n['ptype']}",
                       "A naked leg can gap against you with nothing catching it overnight/intraday."],
                action=(f"Place an *SL-M SELL* order — qty {short}, trigger {stop} — on Groww now"
                        if stop else f"Set a protective stop on {n['sym']} now")),
            fields={"covered": str(n["have"]), "naked": str(short)})
    return {"naked": naked, "covered": covered, "cover_map": cover}


def _rv_ratio(und, state):
    """5d/20d realized-vol ratio (the vol-spike sensor). Cached once per day per name."""
    today = now_ist().strftime("%Y-%m-%d")
    cache = state.setdefault("rvr", {})
    c = cache.get(und)
    if c and c.get("date") == today:
        return c["v"]
    v = 1.0
    try:
        tk = und if (und.startswith("^") or "." in und) else und + ".NS"
        px = yf.Ticker(tk).history(period="3mo", interval="1d")["Close"].dropna()
        r = px.pct_change().dropna()
        s5, s20 = float(r.tail(5).std()), float(r.tail(20).std())
        if s20 > 0:
            v = s5 / s20
    except Exception:
        pass
    cache[und] = {"date": today, "v": round(float(v), 2)}
    return cache[und]["v"]


def assess(p, state, k, px_now):
    """Compute the dynamic stop + status for one equity/MTF position.
    ENTRY-ANCHORED chandelier (replay-validated): the trail hangs off the highest price
    seen SINCE the position/watch began (floored at entry) — never off pre-entry highs,
    which the replay showed force premature exits after market-wide spikes."""
    sym = p["symbol"]
    und = p.get("underlying") or sym
    atr, eod_price, _hi20 = atr_price_hi(und)
    if atr is None:
        return None
    price = px_now.get(und) or eod_price
    entry = p.get("entry")
    hiw = state.setdefault("hiwater", {})
    h = max(hiw.get(sym) or (entry or price), price)
    if entry:
        h = max(h, entry)
    hiw[sym] = round(h, 2)
    is_mis = p.get("type") == "MIS"
    # MIS: intraday units — 15m-ATR ≈ 0.185 x daily ATR, so 1.5xATR_i ≈ 0.28 x ATR_daily
    dist = (0.28 if is_mis else k) * atr
    base = h - dist                                          # chandelier off post-entry high
    prev = state["trail"].get(sym)
    stop = max(base, prev) if prev is not None else base     # RATCHET: never loosens
    state["trail"][sym] = round(stop, 2)
    ph = phase()
    # ---- adaptive defense (transient, evaluation-only) ----
    today = now_ist().strftime("%Y-%m-%d")
    rvr = _rv_ratio(und, state)
    atr_pct = atr / price * 100 if price else 0.0
    armed = atr_pct >= DEF_ARM_ATRPCT or rvr >= DEF_ARM_RVR
    dd = state.setdefault("day", {}).setdefault(sym, {})
    if dd.get("date") != today:
        dd.update({"date": today, "open": price, "hi": price, "lo": price, "px3": []})
    dd["hi"], dd["lo"] = max(dd["hi"], price), min(dd["lo"], price)
    dd["px3"] = (dd.get("px3", []) + [price])[-3:]
    p3 = dd["px3"]
    adverse = len(p3) == 3 and p3[0] > p3[1] > p3[2] and price < dd["open"]
    spike = rvr >= DEF_ARM_RVR or (dd["hi"] - dd["lo"]) > DEF_RANGE_X * atr
    defense = (not is_mis) and armed and (spike or adverse)   # MIS already runs tight units
    eval_stop = stop
    if defense:
        eval_stop = max(eval_stop, h - K_TIGHT * atr)
        entry = p.get("entry")
        if entry and h >= entry + PROFIT_LOCK_ATR * atr:
            eval_stop = max(eval_stop, entry + 0.1 * atr)    # never give a winner back
    # first-hour guard: require a deeper breach while the open is screaming
    trigger = eval_stop - (FIRST_HOUR_EXTRA * atr if ph == "first_hour" else 0.0)
    dist_pct = (price - eval_stop) / price * 100
    risk_rs = round((price - eval_stop) * (p.get("qty") or 0))
    status = "OK"
    if price <= trigger:
        n = state["breach_count"].get(sym, 0) + 1
        state["breach_count"][sym] = n
        status = "EXIT" if n >= CONFIRM_N else f"BREACH {n}/{CONFIRM_N}"
    else:
        state["breach_count"][sym] = 0
        if price <= eval_stop + APPROACH_ATR * atr:
            status = "APPROACH"
    if defense:
        status += " ·DEF"
    return {"symbol": sym, "type": p.get("type", "EQ"), "qty": p.get("qty"),
            "entry": p.get("entry"), "price": round(price, 2), "stop": round(stop, 2),
            "eval_stop": round(eval_stop, 2), "defense": defense,
            "def_why": ("spike" if spike else "") + ("+adverse" if adverse else "") if defense else "",
            "trigger_now": round(trigger, 2), "atr": round(atr, 2),
            "atr_pct": round(atr / price * 100, 1), "dist_pct": round(dist_pct, 1),
            "risk_rs": risk_rs, "status": status, "phase": ph, "mtf": p.get("type") == "MTF"}


def _alert_once(state, sym, kind, text, fields=None):
    """Slack alert deduped per symbol/kind/day."""
    day = now_ist().strftime("%Y-%m-%d")
    key = f"{sym}:{kind}:{day}"
    if state["alerted"].get(key):
        return
    state["alerted"][key] = True
    slack.notify(text, fields)


def gtt_plan(rows, src):
    """The 5-minute evening ritual: broker-side GTT stop triggers to place on Groww."""
    plan = []
    for r in rows:
        if r is None or r.get("stop") is None:
            continue
        if r.get("type") == "OPT":
            short = r.get("side") == "short"
            # SHORT option: stop-loss = BUY-back when premium RISES (trigger above LTP);
            # LONG option: SELL when premium falls. Limit cushioned past the trigger.
            plan.append({"symbol": r["symbol"], "type": "OPT", "qty": r["qty"],
                         "action": "BUY (buy-back stop)" if short else "SELL",
                         "gtt_trigger": r["stop"],
                         "gtt_limit": round(r["stop"] * (1.03 if short else 0.97), 1),
                         "now": r["price"], "dist_pct": r.get("dist_pct"), "atr_pct": None,
                         "note": ("SHORT premium stop — buy back if premium rises to trigger"
                                  if short else "LONG premium stop — sell if premium falls to trigger")})
            continue
        if r.get("status") == "EXIT" or (r.get("price") is not None and r["price"] <= r["stop"]):
            plan.append({"symbol": r["symbol"], "type": r["type"], "qty": r["qty"],
                         "action": "NO GTT — already through stop",
                         "gtt_trigger": None, "gtt_limit": None, "now": r["price"],
                         "dist_pct": r.get("dist_pct"), "atr_pct": r.get("atr_pct"),
                         "note": "exit signal already fired; placing a sell-GTT below LTP "
                                 "triggers instantly — selling now is a manual decision"})
            continue
        trig = r["stop"]
        plan.append({"symbol": r["symbol"], "type": r["type"], "qty": r["qty"],
                     "action": "SELL",
                     "gtt_trigger": round(trig, 1),
                     "gtt_limit": round(trig * 0.997, 1),     # small buffer so the limit fills
                     "now": r["price"], "dist_pct": r["dist_pct"], "atr_pct": r["atr_pct"],
                     "note": ("MTF — leverage doubles the rupee pain; size so risk fits budget"
                              if r["mtf"] else "delivery")})
    out = {"asof": now_ist().strftime("%Y-%m-%d %H:%M IST"), "source": src, "k_atr": K_DEFAULT,
           "ritual": "Place each as a GTT SELL (trigger/limit) on Groww before sleeping. "
                     "GTT sits at the BROKER — no machine needs to stay awake. Adjust qty only.",
           "plan": plan}
    json.dump(out, open(PLAN_FILE, "w"), indent=1)
    if plan:
        slack.card(
            "🌙 *GTT EVENING PLAN* — set these broker-side stops, then sleep protected",
            lines=[f"{p['symbol']}: trigger {p['gtt_trigger']} / limit {p['gtt_limit']} (now {p['now']}, {p['dist_pct']}% room)" for p in plan],
            action="Place each as a *GTT (Good-Till-Triggered) SL-M SELL* on Groww — they persist overnight so a gap can't catch you unhedged",
            fields={"source": src, "method": f"{K_DEFAULT}×ATR chandelier"})
    return out


def cycle(state, k):
    positions, src = load_positions()
    eq = [p for p in positions if p.get("type") in ("EQ", "MTF", "MIS") and (p.get("qty") or 0) > 0]
    opt = [p for p in positions if p.get("type") == "OPT" and p.get("entry")]
    px = _live_px(list({p.get("underlying") or p["symbol"] for p in eq}))
    rows = []
    for p in eq:
        r = assess(p, state, k, px)
        if r is None:                                   # delisted/renamed underlying — surface it
            rows.append({"symbol": p["symbol"], "type": p.get("type", "EQ"), "qty": p.get("qty"),
                         "entry": p.get("entry"), "price": None, "stop": None, "trigger_now": None,
                         "atr": None, "atr_pct": None, "dist_pct": None, "risk_rs": None,
                         "status": "NO DATA — dead/renamed symbol? fix my_positions.json",
                         "phase": phase(), "mtf": p.get("type") == "MTF"})
            continue
        rows.append(r)
        if r["status"] == "EXIT":
            pl = f" ({round((r['price'] / r['entry'] - 1) * 100, 1)}% vs entry {r['entry']})" if r.get("entry") else ""
            _alert_once(state, r["symbol"], "exit",
                slack.build(
                    f"🔴 *EXIT SIGNAL · {r['symbol']}* — confirmed below the dynamic stop",
                    lines=[f"Price {r['price']} ≤ stop {r['stop']}, held {CONFIRM_N} checks{pl}",
                           f"Qty {r['qty']} · {r['type']} · open risk now ₹{r['risk_rs']} · ATR {r['atr_pct']}%",
                           f"The trailing trend-stop has broken ({r['phase']} session)."],
                    action=f"Exit or trim {r['symbol']} now, or knowingly move the stop — don't let it run"),
                fields={"qty": str(r["qty"]), "risk": f"₹{r['risk_rs']}", "type": r["type"]})
        elif r["status"] == "APPROACH":
            _alert_once(state, r["symbol"], "near",
                slack.build(
                    f"⚠ *APPROACHING STOP · {r['symbol']}* — {r['dist_pct']}% above {r['stop']}",
                    lines=[f"Price {r['price']} · stop {r['stop']} · ATR {r['atr_pct']}% · qty {r['qty']} ({r['type']})",
                           f"Entry {r.get('entry')} · {r['phase']} session"],
                    action="Watch closely — tighten the stop or be ready to act if it breaks"),
                fields={"phase": r["phase"]})
    for p in opt:
        side = p.get("side", "long")
        stop = p["entry"] * (1 - OPT_STOP_PCT / 100) if side == "long" else p["entry"] * (1 + OPT_STOP_PCT / 100)
        ltp = p.get("ltp")
        hit = ltp is not None and ((side == "long" and ltp <= stop) or (side == "short" and ltp >= stop))
        prem_ret = round((ltp / p["entry"] - 1) * 100, 1) if (ltp and p.get("entry")) else None
        status = ("EXIT — premium stop hit" if hit
                  else ("OK" if ltp is not None else "watch premium (no live LTP)"))
        if hit:
            _alert_once(state, p["symbol"], "optexit",
                slack.build(
                    f"🔴 *OPTION STOP HIT · {p['symbol']}* ({side.upper()})",
                    lines=[f"LTP {ltp} vs premium stop {round(stop,1)} ({prem_ret:+}% on premium)",
                           f"Entry {p.get('entry')} · qty {p.get('qty')}"],
                    action=f"Close the {p['symbol']} option — the premium stop tripped"),
                fields={"qty": str(p.get("qty")), "entry": str(p.get("entry"))})
        rows.append({"symbol": p["symbol"], "type": "OPT", "qty": p.get("qty"), "entry": p.get("entry"),
                     "price": ltp, "stop": round(stop, 1), "trigger_now": round(stop, 1),
                     "atr": None, "atr_pct": None, "dist_pct": prem_ret, "risk_rs": None,
                     "status": status, "phase": phase(), "mtf": False, "side": side})
    state["last_rows"] = [r for r in rows if r]
    state["last_src"] = src
    state["last_check"] = now_ist().strftime("%Y-%m-%d %H:%M IST")
    # protection-gap detection (the missing piece): scream if a position has no live stop
    if "LIVE" in src:
        try:
            state["coverage"] = protection_report([r for r in rows if r], state)
        except Exception as e:
            print("coverage check error:", str(e)[:80], flush=True)
    _save_state(state)
    return rows, src


def print_rows(rows, src):
    print(f"\nSTOP GUARDIAN  |  {now_ist().strftime('%Y-%m-%d %H:%M IST')}  |  source: {src}  |  phase: {phase()}")
    print(f"{'SYMBOL':<16}{'type':<5}{'qty':>6}{'now':>9}{'STOP':>9}{'dist%':>7}{'ATR%':>6}{'risk Rs':>9}  status")
    print("-" * 84)
    for r in rows:
        if not r:
            continue
        print(f"{r['symbol']:<16}{r['type']:<5}{(r['qty'] or 0):>6}{(r['price'] or 0):>9}"
              f"{r['stop']:>9}{(r['dist_pct'] if r['dist_pct'] is not None else '—'):>7}"
              f"{(r['atr_pct'] if r['atr_pct'] is not None else '—'):>6}"
              f"{(r['risk_rs'] if r['risk_rs'] is not None else '—'):>9}  {r['status']}")
    if "LIVE" in src:
        cov = protection_report([r for r in rows if r], _load_state())
        print("\n--- PROTECTION CHECK (live SL orders at broker) ---")
        if cov["naked"]:
            for n in cov["naked"]:
                print(f"  ⛔ UNPROTECTED  {n['sym']:<14} {n['qty']:>5} — place SL-M SELL trigger {n['suggest_stop']}")
        else:
            print("  ✓ all positions have a live stop order")
        if cov["covered"]:
            print("  protected:", ", ".join(cov["covered"]))


def run(k):
    print("STOP GUARDIAN running — watch loop (IST market hours), GTT plan after close. "
          "MONITOR ONLY: never places orders.", flush=True)
    state = _load_state()
    while True:
        try:
            ph = phase()
            today = now_ist().strftime("%Y-%m-%d")
            if ph == "closed":
                if state.get("plan_day") != today and now_ist().hour >= 15:
                    rows, src = cycle(state, k)
                    gtt_plan(rows, src)
                    state["plan_day"] = today
                    _save_state(state)
                    print(f"[{now_ist().strftime('%H:%M IST')}] GTT plan written", flush=True)
                time.sleep(900)                       # closed: check every 15 min for next open
            else:
                rows, src = cycle(state, k)
                exits = [r["symbol"] for r in rows if r and r["status"] == "EXIT"]
                if exits:
                    print(f"[{now_ist().strftime('%H:%M IST')}] EXIT: {', '.join(exits)}", flush=True)
                time.sleep(180 if ph in ("first_hour", "last_hour") else 300)
        except KeyboardInterrupt:
            return
        except Exception as e:
            print("guardian cycle error:", str(e)[:120], flush=True)
            time.sleep(300)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=float, default=K_DEFAULT)
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()
    if a.once:
        state = _load_state()
        rows, src = cycle(state, a.k)
        print_rows(rows, src)
        plan = gtt_plan(rows, src)
        print(f"\nGTT plan -> {os.path.basename(PLAN_FILE)} ({len(plan['plan'])} positions). "
              f"Slack: {'ON' if slack.enabled() else 'OFF (set MARLEG_SLACK_WEBHOOK)'}")
        return
    run(a.k)


if __name__ == "__main__":
    main()
