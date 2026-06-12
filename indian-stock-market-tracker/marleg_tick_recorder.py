"""
marleg_tick_recorder.py — zero-lag intraday bars.

Groww's historical candle-range endpoint is a full session behind, and yfinance intraday can
hiccup. So this polls Groww's LIVE quote() (~every 20s) for the book + gated leaders + recently
viewed tickers, and accumulates real 1-minute OHLCV bars into a local store. The intraday pod
reads these for a true zero-lag tail (it still falls back to yfinance for earlier-today history).

Run detached during market hours (the launcher does this); it self-throttles off-hours.

  python marleg_tick_recorder.py            # run the loop
  python marleg_tick_recorder.py --once     # one poll (debug)
"""
import os, sys, json, time
from datetime import datetime, timezone, timedelta
import groww_client as gc

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
STORE = os.path.join(HERE, "marleg_tick_store.json")
FOCUS = os.path.join(HERE, "marleg_tick_focus.json")
POLL = 20
KEEP_DAYS = 2
MAX_WATCH = 16


# ---------- focus (tickers the user is viewing) ----------
def note_focus(tk):
    """Called by the pod when a ticker is viewed, so the recorder starts capturing it."""
    tk = (tk or "").upper()
    if not tk:
        return
    try:
        d = json.load(open(FOCUS, encoding="utf-8"))
    except Exception:
        d = {}
    d[tk] = int(time.time())
    d = dict(sorted(d.items(), key=lambda kv: -kv[1])[:12])      # keep 12 most-recent
    try:
        tmp = FOCUS + ".tmp"; json.dump(d, open(tmp, "w")); os.replace(tmp, STORE if False else FOCUS)
    except Exception:
        pass


def _focus_syms(max_age=7200):
    try:
        d = json.load(open(FOCUS, encoding="utf-8")); now = int(time.time())
        return [k for k, t in d.items() if now - t < max_age]
    except Exception:
        return []


def _watch(g):
    syms = set(_focus_syms())
    try:
        for h in (g.holdings_data() or {}).get("holdings") or []:
            if (h.get("quantity") or 0):
                syms.add((h.get("trading_symbol") or "").upper())
        for p in (g.positions_data() or {}).get("positions") or []:
            syms.add((p.get("trading_symbol") or "").upper())
    except Exception:
        pass
    try:
        gd = json.load(open(os.path.join(HERE, "marleg_gated_cache.json"), encoding="utf-8"))
        for p in gd.get("picks", [])[:8]:
            syms.add(p["s"])
    except Exception:
        pass
    syms.discard("")
    return sorted(syms)[:MAX_WATCH]


# ---------- store ----------
def _load():
    try:
        return json.load(open(STORE, encoding="utf-8"))
    except Exception:
        return {}


def _save(d):
    try:
        tmp = STORE + ".tmp"; json.dump(d, open(tmp, "w")); os.replace(tmp, STORE)
    except Exception:
        pass


def load_bars(tk):
    """Reader for the pod: list of [minute_epoch, o, h, l, c, v] for today (+ recent)."""
    return (_load().get(tk.upper(), {}) or {}).get("bars", [])


def _quote(g, sym):
    try:
        r = g.quote(sym)
        p = (r.json().get("payload") if r.status_code == 200 else None) or {}
        price = p.get("last_price")
        vol = p.get("volume")
        return (float(price) if price else None, float(vol) if vol else 0.0)
    except Exception:
        return None, None


def poll_once(g, last_vol):
    watch = _watch(g)
    if not watch:
        return 0
    store = _load()
    now = int(time.time()); minute = now - now % 60
    cutoff = now - KEEP_DAYS * 86400
    n = 0
    for sym in watch:
        price, dayvol = _quote(g, sym)
        if price is None:
            continue
        n += 1
        prev = last_vol.get(sym)
        dvol = max(0.0, dayvol - prev) if (prev is not None and dayvol) else 0.0
        last_vol[sym] = dayvol if dayvol else last_vol.get(sym)
        sd = store.setdefault(sym, {"bars": []})
        bars = sd["bars"]
        if bars and bars[-1][0] == minute:
            b = bars[-1]
            b[2] = max(b[2], price); b[3] = min(b[3], price); b[4] = price; b[5] += dvol
        else:
            bars.append([minute, price, price, price, price, dvol])
        sd["bars"] = [b for b in bars if b[0] >= cutoff]
    store["_meta"] = {"asof": now, "watch": watch}
    _save(store)
    return n


def _market_open():
    t = datetime.now(IST)
    if t.weekday() >= 5:
        return False
    hm = t.hour * 60 + t.minute
    return 555 <= hm <= 935                                       # 09:15–15:35 IST


def run():
    g = gc.GrowwClient(); g.token()
    last_vol = {}; ran = False
    print(f"[tick-recorder] started {datetime.now(IST):%Y-%m-%d %H:%M IST} · store {STORE}", flush=True)
    while True:
        try:
            if _market_open():
                n = poll_once(g, last_vol); ran = True
                print(f"[{datetime.now(IST):%H:%M:%S} IST] polled {n} symbols", flush=True)
                time.sleep(POLL)
            elif ran:
                print("[tick-recorder] session ended — exiting", flush=True); break
            else:
                t = datetime.now(IST)
                if t.weekday() >= 5 or (t.hour * 60 + t.minute) > 935:
                    print("[tick-recorder] no session today — exiting", flush=True); break
                time.sleep(120)                                  # weekday pre-open: wait for the bell
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("[tick-recorder] error:", str(e)[:120], flush=True)
            time.sleep(POLL)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if "--once" in sys.argv:
        g = gc.GrowwClient(); g.token()
        print("polled", poll_once(g, {}), "symbols ->", STORE)
        return
    run()


if __name__ == "__main__":
    main()
