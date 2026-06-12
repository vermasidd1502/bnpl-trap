"""
marleg_diag.py — Marle-G system self-diagnosis ("the harness").

One call, diagnose(), runs every health check the pod depends on and returns a
structured report: per-check status (ok / warn / fail), a human detail line, and
the exact fix command for anything red. The /api/diag endpoint passes in live
handles (the Groww getter, the in-memory cache, a Flask test-client) so the checks
reflect the *running* process, not a cold import.

Market-hours aware: feeds/recorder that are only live during NSE hours are reported
as "idle (market closed)" warnings, never hard failures, when the market is shut.
"""
import os
import json
import time
import datetime
import importlib
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))

# Modules every page leans on — import-health is a fast "is the codebase loadable" check.
CORE_MODULES = [
    "groww_client", "marleg_var", "marleg_winners", "marleg_weekend", "marleg_intraday",
    "marleg_overextension", "marleg_fundamentals", "marleg_factor", "marleg_vwap",
    "marleg_cascade", "marleg_projection", "marleg_regime", "marleg_slack",
]

# Detached helpers that should be running, with the command to relaunch them.
SCHED_TASKS = [
    ("MarleG-MorningPick", "opening-range momentum picker (09:24 IST)"),
    ("MarleG-TickRecorder", "Groww live-quote tick store (market hours)"),
    ("MarleG-EOD", "end-of-day universe + cache refresh"),
    ("MarleG-Guardian", "stop / GTT / naked-position watchdog"),
]

# Representative routes to exercise via the in-process test client (cheap, local-ish).
PROBE_ROUTES = [
    ("/api/symbols?q=REL&limit=3", "symbol search / routing"),
    ("/api/winners", "winners board (book + live quotes)"),
    ("/api/portfolio_var", "VaR / risk engine"),
]


# ----------------------------------------------------------------- helpers
def _ist_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


def _ct_now():
    # US Central — CDT (summer) is UTC-5; close enough for a status footer.
    return datetime.datetime.utcnow() - datetime.timedelta(hours=5)


def _market_open():
    n = _ist_now()
    if n.weekday() >= 5:
        return False
    hm = n.hour * 60 + n.minute
    return 555 <= hm <= 930          # 09:15 .. 15:30 IST


def _age(path):
    p = path if os.path.isabs(path) else os.path.join(HERE, path)
    if not os.path.exists(p):
        return None
    return time.time() - os.path.getmtime(p)


def _fmt_age(s):
    if s is None:
        return "missing"
    if s < 90:
        return f"{int(s)}s ago"
    if s < 5400:
        return f"{int(s / 60)}m ago"
    if s < 172800:
        return f"{int(s / 3600)}h ago"
    return f"{int(s / 86400)}d ago"


def _sched_query(name):
    """Windows: query a single scheduled task. Returns dict or None if absent/non-Windows."""
    try:
        out = subprocess.run(
            ["schtasks", "/query", "/tn", name, "/fo", "list", "/v"],
            capture_output=True, text=True, timeout=8,
        )
        if out.returncode != 0:
            return None
        nxt = status = last = None
        for ln in out.stdout.splitlines():
            k = ln.split(":", 1)
            if len(k) != 2:
                continue
            key, val = k[0].strip().lower(), k[1].strip()
            if key == "next run time":
                nxt = val
            elif key == "status":
                status = val
            elif key == "last run time":
                last = val
        return {"next": nxt, "status": status, "last": last}
    except Exception:
        return None


# ----------------------------------------------------------------- the report
def diagnose(groww_getter=None, cache=None, self_test=None):
    t0 = time.time()
    checks = []
    mkt = _market_open()

    def add(name, status, detail, fix="", cat="core"):
        checks.append({"name": name, "status": status, "detail": detail,
                       "fix": fix, "cat": cat})

    # ---- processes -------------------------------------------------------
    add("Flask server (:8777)", "ok", "responding to this request", "", "process")

    # tick recorder — judged by store freshness, market-aware
    tick_age = _age("marleg_tick_store.json")
    if tick_age is None:
        add("Tick recorder", "warn" if not mkt else "fail",
            "no tick store on disk",
            "Start-Process python -ArgumentList 'marleg_tick_recorder.py' -WindowStyle Hidden", "process")
    elif mkt and tick_age > 150:
        add("Tick recorder", "fail",
            f"stale during market hours — last poll {_fmt_age(tick_age)}",
            "Start-Process python -ArgumentList 'marleg_tick_recorder.py' -WindowStyle Hidden", "process")
    elif not mkt:
        add("Tick recorder", "ok",
            f"idle (market closed) — last poll {_fmt_age(tick_age)}", "", "process")
    else:
        add("Tick recorder", "ok", f"polling — last {_fmt_age(tick_age)}", "", "process")

    # stop guardian — judged by its state file freshness
    g_age = _age("marleg_guardian_state.json")
    if g_age is None:
        add("Stop guardian", "warn",
            "no guardian state on disk",
            "Start-Process python -ArgumentList 'marleg_stop_guardian.py' -WindowStyle Hidden", "process")
    elif mkt and g_age > 1800:
        add("Stop guardian", "warn",
            f"no check in {_fmt_age(g_age)} during market hours",
            "Start-Process python -ArgumentList 'marleg_stop_guardian.py' -WindowStyle Hidden", "process")
    else:
        add("Stop guardian", "ok", f"last check {_fmt_age(g_age)}", "", "process")

    # ---- external data feeds --------------------------------------------
    # Groww auth + live quote
    if groww_getter is None:
        add("Groww auth", "warn", "no groww handle passed to diag", "", "feed")
    else:
        try:
            g = groww_getter()
            if g is None:
                add("Groww auth", "fail",
                    "client unavailable (creds / TOTP)",
                    "re-run TOTP auth; check C:\\Users\\siddh\\groww-secrets", "feed")
            else:
                price = None
                try:
                    q = g.quote("RELIANCE")
                    price = (q or {}).get("last_price") or (q or {}).get("price")
                except Exception:
                    try:
                        qt = g.quote_table(["RELIANCE"])
                        price = (qt.get("RELIANCE", {}) or {}).get("price")
                    except Exception:
                        price = None
                if price:
                    add("Groww auth + live data", "ok",
                        f"RELIANCE ltp ₹{price}", "", "feed")
                elif mkt:
                    add("Groww live data", "warn",
                        "authenticated but no live quote during market hours",
                        "approval lapses daily — client self-renews via TOTP; re-check secrets", "feed")
                else:
                    add("Groww auth", "ok",
                        "authenticated (no live quote — market closed)", "", "feed")
        except Exception as e:
            add("Groww auth", "fail", str(e)[:120],
                "re-run TOTP auth; check C:\\Users\\siddh\\groww-secrets", "feed")

    # yfinance reachability (light, time-boxed)
    try:
        import yfinance as yf
        h = yf.Ticker("RELIANCE.NS").history(period="1d", interval="1d")
        if h is not None and len(h):
            add("yfinance feed", "ok", f"RELIANCE.NS last close ₹{round(float(h['Close'].iloc[-1]), 1)}", "", "feed")
        else:
            add("yfinance feed", "warn", "reachable but returned no rows", "retry; Yahoo can rate-limit", "feed")
    except Exception as e:
        add("yfinance feed", "fail", str(e)[:120], "check network / Yahoo availability", "feed")

    add("NSE market", "ok" if mkt else "warn",
        "OPEN" if mkt else "closed (NSE 09:15–15:30 IST)", "", "feed")

    # ---- data freshness --------------------------------------------------
    for fn, label in [
        ("my_positions.json", "positions snapshot"),
        ("marleg_stop_check.json", "stop-check book"),
        ("marleg_market.duckdb", "OHLCV datastore"),
    ]:
        a = _age(fn)
        if a is None:
            add(f"Data: {label}", "warn", "missing", "", "data")
        elif a > 7 * 86400:
            add(f"Data: {label}", "warn", f"{_fmt_age(a)} (stale)",
                "refresh: python marleg_eod.py" if "datastore" in label else "", "data")
        else:
            add(f"Data: {label}", "ok", _fmt_age(a), "", "data")

    # ---- in-memory cache snapshot ---------------------------------------
    if cache is not None:
        try:
            now = time.time()
            keys = list(cache.keys())
            fresh = sum(1 for k in keys if now - cache[k][0] < 600)
            add("Response cache", "ok",
                f"{len(keys)} keys warm, {fresh} fresh (<10m)", "", "data")
        except Exception:
            add("Response cache", "ok", "active", "", "data")

    # ---- module import health -------------------------------------------
    broken = []
    for m in CORE_MODULES:
        try:
            importlib.import_module(m)
        except Exception as e:
            broken.append(f"{m}: {str(e)[:60]}")
    if broken:
        add("Core modules", "fail",
            f"{len(broken)}/{len(CORE_MODULES)} failed to import: " + "; ".join(broken[:3]),
            "fix the import error above; check missing pip deps", "modules")
    else:
        add("Core modules", "ok", f"all {len(CORE_MODULES)} import clean", "", "modules")

    # ---- live route probes (in-process test client) ---------------------
    if self_test is not None:
        for route, label in PROBE_ROUTES:
            try:
                r = self_test(route)
                code = r.get("code")
                snip = (r.get("snippet") or "").strip()
                bad = (not r.get("ok")) or ('"error"' in snip) or snip.startswith("Traceback")
                if bad:
                    add(f"Route: {label}", "fail",
                        f"{route} → HTTP {code} {snip[:80]}",
                        "see server log (*.err) for the traceback", "route")
                else:
                    add(f"Route: {label}", "ok", f"{route} → 200", "", "route")
            except Exception as e:
                add(f"Route: {label}", "fail", f"{route} → {str(e)[:80]}",
                    "see server log (*.err)", "route")

    # ---- scheduled tasks (Windows) --------------------------------------
    any_sched = False
    for name, desc in SCHED_TASKS:
        info = _sched_query(name)
        if info is None:
            add(f"Task: {name}", "warn", f"not registered — {desc}",
                f'schtasks /create /tn {name} ...  (see scheduler setup)', "sched")
        else:
            any_sched = True
            st = (info.get("status") or "").lower()
            nxt = info.get("next") or "?"
            if "could not" in st or st == "":
                add(f"Task: {name}", "ok", f"registered · next {nxt}", "", "sched")
            else:
                add(f"Task: {name}", "ok", f"{info.get('status')} · next {nxt}", "", "sched")
    if not any_sched:
        # schtasks unavailable (non-Windows) — collapse the noise into one note
        checks[:] = [c for c in checks if c["cat"] != "sched"]
        add("Scheduled tasks", "warn",
            "schtasks unavailable or none registered", "", "sched")

    # ---- summary ---------------------------------------------------------
    summ = {"ok": 0, "warn": 0, "fail": 0}
    for c in checks:
        summ[c["status"]] = summ.get(c["status"], 0) + 1
    overall = "fail" if summ["fail"] else ("warn" if summ["warn"] else "ok")

    return {
        "overall": overall,
        "summary": summ,
        "checks": checks,
        "market_open": mkt,
        "ist": _ist_now().strftime("%a %d %b %H:%M IST"),
        "ct": _ct_now().strftime("%H:%M CT"),
        "elapsed_ms": int((time.time() - t0) * 1000),
    }


if __name__ == "__main__":
    import pprint
    pprint.pprint(diagnose())
