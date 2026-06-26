"""
marleg_userlists.py — multiple NAMED user watchlists, persisted, with held positions auto-included.

The store the notification engine + dashboard read from. Pure CRUD over marleg_userlists.json:
  { "lists": { "Core": ["RELIANCE","LT"], "Infra-cascade": [...], "Themes": [...] } }

(Distinct from marleg_watchlist.py, which is the AUTO-screener that generates triggered/watch/avoid tiers.)

Held Groww positions are merged in on read (effective_symbols / get_all) so nothing you own falls through —
but they're never written into a named list (they come and go with your book). Read-only w.r.t. the broker.
"""
import os
import json
import time
import datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
STORE = os.path.join(HERE, "marleg_userlists.json")
_HELD_CACHE = {"t": 0, "syms": []}


def _ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")


def _load():
    try:
        d = json.load(open(STORE, encoding="utf-8"))
        if isinstance(d, dict) and isinstance(d.get("lists"), dict):
            return d
    except Exception:
        pass
    return {"lists": {"Core": ["RELIANCE", "LT", "TEJASNET"], "Infra-cascade": [], "Themes": []}}


def _save(d):
    json.dump(d, open(STORE, "w", encoding="utf-8"), indent=2)
    return d


def _held_symbols(refresh=False):
    """Best-effort held equity + option underlyings from the live Groww book. Cached 5 min, never throws.
    refresh=False → return the cache instantly (never blocks a UI call); refresh=True → repull if stale."""
    fresh = (time.time() - _HELD_CACHE["t"]) < 300
    if not refresh or fresh:
        return _HELD_CACHE["syms"]
    syms = set()
    try:
        import marleg_opt_position as op
        for p in (op.book().get("positions") or []):
            u = p.get("underlying") or p.get("symbol")
            if u:
                syms.add(str(u).upper())
    except Exception:
        pass
    try:
        import groww_client
        g = groww_client.GrowwClient()
        for h in (g.holdings() or []):
            ts = (h.get("trading_symbol") or h.get("symbol") or "").upper()
            if ts:
                syms.add(ts)
    except Exception:
        pass
    out = sorted(syms)
    _HELD_CACHE.update({"t": time.time(), "syms": out})
    return out


def get_all(include_positions=True):
    d = _load()
    return {"lists": d["lists"], "positions": _held_symbols() if include_positions else [], "asof": _ist()}


def effective_symbols():
    """Union of every named list + held positions — what the notify engine watches."""
    d = _load()
    s = set()
    for lst in d["lists"].values():
        s.update(lst)
    s.update(_held_symbols(refresh=True))     # the scan path warms the held cache for the fast UI path
    return sorted(s)


def add(sym, list_name="Core"):
    sym = (sym or "").upper().strip()
    if not sym:
        return {"ok": False, "error": "empty symbol"}
    d = _load()
    d["lists"].setdefault(list_name, [])
    if sym not in d["lists"][list_name]:
        d["lists"][list_name].append(sym)
    _save(d)
    return {"ok": True, "list": list_name, "symbols": d["lists"][list_name]}


def remove(sym, list_name="Core"):
    sym = (sym or "").upper().strip()
    d = _load()
    if list_name in d["lists"] and sym in d["lists"][list_name]:
        d["lists"][list_name].remove(sym)
        _save(d)
    return {"ok": True, "list": list_name, "symbols": d["lists"].get(list_name, [])}


def create_list(name):
    name = (name or "").strip()
    if not name:
        return {"ok": False, "error": "empty list name"}
    d = _load()
    d["lists"].setdefault(name, [])
    _save(d)
    return {"ok": True, "lists": list(d["lists"].keys())}


def delete_list(name):
    d = _load()
    d["lists"].pop(name, None)
    if not d["lists"]:
        d["lists"]["Core"] = []
    _save(d)
    return {"ok": True, "lists": list(d["lists"].keys())}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    a = get_all()
    print("LISTS:")
    for k, v in a["lists"].items():
        print(f"  {k}: {', '.join(v) or '(empty)'}")
    print("HELD (auto):", ", ".join(a["positions"]) or "(none/unreachable here)")
    print("EFFECTIVE watched:", ", ".join(effective_symbols()))
