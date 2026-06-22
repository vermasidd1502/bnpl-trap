"""
marleg_instruments.py — Groww's AUTHORITATIVE instruments master (every tradeable contract), so we never
GUESS option expiries or symbol formats again. Captures all weeklies/monthlies/quarterlies for NSE *and*
BSE (NIFTY Tue weeklies, SENSEX Thu weeklies, etc.) with their exact trading_symbol, exchange and lot.

Downloaded once per day to instruments_cache.csv (a runtime cache — git-ignored), parsed lazily per
underlying and memoised. Read-only.
"""
import os
import csv
import datetime as dt
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
CACHE = os.path.join(HERE, "instruments_cache.csv")
_BY_UND = {}


def _ist_today():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).date()


def _ensure_cache():
    """Download the master if missing or older than today (IST). Falls back to a stale copy on failure."""
    try:
        if os.path.exists(CACHE) and os.path.getsize(CACHE) > 1_000_000:
            if dt.date.fromtimestamp(os.path.getmtime(CACHE)) >= _ist_today():
                return True
    except Exception:
        pass
    try:
        req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
        data = urllib.request.urlopen(req, timeout=90).read()
        if len(data) > 1_000_000:
            with open(CACHE, "wb") as f:
                f.write(data)
            _BY_UND.clear()
            return True
    except Exception:
        pass
    return os.path.exists(CACHE)


def _und_index(und):
    """Lazy per-underlying option index: {expiry: {strike: {'C'/'P': {symbol,exchange,lot}}}}."""
    und = (und or "").upper().strip()
    if und in _BY_UND:
        return _BY_UND[und]
    if not _ensure_cache():
        return {}
    idx = {}
    try:
        with open(CACHE, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("instrument_type") not in ("CE", "PE"):
                    continue
                if (r.get("underlying_symbol") or r.get("name") or "").upper() != und:
                    continue
                exp = r.get("expiry_date") or ""
                try:
                    strike = float(r.get("strike_price") or 0)
                except Exception:
                    continue
                kind = "C" if r["instrument_type"] == "CE" else "P"
                idx.setdefault(exp, {}).setdefault(strike, {})[kind] = {
                    "symbol": r.get("trading_symbol"), "exchange": r.get("exchange"), "lot": r.get("lot_size")}
    except Exception:
        return {}
    _BY_UND[und] = idx
    return idx


def has_options(und):
    return bool(_und_index(und))


def expiries(und, max_n=None, within_days=None):
    """Sorted future expiry dates (ISO strings) for the underlying's options."""
    idx = _und_index(und)
    today = _ist_today()
    out = []
    for e in idx:
        try:
            d = dt.date.fromisoformat(e)
        except Exception:
            continue
        if d < today:
            continue
        if within_days and (d - today).days > within_days:
            continue
        out.append(e)
    out.sort()
    return out[:max_n] if max_n else out


def strikes(und, expiry):
    return sorted((_und_index(und).get(expiry, {})).keys())


def contract(und, expiry, strike, kind):
    """Exact contract for the strike NEAREST the requested one: {symbol, exchange, strike, lot}."""
    leg = _und_index(und).get(expiry, {})
    if not leg:
        return None
    ks = sorted(leg.keys())
    if not ks:
        return None
    k = min(ks, key=lambda x: abs(x - strike))
    c = leg[k].get("C" if kind in ("C", "CE") else "P")
    if not c:
        return None
    return {"symbol": c["symbol"], "exchange": c["exchange"], "strike": k,
            "lot": (int(c["lot"]) if str(c.get("lot") or "").isdigit() else None)}


def exchange_of(und):
    idx = _und_index(und)
    for e in idx:
        for k in idx[e]:
            for kind in idx[e][k]:
                ex = idx[e][k][kind].get("exchange")
                if ex:
                    return ex
    return "NSE"


def lot_size(und):
    idx = _und_index(und)
    for e in sorted(idx):
        for k in idx[e]:
            for kind in idx[e][k]:
                lot = idx[e][k][kind].get("lot")
                if str(lot or "").isdigit():
                    return int(lot)
    return None


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    u = (sys.argv[1] if len(sys.argv) > 1 else "NIFTY").upper()
    exps = expiries(u, within_days=200)
    print(f"\n  {u}  ·  exchange {exchange_of(u)}  ·  lot {lot_size(u)}  ·  {len(expiries(u))} total expiries")
    print(f"  next ~200d: {exps}")
    if exps:
        ks = strikes(u, exps[0])
        print(f"  {exps[0]} strikes: {len(ks)}  (e.g. {ks[len(ks)//2-2:len(ks)//2+3]})")
        c = contract(u, exps[0], ks[len(ks) // 2] if ks else 0, "C")
        print(f"  sample contract: {c}")
