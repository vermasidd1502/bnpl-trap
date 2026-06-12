"""
Marle-G — UNIVERSE BUILDER. Pull the full NSE equity universe from Groww's official
instrument master and write marleg_universe.json (the watchlist every scanner reads).

Groww's master tags govt-secs, SGBs and bonds as "EQ" too, so we filter to genuine
common stocks + ETFs:
  - exchange NSE, segment CASH, instrument_type EQ
  - drop sovereign gold bonds (SGB*), govt securities (start with a digit / *GS / *GB),
    and bond-series tickers (digits in the symbol)
  - keep pure-alphabetic tickers incl. & and - (RELIANCE, M&M, BAJAJ-AUTO) and ETFs

Industry mapping is a later step — this just maximizes coverage.
  python marleg_universe_build.py
"""
import sys, os, json, re, io, csv
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_universe.json")
MASTER = "https://growwapi-assets.groww.in/instruments/instrument.csv"
UA = {"User-Agent": "Mozilla/5.0"}


def is_equity(sym, name):
    s = (sym or "").upper().strip()
    if not s or "SGB" in s:                       # sovereign gold bonds
        return False
    if re.search(r"\d", s):                        # govt secs / bonds carry digits; equities don't
        return False
    if not re.fullmatch(r"[A-Z&\-]+", s):          # only clean alpha tickers (incl & and -)
        return False
    return True


def build():
    txt = requests.get(MASTER, headers=UA, timeout=120).text
    rd = csv.DictReader(io.StringIO(txt))
    seen, out = set(), []
    for r in rd:
        if r.get("exchange") != "NSE" or r.get("instrument_type") != "EQ" or r.get("segment") != "CASH":
            continue
        s = (r.get("trading_symbol") or "").upper().strip()
        if s in seen or not is_equity(s, r.get("name")):
            continue
        seen.add(s)
        out.append({"s": s, "n": (r.get("name") or s).strip()})
    out.sort(key=lambda d: d["s"])
    json.dump({"n": len(out), "source": "groww_instrument_master", "stocks": out},
              open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=0)
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    out = build()
    print(f"universe built: {len(out)} NSE equities -> {os.path.basename(OUT)}")
    print("sample:", [d["s"] for d in out[:12]])


if __name__ == "__main__":
    main()
