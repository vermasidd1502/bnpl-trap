"""
marleg_book_fib.py — for each LIVE position: where it sits on the fib map, how liquid it is,
and where the fib-logical stop-losses are.

Ties together the book (marleg_winners), the fib structure (marleg_fibmap), and live depth /
turnover (Groww quote) so a stop can be placed at a real structural level (just under the
nearest fib support) rather than a round number — sized to the name's liquidity.

  scan() -> per-position dict
  python marleg_book_fib.py
"""
import sys, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, yfinance as yf
import groww_client as gc
import marleg_winners as mw
import marleg_fibmap as fm


def _liquidity(g, sym, ltp):
    out = {"turnover_cr": None, "rvol": None, "spread_pct": None, "ob_imbalance": None, "tier": "?"}
    try:
        q = (g.quote(sym).json().get("payload") or {})
        bid, off = q.get("bid_price"), q.get("offer_price")
        if bid and off:
            out["spread_pct"] = round((off - bid) / ((off + bid) / 2) * 100, 3)
        tbq, tsq = q.get("total_buy_quantity"), q.get("total_sell_quantity")
        if tbq and tsq:
            out["ob_imbalance"] = round(tbq / tsq, 2)
        vol_today = q.get("volume")
    except Exception:
        vol_today = None
    try:
        d = yf.download(sym + ".NS", period="2mo", interval="1d", progress=False, auto_adjust=False)
        if isinstance(d.columns, pd.MultiIndex):
            d.columns = d.columns.get_level_values(0)
        avgvol = float(d["Volume"].tail(20).mean())
        out["turnover_cr"] = round(avgvol * ltp / 1e7, 1)                 # avg daily traded value, ₹ crore
        if vol_today:
            out["rvol"] = round(vol_today / avgvol, 2)                    # today vs 20d avg
    except Exception:
        pass
    t = out["turnover_cr"]
    out["tier"] = ("highly liquid" if t and t >= 100 else "liquid" if t and t >= 25 else
                   "moderate" if t and t >= 5 else "thin" if t is not None else "?")
    return out


def _fib_levels(fib):
    """Flatten every completed fib price into label->price (macro + micro + extensions + confluence)."""
    lv = {}
    mac = fib.get("macro") or {}
    for k, v in (mac.get("levels") or {}).items():
        lv[f"macro {k}"] = v
    if mac.get("hi"): lv["macro high (1.0)"] = mac["hi"]
    if mac.get("lo"): lv["macro low (0.0)"] = mac["lo"]
    mic = fib.get("micro") or {}
    for k, v in (mic.get("levels") or {}).items():
        lv[f"micro {k}"] = v
    for k, v in (mic.get("extensions") or {}).items():
        lv[f"micro ext {k}"] = v
    return {k: float(v) for k, v in lv.items() if v}


def scan():
    g = gc.GrowwClient(); g.token()
    book = mw.board(g, watch=False).get("book", [])
    rows = []
    for r in book:
        sym, ltp, avg = r["sym"], r["ltp"], r.get("avg")
        if not ltp:
            continue
        fib = fm.fibmap(sym)
        item = {"sym": sym, "name": r.get("name"), "ltp": ltp, "avg": avg, "qty": r["net_qty"],
                "unreal": r["unreal"], "error": fib.get("error")}
        if not fib.get("error"):
            lv = _fib_levels(fib)
            below = {k: v for k, v in lv.items() if v < ltp}
            above = {k: v for k, v in lv.items() if v > ltp}
            sup = max(below.items(), key=lambda kv: kv[1]) if below else None
            res = min(above.items(), key=lambda kv: kv[1]) if above else None
            sup2 = max([kv for kv in below.items() if kv[1] < (sup[1] if sup else ltp)], key=lambda kv: kv[1], default=None)
            mac = fib["macro"]
            rng = (mac["hi"] - mac["lo"]) or 1e-9
            item.update({
                "macro_range_pos": round((ltp - mac["lo"]) / rng * 100),
                "macro_hi": mac["hi"], "macro_lo": mac["lo"],
                "support": ({"label": sup[0], "price": round(sup[1], 1), "dist_pct": round((sup[1] / ltp - 1) * 100, 1)} if sup else None),
                "support2": ({"label": sup2[0], "price": round(sup2[1], 1), "dist_pct": round((sup2[1] / ltp - 1) * 100, 1)} if sup2 else None),
                "resistance": ({"label": res[0], "price": round(res[1], 1), "dist_pct": round((res[1] / ltp - 1) * 100, 1)} if res else None),
                "confluence": fib.get("confluence", [])[:3],
                "upside_eta": fib.get("upside_eta", [])[:3],
            })
        item["liquidity"] = _liquidity(g, sym, ltp)
        # fib-logical stop: just under nearest support; risk + relation to avg
        if item.get("support"):
            stop = round(item["support"]["price"] * 0.997, 1)            # a hair below the fib level
            item["fib_stop"] = {"price": stop, "risk_pct": round((ltp - stop) / ltp * 100, 1),
                                "vs_avg": ("protects profit" if (avg and stop > avg) else "below your avg — would book a loss")}
        rows.append(item)
    return {"book": rows}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = scan()
    for x in r["book"]:
        print(f"\n{'='*64}\n{x['sym']} ({x.get('name')})  LTP {x['ltp']}  avg {x.get('avg')}  ({x['unreal']:+,} unreal)")
        if x.get("error"):
            print("  fib:", x["error"])
        else:
            print(f"  FIB: {x['macro_range_pos']}% up the macro range ({x['macro_lo']:.0f} → {x['macro_hi']:.0f})")
            if x.get("resistance"): print(f"     resistance ↑ {x['resistance']['label']} @ {x['resistance']['price']} (+{x['resistance']['dist_pct']}%)")
            if x.get("support"):    print(f"     support    ↓ {x['support']['label']} @ {x['support']['price']} ({x['support']['dist_pct']}%)")
            if x.get("support2"):   print(f"     next sup   ↓ {x['support2']['label']} @ {x['support2']['price']} ({x['support2']['dist_pct']}%)")
            if x.get("confluence"): print("     confluence:", ", ".join(f"{c['price']} ({c['dist_from_live_pct']}%)" for c in x["confluence"]))
        lq = x["liquidity"]
        print(f"  LIQUIDITY: {lq['tier']} · ~₹{lq['turnover_cr']} cr/day · rvol {lq['rvol']}× · spread {lq['spread_pct']}% · book imbalance {lq['ob_imbalance']} (buy/sell)")
        if x.get("fib_stop"):
            print(f"  FIB STOP: {x['fib_stop']['price']} (risk {x['fib_stop']['risk_pct']}% from LTP) — {x['fib_stop']['vs_avg']}")


if __name__ == "__main__":
    main()
