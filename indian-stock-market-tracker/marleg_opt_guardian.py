"""
marleg_opt_guardian.py — the EXIT mechanism that would've saved the +15k→−15k round-trip.

For every held option it tracks the premium's running PEAK (the "all-time high of the option price" since the
pod first saw it), and ratchets a TRAILING STOP up under it. When the premium falls `trail%` off the peak →
EXIT signal. It also pulls the level-based TAKE-PROFIT (sell where the underlying hits resistance) and the
re-entry hint (ADD/BUY where the underlying sits on support) from marleg_option_levels.

Output = a verdict (EXIT NOW / TRIM @ resistance / HOLD) + ready GTT exit SCRIPTS to arm on Groww.

SAFETY: read-only. The pod NEVER places, modifies, or cancels orders. It tracks the peak, computes the
trigger, and hands YOU the GTT ticket — you arm it (and re-arm the trail upward as the premium rises).
Honest limit: it can only track the peak from when it STARTS watching; it can't recover a peak it never saw.
"""
import os
import json
import datetime as dt

import marleg_opt_position as op
import marleg_option_levels as ol

_DIR = os.path.dirname(os.path.abspath(__file__))
_PEAKS = os.path.join(_DIR, "marleg_opt_peaks.json")
TRAIL_DEFAULT = 25.0          # exit when premium falls this % off its peak


def _ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")


def _load():
    try:
        return json.load(open(_PEAKS, encoding="utf-8"))
    except Exception:
        return {}


def _save(p):
    try:
        json.dump(p, open(_PEAKS, "w", encoding="utf-8"))
    except Exception:
        pass


def status(trail_pct=None):
    trail_pct = float(trail_pct or TRAIL_DEFAULT)
    bk = op.book()
    if not bk.get("ok"):
        return {"ok": False, "error": bk.get("error", "no option book")}
    peaks = _load()
    out = []
    for p in bk.get("positions", []):
        sym = p.get("symbol") or p.get("trading_symbol")
        prem = p.get("premium")
        avg = p.get("avg")
        qty = p.get("qty") or p.get("quantity") or 0
        if not sym or prem is None:
            continue
        peak = max(float(peaks.get(sym, 0) or 0), float(prem), float(avg or 0))
        peaks[sym] = round(peak, 2)
        trail = round(peak * (1 - trail_pct / 100), 2)
        off_peak = (1 - prem / peak) * 100 if peak else 0
        pnl_pct = p.get("pnl_pct")
        if pnl_pct is None and avg:
            pnl_pct = round((prem / avg - 1) * 100, 1)
        try:
            lv = ol.signal(sym, prem)
        except Exception:
            lv = {}
        tp = (lv.get("sell_target") or {}) if lv.get("ok") else {}

        if peak > 0 and prem <= trail and off_peak >= trail_pct - 0.5:
            verdict, tone = "EXIT NOW", "exit"
            why = f"premium ₹{prem} has fallen {off_peak:.0f}% off its peak ₹{peak} — trailing stop hit. Bank it before it round-trips."
        elif lv.get("ok") and str(lv.get("signal", "")).startswith("TRIM"):
            verdict, tone = "TRIM @ resistance", "warn"
            why = lv.get("why", "underlying at resistance — scale out into strength.")
        elif lv.get("ok") and str(lv.get("signal", "")).startswith("ADD"):
            verdict, tone = "HOLD · at support", "buy"
            why = lv.get("why", "underlying at support — re-entry/add zone if the trend gate is green.")
        else:
            verdict, tone = "HOLD", "hold"
            why = f"trailing stop ₹{trail} ({trail_pct:.0f}% off peak ₹{peak}); currently {off_peak:.0f}% off the peak."

        q = abs(int(qty))
        gtt_trail = f"SELL {q} {sym}  |  GTT trigger ₹{trail}   (trailing stop — ratchet UP as premium rises)"
        gtt_tp = None
        if tp.get("opt_value") and (tp.get("opt_ret_pct") or 0) > 8:
            gtt_tp = f"SELL {q} {sym}  |  GTT trigger ₹{tp['opt_value']}   (take-profit: {tp.get('which')} {tp.get('u_level')}, +{tp.get('opt_ret_pct')}%)"

        out.append({"sym": sym, "qty": qty, "avg": avg, "premium": round(float(prem), 2), "peak": round(peak, 2),
                    "trail": trail, "trail_pct": trail_pct, "off_peak_pct": round(off_peak, 0), "pnl_pct": pnl_pct,
                    "verdict": verdict, "tone": tone, "why": why, "underlying": lv.get("underlying"),
                    "spot": lv.get("spot"), "signal": lv.get("signal"), "sell_target": tp or None,
                    "gtt_trail": gtt_trail, "gtt_tp": gtt_tp})
    _save(peaks)
    out.sort(key=lambda x: {"exit": 0, "warn": 1, "buy": 2, "hold": 3}.get(x["tone"], 4))
    return {"ok": True, "asof": _ist(), "trail_pct": trail_pct, "n": len(out), "positions": out,
            "caveat": "READ-ONLY. Tracks the premium's running PEAK since first seen; the trailing stop ratchets UP "
                      "only. These are GTT exit SCRIPTS for YOU to arm/re-arm on Groww — the pod never places, "
                      "modifies, or cancels orders. It can't recover a peak it never watched, so it protects from here."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tp = float(sys.argv[1]) if len(sys.argv) > 1 else TRAIL_DEFAULT
    r = status(tp)
    if not r.get("ok"):
        print("  " + r.get("error", "?")); raise SystemExit
    print(f"\n═══ OPTION EXIT MONITOR · trail {r['trail_pct']:.0f}% off peak · {r['asof']} ═══")
    for p in r["positions"]:
        print(f"\n  {p['sym']}  ({p['underlying']} @ {p['spot']})  qty {p['qty']}")
        print(f"    premium ₹{p['premium']} | avg ₹{p['avg']} | peak ₹{p['peak']} | P&L {p['pnl_pct']}% | {p['off_peak_pct']:.0f}% off peak")
        print(f"    >> {p['verdict']}: {p['why']}")
        print(f"    GTT (arm yourself): {p['gtt_trail']}")
        if p["gtt_tp"]:
            print(f"                        {p['gtt_tp']}")
    print("\n  " + r["caveat"])
