"""
marleg_factor.py — VALUE + MOMENTUM screener (the core strategy).

Targets stocks that are UNDERVALUED *and* GAINING MOMENTUM — the two most-validated factors
(Asness, "Value and Momentum Everywhere"), with this session's refinements baked in:
  VALUE    = below fair value (Graham/DCF gap) + quality (avoid value traps) + room-to-run
  MOMENTUM = above 200-DMA + positive 3-6mo trend + under accumulation, but RSI NOT overbought
             and NOT at-ceiling (early momentum off a base, never the chased spike)

Combined via a harmonic mean so a name must score on BOTH legs (cheap-but-falling or
hot-but-expensive both score low). Reuses marleg_fundamentals / overextension / weekend.

  screen()  -> ranked list
  python marleg_factor.py
"""
import sys, os, json, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))


def _universe(n=40):
    s = set()
    for f, key in [("marleg_gated_cache.json", "picks"), ("marleg_volume_cache.json", None)]:
        try:
            d = json.load(open(os.path.join(HERE, f), encoding="utf-8"))
            if key:
                for p in d.get("picks", [])[:n]:
                    s.add(p["s"])
            else:
                for sec in d.get("sectors", []):
                    for x in sec.get("stocks", [])[:4]:
                        s.add(x["s"])
        except Exception:
            pass
    return sorted(s)[:n] if s else ["TEJASNET", "QUESS", "HFCL", "DLINKINDIA"]


def _clip(x, lo=0, hi=100):
    return max(lo, min(hi, x))


def screen(universe=None, top=15):
    import marleg_fundamentals as mf, marleg_overextension as oe, marleg_weekend as w
    U = universe or _universe()
    rows = []
    for tk in U:
        try:
            ch = oe.chase_check(tk)
            if ch.get("error"):
                continue
            f = mf.fundamentals(tk)
            acc = w.accumulation(tk)
            gap = f.get("gap")                       # % vs intrinsic (+ = undervalued)
            q = f.get("qscore") or 0
            room = ch.get("room_to_52w_high_pct") or 0
            rsi = ch.get("rsi") or 50
            ret20 = ch.get("ret20_pct") or 0; ret60 = ch.get("ret60_pct") or 0
            recovering = ch.get("recovering"); extended = ch.get("at_ceiling")
            stretch = ch.get("stretch_50dma_pct") or 0
            accscore = acc.get("score", 50)

            # ---- VALUE (0-100): cheap + quality + room ----
            vs = 0.0
            if gap is not None:
                vs += 45 if gap > 12 else (28 if gap > -8 else 8)     # undervalued / fair / rich
            else:
                vs += 18                                              # unknown fair value -> neutral-ish
            vs += 22 * (q / 100.0)                                    # quality (anti value-trap)
            vs += 25 * min(1.0, room / 30.0)                         # structural cheapness (room below 52wH)
            if q and q < 25:
                vs -= 12                                              # very weak quality = trap risk
            vs = round(_clip(vs))

            # ---- MOMENTUM (0-100): trend + accumulation, EARLY not extended ----
            ms = 0.0
            ms += 25 if ret60 > 0 else -10                           # 3-mo trend up
            ms += min(20, max(0, ret60 * 0.6))                      # magnitude (capped)
            ms += 22 * _clip((accscore - 45) / 45.0, 0, 1)          # under accumulation
            ms += 18 if (50 <= rsi <= 68) else (-12 if rsi > 72 else 4 if rsi >= 45 else -6)  # rising, not overbought
            ms += 8 if recovering else 0
            ms -= 28 if extended else 0                              # don't-chase veto
            ms -= min(12, max(0, (stretch - 18)))                   # over-stretched from 50-DMA
            ms = round(_clip(ms))

            combined = round(2 * vs * ms / (vs + ms), 1) if (vs + ms) else 0
            rows.append({"tk": tk, "name": f.get("name", tk), "value": vs, "momentum": ms, "score": combined,
                         "gap": gap, "qscore": q, "room": round(room, 1), "rsi": round(rsi),
                         "ret60": round(ret60, 1), "accum": acc.get("label"), "verdict": f.get("verdict"),
                         "extended": bool(extended)})
        except Exception:
            continue
    rows.sort(key=lambda r: -r["score"])
    return {"n": len(rows), "rows": rows[:top],
            "note": "Value+Momentum (harmonic) — must score on BOTH. Undervalued+quality+room ✕ trend+accumulation, "
                    "RSI<70 & not at-ceiling. Positional screen, not a chase; size + verify before acting."}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = screen()
    print(f"\nVALUE + MOMENTUM screen · {r['n']} names\n")
    print(f"{'stock':<12}{'V+M':>6}{'val':>5}{'mom':>5}{'gap%':>7}{'qual':>5}{'room%':>7}{'RSI':>5}{'3mo%':>7}  accumulation")
    for x in r["rows"]:
        print(f"  {x['tk']:<10}{x['score']:>6}{x['value']:>5}{x['momentum']:>5}"
              f"{(str(x['gap']) if x['gap'] is not None else '—'):>7}{x['qscore']:>5}{x['room']:>7}{x['rsi']:>5}{x['ret60']:>7}  {x['accum'] or '—'}"
              + ("  ⚠ext" if x["extended"] else ""))


if __name__ == "__main__":
    main()
