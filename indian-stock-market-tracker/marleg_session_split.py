"""
marleg_session_split.py — should a BEARISH bet be held INTRADAY or OVERNIGHT? Decompose every day's return
into the OVERNIGHT leg (prior close → today's open) and the INTRADAY leg (today's open → close), across the
full panel, overall and CONDITIONED on a weak state (prior day closed below its pivot).

Why it matters for shorting: a short profits from NEGATIVE returns. India's structural up-drift mostly
happens OVERNIGHT (gap-ups), so holding a short overnight pays that drift to the longs. If the weakness a
short wants shows up INTRADAY, then an intraday short (exit by close) is the better vehicle — it skips the
overnight up-drift and the multi-day theta a held put would bleed.

Read-only, pure compute on the canonical panel.
  python marleg_session_split.py
"""
import sys
import numpy as np


def study():
    import marleg_panel_build as pb
    P = pb.load()
    O, C, H, L = P["open"], P["close"], P["high"], P["low"]
    overnight, intraday, on_w, id_w = [], [], [], []
    n_names = 0
    for s in C.columns:
        c = C[s].dropna()
        if len(c) < 80:
            continue
        n_names += 1
        o = O[s].reindex(c.index)
        h = H[s].reindex(c.index)
        l = L[s].reindex(c.index)
        on = (o / c.shift(1) - 1.0) * 100.0          # overnight: prior close -> open
        idr = (c / o - 1.0) * 100.0                  # intraday:  open -> close
        piv = (h.shift(2) + l.shift(2) + c.shift(2)) / 3.0   # prior day's pivot (from t-2 OHLC)
        weak = c.shift(1) < piv                       # prior day closed BELOW its pivot = a weak/bearish state
        m = on.notna() & idr.notna()
        overnight.extend(on[m].values.tolist())
        intraday.extend(idr[m].values.tolist())
        wm = m & weak.fillna(False)
        on_w.extend(on[wm].values.tolist())
        id_w.extend(idr[wm].values.tolist())

    def agg(xs):
        if not xs:
            return None
        a = np.array(xs, float)
        return {"n": int(a.size), "mean": round(float(a.mean()), 3), "win_up": round(float((a > 0).mean()) * 100, 1)}

    ov, idd, ovw, idw = agg(overnight), agg(intraday), agg(on_w), agg(id_w)
    # a short GAINS the negative of the return. Short P&L = -mean.
    def short_pnl(a):
        return round(-a["mean"], 3) if a else None
    verdict = []
    if ov and idd:
        verdict.append(f"All days: overnight drift {ov['mean']:+.3f}%/day (up {ov['win_up']}%) vs intraday {idd['mean']:+.3f}% "
                       f"→ a SHORT loses {short_pnl(ov):+.3f}%/night overnight but {short_pnl(idd):+.3f}% intraday.")
    if ovw and idw:
        verdict.append(f"Weak names (below pivot): overnight {ovw['mean']:+.3f}% vs intraday {idw['mean']:+.3f}% "
                       f"→ short P&L {short_pnl(ovw):+.3f}% overnight vs {short_pnl(idw):+.3f}% intraday.")
    better = ("INTRADAY" if (idd and ov and short_pnl(idd) > short_pnl(ov)) else "OVERNIGHT")
    return {"ok": True, "names": n_names,
            "all": {"overnight": ov, "intraday": idd, "short_overnight": short_pnl(ov), "short_intraday": short_pnl(idd)},
            "weak": {"overnight": ovw, "intraday": idw, "short_overnight": short_pnl(ovw), "short_intraday": short_pnl(idw)},
            "short_better": better, "verdict": verdict,
            "note": "Short P&L = −(mean return) per leg, before costs. India's up-drift is largely OVERNIGHT, so an "
                    "overnight short pays that drift to longs; intraday is the cleaner short window. Still a thin, "
                    "costs-sensitive edge — shorting is an anti-edge here overall. Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = study()
    print(f"\n  SESSION SPLIT — overnight vs intraday · {r['names']} names")
    a, w = r["all"], r["weak"]
    print(f"  ALL DAYS   overnight {a['overnight']['mean']:+.3f}% (up {a['overnight']['win_up']}%) · intraday {a['intraday']['mean']:+.3f}% (up {a['intraday']['win_up']}%)")
    print(f"             → short P&L: overnight {a['short_overnight']:+.3f}% · intraday {a['short_intraday']:+.3f}%")
    print(f"  WEAK NAMES overnight {w['overnight']['mean']:+.3f}% · intraday {w['intraday']['mean']:+.3f}%")
    print(f"             → short P&L: overnight {w['short_overnight']:+.3f}% · intraday {w['short_intraday']:+.3f}%")
    print(f"\n  ➤ short is better held: {r['short_better']}")
    for v in r["verdict"]:
        print("   ", v)
    print(f"\n  {r['note']}")
