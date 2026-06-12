"""
Marle-G — FIBMAP: the honest two-fib setup, computed for any symbol.

  MACRO fib : the completed major swing (global extreme high & low of the window),
              retracement levels only — the map of the war.
  MICRO fib : the most recent COMPLETED swing leg (pivot-to-pivot), retracements +
              extensions (1.272 / 1.618 / 2.618 / 4.236) — the battle in progress.

THE RULE THAT KEEPS IT HONEST (learned from the fake-confluence trap): anchors must be
COMPLETED extremes — a pivot that price has pulled away from for >= 3 bars. Never the
live price: anchoring a fib's endpoint on the current bar mechanically reproduces the
parent fib's levels (0.236 x 1.618 = 0.382 ...) and manufactures confluence that is
algebra, not the market. Confluence is only reported when the two fibs come from
independently chosen completed swings and land within 1%.

  python marleg_fibmap.py TEJASNET
"""
import sys, json
import numpy as np
import pandas as pd
import marleg_datastore as ds

RETR = [0.236, 0.382, 0.5, 0.618, 0.786]
EXT = [1.272, 1.618, 2.618, 4.236]
PIVOT_W = 3            # bar must be the extreme of a +/-3-bar neighborhood
COMPLETE_BARS = 3      # ...and at least this many bars old (completed, not forming)


def _pivots(h, l):
    """Completed swing highs/lows: (index, price, kind). Last COMPLETE_BARS excluded."""
    n = len(h)
    out = []
    for i in range(PIVOT_W, n - max(PIVOT_W, COMPLETE_BARS)):
        win_h = h[i - PIVOT_W:i + PIVOT_W + 1]
        win_l = l[i - PIVOT_W:i + PIVOT_W + 1]
        if np.isfinite(h[i]) and h[i] == np.nanmax(win_h):
            out.append((i, float(h[i]), "H"))
        elif np.isfinite(l[i]) and l[i] == np.nanmin(win_l):
            out.append((i, float(l[i]), "L"))
    return out


def fibmap(symbol, lookback_days=1250):
    symbol = symbol.upper().replace(".NS", "")
    try:
        ds.sync(symbols=[symbol], verbose=False)
    except Exception:
        pass
    c = ds.series(symbol).dropna().tail(lookback_days)
    if len(c) < 120:
        return {"error": f"not enough history for {symbol}"}
    h = ds.series(symbol, "high").reindex(c.index).values
    l = ds.series(symbol, "low").reindex(c.index).values
    idx = c.index
    live = float(c.iloc[-1])

    # ---- MACRO: the ACTIVE major structure, completed anchors ----
    # Candidates: (a) global low -> global high (the big up-swing);
    #             (b) global high -> lowest low AFTER it (the decline being retraced).
    # Prefer (b) when a substantial post-high decline exists — that is the structure
    # the market is currently trading against (the recovering-stock case).
    hi_i = int(np.nanargmax(h[:-COMPLETE_BARS]))
    lo_all_i = int(np.nanargmin(l[:-COMPLETE_BARS]))
    hi = float(h[hi_i])
    use_decline = False
    if hi_i < len(c) - COMPLETE_BARS - 5:
        lo_after_i = hi_i + int(np.nanargmin(l[hi_i:-COMPLETE_BARS]))
        lo_after = float(l[lo_after_i])
        if (hi - lo_after) >= 0.5 * (hi - float(l[lo_all_i])):
            use_decline = True
    if use_decline:
        lo_i, lo = lo_after_i, lo_after
        direction = "down-then-recover (retracing the decline)"
    else:
        lo_i, lo = lo_all_i, float(l[lo_all_i])
        direction = "up-from-low"
    rng = hi - lo
    macro_levels = {f"{r}": round(lo + r * rng, 2) for r in RETR}
    macro = {"hi": round(hi, 2), "lo": round(lo, 2),
             "hi_date": str(idx[hi_i].date()), "lo_date": str(idx[lo_i].date()),
             "direction": direction, "levels": macro_levels}

    # ---- MICRO: latest completed pivot-to-pivot leg ----
    piv = _pivots(h, l)
    micro = None
    if len(piv) >= 2:
        # walk back: find the last two alternating pivots (a completed leg)
        a, b = None, None
        for k in range(len(piv) - 1, 0, -1):
            if piv[k][2] != piv[k - 1][2]:
                a, b = piv[k - 1], piv[k]
                break
        if a and b:
            if a[2] == "L" and b[2] == "H":                      # up-leg
                m_lo, m_hi = a[1], b[1]
                m_rng = m_hi - m_lo
                levels = {f"{r}": round(m_hi - r * m_rng, 2) for r in RETR}
                exts = {f"{e}": round(m_lo + e * m_rng, 2) for e in EXT}
                mdir = "up-leg"
                anchors = {"lo": round(m_lo, 2), "hi": round(m_hi, 2),
                           "lo_date": str(idx[a[0]].date()), "hi_date": str(idx[b[0]].date())}
            else:                                                # down-leg
                m_hi, m_lo = a[1], b[1]
                m_rng = m_hi - m_lo
                levels = {f"{r}": round(m_lo + r * m_rng, 2) for r in RETR}
                exts = {f"{e}": round(m_hi - e * m_rng, 2) for e in EXT}
                mdir = "down-leg"
                anchors = {"hi": round(m_hi, 2), "lo": round(m_lo, 2),
                           "hi_date": str(idx[a[0]].date()), "lo_date": str(idx[b[0]].date())}
            micro = {**anchors, "direction": mdir, "levels": levels, "extensions": exts}

    # ---- honest confluence: independent completed anchors landing within 1% ----
    confluence = []
    if micro:
        allm = {**{f"macro {k}": v for k, v in macro_levels.items()},
                "macro hi": macro["hi"], "macro lo": macro["lo"]}
        allu = {**{f"micro {k}": v for k, v in micro["levels"].items()},
                **{f"micro x{k}": v for k, v in micro["extensions"].items()}}
        for mk, mv in allm.items():
            for uk, uv in allu.items():
                if mv > 0 and abs(uv / mv - 1) < 0.01:
                    confluence.append({"macro_level": mk, "micro_level": uk,
                                       "price": round((mv + uv) / 2, 2),
                                       "dist_from_live_pct": round((((mv + uv) / 2) / live - 1) * 100, 1)})
    # ---- diffusion ETA per upside level: days ~ (distance% / daily-sigma%)^2 ----
    r = c.pct_change().dropna()
    sig_d = float(r.tail(60).std() * 100) or 2.0
    eta = []
    cands = {**{f"macro {k}": v for k, v in macro_levels.items()}, "macro 1.0": macro["hi"]}
    if micro:
        cands.update({"micro m1": micro["hi"],
                      **{f"micro x{k}": v for k, v in micro["extensions"].items()}})
    for name, px_l in sorted(cands.items(), key=lambda kv: kv[1]):
        if not px_l or px_l <= live * 1.002 or px_l > macro["hi"] * 1.05:
            continue
        dist = (px_l / live - 1) * 100
        td = (dist / sig_d) ** 2
        label = (f"~{max(1, round(td / 5))}w" if td < 63 else
                 f"~{round(td / 21)}mo" if td < 504 else f"~{round(td / 252, 1)}y")
        eta.append({"level": name, "price": px_l, "dist_pct": round(dist, 1),
                    "eta_trading_days": round(td), "eta": label})
    return {"symbol": symbol, "asof": str(idx[-1].date()), "live": round(live, 2),
            "daily_sigma_pct": round(sig_d, 2), "upside_eta": eta,
            "macro": macro, "micro": micro, "confluence": confluence,
            "note": ("Anchors are COMPLETED pivots only (>= %d bars old, +/-%d-bar extremes). "
                     "Never anchored to the live price — that manufactures fake confluence. "
                     "Confluence shown only for independent anchors within 1%%." % (COMPLETE_BARS, PIVOT_W))}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sym = sys.argv[1] if len(sys.argv) > 1 else "TEJASNET"
    r = fibmap(sym)
    print(json.dumps(r, indent=1))


if __name__ == "__main__":
    main()
