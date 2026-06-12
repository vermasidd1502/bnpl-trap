"""
Marle-G — SHORT RADAR ("eclipse watch"). Names about to top out: RSI overbought +
extended above their mean in ATR units + intraday momentum fading — cross-checked
against each name's OWN spike-fade history (some names fade their spikes; some are
TEJASNET and keep going — never short those on accumulation days).

HONESTY FIRST: validate() backtests the signal on the decade store BEFORE the live
scan is trusted: signal day (RSI>72, extension>2 ATR, big up day) -> SHORT next day
open-to-close (the intraday proxy), net 12bps. If the numbers are weak, the radar is
a watchlist, not a trade list.

  python marleg_short_radar.py            # validation backtest + live scan
"""
import os, sys, json, math
import numpy as np
import pandas as pd
import marleg_datastore as ds

HERE = os.path.dirname(os.path.abspath(__file__))
RSI_HOT = 72.0
EXT_ATR = 2.0
SPIKE_CHG = 3.0          # % up-day that marks a spike
COST_INTRA = 12 / 1e4


def _rsi(close, n=14):
    d = close.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def _frame():
    ds.sync(verbose=False)
    C = ds.panel("close").ffill()
    keep = [c for c in C.columns if C[c].dropna().shape[0] > 400]
    C = C[keep]
    O = ds.panel("open")[keep].reindex(C.index)
    H = ds.panel("high")[keep].reindex(C.index)
    L = ds.panel("low")[keep].reindex(C.index)
    V = ds.panel("volume")[keep].reindex(C.index)
    pc = C.shift(1)
    tr = pd.concat([(H - L).stack(), (H - pc).abs().stack(), (L - pc).abs().stack()],
                   axis=1).max(axis=1).unstack()
    atr = tr.rolling(14).mean()
    rsi = pd.DataFrame({s: _rsi(C[s]) for s in C.columns})
    sma20 = C.rolling(20).mean()
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    return C, O, atr, rsi, sma20, rc, ud


def validate():
    """Decade test: overbought+extended spike day -> short NEXT day open->close, net."""
    C, O, atr, rsi, sma20, rc, ud = _frame()
    variants = {
        "rsi_only":          lambda i, j: rsi.iloc[i, j] > RSI_HOT,
        "rsi+ext":           lambda i, j: rsi.iloc[i, j] > RSI_HOT and
                             (C.iloc[i, j] - sma20.iloc[i, j]) > EXT_ATR * atr.iloc[i, j],
        "rsi+ext+spike":     lambda i, j: rsi.iloc[i, j] > RSI_HOT and
                             (C.iloc[i, j] - sma20.iloc[i, j]) > EXT_ATR * atr.iloc[i, j] and
                             rc.iloc[i, j] * 100 > SPIKE_CHG,
        "rsi+ext+spike-noUD": lambda i, j: rsi.iloc[i, j] > RSI_HOT and
                             (C.iloc[i, j] - sma20.iloc[i, j]) > EXT_ATR * atr.iloc[i, j] and
                             rc.iloc[i, j] * 100 > SPIKE_CHG and
                             (not np.isfinite(ud.iloc[i, j]) or ud.iloc[i, j] < 2.0),
    }
    out = {}
    n_days, n_sym = C.shape
    for name, cond in variants.items():
        rets = []
        for i in range(210, n_days - 1):
            for j in range(n_sym):
                try:
                    if not all(np.isfinite(x.iloc[i, j]) for x in (C, atr, rsi, sma20)):
                        continue
                    if cond(i, j):
                        o1, c1 = O.iloc[i + 1, j], C.iloc[i + 1, j]
                        if np.isfinite(o1) and np.isfinite(c1) and o1 > 0:
                            rets.append((o1 - c1) / o1 - COST_INTRA)   # short open->close
                except Exception:
                    continue
        if rets:
            a = np.array(rets) * 100
            out[name] = {"n": len(a), "mean_pct": round(float(a.mean()), 3),
                         "median_pct": round(float(np.median(a)), 3),
                         "win_pct": round(float((a > 0).mean() * 100), 1)}
    return out


def scan():
    """Live eclipse watch: overbought + extended + fading, with fade-history check."""
    C, O, atr, rsi, sma20, rc, ud = _frame()
    # live prices for today's action
    live = {}
    try:
        import yfinance as yf
        d = yf.download([s + ".NS" for s in C.columns], period="1d", interval="5m",
                        group_by="ticker", progress=False, threads=True)
        for s in C.columns:
            try:
                bars = d[s + ".NS"]["Close"].dropna()
                if len(bars) > 3:
                    live[s] = {"px": float(bars.iloc[-1]),
                               "fading": float(bars.iloc[-1]) < float(bars.iloc[-min(4, len(bars)):].max())}
            except Exception:
                pass
    except Exception:
        pass
    i = len(C.index) - 1
    rows = []
    for j, s in enumerate(C.columns):
        px = (live.get(s) or {}).get("px") or float(C.iloc[i, j])
        a = float(atr.iloc[i, j]) if np.isfinite(atr.iloc[i, j]) else None
        m20 = float(sma20.iloc[i, j]) if np.isfinite(sma20.iloc[i, j]) else None
        if not (a and m20 and px):
            continue
        # live-ish RSI: yesterday's RSI nudged by today's move direction (approximation)
        r_y = float(rsi.iloc[i, j]) if np.isfinite(rsi.iloc[i, j]) else 50.0
        chg_today = (px / float(C.iloc[i, j]) - 1) * 100
        ext = (px - m20) / a
        u = float(ud.iloc[i, j]) if np.isfinite(ud.iloc[i, j]) else 1.0
        # fade history: avg 3d forward return after past spike days
        sj = C[s]
        spikes = sj.index[(rc[s] * 100 > 4.0)]
        fwd = []
        for d0 in spikes[-30:]:
            k = sj.index.get_loc(d0)
            if k + 3 < len(sj):
                fwd.append(sj.iloc[k + 3] / sj.iloc[k] - 1)
        fade_hist = round(float(np.mean(fwd)) * 100, 2) if len(fwd) >= 5 else None
        fade_prone = fade_hist is not None and fade_hist < -0.5
        hot = r_y > 65 and ext > 1.5 and chg_today > 1.0
        if not hot:
            continue
        score = (r_y - 65) / 10 + ext / 2 + chg_today / 3 + (1.5 if fade_prone else 0) - (2.0 if u >= 2.0 else 0)
        rows.append({"sym": s, "px": round(px, 1), "rsi_y": round(r_y, 0), "ext_atr": round(ext, 1),
                     "chg_today_pct": round(chg_today, 1), "ud": round(u, 2),
                     "fade_hist_3d_pct": fade_hist, "fading_now": bool((live.get(s) or {}).get("fading")),
                     "no_short": u >= 2.0,
                     "verdict": ("DO NOT SHORT — accumulation monster" if u >= 2.0 else
                                 ("SHORT WATCH — fade-prone" if fade_prone else "watch only — no fade history")),
                     "score": round(score, 2)})
    rows.sort(key=lambda x: -x["score"])
    return {"asof": str(C.index[-1].date()), "candidates": rows,
            "plan": "intraday short ONLY: entry on lower-high after 10:15, stop = entry + 1.5xATR(15m), "
                    "breakeven lock at +1xATRi, flat 15:05. Paper-first — see validation.",
            "validation": None}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print("VALIDATION — short next-day open->close after overbought signals (decade, net 12bps):")
    v = validate()
    for k, s in v.items():
        print("  %-22s n=%-6s mean %+6.3f%%  median %+6.3f%%  win %s%%" % (
            k, s["n"], s["mean_pct"], s["median_pct"], s["win_pct"]))
    res = scan()
    res["validation"] = v
    json.dump(res, open(os.path.join(HERE, "marleg_short_radar.json"), "w"), indent=1)
    print("\nECLIPSE WATCH — %s" % res["asof"])
    for r in res["candidates"][:10]:
        print("  %-12s px %-8s rsi %-4s ext %-5s today %+5s%% ud %-5s fade3d %-6s %s" % (
            r["sym"], r["px"], r["rsi_y"], r["ext_atr"], r["chg_today_pct"], r["ud"],
            r["fade_hist_3d_pct"], r["verdict"]))
    print("\n[wrote marleg_short_radar.json]")


if __name__ == "__main__":
    main()
