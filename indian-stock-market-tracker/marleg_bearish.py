"""
marleg_bearish.py — the HONEST bearish / defensive pod.

Our 5y backtest (marleg_short_study) is blunt: OUTRIGHT shorting loses in every category — the average
India stock rises +0.85%/10d and even breakdowns/parabolics bounce, so a "short these weak names" list
bleeds (parabolic shorts were the WORST, -2.62%/10d). What the data DOES support, and all this pod does:

  1. AVOID / EXIT — names that are RELATIVE laggards AND breaking down: don't BUY them, trim/exit longs.
     (the only edge in weakness is relative; the ledger drop-off study already showed laggards underperform.)
  2. PAIRS (market-neutral) — LONG the industry leader, SHORT the industry laggard: captures the relative
     spread without fighting the index drift. The one structure where a short leg pays its way.
  3. HEDGE — when the macro gate is red, size an index-put hedge to your book. Insurance, not alpha.

Read-only; never places an order. Decision-support, not advice.

  python marleg_bearish.py
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_bearish.json")
MIN_SPREAD = 0.15        # min leader-laggard 20d gap to suggest a pair
HEDGE_PCT = {"SHOCK": 50, "STRESS": 35, "BEAR": 25, "NORMAL": 0, "BULL": 0, "—": 0}


def _load(fn):
    try:
        return json.load(open(os.path.join(HERE, fn), encoding="utf-8"))
    except Exception:
        return {}


def _rsi(C, n=14):
    d = C.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def _held():
    """Read-only Groww book → set of held NSE symbols (best-effort, {} on any failure)."""
    held = set()
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        for getter in ("holdings_data", "positions_data"):
            try:
                data = getattr(g, getter)()
            except Exception:
                continue
            rows = data if isinstance(data, list) else (
                data.get("positions") or data.get("holdings") or data.get("data") or []) if isinstance(data, dict) else []
            for r in rows:
                if isinstance(r, dict):
                    sym = (r.get("trading_symbol") or r.get("symbol") or r.get("tradingsymbol") or r.get("nse_symbol") or "").upper()
                    if sym:
                        held.add(sym)
    except Exception:
        pass
    return held


def _frame():
    import marleg_panel_build as pb
    P = pb.load()
    if not P:
        return None
    C, V = P["close"], P["volume"]
    last = C.iloc[-1]
    s50 = C.rolling(50).mean().iloc[-1]
    s200 = C.rolling(200).mean().iloc[-1]
    ret20 = C.iloc[-1] / C.iloc[-21] - 1
    ret5 = C.iloc[-1] / C.iloc[-6] - 1
    rsi = _rsi(C).iloc[-1]
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = (upv / dnv).iloc[-1]
    hi250 = C.rolling(250).max().iloc[-1]
    turnover = (C * V).rolling(20).mean().iloc[-1]              # avg ₹ turnover/day (liquidity)
    df = pd.DataFrame({"price": last, "s50": s50, "s200": s200, "ret20": ret20 * 100,
                       "ret5": ret5 * 100, "rsi": rsi, "ud": ud, "dist_hi": (last / hi250 - 1) * 100,
                       "turnover": turnover})
    return df.dropna(subset=["price", "s50"])


def avoid_exit(df, SECT, held, top=30, turn_min=2.5e8):
    """Relative-laggard + breaking-down names: don't buy, exit longs. (NOT shorts — they still bounce.)
    Liquid names only (turnover >= ~₹10cr/day) plus anything you actually hold."""
    ind = {s: (SECT.get(s) or {}).get("industry") or (SECT.get(s) or {}).get("sector") for s in df.index}
    df = df.assign(industry=pd.Series(ind))
    med = df.groupby("industry")["ret20"].transform("median")
    df = df[(df["turnover"] >= turn_min) | df.index.isin(held)]  # liquid universe + your book
    df = df.assign(rel=df["ret20"] - med)                       # under/over its own industry
    rows = []
    for s, r in df.iterrows():
        below50 = r.price < r.s50
        below200 = bool(np.isfinite(r.s200) and r.price < r.s200)
        weak_ud = bool(np.isfinite(r.ud) and r.ud < 0.7)
        laggard = bool(np.isfinite(r.rel) and r.rel < -5)        # >5% behind industry peers
        score = (35 if below50 else 0) + (20 if below200 else 0) + (20 if laggard else 0) + \
                (15 if weak_ud else 0) + (10 if r.rsi < 40 else 0)
        if score < 50:
            continue
        flags = []
        if below50: flags.append("<50DMA")
        if below200: flags.append("<200DMA")
        if laggard: flags.append(f"laggard ({r.rel:+.0f}% vs peers)")
        if weak_ud: flags.append(f"distribution U/D {r.ud:.2f}")
        if r.rsi < 40: flags.append(f"RSI {r.rsi:.0f}")
        rows.append({"s": s, "industry": r.industry, "price": round(float(r.price), 1),
                     "ret20": round(float(r.ret20), 1), "rel": round(float(r.rel), 1) if np.isfinite(r.rel) else None,
                     "score": int(score), "held": s in held, "flags": flags,
                     "action": "EXIT — you hold this" if s in held else "AVOID — don't buy the dip"})
    rows.sort(key=lambda x: (not x["held"], -x["score"]))         # held names first, then weakest
    return rows[:top]


def pairs(df, SECT, fno, top=12, turn_min=1e8):
    """Market-neutral: long the industry leader, short the industry laggard — the only short leg that pays.
    Both legs liquid; the SHORT leg must be F&O-shortable (you can't short illiquid cash in India). No 'Others'."""
    ind = {s: (SECT.get(s) or {}).get("industry") or (SECT.get(s) or {}).get("sector") for s in df.index}
    df = df.assign(industry=pd.Series(ind))
    df = df[df["turnover"] >= turn_min]                          # liquid legs only
    out = []
    for g, sub in df.groupby("industry"):
        sub = sub.dropna(subset=["ret20"])
        if not g or g == "Others" or len(sub) < 4:
            continue
        ups = sub[sub.price > sub.s50]
        dns = sub[(sub.price < sub.s50) & (sub.index.isin(fno) if fno else True)]   # short leg must be shortable
        if ups.empty or dns.empty:
            continue
        lead = ups["ret20"].idxmax(); lag = dns["ret20"].idxmin()
        spread = float(sub.loc[lead, "ret20"] - sub.loc[lag, "ret20"]) / 100
        if spread < MIN_SPREAD:
            continue
        out.append({"industry": g, "spread_pct": round(spread * 100, 1),
                    "long": lead, "long_ret20": round(float(sub.loc[lead, "ret20"]), 1),
                    "long_price": round(float(sub.loc[lead, "price"]), 1),
                    "short": lag, "short_ret20": round(float(sub.loc[lag, "ret20"]), 1),
                    "short_price": round(float(sub.loc[lag, "price"]), 1), "short_fno": lag in fno,
                    "note": f"long {lead} / short {lag} — same industry, so the index drift cancels; "
                            f"you're trading the {round(spread*100)}% relative gap. Short leg via F&O/SLB."})
    out.sort(key=lambda x: -x["spread_pct"])
    return out[:top]


def hedge(held):
    """Macro-gate-driven index hedge sizing. Insurance when the gate is red; nothing when it's green."""
    try:
        import marleg_shock as sh
        s = sh.build()
    except Exception:
        s = {}
    badge = s.get("badge", "—"); state = s.get("state", "NORMAL")
    pct = HEDGE_PCT.get(badge, 0)
    on = pct > 0
    return {"gate_state": state, "badge": badge, "trade_ok": s.get("trade_ok"),
            "hedge_pct": pct, "hedge_on": on, "verdict": s.get("verdict", ""),
            "held_count": len(held),
            "instrument": "NIFTY (or BANKNIFTY) ATM/slightly-OTM put, near-month — defined risk",
            "rationale": (f"Gate {badge}: hedge ~{pct}% of beta-adjusted book exposure with index puts; "
                          f"remove when the gate turns green." if on else
                          "Gate is green — no hedge. Hedging in an up-market is pure drag; only pay for it when the gate is red."),
            "note": "Hedge sizing scales to YOUR book beta (size puts so put-delta ≈ pct% of book × beta). "
                    "Puts not short futures — loss is capped at the premium. Insurance, not alpha."}


def build():
    df = _frame()
    if df is None or df.empty:
        return {"ok": False, "error": "panel not built", "avoid": [], "pairs": [], "hedge": {}}
    SECT = _load("marleg_sectors.json")
    NAMES = {r["s"]: r["n"] for r in _load("marleg_symbols.json")} if isinstance(_load("marleg_symbols.json"), list) else {}
    held = _held()
    try:
        import marleg_options_monitor as mom
        FNO = set(mom.FNO_UNDERLYINGS)
    except Exception:
        FNO = set()
    av = avoid_exit(df, SECT, held)
    pr = pairs(df, SECT, FNO)
    hg = hedge(held)
    for r in av:
        r["n"] = NAMES.get(r["s"], r["s"])
    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    return {"ok": True, "asof": ist, "avoid": av, "pairs": pr, "hedge": hg,
            "held_n": len(held), "n_avoid": len(av), "n_pairs": len(pr),
            "evidence": {"baseline_drift_10d": 0.85, "parabolic_short_net10": -2.62, "breakdown_short_net10": -1.46,
                         "gap_down_short_net10": -0.48,
                         "verdict": "Outright shorting lost in all 6 tested categories (5y). This pod only does "
                                    "what the data supports: avoid/exit weak names, market-neutral pairs, and "
                                    "regime-gated index hedging."},
            "note": "Bearish ≠ short. India drifts up, so directional shorts bleed; the honest plays are relative "
                    "(pairs), defensive (avoid/exit), and protective (hedge). Read-only — not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = build()
    if not r.get("ok"):
        print(r.get("error")); sys.exit()
    print(f"BEARISH/DEFENSIVE POD — {r['asof']}  (book: {r['held_n']} held)\n")
    h = r["hedge"]
    print(f"HEDGE: gate {h['badge']} → hedge {h['hedge_pct']}%  · {h['rationale'][:90]}")
    print(f"\nAVOID / EXIT (top {min(10, r['n_avoid'])} of {r['n_avoid']}):")
    for x in r["avoid"][:10]:
        print(f"  {x['s']:<12} score {x['score']:>3} {('[HELD→EXIT]' if x['held'] else '          ')} {', '.join(x['flags'])[:60]}")
    print(f"\nPAIRS (market-neutral, top {min(8, r['n_pairs'])} of {r['n_pairs']}):")
    for x in r["pairs"][:8]:
        print(f"  {x['industry'][:24]:<24} LONG {x['long']:<11} / SHORT {x['short']:<11} spread {x['spread_pct']:>5}%")
