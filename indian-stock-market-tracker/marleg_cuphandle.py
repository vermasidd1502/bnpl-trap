"""
marleg_cuphandle.py — LIVE cup-with-handle scanner + the CONFIRMED-breakout strategy.

Backtested edge (marleg_cuphandle_study, 5y panel): the RAW breakout is a coin-flip (51% win, ~half are
false breakouts). The edge is the CONFIRMATION — wait for the breakout to HOLD (62% win / +3.0%/10d) or a
+1% close beyond the pivot (67% / +4.4%). The pattern also pays best held 2-4 weeks, and winners concentrate
in leading cyclical/growth sectors (Capital Goods, IT, Power, Construction, Telecom). This scans for setups
and tags the live STAGE so you never buy the raw bar:

  FORMING        — still rounding the cup (too early; skipped)
  IN_HANDLE      — recovered near the rim, pulling back in a tight handle (the "fight"; watch the pivot)
  BREAKOUT       — crossed the pivot TODAY, not yet held (coin-flip; WAIT for confirmation)
  CONFIRMED      — broke out and HELD (>=1 day above the pivot) — the validated setup
  CONFIRMED+     — held AND >=1% beyond the pivot — the strongest read
  FAILED         — broke out then fell back below the pivot (a TRAP) — avoid

Each setup carries: pivot, measured-move target (O'Neil: pivot + cup depth), stop (handle low), R:R, depth,
prior run-up, days-in-handle, sector-edge flag, bull-gate, and the backtested win/net for its stage.
Read-only; never places an order.
"""
import json
import os
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_cuphandle.json")

WIN_SECTORS = {"Capital Goods", "Information Technology", "Power", "Construction",
               "Telecommunication", "Telecom Networking Equipment & Services", "Forest Materials"}
STAGE_BT = {  # backtested win/net per stage (10d), from marleg_cuphandle_study
    "IN_HANDLE":   {"win": None, "net": None, "note": "not triggered yet — watch the pivot, then demand confirmation"},
    "BREAKOUT":    {"win": 51.1, "net": 0.95, "note": "raw breakout ≈ coin-flip — WAIT one day for it to hold"},
    "CONFIRMED":   {"win": 62.0, "net": 3.02, "note": "held the breakout — the validated setup"},
    "CONFIRMED+":  {"win": 66.9, "net": 4.42, "note": "held AND +1% beyond the pivot — the strongest read"},
    "FAILED":      {"win": None, "net": None, "note": "false breakout (trap) — avoid; the pattern morphed"},
}
STAGE_RANK = {"CONFIRMED+": 0, "CONFIRMED": 1, "BREAKOUT": 2, "IN_HANDLE": 3, "FAILED": 4}


def detect(close, high, low):
    c = close.dropna()
    n = len(c)
    if n < 180:
        return None
    cv = c.values
    t = n - 1
    hv = high.reindex(c.index).values
    lv = low.reindex(c.index).values
    ph = float(np.nanmax(cv[t - 160:t - 35]))                 # left rim / prior high
    i_ph = (t - 160) + int(np.nanargmax(cv[t - 160:t - 35]))
    if i_ph >= t - 8:
        return None
    bottom = float(np.nanmin(cv[i_ph:t - 8]))                  # cup bottom
    if bottom <= 0:
        return None
    depth = (ph - bottom) / ph
    if not (0.12 <= depth <= 0.45):
        return None
    if float(np.nanmax(cv[t - 20:t + 1])) < ph * 0.88:        # must have recovered near the rim
        return None
    handle = cv[t - 10:t + 1]
    hh = float(np.nanmax(handle)); hl = float(np.nanmin(handle))   # pivot + handle low
    if hh <= 0 or (hh - hl) / hh > 0.15:                      # handle must be reasonably tight
        return None
    price = float(cv[t]); prev = float(cv[t - 1])

    failed = float(np.nanmax(cv[t - 5:t])) >= hh * 1.005 and price < hh * 0.985
    if failed:
        stage = "FAILED"
    elif price >= hh * 1.01 and prev >= hh * 0.995:
        stage = "CONFIRMED+"
    elif price >= hh * 0.999 and prev >= hh * 0.995:
        stage = "CONFIRMED"
    elif price >= hh * 0.999 and prev < hh * 0.999:
        stage = "BREAKOUT"
    elif price < hh * 0.999:
        stage = "IN_HANDLE"
    else:
        return None

    # days in handle = bars since price last pulled back from the rim
    dih = 0
    for k in range(t, max(t - 30, 0), -1):
        if cv[k] >= hh * 0.999 and k != t:
            break
        dih += 1
    prior_run = float(cv[i_ph] / cv[max(0, i_ph - 60)] - 1) * 100
    target = hh + (ph - bottom)                               # O'Neil measured move
    stop = round(hl, 1)
    rr = round((target - price) / (price - stop), 1) if price > stop else None
    return {"stage": stage, "price": round(price, 2), "pivot": round(hh, 2),
            "depth_pct": round(depth * 100, 1), "prior_run_pct": round(prior_run, 1), "days_in_handle": dih,
            "target": round(target, 1), "target_pct": round((target - price) / price * 100, 1),
            "stop": stop, "stop_pct": round((price - stop) / price * 100, 1), "rr": rr,
            "rim": round(ph, 1), "bottom": round(bottom, 1)}


def scan(universe=None, fresh=True):
    import marleg_panel_build as pb
    NAMES = {r["s"]: r["n"] for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8"))}
    SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
    panel = pb.load()                                    # canonical panel: clean OHLC (detect needs good high/low),
    if not panel:                                        # instant, and right for a MULTI-WEEK base pattern.
        print("no canonical panel — run marleg_panel_build.py"); return None
    close, high, low = panel["close"], panel["high"], panel["low"]
    if close.shape[1] < 40:
        print(f"[thin] {close.shape[1]} names — keeping prior."); return None
    print(f"cup-handle scan: {close.shape[1]} names (canonical panel, built {panel.get('built')})...")
    # bull regime (NIFTY proxy)
    mkt = (1 + close.pct_change().mean(axis=1)).cumprod()
    bull = bool(mkt.iloc[-1] > mkt.rolling(50).mean().iloc[-1])
    setups = []
    for s in close.columns:
        try:
            d = detect(close[s], high[s], low[s])
        except Exception:
            d = None
        if not d:
            continue
        sec = SECT.get(s, {}).get("sector")
        bt = STAGE_BT.get(d["stage"], {})
        d.update({"s": s, "n": NAMES.get(s, s), "sector": sec,
                  "sector_edge": bool(sec in WIN_SECTORS), "bull": bull,
                  "bt_win": bt.get("win"), "bt_net": bt.get("net"),
                  "read": _read(d, sec in WIN_SECTORS, bull, bt)})
        setups.append(d)
    setups.sort(key=lambda x: (STAGE_RANK.get(x["stage"], 9), 0 if x["sector_edge"] else 1, -(x.get("rr") or 0)))
    ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    out = {"asof": ist, "bull": bull, "n": len(setups),
           "n_confirmed": sum(1 for x in setups if x["stage"].startswith("CONFIRMED")),
           "setups": setups[:60],
           "note": "Cup-with-handle. RULE: buy the CONFIRMED stage (held / +1% past pivot), NOT the raw breakout "
                   "(coin-flip). Best in Capital Goods/IT/Power/Construction, bull regime, held 2-4 weeks."}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"cup-handle setups: {len(setups)} ({out['n_confirmed']} confirmed) -> {OUT}")
    for x in setups[:12]:
        print(f"  {x['s']:<11} {x['stage']:<11} ₹{x['price']} pivot ₹{x['pivot']} -> tgt ₹{x['target']} (+{x['target_pct']}%) "
              f"R:R {x['rr']} {'★sec' if x['sector_edge'] else ''}")
    return out


def _read(d, sec_edge, bull, bt):
    st = d["stage"]
    base = {"CONFIRMED+": f"BUY — confirmed +1% past ₹{d['pivot']} (held). ~67% win / +4.4% (backtested). Target ₹{d['target']} (+{d['target_pct']}%), stop ₹{d['stop']}.",
            "CONFIRMED": f"BUY — breakout held above ₹{d['pivot']}. ~62% win / +3.0%. Target ₹{d['target']} (+{d['target_pct']}%), stop ₹{d['stop']}.",
            "BREAKOUT": f"WAIT — crossed ₹{d['pivot']} today but not held; the raw bar is a coin-flip. Buy only if it holds / closes +1% beyond tomorrow.",
            "IN_HANDLE": f"WATCH — coiling in the handle below the ₹{d['pivot']} pivot ({d['days_in_handle']}d). The buy comes on a CONFIRMED break above it.",
            "FAILED": f"AVOID — broke ₹{d['pivot']} then fell back (false breakout / trap). The pattern has morphed."}.get(st, "")
    if st.startswith(("CONFIRMED", "BREAKOUT")):
        if not bull:
            base += " ⚠ but the market is below its 50DMA — bear-gate trims this edge by ~3x."
        if not sec_edge:
            base += " (note: not in a top cup-handle sector — winners cluster in Capital Goods/IT/Power.)"
    return base


def build():
    try:
        return json.load(open(OUT, encoding="utf-8"))
    except Exception:
        return {"ok": False, "error": "cup-handle radar not built — run marleg_cuphandle.py", "setups": []}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    scan()
