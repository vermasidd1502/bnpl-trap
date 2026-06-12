"""
Marle-G — SIGNAL QUALITY & HORIZON: real winners vs misleading volume spikes, + dated targets.

Three things, one goal — trust the volume signal and bound it in time:

1) REAL vs TRANSIENT spike.  An Indian small/mid-cap can print a one-day volume spike that simply
   FADES (operator-driven, illiquid). We separate sustained accumulation from a misleading blip with:
     persistence  : rvol_now / mean(rvol, 5d)   (>>1 = a single spike, not a trend)
     liquidity    : 20d average turnover         (thin names spike on nothing)
     spike-fade   : after past high-rvol up-days, the mean forward 3-day return  (<0 = this name fades)
   -> tag REAL (winner candidate) / SUSPECT (likely-misleading) / NEUTRAL.

2) Dated targets.  Each signal gets target = +2 ATR, stop = -1 ATR, and an estimated TIME-TO-TARGET
   from diffusion (days ~ (distance/daily-vol)^2) -> a calendar DATE + a horizon label
   (intraday/short, swing, positional). So a target is no longer horizon-less.

3) Pareto timeframe.  Across all historical BUYING signals we measure forward return at 1/2/3/5/10/21
   days and find the smallest horizon that captures ~80% of the eventual move -> the "better timeframe."

  python marleg_signal_quality.py
"""
import os, sys, json
from datetime import datetime, timedelta, timezone
import numpy as np, pandas as pd, yfinance as yf
import marleg_volume_scan as vs

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
HZ = [1, 2, 3, 5, 10, 21]
try:
    NAMES = {k: (v.get("name") or k) for k, v in
             json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json")))["by_symbol"].items()}
except Exception:
    NAMES = {}


def _atr(h, l, c, n=14):
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def _quality(turnover_cr, spike_conc, rev3, ud):
    issues = []
    if turnover_cr is not None and turnover_cr < 30:
        issues.append("illiquid (<Rs30cr/day)")
    if spike_conc is not None and not np.isnan(spike_conc) and spike_conc > 2.2:
        issues.append("one-day volume spike")
    if rev3 is not None and not np.isnan(rev3) and rev3 < 0:
        issues.append("historically fades its spikes")
    if not issues and ud >= 1.3:
        return "REAL", "sustained up-volume - liquid - follows through"
    if issues:
        return "SUSPECT", "; ".join(issues)
    return "NEUTRAL", "no strong volume signal"


def _target(price, atr, dvol):
    if not atr or not price:
        return {}
    target, stop = price + 2 * atr, price - atr
    dist = 2 * atr / price
    days = int(np.clip((dist / dvol) ** 2, 1, 60)) if (dvol and dvol > 0) else None
    cal = round(days * 7 / 5) if days else None
    date = (datetime.now(IST) + timedelta(days=cal)).strftime("%Y-%m-%d") if cal else None
    horizon = ("intraday/short" if days and days <= 2 else "swing (days)" if days and days <= 10
               else "positional (weeks)") if days else None
    return {"target": round(target, 1), "stop": round(stop, 1), "days_to_target": days,
            "target_date": date, "horizon": horizon}


def _fib(d, lookback=60):
    """Recent swing high -> swing low (a decline); the 0.618 retracement is the bounce's resistance.
    at_golden = price is testing that golden level from below (a failing bounce = short setup)."""
    win = d.iloc[-lookback:] if len(d) >= lookback else d
    if len(win) < 10:
        return None
    hi = float(win["High"].max()); hi_pos = win["High"].idxmax()
    after = win.loc[hi_pos:]
    lo = float(after["Low"].min()) if len(after) >= 2 else float(win["Low"].min())
    rng = hi - lo
    if rng <= 0:
        return None
    fib618 = lo + 0.618 * rng
    price = float(d["Close"].iloc[-1])
    dist = (price - fib618) / fib618
    return {"swing_high": round(hi, 1), "swing_low": round(lo, 1), "fib_0618": round(fib618, 1),
            "at_golden": bool(abs(dist) <= 0.025 and price < hi), "dist_to_618_pct": round(dist * 100, 1)}


def scan(universe_n=90):
    uni = vs.SEED[:universe_n]
    data = yf.download([s + ".NS" for s in uni], period="1y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    if data is None or data.empty:
        return {"error": "no data"}
    winners, suspects, shorts = [], [], []
    accum = {h: [] for h in HZ}
    for s in uni:
        try:
            d = data[s + ".NS"][["Open", "High", "Low", "Close", "Volume"]].dropna()
        except Exception:
            continue
        if len(d) < 60:
            continue
        c = d["Close"]; r = c.pct_change()
        up = d["Volume"].where(r > 0, 0); dn = d["Volume"].where(r < 0, 0)
        ud = up.rolling(20).sum() / dn.rolling(20).sum().replace(0, np.nan)
        rvol = d["Volume"] / d["Volume"].rolling(20).mean()
        mom3 = c.pct_change(3)
        sig = (ud >= 1.3) & (mom3 > 0)
        # --- Pareto accumulation: forward returns after each historical signal ---
        sidx = np.where(sig.values)[0]
        cv = c.values
        for t in sidx:
            for h in HZ:
                if t + h < len(cv):
                    accum[h].append(cv[t + h] / cv[t] - 1)
        # --- current snapshot: evaluate LONG and SHORT setups ---
        ud_now = float(ud.iloc[-1]); mom3_now = float(mom3.iloc[-1]) if pd.notna(mom3.iloc[-1]) else 0.0
        long_now = bool(sig.iloc[-1])
        short_now = (ud_now <= 0.85 and mom3_now < 0)                # down-volume distribution
        fib = _fib(d)
        fib_short = bool(fib and fib["at_golden"] and ud_now < 1.0)  # bounce failing at 0.618 + weak volume
        if not (long_now or short_now or fib_short):
            continue
        rvol_now = float(rvol.iloc[-1]); rvol5 = float(rvol.iloc[-5:].mean())
        spike_conc = rvol_now / rvol5 if rvol5 > 0 else np.nan
        turnover = float((c * d["Volume"]).rolling(20).mean().iloc[-1]) / 1e7
        hi = (rvol > 2) & (r > 0); fwd3 = c.shift(-3) / c - 1
        rev3 = float(fwd3[hi].mean()) if hi.sum() >= 5 else np.nan
        atr = float(_atr(d["High"], d["Low"], c).iloc[-1])
        price = float(c.iloc[-1]); dvol = float(r.rolling(20).std().iloc[-1])
        base = {"sym": s, "name": NAMES.get(s, s), "ud": round(ud_now, 2), "rvol": round(rvol_now, 2),
                "turnover_cr": round(turnover), "price": round(price, 1)}
        if long_now:
            tag, why = _quality(turnover, spike_conc, rev3, ud_now)
            (winners if tag == "REAL" else suspects).append({**base,
                "spike_conc": round(spike_conc, 2) if not np.isnan(spike_conc) else None,
                "spike_fade_3d_pct": round(rev3 * 100, 2) if not np.isnan(rev3) else None,
                "quality": tag, "why": why, **_target(price, atr, dvol)})
        if short_now or fib_short:
            reasons = []
            if short_now: reasons.append(f"distribution (bad ud {round(ud_now,2)})")
            if fib_short: reasons.append(f"failing at 0.618 golden (~{fib['fib_0618']})")
            sq = "SUSPECT" if (turnover < 30 or (not np.isnan(spike_conc) and spike_conc > 2.2)) else "REAL"
            tgt = round(price - 2 * atr, 1)
            stp = round(fib["swing_high"], 1) if fib else round(price + atr, 1)
            dist = 2 * atr / price
            days = int(np.clip((dist / dvol) ** 2, 1, 60)) if (dvol and dvol > 0) else None
            cal = round(days * 7 / 5) if days else None
            date = (datetime.now(IST) + timedelta(days=cal)).strftime("%Y-%m-%d") if cal else None
            horizon = ("intraday/short" if days and days <= 2 else "swing (days)" if days and days <= 10
                       else "positional (weeks)") if days else None
            shorts.append({**base, "reasons": reasons, "at_golden": fib_short,
                           "fib_0618": fib["fib_0618"] if fib else None, "swing_high": fib["swing_high"] if fib else None,
                           "target_down": tgt, "stop": stp, "days_to_target": days, "target_date": date,
                           "horizon": horizon, "quality": sq})
    winners.sort(key=lambda x: -x["ud"]); suspects.sort(key=lambda x: -x["ud"]); shorts.sort(key=lambda x: x["ud"])
    # --- Pareto timeframe ---
    avg = {h: (float(np.mean(accum[h])) if accum[h] else 0.0) for h in HZ}
    mx = max(avg.values()) or 1e-9
    capture = {h: avg[h] / mx for h in HZ}
    knee = next((h for h in HZ if capture[h] >= 0.8), HZ[-1])
    pareto = {"avg_fwd_pct": {h: round(avg[h] * 100, 2) for h in HZ},
              "capture_pct": {h: round(capture[h] * 100) for h in HZ},
              "recommended_days": knee, "n_signals": len(accum[1]),
              "verdict": f"~80% of the move is captured within {knee} trading day(s) — hold ~{knee}d, not longer."}
    return {"asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"), "universe": universe_n,
            "winners": winners, "suspects": suspects, "shorts": shorts, "pareto": pareto,
            "note": "REAL = sustained, liquid, follows through. SUSPECT = one-day spike / illiquid / historically fades. Targets are diffusion-dated. Paper research; not advice."}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = scan()
    if r.get("error"):
        print(r["error"]); return
    print(f"\nSIGNAL QUALITY & HORIZON — {r['asof']} · universe {r['universe']}\n")
    print(f"  REAL WINNERS (sustained volume, dated targets):")
    for w in r["winners"][:10]:
        print(f"    {w['sym']:<12}{w['name'][:22]:<24} ud {w['ud']:<5} liq Rs{w['turnover_cr']}cr · "
              f"target {w['target']} by {w['target_date']} ({w['horizon']})")
    if not r["winners"]:
        print("    (none today)")
    print(f"\n  SUSPECT / LIKELY-MISLEADING (avoid):")
    for s in r["suspects"][:10]:
        print(f"    {s['sym']:<12}{s['name'][:22]:<24} ud {s['ud']:<5} -> {s['why']}")
    if not r["suspects"]:
        print("    (none today)")
    print(f"\n  SHORT CANDIDATES (bad ud / failing at 0.618 golden):")
    for sh in r.get("shorts", [])[:10]:
        g = " [@0.618]" if sh.get("at_golden") else ""
        print(f"    {sh['sym']:<12}{sh['name'][:20]:<22} ud {sh['ud']:<5}{g} -> {'; '.join(sh['reasons'])} · tgt {sh['target_down']} by {sh['target_date']}")
    if not r.get("shorts"):
        print("    (none today)")
    p = r["pareto"]
    print(f"\n  PARETO TIMEFRAME (avg fwd return after a BUYING signal, {p['n_signals']} signals):")
    for h in HZ:
        print(f"    {h:>2}d: {p['avg_fwd_pct'][h]:>6}%   capture {p['capture_pct'][h]}%")
    print(f"  -> {p['verdict']}")
    print("\n  " + r["note"])


if __name__ == "__main__":
    main()
