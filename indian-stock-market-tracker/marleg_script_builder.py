"""
Marle-G — STRATEGY SCRIPT BUILDER. Turn a playbook strategy + your mods into a
backtested, runnable script.

One declarative SPEC describes a long-basket strategy:
    base      momentum | volume_swing | mean_reversion | custom
    entry     filters (above 50/200dma, ud_min, rsi_max, mom_lookback)
    rank_by   momentum | ud | rsi_asc     (how candidates are ordered)
    top_n     conviction limit            rebalance_days / horizon_td
    sizing    risk_pct of capital, stop = k_atr x ATR(14), notional cap
    exits     ATR stop | time | optional take-profit

Everything runs through ONE generic simulator on the frozen DuckDB decade store, and
every result ships the honesty battery by default: net of current Indian costs, Sharpe,
PSR, maxDD, Calmar, win rate, yearly breakdown.

GUARDRAILS (learned the hard way, clamped — not optional):
    k_atr >= 1.5  (stops inside daily noise get harvested)
    risk_pct <= 2 (one loss must not matter)
    long-only paper; no leverage field exists (Kelly said no)

  python marleg_script_builder.py                    # demo: backtest the 3 base templates
  POST /api/builder/backtest  {spec}                 # test mods
  POST /api/builder/build     {spec}                 # write my_strategies/<name>.py
"""
import os, re, json, math, sys
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "my_strategies")
IST = timezone(timedelta(hours=5, minutes=30))
CAP = 100000.0
COST_RT = 33.0 / 1e4
TRADING = 252

TEMPLATES = {
    "momentum": {
        "name": "momentum", "base": "momentum", "rank_by": "momentum",
        "mom_lookback": 126, "above_sma": [50, 200], "ud_min": None, "rsi_max": None,
        "top_n": 6, "rebalance_days": 21, "horizon_td": 63,
        "risk_pct": 1.0, "k_atr": 2.5, "notional_cap_pct": 15, "take_profit_pct": None,
    },
    "volume_swing": {
        "name": "volume_swing", "base": "volume_swing", "rank_by": "ud",
        "mom_lookback": None, "above_sma": [50, 200], "ud_min": 1.3, "rsi_max": None,
        "top_n": 3, "rebalance_days": 15, "horizon_td": 21,
        "risk_pct": 1.0, "k_atr": 2.5, "notional_cap_pct": 15, "take_profit_pct": None,
    },
    "mean_reversion": {
        "name": "mean_reversion", "base": "mean_reversion", "rank_by": "rsi_asc",
        "mom_lookback": None, "above_sma": [200], "ud_min": None, "rsi_max": 32,
        "top_n": 4, "rebalance_days": 1, "horizon_td": 10,
        "risk_pct": 1.0, "k_atr": 2.5, "notional_cap_pct": 15, "take_profit_pct": None,
    },
}

LIMITS = {  # guardrails: (min, max) — clamped, with the lesson each encodes
    "mom_lookback": (21, 252), "ud_min": (1.0, 3.0), "rsi_max": (15, 45),
    "top_n": (1, 10), "rebalance_days": (1, 42), "horizon_td": (5, 63),
    "risk_pct": (0.25, 2.0),          # one trade must never matter much
    "k_atr": (1.5, 4.0),              # < 1.5 ATR = noise harvesting (TEJASNET lesson)
    "notional_cap_pct": (5, 25), "take_profit_pct": (5, 60),
}

_DATA = {}


def validate(spec):
    """Merge onto base template, clamp every mod into guardrails. Returns (spec, notes)."""
    base = spec.get("base", "momentum")
    if base not in TEMPLATES and base != "custom":
        base = "momentum"
    full = dict(TEMPLATES.get(base, TEMPLATES["momentum"]))
    notes = []
    for k, v in (spec or {}).items():
        if k in ("base",):
            full["base"] = v
            continue
        if k == "name":
            full["name"] = re.sub(r"[^a-z0-9_]", "_", str(v).lower())[:40] or "custom"
            continue
        if k == "above_sma":
            full["above_sma"] = [x for x in (v or []) if x in (50, 200)]
            continue
        if k in full:
            if v is None:
                full[k] = None
                continue
            try:
                v = float(v)
            except Exception:
                continue
            lo, hi = LIMITS.get(k, (-1e18, 1e18))
            cv = min(max(v, lo), hi)
            if cv != v:
                notes.append(f"{k} clamped {v} -> {cv} (guardrail)")
            full[k] = int(cv) if k in ("mom_lookback", "top_n", "rebalance_days",
                                       "horizon_td", "notional_cap_pct") else cv
    if full["rank_by"] not in ("momentum", "ud", "rsi_asc"):
        full["rank_by"] = "momentum"
    if full["rank_by"] == "momentum" and not full.get("mom_lookback"):
        full["mom_lookback"] = 126
    return full, notes


def _panel():
    if "C" not in _DATA:
        import marleg_datastore as ds
        ds.sync(verbose=False)
        C = ds.panel("close").ffill()
        keep = [c for c in C.columns if C[c].dropna().shape[0] > 400]
        _DATA["C"] = C[keep]
        _DATA["V"] = ds.panel("volume")[keep].reindex(C.index)
    return _DATA["C"], _DATA["V"]


def _signals(C, V, spec):
    rc = C.pct_change()
    sig = {"rc": rc.values, "px": C.values, "cols": list(C.columns), "index": C.index}
    pc = C.shift(1)
    hi = C.rolling(2).max()                      # placeholder; true H/L not needed for ATR proxy
    tr = pd.concat([(C - pc).abs()], axis=1)
    # ATR proxy from close-to-close TR (datastore high/low available; use them properly)
    import marleg_datastore as ds
    H = ds.panel("high")[C.columns].reindex(C.index)
    L = ds.panel("low")[C.columns].reindex(C.index)
    tr = pd.concat([(H - L).stack(), (H - pc).abs().stack(), (L - pc).abs().stack()],
                   axis=1).max(axis=1).unstack()
    sig["atr"] = tr.rolling(14).mean().values
    sig["s50"] = C.rolling(50).mean().values
    sig["s200"] = C.rolling(200).mean().values
    if spec.get("ud_min") or spec["rank_by"] == "ud":
        upv = V.where(rc > 0, 0.0).rolling(20).sum()
        dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
        sig["ud"] = (upv / dnv).values
    if spec.get("rsi_max") or spec["rank_by"] == "rsi_asc":
        d = C.diff()
        up = d.clip(lower=0).rolling(14).mean()
        dn = (-d.clip(upper=0)).rolling(14).mean().replace(0, np.nan)
        sig["rsi"] = (100 - 100 / (1 + up / dn)).values
    if spec.get("mom_lookback"):
        sig["mom"] = C.pct_change(int(spec["mom_lookback"])).values
    return sig


def simulate(spec):
    """Generic long-basket simulator on the decade store. Returns the honesty battery."""
    spec, notes = validate(spec)
    C, V = _panel()
    s = _signals(C, V, spec)
    px, atr = s["px"], s["atr"]
    n_days, n_sym = px.shape
    warm = max(210, (spec.get("mom_lookback") or 0) + 10)
    cash = CAP
    open_pos = {}                                 # j_sym -> dict
    closed = []
    equity = np.empty(n_days - warm - 1)
    rebal = max(1, int(spec["rebalance_days"]))
    for t, i in enumerate(range(warm, n_days - 1)):
        # exits on close i
        for js in list(open_pos):
            p = open_pos[js]
            x = px[i, js]
            if not np.isfinite(x):
                continue
            held = i - p["i0"]
            reason = None
            if x <= p["stop"]:
                reason = "stop"
            elif spec.get("take_profit_pct") and x >= p["entry"] * (1 + spec["take_profit_pct"] / 100):
                reason = "tp"
            elif held >= spec["horizon_td"]:
                reason = "time"
            if reason:
                cash += p["qty"] * x
                closed.append({"pnl": (x - p["entry"]) * p["qty"] - COST_RT * p["entry"] * p["qty"],
                               "days": held, "reason": reason})
                del open_pos[js]
        # entries
        if t % rebal == 0 and len(open_pos) < spec["top_n"]:
            scores = []
            for js in range(n_sym):
                if js in open_pos or not np.isfinite(px[i, js]) or not np.isfinite(atr[i, js]) or atr[i, js] <= 0:
                    continue
                if 50 in spec["above_sma"] and not (np.isfinite(s["s50"][i, js]) and px[i, js] > s["s50"][i, js]):
                    continue
                if 200 in spec["above_sma"] and not (np.isfinite(s["s200"][i, js]) and px[i, js] > s["s200"][i, js]):
                    continue
                if spec.get("ud_min") is not None:
                    u = s["ud"][i, js]
                    if not (np.isfinite(u) and u > spec["ud_min"]):
                        continue
                if spec.get("rsi_max") is not None:
                    rr = s["rsi"][i, js]
                    if not (np.isfinite(rr) and rr < spec["rsi_max"]):
                        continue
                if spec["rank_by"] == "momentum":
                    sc = s["mom"][i, js]
                elif spec["rank_by"] == "ud":
                    sc = s["ud"][i, js]
                else:
                    sc = -s["rsi"][i, js]
                if np.isfinite(sc):
                    scores.append((sc, js))
            scores.sort(reverse=True)
            for sc, js in scores[: spec["top_n"] - len(open_pos)]:
                entry = px[i + 1, js]
                if not np.isfinite(entry) or entry <= 0:
                    continue
                stop_dist = spec["k_atr"] * atr[i, js]
                qty = int((CAP * spec["risk_pct"] / 100) // stop_dist)
                notional = qty * entry
                cap_n = CAP * spec["notional_cap_pct"] / 100
                if notional > cap_n:
                    qty = int(cap_n // entry)
                    notional = qty * entry
                if qty < 1 or notional > cash:
                    continue
                cash -= notional + COST_RT * notional
                open_pos[js] = {"entry": float(entry), "qty": qty,
                                "stop": float(entry - stop_dist), "i0": i + 1}
        mtm = sum(p["qty"] * (px[i, js] if np.isfinite(px[i, js]) else p["entry"])
                  for js, p in open_pos.items())
        equity[t] = cash + mtm
    r = np.diff(equity) / equity[:-1]
    r = r[np.isfinite(r)]
    yrs = len(r) / TRADING
    eq = np.cumprod(1 + r)
    peak = np.maximum.accumulate(eq)
    mdd = float(((eq - peak) / peak).min() * 100)
    sharpe = float(r.mean() / (r.std(ddof=1) + 1e-12) * math.sqrt(TRADING))
    # PSR vs 0 (skew/kurt adjusted)
    try:
        from marleg_robust_bt import psr as _psr
        psr_v = round(float(_psr(r)), 3)
    except Exception:
        psr_v = None
    dates = s["index"][warm + 1:]
    yearly = {}
    rs = pd.Series(r, index=dates[1:len(r) + 1])
    for y, grp in rs.groupby(rs.index.year):
        yearly[int(y)] = round(float(((1 + grp).prod() - 1) * 100), 1)
    wins = [c for c in closed if c["pnl"] > 0]
    out = {"spec": spec, "guardrail_notes": notes,
           "from": str(dates[0].date()), "to": str(dates[-1].date()),
           "stats": {"cagr_pct": round(((eq[-1]) ** (1 / yrs) - 1) * 100, 1) if yrs > 0 else 0,
                     "sharpe": round(sharpe, 2), "psr": psr_v, "maxdd_pct": round(mdd, 1),
                     "calmar": round((((eq[-1]) ** (1 / yrs) - 1) * 100) / max(abs(mdd), 0.5), 2) if yrs > 0 else 0,
                     "trades": len(closed),
                     "win_pct": round(100 * len(wins) / max(len(closed), 1), 1),
                     "avg_hold_days": round(float(np.mean([c["days"] for c in closed])), 1) if closed else 0,
                     "stop_exit_pct": round(100 * sum(1 for c in closed if c["reason"] == "stop") / max(len(closed), 1))},
           "yearly_ret_pct": yearly,
           "note": "Net of ~33bps delivery RT; decade store; long-only paper. PSR>0.95 = survives."}
    return out


SCRIPT_TEMPLATE = '''"""
{name} — generated by the Marle-G Strategy Script Builder on {ts}.

Base: {base} with your mods. PAPER / RESEARCH ONLY — never places real orders.
Backtest at generation time (net of costs, decade store):
  CAGR {cagr}% · Sharpe {sharpe} · PSR {psr} · maxDD {maxdd}% · {trades} trades · win {win}%

  python {fname} --backtest      # re-run the honest backtest on current data
  python {fname} --today         # today's qualifying candidates + position sizes
"""
import sys, json, os
sys.path.insert(0, r"{here}")
import marleg_script_builder as B

SPEC = {spec_json}


def today():
    spec, _ = B.validate(SPEC)
    C, V = B._panel()
    s = B._signals(C, V, spec)
    i = len(C.index) - 1
    px, atr = s["px"], s["atr"]
    import numpy as np
    rows = []
    for js, sym in enumerate(s["cols"]):
        if not (np.isfinite(px[i, js]) and np.isfinite(atr[i, js]) and atr[i, js] > 0):
            continue
        if 50 in spec["above_sma"] and not px[i, js] > s["s50"][i, js]:
            continue
        if 200 in spec["above_sma"] and not px[i, js] > s["s200"][i, js]:
            continue
        if spec.get("ud_min") is not None and not (np.isfinite(s["ud"][i, js]) and s["ud"][i, js] > spec["ud_min"]):
            continue
        if spec.get("rsi_max") is not None and not (np.isfinite(s["rsi"][i, js]) and s["rsi"][i, js] < spec["rsi_max"]):
            continue
        sc = (s["mom"][i, js] if spec["rank_by"] == "momentum"
              else s["ud"][i, js] if spec["rank_by"] == "ud" else -s["rsi"][i, js])
        if np.isfinite(sc):
            rows.append((float(sc), sym, float(px[i, js]), float(atr[i, js])))
    rows.sort(reverse=True)
    print(f"{name} — candidates for next session (top {{spec['top_n']}}), {{C.index[-1].date()}}")
    for sc, sym, p, a in rows[: spec["top_n"]]:
        stop = round(p - spec["k_atr"] * a, 1)
        qty = int((B.CAP * spec["risk_pct"] / 100) // (spec["k_atr"] * a))
        print(f"  {{sym:<12}} px {{p:>9.1f}}  stop {{stop:>9.1f}}  qty {{qty:>5}}  score {{sc:+.2f}}")
    if not rows:
        print("  nothing qualifies today")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if "--today" in sys.argv:
        today()
    else:
        r = B.simulate(SPEC)
        print(json.dumps(r["stats"], indent=1))
        print("yearly:", r["yearly_ret_pct"])
'''


def build(spec):
    """Backtest the spec, then write a standalone script under my_strategies/."""
    res = simulate(spec)
    spec_v = res["spec"]
    os.makedirs(OUT_DIR, exist_ok=True)
    fname = f"{spec_v['name']}.py"
    st = res["stats"]
    import pprint
    code = SCRIPT_TEMPLATE.format(
        name=spec_v["name"], base=spec_v["base"], ts=datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
        cagr=st["cagr_pct"], sharpe=st["sharpe"], psr=st["psr"], maxdd=st["maxdd_pct"],
        trades=st["trades"], win=st["win_pct"], fname=fname, here=HERE,
        spec_json=pprint.pformat(spec_v, indent=1, width=88))
    path = os.path.join(OUT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(code)
    res["script_path"] = path
    res["run_hint"] = f"python my_strategies/{fname} --today"
    return res


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for base in TEMPLATES:
        r = simulate({"base": base})
        st = r["stats"]
        print(f"{base:<16} CAGR {st['cagr_pct']:>6}% · Sharpe {st['sharpe']:>5} · PSR {st['psr']} · "
              f"maxDD {st['maxdd_pct']:>6}% · {st['trades']} trades · win {st['win_pct']}% · "
              f"hold {st['avg_hold_days']}d")


if __name__ == "__main__":
    main()
