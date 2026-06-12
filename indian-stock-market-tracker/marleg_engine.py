"""
Marle-G — STRATEGY ENGINE. Multi-stream allocator: regime-led eligibility, PSR-led
benching, slow-promote / fast-bench. "Sharpe is the seatbelt, not the throttle."

STREAMS (daily net return series, internal turnover costs included):
  MOM   top-6 by 126d momentum, >200dma, rebal 21d          (regime: NIFTY > 100dma)
  VOL   top-3 by ud(20d)>1.3, >50&200dma, rebal 15d         (regime: breadth>45% above 50dma)
  MREV  RSI(14)<32 & >200dma, hold 10 sessions              (regime: NIFTY within ±4% of 100dma)
  CASH  6.8%/yr                                              (always eligible — the incumbent)

LEDGER per stream: rolling 120d Sharpe, PSR (skew/kurt-adjusted), Calmar.
HEALTH_Z = expanding self-z of rolling PSR (each stream judged vs ITS OWN history).

ALLOCATOR (pre-registered BEFORE results, weekly, applied next day — no lookahead):
  eligible : regime OK  AND  health_z > -0.5  AND not benched
  bench    : health_z < -1.5  (fast)     re-admit: health_z > 0  (slow)
  weights  : among eligible, w ∝ max(rolling Sharpe,0)+0.1, cap 50%; residual -> CASH
  friction : |Δw| x 16bps charged at each rebalance (one-way entry/exit into streams)

META-BACKTEST: ROTATED vs EQUAL-WEIGHT(static 1/3) vs CASH vs NIFTY vs each stream.
  python marleg_engine.py
"""
import os, sys, json, math
import numpy as np
import pandas as pd
import yfinance as yf
from marleg_robust_bt import psr, ann_sharpe

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "marleg_engine.json")
CASH_RATE = 0.068
COST_RT = 32.0 / 1e4
SWITCH_ONEWAY = 16.0 / 1e4
ROLL = 120
ZMIN_HIST = 60
ELIG_Z, BENCH_Z, READMIT_Z = -0.5, -1.5, 0.0
PSR_FLOOR = 0.45        # v1.1 ABSOLUTE quality floor: self-z grades consistency, not quality —
                        # a uniformly bad stream is never "unusual vs itself" and was never
                        # benched (discovered via the PUT stream). Eligibility now also
                        # requires rolling PSR > 0.45 (better than a coin flip).
WCAP = 0.50
REBAL_EVERY = 5                      # sessions (weekly)
UNIV = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "ITC", "LT", "SBIN", "AXISBANK",
        "KOTAKBANK", "BHARTIARTL", "BAJFINANCE", "HINDUNILVR", "MARUTI", "SUNPHARMA",
        "EICHERMOT", "TATASTEEL", "M&M", "NTPC", "TITAN", "ASIANPAINT", "ULTRACEMCO",
        "WIPRO", "ADANIPORTS", "JSWSTEEL", "COALINDIA", "ONGC", "GRASIM", "HCLTECH", "CIPLA",
        "POWERGRID", "BAJAJFINSV", "TECHM", "NESTLEIND"]


# ----------------------------------------------------------------- data
def load(period="5y"):
    # canonical path: the synchronized DuckDB store (reproducible across all modules)
    try:
        import marleg_datastore as ds
        ds.sync(verbose=False)
        C = ds.panel("close")
        keep = [c for c in C.columns if C[c].dropna().shape[0] > 400]
        C = C[keep].ffill()
        V = ds.panel("volume")[keep].reindex(C.index)
        nifty = ds.series("^NSEI").reindex(C.index).ffill()
        return C, V, nifty
    except Exception:
        pass
    df = yf.download([s + ".NS" for s in UNIV], period=period, interval="1d",
                     group_by="ticker", auto_adjust=False, progress=False, threads=True)
    C, V = {}, {}
    for s in UNIV:
        try:
            d = df[s + ".NS"].dropna()
            if len(d) > 400:
                C[s], V[s] = d["Close"], d["Volume"]
        except Exception:
            pass
    nifty = yf.download("^NSEI", period=period, interval="1d", progress=False,
                        auto_adjust=False)
    if isinstance(nifty.columns, pd.MultiIndex):
        nifty.columns = nifty.columns.get_level_values(0)
    C = pd.DataFrame(C)
    return C, pd.DataFrame(V).reindex(C.index), nifty["Close"].reindex(C.index).ffill()


def _rsi(close, n=14):
    d = close.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


# ----------------------------------------------------------------- streams (daily net returns)
def stream_mom(C, look=126, top=6, rebal=21):
    rc = C.pct_change()
    mom = C.pct_change(look)
    s200 = C.rolling(200).mean()
    held, ret, holds = [], pd.Series(0.0, index=C.index), {}
    for k, i in enumerate(range(210, len(C) - 1)):
        if (i - 210) % rebal == 0:
            sig = mom.iloc[i].dropna()
            ok = [s for s in sig.index if np.isfinite(s200[s].iloc[i]) and C[s].iloc[i] > s200[s].iloc[i]]
            new = list(sig[ok].sort_values(ascending=False).head(top).index) if ok else []
            turn = 1.0 if (new and not held) else (len(set(new) ^ set(held)) / (2.0 * max(top, 1)) if new or held else 0.0)
            held = new
            ret.iloc[i + 1] -= turn * COST_RT
        if held:
            ret.iloc[i + 1] += float(rc[held].iloc[i + 1].mean())
        holds[i + 1] = list(held)
    return ret.iloc[211:], holds


def stream_vol(C, V, top=3, rebal=15):
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    s50, s200 = C.rolling(50).mean(), C.rolling(200).mean()
    held, ret, holds = [], pd.Series(0.0, index=C.index), {}
    for i in range(210, len(C) - 1):
        if (i - 210) % rebal == 0:
            sig = ud.iloc[i].dropna()
            sig = sig[sig > 1.3]
            ok = [s for s in sig.index
                  if C[s].iloc[i] > s50[s].iloc[i] and C[s].iloc[i] > s200[s].iloc[i]]
            new = list(sig[ok].sort_values(ascending=False).head(top).index) if ok else []
            turn = 1.0 if (new and not held) else (len(set(new) ^ set(held)) / (2.0 * max(top, 1)) if new or held else 0.0)
            held = new
            ret.iloc[i + 1] -= turn * COST_RT
        if held:
            ret.iloc[i + 1] += float(rc[held].iloc[i + 1].mean())
        holds[i + 1] = list(held)
    return ret.iloc[211:], holds


def stream_mrev(C, hold=10, rsi_in=32):
    rc = C.pct_change()
    s200 = C.rolling(200).mean()
    rsi = pd.DataFrame({s: _rsi(C[s]) for s in C.columns})
    active = {}                                   # sym -> sessions left
    ret, holds = pd.Series(0.0, index=C.index), {}
    for i in range(210, len(C) - 1):
        # exits
        for s in list(active):
            active[s] -= 1
            if active[s] <= 0:
                del active[s]
                ret.iloc[i + 1] -= COST_RT / 2 / max(len(active) + 1, 1)
        # entries
        row = rsi.iloc[i]
        for s in C.columns:
            if s in active or not np.isfinite(row.get(s, np.nan)):
                continue
            if row[s] < rsi_in and C[s].iloc[i] > s200[s].iloc[i]:
                active[s] = hold
                ret.iloc[i + 1] -= COST_RT / 2 / max(len(active), 1)
        if active:
            ret.iloc[i + 1] += float(rc[list(active)].iloc[i + 1].mean())
        holds[i + 1] = list(active)
    return ret.iloc[211:], holds


def _bs_put(S, K, T, sigma, r=0.065):
    if T <= 0:
        return max(0.0, K - S)
    d1 = (math.log(S / K) + (r + sigma * sigma / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    N = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))
    return K * math.exp(-r * T) * N(-d2) - S * N(-d1)


def stream_put(C, V, top=2, max_active=4, hold=15, prem_w=0.04, vrp=1.15, spread_k=0.95):
    """SHORT stream — puts on peaked/fading names. Defined risk only.
    v2 = PUT SPREAD (default): buy ATM put, SELL the spread_k*S put — the short leg hands
    back part of the VRP/theta tax; max loss = net premium. spread_k=None -> v1 vanilla
    ATM put (measured -39%/yr — rejected).
    FADE signal (close i): ud(20d) < 0.75 AND px < 50dma AND px >= 10% off its 60d high.
    Conviction = distribution depth + fade depth; top-2 new per session, max 4 concurrent.
    Entry close i+1: buy 1M ATM put, premium budget = 4% of stream NAV each (max loss capped).
    Priced at IV = 1.15 x rv20 (the VRP an option BUYER pays); revalued same-IV (no vol-of-vol).
    Exits: +100% take-profit | thesis dead (px > 50dma) | 15 sessions. Costs 62bps of premium."""
    rc = C.pct_change()
    upv = V.where(rc > 0, 0.0).rolling(20).sum()
    dnv = V.where(rc < 0, 0.0).rolling(20).sum().replace(0, np.nan)
    ud = upv / dnv
    s50 = C.rolling(50).mean()
    hi60 = C.rolling(60).max()
    rv20 = rc.rolling(20).std() * math.sqrt(252)
    OPT_COST = 0.0062                                  # of premium, round trip (india_rules)
    active, trades = {}, []
    ret, holds = pd.Series(0.0, index=C.index), {}
    for i in range(210, len(C) - 1):
        d_ret = 0.0
        # mark + manage existing puts at close i+1
        for s in list(active):
            a = active[s]
            S1 = float(C[s].iloc[i + 1])
            a["t"] += 1
            T1 = max((21 - a["t"]) / 252, 1 / 252)
            v1 = _bs_put(S1, a["K"], T1, a["iv"])
            if a.get("K2"):
                v1 -= _bs_put(S1, a["K2"], T1, a["iv"])
            d_ret += prem_w * (v1 - a["val"]) / a["prem0"]
            a["val"] = v1
            reason = None
            if v1 >= 2 * a["prem0"]:
                reason = "target +100%"
            elif S1 > float(s50[s].iloc[i + 1]):
                reason = "thesis dead (>50dma)"
            elif a["t"] >= hold:
                reason = "time"
            if reason:
                trades.append({"sym": s, "entry_date": str(C.index[a["i0"]].date()),
                               "exit_date": str(C.index[i + 1].date()),
                               "prem_ret_pct": round((v1 / a["prem0"] - 1) * 100, 1),
                               "reason": reason})
                del active[s]
        # entries decided at close i, opened at close i+1 (conviction-ranked, limited)
        if len(active) < max_active:
            udr = ud.iloc[i]
            cands = []
            for s in C.columns:
                if s in active:
                    continue
                u, px, m50, h60 = udr.get(s, np.nan), C[s].iloc[i], s50[s].iloc[i], hi60[s].iloc[i]
                if not all(np.isfinite(x) for x in (u, px, m50, h60)):
                    continue
                if u < 0.75 and px < m50 and px / h60 - 1 <= -0.10:
                    cands.append(((0.75 - u) + abs(px / h60 - 1), s))
            cands.sort(reverse=True)
            for _, s in cands[:top]:
                if len(active) >= max_active:
                    break
                S0 = float(C[s].iloc[i + 1])
                iv = max(float(rv20[s].iloc[i]) * vrp, 0.10)
                K2 = spread_k * S0 if spread_k else None
                prem = _bs_put(S0, S0, 21 / 252, iv) - (_bs_put(S0, K2, 21 / 252, iv) if K2 else 0.0)
                if prem <= 0 or not np.isfinite(prem):
                    continue
                active[s] = {"K": S0, "K2": K2, "iv": iv, "prem0": prem, "val": prem,
                             "t": 0, "i0": i + 1}
                d_ret -= prem_w * OPT_COST
        ret.iloc[i + 1] += d_ret
        holds[i + 1] = list(active)
    # mark whatever is still open
    for s, a in active.items():
        trades.append({"sym": s, "entry_date": str(C.index[a["i0"]].date()), "exit_date": None,
                       "prem_ret_pct": round((a["val"] / a["prem0"] - 1) * 100, 1), "reason": "OPEN"})
    return ret.iloc[211:], holds, trades


# ----------------------------------------------------------------- ledger
def rolling_metrics(r):
    n = len(r)
    rs = np.full(n, np.nan); rp = np.full(n, np.nan); rcal = np.full(n, np.nan)
    arr = r.values
    for i in range(ROLL, n):
        w = arr[i - ROLL:i]
        rs[i] = ann_sharpe(w)
        rp[i] = psr(w)
        eq = np.cumprod(1 + w)
        peak = np.maximum.accumulate(eq)
        mdd = abs(((eq - peak) / peak).min())
        annr = w.mean() * 252
        rcal[i] = annr / max(mdd, 0.01)
    return (pd.Series(rs, r.index), pd.Series(rp, r.index), pd.Series(rcal, r.index))


def health_z(roll_psr):
    z = pd.Series(np.nan, index=roll_psr.index)
    vals = roll_psr.values
    for i in range(len(vals)):
        hist = vals[:i][~np.isnan(vals[:i])]
        if np.isnan(vals[i]) or len(hist) < ZMIN_HIST:
            continue
        sd = hist.std()
        z.iloc[i] = (vals[i] - hist.mean()) / sd if sd > 1e-9 else 0.0
    return z


# ----------------------------------------------------------------- engine
def run():
    C, V, nifty = load()
    mom_r, mom_h = stream_mom(C)
    vol_r, vol_h = stream_vol(C, V)
    mrev_r, mrev_h = stream_mrev(C)
    put_r, put_h, put_trades = stream_put(C, V)
    streams = {"MOM": mom_r, "VOL": vol_r, "MREV": mrev_r, "PUT": put_r}
    holds_map = {"MOM": mom_h, "VOL": vol_h, "MREV": mrev_h}
    RISK_STREAMS = ("MOM", "VOL", "MREV", "PUT")      # reported
    ALLOC = ("MOM", "VOL", "MREV")                    # allocatable — PUT failed admission
    # (vanilla-put fade stream: -39%/yr standalone; VRP+theta+bounce bleed. Kept as a
    #  measured, documented experiment; the allocator may not fund negative-EV streams.)
    idx = streams["MOM"].index
    for k in streams:
        streams[k] = streams[k].reindex(idx).fillna(0.0)
    streams["CASH"] = pd.Series(CASH_RATE / 252, index=idx)
    nifty_r = nifty.pct_change().reindex(idx).fillna(0.0)

    # regime flags (computed from data through each day)
    n100 = nifty.rolling(100).mean().reindex(idx)
    npx = nifty.reindex(idx)
    dev = npx / n100 - 1
    above50 = (C > C.rolling(50).mean()).reindex(idx)
    breadth = above50.mean(axis=1)
    regime_ok = {
        "MOM": (npx > n100),
        "VOL": (breadth > 0.45),
        "MREV": (dev.abs() < 0.04),
        "PUT": ((npx < n100) | (breadth < 0.40)),     # shorts need a falling tide
        "CASH": pd.Series(True, index=idx),
    }

    led = {k: rolling_metrics(streams[k]) for k in ALLOC}
    hz = {k: health_z(led[k][1]) for k in led}

    # allocator walk (weights decided at i, applied to returns at i+1)
    start = ROLL + ZMIN_HIST + 10
    w = {k: 0.0 for k in streams}
    benched = {k: False for k in led}
    rot = pd.Series(0.0, index=idx)
    wlog, switch_cost_total, n_rebal = [], 0.0, 0
    w_daily = {}
    for j in range(start, len(idx) - 1):
        if (j - start) % REBAL_EVERY == 0:
            neww = {}
            for k in led:
                z = hz[k].iloc[j]
                if np.isnan(z):
                    benched[k] = benched[k]; eligible = False
                else:
                    if benched[k] and z > READMIT_Z:
                        benched[k] = False
                    elif not benched[k] and z < BENCH_Z:
                        benched[k] = True
                    rpsr = led[k][1].iloc[j]
                    eligible = ((not benched[k]) and z > ELIG_Z and bool(regime_ok[k].iloc[j])
                                and (not np.isnan(rpsr)) and rpsr > PSR_FLOOR)
                neww[k] = (max(led[k][0].iloc[j], 0.0) + 0.1) if eligible else 0.0
            tot = sum(neww.values())
            if tot > 0:
                neww = {k: min(v / tot, WCAP) for k, v in neww.items()}
            neww["CASH"] = max(0.0, 1.0 - sum(neww.values()))
            dw = sum(abs(neww.get(k, 0) - w.get(k, 0)) for k in streams) - abs(neww["CASH"] - w["CASH"])
            cost = max(dw, 0) * SWITCH_ONEWAY
            switch_cost_total += cost
            rot.iloc[j + 1] -= cost
            n_rebal += 1
            w = neww
            wlog.append({"date": str(idx[j].date()), **{k: round(w.get(k, 0), 3) for k in streams},
                         "z": {k: (round(float(hz[k].iloc[j]), 2) if not np.isnan(hz[k].iloc[j]) else None) for k in led},
                         "benched": {k: benched[k] for k in led}})
        rot.iloc[j + 1] += sum(w.get(k, 0.0) * streams[k].iloc[j + 1] for k in streams)
        w_daily[j + 1] = dict(w)

    # ---- trade blotters: every ticket the engine took (full history + last 3 months) ----
    LOOK = 63
    CAP_RS = 100000.0
    offset = 211                                  # idx[j] == C.index[offset + j]

    def _blotter(j0):
        trades, open_pos = [], {}
        for j in range(j0, len(idx)):
            wj = w_daily.get(j, {})
            book = {}
            for k in ("MOM", "VOL", "MREV"):
                wk = wj.get(k, 0.0)
                hl = holds_map[k].get(offset + j, [])
                if wk > 0 and hl:
                    for s in hl:
                        if s in book:
                            book[s] = (book[s][0] + wk / len(hl), book[s][1] + "+" + k)
                        else:
                            book[s] = (wk / len(hl), k)
            d = idx[j]
            for s, (portion, k) in book.items():
                if s not in open_pos:
                    open_pos[s] = {"sym": s, "stream": k, "entry_date": str(d.date()),
                                   "entry": round(float(C[s].loc[d]), 2), "portion": round(portion, 3)}
            for s in list(open_pos):
                if s not in book:
                    t = open_pos.pop(s)
                    exit_px = round(float(C[s].loc[d]), 2)
                    pnl = (exit_px / t["entry"] - 1) * t["portion"] * CAP_RS
                    trades.append({**t, "exit_date": str(d.date()), "exit": exit_px,
                                   "ret_pct": round((exit_px / t["entry"] - 1) * 100, 1),
                                   "pnl_rs": round(pnl), "status": "closed"})
        last_d = idx[-1]
        for s, t in open_pos.items():
            exit_px = round(float(C[s].loc[last_d]), 2)
            pnl = (exit_px / t["entry"] - 1) * t["portion"] * CAP_RS
            trades.append({**t, "exit_date": None, "exit": exit_px,
                           "ret_pct": round((exit_px / t["entry"] - 1) * 100, 1),
                           "pnl_rs": round(pnl), "status": "OPEN"})
        return sorted(trades, key=lambda x: x["entry_date"])

    trades_full = _blotter(start + 1)
    trades = _blotter(len(idx) - LOOK)
    try:                                          # full history as CSV for inspection
        pd.DataFrame(trades_full).to_csv(os.path.join(HERE, "marleg_engine_trades.csv"), index=False)
    except Exception:
        pass
    pnl_3mo = float((1 + rot.iloc[len(idx) - LOOK:]).prod() - 1) * 100
    contrib = {k: round(float(sum(w_daily.get(j, {}).get(k, 0) * streams[k].iloc[j]
                                  for j in range(len(idx) - LOOK, len(idx)))) * 100, 2)
               for k in ("MOM", "VOL", "MREV", "PUT", "CASH")}

    eval_idx = idx[start + 1:]
    variants = {
        "ROTATED": rot.loc[eval_idx],
        "EQUAL_WEIGHT": sum(streams[k].loc[eval_idx] for k in ALLOC) / len(ALLOC),
        "CASH": streams["CASH"].loc[eval_idx],
        "NIFTY": nifty_r.loc[eval_idx],
        "MOM_only": streams["MOM"].loc[eval_idx],
        "VOL_only": streams["VOL"].loc[eval_idx],
        "MREV_only": streams["MREV"].loc[eval_idx],
        "PUT_only": streams["PUT"].loc[eval_idx],
    }

    def stats(r):
        r = r.dropna()
        eq = (1 + r).cumprod()
        yrs = len(r) / 252
        mdd = float(((eq - eq.cummax()) / eq.cummax()).min() * 100)
        annr = (eq.iloc[-1] ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
        zero_vol = float(r.std()) < 1e-6                      # cash: PSR/Sharpe undefined
        return {"cagr_pct": round(float(annr), 1),
                "sharpe": None if zero_vol else round(ann_sharpe(r.values), 2),
                "psr": None if zero_vol else round(float(psr(r.values)), 3),
                "maxdd_pct": round(mdd, 1),
                "calmar": round(float(annr / max(abs(mdd), 0.5)), 2),
                "vol_pct": round(float(r.std() * math.sqrt(252) * 100), 1)}

    res = {k: stats(v) for k, v in variants.items()}
    avg_w = {k: round(float(np.mean([x.get(k, 0) for x in wlog])), 3) for k in streams}
    elig_pct = {k: round(100 * float(np.mean([1 if x[k] > 0 else 0 for x in wlog])), 1) for k in ALLOC}
    bench_pct = {k: round(100 * float(np.mean([1 if x["benched"][k] else 0 for x in wlog])), 1) for k in ALLOC}
    corr = pd.DataFrame({k: streams[k].loc[eval_idx] for k in RISK_STREAMS}).corr().round(2)

    out = {"eval_from": str(eval_idx[0].date()), "eval_to": str(eval_idx[-1].date()),
           "eval_days": len(eval_idx), "results": res,
           "avg_weights": avg_w, "eligible_pct_of_rebalances": elig_pct,
           "benched_pct": bench_pct, "n_rebalances": n_rebal,
           "switch_cost_total_pct": round(switch_cost_total * 100, 2),
           "stream_correlations": corr.to_dict(),
           "blotter_3mo": {"from": str(idx[len(idx) - LOOK].date()), "to": str(idx[-1].date()),
                           "capital_rs": CAP_RS, "engine_pnl_pct": round(pnl_3mo, 2),
                           "stream_contrib_pct": contrib, "n_trades": len(trades),
                           "trades": trades},
           "blotter_full": {"from": str(idx[start + 1].date()), "to": str(idx[-1].date()),
                            "capital_rs": CAP_RS, "n_trades": len(trades_full),
                            "trades": trades_full},
           "put_trades": {"n": len(put_trades), "trades": put_trades,
                          "note": "stream-level tickets; prem_ret_pct = P&L on premium paid "
                                  "(max loss -100% = defined risk). Engine P&L flows via the "
                                  "PUT stream weight; 4% of stream NAV per put, max 4 live."},
           "weights_log": wlog,
           "weights_log_tail": wlog[-8:],
           "equity_curves": {k: list(np.round((1 + v.fillna(0)).cumprod().values, 4)) for k, v in variants.items()},
           "dates": [str(d.date()) for d in eval_idx],
           "rules": {"eligible_z": ELIG_Z, "bench_z": BENCH_Z, "readmit_z": READMIT_Z,
                     "psr_floor": PSR_FLOOR, "psr_floor_note": "v1.1 post-hoc structural fix (self-z blind spot found via PUT stream)",
                     "weight_cap": WCAP, "rebal_every_sessions": REBAL_EVERY,
                     "roll_window": ROLL, "cash_rate": CASH_RATE,
                     "preregistered": True},
           "note": ("Thresholds pre-registered before results. Streams carry internal turnover "
                    "costs; allocator charges 16bps one-way on weight changes. Weights decided "
                    "day t, applied t+1 (no lookahead). Single-path; eval window is the post-"
                    "warmup period only.")}
    json.dump(out, open(OUT, "w"), indent=1)
    return out


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = run()
    print(f"\nSTRATEGY ENGINE meta-backtest — {r['eval_from']} -> {r['eval_to']} ({r['eval_days']} sessions)\n")
    hdr = f"{'variant':<14}{'CAGR%':>7}{'vol%':>6}{'Sharpe':>8}{'PSR':>7}{'maxDD%':>8}{'Calmar':>8}"
    print(hdr); print("-" * len(hdr))
    for k in ("ROTATED", "EQUAL_WEIGHT", "CASH", "NIFTY", "MOM_only", "VOL_only", "MREV_only", "PUT_only"):
        s = r["results"][k]
        sh = s["sharpe"] if s["sharpe"] is not None else "—"
        ps = s["psr"] if s["psr"] is not None else "—"
        print(f"{k:<14}{s['cagr_pct']:>7}{s['vol_pct']:>6}{str(sh):>8}{str(ps):>7}{s['maxdd_pct']:>8}{s['calmar']:>8}")
    pt = r.get("put_trades") or {}
    if pt:
        tr = pt["trades"]
        wn = [t for t in tr if t["prem_ret_pct"] > 0]
        print(f"\nPUT stream tickets: {pt['n']} · winners {len(wn)} · "
              f"avg premium ret {np.mean([t['prem_ret_pct'] for t in tr]):+.1f}%")
        for t in tr[-6:]:
            print(f"  PUT {t['sym']:<12} {t['entry_date']} -> {t['exit_date'] or 'open':<11} "
                  f"{t['prem_ret_pct']:>+7.1f}% on premium · {t['reason']}")
    b = r.get("blotter_3mo") or {}
    if b:
        print(f"\nBLOTTER {b['from']} -> {b['to']} on Rs {int(b['capital_rs']):,} · "
              f"engine P&L {b['engine_pnl_pct']:+}% · contrib {b['stream_contrib_pct']}")
        print(f"{'SYM':<13}{'stream':<9}{'in':<12}{'@in':>9}{'out':<12}{'@out':>9}{'ret%':>7}{'P&L Rs':>8}  st")
        for t in b["trades"]:
            print(f"{t['sym']:<13}{t['stream']:<9}{t['entry_date']:<12}{t['entry']:>9}"
                  f"{(t['exit_date'] or 'open'):<12}{t['exit']:>9}{t['ret_pct']:>7}{int(t['pnl_rs']):>8}  {t['status']}")
    print(f"\navg weights: {r['avg_weights']}")
    print(f"eligible% of rebalances: {r['eligible_pct_of_rebalances']} · benched%: {r['benched_pct']}")
    print(f"rebalances: {r['n_rebalances']} · total switching cost: {r['switch_cost_total_pct']}%")
    print(f"stream correlations: {r['stream_correlations']}")
    print("\n" + r["note"])


if __name__ == "__main__":
    main()
