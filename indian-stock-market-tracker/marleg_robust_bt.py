"""
Marle-G — ROBUST BACKTEST BATTERY.

The pod's strategy backtests (volume day-book, the option tournament, cascade
event-study) report GROSS, IN-SAMPLE, single-path Sharpes. That flatters every
result. This module stress-tests the headline edges the honest way:

  1. COSTS         charge realistic Indian costs, TURNOVER-AWARE. Daily-flatten
                   strategies (overnight/intraday) pay a full round-trip every day;
                   a positional swing pays only when the basket actually changes.
  2. SUBPERIODS    split history into k slices; is the edge stable, or one lucky run?
  3. BOOTSTRAP CI  block-resample daily returns -> 95% CI on the Sharpe. Is 0 inside?
  4. PSR           Probabilistic Sharpe Ratio: P(true SR > 0) given skew & kurtosis.
  5. DSR           Deflated Sharpe Ratio (Bailey & Lopez de Prado, 2014): corrects the
                   winner's Sharpe for the NUMBER of strategies tried. Picking the best
                   of N inflates Sharpe; DSR is the selection-adjusted probability.

A strategy is only believed if it survives ALL of these net of costs.
Paper / research only.

  python marleg_robust_bt.py
"""
import math, sys, os, json
from statistics import NormalDist
import numpy as np
import pandas as pd
import yfinance as yf

ND = NormalDist()
GAMMA = 0.5772156649015329          # Euler-Mascheroni
TRADING = 252
ROUNDTRIP_BPS = 33.0                # delivery STT 20 + charges 3 + slippage ~10
INTRADAY_BPS = 12.0                 # intraday STT 2.5 + charges + slippage

UNIVERSE = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "ITC", "LT", "SBIN",
            "AXISBANK", "KOTAKBANK", "BHARTIARTL", "BAJFINANCE", "HINDUNILVR", "MARUTI",
            "SUNPHARMA", "EICHERMOT", "TATASTEEL", "M&M", "NTPC", "POWERGRID", "TITAN",
            "ASIANPAINT", "ULTRACEMCO", "WIPRO", "ADANIPORTS", "JSWSTEEL", "COALINDIA",
            "ONGC", "GRASIM", "HCLTECH", "NESTLEIND", "TECHM", "BAJAJFINSV", "CIPLA"]


# ----------------------------------------------------------------- metrics
def _clean(r):
    r = np.asarray(r, float)
    return r[~np.isnan(r)]


def ann_sharpe(r):
    r = _clean(r)
    if len(r) < 2 or r.std(ddof=1) == 0:
        return 0.0
    return float(r.mean() / r.std(ddof=1) * math.sqrt(TRADING))


def maxdd_pct(r):
    r = _clean(r)
    if not len(r):
        return 0.0
    eq = np.cumprod(1 + r)
    peak = np.maximum.accumulate(eq)
    return float(((eq - peak) / peak).min() * 100)


def psr(returns, sr_star_per=0.0):
    """Probabilistic Sharpe Ratio: P(true per-period SR > sr_star), adjusting for
    non-normal returns (Bailey & Lopez de Prado)."""
    r = _clean(returns)
    T = len(r)
    if T < 8 or r.std(ddof=1) == 0:
        return float("nan")
    sr = r.mean() / r.std(ddof=1)                        # per-period Sharpe
    s = pd.Series(r)
    g3 = float(s.skew())
    g4 = float(s.kurt()) + 3.0                            # pandas .kurt is EXCESS -> add 3
    denom = math.sqrt(max(1e-12, 1 - g3 * sr + ((g4 - 1) / 4.0) * sr * sr))
    z = ((sr - sr_star_per) * math.sqrt(T - 1)) / denom
    return float(ND.cdf(z))


def expected_max_sharpe(sr_variance, n_trials):
    """E[max SR] across n independent trials whose per-period Sharpes have the given
    variance — the Sharpe you'd expect from luck alone after trying n strategies."""
    if n_trials < 2 or sr_variance <= 0:
        return 0.0
    a = ND.inv_cdf(1 - 1.0 / n_trials)
    b = ND.inv_cdf(1 - 1.0 / (n_trials * math.e))
    return math.sqrt(sr_variance) * ((1 - GAMMA) * a + GAMMA * b)


def dsr(returns, trial_sharpes_per, n_trials):
    """Deflated Sharpe Ratio = PSR evaluated against the expected-max-Sharpe benchmark
    implied by n_trials strategies. < 0.95 => not significant after selection bias."""
    v = float(np.var(np.asarray(trial_sharpes_per, float), ddof=1)) if len(trial_sharpes_per) > 1 else 0.0
    sr_star = expected_max_sharpe(v, n_trials)
    return psr(returns, sr_star), sr_star


def bootstrap_sharpe_ci(returns, n_boot=1500, block=5, seed=12345):
    """Block bootstrap 95% CI on the ANNUALIZED Sharpe (preserves short autocorrelation)."""
    r = _clean(returns)
    T = len(r)
    if T < 25:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    nblocks = int(math.ceil(T / block))
    out = np.empty(n_boot)
    for k in range(n_boot):
        starts = rng.integers(0, T, nblocks)
        idx = np.concatenate([np.arange(s, s + block) for s in starts]) % T
        out[k] = ann_sharpe(r[idx[:T]])
    lo, hi = np.percentile(out, [2.5, 97.5])
    return float(lo), float(hi)


def subperiod_sharpes(r, k=4):
    r = _clean(r)
    if len(r) < k * 10:
        return []
    return [round(ann_sharpe(s), 2) for s in np.array_split(r, k)]


# ----------------------------------------------------------------- strategy returns
def _load_prices(period="2y"):
    df = yf.download([s + ".NS" for s in UNIVERSE], period=period, interval="1d",
                     group_by="ticker", auto_adjust=False, progress=False, threads=True)
    O, C, V = {}, {}, {}
    for s in UNIVERSE:
        try:
            d = df[s + ".NS"]
            if len(d["Close"].dropna()) > 200:
                O[s], C[s], V[s] = d["Open"], d["Close"], d["Volume"]
        except Exception:
            pass
    return pd.DataFrame(O), pd.DataFrame(C), pd.DataFrame(V)


def build_strategies(period="2y", topn=8, lookback=20, swing_hold=15):
    """Return {name: (gross_daily_returns, daily_cost_array)} for the headline volume
    strategies + a positional swing + a buy-hold benchmark. Signal = volume-conviction
    (up-day volume / down-day volume over `lookback`)."""
    O, C, V = _load_prices(period)
    if C.shape[1] < topn + 2:
        return None
    ret_c = C.pct_change()
    upv = V.where(ret_c > 0, 0.0).rolling(lookback).sum()
    dnv = V.where(ret_c < 0, 0.0).rolling(lookback).sum().replace(0, np.nan)
    ud = upv / dnv
    n = len(C.index)

    overnight, intraday, full, buyhold, days = [], [], [], [], []
    swing_r, swing_c = [], []
    swing_set = []                                       # currently held swing basket
    for i in range(lookback + 1, n - 1):
        sig = ud.iloc[i].dropna()
        if len(sig) < topn:
            continue
        top = sig.sort_values(ascending=False).head(topn).index
        c0, o1, c1 = C[top].iloc[i], O[top].iloc[i + 1], C[top].iloc[i + 1]
        on = float((o1 / c0 - 1).mean())                 # close[i] -> open[i+1]  (the gap)
        ind = float((c1 / o1 - 1).mean())                # open[i+1] -> close[i+1] (the day)
        fu = float((c1 / c0 - 1).mean())                 # close[i] -> close[i+1]  (1-day hold)
        bh = float(ret_c.iloc[i + 1].mean())             # equal-weight universe
        if any(x != x for x in [on, ind, fu, bh]):       # skip rows with NaN
            continue
        overnight.append(on); intraday.append(ind); full.append(fu)
        buyhold.append(bh); days.append(str(C.index[i + 1].date()))

        # SWING: rebalance only every swing_hold days; hold the basket between.
        k = len(days) - 1
        cost_today = 0.0
        if k % swing_hold == 0:
            newset = list(top)
            turn = 1.0 if not swing_set else len(set(newset) ^ set(swing_set)) / (2.0 * topn)
            swing_set = newset
            cost_today = turn * ROUNDTRIP_BPS / 10000.0
        sr_today = float((C[swing_set].iloc[i + 1] / C[swing_set].iloc[i] - 1).mean()) if swing_set else 0.0
        swing_r.append(sr_today); swing_c.append(cost_today)

    L = len(overnight)
    flat = lambda bps: np.full(L, bps / 10000.0)
    return {
        "overnight": (np.array(overnight), flat(ROUNDTRIP_BPS)),       # flatten daily -> full RT/day
        "intraday":  (np.array(intraday),  flat(INTRADAY_BPS)),
        "full":      (np.array(full),      flat(ROUNDTRIP_BPS)),       # 1-day hold, exit each close
        "swing":     (np.array(swing_r),   np.array(swing_c)),         # positional, turnover-aware
        "buyhold":   (np.array(buyhold),   np.zeros(L)),               # benchmark, ~no trading
        "days": days, "universe": C.shape[1], "swing_hold": swing_hold,
    }


# ----------------------------------------------------------------- report
def run(period="2y", topn=8, n_trials=16):
    """n_trials reflects how many strategy variants the pod searched (the ~11 option
    strats + 3 volume legs + variants) — the DSR selection-bias correction."""
    s = build_strategies(period, topn)
    if not s:
        return {"error": "insufficient data"}
    names = ["overnight", "intraday", "full", "swing", "buyhold"]
    net = {nm: _clean(s[nm][0]) - s[nm][1][:len(s[nm][0])] for nm in names}
    trial_sr_per = [ann_sharpe(net[nm]) / math.sqrt(TRADING) for nm in names if nm != "buyhold"]

    rows = []
    for nm in names:
        gross, cost = s[nm]
        r = net[nm]
        nsr = ann_sharpe(r)
        lo, hi = bootstrap_sharpe_ci(r)
        p = psr(r, 0.0)
        d, sr_star = dsr(r, trial_sr_per, n_trials)
        subs = subperiod_sharpes(r, 4)
        survives = (nsr > 0 and not math.isnan(p) and p > 0.95 and not math.isnan(d) and d > 0.90
                    and lo > 0 and sum(1 for x in subs if x > 0) >= max(3, len(subs) - 1))
        rows.append({
            "strategy": nm,
            "gross_sharpe": round(ann_sharpe(gross), 2),
            "net_sharpe": round(nsr, 2),
            "avg_cost_bps_day": round(float(np.mean(cost) * 10000), 1),
            "ann_return_pct": round(float(np.mean(r) * TRADING * 100), 1),
            "maxdd_pct": round(maxdd_pct(r), 1),
            "subperiod_sharpes": subs,
            "boot_ci95": [round(lo, 2), round(hi, 2)],
            "psr_vs0": round(p, 3) if not math.isnan(p) else None,
            "dsr_vs_selection": round(d, 3) if not math.isnan(d) else None,
            "dsr_benchmark_sr": round(sr_star * math.sqrt(TRADING), 2),
            "verdict": "SURVIVES" if survives else "FRAGILE",
        })
    rows.sort(key=lambda x: -x["net_sharpe"])
    return {"n_obs": len(s["overnight"][0]), "universe": s["universe"], "period": period,
            "topn": topn, "swing_hold": s["swing_hold"], "n_trials_assumed": n_trials,
            "from": s["days"][0] if s["days"] else None, "to": s["days"][-1] if s["days"] else None,
            "results": rows,
            "note": ("Net of realistic Indian costs, turnover-aware. DSR corrects for having "
                     "searched ~%d strategy variants. SURVIVES requires net Sharpe>0, PSR>0.95, "
                     "DSR>0.90, bootstrap-CI lower bound>0, and stable subperiods." % n_trials)}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    rep = run()
    if "error" in rep:
        print("ERROR:", rep["error"]); return
    print(f"\nROBUST BACKTEST — {rep['from']} -> {rep['to']}  ({rep['n_obs']} days, "
          f"{rep['universe']} names, top-{rep['topn']} volume basket, swing={rep['swing_hold']}d)")
    print(f"DSR assumes ~{rep['n_trials_assumed']} strategy variants were searched.\n")
    hdr = (f"{'strategy':<11}{'gross':>7}{'net':>7}{'cost/d':>8}{'annR%':>7}{'maxDD':>7}"
           f"{'PSR':>7}{'DSR':>7}  {'CI95(net)':<16}{'verdict':>10}")
    print(hdr); print("-" * len(hdr))
    for r in rep["results"]:
        ci = f"[{r['boot_ci95'][0]:>5},{r['boot_ci95'][1]:>5}]"
        print(f"{r['strategy']:<11}{r['gross_sharpe']:>7}{r['net_sharpe']:>7}"
              f"{r['avg_cost_bps_day']:>6}b{r['ann_return_pct']:>7}{r['maxdd_pct']:>7}"
              f"{(r['psr_vs0'] or 0):>7}{(r['dsr_vs_selection'] or 0):>7}  {ci:<16}{r['verdict']:>10}")
        print(f"           subperiods: {r['subperiod_sharpes']}")
    print("\n" + rep["note"])
    try:
        from datetime import datetime, timezone, timedelta
        rep["asof"] = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
        json.dump(rep, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "marleg_robust_bt.json"), "w"), indent=1)
        print("\n[wrote marleg_robust_bt.json]")
    except Exception as e:
        print("json dump failed:", e)


if __name__ == "__main__":
    main()
