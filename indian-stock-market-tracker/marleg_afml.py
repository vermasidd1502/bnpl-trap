"""
Marle-G — AFML BACKTESTING AGENT (López de Prado methods, modeled on jarjarquant).

jarjarquant (viksharma04, GitHub) is itself an AFML implementation; it won't pip-install
on this Python 3.14 (Cython wheel build fails, no 3.14 wheels). So this module delivers
the SAME capability natively on the stack we already have (numpy/pandas/sklearn/statsmodels),
mirroring jarjarquant's component structure. Full credit: Marcos López de Prado,
"Advances in Financial Machine Learning" (2018), and jarjarquant for the design.

Components (same names as jarjarquant):
  DataGatherer     — get_yf(ticker, period)
  FeatureEngineer  — frac_diff (AFML ch.5), build_features (our volume/technical set)
  Labeller         — get_daily_vol, cusum_events, triple_barrier_method (AFML ch.3)
  FeatureEvaluator — feature_importance_MDA with PURGED K-fold CV (AFML ch.7-8)
  DataAnalyst      — adf_test (stationarity), deflated_sharpe (AFML / Bailey-LdP)
  BacktestAgent    — orchestrates the pipeline -> a rigorous, leakage-aware edge verdict

  python marleg_afml.py TITAN
  python marleg_afml.py RELIANCE --period 6y --hold 10 --ptsl 1.5
"""
import sys, argparse, math
import numpy as np, pandas as pd
import yfinance as yf
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from statsmodels.tsa.stattools import adfuller
from scipy.stats import norm


# ----------------------------------------------------------------- DataGatherer
class DataGatherer:
    @staticmethod
    def get_yf(ticker, period="6y"):
        t = ticker if ticker.startswith("^") else ticker + ".NS"
        df = yf.Ticker(t).history(period=period, interval="1d", auto_adjust=True)
        return df[["Open", "High", "Low", "Close", "Volume"]].dropna()


# ----------------------------------------------------------------- FeatureEngineer
class FeatureEngineer:
    @staticmethod
    def _ffd_weights(d, thres=1e-4, max_k=2000):
        w = [1.0]; k = 1
        while k < max_k:
            wk = -w[-1] * (d - k + 1) / k
            if abs(wk) < thres:
                break
            w.append(wk); k += 1
        return np.array(w[::-1])

    @classmethod
    def frac_diff(cls, series, d=0.4, thres=1e-4):
        """Fixed-width fractional differentiation (AFML 5.5): stationary, keeps memory."""
        w = cls._ffd_weights(d, thres); width = len(w)
        v = series.values.astype(float)
        out = np.full(len(v), np.nan)
        for i in range(width - 1, len(v)):
            out[i] = np.dot(w, v[i - width + 1:i + 1])
        return pd.Series(out, index=series.index)

    @classmethod
    def build_features(cls, df):
        c, v, h, l = df["Close"], df["Volume"], df["High"], df["Low"]
        f = pd.DataFrame(index=df.index)
        # the pod's volume-first signals + classic technicals, all as features
        delta = c.diff()
        up = delta.clip(lower=0).rolling(14).mean(); dn = (-delta.clip(upper=0)).rolling(14).mean().replace(0, np.nan)
        f["rsi"] = 100 - 100 / (1 + up / dn)
        sign = np.sign(delta)
        f["ud_ratio"] = (v.where(sign > 0, 0).rolling(20).sum() / v.where(sign < 0, 0).rolling(20).sum().replace(0, np.nan))
        obv = (sign.fillna(0) * v).cumsum()
        f["obv_slope"] = obv.diff(10) / v.rolling(10).mean()
        mfm = ((c - l) - (h - c)) / (h - l).replace(0, np.nan)
        f["cmf"] = (mfm * v).rolling(20).sum() / v.rolling(20).sum()
        f["rvol"] = v / v.rolling(20).mean()
        f["mom20"] = c.pct_change(20)
        f["dist_50dma"] = c / c.rolling(50).mean() - 1
        f["ffd_close"] = cls.frac_diff(np.log(c), d=0.4)        # stationary price-memory feature
        return f


# ----------------------------------------------------------------- Labeller
class Labeller:
    @staticmethod
    def get_daily_vol(close, span=20):
        return close.pct_change().ewm(span=span).std()

    @staticmethod
    def cusum_events(close, h_mult=1.0, vol=None):
        """Symmetric CUSUM filter (AFML 2.5.2): sample only on meaningful moves."""
        if vol is None:
            vol = Labeller.get_daily_vol(close)
        ret = close.pct_change()
        sp = sn = 0.0; ev = []
        for t in ret.index[1:]:
            x = ret.loc[t]; thr = h_mult * (vol.loc[t] if not pd.isna(vol.loc[t]) else 0)
            if thr <= 0:
                continue
            sp = max(0.0, sp + x); sn = min(0.0, sn + x)
            if sp > thr:
                sp = 0.0; ev.append(t)
            elif sn < -thr:
                sn = 0.0; ev.append(t)
        return pd.DatetimeIndex(ev)

    @staticmethod
    def triple_barrier_method(close, t_events, pt_sl=1.0, n_days=10, vol=None):
        """AFML 3.x: ±pt_sl*vol horizontal barriers + n_days vertical. Returns label + t1."""
        if vol is None:
            vol = Labeller.get_daily_vol(close)
        idx = close.index; rows = []
        for t0 in t_events:
            if t0 not in idx:
                continue
            loc = idx.get_loc(t0); trgt = vol.loc[t0]
            if loc + n_days >= len(idx) or pd.isna(trgt) or trgt <= 0:
                continue
            t1 = idx[loc + n_days]
            up = close.loc[t0] * (1 + pt_sl * trgt); dn = close.loc[t0] * (1 - pt_sl * trgt)
            path = close.iloc[loc:loc + n_days + 1]
            label, touch = None, t1
            for tt, px in path.items():
                if px >= up:
                    label, touch = 1, tt; break
                if px <= dn:
                    label, touch = -1, tt; break
            if label is None:                                    # vertical barrier -> sign of return
                r = close.loc[t1] / close.loc[t0] - 1
                label = 1 if r > 0 else -1
            rows.append({"t0": t0, "t1": touch, "label": label, "ret": close.loc[touch] / close.loc[t0] - 1})
        return pd.DataFrame(rows).set_index("t0") if rows else pd.DataFrame()


# ----------------------------------------------------------------- FeatureEvaluator
class FeatureEvaluator:
    @staticmethod
    def _purged_folds(t0_index, t1, n_splits=5, embargo=0.01):
        """Purged K-fold (AFML 7): drop train obs whose label window overlaps the test window."""
        n = len(t0_index); idx = np.arange(n)
        fold = np.array_split(idx, n_splits)
        emb = int(n * embargo)
        for f in fold:
            te = idx[f[0]:f[-1] + 1]
            te_t0_min, te_t1_max = t0_index[f[0]], t1.iloc[f[-1]]
            tr = []
            for i in idx:
                if i in te:
                    continue
                # purge: training label must not overlap [test start, test end]; + embargo after
                if t1.iloc[i] < te_t0_min or t0_index[i] > te_t1_max:
                    if not (f[-1] < i <= f[-1] + emb):           # embargo
                        tr.append(i)
            yield np.array(tr), te

    @classmethod
    def feature_importance_MDA(cls, X, y, t1, sample_weight=None, n_splits=5):
        """Mean-Decrease-Accuracy on PURGED CV (AFML 8.3). Returns ranked importances + OOS acc."""
        cols = list(X.columns); imp = {c: [] for c in cols}; oos = []
        Xv, yv = X.values, y.values
        rng = np.random.default_rng(7)
        for tr, te in cls._purged_folds(X.index, t1, n_splits):
            if len(tr) < 30 or len(te) < 10 or len(np.unique(yv[tr])) < 2:
                continue
            clf = RandomForestClassifier(n_estimators=200, max_features=1, min_samples_leaf=20,
                                         class_weight="balanced", random_state=7, n_jobs=-1)
            sw = sample_weight.values[tr] if sample_weight is not None else None
            clf.fit(Xv[tr], yv[tr], sample_weight=sw)
            base = accuracy_score(yv[te], clf.predict(Xv[te]))
            oos.append(base)
            for j, c in enumerate(cols):
                Xte = Xv[te].copy(); Xte[:, j] = rng.permutation(Xte[:, j])
                imp[c].append(base - accuracy_score(yv[te], clf.predict(Xte)))
        res = pd.DataFrame({c: [np.mean(imp[c]) if imp[c] else 0, np.std(imp[c]) if imp[c] else 0]
                            for c in cols}, index=["mda", "std"]).T.sort_values("mda", ascending=False)
        return res, (np.mean(oos) if oos else float("nan"))


# ----------------------------------------------------------------- DataAnalyst
class DataAnalyst:
    @staticmethod
    def adf_test(series):
        s = series.dropna()
        if len(s) < 30:
            return {"adf": None, "pvalue": None, "stationary": None}
        stat, p, *_ = adfuller(s, maxlag=1, regression="c", autolag=None)
        return {"adf": round(float(stat), 3), "pvalue": round(float(p), 4), "stationary": bool(p < 0.05)}

    @staticmethod
    def deflated_sharpe(returns, n_trials=10):
        """Deflated Sharpe Ratio (Bailey & López de Prado): is the Sharpe real after n_trials selection?"""
        r = np.asarray(returns, float); r = r[~np.isnan(r)]
        T = len(r)
        if T < 10 or r.std() == 0:
            return {"sharpe": None, "dsr": None}
        sr = r.mean() / r.std()
        sk = float(pd.Series(r).skew()); ku = float(pd.Series(r).kurt()) + 3.0
        emc = 0.5772156649
        sr0 = (np.sqrt(1.0 / max(n_trials - 1, 1)) *
               ((1 - emc) * norm.ppf(1 - 1.0 / n_trials) + emc * norm.ppf(1 - 1.0 / (n_trials * np.e))))
        denom = math.sqrt(max(1e-9, 1 - sk * sr + (ku - 1) / 4.0 * sr ** 2))
        dsr = norm.cdf((sr - sr0) * math.sqrt(T - 1) / denom)
        return {"sharpe": round(float(sr), 3), "dsr": round(float(dsr), 3),   # per-label (events ≠ daily)
                "sr0_haircut": round(float(sr0), 3)}


# ----------------------------------------------------------------- BacktestAgent
class BacktestAgent:
    def __init__(self, ticker, period="6y", n_days=10, pt_sl=1.0):
        self.ticker, self.n_days, self.pt_sl = ticker.upper(), n_days, pt_sl
        self.df = DataGatherer.get_yf(ticker, period)

    def run(self):
        c = self.df["Close"]
        vol = Labeller.get_daily_vol(c)
        events = Labeller.cusum_events(c, h_mult=1.0, vol=vol)
        labels = Labeller.triple_barrier_method(c, events, self.pt_sl, self.n_days, vol)
        feats = FeatureEngineer.build_features(self.df)
        if labels.empty:
            return {"error": "no labeled events"}
        X = feats.reindex(labels.index).dropna()
        y = labels.loc[X.index, "label"]
        rets = labels.loc[X.index, "ret"]
        t1 = labels.loc[X.index, "t1"]
        # sample weight ~ |return| (AFML 4: weight by label magnitude)
        sw = rets.abs() / rets.abs().mean()
        mda, oos = FeatureEvaluator.feature_importance_MDA(X, y, t1, sw)
        return {
            "ticker": self.ticker, "period_days": len(c), "events": len(events), "labeled": len(X),
            "label_balance": {int(k): int(v) for k, v in y.value_counts().items()},
            "oos_accuracy": round(float(oos), 3),
            "mda": mda, "adf_ffd": DataAnalyst.adf_test(X["ffd_close"]),
            "adf_rawprice": DataAnalyst.adf_test(np.log(c)),
            "deflated_sharpe": DataAnalyst.deflated_sharpe(rets, n_trials=len(X.columns)),
        }


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")          # Windows cp1252 can't print σ/±/é
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker", nargs="?", default="TITAN")
    ap.add_argument("--period", default="6y")
    ap.add_argument("--hold", type=int, default=10)
    ap.add_argument("--ptsl", type=float, default=1.0)
    a = ap.parse_args()
    print(f"AFML backtesting agent | {a.ticker} | hold {a.hold}d | barriers ±{a.ptsl}σ | (López de Prado, modeled on jarjarquant)\n")
    r = BacktestAgent(a.ticker, a.period, a.hold, a.ptsl).run()
    if r.get("error"):
        print(r["error"]); return
    print(f"  {r['period_days']} days -> {r['events']} CUSUM events -> {r['labeled']} triple-barrier labels  "
          f"(balance {r['label_balance']})")
    print(f"  out-of-sample accuracy (purged CV): {r['oos_accuracy']:.1%}   (0.50 = no edge)")
    ds = r["deflated_sharpe"]
    print(f"  per-label Sharpe {ds['sharpe']}  ->  DEFLATED Sharpe prob {ds['dsr']} "
          f"(>0.95 = real after {len(r['mda'])} trials; expected-max SR0 {ds['sr0_haircut']})")
    print(f"  stationarity (ADF p): raw log-price {r['adf_rawprice']['pvalue']} (stationary={r['adf_rawprice']['stationary']}) "
          f"| frac-diff {r['adf_ffd']['pvalue']} (stationary={r['adf_ffd']['stationary']})")
    print(f"\n  FEATURE IMPORTANCE (Mean-Decrease-Accuracy, purged CV — which signals actually predict):")
    for name, row in r["mda"].iterrows():
        bar = "#" * max(0, int(row["mda"] * 300))
        print(f"    {name:<12}{row['mda']:>+7.4f} ± {row['std']:.4f}  {bar}")
    print("\n  Read: MDA>0 = the feature carries real predictive signal; ~0 or negative = noise.")
    print("  Honest frame: triple-barrier + purged CV + deflated Sharpe = leakage-aware; still one name, no costs.")


if __name__ == "__main__":
    main()
