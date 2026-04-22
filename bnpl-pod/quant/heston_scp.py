"""
Heston calibration + Squeeze-Compression Premium (SCP) — G2 gate.

MASTERPLAN v4.1 §7, §8 (SCP).

SCP definition
--------------
For each (ticker, observed_at):

    ATM_IV  = implied vol at the strike closest to 100%-moneyness, ~30-60 DTE
    HV20    = 20-day annualized realized vol on the underlying
    SCP     = ATM_IV - HV20                   (vol risk premium)
    z_SCP   = z-score of SCP over a 252-day rolling window per ticker

G2 fires when z_SCP > Φ^{-1}(0.90) ≈ 1.2816. (90th percentile; see §8.)

Heston calibration
------------------
QuantLib-Python performs the full calibration to the option chain. We store
(κ, θ, σ, ρ, v₀) on every calibrated day. The calibration is optional — if
QuantLib isn't installed or fails, we still write SCP (which only needs
market IVs) and leave the Heston params NULL.

Run with:  python -m quant.heston_scp
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

import duckdb

from data.settings import settings

log = logging.getLogger(__name__)

SCP_WINDOW_DAYS = 252          # rolling z-score window
SCP_GATE_PCTILE = 0.90         # G2 fires above this
GATE_Z = 1.2816                # Φ^{-1}(0.90)
DTE_MIN, DTE_MAX = 21, 75      # option maturity band for ATM-IV sampling


# --- Math helpers ---------------------------------------------------------
def _atm_iv_from_chain(options: list[dict], spot: float) -> float | None:
    """Pick the call whose strike is closest to spot in the 21–75-DTE band."""
    if not options or spot is None or spot <= 0:
        return None
    best = None
    best_gap = float("inf")
    for o in options:
        if o.get("option_type") != "C":
            continue
        if o.get("iv") is None or o.get("strike") is None:
            continue
        dte = o.get("dte")
        if dte is None or not (DTE_MIN <= dte <= DTE_MAX):
            continue
        gap = abs(o["strike"] - spot) / spot
        if gap < best_gap:
            best_gap = gap
            best = o
    if best is None or best_gap > 0.15:    # no ATM option within ±15%
        return None
    return float(best["iv"])


def realized_vol(prices: list[float], window: int = 20) -> float | None:
    import numpy as np
    if prices is None or len(prices) < window + 1:
        return None
    arr = np.asarray(prices[-(window + 1):], dtype=float)
    rets = np.diff(np.log(arr))
    if rets.size == 0 or rets.std(ddof=1) == 0:
        return None
    return float(rets.std(ddof=1) * math.sqrt(252.0))


def scp_value(atm_iv: float | None, hv20: float | None) -> float | None:
    if atm_iv is None or hv20 is None:
        return None
    return float(atm_iv - hv20)


def rolling_zscore(series: list[float | None], window: int = SCP_WINDOW_DAYS) -> list[float | None]:
    import numpy as np
    out: list[float | None] = []
    for i in range(len(series)):
        lo = max(0, i - window + 1)
        chunk = [x for x in series[lo : i + 1] if x is not None]
        if len(chunk) < 20 or series[i] is None:
            out.append(None)
            continue
        mu = float(np.mean(chunk))
        sd = float(np.std(chunk, ddof=0)) or 1e-9
        out.append((series[i] - mu) / sd)
    return out


def gate_fires(z: float | None) -> bool:
    return z is not None and z > GATE_Z


# --- Heston calibration (QuantLib, lazy) ---------------------------------
@dataclass
class HestonParams:
    kappa: float
    theta: float
    sigma: float
    rho: float
    v0: float
    rmse: float


def _calibrate_ql(options: list[dict], spot: float,
                  r: float = 0.045, q: float = 0.0) -> HestonParams | None:
    """
    Wrap QuantLib Heston calibration. Returns None on any failure — the caller
    still writes SCP. Tests monkeypatch this function so CI has no QL dependency.
    """
    try:
        import QuantLib as ql   # local import
    except Exception:   # noqa: BLE001
        return None
    try:
        today = ql.Date.todaysDate()
        ql.Settings.instance().evaluationDate = today
        day_count = ql.Actual365Fixed()
        calendar = ql.NullCalendar()
        risk_free = ql.YieldTermStructureHandle(
            ql.FlatForward(today, r, day_count))
        dividend = ql.YieldTermStructureHandle(
            ql.FlatForward(today, q, day_count))
        spot_h = ql.QuoteHandle(ql.SimpleQuote(spot))

        # Heston model initial guesses
        v0 = 0.04; kappa = 1.5; theta = 0.04; sigma = 0.3; rho = -0.5
        process = ql.HestonProcess(risk_free, dividend, spot_h,
                                   v0, kappa, theta, sigma, rho)
        model = ql.HestonModel(process)
        engine = ql.AnalyticHestonEngine(model)

        helpers = []
        for o in options:
            if o.get("option_type") != "C" or o.get("iv") in (None, 0):
                continue
            dte = o.get("dte")
            if dte is None or dte < 7:
                continue
            try:
                period = ql.Period(int(dte), ql.Days)
                vol_q = ql.QuoteHandle(ql.SimpleQuote(float(o["iv"])))
                h = ql.HestonModelHelper(period, calendar, spot,
                                         float(o["strike"]), vol_q,
                                         risk_free, dividend)
                h.setPricingEngine(engine)
                helpers.append(h)
            except Exception:   # noqa: BLE001
                continue
        if len(helpers) < 6:
            return None

        lm = ql.LevenbergMarquardt(1e-8, 1e-8, 1e-8)
        model.calibrate(helpers, lm,
                        ql.EndCriteria(400, 40, 1e-8, 1e-8, 1e-8))
        theta_, kappa_, sigma_, rho_, v0_ = model.params()
        rmse = math.sqrt(sum((h.calibrationError() ** 2) for h in helpers) / len(helpers))
        return HestonParams(kappa=float(kappa_), theta=float(theta_),
                            sigma=float(sigma_), rho=float(rho_),
                            v0=float(v0_), rmse=float(rmse))
    except Exception as e:   # noqa: BLE001
        log.warning("heston calibration failed: %s", e)
        return None


# --- DB I/O ---------------------------------------------------------------
def _load_chain(con, ticker: str, d: date) -> tuple[float | None, list[dict]]:
    row = con.execute(
        "SELECT AVG(underlying_price) FROM options_chain "
        "WHERE ticker=? AND observed_at=?",
        [ticker, d],
    ).fetchone()
    spot = float(row[0]) if row and row[0] is not None else None
    rows = con.execute(
        """SELECT strike, option_type, iv, expiry,
                  CAST(date_diff('day', observed_at, expiry) AS INTEGER) AS dte
           FROM options_chain
           WHERE ticker=? AND observed_at=?""",
        [ticker, d],
    ).fetchall()
    opts = [{"strike": r[0], "option_type": r[1], "iv": r[2],
             "expiry": r[3], "dte": r[4]} for r in rows]
    return spot, opts


def _load_price_history(con, ticker: str, end_d: date, n_days: int = 60) -> list[float]:
    """Best available: use options_chain's underlying_price as a daily proxy."""
    rows = con.execute(
        """SELECT observed_at, AVG(underlying_price) FROM options_chain
           WHERE ticker=? AND observed_at <= ? AND underlying_price IS NOT NULL
           GROUP BY observed_at ORDER BY observed_at DESC LIMIT ?""",
        [ticker, end_d, n_days],
    ).fetchall()
    return list(reversed([float(r[1]) for r in rows if r[1] is not None]))


def compute_scp_for(ticker: str,
                    dates: Iterable[date] | None = None,
                    calibrate: bool = True) -> int:
    """Compute SCP (+ optional Heston calibration) for each date in `dates`.
    If `dates` is None, uses every options_chain observed_at for that ticker."""
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        if dates is None:
            dates = [r[0] for r in con.execute(
                "SELECT DISTINCT observed_at FROM options_chain WHERE ticker=? "
                "ORDER BY observed_at",
                [ticker],
            ).fetchall()]
        dates = list(dates)
        if not dates:
            return 0

        raw_scp: list[float | None] = []
        heston_rows: list[HestonParams | None] = []
        spot_by_date: dict[date, float | None] = {}

        for d in dates:
            spot, opts = _load_chain(con, ticker, d)
            spot_by_date[d] = spot
            iv  = _atm_iv_from_chain(opts, spot)
            prices = _load_price_history(con, ticker, d)
            hv  = realized_vol(prices, window=20)
            raw_scp.append(scp_value(iv, hv))
            if calibrate and spot and opts:
                heston_rows.append(_calibrate_ql(opts, spot))
            else:
                heston_rows.append(None)

        z_scp = rolling_zscore(raw_scp)

        payload = []
        for d, sc, z, hp in zip(dates, raw_scp, z_scp, heston_rows):
            payload.append((
                ticker, d, sc, z,
                hp.kappa if hp else None,
                hp.theta if hp else None,
                hp.sigma if hp else None,
                hp.rho   if hp else None,
                hp.v0    if hp else None,
                hp.rmse  if hp else None,
            ))
        con.executemany(
            """INSERT OR REPLACE INTO scp_daily
               (ticker, observed_at, scp, z_scp,
                kappa, theta, sigma, rho, v0, calibration_rmse)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            payload,
        )
        n_gate = sum(1 for z in z_scp if gate_fires(z))
        log.info("scp | %-6s | %d days | G2 fires on %d", ticker, len(dates), n_gate)
        return len(payload)
    finally:
        con.close()


def compute_all(tickers: Iterable[str] = ("AFRM", "SQ", "PYPL", "SEZL", "UPST"),
                calibrate: bool = False) -> dict[str, int]:
    """Default: skip Heston calibration (fast); pass calibrate=True for full run."""
    return {t: compute_scp_for(t, calibrate=calibrate) for t in tickers}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    summary = compute_all(calibrate=False)
    print("\nSCP computation summary:")
    for t, n in summary.items():
        print(f"  {t:6s} {n:>5d} rows")
