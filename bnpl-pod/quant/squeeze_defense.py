"""
Squeeze Defense Layer — G-SDL veto.

MASTERPLAN v4.1 §10. Prevents the equity short leg (and any equity-adjacent
position) from firing when retail squeeze risk is elevated. This is the
structural reason the primary expression is TRS on junior ABS, not AFRM
equity short: ABS is unsqueezable by retail, equity is not.

Inputs (all from Sprint A warehouse):
  - options_chain  → OTM-call OI concentration, IV skew (25Δ put − 25Δ call)
  - short_interest → utilization, days-to-cover

Composite
---------
Each raw input is rank-normalized to [0, 1] within a trailing 252-day window
per ticker, then a weighted mean gives the squeeze_score. The veto fires
when squeeze_score > SQUEEZE_VETO_THRESHOLD (default 0.75).

Outputs are written to `squeeze_defense`, idempotent by (ticker, observed_at).
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

SQUEEZE_VETO_THRESHOLD = 0.75
OTM_THRESHOLD = 1.10        # "OTM" = strike > 110% of spot
DELTA_TARGET = 0.25         # for the 25-delta skew proxy


# --- Primitive computations -----------------------------------------------
def otm_call_share(options: list[dict], spot: float) -> float | None:
    """Fraction of call open interest at strikes > 110% of spot."""
    if not options or spot is None or spot <= 0:
        return None
    total_oi = 0.0
    otm_oi = 0.0
    for o in options:
        if o.get("option_type") != "C":
            continue
        oi = o.get("open_interest") or 0
        if oi <= 0:
            continue
        total_oi += oi
        if (o.get("strike") or 0) >= OTM_THRESHOLD * spot:
            otm_oi += oi
    if total_oi == 0:
        return None
    return float(otm_oi / total_oi)


def iv_skew_proxy(options: list[dict], spot: float) -> float | None:
    """
    Crude 25Δ skew proxy: IV of the 10%-OTM put minus IV of the 10%-OTM call
    within the 21–75 DTE band. Approximates the true 25Δ/25Δ risk reversal
    when full delta data isn't stored on the chain row.
    """
    if not options or spot is None or spot <= 0:
        return None
    def _pick(opt_type: str, moneyness: float) -> float | None:
        target = spot * moneyness
        best = None
        best_gap = float("inf")
        for o in options:
            if o.get("option_type") != opt_type or o.get("iv") in (None, 0):
                continue
            dte = o.get("dte") or 30
            if dte < 21 or dte > 75:
                continue
            gap = abs((o.get("strike") or 0) - target)
            if gap < best_gap:
                best_gap = gap
                best = o
        return float(best["iv"]) if best else None
    put_iv  = _pick("P", 0.90)   # 10% OTM put
    call_iv = _pick("C", 1.10)   # 10% OTM call
    if put_iv is None or call_iv is None:
        return None
    return float(put_iv - call_iv)


def rank_pctile(series: list[float | None]) -> list[float | None]:
    """Per-window rank → [0,1]. Missing values stay None."""
    clean = [x for x in series if x is not None]
    if len(clean) < 5:
        return [None] * len(series)
    sorted_vals = sorted(clean)
    out: list[float | None] = []
    n = len(sorted_vals)
    for x in series:
        if x is None:
            out.append(None)
            continue
        # fraction of window strictly below x
        lo, hi = 0, n
        while lo < hi:
            mid = (lo + hi) // 2
            if sorted_vals[mid] < x:
                lo = mid + 1
            else:
                hi = mid
        out.append(lo / n)
    return out


@dataclass
class SqueezeRow:
    ticker: str
    observed_at: date
    otm_call_pct: float | None
    utilization: float | None
    days_to_cover: float | None
    iv_skew_25d: float | None
    squeeze_score: float | None
    veto: bool


def combine_score(r_otm: float | None, r_util: float | None,
                  r_dtc: float | None, r_skew: float | None,
                  w_otm: float = 0.30, w_util: float = 0.30,
                  w_dtc: float = 0.25, w_skew: float = 0.15) -> float | None:
    """Weighted mean over whichever inputs are present. Negative skew (put IV
    > call IV) is BEARISH, so high positive skew INCREASES squeeze risk."""
    pieces = []
    weights = []
    for val, w in ((r_otm, w_otm), (r_util, w_util),
                   (r_dtc, w_dtc), (r_skew, w_skew)):
        if val is not None:
            pieces.append(val * w)
            weights.append(w)
    if not pieces:
        return None
    return float(sum(pieces) / sum(weights))


# --- DB I/O ---------------------------------------------------------------
def _chain_for(con, ticker: str, d: date) -> tuple[float | None, list[dict]]:
    row = con.execute(
        "SELECT AVG(underlying_price) FROM options_chain "
        "WHERE ticker=? AND observed_at=?",
        [ticker, d],
    ).fetchone()
    spot = float(row[0]) if row and row[0] is not None else None
    rows = con.execute(
        """SELECT strike, option_type, iv, open_interest,
                  CAST(date_diff('day', observed_at, expiry) AS INTEGER) AS dte
           FROM options_chain WHERE ticker=? AND observed_at=?""",
        [ticker, d],
    ).fetchall()
    opts = [{"strike": r[0], "option_type": r[1], "iv": r[2],
             "open_interest": r[3], "dte": r[4]} for r in rows]
    return spot, opts


def _si_for(con, ticker: str, d: date) -> tuple[float | None, float | None]:
    """Return (utilization, days_to_cover) from the most recent short-interest row ≤ d."""
    row = con.execute(
        """SELECT utilization, days_to_cover FROM short_interest
           WHERE ticker=? AND observed_at <= ?
           ORDER BY observed_at DESC LIMIT 1""",
        [ticker, d],
    ).fetchone()
    if not row:
        return None, None
    return (float(row[0]) if row[0] is not None else None,
            float(row[1]) if row[1] is not None else None)


def compute_for_ticker(ticker: str,
                       dates: Iterable[date] | None = None) -> int:
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        if dates is None:
            dates = [r[0] for r in con.execute(
                "SELECT DISTINCT observed_at FROM options_chain WHERE ticker=? "
                "ORDER BY observed_at", [ticker],
            ).fetchall()]
        dates = list(dates)
        if not dates:
            return 0

        raw_otm, raw_skew, raw_util, raw_dtc = [], [], [], []
        for d in dates:
            spot, opts = _chain_for(con, ticker, d)
            util, dtc  = _si_for(con, ticker, d)
            raw_otm.append(otm_call_share(opts, spot) if spot else None)
            raw_skew.append(iv_skew_proxy(opts, spot) if spot else None)
            raw_util.append(util)
            raw_dtc.append(dtc)

        r_otm  = rank_pctile(raw_otm)
        r_skew = rank_pctile(raw_skew)
        r_util = rank_pctile(raw_util)
        r_dtc  = rank_pctile(raw_dtc)

        payload = []
        for i, d in enumerate(dates):
            score = combine_score(r_otm[i], r_util[i], r_dtc[i], r_skew[i])
            veto = bool(score is not None and score > SQUEEZE_VETO_THRESHOLD)
            payload.append((
                ticker, d,
                raw_otm[i], raw_util[i], raw_dtc[i], raw_skew[i],
                score, veto,
            ))
        con.executemany(
            """INSERT OR REPLACE INTO squeeze_defense
               (ticker, observed_at, otm_call_pct, utilization, days_to_cover,
                iv_skew_25d, squeeze_score, veto)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            payload,
        )
        n_veto = sum(1 for p in payload if p[-1])
        log.info("squeeze | %-6s | %d days | veto on %d", ticker, len(dates), n_veto)
        return len(payload)
    finally:
        con.close()


def compute_all(tickers: Iterable[str] = ("AFRM", "SQ", "PYPL", "SEZL", "UPST")
                ) -> dict[str, int]:
    return {t: compute_for_ticker(t) for t in tickers}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    s = compute_all()
    print("\nSqueeze Defense summary:")
    for t, n in s.items():
        print(f"  {t:6s} {n:>5d} rows")
