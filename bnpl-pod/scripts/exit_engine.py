"""
Exit Engine -- 5-trigger priority overlay
=========================================

Generates FLATTEN / PARTIAL_EXIT actions consumed by risk_engine._apply_writeback.

Priority order (highest fires first; first fire wins per position):

  1. HARD_STOP            (existing journal.stop_price -- enforced by pod intraday)
  2. EARNINGS_SHIELD      (T-2 trading days before earnings -- THIS MODULE, v1)
  3. BSI_THESIS_COOL      (z drops below 1.0 for 3 consecutive days -- stub)
  4. VOLUME_CATALYST      (vol-z > 2.0 AND price gap > 3% same session -- stub)
  5. TIME_STOP            (holding > archetype max -- stub)

v1 ships only #2 (highest EV by far: would have prevented SEZL/WRLD/AFRM FPs,
all earnings-gap rallies). Other triggers are scaffolded but return empty.

API
---

  earnings_shield_actions(enriched_positions, t_minus_days=2) -> list[dict]
      Returns FLATTEN action dicts compatible with risk_engine's action list:
      {trade_id, ticker, type='FLATTEN', reason=..., earnings_date=...}

  bsi_thesis_cool_actions(...)        -- stub, returns []
  volume_catalyst_actions(...)        -- stub, returns []
  time_stop_actions(...)              -- stub, returns []
  all_exit_actions(...)               -- runs all 5 triggers in priority order
"""
from __future__ import annotations

import warnings
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")


# ---------------------------------------------------------------------------
# Trigger #2: Earnings shield  (the only one wired in v1)
# ---------------------------------------------------------------------------

_EARNINGS_CACHE: dict[str, Optional[date]] = {}


def _next_earnings_date(ticker: str) -> Optional[date]:
    """Resolve the next earnings date for a ticker via yfinance.

    yfinance exposes several paths; we try them in order and cache.
    Returns None if no future earnings date can be resolved.
    """
    if ticker in _EARNINGS_CACHE:
        return _EARNINGS_CACHE[ticker]
    nxt: Optional[date] = None
    try:
        t = yf.Ticker(ticker)
        # path 1: calendar (preferred -- a single dict with "Earnings Date" list)
        try:
            cal = t.calendar
            if isinstance(cal, dict):
                eds = cal.get("Earnings Date") or cal.get("earningsDate")
                if eds:
                    # eds may be a list of datetimes or a single value
                    if isinstance(eds, (list, tuple)) and eds:
                        first = eds[0]
                    else:
                        first = eds
                    if isinstance(first, datetime):
                        nxt = first.date()
                    elif isinstance(first, date):
                        nxt = first
                    elif hasattr(first, "to_pydatetime"):
                        nxt = first.to_pydatetime().date()
        except Exception:
            pass
        # path 2: earnings_dates DataFrame (gives both past and future, take next future)
        if nxt is None:
            try:
                ed = t.earnings_dates
                if isinstance(ed, pd.DataFrame) and not ed.empty:
                    today = pd.Timestamp(date.today())
                    future = ed.index[ed.index >= today]
                    if len(future):
                        nxt = future.min().date()
            except Exception:
                pass
        # path 3: info.earningsTimestamp (epoch seconds)
        if nxt is None:
            try:
                info = t.info
                ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
                if ts and ts > 0:
                    nxt = datetime.fromtimestamp(ts).date()
            except Exception:
                pass
    except Exception:
        nxt = None
    _EARNINGS_CACHE[ticker] = nxt
    return nxt


def _trading_days_until(target: date) -> int:
    """Approximate trading days between today and target (excludes weekends only)."""
    today = date.today()
    if target <= today:
        return 0
    days = 0
    cur = today
    while cur < target:
        cur += timedelta(days=1)
        if cur.weekday() < 5:  # Mon-Fri
            days += 1
    return days


def earnings_shield_actions(
    enriched_positions: list[dict],
    t_minus_days: int = 2,
) -> list[dict]:
    """Generate FLATTEN actions for positions within T-N trading days of earnings.

    Per the gray-swan spec:
      "Earnings shield (T-2 days). HIGHEST-EV RULE for short-BNPL. Would have
       prevented SEZL/WRLD/AFRM FPs (earnings-gap rallies)."
    """
    out: list[dict] = []
    seen_tickers: set[str] = set()
    today = date.today()
    for p in enriched_positions:
        tk = p["ticker"]
        if tk in seen_tickers:
            continue  # one FLATTEN per ticker -- pod groups lots
        ed = _next_earnings_date(tk)
        if ed is None or ed < today:
            # stale yfinance "next earnings" sometimes reports the LAST one;
            # only treat strictly-future dates as actionable
            continue
        days_to = _trading_days_until(ed)
        if 0 < days_to <= t_minus_days:
            seen_tickers.add(tk)
            out.append({
                "trade_id": p["id"],
                "ticker": tk,
                "type": "FLATTEN",
                "reason": f"earnings_shield T-{days_to}d (earnings {ed.isoformat()})",
                "earnings_date": ed.isoformat(),
                "days_to_earnings": days_to,
            })
    return out


# ---------------------------------------------------------------------------
# Stubs -- specced, not yet wired
# ---------------------------------------------------------------------------

def bsi_thesis_cool_actions(
    enriched_positions: list[dict],
    cool_threshold_z: float = 1.0,
    consecutive_days: int = 3,
) -> list[dict]:
    """Trigger #3: exit when BSI z drops below threshold for N consecutive days.

    Symmetric to entry but looser. Entry waits for z>=2.0 for 2+ weeks; exit
    fires when z drops below 1.0 for 3 consecutive days. "Slow in, fast out."

    Requires bsi_snapshot history; once the bsi_snapshot writer is journaling
    every morning, this can read N days of z_score per ticker. Returns [] in v1.
    """
    return []


def volume_catalyst_actions(
    enriched_positions: list[dict],
    vol_z_threshold: float = 2.0,
    gap_pct_threshold: float = 0.03,
) -> list[dict]:
    """Trigger #4: vol-z > 2.0 AND price gap > 3% same session.

    Conditioning on price-gap removes the pure-volume false-fire problem.
    With-thesis gap -> take profit; against-thesis gap -> exit at stop.
    Requires intraday session data; v1 returns []. Daily-bar approximation TBD.
    """
    return []


def time_stop_actions(
    enriched_positions: list[dict],
    archetype_max_days: Optional[dict] = None,
) -> list[dict]:
    """Trigger #5: force exit after archetype-max holding period.

    Hit rate at 30d ~ hit rate at 365d (~30%) for every archetype -- the middle
    holding period is dead capital. Force exits at:
      BLITZ  30d
      SCOUT  60d
      GUARDIAN 90d
      ROBO   120d

    Recycles capital into fresher signals. v1 stub. Easy to wire once the
    archetype-max policy is locked.
    """
    archetype_max_days = archetype_max_days or {
        "BLITZ": 30, "SCOUT": 60, "GUARDIAN": 90, "ROBO": 120,
    }
    out: list[dict] = []
    today = date.today()
    for p in enriched_positions:
        try:
            entry_d = date.fromisoformat(p["ts"])
        except Exception:
            continue
        max_d = archetype_max_days.get(p.get("archetype", "SCOUT"), 60)
        held = (today - entry_d).days
        if held >= max_d:
            out.append({
                "trade_id": p["id"], "ticker": p["ticker"], "type": "FLATTEN",
                "reason": f"time_stop {p.get('archetype','SCOUT')} held {held}d >= max {max_d}d",
                "days_held": held,
            })
    return out


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------

def all_exit_actions(
    enriched_positions: list[dict],
    *,
    earnings_t_minus_days: int = 2,
    enable_earnings_shield: bool = True,
    enable_bsi_thesis_cool: bool = False,
    enable_volume_catalyst: bool = False,
    enable_time_stop: bool = True,
) -> list[dict]:
    """Run all enabled triggers in priority order. First fire per ticker wins."""
    out: list[dict] = []
    fired: set[str] = set()

    def _accept(actions: list[dict]) -> None:
        for a in actions:
            if a["ticker"] not in fired:
                fired.add(a["ticker"])
                out.append(a)

    if enable_earnings_shield:
        _accept(earnings_shield_actions(enriched_positions, earnings_t_minus_days))
    if enable_bsi_thesis_cool:
        _accept(bsi_thesis_cool_actions(enriched_positions))
    if enable_volume_catalyst:
        _accept(volume_catalyst_actions(enriched_positions))
    if enable_time_stop:
        _accept(time_stop_actions(enriched_positions))
    return out


if __name__ == "__main__":
    # Smoke test: print earnings dates for the BNPL universe.
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("tickers", nargs="*",
                   default=["PYPL", "AFRM", "SEZL", "KLAR", "XYZ", "UPST",
                            "SOFI", "WRLD", "CRMT", "CVNA", "CACC", "ENVA",
                            "OPFI", "LC", "OMF"])
    p.add_argument("--t-minus", type=int, default=2)
    args = p.parse_args()
    today = date.today()
    print(f"{'TICKER':<7} {'NEXT EARNINGS':<15} {'DAYS':>5}  STATUS")
    print("-" * 60)
    for tk in args.tickers:
        ed = _next_earnings_date(tk)
        if ed is None:
            print(f"{tk:<7} {'-':<15} {'-':>5}  no earnings date resolved")
            continue
        if ed < today:
            print(f"{tk:<7} {ed.isoformat():<15} {'past':>5}  stale (likely last earnings, not next)")
            continue
        d = _trading_days_until(ed)
        flag = "FLATTEN" if 0 < d <= args.t_minus else ""
        print(f"{tk:<7} {ed.isoformat():<15} {d:>5}  {flag}")
