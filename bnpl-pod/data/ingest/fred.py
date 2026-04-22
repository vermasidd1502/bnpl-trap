"""
FRED ingest — St. Louis Fed macro series.

Pulls the series we need for the pod's macro gate (MOVE), the BSI macro
pillar (yield-curve slope, consumer-credit delinquency, SOFR), and event
markers for the backtest. Writes to `fred_series` in DuckDB.

Design
------
- One HTTP dependency (`httpx`). No `fredapi` — that package is a thin
  wrapper and pinning it adds zero value while adding a supply-chain edge.
- Idempotent: uses DuckDB `INSERT OR REPLACE` so re-running a backfill
  never duplicates a row.
- Retries on 429/5xx with exponential backoff via `tenacity`.
- Respects `settings.offline`: short-circuits to a no-op if the pod is in
  offline/CI mode (so unit tests don't hit the network).

Run with:  python -m data.ingest.fred  (backfills all configured series)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import duckdb
import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from data.settings import settings

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

# --- Series catalog ---------------------------------------------------------
# Each series is documented with WHY the pod cares about it. If you add a new
# one, update config/weights.yaml so the BSI aggregator knows about it.
@dataclass(frozen=True)
class FredSeries:
    series_id: str
    name: str
    purpose: str


SERIES: list[FredSeries] = [
    # --- Macro gate (MOVE > 120 is one of the four trade gates) ---
    FredSeries("MOVE",      "ICE BofA MOVE Index",
               "Bond-vol gate. Trade only fires when MOVE > 120."),
    # --- Term-structure + rates ---
    FredSeries("T10Y3M",    "10Y minus 3M Treasury Spread",
               "Yield-curve slope; recession signal."),
    FredSeries("DGS10",     "10Y Treasury CMT",
               "Risk-free discounting for TRS cashflow PV."),
    FredSeries("SOFR",      "Secured Overnight Financing Rate",
               "TRS floating-leg reference rate."),
    # --- Consumer credit ---
    FredSeries("DRCCLACBS", "Delinquency Rate on Credit Card Loans",
               "Ground-truth credit-stress comparand for BSI."),
    FredSeries("DRCLACBS",  "Delinquency Rate on Consumer Loans",
               "Broader consumer-loan distress series."),
    FredSeries("TDSP",      "Household Debt Service Payments / DPI",
               "Macro cash-flow stress indicator."),
    # --- Labor (regime classifier input) ---
    FredSeries("UNRATE",    "Unemployment Rate",
               "Regime flag; BNPL defaults are labor-sensitive."),
    FredSeries("ICSA",      "Initial Jobless Claims",
               "Higher-frequency labor signal."),
    # --- BSI macro-stress extras (added 2026-04-20) ---
    # These replace some of the signal we're losing while Reddit API approval
    # is pending. They're weekly/monthly macro series, so they feed the BSI's
    # *macro* pillar rather than the sentiment pillar — but they capture the
    # same underlying consumer-distress latent factor that Reddit was
    # proxying, and with far less noise.
    FredSeries("UMCSENT",   "U. Michigan Consumer Sentiment",
               "Monthly; direct consumer-mood gauge. Inverts into BSI."),
    FredSeries("BAMLH0A0HYM2", "ICE BofA US High-Yield OAS",
               "Daily; HY credit spread — the canonical 'risk-off' macro "
               "input. Mechanically correlates with ABS junior-tranche "
               "spread widening."),
    FredSeries("STLFSI4",   "St. Louis Fed Financial Stress Index",
               "Weekly; composite of 18 financial-stress inputs. External "
               "comparand for BSI — if STLFSI4 moves without BSI, our "
               "index is missing macro signal; if BSI moves without "
               "STLFSI4, we've found BNPL-specific distress."),
    FredSeries("DRSFRMACBS", "Delinquency Rate on Single-Family Residential Mortgages",
               "Quarterly; housing-credit distress. BNPL users overlap "
               "heavily with marginal-credit mortgage cohorts (Di Maggio "
               "et al., 2022)."),
    FredSeries("CSCICP03USM665S", "OECD US Consumer Confidence Indicator",
               "Monthly; OECD-harmonized consumer confidence. Cross-checks "
               "UMCSENT with different methodology."),
    # Sprint Q (2026-04-21): credit-regime gauge for the consumer-credit
    # Gate-3 variant. Selected over BAMLH0A0HYM2 (HY OAS) because FRED's
    # live feed only returns HY OAS from 2023-04 onward — NFCI has
    # continuous weekly history back to 1971, which covers all five event
    # windows including the 2022-06 KLARNA down-round. NFCI is normalized
    # so that 0 = long-run-neutral conditions; positive = tighter than
    # average. Gate 3 in `gate3_mode='credit'` fires when NFCI >= 0.
    FredSeries("NFCI",      "Chicago Fed National Financial Conditions Index",
               "Weekly; 18-input composite (credit, leverage, risk). "
               "Powers the consumer-credit Gate-3 variant in the paper's "
               "blind-spot counterfactual (§8.4)."),
]


# --- HTTP ------------------------------------------------------------------
class FredAPIError(RuntimeError):
    pass


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1.0, min=1, max=30),
    retry=retry_if_exception_type((httpx.HTTPError, FredAPIError)),
)
def _fetch_series(
    series_id: str,
    *,
    observation_start: str | None = None,
    observation_end: str | None = None,
) -> list[tuple[date, float | None]]:
    if not settings.fred_api_key:
        raise FredAPIError("FRED_API_KEY is not set")

    params = {
        "series_id": series_id,
        "api_key": settings.fred_api_key,
        "file_type": "json",
    }
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end

    with httpx.Client(timeout=30.0) as client:
        r = client.get(FRED_URL, params=params)

    if r.status_code == 429:
        # Rate-limited; tenacity will back off and retry.
        raise FredAPIError(f"429 from FRED on {series_id}")
    if r.status_code >= 500:
        raise FredAPIError(f"{r.status_code} from FRED on {series_id}")
    if r.status_code != 200:
        raise FredAPIError(f"FRED returned {r.status_code}: {r.text[:200]}")

    payload = r.json()
    rows: list[tuple[date, float | None]] = []
    for obs in payload.get("observations", []):
        # FRED uses "." for missing values.
        raw = obs.get("value")
        value: float | None
        if raw in (None, ".", ""):
            value = None
        else:
            try:
                value = float(raw)
            except ValueError:
                value = None
        d = datetime.strptime(obs["date"], "%Y-%m-%d").date()
        rows.append((d, value))
    return rows


# --- DuckDB writer ----------------------------------------------------------
def _upsert(con: duckdb.DuckDBPyConnection, series_id: str,
            rows: Iterable[tuple[date, float | None]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    # DuckDB supports INSERT OR REPLACE with a PRIMARY KEY. We use an explicit
    # parameterized executemany to avoid building a huge VALUES clause.
    con.executemany(
        """
        INSERT OR REPLACE INTO fred_series (series_id, observed_at, value)
        VALUES (?, ?, ?)
        """,
        [(series_id, d, v) for d, v in rows],
    )
    return len(rows)


# --- Public entry points ----------------------------------------------------
def ingest_series(series_id: str, *, start: str = "2018-01-01") -> int:
    """Backfill one FRED series from `start` to today. Returns row count."""
    if settings.offline:
        log.info("offline mode; skipping %s", series_id)
        return 0
    rows = _fetch_series(series_id, observation_start=start)
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, series_id, rows)
    finally:
        con.close()
    log.info("fred | %s | upserted %d rows", series_id, n)
    return n


def ingest_all(*, start: str = "2018-01-01") -> dict[str, int]:
    """Backfill every series in the catalog. Returns {series_id: row_count}."""
    results: dict[str, int] = {}
    for s in SERIES:
        try:
            results[s.series_id] = ingest_series(s.series_id, start=start)
        except Exception as e:   # noqa: BLE001 — one bad series shouldn't kill the run
            log.error("fred | %s | FAILED: %s", s.series_id, e)
            results[s.series_id] = -1
    return results


if __name__ == "__main__":
    summary = ingest_all()
    print("FRED ingest summary:")
    for sid, n in summary.items():
        status = "OK " if n >= 0 else "ERR"
        print(f"  {status} {sid:10s} {n:>6d} rows")
