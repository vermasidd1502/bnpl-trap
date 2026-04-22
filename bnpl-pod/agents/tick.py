"""
Pod tick runner — one entry point, one row to `pod_decisions`.

Usage
-----
    python -m agents.tick                    # dry-run (no DB write)
    python -m agents.tick --persist          # writes one row to pod_decisions
    python -m agents.tick --persist --optimize

The runner is intentionally minimal:
  1) Build an LLMClient if keys are present (otherwise agents stay silent).
  2) Call graph.run_graph() to produce a PodDecision.
  3) Optionally persist a single row to `pod_decisions` (idempotent on run_id).
  4) With --optimize, size the Mean-CVaR TRS book AND the macro-hedge
     sleeve (HYG/UST) via portfolio.book.build.

Persistence is OFF by default so CI / test runs do not pollute the warehouse.

Fix #2: `--expression` and `--tickers` are retired. The pod's only trade
expression is `trs_junior_abs` (see agents/compliance_engine.py). Any
market-neutral exposure is carried by the static macro-hedge sleeve sized
inside portfolio.book — never by an equity short leg.
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import duckdb

from agents.graph import run_graph
from agents.schemas import PodDecision
from data.settings import settings

log = logging.getLogger(__name__)


def _maybe_llm():
    """Return an LLMClient if credentials are configured, else None."""
    if settings.offline:
        return None
    if not (settings.nim_api_key or settings.gemini_api_key):
        return None
    try:
        from agents.llm_client import LLMClient
        return LLMClient()
    except Exception as e:   # noqa: BLE001
        log.warning("LLMClient init failed; running deterministic-only: %s", e)
        return None


def _persist(pod: PodDecision, db_path: Optional[str] = None) -> None:
    """Write one row to pod_decisions. INSERT OR REPLACE on run_id.

    DuckDB's TIMESTAMP (WITHOUT TIME ZONE) converts tz-aware datetimes to
    local time on write — which silently shifts UTC pod times by the machine's
    offset and breaks downstream `.date()` lookups. We normalize to UTC-naive
    so the stored instant is the actual UTC wall-clock.
    """
    path = db_path or str(settings.duckdb_path)
    as_of_utc = pod.as_of
    if as_of_utc.tzinfo is not None:
        as_of_utc = as_of_utc.astimezone(timezone.utc).replace(tzinfo=None)
    con = duckdb.connect(path)
    try:
        con.execute(
            """INSERT OR REPLACE INTO pod_decisions (
                 run_id, as_of, bsi, move_ma30, scp_by_ticker_json,
                 gate_bsi, gate_scp, gate_move, gate_ccd2,
                 squeeze_veto, compliance_ok, compliance_reasons,
                 llm_advisory, trade_signal_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                pod.run_id,
                as_of_utc,
                pod.macro.bsi,
                pod.macro.move_ma30,
                json.dumps(pod.quant.scp_by_ticker),
                pod.gate_bsi,
                pod.gate_scp,
                pod.gate_move,
                pod.gate_ccd2,
                pod.squeeze_veto,
                pod.compliance_approved,
                json.dumps(pod.compliance_reasons),
                pod.llm_advisory,
                pod.trade_signal_json,
            ],
        )
    finally:
        con.close()


def run_pod_tick(
    as_of: Optional[datetime] = None,
    *,
    persist: bool = False,
    optimize: bool = False,
    llm=None,
) -> PodDecision:
    """Programmatic entry point. Returns the PodDecision; optionally writes it.

    When ``optimize=True`` AND compliance approved, the Mean-CVaR book is
    sized via ``portfolio.book.build`` (which also writes the macro-hedge
    sleeve to `portfolio_hedges`). Requires ``persist`` — the optimizer
    reads the pod_decisions row it just wrote.
    """
    as_of = as_of or datetime.now(timezone.utc)
    llm = llm if llm is not None else _maybe_llm()
    pod = run_graph(as_of=as_of, llm=llm)
    if persist:
        _persist(pod)
        log.info("persisted pod_decisions row run_id=%s", pod.run_id)

    if optimize:
        if not persist:
            log.warning("--optimize requires --persist; skipping book build")
        elif not pod.compliance_approved:
            log.info("run=%s not approved; no book to optimize", pod.run_id)
        else:
            # Lazy import: avoids cvxpy dependency for plain ticks.
            from portfolio import book as portfolio_book
            portfolio_book.build(pod.run_id, persist=True)
    return pod


def _cli() -> None:
    p = argparse.ArgumentParser(description="Run one BNPL pod tick.")
    p.add_argument("--persist", action="store_true", help="Write row to pod_decisions.")
    p.add_argument("--optimize", action="store_true",
                   help="After approve, size the TRS book + macro-hedge sleeve. "
                        "Requires --persist.")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    pod = run_pod_tick(
        persist=args.persist,
        optimize=args.optimize,
    )
    print(json.dumps({
        "run_id": pod.run_id,
        "approved": pod.compliance_approved,
        "gates": {
            "bsi": pod.gate_bsi,
            "move": pod.gate_move,
            "ccd2": pod.gate_ccd2,
        },
        "scp_telemetry_fires": pod.gate_scp,
        "reasons": pod.compliance_reasons,
        "thresholds_version": pod.thresholds_version,
    }, indent=2))


if __name__ == "__main__":
    _cli()
