"""
Quant Agent — reads Heston SCP and JT hazard outputs, emits the quant-layer
inputs the compliance engine needs for G2 (SCP).

Responsibilities
----------------
- Pull latest z_scp per treated ticker from `scp_daily`. G2 fires if
  z_scp > Φ⁻¹(0.90) ≈ 1.2816 (see quant.heston_scp.GATE_Z).
- Pull latest lambda_total per issuer from `jt_lambda` — surfaces pricing
  context; the engine uses it indirectly via tranche PV downstream.
- Optional LLM advisory summarizes the vol-premium regime across tickers.

NOTE on the compliance-engine contract: the engine's `GateInputs.scp_by_ticker`
expects the raw SCP value, not the z-score. We forward `z_scp` as the value
BECAUSE thresholds.yaml uses `min_scp_equity_layer: 2.50` calibrated for
z-units. If/when thresholds.yaml moves to dollar-PV units, swap the column.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, Optional

import duckdb

from agents.schemas import QuantReport
from data.settings import settings
from quant.heston_scp import GATE_Z as SCP_GATE_Z

log = logging.getLogger(__name__)

TREATED_TICKERS = ("AFRM", "SQ", "PYPL", "SEZL", "UPST")


def _latest_scp(con: duckdb.DuckDBPyConnection, as_of: datetime,
                tickers: Iterable[str]) -> tuple[dict[str, float], dict[str, bool]]:
    scp: dict[str, float] = {}
    gate: dict[str, bool] = {}
    for t in tickers:
        row = con.execute(
            """SELECT z_scp FROM scp_daily
               WHERE ticker=? AND observed_at<=? AND z_scp IS NOT NULL
               ORDER BY observed_at DESC LIMIT 1""",
            [t, as_of.date()],
        ).fetchone()
        if row and row[0] is not None:
            z = float(row[0])
            scp[t] = z
            gate[t] = z > SCP_GATE_Z
    return scp, gate


def _latest_lambda(con: duckdb.DuckDBPyConnection, as_of: datetime,
                   issuers: Iterable[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for iss in issuers:
        row = con.execute(
            """SELECT lambda_total FROM jt_lambda
               WHERE issuer=? AND observed_at<=? AND lambda_total IS NOT NULL
               ORDER BY observed_at DESC LIMIT 1""",
            [iss, as_of.date()],
        ).fetchone()
        if row and row[0] is not None:
            out[iss] = float(row[0])
    return out


def _build_prompt(scp: dict[str, float], gates: dict[str, bool],
                  lambdas: dict[str, float]) -> tuple[str, str]:
    system = (
        "You are the Quant Agent in a BNPL credit-stress pod. "
        "Summarize the SCP vol-premium state and JT hazard levels in 3-4 sentences. "
        "Be quantitative. Advisory only — do not recommend trades."
    )
    scp_lines = "\n".join(f"  {t}: z_SCP={z:+.2f}  gate={'FIRE' if gates.get(t) else 'hold'}"
                          for t, z in sorted(scp.items()))
    lam_lines = "\n".join(f"  {i}: λ_total={v:.4f}" for i, v in sorted(lambdas.items()))
    user = (
        f"SCP (higher z = richer implied vol over realized):\n{scp_lines or '  (no data)'}\n\n"
        f"JT hazard intensities (capped at J_max=5%):\n{lam_lines or '  (no data)'}\n\n"
        "Compact quant read. Highlight which tickers look stretched on vol premium."
    )
    return system, user


def run(as_of: Optional[datetime] = None,
        tickers: Iterable[str] = TREATED_TICKERS,
        llm=None) -> QuantReport:
    as_of = as_of or datetime.now(timezone.utc)
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        scp, gates = _latest_scp(con, as_of, tickers)
        lambdas = _latest_lambda(con, as_of, tickers)
    finally:
        con.close()

    advisory = ""
    if llm is not None:
        try:
            system, user = _build_prompt(scp, gates, lambdas)
            resp = llm.chat(system=system, user=user, tier="small", temperature=0.0, role="quant")
            advisory = resp.text.strip()
        except Exception as e:   # noqa: BLE001
            log.warning("quant_agent advisory failed: %s", e)

    log.info("quant | SCP fires=%s | λ_total keys=%s",
             sorted(t for t, g in gates.items() if g),
             sorted(lambdas))
    return QuantReport(as_of=as_of,
                       scp_by_ticker=scp,
                       scp_gate_fires=gates,
                       lambda_total_by_issuer=lambdas,
                       advisory=advisory)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    r = run()
    print("SCP:", r.scp_by_ticker)
    print("Lambda:", r.lambda_total_by_issuer)
