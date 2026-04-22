"""
Risk Manager — reads squeeze_defense warehouse output and prepares the
squeeze-related inputs the compliance engine needs.

Responsibilities
----------------
- Pull latest per-ticker utilization, days_to_cover, IV-skew rank, and
  composite squeeze_score from `squeeze_defense`.
- The `iv_skew_25d` raw value is not a percentile; to feed the compliance
  engine's `squeeze_skew_pctile` field we compute a trailing-252d rank here.
- Flag the `squeeze_veto_candidate` if any ticker's stored `veto` column is
  true (this pre-flags what the compliance engine will likely enforce, but
  the engine itself still owns the final decision).
- Optional LLM advisory narrates the squeeze-risk posture.

Post-Fix #2, squeeze-defense is TELEMETRY ONLY. The pod's only expression
is `trs_junior_abs`; there is no equity-short leg for a squeeze veto to
protect. The compliance engine ignores the squeeze fields entirely. We
still compute and surface them so the paper (§9 Squeeze Defense Layer)
and the dashboard can narrate the equity-side picture alongside the
structured-credit trade.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone, timedelta
from typing import Iterable, Optional

import duckdb

from agents.schemas import RiskReport
from data.settings import settings

log = logging.getLogger(__name__)

TREATED_TICKERS = ("AFRM", "SQ", "PYPL", "SEZL", "UPST")
PCTILE_WINDOW_DAYS = 252


def _rank_pct(con: duckdb.DuckDBPyConnection, ticker: str,
              as_of: datetime, col: str) -> float:
    """Percentile rank of latest observation vs trailing 252d."""
    rows = con.execute(
        f"""SELECT {col} FROM squeeze_defense
            WHERE ticker=? AND observed_at<=?
              AND {col} IS NOT NULL
            ORDER BY observed_at DESC LIMIT ?""",
        [ticker, as_of.date(), PCTILE_WINDOW_DAYS],
    ).fetchall()
    if not rows:
        return 0.0
    vals = [float(r[0]) for r in rows]
    current = vals[0]   # most recent
    below = sum(1 for v in vals if v < current)
    return float(below / len(vals))


def _latest(con: duckdb.DuckDBPyConnection, ticker: str,
            as_of: datetime) -> dict | None:
    row = con.execute(
        """SELECT utilization, days_to_cover, iv_skew_25d,
                  squeeze_score, veto
           FROM squeeze_defense
           WHERE ticker=? AND observed_at<=?
           ORDER BY observed_at DESC LIMIT 1""",
        [ticker, as_of.date()],
    ).fetchone()
    if not row:
        return None
    return {
        "utilization":  row[0],
        "days_to_cover": row[1],
        "iv_skew_25d":  row[2],
        "squeeze_score": row[3],
        "veto":          bool(row[4]) if row[4] is not None else False,
    }


def _build_prompt(util: dict, dtc: dict, skew_p: dict,
                  score: dict, veto_candidate: bool) -> tuple[str, str]:
    system = (
        "You are the Risk Manager Agent in a BNPL credit-stress pod. "
        "Summarize squeeze-defense posture across the treated tickers in 3-4 sentences. "
        "Advisory only."
    )
    lines = []
    for t in sorted(set(util) | set(dtc) | set(skew_p) | set(score)):
        lines.append(
            f"  {t}: util={util.get(t, 0.0):.1%}  DTC={dtc.get(t, 0.0):.2f}d  "
            f"skew_pct={skew_p.get(t, 0.0):.0%}  score={score.get(t, 0.0):.2f}"
        )
    body = "\n".join(lines) or "  (no data)"
    user = (
        f"Squeeze Defense inputs:\n{body}\n\n"
        f"Any ticker already flagged veto: {veto_candidate}\n\n"
        "Compact risk read. Recall TRS expression bypasses squeeze defense."
    )
    return system, user


def run(as_of: Optional[datetime] = None,
        tickers: Iterable[str] = TREATED_TICKERS,
        llm=None) -> RiskReport:
    as_of = as_of or datetime.now(timezone.utc)
    util, dtc, skew_p, score = {}, {}, {}, {}
    veto_candidate = False

    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        for t in tickers:
            latest = _latest(con, t, as_of)
            if latest is None:
                continue
            if latest["utilization"] is not None:
                util[t] = float(latest["utilization"])
            if latest["days_to_cover"] is not None:
                dtc[t] = float(latest["days_to_cover"])
            if latest["squeeze_score"] is not None:
                score[t] = float(latest["squeeze_score"])
            # IV-skew percentile ranked against own history
            skew_p[t] = _rank_pct(con, t, as_of, "iv_skew_25d")
            if latest["veto"]:
                veto_candidate = True
    finally:
        con.close()

    advisory = ""
    if llm is not None:
        try:
            system, user = _build_prompt(util, dtc, skew_p, score, veto_candidate)
            resp = llm.chat(system=system, user=user, tier="small", temperature=0.0, role="risk")
            advisory = resp.text.strip()
        except Exception as e:   # noqa: BLE001
            log.warning("risk_manager advisory failed: %s", e)

    log.info("risk | tickers=%d | veto_candidate=%s", len(score), veto_candidate)
    return RiskReport(
        as_of=as_of,
        squeeze_utilization=util,
        squeeze_days_to_cover=dtc,
        squeeze_skew_pctile=skew_p,
        squeeze_score_by_ticker=score,
        squeeze_veto_candidate=veto_candidate,
        advisory=advisory,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    r = run()
    print("Squeeze score:", r.squeeze_score_by_ticker)
    print("Veto candidate:", r.squeeze_veto_candidate)
