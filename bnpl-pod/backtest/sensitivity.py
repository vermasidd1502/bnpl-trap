"""
Sprint P / Q — Gate-3 threshold sensitivity driver.

Runs the 5-window event study under three Gate-3 specifications:
  * absolute : scalar MOVE-MA30 threshold of 120 (paper baseline).
  * dynamic  : rolling 85th-percentile of MOVE-MA30 over the trailing
               504 business days, computed from the full warehouse MOVE
               history with a one-day causal shift (no look-ahead).
  * credit   : replace the MOVE signal entirely with a 180d causal
               rolling z-score of HY OAS (FRED BAMLH0A0HYM2). Gate
               fires when z >= +1.0σ. Motivated by the 2025-01-17
               blind-spot finding: on the empirical stress-peak day
               MOVE=94 (calm) but BSI=+44σ (parabolic), and no macro
               corroboration under MOVE-gated rule. The credit regime
               gauge captures the signal MOVE cannot see.

Emits:
  backtest/outputs/absolute/   <- full per-window pnl_*.csv + summary.csv
  backtest/outputs/dynamic/    <- same, under dynamic Gate-3
  backtest/outputs/credit/     <- same, under credit Gate-3
  backtest/outputs/summary_sensitivity.csv   <- side-by-side comparison

Run:  python -m backtest.sensitivity
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path

from backtest.event_study import (
    PnLMode,
    load_all_windows_from_warehouse,
    run_all_windows,
)

log = logging.getLogger(__name__)

OUT = Path(__file__).resolve().parent / "outputs"


def _run_one(mode: str, subdir: str) -> dict:
    """Run all 5 windows under `mode` ('absolute' | 'dynamic' | 'credit')
    and return a dict {window: ThreePanelComparison}."""
    log.info("sensitivity | building fixtures (gate3_mode=%s)", mode)
    fixtures = load_all_windows_from_warehouse(gate3_mode=mode)
    out_dir = OUT / subdir
    out_dir.mkdir(parents=True, exist_ok=True)
    return run_all_windows(fixtures, out_dir=out_dir)


def _panel_approved(cmp) -> dict[str, int]:
    """Extract approved-day counts per panel from a comparison result."""
    out: dict[str, int] = {}
    for panel_name, panel in cmp.panels.items():
        out[panel_name] = int(panel.gate_approved.sum())
    return out


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    abs_results = _run_one("absolute", "absolute")
    dyn_results = _run_one("dynamic", "dynamic")
    cr_results  = _run_one("credit",   "credit")

    # Side-by-side table — three Gate-3 modes per (window, panel). TRS total
    # return per mode is included so the paper can cite the approved-day /
    # P&L combination in a single CSV.
    rows = []
    for win in abs_results.keys():
        a_app = _panel_approved(abs_results[win])
        d_app = _panel_approved(dyn_results[win])
        c_app = _panel_approved(cr_results[win])
        for panel in ("naive", "fix3_only", "institutional"):
            # ThreePanelComparison.panels is keyed by PnLMode enum (values
            # are the same lowercase strings we iterate over here).
            mode_enum = PnLMode(panel)
            abs_trs = abs_results[win].panels[mode_enum].trs_stats.total_return
            dyn_trs = dyn_results[win].panels[mode_enum].trs_stats.total_return
            cr_trs  = cr_results[win].panels[mode_enum].trs_stats.total_return
            rows.append({
                "window": win,
                "panel": panel,
                "abs_approved_days": a_app.get(panel, 0),
                "dyn_approved_days": d_app.get(panel, 0),
                "credit_approved_days": c_app.get(panel, 0),
                "abs_trs_return":    float(abs_trs),
                "dyn_trs_return":    float(dyn_trs),
                "credit_trs_return": float(cr_trs),
                "delta_credit_minus_abs": c_app.get(panel, 0) - a_app.get(panel, 0),
            })
    out_path = OUT / "summary_sensitivity.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    log.info("sensitivity | wrote %s (%d rows)", out_path, len(rows))

    # Console summary. ASCII-only header — Windows cp1252 default console
    # encoding throws UnicodeEncodeError on the Greek delta glyph we used
    # previously. The CSV keeps the full word `delta` anyway, so there's no
    # information loss from the ASCII render.
    print("\n=== Gate-3 sensitivity (approved days / 61  |  TRS cum. return) ===")
    print(f"{'window':22s} {'panel':14s} "
          f"{'abs':>4s} {'dyn':>4s} {'cr':>4s}  "
          f"{'abs_ret':>8s} {'dyn_ret':>8s} {'cr_ret':>8s}")
    print("-" * 82)
    for r in rows:
        print(f"{r['window']:22s} {r['panel']:14s} "
              f"{r['abs_approved_days']:>4d} {r['dyn_approved_days']:>4d} "
              f"{r['credit_approved_days']:>4d}  "
              f"{r['abs_trs_return']:>+8.4f} {r['dyn_trs_return']:>+8.4f} "
              f"{r['credit_trs_return']:>+8.4f}")


if __name__ == "__main__":
    main()
