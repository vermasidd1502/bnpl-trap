"""
Counterfactual: What if the macro-vol gate (MOVE) were removed?

Motivation (Sprint P, post-Reg-Z finding)
-----------------------------------------
On 2025-01-17 — the Regulation Z compliance deadline for BNPL lenders —
BSI_z spiked to +44σ (driven by 12,838 BNPL complaints vs. <60/day baseline).
Yet every macro gauge was calm:

    MOVE_MA30  ≈  94      (threshold 120, absolute; p85 rolling threshold
                          never crossed either)
    HY OAS     ≈  2.64 %  (tight, near cycle lows)
    SOFR, HYG — both quiet

The 4-gate live strategy therefore did not approve a trade — by design, it
refuses to fire on idiosyncratic signals without macro corroboration. But
this *is* the Subprime-2.0 blind-spot thesis in action: stress concentrated
in an opaque corner of consumer credit that Treasury-vol doesn't see.

This driver runs the same 5-window event study with Gate 3 relaxed
(`move_ma30_threshold = 0.0`) so the MOVE gate always passes. The resulting
panel is a clean counterfactual showing the approved-days and simulated
P&L that a BNPL-specific (not macro-vol-gated) execution would have
delivered. It is NOT the recommended live-strategy variant — it is the
paper's illustration of how much signal the macro gate is discarding.

Run:  python -m backtest.counterfactual_nomove
Writes:
    backtest/outputs/nomove/pnl_<win>_<panel>.csv
    backtest/outputs/nomove/summary.csv
    backtest/outputs/summary_counterfactual_nomove.csv
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path

from backtest.event_study import (
    load_all_windows_from_warehouse,
    run_all_windows,
)

log = logging.getLogger(__name__)

OUT = Path(__file__).resolve().parent / "outputs"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    # Baseline fixtures use absolute Gate 3; we override threshold=0.0 below
    # so gate_move is unconditionally True. Gates 1 (BSI) and 4 (catalyst)
    # remain active.
    fixtures = load_all_windows_from_warehouse(gate3_mode="absolute")
    out_dir = OUT / "nomove"
    out_dir.mkdir(parents=True, exist_ok=True)

    results = run_all_windows(
        fixtures,
        out_dir=out_dir,
        move_ma30_threshold=0.0,   # Gate 3 relaxed — the whole point.
    )

    rows = []
    for name, cmp in results.items():
        for mode, panel in cmp.panels.items():
            t = panel.trs_stats
            n = panel.naive_stats
            rows.append({
                "window": name,
                "panel": mode.value,
                "approved_days": int(panel.gate_approved.sum()),
                "n_days": int(panel.gate_approved.size),
                "gate_bsi_days": int(panel.gate_bsi.sum()),
                "gate_ccd2_days": int(panel.gate_ccd2.sum()),
                "trs_total_return": float(t.total_return),
                "trs_sharpe": float(t.sharpe),
                "trs_max_dd": float(t.max_drawdown),
                "naive_total_return": float(n.total_return),
                "naive_sharpe": float(n.sharpe),
            })

    summary_path = OUT / "summary_counterfactual_nomove.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    log.info("counterfactual_nomove | wrote %s (%d rows)", summary_path, len(rows))

    # Console report. ASCII only — cp1252 console on Windows rejects Greek.
    print("\n=== Gate-3 RELAXED counterfactual (approved days / 61, TRS ret) ===")
    print(f"{'window':22s} {'panel':14s} {'appr':>5s} {'bsi':>5s} "
          f"{'ccd2':>5s} {'trs_ret':>9s} {'trs_sh':>7s} {'naive_ret':>10s}")
    print("-" * 82)
    for r in rows:
        print(f"{r['window']:22s} {r['panel']:14s} "
              f"{r['approved_days']:>5d} {r['gate_bsi_days']:>5d} "
              f"{r['gate_ccd2_days']:>5d} "
              f"{r['trs_total_return']:>+9.4f} {r['trs_sharpe']:>+7.2f} "
              f"{r['naive_total_return']:>+10.4f}")


if __name__ == "__main__":
    main()
