"""
Quarterly-to-daily originations interpolation (Phase C.1 of v2_roadmap.md).

The origination-residual BSI (signals/bsi_residual.py) requires a daily-grain
originations series to regress complaint momentum against. Issuer 10-Q / IR
disclosures provide originations at quarterly grain only. This module is the
pre-registered interpolation layer.

Pre-registered choices
----------------------

1.  **Primary interpolation: piecewise-linear, quarter-end anchored.** A
    quarter-end value V_q is taken as the realised value on the last
    business day of the quarter; the daily series on (q_prev, q] is the
    straight-line segment between V_{q-1} and V_q, evaluated on business
    days.
2.  **Calendar:** US business-day calendar (NYSE holidays excluded). No
    intra-quarter holiday adjustment beyond exclusion of non-business
    days from the daily grid.
3.  **Sensitivity alternative: monotone cubic spline.** Implemented as
    `method="pchip"` (Piecewise Cubic Hermite Interpolating Polynomial),
    reported only in Appendix D of the paper as a robustness check. Pre-
    registered here to prevent post-hoc selection of the smoother that
    produces the "nicer" residual.
4.  **Issuer fan-out:** each issuer interpolated independently; the
    composite daily originations series is the sum across issuers with
    coverage on that date. Issuers without coverage on a given date
    contribute zero to the sum and are flagged in `coverage_mask`
    (downstream scorer must not divide by an all-zero denominator
    without gating).

Data contract (input)
---------------------

A pandas DataFrame with columns

    issuer          : str, one of {"AFRM", "SQ", "PYPL", "KLARNA"}
    quarter_end     : date, last calendar day of the fiscal quarter
    gmv_usd         : float, total BNPL originations in USD for the quarter
    source          : str, e.g. "10-Q", "IR-deck", "S-1"
    accession       : str, EDGAR accession number or IR-deck filename

Data contract (output)
----------------------

A pandas DataFrame with columns

    date                  : date, business-day index
    gmv_daily_usd         : float, interpolated daily originations, composite
    gmv_daily_usd_afrm    : float, per-issuer daily originations
    gmv_daily_usd_sq      : float
    gmv_daily_usd_pypl    : float
    gmv_daily_usd_klarna  : float
    coverage_mask         : int, bitfield of which issuers have coverage
                            on this date (1=AFRM, 2=SQ, 4=PYPL, 8=KLARNA)

Implementation status
---------------------

**STAGED, NOT LIVE.** The input DataFrame cannot currently be materialised.
As of 2026-04-23 the warehouse contains:

    - AFRM: zero SEC EDGAR filings (144A private placement, confirmed via
      sec_filings_index warehouse query 2026-04-23).
    - Block/SQ: filings present in warehouse but segment-level Afterpay
      GMV not yet parsed out.
    - PayPal (PYPL): 21 10-Q filings 2019-03-31 through 2025-09-30 in
      sec_filings_index, but Pay-in-4 GMV disclosure is inconsistent
      quarter-to-quarter (per Phase B.3 of v2_roadmap.md).
    - Klarna: pre-IPO US-unregistered; no S-1 or 10-Q available.

This file exposes the interface and the interpolation algorithm. The
per-issuer parse is the Phase B blocker. Once `data/10q/*.parquet` lands
with columns {issuer, quarter_end, gmv_usd}, the `interpolate_daily()`
entrypoint runs unchanged.

Author: Siddharth Verma, UIUC, FIN 580 Spring 2026 cohort.
Provenance: v2 scorer-surgery staging, 2026-04-23.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


Issuer = Literal["AFRM", "SQ", "PYPL", "KLARNA"]

ISSUER_BITFIELD: dict[Issuer, int] = {
    "AFRM": 1,
    "SQ": 2,
    "PYPL": 4,
    "KLARNA": 8,
}


@dataclass(frozen=True)
class InterpolationSpec:
    """Pre-registered interpolation parameters."""

    method: Literal["linear", "pchip"] = "linear"
    calendar: Literal["us-business", "calendar"] = "us-business"
    anchor: Literal["quarter-end", "quarter-mid"] = "quarter-end"
    extrapolate_forward_days: int = 0  # no extrapolation past last datapoint
    extrapolate_backward_days: int = 0

    def is_sensitivity_variant(self) -> bool:
        return self.method == "pchip"


def load_quarterly_originations(
    warehouse_path: str | Path | None = None,
) -> "object":
    """Load the quarterly originations panel from the warehouse.

    Returns a pandas DataFrame matching the input data contract described
    in the module docstring. Raises FileNotFoundError with a disclosure
    string if the underlying parquet files are absent (Phase B blocker).
    """
    import pandas as pd  # local import: this module is a stub

    base = Path(warehouse_path) if warehouse_path else Path("data") / "10q"
    required = [
        base / "afrm_originations.parquet",
        base / "sq_afterpay_segment.parquet",
        base / "pypl_payin4.parquet",
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Quarterly originations parquet(s) missing: "
            + ", ".join(str(p) for p in missing)
            + ". Phase B of docs/v2_roadmap.md is a prerequisite. See "
            "docs/scorer_surgery_result.md for current gating state."
        )
    frames = [pd.read_parquet(p) for p in required]
    return pd.concat(frames, axis=0, ignore_index=True)


def interpolate_daily(
    quarterly: "object",
    spec: InterpolationSpec | None = None,
    start_date: str = "2019-07-01",
    end_date: str = "2026-04-30",
) -> "object":
    """Interpolate a quarterly issuer panel to a daily business-day panel.

    Parameters
    ----------
    quarterly
        DataFrame matching the input contract.
    spec
        Interpolation parameters. Defaults to the pre-registered
        piecewise-linear, quarter-end-anchored, US-business-day spec.
    start_date, end_date
        Inclusive bounds on the output business-day index. The pre-
        registered sample window is 2019-07-01 to 2026-04-30 per
        v2_roadmap.md §A.5.

    Returns
    -------
    DataFrame with the output contract columns.
    """
    import numpy as np  # noqa: F401  (imported for eventual use)
    import pandas as pd

    spec = spec or InterpolationSpec()

    idx = pd.bdate_range(start=start_date, end=end_date, freq="C")

    per_issuer: dict[str, pd.Series] = {}
    coverage: dict[str, pd.Series] = {}

    for issuer, sub in quarterly.groupby("issuer"):
        sub = sub.sort_values("quarter_end")
        q_idx = pd.to_datetime(sub["quarter_end"])
        q_val = sub["gmv_usd"].astype(float).values
        if spec.method == "linear":
            series = pd.Series(q_val, index=q_idx).reindex(idx).interpolate(
                method="time", limit_direction="both"
            )
        elif spec.method == "pchip":
            from scipy.interpolate import PchipInterpolator  # type: ignore

            xs = q_idx.astype("int64").values.astype(float)
            ys = q_val
            interpolator = PchipInterpolator(xs, ys, extrapolate=False)
            series = pd.Series(
                interpolator(idx.astype("int64").astype(float).values),
                index=idx,
            )
        else:
            raise ValueError(f"unknown interpolation method: {spec.method!r}")

        # coverage: 1 where the date is within [min(q_idx), max(q_idx)]
        cov = ((idx >= q_idx.min()) & (idx <= q_idx.max())).astype(int)
        per_issuer[str(issuer)] = series.fillna(0.0)
        coverage[str(issuer)] = pd.Series(cov, index=idx)

    out = pd.DataFrame(index=idx)
    out.index.name = "date"
    for issuer_key in ("AFRM", "SQ", "PYPL", "KLARNA"):
        col = f"gmv_daily_usd_{issuer_key.lower()}"
        out[col] = per_issuer.get(issuer_key, pd.Series(0.0, index=idx))

    out["gmv_daily_usd"] = sum(
        out[f"gmv_daily_usd_{k.lower()}"] for k in ISSUER_BITFIELD
    )

    mask = pd.Series(0, index=idx, dtype=int)
    for issuer_key, bit in ISSUER_BITFIELD.items():
        if issuer_key in coverage:
            mask = mask + coverage[issuer_key] * bit
    out["coverage_mask"] = mask

    return out.reset_index()


def _cli_disclose() -> None:
    """Print the Phase B gating disclosure and exit non-zero."""
    import sys

    print(
        "originations_interp.py is STAGED but not LIVE.\n"
        "\n"
        "Inputs required (per data contract in module docstring):\n"
        "    data/10q/afrm_originations.parquet\n"
        "    data/10q/sq_afterpay_segment.parquet\n"
        "    data/10q/pypl_payin4.parquet\n"
        "\n"
        "Status as of 2026-04-23 (see docs/scorer_surgery_result.md):\n"
        "    AFRM  : zero EDGAR filings (144A private placement)\n"
        "    SQ    : filings present, segment parse pending\n"
        "    PYPL  : 21 10-Qs available, Pay-in-4 parse pending\n"
        "    KLARNA: deferred to robustness (per user 2026-04-22)\n"
        "\n"
        "This module's interpolate_daily() runs unchanged once the\n"
        "parquets land. The interface is frozen.\n"
    )
    sys.exit(2)


if __name__ == "__main__":
    _cli_disclose()
