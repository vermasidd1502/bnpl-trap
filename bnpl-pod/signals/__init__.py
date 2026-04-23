"""
Signals module — BSI construction and falsification apparatus.

Canonical entry points (paper v2.0.1):

    bsi.compute_bsi(panel, spec=None)
        Equation (1) of paper §6: coverage-gated EWMA-σ weighted z-score.
    bsi.compute_bsi_from_warehouse(conn=None, spec=None)
        Same but wired to the DuckDB warehouse bsi_daily table.
    bsi_residual.fit_residualisation(...)
        Origination-residualised CFPB-pillar scorer (pre-registered but
        data-gated on Phase B 10-Q pulls; see module docstring).
    placebos.run_all()
        Three pre-registered + three warehouse-refinement placebo sensors,
        run against the live CFPB warehouse. Returns the v2.0.1 paper
        Table tab:placebos-live verbatim.
    granger_mde.compute_mde(...)
        Non-central-F minimum-detectable-effect for the Granger
        F-statistic at pre-registered (n, lags, α, power).
    originations_interp.interpolate_daily(...)
        Quarterly → daily originations interpolation for the residualised
        scorer's denominator.

The paper↔code crosswalk for Equation (1) lives at the top of bsi.py;
every symbol in the paper maps to a named code object in that file.
"""

from signals import bsi, bsi_residual, granger_mde, originations_interp, placebos

__all__ = [
    "bsi",
    "bsi_residual",
    "granger_mde",
    "originations_interp",
    "placebos",
]
