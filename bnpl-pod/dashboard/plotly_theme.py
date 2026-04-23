"""
dashboard/plotly_theme.py
─────────────────────────────────────────────────────────────────────────
Registers an "institutional" Plotly template sourced from design_tokens.py.

Import side-effect: the template is registered and set as the process-wide
default. After import, every new go.Figure() inherits this look unless the
caller explicitly overrides ``template=``.

Usage:
    import dashboard.plotly_theme  # noqa: F401  — registers + activates

    fig = go.Figure(...)
    # already themed; no extra update_layout needed for basic chart.

    # To apply to an existing figure built elsewhere:
    from dashboard.plotly_theme import apply_theme
    fig = apply_theme(fig)
─────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from typing import Any

import plotly.graph_objects as go
import plotly.io as pio

from dashboard.design_tokens import C, FONT

_TEMPLATE_NAME = "institutional"


def _build_template() -> go.layout.Template:
    """Build the institutional template from tokens."""
    return go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=C["bg"],
            plot_bgcolor=C["bg"],
            font=dict(
                family=FONT["sans"],
                color=C["textPrimary"],
                size=12,
            ),
            colorway=[
                C["accent"],      # sky — primary series
                C["warn"],        # amber — threshold / stress overlay
                C["critical"],    # red — breach / bypass fired
                C["violet"],      # QUANT overlay
                C["textSecondary"],  # muted
            ],
            hoverlabel=dict(
                bgcolor=C["card"],
                bordercolor=C["border"],
                font=dict(family=FONT["mono"], color=C["textPrimary"], size=12),
            ),
            xaxis=dict(
                gridcolor=C["chartGrid"],
                zerolinecolor=C["border"],
                linecolor=C["chartAxis"],
                tickfont=dict(family=FONT["mono"], color=C["textSecondary"], size=11),
                title=dict(font=dict(family=FONT["sans"], color=C["textSecondary"], size=12)),
                showgrid=True,
                showline=False,
                ticks="outside",
                tickcolor=C["chartAxis"],
            ),
            yaxis=dict(
                gridcolor=C["chartGrid"],
                zerolinecolor=C["border"],
                linecolor=C["chartAxis"],
                tickfont=dict(family=FONT["mono"], color=C["textSecondary"], size=11),
                title=dict(font=dict(family=FONT["sans"], color=C["textSecondary"], size=12)),
                showgrid=True,
                showline=False,
                ticks="outside",
                tickcolor=C["chartAxis"],
            ),
            legend=dict(
                bgcolor="rgba(15,23,42,0.65)",  # bg with alpha for overlay
                bordercolor=C["border"],
                borderwidth=1,
                font=dict(family=FONT["sans"], color=C["textPrimary"], size=11),
            ),
            margin=dict(l=48, r=24, t=40, b=40),
            # Consistent title styling
            title=dict(
                font=dict(family=FONT["sans"], color=C["textPrimary"], size=14),
                x=0.02,
                xanchor="left",
            ),
        )
    )


def register() -> None:
    """Register the template and set it as the default."""
    pio.templates[_TEMPLATE_NAME] = _build_template()
    pio.templates.default = _TEMPLATE_NAME


def apply_theme(fig: go.Figure, **overrides: Any) -> go.Figure:
    """
    Apply the institutional template to an existing figure.

    Convenience for code paths that build a figure with a different default
    (e.g. third-party helpers). Pass ``**overrides`` to update_layout after.
    """
    fig.update_layout(template=_TEMPLATE_NAME)
    if overrides:
        fig.update_layout(**overrides)
    return fig


# Register on import — single source of truth for every Plotly figure
# produced in the Streamlit dashboard.
register()


__all__ = ["register", "apply_theme"]
