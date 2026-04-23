"""
dashboard/design_tokens.py
─────────────────────────────────────────────────────────────────────────
BNPL Pod — institutional design tokens (Layer 2, Python mirror).

Values MUST stay in lockstep with web/design_tokens.ts. Do not edit in
isolation.

Aesthetic: Stripe / Linear / Bloomberg Terminal. No pure black, no neon
emerald, no drop-shadows, no jagged linear chart interpolation.
─────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Final, Mapping

# ── Surfaces ─────────────────────────────────────────────────────────────
_BG: Final = "#0F172A"              # slate-900 — app background
_CARD: Final = "#1E293B"            # slate-800 — card / panel
_CARD_ELEVATED: Final = "#273449"   # slight lift for nested chips
_BORDER: Final = "#334155"          # slate-700 — 1px card borders
_BORDER_MUTED: Final = "#1F2937"    # subtle dividers inside cards

# ── Semantic accents ────────────────────────────────────────────────────
_ACCENT: Final = "#38BDF8"          # sky-blue — primary / calm / pass
_WARN: Final = "#FBBF24"            # amber — thresholds
_CRITICAL: Final = "#EF4444"        # red — breach, bypass fired
_VIOLET: Final = "#8B5CF6"          # QUANT agent avatar

# ── Text ─────────────────────────────────────────────────────────────────
_TEXT_PRIMARY: Final = "#F8FAFC"    # slate-50
_TEXT_SECONDARY: Final = "#94A3B8"  # slate-400
_TEXT_MUTED: Final = "#64748B"      # slate-500

# ── Chart fills ──────────────────────────────────────────────────────────
_CHART_FILL: Final = "#38BDF8"
_CHART_AXIS: Final = "#334155"
_CHART_GRID: Final = "#1F2937"

C: Final[Mapping[str, str]] = MappingProxyType(
    {
        "bg": _BG,
        "card": _CARD,
        "cardElevated": _CARD_ELEVATED,
        "border": _BORDER,
        "borderMuted": _BORDER_MUTED,
        "accent": _ACCENT,
        "warn": _WARN,
        "critical": _CRITICAL,
        "violet": _VIOLET,
        "textPrimary": _TEXT_PRIMARY,
        "textSecondary": _TEXT_SECONDARY,
        "textMuted": _TEXT_MUTED,
        "chartFill": _CHART_FILL,
        "chartAxis": _CHART_AXIS,
        "chartGrid": _CHART_GRID,
    }
)

FONT: Final[Mapping[str, str]] = MappingProxyType(
    {
        "sans": "Inter, system-ui, -apple-system, sans-serif",
        "mono": "'JetBrains Mono', 'Fira Code', ui-monospace, monospace",
    }
)

RADIUS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "sm": "0.25rem",
        "md": "0.375rem",  # default card / chip / button
        "lg": "0.5rem",
    }
)

# ── Agent role colors (shared between chat renderers) ────────────────────
AGENT_COLORS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "MACRO": _ACCENT,
        "QUANT": _VIOLET,
        "RISK": _WARN,
        # Fallbacks
        "ALL": _TEXT_SECONDARY,
        "UNKNOWN": _TEXT_MUTED,
    }
)


__all__ = ["C", "FONT", "RADIUS", "AGENT_COLORS"]
