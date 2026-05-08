"""Render the BSI architecture as a clean PNG diagram.

Eight alt-data pillars on the left, BSI composite in the middle, five
compliance gates on the right. Saved to docs/architecture.png at 300 dpi.

Usage:
    python scripts/render_architecture_diagram.py
"""
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "docs" / "architecture.png"
OUT.parent.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Theme (matches the BSI deck)
# ============================================================================
BG_CREAM   = "#F5EFE6"
BG_PANEL   = "#FBF7EF"
ORANGE     = "#E84A27"   # Illinois orange — pillars
ORANGE_DK  = "#B8341E"
SLATE      = "#4A6E8F"   # BSI composite
GOLD       = "#B8841C"   # gates header accent
GREEN      = "#2D7A4F"
PLUM       = "#6B4A7F"
TEXT       = "#222222"
TEXT_DIM   = "#555555"
TEXT_FAINT = "#888888"

PILLARS = [
    "CFPB complaints",
    "App Store reviews",
    "Reddit posts",
    "Bluesky posts",
    "Google Trends",
    "ABS tranche signals",
    "Macro confound",
    "Firm vitality",
]

GATES = [
    ("G1  BSI",       "z ≥ 2.0",                "consumer-distress signal"),
    ("G2  SCP",       "spread + vol  z ≥ 1.5",  "issuer market-implied stress"),
    ("G3  MOVE",      "MOVE z ≤ 1.0",           "macro-rates regime calm"),
    ("G4  CCD",       "divergence ≥ 0.30",      "consumer vs. corporate credit"),
    ("G5  FDS",       "NCO + provisions z ≥ 1.5", "EDGAR XBRL fundamentals"),
]

# ============================================================================
# Figure setup
# ============================================================================
fig, ax = plt.subplots(figsize=(14, 8.5), dpi=300)
fig.patch.set_facecolor(BG_CREAM)
ax.set_facecolor(BG_CREAM)
ax.set_xlim(0, 14)
ax.set_ylim(0, 9)
ax.axis('off')

# ============================================================================
# Title strip
# ============================================================================
ax.text(7.0, 8.55, "Behavioural Stress Index — Architecture",
        ha='center', va='center', fontsize=20, fontweight='bold',
        family='serif', color=TEXT)
ax.text(7.0, 8.10, "8 alt-data pillars  →  BSI composite  →  5 compliance gates  (all five must fire)",
        ha='center', va='center', fontsize=11.5, style='italic', color=TEXT_DIM,
        family='sans-serif')

# Hairline under title
ax.plot([0.5, 13.5], [7.7, 7.7], color=ORANGE, linewidth=1.4, alpha=0.7)

# ============================================================================
# Helper to draw a rounded box
# ============================================================================
def rbox(x, y, w, h, fc, ec, lw=1.5, alpha=1.0):
    p = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.02,rounding_size=0.10",
        facecolor=fc, edgecolor=ec, linewidth=lw, alpha=alpha,
    )
    ax.add_patch(p)
    return p

def arrow(x1, y1, x2, y2, color=TEXT_FAINT, lw=1.0, alpha=0.55):
    a = FancyArrowPatch(
        (x1, y1), (x2, y2),
        arrowstyle='-|>', mutation_scale=11,
        color=color, linewidth=lw, alpha=alpha,
        shrinkA=2, shrinkB=2,
    )
    ax.add_patch(a)

# ============================================================================
# LEFT COLUMN — eight pillars
# ============================================================================
col_x = 0.4
box_w = 2.7
box_h = 0.55
gap   = 0.18
total_h = 8 * box_h + 7 * gap
start_y = 7.0 - 0.3
# vertical center the pillars within plot from y=0.6 to y=7.4 ish
start_y = 6.95
pillar_centers = []
for i, name in enumerate(PILLARS):
    y = start_y - i * (box_h + gap)
    rbox(col_x, y, box_w, box_h, "#FFFFFF", ORANGE, lw=1.4)
    ax.text(col_x + box_w / 2, y + box_h / 2, name,
            ha='center', va='center', fontsize=10.5, color=TEXT, fontweight='bold',
            family='sans-serif')
    pillar_centers.append((col_x + box_w, y + box_h / 2))

# Section header
ax.text(col_x + box_w / 2, start_y + box_h + 0.3,
        "8  ALT-DATA PILLARS",
        ha='center', va='bottom', fontsize=10.5, fontweight='bold',
        color=ORANGE_DK, family='monospace')

# ============================================================================
# MIDDLE — BSI composite box
# ============================================================================
mid_x = 4.6
mid_w = 4.0
mid_y_top = 5.7
mid_y_bot = 2.7
mid_h = mid_y_top - mid_y_bot

# Outer composite box
rbox(mid_x, mid_y_bot, mid_w, mid_h, BG_PANEL, SLATE, lw=2.2)

# Header
ax.text(mid_x + mid_w / 2, mid_y_top - 0.32,
        "BSI COMPOSITE",
        ha='center', va='center', fontsize=11, fontweight='bold',
        color=SLATE, family='monospace')

# Pipeline steps inside the composite box
steps = [
    ("①  winsorise tails  (1% / 99%)",        TEXT_DIM),
    ("②  exponentially-weighted  (30-day half-life)", TEXT_DIM),
    ("③  z-score vs. 252-day rolling baseline", TEXT_DIM),
    ("④  weighted sum  (pre-registered)",      TEXT_DIM),
]
step_y0 = mid_y_top - 0.85
for i, (s, c) in enumerate(steps):
    y = step_y0 - i * 0.42
    ax.text(mid_x + 0.30, y, s, ha='left', va='center',
            fontsize=10.0, color=c, family='sans-serif')

# Bottom callout
ax.text(mid_x + mid_w / 2, mid_y_bot + 0.45,
        "BSI z-score",
        ha='center', va='center', fontsize=14, fontweight='bold',
        color=SLATE, family='serif')
ax.text(mid_x + mid_w / 2, mid_y_bot + 0.20,
        "daily, per firm",
        ha='center', va='center', fontsize=9.5, color=TEXT_DIM,
        family='sans-serif', style='italic')

# Arrows from pillars to composite
mid_left  = (mid_x, (mid_y_top + mid_y_bot) / 2)
for px, py in pillar_centers:
    arrow(px + 0.05, py, mid_left[0] - 0.05, mid_left[1] - 0.0,
          color=ORANGE, lw=0.9, alpha=0.45)

# ============================================================================
# RIGHT COLUMN — five gates
# ============================================================================
right_x = 9.5
right_w = 4.1
gate_h = 0.85
gate_gap = 0.18
total_gate_h = 5 * gate_h + 4 * gate_gap
gate_start_y = 6.55

gate_colors = [SLATE, GOLD, PLUM, GREEN, ORANGE_DK]
gate_inputs = []
for i, ((title, threshold, sub), color) in enumerate(zip(GATES, gate_colors)):
    y = gate_start_y - i * (gate_h + gate_gap)
    # gate box
    rbox(right_x, y, right_w, gate_h, "#FFFFFF", color, lw=1.6)
    # Gate title (left)
    ax.text(right_x + 0.20, y + gate_h - 0.22, title,
            ha='left', va='center', fontsize=11, fontweight='bold',
            color=color, family='monospace')
    # Threshold (right)
    ax.text(right_x + right_w - 0.18, y + gate_h - 0.22, threshold,
            ha='right', va='center', fontsize=10, color=TEXT,
            family='monospace', fontweight='bold')
    # Sub-line
    ax.text(right_x + 0.20, y + 0.22, sub,
            ha='left', va='center', fontsize=9.0, color=TEXT_DIM,
            family='sans-serif', style='italic')
    gate_inputs.append((right_x, y + gate_h / 2))

# Section header
ax.text(right_x + right_w / 2, gate_start_y + gate_h + 0.3,
        "5  COMPLIANCE GATES",
        ha='center', va='bottom', fontsize=10.5, fontweight='bold',
        color=GOLD, family='monospace')

# Arrows from BSI composite to each gate
mid_right = (mid_x + mid_w, (mid_y_top + mid_y_bot) / 2)
for gx, gy in gate_inputs:
    arrow(mid_right[0] + 0.05, mid_right[1] - (mid_right[1] - gy) * 0.1, gx - 0.05, gy,
          color=SLATE, lw=0.9, alpha=0.45)

# ============================================================================
# Bottom callout — "ALL FIVE MUST FIRE"
# ============================================================================
callout_y = 1.05
callout_w = 6.5
callout_x = 14 / 2 - callout_w / 2
rbox(callout_x, callout_y, callout_w, 0.75,
     BG_PANEL, ORANGE, lw=2.0)
ax.text(14 / 2, callout_y + 0.46,
        "ALL FIVE GATES MUST FIRE BEFORE ANY TRADE GOES LIVE",
        ha='center', va='center', fontsize=11, fontweight='bold',
        color=ORANGE_DK, family='monospace')
ax.text(14 / 2, callout_y + 0.18,
        "weights, thresholds, and gate logic are pre-registered constants  ·  anti-HARKing by design",
        ha='center', va='center', fontsize=9.5, color=TEXT_DIM,
        family='sans-serif', style='italic')

# ============================================================================
# Footer attribution
# ============================================================================
ax.text(14 / 2, 0.30,
        "Verma (2026) — A Deterministic Monitoring System for Consumer-Credit Stress  ·  github.com/vermasidd1502/bnpl-trap",
        ha='center', va='center', fontsize=8.5, color=TEXT_FAINT,
        family='monospace')

# ============================================================================
# Save
# ============================================================================
plt.savefig(str(OUT), dpi=300, bbox_inches='tight', facecolor=BG_CREAM)
plt.close()
print(f"OK -> {OUT}")
print(f"size: {OUT.stat().st_size:,} bytes")
