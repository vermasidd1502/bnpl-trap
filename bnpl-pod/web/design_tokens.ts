// web/design_tokens.ts
// ─────────────────────────────────────────────────────────────────────────
// BNPL Pod — institutional design tokens (Layer 1).
//
// Single source of truth for the React tear-sheet. Values MUST stay in
// lockstep with dashboard/design_tokens.py. Do not edit in isolation.
//
// Aesthetic reference: Stripe dashboards, Linear, modern Bloomberg Terminal.
// No pure black (#000), no neon emerald, no drop-shadows, no jagged linear
// chart interpolation.
//
// This file is loaded by <script type="text/babel"> in web/index.html with
// data-presets="typescript,react" (babel-standalone). Because browser-side
// Babel does not support ES module resolution, tokens are attached to
// `window` rather than exported. PodTerminal.tsx reads them via `window.C`
// / `window.FONT` / `window.RADIUS`.
// ─────────────────────────────────────────────────────────────────────────

declare global {
  interface Window {
    C: typeof __C;
    FONT: typeof __FONT;
    RADIUS: typeof __RADIUS;
  }
}

const __C = Object.freeze({
  // Surfaces
  bg: "#0F172A",           // slate-900 — app background
  card: "#1E293B",         // slate-800 — card / panel
  cardElevated: "#273449", // slight lift for nested chips
  border: "#334155",       // slate-700 — 1px card borders
  borderMuted: "#1F2937",  // subtle dividers inside cards

  // Semantic accents
  accent: "#38BDF8",   // sky-blue — primary / calm / pass
  warn: "#FBBF24",     // amber — thresholds, dashed reference lines
  critical: "#EF4444", // red — breach, bypass fired
  violet: "#8B5CF6",   // QUANT agent avatar

  // Text
  textPrimary: "#F8FAFC",   // slate-50
  textSecondary: "#94A3B8", // slate-400
  textMuted: "#64748B",     // slate-500 — least-emphasis labels

  // Chart aliases (used by Recharts gradient defs + line strokes)
  chartFill: "#38BDF8",
  chartAxis: "#334155",
  chartGrid: "#1F2937",
});

const __FONT = Object.freeze({
  sans: "'Inter', system-ui, -apple-system, sans-serif",
  mono: "'JetBrains Mono', 'Fira Code', ui-monospace, monospace",
});

const __RADIUS = Object.freeze({
  sm: "0.25rem",
  md: "0.375rem", // default card / chip / button
  lg: "0.5rem",
});

// Attach globally so PodTerminal.tsx can reference without a module system.
(window as any).C = __C;
(window as any).FONT = __FONT;
(window as any).RADIUS = __RADIUS;
