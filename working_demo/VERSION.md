# BearWatch × Apollo Hermes — pod snapshot

**Snapshot taken:** 2026-05-06 16:05
**Codename:** `demo-case-pod_human-intervention`
**Source dir:** `apollo-hermes/working_demo/`

---

## What's in this version

### Pages
- `/` — Overview (USP hero with Klarna 50d / AFRM 73d early examples)
- `/squad` — Bear Squad · 4 mascot deep-profile + AUTO-TRADE LAYER toggle strip
- `/monitor` — Universe Monitor · 25 firms, click-firm modal with **historical BSI z-score chart** (NEW)
- `/equity-monitor` — Equity Monitor · open-universe scanner (Apollo-Hermes purple themed)
- `/macro` — Macro Tracker · regime / yield curve / FRED / sector heatmap / commodities
- `/live` — Live Pod · 4-stage pipeline + Stage 03.5 **HUMAN INTERVENTION** layer (renamed from Tech Override)
- `/portfolio` — Portfolio · NEW Section 00 **P&L history chart** (canvas line chart, hand-rolled, no chart-lib dep)
- `/methodology` — Methodology · BSI v3 spec, EWMA explainer, publication-lag diagnostic, competitive-position cascade
- `/empirical` — Empirical · consolidated empirics page (events, ROBO, stress tests, case studies)
- `/case-study` — CVNA 2022 case-study deep-dive (V8 paper §6.10 + §6.13)
- `/demo-case` — **Demo Case Study Pod** (NEW) — case selector landing
- `/demo-case/<id>` — **Demo Case Study Pod** (NEW) — date-locked replay of 5 cases
- `/v2` — v2 preview
- `/diag` — diagnostic / troubleshooter
- `/profile`, `/login`, `/survey` — user account pages

### Nav
- Top-level: Overview · Universe Monitor · Equity Monitor · Macro Tracker · Live Pod · Portfolio · 📖 Research ▾ · ⚙ diag
- 📖 **Research dropdown** contains: Methodology · Empirical · 🐻 Bear Squad · Case study · CVNA · 📼 Demo Case Pod · v2.0 preview

### Backend features added in this version
- **Auto-trade layer** (`/api/autobot/*`)
  - Background daemon thread starts on pod boot (idle until toggled)
  - Reads user's active mascot dynamically — fires under that mascot's thresholds
  - Provenance tag: `AUTO_<mascot>_z<value>`
  - Endpoints: `/api/autobot/status`, `/api/autobot/toggle`, `/api/autobot/scan_now`, `/api/autobot/config`
  - 24h cooldown per ticker · daily cap (default 5 fires)
- **Per-firm BSI history** (`/api/firm/<ticker>/bsi_history`)
  - CFPB-pillar reduced-form EWMA z-score (paper §5.1)
  - Last 365 days · maps 23 tickers to CFPB company patterns
- **Portfolio P&L history** (`/api/portfolio/pnl_history`)
  - Daily-aggregated cumulative + per-day P&L
  - Mark-to-market on open positions, realised on closed
- **Demo case replay** (`/demo-case`, `/demo-case/<id>`)
  - 5 canonical cases (CVNA-2022, AFRM-2022, KLAR-2024, AFRM-2025, Tricolor-2024)
  - Each rendered as date-locked 4-stage live-pod replay

### Key bug fixes
- **BSI z-score consistency** — `_firm_dict()` now uses live CFPB-derived z (matches `/api/monitor/firms`); previously the popup showed scripted z while the tile showed live z. Same firm now reports same number in both places.

### Branding
- All references to internal advisor names redacted from every artifact in `/Desktop/spring 2026/580/BNPL_v9_FINAL/`
- BSI = **Behavioural Stress Index** (locked — never "BearWatch Stress Index")
- "BearWatch" = project name; "Behavioural Stress Index" = the metric

---

## Restore instructions

To restore this version:
```bash
# Stop running pod first
# Then:
cp -r app.py templates static requirements.txt "C:/Users/siddh/Desktop/apollo-hermes/working_demo/"
# Restart:
cd "C:/Users/siddh/Desktop/apollo-hermes/working_demo" && python app.py
```

The `data/apollo.db` SQLite file is NOT included in this snapshot — it lives in the live `working_demo/data/` and persists user state (portfolio, journal, etc.) across snapshots.

The `data/warehouse.duckdb` is read from `C:/Users/siddh/Desktop/spring 2026/580/BNPL-experimental/bnpl-pod/data/warehouse.duckdb` — also not part of this snapshot.
