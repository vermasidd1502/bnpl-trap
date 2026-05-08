# Reproducing the Behavioural Stress Index Warehouse

This document explains how to rebuild the data warehouse from scratch using only the scripts in this repository plus public data sources. It is intended for graders, reviewers, and academic peers verifying the empirical results in the paper.

---

## What is reproducible from this repo alone

| Pillar | Source | Run-time | Repro? |
|---|---|---|---|
| CFPB consumer complaints | CFPB public API | ~30 min | ✅ Fully reproducible |
| FRED macro series | St. Louis Fed FRED API | ~5 min | ✅ Fully reproducible |
| SEC EDGAR filings | SEC EDGAR full-text + RSS | ~20 min | ✅ Fully reproducible |
| Google Trends | pytrends | ~10 min | ✅ Fully reproducible (rate-limited) |
| ABS trustee reports | Manual SEC EDGAR (10-D forms) | ~1 hr | ⚠️ Reproducible (depends on issuer disclosure) |
| Apple App Store reviews | Apple ToS prohibits redistribution; ingest script provided | ~30 min | ⚠️ Re-scrape required |
| Reddit posts | Reddit ToS prohibits redistribution; ingest script provided | ~1 hr | ⚠️ Re-scrape required |
| Bluesky posts | Bluesky ToS — research-only redistribution | ~30 min | ⚠️ Re-scrape required |
| Bloomberg AFRMT tranche prices | Bloomberg academic licence — not redistributable | n/a | ❌ Available privately on request |

A grader running `bash scripts/run_all_ingest.sh` from a clean checkout reproduces ~90% of the warehouse. The remaining 10% (Bloomberg tranche prices, Twitter/X archived snapshots) is documented but not redistributed for licensing reasons.

---

## Prerequisites

```bash
# Python 3.10+
python --version

# Required packages
pip install duckdb pandas requests pytrends pyyaml python-dotenv tqdm beautifulsoup4 lxml sec-cik-mapper
```

Optional but recommended:
- `MiKTeX` or `TeX Live` (for paper rebuild)
- `git` 2.30+ (this repo)

API keys / credentials:
- **FRED API key** — free, register at https://fredaccount.stlouisfed.org/
- **No key needed** for CFPB or SEC EDGAR (rate-limited public endpoints)
- **Reddit API** — credentials required for the social ingest script (free; PRAW)

Place credentials in `bnpl-pod/.env` (template at `bnpl-pod/.env.example`).

---

## End-to-end rebuild

### Option A — One command

```bash
cd bnpl-pod
bash ../scripts/run_all_ingest.sh
```

This runs every public-source ingest in dependency order and produces `bnpl-pod/data/warehouse.duckdb`. Expected runtime: 90–120 minutes on a typical workstation.

### Option B — Step by step

```bash
cd bnpl-pod

# 1. Initialise the DuckDB schema
python -m data.schema --init

# 2. CFPB complaints (largest source ~500k rows)
python -m data.ingest.cfpb --start 2018-01-01 --end 2026-04-30

# 3. FRED macro series
python -m data.ingest.fred

# 4. SEC EDGAR filings index
python -m data.ingest.sec_edgar

# 5. Google Trends
python -m data.ingest.trends

# 6. ABS trustee reports (10-D filings)
python -m data.ingest.abs_parser

# 7. App Store reviews (rate-limited)
python -m data.ingest.app_store_rss

# 8. Reddit + Bluesky social
python scripts/ingest_reddit_bluesky.py
```

Each step writes to `data/warehouse.duckdb` idempotently. Re-running a step does not duplicate rows (uses `INSERT … ON CONFLICT`).

---

## Computing the BSI signal

After ingest:

```bash
cd bnpl-pod

# Compute daily BSI z-scores
python -m signals.bsi --output data/bsi_daily.csv

# Run gate evaluation for each (firm, date)
python -m signals.gates --output data/gate_states.csv

# Run panel regression + robustness suite
python -m paper_formal.run_all
```

Output:
- `bsi_daily.csv` — one row per (firm, date) with the eight pillar z-scores plus the composite BSI z
- `gate_states.csv` — PASS/FAIL/UNKNOWN per gate per firm-day
- `paper_formal/results/*_v21.csv` — pre-registered v21-tagged result tables matching the paper

---

## Verification

To verify your rebuild matches the paper's reported numbers:

| Paper claim | How to verify | Reference |
|---|---|---|
| 5/5 events caught | Open `paper_formal/results/event_study_summary_v21.csv` | §6 sensitivity table |
| 23/27 Granger F-tests reject | Open `paper_formal/results/granger_aggregate_v21.csv` | §6 Granger table |
| β = −0.082, DK p = 0.007 | Open `paper_formal/results/h1_panel_v21.csv` | §7 panel regression |
| 6/6 SE robustness suite | Open `paper_formal/results/sensitivity_full_v21.csv` | §10 robustness |

Within-rounding match expected. Exact replication requires identical ingest snapshots; small differences may arise from CFPB/Reddit data drift between repro runs.

---

## What you cannot rebuild from public sources

These data products are documented in the paper but require licensed access:

- **Bloomberg AFRMT 2025-1A-C / 2025-2X-C / 2026-2A-C tranche prices** — available privately to graders via Dropbox; the paper's §8 case study uses these.
- **Pre-curated Twitter / X snapshots** — Twitter API access changed in 2024; we used Internet Archive snapshots that are not redistributable in bulk.
- **Pre-built DuckDB warehouse** (805 MB) — Available privately to graders via Dropbox to skip the ~90-minute rebuild.

Contact `sverma24@illinois.edu` for the private grader bundle.

---

## Common issues

**`duckdb.IOException: Could not write to file`**
Another process has the warehouse open. Close any pod / dashboard sessions before re-running ingest.

**`requests.exceptions.HTTPError 429` from CFPB API**
Rate-limited. Set `CFPB_RATE_LIMIT_SLEEP=2` in `.env` and re-run.

**`No module named 'data'`**
You forgot to `cd bnpl-pod` before invoking `python -m data.…`. The package is rooted at `bnpl-pod/`.

**Reddit/Bluesky ingest hangs**
Rate limits + retry backoff. Expected. Let it run.

---

## Deterministic output

The BSI specification, weights, gate thresholds, and regression formulas are all hash-locked constants in `bnpl-pod/signals/gates.py` and the paper's Appendix A. Re-running the analytical layer (BSI computation, gate evaluation, regressions) on the same input warehouse should produce **bit-identical** output CSVs.

If your rebuild produces different headline numbers, the source of difference is almost always ingest-side (CFPB drift, scrape-time variation), not analytical-side.
