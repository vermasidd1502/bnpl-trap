#!/usr/bin/env bash
# ============================================================================
#  run_all_ingest.sh
# ----------------------------------------------------------------------------
#  Master ingest script. Rebuilds the bnpl-pod DuckDB warehouse from public
#  sources end-to-end. Idempotent — safe to re-run; uses INSERT … ON CONFLICT
#  semantics inside each ingest module.
#
#  USAGE (from repo root):
#      bash scripts/run_all_ingest.sh
#
#  Expected runtime: 90–120 min on a typical workstation.
#
#  PREREQUISITES:
#  - Python 3.10+
#  - pip install duckdb pandas requests pytrends pyyaml python-dotenv tqdm
#  - bnpl-pod/.env populated (see bnpl-pod/.env.example)
#
#  See REPRODUCING.md for full prerequisites, verification, and licensing
#  caveats (Bloomberg / App Store / Reddit / Bluesky require additional steps).
# ============================================================================

set -euo pipefail

# Resolve repo root from this script's location
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
POD_DIR="$REPO_ROOT/bnpl-pod"

if [[ ! -d "$POD_DIR" ]]; then
    echo "ERROR: cannot find bnpl-pod/ at $POD_DIR" >&2
    exit 1
fi

cd "$POD_DIR"

# Honour project venv if present
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
elif [[ -f ".venv/Scripts/activate" ]]; then
    # Windows / Git-Bash
    # shellcheck disable=SC1091
    source .venv/Scripts/activate
fi

# Date-stamp every step so the user sees progress
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

log "==================================================================="
log "  BSI warehouse ingest — full rebuild"
log "  Repo:      $REPO_ROOT"
log "  Pod dir:   $POD_DIR"
log "==================================================================="

# ----------------------------------------------------------------------------
# STEP 0 — Initialise the DuckDB schema
# ----------------------------------------------------------------------------
log "STEP 0/8  ·  initialising warehouse schema"
python -m data.schema --init || {
    log "WARN: schema init returned non-zero (may be safe if tables already exist)"
}

# ----------------------------------------------------------------------------
# STEP 1 — CFPB consumer complaints (largest, run first)
# ----------------------------------------------------------------------------
log "STEP 1/8  ·  CFPB consumer complaints  (~30 min, ~500k rows)"
python -m data.ingest.cfpb --start 2018-01-01 --end 2026-04-30

# ----------------------------------------------------------------------------
# STEP 2 — FRED macroeconomic series
# ----------------------------------------------------------------------------
log "STEP 2/8  ·  FRED macro series  (~5 min)"
python -m data.ingest.fred

# ----------------------------------------------------------------------------
# STEP 3 — SEC EDGAR filings index
# ----------------------------------------------------------------------------
log "STEP 3/8  ·  SEC EDGAR filings index  (~20 min)"
python -m data.ingest.sec_edgar

# ----------------------------------------------------------------------------
# STEP 4 — Google Trends
# ----------------------------------------------------------------------------
log "STEP 4/8  ·  Google Trends search interest  (~10 min, rate-limited)"
python -m data.ingest.trends

# ----------------------------------------------------------------------------
# STEP 5 — ABS trustee reports + auto-ABS historical
# ----------------------------------------------------------------------------
log "STEP 5/8  ·  ABS trustee reports + auto-ABS historical"
python -m data.ingest.abs_parser
python -m data.ingest.auto_abs_historical || log "INFO: auto_abs_historical skipped (optional)"

# ----------------------------------------------------------------------------
# STEP 6 — Apple App Store reviews (re-scrape; ToS-restricted)
# ----------------------------------------------------------------------------
log "STEP 6/8  ·  Apple App Store reviews  (~30 min, rate-limited)"
log "         NOTE: redistributing scraped App Store data violates Apple ToS;"
log "               this step regenerates it locally for academic verification."
python -m data.ingest.app_store_rss

# ----------------------------------------------------------------------------
# STEP 7 — Reddit + Bluesky social
# ----------------------------------------------------------------------------
log "STEP 7/8  ·  Reddit + Bluesky social posts  (~1 hr)"
log "         NOTE: requires Reddit API credentials in bnpl-pod/.env."
log "               redistributing scraped output violates Reddit ToS;"
log "               this step regenerates it locally for academic verification."
python scripts/ingest_reddit_bluesky.py

# ----------------------------------------------------------------------------
# STEP 8 — Yahoo macro snapshot (firm-vitality + market context)
# ----------------------------------------------------------------------------
log "STEP 8/8  ·  Yahoo macro snapshot  (~5 min)"
python -m data.ingest.yahoo_macro || log "INFO: yahoo_macro skipped (optional)"

# ----------------------------------------------------------------------------
# Done
# ----------------------------------------------------------------------------
log "==================================================================="
log "  Warehouse rebuild COMPLETE"
log "  Path:  $POD_DIR/data/warehouse.duckdb"
log "  Next:  python -m signals.bsi    (compute BSI daily)"
log "         python -m signals.gates  (compute gate states)"
log "==================================================================="

# Print warehouse stats
python -c "
import duckdb
con = duckdb.connect('data/warehouse.duckdb', read_only=True)
print()
print('Warehouse summary:')
for tbl, in con.execute('SHOW TABLES').fetchall():
    n = con.execute(f'SELECT COUNT(*) FROM \"{tbl}\"').fetchone()[0]
    print(f'  {tbl:<35s} {n:>10,d} rows')
"
