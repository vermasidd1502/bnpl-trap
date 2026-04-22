"""
DuckDB schema — single source of truth for the warehouse.

Run with:  python -m data.schema

Design notes
------------
- DuckDB single-file at config.settings.DUCKDB_PATH. Zero-config; no daemon.
- Every ingestion module (data.ingest.*) writes ONLY to the tables defined here.
- Every signal / quant / backtest module reads from these tables — never from
  raw files — so the warehouse is the integration boundary.
- `issued_at` is the AS-OF timestamp the ROW was written; `observed_at` is the
  calendar timestamp of the UNDERLYING OBSERVATION. Two fields are required
  because several sources (AFRMMT trustee reports, FINRA short-interest) are
  published with multi-day lag. Leading-vs-lagging analysis depends on this
  distinction and is the crux of the thesis.
- All tables are idempotent: re-running ingestion does not duplicate rows
  (enforced by PRIMARY KEY on the composite natural key).
"""
from __future__ import annotations

import duckdb

from data.settings import settings

DDL: list[str] = [
    # --- 1. FRED macro series --------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fred_series (
        series_id   VARCHAR  NOT NULL,   -- e.g. 'MOVE', 'T10Y3M', 'DRCCLACBS', 'SOFR'
        observed_at DATE     NOT NULL,
        value       DOUBLE,
        issued_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (series_id, observed_at)
    );
    """,
    # --- 2. SEC EDGAR filings index -------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS sec_filings_index (
        accession_no VARCHAR  NOT NULL,   -- e.g. '0001140361-25-012345'
        cik          VARCHAR  NOT NULL,
        trust_name   VARCHAR,             -- 'AFFIRM ASSET SECURITIZATION TRUST 2024-B'
        form_type    VARCHAR  NOT NULL,   -- '10-D', 'ABS-15G', '8-K', etc.
        filed_at     TIMESTAMP NOT NULL,
        period_end   DATE,
        url          VARCHAR,
        issued_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (accession_no)
    );
    """,
    # --- 3. Parsed AFRMMT trustee report fields --------------------------------
    """
    CREATE TABLE IF NOT EXISTS abs_tranche_metrics (
        accession_no   VARCHAR NOT NULL,
        trust_name     VARCHAR NOT NULL,
        period_end     DATE    NOT NULL,
        roll_rate_60p  DOUBLE,             -- roll-to-60+DPD, percent
        excess_spread  DOUBLE,             -- percent annualized
        cnl            DOUBLE,             -- cumulative net loss, percent of original pool
        senior_enh     DOUBLE,             -- senior credit enhancement percent (optional)
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (accession_no)
    );
    """,
    # --- 4. CFPB complaints ---------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS cfpb_complaints (
        complaint_id   VARCHAR NOT NULL,
        received_at    DATE    NOT NULL,
        product        VARCHAR,
        sub_product    VARCHAR,
        issue          VARCHAR,
        company        VARCHAR,            -- 'AFFIRM, INC.' etc.
        narrative      TEXT,
        tags           VARCHAR,
        state          VARCHAR,
        -- FinBERT output filled by nlp.finbert_sentiment (v4.1 §3).
        finbert_neg    DOUBLE,
        finbert_neu    DOUBLE,
        finbert_pos    DOUBLE,
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (complaint_id)
    );
    """,
    # --- 5. Reddit posts ------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS reddit_posts (
        post_id        VARCHAR NOT NULL,
        subreddit      VARCHAR NOT NULL,
        created_at     TIMESTAMP NOT NULL,
        title          TEXT,
        body           TEXT,
        score          INTEGER,
        num_comments   INTEGER,
        url            VARCHAR,
        -- Author metadata for v4.1 bot-filter credibility (§3.1).
        author           VARCHAR,
        author_age_days  INTEGER,
        author_karma     INTEGER,
        credibility      DOUBLE,   -- in [0,1], computed by nlp.finbert_sentiment
        -- FinBERT output is filled by nlp.finbert_sentiment in Sprint B.
        finbert_neg    DOUBLE,
        finbert_neu    DOUBLE,
        finbert_pos    DOUBLE,
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (post_id)
    );
    """,
    # --- 6. Google Trends (pytrends) ------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS google_trends (
        keyword      VARCHAR NOT NULL,
        observed_at  DATE    NOT NULL,
        interest     DOUBLE,              -- 0..100 scale, region=US
        issued_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (keyword, observed_at)
    );
    """,
    # --- 7. Option chains (AFRM/SQ/PYPL etc.) ---------------------------------
    """
    CREATE TABLE IF NOT EXISTS options_chain (
        ticker       VARCHAR NOT NULL,
        observed_at  DATE    NOT NULL,
        expiry       DATE    NOT NULL,
        strike       DOUBLE  NOT NULL,
        option_type  VARCHAR NOT NULL,     -- 'C' or 'P'
        bid          DOUBLE,
        ask          DOUBLE,
        last_price   DOUBLE,
        volume       BIGINT,
        open_interest BIGINT,
        iv           DOUBLE,
        underlying_price DOUBLE,
        issued_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ticker, observed_at, expiry, strike, option_type)
    );
    """,
    # --- 8. Short interest + days-to-cover ------------------------------------
    """
    CREATE TABLE IF NOT EXISTS short_interest (
        ticker         VARCHAR NOT NULL,
        observed_at    DATE    NOT NULL,
        shares_short   BIGINT,
        free_float     BIGINT,
        utilization    DOUBLE,             -- shares_short / free_float
        avg_daily_vol  BIGINT,
        days_to_cover  DOUBLE,             -- shares_short / avg_daily_vol
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ticker, observed_at)
    );
    """,
    # --- 9. BSI daily series (produced by signals.bsi) ------------------------
    """
    CREATE TABLE IF NOT EXISTS bsi_daily (
        observed_at  DATE PRIMARY KEY,
        bsi          DOUBLE NOT NULL,
        z_bsi        DOUBLE,
        c_cfpb       DOUBLE,
        c_trends     DOUBLE,
        c_reddit     DOUBLE,
        c_appstore   DOUBLE,
        c_move       DOUBLE,
        c_vitality   DOUBLE,               -- firm-vitality component (v4.1 §6.1)
        freeze_flag  BOOLEAN,              -- any treated firm in freeze state today
        weights_hash VARCHAR,             -- hash of active weights.yaml, for reproducibility
        issued_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    # --- 9b. Firm vitality from Wayback (LinkedIn + X) ------------------------
    """
    CREATE TABLE IF NOT EXISTS firm_vitality (
        slug           VARCHAR NOT NULL,   -- 'affirm', 'klarna', ...
        platform       VARCHAR NOT NULL,   -- 'linkedin' | 'x'
        observed_at    DATE    NOT NULL,   -- calendar date of the snapshot
        snapshot_age_d INTEGER,            -- days between snapshot and today at ingest time
        headcount      BIGINT,             -- LinkedIn only (NULL for X)
        openings       BIGINT,             -- LinkedIn only
        followers      BIGINT,             -- X only (NULL for LinkedIn)
        tenure_slope   DOUBLE,             -- openings / headcount for LinkedIn
        freeze_flag    BOOLEAN,            -- ΔTenureSlope < -2σ with flat headcount
        stale_weight   DOUBLE,             -- exp(-max(0, age_d-30)/tau), tau=30
        wayback_url    VARCHAR,
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (slug, platform, observed_at)
    );
    """,
    # --- 9c. JT hazard decomposition (issuer × day) ---------------------------
    """
    CREATE TABLE IF NOT EXISTS jt_lambda (
        issuer         VARCHAR NOT NULL,     -- 'AFRM', 'SQ', 'PYPL', ...
        observed_at    DATE    NOT NULL,
        lambda_sys     DOUBLE,                -- systemic component (macro-CIR)
        lambda_unsys   DOUBLE,                -- idiosyncratic CIR driven by BSI/MOVE
        lambda_total   DOUBLE,                -- = min(lambda_sys + lambda_unsys, J_max)
        kappa          DOUBLE,
        theta          DOUBLE,
        sigma          DOUBLE,
        j_max          DOUBLE,                -- cap parameter (0.05 default)
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (issuer, observed_at)
    );
    """,
    # --- 9d. Heston calibration + SCP gate (G2) -------------------------------
    """
    CREATE TABLE IF NOT EXISTS scp_daily (
        ticker         VARCHAR NOT NULL,
        observed_at    DATE    NOT NULL,
        scp            DOUBLE,                -- vol premium over historical baseline
        z_scp          DOUBLE,                -- z-score of SCP (gate fires at 90th pct)
        kappa          DOUBLE,
        theta          DOUBLE,
        sigma          DOUBLE,
        rho            DOUBLE,
        v0             DOUBLE,
        calibration_rmse DOUBLE,
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ticker, observed_at)
    );
    """,
    # --- 9e. Squeeze Defense Layer (G-SDL veto) -------------------------------
    """
    CREATE TABLE IF NOT EXISTS squeeze_defense (
        ticker         VARCHAR NOT NULL,
        observed_at    DATE    NOT NULL,
        otm_call_pct   DOUBLE,                -- share of call OI above 110% of spot
        utilization    DOUBLE,                -- shares_short / free_float
        days_to_cover  DOUBLE,
        iv_skew_25d    DOUBLE,                -- IV(25D put) - IV(25D call)
        squeeze_score  DOUBLE,                -- composite in [0, 1]
        veto           BOOLEAN,
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ticker, observed_at)
    );
    """,
    # --- 10. Pod decisions (one row per pod tick) -----------------------------
    """
    CREATE TABLE IF NOT EXISTS pod_decisions (
        run_id             VARCHAR NOT NULL,
        as_of              TIMESTAMP NOT NULL,
        bsi                DOUBLE,
        move_ma30          DOUBLE,
        scp_by_ticker_json VARCHAR,
        gate_bsi           BOOLEAN,
        gate_scp           BOOLEAN,
        gate_move          BOOLEAN,
        gate_ccd2          BOOLEAN,
        squeeze_veto       BOOLEAN,
        compliance_ok      BOOLEAN,
        compliance_reasons VARCHAR,
        llm_advisory       VARCHAR,
        trade_signal_json  VARCHAR,
        PRIMARY KEY (run_id)
    );
    """,
    # --- 11. Portfolio weights (Sprint F Mean-CVaR optimizer output) -----------
    """
    CREATE TABLE IF NOT EXISTS portfolio_weights (
        run_id             VARCHAR NOT NULL,
        issuer             VARCHAR NOT NULL,
        weight             DOUBLE,          -- signed weight in [-0.25, 0]; TRS short = negative
        expected_return    DOUBLE,          -- mu_i = spread_tightening - SOFR_carry
        cvar_contribution  DOUBLE,          -- marginal CVaR contribution
        gross_leverage     DOUBLE,          -- book-level, replicated across legs for audit
        cvar_value         DOUBLE,          -- book-level CVaR at solve
        gamma              DOUBLE,          -- risk-aversion used
        solver_status      VARCHAR,
        issued_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (run_id, issuer)
    );
    """,
    # --- 11b. Regulatory catalyst calendar (Sprint H — kills the CCD II time-travel leak) ----
    # The 3-gate compliance rule "days_to_nearest_material_catalyst <= 180"
    # previously referenced a single hardcoded deadline (DEFAULT_CCD_II_DEADLINE
    # = 2026-11-20), which forced gate_ccd2 = False on every historical event
    # window (days_to_deadline = 1593/1547/1380/912 days at the 2022-2023
    # windows we backtest). That's a temporal leak: the gate cannot fire before
    # a future event even existed in the regulatory record. Replace with a
    # time-varying table of (jurisdiction, deadline_date, materiality) so the
    # as_of-aware query returns whatever catalyst was imminent at that point in
    # history. Seeded by data.ingest.regulatory_catalysts.
    """
    CREATE TABLE IF NOT EXISTS regulatory_catalysts (
        catalyst_id   VARCHAR NOT NULL,        -- stable human-readable key: 'cfpb_2022_market_report'
        jurisdiction  VARCHAR NOT NULL,        -- 'US-CFPB' | 'UK-FCA' | 'EU-CCDII' | ...
        deadline_date DATE    NOT NULL,        -- publication / effective date
        title         VARCHAR NOT NULL,
        materiality   DOUBLE  NOT NULL,        -- in [0,1]; >= 0.5 required to count for the 180d gate
        category      VARCHAR,                 -- 'report' | 'rule' | 'consultation' | 'transposition'
        notes         TEXT,
        issued_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (catalyst_id)
    );
    """,
    # --- 12. Macro-hedge sleeve (Fix #2 — static, outside the Mean-CVaR LP) ----
    # Rockafellar-Uryasev optimization sizes the TRS book. Credit-beta (or
    # rates) hedging is carried STATICALLY by a parallel sleeve so the LP's
    # risk budget is not contaminated by hedge leg choices. See
    # agents/schemas.py::MacroHedgeSpec and portfolio/book.py for the sizer.
    """
    CREATE TABLE IF NOT EXISTS portfolio_hedges (
        run_id       VARCHAR NOT NULL,
        instrument   VARCHAR NOT NULL,   -- 'HYG_SHORT' | 'ZT_FUT'
        sizing_rule  VARCHAR,            -- 'beta_credit' | 'dv01_neutral'
        notional     DOUBLE,             -- signed; negative for shorts
        hedge_ratio  DOUBLE,             -- β_credit or DV01 multiplier applied
        trs_gross    DOUBLE,             -- aggregate TRS abs-notional this sleeve hedges
        rationale    VARCHAR,
        issued_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (run_id, instrument)
    );
    """,
    # --- 18. App Store reviews (Apple iTunes RSS, free / no auth) ----------
    """
    CREATE TABLE IF NOT EXISTS app_store_reviews (
        review_id      VARCHAR NOT NULL,
        app_id         VARCHAR NOT NULL,      -- Apple's numeric track_id
        app_name       VARCHAR NOT NULL,      -- canonical slug: 'affirm', 'klarna', ...
        platform       VARCHAR NOT NULL,      -- 'ios' (future: 'android')
        author         VARCHAR,
        title          TEXT,
        body           TEXT,
        rating         INTEGER,               -- 1..5
        version        VARCHAR,
        created_at     TIMESTAMP NOT NULL,    -- review updated-at from feed
        -- FinBERT scores filled by nlp.finbert_sentiment downstream
        finbert_neg    DOUBLE,
        finbert_neu    DOUBLE,
        finbert_pos    DOUBLE,
        issued_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (review_id)
    );
    """,
]


def initialize(path: str | None = None) -> None:
    """Create all tables. Idempotent — safe to re-run."""
    db_path = path or str(settings.duckdb_path)
    con = duckdb.connect(db_path)
    try:
        for stmt in DDL:
            con.execute(stmt)
        # Smoke-check
        tables = con.execute("SELECT table_name FROM duckdb_tables() ORDER BY table_name").fetchall()
        print(f"[schema] initialized {len(tables)} tables at {db_path}")
        for (t,) in tables:
            print(f"  - {t}")
    finally:
        con.close()


if __name__ == "__main__":
    initialize()
