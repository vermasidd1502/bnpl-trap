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
        weights_hash VARCHAR,             -- hash of active weights.yaml, for reproducibility
        issued_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
