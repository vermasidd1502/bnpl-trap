"""
Refresh bsi_daily by extending it forward from the freshly-ingested pillar tables.

Reads cfpb_complaints / app_store_reviews / reddit_posts / fred_series from the
warehouse, computes daily pillar z-scores on a 90-day rolling window, and INSERTs
new rows into bsi_daily for any date beyond the current max(observed_at).

Idempotent: re-running on the same data simply re-derives the same rows.
Run after the ingest chain; the daily driver calls this automatically.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

WAREHOUSE = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"


def rolling_z(s: pd.Series, window: int = 90) -> pd.Series:
    mu = s.rolling(window, min_periods=10).mean()
    sd = s.rolling(window, min_periods=10).std().clip(lower=1e-6)
    return (s - mu) / sd


def daily_count(con, table: str, date_col: str, alias: str) -> pd.DataFrame:
    df = con.execute(
        f"SELECT CAST({date_col} AS DATE) AS d, COUNT(*) AS n "
        f"FROM {table} GROUP BY d ORDER BY d"
    ).fetchdf()
    return df.rename(columns={"n": alias}).set_index("d")


def main() -> int:
    if not WAREHOUSE.exists():
        print(f"ERROR: warehouse not found at {WAREHOUSE}")
        return 1

    con = duckdb.connect(str(WAREHOUSE), read_only=False)
    try:
        last_bsi = con.execute(
            "SELECT MAX(observed_at) FROM bsi_daily WHERE bsi IS NOT NULL"
        ).fetchone()[0]
        print(f"current bsi_daily max: {last_bsi}")

        ceilings = {
            "cfpb": con.execute("SELECT MAX(received_at)::DATE FROM cfpb_complaints").fetchone()[0],
            "app":  con.execute("SELECT MAX(created_at)::DATE FROM app_store_reviews").fetchone()[0],
            "red":  con.execute("SELECT MAX(created_at)::DATE FROM reddit_posts").fetchone()[0],
            "fred": con.execute("SELECT MAX(observed_at) FROM fred_series").fetchone()[0],
        }
        print(f"data ceilings: {ceilings}")

        valid = [d for d in ceilings.values() if d]
        if not valid:
            print("no pillar data available; nothing to do.")
            return 0
        target_max = min(valid)
        start = (last_bsi + timedelta(days=1)) if last_bsi else date(2018, 1, 1)
        print(f"extend window: {start} -> {target_max}")
        if start > target_max:
            print("bsi_daily already current; nothing to extend.")
            return 0

        cfpb_d = daily_count(con, "cfpb_complaints", "received_at", "cfpb_n")
        app_d = daily_count(con, "app_store_reviews", "created_at", "app_n")
        red_d = daily_count(con, "reddit_posts", "created_at", "red_n")
        move_d = con.execute(
            "SELECT observed_at AS d, value AS move FROM fred_series "
            "WHERE series_id='MOVE' ORDER BY d"
        ).fetchdf().set_index("d")

        full_idx = pd.date_range("2018-01-01", target_max, freq="D")
        panel = pd.DataFrame(index=full_idx)
        for src in (cfpb_d, app_d, red_d, move_d):
            src.index = pd.to_datetime(src.index)
            panel = panel.join(src, how="left")
        panel = panel.ffill().fillna(0)

        panel["c_cfpb"] = rolling_z(panel["cfpb_n"])
        panel["c_appstore"] = rolling_z(panel["app_n"])
        panel["c_reddit"] = rolling_z(panel["red_n"])
        panel["c_move"] = rolling_z(panel["move"])
        panel["c_trends"] = 0.0
        panel["c_vitality"] = 0.0

        pillars = ["c_cfpb", "c_appstore", "c_reddit", "c_move"]
        panel["bsi"] = panel[pillars].mean(axis=1)
        mu = panel["bsi"].rolling(180, min_periods=20).mean()
        sd = panel["bsi"].rolling(180, min_periods=20).std().clip(lower=1e-6)
        panel["z_bsi"] = (panel["bsi"] - mu) / sd

        ext = panel.loc[pd.Timestamp(start):pd.Timestamp(target_max)].copy()
        ext = ext.reset_index().rename(columns={"index": "observed_at"})
        ext["observed_at"] = ext["observed_at"].dt.date
        cols = ["observed_at", "bsi", "z_bsi", "c_cfpb", "c_trends",
                "c_reddit", "c_appstore", "c_move", "c_vitality"]
        ext = ext[cols].dropna(subset=["bsi"])
        if ext.empty:
            print("no new rows after dropna; nothing to extend.")
            return 0

        con.execute("DELETE FROM bsi_daily WHERE observed_at >= ?", [start])
        con.register("ext", ext)
        con.execute(
            "INSERT INTO bsi_daily "
            "(observed_at, bsi, z_bsi, c_cfpb, c_trends, c_reddit, c_appstore, c_move, c_vitality) "
            "SELECT observed_at, bsi, z_bsi, c_cfpb, c_trends, c_reddit, c_appstore, c_move, c_vitality "
            "FROM ext"
        )
        mx, n = con.execute("SELECT MAX(observed_at), COUNT(*) FROM bsi_daily").fetchone()
        latest_z = con.execute(
            "SELECT z_bsi FROM bsi_daily ORDER BY observed_at DESC LIMIT 1"
        ).fetchone()[0]
        print(f"inserted {len(ext):,} rows | bsi_daily now max={mx}, count={n:,}, latest z_bsi={latest_z:.4f}")
        con.execute("CHECKPOINT")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
