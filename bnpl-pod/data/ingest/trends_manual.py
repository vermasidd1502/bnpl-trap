"""
Google Trends ingest — manual CSV route.

Rationale
---------
`data.ingest.trends` uses `pytrends`, an unofficial scraper that hits Google's
429 rate-limit within a handful of requests and degrades to empty frames.
Fighting that is a losing battle on a research deadline, so this module reads
CSVs that the user exported by hand from `https://trends.google.com/` (official
Download ⬇ button) — see `data/manual_exports/google_trends/README.md` for the
15-minute export workflow.

The warehouse schema is unchanged: rows land in `google_trends(keyword,
observed_at, interest, issued_at)` exactly like the pytrends path. Upserts use
`INSERT OR REPLACE` so re-exporting a fresh CSV updates the series in place.

Run with:
    python -m data.ingest.trends_manual
    python -m data.ingest.trends_manual --dir /some/other/folder
    python -m data.ingest.trends_manual --dry-run
"""
from __future__ import annotations

import argparse
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from data.settings import settings

log = logging.getLogger(__name__)

DEFAULT_DIR = settings.root / "data" / "manual_exports" / "google_trends"

# Google Trends' first column is the time unit — varies by timeframe granularity.
_TIME_COLS = {"time", "day", "week", "month"}

# Header regex: '<keyword>: (<region>)'  ->  captures keyword.
_HEADER_KEYWORD_RE = re.compile(r"^(.*?):\s*\([^)]+\)\s*$")


@dataclass
class ParsedTrendsCsv:
    source: Path
    keywords: list[str]
    rows: pd.DataFrame   # columns: keyword, observed_at (date), interest (float)

    def __repr__(self) -> str:
        kws = ", ".join(self.keywords)
        return f"ParsedTrendsCsv({self.source.name}, kws=[{kws}], rows={len(self.rows)})"


def _find_header_row(lines: list[str]) -> int:
    """Return the 0-indexed line number of the column header row.

    Google Trends' export prepends 1–3 metadata lines ("Category: All
    categories", sometimes blank). We locate the first line whose first CSV
    token is a known time unit.
    """
    for idx, line in enumerate(lines):
        first_tok = line.split(",", 1)[0].strip().strip('"').lower()
        if first_tok in _TIME_COLS:
            return idx
    raise ValueError(
        "Could not find header row (expected first column in {Time, Day, Week, Month})"
    )


def _extract_keyword(col: str) -> str:
    """'Affirm Late Fee: (United States)' -> 'affirm late fee'."""
    m = _HEADER_KEYWORD_RE.match(col.strip())
    raw = m.group(1) if m else col
    return raw.strip().lower()


def _coerce_interest(val: object) -> float:
    """Google encodes 'between 0 and 1' as literal '<1'; coerce to 0.5."""
    if pd.isna(val):
        return 0.0
    if isinstance(val, str):
        v = val.strip()
        if v == "" or v == "-":
            return 0.0
        if v.startswith("<"):
            return 0.5
        try:
            return float(v)
        except ValueError:
            return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def parse_csv(path: Path) -> ParsedTrendsCsv:
    """Parse one Google Trends CSV (single or multi-keyword compare)."""
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    lines = text.splitlines()
    if not lines:
        raise ValueError(f"{path} is empty")

    hdr_idx = _find_header_row(lines)
    # pandas.read_csv handles quoted fields, varying delimiters, etc. — lean on it.
    df = pd.read_csv(path, skiprows=hdr_idx, encoding="utf-8-sig")
    if df.empty or df.shape[1] < 2:
        raise ValueError(f"{path}: no data rows after header at line {hdr_idx}")

    time_col = df.columns[0]
    keyword_cols = [c for c in df.columns[1:] if c and not c.startswith("Unnamed")]
    if not keyword_cols:
        raise ValueError(f"{path}: no keyword columns detected")

    keywords = [_extract_keyword(c) for c in keyword_cols]

    # Parse the time column — pandas infers weekly / daily / monthly just fine.
    df["_observed_at"] = pd.to_datetime(df[time_col], errors="coerce").dt.date
    df = df.dropna(subset=["_observed_at"])

    melted_parts = []
    for raw_col, kw in zip(keyword_cols, keywords, strict=True):
        s = df[raw_col].map(_coerce_interest)
        part = pd.DataFrame({
            "keyword": kw,
            "observed_at": df["_observed_at"],
            "interest": s.astype(float),
        })
        melted_parts.append(part)

    out = pd.concat(melted_parts, ignore_index=True)
    out = out.drop_duplicates(subset=["keyword", "observed_at"], keep="last")
    return ParsedTrendsCsv(source=path, keywords=keywords, rows=out)


def _upsert(con: duckdb.DuckDBPyConnection, rows: pd.DataFrame) -> int:
    if rows.empty:
        return 0
    # Register as temp relation and run a single INSERT OR REPLACE — ~100× faster
    # than row-by-row executemany for the 400-row weekly-over-5-years case.
    con.register("_trends_staging", rows)
    con.execute(
        """
        INSERT OR REPLACE INTO google_trends (keyword, observed_at, interest)
        SELECT keyword, observed_at, interest FROM _trends_staging
        """
    )
    con.unregister("_trends_staging")
    return len(rows)


def iter_csvs(directory: Path) -> Iterable[Path]:
    return sorted(directory.glob("*.csv"))


def ingest_directory(directory: Path = DEFAULT_DIR, *, dry_run: bool = False) -> dict[str, int]:
    """Parse every .csv in `directory` and upsert into `google_trends`.

    Returns a dict of {keyword: rows_written}. `-1` indicates a parse failure
    for that file (rare — usually a malformed CSV or surprise locale).
    """
    if not directory.exists():
        log.warning("manual-trends dir %s does not exist; nothing to ingest", directory)
        return {}

    paths = list(iter_csvs(directory))
    if not paths:
        log.warning("no CSVs found in %s", directory)
        return {}

    parsed: list[ParsedTrendsCsv] = []
    for p in paths:
        try:
            parsed.append(parse_csv(p))
        except Exception as e:   # noqa: BLE001 — catch-all logged + surfaced
            log.error("trends-manual | %s | PARSE FAIL: %s", p.name, e)

    if not parsed:
        return {}

    total = pd.concat([p.rows for p in parsed], ignore_index=True)
    total = total.drop_duplicates(subset=["keyword", "observed_at"], keep="last")

    if dry_run:
        log.info("trends-manual | DRY RUN | %d unique (kw, date) rows staged from %d CSV(s)",
                 len(total), len(parsed))
        return {kw: int((total["keyword"] == kw).sum()) for kw in sorted(total["keyword"].unique())}

    con = duckdb.connect(str(settings.duckdb_path))
    try:
        n = _upsert(con, total)
    finally:
        con.close()
    log.info("trends-manual | wrote %d rows from %d CSV(s) into %s",
             n, len(parsed), settings.duckdb_path.name)
    return {kw: int((total["keyword"] == kw).sum()) for kw in sorted(total["keyword"].unique())}


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dir", type=Path, default=DEFAULT_DIR,
                   help=f"Folder with Google Trends CSV exports (default: {DEFAULT_DIR})")
    p.add_argument("--dry-run", action="store_true",
                   help="Parse and print summary without touching DuckDB")
    return p


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    args = _build_cli().parse_args()
    summary = ingest_directory(args.dir, dry_run=args.dry_run)
    if not summary:
        print("no data ingested — check the CSVs in", args.dir)
    else:
        print(f"\nGoogle Trends manual ingest — {len(summary)} keyword(s):")
        for kw, n in summary.items():
            print(f"  {kw:55s} {n:>5d} rows")
