# Google Trends — manual CSV exports

pytrends hits Google's 429 rate-limit floor within a handful of requests, which
is fine for smoke-testing but unacceptable for a paper that claims real data.
So this folder holds **official Google Trends CSVs that you exported by hand**
from `https://trends.google.com/`. `data/ingest/trends_manual.py` walks this
folder on ingest, parses every `.csv`, and upserts into the `google_trends`
DuckDB table — same schema as the pytrends path.

## How to export (≤ 15 minutes)

1. Open `https://trends.google.com/trends/explore`.
2. Add up to **5 keywords** via the **+ Compare** button (Trends' hard cap).
3. Set **region = United States**, **time range = 2018-01-01 to present**
   (covers the full Subprime-2.0 micro-leverage epoch), **category = All**,
   **type = Web Search**.
4. Click the ⬇ **download icon** on the *Interest over time* card.
5. Drop the resulting `multiTimeline.csv` into this folder. Rename it to
   something human-readable — the parser ignores filenames, it reads the
   keyword from inside the CSV header. Suggested naming:
   `<bucket>__<slug>.csv`, e.g. `direct_admission__panel_1.csv`.

## Current keyword panel

The 5 "Direct Admission" / distress terms (see
`data/ingest/trends.py::BUCKET_QUERIES`) that we're currently tracking:

| Keyword                                         | Bucket            | Signal                                    |
|-------------------------------------------------|-------------------|-------------------------------------------|
| `affirm late fee`                               | friction          | Mid-funnel stress                         |
| `cant pay klarna`                               | exit              | Acute distress                            |
| `klarna collections`                            | exit              | Collections escalation                    |
| `bnpl lawsuit`                                  | exit              | Legal-exit inquiry                        |
| `afterpay declined`                             | friction          | Credit-box tightening proxy               |
| `i am unable to pay for affirm`                 | direct_admission  | First-person liquidity wall               |
| `affirm hardship program`                       | direct_admission  | Self-identified distress, hardship path   |
| `stop affirm automatic payments`                | direct_admission  | Active cash-flow defence                  |
| `affirm debt collection help`                   | direct_admission  | Post-default help-seeking                 |
| `how to delete affirm account with balance`     | direct_admission  | Uninstall / abandonment proxy             |

10 keywords → **2 CSVs** (5 per compare query). Recommended grouping:

- **`distress_panel_1.csv`**: `affirm late fee`, `cant pay klarna`,
  `klarna collections`, `bnpl lawsuit`, `afterpay declined`
- **`direct_admission_panel.csv`**: `i am unable to pay for affirm`,
  `affirm hardship program`, `stop affirm automatic payments`,
  `affirm debt collection help`, `how to delete affirm account with balance`

## Parser contract

- Skips the first 1–3 metadata lines (`Category: All categories`, blank lines).
- Header row must start with one of `Time`, `Day`, `Week`, `Month` and contain
  at least one `<keyword>: (<region>)` column.
- Numeric cells that are literally `<1` (Trends' "less than 1 but > 0"
  placeholder) are coerced to `0.5`.
- Keyword is normalised to lowercase and stripped — so `Affirm Late Fee` and
  `affirm late fee` upsert to the same row.
- Re-running the ingest is idempotent (PK `(keyword, observed_at)` with
  `INSERT OR REPLACE`). Export a fresher CSV → re-run → warehouse updates.

## When NOT to use this path

- Live intraday monitoring — manual export is a batch/weekly cadence.
- Keyword discovery — if you're still iterating on what to track, use pytrends
  (offline or with a proxy) in a notebook first, then finalise here.
