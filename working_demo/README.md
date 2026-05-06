# Apollo Hermes x BearWatch — WORKING DEMO

A self-contained, runnable Flask app that demos the full BearWatch -> Apollo Hermes pipeline live in a browser. Built for hedge-fund pitch (Prof. Tony).

## Launch

Double-click `start.bat`.

It will:
1. Install Flask + yfinance + pandas (first run only, quietly)
2. Launch the server at `http://127.0.0.1:5000/`
3. Auto-open your default browser to the dashboard

To stop: close the terminal window or hit `Ctrl+C`.

## What to demo (live, in front of an audience)

1. **Boot.** Show the empty dashboard — $100,000 cash, 0 positions, NORMAL drawdown mode.
2. **Fire CVNA bear.** Click the big `Fire CVNA bear` button. Within ~1 second:
   - The BearWatch event payload appears in Step 1
   - Apollo runs all 7 risk checks in real Python (Step 2)
   - A live yfinance quote is fetched and displayed
   - A `VERDICT: APPROVED` (or scaled-down/blocked) is rendered with check-by-check detail
3. **Execute.** Click `Execute trade`. The trade is logged to SQLite with a foreign key back to the BearWatch event.
4. **Refresh the browser.** Everything persists — portfolio, alert history, journal.
5. **Fire UPST and AFRM.** Show how different conviction levels and BSI z-scores route through the same risk engine and produce different sizes.
6. **Show the integration page.** Click `BearWatch Bridge` in the nav. The 5-stage pipeline diagram + spec.

## What every button does

- **Fire CVNA / UPST / AFRM bear** -> POST to `/api/bearwatch/ingest` with a hardcoded BearWatch payload. Triggers real risk-check pipeline, returns verdict, persists event.
- **Execute trade** -> POST to `/api/journal/log`. Debits cash, logs trade with FK to bearwatch_event_id.
- **Reset demo state** -> POST to `/api/portfolio/reset`. Wipes journal, alerts, verdicts; resets cash to $100K and mode to NORMAL.

## Reset journal

Click `Reset demo state` in the top-right of the demo trigger panel.
Or delete `data/apollo.db` and relaunch.

## What's real vs faked

| Component | Real? |
|---|---|
| 7 pre-trade risk checks (sector cap, beta, R:R, drawdown, cash floor, position cap, correlation) | Real Python, deterministic |
| yfinance live quote | Real (with 60s cache + hardcoded fallback if rate-limited / offline) |
| SQLite persistence | Real (apollo.db, WAL mode, survives refresh) |
| BearWatch payloads | Realistic mocks (no live BearWatch wired) |
| Trade execution | Sim (debits cash; no real broker) |

## Files

```
working_demo/
  app.py                  Flask backend, all 8 routes, real risk engine
  start.bat               One-click launcher
  requirements.txt        Flask, yfinance, pandas
  README.md               This file
  templates/
    dashboard.html        Live dashboard (port of redesign_demo/index.html)
    integration.html      Pipeline spec page
  static/
    style.css             Verbatim from redesign_demo/style.css
    app.js                All fetch/render/handler logic
  data/
    apollo.db             SQLite DB (auto-created on first launch)
```

## Endpoints (for curl-curious)

- `GET  /`                          dashboard
- `GET  /integration`               integration spec page
- `POST /api/bearwatch/ingest`      ingest a BearWatch event, run risk, return verdict
- `POST /api/risk/check`            re-run risk against an arbitrary payload (no persist)
- `GET  /api/quote/<ticker>`        live yfinance quote (60s cache)
- `GET  /api/portfolio`             current cash, positions, beta, drawdown mode
- `GET  /api/bears`                 last 25 ingested BearWatch events + their verdicts
- `GET  /api/journal`               last 25 executed trades
- `POST /api/journal/log`           log an executed trade (debits cash)
- `POST /api/portfolio/reset`       wipe everything, reset to $100K cash
