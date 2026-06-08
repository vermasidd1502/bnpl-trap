# Marle-G — Indian Stock-Market Tracker

A live, volume-first research and execution pod for the Indian equity market
(NSE / BSE). Marle-G fuses a **real broker integration** (Groww trading API) with
a **volume-primary signal stack** — an up/down-volume (U/D) engine, Ichimoku, an
interactive Fibonacci charting tool, a full-universe sector "Volume Pod", a gated
O'Neil CAN-SLIM model with backtests, a dynamic ATR trailing-stop engine, and a
verified anti-Knight-Capital execution path. The pod is the **research/signals
brain**; Groww Cloud is the **execution hands**.

Sibling project to `bnpl-pod/`; built to the same engineering conventions.

## What it does

- **Live Groww trading-API integration** — authentication, real-time quotes,
  holdings, positions, orders, and margin, served behind a thin Flask API. Live
  quotes fall back to delayed `yfinance` data when the broker session is
  unavailable, so the research surface never goes dark.
- **U/D (up/down-volume) engine** — the core signal. A 20-day up-volume /
  down-volume ratio with a golden-ratio level ladder (Distribution → Balance →
  Accumulation → Ceiling) crossed with U/D direction (rising / falling) to place
  every name in one of eight behavioural quadrants, plus a "bulls vs bears in
  control" read. OBV, A/D (Chaikin), CMF, MFI, RVOL, and VSA (climax /
  absorption / no-demand / no-supply) round out the volume picture.
- **Ichimoku Kinko Hyo** — cloud position (above / in / below) and the
  Tenkan/Kijun cross, surfaced as confirm/conflict alongside RSI, MACD, the
  50/200-SMA regime, Bollinger %B, VWAP, and 52-week position.
- **Interactive Fibonacci HH/LL chart tool** — a TradingView lightweight-charts
  candlestick view with click-to-anchor Fibonacci retracements drawn off the
  selected higher-high / lower-low swing, used to locate the 0.618 confluence
  the gated model trades around.
- **Sector "Volume Pod"** — the full NSE universe, grouped by the NSE 22-sector
  taxonomy. Each sector gets a momentum verdict, a bulls/bears consensus, and the
  leading/lagging constituents, so leadership rotation is visible before it shows
  up in price.
- **Gated O'Neil CAN-SLIM model (backtested)** — the empirical centrepiece.
  Single volume factors had **no standalone edge**. The *gated confluence* —
  a name in a **leading sector**, with **U/D above its moving average and
  rising**, trading **above its 0.618 Fibonacci** level — beats buy-and-hold on
  a 10-day swing horizon: **~7.7% vs 4.6% CAGR, net of costs**. The edge lives
  in the confluence gate, not in any one indicator.
- **Dynamic ATR trailing-stop engine** — verdict-aware 2:1 R:R targets and an
  ATR-based trailing stop that ratchets with price.
- **Anti-Knight-Capital execution logic** — a verified safety layer between
  signals and order placement (**14/14 safety tests passing**) so a runaway
  loop, a stale price, or a duplicated order cannot reach the broker. Live order
  placement is off by default and gated behind an explicit environment flag.
- **Valuation-reversion + VWAP analysis** — Graham number, a 5-year FCF DCF,
  Piotroski F-score, a 0–100 quality score, and analyst-target reconciliation,
  fused with the volume read into a value × timing quadrant.
- **Groww Cloud auto-trailing-SL script** — the execution arm: a standalone
  script that runs on Groww Cloud, reads positions, and maintains trailing
  stop-losses without the research pod in the loop.

## Architecture

```
        +----------------------------------------------------+
        |  RESEARCH / SIGNALS BRAIN  (this repo, local)      |
        |  marleg_server.py  (Flask, :8777)                  |
        |                                                    |
        |   U/D engine · Ichimoku · Fib chart tool           |
        |   Sector Volume Pod · gated CAN-SLIM + backtests   |
        |   ATR trailing-stop · valuation/VWAP · news        |
        |          |                    ^                    |
        |   groww_client.py             | yfinance / mfapi   |
        |   (auth, quotes, holdings,    | (fallback data,    |
        |    orders, margin)            |  funds, macro)     |
        +----------|-----------------------------------------+
                   |  read-only account + quote calls
                   v
        +----------------------------------------------------+
        |  EXECUTION HANDS  (Groww Cloud)                    |
        |  cloud/groww_cloud_trailing_sl.py                  |
        |  maintains trailing SLs on live positions          |
        +----------------------------------------------------+
```

The split mirrors a desk that separates the analyst's screen from the order
gateway. The pod *decides*; Groww Cloud *acts*. Order placement crosses a
verified safety gate (`marleg_exec_logic.py`) and is disabled unless
`MARLEG_ALLOW_LIVE_ORDERS=1` is set.

## Key API endpoints

All served by `marleg_server.py` on `http://127.0.0.1:8777`.

| Endpoint | Purpose |
|---|---|
| `GET /api/health` | Liveness probe |
| `GET /api/quote?symbols=RELIANCE,HDFCBANK` | Batch LTP + change% (Groww, yfinance fallback) |
| `GET /api/equity/<ticker>` | Full volume-primary analysis (U/D, OBV/AD/CMF/MFI, Ichimoku, VSA, ATR target/stop) |
| `GET /api/volume_position/<ticker>` | U/D quadrant — where the name stands (8-quadrant ladder) |
| `GET /api/candles/<ticker>?period=2y` | OHLCV candles for the Fibonacci chart tool |
| `GET /api/volume_pod` | Sector Volume Pod (full NSE universe, 22-sector taxonomy) |
| `GET /api/gated` | Gated CAN-SLIM screen output (current picks) |
| `POST /api/volume_pod/add` | Add / re-classify a symbol in the Volume Pod |
| `GET /api/analyze/<ticker>` | Universal fusion: technical × fundamental × macro × catalyst |
| `GET /api/fundamentals/<ticker>` | Graham / DCF / Piotroski / quality score / analyst target |
| `GET /api/stock_score/<ticker>` | Combined volume + technical + news z-score |
| `GET /api/macro` | NIFTY / India VIX / USDINR / sector heat / regime |
| `GET /api/outlook` | Narrative next-day market outlook |
| `GET /api/mf/<scheme_code>` | Mutual-fund NAV history + CAGR |
| `GET /api/options/<underlying>` | NSE option chain (best-effort) |
| `GET /api/news?q=...` | Google-News RSS headlines + lexicon sentiment |
| `GET /api/symbols?q=...` | Symbol-master autocomplete |
| `GET /api/holdings`, `/api/positions`, `/api/orders`, `/api/margin` | Groww account (real-time) |
| `POST /api/order` | Place an order (gated; off unless `MARLEG_ALLOW_LIVE_ORDERS=1`) |
| `GET /api/groww/health` | Broker session status |

## How to run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. (optional) Groww credentials for live quotes / account / orders
#    Place secrets under groww-secrets/ (gitignored). Without them the pod
#    still runs fully on yfinance/mfapi data.

# 3. Build the sector Volume Pod and gated screen caches (first run)
python marleg_volume_scan.py     # writes marleg_volume_cache.json
python marleg_gated_scan.py      # writes marleg_gated_cache.json

# 4. Start the pod
python marleg_server.py          # -> http://127.0.0.1:8777
```

Open `http://127.0.0.1:8777/` for the pod dashboard. The `.json` caches live at
the repo root; the HTML/JS front-end is served from `web/`.

## Repo layout

```
indian-stock-market-tracker/
├── marleg_server.py            Flask backend (the brain) + all /api/* endpoints
├── groww_client.py             Groww trading-API client (auth/quotes/holdings/orders)
├── marleg_volume_scan.py       Sector Volume Pod scanner (full NSE universe)
├── marleg_gated_scan.py        Gated CAN-SLIM screen
├── marleg_backtest*.py         Backtests (single-factor + gated confluence)
├── marleg_exec_logic.py        Anti-Knight-Capital execution safety (14/14 tests)
├── marleg_exec_backtest.py     Execution-path backtest
├── marleg_stop_engine.py       Dynamic ATR trailing-stop engine
├── marleg_sector_map.py        NSE 22-sector taxonomy mapping
├── marleg_*.py                 Scan / swing / stops / merge utilities
├── *.json                      Data caches (symbols, sectors, volume, gated, stops)
├── web/                        Front-end (served by the backend)
│   ├── marle_g_*.html          Dashboard / pod / volume / chart / CAN-SLIM / etc.
│   ├── marleg_stockmodal.js    Stock detail modal
│   └── lightweight-charts.js   TradingView charting library (vendored)
├── cloud/
│   └── groww_cloud_trailing_sl.py   Execution arm (runs on Groww Cloud)
├── docs/
│   ├── marle_g_sectormap.html  Sector-map visual
│   └── marle_g_sectormap.png   Sector-map render
├── requirements.txt
└── .gitignore
```

## Honest caveats

- **Modest edge, small sample.** The gated CAN-SLIM advantage (~7.7% vs 4.6%
  CAGR net of costs) is real but thin, on a limited NSE sample over a 10-day
  swing horizon. Treat it as a confluence *filter*, not a money printer; it has
  not been validated across a full market cycle or out-of-sample regime.
- **US-IP order placement is constrained.** From a US IP, Groww order placement
  is not reliable directly — live orders need **Groww Cloud** (the
  `cloud/` script) or a **static India IP**. NSE option-chain pulls similarly
  require a residential India IP (datacenter/VPN IPs are blocked by Akamai). The
  research surface works from anywhere on yfinance/mfapi data.
- **News + earnings are live-only / not backtestable.** The news-sentiment and
  catalyst signals read live Google-News RSS and live fundamentals; there is no
  historical archive, so those components cannot be backtested and are
  context-only, never a standalone trade trigger.
- **Sentiment is lexicon-based.** News sentiment is a simple positive/negative
  word count, not a trained model — directional, not precise.
- **Advisory by default.** Live order placement is off unless explicitly enabled
  with `MARLEG_ALLOW_LIVE_ORDERS=1`, and every order still crosses the
  anti-Knight-Capital safety gate first.
```
