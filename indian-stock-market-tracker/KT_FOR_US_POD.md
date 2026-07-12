# Knowledge Transfer — Marle-G (India pod) → for porting to the US pod (Apollo)

> **Scope & boundary.** This documents the **Marle-G India pod** (`indian-stock-market-tracker/`) so its patterns and findings can be **ported** to the separate US pod (Apollo). Apollo is a *separate project/repo* — this is reference-only; do not run Apollo code from here. Everything below is India-specific unless marked **[market-agnostic]** or **[re-test on US]**.
>
> _Author: Claude (Opus 4.8) session, 2026-07-09. Read-only on the live broker throughout._

---

## 1. What the pod is
A single-user, locally-run **India equity + F&O options research & paper-trading pod**. Python/Flask backend, ~250 engines, ~85 dark-themed HTML pages opened in a real Edge app-window. **READ-ONLY** on the live broker (Groww) — it never places/modifies/cancels orders; it generates signals and *you* place them.

## 2. Architecture [market-agnostic pattern]
- **Server:** `marleg_server.py` — threaded Flask on **:8777**, no auto-reload (restart to load `.py` changes). ~250 `/api/*` endpoints, one per engine. Static HTML served from `web/`.
- **Engines:** one module per concern (`marleg_<thing>.py`), each exposing a pure function the endpoint wraps. Pattern: engine returns a JSON-able dict `{ok, ...}`; endpoint wraps it in a cache.
- **Caching (critical for a rate-limited broker):**
  - `cached(key, fn, ttl, keep=…)` — in-memory TTL cache (used for live broker calls, 10–30s).
  - `marleg_snapcache.py` `swr(key, fn, fresh_sec)` — **disk-persisted stale-while-revalidate** under `marleg_snapshots/*.json`. Serves last snapshot instantly, refreshes in a background thread, **survives restarts**. Use for anything expensive (yfinance macro, multi-ticker pulls).
- **Nav:** `web/marle_g_nav.js` — a `GROUPS` array of 9 hubs; injected into every page. Add a page = add one `[href, label]` line to the right hub.
- **Front-end:** vanilla JS + one vendored `lightweight-charts.js` (loaded via `MGCandles.ensureLib`). Dark theme via CSS variables. Charts **stream** the forming bar (`series.update`), never reload the page.

## 3. Data sources
- **Live broker (Groww):** READ-ONLY. Client exposes `ltp/quote/ohlc/candles/chain/positions/orders/holdings/margin`. Domestic NSE/BSE only (no MCX, no GIFT/NSE-IX). Approval **lapses daily → TOTP self-renew**. Rate-limits under concurrent load (429) → hence the caching. **Secrets live in `.env` (gitignored), read via `os.getenv`. Never hardcode or print them.**
- **Macro (yfinance):** reliable for `^NSEI, ^GSPC, ES=F, NQ=F, CL=F, GC=F, ^VIX, DX-Y.NYB, ^STI, ^KS11, ^N225, ^HSI, ^TWII`. Asian index *levels* can glitch — guard with `abs(chg)<20`. **Balance sheets** (`.balance_sheet`) work for large caps but **rate-limit at scale → add 0.35s delay + retry**.
- **Warehouses (DuckDB, gitignored — proprietary, forks start empty):**
  - `marleg_warehouse.duckdb` table `bhav` — equity OHLCV + `deliv_pct`, 14y, survivorship-free (2012→present).
  - `marleg_fo_warehouse.duckdb` table `fo` — F&O bhavcopy with OI. Columns `date/symbol/instrument{IDF,IDO,STF,STO}/expiry/strike/opt_type/OHLC/settle/oi/volume`. **Coverage ends ~2026-06-29** (no live-day OI history → cannot reconstruct intraday walls for a past day).
- **MF:** AMFI scrapers + STCG/LTCG tax engine.

## 4. This session's deliverables (2026-07)
- **`marleg_asialead.py` → `/api/asialead` → `web/marle_g_asialead.html`** (macro hub 🌏). Pre-open Asian-lead tracker: STI/KOSPI/Taiwan/Nikkei/HangSeng → weighted consensus + a **NIFTY-follows-down probability** for the 9:15 open + macro strip. Fires the probability only when a KEY tell (STI/KOSPI/Taiwan) is down >1%.
- **`marleg_indexcompare.py` → `/api/indexcompare?window=` → `web/marle_g_indexcompare.html`** (macro hub 🔭). Interactive multi-index overlay, rebased to 100, timeframe + per-index toggles, same-day/next-day follow-correlation + co-direction %.
- **`marleg_maxpain.py` FIX** — `board()` now rolls **past a DTE-0 dying contract** to the next live weekly, `_healthy()` rejects degenerate/thin chains (call_wall==put_wall, <5 OI strikes, PCR outside 0.1–12), adds a `thin` flag. Fixed SENSEX pinning to an expiring-today contract with garbage OI.

## 5. Validated findings — port the *method*, RE-TEST the *numbers* on US data
These were validated on **India** data. The mechanisms are often general; the coefficients are **not** transferable — **[re-test on US]** every one.
- **Asian markets lead NIFTY same-day** (STI 82% / KOSPI 76% / Taiwan 76% follow-down when down >1%) — but **next-day corr is ~0/negative** → same-day co-move, not a lead. **[US analog:** overnight futures + European open lead the US pre-market — re-derive.**]**
- **Crude does NOT lead NIFTY** — <1% of daily move, even a same-day oracle loses. Inverse sign only post-2022 (import-cost regime). **Don't build a crude-timing strategy.** **[market-agnostic caution: single-macro-factor timing rarely survives costs.]**
- **Option wall asymmetry** [market-agnostic mechanism]: call wall reverses down ~90% (the "fade"); put wall breaks ~65% (false floor, liquidity magnet); max-pain weak. Dealer-gamma-hedging story.
- **Factors (India):** LOW-VOL dominant (t≈5), near-52w-high momentum + liquidity real, all **LONG**; shorting/puts structurally lose (India drifts up). **High goodwill/intangible on the B/S underperforms** (Q4−Q1 ≈ −18.6%/yr, t−2.46) — a goodwill/acquisition red-flag screen, regime-flavored (2021-26 value comeback). **[US will differ — the 15y pre-2021 favored intangible-heavy.]**
- Shorting is an anti-edge in India; the US structure/borrow/regime differs → **re-test long/short symmetry on US.**

## 6. Run / restart / dev
```
# start (detached, clean — no background scans):
MARLEG_NO_BG=1  python marleg_server.py    # binds :8777
# restart after a .py change (PowerShell):
Get-NetTCPConnection -LocalPort 8777 -State Listen | %{ Stop-Process -Id $_.OwningProcess -Force }
$env:MARLEG_NO_BG='1'; Start-Process python marleg_server.py -WindowStyle Hidden
# open a page in real Edge:
Start-Process msedge "--app=http://127.0.0.1:8777/marle_g_dashboard.html"
```
- Restarts **orphan open Edge windows** → batch code changes into ONE restart and warn first.
- For render-debugging, the Claude preview can own :8777 (`marle-g` launch config) — snapshot/screenshot, then hand back to a detached process.

## 7. Constraints (carry ALL of these to the US pod)
1. **READ-ONLY on the live account.** Never place/modify/cancel real orders. Signals only; the human places them.
2. **Secrets in `.env` only** (gitignored). Never hardcode/print key/secret/token/bearer.
3. **No fabricated data.** If a feed doesn't exist (GIFT, foreign order-book, MCX), say so — don't invent numbers.
4. **Dual-clock** every time-sensitive reply (here IST + US-Central; for Apollo, ET + user-local).
5. **Validated → frontend:** surface validated findings in the UI; refuted ideas become *caveats*, not strategies.
6. **`.duckdb` warehouses are gitignored** (proprietary); a fresh clone starts with zero data and rebuilds via the bhavcopy refresh.

## 8. Porting map (India → US / Apollo)
| India pod | US pod analog |
|---|---|
| Groww (NSE/BSE, read-only, TOTP) | US broker API (read-only) — same "signals only" rule |
| bhavcopy DuckDB warehouses | US EOD vendor (e.g. exchange EOD / Sharadar) → same `bhav`/`fo` schema |
| Asian-lead pre-open tracker | Overnight futures + European-open lead into US pre-market |
| NIFTY/SENSEX option walls (maxpain/fade) | SPX/QQQ/SPY walls — **the engine is market-agnostic**, just point it at the US chain |
| India VIX | ^VIX |
| Factor findings | **re-run the same discovery harness on the US panel — do not copy coefficients** |

**One-line summary for whoever picks this up:** it's a read-only, cache-heavy Flask pod where every feature is `engine.py → /api/x → page.html`; the *plumbing and mechanisms* port cleanly, the *numbers* must be re-validated on US data.
