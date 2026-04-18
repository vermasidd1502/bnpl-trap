# BNPL Agentic Pod — Master Implementation Plan

**Purpose of this document.** This is a complete, self-contained handoff brief. Paste it into a fresh Claude Opus session (or any frontier model) and the model will have everything required to continue the project without access to the prior Sonnet conversation: the thesis, the decided architecture, the locked technology stack, the repo layout, the sprint-by-sprint implementation sequence, the rigor requirements, the known traps, and the verification criteria.

**Author context.** Student, class 580, Spring 2026, solo, local machine + NVIDIA NIM credits + Claude API. Active F&O trader. Broad technical background, not deep in any one area — wants opinionated library-level guidance, not concept-level hand-waving. Dropped the original 17-day timeline on 2026-04-17; now depth-first, targeting institutional rigor ("Nobel Prize level" in the user's words) equivalent to or exceeding `github.com/shreejitverma/Adaptive-Volatility-Regime-Based-Execution-and-Risk-Framework`.

**Two artifacts, equal weight.**
1. A running multi-agent AI pod that continuously ingests data, runs analyses, executes backtests, and emits trading strategy / reports autonomously.
2. An institutional-grade research paper kept in-sync with the pod's empirical output. Current state: `PRELIMINARY_PAPER.md` (~10,000 words, version 0.1, methodology-complete, empirical-pending).

---

## 1. The Thesis in One Paragraph

Buy-Now-Pay-Later lending is a structurally hidden consumer-credit stack. Micro-loans under ~$500 are not reported to Experian/Equifax/TransUnion, so loan-stacking (~63% of BNPL borrowers per CFPB 2023) is invisible to FICO and by extension to the underwriting of every lender above BNPL in the consumer's capital stack. This produces a *leading-vs-reported* asymmetry: alternative-data streams (CFPB complaints, Reddit distress sentiment, Google Trends on keywords like "can't afford Affirm," MOVE index for fixed-income funding stress) move *before* the AFRMMT trustee reports show roll-rate acceleration and excess-spread compression. The thesis is that a composite of these alternative streams (the **BSI**) Granger-causes AFRMMT stress at 4–8 week lags, and that the correct trade is a **Total Return Swap (TRS) Payer position on junior tranches of Affirm Master Trust securitizations** — not an equity short on AFRM/SQ/PYPL, because those are squeeze-vulnerable. The trade fires only when four orthogonal gates agree (BSI ∧ SCP ∧ MOVE>120 ∧ CCD-II-proximity).

## 2. Locked Architectural Decisions

These were debated and resolved during the planning session. Do not relitigate without cause.

| Decision | Choice | Reason |
|---|---|---|
| ABS pricing model | **Jarrow-Turnbull reduced-form**, dynamic λ(t) = λ₀·exp(β_BSI·BSI + β_MOVE·MOVE + β_ES·compression) | User's original spec conflated Heston with ABS pricing. Heston is for equity options. Correct credit-derivatives model for tranche pricing is reduced-form intensity (Jarrow-Turnbull 1995, Duffie-Singleton 1999). |
| Heston usage | **Equity signal layer only** (AFRM/SQ options → SCP metric) | Never on ABS. Heston ≠ credit model. |
| Portfolio optimization | **CVXPY Mean-CVaR (primary)**; cuOpt only for routing/cardinality/turnover/integer constraints | cuOpt is combinatorial — it is not a natural solver for continuous Mean-CVaR. 160× speed claim is irrelevant at <100 assets. Using cuOpt for CVaR is a known trap. |
| LLM hosting | **NVIDIA NIM hosted Nemotron** (integrate.api.nvidia.com/v1, OpenAI-compatible) with **Claude API as fallback** via a unified `llm_client.py` | No GPU infra. User has NIM credits. TensorRT-LLM self-hosting is out of scope. |
| Model tiering | **Nemotron-small** for Data/Macro agents (structured parsing, classification); **Nemotron-4-340B** for Quant/Risk Manager (complex reasoning, final decisions) | Latency/cost vs. reasoning depth. |
| Agent framework | **LangGraph** state machine + **NVIDIA NeMo Agent Toolkit** for observability, tool persistence, audit trail | NeMo Agent Toolkit replaces what the original spec called "AI-Q." Audit trail is required for the paper's reproducibility appendix and the Human-in-the-Loop approve-trade UX. |
| Vector store | **Milvus** with **NV-EmbedQA** embeddings | Local, keeps data in repo, replaces OpenAI text-embedding-ada-002 for SEC/CFPB/AFRMMT document RAG. |
| Data warehouse | **DuckDB** single-file | Zero-config, analytics-fast, no Postgres daemon. |
| Data sources | Tier A free only: FRED, SEC EDGAR (`efts.sec.gov` public API, 10 req/s, **no proxies**), CFPB Complaint DB, Reddit PRAW, Google Trends via `pytrends`, FINRA short-interest public feeds, yfinance/CBOE for option chains | Tier B (Similarweb/Placer/Apptopia) and Tier C (Plaid/Equifax/Qlarifi/Bloomberg/Intex) framed as "plug your Tier C into our signal for a 3× amplification." No residential proxies. No App Store scraping. |
| Earnings-call ASR | **NVIDIA Parakeet-CTC** for real-time transcription of Affirm/Klarna/Block calls → Macro Agent for tone/keyword extraction | Genuine alpha signal on earnings days. |
| Dashboard | **Streamlit first** (fast), port to **Next.js + WebSockets** once core is working | User wants polish. Streamlit is the fast first-pass; Next.js is the institutional-polish final. |
| Paper format | **LaTeX**, hedge-fund-white-paper visual style (Tufte-like), Chicago author-date citations | Class format not specified; LaTeX is default institutional. |
| NLP sentiment | **FinBERT** (Araci 2019) primary, **FinBERT-tone** (Yang et al. 2020) as robustness | Both HuggingFace, commodity. |
| Execution | **Simulated TRS P&L only**. No brokerage integration. No ISDA paperwork. | Thesis does not depend on live execution. |

## 3. The Four-Gate Trade Logic (Non-Negotiable)

A trade fires **only** when all four gates agree:

1. **Gate 1 (BSI, weekly, leading):** BSI z-score > threshold. Default composite: 0.25·CFPB momentum + 0.20·Google Trends + 0.20·Reddit FinBERT negative ratio + 0.15·AppStore-keyword frequency + 0.20·MOVE overlay. Weights start at these priors and are refined by constrained QP regression against AFRMMT ground truth (simplex constraint, weights sum to 1).
2. **Gate 2 (SCP, per-asset):** Heston Expected Shortfall minus GBM Expected Loss, wide gap = equity is mispricing tail risk. Equity signal layer only.
3. **Gate 3 (MOVE, macro):** ICE BofA MOVE Index 30-day MA > 120 (funding stress regime).
4. **Gate 4 (CCD II calendar):** EU Consumer Credit Directive II compliance deadline Nov 2026 within trade horizon.

The **Risk Manager agent** vetoes any trade where fewer than four gates fire. The **Squeeze Defense Layer** is an additional veto on equity expressions (utilization > 85%, days-to-cover > 5, 25-δ IV skew elevated). Structured-credit TRS expression bypasses squeeze defense.

## 4. Repository Layout

```
bnpl-pod/
├── README.md
├── Makefile                        # make ingest | bsi | validate | backtest | paper | pod | dashboard
├── pyproject.toml                  # uv/poetry managed
├── .env.example
├── docker-compose.yml              # Milvus service
├── config/
│   ├── weights.yaml                # BSI weights, refined by QP after calibration
│   └── thresholds.yaml             # gate thresholds, squeeze-defense thresholds
├── data/
│   ├── ingest/
│   │   ├── fred.py                 # FRED API (MOVE, 10Y-3M, CC delinq, SOFR)
│   │   ├── sec_edgar.py            # efts.sec.gov search API, not scraper
│   │   ├── abs_parser.py           # AFRMMT 10-D / ABS-15G → roll rate, excess spread, CNL
│   │   ├── cfpb.py                 # CFPB public Complaint Database API
│   │   ├── reddit_praw.py          # r/povertyfinance, r/Debt, r/personalfinance, r/Affirm
│   │   ├── google_trends.py        # pytrends, keyword set K
│   │   ├── options_chain.py        # yfinance or CBOE free feed
│   │   ├── short_interest.py       # FINRA public short-interest
│   │   └── earnings_audio.py       # fetch earnings-call audio, Parakeet transcription
│   └── warehouse.duckdb
├── nlp/
│   ├── finbert_sentiment.py        # HF pipeline, rolling 30-day negative ratio
│   ├── finbert_tone.py             # robustness classifier
│   ├── embed_nvemb.py              # NV-EmbedQA via NIM
│   └── rag/
│       ├── milvus_store.py
│       └── ingest_docs.py          # index AFRMMT PDFs, 10-Qs, CFPB rules
├── signals/
│   ├── bsi.py                      # weighted z-score composite
│   ├── weights_qp.py               # constrained quadratic program for weight refinement
│   ├── granger.py                  # statsmodels grangercausalitytests + block bootstrap
│   └── rolling_oos.py              # rolling-window out-of-sample backtest of the signal itself
├── quant/
│   ├── jarrow_turnbull.py          # dynamic λ(t), Monte Carlo survival, tranche pricing
│   ├── heston_scp.py               # QuantLib Heston calibration → ES vs GBM EL
│   ├── squeeze_defense.py          # utilization, days-to-cover, IV skew veto logic
│   └── trs_pricer.py               # TRS Payer on junior tranche, SOFR funding leg
├── portfolio/
│   ├── mean_cvar.py                # CVXPY
│   └── routing_cuopt.py            # cuOpt for counterparty routing / cardinality / turnover
├── agents/
│   ├── llm_client.py               # unified NIM + Claude wrapper
│   ├── graph.py                    # LangGraph state machine
│   ├── state.py                    # PodState TypedDict
│   ├── data_agent.py               # Nemotron-small
│   ├── macro_agent.py              # Nemotron-small, reads BSI + FRED + earnings-call ASR
│   ├── quant_agent.py              # Nemotron-340B, runs JT + Heston + pricing
│   ├── risk_manager.py             # Nemotron-340B, four-gate consensus + squeeze veto
│   ├── report_agent.py             # generates paper-section updates from pod output
│   └── nemo_toolkit_wiring.py      # NVIDIA NeMo Agent Toolkit observability / audit trail
├── backtest/
│   ├── event_study.py              # Klarna Jul 2022, Affirm Nov 2022, CFPB May 2024, CCD II
│   ├── pnl_sim.py                  # simulated TRS P&L vs naive equity short
│   └── metrics.py                  # Sharpe, Sortino, max DD, tail metrics
├── dashboard/
│   ├── streamlit_app.py            # fast first-pass
│   └── next/                       # Next.js + WebSockets (v2)
├── paper/
│   ├── PRELIMINARY_PAPER.md        # current draft, version 0.1
│   ├── paper.tex                   # LaTeX build
│   ├── figures/                    # auto-generated by backtest + signals
│   └── references.bib
├── notebooks/                      # exploratory, one per paper section
├── tests/
│   ├── test_abs_parser.py          # regex / roll-rate extraction unit tests
│   ├── test_bsi.py
│   ├── test_jt.py                  # compare to closed-form JT where available
│   └── test_granger.py             # null-data false-positive rate check
└── logs/
    ├── agent_decisions/            # JSONL per run, for audit trail
    └── pod_runs/
```

## 5. Sprint Sequence (Depth-First, No Time Cap)

Order is enforced because later sprints depend on earlier outputs. Within each sprint the requirement is **full rigor, full tests, documented math in docstrings** — not a minimum viable version.

### Sprint 0 — Scaffolding
- `uv init` or `poetry init`, pin Python 3.11.
- Install: `duckdb`, `polars`, `pandas`, `numpy`, `scipy`, `statsmodels`, `cvxpy`, `QuantLib`, `langgraph`, `langchain`, `anthropic`, `openai` (for NIM OpenAI-compatible endpoint), `praw`, `pytrends`, `yfinance`, `transformers`, `torch`, `pymilvus`, `streamlit`, `matplotlib`, `pytest`, `python-dotenv`.
- Set up `.env.example` with: `NVIDIA_NIM_API_KEY`, `ANTHROPIC_API_KEY`, `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `FRED_API_KEY`, `SEC_EDGAR_UA` (required by SEC — a real name + email).
- Docker-compose for Milvus standalone.
- Makefile targets.
- Initialize DuckDB schema (tables: `fred_series`, `sec_abs_filings`, `cfpb_complaints`, `reddit_posts`, `google_trends`, `options_chain`, `short_interest`, `bsi_daily`, `pod_decisions`).

### Sprint A — Data Layer
- `fred.py`: pull MOVE (MOVE), 10Y-3M (T10Y3M), CC delinquency (DRCCLACBS), SOFR (SOFR) to daily series.
- `sec_edgar.py`: use `https://efts.sec.gov/LATEST/search-index?q=...&forms=10-D&dateRange=custom` JSON API. Rate-limit 10 req/s. Store filings list.
- `abs_parser.py`: for each AFRMMT 10-D filing, extract **roll rate to 60+ DPD**, **excess spread**, **cumulative net loss (CNL)**. Start with three fields; expand. Unit-test against three known historical filings with known values.
- `cfpb.py`: CFPB Complaint DB public API, filter on `product=consumer_loan` and sub-product containing "buy now pay later" or issuer names (Affirm/Klarna/Afterpay/Sezzle/PayPal). Daily complaint counts + keyword flags.
- `reddit_praw.py`: subreddits `R = {povertyfinance, Debt, personalfinance, Affirm, Klarna}`, pull posts + comments, rate-limit handling, dedup by ID.
- `google_trends.py`: keyword set `K` (see Paper Appendix C), weekly pull via `pytrends`.
- `options_chain.py`: AFRM, SQ, PYPL option chains, daily snapshot, strike × expiry × IV × volume × OI.
- `short_interest.py`: FINRA bi-monthly feed + daily short-volume from regulatory SHO files.
- **Test:** `make ingest` runs end-to-end, populates DuckDB, row counts sanity-check.

### Sprint B — NLP Layer
- `finbert_sentiment.py`: HF `ProsusAI/finbert`, batched inference over Reddit corpus, emit (post_id, neg_prob, neu_prob, pos_prob). Rolling 30-day negative-ratio aggregate.
- `finbert_tone.py`: `yiyanghkust/finbert-tone` as robustness.
- `nlp/rag/ingest_docs.py`: pull AFRMMT 10-D PDFs (last 3 years), Affirm 10-Qs, CFPB May 2024 interpretive rule, CCD II text. Chunk, embed via NV-EmbedQA, index in Milvus.
- Earnings-call ASR: `earnings_audio.py` fetches audio, Parakeet-CTC transcribes, chunks stored with timestamps. Macro Agent queries on earnings days.
- **Test:** verify FinBERT output distribution plausible on a 100-post sample; verify RAG recall on a known-answer query ("What was AFRMMT excess spread in March 2025?").

### Sprint C — Signal Layer (LOAD-BEARING; THE PAPER RESTS ON THIS)
- `bsi.py`: z-score each component over a rolling 180-day window; weighted composite per `config/weights.yaml`; emit `bsi_daily` series.
- `weights_qp.py`: constrained QP minimize ‖BSI·w − AFRMMT_stress‖² subject to w ≥ 0, Σw = 1, individual caps per `config/thresholds.yaml`. Refresh monthly; freeze during out-of-sample windows.
- `granger.py`: `statsmodels.tsa.stattools.grangercausalitytests(maxlag=10)` at monthly frequency. Target series: AFRMMT 60+ DPD roll rate, excess spread, CNL. Supplement with **block bootstrap** (Politis-Romano 1994) for small-sample-robust p-values. Report F-stat, p, bootstrap CI.
- `rolling_oos.py`: expanding-window refit of BSI weights + Granger test on held-out months. This is the primary defense against the small-sample critique (~40 monthly observations).
- **Pre-registration:** before running Granger, commit `hypotheses.md` specifying the exact test, lag range (4–8 weeks primary, 1–10 secondary), significance level, falsification criteria. Reference §13 of `PRELIMINARY_PAPER.md`.
- **Test:** simulate null data (white noise BSI vs real AFRMMT); Granger should reject at ~5% rate. Power analysis: simulate effect-size grid, document minimum detectable effect given n=40.

### Sprint D — Quant Layer
- `jarrow_turnbull.py`: discrete-time reduced-form pricer. λ(t) = λ₀·exp(β_BSI·BSI_t + β_MOVE·MOVE_t + β_ES·ExcessSpread_t). Recovery R from AFRMMT historical. Monte Carlo 25k paths with antithetic variates. Tranche pricing: price junior, mezz, senior at observed attachment/detachment points from the most recent AFRMMT prospectus supplement. Calibrate β's by matching model-implied tranche PV to observed spread, weekly.
- `heston_scp.py`: QuantLib `HestonModel` + `HestonModelHelper` calibration to AFRM option-chain surface. Compute 99%-ES under Heston. Compute 99%-EL under GBM with same σ(realized). SCP = ES_Heston − EL_GBM. Figure for paper.
- `squeeze_defense.py`: utilization = SI / float; days-to-cover = SI / ADV; 25-δ IV skew = IV(put, 25δ) − IV(call, 25δ). Veto if utilization > 0.85 OR days-to-cover > 5 OR skew > historical 90th percentile.
- `trs_pricer.py`: TRS Payer on junior tranche, funding leg SOFR + spread, Treasury-hedge overlay to isolate credit from duration. Mark-to-model weekly.
- **Test:** JT prices collapse to closed-form constant-λ case when β's set to 0. Heston calibration RMSE vs quoted IV surface < 3% vol points.

### Sprint E — Agent Pod
- `llm_client.py`: single class `LLMClient(model_tier: Literal['small','heavy'])`. Routes to NIM Nemotron by default, falls back to Claude on 5xx or timeout. Logs every call to `logs/agent_decisions/`.
- `state.py`:
  ```python
  class PodState(TypedDict):
      as_of: datetime
      bsi: float
      bsi_components: dict[str, float]
      move: float
      scp_by_ticker: dict[str, float]
      jt_tranche_prices: dict[str, float]
      squeeze_metrics: dict[str, dict[str, float]]
      ccd_ii_days_remaining: int
      gate_results: dict[Literal['bsi','scp','move','ccd2'], bool]
      veto_reasons: list[str]
      trade_signal: TradeSignal | None
      agent_log: list[AgentTurn]
  ```
- `graph.py`: LangGraph nodes Data → Macro → Quant → RiskManager → Report. State edges flow forward; RiskManager can loop back to Quant for a re-check. Persistence layer wired to NeMo Agent Toolkit so every decision is auditable.
- `data_agent.py`: refresh DuckDB, call ingestion modules, summarize what changed since last tick.
- `macro_agent.py`: read BSI, MOVE, CCD II calendar, earnings-call ASR of the day. Emits a macro-state paragraph.
- `quant_agent.py`: runs JT + Heston + SCP, writes outputs into state, justifies the four-gate determination in prose.
- `risk_manager.py`: enforces four-gate AND squeeze defense. Produces final trade signal or no-trade with veto reasons.
- `report_agent.py`: generates markdown diff against `PRELIMINARY_PAPER.md`'s empirical sections (§14 event study, §9 four-gate status).
- **Test:** dry-run over a historical date (2022-07-08, Klarna down-round week) with cached data; verify pod emits a positive signal and the reasoning trace contains BSI spike + MOVE elevation + squeeze-veto check.

### Sprint F — Portfolio Layer
- `mean_cvar.py`: CVXPY problem. Variables: weights w. Objective: minimize CVaR_α(−r'w). Constraints: Σw within leverage cap, per-name cap, cardinality via SOS1 or cuOpt handoff.
- `routing_cuopt.py`: if user configures a counterparty set with TRS-capacity constraints per counterparty, cuOpt handles the integer-assignment of notional across counterparties. This is the *correct* use of cuOpt.

### Sprint G — Backtest
- `event_study.py`: windows = { Klarna Jul 2022 down round, Affirm Nov 2022 guidance cut, CFPB May 2024 interpretive rule, each AFRMMT monthly release 2023–2025, CCD II deadline proximity Nov 2026 (prospective) }. For each window, compute CAR on AFRM equity, AFRMMT junior-tranche spread move, and simulated TRS P&L. Compare BSI-triggered cohort vs. random-trigger control.
- `pnl_sim.py`: deterministic seed. Emits `pnl.csv` + matplotlib PNGs directly into `paper/figures/`.
- `metrics.py`: Sharpe, Sortino, max DD, Calmar, tail ratio, Cornish-Fisher-adjusted VaR.

### Sprint H — Reporting
- `make paper` rebuilds `paper.pdf` end-to-end: runs notebooks, refreshes figures, pandoc/LaTeX compile.
- Streamlit dashboard: Macro Radar (BSI vs. AFRMMT excess spread), Squeeze Gauges (utilization/DTC/skew per ticker), Agent Debate Log panel, Execution Hub with "Approve Trade" stub that writes to `logs/pod_runs/`.
- Next.js port: once Streamlit is stable, reimplement with WebSockets streaming from the pod process.

## 6. Rigor Requirements (The "Nobel Prize Level" Bar)

These are the six places where taking a shortcut collapses the paper's credibility.

1. **Small-sample Granger.** n ≈ 40 monthly observations is insufficient for standard asymptotic F-tests to be trusted. Required mitigations: (a) block bootstrap with block length ~√n, 10k replications; (b) rolling-window out-of-sample tests; (c) explicit power analysis reporting the minimum detectable effect size; (d) pre-registration of hypotheses before touching the test.
2. **BSI weights overfitting.** If weights are optimized on the same sample as the Granger test, the test is invalid. Weights must be refit only on pre-test data; the OOS window is weight-frozen.
3. **Jarrow-Turnbull calibration drift.** Refit β's weekly, but publish the full β-history in the appendix. Cherry-picking a calibration date is a known reviewer trap.
4. **Heston use-case discipline.** Heston *only* on equity. Do not use Heston σ to price ABS tranches. This was a mistake in the original spec; do not revert.
5. **Squeeze defense is a hard gate, not a soft preference.** Any trade that ignores squeeze defense during a high-utilization regime invalidates the framework's self-consistency.
6. **TRS P&L honesty.** Simulated TRS returns must include: SOFR funding leg, bid-ask haircut (assume 25–50 bps for junior BNPL tranches), counterparty credit adjustment (CVA), and mark-to-model rather than mark-to-market for illiquid observations. Reporting gross-spread compression as "P&L" is a common mistake.

## 7. Known Traps (Learned During Planning)

- **cuOpt ≠ CVaR solver.** Do not attempt Mean-CVaR on cuOpt.
- **NVIDIA AI-Q Toolkit is actually the NeMo Agent Toolkit.** The original spec mis-named it.
- **SEC EDGAR has a free documented API** at `efts.sec.gov`. Do not set up BrightData or other residential proxies.
- **App Store scraping is blocked.** Apple/Google actively defeat it. The AppStore weight in BSI can be stubbed with CFPB complaint keyword frequency as a substitute until a clean feed appears.
- **Heston-on-ABS is the user's original spec's biggest error.** The paper explicitly corrects it in §1, §7, and §8.
- **Nemotron on NIM is hosted, OpenAI-compatible, and free with credits.** Do not attempt TensorRT-LLM self-hosting.

## 8. Verification Criteria

The build is "done" when all of the following pass:

1. `make ingest` populates DuckDB with ≥3 years daily FRED, ≥100 AFRMMT filings parsed, >10k Reddit posts classified, weekly Google Trends.
2. `make bsi` emits `bsi_daily` series covering the same window; figure matches paper §5.
3. `make validate` produces Granger F, p, bootstrap CI, rolling-OOS p-values. Primary test (BSI → AFRMMT 60+ DPD, lags 4–8 weeks) reports p < 0.05 on at least one roll-rate series OR the paper section is rewritten to honestly report the null (this is acceptable; the thesis becomes a weaker-form hypothesis).
4. `make backtest` produces `pnl.csv` + figures; simulated TRS strategy outperforms naive AFRM equity short in at least one of the three completed event windows.
5. `make pod` runs the LangGraph state machine end-to-end on the most recent data tick, emits a trade signal (or no-trade), and writes a complete audit trail to `logs/`.
6. `streamlit run dashboard/streamlit_app.py` renders without error; all panels populate.
7. `make paper` rebuilds `paper.pdf`; all figures resolve; citations compile.
8. A fresh `git clone` followed by `make setup && make all` runs to completion on a clean machine (record a screen-capture as proof).

## 9. What Already Exists

- `PRELIMINARY_PAPER.md` (version 0.1, ~10,000 words). Sections 1–17 + Appendices A/B/C. Methodology-complete. Empirical sections are scaffolded but await pod output. Khalil (2025) integrated in §2.4 and §13.3 (H6).
- Memory files at `.claude/projects/.../memory/` documenting scope, architecture, stack, NVIDIA mapping, thesis anchors. These are durable across sessions.

## 10. What to Do First in the Fresh Opus Session

1. Read `PRELIMINARY_PAPER.md` end-to-end.
2. Execute **Sprint 0** (scaffolding) and commit.
3. Execute **Sprint A** (data layer) with full tests; this is the longest sprint and the foundation for everything.
4. Execute **Sprint B → C** in order. Do not skip to Sprint D (Quant) before Sprint C (Signal) passes, because the JT model consumes BSI.
5. After Sprint C passes Granger validation (or honestly reports null), update §13–§14 of the paper with actual results.
6. Proceed to Sprints D → E → F → G → H.
7. Keep the paper in-sync with the pod at every sprint boundary. The pod's `report_agent.py` emits markdown diffs — apply them.

## 11. Non-Goals (Explicit)

- No real brokerage integration. No ISDA paperwork. No live orders.
- No self-hosted LLMs. No TensorRT-LLM. No GPU procurement.
- No residential proxies. No App Store scraping.
- No claim the strategy is deployable as-is. The paper frames it as an institutional research prototype with a "plug your Tier C data in for 3× amplification" commercialization pitch.
- No Postgres. No Kubernetes. No cloud deployment (local-first).

## 12. Reference Links

- User's benchmark repo: `github.com/shreejitverma/Adaptive-Volatility-Regime-Based-Execution-and-Risk-Framework`
- Khalil (2025), *J. Modelling in Management*, DOI 10.1108/JM2-08-2025-0415 — the direct methodological precedent.
- SEC EDGAR API: `https://efts.sec.gov/LATEST/search-index`
- FRED API: `https://fred.stlouisfed.org/docs/api/fred/`
- CFPB Complaint DB: `https://cfpb.github.io/api/ccdb/`
- NVIDIA NIM: `https://build.nvidia.com`
- NVIDIA NeMo Agent Toolkit: `https://github.com/NVIDIA/NeMo-Agent-Toolkit`
- Milvus: `https://milvus.io`

---

**End of masterplan.** Paste into Opus, give it filesystem access to the project directory, and instruct it to begin at Sprint 0. The memory files and `PRELIMINARY_PAPER.md` contain additional detail where needed.
