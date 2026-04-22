# BNPL Pod

A multi-agent AI pod that continuously ingests alternative data, detects
credit-stress build-up in Buy-Now-Pay-Later issuers, and emits **advisory**
trade signals for a Total-Return-Swap (TRS) expression on junior tranches of
Affirm Master Trust (AFRMMT) securitizations.

Companion to `PRELIMINARY_PAPER.md` (research paper, v0.1) and `MASTERPLAN.md`
(handoff brief).

## Architecture in one picture

```
  Alternative data                                Ground truth
  -----------------                               ------------
  CFPB | Reddit | Trends | MOVE | FINRA    --+        AFRMMT 10-D
                                              |        (trailing, from
                                              v        SEC EDGAR)
                                   +------ BSI -------+
                                   | (signals/bsi.py) |
                                   +---------+--------+
                                             |
          +---- Granger causality + bootstrap OOS (signals/granger.py) ----+
                                             |
                                             v
   +-----------------+    +----------------+  +----------------+  +---------+
   | Jarrow-Turnbull |    | Heston SCP     |  | Squeeze Def.   |  | CCD II  |
   | (ABS tranche)   |    | (equity layer) |  | (equity veto)  |  | calendar|
   +--------+--------+    +--------+-------+  +-------+--------+  +----+----+
            |                      |                  |                |
            +----------+-----------+                  +----+-----------+
                       |                                   |
                       v                                   v
                +------------+                     +--------------+
                |  LLM risk_ |  advisory-only      | Deterministic|
                |  manager.py| ------------------> | compliance   |
                |  (LangGraph|                     | engine (no   |
                |   Nemotron)|                     | LLM, hard    |
                +------------+                     | rules only)  |
                                                    +------+------+
                                                           |
                                                           v
                                                   +---------------+
                                                   | Human-in-loop |
                                                   |  approval     |
                                                   +---------------+
```

**Critical invariant:** `agents/compliance_engine.py` is deterministic, LLM-free, and is the sole source of trade approval. The LLM risk manager produces narrative reasoning; compliance decides. Human sign-off is required.

## Institutional Pod Architecture

The repo physically separates **speed of execution** from **depth of verification** —
the same split a tier-one credit desk puts between its PM tear-sheet and its risk
terminal. The layers share one DuckDB warehouse but never collide in the same process.

```
                        data/warehouse.duckdb          <- single source of truth
                             /                \
               build_snapshot.py              live DuckDB read
                           /                      \
              web/pod_snapshot.json          dashboard/sbg_dashboard.py
                         |                              |
           +-------------+-----+            +-----------+------------+
           | Layer 1: PM Tear- |            | Layer 2: Quant Risk    |
           | Sheet (React)     |            | Engine (Streamlit)     |
           | web/PodTerminal   |            | :8501                  |
           | :8765             |            |                        |
           +-------------------+            +------------------------+
                 ^                                      ^
                 |  5-second time-to-conviction         |  deep diagnostic depth
                 |  BSI, gates, bypass, catalyst        |  Granger, JT paths,
                 |                                      |  pillar coverage, agents
                 +------------------ reviewer ----------+

                     agents/  +  quant/   <- Core Engine (LangGraph pod,
                                             compliance gates, JT pricing)
```

| Conceptual layer | Directory | Role | Entry point |
|---|---|---|---|
| **Layer 1 — PM Tear-Sheet** | `web/` | Execution & consensus; 5-second time-to-conviction for the PM | `make frontend` (rebuild snapshot) then `make serve` (http.server on `:8765`) |
| **Layer 2 — Quant Risk Engine** | `dashboard/` | Deep diagnostics, telemetry, and thesis defence | `make dashboard` (Streamlit on `:8501`) |
| **Core Engine** | `agents/` + `quant/` | LangGraph orchestration, deterministic compliance gates, Jarrow-Turnbull pricing | `python -m agents.tick --persist --optimize` |
| **Shared warehouse** | `data/` | DuckDB single source of truth + ingest connectors | `python -m data.schema` |

### Data flow

- **Layer 1 never touches DuckDB directly.** `dashboard/build_snapshot.py` flattens the
  warehouse into `web/pod_snapshot.json` (≈3 k lines: gates, bypass state, backtest
  windows, agent log, radar, 12-month BSI sparkline). `web/PodTerminal.tsx` is a
  single-file React 18 component served statically — no backend, no websocket, no
  database connection. Consequence: the tear-sheet is instant, cacheable, and survives a
  warehouse lock.
- **Layer 2 reads the warehouse live.** `dashboard/sbg_dashboard.py` opens a
  read-only DuckDB connection (TTL-cached 15–120 s) and queries `bsi_daily`,
  `granger_results`, `pod_decisions`, `abs_tranche_metrics`, and the ingestion tables.
  Consequence: risk analysts see state as of the most recent tick, not as of the last
  snapshot build.
- **Both layers are read-only consumers.** Only `agents.tick` and the ingest modules
  write to the warehouse.

### Compliance boundary

Three rules protect this from becoming "an LLM that approves trades":

1. **LLM agents (macro / quant / risk) are advisory-only.** Their JSON reasoning lands
   in `logs/agent_decisions/YYYY-MM-DD.jsonl` and surfaces on both layers as an audit
   trail; it never reaches the trade decision.
2. **The 3-gate AND in `agents/compliance_engine.py` (lines 133–159) is the sole
   approval authority.** Gate 1 is BSI z ≥ +1.50 σ, Gate 2 is MOVE 30-day MA ≥ 120,
   Gate 3 is CCD-II T-minus within calendar window. All three must fire; the engine is
   pure Python, zero LLM, zero network calls.
3. **The `|z| ≥ 10` super-threshold bypass at lines 185–199 is the one explicit
   Type-I premium override.** When BSI alone hits the behavioural-panic tail, the bypass
   approves the trade and stamps `bypass_fired=True` on the decision record, making the
   premium paid for that override auditable after the fact (paper §8.5).

A reviewer can start at the tear-sheet (Layer 1) for the "is this trade on?" question,
flip to the risk terminal (Layer 2) for the "why should I believe it?" question, and read
the compliance engine source for the "what will fire without a human in the loop?"
question — in that order, in under five minutes.

## Known limitations (baked into the design)

1. **EDGAR data is trailing.** AFRMMT 10-D filings land after the OTC credit-derivatives market has repriced. BSI is the leading indicator; EDGAR fields are ground truth for validation only. This is acknowledged in paper §15.
2. **λ-stability.** Social-media sentiment drives part of the JT hazard intensity. To prevent hallucinated crises from viral Reddit posts, `quant/jarrow_turnbull.py` applies EWMA smoothing (half-life 5d) to the sentiment regressor, clips λ to `[0.002, 0.25]`, and regularizes `β_BSI` when one-day sensitivity exceeds 10%. See `config/thresholds.yaml → jarrow_turnbull`.
3. **OTC illiquidity.** Junior ABS tranches are bespoke, ISDA-gated, low-turnover instruments. The pod does **not** execute; it emits advisory signals for a human credit trader to negotiate. No brokerage or ISDA integration exists in this codebase.
4. **LLM reasoning is advisory only.** The risk-manager LLM can and will produce different narratives for identical inputs. Compliance is deterministic precisely because regulators cannot accept LLM output as a trade approval.

## Quickstart

```bash
# 1. Create venv + install
make setup

# 2. Copy secrets template and fill in API keys
cp .env.example .env

# 3. Initialize DuckDB schema
make init-db

# 4. Ingest data (~30 min on first run; subsequent runs are incremental)
make ingest

# 5. Build signal + validate
make bsi
make validate

# 6. Backtest
make backtest

# 7. Run the pod (emits advisory signal + compliance decision for current date)
make pod

# 8. Dashboard
make dashboard

# 9. Rebuild the paper with fresh figures
make paper
```

## Repo layout

```
bnpl-pod/
├── config/              weights.yaml, thresholds.yaml
├── data/                schema.py, settings.py, ingest/*
├── nlp/                 FinBERT, NV-EmbedQA, Milvus RAG
├── signals/             bsi.py, weights_qp.py, granger.py, rolling_oos.py
├── quant/               jarrow_turnbull.py, heston_scp.py, squeeze_defense.py, trs_pricer.py
├── portfolio/           mean_cvar.py, routing_cuopt.py
├── agents/              llm_client.py, compliance_engine.py, graph.py, <per-role>.py
├── backtest/            event_study.py, pnl_sim.py, metrics.py
├── dashboard/           streamlit_app.py, next/
├── paper/               PRELIMINARY_PAPER.md, paper.tex, figures/
├── notebooks/           exploratory, one per paper section
├── tests/               pytest, starts with compliance engine contract tests
└── logs/                agent_decisions/*.jsonl, pod_runs/*.jsonl
```

## Verification

The build is "done" when:

1. `make ingest` produces ≥3y daily FRED, ≥100 AFRMMT filings parsed, >10k Reddit posts, weekly Trends.
2. `make bsi` emits `bsi_daily` series matching paper §5.
3. `make validate` produces Granger F/p, bootstrap CI, rolling-OOS p-values.
4. `make backtest` produces `pnl.csv` and figure PNGs.
5. `make pod` runs LangGraph end-to-end, emits a decision + full audit trail in `logs/`.
6. `pytest` passes, including the contract tests enforcing compliance-engine determinism.
7. `make paper` rebuilds `paper.pdf` with all figures and citations.

## References

- `PRELIMINARY_PAPER.md` — methodology, thesis, references (Jarrow-Turnbull 1995, Duffie-Singleton 1999, Heston 1993, Araci 2019, Di Maggio 2022, Khalil 2025, CFPB 2023/2024).
- `MASTERPLAN.md` — handoff brief.
- SEC EDGAR API: `https://efts.sec.gov/LATEST/search-index`
- NVIDIA NIM: `https://build.nvidia.com`
- NVIDIA NeMo Agent Toolkit: `https://github.com/NVIDIA/NeMo-Agent-Toolkit`
