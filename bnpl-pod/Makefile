# BNPL Pod — canonical build targets.
# Each target is deterministic, idempotent, and writes intermediate CSVs so
# downstream targets can run from cache.

PY ?= python
UV ?= uv

.PHONY: setup init-db ingest bsi validate backtest pod dashboard frontend snapshot serve paper test lint clean all

setup:
	$(UV) venv
	$(UV) pip install -e ".[dev]"

init-db:
	$(PY) -m data.schema

ingest:
	$(PY) -m data.ingest.fred
	$(PY) -m data.ingest.sec_edgar
	$(PY) -m data.ingest.abs_parser
	$(PY) -m data.ingest.cfpb
	$(PY) -m data.ingest.reddit_praw
	$(PY) -m data.ingest.google_trends
	$(PY) -m data.ingest.options_chain
	$(PY) -m data.ingest.short_interest

bsi:
	$(PY) -m signals.bsi
	$(PY) -m signals.weights_qp

validate:
	$(PY) -m signals.granger
	$(PY) -m signals.rolling_oos

backtest:
	$(PY) -m backtest.event_study
	$(PY) -m backtest.pnl_sim

pod:
	$(PY) -m agents.graph

dashboard:
	streamlit run dashboard/streamlit_app.py

# React institutional terminal: warehouse -> JSON -> static-serve web/ on :8765.
snapshot:
	$(PY) -m dashboard.build_snapshot

serve:
	cd web && $(PY) -m http.server 8765

frontend: snapshot
	@echo ""
	@echo "  pod_snapshot.json rebuilt from live warehouse."
	@echo "  run 'make serve' in another terminal, then open http://localhost:8765/"
	@echo ""

paper:
	$(PY) -m paper.make_figures
	cd paper && pdflatex -interaction=nonstopmode paper.tex
	cd paper && bibtex paper
	cd paper && pdflatex -interaction=nonstopmode paper.tex
	cd paper && pdflatex -interaction=nonstopmode paper.tex
	@echo ""
	@echo "  paper.pdf rebuilt at paper/paper.pdf"
	@echo ""

test:
	pytest -q

lint:
	ruff check .
	ruff format --check .

clean:
	rm -rf data/warehouse.duckdb data/warehouse.duckdb.wal
	rm -f logs/agent_decisions/*.jsonl logs/pod_runs/*.jsonl
	rm -f paper/figures/*.png paper/figures/*.pdf

all: setup init-db ingest bsi validate backtest pod paper
