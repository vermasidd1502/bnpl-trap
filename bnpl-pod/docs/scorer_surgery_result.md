# Scorer-surgery result — status as of 2026-04-23

**Short version.** The origination-residual BSI cannot yet be computed. The
blocking dependency is Phase B of `docs/v2_roadmap.md` (10-Q / IR-deck
pulls), and the working-copy state of the repo is missing both the v1 BSI
scorer module and the frozen canonical event-window set. The pre-registered
decision rule (≥4/5 retain, ≤2/5 swap, 3/5 author decides) therefore cannot
be evaluated this morning.

What I have done overnight, per the `auto-accept, always allow` directive,
is stage the full Phase C interface in `signals/` so that the moment the
Phase B data lands the residualised event study is one function call away.
No paper-body edits have been made as a result of this work — the paper
remains in v2 behavioural-sensor framing, and `docs/alt_abstract_sealed.md`
remains sealed.

This document is the honest status doc. Read it in full before deciding
whether to post SSRN under v2 as-is or to block on Phase B.

---

## 1. Pre-registered decision rule (from `v2_roadmap.md` §C.3)

Reproduced verbatim so the rule is visible to anyone reading this doc
first:

- If ≥ 4 of 5 canonical events still fire the 4-gate AND under the
  residualised BSI: **v2 retains the behavioral-sensor framing**. Paper
  body unchanged; robustness appendix discloses the residual panel
  alongside the raw panel.
- If ≤ 2 of 5 events still fire: **v2 swaps to the construct-validity-
  only framing** sealed in `docs/alt_abstract_sealed.md`. Sealed abstract
  becomes v2 abstract verbatim.
- If exactly 3 of 5 fire: **author decides**; whichever framing is
  chosen, the 3/5 result is disclosed in the abstract.

The rule is fixed. The current exercise is to measure the survivor
count, not to reconsider the rule.

---

## 2. Current gating state

### 2.1 What is computable right now

Nothing that depends on originations. The following are computable and
already done (for transparency):

- `signals/granger_mde.py` — numerical MDE via scipy non-central F.
  Reproducible via `python -m signals.granger_mde`. Headline values:
  ΔR² ∈ [0.026, 0.033] across lags 4–8 at R²_full = 0.15. Embedded in
  paper §6.1.
- `signals/placebos.py` — interface stub for P1 / P2 / P3 placebos.
  Requires `cfpb_complaints` and `cfpb_complaint_narratives` warehouse
  tables; returns NaN + disclosure string until they land.
- `signals/originations_interp.py` — NEW this sprint. Quarterly→daily
  interpolation with pre-registered piecewise-linear spec and
  monotone-cubic-spline sensitivity alternative. Runs unchanged on any
  input matching its data contract. Input parquets not yet materialised
  (see 2.2).
- `signals/bsi_residual.py` — NEW this sprint. Residualisation spec
  (`m_t − α − β · log(g_t)`, trained on 2019-07-01 → 2025-06-30, no OOS
  refit), `fit_residualisation()`, `apply_residualisation()`,
  `run_residualised_event_study()` with `NotImplementedError` gate, and
  `format_comparison_table()` that applies the decision rule verbatim.

### 2.2 What is blocking the live run

Three dependencies, listed in dependency order. Each has its own
remediation path.

**Dependency A — Phase B data pulls.**

Required parquets:

- `data/10q/afrm_originations.parquet` — **blocked.** AFRM has zero
  filings in `sec_filings_index` (warehouse query 2026-04-23). Affirm's
  ABS securitisations are 144A private placements; the shelf trusts do
  not file 10-Ds in the issuer's name. Remediation: pull from AFRM's IR
  site directly (quarterly shareholder letters back to 2020-Q3 contain
  GMV and active-consumer counts). Estimate: 1–2 hours once a Selenium
  or `requests` fetch script is authorised.
- `data/10q/sq_afterpay_segment.parquet` — **partially blocked.** Block
  (formerly Square, CIK 0001512673) filings are in `sec_filings_index`,
  but segment-level Afterpay GMV lives in the 10-Q Item 7 MD&A
  narrative, not in a clean XBRL tag pre-2023-Q3. Parsing is manual for
  2022-Q3 through 2023-Q2 (5 quarters). Remediation: manual parse from
  10-Q PDFs. Estimate: 2 hours.
- `data/10q/pypl_payin4.parquet` — **partially blocked.** PayPal
  (CIK 0001633917) has 21 10-Q filings 2019-Q1 through 2025-Q3 in
  `sec_filings_index`. Pay-in-4 GMV disclosure is inconsistent
  quarter-to-quarter. Remediation: parse IR decks rather than 10-Qs for
  the sub-category split. Estimate: 1–2 hours; some quarters will be
  interpolated within an issuer (which is distinct from the
  quarterly→daily interpolation; document both).
- `data/10q/klarna_quarterly.parquet` — **deferred** per user caveat
  2026-04-22 (robustness only, do not block).

**Dependency B — v1 BSI scorer module.**

The paper prose (§5) describes a v1 scorer that produced the
`+27.4σ` headline v1 reading on 2025-01-17. The canonical
implementation is not in the current working copy: `signals/bsi.py`
does not exist. This is not a defect in the paper — the paper refers
to the scorer by its outputs, which are verifiable — but it is a gap
for reproducibility.

Remediation: either find the earlier-sprint working copy and restore
`signals/bsi.py`, or reconstruct the v1 scorer from the specification
in the paper (EWMA-σ with 250d half-life, per-pillar coverage-gate,
constrained QP fuse across CFPB, MOVE, and up-to-three optional
sentiment pillars). The second option is 1 day's work; the first is
minutes.

**Dependency C — frozen event-window set.**

`backtest/event_windows/` is referenced in `v2_roadmap.md` §A.5 and
paper §9 but is absent from the current working copy. The five
canonical events are named in the paper prose (and are frozen, per
pre-registration) but the date ranges and evaluation-window parameters
(t−5 business days through t+10 business days, per convention) are
expected to live in a JSON or YAML file at that path.

Remediation: reconstruct from paper §9 if the earlier-sprint working
copy is unavailable; 30 minutes.

### 2.3 What is NOT blocking

- The paper PDF. `paper_formal/paper_formal.pdf` rebuilt cleanly at
  39 pages this morning; all 13 v2 content markers verified. The paper
  can go to SSRN under v2 behavioural-sensor framing without the
  scorer surgery, because the falsification-gauntlet section (§6)
  discloses exactly what we can and cannot claim in the absence of the
  residualised result.
- The sealed alt abstract. `docs/alt_abstract_sealed.md` remains
  sealed. The Ulysses move does its job: if we post SSRN now under v2,
  and Phase B data lands next week, and the residualised BSI ≤ 2/5
  survives, the sealed abstract becomes v2.1 verbatim — we do not
  need to re-abstract under motivated-reasoning pressure.

---

## 3. Staged code — what ships now

### 3.1 `signals/originations_interp.py`

Responsibility: quarterly→daily on US business-day calendar, per-issuer
then composite-summed with a coverage bitfield.

Pre-registered spec (`InterpolationSpec`):

| Parameter                     | Value                 |
|-------------------------------|-----------------------|
| `method`                      | `"linear"`            |
| `calendar`                    | `"us-business"`       |
| `anchor`                      | `"quarter-end"`       |
| `extrapolate_forward_days`    | `0`                   |
| `extrapolate_backward_days`   | `0`                   |

Sensitivity alternative (Appendix D only): `method="pchip"` — monotone
cubic Hermite. Pre-registered here to prevent post-hoc choice of the
smoother that produces a "nicer" residual.

Coverage bitfield (`coverage_mask`): AFRM=1, SQ=2, PYPL=4, KLARNA=8.
Downstream scorer must gate on `coverage_mask > 0` when dividing;
`bsi_residual.fit_residualisation` already does this.

CLI: `python -m signals.originations_interp` prints the gating
disclosure and exits 2.

### 3.2 `signals/bsi_residual.py`

Responsibility: OLS fit of `m_t = α + β · log(g_t) + r_t` on the
training window; application of trained coefficients to full sample;
dispatch to event-study under `scorer="residual"`; format the
comparison table with the decision rule appended automatically.

`ResidualisationSpec` freezes: momentum window (28d), training
2019-07-01→2025-06-30, holdout 2025-07-01→, natural log, intercept,
**`refit_oos=False`** (raises on True — the one post-hoc choice we
can make by accident).

`fit_residualisation` emits a `ResidualisationFit` dataclass with
α, β, n_train, R², training-residual σ. The OLS is hand-rolled via
`numpy.linalg.lstsq` to avoid taking a statsmodels dependency for
three coefficients.

`apply_residualisation` emits the DataFrame the downstream QP fuse
consumes. The EWMA-σ standardisation is a placeholder here; when the
v1 EWMA module lands, the z-score column is re-computed through that
module for exact parity with the v1 pillar z-score.

`run_residualised_event_study` raises `NotImplementedError` with a
pointer to this doc until the three prerequisites land.

`format_comparison_table` renders a Markdown comparison of raw vs
residualised z-scores and 4-gate-AND outcomes across the 5 events,
and appends the decision rule verdict automatically via
`_decide_framing`. This is the function that will produce the table
in §4 of this doc once the live run is executable.

---

## 4. Comparison table — results

**Not yet computable. Placeholder schema only.**

When the live run executes, this section will be filled in by
`signals.bsi_residual.format_comparison_table()`. The schema is
guaranteed to be:

```
| Event date | Label                        | v1 z  | v2 z (residual) | v1 fires | v2 fires |
|------------|------------------------------|-------|-----------------|----------|----------|
| YYYY-MM-DD | <event name, e.g. Reg Z '25> | +27.4 | +X.XX           | Y        | Y / N    |
| ...        | ...                          | ...   | ...             | ...      | ...      |

Residual-scorer survivors: N / 5. Pre-registered decision: <rule verdict>.
```

The 5 canonical events (frozen, named in paper §9; not reproduced here
to avoid drift — read them from `paper_formal/paper_formal.tex`):

1. 17 January 2025 — Reg Z BNPL interpretive rule filing deadline
2. [event 2 per §9]
3. [event 3 per §9]
4. [event 4 per §9]
5. [event 5 per §9]

Left as bracketed placeholders because this doc is generated with the
paper's event labels as the single source of truth. Replace at live-run
time by reading from the frozen file `backtest/event_windows/` once it
is restored.

---

## 5. What this does NOT do

Explicitly, to pre-empt the motivated-reasoning failure mode:

- **No paper-body edits driven by this doc.** The paper is locked in
  v2 behavioural-sensor framing. If Phase B later delivers ≤ 2/5
  survival, the sealed alt abstract becomes v2.1 — not v2 — and we
  re-post to SSRN with the pivot documented in `docs/v1_retrospective.md`
  + a new `docs/v2_retrospective.md`. The original v2 is not
  retracted; it is superseded.
- **No "interpretation" of the empty result.** "The residualised BSI
  cannot yet be computed" is not evidence that it will survive, and is
  not evidence that it will not. It is evidence that we have not yet
  pulled the originations data. This doc refuses to read the empty
  result as support for either framing.
- **No SSRN action.** SSRN upload is a user-action gate. This doc
  informs the decision; it does not take the decision.

---

## 6. Operational checklist — how to run this live

Once the three blockers are resolved, the live run is:

```bash
# Phase B (one-time, lands in data/10q/)
python -m scripts.pull_afrm_ir      # not yet written
python -m scripts.pull_sq_afterpay  # not yet written
python -m scripts.pull_pypl_payin4  # not yet written

# Phase C (reruns every time Phase B refreshes)
python -c "
from signals.originations_interp import load_quarterly_originations, interpolate_daily
from signals.bsi_residual import (
    fit_residualisation, apply_residualisation,
    run_residualised_event_study, format_comparison_table,
)
q = load_quarterly_originations()
d = interpolate_daily(q)
# complaints loaded from warehouse cfpb_complaints:
# c = duckdb.query('SELECT date, count(*) as bnpl_complaint_count '
#                  'FROM cfpb_complaints WHERE product ILIKE %BNPL% '
#                  'GROUP BY date ORDER BY date').df()
# fit = fit_residualisation(c, d)
# _ = apply_residualisation(c, d, fit)
rows = run_residualised_event_study()
print(format_comparison_table(rows))
"
```

The output Markdown table goes into §4 of this doc. The decision-rule
verdict appends automatically.

---

## 7. Recommendation to self on waking

1. **Sign off on `docs/v1_retrospective.md` first** (three `[author
   confirms]` markers). That is the public attestation; it is
   framing-independent; do it now.
2. **Post SSRN under v2** as currently built (39-page PDF). Rationale:
   the falsification gauntlet discloses everything we can and cannot
   claim; the sealed alt abstract is our insurance if Phase B later
   pivots us. The value of posting sooner exceeds the value of waiting
   on a result that (per §2.2) is 1 calendar day of data-engineering
   work away.
3. **Put Phase B on the v2.1 milestone**, not the v2 milestone. Budget
   1 day for AFRM IR + SQ + PYPL parses; reconstruct v1 BSI scorer
   from paper §5 spec; restore frozen event windows. Then run §6 of
   this doc.
4. **Only if Phase B result is ≤ 2/5**: open `docs/alt_abstract_sealed.md`,
   copy abstract verbatim to `paper_formal/paper_formal.tex`, write
   `docs/v2_retrospective.md`, rebuild PDF as v2.1, re-post to SSRN.

Author: Siddharth Verma.
Provenance: v2 scorer-surgery staging doc, 2026-04-23.
