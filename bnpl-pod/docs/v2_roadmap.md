# v2 Roadmap — what the post-retrospective paper must deliver

**Audience:** the author (Siddharth), reading this cold in the morning
after a long overnight build, deciding what to accept/adjust before
pointing the paper at SSRN.

**Guiding principle:** every quantitative claim in v2 must survive
three filters —
(a) could a referee **falsify** it with the data in this repo alone,
(b) is the σ estimator, horizon, and lag set **pre-registered** in
    §3 before any estimate is reported, and
(c) is the **effect-size claim bounded below** by a numerically
    computed MDE.

---

## Phase A — Statistical infrastructure (no new data, code-only)

These are all runnable from the current `data/warehouse.duckdb` (505 MB)
without SEC or 10-Q pulls. Priority order matches ROI on referee
defensibility.

### A.1 Local-projection IRF (Jordà 2005) on BSI→HYG
- **File:** `signals/lp_irf.py` (new)
- **Spec:** h = 2, 4, 6, 8, 10, 12 weeks; `HYG_ret_{t+h}` on
  `BSI_innovation_t`, controls = {MOVE, SPY, lagged HYG}.
- **Output:** `paper/figures/fig10_lp_irf.png` showing coefficient
  path with 95% CI band.
- **Pre-reg:** horizon set 2–12 in steps of 2; confidence = 95%;
  Newey-West HAC SE with bandwidth = 4.
- **Expected result per §4 intuition:** positive coefficient peaking
  at h=6–8. If negative or non-monotone, paper discloses and
  discusses.

### A.2 Numerical MDE for Granger
- **File:** `signals/granger_mde.py` (new)
- **Spec:** for n=399, lags {4,5,6,7,8}, α=0.05, power=0.80, compute
  the minimum ΔR² detectable via `scipy.stats.ncf.ppf`.
- **Expected:** ΔR² ∈ [0.021, 0.031] across lag set.
- **Output:** inline in §6, replaces any "orthogonal" prose with
  "we cannot detect a Granger ΔR² larger than [MDE]; smaller
  linkages are not ruled out."

### A.3 Three placebo sensors
- **File:** `signals/placebos.py` (new)
- **Placebo 1 — word-count sensor:** ignores sentiment, counts
  all CFPB complaints mentioning "late" or "fee." Z-scored with
  the same EWMA-σ infrastructure. Expected behavior: fires on
  many CFPB volume events, not just BNPL-specific ones.
- **Placebo 2 — randomised-complaint sensor:** permutes complaint
  timestamps within each year. Expected: no signal on 2025-01-17
  deadline (structural window broken).
- **Placebo 3 — non-BNPL complaint-category sensor:** same BSI
  machinery but applied to credit-card complaint momentum.
  Expected: baseline sensor that responds to general CC stress,
  not BNPL-specific stress. If BSI is distinct, the correlation
  between these two sensors must be modest (we pre-register
  |ρ| ≤ 0.5 as the discriminating threshold).

### A.4 Cross-sectional panel (time-cross-cohort)
- **File:** `signals/cross_section.py` (new)
- **Spec:** quintile sort on BSI exposure of BNPL-lender stock
  returns (AFRM, SQ, PYPL) at weekly frequency; Fama-MacBeth
  t-stats on the high-minus-low return.
- **Purpose:** provide a cross-sectional complement to the time-
  series event study that is less sensitive to the 5-event
  small-n problem.

### A.5 Pre-registration document
- **File:** `docs/v2_prereg.md` (new)
- **Contents:**
  - Sample window: 2019-07-01 to 2025-06-30 as training; 2025-07-01
    onward held out.
  - Horizons: 2, 4, 6, 8, 10, 12 weeks for IRFs.
  - σ estimator: EWMA, half-life = 250d, floor = 0.6.
  - Pillar weights: QP-solved monthly on training window only.
  - Event set: the 5 windows currently in `backtest/event_windows/`,
    frozen now; no events added post-freeze.
  - Placebo discrimination threshold: |ρ| ≤ 0.5 between BSI and
    non-BNPL complaint sensor.
  - Granger MDE: reported numerically; non-rejection interpreted
    as "ΔR² ≤ [MDE] cannot be ruled out," nothing stronger.
- **Timestamp:** git-committed before any v2 estimate is
  re-run on held-out data.

### A.6 Notebooks / diagnostics
- **File:** `notebooks/diagnostics.ipynb` (new)
- **Contents:**
  1. BSI σ estimator comparison (180d rolling vs EWMA 250d)
  2. Pillar coverage histograms
  3. Freeze-flag counts by pillar, by year
  4. Residualisation residuals (origination-residual BSI vs
     raw-momentum BSI) — scatter + correlation
  5. LP-IRF coefficient path (replication of §A.1)
  6. Placebo-sensor correlation matrix
- **Purpose:** reviewer can run this notebook end-to-end from
  the committed warehouse and reproduce every figure in §5–§6.

---

## Phase B — External data pulls (warehouse-augmenting)

Phase B runs autonomously if the Phase 2 scorer-surgery auto-chain is
invoked. If the scorer-surgery result does not change the paper's
framing, Phase B is optional for v2 but required for v3.

### B.1 AFRM quarterly actives / GMV (SEC EDGAR 10-Q)
- **Source:** AFRM 10-Q filings, 2020-Q3 to latest.
- **Target table:** `data/warehouse.duckdb::afrm_quarterly`
- **Columns:** `quarter_end`, `active_consumers`, `gmv_usd`,
  `loans_originated`, `delinquency_30d`, `delinquency_90d`.
- **Method:** EDGAR XBRL parser (no HTML scraping required).
- **Budget:** 1–2 hours.

### B.2 SQ / Block Afterpay-segment quarterly (SEC EDGAR 10-Q)
- **Source:** SQ 10-Q filings, 2022-Q2 (Afterpay close) to latest.
- **Target table:** `data/warehouse.duckdb::sq_afterpay_segment`
- **Columns:** `quarter_end`, `afterpay_gmv`, `afterpay_actives`.
- **Method:** segment-disclosure parsing; requires manual note
  review for 1–2 quarters where segment reporting changed.
- **Budget:** 2 hours.

### B.3 PayPal Pay-in-4 (PYPL IR decks)
- **Source:** PYPL quarterly IR decks + shareholder letters.
- **Target table:** `data/warehouse.duckdb::pypl_payin4`
- **Columns:** `quarter_end`, `payin4_gmv`.
- **Method:** PDF parse of IR decks; some quarters disclose,
  some don't. Document missingness.
- **Budget:** 1–2 hours.

### B.4 Klarna quarterly (robustness only, DO NOT BLOCK)
- **Source:** Klarna investor reports (non-public company; partial
  disclosure via Swedish financial press + European press).
- **Target table:** `data/warehouse.duckdb::klarna_quarterly`
- **Budget:** 2–3 hours *(per user's 2026-04-22 caveat — treat as
  robustness check, do not block Phase 2 progress waiting for it).*
- **Priority:** LAST. Only attempt after AFRM, SQ, PYPL are in
  the warehouse.

---

## Phase C — Scorer surgery + paper integration

### C.1 Interpolate quarterly → daily originations
- **File:** `signals/originations_interp.py` (new)
- **Method:** piecewise-linear with holiday adjustment; alternative
  cubic spline for sensitivity.

### C.2 Origination-residual BSI
- **File:** `signals/bsi_residual.py` (new)
- **Method:** regress complaint momentum on log(originations),
  use residual as the CFPB pillar input.

### C.3 Re-run event study with residualised BSI
- **Modify:** `backtest/event_study.py` to accept a `scorer=`
  parameter: `"raw"` (v1), `"residual"` (v2).
- **Output:** `docs/scorer_surgery_result.md` — comparison
  table of catalyst-window pass counts under raw vs residual
  scorer.
- **Decision criterion** (pre-registered here, not post-hoc):
  - If ≥4 of 5 events still fire the 4-gate AND under residual
    scorer: v2 retains behavioral-sensor framing.
  - If ≤2 of 5 events still fire under residual scorer: v2 swaps
    to the construct-validity-only framing sealed in
    `docs/alt_abstract_sealed.md`.
  - If 3 of 5 fire: author decides; paper must disclose the
    3/5 result in the abstract either way.

### C.4 Paper surgery integration
- Rewrite §5 with origination-residual disclosure.
- Re-run §9 tables under residual scorer, report both panels
  side-by-side.
- Regenerate Figures 1, 2, 5, 9 under residual scorer; keep
  raw-scorer versions in Appendix D for comparison.

---

## Phase D — Writing tasks (prose, no new computation)

### D.1 References expansion
- **File:** `paper_formal/references.bib`
- **Adds:** Baker-Wurgler 2006 JFE; Baker-Wurgler 2007 JEP;
  Da-Engelberg-Gao 2011 JF; Da-Engelberg-Gao 2015 RFS (FEARS);
  Loughran-McDonald 2011 JF; Tetlock 2007 JF; Antweiler-Frank
  2004 JF; Hirshleifer-Shumway 2003 JF; Mian-Sufi 2009 QJE;
  Mian-Sufi 2011 AER; Mian-Sufi 2014 Econometrica; Gross-
  Souleles 2002 RFS; deHaan-Kim-Lourie-Zhu 2024 JFE;
  Guttman-Kenney-Firth-Gathergood 2023 JCR; Jordà 2005 AER.

### D.2 Construct-validity §2.5
- Decompose complaint-filing into: (i) distress, (ii) awareness,
  (iii) regulatory-mechanics (filing deadlines, press coverage
  of CFPB activity), (iv) complaint-propensity demographics.
- For each: what can BSI separate, what it cannot, what a
  referee should treat as a confound, and how the
  residualisation helps.

### D.3 Threats-to-validity §10 expansion
- **Sentiment attack surface:** bot farms, prompt-farm
  Google-Trends pumping, paid-review services on App Store.
  Pillar caps in QP help; do not eliminate.
- **AI-search dark-channel migration:** if consumer search
  traffic shifts from Google to Perplexity / ChatGPT, Google
  Trends pillar loses coverage; we cannot monitor dark-channel
  directly.
- **Regulatory fragility of CFPB complaint database:** the
  database is a single point of failure for the sensor; a
  hypothetical CFPB defunding or a change in public-disclosure
  rules kills the primary pillar.

### D.4 Micro-Leverage Epoch / Subprime 2.0 / Paulson removal
- `grep -n "Micro-Leverage Epoch" paper_formal/paper_formal.tex`
- `grep -n "Subprime 2.0" paper_formal/paper_formal.tex`
- `grep -n "Paulson" paper_formal/paper_formal.tex`
- All instances: cut or footnote.

### D.5 "+27σ" removal
- `grep -n "27" paper_formal/paper_formal.tex | grep -i sigma`
- All headline instances: replace with "12,838 complaints on
  2025-01-17 vs. trailing-180d baseline ~58/day (~221× ratio)."

### D.6 "four-pillar" sweep
- `grep -n "four-pillar" paper_formal/paper_formal.tex`
- All instances: replace with "CFPB-MOVE composite" *or* add
  explicit coverage-gate qualifier per v2 §5.

### D.7 Bypass-disclosure paragraph in §9.y
- Replace the v1 "super-threshold bypass" framing with a
  disclosure paragraph that states:
  - Threshold added 2026-04-22 (post-review, post-event)
  - Calibrated to fire exactly once in sample
  - Retained in codebase only for v1 reproducibility
  - Not a predictive rule; no OOS claim made from it
  - Referenced retrospective: `docs/v1_retrospective.md`

### D.8 Author-responsibility footnote on title page
- Single-paragraph footnote pointing reader to the v1
  retrospective and the sealed alt abstract.

---

## Verification

Each phase halts if the prior fails.

1. **Phase A runs end-to-end** via `make v2-signals` (new Makefile
   target). Output: 6 new figures, 1 MDE number, 3 placebo
   correlations, 1 LP-IRF path.

2. **Phase B runs end-to-end** via `make v2-data` (new Makefile
   target). Output: 4 new warehouse tables (afrm, sq, pypl,
   optionally klarna).

3. **Phase C runs end-to-end** via `make v2-surgery` (new Makefile
   target). Output: `docs/scorer_surgery_result.md` + comparison
   figures + (if pivot triggered) paper text updated per sealed
   alt abstract.

4. **Phase D** is writing, not testable by CI. Each sub-task is
   independently commit-able.

5. **Paper builds** via `make paper-formal` — 33+ pages (v2 will
   likely be ~40 pages with §2.5 + §6 rewrite + §10 expansion).

6. **SSRN-ready:** title page, author info, JEL codes (G12, G17,
   G23, G28), keywords, abstract ≤ 250 words, single PDF.
