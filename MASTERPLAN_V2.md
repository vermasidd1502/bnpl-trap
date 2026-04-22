# MASTERPLAN v2 — BNPL Trap

**Author:** Siddharth Verma (UIUC, *Quantamental Investment*, Spring 2026)
**Date:** 2026-04-18
**Status:** supersedes `MASTERPLAN.md` (v1)

---

## 0. Why v2 exists

v1 framed the pod as a *signal-generation engine* with four trade gates and a
deterministic compliance layer. The methodology was sound but the **empirical
claim was weak**: *"BSI predicts BNPL credit stress 4–8 weeks ahead."* A
reviewer's first objection writes itself — *"Maybe BSI just tracks consumer
credit stress generally. Why is this BNPL-specific?"*

v2 rebuilds the paper around a **treated-vs-control research design** and a
much richer statistical toolkit. The claim becomes:

> **BSI predicts BNPL-issuer credit stress with a loading that is statistically
> larger than its loading on traditional and near-prime consumer lenders, and
> this differential widens in extreme-event regimes.**

That is a structural claim. It survives the obvious objections because it
names them inside the test. It is also harder to get right, which is why the
mathematics below is not decorative — it is what makes the claim defensible.

---

## 1. Thesis, restated with identification

### 1.1 Target

Let $D^{(i)}_t$ be a delinquency / stress observable for issuer $i$ at time $t$.
Issuers are partitioned into three groups:

| Group | Members (tickers) | Credit stress proxy $D^{(i)}_t$ |
|---|---|---|
| $\mathcal{T}$ — **Treated (BNPL)** | AFRM, KLAR (private → CDS spread), SQ/Afterpay, PYPL Pay Later sleeve, SEZL, ZIP | 10-D / ABS-15G trustee roll rate, excess spread, CNL |
| $\mathcal{C}_1$ — **Near-control (non-bank consumer lenders)** | COF, SYF, DFS, AXP, OMF | 10-Q net charge-off rate, 30+ DPD rate |
| $\mathcal{C}_2$ — **Placebo (no consumer credit exposure)** | V, MA, JPM segment-ex-card, BRK-B | Revenue, stock return (should *not* load on BSI) |
| $\mathcal{C}_3$ — **Subprime-auto (historical analog)** | CACC, ALLY (subprime sleeve), SC/SDART trust series, AMCAR trust series, BCRST (DriveTime) trust series, EART (Exeter) trust series | 10-Q NCO + auto-ABS trustee reports |

Group $\mathcal{C}_3$ is the closest structural analog to BNPL in the
historical record: short-duration, subprime, thin-file, heavily securitized,
and with a well-documented 2015–2017 and 2023–2024 stress wave. Including
it lets us ask whether BSI's loading on BNPL issuers is larger than on the
product class most similar to BNPL — a much sharper identification claim
than comparing against prime-bank cards alone.

Mortgages / home loans are deliberately excluded: duration (30y vs. 6w),
regulatory regime (post-QM) and structural risk physics are all wrong.

### 1.2 Core econometric statement

Run, for each issuer $i$ and lag $k \in \{1,\dots,K\}$:

$$
\Delta D^{(i)}_t = \alpha^{(i)} + \beta^{(i)}_k \cdot \text{BSI}_{t-k} + \gamma^{(i)} \mathbf{X}_t + \varepsilon^{(i)}_t
$$

where $\mathbf{X}_t$ is a control vector (MOVE, unemployment, term spread,
credit card aggregate delinquency). Then define the **group-mean sensitivity**:

$$
\bar{\beta}^{(\mathcal{G})}_k \;=\; \frac{1}{|\mathcal{G}|}\sum_{i \in \mathcal{G}} \beta^{(i)}_k,
\quad \mathcal{G} \in \{\mathcal{T}, \mathcal{C}_1, \mathcal{C}_2\}.
$$

The **four falsifiable hypotheses** that organize the paper:

* **H1 (specificity):** $\bar\beta^{(\mathcal T)}_k > \bar\beta^{(\mathcal C_1)}_k$ for $k \in [4,8]$ weeks.
* **H2 (null on placebo):** $\bar\beta^{(\mathcal C_2)}_k \overset{p}{\to} 0$ uniformly in $k$.
* **H3 (tail amplification):** the differential $\bar\beta^{(\mathcal T)} - \bar\beta^{(\mathcal C_1)}$ is *larger* when BSI is in its upper tail (stress regime) than in its median regime.
* **H4 (extreme-event asymmetry):** tail dependence between BSI and $\Delta D^{(i)}$ is higher in $\mathcal{T}$ than in $\mathcal{C}_1$, using tail-copula and CoVaR measures defined in §3.

H3 and H4 are the v2-specific upgrades. They are the reason a reviewer will
take this paper seriously.

---

## 2. Mathematical toolkit — why each method is in the paper

### 2.1 Why Granger alone is not enough

Granger-causality tests

$$
H_0: \beta_1 = \dots = \beta_K = 0 \quad \text{in} \quad \Delta D_t = \alpha + \sum_k \beta_k \text{BSI}_{t-k} + \sum_j \phi_j \Delta D_{t-j} + \varepsilon_t
$$

are necessary but they test *predictive content in mean* under a linear-Gaussian
model. Credit stress is nonlinear and regime-dependent — the signal we care
about lives in the tails, not in the average response. Granger stays in the
paper (§6) as a sanity filter, not as the empirical centerpiece.

### 2.2 Mixed-frequency: MIDAS, because BSI is daily and 10-Qs are quarterly

Let $\text{BSI}_{t}^{(d)}$ denote the daily BSI and $D^{(i)}_{T}$ the
quarterly delinquency. Fit a MIDAS regression

$$
D^{(i)}_{T} \;=\; \alpha^{(i)} + \beta^{(i)} \sum_{j=0}^{J} w_j(\theta) \, \text{BSI}^{(d)}_{T-j} + \varepsilon^{(i)}_T
$$

with Almon-exponential weights
$w_j(\theta) = \exp(\theta_1 j + \theta_2 j^2) / \sum_{\ell} \exp(\theta_1 \ell + \theta_2 \ell^2)$.
This avoids downsampling BSI to quarterly (which would destroy the lead
signal). Use `statsmodels.midas` (Python port) or roll our own ~40 lines;
either way it is a must-have when the bank controls report quarterly.

### 2.3 Regime switching: Markov-switching VAR

Credit stress is not homoskedastic. Fit a two-state MS-VAR on
$\mathbf{y}_t = (\text{BSI}_t, \Delta D^{(i)}_t)$:

$$
\mathbf{y}_t = \boldsymbol\mu_{s_t} + \mathbf{A}_{s_t} \mathbf{y}_{t-1} + \boldsymbol\Sigma_{s_t}^{1/2} \boldsymbol\eta_t, \quad s_t \in \{1,2\}
$$

with transition matrix $P_{ij} = \Pr(s_{t+1}=j \mid s_t = i)$.
State 1 is calm; State 2 is stress. Estimate via EM / Kim filter.
Report **state-conditional $\beta$'s** — the number the paper leads with is

$$
\hat\beta_k^{(i, \text{stress})} - \hat\beta_k^{(i, \text{calm})}.
$$

H3 is tested on this object using parametric bootstrap confidence intervals
over the MS-VAR parameters.

### 2.4 Tail dependence — this is the new centerpiece

Standard Pearson/Spearman correlation is a first-moment summary. For credit
stress we care about *co-exceedance* — does the BNPL issuer blow up *when
BSI blows up*? Three complementary measures:

**(a) Upper-tail dependence coefficient.** For random variables $X$ (BSI) and $Y$ ($\Delta D^{(i)}$) with copula $C$,

$$
\lambda_U \;=\; \lim_{u \to 1^-} \Pr\!\left(Y > F_Y^{-1}(u) \;\big|\; X > F_X^{-1}(u)\right) \;=\; \lim_{u \to 1^-} \frac{1 - 2u + C(u,u)}{1-u}.
$$

Estimate non-parametrically via the **empirical tail-dependence function**
(Schmidt–Stadtmüller 2006):

$$
\hat\lambda_U(k_n) \;=\; \frac{1}{k_n} \sum_{i=1}^{n} \mathbf{1}\!\left\{R^X_i > n - k_n,\, R^Y_i > n - k_n\right\}
$$

with threshold $k_n = \lfloor n^{2/3}\rfloor$ (standard choice).
H4 is tested as $\hat\lambda_U^{(\mathcal T)} > \hat\lambda_U^{(\mathcal C_1)}$ with
block-bootstrap CI (Politis–Romano stationary bootstrap, mean block length
$= n^{1/3}$).

**(b) CoVaR.** The $q$-level Conditional VaR of $i$'s stress given BSI is in
its own $q$-tail:

$$
\text{CoVaR}^{i\mid \text{BSI}}_q \;=\; \text{VaR}_q\!\left(\Delta D^{(i)} \mid \text{BSI} \ge \text{VaR}_q(\text{BSI})\right).
$$

Estimated via quantile regression à la Adrian–Brunnermeier (2016):

$$
Q_q\!\left(\Delta D^{(i)}_t \mid \text{BSI}_t\right) \;=\; \alpha^{(i)}_q + \gamma^{(i)}_q \cdot \text{BSI}_t.
$$

Then

$$
\Delta\text{CoVaR}^{(i)}_q \;=\; \gamma^{(i)}_q \cdot \left[\text{VaR}_q(\text{BSI}) - \text{VaR}_{0.5}(\text{BSI})\right].
$$

Report the group-mean $\overline{\Delta\text{CoVaR}}$ for $\mathcal{T}$ vs
$\mathcal{C}_1$; test equality with a stationary bootstrap.

**(c) Tail-copula goodness-of-fit.** Fit a **Clayton** (lower-tail),
**Gumbel** (upper-tail), and **t-copula** to each $(X,Y)$ pair. Use AIC +
Cramér–von Mises GoF. The paper reports which copula dominates in $\mathcal T$
vs $\mathcal C_1$ — BNPL pairs should pick Gumbel (upper-tail concentration),
banks should pick Gaussian or t with low DoF.

### 2.5 Extreme value statistics for the events themselves

Identify stress events on BSI using **block-maxima GEV** and **POT**
(peaks-over-threshold) fits. For the POT model, exceedances $Y = X - u \mid X > u$
follow a Generalized Pareto:

$$
F_Y(y) = 1 - \left(1 + \xi \frac{y}{\sigma}\right)^{-1/\xi}, \quad y \ge 0.
$$

MLE for $(\xi, \sigma)$; use the mean-excess plot to pick threshold $u$.
Then return levels

$$
z_p = u + \frac{\sigma}{\xi}\left[(n \zeta_u p)^{\xi} - 1\right]
$$

define 1-in-$p$ year BSI-stress thresholds, which become the **gate levels**
for the compliance engine (replacing the ad-hoc BSI > 2σ rule in v1).
Meaningful economic content: we can now state "the pod fires when BSI crosses
its estimated 1-in-5-year stress level" rather than "when BSI > 2 std dev."

### 2.6 Change-point detection for the event chronology

To locate structural breaks in BSI and in each $D^{(i)}$, use **PELT**
(Killick–Fearnhead–Eckley 2012) with a Gaussian-mean cost function:

$$
\min_{m, \tau_{1:m}} \sum_{k=1}^{m+1} \mathcal{C}\!\left(y_{(\tau_{k-1}+1):\tau_k}\right) + \beta m
$$

with penalty $\beta = 2 \log n$ (BIC). Compare detected breakpoints in
$\mathcal T$ vs $\mathcal C_1$; a finding that BNPL breakpoints *precede* bank
breakpoints by 4–8 weeks is exactly the thesis.

### 2.7 Robustness: block bootstrap + permutation + rolling OOS

Every headline number carries three confidence intervals:

1. **Stationary block bootstrap** (Politis–Romano), mean block $n^{1/3}$, 5000 resamples.
2. **Placebo permutation test:** randomly permute group labels, recompute $\bar\beta^{(\mathcal T)} - \bar\beta^{(\mathcal C_1)}$; report exact $p$-value.
3. **Rolling 5-year OOS:** refit BSI and $\beta^{(i)}$ on $[t-5\text{y}, t]$, score on $[t, t+1\text{y}]$; report rolling hit rate, not just in-sample.

Any result that doesn't clear all three goes in the limitations section, not
the abstract.

---

## 3. BSI construction — upgrade from v1

v1's BSI was a weighted z-score. v2 replaces that with a **dynamic factor
model** estimated via Kalman smoothing. This buys us two things: (a) data-driven
weights instead of hand-tuned ones, (b) a proper covariance structure for
confidence bands on BSI itself.

Let $\mathbf{x}_t = (x_{1,t}, \dots, x_{N,t})$ be the standardized
alt-data panel (CFPB complaints, Reddit sentiment, Trends, firm-vitality,
macro). Posit a latent factor $f_t$:

$$
\mathbf{x}_t = \boldsymbol\lambda f_t + \boldsymbol\epsilon_t, \quad f_t = \phi f_{t-1} + \eta_t.
$$

Estimate $\boldsymbol\lambda$ and $\phi$ by MLE / Kalman EM. Define
$\text{BSI}_t \equiv \hat f_t$ — the smoothed posterior mean.

This is **exactly the Stock–Watson coincident-index methodology** used by the
NY Fed for their Weekly Economic Index. Citing that lineage makes the
methodology section almost self-defending.

**Ablations to report:** BSI-PCA (static), BSI-weighted-sum (v1),
BSI-dynamic-factor (v2). Paper leads with v2 and shows in an appendix that
results are robust across all three specifications — this is the standard
defense against "you tuned your signal."

---

## 4. Credit pricing — what stays, what changes

### 4.1 Jarrow–Turnbull reduced form (unchanged in spirit, tightened in spec)

Hazard intensity driven by BSI with the guardrails from v1:

$$
\lambda_t = \lambda_0 \cdot \exp\!\left(\beta_{\text{BSI}} \widetilde{\text{BSI}}_t^{\text{EWMA}} + \beta_{\text{MOVE}} \text{MOVE}_t\right),
\quad \lambda_t \in [\lambda_{\min}, \lambda_{\max}].
$$

Survival probability $S(t) = \exp(-\int_0^t \lambda_u \, du)$. Price of a
junior tranche with recovery $R$:

$$
P_0 = \mathbb{E}^{\mathbb Q}\!\left[\int_0^T e^{-rs} C(s) S(s) \, ds + R \int_0^T e^{-rs} \, dS(s)\right].
$$

**v2 addition:** the $\beta_{\text{BSI}}$ parameter is **group-calibrated**
separately for $\mathcal T$ and $\mathcal C_1$, and the paper reports the
*ratio* $\beta^{(\mathcal T)}_{\text{BSI}} / \beta^{(\mathcal C_1)}_{\text{BSI}}$
as the structural-sensitivity differential. This is the quantity H1 is
measured against.

### 4.2 Heston SCP — unchanged (equity-layer only, never ABS)

Stays as-is. Documented extensively in v1.

### 4.3 Squeeze Defense — unchanged in spec, tightened in threshold calibration

The OTM%, utilization, and IV-skew thresholds are now calibrated against the
GEV fits in §2.5 rather than hand-picked.

---

## 5. Trade-gate logic — reconsidered

v1 required **all four gates** (BSI stress ∧ SCP ∧ MOVE>120 ∧ CCD II proximity).
v2 generalizes to a **calibrated score**:

$$
\text{Gate}_t = \sigma\!\left(w_1 z^{\text{BSI}}_t + w_2 z^{\text{SCP}}_t + w_3 z^{\text{MOVE}}_t + w_4 z^{\text{CCD}}_t - \tau\right)
$$

with $\sigma$ the logistic function, weights $w_j$ fit on labelled historical
stress episodes (GFC-adjacent, 2022 Klarna event, 2024 CFPB rulings), and
threshold $\tau$ calibrated to a target false-positive rate (1 per 3 years).
Trades fire when $\text{Gate}_t > 0.5$.

The deterministic compliance engine still enforces hard floors — no trade
without positive BSI differential vs. $\mathcal{C}_1$, no trade with MOVE
below its 90th percentile, no trade if squeeze risk breaches precomputed
GEV return levels. These are *binding vetoes* even when the logistic score
is high. This preserves the v1 invariant: **LLMs advise, compliance
decides.**

---

## 6. Data panel — full specification

### 6.1 Firm panel

| Group | Tickers / entities | Delinquency source | Equity source |
|---|---|---|---|
| $\mathcal T$ (BNPL) | AFRM, SQ/AFTPY, PYPL, SEZL, ZIP, UPST (sleeve) | AFRMMT 10-D, ABS-15G | yfinance |
| $\mathcal C_1$ (near-prime) | COF, SYF, DFS, AXP, OMF, SOFI, LC | 10-Q NCO / 30+ DPD | yfinance |
| $\mathcal C_2$ (placebo) | V, MA, JPM, BRK-B | 10-Q | yfinance |
| $\mathcal C_3$ (subprime auto) | CACC, ALLY, SC/SDART, AMCAR, BCRST, EART | 10-Q + auto-ABS trustee reports | yfinance (equity names only) |

### 6.2 Alt-data sources feeding BSI

* FRED — 9 macro series (already implemented)
* SEC EDGAR — 10-D, 10-Q, ABS-15G (next module)
* CFPB — complaint narratives + metadata (public API)
* Reddit — r/povertyfinance, r/Debt, r/personalfinance (pending approval; Academic Torrents backfill meanwhile)
* Google Trends — "BNPL", "Klarna", "pay in 4", "can't pay" (pytrends)
* Firm vitality — Wayback Machine pulls of LinkedIn company pages + X profiles, monthly resolution
* Options / short interest — yfinance + FINRA

### 6.3 Frequency alignment

BSI is computed daily. Delinquency targets are at native frequency — weekly
(ABS trustee reports), quarterly (10-Q). MIDAS (§2.2) handles the mixed
frequency. No downsampling of BSI.

---

## 7. Pod architecture — essentially unchanged

LangGraph state machine, NIM Nemotron-3-super-120b (heavy tier, thinking mode)
+ Gemini fallback, deterministic `compliance_engine.py` as sole approval
authority. See v1 §4 for full architecture diagram. v2 changes are in the
*analysis* layer (§3–6 above), not the orchestration layer.

---

## 8. Updated playbook

v1 had an 8-sprint cadence (A–H). v2 reorganizes around *deliverable
waypoints* instead of sprints, because the user's priority is research depth,
not ship-date.

### Waypoint 1 — Clean data backbone (2–3 weeks depending on approvals)

* FRED (✅ done)
* SEC EDGAR filings index + 10-D / 10-Q / ABS-15G parsers (next)
* CFPB complaints (public API, no key)
* Reddit — Academic Torrents backfill now; PRAW incremental once approved
* Google Trends, yfinance options, FINRA short interest
* Firm vitality (Wayback Machine pulls for LinkedIn + X)
* `config/panel.yaml` with all 3 groups × tickers × CIKs

**Exit criterion:** `python -m data.warehouse_report` outputs non-empty panels
for all 18 firms, all 9 alt-data sources, at the documented frequencies.

### Waypoint 2 — Signal construction + factor model

* FinBERT pipeline for Reddit + CFPB narratives
* BSI via dynamic factor model (§3) with three ablations reported
* Change-point detection (§2.6) → "event calendar" table for the paper
* GEV / POT fits on BSI → return-level thresholds

**Exit criterion:** `bsi_daily.csv` with three BSI variants; diagnostic plots
for each saved into `paper/figures/`.

### Waypoint 3 — The empirical centerpiece

* Bivariate Granger (sanity filter)
* MIDAS regressions for quarterly-reporting controls
* Markov-switching VAR with state-conditional $\beta$'s
* Tail-dependence estimates, CoVaR, copula fits
* Bootstrap + permutation + rolling OOS for every headline number

**Exit criterion:** a single notebook
`notebooks/empirical_centerpiece.ipynb` that produces Table 1 (group
$\bar\beta$'s), Table 2 (tail metrics), Table 3 (robustness), and Figures 3–6
of the paper. All numbers reproducible from warehouse.

### Waypoint 4 — Credit pricing + portfolio

* JT with group-calibrated $\lambda$ dynamics
* Heston SCP for the equity layer
* TRS pricer (short junior tranche vs. float)
* Mean-CVaR optimization over the $\mathcal T$ panel
* Simulated P&L over 2019–2025 with event-study overlays

**Exit criterion:** `pnl.csv` + event-study figures. Pod produces a deterministic
`trade_signal.json` for the current date.

### Waypoint 5 — Agentic pod

* LangGraph wiring with Macro / Quant / Risk agents
* Compliance engine as sole gate
* Full audit trail in `logs/agent_decisions/`
* Dashboard — Streamlit first, Next.js if time allows

**Exit criterion:** `make pod` runs end-to-end; dashboard renders four-gate
state + agent reasoning + compliance decision for any date.

### Waypoint 6 — Paper

* 30–35 pages. Structure:
  1. Abstract
  2. Introduction (Micro-Leverage Epoch framing)
  3. Literature (Jarrow-Turnbull, Merton, Duffie-Singleton, Adrian-Brunnermeier, Di Maggio, Tetlock, Khalil, CFPB reports)
  4. Five-tier BNPL framework
  5. Data + BSI construction (incl. dynamic factor model)
  6. **Empirical identification** (H1–H4 with every test from §2)
  7. Credit pricing (JT reduced-form, group-calibrated)
  8. Execution framework (why TRS on junior tranches, not equity shorts)
  9. Empirical results + event studies + simulated P&L
  10. Pod architecture
  11. Limitations (data latency, regime non-stationarity, LinkedIn/X ToS, OTC illiquidity)
  12. Conclusion
  13. References
  14. Appendix A — reproducibility (repo walkthrough)
  15. Appendix B — all robustness tables

**Exit criterion:** `make paper` compiles `paper.pdf` with all figures and
citations auto-rebuilt from the warehouse.

---

## 9. What's explicitly out of scope

* Live execution. No ISDA, no broker, no real money.
* Self-hosted LLMs. NIM Nemotron + Gemini via hosted endpoints only.
* Individual LinkedIn profile scraping. Company pages only, via Wayback.
* Any claim that this is a deployable institutional strategy. It is a
  research prototype with engineering rigor, not a fund.

---

## 10. The honest risk register

| Risk | Impact | Mitigation |
|---|---|---|
| Reddit approval doesn't land in time | High | Academic Torrents backfill; document the provenance in §5 |
| AFRMMT 10-D data sparse before 2021 | Medium | Use ABS-15G as secondary; weight pre-2021 observations lower in MIDAS |
| MS-VAR fails to separate regimes cleanly | Medium | Fall back to threshold VAR with BSI's 90th-percentile as the split |
| Tail-dependence estimates too noisy on short panel | Medium | Pool across firms within group; bootstrap block length tuning |
| $\beta^{(\mathcal T)} \approx \beta^{(\mathcal C_1)}$ — thesis fails | Existential | Paper still publishable as a **null result with novel methodology**; frame as "BNPL stress signal is not distinguishable from general consumer credit stress at current data resolution" and specify what data would resolve it |

Item 5 is the one that matters. Research papers live or die on whether the
author is willing to write the "thesis fails" version honestly. v2's design
means the null-result version is still a contribution (the method, the panel,
the alt-data integration) — so we can afford to run the test.

---

## 11. Frozen decisions (locked 2026-04-18)

1. **Panel membership** — 24 entities: $\mathcal T$ (6 BNPL) + $\mathcal C_1$ (7 near-prime) + $\mathcal C_2$ (4 placebo) + $\mathcal C_3$ (6 subprime auto). Full list in `config/panel.yaml`.
2. **Backtest window** — **2019-01-01 → today.** Captures the BNPL rise (2019–2021), COVID stimulus distortion (2020–2021), 2022 Klarna down-round, 2023 rate-shock stress, 2024 CFPB rulings. Subprime-auto comparisons use the same window; the group's separate 2023–2024 stress wave is the in-window identification event.
3. **Paper length** — 30–35 pages main text + unlimited appendix. Appendix B carries all robustness tables (bootstrap, permutation, rolling OOS).
4. **Dashboard** — **both.** Streamlit built first (Waypoint 5) for demo-at-grader-time. Next.js port follows (Waypoint 7, post-paper) as the portfolio-grade artifact. Both read from the same read-only FastAPI adapter over DuckDB so there is no drift.

These are now baseline. Any change requires a new version bump (v3).
