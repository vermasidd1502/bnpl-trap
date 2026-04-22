# MASTERPLAN v4 — BNPL Trap

**Author:** Siddharth Verma (UIUC, *Quantamental Investment*, Spring 2026)
**Date:** 2026-04-18
**Status:** **LOCKED_V4.1 2026-04-18.** Third institutional review added
**four operational reinforcements** on top of the v4 baseline, which are
integrated into this revision (no change to the core two-factor JT or
empirical identification):

* **(i)** MIDAS-aligned accrual-lag correction inside the SBG mapping
  from $\lambda_{\text{unsys}}$ → reported delinquency (§5.5).
* **(ii)** Wayback-snapshot **staleness penalty** — exponential weight
  decay on firm-vitality features older than 30 days (§6.1).
* **(iii)** 3-day moving average on bucket-(a) Trends before the Leg-A
  sizing gate fires, to suppress one-day marketing spikes (§5.4).
* **(iv)** Dashboard **Dual-View SBG**: real-time anomaly gauge
  (primary) + historical heatmap (diagnostics), with credibility-
  weighted Red/Yellow alert tiers driven by bot-filter + co-occurrence
  signals (§7 #10). The stress-test heatmap additionally overlays a
  **Sharpe = 0 survival contour** to visualize the transport-assumption
  safety margin (§7 #9).

Promotes `LOCKED_V3_FINAL` to the coding baseline with no technical deltas
to the core model — the second institutional
review confirmed all six v3-final reinforcements (duration scaler,
joint $3\times3$ sensitivity grid, Bucket-A sizing gate, cross-platform
co-occurrence gate, Tenure Slope, Shadow-Bureau Gap) and both defaults
(2008 auto-ABS calibration ON, BotBust-2023 bot-filter training set).
v4 is the name the codebase now references; the mathematical content is
identical to v3-final. Supersedes `MASTERPLAN_V2.md`, interim `v3.1`,
and `LOCKED_V3_FINAL`. v2 remains the reference for
the DiD identification strategy (§1–3, §6–10), unchanged in v3.
Defaults confirmed: 2008 auto-ABS calibration **ON**, bot-filter uses
public **BotBust 2023** with hand-labeled top-up at refinement.

**Six institutional-review reinforcements** are integrated into this
`_FINAL` revision:

1. Duration scaler $\phi_\theta, \phi_\kappa$ on $\theta_{\text{sys}}^{\text{bad}}$ / $\kappa_{\text{sys}}$ (§4.3)
2. **Joint $3\times3$ sensitivity grid** on $(\phi_\theta, \phi_\kappa)$ replacing independent $\pm 25\%$ sweeps (§4.3, v3-final)
3. Bucket-A sizing constraint on Leg A (§5.4)
4. Cross-platform co-occurrence gate on jumps (§2.4)
5. Tenure-Slope sub-signal in firm vitality (§6.1)
6. Shadow-Bureau-Gap cross-sectional diagnostic (§5.5)

Dashboard requirement added: **2D $(\phi_\theta, \phi_\kappa)$ heatmap** of
headline P&L for the stress-test view (§7 / Waypoint 8).

---

## 0. Scope of v3 vs. v2

v2 locked the empirical design: treated-vs-control difference-in-differences
across four issuer groups ($\mathcal T$ BNPL, $\mathcal C_1$ near-prime,
$\mathcal C_2$ placebo, $\mathcal C_3$ subprime-auto historical analog), with
the full mathematical toolkit (dynamic factor BSI, MS-VAR, MIDAS, tail
dependence, CoVaR, copulas, GEV/POT, PELT, bootstrap/permutation/rolling OOS).

v3 does **three things** on top of that, in response to the institutional
review:

1. **Rewrites the JT hazard spec** to make systematic vs. unsystematic
   decomposition explicit and mathematically clean. This is the change that
   makes the thesis pitchable as **market-neutral alpha** rather than a
   sector-short.
2. **Promotes Google Trends to its own signal layer** separate from
   social-post sentiment, on the grounds that search is *pre-intent* while
   posts are *post-event* — they live at different points in the default
   timeline and deserve separate mathematical treatment.
3. **Adds a crisis-regime calibration step** using 2008 subprime-auto ABS
   performance as the "bad regime" training data, since BNPL has no
   in-sample recession.

Everything in v2 §1–3 (hypotheses H1–H4, dynamic factor BSI, tail metrics,
change-point detection, robustness battery) carries over unchanged.

---

## 1. The systematic / unsystematic hazard decomposition

This is the v3 core. It replaces v2 §4.1 entirely.

### 1.1 Why this matters for the pitch, not just the math

A single-intensity JT model of BNPL issuer $i$ with $\lambda_i(t)$ driven by
"some mix of macro and sentiment" cannot distinguish two economically
different stories:

* **Story A (beta):** "Everything consumer-credit is stressed — BNPL included." Tradable by anyone with an S&P short.
* **Story B (alpha):** "BNPL is stressed *beyond* what macro explains, driven by firm-specific debt-stacking invisible to traditional bureaus." Tradable only with the alt-data signal this paper builds.

The decomposition below makes story B a *quantitative* statement — the
unsystematic intensity $\lambda_{\text{unsys},i}(t)$ is nonzero after
$\Lambda_{\text{sys}}(t)$ has been regressed out. That residual is the
structural-complexity premium the hedge-fund pitch rests on.

### 1.2 The two-factor CIR specification

For issuer $i \in \mathcal T \cup \mathcal C_1 \cup \mathcal C_3$, write:

$$
\lambda_i(t) \;=\; \Lambda_{\text{sys}}(t) \;+\; \lambda_{\text{unsys},i}(t).
$$

**Systematic component** — single economy-wide factor, Cox–Ingersoll–Ross
dynamics, driven by MOVE and term-spread:

$$
d\Lambda_{\text{sys}}(t) \;=\; \kappa_{\text{sys}}\!\left(\theta_{\text{sys}}(M_t) - \Lambda_{\text{sys}}(t)\right) dt \;+\; \sigma_{\text{sys}} \sqrt{\Lambda_{\text{sys}}(t)} \, dW^{\text{sys}}_t,
$$

where the long-run level is a calibrated function of observable macro state:

$$
\theta_{\text{sys}}(M_t) \;=\; \theta_0 + \theta_1 \cdot \mathbf{1}\!\{\text{MOVE}_t > 120\} + \theta_2 \cdot \max(0,\, -\text{T10Y3M}_t).
$$

The indicator on MOVE is the same gate used elsewhere in the pod. The curve-
inversion penalty compounds it. CIR guarantees $\Lambda_{\text{sys}}(t) \ge 0$
almost surely as long as $2\kappa_{\text{sys}}\theta_{\text{sys}} \ge \sigma_{\text{sys}}^2$
(the **Feller condition**), which is enforced as a hard constraint during MLE.

**Unsystematic component** — per-issuer CIR with an additive jump driven by
the alt-data signal:

$$
d\lambda_{\text{unsys},i}(t) \;=\; \kappa_i\!\left(\theta_i - \lambda_{\text{unsys},i}(t)\right) dt \;+\; \sigma_i \sqrt{\lambda_{\text{unsys},i}(t)} \, dW^{(i)}_t \;+\; J_i(t)\, dN_i(t).
$$

Jump intensity $N_i(t)$ is a Poisson counter fired when the firm-specific BSI
shock (see §2.3 below) exceeds a threshold; jump size $J_i(t)$ is capped:

$$
|J_i(t)| \;\le\; J_{\max} \;=\; 0.05 \,\lambda_{\text{unsys},i}(t^-).
$$

This is the hard boundary the institutional review requires. A viral Reddit
meme can at most move $\lambda$ by 5% per firing event, preventing the
single-tweet hallucinated-crisis failure mode.

**Independence and correlation.** By construction,
$\mathrm{Cov}(W^{\text{sys}}_t, W^{(i)}_t) = 0$ for all $i$. Any empirical
co-movement between macro and firm-specific innovations is absorbed into
$\Lambda_{\text{sys}}$ during calibration. This is what makes $\lambda_{\text{unsys},i}$
identifiable as **residual** risk.

### 1.3 Estimation procedure

Two-stage MLE, done once per group:

**Stage 1.** Estimate $(\kappa_{\text{sys}}, \theta_0, \theta_1, \theta_2, \sigma_{\text{sys}})$
by fitting the CIR process to a **macro hazard proxy**: the cross-sectional
median of observed delinquency innovations across $\mathcal C_1 \cup \mathcal C_2$
(the non-BNPL groups). This anchors $\Lambda_{\text{sys}}$ on entities whose
hazard is *only* macro-driven, uncontaminated by BNPL-specific distress.

**Stage 2.** Fix $\Lambda_{\text{sys}}(t)$ from Stage 1. For each issuer $i$,
estimate $(\kappa_i, \theta_i, \sigma_i)$ and the jump parameters
$(J_i, \text{threshold}_i)$ by MLE on the residual
$\lambda_i(t) - \Lambda_{\text{sys}}(t)$ extracted from observed delinquency
innovations.

This is essentially Duffie–Singleton (1999) two-factor intensity estimation
with the residual step made explicit.

### 1.4 Survival probability and tranche pricing

Given the decomposition, issuer $i$ survival to time $T$ is:

$$
S_i(T) \;=\; \mathbb{E}^{\mathbb Q}\!\left[\exp\!\left(-\int_0^T \Lambda_{\text{sys}}(u)\, du - \int_0^T \lambda_{\text{unsys},i}(u)\, du\right)\right].
$$

Because the two Brownian drivers are independent, this factorizes:

$$
S_i(T) \;=\; \mathbb{E}^{\mathbb Q}\!\left[e^{-\int_0^T \Lambda_{\text{sys}}(u) du}\right] \cdot \mathbb{E}^{\mathbb Q}\!\left[e^{-\int_0^T \lambda_{\text{unsys},i}(u) du}\right] \;=\; S_{\text{sys}}(T) \cdot S_{\text{unsys},i}(T).
$$

The junior-tranche price expression in v2 §4.1 is unchanged in form; it now
simply consumes the product $S_{\text{sys}}(T) \cdot S_{\text{unsys},i}(T)$.

---

## 2. Google Trends as a standalone signal layer

v2 folded Google Trends into a single "alt-data panel" that went into the
dynamic factor model. v3 promotes it to its own layer with distinct math.

### 2.1 Why separate it out

Search volume and social posts encode *different stages* of the same consumer
distress process:

| Signal | Temporal position | Information content |
|---|---|---|
| Google Trends (search-volume) | $t_0$: *pre-intent* | Latent distress — user is "looking for answers" (e.g., "how to skip a Klarna payment") but has not yet acted or posted |
| Reddit / social posts | $t_0 + \delta_1$: *post-event* | User has acted / defaulted / been collected on, and is now seeking help or venting |
| CFPB complaints | $t_0 + \delta_2$: *post-grievance* | Formal complaint to the regulator |
| 10-D / 10-Q delinquency | $t_0 + \delta_3$: *reported* | Bureau-visible outcome |

Collapsing Trends into the same latent factor as Reddit forces the
dynamic-factor model to average over these lags, which destroys the leading
property of search.

### 2.2 The three-query taxonomy

Group Trends queries into three semantic buckets, each with a different
economic interpretation:

**(a) Product-interest queries** — proxy for top-of-funnel demand.
Examples: `"Klarna"`, `"Affirm"`, `"Afterpay"`, `"pay in 4"`, `"BNPL"`.
**Interpretation:** rising = firm is gaining market share. Ambiguous credit
signal — more users can mean more risk build-up *or* a healthier book.

**(b) Friction queries** — proxy for mid-funnel stress.
Examples: `"Klarna late fee"`, `"Affirm dispute"`, `"Afterpay declined"`,
`"can't pay Klarna"`.
**Interpretation:** rising = existing users are hitting problems. Direct
leading indicator for delinquency.

**(c) Exit queries** — proxy for acute distress.
Examples: `"Klarna collections"`, `"how to remove Affirm from credit
report"`, `"BNPL lawsuit"`, `"debt consolidation BNPL"`.
**Interpretation:** rising = users in or near default. This is the layer
that *should* lead 10-D reports.

The paper reports a separate panel and separate coefficients for each
bucket. The headline result is expected to be that bucket (c) loads on
BNPL-issuer delinquency at 4–6 week leads with $p < 0.01$, while bucket (a)
does not — exactly the pattern that distinguishes signal from noise.

### 2.3 Mathematical treatment

Let $g^{(b)}_t$ be the z-scored normalized Trends index for bucket
$b \in \{\text{a},\text{b},\text{c}\}$ at week $t$. The BSI v3 composite
becomes:

$$
\text{BSI}_t \;=\; \underbrace{\hat f_t}_{\text{dynamic factor (v2)}} \;+\; \underbrace{\sum_{b \in \{\text{b},\text{c}\}} \omega_b \cdot g^{(b)}_t}_{\text{Trends overlay (v3)}},
$$

where $\omega_b$ are small positive weights (0.05–0.15 range, calibrated by
minimizing out-of-sample prediction error on the treated panel), and bucket
(a) is excluded from the overlay because its sign is ambiguous. Bucket (a)
still appears *inside* the dynamic factor (as a demand proxy) but not as a
direct stress contributor.

This is the **"layer"** the institutional review called for: Trends sits in
parallel with Reddit sentiment, not nested inside it. Ablations reported in
the appendix: BSI without any Trends, BSI with Trends in the factor only,
BSI with Trends as overlay (v3 main spec).

### 2.4 Jump trigger for the unsystematic hazard

The Trends bucket-(c) z-score is also what triggers the $N_i(t)$ jump
counter in §1.2. Concretely, a jump fires at time $t$ for issuer $i$ when:

$$
g^{(c),i}_t \;>\; q_{0.95}\!\left(\{g^{(c),i}_s\}_{s \le t}\right) \quad \text{AND} \quad \Delta g^{(c),i}_t \;>\; 0,
$$

with jump size

$$
J_i(t) \;=\; \min\!\left(J_{\max},\; \alpha \cdot \tanh\!\left(g^{(c),i}_t - q_{0.95}\right)\right),
$$

where $\alpha$ is a scale parameter and $q_{0.95}$ is the rolling 95th
percentile of that issuer's bucket-(c) series. The $\tanh$ saturates the
jump magnitude smoothly; combined with the hard cap $J_{\max}$ it gives
two-layered boundedness (soft + hard), which the institutional review
specifically asked for.

**Cross-platform co-occurrence gate** (v3.1 reinforcement). A single-channel
spike is insufficient for a large jump. Define the Reddit stress indicator
$r^{(i)}_t$ as the z-scored firm-specific BSI residual, and the Trends
bucket-(c) indicator $g^{(c),i}_t$ as in §2.3. Compute the co-occurrence
flag

$$
C^{(i)}_t \;=\; \mathbf{1}\!\left\{r^{(i)}_t > q_{0.80}(\{r^{(i)}_s\})\right\} \cdot \mathbf{1}\!\left\{g^{(c),i}_t > q_{0.80}(\{g^{(c),i}_s\})\right\}.
$$

The jump-size formula is then gated: if $C^{(i)}_t = 0$, the realized jump
is clipped at the **median** of its historical magnitude; only when both
channels fire in the same window is the full $\tanh$-scaled magnitude
(up to $J_{\max}$) permitted. This prevents a Reddit-only raid or a
Trends-only news artifact from driving a large $\lambda$ revision, and
operationalizes the institutional-review principle that sentiment and
search must *corroborate* for a signal to count.

---

## 3. The bot / credibility filter — making the jump trigger robust

Raw Reddit posts and raw Trends queries are both manipulable. §1.2's jump
cap prevents catastrophic damage, but the pod should also actively filter
low-credibility signal *before* it reaches the model.

### 3.1 Reddit credibility scoring

Per-author weight:

$$
w_a \;=\; \frac{1}{1 + \exp\!\left(-\left(\beta_1 \log(1+\text{karma}_a) + \beta_2 \log(1+\text{account\_age\_days}_a) + \beta_3 \cdot \mathbf{1}\{\text{verified\_email}_a\}\right) + \beta_0\right)}.
$$

Posts from authors with $w_a < 0.1$ are dropped; posts with $w_a \in [0.1,
0.5]$ are down-weighted proportionally in the sentiment aggregation. The
$\beta$'s are calibrated against a labeled subset of known-bot vs.
known-human accounts (~1000 each, hand-labeled by us).

Additionally: an **exact-phrase clustering filter** — if more than 20% of
posts in a rolling 24-hour window share the same 5-gram, suspect a
coordinated campaign and zero-weight that window's sentiment contribution.

### 3.2 Trends anomaly filter

Trends data has its own manipulation vectors (viral memes, news-cycle
artifacts). Filter:

* **Velocity cap:** any single-day increase beyond 10 standard deviations of
  the 90-day rolling distribution is truncated at the cap.
* **Cross-query consistency check:** a legitimate BNPL-stress signal should
  show correlated movement across the 3–5 queries in the same bucket. If
  only one query in bucket (c) spikes, it's likely a news artifact; zero-
  weight that day for that bucket.

### 3.3 Kalman smoothing across the aggregate BSI

After the dynamic factor + Trends overlay is combined, pass the resulting
daily BSI through a Kalman smoother with a local-level state-space model:

$$
\text{BSI}^{\text{raw}}_t = \mu_t + v_t, \qquad \mu_t = \mu_{t-1} + w_t,
$$

with $v_t \sim \mathcal N(0, \sigma_v^2)$, $w_t \sim \mathcal N(0, \sigma_w^2)$,
and $\sigma_v^2 / \sigma_w^2$ estimated via MLE. The smoothed $\hat\mu_t$
is the BSI that downstream models consume.

This is the filter step the institutional review called for. It's cheap to
run (seconds for the full history) and it measurably stabilizes the JT
calibration.

---

## 4. 2008 subprime-auto calibration — the crisis regime

BNPL has no recession in its own history. $\mathcal T$ alone cannot inform
what a "bad regime" $\theta_{\text{sys}}$ looks like. v3 borrows the 2008
subprime-auto experience.

### 4.1 The auxiliary calibration dataset

Pull **2005–2010 ABS trustee reports** for the pre-crisis subprime-auto
trust families: AMCAR, BCRST-predecessors, SDART-predecessors. Roll rates,
60+ DPD, CNL, excess spread — same four series we already parse for
post-2019 data.

The 2008–2009 stress period is the target. Specifically, the months around
the Lehman collapse (Sep 2008 – Mar 2009) are a labeled "bad regime"
training window.

### 4.2 Regime-conditional $\theta_{\text{sys}}$ calibration

Fit CIR $\Lambda_{\text{sys}}(t)$ on 2005–2018 auto-ABS data, then take the
*state-2 mean* from a Markov-switching version of the same model:

$$
\theta_{\text{sys}}^{\text{bad}} \;=\; \mathbb{E}\!\left[\Lambda_{\text{sys}}(t) \mid s_t = 2\right],
$$

where state 2 is identified as the high-hazard regime (2008–2009 months). This
$\theta_{\text{sys}}^{\text{bad}}$ becomes the **scenario parameter** for
BNPL stress simulation — the number we plug into §1.2's mean-reversion when
asking "what happens to the AFRMMT junior tranche in a 2008-like regime?"

### 4.3 Transport validity — the assumption that makes this work

The key assumption: **macroeconomic pass-through from unemployment and
funding stress into consumer-lender hazard is structurally similar across
subprime auto and BNPL.** This is defensible because both products target
the same thin-file, liquidity-constrained consumer, both are heavily
securitized, both have short duration, and both are acutely sensitive to
funding-market conditions.

It is *not* defensible to claim the unsystematic dynamics transport — BNPL's
debt-stacking is structurally different from auto-collateral recovery. The
transport assumption is explicitly limited to $\Lambda_{\text{sys}}$. The
unsystematic layer remains BNPL-calibrated.

The paper names this assumption in §11 (limitations) and runs a sensitivity
check: shift $\theta_{\text{sys}}^{\text{bad}}$ by $\pm 25\%$ and report how
the headline P&L changes. If results survive the shift, the transport
assumption isn't load-bearing.

**Duration scaler** (v3.1 reinforcement). BNPL and auto-ABS have materially
different product durations: BNPL pay-in-4 is ~6-week weighted-average life,
subprime auto is 3–5 years. Hazard dynamics *scale with duration*: shorter
products transmit stress into realized losses faster and mean-revert faster.
To translate a 2008-auto $\theta$ into a BNPL-relevant one, apply a scaler
on both the long-run mean and the mean-reversion speed:

$$
\theta_{\text{sys}}^{\text{bad, BNPL}} \;=\; \phi_\theta \cdot \theta_{\text{sys}}^{\text{bad, auto}}, \qquad \kappa_{\text{sys}}^{\text{BNPL}} \;=\; \phi_\kappa \cdot \kappa_{\text{sys}}^{\text{auto}},
$$

with $\phi_\theta \in [1.3, 1.6]$ (BNPL is unsecured; recovery ≈ 0 vs.
auto repossession recovery of 40–60%, so peak hazard is higher) and
$\phi_\kappa \in [6, 10]$ (shorter duration → faster decay back to
long-run mean). Default point estimates: $\phi_\theta = 1.5$,
$\phi_\kappa = 8$.

**Joint $3\times3$ sensitivity grid (v3-final reinforcement).** Independent
$\pm25\%$ sweeps on $\phi_\theta$ and $\phi_\kappa$ understate the combined
risk: a crisis that is simultaneously *more severe* (high $\phi_\theta$)
*and more persistent* (low $\phi_\kappa$, slower mean-reversion) is the
genuine tail scenario. Replace the independent sweep with a joint grid:

$$
(\phi_\theta, \phi_\kappa) \in \{1.2, 1.5, 1.8\} \times \{5, 8, 11\}
$$

Evaluate the full two-factor model and the hedged P&L at each of the 9
nodes. Report:

* **Point estimate** at $(\phi_\theta, \phi_\kappa) = (1.5, 8)$ — headline.
* **Corner cases** — severity × persistence interaction. The $(1.8, 5)$
  corner is the "crunch-and-stay" scenario (high peak hazard + slow decay);
  the $(1.2, 11)$ corner is the "benign flash" (mild peak + fast decay).
* **2D heatmap** of headline net P&L over the grid — this is the figure
  that renders in the Streamlit dashboard stress-test panel (§7).

The scenario surface is now fully bracketed in *joint* parameter space.
Conclusions that survive the worst grid node (typically $(1.8, 5)$)
are reportable as robust; conclusions that only hold at the point
estimate are disclosed as such. The paper discloses these scalers as
*structural* assumptions — defensible but not estimated from BNPL data —
and the grid is what makes the conclusion robust rather than the point
estimate alone.

---

## 5. The market-neutral hedge — turning the trade into alpha

The institutional review's single best insight: *"If you only short when
$\Lambda_{\text{sys}}$ is high, you're trading a recession. The alpha lives
in $\lambda_{\text{unsys}}$."*

### 5.1 The structured expression

**Leg A (short):** TRS on the junior tranche of a live Affirm securitization
(AFRMMT series). Pod pays the total return, receives SOFR + spread. This is
the expression of the thesis.

**Leg B (long):** Long exposure to a matched-duration basket of
$\mathcal C_1$ bank credit — e.g., a position in the LQD ETF or direct CDS
protection *sold* on COF/SYF (short protection = long credit). This leg
neutralizes the $\Lambda_{\text{sys}}$ exposure.

**Leg C (optional, size-dependent):** Short Treasury futures (ZN, ZB) to
hedge the pure rate component — because the TRS receives floating, the rate
exposure on Leg A is already small, so Leg C is typically a rounding-level
adjustment.

### 5.2 P&L decomposition under the intensity model

Under the two-factor intensity specification:

$$
\underbrace{\Delta P\&L_{\text{total}}}_{\text{observed}} \;\approx\; \underbrace{w_A \cdot \Delta S_{\text{unsys},\text{AFRM}}}_{\text{alpha}} \;+\; \underbrace{\left(w_A - w_B\right) \cdot \Delta S_{\text{sys}}}_{\text{residual beta, target} \approx 0} \;+\; \text{basis noise}.
$$

Leg sizing solves $w_A = w_B$ (duration- and DV01-matched) so the middle
term vanishes by construction. The realized P&L is then proportional to
$\Delta S_{\text{unsys},\text{AFRM}}$ — the thing the paper's alt-data
signal is built to predict.

### 5.3 Why this matters for the 580 submission

The paper reports **two P&L figures**:

1. Naive TRS short on AFRMMT junior, unhedged.
2. Market-neutral version with Leg B sized from the two-factor model.

The second version should have substantially lower macro beta (reportable
as the regression coefficient of daily P&L on MOVE + S&P returns) and
comparable or higher Sharpe. That Sharpe-ratio-per-unit-of-beta number is
the slide that makes this pitchable to a Chief Risk Officer. It's also the
number that distinguishes this paper from "another credit-short white
paper."

### 5.4 Bucket-A sizing constraint (growth-story defense)

The two-factor hedge neutralizes macro beta but **not** equity-narrative
beta. If a BNPL issuer's Trends bucket-(a) — product-interest queries —
is spiking, the market is pricing a growth story, and the junior-tranche
TRS can widen more slowly than the issuer's equity rallies. A short
sized against hazard alone gets steamrolled.

Let $g^{(a),i}_t$ be issuer $i$'s bucket-(a) z-score and
$q^{(a)}_{0.90}$ its rolling 90th percentile. Apply a multiplicative
down-sizer on Leg A notional:

$$
w_A(t) \;=\; w_A^{\text{target}} \cdot \left(1 - \gamma \cdot \max\!\left(0,\; g^{(a),i}_t - q^{(a)}_{0.90}\right)\right)_+,
$$

with $\gamma = 0.4$ (calibration default). Interpretation: when product
interest is in its top decile, cut the short by up to ~40%, scaled by
how far above threshold the query volume sits. When $g^{(a)}$ is at or
below its 90th percentile, the constraint is slack and full target
notional is deployed. $(\cdot)_+$ floors the sizer at zero — a viral
moment can fully disarm Leg A but never flip it long.

**Smoothing (v4.1 reinforcement).** To prevent one-day marketing spikes
(Super Bowl ads, single viral influencer post, promotional campaign
launches) from prematurely disarming the short, the gate consumes a
**3-day simple moving average** of the raw z-score:

$$
\tilde g^{(a),i}_t \;=\; \frac{1}{3}\sum_{k=0}^{2} g^{(a),i}_{t-k},
$$

and the sizing formula uses $\tilde g^{(a),i}_t$ in place of
$g^{(a),i}_t$. Three trading days is the empirically observed half-life
of single-event search spikes; sustained viral interest (the case we
actually want to down-size against) persists well beyond that window.

This is cheap insurance against the "Klarna IPO pop" failure mode where
credit stress and narrative euphoria coexist (a pattern documented in
2021 SPAC-era fintech).

### 5.5 Shadow-Bureau-Gap diagnostic

The thesis claim is that BNPL carries debt-stacking invisible to
traditional bureaus. This produces a testable cross-sectional
implication: the gap between **implied delinquency from alt-data** and
**reported delinquency from filings/bureau sources** should be
*materially larger* for $\mathcal T$ (BNPL) than for $\mathcal C_1$
(near-prime). Call this the **Shadow-Bureau Gap** (SBG).

Define, for each issuer $i$ and month $m$:

$$
\text{SBG}_{i,m} \;=\; \hat d^{\text{alt}}_{i,m} \;-\; d^{\text{reported}}_{i,m},
$$

where $\hat d^{\text{alt}}_{i,m}$ is the delinquency rate implied by
mapping $\lambda_{\text{unsys},i}(t)$ through the survival function to a
30-day-equivalent rate, and $d^{\text{reported}}_{i,m}$ is the actual
30-/60-day delinquency reported in the issuer's filings (10-D for trusts,
10-Q for issuers, FRED DRCCLACBS for the banking aggregate).

The headline hypothesis is:

$$
\mathbb{E}[\text{SBG}_{i,m} \mid i \in \mathcal T] \;>\; \mathbb{E}[\text{SBG}_{i,m} \mid i \in \mathcal C_1],
$$

tested via a group-means regression with clustered standard errors.
A significant positive gap-differential is *direct* evidence for the
"debt-stacking-invisible-to-bureaus" mechanism — i.e., the thesis is
not merely that BNPL delinquency will rise, but that *the alt-data
sees it before the official data does, and more so for BNPL than for
comparable consumer-credit issuers*.

SBG also serves as a **real-time pod diagnostic**: if for a given month
the SBG on AFRM is above its 80th-percentile historical value, the
Risk Manager Agent upgrades confidence on the Leg-A trigger. The number
itself is reportable in the dashboard.

**MIDAS accrual-lag correction (v4.1 reinforcement).** The naive SBG
formula subtracts $d^{\text{reported}}_{i,m}$ at the *same* calendar month
as the alt-data-implied $\hat d^{\text{alt}}_{i,m}$. This understates the
gap because reported delinquency reflects hazard events from *earlier*
months rolling through the 30/60-day accrual window. Correct this with
a MIDAS-aligned mapping:

$$
\hat d^{\text{alt}}_{i,m} \;=\; \sum_{\ell=0}^{L} \omega_\ell^{\text{Almon}}(\eta) \cdot \Big(1 - S_{\text{unsys},i}(t_m - \ell)\Big),
$$

where $\omega_\ell^{\text{Almon}}(\eta)$ are exponential Almon weights over
a lag horizon $L$ chosen so the weighted hazard density coincides with the
realized 30-day accrual window (empirically $L \in [30, 60]$ days, $\eta$
fit by minimizing residual autocorrelation against $d^{\text{reported}}$
on the $\mathcal C_1$ sub-panel, where the SBG should be near zero by
construction). This is the same MIDAS machinery used in v2 §3 for
mixed-frequency regression, now repurposed to *align* (rather than
regress across) monthly and weekly frequencies.

**Dual-View SBG reporting (v4.1 reinforcement).** The dashboard exposes
the SBG through two tabs, serving two different decision loops:

* **Live / Anomaly Gauge (primary trigger).** A real-time widget on the
  Streamlit "Live" tab flashing an alert when $\text{SBG}_{i,t}$ crosses
  issuer $i$'s rolling 80th percentile. The gauge is **credibility-
  weighted**:
  * **Red** — SBG above threshold AND cross-platform co-occurrence flag
    $C^{(i)}_t = 1$ (§2.4) AND bot-filter residual-noise score below
    the 50th percentile → high-conviction, Risk Manager upgrades
    Leg-A confidence.
  * **Yellow** — SBG above threshold but co-occurrence OR bot-noise
    conditions fail → caution; do not auto-upgrade confidence; log for
    human review.
  * **Green** — below threshold or de-noised.
* **Diagnostics / Historical Heatmap (paper figure).** A cross-sectional
  monthly view comparing $\mathcal T$ (BNPL) against $\mathcal C_1$
  (near-prime) and $\mathcal C_2$ (placebo). This is the figure used to
  *prove* the debt-stacking hypothesis (§5.5's headline test) rather
  than to trigger trades.

Rationale: information asymmetry decays fast. A monthly summary treats
SBG as a reporting metric; the Anomaly Gauge treats it as a *trading
signal*, which is what the thesis requires. The heatmap is kept for
falsifiability evidence in the paper.

---

## 6. What flows through to the codebase

Translated into Waypoint-1 and Waypoint-4 changes:

### 6.1 Waypoint 1 additions

* `data/ingest/trends.py` — pulls the three query buckets separately into
  `google_trends` table. Uses `pytrends`; no API key.
* `data/ingest/sec_edgar.py` — extended to cover auto-ABS trust families
  (SDART, AMCAR, BCRST, EART) back to 2005, in addition to the BNPL trusts
  back to 2019.
* `data/ingest/auto_abs_historical.py` — one-shot backfill of 2005–2010
  auto-ABS trustee reports. Separate module because it uses a different
  EDGAR endpoint for archival filings.
* `data/ingest/firm_vitality.py` — **LinkedIn + X vitality signals via
  Wayback Machine.** Legally defensible posture: we never hit
  linkedin.com or x.com directly. We query the Internet Archive CDX API
  (`http://web.archive.org/cdx/search/cdx`) for cached snapshots of
  public company pages, then parse the archived HTML with BeautifulSoup.
  Two sub-collectors:
  - **LinkedIn** (`linkedin.com/company/<slug>`): extract employee-count
    band, open job-postings count, "People also viewed" peer graph.
    Weekly snapshot cadence, 2019→present where Wayback has coverage.
    Target slugs: `affirm`, `block`, `paypal`, `sezzle`, `zip-co`,
    `upstart`, `klarna`, plus all $\mathcal C_1$ banks.
    *Derived sub-signal — Tenure Slope (v3.1):* compute the
    openings-to-headcount ratio $T^{(i)}_t = \text{openings}_t /
    \text{headcount}_t$. A sharp collapse $\Delta T^{(i)}_t < -2\sigma$
    while headcount is flat ("hiring freeze while nobody leaves") is
    a documented ~3-month leading indicator of corporate hunker-down
    mode before delinquency shows up in 10-Qs. Encoded as a binary
    feature `freeze_flag_t` feeding the dynamic factor.
    *Staleness penalty (v4.1 reinforcement):* Wayback snapshots are not
    real-time; coverage gaps of 30+ days are common. To prevent a stale
    "hiring freeze" reading from contaminating the current BSI, every
    feature extracted from a Wayback snapshot carries an age $a_t$ in
    days, and its contribution to $\hat f_t$ is exponentially decayed:

    $$
    \text{weight}(a_t) \;=\; \exp\!\left(-\frac{\max(0,\; a_t - 30)}{\tau}\right),
    $$

    with $\tau = 30$ days (half-life ~21 days beyond the 30-day grace
    window). Features fresher than 30 days carry full weight; features
    60 days old carry ~37% weight; 90 days old ~14%. Same rule applies
    to LinkedIn headcount, openings, Tenure Slope, and the X vitality
    sub-signals below.
  - **X / Twitter** (`twitter.com/<handle>`): follower count,
    pinned-tweet text, bio changes. Same Wayback CDX flow.
    Handles: `@Affirm`, `@Klarna`, `@Afterpay`, `@PayPal`, `@Sezzle`,
    `@Upstart`, `@ZipCo`. X carries lower weight in BSI than LinkedIn
    headcount (follower counts are noisy); treated as confirmatory.
  - Both feed the dynamic factor $\hat f_t$ in §2.3, not the Trends
    overlay. Coverage gaps are logged and forward-filled with a
    staleness flag consumed by downstream models.
  - Bot/credibility filter (§3.1) does **not** apply here — these are
    corporate-owned accounts, not user-generated content.
* `data/ingest/reddit.py` — Reddit API ingest. **Blocked on API
  approval** (applied 2026-04; typical turnaround 1–3 weeks). Until
  then, this module is a stub that reads from the Academic Torrents
  Pushshift archive as a backfill source. Once API lands, live stream
  is appended to the same `reddit_posts` table; schema is identical so
  downstream code is unaffected. Bot filter (§3.1) runs on both sources.

### 6.2 Waypoint 4 additions

* `quant/jarrow_turnbull.py` — rewritten as a **two-factor** model. Public
  API: `fit_systematic(macro_proxy) -> SystematicCIR`,
  `fit_unsystematic(issuer, residual) -> UnsystematicCIRWithJumps`.
* `quant/regime_transport.py` — calibrates
  $\theta_{\text{sys}}^{\text{bad}}$ from the 2008 auto-ABS data and
  exports it as a named scenario.
* `nlp/bot_filter.py` — the credibility-scoring classifier from §3.1.
* `nlp/trends_anomaly.py` — velocity cap + cross-query consistency from §3.2.
* `signals/bsi.py` — updated to compose dynamic factor + Trends overlay
  with the §2.3 formula.
* `portfolio/market_neutral.py` — the Leg A / Leg B sizing solver under
  the two-factor model.

Everything else from v2 stays as specified.

---

## 7. Frozen decisions (still locked from v2 §11)

Unchanged:

1. **Panel** — 23 entities across 4 groups (`config/panel.yaml`).
2. **Backtest window** — 2019-01-01 → today for the main empirical work.
   Historical auxiliary dataset: 2005–2010 for auto-ABS regime calibration
   only.
3. **Paper length** — 30–35 pages main + unlimited appendix.
4. **Dashboard** — Streamlit first, Next.js second.

Newly locked:

5. **JT specification** — two-factor CIR with additive jumps, Feller
   condition enforced, $J_{\max} = 5\%$ hard cap, Kalman-smoothed BSI
   driving the jump trigger (§1, §3.3).
6. **Google Trends as standalone layer** — three buckets, bucket-(c) drives
   jumps; buckets (a)(b) feed the dynamic factor only (§2).
7. **Crisis-regime transport** — 2008 subprime-auto fit provides
   $\theta_{\text{sys}}^{\text{bad}}$; transport assumption named and
   sensitivity-tested (§4).
8. **Execution expression** — market-neutral TRS-short + bank-credit-long,
   DV01-matched. Paper reports both naive and hedged P&L (§5).
9. **Stress-test visualization** — Streamlit dashboard renders the joint
   $(\phi_\theta, \phi_\kappa)$ grid (§4.3) as a **2D heatmap** of headline
   P&L, with the default $(1.5, 8)$ node annotated. **Sharpe-ratio = 0
   survival contour** overlaid (v4.1): the line separating positive-Sharpe
   from negative-Sharpe regions is rendered explicitly, so the safety
   margin of the transport assumption is visible at a glance. Distance
   from point estimate to the contour = structural robustness of the
   thesis. Replicated as a static figure in paper §11.
10. **Dual-View SBG dashboard (v4.1)** — primary trigger is the real-time
    Anomaly Gauge with Red/Yellow/Green tiers (credibility-weighted by
    co-occurrence flag + bot-filter score); secondary is the historical
    cross-sectional heatmap comparing $\mathcal T$ vs $\mathcal C_1$ vs
    $\mathcal C_2$ (paper figure). See §5.5 for the alert-tier
    definitions.

---

## 8. Revised paper outline (replaces v2 §6 paper outline)

30–35 pages main + unlimited appendix. Section numbering:

1. Abstract
2. Introduction — the Micro-Leverage Epoch and the beta / alpha dichotomy
3. Literature — Jarrow–Turnbull, Merton, Duffie–Singleton, Adrian–Brunnermeier, Di Maggio, Tetlock, Araci/Khalil, CFPB
4. Five-tier BNPL framework
5. Data architecture — 23-entity panel, 7 alt-data sources, frequency alignment
6. **The BSI: dynamic factor + Trends overlay** (§2 + §3 of this doc)
7. **Empirical identification** (v2 §1–3 unchanged: H1–H4, MS-VAR, MIDAS, tail metrics, bootstrap battery)
8. **The two-factor hazard decomposition** (§1 of this doc)
9. **Crisis-regime transport from 2008 subprime auto** (§4 of this doc)
10. **Market-neutral execution framework** (§5 of this doc)
11. Empirical results — event studies, tail metrics, P&L (naive + hedged)
12. Pod architecture — LangGraph + Nemotron + deterministic compliance
13. Limitations — data latency, transport assumption, ToS risk, OTC illiquidity, regulatory constraints on autonomous execution
14. Conclusion
15. References
16. Appendix A — reproducibility
17. Appendix B — all robustness tables
18. Appendix C — full derivation of the two-factor JT
19. Appendix D — Trends query lists and bucket definitions

---

## 9. Updated risk register

| Risk | Impact | v3 mitigation |
|---|---|---|
| Reddit approval doesn't land in time | High | Academic Torrents backfill (v2) + **bot filter** (v3 §3.1) applied to backfill too |
| JT intensity estimation unstable | High | Two-stage procedure (§1.3); **Feller condition enforced** during MLE; Kalman-smoothed driver |
| Viral-meme / bot-amplification hallucinated crisis | High | §3.1 credibility scoring + §1.2 jump cap at 5% + §2.4 tanh saturation |
| 2008 auto-ABS data hard to parse | Medium | EDGAR filings in this era are mostly HTML; parser can fall back to OCR; only $\Lambda_{\text{sys}}$ calibration depends on it |
| Transport assumption fails | Medium | Explicit $\pm 25\%$ sensitivity test in §4.3; paper result must survive |
| Market-neutral hedge leg introduces its own basis risk | Low-Medium | Report residual-beta regression as a diagnostic; if residual beta isn't near zero, the hedge sizing was wrong and paper says so |
| $\bar\beta^{(\mathcal T)} \approx \bar\beta^{(\mathcal C_1)}$ — thesis fails | Existential | Paper still publishable as null result with novel methodology (v2 §10) |

---

## 10. What this version is *not* doing

* Not claiming live deployability. §13 explicitly frames this as a research
  prototype.
* Not executing TRS live. Simulated P&L only.
* Not using Khalil's exact ensemble architecture (LSTM + Random Forest).
  We use dynamic factor + Kalman + two-factor CIR instead. Khalil is cited
  as methodological precedent in §3 lit review and as replication target in
  appendix H6 (v2 §13.3).
* Not modeling jump correlation *across* BNPL issuers. Each $\lambda_{\text{unsys},i}$
  is independent by construction. Cross-issuer contagion is a follow-on
  paper; v3 deliberately stops short.

---

## 11. What I need from you before coding v3

Two small things; everything else is inferrable from the doc:

1. **Confirm the 2008 auto-ABS calibration is worth the engineering time.** It costs ~3 days of ingest + parser work. The payoff is one credible crisis-regime $\theta_{\text{sys}}^{\text{bad}}$ number. Alternative: synthetic stress via bootstrap resampling of in-sample stress days, which is cheaper but weaker. **Your call.**

2. **Confirm the bot-filter training set.** To calibrate §3.1's $\beta$'s we need ~1000 labeled bot accounts and ~1000 labeled human accounts from r/povertyfinance + r/Debt. I can hand-label a stratified sample in ~4 hours, or we can use a public Reddit-bot dataset (BotBust 2023, ~12k labeled accounts). Public dataset is faster; hand-labeling is more on-topic. **Your call.**

Both decisions are mechanical — they don't affect the paper's argument, only its rigor. Default for both: **yes to 2008 calibration, use public BotBust dataset**. Say "go with defaults" and v3 is fully frozen.
