# v1 Retrospective — what the first submission got wrong, and how we know

**Paper:** *The Micro-Leverage Epoch: BNPL as Subprime 2.0* (v1, committed
`paper_formal/paper_formal.pdf`, 33 pages)
**Author:** Siddharth Verma · FIN 580 · UIUC · Spring 2026
**Status:** v1 frozen as a working-paper. This document is the public
retrospective that accompanies the v2 rewrite.

> This retrospective was drafted the night of 2026-04-22 as part of an
> honest-research discipline: every ex-post calibration we can identify,
> every measurement artefact we accidentally dressed as a finding, and
> every framing overreach we committed, is enumerated here *before*
> anyone else has to point it out. Each first-person admission below is
> tagged `[author confirms]` where the author's voice is required; those
> markers must be read and signed off by Siddharth before this doc leaves
> draft state.

---

## 1. Framing overreach — "Micro-Leverage Epoch" and "Subprime 2.0"

### What v1 claimed
- That BNPL constitutes a "new epoch" of consumer leverage structurally
  analogous to the 2005–2007 subprime-mortgage build-up.
- That the paper identifies a tradable, institutional short via
  Total-Return-Swaps on junior ABS tranches.

### What the evidence actually supports
- A public-channel sentiment instrument (the BSI) that reads a **known
  regulatory catalyst date** — the 2025-01-17 Reg Z interpretive rule —
  more sharply than traditional stress gauges.
- A five-event case study in which a four-gate compliance overlay
  *avoids* a large drawdown on one event, with the other four events
  tracking naive panels within a point.

### The gap
"Epoch," "Subprime 2.0," and "the trade" are hedge-fund-voice rhetorical
packaging. The empirical content is a **construct-validity argument for
a behavioral sensor** plus a **risk-management overlay**, not a
confirmed macro regime claim and not a replicable alpha result.

### v2 correction
- Paper retitled around behavioral sensing and construct validity.
- Abstract explicitly frames the contribution as "measurement
  instrument + pre-registered validation gauntlet," not as a trade
  recommendation.
- "Micro-Leverage Epoch" and "Subprime 2.0" removed from body text;
  retained only in a footnote acknowledging the v1 framing.
- Paulson 2007 comparisons cut.

`[author confirms: I accept that the v1 abstract's framing was louder
than the evidence, and I take responsibility for the overreach.]`

---

## 2. The |BSI_z| ≥ 10 super-threshold bypass was post-hoc calibrated

### The self-convicting evidence

From `config/thresholds.yaml`, committed on **2026-04-22 at 12:50:51 CDT**
in commit `23f154f` ("Full pod build-out"), roughly 15 months after the
2025-01-17 event the threshold fires on:

```yaml
# Sprint Q bypass (post-review, 2026-04-22): when bsi_z >= this
# threshold (default 10σ), the BSI gate alone is sufficient to
# approve the paper's flagship trade. [...] Calibration: z >= 10
# has fired exactly once in the 2018-2026 sample — on the Reg Z
# bypass date.
bypass_z_threshold: 10.0
```

The comment block itself labels the rule "post-review" and states the
calibration rationale: `z >= 10 has fired exactly once ... on the Reg Z
bypass date`. This is a textbook post-hoc calibration:

1. The bypass was added after the event it fires on.
2. The threshold was chosen so that exactly one day in the historical
   sample triggers it.
3. That day is, by design, the flagship catalyst.

Full audit trail is preserved in `docs/bypass_audit.txt` (2,136 lines,
`git log --all -p -G 'bypass|super_threshold|>=\s*10'`).

### What v1 did with it
v1 §9.y ("BSI-only super-threshold bypass," line 1271 in
`paper_formal/paper_formal.tex`) presented the bypass as a *legitimate
regime-override rule* — a ≥10σ reading is extreme enough to justify
approving the trade on BSI alone, even if MOVE/SCP/CCD-II are cold.
That framing is inadmissible given the git-log evidence.

### v2 correction
- The bypass is **relabelled as a post-hoc illustration** of a
  single-event response, not a general rule.
- The paper explicitly discloses: *"The 10σ bypass threshold was
  introduced after the event date and calibrated to fire exactly
  once. It is retained in the codebase for reproducibility of v1
  figures but does not constitute a predictive rule. We make no
  out-of-sample claim from it."*
- All v2 PnL tables report institutional-panel results **with the
  bypass disabled**. If the bypass is referenced, it is only to
  reproduce v1 figures for transparency.

`[author confirms: The bypass was introduced during the Sprint Q
pass on 2026-04-22. I did not pre-register the threshold, did not
pre-register the event date it fires on, and did not subject it to
out-of-sample testing. Presenting it as a predictive rule in v1 was
a mistake, and I am retracting that framing in v2.]`

---

## 3. The +27σ BSI reading is a measurement artefact, not a finding

### What v1 reported
v1 abstract and §5 feature "BSI reaching +27.4σ on 2025-01-17" as a
headline statistic — the largest z-score in the sample, flagged as
evidence of regime-break severity.

### What actually happened
The BSI pillar z-scores are computed against a **180-day rolling
standard deviation**. During the build-up to the 2025-01-17 filing
deadline:

- CFPB daily complaint count ran at a baseline of ~58/day for
  most of the trailing 180-day window.
- The 2025-01-17 deadline produced a one-day spike to 12,838
  complaints as consumers and advocacy groups filed in bulk
  ahead of the Reg Z interpretive rule cut-off.
- Raw-count ratio: 12,838 / (58 × 1 day) ≈ 221× a daily baseline,
  but with most of the window still at baseline, the rolling σ
  itself was compressed — which inflates the resulting z.

### The honest characterisation
The +27σ figure reflects σ-window collapse around a regulatory
filing-deadline spike, not a 27-standard-deviation extreme event.
Any rolling-z sensor applied to a filing-deadline pulse in a growing
credit category will print a similar artefact.

### v2 correction
Three fixes, all disclosed in v2 §5:
1. **EWMA σ with 250-day half-life** replaces the 180-day rolling
   window as the primary σ estimator. This respects slow regime
   build-up without collapsing in quiet periods.
2. **Pre-registered σ floor** of 0.6 (on the residualised pillar
   series) prevents σ → 0 collapse in genuinely quiet windows.
3. All headline figures re-expressed as **raw-count ratios and
   percentage moves** first, with z-scores offered only as a
   secondary scale-free summary with explicit σ-estimator
   disclosure.
4. The phrase "+27σ" is removed from the v2 abstract and body text
   and replaced with "12,838 complaints on Reg Z deadline vs a
   trailing-180-day baseline of ~58/day (~221× ratio)."

`[author confirms: I am retracting the +27σ framing in v1. The
finding is the raw-count pulse, not the sigma multiple. The sigma
multiple is a property of my σ estimator, not of the world.]`

---

## 4. The BSI "four-pillar composite" is actually a CFPB+MOVE composite in practice

### Nominal weights (`config/weights.yaml`)

| Pillar | Prior weight | Cap |
|---|---|---|
| `cfpb_complaint_momentum` | 0.25 | 0.40 |
| `google_trends_distress` | 0.20 | 0.30 |
| `reddit_finbert_neg` | 0.20 | 0.30 |
| `appstore_keyword_freq` | 0.15 | 0.25 |
| `move_index_overlay` | 0.20 | 0.35 |

### Realised coverage (Figure 2 of v1, p.8)

Across the 2019-07 to 2026-04 test window, the percentage of BSI-days
on which each pillar actually had non-null, non-imputed data:

| Pillar | Realised coverage |
|---|---|
| cfpb | ~35.9% |
| move | ~100% |
| google_trends | ~2% |
| reddit | ~8% (with a 5-month Reddit-API gap Apr–Sep 2023) |
| appstore | ~3% |

### What this means
The paper describes BSI as a **four-pillar** fused sentiment index,
but for roughly two thirds of trading days only CFPB and MOVE are
actually contributing information; the other three pillars are
either stale or imputed. v1 Figure 2 does disclose this, but the
prose throughout the paper continues to speak of "four-pillar"
and "fused sentiment" in a way that is inconsistent with the
actual information content.

### v2 correction
- The composite is renamed **"CFPB-MOVE composite"** throughout v2.
- The three low-coverage pillars are **retained as optional
  components that enter only when coverage exceeds a
  pre-registered threshold**, disclosed at the top of §5.
- The v2 abstract makes this explicit: *"When present, Reddit,
  Google Trends, and App Store pillars enter the composite at
  their QP-derived weights. For the majority of trading days in
  our sample, the composite reduces to a CFPB-complaint residual
  plus a MOVE overlay."*

`[author confirms: I wrote 'four-pillar' throughout v1 while
knowing from Figure 2 that three of the four pillars had
<10% coverage. The prose was inconsistent with my own figure.
v2 resolves the inconsistency in favor of the figure.]`

---

## 5. The Granger "orthogonality" finding is underpowered and under-specified

### What v1 reported
p-values > 0.95 across SPY, HYG, XRT, and SDART proxies at lags 1–10
weeks, flipped-null, interpreted as "BSI is statistically orthogonal
to every macro tier — precondition for mispricing satisfied."

### Two problems

**(a) Power.** With n ≈ 399 weekly observations, lags 4–8, and α=0.05,
the Granger specification has a minimum detectable effect size (MDE)
of approximately:

  ΔR² ≈ 2.1–3.1%  (at 80% power, via scipy.stats.ncf)

Any Granger coefficient whose true ΔR² is below ~2% cannot be
distinguished from zero by this test at this sample size. Reporting
non-rejection as "BSI is independent of macro" is stronger than the
test can support. The honest claim is "we cannot detect a Granger
linkage larger than ΔR² ≈ 2–3%; smaller linkages are possible and
not ruled out."

**(b) Specification.** We fit a linear Granger model on BSI
innovations vs. macro index returns. A behavioral sensor's
relationship with realised returns is unlikely to be linear at
weekly frequency. The v1 Granger test may be rejecting linear
linkage while a non-linear or state-dependent relationship exists.

### v2 correction
- §6 (formerly "Statistical Validation") is **demoted** from
  headline result to a **precondition check** inside a new
  §6 "Falsification gauntlet" that also includes three placebo
  sensors (a word-count sensor, a randomised-complaint sensor,
  and a non-BNPL complaint-category sensor) and a local-projection
  IRF (Jordà 2005) at the 2–8 week horizons where we actually
  expect the lead-lag relationship to live.
- The MDE is computed numerically and reported in §6.
- v1 §6.1 ("Macro orthogonality") is cut; its content folded into
  the §6 gauntlet with the sharper framing.

`[author confirms: I overclaimed orthogonality from non-rejection
in v1. Non-rejection at n=399, lags 4-8, α=0.05 rules out effects
larger than ~2% ΔR², not effects of arbitrary size.]`

---

## 6. The synthetic TRS vs. "naive AFRM short" comparison is not a clean alpha test

### What v1 reported
"Institutional 4-gate panel: +4.58% cumulative over 5 events vs.
naive AFRM short: −4.69% cumulative, for ≈925 bp of alpha over
305 days."

### The apples-to-oranges issues
1. The TRS junior-tranche short does not exist in retail brokerage
   and is priced via the Jarrow-Turnbull model from our own
   hazard-calibration pipeline. The 4.58% figure is a **simulated
   fill against our own pricing model**, not an executed trade.
2. The naive benchmark is an *equity* short of AFRM. The
   institutional trade is a *credit* short via synthetic tranche
   TRS. The two instruments have different duration, different
   liquidity, different financing costs, and different correlation
   regimes. Comparing their PnL on the same number line is not a
   like-for-like alpha test; it is a "what happens under two
   different rule sets" comparison.
3. Transaction costs in the institutional panel are modeled (35–80
   bp TRS bid-ask, SOFR carry, HYG hedge costs, 20% margin, HTB
   penalty). Transaction costs in the naive panel are not modeled.

### v2 correction
- The headline +925 bp number is **removed from the abstract**.
- §9 is rewritten to frame the comparison as "signature pattern of
  a no-trade filter" (what the 4-gate AND does in each regime),
  not as an alpha claim.
- A second comparison panel is added: institutional 4-gate overlay
  applied to the same AFRM *equity* short. This is a like-for-like
  instrument comparison and removes the TRS-simulation confound.
- All PnL numbers labelled "illustrative" where the TRS pricing
  comes from our own model.

`[author confirms: The +925 bp 'alpha' headline in v1 compared a
model-priced synthetic to a real equity short. That is not a
like-for-like benchmark and I should not have led the abstract
with it. v2 restricts the apples-to-apples comparison to the
4-gate overlay versus the unfiltered AFRM equity short on the
same 5 events.]`

---

## 7. What the paper *does* still support

After the above cuts, what remains is a narrower, more defensible
contribution:

1. **Construct validity for a complaint-based behavioral sensor** in
   a regulator-monitored category. We walk through why CFPB
   complaint volume, conditional on origination growth, carries
   distress-related information distinct from macro stress gauges.

2. **Residualisation protocol.** We show that naive complaint-count
   momentum is confounded by origination growth, and we introduce
   an origination-residual BSI whose properties we disclose fully
   (EWMA σ, 250-day half-life, floor, per-pillar coverage gates).

3. **A falsification gauntlet** — Granger with MDE disclosure, three
   placebo sensors, local-projection IRF at 2–8 week horizons.

4. **A case-study event panel** showing that, for the five events
   we pre-registered, the 4-gate AND avoided a drawdown on one
   event. We do not claim this generalises. We claim it is a
   signature of a *no-trade filter* whose value lies in regime
   identification, not in return generation.

5. **A reproducible agentic-pod architecture** with deterministic
   compliance (Python rules) gating LLM advisory narratives. The
   engineering contribution — institutional-grade audit discipline
   over an LLM agent pipeline — is unaffected by the above retractions.

---

## 8. What is NOT fixed by this retrospective (known residual issues)

- **Small-n event panel.** Five events is a proof-of-concept sample
  size, not a significance result. v2 does not claim otherwise.
- **ABS tranche inaccessibility.** Retail cannot execute the TRS
  directly. The XRT-puts + HY-credit-short proxy introduces
  tracking error the paper discloses but does not fully quantify.
- **Sentiment-channel attack surface.** Bot farms can move Google
  Trends, Reddit. Pillar caps in the QP and EWMA-smoothing on λ
  help but do not eliminate the attack surface.
- **LLM safety-filter refusals.** Nemotron refused ~3% of macro-
  language prompts in the sample period; Gemini fallback worked
  but introduces provider dependence.

These are flagged as open issues in the v2 §10 "Threats to
validity" section.

---

## Commitments

1. This retrospective ships alongside v2 in the same repository at
   `docs/v1_retrospective.md`. It is linked from v1 and v2 title
   pages.

2. The v1 PDF is preserved at `paper_formal/paper_formal_v1.pdf`
   (committed after this retrospective is accepted) for
   reproducibility and disclosure. It is clearly marked as
   superseded.

3. The `[author confirms]` markers in this document must be
   reviewed line-by-line by Siddharth before v2 is submitted to
   SSRN. Each marker records a first-person admission that the
   author accepts personal responsibility for the v1 framing
   error described above.

4. No v2 PnL claim, threshold, σ floor, or pillar weight will be
   calibrated on data from the 2025-01-17 REGZ event window.
   That window is quarantined as a pre-registered test case.
