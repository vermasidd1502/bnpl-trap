# Sealed alternative abstract — construct-validity framing

> **Sealed-envelope pre-commitment (Ulysses move).**
>
> This alternative abstract was drafted on 2026-04-22, **before** the
> Phase 2 scorer-surgery result is known. Its purpose is to lock in
> the language of a defensible construct-validity paper so that, if
> the scorer-surgery result kills the catalyst event-study case, we
> do not have to ghost-write a replacement abstract under motivated-
> reasoning pressure.
>
> This file is write-once. If we swap to this framing, it becomes the
> v2 abstract verbatim. If we retain the behavioral-sensor framing,
> this file stays in the repo as a disclosed alternative we considered.

---

## Proposed v2 title (alt framing)

**"A Behavioral Stress Sensor for an Off-Ledger Credit Category:
Construct Validity, Falsification Gauntlet, and the Limits of
Complaint-Based Measurement in Consumer BNPL"**

---

## Proposed v2 abstract (alt framing, ~230 words)

Buy-Now-Pay-Later ("BNPL") is the fastest-growing consumer-credit
category in the United States that is not, for the vast majority of
loans, reported to the major credit bureaus or captured in the Federal
Reserve's Z.1 Financial Accounts. Traditional bureau-based and
delinquency-based stress measurement cannot see it. We ask a narrow
question: can publicly available complaint, review, search, and
discussion channels, processed through a finance-tuned sentiment
model, produce a *behavioral stress sensor* whose properties are
disclosable, whose construct validity can be decomposed, and whose
signal is distinct from existing credit-stress gauges?

We introduce the BNPL Stress Index (BSI), a residualised composite of
CFPB complaint momentum and MOVE Treasury-volatility with additional
public-sentiment pillars entering when coverage permits. We document
three known measurement failure modes: (i) complaint volume confounded
by origination growth, addressed by an origination-residual
specification; (ii) rolling-window σ collapse around regulatory
filing-deadline pulses, addressed by an EWMA σ with 250-day half-life
and a pre-registered floor; (iii) coverage heterogeneity across
sentiment channels, addressed by a per-pillar coverage-gate in the
constrained QP fuse.

Construct validity is assessed through a falsification gauntlet:
Granger tests with pre-registered minimum-detectable-effect
disclosure; three placebo sensors (a word-count sensor, a randomised-
complaint sensor, a non-BNPL complaint-category sensor); and local-
projection impulse-response functions at pre-registered 2–8 week
horizons. The paper takes no position on whether BSI is *tradable*. It
takes the narrower position that, if such a sensor is going to be
used in institutional credit-stress monitoring of off-ledger consumer
lending, it needs to be built and disclosed in the specific way we
describe here. We discuss the limits, including sentiment-channel
attack surface, AI-search dark-channel migration, and regulatory
fragility of the CFPB complaint database itself.

---

## What this abstract promises that the paper must deliver

1. **Construct-validity decomposition** (§2.5, new): complaint filing
   = function of (distress, awareness, regulatory mechanics,
   complaint propensity). We trace each component and disclose which
   BSI cannot separate.

2. **Three known measurement failure modes disclosed**, with fixes:
   - Origination confound → residual-on-log-originations BSI
   - σ collapse → EWMA σ with 250d half-life, pre-registered floor
   - Coverage heterogeneity → per-pillar coverage-gate in QP fuse

3. **Falsification gauntlet** (§6, rewrite): Granger with MDE
   reported; three placebo sensors; LP-IRF at 2–8w horizon.

4. **No tradability claim.** Event study is relabeled "illustrative
   case study," headline abstract carries no bp-alpha claim, §9
   reframed as "signature of a no-trade filter."

5. **Explicit limits section** (§10, expanded): sentiment attack
   surface, AI-search dark-channel migration, CFPB political
   fragility.

---

## What v2 body sections this abstract requires

- §1 Introduction — reordered around (a) BNPL bureau-invisible, (b)
  existing stress gauges off-ledger blind, (c) sensor question, (d)
  what BSI reads, (e) construct-validity decomposition.
- §2 Related literature — Baker-Wurgler 2006/2007, Da-Engelberg-Gao
  2011/2015 FEARS, Loughran-McDonald 2011, Tetlock 2007, Mian-Sufi
  2009/2011, Gross-Souleles 2002, deHaan-Kim-Lourie-Zhu 2024 JFE,
  Guttman-Kenney-Firth-Gathergood 2023, Jordà 2005.
- §2.5 Construct validity — NEW.
- §5 Data architecture + BSI — largely preserved from v1 but
  renamed "CFPB-MOVE composite" with disclosed EWMA σ + floor.
- §6 Falsification gauntlet — REWRITTEN from v1 §6.
- §7 Pricing — §7.1 Jarrow-Turnbull moves to Appendix B as
  *illustrative* tranche pricing under stated parameters; §7.2
  Heston/SCP and §7.3 Squeeze Defense CUT.
- §8 Execution — SHORTENED, relabeled as a *framework* not a trade.
- §9 Event panel — REFRAMED as a case study of no-trade filtering;
  no alpha claim in abstract; §9.y super-threshold bypass
  retained *only* as a post-hoc illustration with disclosure.
- §10 Threats to validity — EXPANDED (sentiment attack surface,
  AI-search migration, CFPB fragility).
- §11 Limitations — v1 structure preserved.
- §12 Robustness — v1 structure preserved.
- §13 Conclusion — REWRITTEN: no regime claim, no trade
  recommendation, narrow construct-validity contribution.

---

## Venue target (unchanged across both framings)

- **SSRN working paper** — post 2–3 weeks after v2 rebuild.
- **NBER working-paper series** — submit via affiliated UIUC
  finance faculty once SSRN accrues 4 weeks of downloads.
- **Journal of Financial Stability** — target submission 8–12 weeks
  after SSRN post. The construct-validity framing in this
  alternative abstract is squarely in JFS's scope
  (behavioral-sensor construction + falsification discipline).

Backup venues if JFS rejects:

- *Review of Financial Studies* — less likely given the
  single-category focus, but the construct-validity methodology
  is RFS-calibre.
- *Journal of Banking & Finance* — good fit if JFS rejects on
  fit-not-quality grounds.
- *Journal of Financial Econometrics* — fit for the falsification-
  gauntlet methodology alone, without the policy framing.

---

## Why the sealed envelope

The risk, without pre-commitment, is that when the Phase 2 scorer-
surgery result lands in the morning, motivated reasoning pushes the
author to either (a) re-interpret a null result as "still consistent
with the thesis" or (b) ghost-write a new abstract that backs the
result. Neither is honest science. This file removes both options:
if the origination-residual BSI still reads the 2025-01-17 Reg Z
event cleanly, the behavioral-sensor framing stays. If it does not,
this alt abstract becomes v2 verbatim. No third option.

The pre-commitment is recorded with a git timestamp on this file.
