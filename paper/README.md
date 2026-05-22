# Research Paper

**A Deterministic Monitoring System for Consumer-Credit Stress: Construction and Validation of the Behavioural Stress Index**

Siddharth Verma · UIN 668601217 · MSF Candidate · Gies College of Business · UIUC

## Files

Three parallel versions are shipped — pick the one that matches the reader's time budget.

- **`BearWatch_Research_Paper_Letter.pdf`** — **Letter** · 13 pages · 558 KB. Journal letter-format companion for senior-reviewer first-look (e.g.\ derivatives / fixed-income professors). Full headline empirics in two tables, a dedicated section on the fixed-income instantiation of the methodology, embedded references. The version to send to anyone who has 10 minutes.
- **`BearWatch_Research_Paper.pdf`** — **Long / main version** · 42 pages · 708 KB. Targeted journal-submission length: full methodology, headline empirics, all five case findings, discussion, limitations, conclusion, future scope. The version to send when the reader will spend an hour.
- **`BearWatch_Research_Paper_Extended.pdf`** — **Extended preprint** · 59 pages · 921 KB. Full preprint with the Phase 2C–2T diagnostic suite (archetype backtest, ROBO Monte Carlo, pillar-weight regularised regression, warehouse back-fill, BNPL event study, Phase 2 capstone, Phase 2A v2 six-estimator SE-sensitivity). Use this for maximum technical detail and reviewer transparency.
- **`BearWatch_Research_Paper.tex`** — LaTeX source for the Extended version (compiles to the 59-page PDF).
- **`BearWatch_Research_Paper_Letter.tex`** — LaTeX source for the Letter (compiles to the 12-page PDF).

## Structure (Extended version)

§1 Introduction · §2 Literature review · §3 Theoretical framework (CVA decomposition + P→Q wedge) · §4 Data architecture · §5 Methodology (BSI v3 spec + leading-indicator chain + 5-gate trade architecture) · §6 Empirical results (sensitivity, specificity, Granger, panel regression, robustness suite, case findings, long-pod, denominator-normalised, credit-instrument anchor, archetype backtest, ROBO Monte Carlo, pillar-weight robustness, warehouse back-fill, BNPL event study, Phase 2 capstone, Phase 2A v2 SE-sensitivity) · §7 Discussion (calendar-time alpha null, instrument-selection problem, symmetric architecture, denominator refinement, competitive position) · §8 Limitations · §9 Conclusion + Future research (fixed-income instantiation + cross-asset BNPL/credit-card contagion) · §10 References · Appendices A–D

The Short version covers §1–§9 with the Phase 2C–2T diagnostic block compressed.

## Headline results

- Event sensitivity: **5/5** (Wilson 95% CI [56.6, 100])
- Granger F-test: **23/27** firms reject H₀ at p<0.05 (median p = 0.0005)
- Panel regression: β = −0.082, **Driscoll-Kraay p = 0.007**
- 6-estimator standard-error robustness suite — every estimator rejects H₀
- Honest disclosure: equity calendar-time α t = 0.08 (zero); paper makes no fixed-income alpha claim

## Rebuild from source

```bash
cd paper
latexmk -pdf BearWatch_Research_Paper.tex
```

Requires MiKTeX or TeX Live with: `amsmath`, `booktabs`, `tabularx`, `graphicx`, `natbib`, `hyperref`.

## Citation

```bibtex
@misc{verma2026bsi,
  author       = {Verma, Siddharth},
  title        = {A Deterministic Monitoring System for Consumer-Credit Stress:
                  Construction and Validation of the Behavioural Stress Index},
  howpublished = {Working paper, FIN 580, UIUC},
  year         = {2026},
  note         = {\url{https://github.com/vermasidd1502/bnpl-trap}}
}
```
