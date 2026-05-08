# Research Paper

**A Deterministic Monitoring System for Consumer-Credit Stress: Construction and Validation of the Behavioural Stress Index**

Siddharth Verma · UIN 668601217 · MSF Candidate · Gies College of Business · UIUC

## Files

- **`BearWatch_Research_Paper.pdf`** — Final 59-page compiled paper (943 KB)
- **`BearWatch_Research_Paper.tex`** — LaTeX source

## Structure

§1 Introduction · §2 Literature review · §3 Theoretical framework (CVA decomposition + P→Q wedge) · §4 Data architecture · §5 Methodology (BSI v3 spec + leading-indicator chain + 5-gate trade architecture) · §6–§10 Empirical results (sensitivity, specificity, Granger, panel regression, robustness suite, case findings, long-pod, denominator-normalised, credit-instrument anchor, archetype backtest, ROBO Monte Carlo, pillar-weight robustness, warehouse back-fill, BNPL event study, Phase 2 capstone, Phase 2A SE-sensitivity) · §11 Discussion (calendar-time alpha null, instrument-selection problem, symmetric architecture, denominator refinement, competitive position) · §12 Limitations · §13 Conclusion · §14 Future research (fixed-income instantiation + cross-asset BNPL/credit-card contagion) · References · Appendices

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
