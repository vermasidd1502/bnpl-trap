# Research Paper — BearWatch BSI

## Final deliverables

- **`BearWatch_Research_Paper.pdf`** — final compiled PDF (59 pages, 943 KB)
- **`BearWatch_Research_Paper.tex`** — LaTeX source

## Structure

§1 Introduction · §2 Literature review · §3 Theoretical framework (CVA decomposition + P→Q wedge) · §4 Data architecture · §5 Methodology (BSI v3 spec + leading-indicator chain + 5-gate trade architecture) · §6–§10 Empirical results (sensitivity, specificity, Granger, panel regression, robustness suite, case findings, long-pod, denominator-normalised, credit-instrument anchor, archetype backtest, ROBO Monte Carlo, pillar-weight robustness, warehouse back-fill, BNPL event study, Phase 2 capstone, Phase 2A SE-sensitivity) · §11 Discussion (calendar-time alpha null, instrument-selection problem, symmetric architecture, denominator refinement, competitive position) · §12 Limitations (n=15, CURO, marginal value of EWMA, equity-α=0, no FI data, long-pod survivorship, generalisability, replication, speed-edge limitation) · §13 Conclusion · §14 Future research (fixed-income instantiation + cross-asset BNPL/credit-card contagion) · References · Appendices

## To rebuild from source

```bash
cd paper
latexmk -pdf BearWatch_Research_Paper.tex
```

Requires: MiKTeX or TeX Live + standard packages (`amsmath`, `booktabs`, `tabularx`, `graphicx`, `natbib`, `hyperref`, etc.).

## Key results

- Event sensitivity: **5/5** (Wilson 95% CI [56.6, 100])
- Granger F-test: **23/27** firms reject no-causality null at p<0.05 (median p=0.0005)
- Panel regression: β = −0.082, **Driscoll-Kraay p = 0.007**
- 6-estimator standard-error robustness suite — every estimator rejects H₀
- Honest disclosure: equity calendar-time alpha t=0.08 (zero); paper makes no fixed-income alpha claim
