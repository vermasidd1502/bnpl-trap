# Case Studies — Reference Document

A supplementary reference document covering every case study examined by the
Behavioural Stress Index framework.

## Files

- **`BSI_Case_Studies_Reference.pdf`** (677 KB) — recommended for reading
- **`BSI_Case_Studies_Reference.pptx`** (57 KB) — PowerPoint source, editable

## What's in it

10 slides documenting nine firms plus one architectural case:

| # | Case | Verdict |
|---|---|---|
| 1 | Title | — |
| 2 | Index — all cases at a glance | — |
| 3 | **Carvana 2022** — the canonical case | TRUE POSITIVE |
| 4 | **Affirm 2023** — true positive, fast cycle | TRUE POSITIVE |
| 5 | **Klarna 2024 + Sezzle 2024** — two false-positive scenarios | OBSERVED + SUPPRESSED |
| 6 | **LendingClub 2025 + Tricolor 2025** — recent events | TRUE POSITIVE × 2 |
| 7 | **Upstart 2022 + CURO 2024** — formal EVENT firms | TRUE POSITIVE + EXCLUDED |
| 8 | **Bridgecrest 2022** — Carvana subsidiary | TRUE POSITIVE (formal) |
| 9 | **Two-pod architecture** — BearWatch + Equity Monitor isolation | ARCHITECTURAL |
| 10 | Reproducibility — how to verify any claim | — |

## How this relates to the main paper + deck

This document is a **reference companion**, not a replacement for:

- **`paper/BearWatch_Research_Paper.pdf`** — full 59-page research paper
- **`presentation/`** — 26-slide presentation deck (the main pitch)
- **`working_demo/`** — runnable Apollo Hermes pod

If you want a quick "what cases did this work cover?" answer, this deck is
the fastest path. If you want methodology + empirics in depth, see the paper.

## Architectural case (slide 9)

The most distinctive concept in the project: BearWatch (alt-data signals)
and the Equity Monitor (technical/macro signals) run as **separate pods**
with **independent decision logic**. They never cross-pollinate. A
cross-source exposure cap layer prevents either pod from individually
breaching the joint position limit on any ticker.

This separation is what makes the multi-strategy isolation defensible to
fund-quality reviewers.
