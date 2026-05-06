# Presentation — BearWatch BSI

## Final deliverables

- **`BearWatch_Research_Deck_FINAL.pptx`** — 25-slide presentation deck (cream background, Cambria display font, Illinois orange accent, 6 section colors)
- **`build_deck.py`** — Python script that builds the deck from scratch using `python-pptx`
- **`screenshots/`** — embedded image assets (CVNA chart, math, playbook + live pod placeholder)

## Slide structure

| # | Slide | Section |
|---|---|---|
| 1 | Title | Foundation |
| 2 | Research motivation + BNPL stats | Foundation |
| 3 | What is BSI? | Foundation |
| 4 | 2D neural-style architecture diagram | Foundation |
| 5 | Hypothesis | Foundation |
| 6 | Theoretical framework (cascade) | Foundation |
| 7 | Data (8-pillar table) | Methodology |
| 8 | Methodology · data treatment + EWMA | Methodology |
| 9 | Methodology · regression specification | Methodology |
| 10 | Methodology · analysis & robustness | Methodology |
| 11 | Empirical findings (4 big tiles) | Empirical Findings |
| 12 | Canonical events table (5 events) | Empirical Findings |
| 13 | Event calendar timeline | Empirical Findings |
| 14 | Case study CVNA (analytical) | Empirical Findings |
| 15 | Case studies AFRM + KLAR mini | Empirical Findings |
| 16 | Caveats / scope conditions | Validation & Scope |
| 17 | Panel regression coefficients table | Validation & Scope |
| 18 | SE-sensitivity table (Driscoll-Kraay survives) | Validation & Scope |
| 19 | Future research (Tier 2a + cross-asset contagion) | Research Direction |
| 20 | Contribution to literature | Research Direction |
| 21 | False positive (SEZL) + TRS opportunity bridge | Research Direction |
| 22 | Pod · CVNA case-study chart | Operational Pod |
| 23 | Pod · CVNA math + playbook (screenshots) | Operational Pod |
| 24 | Pod · live pod walkthrough | Operational Pod |
| 25 | Q&A | — |

**Total runtime ≈ 12:30 + Q&A** at conversational pace.

## To rebuild from source

```bash
pip install python-pptx pillow
python build_deck.py
```

Output: `BearWatch_Research_Deck_v9k.pptx` (rename to `_FINAL.pptx` if you want to overwrite the committed version).
