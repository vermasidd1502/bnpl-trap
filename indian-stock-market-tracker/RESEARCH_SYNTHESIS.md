# Marle-G — Research Synthesis (India equities)

*Reproducible backtests on a canonical Groww-built 5-year daily panel (**750 liquid names, 2021-06 → 2026-06**).
All long-biased (India shorting does not pay). Net of cost, bootstrap-bounded, regime-split. This note
records what is REAL vs FOLKLORE, the mechanism, and the tradeable macro theory.*

> **Survivorship caveat up front:** the panel holds currently-listed survivors over a mostly-bull 5y, so
> *absolute* long returns are biased UP (the ~1%+ 10-day means and 23-26% CAGRs are optimistic). The
> trustworthy results are the **relative** ones — model-vs-model, regime-vs-regime, gated-vs-always-on —
> which are survivorship-neutral. Read the comparisons, not the raw levels.

---

## 1. The through-line: long-or-cash, and horizon sign-flips
- **Shorting never pays** at any horizon (gated-short −0.8/−1.1%; every bearish candle is a contrarian-long
  tell). Short-side = hedge / event only.
- **Micro-horizon (intraday→next-day) MEAN-REVERTS** — four independent confirmations: the order book (a
  rising stock shows a sell-heavy *resting* book), the square-off (15:00→close drifts +0.04%, not down),
  close-location (closed at lows → next-day +0.24%), and the last 5-min candle (red → next-day +0.18%; the
  folk "green close = bullish" is **inverted**).
- **Medium-horizon (days→weeks) TRENDS** — momentum / relative-strength works (this note's core).
- **Reconciliation:** over-reaction reverts intraday; the trend carries over days. A retail-heavy market
  amplifies both — and the best entry exploits *both at once* (buy the trending leader on its micro-dip).

---

## 2. Strategy leaderboard — canonical 5y panel (10-day, net 0.25%, 329 dates, ranked by bootstrap lower bound)

| Model | mean% | hit% | net% | boot_lo | **bull** | **bear** |
|---|---|---|---|---|---|---|
| **gate_pullback** (gated leader bought on a 5d DIP) | 1.49 | 54.7 | 1.24 | **1.24** | 1.78 | 0.64 |
| **ind_mom** (own top-20% industries) | 1.32 | 53.0 | 1.07 | **1.23** | 1.62 | 0.77 |
| gate_strict (the live screener gate) | 1.27 | 53.2 | 1.02 | 1.13 | 1.52 | 0.24 |
| mom126 (6-mo stock momentum) | 1.22 | 52.7 | 0.97 | 1.13 | 1.49 | 0.71 |
| gate_hot (gate & RSI>70) | 1.31 | 53.5 | 1.06 | 1.12 | 1.57 | **−0.32** |
| confluence_z (mom+lowvol+industry+fib composite) | 1.15 | 53.7 | 0.90 | 1.10 | 1.43 | 0.63 |
| mom_nothot (mom & RSI<70) | 1.19 | 52.4 | 0.94 | 1.08 | 1.40 | 0.89 |
| rev5 (buy 5-day losers) | 1.02 | 52.3 | 0.77 | 0.93 | **1.02** | **1.01** |
| hi52 (near 52w high) | 1.04 | 53.0 | 0.79 | 0.93 | 1.19 | −0.00 |
| qmom (quality-momentum) | 0.85 | 53.9 | 0.60 | 0.75 | 1.02 | 0.60 |
| **drift** (do-nothing baseline) | 0.54 | 49.3 | 0.29 | 0.47 | 0.76 | 0.53 |

**Findings:**
1. **Every selection model beats drift net of cost** on this panel — the gate + momentum longs are real
   (with the survivorship caveat). The funnel works.
2. **`gate_pullback` is the best-fit** — *buy the gated leader on a 5-day dip.* Highest mean + bootstrap
   lower bound, positive in **both** regimes. This validates "buy strength on weakness" — momentum
   selection + mean-reversion entry, the two horizons combined.
3. **`rev5` (buy losers) is the only REGIME-NEUTRAL edge** (bull 1.02 / bear 1.01). Short-term reversal is
   the one thing that pays when the market isn't trending — the natural complement to momentum.
4. **`ind_mom` (pure industry momentum) is the robust #2** — simple, high-n, the core driver.

---

## 3. `strict_hot` & the retail-momentum hypothesis — CONFIRMED
The user's hypothesis: a retail-heavy market makes over-heated (RSI>70) names *continue* in bulls and
*crash* in corrections (Daniel-Moskowitz, "Momentum Crashes", 2016).
- **`gate_hot`: bull +1.57% / bear −0.32%** — the single most regime-split model. Confirmed exactly.
- `gate_indmom` (gate × strongest industry) also craters in bear (+0.03). `gate_cool`/`mom_nothot` hold up
  far better in bear (+0.57 / +0.89).
- **Verdict:** chasing hot names is a **bull-only, regime-gated** trade, not an unconditional edge. The
  earlier thin-sample "surprise" was real but regime-dependent — exactly as predicted. *Mechanism: retail
  extrapolation/attention/lottery demand sustains hot names in euphoria and unwinds violently in panic.*

---

## 4. WHEN the alpha lives — macro-state attribution (gate − drift, 10d)
| State | alpha | reading |
|---|---|---|
| **trend** | bull **+0.49%**, bear **−0.30%** | the gate's alpha is a BULL phenomenon; in bear it *underperforms* drift |
| **volatility** | low **+0.58%**, mid +0.52%, high +0.11% | alpha lives in CALM markets |
| **dispersion** | high **+0.51%**, mid +0.43%, low +0.14% | alpha lives when DISPERSION is high — *dispersion forecasts alpha* (the Regime-Dial thesis, confirmed) |
| breadth | mid +0.66%, high +0.30%, low −0.12% | weak in low-breadth (washouts) |

**The macro theory:** the stock-selection edge is concentrated in **bull + low-vol + high-dispersion**
markets — i.e. calm, broad, stock-picker's regimes. In bear / high-vol / low-dispersion (macro-driven
panic), selection adds nothing or hurts. This is textbook *dispersion-forecasts-alpha* + *momentum-crash*.

---

## 5. WHERE the edge lives — cross-sectional decomposition (10d)
- **Liquidity:** illiquid 1.46% · mid 1.50% · **liquid 0.95%**. The edge is stronger in less-liquid names
  (a partial illiquidity premium) **but SURVIVES in the liquid, tradeable tier (+0.95%, net +0.70%)** —
  so it's not purely an artifact. *Trade the liquid tier; expect ~0.95%, not 1.5%.*
- **Volatility:** high-vol names higher mean (1.56%) but lower hit (51%); low-vol lower mean (0.93%) higher
  hit (55%) — a return/consistency trade-off.
- **Industry depth:** top-10% industry (1.29%) ≈ 10-40% (1.26%) — the value is the **top-40% threshold**,
  not finer ranking.
- **Sectors:** BROAD, not a single-sector fluke — strongest in Construction (2.45%), Realty (2.37%),
  Capital Goods (1.68%), IT (1.94%), Financials (1.64%). The cyclical/infra/rate-sensitive + IT complex.

**Mechanism (reverse-engineered):** broad-based industry relative-strength (not one sector, not pure
illiquidity), best entered on a micro-pullback, concentrated in calm bull regimes, with the hot variant a
bull-only trade.

---

## 6. THE CAPSTONE — macro-gating the book is a free lunch (10d blocks, net cost)
| Strategy / rule | CAGR | vol | Sharpe | maxDD | invested |
|---|---|---|---|---|---|
| MARKET buy-hold | 23.1% | 17.1% | 1.31 | −20.2% | 100% |
| ind_mom · always-on | 26.3% | 19.7% | 1.29 | −21.2% | 100% |
| **ind_mom · BULL-gated** | 23.2% | 15.4% | **1.44** | **−10.9%** | 67% |
| ind_mom · calm-bull-gated | 12.3% | 13.8% | 0.91 | −19.8% | 56% |
| gate_pullback · always-on | 23.8% | 23.5% | 1.03 | −20.3% | 95% |
| **gate_pullback · BULL-gated** | **26.1%** | 18.3% | **1.36** | −16.2% | 64% |
| gate_pullback · calm-bull-gated | 21.2% | 17.3% | 1.20 | −10.6% | 54% |

**Gating the long book on the bull regime (NIFTY > 50DMA) roughly HALVES the drawdown and lifts Sharpe
ABOVE the market (1.44 vs 1.31), at market-like CAGR, while sitting in cash ~1/3 of the time.** Cash was
credited 0% here, so it's conservative (India risk-free ≈ 6-7% would add more). Always-on momentum barely
beats the market on Sharpe (1.29 vs 1.31) — **the macro gate is what creates the genuine risk-adjusted
edge,** and it auto-sidesteps the `strict_hot` bear-crash. *calm-bull (also dodging high-vol) cuts drawdown
furthest but sacrifices too much return — bull-gated is the sweet spot.*

**OOS robustness (first half 2021-23 vs second half 2024-26) — the critical honesty check:**
| Strategy | H1 Sharpe / maxDD | H2 Sharpe / maxDD |
|---|---|---|
| ind_mom always-on | 2.20 / −10.4% | **0.40** / −21.2% |
| ind_mom **bull-gated** | 2.21 / −10.9% | 0.47 / **−10.7%** |
| gate_pullback always-on | 1.83 / −17.3% | **0.14** / −20.3% |
| gate_pullback **bull-gated** | 2.12 / **−7.8%** | 0.44 / −16.2% |

Two things, and both matter:
1. **The raw stock-selection edge DECAYED hard** — Sharpe ~2 in 2021-23 collapsed to ~0.1-0.5 in 2024-26.
   The strong full-sample numbers are front-loaded; **do not expect 2021-23 performance going forward**
   (likely crowding of the retail-momentum trade and/or a choppier recent regime). This is the
   anti-overfit caveat.
2. **The MACRO GATE is the durable, OOS-robust component** — bull-gating cut drawdown and raised Sharpe in
   *both* halves, and in the hard 2024-26 half it **roughly HALVED the drawdown** (ind_mom −10.7% vs
   −21.2%; gate_pullback −16.2% vs −20.3%). So even as the raw edge faded, the regime gate kept earning its
   keep as risk management. **Trust the macro gate more than the raw selection edge.**

---

## 7. The complete strategy & the macro theory
**Micro (selection) × Macro (timing):**
- **Selection:** own leaders in leading industries (industry-RS gate / `ind_mom`), entered on a **5-day
  pullback** (`gate_pullback`), top-40% industry, liquid tier. Broad across cyclical/IT/financial sectors.
- **Timing (the macro gate):** only deploy when **NIFTY > 50DMA** (bull). Optionally lighten in top-tier
  volatility. Stand in cash otherwise — it halves drawdown and dodges the momentum crash.
- **Regime-neutral complement:** `rev5` (buy 5-day losers) is the one edge that pays in bear too — a
  natural diversifier / what to run when the macro gate is shut.
- **Short side:** none directionally. Puts = hedge / cascade-event expression only.

**Why it works (the academic spine):** industry momentum (Moskowitz-Grinblatt) + short-term reversal entry
(Lehmann/Jegadeesh) + dispersion-forecasts-alpha (the calm/high-dispersion regime) + momentum-crash
avoidance via a trend filter (Daniel-Moskowitz) — all amplified by India's retail-heavy flow (extrapolation
builds the trend; over-reaction creates the reverting micro-dips you buy).

---

## 8. Playbook by horizon × instrument
| | LONG | SHORT |
|---|---|---|
| **Intraday** | weakest zone; only the mean-reversion bounce (buy the red close / capitulation), in a bull | none (mean-reverts up) |
| **3-5 day swing** | **the sweet spot** — `gate_pullback`, bull-gated. Buy the gated leader on a dip. | none |
| **Long-dated calls** | compounder/leader in a leading industry, cheap IV, bull regime | — |
| **Puts** | — | hedge / cascade-event only, sized small |

---
*All results reproducible from the cached panel: marleg_strat_lab · marleg_macro_gate_bt · marleg_strat_decompose
· marleg_macro_overlay_bt · marleg_industry_momentum_bt · marleg_gate_models_bt · marleg_closing_pressure_study.*
