// ============================================================
// Hedge-fund deep-dive modal — sourced from vendor_mirror_pod
// (BNPL_v9_FINAL/09_bearwatch_pod/.../mirror_pod/agents/*.py)
//
// Five archetypes with full Strategy DNA: philosophy, leverage limit,
// target beta, factor exposures, sector concentrations, vulnerability.
// Plus a BEARWATCH_MODEL entry so we can compute correlation vs our own
// model's factor vector.
//
// Used by both /demo (Pod 2 War Room rows) and /live (4-stage Debate panel).
// ============================================================

const HF_FACTOR_AXES = ["growth", "value", "momentum", "duration", "commodity"];

const HEDGE_FUND_DNA = {
  Renaissance: {
    color: "#FF6B6B",
    icon: "🧠",
    logo: "https://logo.clearbit.com/rentec.com",
    archetype: "The Quant",
    philosophy: "Markets exhibit micro-inefficiencies and hidden mathematical patterns. Exploit via high-frequency, mean-reverting pair trades. No macroeconomic intuition, no fundamental analysis, no discretionary timing.",
    thesis: "On this name, Renaissance is pattern-matching against historical subprime-distress regimes. The CFPB-velocity profile rhymes with the 2018 LendingClub setup and the 2022 CVNA pre-stress trajectory. The statistical model says SHORT if the cross-pillar concordance reaches 5 of 8 active pillars — the multi-pillar threshold protects against single-pillar noise.",
    confirms_needed: "Awaiting (i) Bluesky pillar to cross +0.5σ as a co-confirmation, (ii) MOVE z-score to stay below 1.0 (no rates-vol confound), and (iii) at least 30 trailing days of CFPB coverage so the signal isn't a single-day spike.",
    leverage_limit: 2.5,
    target_beta: 0.4,
    factors: { growth: 0.6, value: 0.1, momentum: 0.7, duration: 0.0, commodity: 0.0 },
    sectors: { Technology: 0.50, Communication: 0.15, "Consumer Disc.": 0.15, Financials: 0.10, Other: 0.10 },
    vulnerability: "Quant Quakes: breakdown of historical statistical correlations",
    aum_b: 165, founded: 1982, hq: "East Setauket, NY",
    signature_trade: "Medallion Fund — 39% gross/yr 1988-2018",
  },
  Bridgewater: {
    color: "#45B7D1",
    icon: "🌍",
    logo: "https://logo.clearbit.com/bridgewater.com",
    archetype: "The Macro Strategist",
    philosophy: "Analyze markets through a proprietary 'economic machine' framework. Evaluate assets based on short-term debt cycle, long-term deleveraging, and secular productivity growth. Equalize risk across four quadrants: rising growth, falling growth, rising inflation, falling inflation.",
    thesis: "Bridgewater positions on this name only when the macro composite is calm AND the name-specific complaint acceleration is large. The intuition: when broad macro is uneventful, idiosyncratic stress at a single issuer becomes a clean P-measure signal of borrower deterioration that bleeds into Q-measure (CDS) repricing months later. If macro is in a regime shift, the name-level signal is contaminated.",
    confirms_needed: "Awaiting (i) FRED macro composite z below ±1.0 (no recession or stimulus regime), (ii) CFPB delta on this name above +30% over 90 days, and (iii) the Citadel archetype to also vote SHORT (cross-style validation).",
    leverage_limit: 2.0,
    target_beta: 0.5,
    factors: { growth: 0.3, value: 0.2, momentum: 0.1, duration: 0.7, commodity: 0.6 },
    sectors: { "Fixed Income": 0.35, Commodities: 0.25, Index: 0.20, "EM Equity": 0.10, Other: 0.10 },
    vulnerability: "Stagflation: simultaneous equity and bond decline breaks risk parity",
    aum_b: 124, founded: 1975, hq: "Westport, CT",
    signature_trade: "All Weather — risk-parity flagship since 1996",
  },
  "Two Sigma": {
    color: "#4ECDC4",
    icon: "🎯",
    logo: "https://logo.clearbit.com/twosigma.com",
    archetype: "The Factor Decomposer",
    philosophy: "Traditional asset allocation hides overlapping risks. Deconstruct all investments into orthogonal, statistically uncorrelated risk factors: Core Macro, Secondary Macro, Macro Styles, Equity Styles. Maintain beta neutrality globally.",
    thesis: "Two Sigma decomposes the BSI into its 8 component pillars and only takes a position when at least 3 orthogonal pillars are co-signaling. A single pillar moving alone (e.g. CFPB only, or Reddit only) is treated as noise; concordance across uncorrelated signal sources is what drives conviction. This filters out the Sezzle false-positive class where a single sustained pillar surge produced no actual distress event.",
    confirms_needed: "Awaiting (i) BSI composite z ≥ 2.0, (ii) Bluesky-consumer pillar z ≥ 0.5 as a non-CFPB confirmation, (iii) Reddit pillar z ≥ 0.5, (iv) the cross-source correlation of the active pillars to remain below 0.6 (no degenerate single-source signal).",
    leverage_limit: 1.5,
    target_beta: 0.0,
    factors: { growth: 0.3, value: 0.4, momentum: 0.5, duration: 0.2, commodity: 0.1 },
    sectors: { Technology: 0.25, Financials: 0.20, Index: 0.20, Energy: 0.15, Other: 0.20 },
    vulnerability: "Crowded factor unwinds; momentum crashes",
    aum_b: 60, founded: 2001, hq: "New York, NY",
    signature_trade: "Compass — multi-factor systematic equities",
  },
  Millennium: {
    color: "#96CEB4",
    icon: "⚡",
    logo: "https://logo.clearbit.com/mlp.com",
    archetype: "The Risk Manager",
    philosophy: "Extract idiosyncratic alpha while neutralizing all market beta. Multi-manager platform: hundreds of independent pods. Absolute requirement: aggregate portfolio immune to market direction. Capital allocation driven by strict risk management.",
    thesis: "Millennium is the structural skeptic of the war room. Default behavior is PASS — a position only opens when the trade meets per-pod stop-loss discipline (5% per-position cap, beta-neutral aggregate). On consumer-credit shorts, Millennium is especially conservative because borrow-availability and reflexive-momentum risk can blow through a stop in a single session (Sezzle, GME, AMC pattern). Millennium's PASS vote is informative: it says even a clean signal does not yet justify execution.",
    confirms_needed: "To vote SHORT, Millennium needs (i) Renaissance + Two Sigma to both have voted SHORT first, (ii) borrow availability confirmed (rate < 5%), (iii) the equity's 30-day realized vol below 60% to make the 5% stop survivable, (iv) no active short squeeze indicators (FTD spike, threshold security listing).",
    leverage_limit: 3.0,
    target_beta: 0.0,
    factors: { growth: 0.0, value: 0.0, momentum: 0.0, duration: 0.0, commodity: 0.0 },
    sectors: { Technology: 0.15, Energy: 0.15, Financials: 0.15, Healthcare: 0.15, Other: 0.40 },
    vulnerability: "Liquidity vacuums during strict stop-loss execution",
    aum_b: 70, founded: 1989, hq: "New York, NY",
    signature_trade: "Multi-PM platform — 5% pod-level stop-out",
  },
  Citadel: {
    color: "#FFEAA7",
    icon: "💼",
    logo: "https://logo.clearbit.com/citadel.com",
    archetype: "The Industrial Operator",
    philosophy: "Industrialized extraction of alpha. Combine discretionary fundamental analysis with massive quantitative overlays. Monetize extreme single-stock and sector dispersion. No macro-directional calls — neutralize market-wide risks to isolate idiosyncratic winners and losers.",
    thesis: "Citadel evaluates this name as a single-stock dispersion trade — the question is whether the name will significantly underperform its sub-sector basket over the holding window. The CFPB acceleration on this firm relative to its peer group's CFPB-velocity baseline is the dispersion signal. If the firm's z is high in absolute terms but its peer group is also elevated, Citadel passes (no dispersion). If the firm is the outlier, Citadel takes the position.",
    confirms_needed: "Awaiting (i) BSI z ≥ 1.5 absolute, (ii) firm-vs-peer-group CFPB delta divergence ≥ 25 percentage points, (iii) execution feasibility: borrow available + ABS shelf liquidity confirmed, (iv) no scheduled idiosyncratic catalyst (earnings, debt issuance) within the next 14 days that could create cover-noise.",
    leverage_limit: 3.0,
    target_beta: 0.1,
    factors: { growth: 0.5, value: 0.3, momentum: 0.4, duration: 0.2, commodity: 0.1 },
    sectors: { Technology: 0.35, Index: 0.15, Financials: 0.15, Energy: 0.10, "Fixed Income": 0.15, Other: 0.10 },
    vulnerability: "Collapse of market dispersion; hyper-correlated regimes",
    aum_b: 65, founded: 1990, hq: "Miami, FL",
    signature_trade: "Wellington — flagship multi-strategy",
  },
  // ===== LONG-THESIS ARCHETYPES (post-stress mean-reversion / contrarian value) =====
  // These vote LONG only when the 5-condition recovery test is satisfied:
  //   (1) prior BSI z peak ≥ 2.0  (2) decay ≥ 1.5σ from peak  (3) price near Fib support
  //   (4) U/D vol ratio > 1.2  (5) no new z>2 in trailing 30d
  // Each archetype's vote is a deterministic function of those inputs.
  Buffett: {
    color: "#fde047",
    icon: "💎",
    logo: "https://logo.clearbit.com/berkshirehathaway.com",
    is_long_archetype: true,
    archetype: "The Quality-at-Fair-Price Long",
    philosophy: "Buy wonderful companies at fair prices. Avoid leverage, avoid airlines, avoid anything you don't understand. Hold forever. Wait years for the right pitch and swing only at fat ones.",
    thesis: "On a recovering distressed name, Buffett asks: did this firm ever have a durable economic moat, and did the recent stress impair the moat or just the price? If the moat survived (recurring revenue, switching costs, network effects), the post-stress entry at a Fibonacci support is the closest thing to a fat pitch in alt-credit. If the moat was fictional (subprime growth-via-marketing), pass.",
    confirms_needed: "(i) prior BSI z above 2.0 with current decay ≥ 1.5σ, (ii) price within 3% of a Fibonacci support level (50% or 61.8% preferred), (iii) firm has demonstrable revenue durability (we proxy via 5y CAGR > 5%), (iv) no fresh BSI flare in trailing 30 days.",
    leverage_limit: 1.0,
    target_beta: 0.85,
    factors: { growth: 0.30, value: 0.70, momentum: 0.10, duration: 0.20, commodity: 0.05 },
    sectors: { Insurance: 0.30, Consumer: 0.20, Tech: 0.15, Energy: 0.10, Other: 0.25 },
    vulnerability: "Tech disruption that erodes a moat we thought was durable; misjudging product-margin sustainability.",
    aum_b: 350, founded: 1965, hq: "Omaha, NE",
    signature_trade: "Buy-and-hold concentrated value — Coca-Cola 1988, Apple 2016",
  },
  Pabrai: {
    color: "#fb923c",
    icon: "🎯",
    logo: "https://logo.clearbit.com/pabraifunds.com",
    is_long_archetype: true,
    archetype: "The Concentrated Hated-Names Long",
    philosophy: "Heads I win, tails I don't lose much. Concentrated bets (10–15 names) on hated stocks at deep discounts to intrinsic value. Munger-style cloning of ideas from other great investors.",
    thesis: "On post-stress alt-credit names, Pabrai's question is: is the market pricing this for default when the actual default risk is materially lower? If the bond market is calmly pricing the credit but the equity is panicked, that's the asymmetric setup. He sizes 5–10% per position only when conviction is HIGH.",
    confirms_needed: "(i) BSI z peak was ≥ 2.5 (stress was real, not a head-fake), (ii) post-decay BSI z below 1.0 (stress definitively fading), (iii) price down ≥ 40% from 52w high (deep discount), (iv) Fibonacci support cluster (multiple Fib levels close together at current price).",
    leverage_limit: 1.0,
    target_beta: 0.7,
    factors: { growth: 0.20, value: 0.80, momentum: 0.10, duration: 0.10, commodity: 0.05 },
    sectors: { Financials: 0.25, "Consumer Disc.": 0.20, Energy: 0.20, Auto: 0.15, Other: 0.20 },
    vulnerability: "Value traps where the company keeps deteriorating; over-concentration when one position blows up.",
    aum_b: 1.2, founded: 1999, hq: "Irvine, CA",
    signature_trade: "Fiat 2012 (10-bagger); concentrated subprime auto bets in 2009",
  },
  "Howard Marks": {
    color: "#22d3ee",
    icon: "🌊",
    logo: "https://logo.clearbit.com/oaktreecapital.com",
    is_long_archetype: true,
    archetype: "The Distressed-Credit Long",
    philosophy: "It's not what you buy, it's what you pay. Distressed credit at the bottom of cycles — buy senior-secured bonds at 30 cents on the dollar, ride them back to par. Cycles always overshoot in both directions.",
    thesis: "Marks's framework on a stressed alt-credit name: is the BSI z-decay reflecting genuine improvement in the credit, or just temporary flow? If trustee data on the ABS shelf shows charge-offs flattening AND the bond market spreads have started tightening AND the equity is still discounting bankruptcy, the long is in the credit instrument or the equity recovery — Marks personally prefers the senior credit.",
    confirms_needed: "(i) BSI z decay ≥ 1.5σ from peak, (ii) ABS-shelf NCL trajectory flat or declining (we proxy this via CFPB complaint velocity dropping for 60+ days), (iii) bond market evidence (we use HY spread tightening as proxy), (iv) equity still 30%+ off highs.",
    leverage_limit: 1.5,
    target_beta: 0.4,
    factors: { growth: 0.10, value: 0.65, momentum: 0.20, duration: 0.50, commodity: 0.10 },
    sectors: { "Distressed Credit": 0.50, "HY Bonds": 0.25, "Bank Loans": 0.15, Other: 0.10 },
    vulnerability: "Cycle-extension where stress lasts 2-3 years longer than expected; covenant erosion on the bonds.",
    aum_b: 195, founded: 1995, hq: "Los Angeles, CA",
    signature_trade: "GM 2009 senior secured at 25c → par in 18 months",
  },
  Klarman: {
    color: "#a3e635",
    icon: "🛡️",
    logo: "https://logo.clearbit.com/baupost.com",
    is_long_archetype: true,
    archetype: "The Margin-of-Safety Long",
    philosophy: "Risk = permanent loss of capital, not volatility. Demand a 30% margin of safety against your estimate of intrinsic value. Hold cash when the market doesn't offer it. Patience is a feature.",
    thesis: "On post-stress alt-credit, Klarman's filter is harsh: does the price-to-intrinsic-value gap give 30% margin of safety AFTER assuming a worst-case (further 30% drawdown plus 18-month timeline)? Most names fail. The few that pass are typically forgotten small-caps where institutional coverage has dropped, and the BSI signal-decay tells you the operational worst is behind.",
    confirms_needed: "(i) BSI z peak ≥ 2.0 with current decay to below 0.5, (ii) price down ≥ 50% from 52w high (deep enough discount), (iii) trading at Fib 61.8% or 78.6% retracement (mathematically extreme), (iv) U/D volume ratio rising (accumulation by smart money beginning).",
    leverage_limit: 1.0,
    target_beta: 0.3,
    factors: { growth: 0.05, value: 0.75, momentum: 0.10, duration: 0.30, commodity: 0.05 },
    sectors: { "Distressed Equity": 0.30, "Real Estate": 0.20, "Special Situations": 0.30, Cash: 0.20 },
    vulnerability: "Holding too much cash in bull markets and underperforming the benchmark for years.",
    aum_b: 27, founded: 1982, hq: "Boston, MA",
    signature_trade: "Lehman bonds 2008, Resona Bank 2003 — bought in the panic, held to recovery",
  },
  Greenblatt: {
    color: "#f472b6",
    icon: "🧮",
    logo: "https://logo.clearbit.com/gothamfunds.com",
    is_long_archetype: true,
    archetype: "The Magic-Formula Long",
    philosophy: "Two metrics, ranked across the universe: high earnings yield (cheap) plus high return on invested capital (good business). Top 30 names by combined rank. Hold for one year, repeat.",
    thesis: "On alt-credit recovery names, Greenblatt's quantitative test is: post-stress, does this firm rank in the top quintile on (current cash flow yield × historical ROIC)? The BSI decay tells him operational stress is fading; the magic-formula score tells him whether the underlying business was good to begin with.",
    confirms_needed: "(i) BSI z decay ≥ 1.0σ from peak, (ii) earnings yield (FCF/EV proxy) above 8%, (iii) historical 5y ROIC > 10%, (iv) price below 200-day SMA (still in the bargain bin).",
    leverage_limit: 1.0,
    target_beta: 0.95,
    factors: { growth: 0.40, value: 0.55, momentum: 0.10, duration: 0.05, commodity: 0.05 },
    sectors: { Diversified: 1.00 },
    vulnerability: "Value traps where the magic formula keeps re-selecting the same deteriorating names; momentum crashes that dominate value over short horizons.",
    aum_b: 1.5, founded: 1985, hq: "New York, NY",
    signature_trade: "Magic Formula portfolio — 30%+ annual return in book backtest 1988-2004",
  },
  Druckenmiller: {
    color: "#fbbf24",
    icon: "⚡",
    logo: "https://logo.clearbit.com/duquesnefamilyoffice.com",
    is_long_archetype: true,
    archetype: "The Asymmetric-Risk Long · Tactical Entry",
    philosophy: "Concentrate when right, cut fast when wrong. Asymmetric setups only — accept that most trades will be small losses; the one that runs makes the year. Tight stops are non-negotiable. Don't argue with the market; if the thesis breaks, exit immediately.",
    thesis: "On a recovering distressed name, Druckenmiller's question is not 'is this fundamentally cheap?' but 'is the asymmetry clean enough to size up?' He needs the recovery setup to be at high conviction (5+ of 6 conditions met), the stop placement to be tight (price within 1.5% of nearest Fib support so a hard stop is just below it), and the upside path to next major resistance to be at least 3× the stop distance. If any of those is missing, he passes — even if the name looks attractive otherwise. He'd rather miss a 50% move than chase a setup with loose risk control.",
    confirms_needed: "(i) at least 5 of 6 long-pod conditions met (high-conviction recovery setup), (ii) price within 1.5% of nearest Fibonacci support (tight-stop placement possible), (iii) Fibonacci 38.2% retracement target ≥ 9% above entry (implies ≈3:1 R:R against the ≈3% stop), (iv) BSI confirmed not in fresh-flare regime.",
    leverage_limit: 2.0,
    target_beta: 1.10,
    factors: { growth: 0.30, value: 0.20, momentum: 0.55, duration: 0.10, commodity: 0.10 },
    sectors: { "Macro / Tactical": 0.40, Tech: 0.20, Financials: 0.15, Energy: 0.15, Other: 0.10 },
    vulnerability: "Whipsaws in choppy regimes — tight stops get triggered repeatedly before the trend resolves. Concentration risk when one or two thesis trades dominate the book.",
    aum_b: 13, founded: 2010, hq: "New York, NY",
    signature_trade: "GBP short of 1992 (with Soros, +$1B in a day); 2020 NASDAQ recovery long (called it weeks before market did)",
  },

  BearWatch: {
    color: "#a78bfa",
    icon: "🐻",
    logo: "/static/apollo_hex.svg",
    parent_logo: "https://logo.clearbit.com/illinois.edu",  // styling only — UIUC where the research lives
    parent_label: "research home",
    archetype: "Our Fund · Behavioural-Stress Specialist",
    is_our_fund: true,
    philosophy: "BearWatch × Apollo Hermes is the dedicated alt-credit P-measure observatory. We aggregate eight orthogonal consumer-side stress pillars (CFPB volume, MOVE rates-vol, Reddit text, Bluesky text, app-store reviews, search-expert text, FRED macro, firm-vitality web traces) into a single EWMA-z composite, gate it through a 4-condition AND-architecture (BSI ∧ SCP ∧ MOVE ∧ CCD II), and route the signal to whichever instrument the regime favors — equity short for v1, ABS junior tranche / single-name CDS / sub-IG bond for v2.",
    thesis: "On this name, BearWatch reads the live BSI composite as the consensus of eight independent consumer-distress channels. The signal is treated as a leading indicator of risk-neutral default-probability updates rather than a direct trade trigger. Our position is conditional on three things being simultaneously true: the composite z exceeds the active mascot threshold, the war-room conviction reaches at least 3-of-5, and Apollo's 7-check risk audit returns no critical fails. We treat the war room as a peer-review layer — even our own composite signal is insufficient on its own.",
    confirms_needed: "For BearWatch to advance to Apollo execution, we need (i) BSI z above the SCOUT mascot threshold of 2.0, (ii) the SCP classifier returning phase 2, (iii) MOVE z below 1.0 (no rates regime confound), (iv) CCD II elevated for at least 2 trailing periods, and (v) at least 3 of 5 archetype peers concurring with conviction MEDIUM or higher. If the v2 fixed-income wrapper is live, we additionally check that the chosen credit instrument has a pricing source within the last 24 hours.",
    leverage_limit: 1.5,
    target_beta: -0.2,
    factors: { growth: 0.10, value: 0.55, momentum: 0.45, duration: 0.50, commodity: 0.05 },
    sectors: { "Alt-Credit ABS": 0.40, "BNPL Equity": 0.20, "Subprime Auto": 0.15, "Fintech IG CDS": 0.15, "CDX HY hedge": 0.10 },
    vulnerability: "Reflexive equity regimes (meme momentum) on names with weak credit-instrument liquidity; quiet-runoff failure modes (CURO 2024) where complaints fall pre-event.",
    aum_b: 0.0001, founded: 2026, hq: "Champaign, IL",
    signature_trade: "BSI-driven consumer-credit short — 4-of-5 historical detects, 9-month mean lead time",
  },
};

// BearWatch's "factor exposure" — our 8-pillar BSI weighted across factor space.
// CFPB-velocity is consumer-credit ≈ value/duration; macro pillar ≈ duration; soft signals ≈ momentum.
const BEARWATCH_MODEL = {
  color: "#a78bfa",
  icon: "🐻",
  name: "BearWatch × Apollo",
  factors: { growth: 0.10, value: 0.55, momentum: 0.45, duration: 0.50, commodity: 0.05 },
};

// Per-fund trade-construction logic. Given a target ticker + entry price + bsi z,
// return that fund's *suggested* trade (size, stop, target, horizon) — derived from
// their leverage limit, target-beta band, and risk-style. Used by the "Copy to Pod 3"
// flow: clicking the button populates Pod 3 with this fund's trade and runs the risk engine.
function suggestTradeForFund(fundName, ticker, entryPrice, bsiZ, side) {
  const dna = HEDGE_FUND_DNA[fundName];
  if (!dna) return null;
  side = (side || "SHORT").toUpperCase();
  const z = Math.max(1.0, Math.min(4.5, bsiZ || 2.0));

  // Per-fund position-size cap as % of book (smaller for low-beta / strict-stop funds)
  const sizeCap = {
    Renaissance:  0.07,                       // smaller, many positions
    Bridgewater:  0.04,                       // risk-parity slot
    "Two Sigma":  0.04,                       // factor-decomposed
    Millennium:   0.03,                       // hard stop-loss; single-position cap < 5%
    Citadel:      0.06,                       // high-conviction single-name
  }[fundName] || 0.05;

  // Stop-loss tightness varies by risk-management style
  const stopPct = {
    Renaissance:  0.08,    // statistical mean-reversion: wider
    Bridgewater:  0.10,    // macro positions: widest stops
    "Two Sigma":  0.07,    // factor-controlled
    Millennium:   0.05,    // strict per-pod stop
    Citadel:      0.07,    // industrialised standard
  }[fundName] || 0.079;

  // Target-distance (z-scaled, but capped per fund's typical move expectation)
  const targetMult = {
    Renaissance:  0.04,    // small mean-reversion targets
    Bridgewater:  0.07,    // big macro-cycle moves
    "Two Sigma":  0.05,    // medium factor-unwind
    Millennium:   0.04,    // tight in/out
    Citadel:      0.06,    // dispersion-trade
  }[fundName] || 0.05;

  // Hold horizon (days)
  const horizon = {
    Renaissance:  60,      // mean-reversion typically reverts within weeks
    Bridgewater:  365,     // macro-cycle plays
    "Two Sigma":  120,     // factor unwind
    Millennium:   30,      // strict turnover discipline
    Citadel:      180,     // event-window
  }[fundName] || 540;

  const stop   = side === "SHORT" ? +(entryPrice * (1 + stopPct)).toFixed(2)
                                  : +(entryPrice * (1 - stopPct)).toFixed(2);
  const target = side === "SHORT" ? +(entryPrice * (1 - targetMult * z)).toFixed(2)
                                  : +(entryPrice * (1 + targetMult * z)).toFixed(2);
  const notional = sizeCap * 100000;
  const shares   = entryPrice > 0 ? Math.floor(notional / entryPrice) : 0;
  const realNotional = shares * entryPrice;
  const rr = (Math.abs(entryPrice - target) / Math.max(Math.abs(stop - entryPrice), 0.01)).toFixed(2);

  return {
    fund: fundName, ticker, side, entry: entryPrice,
    stop, target, shares, notional: realNotional,
    horizon_days: horizon, rr,
    size_cap_pct: sizeCap * 100,
    stop_pct: stopPct * 100,
    rationale: `${fundName} sizes at ${(sizeCap*100).toFixed(0)}% (their style cap), wears a ${(stopPct*100).toFixed(0)}% stop (consistent with their ${dna.archetype.toLowerCase()} risk profile), and targets a ${(targetMult*z*100).toFixed(0)}% move scaled to z=${z.toFixed(2)}.`,
  };
}

// Cosine similarity between two factor vectors
function _cosine(a, b) {
  const va = HF_FACTOR_AXES.map(k => a[k] || 0);
  const vb = HF_FACTOR_AXES.map(k => b[k] || 0);
  const dot = va.reduce((s, x, i) => s + x * vb[i], 0);
  const na = Math.sqrt(va.reduce((s, x) => s + x * x, 0));
  const nb = Math.sqrt(vb.reduce((s, x) => s + x * x, 0));
  return (na > 0 && nb > 0) ? dot / (na * nb) : 0;
}

function _corrColor(c) {
  // c in [-1, 1]; positive = green, negative = red
  if (c >= 0.7) return "#4ade80";
  if (c >= 0.4) return "#a3e635";
  if (c >= 0.0) return "#facc15";
  return "#f87171";
}

let _hfRadarChart = null;

function openHedgeFundModal(name, opts) {
  opts = opts || {};
  const dna = HEDGE_FUND_DNA[name];
  if (!dna) return;
  const ourVote = opts.vote || "—";
  const ourRationale = opts.rationale || "—";
  const targetTicker = opts.ticker || "—";

  // Compute correlation row vs all OTHER funds (incl. BearWatch which is now in HEDGE_FUND_DNA)
  const peers = Object.keys(HEDGE_FUND_DNA).filter(k => k !== name);
  const corrRows = peers.map(p => ({
    label: p,
    icon: HEDGE_FUND_DNA[p].icon,
    color: HEDGE_FUND_DNA[p].color,
    corr: _cosine(dna.factors, HEDGE_FUND_DNA[p].factors),
    isUs: HEDGE_FUND_DNA[p].is_our_fund || false,
  }));
  corrRows.sort((a, b) => b.corr - a.corr);

  const sectorTotal = Object.values(dna.sectors).reduce((s, x) => s + x, 0);

  // Logo rendering — clearbit CDN with graceful fallback to icon emoji
  const logoBlock = dna.logo
    ? `<img src="${dna.logo}" alt="${name} logo" class="hf-logo-img" onerror="this.style.display='none'; this.parentElement.querySelector('.hf-icon-fallback').style.display='grid';">
       <div class="hf-icon hf-icon-fallback" style="background:${dna.color}; display:none;">${dna.icon}</div>`
    : `<div class="hf-icon" style="background:${dna.color};">${dna.icon}</div>`;

  // BearWatch gets a special "OUR FUND" badge + a tiny parent-org logo (styling only)
  const ourFundBadge = dna.is_our_fund
    ? `<div style="display:inline-flex;align-items:center;gap:6px;margin-top:6px;padding:3px 9px;background:rgba(167,139,250,0.15);border:1px solid rgba(167,139,250,0.4);border-radius:4px;font-size:9px;font-family:var(--font-mono);color:#c4b5fd;text-transform:uppercase;letter-spacing:0.08em;font-weight:700;">★ Our Fund</div>`
    : '';
  const parentLogoBlock = dna.parent_logo
    ? `<div style="display:flex;align-items:center;gap:8px;margin-top:8px;font-size:10px;color:var(--text-tertiary);">
         <img src="${dna.parent_logo}" alt="" style="width:18px;height:18px;object-fit:contain;border-radius:3px;background:#fff;padding:2px;" onerror="this.style.display='none'">
         <span>${dna.parent_label || 'parent'}</span>
       </div>`
    : '';

  const html = `
    <div class="hf-modal-head ${dna.is_our_fund ? 'hf-our-fund' : ''}">
      <div style="display:flex;align-items:center;gap:14px;">
        ${logoBlock}
        <div>
          <div class="hf-name">${name}</div>
          <div class="hf-arch">${dna.archetype}</div>
          <div class="hf-meta">${dna.is_our_fund ? '— · est. ' + dna.founded + ' · ' + dna.hq : '$' + dna.aum_b + 'B AUM · est. ' + dna.founded + ' · ' + dna.hq}</div>
          ${ourFundBadge}
          ${parentLogoBlock}
        </div>
      </div>
      <div class="hf-vote-pill" data-vote="${ourVote}">
        <div class="hf-vote-lbl">vote on ${targetTicker}</div>
        <div class="hf-vote-val">${ourVote}</div>
      </div>
    </div>

    <div class="hf-philosophy">
      <div class="hf-section-head">Strategy DNA</div>
      <p>${dna.philosophy}</p>
      <div class="hf-stats">
        <div class="hf-stat"><div class="lbl">Leverage limit</div><div class="val">${dna.leverage_limit}×</div></div>
        <div class="hf-stat"><div class="lbl">Target β</div><div class="val">${dna.target_beta.toFixed(2)}</div></div>
        <div class="hf-stat"><div class="lbl">Signature</div><div class="val" style="font-size:11px;">${dna.signature_trade}</div></div>
      </div>
      <div class="hf-vuln">
        <span class="hf-vuln-tag">VULNERABILITY</span>
        <span>${dna.vulnerability}</span>
      </div>
    </div>

    ${dna.thesis ? `
    <div class="hf-thesis">
      <div class="hf-section-head">Current thesis on ${targetTicker}</div>
      <p>${dna.thesis}</p>
      <div class="hf-confirms">
        <div class="hf-confirms-head">⏳ Signals still needed to confirm:</div>
        <p>${dna.confirms_needed}</p>
      </div>
    </div>
    ` : ''}

    <div class="hf-grid">

      <div class="hf-panel">
        <div class="hf-section-head">Factor exposure (radar)</div>
        <div style="height: 260px; position: relative;"><canvas id="hf-radar-${Date.now()}" data-canvas-marker></canvas></div>
        <div class="hf-foot-note">Compared to BearWatch model factor signature</div>
      </div>

      <div class="hf-panel">
        <div class="hf-section-head">Sector concentration</div>
        <div class="hf-sectors">
          ${Object.entries(dna.sectors).map(([s, w]) => `
            <div class="hf-sector-row">
              <div class="nm">${s}</div>
              <div class="bar"><div class="fill" style="width:${(w/sectorTotal*100).toFixed(0)}%;background:${dna.color};"></div></div>
              <div class="vl">${(w*100).toFixed(0)}%</div>
            </div>
          `).join('')}
        </div>
      </div>

    </div>

    <div class="hf-panel" style="margin-top:14px;">
      <div class="hf-section-head">Correlation matrix · this fund's factor vector vs peers + our model</div>
      <table class="hf-corr-tbl">
        <thead><tr><th></th><th>Peer / model</th><th class="num">cosine sim</th><th class="num">interpretation</th></tr></thead>
        <tbody>
          ${corrRows.map(r => `
            <tr ${r.isUs ? 'class="hf-us"' : ''}>
              <td style="font-size:14px;">${r.icon}</td>
              <td><strong style="color:${r.color};">${r.label}</strong></td>
              <td class="num" style="color:${_corrColor(r.corr)};font-weight:700;">${r.corr.toFixed(2)}</td>
              <td class="num" style="font-size:11px;color:var(--text-tertiary);">${
                r.corr >= 0.7 ? 'highly aligned'
                : r.corr >= 0.4 ? 'partially aligned'
                : r.corr >= 0.0 ? 'weakly correlated'
                : 'inversely positioned'
              }</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
      <div class="hf-foot-note">Cosine similarity of 5-factor vectors (growth/value/momentum/duration/commodity). +1 = identical positioning, 0 = orthogonal, −1 = mirror-opposite.</div>
    </div>

    <div class="hf-rationale">
      <div class="hf-section-head">${name}'s vote on ${targetTicker}</div>
      <div class="hf-vote-row">
        <span class="hf-vote-pill-sm" data-vote="${ourVote}">${ourVote}</span>
        <span style="font-size: 13px; color: var(--text-secondary); line-height: 1.6;">${ourRationale}</span>
      </div>
    </div>

    ${(() => {
      // ===== Trade suggestion + copy-to-pod button =====
      const px = opts.entryPrice;
      const z  = opts.bsiZ;
      if (!px || !z || ourVote !== "SHORT") {
        return `<div class="hf-trade-empty">
          <div class="hf-section-head">${name}'s trade suggestion</div>
          <div style="font-size: 11.5px; color: var(--text-tertiary);">No trade suggested — fund voted ${ourVote}${(!px ? ' · entry price unavailable' : '')}.</div>
        </div>`;
      }
      const t = suggestTradeForFund(name, targetTicker, px, z, "SHORT");
      const fmt$ = n => '$' + Math.round(n).toLocaleString();
      return `
        <div class="hf-trade-card">
          <div class="hf-trade-head">
            <div class="hf-section-head" style="margin-bottom: 0;">${name}'s trade suggestion · ${targetTicker}</div>
            <button id="hf-copy-btn" class="hf-copy-btn">Copy to Risk Engine →</button>
          </div>
          <div class="hf-trade-grid">
            <div class="hf-trade-cell"><div class="lbl">Side</div><div class="val" style="color: var(--danger);">${t.side}</div></div>
            <div class="hf-trade-cell"><div class="lbl">Entry</div><div class="val">$${t.entry.toFixed(2)}</div></div>
            <div class="hf-trade-cell"><div class="lbl">Stop</div><div class="val">$${t.stop.toFixed(2)} <span class="sub">+${t.stop_pct.toFixed(1)}%</span></div></div>
            <div class="hf-trade-cell"><div class="lbl">Target</div><div class="val">$${t.target.toFixed(2)}</div></div>
            <div class="hf-trade-cell"><div class="lbl">Size</div><div class="val">${t.shares} sh <span class="sub">${fmt$(t.notional)} (${t.size_cap_pct.toFixed(0)}%)</span></div></div>
            <div class="hf-trade-cell"><div class="lbl">R : R</div><div class="val">${t.rr} : 1</div></div>
            <div class="hf-trade-cell"><div class="lbl">Hold</div><div class="val">${t.horizon_days} d</div></div>
          </div>
          <div class="hf-trade-rationale">${t.rationale}</div>
          <div class="hf-trade-foot">
            <span class="live-dot-amber"></span>
            <span style="font-size: 10.5px; color: var(--text-tertiary);">
              "Copy to Risk Engine" populates Pod 3 with this fund's parameters and runs the 7-check Apollo audit. The trade then enters Pod 4 (Journal) on Confirm.
            </span>
          </div>
        </div>
      `;
    })()}
  `;

  // Inject into modal
  const modal = document.getElementById("hf-modal");
  const body  = document.getElementById("hf-modal-body");
  body.innerHTML = html;
  modal.style.display = "grid";
  document.body.style.overflow = "hidden";

  // Wire Copy-to-Pod-3 button (only on /demo where Pod 3 exists)
  setTimeout(() => {
    const btn = document.getElementById("hf-copy-btn");
    if (btn && opts.entryPrice && ourVote === "SHORT") {
      btn.addEventListener("click", () => {
        const t = suggestTradeForFund(name, targetTicker, opts.entryPrice, opts.bsiZ, "SHORT");
        if (typeof window.applyHedgeFundTrade === "function") {
          window.applyHedgeFundTrade(t, name);
          closeHedgeFundModal();
        } else {
          // Fallback: navigate to /demo with a query param so the demo can pick it up
          const params = new URLSearchParams({
            fund: name, ticker: targetTicker,
            entry: t.entry, stop: t.stop, target: t.target,
            shares: t.shares, horizon: t.horizon_days,
          });
          window.location.href = "/demo?hf=" + encodeURIComponent(params.toString());
        }
      });
    }
  }, 50);

  // Render radar chart (after DOM insertion)
  setTimeout(() => {
    const canvas = body.querySelector("[data-canvas-marker]");
    if (!canvas || typeof Chart === "undefined") return;
    if (_hfRadarChart) { try { _hfRadarChart.destroy(); } catch {} }
    _hfRadarChart = new Chart(canvas.getContext('2d'), {
      type: 'radar',
      data: {
        labels: HF_FACTOR_AXES.map(s => s[0].toUpperCase() + s.slice(1)),
        datasets: [
          {
            label: name,
            data: HF_FACTOR_AXES.map(k => dna.factors[k]),
            backgroundColor: dna.color + "33",
            borderColor: dna.color,
            borderWidth: 2,
            pointBackgroundColor: dna.color,
          },
          {
            label: "BearWatch model",
            data: HF_FACTOR_AXES.map(k => BEARWATCH_MODEL.factors[k]),
            backgroundColor: BEARWATCH_MODEL.color + "22",
            borderColor: BEARWATCH_MODEL.color,
            borderWidth: 2,
            borderDash: [5, 4],
            pointBackgroundColor: BEARWATCH_MODEL.color,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        scales: {
          r: {
            beginAtZero: true, max: 1.0,
            angleLines: { color: "rgba(255,255,255,0.1)" },
            grid:       { color: "rgba(255,255,255,0.08)" },
            pointLabels:{ color: "rgba(225,225,229,0.9)", font: { size: 11 } },
            ticks: { display: false, backdropColor: "transparent" },
          },
        },
        plugins: {
          legend: { labels: { color: "rgba(225,225,229,0.9)", font: { size: 11 }, boxWidth: 12 } },
          tooltip: {
            backgroundColor: "#0a0a0a", borderColor: "rgba(255,255,255,0.1)", borderWidth: 1,
            titleFont: { family: "monospace" }, bodyFont: { family: "monospace" }, padding: 8,
          },
        },
      },
    });
  }, 50);
}

function closeHedgeFundModal() {
  const modal = document.getElementById("hf-modal");
  if (modal) modal.style.display = "none";
  document.body.style.overflow = "";
}

// ESC closes
document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeHedgeFundModal(); });
// Click backdrop closes
document.addEventListener("click", (e) => {
  const m = document.getElementById("hf-modal");
  if (m && e.target === m) closeHedgeFundModal();
});
