"""
Marle-G — TRADING STRATEGIES POD.

Two things at once:
  1. A GUIDE — for each strategy: the thesis, WHEN it works and WHY, prerequisites,
     a confirmation checklist, entry/exit/risk rules, and the classic pitfalls.
  2. A LIBRARY — every strategy carries HONEST backtested stats (net of costs where we
     can run it: CAGR, win%, Sharpe, maxDD), and an evaluate(ticker) function so you can
     "try this on a stock you like" -> it checks the setup live and opens a PAPER trade.

Strategies are ranked by a defensibility score built from the backtested edge (Sharpe,
win%) — the discipline we learned in the Robustness Lab: gross numbers lie.

Asset classes: equity · options · event-driven · mutual funds · MTF (margin).
PAPER / EDUCATIONAL ONLY — never sends a real order.

  python marleg_strategies.py            # compute backtests -> marleg_strategies_bt.json
"""
import math, os, json
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import yfinance as yf
from marleg_robust_bt import ann_sharpe, maxdd_pct, TRADING

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))
BT_FILE = os.path.join(HERE, "marleg_strategies_bt.json")
PAPER_FILE = os.path.join(HERE, "marleg_strat_paper.json")
EQ_COST_BPS = 33.0      # delivery round-trip
MTF_RATE = 0.16         # ~16%/yr broker MTF interest on the borrowed leg

UNIV = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "ITC", "LT", "SBIN", "AXISBANK",
        "KOTAKBANK", "BHARTIARTL", "BAJFINANCE", "HINDUNILVR", "MARUTI", "SUNPHARMA",
        "EICHERMOT", "TATASTEEL", "M&M", "NTPC", "TITAN", "ASIANPAINT", "ULTRACEMCO",
        "WIPRO", "ADANIPORTS", "JSWSTEEL", "COALINDIA", "ONGC", "GRASIM", "HCLTECH", "CIPLA"]


# ----------------------------------------------------------------- indicators
def _daily(ticker, period="1y"):
    try:
        d = yf.download(ticker + ".NS", period=period, interval="1d",
                        auto_adjust=False, progress=False)
        if isinstance(d.columns, pd.MultiIndex):
            d.columns = d.columns.get_level_values(0)
        return d.dropna()
    except Exception:
        return pd.DataFrame()


def _rsi(close, n=14):
    d = close.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def _bb(close, n=20, k=2):
    ma = close.rolling(n).mean()
    sd = close.rolling(n).std()
    return ma - k * sd, ma, ma + k * sd


def _stats(daily_returns, n_trades=None, cost_applied=True):
    r = np.asarray(daily_returns, float)
    r = r[~np.isnan(r)]
    if len(r) < 20:
        return None
    eq = np.cumprod(1 + r)
    yrs = len(r) / TRADING
    cagr = (eq[-1] ** (1 / yrs) - 1) * 100 if eq[-1] > 0 and yrs > 0 else -100.0
    wins = float((r > 0).mean() * 100)
    return {"cagr_pct": round(cagr, 1), "sharpe": round(ann_sharpe(r), 2),
            "win_rate_pct": round(wins, 1), "maxdd_pct": round(maxdd_pct(r), 1),
            "n_days": len(r), "n_trades": n_trades, "net_of_costs": cost_applied}


# ----------------------------------------------------------------- backtests (real, net of cost)
def _load_panel(period="3y"):
    df = yf.download([s + ".NS" for s in UNIV], period=period, interval="1d",
                     group_by="ticker", auto_adjust=False, progress=False, threads=True)
    C = {}
    for s in UNIV:
        try:
            c = df[s + ".NS"]["Close"].dropna()
            if len(c) > 250:
                C[s] = c
        except Exception:
            pass
    return pd.DataFrame(C)


def bt_momentum(period="3y", topn=6, look=126, hold=21):
    """Hold the top-N names by trailing `look`-day return; rebalance every `hold` days."""
    C = _load_panel(period)
    if C.shape[1] < topn + 2:
        return None
    mom = C.pct_change(look)
    rc = C.pct_change()
    rets, held, trades = [], [], 0
    for i in range(look + 1, len(C) - 1):
        if (i - look - 1) % hold == 0:
            sig = mom.iloc[i].dropna()
            newheld = list(sig.sort_values(ascending=False).head(topn).index) if len(sig) >= topn else held
            if newheld != held:
                turn = 1.0 if not held else len(set(newheld) ^ set(held)) / (2.0 * topn)
                trades += 1
            else:
                turn = 0.0
            held = newheld
            cost = turn * EQ_COST_BPS / 1e4
        else:
            cost = 0.0
        rets.append(float(rc[held].iloc[i + 1].mean()) - cost if held else 0.0)
    return _stats(rets, trades)


def bt_meanrev(period="3y", look=200, rsi_in=32, rsi_out=55, maxhold=12):
    """Buy quality names (above 200d MA) when RSI(14) < rsi_in; exit on RSI>rsi_out or maxhold."""
    C = _load_panel(period)
    if C.shape[1] < 4:
        return None
    rc = C.pct_change()
    daily = np.zeros(len(C))
    cnt = np.zeros(len(C))
    trades = 0
    for s in C.columns:
        px = C[s].dropna()
        if len(px) < look + 30:
            continue
        rsi = _rsi(px)
        sma = px.rolling(look).mean()
        i = look + 1
        ix = px.index
        while i < len(px) - 1:
            if rsi.iloc[i] < rsi_in and px.iloc[i] > sma.iloc[i]:
                entry_i = i
                trades += 1
                for h in range(1, maxhold + 1):
                    if i + 1 >= len(px):
                        break
                    pos = ix.get_loc(px.index[i + 1])
                    daily[pos] += (px.iloc[i + 1] / px.iloc[i] - 1)
                    cnt[pos] += 1
                    i += 1
                    if rsi.iloc[i] > rsi_out:
                        break
                # entry+exit cost spread over the trade
                ec = ix.get_loc(px.index[entry_i + 1])
                daily[ec] -= EQ_COST_BPS / 1e4
            i += 1
    avg = np.divide(daily, np.maximum(cnt, 1))
    avg = avg[cnt > 0]
    return _stats(avg, trades)


def bt_volume_swing():
    """Reuse the Robustness Lab's swing result (volume-conviction, 15d hold, net)."""
    try:
        rep = json.load(open(os.path.join(HERE, "marleg_robust_bt.json")))
        sw = next((x for x in rep["results"] if x["strategy"] == "swing"), None)
        if sw:
            return {"cagr_pct": round(sw["ann_return_pct"], 1), "sharpe": sw["net_sharpe"],
                    "win_rate_pct": None, "maxdd_pct": sw["maxdd_pct"], "n_days": rep["n_obs"],
                    "n_trades": None, "net_of_costs": True,
                    "note": "from Robustness Lab — net of costs, DSR-checked"}
    except Exception:
        pass
    return None


def bt_mtf_from(momentum_stats):
    """MTF = the momentum strategy on ~2x leverage minus the borrow interest drag."""
    if not momentum_stats:
        return None
    lev = 2.0
    cagr = momentum_stats["cagr_pct"] * lev - MTF_RATE * 100 * (lev - 1)
    return {"cagr_pct": round(cagr, 1), "sharpe": momentum_stats["sharpe"],   # Sharpe ~invariant to leverage
            "win_rate_pct": momentum_stats.get("win_rate_pct"),
            "maxdd_pct": round(momentum_stats["maxdd_pct"] * lev, 1), "n_days": momentum_stats["n_days"],
            "n_trades": momentum_stats.get("n_trades"), "net_of_costs": True,
            "note": "momentum at 2x MTF, minus ~16%/yr interest; drawdown also doubles"}


# ----------------------------------------------------------------- live evaluators (try on a stock)
def ev_momentum(ticker):
    d = _daily(ticker, "1y")
    if len(d) < 210:
        return {"fires": False, "reasons": ["not enough history"]}
    c = d["Close"]
    px = float(c.iloc[-1])
    sma50, sma200 = float(c.rolling(50).mean().iloc[-1]), float(c.rolling(200).mean().iloc[-1])
    hi52 = float(c.tail(252).max())
    mom6 = float(c.iloc[-1] / c.iloc[-126] - 1) * 100
    reasons, ok = [], []
    (ok if px > sma50 else reasons).append("above 50d MA" if px > sma50 else "below 50d MA")
    (ok if px > sma200 else reasons).append("above 200d MA" if px > sma200 else "below 200d MA")
    near = px >= 0.85 * hi52
    (ok if near else reasons).append(f"within 15% of 52w high ({px/hi52*100:.0f}%)" if near else f"far from 52w high ({px/hi52*100:.0f}%)")
    (ok if mom6 > 0 else reasons).append(f"+{mom6:.0f}% 6m" if mom6 > 0 else f"{mom6:.0f}% 6m")
    fires = px > sma50 and px > sma200 and near and mom6 > 0
    return {"fires": fires, "score": int(40 + min(60, max(0, mom6))), "checks_passed": ok, "reasons": reasons,
            "entry": round(px, 2), "stop": round(min(px * 0.92, sma50), 2),
            "target": round(px * 1.18, 2), "horizon_days": 40}


def ev_meanrev(ticker):
    d = _daily(ticker, "1y")
    if len(d) < 210:
        return {"fires": False, "reasons": ["not enough history"]}
    c = d["Close"]
    px = float(c.iloc[-1])
    rsi = float(_rsi(c).iloc[-1])
    lo, mid, _ = _bb(c)
    lo, mid = float(lo.iloc[-1]), float(mid.iloc[-1])
    sma200 = float(c.rolling(200).mean().iloc[-1])
    quality = px > sma200
    fires = rsi < 35 and px < lo * 1.01 and quality
    reasons = []
    if rsi >= 35: reasons.append(f"RSI {rsi:.0f} not oversold (<35)")
    if px >= lo * 1.01: reasons.append("not at lower Bollinger band")
    if not quality: reasons.append("below 200d MA — not a quality dip")
    return {"fires": fires, "score": int(max(0, 70 - rsi)), "reasons": reasons,
            "entry": round(px, 2), "stop": round(px * 0.94, 2), "target": round(mid, 2),
            "horizon_days": 10, "rsi": round(rsi, 1)}


def ev_short_premium(ticker):
    """Vol-selling check: is realized vol elevated (rich premium to sell)?"""
    d = _daily(ticker, "1y")
    if len(d) < 60:
        return {"fires": False, "reasons": ["not enough history"]}
    r = d["Close"].pct_change().dropna()
    rv20 = float(r.tail(20).std() * math.sqrt(252) * 100)
    rv_year = r.rolling(20).std().dropna() * math.sqrt(252) * 100
    pct = float((rv_year < rv20).mean() * 100)
    fires = pct > 60
    return {"fires": fires, "score": int(pct), "reasons": [] if fires else [f"realized vol only at {pct:.0f}th pct — premium not rich"],
            "note": f"RV20 {rv20:.0f}% ({pct:.0f}th pct). Sell an OTM strangle ~1 SD wide; single-stock options are MONTHLY in India.",
            "horizon_days": 25}


def ev_event_drift(ticker):
    """Post-event drift proxy: a recent up-gap on a volume surge."""
    d = _daily(ticker, "6mo")
    if len(d) < 30:
        return {"fires": False, "reasons": ["not enough history"]}
    o, c, v = d["Open"], d["Close"], d["Volume"]
    gap = float(o.iloc[-1] / c.iloc[-2] - 1) * 100
    vspike = float(v.iloc[-1] / v.tail(20).mean())
    fires = gap > 2 and vspike > 1.5
    reasons = []
    if gap <= 2: reasons.append(f"no fresh up-gap (last gap {gap:+.1f}%)")
    if vspike <= 1.5: reasons.append(f"volume only {vspike:.1f}x — no event footprint")
    return {"fires": fires, "score": int(min(100, gap * 10 + vspike * 10)), "reasons": reasons,
            "entry": round(float(c.iloc[-1]), 2), "stop": round(float(c.iloc[-1]) * 0.95, 2),
            "target": round(float(c.iloc[-1]) * 1.10, 2), "horizon_days": 20}


def ev_guide_only(_ticker):
    return {"fires": False, "guide_only": True,
            "reasons": ["this strategy isn't single-stock — see its dedicated pod"]}


# ----------------------------------------------------------------- the catalog (playbooks)
STRATEGIES = {
    "momentum_breakout": {
        "name": "Momentum Breakout", "asset": "equity", "difficulty": "beginner",
        "tagline": "Buy strength near 52-week highs and ride the trend.",
        "thesis": "Winners keep winning over 3–12 months — institutional accumulation and "
                  "under-reaction to good news create persistent drift.",
        "when_works": ["Trending / risk-on markets (regime gauge high)",
                       "Broad participation — many names making new highs",
                       "Low-to-moderate volatility (clean trends, not whipsaws)"],
        "why": "Documented cross-sectional momentum premium (Jegadeesh–Titman). Flows and "
               "slow information diffusion push winners higher before mean-reversion sets in.",
        "prereqs": ["A cash equity account", "Liquid large/mid-caps (avoid illiquid spikes)",
                    "Discipline to cut losers fast (momentum crashes are violent)"],
        "confirm": ["Price above both 50d and 200d moving averages",
                    "Within ~15% of the 52-week high", "Positive 6-month return",
                    "Rising volume on up-days (accumulation, not a low-volume drift)"],
        "entry": "Buy on a breakout / pullback to the 50d MA with the trend intact.",
        "exit": "Trail a stop under the 50d MA or swing low; take partials at +15–20%.",
        "risk": "Hard stop ~8% below entry. Size so one loss is ≤1–2% of capital.",
        "pitfalls": ["Chasing low-volume spikes (see Quality pod — fades)",
                     "Holding through a regime flip — momentum crashes in reversals"],
        "bt": bt_momentum, "ev": ev_momentum,
    },
    "mean_reversion": {
        "name": "Mean-Reversion Bounce", "asset": "equity", "difficulty": "intermediate",
        "tagline": "Buy quality names on a panic dip to the lower band; sell the snap-back.",
        "thesis": "Short-term oversold extremes in fundamentally sound stocks tend to revert.",
        "when_works": ["Range-bound / choppy markets", "A quality name sold off on no structural news",
                       "Elevated short-term fear (RSI < 35, price at lower Bollinger)"],
        "why": "Liquidity-driven selling overshoots fair value; once forced sellers clear, "
               "price snaps back to the mean. Works ONLY on quality (above 200d MA).",
        "prereqs": ["Cash account", "A watchlist of quality names you'd own anyway",
                    "Patience — you're catching a falling knife with a glove on"],
        "confirm": ["RSI(14) below 35", "Price at/below the lower Bollinger band",
                    "Still above the 200d MA (uptrend intact — a dip, not a breakdown)",
                    "No earnings miss / structural bad news driving the drop"],
        "entry": "Buy in tranches as RSI dips; don't catch it all at once.",
        "exit": "Sell into the bounce to the 20d mid-band, or after ~10 days.",
        "risk": "Stop ~6% below entry. If it breaks the 200d MA, the thesis is dead — exit.",
        "pitfalls": ["Buying mean-reversion on a real breakdown (no 200d filter)",
                     "Averaging down without a stop — the knife wins"],
        "bt": bt_meanrev, "ev": ev_meanrev,
    },
    "volume_swing": {
        "name": "Volume-Conviction Swing", "asset": "equity", "difficulty": "intermediate",
        "tagline": "Hold names with strong up/down volume conviction for ~3 weeks.",
        "thesis": "Sustained buying volume (up-vol >> down-vol) flags accumulation ahead of a move.",
        "when_works": ["Stock-specific accumulation phases", "Positional (~15–21 day) horizon, NOT intraday"],
        "why": "Volume precedes price — but the edge is overnight/positional. The Robustness Lab "
               "showed the DAILY version is a cost trap; only the low-turnover swing is viable.",
        "prereqs": ["Cash account", "Acceptance that net-of-cost this is ~flat — a teaching example"],
        "confirm": ["20d up-volume / down-volume ratio > 1.3", "Liquidity (turnover) adequate",
                    "Not a one-day fade-prone spike (cross-check the Quality pod)"],
        "entry": "Enter the basket of top-conviction names; rebalance every ~15 days.",
        "exit": "Rebalance out names that lose conviction; ~3-week hold.",
        "risk": "Diversify across 5–8 names; this is a weak standalone edge — combine, don't bet.",
        "pitfalls": ["Trading it daily (costs destroy it — see Robustness Lab)",
                     "Mistaking a misleading spike for accumulation"],
        "bt": bt_volume_swing, "ev": ev_momentum,
    },
    "short_strangle": {
        "name": "Short Strangle (Sell Vol)", "asset": "options", "difficulty": "advanced",
        "tagline": "Sell OTM calls + puts to harvest the volatility risk premium.",
        "thesis": "Implied vol usually prints above realized vol; selling that gap pays you to wait.",
        "when_works": ["IV elevated vs realized (rich premium)", "Range-bound / mean-reverting underlying",
                       "After a vol spike that's fading (sell into elevated IV)"],
        "why": "The variance risk premium: option buyers overpay for insurance. You collect theta; "
               "your enemy is a large gap. Define risk or size tiny.",
        "prereqs": ["F&O-approved account + margin", "Strict loss discipline (undefined risk!)",
                    "Understanding of Greeks (delta/gamma/vega/theta)"],
        "confirm": ["India VIX / IV in the upper half of its range", "No binary event before expiry",
                    "Realized vol < implied vol", "Liquid strikes (tight bid/ask)"],
        "entry": "Sell ~1 SD OTM call + put for the monthly expiry (single-stock = monthly in India).",
        "exit": "Buy back at ~50% max profit, or roll/cut if tested. Never hold a loser to expiry.",
        "risk": "Add long wings (-> iron condor) to cap tail risk. Costs/slippage matter — see Robustness Lab.",
        "pitfalls": ["Selling cheap vol (IV < RV)", "Holding through earnings/events",
                     "Ignoring that gross Sharpe ≠ net — option spreads are wide"],
        "bt": None, "ev": ev_short_premium,
        "bt_note": "Index-level sim lives in the Nifty-Sim pod (VRP-filtered short strangle). "
                   "Treat its Sharpe as GROSS — option bid/ask haircuts it hard.",
    },
    "long_straddle_event": {
        "name": "Long Straddle (Event)", "asset": "options", "difficulty": "advanced",
        "tagline": "Buy a call + put before a known catalyst; profit from a big move either way.",
        "thesis": "When a binary event will move the stock more than the priced-in move, buy vol.",
        "when_works": ["Before earnings/policy/result with a fat tail", "IV still cheap vs the expected move",
                       "Historically large post-event gaps in this name"],
        "why": "If the realized move exceeds the straddle's breakevens, you win regardless of direction. "
               "The trap: IV is usually JACKED before known events (you overpay).",
        "prereqs": ["F&O account", "Event calendar", "An estimate of the historical event move vs the priced move"],
        "confirm": ["A dated catalyst before expiry", "Straddle breakeven < typical historical move",
                    "IV not already at extreme highs", "Liquid ATM strikes"],
        "entry": "Buy the ATM call + put a few days before the event.",
        "exit": "Close right after the event on the IV spike/move — don't let theta + IV-crush bleed it.",
        "risk": "Max loss = premium paid. Position small; most event straddles lose to IV crush.",
        "pitfalls": ["Buying when IV is already extreme (crush eats the move)",
                     "Holding past the event into theta decay"],
        "bt": None, "ev": ev_short_premium,   # same RV-percentile read, inverted in the UI note
        "bt_note": "Illustrative — a proper backtest needs an event calendar + per-name IV history (roadmap).",
    },
    "earnings_drift": {
        "name": "Post-Earnings Drift", "asset": "event", "difficulty": "intermediate",
        "tagline": "Ride the drift after a strong earnings surprise + volume confirmation.",
        "thesis": "Markets under-react to earnings surprises; price drifts in the surprise direction for weeks.",
        "when_works": ["Large positive surprise + a gap up on heavy volume",
                       "Analyst upgrades following the print", "Strong sector tailwind"],
        "why": "PEAD (post-earnings-announcement drift) — one of the most robust anomalies. "
               "Slow information diffusion + analyst anchoring.",
        "prereqs": ["Cash account", "Earnings calendar + surprise data", "Speed — the first days matter"],
        "confirm": ["Beat on revenue AND EPS", "Gap up > 2% on > 1.5x volume",
                    "Close near the day's high (buyers in control)", "No guidance cut"],
        "entry": "Buy in the 1–3 days after the gap, on the first orderly pullback.",
        "exit": "Hold ~15–20 trading days; trail a stop. Exit on a close back into the gap.",
        "risk": "Stop below the earnings-day low. The gap failing is the tell — get out.",
        "pitfalls": ["Chasing the gap candle (entry too high)", "Fading a negative surprise too early"],
        "bt": None, "ev": ev_event_drift,
        "bt_note": "Illustrative — a clean backtest needs an earnings-date + surprise dataset (roadmap).",
    },
    "cascade_basket": {
        "name": "Event Cascade Basket", "asset": "event", "difficulty": "advanced",
        "tagline": "An event hits one industry; trade the long/short basket it propagates to.",
        "thesis": "A shock (oil, war, policy) ripples through the supply chain on a predictable lag.",
        "when_works": ["A clear catalyst with known winners/losers down the chain",
                       "The propagation hasn't fully priced in yet (the lag is the edge)"],
        "why": "Markets price the obvious first-order name fast but lag the 2nd/3rd-tier effects — "
               "the Cascade pod maps those tiers into a long/short basket.",
        "prereqs": ["The Cascade pod's taxonomy", "Ability to trade a basket (multiple names)"],
        "confirm": ["A live event in the Thesis Ledger / Cascade engine",
                    "Tier-1 names already moved (confirms the cascade is real)",
                    "Tier-2/3 names still lagging (the opportunity)"],
        "entry": "Long the beneficiaries, short the victims, sized by cascade strength.",
        "exit": "Close as the lag closes (the cascade event-study estimates the half-life).",
        "risk": "Market-neutral the basket to isolate the cascade; cap single-name weight.",
        "pitfalls": ["Trading after the cascade has fully propagated", "Ignoring a regime that overwhelms the event"],
        "bt": None, "ev": ev_guide_only,
        "bt_note": "Historical event-study lives in the Cascade pod (#7). See it for per-event P&L.",
    },
    "fund_momentum": {
        "name": "Mutual-Fund Momentum Rotation", "asset": "mutual_fund", "difficulty": "beginner",
        "tagline": "Rotate your SIP into the funds with the strongest trailing risk-adjusted returns.",
        "thesis": "Fund-level performance persists over 6–12 months within a category.",
        "when_works": ["Long-horizon investors (SIP/lump-sum)", "Clear category leaders pulling ahead"],
        "why": "Manager skill + style tailwinds persist medium-term. Cheaper and calmer than stock-picking; "
               "the edge is consistency + low cost, not timing.",
        "prereqs": ["An MF/demat account or AMC access", "A long horizon (tax: LTCG > 1yr equity)",
                    "Awareness of exit loads + expense ratios"],
        "confirm": ["Top-quartile 1y AND 3y return vs category", "Consistent rolling returns (not one lucky year)",
                    "Reasonable expense ratio", "Stable fund manager"],
        "entry": "Direct your monthly SIP to the top-ranked funds; review quarterly.",
        "exit": "Rotate when a fund drops below median for 2+ quarters (mind exit load + LTCG).",
        "risk": "Don't over-rotate (taxes + loads). Diversify across 3–4 funds/categories.",
        "pitfalls": ["Chasing last year's #1 (often reverts)", "Ignoring expense ratio + exit load drag"],
        "bt": None, "ev": ev_guide_only,
        "bt_note": "See the Funds pod for live fund rankings + the full STCG/LTCG tax engine.",
    },
    "mtf_leveraged_swing": {
        "name": "MTF Leveraged Swing", "asset": "mtf", "difficulty": "advanced",
        "tagline": "Run the momentum swing on ~2x margin — amplified gains AND interest drag.",
        "thesis": "Margin Trading Facility lets you hold ~2x; on a positive-edge swing it scales returns.",
        "when_works": ["A high-conviction momentum setup", "Strong trend + low expected drawdown",
                       "When the edge per trade clearly exceeds the ~16%/yr borrow cost"],
        "why": "Leverage multiplies a real edge — but also the drawdown, AND you pay daily interest "
               "(~16%/yr) on the borrowed leg. Only worth it when the edge is strong and quick.",
        "prereqs": ["MTF-enabled broker account", "Strong risk discipline (margin calls are brutal)",
                    "The math: edge per holding period > interest for that period"],
        "confirm": ["The underlying Momentum Breakout setup fires (all its checks)",
                    "Expected hold is short (interest compounds daily)", "Liquid name (margin-eligible list)"],
        "entry": "Take the momentum entry at ~2x; pledge as required.",
        "exit": "Same as momentum, but tighter — interest is a clock ticking against you.",
        "risk": "Drawdown DOUBLES. A 8% stop on 2x = 16% of your equity. Halve your position count.",
        "pitfalls": ["Holding too long (interest erodes the edge)", "Forgetting drawdown scales with leverage"],
        "bt": "mtf", "ev": ev_momentum,   # bt resolved from momentum in compute_backtests
        "bt_note": "Derived: momentum at 2x minus ~16%/yr MTF interest; drawdown also scales 2x.",
    },
}

ASSET_ORDER = {"equity": 0, "options": 1, "event": 2, "mutual_fund": 3, "mtf": 4}


# ----------------------------------------------------------------- compute + serve
def compute_backtests():
    out, mom = {}, None
    for sid, s in STRATEGIES.items():
        fn = s.get("bt")
        try:
            if fn is None:
                out[sid] = None
            elif fn == "mtf":
                out[sid] = bt_mtf_from(mom)
            else:
                res = fn()
                out[sid] = res
                if sid == "momentum_breakout":
                    mom = res
        except Exception as e:
            out[sid] = {"error": str(e)[:120]}
    json.dump(out, open(BT_FILE, "w"), indent=1)
    return out


def _bt_cache():
    try:
        return json.load(open(BT_FILE))
    except Exception:
        return {}


def _defensibility(stats):
    if not stats or "sharpe" not in stats or stats.get("sharpe") is None:
        return 0
    sh = stats["sharpe"]
    wr = stats.get("win_rate_pct") or 50
    return round(max(0, min(100, 50 + sh * 20 + (wr - 50) * 0.5)))


def catalog():
    bt = _bt_cache()
    rows = []
    for sid, s in STRATEGIES.items():
        stats = bt.get(sid)
        rows.append({
            "id": sid, "name": s["name"], "asset": s["asset"], "difficulty": s["difficulty"],
            "tagline": s["tagline"], "thesis": s["thesis"],
            "tradeable": s["ev"] is not ev_guide_only,
            "stats": stats, "bt_note": s.get("bt_note"),
            "defensibility": _defensibility(stats),
        })
    rows.sort(key=lambda r: (-(r["defensibility"]), ASSET_ORDER.get(r["asset"], 9)))
    return {"strategies": rows, "n": len(rows),
            "asof": datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
            "note": "Ranked by a defensibility score from net-of-cost backtested Sharpe + win%. "
                    "Paper / educational only."}


def detail(sid):
    s = STRATEGIES.get(sid)
    if not s:
        return {"error": "unknown strategy"}
    bt = _bt_cache().get(sid)
    d = {k: v for k, v in s.items() if k not in ("bt", "ev")}
    d.update({"id": sid, "stats": bt, "defensibility": _defensibility(bt),
              "tradeable": s["ev"] is not ev_guide_only})
    return d


def evaluate(sid, ticker):
    s = STRATEGIES.get(sid)
    if not s:
        return {"error": "unknown strategy"}
    ticker = (ticker or "").upper().replace(".NS", "").strip()
    if not ticker:
        return {"error": "no ticker"}
    res = s["ev"](ticker)
    res.update({"strategy": sid, "name": s["name"], "ticker": ticker})
    return res


# ----------------------------------------------------------------- paper trading
def _paper():
    try:
        return json.load(open(PAPER_FILE))
    except Exception:
        return {"cash": 100000.0, "start": 100000.0, "positions": [], "closed": [], "log": []}


def _save_paper(b):
    try:
        json.dump(b, open(PAPER_FILE + ".tmp", "w"), indent=1)
        os.replace(PAPER_FILE + ".tmp", PAPER_FILE)
    except Exception:
        pass


def paper_trade(sid, ticker, alloc=0.15):
    s = STRATEGIES.get(sid)
    if not s:
        return {"error": "unknown strategy"}
    ev = evaluate(sid, ticker)
    if ev.get("guide_only"):
        return {"opened": False, "guide_only": True, "eval": ev,
                "msg": f"{s['name']} isn't a single-stock strategy — open its dedicated pod."}
    if s["asset"] == "options" or not ev.get("entry"):
        # options express a VOL signal, not a stock buy — paper option-legs are roadmap
        return {"opened": False, "signal": True, "fires": bool(ev.get("fires")), "eval": ev,
                "msg": ev.get("note") or ("Premium is rich — consider selling vol." if ev.get("fires")
                                          else "Premium not rich enough right now.")}
    if not ev.get("fires"):
        return {"opened": False, "eval": ev, "msg": "Setup not confirmed right now — conditions unmet."}
    b = _paper()
    px = ev.get("entry") or 0
    if px <= 0:
        return {"opened": False, "eval": ev, "msg": "no entry price"}
    spend = b["cash"] * alloc
    qty = int(spend // px)
    if qty < 1:
        return {"opened": False, "eval": ev, "msg": "insufficient paper cash for one share"}
    side = "LONG"
    pos = {"id": f"{sid}-{ev['ticker']}-{len(b['positions'])+len(b['closed'])+1}",
           "strategy": sid, "name": s["name"], "sym": ev["ticker"], "side": side, "qty": qty,
           "entry": px, "stop": ev.get("stop"), "target": ev.get("target"),
           "horizon_days": ev.get("horizon_days"), "opened": datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
           "why": "; ".join(ev.get("checks_passed", []) or [s["tagline"]])}
    b["cash"] -= qty * px
    b["positions"].append(pos)
    b["log"] = (b.get("log", []) + [f"{pos['opened']} OPEN {s['name']} {qty} {ev['ticker']} @ {px}"])[-80:]
    _save_paper(b)
    return {"opened": True, "position": pos, "eval": ev, "cash_left": round(b["cash"], 1)}


def _mark(b):
    syms = list({p["sym"] for p in b["positions"]})
    px = {}
    if syms:
        try:
            d = yf.download([s + ".NS" for s in syms], period="1d", interval="1d",
                            group_by="ticker", progress=False, threads=True)
            for s in syms:
                try:
                    px[s] = float(d[s + ".NS"]["Close"].dropna().iloc[-1])
                except Exception:
                    pass
        except Exception:
            pass
    up = 0.0
    for p in b["positions"]:
        now = px.get(p["sym"], p["entry"])
        p["now"] = round(now, 2)
        p["upnl"] = round((now - p["entry"]) * p["qty"], 1)
        p["upnl_pct"] = round((now / p["entry"] - 1) * 100, 2)
        up += p["upnl"]
    invested = sum(p["entry"] * p["qty"] for p in b["positions"])
    realized = sum(c.get("pnl", 0) for c in b["closed"])
    b["equity"] = round(b["cash"] + invested + up, 1)
    b["ret_pct"] = round((b["equity"] / b["start"] - 1) * 100, 2)
    b["open_upnl"] = round(up, 1)
    b["realized"] = round(realized, 1)
    return b


def paper_book():
    return _mark(_paper())


def main():
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print("Computing strategy backtests (net of costs)…")
    bt = compute_backtests()
    cat = catalog()
    print(f"\n{cat['n']} strategies — ranked by defensibility:\n")
    for r in cat["strategies"]:
        st = r["stats"] or {}
        sh = st.get("sharpe"); wr = st.get("win_rate_pct"); cg = st.get("cagr_pct")
        line = f"  [{r['defensibility']:>3}] {r['name']:<28} {r['asset']:<12}"
        if sh is not None:
            line += f" Sharpe {sh:>5} · CAGR {cg}% · win {wr}%"
        else:
            line += "  (guide + playbook; see note)"
        print(line)
    print(f"\n[wrote {os.path.basename(BT_FILE)}]")


if __name__ == "__main__":
    main()
