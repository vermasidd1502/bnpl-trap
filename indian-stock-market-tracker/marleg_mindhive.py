"""
Marle-G — MINDHIVE: the local, zero-API scenario brain ("the mind").

The pod has four layers that each answer a different question:
    MACRO  -> regime dial      : is this a stock-picker's market, or macro-coherent?
    SECTOR -> event cascade     : if X happens, who wins / who loses, 5 tiers deep?
    STRUCT -> thesis ledger      : which grey-swans are mispriced (adoption vs the market)?
    FIRM   -> business agent     : does a moat let a name OVERRIDE its sector signal?

Mindhive FUSES them into one queryable mind that "journals the story": it synthesizes
the current state, persists it as a local memory-palace (rooms/drawers/tunnels in
marleg_mindhive_memory.json), auto-refreshes on a TTL, and answers natural questions
by routing intent -> the right layer -> a templated, evidence-grounded answer.

Design rule: DETERMINISTIC + LOCAL = exactly Rs.0 in API cost. Every answer is built
from the pod's own computed signals, never an LLM hallucination. An optional local-LLM
phrasing layer (Ollama/llama.cpp) is a switch (MARLEG_MINDHIVE_LLM) that is OFF by
default — none is installed, and we don't want the spend.

  python marleg_mindhive.py "what's the story"
  python marleg_mindhive.py "what if oil spikes"
  python marleg_mindhive.py "tell me about INDIGO"
"""
import os, sys, json, re, time
import marleg_regime as regime
import marleg_cascade as cascade
import marleg_thesis as thesis
# business + smartmoney are heavy (yfinance peer loops) -> imported lazily inside _answer_firm

HERE = os.path.dirname(os.path.abspath(__file__))
MEM = os.path.join(HERE, "marleg_mindhive_memory.json")
TTL = 3 * 3600                       # the mind re-synthesizes at most every 3h (survives restarts)
TAX = json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json")))
BY_SYM, BY_IND = TAX["by_symbol"], TAX["by_industry"]

ACTIVE_EVENTS = ["iran_war", "andaman_gas"]      # the live scenarios in play (featured in the story)
# Hybrid brain: local Ollama (default) -> Claude API (hard ones) -> deterministic. All optional + graceful.
OLLAMA_URL = os.environ.get("MARLEG_OLLAMA", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("MARLEG_OLLAMA_MODEL", "llama3.2")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.environ.get("MARLEG_CLAUDE_MODEL", "claude-3-5-haiku-20241022")

# event-specific "what's really going on" colour (the cross-currents the user cares about)
EVENT_NOTES = {
    "iran_war": ("Parallel cross-currents: safe-haven bid, Hormuz freight and supply fear push "
                 "Defense, Shipping, Metals and Specialty Chemicals UP — even as the demand-shock side "
                 "pressures Refiners and Airlines. This is NOT a one-way oil trade."),
    "andaman_gas": ("A domestic gas discovery re-rates the gas value chain (city-gas, pipelines, upstream) "
                    "and eases the import bill (rupee-supportive) — a structural slow-burn re-rating, not a shock."),
    "oil_shock_up": ("Upstream/integrated producers win on realisation; refiners, airlines, paints and tyres "
                     "(crude-derivative inputs) get squeezed."),
    "rate_hike": ("Lenders' NIMs and rate-sensitive demand (autos, real-estate, NBFCs) take the hit; "
                  "cash-rich, low-leverage compounders are relatively insulated."),
}


# ---------------------------------------------------------------- the hybrid brain (local Ollama -> Claude API)
def _phrase(text):
    """Deterministic answers pass through untouched (fast + grounded); the LLM is used for OPEN questions."""
    return text


def llm_available():
    import requests
    try:
        if requests.get(f"{OLLAMA_URL}/api/tags", timeout=2).status_code == 200:
            return True
    except Exception:
        pass
    return bool(ANTHROPIC_KEY)


def _llm(prompt, system=None, prefer_api=False):
    """Return (text, engine): try local Ollama, then Claude API; ('', 'none') if neither is reachable."""
    import requests
    system = system or ("You are Mindhive, a grounded India-market analyst inside a quant pod. Use ONLY the CONTEXT — "
                        "never invent numbers. Be concise and opinionated; for a stock end with LONG / SHORT / NEUTRAL. "
                        "Research, not financial advice.")
    if not prefer_api:
        try:
            r = requests.post(f"{OLLAMA_URL}/api/generate",
                              json={"model": OLLAMA_MODEL, "system": system, "prompt": prompt, "stream": False},
                              timeout=90)
            if r.status_code == 200:
                t = (r.json().get("response") or "").strip()
                if t:
                    return t, "ollama:" + OLLAMA_MODEL
        except Exception:
            pass
    if ANTHROPIC_KEY:
        try:
            r = requests.post("https://api.anthropic.com/v1/messages",
                              headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                                       "content-type": "application/json"},
                              json={"model": CLAUDE_MODEL, "max_tokens": 700, "system": system,
                                    "messages": [{"role": "user", "content": prompt}]}, timeout=45)
            if r.status_code == 200:
                t = "".join(b.get("text", "") for b in r.json().get("content", [])).strip()
                if t:
                    return t, "claude-api"
        except Exception:
            pass
    return "", "none"


# ---------------------------------------------------------------- knowledge graph (the tunnels)
def build_kg():
    """Static structural graph behind the mind: event -> factor, factor/industry -> industry, thesis -> industry.
    This is the 'memory palace' wiring — browsable + traversable, sourced from the cascade graph + theses."""
    edges = []
    for ek, ev in cascade.EVENTS.items():
        for f, w in ev.get("seeds", []):
            edges.append({"from": ek, "ftype": "event", "rel": "drives", "to": f, "ttype": "factor", "w": w})
    for s, d, w, mech in cascade.EDGES:
        edges.append({"from": s, "ftype": "factor", "rel": "propagates", "to": d, "ttype": "industry",
                      "w": w, "mech": mech})
    for tk, t in thesis.THESES.items():
        for ind in t.get("winners", []):
            edges.append({"from": tk, "ftype": "thesis", "rel": "long", "to": ind, "ttype": "industry", "w": 1})
        for ind in t.get("losers", []):
            edges.append({"from": tk, "ftype": "thesis", "rel": "short", "to": ind, "ttype": "industry", "w": -1})
    return edges


def neighbors(node, kg=None):
    """Follow the tunnels out of a node (for 'what connects to X')."""
    kg = kg or build_kg()
    n = node.strip()
    return [e for e in kg if e["from"].lower() == n.lower() or e["to"].lower() == n.lower()]


# ---------------------------------------------------------------- synthesis (the auto-updating mind state)
def synthesize(force=False):
    if not force:
        try:
            d = json.load(open(MEM))
            if time.time() - d.get("_ts", 0) < TTL:
                return d
        except Exception:
            pass
    reg = regime.compute()                                  # disk-cached 6h
    led = thesis.ledger(with_momentum=True)                 # grey-swan ledger w/ basket momentum
    evs = []
    for e in ACTIVE_EVENTS:
        try:
            evs.append(cascade.build_cascade(e))
        except Exception:
            pass
    kg = build_kg()
    state = {
        "_ts": time.time(), "asof": reg.get("asof"),
        "regime": reg, "ledger": led, "events": evs, "active_events": ACTIVE_EVENTS,
        "kg": kg, "kg_stats": {"edges": len(kg),
                               "events": len(cascade.EVENTS), "theses": len(thesis.THESES),
                               "industries": len(BY_IND), "firms": len(BY_SYM)},
    }
    state["story"] = _story_text(state)
    try:
        json.dump(state, open(MEM, "w"))
    except Exception:
        pass
    return state


# ---------------------------------------------------------------- small helpers
def _has(q, words):
    pat = r"\b(" + "|".join(re.escape(w) for w in words) + r")\b"
    return re.search(pat, q.lower()) is not None


def _deploy_line(g):
    if g >= 60:
        return "stock-picker's market — DEPLOY single-name theses (scenario-alpha is LIVE)"
    if g <= 35:
        return "macro-coherent — names move together; single-name theses are DORMANT, wait"
    return "transitional — partial edge; be selective, lean on the highest-conviction names"


def _legs(ev, side, n=5):
    return [lg for lg in ev["legs"] if lg["side"] == side][:n]


THESIS_KW = {
    "ai_datacenter": ["ai", "a.i.", "data center", "datacenter", "data-centre", "data centre", "gpu", "compute"],
    "water_scarcity": ["water", "drought", "irrigation", "scarcity"],
    "housing_cycle": ["housing", "real estate", "realty", "property", "mortgage", "home loan"],
}
EVENT_KW = {
    "oil_shock_up": ["oil spike", "oil up", "crude spike", "crude up", "oil shock", "oil rally", "fuel spike"],
    "oil_shock_down": ["oil down", "oil crash", "crude down", "oil falls", "cheap oil"],
    "iran_war": ["iran", "hormuz", "israel", "middle east", "geopolitical", "geopolitics"],
    "andaman_gas": ["andaman", "gas discovery", "gas find", "domestic gas"],
    "war_escalation": ["war escalation", "escalation"],
    "monsoon_good": ["good monsoon", "strong monsoon", "normal monsoon"],
    "monsoon_bad": ["bad monsoon", "weak monsoon", "drought", "el nino", "el-nino"],
    "rate_hike": ["rate hike", "rate rise", "rbi hike", "higher rates", "tightening"],
    "inr_depreciation": ["rupee", "inr", "depreciation", "weak rupee"],
    "infra_push": ["infra", "infrastructure", "capex push"],
}
_STOP = {"THE", "AND", "FOR", "WHAT", "WHATS", "WHY", "HOW", "WHO", "ARE", "YOU", "TELL", "ABOUT", "ME",
         "IS", "IT", "ON", "OF", "TO", "IN", "DO", "IF", "MY", "A", "AN", "GET", "GIVE", "SHOW", "NOW"}


def _find_ticker(q):
    for t in re.findall(r"[A-Za-z][A-Za-z&-]{2,}", q.upper()):
        if t in _STOP:
            continue
        if t in BY_SYM:
            return t
    return None


def _find_thesis(q):
    ql = q.lower()
    for k in thesis.THESES:
        if k in ql:
            return k
    for k, kws in THESIS_KW.items():
        if _has(q, kws):
            return k
    return None


def _find_event(q):
    ql = q.lower()
    for k in cascade.EVENTS:
        if k in ql:
            return k
    for k, kws in EVENT_KW.items():
        if _has(q, kws):
            return k
    if _has(q, ["oil", "crude"]):
        return "oil_shock_up"
    if _has(q, ["war", "conflict"]):
        return "iran_war"
    return None


# ---------------------------------------------------------------- answer builders
def _answer_regime(state):
    r = state["regime"]
    hot = ", ".join(f"{s} ({v}%)" for s, v in (r.get("sector_dispersion") or [])[:5]) or "—"
    txt = (f"MARKET REGIME — gauge {r['gauge']}/100  ->  {r['regime']}\n"
           f"  dispersion {r['dispersion_now']}% ({r['dispersion_pctile']}th pctile) · "
           f"avg pairwise corr {r['avg_correlation']} ({r['corr_pctile']}th) · India VIX {r.get('vix')}\n"
           f"  Read: {_deploy_line(r['gauge'])}.\n"
           f"  Where the incoherence is (a thesis may be activating): {hot}.")
    return {"intent": "regime", "answer": _phrase(txt), "data": {"gauge": r["gauge"], "regime": r["regime"]}}


def _answer_thesis(key, state):
    t = thesis.analyze(key)
    if t.get("error"):
        return _help(unknown=True)
    mom = t.get("momentum") or {}
    cand = f"3m {mom.get('r3m')}% · 1y {mom.get('r1y')}%" if mom else "n/a"
    txt = (f"THESIS — {t['name']}  [{t['nature']}]\n"
           f"  adoption: {t['adoption'].upper()}  vs  market: {t['anticipation'].upper()}  ->  {t['gap_verdict']}\n"
           f"  mechanism: {t['mechanism']}\n"
           f"  payoff: {t['payoff']} · network effect: {t['network_effect']}\n"
           f"  LONG basket ({len(t['basket']['long'])}): {', '.join(t['basket']['long'][:8]) or '—'}\n"
           f"  SHORT basket ({len(t['basket']['short'])}): {', '.join(t['basket']['short'][:6]) or '—'}\n"
           f"  basket momentum (the live 'candlestick' — where the market already is): {cand}\n"
           f"  India caveat: {t['india_caveat']} · conviction: {t['conviction']}")
    return {"intent": "thesis", "answer": _phrase(txt), "data": {"key": key, "gap": t["gap"]}}


def _answer_event(key, state):
    c = cascade.build_cascade(key)
    longs = _legs(c, "LONG", 6)
    shorts = _legs(c, "SHORT", 6)
    fl = lambda L: "\n".join(f"    + {lg['industry']}  (T{lg['tier']}, {lg['impact']:+.2f}) — {lg['mechanism']}"
                            for lg in L) or "    —"
    fs = lambda L: "\n".join(f"    - {lg['industry']}  (T{lg['tier']}, {lg['impact']:+.2f}) — {lg['mechanism']}"
                            for lg in L) or "    —"
    note = EVENT_NOTES.get(key)
    txt = (f"SCENARIO — {c['label']}\n"
           f"  WINNERS (go long):\n{fl(longs)}\n"
           f"  LOSERS (go short):\n{fs(shorts)}\n"
           + (f"  Cross-current read: {note}\n" if note else "")
           + f"  Trade shape: long the top winners vs short the top losers = a market-neutral spread on the thesis.")
    return {"intent": "event", "answer": _phrase(txt), "data": {"event": key, "n_legs": len(c["legs"])}}


def _answer_firm(tk, state):
    try:
        import marleg_business as business
        b = business.analyze(tk)
    except Exception as e:
        return {"intent": "firm", "answer": f"FIRM — couldn't analyse {tk}: {e}"}
    if b.get("error"):
        return {"intent": "firm", "answer": f"FIRM — no data for {tk}."}
    c = b["components"]
    txt = (f"FIRM — {b['name']} ({b['tk']}) · {b['sector']}/{b['industry']} · {b['position']}\n"
           f"  MOAT {b['moat']}/100 [{b['moat_label']}] — pricing {c['pricing_power']} · dominance {c['dominance']} · "
           f"durability {c['durability']} · resilience {c['resilience']}\n"
           f"  sector signal: {b['sector_signal']}\n"
           f"  -> {b['divergence_verdict']}")
    try:
        import marleg_smartmoney as sm
        f = sm.flow(tk)
        if not f.get("error"):
            txt += (f"\n  smart money: {f.get('verdict')} — institutions {f.get('inst_delta_1q')}% (1Q), "
                    f"promoters {f.get('promoter_action')}")
    except Exception:
        pass
    return {"intent": "firm", "answer": _phrase(txt), "data": {"tk": tk, "moat": b["moat"], "override": b["override"]}}


def _answer_underpriced(state):
    led = sorted(state["ledger"], key=lambda t: -t.get("gap", 0))
    lines = []
    for t in led:
        mom = t.get("momentum") or {}
        lines.append(f"  • {t['name']} [{t['nature']}] -> {t['gap_verdict']}  (gap {t['gap']:+d}"
                     + (f", 1y basket {mom.get('r1y')}%" if mom.get("r1y") is not None else "") + ")")
    best = led[0] if led else None
    head = "GREY-SWAN LEDGER — structural mispricings (adoption MINUS market anticipation):\n"
    tail = (f"\n  Best edge: {best['name']} — adoption is ahead of the price, so position EARLY."
            if best and best.get("gap", 0) >= 1 else
            "\n  Nothing screaming under-priced right now; the edge is thin — wait for divergence.")
    return {"intent": "underpriced", "answer": _phrase(head + "\n".join(lines) + tail)}


def _answer_action(state):
    r = state["regime"]
    hot = ", ".join(f"{s}" for s, v in (r.get("sector_dispersion") or [])[:4]) or "—"
    led = sorted(state["ledger"], key=lambda t: -t.get("gap", 0))
    best = led[0] if led else None
    ev = state["events"][0] if state.get("events") else None
    parts = [f"WHERE THE ACTION IS — {state.get('asof')}",
             f"  Regime: {_deploy_line(r['gauge'])} (gauge {r['gauge']}).",
             f"  Live incoherence (stories breaking out): {hot}."]
    if best:
        parts.append(f"  Best structural edge: {best['name']} -> {best['gap_verdict'].split(' — ')[0]}.")
    if ev:
        L = _legs(ev, "LONG", 3); S = _legs(ev, "SHORT", 3)
        parts.append(f"  Live scenario '{ev['label']}': long {', '.join(l['industry'] for l in L)} / "
                     f"short {', '.join(s['industry'] for s in S)}.")
    parts.append("  Ask 'tell me about <ticker>' to check if a moat overrides any of these sector calls.")
    return {"intent": "action", "answer": _phrase("\n".join(parts))}


def _story_text(state):
    r = state["regime"]
    led = sorted(state["ledger"], key=lambda t: -t.get("gap", 0))
    up = led[0] if led else None
    lines = [f"THE STORY — {state.get('asof')}", ""]
    # 1) regime
    hot = ", ".join(s for s, v in (r.get("sector_dispersion") or [])[:3]) or "—"
    lines.append(f"Regime. The market is {r['regime'].split(' — ')[0]} (gauge {r['gauge']}/100) — "
                 f"{_deploy_line(r['gauge'])}. The incoherence is concentrated in {hot}, "
                 f"so that's where single-name stories are alive right now.")
    # 2) structural edge
    if up:
        mom = up.get("momentum") or {}
        where = ("the market hasn't caught on yet" if (mom.get("r1y") or 0) < 5 else "and it's starting to move")
        lines.append(f"Structural edge. The biggest mispricing is {up['name']} [{up['nature']}]: "
                     f"{up['gap_verdict'].split(' — ')[0]}. Its long basket is "
                     f"{mom.get('r1y')}% over the past year — {where}.")
    # 3) live scenarios
    for ev in state.get("events", []):
        L = _legs(ev, "LONG", 2); S = _legs(ev, "SHORT", 2)
        note = EVENT_NOTES.get(ev["event"], "")
        lines.append(f"Live scenario. {ev['label']} — the cascade favours "
                     f"{', '.join(l['industry'] for l in L) or '—'} and pressures "
                     f"{', '.join(s['industry'] for s in S) or '—'}. " + note)
    # 4) firm layer
    lines.append("Firm layer. Sector calls aren't destiny — a wide-moat, dominant name (think IndiGo in Airlines) "
                 "can rise even when its sector cascade says short. Ask me about any ticker to test that.")
    # 5) synthesis
    deploy = "lean in on the names" if r["gauge"] >= 50 else "stay patient and structural"
    lines.append(f"Net. {deploy.capitalize()}: anchor on the under-priced structural theses, "
                 f"trade the live cascades as spreads, and let firm moats veto the sector where they earn it.")
    return "\n".join(lines)


def _help(unknown=False):
    pre = "I didn't quite catch that. " if unknown else ""
    txt = (pre + "MINDHIVE — your local market brain. I fuse the regime dial, event cascades, the grey-swan "
           "thesis ledger and firm moats into one view. Try:\n"
           "  • \"what's the story\"            — the synthesized brief\n"
           "  • \"what's the regime\"           — dispersion / deploy-or-wait\n"
           "  • \"what's under-priced\"         — the grey-swan ledger (best edge)\n"
           "  • \"what if oil spikes\" / \"what if war\"  — event cascades (winners/losers)\n"
           "  • \"thesis on water\" / \"AI datacenter\"   — structural theses\n"
           "  • \"tell me about INDIGO\"        — firm moat + sector override + smart money\n"
           "Every answer is built from the pod's own computed signals — zero API cost.")
    return {"intent": "help", "answer": txt}


# ---------------------------------------------------------------- long/short opinion + RAG (the "Jarvis" layer)
def _quick_ud(tk):
    import yfinance as yf, pandas as pd
    d = yf.download(tk.upper() + ".NS", period="3mo", interval="1d", progress=False, auto_adjust=False)
    if d is None or d.empty:
        return None
    if isinstance(d.columns, pd.MultiIndex):
        d.columns = d.columns.get_level_values(0)
    c, v = d["Close"], d["Volume"]; r = c.pct_change()
    up = v.where(r > 0, 0).rolling(20).sum().iloc[-1]; dn = v.where(r < 0, 0).rolling(20).sum().iloc[-1]
    avgv = v.rolling(20).mean().iloc[-1]
    return {"ud": round(float(up / dn), 2) if dn > 0 else None,
            "rvol": round(float(v.iloc[-1] / avgv), 2) if avgv else None,
            "chg": round(float(r.iloc[-1] * 100), 2) if pd.notna(r.iloc[-1]) else None}


def longshort(tk, state=None):
    """Mindhive's long/short opinion on a stock: fuse volume(ud) + firm-moat + smart-money into a verdict."""
    tk = tk.upper(); score = 0; sig = []; meta = {"tk": tk}
    try:
        q = _quick_ud(tk)
        if q and q["ud"] is not None:
            meta.update({"ud": q["ud"], "rvol": q["rvol"], "chg": q["chg"]})
            if q["ud"] >= 1.6: score += 2; sig.append(f"strong accumulation (ud {q['ud']})")
            elif q["ud"] >= 1.3: score += 1; sig.append(f"accumulation (ud {q['ud']})")
            elif q["ud"] <= 0.6: score -= 2; sig.append(f"heavy distribution (ud {q['ud']})")
            elif q["ud"] <= 0.85: score -= 1; sig.append(f"distribution (ud {q['ud']})")
    except Exception:
        pass
    try:
        import marleg_business as biz
        b = biz.analyze(tk)
        if not b.get("error"):
            meta.update({"name": b["name"], "moat": b["moat"], "industry": b.get("industry"),
                         "sector_signal": b.get("sector_signal")})
            if b.get("override") is True: score += 2; sig.append(f"wide moat OVERRIDES bearish sector (moat {b['moat']})")
            elif b["moat"] >= 65: score += 1; sig.append(f"wide moat ({b['moat']})")
            elif b.get("override") is False: score -= 1; sig.append("weak moat — tends to follow its sector")
    except Exception:
        pass
    try:
        import marleg_smartmoney as sm
        f = sm.flow(tk)
        if not f.get("error"):
            if f.get("verdict") == "ACCUMULATING": score += 1; sig.append("institutions accumulating")
            elif f.get("verdict") == "DISTRIBUTING": score -= 1; sig.append("institutions distributing")
    except Exception:
        pass
    meta.update({"score": score, "signals": sig,
                 "verdict": "LONG" if score >= 2 else "SHORT" if score <= -2 else "NEUTRAL"})
    return meta


def _answer_longshort(tk, state):
    r = longshort(tk, state)
    txt = (f"{r.get('name', tk)} ({tk}) — verdict: {r['verdict']} (score {r['score']:+d})\n"
           + "\n".join(f"  • {s}" for s in r["signals"])
           + "\n  Synthesis of volume(ud) + firm-moat + smart-money. Research, not advice.")
    if llm_available():
        ctx = (f"Stock {r.get('name', tk)} ({tk}). Deterministic verdict {r['verdict']} (score {r['score']:+d}). "
               f"Signals: {'; '.join(r['signals'])}. ud={r.get('ud')} rvol={r.get('rvol')} moat={r.get('moat')} "
               f"sector_signal={r.get('sector_signal')}.")
        gen, eng = _llm("In 3-4 sentences give a long/short read on this stock using ONLY the context; "
                        "end with LONG / SHORT / NEUTRAL.\n\nCONTEXT: " + ctx)
        if gen:
            txt = gen + f"\n\n[{eng} · deterministic score {r['score']:+d}]"
    return {"intent": "longshort", "answer": _phrase(txt), "data": r}


def _retrieve(q, state):
    parts = []
    r = state.get("regime", {})
    hot = [s for s, _ in (r.get("sector_dispersion") or [])[:4]]
    parts.append(f"REGIME: gauge {r.get('gauge')}/100 ({r.get('regime')}); incoherent sectors {hot}.")
    try:
        import marleg_india_rules as ir
        parts.append("INDIA RULES: weekly options NIFTY(Tue)/SENSEX(Thu) only, rest monthly; "
                     f"delivery RT ~{ir.cost_bps('delivery')}bps, options ~{ir.cost_bps('options_premium')}bps of premium; "
                     f"STCG {ir.TAX['stcg_pct']}% LTCG {ir.TAX['ltcg_pct']}% over Rs1.25L; T+1 settlement; "
                     "GSM/ASM surveillance names are stop-loss traps (T2T, 5% bands).")
    except Exception:
        pass
    for t in state.get("ledger", [])[:3]:
        parts.append(f"THESIS {t['name']} [{t['nature']}]: {t['gap_verdict']}; 1y basket {(t.get('momentum') or {}).get('r1y')}%.")
    for ev in state.get("events", []):
        parts.append(f"EVENT {ev['label']}: long {[l['industry'] for l in _legs(ev,'LONG',3)]}, "
                     f"short {[s['industry'] for s in _legs(ev,'SHORT',3)]}.")
    tk = _find_ticker(q)
    if tk:
        ls = longshort(tk, state)
        parts.append(f"STOCK {tk} ({ls.get('name','')}): verdict {ls['verdict']} (score {ls['score']:+d}); {'; '.join(ls['signals'])}.")
    return "\n".join(parts)


def _rag_answer(q, state):
    if not llm_available():
        return _help(unknown=True)
    gen, eng = _llm("Answer the user's question using ONLY the CONTEXT (the pod's live signals). Be concise and "
                    "opinionated; if it's about a stock end with LONG/SHORT/NEUTRAL.\n\nCONTEXT:\n"
                    + _retrieve(q, state) + "\n\nQUESTION: " + q)
    return {"intent": "reasoned", "answer": gen + f"\n\n[{eng}]"} if gen else _help(unknown=True)


# ---------------------------------------------------------------- the brain
def ask(q, state=None):
    state = state or synthesize()
    ql = (q or "").strip().lower()
    if not ql or ql in ("hi", "hello", "hey", "help", "?", "what can you do", "what can you do?", "menu"):
        return _help()
    if _has(q, ["long or short", "short or long", "long/short", "should i long", "should i short",
                "bullish", "bearish", "go long", "go short", "buy or sell", "long it", "short it"]):
        ltk = _find_ticker(q)
        if ltk:
            return _answer_longshort(ltk, state)
    ev = _find_event(q)
    if ev and _has(q, ["what if", "scenario", "spike", "spikes", "shock", "crash", "crashes",
                       "plunge", "surge", "happens", "happen"]):
        return _answer_event(ev, state)            # scenario-framed -> cascade, not a look-alike ticker (e.g. "oil"==OIL)
    tk = _find_ticker(q)
    if tk:
        return _answer_firm(tk, state)
    th = _find_thesis(q)
    if th:
        return _answer_thesis(th, state)
    if ev:
        return _answer_event(ev, state)
    if _has(q, ["under-priced", "underpriced", "under priced", "grey swan", "gray swan", "black swan",
                "mispriced", "mispricing", "best edge", "biggest edge", "cheap"]):
        return _answer_underpriced(state)
    if _has(q, ["regime", "mood", "dispersion", "correlation", "coherent", "incoherent", "deploy",
                "stock-picker", "stock picker", "vix"]):
        return _answer_regime(state)
    if _has(q, ["watch", "action", "hot sector", "opportunity", "opportunities", "ideas", "playbook"]):
        return _answer_action(state)
    if _has(q, ["story", "brief", "summary", "overview", "happening", "catch me up", "today", "recap"]):
        return {"intent": "story", "answer": _phrase(state.get("story") or _story_text(state))}
    return _rag_answer(q, state)            # open / unexpected questions -> hybrid LLM over retrieved context


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    q = " ".join(sys.argv[1:]).strip()
    st = synthesize()
    if q:
        print("\n" + ask(q, st)["answer"] + "\n")
        return
    print("\n=== MINDHIVE SELF-TEST ===")
    print(f"memory palace: {st['kg_stats']}\n")
    for demo in ["what's the story", "what's the regime", "what's under-priced",
                 "what if oil spikes", "thesis on water", "tell me about INDIGO"]:
        print("Q: " + demo)
        print(ask(demo, st)["answer"])
        print("-" * 70)


if __name__ == "__main__":
    main()
