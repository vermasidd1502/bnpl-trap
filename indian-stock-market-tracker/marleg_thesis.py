"""
Marle-G — THESIS LEDGER: the structural grey-swan scenario book.

Each thesis is framed on two axes the analyst (you) reasons in:
  1. NATURE — Evolutionary (gradual, mostly-priced, LINEAR payoff: ride the trend) vs
     Revolutionary (discontinuous, under-the-radar, CONVEX payoff: small size, big upside).
  2. The NETWORK-EFFECT ADOPTION S-CURVE ("the candlestick") — where the real-world
     thing is (latent -> inflection -> steep -> mature) vs where the MARKET is pricing it
     (latent -> anticipating -> playing-out -> fading). The GAP IS THE EDGE:
        adoption AHEAD of price  -> UNDER-PRICED (position early)
        price AHEAD of adoption  -> CROWDED / fade-risk

Winners/losers are industry nodes from the 90-node taxonomy -> member baskets. Basket
momentum (3m/1y) is the live "candlestick" telling you where the market already is.
Gate the whole book by the Regime Dial (scenario-alpha live only when dispersed).

  python marleg_thesis.py            # the ledger
  python marleg_thesis.py ai_datacenter
"""
import os, sys, json, time
import numpy as np, pandas as pd, yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
TAX = json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json")))["by_industry"]

ADOPT = {"latent": 0, "inflection": 1, "steep": 2, "mature": 3}
ANTIC = {"latent": 0, "anticipating": 1, "playing-out": 2, "fading": 3}

# --- the scenario book (3 to start; spans evolutionary<->revolutionary, all India-tradeable) ---
THESES = {
    "ai_datacenter": {
        "name": "AI / Data-Centre Build-out",
        "nature": "Revolutionary",
        "adoption": "steep", "anticipation": "playing-out",
        "network_effect": "Picks-and-shovels (the compute winners are US; India sells the SHOVELS — power & gear)",
        "payoff": "Linear on the India shovels (ride); the revolutionary upside accrues to US compute we can't buy",
        "mechanism": "Hyperscale AI compute → explosive electricity, cooling, transformers, cabling & data-centre construction demand. India has no leading-edge fabs, so the tradeable India play is the POWER + electrical supply chain feeding the build-out.",
        "winners": ["Power Generation - Thermal", "Power - Renewables", "Power Transmission & Distribution",
                    "Electrical Equipment", "Cables & Wires", "Electronics Manufacturing (EMS)", "EPC & Infrastructure"],
        "losers": [],
        "confirmation": "data-centre capex announcements, power-demand prints, electrical-equipment order books, transformer/cable lead-times & pricing.",
        "india_caveat": "NO Indian chip fab / no quantum pure-play — India monetises AI through power & electrical gear, not silicon. Don't pretend otherwise.",
        "conviction": 4,
    },
    "water_scarcity": {
        "name": "Water Scarcity & Treatment",
        "nature": "Evolutionary → Revolutionary",
        "adoption": "inflection", "anticipation": "latent",
        "network_effect": "Broad / infrastructure — no winner-take-all; spread the small basket",
        "payoff": "Convex — small-cap, under-owned; asymmetric if climate/industrial-water stress accelerates",
        "mechanism": "Climate stress + industrial demand (incl. data-centre cooling) + Jal-Jeevan-style public capex → multi-decade demand for water treatment, recycling and pipes. Adoption is inflecting but the market hasn't woken up — the classic adoption-ahead-of-price gap.",
        "winners": ["Water Treatment & Environment", "Building Products"],
        "losers": [],
        "confirmation": "water-treatment order books, government water-capex allocations, industrial reuse mandates, monsoon-deficit headlines.",
        "india_caveat": "Small/mid-cap, thin liquidity (EIEL/WABAG/IONEXCHANG/REFEX) — size for illiquidity; it's a watchlist-and-accumulate, not a crowded trade.",
        "conviction": 3,
    },
    "housing_cycle": {
        "name": "India Housing Up-cycle",
        "nature": "Evolutionary (cyclical)",
        "adoption": "steep", "anticipation": "playing-out",
        "network_effect": "Broad — spread across the value chain (developers, materials, finance)",
        "payoff": "Linear / trend-ride — late-evolutionary; ride the chain but watch for the rate-driven fade",
        "mechanism": "Urbanisation + affordability cycle + low household mortgage penetration → a multi-year residential up-cycle pulling developers, cement, steel, building products, white goods and housing finance.",
        "winners": ["Realty", "Cement", "Steel", "Building Products", "NBFC - Lending", "Consumer Durables / Appliances"],
        "losers": [],
        "confirmation": "new-launch & absorption data, cement dispatch volumes, home-loan growth, building-product volume growth.",
        "india_caveat": "Rate-sensitive — a hawkish RBI is the main falsifier (rising EMIs cool demand; see the rate_hike cascade). Late-evolutionary = lower convexity than the revolutionary theses.",
        "conviction": 3,
    },
}


def basket(thesis):
    """Expand winner/loser industry nodes -> member symbols (capped, deduped)."""
    def members(nodes):
        out = []
        for n in nodes:
            out += (TAX.get(n) or [])[:6]
        seen, uniq = set(), []
        for s in out:
            if s not in seen:
                seen.add(s); uniq.append(s)
        return uniq
    return {"long": members(thesis["winners"]), "short": members(thesis["losers"])}


def basket_momentum(syms):
    """The live 'candlestick' — equal-weight basket 3m / 1y return = where the market already is."""
    if not syms:
        return {}
    try:
        d = yf.download([s + ".NS" for s in syms[:20]], period="1y", interval="1d",
                        group_by="ticker", progress=False, threads=True)
        rr3, rr1 = [], []
        for s in syms[:20]:
            try:
                c = d[s + ".NS"]["Close"].dropna()
                if len(c) > 70:
                    rr1.append(c.iloc[-1] / c.iloc[0] - 1)
                    rr3.append(c.iloc[-1] / c.iloc[-63] - 1)
            except Exception:
                pass
        return {"r3m": round(float(np.mean(rr3)) * 100, 1) if rr3 else None,
                "r1y": round(float(np.mean(rr1)) * 100, 1) if rr1 else None, "n": len(rr1)}
    except Exception:
        return {}


def gap_verdict(t):
    g = ADOPT[t["adoption"]] - ANTIC[t["anticipation"]]
    if g >= 1:
        return g, "UNDER-PRICED — adoption ahead of the market; position EARLY (best edge)"
    if g <= -1:
        return g, "OVER-ANTICIPATED — price ahead of reality; crowded / fade-risk"
    return g, "TRACKED — market roughly pricing adoption; ride leaders or wait for a pullback"


def analyze(key, with_momentum=True):
    t = THESES.get(key)
    if not t:
        return {"error": "unknown thesis"}
    b = basket(t)
    g, verdict = gap_verdict(t)
    out = {"key": key, **{k: t[k] for k in ("name", "nature", "adoption", "anticipation",
           "network_effect", "payoff", "mechanism", "confirmation", "india_caveat", "conviction")},
           "winners": t["winners"], "losers": t["losers"], "basket": b,
           "gap": g, "gap_verdict": verdict}
    if with_momentum:
        out["momentum"] = basket_momentum(b["long"])
    return out


def ledger(with_momentum=True):
    return [analyze(k, with_momentum) for k in THESES]


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    keys = [sys.argv[1]] if len(sys.argv) > 1 and sys.argv[1] in THESES else list(THESES)
    print("\nMARLE-G THESIS LEDGER — structural scenario book (gate with the Regime Dial)\n")
    for k in keys:
        t = analyze(k)
        m = t.get("momentum") or {}
        print(f"━━ {t['name']}  [{t['nature']}]  conviction {t['conviction']}/5")
        print(f"   adoption: {t['adoption'].upper()}  vs  market: {t['anticipation'].upper()}  ->  {t['gap_verdict']}")
        print(f"   payoff: {t['payoff']}")
        print(f"   mechanism: {t['mechanism']}")
        print(f"   LONG basket ({len(t['basket']['long'])}): {', '.join(t['basket']['long'][:10])}")
        if m.get('r1y') is not None:
            print(f"   basket 'candlestick': {m['r3m']:+.0f}% 3m · {m['r1y']:+.0f}% 1y  (where the market already is)")
        print(f"   network effect: {t['network_effect']}")
        print(f"   confirm: {t['confirmation']}")
        print(f"   ⚠ India: {t['india_caveat']}\n")
    print("Read the GAP: adoption ahead of market = position early; market ahead = crowded. Monitor-only.")


if __name__ == "__main__":
    main()
