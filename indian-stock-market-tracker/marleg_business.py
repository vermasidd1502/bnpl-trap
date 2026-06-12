"""
Marle-G — BUSINESS-ANALYST AGENT (the firm-level layer).

Completes the top-down stack: MACRO (regime) -> SECTOR (cascade) -> FIRM (this).
Everything else in the pod is sector/factor-level; this asks what the COMPANY is —
moat, market position, competitive structure — and whether the firm OVERRIDES its
sector signal (the IndiGo problem: cascade says short Airlines on fuel, but a ~60%-share
consolidated near-monopoly with pricing power can rise anyway).

Discipline (so it HELPS, not rationalises): evidence-anchored (objective metrics, not
vibes) + ADVERSARIAL (bull AND bear) + the output is a falsifiable DIVERGENCE verdict,
never a "prove the thesis" narrative.

Components: Moat Score/100 (pricing-power + dominance + durability + resilience),
Porter's 5 Forces (proxy), balanced SWOT, and the firm-vs-sector divergence verdict.

  python marleg_business.py INDIGO
"""
import os, sys, json, time
import numpy as np, yfinance as yf
import marleg_projection as proj

HERE = os.path.dirname(os.path.abspath(__file__))
TAX = json.load(open(os.path.join(HERE, "marleg_industry_taxonomy.json")))
BY_IND, BY_SYM = TAX["by_industry"], TAX["by_symbol"]


def _info(tk):
    try:
        return yf.Ticker(tk.upper() + ".NS").info or {}
    except Exception:
        return {}


def _peers(tk):
    """Industry peers from the taxonomy (the competitive set)."""
    ind = (BY_SYM.get(tk.upper()) or {}).get("industry")
    members = [m for m in (BY_IND.get(ind) or []) if m != tk.upper()]
    return ind, members


def _clip(x, lo=0, hi=100):
    return max(lo, min(hi, x))


def analyze(tk):
    tk = tk.upper()
    info = _info(tk)
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    if not price:
        return {"error": "no data for " + tk}
    ind, peers = _peers(tk)
    name = info.get("shortName") or tk
    opm = (info.get("operatingMargins") or 0) * 100
    npm = (info.get("profitMargins") or 0) * 100
    roe = (info.get("returnOnEquity") or 0) * 100
    de = info.get("debtToEquity"); de = (de / 100) if isinstance(de, (int, float)) else None
    fcf = info.get("freeCashflow"); mcap = info.get("marketCap") or 0
    growth = (info.get("revenueGrowth") or 0) * 100

    # --- margin history (level/stability/trend) from statements ---
    st = proj.statements(yf.Ticker(tk + ".NS"))
    opm_hist = [m for m in (st or {}).get("op_margin", []) if m is not None] if st else []
    stability = (100 - min(100, float(np.std(opm_hist)) * 6)) if len(opm_hist) >= 3 else 50.0   # low std -> stable
    trend = 1 if (len(opm_hist) >= 2 and opm_hist[0] > opm_hist[-1]) else -1 if len(opm_hist) >= 2 else 0

    # --- peer comparison: margin percentile + mcap dominance + consolidation ---
    peer_m, peer_cap = [], []
    for p in peers[:8]:
        pi = _info(p); time.sleep(0.25)
        if pi.get("operatingMargins") is not None:
            peer_m.append(pi["operatingMargins"] * 100)
        if pi.get("marketCap"):
            peer_cap.append(pi["marketCap"])
    margin_pctile = (sum(1 for m in peer_m if opm > m) / len(peer_m) * 100) if peer_m else 50.0
    bigger = sum(1 for c in peer_cap if c > mcap)
    mcap_rank = bigger + 1
    n_peers = len(peers)
    consolidation = _clip(100 - n_peers * 7)          # fewer peers -> more consolidated -> structural moat
    dominance = _clip(0.5 * (100 if mcap_rank == 1 else max(0, 100 - (mcap_rank - 1) * 25)) +
                      0.3 * consolidation + 0.2 * margin_pctile)

    # --- moat components (0-100) ---
    pricing_power = _clip(0.4 * _clip(opm * 4) + 0.4 * stability + 0.2 * margin_pctile)
    # ROE often null / distorted by negative-equity & lease accounting (airlines, etc.)
    # -> fall back to a margin+stability proxy so durability isn't artificially zeroed.
    durability = _clip(roe * 3.3) if roe > 1 else _clip(opm * 2.5 + stability * 0.4)
    resilience = _clip(50 + (25 if (de is not None and de < 0.6) else -15 if (de and de > 1.5) else 0)
                       + (20 if (fcf and fcf > 0) else -10))
    moat = round(0.30 * pricing_power + 0.30 * dominance + 0.25 * durability + 0.15 * resilience)
    moat_label = "WIDE" if moat >= 65 else "NARROW" if moat >= 45 else "NONE / commodity"
    position = ("Dominant / leader" if mcap_rank == 1 and n_peers <= 4 else
                "Leader" if mcap_rank == 1 else "Challenger" if mcap_rank <= 3 else "Also-ran / commodity")

    # --- Porter's 5 forces (proxy-derived, favourable-to-firm 1-5) ---
    forces = {
        "Rivalry (low=good)": ("LOW" if (n_peers <= 3 and stability > 60) else "HIGH" if n_peers > 12 else "MODERATE"),
        "Threat of new entrants": ("LOW (capital/scale barriers)" if mcap > 3e11 else "MODERATE"),
        "Buyer power": ("LOW (pricing power)" if opm > 18 else "HIGH (thin margins)" if opm < 8 else "MODERATE"),
        "Supplier power": ("MODERATE" if (info.get('grossMargins') or 0) > 0.3 else "HIGH (input-cost exposed)"),
        "Substitutes": "case-specific (qualitative)",
    }

    # --- balanced SWOT (evidence-tagged) ---
    S, W, O, T = [], [], [], []
    if opm > 15: S.append(f"Strong operating margin {opm:.1f}% (pricing power)")
    if stability > 65: S.append("Stable margins through cycles (durable)")
    if mcap_rank == 1: S.append(f"#1 by market cap in '{ind}' ({n_peers} listed peers)")
    if roe > 15: S.append(f"High ROE {roe:.0f}%")
    if fcf and fcf > 0: S.append("Free-cash-flow generative")
    if opm < 8: W.append(f"Thin operating margin {opm:.1f}% (little buffer)")
    if de and de > 1.2: W.append(f"Leveraged (D/E {de:.1f}x)")
    if trend < 0: W.append("Operating margin trending down")
    if growth < 5: W.append(f"Soft revenue growth {growth:.0f}%")
    if growth > 12: O.append(f"Revenue compounding {growth:.0f}% — share/category gains")
    if n_peers <= 4: O.append("Consolidated industry — survivor pricing power")
    O.append("Sector tailwinds (check the active cascade / thesis ledger)")
    T.append("Sector cascade headwinds (see divergence below)")
    if opm < 10: T.append("Input-cost / fuel shock compresses thin margins")
    if de and de > 1.2: T.append("Rate hikes raise funding cost")

    # --- DIVERGENCE: does the firm override its sector's cascade signal? ---
    sector_signal, override = None, None
    try:
        import marleg_cascade as casc
        legs = {}
        for ev in ("oil_shock_up", "iran_war", "rate_hike"):
            for lg in casc.build_cascade(ev)["legs"]:
                if lg["industry"] == ind:
                    legs[ev] = lg["side"]
        if legs:
            shorts = [e for e, s in legs.items() if s == "SHORT"]
            longs = [e for e, s in legs.items() if s == "LONG"]
            if shorts and not longs:
                sector_signal = f"cascade SHORTS this sector in: {', '.join(shorts)}"
                override = (moat >= 55 or dominance >= 60)
            elif longs:
                sector_signal = f"cascade favours this sector in: {', '.join(longs)}"
                override = None
        else:
            sector_signal = "sector not a strong cascade leg"
    except Exception:
        sector_signal = "cascade unavailable"
    if override is True:
        verdict = (f"OVERRIDE — sector signal is bearish, but {tk}'s moat ({moat_label}, {position.lower()}) "
                   f"lets it pass costs & take share. The franchise can rise while the SECTOR suffers.")
    elif override is False:
        verdict = (f"CONFIRMS — weak moat ({moat_label}); {tk} likely follows its sector down. The short stands.")
    else:
        verdict = "Firm signal aligns with / not contradicted by its sector; no strong override."

    return {"tk": tk, "name": name, "industry": ind, "sector": (BY_SYM.get(tk) or {}).get("macro"),
            "position": position, "peers_listed": n_peers, "mcap_rank": mcap_rank,
            "moat": moat, "moat_label": moat_label,
            "components": {"pricing_power": round(pricing_power), "dominance": round(dominance),
                           "durability": round(durability), "resilience": round(resilience)},
            "metrics": {"op_margin": round(opm, 1), "net_margin": round(npm, 1), "roe": round(roe, 1),
                        "rev_growth": round(growth, 1), "de": round(de, 2) if de is not None else None,
                        "margin_stability": round(stability), "margin_vs_peers_pctile": round(margin_pctile)},
            "forces": forces, "swot": {"strengths": S, "weaknesses": W, "opportunities": O, "threats": T},
            "sector_signal": sector_signal, "override": override, "divergence_verdict": verdict}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    tk = sys.argv[1] if len(sys.argv) > 1 else "INDIGO"
    r = analyze(tk)
    if r.get("error"):
        print(r["error"]); return
    print(f"\nBUSINESS-ANALYST AGENT — {r['name']} ({r['tk']})")
    print(f"  {r['sector']} · {r['industry']} · {r['position']} (#{r['mcap_rank']} of {r['peers_listed']+1} listed)\n")
    c = r["components"]
    print(f"  MOAT SCORE: {r['moat']}/100  [{r['moat_label']}]")
    print(f"    pricing power {c['pricing_power']} · dominance {c['dominance']} · durability {c['durability']} · resilience {c['resilience']}")
    m = r["metrics"]
    print(f"  metrics: op-margin {m['op_margin']}% (stability {m['margin_stability']}, {m['margin_vs_peers_pctile']}th pctile vs peers) · "
          f"ROE {m['roe']}% · rev growth {m['rev_growth']}% · D/E {m['de']}")
    print(f"\n  PORTER'S 5 FORCES:")
    for k, v in r["forces"].items():
        print(f"    {k:<26} {v}")
    sw = r["swot"]
    print(f"\n  SWOT (bull):  + {' | '.join(sw['strengths']) or '—'}")
    print(f"               opps: {' | '.join(sw['opportunities'])}")
    print(f"  SWOT (bear):  - {' | '.join(sw['weaknesses']) or '—'}")
    print(f"               threats: {' | '.join(sw['threats'])}")
    print(f"\n  SECTOR SIGNAL: {r['sector_signal']}")
    print(f"  ➤ DIVERGENCE VERDICT: {r['divergence_verdict']}")
    print("\n  Evidence-anchored + adversarial. Monitor-only; not advice.")


if __name__ == "__main__":
    main()
