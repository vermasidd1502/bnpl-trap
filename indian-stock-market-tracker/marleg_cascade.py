"""
Marle-G Event Cascade Engine — "the painting".

An event moves macro/commodity FACTORS; factors hit INDUSTRIES; industries chain
to downstream industries. We BFS-propagate a signed impact across up to 5 tiers and
read off a multi-leg long/short basket (industry -> member stocks from the taxonomy).

Why factors (not a pure industry->industry stock graph): on an oil shock a refiner's
*stock* falls but airlines fall too — both hit by the same rising fuel COST, not by
each other. Modelling the transmission medium (crude -> fuel price -> airlines) keeps
the signs economically correct.

Academic grounding: Cohen-Frazzini (2008, customer-supplier momentum), Menzly-Ozbas
(2010, supply-chain return predictability), Barrot-Sauvagnat (2016, input shocks),
Acemoglu et al (2012, network origins of aggregate shocks), Kilian (2009, oil),
Hong-Torous-Valkanov (2007, sector lead-lag), Driesprong et al (2008, oil predicts).

  python marleg_cascade.py --list                 # available events
  python marleg_cascade.py --event oil_shock_up   # print the cascade + legs
"""
import os, sys, json, argparse, collections

HERE = os.path.dirname(os.path.abspath(__file__))
TAX_PATH = os.path.join(HERE, "marleg_industry_taxonomy.json")
DECAY = 0.82            # per-tier signal decay (downstream effects are weaker/noisier)
MAX_TIER = 5

# ----- the structural signed graph: (source, target, weight, mechanism) ---------
# Nodes are EITHER a factor (lowercase, transmission only) OR an industry (exact
# taxonomy name, becomes a tradeable leg). weight = effect on target per +1 source.
EDGES = [
    # crude oil ---------------------------------------------------------------
    ("crude", "Upstream E&P (crude/gas producers)", +0.85, "realise higher crude price"),
    ("crude", "Oil & Gas - Integrated", +0.45, "upstream + GRM tailwind"),
    ("crude", "Refiner & OMC", -0.55, "marketing margin squeezed (can't fully pass through)"),
    ("crude", "fuel_price", +0.80, "crude feeds into pump/ATF price"),
    ("crude", "rubber", +0.45, "synthetic rubber / carbon black track crude"),
    ("crude", "inr_usd", +0.55, "oil import bill widens CAD -> weaker rupee"),
    ("crude", "Specialty Chemicals", -0.45, "petrochemical feedstock cost up"),
    ("crude", "Paints", -0.55, "solvents/monomers are crude derivatives"),
    ("crude", "Lubricants", -0.50, "base oil cost up"),
    ("crude", "Fertilizers", -0.30, "naphtha/gas feedstock (subsidy-cushioned)"),
    # fuel price (pump/ATF/diesel) -------------------------------------------
    ("fuel_price", "Airlines", -0.85, "ATF is ~40% of airline cost"),
    ("fuel_price", "Logistics & Freight", -0.55, "diesel is the dominant cost"),
    ("fuel_price", "Shipping", -0.40, "bunker fuel cost up"),
    ("fuel_price", "discretionary", -0.35, "fuel bill eats household budget"),
    ("fuel_price", "Gas Distribution (city gas)", -0.25, "CNG vs petrol economics shift"),
    # rubber ------------------------------------------------------------------
    ("rubber", "Tyres", -0.65, "rubber+carbon black are key tyre inputs"),
    # rupee (inr_usd up = weaker rupee) --------------------------------------
    ("inr_usd", "IT Services", +0.60, "USD revenue, INR cost -> margin up"),
    ("inr_usd", "Pharma - Formulations", +0.35, "US/EU export realisation up"),
    ("inr_usd", "Specialty Chemicals", +0.20, "exporters benefit, partial offset"),
    ("inr_usd", "imported_inflation", +0.40, "costlier imports feed inflation"),
    # demand / discretionary --------------------------------------------------
    ("discretionary", "Auto OEM - Passenger (4W)", +0.45, "big-ticket discretionary demand"),
    ("discretionary", "Auto OEM - Two/Three Wheeler", +0.40, "discretionary demand"),
    ("discretionary", "Consumer Durables / Appliances", +0.45, "discretionary demand"),
    ("discretionary", "Retail", +0.40, "discretionary spend"),
    ("discretionary", "Jewellery", +0.35, "discretionary spend"),
    ("discretionary", "QSR / Restaurants", +0.35, "eating-out budget"),
    ("discretionary", "Hotels & Travel", +0.40, "travel budget"),
    # auto demand chain (industry -> industry) -------------------------------
    ("Auto OEM - Passenger (4W)", "Auto Ancillary - Parts", +0.70, "OEM build rates drive ancillaries"),
    ("Auto OEM - Passenger (4W)", "Tyres", +0.30, "OEM tyre demand (vs replacement)"),
    ("Auto OEM - Two/Three Wheeler", "Auto Ancillary - Parts", +0.45, "OEM build rates"),
    ("Auto OEM - Commercial Vehicle", "Auto Ancillary - Parts", +0.35, "CV build rates"),
    ("Tyres", "Auto OEM - Passenger (4W)", -0.15, "input cost (small, OEMs pass on)"),
    # inflation / rates -------------------------------------------------------
    ("imported_inflation", "rates", +0.45, "inflation -> tighter policy"),
    ("food_inflation", "rates", +0.40, "food CPI -> tighter policy"),
    ("rates", "NBFC - Lending", -0.55, "funding cost up, NIM + growth pressure"),
    ("rates", "Banks - Private", +0.20, "asset repricing faster than deposits -> NIM up"),
    ("rates", "Insurance", +0.18, "investment-book yields rise"),
    ("rates", "Realty", -0.50, "home-loan EMIs up -> demand down"),
    ("rates", "Auto OEM - Passenger (4W)", -0.30, "auto-loan EMIs up"),
    ("rates", "Consumer Durables / Appliances", -0.25, "consumer-finance EMIs up"),
    ("rates", "discretionary", -0.30, "higher EMIs crowd out spending"),
    # rural income (monsoon) --------------------------------------------------
    ("rural_income", "Auto OEM - Two/Three Wheeler", +0.45, "rural 2W demand"),
    ("rural_income", "Fertilizers", +0.40, "input demand rises with sowing"),
    ("rural_income", "Agrochemicals / Pesticides", +0.40, "crop-protection demand"),
    ("rural_income", "FMCG - Personal & Home Care", +0.35, "rural consumption"),
    ("rural_income", "FMCG - Packaged Foods", +0.30, "rural consumption"),
    ("rural_income", "Farm Equipment / Tractors", +0.50, "tractor demand"),
    ("food_inflation", "FMCG - Packaged Foods", -0.25, "input cost up, margin pressure"),
    ("food_inflation", "discretionary", -0.30, "food bill crowds out discretionary"),
    # defense / geopolitics ---------------------------------------------------
    ("defense_spend", "Defense", +0.80, "order book / budget allocation up"),
    ("defense_spend", "Shipping", +0.10, "naval/logistics ancillary"),
    # steel / construction ----------------------------------------------------
    ("steel_price", "Steel", +0.70, "realisations up"),
    ("steel_price", "Auto Ancillary - Parts", -0.25, "input cost up"),
    ("steel_price", "Consumer Durables / Appliances", -0.20, "input cost up"),
    ("infra_capex", "EPC & Infrastructure", +0.60, "order inflow"),
    ("infra_capex", "Cement", +0.45, "demand up"),
    ("infra_capex", "Steel", +0.35, "demand up"),
    ("infra_capex", "Industrial Machinery & EPC", +0.40, "capex cycle"),
    # geopolitical SUPPLY shock (Iran / Strait of Hormuz) — the regime where signs FLIP:
    # unlike demand-driven crude (chemicals DOWN on input cost), a Mid-East supply shock
    # sends chemicals + metals UP (global prices spike, India gains on import-substitution).
    ("hormuz", "crude", +0.55, "Hormuz risk premium feeds straight into crude"),
    ("hormuz", "Specialty Chemicals", +0.45, "Mid-East petrochem supply threatened -> global prices up -> India import-sub"),
    ("hormuz", "Steel", +0.32, "supply fears + real-asset/inflation hedge bid"),
    ("hormuz", "Aluminium", +0.32, "supply fears + inflation hedge"),
    ("hormuz", "Diversified Metals", +0.30, "real-asset / inflation hedge"),
    ("hormuz", "Upstream E&P (crude/gas producers)", +0.45, "LNG+oil via Hormuz threatened -> domestic producers gain"),
    ("hormuz", "Gas Distribution (city gas)", -0.20, "imported-LNG cost up squeezes CGD margins"),
    ("safe_haven", "Defense", +0.55, "conflict -> defense budgets / order books"),
    ("safe_haven", "Jewellery", -0.25, "gold spikes -> jewellery ticket size hurts volume"),
    ("freight_tanker", "Shipping", +0.55, "war-risk premium spikes tanker/freight rates"),
    ("freight_tanker", "Logistics & Freight", -0.20, "fuel/freight cost up for land logistics"),
    # India gas discovery (Andaman) — energy-independence re-rate ------------------
    ("gas_discovery", "Upstream E&P (crude/gas producers)", +0.80, "the explorer (ONGC/OIL) books the find"),
    ("gas_discovery", "Gas Distribution (city gas)", +0.40, "domestic gas supply + transmission (GAIL/GSPL)"),
    ("gas_discovery", "EPC & Infrastructure", +0.45, "field development + pipeline capex"),
    ("gas_discovery", "Oil & Gas - Integrated", +0.20, "domestic feedstock optionality"),
    ("gas_discovery", "Refiner & OMC", +0.20, "lower import dependence eases under-recovery risk"),
    # ---- PHARMA / HEALTHCARE value chain (independent event family) ----------------
    # USFDA action (warning letters / import alerts): bad for US-facing plants
    ("usfda_action", "Pharma - Formulations", -0.50, "US-facing plants: import alerts + remediation cost"),
    ("usfda_action", "Drug Manufacturers - Specialty & Generic", -0.45, "US approvals / supply at risk"),
    ("usfda_action", "Pharma - CDMO/API", -0.20, "compliance overhang on contract sites"),
    # China API / KSM supply shock: domestic API makers gain, formulators' input cost up
    ("api_china_supply", "Pharma - CDMO/API", +0.60, "domestic API/KSM pricing power + import-substitution"),
    ("api_china_supply", "Pharma - Formulations", -0.35, "API input cost up"),
    ("api_china_supply", "Drug Manufacturers - Specialty & Generic", -0.20, "raw-material cost up"),
    ("api_china_supply", "Agrochemicals / Pesticides", +0.25, "shared chem intermediates — domestic gain"),
    # drug price control (NLEM / DPCO expansion): pricing pressure across the chain
    ("drug_price_control", "Pharma - Formulations", -0.45, "price caps on branded/essential drugs"),
    ("drug_price_control", "Drug Manufacturers - Specialty & Generic", -0.30, "domestic price ceilings"),
    ("drug_price_control", "Hospitals", -0.18, "device/consumable margin caps (stents/implants)"),
    ("drug_price_control", "Pharmaceutical Retailers", -0.22, "retail margin caps"),
    # US generic price erosion (the classic export headwind)
    ("us_generic_pricing", "Pharma - Formulations", +0.55, "US generic realisations (erosion = down)"),
    ("us_generic_pricing", "Drug Manufacturers - Specialty & Generic", +0.50, "US generic pricing"),
    # patent cliff (a big drug goes off-patent): Indian generics opportunity
    ("patent_cliff", "Drug Manufacturers - Specialty & Generic", +0.50, "first-to-file / gDrug revenue"),
    ("patent_cliff", "Pharma - Formulations", +0.35, "generic launch upside"),
    ("patent_cliff", "Pharma - CDMO/API", +0.30, "API supply for the new generic"),
    # intra-chain (industry -> industry)
    ("Pharma - Formulations", "Pharma - CDMO/API", +0.35, "formulation demand pulls API/CDMO"),
    ("Hospitals", "Diagnostics", +0.30, "hospital footfall drives diagnostics"),
    ("Hospitals", "Medical Devices", +0.25, "procedure volumes drive device demand"),
    ("Hospitals", "Healthcare Distribution", +0.20, "drug + consumable throughput"),
]

# ----- events: which factors/industries get the initial (tier-1) shock ----------
EVENTS = {
    "oil_shock_up": {
        "label": "Crude oil spikes (supply shock / OPEC cut / conflict premium)",
        "seeds": [("crude", +1.0)],
    },
    "oil_shock_down": {
        "label": "Crude oil collapses (demand slump / oversupply)",
        "seeds": [("crude", -1.0)],
    },
    "iran_war": {
        "label": "Iran conflict / Strait-of-Hormuz risk — crude+gas+metals+chemicals+defense rally in PARALLEL; airlines/autos/paints hit",
        "seeds": [("crude", +0.5), ("hormuz", +1.0), ("safe_haven", +0.8),
                  ("defense_spend", +0.5), ("freight_tanker", +0.7), ("inr_usd", +0.45)],
    },
    "andaman_gas": {
        "label": "India major gas discovery (Andaman basin) — energy-independence re-rate",
        "seeds": [("gas_discovery", +1.0)],
    },
    "war_escalation": {
        "label": "Geopolitical conflict escalates (oil premium + defense build-up)",
        "seeds": [("crude", +0.7), ("defense_spend", +1.0)],
    },
    "monsoon_good": {
        "label": "Above-normal monsoon -> strong rural income",
        "seeds": [("rural_income", +1.0)],
    },
    "monsoon_bad": {
        "label": "Deficient monsoon -> food inflation + weak rural demand",
        "seeds": [("food_inflation", +1.0), ("rural_income", -0.8)],
    },
    "rate_hike": {
        "label": "RBI hikes / hawkish surprise",
        "seeds": [("rates", +1.0)],
    },
    "inr_depreciation": {
        "label": "Rupee depreciates sharply (USD strength / outflows)",
        "seeds": [("inr_usd", +1.0)],
    },
    "infra_push": {
        "label": "Govt capex / infra push (budget)",
        "seeds": [("infra_capex", +1.0)],
    },
    # ---- PHARMA: independent event family ----
    "pharma_usfda": {
        "label": "USFDA crackdown / import-alert wave on Indian pharma — US-facing formulators hit",
        "seeds": [("usfda_action", +1.0)],
    },
    "pharma_api_china": {
        "label": "China API/KSM supply shock — domestic API makers gain, formulators' input cost up",
        "seeds": [("api_china_supply", +1.0)],
    },
    "pharma_price_control": {
        "label": "Drug price control expands (NLEM/DPCO) — pricing pressure across pharma",
        "seeds": [("drug_price_control", +1.0)],
    },
    "pharma_us_erosion": {
        "label": "US generic price erosion deepens — export realisations fall",
        "seeds": [("us_generic_pricing", -1.0)],
    },
    "pharma_patent_cliff": {
        "label": "Major patent cliff — Indian generics opportunity (first-to-file)",
        "seeds": [("patent_cliff", +1.0)],
    },
}


def load_taxonomy():
    return json.load(open(TAX_PATH))


def propagate(event_key):
    ev = EVENTS[event_key]
    out_edges = collections.defaultdict(list)
    for s, d, w, mech in EDGES:
        out_edges[s].append((d, w, mech))

    impact = collections.defaultdict(float)
    tier = {}
    via = {}                                   # how a node was reached (for the painting)
    frontier = {}
    for node, val in ev["seeds"]:
        impact[node] += val
        tier[node] = 0
        frontier[node] = val

    for t in range(1, MAX_TIER + 1):
        nxt = collections.defaultdict(float)
        contributions = collections.defaultdict(list)
        for src, sval in frontier.items():
            for d, w, mech in out_edges.get(src, []):
                c = sval * w * DECAY
                if abs(c) < 0.02:              # prune negligible ripples
                    continue
                nxt[d] += c
                contributions[d].append((src, c, mech))
        for d, val in nxt.items():
            impact[d] += val
            if d not in tier:
                tier[d] = t
                # record the strongest contributor as the "cause"
                via[d] = max(contributions[d], key=lambda x: abs(x[1]))
        frontier = nxt
        if not nxt:
            break
    return impact, tier, via


def build_cascade(event_key, min_abs=0.05):
    tax = load_taxonomy()
    by_ind = tax["by_industry"]
    impact, tier, via = propagate(event_key)
    legs = []
    for node, val in impact.items():
        if node in by_ind and abs(val) >= min_abs and tier.get(node, 0) >= 1:
            members = by_ind[node]
            legs.append({
                "industry": node,
                "tier": tier[node],
                "impact": round(val, 3),
                "side": "LONG" if val > 0 else "SHORT",
                "members": members,
                "via": via.get(node, ("event", val, "direct"))[0],
                "mechanism": via.get(node, ("", 0, "direct"))[2],
            })
    legs.sort(key=lambda x: (x["tier"], -abs(x["impact"])))
    return {"event": event_key, "label": EVENTS[event_key]["label"], "legs": legs}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", default="oil_shock_up")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    if a.list:
        print("Events:")
        for k, v in EVENTS.items():
            print(f"  {k:<18} {v['label']}")
        return
    if a.event not in EVENTS:
        print(f"unknown event '{a.event}'. --list to see options."); return
    casc = build_cascade(a.event)
    if a.json:
        print(json.dumps(casc, indent=1)); return
    print(f"\nCASCADE: {casc['event']}")
    print(f"  {casc['label']}\n")
    print(f"{'TIER':<5}{'SIDE':<6}{'IMPACT':>8}  {'INDUSTRY':<38}{'#':>3}  caused by")
    print("-" * 100)
    for lg in casc["legs"]:
        sign = "+" if lg["impact"] > 0 else ""
        print(f"T{lg['tier']:<4}{lg['side']:<6}{sign}{lg['impact']:>7}  {lg['industry']:<38}{len(lg['members']):>3}  <- {lg['via']}")
    longs = [l for l in casc["legs"] if l["side"] == "LONG"]
    shorts = [l for l in casc["legs"] if l["side"] == "SHORT"]
    print("-" * 100)
    print(f"{len(longs)} LONG industries, {len(shorts)} SHORT industries across "
          f"{max((l['tier'] for l in casc['legs']), default=0)} tiers.")
    print("\nTop LONG legs :", ", ".join(f"{l['industry']}({l['members'][0]})" for l in longs[:4]))
    print("Top SHORT legs:", ", ".join(f"{l['industry']}({l['members'][0]})" for l in shorts[:4]))


if __name__ == "__main__":
    main()
