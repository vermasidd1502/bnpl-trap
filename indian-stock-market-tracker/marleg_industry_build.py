"""
Marle-G — build the cascade-ready GRANULAR industry taxonomy.

Why: the NSE macro layer (22 sectors, authoritative, in nse_ntm.csv) is too coarse
for a 5-tier event cascade ("oil up -> refiner margin up -> OMC down -> tyres down
-> auto OEM down"). yfinance gives some GICS sub-industries but ~40% of liquid names
fall back to the macro name as a placeholder. So we overlay a CURATED granular map
keyed by NSE symbol for the cascade-critical chains (energy, auto, metals, materials,
power, transport, agri) + finer splits of financials/IT/pharma.

Resolution order per symbol (liquid NTM universe):
  1. curated RULES (symbol -> granular industry)        # cascade nodes, authoritative-by-hand
  2. existing yfinance sub-industry (if not a macro placeholder)
  3. NSE macro sector (always present)

Output: marleg_industry_taxonomy.json
  { by_symbol, by_industry, industries, macro_to_industries, coverage }

  python marleg_industry_build.py            # build + report
  python marleg_industry_build.py --check    # report coverage only
"""
import os, sys, csv, json, collections

HERE = os.path.dirname(os.path.abspath(__file__))

# Water utilities mis-bucketed as "Power" in the old map -> NSE says Utilities.
MACRO_FIX = {"IONEXCHANG": "Utilities", "WABAG": "Utilities", "REFEX": "Utilities", "EIEL": "Utilities"}

# ---- curated granular industries (the cascade nodes) -> NSE symbols -------------
# Members are the liquid (NTM/N500) names I can place by hand; anything missed
# falls back to yfinance/macro. Symbols only (most reliable join key).
RULES = {
    # ENERGY CHAIN ----------------------------------------------------------
    "Upstream E&P (crude/gas producers)": ["ONGC", "OIL", "HINDOILEXP", "SELAN"],
    "Oil & Gas - Integrated": ["RELIANCE"],
    "Refiner & OMC": ["IOC", "BPCL", "HPCL", "MRPL", "CHENNPETRO"],
    "Gas Distribution (city gas)": ["IGL", "MGL", "GUJGASLTD", "GSPL", "ATGL", "GAIL"],
    "Coal Mining": ["COALINDIA"],
    "Oilfield Services & Equipment": ["AEGISLOG", "DEEPINDS", "JINDRILL"],
    # AUTO CHAIN ------------------------------------------------------------
    "Auto OEM - Passenger (4W)": ["MARUTI", "TMPV", "M&M", "MAHINDCIE"],
    "Auto OEM - Two/Three Wheeler": ["BAJAJ-AUTO", "HEROMOTOCO", "TVSMOTOR", "EICHERMOT", "ATULAUTO"],
    "Auto OEM - Commercial Vehicle": ["TMCV", "ASHOKLEY", "FORCEMOT", "SMLISUZU"],
    "Tyres": ["MRF", "APOLLOTYRE", "CEATLTD", "BALKRISIND", "JKTYRE", "GOODYEAR", "TVSSRICHAK"],
    "Auto Ancillary - Parts": ["MOTHERSON", "BHARATFORG", "SONACOMS", "UNOMINDA", "BOSCHLTD",
                                "ENDURANCE", "SUNDRMFAST", "ZFCVINDIA", "SCHAEFFLER", "WHEELS",
                                "GABRIEL", "JAMNAAUTO", "SUPRAJIT", "MINDACORP", "LUMAXTECH", "CRAFTSMAN"],
    "Auto Ancillary - Batteries": ["EXIDEIND", "AMARAJABAT", "ARE&M"],
    # METALS & MINING -------------------------------------------------------
    "Steel": ["TATASTEEL", "JSWSTEEL", "SAIL", "JINDALSTEL", "JSL", "APLAPOLLO", "RATNAMANI", "WELCORP", "JTLIND"],
    "Aluminium": ["HINDALCO", "NATIONALUM"],
    "Diversified Metals": ["VEDL", "HINDZINC", "HINDCOPPER"],
    "Iron Ore & Other Mining": ["NMDC", "MOIL", "GMDCLTD"],
    # CONSTRUCTION MATERIALS ------------------------------------------------
    "Cement": ["ULTRACEMCO", "SHREECEM", "AMBUJACEM", "ACC", "DALBHARAT", "JKCEMENT",
                "RAMCOCEM", "JKLAKSHMI", "HEIDELBERG", "BIRLACORPN", "INDIACEM", "NUVOCO", "STARCEMENT"],
    "Paints": ["ASIANPAINT", "BERGEPAINT", "KANSAINER", "INDIGOPNTS", "AKZOINDIA"],
    "Building Products": ["KAJARIACER", "CERA", "SOMANYCERA", "HSIL", "CENTURYPLY", "GREENPLY",
                           "SUPREMEIND", "ASTRAL", "FINOLEXIND", "PRINCEPIPE", "APOLLOPIPE"],
    # POWER -----------------------------------------------------------------
    "Power Generation - Thermal": ["NTPC", "ADANIPOWER", "JSWENERGY", "TATAPOWER", "TORNTPOWER", "CESC", "NLCINDIA"],
    "Power Generation - Hydro/Nuclear": ["NHPC", "SJVN"],
    "Power - Renewables": ["ADANIGREEN", "SUZLON", "INOXWIND", "ORIENTGREEN", "WAAREEENER", "JPPOWER"],
    "Power Transmission & Distribution": ["POWERGRID", "ADANIENSOL", "JSWINFRA"],
    "Power Financing": ["PFC", "RECLTD", "IREDA"],
    # TRANSPORT / LOGISTICS -------------------------------------------------
    "Airlines": ["INDIGO", "SPICEJET"],
    "Airports & Air Services": ["GMRAIRPORT", "GMRINFRA"],
    "Shipping": ["SCI", "GESHIP", "COCHINSHIP"],
    "Logistics & Freight": ["CONCOR", "DELHIVERY", "BLUEDART", "TCIEXP", "VRLLOG", "MAHLOG", "ALLCARGO", "GATI"],
    # AGRI ------------------------------------------------------------------
    "Fertilizers": ["COROMANDEL", "CHAMBLFERT", "GNFC", "GSFC", "RCF", "NFL", "DEEPAKFERT", "FACT", "MADRASFERT", "PARADEEP"],
    "Agrochemicals / Pesticides": ["UPL", "PIIND", "SUMICHEM", "RALLIS", "BAYERCROP", "DHANUKA", "INSECTICID", "SHARDACROP"],
    "Sugar": ["BALRAMCHIN", "BAJAJHIND", "DHAMPURSUG", "TRIVENI", "DALMIASUG", "DCMSHRIRAM", "EIDPARRY", "RENUKA"],
    # CONSUMER --------------------------------------------------------------
    "FMCG - Personal & Home Care": ["HINDUNILVR", "GODREJCP", "DABUR", "MARICO", "COLPAL", "EMAMILTD", "GILLETTE", "JYOTHYLAB", "HONASA"],
    "FMCG - Packaged Foods": ["NESTLEIND", "BRITANNIA", "TATACONSUM", "MARICO", "PATANJALI", "BIKAJI", "MRSBECTORS", "ADANIWILMAR"],
    "Beverages & Distilleries": ["UNITDSPR", "RADICO", "UBL", "VBL", "GLOBUSSPR"],
    "Tobacco": ["ITC", "GODFRYPHLP", "VSTIND"],
    "QSR / Restaurants": ["JUBLFOOD", "DEVYANI", "WESTLIFE", "SAPPHIRE", "RBA"],
    "Retail": ["DMART", "TRENT", "ABFRL", "VMART", "SHOPERSTOP", "ADANIENT"],
    "Consumer Durables / Appliances": ["HAVELLS", "VOLTAS", "CROMPTON", "WHIRLPOOL", "BLUESTARCO", "BAJAJELEC", "VGUARD", "ORIENTELEC", "TITAN", "RAJESHEXPO", "KALYANKJIL", "SYMPHONY"],
    "Electronics Manufacturing (EMS)": ["DIXON", "KAYNES", "SYRMA", "AMBER", "CYIENTDLM", "AVALON", "PGEL"],
    # FINANCIALS ------------------------------------------------------------
    "Banks - Private": ["HDFCBANK", "ICICIBANK", "AXISBANK", "KOTAKBANK", "INDUSINDBK", "FEDERALBNK",
                         "IDFCFIRSTB", "BANDHANBNK", "RBLBANK", "CITYUNIONBK", "KARURVYSYA", "DCBBANK", "CSBBANK", "TMB", "SOUTHBANK"],
    "Banks - PSU": ["SBIN", "BANKBARODA", "PNB", "CANBK", "UNIONBANK", "BANKINDIA", "INDIANB",
                     "CENTRALBK", "IOB", "UCOBANK", "MAHABANK", "PSB", "J&KBANK"],
    "NBFC - Lending": ["BAJFINANCE", "CHOLAFIN", "SHRIRAMFIN", "SBICARD", "MUTHOOTFIN", "MANAPPURAM",
                        "LICHSGFIN", "BAJAJFINSV", "ABCAPITAL", "POONAWALLA", "L&TFH", "MMFIN", "PNBHOUSING", "CANFINHOME", "HUDCO", "IIFL", "FIVESTAR"],
    "Insurance": ["SBILIFE", "HDFCLIFE", "ICICIPRULI", "ICICIGI", "LICI", "MAXFIN", "STARHEALTH",
                   "GICRE", "NIACL", "MAXHEALTH", "BAJAJFINSV"],
    "Capital Markets & AMC": ["HDFCAMC", "NAM-INDIA", "UTIAMC", "ABSLAMC", "BSE", "MCX", "CDSL",
                               "CAMS", "ANGELONE", "360ONE", "MOTILALOFS", "KFINTECH", "IEX", "NUVAMA"],
    "Fintech": ["PAYTM", "POLICYBZR"],
    # IT & INTERNET ---------------------------------------------------------
    "IT Services": ["TCS", "INFY", "WIPRO", "HCLTECH", "TECHM", "LTIM", "MPHASIS", "COFORGE",
                     "PERSISTENT", "LTTS", "OFSS", "KPITTECH", "TATAELXSI", "CYIENT", "BIRLASOFT", "SONATSOFTW", "ZENSARTECH", "MASTEK", "NEWGEN"],
    "Internet / New-age": ["ETERNAL", "ZOMATO", "NYKAA", "FSN", "PBFINTECH", "PAYTM", "DELHIVERY", "CARTRADE", "EASEMYTRIP", "NAUKRI"],
    # HEALTHCARE ------------------------------------------------------------
    "Pharma - Formulations": ["SUNPHARMA", "DRREDDY", "CIPLA", "LUPIN", "AUROPHARMA", "ZYDUSLIFE",
                               "ALKEM", "TORNTPHARM", "GLENMARK", "MANKIND", "IPCALAB", "JBCHEPHARM", "AJANTPHARM", "ERIS", "NATCOPHARM", "FDC"],
    "Pharma - CDMO/API": ["DIVISLAB", "LAURUSLABS", "GLAND", "SYNGENE", "GRANULES", "SOLARA", "AARTIDRUGS", "NEULANDLAB"],
    "Hospitals": ["APOLLOHOSP", "MAXHEALTH", "FORTIS", "NH", "GLOBALHEALTH", "MEDANTA", "KIMS", "ASTERDM", "RAINBOW", "JUPITER"],
    "Diagnostics": ["DRLAL", "METROPOLIS", "THYROCARE", "VIJAYA"],
    # TELECOM ---------------------------------------------------------------
    "Telecom Services": ["BHARTIARTL", "IDEA", "TATACOMM", "MTNL"],
    "Telecom Infra & Equipment": ["INDUSTOWER", "HFCL", "TEJASNET", "ITI", "STLTECH"],
    # CAPITAL GOODS / DEFENSE ----------------------------------------------
    "Defense": ["HAL", "BEL", "BDL", "MAZDOCK", "COCHINSHIP", "BEML", "DATAPATTNS", "SOLARINDS", "ZENTEC", "PARAS", "MTARTECH", "IDEAFORGE"],
    "Electrical Equipment": ["ABB", "SIEMENS", "CGPOWER", "POLYCAB", "KEI", "THERMAX", "BHEL", "HBLENGINE", "GEVERNOVA", "TRIVENITURB", "INOXINDIA"],
    "Industrial Machinery & EPC": ["LT", "KEC", "KALPATPOWR", "KIRLOSENG", "AIAENG", "GRINDWELL",
                                    "CUMMINSIND", "TIMKEN", "SKFINDIA", "ELGIEQUIP", "VOLTAMP", "HONAUT", "PRAJIND", "ITDCEM", "RVNL", "IRCON", "NCC", "NBCC"],
    # REALTY ----------------------------------------------------------------
    "Realty": ["DLF", "GODREJPROP", "OBEROIRLTY", "PHOENIXLTD", "PRESTIGE", "BRIGADE", "LODHA",
                "MACROTECH", "SOBHA", "MAHLIFE", "SUNTECK", "ANANTRAJ", "SIGNATURE", "RAYMOND"],
    # CHEMICALS -------------------------------------------------------------
    "Specialty Chemicals": ["PIDILITIND", "SRF", "AARTIIND", "DEEPAKNTR", "ATUL", "VINATIORGA",
                             "NAVINFLUOR", "FINEORG", "CLEAN", "ALKYLAMINE", "BALAMINES", "GALAXYSURF",
                             "PCBL", "TATACHEM", "GHCL", "CHEMPLASTS", "NOCIL", "JUBLINGREA", "ROSSARI", "SUDARSCHEM"],
    "Textiles": ["PAGEIND", "KPRMILL", "TRIDENT", "WELSPUNLIV", "VARDHACRLC", "GOKEX", "RAYMOND",
                  "ARVIND", "VTL", "SPLPETRO", "GARFIBRES", "RAYMONDLSL", "PGIL", "ICIL"],
}

# ---- correction/extension layer (real NSE symbols learned from the residuals) --
# Merged into RULES below; corrects mis-spelled symbols + adds finer cascade nodes.
RULES_EXTRA = {
    "Auto OEM - Passenger (4W)": ["HYUNDAI"],
    "Auto OEM - Electric": ["OLAELEC", "OLECTRA"],
    "Farm Equipment / Tractors": ["ESCORTS", "VSTTILLERS"],
    "Auto Ancillary - Parts": ["MSUMI", "JBMA", "RKFORGE", "SHRIPISTON", "CIEINDIA", "VARROC",
                                "PRICOLLTD", "TENNIND", "FIEMIND", "SUBROS"],
    "Gas Distribution (city gas)": ["PETRONET"],
    "Lubricants": ["CASTROLIND", "GULFOILLUB"],
    "Power Generation - Thermal": ["RPOWER", "RTNPOWER", "NAVA"],
    "Power - Renewables": ["NTPCGREEN", "KPIGREEN"],
    "Power Trading": ["PTC"],
    "Banks - Private": ["CUB", "KTKBANK", "YESBANK"],
    "Banks - PSU": ["IDBI"],
    "Small Finance Banks": ["UJJIVANSFB", "AUBANK", "EQUITASBNK", "SURYODAY", "UTKARSHBNK"],
    "NBFC - Lending": ["M&MFIN", "TATACAP", "LTF", "CREDITACC", "SBFC", "HOMEFIRST", "FEDFINA",
                        "CGCL", "PIRAMALFIN", "EDELWEISS", "JMFINANCIL", "RELIGARE", "INDIASHLTR", "HDBFS"],
    "Insurance": ["MFSL", "NIVABUPA", "CANHLIFE"],
    "Fintech": ["GROWW", "PINELABS", "CCAVENUE", "CHOICEIN"],
    "Capital Markets & AMC": ["CRISIL", "CRAMC", "ICICIAMC", "PRUDENT", "IIFLCAPS", "SHAREINDIA",
                               "TATAINVEST", "TSFINV", "CHOLAHLDNG"],
    "Pharma - Formulations": ["WOCKPHARMA", "EMCURE", "GLAXO", "PFIZER", "SANOFICONR", "CAPLIPOINT",
                               "MARKSANS", "JUBLPHARMA", "CONCORDBIO", "SHILPAMED", "PPLPHARMA", "SPARC"],
    "Pharma - CDMO/API": ["COHANCE", "SAILIFE", "ONESOURCE", "BLUEJET"],
    "Hospitals": ["HCG", "PARKHOSPS", "JLHL", "YATHARTH"],
    "Diagnostics": ["LALPATHLAB"],
    "Hotels & Travel": ["EIHOTEL", "ITCHOTELS", "LEMONTREE", "CHALET", "THELEELA", "SAMHI",
                         "THOMASCOOK", "IXIGO", "TBOTEK", "TRAVELFOOD"],
    "Online Services / Platforms": ["INDIAMART", "JUSTDIAL", "FIRSTCRY", "SWIGGY", "MEESHO",
                                     "LENSKART", "BLS", "URBANCO"],
    "Jewellery": ["TITAN", "PCJEWELLER", "SENCO", "PNGJL", "THANGAMAYL", "SKYGOLD", "KALYANKJIL", "RAJESHEXPO"],
    "Footwear": ["RELAXO", "CAMPUS", "REDTAPE"],
    "EPC & Infrastructure": ["ENGINERSIN", "HCC", "HGINFRA", "KNRCON", "KPIL", "PNCINFRA",
                              "POWERMECH", "DBL", "TECHNOE", "NCC", "WELENT", "ITDCEM", "RVNL", "IRCON", "NBCC"],
    "Defense": ["GRSE", "MIDHANI"],
    "Pumps & Compressors": ["KSB", "KIRLOSBROS", "SHAKTIPUMP", "OSWALPUMPS"],
    "Abrasives & Refractories": ["CARBORUNIV", "RHIM"],
    "Graphite Electrodes": ["HEG", "GRAPHITE"],
    "Cables & Wires": ["FINCABLES", "DIACABS"],
    "Electrical Equipment": ["SCHNEIDER", "POWERINDIA", "ELECON"],
    "IT - Products & Platforms": ["TATATECH", "MAPMYINDIA", "TANLA", "INTELLECT", "NETWEB",
                                   "RATEGAIN", "HAPPSTMNDS", "LATENTVIEW", "ZAGGLE", "NEWGEN"],
    "Steel": ["SHYAMMETL", "GALLANTT", "MAHSEAMLES", "JTLIND", "LLOYDSENGG", "ELECTCAST"],
    "Specialty Chemicals": ["FLUOROCHEM", "NEOGEN", "PRIVISCL", "HSCL", "STYRENIX"],
    "Industrial Gases": ["LINDEINDIA"],
    "FMCG - Packaged Foods": ["HERITGFOOD", "KRBL", "LTFOODS", "DOMS", "CCL", "GAEL", "GOKULAGRO", "MANORAMA"],
}
for _ind, _syms in RULES_EXTRA.items():
    RULES.setdefault(_ind, [])
    RULES[_ind].extend(s for s in _syms if s not in RULES[_ind])

# ---- final residual sweep (the leftover macro-fallback names, hand-classified) ----
RULES_FINAL = {
    "Solar & Renewable Equipment": ["BORORENEW", "EMMVEE", "PREMIERENE", "SAATVIKGL", "UTLSOLAR",
                                     "VIKRAMSOLR", "WAAREERTL", "WEBELSOLAR", "INOXGREEN"],
    "Electrical Equipment": ["TARIL", "ENRIN", "QPOWER", "TDPOWERSYS", "TRANSRAILL"],
    "Railways & Wagons": ["TEXRAIL", "TITAGARH"],
    "Industrial Machinery & EPC": ["JYOTICNC", "GMMPFAUDLR", "KRN", "TEGA", "SKFINDUS", "TRITURBINE"],
    "Defense": ["PTCIL", "DYNAMATECH"],
    "Building Products": ["FINPIPE", "SURYAROSNI"],
    "Industrial Packaging": ["TIMETECHNO", "EPL"],
    "Steel": ["JAYNECOIND", "USHAMART", "JAIBALAJI", "LLOYDSME", "LLOYDSENT", "SARDAEN", "IMFA"],
    "Iron Ore & Other Mining": ["SANDUMA"],
    "Diversified Metals": ["GRAVITA", "JAINREC"],
    "Auto OEM - Commercial Vehicle": ["SMLMAH"],
    "Electronics Manufacturing (EMS)": ["CPPLUS", "OPTIEMUS"],
    "Logistics & Freight": ["BLACKBUCK", "GPPL", "TVSSCS", "REDINGTON", "MMTC"],
    "IT Services": ["BSOFT", "HEXT", "DATAMATICS", "ECLERX", "FSL"],
    "IT - Products & Platforms": ["CAPILLARY", "SAGILITY", "IKS", "INDGN"],
    "Staffing & BPO": ["QUESS"],
    "Realty": ["SMARTWORKS", "WEWORK", "HEMIPROP", "NESCO"],
    "Jewellery": ["BLUESTONE", "ETHOSLTD", "VAIBHAVGBL"],
    "Furniture & Home Products": ["SFL", "WAKEFIT", "CELLO"],
    "Consumer Durables / Appliances": ["IFBIND", "EUREKAFORB", "LGEINDIA"],
    "Luggage": ["SAFARI", "VIPIND"],
    "Paints": ["JSWDULUX"],
    "Retail": ["V2RETAIL", "VMM", "EMIL"],
    "Apparel & Fashion": ["MANYAVAR"],
    "Education": ["PWL", "CRIZAC"],
    "Pharma - Formulations": ["CORONA", "STAR", "VIYASH"],
    "Pharma - CDMO/API": ["RUBICON"],
    "Medical Devices": ["POLYMED"],
    "Hospitals": ["JSLL"],
    "Healthcare Distribution": ["ENTERO"],
    "Beverages & Distilleries": ["TI", "PICCADIL"],
    "Seeds & Agri-Inputs": ["KSCL"],
    "Specialty Chemicals": ["INDIAGLYCO", "ACI", "RAIN"],
    "Industrial Gases": ["ELLEN"],
    "FMCG - Personal & Home Care": ["CUPID"],
    "FMCG - Wellness": ["ZYDUSWELL"],
    "Telecom Services": ["BHARTIHEXA", "RAILTEL", "TTML"],
    "Telecom Infra & Equipment": ["ROUTE"],
    "Water Treatment & Environment": ["EIEL", "IONEXCHANG", "REFEX", "WABAG"],
    "NBFC - Lending": ["IFCI", "SUNDARMFIN"],
    "Cement": ["JSWCEMENT", "ORIENTCEM", "PRSMJOHNSN"],
}
RULES_FINAL2 = {
    "Cement": ["CEMPRO"],
    "Solar & Renewable Equipment": ["SWSOLAR"],
    "Logistics & Freight": ["CMSINFO"],
    "Media & Entertainment": ["PFOCUS", "SAREGAMA", "TIPSMUSIC"],
    "Paper & Forest Products": ["JKPAPER"],
}
for _d in (RULES_FINAL, RULES_FINAL2):
    for _ind, _syms in _d.items():
        RULES.setdefault(_ind, [])
        RULES[_ind].extend(s for s in _syms if s not in RULES[_ind])

# ---- yfinance label -> curated label (fold synonyms so widened names don't fragment) -------
# Only CLEAR synonyms are mapped; ambiguous yfinance labels keep their own (and become their
# own granular industry, with the RS engine's min-members floor handling the thin tail).
NORMALIZE = {
    # pharma / healthcare
    "Drug Manufacturers - Specialty & Generic": "Pharma - Formulations",
    "Drug Manufacturers - General": "Pharma - Formulations",
    "Medical Care Facilities": "Hospitals",
    # textiles / apparel / retail
    "Textile Manufacturing": "Textiles",
    "Apparel Manufacturing": "Textiles",
    "Apparel Retail": "Retail",
    "Department Stores": "Retail",
    "Footwear & Accessories": "Footwear",
    # construction / infra / realty / materials
    "Engineering & Construction": "EPC & Infrastructure",
    "Infrastructure Operations": "EPC & Infrastructure",
    "Real Estate - Development": "Realty",
    "Real Estate Services": "Realty",
    "Building Products & Equipment": "Building Products",
    "Building Materials": "Building Products",
    # IT
    "Information Technology Services": "IT Services",
    "Software - Application": "IT - Products & Platforms",
    "Software - Infrastructure": "IT - Products & Platforms",
    # financials
    "Capital Markets": "Capital Markets & AMC",
    "Asset Management": "Capital Markets & AMC",
    "Credit Services": "NBFC - Lending",
    "Banks - Regional": "Banks - Private",
    # industrials / machinery / electrical / metals
    "Specialty Industrial Machinery": "Industrial Machinery & EPC",
    "Electrical Equipment & Parts": "Electrical Equipment",
    "Metal Fabrication": "Steel",
    "Aluminum": "Aluminium",
    "Other Industrial Metals & Mining": "Diversified Metals",
    # auto / logistics / packaging
    "Auto Parts": "Auto Ancillary - Parts",
    "Integrated Freight & Logistics": "Logistics & Freight",
    "Packaging & Containers": "Industrial Packaging",
    # consumer / FMCG / hospitality / media
    "Packaged Foods": "FMCG - Packaged Foods",
    "Confectioners": "FMCG - Packaged Foods",
    "Household & Personal Products": "FMCG - Personal & Home Care",
    "Beverages - Wineries & Distilleries": "Beverages & Distilleries",
    "Furnishings, Fixtures & Appliances": "Consumer Durables / Appliances",
    "Lodging": "Hotels & Travel",
    "Resorts & Casinos": "Hotels & Travel",
    "Entertainment": "Media & Entertainment",
    "Broadcasting": "Media & Entertainment",
    # paper / services / energy / telecom / defense / solar
    "Paper & Paper Products": "Paper & Forest Products",
    "Lumber & Wood Production": "Paper & Forest Products",
    "Staffing & Employment Services": "Staffing & BPO",
    "Education & Training Services": "Education",
    "Oil & Gas Refining & Marketing": "Refiner & OMC",
    "Oil & Gas Equipment & Services": "Oilfield Services & Equipment",
    "Communication Equipment": "Telecom Infra & Equipment",
    "Aerospace & Defense": "Defense",
    "Solar": "Solar & Renewable Equipment",
}


def load_rows(f):
    return list(csv.DictReader(open(os.path.join(HERE, f), encoding="utf-8-sig")))


def build():
    ntm = load_rows("nse_ntm.csv")
    try:
        n500 = load_rows("nse_n500.csv")
    except Exception:
        n500 = []
    macro = {}      # sym -> NSE macro sector
    names = {}      # sym -> company name
    for r in ntm + n500:
        s = r["Symbol"].strip().upper()
        if s.startswith("DUMMY"):                  # NSE demerger placeholders, not tradeable
            continue
        macro.setdefault(s, r["Industry"].strip())
        names.setdefault(s, r["Company Name"].strip())
    for s, m in MACRO_FIX.items():
        if s in macro:
            macro[s] = m

    # existing yfinance granular (skip placeholders == macro name); fold synonyms into curated vocab
    yf_sub, yf_sec = {}, {}
    try:
        old = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))
        for s, info in old.items():
            su = s.upper()
            ind = (info or {}).get("industry")
            sec = (info or {}).get("sector")
            if sec and sec.lower() != "others":
                yf_sec[su] = sec
            if ind and ind != sec and ind.lower() != "others":
                yf_sub[su] = NORMALIZE.get(ind, ind)
    except Exception:
        pass

    # WIDEN the universe beyond NTM+N500: pull in any liquid name yfinance classified by sector.
    # Use the yf sector as that name's macro bucket; grab a display name from marleg_symbols.json.
    sym_names = {}
    try:
        for r in json.load(open(os.path.join(HERE, "marleg_symbols.json"), encoding="utf-8")):
            if isinstance(r, dict) and r.get("s"):
                sym_names[r["s"].upper()] = r.get("n") or r.get("name") or r["s"].upper()
    except Exception:
        pass
    widened = 0
    for su, sec in yf_sec.items():
        if su not in macro:
            macro[su] = sec
            names.setdefault(su, sym_names.get(su, su))
            widened += 1

    sym_to_curated = {}
    for ind, syms in RULES.items():
        for s in syms:
            sym_to_curated[s.upper()] = ind

    by_symbol, src_count = {}, collections.Counter()
    universe = set(macro)                       # the liquid universe (NTM ∪ N500)
    for s in sorted(universe):
        if s in sym_to_curated:
            ind, src = sym_to_curated[s], "curated"
        elif s in yf_sub:
            ind, src = yf_sub[s], "yfinance"
        else:
            ind, src = macro.get(s, "Others"), "macro-fallback"
        src_count[src] += 1
        by_symbol[s] = {"macro": macro.get(s, "Others"), "industry": ind, "name": names.get(s, s)}

    by_industry = collections.defaultdict(list)
    macro_to_ind = collections.defaultdict(set)
    for s, info in by_symbol.items():
        by_industry[info["industry"]].append(s)
        macro_to_ind[info["macro"]].add(info["industry"])

    out = {
        "by_symbol": by_symbol,
        "by_industry": {k: sorted(v) for k, v in sorted(by_industry.items())},
        "industries": sorted(by_industry),
        "macro_to_industries": {k: sorted(v) for k, v in sorted(macro_to_ind.items())},
        "coverage": {
            "universe": len(universe),
            "widened_from_yf": widened,
            "curated": src_count["curated"],
            "yfinance": src_count["yfinance"],
            "macro_fallback": src_count["macro-fallback"],
            "distinct_industries": len(by_industry),
            "curated_industries": len(RULES),
        },
    }
    return out


def main():
    out = build()
    cov = out["coverage"]
    print("Marle-G industry taxonomy")
    print(f"  liquid universe (NTM+N500): {cov['universe']}")
    print(f"  curated granular   : {cov['curated']}  ({cov['curated']*100//cov['universe']}%)")
    print(f"  yfinance granular  : {cov['yfinance']}")
    print(f"  macro fallback     : {cov['macro_fallback']}")
    print(f"  distinct industries: {cov['distinct_industries']}  (of which {cov['curated_industries']} curated cascade nodes)")
    big = sorted(out["by_industry"].items(), key=lambda kv: -len(kv[1]))[:12]
    print("  largest industries :", ", ".join(f"{k}({len(v)})" for k, v in big))
    if "--check" not in sys.argv:
        path = os.path.join(HERE, "marleg_industry_taxonomy.json")
        json.dump(out, open(path, "w"), indent=1)
        print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
