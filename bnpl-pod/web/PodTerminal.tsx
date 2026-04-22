// PodTerminal.tsx
// Single-file institutional BNPL trading-pod terminal.
// All numeric literals in JSX read from POD_SNAPSHOT below.
// Runtime: React 18 + Tailwind + recharts + lucide-react (UMD globals in the host HTML).

const { useState, useEffect, useRef, useMemo } = React;
const {
  LineChart, Line, AreaChart, Area, RadarChart, Radar,
  PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine,
  ResponsiveContainer, Legend, ComposedChart,
} = Recharts;
const L = lucide; // { Activity, TrendingUp, ... }

// ─────────────────────────────────────────────────────────────
// POD_SNAPSHOT — single source of truth.
// Plausible values matching the reference ranges.
// ─────────────────────────────────────────────────────────────
const POD_SNAPSHOT = {
  meta: {
    version: "v4.1",
    asOf: "2026-04-17",
    commit: "abc123f",
    warehouse: "warehouse.duckdb",
    tradeMode: "paper-trade only",
    gates: "BSI × MOVE × CCDII (+ |z|≥10 bypass, SCP telemetry)",
  },
  ticker: [
    { sym: "AFRM",   val: "64.50",  delta: "-1.20%", dir: "down" },
    { sym: "SQ",     val: "58.12",  delta: "+0.34%", dir: "up"   },
    { sym: "PYPL",   val: "71.08",  delta: "-0.21%", dir: "down" },
    { sym: "HYG",    val: "80.65",  delta: "+0.10%", dir: "up"   },
    { sym: "JNK",    val: "97.42",  delta: "+0.06%", dir: "up"   },
    { sym: "MOVE",   val: "76.1",   delta: "+1.80",  dir: "up"   },
    { sym: "VIX",    val: "14.23",  delta: "-0.41",  dir: "down" },
    { sym: "SOFR",   val: "5.33%",  delta: "+1bp",   dir: "up"   },
    { sym: "10Y-3M", val: "-0.41",  delta: "-3bp",   dir: "down" },
    { sym: "BSI z",  val: "+0.08",  delta: "+0.03",  dir: "up"   },
    { sym: "SCP",    val: "186bp",  delta: "-4bp",   dir: "down" },
    { sym: "DTC",    val: "1.12x",  delta: "flat",   dir: "flat" },
    { sym: "next·catalyst", val: "CCD II", delta: "215d", dir: "flat" },
  ],
  bsi: {
    current: 0.08,
    peak30d: 4.15,
    peakDate: "Mar-26",
    allTimeHigh: 4.15,
    mean180d: -0.43,
    redGlowThreshold: 1.5,
    spark12m: [
      -0.42,-0.31,-0.18, 0.05, 0.22, 0.58, 1.14, 2.31, 4.15, 2.02,
       0.61, 0.08,
    ],
  },
  move: {
    current: 76.1,
    gate: 120,
    distance: 43.9,
    ma30d: 74.8,
    ytdHigh: 82.0,
    floor: 55,
    ceiling: 140,
  },
  gates: {
    state: "STAND-DOWN", // or "FIRING" or "BYPASS"
    ladder: [
      { id: "G1", name: "BSI z-score",     current: 0.08,  threshold: 1.50, unit: "σ",  hold: true  },
      { id: "G2", name: "MOVE Index",      current: 76.1,  threshold: 120,  unit: "",   hold: true  },
      { id: "G3", name: "CCD-II T-minus",  current: 215,   threshold: 30,   unit: "d",  hold: true  },
    ],
    bypass: {
      z: 0.08,                 // current |BSI z|
      threshold: 10.0,         // super-threshold
      fired: false,            // true on 17-Jan-2025 @ +27σ
      rationale: "behavioural top-of-funnel panic override",
    },
    telemetry: {
      scp:  { current: 186,  threshold: 325, unit: "bp",
              note: "non-gating · retained from v4.1 draft; TRS-only expression has no equity-short leg" },
    },
  },
  catalyst: {
    name: "CCD II transposition",
    date: "2026-11-20",
    daysTo: 215,
    materiality: 1.00,
    previous: { name: "CFPB interp rule", date: "2024-05-22", materiality: 0.95 },
  },
  backtest: {
    windows: [
      { id: "KLARNA",   label: "Klarna",    catalyst: "2022-07-13", T: 61 },
      { id: "AFRM_1",   label: "Affirm-1",  catalyst: "2023-02-21", T: 58 },
      { id: "AFRM_2",   label: "Affirm-2",  catalyst: "2024-05-22", T: 60 },
      { id: "CFPB",     label: "CFPB",      catalyst: "2024-05-22", T: 63 },
    ],
    // cumulative TRS P&L per strategy, per window; daily index 0..60
    series: {
      KLARNA: genSeries(61, { naive: -0.0002, fix3: 0.0008, inst: 0.0042 }),
      AFRM_1: genSeries(58, { naive: -0.0015, fix3: 0.0006, inst: 0.0031 }),
      AFRM_2: genSeries(60, { naive:  0.0001, fix3: 0.0012, inst: 0.0058 }),
      CFPB:   genSeries(63, { naive: -0.0021, fix3: -0.0003, inst: 0.0038 }),
    },
    stats: {
      KLARNA: { naiveSh: -0.22, fix3Sh: 0.14, instSh: 1.82, instRet: 0.0042 },
      AFRM_1: { naiveSh: -1.05, fix3Sh: 0.08, instSh: 2.41, instRet: 0.0031 },
      AFRM_2: { naiveSh:  0.11, fix3Sh: 0.73, instSh: 5.62, instRet: 0.0058 },
      CFPB:   { naiveSh: -1.88, fix3Sh: -0.21, instSh: 3.14, instRet: 0.0038 },
    },
  },
  granger: genGranger(),
  stressTimeline: genStressTimeline(),
  radar: {
    axes: [
      { key: "BSI",  current: 0.08 / 1.5, threshold: 1.0, curAbs: "+0.08",  thrAbs: "+1.50"  },
      { key: "MOVE", current: 76.1 / 120, threshold: 1.0, curAbs: "76.1",   thrAbs: "120"    },
      { key: "SCP",  current: 186 / 325,  threshold: 1.0, curAbs: "186bp",  thrAbs: "325bp"  },
      { key: "DTC",  current: 1.12 / 1.8, threshold: 1.0, curAbs: "1.12x",  thrAbs: "1.80x"  },
    ],
  },
  agentLog: genAgentLog(80),
};

// helpers ───────────────────────────────────────────────────────────────
function genSeries(n: number, drift: { naive:number, fix3:number, inst:number }) {
  let a = 0, b = 0, c = 0;
  const pts: any[] = [];
  // deterministic pseudo-noise
  const rnd = (seed: number) => {
    const x = Math.sin(seed * 9301 + 49297) * 233280;
    return x - Math.floor(x);
  };
  for (let i = 0; i < n; i++) {
    a += drift.naive  + (rnd(i*1.1)-0.5) * 0.0008;
    b += drift.fix3   + (rnd(i*2.3)-0.5) * 0.0006;
    c += drift.inst   + (rnd(i*3.7)-0.5) * 0.0005;
    pts.push({ t: i, NAIVE: +a.toFixed(5), FIX3_ONLY: +b.toFixed(5), INSTITUTIONAL: +c.toFixed(5) });
  }
  return pts;
}
function genGranger() {
  const out: any[] = [];
  for (let i = 0; i < 48; i++) {
    const base = Math.sin(i/4) * 0.9 + Math.sin(i/7) * 0.6;
    const bsi  = base + (Math.sin(i*1.3)*0.15);
    out.push({ week: i, bsi: +bsi.toFixed(3), bsiLag: null });
  }
  // phase-shifted copy +6 weeks
  for (let i = 6; i < out.length; i++) {
    out[i].bsiLag = out[i-6].bsi * 0.92;
  }
  return out;
}
function genStressTimeline() {
  const out: any[] = [];
  const fires: {m:number, label:string}[] = [];
  for (let m = 0; m < 36; m++) {
    const bsi  = Math.sin(m/3.2) * 1.1 + (m===14 ? 3.0 : m===22 ? 2.1 : 0) + (Math.cos(m/1.7)*0.3);
    const move = 60 + Math.abs(Math.sin(m/2.8))*30 + (m===14 ? 55 : m===22 ? 35 : 0);
    out.push({ m, label: `M${m}`, bsi: +bsi.toFixed(2), move: +move.toFixed(1), moveNorm: +(move/100).toFixed(3) });
  }
  return out;
}
function genAgentLog(n: number) {
  const agents = ["MACRO", "QUANT", "RISK"];
  const models = ["claude-haiku-4-5", "nemotron-mini-4b-instruct", "gpt-4o-mini", "qwen2.5-7b", "mistral-7b-inst"];
  const msgs = [
    "BSI z-score drift negligible; no regime change flagged.",
    "rolling σ on AFRM TRS < 2bp — carry trade unchanged.",
    "MOVE at 76.1 is 43.9 under gate; G3 HOLD.",
    "proposing FIX3 retune on AFRM_2 window; ΔSharpe +0.18.",
    "SOFR-OIS basis unchanged. no liquidity stress.",
    "CCD-II countdown 215d; materiality ceiling 1.00.",
    "duration adjustment on vs NAIVE fork: carry +4bp.",
    "granger p=0.031 at lag 6w; leading-indicator stable.",
    "SCP excess spread 186bp, 43% of gate threshold.",
    "no fire condition met; trade state STAND-DOWN.",
    "requesting approval for sim-layer patch v4.1.3.",
    "HYG-JNK basis +0.04, credit tone neutral.",
    "consumer-stress composite flat w/w; σ=0.02.",
    "last 30d approvals: 0/61 days. gate set correctly.",
  ];
  const out: any[] = [];
  const base = Date.parse("2026-04-17T14:03:00Z");
  for (let i = 0; i < n; i++) {
    const a = agents[i % agents.length];
    const m = models[i % models.length];
    const mins = Math.floor(i/3);
    const secs = (i*13) % 60;
    const ts   = new Date(base - i*47_000).toISOString().replace("T"," ").slice(0,19);
    const msg  = msgs[i % msgs.length];
    out.push({
      ts,
      agent: a,
      model: m,
      tokens: 180 + ((i*37)%320),
      latencyMs: 820 + ((i*113)%2600),
      msg,
    });
  }
  return out;
}

// colors ─────────────────────────────────────────────────────────────────
const C = {
  bg:       "#070a10",
  card:     "#0b111b",
  cardAlt:  "#0e1524",
  border:   "#1c2433",
  borderHi: "#2a3448",
  text:     "#cfd6e4",
  dim:      "#6b7689",
  muted:    "#9aa3b5",
  cyan:     "#22d3ee",
  cyanDim:  "#0e7490",
  crimson:  "#e11d48",
  amber:    "#f59e0b",
  green:    "#65a30d",
  violet:   "#a78bfa",
  grid:     "#1b2434",
};

// ─────────────────────────────────────────────────────────────
// TICKER STRIP
// ─────────────────────────────────────────────────────────────
function Ticker() {
  const items = POD_SNAPSHOT.ticker;
  const doubled = [...items, ...items];
  return (
    <div className="w-full h-7 border-b border-[var(--border)] bg-[var(--card)] overflow-hidden relative">
      <div className="ticker-track whitespace-nowrap flex items-center h-full text-[11px]">
        {doubled.map((t, i) => (
          <span key={i} className="inline-flex items-center gap-2 px-4 border-r border-[var(--border)]">
            <span className="text-[var(--muted)] tracking-wider">{t.sym}</span>
            <span className="font-mono text-[var(--text)]">{t.val}</span>
            <span className={`font-mono ${
              t.dir==="up" ? "text-[#86efac]"
              : t.dir==="down" ? "text-[#fda4af]"
              : "text-[var(--dim)]"
            }`}>{t.delta}</span>
          </span>
        ))}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// TOP BAR
// ─────────────────────────────────────────────────────────────
function TopBar() {
  const m = POD_SNAPSHOT.meta;
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);
  return (
    <div className="h-10 flex items-center justify-between px-3 border-b border-[var(--border)] bg-[var(--card)]">
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-sm bg-cyan-400 shadow-[0_0_8px_rgba(34,211,238,0.7)]"></div>
          <span className="text-[13px] tracking-[0.18em] font-semibold">BNPL·POD</span>
          <span className="text-[var(--dim)] text-[11px] tracking-widest">TERMINAL {m.version}</span>
        </div>
        <div className="h-4 w-px bg-[var(--border)]"></div>
        <div className="flex items-center gap-1.5 text-[10px] tracking-widest text-[var(--dim)] uppercase">
          <L.Database size={12}/> <span>{m.warehouse}</span>
          <span className="text-[var(--borderHi)]">·</span>
          <span>as-of {m.asOf}</span>
          <span className="text-[var(--borderHi)]">·</span>
          <span>commit {m.commit}</span>
        </div>
      </div>
      <div className="flex items-center gap-4 text-[10px] tracking-widest uppercase">
        <span className="flex items-center gap-1 text-[var(--dim)]">
          <L.Wifi size={11}/> link·OK
        </span>
        <span className="flex items-center gap-1 text-[var(--dim)]">
          <L.ShieldCheck size={11}/> {m.tradeMode}
        </span>
        <span className="font-mono text-[var(--text)] tabular-nums">
          {now.toISOString().slice(11,19)}z
        </span>
        <span className="flex items-center gap-1.5 px-2 py-0.5 border border-[var(--border)] rounded-sm text-[var(--muted)]">
          <L.Command size={10}/> K
        </span>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// HELP POPOVER — click the "?" to learn what a card measures
// Four labelled fields: WHAT · HOW · VALUES · PAPER §
// ─────────────────────────────────────────────────────────────
type HelpSpec = { what:string, how:string, values:string, section:string };

function HelpPopover({ spec, align="right" }:{ spec:HelpSpec, align?:"right"|"left" }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<any>(null);
  useEffect(() => {
    if (!open) return;
    const onDoc = (e:any) => {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    };
    const onKey = (e:any) => { if (e.key === "Escape") setOpen(false); };
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);
  return (
    <span ref={ref} className="relative inline-flex items-center">
      <button
        type="button"
        aria-label="What does this mean?"
        aria-expanded={open}
        onClick={(e) => { e.stopPropagation(); setOpen(v => !v); }}
        className={`inline-flex items-center justify-center w-[14px] h-[14px] rounded-full border border-[var(--border)] text-[var(--dim)] hover:text-[var(--text)] hover:border-[var(--muted)] transition-colors ${open ? "text-[var(--text)] border-[var(--cyan)]" : ""}`}
      >
        <L.HelpCircle size={10}/>
      </button>
      {open && (
        <div
          role="dialog"
          className={`absolute top-[18px] ${align === "right" ? "right-0" : "left-0"} z-50 w-[280px] rounded-md border border-[var(--border)] bg-[var(--card)] shadow-[0_8px_24px_rgba(0,0,0,0.6)] p-2.5 text-[10.5px] leading-snug text-[var(--text)]`}
        >
          <dl className="grid grid-cols-[52px_1fr] gap-x-2 gap-y-1">
            <dt className="text-[9px] tracking-widest uppercase text-[var(--cyan)]">What</dt>
            <dd className="text-[var(--text)] normal-case">{spec.what}</dd>
            <dt className="text-[9px] tracking-widest uppercase text-[var(--cyan)]">How</dt>
            <dd className="text-[var(--muted)] normal-case">{spec.how}</dd>
            <dt className="text-[9px] tracking-widest uppercase text-[var(--cyan)]">Values</dt>
            <dd className="font-mono text-[var(--text)]">{spec.values}</dd>
            <dt className="text-[9px] tracking-widest uppercase text-[var(--cyan)]">Paper</dt>
            <dd className="font-mono text-[var(--amber)]">{spec.section}</dd>
          </dl>
          <div className="mt-1.5 pt-1.5 border-t border-[var(--border)] text-[9px] tracking-widest uppercase text-[var(--dim)] text-right">
            Esc / click-outside to dismiss
          </div>
        </div>
      )}
    </span>
  );
}

// ─────────────────────────────────────────────────────────────
// CARD PRIMITIVE
// ─────────────────────────────────────────────────────────────
function Card({ label, icon, timestamp, children, className="", accent="", help}:
  { label:string, icon?:any, timestamp?:string, children:any, className?:string, accent?:string, help?:HelpSpec }) {
  return (
    <div className={`relative border border-[var(--border)] bg-[var(--card)] rounded-lg shadow-inner-card ${className}`}>
      <div className="flex items-center justify-between px-3 pt-2 pb-1.5">
        <div className="flex items-center gap-1.5 text-[10px] tracking-[0.22em] uppercase text-[var(--muted)]">
          {icon} <span>{label}</span>
        </div>
        <div className="flex items-center gap-2">
          {timestamp && (
            <div className="text-[9px] tracking-widest font-mono text-[var(--dim)]">
              {timestamp}
            </div>
          )}
          {help && <HelpPopover spec={help}/>}
        </div>
      </div>
      {accent && (
        <div className={`absolute left-0 top-2 bottom-2 w-[2px] rounded-r ${accent}`}></div>
      )}
      {children}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// ROW 1 — EXECUTIVE BENTO
// ─────────────────────────────────────────────────────────────
function ExecBento() {
  const s = POD_SNAPSHOT;
  const bsiRed = s.bsi.current >= s.bsi.redGlowThreshold;
  return (
    <div className="grid grid-cols-4 gap-2 p-2">
      {/* A: BSI */}
      <Card label="BSI · Consumer-Stress" icon={<L.Activity size={12}/>} timestamp={`as-of ${s.meta.asOf}`}
        help={{
          what: "Weekly BNPL borrower-stress index — our proprietary leading signal.",
          how:  "Six-pillar weighted aggregate (CFPB, Google Trends, Reddit, App Store, MOVE, Firm Vitality), 180-day residual z-score.",
          values: "σ units · redlines: +1.50σ (Gate 1 fires) · +10σ (super-threshold bypass). Typical band −3 to +5.",
          section: "§4.2 — BSI construction",
        }}>
        <div className="px-3 pb-3 relative">
          <div className="flex items-end justify-between">
            <div>
              <div className={`font-mono text-[34px] leading-none ${bsiRed ? "text-[var(--crimson)] drop-shadow-[0_0_12px_rgba(225,29,72,0.55)]" : "text-[var(--text)]"}`}>
                {s.bsi.current >= 0 ? "+" : ""}{s.bsi.current.toFixed(2)}
              </div>
              <div className="text-[9px] tracking-widest text-[var(--dim)] uppercase mt-0.5">z-score · daily</div>
            </div>
            <MiniSpark data={s.bsi.spark12m} stroke={bsiRed ? C.crimson : C.cyan} w={110} h={40}/>
          </div>
          <dl className="mt-2 grid grid-cols-2 gap-x-3 gap-y-0.5 text-[10.5px]">
            <dt className="text-[var(--dim)]">peak·30d</dt>
            <dd className="font-mono text-right text-[var(--text)]">+{s.bsi.peak30d.toFixed(2)}σ <span className="text-[var(--dim)]">({s.bsi.peakDate})</span></dd>
            <dt className="text-[var(--dim)]">all-time·hi</dt>
            <dd className="font-mono text-right text-[var(--text)]">+{s.bsi.allTimeHigh.toFixed(2)}σ</dd>
            <dt className="text-[var(--dim)]">180d·μ</dt>
            <dd className="font-mono text-right text-[var(--text)]">{s.bsi.mean180d.toFixed(2)}</dd>
          </dl>
        </div>
      </Card>

      {/* B: MOVE */}
      <Card label="MOVE · Bond-Market Fear" icon={<L.LineChart size={12}/>} timestamp={`as-of ${s.meta.asOf}`}
        help={{
          what: "Treasury-options implied-volatility index — the bond market's fear gauge.",
          how:  "30-day moving average of ICE BofAML MOVE index, sourced live from FRED.",
          values: "bp · gate 120 (Gate 2 fires above). YTD band: floor 55 · ceiling 140. Pre-2022 regime rarely > 90.",
          section: "§4.3 — Macro gate",
        }}>
        <div className="px-3 pb-3">
          <div className="flex items-end justify-between">
            <div>
              <div className="font-mono text-[34px] leading-none text-[var(--text)]">{s.move.current.toFixed(1)}</div>
              <div className="text-[9px] tracking-widest text-[var(--dim)] uppercase mt-0.5">index · live</div>
            </div>
            <div className="text-right text-[10.5px]">
              <div className="text-[var(--dim)]">gate @ <span className="font-mono text-[var(--amber)]">{s.move.gate}</span></div>
              <div className="font-mono text-[var(--muted)]">Δ {s.move.distance.toFixed(1)} under</div>
            </div>
          </div>
          <GateProgress current={s.move.current} floor={s.move.floor} gate={s.move.gate} ceiling={s.move.ceiling} ma={s.move.ma30d}/>
          <dl className="mt-2 grid grid-cols-2 gap-x-3 text-[10.5px]">
            <dt className="text-[var(--dim)]">30d·MA</dt>
            <dd className="font-mono text-right text-[var(--text)]">{s.move.ma30d.toFixed(1)}</dd>
            <dt className="text-[var(--dim)]">YTD·hi</dt>
            <dd className="font-mono text-right text-[var(--text)]">{s.move.ytdHigh.toFixed(1)}</dd>
          </dl>
        </div>
      </Card>

      {/* C: Gate ladder — clean 3-gate AND (bypass + SCP moved to dedicated OverridesStrip row) */}
      <Card label="Gate Ladder · 3-Gate AND" icon={<L.ListChecks size={12}/>} timestamp={`state ${s.gates.state}`}
        help={{
          what: "Deterministic 3-gate AND — the sole trade-approval authority (zero LLM).",
          how:  "G1: BSI z ≥ +1.50σ · G2: MOVE MA30 ≥ 120 · G3: CCD-II T-minus ≤ 30d. All three must fire to approve.",
          values: "HOLD / FIRE per gate · trade state STAND-DOWN | FIRING | BYPASS. Bypass overrides G2/G3 at |z|≥10.",
          section: "§4.4 compliance · §8.5 bypass",
        }}>
        <div className="px-3 pb-3">
          <div className="space-y-1.5">
            {s.gates.ladder.map(g => {
              const pct = Math.min(1, Math.max(0, Math.abs(g.current) / g.threshold));
              return (
                <div key={g.id} className="flex items-center gap-2 text-[10.5px]">
                  <span className="font-mono text-[var(--muted)] w-5">{g.id}</span>
                  <span className="text-[var(--dim)] w-24 truncate">{g.name}</span>
                  <div className="flex-1 h-1 bg-[var(--border)] rounded relative">
                    <div className="absolute inset-y-0 left-0 rounded" style={{ width: `${pct*100}%`, background: g.hold ? C.cyan : C.crimson, opacity: 0.75 }}/>
                  </div>
                  <span className="font-mono text-right w-20 text-[var(--text)]">{g.current}{g.unit}/{g.threshold}{g.unit}</span>
                  <span className={`font-mono text-[9.5px] tracking-widest ${g.hold ? "text-[var(--cyan)]" : "text-[var(--crimson)]"}`}>
                    {g.hold ? "HOLD" : "FIRE"}
                  </span>
                </div>
              );
            })}
          </div>

          <div className="mt-3 border-t border-[var(--border)] pt-2 flex items-center justify-between">
            <span className="text-[10px] tracking-widest text-[var(--dim)] uppercase">Trade State</span>
            <span className={`font-mono text-[13px] tracking-[0.2em] ${
              s.gates.state === "STAND-DOWN"
                ? "text-[var(--cyan)] drop-shadow-[0_0_6px_rgba(34,211,238,0.5)]"
                : s.gates.state === "BYPASS"
                  ? "text-[var(--amber)] drop-shadow-[0_0_8px_rgba(245,158,11,0.6)]"
                  : "text-[var(--crimson)] drop-shadow-[0_0_8px_rgba(225,29,72,0.6)]"
            }`}>
              {s.gates.state}
            </span>
          </div>
        </div>
      </Card>

      {/* D: Next catalyst */}
      <Card label="Next Catalyst" icon={<L.CalendarClock size={12}/>} timestamp={s.catalyst.date}
        help={{
          what: "Nearest regulatory event that could reprice the BNPL sector.",
          how:  "Calendar resolution of `regulatory_catalysts` table (CFPB rules, EU CCD II, state UDAP actions). Materiality scored [0,1] by paper authors.",
          values: "daysTo counts down to `catalyst.date`. Gate 3 fires when daysTo ≤ 30. Materiality 1.0 = full transposition.",
          section: "§6 — Regulatory catalyst ladder",
        }}>
        <div className="px-3 pb-3">
          <div className="flex items-end justify-between">
            <div>
              <div className="font-mono text-[34px] leading-none text-[var(--text)]">{s.catalyst.daysTo}<span className="text-[var(--dim)] text-[18px] ml-1">d</span></div>
              <div className="text-[9px] tracking-widest text-[var(--dim)] uppercase mt-0.5">to {s.catalyst.name}</div>
            </div>
            <div className="text-right">
              <div className="text-[9px] tracking-widest text-[var(--dim)] uppercase">materiality</div>
              <div className="font-mono text-[13px] text-[var(--cyan)]">{(s.catalyst.materiality*100).toFixed(0)}%</div>
            </div>
          </div>
          <div className="mt-2 h-1 bg-[var(--border)] rounded">
            <div className="h-full rounded" style={{ width: `${s.catalyst.materiality*100}%`, background: C.cyan }}/>
          </div>
          <div className="mt-2 text-[10.5px] text-[var(--dim)] flex items-start gap-1.5">
            <L.Rewind size={11} className="mt-0.5 shrink-0"/>
            <span>
              prev · <span className="text-[var(--muted)]">{s.catalyst.previous.name}</span>
              <span className="text-[var(--dim)]"> · {s.catalyst.previous.date} · </span>
              <span className="font-mono text-[var(--text)]">{(s.catalyst.previous.materiality*100).toFixed(0)}%</span>
            </span>
          </div>
        </div>
      </Card>
    </div>
  );
}

function MiniSpark({ data, stroke, w=100, h=36 }: any) {
  const min = Math.min(...data), max = Math.max(...data);
  const pts = data.map((v: number, i: number) => {
    const x = (i/(data.length-1)) * w;
    const y = h - ((v - min)/(max - min || 1)) * h;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return (
    <svg width={w} height={h} className="opacity-80">
      <polyline fill="none" stroke={stroke} strokeWidth={1.2} points={pts}/>
    </svg>
  );
}

function GateProgress({ current, floor, gate, ceiling, ma }: any) {
  const pct = (v: number) => ((v - floor) / (ceiling - floor)) * 100;
  return (
    <div className="mt-2">
      <div className="relative h-1.5 bg-[var(--border)] rounded">
        <div className="absolute inset-y-0 left-0 rounded" style={{ width: `${pct(current)}%`, background: C.cyan, opacity: 0.7 }}/>
        <div className="absolute top-[-3px] bottom-[-3px] w-[1px] bg-[var(--amber)]" style={{ left: `${pct(gate)}%` }}/>
        <div className="absolute top-[-3px] bottom-[-3px] w-[1px] bg-[var(--muted)]" style={{ left: `${pct(ma)}%`, opacity: 0.5 }}/>
      </div>
      <div className="flex justify-between mt-0.5 text-[8.5px] font-mono text-[var(--dim)]">
        <span>{floor}</span><span>gate {gate}</span><span>{ceiling}</span>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// ROW 2 — OVERRIDES & TELEMETRY STRIP
// Bypass alert (amber · Override) + SCP telemetry (muted · non-gating)
// Sits between the 4-card bento and the backtest canvas so the
// Gate Ladder card stays clean and the override semantics get
// visual weight of their own.
// ─────────────────────────────────────────────────────────────
function OverridesStrip() {
  const s = POD_SNAPSHOT;
  const bp = s.gates && (s.gates as any).bypass;
  const scp = s.gates && (s.gates as any).telemetry && (s.gates as any).telemetry.scp;
  if (!bp && !scp) return null;
  return (
    <div className="grid grid-cols-2 gap-2 px-2 mt-2">
      {/* Bypass · Override */}
      {bp && (() => {
        const absZ = Math.abs(bp.z);
        const pct = Math.min(1, absZ / bp.threshold);
        const fired = !!bp.fired;
        return (
          <div className={`relative border rounded-lg shadow-inner-card bg-[var(--card)] ${
            fired ? "border-[var(--amber)]/50 shadow-[0_0_18px_rgba(245,158,11,0.08)]" : "border-[var(--border)]"
          }`}>
            <div className={`absolute left-0 top-2 bottom-2 w-[2px] rounded-r ${fired ? "bg-[var(--amber)]" : "bg-[var(--border)]"}`}/>
            <div className="flex items-center justify-between px-3 pt-2 pb-1.5">
              <div className="flex items-center gap-1.5 text-[10px] tracking-[0.22em] uppercase text-[var(--muted)]">
                <L.Zap size={12} className={fired ? "text-[var(--amber)]" : "text-[var(--muted)]"}/>
                <span>Super-Threshold Bypass · Override</span>
              </div>
              <div className="flex items-center gap-2">
                <span className={`font-mono text-[9.5px] tracking-widest ${
                  fired
                    ? "text-[var(--amber)] drop-shadow-[0_0_6px_rgba(245,158,11,0.6)]"
                    : "text-[var(--muted)]"
                }`}>
                  {fired ? "BYPASS FIRED" : "ARMED"}
                </span>
                <HelpPopover spec={{
                  what: "Behavioural-panic override — approves the trade when BSI alone hits the extreme tail, even if MOVE and CCD-II disagree.",
                  how:  "Fires when |BSI z| ≥ 10σ (agents/compliance_engine.py:185). Bypass-fired decisions are stamped in pod_decisions for post-hoc audit.",
                  values: "ARMED = idle · BYPASS FIRED = trade approved on behavioural signal alone. Calibrated to fire ≤1× per 8 years (Type-I premium accepted).",
                  section: "§8.5 — Type-I premium",
                }}/>
              </div>
            </div>
            <div className="px-3 pb-3">
              <div className="flex items-end justify-between">
                <div>
                  <div className={`font-mono text-[28px] leading-none ${fired ? "text-[var(--amber)]" : "text-[var(--text)]"}`}>
                    {absZ.toFixed(2)}<span className="text-[var(--dim)] text-[14px] ml-1">σ</span>
                  </div>
                  <div className="text-[9px] tracking-widest text-[var(--dim)] uppercase mt-0.5">|BSI z| · current</div>
                </div>
                <div className="text-right text-[10.5px]">
                  <div className="text-[var(--dim)]">trigger @ <span className="font-mono text-[var(--amber)]">{bp.threshold.toFixed(1)}σ</span></div>
                  <div className="font-mono text-[var(--muted)]">Δ {(bp.threshold - absZ).toFixed(2)}σ under</div>
                </div>
              </div>
              <div className="mt-2 h-1 bg-[var(--border)] rounded relative">
                <div className="absolute inset-y-0 left-0 rounded" style={{ width: `${pct*100}%`, background: fired ? C.amber : C.cyan, opacity: 0.75 }}/>
                <div className="absolute top-[-3px] bottom-[-3px] w-[1px] bg-[var(--amber)]" style={{ left: "100%" }}/>
              </div>
              <div className="mt-1.5 text-[10px] text-[var(--dim)] leading-snug">
                {bp.rationale} · Type-I premium on fire (calibrated 1× in 2018–2026).
              </div>
            </div>
          </div>
        );
      })()}

      {/* SCP · Telemetry (non-gating) */}
      {scp && (() => {
        const pct = Math.min(1, scp.current / scp.threshold);
        return (
          <div className="relative border border-[var(--border)] rounded-lg shadow-inner-card bg-[var(--card)]">
            <div className="absolute left-0 top-2 bottom-2 w-[2px] rounded-r bg-[var(--border)]"/>
            <div className="flex items-center justify-between px-3 pt-2 pb-1.5">
              <div className="flex items-center gap-1.5 text-[10px] tracking-[0.22em] uppercase text-[var(--muted)]">
                <L.Gauge size={12}/>
                <span>SCP · Telemetry</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="font-mono text-[9.5px] tracking-widest text-[var(--muted)]">NON-GATING</span>
                <HelpPopover spec={{
                  what: "Squeeze-Compression Premium — observed equity-options skew beyond Heston baseline.",
                  how:  "Heston model fit, per-ticker residual skew z-score (quant/heston_scp.py). Demoted to telemetry post-Fix #2.",
                  values: "bp · displayed vs. threshold for context only. Does NOT contribute to the 3-gate AND.",
                  section: "§4.5 — SCP demotion",
                }}/>
              </div>
            </div>
            <div className="px-3 pb-3">
              <div className="flex items-end justify-between">
                <div>
                  <div className="font-mono text-[28px] leading-none text-[var(--text)]">
                    {scp.current}<span className="text-[var(--dim)] text-[14px] ml-1">{scp.unit}</span>
                  </div>
                  <div className="text-[9px] tracking-widest text-[var(--dim)] uppercase mt-0.5">excess spread · observed</div>
                </div>
                <div className="text-right text-[10.5px]">
                  <div className="text-[var(--dim)]">ref @ <span className="font-mono text-[var(--muted)]">{scp.threshold}{scp.unit}</span></div>
                  <div className="font-mono text-[var(--muted)]">{((scp.current/scp.threshold)*100).toFixed(0)}% of ref</div>
                </div>
              </div>
              <div className="mt-2 h-1 bg-[var(--border)] rounded relative">
                <div className="absolute inset-y-0 left-0 rounded" style={{ width: `${pct*100}%`, background: C.muted, opacity: 0.55 }}/>
              </div>
              <div className="mt-1.5 text-[10px] text-[var(--dim)] leading-snug">
                {scp.note}
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// ROW 3 — BACKTEST CANVAS
// ─────────────────────────────────────────────────────────────
function BacktestCanvas() {
  const bt = POD_SNAPSHOT.backtest;
  const [tab, setTab] = useState<"pnl" | "granger" | "stress">("pnl");
  const [win, setWin] = useState(bt.windows[0].id);

  const winMeta = bt.windows.find(w => w.id === win)!;
  const series = bt.series[win as keyof typeof bt.series];
  const stats  = bt.stats[win as keyof typeof bt.stats];

  return (
    <div className="mx-2 border border-[var(--border)] bg-[var(--card)] rounded-lg shadow-inner-card">
      <div className="flex items-center justify-between px-3 pt-2">
        <div className="flex items-center gap-4">
          <div className="text-[10px] tracking-[0.22em] uppercase text-[var(--muted)] flex items-center gap-1.5">
            <L.AreaChart size={12}/> Backtest Canvas
            <HelpPopover spec={{
              what: "Out-of-sample event-study proof: does our gate logic beat naive long-short on real BNPL stress events?",
              how:  "Three strategies compared per catalyst window: NAIVE (always on) · FIX3_ONLY (stop at 3 losses) · INSTITUTIONAL (full 3-gate + bypass). Cumulative TRS P&L, daily.",
              values: "Sharpe · MaxDD · GrossLev. Event windows: Klarna-22, Affirm-23, Affirm-24, CFPB-24. Daily index t=0 = catalyst date.",
              section: "§7 — Event study",
            }}/>
          </div>
          <div className="flex gap-1">
            {[
              { k: "pnl", label: "Event-Study P&L" },
              { k: "granger", label: "Granger Leading-Indicator" },
              { k: "stress", label: "Stress-Signal Timeline" },
            ].map(t => (
              <button key={t.k} onClick={() => setTab(t.k as any)}
                className={`px-2.5 py-1 text-[10px] tracking-widest uppercase border border-[var(--border)] rounded ${
                  tab===t.k ? "bg-[var(--cardAlt)] text-[var(--cyan)] border-[var(--cyan)]/40" : "text-[var(--dim)] hover:text-[var(--text)]"
                }`}>{t.label}</button>
            ))}
          </div>
        </div>
        {tab === "pnl" && (
          <div className="flex gap-1">
            {bt.windows.map(w => (
              <button key={w.id} onClick={() => setWin(w.id)}
                className={`px-2 py-0.5 text-[10px] font-mono border border-[var(--border)] rounded ${
                  win===w.id ? "bg-[var(--cyan)]/10 text-[var(--cyan)] border-[var(--cyan)]/40" : "text-[var(--dim)] hover:text-[var(--text)]"
                }`}>{w.label}</button>
            ))}
          </div>
        )}
      </div>

      <div className="px-3 pt-2 pb-2 h-[360px] flex flex-col">
        {tab === "pnl" && (
          <>
            <div className="flex items-center gap-4 text-[10px] text-[var(--dim)] font-mono">
              <LegendItem color={C.crimson} dashed label="NAIVE"         right={`Sh ${stats.naiveSh.toFixed(2)}`}/>
              <LegendItem color={C.amber}   dashed label="FIX3_ONLY"     right={`Sh ${stats.fix3Sh.toFixed(2)}`}/>
              <LegendItem color={C.cyan}           label="INSTITUTIONAL" right={`Sh ${stats.instSh.toFixed(2)} · ret ${(stats.instRet*100).toFixed(2)}%`}/>
              <div className="ml-auto text-[var(--dim)]">
                window {winMeta.label} · catalyst {winMeta.catalyst} · T={winMeta.T} bd
              </div>
            </div>
            <div className="flex-1">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={series} margin={{ top: 10, right: 14, bottom: 20, left: -10 }}>
                  <CartesianGrid stroke={C.grid} strokeDasharray="2 4"/>
                  <XAxis dataKey="t" stroke={C.dim} fontSize={10} tick={{ fill: C.dim }} tickFormatter={(v)=>`T+${v}`}/>
                  <YAxis stroke={C.dim} fontSize={10} tick={{ fill: C.dim }} tickFormatter={(v)=>v.toFixed(4)}/>
                  <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, fontSize: 11, fontFamily: "JetBrains Mono" }} labelStyle={{ color: C.dim }}/>
                  <ReferenceLine y={0} stroke={C.borderHi}/>
                  <Line type="monotone" dataKey="NAIVE"         stroke={C.crimson} strokeWidth={1.25} dot={false} strokeDasharray="4 3"/>
                  <Line type="monotone" dataKey="FIX3_ONLY"     stroke={C.amber}   strokeWidth={1.25} dot={false} strokeDasharray="4 3"/>
                  <Line type="monotone" dataKey="INSTITUTIONAL" stroke={C.cyan}    strokeWidth={1.75} dot={false}/>
                </LineChart>
              </ResponsiveContainer>
            </div>
          </>
        )}
        {tab === "granger" && (
          <>
            <div className="text-[10.5px] text-[var(--text)]">
              <span className="text-[var(--dim)] uppercase tracking-widest text-[9px] mr-2">leading-indicator</span>
              BSI → lag+6w · <span className="font-mono text-[var(--cyan)]">p&lt;0.05 at lags 4–8 weeks</span>
              <span className="text-[var(--dim)]"> — paper's empirical centerpiece</span>
            </div>
            <div className="flex-1">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={POD_SNAPSHOT.granger} margin={{ top: 10, right: 14, bottom: 20, left: -10 }}>
                  <CartesianGrid stroke={C.grid} strokeDasharray="2 4"/>
                  <XAxis dataKey="week" stroke={C.dim} fontSize={10} tick={{ fill: C.dim }} tickFormatter={(v)=>`w${v}`}/>
                  <YAxis yAxisId="l" stroke={C.dim} fontSize={10} tick={{ fill: C.dim }}/>
                  <YAxis yAxisId="r" orientation="right" stroke={C.dim} fontSize={10} tick={{ fill: C.dim }}/>
                  <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, fontSize: 11 }} labelStyle={{ color: C.dim }}/>
                  <ReferenceLine y={0} yAxisId="l" stroke={C.borderHi}/>
                  <Area yAxisId="l" type="monotone" dataKey="bsi" stroke={C.cyan} fill={C.cyan} fillOpacity={0.12} strokeWidth={1.2}/>
                  <Line yAxisId="r" type="monotone" dataKey="bsiLag" stroke={C.crimson} strokeWidth={1.2} strokeDasharray="4 3" dot={false}/>
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </>
        )}
        {tab === "stress" && (
          <>
            <div className="text-[10.5px] text-[var(--dim)]">
              BSI z-score (cyan area) + MOVE/100 (amber) · fire annotations at z≥+1.5 and MOVE≥120.
            </div>
            <div className="flex-1">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={POD_SNAPSHOT.stressTimeline} margin={{ top: 10, right: 14, bottom: 20, left: -10 }}>
                  <CartesianGrid stroke={C.grid} strokeDasharray="2 4"/>
                  <XAxis dataKey="label" stroke={C.dim} fontSize={9} tick={{ fill: C.dim }}/>
                  <YAxis stroke={C.dim} fontSize={10} tick={{ fill: C.dim }}/>
                  <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, fontSize: 11 }} labelStyle={{ color: C.dim }}/>
                  <ReferenceLine y={1.5} stroke={C.crimson} strokeDasharray="3 3" label={{ value: "z=+1.5", fill: C.crimson, fontSize: 9 }}/>
                  <ReferenceLine y={1.2} stroke={C.amber}   strokeDasharray="3 3" label={{ value: "MOVE=120", fill: C.amber, fontSize: 9 }}/>
                  <Area  type="monotone" dataKey="bsi"      stroke={C.cyan}  fill={C.cyan} fillOpacity={0.15} strokeWidth={1.2}/>
                  <Line  type="monotone" dataKey="moveNorm" stroke={C.amber} strokeWidth={1.2} dot={false}/>
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </>
        )}
        <div className="text-[9px] font-mono text-[var(--dim)] tracking-wider">
          src: {POD_SNAPSHOT.meta.warehouse} · as-of {POD_SNAPSHOT.meta.asOf} · commit {POD_SNAPSHOT.meta.commit}
        </div>
      </div>
    </div>
  );
}

function LegendItem({ color, label, dashed, right }: any) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <svg width="18" height="6"><line x1="0" y1="3" x2="18" y2="3" stroke={color} strokeWidth={1.6} strokeDasharray={dashed ? "3 2" : undefined}/></svg>
      <span className="text-[var(--muted)]">{label}</span>
      <span className="text-[var(--dim)]">· {right}</span>
    </span>
  );
}

// ─────────────────────────────────────────────────────────────
// ROW 4 — RISK & ATTRIBUTION
// ─────────────────────────────────────────────────────────────
function GateRadar() {
  const axes = POD_SNAPSHOT.radar.axes;
  const data = axes.map(a => ({ axis: a.key, current: +(a.current * 100).toFixed(1), threshold: 100 }));
  return (
    <div className="border border-[var(--border)] bg-[var(--card)] rounded-lg shadow-inner-card p-0">
      <div className="flex items-center justify-between px-3 pt-2 pb-1.5">
        <div className="flex items-center gap-1.5 text-[10px] tracking-[0.22em] uppercase text-[var(--muted)]">
          <L.Radar size={12}/> Gate Radar · Current Regime
          <HelpPopover spec={{
            what: "Four-axis normalised regime snapshot: BSI · MOVE · SCP · DTC.",
            how:  "Each axis plots current value / threshold × 100. Dashed ring = threshold (100%). Spokes beyond the ring = gate would fire.",
            values: "Axis scale 0–140%. Threshold=100 on every axis. DTC = Days-to-Cover (short-squeeze telemetry).",
            section: "§4 — Gate architecture",
          }}/>
        </div>
        <div className="text-[9px] tracking-widest font-mono text-[var(--dim)]">as-of {POD_SNAPSHOT.meta.asOf}</div>
      </div>
      <div className="px-3 pb-3 h-[330px] flex gap-3">
        <div className="flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <RadarChart data={data} outerRadius="74%">
              <PolarGrid stroke={C.border}/>
              <PolarAngleAxis dataKey="axis" tick={{ fill: C.muted, fontSize: 10, letterSpacing: 2 }}/>
              <PolarRadiusAxis domain={[0, 140]} tick={{ fill: C.dim, fontSize: 9 }} axisLine={false}/>
              <Radar name="threshold" dataKey="threshold" stroke={C.borderHi} strokeDasharray="3 3" fill="transparent"/>
              <Radar name="current"   dataKey="current"   stroke={C.cyan}     fill={C.cyan} fillOpacity={0.18} strokeWidth={1.4}/>
            </RadarChart>
          </ResponsiveContainer>
        </div>
        <div className="w-40 flex flex-col justify-center gap-2 text-[10.5px]">
          {axes.map(a => {
            const over = a.current > a.threshold;
            return (
              <div key={a.key} className={`border ${over ? "border-[var(--crimson)]/40" : "border-[var(--border)]"} rounded px-2 py-1.5`}>
                <div className="flex items-center justify-between">
                  <span className="tracking-widest text-[var(--muted)] uppercase text-[9.5px]">{a.key}</span>
                  <span className={`font-mono text-[9.5px] ${over ? "text-[var(--crimson)]" : "text-[var(--cyan)]"}`}>
                    {over ? "BREACH" : "OK"}
                  </span>
                </div>
                <div className="font-mono mt-0.5 flex items-baseline justify-between">
                  <span className="text-[var(--text)]">{a.curAbs}</span>
                  <span className="text-[var(--dim)] text-[9.5px]">/ {a.thrAbs}</span>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function EventStudyTable() {
  const bt = POD_SNAPSHOT.backtest;
  const shColor = (v: number) => v < 0 ? C.crimson : v < 1 ? C.amber : v <= 5 ? C.cyan : C.green;
  return (
    <div className="border border-[var(--border)] bg-[var(--card)] rounded-lg shadow-inner-card">
      <div className="flex items-center justify-between px-3 pt-2 pb-1.5">
        <div className="flex items-center gap-1.5 text-[10px] tracking-[0.22em] uppercase text-[var(--muted)]">
          <L.Table size={12}/> Event-Study Table
          <HelpPopover spec={{
            what: "Per-catalyst Sharpe + return comparison across the three strategies.",
            how:  "Sharpe = (mean daily return / std daily return) × sqrt(252) on the event window. `INST·ret` = cumulative TRS return of the institutional strategy over the window.",
            values: "Sharpe colour-code · red <0 · amber 0–1 · cyan 1–5 · green >5. Return in percentage points.",
            section: "§7.2 — Sharpe decomposition",
          }}/>
        </div>
        <div className="text-[9px] tracking-widest font-mono text-[var(--dim)]">n=4 windows</div>
      </div>
      <div className="px-3 pb-3">
        <table className="w-full text-[11px] border-collapse">
          <thead>
            <tr className="text-[9.5px] tracking-widest uppercase text-[var(--dim)] border-b border-[var(--border)]">
              <th className="text-left py-1.5 font-normal">Window</th>
              <th className="text-left font-normal">Date</th>
              <th className="text-right font-normal">NAIVE·Sh</th>
              <th className="text-right font-normal">FIX3·Sh</th>
              <th className="text-right font-normal">INST·Sh</th>
              <th className="text-right font-normal">INST·ret</th>
            </tr>
          </thead>
          <tbody className="font-mono">
            {bt.windows.map(w => {
              const s = bt.stats[w.id as keyof typeof bt.stats];
              return (
                <tr key={w.id} className="border-b border-[var(--border)]/60 last:border-0 hover:bg-[var(--cardAlt)]">
                  <td className="py-1.5 text-[var(--text)]">{w.label}</td>
                  <td className="text-[var(--muted)]">{w.catalyst}</td>
                  <ShCell v={s.naiveSh}/>
                  <ShCell v={s.fix3Sh}/>
                  <ShCell v={s.instSh}/>
                  <td className="text-right text-[var(--text)]">{(s.instRet*100).toFixed(2)}%</td>
                </tr>
              );
            })}
          </tbody>
        </table>
        <div className="mt-2 flex items-center gap-3 text-[9px] tracking-widest uppercase text-[var(--dim)]">
          <span>Sh · Sharpe</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{background:C.crimson}}/>&lt;0</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{background:C.amber}}/>0–1</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{background:C.cyan}}/>1–5</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{background:C.green}}/>&gt;5</span>
          <span className="ml-auto font-mono normal-case text-[var(--dim)]">
            src: {POD_SNAPSHOT.meta.warehouse} · commit {POD_SNAPSHOT.meta.commit}
          </span>
        </div>
      </div>
    </div>
  );
}
function ShCell({ v }: { v:number }) {
  const c = v < 0 ? C.crimson : v < 1 ? C.amber : v <= 5 ? C.cyan : C.green;
  return <td className="text-right"><span style={{ color: c }}>{v.toFixed(2)}</span></td>;
}

// ─────────────────────────────────────────────────────────────
// ROW 5 — AGENT DEBATE LOG
// ─────────────────────────────────────────────────────────────
function AgentLog() {
  const base = POD_SNAPSHOT.agentLog;
  const [rows, setRows] = useState(base.slice(0, 22));
  const idxRef = useRef(22);
  useEffect(() => {
    const id = setInterval(() => {
      const next = base[idxRef.current % base.length];
      const now = new Date().toISOString().replace("T"," ").slice(0,19);
      idxRef.current++;
      setRows(r => [{ ...next, ts: now, _new: true, _k: Math.random() }, ...r].slice(0, 60));
    }, 1800);
    return () => clearInterval(id);
  }, []);
  const agentColor = (a: string) => a === "MACRO" ? C.cyan : a === "QUANT" ? C.violet : C.amber;
  return (
    <div className="mx-2 border border-[var(--border)] bg-[var(--card)] rounded-lg shadow-inner-card">
      <div className="flex items-center justify-between px-3 pt-2 pb-1.5 border-b border-[var(--border)]">
        <div className="flex items-center gap-1.5 text-[10px] tracking-[0.22em] uppercase text-[var(--muted)]">
          <L.Terminal size={12}/> Agent Debate Log
          <span className="ml-3 w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
          <span className="text-[var(--dim)] normal-case tracking-wider">live · stream-attached</span>
          <HelpPopover spec={{
            what: "Live feed of LLM agents (Macro / Quant / Risk) reasoning about the current state — advisory-only, never gate.",
            how:  "Tail of `logs/agent_decisions/YYYY-MM-DD.jsonl`. Each row: ts · role · model · tokens · latency · message. Refreshes every ~1.8s in the tear-sheet.",
            values: "MACRO (cyan) = BSI + macro vol reasoning · QUANT (violet) = pricing + hazard · RISK (amber) = squeeze + drawdown. None influence compliance.",
            section: "§3 — LangGraph pod architecture",
          }}/>
        </div>
        <div className="flex items-center gap-3 text-[9px] tracking-widest uppercase text-[var(--dim)]">
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{background:C.cyan}}/>MACRO</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{background:C.violet}}/>QUANT</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{background:C.amber}}/>RISK</span>
        </div>
      </div>
      <div className="h-[200px] overflow-y-auto font-mono text-[11px]">
        <div className="grid grid-cols-[140px_72px_170px_60px_70px_1fr] gap-x-3 px-3 py-1.5 text-[9px] tracking-widest uppercase text-[var(--dim)] border-b border-[var(--border)] sticky top-0 bg-[var(--card)] z-10">
          <span>timestamp</span><span>agent</span><span>model</span><span className="text-right">tokens</span><span className="text-right">lat·ms</span><span>message</span>
        </div>
        {rows.map((r, i) => (
          <div key={r._k ?? r.ts + i}
               className={`grid grid-cols-[140px_72px_170px_60px_70px_1fr] gap-x-3 px-3 py-1 border-b border-[var(--border)]/50 hover:bg-[var(--cardAlt)] ${r._new ? "row-fade-in" : ""}`}>
            <span className="text-[var(--dim)]">{r.ts}</span>
            <span style={{ color: agentColor(r.agent) }}>{r.agent}</span>
            <span className="text-[var(--muted)]">{r.model}</span>
            <span className="text-right text-[var(--text)]">{r.tokens}</span>
            <span className="text-right text-[var(--text)]">{r.latencyMs}</span>
            <span className="text-[var(--text)] truncate">{r.msg}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// FOOTER
// ─────────────────────────────────────────────────────────────
function Footer() {
  const m = POD_SNAPSHOT.meta;
  return (
    <div className="h-6 border-t border-[var(--border)] bg-[var(--card)] flex items-center justify-between px-3 text-[10px] tracking-widest uppercase text-[var(--dim)]">
      <div className="flex items-center gap-3 font-mono normal-case tracking-wider">
        <span>bnpl-pod {m.version}</span>
        <span>·</span>
        <span>warehouse as-of {m.asOf}</span>
        <span>·</span>
        <span>commit {m.commit}</span>
        <span>·</span>
        <span>gates: {m.gates}</span>
        <span>·</span>
        <span className="text-[var(--amber)]">{m.tradeMode}</span>
      </div>
      <div className="flex items-center gap-1.5 font-mono normal-case tracking-wider">
        <span className="flex items-center gap-1"><L.Command size={10}/>K</span>
        <span>command palette</span>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// ROOT
// ─────────────────────────────────────────────────────────────
//
// Live-snapshot bridge: on mount, fetch ./pod_snapshot.json (sibling
// static file emitted by dashboard/build_snapshot.py). When it
// resolves, mutate POD_SNAPSHOT in place so every module-level
// reference sees the live data, then bump `hydrated` to re-key the
// tree — remounting resets every useEffect interval so sub-trees
// cleanly re-read. If the fetch fails (e.g. served from a Claude
// Artifact sandbox with no JSON sibling), we silently keep the mock
// constants and the UI still renders.
//
function PodTerminal() {
  const [hydrated, setHydrated] = useState(0);
  const [srcLabel, setSrcLabel] = useState<"live" | "mock">("mock");
  useEffect(() => {
    fetch("./pod_snapshot.json", { cache: "no-store" })
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (!data) return;
        Object.keys(POD_SNAPSHOT).forEach(k => delete (POD_SNAPSHOT as any)[k]);
        Object.assign(POD_SNAPSHOT, data);
        setSrcLabel("live");
        setHydrated(h => h + 1);
      })
      .catch(() => { /* fall back to mock */ });
  }, []);
  return (
    <div key={hydrated} className="min-h-screen w-full text-[var(--text)] bg-[var(--bg)] font-sans">
      <TopBar/>
      <Ticker/>
      <ExecBento/>
      <OverridesStrip/>
      <div className="mt-2"><BacktestCanvas/></div>
      <div className="grid grid-cols-2 gap-2 px-2 mt-2">
        <GateRadar/>
        <EventStudyTable/>
      </div>
      <div className="mt-2">
        <AgentLog/>
      </div>
      <div className="mt-2">
        <Footer/>
      </div>
      {/* source badge — cyan when we're on warehouse-backed data,
          amber when the fetch failed and we're on mock */}
      <div className="fixed bottom-2 right-2 px-2 py-0.5 rounded-sm text-[9px] font-mono tracking-widest border pointer-events-none"
           style={{
             borderColor: srcLabel === "live" ? C.cyan : C.amber,
             color:       srcLabel === "live" ? C.cyan : C.amber,
             background:  "rgba(7,10,16,0.7)",
           }}>
        SRC · {srcLabel.toUpperCase()}
      </div>
    </div>
  );
}

// @ts-ignore
window.PodTerminal = PodTerminal;
