// Apollo Hermes — WORKING DEMO frontend
// Vanilla JS. No framework. Talks to Flask backend in app.py.

// ============================================================
// Demo BearWatch payloads — extend by adding entries here
// ============================================================
const DEMO_PAYLOADS = {
  CVNA: {
    event_id: null, // backend assigns
    timestamp: null,
    ticker: "CVNA",
    firm_name: "Carvana Co.",
    sector: "subprime_auto",
    bear_state: "FIRED_UP",
    signal: { bsi_z: 3.42, phase: 2, h2_eligible: true, expert_confirm: 2, days_above_2: 7 },
    pillars: {
      cfpb_distress: 2.91, cfpb_narrative: 1.84, reddit: 3.10,
      bluesky_consumer: 2.05, bluesky_expert: 3.88, search_expert: 2.12,
      macro: 1.40, move: 0.95
    },
    war_room: {
      renaissance: "SHORT - pattern match 2022 setup",
      two_sigma:   "SHORT - z > 3, signal-rich",
      bridgewater: "SHORT - macro alignment",
      millennium:  "PASS  - vol regime unfavorable",
      citadel:     "SHORT - execution clean",
      verdict:     "4-of-5 SHORT"
    },
    recommended_action: { side: "SHORT", horizon_days: 540, conviction: "high" }
  },
  UPST: {
    ticker: "UPST",
    firm_name: "Upstart Holdings",
    sector: "subprime_personal",
    bear_state: "FIRED_UP",
    signal: { bsi_z: 2.81, phase: 2, h2_eligible: true, expert_confirm: 1, days_above_2: 4 },
    pillars: { cfpb_distress: 2.4, cfpb_narrative: 2.0, reddit: 1.9,
      bluesky_consumer: 1.8, bluesky_expert: 2.7, search_expert: 1.6, macro: 1.2, move: 0.9 },
    war_room: { verdict: "3-of-5 SHORT" },
    recommended_action: { side: "SHORT", horizon_days: 360, conviction: "medium" }
  },
  AFRM: {
    ticker: "AFRM",
    firm_name: "Affirm Holdings",
    sector: "bnpl",
    bear_state: "TRANSITION",
    signal: { bsi_z: 2.45, phase: 1, h2_eligible: false, expert_confirm: 1, days_above_2: 2 },
    pillars: { cfpb_distress: 1.9, cfpb_narrative: 1.6, reddit: 1.5,
      bluesky_consumer: 1.4, bluesky_expert: 2.1, search_expert: 1.3, macro: 0.9, move: 0.7 },
    war_room: { verdict: "2-of-5 SHORT" },
    recommended_action: { side: "SHORT", horizon_days: 180, conviction: "low" }
  }
};

// ============================================================
// Toast
// ============================================================
function toast(message, kind = "info") {
  const stack = document.getElementById("toast-stack");
  const el = document.createElement("div");
  const palette = {
    info:   "border-[#38bdf8] bg-[#0a1622] text-sky-200",
    ok:     "border-emerald-500 bg-emerald-950/60 text-emerald-200",
    warn:   "border-amber-500 bg-amber-950/60 text-amber-200",
    err:    "border-rose-500 bg-rose-950/60 text-rose-200",
  };
  el.className = `card p-3 px-4 text-sm border ${palette[kind] || palette.info} shadow-lg max-w-sm`;
  el.textContent = message;
  stack.appendChild(el);
  setTimeout(() => { el.style.transition = "opacity 0.4s"; el.style.opacity = "0"; }, 3500);
  setTimeout(() => el.remove(), 4000);
}

// ============================================================
// Helpers
// ============================================================
const fmtUsd = (n) => "$" + Math.round(Number(n) || 0).toLocaleString();
const fmtTime = (iso) => {
  if (!iso) return "-";
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit", month: "short", day: "2-digit" });
  } catch { return iso; }
};

const verdictBadge = (v) => {
  const map = {
    APPROVED:    "text-emerald-300 border-emerald-700",
    SCALED_DOWN: "text-amber-300 border-amber-700",
    BLOCKED:     "text-rose-300 border-rose-700",
  };
  return `<span class="px-2 py-0.5 rounded-full border ${map[v] || "text-slate-300 border-slate-700"} text-[10px] font-bold tracking-wider">${v || "-"}</span>`;
};

const checkRow = (c) => {
  const ok = c.status === "PASS";
  return `<div class="flex items-start gap-2 py-1">
    <span class="${ok ? "text-emerald-400" : "text-rose-400"} font-bold">${ok ? "✓" : "✗"}</span>
    <div>
      <div class="text-[#cbd5e1]">${c.name}</div>
      <div class="text-[#64748b] text-[11px]">${c.detail}</div>
    </div>
  </div>`;
};

// ============================================================
// Renderers
// ============================================================
async function refreshPortfolio() {
  const r = await fetch("/api/portfolio");
  const p = await r.json();
  document.getElementById("port-cash").textContent = fmtUsd(p.cash);
  document.getElementById("port-positions").textContent = p.open_count;
  const modeEl = document.getElementById("port-mode");
  modeEl.textContent = p.drawdown_mode;
  modeEl.className = "text-2xl font-bold mono " +
    (p.drawdown_mode === "NORMAL" ? "text-emerald-300"
     : p.drawdown_mode === "CAUTIOUS" ? "text-amber-300"
     : p.drawdown_mode === "DEFENSIVE" ? "text-orange-300" : "text-rose-300");
  document.getElementById("port-beta").textContent = (p.portfolio_beta >= 0 ? "+" : "") + p.portfolio_beta.toFixed(2);
  document.getElementById("port-value").textContent = fmtUsd(p.portfolio_value);
}

async function refreshBears() {
  const r = await fetch("/api/bears");
  const rows = await r.json();
  const tbody = document.getElementById("bears-tbody");
  document.getElementById("bears-count").textContent = `${rows.length} alerts`;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="px-5 py-6 text-center text-[#64748b]">No alerts yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(r => `
    <tr class="border-t border-[#1f2733]">
      <td class="px-5 py-2">${fmtTime(r.ts)}</td>
      <td class="px-5 py-2 text-amber-300 font-bold">${r.ticker}</td>
      <td class="px-5 py-2 text-[#94a3b8]">${r.sector || "-"}</td>
      <td class="px-5 py-2">${(r.bsi_z ?? 0).toFixed(2)}</td>
      <td class="px-5 py-2">${r.phase ?? "-"}</td>
      <td class="px-5 py-2 text-[#94a3b8]">${r.bear_state || "-"}</td>
      <td class="px-5 py-2">${verdictBadge(r.verdict)}</td>
      <td class="px-5 py-2 text-right">${fmtUsd(r.recommended_usd || 0)}</td>
    </tr>`).join("");
}

async function refreshJournal() {
  const r = await fetch("/api/journal");
  const rows = await r.json();
  const tbody = document.getElementById("journal-tbody");
  document.getElementById("journal-count").textContent = `${rows.length} trades`;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="px-5 py-6 text-center text-[#64748b]">No trades logged yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.slice(0, 10).map(r => `
    <tr class="border-t border-[#1f2733]">
      <td class="px-5 py-2">${fmtTime(r.ts)}</td>
      <td class="px-5 py-2 text-white font-bold">${r.ticker}</td>
      <td class="px-5 py-2 ${r.side === "SHORT" ? "text-rose-300" : "text-emerald-300"} font-bold">${r.side}</td>
      <td class="px-5 py-2 text-right">${r.shares}</td>
      <td class="px-5 py-2 text-right">$${(r.entry_price || 0).toFixed(2)}</td>
      <td class="px-5 py-2 text-right text-[#94a3b8]">$${(r.stop_price ?? 0).toFixed(2)} / $${(r.target_price ?? 0).toFixed(2)}</td>
      <td class="px-5 py-2 text-right">${fmtUsd(r.notional_usd)}</td>
      <td class="px-5 py-2 text-[#64748b]">${r.event_id || "-"}</td>
    </tr>`).join("");
}

// ============================================================
// Verdict panel renderer
// ============================================================
let LAST_RESULT = null;

function renderVerdict(result) {
  LAST_RESULT = result;
  const p = result.received_payload;
  const v = result.verdict;
  const q = result.quote;

  // Step 1
  document.getElementById("step1").innerHTML = `
    <div class="flex items-center gap-2 mb-3">
      <span class="text-3xl">&#128059;&#128293;</span>
      <div>
        <div class="font-bold text-white text-sm">${p.bear_state || "FIRED_UP"}</div>
        <div class="text-xs text-[#d4a574]">${p.ticker} &middot; ${(p.firm_name || "").slice(0,28)}</div>
      </div>
    </div>
    <div class="space-y-1 text-xs mono text-[#cbd5e1]">
      <div>BSI z: <span class="text-amber-300">${(p.signal?.bsi_z ?? 0).toFixed(2)}</span></div>
      <div>Phase: <span class="text-amber-300">${p.signal?.phase ?? "-"}</span></div>
      <div>H2-eligible: <span class="text-amber-300">${p.signal?.h2_eligible ? "true" : "false"}</span></div>
      <div>War Room: <span class="text-amber-300">${p.war_room?.verdict || "-"}</span></div>
      <div class="pt-2 text-[#64748b]">event_id: ${result.event_id}</div>
    </div>`;

  // Step 2
  document.getElementById("step2").innerHTML = `
    <div class="flex items-center gap-2 mb-3">
      <span class="text-3xl">&#9889;</span>
      <div>
        <div class="font-bold text-white text-sm">VERDICT: ${v.verdict}</div>
        <div class="text-xs text-[#38bdf8]">Live quote: $${q.price.toFixed(2)} (${q.source})</div>
      </div>
    </div>
    <div class="space-y-0 text-xs mono">
      ${v.checks.map(checkRow).join("")}
    </div>
    <div class="pt-3 text-sky-300 text-sm mono">
      Recommended: <span class="font-bold">${fmtUsd(v.recommended_usd)}</span>
      ${v.shares > 0 ? ` (${v.shares} sh @ $${v.entry_price.toFixed(2)})` : ""}
    </div>`;

  // Step 3
  if (v.verdict === "BLOCKED" || v.recommended_usd <= 0) {
    document.getElementById("step3").innerHTML = `
      <div class="flex items-center gap-2 mb-3">
        <span class="text-3xl">&#128683;</span>
        <div><div class="font-bold text-rose-300 text-sm">BLOCKED</div>
        <div class="text-xs text-[#94a3b8]">No execution permitted</div></div>
      </div>
      <div class="text-xs text-[#94a3b8]">Risk Engine declined. Adjust portfolio state and re-fire.</div>`;
  } else {
    const side = p.recommended_action?.side || "SHORT";
    document.getElementById("step3").innerHTML = `
      <div class="flex items-center gap-2 mb-3">
        <span class="text-3xl">&#128104;</span>
        <div>
          <div class="font-bold text-white text-sm">READY TO EXECUTE</div>
          <div class="text-xs text-[#94a3b8]">Sim broker</div>
        </div>
      </div>
      <div class="space-y-1 text-xs mono text-[#cbd5e1]">
        <div>${side} ${v.shares} ${p.ticker} @ $${v.entry_price.toFixed(2)}</div>
        <div>Stop: $${v.stop_price.toFixed(2)}</div>
        <div>Target: $${v.target_price.toFixed(2)}</div>
        <div>R:R: ${v.rr}:1</div>
      </div>
      <button id="btn-execute"
        class="mt-4 w-full px-4 py-2 bg-[#38bdf8] hover:bg-[#0ea5e9] text-[#07090d] font-semibold rounded-lg transition-all">
        Execute trade &rarr;
      </button>`;

    document.getElementById("btn-execute").addEventListener("click", () => executeTrade());
  }
}

// ============================================================
// Actions
// ============================================================
async function fireDemo(symbol) {
  const payload = DEMO_PAYLOADS[symbol];
  if (!payload) return toast("Unknown demo symbol", "err");
  toast(`Firing ${symbol} bear -> Apollo...`, "info");
  try {
    const r = await fetch("/api/bearwatch/ingest", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const j = await r.json();
    renderVerdict(j);
    toast(`${symbol} verdict: ${j.verdict.verdict}`, j.verdict.verdict === "APPROVED" ? "ok" : (j.verdict.verdict === "BLOCKED" ? "err" : "warn"));
    await Promise.all([refreshPortfolio(), refreshBears()]);
  } catch (e) {
    toast("Ingest failed: " + e.message, "err");
  }
}

async function executeTrade() {
  if (!LAST_RESULT) return;
  const v = LAST_RESULT.verdict;
  const p = LAST_RESULT.received_payload;
  const body = {
    event_id: LAST_RESULT.event_id,
    ticker: p.ticker,
    side: p.recommended_action?.side || "SHORT",
    shares: v.shares,
    entry_price: v.entry_price,
    stop_price: v.stop_price,
    target_price: v.target_price,
    notional_usd: v.recommended_usd,
    verdict: v.verdict,
  };
  try {
    const r = await fetch("/api/journal/log", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error("HTTP " + r.status);
    toast(`Executed ${body.side} ${body.shares} ${body.ticker}`, "ok");
    await Promise.all([refreshPortfolio(), refreshJournal(), refreshBears()]);
  } catch (e) {
    toast("Execute failed: " + e.message, "err");
  }
}

async function resetDemo() {
  if (!confirm("Reset demo state? This wipes the journal and resets cash to $100,000.")) return;
  try {
    await fetch("/api/portfolio/reset", { method: "POST" });
    LAST_RESULT = null;
    document.getElementById("step1").innerHTML = `<div class="text-sm text-[#64748b]">No event yet. Click a demo button above.</div>`;
    document.getElementById("step2").innerHTML = `<div class="text-sm text-[#64748b]">Verdict will appear here.</div>`;
    document.getElementById("step3").innerHTML = `<div class="text-sm text-[#64748b]">Execute button will appear here.</div>`;
    await Promise.all([refreshPortfolio(), refreshBears(), refreshJournal()]);
    toast("Demo state reset", "ok");
  } catch (e) {
    toast("Reset failed: " + e.message, "err");
  }
}

// ============================================================
// Bear Squad mascot picker
// ============================================================
let ACTIVE_MASCOT = "SCOUT";  // default

function selectMascot(name, btn) {
  ACTIVE_MASCOT = name;
  document.querySelectorAll(".mascot-card").forEach(c => c.classList.remove("mascot-active"));
  btn.classList.add("mascot-active");
  const label = document.getElementById("active-mascot");
  if (label) {
    label.textContent = name;
    label.style.color = btn.dataset.color || "#D4A574";
  }
  const taglines = {
    BLITZ:    'BLITZ active. "Strike first. Strike fast." — z >= 1.5, 12% size, accepts more false positives.',
    SCOUT:    'SCOUT active. "Watch. Wait. Strike clean." — z >= 2.0, 5% size, the institutional default.',
    GUARDIAN: 'GUARDIAN active. "Roar only when the threat is real." — z >= 2.5, 2% size, near-zero false positives.',
    ROBO:     'ROBO-BEAR (alpha) active. Reads each event and picks the configuration. Backtested +20.0% with 4-of-5 win rate.',
  };
  toast(taglines[name] || `${name} active.`, "info");
}

// Modify fireDemo to apply mascot's threshold filter and size override
const _origFireDemo = typeof fireDemo === "function" ? fireDemo : null;
async function fireDemoWithMascot(demoKey) {
  const payload = JSON.parse(JSON.stringify(DEMO_PAYLOADS[demoKey]));
  const activeBtn = document.querySelector(".mascot-card.mascot-active");
  const mascotZ   = parseFloat(activeBtn?.dataset.z   || "2.0");
  const mascotSize= parseFloat(activeBtn?.dataset.size|| "0.05");
  // If signal z below mascot threshold, BLOCK before sending
  if (payload.signal.bsi_z < mascotZ) {
    toast(`${ACTIVE_MASCOT} BLOCKS this event: BSI z=${payload.signal.bsi_z} < threshold ${mascotZ}. (Lower-conviction event would have fired under a more aggressive mascot.)`, "warn");
    return;
  }
  // Stamp mascot + size override into payload
  payload.mascot = ACTIVE_MASCOT;
  payload.size_cap = mascotSize;
  // Call original ingest path
  if (_origFireDemo) return _origFireDemo(demoKey, payload);
  return fireDemo(demoKey, payload);
}

// ============================================================
// Boot
// ============================================================
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".demo-fire").forEach(btn => {
    btn.addEventListener("click", () => fireDemoWithMascot(btn.dataset.demo));
  });
  document.querySelectorAll(".mascot-card").forEach(btn => {
    btn.addEventListener("click", () => selectMascot(btn.dataset.mascot, btn));
  });
  document.getElementById("btn-reset").addEventListener("click", resetDemo);
  refreshPortfolio();
  refreshBears();
  refreshJournal();
});
