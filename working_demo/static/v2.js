// ============================================================
// BearWatch x Apollo Hermes — v2 frontend (4-pod design)
// Linear-inspired, vanilla JS, talks to Flask backend.
// ============================================================

const DEMO_PAYLOADS = {
  CVNA: {
    ticker: "CVNA", firm_name: "Carvana Co.", sector: "subprime_auto",
    bear_state: "FIRED_UP",
    signal: { bsi_z: 3.42, phase: 2, h2_eligible: true, expert_confirm: 2, days_above_2: 7 },
    pillars: { cfpb_distress: 2.91, cfpb_narrative: 1.84, reddit: 3.10,
               bluesky_consumer: 2.05, bluesky_expert: 3.88, search_expert: 2.12,
               macro: 1.40, move: 0.95 },
    war_room: {
      renaissance: { vote: "SHORT", rationale: "Pattern-match: 2018 CARS setup repeating with cleaner entry." },
      bridgewater: { vote: "SHORT", rationale: "Macro: rate-cycle turn confirms credit-deterioration thesis." },
      twosigma:    { vote: "SHORT", rationale: "z = 3.42 with 8-pillar concordance — clean signal." },
      millennium:  { vote: "PASS",  rationale: "Vol regime favors waiting for clearer momentum break." },
      citadel:     { vote: "SHORT", rationale: "Execution path clean — borrow available, 540d horizon OK." },
      verdict: "4-of-5 SHORT"
    },
    recommended_action: { side: "SHORT", horizon_days: 540, conviction: "high" }
  },
  UPST: {
    ticker: "UPST", firm_name: "Upstart Holdings", sector: "marketplace",
    bear_state: "FIRED_UP",
    signal: { bsi_z: 2.81, phase: 2, h2_eligible: true, expert_confirm: 1, days_above_2: 4 },
    pillars: { cfpb_distress: 2.40, cfpb_narrative: 2.00, reddit: 1.90,
               bluesky_consumer: 1.80, bluesky_expert: 2.70, search_expert: 1.60,
               macro: 1.20, move: 0.90 },
    war_room: {
      renaissance: { vote: "SHORT", rationale: "Volatility signature matches 2022 unwind — high probability." },
      bridgewater: { vote: "PASS",  rationale: "Macro alignment unclear in current regime." },
      twosigma:    { vote: "SHORT", rationale: "z = 2.81 across 6 pillars — broad confirmation." },
      millennium:  { vote: "PASS",  rationale: "IV elevated, premium decay against position." },
      citadel:     { vote: "SHORT", rationale: "Borrow available; 360d horizon acceptable." },
      verdict: "3-of-5 SHORT"
    },
    recommended_action: { side: "SHORT", horizon_days: 360, conviction: "medium" }
  },
  AFRM: {
    ticker: "AFRM", firm_name: "Affirm Holdings", sector: "BNPL",
    bear_state: "TRANSITION",
    signal: { bsi_z: 2.05, phase: 1, h2_eligible: false, expert_confirm: 1, days_above_2: 2 },
    pillars: { cfpb_distress: 1.90, cfpb_narrative: 1.60, reddit: 1.50,
               bluesky_consumer: 1.40, bluesky_expert: 2.10, search_expert: 1.30,
               macro: 0.90, move: 0.70 },
    war_room: {
      renaissance: { vote: "PASS", rationale: "Setup unclear — z borderline, no pattern match." },
      bridgewater: { vote: "PASS", rationale: "Phase 1 GROWTH — macro position blocks entry." },
      twosigma:    { vote: "PASS", rationale: "z = 2.05 — too low, signal-noise unclear." },
      millennium:  { vote: "PASS", rationale: "Regime gate: do not engage." },
      citadel:     { vote: "SHORT", rationale: "Borrow available; could probe small." },
      verdict: "1-of-5 SHORT"
    },
    recommended_action: { side: "SHORT", horizon_days: 180, conviction: "low" }
  }
};

// ============================================================
// State
// ============================================================
let ACTIVE_MASCOT = "SCOUT";
let ACTIVE_MASCOT_DATA = { z: 2.0, size: 0.05, stop: 0.07, color: "#d4a574" };
let PENDING_EVENT = null;
let APOLLO_VERDICT = null;
let JOURNAL_TRADES = [];
let PORTFOLIO = { cash: 100000, open_count: 0, drawdown_mode: "NORMAL", portfolio_beta: 0, portfolio_value: 100000 };

// ============================================================
// Helpers
// ============================================================
const $ = (id) => document.getElementById(id);
const fmt$ = (n) => "$" + Math.round(Number(n) || 0).toLocaleString();
const fmtPct = (n) => (n >= 0 ? "+" : "") + n.toFixed(2) + "%";

function toast(msg, kind = "info") {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.innerHTML = `<div class="toast-dot"></div><div>${msg}</div>`;
  $("toast-stack").appendChild(el);
  setTimeout(() => { el.style.transition = "opacity 300ms"; el.style.opacity = "0"; }, 3500);
  setTimeout(() => el.remove(), 4000);
}

// ============================================================
// Mascot dropdown
// ============================================================
$("mascot-toggle").addEventListener("click", (e) => {
  e.stopPropagation();
  $("mascot-menu").classList.toggle("open");
});
document.addEventListener("click", () => $("mascot-menu").classList.remove("open"));

document.querySelectorAll(".menu-item").forEach(item => {
  item.addEventListener("click", (e) => {
    e.stopPropagation();
    document.querySelectorAll(".menu-item").forEach(i => i.classList.remove("active"));
    item.classList.add("active");

    ACTIVE_MASCOT = item.dataset.mascot;
    ACTIVE_MASCOT_DATA = {
      z:     parseFloat(item.dataset.z),
      size:  parseFloat(item.dataset.size),
      stop:  parseFloat(item.dataset.stop),
      color: item.dataset.color
    };
    $("mascot-name").textContent = ACTIVE_MASCOT;
    const bearImg = item.dataset.bear;
    if (bearImg && $("mascot-img")) { $("mascot-img").src = "/static/bears/" + bearImg; }
    $("execute-mascot-tag").textContent =
      `${ACTIVE_MASCOT} · ${(ACTIVE_MASCOT_DATA.size * 100).toFixed(0)}% · −${(ACTIVE_MASCOT_DATA.stop * 100).toFixed(0)}%`;

    $("mascot-menu").classList.remove("open");
    toast(`${ACTIVE_MASCOT} active — ${ACTIVE_MASCOT_DATA.z >= 1.5 ? `z ≥ ${ACTIVE_MASCOT_DATA.z}` : "adaptive"}`, "info");
    // Persist to user profile (server-side) so it sticks across sessions
    fetch("/api/profile/mascot", {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({mascot: ACTIVE_MASCOT})
    }).catch(()=>{});
  });
});

// On page load, fetch user's active mascot and select it in the dropdown
async function loadUserMascot() {
  try {
    const r = await fetch("/api/profile");
    if (!r.ok) return;
    const d = await r.json();
    const active = (d.profile?.active_mascot || d.profile?.recommended_mascot || "SCOUT").toUpperCase();
    const item = document.querySelector(`.menu-item[data-mascot="${active}"]`);
    if (item) item.click();
    // The above triggers the click handler which sets state; closeMenu after auto-trigger
    document.getElementById("mascot-menu")?.classList.remove("open");
  } catch {}
}
loadUserMascot();

// ============================================================
// Demo trigger -> fire event through Pod 1
// ============================================================
document.querySelectorAll(".demo-fire").forEach(btn => {
  btn.addEventListener("click", () => fireEvent(btn.dataset.demo));
});

async function fireEvent(key) {
  const payload = JSON.parse(JSON.stringify(DEMO_PAYLOADS[key]));

  // Mascot threshold gate
  if (payload.signal.bsi_z < ACTIVE_MASCOT_DATA.z) {
    toast(`${ACTIVE_MASCOT} BLOCKS ${payload.ticker}: BSI z=${payload.signal.bsi_z} < ${ACTIVE_MASCOT_DATA.z} threshold`, "warn");
    return;
  }

  PENDING_EVENT = payload;

  // POD 1 — show the gates
  showGates(payload);

  // POD 2 — populate war room
  showDebate(payload);

  // POD 3 — run risk verdict
  await runRiskCheck(payload);

  toast(`Fired ${payload.ticker} — flowing through pipeline`, "ok");
}

// ============================================================
// POD 1 — Signal: gate visualization
// ============================================================
function showGates(p) {
  $("gates-display").style.display = "grid";
  $("gate-bsi").textContent = p.signal.bsi_z.toFixed(2);
  $("gate-phase").textContent = p.signal.phase;
  $("gate-h2").textContent = p.signal.h2_eligible ? "✓" : "✗";
  $("gate-confirm").textContent = p.signal.expert_confirm;

  // Color the gates pass/fail
  const gates = document.querySelectorAll(".gate");
  const passes = [
    p.signal.bsi_z >= ACTIVE_MASCOT_DATA.z,
    p.signal.phase >= 2,
    p.signal.h2_eligible,
    p.signal.expert_confirm >= 2
  ];
  gates.forEach((g, i) => {
    g.classList.remove("pass", "fail");
    g.classList.add(passes[i] ? "pass" : "fail");
  });
}

// ============================================================
// POD 2 — Debate: war room render
// ============================================================
function showDebate(p) {
  $("debate-empty").style.display = "none";
  $("debate-list").style.display = "block";
  $("debate-verdict").style.display = "flex";
  $("debate-event-tag").textContent = `${p.ticker} · z = ${p.signal.bsi_z}`;

  const wr = p.war_room;
  ["renaissance", "bridgewater", "twosigma", "millennium", "citadel"].forEach(name => {
    if (wr[name]) {
      $(`r-${name}`).textContent = wr[name].rationale;
      const v = $(`v-${name}`);
      v.textContent = wr[name].vote;
      v.dataset.vote = wr[name].vote;
    }
  });
  $("verdict-ratio").textContent = wr.verdict;
}

// ============================================================
// POD 3 — Execute: 7 risk checks
// ============================================================
async function runRiskCheck(p) {
  $("execute-empty").style.display = "none";
  $("execute-checks").style.display = "block";
  $("verdict-execute").style.display = "block";

  const r = await fetch("/api/bearwatch/ingest", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...p, mascot: ACTIVE_MASCOT, size_cap: ACTIVE_MASCOT_DATA.size })
  });
  const raw = await r.json();
  // The API returns {event_id, ticker, quote, verdict: {verdict, entry_price, stop_price, target_price, recommended_usd, checks}}
  // Flatten so the rest of the JS (and the modal) can read fields off APOLLO_VERDICT.
  const v = raw.verdict || {};
  const entryPx = Number(v.entry_price ?? raw.quote?.price ?? 0);
  const recUsd  = Number(v.recommended_usd || 0);
  const shares  = entryPx > 0 ? Math.floor(recUsd / entryPx) : 0;
  APOLLO_VERDICT = {
    event_id: raw.event_id,
    verdict: v.verdict || "BLOCKED",
    entry_price: entryPx,
    stop_price: v.stop_price,
    target_price: v.target_price,
    recommended_usd: recUsd,
    recommended_notional: shares * entryPx,
    recommended_shares: shares,
    rr: v.rr,
    risk_checks: v.checks || [],
  };

  // Render checks
  const mount = $("check-list-mount");
  mount.innerHTML = "";
  APOLLO_VERDICT.risk_checks.forEach(c => {
    const ok = c.status === "PASS";
    const row = document.createElement("div");
    row.className = `check-row ${ok ? "pass" : "fail"}`;
    row.innerHTML = `
      <span class="check-icon">${ok ? "✓" : "✗"}</span>
      <span class="check-name">${c.name}</span>
      <span class="check-detail">${c.detail || ""}</span>
    `;
    mount.appendChild(row);
  });

  // Verdict
  const verdict = APOLLO_VERDICT.verdict;
  const result = $("exec-result");
  result.textContent = verdict;
  result.dataset.status = verdict;

  $("exec-entry").textContent  = APOLLO_VERDICT.entry_price  ? `$${APOLLO_VERDICT.entry_price.toFixed(2)}`  : "—";
  $("exec-size").textContent   = `${APOLLO_VERDICT.recommended_shares} sh · ${fmt$(APOLLO_VERDICT.recommended_notional)}`;
  $("exec-stop").textContent   = APOLLO_VERDICT.stop_price   ? `$${Number(APOLLO_VERDICT.stop_price).toFixed(2)}`   : "—";
  $("exec-target").textContent = APOLLO_VERDICT.target_price ? `$${Number(APOLLO_VERDICT.target_price).toFixed(2)}` : "—";
  const horizon = p.recommended_action?.horizon_days || 540;
  $("exec-horizon").textContent = `${horizon} d`;

  $("btn-execute").disabled = (verdict === "BLOCKED");
}

// ============================================================
// Pod 3 — Idle "Suggested next trade" preview
// ============================================================
// Pulls top watchlist asset and computes the trade suggestion CLIENT-SIDE
// using the same math Apollo uses server-side. Updates with watchlist auto-refresh.
async function refreshSuggestion() {
  try {
    const r = await fetch("/api/selections");
    if (!r.ok) return;
    const data = await r.json();
    const sel = data.selected || [];
    const sug = data.suggested || [];
    // In demo mode, use the date-coded historical price; else live yfinance price.
    const demoMap = window.DEMO_DATE_PRICES || {};
    const priceFor = (f) => (demoMap[f.ticker] !== undefined ? demoMap[f.ticker] : f.price);
    // Prefer a SELECTED tradeable asset with the highest BSI z; fall back to suggestion
    const candidates = [...sel.filter(f => f.tradeable && priceFor(f)), ...sug.filter(f => f.tradeable && priceFor(f))];
    if (candidates.length === 0) return;
    candidates.sort((a, b) => b.bsi_z - a.bsi_z);
    const f = candidates[0];

    // Apollo math (mirrors run_risk_checks in app.py):
    //   stop   = entry × 1.079               (smart fixed +7.9% for SHORT)
    //   target = entry × (1 − 0.05 × min(z, 4.5))
    //   shares = floor( (size_cap × 100,000) / entry )
    const z = f.bsi_z;
    const entry = priceFor(f);
    const isDemoEntry = demoMap[f.ticker] !== undefined;
    const stop = +(entry * 1.079).toFixed(2);
    const target = +(entry * (1 - 0.05 * Math.min(z, 4.5))).toFixed(2);
    const stopPct = ((stop - entry) / entry * 100).toFixed(1);
    const targetPct = ((entry - target) / entry * 100).toFixed(1);
    const sizeCap = ACTIVE_MASCOT_DATA.size;
    const notional = sizeCap * 100000;
    const shares = Math.floor(notional / entry);
    const realNotional = shares * entry;
    const rr = (Math.abs(entry - target) / Math.abs(stop - entry)).toFixed(1);

    $("sug-ticker").textContent = f.ticker;
    $("sug-firm").textContent = `${f.name || ''} · ${f.sector_label || f.sector}`;
    $("sug-z").textContent = `z ${z.toFixed(2)}`;
    // Color the z chip by mascot threshold
    const passZ = z >= ACTIVE_MASCOT_DATA.z;
    $("sug-z").style.color = passZ ? "var(--success)" : "var(--text-tertiary)";
    $("sug-z").style.borderColor = passZ ? "rgba(34,197,94,0.4)" : "var(--border)";
    $("sug-z").style.background = passZ ? "rgba(34,197,94,0.06)" : "var(--bg-card)";
    $("sug-z").title = passZ
      ? `z = ${z.toFixed(2)} ≥ ${ACTIVE_MASCOT_DATA.z} (${ACTIVE_MASCOT}) — gate clears`
      : `z = ${z.toFixed(2)} < ${ACTIVE_MASCOT_DATA.z} (${ACTIVE_MASCOT}) — would BLOCK`;

    $("sug-entry").textContent = `$${entry.toFixed(2)}`;
    // In demo mode, color entry yellow + show event-date label
    if (isDemoEntry) {
      $("sug-entry").style.color = '#facc15';
      $("sug-entry").title = window.DEMO_DATE_LABEL || '';
      const lblEl = $("sug-entry").parentElement.querySelector('.lbl');
      if (lblEl) lblEl.innerHTML = `Entry <span style="color:#facc15;font-weight:700;">(${window.DEMO_DATE || ''})</span>`;
    } else {
      $("sug-entry").style.color = '';
    }
    $("sug-stop").textContent = `$${stop.toFixed(2)}`;
    $("sug-stop-sub").textContent = `+${stopPct}% (smart)`;
    $("sug-target").textContent = `$${target.toFixed(2)}`;
    $("sug-target-sub").textContent = `−${targetPct}% (z-scaled)`;
    $("sug-horizon").textContent = `540 d`;
    $("sug-size").textContent = `${shares} sh`;
    $("sug-size-sub").textContent = `≈ ${fmt$(realNotional)} (${(sizeCap * 100).toFixed(0)}%)`;
    $("sug-rr").textContent = `${rr} : 1`;
  } catch (e) {
    /* silent — pod stays in default state */
  }
}

// ============================================================
// POD 4 — Execute trade button -> Journal
// ============================================================
$("btn-execute").addEventListener("click", () => openTradeConfirm());

function openTradeConfirm() {
  if (!APOLLO_VERDICT || !PENDING_EVENT) {
    toast("No pending trade — fire an event first", "warn");
    return;
  }
  // Populate the confirmation modal
  const side = (PENDING_EVENT.recommended_action?.side || "SHORT").toUpperCase();
  const verdict = document.getElementById("exec-result")?.textContent || "APPROVED";
  $("cf-ticker").textContent   = PENDING_EVENT.ticker;
  $("cf-firmname").textContent = PENDING_EVENT.firm_name || "";
  $("cf-side").textContent     = side;
  $("cf-side").className       = `confirm-side-pill ${side}`;
  $("cf-mascot").textContent   = `${ACTIVE_MASCOT} (z ≥ ${ACTIVE_MASCOT_DATA.z})`;
  $("cf-verdict").textContent  = verdict;
  $("cf-shares").textContent   = `${APOLLO_VERDICT.recommended_shares || 0}`;
  $("cf-entry").textContent    = APOLLO_VERDICT.entry_price ? "$" + APOLLO_VERDICT.entry_price.toFixed(2) : "—";
  $("cf-stop").textContent     = APOLLO_VERDICT.stop_price ? "$" + APOLLO_VERDICT.stop_price.toFixed(2) : "—";
  $("cf-target").textContent   = APOLLO_VERDICT.target_price ? "$" + APOLLO_VERDICT.target_price.toFixed(2) : "—";
  $("cf-horizon").textContent  = `${PENDING_EVENT.recommended_action?.horizon_days || 540} days`;
  $("cf-notional").textContent = APOLLO_VERDICT.recommended_notional
    ? "$" + Math.round(APOLLO_VERDICT.recommended_notional).toLocaleString()
    : "—";
  // Disclosure varies by verdict
  const cfBtn = $("cf-commit");
  if (verdict === "OVERRIDE") {
    $("cf-disclosure").innerHTML = "⚠️ <strong>Override mode</strong> — Apollo blocked this trade. Your judgment will be logged for the calibration team.";
    cfBtn.textContent = "Force trade through";
    cfBtn.classList.add("override");
  } else {
    $("cf-disclosure").textContent = "Backtested simulation. No live capital deployed.";
    cfBtn.textContent = "Confirm trade";
    cfBtn.classList.remove("override");
  }
  $("confirm-modal").classList.add("open");
}

function closeTradeConfirm() { $("confirm-modal").classList.remove("open"); }

$("cf-cancel").addEventListener("click", closeTradeConfirm);
$("confirm-modal").addEventListener("click", e => { if (e.target.id === "confirm-modal") closeTradeConfirm(); });
document.addEventListener("keydown", e => { if (e.key === "Escape") closeTradeConfirm(); });

$("cf-commit").addEventListener("click", async () => {
  if (!APOLLO_VERDICT || !PENDING_EVENT) return closeTradeConfirm();
  const r = await fetch("/api/journal/log", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      event_id: APOLLO_VERDICT.event_id,
      ticker: PENDING_EVENT.ticker,
      side: PENDING_EVENT.recommended_action.side,
      shares: APOLLO_VERDICT.recommended_shares,
      entry_price: APOLLO_VERDICT.entry_price,
      stop_price: APOLLO_VERDICT.stop_price,
      target_price: APOLLO_VERDICT.target_price,
      notional: APOLLO_VERDICT.recommended_notional
    })
  });
  closeTradeConfirm();
  if (r.ok) {
    toast(`Trade logged: ${PENDING_EVENT.recommended_action.side} ${APOLLO_VERDICT.recommended_shares} ${PENDING_EVENT.ticker}`, "ok");
    refreshAll();
  }
});

// ============================================================
// Refresh — portfolio, journal, stats
// ============================================================
async function refreshPortfolio() {
  const r = await fetch("/api/portfolio");
  if (!r.ok) return;
  const p = await r.json();
  PORTFOLIO = p;
  $("port-cash").textContent = fmt$(p.cash);
  $("port-positions").textContent = p.open_count;
  const m = $("port-mode");
  m.textContent = p.drawdown_mode;
  m.style.color = p.drawdown_mode === "NORMAL" ? "var(--success)"
                : p.drawdown_mode === "CAUTIOUS" ? "var(--warning)"
                : "var(--danger)";
  $("port-beta").textContent = (p.portfolio_beta >= 0 ? "+" : "") + p.portfolio_beta.toFixed(2);
  $("port-value").textContent = fmt$(p.portfolio_value);
}

async function refreshJournal() {
  const r = await fetch("/api/journal");
  if (!r.ok) return;
  const trades = await r.json();
  JOURNAL_TRADES = trades;
  $("learn-trade-count").textContent = `${trades.length} trade${trades.length === 1 ? "" : "s"}`;

  const list = $("journal-list");
  if (trades.length === 0) {
    list.innerHTML = `<div class="pod-empty" style="padding: 24px 0;">
      <div class="icon" style="font-size: 18px;">📓</div>
      <div style="font-size: 11px;">No trades yet — execute one from the Risk Engine</div>
    </div>`;
    return;
  }
  list.innerHTML = trades.slice(0, 10).map(t => {
    const dt = new Date(t.opened_at).toLocaleString(undefined, { hour12: false, month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
    const pnl = t.unrealized_pnl ?? 0;
    const pnlClass = pnl >= 0 ? "positive" : "negative";
    const pnlText = (pnl >= 0 ? "+" : "−") + "$" + Math.abs(pnl).toLocaleString();
    return `
      <div class="journal-row">
        <div class="j-time">${dt}</div>
        <div class="j-event">
          <span class="ticker">${t.ticker}</span> · ${t.shares} sh @ $${t.entry_price?.toFixed(2)}
          <span class="fk">FK: ${t.event_id || "—"}</span>
        </div>
        <div class="j-side ${t.side}">${t.side}</div>
        <div class="j-pnl ${pnlClass}">${pnlText}</div>
      </div>
    `;
  }).join("");

  // Stats
  const realized = trades.reduce((s, t) => s + (t.realized_pnl ?? 0), 0);
  const wins = trades.filter(t => (t.realized_pnl ?? 0) > 0).length;
  const closed = trades.filter(t => t.realized_pnl !== null && t.realized_pnl !== undefined).length;
  const realEl = $("stat-realized");
  realEl.textContent = (realized >= 0 ? "+" : "−") + "$" + Math.abs(realized).toLocaleString();
  realEl.className = "stat-value " + (realized >= 0 ? "positive" : "negative");
  $("stat-winrate").textContent = closed > 0 ? `${Math.round(100 * wins / closed)}%` : "—";
  $("stat-hold").textContent = trades.length > 0 ? "—" : "—";
}

// ============================================================
// Watchlist (Pod 1 - Signal) — user-selected assets
// ============================================================
const STATE_BEAR_FILE = {
  FIRED_UP: "bear_fired_up.png", ANGRY: "bear_angry.png",
  WORRIED:  "bear_worried.png",  THINKING: "bear_thinking.png",
  CONFUSED: "bear_confused.png", SLEEPING: "bear_sleeping.png",
};
const STATE_COLOR = {
  FIRED_UP: "var(--danger)", ANGRY: "var(--danger)",
  WORRIED:  "var(--warning)", THINKING: "var(--accent-signal)",
  CONFUSED: "var(--text-secondary)", SLEEPING: "var(--text-tertiary)",
};

async function refreshWatchlist() {
  const r = await fetch("/api/selections");
  if (!r.ok) {
    document.getElementById("signal-firms").innerHTML =
      `<div style="font-size:12px;color:var(--text-tertiary);text-align:center;padding:32px 12px;">
        Sign in to build a personal watchlist.<br>
        <a href="/login" style="color:var(--accent-signal);font-size:11px;margin-top:8px;display:inline-block;">Sign in →</a>
      </div>`;
    document.getElementById("watchlist-count").textContent = "—";
    return;
  }
  const data = await r.json();
  const sel = data.selected || [];
  const sug = data.suggested || [];
  document.getElementById("watchlist-count").textContent = `${sel.length}/5`;

  const rowHtml = (f, isSuggestion=false, idx=0) => {
    const bear = STATE_BEAR_FILE[f.bear_state] || "bear_sleeping.png";
    const zCol = STATE_COLOR[f.bear_state] || "var(--text-secondary)";
    const dim  = isSuggestion ? "opacity:0.45;" : "";
    const action = isSuggestion
      ? `<button class="add-btn" data-add="${f.ticker}" data-tip="Add ${f.ticker} to your watchlist">+</button>`
      : `<div style="display:flex;gap:4px;align-items:center;">
           <button class="fire-btn" data-fire="${f.ticker}" data-tip="🔥 Fire up the BearWatch engine — runs all 4 gates + 7 risk checks">🔥</button>
           <button class="rank-btn" data-up="${f.ticker}" data-tip="Move up" ${idx===0?"disabled":""}>↑</button>
           <button class="rank-btn" data-down="${f.ticker}" data-tip="Move down" ${idx===sel.length-1?"disabled":""}>↓</button>
           <button class="rank-btn rm" data-rm="${f.ticker}" data-tip="Remove from watchlist">×</button>
         </div>`;
    // Demo-mode price override: use date-coded historical price (yellow) when DEMO_DATE_PRICES is defined
    const demoPx = (window.DEMO_DATE_PRICES || {})[f.ticker];
    const isDemoPx = demoPx !== undefined && demoPx !== null;
    const effectivePx = isDemoPx ? demoPx : f.price;
    const priceTxt = (effectivePx !== null && effectivePx !== undefined)
      ? `$${Number(effectivePx).toFixed(2)}`
      : (f.private ? 'private' : (f.delisted ? 'delisted' : '—'));
    const priceStyle = isDemoPx ? 'color:#facc15;font-weight:600;' : 'color:var(--text-secondary);';
    const priceTitle = isDemoPx ? `title="${window.DEMO_DATE_LABEL || ''}"` : '';
    const dotMarkup = isDemoPx
      ? `<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:#facc15;box-shadow:0 0 4px #facc15;vertical-align:middle;margin-left:4px;" title="${window.DEMO_DATE_LABEL || ''}"></span>`
      : ((f.price !== null && f.price !== undefined && !isSuggestion)
          ? `<span class="live-dot" title="Live yfinance quote"></span>` : '');
    return `
      <div class="firm-row" data-ticker="${f.ticker}" style="${dim}">
        <div class="firm-rank">${isSuggestion ? "·" : (f.rank ?? idx+1)}</div>
        <img class="firm-bear" src="/static/bears/${bear}" alt="">
        <div>
          <span class="firm-ticker">${f.ticker}${isSuggestion?'<span style="font-size:9px;color:var(--text-quat);margin-left:4px;font-weight:400;text-transform:uppercase;letter-spacing:0.04em;">suggested</span>':''}</span>
          <span class="firm-sector">${f.sector_label || f.sector} · <span style="${priceStyle}font-family:var(--font-mono);" ${priceTitle}>${priceTxt}</span> ${dotMarkup}</span>
        </div>
        <div class="firm-z" style="color:${zCol};">${f.bsi_z.toFixed(2)}</div>
        <div class="bear-state-pill" data-state="${f.bear_state}">${f.bear_state.replace('_',' ')}</div>
        ${action}
      </div>
    `;
  };

  let html = sel.map((f, i) => rowHtml(f, false, i)).join("");
  // Pad with greyed suggestions to reach 5 rows
  for (let i = 0; i < sug.length && (sel.length + i) < 5; i++) {
    html += rowHtml(sug[i], true, sel.length + i);
  }
  if (sel.length === 0 && sug.length === 0) {
    html = `<div style="text-align:center;padding:24px;color:var(--text-tertiary);font-size:12px;">No assets to show.</div>`;
  }
  document.getElementById("signal-firms").innerHTML = html;

  // Wire button handlers
  document.querySelectorAll("[data-add]").forEach(b => b.addEventListener("click", e => addAsset(b.dataset.add)));
  document.querySelectorAll("[data-rm]").forEach(b => b.addEventListener("click", e => removeAsset(b.dataset.rm)));
  document.querySelectorAll("[data-up]").forEach(b => b.addEventListener("click", e => reorderAsset(b.dataset.up, -1)));
  document.querySelectorAll("[data-down]").forEach(b => b.addEventListener("click", e => reorderAsset(b.dataset.down, +1)));
  document.querySelectorAll("[data-fire]").forEach(b => b.addEventListener("click", e => fireFromWatchlist(b.dataset.fire)));
}

// Generic firer — works for ANY ticker by fetching firm metadata first
async function fireFromWatchlist(ticker) {
  // If we have a hardcoded payload (CVNA/UPST/AFRM), use the rich version
  if (DEMO_PAYLOADS[ticker]) {
    return fireEventWithMascot(ticker);
  }
  // Otherwise build the payload from the firm's API metadata
  try {
    const r = await fetch(`/api/firm/${ticker}`);
    if (!r.ok) { toast(`Could not load ${ticker}`, "err"); return; }
    const f = await r.json();
    const payload = {
      ticker: f.ticker, firm_name: f.name, sector: f.sector,
      bear_state: f.bear_state,
      signal: { bsi_z: f.bsi_z, phase: f.phase, h2_eligible: f.h2_eligible,
                expert_confirm: 2, days_above_2: 5 },
      pillars: f.pillars || {},
      war_room: { verdict: f.bsi_z >= 2.5 ? "4-of-5 SHORT" : "3-of-5 SHORT" },
      recommended_action: { side: "SHORT",
                            horizon_days: 540,
                            conviction: f.bsi_z >= 3 ? "high" : f.bsi_z >= 2 ? "medium" : "low" }
    };
    // Inject mascot threshold + size cap (same as hardcoded path)
    if (payload.signal.bsi_z < ACTIVE_MASCOT_DATA.z) {
      toast(`${ACTIVE_MASCOT} BLOCKS ${ticker}: BSI z=${payload.signal.bsi_z} < ${ACTIVE_MASCOT_DATA.z}`, "warn");
      return;
    }
    payload.mascot = ACTIVE_MASCOT;
    payload.size_cap = ACTIVE_MASCOT_DATA.size;
    PENDING_EVENT = payload;
    showGates(payload);
    showDebate(payload);
    await runRiskCheck(payload);
    toast(`Fired ${ticker} — flowing through pipeline`, "ok");
  } catch (e) {
    toast(`Failed: ${e.message}`, "err");
  }
}

// Helper exposed for the original (CVNA/UPST/AFRM) path
async function fireEventWithMascot(key) {
  const payload = JSON.parse(JSON.stringify(DEMO_PAYLOADS[key]));
  if (payload.signal.bsi_z < ACTIVE_MASCOT_DATA.z) {
    toast(`${ACTIVE_MASCOT} BLOCKS ${payload.ticker}: BSI z=${payload.signal.bsi_z} < ${ACTIVE_MASCOT_DATA.z}`, "warn");
    return;
  }
  payload.mascot = ACTIVE_MASCOT;
  payload.size_cap = ACTIVE_MASCOT_DATA.size;
  PENDING_EVENT = payload;
  showGates(payload);
  showDebate(payload);
  await runRiskCheck(payload);
  toast(`Fired ${payload.ticker} — flowing through pipeline`, "ok");
}

async function addAsset(ticker) {
  const r = await fetch("/api/selections", {
    method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({ticker})
  });
  if (r.ok) { toast(`${ticker} added to watchlist`, "ok"); refreshWatchlist(); refreshSuggestion(); }
  else { const e = await r.json(); toast(`Could not add: ${e.error || "error"}`, "err"); }
}
async function removeAsset(ticker) {
  await fetch(`/api/selections/${ticker}`, {method:"DELETE"});
  toast(`${ticker} removed`, "info"); refreshWatchlist(); refreshSuggestion();
}
async function reorderAsset(ticker, dir) {
  const r = await fetch("/api/selections");
  const sel = (await r.json()).selected.map(x => x.ticker);
  const i = sel.indexOf(ticker);
  if (i < 0) return;
  const j = i + dir;
  if (j < 0 || j >= sel.length) return;
  [sel[i], sel[j]] = [sel[j], sel[i]];
  await fetch("/api/selections/reorder", {
    method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({ordered: sel})
  });
  refreshWatchlist(); refreshSuggestion();
}

const _btnClear = document.getElementById("btn-clear-watchlist");
if (_btnClear) _btnClear.addEventListener("click", async () => {
  const r = await fetch("/api/selections");
  if (!r.ok) return;
  const sel = (await r.json()).selected || [];
  for (const s of sel) await fetch(`/api/selections/${s.ticker}`, {method:"DELETE"});
  toast("Watchlist cleared", "info"); refreshWatchlist(); refreshSuggestion();
});

// ============================================================
// Override layer — let user override Apollo's verdict
// ============================================================
async function showOverrideButton() {
  const verdictEl = document.getElementById("exec-result");
  if (!verdictEl) return;
  const verdict = verdictEl.dataset.status;
  // Show override button for any non-APPROVED verdict
  if (verdict === "BLOCKED" || verdict === "SCALED_DOWN") {
    let btn = document.getElementById("btn-override");
    if (!btn) {
      btn = document.createElement("button");
      btn.id = "btn-override";
      btn.className = "btn btn-secondary";
      btn.style.marginTop = "8px";
      btn.textContent = "Override — I know better";
      btn.onclick = openOverrideModal;
      document.getElementById("verdict-execute").appendChild(btn);
    }
    btn.style.display = "block";
  }
}

function openOverrideModal() {
  const reason = prompt(
    "Override Apollo's verdict.\n\n" +
    "Why do you think you know better? Your reasoning is logged for the model-calibration team.\n\n" +
    "(Examples: 'macro context contradicts gate', 'specific firm news Apollo missed', 'gut call')",
    ""
  );
  if (!reason) return;
  const verdict = document.getElementById("exec-result").dataset.status;
  const ticker = PENDING_EVENT?.ticker;
  fetch("/api/override", {
    method: "POST", headers: {"Content-Type":"application/json"},
    body: JSON.stringify({
      ticker, pod_verdict: verdict, user_action: "FORCED_EXECUTE", reason,
      event_id: APOLLO_VERDICT?.event_id
    })
  }).then(r => r.json()).then(d => {
    toast("Override logged. Trade forced through. The pod learns from your judgment.", "warn");
    // Force-update verdict display
    document.getElementById("exec-result").textContent = "OVERRIDE";
    document.getElementById("exec-result").style.color = "#a78bfa";
    document.getElementById("btn-execute").disabled = false;
    document.getElementById("btn-override").style.display = "none";
  }).catch(() => toast("Override failed", "err"));
}

// Wrap runRiskCheck to call showOverrideButton after verdict renders
const _origRunRiskCheck = runRiskCheck;
runRiskCheck = async function(p) {
  await _origRunRiskCheck(p);
  showOverrideButton();
};

function refreshAll() {
  refreshPortfolio();
  refreshJournal();
  refreshWatchlist();
  refreshSuggestion();
}

// Boot
document.addEventListener("DOMContentLoaded", () => {
  refreshAll();
  setInterval(refreshAll, 30000);
});
