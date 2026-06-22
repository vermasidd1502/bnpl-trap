/* Marle-G — universal page guide ("the story").
   A half-animated, spotlight-driven walkthrough of how to use each page.
   nav.js loads this on every page; it injects a "guide" button into the nav bar and,
   on click, plays a short scripted tour: each scene fades in, a saffron ring pulses over
   the real element it's describing, a progress bar auto-advances, arrows / dots let you steer.
   Content is per-page (GUIDES keyed by filename) with a sensible default for any page. */
(function () {
  "use strict";

  // ---------------------------------------------------------------- content
  // Each scene: { h: headline, t: story text, sel: optional CSS selector to spotlight }
  var GUIDES = {
    "marle_g_factors.html": { title: "Factor Lab", scenes: [
      { h: "The edge is long", t: "The top boxes summarise the research: India's tradeable edge is LONG (own/call low-vol names near their highs); shorting is an anti-edge. Read these first.", sel: "#takeaway" },
      { h: "Live long screen", t: "Today's top names by the backtested winning composite — low realized vol + strength near the 52-week high + liquidity. Click any name to open it in the options pod (express it with calls).", sel: "#screen" },
      { h: "Factor IC", t: "Which raw signals predict forward returns: IC = rank-correlation, |t|>2 = real & stable. Green = long-the-high; red = short-the-high; dim = noise. Note RSI/over-extension are noise — overbought-short isn't real.", sel: "#ic" },
      { h: "Strategy leaderboard", t: "Long/short/long-short portfolios net of costs, ranked by annualised Sharpe. The bold rows beat the benchmark; the footnote flags which are regime-robust vs bull-only.", sel: "#lb" }
    ]},
    "marle_g_position.html": { title: "Position & Levels Desk", scenes: [
      { h: "Is the contract viable?", t: "Each held option gets a 0–100 viability band: odds of beating YOUR breakeven × reachability × time runway × decay-drag × liquidity × vol. The intrinsic/time-value split shows how much of the premium will simply bleed away by expiry.", sel: "#positions" },
      { h: "Mind the decay floor", t: "The red strip is the honest base case: what the option is worth if the underlying sits flat to expiry. An OTM call's floor is zero — green-on-screen can still be a losing hold.", sel: ".pcard" },
      { h: "Scope = the upside", t: "The scope ladder reprices the SAME contract at ±1σ / ±2σ moves — what you'd make if it gets there and you sell, vs the flat-floor downside. Odds are risk-neutral (no assumed edge)." },
      { h: "Where's resistance & the stops", t: "Each position shows its underlying's merged levels; the search box runs the full Levels Desk on ANY stock — Groww-style daily pivots (Classic/Fib/Camarilla) + volume HVN + stop pools + the live L1/L2 order book.", sel: "#q" }
    ]},
    "marle_g_pod.html": { title: "Home — the cockpit", scenes: [
      { h: "Welcome to Marle-G", t: "Your India-market decision desk. Every pod is a lens on the same live book — paper-safe, read-only on Groww. Nothing here places an order." },
      { h: "Navigate by group", t: "The top bar is grouped: markets · strategies · scenarios · portfolio · tools. Click any tab to jump. The orange tab is where you are now.", sel: ".tabs" },
      { h: "Refresh anytime", t: "Data is server-cached and auto-refreshes, but hit ‘refresh’ to force a re-pull on demand.", sel: ".mg-refresh" },
      { h: "If something looks off", t: "Open ‘diag’ (far right) to self-diagnose every feed, process and scheduled task — it tells you exactly what to restart." }
    ]},
    "marle_g_equity.html": { title: "Equity — one stock, deep", scenes: [
      { h: "Search any stock", t: "Type a ticker; the autocomplete pulls from the full NSE universe. Pick it to load fundamentals, quality and valuation.", sel: "input" },
      { h: "Read the quality block", t: "Statement-computed ratios (not Yahoo’s sparse .info): growth, margins, Piotroski, a coverage-aware quality score. Low coverage is flagged, never faked." },
      { h: "Then act on it", t: "Use the modal’s buttons to push the name into your volume monitor or straight to the intraday tracker." }
    ]},
    "marle_g_intraday.html": { title: "Intraday — the live trench", scenes: [
      { h: "Pick your interval", t: "5m for entries, 15m for the trend. The bar up top switches every standard timeframe; analytics recompute live.", sel: ".ivbar, #intervalBar, .intervals" },
      { h: "Read the live position", t: "VWAP, EMA, momentum, a day-range meter and your stop/target strip — all marked to the live Groww quote, zero-lag via the tick recorder." },
      { h: "RSI engine", t: "Multi-timeframe RSI (5m·15m·1h·1D) with divergence flags — overbought/oversold to time entries and exits." },
      { h: "Double due-diligence", t: "For a gated volume-pod pick this runs gate ✕ fundamentals ✕ intraday — and the chase-guard vetoes anything already rallied to its ceiling." }
    ]},
    "marle_g_volume.html": { title: "Volume — the funnel", scenes: [
      { h: "What you’re seeing", t: "Today’s universe ranked by volume thrust. The gate column shows which names cleared all checks — those are the candidates." },
      { h: "Filter fast", t: "The search box here is a pure filter over the table — type to narrow, not to look up a new symbol.", sel: "#q" },
      { h: "Push to intraday", t: "Found a mover? Send it to the intraday tracker with one click and watch it minute-by-minute." }
    ]},
    "marle_g_weekend.html": { title: "Weekend — Friday→Monday", scenes: [
      { h: "Two modes, one toggle", t: "Friday = pick names worth carrying over the weekend. Monday = see how the carry is opening. Flip with the toggle.", sel: ".toggle, #modeToggle, .modes" },
      { h: "Accumulation vs distribution", t: "Each name is scored on Chaikin A/D, OBV and O’Neil distribution days — accumulation carries, distribution gets vetoed (🔴)." },
      { h: "The honest edge", t: "Unconditional weekend-hold loses. Only Friday-momentum → Monday paid (+0.70% / 75%) — and only as delivery, not MTF. The pod enforces that." },
      { h: "Check any stock", t: "The search bar runs the same weekend logic on a name you type — independent of the board.", sel: "input" }
    ]},
    "marle_g_winners.html": { title: "Winners / Losers", scenes: [
      { h: "Your live scoreboard", t: "Your real Groww book, marked to the live price every 20s. Green = winning today, red = losing." },
      { h: "Two different numbers", t: "‘today’ is the stock’s move since yesterday’s close. ‘my P&L’ is live price vs YOUR cost — they diverge when you bought above the current price." },
      { h: "MTF-aware", t: "Leveraged lots are de-duplicated (net carry-forward), so no phantom double-counting of MTF shares." }
    ]},
    "marle_g_risk.html": { title: "Risk — VaR & what-if", scenes: [
      { h: "The five gauges", t: "Beta (market gearing), daily vol, 1-day 99% VaR, weekend ES, diversification — your risk at a glance.", sel: "#cards" },
      { h: "Simulate before you trade", t: "The what-if tool: type a ticker + qty, hit Simulate. It re-runs beta, weekend VaR, correlation and diversification against your live book — no order placed.", sel: "#whatifCard" },
      { h: "Monte Carlo to Monday", t: "30,000 correlated, fat-tailed futures of your exact book → the histogram is the distribution of what could happen by Monday.", sel: "#hist" },
      { h: "Systematic vs idiosyncratic", t: "How much of your risk is the market (can’t diversify) vs stock-specific (can). High systematic = a leveraged Nifty bet; only lower beta or a hedge helps.", sel: "#decomp" }
    ]},
    "marle_g_diag.html": { title: "Diagnosis — the harness", scenes: [
      { h: "One glance, whole system", t: "The banner is the overall verdict. Green dots are healthy, amber are warnings (often just market-closed), red need action." },
      { h: "Grouped checks", t: "Processes · feeds · data freshness · code health · live route probes · scheduled tasks. Problems float to the top of each group." },
      { h: "Copy the fix", t: "Every red/amber item ships the exact command to fix it — click ‘copy’ and paste into PowerShell.", sel: ".sect" },
      { h: "Re-run on demand", t: "Hit ‘Re-run diagnosis’ after a fix to confirm it went green.", sel: "#rerun" }
    ]},
    "marle_g_strategies.html": { title: "Strategies — playbooks", scenes: [
      { h: "Honest playbooks", t: "Each strategy ships with a real backtest and a cost-of-trading reality check — survivors only." },
      { h: "Try it on a stock", t: "Paper-trade any playbook on a name you pick before risking capital." }
    ]},
    "marle_g_watch.html": { title: "Watch Desk — sit & fire", scenes: [
      { h: "Your live trade board", t: "Every name in your watchlist + held book is sorted into a lane and re-scanned every 2½ minutes. It does all the thinking — you just tap the order. Read-only: it NEVER places a trade." },
      { h: "Two inputs", t: "‘watch’ = names to scan (RELIANCE,SBIN,BHEL…). ‘held’ = your positions as SYMBOL:qty:avg (TEJASNET:100:560). Both save in this browser. Hit ‘scan ↻’ to refresh now.", sel: "#wl" },
      { h: "The four lanes", t: "🔫 FIRE = a VALIDATED dip-buy is live — place it. 🛠 MANAGE = a held name needs a stop/target move. 👀 WATCH = uptrend, waiting for the dip (shows the buy-zone). ⛔ aside = no trade.", sel: "#board" },
      { h: "Only what matters", t: "The FIRE/MANAGE counts up top tell you instantly if there’s anything to do. Click any symbol to open its full single-stock page.", sel: "#counts" },
      { h: "Slack when you’re away", t: "Run ‘marleg_watch.py --slack --loop 180’ during market hours and it pings only FIRE/MANAGE to Slack — so you can leave the screen and still get the call." }
    ]}
  };

  var DEFAULT = { title: "How this page works", scenes: [
    { h: "You’re on a Marle-G pod", t: "Every pod reads the same live book and is paper-safe — read-only on Groww, no orders." },
    { h: "Navigate up top", t: "Grouped tabs jump between pods; the orange one is the current page.", sel: ".tabs" },
    { h: "Refresh & diagnose", t: "‘refresh’ forces a re-pull; ‘diag’ (far right) self-checks the whole system if anything looks wrong.", sel: ".mg-refresh" }
  ]};

  function guideFor() {
    var p = (location.pathname.split("/").pop() || "marle_g_pod.html").toLowerCase();
    return GUIDES[p] || DEFAULT;
  }

  // ---------------------------------------------------------------- styles
  function injectCSS() {
    if (document.getElementById("mgg-css")) return;
    var s = document.createElement("style"); s.id = "mgg-css";
    s.textContent = [
      ".mgg-btn{margin-left:10px;display:inline-flex;align-items:center;gap:6px;font-family:var(--mono,ui-monospace,monospace);font-size:11px;font-weight:700;color:var(--dim,#646c7a);background:rgba(255,255,255,.03);border:1px solid var(--border,rgba(255,255,255,.08));border-radius:8px;padding:6px 11px;cursor:pointer;flex:0 0 auto}",
      ".mgg-btn:hover{color:var(--saffron,#ff9933);border-color:var(--saffron,#ff9933)}",
      ".mgg-btn.hint{animation:mgg-bp 1.6s ease-in-out 3}",
      "@keyframes mgg-bp{0%,100%{box-shadow:0 0 0 0 rgba(255,153,51,0)}50%{box-shadow:0 0 0 4px rgba(255,153,51,.25)}}",
      ".mgg-catch{position:fixed;inset:0;z-index:99990;background:transparent}",
      ".mgg-veil{position:fixed;inset:0;z-index:99991;background:rgba(6,8,12,.74);pointer-events:none}",
      ".mgg-spot{position:fixed;z-index:99991;border-radius:12px;border:2px solid var(--saffron,#ff9933);box-shadow:0 0 0 9999px rgba(6,8,12,.74),0 0 22px rgba(255,153,51,.6);pointer-events:none;transition:all .45s cubic-bezier(.5,.1,.2,1);animation:mgg-pulse 1.8s ease-in-out infinite}",
      "@keyframes mgg-pulse{0%,100%{box-shadow:0 0 0 9999px rgba(6,8,12,.74),0 0 0 2px rgba(255,153,51,.5)}50%{box-shadow:0 0 0 9999px rgba(6,8,12,.74),0 0 26px 4px rgba(255,153,51,.75)}}",
      ".mgg-card{position:fixed;z-index:99993;left:50%;bottom:34px;transform:translateX(-50%);width:min(520px,92vw);background:#12151c;border:1px solid rgba(255,255,255,.1);border-radius:16px;box-shadow:0 24px 70px rgba(0,0,0,.6);overflow:hidden;font-family:Inter,system-ui,sans-serif}",
      ".mgg-prog{height:3px;background:var(--saffron,#ff9933);width:0%}",
      ".mgg-top{display:flex;align-items:center;gap:10px;padding:14px 18px 0}",
      ".mgg-kick{font-family:var(--mono,monospace);font-size:9px;font-weight:800;letter-spacing:.18em;padding:3px 8px;border-radius:5px;background:linear-gradient(90deg,rgba(255,153,51,.2),rgba(19,136,8,.2));color:var(--saffron,#ff9933)}",
      ".mgg-ttl{font-family:var(--mono,monospace);font-size:11px;color:var(--dim,#9aa3b4);font-weight:700}",
      ".mgg-x{margin-left:auto;background:none;border:none;color:var(--dim,#646c7a);font-size:16px;cursor:pointer;line-height:1}",
      ".mgg-x:hover{color:#eef1f6}",
      ".mgg-scene{padding:12px 18px 4px;min-height:96px}",
      ".mgg-h{font-size:16px;font-weight:800;color:#eef1f6;margin-bottom:7px;letter-spacing:-.01em}",
      ".mgg-t{font-size:13px;line-height:1.62;color:#aeb6c4}",
      ".mgg-anim .mgg-h{animation:mgg-in .42s ease both}",
      ".mgg-anim .mgg-t{animation:mgg-in .42s ease .06s both}",
      "@keyframes mgg-in{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}",
      ".mgg-foot{display:flex;align-items:center;justify-content:space-between;padding:8px 18px 16px}",
      ".mgg-dots{display:flex;gap:6px}",
      ".mgg-dot{width:7px;height:7px;border-radius:50%;background:rgba(255,255,255,.16);cursor:pointer;transition:all .2s}",
      ".mgg-dot.on{background:var(--saffron,#ff9933);width:18px;border-radius:4px}",
      ".mgg-arrows{display:flex;gap:8px}",
      ".mgg-a{font-family:var(--mono,monospace);font-size:11px;font-weight:700;color:#eef1f6;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);border-radius:8px;padding:7px 13px;cursor:pointer}",
      ".mgg-a:hover{border-color:var(--saffron,#ff9933);color:var(--saffron,#ff9933)}",
      ".mgg-a.pri{background:var(--saffron,#ff9933);color:#0b0d12;border-color:var(--saffron,#ff9933)}",
      ".mgg-a[disabled]{opacity:.35;cursor:default}"
    ].join("");
    document.head.appendChild(s);
  }

  // ---------------------------------------------------------------- engine
  var G = null, idx = 0, timer = null, DUR = 7000, paused = false;

  function el(tag, cls) { var e = document.createElement(tag); if (cls) e.className = cls; return e; }

  function open() {
    G = guideFor(); idx = 0;
    injectCSS();
    close(true);                                          // clear any prior
    var catch_ = el("div", "mgg-catch"); catch_.id = "mgg-catch";
    catch_.addEventListener("click", function () { close(); });
    var veil = el("div", "mgg-veil"); veil.id = "mgg-veil";
    var spot = el("div", "mgg-spot"); spot.id = "mgg-spot"; spot.style.display = "none";
    var card = el("div", "mgg-card"); card.id = "mgg-card";
    card.innerHTML =
      '<div class="mgg-prog" id="mgg-prog"></div>' +
      '<div class="mgg-top"><span class="mgg-kick">GUIDE</span><span class="mgg-ttl" id="mgg-ttl"></span><button class="mgg-x" title="close (Esc)">✕</button></div>' +
      '<div class="mgg-scene mgg-anim"><div class="mgg-h" id="mgg-h"></div><div class="mgg-t" id="mgg-t"></div></div>' +
      '<div class="mgg-foot"><div class="mgg-dots" id="mgg-dots"></div>' +
      '<div class="mgg-arrows"><button class="mgg-a" id="mgg-prev">‹ back</button><button class="mgg-a pri" id="mgg-next">next ›</button></div></div>';
    card.querySelector(".mgg-x").addEventListener("click", function () { close(); });
    card.addEventListener("mouseenter", function () { paused = true; });
    card.addEventListener("mouseleave", function () { paused = false; });
    document.body.appendChild(catch_); document.body.appendChild(veil);
    document.body.appendChild(spot); document.body.appendChild(card);
    document.getElementById("mgg-prev").addEventListener("click", function () { go(idx - 1); });
    document.getElementById("mgg-next").addEventListener("click", function () { if (idx >= G.scenes.length - 1) close(); else go(idx + 1); });
    document.addEventListener("keydown", onKey, true);
    show();
  }

  function onKey(e) {
    if (!document.getElementById("mgg-card")) return;
    if (e.key === "Escape") { close(); e.stopPropagation(); }
    else if (e.key === "ArrowRight") { if (idx < G.scenes.length - 1) go(idx + 1); }
    else if (e.key === "ArrowLeft") { go(idx - 1); }
  }

  function go(i) { idx = Math.max(0, Math.min(G.scenes.length - 1, i)); show(); }

  function spotlight(sel) {
    var spot = document.getElementById("mgg-spot"), veil = document.getElementById("mgg-veil");
    var node = null;
    if (sel) { try { node = document.querySelector(sel); } catch (e) {} }
    if (node && node.offsetParent !== null) {
      try { node.scrollIntoView({ block: "center", behavior: "smooth" }); } catch (e) {}
      setTimeout(function () {
        var r = node.getBoundingClientRect(), pad = 8;
        spot.style.display = "block";
        spot.style.left = (r.left - pad) + "px"; spot.style.top = (r.top - pad) + "px";
        spot.style.width = (r.width + pad * 2) + "px"; spot.style.height = (r.height + pad * 2) + "px";
        if (veil) veil.style.display = "none";          // spot's box-shadow does the dimming
      }, 180);
    } else {
      spot.style.display = "none";
      if (veil) veil.style.display = "block";           // no target -> full veil
    }
  }

  function show() {
    var s = G.scenes[idx];
    document.getElementById("mgg-ttl").textContent = G.title;
    var scene = document.querySelector(".mgg-scene");
    scene.classList.remove("mgg-anim"); void scene.offsetWidth; scene.classList.add("mgg-anim");
    document.getElementById("mgg-h").textContent = s.h;
    document.getElementById("mgg-t").textContent = s.t;
    var dots = G.scenes.map(function (_, i) { return '<span class="mgg-dot ' + (i === idx ? "on" : "") + '" data-i="' + i + '"></span>'; }).join("");
    var dwrap = document.getElementById("mgg-dots"); dwrap.innerHTML = dots;
    Array.prototype.forEach.call(dwrap.children, function (d) { d.addEventListener("click", function () { go(+d.dataset.i); }); });
    document.getElementById("mgg-prev").disabled = idx === 0;
    document.getElementById("mgg-next").textContent = idx >= G.scenes.length - 1 ? "done ✓" : "next ›";
    spotlight(s.sel);
    startTimer();
  }

  function startTimer() {
    clearInterval(timer);
    var prog = document.getElementById("mgg-prog"); if (!prog) return;
    var t0 = 0; prog.style.width = "0%";
    timer = setInterval(function () {
      if (paused) return;
      t0 += 100; var pct = Math.min(100, t0 / DUR * 100);
      prog.style.width = pct + "%";
      if (t0 >= DUR) {
        if (idx >= G.scenes.length - 1) { close(); }
        else go(idx + 1);
      }
    }, 100);
  }

  function close(silent) {
    clearInterval(timer);
    ["mgg-catch", "mgg-veil", "mgg-spot", "mgg-card"].forEach(function (id) {
      var n = document.getElementById(id); if (n) n.remove();
    });
    document.removeEventListener("keydown", onKey, true);
    if (!silent) {
      try { localStorage.setItem("mgg_seen_" + (location.pathname.split("/").pop() || "x"), "1"); } catch (e) {}
    }
  }

  // ---------------------------------------------------------------- mount
  function mount() {
    injectCSS();
    document.querySelectorAll(".nav").forEach(function (nav) {
      if (nav.querySelector(".mgg-btn")) return;
      var b = el("button", "mgg-btn");
      b.innerHTML = '<span>▶</span><span>guide</span>';
      b.title = "How this page works";
      b.addEventListener("click", open);
      nav.appendChild(b);
      // gentle one-time hint pulse on first visit to this page
      try {
        var seen = localStorage.getItem("mgg_seen_" + (location.pathname.split("/").pop() || "x"));
        if (!seen) b.classList.add("hint");
      } catch (e) {}
    });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount);
  else mount();
  window.MGGuide = { open: open };
})();
