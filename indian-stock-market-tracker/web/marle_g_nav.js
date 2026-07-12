/* Marle-G — shared top nav. Single source of truth for every pod page.
   Each page just includes <script src="marle_g_nav.js"></script>; this fills its .tabs bar
   with grouped DROPDOWN menus of the complete pod list (home · markets · strategies ·
   scenarios · portfolio · tools) and highlights both the active group and the active page.
   The full list lives here once, so every pod gets the identical toolbar for free. */
(function () {
  if (/[?&]embed=1/.test(location.search)) {                 // EMBED MODE — clean pod chrome off for hub-dashboard iframe tiles
    var _e = document.createElement("style");
    _e.textContent = ".tabs,#mg-macrobar,#mg-chartfab,#mg-sigfab,#mg-predfab,#mg-pafab,#mg-tradebox,#mg-heartbeat,.mg-backfwd,.mg-regime,#mg-macrobanner,#mg-guidebtn{display:none!important}body{padding-top:6px!important}";
    document.head.appendChild(_e);
  }
  /* 9 HUBS — consolidated from 51 pages. Each dropdown = one hub; every page reachable as a tab.
     (Consolidation map: design-handoff/04_CONSOLIDATION_PLAN.md) */
  var GROUPS = [
    { label: "home", solo: true, items: [["marle_g_dashboard.html", "🏠 home"]] },
    { label: "cockpit", icon: "🔫", items: [
      ["marle_g_dayboard.html", "☀ day board"], ["marle_g_pa.html", "🤖 assistant"], ["marle_g_console.html", "🧭 console"], ["marle_g_firm.html", "🏛 the firm"], ["marle_g_signal.html", "🪪 signal card"], ["marle_g_cockpit.html", "🔫 cockpit"], ["marle_g_mistakes.html", "🪞 mistakes"], ["marle_g_gate.html", "🚦 session gate"], ["marle_g_intraday.html", "intraday vol"],
      ["marle_g_router.html", "router"], ["marle_g_warroom.html", "⚔ war room"], ["marle_g_watch.html", "👀 watch desk"]] },
    { label: "deep-dive", icon: "🔬", items: [
      ["marle_g_atlas.html", "🛰 atlas — overview"], ["marle_g_stock.html", "stock"], ["marle_g_chart.html", "chart"],
      ["marle_g_fundamentals.html", "📒 fundamentals"], ["marle_g_intrinsic.html", "🔬 why it's moving"], ["marle_g_canslim.html", "canslim"], ["marle_g_smartmoney.html", "smart-money"],
      ["marle_g_buyhold.html", "buy & hold"], ["marle_g_equity.html", "equity"], ["marle_g_lab.html", "stock lab"]] },
    { label: "options", icon: "⚙", items: [
      ["marle_g_optdesk.html", "🦅 option desk (search)"], ["marle_g_kalman.html", "📡 kalman state"], ["marle_g_fadeboard.html", "🎯 fade board"],
      ["marle_g_options.html", "⚙ options / F&O"], ["marle_g_optboard.html", "🎯 option board (max-pain)"], ["marle_g_theta.html", "🧊 theta 3D surface"], ["marle_g_option_ideas.html", "🌊 option ideas"],
      ["marle_g_vol.html", "vol-lab"], ["marle_g_sectoral.html", "sectoral"], ["marle_g_niftysim.html", "nifty-sim"], ["marle_g_position.html", "📊 position desk"]] },
    { label: "screeners", icon: "📡", items: [
      ["marle_g_breaks.html", "📈 momentum ↔ reversal"], ["marle_g_recommend.html", "🧭 recommender"], ["marle_g_board.html", "🧭 command board"], ["marle_g_volume.html", "volume / gated"], ["marle_g_detector.html", "🔬 detector"], ["marle_g_reality.html", "🔍 industry reality"], ["marle_g_pickborn.html", "🌱 how a pick is born"], ["marle_g_movers.html", "⚡ movers"], ["marle_g_patterns.html", "patterns"], ["marle_g_setups.html", "🎯 setups"], ["marle_g_heatmap.html", "🗺 sector heatmap"],
      ["marle_g_industry.html", "rotation"], ["marle_g_winners.html", "winners/losers"], ["marle_g_weekend.html", "weekend"],
      ["marle_g_live.html", "live"], ["marle_g_volbook.html", "day-book"], ["marle_g_nifty.html", "🇮🇳 nifty pod"]] },
    { label: "strategy", icon: "🧪", items: [
      ["marle_g_strategist.html", "🧭 strategy picker"], ["marle_g_strategies.html", "strategies"], ["marle_g_factors.html", "🔬 factor-lab"], ["marle_g_edge_audit.html", "🧾 edge audit"], ["marle_g_research_head.html", "🧠 research head"], ["marle_g_pit.html", "🧪 PIT fundamentals"], ["marle_g_robust.html", "robust"],
      ["marle_g_builder.html", "builder"], ["marle_g_quality.html", "quality"], ["marle_g_autotrader.html", "auto-trader"],
      ["marle_g_paper.html", "paper"], ["marle_g_bearish.html", "🛡 bearish/hedge"]] },
    { label: "macro", icon: "🌐", items: [
      ["marle_g_cascade.html", "cascade"], ["marle_g_regime.html", "regime"], ["marle_g_asialead.html", "🌏 asian lead (pre-open)"], ["marle_g_indexcompare.html", "🔭 index overlay (does NIFTY follow?)"], ["marle_g_worldclock.html", "🕐 world clock"], ["marle_g_goldmacro.html", "🥇 gold & dollar"], ["marle_g_fii.html", "🌐 foreign flows"], ["marle_g_tradeglobe.html", "🌍 trade globe"], ["marle_g_hygiene.html", "🧼 hygiene"], ["marle_g_moat.html", "💎 quality / x-factor"], ["marle_g_cases.html", "🧪 case library"], ["marle_g_thesis.html", "thesis"],
      ["marle_g_macro_preview.html", "macro weather"], ["marle_g_mindhive.html", "mindhive"]] },
    { label: "portfolio", icon: "💼", items: [
      ["marle_g_journal.html", "📓 journal"], ["marle_g_ordercoach.html", "🎧 order coach"], ["marle_g_alerts.html", "🔔 alerts log"], ["marle_g_portfolio.html", "portfolio"], ["marle_g_risk.html", "risk / VaR"], ["marle_g_profile.html", "profile"],
      ["marle_g_funds.html", "💰 funds"], ["marle_g_etf.html", "📦 ETF portfolio"], ["marle_g_etfscan.html", "📊 ETF scanner + rater"], ["marle_g_allocate.html", "👨 dad's portfolio"], ["marle_g_ipo.html", "🧾 IPO desk"]] },
    { label: "tools", icon: "🔧", items: [
      ["marle_g_hub.html", "🗂 pod hub"], ["marle_g_brain.html", "🧠 engine brain"], ["marle_g_dashboard.html", "command center"], ["marle_g_diag.html", "diag"], ["marle_g_architecture_3d.html", "3D pod map"]] }
  ];
  var cur = (location.pathname.split("/").pop() || "marle_g_dashboard.html").toLowerCase();

  if (!document.getElementById("mg-nav-css")) {
    var st = document.createElement("style"); st.id = "mg-nav-css";
    st.textContent =
      ".tabs{display:flex;align-items:center;gap:13px;flex-wrap:wrap;overflow:visible}" +
      ".tabs .mg-grp{position:relative;display:inline-flex;align-items:center}" +
      ".tabs .mg-trig{font-family:var(--mono,ui-monospace,Consolas,monospace);font-size:11px;font-weight:600;" +
        "color:var(--dim,#646c7a);background:transparent;border:0;cursor:pointer;padding:5px 3px;margin:0;" +
        "display:inline-flex;align-items:center;gap:5px;letter-spacing:.02em;line-height:1}" +
      ".tabs .mg-trig:hover,.tabs .mg-grp.open .mg-trig{color:var(--ink,#eef1f6)}" +
      ".tabs .mg-trig.on{color:var(--saffron,#ff9933)}" +
      ".tabs .mg-gi{font-size:11px;line-height:1}" +
      ".tabs .mg-car{font-size:8px;opacity:.5;transition:transform .16s ease}" +
      ".tabs .mg-grp.open .mg-car{transform:rotate(180deg);opacity:.9}" +
      ".tabs .mg-menu{position:absolute;top:calc(100% + 8px);left:0;min-width:174px;background:#14171e;" +
        "border:1px solid rgba(255,255,255,.1);border-radius:12px;box-shadow:0 18px 46px rgba(0,0,0,.62);" +
        "padding:6px;z-index:99998;display:flex;flex-direction:column;opacity:0;visibility:hidden;" +
        "transform:translateY(-6px);transition:opacity .14s ease,transform .14s ease,visibility .14s}" +
      ".tabs .mg-menu.flip{left:auto;right:0}" +
      ".tabs .mg-grp.open .mg-menu{opacity:1;visibility:visible;transform:translateY(0)}" +
      ".tabs .mg-menu a{font-family:var(--mono,ui-monospace,Consolas,monospace);font-size:11.5px;" +
        "color:var(--muted,#9aa3b4);text-decoration:none;padding:7px 11px;border-radius:8px;" +
        "white-space:nowrap;letter-spacing:.01em}" +
      ".tabs .mg-menu a:hover{background:rgba(255,153,51,.13);color:var(--ink,#eef1f6)}" +
      ".tabs .mg-menu a.on{color:var(--saffron,#ff9933);background:rgba(255,153,51,.10)}" +
      ".tabs .mg-home{font-family:var(--mono,ui-monospace,Consolas,monospace);font-size:11px;font-weight:600;" +
        "color:var(--dim,#646c7a);text-decoration:none;padding:5px 3px;line-height:1;letter-spacing:.02em}" +
      ".tabs .mg-home:hover{color:var(--ink,#eef1f6)}.tabs .mg-home.on{color:var(--saffron,#ff9933)}" +
      ".tabs .mg-divider{width:1px;height:13px;background:var(--border,rgba(255,255,255,.14));opacity:.7}";
    document.head.appendChild(st);
  }

  function build() {
    return GROUPS.map(function (g) {
      var has = g.items.some(function (p) { return p[0].toLowerCase() === cur; });
      if (g.solo) {
        var p = g.items[0];
        return '<a class="mg-home' + (has ? " on" : "") + '" href="' + p[0] + '">' + p[1] + "</a>";
      }
      var dash = '<a href="marle_g_hub.html?hub=' + encodeURIComponent(g.label) + '" style="color:var(--saffron,#ff9933);font-weight:600;border-bottom:1px solid rgba(255,255,255,.08);margin-bottom:3px;padding-bottom:8px">⊞ ' + g.label + ' dashboard</a>';
      var menu = dash + g.items.map(function (p) {
        return '<a href="' + p[0] + '"' + (p[0].toLowerCase() === cur ? ' class="on"' : "") + ">" + p[1] + "</a>";
      }).join("");
      return '<div class="mg-grp" data-hub="' + g.label + '"><button type="button" class="mg-trig' + (has ? " on" : "") + '">' +
        (g.icon ? '<span class="mg-gi">' + g.icon + "</span>" : "") +
        "<span>" + g.label + '</span><span class="mg-car">▾</span></button>' +
        '<div class="mg-menu">' + menu + "</div></div>";
    }).join('<span class="mg-divider"></span>');
  }

  function closeAll(except) {
    Array.prototype.forEach.call(document.querySelectorAll(".mg-grp.open"), function (o) {
      if (o !== except) o.classList.remove("open");
    });
  }
  function wire(scope) {
    Array.prototype.forEach.call(scope.querySelectorAll(".mg-grp"), function (g) {
      var trig = g.querySelector(".mg-trig"), menu = g.querySelector(".mg-menu"), ct = null;
      function shut() { g.classList.remove("open"); }
      function open() {
        closeAll(g); g.classList.add("open");
        menu.classList.remove("flip");                                   // keep the menu on-screen at the right edge
        if (menu.getBoundingClientRect().right > window.innerWidth - 8) menu.classList.add("flip");
      }
      g.addEventListener("mouseenter", function () { clearTimeout(ct); open(); });
      g.addEventListener("mouseleave", function () { clearTimeout(ct); ct = setTimeout(shut, 220); });   // grace to cross the gap
      trig.addEventListener("click", function (e) {                    // HARD-CLICK the hub label → its dashboard (hover still opens the dropdown)
        e.stopPropagation();
        location.href = "marle_g_hub.html?hub=" + encodeURIComponent(g.getAttribute("data-hub") || "");
      });
    });
  }

  window.MG_NAV = GROUPS;                                   // expose the hub config so marle_g_hub.html can build hub-dashboards
  if (!/[?&]embed=1/.test(location.search)) {
    Array.prototype.forEach.call(document.querySelectorAll(".tabs"), function (t) { t.innerHTML = build(); wire(t); });
    document.addEventListener("click", function () { closeAll(null); });
    document.addEventListener("keydown", function (e) { if (e.key === "Escape") closeAll(null); });
  }
})();

/* ---- universal ticker autocomplete: attaches to every pod search bar ---- */
(function () {
  var DD = null, items = [], active = -1, curInput = null, cache = {}, timer = null;
  function isSearch(el) {
    if (!el || el.tagName !== "INPUT") return false;
    if (el.getAttribute && el.getAttribute("data-no-suggest") !== null) return false;  // opt-out (e.g. pure filters)
    var t = (el.type || "text").toLowerCase();
    if (t !== "text" && t !== "search") return false;
    if (el.id === "q") return true;
    return /ticker|symbol|stock|search|e\.g\./i.test(el.placeholder || "");
  }
  function looksEquity(r) {                       // drop bonds / NCDs / SGBs from suggestions
    if (!r || !r.s) return false;
    if (r.s.length > 14) return false;
    return !/NCD|SGB|\bGS\b|BOND|-N\d|%/i.test((r.n || "") + " " + r.s);
  }
  function ensureDD() {
    if (DD) return DD;
    var st = document.createElement("style");
    st.textContent =
      ".mg-sg{position:fixed;z-index:99999;background:#14171e;border:1px solid rgba(255,255,255,.12);border-radius:10px;box-shadow:0 14px 34px rgba(0,0,0,.55);font-family:'SF Mono',ui-monospace,Consolas,monospace;max-height:330px;overflow-y:auto;display:none}" +
      ".mg-sg .it{padding:8px 12px;cursor:pointer;display:flex;justify-content:space-between;gap:12px;border-bottom:1px solid rgba(255,255,255,.04)}" +
      ".mg-sg .it:hover,.mg-sg .it.on{background:rgba(255,153,51,.13)}" +
      ".mg-sg .s{color:#eef1f6;font-weight:700;font-size:12px}" +
      ".mg-sg .n{color:#9aa3b4;font-size:10px;text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:62%}";
    document.head.appendChild(st);
    DD = document.createElement("div"); DD.className = "mg-sg"; document.body.appendChild(DD);
    return DD;
  }
  function place() {
    if (!curInput || !DD) return;
    var r = curInput.getBoundingClientRect();
    DD.style.left = r.left + "px"; DD.style.top = (r.bottom + 4) + "px"; DD.style.width = Math.max(r.width, 220) + "px";
  }
  function hide() { if (DD) DD.style.display = "none"; active = -1; }
  function render() {
    ensureDD();
    if (!items.length) { hide(); return; }
    DD.innerHTML = items.map(function (r, i) {
      return '<div class="it' + (i === active ? " on" : "") + '" data-i="' + i + '"><span class="s">' + r.s +
        '</span><span class="n">' + (r.n || "").replace(/[<>]/g, "") + "</span></div>";
    }).join("");
    place(); DD.style.display = "block";
    Array.prototype.forEach.call(DD.querySelectorAll(".it"), function (el) {
      el.addEventListener("mousedown", function (e) { e.preventDefault(); pick(items[+el.dataset.i]); });
    });
  }
  function pick(rec) {
    if (!curInput || !rec) return;
    curInput.value = rec.s; hide();
    try { localStorage.setItem("mg_last_sym", String(rec.s).toUpperCase()); } catch (e) {}   // keep the symbol fluid across pages
    curInput.dispatchEvent(new Event("input", { bubbles: true }));
    ["keydown", "keyup"].forEach(function (tp) {
      curInput.dispatchEvent(new KeyboardEvent(tp, { key: "Enter", code: "Enter", keyCode: 13, which: 13, bubbles: true }));
    });
  }
  function query(v) {
    v = (v || "").trim().toUpperCase();
    if (v.length < 1) { hide(); return; }
    if (cache[v]) { items = cache[v]; active = -1; render(); return; }
    fetch("/api/symbols?q=" + encodeURIComponent(v) + "&limit=20").then(function (r) { return r.json(); }).then(function (j) {
      var list = (j || []).filter(looksEquity).slice(0, 12);
      cache[v] = list;
      if (curInput && curInput.value.trim().toUpperCase() === v) { items = list; active = -1; render(); }
    }).catch(function () {});
  }
  document.addEventListener("input", function (e) {
    if (!isSearch(e.target)) return;
    curInput = e.target; clearTimeout(timer); var v = e.target.value;
    timer = setTimeout(function () { query(v); }, 110);
  }, true);
  document.addEventListener("focusin", function (e) { if (isSearch(e.target)) { curInput = e.target; if (e.target.value.trim()) query(e.target.value); } });
  document.addEventListener("keydown", function (e) {
    if (!isSearch(e.target) || !DD || DD.style.display === "none") return;
    if (e.key === "ArrowDown") { e.preventDefault(); active = Math.min(active + 1, items.length - 1); render(); }
    else if (e.key === "ArrowUp") { e.preventDefault(); active = Math.max(active - 1, 0); render(); }
    else if (e.key === "Enter" && active >= 0) { e.preventDefault(); e.stopPropagation(); pick(items[active]); }
    else if (e.key === "Escape") { hide(); }
  }, true);
  document.addEventListener("click", function (e) { if (e.target !== curInput && DD && !DD.contains(e.target)) hide(); });
  window.addEventListener("scroll", function () { if (DD && DD.style.display === "block") place(); }, true);
  window.addEventListener("resize", function () { if (DD && DD.style.display === "block") place(); });
})();

/* ---- universal auto-refresh DISABLED on purpose ----
   It reloaded every pod every 60s and kept yanking you out of your place (scroll, snip, open panels,
   the stock you'd searched). Removed. Use the manual ⟳ refresh button in the nav when YOU want fresh data.
   Pages that truly need live data (intraday, portfolio) still soft-poll their OWN data — no full reload. */

/* ---- universal manual Refresh button: injected into every nav bar ---- */
(function () {
  function stamp(el) {
    var d = new Date();
    el.textContent = "updated " + d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
  }
  function build(nav) {
    if (!nav || nav.querySelector(".mg-refresh")) return;
    var b = document.createElement("button");
    b.className = "mg-refresh";
    b.title = "Refresh this page's data";
    b.innerHTML = '<span class="mg-rico" style="display:inline-block">⟳</span><span class="mg-rtxt">refresh</span>';
    b.style.cssText = "margin-left:14px;display:inline-flex;align-items:center;gap:6px;font-family:var(--mono,ui-monospace,monospace);" +
      "font-size:11px;font-weight:700;color:var(--dim,#646c7a);background:rgba(255,255,255,.03);" +
      "border:1px solid var(--border,rgba(255,255,255,.08));border-radius:8px;padding:6px 11px;cursor:pointer;flex:0 0 auto";
    b.addEventListener("mouseenter", function () { b.style.color = "var(--saffron,#ff9933)"; b.style.borderColor = "var(--saffron,#ff9933)"; });
    b.addEventListener("mouseleave", function () { b.style.color = "var(--dim,#646c7a)"; b.style.borderColor = "var(--border,rgba(255,255,255,.08))"; });
    b.addEventListener("click", function () {
      var ic = b.querySelector(".mg-rico"), txt = b.querySelector(".mg-rtxt");
      ic.style.transition = "transform .6s ease"; ic.style.transform = "rotate(360deg)";
      setTimeout(function () { ic.style.transition = "none"; ic.style.transform = "rotate(0deg)"; }, 640);
      // Prefer the page's own soft refresh; fall back to a full reload (data is server-cached, cheap).
      if (typeof window.podRefresh === "function") {
        try { window.podRefresh(); stamp(txt); return; } catch (e) {}
      }
      location.reload();
    });
    nav.appendChild(b);
  }
  document.querySelectorAll(".nav").forEach(build);
})();

/* ---- load the per-page animated guide engine (adds the "▶ guide" button) ---- */
(function () {
  if (document.querySelector('script[src*="marle_g_guide.js"]')) return;
  var s = document.createElement("script"); s.src = "marle_g_guide.js"; document.head.appendChild(s);
})();

/* ---- load the universal 🔮 predictor widget (floating button on every page) ---- */
(function () {
  if (document.querySelector('script[src*="marle_g_predictor.js"]')) return;
  var s = document.createElement("script"); s.src = "marle_g_predictor.js"; document.head.appendChild(s);
})();

/* ---- load the universal 📈 candlestick widget (floating chart on every page) ---- */
(function () {
  if (document.querySelector('script[src*="marle_g_candles.js"]')) return;
  var s = document.createElement("script"); s.src = "marle_g_candles.js"; document.head.appendChild(s);
})();

/* ---- load the global 🔔 signal-feed notification system (poll + desktop alerts + toast) ---- */
(function () {
  if (document.querySelector('script[src*="marle_g_signals.js"]')) return;
  var s = document.createElement("script"); s.src = "marle_g_signals.js"; document.head.appendChild(s);
})();

/* ---- load the reusable trade box (cockpit control bar, droppable; redirects to cockpit) ---- */
(function () {
  if (document.querySelector('script[src*="marle_g_tradebox.js"]')) return;
  var s = document.createElement("script"); s.src = "marle_g_tradebox.js"; document.head.appendChild(s);
})();

/* ---- load the OMNIPRESENT 🤖 Assistant (PA danger+prospect brief on every page) ---- */
(function () {
  if (document.querySelector('script[src*="marle_g_assistant.js"]')) return;
  var s = document.createElement("script"); s.src = "marle_g_assistant.js"; document.head.appendChild(s);
})();

/* ---- ⏱ MARKET-LAG timer (top-right pill): how far behind market are we? Chart price ~live (3s); scans cached mins ---- */
(function () {
  if (/[?&]embed=1/.test(location.search)) return;
  if (document.getElementById("mg-heartbeat")) return;
  var b = document.createElement("div"); b.id = "mg-heartbeat";
  b.style.cssText = "position:fixed;top:7px;right:12px;z-index:99990;font:700 10px ui-monospace,Consolas,monospace;" +
    "background:rgba(14,17,22,.94);border:1px solid rgba(255,255,255,.12);border-radius:7px;padding:4px 8px;color:#646c7a;" +
    "cursor:help;backdrop-filter:blur(6px)";
  b.title = "How far behind market. The CHART price is ~live (polls every 3s + Groww's own lag). The SCANS (gated / movers / " +
    "console) are cached and refresh every few minutes — that's why a LIVE breakout shows on the chart first and the scan " +
    "catches up on its next cycle. Green = scans fresh (<3m) · amber = a few min behind · red = stale / feed error.";
  b.textContent = "⏱ …";
  document.body.appendChild(b);
  function poll() {
    if (document.visibilityState !== "visible") return;
    var t0 = (window.performance || Date).now();
    fetch("/api/heartbeat").then(function (r) { return r.json(); }).then(function (d) {
      var rtt = Math.round((window.performance || Date).now() - t0);
      var sc = d.scans || {}, ages = Object.keys(sc).map(function (k) { return sc[k]; }).filter(function (x) { return x != null; });
      var worst = ages.length ? Math.max.apply(null, ages) : 0;
      var open = d.market === "OPEN";
      var col = !open ? "#646c7a" : worst <= 3 ? "#22c55e" : worst <= 15 ? "#fbbf24" : "#ef4444";
      b.style.color = col; b.style.borderColor = col + "55";
      b.innerHTML = open
        ? "⏱ scan " + (worst < 60 ? worst + "m" : "stale") + " · net " + rtt + "ms"
        : "⏱ " + d.market + " · " + (d.ist || "");
    }).catch(function () { b.style.color = "#ef4444"; b.style.borderColor = "#ef444455"; b.textContent = "⏱ no feed"; });
  }
  poll(); setInterval(poll, 5000);
})();

/* ---- deep-link: ?sym=XYZ → prefill this page's search (#q) + load it, so the global search can route here ---- */
(function () {
  var p; try { p = new URLSearchParams(location.search).get("sym"); } catch (e) {}
  if (!p) { try { p = localStorage.getItem("mg_last_sym"); } catch (e) {} }   // FLUID default: your last-looked-at symbol, NOT a page's hardcoded one
  if (!p) return; p = p.toUpperCase();
  function go() {
    var q = document.getElementById("q");
    if (q) { q.value = p; q.dispatchEvent(new Event("input", { bubbles: true })); }
    if (typeof window.loadTicket === "function") { try { window.loadTicket(p); return; } catch (e) {} }
    if (typeof window.run === "function") { try { window.run(); return; } catch (e) {} }
    if (q) ["keydown", "keyup"].forEach(function (t) { q.dispatchEvent(new KeyboardEvent(t, { key: "Enter", keyCode: 13, which: 13, bubbles: true })); });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", function () { setTimeout(go, 450); });
  else setTimeout(go, 450);
})();

/* ---- keep the last symbol FLUID across pages: persist #q on Enter so the next page opens on it ---- */
(function () {
  document.addEventListener("keydown", function (e) {
    if (e.key !== "Enter") return;
    var t = e.target;
    if (t && t.tagName === "INPUT" && (t.id === "q" || t.id === "und" || t.id === "trigtk")) {
      var v = (t.value || "").trim().toUpperCase(); if (v) { try { localStorage.setItem("mg_last_sym", v); } catch (_e) {} }
    }
  }, true);
})();

/* ---- back/forward nav buttons + market-regime badge (every pod; vital in the chromeless app window) ---- */
(function () {
  function navBtn(txt, title, fn) {
    var b = document.createElement("button");
    b.type = "button"; b.textContent = txt; b.title = title;
    b.style.cssText = "font-family:var(--mono,ui-monospace,monospace);font-size:14px;font-weight:700;color:var(--dim,#646c7a);" +
      "background:rgba(255,255,255,.03);border:1px solid var(--border,rgba(255,255,255,.08));border-radius:8px;width:30px;height:28px;" +
      "cursor:pointer;flex:0 0 auto;line-height:1;display:inline-flex;align-items:center;justify-content:center";
    b.addEventListener("mouseenter", function () { b.style.color = "var(--saffron,#ff9933)"; b.style.borderColor = "var(--saffron,#ff9933)"; });
    b.addEventListener("mouseleave", function () { b.style.color = "var(--dim,#646c7a)"; b.style.borderColor = "var(--border,rgba(255,255,255,.08))"; });
    b.addEventListener("click", fn);
    return b;
  }
  document.querySelectorAll(".nav").forEach(function (nav) {
    if (!nav.querySelector(".mg-navbtns")) {
      var wrap = document.createElement("span");
      wrap.className = "mg-navbtns";
      wrap.style.cssText = "display:inline-flex;gap:6px;margin-left:12px;align-items:center;flex:0 0 auto";
      wrap.appendChild(navBtn("←", "Back (previous page)", function () { history.back(); }));
      wrap.appendChild(navBtn("→", "Forward", function () { history.forward(); }));
      var brand = nav.querySelector(".brand");
      if (brand && brand.parentNode === nav) brand.insertAdjacentElement("afterend", wrap);
      else nav.insertBefore(wrap, nav.firstChild);
    }
    if (!nav.querySelector(".mg-regime")) {
      var rb = document.createElement("span");
      rb.className = "mg-regime";
      rb.style.cssText = "font-family:var(--mono,ui-monospace,monospace);font-size:9.5px;font-weight:800;letter-spacing:.05em;" +
        "padding:4px 9px;border-radius:7px;margin-left:10px;flex:0 0 auto;color:var(--dim,#646c7a);border:1px solid var(--border,rgba(255,255,255,.08))";
      rb.textContent = "REGIME —";
      nav.appendChild(rb);
    }
  });
  function ensureBanner() {
    var b = document.getElementById("mg-macrobanner");
    if (b) return b;
    b = document.createElement("div"); b.id = "mg-macrobanner";
    b.style.cssText = "display:none;width:100%;text-align:center;font-family:var(--mono,ui-monospace,monospace);" +
      "font-size:12px;font-weight:800;letter-spacing:.02em;padding:9px 16px;line-height:1.4;position:sticky;top:0;z-index:60";
    var nav = document.querySelector(".nav");
    if (nav && nav.parentNode) nav.parentNode.insertBefore(b, nav.nextSibling);
    else document.body.insertBefore(b, document.body.firstChild);
    return b;
  }
  function fillRegime() {
    // /api/shock = the macro gate: bull/bear + the fast shock override (VIX/NIFTH/breadth/correlation)
    fetch("/api/shock").then(function (r) { return r.json(); }).then(function (d) {
      var map = { SHOCK: ["⚠ MACRO SHOCK", "#ef4444"], STRESS: ["🟠 MACRO STRESS", "#fbbf24"],
                  BULL: ["🟢 BULL · deploy", "#22c55e"], BEAR: ["🔴 BEAR · cash", "#ef4444"] };
      var m = map[d.badge] || ["REGIME —", "var(--dim,#646c7a)"];
      var extra = d.vix != null ? " · VIX " + d.vix : (d.breadth != null ? " · " + d.breadth + "%" : "");
      document.querySelectorAll(".mg-regime").forEach(function (el) {
        el.textContent = m[0] + extra; el.style.color = m[1]; el.style.borderColor = m[1];
        el.title = (d.verdict || "") + ((d.reasons && d.reasons.length) ? " — " + d.reasons.join(" · ") : "");
      });
      var b = ensureBanner();
      if (d.state === "SHOCK" || d.state === "ELEVATED") {
        var shock = d.state === "SHOCK";
        b.style.background = shock ? "rgba(239,68,68,.96)" : "rgba(251,191,36,.94)";
        b.style.color = shock ? "#fff" : "#180f02";
        b.innerHTML = (shock ? "⚠ MACRO SHOCK — " : "🟠 MACRO STRESS — ") + (d.verdict || "") +
          ((d.reasons && d.reasons.length) ? "<span style='opacity:.85'>  ·  " + d.reasons.join("  ·  ") + "</span>" : "");
        b.style.display = "block";
      } else {
        b.style.display = "none";
      }
    }).catch(function () {});
  }
  fillRegime();
  setInterval(fillRegime, 180000);
})();

/* ---- macro OVERLAY: the "weather" strip pinned at the BOTTOM of every pod page ---- */
(function () {
  if (document.getElementById("mg-macrobar")) return;
  var bar = document.createElement("div"); bar.id = "mg-macrobar";
  bar.style.cssText = "position:fixed;left:0;right:0;bottom:0;z-index:90;background:rgba(14,17,22,.96);backdrop-filter:blur(8px);" +
    "border-top:1px solid rgba(255,255,255,.1);font-family:var(--mono,ui-monospace,Consolas,monospace);font-size:11px;color:#9aa3b4;" +
    "display:flex;align-items:center;gap:8px;padding:5px 12px;overflow-x:auto;white-space:nowrap";
  document.body.appendChild(bar);
  document.body.style.paddingBottom = "34px";
  var DOT = { on: "#22c55e", off: "#ef4444", neutral: "#646c7a" };
  var WC = { "RISK-ON": "#22c55e", "RISK-OFF": "#ef4444", "MIXED": "#fbbf24" };
  function render(d) {
    if (!d || !d.ok) { bar.innerHTML = '<span style="color:#646c7a">macro overlay —</span>'; return; }
    var wc = WC[d.weather] || "#9aa3b4";
    var h = '<span style="font-weight:800;letter-spacing:.05em;color:#646c7a">MACRO</span>' +
      '<span style="font-weight:800;color:' + wc + ';border:1px solid ' + wc + '55;border-radius:6px;padding:2px 8px">' + d.weather + "</span>";
    if (d.fragility) {
      var f = d.fragility;
      var fc = (f.regime === "STRESS" || f.regime === "SHOCK") ? "#ef4444" : f.regime === "FRAGILE" ? "#fbbf24" : f.regime === "RECOVERING" ? "#22c55e" : "#646c7a";
      h += '<span title="' + String(f.advice || "").replace(/"/g, "'") + '" style="font-weight:800;color:' + fc +
           ';border:1px solid ' + fc + '55;border-radius:6px;padding:2px 8px;cursor:help">🔥 ' + f.regime + " " + f.score + "</span>";
    }
    h += (d.themes || []).map(function (t) {
      var c = DOT[t.direction] || "#646c7a";
      var tip = "[" + t.significance + "] " + t.why + " — " + String(t.read || "").replace(/"/g, "'") + " (" + (t.source || "") + ")";
      return '<span title="' + tip + '" style="display:inline-flex;align-items:center;gap:4px;cursor:help;padding:1px 3px">' +
        '<span style="width:7px;height:7px;border-radius:50%;background:' + c + ';display:inline-block"></span>' +
        t.icon + " " + t.name + (t.significance === "HIGH" ? "" : ' <span style="color:#52514e">·' + t.significance.toLowerCase() + "</span>") + "</span>";
    }).join('<span style="color:#2c2c2a">|</span>');
    h += '<span style="margin-left:auto;color:#52514e;flex:0 0 auto">' + (d.asof || "") + " · significance=prior · direction=live · hover for why</span>";
    bar.innerHTML = h;
  }
  function load() { fetch("/api/macro_overlay").then(function (r) { return r.json(); }).then(render).catch(function () {}); }
  load(); setInterval(load, 1200000);
})();
