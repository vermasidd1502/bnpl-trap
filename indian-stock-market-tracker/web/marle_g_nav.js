/* Marle-G — shared top nav. Single source of truth for every pod page.
   Each page just includes <script src="marle_g_nav.js"></script>; this fills its .tabs bar
   with the complete, grouped pod list and highlights the current page. */
(function () {
  var GROUPS = [
    [["marle_g_pod.html", "home"]],
    [["marle_g_equity.html", "equity"], ["marle_g_intraday.html", "intraday"],
     ["marle_g_volume.html", "volume"], ["marle_g_live.html", "live"], ["marle_g_macro_preview.html", "macro"]],
    [["marle_g_strategies.html", "strategies"], ["marle_g_weekend.html", "weekend"], ["marle_g_builder.html", "builder"], ["marle_g_volbook.html", "day-book"], ["marle_g_autotrader.html", "auto-trader"],
     ["marle_g_quality.html", "quality"], ["marle_g_robust.html", "robust"], ["marle_g_niftysim.html", "nifty-sim"],
     ["marle_g_sectoral.html", "sectoral"], ["marle_g_vol.html", "vol-lab"], ["marle_g_paper.html", "paper"]],
    [["marle_g_cascade.html", "cascade"], ["marle_g_regime.html", "regime"],
     ["marle_g_thesis.html", "thesis"], ["marle_g_smartmoney.html", "smart-money"], ["marle_g_mindhive.html", "mindhive"]],
    [["marle_g_portfolio.html", "portfolio"], ["marle_g_winners.html", "winners"], ["marle_g_risk.html", "risk"], ["marle_g_profile.html", "profile"],
     ["marle_g_dashboard.html", "dashboard"], ["marle_g_funds.html", "funds"], ["marle_g_options.html", "options"]],
    [["marle_g_chart.html", "chart"], ["marle_g_canslim.html", "canslim"], ["marle_g_architecture_3d.html", "3D"]]
  ];
  var cur = (location.pathname.split("/").pop() || "marle_g_pod.html").toLowerCase();
  var sep = '<span style="color:var(--dim);opacity:.4;padding:0 3px">|</span>';
  var html = GROUPS.map(function (g) {
    return g.map(function (p) {
      return '<a href="' + p[0] + '"' + (p[0].toLowerCase() === cur ? ' class="on"' : '') + '>' + p[1] + '</a>';
    }).join("");
  }).join(sep);
  document.querySelectorAll(".tabs").forEach(function (t) { t.innerHTML = html; });
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

/* ---- universal live auto-refresh: keeps every pod fresh on a ~60s tick ---- */
(function () {
  var EVERY = 60000;                                  // 60s; pages that self-poll opt out via MG_SELF_REFRESH
  setInterval(function () {
    if (window.MG_SELF_REFRESH) return;               // page already polls its own data faster
    if (document.hidden) return;                      // tab not visible -> don't waste calls
    var a = document.activeElement;
    if (a && (a.tagName === "INPUT" || a.tagName === "TEXTAREA" || a.tagName === "SELECT" || a.isContentEditable)) return;  // user typing
    var sg = document.querySelector(".mg-sg");
    if (sg && sg.style.display === "block") return;   // ticker suggestion dropdown open
    if (typeof window.podRefresh === "function") { try { window.podRefresh(); return; } catch (e) {} }
    location.reload();                                // fallback: soft full reload (data is server-cached, so cheap)
  }, EVERY);
})();
