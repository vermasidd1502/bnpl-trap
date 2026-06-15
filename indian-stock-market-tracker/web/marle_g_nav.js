/* Marle-G — shared top nav. Single source of truth for every pod page.
   Each page just includes <script src="marle_g_nav.js"></script>; this fills its .tabs bar
   with grouped DROPDOWN menus of the complete pod list (home · markets · strategies ·
   scenarios · portfolio · tools) and highlights both the active group and the active page.
   The full list lives here once, so every pod gets the identical toolbar for free. */
(function () {
  var GROUPS = [
    { label: "home", solo: true, items: [["marle_g_pod.html", "home"]] },
    { label: "markets", icon: "📈", items: [
      ["marle_g_equity.html", "equity"], ["marle_g_buyhold.html", "buy & hold"], ["marle_g_intraday.html", "intraday"],
      ["marle_g_patterns.html", "patterns"], ["marle_g_volume.html", "volume"], ["marle_g_industry.html", "rotation"],
      ["marle_g_live.html", "live"], ["marle_g_macro_preview.html", "macro"]] },
    { label: "strategies", icon: "🎯", items: [
      ["marle_g_strategies.html", "strategies"], ["marle_g_weekend.html", "weekend"], ["marle_g_builder.html", "builder"],
      ["marle_g_volbook.html", "day-book"], ["marle_g_autotrader.html", "auto-trader"], ["marle_g_quality.html", "quality"],
      ["marle_g_robust.html", "robust"], ["marle_g_niftysim.html", "nifty-sim"], ["marle_g_sectoral.html", "sectoral"],
      ["marle_g_vol.html", "vol-lab"], ["marle_g_paper.html", "paper"]] },
    { label: "scenarios", icon: "🌐", items: [
      ["marle_g_cascade.html", "cascade"], ["marle_g_regime.html", "regime"], ["marle_g_thesis.html", "thesis"],
      ["marle_g_smartmoney.html", "smart-money"], ["marle_g_mindhive.html", "mindhive"]] },
    { label: "portfolio", icon: "💼", items: [
      ["marle_g_portfolio.html", "portfolio"], ["marle_g_winners.html", "winners"], ["marle_g_risk.html", "risk"],
      ["marle_g_profile.html", "profile"], ["marle_g_dashboard.html", "dashboard"], ["marle_g_funds.html", "funds"],
      ["marle_g_options.html", "options"]] },
    { label: "tools", icon: "🔧", items: [
      ["marle_g_chart.html", "chart"], ["marle_g_canslim.html", "canslim"], ["marle_g_architecture_3d.html", "3D"],
      ["marle_g_diag.html", "diag"]] }
  ];
  var cur = (location.pathname.split("/").pop() || "marle_g_pod.html").toLowerCase();

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
      var menu = g.items.map(function (p) {
        return '<a href="' + p[0] + '"' + (p[0].toLowerCase() === cur ? ' class="on"' : "") + ">" + p[1] + "</a>";
      }).join("");
      return '<div class="mg-grp"><button type="button" class="mg-trig' + (has ? " on" : "") + '">' +
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
      trig.addEventListener("click", function (e) {
        e.stopPropagation();
        if (g.classList.contains("open")) shut(); else open();
      });
    });
  }

  Array.prototype.forEach.call(document.querySelectorAll(".tabs"), function (t) { t.innerHTML = build(); wire(t); });
  document.addEventListener("click", function () { closeAll(null); });
  document.addEventListener("keydown", function (e) { if (e.key === "Escape") closeAll(null); });
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
