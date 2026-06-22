/* marle_g_events.js — pod-wide EVENT BUBBLES overlay for any lightweight-charts candle series.
   ONE global ON/OFF toggle (persisted in this browser, shared across every page).

   Drop-in on a chart page:
       MGEvents.attach(series, "RELIANCE");                 // fetches /api/events/<sym>
       MGEvents.attach(series, "RELIANCE", {events: ev});   // reuse already-fetched events
   Then keep calling  series.setMarkers(yourMarkers)  exactly as before — this layer transparently
   merges the event bubbles in (and re-paints when you change markers OR flip the toggle). The time
   axis (string-date 'YYYY-MM-DD' vs numeric epoch) is auto-detected from your own markers, so the
   same call works on both daily-string charts and the numeric patterns chart.

   Read-only / decision-support. Bubbles: earnings ⊕, dividends $, splits ⤬, gaps ⇅,
   52-week highs/lows ▲▼, volume spikes ≣, F&O expiries ⌛. */
(function () {
  if (window.MGEvents) return;
  var KEY = "mg_events_on";
  var REG = [];                                            // attached chart entries
  function ON() { return localStorage.getItem(KEY) !== "0"; }   // default ON
  function setON(v) { localStorage.setItem(KEY, v ? "1" : "0"); }

  function find(series) { for (var i = 0; i < REG.length; i++) if (REG[i].s === series) return REG[i]; return null; }

  // event payload -> raw {date:'YYYY-MM-DD', shape,color,position,text}
  function rawEvents(d) {
    return (((d && d.events) || []).map(function (e) {
      return { date: e.time, shape: e.shape || "circle", color: e.color || "#888",
               position: e.position || "aboveBar", text: e.text || "" };
    }));
  }
  // encode a raw event's date for this entry's detected axis mode
  function enc(date, mode) {
    if (mode === "num") { var p = ("" + date).split("-"); return Date.UTC(+p[0], (+p[1]) - 1, +p[2]) / 1000; }
    return date;                                           // string-date axis: use as-is
  }
  function paint(e) {
    var base = e.base || [];
    var ev = ON() ? e.ev.map(function (r) {
      return { time: enc(r.date, e.mode), position: r.position, color: r.color, shape: r.shape, text: r.text };
    }) : [];
    var all = base.concat(ev).sort(function (a, b) { return a.time < b.time ? -1 : (a.time > b.time ? 1 : 0); });
    try { e.raw(all); } catch (_) {}
  }
  function attach(series, symbol, opts) {
    opts = opts || {};
    symbol = (symbol || "").toString().toUpperCase();
    var e = find(series);
    if (!e) {
      e = { s: series, base: [], ev: [], sym: null, mode: "str", raw: series.setMarkers.bind(series) };
      REG.push(e);
      // monkey-patch: every future setMarkers feeds the "base" layer + auto-detects the axis mode
      series.setMarkers = function (mk) {
        e.base = mk || [];
        if (e.base.length) e.mode = (typeof e.base[0].time === "number") ? "num" : "str";
        paint(e);
      };
    }
    if (opts.events) { e.ev = rawEvents(opts.events); }
    if (symbol && symbol !== e.sym) {
      e.sym = symbol;
      if (!opts.events) {
        fetch("/api/events/" + encodeURIComponent(symbol)).then(function (r) { return r.json(); })
          .then(function (d) { if (e.sym === symbol) { e.ev = rawEvents(d); paint(e); } })
          .catch(function () {});
      }
    }
    paint(e);
    syncChip();
    return e;
  }
  function toggle() { setON(!ON()); REG.forEach(paint); syncChip(); }

  // ---------- the one global toggle chip ----------
  var CHIP = null;
  function syncChip() {
    if (!CHIP) return;
    var on = ON();
    CHIP.style.borderColor = on ? "rgba(34,197,94,.55)" : "rgba(255,255,255,.14)";
    CHIP.style.color = on ? "#22c55e" : "#8a93a6";
    CHIP.innerHTML = "🫧 events <b>" + (on ? "ON" : "OFF") + "</b>";
  }
  function buildChip() {
    if (CHIP || !document.body) return;
    CHIP = document.createElement("button");
    CHIP.id = "mg-events-toggle";
    CHIP.title = "Show/hide event bubbles on every chart — earnings, dividends, gaps, 52-week highs/lows, "
               + "volume spikes, F&O expiries. One switch, all charts.";
    CHIP.style.cssText = "position:fixed;top:56px;right:14px;z-index:99994;background:#14171e;"
      + "border:1px solid rgba(255,255,255,.14);border-radius:999px;padding:5px 12px;"
      + "font-family:'SF Mono',ui-monospace,Consolas,monospace;font-size:11px;cursor:pointer;"
      + "box-shadow:0 6px 18px rgba(0,0,0,.4);transition:border-color .15s,color .15s";
    CHIP.onclick = toggle;
    document.body.appendChild(CHIP);
    syncChip();
  }
  if (document.readyState !== "loading") buildChip();
  else document.addEventListener("DOMContentLoaded", buildChip);

  window.MGEvents = { attach: attach, toggle: toggle, on: ON };
})();
