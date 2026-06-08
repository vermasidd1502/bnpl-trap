/* Marle-G shared stock-fundamentals popup.
   Include on any page (except the equity monitor) with:
     <script src="marleg_stockmodal.js"></script>
   then call  openStock('RELIANCE')  on a click. Self-initialises (injects CSS + DOM). */
(function () {
  if (window.__mgModal) return;
  window.__mgModal = true;

  const css = `
  .mgm-ov{position:fixed;inset:0;background:rgba(4,6,10,.72);backdrop-filter:blur(4px);z-index:9999;display:none;align-items:flex-start;justify-content:center;padding:40px 16px;overflow:auto}
  .mgm-ov.show{display:flex}
  .mgm{background:#14171e;border:1px solid rgba(255,255,255,.10);border-radius:16px;max-width:580px;width:100%;box-shadow:0 24px 80px rgba(0,0,0,.6);font-family:Inter,system-ui,sans-serif;color:#eef1f6}
  .mgm-h{display:flex;align-items:flex-start;gap:12px;padding:18px 20px;border-bottom:1px solid rgba(255,255,255,.08)}
  .mgm-h .sym{font-family:var(--mono,monospace);font-size:19px;font-weight:800}
  .mgm-h .nm{font-size:12px;color:#9aa3b4;margin-top:2px}
  .mgm-h .px{margin-left:auto;text-align:right;font-family:var(--mono,monospace)}
  .mgm-h .px .p{font-size:18px;font-weight:800}
  .mgm-x{cursor:pointer;color:#646c7a;font-size:20px;line-height:1;padding:2px 4px}.mgm-x:hover{color:#eef1f6}
  .mgm-b{padding:16px 20px}
  .mgm-badge{display:inline-block;font-family:var(--mono,monospace);font-size:11px;font-weight:800;padding:5px 10px;border-radius:7px;letter-spacing:.04em}
  .mgm-cards{display:flex;gap:9px;flex-wrap:wrap;margin:14px 0}
  .mgm-card{flex:1;min-width:150px;background:#0e1116;border:1px solid rgba(255,255,255,.08);border-radius:10px;padding:11px}
  .mgm-card .l{font-family:var(--mono,monospace);font-size:9px;color:#646c7a;text-transform:uppercase;letter-spacing:.05em}
  .mgm-card .v{font-family:var(--mono,monospace);font-size:17px;font-weight:800;margin-top:4px}
  .mgm-card .s{font-family:var(--mono,monospace);font-size:10px;color:#9aa3b4;margin-top:3px}
  .mgm-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(104px,1fr));gap:7px;margin:6px 0 4px}
  .mgm-g{background:#0e1116;border-radius:8px;padding:7px 9px;font-family:var(--mono,monospace)}
  .mgm-g .k{font-size:8.5px;color:#646c7a;text-transform:uppercase}.mgm-g .vv{font-size:13px;font-weight:700;margin-top:2px}
  .mgm-narr h4{font-family:var(--mono,monospace);font-size:10px;letter-spacing:.05em;text-transform:uppercase;color:#ff9933;font-weight:700;margin:14px 0 4px}
  .mgm-narr p{font-size:12.5px;line-height:1.6;color:#9aa3b4}
  .mgm-f{padding:14px 20px;border-top:1px solid rgba(255,255,255,.08);display:flex;justify-content:space-between;align-items:center}
  .mgm-f a{font-family:var(--mono,monospace);font-size:12px;color:#ff9933;text-decoration:none;font-weight:700}
  .mgm .pos{color:#22c55e}.mgm .neg{color:#ef4444}.mgm .neu{color:#fbbf24}
  .mgm-vp{background:#0e1116;border:1px solid rgba(255,255,255,.08);border-radius:11px;padding:13px 14px;margin-bottom:14px}
  .mgm-vp-h{font-family:var(--mono,monospace);font-size:11px;letter-spacing:.04em;color:#9aa3b4;display:flex;align-items:center;gap:8px}
  .mgm-vp-h b{font-size:14px}
  .mgm-vp-q{margin-left:auto;font-size:10px;font-weight:800;padding:3px 8px;border-radius:6px}
  .mgm-track{position:relative;height:10px;background:linear-gradient(90deg,rgba(239,68,68,.30),rgba(251,191,36,.22),rgba(34,197,94,.30));border-radius:6px;margin:15px 0 4px}
  .mgm-mk{position:absolute;top:-3px;width:1px;height:16px;background:rgba(255,255,255,.28)}
  .mgm-mk.g{background:#ff9933;width:2px}
  .mgm-dot{position:absolute;top:50%;width:13px;height:13px;border-radius:50%;transform:translate(-50%,-50%);border:2px solid #14171e;box-shadow:0 0 0 1px rgba(255,255,255,.35)}
  .mgm-scale{display:flex;justify-content:space-between;font-family:var(--mono,monospace);font-size:9px;color:#646c7a}
  .mgm-vp-meta{font-family:var(--mono,monospace);font-size:10.5px;color:#9aa3b4;margin-top:9px}
  .mgm-vp-note{font-size:12px;line-height:1.5;color:#cbd5e1;margin-top:8px;padding-left:10px}`;
  const st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);
  const ov = document.createElement('div'); ov.className = 'mgm-ov'; ov.id = 'mgm-ov';
  ov.innerHTML = '<div class="mgm" id="mgm" onclick="event.stopPropagation()"></div>';
  ov.addEventListener('click', function () { window.closeStock(); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') window.closeStock(); });
  document.addEventListener('DOMContentLoaded', () => document.body.appendChild(ov));
  if (document.body) document.body.appendChild(ov);

  const inr = n => '₹' + (Number(n) || 0).toLocaleString('en-IN');
  const pct = x => (x > 0 ? '+' : '') + (Number(x)).toFixed(1) + '%';
  const cls = x => x > 0 ? 'pos' : x < 0 ? 'neg' : 'neu';

  window.closeStock = function () { document.getElementById('mgm-ov').classList.remove('show'); };

  const vpColor = b => (b === 'long' || b === 'long-trail' || b === 'watch') ? '#22c55e'
    : (b === 'wait' || b === 'trim') ? '#fbbf24' : '#ef4444';
  function renderVP(vp) {
    if (!vp || vp.error) return '';
    const col = vpColor(vp.bias), pos = Math.max(2, Math.min(100, vp.ud / 3 * 100));
    const mk = v => `<span class="mgm-mk${Math.abs(v - 1.618) < 0.01 ? ' g' : ''}" style="left:${Math.min(100, v / 3 * 100)}%"></span>`;
    return `<div class="mgm-vp">
      <div class="mgm-vp-h">VOLUME POSITION · U/D <b style="color:${col}">${vp.ud}</b>
        <span class="mgm-vp-q" style="background:${col}26;color:${col}">${vp.quadrant}</span></div>
      ${vp.control_txt ? `<div style="font-size:13px;font-weight:800;margin:7px 0 2px;color:${vp.control === 'bulls' ? '#22c55e' : '#ef4444'}">${vp.control_txt}</div>` : ''}
      <div class="mgm-track">${mk(0.618)}${mk(1.0)}${mk(1.618)}${mk(2.618)}<span class="mgm-dot" style="left:${pos}%;background:${col}"></span></div>
      <div class="mgm-scale"><span>0.6</span><span>1.0</span><span>φ 1.62</span><span>φ² 2.62</span><span>3+</span></div>
      <div class="mgm-vp-meta">${vp.level} · ${vp.direction} · <b>${vp.percentile}th pctile</b> (1-yr) · vs 50d <b>${vp.ud_sma50 ?? '—'}</b> / 250d <b>${vp.ud_sma250 ?? '—'}</b>${vp.ceiling ? ' · <b style="color:#ef4444">AT CEILING</b>' : ''}</div>
      <div class="mgm-vp-note" style="border-left:3px solid ${col}">${vp.note}</div>
    </div>`;
  }
  window.openStock = async function (sym) {
    const ovl = document.getElementById('mgm-ov'), box = document.getElementById('mgm');
    ovl.classList.add('show');
    box.innerHTML = `<div class="mgm-b" style="padding:40px;text-align:center;color:#9aa3b4;font-family:var(--mono,monospace)">⟳ loading ${sym}…</div>`;
    let f = {}, vp = null;
    [f, vp] = await Promise.all([
      fetch('/api/fundamentals/' + encodeURIComponent(sym)).then(r => r.json()).catch(() => ({ error: 'offline' })),
      fetch('/api/volume_position/' + encodeURIComponent(sym)).then(r => r.json()).catch(() => null)
    ]);
    const hasF = f && !f.error;
    const vc = f.verdict === 'UNDERVALUED' ? 'pos' : f.verdict === 'OVERVALUED' ? 'neg' : 'neu';
    const vbg = f.verdict === 'UNDERVALUED' ? 'rgba(34,197,94,.15)' : f.verdict === 'OVERVALUED' ? 'rgba(239,68,68,.15)' : 'rgba(251,191,36,.15)';
    const health = (f && f.health) || {}, narr = (f && f.narrative) || [];
    const fundHtml = hasF ? `
        ${f.verdict ? `<span class="mgm-badge ${vc}" style="background:${vbg}">${f.verdict}${f.gap != null ? ' · ' + pct(f.gap) + ' vs intrinsic' : ''}</span>` : ''}
        <div class="mgm-cards">
          <div class="mgm-card"><div class="l">Fair value</div><div class="v">${f.fair != null ? inr(f.fair) : 'n/a'}</div><div class="s">Graham ${f.graham ?? '—'} · DCF ${f.dcf ?? '—'}</div></div>
          <div class="mgm-card"><div class="l">1-yr target</div><div class="v ${f.upside > 0 ? 'pos' : 'neg'}">${f.target != null ? inr(f.target) : 'n/a'}</div><div class="s">${f.upside != null ? pct(f.upside) : ''} · ${f.n_analysts || '?'} analysts${f.rec ? ' · ' + String(f.rec).replace('_', ' ') : ''}</div></div>
          <div class="mgm-card"><div class="l">Quality</div><div class="v">${f.qscore ?? '—'}/100</div><div class="s">${f.piotroski ? 'Piotroski ' + f.piotroski.score + '/' + f.piotroski.of : ''}</div></div>
        </div>
        <div class="mgm-grid">${Object.entries(health).map(([k, v]) => `<div class="mgm-g"><div class="k">${k}</div><div class="vv">${v == null ? '—' : (typeof v === 'number' ? Math.round(v * 100) / 100 : v)}</div></div>`).join('')}</div>
        <div class="mgm-narr">${narr.map(s => `<h4>${s.h}</h4><p>${s.p}</p>`).join('')}</div>`
      : `<div style="color:#9aa3b4;font-size:12.5px;margin-top:6px">Fundamentals unavailable for this name.</div>`;
    box.innerHTML = `
      <div class="mgm-h">
        <div><div class="sym">${sym}</div><div class="nm">${(f && f.name) || ''}${f && f.sector ? ' · ' + f.sector : ''}${f && f.industry ? ' / ' + f.industry : ''}</div></div>
        ${f && f.price ? `<div class="px"><div class="p">${inr(f.price)}</div></div>` : ''}
        <div class="mgm-x" onclick="window.closeStock()">✕</div>
      </div>
      <div class="mgm-b">${renderVP(vp)}${fundHtml}</div>
      <div class="mgm-f"><a href="marle_g_chart.html?q=${sym}" style="font-family:var(--mono,monospace);font-size:12px;color:#ff9933;text-decoration:none;font-weight:700">📈 Interactive chart</a><a href="marle_g_equity.html?q=${sym}">Open full analysis →</a></div>`;
  };
})();
