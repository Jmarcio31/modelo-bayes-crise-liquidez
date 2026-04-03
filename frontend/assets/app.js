(function(){
  const state = { latest:null, history:[], modelConfig:{signals:[]}, activeTab:'dashboard', signalViewMode:'log' };

  function byId(id){ return document.getElementById(id); }
  async function fetchJson(path){
    const res = await fetch(path, { cache:'no-store' });
    if(!res.ok) throw new Error('Falha ao carregar ' + path);
    return res.json();
  }
  function pct(v,d=1){ return ((Number(v||0))*100).toFixed(d) + '%'; }
  function num(v,d=2){ return Number(v||0).toFixed(d); }
  function signalClass(status){ return status === 'ATIVO' ? 'ativo' : (status === 'CONTRARIO' ? 'contrario' : 'neutro'); }
  function sourceBadgeClass(type){
    const t = String(type||'').toLowerCase();
    if(t.includes('direct')) return 'blue';
    if(t.includes('proxy')) return 'amber';
    if(t.includes('fallback')) return 'red';
    return 'green';
  }
  function weeklyDelta(){
    if((state.history||[]).length < 2) return '+0.0%';
    const a = Number(state.history[state.history.length-1].posterior || 0);
    const b = Number(state.history[state.history.length-2].posterior || 0);
    const d = a - b; return (d>=0?'+':'-') + (Math.abs(d)*100).toFixed(1) + '%';
  }

  // --- MELHORIA 1: Gauge visual para probabilidade posterior ---
  function buildGaugeSvg(posterior){
    const pct = Math.max(0, Math.min(1, posterior));
    const angle = -135 + pct * 270; // -135deg a +135deg
    const color = pct < 0.20 ? '#067647' : pct < 0.40 ? '#b54708' : pct < 0.60 ? '#b42318' : '#7c0a02';
    return `<svg viewBox="0 0 200 130" xmlns="http://www.w3.org/2000/svg" class="gauge-svg">
      <defs>
        <linearGradient id="g1" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="#067647"/>
          <stop offset="40%" stop-color="#b54708"/>
          <stop offset="70%" stop-color="#b42318"/>
          <stop offset="100%" stop-color="#7c0a02"/>
        </linearGradient>
      </defs>
      <!-- Track -->
      <path d="M 20 110 A 80 80 0 1 1 180 110" fill="none" stroke="#e2e8f0" stroke-width="14" stroke-linecap="round"/>
      <!-- Colored arc -->
      <path d="M 20 110 A 80 80 0 1 1 180 110" fill="none" stroke="url(#g1)" stroke-width="14" stroke-linecap="round" opacity="0.25"/>
      <!-- Active arc (clipped to posterior value) -->
      <path d="M 20 110 A 80 80 0 1 1 180 110" fill="none" stroke="${color}" stroke-width="14" stroke-linecap="round"
        stroke-dasharray="${pct * 251.2} 251.2"/>
      <!-- Zone labels -->
      <text x="14" y="128" font-size="8" fill="#067647" font-weight="700">0%</text>
      <text x="170" y="128" font-size="8" fill="#7c0a02" font-weight="700">100%</text>
      <!-- Needle -->
      <g transform="translate(100,110) rotate(${angle})">
        <line x1="0" y1="0" x2="0" y2="-62" stroke="${color}" stroke-width="2.5" stroke-linecap="round"/>
        <circle cx="0" cy="0" r="5" fill="${color}"/>
      </g>
      <!-- Center value -->
      <text x="100" y="98" text-anchor="middle" font-size="22" font-weight="800" fill="${color}">${(pct*100).toFixed(1)}%</text>
    </svg>`;
  }

  // --- MELHORIA 5: Indicador de defasagem ---
  function stalenessIcon(asOfDate, runDate){
    if(!asOfDate || !runDate) return '';
    const diff = (new Date(runDate) - new Date(asOfDate)) / (1000*60*60*24);
    if(diff > 3){
      return `<span class="staleness-icon" title="Dado defasado: ${asOfDate} (${Math.round(diff)} dias atrás)">⏱</span>`;
    }
    return '';
  }

  function header(){
    const latest = state.latest || {};
    const subtitle = byId('subtitle');
    const runMeta = byId('run-meta');
    if(subtitle){
      subtitle.innerHTML = 'Painel estático consumindo <span class="code">frontend/data/latest.json</span>, <span class="code">frontend/data/history.json</span> e <span class="code">backend/app/model_config.json</span>.';
    }
    if(runMeta){
      runMeta.innerHTML = [
        '<div class="meta-kv"><span class="small">EXECUÇÃO</span><strong>' + (latest.run_date||'-') + '</strong></div>',
        '<div class="meta-kv"><span class="small">CLASSIFICAÇÃO</span><strong style="font-size:22px">' + (latest.risk_label||'-') + '</strong></div>',
        '<div class="meta-kv"><span class="small">ÚLTIMO DELTA SEMANAL</span><strong style="font-size:22px">' + weeklyDelta() + '</strong></div>'
      ].join('');
    }
  }
  function blockSummary(){j
    const out = {};
    (state.latest?.signals || []).forEach(s=>{
      const key = s.block || 'Sem bloco';
      if(!out[key]) out[key] = {score:0, ativos:0, contrarios:0, total:0};
      out[key].score += Number(s.log_contrib || 0);
      out[key].total += 1;
      if(s.status === 'ATIVO') out[key].ativos += 1;
      if(s.status === 'CONTRARIO') out[key].contrarios += 1;
    });
    return out;
  }
  function panelDashboard(){
    const latest = state.latest || {signals:[], external_block:{}, raw_data:{}, source_status:{}};
    const blocks = blockSummary();
    const positives = (latest.signals||[]).filter(s=>Number(s.log_contrib)>0.0001).sort((a,b)=>Number(b.log_contrib)-Number(a.log_contrib));
    const negatives = (latest.signals||[]).filter(s=>Number(s.log_contrib)<-0.0001).sort((a,b)=>Number(a.log_contrib)-Number(b.log_contrib));
    const blockCards = Object.entries(blocks).map(([name,v])=>{
      const width = Math.min(100, Math.abs(v.score)*50);
      return '<div class="block-summary"><div><strong>' + name + '</strong><div class="small">Ativos ' + v.ativos + ' · Contrários ' + v.contrarios + '</div><div class="progress"><span style="width:' + width + '%"></span></div></div><div style="font-weight:800;color:' + (v.score>=0?'#b42318':'#067647') + '">' + (v.score>=0?'+':'') + num(v.score) + '</div></div>';
    }).join('');
    const signalsHtml = (latest.signals||[]).map(s=>{
      return '<div class="signal-card ' + signalClass(s.status) + '"><div class="row"><div><div class="signal-name">' + s.signal_name + '</div><div class="signal-meta">' + s.block + '</div></div><div style="text-align:right"><div class="pill ' + signalClass(s.status) + '">' + s.status + '</div><div style="margin-top:8px;font-weight:800;color:' + (Number(s.log_contrib)>=0?'#b42318':'#067647') + '">' + (Number(s.log_contrib)>=0?'+':'') + num(s.log_contrib) + '</div></div></div><div class="kv-table"><div>Valor bruto</div><div>' + num(s.raw_value) + '</div><div>Peso</div><div>' + num(s.weight) + '</div><div>LR aplicado</div><div>' + num(s.lr_used) + '</div><div>P(E|H) / P(E|~H)</div><div>' + num(s.p_e_h) + ' / ' + num(s.p_e_not_h) + '</div></div></div>';
    }).join('');

    // MELHORIA 1: Gauge no lugar do KPI simples
    const gaugeHtml = `<div class="card dark gauge-card">
      <div class="small" style="color:#94a3b8;margin-bottom:4px">PROBABILIDADE POSTERIOR</div>
      ${buildGaugeSvg(latest.posterior||0)}
      <div class="kpi-sub" style="text-align:center">Prior ${pct(latest.prior)}</div>
    </div>`;

    return '<section class="tab-panel ' + (state.activeTab==='dashboard'?'active':'') + '" data-panel="dashboard">' +
      '<div class="grid grid-4">' +
      gaugeHtml +
      '<div class="card"><div class="small">SINAIS ATIVOS</div><div class="kpi-value" style="font-size:42px;color:var(--ink)">' + (latest.signals||[]).filter(s=>s.status==='ATIVO').length + '</div><div class="kpi-sub">Contrários: ' + (latest.signals||[]).filter(s=>s.status==='CONTRARIO').length + ' · Neutros: ' + (latest.signals||[]).filter(s=>s.status==='NEUTRO').length + '</div></div>' +
      '<div class="card"><div class="small">BLOCO EXTERNO</div><div class="kpi-value" style="font-size:42px;color:var(--ink)">' + num(latest.external_block?.composite_score) + '</div><div class="kpi-sub">Status: ' + (latest.external_block?.status||'-') + '</div></div>' +
      '<div class="card"><div class="small">FONTE / STATUS</div><div style="font-size:15px;font-weight:700;line-height:1.45">' + Object.entries(latest.source_status||{}).map(([k,v])=>k + ': ' + v).join(' | ') + '</div></div>' +
      '</div>' +
      '<div class="grid grid-2" style="margin-top:18px"><div class="card"><h3>Resumo por bloco</h3><div class="list">' + blockCards + '</div></div><div class="card"><h3>Vetores de risco</h3><div class="grid grid-2"><div><div class="small" style="color:#b42318;font-weight:700;margin-bottom:8px">Pressões altistas</div><div class="list">' + positives.slice(0,5).map(s=>'<div class="row" style="padding:10px;border-radius:12px;background:var(--red-bg)"><span>'+s.signal_name+'</span><strong>+'+num(s.log_contrib)+'</strong></div>').join('') + '</div></div><div><div class="small" style="color:#067647;font-weight:700;margin-bottom:8px">Amortecedores</div><div class="list">' + negatives.slice(0,5).map(s=>'<div class="row" style="padding:10px;border-radius:12px;background:var(--green-bg)"><span>'+s.signal_name+'</span><strong>'+num(s.log_contrib)+'</strong></div>').join('') + '</div></div></div></div></div>' +
      '<div class="grid grid-2" style="margin-top:18px"><div class="card"><h3>Sinais do modelo</h3><div class="list">' + signalsHtml + '</div></div><div class="grid"><div class="card"><h3>Bloco externo</h3><table class="table"><tbody>' + Object.entries(latest.external_block||{}).map(([k,v])=>'<tr><td>'+k+'</td><td><strong>' + (typeof v === 'number' ? num(v) : v) + '</strong></td></tr>').join('') + '</tbody></table></div><div class="card"><h3>Dados brutos</h3><table class="table"><tbody>' + Object.entries(latest.raw_data||{}).map(([k,v])=>'<tr><td>'+k+'</td><td><strong>'+num(v)+'</strong></td></tr>').join('') + '</tbody></table></div></div></div>' +
      '</section>';
  }

  function panelHistorico(){
    // MELHORIA 3: Toggle log/probabilidade no gráfico de contribuição
    const toggleHtml = `<div class="toggle-row">
      <span class="small">Escala:</span>
      <button class="toggle-btn ${state.signalViewMode==='log'?'active':''}" onclick="window.__setSignalMode('log')">Log contribuição</button>
      <button class="toggle-btn ${state.signalViewMode==='prob'?'active':''}" onclick="window.__setSignalMode('prob')">Probabilidade</button>
    </div>`;

    return '<section class="tab-panel ' + (state.activeTab==='historico'?'active':'') + '" data-panel="historico"><div class="grid grid-2"><div class="card"><h3>Evolução da probabilidade</h3><div class="canvas-wrap"><canvas id="historyChart"></canvas></div><table class="table" style="margin-top:12px"><tbody>' + (state.history||[]).slice().reverse().map(h=>'<tr><td>'+(h.run_date||h.date)+'</td><td>'+pct(h.posterior)+'</td><td>'+(h.risk_label||'')+'</td></tr>').join('') + '</tbody></table></div><div class="card"><h3>Contribuição por sinal</h3>' + toggleHtml + '<div class="canvas-wrap"><canvas id="signalChart"></canvas></div></div></div></section>';
  }

  function panelMetodologia(){
    const rows = state.modelConfig?.signals || [];
    return '<section class="tab-panel ' + (state.activeTab==='metodologia'?'active':'') + '" data-panel="metodologia"><div class="notice"><strong>Como ler esta aba</strong>Cada parâmetro mostra o que mede, a que bloco pertence e qual a origem declarada no modelo. O detalhe operacional da coleta fica em <span class="code">source_status</span> e <span class="code">data_feed_meta</span>.</div><div class="list" style="margin-top:18px">' + rows.map(r=>'<div class="card"><div class="row"><div><h3 style="margin-bottom:6px">'+r.signal_name+'</h3><div class="small">'+r.block+'</div></div><div><span class="badge '+sourceBadgeClass(r.source_type)+'">'+r.source_type+'</span></div></div><p style="margin:14px 0 10px">'+(r.description||'')+'</p><div class="kv-table"><div>Fonte declarada</div><div>'+(r.source_label||'')+'</div><div>Raw key</div><div><span class="code">'+r.raw_key+'</span></div><div>Peso</div><div>'+num(r.weight)+'</div><div>P(E|H) / P(E|~H)</div><div>'+num(r.p_e_h)+' / '+num(r.p_e_not_h)+'</div><div>Thresholds</div><div><span class="code">'+JSON.stringify(r.thresholds||{})+'</span></div></div></div>').join('') + '</div></section>';
  }

  // MELHORIA 4: Coluna de data de atualização + MELHORIA 5: ícone de defasagem
  function panelDados(){
    const runDate = state.latest?.run_date || '';
    const meta = state.latest?.data_feed_meta || {};
    const rawRows = Object.entries(state.latest?.raw_data||{}).map(([k,v])=>{
      const feedEntry = meta[k];
      const asOf = feedEntry?.as_of_date || '';
      const icon = asOf ? stalenessIcon(asOf, runDate) : '';
      const asOfCell = asOf
        ? `<td class="as-of-cell">${asOf}${icon}</td>`
        : `<td class="as-of-cell muted">—</td>`;
      return `<tr><td>${k}</td><td><strong>${num(v)}</strong></td>${asOfCell}</tr>`;
    }).join('');
    return '<section class="tab-panel ' + (state.activeTab==='dados'?'active':'') + '" data-panel="dados"><div class="card"><h3>Dados brutos</h3><table class="table"><thead><tr><th>Chave</th><th>Valor</th><th>Atualizado em</th></tr></thead><tbody>' + rawRows + '</tbody></table></div></section>';
  }

  function panelFontes(){
    const meta = state.latest?.data_feed_meta || {};
    return '<section class="tab-panel ' + (state.activeTab==='fontes'?'active':'') + '" data-panel="fontes"><div class="grid grid-2"><div class="card"><h3>Source status</h3><table class="table"><thead><tr><th>Chave</th><th>Status</th></tr></thead><tbody>' + Object.entries(state.latest?.source_status||{}).map(([k,v])=>'<tr><td>'+k+'</td><td><span class="badge '+sourceBadgeClass(v)+'">'+v+'</span></td></tr>').join('') + '</tbody></table></div><div class="card"><h3>data_feed_meta</h3><table class="table"><thead><tr><th>Item</th><th>Detalhe</th></tr></thead><tbody>' + Object.entries(meta).map(([k,v])=>'<tr><td>'+k+'</td><td><div><strong>valor:</strong> '+(typeof v.value === 'number' ? num(v.value) : v.value)+'</div><div><strong>as_of_date:</strong> '+(v.as_of_date||'')+'</div><div><strong>source:</strong> '+(v.source||'')+'</div><div><strong>method:</strong> '+(v.method||'')+'</div><div><strong>quality_flag:</strong> '+(v.quality_flag||'')+'</div></td></tr>').join('') + '</tbody></table></div></div></section>';
  }

  function render(){
    const app = byId('app');
    if(!app || !state.latest) return;
    header();
    app.innerHTML = [panelDashboard(), panelHistorico(), panelMetodologia(), panelDados(), panelFontes()].join('');
    bindTabs();
    drawCharts();
  }

  function bindTabs(){
    document.querySelectorAll('.tab').forEach(btn=>{
      btn.onclick = function(){
        state.activeTab = btn.dataset.tab;
        document.querySelectorAll('.tab').forEach(b=>b.classList.toggle('active', b===btn));
        document.querySelectorAll('.tab-panel').forEach(p=>p.classList.toggle('active', p.dataset.panel===state.activeTab));
        drawCharts();
      };
    });
  }

  // Toggle global acessível pelo onclick inline
  window.__setSignalMode = function(mode){
    state.signalViewMode = mode;
    render();
  };

  let historyChart = null, signalChart = null;

  // Converte log_contrib → contribuição em probabilidade (escala natural)
  function logToProb(logC, prior){
    const p = Number(prior || 0.14);
    const posterior_only_this = p * Math.exp(logC) / (p * Math.exp(logC) + (1-p));
    return posterior_only_this - p;
  }

  function drawCharts(){
    if(state.activeTab !== 'historico' || !window.Chart) return;
    const historyCtx = byId('historyChart');
    const signalCtx = byId('signalChart');
    if(!historyCtx || !signalCtx) return;
    if(historyChart) historyChart.destroy();
    if(signalChart) signalChart.destroy();

    // MELHORIA 2: Bandas de classificação no gráfico histórico
    const zonePlugin = {
      id: 'zonePlugin',
      beforeDraw(chart){
        const {ctx, chartArea:{top,bottom,left,right}, scales:{y}} = chart;
        const zones = [
          {from:0,   to:0.20, color:'rgba(6,118,71,0.07)'},
          {from:0.20,to:0.40, color:'rgba(181,71,8,0.07)'},
          {from:0.40,to:0.60, color:'rgba(180,35,24,0.10)'},
          {from:0.60,to:1.00, color:'rgba(124,10,2,0.12)'},
        ];
        zones.forEach(z=>{
          const yTop    = y.getPixelForValue(z.to);
          const yBottom = y.getPixelForValue(z.from);
          ctx.fillStyle = z.color;
          ctx.fillRect(left, yTop, right-left, yBottom-yTop);
        });
      }
    };

    historyChart = new Chart(historyCtx, {
      type:'line',
      plugins:[zonePlugin],
      data:{
        labels:(state.history||[]).map(h=>h.run_date||h.date),
        datasets:[{
          label:'Posterior',
          data:(state.history||[]).map(h=>h.posterior),
          borderColor:'#0b1835',
          backgroundColor:'#0b1835',
          tension:0.15,
          pointRadius:2
        }]
      },
      options:{
        responsive:true,
        maintainAspectRatio:false,
        scales:{
          y:{
            ticks:{ callback:v=>Math.round(v*100)+'%' },
            min:0,
            max:1
          }
        },
        plugins:{
          annotation:{}, // placeholder se quiser adicionar depois
          legend:{ display:true }
        }
      }
    });

    // MELHORIA 3: Toggle entre log e probabilidade
    const signals = state.latest?.signals || [];
    const prior = state.latest?.prior || 0.14;
    const isLog = state.signalViewMode !== 'prob';
    const values = isLog
      ? signals.map(s=>s.log_contrib)
      : signals.map(s=>logToProb(s.log_contrib, prior));

    signalChart = new Chart(signalCtx, {
      type:'bar',
      data:{
        labels: signals.map(s=>s.signal_id),
        datasets:[{
          label: isLog ? 'Log contribuição' : 'Δ Probabilidade',
          data: values,
          backgroundColor: signals.map(s=>Number(s.log_contrib)>=0?'#f3b5b5':'#b7ebc9'),
          borderColor: signals.map(s=>Number(s.log_contrib)>=0?'#b42318':'#067647'),
          borderWidth:1
        }]
      },
      options:{
        responsive:true,
        maintainAspectRatio:false,
        indexAxis:'y',
        scales:{
          x:{
            ticks:{
              callback: v => isLog ? v.toFixed(2) : (v*100).toFixed(1)+'pp'
            }
          }
        }
      }
    });
  }

  async function init(){
    try{
      const latest = await fetchJson('frontend/data/latest.json');
      const history = await fetchJson('frontend/data/history.json');
      let modelConfig = {signals:[]};
      try { modelConfig = await fetchJson('backend/app/model_config.json'); } catch(e) {}
      state.latest = latest;
      state.history = history;
      state.modelConfig = modelConfig;
      render();
    }catch(err){
      const app = byId('app');
      if(app){
        app.innerHTML = '<div class="card"><h3>Falha ao carregar dados</h3><p>' + err.message + '</p></div>';
      }
    }
  }
  init();
})();
