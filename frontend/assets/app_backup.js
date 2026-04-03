
(function(){
  const state = { latest:null, history:[], modelConfig:{signals:[]}, activeTab:'dashboard' };

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
  function header(){
    const latest = state.latest || {};
    const subtitle = byId('subtitle');
    const runMeta = byId('run-meta');
    if(subtitle){
      subtitle.innerHTML = 'Painel estático consumindo <span class="code">data/latest.json</span>, <span class="code">data/history.json</span> e <span class="code">backend/app/model_config.json</span>.';
    }
    if(runMeta){
      runMeta.innerHTML = [
        '<div class="meta-kv"><span class="small">EXECUÇÃO</span><strong>' + (latest.run_date||'-') + '</strong></div>',
        '<div class="meta-kv"><span class="small">CLASSIFICAÇÃO</span><strong style="font-size:22px">' + (latest.risk_label||'-') + '</strong></div>',
        '<div class="meta-kv"><span class="small">ÚLTIMO DELTA SEMANAL</span><strong style="font-size:22px">' + weeklyDelta() + '</strong></div>'
      ].join('');
    }
  }
  function blockSummary(){
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
    return '<section class="tab-panel ' + (state.activeTab==='dashboard'?'active':'') + '" data-panel="dashboard">' +
      '<div class="grid grid-4">' +
      '<div class="card dark"><div class="small">PROBABILIDADE POSTERIOR</div><div class="kpi-value">' + pct(latest.posterior) + '</div><div class="kpi-sub">Prior ' + pct(latest.prior) + '</div></div>' +
      '<div class="card"><div class="small">SINAIS ATIVOS</div><div class="kpi-value" style="font-size:42px;color:var(--ink)">' + (latest.signals||[]).filter(s=>s.status==='ATIVO').length + '</div><div class="kpi-sub">Contrários: ' + (latest.signals||[]).filter(s=>s.status==='CONTRARIO').length + ' · Neutros: ' + (latest.signals||[]).filter(s=>s.status==='NEUTRO').length + '</div></div>' +
      '<div class="card"><div class="small">BLOCO EXTERNO</div><div class="kpi-value" style="font-size:42px;color:var(--ink)">' + num(latest.external_block?.composite_score) + '</div><div class="kpi-sub">Status: ' + (latest.external_block?.status||'-') + '</div></div>' +
      '<div class="card"><div class="small">FONTE / STATUS</div><div style="font-size:15px;font-weight:700;line-height:1.45">' + Object.entries(latest.source_status||{}).map(([k,v])=>k + ': ' + v).join(' | ') + '</div></div>' +
      '</div>' +
      '<div class="grid grid-2" style="margin-top:18px"><div class="card"><h3>Resumo por bloco</h3><div class="list">' + blockCards + '</div></div><div class="card"><h3>Vetores de risco</h3><div class="grid grid-2"><div><div class="small" style="color:#b42318;font-weight:700;margin-bottom:8px">Pressões altistas</div><div class="list">' + positives.slice(0,5).map(s=>'<div class="row" style="padding:10px;border-radius:12px;background:var(--red-bg)"><span>'+s.signal_name+'</span><strong>+'+num(s.log_contrib)+'</strong></div>').join('') + '</div></div><div><div class="small" style="color:#067647;font-weight:700;margin-bottom:8px">Amortecedores</div><div class="list">' + negatives.slice(0,5).map(s=>'<div class="row" style="padding:10px;border-radius:12px;background:var(--green-bg)"><span>'+s.signal_name+'</span><strong>'+num(s.log_contrib)+'</strong></div>').join('') + '</div></div></div></div></div>' +
      '<div class="grid grid-2" style="margin-top:18px"><div class="card"><h3>Sinais do modelo</h3><div class="list">' + signalsHtml + '</div></div><div class="grid"><div class="card"><h3>Bloco externo</h3><table class="table"><tbody>' + Object.entries(latest.external_block||{}).map(([k,v])=>'<tr><td>'+k+'</td><td><strong>' + (typeof v === 'number' ? num(v) : v) + '</strong></td></tr>').join('') + '</tbody></table></div><div class="card"><h3>Dados brutos</h3><table class="table"><tbody>' + Object.entries(latest.raw_data||{}).map(([k,v])=>'<tr><td>'+k+'</td><td><strong>'+num(v)+'</strong></td></tr>').join('') + '</tbody></table></div></div></div>' +
      '</section>';
  }
  function panelHistorico(){
    return '<section class="tab-panel ' + (state.activeTab==='historico'?'active':'') + '" data-panel="historico"><div class="grid grid-2"><div class="card"><h3>Evolução da probabilidade</h3><div class="canvas-wrap"><canvas id="historyChart"></canvas></div><table class="table" style="margin-top:12px"><tbody>' + (state.history||[]).slice().reverse().map(h=>'<tr><td>'+(h.run_date||h.date)+'</td><td>'+pct(h.posterior)+'</td><td>'+(h.risk_label||'')+'</td></tr>').join('') + '</tbody></table></div><div class="card"><h3>Contribuição por sinal</h3><div class="canvas-wrap"><canvas id="signalChart"></canvas></div></div></div></section>';
  }
  function panelMetodologia(){
    const rows = state.modelConfig?.signals || [];
    return '<section class="tab-panel ' + (state.activeTab==='metodologia'?'active':'') + '" data-panel="metodologia"><div class="notice"><strong>Como ler esta aba</strong>Cada parâmetro mostra o que mede, a que bloco pertence e qual a origem declarada no modelo. O detalhe operacional da coleta fica em <span class="code">source_status</span> e <span class="code">data_feed_meta</span>.</div><div class="list" style="margin-top:18px">' + rows.map(r=>'<div class="card"><div class="row"><div><h3 style="margin-bottom:6px">'+r.signal_name+'</h3><div class="small">'+r.block+'</div></div><div><span class="badge '+sourceBadgeClass(r.source_type)+'">'+r.source_type+'</span></div></div><p style="margin:14px 0 10px">'+(r.description||'')+'</p><div class="kv-table"><div>Fonte declarada</div><div>'+(r.source_label||'')+'</div><div>Raw key</div><div><span class="code">'+r.raw_key+'</span></div><div>Peso</div><div>'+num(r.weight)+'</div><div>P(E|H) / P(E|~H)</div><div>'+num(r.p_e_h)+' / '+num(r.p_e_not_h)+'</div><div>Thresholds</div><div><span class="code">'+JSON.stringify(r.thresholds||{})+'</span></div></div></div>').join('') + '</div></section>';
  }
  function panelDados(){
    return '<section class="tab-panel ' + (state.activeTab==='dados'?'active':'') + '" data-panel="dados"><div class="card"><h3>Dados brutos</h3><table class="table"><thead><tr><th>Chave</th><th>Valor</th></tr></thead><tbody>' + Object.entries(state.latest?.raw_data||{}).map(([k,v])=>'<tr><td>'+k+'</td><td>'+num(v)+'</td></tr>').join('') + '</tbody></table></div></section>';
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
  let historyChart = null, signalChart = null;
  function drawCharts(){
    if(state.activeTab !== 'historico' || !window.Chart) return;
    const historyCtx = byId('historyChart');
    const signalCtx = byId('signalChart');
    if(!historyCtx || !signalCtx) return;
    if(historyChart) historyChart.destroy();
    if(signalChart) signalChart.destroy();
    historyChart = new Chart(historyCtx, { type:'line', data:{ labels:(state.history||[]).map(h=>h.run_date||h.date), datasets:[{ label:'Posterior', data:(state.history||[]).map(h=>h.posterior), borderColor:'#0b1835', backgroundColor:'#0b1835', tension:0.15 }] }, options:{ responsive:true, maintainAspectRatio:false, scales:{ y:{ ticks:{ callback:v=>Math.round(v*100)+'%' }, min:0, max:1 } } } });
    signalChart = new Chart(signalCtx, { type:'bar', data:{ labels:(state.latest?.signals||[]).map(s=>s.signal_id), datasets:[{ label:'Log contribution', data:(state.latest?.signals||[]).map(s=>s.log_contrib), backgroundColor:(state.latest?.signals||[]).map(s=>Number(s.log_contrib)>=0?'#f3b5b5':'#b7ebc9'), borderColor:(state.latest?.signals||[]).map(s=>Number(s.log_contrib)>=0?'#b42318':'#067647'), borderWidth:1 }] }, options:{ responsive:true, maintainAspectRatio:false, indexAxis:'y' } });
  }
  async function init(){
    try{
      const latest = await fetchJson('data/latest.json');
      const history = await fetchJson('data/history.json');
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
