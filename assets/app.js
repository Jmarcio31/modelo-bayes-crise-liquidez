async function fetchJson(path) {
  const res = await fetch(path, { cache: 'no-store' });
  if (!res.ok) throw new Error(`Falha ao carregar ${path}`);
  return res.json();
}

function pct(v) { return `${(v * 100).toFixed(1)}%`; }
function fmt(v) { return typeof v === 'number' ? Number(v).toFixed(2) : String(v); }
function badgeClass(status) {
  if (status === 'ATIVO') return 'ativo';
  if (status === 'CONTRARIO') return 'contrario';
  return 'neutro';
}
function signalClass(status) {
  if (status === 'ATIVO') return 'active';
  if (status === 'CONTRARIO') return 'contrary';
  return 'neutral';
}
function sum(arr) { return arr.reduce((a, b) => a + b, 0); }

function groupByBlock(signals) {
  const grouped = {};
  signals.forEach((s) => {
    if (!grouped[s.block]) grouped[s.block] = [];
    grouped[s.block].push(s);
  });
  return Object.entries(grouped).map(([block, items]) => ({
    block,
    items,
    active: items.filter((i) => i.status === 'ATIVO').length,
    contrary: items.filter((i) => i.status === 'CONTRARIO').length,
    score: sum(items.map((i) => i.log_contrib || 0)),
  })).sort((a, b) => Math.abs(b.score) - Math.abs(a.score));
}

function renderBlockSummary(signals) {
  const root = document.getElementById('block-summary');
  root.innerHTML = '';
  groupByBlock(signals).forEach((b) => {
    const div = document.createElement('div');
    const pctAbs = Math.min(100, Math.abs(b.score) * 35);
    div.className = 'block-card';
    div.innerHTML = `
      <div class="block-card-head">
        <div>
          <div class="signal-title">${b.block}</div>
          <div class="small">Ativos ${b.active} · Contrários ${b.contrary}</div>
        </div>
        <div class="${b.score >= 0 ? 'block-score-pos' : 'block-score-neg'}">${b.score >= 0 ? '+' : ''}${fmt(b.score)}</div>
      </div>
      <div class="block-meter"><div style="width:${pctAbs}%"></div></div>`;
    root.appendChild(div);
  });
}

function renderVectorList(containerId, signals, kind) {
  const root = document.getElementById(containerId);
  root.innerHTML = '';
  signals.slice(0, 5).forEach((s) => {
    const div = document.createElement('div');
    div.className = `vector-item ${kind}`;
    div.innerHTML = `<div class="name">${s.signal_name}</div><div class="value">${s.log_contrib >= 0 ? '+' : ''}${fmt(s.log_contrib)}</div>`;
    root.appendChild(div);
  });
}

function renderSignals(signals) {
  const root = document.getElementById('signals');
  root.innerHTML = '';
  signals.forEach((s) => {
    const div = document.createElement('div');
    div.className = `signal-item ${signalClass(s.status)}`;
    div.innerHTML = `
      <div class="signal-top">
        <div>
          <div class="signal-title">${s.signal_name}</div>
          <div class="small">${s.block}</div>
        </div>
        <div class="signal-top-right">
          <span class="badge ${badgeClass(s.status)}">${s.status}</span>
          <div class="contrib ${s.log_contrib >= 0 ? 'pos' : 'neg'}">${s.log_contrib >= 0 ? '+' : ''}${fmt(s.log_contrib)}</div>
        </div>
      </div>
      <div class="table">
        <div class="row"><span>Valor bruto</span><span>${fmt(s.raw_value)}</span><span></span></div>
        <div class="row"><span>Peso</span><span>${fmt(s.weight)}</span><span>P(E|H) ${fmt(s.p_e_h)}</span></div>
        <div class="row"><span>LR aplicado</span><span>${fmt(s.lr_used)}</span><span>P(E|~H) ${fmt(s.p_e_not_h)}</span></div>
      </div>`;
    root.appendChild(div);
  });
}

function renderKeyValue(containerId, payload) {
  const root = document.getElementById(containerId);
  root.innerHTML = '';
  Object.entries(payload).forEach(([k, v]) => {
    const div = document.createElement('div');
    div.className = 'kv';
    div.innerHTML = `<span>${k}</span><strong>${fmt(v)}</strong>`;
    root.appendChild(div);
  });
}

function renderHistory(history) {
  const table = document.getElementById('history-table');
  table.innerHTML = history.map((h) => `<div class="row"><span>${h.run_date}</span><span>${pct(h.posterior)}</span><span>${h.risk_label}</span></div>`).join('');

  const canvas = document.getElementById('history-chart');
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (!history.length) return;

  const padding = 36;
  const w = canvas.width - padding * 2;
  const h = canvas.height - padding * 2;
  const vals = history.map((x) => x.posterior);
  const min = 0;
  const max = 1;

  ctx.strokeStyle = '#cbd5e1';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padding, padding);
  ctx.lineTo(padding, canvas.height - padding);
  ctx.lineTo(canvas.width - padding, canvas.height - padding);
  ctx.stroke();

  [0.25, 0.5, 0.75].forEach((mark) => {
    const y = canvas.height - padding - ((mark - min) / (max - min)) * h;
    ctx.strokeStyle = '#e2e8f0';
    ctx.beginPath();
    ctx.moveTo(padding, y);
    ctx.lineTo(canvas.width - padding, y);
    ctx.stroke();
    ctx.fillStyle = '#64748b';
    ctx.font = '12px Arial';
    ctx.fillText(`${Math.round(mark * 100)}%`, 4, y + 4);
  });

  ctx.strokeStyle = '#0f172a';
  ctx.lineWidth = 2.5;
  ctx.beginPath();
  vals.forEach((v, i) => {
    const x = padding + (i * w) / Math.max(vals.length - 1, 1);
    const y = canvas.height - padding - ((v - min) / (max - min)) * h;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.stroke();

  ctx.fillStyle = '#0f172a';
  vals.forEach((v, i) => {
    const x = padding + (i * w) / Math.max(vals.length - 1, 1);
    const y = canvas.height - padding - ((v - min) / (max - min)) * h;
    ctx.beginPath();
    ctx.arc(x, y, 3, 0, Math.PI * 2);
    ctx.fill();
  });
}

function renderImpactChart(signals) {
  const canvas = document.getElementById('impact-chart');
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (!signals.length) return;

  const sorted = [...signals].sort((a, b) => Math.abs(b.log_contrib) - Math.abs(a.log_contrib)).slice(0, 8);
  const padding = { top: 24, right: 24, bottom: 24, left: 120 };
  const chartW = canvas.width - padding.left - padding.right;
  const chartH = canvas.height - padding.top - padding.bottom;
  const maxAbs = Math.max(...sorted.map((s) => Math.abs(s.log_contrib)), 0.1);
  const zeroX = padding.left + chartW / 2;

  ctx.strokeStyle = '#cbd5e1';
  ctx.beginPath();
  ctx.moveTo(zeroX, padding.top);
  ctx.lineTo(zeroX, canvas.height - padding.bottom);
  ctx.stroke();

  const barH = chartH / sorted.length * 0.65;
  sorted.forEach((s, idx) => {
    const y = padding.top + idx * (chartH / sorted.length) + 6;
    const width = Math.abs(s.log_contrib) / maxAbs * (chartW / 2 - 12);
    const isPos = s.log_contrib >= 0;
    ctx.fillStyle = isPos ? '#fecaca' : '#bbf7d0';
    const x = isPos ? zeroX : zeroX - width;
    ctx.fillRect(x, y, width, barH);
    ctx.fillStyle = '#0f172a';
    ctx.font = '12px Arial';
    ctx.fillText(s.signal_id, 8, y + barH / 2 + 4);
    const valText = `${isPos ? '+' : ''}${fmt(s.log_contrib)}`;
    ctx.fillText(valText, isPos ? x + width + 6 : x - 42, y + barH / 2 + 4);
  });
}

async function boot() {
  try {
    const [latest, history] = await Promise.all([
      fetchJson('data/latest.json'),
      fetchJson('data/history.json'),
    ]);
    const signals = latest.signals || [];
    const positives = signals.filter((s) => s.log_contrib > 0).sort((a, b) => b.log_contrib - a.log_contrib);
    const negatives = signals.filter((s) => s.log_contrib < 0).sort((a, b) => a.log_contrib - b.log_contrib);
    const activeCount = signals.filter((s) => s.status === 'ATIVO').length;
    const contraryCount = signals.filter((s) => s.status === 'CONTRARIO').length;
    const neutralCount = signals.filter((s) => s.status === 'NEUTRO').length;

    document.getElementById('run-date').textContent = latest.run_date;
    document.getElementById('risk-label').textContent = latest.risk_label;
    document.getElementById('posterior').textContent = pct(latest.posterior);
    document.getElementById('prior-inline').textContent = pct(latest.prior);
    document.getElementById('active-count').textContent = String(activeCount);
    document.getElementById('contrary-count').textContent = String(contraryCount);
    document.getElementById('neutral-count').textContent = String(neutralCount);
    document.getElementById('external-status').textContent = latest.external_block.status || '-';
    document.getElementById('external-score').textContent = fmt(latest.external_block.composite_score || 0);
    document.getElementById('source-status').textContent = Object.entries(latest.source_status || {}).map(([k, v]) => `${k}: ${v}`).join(' | ');

    if ((history || []).length >= 2) {
      const delta = history[history.length - 1].posterior - history[history.length - 2].posterior;
      document.getElementById('weekly-delta').textContent = `${delta >= 0 ? '+' : ''}${pct(Math.abs(delta))}`;
    } else {
      document.getElementById('weekly-delta').textContent = 'n/d';
    }

    renderBlockSummary(signals);
    renderVectorList('positive-vectors', positives, 'danger');
    renderVectorList('negative-vectors', negatives, 'safe');
    renderSignals(signals);
    renderKeyValue('external-block', latest.external_block || {});
    renderKeyValue('raw-data', latest.raw_data || {});
    renderHistory(history || []);
    renderImpactChart(signals);
  } catch (err) {
    document.body.innerHTML = `<div class="container"><div class="card"><h1>Falha ao carregar dados</h1><p>${err.message}</p></div></div>`;
  }
}

boot();
