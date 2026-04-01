async function fetchJson(path) {
  const res = await fetch(path, { cache: 'no-store' });
  if (!res.ok) throw new Error(`Falha ao carregar ${path}`);
  return res.json();
}

function pct(v) {
  return `${(v * 100).toFixed(1)}%`;
}

function fmt(v) {
  return typeof v === 'number' ? v.toFixed(2) : String(v);
}

function badgeClass(status) {
  if (status === 'ATIVO') return 'ativo';
  if (status === 'CONTRARIO') return 'contrario';
  return 'neutro';
}

function renderSignals(signals) {
  const root = document.getElementById('signals');
  root.innerHTML = '';
  signals.forEach((s) => {
    const div = document.createElement('div');
    div.className = 'signal-item';
    div.innerHTML = `
      <div class="signal-top">
        <div>
          <strong style="font-size:16px;margin:0;display:block;">${s.signal_name}</strong>
          <div class="small">${s.block}</div>
        </div>
        <span class="badge ${badgeClass(s.status)}">${s.status}</span>
      </div>
      <div class="table">
        <div class="row"><span>Valor bruto</span><span>${fmt(s.raw_value)}</span><span></span></div>
        <div class="row"><span>Peso</span><span>${fmt(s.weight)}</span><span></span></div>
        <div class="row"><span>LR aplicado</span><span>${fmt(s.lr_used)}</span><span>log contrib ${fmt(s.log_contrib)}</span></div>
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
    div.innerHTML = `<span>${k}</span><strong style="font-size:16px;margin:0;">${fmt(v)}</strong>`;
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

  const padding = 28;
  const w = canvas.width - padding * 2;
  const h = canvas.height - padding * 2;
  const vals = history.map((x) => x.posterior);
  const min = Math.min(...vals, 0.0);
  const max = Math.max(...vals, 1.0);

  ctx.strokeStyle = '#cbd5e1';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padding, padding);
  ctx.lineTo(padding, canvas.height - padding);
  ctx.lineTo(canvas.width - padding, canvas.height - padding);
  ctx.stroke();

  ctx.strokeStyle = '#0f172a';
  ctx.lineWidth = 2;
  ctx.beginPath();
  vals.forEach((v, i) => {
    const x = padding + (i * w) / Math.max(vals.length - 1, 1);
    const y = canvas.height - padding - ((v - min) / Math.max(max - min, 0.0001)) * h;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

async function boot() {
  try {
    const [latest, history] = await Promise.all([
      fetchJson('data/latest.json'),
      fetchJson('data/history.json'),
    ]);
    document.getElementById('run-date').textContent = latest.run_date;
    document.getElementById('risk-label').textContent = latest.risk_label;
    document.getElementById('posterior').textContent = pct(latest.posterior);
    document.getElementById('prior').textContent = pct(latest.prior);
    document.getElementById('external-status').textContent = latest.external_block.status || '-';
    document.getElementById('source-status').textContent = Object.entries(latest.source_status || {}).map(([k, v]) => `${k}: ${v}`).join(' | ');
    renderSignals(latest.signals || []);
    renderKeyValue('external-block', latest.external_block || {});
    renderKeyValue('raw-data', latest.raw_data || {});
    renderHistory(history || []);
  } catch (err) {
    document.body.innerHTML = `<div class="container"><div class="card"><h1>Falha ao carregar dados</h1><p>${err.message}</p></div></div>`;
  }
}

boot();
