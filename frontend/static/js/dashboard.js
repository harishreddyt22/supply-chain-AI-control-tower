/* ═══════════════════════════════════════════════════════
   Supply Chain AI Dashboard — PostgreSQL-backed
   Live stream: 5 real DB records every 10 seconds
   ═══════════════════════════════════════════════════════ */

let socket, map, forecastChart, modeSplitChart;
let currentLayer = 'heatmap';
let shipmentMarkers = [], allShipments = [];
let streamActive = false;
let streamTick = 0;
let liveStreamState = {
  batches: 0,
  records: [],
  seenShipments: new Set(),
  totalEstimatedSavings: 0,
};
const STREAM_STATE_KEY = 'supply_chain_live_stream_state_v1';

// ── COLOUR HELPERS ─────────────────────────────────────
const riskColor = s => s < 0.3 ? '#10b981' : s < 0.55 ? '#f59e0b' : s < 0.75 ? '#f97316' : '#ef4444';
const sevClass  = s => ({low:'pill-ok',medium:'pill-warn',high:'pill-risk',critical:'pill-crit'})[s]||'pill-warn';
const toNum = v => {
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : 0;
};
const isTrueFlag = v => ['true', 't', '1', 'yes'].includes(String(v ?? '').toLowerCase());

function persistLiveState() {
  try {
    const payload = {
      batches: liveStreamState.batches,
      records: liveStreamState.records.slice(-250),
      seenShipments: Array.from(liveStreamState.seenShipments),
      totalEstimatedSavings: liveStreamState.totalEstimatedSavings,
      streamTick,
    };
    localStorage.setItem(STREAM_STATE_KEY, JSON.stringify(payload));
  } catch (e) {}
}

function restoreLiveState() {
  try {
    const raw = localStorage.getItem(STREAM_STATE_KEY);
    if (!raw) return false;
    const saved = JSON.parse(raw);
    liveStreamState = {
      batches: saved.batches || 0,
      records: Array.isArray(saved.records) ? saved.records : [],
      seenShipments: new Set(saved.seenShipments || []),
      totalEstimatedSavings: saved.totalEstimatedSavings || 0,
    };
    streamTick = saved.streamTick || 0;
    renderPersistedStreamLog(liveStreamState.records);
    updateKPIsFromStream([]);
    return true;
  } catch (e) {
    return false;
  }
}

// ── CLOCK ──────────────────────────────────────────────
setInterval(() => { const el=document.getElementById('clock'); if(el) el.textContent=new Date().toLocaleTimeString(); }, 1000);

// ── MAP ────────────────────────────────────────────────
function initMap() {
  map = L.map('map', { center: [20, 30], zoom: 2, attributionControl: false });
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { opacity: 0.8 }).addTo(map);
  loadHeatmap();
}

function loadHeatmap() {
  fetch('/api/risk-heatmap').then(r=>r.json()).then(({data, db_connected}) => {
    clearMarkers();
    if (!db_connected) {
      showMapMessage('PostgreSQL not connected. Connect DB and run setup_db.py to load heatmap.');
      return;
    }
    if (!data.length) {
      showMapMessage('PostgreSQL is connected, but no mappable route_events rows matched the dashboard filters.');
      return;
    }
    hideMapMessage();
    data.forEach(pt => {
      const color = riskColor(pt.intensity);
      const marker = L.circleMarker([pt.lat, pt.lon], {
        radius: 8 + pt.intensity * 18, fillColor: color,
        color: 'transparent', fillOpacity: 0.35, weight: 0,
      }).addTo(map)
        .bindPopup(mapPopup(pt, color));
      shipmentMarkers.push(marker);
    });
  }).catch(() => {
    setKPI('active', '0', 'KPI fetch failed');
    setKPI('risk', '0', 'KPI fetch failed');
    setKPI('ontime', 'â€”', 'KPI fetch failed');
    setKPI('alerts', '0', 'KPI fetch failed');
    setKPI('savings', 'â€”', 'KPI fetch failed');
    setKPI('optimized', '0', 'KPI fetch failed');
  });
}

function mapPopup(pt, color) {
  return `<div style="font-family:'IBM Plex Sans',sans-serif;font-size:12px;min-width:180px">
    <strong style="color:#e8f0ff">${pt.region}</strong><br>
    <span style="color:#7a9cc0">Risk:</span> <b style="color:${color}">${(pt.intensity*100).toFixed(0)}%</b><br>
    <span style="color:#7a9cc0">Events:</span> ${pt.event_count||0}<br>
    <span style="color:#7a9cc0">Anomalies:</span> ${pt.anomaly_count||0}<br>
    <span style="color:#7a9cc0">Level:</span> ${(pt.type||'').toUpperCase()}
  </div>`;
}

function showMapMessage(msg) {
  const el = document.getElementById('map-message');
  if (el) { el.textContent = msg; el.style.display = 'block'; }
}

function hideMapMessage() {
  const el = document.getElementById('map-message');
  if (el) {
    el.textContent = '';
    el.style.display = 'none';
  }
}

function clearMarkers() {
  shipmentMarkers.forEach(m => { try { map.removeLayer(m); } catch(e){} });
  shipmentMarkers = [];
}

function setMapLayer(layer) {
  currentLayer = layer;
  ['btn-heatmap','btn-shipments','btn-bottlenecks'].forEach(id => {
    document.getElementById(id)?.classList.remove('active');
  });
  document.getElementById(`btn-${layer}`)?.classList.add('active');
  if (layer === 'heatmap') loadHeatmap();
  else if (layer === 'shipments') loadShipmentMarkers();
  else if (layer === 'bottlenecks') loadBottleneckMarkers();
}

function loadShipmentMarkers() {
  fetch('/api/shipments?limit=100').then(r=>r.json()).then(({data}) => {
    clearMarkers();
    (data||[]).forEach(s => {
      const risk = s.risk_score || 0;
      const color = riskColor(risk);
      const icon = L.divIcon({
        html: `<div style="width:8px;height:8px;border-radius:50%;background:${color};border:1px solid rgba(255,255,255,0.4)"></div>`,
        iconSize:[8,8], className:'',
      });
      const m = L.marker([s.lat||0, s.lon||0], { icon })
        .addTo(map)
        .bindPopup(`<div style="font-family:'IBM Plex Sans',sans-serif;font-size:11px;min-width:200px">
          <b style="color:#e8f0ff">${s.shipment_id||s.id}</b><br>
          <span style="color:#7a9cc0">Carrier:</span> ${s.carrier||'—'}<br>
          <span style="color:#7a9cc0">Route:</span> ${s.origin_port||s.origin} → ${s.destination_port||s.destination}<br>
          <span style="color:#7a9cc0">Status:</span> ${s.status||'—'}<br>
          <span style="color:#7a9cc0">Risk:</span> <b style="color:${color}">${(risk*100).toFixed(0)}%</b>
        </div>`);
      shipmentMarkers.push(m);
    });
  });
}

function loadBottleneckMarkers() {
  fetch('/api/bottlenecks').then(r=>r.json()).then(({data}) => {
    clearMarkers();
    (data||[]).forEach(bn => {
      const color = riskColor(bn.severity_score);
      const icon = L.divIcon({
        html: `<div style="background:${color};color:#fff;font-size:9px;font-weight:700;padding:2px 5px;border-radius:2px;font-family:'Space Mono',monospace;white-space:nowrap">⚠ ${(bn.type||'').replace('_',' ').toUpperCase()}</div>`,
        className:'', iconAnchor:[0,0],
      });
      const m = L.marker([bn.lat||0, bn.lon||0], { icon })
        .addTo(map)
        .bindPopup(`<div style="font-size:11px;font-family:'IBM Plex Sans',sans-serif;min-width:200px">
          <b style="color:#e8f0ff">${bn.location}</b><br>
          <span style="color:#7a9cc0">Severity:</span> <b style="color:${color}">${(bn.severity||'').toUpperCase()}</b><br>
          <span style="color:#7a9cc0">Avg Delay:</span> ${bn.avg_delay_hours}h<br>
          <span style="color:#7a9cc0">Events:</span> ${bn.event_count}<br>
          <span style="color:#7a9cc0">Clears In:</span> ${bn.estimated_clearance_hours}h<br>
          <span style="color:#7a9cc0">Action:</span> ${bn.recommendation}
        </div>`);
      shipmentMarkers.push(m);
    });
  });
}

function refreshMap() { setMapLayer(currentLayer); }

// ── CHARTS ─────────────────────────────────────────────
function initForecastChart() {
  const ctx = document.getElementById('forecastChart')?.getContext('2d');
  if (!ctx) return;
  forecastChart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [
      { label:'Forecast', data:[], borderColor:'#3b82f6', backgroundColor:'rgba(59,130,246,0.08)', borderWidth:2, fill:true, tension:0.4, pointRadius:0 },
      { data:[], borderColor:'rgba(59,130,246,0.25)', borderWidth:1, borderDash:[4,4], fill:false, pointRadius:0 },
      { data:[], borderColor:'rgba(59,130,246,0.25)', borderWidth:1, borderDash:[4,4], fill:'-1', backgroundColor:'rgba(59,130,246,0.04)', pointRadius:0 },
    ]},
    options: {
      responsive:true, maintainAspectRatio:true,
      plugins:{ legend:{ display:false } },
      scales:{
        x:{ ticks:{color:'#3d5878',maxTicksLimit:8,font:{family:'Space Mono',size:9}}, grid:{color:'rgba(30,45,66,0.5)'} },
        y:{ ticks:{color:'#3d5878',font:{family:'Space Mono',size:9}}, grid:{color:'rgba(30,45,66,0.5)'} }
      },
      animation:{ duration:500 }
    }
  });
  loadForecast();
}

function loadForecast() {
  const metric = document.getElementById('forecast-metric')?.value || 'delay_probability';
  fetch(`/api/forecast?metric=${metric}&horizon=24`).then(r=>r.json()).then(({data}) => {
    if (!forecastChart) return;
    forecastChart.data.labels = data.timestamps.map((_,i) => `+${i}h`);
    forecastChart.data.datasets[0].data = data.values;
    forecastChart.data.datasets[1].data = data.upper_bound;
    forecastChart.data.datasets[2].data = data.lower_bound;
    forecastChart.update();
    // Show data source
    const src = document.getElementById('forecast-source');
    if (src) src.textContent = data.data_source === 'postgresql' ? '📊 Source: PostgreSQL' : '📊 Source: ML model';
  });
}

function initModeSplitChart() {
  const ctx = document.getElementById('modeSplitChart')?.getContext('2d');
  if (!ctx) return;
  modeSplitChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['Sea','Air','Rail','Road'],
      datasets: [{ data:[58,21,12,9], backgroundColor:['#3b82f6','#06b6d4','#8b5cf6','#10b981'], borderWidth:0, hoverOffset:4 }]
    },
    options: {
      responsive:true, maintainAspectRatio:true,
      plugins:{ legend:{ position:'bottom', labels:{ color:'#7a9cc0', font:{size:10,family:'Space Mono'}, padding:10, boxWidth:10 } } },
      cutout:'65%', animation:{ duration:800 }
    }
  });
}

// ── KPIs ───────────────────────────────────────────────
function loadKPIs() {
  if (streamActive) return;
  fetch('/api/kpis').then(r=>r.json()).then(({data}) => {
    setKPI('active',    data.total_active_shipments?.toLocaleString()||'0', data.data_source === 'postgresql' ? '✓ From PostgreSQL' : '⚠ DB not connected');
    setKPI('risk',      data.at_risk_shipments?.toLocaleString()||'0', `Risk score > 0.55`);
    setKPI('ontime',    data.on_time_rate ? (data.on_time_rate*100).toFixed(1)+'%' : '—', data.on_time_rate > 0.9 ? '✓ Above target' : data.on_time_rate ? '↓ Below target' : 'No data');
    setKPI('alerts',    data.critical_alerts||0, `${data.disruptions_prevented||0} prevented`);
    setKPI('savings',   data.cost_savings_today_usd ? '$'+(data.cost_savings_today_usd/1000).toFixed(0)+'K' : '—', 'Estimated savings');
    setKPI('optimized', data.routes_optimized_today||0, 'Routes today');

    const badge = document.getElementById('alert-count-badge');
    if (badge) badge.textContent = `${data.critical_alerts||0} Alerts`;

    // Carrier bars
    const barEl = document.getElementById('carrier-bars');
    if (barEl && data.carrier_performance && Object.keys(data.carrier_performance).length) {
      barEl.innerHTML = Object.entries(data.carrier_performance).map(([name,score]) => `
        <div class="carrier-bar-row">
          <div class="carrier-bar-label"><span>${name.replace('_',' ')}</span><span>${(score*100).toFixed(0)}%</span></div>
          <div class="carrier-bar-bg"><div class="carrier-bar-fg" style="width:${score*100}%;background:${riskColor(1-score)}"></div></div>
        </div>`).join('');
    } else if (barEl) {
      barEl.innerHTML = '<div class="empty-state">Connect DB to see carrier performance</div>';
    }

    // Mode split
    if (modeSplitChart && data.mode_split && Object.keys(data.mode_split).length) {
      const modes = ['sea','air','rail','road'];
      const total = modes.reduce((s,m) => s+(data.mode_split[m]||0), 0) || 1;
      modeSplitChart.data.datasets[0].data = modes.map(m => +((data.mode_split[m]||0)/total*100).toFixed(1));
      modeSplitChart.update();
    }

    // DB status indicator
    const dbBadge = document.getElementById('db-status-badge');
    if (dbBadge) {
      dbBadge.textContent = data.data_source === 'postgresql' ? '🟢 DB Connected' : '🔴 DB Offline';
      dbBadge.style.color = data.data_source === 'postgresql' ? 'var(--accent-green)' : 'var(--accent-red)';
    }
  });
}

function setKPI(key, value, trend) {
  const v = document.getElementById(`kv-${key}`); if(v) v.textContent = value;
  const t = document.getElementById(`kt-${key}`); if(t) t.textContent = trend;
}

function resetLiveKPIState() {
  liveStreamState = {
    batches: 0,
    records: [],
    seenShipments: new Set(),
    totalEstimatedSavings: 0,
  };
  streamTick = 0;
  persistLiveState();
}

function updateKPIsFromStream(records = []) {
  if (records.length) {
    liveStreamState.batches += 1;
    records.forEach(record => {
      liveStreamState.records.push(record);
      if (record.shipment_id) liveStreamState.seenShipments.add(record.shipment_id);
    });
  }

  if (liveStreamState.records.length > 250) {
    liveStreamState.records = liveStreamState.records.slice(-250);
  }

  const sample = liveStreamState.records;
  const activeShipments = new Set(sample.map(r => r.shipment_id).filter(Boolean));
  const atRiskShipments = new Set();
  let onTimeEvents = 0;
  let criticalAlerts = 0;

  sample.forEach(r => {
    const delay = toNum(r.delay_added_hours);
    const riskDelta = toNum(r.risk_score_delta);
    const waitHours = toNum(r.port_wait_hours);
    const anomaly = isTrueFlag(r.anomaly_flag);
    const severe = anomaly || riskDelta >= 0.12 || delay >= 6 || waitHours >= 12;

    if (delay <= 0 && !anomaly) onTimeEvents += 1;
    if (severe && r.shipment_id) atRiskShipments.add(r.shipment_id);
    if (anomaly || riskDelta >= 0.18) criticalAlerts += 1;
  });

  const onTimeRate = sample.length ? onTimeEvents / sample.length : 0;
  const routesOptimized = Math.max(liveStreamState.batches, atRiskShipments.size);
  liveStreamState.totalEstimatedSavings += Math.max(0, atRiskShipments.size * 120 + criticalAlerts * 40);

  setKPI('active', activeShipments.size.toLocaleString(), `Live window: ${sample.length} streamed events`);
  setKPI('risk', atRiskShipments.size.toLocaleString(), 'Updated from 5-row live batches');
  setKPI('ontime', `${(onTimeRate * 100).toFixed(1)}%`, 'Based on latest streamed route events');
  setKPI('alerts', criticalAlerts.toLocaleString(), `${liveStreamState.batches} live batches processed`);
  setKPI('savings', `$${Math.round(liveStreamState.totalEstimatedSavings).toLocaleString()}`, 'Estimated from live interventions');
  setKPI('optimized', routesOptimized.toLocaleString(), 'Incremented from live stream decisions');

  const badge = document.getElementById('alert-count-badge');
  if (badge) badge.textContent = `${criticalAlerts} Alerts`;
  persistLiveState();
}

// ── BOTTLENECKS ────────────────────────────────────────
function refreshBottlenecks() {
  fetch('/api/bottlenecks').then(r=>r.json()).then(({data, db_connected}) => {
    const tbody = document.getElementById('bottleneck-tbody');
    if (!tbody) return;
    if (!db_connected || !data.length) {
      tbody.innerHTML = `<tr><td colspan="6" class="empty-state">${db_connected ? 'No bottlenecks detected' : 'Connect PostgreSQL to see bottlenecks'}</td></tr>`;
      return;
    }
    tbody.innerHTML = data.map(bn => `<tr>
      <td><strong style="color:#e8f0ff">${bn.location}</strong></td>
      <td style="color:#7a9cc0">${(bn.type||'').replace('_',' ')}</td>
      <td><span class="pill ${sevClass(bn.severity)}">${(bn.severity||'').toUpperCase()}</span></td>
      <td style="color:#f59e0b">${bn.event_count}</td>
      <td style="font-family:'Space Mono',monospace">${bn.estimated_clearance_hours}h</td>
      <td style="color:#06b6d4;font-size:10px">${bn.recommendation}</td>
    </tr>`).join('');
  });
}

// ── SHIPMENTS TABLE ────────────────────────────────────
function loadShipments() {
  fetch('/api/shipments?limit=50').then(r=>r.json()).then(({data, db_connected}) => {
    allShipments = data || [];
    renderShipmentTable(allShipments.slice(0, 50), db_connected);
  });
}

function renderShipmentTable(ships, dbOk=true) {
  const tbody = document.getElementById('shipment-tbody');
  if (!tbody) return;
  if (!ships.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="empty-state">${dbOk ? 'No shipments found' : '⚠ Connect PostgreSQL — run setup_db.py to load data'}</td></tr>`;
    return;
  }
  tbody.innerHTML = ships.map(s => {
    const id     = s.shipment_id || s.id || '—';
    const risk   = parseFloat(s.risk_score || 0);
    const color  = riskColor(risk);
    const status = s.status || '—';
    const pillCls = {
      'On_Schedule':'pill-ok','In_Transit':'pill-ok','At_Port':'pill-warn',
      'Customs_Hold':'pill-warn','Delayed':'pill-risk','Critical_Delay':'pill-crit','Delivered':'pill-ok'
    }[status] || 'pill-warn';
    const eta = s.eta_date || s.eta || '—';
    return `<tr>
      <td><span style="font-family:'Space Mono',monospace;color:#06b6d4;font-size:10px">${id}</span></td>
      <td>${(s.carrier||'—').replace('_',' ')}</td>
      <td style="color:#7a9cc0">${(s.origin_port||s.origin||'—').replace('_',' ')} → ${(s.destination_port||s.destination||'—').replace('_',' ')}</td>
      <td><span style="text-transform:uppercase;font-size:9px;color:#8b5cf6;font-family:'Space Mono',monospace">${s.transport_mode||s.mode||'—'}</span></td>
      <td><span class="pill ${pillCls}">${status.replace('_',' ')}</span></td>
      <td>
        <div class="risk-bar-wrap">
          <div class="risk-bar"><div class="risk-bar-fill" style="width:${risk*100}%;background:${color}"></div></div>
          <span style="color:${color};font-size:10px;font-family:'Space Mono',monospace">${(risk*100).toFixed(0)}%</span>
        </div>
      </td>
      <td style="font-family:'Space Mono',monospace;font-size:10px">${String(eta).slice(0,10)}</td>
    </tr>`;
  }).join('');
}

function filterShipments() {
  const q = (document.getElementById('shipment-search')?.value||'').toLowerCase();
  if (!q) { renderShipmentTable(allShipments.slice(0,50)); return; }
  renderShipmentTable(allShipments.filter(s =>
    (s.shipment_id||'').toLowerCase().includes(q) ||
    (s.carrier||'').toLowerCase().includes(q) ||
    (s.origin_port||'').toLowerCase().includes(q) ||
    (s.destination_port||'').toLowerCase().includes(q) ||
    (s.status||'').toLowerCase().includes(q)
  ).slice(0, 50));
}

// ── WEBSOCKET STREAM ───────────────────────────────────
function initSocket() {
  socket = io();

  socket.on('connect', () => {
    const el = document.getElementById('conn-status');
    if (el) { el.textContent = '● CONNECTED'; el.classList.add('connected'); }
    socket.emit('get_stream_status');
  });

  socket.on('disconnect', () => {
    const el = document.getElementById('conn-status');
    if (el) { el.textContent = '● DISCONNECTED'; el.classList.remove('connected'); }
    streamActive = false;
    const btn = document.getElementById('btn-sim-toggle');
    if (btn) { btn.textContent = '▶ Start Live Stream'; btn.className = 'btn-sim start'; }
  });

  // ── Real DB stream batch (5 records / 10 sec) ──────
  socket.on('live_stream_batch', ({ records, tick, offset, total_rows, table, timestamp, batch_size }) => {
    streamTick = tick;
    updateStreamPanel(records, tick, offset, total_rows, table, timestamp);
    appendStreamLog(records, table);
    updateKPIsFromStream(records);
  });

  socket.on('stream_status', ({ status, message, tick }) => {
    const log = document.getElementById('stream-log');
    if (log) {
      const item = document.createElement('div');
      item.className = 'stream-item warning';
      item.innerHTML = `<span class="stream-ts">${new Date().toLocaleTimeString()}</span> <span style="color:var(--accent-yellow)">${message}</span>`;
      log.prepend(item);
    }
  });

  socket.on('stream_health', (data) => {
    const hEl = document.getElementById('stream-health');
    if (hEl) hEl.innerHTML = `
      <span style="color:var(--text-muted)">Table:</span> <b>${data.table}</b> &nbsp;|&nbsp;
      <span style="color:var(--text-muted)">Offset:</span> <b>${data.offset?.toLocaleString()}</b> / ${data.total_rows?.toLocaleString()} &nbsp;|&nbsp;
      <span style="color:var(--text-muted)">DB:</span> <b style="color:${data.db_connected?'var(--accent-green)':'var(--accent-red)'}}">${data.db_connected?'Connected':'Offline'}</b> &nbsp;|&nbsp;
      <span style="color:var(--text-muted)">Batch:</span> ${data.batch_size} rows / ${data.interval_sec}s
    `;
  });

  socket.on('stream_status_response', (data) => {
    const stats = document.getElementById('stream-stats');
    if (stats) {
      stats.innerHTML = `
        <span class="stream-stat">Offset <b>${(data.offset || 0).toLocaleString()}</b></span>
        <span class="stream-stat">Processed <b>${(data.processed_records || 0).toLocaleString()}</b></span>
        <span class="stream-stat">Total <b>${(data.total_rows || 0).toLocaleString()}</b></span>
        <span class="stream-stat">Table <b>${data.table}</b></span>
      `;
    }
    const hEl = document.getElementById('stream-health');
    if (hEl) {
      hEl.innerHTML = `
        <span style="color:var(--text-muted)">Resume from:</span> <b>${(data.offset || 0).toLocaleString()}</b> &nbsp;|&nbsp;
        <span style="color:var(--text-muted)">Processed:</span> <b>${(data.processed_records || 0).toLocaleString()}</b> &nbsp;|&nbsp;
        <span style="color:var(--text-muted)">Last batch:</span> <b>${data.last_batch_at ? new Date(data.last_batch_at).toLocaleTimeString() : '—'}</b>
      `;
    }
  });
}

function updateStreamPanel(records, tick, offset, total, table, ts) {
  const panel = document.getElementById('stream-stats');
  if (panel) panel.innerHTML = `
    <span class="stream-stat">Tick <b>${tick}</b></span>
    <span class="stream-stat">Offset <b>${offset?.toLocaleString()}</b></span>
    <span class="stream-stat">Total <b>${total?.toLocaleString()}</b></span>
    <span class="stream-stat">Table <b>${table}</b></span>
    <span class="stream-stat">${new Date(ts).toLocaleTimeString()}</span>
  `;
}

function appendStreamLog(records, table) {
  const log = document.getElementById('stream-log');
  if (!log) return;
  records.forEach(r => {
    const item = document.createElement('div');
    item.className = 'stream-item';
    const id = r.event_id || r.shipment_id || '—';
    const loc = r.location_name || r.origin_port || '—';
    const risk = r.risk_score_delta !== undefined
      ? `Δrisk: ${r.risk_score_delta >= 0 ? '+' : ''}${parseFloat(r.risk_score_delta||0).toFixed(3)}`
      : `risk: ${parseFloat(r.risk_score||0).toFixed(3)}`;
    const anomaly = r.anomaly_flag ? '<span style="color:var(--accent-red);font-weight:700"> ⚠</span>' : '';
    item.innerHTML = `
      <span class="stream-ts">${new Date().toLocaleTimeString()}</span>
      <span style="color:#06b6d4;font-family:'Space Mono',monospace;font-size:10px">${id}</span>
      <span style="color:#7a9cc0"> @ ${(loc).replace(/_/g,' ')}</span>
      <span style="color:var(--accent-yellow)"> ${risk}</span>${anomaly}
    `;
    log.prepend(item);
    // Keep max 100 entries
    while (log.children.length > 100) log.removeChild(log.lastChild);
  });
}

function renderPersistedStreamLog(records) {
  const log = document.getElementById('stream-log');
  if (!log || !records.length) return;
  log.innerHTML = '';
  records.slice().reverse().forEach(r => {
    const item = document.createElement('div');
    item.className = 'stream-item';
    const id = r.event_id || r.shipment_id || '—';
    const loc = r.location_name || r.origin_port || '—';
    const risk = r.risk_score_delta !== undefined
      ? `Δrisk: ${r.risk_score_delta >= 0 ? '+' : ''}${parseFloat(r.risk_score_delta||0).toFixed(3)}`
      : `risk: ${parseFloat(r.risk_score||0).toFixed(3)}`;
    const anomaly = isTrueFlag(r.anomaly_flag) ? '<span style="color:var(--accent-red);font-weight:700"> ⚠</span>' : '';
    item.innerHTML = `
      <span class="stream-ts">${new Date().toLocaleTimeString()}</span>
      <span style="color:#06b6d4;font-family:'Space Mono',monospace;font-size:10px">${id}</span>
      <span style="color:#7a9cc0"> @ ${(loc).replace(/_/g,' ')}</span>
      <span style="color:var(--accent-yellow)"> ${risk}</span>${anomaly}
    `;
    log.appendChild(item);
  });
}

function toggleStream() {
  const btn = document.getElementById('btn-sim-toggle');
  if (!streamActive) {
    socket.emit('start_stream');
    streamActive = true;
    if (btn) { btn.textContent = '■ Stop Stream'; btn.className = 'btn-sim stop'; }
  } else {
    socket.emit('stop_stream');
    streamActive = false;
    if (btn) { btn.textContent = '▶ Start Live Stream'; btn.className = 'btn-sim start'; }
  }
}

// alias for HTML onclick
window.toggleSimulation = toggleStream;

// ── POLLING ────────────────────────────────────────────
function startPolling() {
  loadKPIs(); refreshBottlenecks(); loadShipments(); loadForecast();
  setInterval(loadKPIs, 15000);
  setInterval(refreshBottlenecks, 20000);
  setInterval(loadShipments, 30000);
  setInterval(() => { if(currentLayer==='heatmap') loadHeatmap(); }, 60000);
}

// ── INIT ───────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  restoreLiveState();
  initSocket();
  initMap();
  initForecastChart();
  initModeSplitChart();
  startPolling();
});
