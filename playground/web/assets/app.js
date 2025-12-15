const state = {
  snapshot: null,
  snapshotReceivedAt: 0,
  snapshotInFlight: false,
  events: [],
  lastSeq: 0,
  selectedNode: null,
  selectedUnit: null,
  lastOwners: new Map(),
  unitChanges: new Set(),
  predictResult: null,
  explainResult: null,
  eventFilter: { search: '', type: 'all' },
};

const EVENT_TYPES = [
  'engine.start',
  'engine.stop',
  'node.add',
  'node.remove',
  'node.restart',
  'node.kill',
  'node.weight',
  'node.health',
  'node.redisFault',
  'shedding.toggle',
  'shedding.release',
  'focus.set',
  'scenario.start',
  'scenario.step',
  'scenario.done',
  'scenario.error',
  'predict.down',
  'explain.unit',
  'error',
];

document.addEventListener('DOMContentLoaded', () => {
  bindGlobalControls();
  connectEvents();
  pollSnapshot();
  setInterval(pollSnapshot, 1000);
});

function bindGlobalControls() {
  document.querySelectorAll('[data-drawer]').forEach((btn) => {
    btn.addEventListener('click', () => openDrawer(btn.dataset.drawer));
  });
  document.getElementById('drawer-close').addEventListener('click', closeDrawer);
  document.getElementById('export-events').addEventListener('click', exportEvents);
  document.getElementById('event-search').addEventListener('input', (e) => {
    state.eventFilter.search = e.target.value.toLowerCase();
    renderEvents();
  });
  document.getElementById('event-type-filter').addEventListener('change', (e) => {
    state.eventFilter.type = e.target.value;
    renderEvents();
  });
  const nodesList = document.getElementById('nodes-list');
  nodesList.addEventListener('click', handleNodeClick);
  nodesList.addEventListener('change', handleNodeChange);
  const assignments = document.getElementById('assignments');
  assignments.addEventListener('click', handleAssignmentClick);
}

async function pollSnapshot() {
  if (state.snapshotInFlight) return;
  state.snapshotInFlight = true;
  try {
    const res = await fetch('/api/v1/snapshot', { cache: 'no-store' });
    if (!res.ok) {
      let msg = `snapshot ${res.status}`;
      try {
        const err = await res.json();
        msg = err.error?.message || msg;
      } catch {}
      throw new Error(msg);
    }
    const snap = await res.json();
    applySnapshot(snap);
  } catch (err) {
    showToast(err?.message || 'snapshot error', 'error');
    console.error(err);
  } finally {
    state.snapshotInFlight = false;
  }
}

function applySnapshot(snap) {
  syncSelections(snap);
  const newOwners = new Map();
  snap.units.forEach((u) => {
    const key = slotKey(u.workload, u.unit);
    newOwners.set(key, u.lease.owner);
    const prev = state.lastOwners.get(key);
    if (prev && prev !== u.lease.owner) {
      state.unitChanges.add(key);
      setTimeout(() => state.unitChanges.delete(key), 1200);
    }
  });
  state.lastOwners = newOwners;
  state.snapshot = snap;
  state.snapshotReceivedAt = Date.now();
  renderHUD();
  renderNodes();
  renderAssignments();
  renderDetails();
}

function syncSelections(snap) {
  if (state.selectedNode) {
    const exists = snap.nodes.some((n) => n.id === state.selectedNode);
    if (!exists) state.selectedNode = null;
  }
  if (state.selectedUnit) {
    const exists = snap.units.some(
      (u) => u.workload === state.selectedUnit.workload && u.unit === state.selectedUnit.unit,
    );
    if (!exists) state.selectedUnit = null;
  }
}

function connectEvents() {
  const source = new EventSource(`/api/v1/events?since=${state.lastSeq}`);
  EVENT_TYPES.forEach((t) => {
    source.addEventListener(t, (evt) => {
      const payload = JSON.parse(evt.data);
      ingestEvent(payload);
    });
  });
  source.onerror = () => {
    source.close();
    setTimeout(connectEvents, 1200);
  };
}

function ingestEvent(ev) {
  state.lastSeq = Math.max(state.lastSeq, ev.seq);
  state.events.push(ev);
  if (state.events.length > 600) {
    state.events = state.events.slice(state.events.length - 600);
  }
  if (ev.type === 'error') {
    showToast(ev.message, 'error');
  }
  renderEvents();
  if (ev.type.startsWith('node.') && ev.fields && ev.fields.nodeID) {
    flashNode(ev.fields.nodeID);
  }
}

function renderHUD() {
  if (!state.snapshot) return;
  const snap = state.snapshot;
  document.getElementById('hud-mode').textContent = `mode: ${snap.mode}`;
  document.getElementById('hud-redis').textContent = `redis: ${snap.redisAddr}`;
  document.getElementById('hud-cluster').textContent = `cluster: ${snap.clusterID}`;
  document.getElementById('metric-churn').textContent = snap.metrics.churnPerMinute;
  document.getElementById('metric-convergence').textContent = `${snap.metrics.convergencePct.toFixed(1)}%`;
  document.getElementById('metric-owned').textContent = `${snap.metrics.ownedUnitsTotal} / ${snap.metrics.desiredUnitsTotal}`;
  document.getElementById('metric-misaligned').textContent = snap.metrics.misalignedUnitsTotal;

  const focusSelect = document.getElementById('focus-select');
  focusSelect.innerHTML = '<option value="">all workloads</option>';
  snap.workloads.forEach((wl) => {
    const opt = document.createElement('option');
    opt.value = wl.name;
    opt.textContent = wl.name;
    if (snap.focusWorkload === wl.name) opt.selected = true;
    focusSelect.appendChild(opt);
  });
  focusSelect.value = snap.focusWorkload || '';
  focusSelect.onchange = () => setFocus(focusSelect.value);
}

function renderNodes() {
  if (!state.snapshot) return;
  const list = document.getElementById('nodes-list');
  list.innerHTML = state.snapshot.nodes
    .map((n) => {
      const workloads = (n.workloads || [])
        .map(
          (w) =>
            `<div class="node-bar" title="${w.name} owned ${w.ownedUnits} / desired ${w.desiredUnits}">
              <span style="width:${barPct(w.ownedUnits, Math.max(w.desiredUnits, 1))}%"></span>
            </div>`,
        )
        .join('');
      const stateClass = n.state === 'unhealthy' ? 'pill unhealthy' : n.backoffActive ? 'pill backoff' : 'pill ok';
      const redis = n.redisFault ? '<span class="pill unhealthy">redis fault</span>' : '';
      return `<div class="node-card ${state.selectedNode === n.id ? 'selected' : ''}" data-node-id="${n.id}">
        <div class="node-header">
          <div class="node-id">${n.shortID} <span class="muted">${n.weight.toFixed(2)}</span></div>
          <div class="${stateClass}">${n.state}${n.backoffActive ? ' / backoff' : ''}</div>
        </div>
        <div class="node-stats">
          <span>owned ${n.ownedUnits}</span>
          <span>desired ${n.desiredUnits}</span>
          <span>missing ${n.missingDesiredUnits}</span>
          <span>extra ${n.extraOwnedUnits}</span>
        </div>
        <div class="node-bars">${workloads}</div>
        <div class="node-controls">
          <div class="row">
            <label>weight</label>
            <input class="weight-input" type="number" step="0.1" min="0.1" value="${n.weight.toFixed(2)}">
            <button class="btn ghost" data-action="weight">apply</button>
          </div>
          <label><input type="checkbox" data-action="health" ${n.healthy ? 'checked' : ''}> healthy</label>
          <label><input type="checkbox" data-action="redis" ${n.redisFault ? 'checked' : ''}> redis fault</label>
          <button class="btn ghost" data-action="restart">restart</button>
          <button class="btn ghost" data-action="remove">remove</button>
          <button class="btn ghost" data-action="kill">kill</button>
        </div>
        <div class="node-stats">${redis}</div>
      </div>`;
    })
    .join('');
}

function handleNodeClick(e) {
  const card = e.target.closest('.node-card');
  if (!card) return;
  const nodeID = card.dataset.nodeId;
  const action = e.target.dataset.action;
  if (action === 'restart') return apiPost('/api/v1/nodes/restart', { nodeID });
  if (action === 'remove') return apiPost('/api/v1/nodes/remove', { nodeID, graceful: true });
  if (action === 'kill') return apiPost('/api/v1/nodes/kill', { nodeID });
  if (action === 'weight') {
    const weight = parseFloat(card.querySelector('.weight-input').value);
    return apiPost('/api/v1/nodes/weight', { nodeID, weight });
  }
  if (action) return;
  state.selectedNode = nodeID;
  state.selectedUnit = null;
  state.predictResult = null;
  state.explainResult = null;
  renderDetails();
  renderNodes();
}

function handleNodeChange(e) {
  const action = e.target.dataset.action;
  if (!action) return;
  const card = e.target.closest('.node-card');
  const nodeID = card?.dataset.nodeId;
  if (!nodeID) return;
  if (action === 'health') return apiPost('/api/v1/nodes/health', { nodeID, healthy: e.target.checked });
  if (action === 'redis') return apiPost('/api/v1/nodes/redisFault', { nodeID, fail: e.target.checked });
}

function renderAssignments() {
  if (!state.snapshot) return;
  const snap = state.snapshot;
  const focus = snap.focusWorkload || '';
  const workloads = focus ? snap.workloads.filter((w) => w.name === focus) : snap.workloads;
  const container = document.getElementById('assignments');
  container.innerHTML = workloads
    .map((wl) => {
      const units = snap.units.filter((u) => u.workload === wl.name);
      const misaligned = units.filter((u) => !u.aligned).length;
      const cells = units
        .map((u) => {
          const ttl = interpolatedTTL(u.lease.ttlMs);
          const cd = interpolatedTTL(u.cooldown.ttlMs);
          const ttlPct = pct(ttl, snap.config.leaseTTLms);
          const cdPct = pct(cd, snap.config.slotMoveCooldownms);
          const key = slotKey(u.workload, u.unit);
          const highlight = state.unitChanges.has(key) ? 'pulse' : '';
          const mis = u.aligned ? '' : 'misaligned';
          const desired = u.desiredOwner ? short(u.desiredOwner) : '-';
          const hrwTopK = u.hrwTopK || [];
          const title = [
            `lease ${u.lease.key}`,
            `cooldown ${u.cooldown.key}`,
            `owner ${u.lease.owner || '-'}`,
            `desired ${u.desiredOwner || '-'}`,
            `ttl ${ttl}ms`,
            `cooldown ${cd}ms`,
            `hrw: ${hrwTopK.map((h) => `${h.shortID}(${h.score.toFixed(3)})`).join(', ')}`,
          ].join('\n');
          return `<div class="unit ${mis} ${highlight} ${state.selectedUnit && state.selectedUnit.workload === u.workload && state.selectedUnit.unit === u.unit ? 'selected' : ''}" data-slot="${key}" title="${title}">
            <div class="owner">${short(u.lease.owner) || 'unowned'}</div>
            <div class="desired">desired: ${desired}</div>
            ${!u.aligned ? '<div class="tag">misaligned</div>' : ''}
            <div class="ttl-bars">
              <div class="ttl-bar"><span style="width:${ttlPct}%"></span></div>
              <div class="cooldown-bar"><span style="width:${cdPct}%"></span></div>
            </div>
          </div>`;
        })
        .join('');
      return `<div class="assignments-group">
        <div class="assignments-header">
          <div>${wl.name} - ${wl.units} units</div>
          <div class="muted">misaligned ${misaligned}</div>
        </div>
        <div class="unit-grid">${cells}</div>
      </div>`;
    })
    .join('');
}

function handleAssignmentClick(e) {
  const cell = e.target.closest('.unit');
  if (!cell || !cell.dataset.slot) return;
  const [workload, unit] = cell.dataset.slot.split('|');
  state.selectedUnit = { workload, unit: Number(unit) };
  state.selectedNode = null;
  state.predictResult = null;
  state.explainResult = null;
  renderDetails();
  renderAssignments();
}

function renderDetails() {
  const el = document.getElementById('details-body');
  if (!state.snapshot) {
    el.innerHTML = '<div class="muted">waiting for data...</div>';
    return;
  }
  if (state.selectedUnit) {
    const unit = state.snapshot.units.find(
      (u) => u.workload === state.selectedUnit.workload && u.unit === state.selectedUnit.unit,
    );
    if (!unit) {
      el.innerHTML = '<div class="muted">select a unit</div>';
      return;
    }
    const ttl = interpolatedTTL(unit.lease.ttlMs);
    const cd = interpolatedTTL(unit.cooldown.ttlMs);
    const rankingRows = (unit.hrwTopK || [])
      .map(
        (h, idx) =>
          `<tr><td>${idx + 1}</td><td>${h.shortID}</td><td>${h.weight.toFixed(2)}</td><td>${h.score.toFixed(
            4,
          )}</td></tr>`,
      )
      .join('');
    const explainSection = state.explainResult
      ? renderExplain(state.explainResult)
      : '<div class="muted">run explain to see full ranking</div>';
    const predictSection = state.predictResult
      ? renderPredict(state.predictResult)
      : '<div class="muted">predict the impact of removing a node</div>';
    el.innerHTML = `
      <div class="detail-block">
        <h4>${unit.workload} - unit ${unit.unit}</h4>
        <div>lease: <code>${unit.lease.key}</code> owner ${unit.lease.owner || '-'}</div>
        <div>desired: ${unit.desiredOwner || '-'}</div>
        <div>ttl: ${ttl}ms / cooldown: ${cd}ms (${unit.cooldown.active ? 'active' : 'idle'})</div>
        <div class="muted">cooldown key: <code>${unit.cooldown.key}</code></div>
      </div>
      <div class="detail-block">
        <h4>HRW top ${unit.hrwTopK.length}</h4>
        <table class="table">
          <thead><tr><th>#</th><th>node</th><th>w</th><th>score</th></tr></thead>
          <tbody>${rankingRows}</tbody>
        </table>
        <div class="row" style="margin-top:8px;display:flex;gap:8px;align-items:center;">
          <input id="explain-topn" type="number" min="1" value="10" style="width:70px;">
          <button class="btn ghost" id="explain-btn">Explain (top N)</button>
        </div>
      </div>
      <div class="detail-block">
        <h4>Predict down</h4>
        <div class="row" style="display:flex;gap:8px;align-items:center;">
          <select id="predict-node"></select>
          <button class="btn ghost" id="predict-btn">Predict</button>
        </div>
        <div id="predict-output">${predictSection}</div>
      </div>
      <div class="detail-block">
        ${explainSection}
      </div>
    `;
    const nodeSelect = el.querySelector('#predict-node');
    nodeSelect.innerHTML = state.snapshot.nodes.map((n) => `<option value="${n.id}">${n.shortID}</option>`).join('');
    el.querySelector('#predict-btn').onclick = () => {
      const nodeID = nodeSelect.value;
      apiPost('/api/v1/predict/down', { nodeID }).then((res) => {
        if (res) {
          state.predictResult = res;
          renderDetails();
        }
      });
    };
    el.querySelector('#explain-btn').onclick = () => {
      const topN = Number(el.querySelector('#explain-topn').value || 10);
      apiPost('/api/v1/explain/unit', {
        workload: unit.workload,
        unit: unit.unit,
        topN,
      }).then((res) => {
        if (res) {
          if (topN > 0 && res.ranking.length > topN) {
            res.ranking = res.ranking.slice(0, topN);
          }
          state.explainResult = res;
          renderDetails();
        }
      });
    };
  } else if (state.selectedNode) {
    const node = state.snapshot.nodes.find((n) => n.id === state.selectedNode);
    if (!node) {
      el.innerHTML = '<div class="muted">select a node</div>';
      return;
    }
    const wlRows = (node.workloads || [])
      .map((w) => `<tr><td>${w.name}</td><td>${w.ownedUnits}</td><td>${w.desiredUnits}</td></tr>`)
      .join('');
    el.innerHTML = `
      <div class="detail-block">
        <h4>Node ${node.shortID}</h4>
        <div>state ${node.state} ${node.backoffActive ? '/ backoff' : ''}</div>
        <div>weight ${node.weight.toFixed(2)} / healthy ${node.healthy}</div>
        <div>redis fault ${node.redisFault}</div>
      </div>
      <div class="detail-block">
        <h4>Workloads</h4>
        <table class="table">
          <thead><tr><th>name</th><th>owned</th><th>desired</th></tr></thead>
          <tbody>${wlRows}</tbody>
        </table>
        <div class="row" style="display:flex;gap:8px;align-items:center;margin-top:8px;">
          <button class="btn ghost" onclick="apiPost('/api/v1/nodes/restart', {nodeID: '${node.id}'})">restart</button>
          <button class="btn ghost" onclick="apiPost('/api/v1/nodes/remove', {nodeID: '${node.id}', graceful:true})">remove</button>
          <button class="btn ghost" onclick="apiPost('/api/v1/nodes/kill', {nodeID: '${node.id}'})">kill</button>
        </div>
      </div>
    `;
  } else {
    el.innerHTML = '<div class="muted">Select a node or unit to inspect leases, cooldowns, and HRW ranking.</div>';
  }
}

function renderExplain(res) {
  const ranking = res.ranking
    .map(
      (r) =>
        `<tr><td>${r.rank}</td><td>${r.shortID}</td><td>${r.weight.toFixed(2)}</td><td>${r.score.toFixed(
          4,
        )}</td></tr>`,
    )
    .join('');
  return `
    <h4>Explain ${res.workload}[${res.unit}]</h4>
    <div class="muted">slot ${res.slotKey} / generated ${new Date(res.generatedAtUnixMs).toLocaleTimeString()}</div>
    <div>lease ${res.currentLease.owner || '-'} ttl ${res.currentLease.ttlMs}ms</div>
    <div>cooldown ${res.cooldown.ttlMs}ms (${res.cooldown.active ? 'active' : 'idle'})</div>
    <table class="table">
      <thead><tr><th>#</th><th>node</th><th>w</th><th>score</th></tr></thead>
      <tbody>${ranking}</tbody>
    </table>
  `;
}

function renderPredict(res) {
  const moves = res.moves
    .map(
      (m) =>
        `<div>${m.workload}[${m.unit}] ${short(m.fromOwner)} -> ${short(m.toOwner)} <span class="muted">(${m.reason})</span></div>`,
    )
    .join('');
  const summary = res.summary.byToOwner.map((b) => `${short(b.nodeID)}: ${b.count}`).join(', ');
  return `
    <div class="muted">generated ${new Date(res.generatedAtUnixMs).toLocaleTimeString()}</div>
    <div>units impacted: ${res.summary.unitsImpacted}</div>
    <div class="muted">destinations: ${summary || 'n/a'}</div>
    <div class="muted">${res.removedFromLiveSet ? 'node removed from live set' : 'node not present'}</div>
    <div style="margin-top:6px">${moves || 'no moves predicted'}</div>
  `;
}

function renderEvents() {
  const list = document.getElementById('events-list');
  if (!list) return;
  const { search, type } = state.eventFilter;
  const filtered = state.events.filter((e) => {
    const msg = `${e.type} ${e.message} ${JSON.stringify(e.fields || {})}`.toLowerCase();
    const matchesSearch = search ? msg.includes(search) : true;
    return matchesSearch && matchEventType(e.type, type);
  });
  list.innerHTML = filtered
    .slice(-200)
    .map((ev) => {
      const ts = new Date(ev.atUnixMs).toLocaleTimeString();
      const chip = chipFor(ev.type);
      return `<div class="event-row">
        <div class="event-meta">${ts}<br><span class="log-chip ${chip.cls}">${chip.label}</span></div>
        <div class="event-msg">${ev.message}</div>
      </div>`;
    })
    .join('');
}

function matchEventType(evType, filter) {
  if (filter === 'all') return true;
  if (filter === 'engine') return evType.startsWith('engine');
  if (filter === 'node') return evType.startsWith('node');
  if (filter === 'scenario') return evType.startsWith('scenario');
  if (filter === 'predict') return evType.startsWith('predict');
  if (filter === 'explain') return evType.startsWith('explain');
  if (filter === 'error') return evType === 'error';
  return true;
}

function chipFor(type) {
  if (type.startsWith('node')) return { cls: 'node', label: type };
  if (type.startsWith('scenario')) return { cls: 'scenario', label: type };
  if (type.startsWith('predict')) return { cls: 'predict', label: type };
  if (type.startsWith('explain')) return { cls: 'explain', label: type };
  if (type === 'error') return { cls: 'error', label: type };
  return { cls: 'engine', label: type };
}

function exportEvents() {
  const blob = new Blob([JSON.stringify(state.events, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'rendezgo-events.json';
  a.click();
  URL.revokeObjectURL(url);
}

function openDrawer(mode) {
  const drawer = document.getElementById('drawer');
  const title = document.getElementById('drawer-title');
  const body = document.getElementById('drawer-body');
  drawer.classList.remove('hidden');
  drawer.classList.add('visible');
  if (mode === 'add') {
    title.textContent = 'Add nodes';
    body.innerHTML = `
      <div class="drawer-body">
        <div class="field"><label>Count</label><input id="add-count" type="number" min="1" value="1"></div>
        <div class="field"><label>Weight</label><input id="add-weight" type="number" step="0.1" min="0.1" value="1"></div>
        <button class="btn ghost" id="add-node-confirm">Add</button>
      </div>`;
    document.getElementById('add-node-confirm').onclick = () => {
      const count = Number(document.getElementById('add-count').value || 1);
      const weight = Number(document.getElementById('add-weight').value || 1);
      apiPost('/api/v1/nodes/add', { count, weight });
    };
  } else if (mode === 'scenario') {
    title.textContent = 'Scenarios';
    body.innerHTML = `
      <div class="drawer-body">
        <div class="field"><label>Built-in</label>
          <div class="scenario-buttons">
            ${['scale-out','pod-death','redis-instability','weight-skew','cross-cluster','lease-flapping']
              .map((s) => `<button class="btn ghost" data-scenario="${s}">${s}</button>`).join('')}
          </div>
        </div>
        <div class="field"><label>Upload scenario (.yaml/.json)</label>
          <input id="scenario-file" type="file" accept=".yaml,.yml,.json">
        </div>
      </div>`;
    body.querySelectorAll('[data-scenario]').forEach((btn) => {
      btn.onclick = () => apiPost('/api/v1/scenario/run', { name: btn.dataset.scenario });
    });
    const file = document.getElementById('scenario-file');
    file.onchange = async () => {
      if (!file.files.length) return;
      const form = new FormData();
      form.append('file', file.files[0]);
      try {
        const res = await fetch('/api/v1/scenario/upload', { method: 'POST', body: form });
        if (!res.ok) throw new Error('upload failed');
        showToast('scenario uploaded', 'success');
      } catch {
        showToast('scenario upload failed', 'error');
      }
    };
  } else if (mode === 'settings') {
    const cfg = state.snapshot ? state.snapshot.config : null;
    title.textContent = 'Settings';
    body.innerHTML = `
      <div class="drawer-body">
        <div class="field"><label>Shedding enabled</label>
          <input type="checkbox" id="setting-shedding" ${cfg && cfg.sheddingEnabled ? 'checked' : ''}>
        </div>
        <div class="field"><label>Shedding release / interval</label>
          <input type="number" id="setting-release" min="0" value="${cfg ? cfg.sheddingRelease : 0}">
        </div>
        <button class="btn ghost" id="settings-apply">Apply</button>
      </div>`;
    document.getElementById('settings-apply').onclick = async () => {
      const enabled = document.getElementById('setting-shedding').checked;
      const perInterval = Number(document.getElementById('setting-release').value || 0);
      await apiPost('/api/v1/shedding/toggle', { enabled });
      await apiPost('/api/v1/shedding/release', { perInterval });
    };
  } else if (mode === 'help') {
    title.textContent = 'Help';
    body.innerHTML = `
      <div class="drawer-body">
        <p>Glass HUD to inspect leases, cooldowns, HRW rankings, and node health.</p>
        <ul>
          <li>Click nodes or units to open details.</li>
          <li>Top focus dropdown filters workloads.</li>
          <li>Event stream stays live; export JSON anytime.</li>
        </ul>
      </div>`;
  }
}

function closeDrawer() {
  const drawer = document.getElementById('drawer');
  drawer.classList.add('hidden');
  drawer.classList.remove('visible');
}

async function apiPost(url, payload) {
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      const msg = err.error?.message || 'request failed';
      showToast(msg, 'error');
      return null;
    }
    if (res.headers.get('content-type')?.includes('application/json')) {
      return res.json();
    }
    return {};
  } catch (err) {
    showToast(err.message, 'error');
    return null;
  }
}

function setFocus(workload) {
  apiPost('/api/v1/focus', { workload });
}

function interpolatedTTL(ttlMs) {
  if (!ttlMs) return 0;
  const elapsed = Date.now() - state.snapshotReceivedAt;
  return Math.max(0, Math.round(ttlMs - elapsed));
}

function pct(value, max) {
  if (!max) return 0;
  return Math.min(100, Math.max(0, (value / max) * 100));
}

function barPct(value, max) {
  if (!max) return 0;
  const pctVal = (value / max) * 100;
  return Math.max(4, Math.min(100, pctVal));
}

function slotKey(workload, unit) {
  return `${workload}|${unit}`;
}

function short(id) {
  if (!id) return '-';
  const parts = id.split(':');
  return parts[parts.length - 1];
}

function flashNode(nodeID) {
  const card = document.querySelector(`.node-card[data-node-id="${nodeID}"]`);
  if (!card) return;
  card.classList.add('pulse');
  setTimeout(() => card.classList.remove('pulse'), 900);
}

function showToast(msg, kind = 'error') {
  const container = document.getElementById('toast');
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.textContent = msg;
  container.appendChild(el);
  setTimeout(() => {
    el.style.opacity = '0';
    setTimeout(() => el.remove(), 400);
  }, 2200);
}
