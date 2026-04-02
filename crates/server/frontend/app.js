// Arneb Web UI

let currentView = 'dashboard';
let refreshTimer = null;

function showView(view) {
  document.querySelectorAll('.view').forEach(v => v.style.display = 'none');
  document.querySelectorAll('nav a').forEach(a => a.classList.remove('active'));
  document.getElementById(view).style.display = 'block';
  document.getElementById('nav-' + view).classList.add('active');
  currentView = view;
  refresh();
}

async function fetchJson(url) {
  const res = await fetch(url);
  return res.json();
}

async function refreshDashboard() {
  const [queries, cluster] = await Promise.all([
    fetchJson('/api/v1/queries'),
    fetchJson('/api/v1/cluster')
  ]);

  const states = { Running: 0, Finished: 0, Failed: 0 };
  queries.queries.forEach(q => {
    if (states[q.state] !== undefined) states[q.state]++;
  });

  document.getElementById('running-count').textContent = states.Running;
  document.getElementById('finished-count').textContent = states.Finished;
  document.getElementById('failed-count').textContent = states.Failed;
  document.getElementById('worker-count').textContent = cluster.worker_count;

  const tbody = document.querySelector('#recent-queries tbody');
  tbody.innerHTML = queries.queries.slice(0, 10).map(q =>
    `<tr>
      <td>${q.query_id.substring(0, 8)}</td>
      <td class="sql-preview">${escapeHtml(q.sql)}</td>
      <td><span class="state state-${q.state}">${q.state}</span></td>
    </tr>`
  ).join('');
}

async function refreshQueries() {
  const filter = document.getElementById('state-filter').value;
  const url = filter ? `/api/v1/queries?state=${filter}` : '/api/v1/queries';
  const data = await fetchJson(url);

  const tbody = document.querySelector('#query-list tbody');
  tbody.innerHTML = data.queries.map(q =>
    `<tr>
      <td>${q.query_id.substring(0, 8)}</td>
      <td class="sql-preview">${escapeHtml(q.sql)}</td>
      <td><span class="state state-${q.state}">${q.state}</span></td>
      <td>${q.state === 'Running' || q.state === 'Queued'
        ? `<button class="cancel" onclick="cancelQuery('${q.query_id}')">Cancel</button>`
        : ''}</td>
    </tr>`
  ).join('');
}

async function refreshCluster() {
  const [info, workers] = await Promise.all([
    fetchJson('/api/v1/info'),
    fetchJson('/api/v1/cluster/workers')
  ]);

  document.getElementById('server-info').innerHTML = `
    <strong>Version:</strong> ${info.version} &nbsp;
    <strong>Uptime:</strong> ${formatDuration(info.uptime_secs)} &nbsp;
    <strong>Role:</strong> ${info.role}
  `;

  const list = document.getElementById('worker-list');
  if (workers.length === 0) {
    if (info.role === 'standalone') {
      list.innerHTML = '<p style="color:#718096">Running in standalone mode (no separate workers)</p>';
    } else {
      list.innerHTML = '<p style="color:#718096">No workers connected</p>';
    }
  } else {
    list.innerHTML = workers.map(w =>
      `<div class="worker-card">
        <div>
          <div class="worker-id">${w.worker_id}</div>
          <div class="worker-address">${w.address}</div>
        </div>
        <div>
          <span class="state ${w.alive ? 'state-Running' : 'state-Failed'}">${w.alive ? 'ALIVE' : 'DEAD'}</span>
          &nbsp; ${w.max_splits} splits
        </div>
      </div>`
    ).join('');
  }
}

async function cancelQuery(id) {
  await fetch(`/api/v1/queries/${id}`, { method: 'DELETE' });
  refresh();
}

function refresh() {
  if (currentView === 'dashboard') refreshDashboard();
  else if (currentView === 'queries') refreshQueries();
  else if (currentView === 'cluster') refreshCluster();
}

function formatDuration(secs) {
  if (secs < 60) return secs + 's';
  if (secs < 3600) return Math.floor(secs/60) + 'm ' + (secs%60) + 's';
  return Math.floor(secs/3600) + 'h ' + Math.floor((secs%3600)/60) + 'm';
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Auto-refresh every 2 seconds
refresh();
refreshTimer = setInterval(refresh, 2000);
