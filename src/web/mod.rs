use axum::{
    extract::State,
    response::Html,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::signal::Signal;

const MAX_EVENTS: usize = 500;

/// Shared inspector state — cloneable handle to the ring buffer.
#[derive(Clone)]
pub struct Inspector(Arc<Mutex<InspectorState>>);

#[derive(Default)]
struct InspectorState {
    events:       VecDeque<InspectorEvent>,
    source_stats: std::collections::HashMap<String, SourceStat>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InspectorEvent {
    pub kind:   String,
    pub name:   String,
    pub value:  Option<f64>,
    pub labels: std::collections::HashMap<String, String>,
    pub ts_ms:  i64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SourceStat {
    pub metrics: u64,
    pub logs:    u64,
}

impl Inspector {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(InspectorState::default())))
    }

    pub fn record_signal(&self, signal: &Signal) {
        let mut state = self.0.lock().unwrap();
        match signal {
            Signal::Metric(m) => {
                let event = InspectorEvent {
                    kind:   "metric".into(),
                    name:   m.name.clone(),
                    value:  Some(m.value),
                    labels: m.labels.clone(),
                    ts_ms:  m.timestamp_ms,
                };
                state.events.push_back(event);
                if state.events.len() > MAX_EVENTS {
                    state.events.pop_front();
                }
            }
            Signal::Log(l) => {
                let event = InspectorEvent {
                    kind:   "log".into(),
                    name:   l.line.chars().take(80).collect(),
                    value:  None,
                    labels: l.labels.clone(),
                    ts_ms:  l.timestamp_ns / 1_000_000,
                };
                state.events.push_back(event);
                if state.events.len() > MAX_EVENTS {
                    state.events.pop_front();
                }
            }
        }
    }

    pub fn record_source(&self, source: &str, kind: &str) {
        let mut state = self.0.lock().unwrap();
        let stat = state.source_stats.entry(source.to_string()).or_default();
        if kind == "metric" { stat.metrics += 1; }
        if kind == "log"    { stat.logs    += 1; }
    }

    fn get_events(&self) -> Vec<InspectorEvent> {
        self.0.lock().unwrap().events.iter().rev().take(200).cloned().collect()
    }

    fn get_source_stats(&self) -> std::collections::HashMap<String, SourceStat> {
        self.0.lock().unwrap().source_stats.clone()
    }
}

// ---------------------------------------------------------------------------
// Axum web server
// ---------------------------------------------------------------------------

pub async fn serve(inspector: Inspector, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/",              get(root_handler))
        .route("/api/events",    get(events_handler))
        .route("/api/sources",   get(sources_handler))
        .route("/api/healthz",   get(health_handler))
        .with_state(inspector);

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health_handler() -> &'static str { "ok" }

async fn events_handler(State(insp): State<Inspector>) -> Json<Vec<InspectorEvent>> {
    Json(insp.get_events())
}

async fn sources_handler(
    State(insp): State<Inspector>,
) -> Json<std::collections::HashMap<String, SourceStat>> {
    Json(insp.get_source_stats())
}

async fn root_handler() -> Html<String> {
    Html(INSPECTOR_HTML.replace("__VERSION__", env!("CARGO_PKG_VERSION")))
}

// ---------------------------------------------------------------------------
// Inline HTML for the signal inspector UI
// ---------------------------------------------------------------------------

const INSPECTOR_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseAgent — Signal Inspector</title>
<style>
  :root { --bg:#0d1117; --panel:#161b22; --border:#30363d; --fg:#e6edf3; --muted:#7d8590;
          --accent:#58a6ff; --green:#3fb950; --yellow:#d29922; --red:#f85149; }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:var(--bg); color:var(--fg); font-family:'Segoe UI',system-ui,sans-serif; font-size:13px; }
  header { background:var(--panel); border-bottom:1px solid var(--border);
           padding:10px 20px; display:flex; align-items:center; gap:12px; }
  header h1 { font-size:16px; font-weight:600; }
  .badge { background:var(--accent); color:#000; padding:2px 8px; border-radius:10px; font-size:11px; font-weight:600; }
  .version { color:var(--muted); font-size:11px; }
  .layout { display:grid; grid-template-columns:220px 1fr; height:calc(100vh - 45px); }
  .sidebar { background:var(--panel); border-right:1px solid var(--border); padding:12px; overflow-y:auto; }
  .sidebar h2 { font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); margin-bottom:8px; }
  .source-row { display:flex; justify-content:space-between; padding:5px 0;
                border-bottom:1px solid var(--border); font-size:12px; }
  .source-name { font-weight:600; }
  .source-cnt { color:var(--muted); }
  .main { overflow:hidden; display:flex; flex-direction:column; }
  .toolbar { padding:8px 16px; display:flex; gap:8px; align-items:center;
             border-bottom:1px solid var(--border); }
  .toolbar select, .toolbar input { background:var(--panel); border:1px solid var(--border);
    color:var(--fg); padding:3px 8px; border-radius:4px; font-size:12px; }
  .toolbar .pill { padding:2px 10px; border-radius:10px; font-size:11px; cursor:pointer;
                   border:1px solid var(--border); background:var(--panel); color:var(--muted); }
  .toolbar .pill.active { color:var(--accent); border-color:var(--accent); }
  .event-table { flex:1; overflow-y:auto; }
  table { width:100%; border-collapse:collapse; }
  th { text-align:left; padding:4px 10px; color:var(--muted); font-weight:500;
       font-size:11px; border-bottom:1px solid var(--border); position:sticky; top:0;
       background:var(--bg); }
  td { padding:4px 10px; border-bottom:1px solid var(--border); font-size:12px; max-width:400px;
       white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  tr:hover td { background:var(--panel); }
  .kind-metric { color:var(--accent); }
  .kind-log    { color:var(--green); }
  .val { font-family:monospace; }
  .paused { color:var(--yellow); }
  #status { color:var(--muted); font-size:11px; margin-left:auto; }
</style>
</head>
<body>
<header>
  <h1>PulseAgent</h1>
  <span class="badge">Signal Inspector</span>
  <span class="version">v__VERSION__</span>
  <span id="status">connecting…</span>
</header>
<div class="layout">
  <div class="sidebar">
    <h2>Sources</h2>
    <div id="sources-list"><em style="color:var(--muted)">loading…</em></div>
  </div>
  <div class="main">
    <div class="toolbar">
      <button class="pill active" id="pill-all"   onclick="setFilter('')">All</button>
      <button class="pill"        id="pill-metric" onclick="setFilter('metric')">Metrics</button>
      <button class="pill"        id="pill-log"    onclick="setFilter('log')">Logs</button>
      <input id="search" type="text" placeholder="Filter by name…" oninput="render()" style="width:200px">
      <label style="font-size:12px;display:flex;gap:4px;align-items:center;cursor:pointer;">
        <input type="checkbox" id="pause" onchange="togglePause()"> Pause
      </label>
    </div>
    <div class="event-table">
      <table>
        <thead><tr><th>Kind</th><th>Name</th><th>Value</th><th>Labels</th><th>Time</th></tr></thead>
        <tbody id="event-body"></tbody>
      </table>
    </div>
  </div>
</div>
<script>
let events = [];
let filter = '';
let paused = false;
let lastCount = 0;

function setFilter(f) {
  filter = f;
  document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
  const id = f ? 'pill-' + f : 'pill-all';
  document.getElementById(id)?.classList.add('active');
  render();
}

function togglePause() {
  paused = document.getElementById('pause').checked;
}

function render() {
  const search = document.getElementById('search').value.toLowerCase();
  const tbody = document.getElementById('event-body');
  const visible = events.filter(e =>
    (!filter || e.kind === filter) &&
    (!search || e.name.toLowerCase().includes(search))
  ).slice(0, 200);
  tbody.innerHTML = visible.map(e => {
    const lbl = Object.entries(e.labels).map(([k,v]) => `<span style="color:var(--muted)">${k}</span>=<span style="color:var(--fg)">${v}</span>`).join(' ');
    const t = new Date(e.ts_ms).toTimeString().slice(0,8);
    const val = e.value != null ? e.value.toPrecision(6) : '—';
    return `<tr><td class="kind-${e.kind}">${e.kind}</td><td>${e.name}</td><td class="val">${val}</td><td>${lbl}</td><td>${t}</td></tr>`;
  }).join('');
}

async function poll() {
  try {
    const [evResp, srcResp] = await Promise.all([
      fetch('/api/events'),
      fetch('/api/sources'),
    ]);
    const evData  = await evResp.json();
    const srcData = await srcResp.json();
    if (!paused) {
      events = evData;
      render();
    }
    // Sources sidebar
    const sl = document.getElementById('sources-list');
    const entries = Object.entries(srcData);
    if (entries.length === 0) {
      sl.innerHTML = '<em style="color:var(--muted);font-size:12px">no sources active yet</em>';
    } else {
      sl.innerHTML = entries.map(([name, stat]) =>
        `<div class="source-row"><span class="source-name">${name}</span>
         <span class="source-cnt">${stat.metrics}m ${stat.logs}l</span></div>`
      ).join('');
    }
    document.getElementById('status').textContent = `${evData.length} events · ${new Date().toTimeString().slice(0,8)}`;
  } catch (e) {
    document.getElementById('status').textContent = 'disconnected';
  }
  setTimeout(poll, 1000);
}

poll();
</script>
</body>
</html>
"#;
