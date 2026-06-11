use axum::{
    extract::{Query, State},
    response::Html,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use crate::lint::LintFinding;
use crate::signal::{Labels, Signal};

const MAX_EVENTS: usize = 500;
const MAX_DROPS: usize = 300;
/// Cap on fingerprints tracked per metric for the live cardinality sampler.
const CARDINALITY_SAMPLE_CAP: usize = 5000;
/// Observed series count above which a metric is flagged as a cardinality risk.
const CARDINALITY_WARN_THRESHOLD: usize = 200;

/// Shared inspector state — cloneable handle to the ring buffer.
#[derive(Clone)]
pub struct Inspector(Arc<Mutex<InspectorState>>);

#[derive(Default)]
struct InspectorState {
    events: VecDeque<InspectorEvent>,
    source_stats: HashMap<String, SourceStat>,
    drops: VecDeque<DropEvent>,
    stage_stats: HashMap<String, StageStat>,
    topology: Topology,
    lint: Vec<LintFinding>,
    /// metric name → sampled set of label fingerprints (live cardinality)
    observed: HashMap<String, HashSet<u64>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InspectorEvent {
    pub kind: String,
    pub name: String,
    pub value: Option<f64>,
    pub labels: HashMap<String, String>,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SourceStat {
    pub metrics: u64,
    pub logs: u64,
    pub traces: u64,
}

/// A signal that was dropped by a processor stage, with the reason — powers
/// the live debugger's "why isn't this metric showing up?" flow.
#[derive(Debug, Clone, Serialize)]
pub struct DropEvent {
    pub kind: String,
    pub name: String,
    pub labels: HashMap<String, String>,
    pub stage: String,
    pub reason: String,
    pub ts_ms: i64,
}

/// Throughput counters for one pipeline stage.
#[derive(Debug, Clone, Serialize, Default)]
pub struct StageStat {
    pub passed: u64,
    pub dropped: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct Topology {
    pub nodes: Vec<TopoNode>,
    pub edges: Vec<TopoEdge>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopoNode {
    pub id: String,
    /// "source" | "processor" | "target"
    pub kind: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopoEdge {
    pub from: String,
    pub to: String,
}

/// Combined pipeline view returned to the debugger UI.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineView {
    pub nodes: Vec<TopoNode>,
    pub edges: Vec<TopoEdge>,
    pub stages: HashMap<String, StageStat>,
    pub sources: HashMap<String, SourceStat>,
}

impl Inspector {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(InspectorState::default())))
    }

    pub fn record_signal(&self, signal: &Signal) {
        let mut state = self.0.lock().unwrap();
        match signal {
            Signal::Metric(m) => {
                // Sample live cardinality per metric (bounded).
                let fp = fingerprint(&m.labels);
                let entry = state.observed.entry(m.name.clone()).or_default();
                if entry.len() < CARDINALITY_SAMPLE_CAP {
                    entry.insert(fp);
                }
                let event = InspectorEvent {
                    kind: "metric".into(),
                    name: m.name.clone(),
                    value: Some(m.value),
                    labels: m.labels.clone(),
                    ts_ms: m.timestamp_ms,
                };
                state.events.push_back(event);
                if state.events.len() > MAX_EVENTS {
                    state.events.pop_front();
                }
            }
            Signal::Log(l) => {
                let event = InspectorEvent {
                    kind: "log".into(),
                    name: l.line.chars().take(80).collect(),
                    value: None,
                    labels: l.labels.clone(),
                    ts_ms: l.timestamp_ns / 1_000_000,
                };
                state.events.push_back(event);
                if state.events.len() > MAX_EVENTS {
                    state.events.pop_front();
                }
            }
            Signal::Trace(t) => {
                let event = InspectorEvent {
                    kind: "trace".into(),
                    name: format!("{} spans", t.span_count),
                    value: Some(t.span_count as f64),
                    labels: HashMap::new(),
                    ts_ms: t.received_ms,
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
        if kind == "metric" {
            stat.metrics += 1;
        }
        if kind == "log" {
            stat.logs += 1;
        }
        if kind == "trace" {
            stat.traces += 1;
        }
    }

    /// Record that a signal passed a given pipeline stage.
    pub fn record_stage_pass(&self, stage: &str) {
        let mut state = self.0.lock().unwrap();
        state
            .stage_stats
            .entry(stage.to_string())
            .or_default()
            .passed += 1;
    }

    /// Record that a signal was dropped at `stage` for `reason`.
    pub fn record_drop(&self, signal: &Signal, stage: &str, reason: &str) {
        let mut state = self.0.lock().unwrap();
        state
            .stage_stats
            .entry(stage.to_string())
            .or_default()
            .dropped += 1;

        let (kind, name, labels, ts_ms) = match signal {
            Signal::Metric(m) => (
                "metric".to_string(),
                m.name.clone(),
                m.labels.clone(),
                m.timestamp_ms,
            ),
            Signal::Log(l) => (
                "log".to_string(),
                l.line.chars().take(80).collect(),
                l.labels.clone(),
                l.timestamp_ns / 1_000_000,
            ),
            Signal::Trace(t) => (
                "trace".to_string(),
                format!("{} spans", t.span_count),
                HashMap::new(),
                t.received_ms,
            ),
        };
        state.drops.push_back(DropEvent {
            kind,
            name,
            labels,
            stage: stage.to_string(),
            reason: reason.to_string(),
            ts_ms,
        });
        if state.drops.len() > MAX_DROPS {
            state.drops.pop_front();
        }
    }

    pub fn set_topology(&self, topology: Topology) {
        self.0.lock().unwrap().topology = topology;
    }

    pub fn set_lint(&self, findings: Vec<LintFinding>) {
        self.0.lock().unwrap().lint = findings;
    }

    fn get_events(&self) -> Vec<InspectorEvent> {
        self.0
            .lock()
            .unwrap()
            .events
            .iter()
            .rev()
            .take(200)
            .cloned()
            .collect()
    }

    fn get_source_stats(&self) -> HashMap<String, SourceStat> {
        self.0.lock().unwrap().source_stats.clone()
    }

    fn get_pipeline(&self) -> PipelineView {
        let state = self.0.lock().unwrap();
        PipelineView {
            nodes: state.topology.nodes.clone(),
            edges: state.topology.edges.clone(),
            stages: state.stage_stats.clone(),
            sources: state.source_stats.clone(),
        }
    }

    /// Drop events, optionally filtered to a metric name / log substring.
    fn get_drops(&self, name_filter: Option<&str>) -> Vec<DropEvent> {
        let state = self.0.lock().unwrap();
        state
            .drops
            .iter()
            .rev()
            .filter(|d| name_filter.map(|n| d.name.contains(n)).unwrap_or(true))
            .take(100)
            .cloned()
            .collect()
    }

    /// Static lint findings merged with live cardinality warnings.
    fn get_lint(&self) -> Vec<LintFinding> {
        let state = self.0.lock().unwrap();
        let mut findings = state.lint.clone();
        for (metric, series) in &state.observed {
            if series.len() >= CARDINALITY_WARN_THRESHOLD {
                findings.push(LintFinding {
                    severity: "warning".into(),
                    component: format!("metric:{metric}"),
                    message: format!(
                        "Observed {} distinct series for {metric} in the live sample — \
                         this is a cardinality hotspot. Consider dropping a high-cardinality \
                         label with a relabel or transform rule.",
                        series.len()
                    ),
                });
            }
        }
        findings
    }
}

/// Order-independent fingerprint of a label set.
fn fingerprint(labels: &Labels) -> u64 {
    use std::hash::Hash;
    let mut pairs: Vec<(&String, &String)> = labels.iter().collect();
    pairs.sort_by_key(|(k, _)| k.as_str());
    let mut h = std::collections::hash_map::DefaultHasher::new();
    pairs.hash(&mut h);
    std::hash::Hasher::finish(&h)
}

// ---------------------------------------------------------------------------
// Axum web server
// ---------------------------------------------------------------------------

pub async fn serve(inspector: Inspector, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/api/events", get(events_handler))
        .route("/api/sources", get(sources_handler))
        .route("/api/pipeline", get(pipeline_handler))
        .route("/api/drops", get(drops_handler))
        .route("/api/lint", get(lint_handler))
        .route("/api/healthz", get(health_handler))
        .with_state(inspector);

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health_handler() -> &'static str {
    "ok"
}

async fn events_handler(State(insp): State<Inspector>) -> Json<Vec<InspectorEvent>> {
    Json(insp.get_events())
}

async fn sources_handler(State(insp): State<Inspector>) -> Json<HashMap<String, SourceStat>> {
    Json(insp.get_source_stats())
}

async fn pipeline_handler(State(insp): State<Inspector>) -> Json<PipelineView> {
    Json(insp.get_pipeline())
}

#[derive(serde::Deserialize)]
struct DropsQuery {
    name: Option<String>,
}

async fn drops_handler(
    State(insp): State<Inspector>,
    Query(q): Query<DropsQuery>,
) -> Json<Vec<DropEvent>> {
    Json(insp.get_drops(q.name.as_deref().filter(|s| !s.is_empty())))
}

async fn lint_handler(State(insp): State<Inspector>) -> Json<Vec<LintFinding>> {
    Json(insp.get_lint())
}

async fn root_handler() -> Html<String> {
    Html(INSPECTOR_HTML.replace("__VERSION__", env!("CARGO_PKG_VERSION")))
}

// ---------------------------------------------------------------------------
// Inline HTML for the signal inspector UI
// ---------------------------------------------------------------------------

const INSPECTOR_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseAgent — Live Debugger</title>
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
  #status { color:var(--muted); font-size:11px; margin-left:auto; }
  nav.tabs { background:var(--panel); border-bottom:1px solid var(--border); display:flex; gap:4px; padding:0 12px; }
  nav.tabs button { background:none; border:none; color:var(--muted); padding:9px 14px; cursor:pointer;
    font-size:13px; border-bottom:2px solid transparent; }
  nav.tabs button.active { color:var(--accent); border-bottom-color:var(--accent); }
  .view { display:none; height:calc(100vh - 92px); }
  .view.active { display:block; }

  /* Signals view */
  .layout { display:grid; grid-template-columns:220px 1fr; height:100%; }
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

  /* Pipeline view */
  .pipeline-wrap { padding:24px; overflow:auto; height:100%; }
  .flow { display:flex; align-items:flex-start; gap:0; min-width:max-content; }
  .col { display:flex; flex-direction:column; gap:14px; }
  .col-label { font-size:10px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); margin-bottom:6px; text-align:center; }
  .node { background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:10px 14px;
          min-width:150px; }
  .node.source { border-left:3px solid var(--green); }
  .node.processor { border-left:3px solid var(--accent); }
  .node.target { border-left:3px solid var(--yellow); }
  .node .nname { font-weight:600; font-size:12px; }
  .node .nstat { color:var(--muted); font-size:11px; margin-top:4px; font-family:monospace; }
  .node .ndrop { color:var(--red); }
  .arrow { display:flex; align-items:center; padding:0 4px; align-self:center; }
  .arrow svg { display:block; }
  .arrow .thru { font-size:10px; color:var(--muted); font-family:monospace; text-align:center; }

  /* Lint & Why views */
  .panel-pad { padding:16px 20px; overflow-y:auto; height:100%; }
  .finding { border:1px solid var(--border); border-radius:6px; padding:10px 12px; margin-bottom:8px;
             background:var(--panel); }
  .finding .sev { font-size:10px; text-transform:uppercase; font-weight:700; padding:1px 6px; border-radius:8px; margin-right:8px; }
  .sev-error { background:var(--red); color:#000; }
  .sev-warning { background:var(--yellow); color:#000; }
  .sev-info { background:var(--accent); color:#000; }
  .finding .comp { color:var(--muted); font-family:monospace; font-size:11px; }
  .finding .msg { margin-top:6px; font-size:12px; line-height:1.5; }
  .why-search { display:flex; gap:8px; margin-bottom:14px; }
  .why-search input { flex:1; background:var(--panel); border:1px solid var(--border); color:var(--fg);
    padding:7px 10px; border-radius:6px; font-size:13px; }
  .why-search button { background:var(--accent); color:#000; border:none; padding:7px 16px; border-radius:6px;
    font-weight:600; cursor:pointer; }
  .drop-card { border:1px solid var(--border); border-left:3px solid var(--red); border-radius:6px;
    padding:10px 12px; margin-bottom:8px; background:var(--panel); }
  .drop-card .stage { color:var(--red); font-weight:600; font-size:12px; }
  .drop-card .reason { margin-top:4px; font-size:12px; }
  .drop-card .payload { margin-top:6px; font-family:monospace; font-size:11px; color:var(--muted);
    white-space:pre-wrap; word-break:break-all; }
  .empty { color:var(--muted); font-size:13px; padding:20px; text-align:center; }
</style>
</head>
<body>
<header>
  <h1>PulseAgent</h1>
  <span class="badge">Live Debugger</span>
  <span class="version">v__VERSION__</span>
  <span id="status">connecting…</span>
</header>
<nav class="tabs">
  <button id="tab-signals"  class="active" onclick="showTab('signals')">Signals</button>
  <button id="tab-pipeline"            onclick="showTab('pipeline')">Pipeline</button>
  <button id="tab-lint"                onclick="showTab('lint')">Linter</button>
  <button id="tab-why"                 onclick="showTab('why')">Why dropped?</button>
</nav>

<!-- ============ Signals ============ -->
<div id="view-signals" class="view active">
  <div class="layout">
    <div class="sidebar">
      <h2>Sources</h2>
      <div id="sources-list"><em style="color:var(--muted)">loading…</em></div>
    </div>
    <div class="main">
      <div class="toolbar">
        <button class="pill active" id="pill-all"    onclick="setFilter('')">All</button>
        <button class="pill"        id="pill-metric" onclick="setFilter('metric')">Metrics</button>
        <button class="pill"        id="pill-log"    onclick="setFilter('log')">Logs</button>
        <input id="search" type="text" placeholder="Filter by name…" oninput="renderEvents()" style="width:200px">
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
</div>

<!-- ============ Pipeline ============ -->
<div id="view-pipeline" class="view">
  <div class="pipeline-wrap">
    <div id="flow" class="flow"><em class="empty">building pipeline graph…</em></div>
  </div>
</div>

<!-- ============ Linter ============ -->
<div id="view-lint" class="view">
  <div class="panel-pad">
    <div id="lint-list"><em class="empty">running linter…</em></div>
  </div>
</div>

<!-- ============ Why dropped ============ -->
<div id="view-why" class="view">
  <div class="panel-pad">
    <div class="why-search">
      <input id="why-input" type="text" placeholder="Paste a metric name or log substring…"
             onkeydown="if(event.key==='Enter')runWhy()">
      <button onclick="runWhy()">Trace</button>
    </div>
    <div id="why-results"><em class="empty">Enter a name above to see where matching signals were dropped, and why.</em></div>
  </div>
</div>

<script>
let events = [];
let filter = '';
let paused = false;
let activeTab = 'signals';

function showTab(t) {
  activeTab = t;
  document.querySelectorAll('nav.tabs button').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + t).classList.add('active');
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.getElementById('view-' + t).classList.add('active');
  if (t === 'pipeline') refreshPipeline();
  if (t === 'lint') refreshLint();
}

function setFilter(f) {
  filter = f;
  document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
  document.getElementById(f ? 'pill-' + f : 'pill-all')?.classList.add('active');
  renderEvents();
}

function togglePause() { paused = document.getElementById('pause').checked; }

function fmtLabels(labels) {
  return Object.entries(labels).map(([k,v]) =>
    `<span style="color:var(--muted)">${esc(k)}</span>=<span style="color:var(--fg)">${esc(v)}</span>`).join(' ');
}
function esc(s) { return String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

function renderEvents() {
  const search = document.getElementById('search').value.toLowerCase();
  const tbody = document.getElementById('event-body');
  const visible = events.filter(e =>
    (!filter || e.kind === filter) &&
    (!search || e.name.toLowerCase().includes(search))
  ).slice(0, 200);
  tbody.innerHTML = visible.map(e => {
    const t = new Date(e.ts_ms).toTimeString().slice(0,8);
    const val = e.value != null ? e.value.toPrecision(6) : '—';
    return `<tr><td class="kind-${e.kind}">${e.kind}</td><td>${esc(e.name)}</td><td class="val">${val}</td><td>${fmtLabels(e.labels)}</td><td>${t}</td></tr>`;
  }).join('');
}

// ---- Pipeline graph ----
function arrow(thru) {
  return `<div class="arrow"><div><div class="thru">${thru}</div>
    <svg width="40" height="12"><line x1="0" y1="6" x2="34" y2="6" stroke="#7d8590" stroke-width="1.5"/>
    <polygon points="34,1 40,6 34,11" fill="#7d8590"/></svg></div></div>`;
}
function nodeBox(n, stages) {
  const st = stages[n.id] || {};
  let stat = '';
  if (n.kind === 'processor') {
    stat = `<div class="nstat">✓ ${st.passed||0}${st.dropped ? ` · <span class="ndrop">✕ ${st.dropped}</span>` : ''}</div>`;
  }
  return `<div class="node ${n.kind}"><div class="nname">${esc(n.label)}</div>${stat}</div>`;
}
async function refreshPipeline() {
  try {
    const p = await (await fetch('/api/pipeline')).json();
    const flow = document.getElementById('flow');
    if (!p.nodes || p.nodes.length === 0) { flow.innerHTML = '<em class="empty">pipeline not initialised yet</em>'; return; }
    const sources    = p.nodes.filter(n => n.kind === 'source');
    const processors = p.nodes.filter(n => n.kind === 'processor');
    const targets    = p.nodes.filter(n => n.kind === 'target');

    const srcThru = Object.values(p.sources || {}).reduce((a,s)=>a+(s.metrics||0)+(s.logs||0),0);
    let html = '';
    html += `<div class="col"><div class="col-label">Sources</div>${sources.map(n=>{
      const s = (p.sources||{})[n.id.replace('source:','')] || {};
      const c = (s.metrics||0)+(s.logs||0);
      return `<div class="node source"><div class="nname">${esc(n.label)}</div><div class="nstat">${c} signals</div></div>`;
    }).join('')}</div>`;
    html += arrow(srcThru);
    processors.forEach((n, i) => {
      html += `<div class="col"><div class="col-label">${i===0?'Processors':'&nbsp;'}</div>${nodeBox(n, p.stages)}</div>`;
      const st = p.stages[n.id] || {};
      html += arrow(st.passed || 0);
    });
    html += `<div class="col"><div class="col-label">Target</div>${targets.map(n=>`<div class="node target"><div class="nname">${esc(n.label)}</div></div>`).join('')}</div>`;
    flow.innerHTML = html;
  } catch (e) { document.getElementById('flow').innerHTML = '<em class="empty">failed to load pipeline</em>'; }
}

// ---- Linter ----
async function refreshLint() {
  try {
    const findings = await (await fetch('/api/lint')).json();
    const list = document.getElementById('lint-list');
    if (!findings.length) { list.innerHTML = '<em class="empty">no findings</em>'; return; }
    list.innerHTML = findings.map(f =>
      `<div class="finding"><span class="sev sev-${f.severity}">${f.severity}</span>
       <span class="comp">${esc(f.component)}</span>
       <div class="msg">${esc(f.message)}</div></div>`).join('');
  } catch (e) { document.getElementById('lint-list').innerHTML = '<em class="empty">failed to load lint results</em>'; }
}

// ---- Why dropped ----
async function runWhy() {
  const name = document.getElementById('why-input').value.trim();
  const box = document.getElementById('why-results');
  try {
    const url = '/api/drops' + (name ? ('?name=' + encodeURIComponent(name)) : '');
    const drops = await (await fetch(url)).json();
    if (!drops.length) {
      box.innerHTML = `<div class="finding"><div class="msg">No drops recorded for <b>${esc(name||'(any)')}</b>.
        If you expected this signal, it is either flowing through to the target, or no source has emitted it yet.
        Check the <a style="color:var(--accent)" href="#" onclick="showTab('signals');return false">Signals</a> tab.</div></div>`;
      return;
    }
    box.innerHTML = drops.map(d => {
      const t = new Date(d.ts_ms).toTimeString().slice(0,8);
      return `<div class="drop-card">
        <span class="stage">dropped at ${esc(d.stage)}</span> <span style="color:var(--muted);font-size:11px">${t}</span>
        <div class="reason">${esc(d.reason)}</div>
        <div class="payload">${esc(d.name)}  {${fmtLabelsPlain(d.labels)}}</div>
      </div>`;
    }).join('');
  } catch (e) { box.innerHTML = '<em class="empty">failed to query drops</em>'; }
}
function fmtLabelsPlain(labels) {
  return Object.entries(labels).map(([k,v]) => `${k}="${v}"`).join(', ');
}

async function poll() {
  try {
    const [evResp, srcResp] = await Promise.all([fetch('/api/events'), fetch('/api/sources')]);
    const evData  = await evResp.json();
    const srcData = await srcResp.json();
    if (!paused) { events = evData; if (activeTab === 'signals') renderEvents(); }
    const sl = document.getElementById('sources-list');
    const entries = Object.entries(srcData);
    sl.innerHTML = entries.length === 0
      ? '<em style="color:var(--muted);font-size:12px">no sources active yet</em>'
      : entries.map(([name, stat]) =>
          `<div class="source-row"><span class="source-name">${esc(name)}</span>
           <span class="source-cnt">${stat.metrics}m ${stat.logs}l</span></div>`).join('');
    document.getElementById('status').textContent = `${evData.length} events · ${new Date().toTimeString().slice(0,8)}`;
    if (activeTab === 'pipeline') refreshPipeline();
  } catch (e) {
    document.getElementById('status').textContent = 'disconnected';
  }
  setTimeout(poll, 1000);
}

poll();
</script>
</body>
</html>
"##;
