use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Unified signal type flowing through the agent pipeline.
#[derive(Debug, Clone)]
pub enum Signal {
    Metric(MetricSample),
    Log(LogEntry),
    Trace(TraceBatch),
}

/// Opaque OTLP trace payload received from an upstream exporter.
///
/// We do not currently parse spans inside the agent — traces are forwarded
/// upstream as-is (raw OTLP/JSON body). Holding the parsed `serde_json::Value`
/// keeps batching cheap while letting the target re-serialise into one combined
/// request.
#[derive(Debug, Clone)]
pub struct TraceBatch {
    /// Raw OTLP/JSON envelope: `{ "resourceSpans": [...] }`.
    pub payload: serde_json::Value,
    /// Number of spans in the batch (best-effort, used for metrics/inspector).
    pub span_count: usize,
    /// Receive timestamp in Unix milliseconds.
    pub received_ms: i64,
}

/// A single numeric measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSample {
    /// Metric name (Prometheus naming conventions: snake_case, no unit suffix needed)
    pub name: String,
    /// Key-value label set
    pub labels: Labels,
    /// Value
    pub value: f64,
    /// Unix milliseconds
    pub timestamp_ms: i64,
    /// Metric kind — used when building OTLP payloads
    pub kind: MetricKind,
}

/// A single log line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Stream labels (Loki-style)
    pub labels: Labels,
    /// The log line
    pub line: String,
    /// Unix nanoseconds
    pub timestamp_ns: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    #[default]
    Gauge,
    Counter,
    Histogram,
}

pub type Labels = HashMap<String, String>;

/// Convenience: current Unix milliseconds.
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Convenience: current Unix nanoseconds.
pub fn now_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}
