use anyhow::Result;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::OtlpConfig;
use crate::signal::{now_ms, now_ns, Labels, LogEntry, MetricKind, MetricSample, Signal};
use crate::web::Inspector;

/// In-process OTLP HTTP/JSON receiver.
///
/// Lets applications push OTLP directly to the agent (POST /v1/metrics,
/// /v1/logs, /v1/traces) instead of standing up a separate collector. The
/// agent then runs them through the same processor pipeline as every other
/// source.
pub struct OtlpReceiverSource {
    cfg: OtlpConfig,
}

#[derive(Clone)]
struct ReceiverState {
    tx: mpsc::Sender<Signal>,
    inspector: Inspector,
}

impl OtlpReceiverSource {
    pub fn new(cfg: OtlpConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        let state = ReceiverState { tx, inspector };
        let app = Router::new()
            .route("/v1/metrics", post(metrics_handler))
            .route("/v1/logs", post(logs_handler))
            .route("/v1/traces", post(traces_handler))
            .with_state(state);

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.cfg.port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                warn!("otlp_receiver: cannot bind {}: {}", addr, e);
                return Ok(());
            }
        };
        info!("source: otlp_receiver listening on http://{}", addr);
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn metrics_handler(
    State(state): State<ReceiverState>,
    Json(body): Json<serde_json::Value>,
) -> StatusCode {
    let samples = parse_otlp_metrics(&body);
    for s in samples {
        state.inspector.record_source("otlp_receiver", "metric");
        if state.tx.send(Signal::Metric(s)).await.is_err() {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    StatusCode::OK
}

async fn logs_handler(
    State(state): State<ReceiverState>,
    Json(body): Json<serde_json::Value>,
) -> StatusCode {
    let entries = parse_otlp_logs(&body);
    for e in entries {
        state.inspector.record_source("otlp_receiver", "log");
        if state.tx.send(Signal::Log(e)).await.is_err() {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    StatusCode::OK
}

async fn traces_handler(Json(_body): Json<serde_json::Value>) -> StatusCode {
    // Trace forwarding is not yet wired into the signal pipeline; accept and
    // acknowledge so OTLP exporters don't error or retry-storm.
    debug!("otlp_receiver: received traces payload (not yet forwarded)");
    StatusCode::OK
}

// ---------------------------------------------------------------------------
// OTLP JSON parsing
// ---------------------------------------------------------------------------

/// Extract `{key: stringValue}` pairs from an OTLP attributes array.
fn attrs_to_labels(attrs: Option<&serde_json::Value>, labels: &mut Labels) {
    if let Some(serde_json::Value::Array(arr)) = attrs {
        for a in arr {
            let key = a.get("key").and_then(|x| x.as_str());
            let val = a.get("value").and_then(otlp_any_value);
            if let (Some(k), Some(v)) = (key, val) {
                labels.insert(k.to_string(), v);
            }
        }
    }
}

/// Flatten an OTLP AnyValue to a string.
fn otlp_any_value(v: &serde_json::Value) -> Option<String> {
    if let Some(s) = v.get("stringValue").and_then(|x| x.as_str()) {
        return Some(s.to_string());
    }
    if let Some(b) = v.get("boolValue").and_then(|x| x.as_bool()) {
        return Some(b.to_string());
    }
    if let Some(i) = v.get("intValue") {
        return Some(
            i.as_str()
                .map(str::to_string)
                .unwrap_or_else(|| i.to_string()),
        );
    }
    if let Some(d) = v.get("doubleValue").and_then(|x| x.as_f64()) {
        return Some(d.to_string());
    }
    None
}

/// Read a numeric data-point value from either `asDouble` or `asInt`.
fn data_point_value(dp: &serde_json::Value) -> Option<f64> {
    if let Some(d) = dp.get("asDouble").and_then(|x| x.as_f64()) {
        return Some(d);
    }
    if let Some(i) = dp.get("asInt") {
        // asInt is JSON-encoded as a string per the OTLP/JSON spec.
        return i
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| i.as_f64());
    }
    None
}

fn data_point_ts_ms(dp: &serde_json::Value) -> i64 {
    dp.get("timeUnixNano")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .map(|ns| ns / 1_000_000)
        .unwrap_or_else(now_ms)
}

pub fn parse_otlp_metrics(body: &serde_json::Value) -> Vec<MetricSample> {
    let mut out = vec![];
    let resource_metrics = match body.get("resourceMetrics").and_then(|x| x.as_array()) {
        Some(a) => a,
        None => return out,
    };

    for rm in resource_metrics {
        let mut resource_labels: Labels = HashMap::new();
        attrs_to_labels(
            rm.get("resource").and_then(|r| r.get("attributes")),
            &mut resource_labels,
        );

        let scope_metrics = rm.get("scopeMetrics").and_then(|x| x.as_array());
        for sm in scope_metrics.into_iter().flatten() {
            for metric in sm
                .get("metrics")
                .and_then(|x| x.as_array())
                .into_iter()
                .flatten()
            {
                let name = match metric.get("name").and_then(|x| x.as_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };

                // Gauge and Sum both carry a `dataPoints` array.
                let (points, kind) = if let Some(g) = metric.get("gauge") {
                    (g.get("dataPoints"), MetricKind::Gauge)
                } else if let Some(s) = metric.get("sum") {
                    (s.get("dataPoints"), MetricKind::Counter)
                } else {
                    continue; // histograms/summaries not yet supported
                };

                for dp in points.and_then(|x| x.as_array()).into_iter().flatten() {
                    let value = match data_point_value(dp) {
                        Some(v) => v,
                        None => continue,
                    };
                    let mut labels = resource_labels.clone();
                    attrs_to_labels(dp.get("attributes"), &mut labels);
                    out.push(MetricSample {
                        name: name.clone(),
                        labels,
                        value,
                        timestamp_ms: data_point_ts_ms(dp),
                        kind,
                    });
                }
            }
        }
    }
    out
}

pub fn parse_otlp_logs(body: &serde_json::Value) -> Vec<LogEntry> {
    let mut out = vec![];
    let resource_logs = match body.get("resourceLogs").and_then(|x| x.as_array()) {
        Some(a) => a,
        None => return out,
    };

    for rl in resource_logs {
        let mut resource_labels: Labels = HashMap::new();
        attrs_to_labels(
            rl.get("resource").and_then(|r| r.get("attributes")),
            &mut resource_labels,
        );

        for sl in rl
            .get("scopeLogs")
            .and_then(|x| x.as_array())
            .into_iter()
            .flatten()
        {
            for rec in sl
                .get("logRecords")
                .and_then(|x| x.as_array())
                .into_iter()
                .flatten()
            {
                let line = rec.get("body").and_then(otlp_any_value).unwrap_or_default();
                let mut labels = resource_labels.clone();
                attrs_to_labels(rec.get("attributes"), &mut labels);
                if let Some(sev) = rec.get("severityText").and_then(|x| x.as_str()) {
                    labels.insert("level".into(), sev.to_lowercase());
                }
                let timestamp_ns = rec
                    .get("timeUnixNano")
                    .and_then(|x| x.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or_else(now_ns);
                out.push(LogEntry {
                    labels,
                    line,
                    timestamp_ns,
                });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gauge_and_sum() {
        let body = serde_json::json!({
            "resourceMetrics": [{
                "resource": { "attributes": [
                    { "key": "service.name", "value": { "stringValue": "checkout" } }
                ]},
                "scopeMetrics": [{
                    "metrics": [
                        { "name": "cpu", "gauge": { "dataPoints": [
                            { "asDouble": 0.5, "timeUnixNano": "1700000000000000000",
                              "attributes": [{ "key": "core", "value": { "stringValue": "0" } }] }
                        ]}},
                        { "name": "reqs", "sum": { "dataPoints": [
                            { "asInt": "42", "timeUnixNano": "1700000000000000000" }
                        ]}}
                    ]
                }]
            }]
        });
        let samples = parse_otlp_metrics(&body);
        assert_eq!(samples.len(), 2);
        let cpu = samples.iter().find(|s| s.name == "cpu").unwrap();
        assert_eq!(cpu.value, 0.5);
        assert_eq!(cpu.kind, MetricKind::Gauge);
        assert_eq!(cpu.labels.get("service.name").unwrap(), "checkout");
        assert_eq!(cpu.labels.get("core").unwrap(), "0");
        let reqs = samples.iter().find(|s| s.name == "reqs").unwrap();
        assert_eq!(reqs.value, 42.0);
        assert_eq!(reqs.kind, MetricKind::Counter);
    }

    #[test]
    fn parses_logs() {
        let body = serde_json::json!({
            "resourceLogs": [{
                "resource": { "attributes": [
                    { "key": "service.name", "value": { "stringValue": "api" } }
                ]},
                "scopeLogs": [{
                    "logRecords": [
                        { "body": { "stringValue": "boom" }, "severityText": "ERROR",
                          "timeUnixNano": "1700000000000000000" }
                    ]
                }]
            }]
        });
        let logs = parse_otlp_logs(&body);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].line, "boom");
        assert_eq!(logs[0].labels.get("level").unwrap(), "error");
        assert_eq!(logs[0].labels.get("service.name").unwrap(), "api");
    }

    #[test]
    fn empty_payload_is_empty() {
        assert!(parse_otlp_metrics(&serde_json::json!({})).is_empty());
        assert!(parse_otlp_logs(&serde_json::json!({})).is_empty());
    }
}
