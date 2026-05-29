use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::time::Duration;
use tracing::{debug, warn};

use crate::config::PulseBoardTargetConfig;
use crate::enrollment::AgentCredentials;
use crate::signal::{LogEntry, MetricKind, MetricSample, Signal};

pub struct PulseBoardTarget {
    creds: AgentCredentials,
    client: reqwest::Client,
    cfg: Option<PulseBoardTargetConfig>,
}

impl PulseBoardTarget {
    pub fn new(creds: AgentCredentials, cfg: Option<PulseBoardTargetConfig>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("build reqwest client");
        Self { creds, client, cfg }
    }

    /// Flush a batch of signals to PulseBoard.
    ///
    /// Metrics → OTLP JSON  → POST /v1/metrics
    /// Logs    → Loki push  → POST /loki/api/v1/push
    pub async fn flush(&self, batch: Vec<Signal>) -> Result<()> {
        let mut metrics = vec![];
        let mut logs = vec![];

        for s in batch {
            match s {
                Signal::Metric(m) => metrics.push(m),
                Signal::Log(l) => logs.push(l),
            }
        }

        if !metrics.is_empty() {
            self.flush_metrics(metrics).await?;
        }
        if !logs.is_empty() {
            self.flush_logs(logs).await?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Metrics → OTLP JSON
    // ------------------------------------------------------------------

    async fn flush_metrics(&self, metrics: Vec<MetricSample>) -> Result<()> {
        let payload = build_otlp_metrics(&metrics);
        let url = format!("{}/v1/metrics", self.creds.base_url.trim_end_matches('/'));

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.creds.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("POST {}", url))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!("OTLP metrics {} from {}: {}", status, url, body);
        } else {
            debug!("flushed {} metrics → {}", metrics.len(), url);
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Logs → Loki push
    // ------------------------------------------------------------------

    async fn flush_logs(&self, logs: Vec<LogEntry>) -> Result<()> {
        let payload = build_loki_push(&logs);
        let url = format!(
            "{}/loki/api/v1/push",
            self.creds.base_url.trim_end_matches('/')
        );

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.creds.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("POST {}", url))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!("Loki push {} from {}: {}", status, url, body);
        } else {
            debug!("flushed {} log entries → {}", logs.len(), url);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// OTLP JSON payload builder
// ---------------------------------------------------------------------------

fn build_otlp_metrics(metrics: &[MetricSample]) -> Value {
    // Group by name+kind to build proper OTLP Metric objects
    use std::collections::HashMap;

    // metric_name → list of data points
    let mut by_name: HashMap<&str, Vec<&MetricSample>> = HashMap::new();
    for m in metrics {
        by_name.entry(&m.name).or_default().push(m);
    }

    let otlp_metrics: Vec<Value> = by_name
        .into_iter()
        .map(|(name, points)| {
            let kind = points[0].kind;
            let data_points: Vec<Value> = points
                .iter()
                .map(|p| {
                    let attrs: Vec<Value> = p
                        .labels
                        .iter()
                        .map(|(k, v)| {
                            json!({
                                "key": k,
                                "value": { "stringValue": v }
                            })
                        })
                        .collect();

                    let time_ns = p.timestamp_ms as i64 * 1_000_000;
                    json!({
                        "attributes":   attrs,
                        "timeUnixNano": time_ns.to_string(),
                        "asDouble":     p.value,
                    })
                })
                .collect();

            match kind {
                MetricKind::Counter => json!({
                    "name": name,
                    "sum": {
                        "dataPoints":           data_points,
                        "aggregationTemporality": 2,  // DELTA
                        "isMonotonic":          true,
                    }
                }),
                _ => json!({
                    "name": name,
                    "gauge": { "dataPoints": data_points }
                }),
            }
        })
        .collect();

    json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [{
                    "key":   "service.name",
                    "value": { "stringValue": "pulseagent" }
                }, {
                    "key":   "agent.version",
                    "value": { "stringValue": env!("CARGO_PKG_VERSION") }
                }]
            },
            "scopeMetrics": [{
                "scope": { "name": "pulseagent", "version": env!("CARGO_PKG_VERSION") },
                "metrics": otlp_metrics
            }]
        }]
    })
}

// ---------------------------------------------------------------------------
// Loki push payload builder
// ---------------------------------------------------------------------------

fn build_loki_push(logs: &[LogEntry]) -> Value {
    use std::collections::HashMap;

    // Group by label set (stream)
    let mut streams: HashMap<String, Vec<&LogEntry>> = HashMap::new();
    for entry in logs {
        // Stable key for grouping
        let mut kv: Vec<_> = entry.labels.iter().collect();
        kv.sort_by_key(|(k, _)| k.as_str());
        let key = kv
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");
        streams.entry(key).or_default().push(entry);
    }

    let stream_objs: Vec<Value> = streams
        .into_iter()
        .map(|(_, entries)| {
            let labels: std::collections::HashMap<String, String> = entries[0].labels.clone();
            let values: Vec<Value> = entries
                .iter()
                .map(|e| json!([e.timestamp_ns.to_string(), e.line]))
                .collect();
            json!({ "stream": labels, "values": values })
        })
        .collect();

    json!({ "streams": stream_objs })
}
