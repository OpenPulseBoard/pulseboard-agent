use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::config::{parse_duration_secs, DockerLogsConfig, DockerStatsConfig};
use crate::signal::{now_ms, now_ns, Labels, LogEntry, MetricKind, MetricSample, Signal};
use crate::web::Inspector;

// ===========================================================================
// docker_stats — container resource usage via `docker stats --no-stream`
// ===========================================================================

pub struct DockerStatsSource {
    cfg: DockerStatsConfig,
}

impl DockerStatsSource {
    pub fn new(cfg: DockerStatsConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        let interval_secs = parse_duration_secs(&self.cfg.interval).unwrap_or(15);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));

        loop {
            ticker.tick().await;
            match collect_stats().await {
                Ok(samples) => {
                    for mut s in samples {
                        s.labels.extend(self.cfg.extra_labels.clone());
                        inspector.record_source("docker_stats", "metric");
                        if tx.send(Signal::Metric(s)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Err(e) => warn!("docker_stats: {}", e),
            }
        }
    }
}

async fn collect_stats() -> Result<Vec<MetricSample>> {
    let out = Command::new("docker")
        .args(["stats", "--no-stream", "--format", "{{json .}}"])
        .output()
        .await?;

    if !out.status.success() {
        anyhow::bail!(
            "docker stats exited with status {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }

    let ts = now_ms();
    let mut samples = vec![];
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                debug!("docker_stats: skipping unparseable line: {}", e);
                continue;
            }
        };

        let name = v.get("Name").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let id = v.get("ID").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if name.is_empty() {
            continue;
        }
        let mut labels: Labels = HashMap::new();
        labels.insert("name".into(), name);
        labels.insert("id".into(), id);

        let mk = |metric: &str, value: f64, labels: &Labels| MetricSample {
            name: metric.to_string(),
            labels: labels.clone(),
            value,
            timestamp_ms: ts,
            kind: MetricKind::Gauge,
        };

        if let Some(cpu) = v.get("CPUPerc").and_then(|x| x.as_str()).and_then(parse_percent) {
            samples.push(mk("container_cpu_usage_percent", cpu, &labels));
        }
        if let Some(mem) = v.get("MemPerc").and_then(|x| x.as_str()).and_then(parse_percent) {
            samples.push(mk("container_memory_usage_percent", mem, &labels));
        }
        if let Some((used, limit)) = v.get("MemUsage").and_then(|x| x.as_str()).and_then(parse_mem_usage) {
            samples.push(mk("container_memory_usage_bytes", used, &labels));
            samples.push(mk("container_spec_memory_limit_bytes", limit, &labels));
        }
        if let Some(pids) = v.get("PIDs").and_then(|x| x.as_str()).and_then(|s| s.trim().parse::<f64>().ok()) {
            samples.push(mk("container_pids", pids, &labels));
        }
    }
    Ok(samples)
}

/// Parse a percentage string like "12.34%" into 12.34.
fn parse_percent(s: &str) -> Option<f64> {
    s.trim().trim_end_matches('%').trim().parse::<f64>().ok()
}

/// Parse a "used / limit" pair like "12MiB / 1GiB" into bytes.
fn parse_mem_usage(s: &str) -> Option<(f64, f64)> {
    let (used, limit) = s.split_once('/')?;
    Some((parse_bytes(used.trim())?, parse_bytes(limit.trim())?))
}

/// Parse a human-readable size like "12MiB", "1.5GB", "512kB" into bytes.
fn parse_bytes(s: &str) -> Option<f64> {
    let s = s.trim();
    let split = s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num.trim().parse().ok()?;
    let factor = match unit.trim() {
        "" | "B" => 1.0,
        "kB" => 1e3,
        "KiB" => 1024.0,
        "MB" => 1e6,
        "MiB" => 1024.0 * 1024.0,
        "GB" => 1e9,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        "TB" => 1e12,
        "TiB" => 1024.0_f64.powi(4),
        _ => return None,
    };
    Some(num * factor)
}

// ===========================================================================
// docker_logs — per-container log tail via `docker logs --since --timestamps`
// ===========================================================================

pub struct DockerLogsSource {
    cfg: DockerLogsConfig,
}

impl DockerLogsSource {
    pub fn new(cfg: DockerLogsConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        let interval_secs = parse_duration_secs(&self.cfg.interval).unwrap_or(5);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        let name_re = self
            .cfg
            .name_filter
            .as_deref()
            .and_then(|p| regex::Regex::new(p).ok());
        // container id → RFC3339 timestamp of the last line we emitted
        let mut cursors: HashMap<String, String> = HashMap::new();

        loop {
            ticker.tick().await;

            let containers = match list_containers(&name_re).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("docker_logs: list containers: {}", e);
                    continue;
                }
            };

            for (id, name) in containers {
                let since = cursors
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| format!("{interval_secs}s"));

                match fetch_logs(&id, &since).await {
                    Ok(lines) => {
                        for (ts_rfc3339, message) in lines {
                            // Advance the cursor to the newest line seen.
                            if let Some(cur) = cursors.get(&id) {
                                if ts_rfc3339.as_str() <= cur.as_str() {
                                    continue; // already emitted
                                }
                            }
                            cursors.insert(id.clone(), ts_rfc3339.clone());

                            let entry = make_log(&ts_rfc3339, &message, &id, &name, &self.cfg.extra_labels);
                            inspector.record_source("docker_logs", "log");
                            if tx.send(Signal::Log(entry)).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => debug!("docker_logs: logs for {}: {}", name, e),
                }
            }
        }
    }
}

async fn list_containers(name_re: &Option<regex::Regex>) -> Result<Vec<(String, String)>> {
    let out = Command::new("docker")
        .args(["ps", "--no-trunc", "--format", "{{.ID}}|{{.Names}}"])
        .output()
        .await?;
    if !out.status.success() {
        anyhow::bail!(
            "docker ps exited with status {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let mut result = vec![];
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        if let Some((id, name)) = line.trim().split_once('|') {
            if name_re.as_ref().is_none_or(|re| re.is_match(name)) {
                result.push((id.to_string(), name.to_string()));
            }
        }
    }
    Ok(result)
}

async fn fetch_logs(id: &str, since: &str) -> Result<Vec<(String, String)>> {
    let out = Command::new("docker")
        .args(["logs", "--timestamps", "--since", since, id])
        .output()
        .await?;
    // docker logs writes container stdout to our stdout and stderr to ours;
    // both carry the --timestamps prefix, so merge them.
    let mut combined = String::from_utf8_lossy(&out.stdout).into_owned();
    combined.push_str(&String::from_utf8_lossy(&out.stderr));

    let mut lines = vec![];
    for line in combined.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Some((ts, msg)) = line.split_once(' ') {
            lines.push((ts.to_string(), msg.to_string()));
        }
    }
    Ok(lines)
}

fn make_log(
    ts_rfc3339: &str,
    message: &str,
    id: &str,
    name: &str,
    extra: &HashMap<String, String>,
) -> LogEntry {
    let mut labels = extra.clone();
    labels.insert("source".into(), "docker".into());
    labels.insert("container".into(), name.to_string());
    labels.insert("container_id".into(), id.chars().take(12).collect());

    let timestamp_ns = chrono::DateTime::parse_from_rfc3339(ts_rfc3339)
        .map(|dt| dt.timestamp_nanos_opt().unwrap_or_else(now_ns))
        .unwrap_or_else(|_| now_ns());

    LogEntry {
        labels,
        line: message.to_string(),
        timestamp_ns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_percent() {
        assert_eq!(parse_percent("12.34%"), Some(12.34));
        assert_eq!(parse_percent("0.00%"), Some(0.0));
        assert_eq!(parse_percent("--"), None);
    }

    #[test]
    fn parses_bytes() {
        assert_eq!(parse_bytes("512B"), Some(512.0));
        assert_eq!(parse_bytes("1kB"), Some(1000.0));
        assert_eq!(parse_bytes("1KiB"), Some(1024.0));
        assert_eq!(parse_bytes("12MiB"), Some(12.0 * 1024.0 * 1024.0));
        assert_eq!(parse_bytes("1.5GB"), Some(1.5e9));
    }

    #[test]
    fn parses_mem_usage_pair() {
        let (used, limit) = parse_mem_usage("12MiB / 1GiB").unwrap();
        assert_eq!(used, 12.0 * 1024.0 * 1024.0);
        assert_eq!(limit, 1024.0 * 1024.0 * 1024.0);
    }
}
