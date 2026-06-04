use anyhow::Result;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::debug;

use crate::config::KubernetesPodsConfig;
use crate::signal::{now_ns, LogEntry, Signal};
use crate::web::Inspector;

/// Collects Kubernetes pod logs in DaemonSet mode by tailing the per-container
/// log files the kubelet writes under `/var/log/containers/`.
///
/// This needs **no annotations and no Kubernetes API access** — the standard
/// CRI on-disk layout encodes everything we need in the filename:
///   `<pod>_<namespace>_<container>-<container-id>.log`
/// and each line is `<rfc3339> <stream> <P|F> <message>`.
pub struct KubernetesPodsSource {
    cfg: KubernetesPodsConfig,
}

impl KubernetesPodsSource {
    pub fn new(cfg: KubernetesPodsConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        let dir = PathBuf::from(&self.cfg.log_dir);
        let mut offsets: HashMap<PathBuf, u64> = HashMap::new();

        // Seek existing files to EOF on first pass so we don't replay history.
        for path in list_logs(&dir) {
            if let Ok(meta) = std::fs::metadata(&path) {
                offsets.insert(path, meta.len());
            }
        }

        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;

            for path in list_logs(&dir) {
                let meta = match parse_filename(&path) {
                    Some(m) => m,
                    None => continue,
                };
                if !self.cfg.namespaces.is_empty() && !self.cfg.namespaces.contains(&meta.namespace)
                {
                    continue;
                }

                let offset = offsets.entry(path.clone()).or_insert(0);
                let mut f = match std::fs::File::open(&path) {
                    Ok(f) => f,
                    Err(e) => {
                        debug!("kubernetes_pods: open {:?}: {}", path, e);
                        continue;
                    }
                };

                let file_len = f.metadata().map(|m| m.len()).unwrap_or(0);
                if file_len < *offset {
                    *offset = 0; // rotated
                }
                if f.seek(SeekFrom::Start(*offset)).is_err() {
                    continue;
                }

                let mut reader = BufReader::new(f);
                let mut buf = String::new();
                loop {
                    buf.clear();
                    let n = reader.read_line(&mut buf).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    *offset += n as u64;
                    let trimmed = buf.trim_end_matches(['\n', '\r']);
                    if trimmed.is_empty() {
                        continue;
                    }
                    if let Some(entry) = parse_cri_line(trimmed, &meta, &self.cfg.extra_labels) {
                        inspector.record_source("kubernetes_pods", "log");
                        if tx.send(Signal::Log(entry)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

fn list_logs(dir: &Path) -> Vec<PathBuf> {
    let mut out = vec![];
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) == Some("log") {
                out.push(p);
            }
        }
    }
    out
}

#[derive(Debug, Clone)]
struct PodMeta {
    pod: String,
    namespace: String,
    container: String,
}

/// Parse `<pod>_<namespace>_<container>-<id>.log` into its components.
fn parse_filename(path: &Path) -> Option<PodMeta> {
    let stem = path.file_stem()?.to_str()?;
    // Split into exactly three sections on the first two underscores. The
    // container segment also carries a trailing `-<container-id>` we strip.
    let mut parts = stem.splitn(3, '_');
    let pod = parts.next()?.to_string();
    let namespace = parts.next()?.to_string();
    let container_with_id = parts.next()?;
    if pod.is_empty() || namespace.is_empty() || container_with_id.is_empty() {
        return None;
    }
    let container = container_with_id
        .rsplit_once('-')
        .map(|(name, _id)| name.to_string())
        .unwrap_or_else(|| container_with_id.to_string());
    Some(PodMeta {
        pod,
        namespace,
        container,
    })
}

/// Parse one CRI log line: `<rfc3339> <stream> <P|F> <message>`.
fn parse_cri_line(line: &str, meta: &PodMeta, extra: &HashMap<String, String>) -> Option<LogEntry> {
    let mut it = line.splitn(4, ' ');
    let ts = it.next()?;
    let stream = it.next().unwrap_or("stdout");
    let _tag = it.next().unwrap_or("F");
    let message = it.next().unwrap_or("").to_string();

    let timestamp_ns = chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .and_then(|dt| dt.timestamp_nanos_opt())
        .unwrap_or_else(now_ns);

    let mut labels = extra.clone();
    labels.insert("source".into(), "kubernetes".into());
    labels.insert("namespace".into(), meta.namespace.clone());
    labels.insert("pod".into(), meta.pod.clone());
    labels.insert("container".into(), meta.container.clone());
    labels.insert("stream".into(), stream.to_string());

    Some(LogEntry {
        labels,
        line: message,
        timestamp_ns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_filename() {
        let p = PathBuf::from("/var/log/containers/nginx-abc123_default_nginx-deadbeef0123.log");
        let m = parse_filename(&p).unwrap();
        assert_eq!(m.pod, "nginx-abc123");
        assert_eq!(m.namespace, "default");
        assert_eq!(m.container, "nginx");
    }

    #[test]
    fn parses_cri_line() {
        let meta = PodMeta {
            pod: "p".into(),
            namespace: "ns".into(),
            container: "c".into(),
        };
        let line = "2024-01-02T03:04:05.123456789Z stdout F hello from pod";
        let e = parse_cri_line(line, &meta, &HashMap::new()).unwrap();
        assert_eq!(e.line, "hello from pod");
        assert_eq!(e.labels.get("namespace").unwrap(), "ns");
        assert_eq!(e.labels.get("stream").unwrap(), "stdout");
        assert!(e.timestamp_ns > 0);
    }

    #[test]
    fn rejects_malformed_filename() {
        let p = PathBuf::from("/var/log/containers/no-underscores.log");
        assert!(parse_filename(&p).is_none());
    }
}
