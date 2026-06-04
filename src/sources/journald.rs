use anyhow::Result;
use std::collections::HashMap;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::config::JournaldConfig;
use crate::signal::{LogEntry, Signal};
use crate::web::Inspector;

/// Tails the systemd journal by spawning `journalctl --follow --output=json`.
///
/// Shelling out to `journalctl` (rather than linking `libsystemd`) keeps the
/// agent a single, statically-linked musl binary that runs on any distro and
/// inside minimal containers — at the cost of requiring `journalctl` on PATH,
/// which is always present where a journal exists.
pub struct JournaldSource {
    cfg: JournaldConfig,
}

impl JournaldSource {
    pub fn new(cfg: JournaldConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        // Build the journalctl invocation. `--since now` skips historical
        // backlog so we don't flood the pipeline on startup.
        let mut cmd = Command::new("journalctl");
        cmd.arg("--follow")
            .arg("--output=json")
            .arg("--since=now")
            .arg("--no-pager");
        for unit in &self.cfg.units {
            cmd.arg("--unit").arg(unit);
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::null());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                warn!("journald: failed to spawn journalctl: {} — is systemd present?", e);
                return Ok(());
            }
        };

        let stdout = match child.stdout.take() {
            Some(s) => s,
            None => {
                warn!("journald: journalctl produced no stdout");
                return Ok(());
            }
        };

        let mut lines = BufReader::new(stdout).lines();
        loop {
            let line = match lines.next_line().await {
                Ok(Some(l)) => l,
                Ok(None) => {
                    debug!("journald: journalctl stream ended");
                    break;
                }
                Err(e) => {
                    warn!("journald: read error: {}", e);
                    break;
                }
            };

            if let Some(entry) = parse_journal_json(&line, &self.cfg) {
                inspector.record_source("journald", "log");
                if tx.send(Signal::Log(entry)).await.is_err() {
                    debug!("journald: pipeline channel closed, exiting");
                    let _ = child.kill().await;
                    return Ok(());
                }
            }
        }

        let _ = child.kill().await;
        Ok(())
    }
}

/// Parse one line of `journalctl --output=json` into a `LogEntry`.
///
/// Returns `None` when the line is unparseable or filtered out by priority.
fn parse_journal_json(line: &str, cfg: &JournaldConfig) -> Option<LogEntry> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;

    // MESSAGE may be a string or, for binary payloads, an array of bytes.
    let message = match v.get("MESSAGE") {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(bytes)) => bytes
            .iter()
            .filter_map(|b| b.as_u64().map(|n| n as u8))
            .map(|b| b as char)
            .collect(),
        _ => return None,
    };

    let priority = field_str(&v, "PRIORITY").and_then(|s| s.parse::<u8>().ok());
    if let (Some(max), Some(p)) = (cfg.max_priority, priority) {
        if p > max {
            return None;
        }
    }

    // Realtime timestamp is microseconds since the epoch.
    let ts_ns = field_str(&v, "__REALTIME_TIMESTAMP")
        .and_then(|s| s.parse::<i64>().ok())
        .map(|us| us * 1_000)
        .unwrap_or_else(crate::signal::now_ns);

    let mut labels: HashMap<String, String> = cfg.extra_labels.clone();
    labels.insert("source".into(), "journald".into());
    if let Some(unit) = field_str(&v, "_SYSTEMD_UNIT") {
        labels.insert("unit".into(), unit);
    }
    if let Some(ident) = field_str(&v, "SYSLOG_IDENTIFIER") {
        labels.insert("identifier".into(), ident);
    }
    if let Some(host) = field_str(&v, "_HOSTNAME") {
        labels.insert("host".into(), host);
    }
    if let Some(p) = priority {
        labels.insert("priority".into(), p.to_string());
        labels.insert("level".into(), priority_to_level(p).into());
    }

    Some(LogEntry {
        labels,
        line: message,
        timestamp_ns: ts_ns,
    })
}

/// journalctl renders most fields as strings, but numeric fields are sometimes
/// emitted as JSON numbers — accept both.
fn field_str(v: &serde_json::Value, key: &str) -> Option<String> {
    match v.get(key) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// Map a syslog priority (0–7) to a human-readable level name.
fn priority_to_level(p: u8) -> &'static str {
    match p {
        0 => "emerg",
        1 => "alert",
        2 => "crit",
        3 => "error",
        4 => "warning",
        5 => "notice",
        6 => "info",
        _ => "debug",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> JournaldConfig {
        JournaldConfig::default()
    }

    #[test]
    fn parses_basic_entry() {
        let line = r#"{"MESSAGE":"hello world","PRIORITY":"6","_SYSTEMD_UNIT":"ssh.service","__REALTIME_TIMESTAMP":"1700000000000000","_HOSTNAME":"web-01"}"#;
        let e = parse_journal_json(line, &cfg()).expect("should parse");
        assert_eq!(e.line, "hello world");
        assert_eq!(e.labels.get("unit").unwrap(), "ssh.service");
        assert_eq!(e.labels.get("level").unwrap(), "info");
        assert_eq!(e.labels.get("host").unwrap(), "web-01");
        assert_eq!(e.timestamp_ns, 1_700_000_000_000_000 * 1_000);
    }

    #[test]
    fn priority_filter_drops_low_priority() {
        let mut c = cfg();
        c.max_priority = Some(4); // warnings and above only
        let line = r#"{"MESSAGE":"debug noise","PRIORITY":"7"}"#;
        assert!(parse_journal_json(line, &c).is_none());
        let line2 = r#"{"MESSAGE":"a warning","PRIORITY":"4"}"#;
        assert!(parse_journal_json(line2, &c).is_some());
    }

    #[test]
    fn message_as_byte_array() {
        let line = r#"{"MESSAGE":[104,105],"PRIORITY":"6"}"#;
        let e = parse_journal_json(line, &cfg()).expect("should parse");
        assert_eq!(e.line, "hi");
    }

    #[test]
    fn unparseable_line_returns_none() {
        assert!(parse_journal_json("not json", &cfg()).is_none());
    }
}
