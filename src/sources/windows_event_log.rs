use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::warn;

use crate::config::{parse_duration_secs, WindowsEventLogConfig};
use crate::signal::{now_ns, LogEntry, Signal};
use crate::web::Inspector;

/// Polls a Windows Event Log channel by shelling out to PowerShell's
/// `Get-WinEvent`. Shelling out (rather than linking the Win32 EvtQuery API)
/// keeps the codebase portable and dependency-free; PowerShell ships with
/// every supported Windows release.
pub struct WindowsEventLogSource {
    cfg: WindowsEventLogConfig,
}

impl WindowsEventLogSource {
    pub fn new(cfg: WindowsEventLogConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        let interval_secs = parse_duration_secs(&self.cfg.interval).unwrap_or(15);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        // Look back one interval (plus a small margin) on each poll.
        let lookback = interval_secs + 2;

        loop {
            ticker.tick().await;
            match query_events(&self.cfg.channel, lookback).await {
                Ok(events) => {
                    for ev in events {
                        let entry = ev.into_log(&self.cfg.channel, &self.cfg.extra_labels);
                        inspector.record_source(&self.cfg.name, "log");
                        if tx.send(Signal::Log(entry)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Err(e) => warn!("windows_event_log {}: {}", self.cfg.name, e),
            }
        }
    }
}

#[derive(Debug)]
struct WinEvent {
    time_created: Option<String>,
    id: Option<i64>,
    level: Option<String>,
    provider: Option<String>,
    message: String,
}

impl WinEvent {
    fn into_log(self, channel: &str, extra: &HashMap<String, String>) -> LogEntry {
        let mut labels = extra.clone();
        labels.insert("source".into(), "windows_event_log".into());
        labels.insert("channel".into(), channel.to_string());
        if let Some(p) = &self.provider {
            labels.insert("provider".into(), p.clone());
        }
        if let Some(l) = &self.level {
            labels.insert("level".into(), l.clone());
        }
        if let Some(id) = self.id {
            labels.insert("event_id".into(), id.to_string());
        }

        // PowerShell emits TimeCreated as "/Date(1700000000000)/" or ISO; try
        // to parse epoch-millis from the /Date(...)/ form, else fall back to now.
        let timestamp_ns = self
            .time_created
            .as_deref()
            .and_then(parse_ps_date)
            .map(|ms| ms * 1_000_000)
            .unwrap_or_else(now_ns);

        LogEntry {
            labels,
            line: self.message,
            timestamp_ns,
        }
    }
}

async fn query_events(channel: &str, lookback_secs: u64) -> Result<Vec<WinEvent>> {
    // Build a PowerShell one-liner that returns a compact JSON array.
    let script = format!(
        "Get-WinEvent -FilterHashtable @{{LogName='{channel}'; StartTime=(Get-Date).AddSeconds(-{lookback_secs})}} -ErrorAction SilentlyContinue | \
         Select-Object @{{N='TimeCreated';E={{$_.TimeCreated.ToString('o')}}}},Id,LevelDisplayName,ProviderName,Message | \
         ConvertTo-Json -Compress -Depth 3"
    );

    let out = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()
        .await?;

    if !out.status.success() {
        anyhow::bail!(
            "powershell exited with status {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }

    let stdout = String::from_utf8_lossy(&out.stdout);
    parse_events_json(stdout.trim())
}

fn parse_events_json(json: &str) -> Result<Vec<WinEvent>> {
    if json.is_empty() {
        return Ok(vec![]);
    }
    let v: serde_json::Value = serde_json::from_str(json)?;
    // ConvertTo-Json yields a bare object for a single result, an array otherwise.
    let items: Vec<&serde_json::Value> = match &v {
        serde_json::Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };

    let mut events = vec![];
    for it in items {
        let message = it
            .get("Message")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        events.push(WinEvent {
            time_created: it
                .get("TimeCreated")
                .and_then(|x| x.as_str())
                .map(str::to_string),
            id: it.get("Id").and_then(|x| x.as_i64()),
            level: it
                .get("LevelDisplayName")
                .and_then(|x| x.as_str())
                .map(str::to_string),
            provider: it
                .get("ProviderName")
                .and_then(|x| x.as_str())
                .map(str::to_string),
            message,
        });
    }
    Ok(events)
}

/// Parse PowerShell date forms: ISO-8601 ("o" format) or "/Date(ms)/".
fn parse_ps_date(s: &str) -> Option<i64> {
    if let Some(inner) = s.strip_prefix("/Date(").and_then(|x| x.strip_suffix(")/")) {
        // May include a timezone offset suffix like "+0000".
        let digits: String = inner
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        return digits.parse::<i64>().ok();
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_object() {
        let json = r#"{"TimeCreated":"2024-01-02T03:04:05.0000000Z","Id":7036,"LevelDisplayName":"Information","ProviderName":"Service Control Manager","Message":"The X service entered the running state."}"#;
        let events = parse_events_json(json).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, Some(7036));
        assert!(events[0].message.contains("running state"));
    }

    #[test]
    fn parses_array() {
        let json = r#"[{"Id":1,"Message":"a"},{"Id":2,"Message":"b"}]"#;
        let events = parse_events_json(json).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn empty_is_empty() {
        assert!(parse_events_json("").unwrap().is_empty());
    }

    #[test]
    fn parses_ps_date_forms() {
        assert_eq!(parse_ps_date("/Date(1700000000000)/"), Some(1_700_000_000_000));
        assert_eq!(
            parse_ps_date("2024-01-02T03:04:05Z"),
            chrono::DateTime::parse_from_rfc3339("2024-01-02T03:04:05Z")
                .ok()
                .map(|d| d.timestamp_millis())
        );
    }
}
