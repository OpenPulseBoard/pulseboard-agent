use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

use crate::config::{parse_duration_secs, WindowsEventLogConfig};
use crate::signal::{now_ns, LogEntry, Signal};
use crate::web::Inspector;

/// Polls a Windows Event Log channel using the native `wevtapi` Event Log API
/// (`EvtQuery` / `EvtNext` / `EvtRender` / `EvtFormatMessage`). Each event is
/// rendered to XML to extract the system fields and formatted through the
/// publisher's message catalogue to obtain the human-readable text — the same
/// information the old `Get-WinEvent` shell-out produced, without spawning a
/// PowerShell process on every interval.
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

        // SystemTime in the rendered XML is ISO-8601 (e.g.
        // "2024-01-02T03:04:05.1234567Z"); fall back to now() if absent.
        let timestamp_ns = self
            .time_created
            .as_deref()
            .and_then(parse_system_time)
            .map(|ms| ms * 1_000_000)
            .unwrap_or_else(now_ns);

        LogEntry {
            labels,
            line: self.message,
            timestamp_ns,
        }
    }
}

// ---------------------------------------------------------------------------
// Cross-platform parsing helpers (kept platform-independent so they remain
// unit-testable on any host, even though the FFI below is Windows-only).
// ---------------------------------------------------------------------------

/// Build a `WinEvent` from a rendered event XML fragment plus an optional
/// pre-formatted message string.
fn parse_event_xml(xml: &str, message: Option<String>) -> WinEvent {
    let provider = extract_attr(xml, "Provider", "Name");
    let id = extract_element(xml, "EventID").and_then(|s| {
        let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
        digits.parse::<i64>().ok()
    });
    let level = extract_element(xml, "Level")
        .and_then(|s| s.trim().parse::<u8>().ok())
        .map(level_name);
    let time_created = extract_attr(xml, "TimeCreated", "SystemTime");

    // Prefer the publisher-formatted message; otherwise synthesise something
    // useful from the EventData <Data> substitution values.
    let line = match message {
        Some(m) if !m.trim().is_empty() => m,
        _ => {
            let data = extract_data_values(xml);
            if data.is_empty() {
                String::new()
            } else {
                data.join(" ")
            }
        }
    };

    WinEvent {
        time_created,
        id,
        level,
        provider,
        message: line,
    }
}

/// Map a Windows event level number to the display name used by
/// `LevelDisplayName`. Unknown (publisher-defined) levels fall back to the
/// raw number so no information is lost.
fn level_name(level: u8) -> String {
    match level {
        0 => "Information".into(), // LogAlways
        1 => "Critical".into(),
        2 => "Error".into(),
        3 => "Warning".into(),
        4 => "Information".into(),
        5 => "Verbose".into(),
        other => other.to_string(),
    }
}

/// Extract the value of `attr` from the first `<tag ...>` element. Handles both
/// single- and double-quoted attribute values.
fn extract_attr(xml: &str, tag: &str, attr: &str) -> Option<String> {
    let tag_open = format!("<{tag}");
    let start = xml.find(&tag_open)?;
    let rest = &xml[start..];
    let end = rest.find('>')?;
    let tag_slice = &rest[..end];

    let needle = format!("{attr}=");
    let attr_pos = tag_slice.find(&needle)?;
    let after = &tag_slice[attr_pos + needle.len()..];
    let quote = after.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let value = &after[1..];
    let close = value.find(quote)?;
    Some(value[..close].to_string())
}

/// Extract the text content of the first `<tag>...</tag>` element.
fn extract_element(xml: &str, tag: &str) -> Option<String> {
    let tag_open = format!("<{tag}");
    let start = xml.find(&tag_open)?;
    let rest = &xml[start..];
    let content_start = rest.find('>')? + 1;
    let close_tag = format!("</{tag}>");
    let content_end = rest[content_start..].find(&close_tag)? + content_start;
    Some(rest[content_start..content_end].to_string())
}

/// Collect the text of every `<Data ...>value</Data>` element under EventData.
fn extract_data_values(xml: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(rel) = xml[cursor..].find("<Data") {
        let abs = cursor + rel;
        let rest = &xml[abs..];
        let gt = match rest.find('>') {
            Some(g) => g,
            None => break,
        };
        // Self-closing <Data .../> elements carry no value; skip them.
        if rest.as_bytes().get(gt.saturating_sub(1)) == Some(&b'/') {
            cursor = abs + gt + 1;
            continue;
        }
        let content_start = abs + gt + 1;
        if let Some(end_rel) = xml[content_start..].find("</Data>") {
            let value = &xml[content_start..content_start + end_rel];
            if !value.trim().is_empty() {
                out.push(value.to_string());
            }
            cursor = content_start + end_rel + "</Data>".len();
        } else {
            break;
        }
    }
    out
}

/// Parse an ISO-8601 timestamp to epoch milliseconds.
fn parse_system_time(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

// ---------------------------------------------------------------------------
// Windows: native wevtapi implementation
// ---------------------------------------------------------------------------

#[cfg(windows)]
async fn query_events(channel: &str, lookback_secs: u64) -> Result<Vec<WinEvent>> {
    // The EvtQuery family is synchronous and blocking, so run it on the
    // blocking thread pool to avoid stalling the async runtime.
    let channel = channel.to_string();
    tokio::task::spawn_blocking(move || native::query_events(&channel, lookback_secs)).await?
}

#[cfg(windows)]
mod native {
    use super::{extract_attr, parse_event_xml, WinEvent};
    use anyhow::{bail, Result};
    use windows_sys::Win32::Foundation::{GetLastError, ERROR_NO_MORE_ITEMS};
    use windows_sys::Win32::System::EventLog::{
        EvtClose, EvtFormatMessage, EvtNext, EvtOpenPublisherMetadata, EvtQuery, EvtRender,
    };

    // EVT_QUERY_FLAGS
    const EVT_QUERY_CHANNEL_PATH: u32 = 0x1;
    const EVT_QUERY_FORWARD_DIRECTION: u32 = 0x100;
    const EVT_QUERY_TOLERATE_QUERY_ERRORS: u32 = 0x1000;
    // EVT_RENDER_FLAGS
    const EVT_RENDER_EVENT_XML: u32 = 1;
    // EVT_FORMAT_MESSAGE_FLAGS
    const EVT_FORMAT_MESSAGE_EVENT: u32 = 1;

    // Process events in batches and cap the total per poll so a noisy channel
    // can never produce an unbounded burst.
    const BATCH: usize = 32;
    const MAX_EVENTS_PER_POLL: usize = 2000;
    const NEXT_TIMEOUT_MS: u32 = 5_000;

    /// RAII wrapper that closes an EVT_HANDLE on drop.
    struct EvtHandle(isize);
    impl Drop for EvtHandle {
        fn drop(&mut self) {
            if self.0 != 0 {
                // SAFETY: handle is non-null and owned exclusively by self.
                unsafe { EvtClose(self.0) };
            }
        }
    }

    fn to_wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    fn last_error() -> u32 {
        // SAFETY: GetLastError has no preconditions.
        unsafe { GetLastError() }
    }

    pub fn query_events(channel: &str, lookback_secs: u64) -> Result<Vec<WinEvent>> {
        let lookback_ms = lookback_secs.saturating_mul(1000);
        // Structured XML query: select events newer than the lookback window.
        let query = format!("*[System[TimeCreated[timediff(@SystemTime) <= {lookback_ms}]]]");
        let channel_w = to_wide(channel);
        let query_w = to_wide(&query);

        // SAFETY: both pointers reference NUL-terminated UTF-16 buffers that
        // outlive the call.
        let handle = unsafe {
            EvtQuery(
                0,
                channel_w.as_ptr(),
                query_w.as_ptr(),
                EVT_QUERY_CHANNEL_PATH
                    | EVT_QUERY_FORWARD_DIRECTION
                    | EVT_QUERY_TOLERATE_QUERY_ERRORS,
            )
        };
        if handle == 0 {
            bail!("EvtQuery failed (error {})", last_error());
        }
        let query = EvtHandle(handle);

        let mut events = Vec::new();
        'outer: loop {
            let mut batch: [isize; BATCH] = [0; BATCH];
            let mut returned: u32 = 0;
            // SAFETY: query.0 is a valid result-set handle; batch has BATCH slots.
            let ok = unsafe {
                EvtNext(
                    query.0,
                    BATCH as u32,
                    batch.as_mut_ptr(),
                    NEXT_TIMEOUT_MS,
                    0,
                    &mut returned,
                )
            };
            if ok == 0 {
                let err = last_error();
                if err == ERROR_NO_MORE_ITEMS {
                    break;
                }
                bail!("EvtNext failed (error {})", err);
            }

            for &raw in batch.iter().take(returned as usize) {
                let event = EvtHandle(raw);
                if let Some(parsed) = render_event(event.0) {
                    events.push(parsed);
                }
                if events.len() >= MAX_EVENTS_PER_POLL {
                    break 'outer;
                }
            }
        }
        Ok(events)
    }

    /// Render a single event handle to a `WinEvent`, returning `None` if the
    /// XML could not be produced.
    fn render_event(event: isize) -> Option<WinEvent> {
        let xml = render_xml(event)?;
        let message =
            extract_attr(&xml, "Provider", "Name").and_then(|p| format_message(event, &p));
        Some(parse_event_xml(&xml, message))
    }

    /// EvtRender(EvtRenderEventXml) — two-pass: size, then fill.
    fn render_xml(event: isize) -> Option<String> {
        let mut needed: u32 = 0;
        let mut count: u32 = 0;
        // SAFETY: first call deliberately passes a null/zero buffer to learn
        // the required size; failure with ERROR_INSUFFICIENT_BUFFER is expected.
        unsafe {
            EvtRender(
                0,
                event,
                EVT_RENDER_EVENT_XML,
                0,
                std::ptr::null_mut(),
                &mut needed,
                &mut count,
            )
        };
        if needed == 0 {
            return None;
        }

        // `needed` is a byte count; the buffer is UTF-16.
        let mut buf = vec![0u8; needed as usize];
        let mut used: u32 = 0;
        // SAFETY: buf has `needed` bytes; the API writes at most that many.
        let ok = unsafe {
            EvtRender(
                0,
                event,
                EVT_RENDER_EVENT_XML,
                needed,
                buf.as_mut_ptr() as *mut _,
                &mut used,
                &mut count,
            )
        };
        if ok == 0 {
            return None;
        }

        let u16_len = (used as usize) / 2;
        // SAFETY: buf is `needed` bytes; we read at most `used` (<= needed) of
        // them reinterpreted as u16, bounded again by the buffer length.
        let wide: &[u16] = unsafe {
            std::slice::from_raw_parts(buf.as_ptr() as *const u16, u16_len.min(buf.len() / 2))
        };
        let trimmed = wide.split(|&c| c == 0).next().unwrap_or(wide);
        Some(String::from_utf16_lossy(trimmed))
    }

    /// EvtFormatMessage(EvtFormatMessageEvent) using the publisher's metadata.
    fn format_message(event: isize, provider: &str) -> Option<String> {
        let provider_w = to_wide(provider);
        // SAFETY: provider_w is NUL-terminated; other args are null/zero.
        let meta =
            unsafe { EvtOpenPublisherMetadata(0, provider_w.as_ptr(), std::ptr::null(), 0, 0) };
        if meta == 0 {
            return None;
        }
        let meta = EvtHandle(meta);

        let mut needed: u32 = 0;
        // SAFETY: size-probe pass with a null buffer; ERROR_INSUFFICIENT_BUFFER
        // is the expected result and yields the required wchar count.
        unsafe {
            EvtFormatMessage(
                meta.0,
                event,
                0,
                0,
                std::ptr::null(),
                EVT_FORMAT_MESSAGE_EVENT,
                0,
                std::ptr::null_mut(),
                &mut needed,
            )
        };
        if needed == 0 {
            return None;
        }

        // `needed` here is a count of WCHARs (not bytes).
        let mut buf = vec![0u16; needed as usize];
        let mut used: u32 = 0;
        // SAFETY: buf holds `needed` wchars; the API writes at most that many.
        let ok = unsafe {
            EvtFormatMessage(
                meta.0,
                event,
                0,
                0,
                std::ptr::null(),
                EVT_FORMAT_MESSAGE_EVENT,
                needed,
                buf.as_mut_ptr(),
                &mut used,
            )
        };
        if ok == 0 {
            // Some events have no message in the catalogue; that's not fatal.
            return None;
        }
        let trimmed = buf.split(|&c| c == 0).next().unwrap_or(&buf);
        Some(String::from_utf16_lossy(trimmed))
    }
}

// ---------------------------------------------------------------------------
// Non-Windows: the Event Log does not exist; the source is a quiet no-op so
// the rest of the agent still compiles and runs on every platform.
// ---------------------------------------------------------------------------

#[cfg(not(windows))]
async fn query_events(_channel: &str, _lookback_secs: u64) -> Result<Vec<WinEvent>> {
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_XML: &str = r#"<Event xmlns='http://schemas.microsoft.com/win/2004/08/events/event'><System><Provider Name='Service Control Manager'/><EventID Qualifiers='16384'>7036</EventID><Level>4</Level><TimeCreated SystemTime='2024-01-02T03:04:05.1234567Z'/></System><EventData><Data Name='param1'>Windows Update</Data><Data Name='param2'>running</Data></EventData></Event>"#;

    #[test]
    fn parses_system_fields() {
        let ev = parse_event_xml(
            SAMPLE_XML,
            Some("The service entered the running state.".into()),
        );
        assert_eq!(ev.id, Some(7036));
        assert_eq!(ev.provider.as_deref(), Some("Service Control Manager"));
        assert_eq!(ev.level.as_deref(), Some("Information"));
        assert_eq!(
            ev.time_created.as_deref(),
            Some("2024-01-02T03:04:05.1234567Z")
        );
        assert!(ev.message.contains("running state"));
    }

    #[test]
    fn falls_back_to_event_data_when_no_message() {
        let ev = parse_event_xml(SAMPLE_XML, None);
        assert_eq!(ev.message, "Windows Update running");
    }

    #[test]
    fn level_mapping() {
        assert_eq!(level_name(2), "Error");
        assert_eq!(level_name(3), "Warning");
        assert_eq!(level_name(4), "Information");
        assert_eq!(level_name(99), "99");
    }

    #[test]
    fn skips_self_closing_data() {
        let xml = r#"<Event><EventData><Data Name='x'/></EventData></Event>"#;
        assert!(extract_data_values(xml).is_empty());
    }

    #[test]
    fn parses_system_time_millis() {
        assert_eq!(
            parse_system_time("2024-01-02T03:04:05Z"),
            chrono::DateTime::parse_from_rfc3339("2024-01-02T03:04:05Z")
                .ok()
                .map(|d| d.timestamp_millis())
        );
    }
}
