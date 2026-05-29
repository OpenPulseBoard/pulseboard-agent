use anyhow::Result;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::config::FileLogsConfig;
use crate::signal::{now_ns, LogEntry, Signal};
use crate::web::Inspector;

pub struct FileLogsSource {
    cfg: FileLogsConfig,
}

impl FileLogsSource {
    pub fn new(cfg: FileLogsConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        // Expand glob patterns — simple glob using walkdir-style matching
        let paths = expand_globs(&self.cfg.paths);
        if paths.is_empty() {
            warn!(
                "file_logs {:?}: no files matched {:?}",
                self.cfg.name, self.cfg.paths
            );
        }

        // Track byte offset per file so we tail, not re-read
        let mut offsets: HashMap<PathBuf, u64> = HashMap::new();

        // Seek to end on first open so we don't flood with historical data
        for p in &paths {
            if let Ok(meta) = std::fs::metadata(p) {
                offsets.insert(p.clone(), meta.len());
            }
        }

        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        let multiline_re = self
            .cfg
            .multiline_start
            .as_deref()
            .and_then(|p| regex::Regex::new(p).ok());

        loop {
            ticker.tick().await;

            // Re-expand globs each tick to pick up new files
            let current_paths = expand_globs(&self.cfg.paths);

            for path in &current_paths {
                let offset = offsets.entry(path.clone()).or_insert(0);

                let mut f = match std::fs::File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        debug!("file_logs: open {:?}: {}", path, e);
                        continue;
                    }
                };

                // Handle log rotation: if file is shorter than our offset, reset
                let file_len = f.metadata().map(|m| m.len()).unwrap_or(0);
                if file_len < *offset {
                    *offset = 0;
                }

                if let Err(e) = f.seek(SeekFrom::Start(*offset)) {
                    warn!("file_logs: seek {:?}: {}", path, e);
                    continue;
                }

                let mut reader = BufReader::new(f);
                let mut buf = String::new();
                let mut pending = String::new(); // multiline accumulator

                loop {
                    buf.clear();
                    let n = reader.read_line(&mut buf).unwrap_or(0);
                    if n == 0 {
                        break;
                    } // no more data
                    *offset += n as u64;

                    let trimmed = buf.trim_end_matches('\n').trim_end_matches('\r');

                    // Multiline handling
                    let flush = if let Some(re) = &multiline_re {
                        if re.is_match(trimmed) {
                            // New logical line starts — flush the pending one
                            let prev = std::mem::replace(&mut pending, trimmed.to_string());
                            !prev.is_empty()
                        } else {
                            // Continuation
                            if !pending.is_empty() {
                                pending.push('\n');
                            }
                            pending.push_str(trimmed);
                            false
                        }
                    } else {
                        // No multiline — emit immediately
                        pending = trimmed.to_string();
                        true
                    };

                    if flush && !pending.is_empty() {
                        let line = std::mem::take(&mut pending);
                        let entry = make_entry(&line, path, &self.cfg.name, &self.cfg.extra_labels);
                        inspector.record_source(&self.cfg.name, "log");
                        if tx.send(Signal::Log(entry)).await.is_err() {
                            return Ok(());
                        }
                    }
                }

                // Flush leftover multiline buffer
                if !pending.is_empty() {
                    let entry = make_entry(&pending, path, &self.cfg.name, &self.cfg.extra_labels);
                    inspector.record_source(&self.cfg.name, "log");
                    if tx.send(Signal::Log(entry)).await.is_err() {
                        return Ok(());
                    }
                    pending.clear();
                }
            }
        }
    }
}

fn expand_globs(patterns: &[String]) -> Vec<PathBuf> {
    let mut paths = vec![];
    for pattern in patterns {
        // Simple case: no wildcard
        if !pattern.contains('*') && !pattern.contains('?') {
            paths.push(PathBuf::from(pattern));
            continue;
        }
        if let Ok(entries) = glob::glob(pattern) {
            for entry in entries.flatten() {
                paths.push(entry);
            }
        }
    }
    paths
}

fn make_entry(line: &str, path: &Path, source: &str, extra: &HashMap<String, String>) -> LogEntry {
    let mut labels = extra.clone();
    labels.insert("source".into(), source.into());
    labels.insert(
        "filename".into(),
        path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default(),
    );
    LogEntry {
        labels,
        line: line.to_string(),
        timestamp_ns: now_ns(),
    }
}
