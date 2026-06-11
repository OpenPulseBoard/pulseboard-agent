#![allow(dead_code)]

use regex::Regex;

use crate::config::RedactPiiRule;
use crate::signal::Signal;

/// Redacts PII in log lines and label values using regex replacement.
///
/// Matched text is replaced with `[[pii:FIELD_NAME]]` (or a custom
/// replacement string) so downstream storage can see that redaction
/// happened without storing the sensitive value.
pub struct PiiRedactor {
    rules: Vec<CompiledPiiRule>,
}

struct CompiledPiiRule {
    field: String,
    re: Regex,
    replacement: String,
}

impl PiiRedactor {
    pub fn new(rules: &[RedactPiiRule]) -> Self {
        let compiled = rules
            .iter()
            .filter_map(|r| {
                Regex::new(&r.pattern)
                    .map_err(|e| tracing::warn!("redact_pii: bad regex {:?}: {}", r.pattern, e))
                    .ok()
                    .map(|re| CompiledPiiRule {
                        field: r.field.clone(),
                        re,
                        replacement: r
                            .replacement
                            .clone()
                            .unwrap_or_else(|| format!("[[pii:{}]]", r.field)),
                    })
            })
            .collect();
        Self { rules: compiled }
    }

    pub fn apply(&self, signal: Signal) -> Signal {
        match signal {
            Signal::Log(mut entry) => {
                for rule in &self.rules {
                    if rule.field == "line" || rule.field == "*" {
                        let new_line = rule
                            .re
                            .replace_all(&entry.line, rule.replacement.as_str())
                            .into_owned();
                        entry.line = new_line;
                    }
                    // Redact matching label values
                    // Redact all label values when field is "*"
                    if rule.field == "*" {
                        for v in entry.labels.values_mut() {
                            let new_v = rule
                                .re
                                .replace_all(v, rule.replacement.as_str())
                                .into_owned();
                            *v = new_v;
                        }
                    } else if let Some(v) = entry.labels.get_mut(&rule.field) {
                        let new_v = rule
                            .re
                            .replace_all(v, rule.replacement.as_str())
                            .into_owned();
                        *v = new_v;
                    }
                }
                Signal::Log(entry)
            }
            Signal::Metric(mut sample) => {
                for rule in &self.rules {
                    if let Some(v) = sample.labels.get_mut(&rule.field) {
                        let new_v = rule
                            .re
                            .replace_all(v, rule.replacement.as_str())
                            .into_owned();
                        *v = new_v;
                    }
                }
                Signal::Metric(sample)
            }
            // Trace bodies are opaque OTLP envelopes; redact rules don't apply.
            other @ Signal::Trace(_) => other,
        }
    }
}
