use regex::Regex;
use std::collections::HashMap;

use crate::config::{RelabelAction, RelabelRule};
use crate::signal::{Labels, Signal};

pub struct Relabeler {
    rules: Vec<CompiledRule>,
}

struct CompiledRule {
    source_labels: Vec<String>,
    separator: String,
    target_label: Option<String>,
    regex: Option<Regex>,
    replacement: String,
    action: RelabelAction,
}

impl Relabeler {
    pub fn new(rules: &[RelabelRule]) -> Self {
        let compiled = rules
            .iter()
            .map(|r| CompiledRule {
                source_labels: r.source_labels.clone(),
                separator: r.separator.clone().unwrap_or_else(|| ";".into()),
                target_label: r.target_label.clone(),
                regex: r.regex.as_deref().and_then(|p| Regex::new(p).ok()),
                replacement: r.replacement.clone().unwrap_or_else(|| "$1".into()),
                action: r.action.clone(),
            })
            .collect();
        Self { rules: compiled }
    }

    /// Returns `None` if the signal should be dropped, `Some(signal)` otherwise.
    pub fn apply(&self, mut signal: Signal) -> Option<Signal> {
        match &mut signal {
            Signal::Metric(ref mut m) => {
                for rule in &self.rules {
                    if !apply_rule(rule, &mut m.labels, &mut m.name) {
                        return None;
                    }
                }
            }
            Signal::Log(ref mut l) => {
                // For logs we apply relabel against the label set only
                let mut name = String::new();
                for rule in &self.rules {
                    if !apply_rule(rule, &mut l.labels, &mut name) {
                        return None;
                    }
                }
            }
        }
        Some(signal)
    }
}

/// Returns `false` if the signal should be dropped.
fn apply_rule(rule: &CompiledRule, labels: &mut Labels, metric_name: &mut String) -> bool {
    match rule.action {
        RelabelAction::Drop | RelabelAction::Keep => {
            let src_value = join_source_labels(&rule.source_labels, &rule.separator, labels);
            let matches = rule.regex.as_ref().is_none_or(|re| re.is_match(&src_value));
            if rule.action == RelabelAction::Drop {
                return !matches; // drop if matches
            } else {
                return matches; // keep only if matches
            }
        }

        RelabelAction::Replace => {
            let src_value = join_source_labels(&rule.source_labels, &rule.separator, labels);
            if let Some(re) = &rule.regex {
                if let Some(caps) = re.captures(&src_value) {
                    let replacement = caps.expand_str(&rule.replacement);
                    if let Some(target) = &rule.target_label {
                        if target == "__name__" {
                            *metric_name = replacement;
                        } else if replacement.is_empty() {
                            labels.remove(target);
                        } else {
                            labels.insert(target.clone(), replacement);
                        }
                    }
                }
            } else if let Some(target) = &rule.target_label {
                labels.insert(target.clone(), rule.replacement.clone());
            }
        }

        RelabelAction::LabelDrop => {
            if let Some(re) = &rule.regex {
                labels.retain(|k, _| !re.is_match(k));
            }
        }

        RelabelAction::LabelKeep => {
            if let Some(re) = &rule.regex {
                labels.retain(|k, _| re.is_match(k));
            }
        }

        RelabelAction::LabelMap => {
            if let Some(re) = &rule.regex {
                let new_labels: HashMap<String, String> = labels
                    .iter()
                    .filter_map(|(k, v)| {
                        re.captures(k).map(|caps| {
                            let new_key = caps.expand_str(&rule.replacement);
                            (new_key, v.clone())
                        })
                    })
                    .collect();
                labels.extend(new_labels);
            }
        }
    }
    true
}

fn join_source_labels(sources: &[String], sep: &str, labels: &Labels) -> String {
    sources
        .iter()
        .map(|k| labels.get(k).map(|v| v.as_str()).unwrap_or(""))
        .collect::<Vec<_>>()
        .join(sep)
}

// Extend Captures with an expand_str helper
trait ExpandStr {
    fn expand_str(&self, template: &str) -> String;
}

impl ExpandStr for regex::Captures<'_> {
    fn expand_str(&self, template: &str) -> String {
        let mut out = String::new();
        self.expand(template, &mut out);
        out
    }
}
