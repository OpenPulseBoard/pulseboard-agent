use serde::Serialize;

use crate::config::{Config, RelabelAction, TransformOp};

/// A single configuration lint result surfaced in the live debugger.
#[derive(Debug, Clone, Serialize)]
pub struct LintFinding {
    /// "error" | "warning" | "info"
    pub severity: String,
    /// The component the finding relates to, e.g. "processors.relabel[2]".
    pub component: String,
    pub message: String,
}

impl LintFinding {
    fn err(component: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: "error".into(),
            component: component.into(),
            message: message.into(),
        }
    }
    fn warn(component: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: "warning".into(),
            component: component.into(),
            message: message.into(),
        }
    }
    fn info(component: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: "info".into(),
            component: component.into(),
            message: message.into(),
        }
    }
}

/// Label names that frequently cause cardinality explosions when used as a
/// metric label. Used to warn before a config ships rather than after the bill.
const HIGH_CARDINALITY_LABELS: &[&str] = &[
    "id",
    "uuid",
    "user_id",
    "userid",
    "request_id",
    "requestid",
    "trace_id",
    "traceid",
    "span_id",
    "session",
    "session_id",
    "email",
    "ip",
    "remote_addr",
    "path",
    "url",
    "token",
];

fn looks_high_cardinality(label: &str) -> bool {
    let l = label.to_ascii_lowercase();
    HIGH_CARDINALITY_LABELS.contains(&l.as_str())
}

/// Statically lint a resolved configuration.
pub fn lint(cfg: &Config) -> Vec<LintFinding> {
    let mut out = vec![];

    // --- Target ----------------------------------------------------------
    if cfg.targets.pulseboard.is_none() && cfg.agent.pulseboard_url.is_none() {
        out.push(LintFinding::warn(
            "targets.pulseboard",
            "No target configured — signals will be processed but never shipped.",
        ));
    }

    // --- Sources present? ------------------------------------------------
    let s = &cfg.sources;
    let any_source = s.host_metrics.is_some()
        || !s.file_logs.is_empty()
        || !s.prom_scrape.is_empty()
        || s.otlp.is_some()
        || s.journald.is_some()
        || !s.windows_event_log.is_empty()
        || s.docker_logs.is_some()
        || s.docker_stats.is_some()
        || s.kubernetes_pods.is_some();
    if !any_source {
        out.push(LintFinding::warn(
            "sources",
            "No sources are enabled — the agent will collect nothing.",
        ));
    }

    // --- Cardinality guard recommended for untrusted metric sources ------
    let metric_sources =
        s.prom_scrape.len() + usize::from(s.otlp.is_some()) + usize::from(s.docker_stats.is_some());
    if metric_sources > 0 && cfg.processors.cardinality_guard.is_none() {
        out.push(LintFinding::warn(
            "processors.cardinality_guard",
            "Metric sources are enabled without a cardinality_guard. A label \
             explosion from a scraped exporter could inflate your bill — \
             consider adding [processors.cardinality_guard].",
        ));
    }

    // --- Relabel rules ---------------------------------------------------
    for (i, rule) in cfg.processors.relabel.iter().enumerate() {
        let comp = format!("processors.relabel[{i}]");
        if let Some(pattern) = &rule.regex {
            if let Err(e) = regex::Regex::new(pattern) {
                out.push(LintFinding::err(
                    comp.clone(),
                    format!("invalid regex {pattern:?}: {e}"),
                ));
            }
        }
        if rule.action == RelabelAction::Replace {
            if let Some(target) = &rule.target_label {
                if looks_high_cardinality(target) {
                    out.push(LintFinding::warn(
                        comp.clone(),
                        format!(
                            "relabel writes label {target:?}, which often has very high \
                             cardinality. This may explode series counts."
                        ),
                    ));
                }
            }
        }
    }

    // --- redact_pii regexes ----------------------------------------------
    for (i, rule) in cfg.processors.redact_pii.iter().enumerate() {
        if let Err(e) = regex::Regex::new(&rule.pattern) {
            out.push(LintFinding::err(
                format!("processors.redact_pii[{i}]"),
                format!("invalid regex {:?}: {e}", rule.pattern),
            ));
        }
    }

    // --- transform high-cardinality labels -------------------------------
    for (i, op) in cfg.processors.transform.iter().enumerate() {
        if let TransformOp::SetLabel { label, .. } = op {
            if looks_high_cardinality(label) {
                out.push(LintFinding::warn(
                    format!("processors.transform[{i}]"),
                    format!(
                        "transform sets label {label:?}, which often has very high \
                         cardinality. This may explode series counts."
                    ),
                ));
            }
        }
    }

    if out.is_empty() {
        out.push(LintFinding::info("config", "No issues found."));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, RelabelRule, SourcesConfig};

    fn base() -> Config {
        toml::from_str("").unwrap()
    }

    #[test]
    fn warns_when_no_source_and_no_target() {
        let findings = lint(&base());
        assert!(findings.iter().any(|f| f.component == "sources"));
        assert!(findings.iter().any(|f| f.component == "targets.pulseboard"));
    }

    #[test]
    fn flags_invalid_relabel_regex() {
        let mut cfg = base();
        cfg.processors.relabel.push(RelabelRule {
            source_labels: vec!["__name__".into()],
            separator: None,
            target_label: None,
            regex: Some("(".into()),
            replacement: None,
            action: RelabelAction::Keep,
        });
        let findings = lint(&cfg);
        assert!(findings
            .iter()
            .any(|f| f.severity == "error" && f.component == "processors.relabel[0]"));
    }

    #[test]
    fn warns_on_metric_source_without_guard() {
        let mut cfg = base();
        cfg.sources = SourcesConfig {
            prom_scrape: vec![crate::config::PromScrapeConfig {
                name: "x".into(),
                url: "http://localhost/metrics".into(),
                interval: "30s".into(),
                extra_labels: Default::default(),
            }],
            ..Default::default()
        };
        let findings = lint(&cfg);
        assert!(findings
            .iter()
            .any(|f| f.component == "processors.cardinality_guard"));
    }
}
