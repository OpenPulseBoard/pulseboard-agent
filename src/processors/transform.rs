use std::collections::HashMap;

use crate::config::TransformOp;
use crate::signal::{Labels, Signal};

/// Applies an ordered list of transform operations to each signal.
///
/// The DSL is deliberately tiny: substitution only, no scripting. Templates
/// use `${...}` placeholders resolved against the current signal:
///   * `${line}`        the log line (logs only)
///   * `${name}`        the metric name (metrics only)
///   * `${label.NAME}`  the current value of label NAME
///   * `${json.FIELD}`  top-level FIELD of the log line parsed as JSON
pub struct Transformer {
    ops: Vec<TransformOp>,
}

impl Transformer {
    pub fn new(ops: &[TransformOp]) -> Self {
        Self { ops: ops.to_vec() }
    }

    pub fn apply(&self, mut signal: Signal) -> Signal {
        for op in &self.ops {
            apply_op(op, &mut signal);
        }
        signal
    }
}

fn apply_op(op: &TransformOp, signal: &mut Signal) {
    match op {
        TransformOp::SetLabel { label, value } => {
            let resolved = render_template(value, signal);
            labels_mut(signal).insert(label.clone(), resolved);
        }
        TransformOp::RemoveLabel { label } => {
            labels_mut(signal).remove(label);
        }
        TransformOp::RenameLabel { from, to } => {
            let labels = labels_mut(signal);
            if let Some(v) = labels.remove(from) {
                labels.insert(to.clone(), v);
            }
        }
        TransformOp::ParseJson { fields } => {
            if let Signal::Log(entry) = signal {
                if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(&entry.line) {
                    for field in fields {
                        if let Some(v) = map.get(field) {
                            let s = match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            entry.labels.insert(field.clone(), s);
                        }
                    }
                }
            }
        }
    }
}

fn labels_mut(signal: &mut Signal) -> &mut Labels {
    match signal {
        Signal::Metric(m) => &mut m.labels,
        Signal::Log(l) => &mut l.labels,
    }
}

/// Resolve `${...}` placeholders in `template` against `signal`.
fn render_template(template: &str, signal: &Signal) -> String {
    // Lazily parse JSON only if a ${json.*} placeholder is present.
    let json: Option<HashMap<String, serde_json::Value>> = if template.contains("${json.") {
        if let Signal::Log(l) = signal {
            serde_json::from_str(&l.line).ok()
        } else {
            None
        }
    } else {
        None
    };

    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        if let Some(end) = after.find('}') {
            let key = &after[..end];
            out.push_str(&resolve_key(key, signal, json.as_ref()));
            rest = &after[end + 1..];
        } else {
            // No closing brace — emit the literal and stop.
            out.push_str(&rest[start..]);
            rest = "";
        }
    }
    out.push_str(rest);
    out
}

fn resolve_key(
    key: &str,
    signal: &Signal,
    json: Option<&HashMap<String, serde_json::Value>>,
) -> String {
    match signal {
        Signal::Metric(m) => match key {
            "name" => m.name.clone(),
            _ => key
                .strip_prefix("label.")
                .and_then(|l| m.labels.get(l))
                .cloned()
                .unwrap_or_default(),
        },
        Signal::Log(l) => {
            if key == "line" {
                return l.line.clone();
            }
            if let Some(label) = key.strip_prefix("label.") {
                return l.labels.get(label).cloned().unwrap_or_default();
            }
            if let Some(field) = key.strip_prefix("json.") {
                return json
                    .and_then(|m| m.get(field))
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default();
            }
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{LogEntry, MetricKind, MetricSample};

    fn log(line: &str) -> Signal {
        Signal::Log(LogEntry {
            labels: HashMap::new(),
            line: line.into(),
            timestamp_ns: 0,
        })
    }

    fn metric(name: &str, labels: &[(&str, &str)]) -> Signal {
        Signal::Metric(MetricSample {
            name: name.into(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value: 1.0,
            timestamp_ms: 0,
            kind: MetricKind::Gauge,
        })
    }

    #[test]
    fn set_label_static() {
        let t = Transformer::new(&[TransformOp::SetLabel {
            label: "env".into(),
            value: "prod".into(),
        }]);
        if let Signal::Log(l) = t.apply(log("hi")) {
            assert_eq!(l.labels.get("env").unwrap(), "prod");
        } else {
            panic!();
        }
    }

    #[test]
    fn set_label_from_existing_label() {
        let t = Transformer::new(&[TransformOp::SetLabel {
            label: "svc".into(),
            value: "app-${label.region}".into(),
        }]);
        if let Signal::Metric(m) = t.apply(metric("x", &[("region", "eu")])) {
            assert_eq!(m.labels.get("svc").unwrap(), "app-eu");
        } else {
            panic!();
        }
    }

    #[test]
    fn parse_json_lifts_fields() {
        let t = Transformer::new(&[TransformOp::ParseJson {
            fields: vec!["level".into(), "code".into()],
        }]);
        if let Signal::Log(l) = t.apply(log(r#"{"level":"error","code":500,"msg":"x"}"#)) {
            assert_eq!(l.labels.get("level").unwrap(), "error");
            assert_eq!(l.labels.get("code").unwrap(), "500");
        } else {
            panic!();
        }
    }

    #[test]
    fn json_template_placeholder() {
        let t = Transformer::new(&[TransformOp::SetLabel {
            label: "user".into(),
            value: "${json.user}".into(),
        }]);
        if let Signal::Log(l) = t.apply(log(r#"{"user":"alice"}"#)) {
            assert_eq!(l.labels.get("user").unwrap(), "alice");
        } else {
            panic!();
        }
    }

    #[test]
    fn rename_and_remove() {
        let t = Transformer::new(&[
            TransformOp::RenameLabel {
                from: "old".into(),
                to: "new".into(),
            },
            TransformOp::RemoveLabel {
                label: "drop_me".into(),
            },
        ]);
        if let Signal::Metric(m) = t.apply(metric("x", &[("old", "v"), ("drop_me", "z")])) {
            assert_eq!(m.labels.get("new").unwrap(), "v");
            assert!(!m.labels.contains_key("old"));
            assert!(!m.labels.contains_key("drop_me"));
        } else {
            panic!();
        }
    }

    #[test]
    fn unterminated_placeholder_is_literal() {
        let t = Transformer::new(&[TransformOp::SetLabel {
            label: "x".into(),
            value: "a${broken".into(),
        }]);
        if let Signal::Log(l) = t.apply(log("hi")) {
            assert_eq!(l.labels.get("x").unwrap(), "a${broken");
        } else {
            panic!();
        }
    }
}
