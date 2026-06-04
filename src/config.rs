use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub agent: AgentConfig,

    #[serde(default)]
    pub sources: SourcesConfig,

    #[serde(default)]
    pub processors: ProcessorsConfig,

    #[serde(default)]
    pub targets: TargetsConfig,
}

// ---------------------------------------------------------------------------
// [agent] section
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentConfig {
    /// Directory where the agent persists state (enrolled creds, offsets …)
    pub data_dir: PathBuf,

    /// Structured tracing level
    pub log_level: String,

    /// Optional human-readable name; defaults to hostname
    pub instance_name: Option<String>,

    /// PulseBoard enrollment token. When set, the agent exchanges it on
    /// first startup for a long-lived agent API key stored in `data_dir`.
    /// Clear this line after first enroll — it is not needed again.
    pub enroll_token: Option<String>,

    /// PulseBoard workspace URL, e.g. "https://acme.pulseboard.cloud"
    pub pulseboard_url: Option<String>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/pulseagent"),
            log_level: "info".into(),
            instance_name: None,
            enroll_token: None,
            pulseboard_url: None,
        }
    }
}

// ---------------------------------------------------------------------------
// [sources] section
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourcesConfig {
    pub host_metrics: Option<HostMetricsConfig>,
    #[serde(default)]
    pub file_logs: Vec<FileLogsConfig>,
    #[serde(default)]
    pub prom_scrape: Vec<PromScrapeConfig>,
    /// In-process OTLP HTTP/JSON receiver on a local port
    pub otlp: Option<OtlpConfig>,
    /// systemd journal tail (Linux only) — shells out to `journalctl`
    pub journald: Option<JournaldConfig>,
    /// Windows Event Log poller (Windows only) — shells out to PowerShell
    #[serde(default)]
    pub windows_event_log: Vec<WindowsEventLogConfig>,
    /// Docker container log collection via the `docker` CLI
    pub docker_logs: Option<DockerLogsConfig>,
    /// Docker container resource stats via the `docker` CLI
    pub docker_stats: Option<DockerStatsConfig>,
    /// Kubernetes pod logs in DaemonSet mode — tails /var/log/containers/*.log
    pub kubernetes_pods: Option<KubernetesPodsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostMetricsConfig {
    /// Scrape interval, e.g. "15s"
    #[serde(default = "default_interval")]
    pub interval: String,

    /// Which collectors to enable (cpu, memory, disk, network, load)
    /// Empty = all enabled.
    #[serde(default)]
    pub collectors: Vec<String>,

    /// Extra labels applied to every metric from this source
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}

impl Default for HostMetricsConfig {
    fn default() -> Self {
        Self {
            interval: default_interval(),
            collectors: vec![],
            extra_labels: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileLogsConfig {
    pub name: String,
    pub paths: Vec<String>,

    /// Multiline start pattern (regex). Lines that do NOT match are appended
    /// to the previous log entry.
    pub multiline_start: Option<String>,

    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromScrapeConfig {
    pub name: String,
    pub url: String,

    /// Scrape interval, e.g. "30s"
    #[serde(default = "default_scrape_interval")]
    pub interval: String,

    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpConfig {
    /// Port for the OTLP HTTP/JSON receiver (default 4318)
    #[serde(default = "default_otlp_port")]
    pub port: u16,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct JournaldConfig {
    /// Optional list of systemd units to follow (e.g. ["ssh.service"]).
    /// Empty = the whole journal.
    pub units: Vec<String>,
    /// Minimum priority (0=emerg … 7=debug). Lines above this are dropped.
    /// `None` = no filter.
    pub max_priority: Option<u8>,
    /// Extra labels applied to every emitted log entry.
    pub extra_labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowsEventLogConfig {
    pub name: String,
    /// Channel to read, e.g. "System", "Application", "Security".
    pub channel: String,
    /// Poll interval, e.g. "15s".
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DockerLogsConfig {
    /// Poll interval, e.g. "5s".
    pub interval: String,
    /// Only collect logs from containers whose name matches this regex.
    /// `None` = all running containers.
    pub name_filter: Option<String>,
    pub extra_labels: HashMap<String, String>,
}

impl Default for DockerLogsConfig {
    fn default() -> Self {
        Self {
            interval: "5s".into(),
            name_filter: None,
            extra_labels: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DockerStatsConfig {
    /// Poll interval, e.g. "15s".
    pub interval: String,
    pub extra_labels: HashMap<String, String>,
}

impl Default for DockerStatsConfig {
    fn default() -> Self {
        Self {
            interval: "15s".into(),
            extra_labels: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KubernetesPodsConfig {
    /// Directory holding the per-container log symlinks. The kubelet writes
    /// these in the standard CRI layout: `<pod>_<namespace>_<container>-<id>.log`.
    pub log_dir: String,
    /// Only collect logs from pods in these namespaces. Empty = all.
    pub namespaces: Vec<String>,
    pub extra_labels: HashMap<String, String>,
}

impl Default for KubernetesPodsConfig {
    fn default() -> Self {
        Self {
            log_dir: "/var/log/containers".into(),
            namespaces: vec![],
            extra_labels: HashMap::new(),
        }
    }
}

fn default_interval() -> String {
    "15s".into()
}
fn default_scrape_interval() -> String {
    "30s".into()
}
fn default_otlp_port() -> u16 {
    4318
}

// ---------------------------------------------------------------------------
// [processors] section
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessorsConfig {
    #[serde(default)]
    pub batch: BatchConfig,

    pub cardinality_guard: Option<CardinalityGuardConfig>,

    #[serde(default)]
    pub relabel: Vec<RelabelRule>,

    #[serde(default)]
    pub redact_pii: Vec<RedactPiiRule>,

    /// Ordered list of transform operations applied to every signal.
    #[serde(default)]
    pub transform: Vec<TransformOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    /// Maximum number of signals in a batch before flushing
    pub max_size: usize,
    /// Maximum time to hold a partial batch before flushing (e.g. "5s")
    pub max_delay: String,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 1000,
            max_delay: "5s".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardinalityGuardConfig {
    /// Max unique label-value combinations per metric name before dropping
    pub max_series_per_metric: usize,
}

/// Prometheus-style relabel rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelabelRule {
    pub source_labels: Vec<String>,
    pub separator: Option<String>,
    pub target_label: Option<String>,
    pub regex: Option<String>,
    pub replacement: Option<String>,
    pub action: RelabelAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RelabelAction {
    Replace,
    Keep,
    Drop,
    LabelDrop,
    LabelKeep,
    LabelMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactPiiRule {
    /// Label name or log field to check
    pub field: String,
    /// Regex whose full matches are replaced
    pub pattern: String,
    /// Replacement string (default: "[[redacted]]")
    pub replacement: Option<String>,
}

/// A single transform operation. Operations are applied in order and use a
/// tiny, predictable template syntax with `${...}` placeholders:
///
/// * `${line}`        — the log line (logs only)
/// * `${name}`        — the metric name (metrics only)
/// * `${label.NAME}`  — the current value of label `NAME`
/// * `${json.FIELD}`  — top-level field `FIELD` of the log line parsed as JSON
///
/// There is no scripting, no Lua, no JS — just substitution, so resource
/// usage is bounded and the result is deterministic.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TransformOp {
    /// Set (or overwrite) a label from a template.
    SetLabel { label: String, value: String },
    /// Remove a label if present.
    RemoveLabel { label: String },
    /// Rename a label, preserving its value.
    RenameLabel { from: String, to: String },
    /// Parse the log line as JSON and lift the listed top-level fields to
    /// labels of the same name. No-op for metrics or non-JSON lines.
    ParseJson { fields: Vec<String> },
}

// ---------------------------------------------------------------------------
// [targets] section
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetsConfig {
    pub pulseboard: Option<PulseBoardTargetConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulseBoardTargetConfig {
    /// Workspace URL, e.g. https://acme.pulseboard.cloud
    /// Falls back to `agent.pulseboard_url` when omitted.
    pub url: Option<String>,
    /// API key. Supports ${env:VAR} expansion.
    pub api_key: Option<String>,
    /// Send metrics as OTLP JSON (default: true)
    #[serde(default = "default_true")]
    pub otlp_metrics: bool,
    /// Send logs as Loki push JSON (default: true)
    #[serde(default = "default_true")]
    pub loki_logs: bool,
}

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Loader
// ---------------------------------------------------------------------------

pub fn load(path: &Path) -> Result<Config> {
    if !path.exists() {
        // Return a minimal default config with host_metrics enabled so
        // `pulseagent --check` on a fresh install doesn't error out if
        // the config file doesn't exist yet.
        tracing::warn!("config file {:?} not found — using built-in defaults", path);
        return Ok(Config {
            agent: AgentConfig::default(),
            sources: SourcesConfig {
                host_metrics: Some(HostMetricsConfig::default()),
                ..Default::default()
            },
            processors: ProcessorsConfig::default(),
            targets: TargetsConfig::default(),
        });
    }

    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {path:?}"))?;

    // Perform ${env:VAR} expansion before parsing TOML
    let expanded = expand_env_vars(&raw);

    let cfg: Config = toml::from_str(&expanded).with_context(|| format!("parsing {path:?}"))?;

    validate(&cfg)?;
    Ok(cfg)
}

fn expand_env_vars(s: &str) -> String {
    let re = regex::Regex::new(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}").unwrap();
    re.replace_all(s, |caps: &regex::Captures| {
        std::env::var(&caps[1]).unwrap_or_default()
    })
    .into_owned()
}

fn validate(cfg: &Config) -> Result<()> {
    // At least one target must be configured to be useful
    if cfg.targets.pulseboard.is_none() && cfg.agent.pulseboard_url.is_none() {
        tracing::warn!(
            "no [targets.pulseboard] configured; metrics will be collected but not shipped"
        );
    }

    // Validate interval strings
    if let Some(hm) = &cfg.sources.host_metrics {
        parse_duration_secs(&hm.interval)
            .with_context(|| format!("host_metrics.interval = {:?}", hm.interval))?;
    }

    for fl in &cfg.sources.file_logs {
        if fl.paths.is_empty() {
            bail!("file_logs source {:?} has no paths", fl.name);
        }
    }

    for ps in &cfg.sources.prom_scrape {
        parse_duration_secs(&ps.interval)
            .with_context(|| format!("prom_scrape {:?}: interval = {:?}", ps.name, ps.interval))?;
    }

    for we in &cfg.sources.windows_event_log {
        parse_duration_secs(&we.interval).with_context(|| {
            format!("windows_event_log {:?}: interval = {:?}", we.name, we.interval)
        })?;
    }

    if let Some(dl) = &cfg.sources.docker_logs {
        parse_duration_secs(&dl.interval)
            .with_context(|| format!("docker_logs.interval = {:?}", dl.interval))?;
    }

    if let Some(ds) = &cfg.sources.docker_stats {
        parse_duration_secs(&ds.interval)
            .with_context(|| format!("docker_stats.interval = {:?}", ds.interval))?;
    }

    Ok(())
}

/// Parse a duration string like "15s", "1m", "2h" into seconds.
pub fn parse_duration_secs(s: &str) -> Result<u64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('s') {
        return n
            .trim()
            .parse::<u64>()
            .with_context(|| format!("bad duration {s:?}"));
    }
    if let Some(n) = s.strip_suffix('m') {
        return Ok(n
            .trim()
            .parse::<u64>()
            .with_context(|| format!("bad duration {s:?}"))?
            * 60);
    }
    if let Some(n) = s.strip_suffix('h') {
        return Ok(n
            .trim()
            .parse::<u64>()
            .with_context(|| format!("bad duration {s:?}"))?
            * 3600);
    }
    // Plain integer = seconds
    s.parse::<u64>()
        .with_context(|| format!("bad duration {s:?}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml: &str) -> Config {
        toml::from_str(toml).expect("TOML parse failed")
    }

    // Regression: config with only [sources.host_metrics] (no file_logs /
    // prom_scrape entries) must not fail with "missing field `file_logs`".
    #[test]
    fn host_metrics_only_no_file_logs() {
        let cfg = parse(
            r#"
[sources.host_metrics]
interval = "15s"
"#,
        );
        assert!(cfg.sources.host_metrics.is_some());
        assert!(cfg.sources.file_logs.is_empty());
        assert!(cfg.sources.prom_scrape.is_empty());
    }

    // Completely empty TOML should give back all-default values.
    #[test]
    fn empty_toml_uses_defaults() {
        let cfg = parse("");
        assert_eq!(cfg.agent.log_level, "info");
        assert!(cfg.sources.host_metrics.is_none());
        assert!(cfg.sources.file_logs.is_empty());
        assert!(cfg.sources.prom_scrape.is_empty());
        assert_eq!(cfg.processors.batch.max_size, 1000);
        assert_eq!(cfg.processors.batch.max_delay, "5s");
    }

    // Multiple [[sources.file_logs]] entries should all be collected.
    #[test]
    fn multiple_file_logs_entries() {
        let cfg = parse(
            r#"
[[sources.file_logs]]
name  = "app"
paths = ["/var/log/app/*.log"]

[[sources.file_logs]]
name  = "nginx"
paths = ["/var/log/nginx/access.log"]
"#,
        );
        assert_eq!(cfg.sources.file_logs.len(), 2);
        assert_eq!(cfg.sources.file_logs[0].name, "app");
        assert_eq!(cfg.sources.file_logs[1].name, "nginx");
    }

    // [[sources.prom_scrape]] should apply the default interval when omitted.
    #[test]
    fn prom_scrape_default_interval() {
        let cfg = parse(
            r#"
[[sources.prom_scrape]]
name = "node"
url  = "http://localhost:9100/metrics"
"#,
        );
        assert_eq!(cfg.sources.prom_scrape[0].interval, "30s");
    }

    // Full realistic config (mirrors agent.example.toml) should parse cleanly.
    #[test]
    fn full_example_config_parses() {
        let cfg = parse(
            r#"
[agent]
data_dir       = "/var/lib/pulseagent"
log_level      = "info"
pulseboard_url = "https://acme.pulseboard.cloud"
enroll_token   = "tok_test"

[sources.host_metrics]
interval   = "15s"
collectors = ["cpu", "memory", "disk", "network", "load"]

[[sources.file_logs]]
name  = "app"
paths = ["/var/log/app/*.log"]

[[sources.prom_scrape]]
name     = "nginx"
url      = "http://localhost:9113/metrics"
interval = "30s"

[processors.batch]
max_size  = 500
max_delay = "10s"

[processors.cardinality_guard]
max_series_per_metric = 1000

[targets.pulseboard]
url = "https://acme.pulseboard.cloud"
"#,
        );
        assert_eq!(cfg.agent.log_level, "info");
        assert_eq!(cfg.sources.host_metrics.unwrap().collectors.len(), 5);
        assert_eq!(cfg.sources.file_logs.len(), 1);
        assert_eq!(cfg.sources.prom_scrape.len(), 1);
        assert_eq!(cfg.processors.batch.max_size, 500);
        assert_eq!(
            cfg.processors
                .cardinality_guard
                .unwrap()
                .max_series_per_metric,
            1000
        );
        assert!(cfg.targets.pulseboard.is_some());
    }

    // Regression: config produced by install.sh has a bare [targets.pulseboard]
    // with no `url` field — it must parse cleanly (url is derived from
    // agent.pulseboard_url at runtime).
    #[test]
    fn install_sh_config_parses() {
        let cfg = parse(
            r#"
[agent]
data_dir       = "/var/lib/pulseagent"
log_level      = "info"
pulseboard_url = "https://acme.pulseboard.cloud"
enroll_token   = "tok_test"

[sources.host_metrics]
interval = "15s"

[processors.batch]
max_size  = 1000
max_delay = "5s"

[processors.cardinality_guard]
max_series_per_metric = 2000

[targets.pulseboard]
"#,
        );
        assert!(cfg.targets.pulseboard.is_some());
        assert!(cfg.targets.pulseboard.unwrap().url.is_none());
        assert_eq!(
            cfg.agent.pulseboard_url.as_deref(),
            Some("https://acme.pulseboard.cloud")
        );
    }

    // env-var expansion replaces ${env:VAR} tokens.
    #[test]
    fn env_var_expansion() {
        std::env::set_var("TEST_PULSE_TOKEN", "secret123");
        let expanded = expand_env_vars(r#"enroll_token = "${env:TEST_PULSE_TOKEN}""#);
        assert!(expanded.contains("secret123"));
        std::env::remove_var("TEST_PULSE_TOKEN");
    }

    // Unset env var should expand to empty string, not leave the placeholder.
    #[test]
    fn env_var_expansion_missing_var_becomes_empty() {
        std::env::remove_var("__PULSE_UNSET_VAR__");
        let expanded = expand_env_vars("url = \"${env:__PULSE_UNSET_VAR__}\"");
        assert!(!expanded.contains("${env:"));
    }

    // parse_duration_secs unit tests.
    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration_secs("15s").unwrap(), 15);
        assert_eq!(parse_duration_secs("2m").unwrap(), 120);
        assert_eq!(parse_duration_secs("1h").unwrap(), 3600);
        assert_eq!(parse_duration_secs("60").unwrap(), 60);
    }

    #[test]
    fn parse_duration_invalid_returns_err() {
        assert!(parse_duration_secs("abc").is_err());
        assert!(parse_duration_secs("").is_err());
    }
}
