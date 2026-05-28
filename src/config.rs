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
            data_dir:       PathBuf::from("/var/lib/pulseagent"),
            log_level:      "info".into(),
            instance_name:  None,
            enroll_token:   None,
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
    pub file_logs:    Vec<FileLogsConfig>,
    pub prom_scrape:  Vec<PromScrapeConfig>,
    /// Stub — OTLP HTTP/JSON receiver on a local port
    pub otlp:         Option<OtlpConfig>,
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
            interval:     default_interval(),
            collectors:   vec![],
            extra_labels: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileLogsConfig {
    pub name:  String,
    pub paths: Vec<String>,

    /// Multiline start pattern (regex). Lines that do NOT match are appended
    /// to the previous log entry.
    pub multiline_start: Option<String>,

    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromScrapeConfig {
    pub name:   String,
    pub url:    String,

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

fn default_interval()       -> String { "15s".into() }
fn default_scrape_interval() -> String { "30s".into() }
fn default_otlp_port()       -> u16   { 4318 }

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    /// Maximum number of signals in a batch before flushing
    pub max_size:       usize,
    /// Maximum time to hold a partial batch before flushing (e.g. "5s")
    pub max_delay:      String,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self { max_size: 1000, max_delay: "5s".into() }
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
    pub separator:     Option<String>,
    pub target_label:  Option<String>,
    pub regex:         Option<String>,
    pub replacement:   Option<String>,
    pub action:        RelabelAction,
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
    pub field:       String,
    /// Regex whose full matches are replaced
    pub pattern:     String,
    /// Replacement string (default: "[[redacted]]")
    pub replacement: Option<String>,
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
    pub url: String,
    /// API key. Supports ${env:VAR} expansion.
    pub api_key: Option<String>,
    /// Send metrics as OTLP JSON (default: true)
    #[serde(default = "default_true")]
    pub otlp_metrics: bool,
    /// Send logs as Loki push JSON (default: true)
    #[serde(default = "default_true")]
    pub loki_logs: bool,
}

fn default_true() -> bool { true }

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
            agent:      AgentConfig::default(),
            sources:    SourcesConfig {
                host_metrics: Some(HostMetricsConfig::default()),
                ..Default::default()
            },
            processors: ProcessorsConfig::default(),
            targets:    TargetsConfig::default(),
        });
    }

    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading {:?}", path))?;

    // Perform ${env:VAR} expansion before parsing TOML
    let expanded = expand_env_vars(&raw);

    let cfg: Config = toml::from_str(&expanded)
        .with_context(|| format!("parsing {:?}", path))?;

    validate(&cfg)?;
    Ok(cfg)
}

fn expand_env_vars(s: &str) -> String {
    let re = regex::Regex::new(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}").unwrap();
    re.replace_all(s, |caps: &regex::Captures| {
        std::env::var(&caps[1]).unwrap_or_default()
    }).into_owned()
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

    Ok(())
}

/// Parse a duration string like "15s", "1m", "2h" into seconds.
pub fn parse_duration_secs(s: &str) -> Result<u64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('s') {
        return n.trim().parse::<u64>().with_context(|| format!("bad duration {:?}", s));
    }
    if let Some(n) = s.strip_suffix('m') {
        return Ok(n.trim().parse::<u64>().with_context(|| format!("bad duration {:?}", s))? * 60);
    }
    if let Some(n) = s.strip_suffix('h') {
        return Ok(n.trim().parse::<u64>().with_context(|| format!("bad duration {:?}", s))? * 3600);
    }
    // Plain integer = seconds
    s.parse::<u64>().with_context(|| format!("bad duration {:?}", s))
}
