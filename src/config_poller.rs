//! Desired-config polling for Phase 13.5.
//!
//! The agent periodically asks the edge "what config should I be
//! running?" via `GET /api/agent/v1/config` and, when the answer's
//! version changes, atomically:
//!
//!   1. verifies the HMAC-SHA256 signature using the per-tenant key it
//!      learned at enrollment (defence-in-depth — the bearer already
//!      authenticates the channel);
//!   2. computes `effective = overlay ⊕ base`, where `base` is the
//!      operator's on-disk `agent.toml` and `overlay` is the partial
//!      TOML the edge serves for the agent's group;
//!   3. writes the effective TOML next to the base file and signals the
//!      main loop to rebuild the pipeline in-process.
//!
//! Overlay semantics: recursive table merge. Nested tables merge
//! key-by-key; scalars and arrays replace wholesale. The base file is
//! never mutated — only the effective file is rewritten.
//!
//! Failure modes are non-fatal: poll errors, signature mismatches, and
//! TOML parse errors are logged at WARN and the loop sleeps until the
//! next tick. The pipeline keeps running on whatever config it has.

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::enrollment::AgentCredentials;

type HmacSha256 = Hmac<Sha256>;

/// How often to poll the edge for a new config.
const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Shared, mutable view of the currently-applied config version. The
/// main loop reads it once per reload so its checkins can advertise the
/// real `configHash` (and the portal's drift indicator clears).
#[derive(Debug, Default)]
pub struct AppliedVersion(Mutex<u32>);

impl AppliedVersion {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub async fn get(&self) -> u32 {
        *self.0.lock().await
    }
    pub async fn set(&self, v: u32) {
        *self.0.lock().await = v;
    }
}

/// Edge response payload for /api/agent/v1/config.
#[derive(Debug, serde::Deserialize)]
struct ConfigResponse {
    #[serde(rename = "tenantId", default)]
    tenant_id: String,
    #[serde(rename = "groupId")]
    group_id: String,
    #[serde(rename = "version")]
    version: u32,
    #[serde(rename = "overlayToml", default)]
    overlay_toml: String,
    #[serde(rename = "signature")]
    signature: String,
}

/// Run the poll loop forever. On every successful version change it
/// sends `()` on `reload_tx`, which the main loop consumes to rebuild
/// the pipeline.
///
/// `base_config_path` is the operator's `agent.toml` (immutable from
/// here on); `effective_config_path` is where we write the merged
/// result, which is what the next `pipeline::run` will load.
pub async fn run(
    creds: AgentCredentials,
    base_config_path: PathBuf,
    effective_config_path: PathBuf,
    applied: Arc<AppliedVersion>,
    reload_tx: tokio::sync::mpsc::Sender<()>,
) {
    // We can't poll without a server URL or an HMAC key. The latter
    // would be missing only for pre-13.5 credentials files; just stay
    // idle until the next enrollment refreshes them.
    if creds.base_url.is_empty() || creds.hmac_key_b64.is_empty() {
        debug!("config_poller: missing base_url or hmac key; disabling");
        return;
    }

    let hmac_key = match base64::engine::general_purpose::STANDARD.decode(&creds.hmac_key_b64) {
        Ok(k) => k,
        Err(e) => {
            warn!("config_poller: hmac key is not valid base64: {e}; disabling");
            return;
        }
    };

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    // Stagger the first poll a few seconds after startup so it doesn't
    // race the initial pipeline build.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut interval = tokio::time::interval(POLL_INTERVAL);

    loop {
        interval.tick().await;
        match poll_once(
            &http,
            &creds,
            &hmac_key,
            &base_config_path,
            &effective_config_path,
            &applied,
        )
        .await
        {
            Ok(Outcome::Unchanged) => debug!("config_poller: no change"),
            Ok(Outcome::Applied { version }) => {
                info!(
                    version,
                    "config_poller: applied new config; reloading pipeline"
                );
                if reload_tx.send(()).await.is_err() {
                    warn!("config_poller: reload channel closed; exiting");
                    return;
                }
            }
            Err(e) => warn!("config_poller: {e:#}"),
        }
    }
}

enum Outcome {
    Unchanged,
    Applied { version: u32 },
}

async fn poll_once(
    http: &reqwest::Client,
    creds: &AgentCredentials,
    hmac_key: &[u8],
    base_path: &Path,
    effective_path: &Path,
    applied: &AppliedVersion,
) -> Result<Outcome> {
    let url = format!(
        "{}/api/agent/v1/config",
        creds.base_url.trim_end_matches('/')
    );

    let resp = http
        .get(&url)
        .bearer_auth(format!("{}:{}", creds.agent_id, creds.api_key))
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;

    if !resp.status().is_success() {
        anyhow::bail!("GET {url} returned HTTP {}", resp.status());
    }

    let payload: ConfigResponse = resp.json().await.context("parse config response")?;

    // Tenant id arrived empty for pre-13.5 enrollments; fall back to
    // whatever the agent stored locally.
    let tenant_id = if payload.tenant_id.is_empty() {
        creds.tenant_id.as_str()
    } else {
        payload.tenant_id.as_str()
    };
    if tenant_id.is_empty() {
        anyhow::bail!("no tenant id available to verify config signature");
    }

    verify(hmac_key, tenant_id, &payload)?;

    let current = applied.get().await;
    if payload.version == current && effective_path.exists() {
        return Ok(Outcome::Unchanged);
    }

    // First-contact no-op: the agent boots with `AppliedVersion = 0` and
    // every tenant has an auto-materialised empty `default` group at
    // version 1. Treating that as "new" would force a pointless reload
    // on every fresh start. If there's no overlay to apply and the
    // agent isn't assigned to a custom group, just record the version
    // and keep the base config in place.
    if current == 0
        && payload.overlay_toml.trim().is_empty()
        && payload.group_id == "default"
    {
        applied.set(payload.version).await;
        return Ok(Outcome::Unchanged);
    }

    let base = std::fs::read_to_string(base_path)
        .with_context(|| format!("read base config {base_path:?}"))?;
    let merged = merge(&base, &payload.overlay_toml)?;

    write_atomic(effective_path, &merged)?;
    applied.set(payload.version).await;
    Ok(Outcome::Applied {
        version: payload.version,
    })
}

/// Recompute the canonical bytes the edge signed and check the HMAC.
fn verify(key: &[u8], tenant_id: &str, p: &ConfigResponse) -> Result<()> {
    let canonical = format!(
        "v1|{}|{}|{}|{}",
        tenant_id, p.group_id, p.version, p.overlay_toml
    );
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(canonical.as_bytes());
    let expected = mac.finalize().into_bytes();
    let expected_hex = hex_upper(&expected);
    if !constant_time_eq(expected_hex.as_bytes(), p.signature.as_bytes()) {
        return Err(anyhow!("config signature mismatch — refusing to apply"));
    }
    Ok(())
}

fn hex_upper(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// `effective = overlay ⊕ base`, recursive on tables. Returns the
/// effective TOML as a serialised string. An empty overlay returns the
/// base unchanged.
pub fn merge(base_toml: &str, overlay_toml: &str) -> Result<String> {
    if overlay_toml.trim().is_empty() {
        return Ok(base_toml.to_owned());
    }
    let mut base: toml::Value = toml::from_str(base_toml).context("parse base config")?;
    let overlay: toml::Value = toml::from_str(overlay_toml).context("parse overlay config")?;
    merge_value(&mut base, overlay);
    toml::to_string(&base).context("serialise merged config")
}

fn merge_value(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(b), toml::Value::Table(o)) => {
            for (k, v) in o {
                match b.get_mut(&k) {
                    Some(existing) => merge_value(existing, v),
                    None => {
                        b.insert(k, v);
                    }
                }
            }
        }
        (slot, other) => {
            *slot = other;
        }
    }
}

fn write_atomic(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {parent:?}"))?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents).with_context(|| format!("write {tmp:?}"))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename {tmp:?} -> {path:?}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_replaces_scalars_and_merges_tables() {
        let base = r#"
[agent]
data_dir = "/var/lib/pulseagent"

[sources.host_metrics]
interval = "30s"

[sources.host_metrics.collectors]
cpu = true
"#;
        let overlay = r#"
[sources.host_metrics]
interval = "10s"

[sources.host_metrics.collectors]
disk = true
"#;
        let merged = merge(base, overlay).unwrap();
        let v: toml::Value = toml::from_str(&merged).unwrap();
        assert_eq!(
            v["sources"]["host_metrics"]["interval"].as_str(),
            Some("10s")
        );
        // table merge — both keys survive
        let collectors = &v["sources"]["host_metrics"]["collectors"];
        assert_eq!(collectors["cpu"].as_bool(), Some(true));
        assert_eq!(collectors["disk"].as_bool(), Some(true));
        // untouched section preserved
        assert_eq!(v["agent"]["data_dir"].as_str(), Some("/var/lib/pulseagent"));
    }

    #[test]
    fn empty_overlay_is_identity() {
        let base = "[agent]\ndata_dir = \"/x\"\n";
        assert_eq!(merge(base, "").unwrap(), base);
        assert_eq!(merge(base, "   \n\t  ").unwrap(), base);
    }

    #[test]
    fn merge_replaces_arrays_wholesale() {
        let base = r#"
[[sources.file_logs]]
name = "a"
paths = ["/x"]
"#;
        let overlay = r#"
[[sources.file_logs]]
name = "b"
paths = ["/y"]
"#;
        let merged = merge(base, overlay).unwrap();
        let v: toml::Value = toml::from_str(&merged).unwrap();
        let arr = v["sources"]["file_logs"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"].as_str(), Some("b"));
    }

    #[test]
    fn verify_round_trips_with_signing() {
        let key = b"some-secret-key-bytes";
        let payload = ConfigResponse {
            tenant_id: "t1".into(),
            group_id: "default".into(),
            version: 7,
            overlay_toml: "x = 1".into(),
            signature: {
                let mut mac = HmacSha256::new_from_slice(key).unwrap();
                mac.update(b"v1|t1|default|7|x = 1");
                hex_upper(&mac.finalize().into_bytes())
            },
        };
        verify(key, "t1", &payload).unwrap();
    }

    #[test]
    fn verify_rejects_mismatched_version() {
        let key = b"k";
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(b"v1|t1|default|7|");
        let sig = hex_upper(&mac.finalize().into_bytes());
        let payload = ConfigResponse {
            tenant_id: "t1".into(),
            group_id: "default".into(),
            version: 8, // wrong
            overlay_toml: "".into(),
            signature: sig,
        };
        assert!(verify(key, "t1", &payload).is_err());
    }
}
