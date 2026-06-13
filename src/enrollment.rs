use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::config::Config;

// Shared HTTP client. Reusing one client keeps the connection pool warm
// (HTTP/2 / keep-alive). Building a fresh `reqwest::Client` per checkin
// triggers a cold TLS handshake every minute and, through Caddy's
// on-demand TLS + Fly flycast hop, intermittently surfaces as rustls
// `received corrupt message of type InvalidContentType` errors.
static HTTP: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client")
});

/// Credentials persisted after a successful enrollment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCredentials {
    pub agent_id: String,
    pub api_key: String,
    pub base_url: String,
}

const CREDS_FILE: &str = "credentials.json";

/// Ensure the agent is enrolled with PulseBoard.
///
/// Logic:
/// 1. If `data_dir/credentials.json` exists and is readable — return it.
///    Stale `direct-*` agent ids (from the pre-self-enroll release) are
///    treated as corrupt so we re-enroll cleanly.
/// 2. If `agent.enroll_token` is set — exchange it for permanent creds.
/// 3. If `targets.pulseboard.api_key` is set — POST it to
///    `/api/agent/v1/enroll` as a tenant bearer; the workspace mints a
///    real agent record and returns its permanent (agent_id, api_key)
///    pair. Idempotent server-side by (tenant, hostname).
/// 4. Otherwise — warn and return a "local-only" placeholder (dry-run
///    or misconfigured; signals will still be collected).
pub async fn ensure_enrolled(cfg: &Config) -> Result<AgentCredentials> {
    let creds_path = cfg.agent.data_dir.join(CREDS_FILE);

    // 1. Existing creds
    if creds_path.exists() {
        match load_creds(&creds_path) {
            Ok(creds) if !creds.agent_id.starts_with("direct-") => {
                debug!("loaded existing credentials from {:?}", creds_path);
                return Ok(creds);
            }
            Ok(_) => {
                warn!(
                    "credentials file {:?} carries a stale direct-* agent id \
                     from the pre-self-enroll release; re-enrolling",
                    creds_path
                );
            }
            Err(_) => {
                warn!(
                    "credentials file {:?} is corrupt — re-enrolling",
                    creds_path
                );
            }
        }
    }

    // 2. Enrollment token exchange
    if let Some(token) = &cfg.agent.enroll_token {
        let base_url = cfg
            .agent
            .pulseboard_url
            .clone()
            .or_else(|| cfg.targets.pulseboard.as_ref().and_then(|t| t.url.clone()))
            .context("enroll_token set but no pulseboard_url configured")?;

        let creds = enroll(&base_url, Some(token), None).await?;
        persist_creds(&creds_path, &creds)?;
        info!("enrolled via token as agent_id={}", creds.agent_id);
        return Ok(creds);
    }

    // 3. Self-enrol via tenant API key. The workspace's
    //    /api/agent/v1/enroll accepts a tenant `pk_...` bearer as an
    //    alternative to a single-use token; it mints (or rotates) a
    //    per-host agent record keyed by hostname and returns permanent
    //    agent credentials. From then on the agent uses those for
    //    OTLP / checkin and the tenant key isn't sent again.
    if let Some(target) = &cfg.targets.pulseboard {
        if let Some(api_key) = &target.api_key {
            let base_url = target
                .url
                .clone()
                .or_else(|| cfg.agent.pulseboard_url.clone())
                .context("api_key set but no url configured in [targets.pulseboard] or agent.pulseboard_url")?;
            let creds = enroll(&base_url, None, Some(api_key)).await?;
            persist_creds(&creds_path, &creds)?;
            info!(
                "self-enrolled via tenant key as agent_id={}",
                creds.agent_id
            );
            return Ok(creds);
        }
    }

    // 4. No creds — local-only mode
    warn!(
        "no enrollment token or API key configured; signals will be collected but NOT shipped. \
        Set [agent] enroll_token or [targets.pulseboard] api_key."
    );
    Ok(AgentCredentials {
        agent_id: "local".into(),
        api_key: String::new(),
        base_url: String::new(),
    })
}

/// POST /api/agent/v1/enroll. Exactly one of `token` / `tenant_key` must
/// be provided. `token` goes in the JSON body (legacy operator-minted
/// token flow); `tenant_key` is sent as a `Bearer` header (self-enroll
/// via a long-lived tenant key, e.g. dogfood Fly secrets).
async fn enroll(
    base_url: &str,
    token: Option<&str>,
    tenant_key: Option<&str>,
) -> Result<AgentCredentials> {
    let url = format!("{}/api/agent/v1/enroll", base_url.trim_end_matches('/'));

    let hostname = hostname::get()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();

    let mut body = serde_json::json!({
        "hostname": hostname,
        "version":  env!("CARGO_PKG_VERSION"),
    });
    if let Some(t) = token {
        body["token"] = serde_json::Value::String(t.to_string());
    }

    let mut req = HTTP.post(&url).json(&body);
    if let Some(key) = tenant_key {
        req = req.bearer_auth(key);
    }

    let resp = req.send().await.with_context(|| {
        format!(
            "POST {url} — check that pulseboard_url starts with https:// \
            and the host is reachable"
        )
    })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("enroll failed HTTP {status}: {text}");
    }

    let json: serde_json::Value = resp.json().await.context("parse enroll response")?;

    Ok(AgentCredentials {
        agent_id: json["agentId"]
            .as_str()
            .context("missing agentId in enroll response")?
            .to_string(),
        api_key: json["apiKey"]
            .as_str()
            .context("missing apiKey in enroll response")?
            .to_string(),
        base_url: base_url.to_string(),
    })
}

/// Send a checkin heartbeat. Called once a minute from the pipeline.
pub async fn checkin(
    creds: &AgentCredentials,
    version: &str,
    config_hash: &str,
    stats: &serde_json::Value,
) -> Result<()> {
    if creds.base_url.is_empty() {
        return Ok(()); // local-only mode
    }

    let url = format!(
        "{}/api/agent/v1/checkin",
        creds.base_url.trim_end_matches('/')
    );

    let body = serde_json::json!({
        "version":    version,
        "configHash": config_hash,
        "stats":      stats,
    });

    let resp = HTTP
        .post(&url)
        .bearer_auth(format!("{}:{}", creds.agent_id, creds.api_key))
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        tracing::warn!("checkin failed HTTP {}: {}", status, text);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn load_creds(path: &Path) -> Result<AgentCredentials> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

fn persist_creds(path: &Path, creds: &AgentCredentials) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {parent:?}"))?;
    }
    let raw = serde_json::to_string_pretty(creds)?;
    std::fs::write(path, raw).with_context(|| format!("write {path:?}"))?;
    Ok(())
}
