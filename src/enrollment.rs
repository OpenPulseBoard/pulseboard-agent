use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{debug, info, warn};

use crate::config::Config;

/// Credentials persisted after a successful enrollment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCredentials {
    pub agent_id:    String,
    pub api_key:     String,
    pub base_url:    String,
}

const CREDS_FILE: &str = "credentials.json";

/// Ensure the agent is enrolled with PulseBoard.
///
/// Logic:
/// 1. If `data_dir/credentials.json` exists and is readable — return it.
/// 2. If `agent.enroll_token` is set — exchange it for permanent creds.
/// 3. If `targets.pulseboard.api_key` is set — use that directly (no
///    enrollment ceremony; useful for self-hosted workspaces where the
///    operator already has an API key).
/// 4. Otherwise — warn and return a "local-only" placeholder (dry-run
///    or misconfigured; signals will still be collected).
pub async fn ensure_enrolled(cfg: &Config) -> Result<AgentCredentials> {
    let creds_path = cfg.agent.data_dir.join(CREDS_FILE);

    // 1. Existing creds
    if creds_path.exists() {
        if let Ok(creds) = load_creds(&creds_path) {
            debug!("loaded existing credentials from {:?}", creds_path);
            return Ok(creds);
        }
        warn!("credentials file {:?} is corrupt — re-enrolling", creds_path);
    }

    // 2. Enrollment token exchange
    if let Some(token) = &cfg.agent.enroll_token {
        let base_url = cfg
            .agent
            .pulseboard_url
            .clone()
            .or_else(|| cfg.targets.pulseboard.as_ref().map(|t| t.url.clone()))
            .context("enroll_token set but no pulseboard_url configured")?;

        let creds = exchange_token(&base_url, token).await?;
        persist_creds(&creds_path, &creds)?;
        info!("enrolled as agent_id={}", creds.agent_id);
        return Ok(creds);
    }

    // 3. Direct API key (no enrollment round-trip)
    if let Some(target) = &cfg.targets.pulseboard {
        if let Some(api_key) = &target.api_key {
            let creds = AgentCredentials {
                agent_id: format!("direct-{}", uuid::Uuid::new_v4()),
                api_key:  api_key.clone(),
                base_url: target.url.clone(),
            };
            persist_creds(&creds_path, &creds)?;
            info!("using direct API key (no enrollment)");
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
        api_key:  String::new(),
        base_url: String::new(),
    })
}

async fn exchange_token(base_url: &str, token: &str) -> Result<AgentCredentials> {
    let url = format!("{}/api/agent/v1/enroll", base_url.trim_end_matches('/'));

    let hostname = hostname::get()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();

    let body = serde_json::json!({
        "token":    token,
        "hostname": hostname,
        "version":  env!("CARGO_PKG_VERSION"),
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {}", url))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("enroll failed HTTP {}: {}", status, text);
    }

    let json: serde_json::Value = resp.json().await.context("parse enroll response")?;

    Ok(AgentCredentials {
        agent_id: json["agentId"]
            .as_str()
            .context("missing agentId in enroll response")?
            .to_string(),
        api_key:  json["apiKey"]
            .as_str()
            .context("missing apiKey in enroll response")?
            .to_string(),
        base_url: base_url.to_string(),
    })
}

/// Send a checkin heartbeat. Called once a minute from the pipeline.
pub async fn checkin(
    creds: &AgentCredentials,
    version:     &str,
    config_hash: &str,
    stats:       &serde_json::Value,
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

    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .bearer_auth(&creds.api_key)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {}", url))?;

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
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create {:?}", parent))?;
    }
    let raw = serde_json::to_string_pretty(creds)?;
    std::fs::write(path, raw).with_context(|| format!("write {:?}", path))?;
    Ok(())
}
