use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::info;

mod config;
mod enrollment;
mod pipeline;
mod processors;
mod signal;
mod sources;
mod targets;
mod web;

#[derive(Parser, Debug)]
#[command(
    name = "pulseagent",
    about = "PulseBoard telemetry agent — zero-config host metrics, logs, traces",
    version
)]
struct Cli {
    /// Path to the agent configuration file (TOML)
    #[arg(
        short,
        long,
        default_value = "/etc/pulseagent/agent.toml",
        env = "PULSEAGENT_CONFIG"
    )]
    config: PathBuf,

    /// Override log level (trace|debug|info|warn|error)
    #[arg(long, env = "PULSEAGENT_LOG")]
    log_level: Option<String>,

    /// Check config and exit
    #[arg(long)]
    check: bool,

    /// Print the resolved configuration and exit
    #[arg(long)]
    print_config: bool,

    /// Run in dry-run mode — collect and process signals, print them, but don't ship
    #[arg(long)]
    dry_run: bool,

    /// Port for the built-in debug UI (default: 8000)
    #[arg(long, default_value_t = 8000, env = "PULSEAGENT_UI_PORT")]
    ui_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialise tracing
    let log_level = cli.log_level.as_deref().unwrap_or("info");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    info!(
        "pulseagent starting (version {})",
        env!("CARGO_PKG_VERSION")
    );

    // Load and validate configuration
    let cfg = config::load(&cli.config).map_err(|e| {
        eprintln!("error: failed to load config {:?}: {}", cli.config, e);
        e
    })?;

    if cli.check {
        println!("Config OK: {:?}", cli.config);
        return Ok(());
    }

    if cli.print_config {
        println!("{}", serde_json::to_string_pretty(&cfg)?);
        return Ok(());
    }

    // Enroll with PulseBoard (no-op if already enrolled or if enrollment
    // is disabled — i.e. we already have a stored agent ID + API key)
    let creds = enrollment::ensure_enrolled(&cfg).await?;
    info!(agent_id = %creds.agent_id, "agent enrolled");

    // Spawn the live-debugger web UI
    let inspector = web::Inspector::new();
    let inspector_clone = inspector.clone();
    let ui_port = cli.ui_port;
    tokio::spawn(async move {
        if let Err(e) = web::serve(inspector_clone, ui_port).await {
            tracing::warn!("web UI error: {}", e);
        }
    });
    info!("debug UI listening on http://127.0.0.1:{}", ui_port);

    // Build and run the pipeline
    pipeline::run(cfg, creds, inspector, cli.dry_run).await
}
