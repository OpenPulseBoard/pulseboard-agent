use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::info;

mod config;
mod config_poller;
mod enrollment;
mod lint;
mod pipeline;
mod processors;
mod signal;
mod sources;
mod targets;
mod web;

#[derive(Parser, Debug, Clone)]
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

    /// Run under the Windows Service Control Manager. Set automatically by the
    /// installed service's command line; not intended for interactive use.
    #[arg(long, hide = true)]
    service: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // On Windows, when launched by the Service Control Manager, hand control to
    // the service dispatcher. It blocks until the service stops and is what
    // lets the process answer SCM start/stop requests promptly.
    #[cfg(windows)]
    if cli.service {
        return windows_service_runner::run(cli);
    }

    // Console / foreground mode (Linux, macOS, or Windows without --service).
    init_tracing(cli.log_level.as_deref().unwrap_or("info"), None);
    build_runtime()?.block_on(run_agent(cli))
}

/// Build the multi-threaded Tokio runtime used in both console and service
/// modes. (We construct it explicitly rather than via `#[tokio::main]` so the
/// Windows service path can drive it after the SCM dispatcher hands off.)
fn build_runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

/// Initialise tracing. When `log_file` is provided (service mode, where there
/// is no console) logs are appended to that file with ANSI disabled; otherwise
/// they go to stdout.
fn init_tracing(level: &str, log_file: Option<std::path::PathBuf>) {
    let make_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level))
    };

    if let Some(path) = log_file {
        if let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            tracing_subscriber::fmt()
                .with_env_filter(make_filter())
                .with_ansi(false)
                .with_writer(move || file.try_clone().expect("clone log file handle"))
                .init();
            return;
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(make_filter())
        .init();
}

/// Run the agent: load config, enroll, start the debug UI, and drive the
/// pipeline. Returns when the pipeline stops (or immediately for the
/// `--check` / `--print-config` shortcuts).
async fn run_agent(cli: Cli) -> Result<()> {
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

    // Phase 13.5: poll the edge for the desired-config overlay. The
    // poller writes the merged TOML to an "effective" path next to the
    // operator's agent.toml; that's what each pipeline build loads.
    let base_config_path = cli.config.clone();
    let effective_config_path = effective_config_path(&base_config_path);
    let applied_version = config_poller::AppliedVersion::new();
    let (reload_tx, mut reload_rx) = tokio::sync::mpsc::channel::<()>(8);

    {
        let creds = creds.clone();
        let applied = applied_version.clone();
        let base = base_config_path.clone();
        let eff = effective_config_path.clone();
        tokio::spawn(async move {
            config_poller::run(creds, base, eff, applied, reload_tx).await;
        });
    }

    // Outer loop: build and run the pipeline; when the poller signals
    // a new version, drain gracefully and rebuild on the next iteration.
    loop {
        // Always re-read the (possibly overlay-merged) config so the new
        // pipeline picks up the freshly-written settings.
        let active_cfg = load_active_config(&base_config_path, &effective_config_path)?;

        // One-shot per-iteration reload signal. We don't keep the
        // poller's mpsc directly here so the next pipeline build always
        // starts with a fresh, empty channel.
        let (inner_tx, inner_rx) = tokio::sync::mpsc::channel::<()>(1);
        let forward = tokio::spawn(async move {
            if reload_rx.recv().await.is_some() {
                let _ = inner_tx.send(()).await;
            }
            reload_rx
        });

        let result = pipeline::run(
            active_cfg,
            creds.clone(),
            inspector.clone(),
            cli.dry_run,
            applied_version.clone(),
            inner_rx,
        )
        .await;

        // Recover the original receiver so the next iteration can keep
        // listening for further reload signals.
        reload_rx = forward.await.expect("forward task panicked");

        match result {
            Ok(()) => {
                info!("pipeline stopped; rebuilding");
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Where to write the overlay-merged "effective" config: the base path
/// with `.effective.toml` appended, e.g.
/// `/etc/pulseagent/agent.toml` -> `/etc/pulseagent/agent.effective.toml`.
fn effective_config_path(base: &std::path::Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "agent".to_string());
    p.set_file_name(format!("{stem}.effective.toml"));
    p
}

fn load_active_config(
    base: &std::path::Path,
    effective: &std::path::Path,
) -> Result<config::Config> {
    if effective.exists() {
        match config::load(effective) {
            Ok(c) => return Ok(c),
            Err(e) => tracing::warn!(
                "failed to load effective config {:?}: {:#}; falling back to base",
                effective,
                e
            ),
        }
    }
    config::load(base)
}

// ---------------------------------------------------------------------------
// Windows service integration
// ---------------------------------------------------------------------------

#[cfg(windows)]
mod windows_service_runner {
    use crate::{build_runtime, init_tracing, run_agent, Cli};
    use anyhow::Result;
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;
    use tokio::sync::Notify;
    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::{define_windows_service, service_dispatcher};

    const SERVICE_NAME: &str = "PulseAgent";

    // The SCM-invoked service_main only receives the service-start arguments,
    // not the process command line, so we stash the parsed CLI here (set before
    // the dispatcher takes over) to recover the --config path and other flags.
    static CLI: OnceLock<Cli> = OnceLock::new();

    define_windows_service!(ffi_service_main, service_main);

    pub fn run(cli: Cli) -> Result<()> {
        let _ = CLI.set(cli);
        // Blocks until the service stops; must be reached quickly after launch.
        service_dispatcher::start(SERVICE_NAME, ffi_service_main)?;
        Ok(())
    }

    fn service_main(_args: Vec<OsString>) {
        if let Err(e) = run_service() {
            tracing::error!("service exited with error: {}", e);
        }
    }

    /// Log next to the configuration file, since a service has no console.
    fn service_log_path(cli: &Cli) -> PathBuf {
        cli.config
            .parent()
            .map(|p| p.join("pulseagent.log"))
            .unwrap_or_else(|| PathBuf::from("pulseagent.log"))
    }

    fn run_service() -> Result<()> {
        let cli = CLI.get().expect("CLI stashed before dispatch").clone();
        init_tracing(
            cli.log_level.as_deref().unwrap_or("info"),
            Some(service_log_path(&cli)),
        );

        let shutdown = Arc::new(Notify::new());
        let shutdown_for_handler = shutdown.clone();

        let event_handler = move |control_event| -> ServiceControlHandlerResult {
            match control_event {
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    shutdown_for_handler.notify_one();
                    ServiceControlHandlerResult::NoError
                }
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        };

        let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)?;

        // Report Running *before* the (potentially slow) enrollment and pipeline
        // startup so the SCM never times out the start request.
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Running,
            controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        })?;

        let result = build_runtime()?.block_on(async {
            tokio::select! {
                r = run_agent(cli) => r,
                _ = shutdown.notified() => Ok(()),
            }
        });

        // Always report Stopped, propagating a non-zero exit code on failure.
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Stopped,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code: ServiceExitCode::Win32(if result.is_ok() { 0 } else { 1 }),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        })?;

        result
    }
}
