use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{parse_duration_secs, Config};
use crate::config_poller::AppliedVersion;
use crate::enrollment::{checkin, AgentCredentials};
use crate::processors::{
    batch::BatchProcessor, cardinality_guard::CardinalityGuard, redact_pii::PiiRedactor,
    relabel::Relabeler, transform::Transformer,
};
use crate::signal::Signal;
use crate::sources::{
    docker::{DockerLogsSource, DockerStatsSource},
    file_logs::FileLogsSource,
    host_metrics::HostMetricsSource,
    journald::JournaldSource,
    kubernetes_pods::KubernetesPodsSource,
    otlp_receiver::OtlpReceiverSource,
    prom_scrape::PromScrapeSource,
    windows_event_log::WindowsEventLogSource,
};
use crate::targets::pulseboard::PulseBoardTarget;
use crate::web::{Inspector, TopoEdge, TopoNode, Topology};

const CHANNEL_CAPACITY: usize = 8192;

// Canonical pipeline stage identifiers (also used as graph node ids).
const STAGE_RELABEL: &str = "relabel";
const STAGE_TRANSFORM: &str = "transform";
const STAGE_CARDINALITY: &str = "cardinality_guard";
const STAGE_REDACT: &str = "redact_pii";
const STAGE_BATCH: &str = "batch";

pub async fn run(
    cfg: Config,
    creds: AgentCredentials,
    inspector: Inspector,
    dry_run: bool,
    applied_version: Arc<AppliedVersion>,
    mut reload_rx: mpsc::Receiver<()>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<Signal>(CHANNEL_CAPACITY);
    let cfg = Arc::new(cfg);

    // Publish the lint results so the debugger's Linter tab is populated
    // immediately, before any data flows.
    inspector.set_lint(crate::lint::lint(&cfg));

    // ----- Sources ---------------------------------------------------------

    let mut source_handles = vec![];
    let mut source_nodes: Vec<TopoNode> = vec![];

    if let Some(hm_cfg) = &cfg.sources.host_metrics {
        let tx2 = tx.clone();
        let hm_cfg = hm_cfg.clone();
        let insp = inspector.clone();
        source_handles.push(tokio::spawn(async move {
            HostMetricsSource::new(hm_cfg).run(tx2, insp).await
        }));
        info!("source: host_metrics enabled");
        source_nodes.push(node("source:host_metrics", "source", "host_metrics"));
    }

    for fl_cfg in &cfg.sources.file_logs {
        let tx2 = tx.clone();
        let fl_cfg = fl_cfg.clone();
        let insp = inspector.clone();
        info!(
            "source: file_logs {:?} (paths: {:?})",
            fl_cfg.name, fl_cfg.paths
        );
        source_nodes.push(node(
            &format!("source:{}", fl_cfg.name),
            "source",
            &format!("file_logs · {}", fl_cfg.name),
        ));
        source_handles.push(tokio::spawn(async move {
            FileLogsSource::new(fl_cfg).run(tx2, insp).await
        }));
    }

    for ps_cfg in &cfg.sources.prom_scrape {
        let tx2 = tx.clone();
        let ps_cfg = ps_cfg.clone();
        let insp = inspector.clone();
        info!(
            "source: prom_scrape {:?} (url: {})",
            ps_cfg.name, ps_cfg.url
        );
        source_nodes.push(node(
            &format!("source:{}", ps_cfg.name),
            "source",
            &format!("prom_scrape · {}", ps_cfg.name),
        ));
        source_handles.push(tokio::spawn(async move {
            PromScrapeSource::new(ps_cfg).run(tx2, insp).await
        }));
    }

    if let Some(jd_cfg) = &cfg.sources.journald {
        let tx2 = tx.clone();
        let jd_cfg = jd_cfg.clone();
        let insp = inspector.clone();
        info!("source: journald enabled");
        source_nodes.push(node("source:journald", "source", "journald"));
        source_handles.push(tokio::spawn(async move {
            JournaldSource::new(jd_cfg).run(tx2, insp).await
        }));
    }

    for we_cfg in &cfg.sources.windows_event_log {
        let tx2 = tx.clone();
        let we_cfg = we_cfg.clone();
        let insp = inspector.clone();
        info!(
            "source: windows_event_log {:?} ({})",
            we_cfg.name, we_cfg.channel
        );
        source_nodes.push(node(
            &format!("source:{}", we_cfg.name),
            "source",
            &format!("win_event · {}", we_cfg.name),
        ));
        source_handles.push(tokio::spawn(async move {
            WindowsEventLogSource::new(we_cfg).run(tx2, insp).await
        }));
    }

    if let Some(dl_cfg) = &cfg.sources.docker_logs {
        let tx2 = tx.clone();
        let dl_cfg = dl_cfg.clone();
        let insp = inspector.clone();
        info!("source: docker_logs enabled");
        source_nodes.push(node("source:docker_logs", "source", "docker_logs"));
        source_handles.push(tokio::spawn(async move {
            DockerLogsSource::new(dl_cfg).run(tx2, insp).await
        }));
    }

    if let Some(ds_cfg) = &cfg.sources.docker_stats {
        let tx2 = tx.clone();
        let ds_cfg = ds_cfg.clone();
        let insp = inspector.clone();
        info!("source: docker_stats enabled");
        source_nodes.push(node("source:docker_stats", "source", "docker_stats"));
        source_handles.push(tokio::spawn(async move {
            DockerStatsSource::new(ds_cfg).run(tx2, insp).await
        }));
    }

    if let Some(kp_cfg) = &cfg.sources.kubernetes_pods {
        let tx2 = tx.clone();
        let kp_cfg = kp_cfg.clone();
        let insp = inspector.clone();
        info!("source: kubernetes_pods enabled (dir: {})", kp_cfg.log_dir);
        source_nodes.push(node("source:kubernetes_pods", "source", "kubernetes_pods"));
        source_handles.push(tokio::spawn(async move {
            KubernetesPodsSource::new(kp_cfg).run(tx2, insp).await
        }));
    }

    if let Some(otlp_cfg) = &cfg.sources.otlp {
        let tx2 = tx.clone();
        let otlp_cfg = otlp_cfg.clone();
        let insp = inspector.clone();
        info!("source: otlp_receiver enabled (port: {})", otlp_cfg.port);
        source_nodes.push(node("source:otlp_receiver", "source", "otlp_receiver"));
        source_handles.push(tokio::spawn(async move {
            OtlpReceiverSource::new(otlp_cfg).run(tx2, insp).await
        }));
    }

    // Drop the last sender so the pipeline loop ends when all sources exit
    drop(tx);

    // ----- Processors ------------------------------------------------------

    let relabeler = if cfg.processors.relabel.is_empty() {
        None
    } else {
        Some(Relabeler::new(&cfg.processors.relabel))
    };

    let transformer = if cfg.processors.transform.is_empty() {
        None
    } else {
        Some(Transformer::new(&cfg.processors.transform))
    };

    let cardinality_guard = cfg
        .processors
        .cardinality_guard
        .as_ref()
        .map(|c| CardinalityGuard::new(c.max_series_per_metric));

    let redactor = if cfg.processors.redact_pii.is_empty() {
        None
    } else {
        Some(PiiRedactor::new(&cfg.processors.redact_pii))
    };

    let batch_delay_secs = parse_duration_secs(&cfg.processors.batch.max_delay).unwrap_or(5);
    let batch_max = cfg.processors.batch.max_size;
    let mut batcher = BatchProcessor::new(batch_max, batch_delay_secs);

    // ----- Pipeline topology (for the live-debugger graph) -----------------

    let target_node = node("target:pulseboard", "target", "pulseboard");
    inspector.set_topology(build_topology(
        &source_nodes,
        relabeler.is_some(),
        transformer.is_some(),
        cardinality_guard.is_some(),
        redactor.is_some(),
        target_node.clone(),
    ));

    // ----- Target ----------------------------------------------------------

    let target: Option<PulseBoardTarget> = if dry_run {
        info!("dry-run mode — signals will be printed, not shipped");
        None
    } else if !creds.base_url.is_empty() {
        let pb_cfg = cfg.targets.pulseboard.clone();
        Some(PulseBoardTarget::new(creds.clone(), pb_cfg))
    } else {
        warn!("no target configured — signals will be dropped after processing");
        None
    };

    // ----- Checkin ticker --------------------------------------------------

    let creds_clone = creds.clone();
    let applied_clone = applied_version.clone();
    let checkin_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await; // consume the immediate tick
        loop {
            interval.tick().await;
            let stats = serde_json::json!({ "version": env!("CARGO_PKG_VERSION") });
            let v = applied_clone.get().await;
            let cfg_hash = v.to_string();
            if let Err(e) =
                checkin(&creds_clone, env!("CARGO_PKG_VERSION"), &cfg_hash, &stats).await
            {
                warn!("checkin error: {:#}", e);
            }
        }
    });

    // ----- Pipeline loop ---------------------------------------------------

    let mut signals_in: u64 = 0;
    let mut signals_out: u64 = 0;
    let mut signals_drop: u64 = 0;

    let reload_requested = loop {
        let signal = tokio::select! {
            biased;
            _ = reload_rx.recv() => break true,
            maybe = rx.recv() => match maybe {
                Some(s) => s,
                None => break false,
            },
        };
        signals_in += 1;

        // Relabel
        let signal = if let Some(ref r) = relabeler {
            match r.apply_traced(signal) {
                Ok(s) => {
                    inspector.record_stage_pass(STAGE_RELABEL);
                    s
                }
                Err((dropped, reason)) => {
                    inspector.record_drop(&dropped, STAGE_RELABEL, &reason);
                    signals_drop += 1;
                    continue;
                }
            }
        } else {
            signal
        };

        // Transform
        let signal = if let Some(ref t) = transformer {
            let s = t.apply(signal);
            inspector.record_stage_pass(STAGE_TRANSFORM);
            s
        } else {
            signal
        };

        // Cardinality guard
        if let Signal::Metric(ref m) = signal {
            if let Some(ref cg) = cardinality_guard {
                if cg.check_and_record(&m.name, &m.labels)
                    == crate::processors::cardinality_guard::Verdict::Drop
                {
                    inspector.record_drop(
                        &signal,
                        STAGE_CARDINALITY,
                        &format!(
                            "metric {} exceeded the configured series budget ({} max) — new series dropped",
                            m.name,
                            cg.max_series()
                        ),
                    );
                    signals_drop += 1;
                    continue;
                }
                inspector.record_stage_pass(STAGE_CARDINALITY);
            }
        }

        // Redact PII (mutates in place; never drops)
        let signal = if let Some(ref red) = redactor {
            let s = red.apply(signal);
            inspector.record_stage_pass(STAGE_REDACT);
            s
        } else {
            signal
        };

        // Feed inspector
        inspector.record_signal(&signal);

        // Batch
        inspector.record_stage_pass(STAGE_BATCH);
        if let Some(batch) = batcher.push(signal) {
            let n = batch.len();
            if dry_run {
                for s in &batch {
                    println!("{s:?}");
                }
            } else if let Some(ref t) = target {
                match t.flush(batch).await {
                    Ok(()) => signals_out += n as u64,
                    Err(e) => warn!("target flush error: {:#}", e),
                }
            }
        }
    };

    // Flush any remaining signals
    if let Some(batch) = batcher.drain() {
        let n = batch.len();
        if dry_run {
            for s in &batch {
                println!("{s:?}");
            }
        } else if let Some(ref t) = target {
            if let Err(e) = t.flush(batch).await {
                warn!("final flush error: {:#}", e);
            } else {
                signals_out += n as u64;
            }
        }
    }

    info!(signals_in, signals_out, signals_drop, "pipeline finished");

    // Stop the checkin task before returning so the next pipeline
    // build doesn't end up with two of them.
    checkin_task.abort();
    let _ = reload_requested; // currently unused beyond loop control

    Ok(())
}

fn node(id: &str, kind: &str, label: &str) -> TopoNode {
    TopoNode {
        id: id.to_string(),
        kind: kind.to_string(),
        label: label.to_string(),
    }
}

/// Build the linear source → processors → target graph for the debugger,
/// including only the processor stages that are actually enabled.
fn build_topology(
    sources: &[TopoNode],
    has_relabel: bool,
    has_transform: bool,
    has_cardinality: bool,
    has_redact: bool,
    target: TopoNode,
) -> Topology {
    let mut nodes: Vec<TopoNode> = sources.to_vec();
    let mut processors: Vec<TopoNode> = vec![];
    if has_relabel {
        processors.push(node(STAGE_RELABEL, "processor", "relabel"));
    }
    if has_transform {
        processors.push(node(STAGE_TRANSFORM, "processor", "transform"));
    }
    if has_cardinality {
        processors.push(node(STAGE_CARDINALITY, "processor", "cardinality_guard"));
    }
    if has_redact {
        processors.push(node(STAGE_REDACT, "processor", "redact_pii"));
    }
    processors.push(node(STAGE_BATCH, "processor", "batch"));

    let mut edges: Vec<TopoEdge> = vec![];
    let first_stage = processors
        .first()
        .map(|p| p.id.clone())
        .unwrap_or_else(|| target.id.clone());
    for s in sources {
        edges.push(TopoEdge {
            from: s.id.clone(),
            to: first_stage.clone(),
        });
    }
    for pair in processors.windows(2) {
        edges.push(TopoEdge {
            from: pair[0].id.clone(),
            to: pair[1].id.clone(),
        });
    }
    if let Some(last) = processors.last() {
        edges.push(TopoEdge {
            from: last.id.clone(),
            to: target.id.clone(),
        });
    }

    nodes.extend(processors);
    nodes.push(target);
    Topology { nodes, edges }
}
