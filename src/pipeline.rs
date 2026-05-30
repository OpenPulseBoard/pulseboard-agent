use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{parse_duration_secs, Config};
use crate::enrollment::{checkin, AgentCredentials};
use crate::processors::{
    batch::BatchProcessor, cardinality_guard::CardinalityGuard, relabel::Relabeler,
};
use crate::signal::Signal;
use crate::sources::{
    file_logs::FileLogsSource, host_metrics::HostMetricsSource, prom_scrape::PromScrapeSource,
};
use crate::targets::pulseboard::PulseBoardTarget;
use crate::web::Inspector;

const CHANNEL_CAPACITY: usize = 8192;

pub async fn run(
    cfg: Config,
    creds: AgentCredentials,
    inspector: Inspector,
    dry_run: bool,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<Signal>(CHANNEL_CAPACITY);
    let cfg = Arc::new(cfg);

    // ----- Sources ---------------------------------------------------------

    let mut source_handles = vec![];

    if let Some(hm_cfg) = &cfg.sources.host_metrics {
        let tx2 = tx.clone();
        let hm_cfg = hm_cfg.clone();
        let insp = inspector.clone();
        source_handles.push(tokio::spawn(async move {
            HostMetricsSource::new(hm_cfg).run(tx2, insp).await
        }));
        info!("source: host_metrics enabled");
    }

    for fl_cfg in &cfg.sources.file_logs {
        let tx2 = tx.clone();
        let fl_cfg = fl_cfg.clone();
        let insp = inspector.clone();
        info!(
            "source: file_logs {:?} (paths: {:?})",
            fl_cfg.name, fl_cfg.paths
        );
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
        source_handles.push(tokio::spawn(async move {
            PromScrapeSource::new(ps_cfg).run(tx2, insp).await
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

    let cardinality_guard = cfg
        .processors
        .cardinality_guard
        .as_ref()
        .map(|c| CardinalityGuard::new(c.max_series_per_metric));

    let batch_delay_secs = parse_duration_secs(&cfg.processors.batch.max_delay).unwrap_or(5);
    let batch_max = cfg.processors.batch.max_size;
    let mut batcher = BatchProcessor::new(batch_max, batch_delay_secs);

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
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await; // consume the immediate tick
        loop {
            interval.tick().await;
            let stats = serde_json::json!({ "version": env!("CARGO_PKG_VERSION") });
            if let Err(e) = checkin(&creds_clone, env!("CARGO_PKG_VERSION"), "0", &stats).await {
                warn!("checkin error: {:#}", e);
            }
        }
    });

    // ----- Pipeline loop ---------------------------------------------------

    let mut signals_in: u64 = 0;
    let mut signals_out: u64 = 0;
    let mut signals_drop: u64 = 0;

    while let Some(signal) = rx.recv().await {
        signals_in += 1;

        // Relabel
        let signal = if let Some(ref r) = relabeler {
            match r.apply(signal) {
                Some(s) => s,
                None => {
                    signals_drop += 1;
                    continue;
                }
            }
        } else {
            signal
        };

        // Cardinality guard
        if let Signal::Metric(ref m) = signal {
            if let Some(ref cg) = cardinality_guard {
                if cg.check_and_record(&m.name, &m.labels)
                    == crate::processors::cardinality_guard::Verdict::Drop
                {
                    signals_drop += 1;
                    continue;
                }
            }
        }

        // Feed inspector
        inspector.record_signal(&signal);

        // Batch
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
    }

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

    Ok(())
}
