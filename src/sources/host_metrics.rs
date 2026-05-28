use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use sysinfo::{Disks, Networks, System};
use tokio::sync::mpsc;
use tracing::debug;

use crate::config::{HostMetricsConfig, parse_duration_secs};
use crate::signal::{Labels, MetricKind, MetricSample, Signal, now_ms};
use crate::web::Inspector;

pub struct HostMetricsSource {
    cfg: HostMetricsConfig,
}

impl HostMetricsSource {
    pub fn new(cfg: HostMetricsConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        let interval_secs = parse_duration_secs(&self.cfg.interval).unwrap_or(15);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        let mut sys = System::new_all();
        let mut nets = Networks::new_with_refreshed_list();
        let mut disks = Disks::new_with_refreshed_list();

        loop {
            ticker.tick().await;
            sys.refresh_all();
            nets.refresh();
            disks.refresh();

            let ts = now_ms();
            let extra = &self.cfg.extra_labels;

            let collectors: Vec<&str> = if self.cfg.collectors.is_empty() {
                vec!["cpu", "memory", "disk", "network", "load"]
            } else {
                self.cfg.collectors.iter().map(|s| s.as_str()).collect()
            };

            for collector in &collectors {
                let samples = match *collector {
                    "cpu"     => collect_cpu(&sys, ts, extra),
                    "memory"  => collect_memory(&sys, ts, extra),
                    "disk"    => collect_disk(&disks, ts, extra),
                    "network" => collect_network(&nets, ts, extra),
                    "load"    => collect_load(ts, extra),
                    other     => { tracing::warn!("unknown collector {:?}", other); vec![] }
                };

                for s in samples {
                    inspector.record_source("host_metrics", "metric");
                    if tx.send(Signal::Metric(s)).await.is_err() {
                        debug!("host_metrics: pipeline channel closed, exiting");
                        return Ok(());
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CPU
// ---------------------------------------------------------------------------

fn collect_cpu(sys: &System, ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let mut out = Vec::new();

    // Per-CPU usage
    for (i, cpu) in sys.cpus().iter().enumerate() {
        let mut labels = extra.clone();
        labels.insert("cpu".into(), i.to_string());
        out.push(metric(
            "node_cpu_usage_ratio",
            labels.clone(),
            cpu.cpu_usage() as f64 / 100.0,
            ts,
            MetricKind::Gauge,
        ));
    }

    // Aggregate CPU usage  
    let global = sys.global_cpu_info().cpu_usage() as f64 / 100.0;
    let mut labels = extra.clone();
    labels.insert("cpu".into(), "total".into());
    out.push(metric("node_cpu_usage_ratio", labels, global, ts, MetricKind::Gauge));

    out
}

// ---------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------

fn collect_memory(sys: &System, ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let labels = extra.clone();
    vec![
        metric("node_memory_total_bytes",     labels.clone(), sys.total_memory() as f64,     ts, MetricKind::Gauge),
        metric("node_memory_available_bytes", labels.clone(), sys.available_memory() as f64, ts, MetricKind::Gauge),
        metric("node_memory_used_bytes",      labels.clone(), sys.used_memory() as f64,      ts, MetricKind::Gauge),
        metric("node_swap_total_bytes",       labels.clone(), sys.total_swap() as f64,       ts, MetricKind::Gauge),
        metric("node_swap_used_bytes",        labels.clone(), sys.used_swap() as f64,        ts, MetricKind::Gauge),
    ]
}

// ---------------------------------------------------------------------------
// Disk
// ---------------------------------------------------------------------------

fn collect_disk(disks: &Disks, ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let mut out = Vec::new();
    for disk in disks.list() {
        let mount = disk.mount_point().to_string_lossy().to_string();
        let name  = disk.name().to_string_lossy().to_string();
        let mut labels = extra.clone();
        labels.insert("device".into(), name);
        labels.insert("mountpoint".into(), mount);

        let total = disk.total_space() as f64;
        let avail = disk.available_space() as f64;
        out.push(metric("node_filesystem_size_bytes",      labels.clone(), total,         ts, MetricKind::Gauge));
        out.push(metric("node_filesystem_avail_bytes",     labels.clone(), avail,         ts, MetricKind::Gauge));
        out.push(metric("node_filesystem_used_bytes",      labels.clone(), total - avail, ts, MetricKind::Gauge));
    }
    out
}

// ---------------------------------------------------------------------------
// Network
// ---------------------------------------------------------------------------

fn collect_network(nets: &Networks, ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let mut out = Vec::new();
    for (iface, data) in nets.iter() {
        if iface == "lo" || iface == "loopback" { continue; }
        let mut labels = extra.clone();
        labels.insert("device".into(), iface.clone());

        out.push(metric("node_network_receive_bytes_total",    labels.clone(), data.total_received()    as f64, ts, MetricKind::Counter));
        out.push(metric("node_network_transmit_bytes_total",   labels.clone(), data.total_transmitted() as f64, ts, MetricKind::Counter));
        out.push(metric("node_network_receive_packets_total",  labels.clone(), data.total_packets_received()    as f64, ts, MetricKind::Counter));
        out.push(metric("node_network_transmit_packets_total", labels.clone(), data.total_packets_transmitted() as f64, ts, MetricKind::Counter));
    }
    out
}

// ---------------------------------------------------------------------------
// Load average
// ---------------------------------------------------------------------------

fn collect_load(ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let avg   = System::load_average();
    let labels = extra.clone();
    vec![
        metric("node_load1",  labels.clone(), avg.one,     ts, MetricKind::Gauge),
        metric("node_load5",  labels.clone(), avg.five,    ts, MetricKind::Gauge),
        metric("node_load15", labels.clone(), avg.fifteen, ts, MetricKind::Gauge),
    ]
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn metric(name: &str, labels: Labels, value: f64, timestamp_ms: i64, kind: MetricKind) -> MetricSample {
    MetricSample {
        name: name.into(),
        labels,
        value,
        timestamp_ms,
        kind,
    }
}
