use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use sysinfo::{Disks, Networks, System};
use tokio::sync::mpsc;
use tracing::debug;

use crate::config::{parse_duration_secs, HostMetricsConfig};
use crate::signal::{now_ms, Labels, MetricKind, MetricSample, Signal};
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
        // Per-CPU cumulative seconds-per-mode accumulator. Used on non-Linux
        // platforms where sysinfo only exposes instantaneous percentage; on
        // Linux we read /proc/stat directly and ignore this.
        let mut cpu_accum = CpuAccumulator::new();

        loop {
            ticker.tick().await;
            sys.refresh_all();
            nets.refresh();
            disks.refresh();

            let ts = now_ms();
            let extra = &self.cfg.extra_labels;

            let collectors: Vec<&str> = if self.cfg.collectors.is_empty() {
                vec!["cpu", "memory", "disk", "diskio", "network", "load"]
            } else {
                self.cfg.collectors.iter().map(|s| s.as_str()).collect()
            };

            for collector in &collectors {
                let samples = match *collector {
                    "cpu" => collect_cpu(&sys, &mut cpu_accum, ts, extra),
                    "memory" => collect_memory(&sys, ts, extra),
                    "disk" => collect_disk(&disks, ts, extra),
                    "diskio" => collect_diskio(ts, extra),
                    "network" => collect_network(&nets, ts, extra),
                    "load" => collect_load(ts, extra),
                    other => {
                        tracing::warn!("unknown collector {:?}", other);
                        vec![]
                    }
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
// CPU — emits node_exporter-compatible `node_cpu_seconds_total{cpu,mode}`
// (counter). On Linux the cumulative jiffies are read from /proc/stat. On
// other platforms a synthetic counter is accumulated from sysinfo's
// percentage gauge across `idle` and `user` modes only.
// ---------------------------------------------------------------------------

fn collect_cpu(
    sys: &System,
    accum: &mut CpuAccumulator,
    ts: i64,
    extra: &HashMap<String, String>,
) -> Vec<MetricSample> {
    #[cfg(target_os = "linux")]
    {
        if let Some(samples) = collect_cpu_proc_stat(ts, extra) {
            return samples;
        }
    }
    accum.collect_from_sysinfo(sys, ts, extra)
}

#[cfg(target_os = "linux")]
fn collect_cpu_proc_stat(ts: i64, extra: &HashMap<String, String>) -> Option<Vec<MetricSample>> {
    use std::fs;
    // /proc/stat columns after the leading label:
    //   user nice system idle iowait irq softirq steal guest guest_nice
    // Units are USER_HZ (typically 100 jiffies/sec). We divide by 100.0 to
    // match node_exporter's reporting in seconds.
    const MODES: &[&str] = &[
        "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal",
    ];
    const USER_HZ: f64 = 100.0;
    let text = fs::read_to_string("/proc/stat").ok()?;
    let mut out = Vec::new();
    for line in text.lines() {
        if !line.starts_with("cpu") {
            continue;
        }
        let mut parts = line.split_whitespace();
        let label = parts.next()?;
        // Skip the aggregate "cpu" row; node_exporter only emits per-CPU.
        if label == "cpu" {
            continue;
        }
        let cpu_index = label.trim_start_matches("cpu");
        let values: Vec<f64> = parts
            .take(MODES.len())
            .map(|s| s.parse::<f64>().unwrap_or(0.0))
            .collect();
        for (mode, jiffies) in MODES.iter().zip(values.iter()) {
            let mut labels = extra.clone();
            labels.insert("cpu".into(), cpu_index.into());
            labels.insert("mode".into(), (*mode).into());
            out.push(metric(
                "node_cpu_seconds_total",
                labels,
                jiffies / USER_HZ,
                ts,
                MetricKind::Counter,
            ));
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

struct CpuAccumulator {
    // Per-CPU cumulative seconds in (idle, user).
    state: Vec<(f64, f64)>,
    last_ts_ms: Option<i64>,
}

impl CpuAccumulator {
    fn new() -> Self {
        Self {
            state: Vec::new(),
            last_ts_ms: None,
        }
    }

    fn collect_from_sysinfo(
        &mut self,
        sys: &System,
        ts: i64,
        extra: &HashMap<String, String>,
    ) -> Vec<MetricSample> {
        let cpus = sys.cpus();
        if self.state.len() != cpus.len() {
            self.state = vec![(0.0, 0.0); cpus.len()];
        }
        let dt = match self.last_ts_ms {
            Some(prev) if ts > prev => (ts - prev) as f64 / 1000.0,
            _ => 0.0,
        };
        self.last_ts_ms = Some(ts);

        let mut out = Vec::with_capacity(cpus.len() * 2);
        for (i, cpu) in cpus.iter().enumerate() {
            if dt > 0.0 {
                let busy = (cpu.cpu_usage() as f64 / 100.0).clamp(0.0, 1.0);
                let (idle, user) = &mut self.state[i];
                *user += busy * dt;
                *idle += (1.0 - busy) * dt;
            }
            let (idle, user) = self.state[i];
            for (mode, value) in [("idle", idle), ("user", user)] {
                let mut labels = extra.clone();
                labels.insert("cpu".into(), i.to_string());
                labels.insert("mode".into(), mode.into());
                out.push(metric(
                    "node_cpu_seconds_total",
                    labels,
                    value,
                    ts,
                    MetricKind::Counter,
                ));
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Memory — node_exporter naming (`node_memory_<Field>_bytes`).
// ---------------------------------------------------------------------------

fn collect_memory(sys: &System, ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let labels = extra.clone();
    vec![
        metric(
            "node_memory_MemTotal_bytes",
            labels.clone(),
            sys.total_memory() as f64,
            ts,
            MetricKind::Gauge,
        ),
        metric(
            "node_memory_MemAvailable_bytes",
            labels.clone(),
            sys.available_memory() as f64,
            ts,
            MetricKind::Gauge,
        ),
        metric(
            "node_memory_MemFree_bytes",
            labels.clone(),
            sys.free_memory() as f64,
            ts,
            MetricKind::Gauge,
        ),
        metric(
            "node_memory_SwapTotal_bytes",
            labels.clone(),
            sys.total_swap() as f64,
            ts,
            MetricKind::Gauge,
        ),
        metric(
            "node_memory_SwapFree_bytes",
            labels.clone(),
            (sys.total_swap().saturating_sub(sys.used_swap())) as f64,
            ts,
            MetricKind::Gauge,
        ),
    ]
}

// ---------------------------------------------------------------------------
// Filesystem capacity — node_exporter naming (already matched).
// ---------------------------------------------------------------------------

fn collect_disk(disks: &Disks, ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let mut out = Vec::new();
    for disk in disks.list() {
        let mount = disk.mount_point().to_string_lossy().to_string();
        let name = disk.name().to_string_lossy().to_string();
        let mut labels = extra.clone();
        labels.insert("device".into(), name);
        labels.insert("mountpoint".into(), mount);

        let total = disk.total_space() as f64;
        let avail = disk.available_space() as f64;
        out.push(metric(
            "node_filesystem_size_bytes",
            labels.clone(),
            total,
            ts,
            MetricKind::Gauge,
        ));
        out.push(metric(
            "node_filesystem_avail_bytes",
            labels.clone(),
            avail,
            ts,
            MetricKind::Gauge,
        ));
        out.push(metric(
            "node_filesystem_used_bytes",
            labels.clone(),
            total - avail,
            ts,
            MetricKind::Gauge,
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Disk I/O — node_exporter-compatible counters from /proc/diskstats.
// Linux-only; no-op on other platforms.
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn collect_diskio(ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    use std::fs;
    // Sector size is fixed at 512 bytes by the kernel for the purposes of
    // /proc/diskstats accounting, regardless of the device's logical block
    // size — same convention node_exporter uses.
    const SECTOR_BYTES: f64 = 512.0;
    let text = match fs::read_to_string("/proc/diskstats") {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for line in text.lines() {
        // Fields:
        //   1 major  2 minor  3 device
        //   4 reads_completed   5 reads_merged   6 sectors_read   7 read_time_ms
        //   8 writes_completed  9 writes_merged 10 sectors_written 11 write_time_ms
        //  12 ios_in_progress  13 io_time_ms    14 weighted_io_time_ms
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 14 {
            continue;
        }
        let device = parts[2];
        // Skip partitions and pseudo devices to keep cardinality bounded.
        // Heuristic mirrors node_exporter's default `ignored-devices`.
        if device.starts_with("loop")
            || device.starts_with("ram")
            || device.starts_with("dm-")
            || device.starts_with("sr")
            || (device.starts_with("sd")
                && device.chars().last().is_some_and(|c| c.is_ascii_digit()))
            || (device.starts_with("nvme") && device.contains('p'))
            || (device.starts_with("mmcblk") && device.contains('p'))
        {
            continue;
        }
        let parse = |i: usize| parts[i].parse::<f64>().unwrap_or(0.0);
        let reads_completed = parse(3);
        let sectors_read = parse(5);
        let writes_completed = parse(7);
        let sectors_written = parse(9);
        let mut labels = extra.clone();
        labels.insert("device".into(), device.into());

        out.push(metric(
            "node_disk_read_bytes_total",
            labels.clone(),
            sectors_read * SECTOR_BYTES,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_disk_written_bytes_total",
            labels.clone(),
            sectors_written * SECTOR_BYTES,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_disk_reads_completed_total",
            labels.clone(),
            reads_completed,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_disk_writes_completed_total",
            labels.clone(),
            writes_completed,
            ts,
            MetricKind::Counter,
        ));
    }
    out
}

#[cfg(not(target_os = "linux"))]
fn collect_diskio(_ts: i64, _extra: &HashMap<String, String>) -> Vec<MetricSample> {
    Vec::new()
}

// ---------------------------------------------------------------------------
// Network
// ---------------------------------------------------------------------------

fn collect_network(nets: &Networks, ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let mut out = Vec::new();
    for (iface, data) in nets.iter() {
        if iface == "lo" || iface == "loopback" {
            continue;
        }
        let mut labels = extra.clone();
        labels.insert("device".into(), iface.clone());

        out.push(metric(
            "node_network_receive_bytes_total",
            labels.clone(),
            data.total_received() as f64,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_network_transmit_bytes_total",
            labels.clone(),
            data.total_transmitted() as f64,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_network_receive_packets_total",
            labels.clone(),
            data.total_packets_received() as f64,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_network_transmit_packets_total",
            labels.clone(),
            data.total_packets_transmitted() as f64,
            ts,
            MetricKind::Counter,
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Load average
// ---------------------------------------------------------------------------

fn collect_load(ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    let avg = System::load_average();
    let labels = extra.clone();
    vec![
        metric("node_load1", labels.clone(), avg.one, ts, MetricKind::Gauge),
        metric(
            "node_load5",
            labels.clone(),
            avg.five,
            ts,
            MetricKind::Gauge,
        ),
        metric(
            "node_load15",
            labels.clone(),
            avg.fifteen,
            ts,
            MetricKind::Gauge,
        ),
    ]
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn metric(
    name: &str,
    labels: Labels,
    value: f64,
    timestamp_ms: i64,
    kind: MetricKind,
) -> MetricSample {
    MetricSample {
        name: name.into(),
        labels,
        value,
        timestamp_ms,
        kind,
    }
}
