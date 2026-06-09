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
// Windows the per-CPU 100ns time counters are read via NtQuerySystemInformation
// (idle/user/system modes). On any other platform a synthetic counter is
// accumulated from sysinfo's percentage gauge across `idle` and `user` only.
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
    #[cfg(target_os = "windows")]
    {
        if let Some(samples) = collect_cpu_windows(sys.cpus().len(), ts, extra) {
            return samples;
        }
    }
    accum.collect_from_sysinfo(sys, ts, extra)
}

// Per-CPU cumulative processor times on Windows, read directly from the kernel
// via NtQuerySystemInformation(SystemProcessorPerformanceInformation). All time
// fields are cumulative in 100-nanosecond units. KernelTime *includes* IdleTime,
// so the "system" mode is KernelTime - IdleTime. Returns `None` (falling back to
// the sysinfo accumulator) if the syscall fails, e.g. on >64-CPU machines that
// span multiple processor groups.
#[cfg(target_os = "windows")]
fn collect_cpu_windows(
    cpu_count: usize,
    ts: i64,
    extra: &HashMap<String, String>,
) -> Option<Vec<MetricSample>> {
    use std::ffi::c_void;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct SystemProcessorPerformanceInformation {
        idle_time: i64,
        kernel_time: i64,
        user_time: i64,
        dpc_time: i64,
        interrupt_time: i64,
        interrupt_count: u32,
    }

    // SYSTEM_INFORMATION_CLASS::SystemProcessorPerformanceInformation
    const CLASS_PROCESSOR_PERFORMANCE: u32 = 8;
    const HUNDRED_NS_PER_SEC: f64 = 10_000_000.0;

    #[link(name = "ntdll")]
    extern "system" {
        fn NtQuerySystemInformation(
            system_information_class: u32,
            system_information: *mut c_void,
            system_information_length: u32,
            return_length: *mut u32,
        ) -> i32; // NTSTATUS (negative = error, 0 = STATUS_SUCCESS)
    }

    if cpu_count == 0 {
        return None;
    }

    let mut buf = vec![
        SystemProcessorPerformanceInformation {
            idle_time: 0,
            kernel_time: 0,
            user_time: 0,
            dpc_time: 0,
            interrupt_time: 0,
            interrupt_count: 0,
        };
        cpu_count
    ];
    let mut return_length: u32 = 0;
    // SAFETY: `buf` holds `cpu_count` entries and `size_of_val` reports its exact
    // byte length, so the kernel writes at most that many bytes.
    let status = unsafe {
        NtQuerySystemInformation(
            CLASS_PROCESSOR_PERFORMANCE,
            buf.as_mut_ptr() as *mut c_void,
            std::mem::size_of_val(buf.as_slice()) as u32,
            &mut return_length,
        )
    };
    if status != 0 {
        return None;
    }

    let entry_size = std::mem::size_of::<SystemProcessorPerformanceInformation>();
    let n = (return_length as usize) / entry_size;
    let mut out = Vec::with_capacity(n * 3);
    for (i, info) in buf.iter().take(n).enumerate() {
        let idle = info.idle_time as f64 / HUNDRED_NS_PER_SEC;
        // KernelTime includes IdleTime; isolate genuine kernel/system time.
        let system = (info.kernel_time - info.idle_time).max(0) as f64 / HUNDRED_NS_PER_SEC;
        let user = info.user_time as f64 / HUNDRED_NS_PER_SEC;
        for (mode, value) in [("idle", idle), ("user", user), ("system", system)] {
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
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
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

// Disk I/O — node_exporter-compatible counters on Windows, read directly from
// the kernel. We open each \\.\PhysicalDriveN and issue IOCTL_DISK_PERFORMANCE,
// which returns a DISK_PERFORMANCE struct of cumulative-since-boot totals
// (BytesRead/BytesWritten/ReadCount/WriteCount). No admin rights are required:
// the device is opened with zero access purely for the control code, and the
// disk performance counters are enabled on-demand by the IOCTL itself.
#[cfg(target_os = "windows")]
fn collect_diskio(ts: i64, extra: &HashMap<String, String>) -> Vec<MetricSample> {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };
    use windows_sys::Win32::System::Ioctl::{DISK_PERFORMANCE, IOCTL_DISK_PERFORMANCE};
    use windows_sys::Win32::System::IO::DeviceIoControl;

    // Probe a generous fixed range of drive numbers; gaps are skipped rather
    // than treated as the end, since drive indices need not be contiguous.
    const MAX_PHYSICAL_DRIVES: u32 = 32;

    let mut out = Vec::new();
    for index in 0..MAX_PHYSICAL_DRIVES {
        let path = format!(r"\\.\PhysicalDrive{index}");
        let wide: Vec<u16> = OsStr::new(&path)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // SAFETY: `wide` is a NUL-terminated UTF-16 path; the remaining
        // arguments are constants / null pointers per the Win32 contract.
        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                0, // query only — no read/write access needed for the IOCTL
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                std::ptr::null(),
                OPEN_EXISTING,
                0,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            continue; // no drive at this index
        }

        let mut perf: DISK_PERFORMANCE = unsafe { std::mem::zeroed() };
        let mut bytes_returned: u32 = 0;
        // SAFETY: `handle` is valid; the output buffer matches DISK_PERFORMANCE.
        let ok = unsafe {
            DeviceIoControl(
                handle,
                IOCTL_DISK_PERFORMANCE,
                std::ptr::null(),
                0,
                &mut perf as *mut DISK_PERFORMANCE as *mut _,
                std::mem::size_of::<DISK_PERFORMANCE>() as u32,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };
        // SAFETY: `handle` came from CreateFileW and has not been closed.
        unsafe { CloseHandle(handle) };

        if ok == 0 {
            continue;
        }

        let mut labels = extra.clone();
        labels.insert("device".into(), format!("PhysicalDrive{index}"));

        out.push(metric(
            "node_disk_read_bytes_total",
            labels.clone(),
            perf.BytesRead as f64,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_disk_written_bytes_total",
            labels.clone(),
            perf.BytesWritten as f64,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_disk_reads_completed_total",
            labels.clone(),
            perf.ReadCount as f64,
            ts,
            MetricKind::Counter,
        ));
        out.push(metric(
            "node_disk_writes_completed_total",
            labels.clone(),
            perf.WriteCount as f64,
            ts,
            MetricKind::Counter,
        ));
    }
    out
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
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

#[cfg(not(target_os = "windows"))]
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

// Windows has no Unix-style load average; sysinfo reports zeros there, so we
// skip the collector entirely rather than emit misleading constant metrics.
#[cfg(target_os = "windows")]
fn collect_load(_ts: i64, _extra: &HashMap<String, String>) -> Vec<MetricSample> {
    Vec::new()
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
