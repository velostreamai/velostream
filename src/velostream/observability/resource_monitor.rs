//! Lightweight process resource monitor.
//!
//! Logs CPU and memory statistics every 5 minutes as single-line JSON
//! for easy parsing by log aggregation tools (Loki, Splunk, etc.).

use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Sample interval in seconds (30 samples over 5 minutes)
const SAMPLE_INTERVAL_SECS: u64 = 10;

/// Number of samples before logging (5 minutes / 10 seconds = 30)
const SAMPLES_PER_LOG: u32 = 30;

/// Resource statistics for JSON serialization.
#[derive(Serialize)]
struct ResourceStatsLog {
    event: &'static str,
    cpu_min: f32,
    cpu_avg: f32,
    cpu_max: f32,
    mem_min_mb: f64,
    mem_avg_mb: f64,
    mem_max_mb: f64,
    samples: u32,
}

/// Accumulated resource statistics over a reporting period.
struct ResourceStats {
    cpu_min: f32,
    cpu_max: f32,
    cpu_sum: f32,
    mem_min_bytes: u64,
    mem_max_bytes: u64,
    mem_sum_bytes: u64,
    samples: u32,
}

impl ResourceStats {
    fn new() -> Self {
        Self {
            cpu_min: f32::MAX,
            cpu_max: 0.0,
            cpu_sum: 0.0,
            mem_min_bytes: u64::MAX,
            mem_max_bytes: 0,
            mem_sum_bytes: 0,
            samples: 0,
        }
    }

    fn record(&mut self, cpu_percent: f32, mem_bytes: u64) {
        self.cpu_min = self.cpu_min.min(cpu_percent);
        self.cpu_max = self.cpu_max.max(cpu_percent);
        self.cpu_sum += cpu_percent;
        self.mem_min_bytes = self.mem_min_bytes.min(mem_bytes);
        self.mem_max_bytes = self.mem_max_bytes.max(mem_bytes);
        self.mem_sum_bytes += mem_bytes;
        self.samples += 1;
    }

    fn log_and_reset(&mut self) {
        if self.samples == 0 {
            return;
        }

        let stats = ResourceStatsLog {
            event: "resource_stats",
            cpu_min: round1(self.cpu_min),
            cpu_avg: round1(self.cpu_sum / self.samples as f32),
            cpu_max: round1(self.cpu_max),
            mem_min_mb: round1_f64(self.mem_min_bytes as f64 / 1_048_576.0),
            mem_avg_mb: round1_f64(self.mem_sum_bytes as f64 / self.samples as f64 / 1_048_576.0),
            mem_max_mb: round1_f64(self.mem_max_bytes as f64 / 1_048_576.0),
            samples: self.samples,
        };

        if let Ok(json) = serde_json::to_string(&stats) {
            log::info!("{}", json);
        }

        *self = Self::new();
    }
}

fn round1(v: f32) -> f32 {
    (v * 10.0).round() / 10.0
}

fn round1_f64(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

/// Spawns a background thread that monitors CPU and memory usage.
///
/// Samples every 10 seconds and logs min/avg/max statistics every 5 minutes
/// as single-line JSON: `{"event":"resource_stats","cpu_min":0.5,...}`
///
/// Returns a shutdown handle that stops the monitor when set to true.
pub fn spawn() -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    std::thread::Builder::new()
        .name("resource-monitor".to_string())
        .spawn(move || {
            let pid = Pid::from_u32(std::process::id());
            let mut sys = System::new();
            let mut stats = ResourceStats::new();
            let refresh_kind = ProcessRefreshKind::new().with_cpu().with_memory();

            // Initial refresh to get baseline (first cpu_usage() call returns 0)
            sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh_kind);
            std::thread::sleep(Duration::from_millis(500));

            while !shutdown_clone.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(SAMPLE_INTERVAL_SECS));

                sys.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[pid]),
                    true,
                    refresh_kind,
                );
                if let Some(process) = sys.process(pid) {
                    stats.record(process.cpu_usage(), process.memory());
                }

                if stats.samples >= SAMPLES_PER_LOG {
                    stats.log_and_reset();
                }
            }

            // Log final stats on shutdown if any samples collected
            if stats.samples > 0 {
                stats.log_and_reset();
            }
        })
        .expect("Failed to spawn resource monitor thread");

    shutdown
}

/// Stops the resource monitor.
pub fn stop(shutdown: &Arc<AtomicBool>) {
    shutdown.store(true, Ordering::Relaxed);
}
