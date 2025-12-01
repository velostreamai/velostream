//! Stress testing module for SQL application test harness
//!
//! Provides high-volume data generation and performance measurement
//! for stress testing SQL streaming applications.
//!
//! ## Memory Tracking
//!
//! Memory tracking is implemented using platform-specific APIs:
//! - **macOS**: Uses `task_info` via `mach` API to get resident set size (RSS)
//! - **Linux**: Reads from `/proc/self/statm` to get memory usage
//! - **Other**: Falls back to estimation based on generated data
//!
//! Memory is sampled periodically during stress test execution and
//! the peak value is reported in the final metrics.

use super::error::{TestHarnessError, TestHarnessResult};
use super::generator::SchemaDataGenerator;
use super::schema::Schema;
use super::spec::TestSpec;
use crate::velostream::sql::execution::FieldValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Stress test configuration
#[derive(Debug, Clone)]
pub struct StressConfig {
    /// Number of records to generate per source
    pub records_per_source: usize,
    /// Duration to run the stress test (if time-based)
    pub duration: Option<Duration>,
    /// Batch size for record generation
    pub batch_size: usize,
    /// Whether to measure latency per record
    pub measure_latency: bool,
    /// Whether to track memory usage
    pub track_memory: bool,
    /// Report interval in seconds
    pub report_interval_secs: u64,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            records_per_source: 100_000,
            duration: None,
            batch_size: 1000,
            measure_latency: false,
            track_memory: true, // Memory tracking enabled by default
            report_interval_secs: 5,
        }
    }
}

impl StressConfig {
    /// Create new stress config with record count
    pub fn with_records(records: usize) -> Self {
        Self {
            records_per_source: records,
            ..Default::default()
        }
    }

    /// Create new stress config with duration
    pub fn with_duration(duration: Duration) -> Self {
        Self {
            duration: Some(duration),
            ..Default::default()
        }
    }

    /// Set batch size
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Enable latency measurement
    pub fn measure_latency(mut self, enabled: bool) -> Self {
        self.measure_latency = enabled;
        self
    }

    /// Enable memory tracking
    pub fn track_memory(mut self, enabled: bool) -> Self {
        self.track_memory = enabled;
        self
    }
}

/// Stress test metrics collected during execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StressMetrics {
    /// Total records generated
    pub total_records: u64,
    /// Total bytes generated (estimated)
    pub total_bytes: u64,
    /// Total duration in milliseconds
    pub duration_ms: u64,
    /// Records per second
    pub records_per_second: f64,
    /// Bytes per second
    pub bytes_per_second: f64,
    /// Latency percentiles (if measured)
    pub latency_p50_us: Option<u64>,
    pub latency_p95_us: Option<u64>,
    pub latency_p99_us: Option<u64>,
    /// Peak memory usage in bytes (if available)
    pub peak_memory_bytes: Option<u64>,
    /// Starting memory usage in bytes (if available)
    pub start_memory_bytes: Option<u64>,
    /// Final memory usage in bytes (if available)
    pub final_memory_bytes: Option<u64>,
    /// Memory growth during test (final - start)
    pub memory_growth_bytes: Option<i64>,
    /// Per-source metrics
    pub source_metrics: HashMap<String, SourceMetrics>,
}

/// Metrics for a single source
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceMetrics {
    /// Records generated for this source
    pub records: u64,
    /// Bytes generated for this source
    pub bytes: u64,
    /// Generation time in milliseconds
    pub generation_time_ms: u64,
}

/// Memory tracker for stress tests
///
/// Tracks peak memory usage during stress test execution using
/// platform-specific APIs for accurate measurement.
#[derive(Debug)]
pub struct MemoryTracker {
    /// Peak memory observed (atomic for thread-safe updates)
    peak_memory: Arc<AtomicU64>,
    /// Starting memory at tracker creation
    start_memory: u64,
    /// Whether tracking is enabled
    enabled: bool,
}

impl MemoryTracker {
    /// Create a new memory tracker
    pub fn new(enabled: bool) -> Self {
        let start_memory = if enabled {
            get_current_memory_usage().unwrap_or(0)
        } else {
            0
        };

        Self {
            peak_memory: Arc::new(AtomicU64::new(start_memory)),
            start_memory,
            enabled,
        }
    }

    /// Sample current memory usage and update peak if higher
    pub fn sample(&self) {
        if !self.enabled {
            return;
        }

        if let Some(current) = get_current_memory_usage() {
            // Update peak if current is higher (atomic max)
            let mut old_peak = self.peak_memory.load(Ordering::Relaxed);
            while current > old_peak {
                match self.peak_memory.compare_exchange_weak(
                    old_peak,
                    current,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(x) => old_peak = x,
                }
            }
        }
    }

    /// Get peak memory usage observed
    pub fn peak_memory(&self) -> Option<u64> {
        if self.enabled {
            Some(self.peak_memory.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    /// Get starting memory
    pub fn start_memory(&self) -> Option<u64> {
        if self.enabled {
            Some(self.start_memory)
        } else {
            None
        }
    }

    /// Get current memory usage
    pub fn current_memory(&self) -> Option<u64> {
        if self.enabled {
            get_current_memory_usage()
        } else {
            None
        }
    }
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new(true)
    }
}

/// Get current process memory usage in bytes (platform-specific)
///
/// Returns the resident set size (RSS) which represents the actual
/// physical memory being used by the process.
#[cfg(target_os = "macos")]
fn get_current_memory_usage() -> Option<u64> {
    use std::mem::MaybeUninit;
    use std::os::raw::c_void;

    // Use mach API to get task info
    // SAFETY: These are standard macOS mach kernel APIs
    unsafe extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(
            target_task: u32,
            flavor: i32,
            task_info_out: *mut c_void,
            task_info_outCnt: *mut u32,
        ) -> i32;
    }

    const TASK_BASIC_INFO: i32 = 5;
    const KERN_SUCCESS: i32 = 0;

    #[repr(C)]
    struct TaskBasicInfo {
        suspend_count: i32,
        virtual_size: u64,
        resident_size: u64,
        user_time: [u32; 2],
        system_time: [u32; 2],
        policy: i32,
    }

    // SAFETY: We're calling stable macOS system APIs with properly sized buffers
    unsafe {
        let mut info = MaybeUninit::<TaskBasicInfo>::uninit();
        let mut count = (std::mem::size_of::<TaskBasicInfo>() / std::mem::size_of::<u32>()) as u32;

        let result = task_info(
            mach_task_self(),
            TASK_BASIC_INFO,
            info.as_mut_ptr() as *mut c_void,
            &mut count,
        );

        if result == KERN_SUCCESS {
            let info = info.assume_init();
            Some(info.resident_size)
        } else {
            None
        }
    }
}

/// Get current process memory usage in bytes (Linux)
#[cfg(target_os = "linux")]
fn get_current_memory_usage() -> Option<u64> {
    // Read from /proc/self/statm
    if let Ok(content) = std::fs::read_to_string("/proc/self/statm") {
        // Format: size resident shared text lib data dt
        // We want the second field (resident) * page_size
        let parts: Vec<&str> = content.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(pages) = parts[1].parse::<u64>() {
                // Get page size - default to 4KB if we can't determine it
                let page_size = get_page_size();
                return Some(pages * page_size);
            }
        }
    }
    None
}

/// Get the system page size on Linux
#[cfg(target_os = "linux")]
fn get_page_size() -> u64 {
    // Try to read page size from /proc/self/stat or use default 4KB
    // Most Linux systems use 4KB pages
    4096
}

/// Fallback for other platforms - returns None
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_current_memory_usage() -> Option<u64> {
    // Memory tracking not available on this platform
    None
}

/// Stress test runner
pub struct StressRunner {
    config: StressConfig,
    schemas: HashMap<String, Schema>,
    generator: SchemaDataGenerator,
}

impl StressRunner {
    /// Create new stress runner
    pub fn new(config: StressConfig) -> Self {
        Self {
            config,
            schemas: HashMap::new(),
            generator: SchemaDataGenerator::new(None),
        }
    }

    /// Load schema for a source
    pub fn load_schema(&mut self, source: &str, schema: Schema) -> TestHarnessResult<()> {
        self.schemas.insert(source.to_string(), schema);
        Ok(())
    }

    /// Load schema from file
    pub fn load_schema_file(
        &mut self,
        source: &str,
        path: impl AsRef<Path>,
    ) -> TestHarnessResult<()> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        let schema: Schema =
            serde_yaml::from_str(&content).map_err(|e| TestHarnessError::SchemaParseError {
                message: e.to_string(),
                file: path.display().to_string(),
            })?;

        self.load_schema(source, schema)
    }

    /// Run stress test and collect metrics
    pub fn run(&mut self) -> TestHarnessResult<StressMetrics> {
        let start = Instant::now();
        let mut metrics = StressMetrics::default();
        let mut latencies: Vec<u64> = Vec::new();

        // Initialize memory tracking
        let memory_tracker = MemoryTracker::new(self.config.track_memory);

        // Record starting memory
        metrics.start_memory_bytes = memory_tracker.start_memory();

        // Generate data for each source
        let sources: Vec<_> = self.schemas.keys().cloned().collect();
        for source in sources {
            let schema = self.schemas.get(&source).unwrap().clone();
            let source_start = Instant::now();
            let mut source_metrics = SourceMetrics::default();

            let records_to_generate = self.config.records_per_source;
            let mut generated = 0;

            while generated < records_to_generate {
                let batch_size =
                    std::cmp::min(self.config.batch_size, records_to_generate - generated);

                let batch_start = Instant::now();
                let records = self.generator.generate(&schema, batch_size)?;
                let batch_elapsed = batch_start.elapsed();

                // Sample memory after each batch
                memory_tracker.sample();

                if self.config.measure_latency && !records.is_empty() {
                    let per_record_us = batch_elapsed.as_micros() as u64 / records.len() as u64;
                    latencies.push(per_record_us);
                }

                // Estimate bytes (rough approximation)
                let bytes_estimate: u64 = records.iter().map(estimate_record_size).sum();

                source_metrics.records += records.len() as u64;
                source_metrics.bytes += bytes_estimate;
                generated += records.len();

                // Progress reporting (every report_interval)
                if source_start
                    .elapsed()
                    .as_secs()
                    .is_multiple_of(self.config.report_interval_secs)
                    && generated > 0
                    && generated % (self.config.batch_size * 10) == 0
                {
                    let rate = generated as f64 / source_start.elapsed().as_secs_f64();
                    log::info!(
                        "Stress test progress: {} - {}/{} records ({:.0} rec/s)",
                        source,
                        generated,
                        records_to_generate,
                        rate
                    );
                }

                // Check duration limit
                if let Some(max_duration) = self.config.duration
                    && start.elapsed() >= max_duration
                {
                    break;
                }
            }

            source_metrics.generation_time_ms = source_start.elapsed().as_millis() as u64;
            metrics.total_records += source_metrics.records;
            metrics.total_bytes += source_metrics.bytes;
            metrics.source_metrics.insert(source, source_metrics);
        }

        // Calculate final metrics
        metrics.duration_ms = start.elapsed().as_millis() as u64;
        if metrics.duration_ms > 0 {
            metrics.records_per_second =
                (metrics.total_records as f64 * 1000.0) / metrics.duration_ms as f64;
            metrics.bytes_per_second =
                (metrics.total_bytes as f64 * 1000.0) / metrics.duration_ms as f64;
        }

        // Calculate latency percentiles
        if !latencies.is_empty() {
            latencies.sort_unstable();
            let len = latencies.len();
            metrics.latency_p50_us = Some(latencies[len / 2]);
            metrics.latency_p95_us = Some(latencies[(len as f64 * 0.95) as usize]);
            metrics.latency_p99_us = Some(latencies[(len as f64 * 0.99) as usize]);
        }

        // Final memory sampling and metrics
        memory_tracker.sample();
        metrics.peak_memory_bytes = memory_tracker.peak_memory();
        metrics.final_memory_bytes = memory_tracker.current_memory();

        // Calculate memory growth
        if let (Some(start_mem), Some(final_mem)) =
            (metrics.start_memory_bytes, metrics.final_memory_bytes)
        {
            metrics.memory_growth_bytes = Some(final_mem as i64 - start_mem as i64);
        }

        Ok(metrics)
    }

    /// Generate a simple stress test report
    pub fn generate_report(&self, metrics: &StressMetrics) -> String {
        let mut report = String::new();

        report.push_str("╔══════════════════════════════════════════════════════════════╗\n");
        report.push_str("║               STRESS TEST RESULTS                            ║\n");
        report.push_str("╠══════════════════════════════════════════════════════════════╣\n");

        report.push_str(&format!(
            "║ Total Records:     {:>12}                              ║\n",
            format_number(metrics.total_records)
        ));
        report.push_str(&format!(
            "║ Total Bytes:       {:>12}                              ║\n",
            format_bytes(metrics.total_bytes)
        ));
        report.push_str(&format!(
            "║ Duration:          {:>12}                              ║\n",
            format_duration(metrics.duration_ms)
        ));
        report.push_str("╠══════════════════════════════════════════════════════════════╣\n");
        report.push_str(&format!(
            "║ Throughput:        {:>12} rec/s                        ║\n",
            format_number(metrics.records_per_second as u64)
        ));
        report.push_str(&format!(
            "║ Bandwidth:         {:>12}/s                            ║\n",
            format_bytes(metrics.bytes_per_second as u64)
        ));

        if metrics.latency_p50_us.is_some() {
            report.push_str("╠══════════════════════════════════════════════════════════════╣\n");
            report.push_str("║ Latency Percentiles (per batch):                             ║\n");
            if let Some(p50) = metrics.latency_p50_us {
                report.push_str(&format!(
                    "║   P50:             {:>12} μs                           ║\n",
                    p50
                ));
            }
            if let Some(p95) = metrics.latency_p95_us {
                report.push_str(&format!(
                    "║   P95:             {:>12} μs                           ║\n",
                    p95
                ));
            }
            if let Some(p99) = metrics.latency_p99_us {
                report.push_str(&format!(
                    "║   P99:             {:>12} μs                           ║\n",
                    p99
                ));
            }
        }

        // Memory metrics section
        if metrics.peak_memory_bytes.is_some()
            || metrics.start_memory_bytes.is_some()
            || metrics.memory_growth_bytes.is_some()
        {
            report.push_str("╠══════════════════════════════════════════════════════════════╣\n");
            report.push_str("║ Memory Usage:                                                ║\n");
            if let Some(start) = metrics.start_memory_bytes {
                report.push_str(&format!(
                    "║   Start:           {:>12}                              ║\n",
                    format_bytes(start)
                ));
            }
            if let Some(peak) = metrics.peak_memory_bytes {
                report.push_str(&format!(
                    "║   Peak:            {:>12}                              ║\n",
                    format_bytes(peak)
                ));
            }
            if let Some(final_mem) = metrics.final_memory_bytes {
                report.push_str(&format!(
                    "║   Final:           {:>12}                              ║\n",
                    format_bytes(final_mem)
                ));
            }
            if let Some(growth) = metrics.memory_growth_bytes {
                let growth_str = if growth >= 0 {
                    format!("+{}", format_bytes(growth as u64))
                } else {
                    format!("-{}", format_bytes((-growth) as u64))
                };
                report.push_str(&format!(
                    "║   Growth:          {:>12}                              ║\n",
                    growth_str
                ));
            }
        }

        if !metrics.source_metrics.is_empty() {
            report.push_str("╠══════════════════════════════════════════════════════════════╣\n");
            report.push_str("║ Per-Source Metrics:                                          ║\n");
            for (source, sm) in &metrics.source_metrics {
                let rate = if sm.generation_time_ms > 0 {
                    (sm.records as f64 * 1000.0) / sm.generation_time_ms as f64
                } else {
                    0.0
                };
                report.push_str(&format!(
                    "║   {:<20} {:>10} rec ({:>8} rec/s)         ║\n",
                    truncate_string(source, 20),
                    format_number(sm.records),
                    format_number(rate as u64)
                ));
            }
        }

        report.push_str("╚══════════════════════════════════════════════════════════════╝\n");

        report
    }
}

/// Estimate the size of a record in bytes
fn estimate_record_size(record: &HashMap<String, FieldValue>) -> u64 {
    let mut size: u64 = 0;
    for (key, value) in record {
        size += key.len() as u64;
        size += match value {
            FieldValue::String(s) => s.len() as u64,
            FieldValue::Integer(_) => 8,
            FieldValue::Float(_) => 8,
            FieldValue::Boolean(_) => 1,
            FieldValue::Timestamp(_) => 8,
            FieldValue::ScaledInteger(_, _) => 16,
            FieldValue::Null => 0,
            FieldValue::Array(arr) => arr.len() as u64 * 8,
            FieldValue::Map(map) => map.len() as u64 * 16,
            FieldValue::Date(_) => 4,
            FieldValue::Decimal(_) => 16,
            FieldValue::Struct(fields) => fields.len() as u64 * 16,
            FieldValue::Interval { .. } => 16,
        };
    }
    size
}

/// Format a number with thousands separators
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}

/// Format bytes in human-readable format
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format duration in human-readable format
fn format_duration(ms: u64) -> String {
    if ms >= 60_000 {
        format!("{:.1} min", ms as f64 / 60_000.0)
    } else if ms >= 1000 {
        format!("{:.2} s", ms as f64 / 1000.0)
    } else {
        format!("{} ms", ms)
    }
}

/// Truncate string to max length
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

/// Stress test from test spec
pub fn run_stress_from_spec(
    spec: &TestSpec,
    schema_dir: impl AsRef<Path>,
    config: StressConfig,
) -> TestHarnessResult<StressMetrics> {
    let schema_dir = schema_dir.as_ref();
    let mut runner = StressRunner::new(config);

    // Load schemas for all inputs in the spec
    for query in &spec.queries {
        for input in &query.inputs {
            if let Some(schema_file) = &input.schema {
                let schema_path = schema_dir.join(schema_file);
                if schema_path.exists() {
                    runner.load_schema_file(&input.source, &schema_path)?;
                } else {
                    log::warn!(
                        "Schema file not found: {}, using default schema for {}",
                        schema_path.display(),
                        input.source
                    );
                    // Create a minimal default schema
                    let default_schema = Schema {
                        name: input.source.clone(),
                        description: None,
                        fields: vec![],
                        record_count: 1000,
                    };
                    runner.load_schema(&input.source, default_schema)?;
                }
            }
        }
    }

    runner.run()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::test_harness::schema::{
        FieldConstraints, FieldDefinition, FieldType, RangeConstraint,
    };

    #[test]
    fn test_stress_config_default() {
        let config = StressConfig::default();
        assert_eq!(config.records_per_source, 100_000);
        assert_eq!(config.batch_size, 1000);
        assert!(!config.measure_latency);
        assert!(config.track_memory); // Memory tracking enabled by default
    }

    #[test]
    fn test_stress_config_builder() {
        let config = StressConfig::with_records(50_000)
            .batch_size(500)
            .measure_latency(true)
            .track_memory(false);

        assert_eq!(config.records_per_source, 50_000);
        assert_eq!(config.batch_size, 500);
        assert!(config.measure_latency);
        assert!(!config.track_memory);
    }

    #[test]
    fn test_stress_runner_basic() {
        let schema = Schema {
            name: "test_source".to_string(),
            description: None,
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    field_type: FieldType::integer(),
                    nullable: false,
                    constraints: FieldConstraints {
                        range: Some(RangeConstraint {
                            min: 1.0,
                            max: 1000.0,
                        }),
                        ..Default::default()
                    },
                    description: None,
                },
                FieldDefinition {
                    name: "value".to_string(),
                    field_type: FieldType::string(),
                    nullable: false,
                    constraints: FieldConstraints::default(),
                    description: None,
                },
            ],
            record_count: 1000,
        };

        let config = StressConfig::with_records(1000).batch_size(100);
        let mut runner = StressRunner::new(config);
        runner.load_schema("test_source", schema).unwrap();

        let metrics = runner.run().unwrap();

        assert_eq!(metrics.total_records, 1000);
        assert!(metrics.duration_ms > 0);
        assert!(metrics.records_per_second > 0.0);
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(1), "1");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1000000), "1,000,000");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_generate_report() {
        let metrics = StressMetrics {
            total_records: 100_000,
            total_bytes: 10_000_000,
            duration_ms: 5000,
            records_per_second: 20_000.0,
            bytes_per_second: 2_000_000.0,
            latency_p50_us: Some(50),
            latency_p95_us: Some(150),
            latency_p99_us: Some(300),
            peak_memory_bytes: None,
            start_memory_bytes: None,
            final_memory_bytes: None,
            memory_growth_bytes: None,
            source_metrics: HashMap::new(),
        };

        let runner = StressRunner::new(StressConfig::default());
        let report = runner.generate_report(&metrics);

        assert!(report.contains("100,000"));
        assert!(report.contains("20,000"));
        assert!(report.contains("P50"));
        assert!(report.contains("P95"));
        assert!(report.contains("P99"));
    }

    #[test]
    fn test_generate_report_with_memory() {
        let metrics = StressMetrics {
            total_records: 100_000,
            total_bytes: 10_000_000,
            duration_ms: 5000,
            records_per_second: 20_000.0,
            bytes_per_second: 2_000_000.0,
            latency_p50_us: None,
            latency_p95_us: None,
            latency_p99_us: None,
            peak_memory_bytes: Some(104_857_600),  // 100 MB
            start_memory_bytes: Some(52_428_800),  // 50 MB
            final_memory_bytes: Some(78_643_200),  // 75 MB
            memory_growth_bytes: Some(26_214_400), // +25 MB
            source_metrics: HashMap::new(),
        };

        let runner = StressRunner::new(StressConfig::default());
        let report = runner.generate_report(&metrics);

        assert!(report.contains("Memory Usage"));
        assert!(report.contains("Start:"));
        assert!(report.contains("Peak:"));
        assert!(report.contains("Final:"));
        assert!(report.contains("Growth:"));
    }

    #[test]
    fn test_memory_tracker_basic() {
        let tracker = MemoryTracker::new(true);

        // On supported platforms, should get memory values
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            assert!(tracker.start_memory().is_some());
            assert!(tracker.peak_memory().is_some());
            assert!(tracker.current_memory().is_some());
        }

        // Sample should not panic
        tracker.sample();
    }

    #[test]
    fn test_memory_tracker_disabled() {
        let tracker = MemoryTracker::new(false);

        assert!(tracker.start_memory().is_none());
        assert!(tracker.peak_memory().is_none());
        assert!(tracker.current_memory().is_none());

        // Sample should be no-op when disabled
        tracker.sample();
    }

    #[test]
    fn test_stress_runner_with_memory_tracking() {
        let schema = Schema {
            name: "test_source".to_string(),
            description: None,
            fields: vec![FieldDefinition {
                name: "id".to_string(),
                field_type: FieldType::integer(),
                nullable: false,
                constraints: FieldConstraints {
                    range: Some(RangeConstraint {
                        min: 1.0,
                        max: 1000.0,
                    }),
                    ..Default::default()
                },
                description: None,
            }],
            record_count: 1000,
        };

        let config = StressConfig::with_records(100)
            .batch_size(10)
            .track_memory(true);
        let mut runner = StressRunner::new(config);
        runner.load_schema("test_source", schema).unwrap();

        let metrics = runner.run().unwrap();

        assert_eq!(metrics.total_records, 100);

        // On supported platforms, should have memory metrics
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            assert!(metrics.start_memory_bytes.is_some());
            assert!(metrics.peak_memory_bytes.is_some());
            assert!(metrics.final_memory_bytes.is_some());
            assert!(metrics.memory_growth_bytes.is_some());
        }
    }

    #[test]
    fn test_stress_runner_without_memory_tracking() {
        let schema = Schema {
            name: "test_source".to_string(),
            description: None,
            fields: vec![FieldDefinition {
                name: "id".to_string(),
                field_type: FieldType::integer(),
                nullable: false,
                constraints: FieldConstraints::default(),
                description: None,
            }],
            record_count: 1000,
        };

        let config = StressConfig::with_records(100)
            .batch_size(10)
            .track_memory(false);
        let mut runner = StressRunner::new(config);
        runner.load_schema("test_source", schema).unwrap();

        let metrics = runner.run().unwrap();

        assert_eq!(metrics.total_records, 100);

        // Memory tracking disabled - should have None values
        assert!(metrics.start_memory_bytes.is_none());
        assert!(metrics.peak_memory_bytes.is_none());
        assert!(metrics.final_memory_bytes.is_none());
        assert!(metrics.memory_growth_bytes.is_none());
    }

    #[test]
    fn test_get_current_memory_usage() {
        // On supported platforms, should get a value
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            let memory = get_current_memory_usage();
            // Memory tracking should return Some value on supported platforms
            // The value may be 0 in some edge cases (like sandboxed environments)
            // so we just check it returns Some
            assert!(
                memory.is_some(),
                "Memory tracking should be available on macOS/Linux"
            );
        }

        // On unsupported platforms, should return None
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let memory = get_current_memory_usage();
            assert!(memory.is_none());
        }
    }
}
