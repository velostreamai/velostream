//! Stress testing module for SQL application test harness
//!
//! Provides high-volume data generation and performance measurement
//! for stress testing SQL streaming applications.

use super::error::{TestHarnessError, TestHarnessResult};
use super::generator::SchemaDataGenerator;
use super::schema::Schema;
use super::spec::TestSpec;
use crate::velostream::sql::execution::FieldValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
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

                if self.config.measure_latency && !records.is_empty() {
                    let per_record_us = batch_elapsed.as_micros() as u64 / records.len() as u64;
                    latencies.push(per_record_us);
                }

                // Estimate bytes (rough approximation)
                let bytes_estimate: u64 = records.iter().map(|r| estimate_record_size(r)).sum();

                source_metrics.records += records.len() as u64;
                source_metrics.bytes += bytes_estimate;
                generated += records.len();

                // Progress reporting (every report_interval)
                if source_start.elapsed().as_secs() % self.config.report_interval_secs == 0
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
                if let Some(max_duration) = self.config.duration {
                    if start.elapsed() >= max_duration {
                        break;
                    }
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
    }

    #[test]
    fn test_stress_config_builder() {
        let config = StressConfig::with_records(50_000)
            .batch_size(500)
            .measure_latency(true);

        assert_eq!(config.records_per_source, 50_000);
        assert_eq!(config.batch_size, 500);
        assert!(config.measure_latency);
    }

    #[test]
    fn test_stress_runner_basic() {
        let schema = Schema {
            name: "test_source".to_string(),
            description: None,
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    field_type: FieldType::Integer,
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
                    field_type: FieldType::String,
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
}
