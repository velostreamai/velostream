//! Shared helpers for SQL operation performance tests
//!
//! Provides configurable record counts, cardinality, and AdaptiveJobProcessor helpers for benchmarking

pub use crate::velostream::server::v2::{AdaptiveJobProcessor, PartitionedJobConfig};
pub use std::sync::Arc;
pub use std::time::Duration;

/// Get the number of records for performance tests
///
/// Defaults to 10,000 records but can be overridden via environment variable
///
/// Environment variables:
/// - `VELOSTREAM_PERF_RECORDS`: Total number of records to test with
/// - `VELOSTREAM_PERF_CARDINALITY`: Cardinality/group count (defaults to records/10)
///
/// # Examples
///
/// ```bash
/// # Use 100,000 records instead of default 10,000
/// VELOSTREAM_PERF_RECORDS=100000 cargo test
///
/// # Use 1M records with 10K unique groups
/// VELOSTREAM_PERF_RECORDS=1000000 VELOSTREAM_PERF_CARDINALITY=10000 cargo test
/// ```
pub fn get_perf_record_count() -> usize {
    let count = std::env::var("VELOSTREAM_PERF_RECORDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000); // Default to 10,000 records

    // Enforce minimum of 10,000 for accurate performance measurement
    // (smaller datasets have significant measurement overhead)
    count.max(10_000)
}

/// Get the cardinality (number of unique groups) for GROUP BY operations
///
/// Defaults to 1/10th of record count if not specified
/// Can be overridden via VELOSTREAM_PERF_CARDINALITY environment variable
pub fn get_perf_cardinality(record_count: usize) -> usize {
    std::env::var("VELOSTREAM_PERF_CARDINALITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(record_count / 10) // Default to 1/10th of record count
}

/// Display configuration information
pub fn print_perf_config(record_count: usize, cardinality: Option<usize>) {
    println!("📊 Performance Test Configuration:");
    println!(
        "   Records: {} (env: VELOSTREAM_PERF_RECORDS)",
        record_count
    );
    if let Some(card) = cardinality {
        println!(
            "   Cardinality: {} (env: VELOSTREAM_PERF_CARDINALITY)",
            card
        );
    }
    println!("   Tip: Set VELOSTREAM_PERF_RECORDS=100000 for higher-volume testing");
    println!("   Tip: Set VELOSTREAM_PERF_CARDINALITY=100 for group count override");
}

/// Create AdaptiveJobProcessor with specified number of partitions
///
/// # Arguments
/// - `num_partitions`: Number of partitions/cores (e.g., 1 or 4 for benchmarking)
///
/// # Returns
/// Arc-wrapped AdaptiveJobProcessor configured for the specified partition count
pub fn create_adaptive_processor(num_partitions: usize) -> Arc<AdaptiveJobProcessor> {
    let config = PartitionedJobConfig {
        num_partitions: Some(num_partitions),
        annotation_partition_count: None,
        ..Default::default()
    };
    Arc::new(AdaptiveJobProcessor::new(config))
}
