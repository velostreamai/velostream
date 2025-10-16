//! Shared observability utilities to eliminate duplication
//!
//! This module provides helper functions for common observability patterns used across
//! ProcessorMetricsHelper and ObservabilityHelper, reducing code duplication by ~30%.

use crate::velostream::observability::{
    label_extraction::{extract_label_values, LabelExtractionConfig},
    SharedObservabilityManager,
};
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::parser::annotations::MetricAnnotation;
use log::debug;
use std::time::Instant;

/// Safely access observability manager with read lock
///
/// This helper eliminates the repeated pattern of:
/// ```rust,ignore
/// if let Some(obs) = observability {
///     match obs.read().await {
///         obs_lock => { ... }
///     }
/// }
/// ```
///
/// # Arguments
/// * `observability` - Optional observability manager
/// * `f` - Closure that receives the locked observability manager
///
/// # Returns
/// Result of the closure if observability is available and lock acquired
pub async fn with_observability_lock<F, T>(
    observability: &Option<SharedObservabilityManager>,
    f: F,
) -> Option<T>
where
    F: FnOnce(&crate::velostream::observability::ObservabilityManager) -> Option<T>,
{
    if let Some(obs) = observability {
        match obs.read().await {
            obs_lock => f(&*obs_lock),
        }
    } else {
        None
    }
}

/// Try-lock version for non-async contexts
///
/// Used when we can't await and need try_read() instead of read()
pub fn with_observability_try_lock<F, T>(
    observability: &Option<SharedObservabilityManager>,
    f: F,
) -> Option<T>
where
    F: FnOnce(&crate::velostream::observability::ObservabilityManager) -> Option<T>,
{
    if let Some(obs) = observability {
        if let Ok(obs_lock) = obs.try_read() {
            f(&obs_lock)
        } else {
            None
        }
    } else {
        None
    }
}

/// Calculate throughput (records per second)
///
/// Centralizes the throughput calculation formula to ensure consistency
/// across deserialization, serialization, and other telemetry operations.
///
/// # Arguments
/// * `record_count` - Number of records processed
/// * `duration_ms` - Time taken in milliseconds
///
/// # Returns
/// Throughput in records per second
pub fn calculate_throughput(record_count: usize, duration_ms: u64) -> f64 {
    if duration_ms > 0 {
        (record_count as f64 / duration_ms as f64) * 1000.0
    } else {
        0.0
    }
}

/// Extract and validate labels in a single operation
///
/// Combines label extraction with performance telemetry and validation,
/// eliminating the repeated 15-line pattern across emit methods.
///
/// # Arguments
/// * `record` - Stream record to extract labels from
/// * `annotation` - Metric annotation defining expected labels
/// * `metric_type_name` - Type of metric (counter/gauge/histogram) for logging
/// * `job_name` - Job name for logging
/// * `strict_mode` - Whether to require exact label match
/// * `record_label_extract_time` - Callback to record extraction time
///
/// # Returns
/// - `Some(labels)` if extraction and validation succeeded
/// - `None` if validation failed (strict mode mismatch)
pub async fn extract_and_validate_labels<F>(
    record: &StreamRecord,
    annotation: &MetricAnnotation,
    metric_type_name: &str,
    job_name: &str,
    strict_mode: bool,
    mut record_label_extract_time: F,
) -> Option<Vec<String>>
where
    F: FnMut(u64),
{
    let extract_start = Instant::now();
    let config = LabelExtractionConfig::default();
    let label_values = extract_label_values(record, &annotation.labels, &config);
    let extract_duration = extract_start.elapsed().as_micros() as u64;
    record_label_extract_time(extract_duration);

    // Validate labels
    let labels_valid = if strict_mode {
        // Strict mode: require exact match
        label_values.len() == annotation.labels.len()
    } else {
        // Permissive mode: always valid
        true
    };

    if !labels_valid {
        debug!(
            "Job '{}': Skipping {} '{}' - strict mode: missing label values (expected {}, got {})",
            job_name,
            metric_type_name,
            annotation.name,
            annotation.labels.len(),
            label_values.len()
        );
        return None;
    }

    Some(label_values)
}
