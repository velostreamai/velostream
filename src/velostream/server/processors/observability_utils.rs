//! Shared observability utilities
//!
//! This module provides helper functions for common observability patterns used across
//! ProcessorMetricsHelper and ObservabilityHelper.

use crate::velostream::observability::SharedObservabilityManager;

/// Try-lock helper for non-async contexts
///
/// Acquires a read lock on the `SharedObservabilityManager` using `try_read()`.
/// Returns `None` if observability is not configured or the lock cannot be acquired.
///
/// During normal runtime, `SharedObservabilityManager` only acquires read locks
/// (metrics emission, span creation), so `try_read()` should virtually always
/// succeed. The only write lock is held during initialization.
pub fn with_observability_try_lock<F, T>(
    observability: &Option<SharedObservabilityManager>,
    f: F,
) -> Option<T>
where
    F: FnOnce(&crate::velostream::observability::ObservabilityManager) -> Option<T>,
{
    if let Some(obs) = observability {
        match obs.try_read() {
            Ok(obs_lock) => f(&obs_lock),
            Err(_) => {
                log::warn!(
                    "with_observability_try_lock: could not acquire read lock, \
                     observability data will be dropped for this batch"
                );
                None
            }
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
/// Throughput in records per second. For sub-millisecond durations (duration_ms == 0)
/// with non-zero records, treats the duration as 1ms to avoid reporting 0 throughput
/// for fast operations.
pub fn calculate_throughput(record_count: usize, duration_ms: u64) -> f64 {
    if record_count == 0 {
        return 0.0;
    }
    // Treat sub-millisecond durations as 1ms to avoid reporting 0 throughput
    // for fast operations that complete faster than timer resolution.
    let effective_ms = duration_ms.max(1);
    (record_count as f64 / effective_ms as f64) * 1000.0
}
