//! Error tracking helper for processors
//!
//! Provides common error tracking functionality for recording errors to the metrics system,
//! reducing code duplication across processor implementations (simple and transactional).

use crate::velostream::observability::SharedObservabilityManager;

/// Helper to record errors to the observability system for dashboard visibility
pub struct ErrorTracker;

impl ErrorTracker {
    /// Record an error message to the metrics system asynchronously
    ///
    /// This spawns a background task to avoid blocking the processor operations.
    /// Errors are recorded in the error_tracker within MetricsProvider, making them
    /// visible in Grafana's Error Tracking dashboard.
    ///
    /// # Arguments
    /// * `observability` - Optional observability manager
    /// * `job_name` - Name of the job (will be prefixed to error message)
    /// * `error_message` - Error message to record
    pub fn record_error(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        error_message: String,
    ) {
        if let Some(ref obs_manager) = observability {
            let manager = obs_manager.clone();
            let prefixed_message = format!("[{}] {}", job_name, error_message);
            tokio::spawn(async move {
                let manager_read = manager.read().await;
                if let Some(metrics) = manager_read.metrics() {
                    metrics.record_error_message(prefixed_message);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_tracker_with_none() {
        // Should not panic when observability is None
        ErrorTracker::record_error(&None, "test-job", "Test error".to_string());
    }
}
