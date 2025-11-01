//! Error tracking helper for processors
//!
//! Provides error tracking functionality for recording errors to the metrics system
//! with integrated distributed tracing support via parent span linking.

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
        Self::record_error_traced(observability, job_name, error_message, None);
    }

    /// Record an error message with trace context linking for distributed tracing
    ///
    /// This spawns a background task while optionally linking the error tracking
    /// operation to a parent trace span for distributed tracing.
    ///
    /// # Arguments
    /// * `observability` - Optional observability manager
    /// * `job_name` - Name of the job (will be prefixed to error message)
    /// * `error_message` - Error message to record
    /// * `parent_context` - Optional parent span context for linking the error tracking span
    pub fn record_error_traced(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        error_message: String,
        parent_context: Option<opentelemetry::trace::SpanContext>,
    ) {
        if let Some(obs_manager) = observability {
            let manager = obs_manager.clone();
            let prefixed_message = format!("[{}] {}", job_name, error_message);
            let job_name_owned = job_name.to_string();

            tokio::spawn(async move {
                let manager_read = manager.read().await;

                // Create error tracking span if telemetry available, linked to parent if context provided
                if let Some(telemetry) = manager_read.telemetry() {
                    let mut error_span = telemetry.start_streaming_span(
                        &job_name_owned,
                        "error_tracking",
                        1,
                        parent_context,
                    );

                    if let Some(metrics) = manager_read.metrics() {
                        metrics.record_error_message(prefixed_message);
                    }
                    error_span.set_success();
                } else if let Some(metrics) = manager_read.metrics() {
                    // No telemetry, just record metric
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
