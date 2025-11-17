//! ObservabilityWrapper - Unified observability and metrics initialization
//!
//! This module consolidates common observability, metrics, and tracing patterns
//! used across Simple, Transactional, and V2 Coordinator processors.
//!
//! It provides:
//! - Unified initialization of observability infrastructure
//! - Shared metrics collection and aggregation
//! - Consistent error tracking across processors
//! - Optional Dead Letter Queue support (for non-transactional processors)

use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::server::processors::common::DeadLetterQueue;
use crate::velostream::server::processors::metrics_collector::MetricsCollector;
use crate::velostream::server::processors::metrics_helper::ProcessorMetricsHelper;
use std::sync::Arc;

/// Unified observability and metrics wrapper for all processors
///
/// This struct consolidates all observability-related fields and provides
/// a single initialization point for metrics, tracing, and dead-letter handling.
///
/// # Features
/// - Optional observability/tracing support (for span creation and propagation)
/// - Unified metrics collection (counter, gauge, histogram via ProcessorMetricsHelper)
/// - Runtime metrics aggregation (via MetricsCollector)
/// - Optional Dead Letter Queue for failed record handling
/// - Thread-safe via Arc for shared access
///
/// # Initialization Patterns
///
/// Basic initialization (no observability):
/// ```ignore
/// let wrapper = ObservabilityWrapper::new();
/// ```
///
/// With observability/tracing:
/// ```ignore
/// let wrapper = ObservabilityWrapper::with_observability(Some(obs_manager));
/// ```
///
/// With Dead Letter Queue (non-transactional processors only):
/// ```ignore
/// let wrapper = ObservabilityWrapper::builder()
///     .with_observability(Some(obs_manager))
///     .with_dlq(true)
///     .build();
/// ```
#[derive(Clone)]
pub struct ObservabilityWrapper {
    /// Optional observability manager for tracing and span propagation
    observability: Option<SharedObservabilityManager>,

    /// SQL-annotated metrics helper for SQL query metrics
    /// Handles counter, gauge, and histogram metrics from SQL annotations
    metrics_helper: Arc<ProcessorMetricsHelper>,

    /// Runtime metrics collector for batch-level and record-level metrics
    metrics_collector: Arc<MetricsCollector>,

    /// Optional Dead Letter Queue for failed records
    /// Only used in non-transactional processors (Simple)
    dlq: Option<Arc<DeadLetterQueue>>,
}

impl ObservabilityWrapper {
    /// Create a new wrapper with default settings (no observability, no DLQ)
    pub fn new() -> Self {
        Self {
            observability: None,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: None,
        }
    }

    /// Create a new wrapper with observability support
    pub fn with_observability(observability: Option<SharedObservabilityManager>) -> Self {
        Self {
            observability,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: None,
        }
    }

    /// Create a new wrapper with DLQ support (for non-transactional processors)
    pub fn with_dlq() -> Self {
        Self {
            observability: None,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: Some(Arc::new(DeadLetterQueue::new())),
        }
    }

    /// Create a new wrapper with both observability and DLQ
    pub fn with_observability_and_dlq(observability: Option<SharedObservabilityManager>) -> Self {
        Self {
            observability,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: Some(Arc::new(DeadLetterQueue::new())),
        }
    }

    /// Create a new builder for more complex configurations
    pub fn builder() -> ObservabilityWrapperBuilder {
        ObservabilityWrapperBuilder::new()
    }

    // ===== Accessors =====

    /// Get reference to observability manager (if present)
    pub fn observability(&self) -> Option<&SharedObservabilityManager> {
        self.observability.as_ref()
    }

    /// Check if observability is enabled
    pub fn has_observability(&self) -> bool {
        self.observability.is_some()
    }

    /// Get observability as reference for helper functions
    /// This returns &Option which is what ObservabilityHelper expects
    pub fn observability_ref(&self) -> &Option<SharedObservabilityManager> {
        &self.observability
    }

    /// Get reference to metrics helper
    pub fn metrics_helper(&self) -> &ProcessorMetricsHelper {
        &self.metrics_helper
    }

    /// Get reference to metrics collector
    pub fn metrics_collector(&self) -> &MetricsCollector {
        &self.metrics_collector
    }

    /// Check if DLQ is enabled
    pub fn has_dlq(&self) -> bool {
        self.dlq.is_some()
    }

    /// Get reference to DLQ (if enabled)
    pub fn dlq(&self) -> Option<&Arc<DeadLetterQueue>> {
        self.dlq.as_ref()
    }

    // ===== Metrics Methods =====

    /// Get total records processed
    pub fn total_records_processed(&self) -> u64 {
        self.metrics_collector.total_records()
    }

    /// Get total records failed
    pub fn total_records_failed(&self) -> u64 {
        self.metrics_collector.failed_records()
    }

    /// Record successful record processing
    pub fn record_success(&self, count: u64) {
        self.metrics_collector.add_records(count);
    }

    /// Record failed record processing
    pub fn record_failure(&self, count: u64) {
        self.metrics_collector.add_failed_records(count);
    }

    /// Get a summary of all observability metrics
    pub fn get_summary(&self) -> ObservabilityMetricsSummary {
        ObservabilityMetricsSummary {
            records_processed: self.total_records_processed(),
            records_failed: self.total_records_failed(),
            has_observability: self.has_observability(),
            has_dlq: self.has_dlq(),
        }
    }
}

impl Default for ObservabilityWrapper {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for ObservabilityWrapper with more control over initialization
pub struct ObservabilityWrapperBuilder {
    observability: Option<SharedObservabilityManager>,
    enable_dlq: bool,
}

impl ObservabilityWrapperBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            observability: None,
            enable_dlq: false,
        }
    }

    /// Set observability manager
    pub fn with_observability(mut self, obs: Option<SharedObservabilityManager>) -> Self {
        self.observability = obs;
        self
    }

    /// Enable Dead Letter Queue
    pub fn with_dlq(mut self, enable: bool) -> Self {
        self.enable_dlq = enable;
        self
    }

    /// Build the wrapper
    pub fn build(self) -> ObservabilityWrapper {
        ObservabilityWrapper {
            observability: self.observability,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: if self.enable_dlq {
                Some(Arc::new(DeadLetterQueue::new()))
            } else {
                None
            },
        }
    }
}

impl Default for ObservabilityWrapperBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of observability metrics
#[derive(Debug, Clone, Copy)]
pub struct ObservabilityMetricsSummary {
    pub records_processed: u64,
    pub records_failed: u64,
    pub has_observability: bool,
    pub has_dlq: bool,
}

impl std::fmt::Display for ObservabilityMetricsSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ObservabilityMetricsSummary {{ records: {}/{}, observability: {}, dlq_enabled: {} }}",
            self.records_processed, self.records_failed, self.has_observability, self.has_dlq,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_wrapper_new() {
        let wrapper = ObservabilityWrapper::new();
        assert!(!wrapper.has_observability());
        assert!(!wrapper.has_dlq());
        assert_eq!(wrapper.total_records_processed(), 0);
    }

    #[test]
    fn test_observability_wrapper_with_dlq() {
        let wrapper = ObservabilityWrapper::with_dlq();
        assert!(!wrapper.has_observability());
        assert!(wrapper.has_dlq());
    }

    #[test]
    fn test_observability_wrapper_record_success() {
        let wrapper = ObservabilityWrapper::new();
        wrapper.record_success(10);
        assert_eq!(wrapper.total_records_processed(), 10);
    }

    #[test]
    fn test_observability_wrapper_record_failure() {
        let wrapper = ObservabilityWrapper::new();
        wrapper.record_failure(5);
        assert_eq!(wrapper.total_records_failed(), 5);
    }

    #[test]
    fn test_observability_wrapper_builder() {
        let wrapper = ObservabilityWrapper::builder().with_dlq(true).build();
        assert!(wrapper.has_dlq());
        assert!(!wrapper.has_observability());
    }

    #[test]
    fn test_observability_wrapper_cloneable() {
        let wrapper1 = ObservabilityWrapper::new();
        wrapper1.record_success(5);

        let wrapper2 = wrapper1.clone();
        // Both should share the same Arc references
        assert_eq!(wrapper2.total_records_processed(), 5);

        // Changes in one should be visible in the other
        wrapper2.record_success(3);
        assert_eq!(wrapper1.total_records_processed(), 8);
    }

    #[test]
    fn test_observability_metrics_summary() {
        let wrapper = ObservabilityWrapper::with_dlq();
        wrapper.record_success(100);
        wrapper.record_failure(5);

        let summary = wrapper.get_summary();
        assert_eq!(summary.records_processed, 100);
        assert_eq!(summary.records_failed, 5);
        assert!(summary.has_dlq);
        assert!(!summary.has_observability);
    }

    #[test]
    fn test_observability_wrapper_default() {
        let wrapper = ObservabilityWrapper::default();
        assert!(!wrapper.has_observability());
        assert!(!wrapper.has_dlq());
    }

    #[test]
    fn test_observability_wrapper_builder_default() {
        let wrapper = ObservabilityWrapperBuilder::default()
            .with_dlq(true)
            .build();
        assert!(wrapper.has_dlq());
    }

    #[test]
    fn test_observability_metrics_summary_display() {
        let summary = ObservabilityMetricsSummary {
            records_processed: 100,
            records_failed: 5,
            has_observability: true,
            has_dlq: true,
        };
        let display_str = summary.to_string();
        assert!(display_str.contains("100"));
        assert!(display_str.contains("5"));
        assert!(display_str.contains("dlq_enabled: true"));
    }
}
