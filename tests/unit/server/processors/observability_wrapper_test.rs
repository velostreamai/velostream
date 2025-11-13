//! Comprehensive tests for ObservabilityWrapper unified metrics and observability system
//!
//! Tests verify:
//! - Basic initialization and configuration
//! - Observability manager integration
//! - Metrics collection and aggregation
//! - Dead Letter Queue (DLQ) support
//! - Builder pattern functionality
//! - Cloning and shared state semantics

use velostream::velostream::server::processors::observability_wrapper::{
    ObservabilityMetricsSummary, ObservabilityWrapper,
};

#[test]
fn test_observability_wrapper_new() {
    let wrapper = ObservabilityWrapper::new();
    assert!(!wrapper.has_observability());
    assert!(!wrapper.has_dlq());
    assert_eq!(wrapper.total_records_processed(), 0);
    assert_eq!(wrapper.total_records_failed(), 0);
}

#[test]
fn test_observability_wrapper_with_dlq() {
    let wrapper = ObservabilityWrapper::with_dlq();
    assert!(!wrapper.has_observability());
    assert!(wrapper.has_dlq());
    assert_eq!(wrapper.total_records_processed(), 0);
}

#[test]
fn test_observability_wrapper_record_success() {
    let wrapper = ObservabilityWrapper::new();

    // Record single success
    wrapper.record_success(1);
    assert_eq!(wrapper.total_records_processed(), 1);

    // Record multiple successes
    wrapper.record_success(10);
    assert_eq!(wrapper.total_records_processed(), 11);

    // Record batch
    wrapper.record_success(100);
    assert_eq!(wrapper.total_records_processed(), 111);
}

#[test]
fn test_observability_wrapper_record_failure() {
    let wrapper = ObservabilityWrapper::new();

    // Record single failure
    wrapper.record_failure(1);
    assert_eq!(wrapper.total_records_failed(), 1);

    // Record multiple failures
    wrapper.record_failure(5);
    assert_eq!(wrapper.total_records_failed(), 6);

    // Record batch failures
    wrapper.record_failure(50);
    assert_eq!(wrapper.total_records_failed(), 56);
}

#[test]
fn test_observability_wrapper_mixed_success_failure() {
    let wrapper = ObservabilityWrapper::new();

    // Process a batch with mixed results
    wrapper.record_success(80);
    wrapper.record_failure(20);

    assert_eq!(wrapper.total_records_processed(), 80);
    assert_eq!(wrapper.total_records_failed(), 20);

    // Continue processing
    wrapper.record_success(45);
    wrapper.record_failure(5);

    assert_eq!(wrapper.total_records_processed(), 125);
    assert_eq!(wrapper.total_records_failed(), 25);
}

#[test]
fn test_observability_wrapper_builder() {
    let wrapper = ObservabilityWrapper::builder().with_dlq(true).build();

    assert!(wrapper.has_dlq());
    assert!(!wrapper.has_observability());
}

#[test]
fn test_observability_wrapper_builder_default() {
    let wrapper = ObservabilityWrapper::builder().build();

    assert!(!wrapper.has_dlq());
    assert!(!wrapper.has_observability());
}

#[test]
fn test_observability_wrapper_cloneable() {
    let wrapper1 = ObservabilityWrapper::new();
    wrapper1.record_success(5);

    // Clone the wrapper
    let wrapper2 = wrapper1.clone();

    // Both should share the same Arc references
    assert_eq!(wrapper2.total_records_processed(), 5);

    // Changes in one should be visible in the other (shared state)
    wrapper2.record_success(3);
    assert_eq!(wrapper1.total_records_processed(), 8);

    // Record failure through wrapper2
    wrapper2.record_failure(2);
    assert_eq!(wrapper1.total_records_failed(), 2);
}

#[test]
fn test_observability_wrapper_concurrent_updates() {
    use std::sync::Arc;
    use std::thread;

    let wrapper = Arc::new(ObservabilityWrapper::new());
    let mut handles = vec![];

    // Spawn multiple threads to update metrics
    for _ in 0..10 {
        let w = Arc::clone(&wrapper);
        let handle = thread::spawn(move || {
            w.record_success(10);
            w.record_failure(1);
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify aggregated metrics
    assert_eq!(wrapper.total_records_processed(), 100);
    assert_eq!(wrapper.total_records_failed(), 10);
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
    assert_eq!(wrapper.total_records_processed(), 0);
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

#[test]
fn test_observability_wrapper_with_dlq_and_metrics() {
    let wrapper = ObservabilityWrapper::with_dlq();

    // Verify all features are initialized
    assert!(wrapper.has_dlq());
    assert!(!wrapper.has_observability());

    // Verify metrics are tracked
    wrapper.record_success(50);
    wrapper.record_failure(10);

    assert_eq!(wrapper.total_records_processed(), 50);
    assert_eq!(wrapper.total_records_failed(), 10);

    // Verify summary includes all information
    let summary = wrapper.get_summary();
    assert_eq!(summary.records_processed, 50);
    assert_eq!(summary.records_failed, 10);
    assert!(summary.has_dlq);
}

#[test]
fn test_observability_wrapper_zero_records() {
    let wrapper = ObservabilityWrapper::new();

    // Test initial state
    assert_eq!(wrapper.total_records_processed(), 0);
    assert_eq!(wrapper.total_records_failed(), 0);

    // Test summary
    let summary = wrapper.get_summary();
    assert_eq!(summary.records_processed, 0);
    assert_eq!(summary.records_failed, 0);
}

#[test]
fn test_observability_wrapper_large_numbers() {
    let wrapper = ObservabilityWrapper::new();

    // Test with large numbers (simulating real production loads)
    wrapper.record_success(1_000_000);
    wrapper.record_failure(10_000);

    assert_eq!(wrapper.total_records_processed(), 1_000_000);
    assert_eq!(wrapper.total_records_failed(), 10_000);

    // Continue processing
    wrapper.record_success(500_000);
    wrapper.record_failure(5_000);

    assert_eq!(wrapper.total_records_processed(), 1_500_000);
    assert_eq!(wrapper.total_records_failed(), 15_000);
}
