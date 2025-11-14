//! Comprehensive tests for ProfilingHelper timing instrumentation system
//!
//! Tests verify:
//! - Basic timing measurement and recording
//! - Metrics collection and aggregation
//! - Performance summary generation
//! - Cloning and shared state semantics
//! - Accuracy of timing measurements

use std::thread;
use std::time::Duration;
use velostream::velostream::server::processors::profiling_helper::{
    ProfilingHelper, ProfilingMetrics, TimingScope,
};

#[test]
fn test_profiling_helper_new() {
    let helper = ProfilingHelper::new();
    assert_eq!(helper.batch_count(), 0);
    assert_eq!(helper.total_batch_ms(), 0);
}

#[test]
fn test_profiling_helper_basic_timing() {
    let helper = ProfilingHelper::new();

    // Start a timing scope
    let scope = TimingScope::start();

    // Simulate some work
    thread::sleep(Duration::from_millis(10));

    // Finish the scope and record metrics
    let duration_ms = scope.finish();
    let metrics = ProfilingMetrics {
        deser_ms: 0,
        sql_ms: duration_ms,
        ser_ms: 0,
        batch_ms: duration_ms,
    };
    helper.record_metrics(metrics);

    // Verify metrics were recorded
    assert_eq!(helper.batch_count(), 1);
    assert!(helper.total_batch_ms() >= 10);
}

#[test]
fn test_profiling_helper_multiple_operations() {
    let helper = ProfilingHelper::new();

    // Time multiple operations
    for i in 0..5 {
        let scope = TimingScope::start();
        thread::sleep(Duration::from_millis(5));
        let duration_ms = scope.finish();
        let metrics = ProfilingMetrics {
            deser_ms: i as u64,
            sql_ms: duration_ms,
            ser_ms: 0,
            batch_ms: duration_ms,
        };
        helper.record_metrics(metrics);
    }

    // Verify helper recorded all operations
    assert_eq!(helper.batch_count(), 5);
}

#[test]
fn test_profiling_helper_cloneable() {
    let helper1 = ProfilingHelper::new();
    let metrics = ProfilingMetrics {
        deser_ms: 10,
        sql_ms: 20,
        ser_ms: 5,
        batch_ms: 35,
    };
    helper1.record_metrics(metrics);

    // Clone the helper
    let helper2 = helper1.clone();

    // Both should show the same metrics (shared Arc references)
    assert_eq!(helper2.batch_count(), 1);
    assert_eq!(helper2.total_batch_ms(), 35);

    // Record more metrics through helper2
    helper2.record_metrics(metrics);

    // Both should reflect the update
    assert_eq!(helper1.batch_count(), 2);
    assert_eq!(helper2.batch_count(), 2);
}

#[test]
fn test_profiling_helper_record_metrics() {
    let helper = ProfilingHelper::new();

    // Record various metrics
    let m1 = ProfilingMetrics {
        deser_ms: 5,
        sql_ms: 15,
        ser_ms: 10,
        batch_ms: 30,
    };
    let m2 = ProfilingMetrics {
        deser_ms: 3,
        sql_ms: 20,
        ser_ms: 8,
        batch_ms: 31,
    };

    helper.record_metrics(m1);
    helper.record_metrics(m2);

    assert_eq!(helper.batch_count(), 2);
    assert_eq!(helper.total_deser_ms(), 8);
    assert_eq!(helper.total_sql_ms(), 35);
    assert_eq!(helper.total_ser_ms(), 18);
    assert_eq!(helper.total_batch_ms(), 61);
}

#[test]
fn test_profiling_helper_concurrent_measurements() {
    use std::sync::Arc;

    let helper = Arc::new(ProfilingHelper::new());
    let mut handles = vec![];

    // Spawn multiple threads to record metrics
    for i in 0..10 {
        let h = Arc::clone(&helper);
        let handle = thread::spawn(move || {
            let scope = TimingScope::start();
            thread::sleep(Duration::from_millis(5));
            let duration_ms = scope.finish();
            let metrics = ProfilingMetrics {
                deser_ms: i as u64,
                sql_ms: duration_ms,
                ser_ms: 0,
                batch_ms: duration_ms,
            };
            h.record_metrics(metrics);
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all measurements were recorded
    assert_eq!(helper.batch_count(), 10);
}

#[test]
fn test_profiling_helper_average_calculations() {
    let helper = ProfilingHelper::new();

    // Record consistent metrics for testing average calculations
    for _ in 0..4 {
        let metrics = ProfilingMetrics {
            deser_ms: 10,
            sql_ms: 20,
            ser_ms: 5,
            batch_ms: 35,
        };
        helper.record_metrics(metrics);
    }

    assert_eq!(helper.batch_count(), 4);
    assert_eq!(helper.average_deser_ms(), 10);
    assert_eq!(helper.average_sql_ms(), 20);
    assert_eq!(helper.average_ser_ms(), 5);
    assert_eq!(helper.average_batch_ms(), 35);
}

#[test]
fn test_profiling_helper_summary() {
    let helper = ProfilingHelper::new();

    let metrics = ProfilingMetrics {
        deser_ms: 10,
        sql_ms: 20,
        ser_ms: 5,
        batch_ms: 35,
    };
    helper.record_metrics(metrics);

    let summary = helper.get_summary();
    assert_eq!(summary.total_deser_ms, 10);
    assert_eq!(summary.total_sql_ms, 20);
    assert_eq!(summary.total_ser_ms, 5);
    assert_eq!(summary.total_batch_ms, 35);
    assert_eq!(summary.batch_count, 1);
}

#[test]
fn test_profiling_helper_reset() {
    let helper = ProfilingHelper::new();

    let metrics = ProfilingMetrics {
        deser_ms: 10,
        sql_ms: 20,
        ser_ms: 5,
        batch_ms: 35,
    };
    helper.record_metrics(metrics);

    // Verify metrics were recorded
    assert_eq!(helper.batch_count(), 1);
    assert_eq!(helper.total_batch_ms(), 35);

    // Reset metrics
    helper.reset();

    // Verify all metrics are cleared
    assert_eq!(helper.batch_count(), 0);
    assert_eq!(helper.total_deser_ms(), 0);
    assert_eq!(helper.total_sql_ms(), 0);
    assert_eq!(helper.total_ser_ms(), 0);
    assert_eq!(helper.total_batch_ms(), 0);
}

#[test]
fn test_timing_scope_elapsed() {
    let scope = TimingScope::start();
    thread::sleep(Duration::from_millis(10));
    let elapsed = scope.elapsed_ms();
    assert!(elapsed >= 10);
}

#[test]
fn test_timing_scope_finish() {
    let scope = TimingScope::start();
    thread::sleep(Duration::from_millis(10));
    let duration = scope.finish();
    assert!(duration >= 10);
}

#[test]
fn test_profiling_metrics_total() {
    let metrics = ProfilingMetrics {
        deser_ms: 10,
        sql_ms: 20,
        ser_ms: 5,
        batch_ms: 35,
    };
    assert_eq!(metrics.total_ms(), 35);
}

#[test]
fn test_profiling_metrics_is_empty() {
    let empty_metrics = ProfilingMetrics::new();
    assert!(empty_metrics.is_empty());

    let non_empty = ProfilingMetrics {
        deser_ms: 1,
        sql_ms: 0,
        ser_ms: 0,
        batch_ms: 1,
    };
    assert!(!non_empty.is_empty());
}

#[test]
fn test_profiling_helper_large_numbers() {
    let helper = ProfilingHelper::new();

    // Test with large numbers (simulating real production loads)
    let metrics = ProfilingMetrics {
        deser_ms: 1_000_000,
        sql_ms: 2_000_000,
        ser_ms: 500_000,
        batch_ms: 3_500_000,
    };
    helper.record_metrics(metrics);

    assert_eq!(helper.total_deser_ms(), 1_000_000);
    assert_eq!(helper.total_sql_ms(), 2_000_000);
    assert_eq!(helper.total_batch_ms(), 3_500_000);
}

#[test]
fn test_profiling_helper_throughput_estimation() {
    let helper = ProfilingHelper::new();

    // Record 100ms of processing
    let metrics = ProfilingMetrics {
        deser_ms: 20,
        sql_ms: 50,
        ser_ms: 30,
        batch_ms: 100,
    };
    helper.record_metrics(metrics);

    // Estimate: 1000 records in 100ms = 10,000 records/sec
    let throughput = helper.estimated_records_per_sec(1000);
    assert!(throughput >= 9000.0 && throughput <= 11000.0); // Allow some variance
}
