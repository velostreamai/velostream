//! Unit tests for PartitionMetrics
//!
//! Tests throughput tracking, queue depth monitoring, latency tracking, and backpressure detection.

use std::thread;
use std::time::Duration;
use velostream::velostream::server::v2::{PartitionMetrics, PartitionMetricsSnapshot};

#[test]
fn test_metrics_creation() {
    let metrics = PartitionMetrics::new(0);
    assert_eq!(metrics.partition_id(), 0);
    assert_eq!(metrics.total_records_processed(), 0);
    assert_eq!(metrics.queue_depth(), 0);
}

#[test]
fn test_record_batch_processed() {
    let metrics = PartitionMetrics::new(0);

    metrics.record_batch_processed(100);
    assert_eq!(metrics.total_records_processed(), 100);

    metrics.record_batch_processed(50);
    assert_eq!(metrics.total_records_processed(), 150);
}

#[test]
fn test_queue_depth_tracking() {
    let metrics = PartitionMetrics::new(0);

    metrics.update_queue_depth(500);
    assert_eq!(metrics.queue_depth(), 500);

    metrics.update_queue_depth(250);
    assert_eq!(metrics.queue_depth(), 250);
}

#[test]
fn test_latency_tracking() {
    let metrics = PartitionMetrics::new(0);

    metrics.record_latency(Duration::from_micros(100));
    metrics.record_latency(Duration::from_micros(200));

    let avg = metrics.avg_latency_micros();
    assert_eq!(avg, 150); // (100 + 200) / 2
}

#[test]
fn test_throughput_calculation() {
    let metrics = PartitionMetrics::new(0);

    metrics.record_batch_processed(1000);
    thread::sleep(Duration::from_secs(1));

    let throughput = metrics.throughput_per_sec();
    // Should be approximately 1000 rec/sec (allowing for timing variance)
    assert!(
        throughput >= 900 && throughput <= 1100,
        "Throughput {} not in range [900, 1100]",
        throughput
    );
}

#[test]
fn test_backpressure_detection_queue_depth() {
    let metrics = PartitionMetrics::new(0);

    // No backpressure initially
    assert!(!metrics.has_backpressure(1000, Duration::from_millis(100)));

    // Trigger backpressure via queue depth
    metrics.update_queue_depth(1500);
    assert!(metrics.has_backpressure(1000, Duration::from_millis(100)));
}

#[test]
fn test_backpressure_detection_latency() {
    let metrics = PartitionMetrics::new(0);

    // No backpressure initially
    assert!(!metrics.has_backpressure(1000, Duration::from_millis(1)));

    // Trigger backpressure via high latency
    metrics.record_latency(Duration::from_millis(5));
    assert!(metrics.has_backpressure(1000, Duration::from_millis(1)));
}

#[test]
fn test_metrics_snapshot() {
    let metrics = PartitionMetrics::new(2);
    metrics.record_batch_processed(5000);
    metrics.update_queue_depth(100);
    metrics.record_latency(Duration::from_micros(50));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.partition_id, 2);
    assert_eq!(snapshot.records_processed, 5000);
    assert_eq!(snapshot.queue_depth, 100);
    assert_eq!(snapshot.avg_latency_micros, 50);
}

#[test]
fn test_snapshot_formatting() {
    let snapshot = PartitionMetricsSnapshot {
        partition_id: 3,
        records_processed: 10000,
        throughput_per_sec: 200000,
        queue_depth: 50,
        avg_latency_micros: 25,
    };

    let summary = snapshot.format_summary();
    assert!(summary.contains("Partition 3"));
    assert!(summary.contains("200000 rec/sec"));
    assert!(summary.contains("10000 records"));
    assert!(summary.contains("queue depth: 50"));
    assert!(summary.contains("25μs"));
}

#[test]
fn test_metrics_reset() {
    let metrics = PartitionMetrics::new(0);

    metrics.record_batch_processed(1000);
    metrics.update_queue_depth(500);
    metrics.record_latency(Duration::from_micros(100));

    metrics.reset();

    assert_eq!(metrics.total_records_processed(), 0);
    assert_eq!(metrics.queue_depth(), 0);
    assert_eq!(metrics.avg_latency_micros(), 0);
}
