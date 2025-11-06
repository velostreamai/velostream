//! FR-082 Phase 3: Coordinator Performance Benchmark
//!
//! Tests backpressure detection, throttling, and multi-partition throughput
//! Target: 800K rec/sec on 4 cores with backpressure detection <1ms lag

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use velostream::velostream::server::v2::{
    HashRouter, PartitionMetrics, PartitionPrometheusExporter, PartitionStateManager,
    PartitionStrategy, PartitionedJobConfig, PartitionedJobCoordinator,
};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Create test record with trader ID
fn create_test_record(trader_id: usize) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "trader_id".to_string(),
        FieldValue::Integer(trader_id as i64),
    );
    fields.insert(
        "price".to_string(),
        FieldValue::Float(100.0 + trader_id as f64),
    );
    fields.insert(
        "symbol".to_string(),
        FieldValue::String(format!("STOCK{}", trader_id % 100)),
    );
    StreamRecord::new(fields)
}

/// Benchmark: 4-partition coordinator baseline throughput
#[tokio::test]
#[ignore] // Run with: cargo test --release phase3_4partition_baseline -- --ignored --nocapture
async fn phase3_4partition_baseline_throughput() {
    println!("\n=== FR-082 Phase 3: 4-Partition Baseline Throughput ===\n");

    let config = PartitionedJobConfig {
        num_partitions: Some(4),
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let (managers, senders) = coordinator.initialize_partitions();
    let partition_metrics: Vec<_> = managers.iter().map(|m| m.metrics()).collect();

    // Spawn partition workers
    let mut handles = vec![];
    for (partition_id, mut rx) in senders
        .iter()
        .map(|_tx| mpsc::channel::<StreamRecord>(1000))
        .enumerate()
    {
        let manager = managers[partition_id].clone();
        handles.push(tokio::spawn(async move {
            let mut count = 0;
            while let Some(record) = rx.1.recv().await {
                let _ = manager.process_record(&record);
                count += 1;
            }
            count
        }));
    }

    // Benchmark: Send 1M records
    const TOTAL_RECORDS: usize = 1_000_000;
    let start = Instant::now();

    for i in 0..TOTAL_RECORDS {
        let record = create_test_record(i);
        let partition_id = i % 4;
        let _ = senders[partition_id].send(record).await;
    }

    // Wait for processing
    drop(senders);
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let throughput = TOTAL_RECORDS as f64 / elapsed.as_secs_f64();

    println!("Records processed: {}", TOTAL_RECORDS);
    println!("Elapsed time: {:?}", elapsed);
    println!("Throughput: {:.0} rec/sec", throughput);
    println!("\nPer-Partition Metrics:");
    for (idx, metrics) in partition_metrics.iter().enumerate() {
        let snapshot = metrics.snapshot();
        println!(
            "  Partition {}: {} records, {} rec/sec",
            idx, snapshot.records_processed, snapshot.throughput_per_sec
        );
    }

    // Target: 800K rec/sec on 4 cores
    println!("\nTarget: 800,000 rec/sec (4 cores)");
    println!(
        "Achievement: {:.0} rec/sec ({:.1}%)",
        throughput,
        (throughput / 800_000.0) * 100.0
    );

    assert!(
        throughput > 500_000.0,
        "Throughput too low: {} rec/sec",
        throughput
    );
}

/// Benchmark: Backpressure detection latency
#[tokio::test]
#[ignore] // Run with: cargo test --release phase3_backpressure_detection_lag -- --ignored --nocapture
async fn phase3_backpressure_detection_lag() {
    println!("\n=== FR-082 Phase 3: Backpressure Detection Lag ===\n");

    let config = PartitionedJobConfig {
        num_partitions: Some(4),
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![
        Arc::new(PartitionMetrics::new(0)),
        Arc::new(PartitionMetrics::new(1)),
        Arc::new(PartitionMetrics::new(2)),
        Arc::new(PartitionMetrics::new(3)),
    ];

    // Simulate queue filling up
    metrics[2].update_queue_depth(850); // 85% utilization (critical)

    // Measure backpressure detection latency
    let iterations = 100_000;
    let start = Instant::now();

    for _ in 0..iterations {
        let _ = coordinator.check_backpressure(&metrics);
    }

    let elapsed = start.elapsed();
    let avg_latency_nanos = elapsed.as_nanos() / iterations;
    let avg_latency_micros = avg_latency_nanos as f64 / 1000.0;

    println!("Backpressure checks: {}", iterations);
    println!("Total time: {:?}", elapsed);
    println!(
        "Avg latency per check: {:.3}μs ({} ns)",
        avg_latency_micros, avg_latency_nanos
    );

    // Target: <1ms lag (<1000μs)
    println!("\nTarget: <1ms per check");
    println!("Achievement: {:.3}μs", avg_latency_micros);

    assert!(
        avg_latency_micros < 1000.0,
        "Backpressure detection too slow: {:.3}μs",
        avg_latency_micros
    );
}

/// Benchmark: Hot partition detection performance
#[tokio::test]
#[ignore] // Run with: cargo test --release phase3_hot_partition_detection -- --ignored --nocapture
async fn phase3_hot_partition_detection() {
    println!("\n=== FR-082 Phase 3: Hot Partition Detection ===\n");

    let config = PartitionedJobConfig {
        num_partitions: Some(4),
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![
        Arc::new(PartitionMetrics::new(0)),
        Arc::new(PartitionMetrics::new(1)),
        Arc::new(PartitionMetrics::new(2)),
        Arc::new(PartitionMetrics::new(3)),
    ];

    // Simulate throughput imbalance (partition 2 is hot)
    metrics[0].record_batch_processed(100_000);
    metrics[1].record_batch_processed(100_000);
    metrics[2].record_batch_processed(500_000); // 5x hotter
    metrics[3].record_batch_processed(100_000);

    // Advance time for throughput calculation
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Measure hot partition detection latency
    let iterations = 10_000;
    let start = Instant::now();

    for _ in 0..iterations {
        let _ = coordinator.detect_hot_partitions(&metrics, 2.0);
    }

    let elapsed = start.elapsed();
    let avg_latency_micros = elapsed.as_micros() as f64 / iterations as f64;

    println!("Hot partition checks: {}", iterations);
    println!("Total time: {:?}", elapsed);
    println!("Avg latency per check: {:.3}μs", avg_latency_micros);

    // Verify detection works
    let hot_partitions = coordinator.detect_hot_partitions(&metrics, 2.0);
    println!("\nDetected hot partitions: {:?}", hot_partitions);

    println!("\nTarget: <10μs per check");
    println!("Achievement: {:.3}μs", avg_latency_micros);

    assert!(
        avg_latency_micros < 10.0,
        "Hot partition detection too slow: {:.3}μs",
        avg_latency_micros
    );
}

/// Benchmark: Throttle delay calculation overhead
#[tokio::test]
#[ignore] // Run with: cargo test --release phase3_throttle_calculation_overhead -- --ignored --nocapture
async fn phase3_throttle_calculation_overhead() {
    println!("\n=== FR-082 Phase 3: Throttle Delay Calculation Overhead ===\n");

    let config = PartitionedJobConfig {
        num_partitions: Some(4),
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![
        Arc::new(PartitionMetrics::new(0)),
        Arc::new(PartitionMetrics::new(1)),
        Arc::new(PartitionMetrics::new(2)),
        Arc::new(PartitionMetrics::new(3)),
    ];

    // Set varying queue depths
    metrics[0].update_queue_depth(300); // Healthy
    metrics[1].update_queue_depth(750); // Warning
    metrics[2].update_queue_depth(900); // Critical
    metrics[3].update_queue_depth(500); // Healthy

    // Measure throttle calculation latency
    let iterations = 100_000;
    let start = Instant::now();

    for _ in 0..iterations {
        let _ = coordinator.calculate_throttle_delay(&metrics);
    }

    let elapsed = start.elapsed();
    let avg_latency_nanos = elapsed.as_nanos() / iterations;
    let avg_latency_micros = avg_latency_nanos as f64 / 1000.0;

    println!("Throttle calculations: {}", iterations);
    println!("Total time: {:?}", elapsed);
    println!(
        "Avg latency per calc: {:.3}μs ({} ns)",
        avg_latency_micros, avg_latency_nanos
    );

    println!("\nTarget: <1μs per calculation");
    println!("Achievement: {:.3}μs", avg_latency_micros);

    assert!(
        avg_latency_micros < 1.0,
        "Throttle calculation too slow: {:.3}μs",
        avg_latency_micros
    );
}

/// Benchmark: Prometheus metrics export overhead
#[tokio::test]
#[ignore] // Run with: cargo test --release phase3_prometheus_export_overhead -- --ignored --nocapture
async fn phase3_prometheus_export_overhead() {
    println!("\n=== FR-082 Phase 3: Prometheus Export Overhead ===\n");

    let exporter = PartitionPrometheusExporter::new(4).unwrap();

    let metrics = vec![
        Arc::new(PartitionMetrics::new(0)),
        Arc::new(PartitionMetrics::new(1)),
        Arc::new(PartitionMetrics::new(2)),
        Arc::new(PartitionMetrics::new(3)),
    ];

    // Set metrics
    for m in &metrics {
        m.record_batch_processed(100_000);
        m.update_queue_depth(500);
    }

    // Measure update overhead
    let iterations = 10_000;
    let start = Instant::now();

    for _ in 0..iterations {
        exporter.update_metrics(&metrics, 1000);
    }

    let elapsed = start.elapsed();
    let avg_latency_micros = elapsed.as_micros() as f64 / iterations as f64;

    println!("Metrics updates: {}", iterations);
    println!("Total time: {:?}", elapsed);
    println!("Avg latency per update: {:.3}μs", avg_latency_micros);

    // Measure export overhead
    let export_start = Instant::now();
    let output = exporter.export_metrics();
    let export_elapsed = export_start.elapsed();

    println!("\nExport time: {:?}", export_elapsed);
    println!("Output size: {} bytes", output.len());

    println!("\nTarget: <10μs update, <1ms export");
    println!(
        "Achievement: {:.3}μs update, {:?} export",
        avg_latency_micros, export_elapsed
    );

    assert!(
        avg_latency_micros < 10.0,
        "Metrics update too slow: {:.3}μs",
        avg_latency_micros
    );
    assert!(
        export_elapsed < Duration::from_millis(1),
        "Export too slow: {:?}",
        export_elapsed
    );
}

// Note: To run all Phase 3 benchmarks together, use:
// cargo test --tests --no-default-features --release phase3 -- --ignored --nocapture
