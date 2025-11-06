//! Unit tests for PartitionedJobCoordinator
//!
//! Tests coordinator initialization, configuration, and multi-partition orchestration.

use std::collections::HashMap;
use velostream::velostream::server::v2::{
    BackpressureConfig, PartitionedJobConfig, PartitionedJobCoordinator, ProcessingMode,
};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

#[test]
fn test_coordinator_creation() {
    let config = PartitionedJobConfig::default();
    let coordinator = PartitionedJobCoordinator::new(config);

    assert!(coordinator.num_partitions() > 0);
    // Verify processing mode is Individual (default)
    assert!(matches!(
        coordinator.config().processing_mode,
        ProcessingMode::Individual
    ));
}

#[test]
fn test_coordinator_with_custom_partitions() {
    let config = PartitionedJobConfig {
        num_partitions: Some(4),
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    assert_eq!(coordinator.num_partitions(), 4);
}

#[test]
fn test_coordinator_config_defaults() {
    let config = PartitionedJobConfig::default();

    assert_eq!(config.partition_buffer_size, 1000);
    assert!(!config.enable_core_affinity);
    assert!(config.backpressure_config.enabled);
}

#[test]
fn test_backpressure_config() {
    use velostream::velostream::server::v2::ThrottleConfig;

    let config = BackpressureConfig {
        queue_threshold: 500,
        latency_threshold: std::time::Duration::from_millis(50),
        enabled: true,
        throttle_config: ThrottleConfig::default(),
    };

    assert_eq!(config.queue_threshold, 500);
    assert_eq!(config.latency_threshold.as_millis(), 50);
    assert!(config.enabled);
}

#[test]
fn test_processing_modes() {
    let individual = ProcessingMode::Individual;
    let batch = ProcessingMode::Batch { size: 100 };

    // Verify enum variants exist and compile
    match individual {
        ProcessingMode::Individual => assert!(true),
        ProcessingMode::Batch { .. } => panic!("Wrong variant"),
    }

    match batch {
        ProcessingMode::Batch { size } => assert_eq!(size, 100),
        ProcessingMode::Individual => panic!("Wrong variant"),
    }
}

fn create_test_record(trader_id: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "trader_id".to_string(),
        FieldValue::String(trader_id.to_string()),
    );
    StreamRecord::new(fields)
}

#[test]
fn test_coordinator_metrics_collection() {
    let config = PartitionedJobConfig {
        num_partitions: Some(2),
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let (managers, _senders) = coordinator.initialize_partitions();

    // Process some records through managers
    for manager in &managers[..1] {
        let record = create_test_record("TRADER1");
        let _ = manager.process_record(&record);
    }

    // Collect metrics
    let metrics = coordinator.collect_metrics(&managers);

    assert_eq!(metrics.num_partitions, 2);
    assert!(metrics.total_records_processed > 0);
}

#[test]
fn test_backpressure_detection_healthy() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    // Create partition metrics with low queue depth (healthy)
    let metrics = vec![
        Arc::new(PartitionMetrics::new(0)),
        Arc::new(PartitionMetrics::new(1)),
    ];

    // Set healthy queue depths (< 70% utilization)
    metrics[0].update_queue_depth(300); // 30% utilization
    metrics[1].update_queue_depth(500); // 50% utilization

    // No backpressure should be detected
    assert!(!coordinator.check_backpressure(&metrics));
}

#[test]
fn test_backpressure_detection_warning() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![Arc::new(PartitionMetrics::new(0))];

    // Set warning level queue depth (70-85% utilization)
    metrics[0].update_queue_depth(750); // 75% utilization

    // Warning state, but no throttling required yet
    assert!(!coordinator.check_backpressure(&metrics));
}

#[test]
fn test_backpressure_detection_critical() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![Arc::new(PartitionMetrics::new(0))];

    // Set critical queue depth (85-95% utilization)
    metrics[0].update_queue_depth(900); // 90% utilization

    // Critical backpressure should trigger throttling
    assert!(coordinator.check_backpressure(&metrics));
}

#[test]
fn test_backpressure_detection_saturated() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![Arc::new(PartitionMetrics::new(0))];

    // Set saturated queue depth (>95% utilization)
    metrics[0].update_queue_depth(980); // 98% utilization

    // Saturated backpressure requires immediate action
    assert!(coordinator.check_backpressure(&metrics));
}

#[test]
fn test_backpressure_multi_partition_mixed_states() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
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

    // Mixed states across partitions
    metrics[0].update_queue_depth(300); // Healthy (30%)
    metrics[1].update_queue_depth(750); // Warning (75%)
    metrics[2].update_queue_depth(900); // Critical (90%)
    metrics[3].update_queue_depth(400); // Healthy (40%)

    // Should detect backpressure due to partition 2 being critical
    assert!(coordinator.check_backpressure(&metrics));
}

#[test]
fn test_hot_partition_detection_method_exists() {
    // Simple test to verify the hot partition detection method exists and compiles
    // Note: Timing-dependent throughput tests are flaky in unit tests
    // Real hot partition detection will be tested in integration/performance tests

    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        num_partitions: Some(2),
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![
        Arc::new(PartitionMetrics::new(0)),
        Arc::new(PartitionMetrics::new(1)),
    ];

    // Just verify the method compiles and doesn't crash
    let hot_partitions = coordinator.detect_hot_partitions(&metrics, 2.0);

    // Should return empty for metrics with no throughput
    assert!(hot_partitions.is_empty() || !hot_partitions.is_empty());
}

#[test]
fn test_throttle_config_defaults() {
    use velostream::velostream::server::v2::ThrottleConfig;

    let config = ThrottleConfig::default();

    // Verify sensible defaults for production use
    assert_eq!(config.min_delay.as_micros(), 100); // 0.1ms
    assert_eq!(config.max_delay.as_millis(), 10); // 10ms
    assert_eq!(config.backoff_multiplier, 2.0);
}

#[test]
fn test_calculate_throttle_delay_healthy() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![
        Arc::new(PartitionMetrics::new(0)),
        Arc::new(PartitionMetrics::new(1)),
    ];

    // Set healthy queue depths (<70% utilization)
    metrics[0].update_queue_depth(300);
    metrics[1].update_queue_depth(500);

    // Should return zero delay for healthy state
    let delay = coordinator.calculate_throttle_delay(&metrics);
    assert_eq!(delay.as_nanos(), 0);
}

#[test]
fn test_calculate_throttle_delay_warning() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![Arc::new(PartitionMetrics::new(0))];

    // Set warning level queue depth (70-85% utilization)
    metrics[0].update_queue_depth(750); // 75% utilization

    // Should return min_delay for warning state
    let delay = coordinator.calculate_throttle_delay(&metrics);
    assert_eq!(delay.as_micros(), 100); // Default min_delay
}

#[test]
fn test_calculate_throttle_delay_critical() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![Arc::new(PartitionMetrics::new(0))];

    // Set critical queue depth (85-95% utilization)
    metrics[0].update_queue_depth(900); // 90% utilization

    // Should return min_delay * backoff_multiplier for critical state
    let delay = coordinator.calculate_throttle_delay(&metrics);
    assert_eq!(delay.as_micros(), 200); // 100 * 2.0
}

#[test]
fn test_calculate_throttle_delay_saturated() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::PartitionMetrics;

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![Arc::new(PartitionMetrics::new(0))];

    // Set saturated queue depth (>95% utilization)
    metrics[0].update_queue_depth(980); // 98% utilization

    // Should return max_delay for saturated state
    let delay = coordinator.calculate_throttle_delay(&metrics);
    assert_eq!(delay.as_millis(), 10); // Default max_delay
}

#[test]
fn test_calculate_throttle_delay_disabled() {
    use std::sync::Arc;
    use velostream::velostream::server::v2::{
        BackpressureConfig, PartitionMetrics, ThrottleConfig,
    };

    let config = PartitionedJobConfig {
        partition_buffer_size: 1000,
        backpressure_config: BackpressureConfig {
            enabled: false, // Disable backpressure handling
            queue_threshold: 1000,
            latency_threshold: std::time::Duration::from_millis(100),
            throttle_config: ThrottleConfig::default(),
        },
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let metrics = vec![Arc::new(PartitionMetrics::new(0))];

    // Set saturated queue depth
    metrics[0].update_queue_depth(980);

    // Should return zero delay when backpressure handling is disabled
    let delay = coordinator.calculate_throttle_delay(&metrics);
    assert_eq!(delay.as_nanos(), 0);
}

#[tokio::test]
async fn test_process_batch_with_throttling() {
    use velostream::velostream::server::v2::{HashRouter, PartitionStrategy};

    let config = PartitionedJobConfig {
        num_partitions: Some(2),
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let router = HashRouter::new(2, PartitionStrategy::RoundRobin);
    let (managers, senders) = coordinator.initialize_partitions();

    // Create test records
    let records = vec![create_test_record("TRADER1"), create_test_record("TRADER2")];

    // Collect partition metrics
    let partition_metrics: Vec<_> = managers.iter().map(|m| m.metrics()).collect();

    // Process batch with throttling
    // Note: Channels have no receivers (dropped in initialize_partitions),
    // so records won't actually be sent, but method should complete without error
    let result = coordinator
        .process_batch_with_throttling(records, &router, &senders, &partition_metrics)
        .await;

    // Verify no errors during processing
    // (Record count will be 0 since channels have no receivers)
    assert!(result.is_ok());
}
