//! Unit tests for PartitionedJobCoordinator
//!
//! Tests coordinator initialization, configuration, and multi-partition orchestration.

use std::collections::HashMap;
use velostream::velostream::server::v2::{
    BackpressureConfig, PartitionStrategy, PartitionedJobConfig, PartitionedJobCoordinator,
    ProcessingMode,
};
use velostream::velostream::sql::ast::Expr;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

#[test]
fn test_coordinator_creation() {
    let config = PartitionedJobConfig::default();
    let coordinator = PartitionedJobCoordinator::new(config);

    assert!(coordinator.num_partitions() > 0);
    assert_eq!(
        coordinator.config().processing_mode as u8,
        ProcessingMode::Individual as u8
    );
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
    let config = BackpressureConfig {
        queue_threshold: 500,
        latency_threshold: std::time::Duration::from_millis(50),
        enabled: true,
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
    assert!(metrics.total_records_processed >= 0);
}
