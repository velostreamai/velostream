//! Unit tests for PartitionStateManager
//!
//! Tests per-partition state management, record processing, and metrics integration.

use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::server::v2::{PartitionMetrics, PartitionStateManager};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "trader_id".to_string(),
        FieldValue::String("TRADER1".to_string()),
    );
    StreamRecord::new(fields)
}

#[test]
fn test_manager_creation() {
    let manager = PartitionStateManager::new(0);
    assert_eq!(manager.partition_id(), 0);
    assert_eq!(manager.total_records_processed(), 0);
}

#[test]
fn test_manager_with_custom_metrics() {
    let metrics = Arc::new(PartitionMetrics::new(2));
    let manager = PartitionStateManager::with_metrics(2, metrics.clone());

    assert_eq!(manager.partition_id(), 2);
    assert_eq!(manager.metrics().partition_id(), 2);
}

#[test]
fn test_process_single_record() {
    let manager = PartitionStateManager::new(0);
    let record = create_test_record();

    let result = manager.process_record(&record);
    assert!(result.is_ok());
    assert_eq!(manager.total_records_processed(), 1);
}

#[test]
fn test_process_batch() {
    let manager = PartitionStateManager::new(0);
    let records: Vec<StreamRecord> = (0..100).map(|_| create_test_record()).collect();

    let result = manager.process_batch(&records);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 100);
    assert_eq!(manager.total_records_processed(), 100);
}

#[test]
fn test_metrics_integration() {
    let manager = PartitionStateManager::new(0);
    let records: Vec<StreamRecord> = (0..1000).map(|_| create_test_record()).collect();

    manager.process_batch(&records).unwrap();

    let metrics_snapshot = manager.metrics().snapshot();
    assert_eq!(metrics_snapshot.records_processed, 1000);
    assert_eq!(metrics_snapshot.partition_id, 0);
}

#[test]
fn test_backpressure_detection() {
    let manager = PartitionStateManager::new(0);

    // Initially no backpressure
    assert!(!manager.has_backpressure(1000, std::time::Duration::from_millis(100)));

    // Simulate high queue depth
    manager.metrics().update_queue_depth(1500);
    assert!(manager.has_backpressure(1000, std::time::Duration::from_millis(100)));
}

#[test]
fn test_reset_metrics() {
    let manager = PartitionStateManager::new(0);
    let records: Vec<StreamRecord> = (0..100).map(|_| create_test_record()).collect();

    manager.process_batch(&records).unwrap();
    assert_eq!(manager.total_records_processed(), 100);

    manager.reset_metrics();
    assert_eq!(manager.total_records_processed(), 0);
}

#[test]
fn test_cpu_affinity_no_panic() {
    let manager = PartitionStateManager::new(0);

    // Should not panic on any platform
    let result = manager.set_cpu_affinity(0);
    assert!(result.is_ok());
}
