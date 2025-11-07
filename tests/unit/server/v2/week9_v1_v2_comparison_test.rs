//! Week 9: V1 vs V2 Architecture Comparison Tests
//!
//! This test suite validates the JobProcessor trait implementation for both
//! V1 (SimpleJobProcessor) and V2 (PartitionedJobCoordinator) architectures.
//!
//! ## Test Goals
//! 1. Verify both V1 and V2 implement the JobProcessor trait correctly
//! 2. Validate V1 single-partition behavior
//! 3. Validate V2 multi-partition behavior
//! 4. Establish baseline performance characteristics
//!
//! ## Architecture Overview
//! - **V1 (SimpleJobProcessor)**: Single-threaded, single partition
//! - **V2 (PartitionedJobCoordinator)**: Multi-threaded, N partitions

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::server::processors::JobProcessor;
use velostream::velostream::server::processors::SimpleJobProcessor;
use velostream::velostream::server::v2::PartitionedJobCoordinator;
use velostream::velostream::sql::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Helper: Create a test record with partition field
fn create_test_record(id: &str, partition: Option<i64>) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::String(id.to_string()));
    if let Some(p) = partition {
        fields.insert("__partition__".to_string(), FieldValue::Integer(p));
    }
    StreamRecord::new(fields)
}

/// Helper: Create test records in bulk
fn create_test_batch(count: usize, partition: Option<i64>) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| create_test_record(&format!("record_{}", i), partition))
        .collect()
}

/// Helper: Create execution engine with output channel
fn create_test_engine() -> (
    Arc<StreamExecutionEngine>,
    mpsc::UnboundedReceiver<StreamRecord>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let engine = StreamExecutionEngine::new(tx);
    (Arc::new(engine), rx)
}

// =============================================================================
// V1 ARCHITECTURE TESTS
// =============================================================================

#[test]
fn test_v1_processor_implements_trait() {
    let processor = SimpleJobProcessor::new(Default::default());

    assert_eq!(processor.processor_name(), "SimpleJobProcessor");
    assert_eq!(processor.processor_version(), "V1");
    assert_eq!(processor.num_partitions(), 1);
}

#[test]
fn test_v1_single_partition() {
    let processor = SimpleJobProcessor::new(Default::default());
    assert_eq!(
        processor.num_partitions(),
        1,
        "V1 should use single partition"
    );
}

#[tokio::test]
async fn test_v1_process_batch_interface() {
    let processor = SimpleJobProcessor::new(Default::default());
    let (engine, _rx) = create_test_engine();

    let records = create_test_batch(10, None);
    let result = processor.process_batch(records.clone(), engine).await;

    assert!(
        result.is_ok(),
        "V1 process_batch should succeed with valid input"
    );
    // V1 baseline: returns same records (pass-through for interface validation)
    let output = result.unwrap();
    assert_eq!(output.len(), 10, "V1 should return same number of records");
}

#[tokio::test]
async fn test_v1_empty_batch() {
    let processor = SimpleJobProcessor::new(Default::default());
    let (engine, _rx) = create_test_engine();

    let records = vec![];
    let result = processor.process_batch(records, engine).await;

    assert!(result.is_ok(), "V1 should handle empty batch");
    assert_eq!(
        result.unwrap().len(),
        0,
        "V1 should return empty for empty input"
    );
}

#[tokio::test]
async fn test_v1_large_batch() {
    let processor = SimpleJobProcessor::new(Default::default());
    let (engine, _rx) = create_test_engine();

    let records = create_test_batch(10_000, None);
    let result = processor.process_batch(records.clone(), engine).await;

    assert!(result.is_ok());
    assert_eq!(
        result.unwrap().len(),
        10_000,
        "V1 should handle large batches"
    );
}

// =============================================================================
// V2 ARCHITECTURE TESTS
// =============================================================================

#[test]
fn test_v2_processor_implements_trait() {
    let processor = PartitionedJobCoordinator::new(Default::default());

    assert_eq!(processor.processor_name(), "PartitionedJobCoordinator");
    assert_eq!(processor.processor_version(), "V2");
    assert!(
        processor.num_partitions() > 0,
        "V2 should have at least 1 partition"
    );
}

#[test]
fn test_v2_multi_partition_count() {
    let config = velostream::velostream::server::v2::PartitionedJobConfig {
        num_partitions: Some(8),
        ..Default::default()
    };
    let processor = PartitionedJobCoordinator::new(config);

    assert_eq!(
        processor.num_partitions(),
        8,
        "V2 should respect configured partition count"
    );
}

#[tokio::test]
async fn test_v2_process_batch_interface() {
    let processor = PartitionedJobCoordinator::new(Default::default());
    let (engine, _rx) = create_test_engine();

    let records = create_test_batch(100, Some(0));
    let result = processor.process_batch(records.clone(), engine).await;

    assert!(
        result.is_ok(),
        "V2 process_batch should succeed with valid input"
    );
    // V2 baseline: returns records (pass-through for interface validation)
    let output = result.unwrap();
    assert_eq!(
        output.len(),
        100,
        "V2 should return same number of records (baseline)"
    );
}

#[tokio::test]
async fn test_v2_empty_batch() {
    let processor = PartitionedJobCoordinator::new(Default::default());
    let (engine, _rx) = create_test_engine();

    let records = vec![];
    let result = processor.process_batch(records, engine).await;

    assert!(result.is_ok(), "V2 should handle empty batch");
    assert_eq!(
        result.unwrap().len(),
        0,
        "V2 should return empty for empty input"
    );
}

#[tokio::test]
async fn test_v2_records_with_partition_field() {
    let processor = PartitionedJobCoordinator::new(Default::default());
    let (engine, _rx) = create_test_engine();

    // Create records with partition field (Kafka-like data)
    let records: Vec<StreamRecord> = (0..50)
        .map(|i| create_test_record(&format!("record_{}", i), Some((i % 8) as i64)))
        .collect();

    let result = processor.process_batch(records, engine).await;
    assert!(
        result.is_ok(),
        "V2 should process records with partition field"
    );
}

// =============================================================================
// V1 vs V2 COMPARISON TESTS
// =============================================================================

#[tokio::test]
async fn test_v1_v2_both_accept_same_input() {
    let v1 = SimpleJobProcessor::new(Default::default());
    let v2 = PartitionedJobCoordinator::new(Default::default());

    let (engine_v1, _rx1) = create_test_engine();
    let (engine_v2, _rx2) = create_test_engine();

    let records = create_test_batch(50, None);

    let v1_result = v1.process_batch(records.clone(), engine_v1).await;
    let v2_result = v2.process_batch(records.clone(), engine_v2).await;

    assert!(v1_result.is_ok(), "V1 should process records");
    assert!(v2_result.is_ok(), "V2 should process records");
}

#[tokio::test]
async fn test_v1_v2_return_all_records() {
    let v1 = SimpleJobProcessor::new(Default::default());
    let v2 = PartitionedJobCoordinator::new(Default::default());

    let (engine_v1, _rx1) = create_test_engine();
    let (engine_v2, _rx2) = create_test_engine();

    let records = create_test_batch(100, None);
    let record_count = records.len();

    let v1_output = v1.process_batch(records.clone(), engine_v1).await.unwrap();
    let v2_output = v2.process_batch(records, engine_v2).await.unwrap();

    assert_eq!(
        v1_output.len(),
        record_count,
        "V1 should return all records (baseline)"
    );
    assert_eq!(
        v2_output.len(),
        record_count,
        "V2 should return all records (baseline)"
    );
}

#[test]
fn test_v1_v2_partition_count_difference() {
    let v1 = SimpleJobProcessor::new(Default::default());
    let v2_config = velostream::velostream::server::v2::PartitionedJobConfig {
        num_partitions: Some(8),
        ..Default::default()
    };
    let v2 = PartitionedJobCoordinator::new(v2_config);

    // This is the key architectural difference
    assert_eq!(v1.num_partitions(), 1, "V1 is single-partition");
    assert_eq!(v2.num_partitions(), 8, "V2 is multi-partition (8x)");
}

#[test]
fn test_v1_v2_processor_metadata() {
    let v1 = SimpleJobProcessor::new(Default::default());
    let v2 = PartitionedJobCoordinator::new(Default::default());

    assert_eq!(v1.processor_version(), "V1");
    assert_eq!(v2.processor_version(), "V2");

    assert_ne!(v1.processor_name(), v2.processor_name());
}

// =============================================================================
// TRAIT OBJECT TESTS
// =============================================================================

#[tokio::test]
async fn test_trait_object_v1() {
    let v1: Arc<dyn JobProcessor> = Arc::new(SimpleJobProcessor::new(Default::default()));
    let (engine, _rx) = create_test_engine();

    let records = create_test_batch(10, None);
    let result = v1.process_batch(records, engine).await;

    assert!(result.is_ok(), "V1 should work as trait object");
    assert_eq!(
        v1.num_partitions(),
        1,
        "V1 num_partitions through trait object should be 1"
    );
}

#[tokio::test]
async fn test_trait_object_v2() {
    let v2: Arc<dyn JobProcessor> = Arc::new(PartitionedJobCoordinator::new(Default::default()));
    let (engine, _rx) = create_test_engine();

    let records = create_test_batch(10, None);
    let result = v2.process_batch(records, engine).await;

    assert!(result.is_ok(), "V2 should work as trait object");
    assert!(
        v2.num_partitions() > 0,
        "V2 num_partitions through trait object should be > 0"
    );
}

#[tokio::test]
async fn test_trait_object_interchangeable() {
    // This demonstrates the key design: both can be used interchangeably
    let processors: Vec<Arc<dyn JobProcessor>> = vec![
        Arc::new(SimpleJobProcessor::new(Default::default())),
        Arc::new(PartitionedJobCoordinator::new(Default::default())),
    ];

    for processor in processors {
        let (engine, _rx) = create_test_engine();
        let records = create_test_batch(10, None);

        let result = processor.process_batch(records, engine).await;
        assert!(
            result.is_ok(),
            "All processors should handle batches: {}",
            processor.processor_version()
        );
        assert!(
            processor.num_partitions() > 0,
            "All processors should have >= 1 partition"
        );
    }
}

// =============================================================================
// WEEK 9 BASELINE CHARACTERISTICS
// =============================================================================

#[test]
fn test_baseline_characteristic_v1_single_threaded() {
    let v1 = SimpleJobProcessor::new(Default::default());
    assert_eq!(
        v1.num_partitions(),
        1,
        "V1 is single-threaded (1 partition)"
    );
}

#[test]
fn test_baseline_characteristic_v2_multi_threaded() {
    let config = velostream::velostream::server::v2::PartitionedJobConfig {
        num_partitions: Some(8),
        ..Default::default()
    };
    let v2 = PartitionedJobCoordinator::new(config);
    assert_eq!(
        v2.num_partitions(),
        8,
        "V2 is multi-threaded (8 partitions expected)"
    );
}

#[test]
fn test_baseline_characteristic_names() {
    let v1 = SimpleJobProcessor::new(Default::default());
    let v2 = PartitionedJobCoordinator::new(Default::default());

    assert!(v1.processor_name().contains("Simple"));
    assert!(v2.processor_name().contains("Coordinator"));
}

#[tokio::test]
async fn test_week9_baseline_both_process_records() {
    // This test validates the minimum Week 9 requirement:
    // Both V1 and V2 must successfully implement JobProcessor

    let v1 = SimpleJobProcessor::new(Default::default());
    let v2_config = velostream::velostream::server::v2::PartitionedJobConfig {
        num_partitions: Some(4),
        ..Default::default()
    };
    let v2 = PartitionedJobCoordinator::new(v2_config);

    let (engine_v1, _rx1) = create_test_engine();
    let (engine_v2, _rx2) = create_test_engine();

    let batch = create_test_batch(1000, Some(0));

    // Both should successfully process a 1000-record batch
    assert!(
        v1.process_batch(batch.clone(), engine_v1).await.is_ok(),
        "V1 must process 1000-record batch"
    );
    assert!(
        v2.process_batch(batch, engine_v2).await.is_ok(),
        "V2 must process 1000-record batch"
    );
}
