//! V2 Partition Receiver Integration Test
//!
//! Validates that the Phase 6.0 fix actually works:
//! - Partition receivers process records (not silent drain)
//! - Watermarks are updated per partition
//! - Metrics are tracked correctly
//! - Late records are handled per strategy

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use velostream::velostream::server::v2::{
    PartitionedJobConfig, PartitionedJobCoordinator, ProcessingMode,
};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Create a test record with event_time for watermark testing
/// event_time_offset: seconds after epoch
fn create_record_with_time(trader_id: &str, event_time_offset_secs: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "trader_id".to_string(),
        FieldValue::String(trader_id.to_string()),
    );
    fields.insert("price".to_string(), FieldValue::Float(100.0));
    fields.insert("quantity".to_string(), FieldValue::Integer(10));

    let event_time = Utc::now() + ChronoDuration::seconds(event_time_offset_secs);
    let mut record = StreamRecord::new(fields);
    record.event_time = Some(event_time);
    record
}

/// Verify partition receiver actually processes records (not silent drain)
#[tokio::test]
async fn test_partition_receiver_processes_records() {
    let config = PartitionedJobConfig {
        num_partitions: Some(2),
        processing_mode: ProcessingMode::Individual,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let (managers, senders) = coordinator.initialize_partitions();

    // Give tokio a moment to spawn the partition receiver tasks
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Send a record to partition 0
    let record = create_record_with_time("TRADER1", 1000);
    senders[0]
        .send(record)
        .await
        .expect("Failed to send record to partition");

    // Give partition receiver time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // VERIFY: Partition metrics should show the record was processed
    let metrics = managers[0].metrics();
    let total_processed = metrics.total_records_processed();

    assert!(
        total_processed > 0,
        "Partition should have processed at least 1 record. Got: {}",
        total_processed
    );
}

/// Verify watermark is updated when records are processed
#[tokio::test]
async fn test_partition_receiver_updates_watermark() {
    let config = PartitionedJobConfig {
        num_partitions: Some(2),
        processing_mode: ProcessingMode::Individual,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let (managers, senders) = coordinator.initialize_partitions();

    // Give tokio a moment to spawn tasks
    tokio::time::sleep(Duration::from_millis(10)).await;

    let manager = &managers[0];
    let watermark_mgr = manager.watermark_manager();

    // Verify initial watermark is not set (-1 = not initialized)
    let initial_watermark = watermark_mgr.current_watermark();
    assert!(
        initial_watermark.is_none(),
        "Initial watermark should be None"
    );

    // Send record with event_time = 5 seconds in future
    let record = create_record_with_time("TRADER1", 5);
    senders[0]
        .send(record)
        .await
        .expect("Failed to send record");

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    // VERIFY: Watermark should be updated (not None)
    let updated_watermark = watermark_mgr.current_watermark();
    assert!(
        updated_watermark.is_some(),
        "Watermark should be updated after processing record"
    );
}

/// Verify multiple records increase metrics correctly
#[tokio::test]
async fn test_partition_receiver_tracks_multiple_records() {
    let config = PartitionedJobConfig {
        num_partitions: Some(1),
        processing_mode: ProcessingMode::Individual,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let (managers, senders) = coordinator.initialize_partitions();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let manager = &managers[0];
    let metrics = manager.metrics();

    // Send 10 records with increasing offsets
    let mut last_record_time = None;
    for i in 0..10 {
        let record = create_record_with_time(&format!("TRADER{}", i), i as i64);
        last_record_time = record.event_time;
        senders[0]
            .send(record)
            .await
            .expect("Failed to send record");
    }

    // Wait for all records to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    // VERIFY: All 10 records processed
    let total_processed = metrics.total_records_processed();
    assert_eq!(
        total_processed, 10,
        "Should have processed 10 records. Got: {}",
        total_processed
    );

    // VERIFY: Watermark is set (non-None) after processing records
    let watermark = manager.watermark_manager().current_watermark();
    assert!(
        watermark.is_some(),
        "Watermark should be set after processing records"
    );
}

/// Verify late records are handled (ProcessWithWarning strategy is default)
#[tokio::test]
async fn test_partition_receiver_handles_late_records() {
    let config = PartitionedJobConfig {
        num_partitions: Some(1),
        processing_mode: ProcessingMode::Individual,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let (managers, senders) = coordinator.initialize_partitions();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let manager = &managers[0];
    let watermark_mgr = manager.watermark_manager();

    // Send record with event_time = 5 seconds (sets watermark)
    let record = create_record_with_time("TRADER1", 5);
    let first_time = record.event_time;
    senders[0]
        .send(record)
        .await
        .expect("Failed to send record");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send record with earlier event_time (creates out-of-order scenario)
    // With default ProcessWithWarning strategy, this should still be processed
    let earlier_record = create_record_with_time("TRADER2", 0); // now
    senders[0]
        .send(earlier_record)
        .await
        .expect("Failed to send earlier record");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // VERIFY: Watermark is set after processing records
    let watermark = watermark_mgr.current_watermark();
    assert!(
        watermark.is_some(),
        "Watermark should be set after processing records"
    );

    // VERIFY: Both records processed (ProcessWithWarning strategy processes late records)
    let processed = manager.metrics().total_records_processed();
    assert_eq!(
        processed, 2,
        "Should have processed 2 records with ProcessWithWarning strategy. Got: {}",
        processed
    );
}

/// Verify partition isolation - records sent to specific partitions are processed independently
#[tokio::test]
async fn test_partition_isolation_per_key() {
    let config = PartitionedJobConfig {
        num_partitions: Some(2),
        processing_mode: ProcessingMode::Individual,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let (managers, senders) = coordinator.initialize_partitions();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Send 5 records to partition 0
    let mut last_time_0 = None;
    for i in 0..5 {
        let record = create_record_with_time("TRADER1", i as i64);
        last_time_0 = record.event_time;
        senders[0]
            .send(record)
            .await
            .expect("Failed to send to partition 0");
    }

    // Send 3 records to partition 1
    let mut last_time_1 = None;
    for i in 0..3 {
        let record = create_record_with_time("TRADER2", 10 + i as i64);
        last_time_1 = record.event_time;
        senders[1]
            .send(record)
            .await
            .expect("Failed to send to partition 1");
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // VERIFY: Partition 0 processed 5 records
    let processed_0 = managers[0].metrics().total_records_processed();
    assert_eq!(
        processed_0, 5,
        "Partition 0 should have 5 records. Got: {}",
        processed_0
    );

    // VERIFY: Partition 1 processed 3 records
    let processed_1 = managers[1].metrics().total_records_processed();
    assert_eq!(
        processed_1, 3,
        "Partition 1 should have 3 records. Got: {}",
        processed_1
    );

    // VERIFY: Each partition has set its watermark
    let watermark_0 = managers[0].watermark_manager().current_watermark();
    assert!(
        watermark_0.is_some(),
        "Partition 0 should have set its watermark"
    );

    let watermark_1 = managers[1].watermark_manager().current_watermark();
    assert!(
        watermark_1.is_some(),
        "Partition 1 should have set its watermark"
    );
}

/// Verify receiver task shuts down gracefully when channel closes
#[tokio::test]
async fn test_partition_receiver_shutdown() {
    let config = PartitionedJobConfig {
        num_partitions: Some(1),
        processing_mode: ProcessingMode::Individual,
        ..Default::default()
    };
    let coordinator = PartitionedJobCoordinator::new(config);

    let (managers, senders) = coordinator.initialize_partitions();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Send a record
    let record = create_record_with_time("TRADER1", 1000);
    senders[0].send(record).await.ok();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let processed_before = managers[0].metrics().total_records_processed();
    assert_eq!(processed_before, 1, "Should have 1 processed record");

    // Drop sender channel (receiver should shut down)
    drop(senders);

    // Wait for shutdown
    tokio::time::sleep(Duration::from_millis(100)).await;

    // VERIFY: Metrics don't change after shutdown
    let processed_after = managers[0].metrics().total_records_processed();
    assert_eq!(
        processed_after, processed_before,
        "Metrics should not change after shutdown"
    );
}
