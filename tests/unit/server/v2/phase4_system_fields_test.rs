//! FR-082 Phase 4: System Fields & Watermarks Integration Tests
//!
//! Tests complete Phase 4 functionality:
//! - System field routing (_TIMESTAMP, _PARTITION, _OFFSET, _EVENT_TIME)
//! - WatermarkManager integration with PartitionStateManager
//! - Late record detection and handling
//! - Window field injection (_WINDOW_START, _WINDOW_END)

use chrono::{Duration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::server::v2::{
    extract_window_fields, has_window_fields, inject_window_fields, PartitionStateManager,
    WatermarkConfig, WatermarkManager, WatermarkStrategy,
};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Test that PartitionStateManager properly integrates WatermarkManager
#[test]
fn test_partition_manager_watermark_integration() {
    let manager = PartitionStateManager::new(0);

    // Verify watermark_manager is accessible
    let watermark_mgr = manager.watermark_manager();
    assert_eq!(watermark_mgr.partition_id(), 0);
    assert!(watermark_mgr.current_watermark().is_none());
}

/// Test event-time processing with watermark updates
#[test]
fn test_event_time_watermark_updates() {
    let manager = PartitionStateManager::new(0);

    let now = Utc::now();
    let mut record = StreamRecord::new(HashMap::new());
    record.event_time = Some(now);

    // Process record - should update watermark
    let result = manager.process_record(&record);
    assert!(result.is_ok());

    // Verify watermark was updated
    let watermark = manager.watermark_manager().current_watermark();
    assert!(watermark.is_some());
}

/// Test late record detection with ProcessWithWarning strategy
#[test]
fn test_late_record_process_with_warning() {
    let config = WatermarkConfig {
        max_lateness: std::time::Duration::from_secs(60),
        strategy: WatermarkStrategy::ProcessWithWarning,
        ..Default::default()
    };
    let watermark_mgr = Arc::new(WatermarkManager::new(0, config));
    let manager = PartitionStateManager::with_watermark_manager(0, watermark_mgr.clone());

    let now = Utc::now();

    // Process record to establish watermark
    let mut record1 = StreamRecord::new(HashMap::new());
    record1.event_time = Some(now);
    assert!(manager.process_record(&record1).is_ok());

    // Process late record (90 seconds in the past)
    let mut late_record = StreamRecord::new(HashMap::new());
    late_record.event_time = Some(now - Duration::seconds(90));

    // Should process with warning (not dropped)
    let result = manager.process_record(&late_record);
    assert!(result.is_ok(), "Late record should be processed with warning");

    // Verify late record was counted
    let metrics = watermark_mgr.metrics();
    assert_eq!(metrics.late_records_count, 1);
    assert_eq!(metrics.dropped_records_count, 0);
}

/// Test late record dropping with Drop strategy
#[test]
fn test_late_record_drop_strategy() {
    let config = WatermarkConfig {
        max_lateness: std::time::Duration::from_secs(60),
        strategy: WatermarkStrategy::Drop,
        ..Default::default()
    };
    let watermark_mgr = Arc::new(WatermarkManager::new(0, config));
    let manager = PartitionStateManager::with_watermark_manager(0, watermark_mgr.clone());

    let now = Utc::now();

    // Process record to establish watermark
    let mut record1 = StreamRecord::new(HashMap::new());
    record1.event_time = Some(now);
    assert!(manager.process_record(&record1).is_ok());

    // Process late record (90 seconds in the past)
    let mut late_record = StreamRecord::new(HashMap::new());
    late_record.event_time = Some(now - Duration::seconds(90));

    // Should be dropped
    let result = manager.process_record(&late_record);
    assert!(result.is_err(), "Late record should be dropped");

    // Verify late record was counted and dropped
    let metrics = watermark_mgr.metrics();
    assert_eq!(metrics.late_records_count, 1);
    assert_eq!(metrics.dropped_records_count, 1);
}

/// Test late record processing with ProcessAll strategy
#[test]
fn test_late_record_process_all_strategy() {
    let config = WatermarkConfig {
        max_lateness: std::time::Duration::from_secs(60),
        strategy: WatermarkStrategy::ProcessAll,
        ..Default::default()
    };
    let watermark_mgr = Arc::new(WatermarkManager::new(0, config));
    let manager = PartitionStateManager::with_watermark_manager(0, watermark_mgr.clone());

    let now = Utc::now();

    // Process record to establish watermark
    let mut record1 = StreamRecord::new(HashMap::new());
    record1.event_time = Some(now);
    assert!(manager.process_record(&record1).is_ok());

    // Process late record (90 seconds in the past)
    let mut late_record = StreamRecord::new(HashMap::new());
    late_record.event_time = Some(now - Duration::seconds(90));

    // Should be processed silently (no error)
    let result = manager.process_record(&late_record);
    assert!(result.is_ok(), "Late record should be processed silently");

    // ProcessAll strategy still counts late records in metrics, but processes them without warning
    let metrics = watermark_mgr.metrics();
    assert_eq!(metrics.late_records_count, 1); // Late records are counted
    assert_eq!(metrics.dropped_records_count, 0); // But never dropped
}

/// Test batch processing with late records
#[test]
fn test_batch_processing_with_late_records() {
    let config = WatermarkConfig {
        max_lateness: std::time::Duration::from_secs(60),
        strategy: WatermarkStrategy::Drop,
        ..Default::default()
    };
    let watermark_mgr = Arc::new(WatermarkManager::new(0, config));
    let manager = PartitionStateManager::with_watermark_manager(0, watermark_mgr.clone());

    let now = Utc::now();

    // Create batch with mix of on-time and late records
    let mut records = Vec::new();

    // Record 1: On-time (establishes watermark)
    let mut record1 = StreamRecord::new(HashMap::new());
    record1.event_time = Some(now);
    records.push(record1);

    // Record 2: On-time
    let mut record2 = StreamRecord::new(HashMap::new());
    record2.event_time = Some(now + Duration::seconds(10));
    records.push(record2);

    // Record 3: Late (should be dropped)
    let mut record3 = StreamRecord::new(HashMap::new());
    record3.event_time = Some(now - Duration::seconds(90));
    records.push(record3);

    // Record 4: On-time
    let mut record4 = StreamRecord::new(HashMap::new());
    record4.event_time = Some(now + Duration::seconds(20));
    records.push(record4);

    // Process batch
    let result = manager.process_batch(&records);
    assert!(result.is_ok());

    // Should have processed 3 records (1 dropped)
    assert_eq!(result.unwrap(), 3);

    // Verify metrics
    let metrics = watermark_mgr.metrics();
    assert_eq!(metrics.late_records_count, 1);
    assert_eq!(metrics.dropped_records_count, 1);
}

/// Test window field injection
#[test]
fn test_window_field_injection() {
    let mut record = StreamRecord::new(HashMap::new());
    let window_start = Utc::now();
    let window_end = window_start + Duration::minutes(5);

    // Initially no window fields
    assert!(!has_window_fields(&record));

    // Inject window fields
    inject_window_fields(&mut record, window_start, window_end);

    // Verify window fields present
    assert!(has_window_fields(&record));

    // Extract and verify values
    let (extracted_start, extracted_end) = extract_window_fields(&record);
    assert!(extracted_start.is_some());
    assert!(extracted_end.is_some());

    // Verify millisecond precision
    assert_eq!(
        extracted_start.unwrap().timestamp_millis(),
        window_start.timestamp_millis()
    );
    assert_eq!(
        extracted_end.unwrap().timestamp_millis(),
        window_end.timestamp_millis()
    );
}

/// Test window field injection with SQL field access
#[test]
fn test_window_fields_in_record() {
    let mut record = StreamRecord::new(HashMap::new());
    let window_start = Utc::now();
    let window_end = window_start + Duration::minutes(5);

    inject_window_fields(&mut record, window_start, window_end);

    // Verify fields are accessible as FieldValue::Integer
    match record.fields.get("_WINDOW_START") {
        Some(FieldValue::Integer(millis)) => {
            assert_eq!(*millis, window_start.timestamp_millis());
        }
        _ => panic!("_WINDOW_START should be FieldValue::Integer"),
    }

    match record.fields.get("_WINDOW_END") {
        Some(FieldValue::Integer(millis)) => {
            assert_eq!(*millis, window_end.timestamp_millis());
        }
        _ => panic!("_WINDOW_END should be FieldValue::Integer"),
    }
}

/// Test watermark monotonicity (should never go backwards)
#[test]
fn test_watermark_monotonicity() {
    let manager = PartitionStateManager::new(0);

    let now = Utc::now();

    // Process record at time T
    let mut record1 = StreamRecord::new(HashMap::new());
    record1.event_time = Some(now);
    manager.process_record(&record1).unwrap();

    let watermark1 = manager
        .watermark_manager()
        .current_watermark()
        .unwrap()
        .timestamp_millis();

    // Process record at time T - 10s (out of order, but not late enough to drop)
    let mut record2 = StreamRecord::new(HashMap::new());
    record2.event_time = Some(now - Duration::seconds(10));
    manager.process_record(&record2).unwrap();

    let watermark2 = manager
        .watermark_manager()
        .current_watermark()
        .unwrap()
        .timestamp_millis();

    // Watermark should not go backwards
    assert!(
        watermark2 >= watermark1,
        "Watermark should be monotonic (never decrease)"
    );
}

/// Test records without event_time (should not affect watermark)
#[test]
fn test_records_without_event_time() {
    let manager = PartitionStateManager::new(0);

    // Process record without event_time
    let record = StreamRecord::new(HashMap::new());
    let result = manager.process_record(&record);

    // Should process successfully
    assert!(result.is_ok());

    // Watermark should remain unset
    assert!(manager.watermark_manager().current_watermark().is_none());
}

/// Test custom watermark configuration
#[test]
fn test_custom_watermark_configuration() {
    let config = WatermarkConfig {
        max_lateness: std::time::Duration::from_secs(120), // 2 minutes
        strategy: WatermarkStrategy::ProcessWithWarning,
        min_advance: std::time::Duration::from_millis(200),
        idle_timeout: Some(std::time::Duration::from_secs(60)),
    };

    let watermark_mgr = Arc::new(WatermarkManager::new(0, config));
    let manager = PartitionStateManager::with_watermark_manager(0, watermark_mgr);

    // Verify custom configuration is used
    let now = Utc::now();
    let mut record = StreamRecord::new(HashMap::new());
    record.event_time = Some(now);

    manager.process_record(&record).unwrap();

    // Record that's 90 seconds late should NOT be late (max_lateness = 120s)
    let mut almost_late_record = StreamRecord::new(HashMap::new());
    almost_late_record.event_time = Some(now - Duration::seconds(90));

    let result = manager.process_record(&almost_late_record);
    assert!(result.is_ok(), "Record within 2 minute window should not be late");
}
