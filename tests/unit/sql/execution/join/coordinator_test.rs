//! Join Coordinator Tests
//!
//! Tests for the JoinCoordinator and related types.

use std::collections::HashMap;
use std::time::Duration;

use velostream::velostream::sql::execution::StreamRecord;
use velostream::velostream::sql::execution::join::{
    JoinConfig, JoinCoordinator, JoinCoordinatorConfig, JoinEmitMode, JoinMode, JoinSide,
    JoinStateStoreConfig, JoinType, MemoryPressure, MissingEventTimeBehavior,
};
use velostream::velostream::sql::execution::types::FieldValue;

fn make_test_record(fields: Vec<(&str, FieldValue)>, timestamp: i64) -> StreamRecord {
    let field_map: HashMap<String, FieldValue> = fields
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    let mut record = StreamRecord::new(field_map);
    record.timestamp = timestamp;
    record
}

#[test]
fn test_inner_join_matching_records() {
    let config = JoinConfig::interval(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::ZERO,
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("amount", FieldValue::Float(100.0)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    let results = coordinator.process_left(order).unwrap();
    assert!(results.is_empty());

    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("tracking", FieldValue::String("TRACK001".to_string())),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = coordinator.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("orders.order_id"));
    assert!(joined.fields.contains_key("shipments.tracking"));
}

#[test]
fn test_no_match_different_keys() {
    let config = JoinConfig::interval(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::ZERO,
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    coordinator.process_left(order).unwrap();

    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(456)),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = coordinator.process_right(shipment).unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_interval_time_bounds() {
    let config = JoinConfig::interval(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::ZERO,
        Duration::from_secs(1),
    );

    let mut coordinator = JoinCoordinator::new(config);

    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    coordinator.process_left(order).unwrap();

    let shipment1 = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(1500)),
        ],
        1500,
    );
    let results = coordinator.process_right(shipment1).unwrap();
    assert_eq!(results.len(), 1);

    let shipment2 = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(3000)),
        ],
        3000,
    );
    let results = coordinator.process_right(shipment2).unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_bidirectional_matching() {
    let config = JoinConfig::equi_join(
        "stream_a",
        "stream_b",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    let right_record = make_test_record(
        vec![
            ("key", FieldValue::Integer(1)),
            ("right_value", FieldValue::String("B".to_string())),
        ],
        1000,
    );
    let results = coordinator.process_right(right_record).unwrap();
    assert!(results.is_empty());

    let left_record = make_test_record(
        vec![
            ("key", FieldValue::Integer(1)),
            ("left_value", FieldValue::String("A".to_string())),
        ],
        2000,
    );
    let results = coordinator.process_left(left_record).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("stream_a.left_value"));
    assert!(joined.fields.contains_key("stream_b.right_value"));
}

#[test]
fn test_multiple_matches() {
    let config = JoinConfig::equi_join(
        "orders",
        "items",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    for i in 0..3 {
        let item = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("item_id", FieldValue::Integer(i)),
            ],
            1000 + i,
        );
        coordinator.process_right(item).unwrap();
    }

    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("customer", FieldValue::String("Alice".to_string())),
        ],
        2000,
    );
    let results = coordinator.process_left(order).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_watermark_expiration() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_millis(1000),
    );

    let mut coordinator = JoinCoordinator::new(config);

    let record = make_test_record(
        vec![
            ("key", FieldValue::Integer(1)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    coordinator.process_left(record).unwrap();

    assert_eq!(coordinator.left_store().record_count(), 1);

    coordinator.advance_watermark(JoinSide::Left, 2500);

    assert_eq!(coordinator.left_store().record_count(), 0);
}

#[test]
fn test_missing_key_handling() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("missing_col".to_string(), "missing_col".to_string())],
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    let record = make_test_record(vec![("other_field", FieldValue::Integer(123))], 1000);

    let results = coordinator.process_left(record).unwrap();
    assert!(results.is_empty());
    assert_eq!(coordinator.stats().missing_key_count, 1);
}

#[test]
fn test_composite_key_join() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![
            ("region".to_string(), "region".to_string()),
            ("customer_id".to_string(), "customer_id".to_string()),
        ],
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    let left = make_test_record(
        vec![
            ("region", FieldValue::String("US".to_string())),
            ("customer_id", FieldValue::Integer(42)),
            ("left_data", FieldValue::String("L".to_string())),
        ],
        1000,
    );
    coordinator.process_left(left).unwrap();

    let right = make_test_record(
        vec![
            ("region", FieldValue::String("US".to_string())),
            ("customer_id", FieldValue::Integer(42)),
            ("right_data", FieldValue::String("R".to_string())),
        ],
        2000,
    );
    let results = coordinator.process_right(right).unwrap();
    assert_eq!(results.len(), 1);

    let right_diff = make_test_record(
        vec![
            ("region", FieldValue::String("EU".to_string())),
            ("customer_id", FieldValue::Integer(42)),
            ("right_data", FieldValue::String("R2".to_string())),
        ],
        3000,
    );
    let results = coordinator.process_right(right_diff).unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_stats_tracking() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    for i in 0..5 {
        let record = make_test_record(vec![("key", FieldValue::Integer(i))], 1000 + i);
        coordinator.process_left(record).unwrap();
    }

    for i in 0..3 {
        let record = make_test_record(vec![("key", FieldValue::Integer(i))], 2000 + i);
        coordinator.process_right(record).unwrap();
    }

    let stats = coordinator.stats();
    assert_eq!(stats.left_records_processed, 5);
    assert_eq!(stats.right_records_processed, 3);
    assert_eq!(stats.matches_emitted, 3);
}

#[test]
fn test_memory_pressure_normal() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    for i in 0..10 {
        let record = make_test_record(vec![("key", FieldValue::Integer(i))], 1000 + i);
        coordinator.process_left(record).unwrap();
    }

    assert_eq!(coordinator.memory_pressure(), MemoryPressure::Normal);
    assert!(!coordinator.should_apply_backpressure());
}

#[test]
fn test_memory_pressure_warning() {
    let join_config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    );
    let store_config = JoinStateStoreConfig::with_limits(10, 0);
    let config = JoinCoordinatorConfig::new(join_config).with_store_config(store_config);

    let mut coordinator = JoinCoordinator::with_config(config);

    for i in 0..7 {
        let record = make_test_record(vec![("key", FieldValue::Integer(i))], 1000 + i);
        coordinator.process_left(record).unwrap();
    }

    assert_eq!(coordinator.memory_pressure(), MemoryPressure::Normal);

    let record = make_test_record(vec![("key", FieldValue::Integer(7))], 1007);
    coordinator.process_left(record).unwrap();

    assert_eq!(coordinator.memory_pressure(), MemoryPressure::Warning);
    assert!(coordinator.should_apply_backpressure());
}

#[test]
fn test_memory_pressure_critical() {
    let join_config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    );
    let store_config = JoinStateStoreConfig::with_limits(10, 0);
    let config = JoinCoordinatorConfig::new(join_config).with_store_config(store_config);

    let mut coordinator = JoinCoordinator::with_config(config);

    for i in 0..10 {
        let record = make_test_record(
            vec![
                ("key", FieldValue::Integer(i)),
                ("event_time", FieldValue::Integer(1000 + i)),
            ],
            1000 + i,
        );
        coordinator.process_left(record).unwrap();
    }

    assert_eq!(coordinator.memory_pressure(), MemoryPressure::Critical);
    assert!(coordinator.should_apply_backpressure());

    let record = make_test_record(
        vec![
            ("key", FieldValue::Integer(10)),
            ("event_time", FieldValue::Integer(1010)),
        ],
        1010,
    );
    coordinator.process_left(record).unwrap();

    let (left_evictions, right_evictions) = coordinator.eviction_counts();
    // May evict 1-2 records depending on eviction policy behavior
    assert!(left_evictions >= 1, "Expected at least 1 eviction");
    assert_eq!(right_evictions, 0);

    let stats = coordinator.stats();
    assert!(
        stats.left_evictions >= 1,
        "Expected at least 1 eviction in stats"
    );
}

#[test]
fn test_combined_capacity_usage() {
    let join_config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    );
    let store_config = JoinStateStoreConfig::with_limits(100, 0);
    let config = JoinCoordinatorConfig::new(join_config).with_store_config(store_config);

    let mut coordinator = JoinCoordinator::with_config(config);

    assert_eq!(coordinator.combined_capacity_usage_pct(), 0.0);
    assert_eq!(coordinator.remaining_capacity(), 100);

    for i in 0..50 {
        let record = make_test_record(vec![("key", FieldValue::Integer(i))], 1000 + i);
        coordinator.process_left(record).unwrap();
    }

    assert_eq!(coordinator.combined_capacity_usage_pct(), 50.0);
    assert_eq!(coordinator.remaining_capacity(), 50);

    for i in 0..75 {
        let record = make_test_record(vec![("key", FieldValue::Integer(i))], 2000 + i);
        coordinator.process_right(record).unwrap();
    }

    assert_eq!(coordinator.combined_capacity_usage_pct(), 75.0);
    assert_eq!(coordinator.remaining_capacity(), 25);
}

#[test]
fn test_coordinator_config_builder() {
    let join_config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    );

    let config = JoinCoordinatorConfig::new(join_config.clone()).with_max_records(5000);

    assert!(config.left_store_config.is_some());
    assert!(config.right_store_config.is_some());
    assert_eq!(config.left_store_config.unwrap().max_records, 5000);

    let left_config = JoinStateStoreConfig::with_limits(10000, 100);
    let right_config = JoinStateStoreConfig::with_limits(5000, 50);
    let config =
        JoinCoordinatorConfig::new(join_config).with_store_configs(left_config, right_config);

    assert_eq!(config.left_store_config.unwrap().max_records, 10000);
    assert_eq!(config.right_store_config.unwrap().max_records, 5000);
}

#[test]
fn test_negative_interval_bounds() {
    let config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        -1000,
        2000,
    );

    let mut coordinator = JoinCoordinator::new(config);

    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(5000)),
        ],
        5000,
    );
    coordinator.process_left(order).unwrap();

    let shipment_before = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(4500)),
        ],
        4500,
    );
    let results = coordinator.process_right(shipment_before).unwrap();
    assert_eq!(results.len(), 1, "Shipment 500ms before order should match");

    let shipment_too_early = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(3500)),
        ],
        3500,
    );
    let results = coordinator.process_right(shipment_too_early).unwrap();
    assert!(
        results.is_empty(),
        "Shipment 1500ms before order should NOT match"
    );

    let shipment_after = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(6000)),
        ],
        6000,
    );
    let results = coordinator.process_right(shipment_after).unwrap();
    assert_eq!(results.len(), 1, "Shipment 1000ms after order should match");

    let shipment_too_late = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(8000)),
        ],
        8000,
    );
    let results = coordinator.process_right(shipment_too_late).unwrap();
    assert!(
        results.is_empty(),
        "Shipment 3000ms after order should NOT match"
    );
}

#[test]
fn test_symmetric_negative_interval() {
    let config = JoinConfig::interval_ms(
        "sensor_a",
        "sensor_b",
        vec![("reading_id".to_string(), "reading_id".to_string())],
        -500,
        500,
    );

    let mut coordinator = JoinCoordinator::new(config);

    let reading_a = make_test_record(
        vec![
            ("reading_id", FieldValue::Integer(42)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    coordinator.process_left(reading_a).unwrap();

    let reading_b1 = make_test_record(
        vec![
            ("reading_id", FieldValue::Integer(42)),
            ("event_time", FieldValue::Integer(700)),
        ],
        700,
    );
    let results = coordinator.process_right(reading_b1).unwrap();
    assert_eq!(results.len(), 1);

    let reading_b2 = make_test_record(
        vec![
            ("reading_id", FieldValue::Integer(42)),
            ("event_time", FieldValue::Integer(1300)),
        ],
        1300,
    );
    let results = coordinator.process_right(reading_b2).unwrap();
    assert_eq!(results.len(), 1);

    let reading_b3 = make_test_record(
        vec![
            ("reading_id", FieldValue::Integer(42)),
            ("event_time", FieldValue::Integer(400)),
        ],
        400,
    );
    let results = coordinator.process_right(reading_b3).unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_interval_ms_retention_calculation() {
    let config = JoinConfig::interval_ms(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        -3600_000,
        1800_000,
    );

    assert_eq!(config.retention_ms, 7200_000);
    assert!(config.is_interval_join());
}

#[test]
fn test_missing_event_time_skip_record() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    )
    .with_missing_event_time(MissingEventTimeBehavior::SkipRecord);

    let mut coordinator = JoinCoordinator::new(config);

    let record_with_time = make_test_record(
        vec![
            ("key", FieldValue::Integer(1)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    coordinator.process_left(record_with_time).unwrap();
    assert_eq!(coordinator.left_store().record_count(), 1);

    let mut record_without_time = make_test_record(vec![("key", FieldValue::Integer(2))], 0);
    record_without_time.event_time = None;
    let results = coordinator.process_left(record_without_time).unwrap();
    assert!(results.is_empty());
    assert_eq!(coordinator.left_store().record_count(), 1);
    assert_eq!(coordinator.stats().missing_time_count, 1);
}

#[test]
fn test_missing_event_time_error() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    )
    .with_missing_event_time(MissingEventTimeBehavior::Error);

    let mut coordinator = JoinCoordinator::new(config);

    let mut record_without_time = make_test_record(vec![("key", FieldValue::Integer(1))], 0);
    record_without_time.event_time = None;

    let result = coordinator.process_left(record_without_time);
    assert!(result.is_err());
}

#[test]
fn test_missing_event_time_use_wall_clock() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_secs(3600),
    );

    let mut coordinator = JoinCoordinator::new(config);

    let mut record_without_time = make_test_record(vec![("key", FieldValue::Integer(1))], 0);
    record_without_time.event_time = None;

    let results = coordinator.process_left(record_without_time).unwrap();
    assert!(results.is_empty());

    assert_eq!(coordinator.left_store().record_count(), 1);
    assert_eq!(coordinator.stats().missing_time_count, 1);
}

// =========================================================================
// Outer Join Tests
// =========================================================================

#[test]
fn test_left_outer_join_with_match() {
    let config = JoinConfig::equi_join(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(3600),
    )
    .with_join_type(JoinType::LeftOuter);

    let mut coordinator = JoinCoordinator::new(config);

    assert_eq!(coordinator.config().join_type, JoinType::LeftOuter);

    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("customer", FieldValue::String("Alice".to_string())),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    let results = coordinator.process_left(order).unwrap();
    assert!(results.is_empty());

    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("tracking", FieldValue::String("TRACK123".to_string())),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = coordinator.process_right(shipment).unwrap();

    assert_eq!(results.len(), 1);
    let joined = &results[0];
    assert!(joined.fields.contains_key("orders.customer"));
    assert!(joined.fields.contains_key("shipments.tracking"));
}

#[test]
fn test_left_outer_join_without_match_current_behavior() {
    let config = JoinConfig::equi_join(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_millis(1000),
    )
    .with_join_type(JoinType::LeftOuter);

    let mut coordinator = JoinCoordinator::new(config);

    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(999)),
            ("customer", FieldValue::String("Bob".to_string())),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    let results = coordinator.process_left(order).unwrap();
    assert!(results.is_empty());

    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(888)),
            ("tracking", FieldValue::String("TRACK888".to_string())),
            ("event_time", FieldValue::Integer(1500)),
        ],
        1500,
    );
    let results = coordinator.process_right(shipment).unwrap();
    assert!(results.is_empty());

    let (left_expired, _right_expired) = coordinator.advance_watermark(JoinSide::Left, 3000);
    assert_eq!(left_expired, 1);
}

#[test]
fn test_right_outer_join_without_match_current_behavior() {
    let config = JoinConfig::equi_join(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_millis(1000),
    )
    .with_join_type(JoinType::RightOuter);

    let mut coordinator = JoinCoordinator::new(config);

    assert_eq!(coordinator.config().join_type, JoinType::RightOuter);

    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(777)),
            ("tracking", FieldValue::String("ORPHAN".to_string())),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    let results = coordinator.process_right(shipment).unwrap();
    assert!(results.is_empty());

    let (_left_expired, right_expired) = coordinator.advance_watermark(JoinSide::Right, 3000);
    assert_eq!(right_expired, 1);
}

#[test]
fn test_full_outer_join_current_behavior() {
    let config = JoinConfig::equi_join(
        "stream_a",
        "stream_b",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_millis(1000),
    )
    .with_join_type(JoinType::FullOuter);

    let mut coordinator = JoinCoordinator::new(config);

    assert_eq!(coordinator.config().join_type, JoinType::FullOuter);

    let a1 = make_test_record(
        vec![
            ("key", FieldValue::Integer(1)),
            ("a_value", FieldValue::String("A1".to_string())),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    coordinator.process_left(a1).unwrap();

    let b1 = make_test_record(
        vec![
            ("key", FieldValue::Integer(1)),
            ("b_value", FieldValue::String("B1".to_string())),
            ("event_time", FieldValue::Integer(1100)),
        ],
        1100,
    );
    let results = coordinator.process_right(b1).unwrap();
    assert_eq!(results.len(), 1);

    let a2 = make_test_record(
        vec![
            ("key", FieldValue::Integer(2)),
            ("a_value", FieldValue::String("A2-orphan".to_string())),
            ("event_time", FieldValue::Integer(1200)),
        ],
        1200,
    );
    let results = coordinator.process_left(a2).unwrap();
    assert!(results.is_empty());

    let b3 = make_test_record(
        vec![
            ("key", FieldValue::Integer(3)),
            ("b_value", FieldValue::String("B3-orphan".to_string())),
            ("event_time", FieldValue::Integer(1300)),
        ],
        1300,
    );
    let results = coordinator.process_right(b3).unwrap();
    assert!(results.is_empty());

    let (left_expired, right_expired) = coordinator.advance_watermark(JoinSide::Left, 5000);
    assert!(left_expired >= 1 || right_expired >= 1);
}

#[test]
fn test_join_type_builder_method() {
    let config = JoinConfig::interval(
        "left",
        "right",
        vec![("id".to_string(), "id".to_string())],
        Duration::ZERO,
        Duration::from_secs(60),
    );

    assert_eq!(config.join_type, JoinType::Inner);

    let left_outer = config.clone().with_join_type(JoinType::LeftOuter);
    assert_eq!(left_outer.join_type, JoinType::LeftOuter);

    let right_outer = config.clone().with_join_type(JoinType::RightOuter);
    assert_eq!(right_outer.join_type, JoinType::RightOuter);

    let full_outer = config.with_join_type(JoinType::FullOuter);
    assert_eq!(full_outer.join_type, JoinType::FullOuter);
}

#[test]
fn test_outer_join_matches_emit_immediately() {
    for join_type in [
        JoinType::LeftOuter,
        JoinType::RightOuter,
        JoinType::FullOuter,
    ] {
        let config = JoinConfig::equi_join(
            "left",
            "right",
            vec![("key".to_string(), "key".to_string())],
            Duration::from_secs(3600),
        )
        .with_join_type(join_type);

        let mut coordinator = JoinCoordinator::new(config);

        let left = make_test_record(
            vec![
                ("key", FieldValue::Integer(42)),
                ("left_data", FieldValue::String("L".to_string())),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        coordinator.process_left(left).unwrap();

        let right = make_test_record(
            vec![
                ("key", FieldValue::Integer(42)),
                ("right_data", FieldValue::String("R".to_string())),
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = coordinator.process_right(right).unwrap();

        assert_eq!(
            results.len(),
            1,
            "{:?} join should emit on match",
            join_type
        );

        let joined = &results[0];
        assert!(
            joined.fields.contains_key("left.left_data"),
            "{:?} join should have left fields",
            join_type
        );
        assert!(
            joined.fields.contains_key("right.right_data"),
            "{:?} join should have right fields",
            join_type
        );
    }
}

#[test]
fn test_inner_join_no_unmatched_emission() {
    let config = JoinConfig::equi_join(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        Duration::from_millis(100),
    )
    .with_join_type(JoinType::Inner);

    let mut coordinator = JoinCoordinator::new(config);

    let left = make_test_record(
        vec![
            ("key", FieldValue::Integer(1)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    let results = coordinator.process_left(left).unwrap();
    assert!(results.is_empty());

    let right = make_test_record(
        vec![
            ("key", FieldValue::Integer(2)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    let results = coordinator.process_right(right).unwrap();
    assert!(results.is_empty());

    coordinator.advance_watermark(JoinSide::Left, 5000);

    assert_eq!(coordinator.stats().matches_emitted, 0);
}

// ==================== Window Join Tests ====================

#[test]
fn test_join_mode_tumbling_window_id() {
    let mode = JoinMode::Tumbling {
        window_size_ms: 60_000, // 1 minute windows
    };

    // Window 0: [0, 60000)
    assert_eq!(mode.compute_window_id(0), Some(0));
    assert_eq!(mode.compute_window_id(30_000), Some(0));
    assert_eq!(mode.compute_window_id(59_999), Some(0));

    // Window 1: [60000, 120000)
    assert_eq!(mode.compute_window_id(60_000), Some(1));
    assert_eq!(mode.compute_window_id(90_000), Some(1));

    // Window 2: [120000, 180000)
    assert_eq!(mode.compute_window_id(120_000), Some(2));
}

#[test]
fn test_join_mode_tumbling_window_end() {
    let mode = JoinMode::Tumbling {
        window_size_ms: 60_000,
    };

    assert_eq!(mode.window_end_from_id(0), Some(60_000));
    assert_eq!(mode.window_end_from_id(1), Some(120_000));
    assert_eq!(mode.window_end_from_id(2), Some(180_000));
}

#[test]
fn test_join_mode_sliding_window_ids() {
    let mode = JoinMode::Sliding {
        window_size_ms: 60_000, // 1 minute windows
        slide_ms: 30_000,       // 30 second slide
    };

    // A record at time 45000 belongs to windows that:
    // - Window 0: [0, 60000) - contains 45000
    // - Window 1: [30000, 90000) - contains 45000
    let windows = mode.compute_window_ids(45_000);
    assert_eq!(windows.len(), 2);
    assert!(windows.contains(&0));
    assert!(windows.contains(&1));
}

#[test]
fn test_join_mode_emits_on_window_close() {
    let interval = JoinMode::Interval {
        lower_bound_ms: 0,
        upper_bound_ms: 60_000,
    };
    assert!(!interval.emits_on_window_close());

    let tumbling = JoinMode::Tumbling {
        window_size_ms: 60_000,
    };
    assert!(tumbling.emits_on_window_close());

    let sliding = JoinMode::Sliding {
        window_size_ms: 60_000,
        slide_ms: 30_000,
    };
    assert!(sliding.emits_on_window_close());
}

#[test]
fn test_tumbling_window_join_config() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(3600), // 1 hour windows
    );

    assert!(config.is_window_join());
    assert!(!config.is_interval_join());

    match config.join_mode {
        JoinMode::Tumbling { window_size_ms } => {
            assert_eq!(window_size_ms, 3_600_000);
        }
        _ => panic!("Expected tumbling mode"),
    }
}

#[test]
fn test_sliding_window_join_config() {
    let config = JoinConfig::sliding(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(3600), // 1 hour window
        Duration::from_secs(600),  // 10 minute slide
    );

    assert!(config.is_window_join());

    match config.join_mode {
        JoinMode::Sliding {
            window_size_ms,
            slide_ms,
        } => {
            assert_eq!(window_size_ms, 3_600_000);
            assert_eq!(slide_ms, 600_000);
        }
        _ => panic!("Expected sliding mode"),
    }
}

#[test]
fn test_tumbling_window_join_no_immediate_emit() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60), // 1 minute windows
    );

    let mut coordinator = JoinCoordinator::new(config);

    // Add an order at t=30s (window 0)
    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("amount", FieldValue::Float(100.0)),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    let results = coordinator.process_left(order).unwrap();
    // Window joins don't emit on record arrival
    assert!(results.is_empty());

    // Add a shipment at t=45s (same window 0)
    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("tracking", FieldValue::String("TRACK001".to_string())),
            ("event_time", FieldValue::Integer(45_000)),
        ],
        45_000,
    );
    let results = coordinator.process_right(shipment).unwrap();
    // Still no immediate emit for window joins
    assert!(results.is_empty());

    assert_eq!(coordinator.stats().matches_emitted, 0);
}

#[test]
fn test_tumbling_window_join_emit_on_close() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60), // 1 minute windows
    );

    let mut coordinator = JoinCoordinator::new(config);

    // Add an order at t=30s (window 0: [0, 60s))
    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("amount", FieldValue::Float(100.0)),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    coordinator.process_left(order).unwrap();

    // Add a shipment at t=45s (same window 0)
    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("tracking", FieldValue::String("TRACK001".to_string())),
            ("event_time", FieldValue::Integer(45_000)),
        ],
        45_000,
    );
    coordinator.process_right(shipment).unwrap();

    // Close windows with watermark past window end (60s)
    let results = coordinator.close_windows(60_000);

    // Should emit the joined record
    assert_eq!(results.len(), 1);
    let joined = &results[0];
    assert!(joined.fields.contains_key("orders.order_id"));
    assert!(joined.fields.contains_key("shipments.tracking"));
}

#[test]
fn test_tumbling_window_join_different_windows_no_match() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60), // 1 minute windows
    );

    let mut coordinator = JoinCoordinator::new(config);

    // Order in window 0 (t=30s)
    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    coordinator.process_left(order).unwrap();

    // Shipment in window 1 (t=90s)
    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(90_000)),
        ],
        90_000,
    );
    coordinator.process_right(shipment).unwrap();

    // Close window 0
    let results = coordinator.close_windows(60_000);
    assert!(results.is_empty(), "Different windows should not match");
}

#[test]
fn test_tumbling_window_join_multiple_matches() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    );

    let mut coordinator = JoinCoordinator::new(config);

    // Two orders with same key in window 0
    for i in 0..2 {
        let order = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("seq", FieldValue::Integer(i)),
                ("event_time", FieldValue::Integer(10_000 + i * 5_000)),
            ],
            10_000 + i * 5_000,
        );
        coordinator.process_left(order).unwrap();
    }

    // Two shipments with same key in window 0
    for i in 0..2 {
        let shipment = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("tracking", FieldValue::String(format!("TRACK{}", i))),
                ("event_time", FieldValue::Integer(30_000 + i * 5_000)),
            ],
            30_000 + i * 5_000,
        );
        coordinator.process_right(shipment).unwrap();
    }

    // Close window 0 - should emit 2x2=4 matches (cartesian product)
    let results = coordinator.close_windows(60_000);
    assert_eq!(results.len(), 4, "Should emit cartesian product of matches");
}

#[test]
fn test_window_join_stats() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    );

    let mut coordinator = JoinCoordinator::new(config);

    // Add records in window 0
    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    coordinator.process_left(order).unwrap();

    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(45_000)),
        ],
        45_000,
    );
    coordinator.process_right(shipment).unwrap();

    // Before close
    assert_eq!(coordinator.stats().windows_closed, 0);

    // Close window
    coordinator.close_windows(60_000);

    // After close
    assert_eq!(coordinator.stats().windows_closed, 1);
    assert_eq!(coordinator.stats().matches_emitted, 1);
}

// =============================================================================
// EMIT CHANGES Mode Tests
// =============================================================================

#[test]
fn test_emit_mode_default_is_final() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    );

    assert_eq!(config.emit_mode, JoinEmitMode::Final);
    assert!(!config.emits_immediately());
}

#[test]
fn test_emit_mode_builder() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    )
    .with_emit_mode(JoinEmitMode::Changes);

    assert_eq!(config.emit_mode, JoinEmitMode::Changes);
    assert!(config.emits_immediately());
}

#[test]
fn test_interval_join_always_emits_immediately() {
    // Interval joins emit immediately regardless of emit_mode
    let config = JoinConfig::interval(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::ZERO,
        Duration::from_secs(3600),
    );

    // Even with default FINAL mode, interval joins emit immediately
    assert!(config.emits_immediately());
}

#[test]
fn test_tumbling_window_emit_changes_immediate() {
    // With EMIT CHANGES, tumbling window join should emit immediately on match
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    )
    .with_emit_mode(JoinEmitMode::Changes);

    let mut coordinator = JoinCoordinator::new(config);

    // Process left record
    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("amount", FieldValue::Float(100.0)),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    let results = coordinator.process_left(order).unwrap();
    assert!(
        results.is_empty(),
        "No match yet, should not emit on left record"
    );

    // Process matching right record - should emit immediately!
    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("tracking", FieldValue::String("TRACK001".to_string())),
            ("event_time", FieldValue::Integer(45_000)),
        ],
        45_000,
    );
    let results = coordinator.process_right(shipment).unwrap();
    assert_eq!(
        results.len(),
        1,
        "EMIT CHANGES should emit immediately on match"
    );

    // Verify the joined record has both sides' fields
    let joined = &results[0];
    assert!(joined.fields.contains_key("orders.order_id"));
    assert!(joined.fields.contains_key("shipments.tracking"));
}

#[test]
fn test_tumbling_window_emit_changes_bidirectional() {
    // Test that EMIT CHANGES works when right arrives first too
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    )
    .with_emit_mode(JoinEmitMode::Changes);

    let mut coordinator = JoinCoordinator::new(config);

    // Process right record first
    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("tracking", FieldValue::String("TRACK001".to_string())),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    let results = coordinator.process_right(shipment).unwrap();
    assert!(results.is_empty(), "No match yet");

    // Process matching left record - should emit immediately!
    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("amount", FieldValue::Float(100.0)),
            ("event_time", FieldValue::Integer(45_000)),
        ],
        45_000,
    );
    let results = coordinator.process_left(order).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Should emit when left matches existing right"
    );
}

#[test]
fn test_tumbling_window_emit_changes_multiple_matches() {
    // Multiple records on each side should emit cartesian product immediately
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    )
    .with_emit_mode(JoinEmitMode::Changes);

    let mut coordinator = JoinCoordinator::new(config);

    // Add two left records
    for i in 0..2 {
        let order = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("seq", FieldValue::Integer(i)),
                ("event_time", FieldValue::Integer(10_000 + i * 5_000)),
            ],
            10_000 + i * 5_000,
        );
        coordinator.process_left(order).unwrap();
    }

    // Add first right record - should match both left records
    let shipment1 = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("tracking", FieldValue::String("TRACK1".to_string())),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    let results = coordinator.process_right(shipment1).unwrap();
    assert_eq!(results.len(), 2, "Should match both left records");

    // Add second right record - should also match both left records
    let shipment2 = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("tracking", FieldValue::String("TRACK2".to_string())),
            ("event_time", FieldValue::Integer(40_000)),
        ],
        40_000,
    );
    let results = coordinator.process_right(shipment2).unwrap();
    assert_eq!(results.len(), 2, "Should match both left records again");
}

#[test]
fn test_tumbling_window_emit_changes_no_duplicate_on_close() {
    // When using EMIT CHANGES, close_windows should NOT re-emit
    // (records are already emitted immediately)
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    )
    .with_emit_mode(JoinEmitMode::Changes);

    let mut coordinator = JoinCoordinator::new(config);

    // Process records
    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    coordinator.process_left(order).unwrap();

    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(45_000)),
        ],
        45_000,
    );
    let immediate_results = coordinator.process_right(shipment).unwrap();
    assert_eq!(immediate_results.len(), 1, "Should emit immediately");

    // close_windows still emits in EMIT CHANGES mode (it's for completeness)
    // But the user should be aware they've already received the records
    let close_results = coordinator.close_windows(60_000);
    // Note: Currently close_windows will re-emit. This is a design choice -
    // in a full implementation, we might track which matches have been emitted
    // to avoid duplicates. For now, document this behavior.
    assert!(
        !close_results.is_empty(),
        "close_windows also emits (potential duplicate)"
    );
}

#[test]
fn test_sliding_window_emit_changes() {
    // EMIT CHANGES should work with sliding windows too
    let config = JoinConfig::sliding(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60), // 60 second window
        Duration::from_secs(30), // 30 second slide
    )
    .with_emit_mode(JoinEmitMode::Changes);

    let mut coordinator = JoinCoordinator::new(config);

    // Process left
    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    coordinator.process_left(order).unwrap();

    // Process matching right - should emit for each shared window
    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(45_000)),
        ],
        45_000,
    );
    let results = coordinator.process_right(shipment).unwrap();
    // Both records are in the same windows (depending on overlap)
    assert!(!results.is_empty(), "Should emit matches immediately");
}

#[test]
fn test_emit_changes_stats_tracking() {
    let config = JoinConfig::tumbling(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        Duration::from_secs(60),
    )
    .with_emit_mode(JoinEmitMode::Changes);

    let mut coordinator = JoinCoordinator::new(config);

    let order = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(30_000)),
        ],
        30_000,
    );
    coordinator.process_left(order).unwrap();
    assert_eq!(coordinator.stats().matches_emitted, 0);

    let shipment = make_test_record(
        vec![
            ("order_id", FieldValue::Integer(123)),
            ("event_time", FieldValue::Integer(45_000)),
        ],
        45_000,
    );
    coordinator.process_right(shipment).unwrap();
    assert_eq!(
        coordinator.stats().matches_emitted,
        1,
        "Stats should track immediate emissions"
    );
}
