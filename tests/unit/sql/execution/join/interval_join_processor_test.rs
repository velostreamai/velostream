//! Unit tests for IntervalJoinProcessor
//!
//! Tests for config construction, basic join operations, batch processing,
//! stats tracking, watermark expiration, projection, and field resolution.
//!
//! Moved from inline #[cfg(test)] in interval_join.rs to follow project convention.

use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::sql::ast::{Expr, LiteralValue, SelectField};
use velostream::velostream::sql::execution::join::{JoinSide, JoinType};
use velostream::velostream::sql::execution::processors::interval_join::{
    IntervalJoinConfig, IntervalJoinProcessor,
};
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};

fn make_record(fields: Vec<(&str, FieldValue)>, timestamp: i64) -> StreamRecord {
    let field_map: HashMap<String, FieldValue> = fields
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    let mut record = StreamRecord::new(field_map);
    record.timestamp = timestamp;
    record
}

#[test]
fn test_interval_join_config() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600))
        .with_retention(Duration::from_secs(7200))
        .with_join_type(JoinType::Inner);

    assert_eq!(config.left_source, "orders");
    assert_eq!(config.right_source, "shipments");
    assert_eq!(config.key_columns.len(), 1);
    assert_eq!(config.upper_bound, Duration::from_secs(3600));
}

#[test]
fn test_processor_basic_join() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    let order = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("customer", FieldValue::String("Alice".to_string())),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    let results = processor.process_left(order).unwrap();
    assert!(results.is_empty());

    let shipment = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("carrier", FieldValue::String("FedEx".to_string())),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = processor.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("orders.customer"));
    assert!(joined.fields.contains_key("shipments.carrier"));
}

#[test]
fn test_processor_batch_processing() {
    let config = IntervalJoinConfig::new("left", "right")
        .with_key("key", "key")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    let left_batch: Vec<StreamRecord> = (0..5)
        .map(|i| {
            make_record(
                vec![
                    ("key", FieldValue::Integer(i)),
                    ("left_val", FieldValue::String(format!("L{}", i))),
                ],
                1000 + i,
            )
        })
        .collect();

    let results = processor.process_batch(JoinSide::Left, left_batch).unwrap();
    assert!(results.is_empty());

    let right_batch: Vec<StreamRecord> = (0..3)
        .map(|i| {
            make_record(
                vec![
                    ("key", FieldValue::Integer(i)),
                    ("right_val", FieldValue::String(format!("R{}", i))),
                ],
                2000 + i,
            )
        })
        .collect();

    let results = processor
        .process_batch(JoinSide::Right, right_batch)
        .unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_stats_tracking() {
    let config = IntervalJoinConfig::new("left", "right")
        .with_key("id", "id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    for i in 0..5 {
        processor
            .process_left(make_record(vec![("id", FieldValue::Integer(i))], 1000 + i))
            .unwrap();
    }

    for i in 0..3 {
        processor
            .process_right(make_record(vec![("id", FieldValue::Integer(i))], 2000 + i))
            .unwrap();
    }

    let stats = processor.stats();
    assert_eq!(stats.left_records_processed, 5);
    assert_eq!(stats.right_records_processed, 3);
    assert_eq!(stats.matches_emitted, 3);
}

#[test]
fn test_watermark_expiration() {
    let config = IntervalJoinConfig::new("left", "right")
        .with_key("id", "id")
        .with_retention(Duration::from_millis(1000));

    let mut processor = IntervalJoinProcessor::new(config);

    processor
        .process_left(make_record(
            vec![
                ("id", FieldValue::Integer(1)),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        ))
        .unwrap();

    assert_eq!(processor.buffered_count(), 1);

    processor.advance_watermark(JoinSide::Left, 2500);

    assert_eq!(processor.buffered_count(), 0);
}

#[test]
fn test_empty_processor() {
    let config = IntervalJoinConfig::new("left", "right").with_key("id", "id");

    let processor = IntervalJoinProcessor::new(config);

    assert!(processor.is_empty());
    assert_eq!(processor.buffered_count(), 0);
}

#[test]
fn test_projection_applies_select_fields_and_aliases() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    let projection = vec![
        SelectField::AliasedColumn {
            column: "orders.customer".to_string(),
            alias: "customer_name".to_string(),
        },
        SelectField::AliasedColumn {
            column: "shipments.carrier".to_string(),
            alias: "shipping_carrier".to_string(),
        },
        SelectField::Column("orders.order_id".to_string()),
    ];
    processor = processor.with_projection(projection);

    let order = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("customer", FieldValue::String("Alice".to_string())),
            ("amount", FieldValue::Integer(500)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    processor.process_left(order).unwrap();

    let shipment = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("carrier", FieldValue::String("FedEx".to_string())),
            ("tracking", FieldValue::String("123456".to_string())),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = processor.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];

    assert!(joined.fields.contains_key("customer_name"));
    assert_eq!(
        joined.fields.get("customer_name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert!(joined.fields.contains_key("shipping_carrier"));
    assert_eq!(
        joined.fields.get("shipping_carrier"),
        Some(&FieldValue::String("FedEx".to_string()))
    );
    assert!(joined.fields.contains_key("orders.order_id"));
    assert!(!joined.fields.contains_key("orders.amount"));
    assert!(!joined.fields.contains_key("shipments.tracking"));
    assert_eq!(joined.fields.len(), 3);
}

#[test]
fn test_projection_with_wildcard_passes_all_fields() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);
    processor = processor.with_projection(vec![SelectField::Wildcard]);

    let order = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("customer", FieldValue::String("Alice".to_string())),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    processor.process_left(order).unwrap();

    let shipment = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("carrier", FieldValue::String("FedEx".to_string())),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = processor.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("orders.customer"));
    assert!(joined.fields.contains_key("shipments.carrier"));
}

#[test]
fn test_projection_with_expression() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    let projection = vec![
        SelectField::Column("orders.order_id".to_string()),
        SelectField::Expression {
            expr: Expr::Literal(LiteralValue::String("MATCHED".to_string())),
            alias: Some("status".to_string()),
        },
    ];
    processor = processor.with_projection(projection);

    let order = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("customer", FieldValue::String("Alice".to_string())),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    processor.process_left(order).unwrap();

    let shipment = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("carrier", FieldValue::String("FedEx".to_string())),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = processor.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("orders.order_id"));
    assert!(joined.fields.contains_key("status"));
    assert_eq!(
        joined.fields.get("status"),
        Some(&FieldValue::String("MATCHED".to_string()))
    );
    assert_eq!(joined.fields.len(), 2);
}

#[test]
fn test_projection_missing_field_produces_null() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    let projection = vec![
        SelectField::Column("orders.order_id".to_string()),
        SelectField::Column("orders.nonexistent_field".to_string()),
    ];
    processor = processor.with_projection(projection);

    let order = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    processor.process_left(order).unwrap();

    let shipment = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = processor.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("orders.order_id"));
    assert_eq!(
        joined.fields.get("orders.order_id"),
        Some(&FieldValue::Integer(100))
    );
    assert!(joined.fields.contains_key("orders.nonexistent_field"));
    assert_eq!(
        joined.fields.get("orders.nonexistent_field"),
        Some(&FieldValue::Null)
    );
    assert_eq!(joined.fields.len(), 2);
}

#[test]
fn test_projection_ambiguous_unqualified_field_logs_warning() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    let projection = vec![SelectField::Column("order_id".to_string())];
    processor = processor.with_projection(projection);

    let order = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    processor.process_left(order).unwrap();

    let shipment = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = processor.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("order_id"));
    assert_eq!(
        joined.fields.get("order_id"),
        Some(&FieldValue::Integer(100))
    );
}

#[test]
fn test_projection_qualified_name_alias_mismatch_resolves() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    let projection = vec![SelectField::AliasedColumn {
        column: "o.customer".to_string(),
        alias: "customer_name".to_string(),
    }];
    processor = processor.with_projection(projection);

    let order = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("customer", FieldValue::String("Alice".to_string())),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    processor.process_left(order).unwrap();

    let shipment = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = processor.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("customer_name"));
    assert_eq!(
        joined.fields.get("customer_name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
}

#[test]
fn test_resolve_field_qualified_with_multiple_matches_logs_warning() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let mut processor = IntervalJoinProcessor::new(config);

    let projection = vec![SelectField::AliasedColumn {
        column: "x.order_id".to_string(),
        alias: "matched_id".to_string(),
    }];
    processor = processor.with_projection(projection);

    let order = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(1000)),
        ],
        1000,
    );
    processor.process_left(order).unwrap();

    let shipment = make_record(
        vec![
            ("order_id", FieldValue::Integer(100)),
            ("event_time", FieldValue::Integer(2000)),
        ],
        2000,
    );
    let results = processor.process_right(shipment).unwrap();
    assert_eq!(results.len(), 1);

    let joined = &results[0];
    assert!(joined.fields.contains_key("matched_id"));
    assert_eq!(
        joined.fields.get("matched_id"),
        Some(&FieldValue::Integer(100))
    );
}

#[test]
fn test_resolve_field_qualified_fallback_to_unqualified() {
    let config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let processor = IntervalJoinProcessor::new(config);

    let mut fields = HashMap::new();
    fields.insert("orders.order_id".to_string(), FieldValue::Integer(100));
    fields.insert(
        "status".to_string(),
        FieldValue::String("ACTIVE".to_string()),
    );
    let record = StreamRecord::new(fields);

    let value = processor.resolve_field(&record, "x.status");
    assert_eq!(value, Some(FieldValue::String("ACTIVE".to_string())));

    let value = processor.resolve_field(&record, "orders.status");
    assert_eq!(value, Some(FieldValue::String("ACTIVE".to_string())));

    let value = processor.resolve_field(&record, "x.nonexistent");
    assert_eq!(value, None);
}
