//! Generator unit tests for test harness
//!
//! Tests for schema-driven data generation including:
//! - timestamp_epoch_ms constraint
//! - Integer field generation with constraints
//! - Random timestamp range generation

use chrono::{Duration, Utc};
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::test_harness::generator::SchemaDataGenerator;
use velostream::velostream::test_harness::schema::Schema;

/// Test that timestamp_epoch_ms constraint generates Integer values (epoch milliseconds)
#[test]
fn test_timestamp_epoch_ms_generates_integer() {
    let schema_yaml = r#"
name: epoch_millis_test
description: Test epoch milliseconds generation
fields:
  - name: event_time
    type: integer
    nullable: false
    constraints:
      timestamp_epoch_ms:
        start: "-1h"
        end: "now"
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    let mut generator = SchemaDataGenerator::new(Some(42));
    let records = generator
        .generate(&schema, 10)
        .expect("Failed to generate records");

    assert_eq!(records.len(), 10);

    let now_ms = Utc::now().timestamp_millis();
    let one_hour_ago_ms = (Utc::now() - Duration::hours(1)).timestamp_millis();

    for record in &records {
        assert!(
            record.contains_key("event_time"),
            "Record should have event_time"
        );

        match record.get("event_time") {
            Some(FieldValue::Integer(ms)) => {
                // Should be a reasonable epoch milliseconds value (within our range)
                assert!(
                    *ms >= one_hour_ago_ms && *ms <= now_ms,
                    "Epoch millis {} should be between {} and {}",
                    ms,
                    one_hour_ago_ms,
                    now_ms
                );
            }
            other => panic!("event_time should be Integer, got {:?}", other),
        }
    }
}

/// Test that timestamp_epoch_ms values are within the specified relative range
#[test]
fn test_timestamp_epoch_ms_respects_relative_range() {
    let schema_yaml = r#"
name: epoch_millis_range_test
description: Test epoch milliseconds respects range
fields:
  - name: event_time
    type: integer
    constraints:
      timestamp_epoch_ms:
        start: "-30m"
        end: "-10m"
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    let mut generator = SchemaDataGenerator::new(Some(123));
    let records = generator
        .generate(&schema, 50)
        .expect("Failed to generate records");

    let now = Utc::now();
    let start_ms = (now - Duration::minutes(30)).timestamp_millis();
    let end_ms = (now - Duration::minutes(10)).timestamp_millis();

    for record in &records {
        if let Some(FieldValue::Integer(ms)) = record.get("event_time") {
            assert!(
                *ms >= start_ms && *ms <= end_ms,
                "Epoch millis {} should be between {} and {} (-30m to -10m)",
                ms,
                start_ms,
                end_ms
            );
        }
    }
}

/// Test that timestamp_epoch_ms with time simulation uses sequential timestamps
#[test]
fn test_timestamp_epoch_ms_with_time_simulation() {
    use velostream::velostream::test_harness::TimeSimulationConfig;

    let schema_yaml = r#"
name: epoch_millis_sim_test
description: Test epoch milliseconds with time simulation
fields:
  - name: event_time
    type: integer
    constraints:
      timestamp_epoch_ms:
        start: "-1h"
        end: "now"
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    let config = TimeSimulationConfig {
        start_time: Some("-1h".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator.set_time_simulation(&config, 10).unwrap();

    let records = generator
        .generate(&schema, 10)
        .expect("Failed to generate records");

    // Collect timestamps
    let mut timestamps: Vec<i64> = Vec::new();
    for record in &records {
        if let Some(FieldValue::Integer(ms)) = record.get("event_time") {
            timestamps.push(*ms);
        }
    }

    assert_eq!(timestamps.len(), 10, "Should have 10 timestamps");

    // Verify timestamps are sequential (increasing)
    for i in 1..timestamps.len() {
        assert!(
            timestamps[i] > timestamps[i - 1],
            "Timestamps should be sequential: {} should be > {}",
            timestamps[i],
            timestamps[i - 1]
        );
    }
}

/// Test that regular integer fields (without timestamp_epoch_ms) use standard range constraints
#[test]
fn test_integer_field_without_epoch_ms_uses_range() {
    let schema_yaml = r#"
name: integer_range_test
description: Test regular integer range constraint
fields:
  - name: quantity
    type: integer
    constraints:
      range:
        min: 100
        max: 500
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    let mut generator = SchemaDataGenerator::new(Some(42));
    let records = generator
        .generate(&schema, 100)
        .expect("Failed to generate records");

    for record in &records {
        if let Some(FieldValue::Integer(val)) = record.get("quantity") {
            assert!(
                *val >= 100 && *val <= 500,
                "Integer value {} should be in range [100, 500]",
                val
            );
        }
    }
}

/// Test that timestamp_epoch_ms constraint takes precedence over range constraint
#[test]
fn test_timestamp_epoch_ms_takes_precedence_over_range() {
    let schema_yaml = r#"
name: epoch_millis_precedence_test
description: Test that timestamp_epoch_ms takes precedence
fields:
  - name: event_time
    type: integer
    constraints:
      range:
        min: 0
        max: 100
      timestamp_epoch_ms:
        start: "-1h"
        end: "now"
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    let mut generator = SchemaDataGenerator::new(Some(42));
    let records = generator
        .generate(&schema, 10)
        .expect("Failed to generate records");

    let now_ms = Utc::now().timestamp_millis();

    for record in &records {
        if let Some(FieldValue::Integer(ms)) = record.get("event_time") {
            // Should be epoch millis (much larger than 100), not range [0, 100]
            assert!(
                *ms > 1_000_000_000_000, // Must be after year 2001 in epoch ms
                "Value {} should be epoch milliseconds, not range value",
                ms
            );
            assert!(*ms <= now_ms, "Value {} should be <= now ({})", ms, now_ms);
        }
    }
}

/// Test schema parsing of timestamp_epoch_ms constraint
#[test]
fn test_timestamp_epoch_ms_schema_parsing() {
    let schema_yaml = r#"
name: parsing_test
description: Test schema parsing
fields:
  - name: event_time
    type: integer
    constraints:
      timestamp_epoch_ms:
        start: "-24h"
        end: "now"
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    assert_eq!(schema.fields.len(), 1);

    let field = &schema.fields[0];
    assert_eq!(field.name, "event_time");

    let timestamp_range = field
        .constraints
        .timestamp_epoch_ms
        .as_ref()
        .expect("Should have timestamp_epoch_ms constraint");

    match timestamp_range {
        velostream::velostream::test_harness::schema::TimestampRange::Relative { start, end } => {
            assert_eq!(start, "-24h");
            assert_eq!(end, "now");
        }
        _ => panic!("Expected relative timestamp range"),
    }
}

/// Test multiple fields with different timestamp types in same schema
#[test]
fn test_mixed_timestamp_types() {
    let schema_yaml = r#"
name: mixed_timestamps_test
description: Test mixed timestamp types
fields:
  - name: event_time_millis
    type: integer
    constraints:
      timestamp_epoch_ms:
        start: "-1h"
        end: "now"
  - name: created_at
    type: timestamp
    constraints:
      timestamp_range:
        start: "-1h"
        end: "now"
  - name: quantity
    type: integer
    constraints:
      range:
        min: 1
        max: 100
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    let mut generator = SchemaDataGenerator::new(Some(42));
    let records = generator
        .generate(&schema, 10)
        .expect("Failed to generate records");

    for record in &records {
        // event_time_millis should be Integer (epoch millis)
        match record.get("event_time_millis") {
            Some(FieldValue::Integer(ms)) => {
                assert!(*ms > 1_000_000_000_000, "Should be epoch millis");
            }
            other => panic!("event_time_millis should be Integer, got {:?}", other),
        }

        // created_at should be Timestamp
        match record.get("created_at") {
            Some(FieldValue::Timestamp(_)) => {}
            other => panic!("created_at should be Timestamp, got {:?}", other),
        }

        // quantity should be Integer in range
        match record.get("quantity") {
            Some(FieldValue::Integer(val)) => {
                assert!(
                    *val >= 1 && *val <= 100,
                    "quantity should be in range [1, 100]"
                );
            }
            other => panic!("quantity should be Integer, got {:?}", other),
        }
    }
}
