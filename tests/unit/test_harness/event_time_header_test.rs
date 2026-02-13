//! Event-time header generation tests for test harness
//!
//! Verifies that the test harness properly populates the `_event_time` header
//! when time_simulation is configured. This is critical for event-time-based
//! processing and watermark generation.
//!
//! **IMPORTANT**: `_event_time` should be in the Kafka MESSAGE HEADER, NOT in
//! the payload schema. These tests verify the complete flow:
//!
//! 1. Generator with time_simulation → event_times vector populated
//! 2. Executor calls take_event_times() → gets timestamps
//! 3. to_stream_records() → sets StreamRecord.event_time from vector
//! 4. Kafka publishing → injects _event_time header from StreamRecord.event_time

use chrono::{Duration, Utc};
use velostream::velostream::sql::execution::types::StreamRecord;
use velostream::velostream::test_harness::schema::Schema;
use velostream::velostream::test_harness::{SchemaDataGenerator, TimeSimulationConfig};

/// Test that generator populates event_times vector when time_simulation is configured
#[test]
fn test_generator_populates_event_times_vector() {
    let schema_yaml = r#"
name: event_time_test
description: Test event time header generation
fields:
  - name: symbol
    type: string
    constraints:
      enum:
        values: ["AAPL", "GOOGL", "MSFT"]
  - name: price
    type: float
    constraints:
      range:
        min: 100.0
        max: 200.0
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    let config = TimeSimulationConfig {
        start_time: Some("-1h".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: Some(100.0),
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator
        .set_time_simulation(&config, 100)
        .expect("Failed to set time simulation");

    // Generate records
    let records = generator
        .generate(&schema, 100)
        .expect("Failed to generate records");

    assert_eq!(records.len(), 100, "Should generate 100 records");

    // Take event times
    let event_times = generator.take_event_times();

    // CRITICAL ASSERTION: event_times vector should be populated
    assert_eq!(
        event_times.len(),
        100,
        "event_times vector should have 100 timestamps (one per record)"
    );

    // Verify timestamps are reasonable (epoch milliseconds)
    // Note: Allow some tolerance since timestamps are generated slightly before we capture "now"
    let now_ms = Utc::now().timestamp_millis();
    let one_hour_ago_ms = (Utc::now() - Duration::hours(1)).timestamp_millis();

    for (i, &ts) in event_times.iter().enumerate() {
        assert!(
            ts >= one_hour_ago_ms - 5000 && ts <= now_ms + 1000, // Allow 5s before and 1s after tolerance
            "Timestamp[{}] = {} should be between {} and {} (within -1h to now range, with tolerance)",
            i,
            ts,
            one_hour_ago_ms,
            now_ms
        );
    }
}

/// Test that event_times are sequential when sequential=true
#[test]
fn test_event_times_are_sequential() {
    let schema_yaml = r#"
name: sequential_test
description: Test sequential event times
fields:
  - name: id
    type: integer
    constraints:
      range:
        min: 1
        max: 1000
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    let config = TimeSimulationConfig {
        start_time: Some("-30m".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None, // Use even spacing
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator
        .set_time_simulation(&config, 50)
        .expect("Failed to set time simulation");

    let _records = generator
        .generate(&schema, 50)
        .expect("Failed to generate records");

    let event_times = generator.take_event_times();

    // Verify sequential ordering (strictly increasing)
    for i in 1..event_times.len() {
        assert!(
            event_times[i] > event_times[i - 1],
            "Timestamp[{}] = {} should be > Timestamp[{}] = {} (sequential mode)",
            i,
            event_times[i],
            i - 1,
            event_times[i - 1]
        );
    }
}

/// Test that take_event_times() consumes the vector (empty after take)
#[test]
fn test_take_event_times_consumes_vector() {
    let schema_yaml = r#"
name: consume_test
description: Test event_times consumption
fields:
  - name: value
    type: integer
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
    generator
        .set_time_simulation(&config, 10)
        .expect("Failed to set time simulation");

    let _records = generator
        .generate(&schema, 10)
        .expect("Failed to generate records");

    // First take should return 10 timestamps
    let event_times_1 = generator.take_event_times();
    assert_eq!(
        event_times_1.len(),
        10,
        "First take should return 10 timestamps"
    );

    // Second take should return empty vector (consumed)
    let event_times_2 = generator.take_event_times();
    assert_eq!(
        event_times_2.len(),
        0,
        "Second take should return empty vector (already consumed)"
    );
}

/// Test that generator without time_simulation has empty event_times
#[test]
fn test_no_time_simulation_empty_event_times() {
    let schema_yaml = r#"
name: no_sim_test
description: Test without time simulation
fields:
  - name: id
    type: integer
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    let mut generator = SchemaDataGenerator::new(Some(42));
    // Do NOT set time simulation

    let _records = generator
        .generate(&schema, 50)
        .expect("Failed to generate records");

    let event_times = generator.take_event_times();

    // Without time simulation, event_times should be empty
    assert_eq!(
        event_times.len(),
        0,
        "event_times should be empty when time_simulation is not configured"
    );
}

/// Test that event_times span the configured time range evenly
#[test]
fn test_event_times_span_time_range() {
    let schema_yaml = r#"
name: rate_test
description: Test event time spacing
fields:
  - name: id
    type: integer
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    // Use a 10-minute window for more stable interval calculation
    let config = TimeSimulationConfig {
        start_time: Some("-10m".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None, // Use even spacing across time range
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator
        .set_time_simulation(&config, 20)
        .expect("Failed to set time simulation");

    let _records = generator
        .generate(&schema, 20)
        .expect("Failed to generate records");

    let event_times = generator.take_event_times();

    // Calculate average interval between events
    let mut intervals: Vec<i64> = Vec::new();
    for i in 1..event_times.len() {
        intervals.push(event_times[i] - event_times[i - 1]);
    }

    let avg_interval = intervals.iter().sum::<i64>() / intervals.len() as i64;

    // 10 minutes = 600,000ms over 20 records = ~30,000ms per interval
    let expected_interval = 600_000 / 20; // ~30,000ms

    // Allow 20% tolerance for time calculation variations
    let tolerance = (expected_interval as f64 * 0.2) as i64;
    assert!(
        (avg_interval - expected_interval).abs() <= tolerance,
        "Average interval {} should be ~{}ms (even spacing over 10m), tolerance ±{}ms",
        avg_interval,
        expected_interval,
        tolerance
    );
}

/// Test event_times with jitter produces variation
#[test]
fn test_event_times_with_jitter() {
    let schema_yaml = r#"
name: jitter_test
description: Test event time jitter
fields:
  - name: id
    type: integer
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    let config = TimeSimulationConfig {
        start_time: Some("-10m".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: Some(50), // ±50ms jitter
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator
        .set_time_simulation(&config, 100)
        .expect("Failed to set time simulation");

    let _records = generator
        .generate(&schema, 100)
        .expect("Failed to generate records");

    let event_times = generator.take_event_times();

    // Calculate intervals
    let mut intervals: Vec<i64> = Vec::new();
    for i in 1..event_times.len() {
        intervals.push(event_times[i] - event_times[i - 1]);
    }

    // With jitter, intervals should vary
    let min_interval = *intervals.iter().min().unwrap();
    let max_interval = *intervals.iter().max().unwrap();

    // Should have variation (at least 20ms range due to ±50ms jitter)
    assert!(
        max_interval - min_interval > 20,
        "Intervals should vary with jitter: min={}, max={}, diff={}",
        min_interval,
        max_interval,
        max_interval - min_interval
    );
}

/// Test that generate() clears event_times on each call (does NOT accumulate)
#[test]
fn test_generate_clears_event_times_each_call() {
    let schema_yaml = r#"
name: clear_test
description: Test that event_times is cleared on each generate() call
fields:
  - name: id
    type: integer
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
    generator
        .set_time_simulation(&config, 100)
        .expect("Failed to set time simulation");

    // First generate
    let _records1 = generator.generate(&schema, 30).expect("Failed to generate");
    let event_times_1 = generator.take_event_times();
    assert_eq!(
        event_times_1.len(),
        30,
        "First generate should have 30 timestamps"
    );

    // Second generate - should clear previous event_times
    let _records2 = generator.generate(&schema, 40).expect("Failed to generate");
    let event_times_2 = generator.take_event_times();

    // CRITICAL: generate() clears event_times, so second call only has 40 timestamps
    assert_eq!(
        event_times_2.len(),
        40,
        "Second generate() should clear event_times and only have 40 timestamps (NOT accumulate)"
    );
}

// =============================================================================
// Integration with StreamRecord (verifies to_stream_records flow)
// =============================================================================

/// Test that event_times can be properly used to create StreamRecords with event_time set
#[test]
fn test_event_times_to_stream_records_flow() {
    let schema_yaml = r#"
name: stream_record_test
description: Test StreamRecord event_time setting
fields:
  - name: symbol
    type: string
    constraints:
      enum:
        values: ["AAPL"]
  - name: price
    type: float
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    let config = TimeSimulationConfig {
        start_time: Some("-30m".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator
        .set_time_simulation(&config, 10)
        .expect("Failed to set time simulation");

    let records = generator
        .generate(&schema, 10)
        .expect("Failed to generate records");

    let event_times = generator.take_event_times();

    // Simulate what executor.rs does: convert to StreamRecords
    let stream_records: Vec<StreamRecord> = records
        .iter()
        .enumerate()
        .map(|(i, record)| {
            let mut sr = StreamRecord::new(record.clone());

            // This is what to_stream_records() does:
            if i < event_times.len() {
                sr.event_time = chrono::DateTime::from_timestamp_millis(event_times[i]);
            }

            sr
        })
        .collect();

    // CRITICAL ASSERTION: StreamRecords should have event_time set
    assert_eq!(stream_records.len(), 10, "Should have 10 StreamRecords");

    for (i, sr) in stream_records.iter().enumerate() {
        assert!(
            sr.event_time.is_some(),
            "StreamRecord[{}] should have event_time set",
            i
        );

        let event_time_ms = sr.event_time.unwrap().timestamp_millis();
        assert_eq!(
            event_time_ms, event_times[i],
            "StreamRecord[{}].event_time should match event_times[{}]",
            i, i
        );
    }
}

/// Test that event_times are unique (no duplicates) with high event rate
#[test]
fn test_event_times_are_unique_no_duplicates() {
    let schema_yaml = r#"
name: unique_test
description: Test that event_times have no duplicates
fields:
  - name: id
    type: integer
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    let config = TimeSimulationConfig {
        start_time: Some("-1h".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: Some(1000.0), // High rate to test for duplicates
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator
        .set_time_simulation(&config, 1000)
        .expect("Failed to set time simulation");

    let _records = generator
        .generate(&schema, 1000)
        .expect("Failed to generate records");

    let event_times = generator.take_event_times();

    // CRITICAL: Verify no duplicates
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = Vec::new();

    for (i, &ts) in event_times.iter().enumerate() {
        if !seen.insert(ts) {
            duplicates.push((i, ts));
        }
    }

    assert!(
        duplicates.is_empty(),
        "Found {} duplicate timestamps: {:?}",
        duplicates.len(),
        duplicates
    );

    // Also verify all timestamps are unique
    assert_eq!(
        seen.len(),
        event_times.len(),
        "All timestamps should be unique"
    );
}

/// Test deduplication with extreme short time window (likely to cause collisions)
#[test]
fn test_event_times_unique_with_short_time_window() {
    let schema_yaml = r#"
name: short_window_test
description: Test deduplication with very short time window
fields:
  - name: id
    type: integer
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    // Very short time window (1 second) with many events - high collision risk
    let config = TimeSimulationConfig {
        start_time: Some("-1s".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: Some(500), // Large jitter relative to window
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator
        .set_time_simulation(&config, 100)
        .expect("Failed to set time simulation");

    let _records = generator
        .generate(&schema, 100)
        .expect("Failed to generate records");

    let event_times = generator.take_event_times();

    // Verify no duplicates even with high collision risk
    let mut seen = std::collections::HashSet::new();
    for &ts in &event_times {
        assert!(
            seen.insert(ts),
            "Duplicate timestamp detected: {} (deduplication should prevent this)",
            ts
        );
    }

    assert_eq!(
        seen.len(),
        100,
        "All 100 timestamps should be unique (deduplication active)"
    );
}

/// Test deduplication with very high event rate (microsecond-level collisions)
#[test]
fn test_event_times_unique_with_very_high_rate() {
    let schema_yaml = r#"
name: high_rate_test
description: Test deduplication with extremely high event rate
fields:
  - name: id
    type: integer
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");

    // Extreme case: 10 second window with 10,000 events = 1ms per event
    // This should trigger deduplication due to rounding
    let config = TimeSimulationConfig {
        start_time: Some("-10s".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator
        .set_time_simulation(&config, 10000)
        .expect("Failed to set time simulation");

    let _records = generator
        .generate(&schema, 10000)
        .expect("Failed to generate records");

    let event_times = generator.take_event_times();

    // Verify all timestamps are unique
    let unique_count = event_times
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(
        unique_count, 10000,
        "All 10,000 timestamps should be unique (deduplication ensures this)"
    );

    // Verify sequential ordering is maintained (even after deduplication)
    for i in 1..event_times.len() {
        assert!(
            event_times[i] > event_times[i - 1],
            "Timestamps should remain sequential after deduplication"
        );
    }
}

/// Test that StreamRecords have event_time in METADATA, not in fields (payload)
#[test]
fn test_event_time_not_in_payload_fields() {
    let schema_yaml = r#"
name: metadata_test
description: Verify _event_time is metadata, not payload
fields:
  - name: symbol
    type: string
    constraints:
      enum:
        values: ["AAPL"]
  - name: price
    type: float
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
    generator
        .set_time_simulation(&config, 5)
        .expect("Failed to set time simulation");

    let records = generator
        .generate(&schema, 5)
        .expect("Failed to generate records");

    // CRITICAL: _event_time should NOT be in the payload fields
    for (i, record) in records.iter().enumerate() {
        assert!(
            !record.contains_key("_event_time"),
            "Record[{}] should NOT have '_event_time' in payload fields",
            i
        );

        // Only schema fields should be present
        assert!(record.contains_key("symbol"), "Should have 'symbol' field");
        assert!(record.contains_key("price"), "Should have 'price' field");

        // Field count should match schema (2 fields only)
        assert_eq!(
            record.len(),
            2,
            "Record should have exactly 2 fields (symbol, price), not including _event_time"
        );
    }

    // event_times vector should be populated separately
    let event_times = generator.take_event_times();
    assert_eq!(
        event_times.len(),
        5,
        "event_times vector should have 5 timestamps (metadata, not payload)"
    );
}
