//! Data generation tests for the SQL Application Test Harness
//!
//! Tests schema-driven test data generation:
//! - Basic schema parsing and generation
//! - All supported field types
//! - Constraints (range, enum, length)
//! - Timestamp preservation through Kafka publish/consume cycle

#![cfg(feature = "test-support")]

use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::test_harness::TestHarnessInfra;
use velostream::velostream::test_harness::generator::SchemaDataGenerator;
use velostream::velostream::test_harness::schema::Schema;

/// Test schema data generation with basic market data schema
#[test]
fn test_schema_data_generation() {
    let schema_yaml = r#"
name: market_data
description: Market data events for testing
fields:
  - name: symbol
    type: string
    constraints:
      enum_values:
        values: [AAPL, GOOGL, MSFT]
  - name: price
    type: float
    constraints:
      range:
        min: 100.0
        max: 500.0
  - name: volume
    type: integer
    constraints:
      range:
        min: 100
        max: 10000
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    assert_eq!(schema.name, "market_data");
    assert_eq!(schema.fields.len(), 3);

    let mut generator = SchemaDataGenerator::new(Some(42)); // Fixed seed for reproducibility
    let records = generator
        .generate(&schema, 100)
        .expect("Failed to generate records");

    assert_eq!(records.len(), 100, "Should generate 100 records");

    for record in &records {
        assert!(record.contains_key("symbol"), "Record should have symbol");
        assert!(record.contains_key("price"), "Record should have price");
        assert!(record.contains_key("volume"), "Record should have volume");

        // Verify symbol is one of the enum values
        if let Some(FieldValue::String(symbol)) = record.get("symbol") {
            assert!(
                ["AAPL", "GOOGL", "MSFT"].contains(&symbol.as_str()),
                "Symbol should be one of the enum values"
            );
        }

        // Verify price is in range
        if let Some(FieldValue::Float(price)) = record.get("price") {
            assert!(
                *price >= 100.0 && *price <= 500.0,
                "Price should be in range [100, 500]"
            );
        }

        // Verify volume is in range
        if let Some(FieldValue::Integer(volume)) = record.get("volume") {
            assert!(
                *volume >= 100 && *volume <= 10000,
                "Volume should be in range [100, 10000]"
            );
        }
    }

    println!(
        "✅ Schema data generation test passed with {} records",
        records.len()
    );
}

/// Test schema-driven data generation with all supported types
#[test]
fn test_schema_generation_all_types() {
    let schema_yaml = r#"
name: all_types_test
description: Schema with all supported types
fields:
  - name: int_field
    type: integer
    constraints:
      range:
        min: 0
        max: 100
  - name: float_field
    type: float
    constraints:
      range:
        min: 0.0
        max: 1.0
  - name: string_field
    type: string
    constraints:
      length:
        min: 5
        max: 10
  - name: bool_field
    type: boolean
  - name: timestamp_field
    type: timestamp
  - name: enum_field
    type: string
    constraints:
      enum_values:
        values: [RED, GREEN, BLUE]
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    let mut generator = SchemaDataGenerator::new(Some(12345));
    let records = generator.generate(&schema, 20).expect("Failed to generate");

    assert_eq!(records.len(), 20);

    for record in &records {
        // Verify all fields exist
        assert!(record.contains_key("int_field"));
        assert!(record.contains_key("float_field"));
        assert!(record.contains_key("string_field"));
        assert!(record.contains_key("bool_field"));
        assert!(record.contains_key("timestamp_field"));
        assert!(record.contains_key("enum_field"));

        // Verify integer range
        if let Some(FieldValue::Integer(v)) = record.get("int_field") {
            assert!(*v >= 0 && *v <= 100);
        }

        // Verify float range
        if let Some(FieldValue::Float(v)) = record.get("float_field") {
            assert!(*v >= 0.0 && *v <= 1.0);
        }

        // Verify enum values
        if let Some(FieldValue::String(v)) = record.get("enum_field") {
            assert!(["RED", "GREEN", "BLUE"].contains(&v.as_str()));
        }
    }

    println!("✅ All types schema generation test passed");
}

/// Test timestamp preservation through Kafka publish/consume cycle
///
/// This test verifies that when records with event_time are published to Kafka,
/// the Kafka message timestamp (_TIMESTAMP) is correctly set from the event_time field.
/// This is critical for windowed queries to work correctly with time simulation.
#[tokio::test]
async fn test_timestamp_preservation_through_kafka() {
    use chrono::Utc;
    use futures::StreamExt;
    use rdkafka::Message as RdkafkaMessage;
    use rdkafka::consumer::Consumer;
    use rdkafka::producer::BaseRecord;
    use velostream::velostream::kafka::kafka_fast_producer::PolledProducer;

    // Skip test if Docker is not available
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping test: SKIP_DOCKER_TESTS is set");
        return;
    }

    // Create infrastructure with testcontainers
    let mut infra = match TestHarnessInfra::with_testcontainers().await {
        Ok(infra) => infra,
        Err(e) => {
            println!("Skipping test: Failed to start testcontainers: {:?}", e);
            return;
        }
    };

    // Start infrastructure
    infra.start().await.expect("Failed to start infrastructure");

    // create_topic returns the full topic name (with prefix)
    let topic = infra
        .create_topic("timestamp_test", 1)
        .await
        .expect("Failed to create topic");

    // Generate test records with event_time spread over 30 seconds
    // This simulates time-series data that should span multiple 10-second windows
    let base_time = Utc::now() - chrono::Duration::seconds(60);
    let mut records: Vec<HashMap<String, FieldValue>> = Vec::new();

    for i in 0..10 {
        let mut record = HashMap::new();
        // Each record is 3 seconds apart (total span: 27 seconds)
        let event_time = base_time + chrono::Duration::seconds(i * 3);
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert(
            "event_time".to_string(),
            FieldValue::Timestamp(event_time.naive_utc()),
        );
        record.insert("value".to_string(), FieldValue::Float(100.0 + i as f64));
        records.push(record);
    }

    // Publish records with Kafka timestamps set from event_time
    let mut producer = infra
        .create_async_producer()
        .expect("Failed to create producer");

    let mut published_timestamps: Vec<i64> = Vec::new();

    for record in &records {
        // Extract event_time and convert to milliseconds
        let event_time_ms = if let Some(FieldValue::Timestamp(dt)) = record.get("event_time") {
            dt.and_utc().timestamp_millis()
        } else {
            panic!("Record missing event_time");
        };

        published_timestamps.push(event_time_ms);

        // Serialize to JSON
        let json_value = serde_json::json!({
            "id": match record.get("id") {
                Some(FieldValue::Integer(i)) => *i,
                _ => 0,
            },
            "event_time": match record.get("event_time") {
                Some(FieldValue::Timestamp(dt)) => dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                _ => "".to_string(),
            },
            "value": match record.get("value") {
                Some(FieldValue::Float(f)) => *f,
                _ => 0.0,
            }
        });
        let payload = serde_json::to_string(&json_value).expect("Failed to serialize");

        // Publish with explicit Kafka timestamp set from event_time
        let base_record = BaseRecord::<str, [u8]>::to(&topic)
            .payload(payload.as_bytes())
            .timestamp(event_time_ms);

        if let Err((e, _)) = producer.send(base_record) {
            panic!("Failed to send record: {:?}", e);
        }
    }

    // Flush to ensure all records are sent
    producer
        .flush(Duration::from_secs(10))
        .expect("Failed to flush producer");

    println!(
        "Published {} records with timestamps spanning {} seconds",
        records.len(),
        27
    );

    // Allow time for records to be available
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Consume records and verify timestamps
    let consumer = infra
        .create_consumer("timestamp-verify-group")
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let mut message_stream = consumer.stream();
    let mut received_timestamps: Vec<i64> = Vec::new();
    let mut received_event_times: Vec<String> = Vec::new();

    let timeout_result = tokio::time::timeout(Duration::from_secs(15), async {
        while received_timestamps.len() < records.len() {
            if let Some(result) = message_stream.next().await {
                match result {
                    Ok(msg) => {
                        // Get Kafka message timestamp
                        let kafka_timestamp = match msg.timestamp() {
                            rdkafka::Timestamp::CreateTime(t)
                            | rdkafka::Timestamp::LogAppendTime(t) => t,
                            rdkafka::Timestamp::NotAvailable => {
                                panic!("Kafka timestamp not available");
                            }
                        };
                        received_timestamps.push(kafka_timestamp);

                        // Also extract event_time from payload for comparison
                        if let Some(payload) = msg.payload() {
                            let json: serde_json::Value =
                                serde_json::from_slice(payload).expect("Failed to parse JSON");
                            if let Some(event_time) = json.get("event_time") {
                                received_event_times.push(event_time.to_string());
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error consuming message: {:?}", e);
                    }
                }
            }
        }
    })
    .await;

    if timeout_result.is_err() {
        panic!(
            "Timeout: Only received {}/{} messages",
            received_timestamps.len(),
            records.len()
        );
    }

    // Verify all timestamps match
    assert_eq!(
        received_timestamps.len(),
        published_timestamps.len(),
        "Should receive all published records"
    );

    // Sort both lists since Kafka may deliver out of order
    let mut sorted_published: Vec<i64> = published_timestamps.clone();
    sorted_published.sort();
    let mut sorted_received: Vec<i64> = received_timestamps.clone();
    sorted_received.sort();

    for (i, (published, received)) in sorted_published
        .iter()
        .zip(sorted_received.iter())
        .enumerate()
    {
        assert_eq!(
            published, received,
            "Record {}: Kafka timestamp ({}) should match published event_time ({})",
            i, received, published
        );
    }

    // Verify timestamps span multiple windows (at least 20 seconds apart between first and last)
    let time_span_ms = sorted_received.last().unwrap() - sorted_received.first().unwrap();
    assert!(
        time_span_ms >= 20_000,
        "Timestamps should span at least 20 seconds for window testing, got {} ms",
        time_span_ms
    );

    println!(
        "✅ Timestamp preservation test passed: {} records with {} second span",
        received_timestamps.len(),
        time_span_ms / 1000
    );

    // Cleanup
    infra.stop().await.expect("Failed to stop infrastructure");
}
