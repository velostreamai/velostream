//! Integration tests for table retry functionality that require Kafka
//!
//! These tests require a running Kafka instance on localhost:9092

use std::collections::HashMap;
use velostream::velostream::kafka::consumer_config::ConsumerConfig;
use velostream::velostream::kafka::serialization::StringSerializer;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::table::Table;

#[tokio::test]
#[ignore = "Test requires Kafka running on localhost:9092"]
async fn test_table_creation_with_retry_properties() {
    let config = ConsumerConfig::new("localhost:9092", "test-group");
    let mut properties = HashMap::new();

    // Test with 0 timeout (no retry - should fail immediately)
    properties.insert("topic.wait.timeout".to_string(), "0s".to_string());
    properties.insert("topic.retry.interval".to_string(), "1s".to_string());

    let result = Table::new_with_properties(
        config.clone(),
        "non-existent-topic-test".to_string(),
        StringSerializer,
        JsonFormat,
        properties.clone(),
    )
    .await;

    // Should fail immediately with 0 timeout
    assert!(result.is_err(), "Should fail immediately with 0 timeout");

    // Test with very short timeout (should still fail but try retry logic)
    properties.insert("topic.wait.timeout".to_string(), "2s".to_string());
    properties.insert("topic.retry.interval".to_string(), "1s".to_string());

    let start = std::time::Instant::now();
    let result = Table::new_with_properties(
        config,
        "non-existent-topic-test-2".to_string(),
        StringSerializer,
        JsonFormat,
        properties,
    )
    .await;

    let elapsed = start.elapsed();

    // Should fail after attempting retry
    assert!(result.is_err(), "Should fail after retry attempts");

    // Should have taken at least 2 seconds (the timeout)
    assert!(
        elapsed.as_secs() >= 2,
        "Should have waited for timeout period, elapsed: {:?}",
        elapsed
    );

    // Should not have taken much longer than timeout + one retry interval
    assert!(
        elapsed.as_secs() < 5,
        "Should not have waited too long, elapsed: {:?}",
        elapsed
    );
}

#[tokio::test]
#[ignore = "Test requires Kafka running on localhost:9092"]
async fn test_table_creation_backward_compatibility() {
    // Test that existing behavior (no retry properties) still works
    let config = ConsumerConfig::new("localhost:9092", "test-group");
    let properties = HashMap::new(); // No retry properties

    let start = std::time::Instant::now();
    let result = Table::new_with_properties(
        config,
        "non-existent-topic-backward-compat".to_string(),
        StringSerializer,
        JsonFormat,
        properties,
    )
    .await;

    let elapsed = start.elapsed();

    // Should fail immediately (no retry)
    assert!(result.is_err(), "Should fail immediately without retry");

    // Should be very fast (no waiting)
    assert!(
        elapsed.as_millis() < 1000,
        "Should fail quickly without retry, elapsed: {:?}",
        elapsed
    );
}