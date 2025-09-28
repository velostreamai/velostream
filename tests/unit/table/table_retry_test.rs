use std::collections::HashMap;
use velostream::velostream::kafka::consumer_config::ConsumerConfig;
use velostream::velostream::kafka::serialization::StringSerializer;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::table::retry_utils::{is_topic_missing_error, parse_duration};
use velostream::velostream::table::Table;

#[test]
fn test_parse_duration_various_formats() {
    // Test seconds
    assert_eq!(
        parse_duration("30s"),
        Some(std::time::Duration::from_secs(30))
    );
    assert_eq!(
        parse_duration("5.5s"),
        Some(std::time::Duration::from_millis(5500))
    );

    // Test minutes
    assert_eq!(
        parse_duration("2m"),
        Some(std::time::Duration::from_secs(120))
    );
    assert_eq!(
        parse_duration("1.5m"),
        Some(std::time::Duration::from_secs(90))
    );

    // Test hours
    assert_eq!(
        parse_duration("1h"),
        Some(std::time::Duration::from_secs(3600))
    );

    // Test milliseconds
    assert_eq!(
        parse_duration("500ms"),
        Some(std::time::Duration::from_millis(500))
    );

    // Test zero
    assert_eq!(parse_duration("0"), Some(std::time::Duration::from_secs(0)));
    assert_eq!(
        parse_duration("0s"),
        Some(std::time::Duration::from_secs(0))
    );

    // Test invalid formats
    assert_eq!(parse_duration("invalid"), None);
    assert_eq!(parse_duration(""), None);
    assert_eq!(parse_duration("-5s"), None);
}

#[test]
fn test_topic_missing_error_detection() {
    use rdkafka::error::KafkaError;
    use velostream::velostream::kafka::kafka_error::ConsumerError;

    // Test error messages that should be detected as missing topic
    let missing_topic_messages = vec![
        "Unknown topic or partition",
        "Topic does not exist",
        "unknown topic test-topic",
        "UNKNOWN_TOPIC_OR_PARTITION",
    ];

    for msg in missing_topic_messages {
        // Use MetadataFetch error variant which is appropriate for topic missing errors
        let kafka_error = KafkaError::MetadataFetch(rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition);
        let error = ConsumerError::KafkaError(kafka_error);
        assert!(
            is_topic_missing_error(&error),
            "Should detect missing topic for: {}",
            msg
        );
    }

    // Test error messages that should NOT be detected as missing topic
    let other_error_messages = vec![
        "Connection refused",
        "Authentication failed",
        "Network timeout",
        "Invalid configuration",
    ];

    for msg in other_error_messages {
        let kafka_error = KafkaError::MetadataFetch(rdkafka::error::RDKafkaErrorCode::BrokerNotAvailable);
        let error = ConsumerError::KafkaError(kafka_error);
        assert!(
            !is_topic_missing_error(&error),
            "Should NOT detect missing topic for: {}",
            msg
        );
    }
}

#[tokio::test]
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

#[test]
fn test_property_parsing_edge_cases() {
    // Test that invalid timeout values are handled gracefully
    let test_cases = vec![
        ("", None),
        ("invalid", None),
        ("-5s", None),
        ("0", Some(std::time::Duration::from_secs(0))),
        ("1000ms", Some(std::time::Duration::from_millis(1000))),
        ("1.5h", Some(std::time::Duration::from_secs(5400))),
    ];

    for (input, expected) in test_cases {
        assert_eq!(
            parse_duration(input),
            expected,
            "Failed for input: '{}'",
            input
        );
    }
}
