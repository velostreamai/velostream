use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::kafka::kafka_error::ConsumerError;
use velostream::velostream::table::retry_utils::{
    format_topic_missing_error, is_topic_missing_error, parse_duration,
};

#[test]
fn test_parse_duration_variations() {
    // Test standard formats
    assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
    assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
    assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
    assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
    assert_eq!(parse_duration("2.5s"), Some(Duration::from_millis(2500)));
    assert_eq!(parse_duration("0"), Some(Duration::from_secs(0)));
    assert_eq!(parse_duration("0s"), Some(Duration::from_secs(0)));

    // Test with spaces
    assert_eq!(parse_duration(" 30s "), Some(Duration::from_secs(30)));
    assert_eq!(parse_duration("30 s"), Some(Duration::from_secs(30)));

    // Test full words
    assert_eq!(parse_duration("30 seconds"), Some(Duration::from_secs(30)));
    assert_eq!(parse_duration("5 minutes"), Some(Duration::from_secs(300)));
    assert_eq!(parse_duration("1 hour"), Some(Duration::from_secs(3600)));
    assert_eq!(parse_duration("2 days"), Some(Duration::from_secs(172800)));

    // Test invalid formats
    assert_eq!(parse_duration("invalid"), None);
    assert_eq!(parse_duration(""), None);
    assert_eq!(parse_duration("-5s"), None);
    assert_eq!(parse_duration("abc123"), None);
}

#[test]
fn test_is_topic_missing_error_detection() {
    // Create various Kafka errors that should be detected as missing topics
    let missing_topic_errors = vec![
        rdkafka::error::KafkaError::MetadataFetch(
            rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition,
        ),
        rdkafka::error::KafkaError::MetadataFetch(rdkafka::error::RDKafkaErrorCode::UnknownTopic),
    ];

    for kafka_error in missing_topic_errors {
        let error = ConsumerError::KafkaError(kafka_error);
        assert!(
            is_topic_missing_error(&error),
            "Should detect missing topic error: {:?}",
            error
        );
    }

    // Test errors that should NOT be detected as missing topics
    let other_errors = vec![
        rdkafka::error::KafkaError::MetadataFetch(
            rdkafka::error::RDKafkaErrorCode::BrokerNotAvailable,
        ),
        rdkafka::error::KafkaError::MetadataFetch(
            rdkafka::error::RDKafkaErrorCode::NetworkException,
        ),
    ];

    for kafka_error in other_errors {
        let error = ConsumerError::KafkaError(kafka_error);
        assert!(
            !is_topic_missing_error(&error),
            "Should NOT detect as missing topic error: {:?}",
            error
        );
    }
}

#[test]
fn test_format_topic_missing_error_content() {
    let kafka_error = rdkafka::error::KafkaError::MetadataFetch(
        rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition,
    );
    let error = ConsumerError::KafkaError(kafka_error);
    let formatted = format_topic_missing_error("test_topic", &error);

    // Check that the error message contains all helpful information
    assert!(
        formatted.contains("test_topic"),
        "Should contain topic name"
    );
    assert!(
        formatted.contains("kafka-topics --create"),
        "Should contain creation command"
    );
    assert!(
        formatted.contains("--topic test_topic"),
        "Should contain specific topic in command"
    );
    assert!(
        formatted.contains("topic.wait.timeout"),
        "Should suggest retry configuration"
    );
    assert!(formatted.contains("WITH"), "Should show SQL syntax hint");
    assert!(
        formatted.contains("Original error"),
        "Should include original error"
    );
}

#[test]
fn test_retry_configuration_properties() {
    let mut props: HashMap<String, String> = HashMap::new();

    // Test default values (no properties set)
    let wait_timeout = props
        .get("topic.wait.timeout")
        .and_then(|s| parse_duration(s))
        .unwrap_or(Duration::from_secs(0));
    let retry_interval = props
        .get("topic.retry.interval")
        .and_then(|s| parse_duration(s))
        .unwrap_or(Duration::from_secs(5));

    assert_eq!(wait_timeout, Duration::from_secs(0));
    assert_eq!(retry_interval, Duration::from_secs(5));

    // Test custom values
    props.insert("topic.wait.timeout".to_string(), "30s".to_string());
    props.insert("topic.retry.interval".to_string(), "2s".to_string());

    let wait_timeout = props
        .get("topic.wait.timeout")
        .and_then(|s| parse_duration(s))
        .unwrap_or(Duration::from_secs(0));
    let retry_interval = props
        .get("topic.retry.interval")
        .and_then(|s| parse_duration(s))
        .unwrap_or(Duration::from_secs(5));

    assert_eq!(wait_timeout, Duration::from_secs(30));
    assert_eq!(retry_interval, Duration::from_secs(2));
}

#[test]
fn test_retry_configuration_edge_cases() {
    let mut props: HashMap<String, String> = HashMap::new();

    // Test with invalid duration format (should use defaults)
    props.insert("topic.wait.timeout".to_string(), "invalid".to_string());
    props.insert(
        "topic.retry.interval".to_string(),
        "also-invalid".to_string(),
    );

    let wait_timeout = props
        .get("topic.wait.timeout")
        .and_then(|s| parse_duration(s))
        .unwrap_or(Duration::from_secs(0));
    let retry_interval = props
        .get("topic.retry.interval")
        .and_then(|s| parse_duration(s))
        .unwrap_or(Duration::from_secs(5));

    assert_eq!(
        wait_timeout,
        Duration::from_secs(0),
        "Invalid timeout should default to 0"
    );
    assert_eq!(
        retry_interval,
        Duration::from_secs(5),
        "Invalid interval should default to 5s"
    );

    // Test with very large values
    props.insert("topic.wait.timeout".to_string(), "1h".to_string());
    props.insert("topic.retry.interval".to_string(), "1m".to_string());

    let wait_timeout = props
        .get("topic.wait.timeout")
        .and_then(|s| parse_duration(s))
        .unwrap_or(Duration::from_secs(0));
    let retry_interval = props
        .get("topic.retry.interval")
        .and_then(|s| parse_duration(s))
        .unwrap_or(Duration::from_secs(5));

    assert_eq!(
        wait_timeout,
        Duration::from_secs(3600),
        "Should support 1 hour timeout"
    );
    assert_eq!(
        retry_interval,
        Duration::from_secs(60),
        "Should support 1 minute interval"
    );
}

#[test]
fn test_enhanced_error_message_formatting() {
    // Test that enhanced error messages provide actionable guidance
    let kafka_error = rdkafka::error::KafkaError::MetadataFetch(
        rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition,
    );
    let error = ConsumerError::KafkaError(kafka_error);

    // Test with different topic names
    let topics = vec!["orders", "user-events", "financial_transactions"];

    for topic in topics {
        let formatted = format_topic_missing_error(topic, &error);

        // Verify the message structure
        let lines: Vec<&str> = formatted.lines().collect();
        assert!(lines.len() >= 4, "Should have multiple lines of help");

        // Check for specific actionable items
        assert!(
            formatted.contains(&format!("Kafka topic '{}' does not exist", topic)),
            "Should clearly state the problem"
        );
        assert!(
            formatted.contains(&format!("--topic {}", topic)),
            "Should include specific topic in creation command"
        );
        assert!(
            formatted.contains("\"topic.wait.timeout\" = \"30s\""),
            "Should show example configuration"
        );
    }
}

#[test]
fn test_error_message_sql_syntax() {
    // Ensure error messages show proper SQL syntax for configuration
    let kafka_error = rdkafka::error::KafkaError::MetadataFetch(
        rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition,
    );
    let error = ConsumerError::KafkaError(kafka_error);
    let formatted = format_topic_missing_error("test_topic", &error);

    // Should show WITH clause syntax
    assert!(formatted.contains("WITH"), "Should mention WITH clause");
    assert!(
        formatted.contains("\"topic.wait.timeout\""),
        "Should use proper quote style"
    );

    // Should provide complete example that users can copy
    assert!(
        formatted.contains("WITH (\"topic.wait.timeout\" = \"30s\")"),
        "Should provide complete WITH clause example"
    );
}
