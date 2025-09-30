use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::kafka::kafka_error::ConsumerError;
use velostream::velostream::table::retry_utils::{
    calculate_retry_delay, categorize_kafka_error, format_categorized_error, parse_retry_strategy,
    should_retry_for_category, ErrorCategory, RetryMetrics, RetryStrategy,
};

#[test]
fn test_error_categorization_comprehensive() {
    // Test TopicMissing errors
    let topic_missing_errors = vec![
        RDKafkaErrorCode::UnknownTopicOrPartition,
        RDKafkaErrorCode::UnknownTopic,
    ];

    for code in topic_missing_errors {
        let kafka_error = KafkaError::MetadataFetch(code);
        let error = ConsumerError::KafkaError(kafka_error);
        let category = categorize_kafka_error(&error);
        assert_eq!(
            category,
            ErrorCategory::TopicMissing,
            "Failed for: {:?}",
            code
        );
        assert!(
            should_retry_for_category(&category),
            "Should retry for topic missing"
        );
    }

    // Test NetworkIssue errors
    let network_errors = vec![
        RDKafkaErrorCode::BrokerNotAvailable,
        RDKafkaErrorCode::NetworkException,
        RDKafkaErrorCode::AllBrokersDown,
        RDKafkaErrorCode::BrokerTransportFailure,
        RDKafkaErrorCode::RequestTimedOut,
    ];

    for code in network_errors {
        let kafka_error = KafkaError::MetadataFetch(code);
        let error = ConsumerError::KafkaError(kafka_error);
        let category = categorize_kafka_error(&error);
        assert_eq!(
            category,
            ErrorCategory::NetworkIssue,
            "Failed for: {:?}",
            code
        );
        assert!(
            should_retry_for_category(&category),
            "Should retry for network issues"
        );
    }

    // Test AuthenticationIssue errors
    let auth_errors = vec![
        RDKafkaErrorCode::SaslAuthenticationFailed,
        RDKafkaErrorCode::TopicAuthorizationFailed,
        RDKafkaErrorCode::ClusterAuthorizationFailed,
    ];

    for code in auth_errors {
        let kafka_error = KafkaError::MetadataFetch(code);
        let error = ConsumerError::KafkaError(kafka_error);
        let category = categorize_kafka_error(&error);
        assert_eq!(
            category,
            ErrorCategory::AuthenticationIssue,
            "Failed for: {:?}",
            code
        );
        assert!(
            !should_retry_for_category(&category),
            "Should NOT retry for auth issues"
        );
    }

    // Test ConfigurationIssue errors
    let config_errors = vec![
        RDKafkaErrorCode::InvalidConfig,
        RDKafkaErrorCode::InvalidRequest,
        RDKafkaErrorCode::UnsupportedVersion,
    ];

    for code in config_errors {
        let kafka_error = KafkaError::MetadataFetch(code);
        let error = ConsumerError::KafkaError(kafka_error);
        let category = categorize_kafka_error(&error);
        assert_eq!(
            category,
            ErrorCategory::ConfigurationIssue,
            "Failed for: {:?}",
            code
        );
        assert!(
            !should_retry_for_category(&category),
            "Should NOT retry for config issues"
        );
    }
}

#[test]
fn test_retry_strategies() {
    // Test FixedInterval
    let fixed = RetryStrategy::FixedInterval(Duration::from_secs(5));
    assert_eq!(calculate_retry_delay(&fixed, 0), Duration::from_secs(5));
    assert_eq!(calculate_retry_delay(&fixed, 5), Duration::from_secs(5));
    assert_eq!(calculate_retry_delay(&fixed, 100), Duration::from_secs(5));

    // Test ExponentialBackoff
    let exponential = RetryStrategy::ExponentialBackoff {
        initial: Duration::from_secs(1),
        max: Duration::from_secs(60),
        multiplier: 2.0,
    };
    assert_eq!(
        calculate_retry_delay(&exponential, 0),
        Duration::from_secs(1)
    );
    assert_eq!(
        calculate_retry_delay(&exponential, 1),
        Duration::from_secs(2)
    );
    assert_eq!(
        calculate_retry_delay(&exponential, 2),
        Duration::from_secs(4)
    );
    assert_eq!(
        calculate_retry_delay(&exponential, 3),
        Duration::from_secs(8)
    );
    assert_eq!(
        calculate_retry_delay(&exponential, 10),
        Duration::from_secs(60)
    ); // Capped at max

    // Test LinearBackoff
    let linear = RetryStrategy::LinearBackoff {
        initial: Duration::from_secs(2),
        increment: Duration::from_secs(3),
        max: Duration::from_secs(20),
    };
    assert_eq!(calculate_retry_delay(&linear, 0), Duration::from_secs(2));
    assert_eq!(calculate_retry_delay(&linear, 1), Duration::from_secs(5));
    assert_eq!(calculate_retry_delay(&linear, 2), Duration::from_secs(8));
    assert_eq!(calculate_retry_delay(&linear, 10), Duration::from_secs(20)); // Capped at max
}

#[test]
fn test_parse_retry_strategy_configurations() {
    // Test exponential backoff parsing
    let mut props = HashMap::new();
    props.insert(
        "topic.retry.strategy".to_string(),
        "exponential".to_string(),
    );
    props.insert("topic.retry.interval".to_string(), "1s".to_string());
    props.insert("topic.retry.multiplier".to_string(), "2.5".to_string());
    props.insert("topic.retry.max.delay".to_string(), "120s".to_string());

    let strategy = parse_retry_strategy(&props);
    match strategy {
        RetryStrategy::ExponentialBackoff {
            initial,
            max,
            multiplier,
        } => {
            assert_eq!(initial, Duration::from_secs(1));
            assert_eq!(max, Duration::from_secs(120));
            assert_eq!(multiplier, 2.5);
        }
        _ => panic!("Expected exponential backoff strategy"),
    }

    // Test linear backoff parsing
    props.clear();
    props.insert("topic.retry.strategy".to_string(), "linear".to_string());
    props.insert("topic.retry.interval".to_string(), "3s".to_string());
    props.insert("topic.retry.increment".to_string(), "2s".to_string());
    props.insert("topic.retry.max.delay".to_string(), "60s".to_string());

    let strategy = parse_retry_strategy(&props);
    match strategy {
        RetryStrategy::LinearBackoff {
            initial,
            increment,
            max,
        } => {
            assert_eq!(initial, Duration::from_secs(3));
            assert_eq!(increment, Duration::from_secs(2));
            assert_eq!(max, Duration::from_secs(60));
        }
        _ => panic!("Expected linear backoff strategy"),
    }

    // Test default (fixed) strategy
    props.clear();
    props.insert("topic.retry.interval".to_string(), "10s".to_string());

    let strategy = parse_retry_strategy(&props);
    match strategy {
        RetryStrategy::FixedInterval(duration) => {
            assert_eq!(duration, Duration::from_secs(10));
        }
        _ => panic!("Expected fixed interval strategy"),
    }

    // Test strategy aliases
    props.clear();
    props.insert("topic.retry.strategy".to_string(), "exp".to_string());
    props.insert("topic.retry.interval".to_string(), "500ms".to_string());

    let strategy = parse_retry_strategy(&props);
    match strategy {
        RetryStrategy::ExponentialBackoff { initial, .. } => {
            assert_eq!(initial, Duration::from_millis(500));
        }
        _ => panic!("Expected exponential backoff for 'exp' alias"),
    }
}

#[test]
fn test_retry_metrics() {
    let metrics = RetryMetrics::new();

    // Initial state
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.attempts_total, 0);
    assert_eq!(snapshot.successes_total, 0);
    assert_eq!(snapshot.success_rate(), 0.0);

    // Record some metrics
    metrics.record_attempt();
    metrics.record_attempt();
    metrics.record_attempt();
    metrics.record_success();
    metrics.record_error_category(&ErrorCategory::TopicMissing);
    metrics.record_error_category(&ErrorCategory::NetworkIssue);
    metrics.record_timeout();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.attempts_total, 3);
    assert_eq!(snapshot.successes_total, 1);
    assert_eq!(snapshot.timeouts_total, 1);
    assert_eq!(snapshot.topic_missing_total, 1);
    assert_eq!(snapshot.network_errors_total, 1);
    assert_eq!(snapshot.auth_errors_total, 0);

    // Test calculated metrics
    assert!((snapshot.success_rate() - 33.333333333333336).abs() < 0.0001);
    assert!((snapshot.timeout_rate() - 33.333333333333336).abs() < 0.0001);
}

#[test]
fn test_categorized_error_messages() {
    let kafka_error = KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownTopicOrPartition);
    let error = ConsumerError::KafkaError(kafka_error);

    // Test TopicMissing error message
    let topic_missing_msg =
        format_categorized_error("test_topic", &error, &ErrorCategory::TopicMissing);
    assert!(topic_missing_msg.contains("test_topic"));
    assert!(topic_missing_msg.contains("kafka-topics --create"));
    assert!(topic_missing_msg.contains("exponential"));

    // Test NetworkIssue error message
    let network_msg = format_categorized_error("test_topic", &error, &ErrorCategory::NetworkIssue);
    assert!(network_msg.contains("Network/broker connectivity"));
    assert!(network_msg.contains("exponential backoff"));
    assert!(network_msg.contains("connectivity and firewall"));

    // Test AuthenticationIssue error message
    let auth_msg =
        format_categorized_error("test_topic", &error, &ErrorCategory::AuthenticationIssue);
    assert!(auth_msg.contains("Authentication/authorization"));
    assert!(auth_msg.contains("won't resolve with retry"));
    assert!(auth_msg.contains("SASL/SSL"));

    // Test ConfigurationIssue error message
    let config_msg =
        format_categorized_error("test_topic", &error, &ErrorCategory::ConfigurationIssue);
    assert!(config_msg.contains("Configuration error"));
    assert!(config_msg.contains("won't resolve with retry"));
    assert!(config_msg.contains("bootstrap.servers"));

    // Test Unknown error message
    let unknown_msg = format_categorized_error("test_topic", &error, &ErrorCategory::Unknown);
    assert!(unknown_msg.contains("Unknown error"));
    assert!(unknown_msg.contains("broker logs"));
    assert!(unknown_msg.contains("conservative retry"));
}

#[test]
fn test_exponential_backoff_edge_cases() {
    // Test with very large attempt numbers
    let strategy = RetryStrategy::ExponentialBackoff {
        initial: Duration::from_secs(1),
        max: Duration::from_secs(300),
        multiplier: 2.0,
    };

    // Should be capped at max value
    let delay = calculate_retry_delay(&strategy, 50);
    assert_eq!(delay, Duration::from_secs(300));

    // Test with small multiplier
    let strategy = RetryStrategy::ExponentialBackoff {
        initial: Duration::from_secs(10),
        max: Duration::from_secs(60),
        multiplier: 1.1,
    };

    let delay0 = calculate_retry_delay(&strategy, 0);
    let delay1 = calculate_retry_delay(&strategy, 1);
    let delay2 = calculate_retry_delay(&strategy, 2);

    assert_eq!(delay0, Duration::from_secs(10));
    assert_eq!(delay1, Duration::from_secs(11)); // 10 * 1.1
    assert_eq!(delay2, Duration::from_secs(12)); // 10 * 1.1^2 ≈ 12.1 -> 12
}

#[test]
fn test_linear_backoff_edge_cases() {
    // Test with zero increment
    let strategy = RetryStrategy::LinearBackoff {
        initial: Duration::from_secs(5),
        increment: Duration::from_secs(0),
        max: Duration::from_secs(30),
    };

    assert_eq!(calculate_retry_delay(&strategy, 0), Duration::from_secs(5));
    assert_eq!(calculate_retry_delay(&strategy, 10), Duration::from_secs(5));

    // Test increment larger than max
    let strategy = RetryStrategy::LinearBackoff {
        initial: Duration::from_secs(1),
        increment: Duration::from_secs(100),
        max: Duration::from_secs(30),
    };

    assert_eq!(calculate_retry_delay(&strategy, 0), Duration::from_secs(1));
    assert_eq!(calculate_retry_delay(&strategy, 1), Duration::from_secs(30)); // Capped
}

#[test]
fn test_configuration_parsing_edge_cases() {
    // Test empty properties
    let props = HashMap::new();
    let strategy = parse_retry_strategy(&props);
    match strategy {
        RetryStrategy::FixedInterval(duration) => {
            assert_eq!(duration, Duration::from_secs(5)); // Default
        }
        _ => panic!("Expected default fixed interval"),
    }

    // Test invalid multiplier (should use default)
    let mut props = HashMap::new();
    props.insert(
        "topic.retry.strategy".to_string(),
        "exponential".to_string(),
    );
    props.insert("topic.retry.multiplier".to_string(), "invalid".to_string());

    let strategy = parse_retry_strategy(&props);
    match strategy {
        RetryStrategy::ExponentialBackoff { multiplier, .. } => {
            assert_eq!(multiplier, 2.0); // Default fallback
        }
        _ => panic!("Expected exponential backoff with default multiplier"),
    }

    // Test unknown strategy (should use fixed)
    props.clear();
    props.insert(
        "topic.retry.strategy".to_string(),
        "unknown_strategy".to_string(),
    );
    props.insert("topic.retry.interval".to_string(), "7s".to_string());

    let strategy = parse_retry_strategy(&props);
    match strategy {
        RetryStrategy::FixedInterval(duration) => {
            assert_eq!(duration, Duration::from_secs(7));
        }
        _ => panic!("Expected fallback to fixed interval"),
    }
}
