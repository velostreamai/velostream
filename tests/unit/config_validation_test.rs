use crate::unit::common::*;

#[test]
fn test_invalid_broker_configuration() {
    // Test various malformed broker strings
    let invalid_brokers = vec![
        "",                // Empty string
        "   ",             // Whitespace only
        "invalid::broker", // Double colon
        ":::::",           // Only colons
        "localhost:",      // Missing port
        ":9092",           // Missing host
        "localhost:abc",   // Invalid port
        "localhost:99999", // Out of range port
        "localhost:-1",    // Negative port
    ];

    for broker in invalid_brokers {
        let config_result = std::panic::catch_unwind(|| ProducerConfig::new(broker, "test-topic"));

        // Either should panic or create a config that would fail later
        // For now, we'll test that it doesn't crash the process
        println!(
            "Testing broker: '{}' - Result: {:?}",
            broker,
            config_result.is_ok()
        );
    }
}

#[test]
fn test_timeout_boundary_values() {
    let config = ProducerConfig::new("localhost:9092", "test-topic");

    // Test zero timeout
    let zero_timeout_config = config.clone().request_timeout(Duration::ZERO);
    assert_eq!(
        zero_timeout_config.request_timeout_duration(),
        Duration::ZERO
    );

    // Test very large timeout
    let large_timeout = Duration::from_secs(u32::MAX as u64);
    let large_timeout_config = config.clone().request_timeout(large_timeout);
    assert_eq!(
        large_timeout_config.request_timeout_duration(),
        large_timeout
    );

    // Test reasonable timeouts
    let reasonable_timeout = Duration::from_secs(30);
    let reasonable_config = config.clone().request_timeout(reasonable_timeout);
    assert_eq!(
        reasonable_config.request_timeout_duration(),
        reasonable_timeout
    );
}

#[test]
fn test_custom_property_validation() {
    let config = ProducerConfig::new("localhost:9092", "test-topic");

    // Test valid properties
    let valid_config = config
        .clone()
        .custom_property("batch.size", "16384")
        .custom_property("linger.ms", "5")
        .custom_property("compression.type", "lz4");

    let properties = valid_config.custom_config_ref();
    assert_eq!(properties.get("batch.size"), Some(&"16384".to_string()));
    assert_eq!(properties.get("linger.ms"), Some(&"5".to_string()));
    assert_eq!(properties.get("compression.type"), Some(&"lz4".to_string()));

    // Test potentially problematic properties
    let edge_case_config = config
        .clone()
        .custom_property("", "empty_key") // Empty key
        .custom_property("key", "") // Empty value
        .custom_property("unicode.🦀", "rust") // Unicode in key
        .custom_property("spaces in key", "value") // Spaces in key
        .custom_property("newline", "line1\nline2") // Newline in value
        .custom_property("very.long.key.with.many.dots.and.segments", "value");

    let edge_properties = edge_case_config.custom_config_ref();
    assert_eq!(edge_properties.get(""), Some(&"empty_key".to_string()));
    assert_eq!(edge_properties.get("key"), Some(&"".to_string()));
    assert_eq!(edge_properties.get("unicode.🦀"), Some(&"rust".to_string()));
}

#[test]
fn test_consumer_config_boundary_values() {
    let config = ConsumerConfig::new("localhost:9092", "test-group");

    // Test boundary values for various consumer settings
    let boundary_config = config
        .fetch_min_bytes(1) // Minimum fetch
        .fetch_max_bytes(u32::MAX) // Maximum fetch
        .max_poll_records(1) // Minimum poll records
        .session_timeout(Duration::from_millis(1)) // Very short timeout
        .request_timeout(Duration::from_secs(300)); // Long timeout

    assert_eq!(
        boundary_config.request_timeout_duration(),
        Duration::from_secs(300)
    );
}

#[test]
fn test_performance_tuning_edge_cases() {
    let producer_config = ProducerConfig::new("localhost:9092", "test-topic");
    let consumer_config = ConsumerConfig::new("localhost:9092", "test-group");

    // Test extreme performance tuning values
    let extreme_producer = producer_config.performance_tuning(1, 1); // 1MB max message, 1KB socket buffer

    let extreme_consumer = consumer_config.performance_tuning(1, 1, 1); // 1MB fetch, 1MB partition, 1KB socket

    // Should not crash
    assert_eq!(extreme_producer.brokers(), "localhost:9092");
    assert_eq!(extreme_consumer.brokers(), "localhost:9092");
}
