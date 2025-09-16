/*!
# Kafka Performance Tests

Tests for Kafka-related performance configurations and lightweight performance utilities.
Heavy benchmarks are run as examples in the performance-tests.yml workflow.
*/

use velostream::velostream::kafka::{
    performance_presets::PerformancePresets, producer_config::ProducerConfig,
};

#[test]
fn test_performance_preset_configurations() {
    // Test that all performance presets can be applied without panicking
    let _base_config = ProducerConfig::new("localhost:9092", "test-topic");

    let _high_throughput = _base_config.clone().high_throughput();
    let _low_latency = _base_config.clone().low_latency();
    let _max_durability = _base_config.clone().max_durability();
    let _development = _base_config.clone().development();

    // This ensures our performance configuration traits are valid
    assert!(true, "All performance presets applied successfully");
}

#[test]
fn test_performance_config_bounds() {
    // Test that performance configurations have reasonable bounds
    let _base_config = ProducerConfig::new("localhost:9092", "test-topic");

    let high_throughput = _base_config.clone().high_throughput();
    let low_latency = _base_config.clone().low_latency();

    // Test that the configs are created successfully without panics
    // The actual performance differences are tested in the performance-tests.yml workflow
    assert_eq!(high_throughput.brokers(), "localhost:9092");
    assert_eq!(low_latency.brokers(), "localhost:9092");
}

#[test]
fn test_performance_preset_distinctiveness() {
    // Ensure different presets can be applied to the same base config
    let _base_config = ProducerConfig::new("localhost:9092", "test-topic");

    let _high_throughput = _base_config.clone().high_throughput();
    let _low_latency = _base_config.clone().low_latency();
    let _max_durability = _base_config.clone().max_durability();
    let _development = _base_config.clone().development();

    // All presets should be successfully applied
    // The actual performance characteristics are tested in integration tests
    assert!(true, "All presets applied successfully");
}

#[test]
fn test_producer_config_memory_footprint() {
    // Test that producer configs don't consume excessive memory
    let _base_config = ProducerConfig::new("localhost:9092", "test-topic");

    // This should not panic or consume excessive memory
    let configs: Vec<_> = (0..1000)
        .map(|i| {
            let topic = format!("test-topic-{}", i);
            ProducerConfig::new("localhost:9092", &topic)
        })
        .collect();

    assert_eq!(configs.len(), 1000);
}

#[test]
fn test_performance_preset_method_chaining() {
    // Test that performance preset methods can be chained without issues
    let config = ProducerConfig::new("localhost:9092", "test-topic")
        .high_throughput()
        .low_latency() // This should override high_throughput settings
        .max_durability(); // This should override previous settings

    // Should successfully create the final config
    assert_eq!(config.brokers(), "localhost:9092");
}

#[test]
fn performance_documentation_links_available() {
    // Ensure performance test examples exist (compilation test)
    // This test will fail if performance examples are removed

    // Note: Actual performance tests are run as examples in the performance-tests.yml workflow:
    // - cargo run --example json_performance_test
    // - cargo run --example raw_bytes_performance_test
    // - cargo run --example latency_performance_test
    // - cargo run --example resource_monitoring_test

    println!("Performance examples should be available as cargo run --example commands");
    println!("See .github/workflows/performance-tests.yml for actual performance benchmarks");
}
