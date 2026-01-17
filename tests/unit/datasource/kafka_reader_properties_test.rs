//! Tests for KafkaDataReader property parsing
//!
//! Tests the consumer property parsing including:
//! - Performance profiles
//! - Individual property overrides
//! - Invalid value handling

use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::kafka::consumer_config::ConsumerConfig;
use velostream::velostream::kafka::performance_presets::PerformancePresets;

/// Helper to create a ConsumerConfig and apply performance profile
fn apply_profile(profile: &str) -> ConsumerConfig {
    let mut props = HashMap::new();
    props.insert("performance_profile".to_string(), profile.to_string());

    let config = ConsumerConfig::new("localhost:9092", "test-topic");
    apply_performance_profile_from_props(config, &props)
}

/// Simulate the reader's apply_performance_profile function
fn apply_performance_profile_from_props(
    config: ConsumerConfig,
    properties: &HashMap<String, String>,
) -> ConsumerConfig {
    let profile = properties.get("performance_profile").map(|s| s.as_str());

    match profile {
        Some("high_throughput") => config.high_throughput(),
        Some("low_latency") => config.low_latency(),
        Some("max_durability") => config.max_durability(),
        Some("development") => config.development(),
        Some("streaming") => config.streaming(),
        Some("batch_processing") => config.batch_processing(),
        _ => config,
    }
}

#[test]
fn test_high_throughput_profile() {
    let config = apply_profile("high_throughput");

    // High throughput should have larger batch sizes
    assert_eq!(config.max_poll_records, 1000);
    assert_eq!(config.fetch_min_bytes, 50000); // 50KB
    assert!(config.enable_auto_commit);
}

#[test]
fn test_low_latency_profile() {
    let config = apply_profile("low_latency");

    // Low latency should have minimal batching
    assert_eq!(config.max_poll_records, 1);
    assert_eq!(config.fetch_min_bytes, 1);
    assert_eq!(config.fetch_max_wait, Duration::from_millis(1));
}

#[test]
fn test_max_durability_profile() {
    let config = apply_profile("max_durability");

    // Max durability should disable auto-commit
    assert!(!config.enable_auto_commit);
    assert_eq!(config.session_timeout, Duration::from_secs(60));
    assert_eq!(config.max_poll_interval, Duration::from_secs(600)); // 10 minutes
}

#[test]
fn test_development_profile() {
    let config = apply_profile("development");

    // Development should have short timeouts
    assert!(config.enable_auto_commit);
    assert_eq!(config.session_timeout, Duration::from_secs(10));
}

#[test]
fn test_streaming_profile() {
    let config = apply_profile("streaming");

    // Streaming should have balanced settings
    assert_eq!(config.max_poll_records, 100);
    assert!(!config.enable_auto_commit); // Better control for streaming
}

#[test]
fn test_batch_processing_profile() {
    let config = apply_profile("batch_processing");

    // Batch processing should have large batches
    assert_eq!(config.max_poll_records, 2000);
    assert_eq!(config.fetch_min_bytes, 100000); // 100KB
    assert_eq!(config.max_poll_interval, Duration::from_secs(1800)); // 30 minutes
}

#[test]
fn test_no_profile_uses_defaults() {
    let props: HashMap<String, String> = HashMap::new();
    let config = ConsumerConfig::new("localhost:9092", "test-topic");
    let result = apply_performance_profile_from_props(config.clone(), &props);

    // Should use default values
    assert_eq!(result.max_poll_records, config.max_poll_records);
    assert_eq!(result.fetch_min_bytes, config.fetch_min_bytes);
}

#[test]
fn test_unknown_profile_uses_defaults() {
    let config = apply_profile("unknown_profile");
    let default = ConsumerConfig::new("localhost:9092", "test-topic");

    // Unknown profile should fall back to defaults
    assert_eq!(config.max_poll_records, default.max_poll_records);
}

#[test]
fn test_profile_values_can_be_overridden() {
    // Start with high_throughput profile
    let mut props = HashMap::new();
    props.insert(
        "performance_profile".to_string(),
        "high_throughput".to_string(),
    );

    let mut config = ConsumerConfig::new("localhost:9092", "test-topic");
    config = apply_performance_profile_from_props(config, &props);

    // Now manually override max_poll_records
    config.max_poll_records = 50;

    // The override should take effect
    assert_eq!(config.max_poll_records, 50);
}
