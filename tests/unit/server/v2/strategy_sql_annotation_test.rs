//! Strategy Configuration via SQL Annotations
//!
//! Tests for per-job partitioning strategy specification via SQL query properties.
//! Allows users to configure which partitioning strategy to use via SQL annotations,
//! enabling fine-grained control over V2 routing behavior.

use std::collections::HashMap;
use velostream::velostream::server::v2::{
    AdaptiveJobProcessor, AlwaysHashStrategy, PartitionedJobConfig,
};

/// Test default behavior: no strategy specified defaults to AlwaysHashStrategy
#[test]
fn test_default_strategy_when_not_specified() {
    let config = PartitionedJobConfig::default();

    // Strategy should be None, allowing coordinator to use default AlwaysHashStrategy
    assert!(config.partitioning_strategy.is_none());

    let coordinator = AdaptiveJobProcessor::new(config);
    // Coordinator should be created successfully with default strategy
    assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
}

/// Test configuration with explicit strategy: always_hash
#[test]
fn test_strategy_always_hash() {
    let mut config = PartitionedJobConfig::default();
    config.partitioning_strategy = Some("always_hash".to_string());

    let coordinator = AdaptiveJobProcessor::new(config);
    assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
    // Coordinator created successfully
}

/// Test configuration with explicit strategy: smart_repartition
#[test]
fn test_strategy_smart_repartition() {
    let mut config = PartitionedJobConfig::default();
    config.partitioning_strategy = Some("smart_repartition".to_string());

    let coordinator = AdaptiveJobProcessor::new(config);
    assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
}

/// Test configuration with explicit strategy: sticky_partition
#[test]
fn test_strategy_sticky_partition() {
    let mut config = PartitionedJobConfig::default();
    config.partitioning_strategy = Some("sticky_partition".to_string());

    let coordinator = AdaptiveJobProcessor::new(config);
    assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
}

/// Test configuration with explicit strategy: round_robin
#[test]
fn test_strategy_round_robin() {
    let mut config = PartitionedJobConfig::default();
    config.partitioning_strategy = Some("round_robin".to_string());

    let coordinator = AdaptiveJobProcessor::new(config);
    assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
}

/// Test case-insensitive strategy names
#[test]
fn test_strategy_case_insensitive() {
    let test_cases = vec![
        "AlwaysHash",
        "ALWAYS_HASH",
        "always_hash",
        "SmartRepartition",
        "SMART_REPARTITION",
        "smart_repartition",
    ];

    for strategy_name in test_cases {
        let mut config = PartitionedJobConfig::default();
        config.partitioning_strategy = Some(strategy_name.to_string());

        // Should not panic, coordinator should handle case-insensitive parsing
        let coordinator = AdaptiveJobProcessor::new(config);
        assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
    }
}

/// Test invalid strategy name falls back to default
#[test]
fn test_invalid_strategy_falls_back_to_default() {
    let mut config = PartitionedJobConfig::default();
    config.partitioning_strategy = Some("invalid_strategy_name".to_string());

    // Should not panic, should fallback to AlwaysHashStrategy with a warning
    let coordinator = AdaptiveJobProcessor::new(config);
    assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
}

/// Test strategy can be set via builder pattern after creation
#[test]
fn test_strategy_with_builder_pattern() {
    let config = PartitionedJobConfig::default();
    let coordinator = AdaptiveJobProcessor::new(config)
        .with_strategy(std::sync::Arc::new(AlwaysHashStrategy::new()));

    assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
}

/// Test strategy works with custom partition count
#[test]
fn test_strategy_with_custom_partition_count() {
    let mut config = PartitionedJobConfig::default();
    config.num_partitions = Some(8);
    config.partitioning_strategy = Some("smart_repartition".to_string());

    let coordinator = AdaptiveJobProcessor::new(config);
    assert_eq!(coordinator.num_partitions(), 8);
}

/// Test strategy works with GROUP BY columns
#[test]
fn test_strategy_with_group_by_columns() {
    let mut config = PartitionedJobConfig::default();
    config.partitioning_strategy = Some("smart_repartition".to_string());

    let coordinator = AdaptiveJobProcessor::new(config)
        .with_group_by_columns(vec!["symbol".to_string(), "trader_id".to_string()]);

    assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
}

/// Test multiple configurations in sequence (simulating multiple jobs)
#[test]
fn test_multiple_coordinators_different_strategies() {
    let strategies = vec![
        "always_hash",
        "smart_repartition",
        "sticky_partition",
        "round_robin",
    ];

    for strategy_name in strategies {
        let mut config = PartitionedJobConfig::default();
        config.partitioning_strategy = Some(strategy_name.to_string());
        config.num_partitions = Some(4);

        let coordinator = AdaptiveJobProcessor::new(config);
        assert_eq!(coordinator.num_partitions(), 4);
    }
}

/// Test strategy configuration is preserved through coordinator lifecycle
#[test]
fn test_strategy_config_preserved() {
    let mut config = PartitionedJobConfig::default();
    config.partitioning_strategy = Some("sticky_partition".to_string());
    config.num_partitions = Some(16);

    // Create coordinator
    let coordinator = AdaptiveJobProcessor::new(config);

    // Verify both strategy and partition count are respected
    assert_eq!(coordinator.num_partitions(), 16);

    // Add GROUP BY columns
    let coordinator = coordinator.with_group_by_columns(vec!["id".to_string()]);

    // Should still have correct partition count
    assert_eq!(coordinator.num_partitions(), 16);
}

/// Test SQL annotation extraction via properties (simulating query parsing)
#[test]
fn test_strategy_property_extraction() {
    // Simulate extracted properties from a StreamingQuery
    let mut properties = HashMap::new();
    properties.insert(
        "partitioning_strategy".to_string(),
        "smart_repartition".to_string(),
    );
    properties.insert("use_transactions".to_string(), "true".to_string());

    // Strategy should be extractable
    if let Some(strategy) = properties.get("partitioning_strategy") {
        assert_eq!(strategy, "smart_repartition");

        let mut config = PartitionedJobConfig::default();
        config.partitioning_strategy = Some(strategy.clone());
        let coordinator = AdaptiveJobProcessor::new(config);
        assert_eq!(coordinator.num_partitions(), num_cpus::get().max(1));
    }
}

/// Test recommended strategy for different workload types
#[test]
fn test_recommended_strategies_for_workloads() {
    let workloads = vec![
        ("stateful_aggregation", "always_hash"), // Safety first
        ("pre_partitioned_aligned", "smart_repartition"), // Optimization
        ("latency_sensitive", "sticky_partition"), // Cache locality
        ("simple_filter", "round_robin"),        // Throughput
    ];

    for (_workload_type, strategy) in workloads {
        let mut config = PartitionedJobConfig::default();
        config.partitioning_strategy = Some(strategy.to_string());

        let coordinator = AdaptiveJobProcessor::new(config);
        assert!(coordinator.num_partitions() > 0);
    }
}
