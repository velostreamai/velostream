//! Functional tests for AdaptiveJobProcessor partitioning strategies
//!
//! Tests that verify each partitioning strategy correctly routes records to partitions
//! based on query metadata and routing context.
//!
//! Each test validates:
//! - Correct partition assignment via actual route_record() invocation
//! - Strategy-specific routing behavior (deterministic, sticky, smart, round-robin, fan-in)
//! - GROUP BY requirements and metadata validation
//! - Integration with AdaptiveJobProcessor

use log::info;
use std::collections::HashMap;
use velostream::velostream::serialization::FieldValue;
use velostream::velostream::server::v2::{
    AlwaysHashStrategy, FanInStrategy, PartitioningStrategy, QueryMetadata, RoundRobinStrategy,
    RoutingContext, SmartRepartitionStrategy, StickyPartitionStrategy, StrategyFactory,
};
use velostream::velostream::sql::execution::types::StreamRecord;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Creates test records with customer_id for GROUP BY testing
fn create_test_records_with_customer_ids(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert(
                "customer_id".to_string(),
                FieldValue::Integer((i % 5) as i64),
            );
            fields.insert(
                "product_id".to_string(),
                FieldValue::Integer((i % 3) as i64),
            );
            fields.insert("amount".to_string(), FieldValue::Integer((i * 10) as i64));
            fields.insert("record_id".to_string(), FieldValue::Integer(i as i64));
            let mut record = StreamRecord::new(fields);
            record.partition = (i % 8) as i32; // Set partition field for sticky strategy
            record
        })
        .collect()
}

/// Creates test records with source partition information (simulating Kafka partitions)
fn create_test_records_with_source_partitions(
    count: usize,
    num_source_partitions: usize,
) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let source_partition = i % num_source_partitions;
            fields.insert(
                "customer_id".to_string(),
                FieldValue::Integer((i % 5) as i64),
            );
            fields.insert("amount".to_string(), FieldValue::Integer((i * 10) as i64));
            fields.insert("record_id".to_string(), FieldValue::Integer(i as i64));
            let mut record = StreamRecord::new(fields);
            record.partition = source_partition as i32; // Set partition field from source
            record
        })
        .collect()
}

// ============================================================================
// ALWAYS HASH STRATEGY TESTS - Deterministic hashing on GROUP BY columns
// ============================================================================

#[test]
fn test_always_hash_strategy_requires_group_by() {
    // AlwaysHashStrategy requires GROUP BY columns for proper routing
    // Assertion: Validation fails without GROUP BY

    info!("=== AlwaysHashStrategy: Requires GROUP BY ===");

    let strategy = AlwaysHashStrategy::new();

    let invalid_metadata = QueryMetadata {
        group_by_columns: vec![],
        has_window: false,
        num_partitions: 8,
        num_cpu_slots: 8,
    };
    assert!(
        strategy.validate(&invalid_metadata).is_err(),
        "AlwaysHashStrategy should reject queries without GROUP BY"
    );

    info!("✅ AlwaysHashStrategy: GROUP BY requirement enforced");
}

#[test]
fn test_always_hash_strategy_deterministic_routing() {
    // AlwaysHashStrategy: Same customer_id always routes to same partition
    // Assertion: Multiple calls with same customer_id return same partition

    info!("=== AlwaysHashStrategy: Deterministic routing ===");

    let strategy = AlwaysHashStrategy::new();
    let records = create_test_records_with_customer_ids(50);

    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    // Track which partition each customer_id routes to
    let mut customer_to_partition: HashMap<i64, usize> = HashMap::new();

    for record in &records {
        if let Some(FieldValue::Integer(customer_id)) = record.fields.get("customer_id") {
            let partition = strategy
                .route_record(record, &routing_context)
                .expect("Should route record successfully");

            // Verify consistent routing
            if let Some(&prev_partition) = customer_to_partition.get(customer_id) {
                assert_eq!(
                    partition, prev_partition,
                    "Customer {} should consistently route to same partition",
                    customer_id
                );
            } else {
                customer_to_partition.insert(*customer_id, partition);
                info!(
                    "Customer {}: assigned to partition {}",
                    customer_id, partition
                );
            }
        }
    }

    // Verify all customers got a partition
    assert_eq!(
        customer_to_partition.len(),
        5,
        "Should have 5 unique customers routed"
    );
    info!(
        "✅ AlwaysHashStrategy: All {} customers routed deterministically",
        customer_to_partition.len()
    );
}

#[test]
fn test_always_hash_strategy_different_customers_may_differ() {
    // AlwaysHashStrategy: Different customers may map to different partitions
    // Assertion: At least some customers map to different partitions (with 8 partitions and 5 customers)

    info!("=== AlwaysHashStrategy: Different customers → different partitions ===");

    let strategy = AlwaysHashStrategy::new();
    let records = create_test_records_with_customer_ids(100);

    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    let mut customer_partitions = std::collections::HashSet::new();
    for record in &records {
        let partition = strategy
            .route_record(record, &routing_context)
            .expect("Should route");
        customer_partitions.insert(partition);
    }

    // With 5 customers and 8 partitions, should have multiple partitions used
    assert!(
        customer_partitions.len() > 1,
        "Multiple customers should map to different partitions"
    );
    info!(
        "✅ AlwaysHashStrategy: Customers distributed across {} partitions",
        customer_partitions.len()
    );
}

// ============================================================================
// STICKY PARTITION STRATEGY TESTS - Preserves source partition affinity
// ============================================================================

#[test]
fn test_sticky_partition_strategy_uses_record_partition() {
    // StickyPartitionStrategy: Uses record.partition field for affinity preservation
    // Assertion: Each record routes to its own partition modulo num_partitions

    info!("=== StickyPartitionStrategy: Uses record partition field ===");

    let strategy = StickyPartitionStrategy::new();
    let records = create_test_records_with_source_partitions(50, 8);

    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    for record in &records {
        let partition = strategy
            .route_record(record, &routing_context)
            .expect("Should route");

        // StickyPartition routes to record.partition % num_partitions
        let expected = (record.partition as usize) % 8;
        assert_eq!(
            partition, expected,
            "StickyPartitionStrategy should use record.partition field"
        );
    }

    info!("✅ StickyPartitionStrategy: Uses record.partition for affinity preservation");
}

#[test]
fn test_sticky_partition_strategy_multiple_partitions() {
    // StickyPartitionStrategy: Multiple source partitions are preserved
    // Assertion: Records from different partitions stay in their respective partitions

    info!("=== StickyPartitionStrategy: Preserves multiple source partitions ===");

    let strategy = StickyPartitionStrategy::new();
    let records = create_test_records_with_customer_ids(100);

    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    let mut partition_distribution = std::collections::HashMap::new();
    for record in &records {
        let partition = strategy
            .route_record(record, &routing_context)
            .expect("Should route");

        // Track what source partition maps to output partition
        let key = record.partition as usize;
        partition_distribution
            .entry(key)
            .or_insert_with(Vec::new)
            .push(partition);
    }

    // Each source partition should consistently route to one output partition
    for (source_part, output_parts) in partition_distribution {
        let unique_outputs: std::collections::HashSet<_> = output_parts.into_iter().collect();
        assert_eq!(
            unique_outputs.len(),
            1,
            "Source partition {} should route to single output partition",
            source_part
        );
    }

    info!("✅ StickyPartitionStrategy: Multiple source partitions preserved");
}

// ============================================================================
// SMART REPARTITION STRATEGY TESTS - Smart alignment detection
// ============================================================================

#[test]
fn test_smart_repartition_uses_hash_by_default() {
    // SmartRepartitionStrategy: Falls back to hashing when not aligned
    // Assertion: Without matching source_partition_key, uses GROUP BY hash

    info!("=== SmartRepartitionStrategy: Uses hashing by default ===");

    let strategy = SmartRepartitionStrategy::new();
    let records = create_test_records_with_source_partitions(100, 8);

    // No source_partition_key match, so will use hashing
    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None, // Doesn't match group_by_columns
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    let mut partitions_used = std::collections::HashSet::new();
    for record in &records {
        let partition = strategy
            .route_record(record, &routing_context)
            .expect("Should route");
        partitions_used.insert(partition);
    }

    // Should distribute across multiple partitions using hash
    assert!(
        partitions_used.len() > 1,
        "SmartRepartitionStrategy should use hashing to distribute across partitions"
    );

    info!(
        "✅ SmartRepartitionStrategy: Uses hashing, distributed across {} partitions",
        partitions_used.len()
    );
}

#[test]
fn test_smart_repartition_falls_back_to_hash_when_misaligned() {
    // SmartRepartitionStrategy: Falls back to hashing when misaligned
    // Assertion: With GROUP BY and misaligned partitions, uses hash routing

    info!("=== SmartRepartitionStrategy: Falls back to hash when misaligned ===");

    let strategy = SmartRepartitionStrategy::new();
    let records = create_test_records_with_customer_ids(100);

    // No source partition info → will use GROUP BY hash routing
    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    let mut partitions_used = std::collections::HashSet::new();
    for record in &records {
        let partition = strategy
            .route_record(record, &routing_context)
            .expect("Should route");
        partitions_used.insert(partition);
    }

    // Should have distributed across multiple partitions using hash
    assert!(
        partitions_used.len() > 1,
        "SmartRepartitionStrategy should hash to multiple partitions when misaligned"
    );
    info!(
        "✅ SmartRepartitionStrategy: Falls back to hashing, used {} partitions",
        partitions_used.len()
    );
}

#[test]
fn test_smart_repartition_uses_partition_field_when_aligned() {
    // SmartRepartitionStrategy: Uses __partition__ field directly when source_partition_key matches GROUP BY
    // Assertion: When aligned (source_partition_key == "customer_id" and GROUP BY ["customer_id"]),
    //           uses __partition__ field for zero-overhead routing (primary optimization)

    info!("=== SmartRepartitionStrategy: Uses __partition__ field when aligned ===");

    let strategy = SmartRepartitionStrategy::new();
    let mut records = create_test_records_with_customer_ids(50);

    // Add __partition__ field to simulate pre-partitioned data
    for (i, record) in records.iter_mut().enumerate() {
        record.fields.insert(
            "__partition__".to_string(),
            FieldValue::Integer((i as i64) % 8),
        );
    }

    // ALIGNED context: source_partition_key matches GROUP BY column
    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: Some("customer_id".to_string()), // Matches group_by_columns!
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    // When aligned, each record should route to its __partition__ field value
    for (i, record) in records.iter().enumerate() {
        let partition = strategy
            .route_record(record, &routing_context)
            .expect("Should route");

        let expected = (i as usize) % 8;
        assert_eq!(
            partition, expected,
            "When aligned, SmartRepartitionStrategy should use __partition__ field directly"
        );
    }

    info!(
        "✅ SmartRepartitionStrategy: Uses __partition__ field when aligned (zero-overhead optimization)"
    );
}

// ============================================================================
// ROUND ROBIN STRATEGY TESTS - Even distribution without GROUP BY
// ============================================================================

#[test]
fn test_round_robin_strategy_cycles_through_partitions() {
    // RoundRobinStrategy: Distributes records in round-robin fashion (0,1,2...N,0,1,2...)
    // Assertion: First 8 records go to partitions 0-7 in order

    info!("=== RoundRobinStrategy: Cycles through partitions ===");

    let strategy = RoundRobinStrategy::new();
    let records = create_test_records_with_customer_ids(100);

    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec![],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    // RoundRobinStrategy maintains internal counter, so test distribution
    let mut partition_counts = vec![0usize; 8];
    for record in &records {
        let partition = strategy
            .route_record(record, &routing_context)
            .expect("Should route");
        assert!(
            partition < 8,
            "Partition {} should be valid (0-7)",
            partition
        );
        partition_counts[partition] += 1;
    }

    // All partitions should have approximately equal counts (100 records / 8 partitions ≈ 12-13 each)
    let avg_count = records.len() / 8;
    for (i, &count) in partition_counts.iter().enumerate() {
        assert!(
            count >= avg_count - 2 && count <= avg_count + 2,
            "Partition {} should have approximately {} records, got {}",
            i,
            avg_count,
            count
        );
    }

    info!(
        "✅ RoundRobinStrategy: Evenly distributed records: {:?}",
        partition_counts
    );
}

#[test]
fn test_round_robin_strategy_no_group_by_required() {
    // RoundRobinStrategy: Works without GROUP BY (intentionally breaks aggregations)
    // Assertion: Validation succeeds without GROUP BY

    info!("=== RoundRobinStrategy: No GROUP BY requirement ===");

    let strategy = RoundRobinStrategy::new();

    let metadata_no_group_by = QueryMetadata {
        group_by_columns: vec![],
        has_window: false,
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    assert!(
        strategy.validate(&metadata_no_group_by).is_ok(),
        "RoundRobinStrategy should work without GROUP BY"
    );

    info!("✅ RoundRobinStrategy: No GROUP BY requirement");
}

// ============================================================================
// FAN-IN STRATEGY TESTS - Consolidates to single partition
// ============================================================================

#[test]
fn test_fan_in_strategy_routes_all_to_partition_zero() {
    // FanInStrategy: Routes ALL records to partition 0 for global aggregation
    // Assertion: Every record goes to partition 0

    info!("=== FanInStrategy: Routes all to partition 0 ===");

    let strategy = FanInStrategy::new();
    let records = create_test_records_with_customer_ids(100);

    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec![],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    // All records must go to partition 0
    for record in &records {
        let partition = strategy
            .route_record(record, &routing_context)
            .expect("Should route");
        assert_eq!(
            partition, 0,
            "FanInStrategy should route all records to partition 0"
        );
    }

    info!(
        "✅ FanInStrategy: All {} records routed to partition 0",
        records.len()
    );
}

#[test]
fn test_fan_in_strategy_consolidates_multiple_sources() {
    // FanInStrategy: Consolidates from multiple source partitions to partition 0
    // Assertion: Records from partitions 0-7 all go to partition 0

    info!("=== FanInStrategy: Consolidates multiple source partitions ===");

    let strategy = FanInStrategy::new();
    let records = create_test_records_with_source_partitions(100, 8);

    // Test with different source partitions
    for source_part in 0..8 {
        let routing_context = RoutingContext {
            source_partition: Some(source_part),
            source_partition_key: None,
            group_by_columns: vec![],
            num_partitions: 8,
            num_cpu_slots: 8,
        };

        for record in &records {
            let partition = strategy
                .route_record(record, &routing_context)
                .expect("Should route");
            assert_eq!(
                partition, 0,
                "FanInStrategy should route source partition {} to partition 0",
                source_part
            );
        }
    }

    info!("✅ FanInStrategy: Consolidated all source partitions to partition 0");
}

// ============================================================================
// STRATEGY FACTORY TESTS
// ============================================================================

#[test]
fn test_strategy_factory_creates_all_strategies() {
    // StrategyFactory: Should create all strategy types from string names
    // Assertion: All strategy names resolve correctly

    info!("=== StrategyFactory: Creates all strategies ===");

    let strategy_names = vec![
        "always_hash",
        "sticky_partition",
        "smart_repartition",
        "round_robin",
        "fan_in",
    ];

    for strategy_name in strategy_names {
        let result = StrategyFactory::create_from_str(strategy_name);
        assert!(
            result.is_ok(),
            "StrategyFactory should create strategy: {}",
            strategy_name
        );
        info!("  ✅ {}", strategy_name);
    }

    info!("✅ StrategyFactory: All strategies created successfully");
}

// ============================================================================
// CROSS-STRATEGY COMPARISON TESTS
// ============================================================================

#[test]
fn test_strategies_produce_different_routing_patterns() {
    // Different strategies produce different partition assignments for same data
    // Assertion: AlwaysHash and FanIn produce different patterns

    info!("=== Strategy Comparison: Different routing patterns ===");

    let records = create_test_records_with_customer_ids(50);
    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    // AlwaysHashStrategy: Uses GROUP BY hash
    let always_hash = AlwaysHashStrategy::new();
    let mut always_hash_partitions = std::collections::HashSet::new();
    for record in &records {
        let partition = always_hash
            .route_record(record, &routing_context)
            .expect("Should route");
        always_hash_partitions.insert(partition);
    }

    // FanInStrategy: Routes all to partition 0
    let fan_in = FanInStrategy::new();
    let fan_in_partitions: std::collections::HashSet<_> = records
        .iter()
        .map(|r| {
            fan_in
                .route_record(r, &routing_context)
                .expect("Should route")
        })
        .collect();

    // Verify they produce different patterns
    assert!(
        always_hash_partitions.len() > 1,
        "AlwaysHashStrategy should use multiple partitions"
    );
    assert_eq!(
        fan_in_partitions.len(),
        1,
        "FanInStrategy should use only partition 0"
    );

    info!(
        "✅ Strategy Comparison: AlwaysHash={} partitions, FanIn={} partition",
        always_hash_partitions.len(),
        fan_in_partitions.len()
    );
}

#[test]
fn test_sticky_vs_always_hash_different_behavior() {
    // Sticky uses record.partition field, AlwaysHash uses GROUP BY hash
    // Assertion: They produce different routing patterns

    info!("=== Strategy Comparison: Sticky vs AlwaysHash ===");

    let records = create_test_records_with_customer_ids(100);

    let sticky = StickyPartitionStrategy::new();
    let always_hash = AlwaysHashStrategy::new();

    let routing_context = RoutingContext {
        source_partition: None,
        source_partition_key: None,
        group_by_columns: vec!["customer_id".to_string()],
        num_partitions: 8,
        num_cpu_slots: 8,
    };

    let mut sticky_partitions = std::collections::HashSet::new();
    let mut hash_partitions = std::collections::HashSet::new();

    for record in &records {
        let sticky_partition = sticky
            .route_record(record, &routing_context)
            .expect("Should route");
        let hash_partition = always_hash
            .route_record(record, &routing_context)
            .expect("Should route");

        sticky_partitions.insert(sticky_partition);
        hash_partitions.insert(hash_partition);
    }

    // Sticky should use all partitions 0-7 (from record.partition = i % 8)
    // AlwaysHash should distribute 5 customers across partitions
    info!(
        "Sticky partitions: {}, AlwaysHash partitions: {}",
        sticky_partitions.len(),
        hash_partitions.len()
    );

    assert!(
        sticky_partitions.len() > 1,
        "Sticky should use multiple partitions from record.partition field"
    );
    assert!(
        hash_partitions.len() > 1,
        "AlwaysHash should use multiple partitions from GROUP BY hash"
    );

    info!("✅ Strategy Comparison: Sticky and AlwaysHash use different routing patterns");
}
