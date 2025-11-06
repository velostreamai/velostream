//! Unit tests for HashRouter
//!
//! Tests deterministic routing and distribution of records across partitions.

use std::collections::HashMap;
use velostream::velostream::server::v2::{HashRouter, PartitionStrategy};
use velostream::velostream::sql::ast::Expr;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

#[test]
fn test_hash_router_deterministic() {
    // Test that same group key always routes to same partition
    let strategy = PartitionStrategy::HashByGroupBy {
        group_by_columns: vec![Expr::Column("trader_id".to_string())],
    };
    let router = HashRouter::new(4, strategy);

    // Create test record
    let mut fields = HashMap::new();
    fields.insert(
        "trader_id".to_string(),
        FieldValue::String("TRADER1".to_string()),
    );
    let record = StreamRecord::new(fields);

    // Route same record multiple times
    let partition1 = router.route_record(&record).unwrap();
    let partition2 = router.route_record(&record).unwrap();
    let partition3 = router.route_record(&record).unwrap();

    // Should always route to same partition
    assert_eq!(partition1, partition2);
    assert_eq!(partition2, partition3);
    assert!(partition1 < 4, "Partition ID must be in range [0, 4)");
}

#[test]
fn test_hash_router_distribution() {
    // Test that different keys distribute across partitions
    let strategy = PartitionStrategy::HashByGroupBy {
        group_by_columns: vec![Expr::Column("trader_id".to_string())],
    };
    let router = HashRouter::new(4, strategy);

    let mut partition_counts = vec![0; 4];

    // Create 100 records with different trader IDs
    for i in 0..100 {
        let mut fields = HashMap::new();
        fields.insert(
            "trader_id".to_string(),
            FieldValue::String(format!("TRADER{}", i)),
        );
        let record = StreamRecord::new(fields);

        let partition = router.route_record(&record).unwrap();
        partition_counts[partition] += 1;
    }

    // Each partition should have received some records
    // (not perfectly balanced, but no partition should be empty)
    for count in &partition_counts {
        assert!(*count > 0, "All partitions should receive records");
    }
}
