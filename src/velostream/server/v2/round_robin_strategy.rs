//! Round-robin partitioning strategy
//!
//! Distributes records evenly across partitions without hashing.
//! Use only for non-aggregated queries where state consistency is not required.
//!
//! ## Performance Profile
//!
//! - **Throughput**: Maximum (no hashing overhead)
//! - **Latency**: Low
//! - **State Consistency**: ❌ NOT GUARANTEED
//!
//! ## ⚠️ WARNING
//!
//! This strategy BREAKS stateful aggregations like GROUP BY!
//! Only use for pass-through filtering and non-stateful transformations.

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::StreamRecord;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{PartitioningStrategy, QueryMetadata, RoutingContext};

/// Round-robin partitioning strategy
///
/// Distributes records evenly across partitions without hashing.
/// Maximum throughput but loses state locality.
///
/// ⚠️ Only use for non-aggregated queries!
pub struct RoundRobinStrategy {
    /// Current partition counter for round-robin distribution
    next_partition: Arc<AtomicUsize>,
}

impl RoundRobinStrategy {
    /// Create new round-robin strategy
    pub fn new() -> Self {
        Self {
            next_partition: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Default for RoundRobinStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitioningStrategy for RoundRobinStrategy {
    fn route_record(
        &self,
        _record: &StreamRecord,
        context: &RoutingContext,
    ) -> Result<usize, SqlError> {
        // Simple round-robin: use atomic counter modulo partition count
        let partition_id =
            self.next_partition.fetch_add(1, Ordering::Relaxed) % context.num_partitions;
        Ok(partition_id)
    }

    fn name(&self) -> &str {
        "RoundRobin"
    }

    fn version(&self) -> &str {
        "v1"
    }

    fn validate(&self, metadata: &QueryMetadata) -> Result<(), String> {
        // Round-robin works with or without GROUP BY
        // But WARN if used with GROUP BY (indicates user error)
        if !metadata.group_by_columns.is_empty() {
            return Err(
                "⚠️ WARNING: RoundRobinStrategy breaks GROUP BY aggregations! \
                 Use AlwaysHashStrategy or SmartRepartitionStrategy instead."
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_strategy_creates() {
        let strategy = RoundRobinStrategy::new();
        assert_eq!(strategy.name(), "RoundRobin");
        assert_eq!(strategy.version(), "v1");
    }

    #[test]
    fn test_round_robin_strategy_validates() {
        let strategy = RoundRobinStrategy::new();

        // Valid: no GROUP BY (pass-through use case)
        let valid_metadata = QueryMetadata {
            group_by_columns: vec![],
            has_window: false,
            num_partitions: 8,
            num_cpu_slots: 8,
        };
        assert!(strategy.validate(&valid_metadata).is_ok());

        // Invalid: has GROUP BY (stateful aggregation)
        let invalid_metadata = QueryMetadata {
            group_by_columns: vec!["trader_id".to_string()],
            has_window: false,
            num_partitions: 8,
            num_cpu_slots: 8,
        };
        assert!(strategy.validate(&invalid_metadata).is_err());
    }

    #[test]
    fn test_round_robin_strategy_default() {
        let strategy = RoundRobinStrategy::default();
        assert_eq!(strategy.name(), "RoundRobin");
    }

    #[tokio::test]
    async fn test_round_robin_distribution() {
        use crate::velostream::sql::execution::types::StreamRecord;
        use std::collections::HashMap;

        let strategy = RoundRobinStrategy::new();

        // Test that 8 consecutive records distribute across all 8 partitions
        let routing_context = RoutingContext {
            source_partition: None,
            source_partition_key: None,
            group_by_columns: vec![],
            num_partitions: 8,
            num_cpu_slots: 8,
        };

        let mut partitions = vec![];
        for _ in 0..8 {
            let record = StreamRecord::new(HashMap::new());
            let partition = strategy.route_record(&record, &routing_context).unwrap();
            partitions.push(partition);
        }

        // Should have cycled through all 8 partitions
        assert_eq!(partitions, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_round_robin_wraps_around() {
        use crate::velostream::sql::execution::types::StreamRecord;
        use std::collections::HashMap;

        let strategy = RoundRobinStrategy::new();

        let routing_context = RoutingContext {
            source_partition: None,
            source_partition_key: None,
            group_by_columns: vec![],
            num_partitions: 4,
            num_cpu_slots: 4,
        };

        // Test 10 records with 4 partitions (should wrap around)
        let mut partitions = vec![];
        for _ in 0..10 {
            let record = StreamRecord::new(HashMap::new());
            let partition = strategy.route_record(&record, &routing_context).unwrap();
            partitions.push(partition);
        }

        // Should cycle: 0,1,2,3,0,1,2,3,0,1
        assert_eq!(partitions, vec![0, 1, 2, 3, 0, 1, 2, 3, 0, 1]);
    }
}
