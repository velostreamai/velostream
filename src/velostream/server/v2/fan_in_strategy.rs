//! Fan-In Partitioning Strategy
//!
//! Routes all records to a single partition (partition 0) for global aggregation.
//!
//! **Use Cases**:
//! - Global COUNT(*) aggregations
//! - Cross-partition SUM/AVG calculations
//! - Windowed aggregations that need all data in one partition
//! - Final aggregation stages after distributed processing
//!
//! **Performance Characteristics**:
//! - Throughput: Limited by single partition capacity (~200K rec/sec)
//! - Latency: Minimal record-level latency (no hashing)
//! - Scalability: Does not scale with partition count (intentional)
//!
//! **Trade-offs**:
//! - ✅ Simple, fast (no hashing)
//! - ✅ Correct for all global aggregations
//! - ❌ Single partition bottleneck (cannot exceed partition capacity)
//! - ❌ Not suitable for ongoing stream processing without pre-aggregation

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::StreamRecord;

use super::{PartitioningStrategy, QueryMetadata, RoutingContext};

/// Fan-in strategy that concentrates all records into a single partition
///
/// Routes all records to partition 0, suitable for final aggregation stages
/// where cross-partition data concentration is required.
pub struct FanInStrategy {
    target_partition: usize,
}

impl FanInStrategy {
    /// Create a new FanInStrategy
    ///
    /// # Arguments
    /// * `target_partition` - The partition to concentrate all records into (default: 0)
    pub fn new() -> Self {
        Self {
            target_partition: 0,
        }
    }

    /// Create a FanInStrategy targeting a specific partition
    pub fn with_target(target_partition: usize) -> Self {
        Self { target_partition }
    }
}

impl Default for FanInStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitioningStrategy for FanInStrategy {
    fn route_record(
        &self,
        _record: &StreamRecord,
        context: &RoutingContext,
    ) -> Result<usize, SqlError> {
        // Validate target partition is within bounds
        if self.target_partition >= context.num_partitions {
            return Err(SqlError::ConfigurationError {
                message: format!(
                    "Fan-in target partition {} exceeds partition count {}",
                    self.target_partition, context.num_partitions
                ),
            });
        }

        Ok(self.target_partition)
    }

    fn name(&self) -> &str {
        "FanIn"
    }

    fn version(&self) -> &str {
        "1.0"
    }

    fn validate(&self, metadata: &QueryMetadata) -> Result<(), String> {
        // Validate that target partition is within bounds
        if self.target_partition >= metadata.num_partitions {
            return Err(format!(
                "Fan-in strategy: target partition {} exceeds partition count {}",
                self.target_partition, metadata.num_partitions
            ));
        }

        // Warn if used with complex queries (not ideal for stateful operations)
        if metadata.has_window && !metadata.group_by_columns.is_empty() {
            // This is actually fine - fan-in is good for final windowed aggregations
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fan_in_strategy_routes_to_partition_zero() {
        let strategy = FanInStrategy::new();
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "id".to_string(),
            crate::velostream::sql::execution::types::FieldValue::Integer(1),
        );
        let record = StreamRecord::new(fields);

        let context = RoutingContext {
            source_partition: Some(0),
            source_partition_key: None,
            group_by_columns: vec!["id".to_string()],
            num_partitions: 4,
            num_cpu_slots: 4,
        };

        let partition = strategy.route_record(&record, &context);
        assert!(partition.is_ok());
        assert_eq!(partition.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_fan_in_strategy_consistent_routing() {
        let strategy = FanInStrategy::new();

        let context = RoutingContext {
            source_partition: Some(0),
            source_partition_key: None,
            group_by_columns: vec!["id".to_string()],
            num_partitions: 8,
            num_cpu_slots: 8,
        };

        // All records should route to the same partition
        for i in 0..10 {
            let mut fields = std::collections::HashMap::new();
            fields.insert(
                "id".to_string(),
                crate::velostream::sql::execution::types::FieldValue::Integer(i),
            );
            let record = StreamRecord::new(fields);

            let partition = strategy.route_record(&record, &context);
            assert_eq!(partition.unwrap(), 0);
        }
    }

    #[tokio::test]
    async fn test_fan_in_strategy_with_custom_target() {
        let strategy = FanInStrategy::with_target(2);

        let context = RoutingContext {
            source_partition: Some(0),
            source_partition_key: None,
            group_by_columns: vec![],
            num_partitions: 4,
            num_cpu_slots: 4,
        };

        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "value".to_string(),
            crate::velostream::sql::execution::types::FieldValue::Integer(42),
        );
        let record = StreamRecord::new(fields);

        let partition = strategy.route_record(&record, &context);
        assert_eq!(partition.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_fan_in_strategy_bounds_check() {
        let strategy = FanInStrategy::with_target(10);

        let context = RoutingContext {
            source_partition: Some(0),
            source_partition_key: None,
            group_by_columns: vec![],
            num_partitions: 4,
            num_cpu_slots: 4,
        };

        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "value".to_string(),
            crate::velostream::sql::execution::types::FieldValue::Integer(42),
        );
        let record = StreamRecord::new(fields);

        let partition = strategy.route_record(&record, &context);
        assert!(partition.is_err());
    }

    #[test]
    fn test_fan_in_strategy_validation() {
        let strategy = FanInStrategy::new();
        let metadata = QueryMetadata {
            group_by_columns: vec!["category".to_string()],
            has_window: true,
            num_partitions: 4,
            num_cpu_slots: 4,
        };

        let result = strategy.validate(&metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fan_in_strategy_invalid_partition() {
        let strategy = FanInStrategy::with_target(10);
        let metadata = QueryMetadata {
            group_by_columns: vec![],
            has_window: false,
            num_partitions: 4,
            num_cpu_slots: 4,
        };

        let result = strategy.validate(&metadata);
        assert!(result.is_err());
    }
}
