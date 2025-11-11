//! Pluggable partitioning strategies for V2 architecture
//!
//! Different strategies optimize for different data characteristics:
//! - StickyPartitionStrategy: Default - minimizes data movement, latency-optimized
//! - AlwaysHashStrategy: Conservative, always correct
//! - SmartRepartitionStrategy: Optimized for naturally partitioned data
//! - RoundRobinStrategy: Maximum throughput for non-grouped queries
//!
//! See: docs/feature/FR-082-perf-part-2/V2-PLUGGABLE-PARTITIONING-STRATEGIES.md

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::StreamRecord;
use async_trait::async_trait;
use std::collections::HashMap;

/// Metadata about the query for strategy validation
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    /// GROUP BY columns if present
    pub group_by_columns: Vec<String>,

    /// Whether query has window function
    pub has_window: bool,

    /// Number of partitions to route to
    pub num_partitions: usize,

    /// Number of CPU slots available
    pub num_cpu_slots: usize,
}

/// Context provided to routing strategy
#[derive(Debug, Clone)]
pub struct RoutingContext {
    /// Source partition if known (from Kafka, etc.)
    pub source_partition: Option<usize>,

    /// Source partition key if known (e.g., "trader_id")
    pub source_partition_key: Option<String>,

    /// Query GROUP BY columns
    pub group_by_columns: Vec<String>,

    /// Number of partitions
    pub num_partitions: usize,

    /// Number of CPU cores/slots
    pub num_cpu_slots: usize,
}

/// Trait for different partitioning strategies
///
/// Implementations:
/// - AlwaysHashStrategy: Safe, always correct
/// - SmartRepartitionStrategy: Optimized for aligned data (Phase 2)
/// - RoundRobinStrategy: Maximum throughput (Phase 3)
#[async_trait]
pub trait PartitioningStrategy: Send + Sync {
    /// Route a record to a partition
    ///
    /// Returns partition_id in range [0, num_partitions)
    async fn route_record(
        &self,
        record: &StreamRecord,
        context: &RoutingContext,
    ) -> Result<usize, SqlError>;

    /// Get strategy name for logging and metrics
    fn name(&self) -> &str;

    /// Get strategy version
    fn version(&self) -> &str;

    /// Validate strategy is compatible with query
    fn validate(&self, metadata: &QueryMetadata) -> Result<(), String>;
}

/// Conservative strategy: Always hash GROUP BY keys
///
/// **Guarantee**: Always correct, state never fragments
/// **Cost**: Hash computation on every record
/// **Performance**: ~191K rec/sec
///
/// Use when:
/// - You need 100% correctness guarantee
/// - Source partitioning is unknown
/// - Safety-first requirement
pub struct AlwaysHashStrategy;

impl AlwaysHashStrategy {
    /// Create new AlwaysHashStrategy
    pub fn new() -> Self {
        Self {}
    }

    /// Hash GROUP BY key values using FNV-1a hash
    fn hash_group_key(&self, key_values: &[&str]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for value in key_values {
            for byte in value.as_bytes() {
                hash ^= *byte as u64;
                hash = hash.wrapping_mul(FNV_PRIME);
            }
        }
        hash
    }
}

impl Default for AlwaysHashStrategy {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PartitioningStrategy for AlwaysHashStrategy {
    async fn route_record(
        &self,
        record: &StreamRecord,
        context: &RoutingContext,
    ) -> Result<usize, SqlError> {
        // Extract GROUP BY column values from record
        let mut key_values = Vec::with_capacity(context.group_by_columns.len());

        for column in &context.group_by_columns {
            let value = record
                .get_field(column)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!(
                        "AlwaysHashStrategy: Column '{}' not found in record",
                        column
                    ),
                    query: None,
                })?
                .to_display_string();

            key_values.push(value);
        }

        // Hash the combined key
        let key_refs: Vec<&str> = key_values.iter().map(|s| s.as_str()).collect();
        let hash = self.hash_group_key(&key_refs);

        // Map to partition
        let partition_id = (hash as usize) % context.num_partitions;

        Ok(partition_id)
    }

    fn name(&self) -> &str {
        "AlwaysHash"
    }

    fn version(&self) -> &str {
        "v1"
    }

    fn validate(&self, metadata: &QueryMetadata) -> Result<(), String> {
        if metadata.group_by_columns.is_empty() {
            return Err(
                "AlwaysHashStrategy requires GROUP BY columns for proper aggregation".to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_always_hash_strategy_creates() {
        let strategy = AlwaysHashStrategy::new();
        assert_eq!(strategy.name(), "AlwaysHash");
        assert_eq!(strategy.version(), "v1");
    }

    #[test]
    fn test_always_hash_strategy_validates() {
        let strategy = AlwaysHashStrategy::new();

        // Valid: has GROUP BY columns
        let valid_metadata = QueryMetadata {
            group_by_columns: vec!["trader_id".to_string()],
            has_window: false,
            num_partitions: 8,
            num_cpu_slots: 8,
        };
        assert!(strategy.validate(&valid_metadata).is_ok());

        // Invalid: no GROUP BY columns
        let invalid_metadata = QueryMetadata {
            group_by_columns: vec![],
            has_window: false,
            num_partitions: 8,
            num_cpu_slots: 8,
        };
        assert!(strategy.validate(&invalid_metadata).is_err());
    }

    #[test]
    fn test_always_hash_deterministic_routing() {
        let strategy = AlwaysHashStrategy::new();

        // Same key should always route to same partition
        let key1 = vec!["trader_1"];
        let key2 = vec!["trader_1"];
        let key3 = vec!["trader_2"];

        let hash1 = strategy.hash_group_key(&key1);
        let hash2 = strategy.hash_group_key(&key2);
        let hash3 = strategy.hash_group_key(&key3);

        assert_eq!(hash1, hash2, "Same key should produce same hash");
        assert_ne!(
            hash1, hash3,
            "Different keys should produce different hashes"
        );

        // Verify modulo consistency
        assert_eq!(hash1 as usize % 8, hash2 as usize % 8);
        assert_ne!(hash1 as usize % 8, hash3 as usize % 8);
    }
}
