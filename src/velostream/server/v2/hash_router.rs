//! Hash-based record routing for partitioned execution
//!
//! Routes records to partitions based on GROUP BY key hashing for deterministic distribution.

use crate::velostream::sql::ast::Expr;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::aggregation::state::GroupByStateManager;
use crate::velostream::sql::execution::internal::GroupKey;
use crate::velostream::sql::execution::types::StreamRecord;

/// Partition routing strategy
#[derive(Debug, Clone)]
pub enum PartitionStrategy {
    /// Hash by GROUP BY columns (standard case)
    HashByGroupBy { group_by_columns: Vec<Expr> },

    /// Broadcast small lookup table to all partitions (stream-table JOIN)
    BroadcastJoin { replicated_table: String },

    /// Round-robin distribution (no GROUP BY)
    RoundRobin,
}

/// Routes records to partitions using consistent hashing
///
/// ## Phase 1 Implementation
///
/// Supports GROUP BY hash routing for pure aggregation queries:
///
/// ```sql
/// SELECT trader_id, symbol, COUNT(*), AVG(price)
/// FROM trades
/// GROUP BY trader_id, symbol
/// ```
///
/// Records with same (trader_id, symbol) always route to same partition.
///
/// ## Future Enhancements (Phase 2+)
///
/// - Broadcast JOIN support (replicate small tables)
/// - CPU affinity pinning per partition
/// - Hot key detection and mitigation (salted keys)
pub struct HashRouter {
    num_partitions: usize,
    strategy: PartitionStrategy,
}

impl HashRouter {
    /// Create new hash router with specified partition count
    pub fn new(num_partitions: usize, strategy: PartitionStrategy) -> Self {
        assert!(num_partitions > 0, "Must have at least 1 partition");
        Self {
            num_partitions,
            strategy,
        }
    }

    /// Route a record to its target partition
    ///
    /// Returns partition ID in range [0, num_partitions)
    pub fn route_record(&self, record: &StreamRecord) -> Result<usize, SqlError> {
        match &self.strategy {
            PartitionStrategy::HashByGroupBy { group_by_columns } => {
                self.route_by_group_key(group_by_columns, record)
            }
            PartitionStrategy::BroadcastJoin { .. } => {
                // Broadcast to all partitions (handled by coordinator)
                // For now, route to partition 0 as placeholder
                Ok(0)
            }
            PartitionStrategy::RoundRobin => {
                // Round-robin distribution
                // Use a simple counter (would need to be Arc<AtomicUsize> in production)
                Ok(0) // Placeholder for Phase 1
            }
        }
    }

    /// Route record by GROUP BY key hash (Phase 1 implementation)
    fn route_by_group_key(
        &self,
        group_by_columns: &[Expr],
        record: &StreamRecord,
    ) -> Result<usize, SqlError> {
        // Generate group key using existing SQL engine logic
        let group_key = GroupByStateManager::generate_group_key(group_by_columns, record)?;

        // Compute partition from group key hash
        let partition_id = self.hash_group_key(&group_key) % self.num_partitions;

        Ok(partition_id)
    }

    /// Get hash of group key
    ///
    /// GroupKey already contains a pre-computed hash using FxHasher for performance.
    fn hash_group_key(&self, group_key: &GroupKey) -> usize {
        group_key.hash() as usize
    }

    /// Get number of partitions
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }
}
