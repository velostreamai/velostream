# Fan-In Partitioning Strategy

## Overview

The **Fan-In** partitioning strategy concentrates all records into a single partition (partition 0 by default) for global aggregation. This strategy is essential for computing aggregations that span the entire dataset, such as global `COUNT(*)`, `SUM()`, and `AVG()` across all partitions.

## Motivation

In a distributed streaming system, data is normally partitioned across multiple partitions to enable parallel processing. However, some queries require combining data from ALL partitions to compute results:

- **Global Aggregations**: `SELECT COUNT(*) FROM table` requires counting all records regardless of partition
- **Final Aggregation Stage**: Multi-stage aggregations often need a final stage where results from all partitions are combined
- **Cross-Partition Analytics**: Business metrics that require viewing data across all groups

Without Fan-In, these operations would be impossible or would require external aggregation logic.

## Design

### Architecture

```
Record Stream
     │
     ├─ Source Partition 0 ──┐
     ├─ Source Partition 1 ──┤
     ├─ Source Partition 2 ──┼──> Fan-In Router
     └─ Source Partition N ──┘
                    │
                    ▼
            Target Partition 0
            (All records end up here)
                    │
                    ▼
              Global Aggregator
```

### Implementation Details

The FanInStrategy implementation:

```rust
pub struct FanInStrategy {
    target_partition: usize,  // Default: 0
}

impl PartitioningStrategy for FanInStrategy {
    async fn route_record(
        &self,
        _record: &StreamRecord,
        context: &RoutingContext,
    ) -> Result<usize, SqlError> {
        // Simply return target partition for all records
        if self.target_partition >= context.num_partitions {
            return Err(/* out of bounds */);
        }
        Ok(self.target_partition)
    }
}
```

**Key Characteristics**:
- **Deterministic Routing**: Every record routes to the same partition
- **No Hashing**: Constant-time decision (O(1))
- **No Grouping Dependencies**: Does NOT require GROUP BY columns
- **Optional Custom Target**: Can target any valid partition, defaults to 0

## Usage

### Configuration

```yaml
# In velostream configuration
partitioning_strategy: "fan_in"

# Or programmatically
let strategy = FanInStrategy::new();
// Or target a specific partition:
let strategy = FanInStrategy::with_target(2);
```

### SQL Examples

**Global Count**:
```sql
SELECT COUNT(*) as total_records
FROM market_data
-- All records routed to partition 0 for counting
```

**Global Aggregation with Window**:
```sql
SELECT
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price
FROM market_data
WINDOW TUMBLING (event_time, INTERVAL '1' MINUTE)
-- Uses Fan-In for final aggregation stage
```

**Multi-Stage Pipeline** (with smart partitioning for initial aggregation, Fan-In for final):
```sql
-- Stage 1: Pre-aggregate by trader
SELECT trader_id, COUNT(*) as trader_count
FROM market_data
GROUP BY trader_id

-- Stage 2: Combine trader results (uses Fan-In)
SELECT SUM(trader_count) as total_traders
FROM trader_aggregates
-- All records routed to partition 0 for final sum
```

## Performance Characteristics

### Throughput

- **Single Partition Bottleneck**: Limited to ~200K records/sec (single partition capacity)
- **Network Efficiency**: Minimal network overhead (no hashing calculations)
- **Memory Efficient**: No state distribution needed

### Latency

- **Record-Level Latency**: Minimal routing overhead (O(1))
- **Global Latency**: Depends on buffer/batch size in target partition
- **Scaling**: Does NOT scale with partition count (intentional design)

### Comparative Performance

| Metric | Value |
|--------|-------|
| Routing Decision | O(1) constant time |
| Network Transfers | Minimal (no hashing) |
| Per-Partition Throughput | ~200K rec/sec max |
| State Distribution | None (all in one partition) |

## When to Use Fan-In

### ✅ Ideal Use Cases

1. **Global Aggregations**
   ```sql
   SELECT COUNT(*) FROM events  -- Single global count
   ```

2. **Cross-Partition Statistics**
   ```sql
   SELECT SUM(revenue) as total_revenue
   FROM transactions
   ```

3. **Final Aggregation in Multi-Stage Pipelines**
   - Stage 1: Distributed group-by aggregation
   - Stage 2: Fan-In combines stage 1 results

4. **Global Top-K Queries** (with sorting after collection)
   ```sql
   SELECT * FROM logs
   ORDER BY timestamp DESC
   LIMIT 10
   ```

5. **Windowed Global Analytics**
   ```sql
   SELECT COUNT(*) over_last_minute
   FROM events
   WINDOW TUMBLING (event_time, INTERVAL '1' MINUTE)
   ```

### ❌ Avoid For

- **High-Throughput Streaming**: Single partition becomes bottleneck
- **Stateful Operations Without Pre-Aggregation**: Must pre-aggregate first
- **Operations Requiring State Consistency**: Use AlwaysHash instead
- **Load-Balanced Distribution**: Use SmartRepartition or StickyPartition

## Tradeoffs

### Advantages ✅
- **Simple Implementation**: Straightforward routing logic
- **Zero Computation**: No hashing or complex decisions
- **Correct Results**: Mathematically correct for all global aggregations
- **Predictable Behavior**: Deterministic single-partition routing

### Disadvantages ❌
- **Throughput Limited**: Single partition caps at ~200K rec/sec
- **No Scaling**: Cannot scale beyond single partition capacity
- **Data Bottleneck**: All data must flow through one partition
- **Not Suitable for Continuous Processing**: Without pre-aggregation phase

## Best Practices

### 1. Pre-Aggregation Pattern

For high-volume streams, use multi-stage processing:

```sql
-- Stage 1: Distributed pre-aggregation (use AlwaysHash)
CREATE TABLE stage1_agg AS
SELECT trader_id, COUNT(*) as trade_count
FROM market_data
GROUP BY trader_id;

-- Stage 2: Global aggregation (use Fan-In)
SELECT SUM(trade_count) as total_trades
FROM stage1_agg;
```

**Benefits**:
- Stage 1 distributes load across partitions
- Stage 2 operates on pre-aggregated data (much smaller volume)
- Total throughput: ~1.5M rec/sec (distributed) + ~200K rec/sec (global)

### 2. Combine with Windows

```sql
-- Pre-aggregate by window and group
SELECT
    trader_id,
    window_start,
    COUNT(*) as count,
    SUM(quantity) as total
FROM market_data
GROUP BY trader_id, window_start
WINDOW TUMBLING (event_time, INTERVAL '1' MINUTE)

-- Then apply Fan-In for final aggregation across traders
```

### 3. Monitor Backpressure

Since all data flows to one partition, watch for:

```rust
// Monitor the target partition for backpressure
let metrics = partition_managers[0].metrics();
if metrics.queue_depth > buffer_size * 0.8 {
    // Consider reducing input rate or adding pre-aggregation stage
}
```

## Implementation Notes

### Thread Safety

- FanInStrategy is stateless and thread-safe
- Route decision is deterministic across threads
- Safe for concurrent record routing

### Async Support

- Fully async-compatible with tokio runtime
- Non-blocking route_record operation
- Works with async channel sends

### Error Handling

```rust
// Bounds checking
if self.target_partition >= context.num_partitions {
    return Err(SqlError::ConfigurationError {
        message: format!("Invalid target partition {}", self.target_partition)
    });
}
```

## Metrics and Observability

Track Fan-In performance with:

```rust
// Monitor target partition
let partition_metrics = coordinator.partition_metrics(0);
println!("Target Partition Throughput: {} rec/sec",
    partition_metrics.records_per_second);
println!("Queue Depth: {}", partition_metrics.queue_depth);
println!("Backpressure: {}", partition_metrics.backpressure_state);
```

## Examples

### Example 1: Simple Global Count

```rust
let strategy = FanInStrategy::new();
let coordinator = PartitionedJobCoordinator::new(config)
    .with_strategy(Arc::new(strategy));

// All records routed to partition 0
let result = coordinator.process_batch_with_strategy(records, &senders).await;
// Result: Single global count computed in partition 0
```

### Example 2: Custom Target Partition

```rust
// Route all records to partition 3 instead of 0
let strategy = FanInStrategy::with_target(3);
let coordinator = PartitionedJobCoordinator::new(config)
    .with_strategy(Arc::new(strategy));
```

### Example 3: Multi-Stage Pipeline

```rust
// Stage 1: Distributed aggregation
let distributed_strategy = Arc::new(AlwaysHashStrategy::new());
let stage1_coordinator = PartitionedJobCoordinator::new(config)
    .with_group_by_columns(vec!["trader_id".to_string()])
    .with_strategy(distributed_strategy);

// Stage 2: Global aggregation
let fan_in_strategy = Arc::new(FanInStrategy::new());
let stage2_coordinator = PartitionedJobCoordinator::new(config)
    .with_strategy(fan_in_strategy);

// Process stage 1 in parallel across partitions,
// then feed results to stage 2 for global aggregation
```

## Configuration

### Strategy Selection

```yaml
# YAML configuration
job:
  partitioning_strategy: "fan_in"
  # Optional: specify custom target partition
  # target_partition: 0
```

### Programmatic Configuration

```rust
use velostream::server::v2::{StrategyConfig, StrategyFactory};

// From config enum
let strategy = StrategyFactory::create(StrategyConfig::FanIn)?;

// From string
let strategy = StrategyFactory::create_from_str("fan_in")?;

// With custom target
let strategy = FanInStrategy::with_target(2);
```

## See Also

- [AlwaysHash Strategy](./PARTITIONING_STRATEGIES.md#alwayshash) - Safe default with hashing
- [SmartRepartition Strategy](./PARTITIONING_STRATEGIES.md#smartrepartition) - Optimized for aligned keys
- [StickyPartition Strategy](./PARTITIONING_STRATEGIES.md#stickypartition) - Minimizes data movement
- [Job Server V2 Architecture](./FR-082-JOB-SERVER-V2-PARTITIONED-PIPELINE.md)

## Troubleshooting

### Q: My global count is not updating quickly?

**A**: Fan-In concentrates all data into one partition, creating a bottleneck. If you need faster updates:
1. Use a pre-aggregation stage to reduce volume
2. Consider materialized views with periodic updates
3. Accept that global aggregations have throughput limits

### Q: Can I change the target partition dynamically?

**A**: Currently, target partition is fixed at FanInStrategy creation. For dynamic routing, you would need to:
1. Create multiple strategies with different targets
2. Implement a wrapper strategy that selects based on current load
3. Submit a feature request for dynamic target support

### Q: Is Fan-In suitable for my use case?

**A**: Ask yourself:
- Do I need a GLOBAL (across all data) aggregation? → Use Fan-In
- Do I need grouped aggregations? → Use AlwaysHash
- Is throughput critical (>200K rec/sec)? → Use pre-aggregation + Fan-In

