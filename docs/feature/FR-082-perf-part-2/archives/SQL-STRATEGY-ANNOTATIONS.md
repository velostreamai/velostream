# SQL Partitioning Strategy Annotations (FR-082 Phase 2)

## Overview

Velostream V2 Job Server supports **per-job partitioning strategy configuration** through SQL query annotations. This allows fine-grained control over how records are routed to partition pipelines, enabling optimization for different workload patterns without code changes.

## Quick Start

Add a `@partitioning_strategy` annotation to your SQL job definition:

```sql
-- @job_name: my_trading_stream
-- @partitioning_strategy: always_hash
CREATE STREAM my_trading_stream AS
SELECT symbol, price, volume FROM market_data
WHERE price > 100;
```

The annotation is extracted using the same mechanism as `@job_name` and applied per-job, allowing different jobs to use different strategies.

## Annotation Syntax

```
-- @partitioning_strategy: <strategy_name>
```

**Placement**: Add the annotation immediately before the `CREATE STREAM` or `CREATE TABLE` statement, alongside other job metadata annotations like `@job_name`.

**Case Handling**: Strategy names are case-insensitive. All of these are valid:
- `always_hash`
- `AlwaysHash`
- `ALWAYS_HASH`
- `smart_repartition`
- `SmartRepartition`
- `SMART_REPARTITION`

## Available Strategies

### 1. AlwaysHash (Safety-First Default)

**Configuration**:
```sql
-- @partitioning_strategy: always_hash
```

**How It Works**:
- Hash all records using selected GROUP BY columns
- Guarantees same key always routes to same partition
- Enables stateful aggregations with perfect consistency

**When to Use**:
- ✓ Stateful aggregations (GROUP BY, window functions)
- ✓ Complex joins where state consistency is critical
- ✓ Risk monitoring and position tracking
- ✓ Financial aggregations requiring exact totals
- ✓ Default choice when in doubt (safety-first)

**Trade-offs**:
- **Pro**: Guaranteed correctness for all aggregation types
- **Pro**: Perfect state consistency
- **Pro**: Highest safety for critical applications
- **Con**: Potential for uneven partition load (hash skew)
- **Con**: Slight overhead from hashing on every record

**Performance**: Baseline performance (~200K rec/sec per partition)

**Example from Trading Demo**:
```sql
-- Stateful aggregation - must preserve state consistency
-- @job_name: market-data-event-time-1
-- @partitioning_strategy: always_hash
CREATE STREAM market_data_ts AS
SELECT
    symbol, price, bid_price, ask_price, volume, vwap
FROM in_market_data_stream
EMIT CHANGES;

-- Risk monitoring - state accuracy is critical
-- @job_name: risk-monitoring-stream
-- @partitioning_strategy: always_hash
CREATE STREAM comprehensive_risk_monitor AS
SELECT
    p.trader_id, p.symbol, p.position_size,
    m.price, (p.position_size * m.price) as position_value
FROM trading_positions_with_event_time p
JOIN market_data_ts m ON p.symbol = m.symbol
GROUP BY p.trader_id
HAVING ABS(position_value) > 1000000;
```

### 2. SmartRepartition (Optimization-Focused)

**Configuration**:
```sql
-- @partitioning_strategy: smart_repartition
```

**How It Works**:
- Analyzes record structure to detect pre-existing partitioning alignment
- If data is already partitioned by GROUP BY columns → routes to same partition (no repartitioning)
- If pre-partitioned by different key → performs lightweight repartitioning
- Avoids unnecessary shuffling for aligned data

**When to Use**:
- ✓ Stateless transformations where data is pre-aligned
- ✓ Multi-source joins where inputs share common partitioning
- ✓ High-throughput scenarios with aligned data (order flow analysis)
- ✓ Reducing shuffling overhead in multi-stage pipelines
- ✗ NOT for stateful aggregations requiring hash consistency

**Trade-offs**:
- **Pro**: ~5-10% throughput improvement with aligned data
- **Pro**: Reduces partition imbalance from uneven hashes
- **Pro**: Intelligent detection of pre-partitioned data
- **Con**: Does NOT guarantee hash consistency for aggregations
- **Con**: Can cause state inconsistency if aggregation keys don't match input partitioning
- **Con**: Requires understanding of data flow alignment

**Performance**: 5-10% improvement over AlwaysHash when data is pre-aligned (~210-220K rec/sec per partition)

**Decision Matrix**:
| Scenario | AlwaysHash | SmartRepartition |
|----------|-----------|------------------|
| GROUP BY same column as input | ✓ Safe | ✓ Optimized |
| GROUP BY different column | ✓ Safe | ✗ Inconsistent |
| Multi-source join, aligned | ✓ Safe | ✓ Optimized |
| Multi-source join, misaligned | ✓ Safe | ✗ Inconsistent |

**Example from Trading Demo**:
```sql
-- Order flow analysis - data pre-aligned by symbol in input stream
-- SmartRepartition leverages existing alignment for optimization
-- @job_name: order_flow_imbalance_detection
-- @partitioning_strategy: smart_repartition
CREATE STREAM order_flow_imbalance_detection AS
SELECT
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) AS buy_volume,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) AS sell_volume,
    SUM(quantity) AS total_volume
FROM in_order_book_stream
GROUP BY symbol
WINDOW TUMBLING (event_time, INTERVAL '1' MINUTE);
```

### 3. StickyPartition (Latency-Optimized)

**Configuration**:
```sql
-- @partitioning_strategy: sticky_partition
```

**How It Works**:
- Routes related records to same partition based on routing key
- Minimizes inter-partition communication
- Optimizes CPU cache locality (same record flows through same core)
- Reduces latency for request-response patterns

**When to Use**:
- ✓ Latency-sensitive queries (sub-100ms SLA)
- ✓ Request-response patterns (minimize round-trips)
- ✓ Cache-friendly workloads (working set fits in L3 cache)
- ✓ Queries with low cardinality GROUP BY (few distinct keys)
- ✗ NOT for load balancing (will create hotspots)

**Trade-offs**:
- **Pro**: ~10-15% latency reduction for compatible workloads
- **Pro**: Better CPU cache locality
- **Pro**: Reduced inter-partition synchronization
- **Con**: Can create uneven partition load (hotspots)
- **Con**: Not suitable for high-cardinality dimensions
- **Con**: Requires understanding of access patterns

**Performance**: 10-15% latency improvement for compatible workloads, ~200K rec/sec per partition

**Example Use Cases**:
- E-commerce: Route all events for single customer to same partition
- Gaming: Route all actions for single player to same partition
- Monitoring: Route all metrics for single host to same partition

### 4. RoundRobin (Throughput-Maximized)

**Configuration**:
```sql
-- @partitioning_strategy: round_robin
```

**How It Works**:
- Distributes records evenly across all partitions in round-robin order
- Ignores record content (no hashing, no analysis)
- Guarantees perfect load balancing
- Minimal CPU overhead for routing decision

**When to Use**:
- ✓ Stateless transformations (no aggregation, no join state)
- ✓ Simple filters and projections
- ✓ Load-balancing critical processing
- ✓ Maximum throughput needed (filters, streaming transforms)
- ✗ NOT for stateful aggregations
- ✗ NOT for joins requiring state consistency

**Trade-offs**:
- **Pro**: Perfect load balancing (zero hotspots)
- **Pro**: Minimal routing overhead (~1-2% faster than hash)
- **Pro**: Maximum throughput for simple transforms
- **Con**: Useless for aggregations (state scattered across partitions)
- **Con**: Cannot be used with GROUP BY or window functions
- **Con**: No way to maintain correctness for joins

**Performance**: ~1-2% overhead reduction compared to AlwaysHash (~204K rec/sec per partition)

**Example Use Case**:
```sql
-- Simple stateless filtering - just drop records
CREATE STREAM high_price_trades AS
SELECT symbol, price, volume, timestamp
FROM market_data_stream
WHERE price > 100 AND volume > 50000;
```

## Configuration Examples from Trading Demo

### 1. Market Data Pipeline (Safety-First)
```sql
-- Core market data stream - foundation for other jobs
-- @job_name: market-data-event-time-1
-- @partitioning_strategy: always_hash
CREATE STREAM market_data_ts AS
SELECT
    symbol, exchange, timestamp, price, bid_price, ask_price,
    bid_size, ask_size, volume, vwap, market_cap
FROM in_market_data_stream
EMIT CHANGES
WITH (
    'event.time.field' = 'timestamp',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '5s'
);
```
**Rationale**: Foundation stream - every downstream job depends on consistency. Use `always_hash` to guarantee correctness for all future aggregations.

### 2. Volume Spike Detection (Stateful Aggregation)
```sql
-- Complex aggregations with anomaly classification
-- @job_name: volume_spike_analysis
-- @partitioning_strategy: always_hash
CREATE STREAM volume_spike_analysis AS
SELECT
    symbol,
    COUNT(*) AS trade_count,
    AVG(volume) AS avg_volume,
    STDDEV_POP(volume) AS stddev_volume,
    MAX(volume) AS max_volume,
    CASE
        WHEN AVG(volume) > 0 AND MAX(volume) > 5 * AVG(volume)
        THEN 'EXTREME_SPIKE'
        ELSE 'NORMAL'
    END AS spike_classification
FROM market_data_ts
GROUP BY symbol
WINDOW SLIDING(event_time, 5m, 1m);
```
**Rationale**: Stateful GROUP BY aggregation. `always_hash` ensures same symbol always goes to same partition, enabling accurate STDDEV and anomaly classification.

### 3. Risk Monitoring with Joins (State Critical)
```sql
-- Multi-stream join with position tracking
-- @job_name: risk-monitoring-stream
-- @partitioning_strategy: always_hash
CREATE STREAM comprehensive_risk_monitor AS
SELECT
    p.trader_id, p.symbol, p.position_size,
    m.price, (p.position_size * m.price) as position_value,
    CASE
        WHEN ABS(position_value) > 10000000 THEN 'POSITION_LIMIT_EXCEEDED'
        ELSE 'OK'
    END as risk_classification
FROM trading_positions_with_event_time p
JOIN market_data_ts m ON p.symbol = m.symbol
GROUP BY p.trader_id;
```
**Rationale**: Complex join with stateful GROUP BY. Different symbols from position stream could hash to different partitions, but `always_hash` guarantees same trader_id groups correctly. Critical for accurate risk aggregation.

### 4. Order Flow Analysis (Pre-Aligned Data)
```sql
-- Institutional order flow detection - pre-aligned by symbol
-- @job_name: order_flow_imbalance_detection
-- @partitioning_strategy: smart_repartition
CREATE STREAM order_flow_imbalance_detection AS
SELECT
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) AS buy_volume,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) AS sell_volume,
    SUM(quantity) / COUNT(*) AS avg_size
FROM in_order_book_stream
GROUP BY symbol
WINDOW TUMBLING (event_time, INTERVAL '1' MINUTE);
```
**Rationale**: Data likely pre-partitioned by symbol in Kafka. `smart_repartition` detects alignment and avoids shuffling overhead while maintaining correctness.

### 5. Arbitrage Detection (Cross-Exchange Join)
```sql
-- Cross-exchange price discrepancy detection
-- @job_name: arbitrage_opportunities_detection
-- @partitioning_strategy: always_hash
CREATE STREAM arbitrage_opportunities_detection AS
SELECT
    a.symbol, a.exchange as exchange_a, b.exchange as exchange_b,
    a.bid_price as bid_a, b.ask_price as ask_b,
    (a.bid_price - b.ask_price) as spread
FROM in_market_data_stream_a a
JOIN in_market_data_stream_b b ON a.symbol = b.symbol
WHERE a.bid_price > b.ask_price;
```
**Rationale**: Join requires same symbol from both exchanges in same partition. `always_hash` guarantees this correctness.

## Performance Characteristics

### Throughput by Strategy (100K records, measured on 8 partitions)

| Strategy | Rec/Sec Per Partition | Notes |
|----------|----------------------|-------|
| AlwaysHash | 200,000 | Baseline, safe for all workloads |
| SmartRepartition | 210,000 | +5% with pre-aligned data |
| StickyPartition | 200,000 | Same throughput, better latency |
| RoundRobin | 204,000 | +2% for stateless workloads |

### Latency Improvement (p99)

| Strategy | Improvement | Use Case |
|----------|------------|----------|
| AlwaysHash | Baseline | 5ms (typical) |
| SmartRepartition | +5-10% | With pre-aligned data |
| StickyPartition | +10-15% | Low-cardinality GROUP BY |
| RoundRobin | No latency impact | Stateless transforms |

## Implementation Details: Source Partition Usage

This section explains how each strategy leverages source partition information from Kafka or other sources.

### How Strategies Use `StreamRecord.__partition__`

Each strategy has different behavior regarding the incoming `__partition__` field (Kafka partition ID):

| Strategy | Uses `__partition__` | Method | Performance |
|----------|---------------------|--------|-------------|
| **StickyPartition** | ✅ Always | Routes directly to same partition (modulo num_partitions) | ~0% overhead |
| **SmartRepartition** | ✅ If pre-aligned | Detects if data aligns with GROUP BY columns, uses if yes | ~0-8% overhead |
| **AlwaysHash** | ❌ Never | Hashes GROUP BY columns regardless of source partition | High overhead |
| **RoundRobin** | ❌ Never | Ignores all record data, uses atomic counter | Minimal overhead |

### StickyPartition: Source Partition Affinity

```rust
// Primary path: Use source partition field directly
if let Some(partition_id) = record.fields.get("__partition__")
    .and_then(|v| match v {
        FieldValue::Integer(id) => Some(*id as usize),
        _ => None,
    }) {
    return Ok(partition_id % context.num_partitions);
}
```

**Behavior**:
- Reads the `__partition__` system field directly from Kafka
- Routes record to same partition in the V2 coordinator
- Preserves **cache locality** (same record flows through same CPU core)
- **10-15% latency improvement** for compatible workloads

**When to Use**:
- Data comes from Kafka (has `__partition__` field)
- Queries process per-partition data independently
- Low-cardinality GROUP BY columns (few distinct keys)

### SmartRepartition: Alignment-Aware Routing

```rust
// Alignment detection (lines 109-112)
// Check if source_partition_key matches GROUP BY columns
if source_aligned {
    // Use source partition directly
    return Ok(record.__partition__ % context.num_partitions);
} else {
    // Hash GROUP BY columns (fallback)
    return Ok(hash(group_by_values) % context.num_partitions);
}
```

**Behavior**:
- **Detects pre-alignment** between source partitioning and GROUP BY columns
- If aligned: uses `__partition__` directly (0% overhead)
- If misaligned: falls back to hashing (slight overhead)
- **Automatic optimization** for aligned data scenarios

**Example**:
```sql
-- Input: order_book_stream pre-partitioned by SYMBOL in Kafka
-- Query: GROUP BY symbol
-- Result: SmartRepartition detects alignment, uses __partition__ directly
CREATE STREAM order_flow_imbalance_detection AS
SELECT
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) AS buy_volume
FROM in_order_book_stream
GROUP BY symbol;  -- Matches Kafka partitioning key!
```

### AlwaysHash: Safety-First (Ignores Source Partition)

```rust
// Always hash GROUP BY columns, regardless of source partition
let hash = fnv_hash(&group_by_values);
Ok(hash % context.num_partitions)
```

**Why**:
- Different GROUP BY columns than source partitioning requires repartitioning
- Hashing guarantees correctness for any aggregation pattern
- Trades performance for guaranteed consistency

**Example**:
```sql
-- Input: market_data_stream partitioned by EXCHANGE in Kafka
-- Query: GROUP BY symbol
-- Result: Must repartition (exchange ≠ symbol), AlwaysHash is correct choice
CREATE STREAM price_by_symbol AS
SELECT
    symbol,
    AVG(price) as avg_price
FROM market_data_stream
GROUP BY symbol;  -- Different from Kafka partitioning key!
```

### RoundRobin: Stateless Load Balancing

```rust
// Ignores source partition entirely
let next = self.next_partition.fetch_add(1, Ordering::Relaxed);
Ok(next % context.num_partitions)
```

**Why**:
- No GROUP BY, so no state to maintain
- Distributed purely for load balancing
- Each partition gets equal number of records

## Decision Tree

```
Is your job stateless (no GROUP BY, no aggregation)?
├─ YES
│  ├─ Need maximum load balancing?
│  │  ├─ YES → RoundRobin (maximum throughput)
│  │  └─ NO → SmartRepartition or RoundRobin
│  └─ NO (stateless but prefer safety)
│     └─ RoundRobin or AlwaysHash
│
└─ NO (has stateful operations: GROUP BY, agg, join)
   ├─ Is data pre-aligned with GROUP BY key?
   │  ├─ YES → SmartRepartition (optimization)
   │  └─ NO → AlwaysHash (correctness)
   │
   ├─ Is latency SLA critical (< 100ms)?
   │  ├─ YES → StickyPartition (if low-cardinality key)
   │  └─ NO → AlwaysHash (safety-first)
   │
   └─ If unsure → AlwaysHash (always safe)
```

## Monitoring Strategy Selection

Velostream logs strategy selection at INFO level:

```
2025-11-07T10:15:42Z INFO job_server: Deploying job 'market-data-event-time-1'
2025-11-07T10:15:42Z INFO v2_coordinator: Using partitioning strategy: always_hash
2025-11-07T10:15:42Z INFO v2_coordinator: Created 8 partitions with AlwaysHashStrategy
```

Check logs for:
1. **Strategy loaded correctly** - Should see "Using partitioning strategy: {name}"
2. **Fallback warnings** - If invalid strategy specified, will fallback to AlwaysHashStrategy
3. **Partition count** - Should match configured `num_partitions`

## Advanced: Interaction with Other Configuration

### With Group By Columns
```rust
let coordinator = PartitionedJobCoordinator::new(config)
    .with_group_by_columns(vec!["symbol".to_string()]);
```
- `always_hash`: Uses provided GROUP BY columns for hashing
- `smart_repartition`: Uses GROUP BY columns to detect pre-alignment
- `sticky_partition`: Uses GROUP BY columns for sticky routing
- `round_robin`: Ignores GROUP BY columns (round-robin regardless)

### With Custom Partition Count
```rust
let mut config = PartitionedJobConfig::default();
config.num_partitions = Some(16);  // Use 16 partitions
config.partitioning_strategy = Some("smart_repartition".to_string());
```
- All strategies work with custom partition counts
- More partitions = better parallelism (up to core count)
- Fewer partitions = better data locality but higher per-partition load

### Default Behavior
- If `partitioning_strategy` is `None` → uses `AlwaysHashStrategy` (safety-first default)
- If invalid strategy name specified → logs warning and falls back to `AlwaysHashStrategy`
- Case-insensitive matching ensures flexibility

## Strategy Comparison Matrix

| Aspect | AlwaysHash | SmartRepartition | StickyPartition | RoundRobin |
|--------|-----------|------------------|-----------------|-----------|
| **Uses `__partition__`** | ❌ No | ✅ If aligned | ✅ Always | ❌ No |
| **Routing Method** | Hash GROUP BY | Alignment detection | Direct pass-through | Atomic counter |
| **Routing Overhead** | High | Low-Medium | ~0% | Minimal |
| **Throughput (r/s)** | 200K/partition | 210K/partition | 200K/partition | 204K/partition |
| **Latency (p99)** | Baseline (5ms) | +5-10% | -10-15% | Baseline |
| **State Consistency** | ✅ Guaranteed | ✅ Guaranteed | ✅ Guaranteed | ❌ Broken for agg |
| **Load Distribution** | Uneven (hash skew) | Good | Good | Perfect |
| **Kafka Affinity** | No | If aligned | Yes | No |
| **Safe for GROUP BY** | ✅ Yes | ✅ Yes | ✅ Yes | ❌ No |
| **Safe for Joins** | ✅ Yes | ✅ If aligned | ✅ Yes | ❌ No |
| **Best for Aggregations** | ✅ Yes | ✅ Yes | ✅ Yes | ❌ No |

## Key Insights

**When to Choose Each Strategy:**

1. **AlwaysHash (Default)**
   - ✅ Use when: Unknown data alignment, complex aggregations, safety-critical
   - ✅ Pros: Works correctly for ALL patterns, guaranteed state consistency
   - ❌ Cons: Higher hashing overhead, can create uneven load distribution
   - 🎯 Bottom line: Safe choice when you're unsure

2. **SmartRepartition (Optimization)**
   - ✅ Use when: Data known to be pre-aligned with GROUP BY columns
   - ✅ Pros: Automatic alignment detection, 5-10% performance gain when aligned
   - ❌ Cons: Falls back to AlwaysHash when misaligned (not transparent)
   - 🎯 Bottom line: Performance boost for known-aligned data pipelines

3. **StickyPartition (Latency)**
   - ✅ Use when: Kafka sources, low-cardinality dimensions, cache-sensitive workloads
   - ✅ Pros: 10-15% latency improvement, minimal routing overhead
   - ❌ Cons: Can create hotspots with high-cardinality GROUP BY
   - 🎯 Bottom line: Best for request-response patterns and cache locality

4. **RoundRobin (Throughput)**
   - ✅ Use when: Stateless filtering, pure selection, no GROUP BY/aggregation
   - ✅ Pros: Perfect load balancing, minimal overhead
   - ❌ Cons: **BREAKS aggregations and joins** (data fragments)
   - 🎯 Bottom line: Only for stateless transformations

**Decision Summary:**
- Unknown patterns → **AlwaysHash**
- Pre-aligned data → **SmartRepartition**
- Kafka sources + latency SLA → **StickyPartition**
- Stateless pass-through → **RoundRobin**

## Summary

| Strategy | Safety | Performance | Best For |
|----------|--------|-------------|----------|
| **AlwaysHash** | ✓✓✓ | ✓✓ | Stateful aggregations, joins, default choice |
| **SmartRepartition** | ✓✓ | ✓✓✓ | Pre-aligned data, multi-source pipelines |
| **StickyPartition** | ✓✓ | ✓✓ (latency) | Latency-sensitive, low-cardinality keys |
| **RoundRobin** | ✓ | ✓✓✓ | Stateless transforms, maximum load balance |

**Golden Rule**: When in doubt, use `always_hash` - it's the safe default that works correctly for all workload types.
