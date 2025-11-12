# Batch Configuration Guide

## Overview

Velostream supports comprehensive batch processing configuration via SQL `WITH` clauses. This allows you to optimize performance for high-throughput scenarios by configuring how records are grouped and processed together.

## Choosing the Right Batch Strategy

### The Key Relationship: batch.size Drives Everything

**Critical Insight**: Your `batch.size` configuration directly impacts:
1. **Kafka polling**: Sets `max.poll.records` (how many records Kafka fetches)
2. **Consumer tier**: Automatically selects Standard (10-15K msg/s) or Buffered (50-75K msg/s) tier
3. **Processing efficiency**: Matches poll size with processing batch size

```
batch.size = 500
    ↓
max.poll.records = 500  (Kafka polls 500 records at once)
    ↓
Consumer Tier = Buffered (processes 500 records efficiently)
    ↓
Result: Optimal throughput with no wasted polls!
```

### Decision Matrix: Low Latency vs High Throughput

| Use Case | Best Strategy | batch.size | Consumer Tier | Typical Throughput |
|----------|--------------|------------|---------------|-------------------|
| **Real-time alerts** | `low_latency` | 1-10 | Standard | 5-10K msg/s |
| **Live dashboards** | `fixed_size` | 50-100 | Standard | 10-15K msg/s |
| **Streaming ETL** | `fixed_size` | 500-1000 | Buffered | 50-75K msg/s |
| **Bulk data ingestion** | `mega_batch` | 10K-100K | Buffered | 100K-150K msg/s |
| **Analytics queries** | `adaptive_size` | 50-2000 | Auto | 10-75K msg/s |
| **Memory-constrained** | `memory_based` | Auto | Buffered | 30-60K msg/s |

### When to Choose Each Strategy

#### 1. Low Latency (`batch.strategy = 'low_latency'`)
**Choose when**:
- ✅ Sub-second latency requirements (fraud detection, alerting)
- ✅ Processing time more important than throughput
- ✅ Small data volumes (<10K msg/s)
- ✅ Real-time user-facing applications

**Configuration**:
```sql
WITH (
    'batch.strategy' = 'low_latency',
    'batch.low_latency_max_size' = '5',      -- Very small batches
    'batch.low_latency_wait' = '1ms',        -- Aggressive timeout
    'batch.eager_processing' = 'true'        -- Process immediately
);
```

**Result**: `max.poll.records=5`, Standard tier, ~5-10K msg/s, <10ms latency

#### 2. Fixed Size Batching (`batch.strategy = 'fixed_size'`)
**Choose when**:
- ✅ Predictable, consistent workloads
- ✅ Balance between latency and throughput
- ✅ Most common use case (recommended default)

**Small Batches** (Standard tier):
```sql
WITH (
    'batch.strategy' = 'fixed_size',
    'batch.size' = '100'  -- → Standard tier (10-15K msg/s)
);
```

**Large Batches** (Buffered tier):
```sql
WITH (
    'batch.strategy' = 'fixed_size',
    'batch.size' = '500'  -- → Buffered tier (50-75K msg/s)
);
```

**Result**:
- `batch.size ≤ 100` → Standard tier, 10-15K msg/s
- `batch.size > 100` → Buffered tier, 50-75K msg/s

#### 3. High Throughput (`batch.strategy = 'mega_batch'`)
**Choose when**:
- ✅ Maximum throughput required (>100K msg/s)
- ✅ Latency is less critical (seconds acceptable)
- ✅ Large data volumes (millions of records)
- ✅ Bulk loading, backfills, batch ETL

**Configuration**:
```sql
WITH (
    'batch.strategy' = 'mega_batch',
    'batch.mega_batch_size' = '50000',  -- Very large batches
    'batch.parallel' = 'true',          -- Multi-core processing
    'batch.reuse_buffer' = 'true'       -- Memory optimization
);
```

**Result**: `max.poll.records=50000`, Buffered tier, 100K-150K msg/s, seconds latency

#### 4. Adaptive Batching (`batch.strategy = 'adaptive_size'`)
**Choose when**:
- ✅ Variable workloads (traffic spikes, off-peak periods)
- ✅ Unknown data patterns
- ✅ Want automatic optimization
- ✅ Multi-tenant systems with varying loads

**Configuration**:
```sql
WITH (
    'batch.strategy' = 'adaptive_size',
    'batch.min_size' = '50',           -- Start small
    'batch.adaptive_max_size' = '1000', -- Grow as needed
    'batch.target_latency' = '100ms'    -- Optimize for this latency
);
```

**Result**: Dynamic tier selection, adapts between 10K-75K msg/s based on load

#### 5. Memory-Based Batching (`batch.strategy = 'memory_based'`)
**Choose when**:
- ✅ Large individual records (>10KB each)
- ✅ Memory constraints are primary concern
- ✅ Record size varies significantly
- ✅ Preventing OOM errors is critical

**Configuration**:
```sql
WITH (
    'batch.strategy' = 'memory_based',
    'batch.memory_size' = '10485760'  -- 10MB limit
);
```

**Result**: Auto-calculates batch size, Buffered tier, 30-60K msg/s

### Real-World Examples

#### Example 1: Fraud Detection (Low Latency)
```sql
CREATE STREAM fraud_alerts AS
SELECT
    transaction_id,
    user_id,
    amount,
    fraud_score(amount, location, time) as score
FROM transactions
WHERE fraud_score(amount, location, time) > 0.8
WITH (
    'batch.strategy' = 'low_latency',
    'batch.low_latency_max_size' = '1',  -- Process immediately
    'batch.eager_processing' = 'true'
);
-- Result: <10ms latency, ~5K msg/s, Standard tier
```

#### Example 2: Analytics Dashboard (Balanced)
```sql
CREATE STREAM user_metrics AS
SELECT
    user_id,
    COUNT(*) as event_count,
    AVG(session_duration) as avg_duration
FROM events
GROUP BY user_id
WITH (
    'batch.strategy' = 'fixed_size',
    'batch.size' = '200',  -- Balanced batch size
    'batch.timeout' = '500ms'
);
-- Result: ~100ms latency, ~40K msg/s, Buffered tier
```

#### Example 3: Data Lake Ingestion (High Throughput)
```sql
CREATE STREAM data_lake_ingest AS
SELECT * FROM raw_events
WITH (
    'batch.strategy' = 'mega_batch',
    'batch.mega_batch_size' = '50000',  -- Maximum throughput
    'batch.parallel' = 'true',
    'batch.reuse_buffer' = 'true'
);
-- Result: ~5s latency, ~120K msg/s, Buffered tier
```

### Performance Impact Summary

| Strategy | batch.size | max.poll.records | Consumer Tier | Throughput | Latency |
|----------|-----------|------------------|---------------|------------|---------|
| `low_latency` | 1-10 | 1-10 | Standard | 5-10K/s | <10ms |
| `fixed_size` (small) | 50-100 | 50-100 | Standard | 10-15K/s | 50-100ms |
| `fixed_size` (large) | 500-1000 | 500-1000 | Buffered | 50-75K/s | 200-500ms |
| `mega_batch` | 50K-100K | 50K-100K | Buffered | 100K-150K/s | 2-5s |
| `adaptive_size` | Dynamic | Dynamic | Auto | 10K-75K/s | 50-500ms |
| `memory_based` | Auto | Auto | Buffered | 30-60K/s | 200-800ms |

### Best Practices

1. **Start with fixed_size**: Most predictable and easy to tune
2. **Low latency costs throughput**: Accept 2-10x lower throughput for <10ms latency
3. **Large batches need more memory**: Monitor memory usage with batch.size > 1000
4. **Test with production load**: Performance varies by record size and complexity
5. **Use adaptive for unknown workloads**: Safe default when traffic patterns unclear

## Batch Strategies

### 1. Fixed Size Batching
Process records in fixed-size batches.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'fixed_size',
    'batch.size' = '1000',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.size` (default: 100): Number of records per batch
- `batch.timeout` (default: 1000ms): Maximum wait time before processing incomplete batch

### 2. Time Window Batching
Process records within time-based windows.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'time_window',
    'batch.window' = '5s',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.window` (default: 1s): Time window duration (supports ms, s, m, h, d)

### 3. Memory-Based Batching
Process records based on memory consumption.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'memory_based',
    'batch.memory_size' = '2097152',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.memory_size` (default: 1048576): Memory limit in bytes (1MB default)

### 4. Adaptive Size Batching
Dynamic batching that adapts based on system performance.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'adaptive_size',
    'batch.min_size' = '10',
    'batch.adaptive_max_size' = '2000',
    'batch.target_latency' = '100ms',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.min_size` (default: 10): Minimum batch size
- `batch.adaptive_max_size` (default: 1000): Maximum batch size
- `batch.target_latency` (default: 100ms): Target processing latency

### 5. Low Latency Batching
Optimized for minimal processing delay with eager processing.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'low_latency',
    'batch.low_latency_max_size' = '5',
    'batch.low_latency_wait' = '10ms',
    'batch.eager_processing' = 'true',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.low_latency_max_size` (default: 1): Maximum records before immediate processing
- `batch.low_latency_wait` (default: 1ms): Maximum wait time
- `batch.eager_processing` (default: true): Enable immediate processing

### 6. MegaBatch High-Throughput Processing (NEW - Phase 4)
Optimized for maximum throughput with large batch sizes, ring buffer reuse, and optional parallel processing.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'mega_batch',
    'batch.mega_batch_size' = '50000',
    'batch.parallel' = 'true',
    'batch.reuse_buffer' = 'true',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.mega_batch_size` (default: 50000): Large batch size (10K-100K records)
- `batch.parallel` (default: true): Enable parallel batch processing on multi-core systems
- `batch.reuse_buffer` (default: true): Use ring buffer for allocation reuse (20-30% faster)

**Performance Characteristics:**
- **Throughput**: Targets 8.37M+ records/sec (5x improvement over standard batching)
- **Memory Efficiency**: Ring buffer eliminates per-batch allocation overhead
- **Multi-Core Scaling**: Parallel processing leverages 4+ core systems (2-4x improvement)
- **Best For**: High-throughput data ingestion, bulk table loading, streaming ETL

**Rust API Usage:**
```rust
use velostream::velostream::datasource::{BatchConfig, BatchStrategy};

// High-throughput configuration (50K batch size)
let config = BatchConfig::high_throughput();

// Ultra-throughput configuration (100K batch size)
let config = BatchConfig::ultra_throughput();

// Custom MegaBatch configuration
let config = BatchConfig {
    strategy: BatchStrategy::MegaBatch {
        batch_size: 75_000,
        parallel: true,
        reuse_buffer: true,
    },
    max_batch_size: 100_000,
    batch_timeout: Duration::from_millis(100),
    enable_batching: true,
};
```

**Transaction Safety Note:**
- ✅ Safe for read-only operations (queries, aggregations, filters)
- ✅ Safe for independent writes (each batch has its own transaction)
- ⚠️ Use with caution for writes spanning multiple batches
- Set `batch.parallel = 'false'` for transactional writes that span batches

## Failure Strategy Configuration

Configure how processing failures are handled:

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'failure_strategy' = 'RetryWithBackoff',
    'max_retries' = '5',
    'retry_backoff' = '2000ms'
);
```

**Available Strategies:**
- `LogAndContinue`: Log errors and continue processing
- `SendToDLQ`: Route failed records to Dead Letter Queue
- `FailBatch`: Fail entire batch on any record failure
- `RetryWithBackoff`: Retry with exponential backoff

**Configuration Keys:**
- `failure_strategy`: Strategy for handling failures
- `max_retries` (default: 3): Maximum retry attempts
- `retry_backoff` (default: 1000ms): Backoff delay between retries

## Source Configuration

Batch and failure strategies also apply to data sources:

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'source.failure_strategy' = 'RetryWithBackoff',
    'source.retry_backoff' = '1000ms',
    'source.max_retries' = '3'
);
```

## Combined Configuration Example

```sql
CREATE STREAM optimized_stream AS
SELECT * FROM kafka_source
WITH (
    -- Global Batch Configuration
    'batch.strategy' = 'adaptive_size',
    'batch.enable' = 'true',
    'batch.min_size' = '50',
    'batch.adaptive_max_size' = '2000',
    'batch.target_latency' = '100ms',
    
    -- Failure Handling
    'failure_strategy' = 'RetryWithBackoff',
    'max_retries' = '5',
    'retry_backoff' = '2000ms',
    
    -- Source Configuration  
    'source.failure_strategy' = 'LogAndContinue'
);
```

## Performance Considerations

### Throughput Optimization
- **FixedSize**: Best for consistent workloads, use larger batch sizes (500-2000)
- **TimeWindow**: Good for time-sensitive processing, balance window size with latency
- **AdaptiveSize**: Best for variable workloads, automatically optimizes

### Memory Management
- **MemoryBased**: Prevents memory overflow, useful for large record processing
- Monitor memory usage with different batch sizes

### Latency Optimization
- **LowLatency**: Use for real-time requirements, smaller batch sizes
- **EagerProcessing**: Process immediately when conditions met

## Duration Format Support

All time-based configurations support multiple formats:
- `ms`, `milliseconds`: Milliseconds
- `s`, `seconds`: Seconds  
- `m`, `minutes`: Minutes
- `h`, `hours`: Hours
- `d`, `days`: Days

Examples: `500ms`, `30s`, `5m`, `2h`, `1d`

## Validation and Schema

All batch configuration is validated against a comprehensive schema:
- Type checking (string, integer, duration, boolean)
- Value validation (positive numbers, valid enums)
- Required field validation
- Default value provision

## Best Practices

1. **Start with defaults** and tune based on performance metrics
2. **Monitor batch processing** latency and throughput
3. **Use adaptive strategies** for variable workloads
4. **Combine with appropriate failure strategies** for reliability
5. **Test with realistic data volumes** before production deployment

## Testing and Validation

Velostream includes comprehensive test binaries for batch configuration:

```bash
# Test simple batch configurations
cargo run --bin test_batch_with_clause --no-default-features

# Test failure strategy configurations  
cargo run --bin test_failure_strategy_config --no-default-features
```

This ensures all configurations are validated and working before deployment.