# SQL Job Annotations Configuration Guide

## Overview

SQL job annotations are special SQL comments that configure job processing behavior without modifying query logic. They use the `-- @` prefix and allow fine-grained control over processor selection, batch sizing, partitioning, and more - all directly in your SQL.

This guide covers the complete annotation system for job configuration:
- `@job_mode` - Select processor type (simple, transactional, adaptive)
- `@batch_size` - Configure batch processing size
- `@num_partitions` - Set partition count for adaptive mode
- `@partitioning_strategy` - Choose record routing strategy

## Quick Start

```sql
-- Simple example: Use transactional mode with 8 partitions
-- @job_mode: transactional
-- @num_partitions: 8
-- @partitioning_strategy: hash

SELECT symbol, COUNT(*) as trade_count
FROM market_data
GROUP BY symbol
EMIT CHANGES;
```

## Annotation Syntax

### Basic Format

```sql
-- @annotation_name: value
```

### Placement

Annotations must appear **before the SQL statement** (in comment lines):

```sql
-- ✅ CORRECT: Annotations before SELECT
-- @job_mode: adaptive
SELECT * FROM stream;

-- ❌ WRONG: Annotations after SELECT (won't parse)
SELECT * FROM stream;
-- @job_mode: adaptive

-- ✅ CORRECT: Multiple annotations
-- @job_mode: transactional
-- @batch_size: 500
-- @num_partitions: 8
SELECT * FROM stream;
```

### Case Sensitivity

- Annotation names are **case-insensitive**: `@JOB_MODE`, `@job_mode`, `@Job_Mode` all work
- Values are **case-insensitive** for enums: `adaptive`, `ADAPTIVE`, `Adaptive` all work

```sql
-- All equivalent:
-- @job_mode: simple
-- @JOB_MODE: SIMPLE
-- @Job_Mode: Simple
SELECT * FROM stream;
```

---

## 1. Job Mode Annotation (`@job_mode`)

Controls which job processor is used for execution.

### Syntax

```sql
-- @job_mode: <mode>
```

### Valid Values

| Mode | Description | Use Case |
|------|-------------|----------|
| `simple` | Single-threaded, best-effort delivery | High throughput, acceptable data loss |
| `transactional` | Single-threaded, ACID guarantees | Financial, payment processing |
| `adaptive` | Multi-partition parallel execution | High-throughput, multi-core systems |

### Examples

#### Simple Mode

```sql
-- @job_mode: simple
CREATE STREAM market_analysis AS
SELECT symbol, AVG(price)
FROM market_data
GROUP BY symbol
EMIT CHANGES;
```

**Characteristics:**
- ✅ Single-threaded (simple coordination)
- ✅ 656K rec/sec throughput
- ✅ Best-effort delivery (LogAndContinue)
- ❌ No ACID guarantees

**Best for:** Analytics, metrics, non-critical data processing

#### Transactional Mode

```sql
-- @job_mode: transactional
CREATE STREAM payment_processor AS
SELECT account_id, SUM(amount) as total
FROM payments
GROUP BY account_id
EMIT CHANGES;
```

**Characteristics:**
- ✅ Single-threaded (simple coordination)
- ✅ 961K rec/sec throughput (fastest overall!)
- ✅ ACID guarantees
- ✅ Atomic batch semantics
- ✅ At-least-once delivery

**Best for:** Financial transactions, critical data, regulatory compliance

#### Adaptive Mode

```sql
-- @job_mode: adaptive
CREATE STREAM parallel_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Characteristics:**
- ✅ Multi-partition parallel execution
- ✅ Scales to multiple CPU cores
- ⚠️ Currently has scaling issues (see notes below)
- ❌ More complex coordination

**Best for:** (Once scaling is fixed) Multi-core throughput-critical workloads

### Performance Comparison

```
Mode           Throughput      ACID    Best For
─────────────────────────────────────────────────
transactional  961,531 rec/s   ✅      PRIMARY CHOICE
simple         656,347 rec/s   ❌      Analytics
adaptive       24,500 rec/s    ❌      Under investigation
```

**Recommendation:** Use `transactional` for 95% of use cases - it's the fastest AND most reliable.

---

## 2. Batch Size Annotation (`@batch_size`)

Controls how many records are processed per batch.

### Syntax

```sql
-- @batch_size: <integer>
```

### Valid Values

- Integer >= 0
- Typical range: 10-1000
- Default (if not specified): 100

### Impact

| Size | Throughput | Latency | Use Case |
|------|-----------|---------|----------|
| 10 | Lowest | <1ms per record | Ultra-responsive APIs |
| 100 | Normal | ~10ms | Balanced (default) |
| 500 | High | ~50ms | Throughput-optimized |
| 1000 | Highest | ~100ms | Bulk processing |

### Examples

#### Low Latency: Responsive API

```sql
-- @job_mode: simple
-- @batch_size: 10

CREATE STREAM api_processor AS
SELECT request_id, process(request_data)
FROM http_requests
EMIT CHANGES;
```

**Effect:**
- ✅ 10 records per batch = low latency
- ❌ Lower throughput (~100-200K rec/s)
- ✅ Perfect for interactive systems

#### Balanced: Default Configuration

```sql
-- @job_mode: transactional
-- @batch_size: 100

CREATE STREAM standard_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Effect:**
- ✅ 100 records per batch = balanced
- ✅ Good throughput (650-960K rec/s)
- ✅ Reasonable latency (~10ms)

#### High Throughput: Bulk Processing

```sql
-- @job_mode: simple
-- @batch_size: 1000

CREATE STREAM bulk_loader AS
SELECT * FROM raw_events
EMIT CHANGES;
```

**Effect:**
- ✅ 1000 records per batch = max throughput
- ✅ Highest throughput (920-960K rec/s)
- ❌ Higher latency (~100ms per batch)

### Tuning Guide

```
If throughput < target:
  → Increase batch_size (try 500, 1000)

If latency too high:
  → Decrease batch_size (try 10, 50)

For financial data:
  → Use @batch_size: 100 (balanced)

For analytics:
  → Use @batch_size: 1000 (throughput)
```

---

## 3. Partition Count Annotation (`@num_partitions`)

Sets the number of parallel partitions for adaptive mode.

### Syntax

```sql
-- @num_partitions: <integer>
```

### Valid Values

- Integer > 0
- Typical range: 1-16 (up to CPU core count)
- Default (if not specified): Auto-detect (CPU core count)

### When to Use

- Only applies when `@job_mode: adaptive` is set
- For simple/transactional modes, this annotation is ignored

### Examples

#### Auto-Detect (Recommended)

```sql
-- @job_mode: adaptive

CREATE STREAM parallel_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Effect:**
- Automatically detects CPU core count
- 4-core system → 4 partitions
- 8-core system → 8 partitions
- Scales with hardware

#### Explicit Partition Count

```sql
-- @job_mode: adaptive
-- @num_partitions: 8

CREATE STREAM 8way_parallel AS
SELECT * FROM events
EMIT CHANGES;
```

**Effect:**
- Always uses 8 partitions regardless of CPU count
- Use when you want to override auto-detection

#### Single Partition (Force Sequential)

```sql
-- @job_mode: adaptive
-- @num_partitions: 1

CREATE STREAM sequential AS
SELECT * FROM events
EMIT CHANGES;
```

**Effect:**
- Disables parallel execution
- Useful for debugging or ordered processing

### Performance Guidance

```
1-2 cores:   Use @job_mode: simple or transactional
             (Adaptive overhead exceeds benefit)

4-8 cores:   @num_partitions: <cpu_count>
             (Good scaling, let it auto-detect)

16+ cores:   Consider multiple adaptive instances
             (Single instance may hit lock contention)
```

### ⚠️ Known Issue

Currently, adaptive mode does not scale linearly. Expected improvements coming in future versions.

---

## 4. Partitioning Strategy Annotation (`@partitioning_strategy`)

Controls how records are routed to partitions.

### Syntax

```sql
-- @partitioning_strategy: <strategy>
```

### Valid Values

| Strategy | Best For | Behavior |
|----------|----------|----------|
| `sticky` | ROWS WINDOW, session windows | Use record's source partition |
| `hash` | GROUP BY, aggregate queries | Hash on GROUP BY columns |
| `smart` | Mixed workloads | Auto-select based on query pattern |
| `roundrobin` | Uniform distribution | Round-robin assignment |
| `fanin` | JOIN operations | Broadcast to all partitions |

### Alternate Names

Some strategies accept alternate names for clarity:

```sql
-- These are equivalent:
-- @partitioning_strategy: sticky
-- @partitioning_strategy: stickypartition

-- These are equivalent:
-- @partitioning_strategy: hash
-- @partitioning_strategy: alwayshash

-- These are equivalent:
-- @partitioning_strategy: smart
-- @partitioning_strategy: smartrepartition
```

### Examples

#### Sticky Partition Strategy

```sql
-- @job_mode: adaptive
-- @num_partitions: 8
-- @partitioning_strategy: sticky

CREATE STREAM time_series_window AS
SELECT symbol,
       AVG(price) OVER (
           PARTITION BY symbol
           ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
       ) as moving_avg
FROM market_data
EMIT CHANGES;
```

**Best for:**
- ✅ ROWS WINDOW (keeps related records together)
- ✅ Session windows
- ✅ Zero-overhead (uses source partition field)
- ❌ Uneven data distribution

**Effect:** Each partition processes records from its corresponding source partition

#### Hash Partitioning Strategy

```sql
-- @job_mode: adaptive
-- @num_partitions: 8
-- @partitioning_strategy: hash

CREATE STREAM group_by_processor AS
SELECT symbol, COUNT(*) as trade_count
FROM market_data
GROUP BY symbol
EMIT CHANGES;
```

**Best for:**
- ✅ GROUP BY operations
- ✅ Aggregate queries
- ✅ Even distribution across partitions
- ✅ Consistent ordering

**Effect:** Records are hashed to partitions based on GROUP BY columns

#### Smart Repartitioning Strategy

```sql
-- @job_mode: adaptive
-- @num_partitions: 8
-- @partitioning_strategy: smart

CREATE STREAM mixed_workload AS
SELECT symbol, COUNT(*) as cnt,
       AVG(price) OVER (PARTITION BY symbol) as avg_price
FROM market_data
GROUP BY symbol
EMIT CHANGES;
```

**Best for:**
- ✅ Mixed queries with multiple operations
- ✅ Automatic optimization
- ✅ Hybrid aggregation + windowing

**Effect:** Automatically selects best strategy based on query pattern

#### Round-Robin Strategy

```sql
-- @job_mode: adaptive
-- @num_partitions: 4
-- @partitioning_strategy: roundrobin

CREATE STREAM uniform_distribution AS
SELECT * FROM events
EMIT CHANGES;
```

**Best for:**
- ✅ Uniform load distribution
- ✅ Simple transformations
- ✅ No GROUP BY or PARTITION BY

**Effect:** Simple round-robin distribution across partitions

#### FanIn Strategy (Broadcast)

```sql
-- @job_mode: adaptive
-- @partitioning_strategy: fanin

CREATE STREAM broadcast_join AS
SELECT m.symbol, m.price, r.region
FROM market_data m
JOIN regional_config r ON m.symbol = r.symbol
EMIT CHANGES;
```

**Best for:**
- ✅ JOIN operations (broadcast small tables)
- ✅ Reference data lookups
- ❌ Very large tables (memory overhead)

**Effect:** Broadcasts record to all partitions (synchronization overhead)

### Auto-Selection

If you don't specify a strategy, the system auto-selects based on query:

```sql
-- @job_mode: adaptive
-- (No @partitioning_strategy specified)

-- For this query with GROUP BY:
SELECT symbol, SUM(amount)
FROM trades
GROUP BY symbol
-- → Auto-selects: hash partitioning
```

---

## Complete Examples

### Example 1: Financial Trading (Most Important Use Case)

```sql
-- Trading system requiring ACID compliance
-- @job_mode: transactional
-- @batch_size: 100
-- @num_partitions: 8
-- @partitioning_strategy: hash

CREATE STREAM trade_aggregator AS
SELECT
    trader_id,
    symbol,
    SUM(quantity) as total_quantity,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price
FROM trades
GROUP BY trader_id, symbol
EMIT CHANGES;
```

**Why these settings:**
- `transactional` → Financial data requires ACID
- `batch_size: 100` → Balanced latency/throughput
- `num_partitions: 8` → Standard multi-core scaling
- `hash` → GROUP BY requires hash partitioning

### Example 2: Real-Time Analytics (Throughput Focus)

```sql
-- High-volume analytics processing
-- @job_mode: simple
-- @batch_size: 1000
-- @num_partitions: 8
-- @partitioning_strategy: sticky

CREATE STREAM event_analytics AS
SELECT
    event_type,
    COUNT(*) as event_count,
    AVG(EXTRACT(MILLISECOND FROM event_time)) as avg_latency
FROM user_events
WINDOW TUMBLING(INTERVAL 5 MINUTES)
GROUP BY event_type
EMIT CHANGES;
```

**Why these settings:**
- `simple` → Analytics data allows best-effort
- `batch_size: 1000` → Maximize throughput (960K rec/s)
- `sticky` → Tumbling window keeps data together
- No `partitioning_strategy` → Auto-selects `sticky` for window

### Example 3: Interactive API (Latency Focus)

```sql
-- Low-latency query response system
-- @job_mode: transactional
-- @batch_size: 10
-- @num_partitions: 4

CREATE STREAM api_responder AS
SELECT
    request_id,
    user_id,
    response_status,
    process_time_ms
FROM http_requests
WHERE response_time_ms < 1000
EMIT CHANGES;
```

**Why these settings:**
- `transactional` → Ensures request ordering
- `batch_size: 10` → Ultra-low latency (<1ms)
- `num_partitions: 4` → Moderate parallelism

### Example 4: Reference Data Join (Broadcasting)

```sql
-- Join live trades with region configuration
-- @job_mode: adaptive
-- @num_partitions: 8
-- @partitioning_strategy: fanin

CREATE STREAM enriched_trades AS
SELECT
    t.trade_id,
    t.symbol,
    t.amount,
    r.region,
    r.regulatory_level
FROM trades t
JOIN regions r ON t.symbol = r.symbol
EMIT CHANGES;
```

**Why these settings:**
- `fanin` → Broadcasts region config to all partitions
- `num_partitions: 8` → Processes trades in parallel

---

## Annotation Reference Table

| Annotation | Type | Values | Default | Example |
|-----------|------|--------|---------|---------|
| `@job_mode` | String | `simple`, `transactional`, `adaptive` | `simple` | `-- @job_mode: transactional` |
| `@batch_size` | Integer | 1-10000 | 100 | `-- @batch_size: 500` |
| `@num_partitions` | Integer | 1-16+ | CPU count | `-- @num_partitions: 8` |
| `@partitioning_strategy` | String | `sticky`, `hash`, `smart`, `roundrobin`, `fanin` | `sticky` | `-- @partitioning_strategy: hash` |

---

## Error Handling

### Invalid Annotation Values

Invalid values are **silently ignored** (annotation not applied):

```sql
-- @job_mode: invalid_mode
-- ↑ Ignored - falls back to default (simple)

-- @batch_size: not_a_number
-- ↑ Ignored - uses default (100)

-- @num_partitions: zero
-- ↑ Ignored - uses CPU count

-- @partitioning_strategy: unknown_strategy
-- ↑ Ignored - uses default (sticky)
```

### Debugging

To verify annotations are being parsed:

1. Check server logs for annotation parsing debug messages
2. Inspect the StreamingQuery AST fields (they should be `Some(...)` not `None`)
3. Run tests to confirm behavior

### Common Issues

**Issue: Annotations not taking effect**

```sql
-- ❌ WRONG: Annotation after SELECT
SELECT * FROM stream;
-- @job_mode: transactional

-- ✅ CORRECT: Annotation before SELECT
-- @job_mode: transactional
SELECT * FROM stream;
```

**Issue: Case sensitivity problems**

```sql
-- All of these work (case-insensitive):
-- @job_mode: simple
-- @JOB_MODE: SIMPLE
-- @Job_Mode: Simple
SELECT * FROM stream;
```

---

## Performance Impact

### Throughput by Configuration

```
Config                              Throughput    Notes
────────────────────────────────────────────────────────
@job_mode: simple                   656,347 rec/s Good baseline
@job_mode: transactional            961,531 rec/s ⭐ BEST
@job_mode: adaptive                 24,500 rec/s  ⚠️ Under investigation

With @batch_size: 10                100-200K rec/s Ultra-low latency
With @batch_size: 100               650-960K rec/s Balanced
With @batch_size: 1000              920-960K rec/s Maximum throughput
```

### Configuration Combinations

**For maximum throughput:**
```sql
-- @job_mode: simple
-- @batch_size: 1000
-- Achieves: 960K rec/sec
```

**For ACID + throughput (recommended):**
```sql
-- @job_mode: transactional
-- @batch_size: 100
-- Achieves: 961K rec/sec + ACID guarantees
```

**For low latency:**
```sql
-- @job_mode: simple
-- @batch_size: 10
-- Achieves: <1ms latency per record
```

---

## Best Practices

### 1. Start with Transactional Mode

```sql
-- ✅ RECOMMENDED: Default to transactional
-- @job_mode: transactional

SELECT * FROM events EMIT CHANGES;
```

Why:
- Fastest overall (961K rec/sec)
- ACID guarantees included
- Suitable for all use cases

### 2. Set Batch Size Based on Latency Requirements

```sql
-- Interactive API: ultra-responsive
-- @batch_size: 10

-- Balanced system: default
-- @batch_size: 100

-- Bulk processing: maximum throughput
-- @batch_size: 1000
```

### 3. Use Hash Partitioning for GROUP BY

```sql
-- Always use hash for GROUP BY:
-- @partitioning_strategy: hash

SELECT account_id, SUM(amount)
FROM transactions
GROUP BY account_id
```

### 4. Use Sticky Partitioning for Windows

```sql
-- Always use sticky for ROWS/Session windows:
-- @partitioning_strategy: sticky

SELECT symbol,
       AVG(price) OVER (PARTITION BY symbol ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
FROM market_data
```

### 5. Document Your Choices

```sql
-- Financial trading system requiring ACID compliance and low latency
-- Uses transactional mode for atomicity
-- Batch size of 100 balances throughput and latency
-- @job_mode: transactional
-- @batch_size: 100
-- @partitioning_strategy: hash

SELECT ...
```

---

## Advanced Usage

### Dynamic Partition Selection

The system can auto-select partitioning strategy based on query:

```sql
-- No explicit strategy → auto-selection based on query
-- @job_mode: adaptive
-- (system detects GROUP BY and selects hash automatically)

SELECT symbol, COUNT(*)
FROM trades
GROUP BY symbol
```

### Three-Level Priority for Annotations

If multiple configuration sources exist:

1. **SQL Annotation** (highest priority)
   ```sql
   -- @job_mode: transactional
   ```

2. **Server Default** (if annotation not specified)

3. **System Default** (if server default not set)

```
Example:
  Annotation: @job_mode: transactional
  Server:     mode: simple
  System:     simple

  Result: Uses transactional (annotation wins)
```

---

## Migration Guide

### From Simple to Transactional

```sql
-- Before (simple mode)
SELECT * FROM events EMIT CHANGES;

-- After (transactional mode - recommended)
-- @job_mode: transactional
SELECT * FROM events EMIT CHANGES;

-- Result: 47% faster + ACID guarantees
```

### From Small to Large Batches

```sql
-- Before (low throughput)
-- @batch_size: 100
SELECT * FROM events EMIT CHANGES;

-- After (high throughput)
-- @batch_size: 1000
SELECT * FROM events EMIT CHANGES;

-- Result: ~50% throughput increase
```

---

## Related Documentation

- [Job Processor Configuration Guide](./job-processor-configuration-guide.md) - Detailed processor configuration
- [SQL Application Annotations](./deployment/sql-application-annotations.md) - Application-level annotations
- [Performance Tuning Guide](../developer/performance-tuning.md) - Optimization techniques
- [Job Processor Selection Guide](../developer/job-processor-selection-guide.md) - When to use each processor
