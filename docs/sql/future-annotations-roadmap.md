# SQL Annotations - Future Roadmap

## Overview

This document outlines the complete set of configurable properties in Velostream's job processors and identifies which are currently exposed as SQL annotations vs. which could be exposed in future releases.

**Current Status (v0.1):**
- ✅ 4 annotations implemented and fully tested
- 📋 10+ additional annotations planned for future releases
- 📚 All defaults documented in `job-processor-configuration-guide.md`

---

## Currently Exposed Annotations (v0.1)

### Implemented & Tested

| Annotation | Type | Example | Impact |
|-----------|------|---------|--------|
| `@job_mode` | Enum | `simple`, `transactional`, `adaptive` | Processor architecture selection |
| `@batch_size` | Integer | `10-1000` | Throughput vs. latency tradeoff |
| `@num_partitions` | Integer | `1-16+` | Parallel partition count (adaptive mode) |
| `@partitioning_strategy` | Enum | `sticky`, `hash`, `smart`, `roundrobin`, `fanin` | Record routing strategy |

**Code Status:**
- ✅ Enums defined in `src/velostream/sql/ast.rs`
- ✅ FromStr trait implementations
- ✅ Parser integration in `annotation_parser.rs`
- ✅ Execution wiring in `stream_job_server.rs`
- ✅ 30 comprehensive tests
- ✅ Full documentation with examples

---

## Planned Annotations (Future Releases)

### Phase 2: Failure & Retry Handling

These properties control how failures are handled - critical for production reliability.

#### `@failure_strategy` - Record Failure Handling

**Purpose:** Specify how to handle failed records during processing

**Values:**
- `LogAndContinue` - Log error and skip record (default, best-effort)
- `FailBatch` - Rollback entire batch on any failure (ACID)
- `SendToDLQ` - Send to Dead Letter Queue
- `RetryWithBackoff` - Retry with exponential backoff

**Default:** `LogAndContinue`

**Example:**
```sql
-- @job_mode: transactional
-- @failure_strategy: FailBatch

-- Entire batch rolls back if any record fails
CREATE STREAM payment_processor AS
SELECT * FROM payments
EMIT CHANGES;
```

**Impact:** Determines delivery semantics and error recovery

**Code Location:** `src/velostream/server/processors/common.rs:FailureStrategy`

---

#### `@max_retries` - Retry Attempt Count

**Purpose:** Maximum number of retries for failed batches

**Values:**
- Integer: 0-100 (typical: 1-10)
- 0 = no retries
- 10 = default (current)

**Default:** `10`

**Example:**
```sql
-- @job_mode: transactional
-- @max_retries: 5

CREATE STREAM resilient_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Impact:** Tradeoff between reliability (more retries) vs. latency (fewer retries)

**Code Location:** `JobProcessingConfig.max_retries`

---

#### `@retry_backoff_ms` - Retry Delay

**Purpose:** Wait time between retry attempts (milliseconds)

**Values:**
- Integer: 0-60000 (typical: 100-5000)
- Default: 5000 (5 seconds)

**Example:**
```sql
-- @failure_strategy: RetryWithBackoff
-- @retry_backoff_ms: 2000

CREATE STREAM quick_retry_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Impact:** Controls recovery speed (lower = faster recovery, higher = more graceful degradation)

**Code Location:** `JobProcessingConfig.retry_backoff`

---

#### `@enable_dlq` - Dead Letter Queue Control

**Purpose:** Enable/disable Dead Letter Queue for error tracking

**Values:**
- `true` (default for LogAndContinue strategy)
- `false` (for maximum throughput)

**Default:** `true` (for SimpleJobProcessor), `false` (for TransactionalJobProcessor)

**Example:**
```sql
-- @job_mode: simple
-- @failure_strategy: LogAndContinue
-- @enable_dlq: false

-- Disable DLQ for ultra-high throughput (trade off error visibility)
CREATE STREAM high_throughput AS
SELECT * FROM events
EMIT CHANGES;
```

**Impact:** Enables error tracking and recovery (with small overhead)

**Code Location:** `JobProcessingConfig.enable_dlq`

---

#### `@dlq_max_size` - DLQ Capacity Limit

**Purpose:** Maximum number of entries to store in Dead Letter Queue

**Values:**
- Integer: 1-10000
- Default: 100

**Example:**
```sql
-- @enable_dlq: true
-- @dlq_max_size: 500

-- Store up to 500 failed records for investigation
CREATE STREAM debug_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Impact:** Memory usage and error capture capacity (100 entries ≈ 10KB-100KB depending on record size)

**Code Location:** `JobProcessingConfig.dlq_max_size`

---

### Phase 3: Batch Timing Control

Fine-grained control over batching behavior - important for latency-sensitive systems.

#### `@batch_timeout_ms` - Batch Wait Time

**Purpose:** Maximum wait time to collect a full batch (milliseconds)

**Values:**
- Integer: 1-10000 (typical: 10-5000)
- Default: 1000 (1 second)

**Example:**
```sql
-- @batch_size: 500
-- @batch_timeout_ms: 100

-- Emit batches every 100ms OR when 500 records collected (whichever comes first)
CREATE STREAM responsive_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Impact:** Directly controls end-to-end latency

**Latency Calculation:**
```
Worst-case latency ≈ batch_timeout_ms + processing_time

Example:
  batch_timeout: 100ms
  processing_time: 50ms
  → ~150ms latency
```

**Code Location:** `JobProcessingConfig.batch_timeout`

---

#### `@empty_batch_count` - Source EOF Detection

**Purpose:** Number of consecutive empty batches before exiting (for finite sources)

**Values:**
- Integer: 0-10000
- 0 = immediate EOF (for tests)
- 1000 = default (wait for slow sources)

**Default:** `1000`

**Example:**
```sql
-- For testing with finite datasets
-- @empty_batch_count: 0

-- Exit immediately when source exhausted (no polling)
CREATE STREAM test_processor AS
SELECT * FROM file_source
EMIT CHANGES;
```

**Impact:** Affects shutdown time for finite data sources

**Code Location:** `JobProcessingConfig.empty_batch_count`

---

#### `@wait_on_empty_batch_ms` - Empty Batch Wait

**Purpose:** Wait time between empty batch checks (milliseconds)

**Values:**
- Integer: 1-10000
- Default: 1000 (1 second)

**Example:**
```sql
-- For real-time systems
-- @wait_on_empty_batch_ms: 100

-- Check source every 100ms instead of 1000ms (more responsive)
CREATE STREAM responsive_system AS
SELECT * FROM kafka_stream
EMIT CHANGES;
```

**Impact:** Tradeoff between responsiveness and CPU usage

**Code Location:** `JobProcessingConfig.wait_on_empty_batch_ms`

---

### Phase 4: Logging & Observability

Control over logging and monitoring behavior.

#### `@log_progress` - Progress Logging

**Purpose:** Enable/disable periodic progress logging

**Values:**
- `true` (default)
- `false` (for low-overhead systems)

**Default:** `true`

**Example:**
```sql
-- @log_progress: false

-- No progress logging (reduced logging overhead)
CREATE STREAM silent_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Impact:** ~1-2% logging overhead reduction

**Code Location:** `JobProcessingConfig.log_progress`

---

#### `@progress_interval` - Logging Frequency

**Purpose:** Batch count between progress log messages

**Values:**
- Integer: 1-1000
- Default: 10 (log every 10 batches)

**Example:**
```sql
-- @log_progress: true
-- @progress_interval: 100

-- Log progress every 100 batches instead of 10 (reduced log volume)
CREATE STREAM quiet_processor AS
SELECT * FROM events
EMIT CHANGES;
```

**Impact:** Reduces log volume without disabling progress tracking

**Code Location:** `JobProcessingConfig.progress_interval`

---

### Phase 5: Adaptive Mode Specific

Configuration specific to the Adaptive (multi-partition) processor.

#### `@enable_core_affinity` - CPU Core Pinning

**Purpose:** Pin partition threads to CPU cores for cache locality (Linux only)

**Values:**
- `true` (enable pinning)
- `false` (default, no pinning)

**Default:** `false`

**Example:**
```sql
-- @job_mode: adaptive
-- @num_partitions: 8
-- @enable_core_affinity: true

-- Pin each partition to a specific CPU core
CREATE STREAM cache_optimized AS
SELECT * FROM events
EMIT CHANGES;
```

**Impact:**
- 5-10% throughput improvement on large systems
- Reduced context switching
- Requires Linux kernel support

**Prerequisites:**
- `libnuma` library
- CPU affinity capabilities

**Code Location:** `JobProcessorConfig.enable_core_affinity`

---

## Default Values Reference Table

Complete list of all configurable properties with their defaults:

| Property | Type | Default | Min | Max | Unit | Used By |
|----------|------|---------|-----|-----|------|---------|
| `job_mode` | Enum | simple | - | - | - | All |
| `batch_size` | Integer | 100 | 1 | 10000 | records | Simple, Trans |
| `batch_timeout` | Duration | 1000 | 1 | 60000 | ms | Simple, Trans |
| `use_transactions` | Boolean | false | - | - | - | All |
| `failure_strategy` | Enum | LogAndContinue | - | - | - | All |
| `max_retries` | Integer | 10 | 0 | 100 | attempts | Simple, Trans |
| `retry_backoff` | Duration | 5000 | 0 | 300000 | ms | Simple, Trans |
| `log_progress` | Boolean | true | - | - | - | All |
| `progress_interval` | Integer | 10 | 1 | 1000 | batches | All |
| `empty_batch_count` | Integer | 1000 | 0 | 10000 | batches | All |
| `wait_on_empty_batch_ms` | Duration | 1000 | 1 | 10000 | ms | All |
| `enable_dlq` | Boolean | true* | - | - | - | Simple, Trans |
| `dlq_max_size` | Integer | 100 | 1 | 10000 | entries | Simple, Trans |
| `num_partitions` | Integer | CPU count | 1 | 32 | partitions | Adaptive |
| `enable_core_affinity` | Boolean | false | - | - | - | Adaptive |
| `partitioning_strategy` | Enum | sticky | - | - | - | Adaptive |

*DLQ default: `true` for Simple/Adaptive, `false` for Transactional

---

## Annotation Priority & Defaults

### Three-Level Configuration Hierarchy

For each property, Velostream checks sources in this order:

1. **SQL Annotation** (highest priority) - `-- @property: value`
2. **Server Configuration** - From config file/environment
3. **System Default** - Hard-coded in `JobProcessingConfig::default()`

**Example:**
```sql
-- Annotation specifies: @batch_size: 500
-- Server config specifies: batch_size: 200
-- System default: batch_size: 100

-- Result: Uses 500 (annotation wins)
```

### When Properties Aren't Specified

If a property is never set at any level, the system default is used:

```sql
-- Only specifies job mode
-- @job_mode: transactional

-- All other properties use defaults:
-- batch_size: 100
-- max_retries: 10
-- failure_strategy: LogAndContinue
-- enable_dlq: true (for TransactionalJobProcessor, it's false)
-- ... etc
```

---

## Implementation Roadmap

### v0.1 (Current) ✅
- ✅ `@job_mode` - Processor selection
- ✅ `@batch_size` - Batch size control
- ✅ `@num_partitions` - Partition count (adaptive)
- ✅ `@partitioning_strategy` - Routing strategy (adaptive)
- 30 comprehensive tests
- Full documentation

### v0.2 (Planned) 📋
- `@failure_strategy` - Failure handling
- `@max_retries` - Retry count
- `@retry_backoff_ms` - Retry delay
- `@enable_dlq` - DLQ control
- `@dlq_max_size` - DLQ capacity

### v0.3 (Planned) 📋
- `@batch_timeout_ms` - Batch wait time
- `@empty_batch_count` - EOF detection
- `@wait_on_empty_batch_ms` - Empty batch wait
- Fine-grained latency control

### v0.4 (Planned) 📋
- `@log_progress` - Progress logging
- `@progress_interval` - Logging frequency
- Observability tuning

### v0.5 (Planned) 📋
- `@enable_core_affinity` - CPU pinning
- Adaptive mode optimization
- Performance tuning annotations

---

## Recommended Implementation Order

Based on user impact and complexity:

### High Priority (Users Ask For These Often)
1. **`@failure_strategy`** - Critical for reliability
2. **`@max_retries`** - Common production need
3. **`@batch_timeout_ms`** - Latency control (high demand)
4. **`@enable_dlq`** - Debug/monitoring

### Medium Priority (Nice to Have)
5. `@retry_backoff_ms` - Fine-tuning retries
6. `@dlq_max_size` - Memory management
7. `@log_progress` - Noise reduction

### Lower Priority (Advanced Use Cases)
8. `@empty_batch_count` - Finite sources, tests
9. `@wait_on_empty_batch_ms` - Fine-tuning
10. `@enable_core_affinity` - Performance optimization
11. `@progress_interval` - Logging control

---

## Example: Fully Configured Query (Future)

Once all annotations are available:

```sql
-- Financial trading system with precise control
-- @job_mode: transactional          -- ACID compliance
-- @batch_size: 50                   -- Small batches for low latency
-- @batch_timeout_ms: 200            -- 200ms max wait
-- @failure_strategy: FailBatch      -- Atomic batches
-- @max_retries: 5                   -- Retry failed batches
-- @retry_backoff_ms: 1000           -- 1 second between retries
-- @enable_dlq: true                 -- Track failures
-- @dlq_max_size: 500                -- Large DLQ for investigation
-- @log_progress: true               -- Monitor progress
-- @progress_interval: 50            -- Frequent updates
-- @num_partitions: 8                -- Multi-core processing
-- @partitioning_strategy: hash      -- Hash on GROUP BY
-- @enable_core_affinity: true       -- CPU pinning for performance

SELECT
    trader_id,
    symbol,
    SUM(quantity) as total_volume,
    AVG(price) as avg_price
FROM trade_events
GROUP BY trader_id, symbol
EMIT CHANGES;
```

**Behavior:**
- Processes in atomic batches (FailBatch)
- Max 50 records per batch, 200ms timeout
- Retries up to 5 times with 1s between retries
- Tracks failures in DLQ (up to 500 entries)
- Logs progress every 50 batches
- Executes in 8 parallel partitions
- Records hashed by GROUP BY columns
- Threads pinned to CPU cores
- ~961K rec/sec throughput with ACID guarantees

---

## Migration Path

### Simple → Feature-Rich (Gradual)

```sql
-- v0.1: Start simple
-- @job_mode: transactional
SELECT * FROM events EMIT CHANGES;

-- v0.2: Add reliability
-- @job_mode: transactional
-- @failure_strategy: FailBatch
-- @max_retries: 5
SELECT * FROM events EMIT CHANGES;

-- v0.3: Add latency control
-- @job_mode: transactional
-- @batch_timeout_ms: 100
-- @failure_strategy: FailBatch
-- @max_retries: 5
SELECT * FROM events EMIT CHANGES;

-- v0.4: Add visibility
-- @job_mode: transactional
-- @batch_timeout_ms: 100
-- @failure_strategy: FailBatch
-- @max_retries: 5
-- @log_progress: true
-- @progress_interval: 100
SELECT * FROM events EMIT CHANGES;
```

---

## Documentation Strategy

### Current (v0.1)
- ✅ [Job Annotations Guide](./job-annotations-guide.md) - 4 annotations
- ✅ [Job Processor Configuration Guide](./job-processor-configuration-guide.md) - All defaults documented

### Future Updates
Each new annotation release should:
1. Update `job-annotations-guide.md` with new annotation sections
2. Update defaults table in `job-processor-configuration-guide.md`
3. Add usage examples
4. Update this roadmap with completion status

---

## See Also

- [Job Annotations Guide](./job-annotations-guide.md) - Currently supported annotations
- [Job Processor Configuration Guide](./job-processor-configuration-guide.md) - Complete defaults reference
- [Failure Handling Guide](./by-task/failure-handling.md) - Planned document for failure strategies
- [Performance Tuning Guide](../developer/performance-tuning.md) - Optimization techniques
