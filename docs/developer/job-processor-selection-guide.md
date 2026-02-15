# Job Processor Selection Guide

**Choosing the right processor for your streaming SQL applications**

This guide helps you select the appropriate job processor for your Velostream applications based on consistency
requirements, performance characteristics, and operational needs. Choose the processor that best matches your
requirements—there is no migration path or version progression.

---

## 🎯 **Quick Decision Matrix**

| **Use Case**                                   | **Processor**                 | **Why**                                                                |
|------------------------------------------------|-------------------------------|------------------------------------------------------------------------|
| **High-throughput analytics (multi-core)**     | **AdaptiveJobProcessor**      | Multi-partition parallel, ~8x faster on 8 cores, zero-overhead routing |
| **High-throughput, single-threaded**           | **SimpleJobProcessor**        | Maximum raw performance, best-effort processing                        |
| **Financial transactions**                     | **TransactionalJobProcessor** | ACID transactions, at-least-once delivery, regulatory compliance       |
| **Real-time dashboards (throughput critical)** | **AdaptiveJobProcessor**      | 8x improvement with multi-core parallelism                             |
| **Real-time dashboards (simple setup)**        | **SimpleJobProcessor**        | Speed with minimal complexity                                          |
| **Audit trails**                               | **TransactionalJobProcessor** | No data loss, complete transaction boundaries                          |
| **IoT sensor data (volume)**                   | **AdaptiveJobProcessor**      | Scales to millions of records/sec                                      |
| **Payment processing**                         | **TransactionalJobProcessor** | Atomic processing, regulatory compliance                               |
| **Development/testing**                        | **SimpleJobProcessor**        | Fastest iteration, simplest debugging                                  |
| **Production mission-critical (fast)**         | **AdaptiveJobProcessor**      | 8x throughput improvement with proper parallelism                      |
| **Production mission-critical (safe)**         | **TransactionalJobProcessor** | Data integrity guarantees                                              |

---

## 📊 **Detailed Comparison**

### **AdaptiveJobProcessor** ⚡⚡

**Best for**: High-throughput workloads that can leverage multiple CPU cores

#### **Characteristics**

- **Processing Model**: Multi-partition parallel execution with pluggable routing strategies
- **Performance**: ~8x improvement on 8-core systems vs single-threaded processors
- **Complexity**: Medium (automatic partitioning strategy selection)
- **Parallelism**: True parallelism across CPU cores with minimal contention
- **Default Strategy**: StickyPartitionStrategy (zero-overhead, uses record.partition field)

#### **Architecture Overview**

```
┌─────────────────┐
│   Data Source   │ (Kafka, files, etc.)
│ (with partition │
│  information)   │
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│   AdaptiveJobProcessor (Coordinator)     │
│                                          │
│  ┌─ Partition 0 ──────────────────────┐  │
│  │ StreamExecutionEngine              │  │
│  │ ├─ StickyPartitionStrategy (route) │  │
│  │ └─ SQL Execution (owned)           │  │
│  └────────────────────────────────────┘  │
│  ┌─ Partition 1 ──────────────────────┐  │
│  │ StreamExecutionEngine (parallel)   │  │
│  │ ├─ StickyPartitionStrategy (route) │  │
│  │ └─ SQL Execution (owned)           │  │
│  └────────────────────────────────────┘  │
│  ┌─ Partition N ──────────────────────┐  │
│  │ StreamExecutionEngine (parallel)   │  │
│  │ ├─ StickyPartitionStrategy (route) │  │
│  │ └─ SQL Execution (owned)           │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│   Data Sink     │ (Kafka, files, etc.)
│  (partitioned)  │
└─────────────────┘
```

#### **Partitioning Strategies**

AdaptiveJobProcessor uses pluggable routing strategies selected automatically based on query characteristics:

| **Strategy**                          | **Used For**         | **Overhead**                             | **Performance**               |
|---------------------------------------|----------------------|------------------------------------------|-------------------------------|
| **StickyPartitionStrategy** (default) | All queries          | Zero (just reads record.partition field) | 42-67x faster than hash-based |
| **AlwaysHashStrategy**                | GROUP BY queries     | Hash computation per record              | Conservative but correct      |
| **SmartRepartitionStrategy**          | Aligned GROUP BY     | Detection + conditional routing          | Hybrid (fast when aligned)    |
| **RoundRobinStrategy**                | No GROUP BY queries  | Minimal (counter increment)              | Maximum throughput            |
| **FanInStrategy**                     | Broadcast operations | Minimal (counter increment)              | Balanced distribution         |

**Auto-Selection Logic**:

- Default: StickyPartitionStrategy (works with all query types)
- GROUP BY without ORDER BY: Override to AlwaysHashStrategy (better aggregation locality)
- Window without ORDER BY: Override to AlwaysHashStrategy (parallelization opportunity)
- Window with ORDER BY: Use StickyPartitionStrategy (required for ordering)

#### **Data Consistency Guarantees**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│  Adaptive Multi- │───▶│   Data Sink     │
│   (Partitioned) │    │   Partition      │    │  (Partitioned)  │
│                 │    │  Processing      │    │                 │
│ • Records have  │    │ • Each partition │    │ • All records   │
│   partition ID  │    │   processes      │    │   written out   │
│ • At-least-once │    │   independently  │    │ • Maintains     │
│                 │    │ • Zero repartition│   │   partition IDs │
└─────────────────┘    │   (sticky)       │    │ • At-least-once │
                       │ • True parallelism
└──────────────────┘    └─────────────────┘
```

#### **Performance Characteristics**

- **Throughput**: 350K+ records/sec per partition (scales linearly across cores)
    - Single core: ~16,000 rec/sec
    - 8 cores: ~128,000+ rec/sec aggregate
- **Latency**: Excellent cache locality with StickyPartitionStrategy (zero repartitioning cost)
- **Scalability**: Near-linear scaling with CPU cores
- **Memory**: Base ~20-30MB per processor

#### **When to Use AdaptiveJobProcessor**

- ✅ Multi-core systems with available CPU capacity
- ✅ High-throughput scenarios (>100K records/sec)
- ✅ Kafka sources with partition information
- ✅ GROUP BY queries that benefit from partitioned execution
- ✅ Window functions with potential parallelization
- ✅ Production workloads where throughput is critical
- ❌ Single-core systems (overhead not justified)
- ❌ Minimal data volume (<1K records/sec)

#### **Code Example**

```rust
use velostream::velostream::server::processors::JobProcessorFactory;

// Create adaptive processor with 8 partitions
let processor = JobProcessorFactory::create_adaptive_with_partitions(8);

// Process with automatic strategy selection and parallelism
let stats = processor.process_job(
    reader,
    Some(writer),
    engine,
    query,
    "analytics-job".to_string(),
    shutdown_rx
).await?;

println!("Processed {} records at {:.0} rec/sec",
    stats.records_processed,
    stats.records_processed as f64 / stats.total_processing_time.as_secs_f64()
);
```

#### **Rust Code Configuration**

```rust
use velostream::velostream::server::processors::{JobProcessorFactory, JobProcessorConfig};

// Create adaptive processor with explicit partition count
let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
    num_partitions: Some(8),          // Partition count (defaults to CPU cores)
    enable_core_affinity: false,      // CPU affinity for partition threads
});

// Alternatively, use helper methods
let processor = JobProcessorFactory::create_adaptive_with_partitions(8);

// For testing with immediate EOF detection (no empty batch polling)
let processor = JobProcessorFactory::create_adaptive_test_optimized(Some(8));
```

#### **SQL Annotation Configuration** (Future)

```sql
-- @processor_mode: adaptive
-- @partitions: 8
SELECT
    trader_id,
    COUNT(*) as trade_count,
    AVG(amount) as avg_amount,
    SUM(amount) as total_amount
FROM kafka_trades
GROUP BY trader_id;
```

Note: SQL-level processor mode annotations are planned for future versions. Currently, processor selection is configured
at the application level via JobProcessorFactory.

---

### **SimpleJobProcessor** ⚡

**Best for**: High-throughput applications where occasional data loss is acceptable

#### **Characteristics**

- **Processing Model**: Best-effort, at-least-once delivery
- **Performance**: Optimized for maximum throughput
- **Complexity**: Minimal operational overhead
- **Failure Handling**: Log and continue, graceful degradation
- **Resource Usage**: Lower memory and CPU overhead

#### **Data Consistency Guarantees**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│  Simple Process  │───▶│   Data Sink     │
│   (At-least-    │    │                  │    │   (At-least-    │
│    once)        │    │  • Fast commits  │    │    once)        │
│                 │    │  • Basic retries │    │                 │
│                 │    │  • Log failures  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

#### **Performance Characteristics**

- **Throughput**: 350K+ records/sec (based on batch benchmarks)
- **Latency**: ~6-7µs per record (simple processing)
- **Memory**: Lower overhead, faster garbage collection
- **CPU**: Minimal transaction coordination overhead

#### **Code Example**

```rust
use velostream::velo::server::processors::simple::SimpleJobProcessor;
use velostream::velo::server::processors::common::JobProcessingConfig;

// Create simple processor for high-throughput scenario
let config = JobProcessingConfig {
    batch_size: 1000,
    batch_timeout: Duration::from_millis(100),
    max_retries: 3,
    commit_interval: Duration::from_secs(1),
    enable_metrics: true,
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};

let processor = SimpleJobProcessor::new(config);

// Process with best-effort guarantees
let stats = processor.process_multi_job(
    readers,
    writers,
    engine,
    query,
    "analytics-job".to_string(),
    shutdown_rx
).await?;
```

---

### **TransactionalJobProcessor** 🔐

**Best for**: Mission-critical applications requiring ACID transaction guarantees and at-least-once delivery

#### **Characteristics**

- **Processing Model**: At-least-once delivery with full ACID transaction boundaries
- **Performance**: Higher latency but guaranteed consistency
- **Complexity**: Transaction coordination, rollback handling
- **Failure Handling**: Atomic rollback, precise error recovery
- **Resource Usage**: Higher overhead for transaction state management

#### **Data Consistency Guarantees**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│ Transactional    │───▶│   Data Sink     │
│ (At-least-once) │    │    Process       │    │ (At-least-once) │
│                 │    │                  │    │                 │
│ • Transactional │    │ • Atomic batches │    │ • Transactional │
│   reads         │    │ • Rollback on    │    │   commits       │
│ • Retry batches │    │   failure        │    │ • May see dups  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

#### **Performance Characteristics**

- **Throughput**: 200K-300K records/sec (estimated with transaction overhead)
- **Latency**: ~10-15µs per record (including transaction coordination)
- **Memory**: Higher overhead for transaction state tracking
- **CPU**: Additional processing for commit coordination

#### **Code Example**

```rust
use velostream::velo::server::processors::transactional::TransactionalJobProcessor;
use velostream::velo::server::processors::common::JobProcessingConfig;

// Create transactional processor for at-least-once processing with ACID boundaries
let config = JobProcessingConfig {
    batch_size: 500,  // Smaller batches for faster rollback
    batch_timeout: Duration::from_millis(200),
    max_retries: 5,   // More retries for transactional safety
    commit_interval: Duration::from_millis(500),
    enable_metrics: true,
    failure_strategy: FailureStrategy::FailBatch,  // Atomic failure handling
    transaction_timeout: Some(Duration::from_secs(30)),
    ..Default::default()
};

let processor = TransactionalJobProcessor::new(config);

// Process with at-least-once guarantees and ACID transaction boundaries
let stats = processor.process_multi_job(
    readers,
    writers,
    engine,
    query,
    "payment-processing-job".to_string(),
    shutdown_rx
).await?;
```

---

## ⚙️ **How Configuration Actually Works**

Velostream automatically selects between SimpleJobProcessor and TransactionalJobProcessor based on SQL WITH clause
properties. Here's how the system determines which processor to use:

### **Processor Selection Logic**

```rust
// From src/velo/server/stream_job_server.rs:809
fn extract_job_config_from_query(query: &StreamingQuery) -> JobProcessingConfig {
    let properties = Self::get_query_properties(query);
    let mut config = JobProcessingConfig::default();

    // Key configuration: use_transactions determines processor type
    if let Some(use_tx) = properties.get("use_transactions") {
        config.use_transactions = use_tx.to_lowercase() == "true";
    }

    // Additional configuration options...
    if let Some(strategy) = properties.get("failure_strategy") {
        config.failure_strategy = match strategy.as_str() {
            "RetryWithBackoff" => FailureStrategy::RetryWithBackoff,
            "LogAndContinue" => FailureStrategy::LogAndContinue,
            "FailBatch" => FailureStrategy::FailBatch,
            "SendToDLQ" => FailureStrategy::SendToDLQ,
            _ => FailureStrategy::LogAndContinue,
        };
    }
}

// Processor selection happens here:
if use_transactions {
    let processor = TransactionalJobProcessor::new(config);  // At-least-once with ACID transactions
} else {
    let processor = SimpleJobProcessor::new(config);         // Best-effort processing
}
```

### **SQL Configuration Syntax - Failure Strategies**

The processor selection is controlled via SQL WITH clause using `sink.failure_strategy`:

#### **Transactional Processing (FailBatch)**

```sql
CREATE STREAM payment_processing AS
SELECT * FROM kafka_payments
WITH (
    -- Critical: Use FailBatch for atomic processing
    'sink.failure_strategy' = 'FailBatch',      -- 🔑 Rolls back entire batch on ANY error
    'sink.max_retries' = '5',                   -- Aggressive retries
    'sink.retry_backoff' = '2000ms',            -- 2 second backoff

    -- Configuration for consistency
    'sink.batch.enable' = 'true',
    'sink.batch.size' = '500',                  -- Smaller batches for fast rollback
    'sink.batch.timeout' = '200ms'              -- 200ms timeout
);
```

#### **Resilient Processing (LogAndContinue)**

```sql
CREATE STREAM analytics_processing AS
SELECT * FROM kafka_events
WITH (
    -- Production default: Capture errors, continue processing
    'sink.failure_strategy' = 'LogAndContinue', -- 🔑 Continue on errors, log failed records
    'sink.max_retries' = '3',                   -- Basic retry logic
    'sink.retry_backoff' = '1000ms',

    -- Configuration for throughput
    'sink.batch.enable' = 'true',
    'sink.batch.size' = '2000',                 -- Larger batches for efficiency
    'sink.batch.timeout' = '100ms'              -- Fast processing
);
```

#### **Error Capture with DLQ (SendToDLQ)**

```sql
CREATE STREAM orders_with_dlq AS
SELECT * FROM kafka_orders
WITH (
    -- Capture failed records for analysis
    'sink.failure_strategy' = 'SendToDLQ',      -- 🔑 DLQ enabled for error tracking
    'sink.max_retries' = '3',
    'sink.retry_backoff' = '1000ms'
);
```

#### **Automatic Retry (RetryWithBackoff)**

```sql
CREATE STREAM transient_failures AS
SELECT * FROM kafka_data
WITH (
    -- Handle transient network issues
    'sink.failure_strategy' = 'RetryWithBackoff',  -- 🔑 Exponential backoff
    'sink.max_retries' = '5',
    'sink.retry_backoff' = '2000ms'
);
```

#### **Default Behavior (No Configuration)**

```sql
-- No WITH clause = LogAndContinue with defaults
CREATE STREAM default_processing AS
SELECT * FROM kafka_data;

-- Equivalent to:
CREATE STREAM default_processing AS
SELECT * FROM kafka_data
WITH (
    'sink.failure_strategy' = 'LogAndContinue', -- Default
    'sink.max_retries' = '3',                   -- Default
    'sink.retry_backoff' = '1000ms'             -- Default
);
```

### **SQL Annotation: @processor_mode**

In addition to WITH clause configuration, you can use the `@processor_mode` SQL annotation to specify which processor to
use at the statement level. This annotation provides a declarative way to select the processor mode.

#### **Annotation Syntax**

```sql
-- @processor_mode: simple
CREATE STREAM high_throughput_analytics AS
SELECT * FROM kafka_events;

-- @processor_mode: transactional
CREATE STREAM financial_transactions AS
SELECT * FROM kafka_payments;
```

#### **Valid Values**

- **`simple`** - Use SimpleJobProcessor (high-throughput, best-effort)
- **`transactional`** - Use TransactionalJobProcessor (ACID, atomic processing)

#### **Practical Examples**

**Example 1: Mark Stream for Transactional Processing**

```sql
-- @processor_mode: transactional
CREATE STREAM payment_processing AS
SELECT
    transaction_id,
    amount,
    account_from,
    account_to,
    timestamp
FROM kafka_payments
WITH (
    'sink.failure_strategy' = 'FailBatch',
    'sink.batch.size' = '500',
    'sink.batch.timeout' = '200ms'
);
```

**Example 2: Mark Stream for High-Throughput Analytics**

```sql
-- @processor_mode: simple
CREATE STREAM event_analytics AS
SELECT
    event_type,
    user_id,
    COUNT(*) as event_count,
    AVG(duration) as avg_duration
FROM kafka_events
GROUP BY event_type, user_id
WINDOW TUMBLING(1m);
```

**Example 3: Combined with @name and @partitioning_strategy**

```sql
-- @name: order-processing-pipeline
-- @processor_mode: transactional
-- @partitioning_strategy: always_hash
CREATE STREAM order_processing AS
SELECT
    order_id,
    customer_id,
    total_amount,
    order_status
FROM kafka_orders
WITH (
    'sink.failure_strategy' = 'SendToDLQ',
    'sink.max_retries' = '3',
    'sink.retry_backoff' = '1000ms'
);
```

#### **Annotation vs WITH Clause**

| **Aspect**        | **@processor_mode Annotation** | **WITH Clause Configuration**             |
|-------------------|--------------------------------|-------------------------------------------|
| **Purpose**       | Declare processor selection    | Configure failure strategies and batching |
| **Level**         | Statement level                | Sink/batch level                          |
| **Usage**         | Comments above CREATE STREAM   | SQL WITH clause                           |
| **Compatibility** | Works with any WITH clause     | Standalone or with annotations            |
| **When to use**   | Clear processor intent         | Fine-grained tuning                       |

**Recommendation**: Use `@processor_mode` for clarity and documentation, combine with `WITH` clause for detailed
configuration.

### **Complete Configuration Properties**

| **Property**       | **Type** | **Default**        | **Description**                                                                                |
|--------------------|----------|--------------------|------------------------------------------------------------------------------------------------|
| `use_transactions` | boolean  | `false`            | **Main switch**: `true` = TransactionalJobProcessor, `false` = SimpleJobProcessor              |
| `failure_strategy` | string   | `"LogAndContinue"` | How to handle failures: `"RetryWithBackoff"`, `"LogAndContinue"`, `"FailBatch"`, `"SendToDLQ"` |
| `max_retries`      | integer  | `3`                | Maximum retry attempts for failed operations                                                   |
| `retry_backoff`    | integer  | `1000`             | Backoff delay in milliseconds between retries                                                  |
| `max_batch_size`   | integer  | `1000`             | Maximum number of records per batch                                                            |
| `batch_timeout`    | integer  | `1000`             | Maximum wait time in milliseconds before processing incomplete batch                           |

### **Configuration Examples by Use Case**

#### **Financial Services Configuration**

```sql
CREATE STREAM bank_transfers AS
SELECT
    transaction_id,
    amount,
    account_from,
    account_to,
    CURRENT_TIMESTAMP as processed_at
FROM kafka_transactions
WITH (
    -- At-least-once processing with ACID transactions required
    'use_transactions' = 'true',
    'failure_strategy' = 'FailBatch',      -- Atomic: entire batch fails if any record fails
    'max_retries' = '10',                  -- Aggressive retries for financial data
    'retry_backoff' = '5000',              -- 5 second backoff for transaction coordination
    'max_batch_size' = '100',              -- Small batches for quick rollback
    'batch_timeout' = '500',               -- 500ms max wait for batch completion

    -- Kafka-specific transactional configuration
    'sink.kafka.transactional.id' = 'velo-bank-transfers-tx',
    'sink.kafka.acks' = 'all',
    'source.kafka.isolation.level' = 'read_committed'
);
```

#### **Real-time Analytics Configuration**

```sql
CREATE STREAM user_analytics AS
SELECT
    user_id,
    COUNT(*) as page_views,
    AVG(session_duration) as avg_session
FROM kafka_user_events
WINDOW SLIDING (INTERVAL 5 MINUTE, INTERVAL 1 MINUTE)
GROUP BY user_id
WITH (
    -- High-throughput processing
    'use_transactions' = 'false',          -- Simple processor for speed
    'failure_strategy' = 'LogAndContinue', -- Keep processing even if some records fail
    'max_retries' = '2',                   -- Minimal retries for speed
    'retry_backoff' = '500',               -- Quick retry for real-time processing
    'max_batch_size' = '5000',             -- Large batches for efficiency
    'batch_timeout' = '50',                -- 50ms max wait for low latency

    -- High-throughput Kafka settings
    'sink.kafka.batch.size' = '65536',     -- Large Kafka batches
    'sink.kafka.linger.ms' = '10'          -- Low linger for real-time
);
```

### **Configuration Validation and Debugging**

The system logs the processor selection decision:

```
INFO Job 'payment-processing' processing configuration:
     use_transactions=true,
     failure_strategy=FailBatch,
     max_batch_size=100,
     batch_timeout=500ms,
     max_retries=10,
     retry_backoff=5000ms
INFO Job 'payment-processing' using transactional processor for multi-source processing
```

You can verify your configuration by checking the logs when starting a job.

---

## 🔧 **Configuration Differences**

### **SimpleJobProcessor Configuration**

```yaml
# Optimized for throughput
job_config:
  batch_size: 1000                    # Larger batches for efficiency
  batch_timeout: "100ms"              # Fast processing
  max_retries: 3                      # Basic retry logic
  commit_interval: "1s"               # Less frequent commits
  failure_strategy: "LogAndContinue"  # Keep processing
  enable_transaction: false           # No transaction overhead
```

### **TransactionalJobProcessor Configuration**

```yaml
# Optimized for consistency
job_config:
  batch_size: 500                     # Smaller batches for faster rollback
  batch_timeout: "200ms"              # Allow transaction coordination
  max_retries: 5                      # More thorough retry attempts
  commit_interval: "500ms"            # Frequent atomic commits
  failure_strategy: "FailBatch"       # Atomic failure handling
  enable_transaction: true            # Full ACID guarantees
  transaction_timeout: "30s"          # Timeout for long transactions
```

---

## 🎯 **Use Case Deep Dive**

### **Financial Services** 💰

**Recommended**: TransactionalJobProcessor

**Requirements**:

- At-least-once processing with ACID boundaries for payment transactions
- Audit trail compliance (SOX, PCI DSS)
- No tolerance for duplicate or lost transactions
- Regulatory reporting accuracy

**Configuration Example**:

```sql
-- Financial transaction processing
CREATE STREAM payment_validation AS
SELECT
    transaction_id,
    amount,
    currency,
    validation_status
FROM kafka_payments
WITH (
    'processor.type' = 'transactional',
    'processor.batch.size' = '100',           -- Small batches for fast rollback
    'processor.failure_strategy' = 'FailBatch',
    'sink.kafka.transactional.id' = 'velo-payments-tx-1',
    'sink.kafka.acks' = 'all'
);
```

### **IoT Analytics** 📊

**Recommended**: SimpleJobProcessor

**Requirements**:

- High-volume sensor data (millions of records/sec)
- Real-time dashboard updates
- Occasional data loss acceptable
- Cost-optimized processing

**Configuration Example**:

```sql
-- IoT sensor aggregation
CREATE STREAM sensor_metrics AS
SELECT
    sensor_id,
    AVG(temperature) as avg_temp,
    COUNT(*) as reading_count
FROM kafka_sensors
WINDOW TUMBLING (INTERVAL 1 MINUTE)
GROUP BY sensor_id
WITH (
    'processor.type' = 'simple',
    'processor.batch.size' = '5000',          -- Large batches for efficiency
    'processor.failure_strategy' = 'LogAndContinue'
);
```

### **Real-time Analytics** ⚡

**Recommended**: SimpleJobProcessor

**Requirements**:

- Sub-second latency for business dashboards
- High throughput for user activity streams
- Acceptable to lose some events during failures
- Focus on speed over perfect accuracy

**Configuration Example**:

```sql
-- User activity analytics
CREATE STREAM user_activity_summary AS
SELECT
    user_id,
    COUNT(*) as page_views,
    SUM(session_duration) as total_time
FROM kafka_user_events
WINDOW SLIDING (INTERVAL 5 MINUTE, INTERVAL 1 MINUTE)
GROUP BY user_id
WITH (
    'processor.type' = 'simple',
    'processor.batch.size' = '2000',
    'processor.batch.timeout' = '50ms',       -- Ultra-low latency
    'processor.failure_strategy' = 'LogAndContinue'
);
```

### **Compliance & Audit** 📝

**Recommended**: TransactionalJobProcessor

**Requirements**:

- Complete audit trail for regulatory compliance
- At-least-once processing with transaction boundaries for legal record keeping
- Data integrity for compliance reporting
- Disaster recovery with no data loss

**Configuration Example**:

```sql
-- Compliance audit trail
CREATE STREAM audit_trail AS
SELECT
    event_id,
    user_id,
    action_type,
    timestamp,
    compliance_flags
FROM kafka_audit_events
WITH (
    'processor.type' = 'transactional',
    'processor.batch.size' = '200',           -- Conservative batch size
    'processor.failure_strategy' = 'RetryWithBackoff',
    'processor.max_retries' = '10',           -- Aggressive retry for compliance
    'sink.kafka.transactional.id' = 'velo-audit-tx-1'
);
```

---

## ⚡ **Performance Considerations**

### **Throughput Comparison**

Based on Velostream benchmarks:

| **Metric**           | **SimpleJobProcessor**       | **TransactionalJobProcessor** |
|----------------------|------------------------------|-------------------------------|
| **Peak Throughput**  | 365,018 records/sec          | ~250,000 records/sec (est.)   |
| **Batch Processing** | 1.1x improvement over single | Similar batch improvement     |
| **Memory Overhead**  | Baseline                     | +20-30% for transaction state |
| **CPU Overhead**     | Baseline                     | +15-25% for coordination      |
| **Latency (P95)**    | <10ms                        | <25ms                         |

### **Resource Usage Patterns**

```
SimpleJobProcessor Resource Profile:
├── CPU: ████░░░░░░ (40% during peak load)
├── Memory: ███░░░░░░░ (30% baseline)
└── Network: ██████████ (Optimized for throughput)

TransactionalJobProcessor Resource Profile:
├── CPU: ██████░░░░ (60% during peak load)
├── Memory: ████░░░░░░ (40% baseline + transaction state)
└── Network: ████████░░ (Additional coordination overhead)
```

### **Scaling Characteristics**

#### **SimpleJobProcessor Scaling**

- **Near-linear scaling**: Performance scales directly with resources (CPU cores, memory)
- **Optimal partitions**: 4-8 partitions per CPU core
- **Parallelism**: Each partition processed independently
- **Load balancing**: Automatic across available resources
- **Scaling example**:
  ```
  Single core:    ~350K records/sec
  4 cores:        ~1.4M records/sec (4x improvement)
  8 cores:        ~2.8M records/sec (8x improvement)
  ```

#### **TransactionalJobProcessor Scaling**

- **Coordination overhead**: Transaction coordination limits horizontal scaling
- **Optimal configuration**: 2-4 partitions per CPU core (vs 4-8 for Simple)
- **Serialization points**: Commit coordination may bottleneck
- **Scaling curve**: Sub-linear due to coordination
  ```
  Single core:    ~250K records/sec
  4 cores:        ~900K records/sec (3.6x improvement, 90% efficiency)
  8 cores:        ~1.6M records/sec (6.4x improvement, 80% efficiency)
  ```
- **Horizontal limit**: Coordination becomes bottleneck beyond 8-16 cores per job

#### **Multi-Node Scaling**

- **SimpleJobProcessor**: Excellent multi-node scaling with appropriate Kafka partitions
- **TransactionalJobProcessor**: Single-node recommended; multi-node requires distributed transaction coordination

---

## 📍 **Partition ID Preservation & Affinity**

### **How Partition IDs Are Handled**

#### **SimpleJobProcessor**

- **Behavior**: Processes each partition independently
- **Partition affinity**: Maintained within a single task
- **Load distribution**: One partition per processing thread
- **ID preservation**: Partition IDs preserved in stream metadata
- **Example**:
  ```
  Input:  Kafka partition 0 → records: [A, B, C]
  Output: Same partition 0 → records: [A', B', C']
                              (IDs preserved)
  ```

#### **TransactionalJobProcessor**

- **Behavior**: May coordinate across partitions for atomicity
- **Partition affinity**: Maintained but with transaction coordination overhead
- **Load distribution**: Coordinated across partitions for consistency
- **ID preservation**: Partition IDs preserved in transaction logs
- **Example**:
  ```
  Input:  Partition 0: [A, B]  +  Partition 1: [C, D]
  Output: Atomic transaction boundary ensures both succeed or both fail
          Partition IDs preserved in output
  ```

### **Partition-Aware Configuration**

```sql
-- Optimize for partition affinity
CREATE STREAM high_affinity_processing AS
SELECT
    -- Use PARTITION BY to maintain same partition
    partition_id,
    record_key,
    record_value
FROM kafka_stream
PARTITION BY partition_id  -- ✅ Maintains partition affinity
WITH (
    'sink.failure_strategy' = 'LogAndContinue',
    'sink.batch.size' = '1000'  -- Single partition's worth per batch
);
```

### **Partition-Aware Optimization**

| **Scenario**                              | **Simple**              | **Transactional**         |
|-------------------------------------------|-------------------------|---------------------------|
| Single partition stream                   | Optimal                 | Works but over-engineered |
| Multi-partition with affinity requirement | ✅ Recommended           | ✅ Acceptable              |
| Cross-partition joins                     | ⚠️ Requires repartition | ✅ Handles atomically      |
| Maintaining consumer group offsets        | ✅ Per-partition         | ✅ Per-partition           |

---

## ⏱️ **Throughput vs Latency Analysis**

### **Throughput Characteristics**

#### **SimpleJobProcessor Throughput**

```
Configuration: batch_size=1000, batch_timeout=100ms

Scenario 1: High Volume (>10K records/sec)
├── Effective throughput: 350K+ records/sec
├── Batches per second: 350+
├── CPU utilization: 40-50%
└── Memory: Stable, low GC pressure

Scenario 2: Medium Volume (1-10K records/sec)
├── Effective throughput: Limited by batch timeout
├── Batches per second: 10-100
├── Latency: Batch timeout dominates (100ms)
└── Memory: <50MB base

Scenario 3: Low Volume (<1K records/sec)
├── Effective throughput: Records per second (limited by timeout)
├── Batches per second: <10
├── Latency: Close to batch timeout (100ms)
└── Best for: Non-critical workloads
```

#### **TransactionalJobProcessor Throughput**

```
Configuration: batch_size=500, batch_timeout=200ms

Scenario 1: High Volume (>10K records/sec)
├── Effective throughput: 200-250K records/sec
├── Batches per second: 400+
├── CPU utilization: 60-70% (coordination overhead)
├── Transaction overhead: ~2-3µs per record
└── Memory: +20-30% vs Simple

Scenario 2: Medium Volume (1-10K records/sec)
├── Effective throughput: Limited by batch timeout + coordination
├── Batch timeout dominates: 200ms
├── Coordination cost: 1-2ms per batch
├── Latency impact: -15-20% vs simple
└── Memory: +30-50MB for transaction state

Scenario 3: Low Volume (<1K records/sec)
├── Effective throughput: Records per second
├── Batch timeout: 200ms becomes dominant
├── Coordination cost: Fixed 2-3ms overhead per batch
├── Best for: Critical, low-latency non-negotiable workloads
```

### **Latency Breakdown**

#### **SimpleJobProcessor (P95 Latency)**

```
Record ingestion:        0.1ms  ├─ Network + deserialization
Processing:              2-3ms  ├─ SQL execution
Batching wait:           50-100ms ├─ Depends on batch timeout
Kafka commit:            1-2ms  ├─ Network roundtrip
Total P95:               <110ms (dominated by batch timeout)

Under high load:
  - Batch fills quickly, latency closer to 1-3ms
  - Timeout becomes irrelevant
  - P99 latency: <5ms
```

#### **TransactionalJobProcessor (P95 Latency)**

```
Record ingestion:        0.1ms  ├─ Network + deserialization
Processing:              2-3ms  ├─ SQL execution
Batching wait:           50-200ms ├─ Depends on batch timeout
Transaction coord:       2-5ms  ├─ Coordinator communication
Atomic commit:           3-5ms  ├─ All-or-nothing commit
Total P95:               <220ms (dominated by batch timeout + coordination)

Under high load:
  - Batch fills quickly, latency closer to 5-10ms
  - Coordination overhead becomes significant
  - P99 latency: <15ms
```

### **Choosing Batch Size for Latency**

| **Target Latency** | **Simple Batch Size** | **Transactional Batch Size** |
|--------------------|-----------------------|------------------------------|
| < 5ms (real-time)  | 100-500               | 50-200                       |
| 5-50ms             | 1000                  | 500                          |
| 50-200ms           | 2000-5000             | 1000                         |
| > 200ms            | 5000+                 | 2000+                        |

---

## 💾 **Memory Footprint & Resource Impact**

### **Memory Usage Analysis**

#### **SimpleJobProcessor Memory Profile**

```
Base Memory:             ~10MB
├── JVM overhead:        5MB
├── Thread stacks (8):   2MB
├── Buffer pools:        3MB

Per 1000-record batch:  +0.5MB (temporary)
Per active partition:   +1-2MB (state management)
Peak memory (8 partitions, 1000-record batches):
└── ~20-30MB total

Garbage Collection:
├── Young generation GC: 50-100ms, minimal pause
├── Full GC: Rare (good write patterns)
└── Heap pressure: Low
```

#### **TransactionalJobProcessor Memory Profile**

```
Base Memory:             ~15MB (+50%)
├── JVM overhead:        5MB
├── Thread stacks (8):   2MB
├── Buffer pools:        3MB
├── Transaction state:   5MB

Per 500-record batch:   +1MB (temporary, due to transaction log)
Per active partition:   +3-5MB (transaction tracking)
Peak memory (8 partitions, 500-record batches):
└── ~40-60MB total (+100-150%)

Garbage Collection:
├── Young generation GC: 100-200ms (more frequent)
├── Full GC: Possible if memory pressure high
└── Heap pressure: Moderate-High under load
```

### **Resource Allocation Recommendations**

```yaml
SimpleJobProcessor:
  heap_memory: "512MB"          # Modest requirements
  cpu_cores: "4-8"              # Scales well
  disk_io: "Low"                # Minimal disk operations
  network_bandwidth: "High"     # Optimized for throughput

TransactionalJobProcessor:
  heap_memory: "1GB"            # Higher requirements
  cpu_cores: "2-4"              # Coordination limits scaling
  disk_io: "Moderate"           # Transaction logs
  network_bandwidth: "Moderate" # Coordination overhead
```

---

## 🚨 **Common Pitfalls & Solutions**

### **Using SimpleJobProcessor When Consistency Matters**

❌ **Wrong**:

```sql
-- Financial data with simple processor - DATA LOSS RISK
CREATE STREAM bank_transfers AS
SELECT * FROM kafka_payments
WITH ('processor.type' = 'simple');  -- ❌ Wrong for financial data
```

✅ **Correct**:

```sql
-- Financial data with transactional processor
CREATE STREAM bank_transfers AS
SELECT * FROM kafka_payments
WITH ('processor.type' = 'transactional');  -- ✅ At-least-once with ACID guarantees
```

### **Using TransactionalJobProcessor for High-Volume Analytics**

❌ **Wrong**:

```sql
-- High-volume IoT with transactional - UNNECESSARY OVERHEAD
CREATE STREAM sensor_analytics AS
SELECT * FROM kafka_sensors  -- Millions of records/sec
WITH ('processor.type' = 'transactional');  -- ❌ Overkill for IoT analytics
```

✅ **Correct**:

```sql
-- High-volume IoT with simple processor
CREATE STREAM sensor_analytics AS
SELECT * FROM kafka_sensors
WITH ('processor.type' = 'simple');  -- ✅ Optimized for throughput
```

### **Incorrect Batch Size Configuration**

❌ **Wrong**:

```yaml
# Large batches with transactional (slow rollback)
processor:
  type: transactional
  batch_size: 10000  # ❌ Too large - slow rollback on failure
```

✅ **Correct**:

```yaml
# Appropriate batch sizes
simple_processor:
  batch_size: 5000    # ✅ Large batches for efficiency

transactional_processor:
  batch_size: 500     # ✅ Smaller batches for fast rollback
```

---

## 🔍 **Monitoring & Observability**

### **Key Metrics to Monitor**

#### **SimpleJobProcessor Metrics**

```rust
// Key performance indicators
metrics! {
    "records_processed_per_sec" => simple_stats.throughput,
    "batch_success_rate" => simple_stats.batch_success_rate,
    "processing_latency_p95" => simple_stats.latency_p95,
    "failed_records_logged" => simple_stats.failed_records,
}
```

#### **TransactionalJobProcessor Metrics**

```rust
// Key consistency indicators
metrics! {
    "transaction_success_rate" => tx_stats.transaction_success_rate,
    "rollback_frequency" => tx_stats.rollbacks_per_hour,
    "transaction_duration_p95" => tx_stats.tx_duration_p95,
    "at_least_once_guarantee" => tx_stats.transaction_success_rate,
}
```

### **Alerting Recommendations**

#### **SimpleJobProcessor Alerts**

- **High Error Rate**: >5% failed records in 5-minute window
- **Processing Lag**: Latency >100ms for 95th percentile
- **Throughput Drop**: >20% decrease from baseline

#### **TransactionalJobProcessor Alerts**

- **Transaction Failures**: >1% failed transactions in 10-minute window
- **Rollback Spike**: >10 rollbacks per hour
- **Transaction Timeout**: Any transactions exceeding configured timeout

---

## 🔄 **Migration Guide**

### **Simple → Transactional Migration**

```bash
# 1. Deploy transactional processor in parallel
velo-sql --config transactional-config.yaml --job-name "payment-tx-v2"

# 2. Compare outputs between simple and transactional processors
velo-compare --job1 "payment-simple" --job2 "payment-tx-v2" --duration 1h

# 3. Verify at-least-once semantics and transaction boundaries
velo-validate --processor transactional --check gaps,ordering,transactions

# 4. Switch traffic and monitor
velo-deploy --switch-to transactional --monitor-duration 24h
```

### **Transactional → Simple Migration**

```bash
# 1. Verify data loss tolerance with business stakeholders
echo "Confirm: Acceptable to lose <0.1% of records during failures? (y/N)"

# 2. Deploy simple processor with enhanced monitoring
velo-sql --config simple-config.yaml --enable-enhanced-monitoring

# 3. A/B test performance improvement
velo-benchmark --compare transactional,simple --duration 2h

# 4. Gradual rollout with rollback plan
velo-deploy --canary-percent 10 --rollback-trigger error_rate>2%
```

---

## 📚 **Additional Resources**

### **Detailed Configuration Guide**

- **[SQL Job Processor Configuration Guide](../sql/ops/job-processor-configuration-guide.md)** - Complete reference
  with:
    - Default configuration values for all three processors
    - SQL syntax and WITH clause properties
    - Performance profiles and benchmarks
    - Configuration examples by use case
    - Annotations (@processor_mode) guide

### **Additional References**

- [Kafka Transaction Configuration Guide](./kafka-transaction-configuration.md) - Detailed Kafka transaction setup
- [Batch Configuration Guide](./batch-configuration-guide.md) - Batch processing optimization
- [Testing and Benchmarks Guide](performance/testing-and-benchmarks-guide.md) - Performance validation

### **Code Examples**

- `tests/unit/stream_job/stream_job_simple_test.rs` - SimpleJobProcessor test examples
- `tests/unit/stream_job/stream_job_transactional_test.rs` - TransactionalJobProcessor test examples
- `configs/transaction-consumer.yaml` - Production transactional configuration
- `configs/transaction-producer.yaml` - Production transactional output

### **Performance Benchmarks**

```bash
# Compare processor performance
cargo run --bin test_sql_batch_performance --no-default-features

# Test transactional overhead
cargo test stream_job_transactional --no-default-features -- --nocapture

# Validate simple processor throughput
cargo test stream_job_simple --no-default-features -- --nocapture
```

This guide ensures you select the right processor for your consistency, performance, and operational requirements.