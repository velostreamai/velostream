# Job Processor Selection Guide

**Choosing between SimpleJobProcessor and TransactionalJobProcessor for your streaming applications**

This guide helps you select the appropriate job processor for your FerrisStreams applications based on data consistency requirements, performance characteristics, and operational complexity.

---

## 🎯 **Quick Decision Matrix**

| **Use Case** | **Processor** | **Why** |
|--------------|---------------|---------|
| **High-throughput analytics** | SimpleJobProcessor | Maximum performance, acceptable data loss |
| **Financial transactions** | TransactionalJobProcessor | ACID transactions with at-least-once delivery |
| **Real-time dashboards** | SimpleJobProcessor | Speed over perfect consistency |
| **Audit trails** | TransactionalJobProcessor | No data loss acceptable |
| **IoT sensor data** | SimpleJobProcessor | Volume over perfect accuracy |
| **Payment processing** | TransactionalJobProcessor | Regulatory compliance required |
| **Development/testing** | SimpleJobProcessor | Faster iteration, simpler debugging |
| **Production mission-critical** | TransactionalJobProcessor | Data integrity paramount |

---

## 📊 **Detailed Comparison**

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
use ferrisstreams::ferris::server::processors::simple::SimpleJobProcessor;
use ferrisstreams::ferris::server::processors::common::JobProcessingConfig;

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
use ferrisstreams::ferris::server::processors::transactional::TransactionalJobProcessor;
use ferrisstreams::ferris::server::processors::common::JobProcessingConfig;

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

FerrisStreams automatically selects between SimpleJobProcessor and TransactionalJobProcessor based on SQL WITH clause properties. Here's how the system determines which processor to use:

### **Processor Selection Logic**
```rust
// From src/ferris/server/stream_job_server.rs:809
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

### **SQL Configuration Syntax**
The processor selection is controlled via SQL WITH clauses:

#### **Enable Transactional Processor**
```sql
CREATE STREAM payment_processing AS
SELECT * FROM kafka_payments
WITH (
    'use_transactions' = 'true',           -- 🔑 KEY: Enables TransactionalJobProcessor
    'failure_strategy' = 'FailBatch',      -- Atomic failure handling
    'max_retries' = '5',                   -- More aggressive retries
    'retry_backoff' = '2000',              -- 2 second backoff
    'max_batch_size' = '500',              -- Smaller batches for fast rollback
    'batch_timeout' = '200'                -- 200ms timeout
);
```

#### **Enable Simple Processor (Default)**
```sql
CREATE STREAM analytics_processing AS
SELECT * FROM kafka_events
WITH (
    'use_transactions' = 'false',          -- 🔑 KEY: Enables SimpleJobProcessor (or omit)
    'failure_strategy' = 'LogAndContinue', -- Keep processing on errors
    'max_retries' = '3',                   -- Basic retry logic
    'max_batch_size' = '2000',             -- Larger batches for efficiency
    'batch_timeout' = '100'                -- 100ms timeout
);
```

#### **Default Behavior (No Configuration)**
```sql
-- No WITH clause = SimpleJobProcessor with defaults
CREATE STREAM default_processing AS
SELECT * FROM kafka_data;

-- Equivalent to:
CREATE STREAM default_processing AS
SELECT * FROM kafka_data
WITH (
    'use_transactions' = 'false',          -- Default
    'failure_strategy' = 'LogAndContinue', -- Default
    'max_retries' = '3',                   -- Default
    'max_batch_size' = '1000',             -- Default
    'batch_timeout' = '1000'               -- Default (1 second)
);
```

### **Complete Configuration Properties**

| **Property** | **Type** | **Default** | **Description** |
|--------------|----------|-------------|-----------------|
| `use_transactions` | boolean | `false` | **Main switch**: `true` = TransactionalJobProcessor, `false` = SimpleJobProcessor |
| `failure_strategy` | string | `"LogAndContinue"` | How to handle failures: `"RetryWithBackoff"`, `"LogAndContinue"`, `"FailBatch"`, `"SendToDLQ"` |
| `max_retries` | integer | `3` | Maximum retry attempts for failed operations |
| `retry_backoff` | integer | `1000` | Backoff delay in milliseconds between retries |
| `max_batch_size` | integer | `1000` | Maximum number of records per batch |
| `batch_timeout` | integer | `1000` | Maximum wait time in milliseconds before processing incomplete batch |

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
    'sink.kafka.transactional.id' = 'ferris-bank-transfers-tx',
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
    'sink.kafka.transactional.id' = 'ferris-payments-tx-1',
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
    'sink.kafka.transactional.id' = 'ferris-audit-tx-1'
);
```

---

## ⚡ **Performance Considerations**

### **Throughput Comparison**
Based on FerrisStreams benchmarks:

| **Metric** | **SimpleJobProcessor** | **TransactionalJobProcessor** |
|------------|----------------------|------------------------------|
| **Peak Throughput** | 365,018 records/sec | ~250,000 records/sec (est.) |
| **Batch Processing** | 1.1x improvement over single | Similar batch improvement |
| **Memory Overhead** | Baseline | +20-30% for transaction state |
| **CPU Overhead** | Baseline | +15-25% for coordination |
| **Latency (P95)** | <10ms | <25ms |

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
- **SimpleJobProcessor**: Near-linear scaling with added resources
- **TransactionalJobProcessor**: Coordination overhead may limit horizontal scaling

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
ferris-sql --config transactional-config.yaml --job-name "payment-tx-v2"

# 2. Compare outputs between simple and transactional processors
ferris-compare --job1 "payment-simple" --job2 "payment-tx-v2" --duration 1h

# 3. Verify at-least-once semantics and transaction boundaries
ferris-validate --processor transactional --check gaps,ordering,transactions

# 4. Switch traffic and monitor
ferris-deploy --switch-to transactional --monitor-duration 24h
```

### **Transactional → Simple Migration**
```bash
# 1. Verify data loss tolerance with business stakeholders
echo "Confirm: Acceptable to lose <0.1% of records during failures? (y/N)"

# 2. Deploy simple processor with enhanced monitoring
ferris-sql --config simple-config.yaml --enable-enhanced-monitoring

# 3. A/B test performance improvement
ferris-benchmark --compare transactional,simple --duration 2h

# 4. Gradual rollout with rollback plan
ferris-deploy --canary-percent 10 --rollback-trigger error_rate>2%
```

---

## 📚 **Additional Resources**

### **Configuration References**
- [Kafka Transaction Configuration Guide](./kafka-transaction-configuration.md) - Detailed Kafka transaction setup
- [Batch Configuration Guide](./batch-configuration-guide.md) - Batch processing optimization
- [Testing and Benchmarks Guide](./testing-and-benchmarks-guide.md) - Performance validation

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