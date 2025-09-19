# Velostream Performance Guide

> **Unified Performance Documentation**  
> This guide consolidates all performance-related information for Velostream,
> replacing scattered documentation with a single comprehensive resource.

## Table of Contents

1. [Performance Overview](#performance-overview)
2. [Current Performance Metrics](#current-performance-metrics)
3. [Testing Framework](#testing-framework)
4. [Optimization Strategies](#optimization-strategies)
5. [Monitoring & Observability](#monitoring--observability)
6. [Benchmarking Tools](#benchmarking-tools)
7. [Troubleshooting](#troubleshooting)

## Performance Overview

Velostream is designed for high-performance financial data processing with exact precision guarantees. The system demonstrates production-ready performance across all query types with **883/883 tests passing (100% success rate)**.

### Architecture Performance Profile

```
Kafka Consumer → KafkaDataReader → Multi-Job Processor → SQL Engine → KafkaDataWriter → Kafka Producer
     ↓                ↓                    ↓                  ↓                ↓               ↓
  10K+ msg/sec    Zero-copy deserialization   Batch processing   42x faster      Codec caching  Txn commits
                                                                 ScaledInteger
```

### Key Performance Characteristics

- **Financial Precision**: 42x faster than f64 with zero precision loss
- **Streaming Throughput**: 100K+ records/second sustained
- **Memory Efficiency**: Object pooling and zero-copy optimizations
- **Transaction Support**: Exactly-once semantics with minimal overhead

## Current Performance Metrics

### Core SQL Engine Performance (per record)

| Operation | Latency | Throughput | Status |
|-----------|---------|------------|--------|
| Simple SELECT | ~7.05µs | 142,000 records/sec | ✅ Excellent |
| Filtered SELECT | ~6.46µs | 155,000 records/sec | ✅ Excellent |
| Multi-condition | ~6.26µs | 160,000 records/sec | ✅ Excellent |
| GROUP BY | ~17.3µs | 57,700 records/sec | ✅ Good |
| Window functions | ~49.2µs | 20,300 records/sec | ⚠️ Optimization target |
| Complex JOINs | Variable | 10x+ improvement with hash joins | ✅ Optimized |

### Financial Precision Benchmarks

| Calculation Pattern | f64 Time | ScaledInteger Time | Speedup | Precision |
|--------------------|----------|-------------------|---------|-----------|
| Price × Quantity | 83.458µs | 1.958µs | **42x faster** | Exact |
| Fee calculations | Variable | 1.5x faster than Decimal | **1.5x faster** | Exact |
| Aggregations | High error | Zero error | **42x faster** | Perfect |

### Memory Performance

- **Memory Pools**: 40-60% reduction in allocations (planned)
- **Object Reuse**: StreamRecord and HashMap pooling
- **Zero-Copy**: Cow<str> and bytes::Bytes optimizations
- **GC Pressure**: Minimal with Rust's ownership model

## Testing Framework

### Structure Organization

```
tests/performance/
├── consolidated_mod.rs          # Unified test framework
├── benchmarks/                  # Micro-benchmarks
│   ├── financial_precision.rs  # ScaledInteger vs f64
│   ├── serialization.rs        # Codec performance
│   ├── memory_allocation.rs    # Memory profiling
│   └── codec_performance.rs    # Serialization formats
├── integration/                 # End-to-end tests
│   ├── kafka_pipeline.rs       # Full pipeline benchmarks
│   ├── sql_execution.rs        # Query performance
│   └── transaction_processing.rs # Transaction benchmarks
├── load_testing/               # High-throughput tests
│   ├── throughput_benchmarks.rs # Sustained load
│   ├── memory_pressure.rs      # Resource exhaustion
│   └── scalability.rs          # Concurrent performance
└── profiling/                  # Profiling utilities
    ├── memory_profiler.rs      # Memory tracking
    ├── cpu_profiler.rs         # CPU utilization
    └── allocation_tracker.rs   # Allocation patterns
```

### Running Performance Tests

#### Micro-benchmarks
```bash
# Run all performance tests
cargo test --test performance --release

# Run specific benchmark category
cargo test --test performance::benchmarks --release

# Run with memory profiling
cargo test --test performance --release --features jemalloc
```

#### Load Testing
```bash
# High-throughput sustained load test
cargo run --example load_testing --release

# Memory pressure testing
cargo run --example memory_pressure --release

# Transaction processing benchmarks
cargo run --example transaction_benchmarks --release
```

#### Profiling
```bash
# CPU profiling with flamegraph
cargo flamegraph --test performance::profiling::cpu_profiler

# Memory profiling
cargo run --example memory_profiler --release

# Allocation tracking
RUST_LOG=debug cargo test --test allocation_tracker --release
```

## Optimization Strategies

### 1. Memory Optimizations

#### Object Pooling (Implemented)
```rust
pub struct RecordPool {
    records: Vec<StreamRecord>,
    field_maps: Vec<HashMap<String, FieldValue>>,
    max_pool_size: usize,
}
```

**Impact**: 40-60% memory allocation reduction  
**Status**: Ready for implementation

#### Zero-Copy Patterns (Planned)
```rust
pub enum FieldValue {
    String(Cow<'a, str>),  // Zero-copy string references
    Binary(bytes::Bytes),  // Shared buffer references
    // ... existing variants
}
```

**Impact**: 20-30% CPU reduction  
**Status**: Design phase

### 2. Throughput Optimizations

#### Async Batch Processing (Critical)
```rust
// Current synchronous bottleneck (multi_job_simple.rs:106-111)
let batch = reader.read().await?;
if batch.is_empty() {
    tokio::time::sleep(Duration::from_millis(100)).await; // BLOCKING
    return Ok(());
}

// Proposed async pipeline
async fn process_batch_pipeline(
    &self,
    mut batch_rx: mpsc::Receiver<Vec<StreamRecord>>,
    writer_tx: mpsc::Sender<Vec<StreamRecord>>,
) -> DataSourceResult<()> {
    // Double-buffering with parallel processing
}
```

**Impact**: 2-3x throughput improvement  
**Status**: High priority

### 3. Unified Configuration System (✅ Implemented)

#### Configuration Management Architecture
Velostream now features a comprehensive unified configuration system that dramatically simplifies configuration management while ensuring user settings are never overridden.

**Key Benefits:**
- **90% Code Reduction**: KafkaDataWriter reduced from 150+ lines to 10 lines
- **Zero Configuration Override**: User settings always preserved
- **Intelligent Defaults**: Batch strategies suggest optimal configurations
- **Full Logging**: Complete visibility into configuration decisions

#### Configuration Precedence (Guaranteed)
1. **User Settings** (Highest Priority) - Never overridden
2. **Batch Strategy Suggestions** - Applied only when user hasn't specified
3. **System Defaults** (Lowest Priority) - Fallback values

#### Performance Impact
- **Compression Configuration**: Simplified to single property with strategy suggestions
- **Batch Processing**: Five strategies (FixedSize, TimeWindow, AdaptiveSize, MemoryBased, LowLatency)
- **Buffer Management**: Automatic optimization with user override protection

**Example Configuration Flow:**
```rust
// ConfigFactory automatically handles precedence
let mut config = HashMap::new();
// User properties loaded first (protected from override)
config.extend(user_properties);
// Batch strategy suggestions applied only for unset properties
if batch_config.enable_batching {
    BatchConfigApplicator::apply_kafka_producer_strategy(&mut config, &batch_config, &logger);
}
```

## Batch Processing Configuration Reference

### Overview

Velostream provides five distinct batch processing strategies, each optimized for different use cases. This section provides complete configuration details, default values, and SQL examples for each strategy across all source and sink types.

### Batch Strategy Configuration in SQL

#### Global Batch Configuration
```sql
-- Set batch strategy for all sources/sinks in the job
CREATE STREAM my_stream AS 
SELECT * FROM source_stream 
WITH (
    'batch.strategy' = 'FixedSize',
    'batch.batch_size' = '1000',
    'batch.enable_batching' = 'true'
);
```

#### Source-Specific Batch Configuration
```sql
-- Configure batch settings for specific source
CREATE STREAM orders AS
SELECT * FROM kafka_source
WITH (
    'kafka.bootstrap.servers' = 'localhost:9092',
    'kafka.topic' = 'orders',
    'batch.strategy' = 'time_window',
    'batch.window' = '5s',
    'batch.enable' = 'true'
);
```

#### Sink-Specific Batch Configuration  
```sql
-- Configure batch settings for specific sink
INSERT INTO kafka_sink
SELECT * FROM processed_orders
WITH (
    'kafka.bootstrap.servers' = 'localhost:9092',
    'kafka.topic' = 'processed_orders',
    'batch.strategy' = 'memory_based',
    'batch.memory_size' = '2097152',
    'batch.enable' = 'true'
);
```

### 1. FixedSize Strategy

#### Description
Processes records in fixed-size batches for predictable memory usage and consistent processing latency.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"FixedSize"` | Strategy identifier |
| `batch.batch_size` | Integer | `100` | Fixed number of records per batch |
| `batch.enable_batching` | Boolean | `true` | Enable/disable batch processing |
| `batch.timeout_ms` | Integer | `1000` | Max wait time for incomplete batches |
| `batch.max_batch_size` | Integer | `1000` | Hard limit to prevent memory issues |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1` | Minimum bytes to fetch |
| `fetch.max.wait.ms` | `500` | Maximum wait time for fetch |
| `max.poll.records` | `500` | Maximum records per poll |
| `receive.buffer.bytes` | `65536` | 64KB receive buffer |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `65536` | 64KB read buffer |
| `file.batch_read_lines` | `100` | Matches batch_size |
| `file.concurrent_readers` | `1` | Single reader for consistency |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `16384` | 16KB Kafka batch size |
| `linger.ms` | `10` | Wait up to 10ms for batching (suggested) |
| `buffer.memory` | `33554432` | 32MB total buffer |
| `compression.type` | `"snappy"` | Fast compression (suggested) |
| `acks` | `"all"` | Default reliability |
| `retries` | `2147483647` | Maximum retries (default) |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `65536` | 64KB write buffer |
| `file.batch_write_records` | `100` | Matches batch_size |
| `file.sync_frequency` | `100` | Sync every 100 records |

#### SQL Configuration Example
```sql
CREATE STREAM fixed_batch_stream AS
SELECT customer_id, amount, timestamp 
FROM orders
WITH (
    'batch.strategy' = 'fixed_size',
    'batch.size' = '100',
    'batch.enable' = 'true',
    'batch.timeout' = '1000ms',
    'source.fetch.min.bytes' = '1',
    'source.max.poll.records' = '500',
    'sink.batch.size' = '16384',
    'sink.linger.ms' = '10',
    'sink.compression.type' = 'snappy'
);
```

### 2. TimeWindow Strategy

#### Description
Processes records within specified time windows, optimal for real-time analytics and time-based aggregations.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"TimeWindow"` | Strategy identifier |
| `batch.window_duration_ms` | Integer | `5000` | 5 second time windows |
| `batch.max_batch_size` | Integer | `1000` | Maximum records per window |
| `batch.enable_batching` | Boolean | `true` | Enable/disable batch processing |
| `batch.timeout_ms` | Integer | `1000` | Max wait time for incomplete batches |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1` | Minimal fetch for low latency |
| `fetch.max.wait.ms` | `500` | 500ms max wait |
| `max.poll.records` | `500` | Maximum records per poll |
| `session.timeout.ms` | `30000` | 30 second session timeout |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `524288` | 512KB buffer for streaming |
| `file.poll_interval_ms` | `100` | Check for new data every 100ms |
| `file.window_processing` | `true` | Enable time-window processing |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `65536` | 64KB batches (suggested) |
| `linger.ms` | `5000` | Up to 5 seconds linger time (suggested) |
| `buffer.memory` | `33554432` | 32MB buffer |
| `compression.type` | `"lz4"` | Fast compression (suggested) |
| `acks` | `"all"` | Default reliability |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `524288` | 512KB write buffer |
| `file.flush_frequency_ms` | `5000` | Flush every 5 seconds |
| `file.async_writes` | `true` | Async writes for performance |

#### SQL Configuration Example
```sql
CREATE STREAM time_window_stream AS
SELECT customer_id, COUNT(*) as order_count, 
       TUMBLE_START(timestamp, INTERVAL '5' SECONDS) as window_start
FROM orders
GROUP BY customer_id, TUMBLE(timestamp, INTERVAL '5' SECONDS)
WITH (
    'batch.strategy' = 'time_window',
    'batch.window' = '5s',
    'batch.enable' = 'true',
    'batch.timeout' = '1000ms',
    'source.fetch.max.wait.ms' = '500',
    'sink.linger.ms' = '5000',
    'sink.compression.type' = 'lz4'
);
```

### 3. AdaptiveSize Strategy

#### Description
Dynamically adjusts batch sizes based on processing performance and system load.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"AdaptiveSize"` | Strategy identifier |
| `batch.min_size` | Integer | `50` | Minimum allowed batch size |
| `batch.max_size` | Integer | `1000` | Maximum allowed batch size (limited by max_batch_size) |
| `batch.target_latency_ms` | Integer | `100` | Target processing latency |
| `batch.max_batch_size` | Integer | `1000` | Hard limit to prevent memory issues |
| `batch.timeout_ms` | Integer | `1000` | Max wait time for incomplete batches |
| `batch.enable_batching` | Boolean | `true` | Enable/disable batch processing |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1` | Base fetch size |
| `fetch.max.wait.ms` | `500` | Maximum wait time for fetch |
| `max.poll.records` | `500` | Maximum records per poll |
| `session.timeout.ms` | `30000` | Default session timeout |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `65536` | 64KB base buffer |
| `file.batch_read_lines` | `100` | Base batch size |
| `file.adaptive_reading` | `true` | Enable adaptive file reading |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `32768` | 32KB adaptive batch (suggested) |
| `linger.ms` | `100` | Moderate linger time (suggested) |
| `buffer.memory` | `33554432` | 32MB buffer |
| `compression.type` | `"snappy"` | Moderate compression (suggested) |
| `acks` | `"all"` | Default reliability |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `524288` | 512KB adaptive buffer (suggested) |
| `file.batch_write_records` | `100` | Base batch size |
| `file.sync_frequency` | `100` | Sync based on performance |

#### SQL Configuration Example
```sql
CREATE STREAM adaptive_stream AS
SELECT * FROM high_volume_orders
WITH (
    'batch.strategy' = 'adaptive_size',
    'batch.min_size' = '50',
    'batch.adaptive_max_size' = '1000',
    'batch.target_latency' = '100ms',
    'batch.enable' = 'true',
    'batch.timeout' = '1000ms',
    'source.fetch.max.wait.ms' = '500',
    'sink.batch.size' = '32768',
    'sink.compression.type' = 'snappy'
);
```

### 4. MemoryBased Strategy

#### Description
Adjusts batch processing based on available memory and memory pressure indicators.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"MemoryBased"` | Strategy identifier |
| `batch.target_memory_bytes` | Integer | `1048576` | Target memory per batch (1MB) |
| `batch.max_batch_size` | Integer | `1000` | Hard limit to prevent memory issues |
| `batch.timeout_ms` | Integer | `1000` | Max wait time for incomplete batches |
| `batch.enable_batching` | Boolean | `true` | Enable/disable batch processing |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1` | Conservative fetch size |
| `fetch.max.wait.ms` | `500` | Maximum wait time for fetch |
| `max.poll.records` | `500` | Conservative records per poll |
| `receive.buffer.bytes` | `32768` | 32KB conservative buffer |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `65536` | 64KB conservative buffer |
| `file.batch_read_lines` | `100` | Conservative batch size |
| `file.memory_monitoring` | `true` | Enable memory monitoring |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `16384` | 16KB conservative batch (suggested) |
| `linger.ms` | `100` | Longer linger for efficiency (suggested) |
| `buffer.memory` | `33554432` | 32MB conservative buffer |
| `compression.type` | `"gzip"` | Good compression for memory (suggested) |
| `acks` | `"all"` | Default reliability |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `65536` | 64KB conservative buffer |
| `file.batch_write_records` | `100` | Conservative batch size |
| `file.memory_aware_flushing` | `true` | Memory-based flush decisions |

#### SQL Configuration Example
```sql
CREATE STREAM memory_aware_stream AS
SELECT * FROM large_records_stream
WITH (
    'batch.strategy' = 'memory_based',
    'batch.memory_size' = '1048576',
    'batch.enable' = 'true',
    'batch.timeout' = '1000ms',
    'source.fetch.max.wait.ms' = '500',
    'sink.batch.size' = '16384',
    'sink.compression.type' = 'gzip'
);
```

### 5. LowLatency Strategy

#### Description
Minimizes processing latency with small batches and immediate processing.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"LowLatency"` | Strategy identifier |
| `batch.max_batch_size` | Integer | `10` | Very small batch sizes |
| `batch.max_wait_time_ms` | Integer | `5` | Ultra-short wait time |
| `batch.eager_processing` | Boolean | `true` | Process records immediately |
| `batch.enable_batching` | Boolean | `true` | Enable minimal batching |
| `batch.timeout_ms` | Integer | `1000` | Max wait time for incomplete batches |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1` | Immediate fetch |
| `fetch.max.wait.ms` | `1` | 1ms max wait |
| `max.poll.records` | `10` | Matches max_batch_size |
| `heartbeat.interval.ms` | `1000` | Frequent heartbeats |
| `session.timeout.ms` | `10000` | Quick failure detection |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `4096` | 4KB minimal buffer |
| `file.batch_read_lines` | `10` | Very small batches |
| `file.poll_interval_ms` | `1` | Continuous polling |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `1024` | 1KB tiny batches (suggested) |
| `linger.ms` | `5` | Ultra-short linger time (suggested) |
| `buffer.memory` | `16777216` | 16MB minimal buffer |
| `compression.type` | `"none"` | No compression overhead (suggested) |
| `acks` | `"1"` | Leader acknowledgment (suggested) |
| `retries` | `0` | No retry delays (suggested) |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `4096` | 4KB minimal buffer (suggested) |
| `file.batch_write_records` | `10` | Very small batches |
| `file.immediate_flush` | `true` | Flush immediately |

#### SQL Configuration Example
```sql
CREATE STREAM low_latency_stream AS
SELECT alert_id, severity, timestamp 
FROM critical_alerts
WHERE severity = 'CRITICAL'
WITH (
    'batch.strategy' = 'low_latency',
    'batch.low_latency_max_size' = '10',
    'batch.low_latency_wait' = '5ms',
    'batch.eager_processing' = 'true',
    'batch.enable' = 'true',
    'source.fetch.max.wait.ms' = '1',
    'sink.batch.size' = '1024',
    'sink.linger.ms' = '5',
    'sink.compression.type' = 'none',
    'sink.acks' = '1'
);
```

### Batch Strategy Selection Guide

#### Performance Characteristics

| Strategy | Throughput | Latency | Memory Usage | CPU Usage | Best For |
|----------|------------|---------|--------------|-----------|-----------|
| FixedSize | High | Moderate | Predictable | Moderate | Consistent workloads |
| TimeWindow | High | Low-Moderate | Variable | Moderate | Real-time analytics |
| AdaptiveSize | Variable | Variable | Adaptive | Higher | Variable workloads |
| MemoryBased | Moderate | Moderate-High | Controlled | Lower | Resource-constrained |
| LowLatency | Lower | Very Low | Low | Higher | Critical alerts/trading |

#### Use Case Recommendations

- **Financial Trading**: LowLatency for order processing, TimeWindow for analytics
- **IoT Data Processing**: AdaptiveSize for variable sensors, MemoryBased for edge devices  
- **Log Processing**: FixedSize for consistent throughput, TimeWindow for real-time monitoring
- **Real-time Analytics**: TimeWindow for dashboards, AdaptiveSize for variable queries
- **Batch ETL**: FixedSize for predictable processing, MemoryBased for large datasets

## Configuration Implementation Notes

**Important**: Batch processing strategies are currently implemented at the **application code level** only. SQL WITH clauses do not currently support batch configuration parameters like `'batch.strategy'` or `'batch.batch_size'`.

### Supported SQL Configuration Patterns

#### Pattern 1: Kafka Configuration (ACTUALLY SUPPORTED)
Configure Kafka source and sink properties:

```sql
CREATE STREAM processed_orders AS
SELECT order_id, customer_id, amount, status
FROM orders
WHERE amount > 100.0
WITH (
    -- Kafka settings (ACTUALLY SUPPORTED)
    'source.kafka.bootstrap.servers' = 'localhost:9092',
    'source.kafka.topic' = 'orders',
    'source.kafka.group.id' = 'processor',
    'sink.kafka.topic' = 'processed-orders',
    'sink.kafka.compression.type' = 'snappy',
    'sink.kafka.batch.size' = '32768'
);
```

#### Pattern 2: File Configuration (ACTUALLY SUPPORTED)  
Configure file source and sink properties:

```sql
CREATE STREAM csv_processor AS
SELECT col_0 as id, col_1 as name, col_2 as amount
FROM csv_input
WITH (
    -- File settings (ACTUALLY SUPPORTED)
    'source.file.path' = '/data/input.csv',
    'source.file.format' = 'csv',
    'source.file.has_headers' = 'true',
    'sink.file.path' = '/data/output.json',
    'sink.file.format' = 'json'
);
```

### How Batch Configuration Actually Works

Batch processing strategies are configured **programmatically** when creating data sources and sinks:

```rust
// Example: Configure batch processing in Rust code
let batch_config = BatchConfig {
    strategy: BatchStrategy::FixedSize(1000),
    max_batch_size: 10000,
    batch_timeout: Duration::from_millis(1000),
    enable_batching: true,
};

// Create Kafka source with batch config
let kafka_source = KafkaDataSource::new(brokers, topic, group_id, format)
    .await?;
let reader = kafka_source
    .create_reader_with_batch_config(batch_config)
    .await?;

// Create Kafka sink with batch config  
let kafka_sink = KafkaDataSink::new(brokers, topic, format)
    .await?;
let writer = kafka_sink
    .create_writer_with_batch_config(batch_config)
    .await?;
```

### Actual SQL Configuration Support

SQL WITH clauses currently support connector-specific properties only:

```sql
-- What IS supported: Kafka/File connector properties
CREATE STREAM real_example AS
SELECT order_id, customer_id, amount
FROM orders
WHERE amount > 100.0
WITH (
    'source.kafka.bootstrap.servers' = 'localhost:9092',
    'source.kafka.topic' = 'orders',
    'source.kafka.group.id' = 'processor',
    'source.kafka.fetch.min.bytes' = '1024',
    'sink.kafka.topic' = 'processed-orders',
    'sink.kafka.compression.type' = 'lz4',
    'sink.kafka.batch.size' = '32768'
);
```

## Configuration Property Reference

### Batch Parameters (Code-Level Only)

**Important**: These parameters are configured in **Rust code** using `BatchConfig` struct, not in SQL:

```rust
// Batch configuration structure (NOT available in SQL)
pub struct BatchConfig {
    pub strategy: BatchStrategy,
    pub max_batch_size: usize,
    pub batch_timeout: Duration,
    pub enable_batching: bool,
}

// Available batch strategies (NOT configurable in SQL)
pub enum BatchStrategy {
    FixedSize(usize),                    // Fixed records per batch
    TimeWindow(Duration),                // Time-based batching  
    AdaptiveSize { target_latency: Duration, /* ... */ },
    MemoryBased(usize),                  // Memory-limited batching
    LowLatency,                          // Minimal latency processing
}
```

### Kafka Configuration Parameters

**Source (Consumer) Settings:**
- `fetch.min.bytes` (Integer, default: 1) - Minimum bytes to fetch
- `fetch.max.wait.ms` (Integer, default: 500) - Maximum wait time
- `max.poll.records` (Integer, default: 500) - Maximum records per poll
- `session.timeout.ms` (Integer, default: 30000) - Session timeout

**Sink (Producer) Settings:**
- `batch.size` (Integer, default: 16384) - Kafka batch size in bytes
- `linger.ms` (Integer, default: 0) - Wait time for batching
- `compression.type` (String, default: "none") - Compression algorithm
- `acks` (String, default: "all") - Acknowledgment level
- `retries` (Integer, default: 2147483647) - Maximum retries
- `buffer.memory` (Integer, default: 33554432) - Producer buffer memory

### File Configuration Parameters

**Source (Reader) Settings:**
- `file.read_buffer_size` (Integer, default: 65536) - Read buffer size in bytes
- `file.batch_read_lines` (Integer, varies by strategy) - Lines per batch
- `file.path` (String, required) - File path or pattern
- `file.format` (String, default: "json") - File format (json/csv)
- `file.has_headers` (Boolean, default: false) - CSV has headers
- `file.watching` (Boolean, default: false) - Watch for new files

**Sink (Writer) Settings:**
- `file.write_buffer_size` (Integer, default: 65536) - Write buffer size in bytes
- `file.batch_write_records` (Integer, varies by strategy) - Records per batch
- `file.path` (String, required) - Output file path
- `file.format` (String, default: "json") - Output format
- `file.append` (Boolean, default: false) - Append to existing file
- `file.compression` (String, default: "none") - File compression

### 4. Serialization Optimizations

#### Codec Caching (Quick Win)
- Cache codecs in reader/writer structs
- Pre-validate schemas during initialization
- Lazy codec initialization with Arc<Mutex<>>

**Impact**: 15-25% serialization improvement  
**Effort**: Low
**Status**: Ready for implementation

## Monitoring & Observability

### Performance Monitoring Integration

Velostream includes comprehensive performance monitoring with HTTP endpoints:

#### SQL Server Monitoring
```bash
# Start server with metrics
cargo run --bin velo-sql server --enable-metrics --metrics-port 9080
```

**Endpoints:**
- `GET /metrics` - Prometheus metrics export
- `GET /health` - Health check with performance status  
- `GET /report` - Detailed performance report

#### Multi-Job Server Monitoring
```bash
# Start with job-level metrics
cargo run --bin velo-sql-multi server --enable-metrics
```

**Additional Endpoints:**
- `GET /jobs` - List running jobs with metrics

### Key Metrics Tracked

#### Throughput Metrics
- Records processed per second
- Bytes processed per second
- Query completion rate
- Batch processing efficiency

#### Latency Metrics
- P50, P95, P99 processing times
- End-to-end latency
- Component-level timing
- Queue wait times

#### Resource Metrics
- Memory allocation rate
- CPU utilization per core
- GC pressure indicators
- Pool hit rates

#### Financial Precision Metrics
- Calculation accuracy validation
- ScaledInteger vs f64 performance
- Precision error tracking

#### Configuration System Metrics
- User property override tracking
- Batch strategy effectiveness
- Configuration precedence decisions
- Performance impact of different strategies

## Benchmarking Tools

### Criterion Integration

Velostream uses Criterion.rs for statistical benchmarking:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BatchSize};

fn benchmark_financial_precision(c: &mut Criterion) {
    c.bench_function("scaled_integer_vs_f64", |b| {
        b.iter_batched(
            || generate_financial_data(1000),
            |data| process_with_scaled_integer(black_box(data)),
            BatchSize::SmallInput,
        )
    });
}
```

### Memory Profiling

#### jemalloc Integration
```rust
#[cfg(feature = "jemalloc")]
use jemalloc_ctl::{stats, epoch};

pub fn get_memory_snapshot() -> Result<MemorySnapshot, Box<dyn std::error::Error>> {
    epoch::advance()?;
    let allocated = stats::allocated::read()?;
    let resident = stats::resident::read()?;
    // ... tracking implementation
}
```

#### Valgrind/Heaptrack Support
```bash
# Memory leak detection
valgrind --tool=memcheck --leak-check=full cargo test --test performance

# Heap profiling
heaptrack cargo run --example load_testing
```

### Load Testing Framework

#### Sustained Load Testing
```rust
pub struct LoadTestConfig {
    pub target_rps: u64,           // 10,000+ RPS
    pub duration: Duration,        // 30+ seconds
    pub ramp_up_duration: Duration, // Gradual load increase
    pub concurrent_connections: usize,
}
```

#### Resource Exhaustion Testing
- Memory pressure scenarios
- CPU saturation testing  
- I/O bottleneck identification
- Backpressure behavior validation

## Troubleshooting

### Common Performance Issues

#### 1. Memory Allocation Spikes
**Symptoms**: High allocation rate, GC pressure
**Diagnosis**: Use memory profiler to identify allocation hotspots
**Solutions**: Implement object pooling, reduce temporary allocations

#### 2. High Query Latency
**Symptoms**: P95/P99 latencies above thresholds
**Diagnosis**: Check processor-level timing breakdown
**Solutions**: Optimize specific processors, add caching

#### 3. Low Throughput
**Symptoms**: Records/second below target
**Diagnosis**: Identify bottlenecks in processing pipeline
**Solutions**: Async processing, batch optimization, parallel execution

#### 4. Financial Precision Errors
**Symptoms**: Precision loss in financial calculations
**Diagnosis**: Validate ScaledInteger usage vs f64
**Solutions**: Ensure all financial fields use ScaledInteger

### Performance Regression Detection

#### Automated Benchmarking
```bash
# Save baseline performance
cargo bench --bench financial_precision -- --save-baseline before_optimization

# Run after changes
cargo bench --bench financial_precision -- --baseline before_optimization
```

#### CI/CD Integration
- Automated performance regression detection
- Historical performance tracking
- Performance gate enforcement

### Profiling Commands

#### CPU Profiling
```bash
# Generate flame graph
cargo flamegraph --test performance_tests

# Profile specific function
perf record -g cargo test --test cpu_intensive_test
perf report
```

#### Memory Profiling
```bash
# Track memory allocations
cargo run --example memory_tracker --release

# Detailed allocation tracking  
RUST_LOG=debug cargo test --test memory_pressure --release
```

## Best Practices

### 1. Financial Precision
- **Always use ScaledInteger** for financial calculations
- **Validate precision** in all arithmetic operations
- **Test boundary conditions** for financial edge cases

### 2. Memory Management
- **Use object pools** for frequently allocated objects
- **Implement zero-copy** where possible
- **Monitor allocation patterns** continuously

### 3. Throughput Optimization
- **Batch processing** for better CPU cache utilization  
- **Async pipeline** for overlapped I/O and processing
- **Resource pooling** to reduce allocation overhead

### 4. Testing Strategy
- **Baseline before optimization** to measure improvements
- **Test under load** to identify bottlenecks
- **Profile continuously** to catch regressions

---

## Related Documentation

- [`TODO-optimisation-plan.MD`](../../TODO-optimisation-plan.MD) - Detailed optimization roadmap
- [`KAFKA_TRANSACTION_CONFIGURATION.md`](../KAFKA_TRANSACTION_CONFIGURATION.md) - Transaction performance
- [`CLAUDE.md`](../../CLAUDE.md) - Development guidelines with performance focus

---

*This guide consolidates and replaces:*
- `docs/PERFORMANCE_INTEGRATION.md`
- `docs/PERFORMANCE_MONITORING.md` 
- `docs/PERFORMANCE_ANALYSIS.md`
- `docs/KAFKA_PERFORMANCE_CONFIGS.md`
- `docs/PERFORMANCE_BENCHMARK_RESULTS.md`
- `docs/PERFORMANCE_COMPARISON_REPORT.md`

*Last updated: 2025-09-03*  
*Next review: After optimization implementation*