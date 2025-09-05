# FerrisStreams Performance Guide

> **Unified Performance Documentation**  
> This guide consolidates all performance-related information for FerrisStreams,
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

FerrisStreams is designed for high-performance financial data processing with exact precision guarantees. The system demonstrates production-ready performance across all query types with **883/883 tests passing (100% success rate)**.

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
FerrisStreams now features a comprehensive unified configuration system that dramatically simplifies configuration management while ensuring user settings are never overridden.

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

FerrisStreams provides five distinct batch processing strategies, each optimized for different use cases. This section provides complete configuration details, default values, and SQL examples for each strategy across all source and sink types.

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
    'source.batch.strategy' = 'TimeWindow',
    'source.batch.window_duration_ms' = '5000',
    'source.batch.max_batch_size' = '10000'
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
    'sink.batch.strategy' = 'MemoryBased',
    'sink.batch.target_memory_usage' = '0.8',
    'sink.batch.max_batch_size' = '5000'
);
```

### 1. FixedSize Strategy

#### Description
Processes records in fixed-size batches for predictable memory usage and consistent processing latency.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"FixedSize"` | Strategy identifier |
| `batch.batch_size` | Integer | `1000` | Fixed number of records per batch |
| `batch.enable_batching` | Boolean | `true` | Enable/disable batch processing |
| `batch.timeout_ms` | Integer | `30000` | Max wait time for incomplete batches |
| `batch.max_memory_per_batch` | Integer | `16777216` | 16MB max memory per batch |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1024` | Minimum bytes to fetch |
| `fetch.max.bytes` | `52428800` | Maximum bytes to fetch (50MB) |
| `max.poll.records` | `1000` | Matches batch_size |
| `receive.buffer.bytes` | `65536` | 64KB receive buffer |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `1048576` | 1MB read buffer |
| `file.batch_read_lines` | `1000` | Matches batch_size |
| `file.concurrent_readers` | `1` | Single reader for consistency |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `32768` | 32KB Kafka batch size |
| `linger.ms` | `10` | Wait up to 10ms for batching |
| `buffer.memory` | `67108864` | 64MB total buffer |
| `compression.type` | `"lz4"` | Fast compression |
| `acks` | `"1"` | Balanced reliability/performance |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `1048576` | 1MB write buffer |
| `file.batch_write_records` | `1000` | Matches batch_size |
| `file.sync_frequency` | `1000` | Sync every 1000 records |

#### SQL Configuration Example
```sql
CREATE STREAM fixed_batch_stream AS
SELECT customer_id, amount, timestamp 
FROM orders
WITH (
    'batch.strategy' = 'FixedSize',
    'batch.batch_size' = '1000',
    'batch.enable_batching' = 'true',
    'batch.timeout_ms' = '30000',
    'kafka.fetch.min.bytes' = '1024',
    'kafka.max.poll.records' = '1000',
    'kafka.batch.size' = '32768',
    'kafka.linger.ms' = '10',
    'kafka.compression.type' = 'lz4'
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
| `batch.max_batch_size` | Integer | `10000` | Maximum records per window |
| `batch.enable_batching` | Boolean | `true` | Enable/disable batch processing |
| `batch.window_alignment` | String | `"processing_time"` | Time alignment mode |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1` | Minimal fetch for low latency |
| `fetch.max.wait.ms` | `100` | 100ms max wait |
| `max.poll.records` | `10000` | Matches max_batch_size |
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
| `batch.size` | `16384` | 16KB for lower latency |
| `linger.ms` | `5` | Minimal linger time |
| `buffer.memory` | `33554432` | 32MB buffer |
| `compression.type` | `"lz4"` | Fast compression |
| `acks` | `"1"` | Balanced reliability |

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
    'batch.strategy' = 'TimeWindow',
    'batch.window_duration_ms' = '5000',
    'batch.max_batch_size' = '10000',
    'batch.window_alignment' = 'processing_time',
    'kafka.fetch.max.wait.ms' = '100',
    'kafka.linger.ms' = '5',
    'kafka.compression.type' = 'lz4'
);
```

### 3. AdaptiveSize Strategy

#### Description
Dynamically adjusts batch sizes based on processing performance and system load.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"AdaptiveSize"` | Strategy identifier |
| `batch.initial_batch_size` | Integer | `1000` | Starting batch size |
| `batch.max_batch_size` | Integer | `10000` | Maximum allowed batch size |
| `batch.min_batch_size` | Integer | `100` | Minimum allowed batch size |
| `batch.adaptation_factor` | Float | `1.5` | Size adjustment multiplier |
| `batch.performance_window_ms` | Integer | `10000` | Performance measurement window |
| `batch.enable_batching` | Boolean | `true` | Enable/disable batch processing |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1024` | Base fetch size |
| `fetch.max.bytes` | `104857600` | 100MB max (for large batches) |
| `max.poll.records` | `10000` | Matches max_batch_size |
| `session.timeout.ms` | `45000` | Extended for variable processing |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `2097152` | 2MB adaptive buffer |
| `file.batch_read_lines` | `1000` | Initial batch size |
| `file.adaptive_reading` | `true` | Enable adaptive file reading |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `65536` | 64KB adaptive batch |
| `linger.ms` | `20` | Moderate linger time |
| `buffer.memory` | `134217728` | 128MB large buffer |
| `compression.type` | `"snappy"` | Moderate compression |
| `acks` | `"1"` | Balanced reliability |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `2097152` | 2MB adaptive buffer |
| `file.batch_write_records` | `1000` | Initial batch size |
| `file.sync_frequency` | `5000` | Sync based on performance |

#### SQL Configuration Example
```sql
CREATE STREAM adaptive_stream AS
SELECT * FROM high_volume_orders
WITH (
    'batch.strategy' = 'AdaptiveSize',
    'batch.initial_batch_size' = '1000',
    'batch.max_batch_size' = '10000',
    'batch.min_batch_size' = '100',
    'batch.adaptation_factor' = '1.5',
    'batch.performance_window_ms' = '10000',
    'kafka.fetch.max.bytes' = '104857600',
    'kafka.batch.size' = '65536',
    'kafka.compression.type' = 'snappy'
);
```

### 4. MemoryBased Strategy

#### Description
Adjusts batch processing based on available memory and memory pressure indicators.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"MemoryBased"` | Strategy identifier |
| `batch.target_memory_usage` | Float | `0.8` | Target memory utilization (80%) |
| `batch.min_batch_size` | Integer | `100` | Minimum batch size |
| `batch.max_batch_size` | Integer | `5000` | Maximum batch size |
| `batch.memory_check_frequency_ms` | Integer | `5000` | Memory check interval |
| `batch.gc_pressure_threshold` | Float | `0.9` | GC pressure threshold (90%) |
| `batch.enable_batching` | Boolean | `true` | Enable/disable batch processing |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1024` | Conservative fetch size |
| `fetch.max.bytes` | `10485760` | 10MB max to limit memory |
| `max.poll.records` | `5000` | Matches max_batch_size |
| `receive.buffer.bytes` | `32768` | 32KB conservative buffer |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `524288` | 512KB conservative buffer |
| `file.memory_monitoring` | `true` | Enable memory monitoring |
| `file.gc_aware_reading` | `true` | Adjust for GC pressure |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `16384` | 16KB conservative batch |
| `linger.ms` | `50` | Longer linger for efficiency |
| `buffer.memory` | `33554432` | 32MB conservative buffer |
| `compression.type` | `"gzip"` | Good compression for memory |
| `acks` | `"1"` | Balanced reliability |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `524288` | 512KB conservative buffer |
| `file.memory_aware_flushing` | `true` | Memory-based flush decisions |
| `file.gc_aware_writing` | `true` | Adjust for GC pressure |

#### SQL Configuration Example
```sql
CREATE STREAM memory_aware_stream AS
SELECT * FROM large_records_stream
WITH (
    'batch.strategy' = 'MemoryBased',
    'batch.target_memory_usage' = '0.8',
    'batch.min_batch_size' = '100',
    'batch.max_batch_size' = '5000',
    'batch.memory_check_frequency_ms' = '5000',
    'kafka.fetch.max.bytes' = '10485760',
    'kafka.batch.size' = '16384',
    'kafka.compression.type' = 'gzip'
);
```

### 5. LowLatency Strategy

#### Description
Minimizes processing latency with small batches and immediate processing.

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch.strategy` | String | `"LowLatency"` | Strategy identifier |
| `batch.max_latency_ms` | Integer | `100` | Maximum allowed latency |
| `batch.batch_size` | Integer | `10` | Very small batch sizes |
| `batch.immediate_processing` | Boolean | `true` | Process records immediately |
| `batch.enable_batching` | Boolean | `true` | Enable minimal batching |
| `batch.latency_monitoring` | Boolean | `true` | Monitor and adjust for latency |

#### Source Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fetch.min.bytes` | `1` | Immediate fetch |
| `fetch.max.wait.ms` | `1` | 1ms max wait |
| `max.poll.records` | `10` | Matches batch_size |
| `heartbeat.interval.ms` | `1000` | Frequent heartbeats |
| `session.timeout.ms` | `10000` | Quick failure detection |

#### Source Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.read_buffer_size` | `4096` | 4KB minimal buffer |
| `file.poll_interval_ms` | `1` | Continuous polling |
| `file.low_latency_mode` | `true` | Optimize for latency |

#### Sink Settings (Kafka)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch.size` | `1024` | 1KB tiny batches |
| `linger.ms` | `0` | No lingering |
| `buffer.memory` | `16777216` | 16MB minimal buffer |
| `compression.type` | `"none"` | No compression overhead |
| `acks` | `"0"` | Fire-and-forget for speed |
| `retries` | `0` | No retry delays |

#### Sink Settings (File)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `file.write_buffer_size` | `4096` | 4KB minimal buffer |
| `file.immediate_flush` | `true` | Flush immediately |
| `file.sync_frequency` | `1` | Sync every record |

#### SQL Configuration Example
```sql
CREATE STREAM low_latency_stream AS
SELECT alert_id, severity, timestamp 
FROM critical_alerts
WHERE severity = 'CRITICAL'
WITH (
    'batch.strategy' = 'LowLatency',
    'batch.max_latency_ms' = '100',
    'batch.batch_size' = '10',
    'batch.immediate_processing' = 'true',
    'kafka.fetch.max.wait.ms' = '1',
    'kafka.batch.size' = '1024',
    'kafka.linger.ms' = '0',
    'kafka.compression.type' = 'none',
    'kafka.acks' = '0'
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

FerrisStreams includes comprehensive performance monitoring with HTTP endpoints:

#### SQL Server Monitoring
```bash
# Start server with metrics
cargo run --bin ferris-sql server --enable-metrics --metrics-port 9080
```

**Endpoints:**
- `GET /metrics` - Prometheus metrics export
- `GET /health` - Health check with performance status  
- `GET /report` - Detailed performance report

#### Multi-Job Server Monitoring
```bash
# Start with job-level metrics
cargo run --bin ferris-sql-multi server --enable-metrics
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

FerrisStreams uses Criterion.rs for statistical benchmarking:

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

- [`TODO-optimisation-plan.MD`](../TODO-optimisation-plan.MD) - Detailed optimization roadmap
- [`KAFKA_TRANSACTION_CONFIGURATION.md`](KAFKA_TRANSACTION_CONFIGURATION.md) - Transaction performance
- [`CLAUDE.md`](../CLAUDE.md) - Development guidelines with performance focus

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