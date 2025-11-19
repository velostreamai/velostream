# Velostream Benchmarks

This directory contains production-ready benchmarks for Velostream performance evaluation.

## Structure

```
benchmarks/
├── README.md                    # This file
├── Cargo.toml                   # Benchmark-specific dependencies
├── benches/                     # Criterion.rs benchmarks
│   ├── financial_precision.rs  # ScaledInteger vs f64 benchmarks
│   ├── serialization.rs        # Codec performance benchmarks
│   ├── memory_allocation.rs    # Memory profiling benchmarks
│   ├── kafka_pipeline.rs       # End-to-end pipeline benchmarks
│   └── sql_execution.rs        # SQL engine benchmarks
└── src/                         # Benchmark utilities and test data
    ├── lib.rs                   # Common benchmark utilities
    ├── test_data.rs            # Test data generators
    └── profiling.rs            # Profiling utilities
```

## Running Benchmarks

### Baseline Scenario Comparison (FR-082)

**Comprehensive baseline comparison test** measuring all 5 scenarios with query-aware strategy selection:

```bash
# Quick iteration with incremental compilation (5-10 seconds on cached builds)
./run_baseline_quick.sh

# Full release build with optimizations
./run_baseline.sh

# Flexible mode: choose build profile and scenarios
./run_baseline_flexible.sh release    # Release build, all scenarios
./run_baseline_flexible.sh debug 1    # Debug build, scenario 1 only
./run_baseline_flexible.sh profile 2  # Profile build, scenario 2 only

# Run comprehensive baseline comparison test directly
cargo test --test comprehensive_benchmark_test --release --no-default-features comprehensive_baseline_comparison -- --nocapture
```

**Scenarios Tested**:
- **Scenario 0**: Pure SELECT (passthrough, no aggregation)
- **Scenario 1**: ROWS WINDOW (analytic function, memory-bounded buffers)
- **Scenario 2**: GROUP BY (hash aggregation)
- **Scenario 3a**: TUMBLING WINDOW + GROUP BY (batch emission)
- **Scenario 3b**: TUMBLING WINDOW + EMIT CHANGES (continuous emission)

**Implementations Compared**:
- SQL Engine Sync (baseline, synchronous)
- SQL Engine Async (baseline, asynchronous)
- SimpleJobProcessor (single-threaded, best-effort)
- TransactionalJobProcessor (single-threaded, at-least-once)
- AdaptiveJobProcessor @ 1-core (partitioned, 1 core)
- AdaptiveJobProcessor @ 4-core (partitioned, 4 cores)

**Query-Aware Strategy Selection**:
Each scenario automatically selects optimal partitioning strategy:
- Pure SELECT → sticky_partition (no rekeying)
- ROWS WINDOW → sticky_partition (analytic, no rekeying)
- GROUP BY → always_hash (aggregation requires rekeying)
- TUMBLING/SLIDING/SESSION WINDOW → always_hash (window boundaries require rekeying)

**Output Example**:
```
┌─ Scenario 1: Pure SELECT
│
│  SQL Engine Sync (sent: 5000, processed: 5000)
│    461727 rec/sec
│
│  SimpleJp:          345000 rec/sec
│  TransactionalJp:   340000 rec/sec
│  AdaptiveJp@1c:     390000 rec/sec
│  AdaptiveJp@4c:     420000 rec/sec
│
│  Partitioner:       sticky_partition (no rekeying required)
│
│  Ratios vs SQL Engine Sync:
│    SimpleJp:        0.75x
│    AdaptiveJp@4c:   0.91x
│
│  Best: AdaptiveJp@4c
└
```

### Criterion.rs Benchmarks

#### All Benchmarks
```bash
# Run all benchmarks with detailed output
cargo bench

# Run with statistical analysis
cargo bench -- --verbose
```

#### Specific Benchmark Categories
```bash
# Financial precision benchmarks
cargo bench financial_precision

# Memory allocation benchmarks
cargo bench memory_allocation

# Kafka pipeline benchmarks
cargo bench kafka_pipeline
```

### Performance Profiling
```bash
# CPU profiling with flamegraph
cargo flamegraph --bench financial_precision

# Memory profiling (requires jemalloc feature)
cargo bench memory_allocation --features jemalloc
```

## Benchmark Categories

### 0. Baseline Scenario Comparison (`tests/performance/analysis/comprehensive_baseline_comparison.rs`)

**FR-082: Comprehensive Baseline Comparison** - Multi-implementation performance testing with automatic strategy selection.

Features:
- **5 realistic scenarios** covering Pure SELECT, ROWS WINDOW, GROUP BY, TUMBLING windows
- **6 implementations** from SQL Engine baseline to partitioned AdaptiveJobProcessor
- **Query-aware partitioning** auto-selects optimal strategy per scenario
- **Kafka-like delivery** with KafkaSimulatorDataSource providing partition-grouped batches
- **Detailed metrics** including throughput, sent/processed records, and performance ratios

Key benchmarks:
- **Data loading**: 5000+ rec/sec with pre-allocated batches
- **Query throughput**: Varies by scenario and implementation (290K-420K rec/sec)
- **Aggregation throughput**: 150K-280K rec/sec with proper partitioning

Test infrastructure:
- `KafkaSimulatorDataSource`: Lock-free data delivery with partition grouping
- `MockDataWriter`: Tracks output records for verification
- Automatic strategy selection via PartitionerSelector (no hardcoded partitioners)

Related documentation:
- `run_baseline_quick.sh` - Quick iteration (5-10 seconds with cache)
- `run_baseline.sh` - Full release build with all scenarios
- `run_baseline_flexible.sh` - Configurable profile and scenario selection
- See `docs/benchmarks/BASELINE_TESTING.md` for detailed methodology

### 1. Financial Precision (`benches/financial_precision.rs`)
- ScaledInteger vs f64 performance comparison
- Financial calculation accuracy validation
- Precision vs performance trade-offs

### 2. Serialization (`benches/serialization.rs`)
- JSON, Avro, Protobuf codec performance
- Schema validation overhead
- Compression efficiency

### 3. Memory Allocation (`benches/memory_allocation.rs`)
- Object pooling efficiency
- Zero-copy optimization validation
- Memory pressure scenarios

### 4. Kafka Pipeline (`benches/kafka_pipeline.rs`)
- End-to-end pipeline throughput
- Transaction processing performance
- Backpressure handling

### 5. SQL Execution (`benches/sql_execution.rs`)
- Query processing latency
- Complex aggregation performance
- Join algorithm efficiency

## Interpreting Results

### Throughput Metrics
- **Records/second**: Primary throughput measure
- **Bytes/second**: Data processing rate
- **Operations/second**: Function call rate

### Latency Metrics
- **Mean**: Average processing time
- **P50/P95/P99**: Percentile latencies
- **Standard deviation**: Consistency measure

### Memory Metrics
- **Allocations/operation**: Memory efficiency
- **Peak memory**: Resource requirements
- **Pool hit rate**: Object reuse effectiveness

## Performance Targets

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| SQL Query Latency (P95) | <10ms | ~7µs | ✅ Excellent |
| Kafka Throughput | 100K+ records/sec | 142K records/sec | ✅ Excellent |
| Memory Efficiency | <1MB/1K records | TBD | 📊 Measuring |
| Financial Precision | Zero error | Zero error | ✅ Perfect |

## Contributing

When adding new benchmarks:

1. Follow the existing structure and naming conventions
2. Include both performance and correctness validation
3. Document expected performance characteristics
4. Add regression detection for critical paths

## CI/CD Integration

Benchmarks are automatically run in CI/CD pipeline:
- Performance regression detection
- Historical performance tracking
- Automated baseline comparison