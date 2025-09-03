# FerrisStreams Benchmarks

This directory contains production-ready benchmarks for FerrisStreams performance evaluation.

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

### All Benchmarks
```bash
# Run all benchmarks with detailed output
cargo bench

# Run with statistical analysis
cargo bench -- --verbose
```

### Specific Benchmark Categories
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