# Infrastructure Performance Tests

Low-level implementation quality tests for core Velostream components.

## Test Categories

### In-Process Tests (Run in CI/CD)

These tests run without external dependencies and are included in the CI/CD pipeline:

#### `table_lookup.rs`
**Tests**: OptimizedTableImpl performance
- **Metrics**: O(1) key lookups, query caching, memory efficiency
- **Runs**: ~500ms
- **Dependencies**: None (in-process)
- **Purpose**: Validate fundamental table operations don't regress

Run with:
```bash
cargo test --test mod infrastructure::table_lookup -- --nocapture
```

### Kafka-Dependent Tests (Optional Local Testing)

These tests require Kafka running locally and are kept in `examples/performance/` for optional testing:

| File | Purpose | Run Command |
|------|---------|-------------|
| `phase4_batch_benchmark.rs` | Batch strategy optimization | `cargo run --example phase4_batch_benchmark` |
| `datasource_performance_test.rs` | Abstraction layer overhead | `cargo run --example datasource_performance_test` |
| `json_performance_test.rs` | JSON serialization throughput | `cargo run --example json_performance_test` |
| `raw_bytes_performance_test.rs` | Raw byte processing | `cargo run --example raw_bytes_performance_test` |
| `latency_performance_test.rs` | End-to-end latency | `cargo run --example latency_performance_test` |
| `simple_async_optimization_test.rs` | Async pattern optimization | `cargo run --example simple_async_optimization_test` |
| `simple_zero_copy_test.rs` | Zero-copy patterns | `cargo run --example simple_zero_copy_test` |

**Requirements**:
- Kafka running on localhost:9092
- Topic auto-creation enabled

## Running Tests

### All infrastructure tests (in-process only)
```bash
cargo test --test mod infrastructure -- --nocapture
```

### Specific infrastructure test
```bash
cargo test --test mod infrastructure::table_lookup -- --nocapture
```

### With performance output
```bash
RUST_LOG=info cargo test --test mod infrastructure -- --nocapture --ignored
```

## Performance Baseline

### OptimizedTableImpl

| Metric | Baseline | Threshold |
|--------|----------|-----------|
| Key Lookup Throughput | >100,000 lookups/sec | ✅ O(1) performance |
| Query Cache Speedup | >1.1x | ✅ Caching effective |
| Memory per Record | <1000 bytes | ✅ Efficient |

## When to Use Each Test

### In-Process Tests (table_lookup.rs)
- ✅ CI/CD pipelines (no Kafka dependency)
- ✅ Quick regression detection
- ✅ Development iteration
- ✅ Pull request validation

### Kafka Tests (examples/)
- ✅ Local deep performance analysis
- ✅ Load testing with real Kafka
- ✅ Optimization verification before commit
- ⚠️ Require Kafka setup
- ⚠️ Slower to run

## Adding New Tests

### Process

1. **Write test** in dedicated file: `infrastructure/[feature_name].rs`
2. **Register module** in `infrastructure/mod.rs`: `pub mod [feature_name];`
3. **Add documentation** with:
   - Purpose statement
   - Key metrics
   - Performance thresholds
   - Run instructions
4. **Verify compilation**: `cargo check --all-targets --no-default-features`
5. **Run test**: `cargo test --test mod infrastructure::[feature_name]`

### Template

```rust
//! [Feature] Performance - Infrastructure Test
//!
//! **Purpose**: [What does this test measure?]
//! **Key Metrics**: [List of measurements]
//! **Threshold**: [Minimum acceptable performance]

#[test]
fn test_[feature]_infrastructure_performance() {
    // Setup
    // Benchmark
    // Assert thresholds
    // Print results
}
```

## Consolidated Infrastructure Categories

The following infrastructure test categories have been consolidated:

| Category | Test Files | Purpose |
|----------|-----------|---------|
| **Table Operations** | table_lookup | O(1) lookups, caching, memory |
| **Batch Processing** | phase4_batch_benchmark (Kafka) | MegaBatch, RingBuffer strategies |
| **Abstraction Layer** | datasource_performance_test (Kafka) | Zero-overhead abstractions |
| **Serialization** | json_performance_test (Kafka) | Format throughput |
| **Raw Performance** | raw_bytes_performance_test (Kafka) | Byte-level processing |
| **Latency** | latency_performance_test (Kafka) | End-to-end timing |
| **Async** | simple_async_optimization_test (Kafka) | Async pattern performance |
| **Zero-Copy** | simple_zero_copy_test (Kafka) | Memory-efficient patterns |

## Strategy

**Why Keep Kafka Tests as Examples?**

1. **Complexity**: Async/Kafka integration is high-friction for test setup
2. **CI/CD**: Tests don't fail, infrastructure does - confusing error messages
3. **Value**: Better to have working examples than broken tests
4. **Flexibility**: Users who need deep testing can run locally with Kafka
5. **Simplicity**: In-process tests provide core validation for CI/CD

**Future Improvements**:
- Mock Kafka interface for simpler testing
- Extract core logic into standalone benchmarks
- Add testcontainers support for containerized Kafka

See [CONSOLIDATION_PLAN.md](./CONSOLIDATION_PLAN.md) for detailed strategy.

## References

- **Phase**: Performance Testing Infrastructure Reorganization Phase 2
- **Related**: SQL Operations tests in `sql_operations/`
- **Documentation**: STREAMING_SQL_OPERATION_RANKING.md
