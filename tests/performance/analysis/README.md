# Performance Analysis Tests

Detailed profiling tests to identify performance bottlenecks and measure overhead in various execution paths.

## Test Modes

Performance tests support three modes controlled by environment variables:

### 1. CI/CD Mode (Default)
Fast smoke tests with minimal records for regression detection.

```bash
# Explicit CI mode
PERF_TEST_MODE=ci cargo test --tests --no-default-features --release

# Or just run without env vars (defaults to CI)
cargo test --tests --no-default-features --release
```

**Configuration**: 1,000 records, 100 batch size
**Purpose**: Fast regression detection in CI/CD pipelines
**Runtime**: ~1-2 seconds per test

### 2. Performance Mode
Full profiling with large datasets for detailed performance analysis.

```bash
PERF_TEST_MODE=performance cargo test --tests --no-default-features --release
```

**Configuration**: 100,000 records, 1,000 batch size
**Purpose**: Detailed performance profiling and bottleneck identification
**Runtime**: ~10-30 seconds per test

### 3. Custom Mode
User-defined record count for specific profiling needs.

```bash
PERF_TEST_MODE=custom PERF_TEST_RECORDS=50000 cargo test --tests --no-default-features --release
```

**Configuration**: Custom record count, auto-calculated batch size
**Purpose**: Flexible profiling for specific scenarios

## Running Specific Tests

### FR-082 Phase 4E: Job Server Overhead Analysis

```bash
# CI mode (fast)
PERF_TEST_MODE=ci cargo test --release job_server_tumbling_window_performance -- --nocapture

# Performance mode (full profiling)
PERF_TEST_MODE=performance cargo test --release job_server_tumbling_window_performance -- --nocapture
```

### Pure SQL Engine Tests

```bash
# Tumbling window profiling
PERF_TEST_MODE=performance cargo test --release profile_tumbling_instrumented_standard_path -- --nocapture

# GROUP BY profiling
PERF_TEST_MODE=performance cargo test --release profile_pure_group_by_direct_execution -- --nocapture
```

## Test Categories

### 1. Pure SQL Engine Tests
Tests that measure raw SQL execution engine performance without job server overhead.

- `tumbling_instrumented_profiling.rs` - Tumbling window + GROUP BY baseline
- `group_by_pure_profiling.rs` - Pure GROUP BY performance
- `overhead_profiling.rs` - Component-level overhead measurement

### 2. Job Server Tests
Tests that measure full job server infrastructure performance.

- `tumbling_job_server_test.rs` - Job server tumbling window performance
- `tumbling_emit_changes_job_server_test.rs` - EMIT CHANGES overhead measurement

### 3. Window-Specific Tests
- `tumbling_window_profiling.rs` - Tumbling window detailed profiling
- `sliding_window_profiling.rs` - Sliding window performance
- `session_window_profiling.rs` - Session window performance
- `rows_window_profiling.rs` - ROWS window performance

## Expected Performance Baselines

### FR-082 Phase 4E Results (5K records, release mode)

| Execution Path | Throughput | Overhead vs Pure Engine |
|----------------|------------|------------------------|
| Pure SQL Engine (Tumbling + GROUP BY) | 790,399 rec/sec | Baseline (0%) |
| Job Server (Standard Mode) | 23,591 rec/sec | 97.0% (33.5x slowdown) |
| Job Server (EMIT CHANGES) | TBD | TBD |

### Overhead Sources
The 97% overhead from job server infrastructure includes:
1. Batch allocation and coordination
2. Arc<Mutex<>> lock contention per batch
3. Metrics collection and tracking
4. Channel communication overhead
5. Error handling and retry logic

## Using Test Helpers

The `test_helpers.rs` module provides shared utilities:

```rust
use crate::performance::analysis::test_helpers::*;
use crate::performance::analysis::test_config::PerfTestConfig;

#[tokio::test]
async fn my_performance_test() {
    // Get configuration from environment
    let config = PerfTestConfig::from_env();

    println!("Running in {} mode", config.mode_description());
    println!("Testing with {} records", config.num_records);

    // Generate test data
    let records = generate_tumbling_window_records(config.num_records);

    // Use mock data source/writer
    let data_source = MockDataSource::new(records, config.batch_size);
    let data_writer = MockDataWriter::new();

    // Run test...

    // Print comparison
    print_performance_comparison(
        "My Test",
        "Baseline:",
        100_000.0,
        "Measured:",
        measured_throughput,
    );
}
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Performance Regression Tests

on: [pull_request]

jobs:
  performance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Run performance smoke tests
        run: |
          PERF_TEST_MODE=ci cargo test --tests --no-default-features --release -- --test-threads=1
        env:
          RUST_BACKTRACE: 1
```

### Local Development

```bash
# Quick smoke test before commit
PERF_TEST_MODE=ci cargo test --release

# Full profiling for performance work
PERF_TEST_MODE=performance cargo test --release -- --nocapture

# Custom profiling for specific investigation
PERF_TEST_MODE=custom PERF_TEST_RECORDS=25000 PERF_TEST_VERBOSE=true \
  cargo test --release my_test -- --nocapture
```

## Best Practices

1. **Always use `--release` mode** for performance tests
2. **Use `--test-threads=1`** for consistent results
3. **Run CI mode tests before committing** to catch regressions
4. **Use performance mode for benchmarking** and optimization work
5. **Document baseline numbers** in test comments for comparison

## Adding New Performance Tests

1. Create test file in `tests/performance/analysis/`
2. Use `PerfTestConfig::from_env()` for configurable record counts
3. Use shared helpers from `test_helpers.rs`
4. Add module to `mod.rs`
5. Document expected performance in test comments

Example template:

```rust
use crate::performance::analysis::test_config::PerfTestConfig;
use crate::performance::analysis::test_helpers::*;

#[tokio::test]
async fn my_new_performance_test() {
    let config = PerfTestConfig::from_env();

    println!("Testing with {} records", config.num_records);

    let records = generate_tumbling_window_records(config.num_records);

    // Test implementation...

    print_performance_comparison(
        "My Test Comparison",
        "Baseline:",
        baseline_throughput,
        "Measured:",
        measured_throughput,
    );
}
```

## Troubleshooting

### Tests running too slow in CI
```bash
# Verify CI mode is active
PERF_TEST_MODE=ci cargo test --release -- --show-output
```

### Inconsistent results
```bash
# Run with single thread for consistency
PERF_TEST_MODE=performance cargo test --release -- --test-threads=1 --nocapture
```

### Memory issues
```bash
# Use smaller custom dataset
PERF_TEST_MODE=custom PERF_TEST_RECORDS=10000 cargo test --release
```
