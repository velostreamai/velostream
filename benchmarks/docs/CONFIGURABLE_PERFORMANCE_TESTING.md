# Configurable Performance Testing Guide

## Overview

All SQL operation performance tests in `tests/performance/analysis/sql_operations/` now support configurable record counts and cardinality via environment variables. This allows you to run tests at different scales without modifying code.

## Environment Variables

### `VELOSTREAM_PERF_RECORDS`

Controls the total number of records processed in each test.

**Default**: 10,000 records
**Format**: Integer (e.g., `100000`, `1000000`)

**Examples**:
```bash
# Run tests with 100K records (10x baseline)
VELOSTREAM_PERF_RECORDS=100000 cargo test --tests tier1_essential

# Run tests with 1M records (100x baseline)
VELOSTREAM_PERF_RECORDS=1000000 cargo test --tests tier1_essential

# Run specific test with custom record count
VELOSTREAM_PERF_RECORDS=50000 cargo test --tests select_where_performance
```

### `VELOSTREAM_PERF_CARDINALITY`

Controls the number of unique groups for GROUP BY operations.

**Default**: 1/10th of record count (e.g., 1000 for 10K records)
**Format**: Integer (e.g., `100`, `10000`)

**Examples**:
```bash
# Run GROUP BY tests with 10,000 unique groups
VELOSTREAM_PERF_CARDINALITY=10000 cargo test --tests group_by_continuous

# Combine with record count for high-cardinality stress test
VELOSTREAM_PERF_RECORDS=1000000 VELOSTREAM_PERF_CARDINALITY=100000 cargo test --tests group_by_continuous
```

## Common Testing Scenarios

### Scenario 1: Quick Smoke Test
```bash
# Fast validation with minimal records (default 10K)
VELOSTREAM_PERF_RECORDS=1000 cargo test --tests tier1_essential --release

# Expected time: ~30 seconds for all Tier 1 tests
```

### Scenario 2: Standard Benchmark
```bash
# Production-quality benchmark with default 10K records
./benchmarks/run_all_sql_operations.sh release

# Expected time: ~5-10 minutes for all operations
```

### Scenario 3: High-Volume Testing
```bash
# Stress test with 100K records
VELOSTREAM_PERF_RECORDS=100000 ./benchmarks/run_all_sql_operations.sh release

# Expected time: ~30-45 minutes for all operations
```

### Scenario 4: High-Cardinality GROUP BY
```bash
# Test GROUP BY performance with many unique groups
VELOSTREAM_PERF_RECORDS=100000 VELOSTREAM_PERF_CARDINALITY=10000 \
  cargo test --tests group_by_continuous --release

# Expected time: ~2-3 minutes for this test
```

### Scenario 5: Memory Stress Test
```bash
# Stream-Stream JOIN with large dataset (memory-intensive)
VELOSTREAM_PERF_RECORDS=500000 cargo test --tests stream_stream_join --release

# Warning: May consume 2-4GB RAM, ensure system has available memory
```

### Scenario 6: Custom Benchmark Script
```bash
# Run Tier 1 tests with 50K records for faster iteration
VELOSTREAM_PERF_RECORDS=50000 ./benchmarks/run_all_sql_operations.sh debug tier1

# Expected time: ~2-3 minutes
```

## Performance Test Coverage

| Test | Default Records | Cardinality | Notes |
|------|-----------------|-------------|-------|
| **Tier 1** | | | |
| select_where | 10K | 1K (fixed) | Stateless filtering |
| rows_window | 10K | 10 (fixed) | Sliding row buffer |
| group_by_continuous | 10K | 1K (1/10) | Configurable cardinality |
| tumbling_window | 10K | 50 × 100 (composite) | Trader ID + Symbol |
| stream_table_join | 1K stream | 5K table | Table size matters |
| **Tier 2** | | | |
| scalar_subquery | 10K | N/A | Config lookups |
| timebased_join | 10K | 10 (combined) | Two stream join |
| having_clause | 10K | 200 (1/50) | High cardinality GROUP BY |
| **Tier 3** | | | |
| exists_subquery | 10K | 1K (1/10) | Existence checks |
| stream_stream_join | 10K | N/A | Memory-intensive |
| in_subquery | 10K | 1K (1/10) | Set membership |
| correlated_subquery | 10K | 1K (1/10) | Per-row correlation |
| **Tier 4** | | | |
| any_all_operators | 10K | N/A | Comparison operators |
| recursive_ctes | 10K | N/A | Hierarchical data |

## Scaling Guidelines

### Record Count Impact

**10K → 100K (10x)**: ~10x slower throughput (due to more data, similar algorithm complexity)
- Small impact on stateless operations (SELECT, ROWS WINDOW)
- Moderate impact on stateful operations (GROUP BY, Stream-Table JOIN)
- Large impact on memory-intensive operations (Stream-Stream JOIN)

**100K → 1M (10x)**: Linear slowdown continues
- Useful for finding memory bottlenecks
- May reveal cache efficiency issues
- Can identify algorithmic complexity problems

### Cardinality Impact (for GROUP BY)

**1K → 10K (10x)**: ~15-30% throughput degradation
- Larger state dictionary
- More context switches in HashMap lookups
- Cascading aggregation overhead

**10K → 100K (10x)**: ~20-50% additional throughput degradation
- Hash table growth beyond L1/L2 cache
- Potential memory fragmentation
- Spilling to main memory from cache

## Monitoring and Analysis

### Check Test Output Configuration

```bash
# See what configuration is being used
cargo test --tests select_where_performance -- --nocapture 2>&1 | \
  grep -A 10 "Performance Test Configuration"
```

### Collect Baseline Measurements

```bash
# Create baseline with 10K records (default)
./benchmarks/run_all_sql_operations.sh release all > baseline_10k.txt

# Create comparison with 100K records
VELOSTREAM_PERF_RECORDS=100000 ./benchmarks/run_all_sql_operations.sh release all > baseline_100k.txt

# Compare results
diff baseline_10k.txt baseline_100k.txt
```

### Profile Specific Operations

```bash
# Profile GROUP BY with increasing cardinality
for card in 100 1000 10000 100000; do
  echo "=== Cardinality: $card ==="
  VELOSTREAM_PERF_RECORDS=100000 VELOSTREAM_PERF_CARDINALITY=$card \
    cargo test --tests group_by_continuous --release -- --nocapture 2>&1 | \
    grep "Throughput"
done
```

## Troubleshooting

### Test Runs Out of Memory

If you encounter OOM errors with high record counts:

1. **Reduce record count**: Start with 50K instead of 100K
2. **Focus on specific operations**: Test one tier or operation at a time
3. **Monitor system memory**: Use `top` or `Activity Monitor` while running tests
4. **Stream-Stream JOIN is most memory-intensive**: Test this last

**Example safe progression**:
```bash
VELOSTREAM_PERF_RECORDS=10000 cargo test  # Works everywhere
VELOSTREAM_PERF_RECORDS=50000 cargo test  # Safe for most systems
VELOSTREAM_PERF_RECORDS=100000 cargo test # Requires 4-8GB available RAM
```

### Test Timeout

If tests timeout (180-second limit per test):

1. **Reduce record count**: Check current setting with `env | grep PERF_RECORDS`
2. **Run in release mode**: Debug mode is much slower
3. **Run individual tests**: Isolate slow operations
4. **Check system load**: Background processes slow down benchmarks

```bash
# Run just Tier 1 in release mode (should complete in <2 minutes)
./benchmarks/run_all_sql_operations.sh release tier1
```

### Results Vary Significantly

If throughput measurements vary >20% between runs:

1. **Increase sample size**: Run test multiple times and average
2. **Ensure system is idle**: Close other applications
3. **Check CPU throttling**: System may be thermal throttling
4. **Use consistent record count**: Use same VELOSTREAM_PERF_RECORDS for comparisons

## Integration with CI/CD

Add configurable testing to continuous integration:

```yaml
# GitHub Actions example
- name: Run Performance Benchmarks (Quick)
  run: |
    VELOSTREAM_PERF_RECORDS=5000 ./benchmarks/run_all_sql_operations.sh release

- name: Run Performance Benchmarks (Standard)
  if: github.event_name == 'push' && github.ref == 'refs/heads/master'
  run: |
    ./benchmarks/run_all_sql_operations.sh release all

- name: Run Performance Benchmarks (Extended - scheduled)
  if: github.event.schedule == '0 2 * * *'  # 2 AM UTC
  run: |
    VELOSTREAM_PERF_RECORDS=100000 ./benchmarks/run_all_sql_operations.sh release all
```

## Documentation

Each test file displays its configuration automatically:

```
📊 Performance Test Configuration:
   Records: 10000 (env: VELOSTREAM_PERF_RECORDS)
   Cardinality: 1000 (env: VELOSTREAM_PERF_CARDINALITY)
   Tip: Set VELOSTREAM_PERF_RECORDS=100000 for higher-volume testing
```

This helps users understand:
- What scale they're testing at
- How to modify the configuration
- Recommendations for different test scenarios

## Summary

| Use Case | Command | Time | Memory |
|----------|---------|------|--------|
| Quick validation | `VELOSTREAM_PERF_RECORDS=1000 cargo test` | 30s | <500MB |
| Standard benchmark | `./run_all_sql_operations.sh release` | 5-10m | 1-2GB |
| High-volume test | `VELOSTREAM_PERF_RECORDS=100000 ./run_all_sql_operations.sh release` | 30-45m | 2-4GB |
| Stress test | `VELOSTREAM_PERF_RECORDS=500000 cargo test --tests stream_stream_join` | 5-10m | 4-8GB |
| Development iteration | `VELOSTREAM_PERF_RECORDS=5000 cargo test --tests tier1` | 1m | <500MB |

---

**Version**: 1.0
**Last Updated**: November 2025
**Compatible With**: All SQL operation tests in `tests/performance/analysis/sql_operations/`
