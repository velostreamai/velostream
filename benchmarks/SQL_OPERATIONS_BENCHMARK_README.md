# SQL Operations Performance Benchmark Collector

## Overview

`run_all_sql_operations.sh` is a comprehensive benchmark collection script that runs all streaming SQL operation performance tests organized by tier and collects detailed metrics for updating the STREAMING_SQL_OPERATION_RANKING.md documentation.

This script automates the process of measuring performance across all 16 SQL operations (15 implemented, 1 pending) and generates a single results file containing all throughput metrics.

## Features

✅ **Tier-Based Organization**: Runs tests organized by business probability (Tier 1-4)
✅ **Flexible Filtering**: Run all tests or specific tiers
✅ **Build Mode Control**: Choose debug or release mode
✅ **Automatic Results Collection**: Captures throughput and implementation-specific metrics
✅ **Timestamped Output**: Results saved with timestamp for tracking
✅ **Detailed Logging**: Full test output preserved for analysis

## Usage

### Basic Commands

```bash
# Run all tests in debug mode (fastest compilation)
./run_all_sql_operations.sh

# Run all tests in release mode (optimized throughput)
./run_all_sql_operations.sh release

# Run only Tier 1 tests in debug mode
./run_all_sql_operations.sh debug tier1

# Run only Tier 2 tests in release mode
./run_all_sql_operations.sh release tier2

# Run Tier 3 and 4 (advanced operations)
./run_all_sql_operations.sh release tier3
./run_all_sql_operations.sh release tier4
```

### Recommended Workflow

For production-quality performance metrics:

```bash
# 1. Run all tests in release mode (this takes ~5-10 minutes)
cd benchmarks
./run_all_sql_operations.sh release all

# 2. Review results
cat results/sql_operations_results_*.txt

# 3. Extract key metrics
grep -E "Throughput|Best Implementation|Operation:" \
  results/sql_operations_results_*.txt | tail -50

# 4. Update documentation with measured values
# See section below for updating STREAMING_SQL_OPERATION_RANKING.md
```

## Command Syntax

```
./run_all_sql_operations.sh [MODE] [TIER_FILTER]

MODE (optional):
  debug    - Fast compilation, debug symbols (DEFAULT)
  release  - Optimized performance, accurate throughput metrics

TIER_FILTER (optional):
  all      - Run all tiers Tier 1-4 (DEFAULT)
  tier1    - Run only Tier 1 (Essential - 5 operations)
  tier2    - Run only Tier 2 (Common - 3 operations)
  tier3    - Run only Tier 3 (Advanced - 4 operations)
  tier4    - Run only Tier 4 (Specialized - 2 operations)
```

## Output Structure

Results are saved to: `benchmarks/results/sql_operations_results_YYYYMMDD_HHMMSS.txt`

Example output format:

```
════════════════════════════════════════════════════════════════
Tier 1: Essential Operations (90-100% Probability)
════════════════════════════════════════════════════════════════

📊 Operation: select_where
   Throughput (best): 135,020 rec/sec
   SQL Sync: 135020 rec/sec
   SQL Async: 111715 rec/sec
   Best Implementation: SQL Sync: 135,020 rec/sec

📊 Operation: rows_window
   Throughput (best): 178,534 rec/sec
   SQL Sync: 178534 rec/sec
   SQL Async: 132711 rec/sec
   Best Implementation: SQL Sync: 178,534 rec/sec

[... more operations ...]
```

## Tier Coverage

### Tier 1: Essential Operations (90-100% Probability)
- Operation #1: **SELECT + WHERE** (99%)
- Operation #2: **ROWS WINDOW** (78%)
- Operation #3: **Stream-Table JOIN** (94%)
- Operation #4: **GROUP BY Continuous** (93%)
- Operation #5: **Windowed Aggregation (TUMBLING)** (92%)

### Tier 2: Common Operations (60-89% Probability)
- Operation #7: **Scalar Subquery** (71%)
- Operation #8: **Time-Based JOIN (WITHIN)** (68%)
- Operation #9: **HAVING Clause** (72%)

### Tier 3: Advanced Operations (30-59% Probability)
- Operation #10: **EXISTS/NOT EXISTS** (48%)
- Operation #11: **Stream-Stream JOIN** (42%)
- Operation #12: **IN/NOT IN Subquery** (55%)
- Operation #13: **Correlated Subquery** (35%)

### Tier 4: Specialized Operations (10-29% Probability)
- Operation #14: **ANY/ALL Operators** (22%)
- Operation #16: **Recursive CTEs** (12%)
- ❌ Operation #15: **MATCH_RECOGNIZE** (15%) - Not yet implemented

## Updating STREAMING_SQL_OPERATION_RANKING.md

After collecting benchmark results, update the documentation:

### 1. Extract Peak Throughput Values

```bash
# Get all throughput measurements
grep "Throughput (best):" results/sql_operations_results_*.txt

# Example output:
# 📊 Operation: select_where
#    Throughput (best): 135,020 rec/sec
# 📊 Operation: rows_window
#    Throughput (best): 178,534 rec/sec
```

### 2. Update the Quick Reference Table

Edit `docs/sql/STREAMING_SQL_OPERATION_RANKING.md` and update the "All Operations at a Glance" table:

Example row format:
```markdown
| 1 | SELECT + WHERE | Tier 1 | 99% | ✅ Complete | **135K evt/sec** | 150-200K evt/sec | `tests/.../select_where.rs` | Peak: SQL Sync - **Good** |
```

Key columns to update:
- **Velostream Peak¹**: Use the "Throughput (best)" value from results
- **Test Location**: Already set to tier-based test locations
- **Notes**: Update with best implementation and any observations

### 3. Update Performance Details Section

For each operation, update the "Performance Details by Category" section with:
- Actual measured throughput
- Best-performing implementation (SQL Sync, SQL Async, SimpleJp, TransactionalJp, etc.)
- Performance multiplier vs Flink baseline (if known)

### 4. Commit Changes

```bash
# Stage updated documentation
git add docs/sql/STREAMING_SQL_OPERATION_RANKING.md

# Commit with benchmark reference
git commit -m "perf: Update SQL operation benchmarks - $(date +%Y%m%d)

Measured performance for all tier-based SQL operations:
- Tier 1: 5/5 operations complete
- Tier 2: 3/3 operations complete
- Tier 3: 4/4 operations complete
- Tier 4: 2/3 operations complete (MATCH_RECOGNIZE pending)

Benchmark results: $(ls -t results/sql_operations_results_*.txt | head -1)
"
```

## Performance Metrics Guide

### Interpreting Throughput Numbers

**Excellent (>150K rec/sec)**
- Stateless operations: SELECT, ROWS WINDOW
- Simple subqueries
- Expected for high-volume filtering

**Good (50-150K rec/sec)**
- Light stateful ops: GROUP BY, scalar lookups
- Stream-Table JOINs
- Reasonable for aggregations

**Acceptable (15-50K rec/sec)**
- Moderate stateful: windowed aggregations
- Time-based JOINs
- Expected trade-off for complex operations

**Challenging (<15K rec/sec)**
- Complex operations: Stream-Stream JOIN
- Advanced patterns
- May be memory-bounded

### Key Performance Factors

1. **Record Count**: Tests use 10K records (adjustable via VELOSTREAM_BASELINE_RECORDS)
2. **Cardinality**: Number of unique groups/keys affects throughput
3. **Window Size**: Larger windows → lower throughput
4. **Aggregation Complexity**: More aggregates → lower throughput

## Troubleshooting

### Test Timeout (180 seconds)

If tests timeout:
```bash
# Run a single tier in debug mode first
./run_all_sql_operations.sh debug tier1

# Check if specific operation is hanging
cargo test --tests performance::analysis::sql_operations::tier1::select_where -- --nocapture
```

### Missing Results

If an operation shows "N/A" for throughput:
1. Check test compiled successfully: `cargo test --tests tier1 --no-default-features -- --list`
2. Run test manually: `cargo test --tests OPERATION_NAME -- --nocapture`
3. Review test output for errors

### Performance Variance

If results vary significantly between runs:
1. Ensure system is idle (no other heavy processes)
2. Use release mode for production metrics: `./run_all_sql_operations.sh release`
3. Run multiple times and average results
4. Check system temperature/throttling

## File Locations

- **Script**: `benchmarks/run_all_sql_operations.sh`
- **Results**: `benchmarks/results/sql_operations_results_*.txt`
- **Tests**: `tests/performance/analysis/sql_operations/`
  - `tier1_essential/` - 5 operations
  - `tier2_common/` - 3 operations
  - `tier3_advanced/` - 4 operations
  - `tier4_specialized/` - 2 operations
- **Documentation**: `docs/sql/STREAMING_SQL_OPERATION_RANKING.md`

## Advanced Usage

### Custom Record Counts

Modify test behavior by setting environment variables:

```bash
# Test with 100K records instead of default 10K
VELOSTREAM_BASELINE_RECORDS=100000 ./run_all_sql_operations.sh release

# Test with 1M records (for stress testing)
VELOSTREAM_BASELINE_RECORDS=1000000 ./run_all_sql_operations.sh release tier1
```

### Profiling with Flamegraph

To create a CPU flame graph of a specific operation:

```bash
# Install cargo-flamegraph if not already installed
cargo install flamegraph

# Profile a specific test
cargo flamegraph --test mod -- \
  performance::analysis::sql_operations::tier1::select_where \
  --nocapture -- --test-threads=1
```

## Integration with CI/CD

This script can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions integration
- name: Collect SQL Operation Benchmarks
  run: |
    cd benchmarks
    ./run_all_sql_operations.sh release all

- name: Archive Results
  uses: actions/upload-artifact@v3
  with:
    name: benchmark-results
    path: benchmarks/results/
```

## Next Steps

1. **Run the benchmark collector**
   ```bash
   ./run_all_sql_operations.sh release all
   ```

2. **Review the results**
   ```bash
   cat results/sql_operations_results_*.txt
   ```

3. **Update STREAMING_SQL_OPERATION_RANKING.md** with actual measurements

4. **Commit with timestamp**
   ```bash
   git add docs/ benchmarks/results/
   git commit -m "perf: Updated SQL operation benchmarks $(date)"
   ```

## Questions?

For detailed information about specific operations:
- Tier 1: `docs/sql/STREAMING_SQL_OPERATION_RANKING.md` Section 1-4
- Tier 2: `docs/sql/STREAMING_SQL_OPERATION_RANKING.md` Section 5-8
- Tier 3: `docs/sql/STREAMING_SQL_OPERATION_RANKING.md` Section 9-12
- Tier 4: `docs/sql/STREAMING_SQL_OPERATION_RANKING.md` Section 13-15

---

**Script Version**: 1.0
**Last Updated**: November 2025
**Compatible With**: Velostream main branch
