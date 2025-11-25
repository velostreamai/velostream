# 🚀 Velostream Benchmark Workflow Guide

## Quick Start: Run Everything

```bash
# Complete workflow: benchmark + update docs + generate report
./benchmarks/run_and_update_benchmarks.sh [MODE] [TIER]
```

---

## 🔧 Configure Records and Cardinality

Set these environment variables **before** running the script:

```bash
# Records: total number of events to test with
export VELOSTREAM_PERF_RECORDS=1000000      # 1 million records

# Cardinality: number of unique groups for GROUP BY operations
export VELOSTREAM_PERF_CARDINALITY=50       # 50 unique groups
```

**Then run**:
```bash
./benchmarks/run_and_update_benchmarks.sh release all
```

**Or as a one-liner**:
```bash
VELOSTREAM_PERF_RECORDS=1000000 VELOSTREAM_PERF_CARDINALITY=50 ./benchmarks/run_and_update_benchmarks.sh release all
```

### Configuration Options

| Parameter | Default | Purpose | Example |
|-----------|---------|---------|---------|
| `VELOSTREAM_PERF_RECORDS` | 10,000 | Total number of records | `1000000` (1M) |
| `VELOSTREAM_PERF_CARDINALITY` | records/10 | Unique groups for GROUP BY | `50` |

**Note**: If `VELOSTREAM_PERF_CARDINALITY` is not set, it defaults to 1/10th of record count. So 1M records = 100K groups by default.

### Common Scenarios

```bash
# Small cardinality test (1M records, 50 groups)
VELOSTREAM_PERF_RECORDS=1000000 VELOSTREAM_PERF_CARDINALITY=50 ./benchmarks/run_and_update_benchmarks.sh release tier1

# Default cardinality (1M records, 100K groups = 1/10th)
VELOSTREAM_PERF_RECORDS=1000000 ./benchmarks/run_and_update_benchmarks.sh release all

# High cardinality test (1M records, 1M unique groups)
VELOSTREAM_PERF_RECORDS=1000000 VELOSTREAM_PERF_CARDINALITY=1000000 ./benchmarks/run_and_update_benchmarks.sh release tier3
```

---

## Main Benchmark Script

### `run_and_update_benchmarks.sh` ⭐ **[USE THIS ONE]**

The unified script that runs the **complete workflow** in one command:

1. **Step 1**: Run SQL operation benchmarks
2. **Step 2**: Extract performance metrics (SQL Sync + fastest JobProcessor)
3. **Step 3**: Update documentation with results
4. **Step 4**: Generate final summary report

**Usage**:
```bash
# Default: release mode, all tiers
./benchmarks/run_and_update_benchmarks.sh

# Specific tier (faster)
./benchmarks/run_and_update_benchmarks.sh debug tier1

# Full options
./benchmarks/run_and_update_benchmarks.sh [MODE] [TIER]
  MODE: release (default) | debug | profile
  TIER: all (default) | tier1 | tier2 | tier3 | tier4
```

**Output**:
- Benchmark results: `benchmarks/results/sql_operations_results_YYYYMMDD_HHMMSS.txt`
- Summary report: `benchmarks/results/LATEST_SUMMARY.txt`
- Updated docs: `docs/sql/STREAMING_SQL_OPERATION_RANKING.md`

---

## Other Benchmark Scripts

### Baseline Scripts (Legacy, for reference)
- `run_baseline.sh` - Run comprehensive baseline comparison
- `run_baseline_flexible.sh` - Run specific scenarios in debug/release/profile mode
- `run_baseline_options.sh` - Multiple compilation modes
- `run_baseline_quick.sh` - Quick iteration with minimal recompilation

### Utility Scripts
- `run_all_sql_operations.sh` - Run all 14 SQL operation benchmarks (used by main workflow)
- `generate_performance_summary.sh` - Extract metrics from results (used by main workflow)
- `update_docs_with_results.sh` - Update docs from results (used by main workflow)

---

## Common Workflows

### Scenario 1: Quick Testing (Development)
```bash
export VELOSTREAM_PERF_RECORDS=100
./benchmarks/run_and_update_benchmarks.sh debug tier1
```
⏱️ **Time**: ~2-3 minutes | Uses 100 records per test

### Scenario 2: Full Benchmarks (Pre-Release)
```bash
export VELOSTREAM_PERF_RECORDS=10000
./benchmarks/run_and_update_benchmarks.sh release all
```
⏱️ **Time**: ~15-20 minutes | Uses 10K records (default)

### Scenario 3: Extended Testing (Performance Analysis)
```bash
export VELOSTREAM_PERF_RECORDS=100000
./benchmarks/run_and_update_benchmarks.sh release all
```
⏱️ **Time**: ~60+ minutes | Uses 100K records

### Scenario 4: Specific Operation Testing
```bash
export VELOSTREAM_PERF_RECORDS=1000
./benchmarks/run_and_update_benchmarks.sh debug tier3
```
⏱️ **Time**: ~5-10 minutes | Tests only Tier 3 operations

---

## Configuration

### Performance Record Count
```bash
export VELOSTREAM_PERF_RECORDS=100      # Quick testing
export VELOSTREAM_PERF_RECORDS=10000    # Standard (default)
export VELOSTREAM_PERF_RECORDS=100000   # Extended analysis
```

### Build Mode
- `release` - Optimized performance (default)
- `debug` - Fast compilation, slower execution
- `profile` - Debug symbols included

### Tier Filtering
- `all` - All 14 operations (default)
- `tier1` - 5 essential operations
- `tier2` - 3 common operations
- `tier3` - 3 advanced operations
- `tier4` - 2 specialized operations

---

## Documentation

📚 **See `benchmarks/docs/` for detailed guides**:

- **README.md** - Overview of benchmark system
- **SQL_OPERATIONS_BENCHMARK_README.md** - Details on SQL operation tests
- **UPDATE_DOCS_README.md** - How to update documentation
- **CONFIGURABLE_PERFORMANCE_TESTING.md** - Advanced configuration options

---

## Next Steps After Running

### 1. Review Results
```bash
cat ./benchmarks/results/LATEST_SUMMARY.txt
```

### 2. View Documentation Changes
```bash
git diff docs/sql/STREAMING_SQL_OPERATION_RANKING.md
```

### 3. Compare with Flink Baseline
```bash
grep -A 20 "BENCHMARK_TABLE_START" docs/sql/STREAMING_SQL_OPERATION_RANKING.md
```

### 4. Commit Changes (if satisfied)
```bash
git add docs/ benchmarks/results/
git commit -m "perf: Update benchmark results - $(date +%Y%m%d)"
```

---

## Key Metrics Tracked

For each SQL operation:
- **SQL Sync**: Synchronous SQL Engine baseline
- **SQL Async**: Asynchronous SQL Engine
- **SimpleJp**: Simple Job Processor (V1)
- **TransactionalJp**: Transactional Job Processor
- **AdaptiveJp (1c)**: Adaptive with 1 core
- **AdaptiveJp (4c)**: Adaptive with 4 cores
- **Flink Baseline**: Approximate 2024 Flink/ksqlDB performance (for comparison)

---

## Troubleshooting

### Script not executable?
```bash
chmod +x ./benchmarks/*.sh
```

### Tests timing out?
Reduce record count or use faster compilation mode:
```bash
export VELOSTREAM_PERF_RECORDS=100
./benchmarks/run_and_update_benchmarks.sh debug tier1
```

### Documentation markers missing?
The comparison script requires markers in the docs file:
```html
<!-- BENCHMARK_TABLE_START -->
<!-- BENCHMARK_TABLE_END -->
```

---

**Status**: Production Ready
**Last Updated**: November 2025
