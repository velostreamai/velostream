# One Billion Row Challenge (1BRC) — Velostream Edition

Compute **MIN / AVG / MAX** temperature per weather station using Velostream's streaming SQL engine with memory-mapped file I/O.

Based on the [1BRC challenge](https://github.com/gunnarmorling/1brc): given a file of `station;temperature` rows, produce per-station aggregates as fast as possible.

## Architecture

```
measurements.txt ──▶ file_source_mmap ──▶ SQL Engine (GROUP BY + EMIT FINAL) ──▶ file_sink ──▶ 1brc_results.csv
                     (mmap reader)         MIN/AVG/MAX per station                             (CSV output)
```

- **`velo-1brc generate`** — generates `measurements.txt` (semicolon-delimited) and `expected.csv` (ground truth)
- **`1brc.sql`** — SQL application using `file_source_mmap` for zero-copy reads and `EMIT FINAL` for deferred output
- **`velo-test`** — runs the SQL application and validates output against expected results

## Quick Start

```bash
# Build (from project root)
cargo build --release --bin velo-1brc --bin velo-test --no-default-features

# Run with validation (default: 1M rows)
./demo/1brc/run-1brc.sh

# Run with 100M rows
./demo/1brc/run-1brc.sh 100

# Run with 1B rows
./demo/1brc/run-1brc.sh 1000
```

## Step-by-Step

```bash
# 1. Generate test data (1M rows) + expected results
./target/release/velo-1brc generate --rows 1 --output measurements.txt --expected-output expected.csv --seed 42

# 2. Run SQL application with test harness validation
./target/release/velo-test run demo/1brc/1brc.sql --spec demo/1brc/test_spec.yaml -y
```

## Files

| File | Description |
|------|-------------|
| `1brc.sql` | SQL application — `GROUP BY station` with `EMIT FINAL` using `file_source_mmap` |
| `test_spec.yaml` | Test harness spec — validates file existence, row count (408), and correctness |
| `run-1brc.sh` | Runner script — generates data, runs SQL, validates output |

## SQL Application

```sql
CREATE STREAM results AS
SELECT
    station,
    MIN(temperature) AS min_temp,
    AVG(temperature) AS avg_temp,
    MAX(temperature) AS max_temp
FROM measurements
GROUP BY station
EMIT FINAL
WITH (
    'measurements.type' = 'file_source_mmap',
    'measurements.path' = './measurements.txt',
    'measurements.format' = 'csv',
    'measurements.delimiter' = ';',
    'results.type' = 'file_sink',
    'results.path' = './1brc_results.csv',
    'results.format' = 'csv'
);
```

Key features:
- **`file_source_mmap`** — memory-mapped file reader for zero-copy I/O
- **`EMIT FINAL`** — suppresses per-record output; emits all group results when the source is exhausted
- **`@job_mode: adaptive`** — overlaps I/O with SQL execution for ~10% throughput gain
- **`@batch_size: 10000`** — processes 10K records per batch for optimal throughput

## Performance

### Job Processor Comparison

The adaptive processor overlaps I/O reads with SQL execution, reclaiming the mmap read time that would otherwise block the pipeline. Since SQL execution dominates (~82% of wall time), the improvement is bounded by the I/O fraction.

| | Simple (V1) | Adaptive (V2) | Improvement |
|---|---|---|---|
| **1M rows** | 2.92s (343K rows/s) | 2.66s (377K rows/s) | +10% |
| **10M rows** | 31.1s (322K rows/s) | 27.8s (360K rows/s) | +11% |

The default is `@job_mode: adaptive`.

### Time Breakdown (Simple mode, 1M rows)

Simple mode reports per-phase timing, showing where time is spent:

```
SQL execution:  2.43s  (83%)  ◀ bottleneck — GROUP BY + MIN/AVG/MAX aggregation
Mmap I/O read:  0.24s  ( 8%)  ◀ overlapped by adaptive mode
Overhead:       0.25s  ( 9%)    batch orchestration, EMIT FINAL flush, CSV write
─────────────────────────────
Total:          2.92s (100%)
```

### Why Adaptive is Faster

```
Simple:    [read]──▶[sql]──▶[read]──▶[sql]──▶[read]──▶[sql]──▶...
Adaptive:  [read]──▶[sql]──▶[sql]──▶[sql]──▶[sql]──▶...
                    [read]──▶[read]──▶[read]──▶...
                    └─ overlapped ──┘
```

Adaptive pipelines the next batch read while the current batch is being processed through SQL. This hides the ~8% I/O time, yielding a ~10% wall-time improvement. The remaining ~82% SQL bottleneck is the ceiling for further gains from processor architecture alone.

### Scaling Behavior

Throughput decreases slightly at larger scales due to GROUP BY hash map growth:

| Rows | Adaptive Time | Rows/sec |
|------|---------------|----------|
| 1M   | 2.66s         | 377K     |
| 10M  | 27.8s         | 360K     |

## Test Assertions

The test spec validates three properties:

1. **`file_exists`** — output file was created with non-trivial size
2. **`file_row_count`** — exactly 408 rows (one per weather station)
3. **`file_matches`** — station names and MIN/MAX values match expected results (order-independent)

## Data Format

**Input** (`measurements.txt`):
```
station;temperature
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
```

**Output** (`1brc_results.csv`):
```
station,min_temp,avg_temp,max_temp
Hamburg,-15.3,12.1,42.7
Bulawayo,-8.2,19.4,46.1
```

## Generator Options

```
velo-1brc generate [OPTIONS]

  -r, --rows <N>               Rows in millions (default: 1)
  -o, --output <PATH>          Output file (default: measurements.txt)
  -e, --expected-output <PATH> Expected results CSV (default: expected.csv)
  -s, --seed <N>               Random seed for reproducibility
```
