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
cd demo/1brc

# Run with validation (default: 1M rows)
./run-1brc.sh

# Run with 100M rows
./run-1brc.sh 100

# Run with 1B rows
./run-1brc.sh 1000
```

The script automatically finds `velo-1brc` and `velo-test` binaries from `target/release/` or your PATH.

> **From source?** Build first: `cargo build --release --bin velo-1brc --bin velo-test --no-default-features`

## Step-by-Step

```bash
# 1. Generate test data (1M rows) + expected results
velo-1brc generate --rows 1 --output measurements.txt --expected-output expected.csv --seed 42

# 2. Run SQL application with test harness validation
velo-test run demo/1brc/1brc.sql --spec demo/1brc/test_spec.yaml -y
```

## Files

| File | Description |
|------|-------------|
| `1brc.sql` | SQL application — `GROUP BY station` with `EMIT FINAL` using `file_source_mmap` |
| `test_spec.yaml` | Test harness spec — validates file existence, row count (408), and correctness |
| `run-1brc.sh` | Runner script — generates data, runs SQL, validates output |

## SQL Application

```sql
-- @job_mode: adaptive
-- @batch_size: 10000
-- @num_partitions: 6
-- @partitioning_strategy: hash

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
- **`@num_partitions: 6`** — distributes GROUP BY work across 6 parallel workers
- **`@partitioning_strategy: hash`** — FNV-1a hash on GROUP BY columns routes each station to a dedicated partition

## Performance

### Partition Parallelism (Adaptive + Hash Partitioning)

The adaptive processor can distribute GROUP BY aggregation across multiple partitions. Each record is hash-routed by its GROUP BY key (station name) to one of N partition workers, each running an independent SQL execution engine. This parallelizes the SQL bottleneck that dominates wall time.

| | Adaptive (1 partition) | Adaptive (6 partitions) | Speedup |
|---|---|---|---|
| **1M rows** | 2.66s (377K rows/s) | 0.68s (1.47M rows/s) | **3.9x** |

```
Single partition:    [read] ──▶ [sql engine] ──▶ [output]
                                    ▲
                              all 408 stations

Hash partitioned:    [read] ──▶ [hash route] ──┬──▶ [sql engine 0] ──▶ [merge output]
                                               ├──▶ [sql engine 1] ──▶
                                               ├──▶ [sql engine 2] ──▶
                                               ├──▶ [sql engine 3] ──▶
                                               ├──▶ [sql engine 4] ──▶
                                               └──▶ [sql engine 5] ──▶
                                                    ~68 stations each
```

Each partition worker processes ~68 stations (408 / 6), reducing per-worker hash map size and enabling parallel CPU utilization. The hash routing overhead is negligible compared to the SQL execution savings.

### Job Processor Comparison

The adaptive processor overlaps I/O reads with SQL execution, reclaiming the mmap read time that would otherwise block the pipeline. Since SQL execution dominates (~82% of wall time), the improvement is bounded by the I/O fraction.

| | Simple (V1) | Adaptive (1 part.) | Adaptive (6 part.) | Total Speedup |
|---|---|---|---|---|
| **1M rows** | 2.92s (343K rows/s) | 2.66s (377K rows/s) | 0.68s (1.47M rows/s) | **4.3x** |
| **10M rows** | 31.1s (322K rows/s) | 27.8s (360K rows/s) | — | — |

### Time Breakdown (Simple mode, 1M rows)

Simple mode reports per-phase timing, showing where time is spent:

```
SQL execution:  2.43s  (83%)  ◀ bottleneck — parallelized by hash partitioning
Mmap I/O read:  0.24s  ( 8%)  ◀ overlapped by adaptive mode
Overhead:       0.25s  ( 9%)    batch orchestration, EMIT FINAL flush, CSV write
─────────────────────────────
Total:          2.92s (100%)
```

### Why Adaptive + Partitioning is Fastest

```
Simple:          [read]──▶[sql]──▶[read]──▶[sql]──▶...         2.92s

Adaptive:        [read]──▶[sql]──▶[sql]──▶[sql]──▶...          2.66s  (+10%)
                          [read]──▶[read]──▶...
                          └─ I/O overlapped ──┘

Adaptive+Hash:   [read]──▶[hash]──▶[sql₀]──▶...               0.68s  (3.9x)
                          [read]   [sql₁]──▶...
                                   [sql₂]──▶...
                                   [sql₃]──▶...
                                   [sql₄]──▶...
                                   [sql₅]──▶...
                                   └─ parallel SQL ──┘
```

1. **Adaptive mode** overlaps I/O with SQL, reclaiming the ~8% I/O time
2. **Hash partitioning** distributes the ~83% SQL bottleneck across N workers
3. Combined effect: **4.3x** speedup over simple single-threaded mode

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
