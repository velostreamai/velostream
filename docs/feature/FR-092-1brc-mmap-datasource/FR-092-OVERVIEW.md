# FR-092: 1BRC Demo with `file_source_mmap` Datasource

## Status

| Field         | Value                                    |
|---------------|------------------------------------------|
| **FR**        | FR-092                                   |
| **Title**     | 1BRC Demo with `file_source_mmap` Datasource |
| **Status**    | In Progress                              |
| **Branch**    | `feature/FR-092-1brc-mmap-datasource`    |
| **Created**   | 2026-02-08                               |

## Summary

Implement the One Billion Row Challenge (1BRC) as a Velostream demo, showcasing the SQL engine processing 1B rows of weather station data to compute MIN/AVG/MAX per station. This FR introduces a new `file_source_mmap` datasource that uses memory-mapped file I/O for high-throughput batch file processing.

## Motivation

The 1BRC is a well-known benchmark for data processing systems. Building a Velostream demo for it:

1. **Demonstrates real SQL capability** — the aggregation runs through Velostream's full SQL engine
2. **Introduces mmap-based file reading** — zero-copy reads for maximum throughput on large files
3. **Provides a compelling benchmark** — measurable, comparable performance on a standard workload

## Architecture

### Component Overview

```
                    ┌──────────────────┐
                    │   velo-1brc      │   (data generator only)
                    │   generate cmd   │
                    └────────┬─────────┘
                             │ writes
                             ▼
                    ┌──────────────────┐
                    │ measurements.txt │   (station;temperature rows)
                    └────────┬─────────┘
                             │ read via mmap
                             ▼
                    ┌──────────────────┐
                    │ file_source_mmap │   (new datasource type)
                    │  FileMmapReader  │
                    └────────┬─────────┘
                             │ records
                             ▼
                    ┌──────────────────┐
                    │  SQL Engine      │   (GROUP BY + MIN/AVG/MAX)
                    │  1brc.sql        │
                    └────────┬─────────┘
                             │ results
                             ▼
                    ┌──────────────────┐
                    │  file_sink       │   (1brc_results.csv)
                    └──────────────────┘
```

### `velo-1brc` Binary

**Generator only.** Produces `measurements.txt` with rows of the form `station;temperature`. The existing `Generate` subcommand and `STATIONS` data are retained; the `Run` subcommand and all in-binary processing logic are removed.

### `file_source_mmap` Datasource

A new datasource type parallel to the existing `file_source`, using `memmap2::Mmap` for zero-copy file access.

**Key differences from `file_source`:**

| Aspect             | `file_source`              | `file_source_mmap`           |
|--------------------|----------------------------|------------------------------|
| I/O method         | `BufReader::read_line()`   | `mmap` byte scanning         |
| Copy behavior      | Line copied into `String`  | Zero-copy `&[u8]` slices     |
| Seek support       | Line-by-line only          | Direct byte offset           |
| Streaming support  | Yes                        | No (batch only)              |
| Best for           | Moderate files, streaming  | Large files, batch workloads |

**Reading strategy:**
1. Memory-map the file via `memmap2::Mmap`
2. Scan `mmap[position..]` for `\n` bytes
3. Parse each line via `std::str::from_utf8()` (zero-copy from mmap)
4. Reuse existing CSV parsing logic (`parse_csv_fields`, type inference)
5. Collect records into batches using `FixedSize` batch strategy

### SQL Application (`demo/1brc/1brc.sql`)

```sql
CREATE STREAM results AS
SELECT
    station,
    MIN(temperature) AS min_temp,
    AVG(temperature) AS avg_temp,
    MAX(temperature) AS max_temp
FROM measurements
GROUP BY station
EMIT CHANGES
WITH (
    'measurements.type' = 'file_source_mmap',
    'measurements.path' = './measurements.txt',
    'measurements.format' = 'csv_no_header',
    'measurements.delimiter' = ';',
    'results.type' = 'file_sink',
    'results.path' = './1brc_results.csv',
    'results.format' = 'csv'
);
```

## Integration Points

### Datasource Resolution Chain

Three files need updates to wire `file_source_mmap` into the engine:

1. **`query_analyzer.rs`** — Register `FileMmapDataSource` schema; add `"file_source_mmap"` match arm
2. **`processors/common.rs`** — Route `file_source_mmap` type to `FileMmapReader` creation
3. **`stream_job_server.rs`** — Include `file_source_mmap` in file table auto-loading detection

### New Files

| File | Purpose |
|------|---------|
| `src/velostream/datasource/file/reader_mmap.rs` | `FileMmapReader` implementing `DataReader` |
| `src/velostream/datasource/file/data_source_mmap.rs` | `FileMmapDataSource` implementing `DataSource` + `ConfigSchemaProvider` |
| `demo/1brc/1brc.sql` | SQL application file |
| `demo/1brc/run-1brc.sh` | Runner script |
| `tests/unit/datasource/file/reader_mmap_test.rs` | Unit tests for mmap reader |

### Modified Files

| File | Change |
|------|--------|
| `src/bin/velo-1brc.rs` | Remove `Run` subcommand — generator only |
| `src/velostream/datasource/file/mod.rs` | Add module declarations and re-exports |
| `src/velostream/sql/query_analyzer.rs` | Register schema, add match arm |
| `src/velostream/server/processors/common.rs` | Route mmap type to reader |
| `src/velostream/server/stream_job_server.rs` | Detect mmap tables for auto-loading |
| `tests/unit/datasource/file/mod.rs` | Register test module |

## Test Plan

### Unit Tests (`tests/unit/datasource/file/reader_mmap_test.rs`)

| Test | Description |
|------|-------------|
| `test_mmap_reader_csv_records` | Reads CSV records correctly |
| `test_mmap_reader_semicolon_delimiter` | Semicolon delimiter parsing (1BRC format) |
| `test_mmap_reader_has_more` | Returns false after all records consumed |
| `test_mmap_reader_empty_file` | Handles empty files gracefully |
| `test_mmap_reader_type_inference` | station=String, temperature=Float inference |
| `test_mmap_reader_batch_reading` | Multiple records per `read()` call |
| `test_mmap_reader_seek` | Byte offset seek support |

### Integration / E2E

```bash
# Generate test data
./target/release/velo-1brc generate --rows 1 --output measurements.txt

# Run through SQL engine
./target/release/velo-sql deploy-app --file demo/1brc/1brc.sql
```

## Verification Checklist

```bash
# Pre-commit checks
cargo fmt --all -- --check
cargo check --all-targets --no-default-features
cargo clippy --all-targets --no-default-features

# Unit tests
cargo test reader_mmap_test --no-default-features -- --nocapture

# Build release binaries
cargo build --release --bin velo-1brc --bin velo-sql --no-default-features

# Full pre-commit suite
cargo test --tests --no-default-features -- --skip integration:: --skip performance::
cargo build --examples --no-default-features
cargo build --bins --no-default-features
```
