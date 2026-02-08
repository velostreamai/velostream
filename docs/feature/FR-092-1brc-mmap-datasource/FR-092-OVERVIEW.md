# FR-092: 1BRC Demo with `file_source_mmap` Datasource + EMIT FINAL

## Status

| Field         | Value                                    |
|---------------|------------------------------------------|
| **FR**        | FR-092                                   |
| **Title**     | 1BRC Demo with `file_source_mmap` + EMIT FINAL |
| **Status**    | Complete                                 |
| **Branch**    | `feature/FR-092-1brc-mmap-datasource`    |
| **Created**   | 2026-02-08                               |

## Summary

Implements the One Billion Row Challenge (1BRC) as a Velostream demo, introducing two new capabilities:

1. **`file_source_mmap` datasource** — memory-mapped file reader for zero-copy batch processing
2. **`EMIT FINAL` without WINDOW** — deferred output for GROUP BY on bounded sources

Together these enable a 1BRC pipeline that reads 1B rows via mmap, accumulates MIN/AVG/MAX per station, and emits ~400 final result rows when the source is exhausted.

## Architecture

```
measurements.txt ──▶ file_source_mmap ──▶ SQL Engine (GROUP BY + EMIT FINAL) ──▶ file_sink ──▶ 1brc_results.csv
                     (mmap reader)         MIN/AVG/MAX per station                             (CSV output)
```

### Components

| Component | Description |
|-----------|-------------|
| `velo-1brc generate` | Data generator — produces `measurements.txt` and `expected.csv` |
| `file_source_mmap` | Memory-mapped file reader implementing `DataReader` |
| `EMIT FINAL` | Suppresses per-record output; flushes all group results on source exhaustion |
| `1brc.sql` | SQL application wiring it all together |
| `velo-test` | Test harness validating output against expected results |

---

## Part 1: `file_source_mmap` Datasource

A new datasource type parallel to the existing `file_source`, using `memmap2::Mmap` for zero-copy file access.

### Comparison with `file_source`

| Aspect             | `file_source`              | `file_source_mmap`           |
|--------------------|----------------------------|------------------------------|
| I/O method         | `BufReader::read_line()`   | `mmap` byte scanning         |
| Copy behavior      | Line copied into `String`  | Zero-copy `&[u8]` slices     |
| Seek support       | Line-by-line only          | Direct byte offset           |
| Streaming support  | Yes                        | No (batch only)              |
| Best for           | Moderate files, streaming  | Large files, batch workloads |

### Reading Strategy

1. Memory-map the file via `memmap2::Mmap`
2. Scan `mmap[position..]` for `\n` bytes
3. Parse each line via `std::str::from_utf8()` (zero-copy from mmap)
4. Reuse existing CSV parsing logic (`parse_csv_fields`, type inference)
5. Collect records into batches using `FixedSize` batch strategy

### New Files

| File | Purpose |
|------|---------|
| `src/velostream/datasource/file/reader_mmap.rs` | `FileMmapReader` implementing `DataReader` |
| `src/velostream/datasource/file/data_source_mmap.rs` | `FileMmapDataSource` implementing `DataSource` + `ConfigSchemaProvider` |

### Integration Points

| File | Change |
|------|--------|
| `src/velostream/datasource/file/mod.rs` | Module declarations and re-exports |
| `src/velostream/sql/query_analyzer.rs` | Register schema, add `"file_source_mmap"` match arm |
| `src/velostream/server/processors/common.rs` | Route `file_source_mmap` to mmap reader creation |
| `src/velostream/server/stream_job_server.rs` | Include in file table auto-loading detection |

---

## Part 2: EMIT FINAL without WINDOW

### Motivation

`EMIT CHANGES` produces one output row per input record (1M rows in = 1M rows out). For batch workloads on bounded sources, the natural semantic is: accumulate all data, emit final aggregation results when the source is exhausted (~400 rows out).

### Semantics

**EMIT FINAL emits on source exhaustion** — when `DataReader::has_more()` returns `false`.

| Source Type | Exhaustion Signal | EMIT FINAL without WINDOW |
|-------------|-------------------|---------------------------|
| `file_source` | EOF reached | Works naturally |
| `file_source_mmap` | All bytes consumed | Works naturally |
| `kafka_source` | Never (polls indefinitely) | Accumulates forever — validator warns |

### Data Flow

```
Record 1 → GROUP BY accumulate → suppress output (return None)
Record 2 → GROUP BY accumulate → suppress output (return None)
...
Record N → GROUP BY accumulate → suppress output (return None)
EOF → flush_final_aggregations() → emit one record per group → write to sink → exit
```

### Implementation

| File | Change |
|------|--------|
| `src/velostream/sql/ast.rs` | Updated `EmitMode::Final` doc comment |
| `src/velostream/sql/execution/processors/select.rs` | Removed validation gate rejecting EMIT FINAL without WINDOW |
| `src/velostream/sql/validator.rs` | Source-aware warnings (bounded vs unbounded) |
| `src/velostream/sql/execution/engine.rs` | Added `flush_final_aggregations()` method |
| `src/velostream/server/processors/common.rs` | Shared `flush_final_aggregations_to_sinks()` helper |
| `src/velostream/server/processors/simple.rs` | Call flush on source exhaustion, track `total_processing_time` |
| `src/velostream/server/processors/transactional.rs` | Same as simple |
| `src/velostream/server/v2/partition_receiver.rs` | Call flush on source exhaustion (V2 processor) |
| `src/velostream/server/v2/job_processor_v2.rs` | Call flush on source exhaustion (V2 processor) |

### Safety

`flush_final_aggregations()` is safe to call on any query — returns empty `Vec` unless the query has `EMIT FINAL` + `GROUP BY` + no `WINDOW`.

---

## Demo Files

| File | Description |
|------|-------------|
| `demo/1brc/1brc.sql` | SQL application |
| `demo/1brc/test_spec.yaml` | Test harness spec (file_exists, file_row_count, file_matches) |
| `demo/1brc/run-1brc.sh` | Runner script (generate + test) |
| `demo/1brc/README.md` | User-facing documentation |

## Test Files

| File | Description |
|------|-------------|
| `tests/unit/datasource/file/reader_mmap_test.rs` | 10 tests for mmap reader |
| `tests/unit/sql/execution/processors/emit_final_flush_test.rs` | 7 tests for EMIT FINAL flush |

## Verification

```bash
# Generate data + run with validation
./target/release/velo-1brc generate --rows 1 --output measurements.txt --expected-output expected.csv --seed 42
./target/release/velo-test run demo/1brc/1brc.sql --spec demo/1brc/test_spec.yaml -y

# Pre-commit checks
cargo fmt --all -- --check
cargo check --all-targets --no-default-features
cargo clippy --all-targets --no-default-features
cargo test --tests --no-default-features -- --skip integration:: --skip performance::
```
