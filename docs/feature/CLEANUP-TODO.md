# Codebase Cleanup — TODO

## Completed

- [x] `src/main.rs` — deleted (redundant Kafka producer example; lib.rs is the proper entry point)
- [x] `bincode` dependency — removed from `Cargo.toml` (unused in entire `src/`)
- [x] `type_validator.rs` — deleted (`TypeValidator`, `TypeCategory` never used; removed from `validation/mod.rs`)
- [x] `providers.rs` — removed unused `JsonSchemaProvider` struct, `sample_size` fields from `FileSchemaProvider` and `S3SchemaProvider`
- [x] `circuit_breaker.rs` — audited; all items used in `phase_2_error_resource_test.rs` (false positives from cross-directory test analysis)
- [x] 4 disabled file datasource tests — deleted then **recreated** (see below)
- [x] Stale commented-out test references — removed from `tests/unit/server/mod.rs` and `tests/unit/sql/execution/mod.rs`

### File Datasource Test Recreation & Bug Fixes

The 4 deleted test files were originally disabled (commented out in `mod.rs`) with 22+ compilation errors against removed APIs. After analysis showed critical coverage gaps (FileReader batch path ~5%, FileDataSourceError 0%), all 4 were **rewritten from scratch** against current APIs:

| Test File | Tests | Coverage Target |
|-----------|-------|-----------------|
| `file_config_test.rs` | 25 | FileFormat parsing/display, FileSourceConfig defaults, builder, 6 validation edge cases, 10 URI parsing scenarios, SourceConfig conversion/roundtrip |
| `file_datasource_test.rs` | 20 | FileDataSource creation/init/properties/metadata, FileReader CSV/JSONL/delimiter/no-header/empty/skip/has_more/max_records/batch, type inference, error handling, schema inference |
| `file_error_test.rs` | 8 | Display for all 9 variants, Debug, Error trait, From conversions, variant coverage, error chains |
| `file_watcher_test.rs` | 17 | Creation/default, watch file/dir/glob/nonexistent, modification/new-file detection, polling, rate limiting, timeout, stop/drop, multi-file |

**Total: 70 new tests** (plus 6 existing inline tests in `config.rs` and `watcher.rs` = 76 file datasource tests).

#### Bugs Found & Fixed in `reader.rs` Batch Path

The new tests caught **4 bugs** in `parse_csv_line_batch()` / `read_lines_batch()`:

1. **Hard-coded comma delimiter** — `line.split(',')` ignored `self.config.csv_delimiter`, breaking custom delimiters (`;`, `|`, etc.) in batch mode. Fixed: use `line.split(self.config.csv_delimiter)`.

2. **Inconsistent no-header field naming** — Batch path generated `field_0`, `field_1`... while single-record path and mmap reader generated `column_0`, `column_1`... Fixed: changed both locations in `parse_csv_line_batch()` to `column_N`.

3. **Data loss at batch boundaries** — `lines_buffer.drain(..)` consumed ALL buffered lines even when `break` on `batch_size` stopped processing early. Lines exceeding batch size were silently dropped. Also `lines_buffer` was function-local, so unprocessed lines were lost between `read()` calls. Fixed:
   - Added `pending_lines: Vec<String>` field to `FileReader` struct
   - Start each `read_lines_batch()` with `std::mem::take(&mut self.pending_lines)`
   - Changed `drain(..)` to indexed iteration with `drain(..processed_count)`
   - Save unprocessed lines back to `self.pending_lines`
   - Added `pending_lines` check in `has_more()`

4. **max_records not enforced in batch mode** — `max_records` was only checked at the outer loop top, allowing a full batch to be processed before the limit was applied. Fixed: added max_records check inside the line processing loop.

**Files modified:**
- `src/velostream/datasource/file/reader.rs` — 4 bug fixes in batch processing path
- `tests/unit/datasource/file/mod.rs` — registered 4 new test modules
- `tests/unit/datasource/file/file_config_test.rs` — NEW (25 tests)
- `tests/unit/datasource/file/file_datasource_test.rs` — NEW (20 tests)
- `tests/unit/datasource/file/file_error_test.rs` — NEW (8 tests)
- `tests/unit/datasource/file/file_watcher_test.rs` — NEW (17 tests)

- [x] `velo-test.rs:1904` `was_interrupted` — turned out to be a real bug: in `data_only` mode, `was_interrupted` was set but never checked, so Ctrl+C printed a success summary instead of cleaning up. Fixed by adding interruption handling before the summary (clean up containers, `exit(130)`)
- [x] `FileMmapDataSource` / `FileMmapReader` test coverage — brought to parity with standard `file_source` (see below)

### Mmap Datasource Test Parity

Coverage was at ~19% (10 reader tests, 0 data source tests). Added 33 new tests to reach parity with the standard `FileDataSource` / `FileReader` test suite:

| Test File | Tests | Coverage Target |
|-----------|-------|-----------------|
| `data_source_mmap_test.rs` | 18 | **NEW** — creation, default, partition_count, from_properties (basic/source-prefix/defaults), initialize (success/error), metadata (before/after init), schema inference (CSV/not-initialized), create_reader (success/error/with-batch-config), to_source_config (no-init/CSV/JSONL) |
| `reader_mmap_test.rs` | 25 | Was 10 — added 15: JSONL reading, JSONL with null, skip_lines, type inference (integer/float/boolean as separate tests), commit no-op, error handling (file-not-found/invalid-JSONL/unsupported-JSON-array), CSV edge cases (quoted fields/escaped quotes/header-only) |

**Total mmap tests: 43** (was 10). No bugs found — mmap reader implementation was correct.

**Files added/modified:**
- `tests/unit/datasource/file/data_source_mmap_test.rs` — NEW (18 tests)
- `tests/unit/datasource/file/reader_mmap_test.rs` — enhanced (10 → 25 tests)
- `tests/unit/datasource/file/mod.rs` — registered `data_source_mmap_test`

- [x] `once_cell` dependency removed — replaced with `std::sync::OnceLock` (stdlib since Rust 1.70)
- [x] All `src/` clippy warnings eliminated (see below)
- [x] Parser refactoring — `parser.rs` (4.6K) modularized into directory structure (see below)

### Parser Refactoring (`src/velostream/sql/parser/`)

Converted monolithic `parser.rs` (4.6K lines) into organized directory structure:

| Module | Purpose |
|--------|---------|
| `parser/core.rs` | Main parser implementation (4.6K lines) |
| `parser/lexer.rs` | Tokenization logic |
| `parser/mod.rs` | Module coordination and re-exports |
| `parser/annotations.rs` | SQL annotation parsing (existing) |
| `parser/validator.rs` | Aggregate validation (existing) |

**Benefits:**
- Better code organization and navigation
- Clearer separation of concerns (lexer vs parser)
- Foundation for future fine-grained splits (select.rs, ddl.rs, clauses.rs)
- Maintains all functionality and tests (977 passing)

**Files changed:**
- Deleted: `src/velostream/sql/parser.rs`
- Created: `parser/core.rs`, `parser/lexer.rs`, `parser/mod.rs`

### `once_cell` → `std::sync::OnceLock` Migration

Only 1 file used `once_cell`: `tests/performance/common/shared_container.rs`. Replaced `once_cell::sync::OnceCell` with `std::sync::OnceLock` (compatible API). Removed `once_cell = "1.19"` from `Cargo.toml`.

### src/ Clippy Warnings — All 29 Fixed (0 remaining)

Eliminated all clippy warnings in `src/` and `src/bin/` — only `tests/` warnings remain (~1,354).

| File | Warning | Fix |
|------|---------|-----|
| `kafka/utils.rs` | `match_overlapping_arm` — overlapping ranges in idle sleep match | Converted to if/else chain |
| `kafka/kafka_fast_consumer.rs` | `items_after_test_module` — `Drop` impl after `#[cfg(test)]` | Moved `Drop` impl before test module |
| `server/processors/metrics_helper.rs` | `type_complexity` — complex inline HashMap type | Added `type GaugeDedupMap` alias |
| `server/processors/metrics_helper.rs` (×3) | `needless_borrow` — `&effective_label_names` already a reference | Removed redundant `&` |
| `server/v2/join_job_processor.rs` | `await_holding_lock` — `MutexGuard` held across `.await` | Scoped guard in block before await |
| `sql/execution/aggregation/compute.rs` | `manual_is_multiple_of` — `len % 2 == 0` | Used `len.is_multiple_of(2)` |
| `sql/execution/processors/context.rs` | `type_complexity` — complex tuple type in HashMap | Added `pub type RowsWindowConfig` alias |
| `sql/execution/processors/select.rs` | match reimplements `unwrap_or` | Used `.unwrap_or(true)` |
| `sql/execution/window_v2/adapter.rs` | unnecessary closure in `or_else` | Used `.or(Ok(FieldValue::Null))` |
| `test_harness/assertions.rs` | `useless_conversion` — `.into_iter()` on iterator | Removed `.into_iter()` |
| `test_harness/generator.rs` | `manual_range_contains` | Used `(10.0..=100.0).contains(&float_value)` |
| `test_harness/spec.rs` (×2) | `field_reassign_with_default` | Used struct init with `..Default::default()` |
| `bin/velo-test.rs` | `nonminimal_bool` — `!x.is_some()` | Used `x.is_none()` |
| `bin/velo-test.rs` (×4) | `collapsible_if` — nested if statements | Collapsed into let-chains with `&&` |
| `bin/velo-test.rs` | `field_reassign_with_default` — `HealthConfig` | Used struct init with `..Default::default()` |
| `bin/velo-test.rs` | `was_interrupted` unused assignment (real bug) | Added interrupt handling in data_only path |

- [x] Test code warnings — bulk cleanup of top 4 categories (see below)

### Test Code Warning Cleanup (1,354 → 775)

Used `cargo clippy --message-format=json` with a Python script to parse machine-applicable suggestions and apply them by byte offset. Skipped 45 files where fixes broke closure/macro contexts.

| Category | Before | After | Fixed |
|----------|--------|-------|-------|
| `unused_mut` | 199 | 43 | 156 |
| `unused_imports` | 226 | 43 | 183 |
| `unused_variables` | 181 | 42 | 139 |
| `clippy::useless_vec` | 108 | 6 | 102 |
| **Total (4 categories)** | **714** | **134** | **580** |
| **All warnings** | **1,354** | **775** | **579** |

172 files changed, -188 net lines. Remaining 134 in these categories are in ~45 files with macro/closure contexts where auto-fix produces invalid syntax.

## Medium Priority (future work)

- [ ] ~775 clippy warnings in test code — remaining: `dead_code` (83), `collapsible_if` (68), `needless_borrow` (45), residual `unused_*` in macro contexts (128)

## Low Priority (long-term)

- [ ] Large files — `assertions.rs` (5.7K lines), `velo-test.rs` (5.2K)
- [ ] Further parser splits — `parser/core.rs` (4.6K) into select.rs, ddl.rs, clauses.rs, etc.
- [ ] 27 files with TODO/FIXME — audit and ticket
- [ ] ~141 files in `src/` with wildcard imports (`use ..::*`)
- [ ] Duplicate transitive deps (axum 0.6/0.8, base64 0.21/0.22) — blocked on upstream
