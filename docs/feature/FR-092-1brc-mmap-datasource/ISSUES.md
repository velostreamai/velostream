# FR-092 — Open Issues & Follow-Up Items

## Resolved

### Stale Comment — FIXED

| File | Line | Issue | Status |
|------|------|-------|--------|
| `src/velostream/server/stream_job_server.rs` | ~924 | Comment said "e.g., file sources only support 1 partition" but file sources now return `None`. | Fixed: now reads "e.g., sources with fixed reader limits" |

### Missing Unit Tests — FIXED

| Gap | Status | Test |
|-----|--------|------|
| `group_by: Some(vec![])` | Added | `test_select_explicit_empty_group_by` in `extract_group_by_columns_test.rs` |
| `CreateStream` with non-Select inner | Added | `test_create_stream_with_non_select_inner` in `extract_group_by_columns_test.rs` |
| `KafkaDataSource::partition_count()` | Added | `test_kafka_data_source_partition_count_is_none` in `partition_count_test.rs` |
| All sources agree on `None` | Added | `test_all_sources_agree_on_none` in `partition_count_test.rs` |

## Open

### Design Inconsistency — Dual GROUP BY Column Mechanisms

The coordinator has **two parallel mechanisms** for supplying GROUP BY columns to the routing logic:

1. **Struct field + builder** (test-facing API):
   - `self.group_by_columns: Vec<String>` field on `AdaptiveJobProcessor` (coordinator.rs:262)
   - Set via `with_group_by_columns()` builder method (coordinator.rs:365)
   - Used by `process_batch_with_strategy()` (coordinator.rs:563, 583)
   - Used by `process_batch_with_strategy_and_throttling()` (coordinator.rs:862, 883)
   - Called from: `strategy_integration_test.rs`, `phase6_2_validation.rs`, `adaptive_job_processor_integration_test.rs`

2. **Parameter-based extraction** (production path):
   - `extract_group_by_columns(&query)` called at the start of `process_job` / `process_multi_job` (job_processor_v2.rs:155, 376)
   - Passed as `&[String]` parameter to `process_batch_for_receivers()` (coordinator.rs:1483)
   - Also extracted in `process_multi_job()` on the coordinator (coordinator.rs:952) and threaded to `partition_pipeline()` (coordinator.rs:1033→1112→1146)

**Risk:** If someone adds a new production code path using `process_batch_with_strategy()` without calling `with_group_by_columns()`, routing will silently fall back to round-robin (empty columns = no hash key).

**Recommendation:** Consider deprecating the struct field in favour of always extracting from the query, or unifying the two APIs behind a single routing entry point.

### Missing Integration/E2E Tests

| Gap | Severity | Description |
|-----|----------|-------------|
| Annotation passthrough chain | Medium | No test verifying `@partitioning_strategy: hash` in SQL flows through `SqlAnnotationParser` → `create_processor_for_job` → `JobProcessorFactory` and results in `AlwaysHash` strategy. Currently verified only by manual observation (1BRC demo output). |
| `process_batch_for_receivers` with GROUP BY | Medium | No test for the new `group_by_columns` parameter on `process_batch_for_receivers`. Existing tests use the older `process_batch_with_strategy` API which reads from the struct field. |
| `apply_source_partition_limit` passthrough | Low | When all sources return `None`, the Adaptive config should pass through unchanged — no test. |

## Summary

| Category | Count | Status |
|----------|-------|--------|
| Stale comments | 1 | Fixed |
| Missing unit tests | 4 | Fixed (4 new tests added) |
| Design inconsistency | 1 | Open (test-only API, no production risk) |
| Missing integration tests | 3 | Open |
