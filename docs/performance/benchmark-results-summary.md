# Velostream Performance Benchmark Results

**Last Updated**: 2026-01-18
**Mode**: Release build with `--no-default-features`

---

## Executive Summary

Velostream achieves high-throughput performance across all SQL operations, with stream-stream joins at 624K rec/sec, state store operations at 4.8-5.4M rec/sec, and SQL engine end-to-end at 256-390K rec/sec.

---

## Performance Dashboard

```
┌─────────────────────────────┬──────────────────┬──────────────┐
│ Feature                     │ Throughput       │ Status       │
├─────────────────────────────┼──────────────────┼──────────────┤
│ StateStore (with expiry)    │ 5.4M rec/sec     │ ✅ Excellent │
│ StateStore (store+lookup)   │ 4.8M rec/sec     │ ✅ Excellent │
│ High Cardinality Join       │ 1.8M rec/sec     │ ✅ Excellent │
│ JoinCoordinator (unlimited) │ 624K rec/sec     │ ✅ Excellent │
│ SQL Engine (sync)           │ 390K rec/sec     │ ✅ Good      │
│ SQL Engine (async)          │ 256K rec/sec     │ ✅ Good      │
│ JoinCoordinator (bounded)   │ 150K rec/sec     │ ✅ Good      │
└─────────────────────────────┴──────────────────┴──────────────┘
```

---

## Stream-Stream Joins (FR-085)

**Location**: `tests/performance/analysis/sql_operations/tier3_advanced/interval_stream_join.rs`
**Test Date**: 2026-01-18

### JoinCoordinator Performance

| Configuration | Throughput | Records | Matches | Duration |
|---------------|------------|---------|---------|----------|
| Unlimited state | 624,107 rec/sec | 18,000 | 8,000 | 28.84ms |
| Memory-bounded (5K limit) | 149,570 rec/sec | 18,000 | 4,024 | 120.35ms |

### JoinStateStore Performance

| Operation | Throughput | Records | Matches | Duration |
|-----------|------------|---------|---------|----------|
| Store + Lookup | 4,835,152 rec/sec | 20,000 | 10,000 | 4.14ms |
| With Expiration | 5,431,341 rec/sec | 10,000 | 9,799 | 1.84ms |

### SQL Engine End-to-End

| Mode | Throughput | Records | Duration |
|------|------------|---------|----------|
| Synchronous | 389,823 rec/sec | 18,000 | 46.17ms |
| Asynchronous | 255,920 rec/sec | 18,000 | 70.33ms |

### Stress Tests

| Test | Throughput | Records | Matches | Notes |
|------|------------|---------|---------|-------|
| High Cardinality | 1,785,841 rec/sec | 20,000 | 51 | Unique keys (worst case) |
| Memory Bounded | 1,130,000 rec/sec | 18,000 | 79 | 100 record limit, 17.8K evictions |

---

## Running Benchmarks

### Stream-Stream Join Benchmarks
```bash
# Full benchmark suite
cargo test --release --no-default-features interval_stream_join -- --nocapture

# Individual tests
cargo test --release --no-default-features test_interval_stream_join_performance -- --nocapture
cargo test --release --no-default-features test_interval_join_high_cardinality -- --nocapture
cargo test --release --no-default-features test_interval_join_memory_bounded -- --nocapture
```

---

## Benchmark Locations

| Category | Location |
|----------|----------|
| Stream-Stream Joins | `tests/performance/analysis/sql_operations/tier3_advanced/interval_stream_join.rs` |
| Stream-Table Joins | `tests/performance/analysis/sql_operations/tier3_advanced/stream_table_join.rs` |

---

## Related Documentation

- [Benchmarks Guide](../benchmarks/README.md) - How to run benchmarks
- [Stream-Stream Joins Design](../design/stream-stream-joins.md) - FR-085 architecture
- [Stream-Table Join Performance](../architecture/stream-table-join-performance.md) - Hash-based joins
