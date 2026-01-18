# Velostream Performance Benchmark Results

**Last Updated**: 2026-01-18
**Mode**: Release build with `--no-default-features`

---

## Executive Summary

Velostream achieves high-throughput performance across all SQL operations, with stream-stream joins at 517K rec/sec, subqueries at 30-50M rec/sec, and WHERE clause evaluation at 90-113M evaluations/sec.

---

## Performance Dashboard

```
┌─────────────────────────────┬──────────────────┬──────────────┐
│ Feature                     │ Throughput       │ Status       │
├─────────────────────────────┼──────────────────┼──────────────┤
│ WHERE Evaluation            │ 90-113M/sec      │ ✅ Excellent │
│ IN Subquery                 │ 49.7M/sec        │ ✅ Excellent │
│ EXISTS Subquery             │ 30.1M/sec        │ ✅ Excellent │
│ HAVING Clause               │ 7.7M/sec         │ ✅ Excellent │
│ JoinStateStore              │ 5.1M/sec         │ ✅ Excellent │
│ High Cardinality Join       │ 1.9M/sec         │ ✅ Excellent │
│ CTAS Operation              │ 864K/sec         │ ✅ Good      │
│ Stream-Stream Join          │ 517K/sec         │ ✅ Good      │
│ SQL Engine (sync)           │ 376K/sec         │ ✅ Good      │
│ SQL Engine (async)          │ 235K/sec         │ ✅ Good      │
└─────────────────────────────┴──────────────────┴──────────────┘
```

---

## Stream-Stream Joins (FR-085)

**Location**: `tests/performance/analysis/sql_operations/tier3_advanced/interval_stream_join.rs`

### JoinCoordinator Performance

| Configuration | Throughput | Notes |
|---------------|------------|-------|
| Unlimited state | 517,450 rec/sec | Full join pipeline |
| Memory-bounded (5K limit) | 151,790 rec/sec | With eviction overhead |

### JoinStateStore Performance

| Operation | Throughput | Notes |
|-----------|------------|-------|
| Store + Lookup | 5,080,279 rec/sec | BTreeMap time range queries |
| With Expiration | 4,998,228 rec/sec | Watermark-driven cleanup |

### SQL Engine End-to-End

| Mode | Throughput |
|------|------------|
| Synchronous | 376,355 rec/sec |
| Asynchronous | 234,513 rec/sec |

### Stress Tests

| Test | Throughput | Notes |
|------|------------|-------|
| High Cardinality | 1,869,865 rec/sec | Unique keys (worst case) |
| Memory Bounded | 151,790 rec/sec | 100 record limit, 17K+ evictions |

---

## SQL Operations

### Subquery Performance

| Benchmark | Throughput |
|-----------|------------|
| EXISTS Subquery | 30.1M rec/sec |
| IN Subquery | 49.7M rec/sec |

### HAVING Clause

| Benchmark | Throughput |
|-----------|------------|
| HAVING Evaluation | 7.7M rec/sec |

### CTAS/CSAS Operations

| Benchmark | Throughput |
|-----------|------------|
| CTAS (CREATE TABLE AS SELECT) | 864K rec/sec |

### WHERE Clause

| Benchmark | Performance |
|-----------|-------------|
| Parsing (cached) | 0-5μs |
| Evaluation (Integer) | 11.01ns/eval |
| Evaluation (Boolean) | 8.82ns/eval |
| Evaluation (Float) | 10.19ns/eval |
| Evaluation (String) | 11.17ns/eval |
| Throughput | 90-113M eval/sec |

---

## Serialization Formats

| Format | Serialization | Deserialization | Size | Best Use Case |
|--------|--------------|-----------------|------|---------------|
| JSON | Fast | Fast | Larger | Human-readable, debugging |
| Avro | Fast | Fast | Compact | Schema evolution, compression |
| Protobuf | Fastest | Fastest | Smallest | High-throughput, microservices |

---

## Running Benchmarks

### Stream-Stream Join Benchmarks
```bash
cargo test --release --no-default-features interval_stream_join -- --nocapture
```

### SQL Benchmarks
```bash
cargo test --release --no-default-features performance::unit::comprehensive_sql_benchmarks -- --nocapture
```

### Serialization Benchmarks
```bash
cargo test --release --no-default-features performance::unit::serialization_formats -- --nocapture
```

---

## Benchmark Locations

| Category | Location |
|----------|----------|
| Stream-Stream Joins | `tests/performance/analysis/sql_operations/tier3_advanced/interval_stream_join.rs` |
| Stream-Table Joins | `tests/performance/analysis/sql_operations/tier3_advanced/stream_table_join.rs` |
| SQL Operations | `tests/performance/unit/comprehensive_sql_benchmarks.rs` |
| WHERE Clause | `tests/performance/where_clause_performance_test.rs` |
| Serialization | `tests/performance/unit/serialization_formats.rs` |

---

## Related Documentation

- [Benchmarks Guide](../benchmarks/README.md) - How to run benchmarks
- [Stream-Stream Joins Design](../design/stream-stream-joins.md) - FR-085 architecture
- [Stream-Table Join Performance](../architecture/stream-table-join-performance.md) - Hash-based joins
