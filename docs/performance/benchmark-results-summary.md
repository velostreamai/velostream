# Velostream Performance Benchmark Results

**Last Updated**: 2026-01-18
**Mode**: Release build with `--no-default-features`

---

## Executive Summary

Comprehensive benchmark results across all SQL operations. Throughput measured in records/sec.

---

## Performance Dashboard

```
┌─────────────────────────────┬──────────────┬──────────────┬──────────────┐
│ Operation                   │ SQL Sync     │ SQL Async    │ Tier         │
├─────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ ANY/ALL Operators           │ 1,344K       │ 742K         │ 4            │
│ IN Subquery                 │ 1,217K       │ 865K         │ 3            │
│ EXISTS Subquery             │ 1,079K       │ 546K         │ 3            │
│ ROWS Window                 │ 865K         │ 380K         │ 1            │
│ SELECT WHERE                │ 767K         │ 354K         │ 1            │
│ Correlated Subquery         │ 538K         │ 547K         │ 3            │
│ Scalar Subquery             │ 468K         │ 266K         │ 2            │
│ Stream-Table Join (tier3)   │ 463K         │ 460K         │ 3            │
│ Time-based Join             │ 435K         │ 436K         │ 2            │
│ Interval Stream Join        │ 364K         │ 228K         │ 3            │
│ HAVING Clause               │ 331K         │ 231K         │ 2            │
│ Stream-Table Join (tier1)   │ 280K         │ 188K         │ 1            │
│ Scalar Subquery w/EXISTS    │ 239K         │ 248K         │ 2            │
│ Tumbling Window             │ 231K         │ 180K         │ 1            │
│ GROUP BY Continuous         │ 218K         │ 159K         │ 1            │
│ EMIT CHANGES                │ 9K           │ 7K           │ 1            │
└─────────────────────────────┴──────────────┴──────────────┴──────────────┘
```

---

## Tier 1: Essential Operations

| Operation | SQL Sync | SQL Async | SimpleJp | AdaptiveJp (4c) |
|-----------|----------|-----------|----------|-----------------|
| ROWS Window | 865,248 | 379,944 | 620,153 | 642,853 |
| SELECT WHERE | 766,923 | 353,854 | 523,756 | 519,559 |
| Stream-Table Join | 279,744 | 187,915 | 234,816 | 233,076 |
| Tumbling Window | 230,687 | 179,981 | 742,643 | 681,991 |
| GROUP BY Continuous | 218,470 | 158,876 | 242,226 | 242,629 |
| EMIT CHANGES | 9,390 | 6,684 | 28,874 | 28,809 |

---

## Tier 2: Common Operations

| Operation | SQL Sync | SQL Async | SimpleJp | AdaptiveJp (4c) |
|-----------|----------|-----------|----------|-----------------|
| Scalar Subquery | 467,851 | 265,556 | 351,420 | 357,744 |
| Time-based Join | 435,010 | 436,315 | 356,526 | 364,544 |
| HAVING Clause | 331,280 | 230,728 | 276,575 | 276,503 |
| Scalar Subquery w/EXISTS | 239,192 | 247,647 | 157,704 | 149,742 |

---

## Tier 3: Advanced Operations

| Operation | SQL Sync | SQL Async | SimpleJp | AdaptiveJp (4c) |
|-----------|----------|-----------|----------|-----------------|
| IN Subquery | 1,217,100 | 865,476 | 683,905 | 744,701 |
| EXISTS Subquery | 1,079,467 | 545,504 | 675,009 | 37,347 |
| Correlated Subquery | 538,449 | 547,060 | 450,935 | 433,978 |
| Stream-Table Join | 462,808 | 460,110 | 372,218 | 394,468 |

### Stream-Stream Joins (FR-085)

| Component | Throughput | Notes |
|-----------|------------|-------|
| JoinStateStore | 5,346,820 | BTreeMap operations |
| High Cardinality | 1,722,461 | Unique keys stress test |
| JoinCoordinator | 607,437 | Full join pipeline |
| SQL Engine (sync) | 363,823 | End-to-end |
| SQL Engine (async) | 228,083 | Async pipeline |

---

## Tier 4: Specialized Operations

| Operation | SQL Sync | SQL Async | SimpleJp | AdaptiveJp (4c) |
|-----------|----------|-----------|----------|-----------------|
| ANY/ALL Operators | 1,344,704 | 742,184 | 999,908 | 915,150 |

---

## Running Benchmarks

```bash
# Run all SQL operation benchmarks
cargo test --release --no-default-features "performance::analysis::sql_operations" -- --nocapture

# Run by tier
cargo test --release --no-default-features "performance::analysis::sql_operations::tier1" -- --nocapture
cargo test --release --no-default-features "performance::analysis::sql_operations::tier2" -- --nocapture
cargo test --release --no-default-features "performance::analysis::sql_operations::tier3" -- --nocapture
cargo test --release --no-default-features "performance::analysis::sql_operations::tier4" -- --nocapture

# Run specific benchmark
cargo test --release --no-default-features test_interval_stream_join_performance -- --nocapture
```

---

## Benchmark Locations

| Tier | Location |
|------|----------|
| Tier 1 | `tests/performance/analysis/sql_operations/tier1_essential/` |
| Tier 2 | `tests/performance/analysis/sql_operations/tier2_common/` |
| Tier 3 | `tests/performance/analysis/sql_operations/tier3_advanced/` |
| Tier 4 | `tests/performance/analysis/sql_operations/tier4_specialized/` |

---

## Related Documentation

- [Benchmarks Guide](../benchmarks/README.md)
- [Stream-Stream Joins Design](../design/stream-stream-joins.md)
- [Stream-Table Join Architecture](../architecture/stream-table-join-performance.md)
