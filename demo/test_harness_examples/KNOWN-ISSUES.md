# Known Issues - Test Harness Examples

This document tracks known limitations and issues discovered during test harness validation.

## Current Status (2026-01-12)

> **TEST HARNESS: 40 Runnable / 2 Blocked (95%)**

| Tier | Runnable | Blocked | Status |
|------|----------|---------|--------|
| tier1_basic | 8 | 0 | **100%** |
| tier2_aggregations | 7 | 0 | **100%** |
| tier3_joins | 5 | 0 | **100%** |
| tier4_window_funcs | 4 | 0 | **100%** |
| tier5_complex | 3 | 2 | 60% |
| tier6_edge_cases | 4 | 0 | **100%** |
| tier7_serialization | 4 | 0 | **100%** |
| tier8_fault_tol | 4 | 0 | **100%** |
| **TOTAL** | **40** | **2** | **95%** |

**Run tests:** `./run-tests.sh --skip-blocked`

### Blocking Issues

| Issue | Description | Tests Affected |
|-------|-------------|----------------|
| #11 | IN (SELECT) subquery execution | 1 |
| #12 | UNION multi-source configuration | 1 |

---

## Resolved Issues

### ~~1. SELECT DISTINCT Not Supported~~ ✅ RESOLVED

**Status:** Implemented (2026-01-10)
**Affected Tests:** `tier1_basic/05_distinct.sql` - Now unblocked

**What Now Works:**
```sql
-- SELECT DISTINCT clause (NOW SUPPORTED)
SELECT DISTINCT value, active FROM input_stream
SELECT DISTINCT category, status FROM orders WHERE amount > 100
SELECT DISTINCT * FROM input_stream
```

**Implementation Details:**
- Added `Distinct` token to parser tokenizer
- Added `distinct: bool` field to `StreamingQuery::Select` AST
- Implemented deduplication using record hashing in SelectProcessor
- State tracking via `distinct_seen` HashMap in ProcessorContext

---

### ~~13. Kafka Sources Not Detected (0 Data Readers)~~ ✅ RESOLVED

**Status:** Fixed (2026-01-12)
**Affected Tests:** All tests using kafka_source - test harness would hang

**What Was Fixed:**
Kafka sources are now properly detected and data readers are created. Previously,
the test harness would hang indefinitely because jobs had "0 sources, 1 sinks".

**Root Cause:**
`add_known_tables()` in QueryAnalyzer was called with ALL extracted table dependencies
(including Kafka sources). The `analyze_source()` method skips tables in `known_tables`,
so Kafka sources were never added to `required_sources`, resulting in 0 data readers.

**Fix Applied:**
Only add `file_source` tables (which are actually in the registry) to `known_tables`.
External sources like `kafka_source` are now properly analyzed and added to `required_sources`.

```rust
// BEFORE (broken): All tables skipped
analyzer.add_known_tables(required_tables.clone());

// AFTER (fixed): Only file_source tables skipped
analyzer.add_known_tables(file_source_tables);
```

**Verification:**
```
BEFORE: "Starting job ... with 0 sources and 1 sinks" → hang
AFTER:  "Starting job ... with 1 sources and 1 sinks" → ALL TESTS PASSED
```

---

### ~~10. Multi-Stage Pipeline Topic Routing~~ ✅ RESOLVED

**Status:** Fixed (2026-01-10)
**Affected Tests:** `tier5_complex/40_pipeline.sql` - Now unblocked

**What Was Fixed:**
Multi-stage pipelines now correctly route data between stages. Each stage reads from
the correct upstream topic and writes to its own output topic.

**Root Cause:**
The `normalize_topic_property` function in `config_loader.rs` was ignoring explicit
`topic.name` from SQL WITH clauses when a `topic` key already existed from YAML configs.
This caused all sources to read from the same YAML default topic.

**Fix Applied:**
Changed `normalize_topic_property` to make `topic.name` override `topic`, ensuring
SQL WITH clause values take precedence over YAML config defaults.

**What Now Works:**
```sql
-- Multi-stage pipeline with explicit topic routing
CREATE STREAM cleaned_transactions AS SELECT ... FROM raw_transactions
WITH ('raw_transactions.topic.name' = 'test_raw_transactions', ...);

CREATE TABLE regional_summary AS SELECT ... FROM cleaned_transactions
WITH ('cleaned_transactions.topic.name' = 'test_cleaned_transactions', ...);

CREATE STREAM flagged_regions AS SELECT ... FROM regional_summary
WITH ('regional_summary.topic.name' = 'test_regional_summary', ...);
```

---

## Active Issues

### 11. IN (SELECT) Subquery Filter Not Applied

**Status:** Implementation Bug
**Affected Tests:** `tier5_complex/41_subqueries.sql`
**Severity:** Medium

**Description:**
The `IN (SELECT ...)` subquery filter is not being evaluated - all records pass through unfiltered.

**SQL Pattern:**
```sql
WHERE o.customer_id IN (
    SELECT customer_id FROM vip_customers WHERE tier IN ('gold', 'platinum')
)
```

**Observed Behavior:**
- ✅ File source table loads correctly (10 rows into `vip_customers`)
- ✅ Records flow through pipeline (200 input → 200 output)
- ❌ Subquery WHERE clause not applied (expected ~100 filtered records)

**Root Cause:**
The IN subquery evaluation logic doesn't execute the subquery against the loaded table.
The filter is being ignored, allowing all records to pass through.

**Workaround:**
Use JOINs instead of IN subqueries for filtering against reference tables.

---

### 12. UNION Multi-Source Configuration

**Status:** Configuration Issue
**Affected Tests:** `tier5_complex/44_union.sql`
**Severity:** Medium

**Description:**
UNION ALL queries combining multiple Kafka sources fail when the test harness runs in
"file-only mode" or when sources aren't properly configured.

**Error Symptom:**
```
Source type must be explicitly specified for 'us_transactions'
```

**Root Cause:**
The test harness detects "file-only mode" when no external Kafka is available, but UNION
queries require multiple Kafka sources. The configuration resolution doesn't properly
handle multi-source UNION patterns.

**Workaround:**
Ensure testcontainers Kafka is available, or redesign tests to use file sources.

---

## Test Progress by Tier

**Overall Progress: 40 runnable, 2 blocked (42 total tests) - 95%**

### tier1_basic (8 tests) - Basic SQL Operations ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 01_passthrough | ✅ PASSED | Basic SELECT * passthrough |
| 02_projection | ✅ PASSED | Column selection |
| 03_filter | ✅ PASSED | WHERE clause filtering |
| 04_casting | ✅ PASSED | Type casting operations |
| 05_distinct | ✅ PASSED | SELECT DISTINCT deduplication |
| 06_order_by | ✅ PASSED | ORDER BY in windowed context (unbounded ORDER BY is unsupported by design) |
| 07_limit | ✅ PASSED | LIMIT clause |
| 08_headers | ✅ PASSED | HEADER, SET_HEADER, HAS_HEADER, HEADER_KEYS |

### tier2_aggregations (7 tests) - Aggregation Functions ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 10_count.annotated | ⏭️ SKIP | No test spec (demo only) |
| 10_count | ✅ PASSED | COUNT(*), COUNT(col) |
| 11_sum_avg | ✅ PASSED | SUM, AVG, MIN, MAX |
| 12_tumbling_window | ✅ PASSED | TUMBLING windows |
| 13_sliding_window | ✅ PASSED | SLIDING windows |
| 14_session_window | ✅ PASSED | SESSION windows |
| 15_compound_keys | ✅ PASSED | Compound GROUP BY keys |

### tier3_joins (5 tests) - Join Operations ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 20_stream_table_join | ✅ PASSED | Stream-table join |
| 21_stream_stream_join | ✅ PASSED | Stream-stream temporal join |
| 22_multi_join | ✅ PASSED | Multi-table joins |
| 23_right_join | ✅ PASSED | RIGHT JOIN |
| 24_full_outer_join | ✅ PASSED | FULL OUTER JOIN |

### tier4_window_functions (4 tests) - Window Functions ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 30_lag_lead | ✅ PASSED | LAG/LEAD functions |
| 31_row_number | ✅ PASSED | ROW_NUMBER(), RANK() |
| 32_running_agg | ✅ PASSED | Running SUM, AVG, COUNT |
| 33_rows_buffer | ✅ PASSED | ROWS WINDOW BUFFER aggregates |

### tier5_complex (5 tests) - Complex Queries
| Test | Status | Notes |
|------|--------|-------|
| 40_pipeline | ✅ PASSED | Multi-stage pipeline (3 stages) |
| 41_subqueries | ⚠️ BLOCKED | Issue #11 - IN (SELECT) subquery produces 0 records |
| 42_case | ✅ PASSED | CASE WHEN expressions |
| 43_complex_filter | ✅ PASSED | BETWEEN, IN, complex filters |
| 44_union | ⚠️ BLOCKED | Issue #12 - UNION multi-source configuration |

### tier6_edge_cases (4 tests) - Edge Cases ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 50_nulls | ✅ PASSED | COALESCE null handling |
| 51_empty | ✅ PASSED | Empty stream handling |
| 52_large_volume | ✅ PASSED | High volume processing |
| 53_late_arrivals | ✅ PASSED | Watermark handling |

### tier7_serialization (4 tests) - Serialization Formats ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 60_json_format | ✅ PASSED | JSON serialization |
| 61_avro_format | ✅ PASSED | Avro serialization |
| 62_protobuf_format | ✅ PASSED | Protobuf serialization |
| 63_format_conversion | ✅ PASSED | Format conversion |

### tier8_fault_tolerance (4 tests) - Fault Tolerance ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 70_dlq_basic | ✅ PASSED | DLQ handling |
| 72_fault_injection | ✅ PASSED | Fault injection testing |
| 73_debug_mode | ✅ PASSED | Debug mode features |
| 74_stress_test | ✅ PASSED | Stress testing |

### Progress Summary
```
tier1_basic:        8/8 runnable  (100%) ★
tier2_aggregations: 7/7 runnable  (100%) ★
tier3_joins:        5/5 runnable  (100%) ★
tier4_window_funcs: 4/4 runnable  (100%) ★
tier5_complex:      3/5 runnable  (60%)  - 2 blocked by advanced features
tier6_edge_cases:   4/4 runnable  (100%) ★
tier7_serialization:4/4 runnable  (100%) ★
tier8_fault_tol:    4/4 runnable  (100%) ★
─────────────────────────────────────────
TOTAL:              40/42 runnable (95%)
```

---

## Recommendations

1. **Skip unsupported tests** until features are implemented
2. **Use workarounds** where possible (e.g., windowed ORDER BY instead of unbounded)

---

## Related Documentation

- Parser Grammar: `docs/sql/PARSER_GRAMMAR.md`
- SQL Functions: `docs/sql/functions/`
- Copy-Paste Examples: `docs/sql/COPY_PASTE_EXAMPLES.md`
- Claude SQL Rules: `docs/claude/SQL_GRAMMAR_RULES.md`
