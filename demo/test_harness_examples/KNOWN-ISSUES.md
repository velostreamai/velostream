# Known Issues - Test Harness Examples

This document tracks known limitations and issues discovered during test harness validation.

## Current Status (2026-01-09)

> **TEST HARNESS: 36 Runnable / 5 Blocked (88%)**

| Tier | Runnable | Blocked | Status |
|------|----------|---------|--------|
| tier1_basic | 5 | 3 | 63% |
| tier2_aggregations | 7 | 0 | **100%** |
| tier3_joins | 5 | 0 | **100%** |
| tier4_window_funcs | 4 | 0 | **100%** |
| tier5_complex | 2 | 3 | 40% |
| tier6_edge_cases | 4 | 0 | **100%** |
| tier7_serialization | 4 | 0 | **100%** |
| tier8_fault_tol | 4 | 0 | **100%** |
| **TOTAL** | **36** | **5** | **88%** |

**Run tests:** `./run-tests.sh --skip-blocked`

### Blocking Issues

| Issue | Description | Tests Affected |
|-------|-------------|----------------|
| #1 | SELECT DISTINCT not implemented | 1 |
| #2 | ORDER BY on unbounded streams (design limitation) | 2 |
| #10 | Multi-stage pipeline topic routing | 1 |
| #11 | IN (SELECT) subquery execution | 1 |
| #12 | UNION multi-source configuration | 1 |

---

## Active Issues

### 1. SELECT DISTINCT Not Supported

**Status:** Not Implemented
**Affected Tests:** `tier1_basic/05_distinct.sql`
**Severity:** Feature Gap

**Description:**
The SQL parser does not recognize the `DISTINCT` keyword after `SELECT`. When parsing `SELECT DISTINCT ...`, the parser fails silently, treating `DISTINCT` as a column name.

**What Works:**
```sql
-- Aggregation functions with DISTINCT (SUPPORTED)
SELECT COUNT(DISTINCT customer_id) FROM orders
SELECT SUM(DISTINCT amount) FROM transactions
```

**What Doesn't Work:**
```sql
-- SELECT DISTINCT clause (NOT SUPPORTED)
SELECT DISTINCT value, active FROM input_stream
```

**Workaround:**
Use `GROUP BY` to achieve deduplication:
```sql
-- Instead of: SELECT DISTINCT value, active FROM input_stream
SELECT value, active FROM input_stream GROUP BY value, active
```

---

### 2. ORDER BY in Streaming Mode

**Status:** Partially Resolved - Works in windowed queries
**Affected Tests:** `tier1_basic/06_order_by.sql` (still blocked - unbounded stream)
**Severity:** Design Limitation

**Description:**
ORDER BY is now supported for **windowed queries** (Flink-style bounded sorting). When a window
emits, results are sorted according to the ORDER BY clause.

**What Works:**
```sql
-- ORDER BY in windowed queries (FR-084)
SELECT symbol, AVG(price) as avg_price
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
ORDER BY avg_price DESC

-- ORDER BY inside window functions
SELECT ticker, price,
       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price DESC) as rank
FROM trades
```

**What Doesn't Work (By Design):**
```sql
-- Top-level ORDER BY on unbounded stream (fundamentally impossible)
SELECT * FROM trades ORDER BY amount DESC
```

**Why Unbounded ORDER BY Can't Work:**
Streaming SQL fundamentally cannot sort unbounded data - you'd need infinite memory to buffer
all records before emitting any output. This is the same behavior as Apache Flink.

---

### 10. Multi-Stage Pipeline Topic Routing

**Status:** Test Harness Limitation
**Affected Tests:** `tier5_complex/40_pipeline.sql`
**Severity:** Medium

**Description:**
Multi-stage pipelines with cascading CREATE STREAM/TABLE statements don't properly route
data between stages. Stage 3 (`flagged_regions`) reads from the raw input topic instead
of the aggregated output from Stage 2 (`regional_summary`).

**Error Symptom:**
```
Multiple fields not found during SELECT clause: window_start, window_end, total_revenue,
avg_transaction, transaction_count
```

**Root Cause:**
The test harness executes each query independently but doesn't properly chain topic outputs
to downstream query inputs. The `from_previous` test spec feature needs enhancement.

**Workaround:**
Split multi-stage pipelines into separate test files or use explicit topic names.

---

### 11. IN (SELECT) Subquery Execution

**Status:** Runtime Issue
**Affected Tests:** `tier5_complex/41_subqueries.sql`
**Severity:** Medium

**Description:**
Subqueries using `IN (SELECT ...)` pattern against file sources produce 0 records.

**SQL Pattern:**
```sql
WHERE o.customer_id IN (
    SELECT customer_id FROM vip_customers WHERE tier IN ('gold', 'platinum')
)
```

**Root Cause:**
The subquery against `vip_customers` file source isn't being properly loaded or executed
during the main query processing. The file source table may not be registered in the
processor context when the IN subquery executes.

**Workaround:**
Consider using JOINs instead of IN subqueries for file source lookups.

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

**Overall Progress: 36 runnable, 5 blocked (41 total tests) - 88%**

### tier1_basic (8 tests) - Basic SQL Operations
| Test | Status | Notes |
|------|--------|-------|
| 01_passthrough | ✅ PASSED | Basic SELECT * passthrough |
| 02_projection | ✅ PASSED | Column selection |
| 03_filter | ✅ PASSED | WHERE clause filtering |
| 04_casting | ✅ PASSED | Type casting operations |
| 05_distinct | ⚠️ BLOCKED | Issue #1 - SELECT DISTINCT not implemented |
| 06_order_by | ⚠️ BLOCKED | Issue #2 - ORDER BY not applied |
| 07_limit | ⚠️ BLOCKED | Issue #2 - Uses ORDER BY (LIMIT itself works) |
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
| 40_pipeline | ⚠️ BLOCKED | Issue #10 - Multi-stage pipeline topic routing |
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
tier1_basic:        5/8 runnable  (63%)  - 3 blocked by SQL features
tier2_aggregations: 7/7 runnable  (100%) ★
tier3_joins:        5/5 runnable  (100%) ★
tier4_window_funcs: 4/4 runnable  (100%) ★
tier5_complex:      2/5 runnable  (40%)  - 3 blocked by advanced features
tier6_edge_cases:   4/4 runnable  (100%) ★
tier7_serialization:4/4 runnable  (100%) ★
tier8_fault_tol:    4/4 runnable  (100%) ★
─────────────────────────────────────────
TOTAL:              36/41 runnable (88%)
```

---

## Recommendations

1. **Skip unsupported tests** until features are implemented
2. **Use workarounds** where possible (GROUP BY for DISTINCT)
3. **Track feature requests** for SELECT DISTINCT

---

## Related Documentation

- Parser Grammar: `docs/sql/PARSER_GRAMMAR.md`
- SQL Functions: `docs/sql/functions/`
- Copy-Paste Examples: `docs/sql/COPY_PASTE_EXAMPLES.md`
- Claude SQL Rules: `docs/claude/SQL_GRAMMAR_RULES.md`
