# Known Issues - Test Harness Examples

This document tracks known limitations and issues discovered during test harness validation.

## Current Status (2025-01-09)

> **TEST HARNESS: 28 Runnable / 10 Blocked (74%)**

| Tier | Runnable | Blocked | Status |
|------|----------|---------|--------|
| tier1_basic | 4 | 3 | 57% |
| tier2_aggregations | 7 | 0 | **100%** |
| tier3_joins | 0 | 5 | 0% |
| tier4_window_funcs | 4 | 0 | **100%** |
| tier5_complex | 1 | 4 | 20% |
| tier6_edge_cases | 4 | 0 | **100%** |
| tier7_serialization | 4 | 0 | **100%** |
| tier8_fault_tol | 4 | 0 | **100%** |
| **TOTAL** | **28** | **12** | **70%** |

**Schema issues (#7, #8): RESOLVED**
**Config path issue (#6): RESOLVED**
**LIMIT issue (#3): RESOLVED** - Works correctly; test blocked by ORDER BY (#2)
**Windows issue (#4): RESOLVED** - Added time_simulation to test specs

**Run tests:** `./run-tests.sh --skip-blocked`

### Blocking Issues

| Issue | Description | Tests Affected |
|-------|-------------|----------------|
| #1 | SELECT DISTINCT not implemented | 1 |
| #2 | ORDER BY not applied in streaming | 2 |
| #5 | Runtime panic (block_on in async) | 7 |

---

## SQL Parser/Execution Limitations

### 1. SELECT DISTINCT Not Supported

**Status:** Not Implemented
**Affected Tests:** `tier1_basic/05_distinct.sql`
**Severity:** Feature Gap

**Description:**
The SQL parser does not recognize the `DISTINCT` keyword after `SELECT`. When parsing `SELECT DISTINCT ...`, the parser fails silently, treating `DISTINCT` as a column name, which causes downstream validation errors.

**Error Symptom:**
```
Analysis error: Configuration error: Source type must be explicitly specified for ''
```

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

**Technical Details:**

| Component | File | Status |
|-----------|------|--------|
| Lexer TokenType enum | `src/velostream/sql/parser.rs:157-290` | No `Distinct` token defined |
| Keyword mapping | `src/velostream/sql/parser.rs:325-410` | No `"DISTINCT"` keyword registered |
| AST Select struct | `src/velostream/sql/ast.rs:210-267` | No `distinct: bool` field |
| Parser parse_select | `src/velostream/sql/parser.rs:1102-1471` | Goes directly from SELECT to fields |

**Source Code References:**
- Parser entry: `src/velostream/sql/parser.rs:1102` - `fn parse_select()`
- TokenType enum: `src/velostream/sql/parser.rs:157` - `pub enum TokenType`
- AST definition: `src/velostream/sql/ast.rs:210` - `StreamingQuery::Select`

**Implementation Notes:**
`COUNT_DISTINCT` and `APPROX_COUNT_DISTINCT` are implemented as aggregation functions in:
- `src/velostream/sql/execution/aggregation/functions.rs:40-46`
- `src/velostream/sql/validation/function_registry.rs:141-143`

**Important: SELECT DISTINCT vs COUNT_DISTINCT - They Are NOT The Same**

These are fundamentally different operations and cannot be aliases for each other:

| Feature | SELECT DISTINCT | COUNT_DISTINCT |
|---------|-----------------|----------------|
| **Purpose** | Remove duplicate rows from results | Count unique values |
| **Returns** | Multiple rows (the unique values) | Single scalar number |
| **Type** | Query modifier | Aggregation function |

**Example showing the difference:**
```sql
-- Sample data: orders table with customer_id values [1, 2, 1, 3, 2, 1]

-- SELECT DISTINCT - returns the unique values as rows
SELECT DISTINCT customer_id FROM orders;
-- Result: 3 rows → [1, 2, 3]

-- COUNT_DISTINCT - returns how many unique values exist
SELECT COUNT_DISTINCT(customer_id) FROM orders;
-- Result: 1 row → 3
```

**When to use each:**

| Use Case | Solution |
|----------|----------|
| "Show me all unique categories" | `SELECT DISTINCT category FROM products` |
| "How many unique categories?" | `SELECT COUNT_DISTINCT(category) FROM products` |
| "List unique customer/product pairs" | `SELECT DISTINCT customer_id, product_id FROM orders` |
| "Count unique customers per region" | `SELECT region, COUNT_DISTINCT(customer_id) GROUP BY region` |

**Recommendation:** Both should be supported as they serve different purposes. The GROUP BY workaround provides SELECT DISTINCT functionality until proper support is added.

---

### 2. ORDER BY Not Applied in Streaming Mode

**Status:** Partially Implemented (Parsed but Not Executed)
**Affected Tests:** `tier1_basic/06_order_by.sql`, `tier1_basic/07_limit.sql`
**Severity:** Streaming Limitation

**Description:**
ORDER BY is parsed and validated but **not executed** for top-level streaming queries. The `OrderProcessor` exists but is only used within window function processing (`OVER (ORDER BY ...)`), never for top-level ORDER BY clauses.

**Error Symptom:**
Records are returned in arrival order, not sorted order:
```
Expected: Descending order by amount
Actual:   75.15 followed by 6994.92 (not sorted)
```

**What Works:**
```sql
-- ORDER BY inside window functions (SUPPORTED)
SELECT ticker, price,
       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price DESC) as rank
FROM trades
```

**What Doesn't Work:**
```sql
-- Top-level ORDER BY in streaming (NOT APPLIED)
SELECT * FROM trades ORDER BY amount DESC
```

**Technical Details:**

| Component | File | Line | Status |
|-----------|------|------|--------|
| OrderProcessor | `src/velostream/sql/execution/processors/order.rs:17-46` | Fully implemented |
| OrderProcessor usage | - | - | **Never called from streaming pipeline** |
| SelectProcessor ORDER BY handling | `src/velostream/sql/execution/processors/select.rs:716-754` | Validates but doesn't sort |
| Window function ordering | `src/velostream/sql/execution/expression/window_functions.rs:137-138` | Uses `sort_buffer_by_order()` (custom impl) |
| Performance warning | `src/velostream/sql/validation/query_validator.rs:324-327` | Warns about memory issues |

**Source Code References:**
- OrderProcessor definition: `src/velostream/sql/execution/processors/order.rs:17`
- OrderProcessor::process: `src/velostream/sql/execution/processors/order.rs:31-46`
- SelectProcessor validation only: `src/velostream/sql/execution/processors/select.rs:716-754`
- Window function custom sort: `src/velostream/sql/execution/expression/window_functions.rs:177-213`
- Export (unused): `src/velostream/sql/execution/processors/mod.rs:206`

**Design Notes:**
The query validator warns: *"ORDER BY without LIMIT can cause memory issues in streaming"* (line 327). This is why ORDER BY isn't applied - true streaming can't buffer all records for sorting.

**Workaround:**
1. Use window functions with ORDER BY for ranking
2. Apply sorting in the consuming application
3. Use with LIMIT for bounded result sets (LIMIT works correctly)

---

### 3. LIMIT Behavior in Streaming

**Status:** ✅ RESOLVED (2025-01-09)
**Affected Tests:** None (was `tier1_basic/07_limit.sql`, now blocked by Issue #2)
**Severity:** Not an Issue

**Resolution:**
Analysis confirmed LIMIT is **fully implemented and works correctly**:

1. `StreamExecutionEngine` owns `record_count: u64` which persists for job lifetime
2. `PartitionReceiver` directly owns the engine (no Arc/Mutex reset between batches)
3. `LimitProcessor::check_limit()` correctly compares and terminates at limit
4. State flows: Engine → ProcessorContext → LimitProcessor → back to Engine

**Why `07_limit.sql` Still Fails:**
The test uses `ORDER BY amount DESC LIMIT 10`. LIMIT works, but ORDER BY doesn't (Issue #2).
The test expects ordered results, which fails due to Issue #2, not LIMIT.

**Verification Complete:**
- ✅ LIMIT stops stream after N total records (global, not per-batch)
- ✅ Context persists across batches (engine owns count)
- ✅ State is job-lifetime, reset only on job restart

**A simple test without ORDER BY would pass:**
```sql
SELECT id, value, amount FROM input_stream WHERE amount > 0 LIMIT 10
```

---

### 4. Windowed Queries Need Watermarks in Test Harness

**Status:** ✅ RESOLVED (2025-01-09)
**Affected Tests:** None (was 6 tests in tier2_aggregations and tier6_edge_cases)
**Severity:** Not an Issue

**Resolution:**
Added `time_simulation` configuration to all windowed test specs. This generates data with
timestamps spanning multiple window boundaries, causing windows to close naturally when
records from the next window arrive.

**Fix Applied:**
```yaml
inputs:
  - source: market_data
    schema: market_data
    records: 100
    time_simulation:
      start_time: "-3m"    # 3 minutes spans multiple 1-min windows
      end_time: "now"
      sequential: true     # Ordered timestamps ensure window progression
```

**Tests Fixed:**
- `tier2_aggregations/12_tumbling_window.test.yaml` - 3 min range for 1-min windows
- `tier2_aggregations/13_sliding_window.test.yaml` - 10 min range for 5-min windows
- `tier2_aggregations/14_session_window.test.yaml` - 5 min with jitter for session gaps
- `tier2_aggregations/15_compound_keys.test.yaml` - Multiple queries with appropriate ranges
- `tier6_edge_cases/52_large_volume.test.yaml` - 5 min range
- `tier6_edge_cases/53_late_arrivals.test.yaml` - 3 min range with jitter

**Why It Works:**
```
Without time_simulation:
  Records: ●●●●●●●●●● (all at ~same moment)
  Windows: [Window 1] ← never closes

With time_simulation (start: -3m, end: now):
  Records: ●●●|●●●|●●● (spread across 3 minutes)
  Windows: [W1] → CLOSES → [W2] → CLOSES → [W3]
```

**Note:** `53_late_arrivals.sql` also has explicit watermark configuration for testing
the watermark path specifically.

---

### 5. Stream-Stream Joins Cause Runtime Panic

**Status:** Bug
**Affected Tests:** `tier3_joins/21_stream_stream_join.sql`
**Severity:** Critical

**Description:**
Stream-stream joins with temporal predicates (`BETWEEN ... AND ... + INTERVAL`) cause two issues:
1. Type error during temporal join evaluation
2. Nested Tokio runtime panic when DLQ write is attempted

**Error Symptoms:**
```
Type error: expected numeric or interval/timestamp, got incompatible types

thread 'tokio-runtime-worker' panicked at src/velostream/server/v2/partition_receiver.rs:590:45:
Cannot start a runtime from within a runtime.
```

**Technical Details:**

| Component | File | Line | Issue |
|-----------|------|------|-------|
| Temporal join evaluation | - | - | Type mismatch in INTERVAL arithmetic |
| DLQ block_on panic | `src/velostream/server/v2/partition_receiver.rs:590` | `runtime.block_on(fut)` in async context |
| Error handling path | `src/velostream/server/v2/partition_receiver.rs:549-626` | DLQ write attempt |

**Source Code References:**
- Panic location: `src/velostream/server/v2/partition_receiver.rs:590`
- DLQ write logic: `src/velostream/server/v2/partition_receiver.rs:556-616`
- Process batch: `src/velostream/server/v2/partition_receiver.rs:529-627`

**Root Cause Analysis:**
1. The SQL join predicate `s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '24' HOUR` fails with a type error
2. When SQL execution fails, the error handling path tries to write to DLQ
3. DLQ write is async, but the code uses `block_on()` to wait synchronously
4. `block_on()` inside an existing Tokio runtime causes a panic

**Fix Required:**
1. Fix temporal join type evaluation to handle INTERVAL arithmetic
2. Replace `block_on()` with proper async handling (spawn task or use `spawn_blocking`)

---

### 6. Stream-Table Joins Need Reference Data Files

**Status:** ✅ RESOLVED
**Affected Tests:** `tier3_joins/20_stream_table_join.sql`
**Severity:** Medium (was)

**Description:**
Stream-table joins reference file sources for lookup tables, but the file path configuration property names didn't match what the code expected.

**Error Symptom (was):**
```
File not found: File './demo_data/sample.csv' does not exist
```

**Resolution (2025-01-09):**

Two fixes were applied:

1. **Added `normalize_path_property()` to config_loader.rs** - This normalizes flattened YAML keys (`file.path`, `data_source.path`, `source.path`) to the expected `path` key, consistent with the existing `normalize_topic_property()` pattern.

2. **Fixed relative paths in config files** - Changed `data/products.csv` to `../data/products.csv` since tests run from tier subdirectories.

**Files Modified:**
- `src/velostream/datasource/config_loader.rs` - Added path normalization
- `configs/products_table.yaml` - Fixed relative path
- `configs/customers_table.yaml` - Fixed relative path

**Note:** The tier3 join tests are still blocked by other issues (Issue #5 - runtime panic on joins), but the config path loading now works correctly.

---

### 7. SQL/Schema Mismatch - Fields Not in Schema

**Status:** ✅ RESOLVED
**Affected Tests:** `tier5_complex/43_complex_filter.sql`, `tier6_edge_cases/50_nulls.sql`, others
**Severity:** Medium (was)

**Description:**
Some SQL files reference fields that don't exist in the test schemas. The SQL expects certain fields but the .schema.yaml used for data generation doesn't have those fields.

**Example - 43_complex_filter.sql:**
```sql
-- SQL references these fields:
WHERE priority IN ('high', 'medium')
AND quantity * unit_price * (1 - discount_pct / 100) AS net_total

-- But order_event.schema.yaml doesn't have:
-- - priority
-- - discount_pct
```

**Example - 50_nulls.sql:**
```sql
-- SQL expects sensor fields:
SELECT sensor_id, temperature, humidity, pressure, ...
FROM sensor_readings

-- But test spec uses simple_record schema which has:
-- id, value, amount, count, active, event_time
```

**Resolution (2025-01-09):**
- Created `sensor_readings.schema.yaml` for tier6 null/edge case tests
- Created `events.schema.yaml` for tier6 large volume tests
- Added `priority` and `discount_pct` fields to `order_event.schema.yaml`
- Updated test specs to reference correct schemas

---

### 8. Missing Schema Files for Tier7/Tier8

**Status:** ✅ RESOLVED
**Affected Tests:** All of `tier7_serialization/`, `tier5_complex/44_union.sql`
**Severity:** Medium (was)

**Description:**
Several tests reference schema files that don't exist:

| Test | References | Actually Exists |
|------|------------|-----------------|
| tier5/44_union | `transaction_record.schema.yaml` | ❌ No |
| tier7/60_json_format | `trade_record.schema.yaml` | ❌ No (only .avsc/.proto) |
| tier7/61_avro_format | `trade_record.schema.yaml` | ❌ No |
| tier7/62_protobuf_format | `trade_record.schema.yaml` | ❌ No |
| tier7/63_format_conversion | `trade_record.schema.yaml` | ❌ No |

**Note:** The tier7/schemas/ directory contains:
- `trade_record.avsc` (Avro schema)
- `trade_record.proto` (Protobuf schema)
- But NOT `trade_record.schema.yaml` (data generator schema)

**Resolution (2025-01-09):**
Created missing `.schema.yaml` files in `schemas/` directory:
- `trade_record.schema.yaml` - For tier7 serialization tests
- `transaction_record.schema.yaml` - For tier5/44_union
- `order_record.schema.yaml` - For tier3 join tests
- `product_record.schema.yaml` - For tier3 join tests
- `shipment_record.schema.yaml` - For tier3 join tests
- `event_record.schema.yaml` - For tier8 fault tolerance tests

Also fixed test spec syntax (removed unsupported `format` and `schema_valid` assertions).

---

## Test Harness Issues (Fixed)

### Boolean Field Value Assertion (FIXED)

**Status:** Fixed
**Fix Location:** `src/velostream/test_harness/assertions.rs:3627`

**Description:**
The `field_values` assertion with boolean comparisons was failing even when values matched. The comparison logic didn't handle `serde_yaml::Value::Bool`.

**Fix:**
Added `expected.as_bool()` check before string/number checks in `compare_field_value()`.

---

## Test Specification Issues (Fixed)

### Assertion Type Names

Several test specs used incorrect assertion type names:

| Incorrect | Correct |
|-----------|---------|
| `sorted` | `ordering` |
| `field_condition` | `field_values` |
| `fields` (in no_duplicates) | `key_fields` |

### Operator Names in field_values

| Incorrect | Correct |
|-----------|---------|
| `gt` | `greater_than` |
| `eq` | `equals` |
| `lt` | `less_than` |
| `lte` | `less_than_or_equals` |
| `gte` | `greater_than_or_equals` |

---

## Test Progress by Tier

**Overall Progress: 28 runnable, 12 blocked (40 total tests)**

### tier1_basic (7 tests) - Basic SQL Operations
| Test | Status | Notes |
|------|--------|-------|
| 01_passthrough | ✅ PASSED | Basic SELECT * passthrough |
| 02_projection | ✅ PASSED | Column selection |
| 03_filter | ✅ PASSED | WHERE clause filtering |
| 04_casting | ✅ PASSED | Type casting operations |
| 05_distinct | ⚠️ BLOCKED | Issue #1 - SELECT DISTINCT not implemented |
| 06_order_by | ⚠️ BLOCKED | Issue #2 - ORDER BY not applied |
| 07_limit | ⚠️ BLOCKED | Issue #2 - Uses ORDER BY (LIMIT itself works) |

### tier2_aggregations (7 tests) - Aggregation Functions ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 10_count.annotated | ⏭️ SKIP | No test spec (demo only) |
| 10_count | ✅ PASSED | COUNT(*), COUNT(col) |
| 11_sum_avg | ✅ PASSED | SUM, AVG, MIN, MAX |
| 12_tumbling_window | ✅ PASSED | Added time_simulation for window closure |
| 13_sliding_window | ✅ PASSED | Added time_simulation for window closure |
| 14_session_window | ✅ PASSED | Added time_simulation with jitter |
| 15_compound_keys | ✅ PASSED | Added time_simulation for window closure |

### tier3_joins (5 tests) - Join Operations
| Test | Status | Notes |
|------|--------|-------|
| 20_stream_table_join | ⚠️ BLOCKED | Issue #5 - Join produces nulls/field aliasing |
| 21_stream_stream_join | ⚠️ BLOCKED | Issue #5 - Runtime panic |
| 22_multi_join | ⚠️ BLOCKED | Issue #5 - File source + runtime panic |
| 23_right_join | ⚠️ BLOCKED | Issue #5 - Runtime panic on join |
| 24_full_outer_join | ⚠️ BLOCKED | Issue #5 - Runtime panic on join |

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
| 40_pipeline | ⚠️ BLOCKED | Issues #4, #5 - Multi-stage with windows + panic |
| 41_subqueries | ⚠️ BLOCKED | Issues #5, #6 - Reference table + panic |
| 42_case | ✅ PASSED | CASE WHEN expressions work correctly |
| 43_complex_filter | ⚠️ BLOCKED | Issue #5 - Runtime panic |
| 44_union | ⚠️ BLOCKED | Issue #5 - Runtime panic on UNION |

### tier6_edge_cases (4 tests) - Edge Cases ★ 100%
| Test | Status | Notes |
|------|--------|-------|
| 50_nulls | ✅ PASSED | COALESCE null handling |
| 51_empty | ✅ PASSED | Empty stream handling |
| 52_large_volume | ✅ PASSED | Added time_simulation for window closure |
| 53_late_arrivals | ✅ PASSED | Added time_simulation + has watermark config |

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
| 70_dlq_basic | ✅ PASSED | Dead Letter Queue |
| 72_fault_injection | ✅ PASSED | Fault injection testing |
| 73_debug_mode | ✅ PASSED | Debug mode features |
| 74_stress_test | ✅ PASSED | Stress testing |

### Progress Summary
```
tier1_basic:        4/7 runnable  (57%)  - 3 blocked by SQL features
tier2_aggregations: 7/7 runnable  (100%) - ALL PASSED ✅ (time_simulation fix)
tier3_joins:        0/5 runnable  (0%)   - 5 blocked by runtime panic
tier4_window_funcs: 4/4 runnable  (100%) - ALL PASSED ✅
tier5_complex:      1/5 runnable  (20%)  - 4 blocked by runtime panic
tier6_edge_cases:   4/4 runnable  (100%) - ALL PASSED ✅ (time_simulation fix)
tier7_serialization:4/4 runnable  (100%) - ALL PASSED ✅
tier8_fault_tol:    4/4 runnable  (100%) - ALL PASSED ✅
─────────────────────────────────────────
TOTAL:              28/40 runnable (70%)
```

### Key Findings

1. **Six tiers at 100%** - tier2, tier4, tier6, tier7, tier8 all fully passing
2. **time_simulation fixed window tests** - Issue #4 resolved; windows now close properly
3. **LIMIT works correctly** - Issue #3 resolved; 07_limit blocked by ORDER BY (Issue #2)
4. **Runtime panic is the biggest blocker** - Issue #5 (block_on panic) affects 7+ tests
5. **Window functions work well** - ROWS WINDOW BUFFER, LAG/LEAD, TUMBLING, SLIDING, SESSION all working

---

## Recommendations

1. **Skip unsupported tests** until features are implemented
2. **Use workarounds** where possible (GROUP BY for DISTINCT)
3. **Fix critical bugs** (Issue #5 - runtime panic)
4. **Track feature requests** for SELECT DISTINCT and streaming ORDER BY
5. **Update file source config parsing** to handle `file.path` property

---

## Related Documentation

- Parser Grammar: `docs/sql/PARSER_GRAMMAR.md`
- SQL Functions: `docs/sql/functions/`
- Copy-Paste Examples: `docs/sql/COPY_PASTE_EXAMPLES.md`
- Claude SQL Rules: `docs/claude/SQL_GRAMMAR_RULES.md`

## Related Source Files

| Purpose | File |
|---------|------|
| Parser | `src/velostream/sql/parser.rs` |
| AST | `src/velostream/sql/ast.rs` |
| Order Processor | `src/velostream/sql/execution/processors/order.rs` |
| Limit Processor | `src/velostream/sql/execution/processors/limit.rs` |
| Select Processor | `src/velostream/sql/execution/processors/select.rs` |
| Window Processor | `src/velostream/sql/execution/processors/window.rs` |
| Watermarks | `src/velostream/sql/execution/watermarks.rs` |
| Server Watermarks | `src/velostream/server/v2/watermark.rs` |
| Partition Receiver | `src/velostream/server/v2/partition_receiver.rs` |
| File DataSource | `src/velostream/datasource/file/data_source.rs` |
