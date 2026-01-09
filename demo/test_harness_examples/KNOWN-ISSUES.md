# Known Issues - Test Harness Examples

This document tracks known limitations and issues discovered during test harness validation.

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
**Affected Tests:** `tier1_basic/06_order_by.sql`
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
3. Use with LIMIT for bounded result sets (may work - see Issue #3)

---

### 3. LIMIT Behavior in Streaming

**Status:** Implemented but Context-Dependent
**Affected Tests:** `tier1_basic/07_limit.sql`
**Severity:** Needs Verification

**Description:**
LIMIT is implemented via `LimitProcessor` and works within a single query execution context. However, behavior in continuous streaming may be per-batch rather than global depending on how the context is managed.

**Implementation Details:**

| Component | File | Line | Description |
|-----------|------|------|-------------|
| LimitProcessor | `src/velostream/sql/execution/processors/limit.rs:9-61` | Core LIMIT logic |
| Context tracking | `src/velostream/sql/execution/processors/context.rs:26` | `record_count: u64` |
| SelectProcessor integration | `src/velostream/sql/execution/processors/select.rs:430-435` | Checks after WHERE |
| Engine context setup | `src/velostream/sql/execution/engine.rs:504-506` | Sets `max_records` |

**Source Code References:**
- LimitProcessor: `src/velostream/sql/execution/processors/limit.rs:9`
- check_limit: `src/velostream/sql/execution/processors/limit.rs:13-28`
- increment_count: `src/velostream/sql/execution/processors/limit.rs:31-33`
- Context persistence: `src/velostream/sql/execution/engine.rs:494-510`

**How It Works:**
1. LIMIT value is stored in `ProcessorContext.max_records`
2. `LimitProcessor::check_limit()` compares `context.record_count` vs limit
3. When limit reached, returns early termination signal
4. Context is persistent via `Arc<Mutex>` for windowed queries

**Verification Needed:**
- Does LIMIT stop the stream after N total records?
- Or does it limit each batch/execution to N records?
- Is context reset between job restarts?

---

### 4. Windowed Queries Need Watermarks in Test Harness

**Status:** Test Infrastructure Limitation
**Affected Tests:** `tier2_aggregations/12_tumbling_window.sql`, `13_sliding_window.sql`, `14_session_window.sql`, `15_compound_keys.sql`
**Severity:** Test Infrastructure

**Description:**
Windowed queries (WINDOW TUMBLING, SLIDING, SESSION) require watermarks to trigger window closure and emit results. In the test harness, data is produced in a burst but windows don't close because:
1. Event time doesn't progress beyond window boundary
2. No watermark strategy is configured
3. Windows wait indefinitely for more data

**Error Symptom:**
```
Captured 0 records from topic 'test_output'
record_count: Expected > 0, got 0
```

**Technical Details:**

| Component | File | Description |
|-----------|------|-------------|
| WatermarkManager (SQL) | `src/velostream/sql/execution/watermarks.rs:40-442` | Full watermark implementation |
| WatermarkManager (Server) | `src/velostream/server/v2/watermark.rs` | Server-side watermark tracking |
| Window emit logic | `src/velostream/sql/execution/window_v2/strategies/tumbling.rs:446` | `should_emit()` |
| Emit changes strategy | `src/velostream/sql/execution/window_v2/emission/emit_changes.rs:60-78` | Emit on change |
| Emit final strategy | `src/velostream/sql/execution/window_v2/emission/emit_final.rs:41-43` | Emit on window close |

**Source Code References:**
- WatermarkStrategy enum: `src/velostream/sql/execution/watermarks.rs:59-80`
- WatermarkManager::update_watermark: `src/velostream/sql/execution/watermarks.rs:242-272`
- Tumbling should_emit: `src/velostream/sql/execution/window_v2/strategies/tumbling.rs:446`
- Session should_emit: `src/velostream/sql/execution/window_v2/strategies/session.rs:368`

**What Works:**
- Non-windowed aggregations (GROUP BY without WINDOW)
- Streaming aggregations with EMIT CHANGES (no windows)
- Window queries with proper watermark configuration

**What Doesn't Work:**
- WINDOW TUMBLING, SLIDING, SESSION queries in test harness without watermark configuration

**Workaround:**
1. Test non-windowed aggregations instead
2. Configure watermark strategy in SQL WITH clause:
   ```sql
   WITH (
       'watermark.strategy' = 'bounded_out_of_orderness',
       'watermark.max_lateness' = '5s'
   )
   ```
3. Use longer timeouts and data spanning multiple window boundaries
4. Generate data with event_time values that cross window boundaries

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

**Status:** Configuration Mismatch
**Affected Tests:** `tier3_joins/20_stream_table_join.sql`
**Severity:** Medium

**Description:**
Stream-table joins reference file sources for lookup tables, but the file path configuration property names don't match what the code expects.

**Error Symptom:**
```
File not found: File './demo_data/sample.csv' does not exist
```

**Technical Details:**

| Component | File | Line | Issue |
|-----------|------|------|-------|
| Config YAML | `configs/products_table.yaml:10` | Uses `file.path` |
| FileDataSource | `src/velostream/datasource/file/data_source.rs:61` | Looks for `source.path` or `path` |
| Default fallback | `src/velostream/datasource/file/data_source.rs:61` | Falls back to `./demo_data/sample.csv` |

**Source Code References:**
- Property lookup: `src/velostream/datasource/file/data_source.rs:53-58`
- Default path: `src/velostream/datasource/file/data_source.rs:61`
- Config template: `demo/test_harness_examples/configs/products_table.yaml`

**Configuration Mismatch:**

YAML config uses:
```yaml
file:
  path: "data/products.csv"
```

Code looks for:
```rust
props.get("source.path").or_else(|| props.get("path"))
```

The flattened YAML key would be `file.path`, not `path` or `source.path`.

**Workaround:**
1. Change YAML to use `path` directly:
   ```yaml
   path: "data/products.csv"
   ```
2. Or change code to look for `file.path` as well

**Note:** The actual data files exist at:
- `demo/test_harness_examples/data/products.csv`
- `demo/test_harness_examples/data/customers.csv`

---

### 7. SQL/Schema Mismatch - Fields Not in Schema

**Status:** Test Spec Issue
**Affected Tests:** `tier5_complex/43_complex_filter.sql`, `tier6_edge_cases/50_nulls.sql`, others
**Severity:** Medium

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

**Fix Required:**
1. Create matching schemas for each SQL file
2. Or update SQL to match existing schemas
3. Or update test specs to use correct schemas

---

### 8. Missing Schema Files for Tier7/Tier8

**Status:** Test Infrastructure Gap
**Affected Tests:** All of `tier7_serialization/`, `tier5_complex/44_union.sql`
**Severity:** Medium

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

**Fix Required:**
Create `.schema.yaml` files for test data generation that match the SQL field expectations.

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

**Overall Progress: 11 passed, 14 blocked, 15 not fully tested (40 total tests)**

### tier1_basic (7 tests) - Basic SQL Operations
| Test | Status | Notes |
|------|--------|-------|
| 01_passthrough | ✅ PASSED | Basic SELECT * passthrough |
| 02_projection | ✅ PASSED | Column selection |
| 03_filter | ✅ PASSED | WHERE clause filtering |
| 04_casting | ✅ PASSED | Type casting operations |
| 05_distinct | ❌ BLOCKED | Issue #1 - SELECT DISTINCT not implemented |
| 06_order_by | ❌ BLOCKED | Issue #2 - ORDER BY not applied |
| 07_limit | ⏳ NOT TESTED | Issue #3 - Needs verification |

### tier2_aggregations (6 tests) - Aggregation Functions
| Test | Status | Notes |
|------|--------|-------|
| 10_count | ✅ PASSED | COUNT(*), COUNT(col) |
| 11_sum_avg | ✅ PASSED | SUM, AVG, MIN, MAX |
| 12_tumbling_window | ❌ BLOCKED | Issue #4 - Needs watermarks |
| 13_sliding_window | ❌ BLOCKED | Issue #4 - Needs watermarks |
| 14_session_window | ❌ BLOCKED | Issue #4 - Needs watermarks |
| 15_compound_keys | ❌ BLOCKED | Issue #4 - Needs watermarks |

### tier3_joins (5 tests) - Join Operations
| Test | Status | Notes |
|------|--------|-------|
| 20_stream_table_join | ❌ BLOCKED | Issue #6 - Config mismatch |
| 21_stream_stream_join | ❌ BLOCKED | Issue #5 - Runtime panic |
| 22_multi_join | ⏳ NOT TESTED | |
| 23_right_join | ⏳ NOT TESTED | |
| 24_full_outer_join | ⏳ NOT TESTED | |

### tier4_window_functions (4 tests) - Window Functions
| Test | Status | Notes |
|------|--------|-------|
| 30_lag_lead | ✅ PASSED | LAG/LEAD functions |
| 31_row_number | ✅ PASSED | ROW_NUMBER(), RANK() |
| 32_running_agg | ✅ PASSED | Running SUM, AVG, COUNT |
| 33_rows_buffer | ✅ PASSED | ROWS WINDOW BUFFER aggregates |

**Note:** Tier4 requires explicit `--schemas ../schemas` flag. All 4 tests pass.

### tier5_complex (5 tests) - Complex Queries
| Test | Status | Notes |
|------|--------|-------|
| 40_pipeline | ❌ BLOCKED | Issues #4, #5 - Multi-stage with windows + block_on panic |
| 41_subqueries | ❌ BLOCKED | Issues #5, #6 - Reference table not found + block_on panic |
| 42_case | ✅ PASSED | CASE WHEN expressions work correctly |
| 43_complex_filter | ❌ BLOCKED | Issue #7 - Schema mismatch (priority, discount_pct) |
| 44_union | ❌ BLOCKED | Issue #8 - Missing transaction_record.schema.yaml |

### tier6_edge_cases (4 tests) - Edge Cases
| Test | Status | Notes |
|------|--------|-------|
| 50_nulls | ❌ BLOCKED | Issue #7 - Schema mismatch (sensor fields vs simple_record) |
| 51_empty | ⏳ NOT TESTED | Likely schema mismatch |
| 52_large_volume | ⏳ NOT TESTED | Likely schema mismatch |
| 53_late_arrivals | ⏳ NOT TESTED | Likely schema mismatch |

### tier7_serialization (4 tests) - Serialization Formats
| Test | Status | Notes |
|------|--------|-------|
| 60_json_format | ❌ BLOCKED | Issue #8 - Missing trade_record.schema.yaml |
| 61_avro_format | ❌ BLOCKED | Issue #8 - Missing trade_record.schema.yaml |
| 62_protobuf_format | ❌ BLOCKED | Issue #8 - Missing trade_record.schema.yaml |
| 63_format_conversion | ❌ BLOCKED | Issue #8 - Missing trade_record.schema.yaml |

**Note:** tier7/schemas/ has .avsc and .proto files but NOT .schema.yaml for data generation.

### tier8_fault_tolerance (4 tests) - Fault Tolerance
| Test | Status | Notes |
|------|--------|-------|
| 70_dlq_basic | ⏳ NOT TESTED | Dead Letter Queue |
| 72_fault_injection | ⏳ NOT TESTED | Fault injection testing |
| 73_debug_mode | ⏳ NOT TESTED | Debug mode features |
| 74_stress_test | ⏳ NOT TESTED | Stress testing |

### Progress Summary
```
tier1_basic:        4/7 passed  (57%)  - 2 blocked, 1 not tested
tier2_aggregations: 2/6 passed  (33%)  - 4 blocked by watermarks
tier3_joins:        0/5 passed  (0%)   - 2 blocked, 3 not tested
tier4_window_funcs: 4/4 passed  (100%) - ALL PASSED ✅
tier5_complex:      1/5 passed  (20%)  - 4 blocked by various issues
tier6_edge_cases:   0/4 tested  (0%)   - 1 blocked, 3 not tested
tier7_serialization:0/4 tested  (0%)   - 4 blocked by missing schemas
tier8_fault_tol:    0/4 tested  (0%)   - all not tested
─────────────────────────────────────────
TOTAL:              11/40 passed (28%)
```

### Key Findings

1. **Window functions work well** - tier4 is 100% passing
2. **CASE expressions work** - 42_case passed
3. **Schema management is the biggest blocker** - Issues #7, #8 affect multiple tiers
4. **Critical runtime bug** - Issue #5 (block_on panic) affects error handling
5. **Test specs need `--schemas ../schemas`** - Schema auto-discovery doesn't work consistently

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
