# Velostream Phase 8 Development Plan: ROWS WINDOW Implementation

**Last Updated**: October 30, 2025 (07:15 UTC)
**Status**: Phase 8 COMPLETE - All Tasks Finished ✅
**Reference**: FR-078 ROWS WINDOW Buffer Execution - Exclusive ROWS WINDOW BUFFER Syntax Only

---

## Overview

This document tracks outstanding development work for ROWS WINDOW execution (FR-078) with exclusive support for ROWS WINDOW BUFFER syntax. Previous phases (1-7) focused on traditional window frame analysis; Phase 8 implements the new memory-safe ROWS WINDOW architecture.

### Completed Phases
- ✅ **Phase 1-5**: Traditional window frame analysis and gap analysis (completed in previous work)
- ✅ **Phase 8.1**: ROWS WINDOW BUFFER Parser Implementation (October 29, 2025)
  - Implemented exclusive ROWS WINDOW BUFFER syntax in parse_over_clause()
  - Removed traditional PARTITION BY/ORDER BY fallback paths
  - Fixed all WindowSpec::Rows type mismatches
  - Added window_spec: None field to 14 test cases
  - **Compilation Status**: ✅ CLEAN (no errors)
- ✅ **Phase 8.2**: RowsWindowState Implementation & Window Processing (October 30, 2025)
  - Created RowsWindowState with VecDeque buffer + BTreeMap ranking index
  - Implemented process_rows_window() with time-gap detection
  - Implemented incremental RANK/DENSE_RANK/PERCENT_RANK computation
  - **Status**: COMPLETE
- ✅ **Phase 8.3**: ProcessorContext Enhancement for Rows Window State Tracking (October 30, 2025)
  - Enhanced ProcessorContext with rows_window_states field
  - Integrated state management into query execution pipeline
  - **Status**: COMPLETE
- ✅ **Phase 8.4**: Comprehensive Unit Tests & Pre-commit Validation (October 30, 2025)
  - Created 11 comprehensive unit tests in rows_window_test.rs
  - All tests compiled and passing
  - Pre-commit validation: 365 unit tests passing, formatting clean
  - **Status**: COMPLETE

---

## Phase 8 Completion Summary (October 30, 2025)

**Status**: ✅ ALL PHASE 8 TASKS COMPLETE

### Key Achievements
1. **Parser Implementation** (Phase 8.1)
   - Exclusive ROWS WINDOW BUFFER syntax support
   - Clean compilation with zero errors

2. **State Management** (Phase 8.2)
   - VecDeque-based row buffer with configurable capacity
   - BTreeMap-based ranking index for efficient rank computation
   - Time-gap detection with millisecond precision
   - Incremental RANK/DENSE_RANK/PERCENT_RANK computation

3. **Context Enhancement** (Phase 8.3)
   - ProcessorContext integration with rows_window_states HashMap
   - State persistence and recovery mechanisms

4. **Testing & Validation** (Phase 8.4)
   - 11 comprehensive unit tests in rows_window_test.rs
   - All tests passing
   - Pre-commit validation: 365 unit tests passing
   - Code formatting: clean

### Files Modified
- `src/velostream/sql/execution/processors/window.rs` - Core implementation
- `src/velostream/sql/execution/processors/context.rs` - State management
- `src/velostream/sql/ast.rs` - Parser structures
- `tests/unit/sql/execution/processors/window/rows_window_test.rs` - NEW
- `tests/unit/sql/execution/processors/window/mod.rs` - Registration

### Test Coverage
- Buffer initialization and operations: ✅
- Buffer overflow detection: ✅
- Partition isolation: ✅
- Ranking functions (RANK, DENSE_RANK, PERCENT_RANK): ✅
- Time-gap detection: ✅
- Emission strategies (EveryRecord, BufferFull): ✅

### Validation Results
```
✅ Code formatting: PASSED
✅ Compilation: PASSED (0 errors)
✅ Clippy linting: PASSED
✅ Unit tests: 365 PASSED
✅ Pre-commit checks: ALL PASSED
```

---

## Work Summary

| Phase | Task | Documentation | Est. Hours | Status |
|-------|------|-----------------|-----------|--------|
| **8.1** | ROWS WINDOW BUFFER Parser Implementation | FR-078 Phase 8.1 | 3-4 | ✅ **COMPLETE** |
| **8.2** | RowsWindowState & Window Processing | FR-078 Phase 8.2 | 4-6 | ✅ **COMPLETE** |
| 8.2.1 | Create RowsWindowState struct with VecDeque buffer | window.rs | 2-3 | ✅ **COMPLETE** |
| 8.2.2 | Implement process_rows_window() with gap detection | window.rs | 2-3 | ✅ **COMPLETE** |
| 8.2.3 | Implement rank computation (RANK/DENSE_RANK/PERCENT_RANK) | window.rs | 1-2 | ✅ **COMPLETE** |
| **8.3** | ProcessorContext Enhancement | FR-078 Phase 8.3 | 2-3 | ✅ **COMPLETE** |
| **8.4** | Unit Tests & Pre-commit Validation | FR-078 Phase 8.4 | 3-5 | ✅ **COMPLETE** |
| **TOTAL (Phase 8)** | | | **18-26** | ✅ **ALL COMPLETE** |

---

## Phase 8.1: ROWS WINDOW BUFFER Parser Implementation - COMPLETE ✅

**Completed**: October 29, 2025 (22:45 UTC)
**Documentation**: `/docs/feature/FR-078-PHASE8-ROWS-WINDOW.md`
**Reference**: Architectural Requirement: "we are NOT going to support this without ROWS WINDOW BUFFER"
**Compilation Status**: ✅ CLEAN (no errors, 323 warnings only)

### Summary
Successfully implemented exclusive ROWS WINDOW BUFFER syntax parser that ONLY accepts row-count-based window specifications with explicit buffer sizes. All traditional PARTITION BY/ORDER BY fallback syntax has been removed per user requirement.

### What Was Completed ✅
1. **Parser Implementation** (`src/velostream/sql/parser.rs:4095-4225`)
   - Implemented `parse_over_clause()` to ONLY accept ROWS WINDOW BUFFER syntax
   - Removed all traditional PARTITION BY/ORDER BY fallback paths
   - Parser expects exact sequence: `OVER (ROWS WINDOW BUFFER <size> ROWS [optional clauses])`
   - Added support for optional PARTITION BY, ORDER BY, window frames, and EMIT modes

2. **AST Enhancement** (`src/velostream/sql/ast.rs:515-528`)
   - Added `window_spec: Option<Box<WindowSpec>>` field to OverClause struct
   - Enables embedding WindowSpec::Rows directly in OVER clauses
   - Maintains backward compatibility with optional field

3. **WindowSpec::Rows Definition** (`src/velostream/sql/ast.rs:484-499`)
   - buffer_size: u32 (e.g., 100, 1000)
   - partition_by: Vec<Expr> (for logical partitioning)
   - order_by: Vec<OrderByExpr> (for ranking/LAG-LEAD)
   - time_gap: Option<Duration> (for session-aware semantics)
   - window_frame: Option<WindowFrame> (for frame specifications)
   - emit_mode: RowsEmitMode (EveryRecord or BufferFull)

4. **Type System Fixes**
   - Fixed WindowSpec::Rows field type mismatches (3 compilation errors resolved)
   - Updated 14 test case OverClause initializations with `window_spec: None`
   - All type errors eliminated through proper unwrap_or_default() handling

### Syntax Example
```rust
// ONLY supported syntax going forward:
SELECT
    symbol,
    price,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
        EMIT EVERY RECORD
    ) as moving_avg
FROM trades
WHERE status = 'active';
```

### Files Modified
1. `src/velostream/sql/parser.rs` - parse_over_clause() implementation (130 lines)
2. `src/velostream/sql/ast.rs` - OverClause struct enhancement
3. `src/velostream/sql/execution/validation/window_frame_validator.rs` - Test updates (2 cases)
4. `tests/unit/sql/execution/expression/enhanced_window_functions_test.rs` - Field additions (14 occurrences)

### Compilation Status
```
✅ Compiling velostream v0.1.0
✅ Finished `dev` profile [unoptimized + debuginfo] target(s) in 7.78s
```
- **Errors**: 0
- **Warnings**: 323 (pre-existing, non-blocking)
- **Type Mismatches**: 0 (fixed with unwrap_or_default())

### Next Step: Phase 8.2 - RowsWindowState Implementation

---

## Phase 6: Integration Debugging and window_frame Propagation - IN PROGRESS 🔄

**Started**: October 29, 2025
**Documentation**: `/docs/feature/fr-078-window-frame-bounds-analysis.md` (Phase 6 section)
**Estimated Effort**: 4-6 hours

### Task 6.1: Trace OverClause Pipeline (2-3 hours)
**Goal**: Verify how OverClause flows from parser through SELECT processor to window functions

**Investigation Steps**:
1. Trace parser output of OverClause with populated window_frame
2. Track OverClause through SELECT processor execution path
3. Verify OverClause reaches window function context creation
4. Identify where window_frame field becomes None

**Key Code Locations**:
- `select.rs` - SELECT query processor
- `window_functions.rs:120-170` - `create_window_context()` function
- Window function evaluation points

### Task 6.2: Identify window_frame Loss Point (1-2 hours)
**Goal**: Find where/why window_frame is cleared or replaced in the pipeline

**Debugging Approach**:
- Add debug logging at OverClause extraction points
- Log window_frame value at each pipeline stage
- Identify transformation that clears the field
- Check for WindowSpec vs OverClause confusion

### Task 6.3: Implement Integration Fix (1-2 hours)
**Goal**: Ensure parsed window_frame flows through to window function evaluation

**Implementation Steps**:
1. Fix data flow bottleneck (likely simple fix)
2. Verify frame_bounds are correctly calculated
3. Confirm window functions use frame bounds
4. Run window_frame_execution tests to validate

---

## Priority 1 - Phase 7: Add Value Assertions to 194 Parsing-Only SQL Tests

### Issue Discovery
Comprehensive test audit reveals **61% of SQL tests (194/318) only validate SQL parsing, NOT computed values**. This is a systematic test quality issue across the entire codebase.

### Test Quality Breakdown
- **194 tests (61%)** - Parsing-only (HIGH RISK)
- **124 tests (39%)** - Have value assertions (GOOD)
- **83 tests (26%)** - ZERO assertions whatsoever

### Critical Test Files (83 parsing-only tests)

**Tier 1 - Most Critical (8 files, 83 tests)**:
1. `tests/unit/sql/execution/processors/window/window_edge_cases_test.rs` (15 tests)
2. `tests/unit/sql/execution/processors/window/statistical_functions_test.rs` (12 tests)
3. `tests/unit/sql/execution/processors/window/complex_having_clauses_test.rs` (6 tests)
4. `tests/unit/sql/execution/processors/window/emit_changes_late_data_semantics_test.rs` (8 tests)
5. `tests/unit/sql/execution/processors/window/window_frame_execution_test.rs` (9 tests)
6. `tests/unit/sql/execution/processors/window/session_window_functions_test.rs` (8 tests)
7. `tests/unit/sql/execution/processors/window/fr079_phase1_detection_test.rs` (22 tests)
8. `tests/unit/sql/execution/processors/window/timebased_joins_test.rs` (3 tests)

**Tier 2 - Mixed Quality (3 files, 43 tests with 50-79% assertions)**:
- `fr079_aggregate_expressions_test.rs` (77% parsing-only)
- `window_gaps_test.rs` (67% parsing-only)
- `emit_changes_advanced_test.rs` (56% parsing-only)

### Reference Models (Best Practices)
These files show excellent value assertion patterns to copy from:
- `tests/unit/sql/execution/aggregation/functions_test.rs` (100% assertions)
- `tests/unit/sql/execution/aggregation/group_by_test.rs` (90% assertions)
- `tests/unit/sql/execution/processors/window/emit_changes_basic_test.rs` (100% assertions)

### Pattern: Before vs After

**❌ BEFORE (Parsing-Only - Insufficient)**:
```rust
#[tokio::test]
async fn test_window_aggregation() {
    let results = SqlExecutor::execute_query(sql, records).await;
    WindowTestAssertions::print_results(&results, "Debug output");
    // ❌ No actual assertions! Bug could be hiding here.
}
```

**✅ AFTER (With Value Assertions - Comprehensive)**:
```rust
#[tokio::test]
async fn test_window_aggregation() {
    let results = SqlExecutor::execute_query(sql, records).await;
    assert!(!results.is_empty(), "Should produce results");

    if let Some(record) = results.first() {
        // ✅ Validate actual computed values
        assert_eq!(
            record.fields.get("sum_amount"),
            Some(&FieldValue::Float(150.0)),
            "SUM should be 150.0 for window [10, 20, 30, 40, 50]"
        );
        assert_eq!(
            record.fields.get("count_rows"),
            Some(&FieldValue::Integer(3)),
            "COUNT should be 3"
        );
    }
}
```

### Implementation Strategy

**Phase 1 - High-Risk Tests (Tier 1, 83 tests)**:
- Add 2-3 value assertions per test
- Focus on: window boundaries, aggregation results, watermark behavior
- Estimated effort: 2-4 weeks
- Expected result: +26 percentage points (39% → 65% coverage)

**Phase 2 - Medium-Risk Tests (Tier 2, 43 tests)**:
- Add additional assertions to partially-tested functions
- Estimated effort: 1-2 weeks
- Expected result: +14 percentage points (65% → 79% coverage)

**Phase 3 - Remaining Tests (remaining test files)**:
- Systematic audit and enhancement
- Estimated effort: 1 week
- Expected result: +6 percentage points (79% → 85% coverage)

### Business Impact

**Current Risk: HIGH**
- Bugs in aggregation logic undetected
- Window semantics not validated at all
- Watermark behavior not verified
- Statistical functions not mathematically checked
- Edge cases silently fail

**After Implementation: LOW**
- All computational bugs caught immediately
- Complete semantic validation
- Regression detection enabled
- Edge cases properly verified

### Success Criteria
- Phase 1: All 83 Tier 1 tests have at least 2 value assertions
- Phase 2: All 43 Tier 2 tests have ≥80% assertion coverage
- Overall: Achieve 85%+ value assertion coverage across all SQL tests

---

## Priority 2: Window Frame Execution Implementation

### Issue
Window frame specifications (ROWS BETWEEN, RANGE BETWEEN) are parsed correctly but **NOT applied during query execution**. Tests in `window_frame_execution_test.rs` are detecting real bugs in aggregation calculations.

### Root Cause Analysis
**File**: `src/velostream/sql/execution/expression/window_functions.rs` (lines 281-353)
- `calculate_frame_bounds()` function exists but is **NEVER CALLED**
- Window frame specification is parsed but discarded during execution

**File**: `src/velostream/sql/execution/aggregation/accumulator.rs`
- `process_record_into_accumulator()` includes **ALL records** in aggregation
- No filtering by window frame bounds (start_offset, end_offset)
- Rebuild aggregations for each row position considering only frame-filtered records

### Test Evidence
**Test File**: `tests/unit/sql/execution/processors/window/window_frame_execution_test.rs`
**Test Count**: 9 async functions with value assertions

**Example Failure**:
```
Test: test_rows_between_unbounded_preceding_execution
Row 2 calculation:
  Expected: AVG(10.0, 20.0) = 15.0  (rows 1-2 in UNBOUNDED PRECEDING frame)
  Actual: 20.0 (only current row or miscalculated)
  Status: FAILS ❌
```

### Implementation Tasks

#### Task 1.1: Integrate Frame Bounds into Aggregation Pipeline
- [ ] **File**: `src/velostream/sql/execution/aggregation/accumulator.rs`
- [ ] **Function**: `process_record_into_accumulator()`
- [ ] **Change**: Before aggregating a record, calculate frame bounds using `calculate_frame_bounds()`
- [ ] **Complexity**: Medium
- [ ] **Estimated Effort**: 4-6 hours

#### Task 1.2: Handle Frame Boundary Edge Cases
- [ ] **File**: `src/velostream/sql/execution/aggregation/accumulator.rs`
- [ ] **Cases**:
  - UNBOUNDED PRECEDING: include all rows from start to current
  - CURRENT ROW: only current row
  - UNBOUNDED FOLLOWING: include current row to end
  - PRECEDING(n): last n rows before current
  - FOLLOWING(n): next n rows after current
- [ ] **Complexity**: Medium-High
- [ ] **Estimated Effort**: 3-4 hours

#### Task 1.3: Verify Against Test Suite
- [ ] **Test File**: `window_frame_execution_test.rs` (9 tests)
- [ ] **Success Criteria**: All 9 tests pass with correct value assertions
- [ ] **Complexity**: Low (tests already exist)
- [ ] **Estimated Effort**: 1-2 hours (will iterate with implementation)

**Total Priority 2 Effort**: ~8-12 hours

---

## Priority 3: SQL Parser Gap Fixes

### Issue
5 critical gaps affecting 5 SQL demo/example files. Parser rejects valid SQL syntax.

### Gap 1: CREATE STREAM...WITH Configuration Syntax

**Files Affected** (3 files):
- `examples/ecommerce_analytics.sql`
- `examples/iot_monitoring.sql`
- `examples/social_media_analytics.sql`

**Error**: `Expected As, found With`

#### Tasks:
- [ ] Task 2.1.1: Extend parser for WITH clause in CREATE STREAM
  - [ ] Estimated Effort: 3-4 hours
- [ ] Task 2.1.2: Implement WITH configuration parser (key-value pairs)
  - [ ] Estimated Effort: 2-3 hours
- [ ] Task 2.1.3: Validate against 3 affected SQL files
  - [ ] Estimated Effort: 1 hour

**Subtotal Gap 1**: ~6-8 hours

---

### Gap 2: WITH Clause Property Configuration

**Files Affected** (1 file):
- `examples/file_processing_sql_demo.sql`

**Error**: `Expected String, found Identifier`

#### Tasks:
- [ ] Task 2.2.1: Enhance parser for contextual property blocks
  - [ ] Estimated Effort: 2-3 hours
- [ ] Task 2.2.2: Validate against test file
  - [ ] Estimated Effort: 0.5 hours

**Subtotal Gap 2**: ~2-3.5 hours

---

### Gap 3: Special Character Handling (Colon)

**Files Affected** (1 file):
- `examples/iot_monitoring_with_metrics.sql`

**Error**: `Unexpected character ':' at position 10247`

#### Tasks:
- [ ] Task 2.3.1: Debug and locate exact colon usage
  - [ ] Estimated Effort: 0.5 hours
- [ ] Task 2.3.2: Implement targeted colon support
  - [ ] Estimated Effort: 1-2 hours
- [ ] Task 2.3.3: Validate against test file
  - [ ] Estimated Effort: 0.5 hours

**Subtotal Gap 3**: ~2-3 hours

---

## Priority 4: Pre-Commit Verification & Reporting

### Task 3.1: Complete Pre-Commit Verification Suite
- [ ] Code formatting check
- [ ] Compilation check
- [ ] Clippy linting
- [ ] Unit tests
- [ ] Full test suite
- [ ] Example compilation
- [ ] Binary compilation
- [ ] Documentation tests
- [ ] Estimated Effort**: 1-2 hours

### Task 3.2: Generate Test Coverage Report
- [ ] Create summary of all test results
- [ ] Document pass/fail status for each module
- [ ] Include metrics (total tests, pass rate, coverage)
- [ ] **Estimated Effort**: 1-2 hours

### Task 3.3: Git Commit Phase 2-3 Work
- [ ] Stage all changes
- [ ] Create commit with detailed message
- [ ] **Estimated Effort**: 0.5 hours

---

## Testing & Validation

### Window Frame Tests
**File**: `tests/unit/sql/execution/processors/window/window_frame_execution_test.rs`
- 9 test functions with explicit expected values
- Value assertions validate actual aggregation calculations

### Parser Validation
**Files**: 5 demo/example SQL files requiring parser fixes

Run after parser implementation:
```bash
cargo build --examples --no-default-features
cargo run --bin velo-cli validate examples/ecommerce_analytics.sql
```

---

## SQL Parser Gaps - Detailed Analysis

### Gap 1: CREATE STREAM...WITH Configuration (3 files, 72% demo compatibility)

**Files Affected**:
- `examples/ecommerce_analytics.sql`
- `examples/iot_monitoring.sql`
- `examples/social_media_analytics.sql`

**Error**: `Expected As, found With`

**Current Issue**:
```sql
-- Currently FAILS
CREATE STREAM orders WITH (
    config_file = 'examples/configs/orders_topic.config',
    format = 'json',
    compression = 'gzip'
);
```

**Parser Currently Accepts**:
```sql
CREATE STREAM orders AS
SELECT * FROM kafka_source;
```

**Solution**: Implement full `CREATE STREAM ... WITH (property = value, ...) ...` syntax support

**Impact**: Medium - blocks configuration-driven stream definitions

---

### Gap 2: WITH Clause Property Configuration (1 file)

**Files Affected**:
- `examples/file_processing_sql_demo.sql`

**Error**: `Expected String, found Identifier`

**Current Issue**:
```sql
WITH (
    format = 'jsonlines',
    compression = 'gzip',
    batch_size = 1000
)
```

**Solution**: Enhance parser for property-style WITH clauses with contextual parsing

**Impact**: Medium - affects configuration blocks in various contexts

---

### Gap 3: Special Character Handling - Colon (1 file)

**Files Affected**:
- `examples/iot_monitoring_with_metrics.sql`

**Error**: `Unexpected character ':' at position 10247`

**Likely Causes**:
- YAML-style configuration blocks (key: value)
- URL specifications (http://...)
- Type annotations or namespacing
- Time specifications (12:34:56)

**Solution**: Debug exact context and implement targeted colon support

**Impact**: Low-Medium - context-specific, requires investigation

---

## Phase 1 Validation Results Summary

**Date**: October 28, 2025
**Total Files Tested**: 18
**Passed**: 13 (72.2%)
**Failed**: 5 (27.8%)

### Successful Parses (13 files)
- ✅ enhanced_sql_demo.sql
- ✅ simple_test.sql
- ✅ test_kafka.sql
- ✅ financial_trading.sql
- ✅ ctas_file_trading.sql
- ✅ ecommerce_analytics_phase4.sql
- ✅ ecommerce_with_metrics.sql
- ✅ financial_trading_with_metrics.sql
- ✅ iot_monitoring_phase4.sql
- ✅ social_media_analytics_phase4.sql
- ✅ test_emit_changes.sql
- ✅ test_simple_validation.sql
- ✅ test_parsing_error.sql (intentionally invalid)

### Failed Parses (5 files)
- ❌ file_processing_sql_demo.sql (Gap 2: WITH clause syntax)
- ❌ ecommerce_analytics.sql (Gap 1: CREATE STREAM...WITH)
- ❌ iot_monitoring.sql (Gap 1: CREATE STREAM...WITH)
- ❌ iot_monitoring_with_metrics.sql (Gap 3: Special character handling)
- ❌ social_media_analytics.sql (Gap 1: CREATE STREAM...WITH)

---

## Phase 2 Test Coverage Summary

### Test Files Created (41 tests)
1. **Session Window Functions** (8 tests)
   - `tests/unit/sql/execution/processors/window/session_window_functions_test.rs`
   - Tests: SESSION_DURATION, SESSION_START, SESSION_END
   - Status: ✅ 8/8 PASSING

2. **Statistical Functions** (14 tests)
   - `tests/unit/sql/execution/processors/window/statistical_functions_test.rs`
   - Tests: PERCENTILE_CONT, PERCENTILE_DISC, STDDEV, VARIANCE + value assertions
   - Status: ✅ 14/14 PASSING (9 parser + 5 value assertions)

3. **Complex HAVING Clauses** (10 tests)
   - `tests/unit/sql/execution/processors/window/complex_having_clauses_test.rs`
   - Tests: Advanced aggregation filtering with complex boolean logic
   - Status: ✅ 10/10 COMPILED

4. **Temporal JOINs** (9 tests)
   - `tests/unit/sql/execution/processors/window/timebased_joins_test.rs`
   - Tests: Time-based JOIN patterns with BETWEEN constraints
   - Status: ✅ 9/9 COMPILED

### Phase 2 Key Achievements
- Implemented value assertion testing pattern (floating-point tolerance < 0.001)
- Discovered SQL syntax requirement: GROUP BY must precede WINDOW clause
- Created comprehensive test infrastructure for advanced SQL features
- Total new tests: 41 (31 passing + 10 compiled)

---

## References

### Key Documentation Files
- `CLAUDE.md` - Project guidelines, development commands, and architecture principles
- Updated test modules in `tests/unit/sql/execution/processors/window/mod.rs`
- Updated validation tests in `tests/unit/sql/validation/mod.rs`

### Test Files
- `tests/unit/sql/execution/processors/window/window_frame_execution_test.rs` (9 tests)
- `tests/unit/sql/execution/processors/window/session_window_functions_test.rs` (8 tests)
- `tests/unit/sql/execution/processors/window/statistical_functions_test.rs` (14 tests)
- `tests/unit/sql/execution/processors/window/complex_having_clauses_test.rs` (10 tests)
- `tests/unit/sql/execution/processors/window/timebased_joins_test.rs` (9 tests)

### Implementation Files (Targets)
- `src/velostream/sql/execution/expression/window_functions.rs` (lines 281-353: calculate_frame_bounds)
- `src/velostream/sql/execution/aggregation/accumulator.rs` (process_record_into_accumulator)
- `src/velostream/sql/parser.rs` (CREATE STREAM, WITH clause, colon handling)

---

## Success Criteria

✅ Window Frame Implementation
- All 9 window_frame_execution_test.rs tests PASS
- Value assertions match expected calculations
- No regression in existing tests

✅ Parser Gap Fixes
- All 5 demo/example files parse without errors
- No regression in existing parser tests

✅ Pre-Commit Verification
- All formatting, compilation, and test checks pass
- Code ready for merge to main branch
