# Velostream Phase 4 Development Plan

**Last Updated**: October 29, 2025
**Status**: Ready for implementation
**Reference**: Phase 1-3 completed with window frame execution tests and SQL gap analysis

---

## Overview

This document tracks outstanding development work identified during Phase 1-3 testing and gap analysis. Work is organized by priority and implementation complexity.

### Completed Phases
- ✅ **Phase 1**: SQL validation of 18 demo/example files (72.2% pass rate)
- ✅ **Phase 2**: Created 41 new test cases validating advanced SQL features (31 passing, 10 compiled)
- ✅ **Phase 3**: Comprehensive gap analysis with prioritized recommendations
- 🔄 **Phase 4**: Implementation of identified gaps and pre-commit verification (IN PROGRESS)

---

## Priority 1: Critical - Window Frame Execution Implementation

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

**Total Priority 1 Effort**: ~8-12 hours

---

## Priority 2: High - SQL Parser Gap Fixes

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

## Priority 3: Phase 4 Pre-Commit Verification & Reporting

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

## Work Summary

| Priority | Task | Files | Est. Hours | Status |
|----------|------|-------|-----------|--------|
| 1 | Window Frame Execution | aggregation, window_functions | 8-12 | Pending |
| 2.1 | CREATE STREAM...WITH | parser.rs | 6-8 | Pending |
| 2.2 | WITH Clause Properties | parser.rs | 2-3.5 | Pending |
| 2.3 | Colon Character | parser.rs | 2-3 | Pending |
| 3 | Pre-Commit & Report | all | 2-4 | Pending |
| **TOTAL** | | | **20.5-30.5** | |

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

## References

### Documentation
- `docs/sql-feature-gaps.md` - Comprehensive gap analysis
- `CLAUDE.md` - Project guidelines and development commands

### Test Files
- `tests/unit/sql/execution/processors/window/window_frame_execution_test.rs`
- `tests/unit/sql/execution/processors/window/session_window_functions_test.rs`
- `tests/unit/sql/execution/processors/window/statistical_functions_test.rs`
- `tests/unit/sql/execution/processors/window/complex_having_clauses_test.rs`
- `tests/unit/sql/execution/processors/window/timebased_joins_test.rs`

### Implementation Files
- `src/velostream/sql/execution/expression/window_functions.rs` (lines 281-353)
- `src/velostream/sql/execution/aggregation/accumulator.rs`
- `src/velostream/sql/parser.rs`

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
