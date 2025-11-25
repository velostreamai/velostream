# FR-078: Phase 6 Investigation Summary - Window Frame Execution Architecture

**Date**: October 29, 2025
**Status**: 🔍 INVESTIGATION COMPLETE - Critical Architectural Issues Identified
**Phase**: 6 (Integration Debugging)

---

## Executive Summary

Phase 6 investigation revealed that **window frame execution failure is NOT due to data propagation issues**, but rather **fundamental architectural limitations** in how window functions are evaluated. The system evaluates window functions in isolation with **empty buffers**, making frame-based filtering impossible.

**Key Finding**: The problem is not that `window_frame` is lost; it's that window functions never receive the data needed to apply frame bounds.

---

## Phase 6 Investigation Scope

### 6.1: Parser AST Analysis ✅ COMPLETE

**Finding**: Parser is fully correct.

- `window_frame` field in `OverClause` is properly defined in `/src/velostream/sql/ast.rs:484-491`
- `parse_over_clause()` correctly initializes window_frame as `None` then populates it via `parse_window_frame()` if ROWS/RANGE tokens present
- Window frame variants (ROWS, RANGE, INTERVAL bounds) are all properly parsed
- All parser tests pass, including INTERVAL syntax tests (advanced_window_test.rs)

**Status**: ✅ No issues found at parse time

### 6.2: OverClause Propagation Trace ✅ COMPLETE

**Finding**: `OverClause` with `window_frame` successfully flows through entire system.

**Code Flow Verified**:
1. ✅ **Parser** (`parser.rs:4096-4215`): Creates `OverClause { window_frame: Some(...) }`
2. ✅ **AST** (`ast.rs:484-491`): `Expr::WindowFunction` includes `over_clause` field
3. ✅ **Evaluator** (`evaluator.rs`: lines where Expr::WindowFunction is matched): `over_clause` passed to window function evaluation
4. ✅ **Window Functions** (`window_functions.rs:76-118`): `over_clause` received and `window_frame` extracted
5. ✅ **Frame Calculator** (`window_functions.rs:281-353`): `calculate_frame_bounds()` receives `&over_clause.window_frame`

**Status**: ✅ No propagation gap exists

---

## Phase 6: The Real Problem - Architectural Limitations

### Problem 1: Empty Window Buffer ⚠️ CRITICAL

**Location**: `select.rs` (window function evaluation)

**Issue**: Window functions are evaluated with an **empty buffer**:

```rust
// From agent investigation
let empty_buffer: Vec<crate::velostream::sql::execution::StreamRecord> = Vec::new();
super::WindowFunctions::evaluate_window_function(
    function_name,
    args,
    over_clause,
    record,
    &empty_buffer,  // <-- ALWAYS EMPTY, NO RELATED ROWS
)
```

**Impact**: Window functions cannot access related rows in the window, making:
- Frame bounds impossible to apply (no other rows to filter)
- LAG/LEAD impossible to implement correctly (can't access previous/next rows)
- Window aggregates impossible to compute (only have current row)

### Problem 2: Single-Record Evaluation ⚠️ CRITICAL

**Location**: `window_functions.rs:127-130`

**Issue**: Even if frame_bounds are calculated, only current record is available:

```rust
let mut ordered_buffer = window_buffer.to_vec();  // Empty
ordered_buffer.push(current_record.clone());      // Only current record
```

**Impact**: Frame bounds calculated relative to a buffer with only 1 record are meaningless.

### Problem 3: Frame Bounds Calculated But Unused ⚠️ CRITICAL

**Location**: `window_functions.rs:355-427` (LAG/LEAD functions)

**Issue**: `frame_bounds` are calculated in `WindowContext` but never referenced in aggregation logic:

```rust
// calculate_frame_bounds() is called and frame_bounds set in WindowContext
// BUT the LAG/LEAD evaluation logic (lines 355-427) NEVER checks or uses frame_bounds
```

**Impact**: Even if data were available, window functions ignore the calculated frame specifications.

---

## Root Cause Analysis: Architectural Design

The current architecture evaluates **all expressions (including window functions) row-by-row**:

```
Input: StreamRecord[]
       ↓
For each record:
  ├─ Evaluate WHERE clause
  ├─ Evaluate non-window SELECT fields
  ├─ Evaluate window functions  ← HERE: empty buffer, no access to related rows
  └─ Emit result record
```

This works fine for scalar expressions but breaks for window functions which **require access to multiple rows**.

### What Should Happen (SQL Standard)

```
Input: StreamRecord[]
       ↓
PARTITION BY → Group records by partition key
       ↓
ORDER BY → Sort within each partition
       ↓
For each partition:
  ├─ Calculate window frames (using parsed window_frame spec)
  ├─ For each row in partition:
  │  └─ Apply window function over frame bounds (NOT empty, real data)
  └─ Emit results
```

---

## Why This Matters

### Current Behavior
- Window functions receive no data to work with
- Frame bounds are calculated but never applied
- All window operations fail silently or return NULL

### Required Architecture
- Window processor needs access to **buffered data** for entire windows/partitions
- Frame bounds need to be applied **when data is available**
- Separate evaluation path for window functions vs scalar expressions

---

## Why Previous Analysis Was Wrong

The FR-078 documentation claimed infrastructure was "95% complete" because:
- ✅ Parser correctly creates `WindowFrame` structures
- ✅ Frame bounds calculation is fully implemented
- ✅ Window function code checks for frame_bounds field

**But this ignored a critical fact**: The calculated `frame_bounds` are never actually **used** because the window function evaluation context lacks the data needed to apply them.

---

## Technical Implications

| Component | Current Status | Issue |
|-----------|---|---|
| Parser | ✅ Perfect | Correctly parses all frame syntax |
| AST | ✅ Complete | Proper structures defined |
| Frame Bounds Calculation | ✅ Implemented | All frame types handled correctly |
| Data Access | ❌ BROKEN | Empty buffer passed to window functions |
| Frame Application | ❌ NOT USED | Calculated bounds never referenced |
| Architecture | ❌ INCOMPATIBLE | Row-by-row evaluation incompatible with window functions |

---

## Options for Resolution

### Option 1: Full Architectural Redesign (8-12 weeks)
- Implement proper windowing processor with buffered data
- Support frame bounds with full data access
- Full window function support (LAG, LEAD, aggregates, etc.)
- **Trade-off**: Major refactoring, high effort, complete solution

### Option 2: Pragmatic Limitation (2-4 weeks)
- Document window frames as "not implemented" at execution level
- Remove empty buffer passing (prevents confusion)
- Focus on other SQL features first
- **Trade-off**: Incomplete feature, users can parse but not execute frames

### Option 3: Partial Implementation (4-6 weeks)
- Implement buffer passing for windowed aggregates only
- Support ROWS BETWEEN ... UNBOUNDED PRECEDING patterns
- Defer RANGE BETWEEN and INTERVAL bounds
- **Trade-off**: Limited functionality, incremental value

---

## Phase 6 Conclusion

**Investigation Finding**: Window frame bounds **cannot be implemented** in the current row-by-row evaluation architecture. The system needs a fundamental redesign to support buffered, partition-level evaluation.

**Status**: Phase 6 investigation complete. Root cause identified as architectural, not propagation.

**Recommendation**: Before implementing Phase 7 fixes, team should decide:
1. Is full window function support a priority?
2. If yes, accept major architectural refactoring
3. If no, document as limitation and move to other features

---

## Cross-References

- **FR-078 Documentation**: `/docs/feature/fr-078-window-frame-bounds-analysis.md`
- **Parser Tests**: `/tests/unit/sql/parser/window_frame_test.rs`
- **Window Function Code**: `/src/velostream/sql/execution/expression/window_functions.rs:76-550`
- **SELECT Processor**: `/src/velostream/sql/execution/processors/select.rs`
- **Investigation Files**: `/tmp/window_frame_flow_trace.md` (from agent analysis)

---

## Document Metadata

**Version**: 1.0
**Date**: 2025-10-29
**Status**: Investigation Complete
**Priority**: High (Blocks window frame feature)
**Effort Estimate**: 8-12 weeks (full fix) or document as limitation
**Impact**: Critical (affects complex SQL queries requiring window functions)
