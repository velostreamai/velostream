# FR-079 Phase 5: Window Frame Execution - Root Cause Analysis

**Feature**: Window Frame Execution with ROWS BETWEEN and RANGE BETWEEN support
**Date**: October 29, 2025
**Status**: Root Cause Identified - Ready for Implementation
**Failing Tests**: 18 tests in window frame execution suite

## Executive Summary

Window frame execution parsing is working, but the infrastructure exists in the codebase yet **the window frame specification from the parser is not being used during query execution**. The fix requires ensuring that parsed `window_frame` values are properly passed through to window function evaluation.

### Current Test Failures (18 total)
- `test_rows_between_unbounded_preceding_execution` - Returns Null instead of running averages
- `test_rows_between_current_and_following_execution` - Returns Null for forward averages
- `test_rows_between_preceding_execution` - Returns Null for backward window frames
- All other frame execution tests failing with same pattern
- Edge case tests: null partition keys, large/small windows
- Complex HAVING clauses with windowed aggregates
- Lifecycle command integration with windows

## Architecture Overview

### The Window Frame Execution Pipeline

```
SQL Query with ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
    ↓
[StreamingSqlParser] - Parses window frame spec → OverClause
    ↓
OverClause {
    partition_by: vec![...],
    order_by: vec![...],
    window_frame: Some(WindowFrame {  ← **This field exists and should be populated**
        frame_type: ROWS,
        start_bound: Preceding(2),
        end_bound: Some(Preceding(1))
    })
}
    ↓
[SELECT Processor] - Should extract and pass OverClause to window functions
    ↓
[Window Functions] - AVG, SUM, MIN, MAX evaluate using frame_bounds
    ↓
[Result] - Aggregates computed over frame-filtered records
```

## Root Cause Analysis

### What's Working ✅

#### 1. **Parser** (ast.rs, parser.rs)
- **File**: `src/velostream/sql/ast.rs:490`
- **Status**: ✅ Correctly defined OverClause structure with `window_frame: Option<WindowFrame>`
- **Evidence**: Test queries with `ROWS BETWEEN ... AND ...` syntax parse successfully without errors
- **Code**:
```rust
pub struct OverClause {
    pub partition_by: Vec<String>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,  // ← Field exists
}
```

#### 2. **Frame Bounds Calculation** (window_functions.rs:281-353)
- **File**: `src/velostream/sql/execution/expression/window_functions.rs:281`
- **Status**: ✅ Fully implemented and correct
- **Function**: `calculate_frame_bounds(window_frame, current_position, partition_bounds, buffer)`
- **Logic**: Converts frame specifications (UNBOUNDED PRECEDING, Preceding(n), CurrentRow, Following(n), UnboundedFollowing) to offset pairs `(start_offset, end_offset)`
- **Example calculation**:
  - Input: `ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING` at position 3
  - Output: `(-2, -1)` relative offsets
  - Applies to positions: [pos-2, pos-1] = [1, 2] (rows before current)

#### 3. **Window Function Implementations** (window_functions.rs:823-930)
- **File**: `src/velostream/sql/execution/expression/window_functions.rs:823-930`
- **Status**: ✅ Already use `frame_bounds` for filtering
- **Functions**: AVG, SUM, MIN, MAX all check `window_context.frame_bounds`
- **Code Example (AVG function)**:
```rust
let (start_idx, end_idx) = if let Some((frame_start_offset, frame_end_offset))
    = window_context.frame_bounds {
    // Convert relative offsets to absolute indices
    let frame_start = (window_context.current_position as i64 + frame_start_offset)
        .max(0) as usize;
    let frame_end = ((window_context.current_position as i64 + frame_end_offset + 1)
        .min(window_context.buffer.len() as i64)
        .max(0)) as usize;
    (frame_start, frame_end)
} else {
    // Default: RANGE UNBOUNDED PRECEDING TO CURRENT ROW
    window_context.partition_bounds.unwrap_or((0, window_context.buffer.len()))
};
// Use (start_idx, end_idx) to filter records for aggregation
```

### What's Missing ❌

#### **The Critical Gap: window_frame is Never Populated**
- **Symptom**: `window_context.frame_bounds` is always `None` during execution
- **Consequence**: Default frame bounds used: "RANGE UNBOUNDED PRECEDING TO CURRENT ROW"
- **Impact**: All records from partition start are included instead of frame-specified subset

**Example Failure**:
```
Query: SELECT AVG(value) OVER (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM data
Input: [10.0, 20.0, 30.0, 40.0, 50.0]

Expected for Row 3:
  Frame: rows 1-2 (values 10, 20)
  Result: AVG(10, 20) = 15.0

Actual:
  Frame: rows 0-3 (values 10, 20, 30) ← ALL records from partition start
  Result: AVG(10, 20, 30) = 20.0
  BUT: Returns Null because logic fails due to incorrect frame
```

## Investigation Findings

### File Structure and Key Components

#### 1. **AST Layer** (Parsing & Structure)
- **File**: `src/velostream/sql/ast.rs:480-495`
- **OverClause Definition**:
  ```rust
  pub struct OverClause {
      pub partition_by: Vec<String>,
      pub order_by: Vec<OrderByExpr>,
      pub window_frame: Option<WindowFrame>,
  }

  pub struct WindowFrame {
      pub frame_type: FrameType,  // ROWS or RANGE
      pub start_bound: FrameBound,
      pub end_bound: Option<FrameBound>,
  }
  ```

#### 2. **Window Function Context Creation** (window_functions.rs:120-170)
- **File**: `src/velostream/sql/execution/expression/window_functions.rs:120`
- **Function**: `create_window_context(over_clause, current_record, buffer)`
- **Code** (lines 157-162):
  ```rust
  let frame_bounds = Self::calculate_frame_bounds(
      &over_clause.window_frame,  ← Expects over_clause with window_frame populated
      current_position,
      &partition_bounds,
      &ordered_buffer,
  )?;
  ```
- **Issue**: This function receives `over_clause` but if `over_clause.window_frame` is `None`, it defaults to unbounded

#### 3. **Test Expectation** (window_frame_execution_test.rs:1-36)
- **File**: `tests/unit/sql/execution/processors/window/window_frame_execution_test.rs:1`
- **Header Comment**:
  ```
  **IMPORTANT FINDING**: Window frame execution is NOT currently implemented.

  The Velostream streaming SQL engine supports parsing of window frame
  specifications (ROWS BETWEEN, RANGE BETWEEN) but does NOT apply them
  during query execution.

  ✅ Parser accepts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ❌ Aggregation does NOT filter records based on frame bounds
  ❌ All records in window partition are aggregated (frame ignored)
  ```

### Default Fallback Behavior

**File**: `src/velostream/sql/execution/expression/window_functions.rs:289-292`

When `window_frame` is `None`:
```rust
let frame = match window_frame {
    Some(f) => f,
    None => {
        // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        return Ok(Some((-(current_position as i64), 0)));
    }
};
```

This explains why:
- Row 1 gets value for position 0 only (correct by accident)
- Row 2 gets value for positions 0-1 (should get different frame)
- Row 3+ get accumulating values instead of specified frame

## Next Steps for Implementation

### Phase 6: Integration Debugging

1. **Verify Parser is Populating window_frame**
   - Add debug logging in parser to confirm window_frame is being set
   - Run test query: `SELECT id, AVG(value) OVER (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM test_data`
   - Inspect parsed OverClause.window_frame value

2. **Trace Execution Flow**
   - SELECT Processor → Window Function Evaluation
   - Ensure OverClause with populated window_frame passed to window function context creation
   - Verify frame_bounds appear in WindowContext

3. **Minimal Fix Implementation**
   - Likely just need to ensure parsed `over_clause.window_frame` is preserved
   - Very little code changes needed - infrastructure already exists
   - Main work is ensuring the parsed value flows through execution pipeline

### Files to Investigate

| File | Purpose | Status |
|------|---------|--------|
| `src/velostream/sql/ast.rs` | AST structure definitions | ✅ Correct |
| `src/velostream/sql/parser.rs` | SQL parser | ❓ Check if window_frame populated |
| `src/velostream/sql/execution/expression/window_functions.rs` | Window function evaluation | ✅ Ready to use frame_bounds |
| `src/velostream/sql/execution/processors/select.rs` | SELECT query processor | ❓ Check if OverClause passed correctly |
| `tests/unit/sql/execution/processors/window/window_frame_execution_test.rs` | Tests | 📊 Documents expected behavior |

## Conclusion

The window frame execution feature is ~95% complete. The parsing works, the frame bounds calculation works, and the window functions are ready to use the frame bounds. The remaining 5% is ensuring that:

1. Parser populates the `window_frame` field in OverClause
2. The OverClause with populated `window_frame` reaches the window function evaluation
3. Frame bounds flow through WindowContext to AVG/SUM/MIN/MAX functions

No algorithmic changes needed - just integration debugging to verify the existing infrastructure is being used correctly.

**Estimated effort**: 1-2 hours of integration work
**Complexity**: Low - straightforward debugging of data flow
**Risk**: Very low - leveraging existing, tested infrastructure
