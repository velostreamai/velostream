# FR-078: Phase 6 Root Cause Correction - NOT Architectural, Simple Data Flow Issue

**Date**: October 29, 2025 - Evening (Correction)
**Status**: ✅ ACTUAL ROOT CAUSE IDENTIFIED - Ready for Phase 7 Implementation
**Phase**: 6.3 (Architecture Analysis Correction)
**Implementation Effort**: 2-4 hours

---

## Critical Discovery: Not an Architecture Problem!

The initial Phase 6 analysis concluded it was a fundamental architectural limitation requiring 8-12 weeks of redesign. **This was WRONG**.

The actual problem is **much simpler**: The window buffer exists in `ProcessorContext.window_context` but is **not being passed** to window function evaluation.

---

## Where the Window State IS Held

**File**: `/src/velostream/sql/execution/processors/context.rs:29` and `124-131`

```rust
pub struct ProcessorContext {
    // ... other fields ...

    /// Window processing state (legacy - kept for compatibility)
    pub window_context: Option<WindowContext>,

    // ... more fields ...
}

/// Window processing context
pub struct WindowContext {
    /// Buffered records for windowing
    pub buffer: Vec<StreamRecord>,  // ← BUFFER IS HERE!
    /// Last emission time
    pub last_emit: i64,
    /// Should emit in this processing cycle
    pub should_emit: bool,
}
```

**The buffer EXISTS and can hold multiple records!**

---

## Where the Buffer IS Populated

**File**: `/src/velostream/sql/execution/engine.rs`

```rust
context.window_context = self.get_window_context_for_processors(query_id);
```

The engine sets up window_context with buffered data!

---

## Where the Buffer ISN'T Being Used

**File**: `/src/velostream/sql/execution/expression/evaluator.rs`

When window functions are evaluated, they call:

```rust
super::WindowFunctions::evaluate_window_function(
    function_name,
    args,
    over_clause,
    record,
    window_buffer,  // ← This is an EMPTY buffer, not from context!
)
```

The evaluator is passing an **empty local buffer** instead of using `context.window_context.buffer`!

---

## The Actual Fix (2-4 hours, not 8-12 weeks)

### Fix for Location 2 (PRIORITY - Line 1622 in evaluate_expression_value_with_subqueries)

This location has access to `context` parameter, so we can immediately use it:

```rust
// BEFORE (current, broken at line 1622):
let empty_buffer: Vec<crate::velostream::sql::execution::StreamRecord> = Vec::new();
super::WindowFunctions::evaluate_window_function(
    function_name,
    args,
    over_clause,
    record,
    &empty_buffer,  // ❌ EMPTY BUFFER
)

// AFTER (fix):
let window_buffer = context.window_context
    .as_ref()
    .map(|wc| &wc.buffer)
    .unwrap_or(&Vec::new());  // Fallback to empty if no context
super::WindowFunctions::evaluate_window_function(
    function_name,
    args,
    over_clause,
    record,
    window_buffer,  // ✅ POPULATED FROM CONTEXT
)
```

### Fix for Location 1 (Line 880 in evaluate_expression_value)

This location doesn't have `context` parameter. Two options:

**Option A: Add context parameter** (Recommended - cleaner)
```rust
// Change function signature to accept context
pub fn evaluate_expression_value(
    expr: &Expr,
    record: &StreamRecord,
    context: Option<&ProcessorContext>,  // ← ADD THIS
) -> Result<FieldValue, SqlError> {
    // Then use same logic as Location 2
}
```

**Option B: Use empty buffer** (Acceptable if Location 2 is the primary call site)
```rust
// Keep as-is for now, since Location 2 handles subqueries (primary use case)
let empty_buffer: Vec<crate::velostream::sql::execution::StreamRecord> = Vec::new();
// (Existing code)
```

**Recommendation**: Implement Option A (add context parameter) for consistency and future-proofing.

---

## Why Phase 6 Analysis Was Wrong

The initial analysis found:
1. ✅ Parser correctly populates `OverClause.window_frame`
2. ✅ `ProcessorContext.window_context` exists with a buffer
3. ✅ Frame bounds calculation is fully implemented
4. ✅ Window functions are ready to use frame bounds
5. ❌ "Empty buffer passed to window functions"

But concluded: "This is an architectural problem requiring redesign"

**WRONG CONCLUSION**: The infrastructure is already there! The problem is just not **connecting the pieces** in the evaluator.

---

## Actual Root Cause Chain

```
1. Engine populates ProcessorContext.window_context with buffer ✅
2. Evaluator has access to context ✅
3. But evaluator creates EMPTY buffer instead of using context ❌
4. Passes empty buffer to window function evaluation
5. Window functions can't access related rows → fails
```

**This is a 2-4 hour fix, not an architectural redesign!**

---

## What Was Right About Phase 6.1-6.2

✅ Parser AST analysis was correct
✅ OverClause propagation trace was correct
✅ Found the actual problem location (evaluator.rs)

❌ Wrong interpretation that it required architectural redesign

---

## Why the User's Question Was Key

User asked: **"Where is window state held? Is it in ProcessorContext?"**

Answer: **YES!** And that insight immediately revealed the problem wasn't architectural—the buffer already exists, we just need to pass it through properly.

---

## Phase 7 Implementation Plan

### Files to Modify (in priority order):

#### 1. PRIMARY FIX - `/src/velostream/sql/execution/expression/evaluator.rs:1622-1628`
**Lines**: 1622-1628
**Function**: `evaluate_expression_value_with_subqueries<T: SubqueryExecutor>()` (defined at line 1070)
**Action**: Replace empty buffer creation with context-based buffer extraction

```rust
// FROM (line 1622):
let empty_buffer: Vec<crate::velostream::sql::execution::StreamRecord> = Vec::new();

// TO:
let window_buffer = context.window_context
    .as_ref()
    .map(|wc| &wc.buffer)
    .unwrap_or(&Vec::new());
```

#### 2. SECONDARY FIX - `/src/velostream/sql/execution/expression/evaluator.rs:880-886`
**Lines**: 880-886
**Function**: `evaluate_expression_value()` (defined at line 393)
**Action**: One of two options:
- **Option A** (Preferred): Modify function signature to accept context parameter
- **Option B**: Accept limitation (empty buffer OK for non-subquery window calls)

### Already Correct (No Changes Needed):

#### Frame Bounds Calculation - `/src/velostream/sql/execution/expression/window_functions.rs:281-353`
**Lines**: 281-353
**Function**: `calculate_frame_bounds()`
**Status**: ✅ Already correctly calculates offsets from WindowFrame
**What Works**:
- Converts ROWS/RANGE specifications to relative offsets
- Handles all bound types (UnboundedPreceding, Preceding(n), CurrentRow, Following(n), etc.)
- Handles INTERVAL time-based bounds

#### Window Function Aggregates - `/src/velostream/sql/execution/expression/window_functions.rs:823-930`
**Lines**: 823-930 (AVG), 884-930 (SUM), and similar for MIN/MAX/COUNT
**Status**: ✅ Already checks and uses frame_bounds correctly
**What Works**:
- All aggregate functions extract frame_bounds from WindowContext
- Apply frame bounds to filter records for aggregation
- Proper default behavior when frame_bounds are None

### Testing After Fix:
- Existing window frame tests should pass once buffer flows through
- No architectural changes needed
- Backward compatible
- Performance impact: Negligible (just passing reference)

---

## Conclusion

**Phase 6 Discovery**: The window frame execution failure is NOT due to architectural limitations, but due to a simple data flow oversight in the evaluator not passing the buffered window context to window function evaluation.

**Estimated Fix Time**: 2-4 hours (not 8-12 weeks)
**Complexity**: Low (pass existing data through)
**Risk**: Very low (isolated change, infrastructure already complete)

---

## Document Metadata

**Version**: 1.0 (Correction to Phase 6 Analysis)
**Date**: 2025-10-29
**Status**: Root Cause Corrected
**Effort Estimate**: 2-4 hours implementation + testing
**Impact**: High - Enables full window frame support with minimal changes
