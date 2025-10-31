# FR-078: Window Function Frame Bounds - Known Limitation & Roadmap

**Date**: 2025-10-20
**Status**: 📋 DOCUMENTED LIMITATION (Ready for Phase 5 Implementation)
**Related Features**: SELECT Alias Reuse (✅ Complete), Subquery Support (⏳ In Progress)

---

## Overview

This document identifies and documents a known limitation in Velostream's window function implementation: **frame bounds are parsed but not executed correctly**. This limitation affects the demo SQL application (`financial_trading.sql`) and should be addressed in Phase 5 of the FR-078 roadmap.

---

## Problem Statement

### Parser vs. Execution Gap

Velostream's SQL parser **fully supports** window function frame bound syntax:
- ✅ ROWS BETWEEN syntax
- ✅ RANGE BETWEEN syntax
- ✅ UNBOUNDED PRECEDING / CURRENT ROW
- ✅ Numeric PRECEDING (e.g., 99 PRECEDING)
- ✅ INTERVAL-based bounds (e.g., INTERVAL '1' DAY PRECEDING)

However, the **execution engine ignores frame bounds** and processes all window functions over the entire partition.

### Example: Cumulative Sum

```sql
-- Expected: Running cumulative total
-- Row 1: 100
-- Row 2: 100 + 150 = 250
-- Row 3: 100 + 150 + 200 = 450
SELECT
    trader_id,
    current_pnl,
    SUM(current_pnl) OVER (
        PARTITION BY trader_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_pnl
FROM positions;

-- Actual: All rows show same value (total of entire partition)
-- Row 1: 450
-- Row 2: 450
-- Row 3: 450
```

---

## Root Cause Analysis - Phase 5 Investigation (October 29, 2025)

### Summary

**CORRECTED FINDINGS**: Recent investigation reveals the actual state of the codebase is significantly more advanced than previously documented. The window frame execution infrastructure is ~95% complete:

- ✅ **Parser**: Correctly parses window frame specifications (ROWS BETWEEN, RANGE BETWEEN)
- ✅ **Frame Bounds Calculation**: Fully implemented and correct (`calculate_frame_bounds()`)
- ✅ **Window Functions**: Ready to use frame bounds (AVG, SUM, MIN, MAX all check `frame_bounds`)
- ❌ **Critical Gap**: `window_frame` from parser is `None` during execution - not being propagated to window function evaluation

### Updated Architecture Analysis

#### File: `/src/velostream/sql/ast.rs:490`

**OverClause Structure** (✅ Correct):
```rust
pub struct OverClause {
    pub partition_by: Vec<String>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,  // ← Field exists and should be populated
}
```

The AST properly defines the window_frame field that should contain parsed frame specifications.

#### File: `/src/velostream/sql/execution/expression/window_functions.rs:281-353`

**Frame Bounds Calculation** (✅ Fully Implemented and Correct):

The `calculate_frame_bounds()` function is well-implemented and handles all frame types:
```rust
fn calculate_frame_bounds(
    window_frame: &Option<WindowFrame>,
    current_position: usize,
    partition_bounds: &Option<(usize, usize)>,
    buffer: &[StreamRecord],
) -> Result<Option<(i64, i64)>, SqlError> {
    let frame = match window_frame {
        Some(f) => f,
        None => {
            // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            return Ok(Some((-(current_position as i64), 0)));
        }
    };

    // Proper handling of frame types and bounds
    // Returns (start_offset, end_offset) relative to current position
}
```

**What Works**:
- Converts frame specifications to relative offset pairs (start_offset, end_offset)
- Handles all frame bound types: UnboundedPreceding, Preceding(n), CurrentRow, Following(n), UnboundedFollowing, IntervalPreceding, IntervalFollowing
- Returns proper frame bounds that can be used for filtering
- Default behavior when window_frame is None is correct

#### File: `/src/velostream/sql/execution/expression/window_functions.rs:823-930`

**Window Functions** (✅ Ready to Use Frame Bounds):

All aggregate functions already check `window_context.frame_bounds`:

**AVG() aggregate (lines 823-882)**:
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

**Status**: SUM (lines 884-930), MIN, MAX - all properly check and use frame_bounds.

**What Works**:
- Functions check `window_context.frame_bounds` field
- If frame_bounds exist, they're used to filter records
- If frame_bounds are None, proper default behavior applies
- All infrastructure is in place and correct

### The Critical Gap: window_frame Not Populated During Execution

**Problem**: When window functions are evaluated, `window_context.frame_bounds` is always `None`, causing default bounds to be used instead of parsed frame specifications.

**Root Cause**: The `window_frame` field in OverClause is parsed correctly but is `None` when it reaches window function context creation.

**Example Failure**:
```
Query: SELECT AVG(value) OVER (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM data
Input: [10.0, 20.0, 30.0, 40.0, 50.0]

Expected for Row 3:
  Frame: rows 1-2 (values 10, 20)
  Result: AVG(10, 20) = 15.0

Actual:
  Frame: rows 0-3 (full partition subset) due to None causing default bounds
  Result: Returns Null because incorrect frame applied
```

**Investigation Files**:
- Test expectations: `/tests/unit/sql/execution/processors/window/window_frame_execution_test.rs:1-36`
- Window function evaluation: `/src/velostream/sql/execution/expression/window_functions.rs:120-170` (`create_window_context()`)

### Why This Happened

The frame bounds calculation was implemented with the infrastructure to work correctly, but the parsed window frame specification is not being passed through the execution pipeline to window function context creation. This suggests a gap in:
1. How OverClause is extracted from parsed query
2. How it flows from SELECT processor to window function evaluation
3. Whether window_frame field is being cleared or replaced somewhere in the pipeline

---

## Impact Analysis

### Affected Demo Queries

The `demo/trading/sql/financial_trading.sql` contains **5 window functions with frame bounds**:

#### 1. Cumulative PnL (Lines 347-351)
```sql
SUM(p.current_pnl) OVER (
    PARTITION BY p.trader_id
    ORDER BY p.event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS cumulative_pnl
```
**Expected**: Running cumulative total
**Actual**: Same value for all rows (total PnL)

#### 2. Trades Today (Lines 353-357)
```sql
COUNT(*) OVER (
    PARTITION BY p.trader_id
    ORDER BY p.event_time
    RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
) AS trades_today
```
**Expected**: Count of trades within last 24 hours
**Actual**: Total count of ALL trades for trader

#### 3. PnL Volatility (Lines 359-363)
```sql
STDDEV(p.current_pnl) OVER (
    PARTITION BY p.trader_id
    ORDER BY p.event_time
    ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
) AS pnl_volatility
```
**Expected**: STDDEV over last 100 rows
**Actual**: STDDEV over ALL rows

#### 4. Total Exposure (Lines 367-371)
```sql
SUM(ABS(p.position_size * COALESCE(m.price, 0))) OVER (
    PARTITION BY p.trader_id
    ORDER BY p.event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS total_exposure
```
**Expected**: Running cumulative exposure
**Actual**: Total exposure for entire period

#### 5. High Risk Profile (Lines 382-386 in CASE)
```sql
WHEN STDDEV(p.current_pnl) OVER (
    PARTITION BY p.trader_id
    ORDER BY p.event_time
    ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
) > 25000 THEN 'HIGH_RISK_PROFILE'
```
**Expected**: HIGH_RISK if recent 100-row volatility > 25000
**Actual**: HIGH_RISK if ALL-TIME volatility > 25000

### Risk Classification Impact

The `risk_classification` logic depends on frame-bounded metrics:
- Line 379: Depends on `trades_today` (RANGE BETWEEN INTERVAL '1' DAY)
- Line 382-386: Depends on `pnl_volatility` (ROWS BETWEEN 99 PRECEDING)

**Result**: Risk alerts will be inaccurate and overly sensitive/insensitive.

---

## Supported vs. Unsupported Patterns

### ✅ Fully Supported

| Pattern | Status | Notes |
|---------|--------|-------|
| `PARTITION BY column` | ✅ Works | Partitions data correctly |
| `ORDER BY column` | ✅ Works | Ordering applied within partitions |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | ⚠️ Partial | Parses OK; executes as full partition |
| `ROWS BETWEEN N PRECEDING AND CURRENT ROW` | ⚠️ Partial | Parses OK; executes as full partition |
| `RANGE BETWEEN INTERVAL '...' PRECEDING AND CURRENT ROW` | ⚠️ Partial | Parses OK; executes as full partition |

### ❌ Frame Bounds Not Implemented

- All ROWS frame specifications → treated as full partition
- All RANGE frame specifications → treated as full partition
- PRECEDING/FOLLOWING offsets → ignored
- INTERVAL time-based bounds → ignored
- Custom frame start/end bounds → ignored

---

## Test Coverage Gap

### Parser Tests: ✅ 100% Coverage
**File**: `/tests/unit/sql/parser/window_frame_test.rs`
- 40+ tests covering all frame bound syntax
- All tests PASS

### Execution Tests: ⚠️ Incomplete Coverage
**File**: `/tests/unit/sql/execution/expression/enhanced_window_functions_test.rs`
- Tests PARTITION BY / ORDER BY functionality
- **No tests** verify frame bounds are applied
- Line 130: TODO comment: "Fix partition bounds calculation in window_functions.rs"
- Line 132: Known issue: "Currently getting 150.0 instead due to partition filtering issue"

---

## Workarounds for Current Implementation

### Workaround 1: Remove Frame Bounds (Not Recommended)
```sql
-- Falls back to default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
-- Will still process entire partition but with slightly different semantics
SUM(field) OVER (PARTITION BY col ORDER BY col)
```
**Limitation**: Only solves syntax, not semantics.

### Workaround 2: Use Subqueries (Complex)
```sql
SELECT
    trader_id,
    (
        SELECT SUM(current_pnl)
        FROM positions p2
        WHERE p2.trader_id = p1.trader_id
          AND p2.event_time >= p1.event_time - INTERVAL '1' DAY
          AND p2.event_time <= p1.event_time
    ) AS sum_last_day
FROM positions p1;
```
**Limitation**: Complex, performance overhead, requires rewriting queries.

### Workaround 3: Accept Known Limitation
Document that frame bounds are **not yet implemented** and metrics will use full partition scope.

**For Demo**: Acceptable - document in demo notes.

---

## Implementation Roadmap

### Phase 5: Window Function Frame Bounds Support (Estimated 8-12 hours)

#### Task 1: Refactor `calculate_frame_bounds()` (2-3 hours)
- Parse `WindowFrame::frame_type` (ROWS vs RANGE)
- Handle `FrameBound` variants:
  - `UnboundedPreceding` → start of partition
  - `Preceding(n)` → n rows back from current
  - `IntervalPreceding` → time-based interval back
  - `CurrentRow` → current position
  - `Following(n)` → n rows forward
  - `IntervalFollowing` → time-based interval forward
  - `UnboundedFollowing` → end of partition
- Return actual frame bounds instead of partition bounds

#### Task 2: Update Aggregate Functions (3-4 hours)
- **SUM()**: Apply frame bounds when iterating (lines 558-564)
- **COUNT()**: Apply frame bounds when counting (lines 674-676)
- **STDDEV()**: Apply frame bounds when calculating (lines 730-732)
- Ensure frame bounds are checked before each aggregation

#### Task 3: Add Comprehensive Tests (2-3 hours)
- Test ROWS BETWEEN with numeric offsets
- Test RANGE BETWEEN with INTERVAL bounds
- Test edge cases (empty frames, unbounded, etc.)
- Regression tests for existing functionality

#### Task 4: Update Documentation (1-2 hours)
- Update function documentation with frame bound support
- Add examples to API reference
- Update demo notes showing frame bounds are now supported

### Success Criteria
- [ ] All 5 window functions in demo produce correct results
- [ ] 20+ new execution tests pass
- [ ] No regressions in existing tests
- [ ] Performance impact < 5%
- [ ] Documentation updated

---

## Affected Code Locations

| File | Lines | Issue |
|------|-------|-------|
| `/src/velostream/sql/execution/expression/window_functions.rs` | 278-300 | Frame bounds calculated but ignored |
| `/src/velostream/sql/execution/expression/window_functions.rs` | 544-577 | SUM() ignores frame bounds |
| `/src/velostream/sql/execution/expression/window_functions.rs` | 670-712 | COUNT() ignores frame bounds |
| `/src/velostream/sql/execution/expression/window_functions.rs` | 715-763 | STDDEV() ignores frame bounds |
| `/demo/trading/sql/financial_trading.sql` | 347-351, 353-357, 359-363, 367-371, 382-386 | Uses frame bounds with current limitation |

---

## Migration Path

### Current State (Today)
- Window functions work with PARTITION BY / ORDER BY
- Frame bounds are **parsed but ignored**
- Demo runs but produces inaccurate results

### Phase 5 (After Implementation)
- Window functions work with **full frame bound support**
- Demo produces accurate risk calculations
- No SQL changes required (existing queries auto-benefit)

---

## Recommendations

### For Immediate Use
1. ✅ Use SELECT alias reuse feature (Phase 3 - Complete)
2. ⚠️ Use window functions WITHOUT frame bounds, or
3. ⚠️ Document frame bound usage as "known limitation"

### For Production
- Wait for Phase 5 implementation before using frame bounds
- Current implementation not suitable for critical risk calculations

### For Demo
- Document frame bound limitation in demo notes
- Mention that metrics use full partition scope (not time-windowed)
- Plan upgrade path to Phase 5

---

## Cross-References

- **FR-078 Progress Summary**: `/docs/feature/FR-078-PROGRESS-SUMMARY.md`
- **Design Document**: `/docs/feature/FR-078-column-alias-reuse-DESIGN.md`
- **Execution Tests**: `/tests/unit/sql/execution/expression/enhanced_window_functions_test.rs`
- **Parser Tests**: `/tests/unit/sql/parser/window_frame_test.rs`

---

## Document Metadata

**Version**: 1.0
**Date**: 2025-10-20
**Status**: Documented Limitation
**Priority**: Medium (Phase 5 of FR-078 roadmap)
**Effort**: 8-12 hours for full implementation
**Impact**: High (enables production-grade risk calculations)
