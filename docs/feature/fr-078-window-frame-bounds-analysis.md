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

## Root Cause Analysis

### File: `/src/velostream/sql/execution/expression/window_functions.rs`

#### Issue 1: Frame Bounds Intentionally Ignored (Lines 278-300)

```rust
fn calculate_frame_bounds(
    window_frame: &Option<WindowFrame>,
    current_position: usize,
    partition_bounds: &Option<(usize, usize)>,
    buffer: &[StreamRecord],
) -> Result<Option<(i64, i64)>, SqlError> {
    let _frame = match window_frame {  // ← UNDERSCORE: ignored!
        Some(frame) => frame,
        None => {
            return Ok(Some((-(current_position as i64), 0)));
        }
    };

    // For now, use simplified frame calculation
    // In a complete implementation, this would handle ROWS/RANGE
    // and various frame bounds
    let (start, end) = partition_bounds.unwrap_or((0, buffer.len()));

    Ok(Some((frame_start, frame_end)))
}
```

**Problem**: The parsed `WindowFrame` is bound to `_frame` (underscore = intentionally unused) and the function returns partition bounds instead of frame-specific bounds.

#### Issue 2: All Aggregate Functions Ignore Frame Bounds (Lines 544-763)

All three affected functions follow identical patterns:

**SUM() aggregate (lines 558-564)**:
```rust
let (start_idx, end_idx) = window_context
    .partition_bounds  // ← Uses partition bounds, not frame bounds
    .unwrap_or((0, window_context.buffer.len()));

for i in start_idx..end_idx {
    // Processes entire partition
}
```

**COUNT() aggregate (lines 674-676)**: Same pattern

**STDDEV() aggregate (lines 730-732)**: Same pattern

**Problem**: The `window_context.frame_bounds` field exists but is never used. All functions iterate over entire partition.

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
