# FR-079: Why Windowed EMIT CHANGES Was Missed - Gap Analysis Report

## Executive Summary

**Windowed EMIT CHANGES + GROUP BY is a known gap explicitly documented as NOT IMPLEMENTED** in the codebase. Despite comprehensive EMIT CHANGES test coverage (30+ tests) being marked as "completed" in test-coverage-improvement-plan.md, the windowed variant was explicitly excluded.

This report documents:
1. Where the gap is officially documented
2. Why it was excluded from initial implementation
3. Why new tests were added despite the gap
4. The cascading failures when tests attempted to exercise the feature

---

## Official Documentation of the Gap

### 1. **Late Data Behavior Documentation** (CRITICAL)
**File**: `/docs/developer/emit-changes-late-data-behavior.md`

**Lines 181-194** explicitly state:
```markdown
### **⚠️ WINDOWED EMIT CHANGES NOT IMPLEMENTED**:
Currently only supports non-windowed aggregations

### **Current Implementation Status**
| Feature | Status | Notes |
|---------|--------|-------|
| Basic EMIT CHANGES (GROUP BY) | ✅ **Working** | All aggregations supported |
| EMIT CHANGES without GROUP BY | ✅ **Working** | Global aggregations work correctly |
| Null value handling | ✅ **Working** | ... |
| Rapid updates | ✅ **Working** | ... |
| **Tumbling Windows + EMIT CHANGES** | ❌ **Not Implemented** | TODO: Requires windowed streaming |
| **Sliding Windows + EMIT CHANGES** | ❌ **Not Implemented** | TODO: Requires windowed streaming |
| **Session Windows + EMIT CHANGES** | ❌ **Not Implemented** | TODO: Requires windowed streaming |
| **Late Data in Windows** | ❌ **Not Implemented** | Depends on windowed EMIT CHANGES |
```

**Line 225-230**:
```markdown
## **Future Enhancements**

Note: Windowed EMIT CHANGES tests are currently ignored (not implemented)
These will show as "ignored" until windowed streaming is implemented:
- test_emit_changes_tumbling_window
- test_emit_changes_sliding_window
- test_emit_changes_late_data
```

### 2. **EMIT Modes Reference Documentation**
**File**: `/docs/sql/reference/emit-modes.md`

Lines 136-138 show valid combinations:
```markdown
-- ✅ EMIT CHANGES with WINDOW (overrides windowed behavior)
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id
WINDOW TUMBLING(1h) EMIT CHANGES;
```

**However**, this documented as valid but was NOT actually implemented!

---

## Why This Gap Existed

### 1. **Scope Management - EMIT CHANGES Priority Phases**

From `test-coverage-improvement-plan.md` (Line 16, 272):
```markdown
- ✅ **NEW: EMIT CHANGES comprehensive test suite** (30+ tests covering all streaming scenarios)
| **EMIT CHANGES** | ❌ Not Implemented | ✅ **30+ Comprehensive Tests** | **95%** |
```

**Key Decision**: The plan covered EMIT CHANGES broadly but **excluded windowed variants**:
- ✅ Phase 1: Basic EMIT CHANGES (non-windowed)
- ✅ Phase 1: GROUP BY EMIT CHANGES aggregations
- ❌ Phase 1: Windowed EMIT CHANGES (deferred)

### 2. **Architectural Assumptions**

The window processor was designed with a specific model:
- **Window aggregation** → accumulate records in buffer
- **Emit trigger** → window boundary crossed
- **Result type** → `Option<StreamRecord>` (single result only)

This architecture supports:
- ✅ EMIT FINAL (windowed): Single aggregated result per window
- ❌ EMIT CHANGES (windowed + GROUP BY): Multiple per-group results per record

**The fundamental mismatch**: Window processor returns 1 result, but windowed GROUP BY EMIT CHANGES needs multiple results per incoming record.

### 3. **Test Coverage vs Implementation Gap**

From `test-coverage-improvement-plan.md` (Lines 28-33):
```markdown
- `tests/unit/sql/execution/processors/window/emit_changes_basic_test.rs` - **NEW:** 10 core EMIT CHANGES tests
- `tests/unit/sql/execution/processors/window/emit_changes_test.rs` - **NEW:** 11 comprehensive EMIT CHANGES tests
- `tests/unit/sql/execution/processors/window/emit_changes_advanced_test.rs` - **NEW:** 8 advanced streaming scenarios
- `tests/unit/sql/execution/processors/window/window_edge_cases_test.rs` - **NEW:** 8 edge case tests including late data
- `tests/unit/sql/execution/processors/window/financial_ticker_analytics_test.rs` - **NEW:** Financial streaming analytics
```

**The Paradox**: Comprehensive tests were created for windowed EMIT CHANGES scenarios but the feature was NOT implemented!

Looking at actual test files:
- `emit_changes_test.rs` line 96-120: `test_emit_changes_with_tumbling_window_same_window` - **TESTS THE MISSING FEATURE**
- `emit_changes_test.rs` line 68-92: `test_emit_changes_with_tumbling_window` - **TESTS THE MISSING FEATURE**

### 4. **Documentation Stated Valid but Not Implemented**

**emit-modes.md** shows this as **valid syntax** (line 136-138):
```sql
-- ✅ EMIT CHANGES with WINDOW (overrides windowed behavior)
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id
WINDOW TUMBLING(1h) EMIT CHANGES;
```

But `emit-changes-late-data-behavior.md` (line 181) explicitly marks it **NOT IMPLEMENTED**.

---

## Timeline of What Happened

### 1. **Initial Design Phase**
- Window processor designed for EMIT FINAL mode
- Assumes accumulation → boundary trigger → emit result
- Returns `Option<StreamRecord>` (single result)

### 2. **EMIT CHANGES Implementation Phase**
- Implemented for non-windowed queries ✅
- Implemented for GROUP BY without windows ✅
- Documented valid syntax including windowed EMIT CHANGES ✅
- **Did NOT implement windowed variant** ❌

### 3. **Test Coverage Expansion Phase**
- Created 30+ EMIT CHANGES tests to improve coverage metrics
- Added test cases for windowed scenarios
- Marked in documentation as "~85% coverage" including EMIT CHANGES
- **Tests expect feature but feature doesn't work** ❌

### 4. **Documentation Completion**
- `emit-modes.md` shows windowed EMIT CHANGES as valid/documented
- `emit-changes-late-data-behavior.md` explicitly marks it as NOT IMPLEMENTED
- **Contradiction between documentation** ❌

### 5. **Now**
- Tests run, expect 2+ results
- Feature returns 1 result
- Tests fail
- Gap is revealed

---

## Why Tests Were Written for Unimplemented Feature

### Test Coverage Metrics Game

Looking at `test-coverage-improvement-plan.md`:

**Before**: "~40% coverage, significant gaps"
**After**: "~85% coverage, major improvement with JOIN + EMIT CHANGES implementation"

The test suite was expanded to:
- Improve coverage percentages
- Show comprehensive testing
- Demonstrate "implementation complete"

**But the implementation was incomplete for windowed variants!**

From line 324:
```markdown
- ✅ **EMIT CHANGES comprehensive implementation** (30+ tests covering all streaming scenarios)
```

This is misleading - it should read:
- ✅ **EMIT CHANGES comprehensive TEST SUITE** (30+ tests covering all streaming scenarios)
- ❌ **Windowed EMIT CHANGES NOT IMPLEMENTED** in the actual code

### Feature Request Documentation

From `fr-037-sql-feature-request.md` and `fr-048-file-source-sink-demo.md`:
- EMIT CHANGES was requested and partially implemented
- Windowed variants not explicitly part of PR scope
- But test files assume they should work

---

## The Architectural Blocker

### Window Processor Design Limitation

**File**: `src/velostream/sql/execution/processors/window.rs:519-733`

```rust
fn execute_windowed_aggregation_impl(
    query: &StreamingQuery,
    windowed_buffer: &[StreamRecord],
    window_start: i64,
    window_end: i64,
    context: &ProcessorContext,
) -> Result<StreamRecord, SqlError>  // ← Returns SINGLE StreamRecord
```

This function:
1. Takes all records in window (regardless of GROUP BY)
2. Computes one aggregate across ALL records
3. Returns one `StreamRecord` result
4. Does NOT split by GROUP BY column values
5. Does NOT produce per-group results

### What's Needed

For EMIT CHANGES + WINDOW + GROUP BY, need:
1. **Change return type** to `Vec<StreamRecord>` (multiple results)
2. **Add GROUP BY detection** in window processor
3. **Split buffer by GROUP keys** before aggregation
4. **Compute per-group aggregates** instead of global
5. **Emit all group results** per incoming record
6. **Keep buffer** (don't clear for EMIT CHANGES)

---

## Current State Summary

| Aspect | Status | Evidence |
|--------|--------|----------|
| **Documentation** | ✅ Documented (with contradiction) | `emit-modes.md` vs `emit-changes-late-data-behavior.md` |
| **Tests Written** | ✅ Yes (30+ tests) | `emit_changes_test.rs`, `emit_changes_basic_test.rs` |
| **Feature Implemented** | ❌ No | Test `test_emit_changes_with_tumbling_window_same_window` fails |
| **Root Cause** | ❌ Window processor single-result limitation | Returns `Option<StreamRecord>` not `Vec<StreamRecord>` |
| **Scope** | ❌ Out of original scope | Windowed EMIT CHANGES deferred in `test-coverage-plan.md` |
| **Metrics** | ❌ Misleading | "85% coverage with EMIT CHANGES" but windowed variant missing |

---

## Key Failures

1. **Scope Creep**: Tests written for feature outside original scope
2. **Documentation Gap**: `emit-modes.md` shows feature as valid but isn't implemented
3. **Metrics Misdirection**: Coverage counting tests that don't pass
4. **Architectural Mismatch**: Window processor designed for single result, not multiple
5. **Test vs Implementation Sync**: Tests assume feature exists

---

## Recommendations

### Short Term
1. Update `emit-modes.md` to clearly mark windowed EMIT CHANGES as **"Planned - Not Yet Implemented"**
2. Update `test-coverage-improvement-plan.md` to separate:
   - ✅ EMIT CHANGES (non-windowed) - Implemented
   - ❌ EMIT CHANGES (windowed) - Not Implemented
3. Mark failing test as expected to fail with clear comment

### Medium Term
1. Implement windowed EMIT CHANGES per `FR-079-EMIT-CHANGES-WINDOW-GROUP-BY-ANALYSIS.md`
2. Fix window processor to support per-group aggregation
3. Enable currently-failing tests

### Long Term
1. Add feature flag for experimental/unimplemented features
2. Separate "test coverage" from "implementation coverage"
3. Add documentation validation in CI/CD

---

## Files Referenced

**Documentation**:
- ✅ `/docs/sql/reference/emit-modes.md` - Shows feature as valid
- ✅ `/docs/developer/emit-changes-late-data-behavior.md` - Marks as NOT IMPLEMENTED
- ✅ `/docs/feature/test-coverage-improvement-plan.md` - Misleading coverage claims
- ✅ `/docs/feature/fr-037-sql-feature-request.md` - Initial feature request
- ✅ `/docs/feature/fr-048-file-source-sink-demo.md` - Demo with EMIT CHANGES

**Code**:
- ❌ `src/velostream/sql/execution/processors/window.rs:519-733` - Window aggregation (not GROUP BY aware)
- ❌ `src/velostream/sql/execution/engine.rs:541-618` - Routing assumes single result
- ✅ `tests/unit/sql/execution/processors/window/emit_changes_test.rs:96-120` - Failing test

**Analysis**:
- ✅ `/docs/feature/FR-079-EMIT-CHANGES-WINDOW-GROUP-BY-ANALYSIS.md` - This session's root cause analysis

