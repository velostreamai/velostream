# FR-079: Windowed EMIT CHANGES with GROUP BY - Complete Implementation Guide

## Overview

**Status**: ❌ **NOT IMPLEMENTED** (Feature Request for implementation)

**Scope**: Implement EMIT CHANGES semantics for windowed queries with GROUP BY aggregations

**Impact**: Critical streaming feature for real-time windowed analytics with per-group change emission

**Test Status**: Tests written but failing (30+ test cases expect this feature)

---

## Background & Problem Statement

### Current Gap

Velostream currently supports:
- ✅ EMIT CHANGES for non-windowed GROUP BY aggregations
- ✅ EMIT FINAL for windowed queries (emit on window close)
- ❌ **EMIT CHANGES for windowed GROUP BY** (this feature)

### Failing Test

**File**: `tests/unit/sql/execution/processors/window/emit_changes_test.rs:96-120`

**Test**: `test_emit_changes_with_tumbling_window_same_window`

```rust
#[tokio::test]
async fn test_emit_changes_with_tumbling_window_same_window() {
    let sql = r#"
        SELECT
            status,
            SUM(amount) as total_amount,
            COUNT(*) as order_count
        FROM orders
        WINDOW TUMBLING(1m)
        GROUP BY status
        EMIT CHANGES
    "#;

    let records = vec![
        // 5 records: 3 "pending", 2 "completed"
        // All within same 1-minute window
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    // ❌ FAILS: Expected 2+ results (one per GROUP BY group)
    // ✅ ACTUAL: Only 1 result (aggregated single result, not per-group)
    WindowTestAssertions::assert_result_count_min(&results, 2, "EMIT CHANGES with Tumbling Window");
}
```

**Expected Behavior**:
- Result 1: `{status: "pending", total_amount: 600, order_count: 3}`
- Result 2: `{status: "completed", total_amount: 400, order_count: 2}`

**Actual Behavior**:
- Only 1 aggregated result (treating all records as one group)

---

## Technical Analysis

### Root Cause

**Window Processor Limitation** (`src/velostream/sql/execution/processors/window.rs:519-733`)

The `execute_windowed_aggregation_impl()` function:

```rust
fn execute_windowed_aggregation_impl(
    query: &StreamingQuery,
    windowed_buffer: &[StreamRecord],
    window_start: i64,
    window_end: i64,
    context: &ProcessorContext,
) -> Result<StreamRecord, SqlError>  // ← Returns SINGLE StreamRecord
```

**Issues**:
1. **Single Result Only**: Return type `Result<StreamRecord, ...>` can only return one result
2. **No GROUP BY Support**: Doesn't split records by GROUP BY column values
3. **Global Aggregation**: Computes one aggregate across ALL records regardless of GROUP BY
4. **Architectural Mismatch**: Designed for EMIT FINAL (one result per window), not EMIT CHANGES (multiple per-group results per record)

### Why This Was Missed

See `FR-079-EMIT-CHANGES-WINDOW-GAP-ANALYSIS.md` for complete gap analysis. Summary:

1. **Intentional Deferral**: Windowed EMIT CHANGES explicitly marked as "not implemented" in design docs
2. **Test Coverage Metrics**: 30+ tests written to boost coverage claims, but feature wasn't implemented
3. **Documentation Contradiction**:
   - `emit-modes.md` shows feature as valid/supported
   - `emit-changes-late-data-behavior.md` marks it as not implemented
4. **Scope Boundary**: Feature was out of scope for initial EMIT CHANGES implementation

---

## Implementation Design

### Architecture Decision: Multi-Result Emission

For EMIT CHANGES + WINDOW + GROUP BY, we need to:

1. **Detect GROUP BY + EMIT CHANGES** in windowed queries
2. **Split window buffer by GROUP BY keys** into separate groups
3. **Compute per-group aggregates** instead of global aggregates
4. **Emit multiple results** (one per group) per incoming record
5. **Preserve buffer** (don't clear - EMIT CHANGES semantic)

### High-Level Implementation Plan

#### Phase 1: Window Processor Enhancement

**File**: `src/velostream/sql/execution/processors/window.rs`

Add GROUP BY support to window aggregation:

```rust
// In process_window_emission_state() around line 193
fn process_window_emission_state(...) -> Result<Option<StreamRecord>, SqlError> {
    // Detect GROUP BY + EMIT CHANGES
    let group_by = if let StreamingQuery::Select { group_by, .. } = query {
        group_by.clone()
    } else {
        None
    };

    let is_emit_changes = if let StreamingQuery::Select { emit_mode, .. } = query {
        matches!(emit_mode, Some(EmitMode::Changes))
    } else {
        false
    };

    // Route to appropriate handler
    if group_by.is_some() && is_emit_changes {
        // NEW: Handle GROUP BY EMIT CHANGES
        return Self::process_windowed_group_by_emission(
            query, window_spec, group_by.unwrap(), &buffer, context
        );
    }

    // Existing single-result logic for non-GROUP BY or EMIT FINAL
    ...
}
```

#### Phase 2: Group Splitting Logic

Add new helper function:

```rust
fn process_windowed_group_by_emission(
    query: &StreamingQuery,
    window_spec: &WindowSpec,
    group_by_cols: Vec<String>,
    buffer: &[StreamRecord],
    context: &ProcessorContext,
) -> Result<Option<StreamRecord>, SqlError> {
    // 1. Split buffer into groups
    let mut groups: HashMap<Vec<FieldValue>, Vec<&StreamRecord>> = HashMap::new();
    for record in buffer {
        let group_key = Self::extract_group_key(record, &group_by_cols);
        groups.entry(group_key).or_insert_with(Vec::new).push(record);
    }

    // 2. Compute aggregates per group
    let mut results = Vec::new();
    for (group_key, group_records) in groups {
        // Use existing aggregation logic but per-group
        let result = Self::compute_group_aggregate(
            query,
            group_key,
            &group_records,
            window_spec,
            context,
        )?;
        results.push(result);
    }

    // 3. Send multiple results
    // (See Phase 3 for emission strategy)
    Ok(results)
}
```

#### Phase 3: Result Emission Strategy

**Two Options**:

**Option A: Change Return Type** (Requires API changes)
```rust
// Change from Option<StreamRecord> to Vec<StreamRecord>
pub fn process_windowed_query(
    query_id: &str,
    query: &StreamingQuery,
    record: &StreamRecord,
    context: &mut ProcessorContext,
) -> Result<Vec<StreamRecord>, SqlError>
```

Update engine to emit all results:
```rust
// In engine.rs execute_internal()
let results = WindowProcessor::process_windowed_query(...)?;
for result in results {
    self.output_sender.send(result)?;
}
```

**Option B: Emit Directly from Context** (Minimal API changes)
```rust
// Window processor adds results to context
pub struct ProcessorContext {
    pub windowed_group_results: Vec<StreamRecord>,
}

// Engine iterates context results:
if !context.windowed_group_results.is_empty() {
    for result in context.windowed_group_results.drain(..) {
        self.output_sender.send(result)?;
    }
}
```

**Recommendation**: Option A (cleaner, explicit, less magic)

#### Phase 4: Integration Points

Update execution path:

```
Input Record
    ↓
WindowProcessor::process_windowed_query()
    ├─ Check: GROUP BY + EMIT CHANGES?
    │  ├─ YES → process_windowed_group_by_emission()
    │  │         ├─ Split buffer by GROUP BY keys
    │  │         ├─ Compute per-group aggregates
    │  │         └─ Return Vec<StreamRecord> (multiple results)
    │  │
    │  └─ NO → process_window_emission_state() (existing)
    │          └─ Return Option<StreamRecord> (single result)
    ↓
Engine emits all results to output_sender
    ↓
Results appear in output channel (one per group)
```

---

## Implementation Checklist

### Code Changes Required

- [ ] **Window Processor** (`window.rs`)
  - [ ] Add GROUP BY detection in `process_windowed_query()`
  - [ ] Implement `extract_group_key()` helper
  - [ ] Implement `process_windowed_group_by_emission()` function
  - [ ] Add `compute_group_aggregate()` for per-group aggregation
  - [ ] Change return type to `Result<Vec<StreamRecord>, SqlError>`

- [ ] **Engine** (`engine.rs`)
  - [ ] Update `execute_internal()` to handle Vec<StreamRecord>
  - [ ] Update `apply_query_with_processors()` if routing through SelectProcessor
  - [ ] Ensure all results sent to output_sender

- [ ] **Expression Evaluator** (as needed)
  - [ ] Ensure GROUP BY key extraction works with all field types
  - [ ] Handle NULL values in GROUP BY keys

### Testing Changes

- [ ] Enable currently-disabled windowed EMIT CHANGES tests
- [ ] Verify `test_emit_changes_with_tumbling_window_same_window` passes
- [ ] Verify `test_emit_changes_with_sliding_window` passes
- [ ] Verify `test_emit_changes_with_session_window` passes
- [ ] Run full EMIT CHANGES test suite to ensure no regressions

### Documentation Updates

- [ ] Update `emit-changes-late-data-behavior.md` (remove "NOT IMPLEMENTED" for windowed)
- [ ] Update `emit-modes.md` to confirm windowed EMIT CHANGES works
- [ ] Add implementation notes to window processor documentation
- [ ] Update test coverage plan to reflect actual implementation status

---

## Testing Strategy

### Unit Tests (Already Written)

All tests in `emit_changes_test.rs` should pass:

```bash
cargo test test_emit_changes_with_tumbling_window_same_window --no-default-features
cargo test test_emit_changes_with_sliding_window --no-default-features
cargo test test_emit_changes_with_session_window --no-default-features
```

### Integration Tests

```bash
cargo test emit_changes --no-default-features
cargo test window --no-default-features
```

### Regression Tests

Ensure existing EMIT CHANGES tests still pass:
```bash
cargo test test_basic_emit_changes --no-default-features
cargo test test_emit_changes_rapid_updates --no-default-features
```

---

## Performance Considerations

### Memory Impact
- **Before**: Buffer accumulates all records for window
- **After**: Same buffer, but split into group tracking
- **Impact**: Minimal (groups extracted on demand, not stored separately)

### CPU Impact
- **Before**: One aggregation per window close
- **After**: Multiple aggregations per record (one per group)
- **Impact**: Linear with number of distinct groups
- **Mitigation**: Group count typically small (< 1000)

### Buffer Management
- **EMIT CHANGES**: Buffer persists between emissions (accumulates)
- **EMIT FINAL**: Buffer cleared per window boundary (existing behavior)
- **No change**: To buffer lifecycle

---

## References

### Analysis Documents
- **Root Cause**: `FR-079-EMIT-CHANGES-WINDOW-GROUP-BY-ANALYSIS.md`
- **Gap Analysis**: `FR-079-EMIT-CHANGES-WINDOW-GAP-ANALYSIS.md`

### Related Code
- **Window Processor**: `src/velostream/sql/execution/processors/window.rs:519-733`
- **Engine Flow**: `src/velostream/sql/execution/engine.rs:541-618`
- **Test File**: `tests/unit/sql/execution/processors/window/emit_changes_test.rs:96-120`

### Related Documentation
- **EMIT Modes**: `docs/sql/reference/emit-modes.md` (lines 136-138)
- **Late Data**: `docs/developer/emit-changes-late-data-behavior.md` (lines 181-194)
- **Test Coverage**: `docs/feature/test-coverage-improvement-plan.md` (lines 16, 272)

---

## Success Criteria

✅ **Implementation Complete When**:
1. All windowed EMIT CHANGES tests pass
2. Per-group results emitted on each incoming record
3. Buffer accumulates (not cleared) for EMIT CHANGES
4. No regressions in existing EMIT FINAL functionality
5. Documentation updated to reflect implementation status
6. Code review approved

---

## Estimated Effort

- **Design Review**: 1-2 hours
- **Window Processor Changes**: 4-6 hours
- **Engine Integration**: 2-3 hours
- **Testing & Validation**: 3-4 hours
- **Documentation**: 1-2 hours
- **Total**: ~12-17 hours

---

## Related Features

- **EMIT FINAL**: ✅ Already implemented (window close trigger)
- **GROUP BY without WINDOW**: ✅ Already implemented (non-windowed)
- **Late Data Handling**: Blocked by this feature (depends on windowed EMIT CHANGES)
- **Watermark Support**: Phase 1B (complements windowed EMIT CHANGES)

---

## Next Steps

1. ✅ Approve design
2. Implement Phase 1 (Window Processor GROUP BY detection)
3. Implement Phase 2 (Group splitting logic)
4. Implement Phase 3 (Result emission)
5. Run full test suite
6. Code review
7. Merge and deploy

