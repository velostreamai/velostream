# FR-079: EMIT CHANGES with Window + GROUP BY - Complete Analysis

## Problem Statement

**Failing Test**: `test_emit_changes_with_tumbling_window_same_window`

**Expected Behavior**: At least 2 results (one per GROUP BY group: "pending" and "completed")

**Actual Behavior**: Only 1 result collected after final flush

**Query**:
```sql
SELECT
    status,
    SUM(amount) as total_amount,
    COUNT(*) as order_count
FROM orders
WINDOW TUMBLING(1m)
GROUP BY status
EMIT CHANGES
```

**Test Data** (5 records in same 1-minute window):
- Record 1: status="pending", amount=100
- Record 2: status="pending", amount=200
- Record 3: status="completed", amount=150
- Record 4: status="pending", amount=300
- Record 5: status="completed", amount=250

**Expected Result**:
- At least 2 GROUP BY aggregations should be emitted:
  1. status="pending" group: SUM=600, COUNT=3
  2. status="completed" group: SUM=400, COUNT=2

---

## Root Cause Analysis

### The Core Issue: Window Processor Cannot Handle GROUP BY

The window processor's `execute_windowed_aggregation_impl()` function (window.rs:519-733) has fundamental limitations:

1. **No GROUP BY Support**
   - It processes the entire windowed buffer as a single aggregate
   - It does NOT split records by GROUP BY column values
   - It treats all records as one group regardless of GROUP BY clause

2. **Single Result Only**
   - Function signature: `-> Result<StreamRecord, SqlError>`
   - Can only return one StreamRecord, not multiple
   - This is a hard constraint for GROUP BY support

3. **Aggregation Logic**
   - Lines 623-647: Aggregation loops through all `filtered_records` without grouping
   - No logic to separate records by GROUP BY keys
   - Cannot compute per-group SUM, COUNT, etc.

**Example of What's Happening**:
```
Window Buffer: [rec1(pending,100), rec2(pending,200), rec3(completed,150), rec4(pending,300), rec5(completed,250)]

Current Logic: Computes ONE aggregate across ALL records
Expected Logic: Should compute:
  - Group "pending": [rec1, rec2, rec4] → SUM=600, COUNT=3
  - Group "completed": [rec3, rec5] → SUM=400, COUNT=2
  - Emit both groups
```

---

## Architecture Flow Analysis

### Current Execution Path (Windowed Query)

**File**: `engine.rs` lines 541-618 (`execute_internal()`)

```
Input Record → execute_internal()
    ↓
Is Windowed Query? YES
    ↓
Initialize window_state (empty initially)
    ↓
Create ProcessorContext
    ↓
WindowProcessor::process_windowed_query()
    ├─ Add record to window buffer
    ├─ Check emit_mode
    │  └─ For EMIT CHANGES: should_emit = true
    └─ Call process_window_emission_state()
        ├─ Get buffer records for window
        ├─ Call execute_windowed_aggregation_impl()
        │  └─ Returns Option<StreamRecord> (ONE result or None)
        └─ Return Option<StreamRecord>
    ↓
Result = Option<StreamRecord> (0 or 1 result)
    ↓
If result exists:
    └─ Send to output_sender (line 609-614)
    ↓
End
```

**Problem**: The window processor can only emit ONE result per record, but for GROUP BY it should emit MULTIPLE results (one per group).

### GROUP BY State Management (Non-Windowed)

**File**: `engine.rs` lines 498-684

The engine has `flush_group_by_results()` which:
- Iterates through `self.group_states` (accumulated GROUP BY states)
- For each group, emits a separate result via `output_sender` (line 677)
- This works ONLY for non-windowed queries

**Issue**: Window processor doesn't use this GROUP BY state mechanism. It handles aggregation internally and returns only one result.

---

## What Needs to Happen for EMIT CHANGES + WINDOW + GROUP BY

### Desired Behavior

For each incoming record with EMIT CHANGES:

1. **Add record to window buffer**
2. **Split buffer by GROUP BY keys**:
   - pending: [rec1, rec2, ..., recN]
   - completed: [recM, recK, ...]
3. **Compute aggregates per group**:
   - For "pending": SUM(amount)=X, COUNT(*)=Y
   - For "completed": SUM(amount)=A, COUNT(*)=B
4. **Emit ONE result per group**:
   - Emit result for "pending" group
   - Emit result for "completed" group
5. **Do NOT clear buffer** (EMIT CHANGES semantic: buffer accumulates)

### Example Flow

```
Record 1 (pending, 100):
  Buffer: [rec1]
  Groups: pending [rec1]
  Emit: pending_result(SUM=100, COUNT=1)

Record 2 (pending, 200):
  Buffer: [rec1, rec2]
  Groups: pending [rec1, rec2]
  Emit: pending_result(SUM=300, COUNT=2)

Record 3 (completed, 150):
  Buffer: [rec1, rec2, rec3]
  Groups: pending [rec1, rec2], completed [rec3]
  Emit: pending_result(SUM=300, COUNT=2)
        completed_result(SUM=150, COUNT=1)

... and so on
```

---

## Required Changes

### 1. **Window Processor Enhancement**

**Location**: `src/velostream/sql/execution/processors/window.rs`

**Changes Needed**:

#### A. Add GROUP BY Detection
```rust
fn process_window_emission_state(...) -> Result<Option<StreamRecord>, SqlError> {
    // NEW: Check if query has GROUP BY
    let group_by = if let StreamingQuery::Select { group_by, .. } = query {
        group_by.clone()
    } else {
        None
    };

    // If GROUP BY exists with EMIT CHANGES, use new logic
    if group_by.is_some() && is_emit_changes {
        return Self::process_windowed_group_by_aggregation(...)
    }

    // Otherwise use existing single-aggregate logic
    ...
}
```

#### B. Add GROUP BY Aggregation Method
```rust
fn process_windowed_group_by_aggregation(
    query: &StreamingQuery,
    window_spec: &WindowSpec,
    group_by_cols: &[String],  // Columns to group by
    buffer: &[StreamRecord],
    context: &mut ProcessorContext,
) -> Result<Vec<StreamRecord>, SqlError> {
    // 1. Split buffer into groups by GROUP BY keys
    let mut groups: HashMap<Vec<FieldValue>, Vec<&StreamRecord>> = HashMap::new();
    for record in buffer {
        let group_key = extract_group_key(record, group_by_cols);
        groups.entry(group_key).or_insert_with(Vec::new).push(record);
    }

    // 2. For each group, compute aggregates
    let mut results = Vec::new();
    for (group_key, group_records) in groups {
        let result = compute_group_aggregate(
            group_key,
            &group_records,
            query,
            window_spec,
            context,
        )?;
        results.push(result);
    }

    Ok(results)
}
```

#### C. Change Return Type Strategy

**Option 1**: Change window processor to emit directly
```rust
// Instead of returning Option<StreamRecord>,
// the processor sends results to context's output handler
pub fn process_windowed_query(...) -> Result<(), SqlError> {
    // ... emit multiple results via context
}
```

**Option 2**: Return Multiple Results (Requires API Change)
```rust
pub fn process_windowed_query(...) -> Result<Vec<StreamRecord>, SqlError> {
    // Return multiple results for GROUP BY
}
```

### 2. **Engine Flow Update**

**Location**: `src/velostream/sql/execution/engine.rs`

**Changes Needed**:

Update `execute_internal()` to handle multiple results from window processor:

```rust
// Current (lines 573-586):
let result = WindowProcessor::process_windowed_query(
    &query_id,
    query,
    &stream_record,
    &mut context,
)?;

// If windowed: process result
if let Some(result) = result {
    self.output_sender.send(result)?;
}

// NEW (handles multiple results):
let results = WindowProcessor::process_windowed_query_multi(
    &query_id,
    query,
    &stream_record,
    &mut context,
)?;

// Emit all results
for result in results {
    self.output_sender.send(result)?;
}
```

### 3. **ProcessorContext Enhancement** (Optional)

Add a result accumulation buffer to context:

```rust
pub struct ProcessorContext {
    // ... existing fields
    pub windowed_group_results: Vec<StreamRecord>,  // NEW
}
```

This allows window processor to accumulate multiple group results that engine can then emit.

---

## Implementation Strategy (Recommended)

### Phase 1: Add GROUP BY Detection to Window Processor
- Modify `process_windowed_query()` to check for GROUP BY
- If GROUP BY + EMIT CHANGES, route to new method
- Keep existing logic for non-GROUP BY

### Phase 2: Implement Group Splitting Logic
- Add `extract_group_key()` function
- Create `HashMap<GroupKey, Vec<Records>>` for buffered records
- Compute aggregates per group using existing aggregation functions

### Phase 3: Update Result Emission
- Modify engine to handle multiple results from window processor
- Ensure all group results are sent to output_sender
- Verify buffer is NOT cleared for EMIT CHANGES

### Phase 4: Testing
- Run `test_emit_changes_with_tumbling_window_same_window`
- Verify 2+ results are emitted (one per group)
- Run all EMIT CHANGES tests to ensure no regressions

---

## Key Points to Remember

1. **EMIT CHANGES semantic**: Buffer accumulates, does NOT clear between emissions
2. **Window + GROUP BY behavior**: Should emit per-group aggregates on each record
3. **Test expectation**: At least 2 results for 2 different GROUP BY values
4. **Current blocker**: Window processor can only return 1 result, needs to return multiple or emit directly

---

## References

- **Window Processor**: `src/velostream/sql/execution/processors/window.rs:519-733`
- **Engine Flow**: `src/velostream/sql/execution/engine.rs:541-618`
- **GROUP BY Handling**: `src/velostream/sql/execution/engine.rs:498-684`
- **Failing Test**: `tests/unit/sql/execution/processors/window/emit_changes_test.rs:96-120`
- **Test Utilities**: `tests/unit/sql/execution/processors/window/shared_test_utils.rs`
