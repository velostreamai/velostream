# Phase 6.4C Implementation Guide: Eliminate State Duplication

**Status**: Implementation Plan
**Effort**: M (Medium - 2-3 days)
**Expected Improvement**: +5-10% throughput (730-750K rec/sec for Scenario 0)
**Target**: Complete state migration to PartitionStateManager

---

## Overview

Phase 6.4C eliminates the architectural duplication where state exists in **TWO places simultaneously**:
- **StreamExecutionEngine** (master copy)
- **ProcessorContext** (working copy)

This requires manual synchronization (~20 locations) with HashMap cloning overhead.

### Why This Matters

```
Current (Phase 6.4): State lives in TWO places
┌─────────────────────────────────────────────┐
│  StreamExecutionEngine                       │
│  ├─ group_states: HashMap<...>  (Arc)       │
│  └─ (per partition, still has overhead)     │
└─────────────────────────────────────────────┘
                    ↓ (clone on read)
        ┌─────────────────────┐
        │  ProcessorContext   │
        │  ├─ group_by_states │
        │  └─ (working copy)  │
        └─────────────────────┘
                    ↓ (sync back)

Target (After 6.4C): State lives in ONE place
┌──────────────────────────────────────────────┐
│ PartitionStateManager (per partition)        │
│ ├─ group_by_states: Arc<Mutex<HashMap<...>>>│
│ └─ (initialized on first record)            │
└──────────────────────────────────────────────┘
         ↓
   ┌──────────────────────┐
   │ ProcessorContext     │
   │ ├─ Arc ref to states │  (no copy, just ref)
   │ └─ (direct access)   │
   └──────────────────────┘
```

---

## Implementation Phases

### Phase 1: Add State Storage to PartitionStateManager (2 hours)

**What to do**:
- Add `group_by_states` field to `PartitionStateManager`
- Initialize as empty on first construction
- Implement getter method

**Files to modify**:
- `src/velostream/server/v2/partition_manager.rs`

**Code changes**:
```rust
pub struct PartitionStateManager {
    partition_id: usize,
    metrics: Arc<PartitionMetrics>,
    watermark_manager: Arc<WatermarkManager>,
    pub execution_engine: tokio::sync::Mutex<Option<StreamExecutionEngine>>,
    pub query: Arc<tokio::sync::Mutex<Option<Arc<StreamingQuery>>>>,
    // NEW: Phase 6.4C - Group BY state storage
    pub group_by_states: Arc<tokio::sync::Mutex<HashMap<String, Arc<GroupByState>>>>,
}

impl PartitionStateManager {
    pub fn new(partition_id: usize) -> Self {
        let metrics = Arc::new(PartitionMetrics::new(partition_id));
        let watermark_manager = Arc::new(WatermarkManager::with_defaults(partition_id));
        Self {
            partition_id,
            metrics,
            watermark_manager,
            execution_engine: tokio::sync::Mutex::new(None),
            query: Arc::new(tokio::sync::Mutex::new(None)),
            group_by_states: Arc::new(tokio::sync::Mutex::new(HashMap::new())),  // NEW
        }
    }

    /// Get reference to group_by_states for external processing
    pub fn get_group_by_states_ref(&self) -> Arc<tokio::sync::Mutex<HashMap<String, Arc<GroupByState>>>> {
        Arc::clone(&self.group_by_states)
    }
}
```

**Testing**:
- Verify PartitionStateManager::new() creates empty states
- Test state initialization and persistence

### Phase 2: Update ProcessorContext to Accept State Reference (2 hours)

**What to do**:
- Modify ProcessorContext to hold Arc reference to state instead of owned copy
- Update initialization to use partition state
- Ensure backward compatibility for tests

**Files to modify**:
- `src/velostream/sql/execution/processors/context.rs`

**Key concept**:
Instead of copying state INTO the context, pass a reference that the context holds:
```rust
// Before (with copying):
pub struct ProcessorContext {
    pub group_by_states: HashMap<String, Arc<GroupByState>>,  // Owned copy
}

// After (with reference):
pub struct ProcessorContext {
    pub group_by_states: Arc<tokio::sync::Mutex<HashMap<String, Arc<GroupByState>>>>,  // Reference
}
```

**Impact**:
- No copying on context creation
- No synchronization back to engine
- Direct access to authoritative state

### Phase 3: Update Coordinator to Use Partition State (3 hours)

**Files to modify**:
- `src/velostream/server/v2/coordinator.rs`

**Changes**:
1. In `execute_batch_for_partition()`:
```rust
// OLD: Copy state out
let group_states = engine.get_group_states().clone();
let mut context = ProcessorContext::new(&query_id);
context.group_by_states = group_states;  // COPY

// Process records
for record in records {
    QueryProcessor::process_query(query, record, &mut context)?;
}

// NEW: Direct reference
let state_ref = partition_manager.get_group_by_states_ref();
let mut context = ProcessorContext::with_state(&query_id, state_ref);  // REFERENCE

// Process records
for record in records {
    QueryProcessor::process_query(query, record, &mut context)?;
}
```

2. Remove `set_group_states()` calls since state is already updated

### Phase 4: Update Common Processor to Use Partition State (3 hours)

**Files to modify**:
- `src/velostream/server/processors/common.rs`

**Changes**:
Similar to Phase 3:
- Instead of extracting state from engine and copying to context
- Pass partition state reference to ProcessorContext
- Remove state sync-back operations

**Locations to update**:
- Line ~348: `engine_lock.get_group_states().clone()`
- Line ~411: `engine_lock.set_group_states()`
- Line ~423: `engine_lock.get_group_states().clone()`
- Line ~544: `engine_lock.set_group_states()`

### Phase 5: Remove State from StreamExecutionEngine (1 hour)

**Files to modify**:
- `src/velostream/sql/execution/engine.rs`

**Changes**:
1. Delete `group_states` field
2. Delete `get_group_states()` method
3. Delete `set_group_states()` method
4. Remove initialization in constructor
5. Update any internal usages (e.g., line 1269)

**Code cleanup**:
```rust
// DELETE these methods entirely
pub fn get_group_states(&self) -> &HashMap<String, Arc<GroupByState>> { ... }
pub fn set_group_states(&mut self, states: HashMap<String, Arc<GroupByState>>) { ... }

// DELETE field
group_states: HashMap<String, Arc<GroupByState>>,
```

---

## Implementation Order (Recommended)

**Why this order matters**:
- **Phase 1 FIRST**: Add storage before we reference it
- **Phase 2 SECOND**: Update context to accept new parameter type
- **Phase 3/4 THIRD**: Update callers to use new pattern
- **Phase 5 LAST**: Remove old code after references are gone

```
┌─────────────────────────────────────────────────────┐
│ Phase 1: Add state to PartitionStateManager         │  (Easy, no conflicts)
└─────────────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────────────┐
│ Phase 2: Update ProcessorContext initialization     │  (Update 1 file)
└─────────────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────────────┐
│ Phase 3: Update coordinator.rs to use partition state
│          (Most critical, per-partition execution)   │  (Update 2 locations)
└─────────────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────────────┐
│ Phase 4: Update common.rs to use partition state    │  (Update 4 locations)
│          (V1 processor chain compatibility)         │
└─────────────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────────────┐
│ Phase 5: Remove state from engine.rs               │  (Clean up)
│          Run full test suite to verify             │
└─────────────────────────────────────────────────────┘
```

---

## Testing Strategy

### Unit Tests
- ✅ PartitionStateManager: State initialization and persistence
- ✅ ProcessorContext: Initialization with state reference
- ✅ Coordinator: Batch execution with partition state
- ✅ Common processor: Record processing with partition state

### Integration Tests
- ✅ Comprehensive baseline test (all 5 scenarios)
  - Verify state is maintained across batches
  - Verify GROUP BY aggregations still work correctly
  - Verify state doesn't leak between partitions
- ✅ Performance benchmarks
  - Measure throughput improvement
  - Verify 5-10% gain expected

### Regression Tests
- ✅ All 530 unit tests pass
- ✅ All scenario tests pass
- ✅ No new compilation warnings
- ✅ Code formatting compliant

---

## Risk Analysis

### Low Risk
- ✅ State is scoped to single partition
- ✅ No sharing between partitions
- ✅ Mutex protection still in place
- ✅ Clear data ownership (processor owns state access)

### Medium Risk
- ⚠️ ProcessorContext lifetime must not exceed partition lifetime
- ⚠️ Must ensure no concurrent access to same partition state
- ⚠️ Requires careful coordination between modules

**Mitigation**:
- Document ownership in comments
- Add compile-time checks via Arc scoping
- Run comprehensive test suite

### Error Handling
- If state is None: Initialize empty state on first record
- If state mutation fails: Propagate error to caller
- If partition dies: State is dropped with PartitionStateManager

---

## Performance Impact Analysis

### Expected Improvements
- **HashMap clone elimination**: Currently happens 2× per batch (read + write)
  - Per batch (1000 records): ~20µs saved
  - With 100K batches/sec: ~2ms saved globally

- **Lock contention reduction**: No write lock needed to synchronize state
  - Per partition: eliminates 1 write lock per batch
  - With 8 partitions: eliminates 8 write locks per batch

- **Total expected**: 5-10% throughput improvement
  - Current Phase 6.4: 694K rec/sec
  - Target Phase 6.4C: 730-750K rec/sec

### No Downside Cases
- Memory usage: Same (just moved location)
- CPU: Less (no cloning)
- Latency: Same or better (no extra copies)

---

## Dependency: Phase 6.5

**Phase 6.4C enables Phase 6.5** (Window State Optimization):
- 6.4C establishes the pattern: state attached to partition manager
- 6.5 extends pattern to `window_v2_states` (same approach)
- If done in reverse order: Would require double refactoring

**Recommendation**: Complete 6.4C fully before starting 6.5

---

## Checklist for Implementation

### Phase 1: PartitionStateManager
- [ ] Add `group_by_states` field
- [ ] Update `new()` constructor
- [ ] Update `with_metrics()` constructor
- [ ] Add getter method
- [ ] Add initialization comment explaining O(1) on creation

### Phase 2: ProcessorContext
- [ ] Update field type
- [ ] Add new initialization path with state reference
- [ ] Update existing initialization for backward compatibility
- [ ] Add comment explaining state ownership

### Phase 3: Coordinator.rs
- [ ] Update `execute_batch_for_partition()` to use partition state
- [ ] Remove `get_group_states()` call
- [ ] Remove `set_group_states()` call
- [ ] Test with comprehensive baseline

### Phase 4: Common.rs
- [ ] Update all 4 locations that access group_states
- [ ] Remove `get_group_states()` calls
- [ ] Remove `set_group_states()` calls
- [ ] Test with V1 processor chain

### Phase 5: Engine.rs
- [ ] Delete `group_states` field
- [ ] Delete `get_group_states()` method
- [ ] Delete `set_group_states()` method
- [ ] Remove from constructor
- [ ] Update any internal usages
- [ ] Run full test suite

### Final Validation
- [ ] All 530+ unit tests pass
- [ ] All 5 benchmark scenarios complete
- [ ] Comprehensive baseline test shows 5-10% improvement
- [ ] Code formatting compliant
- [ ] Clippy checks pass
- [ ] Pre-commit checks pass

---

## Estimated Timeline

- **Phase 1**: 2 hours
- **Phase 2**: 2 hours
- **Phase 3**: 3 hours
- **Phase 4**: 3 hours
- **Phase 5**: 1 hour
- **Testing & fixes**: 3-4 hours
- **Total**: 14-16 hours (2 days)

---

## Success Metrics

✅ Code compiles without errors or warnings
✅ All tests pass (530+ unit tests)
✅ 5-10% throughput improvement measured
✅ State correctly maintained across batches
✅ No state leakage between partitions
✅ No performance regressions

---

## References

- **FR-082-STATE-SHARING-ANALYSIS.md** - Problem analysis
- **FR-082-SCHEDULE.md** - Overall roadmap
- **src/velostream/server/v2/partition_manager.rs** - Target location
- **src/velostream/sql/execution/processors/context.rs** - ProcessorContext definition
