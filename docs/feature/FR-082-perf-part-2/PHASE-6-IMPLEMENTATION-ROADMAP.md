# Phase 6 Implementation Roadmap: Lock-Free Structures

**Date**: November 9, 2025
**Target**: Single-core 70-142K rec/sec (3.8-7.6x improvement)
**Effort**: S (2-3 weeks for complete Phase 6)
**Risk**: Low (no algorithmic changes, only data structure replacements)

---

## Phase 6 Overview

Phase 6 will eliminate per-record coordination overhead by:
1. Converting global locks to per-entry locks (DashMap)
2. Converting counters to lock-free atomics
3. Reducing context switches via batch-level locking
4. Eliminating record cloning with Arc<StreamRecord>

**Expected Improvement**: 97.9% overhead → 85% overhead (13% absolute improvement)
**Throughput Target**: 16.6K rec/sec → 70-142K rec/sec

---

## Components for Lock-Free Conversion

### Priority 1: StreamExecutionEngine State (CRITICAL)

**File**: `src/velostream/sql/execution/engine.rs:152-173`

**Current State**:
```rust
pub struct StreamExecutionEngine {
    group_states: HashMap<String, Arc<GroupByState>>,  // ← NEEDS DASHMAP
    window_v2_states: HashMap<String, Box<dyn Any + Send + Sync>>,  // ← NEEDS DASHMAP
    record_count: u64,  // ← NEEDS ATOMICU64
    // ... other fields
}
```

**Problem**: All access protected by single `Arc<RwLock<StreamExecutionEngine>>`

**Solution**:
```rust
pub struct StreamExecutionEngine {
    group_states: Arc<DashMap<String, Arc<GroupByState>>>,  // ✅ Per-entry locking
    window_v2_states: Arc<DashMap<String, Box<dyn Any + Send + Sync>>>,  // ✅ Per-entry locking
    record_count: Arc<AtomicU64>,  // ✅ Lock-free counter
    // ... other fields unchanged
}
```

**Impact**: 20-30% improvement (eliminates global lock on state)
**Effort**: M (requires updating all access patterns)
**Risk**: Medium (state access patterns change)

---

### Priority 2: Batch-Level Locking (HIGH IMPACT)

**File**: `src/velostream/server/processors/simple.rs` (or relevant processor)

**Current Pattern**:
```rust
// BEFORE: Lock per record
for record in batch {
    let mut engine_guard = engine.write().await;  // ← LOCK PER RECORD
    engine_guard.execute_with_record(&query, &record)?;
    drop(engine_guard);  // ← UNLOCK
}
```

**Problem**: 1000 locks per batch (1000 records)
**Cost**: ~5000 context switches (engine.write().await)

**Solution**:
```rust
// AFTER: Lock per batch
let mut engine_guard = engine.write().await;  // ← LOCK ONCE
for record in batch {
    engine_guard.execute_with_record(&query, &record)?;  // NO AWAIT
}
drop(engine_guard);  // ← UNLOCK ONCE
```

**Impact**: 30% improvement (eliminates ~5000 context switches per batch)
**Effort**: S (simple loop restructuring)
**Risk**: Low (logic unchanged, just locking scope)

---

### Priority 3: Arc<StreamRecord> (RECORD CLONING)

**File**: `src/velostream/sql/execution/types.rs` and callers

**Current Pattern**:
```rust
engine_guard.execute_with_record(&query, record.clone())?;  // ← FULL CLONE
```

**Problem**: Every record cloned before execution
**Cost**: 5000 clones × 20 µs = 100 ms per batch

**Solution**:
```rust
let record_arc = Arc::new(record);  // ← CREATE ARC ONCE
engine_guard.execute_with_record(&query, record_arc)?;  // ← CLONE ARC (cheap)
```

**Impact**: 15% improvement (eliminates record cloning overhead)
**Effort**: S (signature changes in execute_with_record)
**Risk**: Low (Arc cloning is cheap, no semantics change)

---

### Priority 4: Atomic Metrics (OPTIONAL)

**File**: `src/velostream/server/v2/partition_manager.rs` (metrics)

**Current Pattern**:
```rust
// Metrics protected by Arc<Mutex<>>
metrics.record_batch_processed(1);  // ← MUTEX LOCK
```

**Solution**:
```rust
// Metrics using atomics
metrics.atomic_record_batch(1);  // ← ATOMIC ADD (lock-free)
```

**Impact**: 5% improvement (atomic ops 100x faster than mutex)
**Effort**: S (convert to AtomicU64)
**Risk**: Low (atomics well-understood)

---

## Phase 6 Execution Plan

### Week 1: DashMap Integration (Priority 1)

**Task 6.1.1: Convert group_states to DashMap**
```rust
// BEFORE
group_states: HashMap<String, Arc<GroupByState>>

// AFTER
group_states: Arc<DashMap<String, Arc<GroupByState>>>
```

**Changes Required**:
1. Add dashmap dependency to Cargo.toml
2. Update StreamExecutionEngine struct
3. Update all access patterns:
   - `group_states.get_mut(&key)` → `group_states.get_mut(&key)` (same API!)
   - `group_states.entry(key).or_insert()` → `group_states.entry(key).or_insert()`
4. Remove engine-level locks (per-entry locking replaces them)

**Tests to Update**:
- `tests/unit/sql/execution/engine_test.rs`
- `tests/unit/sql/execution/aggregation/group_by_test.rs`

**Expected Result**: GROUP BY state no longer has global lock contention

---

**Task 6.1.2: Convert window_v2_states to DashMap**
```rust
// BEFORE
window_v2_states: HashMap<String, Box<dyn Any + Send + Sync>>

// AFTER
window_v2_states: Arc<DashMap<String, Box<dyn Any + Send + Sync>>>
```

**Changes Required**: Same as group_states (similar access patterns)

**Tests to Update**:
- `tests/unit/sql/execution/windows/window_v2_test.rs`

**Baseline Benchmark**:
```bash
cargo test scenario_2_pure_group_by_baseline -- --nocapture
# Expected: 16.6K → 35-40K rec/sec
```

---

### Week 2: Batch-Level Locking + Record Arc (Priority 2-3)

**Task 6.2.1: Implement Batch-Level Locking**
```rust
// Find: for record in batch { engine.write().await }
// Replace: engine.write().await; for record in batch { /* no await */ }
```

**Files to Modify**:
- `src/velostream/server/processors/simple.rs` (V1 processor)
- `src/velostream/server/v2/partition_manager.rs` (process_record_with_sql)

**Measurement**: Count context switches before/after
```bash
perf stat -e context-switches cargo test scenario_2_pure_group_by_baseline
# Expected: ~30,000 → ~2,000 context switches
```

**Baseline Benchmark**:
```bash
cargo test scenario_2_pure_group_by_baseline -- --nocapture
# Expected: 35-40K → 50-60K rec/sec
```

---

**Task 6.2.2: Convert Records to Arc<StreamRecord>**
```rust
// BEFORE
fn execute_with_record(
    &mut self,
    query: &StreamingQuery,
    record: StreamRecord,  // ← OWNED CLONE
)

// AFTER
fn execute_with_record(
    &mut self,
    query: &StreamingQuery,
    record: Arc<StreamRecord>,  // ← SHARED ARC
)
```

**Files to Modify**:
- `src/velostream/sql/execution/engine.rs` (signature change)
- `src/velostream/server/processors/simple.rs` (caller)
- `src/velostream/server/v2/partition_manager.rs` (caller)
- All tests using execute_with_record

**Impact**:
- Eliminates 5000 record clones per batch
- Cheap Arc clones instead

**Baseline Benchmark**:
```bash
cargo test scenario_2_pure_group_by_baseline -- --nocapture
# Expected: 50-60K → 70-85K rec/sec
```

---

### Week 3: Validation & Performance Tuning

**Task 6.3.1: Comprehensive Testing**
```bash
# Run all scenario benchmarks
cargo test scenario_1_rows_window_baseline -- --nocapture
cargo test scenario_2_pure_group_by_baseline -- --nocapture
cargo test scenario_3a_tumbling_standard_baseline -- --nocapture
cargo test scenario_3b_tumbling_emit_changes_baseline -- --nocapture

# Measure improvement at each step
```

**Expected Results**:

| Scenario | Current | After DashMap | After Batch Lock | After Arc | Target |
|----------|---------|---------------|------------------|-----------|--------|
| Scenario 2 | 16.6K | 35K | 50K | 70K | 70-142K |
| Scenario 1 | 19.8K | 40K | 60K | 75K | 70-142K |
| Scenario 3a | 23.1K | 45K | 65K | 90K | 70-142K |

---

**Task 6.3.2: Profiling & Optimization**
```bash
# Profile with flamegraph to verify improvements
cargo install flamegraph
cargo flamegraph --test scenario_2_pure_group_by_baseline

# Check CPU utilization increase
# Current: ~2% (lock-bound)
# Target: ~25-35% (computation-bound)
```

---

**Task 6.3.3: Documentation & Commit**
- Update PHASE-6-SCHEDULE.md with actual results
- Document DashMap integration patterns
- Create PHASE-6-RESULTS.md with benchmarks
- Commit: "feat(FR-082 Phase 6): Lock-free structures - DashMap, Atomics, batch locking"

---

## Dependencies to Add

**Cargo.toml**:
```toml
[dependencies]
dashmap = "5.5"  # Lock-free concurrent HashMap
```

**No other external dependencies needed!**

---

## Code Patterns: Before & After

### Pattern 1: HashMap Access → DashMap

**Before** (requires global lock):
```rust
let mut engine = engine.write().await;
if let Some(state) = engine.group_states.get_mut(&key) {
    state.update(&record);
}
```

**After** (per-entry lock, no outer lock needed):
```rust
if let Some(mut state) = group_states.get_mut(&key) {
    state.update(&record);
}
```

---

### Pattern 2: Per-Record Locks → Batch Locking

**Before**:
```rust
for record in batch {
    let mut guard = engine.write().await;  // Lock per record!
    guard.execute_with_record(&query, record)?;
}  // Unlock per record
```

**After**:
```rust
let mut guard = engine.write().await;  // Lock once
for record in batch {
    guard.execute_with_record(&query, record)?;  // No await!
}
```

---

### Pattern 3: Record Clone → Arc

**Before**:
```rust
let record_clone = record.clone();  // Full allocation + copy
engine.execute_with_record(&query, record_clone)?;
```

**After**:
```rust
let record_arc = Arc::new(record);  // Allocation once
engine.execute_with_record(&query, record_arc.clone())?;  // Cheap Arc clone
```

---

## Risk Mitigation

### Risk 1: DashMap API Compatibility
**Mitigation**: DashMap provides Ref/RefMut types compatible with HashMap patterns
```rust
// Same API
let mut entry = map.get_mut(&key);
entry.update();
```

### Risk 2: Testing Coverage
**Mitigation**: Run full test suite after each change
```bash
cargo test --lib --no-default-features
# Expected: All 531+ tests pass
```

### Risk 3: Performance Regression
**Mitigation**: Benchmark before/after each optimization
```bash
# Verify improvement at each step
cargo test scenario_2_pure_group_by_baseline
```

---

## Success Criteria

✅ **Functional**:
- All 531+ unit tests pass
- Scenario 1-3 all process records correctly
- Metrics reported accurately
- GROUP BY produces correct aggregations

✅ **Performance**:
- Throughput: 70-142K rec/sec (minimum 70K)
- Per-core efficiency: 80%+ (vs 2% current)
- Context switches: <2,000 per batch (vs 30,000 current)

✅ **Quality**:
- Code formatting: `cargo fmt --all -- --check` ✅
- Clippy linting: `cargo clippy --all-targets` ✅
- Documentation: PHASE-6-RESULTS.md with detailed analysis

---

## Timeline

| Week | Task | Target | Status |
|------|------|--------|--------|
| 1 | DashMap integration | 35-40K rec/sec | Pending |
| 2 | Batch locking + Arc | 70-85K rec/sec | Pending |
| 3 | Validation + tuning | 70-142K rec/sec | Pending |

**Critical Path**: All 3 weeks required for full Phase 6 completion
**Fast Track**: Batch locking alone (Week 2) = quick 30% win

---

## Next Steps (Immediate)

1. **Add dashmap to Cargo.toml**
   ```bash
   cargo add dashmap
   ```

2. **Start Task 6.1.1**: Convert group_states to DashMap
   - File: `src/velostream/sql/execution/engine.rs`
   - Lines: 152-173

3. **Update access patterns**
   - Search: `.get_mut(&key)`
   - Verify DashMap API compatibility

4. **Run baseline benchmark**
   ```bash
   cargo test scenario_2_pure_group_by_baseline -- --nocapture
   ```

5. **Measure improvement**: Expected 35-40K rec/sec

---

## Performance Expectation Justification

**Current overhead breakdown** (from PHASE-6-DEEP-ANALYSIS):
- Context switches: ~40%
- Record cloning: ~15%
- Channel ops: ~30%
- Other: ~15%

**Phase 6 targets**:
- Batch locking reduces context switches: 40% → 10% (saves 30%)
- Arc<Record> eliminates cloning: 15% → 0% (saves 15%)
- DashMap reduces lock contention: 5% → 2% (saves 3%)
- **Total**: ~13% absolute improvement (97% → 85%)

**Throughput calculation**:
```
Current: 16.6K rec/sec (overhead = 97%)
Target: 16.6K / (1 - 0.85) = 16.6K / 0.15 = 110.7K rec/sec

Conservative estimate: 70-142K rec/sec
Realistic range: 85-110K rec/sec
```

---

*Phase 6 is the foundation for single-core breakthrough.*
*Phase 7 will add vectorization for additional 4-8x improvement.*
*Combined: 1.5M+ rec/sec target becomes achievable.*
