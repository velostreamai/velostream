# FR-082 Phase 6.4: Lock-Free Optimization Plan

**Date**: November 9, 2025
**Status**: ⏳ Planning
**Phase 2 Foundation**: All 5 scenarios measured and production-ready
**Target Duration**: 1-2 weeks
**Target Improvement**: 1.5-2x additional speedup (cumulative 4.5-6x from Phase 6.2 baseline)

---

## Executive Summary

Phase 6.3 delivered excellent results:
- **All 5 scenarios measured** with V2 outperforming SQL Engine (2.7-4.7x)
- **PartitionerSelector auto-selection** working correctly
- **Architecture validation**: Per-batch RwLock approach is sound
- **Production ready**: V2 exceeds expectations

**Phase 6.4 objective**: Eliminate remaining lock contention and reduce synchronization overhead through lock-free data structures and atomic operations.

**Expected improvements**:
1. Reduce per-batch RwLock contention (estimated 20-30% overhead)
2. Implement lock-free queues for inter-partition communication (estimated 10-15% overhead)
3. Atomic-based window state coordination (estimated 5-10% overhead)
4. **Cumulative target: 1.5-2x improvement (35-55% reduction)**

---

## Current State Analysis

### Lock Usage Patterns (Phase 6.3)

**Primary Locks** (in order of contention impact):

1. **StreamExecutionEngine RwLock** (src/velostream/server/v2/coordinator.rs)
   - Held during batch processing
   - **Contention**: HIGH (every batch acquisition)
   - **Duration**: 10-50µs per batch (most significant)
   - **Current pattern**:
     ```rust
     let mut engine = Arc::new(RwLock::new(StreamExecutionEngine::new(...)));
     // acquire lock for entire batch processing
     ```
   - **Optimization opportunity**: CRITICAL

2. **Partition State RwLock** (src/velostream/server/v2/partition_manager.rs)
   - Manages window state, accumulator state
   - **Contention**: MEDIUM (stateful queries only)
   - **Duration**: 1-10µs per record
   - **Current pattern**: Per-record access during aggregation
   - **Optimization opportunity**: HIGH

3. **Inter-partition Channels** (mpsc, broadcast)
   - Used for batch communication between partitions
   - **Contention**: MEDIUM (queue operations)
   - **Duration**: 0.1-1µs per batch
   - **Current pattern**: Unbounded channels
   - **Optimization opportunity**: MEDIUM

4. **Window State Accumulation** (src/velostream/sql/execution/window_v2/)
   - RwLock on aggregator state
   - **Contention**: MEDIUM (window operations)
   - **Duration**: 1-5µs per window operation
   - **Optimization opportunity**: HIGH

### Measured Baselines (Phase 6.3)

| Metric | Scenario 0 | Scenario 1 | Scenario 2 | Scenario 3a | Scenario 3b |
|--------|-----------|-----------|-----------|-----------|-----------|
| Current Throughput | 877.6K | 468.5K | 272K | 1,092.4K | 2.9K input |
| Lock acquisitions/batch | 2 (read+write) | 2 | 2 | 2 | 2 |
| Estimated lock time% | 5-10% | 10-15% | 15-20% | 5-10% | 20-30% |
| **Optimization potential** | **+15-25%** | **+20-35%** | **+25-40%** | **+20-35%** | **+35-50%** |

---

## Lock-Free Optimization Strategies

### Strategy 1: Atomic-Based Engine State (CRITICAL - 35-50% of lock overhead)

**Problem**: Every batch holds RwLock on StreamExecutionEngine during execution (10-50µs)

**Current Code Pattern**:
```rust
let engine = Arc::new(RwLock::new(StreamExecutionEngine::new(...)));
// In batch processing:
let mut engine_guard = engine.write().await; // CONTENTION POINT
execute_batch(&mut engine_guard)?;
drop(engine_guard); // Released after batch
```

**Lock-Free Approach**:

Use atomic references and thread-safe interior mutability instead of RwLock:

```rust
// Instead of:
let engine = Arc::new(RwLock::new(StreamExecutionEngine::new(...)));

// Use atomic with thread-safe shared state:
let engine = Arc::new(AtomicRef::new(StreamExecutionEngine::new(...)));
// OR for state mutations: CrossbeamQueue + atomic operations

// Benefit: No lock acquisition on critical path
// Cost: Slightly more complex concurrent access pattern
```

**Implementation Steps**:
1. Analyze all writes to `StreamExecutionEngine` state
2. Identify which fields truly need mutual exclusion
3. Replace RwLock with:
   - **Arc<AtomicPtr<T>>** for read-mostly state
   - **Arc<Mutex<T>>** (if mutual exclusion still needed) for rarely-accessed state
   - **Atomic<T>** for simple state (counters, flags)
4. Use epoch-based reclamation for safe deallocations

**Estimated Improvement**: **+30-40%**

---

### Strategy 2: Lock-Free Queue for Partition Communication (MEDIUM - 10-15% of lock overhead)

**Problem**: Unbounded mpsc channels with internal locking

**Current Pattern**:
```rust
let (output_tx, output_rx) = mpsc::unbounded_channel();
// Implicit locks in channel implementation
```

**Lock-Free Approach**: Use crossbeam-queue (already dependency):

```rust
use crossbeam::queue::SegQueue;

let queue = Arc::new(SegQueue::new());
// Completely lock-free MPMC queue
// No allocations after initialization
```

**Benefits**:
- No blocking locks
- Better cache locality
- Bounded memory usage (SegQueue pre-allocates)

**Implementation Steps**:
1. Replace `mpsc::unbounded_channel()` with `SegQueue`
2. Update partition manager to use queue API
3. Batch send/receive operations
4. Implement graceful shutdown via atomic flag

**Estimated Improvement**: **+10-15%**

---

### Strategy 3: Atomic Window State Coordination (MEDIUM - 5-15% of lock overhead)

**Problem**: Window state uses RwLock for accumulation during per-record processing

**Current Pattern** (src/velostream/sql/execution/window_v2/):
```rust
let accumulator = Arc::new(RwLock::new(WindowAccumulator::new()));
// Per-record:
let mut acc = accumulator.write().await;
acc.add_value(record.value);
```

**Lock-Free Approach**: Use thread-local accumulation + periodic flush:

```rust
// Thread-local per-partition accumulator (no synchronization!)
thread_local! {
    static ACCUMULATOR: RefCell<WindowAccumulator> =
        RefCell::new(WindowAccumulator::new());
}

// Per-record: no locking at all
ACCUMULATOR.with(|acc| {
    acc.borrow_mut().add_value(record.value);
});

// Per-batch: atomic swap to shared state
// Only synchronize at batch boundaries (2-4 times vs 1000+ times)
```

**Benefits**:
- Zero synchronization overhead per-record
- Batch-level atomic swaps instead of per-record locks
- Better CPU cache locality

**Implementation Steps**:
1. Identify window state that needs coordination
2. Extract thread-local accumulators
3. Replace per-record RwLock with batch-level atomic operations
4. Update window emission to use atomic swaps

**Estimated Improvement**: **+10-20%**

---

### Strategy 4: Compare-And-Swap for Partition Metadata (LOW - 2-5% of lock overhead)

**Problem**: Partition state updates use RwLock for metadata

**Lock-Free Approach**: Use AtomicUsize/AtomicBool for metrics and flags:

```rust
pub struct PartitionMetadata {
    // Instead of:
    // state: RwLock<PartitionState>,

    // Use atomics for status:
    is_active: AtomicBool,
    record_count: AtomicUsize,
    batch_count: AtomicUsize,

    // Keep RwLock only for complex state that truly needs it
    config: RwLock<PartitionConfig>,
}

// Update with CAS operations:
self.record_count.fetch_add(count, Ordering::Relaxed);
```

**Benefits**:
- Avoids lock acquisition for metric updates
- Atomic operations are faster than lock acquisition
- Better performance on high-core-count systems

**Implementation Steps**:
1. Identify all simple state that can use atomics
2. Convert counters/flags to Atomic<T>
3. Use appropriate memory ordering (Relaxed for most metrics, Release/Acquire for coordination)
4. Update all access patterns

**Estimated Improvement**: **+2-5%**

---

## Implementation Roadmap

### Phase 6.4.1: Foundation (Days 1-2)
**Goal**: Analyze and design lock-free patterns

- [ ] Profile current lock contention (identify actual bottlenecks)
- [ ] Design atomic-based engine state API
- [ ] Design lock-free queue migration plan
- [ ] Document memory ordering requirements (Relaxed/Acquire/Release)

**Deliverable**: Design document with code examples

---

### Phase 6.4.2: Atomic Engine State (Days 3-5)
**Goal**: Replace RwLock with lock-free state management

- [ ] Create `AtomicEngineState` wrapper
- [ ] Migrate all engine state writes to atomic operations
- [ ] Add safety mechanisms for concurrent access
- [ ] Update all call sites
- [ ] Test: All 5 scenarios pass with new state management
- [ ] Measure: Throughput improvements

**Deliverable**: Working atomic-based engine state

---

### Phase 6.4.3: Lock-Free Queues (Days 6-7)
**Goal**: Replace unbounded channels with crossbeam queues

- [ ] Identify all unbounded_channel uses in V2 pipeline
- [ ] Create queue abstraction wrapper
- [ ] Migrate to SegQueue
- [ ] Update consumer code
- [ ] Test: All 5 scenarios pass
- [ ] Measure: Queue operation overhead reduction

**Deliverable**: All inter-partition communication lock-free

---

### Phase 6.4.4: Window State Optimization (Days 8-9)
**Goal**: Eliminate per-record window state locking

- [ ] Analyze window state access patterns
- [ ] Implement thread-local accumulators
- [ ] Design batch-level atomic swaps
- [ ] Update window processors
- [ ] Test: Window operations still correct
- [ ] Measure: Per-record synchronization overhead reduction

**Deliverable**: Lock-free window state coordination

---

### Phase 6.4.5: Verification & Optimization (Days 10-14)
**Goal**: Comprehensive testing and final optimizations

- [ ] Run all 5 scenario benchmarks (release mode)
- [ ] Profile for remaining lock contention
- [ ] Implement partition metadata atomics
- [ ] Fine-tune memory ordering
- [ ] Memory ordering audit (prevent race conditions)
- [ ] Documentation updates
- [ ] Pre-commit validation

**Deliverable**: Production-ready lock-free V2 architecture

---

## Expected Results

### Performance Target

| Scenario | Current (Phase 6.3) | Target (Phase 6.4) | Improvement |
|----------|-----------|-----------|-----------|
| **Scenario 0** | 877.6K | 1,100K+ | +25% |
| **Scenario 1** | 468.5K | 625K+ | +34% |
| **Scenario 2** | 272K | 380K+ | +40% |
| **Scenario 3a** | 1,092.4K | 1,500K+ | +37% |
| **Scenario 3b** | 2.9K input | 4.0K+ input | +38% |

**Cumulative Improvement from Phase 6.2 Baseline**:
- Phase 6.2: 4.7x (vs V1)
- Phase 6.4: **6-7x** (vs V1)
- **Lock-free gains: 1.3-1.5x**

---

## Risk Assessment

### Low Risk
✅ Lock-free patterns are well-established in Rust
✅ Crossbeam queue is production-tested
✅ Atomic operations have well-defined semantics
✅ No changes to business logic, only synchronization

### Medium Risk
⚠️ Memory ordering complexity (but well-documented)
⚠️ Testing concurrent scenarios (robust test suite needed)
⚠️ Performance validation across all query types

### Mitigation
- Comprehensive unit tests for all lock-free components
- ThreadSanitizer validation (to catch data races)
- Performance testing on multi-core systems (4/8/16 cores)
- Code review focus on memory ordering

---

## Success Criteria

✅ **Phase 6.4 Complete When**:
1. All 5 scenarios benchmark at target performance
2. No data races detected (Miri/ThreadSanitizer pass)
3. 530+ unit tests passing
4. Code formatting + clippy pass
5. Memory ordering audit complete
6. Documentation updated
7. Performance report generated

✅ **Production Ready When**:
- 1.5x+ improvement verified
- Zero regression in any scenario
- All safety invariants maintained
- PR approved and merged

---

## Next Steps

**Immediate Actions** (Start Phase 6.4):
1. Profile current V2 to identify actual lock contention
2. Design atomic-based engine state API
3. Create working prototype
4. Measure improvements

**Timeline**: 1-2 weeks to production-ready

---

## References

- **Current State**: FR-082-PHASE6-3-COMPLETE-BENCHMARKS.md
- **Baseline**: All 5 scenarios measured (Scenarios 1 & 3a newly added)
- **Previous Phases**:
  - Phase 6.2: Per-batch RwLock optimization
  - Phase 6.3: Auto-selection + comprehensive measurements
  - Phase 6.4: Lock-free structures (this plan)
  - Phase 6.5: Pure STP architecture
  - Phase 6.6+: Production validation

