# FR-082: Phase 6.6 - Synchronous Partition Receivers Implementation Plan

**Phase**: 6.6
**Title**: Synchronous Partition Receivers (Async/Arc/Mutex Elimination)
**Status**: 📋 PLANNED (Ready to start after Phase 6.5B completion)
**Duration**: 9-13 days (2-3 weeks)
**Expected Improvement**: 2-3x throughput (47K → 100-140K rec/sec for Scenario 0)
**Last Updated**: November 11, 2025

---

## Executive Summary

Phase 6.6 eliminates async/await complexity from the hot path by introducing synchronous partition receivers that own their execution engines directly (no Arc/Mutex wrappers). This removes ~15-25% of architectural overhead that comes from async state machines, Arc allocations, and channel operations.

**Key Strategy**: Direct ownership model where each partition receiver thread owns:
- StreamExecutionEngine (no Arc wrapper)
- StreamingQuery reference
- ProcessorContext (no Mutex wrapper)
- Synchronized record processing loop

**Target**: 100-140K rec/sec (2-3x improvement over 47-48K baseline)

---

## Performance Analysis

### Current Bottleneck Breakdown (Phase 6.5B baseline)

**Current Architecture**: Async/Arc/Mutex wrappers on partition receiver

```
Per-record processing cost (21 microseconds):
├─ Lock overhead: 0.1-0.3% (~2.1ns) - negligible
├─ Arc allocations: ~2-5% (~420-1050ns) - allocation overhead
├─ Channel operations: ~3-5% (~630-1050ns) - MPSC channel latency
├─ Async/await overhead: ~5-10% (~1050-2100ns) - runtime state machine
└─ Architectural indirection: ~5-10% (~1050-2100ns) - method call chains
```

**Total Removable Overhead**: ~15-25% (3150-5250 nanoseconds per record)

### Phase 6.6 Elimination Strategy

**Arc Allocations** (2-5% → 0%):
- BEFORE: Arc<StreamExecutionEngine> in shared state
- AFTER: Direct owned engine in PartitionReceiver
- IMPACT: Eliminate 5-10K Arc allocations per batch

**Mutex Operations** (0.1-0.3% → 0%):
- BEFORE: tokio::sync::Mutex on engine/query/context
- AFTER: Direct mutable ownership in receiver thread
- IMPACT: Eliminate 15-30K lock operations per batch

**Channel Operations** (3-5% → 0.5%):
- BEFORE: MPSC channels for every record routing
- AFTER: Direct thread-to-partition handoff
- IMPACT: 90% reduction in channel latency

**Async/Await** (5-10% → 0%):
- BEFORE: async process_record_with_sql()
- AFTER: sync process_record()
- IMPACT: Eliminate async state machine overhead

**Architectural Indirection** (5-10% → 1-2%):
- BEFORE: Coordinator -> Partition manager -> Receiver -> Engine
- AFTER: Receiver owns engine directly
- IMPACT: Simplified call chains

### Expected Performance

```
Phase 6.5B:  47-48K rec/sec    (current baseline, single partition receiver)
Phase 6.6:   100-140K rec/sec  (2.1-2.9x improvement)
   With 8 partitions: 800K-1.1M rec/sec (total throughput)
```

---

## Architecture Design

### Current Architecture (Phase 6.5B)

```
Coordinator
  ├─ PartitionStateManager[0]
  │  └─ execution_engine: tokio::sync::Mutex<StreamExecutionEngine>
  │     └─ ProcessorContext (via Arc<Mutex>)
  ├─ PartitionStateManager[1]
  │  └─ execution_engine: tokio::sync::Mutex<StreamExecutionEngine>
  └─ ...

Partition Receiver Task (async)
  └─ Locks engine -> calls execute_with_record (async)
     └─ Multiple Mutex guards held simultaneously
```

**Problems**:
- Multiple lock operations per record
- Async state machine overhead
- Contention on shared state
- Arc allocations per message

### Phase 6.6 Architecture (Target)

```
Coordinator
  ├─ PartitionReceiver[0] (owned thread)
  │  ├─ execution_engine: StreamExecutionEngine (direct ownership)
  │  ├─ query: StreamingQuery (owned reference)
  │  ├─ context: ProcessorContext (direct ownership)
  │  └─ receiver: mpsc::Receiver<Vec<StreamRecord>> (shared channel)
  ├─ PartitionReceiver[1] (owned thread)
  │  └─ ...
  └─ ...

Partition Receiver Thread (synchronous)
  └─ while let Some(batch) = receiver.recv() {
       for record in batch {
         // Direct ownership, no locks
         engine.process_record(&record)?;
       }
     }
```

**Benefits**:
- No locks in hot path
- Synchronous processing (no async overhead)
- Direct ownership (no Arc allocations)
- Clean separation of concerns

---

## Implementation Phases

### Phase 1: Create PartitionReceiver Struct (2-3 days)

**File**: Create `src/velostream/server/v2/partition_receiver.rs` (~300 lines)

**Responsibilities**:
1. Own execution engine, query, context directly (no Arc/Mutex)
2. Implement synchronous record processing loop
3. Manage partition-specific state
4. Track metrics per partition

**Key Methods**:
```rust
pub struct PartitionReceiver {
    partition_id: usize,
    execution_engine: StreamExecutionEngine,  // Direct ownership
    query: Arc<StreamingQuery>,
    context: ProcessorContext,               // Direct ownership
    receiver: mpsc::Receiver<Vec<StreamRecord>>,
    metrics: Arc<PartitionMetrics>,
}

impl PartitionReceiver {
    pub async fn run(mut self) {
        // Main event loop
        while let Some(batch) = self.receiver.recv().await {
            self.process_batch(&batch).await?;
        }
    }

    async fn process_batch(&mut self, records: &[StreamRecord]) -> Result<()> {
        for record in records {
            self.execution_engine.execute_with_record(&self.query, record).await?;
        }
        self.metrics.record_batch_processed(records.len() as u64);
    }
}
```

**Test Coverage**:
- Basic partition receiver creation
- Record processing loop
- Metric tracking
- Cleanup on shutdown

**LoE Breakdown**:
- Core struct definition: 4-6 hours
- Process loop implementation: 6-8 hours
- Tests and validation: 4-5 hours
- Total: 14-19 hours (1.8-2.4 days)

### Phase 2: Refactor PartitionStateManager (3-4 days)

**File**: Modify `src/velostream/server/v2/partition_manager.rs`

**Changes**:
1. Remove Arc<Mutex> wrappers
2. Add methods to initialize PartitionReceiver
3. Update initialization_partitions() to create receivers
4. Simplify state ownership

**Key Changes**:
```rust
// BEFORE (Phase 6.5B)
pub struct PartitionStateManager {
    partition_id: usize,
    execution_engine: tokio::sync::Mutex<Option<StreamExecutionEngine>>,
    query: Arc<tokio::sync::Mutex<Option<Arc<StreamingQuery>>>>,
    // ...
}

// AFTER (Phase 6.6)
pub struct PartitionStateManager {
    partition_id: usize,
    metrics: Arc<PartitionMetrics>,
    // (engine and query are now owned by PartitionReceiver)
}

// New method to create partition receiver
pub fn create_receiver(
    self,
    engine: StreamExecutionEngine,
    query: Arc<StreamingQuery>,
    context: ProcessorContext,
    receiver: mpsc::Receiver<Vec<StreamRecord>>,
) -> PartitionReceiver {
    // Create and return PartitionReceiver
}
```

**Test Updates**:
- Update existing partition manager tests
- Add PartitionReceiver creation tests
- Verify state isolation between partitions

**LoE Breakdown**:
- Remove Arc/Mutex wrappers: 4-6 hours
- Add receiver creation methods: 4-5 hours
- Update tests: 6-8 hours
- Integration testing: 4-5 hours
- Total: 18-24 hours (2.3-3.0 days)

### Phase 3: Update Coordinator (2-3 days)

**File**: Modify `src/velostream/server/v2/coordinator.rs`

**Changes**:
1. Update initialize_partitions() to create receivers
2. Create partition receiver threads instead of tasks
3. Simplify batch routing (direct to partition)
4. Remove Arc<RwLock> engine handling

**Key Changes**:
```rust
// BEFORE (Phase 6.5B)
pub fn initialize_partitions(&self) -> (Vec<Arc<PartitionStateManager>>, Vec<mpsc::Sender<...>>) {
    let partition_managers = (0..self.num_partitions())
        .map(|id| Arc::new(PartitionStateManager::new(id)))
        .collect();

    // Spawn async tasks
    let tasks = partition_managers.iter().map(|manager| {
        let manager_clone = Arc::clone(manager);
        tokio::spawn(async move {
            // ... async receiver loop
        })
    }).collect();

    (partition_managers, tasks)
}

// AFTER (Phase 6.6)
pub fn initialize_partitions(&self) -> (Vec<PartitionReceiver>, Vec<mpsc::Sender<...>>) {
    let receivers = (0..self.num_partitions())
        .map(|id| {
            let engine = StreamExecutionEngine::new();
            let query = self.query.clone();
            let context = ProcessorContext::new();
            let (sender, receiver) = mpsc::channel(1000);

            // Create owned receiver
            PartitionReceiver::new(id, engine, query, context, receiver)
        })
        .collect();

    (receivers, senders)
}
```

**Key Benefits**:
- Simpler ownership model
- Direct partition-to-engine mapping
- Cleaner initialization path

**Test Updates**:
- Update coordinator tests for new receiver model
- Add integration tests for direct ownership
- Verify batch routing without Arc/Mutex

**LoE Breakdown**:
- Update initialize_partitions(): 4-5 hours
- Update batch routing: 3-4 hours
- Remove Arc/Mutex handling: 2-3 hours
- Update tests: 6-8 hours
- Total: 15-20 hours (1.9-2.5 days)

### Phase 4: Update Job Processor (1-2 days)

**File**: Modify `src/velostream/server/v2/job_processor_v2.rs`

**Changes**:
1. Update process_job() for new receiver model
2. Simplify record routing (no strategy overhead)
3. Remove Arc/Mutex references
4. Update documentation

**Key Changes**:
```rust
// Simplified job processing with owned receivers
let job_coordinator = PartitionedJobCoordinator::new(config)
    .with_query(Arc::clone(&query_arc));

let (partition_receivers, partition_senders) = job_coordinator.initialize_partitions();

// Route records directly to partition channels
for sender in &partition_senders {
    sender.send(batch).await?;
}

// Receivers own their engines and process synchronously
for receiver in partition_receivers {
    tokio::spawn_blocking(move || {
        receiver.run_blocking()  // Synchronous processing
    });
}
```

**LoE Breakdown**:
- Update process_job() logic: 3-4 hours
- Simplify record routing: 2-3 hours
- Tests and validation: 4-5 hours
- Total: 9-12 hours (1.1-1.5 days)

### Phase 5: Test Updates (1-2 days)

**Files to Update**:
1. `tests/unit/server/v2/coordinator_test.rs`
2. `tests/unit/server/v2/partition_manager_test.rs`
3. `tests/unit/server/v2/job_processor_test.rs`
4. `tests/integration/server/v2/partition_receiver_test.rs`
5. `tests/integration/performance/partition_receiver_benchmark.rs`

**Test Coverage**:
- PartitionReceiver creation and initialization
- Synchronous record processing
- Metric tracking
- State isolation between partitions
- Batch routing correctness
- Performance baseline validation

**LoE Breakdown**:
- Update existing tests: 6-8 hours
- Create new partition receiver tests: 6-8 hours
- Performance testing: 4-5 hours
- Total: 16-21 hours (2.0-2.6 days)

### Phase 6: Performance Validation (1 day)

**Goals**:
1. Verify 2-3x improvement (100-140K rec/sec target)
2. Validate no regressions in correctness
3. Benchmark across all scenarios
4. Document findings

**Benchmarks to Run**:
- Scenario 0 (Pure SELECT)
- Scenario 1 (Filter)
- Scenario 2 (GROUP BY)
- Scenario 3a (Tumbling + GROUP BY)
- Scenario 3b (EMIT CHANGES)

**Success Criteria**:
- ✅ 2-3x improvement confirmed (100K+ rec/sec minimum)
- ✅ All tests passing (528+ unit tests)
- ✅ No behavioral regressions
- ✅ Clean code compilation

---

## Week-by-Week Schedule

### Week 1 (Days 1-3): Foundations
- **Day 1**: Phase 1 - PartitionReceiver struct (2-3 days)
  - Create partition_receiver.rs
  - Implement basic structure
  - Basic tests
- **Day 2-3**: Continue Phase 1 + Start Phase 2
  - Complete PartitionReceiver tests
  - Begin PartitionStateManager refactoring

### Week 2 (Days 4-7): Core Implementation
- **Days 4-5**: Complete Phase 2 - PartitionStateManager refactoring (3-4 days)
  - Remove Arc/Mutex wrappers
  - Add receiver creation methods
  - Update tests
- **Days 6-7**: Start Phase 3 - Coordinator updates (2-3 days)
  - Update initialize_partitions()
  - Simplify batch routing

### Week 3 (Days 8-13): Integration & Validation
- **Days 8-9**: Complete Phase 3 + Phase 4
  - Finish coordinator updates
  - Update job processor
  - Integration testing
- **Days 10-11**: Phase 5 - Test updates (1-2 days)
  - Comprehensive test coverage
  - Edge case testing
- **Days 12-13**: Phase 6 - Performance validation
  - Benchmark all scenarios
  - Documentation and reporting

---

## Risk Analysis & Mitigation

### Risk 1: Thread Safety Issues (Medium Risk)

**Risk**: Direct ownership removes synchronization guarantees that Arc<Mutex> provided.

**Mitigation**:
1. Partition receiver thread owns all state exclusively
2. No state sharing between partitions (except metrics)
3. Extensive unit tests for state isolation
4. Integration tests for concurrent partition processing
5. Use Rust's type system to prevent sharing

**Prevention**:
- Code review focusing on ownership and borrowing
- Compile-time checks (Rust borrow checker)
- Runtime assertions for state isolation
- Partition-level integration tests

### Risk 2: Performance Regression (Low Risk)

**Risk**: Changes could introduce unexpected overhead.

**Mitigation**:
1. Baseline benchmark before implementation
2. Continuous performance validation during work
3. Compare with Phase 6.5B baseline (47-48K rec/sec)
4. Early exit if regression > 10%

**Prevention**:
- Daily performance checks
- Flamegraph profiling after each phase
- Keep Phase 6.5B as fallback (git branch)

### Risk 3: Incomplete Testing (Medium Risk)

**Risk**: Synchronous model may have edge cases not caught by tests.

**Mitigation**:
1. Comprehensive test suite (528+ tests)
2. Integration tests for all scenarios
3. Stress testing with high message rates
4. Long-running stability tests

**Prevention**:
- Test-driven development approach
- Pair testing (unit + integration)
- Performance test suite

### Risk 4: Architectural Mismatch (Low Risk)

**Risk**: Synchronous receivers may not integrate cleanly with rest of system.

**Mitigation**:
1. Incremental integration (one component at a time)
2. Backward compatibility with job processor interface
3. Comprehensive integration tests
4. Documentation of architectural changes

**Prevention**:
- Design review before implementation
- Integration with existing coordinator API
- Maintain JobProcessor trait compatibility

---

## Success Criteria

### Functional Correctness
✅ All 528+ unit tests pass
✅ All integration tests pass
✅ No behavioral changes from Phase 6.5B
✅ State isolation verified between partitions

### Performance Targets
✅ Minimum 2x improvement (100K rec/sec for Scenario 0)
✅ Target 2-3x improvement (140K rec/sec ideal)
✅ No regressions in other scenarios
✅ Stable performance (< 5% variance)

### Code Quality
✅ Zero compiler errors
✅ No clippy warnings (new ones)
✅ Code formatted with cargo fmt
✅ Comprehensive documentation

### Documentation
✅ Implementation guide created
✅ Architecture diagrams updated
✅ Code comments explain ownership model
✅ Phase 6.6 completion summary

---

## Dependency Chain

### Enables Phase 6.7
Phase 6.6 enables **zero-copy record processing**:
- Synchronous path allows lifetime-based zero-copy
- No async complexity to manage borrows
- Can implement streaming processors
- Expected: +10-20% improvement over Phase 6.6

### Related to Phase 6.8
Phase 6.8 (lock-free metrics) becomes simpler:
- No Arc<Mutex> locks to optimize
- Can use atomic operations directly
- Per-partition metrics without contention
- Expected: +1-5% improvement

---

## Performance Trajectory

```
Phase 6.4:   694K rec/sec (per-partition engine migration)
Phase 6.5:   ~750K rec/sec (+5-10% window state optimization)
Phase 6.5B:  ~47-48K rec/sec** (single partition receiver baseline)
Phase 6.6:   ~100-140K rec/sec (2-3x improvement, async elimination)
Phase 6.7:   ~120-170K rec/sec (+10-20%, zero-copy processing)
Phase 6.8:   ~125-180K rec/sec (+5%, lock-free metrics)
Phase 7:     ~300-500K rec/sec (+2-3x, vectorization & SIMD)

** Note: Phase 6.6 targets single-partition performance (current 47K)
After Phase 6.6, multi-partition throughput = single × 8 = 800K-1.1M rec/sec
```

---

## Next Steps After Phase 6.6

1. **Performance Validation**: Verify 2-3x improvement achieved
2. **Documentation**: Create Phase 6.6 completion summary
3. **Phase 6.7 Planning**: Zero-copy record processing design
4. **Long-term**: Vectorization (Phase 7) roadmap

---

## References

- **Phase 6.5B**: Scoped Borrow State Persistence (foundation)
- **Phase 6.4**: Per-Partition Engine Architecture (ownership model)
- **FR-082-SCHEDULE.md**: Overall roadmap and timeline

---

**Approval Date**: November 11, 2025
**Status**: Ready for implementation
**Assigned**: Available for next dev cycle
**Estimated Start**: After Phase 6.5B completion verification
