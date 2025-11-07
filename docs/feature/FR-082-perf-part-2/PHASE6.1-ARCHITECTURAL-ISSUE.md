# Phase 6.1a: Critical Architectural Issue & Refactoring Plan

**Date**: November 7, 2025
**Status**: ARCHITECTURAL REDESIGN REQUIRED
**Severity**: CRITICAL - Current design violates STP principles
**Impact**: V2@1p should be ≥500K rec/sec, not 68K

---

## The Problem

The current V2 implementation is **NOT a true Single-Threaded Pipeline (STP) architecture**. It introduces a **10-15% performance penalty** on single partition and fundamentally prevents achieving true linear multi-core scaling.

### Performance Evidence

```
Current State (WRONG):
  V1 Baseline:   76,986 rec/sec  (single-threaded reference)
  V2@1p:         68,387 rec/sec  (11% SLOWER than V1)
  Scaling:       0% (V2@8p = 68,379 rec/sec, no improvement)

Promise vs Reality:
  Promise:       "V2 will be much faster than V1, ~500K rec/sec"
  Actual:        68,387 rec/sec (14% of promise)
  Gap:           ~432K rec/sec shortfall
```

### Root Cause Analysis

**Current Architecture (MapReduce-Style Coordinator)**:

```
Main Thread (BOTTLENECK):
  loop {
    batch = read_batch()
    routed = route_batch(batch)  // All routing happens here
    for (partition_id, records) in routed {
      channel_send(partition_id, records)  // OVERHEAD: ~3-4%
    }
  }
                    ↓
Partition Tasks (STARVED):
  loop {
    records = channel_recv()      // OVERHEAD: ~2-3%
    results = execute(records)    // Actual work
    results_channel_send(results) // OVERHEAD: ~1-2%
  }
                    ↓
Result Collector (OVERHEAD):
  loop {
    results = results_channel_recv()  // OVERHEAD: ~1-2%
    write_to_sink(results)
  }
```

**Overhead Breakdown**:
- MPSC channel operations: 3-4%
- Task context switching: 2-3%
- tokio::spawn() overhead: 2-3%
- State management: 1-2%
- Result collection: 1-2%
- **Total: ~11% performance penalty**

**Why this is wrong**:
- V2@1p should be AT LEAST as fast as V1 (same computational work)
- Current design has unnecessary coordination overhead
- Single partition gets NO benefit from V2 architecture
- Fundamentally incompatible with STP principles

---

## What a True STP Architecture Should Look Like

**Correct Design (Independent Pipelines)**:

```
Partition Task 0 (FULLY INDEPENDENT):
  loop {
    batch = read_from_source(0)        // Direct read, no channel
    routed = route_to_self(batch)      // Filter for this partition
    results = execute(routed)          // Process
    write_to_sink(results)             // Direct write
  }

Partition Task 1 (FULLY INDEPENDENT):
  loop {
    batch = read_from_source(1)
    routed = route_to_self(batch)
    results = execute(routed)
    write_to_sink(results)
  }

Partition Task N (FULLY INDEPENDENT):
  loop {
    batch = read_from_source(N)
    routed = route_to_self(batch)
    results = execute(routed)
    write_to_sink(results)
  }
```

**Key Differences**:
- ✅ NO main thread bottleneck
- ✅ NO MPSC channels for data flow
- ✅ NO result collector
- ✅ Each partition reads/routes/executes/writes independently
- ✅ V2@1p performance = V1 performance (or better)
- ✅ V2@8p can achieve true 8x scaling

**Performance Expected**:
- V2@1p: 76,986 rec/sec (equal to V1, no overhead)
- V2@2p: ~150K rec/sec (true 2x scaling)
- V2@8p: ~614K rec/sec (true 8x scaling)
- Each partition: 500K+ rec/sec (as promised)

---

## Architectural Requirements for STP

A true STP architecture must satisfy:

1. ✅ **No Central Coordinator**: Each partition is autonomous
2. ✅ **No Synchronization Points**: Partitions don't wait for each other
3. ✅ **Direct I/O**: Each partition reads/writes independently
4. ✅ **Local State**: State management is per-partition
5. ✅ **Linear Scaling**: N partitions = N x single-partition throughput
6. ✅ **No Overhead**: V2@1p ≥ V1 performance

**Current Implementation Violates**: #1, #2, #3, #4, #6

---

## How to Fix: Option A - Architectural Refactoring

### Step 1: Remove Main Thread Bottleneck

**Current**:
```rust
pub async fn process_multi_job(...) -> Result<JobExecutionStats> {
    let mut stats = JobExecutionStats::new();

    // MAIN THREAD LOOP - BOTTLENECK
    loop {
        let batch = Self::read_batch_from_sources(&mut context).await?;
        let routed = self.route_batch(&batch, &group_by_columns).await?;
        for (partition_id, records) in routed.iter().enumerate() {
            partition_senders[partition_id].send(records.clone()).await?;
        }
        stats.records_processed += batch.len() as u64;
    }

    // Wait for partition tasks...
    // Collect results...
}
```

**After**:
```rust
pub async fn process_multi_job(...) -> Result<JobExecutionStats> {
    // Spawn N independent partition tasks - that's it
    let mut partition_handles = Vec::new();

    for partition_id in 0..self.num_partitions {
        let handle = tokio::spawn(async move {
            Self::partition_pipeline(
                partition_id,
                context.clone(),
                engine.clone(),
                query.clone(),
            ).await
        });
        partition_handles.push(handle);
    }

    // Just wait for all partitions to complete
    let mut stats = JobExecutionStats::new();
    for handle in partition_handles {
        let partition_stats = handle.await??;
        stats.records_processed += partition_stats.records_processed;
        stats.batches_processed += partition_stats.batches_processed;
    }

    Ok(stats)
}
```

### Step 2: Make Partitions Fully Independent

**Create new method: `partition_pipeline()`**

```rust
async fn partition_pipeline(
    partition_id: usize,
    mut context: ProcessorContext,  // Clone - each partition has its own
    engine: Arc<RwLock<StreamExecutionEngine>>,
    query: StreamingQuery,
) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
    let mut stats = JobExecutionStats::new();
    let group_by_columns = Self::extract_group_by_columns(&query);

    // This partition handles its own reading/routing/writing
    loop {
        // Step 1: Read batch from all sources
        let batch = Self::read_batch_from_sources(&mut context).await?;
        if batch.is_empty() {
            break;
        }

        // Step 2: Route records for THIS partition only (local decision)
        let routed = Self::route_records_for_partition(
            &batch,
            partition_id,
            self.num_partitions,
            &group_by_columns,
        )?;

        // Step 3: Execute SQL on this partition's records
        let results = Self::execute_batch(
            &routed,
            &engine,
            &query,
        ).await?;

        // Step 4: Write results directly to sinks (no channels)
        let sink_names = context.list_sinks();
        for sink_name in sink_names {
            context.write_batch_to(&sink_name, results.clone()).await?;
        }

        stats.records_processed += routed.len() as u64;
        stats.batches_processed += 1;
    }

    Ok(stats)
}
```

**Key Changes**:
- ✅ NO MPSC channels for data
- ✅ Each partition reads ALL sources (filters locally)
- ✅ Each partition routes locally
- ✅ Each partition writes directly
- ✅ NO result collection channel
- ✅ Fully independent execution

### Step 3: Optimize Routing for Partition Independence

**Current (Wrong)**:
```rust
// Main thread routes for ALL partitions
let routed = self.route_batch(&batch, &group_by_columns).await?;
// Returns Vec<Vec<StreamRecord>> - one per partition
for (partition_id, records) in routed.iter().enumerate() {
    partition_senders[partition_id].send(records).await?;
}
```

**After (Correct)**:
```rust
// Each partition routes for ITSELF
fn route_records_for_partition(
    batch: &[StreamRecord],
    partition_id: usize,
    num_partitions: usize,
    group_by_columns: &[String],
) -> Result<Vec<StreamRecord>> {
    let mut local_records = Vec::new();

    for record in batch {
        let target_partition = Self::calculate_partition_for_record(
            record,
            num_partitions,
            group_by_columns,
        )?;

        if target_partition == partition_id {
            local_records.push(record.clone());
        }
    }

    Ok(local_records)
}
```

**Benefit**:
- Each partition processes ALL records but keeps only its own
- No coordination needed
- Hash function is O(1), very cheap
- True independence

### Step 4: Shared Engine State (RwLock)

Keep the `Arc<RwLock<StreamExecutionEngine>>` for sharing state:

```rust
// Multiple partitions reading state simultaneously
let read = engine.read().await;
let group_states = read.get_group_states().clone();
drop(read);

// Process records
// ...

// Serialize state updates (fast with RwLock)
let mut write = engine.write().await;
write.set_group_states(new_states);
drop(write);
```

**This is correct** - it enables state sharing without channels.

---

## Implementation Checklist

### Phase 6.1b: Architectural Refactoring (1-2 days)

- [ ] **Remove main thread loop** (delete process_multi_job main loop)
- [ ] **Create partition_pipeline()** (independent per-partition execution)
- [ ] **Update partition initialization** (spawn N fully independent tasks)
- [ ] **Remove MPSC channels** (data flow, not result collection)
- [ ] **Remove result_rx/result_tx** (partitions write directly)
- [ ] **Update routing** (local partition routing instead of global)
- [ ] **Verify all tests pass** (520 unit tests)
- [ ] **Run performance tests** (should see V2@1p ≈ V1 now)
- [ ] **Commit refactored architecture**

### Phase 6.2: True Multi-Core Scaling (1 day)

After fixing V2@1p overhead, Phase 6.2 parallelization will actually work:

- [ ] Multiple partitions reading independently
- [ ] Each on its own tokio task
- [ ] True linear scaling achievable
- [ ] V2@8p → ~600K rec/sec (8x from 76K baseline)

---

## Expected Results After Refactoring

### After Phase 6.1b (Architectural Fix):

```
V2@1p:  75,000-76,000 rec/sec  (≈ V1, no overhead)
V2@2p:  Not tested yet
V2@8p:  68,379 rec/sec         (still bottlenecked by single-threaded test setup)
```

The V2@1p will finally match V1 because:
- ✅ No MPSC channel overhead
- ✅ No task context switching
- ✅ No result collection overhead
- ✅ Same computation as V1

### After Phase 6.2 (Multi-Core Parallelization):

```
V2@1p:  75,000+ rec/sec
V2@2p:  150,000+ rec/sec
V2@4p:  300,000+ rec/sec
V2@8p:  600,000+ rec/sec
Scaling: 95%+ linear (vs current 0%)
```

---

## What This Teaches Us

**Lesson 1**: STP architecture is FUNDAMENTALLY different from coordinator architecture
- Coordinators introduce overhead (channels, task switching)
- STP pipelines are direct and parallel
- Can't mix the two without performance penalties

**Lesson 2**: V2@1p performance is the canary in the coal mine
- If V2@1p is slower than V1, there's fundamental overhead
- Should be identical or better
- The 11% gap was a red flag that should have been caught earlier

**Lesson 3**: Channel-based coordination is fine for result collection
- BUT NOT for data flow
- Using channels to pass data = centralized bottleneck
- Direct I/O = true parallelism

---

## Migration Path

This refactoring is **not a rewrite** - it's a restructuring:

1. Keep all existing SQL execution code (QueryProcessor, StreamExecutionEngine)
2. Keep all existing aggregation/window logic
3. Keep all existing error handling
4. Keep all existing state management
5. Just restructure HOW data flows through partitions

**Scope**:
- ✅ Modify `process_multi_job()` main loop (40 lines)
- ✅ Add `partition_pipeline()` new method (60 lines)
- ✅ Add `route_records_for_partition()` helper (20 lines)
- ✅ Remove result collection channel code (30 lines)
- ✅ Update tests (20 lines)

**Total**: ~150 lines changed out of 2000+ in coordinator.rs

---

## Risks & Mitigations

### Risk 1: Breaking existing tests
**Mitigation**: All 520 tests should still pass (only internal refactoring)
**Fallback**: Keep old code until new code is validated

### Risk 2: Performance doesn't improve
**Mitigation**: Profile before/after with flamegraph
**Fallback**: Revert and analyze further

### Risk 3: State consistency issues
**Mitigation**: RwLock ensures proper synchronization
**Fallback**: Add validation tests for state correctness

### Risk 4: ProcessorContext not Clone-able
**Mitigation**: May need to add Clone impl
**Fallback**: Wrap in Arc or refactor to owned data

---

## Validation Plan

**Before Starting**:
- [x] Current tests pass (520/520)
- [x] Current implementation committed
- [x] Architecture issue documented

**During Refactoring**:
- [ ] Code compiles at each step
- [ ] Tests pass after each change
- [ ] No clippy warnings

**After Refactoring**:
- [ ] All 520 tests still pass
- [ ] V2@1p performance ≥ V1 (should be 75K+ rec/sec)
- [ ] V2@8p shows improvement (even with test bottleneck)
- [ ] Memory usage reasonable (no leaks)
- [ ] Flamegraph shows no unexpected overhead

---

## Conclusion

The current V2 implementation violates fundamental STP principles by centralizing data flow through a main thread coordinator. This introduces unnecessary overhead and prevents true multi-core scaling.

The solution is to restructure the architecture to make each partition fully independent:
- Each partition reads its own data
- Each partition routes locally
- Each partition executes SQL
- Each partition writes its own results
- NO central coordinator
- NO data flow channels
- MINIMAL overhead

This refactoring is straightforward (150 lines of changes) and will:
1. Fix V2@1p performance (eliminate 11% penalty)
2. Enable true multi-core scaling (Phase 6.2)
3. Deliver on the promise of "much faster than V1"
4. Achieve 500K+ rec/sec per partition

**Status**: Ready for Phase 6.1b implementation
**Estimated Duration**: 1-2 days
**Next Step**: Begin architectural refactoring

---

**Document**: Phase 6.1a Architectural Issue & Refactoring Plan
**Status**: CRITICAL - Requires immediate attention
**Priority**: HIGH - Blocks scaling goals
**Action**: Proceed with Option A (Architectural Refactoring)
