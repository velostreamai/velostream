# FR-082: Job Server V2 Unified Schedule

**Project**: Hash-Partitioned Pipeline Architecture for "Better than Flink" Performance
**Last Updated**: November 8, 2025
**Status**: Phase 6 Starting (Critical Bug Fix + Integration)

---

## Executive Summary

### What's Working
- ✅ **Pluggable Partitioning Strategies** (5 complete implementations)
  - AlwaysHashStrategy, SmartRepartitionStrategy, StickyPartitionStrategy, RoundRobinStrategy, FanInStrategy
  - Full routing logic with validation and metrics

- ✅ **Metrics & Backpressure Detection** (Production-ready)
  - Per-partition throughput, latency, queue depth
  - Backpressure state classification (Healthy/Warning/Critical/Saturated)
  - Prometheus exporter integration

- ✅ **Watermark Management** (Event-time processing)
  - Per-partition watermark tracking
  - Late record handling strategies

- ✅ **SQL Engine Baseline** (200K rec/sec per partition)
  - Phase 0: 56x improvement (3.58K → 200K rec/sec)
  - Single-partition V1 baseline established

### The Critical Issue (Phase 6.0)
**Location**: `src/velostream/server/v2/coordinator.rs:263-270`

Partition receivers **silently drain messages** instead of processing them:
```rust
// CURRENT (BROKEN):
tokio::spawn(async move {
    let mut receiver = rx;
    while let Some(_record) = receiver.recv().await {
        // Silent sink - records discarded!
    }
});

// NEEDED (PROCESSING):
tokio::spawn(async move {
    let mut receiver = rx;
    while let Some(record) = receiver.recv().await {
        // Actually process the record
        if let Some(output) = state_manager.process_record(record).await {
            // Send to output channel/sink
        }
    }
});
```

**Impact**: Records route to partitions but get discarded immediately. No actual V2 processing happens.

### What Needs to Be Done (Phase 6-8)
1. **Phase 6.1** (Week 10-11): Fix partition receiver + integrate SQL execution
2. **Phase 6.2** (Week 12): Baseline validation (V1 vs V2 comparison)
3. **Phase 7** (Weeks 13-15): Vectorization & SIMD optimization
4. **Phase 8** (Weeks 16+): Distributed processing

---

## Phase 6: Lock-Free Optimization & Real SQL Execution

### Phase 6.0: CRITICAL FIX - Partition Receiver Processing

**Timeline**: IMMEDIATE (Nov 8-10, 2025)
**Priority**: BLOCKING
**Effort**: 2-3 hours

#### Task 6.0.1: Fix Coordinator Receiver Logic
**File**: `src/velostream/server/v2/coordinator.rs` (lines 263-270)

**Change**:
```rust
// Initialize partitions and return senders/receivers
fn initialize_partitions(&self) -> (Vec<Arc<PartitionStateManager>>, Vec<mpsc::Sender<StreamRecord>>) {
    let mut managers = Vec::with_capacity(self.num_partitions);
    let mut senders = Vec::with_capacity(self.num_partitions);

    for partition_id in 0..self.num_partitions {
        let manager = Arc::new(PartitionStateManager::new(partition_id));
        let (tx, rx) = mpsc::channel(self.config.partition_buffer_size);

        // BEFORE: Silent drain
        // tokio::spawn(async move {
        //     while let Some(_) = receiver.recv().await {}
        // });

        // AFTER: Process records
        let manager_clone = Arc::clone(&manager);
        tokio::spawn(async move {
            let mut receiver = rx;
            while let Some(record) = receiver.recv().await {
                // Phase 6.1: Process record through partition
                if let Err(e) = manager_clone.process_record(&record) {
                    log::warn!("Partition {}: Failed to process record: {}", partition_id, e);
                }
            }
        });

        managers.push(manager);
        senders.push(tx);
    }

    (managers, senders)
}
```

**Testing**: Update `tests/unit/server/v2/coordinator_test.rs` to verify:
- Partition receiver actually processes records (not silent drain)
- Records flow through routing → partition → processing
- Watermark updates happen per-partition

#### Task 6.0.2: Add Output Channel to Coordinator
**Decision**: Per-partition sinks (each partition → own DataWriter) for true STP

```rust
pub struct PartitionedJobCoordinator {
    // ... existing fields ...
    output_writers: Vec<Arc<dyn DataWriter>>,  // One per partition
}
```

**Integration Points**:
- `process_batch_with_strategy()` routes records to partitions
- Partition receiver calls `manager.process_record()`
- Partition task sends output to `output_writers[partition_id]`

**Benefit**: Independent backpressure per partition (no shared bottleneck)

---

### Phase 6.1: Real SQL Execution Routing

**Timeline**: Nov 10-14, 2025
**Effort**: 2-3 days
**Target**: 200K rec/sec per partition (baseline = 200K × N partitions)

#### Task 6.1.1: Integrate StreamExecutionEngine in Partitions

Current `PartitionStateManager.process_record()` only tracks metrics. Add SQL execution:

```rust
pub async fn process_record_with_engine(
    &self,
    record: &StreamRecord,
    engine: Arc<StreamExecutionEngine>,
) -> Result<Option<Arc<StreamRecord>>, SqlError> {
    // 1. Update watermark
    if let Some(event_time) = record.event_time {
        self.watermark_manager.update(event_time);
        let (is_late, should_drop) = self.watermark_manager.is_late(event_time);
        if should_drop {
            return Ok(None);  // Drop late records
        }
    }

    // 2. Process through SQL engine
    let output = engine.execute(&record)?;

    // 3. Track metrics
    self.metrics.record_batch_processed(1);

    Ok(Some(Arc::new(output)))
}
```

#### Task 6.1.2: Update Partition Receiver to Use Engine

```rust
let engine_clone = Arc::clone(&engine);  // Pass engine to task
tokio::spawn(async move {
    let mut receiver = rx;
    while let Some(record) = receiver.recv().await {
        if let Ok(Some(output)) = manager_clone.process_record_with_engine(&record, engine_clone.clone()).await {
            // Send to output writer (Phase 6.0.2)
            output_tx.send(output).ok();
        }
    }
});
```

#### Task 6.1.3: Validate GROUP BY State Isolation

Verify that:
- Same GROUP BY key always routes to same partition ✓ (AlwaysHashStrategy guarantees this)
- Each partition maintains independent state (no cross-partition locks)
- State is not shared/cloned between partitions

#### Task 6.1.4: Run Scenario Tests

Execute tests with V2 enabled:
```bash
cargo test scenario_1_rows_window -- --nocapture
cargo test scenario_2_pure_group_by -- --nocapture
cargo test scenario_3a_tumbling_standard -- --nocapture
cargo test scenario_3b_tumbling_emit_changes -- --nocapture
```

Expected: 0 errors (same results as V1, just distributed across partitions)

---

### Phase 6.2: Baseline Validation

**Timeline**: Nov 14-19, 2025
**Effort**: 1-2 days
**Goal**: Measure actual V2 throughput vs V1

#### Task 6.2.1: Run Baseline Benchmarks

```bash
# V1 Baseline (single partition)
cargo test profile_group_by_baseline -- --nocapture

# V2 Baseline (8 partitions)
cargo run --bin benchmark_v2_partitioned --release

# Compare scaling efficiency
```

**Expected Results**:
| Setup | Throughput | Scaling |
|-------|-----------|---------|
| V1 (1 partition) | 200K rec/sec | Baseline |
| V2 (8 partitions) | 1.5M+ rec/sec | ~7.5x |
| V2 Efficiency | 1500K ÷ 200K ÷ 8 | ~93% |

#### Task 6.2.2: Profile Bottlenecks

If V2 throughput < 1.5M:
1. Check partition queue depths (backpressure)
2. Verify CPU core utilization (should see 8 cores active)
3. Profile hot functions (watermark updates, routing, SQL execution)
4. Identify new bottleneck

#### Task 6.2.3: Document Results

Create: `docs/feature/FR-082-perf-part-2/FR-082-WEEK-11-BASELINE.md`
- V1 vs V2 throughput comparison
- Scaling efficiency calculation
- Bottleneck analysis (if not meeting targets)
- Recommendations for Phase 7

---

## Phase 7: Vectorization & SIMD Optimization

**Timeline**: Weeks 13-15 (Dec 23 - Jan 12, 2026)
**Starting Point**: 1.5M rec/sec (8 partitions × 200K)
**Target**: 2.2M-3.0M rec/sec

### Phase 7.1: SIMD Aggregation
- Vectorize GROUP BY key extraction
- SIMD comparison for key matching
- Batch aggregation updates

### Phase 7.2: Zero-Copy Emission
- Arc<StreamRecord> throughout pipeline
- No intermediate JSON/Avro serialization
- Direct field-to-bytes streaming

### Phase 7.3: Adaptive Batching
- Dynamic batch size based on throughput
- Latency vs throughput trade-off optimization

---

## Phase 8: Distributed Processing

**Timeline**: Weeks 16+ (Feb 3+, 2026)
**Target**: 2.0M-3.0M+ rec/sec across multiple machines

---

## Success Criteria (Phase 6 Complete)

### Functional Requirements
- [ ] Coordinator receiver processes records (not silent drain)
- [ ] Partitions send output to writers/channels
- [ ] Router does not block on responses
- [ ] All existing tests pass
- [ ] Code formatting passes (`cargo fmt --all -- --check`)
- [ ] No clippy warnings
- [ ] Scenario tests validate correct SQL execution

### Performance Requirements
- [ ] V2 baseline: 1.5M rec/sec on 8 cores (minimum)
- [ ] V1 baseline: 200K rec/sec single partition (unchanged)
- [ ] Scaling efficiency: >90% (1.5M / 200K / 8 ≈ 93%)
- [ ] Latency: p95 < 200µs (improvement from ~500µs)

### Documentation Requirements
- [ ] FR-082-SCHEDULE.md updated (this file) ✓
- [ ] FR-082-WEEK-11-BASELINE.md created (after Phase 6.2)
- [ ] Coordinator's partition receiver documented with comments
- [ ] V1 vs V2 performance comparison documented

---

## Implementation Priority (Starting Nov 8, 2025)

### Week of Nov 8-10 (IMMEDIATE)
**Task**: Fix critical partition receiver bug (Phase 6.0)
- 🔴 BLOCKING: Partition receiver silently drains messages
- Impact: No V2 processing happens currently
- Time: 2-3 hours
- Files: `src/velostream/server/v2/coordinator.rs` (lines 263-270)

### Week of Nov 10-14 (CRITICAL)
**Task**: Integrate SQL execution in partitions (Phase 6.1)
- Implement `process_record_with_engine()` in PartitionStateManager
- Update partition receiver to call engine
- Validate GROUP BY state isolation
- Run scenario tests

### Week of Nov 14-19
**Task**: Baseline validation (Phase 6.2)
- Measure V1 vs V2 throughput
- Profile bottlenecks if needed
- Document results

---

## Risk Mitigation

### Risk: Records Still Bypass Partitions
**Mitigation**:
1. Add explicit logging when record enters partition task
2. Verify output shows records being processed
3. Unit test verifies partition receiver receives messages

### Risk: Performance Not 1.5M rec/sec
**Mitigation**:
1. Profile per-partition throughput independently
2. Check for lock contention (Arc::make_mut may clone if refcount > 1)
3. Verify no serialization copies in hot path
4. Measure CPU core saturation

### Risk: SQL Execution Crashes in Partition Task
**Mitigation**:
1. Comprehensive error handling in partition task
2. Test with various query types (GROUP BY, WINDOW, EMIT CHANGES)
3. Graceful degradation (log errors, continue processing)

---

## Files Requiring Changes

### Phase 6.0 (Critical Fix)
- `src/velostream/server/v2/coordinator.rs` (lines 263-270)
- `tests/unit/server/v2/coordinator_test.rs` (update tests)

### Phase 6.1 (Integration)
- `src/velostream/server/v2/partition_manager.rs` (add process_record_with_engine)
- `src/velostream/server/v2/coordinator.rs` (update partition task)

### Phase 6.2 (Validation)
- `tests/performance/analysis/scenario_*.rs` (run with V2)
- Create: `docs/feature/FR-082-perf-part-2/FR-082-WEEK-11-BASELINE.md`

### Cleanup (Done)
- ✅ Deleted: `FR-082-CORRECTED-ARCHITECTURE.md` (theoretical)
- ✅ Deleted: `BOTTLENECK-ANALYSIS.md` (theoretical)
- ✅ Deleted: `FR-082-SCHEDULE-CORRECTED.md` (duplicate)
- ✅ Deleted: `src/velostream/server/v2/hash_router.rs` (replaced by strategies)
- ✅ Updated: `src/velostream/server/v2/mod.rs` (removed hash_router exports)

---

## Key Insight

**V2 is not broken - it's incomplete.**

The foundation is well-architected:
- ✅ Pluggable routing strategies (5 implementations)
- ✅ Comprehensive metrics and backpressure
- ✅ Event-time watermark management

The missing piece is **20-30 lines** of code in the partition receiver to actually process records instead of silently draining them.

Once fixed, V2 will scale to 1.5M+ rec/sec (7.5x improvement over V1).

---

## Timeline Summary

| Week | Phase | Task | Target |
|------|-------|------|--------|
| Nov 8-10 | Phase 6.0 | Fix partition receiver | BLOCKING |
| Nov 10-14 | Phase 6.1 | Integrate SQL execution | 200K rec/sec per partition |
| Nov 14-19 | Phase 6.2 | Baseline validation | 1.5M rec/sec aggregate |
| Dec 23-Jan 12 | Phase 7 | Vectorization & SIMD | 2.2M-3.0M rec/sec |
| Feb 3+ | Phase 8 | Distributed processing | 2.0M-3.0M+ rec/sec |

**Goal**: Phase 6 complete by Nov 19, 2025 → Foundation for Phase 7 optimization
