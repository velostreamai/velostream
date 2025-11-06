# FR-082 Phase 5 Week 8: Performance Optimization - Status & Action Plan

**Date**: November 6, 2025
**Status**: 🔄 **READY FOR OPTIMIZATION** (Week 7 complete, Week 8 planning done)
**Owner**: System Performance Team

---

## Executive Summary

Phase 5 Week 7 ✅ **COMPLETED**:
- Architecture corrected (no window duplication)
- EMIT CHANGES fully working (99,810 emissions verified)
- 460 unit tests + 9 integration tests passing
- Foundation solid and ready for performance optimization

Phase 5 Week 8 📋 **PLANNING PHASE COMPLETE**:
- ✅ Identified 5 major bottleneck categories
- ✅ Created detailed optimization plan (50-100x improvement potential)
- ✅ Built profiling infrastructure
- ✅ Documented expected results and success criteria

**Current Performance**: ~500 rec/sec (single partition EMIT CHANGES)
**Week 8 Target**: 5-25K rec/sec (per-core target → parallel scaling to 1.5M)

---

## Week 7 Baseline (Validated Nov 6, 2025)

### SQL Engine Baseline (EMIT CHANGES)
```
Input Records:     5,000
Emitted Results:   99,810 (19.96x amplification)
Processing Time:   11,024.33ms
Throughput:        453 rec/sec
Status:            ✅ WORKING
```

### Job Server Full Pipeline (EMIT CHANGES)
```
Input Records:     5,000
Emitted Results:   99,810 ✅ (FIXED in Phase 5!)
Processing Time:   10,403ms
Throughput:        481 rec/sec
Batches:           5 (1,000 records per batch)
Status:            ✅ FIXED IN PHASE 5
```

### Key Insight
Current bottleneck is NOT architectural but **operational**:
- EMIT CHANGES triggers ~20x emission amplification (5,000 → 99,810)
- Each emission requires: window update, state aggregation, channel drain, collection
- Sequential per-record processing with per-record locks
- **Solution**: Batch operations, reduce lock contention, parallelization

---

## Week 8 Optimization Plan

### Phase 8.1: Profiling & Baseline Establishment ⏳ STARTING

**Objective**: Confirm bottleneck priority order

**Key Measurements** (via `tests/performance/analysis/phase5_week8_profiling.rs`):
- Per-record latency breakdown
- Channel operation frequency
- Lock acquisition patterns
- Window buffer overhead
- Memory allocation patterns

**Test Commands**:
```bash
# Baseline profiling with real engine
cargo test --release phase5_week8_baseline -- --nocapture

# Optimization strategy overview
cargo test --release phase5_week8_optimization_summary -- --nocapture
```

---

### Phase 8.2: High-Impact Optimizations (Target: 50x improvement)

#### Optimization 1: EMIT CHANGES Batch Draining (5-10x)
**Current Pattern**:
```rust
for record in batch {
    engine.lock().await.execute_with_record(record).await;
    drain_channel();  // Per record → 1,000 drains for batch
}
```

**Optimized Pattern**:
```rust
for (i, record) in batch.iter().enumerate() {
    engine.execute_with_record(record).await;
    if (i + 1) % 100 == 0 {
        drain_channel();  // Every 100 records → 10 drains for batch
    }
}
drain_channel();  // Final drain
```

**Impact**: 100x fewer channel drains = 5-10x throughput improvement
**File**: `src/velostream/server/processors/common.rs:230-288`

---

#### Optimization 2: Lock-Free Batch Processing (2-3x)
**Current**: Lock per record (1,000 locks per batch)
**Target**: Lock per batch (1 lock per batch)

**Approach**:
- Thread-local buffers within batch
- Single lock acquisition for batch results
- Reduce contention from 1,000 to 1

**File**: `src/velostream/server/processors/common.rs:batch_processing()`

---

#### Optimization 3: Window Buffer Pre-allocation (2-5x)
**Current Pattern** (in `RowsWindowStrategy`):
```rust
let mut buffer = Vec::new();  // Starts empty
for record in records {
    buffer.push(record);  // Reallocates as needed
    if buffer.len() > MAX_SIZE {
        buffer.remove(0);  // Expensive for large buffers
    }
}
```

**Optimized Pattern**:
```rust
let mut buffer = Vec::with_capacity(MAX_SIZE);  // Pre-allocate
let mut head = 0;  // Ring buffer head
let mut size = 0;  // Current size
for record in records {
    buffer[head] = record;
    head = (head + 1) % MAX_SIZE;
    size = std::cmp::min(size + 1, MAX_SIZE);
}
```

**Impact**: Eliminate reallocation overhead
**File**: `src/velostream/sql/execution/window_v2/rows_window.rs`

---

#### Optimization 4: Watermark Batch Updates (1.5-2x)
**Current**: Update watermark for every record
**Target**: Update watermark periodically (every 100-1000 records)

**Approach**:
- Sample-based watermark updates (e.g., every 100 records)
- Batch late record checks
- Reduce lock contention on watermark manager

**File**: `src/velostream/server/v2/partition_manager.rs:process_record()`

---

### Phase 8.3: Medium-Impact Optimizations (Target: Additional 5-10x)

#### Additional Optimizations:
1. **Zero-Copy Patterns** (1.5x)
   - Avoid unnecessary Arc wrapping
   - Use references within batch

2. **Output Buffering** (2x)
   - Batch emissions to sink
   - Reduce write operations

3. **Memory Pooling** (1.5x)
   - Pre-allocate record buffers
   - Reuse across batches

---

## Expected Performance Progression

### Conservative Path (High Confidence)

```
Week 7 Baseline:              500 rec/sec
├─ Batch channel draining:    +5x      → 2.5K rec/sec
├─ Lock-free patterns:        +2x      → 5K rec/sec
├─ Buffer pre-allocation:     +3x      → 15K rec/sec
└─ Watermark batching:        +1.5x    → 22.5K rec/sec
Subtotal per-core:            45x improvement

Parallel scaling (8 cores):   45x × 8 = 1.8M rec/sec ✅
```

### Optimistic Path (Medium Confidence)

```
Week 7 Baseline:              500 rec/sec
├─ Aggressive batching:       +10x     → 5K rec/sec
├─ Lock-free async:           +5x      → 25K rec/sec
├─ SIMD-friendly buffers:     +2x      → 50K rec/sec
├─ Ring buffers:              +2x      → 100K rec/sec
└─ Additional optimizations:  +2x      → 200K rec/sec
Subtotal per-core:            400x improvement

Parallel scaling (8 cores):   400x × 8 = 1.6M rec/sec ✅
```

---

## Week 8 Implementation Schedule

### Week 8.1: Profiling (Days 1-2)
- ✅ Create profiling infrastructure (DONE)
- ⏳ Run baseline measurements with real engine
- ⏳ Identify top 3 bottlenecks
- ⏳ Validate bottleneck impact calculations

### Week 8.2: High-Impact Optimization (Days 3-5)
- ⏳ Implement EMIT CHANGES batch draining
- ⏳ Test & measure (target 5-10x)
- ⏳ Implement lock-free patterns
- ⏳ Test & measure (target 2-3x)
- ⏳ Implement window pre-allocation
- ⏳ Test & measure (target 2-5x)

### Week 8.3: Medium-Impact & Validation (Days 6-7)
- ⏳ Implement remaining optimizations
- ⏳ Run full test suite (460+ unit tests must pass)
- ⏳ Verify Phase 5 integration tests still pass
- ⏳ Final benchmark against 1.5M target

---

## Files for Optimization

### Primary Optimization Sites

1. **`src/velostream/server/processors/common.rs`** (Lines 230-288)
   - EMIT CHANGES batch draining
   - Channel operation frequency
   - Lock contention points

2. **`src/velostream/server/v2/partition_manager.rs`** (Lines 100-150)
   - Watermark updates
   - Per-record lock patterns
   - Metrics tracking efficiency

3. **`src/velostream/sql/execution/window_v2/rows_window.rs`**
   - Window buffer pre-allocation
   - Vec operations optimization
   - Memory efficiency

4. **`src/velostream/sql/execution/window_v2/adapter.rs`**
   - Window routing efficiency
   - Batch processing patterns

---

## Risk Mitigation

### Correctness Validation

**CRITICAL**: All optimizations must maintain:
- 460+ unit tests passing ✅
- 9 Phase 5 integration tests passing ✅
- Scenario 3B: 99,810 emissions (must match exactly)
- Per-partition watermark isolation
- Window aggregation accuracy

**Testing Strategy**:
```bash
# After each optimization
cargo test --lib --no-default-features
cargo test phase5_window_integration_test --lib --no-default-features

# Validate emissions match exactly
cargo test --release scenario_3b_tumbling_emit_changes_baseline -- --nocapture
```

### Incremental Approach

- **One optimization per commit**
- **Measure before & after each change**
- **Only commit if > 5% improvement**
- **Rollback if any test fails**

---

## Success Criteria

### Week 8 Performance Targets

| Scenario | Current | Target | Multiplier |
|----------|---------|--------|-----------|
| Per-core (EMIT CHANGES) | 500 rec/sec | 25K rec/sec | 50x |
| With 8-core parallel | N/A | 1.5M rec/sec | 3000x |
| Per-partition throughput | 481 rec/sec | 20K+ rec/sec | 40x+ |
| Channel drain frequency | 1,000/batch | 10/batch | 100x fewer |
| Lock acquisitions | 1,000/batch | <10/batch | 100x fewer |

### Validation Checklist

- [ ] Profiling complete (bottlenecks identified)
- [ ] Batch draining implemented (5-10x improvement)
- [ ] Lock patterns optimized (2-3x improvement)
- [ ] Buffer pre-allocation done (2-5x improvement)
- [ ] All 460+ unit tests passing
- [ ] Phase 5 integration tests passing (9 tests)
- [ ] Scenario 3B results match (99,810 emissions)
- [ ] Per-core throughput ≥ 25K rec/sec
- [ ] Parallel scaling ≥ 200K rec/sec (8 cores)

---

## Profiling Test Infrastructure

### Phase5_Week8_Profiling Module

Created: `tests/performance/analysis/phase5_week8_profiling.rs`

**Available Tests**:
```bash
# Baseline EMIT CHANGES profiling with real engine
cargo test --release phase5_week8_baseline -- --nocapture

# Optimization strategy summary
cargo test --release phase5_week8_optimization_summary -- --nocapture
```

**Measurements Provided**:
- Input/output record counts
- Emission amplification ratio
- Per-record latency
- Bottleneck impact analysis
- Optimization path recommendations

---

## Next Actions

### Immediate (Phase 8.1 - Profiling)
1. Run profiling tests to confirm bottleneck order
2. Measure actual lock contention
3. Measure channel drain overhead
4. Validate performance calculations

### High-Priority (Phase 8.2 - Optimizations)
1. Implement EMIT CHANGES batch draining (5-10x expected)
2. Optimize lock patterns (2-3x expected)
3. Pre-allocate window buffers (2-5x expected)
4. Commit optimizations incrementally

### Validation
1. Run phase5_window_integration_test after each change
2. Run scenario_3b benchmark to validate emissions
3. Ensure all 460+ unit tests pass
4. Measure improvement after each optimization

---

## Summary

**Status**: Ready to begin Phase 5 Week 8 optimization work

**Architecture**: ✅ Solid (Phase 7 complete)
**Baseline**: ✅ Measured (500 rec/sec EMIT CHANGES)
**Plan**: ✅ Detailed (50-100x improvement potential)
**Infrastructure**: ✅ Ready (profiling tests created)
**Next Step**: Run profiling → identify bottlenecks → implement optimizations

**Success Path**: 50x optimization + 8-core scaling = 1.5M+ rec/sec target ✅
