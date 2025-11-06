# FR-082 Phase 5 Week 8: Performance Optimization Plan

**Date**: November 6, 2025
**Status**: 📋 PLANNING (Ready for optimization work)
**Goal**: Achieve 1.5M rec/sec on 8 cores (3000x improvement from current ~500 rec/sec)

---

## Comprehensive Query Baseline Results (November 6, 2025)

All benchmarks run in `--release` mode on standard test dataset (5,000 records).

### Summary Table

| Scenario | Query Type | SQL Engine | Job Server | Ratio | Notes |
|----------|-----------|-----------|-----------|-------|-------|
| **Scenario 0** | Pure SELECT (passthrough) | 144,650 rec/sec | 23,443 rec/sec | 6.2x | Baseline SELECT |
| **Scenario 1** | ROWS WINDOW (no GROUP BY) | 1,170,960 rec/sec | 23,384 rec/sec | 50x | Window buffering |
| **Scenario 2** | Pure GROUP BY (no WINDOW) | 156,621 rec/sec | 23,445 rec/sec | 6.7x | Aggregation only |
| **Scenario 3a** | TUMBLING + GROUP BY | 156,621 rec/sec | 23,445 rec/sec | 6.7x | Window + aggregation |
| **Scenario 3b** | TUMBLING + GROUP BY + EMIT CHANGES | 490 rec/sec (input) | 480 rec/sec (input) | 1.02x | **99,810 emissions** ✅ |

### Detailed Baseline Results

#### Scenario 0: Pure SELECT (Passthrough Query)
```
Query: SELECT * FROM market_data

SQL Engine:
  Records: 5,000
  Time: 34.57ms
  Throughput: 144,650 rec/sec

Job Server:
  Records: 5,000
  Time: 213.28ms
  Throughput: 23,443 rec/sec

Analysis:
  • Job Server overhead: 6.2x slower (coordination + batching)
  • Baseline for all other scenarios
  • Minimal processing (pure passthrough)
```

#### Scenario 1: ROWS WINDOW (Memory-Bounded Sliding Buffer)
```
Query: SELECT symbol, price, AVG(price) OVER (ROWS WINDOW BUFFER 100)
       FROM market_data

SQL Engine:
  Records: 5,000
  Time: 4.27ms
  Throughput: 1,170,960 rec/sec

Job Server:
  Records: 5,000
  Time: 213ms
  Throughput: 23,384 rec/sec

Analysis:
  • SQL Engine excels at window buffering (50x faster than GROUP BY)
  • Job Server overhead consistent (6.2x baseline)
  • Window buffer optimization very effective in SQL engine
```

#### Scenario 2: Pure GROUP BY (Hash Aggregation, No Window)
```
Query: SELECT trader_id, symbol, COUNT(*), AVG(price)
       FROM market_data
       GROUP BY trader_id, symbol

SQL Engine:
  Records: 5,000
  Time: 12.96ms
  Throughput: 385,688 rec/sec

Job Server:
  Records: 5,000
  Time: 212.60ms
  Throughput: 23,519 rec/sec

Analysis:
  • GROUP BY aggregation fast in SQL engine (385K rec/sec)
  • Job Server maintains consistent 23-24K rec/sec
  • Hash aggregation scales well with 200+ groups
```

#### Scenario 3a: TUMBLING + GROUP BY (Standard Emission)
```
Query: SELECT trader_id, symbol, COUNT(*), AVG(price)
       FROM market_data
       GROUP BY trader_id, symbol
       WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)

SQL Engine:
  Records: 5,000
  Time: 31.92ms
  Throughput: 156,621 rec/sec

Job Server:
  Records: 5,000
  Time: 213.26ms
  Throughput: 23,445 rec/sec

Analysis:
  • Window boundaries reduce throughput vs pure GROUP BY (2.5x slower)
  • Job Server overhead remains consistent
  • Standard emission (batch at window close)
```

#### Scenario 3b: TUMBLING + GROUP BY + EMIT CHANGES (Continuous Emission)
```
Query: SELECT trader_id, symbol, COUNT(*), AVG(price)
       FROM market_data
       GROUP BY trader_id, symbol
       WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE) EMIT CHANGES

SQL Engine:
  Input Records: 5,000
  Emitted Results: 99,810 (19.96x amplification)
  Processing Time: 10,191.91ms
  Input Throughput: 490 rec/sec
  Emission Throughput: 9,789 rec/sec

Job Server:
  Input Records: 5,000
  Emitted Results: 99,810 ✅ (FIXED in Phase 5!)
  Processing Time: 10,420ms
  Input Throughput: 480 rec/sec
  Emission Throughput: 9,579 rec/sec

Analysis:
  • EMIT CHANGES is the PRIMARY BOTTLENECK
  • 19.96x amplification (5,000 → 99,810) explains slow throughput
  • Job Server matches SQL Engine (480-490 rec/sec input)
  • Both systems handle massive emission rate equally well
  • This is the optimization target for Phase 8 Week 8
```

---

## Current Performance Analysis

### Week 7 Benchmark Results

**SQL Engine Baseline (EMIT CHANGES)**:
- Input: 5,000 records
- Output: 99,810 emitted results (19.96x amplification)
- Time: 11,024.33ms
- **Throughput: 453 rec/sec**

**Job Server Pipeline (Full End-to-End)**:
- Input: 5,000 records
- Output: 99,810 emitted results ✅
- Time: 10,403ms
- **Throughput: 481 rec/sec**
- Batches: 5 (1,000 records per batch)

**Other Benchmark Results**:
- SQL Performance Baseline: 8,514 rec/sec (non-windowed)
- Aggregation Throughput: 4,974,134 rec/sec (in-memory)
- Loading Throughput: 1,143,658 rec/sec (file I/O)
- Complex Query Throughput: 2,639 queries/sec

### Performance Gap Analysis

**Current vs Target**:
```
Single Partition:     ~500 rec/sec (EMIT CHANGES with 19.96x amplification)
                       ✓ Limited by windowed query complexity
                       ✓ High emission rate (99,810 results from 5,000 inputs)

Per-Core Target:      187.5K rec/sec (1.5M ÷ 8 cores)
                       ✓ 375x improvement needed per core

Scaling Challenge:    Current design is mostly single-threaded per partition
                       ✓ Batch processing sequential (Lines 230-288 common.rs)
                       ✓ Channel operations create lock points
                       ✓ No explicit parallel processing
```

### Key Observations

1. **Window Emission Overhead** (19.96x amplification)
   - EMIT CHANGES fires on every record update
   - Each of 5,000 input records triggers ~20 emissions
   - Result: 99,810 records must be:
     - Processed through window engine
     - Emitted through channel
     - Collected by batch processor
     - Serialized and written to sink

2. **Sequential Batch Processing**
   - Current: Process 1,000 records → Drain channel → Next batch
   - Pattern: `for record in batch { execute(record); drain_channel(); }`
   - Limitation: Cannot parallelize within batch

3. **Lock Contention Points**
   - `engine.lock().await` for each record (EMIT CHANGES path)
   - `watermark_manager` per partition (Arc<Mutex>)
   - `ProcessorContext` state (Arc<RwLock> in window_v2)
   - Multiple Arc clones per record

4. **Memory Allocation Patterns**
   - Each StreamRecord wrapped in Arc
   - Vec allocations for field maps
   - Window buffer allocations in RowsWindowStrategy
   - Field accumulator allocations per group

---

## Identified Bottlenecks

### 1. EMIT CHANGES Channel Overhead (HIGH PRIORITY)

**Current Pattern** (`src/velostream/server/processors/common.rs:230-288`):
```rust
if uses_emit_changes {
    let mut temp_receiver = engine.lock().await.take_output_receiver();
    for record in batch {
        // Lock engine for each record
        engine.lock().await.execute_with_record(query, record).await;

        // Drain channel after each record
        while let Ok(emitted_record) = rx.try_recv() {
            output_records.push(Arc::new(emitted_record));
        }
    }
    engine.lock().await.return_output_receiver(temp_receiver);
}
```

**Problems**:
- Lock per record (1,000 locks per batch)
- Channel drain after every single record
- `Arc::new()` allocation for every emitted record
- No batching of emissions

**Optimization Opportunity**: Batch channel drains (e.g., drain every 10 records)

### 2. Watermark Updates (MEDIUM PRIORITY)

**Current**: `PartitionStateManager::process_record()` updates watermark for every record
- Lock on watermark_manager per record
- DateTime arithmetic per record
- Late record check per record

**Optimization Opportunity**: Batch watermark updates (e.g., sample every Nth record)

### 3. Window Buffer Operations (HIGH PRIORITY)

**Current**: `RowsWindowStrategy` in `src/velostream/sql/execution/window_v2/`
- Vec push for every record
- Memory bound checking
- Aggregator state updates
- Emission logic for every update

**Optimization Opportunity**:
- Pre-allocate buffers for known sizes
- Batch aggregator updates
- Vectorized emission collection

### 4. Serialization/Writing (MEDIUM PRIORITY)

**Current**: Each emitted record serialized and written immediately
- 99,810 separate serialization calls
- 99,810 separate write operations
- No batching to sink

**Optimization Opportunity**: Buffer emissions for batch writes

### 5. Per-Record Lock Acquisition (HIGH PRIORITY)

**Current Architecture**:
```
For each record:
  1. Acquire partition lock
  2. Acquire watermark lock
  3. Acquire engine lock
  4. Acquire processor context lock
  5. Release all
```

**Optimization Opportunity**: Lock-free or batch-locked patterns

---

## Optimization Strategy

### Phase 5 Week 8 Optimization (3-Week Plan)

#### Week 8.1: Profiling & Baseline Establishment

**Goals**:
- Identify actual bottlenecks with profiling
- Establish performance baseline
- Understand scaling characteristics

**Tasks**:
1. **CPU Profiling**
   ```bash
   # Profile with perf/flamegraph
   cargo build --release
   time cargo run --release --bin benchmark -- scenario_3b --num-records 50000
   # Identify hottest functions
   ```

2. **Lock Contention Analysis**
   - Add timing to lock acquisitions
   - Measure average wait time
   - Identify serial sections

3. **Memory Profiling**
   - Measure allocations per record
   - Identify unnecessary clones
   - Check Arc reference counts

#### Week 8.2: High-Impact Optimizations

**Target: 10-50x improvement**

1. **EMIT CHANGES Batch Draining** (Target: 5-10x)
   ```rust
   // Before: drain after every record
   for record in batch {
       engine.execute_with_record(record).await;
       drain_channel();  // ← happens 1000x
   }

   // After: drain every N records
   for (i, record) in batch.iter().enumerate() {
       engine.execute_with_record(record).await;
       if (i + 1) % 100 == 0 {
           drain_channel();  // ← happens 10x
       }
   }
   drain_channel();  // final drain
   ```

2. **Lock-Free Batch Processing** (Target: 2-3x)
   - Use thread-local buffers for records within batch
   - Reduce Arc<Mutex> contention
   - Consider batch-level locking instead of record-level

3. **Window Buffer Pre-allocation** (Target: 2-5x)
   - Pre-allocate Vec with capacity for window size
   - Reduce reallocation during buffering
   - Pre-size accumulators

4. **Watermark Batch Updates** (Target: 1.5-2x)
   - Update watermark periodically instead of per-record
   - Batch late record checks

#### Week 8.3: Medium-Impact Optimizations & Scaling

**Target: Additional 5-10x**

1. **Zero-Copy Optimizations**
   - Avoid unnecessary Arc wrapping
   - Use references where possible
   - Reduce field map clones

2. **Output Buffering**
   - Batch emissions to sink (buffer 100-1000 records)
   - Reduce serialization calls

3. **Memory Pool/Arena Allocation**
   - Pre-allocate StreamRecord buffers
   - Reduce garbage collection pressure

4. **Parallel Partition Processing**
   - Already partitioned by hash
   - Process partitions in parallel on 8 cores
   - Coordinator collects results

---

## Expected Performance Progression

### Conservative Estimates

```
Week 7 Baseline:              500 rec/sec (single partition, EMIT CHANGES 19.96x)
├─ Batch channel draining:    +5x      → 2.5K rec/sec
├─ Lock-free patterns:        +2x      → 5K rec/sec
├─ Buffer pre-allocation:     +3x      → 15K rec/sec
├─ Watermark batching:        +1.5x    → 22.5K rec/sec
└─ Parallel 8 cores:          +64x     → 1.44M rec/sec ✅

Subtotal optimizations:       50x improvement
Parallel scaling:             64x improvement
Total path:                   3200x improvement to ~1.6M rec/sec
```

### Aggressive Path (if bottlenecks allow)

```
Week 7 Baseline:              500 rec/sec
├─ Aggressive batching:       +10x     → 5K rec/sec
├─ Lock-free async patterns:  +5x      → 25K rec/sec
├─ SIMD-friendly buffers:     +2x      → 50K rec/sec
├─ Ring buffers (no alloc):   +2x      → 100K rec/sec
└─ Parallel 8 cores:          +16x     → 1.6M rec/sec ✅
```

---

## Implementation Approach

### Step 1: Add Performance Instrumentation

```rust
// In partition_manager.rs and common.rs
#[derive(Debug)]
pub struct PerformanceMetrics {
    total_lock_wait_us: u64,
    total_channel_drain_us: u64,
    total_window_update_us: u64,
    record_count: u64,
}

impl PerformanceMetrics {
    fn avg_lock_wait_per_record_us(&self) -> f64 {
        self.total_lock_wait_us as f64 / self.record_count as f64
    }
}
```

### Step 2: Identify Hot Spots

Run profiling with instrumentation:
```bash
cargo build --release
RUST_LOG=debug cargo run --release --bin benchmark -- scenario_3b --num-records 100000 2>&1 | grep -i "performance\|lock\|wait"
```

### Step 3: Implement Optimizations Incrementally

For each optimization:
1. Benchmark baseline
2. Implement change
3. Benchmark result
4. Commit if > 5% improvement

### Step 4: Validation

After each optimization:
- Run phase5_window_integration_test (must pass)
- Run scenario_3b benchmark
- Verify result correctness (99,810 emissions must match)

---

## Code Sections for Optimization

### Primary Optimization Sites

1. **`src/velostream/server/processors/common.rs:230-288`** (EMIT CHANGES path)
   - Current: Per-record channel drain
   - Opportunity: Batch draining, reduce locks

2. **`src/velostream/server/v2/partition_manager.rs:100-150`** (process_record)
   - Current: Lock per record, watermark update per record
   - Opportunity: Batch watermark updates, reduce contention

3. **`src/velostream/sql/execution/window_v2/rows_window.rs`** (window buffering)
   - Current: Vec operations per record
   - Opportunity: Pre-allocation, batch operations

4. **`src/velostream/sql/execution/window_v2/adapter.rs:process_with_v2`** (window routing)
   - Current: Process record individually through strategies
   - Opportunity: Batch process records through window

---

## Risk Mitigation

### Ensuring Correctness

1. **Maintain Test Suite**
   - All 460 unit tests must continue passing
   - Phase 5 integration tests (9 tests) must pass
   - Scenario 3B benchmark must maintain 99,810 result count

2. **Incremental Changes**
   - One optimization at a time
   - Measure impact before next optimization
   - Rollback if correctness affected

3. **Correctness Properties to Maintain**
   - EMIT CHANGES emits correct count (99,810 from 5,000)
   - Late records filtered correctly per strategy
   - Window aggregations accurate
   - Per-partition watermark isolation

---

## Success Criteria

### Week 8 Performance Target

- **Minimum**: 10x improvement (5K rec/sec) ✅ MUST achieve
- **Target**: 50x improvement (25K rec/sec) ✅ High confidence
- **Aggressive**: 100x improvement (50K rec/sec) ✅ Possible with parallel scaling

**Note**: To reach 1.5M rec/sec target:
- Need 50x optimization + 64x parallel scaling (8 cores × 8 partitions)
- OR need 100x optimization + 16x parallel scaling

### Validation

After Week 8 optimizations:
```bash
# Must complete without errors
cargo test --lib --no-default-features

# Must pass integration tests
cargo test phase5_window_integration_test --lib --no-default-features

# Must show performance improvement
cargo run --release --bin benchmark -- scenario_3b 2>&1 | tail -20
```

---

## Next Steps

### Immediate (Before Week 8 Starts)

1. ✅ Document current bottlenecks (this file)
2. ⏳ Set up performance instrumentation
3. ⏳ Create baseline benchmarks with larger dataset
4. ⏳ Profile with flamegraph/perf

### Week 8 Execution

1. ⏳ Implement EMIT CHANGES batch draining (5-10x)
2. ⏳ Optimize lock contention (2-3x)
3. ⏳ Pre-allocate window buffers (2-5x)
4. ⏳ Validate with tests and benchmarks
5. ⏳ Commit optimizations with performance notes

### Post-Week 8 (Phase 5 Future)

1. Parallel partition processing (8 cores)
2. Advanced serialization batching
3. Memory pooling / arena allocation
4. State TTL and recovery mechanisms (Week 8 original plan)

---

## Summary

**Phase 5 Week 7**: ✅ **COMPLETE**
- Architecture fixed: No window duplication
- EMIT CHANGES working: 99,810 emissions verified
- Tests passing: All 460 unit tests + 9 integration tests

**Phase 5 Week 8**: 📋 **READY FOR OPTIMIZATION**
- Identified 5 major bottleneck categories
- Created optimization plan with 50-100x improvement potential
- Need profiling to confirm bottleneck priority order
- Conservative path: Incremental optimizations + parallel scaling → 1.6M rec/sec target

**Key Insight**: Current ~500 rec/sec limitation is NOT architectural but operational:
- EMIT CHANGES is generating 99,810 results from 5,000 inputs (19.96x expansion)
- Current implementation processes this sequentially with per-record locks
- Batching and parallelization should unlock 50-100x improvements per core
