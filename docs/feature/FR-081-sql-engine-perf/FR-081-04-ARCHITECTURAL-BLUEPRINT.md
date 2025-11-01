# FR-081-04: Architectural Blueprint for Window Processing Refactoring

**Part of**: FR-081 SQL Window Processing Performance Optimization
**Date**: 2025-11-01
**Status**: 📋 PROPOSED (for Phase 2)
**Estimated Effort**: 2-3 weeks
**Expected Performance Gain**: 3-5x additional improvement

---

## Executive Summary

This document proposes a comprehensive refactoring of Velostream's window processing architecture to address identified performance bottlenecks and architectural issues. The current system achieves 15.7K rec/sec after O(N²) fixes, but analysis reveals potential for 50-100K+ rec/sec through strategic refactoring.

**Current Architecture Grade**: B- (functional but needs refactoring)
**Target Architecture Grade**: A (production-ready, highly optimized)

---

## Current Architecture Analysis

### File Structure

```
src/velostream/sql/execution/
├── engine.rs (1,539 lines)
│   ├── StreamExecutionEngine (main entry point)
│   ├── execute_with_record() [line 516]
│   ├── execute_internal() [line 541]
│   ├── load_window_states_for_context() [line 374-386]
│   └── save_window_states_from_context() [line 391-403]
│
├── processors/
│   ├── window.rs (2,793 lines) ← MONOLITHIC
│   │   ├── WindowProcessor::process_windowed_query() [line 124-185]
│   │   ├── should_emit_window() [line 587-635]
│   │   ├── compute_all_group_results() [line 203-334]
│   │   ├── extract_event_time() [line 567-584]
│   │   └── 38 other functions
│   │
│   └── context.rs (350+ lines)
│       ├── ProcessorContext (high-performance context)
│       ├── get_or_create_window_state() [line 100-130]
│       ├── get_dirty_window_states_mut() [line 272-283]
│       └── Dirty state tracking (32-bit bitmap)
│
└── internal.rs (807 lines)
    ├── WindowState [line 557-587]
    ├── RowsWindowState [line 607-647]
    └── StreamRecord (core data structure)
```

### Primary Bottleneck (Identified)

**Location**: `src/velostream/sql/execution/processors/window.rs:80`

```rust
// Inside process_windowed_query()
window_state.add_record(record.clone()); // ← O(N×M) memory allocations
//                              ^^^^^^
//                      Deep clone of entire StreamRecord
```

**Impact Analysis**:
- **Clone frequency**: Every record added to window buffer
- **Clone cost**: ~500 bytes × N records in buffer
- **SLIDING windows**: Worst case - clone every record, keep in buffer for window duration
- **Aggregate workload**: 10,000 records with 60-second window = 10,000 clones

**Current workaround**: O(N²) state cloning fixed, but this O(N) clone remains.

---

### Code Organization Issues

#### Issue #1: Monolithic Window.rs (2,793 Lines)

**File**: `src/velostream/sql/execution/processors/window.rs`

**Responsibilities Mixed**:
1. **Window Type Logic** (TUMBLING, SLIDING, SESSION, ROWS)
2. **Emission Strategy** (EMIT CHANGES, EMIT FINAL, watermarks)
3. **GROUP BY Routing** (single vs multi-group paths)
4. **Buffer Management** (add, cleanup, filtering)
5. **Time Extraction** (various timestamp formats)
6. **Aggregation Computation** (GROUP BY processing)
7. **Result Construction** (StreamRecord creation)

**Violation**: Single Responsibility Principle

**Metrics**:
- Functions: 42
- Cyclomatic complexity: High (nested match statements)
- Duplicate logic: 3 emission check methods, 2 state update methods

---

#### Issue #2: Duplicate Emission Logic

**Locations**:
- `should_emit_window()` [line 587-635] - Main emission logic
- `is_emit_changes()` [line 540-565] - EMIT CHANGES detection
- Scattered checks in `process_windowed_query()` [line 150-180]

**Problem**: Changes to emission semantics require updates in 3+ locations.

---

#### Issue #3: Mixed WindowState and RowsWindowState

**File**: `src/velostream/sql/execution/internal.rs`

```rust
// Time-based windows
pub struct WindowState {
    pub window_spec: WindowSpec,
    pub buffer: Vec<StreamRecord>,  // [line 557-587]
    pub last_emit: i64,
}

// Row-based windows
pub struct RowsWindowState {
    pub state_id: String,
    pub row_buffer: VecDeque<StreamRecord>,  // [line 607-647]
    pub buffer_size: u32,
    pub ranking_index: BTreeMap<i64, Vec<usize>>,
    pub emit_mode: RowsEmitMode,
    // ... 8 more fields
}
```

**Problem**: No shared trait, code duplication for similar operations.

---

## Proposed Architecture

### Design Principles

1. **Trait-Based**: Define behavior through traits, not concrete types
2. **Strategy Pattern**: Pluggable window strategies, emission strategies, group strategies
3. **Zero-Copy**: Use `Arc<StreamRecord>` for shared ownership
4. **Efficient Buffers**: Ring buffers for SLIDING windows, VecDeque for ROWS windows
5. **Separation of Concerns**: Each component has one well-defined responsibility

---

### Proposed Structure

```
src/velostream/sql/execution/
├── engine.rs (unchanged interface)
│
├── processors/
│   ├── window/
│   │   ├── mod.rs (module exports)
│   │   ├── processor.rs (main WindowProcessor - 300 lines)
│   │   ├── strategies/
│   │   │   ├── mod.rs
│   │   │   ├── window_strategy.rs (trait + implementations)
│   │   │   │   ├── TumblingWindowStrategy
│   │   │   │   ├── SlidingWindowStrategy
│   │   │   │   ├── SessionWindowStrategy
│   │   │   │   └── RowsWindowStrategy
│   │   │   │
│   │   │   ├── emission_strategy.rs
│   │   │   │   ├── EmitChangesStrategy
│   │   │   │   ├── EmitFinalStrategy
│   │   │   │   └── WatermarkEmissionStrategy
│   │   │   │
│   │   │   └── group_strategy.rs
│   │   │       ├── SingleGroupStrategy
│   │   │       └── MultiGroupStrategy
│   │   │
│   │   ├── buffer/
│   │   │   ├── mod.rs
│   │   │   ├── ring_buffer.rs (for SLIDING)
│   │   │   └── deque_buffer.rs (for ROWS)
│   │   │
│   │   └── time.rs (time extraction utilities)
│   │
│   └── context.rs (unchanged)
│
└── types/
    ├── window_state.rs (refactored state structures)
    └── stream_record.rs (Arc-wrapped records)
```

---

### Async vs Sync Design Decision

#### Current Architecture: Optimal Sync Core + Async Wrapper

**Decision**: Maintain synchronous window processing with async wrapper for ecosystem compatibility.

**Rationale**: Window processing is CPU-bound, not I/O-bound. Synchronous processing provides:
- Better CPU cache locality (stack-allocated, no heap indirection)
- Zero Future state machine overhead (~100-500 bytes per async call eliminated)
- No async polling overhead (~100-200ns per .await point eliminated)
- Predictable memory access patterns
- **3-5x faster** than full async architecture would be

#### Current Implementation

**Synchronous Core** (`window.rs:124-185`):
```rust
pub fn process_windowed_query(
    query_id: &str,
    query: &StreamingQuery,
    record: &StreamRecord,
    context: &mut ProcessorContext,
) -> Result<Option<StreamRecord>, SqlError> {
    // ← NO async, NO .await - Pure synchronous computation
    let event_time = Self::extract_event_time(record, window_spec.time_column());
    let window_state = context.get_or_create_window_state(query_id, window_spec);
    window_state.add_record(record.clone());
    // All aggregation, grouping, emission logic is synchronous
}
```

**Async Wrapper** (`engine.rs:523`):
```rust
pub async fn execute_with_record(&mut self, ...) -> Result<...> {
    // Thin async wrapper for ecosystem compatibility
    // Adds ~100-200ns overhead per call (acceptable)
    self.execute_internal(query, stream_record).await
}
```

#### Performance Breakdown

**Current architecture** (sync core + async wrapper):
```
Total per-record time:     ~60µs (100%)
├─ Window logic:           ~55µs (92%)  ← Sync, optimal
└─ Tokio overhead:         ~5µs (8%)    ← Acceptable
```

**Hypothetical all-async architecture**:
```
Total per-record time:     ~180-300µs (100%)
├─ Window logic:           ~55µs (18-31%)
├─ Async overhead:         ~100-150µs (33-83%)
└─ Memory overhead:        2-3x more allocations
```

**Throughput comparison**:
- Current (sync core): 15,721 rec/sec ✅
- Hypothetical (full async): 3,000-5,000 rec/sec ❌

#### Industry Validation

**Apache Flink**: Synchronous operators with async I/O connectors
- Window functions: Synchronous
- State backends: Synchronous
- Network I/O: Async

**ksqlDB (Kafka Streams)**: Synchronous stream processing
- Stream processing: Synchronous
- Windowing: Synchronous
- Kafka I/O: Async (librdkafka)

**Conclusion**: Industry leaders use synchronous processing for CPU-bound operations. Velostream's design aligns with best practices.

#### Optional Future Enhancement: Sync-Only API

**If** profiling shows async wrapper overhead >15% (currently 8%), consider adding:

```rust
// New synchronous API (no async overhead)
pub fn execute_with_record_sync(
    &mut self,
    query: &StreamingQuery,
    record: StreamRecord,
) -> Result<Option<StreamRecord>, SqlError> {
    self.execute_internal_sync(query, record)
    // No async overhead (~5% improvement)
}

// Keep async API for ecosystem compatibility
pub async fn execute_with_record(
    &mut self,
    query: &StreamingQuery,
    record: StreamRecord,
) -> Result<Option<StreamRecord>, SqlError> {
    // Delegates to sync version
    Ok(self.execute_with_record_sync(query, record)?)
}
```

**Expected Improvement**: 5-10% additional throughput (16.5K → 18K rec/sec)

**Trade-offs**:
- ✅ Better performance for CPU-bound workloads
- ❌ Loses seamless async ecosystem integration
- ❌ More complex API surface

**Recommendation**: Only implement if monitoring shows async overhead becomes a bottleneck. Current 8% overhead is acceptable given ecosystem benefits.

**Detailed Analysis**: See [FR-081-07-TOKIO-PERFORMANCE-ANALYSIS.md](./FR-081-07-TOKIO-PERFORMANCE-ANALYSIS.md) for comprehensive tokio overhead breakdown.

---

## Detailed Design

### 1. WindowStrategy Trait

**File**: `src/velostream/sql/execution/processors/window/strategies/window_strategy.rs` (new)

```rust
/// Trait for window type strategies
pub trait WindowStrategy: Send + Sync {
    /// Check if window should emit
    fn should_emit(&self, state: &WindowState, event_time: i64) -> bool;

    /// Add record to window buffer
    fn add_record(&mut self, state: &mut WindowState, record: Arc<StreamRecord>);

    /// Cleanup old records from buffer
    fn cleanup_buffer(&mut self, state: &mut WindowState, current_time: i64);

    /// Get window boundaries for current time
    fn get_window_bounds(&self, current_time: i64) -> (i64, i64);
}

/// TUMBLING window implementation
pub struct TumblingWindowStrategy {
    window_size_ms: i64,
}

impl WindowStrategy for TumblingWindowStrategy {
    fn should_emit(&self, state: &WindowState, event_time: i64) -> bool {
        if state.last_emit == 0 {
            if let Some(first_record) = state.buffer.first() {
                let first_time = first_record.timestamp;
                event_time - first_time >= self.window_size_ms
            } else {
                false
            }
        } else {
            event_time >= state.last_emit + self.window_size_ms
        }
    }

    fn add_record(&mut self, state: &mut WindowState, record: Arc<StreamRecord>) {
        state.buffer_arc.push(record);  // Arc, not clone!
    }

    fn cleanup_buffer(&mut self, state: &mut WindowState, _current_time: i64) {
        // TUMBLING: clear entire buffer after emission
        state.buffer_arc.clear();
    }

    fn get_window_bounds(&self, current_time: i64) -> (i64, i64) {
        let window_start = (current_time / self.window_size_ms) * self.window_size_ms;
        (window_start, window_start + self.window_size_ms)
    }
}

/// SLIDING window implementation with ring buffer
pub struct SlidingWindowStrategy {
    window_size_ms: i64,
    advance_ms: i64,
    ring_buffer: RingBuffer<Arc<StreamRecord>>,
}

impl WindowStrategy for SlidingWindowStrategy {
    fn should_emit(&self, state: &WindowState, event_time: i64) -> bool {
        if state.last_emit == 0 {
            if let Some(first_record) = self.ring_buffer.first() {
                event_time - first_record.timestamp >= self.advance_ms
            } else {
                false
            }
        } else {
            event_time >= state.last_emit + self.advance_ms
        }
    }

    fn add_record(&mut self, state: &mut WindowState, record: Arc<StreamRecord>) {
        self.ring_buffer.push(record);
    }

    fn cleanup_buffer(&mut self, state: &mut WindowState, current_time: i64) {
        // SLIDING: keep only records within window
        let window_start = current_time - self.window_size_ms;
        self.ring_buffer.retain(|r| r.timestamp >= window_start);
    }

    fn get_window_bounds(&self, current_time: i64) -> (i64, i64) {
        (current_time - self.window_size_ms, current_time)
    }
}
```

**Benefits**:
- **Pluggable**: Easy to add new window types
- **Testable**: Each strategy independently testable
- **Maintainable**: Changes isolated to specific strategy
- **Performance**: Specialized buffers per strategy (ring buffer vs Vec)

**Source References**:
- Current TUMBLING logic: `window.rs:600-609`
- Current SLIDING logic: `window.rs:612-621`
- Current SESSION logic: `window.rs:623-635`

---

### 2. EmissionStrategy Trait

**File**: `src/velostream/sql/execution/processors/window/strategies/emission_strategy.rs` (new)

```rust
/// Trait for emission timing strategies
pub trait EmissionStrategy: Send + Sync {
    /// Determine if this record should trigger emission
    fn should_emit(&self,
                   window_ready: bool,
                   watermark: Option<i64>,
                   record: &StreamRecord) -> bool;

    /// Process emission results (filtering, transformation)
    fn process_results(&self, results: Vec<StreamRecord>) -> Vec<StreamRecord>;
}

/// EMIT CHANGES: emit on every record
pub struct EmitChangesStrategy;

impl EmissionStrategy for EmitChangesStrategy {
    fn should_emit(&self,
                   _window_ready: bool,
                   _watermark: Option<i64>,
                   _record: &StreamRecord) -> bool {
        true  // Always emit
    }

    fn process_results(&self, results: Vec<StreamRecord>) -> Vec<StreamRecord> {
        results  // No filtering
    }
}

/// EMIT FINAL: emit only on window boundaries
pub struct EmitFinalStrategy;

impl EmissionStrategy for EmitFinalStrategy {
    fn should_emit(&self,
                   window_ready: bool,
                   watermark: Option<i64>,
                   record: &StreamRecord) -> bool {
        // Emit when window is complete OR watermark passed
        window_ready || watermark.map_or(false, |wm| record.timestamp >= wm)
    }

    fn process_results(&self, results: Vec<StreamRecord>) -> Vec<StreamRecord> {
        results  // No filtering
    }
}
```

**Benefits**:
- **Clear Semantics**: EMIT CHANGES vs EMIT FINAL logic isolated
- **Easy Extension**: Add EMIT ON WATERMARK, EMIT WHEN strategies
- **Composable**: Combine with window strategies

**Source References**:
- Current EMIT CHANGES logic: `window.rs:540-565`
- Current emission checks: `window.rs:150-180`

---

### 3. GroupByStrategy Trait

**File**: `src/velostream/sql/execution/processors/window/strategies/group_strategy.rs` (new)

```rust
/// Trait for GROUP BY aggregation strategies
pub trait GroupByStrategy: Send + Sync {
    /// Process records and produce grouped results
    fn process(&self,
               records: &[Arc<StreamRecord>],
               group_cols: &[String],
               aggregations: &[AggregationFunction]) -> Vec<StreamRecord>;
}

/// Single-group aggregation (no GROUP BY)
pub struct SingleGroupStrategy;

impl GroupByStrategy for SingleGroupStrategy {
    fn process(&self,
               records: &[Arc<StreamRecord>],
               _group_cols: &[String],
               aggregations: &[AggregationFunction]) -> Vec<StreamRecord> {
        // Compute single result for all records
        let mut accumulators = create_accumulators(aggregations);
        for record in records {
            update_accumulators(&mut accumulators, record);
        }
        vec![finalize_result(accumulators)]
    }
}

/// Multi-group aggregation (with GROUP BY)
pub struct MultiGroupStrategy;

impl GroupByStrategy for MultiGroupStrategy {
    fn process(&self,
               records: &[Arc<StreamRecord>],
               group_cols: &[String],
               aggregations: &[AggregationFunction]) -> Vec<StreamRecord> {
        // Hash-based grouping
        let mut groups: HashMap<Vec<FieldValue>, Vec<Accumulator>> = HashMap::new();

        for record in records {
            let group_key = extract_group_key(record, group_cols);
            let accumulators = groups.entry(group_key)
                                    .or_insert_with(|| create_accumulators(aggregations));
            update_accumulators(accumulators, record);
        }

        groups.into_iter()
              .map(|(key, accumulators)| finalize_result_with_key(key, accumulators))
              .collect()
    }
}
```

**Benefits**:
- **Routing Clarity**: No more GROUP BY routing logic in window processor
- **Optimization**: Specialized implementation per strategy (e.g., SIMD for single-group)
- **Testing**: Each strategy independently testable

**Source References**:
- Current GROUP BY routing: `window.rs:203-334`
- Single vs multi-group paths: scattered throughout `window.rs`

---

### 4. Arc<StreamRecord> Zero-Copy

**File**: `src/velostream/sql/execution/types.rs` (modify)

```rust
// BEFORE: Owned records, cloned everywhere
pub struct WindowState {
    pub buffer: Vec<StreamRecord>,  // Deep clones
}

// AFTER: Arc-wrapped records, shared ownership
pub struct WindowState {
    pub buffer: Vec<Arc<StreamRecord>>,  // Cheap Arc::clone()
}

impl WindowState {
    pub fn add_record(&mut self, record: Arc<StreamRecord>) {
        self.buffer.push(record);  // Just increments ref count!
    }
}
```

**Performance Impact**:
- **Before**: 500 bytes × 10,000 records = **5 MB cloned**
- **After**: 8 bytes (pointer) × 10,000 = **80 KB**
- **Improvement**: **62x less memory copying**

**Implementation Steps**:
1. Wrap StreamRecord in Arc at ingestion point (engine.rs:516)
2. Update all buffer operations to use Arc<StreamRecord>
3. Update WindowState, RowsWindowState structures
4. Update aggregation functions to accept &Arc<StreamRecord>

**Source References**:
- Current cloning: `window.rs:80`
- WindowState: `internal.rs:557-587`
- RowsWindowState: `internal.rs:607-647`

---

### 5. Ring Buffer for SLIDING Windows

**File**: `src/velostream/sql/execution/processors/window/buffer/ring_buffer.rs` (new)

```rust
/// Ring buffer for efficient SLIDING window management
pub struct RingBuffer<T> {
    buffer: Vec<T>,
    capacity: usize,
    start: usize,
    len: usize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            capacity,
            start: 0,
            len: 0,
        }
    }

    pub fn push(&mut self, item: T) {
        if self.len < self.capacity {
            self.buffer.push(item);
            self.len += 1;
        } else {
            let index = (self.start + self.len) % self.capacity;
            self.buffer[index] = item;
            self.start = (self.start + 1) % self.capacity;
        }
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        // Efficient in-place filtering
        let mut write_idx = self.start;
        let mut new_len = 0;

        for i in 0..self.len {
            let read_idx = (self.start + i) % self.capacity;
            if f(&self.buffer[read_idx]) {
                if write_idx != read_idx {
                    self.buffer.swap(write_idx, read_idx);
                }
                write_idx = (write_idx + 1) % self.capacity;
                new_len += 1;
            }
        }

        self.len = new_len;
    }
}
```

**Benefits**:
- **O(1) insertions**: No Vec reallocation
- **Efficient cleanup**: In-place filtering
- **Memory efficient**: Pre-allocated, no growth

**Performance Impact**: 1.5-2x improvement for SLIDING windows

---

## Implementation Roadmap

### Phase 1: Preparation (Week 1)
**Goal**: Set up refactoring infrastructure

1. Create new module structure
2. Add `Arc<StreamRecord>` wrapper (feature-flagged)
3. Write comprehensive integration tests
4. Create performance regression test suite

**Deliverables**:
- [ ] New module structure in place
- [ ] Feature flag for Arc migration: `--features arc-records`
- [ ] 50+ integration tests covering all window types
- [ ] Performance baseline established

---

### Phase 2: Strategy Traits (Week 2)
**Goal**: Implement trait-based architecture

1. Implement WindowStrategy trait + all strategies
2. Implement EmissionStrategy trait
3. Implement GroupByStrategy trait
4. Migrate processor.rs to use strategies

**Deliverables**:
- [ ] All strategy traits implemented
- [ ] Old code paths still working (parallel implementation)
- [ ] New code paths passing all tests

---

### Phase 3: Migration & Optimization (Week 3)
**Goal**: Complete migration, optimize, cleanup

1. Enable Arc<StreamRecord> by default
2. Implement ring buffer for SLIDING
3. Remove old code paths
4. Performance testing & tuning
5. Documentation updates

**Deliverables**:
- [ ] Arc migration complete
- [ ] Ring buffer operational
- [ ] 3-5x performance improvement verified
- [ ] All old code removed
- [ ] Documentation updated

---

## Performance Projections

### Current Performance (After FR-081 Phase 1)

| Window Type | Current | Target (Phase 2) | Improvement |
|-------------|---------|------------------|-------------|
| TUMBLING | 15.7K rec/sec | 50-75K rec/sec | **3-4.7x** |
| SLIDING | 15.7K rec/sec | 40-60K rec/sec | **2.5-3.8x** |
| SESSION | TBD | 30-50K rec/sec | **2-3x** |
| EMIT CHANGES | 29.3K rec/sec | 80-120K rec/sec | **2.7-4.1x** |

### Breakdown by Optimization

| Optimization | Impact | Cumulative |
|--------------|--------|------------|
| Baseline (current) | 15.7K rec/sec | 1.0x |
| Arc<StreamRecord> | +3x | **47.1K rec/sec** (3.0x) |
| Ring buffer (SLIDING) | +1.5x | **70.6K rec/sec** (4.5x) |
| Trait-based dispatch | +10% | **77.7K rec/sec** (4.9x) |
| SIMD aggregations (future) | +2x | **155K rec/sec** (9.9x) |

**Conservative Estimate (Phase 2)**: 50K rec/sec (**3.2x improvement**)
**Optimistic Estimate (Phase 2)**: 100K rec/sec (**6.4x improvement**)

---

## Risks & Mitigations

### Risk #1: Breaking Changes
**Likelihood**: HIGH
**Impact**: HIGH
**Mitigation**:
- Feature flags for gradual rollout
- Comprehensive test coverage (50+ tests)
- Parallel implementation (old + new paths)
- Staged migration over 3 weeks

### Risk #2: Performance Regression
**Likelihood**: MEDIUM
**Impact**: HIGH
**Mitigation**:
- Performance regression test suite
- Continuous benchmarking
- Rollback plan (feature flag)

### Risk #3: Complexity Increase
**Likelihood**: MEDIUM
**Impact**: MEDIUM
**Mitigation**:
- Clear documentation
- Design review before implementation
- Code review process
- Architectural decision records (ADRs)

---

## Comparison with Industry Standards

### Apache Flink

**Architecture**:
- Window assigners (equivalent to our WindowStrategy)
- Triggers (equivalent to our EmissionStrategy)
- Evictor pattern for cleanup

**Lessons**:
- Separation of concerns works well at scale
- Trait-based design enables optimization

**Reference**: https://flink.apache.org/

---

### ksqlDB

**Architecture**:
- Windowed KTable abstraction
- Suppression semantics (EMIT FINAL equivalent)
- Materialized aggregations

**Lessons**:
- Clear emit semantics critical for correctness
- GROUP BY routing needs careful design

**Reference**: https://docs.ksqldb.io/

---

## Success Metrics

### Performance
- [ ] TUMBLING: 50K+ rec/sec (current: 15.7K)
- [ ] SLIDING: 40K+ rec/sec (current: 15.7K)
- [ ] Memory usage: <200 MB for 100K records
- [ ] CPU usage: <50% of single core

### Code Quality
- [ ] window.rs: <500 lines (current: 2,793)
- [ ] Cyclomatic complexity: <10 (current: >20)
- [ ] Test coverage: >90% (current: ~80%)
- [ ] No clippy warnings

### Maintainability
- [ ] New window type: <100 lines of code
- [ ] New emission strategy: <50 lines
- [ ] Documentation: 100% of public APIs

---

## Related Documents

- **Overview**: [FR-081-01-OVERVIEW.md](./FR-081-01-OVERVIEW.md)
- **O(N²) Fix**: [FR-081-02-O-N2-FIX-ANALYSIS.md](./FR-081-02-O-N2-FIX-ANALYSIS.md)
- **Emission Fix**: [FR-081-03-EMISSION-LOGIC-FIX.md](./FR-081-03-EMISSION-LOGIC-FIX.md)
- **SQL Window Documentation**: [../../sql/by-task/window-analysis.md](../../sql/by-task/window-analysis.md)

---

**Document Version**: 1.0
**Last Updated**: 2025-11-01
**Status**: Proposed for Phase 2 Implementation
**Approval Required**: Yes (architectural review)
