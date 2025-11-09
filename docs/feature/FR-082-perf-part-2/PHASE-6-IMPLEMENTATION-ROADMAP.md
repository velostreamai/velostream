# Phase 6 Implementation Roadmap: Eliminate Unnecessary Locks & Cloning

**Date**: November 9, 2025 (REVISED)
**Target**: Single-core 70-142K rec/sec (4.2-8.5x improvement)
**Effort**: S (2-3 weeks for complete Phase 6)
**Risk**: Low (ownership changes only, no algorithmic changes)

---

## Phase 6 Overview (REVISED)

**Critical Discovery**: Per-partition engines are accessed by ONLY ONE task (partition receiver).
- RwLock is **unnecessary** (no concurrent access)
- Arc cloning is **unnecessary** (single owner)
- Record cloning is **unnecessary** (engine only reads)

Phase 6 will eliminate this unnecessary overhead:
1. **Phase 6.3a**: Remove RwLock from per-partition engines (direct ownership)
2. **Phase 6.3b**: Remove record cloning (pass by reference)
3. **Phase 6.4**: Fix locking/cloning in DataReader & DataWriter
4. **Phase 6.5**: Use DashMap only where concurrency exists (state structures)
5. **Phase 6.6**: Validation and tuning

**Key Principle**: Use locks only where concurrent access actually exists.

**Expected Improvement**: 97.9% overhead → 85% overhead
**Throughput Target**: 16.6K rec/sec → 70-142K rec/sec

---

## Phase 6 Execution Plan

### Phase 6.3a: Remove RwLock from Per-Partition Engines (CRITICAL)

**Effort**: S (2-3 days)
**Target**: 16.6K → 50K rec/sec (30-50% improvement)
**Impact**: Eliminates ~5000 lock operations per batch from hot path

**Changes Required**:
1. File: `src/velostream/server/v2/partition_manager.rs`
   - Change field: `StdRwLock<Option<Arc<RwLock<StreamExecutionEngine>>>>` → `Option<StreamExecutionEngine>`
   - Remove `.read()` and `.write()` calls
   - Remove `.clone()` on engine reference
   - Remove async/await on engine operations

2. File: `src/velostream/server/v2/coordinator.rs`
   - Update set_execution_engine() to take ownership (not Arc)
   - Pass engine directly instead of Arc::clone()

**Baseline Benchmark**:
```bash
cargo test scenario_2_pure_group_by_baseline -- --nocapture
# Expected: 16.6K → 50K rec/sec
```

---

### Phase 6.3b: Remove Record Cloning (Use References)

**Effort**: S (1-2 days)
**Target**: 50K → 58K rec/sec (3.5x cumulative improvement)
**Impact**: Eliminates 100ms allocation overhead per batch

**Changes Required**:
1. File: `src/velostream/sql/execution/engine.rs`
   - Change signature: `execute_with_record(&mut self, query: &StreamingQuery, record: &StreamRecord)`
   - Remove record parameter ownership requirement

2. Files: All callers of execute_with_record
   - `src/velostream/server/processors/simple.rs`
   - `src/velostream/server/v2/partition_manager.rs`
   - All test files
   - Pass `&record` instead of `record.clone()`

**Baseline Benchmark**:
```bash
cargo test scenario_2_pure_group_by_baseline -- --nocapture
# Expected: 50K → 58K rec/sec
```

---

### Phase 6.4: Fix DataReader & DataWriter (I/O Pipeline)

**Effort**: M (3-5 days)
**Target**: 58K → 70K rec/sec (4.2x cumulative improvement)
**Impact**: Removes hidden locking/cloning in I/O pipeline

**Analysis Phase**:
1. **Check DataReader** (`src/velostream/datasource/`)
   - Does it clone entire batch templates?
   - Are records copied or referenced?
   - Identify allocation overhead

2. **Check DataWriter** (`src/velostream/datasource/`)
   - Does it acquire locks per record?
   - Does it clone for serialization?
   - Any Arc<Mutex<>> operations?

**Expected Issues**:
- Mock readers: `batch_template.clone()`
- Serialization: Records cloned before format conversion
- Output buffers: Locks per record operation

**Optimizations**:
- Reuse batch buffers instead of cloning
- Batch lock acquisitions on output
- Serialize in-place without cloning

**Baseline Benchmark**:
```bash
cargo test scenario_2_pure_group_by_baseline -- --nocapture
# Expected: 58K → 70K rec/sec
```

---

### Phase 6.5: DashMap for State Structures (Per-Entry Locking)

**Effort**: S (2-3 days)
**Target**: 70K → 77K rec/sec (4.6x cumulative improvement)
**Impact**: Per-entry locking for concurrent access

**Changes Required**:
1. File: `Cargo.toml`
   - Add: `dashmap = "5.5"`

2. File: `src/velostream/sql/execution/engine.rs`
   - Change: `group_states: HashMap<...>` → `group_states: Arc<DashMap<...>>`
   - Change: `window_v2_states: HashMap<...>` → `window_v2_states: Arc<DashMap<...>>`
   - Update all access patterns (API is mostly compatible)

**Tests to Update**:
- `tests/unit/sql/execution/engine_test.rs`
- `tests/unit/sql/execution/aggregation/group_by_test.rs`
- `tests/unit/sql/execution/windows/window_v2_test.rs`

**Baseline Benchmark**:
```bash
cargo test scenario_2_pure_group_by_baseline -- --nocapture
# Expected: 70K → 77K rec/sec
```

---

### Phase 6.6: Validation & Performance Tuning

**Effort**: S (2-3 days)
**Target**: 70-142K rec/sec (4.2-8.5x final improvement)
**Impact**: Comprehensive testing and verification

**Testing**:
```bash
cargo test scenario_1_rows_window_baseline -- --nocapture
cargo test scenario_2_pure_group_by_baseline -- --nocapture
cargo test scenario_3a_tumbling_standard_baseline -- --nocapture
cargo test scenario_3b_tumbling_emit_changes_baseline -- --nocapture
```

**Expected Results**:
| Phase | Change | Target | Cumulative |
|-------|--------|--------|-----------|
| 6.3a | Remove RwLock | 50K | 3.0x |
| 6.3b | Remove cloning | 58K | 3.5x |
| 6.4 | Fix I/O | 70K | 4.2x |
| 6.5 | DashMap | 77K | 4.6x |
| 6.6 | Validation | 70-142K | 4.2-8.5x |

**Profiling**:
- Use flamegraph: `cargo flamegraph --test scenario_2_pure_group_by_baseline`
- Measure CPU utilization (expect 2% → 25-35%)
- Verify lock bottlenecks eliminated

**Documentation**:
- Create PHASE-6-RESULTS.md with benchmarks
- Document lock removal patterns
- Commit: "feat(FR-082 Phase 6): Remove unnecessary locks and cloning - 4.2-8.5x improvement"

---

## Success Criteria

✅ **Functional**:
- All 531+ unit tests pass
- All scenarios process correctly
- Metrics reported accurately
- No cross-partition interference

✅ **Performance**:
- Throughput: 70-142K rec/sec minimum
- Per-core efficiency: 80%+ (vs 2% current)
- No remaining lock bottlenecks

✅ **Quality**:
- Code formatting: `cargo fmt --all -- --check` ✅
- Clippy linting: `cargo clippy --all-targets` ✅
- Documentation: PHASE-6-RESULTS.md with analysis

---

## Key Principle

**Use locks only where concurrent access actually exists.**

- Per-partition engines: Direct ownership (no locks)
- Multi-key state (GROUP BY): DashMap (per-entry locks)
- Shared resources: Arc<RwLock> (only if necessary)

---

*Phase 6 is the foundation for single-core breakthrough.*
*Phase 7 will add vectorization for additional 4-8x improvement.*
*Combined: 1.5M+ rec/sec target becomes achievable.*
