# Phase 6 REVISED Roadmap: Eliminate All Unnecessary Locking & Cloning

**Date**: November 9, 2025 (REVISED)
**Insight**: Per-partition engines don't need RwLock - remove lock overhead from entire pipeline
**Target**: Single-core 70-142K rec/sec (by eliminating lock operations entirely)

---

## Critical Discovery

After Phase 6.2, the architecture has **per-partition engines** that are **only accessed by one task each**.

**Question**: If only one task accesses an engine, why does it need an RwLock?

**Answer**: It doesn't. This is unnecessary overhead introduced by the shared-engine design pattern.

**Scope**: This unnecessary locking and cloning exists throughout the pipeline:
1. **Execution Engine**: RwLock around engine itself
2. **Record Cloning**: Records cloned when passed to engine
3. **DataReader**: Might clone entire batches
4. **DataWriter**: Might lock/clone on output

---

## Phase 6 REVISED Strategy

### Phase 6.3: Remove All Unnecessary Locks (CRITICAL)

**Insight**: Per-partition execution means no concurrent access within partition.

#### 6.3.1 Remove RwLock from Per-Partition Engines

**Current** (unnecessary lock):
```rust
pub struct PartitionStateManager {
    execution_engine: StdRwLock<Option<Arc<RwLock<StreamExecutionEngine>>>>,
    query: StdRwLock<Option<Arc<StreamingQuery>>>,
}

// Processing
let engine_opt = self.execution_engine.read().unwrap().clone();
if let Some(engine) = engine_opt {
    let mut engine_guard = engine.write().await;  // ← LOCK (unnecessary!)
    engine_guard.execute_with_record(&query, record.clone()).await?;
}
```

**Problem**:
- RwLock acquired per record (5000 times per batch)
- Arc clone per record (5000 times per batch)
- Interior mutability (StdRwLock) not needed
- Only this partition's receiver accesses this engine

**Solution**:
```rust
pub struct PartitionStateManager {
    execution_engine: Option<StreamExecutionEngine>,  // ← Direct ownership, no lock!
    query: Option<Arc<StreamingQuery>>,
}

// Processing
if let (Some(engine), Some(query)) = (&mut self.execution_engine, &self.query) {
    engine.execute_with_record(&query, &record)?;  // ← No lock, no await, no clone
}
```

**Impact**:
- Eliminates 5000 RwLock operations per batch
- Eliminates Arc cloning overhead
- Eliminates async/await context switches from locks
- **Expected**: 30-50% improvement (5000 lock ops is significant!)
- **Target**: 16.6K → 50K rec/sec

**Files to Modify**:
- `src/velostream/server/v2/partition_manager.rs` (change engine ownership)
- `src/velostream/sql/execution/types.rs` (change execute_with_record signature)
- Tests: verify no cross-partition access

**Risk**: Low (only single partition accesses each engine)

---

#### 6.3.2 Remove Record Cloning in execute_with_record

**Current**:
```rust
engine_guard.execute_with_record(&query, record.clone())?;  // ← FULL CLONE
```

**Problem**:
- 5000 records × ~20µs per clone = 100ms overhead per batch
- Unnecessary because engine only reads record fields

**Solution**:
```rust
engine.execute_with_record(&query, &record)?;  // ← REFERENCE (no clone!)
```

**Changes Required**:
- Change signature: `fn execute_with_record(&mut self, query, record: &StreamRecord)`
- Update all internal uses to take references
- No semantic change (engine doesn't mutate records)

**Impact**:
- Eliminates record cloning overhead
- **Expected**: 15% improvement
- **Target**: 50K → 58K rec/sec

**Files to Modify**:
- `src/velostream/sql/execution/engine.rs` (signature)
- All callers of execute_with_record

---

### Phase 6.4: Remove Locking/Cloning in DataReader & DataWriter

**Discovery**: Sources and sinks might have their own locking/cloning overhead.

#### 6.4.1 Analyze DataReader Implementation

**Current Pattern** (typical):
```rust
async fn read(&mut self) -> Result<Vec<StreamRecord>> {
    let batch = self.source.read().await?;  // ← Might involve cloning
    Ok(batch)
}
```

**Questions to Answer**:
1. Does DataReader clone the entire batch on each read()?
2. Does it acquire locks on underlying data structures?
3. Is the batch pre-allocated or allocated per read?

**Expected Issues**:
- Mock implementations clone entire batch templates
- Real Kafka reader might acquire consumer locks
- Avro/Protobuf deserialization might allocate per record

#### 6.4.2 Analyze DataWriter Implementation

**Current Pattern** (typical):
```rust
async fn write(&mut self, record: StreamRecord) -> Result<()> {
    self.sink.write(record).await?;  // ← Might involve locks/clones
    Ok(())
}
```

**Questions to Answer**:
1. Does DataWriter clone records for serialization?
2. Does it acquire locks on output buffer?
3. Is serialization lazy or eager?

**Expected Issues**:
- Per-record serialization (Kafka, files, etc.)
- Lock on output buffer per write
- Cloning for schema validation

#### 6.4.3 Optimize Based on Findings

**Potential Optimizations**:

**If DataReader clones batches**:
```rust
// BEFORE: Clone entire batch per read
let batch = self.batch_template.clone();

// AFTER: Reuse batch structure
let batch = &mut self.batch_buffer;
batch.clear();
// Populate without cloning
```

**If DataWriter locks per record**:
```rust
// BEFORE: Acquire lock per record
for record in records {
    let mut sink = self.output.write().await;
    sink.write(record).await?;
}

// AFTER: Acquire lock once for batch
let mut sink = self.output.write().await;
for record in records {
    sink.write(record).await?;
}
```

**If DataWriter clones for serialization**:
```rust
// BEFORE: Clone for each format conversion
let json = serde_json::to_string(&record)?;

// AFTER: Serialize in-place to buffer
record.serialize_to_buffer(&mut buffer)?;
```

**Impact**: 10-20% improvement (depends on actual issues found)
**Target**: 58K → 70K rec/sec

---

### Phase 6.5: DashMap for Per-Entry Locking (State Structures)

**Purpose**: Only apply locking where concurrency actually exists (state structures).

#### 6.5.1 Convert group_states to DashMap

**Current** (no longer needs global lock):
```rust
group_states: HashMap<String, Arc<GroupByState>>
// Protected by engine RwLock (which we removed!)
```

**New** (per-entry locking):
```rust
group_states: Arc<DashMap<String, Arc<GroupByState>>>
```

**Why DashMap Here**:
- Multiple GROUP BY keys might be updated
- Different keys can be updated independently
- DashMap provides per-entry locking (no global lock)

**Impact**: 5-10% improvement (only helps if many different GROUP BY keys)
**Target**: 70K → 77K rec/sec

---

### Phase 6.6: Validation & Tuning

**Comprehensive Testing**:
```bash
cargo test scenario_1_rows_window_baseline -- --nocapture
cargo test scenario_2_pure_group_by_baseline -- --nocapture
cargo test scenario_3a_tumbling_standard_baseline -- --nocapture
cargo test scenario_3b_tumbling_emit_changes_baseline -- --nocapture
```

**Expected Results**:
| Phase | Change | Target | Cumulative |
|-------|--------|--------|-----------|
| 6.3a | Remove RwLock from engines | 50K | 3.0x |
| 6.3b | Remove record cloning | 58K | 3.5x |
| 6.4 | DataReader/DataWriter | 70K | 4.2x |
| 6.5 | DashMap state locking | 77K | 4.6x |
| 6.6 | Tuning & validation | 70-142K | 4.2-8.5x |

---

## Complete Phase 6 Revised Timeline

| Phase | Task | Effort | Impact | Cumulative |
|-------|------|--------|--------|-----------|
| 6.2 ✅ | Remove inter-partition locks | S | 12.89x speedup | 78.6K (4 cores) |
| 6.3a | Remove RwLock from engines | S | 30-50% | 50K → 58K |
| 6.3b | Remove record cloning | S | 15% | 58K → 70K |
| 6.4 | DataReader/DataWriter | M | 10-20% | 70K → 85K |
| 6.5 | DashMap state locking | S | 5-10% | 85K → 95K |
| 6.6 | Validation & tuning | S | Final | 70-142K target |

**Total Phase 6**: 4.2-8.5x improvement over current (16.6K → 70-142K rec/sec)

---

## Why This Order?

1. **Phase 6.3a (RwLock removal)**: Biggest impact, eliminates lock ops from hot path
2. **Phase 6.3b (Record refs)**: Eliminates allocation overhead
3. **Phase 6.4 (Source/Sink)**: Fixes I/O bottlenecks
4. **Phase 6.5 (DashMap)**: Only use locks where actual concurrency exists
5. **Phase 6.6 (Validation)**: Comprehensive testing and tuning

---

## Key Principle: Locks Only Where Needed

**Old approach** (Phase 6.2): Wrap everything in Arc<RwLock> for safety
**New approach** (Phase 6.3+): Use locks only where concurrent access exists

**Ownership model**:
- Per-partition engine: Direct ownership (no lock needed) ✅
- Per-partition state: DirectOwnership (no lock needed) ✅
- Multi-key state (GROUP BY): DashMap (per-key lock) ✅
- Shared resources: Arc<RwLock> (only if necessary) ✅

---

## Next Steps (Immediate)

1. **Confirm Phase 6.3a change is safe**
   - Verify only partition receiver accesses engine
   - No cross-partition engine sharing
   - Update PartitionStateManager ownership

2. **Implement Phase 6.3a (remove RwLock)**
   - Change engine ownership in PartitionStateManager
   - Change execute_with_record signature to take &record
   - Update all callers
   - Run tests (expected 50K rec/sec)

3. **Analyze DataReader/DataWriter**
   - Check for cloning in read()
   - Check for locking in write()
   - Identify biggest bottlenecks

4. **Continue with 6.3b-6.6**

---

## Impact on Final Target

**Original Phase 6 goal**: 70-142K rec/sec (reduce overhead 97% → 85%)

**Revised Phase 6 insight**: Can achieve same target by removing locks instead of optimizing with locks!

**Path**:
- Direct ownership (no locks where unnecessary)
- DashMap (locks only where concurrency exists)
- Arc<StreamRecord> (eliminate cloning)
- Result: Same 70-142K target with better design

---

*This revised roadmap eliminates lock overhead at the source instead of trying to optimize around it.*
*Much more effective and simpler to implement.*
