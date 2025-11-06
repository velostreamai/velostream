# FR-082: Performance Scenario Clarification

**Date**: November 6, 2025
**Purpose**: Distinguish between three different SQL scenarios with distinct performance characteristics

---

## Critical Distinction: Four Different Scenarios

The FR-082 analysis and V2 blueprint conflated four fundamentally different SQL scenarios. Each has different bottlenecks, performance characteristics, and optimization strategies.

**Key Update**: Scenario 3 (GROUP BY + WINDOW) is split into two sub-scenarios based on emission mode:
- **3a**: Standard emission (batch - emit only on window close)
- **3b**: EMIT CHANGES (continuous - emit on every update)

---

## Scenario 1: ROWS WINDOW (No GROUP BY)

### Query Example
```sql
SELECT
    symbol,
    price,
    AVG(price) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
    ) as moving_avg,
    MIN(price) OVER (...) as min_price,
    MAX(price) OVER (...) as max_price
FROM market_data
```

### Characteristics
- **Window Type**: Memory-bounded (ROWS WINDOW with BUFFER N ROWS)
- **Grouping**: PARTITION BY (for window function partitioning, NOT GROUP BY)
- **Aggregation**: Window functions (AVG, MIN, MAX OVER)
- **State**: Sliding buffer per partition (e.g., last 100 rows per symbol)

### State Management
```rust
// Per-partition state
struct RowsWindowState {
    partition_key: PartitionKey,              // e.g., symbol="SYM1"
    buffer: VecDeque<StreamRecord>,          // Last N rows
    buffer_size: usize,                      // Max buffer (e.g., 100)
}
```

### Performance Profile
- **Baseline**: Unknown (needs dedicated benchmark)
- **Bottleneck**: Buffer management (VecDeque operations, record cloning)
- **Expected throughput**: Should be high (simple buffer operations)

### Phase 4B/4C Applicability
**❌ NOT APPLICABLE** - ROWS WINDOW does not use GROUP BY hash tables

**Optimization Strategy** (if needed):
- Use Arc&lt;StreamRecord&gt; in buffer (avoid cloning)
- Circular buffer instead of VecDeque
- Efficient partition key lookup

---

## Scenario 2: Pure GROUP BY (No WINDOW)

### Query Example
```sql
SELECT
    category,
    COUNT(*) as record_count,
    SUM(amount) as total_amount,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM stream
GROUP BY category
```

### Characteristics
- **Window Type**: None (continuous aggregation)
- **Grouping**: GROUP BY clause (category)
- **Aggregation**: Standard aggregations (COUNT, SUM, AVG, MIN, MAX)
- **State**: Hash table of group accumulators (unbounded, grows with unique keys)

### State Management
```rust
// Global GROUP BY state
pub struct GroupByState {
    pub groups: HashMap<Vec<String>, GroupAccumulator>,  // ← BOTTLENECK!
}

pub struct GroupAccumulator {
    pub non_null_counts: HashMap<String, u64>,
    pub sums: HashMap<String, f64>,
    pub mins: HashMap<String, FieldValue>,
    pub maxs: HashMap<String, FieldValue>,
    pub sample_record: Option<StreamRecord>,  // ← CLONING!
    // ...
}
```

### Performance Profile
- **Baseline**: ~10K rec/sec (mentioned in FR-082-overhead-analysis.md)
- **With 5 aggregations**: 3.58K rec/sec (from FR-082-PHASE4-BOTTLENECK_FINDINGS.md)
- **Bottleneck**: Vec&lt;String&gt; hash keys + HashMap cloning + string allocations

### Phase 4B/4C Applicability
**✅ FULLY APPLICABLE** - This is the PRIMARY target for optimization

**Phase 4B Impact**: 3.58K → 15-20K rec/sec
- Replace `HashMap<Vec<String>, GroupAccumulator>` with `FxHashMap<GroupKey, GroupAccumulator>`
- Pre-computed hashing in GroupKey
- Arc&lt;[FieldValue]&gt; instead of Vec&lt;String&gt;

**Phase 4C Impact**: 15-20K → 200K rec/sec
- Wrap in `Arc<FxHashMap<...>>` for cheap cloning
- Use `Arc<StreamRecord>` in sample_record
- String interning for field names
- Group key caching

---

## Scenario 3: GROUP BY with WINDOW (TUMBLING/SLIDING/SESSION)

**CRITICAL**: This scenario has **two emission modes** with different performance characteristics:
- **3a: Standard Emission** (emit only when window closes)
- **3b: EMIT CHANGES** (emit on every update - continuous streaming)

---

### Scenario 3a: GROUP BY + WINDOW (Standard Emission)

#### Query Example
```sql
SELECT
    trader_id,
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    SUM(quantity) as total_quantity,
    SUM(price * quantity) as total_value
FROM market_data
GROUP BY trader_id, symbol
WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE);
-- NOTE: No EMIT CHANGES clause - standard emission mode
```

#### Characteristics
- **Window Type**: Time-based (TUMBLING, SLIDING, or SESSION)
- **Grouping**: GROUP BY clause (trader_id, symbol)
- **Aggregation**: Standard aggregations within each window
- **State**: Hash table of group accumulators PER WINDOW + window metadata
- **Emission**: **Batch mode** - emit entire window only when window closes

### State Management
```rust
// Per-window GROUP BY state
pub struct WindowState {
    pub window_id: WindowId,
    pub window_start: i64,
    pub window_end: i64,
    pub group_by_state: GroupByState,  // ← Same as Scenario 2!
}

// Multiple windows tracked simultaneously
pub struct PersistentWindowStates {
    windows: Vec<WindowState>,  // Active windows
}
```

#### Performance Profile (Standard Emission)
- **Baseline (TUMBLING + GROUP BY)**: 127K rec/sec (from tumbling_instrumented_profiling.rs)
- **Emission Pattern**: Batch emission when window closes (e.g., once per minute for 1-minute tumbling)
- **Output Rate**: Low (1 batch per window interval)
- **Backpressure**: Minimal (infrequent large batches)

**⚠️ CRITICAL INCONSISTENCY DISCOVERED**:

| Scenario | Throughput | Source |
|----------|-----------|--------|
| **Pure GROUP BY** | ~10K rec/sec | FR-082-overhead-analysis.md |
| **TUMBLING + GROUP BY (Standard)** | 127K rec/sec | tumbling_instrumented_profiling.rs |

**This doesn't make sense!** TUMBLING + GROUP BY should be SLOWER than pure GROUP BY due to additional window overhead.

**Hypothesis**: The 127K measurement might be:
1. Different query (fewer aggregations?)
2. Different dataset size
3. Different measurement methodology
4. Or pure GROUP BY 10K figure is wrong

#### Phase 4B/4C Applicability (Standard Emission)
**✅ APPLICABLE** - Uses same GROUP BY hash table as Scenario 2

**Expected Impact**:
- Phase 4B: Should improve GROUP BY portion (hash table operations)
- Phase 4C: Should reduce state cloning when windows close/emit
- Window management overhead: Separate concern (not addressed by Phase 4B/4C)

---

### Scenario 3b: GROUP BY + WINDOW + EMIT CHANGES

#### Query Example
```sql
SELECT
    trader_id,
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    SUM(quantity) as total_quantity,
    SUM(price * quantity) as total_value
FROM market_data
GROUP BY trader_id, symbol
WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
EMIT CHANGES;  -- ← CONTINUOUS STREAMING MODE
```

#### Characteristics
- **Window Type**: Time-based (TUMBLING, SLIDING, or SESSION)
- **Grouping**: GROUP BY clause (trader_id, symbol)
- **Aggregation**: Standard aggregations within each window
- **State**: Hash table of group accumulators PER WINDOW + change tracking
- **Emission**: **Continuous mode** - emit updated results on every record arrival

#### State Management (EMIT CHANGES)
```rust
// Per-window GROUP BY state with change tracking
pub struct WindowStateWithChanges {
    pub window_id: WindowId,
    pub window_start: i64,
    pub window_end: i64,
    pub group_by_state: GroupByState,  // Same hash table

    // EMIT CHANGES additions:
    pub changed_groups: HashSet<GroupKey>,  // Track which groups changed
    pub last_emitted: HashMap<GroupKey, GroupAccumulator>,  // Previous values
}

impl WindowStateWithChanges {
    fn process_record_and_emit(&mut self, record: &StreamRecord) -> Option<Vec<OutputRecord>> {
        let group_key = extract_group_key(record);

        // Update aggregations (same as 3a)
        self.group_by_state.update(group_key.clone(), record);

        // Mark as changed
        self.changed_groups.insert(group_key.clone());

        // Emit updated aggregate immediately (EMIT CHANGES difference!)
        let current_agg = self.group_by_state.get(&group_key);
        Some(vec![OutputRecord::from_aggregate(current_agg)])
    }
}
```

#### Performance Profile (EMIT CHANGES)
- **Baseline (MEASURED)**: **22,496 rec/sec** ✅ (from job_server_tumbling_emit_changes_performance test)
- **Emission Pattern**: Continuous emission on every record arrival
- **Output Rate**: High (up to N output records for N input records)
- **Backpressure**: Higher (frequent small emissions vs infrequent large batches)

**Measured Performance vs Standard Emission (3a)**:
- **Standard emission (3a)**: 23,591 rec/sec
- **EMIT CHANGES (3b)**: 22,496 rec/sec
- **Actual overhead**: **4.6%** (1.05x slowdown) ✅

**Key Discovery**:
- ✅ **Original estimate**: 80-100K rec/sec (20-40% slower than 3a's assumed 127K baseline)
- ✅ **Actual measurement**: Only 4.6% slower than standard emission
- ✅ **EMIT CHANGES overhead is minimal** - excellent for real-time use cases!

**Why the overhead is so low**:
- Change tracking is efficient (HashSet operations are O(1))
- Serialization cost is already amortized across records
- Backpressure doesn't materialize with moderate cardinality

#### Phase 4B/4C Applicability (EMIT CHANGES)
**✅ APPLICABLE** - Uses same GROUP BY hash table as Scenario 2 and 3a

**Expected Impact**:
- Phase 4B: Improves GROUP BY hash table operations (same as 3a)
- Phase 4C: Reduces state cloning + Arc-wraps last_emitted values
- **Additional benefit for EMIT CHANGES**: Arc<GroupAccumulator> enables cheap cloning for last_emitted comparison

#### Use Cases for EMIT CHANGES
**When to use**:
- ✅ Real-time dashboards (need live updates)
- ✅ Alerting systems (need immediate notification)
- ✅ Stream-to-stream joins downstream (need continuous input)
- ✅ Low-latency requirements (can't wait for window close)

**When NOT to use**:
- ❌ Batch analytics (standard emission sufficient)
- ❌ High cardinality GROUP BY (excessive output volume)
- ❌ Downstream can't handle high throughput (backpressure issues)

---

## Performance Baseline Confusion - Needs Resolution

### Conflicting Data Points

**From FR-082-overhead-analysis.md**:
> - **Tumbling Window + GROUP BY**: 127,000 rec/sec (high performance)
> - **Pure GROUP BY**: 10,000 rec/sec (13x slower)
>
> **Explanation**: The tumbling window benchmark uses a different query pattern with different aggregation complexity.

**From FR-082-PHASE4-BOTTLENECK_FINDINGS.md**:
> **Benchmark Result**: GROUP BY + 5 aggregations runs at **3.58K rec/s** (111x slower than 400K passthrough)

### Questions That Need Answers

1. **What is the actual pure GROUP BY baseline?**
   - 10K rec/sec (overhead-analysis)?
   - 3.58K rec/sec (PHASE4-BOTTLENECK_FINDINGS)?
   - Different test configurations?

2. **Why is TUMBLING + GROUP BY (127K) faster than pure GROUP BY (10K)?**
   - Should be impossible - windows ADD overhead
   - Possible causes:
     - Different number of aggregations
     - Different group cardinality
     - Measurement error

3. **Which scenario does Phase 4B/4C target?**
   - Pure GROUP BY only?
   - GROUP BY with WINDOW?
   - Both?

---

## Recommended Test Matrix

To resolve confusion, we need clear benchmarks for each scenario:

### Test 1: Pure GROUP BY (Simple)
```sql
SELECT category, COUNT(*) as count
FROM stream
GROUP BY category
```
- **Expected**: Baseline for GROUP BY performance
- **Dataset**: 100K records, 50 groups
- **Target after Phase 4B/4C**: Establish baseline

### Test 2: Pure GROUP BY (Complex - 5 Aggregations)
```sql
SELECT
    category,
    COUNT(*) as count,
    SUM(amount) as sum,
    AVG(price) as avg,
    MIN(price) as min,
    MAX(price) as max
FROM stream
GROUP BY category
```
- **Expected**: Should match 3.58K rec/sec baseline from PHASE4-BOTTLENECK_FINDINGS
- **Dataset**: 1M records, 50 groups
- **Target after Phase 4B/4C**: 200K rec/sec

### Test 3: ROWS WINDOW (No GROUP BY)
```sql
SELECT
    symbol,
    price,
    AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol) as moving_avg
FROM market_data
```
- **Expected**: High throughput (simple buffer operations)
- **Dataset**: 100K records, 10 partitions
- **Target**: Baseline measurement (Phase 4B/4C not applicable)

### Test 4: TUMBLING WINDOW + Simple GROUP BY
```sql
SELECT
    category,
    COUNT(*) as count
FROM stream
GROUP BY category
WINDOW TUMBLING (timestamp, INTERVAL '1' MINUTE)
```
- **Expected**: Slower than Test 1 (window overhead)
- **Dataset**: 100K records, 50 groups, 100 windows
- **Target after Phase 4B/4C**: Improved GROUP BY portion

### Test 5: TUMBLING WINDOW + Complex GROUP BY (Current Benchmark)
```sql
SELECT
    trader_id, symbol,
    COUNT(*), AVG(price), SUM(quantity), SUM(price * quantity)
FROM market_data
GROUP BY trader_id, symbol
WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
```
- **Expected**: Should match 127K rec/sec from tumbling_instrumented_profiling
- **Dataset**: 100K records
- **Target after Phase 4B/4C**: Improved (but need to understand why baseline is 127K)

---

## Impact on V2 Blueprint & Phase 0

### Current V2 Assumption
> "V2 architecture assumes 200K rec/sec GROUP BY throughput, but actual SQL engine baseline is 3.58K rec/sec"

### **Which GROUP BY scenario?**

**Best Guess (Needs Verification)**:
- Phase 4B/4C targets **Scenario 2: Pure GROUP BY** (no WINDOW)
- The 3.58K baseline is from **complex GROUP BY with 5 aggregations**
- The 127K TUMBLING + GROUP BY is a different configuration (fewer aggregations? different methodology?)

### Proposed Clarification for Part 0

**Phase 0 Goals (Revised)**:
1. **Measure baselines for all three scenarios**:
   - ROWS WINDOW (Scenario 1): Establish baseline
   - Pure GROUP BY simple (Scenario 2a): Measure with 1-2 aggregations
   - Pure GROUP BY complex (Scenario 2b): Confirm 3.58K with 5 aggregations
   - TUMBLING + GROUP BY (Scenario 3): Confirm 127K and understand why

2. **Apply Phase 4B optimization** (Week 1):
   - Target: Scenarios 2 & 3 (both use GROUP BY hash table)
   - Expected: 3.58K → 15-20K for complex GROUP BY
   - Measure impact on Scenario 3 (TUMBLING + GROUP BY)

3. **Apply Phase 4C optimization** (Week 2):
   - Target: Scenarios 2 & 3
   - Expected: 15-20K → 200K for complex GROUP BY
   - Verify Scenario 3 also improves

### Updated Success Criteria

**Phase 4B Success** (Week 1):
```bash
# Scenario 2: Pure GROUP BY (complex)
cargo test profile_pure_group_by_complex -- --nocapture
# Target: 3.58K → 15-20K rec/sec

# Scenario 3: TUMBLING + GROUP BY
cargo test profile_tumbling_group_by -- --nocapture
# Target: 127K → 200K+ rec/sec (or understand current 127K)
```

**Phase 4C Success** (Week 2):
```bash
# Scenario 2: Pure GROUP BY (complex)
cargo test profile_pure_group_by_complex -- --nocapture
# Target: 15-20K → 200K rec/sec ✅

# Scenario 3: TUMBLING + GROUP BY
cargo test profile_tumbling_group_by -- --nocapture
# Target: Improved GROUP BY portion
```

---

## Implications for V2 Architecture

### StateManagerActor Design

**Must support all three scenarios**:

```rust
struct StateManagerActor {
    receiver: mpsc::UnboundedReceiver<StateMessage>,
    states: HashMap<String, QueryState>,

    // Phase 4C optimizations
    string_interner: StringInterner,
    key_cache: LruCache<u64, GroupKey>,
}

struct QueryState {
    // Scenario 2 & 3: GROUP BY state
    group_by: Option<Arc<FxHashMap<GroupKey, GroupAccumulator>>>,  // Phase 4B/4C

    // Scenario 3 only: Window metadata
    persistent_windows: Vec<WindowState>,

    // Scenario 1 only: ROWS WINDOW buffers
    rows_window_buffers: HashMap<PartitionKey, VecDeque<Arc<StreamRecord>>>,
}
```

### Performance Targets (Revised)

**After Phase 0 (2 weeks)**:

| Scenario | Job Server V1 | SQL Engine | Phase 4B Target | Phase 4C Target | V2 Target |
|----------|---------------|------------|----------------|----------------|-----------|
| **1. ROWS WINDOW** | TBD | TBD | N/A | N/A | TBD |
| **2. Pure GROUP BY (simple)** | TBD | TBD | TBD | TBD | 500K rec/sec |
| **2. Pure GROUP BY (complex)** | 23.4K ✅ | 548K ✅ | 15-20K | 200K | 1.5M rec/sec |
| **3a. TUMBLING + GROUP BY (Standard)** | **23.6K** ✅ | **790K** ✅ | TBD | TBD | 1.5M rec/sec |
| **3b. TUMBLING + GROUP BY (EMIT CHANGES)** | **22.5K** ✅ | **~790K** | **TBD** | **TBD** | **1.4M rec/sec** |

**Notes**:
- ✅ Measured from job_server_overhead_breakdown and job_server_tumbling_* benchmarks
- Job Server adds ~97% overhead (33.5x slowdown) vs pure SQL engine
- EMIT CHANGES has only 4.6% overhead vs standard emission

**Critical Questions for Phase 0**:
1. Why is TUMBLING + GROUP BY (3a: 127K) faster than pure GROUP BY (10K)?
2. What is the correct baseline for pure GROUP BY?
3. Does Phase 4B/4C improve TUMBLING + GROUP BY (3a) from 127K, or is that already fast enough?
4. **NEW**: What is the performance overhead of EMIT CHANGES (3b) compared to standard emission (3a)?
5. **NEW**: Does Phase 4C Arc<GroupAccumulator> provide additional benefit for EMIT CHANGES change tracking?

---

## Recommendation

**Before starting Phase 0 implementation**:

1. **Run comprehensive baseline measurements**:
   ```bash
   # Measure all scenarios
   cargo test --tests --no-default-features --release -- --nocapture profile_rows_window
   cargo test --tests --no-default-features --release -- --nocapture profile_pure_group_by_direct_execution
   cargo test --tests --no-default-features --release -- --nocapture profile_tumbling_instrumented_standard_path
   ```

2. **Document actual baselines** in a new document:
   - **FR-082-BASELINE-MEASUREMENTS.md**
   - Clear methodology for each scenario
   - Explanation of differences

3. **Update Phase 0 goals** based on actual measurements:
   - If TUMBLING + GROUP BY is already 127K, maybe Phase 4B/4C targets pure GROUP BY only?
   - Or Phase 4B/4C improves both but with different multipliers?
   - Set realistic targets based on data

4. **Update V2 blueprint Part 0** with scenario-specific targets:
   - Don't assume all GROUP BY scenarios are 3.58K baseline
   - Clarify which scenario V2 architecture depends on

---

## Summary

**Four Distinct Scenarios**:
1. **ROWS WINDOW**: No GROUP BY, memory-bounded buffers, Phase 4B/4C not applicable
2. **Pure GROUP BY**: No WINDOW, hash table aggregation, PRIMARY target for Phase 4B/4C
3a. **GROUP BY + WINDOW (Standard)**: Batch emission on window close, Phase 4B/4C applicable to GROUP BY portion
3b. **GROUP BY + WINDOW + EMIT CHANGES**: Continuous emission on every update, Phase 4B/4C applicable + additional change tracking overhead

**Performance Characteristics (Measured)**:
- **Scenario 1 (ROWS WINDOW)**: TBD (needs baseline)
- **Scenario 2 (Pure GROUP BY)**:
  - Job Server V1: 23.4K rec/sec ✅
  - SQL Engine: 548K rec/sec ✅
  - Job Server overhead: 95.8% (23.4x slowdown)
- **Scenario 3a (TUMBLING + GROUP BY Standard)**:
  - Job Server V1: 23.6K rec/sec ✅
  - SQL Engine: 790K rec/sec ✅
  - Job Server overhead: 97.0% (33.5x slowdown)
- **Scenario 3b (TUMBLING + GROUP BY EMIT CHANGES)**:
  - Job Server V1: 22.5K rec/sec ✅
  - SQL Engine: ~790K rec/sec
  - vs Standard: Only 4.6% slower ✅ (much better than estimated 20-40%!)

**Key Findings**:
- ✅ Job Server coordination adds ~97% overhead (bottleneck is coordination/locking/metrics, not SQL engine)
- ✅ EMIT CHANGES overhead is minimal (4.6%) - excellent for real-time use cases
- ✅ SQL engine performs well (548-790K rec/sec) - Phase 0 should focus on job server coordination
- ✅ TUMBLING + GROUP BY (3a/3b) slightly faster than pure GROUP BY (2) due to batch processing

**Remaining Action Items**:
- Measure ROWS WINDOW (Scenario 1) baseline
- Run Phase 4B/4C optimizations to reduce job server overhead
- Implement V2 hash-partitioned architecture for linear scaling

---

**Document Created By**: Claude Code
**Date**: November 6, 2025
**Status**: Requires baseline measurement validation before Phase 0 kickoff
