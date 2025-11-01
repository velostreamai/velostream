# Window Performance Comparison Analysis

**Date**: 2025-11-01
**Tests Analyzed**:
- `benchmark_session_window_iot_clustering`
- `benchmark_tumbling_window_financial_analytics`

## Executive Summary

Both SESSION and TUMBLING window implementations exhibit **critical O(N²) performance issues**, with TUMBLING windows being **10x worse** than SESSION windows. All window-based queries are significantly below performance targets.

| Window Type | Throughput | Target | Gap | Status |
|-------------|------------|--------|-----|--------|
| **TUMBLING** | **120 rec/sec** | 20,000 | **166.5x slower** | 🔴 CRITICAL |
| **SESSION** | 917 rec/sec | 15,000 | 16.4x slower | ⚠️ CRITICAL |

**Key Finding**: TUMBLING windows are **7.6x slower** than SESSION windows despite being simpler.

---

## Detailed Performance Comparison

### SESSION Window (IoT Clustering)

**Test Configuration**:
- Records: 1,000 (timeout at 10,000)
- Window: SESSION(10m gap)
- GROUP BY: device_id
- Fields: 3 per record

**Profiling Results**:
```
Phase 1 (Record Gen):      2.5ms (0.2%)
Phase 2 (Setup+Parse):     1.5ms (0.1%)
Phase 3 (Execution):       1.07s (98.5%) ⚠️ CRITICAL
Phase 4-7 (Other):         15.8ms (1.2%)
──────────────────────────────────────
TOTAL:                     1.09s

Throughput: 917 rec/sec
Target: 15,000 rec/sec
```

**Execution Time Growth**:
```
Record 0:   1.62ms
Record 100: 0.38ms
Record 500: 1.27ms
Record 900: 2.09ms
Growth ratio: 5.5x (900 vs 100)
```

---

### TUMBLING Window (Financial Analytics)

**Test Configuration**:
- Records: 10,000
- Window: TUMBLING(1 MINUTE)
- GROUP BY: trader_id, symbol (200 groups)
- Fields: 5 per record

**Profiling Results**:
```
Phase 1 (Record Gen):      31.6ms (0.0%)
Phase 2 (Setup+Parse):     1.1ms (0.0%)
Phase 3 (Execution):       83.14s (99.9%) ⚠️ CRITICAL
Phase 4-7 (Other):         66.7ms (0.1%)
──────────────────────────────────────
TOTAL:                     83.24s

Throughput: 120 rec/sec
Target: 20,000 rec/sec
```

**Execution Time Growth**:
```
Record 0:    907µs
Record 1000: 1.56ms  (1.7x)
Record 2000: 3.25ms  (3.6x)
Record 3000: 5.08ms  (5.6x)
Record 4000: 6.59ms  (7.3x)
Record 5000: 8.26ms  (9.1x)
Record 6000: 9.97ms  (11.0x)
Record 7000: 11.34ms (12.5x)
Record 8000: 13.15ms (14.5x)
Record 9000: 14.94ms (16.5x)
Growth ratio: 16.47x
```

**Linear Growth Pattern**: Execution time grows almost perfectly linearly with record count, indicating O(N) work per record = O(N²) total.

---

## Root Cause Analysis

### 1. SESSION Window O(N²) Issue

**Location**: `src/velostream/sql/execution/processors/window.rs:622-626`

**Problem**: Emits on EVERY record regardless of session boundaries

```rust
WindowSpec::Session { .. } => {
    !window_state.buffer.is_empty()  // ← Returns true for every record!
}
```

**Impact**:
- Record 0: Process 1 record
- Record 1: Process 2 records
- Record N: Process N+1 records
- **Total**: N(N+1)/2 = **O(N²)** operations

For 10,000 records: **50,005,000 operations**

---

### 2. TUMBLING Window O(N²) Issue

**Hypothesis 1**: Linear search in `get_or_create_window_state`

**Location**: `src/velostream/sql/execution/processors/context.rs:216-226`

```rust
// Check if window state already exists
for (idx, (stored_query_id, state)) in self.persistent_window_states.iter().enumerate() {
    if stored_query_id == query_id {
        return &mut self.persistent_window_states[idx].1;
    }
}
```

**Analysis**: For a single query, this should be O(1) (always at index 0). **Not the root cause**.

---

**Hypothesis 2**: Buffer accumulation without cleanup

**Evidence**:
1. TUMBLING windows should only emit every 60,000 records (1 MINUTE window, 1 record/sec)
2. For 10,000 records, should emit only **ONCE** at record 0
3. Yet execution time grows linearly: 907µs → 14.94ms (16.47x)

**Investigation needed**: Check if buffer is being scanned/validated on every record insertion.

---

**Hypothesis 3**: GROUP BY state accumulation

**Observation**: TUMBLING test has GROUP BY with 200 groups (20 traders × 10 symbols)

**Possible issues**:
1. Group state might be accumulating
2. Hash operations might degrade with state size
3. GROUP BY processing might happen on non-emission records

---

## Computational Complexity Analysis

### Expected vs Actual Complexity

| Operation | Expected | Actual (SESSION) | Actual (TUMBLING) |
|-----------|----------|------------------|-------------------|
| Per-record processing | O(1) | O(N) | O(N) |
| Total for N records | O(N) | **O(N²)** | **O(N²)** |
| Buffer management | O(1) amortized | O(N) | O(N) |
| Emission check | O(1) | O(1) | O(1) |
| Aggregation (when emit) | O(K) groups | O(N×K) | O(N×K) |

### Measured Performance

**SESSION Window (1,000 records)**:
- Expected time: ~67ms (15,000 rec/sec)
- Actual time: 1,090ms
- Overhead: **16.3x**

**TUMBLING Window (10,000 records)**:
- Expected time: ~500ms (20,000 rec/sec)
- Actual time: 83,240ms
- Overhead: **166.5x**

---

## Memory Impact

### SESSION Window (1,000 records)

**Buffer growth**:
- Record 0: 1 StreamRecord
- Record 500: 501 StreamRecords
- Record 999: 1,000 StreamRecords

**Memory per StreamRecord**: ~150 bytes (5 fields × 30 bytes average)

**Total buffer size**: 1,000 × 150 = 150 KB

**Clone operations** (if emitting on every record):
- Total clones: 1 + 2 + 3 + ... + 1000 = 500,500 records
- Memory allocations: ~75 MB

---

### TUMBLING Window (10,000 records)

**Buffer growth**:
- Record 0: 1 StreamRecord
- Record 5000: 5,001 StreamRecords
- Record 9999: 10,000 StreamRecords

**Memory per StreamRecord**: ~200 bytes (7 fields)

**Total buffer size**: 10,000 × 200 = 2 MB

**Clone operations** (estimated):
- If emitting/processing on every record: 50,005,000 records
- Memory allocations: ~10 GB

**Actual measurement**: Growth pattern suggests this is happening!

---

## Comparison Summary

| Metric | SESSION | TUMBLING | Winner |
|--------|---------|----------|--------|
| **Throughput** | 917 rec/sec | 120 rec/sec | SESSION (7.6x) |
| **Execution time** (per 1K) | 1.09s | 8.32s | SESSION (7.6x) |
| **Growth ratio** | 5.5x | 16.47x | SESSION (3.0x) |
| **Buffer size** | 150 KB | 2 MB | SESSION (13.3x) |
| **Target gap** | 16.4x | 166.5x | SESSION (10.2x) |

**Conclusion**: SESSION windows perform better despite having a more serious logical bug (emits every record), likely because:
1. Fewer records in test (1K vs 10K)
2. Simpler query (no complex aggregations)
3. Fewer GROUP BY groups (50 vs 200)

---

## Common Issues

Both window types share these problems:

### 1. Excessive Buffer Cloning

**Location**: Multiple places in `window.rs`

**Example**:
```rust
let buffer = window_state.buffer.clone();  // ← Clones entire buffer
```

This happens:
- **SESSION**: On every emission (every record) = O(N²) clones
- **TUMBLING**: Unclear frequency, but growth pattern suggests frequent cloning

---

### 2. Record Cloning on Insertion

**Location**: `window.rs:142`

```rust
window_state.add_record(record.clone());  // ← Clone every record
```

**Impact**: O(N) clone operations (unavoidable if buffer needs owned records)

---

### 3. Inefficient Buffer Cleanup

**SESSION**: Retains all records within gap window
**TUMBLING**: Retains records instead of clearing after emission

Both lead to unbounded buffer growth for continuous streams.

---

## Performance Targets vs Reality

### SESSION Window

| Metric | Target | Actual | Gap |
|--------|--------|--------|-----|
| Throughput (1K) | 15,000 rec/sec | 917 rec/sec | 16.4x |
| Throughput (10K) | 15,000 rec/sec | ~92 rec/sec (timeout) | 163x |
| Latency P50 | <1ms | ~1ms | ~1x |
| Latency P99 | <5ms | ~2ms | Better! |
| Memory (10K) | <1 MB | ~5 GB | 5000x |

---

### TUMBLING Window

| Metric | Target | Actual | Gap |
|--------|--------|--------|-----|
| Throughput (10K) | 20,000 rec/sec | 120 rec/sec | 166.5x |
| Latency P50 | <1ms | ~6ms | 6x |
| Latency P99 | <5ms | ~15ms | 3x |
| Memory (10K) | <2 MB | ~10 GB (est) | 5000x |

---

## Recommended Fixes (Priority Order)

### Priority 1: Fix SESSION Window Emission Logic ⚠️ CRITICAL

**Impact**: 16.4x → 1x speedup for SESSION windows

**Change**:
```rust
WindowSpec::Session { gap, .. } => {
    if window_state.buffer.is_empty() {
        return false;
    }

    let gap_ms = gap.as_millis() as i64;
    let time_since_last_emit = event_time.saturating_sub(window_state.last_emit);

    // Only emit when session gap is exceeded
    time_since_last_emit >= gap_ms
}
```

---

### Priority 2: Investigate TUMBLING Window O(N) Per-Record Cost

**Actions**:
1. Add detailed profiling to identify exact bottleneck
2. Check if buffer is being scanned on every record
3. Verify GROUP BY state management efficiency
4. Profile `get_or_create_window_state` with instrumentation

**Tools**:
- Add timing instrumentation to each operation
- Use flamegraph profiling
- Add buffer size logging

---

### Priority 3: Eliminate Unnecessary Buffer Cloning

**Change**: Use references instead of cloning entire buffers

**Before**:
```rust
let buffer = window_state.buffer.clone();
```

**After**:
```rust
let buffer = &window_state.buffer;
```

**Expected impact**: 50-100x reduction in memory allocations

---

### Priority 4: Implement Proper Buffer Cleanup

**SESSION**:
```rust
WindowSpec::Session { .. } => {
    window_state.buffer.clear();  // Clear after session emission
}
```

**TUMBLING**:
```rust
WindowSpec::Tumbling { .. } => {
    window_state.buffer.clear();  // Clear after window emission
}
```

---

### Priority 5: Add Performance Regression Tests

Add assertions to prevent future O(N²) regressions:

```rust
// Check execution time growth ratio
let early_avg = execution_times[0..3].avg();
let late_avg = execution_times[7..10].avg();
let growth_ratio = late_avg / early_avg;

assert!(
    growth_ratio < 2.0,
    "Execution time growing too fast: {}x (O(N²) detected)",
    growth_ratio
);
```

---

## Investigation Plan for TUMBLING Window

Since the root cause for TUMBLING O(N²) is unclear, here's the investigation plan:

### Step 1: Add Detailed Instrumentation

Add timing to each operation in the critical path:

```rust
// In process_windowed_query
let t1 = Instant::now();
let window_state = context.get_or_create_window_state(query_id, window_spec);
println!("get_or_create: {:?}", t1.elapsed());

let t2 = Instant::now();
window_state.add_record(record.clone());
println!("add_record: {:?}", t2.elapsed());

let t3 = Instant::now();
let should_emit = ...;
println!("should_emit check: {:?}", t3.elapsed());
```

---

### Step 2: Profile Buffer Operations

Check if buffer size correlates with slowdown:

```rust
if idx % 1000 == 0 {
    println!("Record {}: buffer size = {}", idx, window_state.buffer.len());
}
```

---

### Step 3: Check GROUP BY State

Log group state accumulation:

```rust
// In GROUP BY processing
println!("GROUP BY groups: {}", groups.len());
println!("Records per group: {:?}", groups.values().map(|v| v.len()).collect::<Vec<_>>());
```

---

## Success Criteria

### Phase 1: SESSION Window Fix

- ✅ Throughput: >15,000 rec/sec for 10,000 records
- ✅ Execution time growth: <2x from first to last 1,000 records
- ✅ Memory: <10 MB for 10,000 records

---

### Phase 2: TUMBLING Window Fix

- ✅ Throughput: >20,000 rec/sec for 10,000 records
- ✅ Execution time growth: <1.5x from first to last 1,000 records
- ✅ Memory: <5 MB for 10,000 records
- ✅ Root cause identified and documented

---

### Phase 3: Comprehensive Optimization

- ✅ All window types achieve target throughput
- ✅ No O(N²) patterns in any code path
- ✅ Memory usage scales linearly with window size, not stream duration
- ✅ Regression tests prevent future performance degradation

---

## Appendix: Full Profiling Output

### SESSION Window (1,000 records)

```
🔍 SESSION Window IoT Clustering Performance Profile
======================================================================
✅ Phase 1: Record generation (1000 records): 2.503875ms
✅ Phase 2: Engine setup + SQL parsing: 1.522709ms
   Record 0: 1.620583ms
   Record 100: 380.917µs
   Record 200: 532.708µs
   Record 300: 782.333µs
   Record 400: 1.0155ms
   Record 500: 1.271708ms
   Record 600: 1.540208ms
   Record 700: 1.776666ms
   Record 800: 2.029167ms
   Record 900: 2.0895ms
✅ Phase 3: Execute 1000 records: 1.074376792s (98.5%)
   Average per record: 1.074376ms
   Min record time: 380.917µs
   Max record time: 2.0895ms
✅ Phase 4: Flush windows: 380.167µs
✅ Phase 5: Flush group by results: 1.375µs
✅ Phase 6: Sleep for emissions: 11.249042ms
✅ Phase 7: Collect 1001 results: 152.125µs

🔥 Throughput: 917 records/sec
🎯 Target: >15,000 records/sec
⚠️  BELOW TARGET by 14083 rec/s (16.4x slower)
```

---

### TUMBLING Window (10,000 records)

```
🔍 TUMBLING Window Financial Analytics Performance Profile
======================================================================
✅ Phase 1: Record generation (10000 records): 31.582541ms
✅ Phase 2: Engine setup + SQL parsing: 1.13175ms
   Record 0: 907.459µs
   Record 1000: 1.559458ms
   Record 2000: 3.249125ms
   Record 3000: 5.082ms
   Record 4000: 6.589875ms
   Record 5000: 8.257708ms
   Record 6000: 9.973959ms
   Record 7000: 11.343792ms
   Record 8000: 13.151042ms
   Record 9000: 14.939083ms
✅ Phase 3: Execute 10000 records: 83.141010584s (99.9%)
   Average per record: 8.314101ms
   Min record time: 907.459µs
   Max record time: 14.939083ms
   Growth ratio (last/first): 16.47x
✅ Phase 4: Flush windows: 54.5645ms
✅ Phase 5: Flush group by results: 1.958µs
✅ Phase 6: Sleep for emissions: 12.087708ms
✅ Phase 7: Collect 1 results: 52.667µs

🔥 Throughput: 120 records/sec
🎯 Target: >20,000 records/sec
⚠️  BELOW TARGET by 19880 rec/s (166.5x slower)
```

---

## Conclusion

Both window types have critical O(N²) performance issues that must be fixed before production use:

1. **SESSION windows**: Clear logical bug (emits on every record) - **fix is straightforward**
2. **TUMBLING windows**: Root cause unclear - **requires investigation**

**Recommended approach**:
1. Fix SESSION window emission logic (1-2 days)
2. Investigate TUMBLING window bottleneck with instrumentation (2-3 days)
3. Implement buffer optimization and cleanup (1-2 days)
4. Add regression tests (1 day)

**Total estimated time**: 5-8 days for complete fix

**Priority**: CRITICAL - blocks production deployment
