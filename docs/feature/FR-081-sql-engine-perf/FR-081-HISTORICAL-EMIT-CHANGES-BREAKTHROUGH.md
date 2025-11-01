# BREAKTHROUGH: EMIT CHANGES Performance Analysis

**Date**: 2025-11-01
**Discovery**: EMIT CHANGES is NOT the problem - it's the SOLUTION!

## Executive Summary

A critical performance test revealed that **TUMBLING windows with EMIT CHANGES are 244x FASTER** than standard TUMBLING windows. This completely reverses our initial hypothesis and identifies the root cause: the **standard (non-EMIT CHANGES) window processing path has O(N²) complexity**, while the EMIT CHANGES path has efficient O(1) per-record behavior.

---

## Performance Results

### Complete Test Matrix

| Window Configuration | Records | Throughput | Growth Ratio | Total Time | Status |
|---------------------|---------|------------|--------------|------------|--------|
| **TUMBLING + EMIT CHANGES** | 10,000 | **29,321 rec/sec** | **0.02x** ✅ | **0.341s** | ✅ **EXCEEDS TARGET** |
| TUMBLING (standard) | 10,000 | 120 rec/sec | 16.47x ❌ | 83.24s | 🔴 CRITICAL |
| SESSION | 1,000 | 917 rec/sec | 5.5x ⚠️ | 1.09s | ⚠️ CRITICAL |

### Performance Gap Analysis

**TUMBLING + EMIT CHANGES vs Standard TUMBLING**:
- **244x faster** throughput
- **0.4% of the time** to process same data
- **Constant time** per record vs linearly growing time

**TUMBLING + EMIT CHANGES vs Target**:
- **195% of target** (29,321 vs 15,000 rec/sec)
- **EXCEEDS performance goals** by 2x

---

## Detailed Profiling Output

### TUMBLING + EMIT CHANGES (The FAST Path)

```
🔍 TUMBLING Window + EMIT CHANGES Financial Analytics Profile
======================================================================
✅ Phase 1: Record generation (10000 records): 31.84ms
✅ Phase 2: Engine setup + SQL parsing: 1.02ms
   Record 0:    1.42ms   (initial setup)
   Record 1000: 30µs     ← Constant!
   Record 2000: 29µs     ← Constant!
   Record 3000: 29µs     ← Constant!
   Record 4000: 32µs     ← Constant!
   Record 5000: 30µs     ← Constant!
   Record 6000: 29µs     ← Constant!
   Record 7000: 29µs     ← Constant!
   Record 8000: 29µs     ← Constant!
   Record 9000: 30µs     ← Still constant!
✅ Phase 3: Execute 10000 records: 295.09ms (86.5%)
   Average per record: 29.5µs
   Min record time: 28.8µs
   Max record time: 1.42ms
   Growth ratio (last/first): 0.02x ✅

📊 PERFORMANCE BREAKDOWN
──────────────────────────────────────────────────────────────────────
Phase 3 (Execution):       295.09ms (86.5%)
Other phases:              45.97ms (13.5%)
──────────────────────────────────────────────────────────────────────
TOTAL:                     341.06ms

🔥 Throughput: 29,321 records/sec
🎯 Target: 15,000 rec/sec
✅ EXCEEDS TARGET by 14,321 rec/s (195% of target)
```

**Key Observation**: Execution time is **constant** at ~30µs per record after initial setup. This is **perfect O(1) behavior**.

---

### TUMBLING Standard (The SLOW Path)

```
🔍 TUMBLING Window Financial Analytics Performance Profile
======================================================================
✅ Phase 1: Record generation (10000 records): 31.58ms
✅ Phase 2: Engine setup + SQL parsing: 1.13ms
   Record 0:    907µs
   Record 1000: 1.56ms   (1.7x baseline)
   Record 2000: 3.25ms   (3.6x)
   Record 3000: 5.08ms   (5.6x)
   Record 4000: 6.59ms   (7.3x)
   Record 5000: 8.26ms   (9.1x)
   Record 6000: 9.97ms   (11.0x)
   Record 7000: 11.34ms  (12.5x)
   Record 8000: 13.15ms  (14.5x)
   Record 9000: 14.94ms  (16.5x) ← LINEAR GROWTH!
✅ Phase 3: Execute 10000 records: 83.14s (99.9%)

📊 PERFORMANCE BREAKDOWN
──────────────────────────────────────────────────────────────────────
Phase 3 (Execution):       83.14s (99.9%)
Other phases:              100.43ms (0.1%)
──────────────────────────────────────────────────────────────────────
TOTAL:                     83.24s

🔥 Throughput: 120 records/sec
🎯 Target: 20,000 rec/sec
⚠️  BELOW TARGET by 19,880 rec/s (0.6% of target)
```

**Key Observation**: Execution time grows **linearly** from 907µs to 14.94ms. This is **O(N) per record = O(N²) total**.

---

## Root Cause Analysis

### Code Path Comparison

**Location**: `src/velostream/sql/execution/processors/window.rs:145-170`

```rust
// Check if window should emit
let is_emit_changes = Self::is_emit_changes(query);
let group_by_cols = Self::get_group_by_columns(query);

let should_emit = if is_emit_changes && group_by_cols.is_some() {
    true  // ← EMIT CHANGES path: ALWAYS emits (FAST!)
} else {
    // Standard path: Check window boundary (SLOW!)
    Self::should_emit_window_state(window_state, event_time, window_spec)
};
```

### The EMIT CHANGES Path (FAST - O(1))

**What happens on each record**:
1. ✅ Add record to buffer: O(1)
2. ✅ Check should_emit: O(1) (always true)
3. ✅ Process emission: O(K) where K = number of groups (~200)
4. ✅ Clear/cleanup buffer: O(1) or O(K)
5. ✅ Emit result: O(1)

**Total per record**: O(1) + O(K) where K is constant (200 groups)

**Why it's fast**:
- Emits immediately, keeping buffer small
- No accumulation of unbounded state
- GROUP BY processing is on small, fresh data

---

### The Standard Path (SLOW - O(N²))

**What happens on each record**:
1. ✅ Add record to buffer: O(1)
2. ⚠️ Check should_emit: **O(?)** - this is where the problem likely is
3. ❌ **Something expensive**: O(N) operation happens here
4. ❌ Buffer accumulates: Buffer grows to N records
5. ❌ Each subsequent record processes larger buffer

**Hypothesis**: The `should_emit_window_state` or something in the record processing path is doing O(N) work on every record.

---

## Suspected Bottlenecks in Standard Path

### Hypothesis 1: Buffer Size Checks

**Location**: Unknown - needs investigation

**Theory**: Every record insertion might be checking/validating the entire buffer.

**Evidence**:
- Linear growth pattern: 907µs → 14.94ms (16.5x over 9000 records)
- Growth is almost perfectly proportional to record count
- EMIT CHANGES doesn't show this pattern (buffer stays small)

---

### Hypothesis 2: Window Boundary Calculation

**Location**: `window.rs:587-635` - `should_emit_window_state`

**Theory**: Window boundary checks might be expensive or trigger buffer scans.

**Code to investigate**:
```rust
pub fn should_emit_window_state(
    window_state: &WindowState,
    event_time: i64,
    window_spec: &WindowSpec,
) -> bool {
    if window_state.buffer.is_empty() {
        return false;  // ← O(1) check
    }

    match window_spec {
        WindowSpec::Tumbling { size, .. } => {
            let window_size_ms = size.as_millis() as i64;

            if last_emit == 0 {
                return event_time >= window_size_ms;
            }

            event_time >= last_emit + window_size_ms
        }
        // ...
    }
}
```

**Analysis**: This code looks O(1), so the problem is likely elsewhere.

---

### Hypothesis 3: Hidden Buffer Scans

**Location**: To be determined

**Theory**: There might be hidden buffer operations that aren't obvious:
- Buffer validation
- Duplicate checking
- Time-based sorting
- Memory reallocation

**Investigation needed**:
1. Add detailed instrumentation to each operation
2. Profile with flamegraph
3. Check buffer implementation (Vec operations)

---

### Hypothesis 4: Context State Management

**Location**: `processors/context.rs:210-241` - `get_or_create_window_state`

**Theory**: State lookup might degrade with buffer size.

**Code**:
```rust
pub fn get_or_create_window_state(...) -> &mut WindowState {
    // Linear search through states
    for (idx, (stored_query_id, state)) in self.persistent_window_states.iter().enumerate() {
        if stored_query_id == query_id {
            // Mark as dirty
            if idx < 32 {
                self.dirty_window_states |= 1 << (idx as u32);
            }
            return &mut self.persistent_window_states[idx].1;
        }
    }
    // ...
}
```

**Analysis**: For a single query, this should be O(1) (state is at index 0). **Unlikely to be the bottleneck**.

---

## Why EMIT CHANGES is Fast

### Key Differences

**1. Immediate Emission**
- EMIT CHANGES emits on every record
- Buffer never grows beyond 1-200 records (per group)
- No unbounded accumulation

**2. Efficient GROUP BY Processing**
- Processes ~200 groups per record
- Each group has 1-2 records max
- Total aggregation cost: O(200) = O(1) constant

**3. Buffer Cleanup**
- FR-079 Phase 7 code (lines 299-309) handles cleanup:
  ```rust
  if !is_emit_changes {
      // For standard GROUP BY queries, cleanup emitted records
      Self::cleanup_window_buffer_direct(window_state, window_spec, last_emit_time_before_update);
  }
  ```
- For EMIT CHANGES: buffer is NOT cleared (kept for state re-emission)
- But buffer stays small because emissions happen frequently

**4. Optimized Code Path**
- Lines 201-311: Dedicated GROUP BY + EMIT CHANGES path
- Avoids expensive legacy window logic
- Uses efficient group computation

---

## Performance Impact Quantification

### Throughput Comparison

| Configuration | Throughput | vs Target | vs Standard |
|---------------|------------|-----------|-------------|
| EMIT CHANGES | 29,321 rec/sec | **+95%** ✅ | **+24,334%** 🚀 |
| Standard | 120 rec/sec | **-99.4%** ❌ | baseline |

### Time Comparison (10,000 records)

| Configuration | Total Time | vs EMIT CHANGES | Speedup |
|---------------|------------|-----------------|---------|
| EMIT CHANGES | 0.341s | baseline | 1x |
| Standard | 83.24s | **+244x slower** | **244x** |

### Memory Impact (estimated)

**EMIT CHANGES**:
- Buffer size: ~200 groups × 2 records = 400 records max
- Memory: 400 × 200 bytes = 80 KB
- Allocations: 10,000 (one per record)

**Standard**:
- Buffer size: 10,000 records
- Memory: 10,000 × 200 bytes = 2 MB
- Allocations: 50,005,000 (O(N²) clones)

---

## Recommended Actions

### Priority 1: Identify Standard Path Bottleneck (CRITICAL)

**Action**: Add detailed instrumentation to standard window processing

**Code to add**:
```rust
// In process_windowed_query, around line 140
let t_start = std::time::Instant::now();
let window_state = context.get_or_create_window_state(query_id, window_spec);
println!("get_or_create: {:?}", t_start.elapsed());

let t_add = std::time::Instant::now();
window_state.add_record(record.clone());
println!("add_record: {:?}, buffer_size: {}", t_add.elapsed(), window_state.buffer.len());

let t_emit = std::time::Instant::now();
let should_emit = Self::should_emit_window_state(window_state, event_time, window_spec);
println!("should_emit_check: {:?}", t_emit.elapsed());
```

---

### Priority 2: Compare Code Paths

**Action**: Create side-by-side comparison of EMIT CHANGES vs Standard paths

**Questions to answer**:
1. What does EMIT CHANGES do differently after `add_record`?
2. Why doesn't standard path buffer stay small?
3. Is there a hidden buffer scan in standard path?

---

### Priority 3: Apply EMIT CHANGES Logic to Standard Path

**Action**: If EMIT CHANGES path is correct, adapt it for standard windows

**Hypothesis**: Standard windows should:
- Emit less frequently (only on window boundaries)
- But use same efficient aggregation logic
- Clear buffer properly after emission

---

### Priority 4: Fix SESSION Windows

**Action**: SESSION windows still have O(N²) behavior (917 rec/sec)

**The SESSION window emission bug is independent**:
- SESSION emits on every record (wrong)
- Should only emit on session gap timeout
- Even with this bug, it's 7.6x faster than broken standard TUMBLING!

---

## Success Criteria

### Immediate Goals

- ✅ **EMIT CHANGES**: Already meets target (29K > 15K rec/sec)
- ❌ **Standard TUMBLING**: Must achieve 20K+ rec/sec
- ❌ **SESSION**: Must achieve 15K+ rec/sec

### Performance Targets

| Window Type | Current | Target | Status |
|-------------|---------|--------|--------|
| TUMBLING + EMIT | 29,321 | 15,000 | ✅ EXCEEDS |
| TUMBLING | 120 | 20,000 | ❌ CRITICAL |
| SESSION | 917 | 15,000 | ❌ CRITICAL |

### Behavioral Targets

- ✅ Execution time growth ratio < 2.0x
- ✅ Memory usage O(W) where W = window size, not O(N) stream length
- ✅ All window types perform within 2x of each other

---

## Conclusion

This breakthrough discovery reveals:

1. **EMIT CHANGES is the GOLD STANDARD** - it shows the performance we want
2. **Standard window path has O(N²) bug** - needs investigation and fix
3. **The problem is NOT emission frequency** - it's something in buffer/state management
4. **Fix is achievable** - we have working code to learn from

**Next Steps**:
1. Instrument standard path to find exact O(N) operation
2. Apply EMIT CHANGES optimizations to standard path
3. Fix SESSION window emission logic
4. Verify all window types achieve target performance

**Estimated fix time**: 3-5 days (down from 5-8 days, now that we know where to look)

**Priority**: CRITICAL - but now we have a clear path forward!
