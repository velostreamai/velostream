# Velostream Development TODO

## 🎯 Current Session: Window Adapter Performance Analysis

### Issues Being Addressed

#### Issue #1: 60x Performance Slowdown with Partition-Batched Data
- **Symptom**: Scenario 4 (GROUP BY + TUMBLING WINDOW) slows from 0.15s to 9.01s with partition batching
- **Impact**: Throughput drops from 66,238 rec/sec to 1,110 rec/sec
- **Root Cause**: Records grouped by partition break GROUP BY aggregation ordering assumptions
- **Status**: ROOT CAUSE IDENTIFIED, FIX PENDING
- **File**: `src/velostream/sql/execution/window_v2/adapter.rs` - `compute_aggregations_over_window()`

#### Issue #2: 90% Data Loss with Partition-Batched Records
- **Symptom**: Only 1,000 results produced instead of 9,980 (10% of expected)
- **Impact**: Correctness bug - results silently dropped
- **Root Cause**: Late partitions' groups arrive after window clears, never get processed
- **Status**: ROOT CAUSE IDENTIFIED, FIX PENDING
- **Evidence**:
  - Window emissions: 166-167 (correct) ✅
  - Buffer sizes: ~60 records (correct) ✅
  - Results per emission: 6.0 instead of 60.1 (WRONG) ❌

#### Issue #3: Progressive Performance Degradation
- **Symptom**: With partition batching, throughput decreases as processing continues
  - 2K records: 2,453 rec/sec
  - 8K records: 1,182 rec/sec (52% slower)
- **Root Cause**: Overhead compounds as more data accumulates with skewed group distribution
- **Status**: SECONDARY TO ISSUES #1 AND #2

---

## Current Session: Window Adapter Performance Analysis

### ✅ Completed

- [x] Investigate Scenario 4 performance degradation with partition-batched data
- [x] Implement WindowMetrics instrumentation in TumblingWindowStrategy
  - Added `clear_calls`, `total_buffer_sizes_at_clear`, `max_buffer_size`, `add_record_calls`
  - Instrumented `add_record()` and `clear()` methods
  - File: `src/velostream/sql/execution/window_v2/strategies/tumbling.rs`

- [x] Create linear vs partition batching comparison tests
  - `profile_adapter_execution_linear_10k()` - Linear batching test
  - `profile_adapter_execution_10k()` - Partition batching test
  - File: `tests/unit/server/processors/window_adapter_instrumentation_test.rs`

- [x] Run tests and measure key metrics
  - Emission frequency: Both systems emit ~167 times ✅
  - Buffer size: Both maintain ~60 records at emission ✅
  - Iteration count: Linear 9,980 results ✅ vs Partition 1,000 results ❌

- [x] Document findings in WINDOW_ADAPTER_ANALYSIS.md
  - File: `docs/performance/WINDOW_ADAPTER_ANALYSIS.md`
  - Complete analysis of root cause and measurements

- [x] Analyze root cause: GROUP BY aggregation with partition-batched records
  - Root cause: Records grouped by partition → skewed group distribution
  - Late partitions' groups miss window boundaries
  - Only ~2% of unique groups (100/5000) emit results

### ⏳ Pending

- [ ] **Add Watermark Tracking to TumblingWindowStrategy** (CRITICAL)
  - Add `max_watermark_seen: i64` field to track highest timestamp seen
  - Add `allowed_lateness_ms: i64` configuration field
  - Add `historical_windows: HashMap<i64, WindowState>` for late firings
  - This is the REFERENCE POINT needed to distinguish late vs on-time data

- [ ] **Implement Watermark Advancement Logic**
  - Update watermark on each record: `max_watermark_seen = max(max_watermark_seen, ts)`
  - Use watermark to classify records:
    - On-time: ts > current_window_end
    - Late: ts <= current_window_end && ts > current_window_end - allowed_lateness
    - Too late: ts <= current_window_end - allowed_lateness

- [ ] **Implement Allowed Lateness with State Retention**
  - Keep window state alive while: `watermark < window_end + allowed_lateness`
  - Delete window state when: `watermark >= window_end + allowed_lateness`
  - Bounded memory: max concurrent windows = (allowed_lateness + window_size) / window_size

- [ ] **Implement Late Firing Mechanism**
  - When late record arrives in "wrong" window:
    - Retrieve historical window state (if still alive)
    - Add record to that window's group accumulator
    - Re-compute aggregations
    - **EMIT UPDATED RESULT** (mark as "late firing")
  - Emit deduplicated results at sink (multiple emissions expected)

- [ ] **Verify fixes resolve data loss and slowdown**
  - Run partition batching test with fixes
  - Confirm results improve from 1,000 to ~9,980
  - Confirm throughput improves from 1,110 to 66,238 rec/sec

- [ ] **Run pre-commit checks**
  - `cargo fmt --all -- --check`
  - `cargo check --all-targets --no-default-features`
  - `cargo clippy --all-targets --no-default-features`
  - `cargo test --lib --no-default-features`

## Key Findings Summary

### Problem
- **60x slowdown** with partition-batched records (9.01s vs 0.15s)
- **90% data loss** - only 1,000 results instead of 9,980
- **Progressive degradation** - performance gets worse as processing continues

### Measurements

| Metric | Linear | Partition | Status |
|--------|--------|-----------|--------|
| **Total Time** | 0.15s | 9.01s | 60x slower |
| **Throughput** | 66,238 rec/sec | 1,110 rec/sec | 60x slower |
| **Results** | 9,980 ✅ | 1,000 ❌ | 90% loss |
| **Emission Frequency** | 166 ✅ | 167 ✅ | Both correct |
| **Buffer Size** | ~60 ✅ | ~60 ✅ | Both consistent |

### Root Cause
Window buffer management works correctly, but GROUP BY aggregation fails because:
1. Records arrive grouped by partition (not timestamp order)
2. Groups from early partitions fill quickly
3. Groups from late partitions never get processed before window clears
4. Result: Only 2% of unique groups (100/5000) emit any results

### Fix Approaches
1. **Preprocessing**: Sort records by timestamp before window adapter
2. **Late-Arriving Data**: Modify aggregation to buffer and retry late groups
3. **Grace Period**: Add configurable window for out-of-order records
4. **State Recovery**: Snapshot window state to recover lost groups

## Files Modified

### New Files
- `docs/performance/WINDOW_ADAPTER_ANALYSIS.md` - Complete analysis document
- `tests/unit/server/processors/window_adapter_instrumentation_test.rs` - Comparison tests

### Modified Files
- `src/velostream/sql/execution/window_v2/strategies/tumbling.rs` - Added WindowMetrics

## Test Results

### Linear Batching (Baseline)
```
Input records: 10,000
Output results: 9,980 ✅
Total time: 0.15s
Throughput: 66,238 rec/sec
Avg time per record: 15.10µs
```

### Partition Batching (Problem Case)
```
Input records: 10,000
Output results: 1,000 ❌ (Only 10% of expected)
Total time: 9.01s
Throughput: 1,110 rec/sec
Avg time per record: 901.18µs
```

### Performance Degradation Timeline (Partition Batching)
- 2K records: 2,453 rec/sec
- 4K records: 1,707 rec/sec (30% slower)
- 6K records: 1,377 rec/sec (44% slower)
- 8K records: 1,182 rec/sec (52% slower)

## Related Issues - Conflicting Analyses ⚠️

### **THREE CONFLICTING ROOT CAUSE THEORIES**

#### 🔴 **Theory 1: Test Infrastructure** (`scenario_4_hang_fix_summary.md`)
- **Claim**: Hang fixed by channel receiver + lock management
- **Evidence**: All tests pass (✅ VERIFIED with current data)
- **Issue**: Explains TEST hang, not ALGORITHM behavior

#### 🟡 **Theory 2: Window State Management** (`window_hang_root_cause_analysis.md`)
- **Claim**: O(N²) window state scanning causes slowdown with partition batching
- **Evidence**: 1K records show 11.8x slowdown (0.10s vs 0.01s)
- **Hypothesis**: Quadratic growth in window boundary checking
- **Issue**: Theoretical analysis, not verified with profiling data

#### 🟢 **Theory 3: GROUP BY Aggregation Order** (Current Session - `WINDOW_ADAPTER_ANALYSIS.md`)
- **Claim**: Late partitions' groups missed during aggregation → 90% data loss
- **Evidence**: Measured 1000 results vs 9980 expected (10% loss) + 60x slowdown
- **Root Cause**: Skewed group distribution prevents all groups from aggregating
- **Status**: ROOT CAUSE IDENTIFIED with metrics-based evidence

### **Current Understanding**

The evidence suggests:
- **Data loss (Issue #2)**: ✅ CONFIRMED by measurements - only 2% of groups emit
- **60x Slowdown (Issue #1)**: ⚠️ PARTIALLY EXPLAINED
  - Part 1: Missing data causes fewer aggregations (obvious)
  - Part 2: Performance degradation pattern (52% slower from 2K→8K) needs explanation

**Possible Explanation**: Both Theories 2 and 3 are partially correct
- Theory 3 explains the DATA LOSS
- Theory 2 might explain the ADDITIONAL PERFORMANCE DEGRADATION

### **Previous Session: Scenario 4 Hang Fix** (`scenario_4_hang_fix_summary.md`)
- **Status**: ✅ COMPLETED AND VERIFIED
- **Test Results**: All 4 tests pass (small, medium, large, 1m datasets)
- **What it Fixed**: Test infrastructure issues
  1. Dropped output channel receiver (CRITICAL)
  2. Lock contention with Mutex (HIGH)
  3. No backpressure mechanism (MEDIUM)
- **Verification**: `cargo test test_scenario_4_simple_jp --no-default-features` → All 4 passed in 22.86s
- **Relationship**: Different problem layer (test infra vs algorithm)

## Critical Clarification: NOT A HANG - SEVERE PROGRESSIVE DEGRADATION

The SimpleJp "timeout" at 120 seconds with 1M records is **NOT a hang** but **extreme performance degradation**.

### Issue #3 (Clarified): Progressive Performance Degradation with Partition Batching

**Status**: ✅ CONFIRMED and MEASURED with concrete data

**Location**: Core algorithm in `src/velostream/sql/execution/window_v2/adapter.rs`

**Primary Test**: `test_sql_engine_partition_batching_100k` in `tests/unit/server/processors/sql_engine_partition_batching_test.rs`

**Degradation Evidence (100K records, partition batched):**
```
Processing 10K:  992 rec/sec (baseline)
Processing 20K:  946 rec/sec (5% slower)
Processing 30K:  545 rec/sec (42% slower)
Processing 40K:  355 rec/sec (64% slower - continues degrading)
```

**Extrapolation to 1M records:**
- At this degradation rate, speed would be ~60 rec/sec or worse
- 1M records ÷ 60 rec/sec = 16,667 seconds needed
- 120-second timeout processes only ~7,200 records
- **Not a hang, but making 1M records impractical**

---

## Root Cause: Two Related But Distinct Issues

### Issue #2: 90% Data Loss (Correctness)
- **Symptom**: 1,000 results instead of 9,980 (10% of expected)
- **Root Cause**: Groups from late partitions missed at window emission
- **Evidence**: Window emissions fire correctly (~167) but only 2% of groups emit

### Issue #3: Progressive Performance Degradation (Performance)
- **Symptom**: 992 → 355 rec/sec (64% slower by 40K records)
- **Root Cause**: Accumulating overhead from skewed group distribution + O(N) iterations
- **Pattern**: Each window emission processes more skipped groups

### Both Issues Share Root Cause

1. Records arrive **grouped by partition** (not timestamp order)
2. Window buffer receives **non-uniform group distribution**
3. `compute_aggregations_over_window()` in `window_v2/adapter.rs` iterates **O(N) over entire buffer**
4. Early partitions' groups processed, **late partitions' groups never get processed before window clears**
5. Each window emission accumulates more overhead
6. Result: **Both correctness loss AND progressive slowdown**

---

## Tests Exposing the Issue

### Test 1: Data Loss Measurement ✅
- **File**: `tests/unit/server/processors/window_adapter_instrumentation_test.rs`
- **Tests**:
  - `profile_adapter_execution_linear_10k()` - Baseline (9,980 results)
  - `profile_adapter_execution_10k()` - Partition batching (1,000 results)
- **Finds**: 90% data loss immediately visible

### Test 2: Progressive Degradation Measurement ✅ (BEST)
- **File**: `tests/unit/server/processors/sql_engine_partition_batching_test.rs`
- **Test**: `test_sql_engine_partition_batching_100k()`
- **Finds**: Clear performance degradation at checkpoints (10K, 20K, 30K, 40K)
- **Advantage**: Isolates issue to SQL Engine layer (not SimpleJobProcessor)

### Test 3: Comprehensive Baseline ✅
- **File**: `tests/performance/analysis/comprehensive_baseline_comparison.rs`
- **Scenario**: Scenario 4 (TUMBLING WINDOW + GROUP BY)
- **Records**: 1M (default, configurable via VELOSTREAM_BASELINE_RECORDS)
- **Finds**: SimpleJp timeout at 120s = degradation making 1M impractical

### Test 4: Pure SELECT Baseline ✅
- **File**: `tests/unit/server/processors/sql_engine_partition_batching_test.rs`
- **Test**: `test_sql_engine_partition_batching_pure_select()`
- **Finds**: No degradation with simple queries (proves windowing is the issue)

---

## Uncommitted Test Files

**6 new/modified test files in uncommitted changes:**

1. **`window_adapter_instrumentation_test.rs`** - Data loss metrics ✅
2. **`sql_engine_partition_batching_test.rs`** - Progressive degradation ✅✅ (BEST)
3. **`scenario_4_tumbling_hang_test.rs`** - Modified (already passing)
4. **`window_state_analysis_test.rs`** - Analysis helper
5. **`aggregation_microbench_test.rs`** - Microbench analysis
6. **`window_boundaries_group_by_sql_test.rs`** - Window boundary testing

---

## Analysis Complete ✅

### What We Know
- ✅ Issue is in core algorithm (`window_v2/adapter.rs`)
- ✅ Not in test infrastructure (separate issue was fixed)
- ✅ Two related problems: data loss + degradation
- ✅ Root cause: partition batching causes skewed group distribution
- ✅ Best exposing test: `test_sql_engine_partition_batching_100k`

### What We Don't Know Yet
- Where exactly in the algorithm the degradation occurs
- Whether both issues share exact same code path or different
- Precise algorithmic complexity of degradation
- Best fix approach (sorting, buffering, grace period, etc.)

## Deep Dive Analysis: ROOT CAUSE IDENTIFIED ✅

### Bottleneck Discovery
**Location**: `ProcessorContext::window_v2_states` HashMap in `src/velostream/sql/execution/processors/context.rs:115`

**Problem**: Window state is NEVER cleared after emission
- States accumulate indefinitely in the HashMap
- Each new window adds to state size
- When computing aggregations, O(N) iteration compounds with growing state
- Result: Super-linear degradation (1792 → 384 rec/sec over 40K records)

**Evidence**:
- `rows_window_states.clear()` exists (line 347)
- NO CORRESPONDING clear for `window_v2_states`
- States grow unbounded during processing

### Data Generation Verification ✅
Confirmed test data is SUB-LINEAR (5.5x time for 10x records):
- Test data generation is **NOT** the bottleneck
- Non-linear degradation is **100% from algorithm**
- Real-world partition skew would make it WORSE

### Implementation: Grace Period Window Clearing with Metrics

**Files Modified**:
- `src/velostream/sql/execution/window_v2/strategies/tumbling.rs`

**Changes**:
1. **Enhanced WindowMetrics struct** (8 fields):
   - `clear_calls` - Number of window clears
   - `total_buffer_sizes_at_clear` - Sum of buffer sizes at clear time
   - `max_buffer_size` - Peak buffer size
   - `add_record_calls` - Total records added
   - `emission_count` - Number of window emissions
   - `grace_period_delays` - Times grace period prevented early clear
   - `total_grace_period_ms` - Total grace period time
   - `late_arrival_discards` - Records arriving after grace period

2. **Grace Period State** (3 fields added to TumblingWindowStrategy):
   - `grace_period_percent: f64` - Default 50% of window size
   - `grace_period_start_time: Option<i64>` - When grace period starts
   - `window_emitted: bool` - Tracks if window has been emitted

3. **Enhanced clear() Method**:
   - Marks window as emitted (enters grace period)
   - Calculates grace period as 50% of window size
   - Does NOT evict records immediately
   - Only removes records outside grace period window
   - Tracks grace period delays in metrics
   - Records all metrics for performance analysis

### How Grace Period Works

```
Window Lifecycle:
[Window Start] -------- [Window Boundary] -------- [Grace Period End] -------- [Fully Clear]
                        ↑                           ↑
                        emit() called               records removed if
                        grace period begins         timestamp < grace_period_end
```

**Benefits**:
1. **Handles late arrivals** - Records within grace period are still processed
2. **Reduces data loss** - Partition skew records get extra time
3. **Bounded memory** - Grace period ensures eventual cleanup
4. **Measurable** - Metrics track grace period effectiveness

---

## 🔴 CRITICAL ANALYSIS: BUFFER ACCUMULATION & LATE DATA HANDLING (Session 2)

### What the Total Buffer Records Metric is Telling Us

**The window buffer is ACCUMULATING records, not clearing them properly.**

Current behavior in `TumblingWindowStrategy::clear()`:
```rust
// Grace period: allow records up to (window_end_time + grace_period_ms)
let grace_period_end = window_end_time + grace_period_ms;

if let Some(start) = self.window_start_time {
    while let Some(record) = self.buffer.front() {
        if let Ok(ts) = self.extract_timestamp(record) {
            // Only remove if record is before start of current window
            // AND outside grace period of previous window
            if ts < start && ts < grace_period_end - self.window_size_ms {
                self.buffer.pop_front();  // ← Only removes VERY old records
            } else {
                break;  // ← STOPS if record is anywhere near current window!
            }
        }
    }
}
```

**PROBLEM**: The condition `ts < grace_period_end - self.window_size_ms` is **TOO CONSERVATIVE**.

When partition-batched data arrives:
1. Partition 0: timestamps 1000000-1100000 (records with 10 unique groups)
2. First window boundary fires at 1060000
3. Window clears, advances to [1060000, 1120000)
4. **Buffer removal only deletes records before `1030000` (grace_period_end - window_size)**
5. Partition 1 arrives: timestamps 1100000-1200000 (new groups!)
6. These records have LATER timestamps than what we're checking
7. Buffer never gets cleaned of partition 0 records
8. **Result: Buffer grows unbounded** → degradation increases

### Emission Metrics Revealed the Smoking Gun

From our test run:
```
Total unique groups in input:  100 (only first ~2K records)
Expected groups:              5000 (T0-T49 × SYM0-SYM99)
Actual groups emitting:       100 (exactly what's in first 2K!)
Groups LOST:                  4900 (98% - never appear in results!)

Timeline of emissions:
- Emission 1: Record 3 (first window complete)
- Emission 2-168: Records 804-1001 (partition 0 data, then stops)
- NO EMISSIONS AFTER RECORD 1001 (90% of data gets no results!)

Buffer state at each emission:
- Early emissions: 2-10 records in buffer
- Later emissions: 5-10 records in buffer
- ALWAYS contains partition 0 records even after emission!
```

**This proves records are NOT being cleaned properly.**

### How Flink Handles This (The Correct Approach)

Apache Flink uses **Watermarks + Allowed Lateness** pattern:

1. **Watermark**: A special timestamp that indicates "all records older than X have arrived"
   - Advances as new data arrives
   - Tells system when to finalize windows

2. **Allowed Lateness**: Grace period in milliseconds
   - Default: 0 (no late data allowed)
   - Configurable: Can be hours for late-arriving partitions
   - Example: `stream.allowedLateness(Time.hours(24))`

3. **Trigger Mechanism**:
   ```
   Window [1000000, 1060000):
   ├─ INITIAL FIRE: When watermark passes 1060000
   │  └─ Emit results for groups seen so far
   │
   ├─ GRACE PERIOD: Allowed lateness = 30000ms
   │  └─ Window state retained until watermark passes 1090000
   │
   └─ LATE FIRE: If records arrive during grace period
      ├─ Add record to window
      ├─ Re-evaluate all groups
      ├─ EMIT UPDATED RESULTS (called "late firing")
      └─ Deduplicate with earlier results at sink
   ```

4. **State Management**:
   - Window state kept until watermark > window_end + allowed_lateness
   - Once state cleared, NO MORE LATE FIRINGS possible
   - Bounded memory: Old windows deleted automatically

### What Velostream Currently Does (BROKEN)

```
Window [1000000, 1060000):
├─ EMIT: When record with ts > 1060000 arrives
│  └─ Returns EmitDecision::EmitAndClear
│
├─ CLEAR: Called immediately after emit
│  └─ Advances window to [1060000, 1120000)
│  └─ Grace period delays removal by 50% (not proper watermarking!)
│  └─ Records removed only if ts < 1030000 - WRONG!
│
└─ PROBLEM: New records from partition 1 (ts > 1060000)
   ├─ Arrive AFTER initial fire already happened
   ├─ CANNOT trigger new emissions (window already cleared)
   ├─ LOST - Never processed in any window
   └─ Result: 90% data loss
```

### Why Grace Period Alone DOESN'T WORK

Grace period (50% delay) helps with:
- ✅ Records arriving 30 seconds after window boundary
- ✅ Within-partition out-of-order data
- ✅ Network jitter delays

Grace period FAILS with:
- ❌ Partition batching (partition 1 data arrives 100k+ ms later)
- ❌ Skewed group distribution (different groups in different partitions)
- ❌ No mechanism to trigger RE-EMISSION when late data arrives
- ❌ No watermark concept (doesn't know when all data for a window has arrived)

### The Core Issue: No Watermark = No Late Firing

Without watermarks, the system cannot:
1. Know which records are "late" vs "on-time"
2. Distinguish between "slow partition" and "lost data"
3. Trigger re-emissions when late data arrives
4. Know when window state is safe to discard

**This is why Flink REQUIRES watermarks for proper windowing!**

### CRITICAL MISSING PIECE: No Watermark Reference Point

**Current TumblingWindowStrategy is MISSING the watermark:**

```rust
pub struct TumblingWindowStrategy {
    window_size_ms: i64,
    buffer: VecDeque<SharedRecord>,
    window_start_time: Option<i64>,
    window_end_time: Option<i64>,
    emission_count: usize,
    time_field: String,
    grace_period_percent: f64,
    grace_period_start_time: Option<i64>,
    window_emitted: bool,
    // ❌ MISSING: max_watermark_seen: i64
    // ❌ MISSING: allowed_lateness_ms: i64
    metrics: Arc<Mutex<WindowMetrics>>,
}
```

**Without watermark, the window CANNOT**:
1. Distinguish "record is late" from "record is out-of-order"
2. Know when it's safe to delete window state
3. Trigger re-emissions for late-arriving records
4. Implement proper allowed lateness window

### Required Fix Architecture

To properly handle partition-batched data:

1. **Add Watermark Tracking to TumblingWindowStrategy**:
   ```rust
   pub struct TumblingWindowStrategy {
       // ... existing fields ...

       /// Highest timestamp seen so far (the watermark)
       /// CRITICAL: This is the reference point for determining if a record is "late"
       max_watermark_seen: i64,

       /// How long to keep window state after initial emission (milliseconds)
       /// Example: 2 hours = 7,200,000 ms (allows all partitions to arrive)
       allowed_lateness_ms: i64,

       /// Map to keep multiple windows alive for late data
       /// Key: window_start_time, Value: WindowState
       /// Allows records that miss their window to still be processed as late firings
       historical_windows: HashMap<i64, WindowState>,
   }
   ```

2. **Implement Watermark Advancement Logic**:
   - For each incoming record, update watermark: `max_watermark_seen = max(max_watermark_seen, record.timestamp)`
   - Use watermark to determine:
     - Is record "on-time"? → `record.ts > current_window_end`
     - Is record "late"? → `record.ts <= current_window_end && record.ts > current_window_end - allowed_lateness`
     - Is record "too late"? → `record.ts <= current_window_end - allowed_lateness`

3. **Implement Allowed Lateness**:
   - Keep window state alive while: `watermark < window_end + allowed_lateness`
   - Once `watermark >= window_end + allowed_lateness`: Delete window state (no more late firings possible)
   - Example: Allow 2 hours for all partitions to arrive
   ```rust
   let grace_period_end = window_end_time + allowed_lateness_ms;
   if watermark >= grace_period_end {
       // Window is truly closed - no more records for this window
       delete_window_state(window_start_time)
   }
   ```

4. **Implement Late Firing Mechanism**:
   - When record arrives with `record.ts <= current_window_end`:
     - Check if we have state for that window
     - If yes, add record to that window's accumulator
     - Re-compute aggregations
     - **EMIT UPDATED RESULT** (marked as "late firing")
     - Update watermark
   - If no, and watermark hasn't passed grace period:
     - Store as "pending late record" for future processing
     - Deduplicate at sink (multiple emissions for same window are expected)

5. **Implement State Cleanup**:
   - Clear window state when `watermark > window_end + allowed_lateness`
   - Bounded memory: `num_concurrent_windows = (allowed_lateness_ms + window_size_ms) / window_size_ms`
   - Example: With 2-hour lateness and 1-minute windows: ~120 windows in flight
   ```rust
   for (start_time, window_state) in &mut historical_windows {
       let window_end = start_time + window_size_ms;
       if watermark > window_end + allowed_lateness_ms {
           historical_windows.remove(start_time);  // Safe to delete
       }
   }
   ```

### How Watermark Changes Everything

**BEFORE (Current - No Watermark)**:
```
Record arrives: ts=1050000
├─ Window boundary check: ts < 1060000? → Yes, add to buffer
└─ Is it "late"? → DON'T KNOW! (no reference point)
   └─ No mechanism to trigger re-emission
   └─ Result: Data might be lost silently
```

**AFTER (With Watermark)**:
```
Record arrives: ts=1050000
├─ Update watermark: max_watermark_seen = max(1050000, prev_watermark)
├─ Window boundary check: ts < 1060000? → Yes
│  └─ Is it "on-time"? → ts > watermark - allowed_lateness?
│     └─ Yes → Add to current window → Normal processing
│
└─ Record arrives: ts=1000000 (late from partition 2)
   ├─ Update watermark: max_watermark_seen = 1050000 (unchanged)
   ├─ Window boundary check: ts=1000000 < current_window_start=1060000?
   │  └─ This should have been in window [1000000, 1060000)
   │
   ├─ Check allowed lateness:
   │  └─ Is watermark < 1060000 + 7200000?
   │     └─ Yes (watermark=1050000 < grace=8260000)
   │     └─ Retrieve historical window state for [1000000, 1060000)
   │     └─ Add record to that window
   │     └─ Re-compute aggregations
   │     └─ **EMIT UPDATED RESULT** (late firing)
   │
   └─ Result: Zero data loss! ✅
```

### Expected Impact

With watermark + allowed lateness properly implemented:

**Partition batching test should show:**
```
Metrics BEFORE fix (current):
├─ Total emissions: 168 (initial only, no late firings)
├─ Results: 1,000 (90% loss)
├─ Groups emitting: 100 (only first partition)
└─ Throughput: 1,082 rec/sec (87% degradation)

Metrics AFTER fix:
├─ Total emissions: 168 (initial) + ~100 (late firings)
├─ Results: 9,980 (0% loss)
├─ Groups emitting: 5,000 (all partitions)
└─ Throughput: ~8,000 rec/sec (minimal degradation)
```

### Metrics That Should Exist (After Fix)

```rust
pub struct WindowMetrics {
    // Current metrics (correct)
    pub clear_calls: usize,
    pub total_buffer_sizes_at_clear: usize,
    pub max_buffer_size: usize,
    pub add_record_calls: usize,
    pub emission_count: usize,

    // Grace period metrics (current - insufficient)
    pub grace_period_delays: usize,
    pub total_grace_period_ms: u64,
    pub late_arrival_discards: usize,

    // MISSING METRICS (needed for proper late arrival handling)
    pub initial_firings: usize,           // First emission per window
    pub late_firings: usize,              // Re-emissions due to late data
    pub records_in_late_firing: usize,    // Total records causing re-emission
    pub windows_with_late_data: usize,    // Count of windows receiving late arrivals
    pub data_dropped_after_grace: usize,  // Records discarded (too late)
}
```

### Expected Behavior After Fix

```
Test: 10K records, partition-batched, 5000 unique groups

BEFORE FIX:
- Results: 1,000 (90% loss)
- Emissions: 168 (only initial fires)
- Throughput: 1,082 rec/sec (87% degradation)
- Buffer max: ~60 records
- Data loss: 8,980 records

AFTER FIX (with watermarks + 2-minute allowed lateness):
- Results: ~9,980 (0% loss)
- Emissions: ~168 initial + ~100 late fires
- Throughput: ~8,000 rec/sec (minimal degradation)
- Buffer max: ~60 records per window × multiple windows in flight
- Data loss: 0 (within lateness window)
```

### Implementation Priority

**CRITICAL PATH**:
1. Implement watermark concept (detect when partition data arrives)
2. Implement late firing mechanism (re-emit when late data arrives)
3. Keep window state alive for allowed lateness period
4. Add proper state cleanup

**SECONDARY**:
5. Optimize buffer management for multiple windows in flight
6. Add configurable allowed_lateness parameter
7. Track late firing metrics
8. Add side-output for dropped records

---

### Next Steps (Testing Phase)
- Run partition batching test with grace period enabled
- Compare metrics: before vs after
- Measure improvement in throughput degradation
- Verify data loss is eliminated or reduced
