# SLIDING Window Correctness Analysis

**Date**: 2025-11-01
**Status**: Analysis in progress

## SLIDING Window Implementation

**Location**: `window.rs:612-621`

```rust
WindowSpec::Sliding { advance, .. } => {
    let advance_ms = advance.as_millis() as i64;

    // For sliding windows, emit based on advance interval
    if last_emit == 0 {
        return event_time >= advance_ms;  // ⚠️ Absolute vs relative comparison
    }

    event_time >= last_emit + advance_ms
}
```

---

## Issue #1: Same Absolute vs Relative Time Bug as TUMBLING

### The Problem

```rust
if last_emit == 0 {
    return event_time >= advance_ms;
    //     ^^^^^^^^^^^^^    ^^^^^^^^^^
    //     Absolute time    Relative duration
    //     (1.7 billion)    (300,000 for 5 minutes)
}
```

**What happens**:
- `event_time` = 1700000000 (milliseconds since epoch)
- `advance_ms` = 300,000 (5 minute slide interval)
- Comparison: `1700000000 >= 300000` = **TRUE** (always!)

**Result**: First window emits immediately on the first record.

---

## Issue #2: Test Data Analysis

### Test: `benchmark_sliding_window_moving_average`

**Window spec**:
```sql
WINDOW SLIDING(10m, 5m)  -- 10 minute window, 5 minute slide
```

**Converted to milliseconds**:
- Window size: 600,000 ms
- Advance interval: 300,000 ms

**Test data**:
```rust
let timestamp = base_time + (i as i64 * 10);  // 10 second intervals
//              1700000000 + (0..9999) * 10
//              = 1700000000 to 1700099990
```

**Time span**: 99,990 (assuming these are milliseconds = 99.99 seconds)

---

## Issue #3: Emission Frequency Analysis

### Expected Behavior

For SLIDING(10m, 5m):
- Window size: 10 minutes
- Slide: 5 minutes
- Should emit every 5 minutes of event time

**For 99.99 seconds of data**:
- Expected emissions: 99.99 / 300 = ~0 emissions (data < 5 minute interval!)

**Actually**:
- First emission at record 0 (because `1700000000 >= 300000`)
- Next emission when: `event_time >= 1700000000 + 300000 = 1700300000`
- Record 9999 timestamp: 1700099990
- `1700099990 >= 1700300000`? **NO**

**Conclusion**: Only **1 emission** (the first record), then no more until reaching timestamp 1700300000.

---

## Issue #4: Correctness vs Performance

### Does SLIDING have the O(N²) cloning issue?

**YES!** Same problem:

```rust
// engine.rs:583 - called on EVERY record
self.save_window_states_from_context(&context);
    └── context.get_dirty_window_states()
        └── window_state.clone()  // ← Clones entire buffer!
```

### How many times does SLIDING emit?

Based on analysis above: **Only 1 emission** (first record).

**But** - the buffer still accumulates all 10,000 records because:
1. No cleanup happens (no emission after first record)
2. Every record still causes the O(N²) clone

**Impact**:
- Same O(N²) performance issue
- But only 1-2 emissions instead of 0 (TUMBLING) or 10,000 (SESSION)

---

## Test Data Issues

### Problem: Unit Ambiguity

All tests use:
```rust
let timestamp = base_time + (i as i64 * interval);
```

Where `interval` varies:
- TUMBLING: `1` (1 millisecond? 1 second?)
- SLIDING: `10` (10 milliseconds? 10 seconds?)
- SESSION: `10` or `2000` (depending on gap)

**Code comment** (line 256): `// 10 second intervals`

**But**: No `* 1000` to convert to milliseconds!

### Correct Test Data

Should be:
```rust
let timestamp = base_time + (i as i64 * 10 * 1000);  // 10 second intervals in ms
```

---

## SLIDING Window Correctness Issues

| Issue | Severity | Impact |
|-------|----------|--------|
| **Absolute vs relative time** | 🔴 Critical | Wrong emission logic |
| **Test data units** | 🔴 Critical | Windows don't emit correctly |
| **Buffer cloning O(N²)** | 🔴 Critical | 166x performance degradation |
| **Window boundary logic** | 🟡 Moderate | Emissions may be off by one |

---

## Correct Implementation

### Option A: Use Window-Relative Timestamps

```rust
WindowSpec::Sliding { size, advance, .. } => {
    let window_size_ms = size.as_millis() as i64;
    let advance_ms = advance.as_millis() as i64;

    // Calculate window boundaries
    if last_emit == 0 {
        // First window - emit when we have enough data for one window
        let earliest_time = window_state.buffer.first()
            .map(|r| Self::extract_event_time(r, window_spec.time_column()))
            .unwrap_or(event_time);

        event_time >= earliest_time + window_size_ms
    } else {
        // Subsequent windows - emit based on slide interval
        event_time >= last_emit + advance_ms
    }
}
```

### Option B: Track Window Start Time

```rust
pub struct WindowState {
    pub window_spec: WindowSpec,
    pub buffer: Vec<StreamRecord>,
    pub last_emit: i64,
    pub window_start: Option<i64>,  // ← NEW: Track when window started
}

// In should_emit logic:
if window_state.window_start.is_none() {
    // Initialize window start on first record
    window_state.window_start = Some(event_time);
}

let window_start = window_state.window_start.unwrap();
let elapsed = event_time - window_start;

// Emit when we've advanced by the slide interval
elapsed >= advance_ms
```

---

## Required Fixes for SLIDING Windows

### Fix #1: Emission Logic

Replace absolute/relative comparison with proper window-relative logic:

```rust
WindowSpec::Sliding { size, advance, .. } => {
    let advance_ms = advance.as_millis() as i64;

    if last_emit == 0 {
        // First window: need at least one slide interval of data
        let first_time = window_state.buffer.first()
            .map(|r| Self::extract_event_time(r, window_spec.time_column()))
            .unwrap_or(event_time);

        event_time - first_time >= advance_ms
    } else {
        // Subsequent windows: emit every slide interval
        event_time >= last_emit + advance_ms
    }
}
```

### Fix #2: Test Data Units

Convert to milliseconds:
```rust
let timestamp = base_time + (i as i64 * 10 * 1000);  // 10 seconds in ms
```

Or use actual second-granularity timestamps and update window logic to handle both.

### Fix #3: Buffer Cleanup

SLIDING windows should only keep records within the window size:

```rust
// After emission, cleanup old records
let window_start_time = event_time - window_size_ms;
window_state.buffer.retain(|r| {
    let record_time = Self::extract_event_time(r, window_spec.time_column());
    record_time >= window_start_time
});
```

---

## Summary: All Window Types Have Issues

| Window Type | Emission Bug | Test Data Bug | O(N²) Clone Bug |
|-------------|--------------|---------------|-----------------|
| **TUMBLING** | Absolute/relative time | ✅ Yes (sec/ms) | ✅ Yes |
| **SLIDING** | Absolute/relative time | ✅ Yes (sec/ms) | ✅ Yes |
| **SESSION** | Emits every record | ⚠️ Partial | ✅ Yes |

**All three window types need fixing!**

---

## Recommendation

Fix all window types together:

1. **Emission logic**: Use window-relative time calculations
2. **Test data**: Consistent millisecond timestamps
3. **Performance**: Eliminate O(N²) buffer cloning
4. **Cleanup**: Proper buffer management for each window type

**Estimated fix time**: 1-2 days for all window types + comprehensive testing
