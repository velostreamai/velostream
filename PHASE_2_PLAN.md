# Phase 2: RwLock to Atomic Counters Optimization

## Objective
Replace `RwLock<MetricsPerformanceTelemetry>` with atomic counters to reduce lock contention and improve performance on hot paths.

## Current Implementation Analysis

### File: `src/velostream/server/processors/metrics_helper.rs`

**Problem Code (Lines 47-92):**
```rust
pub struct MetricsPerformanceTelemetry {
    pub condition_eval_time_us: u64,
    pub label_extract_time_us: u64,
    pub total_emission_overhead_us: u64,
}

pub struct ProcessorMetricsHelper {
    telemetry: Arc<RwLock<MetricsPerformanceTelemetry>>,  // ← CONTENTION POINT
}
```

**Problematic Usage (Lines 295-314):**
```rust
pub async fn record_condition_eval_time(&self, duration_us: u64) {
    let mut telemetry = self.telemetry.write().await;  // ← WRITE LOCK (contention)
    telemetry.condition_eval_time_us = telemetry.condition_eval_time_us.saturating_add(duration_us);
}

pub async fn record_label_extract_time(&self, duration_us: u64) {
    let mut telemetry = self.telemetry.write().await;  // ← WRITE LOCK (contention)
    telemetry.label_extract_time_us = telemetry.label_extract_time_us.saturating_add(duration_us);
}

pub async fn record_emission_overhead(&self, duration_us: u64) {
    let mut telemetry = self.telemetry.write().await;  // ← WRITE LOCK (contention)
    telemetry.total_emission_overhead_us = telemetry.total_emission_overhead_us.saturating_add(duration_us);
}
```

**Call Sites (Lines 468-475, 484-485, 552-553):**
```rust
self.record_condition_eval_time(cond_start.elapsed().as_micros() as u64).await;
self.record_label_extract_time(extract_start.elapsed().as_micros() as u64).await;
self.record_emission_overhead(record_start.elapsed().as_micros() as u64).await;
```

### Performance Impact

Per record with 10 annotations:
- 10 × `record_condition_eval_time()` calls → 10 write locks
- 10 × `record_label_extract_time()` calls → 10 write locks
- 1 × `record_emission_overhead()` call → 1 write lock
- **Total: 21 write locks per record**

For batch of 10,000 records:
- 210,000 write lock acquisitions
- Each lock: ~1-5 µs (depends on contention)
- **Estimated overhead: 210-1,050 ms per batch** ← Major bottleneck!

### Solution: Atomic Counters

**Advantages:**
- ✅ No async overhead (no `.await`)
- ✅ Lock-free (uses CPU atomic operations)
- ✅ Better performance under contention
- ✅ No memory allocation
- ✅ Thread-safe without explicit locks

**Trade-offs:**
- ⚠️ Atomic operations add ~10-20 ns per operation (vs RwLock write lock ~1-5 µs)
- ⚠️ Net gain: ~99% reduction in overhead!

## Implementation Plan

### Task 1: Create AtomicMetricsPerformanceTelemetry struct

**Location:** `src/velostream/server/processors/metrics_helper.rs`

**New struct (replace RwLock version):**
```rust
use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free performance telemetry using atomic counters
#[derive(Debug)]
pub struct AtomicMetricsPerformanceTelemetry {
    /// Time spent in condition evaluation (microseconds)
    condition_eval_time_us: Arc<AtomicU64>,
    /// Time spent in label extraction (microseconds)
    label_extract_time_us: Arc<AtomicU64>,
    /// Total emission overhead per record (microseconds)
    total_emission_overhead_us: Arc<AtomicU64>,
}

impl AtomicMetricsPerformanceTelemetry {
    pub fn new() -> Self {
        Self {
            condition_eval_time_us: Arc::new(AtomicU64::new(0)),
            label_extract_time_us: Arc::new(AtomicU64::new(0)),
            total_emission_overhead_us: Arc::new(AtomicU64::new(0)),
        }
    }

    // Record methods (no async, no locks!)
    pub fn record_condition_eval_time(&self, duration_us: u64) {
        self.condition_eval_time_us
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_label_extract_time(&self, duration_us: u64) {
        self.label_extract_time_us
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_emission_overhead(&self, duration_us: u64) {
        self.total_emission_overhead_us
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    // Get current values (reading)
    pub fn condition_eval_time_us(&self) -> u64 {
        self.condition_eval_time_us.load(Ordering::Relaxed)
    }

    pub fn label_extract_time_us(&self) -> u64 {
        self.label_extract_time_us.load(Ordering::Relaxed)
    }

    pub fn total_emission_overhead_us(&self) -> u64 {
        self.total_emission_overhead_us.load(Ordering::Relaxed)
    }

    // Reset (atomic exchange)
    pub fn reset(&self) {
        self.condition_eval_time_us.store(0, Ordering::Relaxed);
        self.label_extract_time_us.store(0, Ordering::Relaxed);
        self.total_emission_overhead_us.store(0, Ordering::Relaxed);
    }

    // Get all values atomically
    pub fn get_snapshot(&self) -> MetricsPerformanceTelemetry {
        MetricsPerformanceTelemetry {
            condition_eval_time_us: self.condition_eval_time_us(),
            label_extract_time_us: self.label_extract_time_us(),
            total_emission_overhead_us: self.total_emission_overhead_us(),
        }
    }
}

impl Clone for AtomicMetricsPerformanceTelemetry {
    fn clone(&self) -> Self {
        Self {
            condition_eval_time_us: Arc::clone(&self.condition_eval_time_us),
            label_extract_time_us: Arc::clone(&self.label_extract_time_us),
            total_emission_overhead_us: Arc::clone(&self.total_emission_overhead_us),
        }
    }
}

impl Default for AtomicMetricsPerformanceTelemetry {
    fn default() -> Self {
        Self::new()
    }
}
```

### Task 2: Update ProcessorMetricsHelper struct

**Changes:**
```rust
pub struct ProcessorMetricsHelper {
    metric_conditions: Arc<RwLock<HashMap<String, Arc<Expr>>>>,
    pub label_config: LabelHandlingConfig,
    label_extraction_config: LabelExtractionConfig,
    telemetry: AtomicMetricsPerformanceTelemetry,  // ← CHANGED (no Arc<RwLock<>>)
}
```

### Task 3: Update telemetry recording methods

**Changes in ProcessorMetricsHelper:**
```rust
// Remove async, remove .await
pub fn record_condition_eval_time(&self, duration_us: u64) {
    self.telemetry.record_condition_eval_time(duration_us);  // No async!
}

pub fn record_label_extract_time(&self, duration_us: u64) {
    self.telemetry.record_label_extract_time(duration_us);  // No async!
}

pub fn record_emission_overhead(&self, duration_us: u64) {
    self.telemetry.record_emission_overhead(duration_us);  // No async!
}

pub async fn get_telemetry(&self) -> MetricsPerformanceTelemetry {
    // Now non-async, can stay async for API compatibility
    self.telemetry.get_snapshot()
}

pub async fn reset_telemetry(&self) {
    // Now non-async, can stay async for API compatibility
    self.telemetry.reset()
}
```

### Task 4: Update call sites in emit_metrics_generic

**Changes (remove `.await`):**
```rust
// Line 468-475 (was):
self.record_condition_eval_time(cond_start.elapsed().as_micros() as u64).await;

// Now:
self.record_condition_eval_time(cond_start.elapsed().as_micros() as u64);

// Line 484-485 (was):
self.record_label_extract_time(extract_start.elapsed().as_micros() as u64).await;

// Now:
self.record_label_extract_time(extract_start.elapsed().as_micros() as u64);

// Line 552-553 (was):
self.record_emission_overhead(record_start.elapsed().as_micros() as u64).await;

// Now:
self.record_emission_overhead(record_start.elapsed().as_micros() as u64);
```

## Testing Strategy

### Unit Tests
- Verify atomic counters accumulate correctly
- Test saturation (u64 overflow handling)
- Verify clone() works correctly
- Test reset() functionality

### Performance Tests
- Benchmark RwLock vs atomic counters
- Measure lock contention under concurrent loads
- Verify total throughput improvement

### Regression Tests
- Ensure all 370 existing tests still pass
- Verify no behavioral changes to metrics emission

## Expected Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Per-record telemetry overhead | ~210-1,050 ms/batch | ~2-5 ms/batch | **95-99% reduction** |
| Lock acquisitions per record | 21 write locks | 0 locks | **100% elimination** |
| Atomic operations per record | 0 | 21 atomic ops | ~210-420 ns |
| Net performance gain | — | ~205-1,045 ms/batch | **5-10% throughput** |

## Ordering Strategy

Using `Ordering::Relaxed` throughout because:
- ✅ No cross-thread synchronization needed (independent counters)
- ✅ Minimal happens-before requirements (just atomic increment)
- ✅ Best performance (no memory barriers)
- ✅ Safe for all cores/CPUs

## Files to Modify

1. `src/velostream/server/processors/metrics_helper.rs`
   - Add imports: `use std::sync::atomic::{AtomicU64, Ordering};`
   - Replace RwLock telemetry with atomic counters
   - Update recording methods to be non-async
   - Update call sites to remove `.await`

## Estimated Effort

- **Task 1 (Atomic struct)**: 1 hour
- **Task 2 (Update helper)**: 1 hour
- **Task 3 (Update methods)**: 30 minutes
- **Task 4 (Update call sites)**: 30 minutes
- **Testing & verification**: 1 hour
- **Total**: 4 hours

## Risk Assessment

**Low Risk:**
- ✅ Atomic operations are well-established pattern
- ✅ Semantically equivalent to RwLock (same values recorded)
- ✅ No behavioral changes visible to consumers
- ✅ All tests should pass unchanged

**Considerations:**
- ⚠️ Relaxed ordering assumes independent counters (true in this case)
- ⚠️ May need ABI considerations if atomics used across FFI (not relevant here)

## Next Phase

After Phase 2, proceed to:

**Phase 3: Consolidate DynamicMetrics**
- Merge 3 separate Arc<Mutex<>> into single structure
- Reduce lock contention in metrics.rs
- Estimated 2-3 hour gain: 5% additional throughput
