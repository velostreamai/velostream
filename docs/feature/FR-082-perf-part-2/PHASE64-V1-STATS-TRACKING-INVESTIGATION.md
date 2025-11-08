# FR-082 Phase 6.4: V1 Stats Tracking Anomaly Investigation

**Status**: 🔍 INVESTIGATION REQUIRED
**Date Created**: November 8, 2025
**Priority**: HIGH (Blocks accurate performance reporting)
**Assigned Phase**: Phase 6.4

---

## Executive Summary

During Phase 6.3 test validation, a critical anomaly was discovered in V1 JobProcessor's stats tracking:

**The Problem**: V1 reports `records_processed = 0` despite data provably flowing through the system

```
Test Input:  5000 records, 5 batches of 1000 each
Elapsed Time: 27.47ms
Measured Throughput: 182,112 rec/sec (proves 5000 records processed)
Reported Stats:
  ✗ records_processed = 0  (WRONG - should be 5000)
  ✗ batches_processed = 0  (WRONG - should be 5)
  ✓ records_failed = 5000  (Correct - by design)
  ✓ total_processing_time = 27.47ms (Correct)
```

**Impact**:
- Can't trust V1 throughput comparison (must use elapsed time calculations)
- V2 stats appear to work correctly (shows 5000 records processed)
- Test metrics validation is blind to actual record counts for V1

---

## Evidence

### Proof Data IS Flowing
1. **Non-Zero Elapsed Time**: 27.47ms (not instant)
2. **Valid Throughput**: 182,112 rec/sec = 5000 / 0.02747s
3. **MockDataWriter Receives Records**: Sampling 1 in 10k records works
4. **Tests Don't Crash**: Full pipeline completes successfully
5. **V2 Works Correctly**: V2 shows 5000 records_processed (stats are correct)

### Evidence Stats Are NOT Being Updated
1. **Zero records_processed**: Even though data flows through for 27.47ms
2. **Zero batches_processed**: Even though 5 batches were read
3. **All records_failed**: 5000/5000 (likely intentional design)
4. **Contrast with V2**: V2 reports 5000 records_processed correctly

### Testing Evidence

**Scenario 0 (Pure SELECT - 5000 records)**
```
V1 Results:
  Measured throughput: 21,932 rec/sec
  Time: 227.93ms
  Stats: records_processed = 0, batches_processed = 0

Diagnostic output: "V1 STATS ANOMALY DETECTED"
Reason: Throughput proves data flows (non-zero elapsed time)
```

**Scenario 2 (GROUP BY - 5000 records)**
```
V1 Results:
  Measured throughput: 182,112 rec/sec
  Time: 27.47ms
  Stats: records_processed = 0, batches_processed = 0

Diagnostic output: "Data IS flowing through V1, but stats not captured correctly"
```

---

## Root Cause Analysis

### Suspected Location: `src/velostream/server/processors/v1/job_processor_v1.rs`

The V1 JobProcessor's `process_job()` method likely has an issue in:

1. **Stats Initialization**
   - May be initializing stats outside the processing loop
   - Stats may be getting reset or lost

2. **Stats Update Path**
   - Records may be processed through engine without updating `aggregated_stats`
   - The counter may be in wrong scope (local vs returned struct)

3. **Batch Processing**
   - Individual records may be processed without updating `batches_processed`
   - Batch count may not increment properly

4. **Stats Return**
   - Stats struct may be returned empty/uninitialized
   - Stats may be collected but not returned in Ok(stats)

### Key Questions

1. **Where are records processed?**
   - Via QueryProcessor? Via engine.execute_with_record()?
   - Are there stats updates in those code paths?

2. **When is batches_processed incremented?**
   - After reading a batch?
   - After processing batch records?
   - Is it being done at all?

3. **When is records_processed incremented?**
   - Per record processed?
   - Per batch processed?
   - Is the code path even being executed?

4. **Why does V2 work correctly?**
   - V2 increments `aggregated_stats.records_processed += batch_size` after processing
   - What's different in V1's implementation?

---

## Investigation Steps

### Step 1: Compare V1 and V2 Stats Code
```bash
# View V1 stats update logic
grep -n "records_processed" src/velostream/server/processors/v1/job_processor_v1.rs

# View V2 stats update logic (known working)
grep -n "records_processed" src/velostream/server/v2/job_processor_v2.rs

# Show full V1 process_job() implementation
sed -n '/async fn process_job/,/^    }/p' src/velostream/server/processors/v1/job_processor_v1.rs | head -100
```

### Step 2: Trace Record Processing Path
- [ ] Find where records are processed in V1
- [ ] Verify stats are updated after processing
- [ ] Check if records go through engine or direct processing
- [ ] Verify aggregated_stats is in correct scope

### Step 3: Add Debug Logging
- [ ] Add `log::debug!()` before/after each stats update
- [ ] Run test with `RUST_LOG=debug` to see stat updates
- [ ] Trace the exact point where stats stop being updated

### Step 4: Unit Test Stats Tracking
- [ ] Create isolated test of V1 stats tracking
- [ ] Mock DataReader/DataWriter to control record flow
- [ ] Verify stats increment correctly for known inputs
- [ ] Compare with V2 unit test (if exists)

### Step 5: Fix and Validate
- [ ] Apply fix to V1 process_job()
- [ ] Re-run scenario tests
- [ ] Verify stats now show correct values
- [ ] Commit fix with "fix" prefix

---

## Related Files

**V1 Implementation**
- `src/velostream/server/processors/v1/job_processor_v1.rs` (likely contains bug)
- `src/velostream/server/processors/common.rs` (JobExecutionStats definition)

**V2 Reference (Working)**
- `src/velostream/server/v2/job_processor_v2.rs` (lines 195-197 show correct pattern)
  ```rust
  aggregated_stats.records_processed += batch_size as u64;
  aggregated_stats.batches_processed += 1;
  ```

**Tests Detecting Anomaly**
- `tests/performance/analysis/scenario_0_pure_select_baseline.rs` (lines 374-381)
- `tests/performance/analysis/scenario_2_pure_group_by_baseline.rs` (lines 396-404)

**Validation Infrastructure**
- `tests/performance/validation.rs` (metrics validation module)
- Uses `MetricsValidation::validate_metrics()` for reporting

---

## Success Criteria

When fixed, V1 should report stats matching this pattern:

```
records_processed = 5000  (for 5000 input records)
batches_processed = 5    (for 5 batches of 1000)
records_failed = 5000    (by design - failure_strategy)
```

**Validation Test**:
```rust
// Should pass after fix
let v1_stats = /* run V1 test */;
assert_eq!(v1_stats.records_processed, 5000);
assert_eq!(v1_stats.batches_processed, 5);
// records_failed = 5000 is OK (by design)
```

---

## Implementation Notes

### Don't Break V2
- When fixing V1, ensure V2 stats still show 5000 records_processed
- V2 is currently working correctly (verified)

### Consider Unified Stats Pattern
- V1 and V2 should use same stats tracking pattern
- Could extract to common helper function:
  ```rust
  aggregated_stats.records_processed += batch_size as u64;
  aggregated_stats.batches_processed += 1;
  ```

### Test Both Paths
- After fix, run both V1 and V2 scenario tests
- Verify stats now match for both processors
- Check that speedup numbers still look right (V2 should still be faster)

---

## Timeline

**Phase 6.4 (Next Phase)**
- [ ] Day 1: Profile V1 stats tracking to find root cause
- [ ] Day 2: Fix stats update logic in V1
- [ ] Day 3: Add unit tests for stats tracking
- [ ] Day 4: Validate across all 5 scenarios
- [ ] Day 5: Commit with comprehensive test coverage

**Phase 6.5 (Future)**
- [ ] Implement Prometheus metrics collection (current blocker removed)
- [ ] Add real-time stats monitoring
- [ ] Create production observability dashboard

---

## Related Issues

- **Prometheus Metrics**: Not implemented (waiting for stats to be correct first)
- **Data Correctness Validation**: Sampling works but results not validated
- **Performance Overhead**: 97% overhead still present (not related to stats bug)

---

## Notes for Phase 6.4 Team

1. **This is not critical for performance measurement**: Throughput calculations are still valid (based on elapsed time, not stats)
2. **V2 is working correctly**: Stats anomaly only affects V1, V2 stats are accurate
3. **Tests still pass**: The anomaly is discovered within validation code, not during test execution
4. **Easy to spot after fix**: Test output will clearly show stats now match expectations

---

## Checklist for Investigation

- [ ] Locate V1 process_job() implementation
- [ ] Find where stats should be updated
- [ ] Compare line-by-line with V2 (working version)
- [ ] Identify missing or incorrectly scoped stats updates
- [ ] Create fix with explanation
- [ ] Add debug logging to trace execution
- [ ] Run unit test to verify fix
- [ ] Run all 5 scenario tests to verify fix
- [ ] Document fix in commit message
- [ ] Update this tracking document with resolution
