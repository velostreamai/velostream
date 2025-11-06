# FR-082 Phase 5: Window Engine Integration Analysis

**Date**: November 6, 2025
**Status**: Analysis & Planning
**Scope**: Proper integration of window_v2 engine with Job Server V2 hash-partitioned architecture

## Executive Summary

The Phase 5 task is to integrate the existing `window_v2` engine (already complete with full EMIT CHANGES support) with the Job Server V2 hash-partitioned architecture. The mistake in the initial approach was duplicating window logic in PartitionStateManager instead of leveraging the existing, battle-tested window engine.

**Correct Architecture**: Per-partition watermark tracking + window_v2 engine delegation = complete Phase 5 support.

---

## Current Architecture Analysis

### 1. Window_V2 Engine (EXISTING - FULLY FUNCTIONAL)

**Location**: `src/velostream/sql/execution/window_v2/`

**Key Components**:
- **WindowAdapter** (`adapter.rs`): Integration layer bridging legacy processor interface with window_v2 strategies
- **WindowStrategy Trait** (`traits.rs`): Pluggable window implementations
  - TumblingWindowStrategy
  - SlidingWindowStrategy
  - SessionWindowStrategy
  - RowsWindowStrategy (already complete with 17 tests)
- **EmissionStrategy Trait** (`traits.rs`): Controls EMIT CHANGES vs EMIT FINAL behavior
  - EmitChangesStrategy: Emits on every record update
  - EmitFinalStrategy: Emits once per window at boundary
- **State Management**: All window state stored in `ProcessorContext.window_v2_states` (HashMap with type erasure)

**Current Integration Points**:
1. `QueryProcessor::process_query()` routes to `WindowProcessor` if query has WINDOW clause
2. `WindowProcessor::process_windowed_query_enhanced()` delegates to `WindowAdapter::process_with_v2()`
3. `WindowAdapter` creates window strategies and manages state lifecycle

**Capabilities Already Implemented**:
- ✅ ROWS window buffers with fixed memory bounds
- ✅ EMIT CHANGES mode for CDC-style continuous emission
- ✅ GROUP BY aggregations (AVG, MIN, MAX, COUNT, SUM, etc.)
- ✅ Late record handling with watermark-aware emission
- ✅ All 255+ tests passing
- ✅ Zero-copy Arc<StreamRecord> for efficient record passing

---

### 2. Job Server V2 Architecture (Phase 4 - PARTIALLY COMPLETE)

**Location**: `src/velostream/server/v2/`

**Key Components**:

#### PartitionStateManager
- **Responsibility**: Per-partition state management
- **Current Implementation**:
  - Partition metrics tracking ✅
  - Watermark management per partition ✅ (Phase 4)
  - Late record detection (Drop, ProcessWithWarning, ProcessAll) ✅

- **What It Should NOT Do**:
  - ❌ Duplicate window processing logic (window_v2 already handles this)
  - ❌ Manage window buffers independently
  - ❌ Implement emission strategies
  - ❌ Handle GROUP BY state

#### Coordinator & Hash Router
- Routes records to partitions by hash
- Collects results from partition processors

#### Current Processing Flow
```
Job Server Input
    ↓
Hash Router (partition key)
    ↓
PartitionStateManager (per partition)
    ├─ Metrics tracking
    ├─ Watermark update & late record check
    └─ → Forward to StreamExecutionEngine
         ↓
    StreamExecutionEngine
         ↓
    QueryProcessor
         ↓
    WindowProcessor (if WINDOW clause exists)
         ↓
    WindowAdapter
         ↓
    window_v2 strategies
         ↓
    Results with EMIT CHANGES
```

---

### 3. Batch Processing in Common Path (Phase 5 - PARTIALLY COMPLETE)

**Location**: `src/velostream/server/processors/common.rs`

**Current Implementation**:
- **EMIT CHANGES Path** (Lines 230-288):
  - Routes through `engine.execute_with_record()` for each record
  - Temporarily takes `output_receiver` to drain channel
  - Collects emitted results
  - Returns receiver to engine
  - ✅ Works correctly per Scenario 3B test

- **Standard Path** (Lines 290+):
  - Uses QueryProcessor with local state copies
  - Optimized for batch processing without per-record locks
  - Works for non-EMIT queries

---

## Problem: The Duplication

**What Was Added in Initial Phase 5 Attempt**:
```rust
// In PartitionStateManager
rows_window: Option<Arc<Mutex<RowsWindowStrategy>>>,
output_sender: Option<mpsc::UnboundedSender<StreamRecord>>,
```

**Why This Is Wrong**:
1. **Redundancy**: Window_v2 engine already manages window state completely
2. **Inconsistency**: Duplicating window logic creates two sources of truth
3. **Maintenance**: Any window feature update needs changes in two places
4. **Testing**: Tests would need to test window logic in two locations
5. **Complexity**: Per-partition window state + engine window state = confusion

**The Engine Already Handles All This**:
- Window buffer management via RowsWindowStrategy
- EMIT CHANGES emission via EmitChangesStrategy
- GROUP BY aggregations via AccumulatorManager
- Late record detection via watermarks
- State persistence in ProcessorContext

---

## Correct Architecture: Separation of Concerns

### What PartitionStateManager Should Do

**Partition-Level Concerns** (not window-specific):
1. **Metrics Tracking** ✅
   - Throughput per partition
   - Latency tracking
   - Backpressure detection

2. **Watermark Management** ✅ (Phase 4)
   - Update watermark from record.event_time
   - Detect late arrivals
   - Apply late-record strategy (Drop/ProcessWithWarning/ProcessAll)

3. **Query Routing** (to implement)
   - Route record to appropriate engine/query processor
   - Handle partition-specific query state if needed

### What WindowAdapter/Window_V2 Engine Does

**Window-Level Concerns** (generic, engine-owned):
1. **Window Strategy Execution**
   - Buffer management (ROWS, Tumbling, etc.)
   - Window boundary detection
   - Memory bounds enforcement

2. **Aggregation Computation**
   - GROUP BY state management
   - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
   - HAVING clause evaluation

3. **Emission Strategy**
   - EMIT CHANGES: every record update
   - EMIT FINAL: window boundary only
   - Result formatting

4. **State Persistence**
   - Stored in ProcessorContext.window_v2_states
   - Per-query state (not per-partition)
   - Generic, reusable across all use cases

---

## Proposed Integration Pattern

### Phase 5 Final Architecture

```
Record Input (from Kafka/File)
    ↓
Hash Router
    ├─ Extract partition_key (from record)
    └─ Route to partition handler
         ↓
PartitionStateManager.process_record()
    ├─ Update metrics
    │   └─ Track throughput, latency
    │
    ├─ Update watermark
    │   ├─ record.event_time → watermark_manager.update()
    │   └─ Check if late: late = watermark > event_time
    │
    └─ Check late-record strategy
        ├─ Drop: return Err() → skip record
        ├─ ProcessWithWarning: log warning, continue
        └─ ProcessAll: continue silently
         ↓
StreamExecutionEngine.execute_with_record()
    └─ QueryProcessor.process_query()
         └─ WindowProcessor.process_windowed_query_enhanced()
              └─ WindowAdapter.process_with_v2()
                   └─ window_v2 engine (handles ALL window logic)
                        ├─ WindowStrategy.add_record()
                        ├─ EmissionStrategy.process_record()
                        └─ AccumulatorManager (GROUP BY + aggregations)
                             ↓
                        Output channel (for EMIT CHANGES)
                             ↓
Batch processor drains channel
    └─ Collects output_records → writes to sink
```

---

## Key Insights

### 1. Window_V2 Already Handles Late Records
The window_v2 engine doesn't need per-partition watermark information because:
- **Watermark check happens BEFORE windowing** (in PartitionStateManager)
- Records that fail late-record check are dropped before reaching engine
- Engine only sees "valid" records (either on-time or allowed by strategy)

### 2. EMIT CHANGES Already Works
The existing common.rs batch processing already supports EMIT CHANGES:
- Routes through `execute_with_record()`
- Drains channel after each record
- Collects output_records
- **No per-partition window buffer needed**

### 3. Window State Is Query-Specific, Not Partition-Specific
- Window buffers maintain records from ALL partitions for the same query
- GroupBy state is query-scoped
- Window boundaries are time-based, not partition-based
- **Storing window state in PartitionStateManager would break this model**

### 4. Partitioning Happens ABOVE Window Processing
Hash partitioning is an optimization for metrics/watermarks, not for windows:
- Routes to partition handler (metrics, watermark)
- Then forwards to shared window engine
- Results aggregated across partitions in sink

---

## What Phase 5 Actually Needs

### 1. ✅ EMIT CHANGES Support (DONE)
- Already implemented in common.rs (Lines 230-288)
- Demonstrated working in Scenario 3B test (99,810 emissions)
- No additional work needed

### 2. ✅ Per-Partition Watermark Management (DONE - Phase 4)
- PartitionStateManager.watermark_manager (Phase 4)
- Late record handling (Drop/ProcessWithWarning/ProcessAll)
- Tested in phase4_system_fields_test

### 3. ✅ ROWS Window Support (ALREADY EXISTS)
- RowsWindowStrategy fully implemented (321 lines, 17 tests)
- Used via window_v2 engine
- Supports emit_per_record (EMIT CHANGES)
- No additional work needed

### 4. ❌ REMOVE: Duplicate Window Logic in PartitionStateManager
- Delete rows_window field
- Delete output_sender field
- Delete with_rows_window() constructor
- Delete window processing code (lines 239-277)
- Return to clean per-partition state management

### 5. ✅ Integration Points to Verify
- PartitionStateManager watermark → affects window_v2 processing
- EMIT CHANGES path uses execute_with_record() → triggers window_v2
- Results collected in common.rs batch processor → written to sink

---

## Testing Strategy

### Unit Tests
1. **PartitionStateManager Tests**
   - Watermark update and late-record detection
   - Metrics tracking accuracy
   - Backpressure detection

2. **Window_V2 Engine Tests** (Already comprehensive)
   - ROWS window buffering (17 tests)
   - EMIT CHANGES emission mode
   - GROUP BY + aggregations
   - Late record handling

3. **Integration Tests**
   - End-to-end: record → partition → watermark → window → emission
   - EMIT CHANGES with multiple groups
   - Late records with drop/warn/process strategies

### Performance Benchmarks (Phase 5 Goals)
- Target: 1.5M rec/sec on 8 cores (65x over Phase 1's 23K rec/sec)
- Baseline from Scenario 3B already exists (99,810 emissions working)
- Measure per-partition throughput, watermark overhead

---

## Implementation Checklist

- [ ] Revert duplicate window fields from PartitionStateManager
- [ ] Remove with_rows_window() constructor
- [ ] Remove window processing logic (lines 239-277)
- [ ] Clean up imports (remove window_v2 imports)
- [ ] Verify existing Phase 4 tests still pass
- [ ] Verify Scenario 3B EMIT CHANGES test still passes
- [ ] Create comprehensive Phase 5 integration tests
- [ ] Performance testing: 1.5M rec/sec target
- [ ] Update FR-082-SCHEDULE.md

---

## Files Affected

### To Revert
- `src/velostream/server/v2/partition_manager.rs`
  - Lines 10-15: Remove window_v2 imports
  - Lines 64-71: Remove rows_window and output_sender fields
  - Lines 133-168: Remove with_rows_window() constructor
  - Lines 239-277: Remove duplicate window processing code

### To Verify
- `src/velostream/server/processors/common.rs` - Already correctly implements EMIT CHANGES
- `src/velostream/sql/execution/window_v2/adapter.rs` - All window logic here
- `tests/performance/analysis/scenario_3b_tumbling_emit_changes_baseline.rs` - Verify still passes

### To Create
- `tests/unit/server/v2/phase5_window_integration_test.rs` - Integration tests
- Performance tests for 1.5M rec/sec target

---

## Summary

**Phase 5 is NOT about replicating window processing at the partition level.**
**Phase 5 is about ensuring the existing window_v2 engine works correctly with per-partition watermark tracking.**

The architecture is already complete:
- Window_v2 engine: ✅ All window types, EMIT CHANGES, aggregations
- EMIT CHANGES support: ✅ Implemented in common.rs
- Per-partition watermarks: ✅ Implemented in Phase 4
- Late record handling: ✅ In PartitionStateManager

The remaining work is:
1. Remove duplicate window logic from PartitionStateManager
2. Create integration tests proving window + partition + watermark work together
3. Measure performance and optimize to 1.5M rec/sec target