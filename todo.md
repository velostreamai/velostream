# Velostream Active Development TODO

**Last Updated**: October 7, 2025
**Status**: 🚨 **CRITICAL BUG** - Multi-Source Sink Writing Not Working
**Current Priority**: **🔥 FIX: Processor Architecture Refactor for Low-Latency Sink Writes**

**Related Files**:
- 📋 **Archive**: [todo-consolidated.md](todo-consolidated.md) - Full historical TODO with completed work
- ✅ **Completed**: [todo-complete.md](todo-complete.md) - Successfully completed features

---

## 🎯 **CURRENT STATUS & NEXT PRIORITIES**

### **🚨 CRITICAL BUG: Multi-Source Processor Sink Writing Not Working - October 7, 2025**

**Identified**: October 7, 2025
**Priority**: **🔥 CRITICAL** - Blocking multi-source stream processing
**Status**: ❌ **BROKEN** - Records processed but never written to sinks
**Test**: `tests/unit/server/processors/multi_source_sink_write_test.rs` (FAILING)
**Impact**: Multi-source jobs process records but produce NO OUTPUT

#### **Root Cause Analysis**

**The Bug**: `process_batch_with_output()` uses wrong execution path
**File**: `src/velostream/server/processors/common.rs:196-258`

```rust
// CURRENT (BROKEN) - Line 226
match engine_lock.execute_with_record(query, record.clone()).await {
    Ok(()) => {
        records_processed += 1;
        output_records.push(record);  // ← WRONG! Pushes INPUT, not SQL output
    }
```

**What happens**:
1. ✅ `execute_with_record()` processes record through SQL engine
2. ✅ SQL result sent to `engine.output_sender` (for terminal display)
3. ❌ `process_batch_with_output()` ignores `output_sender` and pushes **input record** to `output_records`
4. ❌ Sinks receive input records instead of SQL query results
5. ❌ For transformations (aggregations, projections), sinks get WRONG data

**Architecture Confusion**:
- **Path 1 (Terminal/Display)**: `execute_with_record()` → `output_sender` → Terminal
- **Path 2 (Sink Writes)**: Should be `QueryProcessor::process_query()` → `ProcessorResult.record` → Sinks
- **Current code incorrectly mixes both paths**

#### **Why This Matters for GROUP BY/Windows**

**Question**: If we bypass `output_sender`, how do GROUP BY aggregates work?

**Answer**: ✅ **They work PERFECTLY** because they use `ProcessorContext.group_by_states`, NOT `output_sender`

**GROUP BY State Management**:
```rust
// File: src/velostream/sql/execution/processors/context.rs:33
pub struct ProcessorContext {
    /// GROUP BY processing state
    pub group_by_states: HashMap<String, GroupByState>,
    // ...
}

// File: src/velostream/sql/execution/engine.rs:353-358
// Share state with processor
context.group_by_states = self.group_states.clone();
let result = QueryProcessor::process_query(query, record, &mut context)?;
// Sync state back
self.group_states = std::mem::take(&mut context.group_by_states);
```

**GROUP BY Emission Modes** (File: `src/velostream/sql/execution/processors/select.rs:952-984`):

1. **EMIT CHANGES (Default)**: Returns result on EVERY record via `ProcessorResult.record`
   ```rust
   EmitMode::Changes => {
       Ok(ProcessorResult {
           record: Some(final_record),  // ← RETURNED, not sent to output_sender
           should_count: true,
       })
   }
   ```

2. **EMIT FINAL**: Accumulates state, returns `None` until explicit flush
   ```rust
   EmitMode::Final => {
       Ok(ProcessorResult {
           record: None,  // ← No emission per-record
           should_count: false,
       })
   }
   ```

**When `output_sender` IS used**:
- ✅ Explicit `flush_group_by_results()` calls (engine.rs:972-1157)
- ✅ Terminal/CLI display output
- ✅ Window close triggers
- ❌ **NOT used for batch processing sink writes**

#### **Implementation Plan: Low-Latency Direct Processing**

**Strategy**: Replace `execute_with_record()` with direct `QueryProcessor::process_query()` calls

**Benefits**:
- ✅ Eliminates engine lock contention
- ✅ Removes channel overhead
- ✅ Direct path: input → SQL → sink (minimal latency)
- ✅ Correct SQL results (not input passthroughs)
- ✅ GROUP BY/Windows work via `ProcessorContext` state
- ✅ Matches high-performance pattern at engine.rs:1231-1237

---

### **Phase 1: Core Refactor (High Priority - 2 days)**

**Goal**: Fix `process_batch_with_output()` to use direct processor calls

#### **Task 1.1: Add State Accessors to Engine**
**File**: `src/velostream/sql/execution/engine.rs`
**Lines**: Add after line 200

```rust
impl StreamExecutionEngine {
    /// Get GROUP BY states for external processing
    pub fn get_group_states(&self) -> &HashMap<String, GroupByState> {
        &self.group_states
    }

    /// Set GROUP BY states after external processing
    pub fn set_group_states(&mut self, states: HashMap<String, GroupByState>) {
        self.group_states = states;
    }

    /// Get window states for external processing
    pub fn get_window_states(&self) -> &Vec<(String, WindowState)> {
        // Access from context or engine storage
        &self.persistent_window_states
    }

    /// Set window states after external processing
    pub fn set_window_states(&mut self, states: Vec<(String, WindowState)>) {
        self.persistent_window_states = states;
    }
}
```

**Deliverables**:
- [ ] Add 4 state accessor methods
- [ ] Add unit tests for state get/set operations
- [ ] Document thread-safety considerations

---

#### **Task 1.2: Refactor `process_batch_with_output()`**
**File**: `src/velostream/server/processors/common.rs`
**Lines**: Replace lines 196-258

```rust
/// Process a batch of records and capture SQL engine output for sink writing
/// Uses direct QueryProcessor calls for low-latency processing
pub async fn process_batch_with_output(
    batch: Vec<StreamRecord>,
    engine: &Arc<tokio::sync::Mutex<StreamExecutionEngine>>,
    query: &StreamingQuery,
    job_name: &str,
) -> BatchProcessingResultWithOutput {
    let batch_start = Instant::now();
    let batch_size = batch.len();
    let mut records_processed = 0;
    let mut records_failed = 0;
    let mut error_details = Vec::new();
    let mut output_records = Vec::new();

    // Get shared state ONCE at batch start (minimal lock time)
    let (mut group_states, mut window_states) = {
        let engine_lock = engine.lock().await;
        (
            engine_lock.get_group_states().clone(),
            engine_lock.get_window_states().clone(),
        )
    };

    // Generate query ID for state management
    let query_id = generate_query_id(query);

    // Process batch WITHOUT holding engine lock
    for (index, record) in batch.into_iter().enumerate() {
        // Create lightweight context with shared state
        let mut context = ProcessorContext::new();
        context.group_by_states = group_states.clone();
        context.persistent_window_states = window_states.clone();

        // Direct processing (no engine lock, no output_sender)
        match QueryProcessor::process_query(query, &record, &mut context) {
            Ok(result) => {
                records_processed += 1;

                // Collect outputs for sink writing (ACTUAL SQL results)
                if let Some(output) = result.record {
                    output_records.push(output);
                }

                // Update shared state for next iteration
                group_states = context.group_by_states;
                window_states = context.persistent_window_states;
            }
            Err(e) => {
                records_failed += 1;
                error_details.push(ProcessingError {
                    record_index: index,
                    error_message: format!("{:?}", e),
                    recoverable: is_recoverable_error(&e),
                });
                warn!(
                    "Job '{}' failed to process record {}: {:?}",
                    job_name, index, e
                );
            }
        }
    }

    // Sync state back to engine ONCE at batch end
    {
        let mut engine_lock = engine.lock().await;
        engine_lock.set_group_states(group_states);
        engine_lock.set_window_states(window_states);
    }

    BatchProcessingResultWithOutput {
        records_processed,
        records_failed,
        processing_time: batch_start.elapsed(),
        batch_size,
        error_details,
        output_records,  // ← CORRECT SQL results for sink writes!
    }
}

/// Generate a consistent query ID for state management
fn generate_query_id(query: &StreamingQuery) -> String {
    match query {
        StreamingQuery::Select { from, window, .. } => {
            let base = format!(
                "select_{}",
                match from {
                    StreamSource::Stream(name) | StreamSource::Table(name) => name,
                    StreamSource::Uri(uri) => uri,
                    StreamSource::Subquery(_) => "subquery",
                }
            );
            if window.is_some() {
                format!("{}_windowed", base)
            } else {
                base
            }
        }
        StreamingQuery::CreateStream { name, .. } => format!("create_stream_{}", name),
        StreamingQuery::CreateTable { name, .. } => format!("create_table_{}", name),
        _ => "unknown_query".to_string(),
    }
}
```

**Key Changes**:
1. **Line 209-215**: Get state once, minimize lock time
2. **Line 221**: Create lightweight context (no engine dependency)
3. **Line 226**: Direct `QueryProcessor::process_query()` call (no `execute_with_record()`)
4. **Line 232**: Collect **ACTUAL** SQL results (not input records)
5. **Line 238-239**: Update shared state for next iteration
6. **Line 250-254**: Sync state back once at end

**Deliverables**:
- [ ] Refactor `process_batch_with_output()` (complete rewrite)
- [ ] Add `generate_query_id()` helper function
- [ ] Remove placeholder comments about "TODO: capture actual SQL output"
- [ ] Update all call sites (verify no breaking changes)

---

#### **Task 1.3: Update Test to Verify Fix**
**File**: `tests/unit/server/processors/multi_source_sink_write_test.rs`
**Lines**: Update assertions at lines 252-266

```rust
// CRITICAL: Verify sink writes (this is what was missing and caused the bug)
let written_count = writer_clone.get_written_count();
println!("Records written to sink: {}", written_count);

assert!(
    written_count > 0,
    "REGRESSION: Records were processed (stats.records_processed={}) but NOT written to sink! \
     This is the bug we fixed - processor must write output to sinks.",
    stats.records_processed
);

// Ideally, all processed records should be written (for simple passthrough queries)
assert_eq!(
    written_count, stats.records_processed as usize,
    "All processed records should be written to sink"
);

// NEW: Verify records are SQL OUTPUT, not input passthrough
let written_records = writer_clone.get_written_records();
for (i, record) in written_records.iter().enumerate() {
    // For SELECT * queries, output should match input
    // But verify it went through SQL processing
    assert!(record.fields.len() > 0, "Record {} should have fields", i);
    debug!("Written record {}: {:?}", i, record);
}
```

**Deliverables**:
- [ ] Add detailed assertion messages
- [ ] Add debug logging for written records
- [ ] Verify test PASSES after refactor
- [ ] Add MockDataWriter method to get written records (if needed)

---

### **Phase 2: Comprehensive Testing (2 days)**

**Goal**: Ensure refactor works for all query types

#### **Task 2.1: Unit Tests for Direct Processing**
**File**: `tests/unit/server/processors/direct_processing_test.rs` (NEW)

**Test Cases**:
```rust
#[tokio::test]
async fn test_simple_select_direct_processing() {
    // Given: SELECT * FROM source query
    // When: process_batch_with_output() called
    // Then: Output records match SQL result
}

#[tokio::test]
async fn test_group_by_emit_changes_direct_processing() {
    // Given: SELECT COUNT(*) FROM source GROUP BY field WITH (EMIT = CHANGES)
    // When: Processing 10 records with 3 groups
    // Then: 10 output records (one per input, updated aggregates)
}

#[tokio::test]
async fn test_group_by_emit_final_direct_processing() {
    // Given: SELECT COUNT(*) FROM source GROUP BY field WITH (EMIT = FINAL)
    // When: Processing 10 records
    // Then: 0 output records (state accumulated, no emission)
}

#[tokio::test]
async fn test_window_aggregation_direct_processing() {
    // Given: SELECT COUNT(*) FROM source WINDOW TUMBLING(5 SECONDS)
    // When: Processing records in window
    // Then: No output until window closes
}

#[tokio::test]
async fn test_projection_query_direct_processing() {
    // Given: SELECT field1, field2 * 2 AS doubled FROM source
    // When: Processing records
    // Then: Output has only projected fields with transformation
}

#[tokio::test]
async fn test_filter_query_direct_processing() {
    // Given: SELECT * FROM source WHERE value > 100
    // When: Processing 10 records (5 match filter)
    // Then: 5 output records
}

#[tokio::test]
async fn test_state_synchronization_across_batches() {
    // Given: GROUP BY query with EMIT CHANGES
    // When: Processing 3 batches with same group keys
    // Then: Aggregates accumulate correctly across batches
}

#[tokio::test]
async fn test_error_handling_preserves_state() {
    // Given: Batch with 1 failing record in middle
    // When: Processing batch
    // Then: State preserved, subsequent records processed correctly
}
```

**Deliverables**:
- [ ] Create 8+ unit tests for direct processing
- [ ] Test all query types (SELECT, GROUP BY, WINDOW, projections, filters)
- [ ] Verify state management across batches
- [ ] Test error scenarios

---

#### **Task 2.2: Integration Tests for Multi-Source**
**File**: `tests/integration/multi_source_sink_integration_test.rs` (NEW)

**Test Cases**:
```rust
#[tokio::test]
async fn test_multi_source_simple_union() {
    // Given: 2 Kafka sources with different data
    // When: SELECT * FROM source1 UNION SELECT * FROM source2
    // Then: All records written to sink
}

#[tokio::test]
async fn test_multi_source_aggregation() {
    // Given: 3 sources with numeric data
    // When: SELECT source, SUM(value) FROM sources GROUP BY source
    // Then: Aggregated results written to sink
}

#[tokio::test]
async fn test_multi_sink_fanout() {
    // Given: 1 source, 3 sinks
    // When: Processing records
    // Then: All sinks receive all records
}

#[tokio::test]
async fn test_backpressure_handling() {
    // Given: Fast source, slow sink
    // When: Processing large batch
    // Then: Batches processed without loss, backpressure respected
}
```

**Deliverables**:
- [ ] Create 4+ integration tests
- [ ] Test multi-source scenarios
- [ ] Test multi-sink scenarios
- [ ] Verify backpressure handling

---

#### **Task 2.3: Performance Benchmarks**
**File**: `tests/performance/batch_processing_benchmark.rs`

**Benchmarks**:
```rust
#[bench]
fn bench_old_execute_with_record_path(b: &mut Bencher) {
    // Measure old path: execute_with_record() + engine lock
}

#[bench]
fn bench_new_direct_processing_path(b: &mut Bencher) {
    // Measure new path: direct QueryProcessor calls
}

#[bench]
fn bench_group_by_state_sync(b: &mut Bencher) {
    // Measure state sync overhead
}

#[bench]
fn bench_large_batch_processing(b: &mut Bencher) {
    // 10K records with GROUP BY
}
```

**Success Criteria**:
- Direct processing ≥ 2x faster (no lock contention)
- State sync overhead < 5% of batch time
- Large batches (10K+ records) scale linearly

**Deliverables**:
- [ ] Create 4 performance benchmarks
- [ ] Compare old vs new implementation
- [ ] Document performance improvements
- [ ] Verify linear scaling

---

### **Phase 3: Documentation & Cleanup (1 day)**

#### **Task 3.1: Update Architecture Documentation**
**File**: `docs/architecture/processor-execution-flow.md` (NEW)

**Sections**:
```markdown
# Processor Execution Flow

## Two Distinct Execution Paths

### Path 1: Terminal/Interactive (uses output_sender)
- **Purpose**: CLI query results, REPL display
- **Flow**: User Query → engine.execute_with_record() → output_sender → Terminal Display
- **Use Cases**: Interactive queries, debugging, testing

### Path 2: Batch Processing (direct QueryProcessor)
- **Purpose**: Multi-source stream processing, sink writes
- **Flow**: Batch → QueryProcessor::process_query() → ProcessorResult.record → Sink Writes
- **Use Cases**: Production pipelines, low-latency processing

## State Management

### GROUP BY State
- **Storage**: ProcessorContext.group_by_states
- **Scope**: Query-specific, persisted across batches
- **Synchronization**: Clone on batch start, sync back on batch end

### Window State
- **Storage**: ProcessorContext.persistent_window_states
- **Scope**: Query-specific, time-based
- **Synchronization**: Same pattern as GROUP BY

## Performance Characteristics

| Aspect | Path 1 (Interactive) | Path 2 (Batch) |
|--------|---------------------|----------------|
| **Latency** | Medium (lock + channel) | Low (direct) |
| **Throughput** | Low (sequential) | High (batched) |
| **Lock Contention** | High | Minimal (2 locks per batch) |
| **Use Case** | CLI, debugging | Production pipelines |
```

**Deliverables**:
- [ ] Create architecture documentation
- [ ] Diagram execution paths
- [ ] Document state management patterns
- [ ] Add performance characteristics

---

#### **Task 3.2: Update Code Comments**
**Files**:
- `src/velostream/server/processors/common.rs`
- `src/velostream/server/processors/simple.rs`
- `src/velostream/sql/execution/engine.rs`

**Updates**:
- Remove placeholder TODOs about "capture actual SQL output"
- Add comments explaining direct processing rationale
- Document state synchronization pattern
- Add examples for common query types

**Deliverables**:
- [ ] Remove 5+ obsolete TODO comments
- [ ] Add 10+ explanatory comments
- [ ] Update module-level documentation

---

#### **Task 3.3: Update CLAUDE.md**
**File**: `CLAUDE.md`

Add section:
```markdown
## Processor Architecture: Direct vs Interactive Processing

### Two Execution Paths

**Interactive Path** (CLI, REPL, testing):
```rust
engine.execute_with_record(query, record).await?;
// Results sent to output_sender for display
```

**Batch Processing Path** (production, low-latency):
```rust
let result = QueryProcessor::process_query(query, &record, &mut context)?;
if let Some(output) = result.record {
    write_to_sink(output).await?;
}
```

### When to Use Each Path

- **Use Interactive Path**:
  - CLI/REPL query execution
  - Unit tests checking SQL correctness
  - Debugging query behavior

- **Use Batch Processing Path**:
  - Multi-source stream processing
  - High-throughput pipelines
  - Low-latency requirements
  - Sink writing operations

### State Management Pattern

GROUP BY and Window aggregations use `ProcessorContext` for state:

```rust
// Get state once
let mut group_states = engine.lock().await.get_group_states().clone();

// Process batch
for record in batch {
    let mut context = ProcessorContext::new();
    context.group_by_states = group_states.clone();

    let result = QueryProcessor::process_query(query, &record, &mut context)?;

    group_states = context.group_by_states;  // Update for next iteration
}

// Sync back once
engine.lock().await.set_group_states(group_states);
```
```

**Deliverables**:
- [ ] Add processor architecture section to CLAUDE.md
- [ ] Document when to use each path
- [ ] Add state management examples
- [ ] Update testing guidelines

---

### **Phase 4: Remove output_receiver Parameter (Optional Cleanup - 1 day)**

**Goal**: Clean up vestigial `output_receiver` parameter

#### **Task 4.1: Remove Unused Parameter**
**Files**:
- `src/velostream/server/processors/simple.rs:39`
- `src/velostream/server/processors/transactional.rs:47`

**Change**:
```rust
// BEFORE
pub async fn process_multi_job(
    &self,
    readers: HashMap<String, Box<dyn DataReader>>,
    writers: HashMap<String, Box<dyn DataWriter>>,
    engine: Arc<Mutex<StreamExecutionEngine>>,
    query: StreamingQuery,
    job_name: String,
    mut shutdown_rx: mpsc::Receiver<()>,
    _output_receiver: mpsc::UnboundedReceiver<StreamRecord>,  // ← UNUSED
) -> Result<JobExecutionStats, ...> {

// AFTER
pub async fn process_multi_job(
    &self,
    readers: HashMap<String, Box<dyn DataReader>>,
    writers: HashMap<String, Box<dyn DataWriter>>,
    engine: Arc<Mutex<StreamExecutionEngine>>,
    query: StreamingQuery,
    job_name: String,
    mut shutdown_rx: mpsc::Receiver<()>,
    // Removed: output_receiver no longer needed for batch processing
) -> Result<JobExecutionStats, ...> {
```

**Deliverables**:
- [ ] Remove `_output_receiver` parameter from both processors
- [ ] Update all call sites (tests, job server, etc.)
- [ ] Add migration note in CHANGELOG
- [ ] Verify backward compatibility

---

### **Success Metrics**

| Metric | Current (Broken) | Target | Measurement |
|--------|------------------|--------|-------------|
| **Test Pass Rate** | 0% (FAILING) | 100% | `multi_source_sink_write_test.rs` passes |
| **Sink Write Correctness** | 0% (input records) | 100% (SQL results) | Output records match SQL query results |
| **Latency** | High (lock + channel) | Low (direct) | ≥2x faster batch processing |
| **Lock Contention** | High (per-record) | Minimal (per-batch) | 2 locks per batch vs N locks per batch |
| **GROUP BY Correctness** | Unknown | 100% | Aggregates work across batches |
| **State Sync Overhead** | N/A | < 5% | State sync time / total batch time |

---

### **Timeline & Milestones**

| Phase | Duration | Completion Date | Key Deliverable |
|-------|----------|-----------------|-----------------|
| **Phase 1: Core Refactor** | 2 days | Oct 9, 2025 | `process_batch_with_output()` fixed |
| **Phase 2: Testing** | 2 days | Oct 11, 2025 | 12+ tests passing |
| **Phase 3: Documentation** | 1 day | Oct 12, 2025 | Architecture docs complete |
| **Phase 4: Cleanup** | 1 day | Oct 13, 2025 | Parameter removal (optional) |

**Total**: 5-6 days (depending on Phase 4)

---

### **Risk Assessment**

🟡 **Medium Risk**:
- Breaking change to core execution path
- State management complexity
- Potential regressions in GROUP BY/Window queries

**Mitigation**:
- ✅ Comprehensive test suite (20+ tests)
- ✅ Performance benchmarks to verify improvements
- ✅ Gradual rollout (Phase 1 → Phase 2 → Phase 3)
- ✅ Detailed documentation for maintenance

**Rollback Plan**:
- Keep old `execute_with_record()` path available
- Feature flag for direct processing
- Comprehensive regression testing

---

### **Implementation References**

**Key Files**:
- `src/velostream/server/processors/common.rs:196-258` - Core refactor location
- `src/velostream/sql/execution/engine.rs:353-358` - State sync pattern
- `src/velostream/sql/execution/processors/select.rs:668-984` - GROUP BY emission
- `src/velostream/sql/execution/processors/context.rs:33` - State storage
- `tests/unit/server/processors/multi_source_sink_write_test.rs` - Verification test

**Existing Patterns**:
- `engine.rs:1231-1237` - Direct QueryProcessor usage (high-performance path)
- `engine.rs:1352-1360` - Writer integration example
- `simple.rs:449-563` - Current batch processing (to be fixed)

---

### **✅ Recent Completions - October 6, 2025**
- ✅ **HAVING Clause Enhancement Complete**: Phases 1-4 implemented (11,859 errors → 0)
  - ✅ Phase 1: BinaryOp support (arithmetic operations in HAVING)
  - ✅ Phase 2: Column alias support (reference SELECT aliases)
  - ✅ Phase 3: CASE expression support (conditional logic)
  - ✅ Phase 4: Enhanced args_match (complex expression matching)
  - ✅ Added 12 comprehensive unit tests (all passing)
  - ✅ ~350 lines production code + extensive test coverage
- ✅ **Demo Resilience**: Automated startup and health checking scripts
- ✅ **SQL Validation**: Financial trading demo validates successfully
- ✅ **100% Query Success**: All 8 trading queries execute without errors

### **Previous Completions - September 27, 2024**
- ✅ **Test Failures Resolved**: Both `test_optimized_aggregates` and `test_error_handling` fixed
- ✅ **OptimizedTableImpl Complete**: Production-ready with enterprise performance (1.85M+ lookups/sec)
- ✅ **Phase 2 CTAS**: All 65 CTAS tests passing with comprehensive validation
- ✅ **Reserved Keywords Fixed**: STATUS, METRICS, PROPERTIES now usable as field names

*Full details moved to [todo-complete.md](todo-complete.md)*

---

---

## ✅ **RESOLVED: HAVING Clause Enhancement**

**Status**: ✅ **COMPLETED** October 6, 2025
**Issue**: GitHub #75
**Solution**: Phases 1-4 implementation (11,859 errors → 0)

See "Recent Completions" section above for full details.

---

## 🚨 **CRITICAL GAP: Event-Time Extraction Not Implemented (ALL Data Sources)**

**Identified**: October 7, 2025
**Priority**: **CRITICAL** - Phase 1B watermarks feature incomplete across ALL data sources
**Status**: ❌ **NOT IMPLEMENTED** - Configuration accepted but no extraction logic
**Risk Level**: 🔴 **HIGH** - Documented feature not working, demo using processing-time instead of event-time
**Impact**: Financial trading demo and all event-time applications using wrong timestamps
**Scope**: **ALL DATA SOURCES** - Kafka, File, HTTP, SQL, S3, etc.

### **Problem Statement**

The Phase 1B watermarks feature is **partially implemented**:
- ✅ Watermark infrastructure exists and works correctly
- ✅ Configuration properties (`event.time.field`, `event.time.format`) are accepted
- ✅ WatermarkManager checks `record.event_time` field
- ❌ **NO DATA SOURCE extracts event-time from record fields** (all hardcoded to `None`)
- ❌ **Field extraction logic not implemented anywhere**
- ❌ **Timestamp format parsing not implemented**
- ❌ **Generic extraction trait missing** (should work for Kafka, File, HTTP, etc.)

**Current Behavior**:
```rust
// src/velostream/datasource/kafka/reader.rs:660-669
Ok(StreamRecord {
    fields,
    timestamp: message.timestamp()...,
    offset: message.offset(),
    partition: message.partition(),
    headers: message.take_headers().into_map(),
    event_time: None,  // ← ALWAYS None! Configuration ignored
})
```

**Expected Behavior**:
```rust
// Should extract from fields based on config
let event_time = if let Some(field_name) = config.get("event.time.field") {
    extract_and_parse_timestamp(&fields, field_name, config.get("event.time.format"))
} else {
    None
};

Ok(StreamRecord {
    fields,
    timestamp: message.timestamp()...,
    event_time,  // ← Populated from message data
    // ...
})
```

### **Impact Analysis**

**Production Impact**:
```
DOCUMENTED (docs/sql/watermarks-time-semantics.md):
  'event.time.field' = 'timestamp',
  'event.time.format' = 'epoch_millis'

ACTUAL BEHAVIOR:
  → Config accepted (no error)
  → Watermarks enabled
  → BUT uses processing-time instead of event-time!
  → Late data detection WRONG
  → Window emissions based on WRONG timestamps
```

**Financial Trading Demo**:
- Demo configured with `'event.time.field' = 'timestamp'`
- Demo expects event-time windowing
- **Actually using processing-time** (data arrival time, not trade execution time)
- Market data analysis timestamps are INCORRECT
- Risk calculations based on WRONG timing

**User Trust Issue**:
- Feature is fully documented as working
- Configuration is silently accepted
- No error/warning that it's not implemented
- Users believe event-time is working when it's not

### **Gap Analysis**

#### **Code References**

**Hardcoded None Assignment (ALL Data Sources)**:
- `src/velostream/datasource/kafka/reader.rs:668` - Kafka: `event_time: None` (ALWAYS)
- File readers: Also hardcoded to `None` (same pattern)
- All data sources share this problem - **NONE extract event-time**
- No extraction logic exists in ANY data source

**Configuration Detection (Works)**:
- `src/velostream/server/stream_job_server.rs:1117` - Checks for `event.time.field` config
- Enables watermarks correctly

**Watermark Infrastructure (Works)**:
- `src/velostream/sql/execution/watermarks.rs:252` - Checks `record.event_time`
- `src/velostream/sql/execution/watermarks.rs:290` - Falls back to processing-time
- Infrastructure correctly handles event_time when present

**Missing Implementation (Generic Across All Sources)**:
- ❌ **Generic timestamp extraction trait/module**
- ❌ Field extraction from `record.fields` HashMap
- ❌ Timestamp format parsing (`epoch_millis`, `ISO8601`, custom formats)
- ❌ Error handling for missing/invalid timestamps
- ❌ Configuration validation
- ❌ Integration in Kafka, File, HTTP, SQL data sources

#### **Documentation Status**

**Documented as Working**:
- ✅ `docs/sql/watermarks-time-semantics.md` - Full guide with examples (354 lines)
- ✅ Lines 130-132: SQL configuration example
- ✅ Lines 168-170: WITH clause example
- ✅ Complete API documentation for event-time semantics

**Documentation Gap**:
- ❌ No mention that extraction is NOT implemented
- ❌ No warning about processing-time fallback
- ❌ Examples show configurations that don't work

**Demo Files Using It**:
- ✅ `demo/trading/sql/financial_trading.sql` - 3 queries configured (lines 31-32, 286-287)
- ❌ Demo believes it's using event-time but uses processing-time

#### **Testing Gaps**

**Tests That Exist** ✅:
- `tests/unit/sql/execution/phase_1b_watermarks_test.rs` - 10+ watermark tests
- Tests manually create records with `event_time: Some(...)`
- Tests verify watermark logic works when event_time is present
- **BUT no tests for extraction from fields**

**Tests That Don't Exist** ❌:
- ❌ Kafka reader event-time extraction from message fields
- ❌ Timestamp format parsing (`epoch_millis`, `ISO8601`, etc.)
- ❌ Configuration-driven field extraction
- ❌ Error handling for invalid timestamps
- ❌ Integration test: Kafka message → event_time populated
- ❌ End-to-end test: event.time.field config → watermarks use event-time

**Test File Locations**:
- Unit tests needed in: `tests/unit/datasource/kafka/event_time_extraction_test.rs` (NEW)
- Integration tests needed in: `tests/integration/kafka/event_time_integration_test.rs` (NEW)

### **Implementation Requirements**

#### **Phase 1: Generic Event-Time Extraction Infrastructure** (Estimated: 4 days)

**New Module**: `src/velostream/datasource/event_time.rs` (Generic for ALL sources)

```rust
/// Generic event-time extraction module
/// Used by Kafka, File, HTTP, SQL, and all other data sources

/// Timestamp format enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum TimestampFormat {
    /// Unix epoch milliseconds (1696723200000)
    EpochMillis,
    /// Unix epoch seconds (1696723200)
    EpochSeconds,
    /// ISO 8601 format (2023-10-08T00:00:00Z)
    ISO8601,
    /// Custom chrono format string
    Custom(String),
}

impl TimestampFormat {
    pub fn parse(s: &str) -> Result<Self, EventTimeError> {
        match s {
            "epoch_millis" => Ok(TimestampFormat::EpochMillis),
            "epoch_seconds" | "epoch" => Ok(TimestampFormat::EpochSeconds),
            "iso8601" | "ISO8601" => Ok(TimestampFormat::ISO8601),
            custom => Ok(TimestampFormat::Custom(custom.to_string())),
        }
    }
}

/// Configuration for event-time extraction
#[derive(Debug, Clone)]
pub struct EventTimeConfig {
    /// Field name to extract timestamp from
    pub field_name: String,
    /// Format of the timestamp
    pub format: Option<TimestampFormat>,
}

impl EventTimeConfig {
    pub fn from_properties(properties: &HashMap<String, String>) -> Option<Self> {
        let field_name = properties.get("event.time.field")?.clone();
        let format = properties.get("event.time.format")
            .map(|s| TimestampFormat::parse(s))
            .transpose()
            .ok()?;

        Some(EventTimeConfig { field_name, format })
    }
}

/// Generic event-time extraction from StreamRecord fields
/// Works for ANY data source (Kafka, File, HTTP, SQL, etc.)
pub fn extract_event_time(
    fields: &HashMap<String, FieldValue>,
    config: &EventTimeConfig,
) -> Result<DateTime<Utc>, EventTimeError> {
    let field_value = fields.get(&config.field_name)
        .ok_or_else(|| EventTimeError::MissingField {
            field: config.field_name.clone(),
            available_fields: fields.keys().cloned().collect(),
        })?;

    let datetime = match &config.format {
        Some(TimestampFormat::EpochMillis) => {
            let millis = field_value.as_integer()
                .map_err(|_| EventTimeError::TypeMismatch {
                    field: config.field_name.clone(),
                    expected: "Integer (epoch millis)",
                    actual: field_value.type_name(),
                })?;
            DateTime::from_timestamp_millis(millis)
                .ok_or(EventTimeError::InvalidTimestamp {
                    value: format!("{}", millis),
                    format: "epoch_millis",
                })?
        }
        Some(TimestampFormat::EpochSeconds) => {
            let secs = field_value.as_integer()
                .map_err(|_| EventTimeError::TypeMismatch {
                    field: config.field_name.clone(),
                    expected: "Integer (epoch seconds)",
                    actual: field_value.type_name(),
                })?;
            DateTime::from_timestamp(secs, 0)
                .ok_or(EventTimeError::InvalidTimestamp {
                    value: format!("{}", secs),
                    format: "epoch_seconds",
                })?
        }
        Some(TimestampFormat::ISO8601) => {
            let s = field_value.as_string()
                .map_err(|_| EventTimeError::TypeMismatch {
                    field: config.field_name.clone(),
                    expected: "String (ISO 8601)",
                    actual: field_value.type_name(),
                })?;
            DateTime::parse_from_rfc3339(s)
                .map_err(|e| EventTimeError::ParseError {
                    value: s.clone(),
                    format: "ISO8601",
                    error: e.to_string(),
                })?
                .with_timezone(&Utc)
        }
        Some(TimestampFormat::Custom(fmt)) => {
            let s = field_value.as_string()
                .map_err(|_| EventTimeError::TypeMismatch {
                    field: config.field_name.clone(),
                    expected: format!("String ({})", fmt),
                    actual: field_value.type_name(),
                })?;
            NaiveDateTime::parse_from_str(s, fmt)
                .map_err(|e| EventTimeError::ParseError {
                    value: s.clone(),
                    format: fmt.clone(),
                    error: e.to_string(),
                })?
                .and_local_timezone(Utc)
                .single()
                .ok_or(EventTimeError::AmbiguousTimezone {
                    value: s.clone(),
                })?
        }
        None => {
            // Auto-detect: try integer (epoch millis), then ISO 8601
            auto_detect_timestamp(field_value, &config.field_name)?
        }
    };

    Ok(datetime)
}

/// Auto-detect timestamp format from field value
fn auto_detect_timestamp(
    field_value: &FieldValue,
    field_name: &str,
) -> Result<DateTime<Utc>, EventTimeError> {
    // Try integer (epoch millis) first
    if let Ok(millis) = field_value.as_integer() {
        if let Some(dt) = DateTime::from_timestamp_millis(millis) {
            return Ok(dt);
        }
    }

    // Try string (ISO 8601)
    if let Ok(s) = field_value.as_string() {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(dt.with_timezone(&Utc));
        }
    }

    Err(EventTimeError::AutoDetectFailed {
        field: field_name.to_string(),
        value_type: field_value.type_name(),
    })
}

/// Event-time extraction errors
#[derive(Debug, thiserror::Error)]
pub enum EventTimeError {
    #[error("Field '{field}' not found in record. Available fields: {}", available_fields.join(", "))]
    MissingField {
        field: String,
        available_fields: Vec<String>,
    },

    #[error("Type mismatch for field '{field}': expected {expected}, got {actual}")]
    TypeMismatch {
        field: String,
        expected: &'static str,
        actual: &'static str,
    },

    #[error("Invalid timestamp value '{value}' for format '{format}'")]
    InvalidTimestamp {
        value: String,
        format: &'static str,
    },

    #[error("Failed to parse '{value}' as {format}: {error}")]
    ParseError {
        value: String,
        format: String,
        error: String,
    },

    #[error("Ambiguous timezone for value '{value}'")]
    AmbiguousTimezone {
        value: String,
    },

    #[error("Auto-detect failed for field '{field}' with type {value_type}")]
    AutoDetectFailed {
        field: String,
        value_type: &'static str,
    },
}
```

**Then integrate in ALL data sources:**

```rust
// Kafka: src/velostream/datasource/kafka/reader.rs
fn deserialize_with_format(&self, ...) -> Result<StreamRecord, KafkaError> {
    // ... existing deserialization code ...

    let event_time = if let Some(ref config) = self.event_time_config {
        Some(extract_event_time(&fields, config)?)
    } else {
        None
    };

    Ok(StreamRecord {
        fields,
        timestamp: message.timestamp()...,
        event_time,  // ← Now populated!
        // ...
    })
}

// File: src/velostream/datasource/file/reader.rs
fn read_record(&self, line: &str) -> Result<StreamRecord, FileError> {
    // ... parse line into fields ...

    let event_time = if let Some(ref config) = self.event_time_config {
        Some(extract_event_time(&fields, config)?)
    } else {
        None
    };

    Ok(StreamRecord {
        fields,
        timestamp: Utc::now().timestamp_millis(),
        event_time,  // ← Now populated!
        // ...
    })
}

// Same pattern for HTTP, SQL, S3, etc.
```

// Add extraction method
fn extract_event_time(
    &self,
    fields: &HashMap<String, FieldValue>
) -> Result<Option<DateTime<Utc>>, KafkaError> {
    let Some(field_name) = &self.event_time_field else {
        return Ok(None);  // Not configured, use processing-time
    };

    let Some(field_value) = fields.get(field_name) else {
        return Err(KafkaError::MissingEventTimeField {
            field: field_name.clone(),
        });
    };

    let datetime = match &self.event_time_format {
        Some(TimestampFormat::EpochMillis) => {
            let millis = field_value.as_integer()?;
            DateTime::from_timestamp_millis(millis)
        }
        Some(TimestampFormat::EpochSeconds) => {
            let secs = field_value.as_integer()?;
            DateTime::from_timestamp(secs, 0)
        }
        Some(TimestampFormat::ISO8601) => {
            let s = field_value.as_string()?;
            DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc)
        }
        Some(TimestampFormat::Custom(fmt)) => {
            let s = field_value.as_string()?;
            NaiveDateTime::parse_from_str(s, fmt)?
                .and_local_timezone(Utc)
                .single()
        }
        None => {
            // Auto-detect: try integer first, then string
            self.auto_detect_timestamp(field_value)?
        }
    };

    Ok(Some(datetime.ok_or(KafkaError::InvalidTimestamp)?))
}
```

**Deliverables**:
- [ ] `TimestampFormat` enum with 4 formats
- [ ] `extract_event_time()` method in KafkaDataSource
- [ ] Configuration parsing in `new()` / `from_properties()`
- [ ] Error types for missing/invalid timestamps
- [ ] Auto-detection fallback logic

#### **Phase 2: Configuration Integration** (Estimated: 2 days)

**File**: `src/velostream/datasource/kafka/data_source.rs`

```rust
impl KafkaDataSource {
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, KafkaError> {
        // ... existing code ...

        let event_time_field = properties.get("event.time.field").cloned();
        let event_time_format = properties.get("event.time.format")
            .map(|s| TimestampFormat::parse(s))
            .transpose()?;

        Ok(Self {
            // ... existing fields ...
            event_time_field,
            event_time_format,
        })
    }
}

impl TimestampFormat {
    pub fn parse(s: &str) -> Result<Self, KafkaError> {
        match s {
            "epoch_millis" => Ok(TimestampFormat::EpochMillis),
            "epoch_seconds" | "epoch" => Ok(TimestampFormat::EpochSeconds),
            "iso8601" | "ISO8601" => Ok(TimestampFormat::ISO8601),
            custom => Ok(TimestampFormat::Custom(custom.to_string())),
        }
    }
}
```

**Deliverables**:
- [ ] Configuration property extraction
- [ ] Format string parsing
- [ ] Backward compatibility (None = processing-time)
- [ ] Configuration validation

#### **Phase 3: Testing** (Estimated: 3 days)

**New Test Files**:
1. `tests/unit/datasource/kafka/event_time_extraction_test.rs`
2. `tests/integration/kafka/event_time_watermarks_test.rs`

**Test Cases**:
```rust
#[test]
fn test_extract_event_time_epoch_millis() {
    // Given: Kafka message with timestamp field (epoch millis)
    // When: event.time.field = 'timestamp', format = 'epoch_millis'
    // Then: StreamRecord.event_time populated correctly
}

#[test]
fn test_extract_event_time_iso8601() {
    // Given: Kafka message with ISO 8601 timestamp string
    // When: event.time.field = 'event_timestamp', format = 'iso8601'
    // Then: StreamRecord.event_time parsed correctly
}

#[test]
fn test_extract_event_time_custom_format() {
    // Given: Kafka message with custom timestamp format
    // When: event.time.field = 'ts', format = '%Y-%m-%d %H:%M:%S'
    // Then: StreamRecord.event_time parsed using custom format
}

#[test]
fn test_missing_event_time_field_error() {
    // Given: event.time.field configured but field missing in message
    // When: Processing message
    // Then: Clear error with field name
}

#[test]
fn test_invalid_timestamp_format_error() {
    // Given: Field value doesn't match expected format
    // When: Parsing timestamp
    // Then: Clear error with format mismatch details
}

#[test]
fn test_no_config_uses_processing_time() {
    // Given: No event.time.field configured
    // When: Processing message
    // Then: StreamRecord.event_time = None (fallback to processing-time)
}

#[tokio::test]
async fn test_integration_watermarks_use_event_time() {
    // Given: Kafka messages with out-of-order event-times
    // When: Watermarks enabled with event.time.field
    // Then: Watermarks advance based on event-time, not processing-time
}
```

**Deliverables**:
- [ ] 15+ unit tests covering all formats
- [ ] 5+ integration tests for watermark interaction
- [ ] Error handling test coverage
- [ ] Performance benchmark (extraction overhead)

#### **Phase 4: Documentation Update** (Estimated: 1 day)

**Files to Update**:
- `docs/sql/watermarks-time-semantics.md` - Add implementation status
- `docs/sql/integration/kafka-configuration.md` - Event-time field extraction guide
- `CHANGELOG.md` - Document feature completion

**New Documentation**:
```markdown
## Event-Time Field Extraction

### Supported Timestamp Formats

| Format | Config Value | Example |
|--------|-------------|---------|
| **Unix Epoch (milliseconds)** | `epoch_millis` | `1696723200000` |
| **Unix Epoch (seconds)** | `epoch_seconds` or `epoch` | `1696723200` |
| **ISO 8601** | `iso8601` or `ISO8601` | `2023-10-08T00:00:00Z` |
| **Custom Format** | Any chrono format string | `%Y-%m-%d %H:%M:%S` |

### Configuration Examples

```sql
-- Extract from epoch milliseconds field
CREATE STREAM trades AS
SELECT * FROM market_data_stream
WITH (
    'event.time.field' = 'timestamp',
    'event.time.format' = 'epoch_millis'
);

-- Extract from ISO 8601 string field
CREATE STREAM events AS
SELECT * FROM event_stream
WITH (
    'event.time.field' = 'event_timestamp',
    'event.time.format' = 'iso8601'
);

-- Extract with custom format
CREATE STREAM logs AS
SELECT * FROM log_stream
WITH (
    'event.time.field' = 'log_time',
    'event.time.format' = '%Y-%m-%d %H:%M:%S%.3f'
);
```
```

**Deliverables**:
- [ ] Updated watermarks guide
- [ ] Kafka configuration guide
- [ ] Migration guide for existing demos
- [ ] Performance characteristics documentation

### **Success Metrics**

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| **Event-Time Extraction** | 0% | 100% | All configured messages extract event-time |
| **Timestamp Format Support** | 0 | 4 | epoch_millis, epoch_seconds, ISO8601, custom |
| **Test Coverage** | 0 tests | 20+ tests | Unit + integration coverage |
| **Documentation Accuracy** | 0% | 100% | Docs match implementation |
| **Demo Correctness** | Processing-time | Event-time | Trading demo uses trade execution time |
| **Error Handling** | Silent failure | Clear errors | Missing/invalid timestamps reported |

### **Timeline Estimate**

**Total Duration**: 2 weeks (9 working days)
- Phase 1 (Core Extraction): 3 days
- Phase 2 (Configuration): 2 days
- Phase 3 (Testing): 3 days
- Phase 4 (Documentation): 1 day

**Priority**: **CRITICAL** - This blocks proper event-time processing for all applications

### **Risk Assessment**

🔴 **High Risk**:
- Users may have deployed systems believing event-time works
- Financial applications using wrong timestamps for compliance
- Silent failures are production incidents waiting to happen

**Mitigation**:
- Add validation that errors if event.time.field configured but not implemented
- Update demo to show warning if using processing-time
- Document current status immediately

---

## 🚀 **NEW ARCHITECTURE: Generic Table Loading System**

**Identified**: September 29, 2024
**Priority**: **HIGH** - Performance & scalability enhancement
**Status**: 📋 **DESIGNED** - Ready for implementation
**Impact**: **🎯 MAJOR** - Unified loading for all data source types

### **Architecture Overview**

Replace source-specific loading with generic **Bulk + Incremental Loading** pattern that works across all data sources (Kafka, File, SQL, HTTP, S3).

#### **Two-Phase Loading Pattern**
```rust
trait TableDataSource {
    /// Phase 1: Initial bulk load of existing data
    async fn bulk_load(&self) -> Result<Vec<StreamRecord>, Error>;

    /// Phase 2: Incremental updates for new/changed data
    async fn incremental_load(&self, since: SourceOffset) -> Result<Vec<StreamRecord>, Error>;

    /// Get current position/offset for incremental loading
    async fn get_current_offset(&self) -> Result<SourceOffset, Error>;

    /// Check if incremental loading is supported
    fn supports_incremental(&self) -> bool;
}
```

#### **Loading Strategies by Source Type**
| Data Source | Bulk Load | Incremental Load | Offset Tracking |
|-------------|-----------|------------------|-----------------|
| **Kafka** | ✅ Consume from earliest | ✅ Consumer offset | ✅ Kafka offsets |
| **Files** | ✅ Read full file | ✅ File position/tail | ✅ Byte position |
| **SQL DB** | ✅ Full table scan | ✅ Change tracking | ✅ Timestamp/ID |
| **HTTP API** | ✅ Initial GET request | ✅ Polling/webhooks | ✅ ETag/timestamp |
| **S3** | ✅ List + read objects | ✅ Event notifications | ✅ Last modified |

### **Implementation Tasks**

#### **Phase 1: Core Trait & Interface** (Estimated: 1 week)
- [ ] Define `TableDataSource` trait with bulk/incremental methods
- [ ] Create `SourceOffset` enum for different offset types
- [ ] Implement generic CTAS loading orchestrator
- [ ] Add offset persistence for resume capability

#### **Phase 2: Source Implementations** (Estimated: 2 weeks)
- [ ] **KafkaDataSource**: Implement bulk (earliest→latest) + incremental (offset-based)
- [ ] **FileDataSource**: Implement bulk (full read) + incremental (file position tracking)
- [ ] **SqlDataSource**: Implement bulk (full query) + incremental (timestamp-based)

#### **Phase 3: Advanced Features** (Estimated: 1 week)
- [ ] Configurable incremental loading intervals
- [ ] Error recovery and retry logic
- [ ] Performance monitoring and metrics
- [ ] Health checks for loading status

### **Benefits**
- **🚀 Fast Initial Load**: Bulk load gets tables operational quickly
- **🔄 Real-time Updates**: Incremental load keeps data fresh
- **📊 Consistent Behavior**: Same pattern across all source types
- **⚡ Performance**: Minimal overhead for incremental updates
- **🛡️ Resilience**: Bulk load works even if incremental fails

---

## 🚨 **CRITICAL GAP: Stream-Table Load Coordination**

**Identified**: September 27, 2024
**Priority**: **LOW** - Core features complete, only optimization remaining
**Status**: 🟢 **PHASES 1-3 COMPLETE** - Core synchronization, graceful degradation, and progress monitoring all implemented
**Risk Level**: 🟢 **MINIMAL** - All critical gaps addressed, only optimization features remain

### **Problem Statement**

Streams can start processing before reference tables are fully loaded, causing:
- **Missing enrichment data** in stream-table joins
- **Inconsistent results** during startup phase
- **Silent failures** with no warning about incomplete tables
- **Production incidents** when tables are slow to load

### **Current State Analysis**

#### **What EXISTS** ✅
- `TableRegistry` with basic table management
- Background job tracking via `JoinHandle`
- Table status tracking (`Populating`, `BackgroundJobFinished`)
- Health monitoring for job completion checks
- **Progress monitoring system** - Complete real-time tracking ✅
- **Health dashboard** - Full REST API with Prometheus metrics ✅
- **Progress streaming** - Broadcast channels for real-time updates ✅
- **Circuit breaker pattern** - Production-ready with comprehensive tests ✅

#### **What's REMAINING** ⚠️
- ✅ ~~Synchronization barriers~~ - `wait_for_table_ready()` method **IMPLEMENTED**
- ✅ ~~Startup coordination~~ - Streams wait for table readiness **IMPLEMENTED**
- ✅ ~~Graceful degradation~~ - 5 fallback strategies **IMPLEMENTED**
- ✅ ~~Retry logic~~ - Exponential backoff retry **IMPLEMENTED**
- ✅ ~~Progress monitoring~~ - Complete implementation **COMPLETED**
- ✅ ~~Health dashboard~~ - Full REST API **COMPLETED**
- ❌ **Dependency graph resolution** - Table dependency tracking not implemented
- ❌ **Parallel loading optimization** - Multi-table parallel loading not implemented
- ✅ ~~Async Integration~~ - **VERIFIED WORKING** (225/225 tests passing, no compilation errors)

### **Production Impact**

```
BEFORE (BROKEN):
Stream Start ──────┐
                   ├──> JOIN (Missing Data!) ──> ❌ Incorrect Results
Table Loading ─────┘

NOW (IMPLEMENTED):
Table Loading ──> Ready Signal ──┐
                                  ├──> JOIN ──> ✅ Complete Results
Stream Start ───> Wait for Ready ┘
                      ↓
                Graceful Degradation
                (UseDefaults/Retry/Skip)
```

### **Implementation Plan**

#### **✅ Phase 1: Core Synchronization - COMPLETED September 27, 2024**
**Timeline**: October 1-7, 2024 → **COMPLETED EARLY**
**Goal**: Make table coordination the DEFAULT behavior → **✅ ACHIEVED**

```rust
// 1. Add synchronization as CORE functionality
impl TableRegistry {
    pub async fn wait_for_table_ready(
        &self,
        table_name: &str,
        timeout: Duration
    ) -> Result<TableReadyStatus, SqlError> {
        // Poll status with exponential backoff
        // Return Ready/Timeout/Error
    }
}

// 2. ENFORCE coordination in ALL stream starts
impl StreamJobServer {
    async fn start_job(&self, query: &StreamingQuery) -> Result<(), SqlError> {
        // MANDATORY: Extract and wait for ALL table dependencies
        let required_tables = extract_table_dependencies(query);

        // Block until ALL tables ready (no bypass option)
        for table in required_tables {
            self.table_registry.wait_for_table_ready(
                &table,
                Duration::from_secs(60)
            ).await?;
        }

        // Only NOW start stream processing
        self.execute_streaming_query(query).await
    }
}
```

**✅ DELIVERABLES COMPLETED**:
- ✅ `wait_for_table_ready()` method with exponential backoff
- ✅ `wait_for_tables_ready()` for multiple dependencies
- ✅ MANDATORY coordination in StreamJobServer.deploy_job()
- ✅ Clear timeout errors (60s default)
- ✅ Comprehensive test suite (8 test scenarios)
- ✅ No bypass options - correct behavior enforced
- ✅ Production-ready error messages and logging

**🎯 PRODUCTION IMPACT**: Streams now WAIT for tables, preventing missing enrichment data

#### **🔄 Phase 2: Graceful Degradation - IN PROGRESS September 27, 2024**
**Timeline**: October 8-14, 2024 → **STARTED EARLY**
**Goal**: Handle partial data scenarios gracefully → **⚡ CORE IMPLEMENTATION COMPLETE**

```rust
// 1. Configurable fallback behavior
pub enum TableMissingDataStrategy {
    UseDefaults(HashMap<String, FieldValue>),
    SkipRecord,
    EmitWithNulls,
    WaitAndRetry { max_retries: u32, delay: Duration },
    FailFast,
}

// 2. Implement in join processor
impl StreamTableJoinProcessor {
    fn handle_missing_table_data(
        &self,
        strategy: &TableMissingDataStrategy,
        stream_record: &StreamRecord
    ) -> Result<Option<StreamRecord>, SqlError> {
        match strategy {
            UseDefaults(defaults) => Ok(Some(enrich_with_defaults(stream_record, defaults))),
            SkipRecord => Ok(None),
            EmitWithNulls => Ok(Some(add_null_fields(stream_record))),
            WaitAndRetry { .. } => self.retry_with_backoff(stream_record),
            FailFast => Err(SqlError::TableNotReady),
        }
    }
}
```

**✅ DELIVERABLES - CORE IMPLEMENTATION COMPLETE**:
- ✅ **Graceful Degradation Framework**: Complete `graceful_degradation.rs` module
- ✅ **5 Fallback Strategies**: UseDefaults, SkipRecord, EmitWithNulls, WaitAndRetry, FailFast
- ✅ **StreamRecord Optimization**: Renamed to SimpleStreamRecord (48% memory savings)
- ✅ **StreamTableJoinProcessor Integration**: Graceful degradation in all join methods
- ✅ **Batch Processing Support**: Degradation for both individual and bulk operations
- ✅ **Async Compilation**: **VERIFIED WORKING** - All tests passing (no blocking issues)

**🎯 PRODUCTION IMPACT**: Missing table data now handled gracefully with configurable strategies

#### **✅ Phase 3: Progress Monitoring - COMPLETED October 2024**
**Timeline**: October 15-21, 2024 → **COMPLETED EARLY**
**Goal**: Real-time visibility into table loading → **✅ ACHIEVED**

**Implementation Files**:
- `src/velostream/server/progress_monitoring.rs` (564 lines) - Complete progress tracking system
- `src/velostream/server/health_dashboard.rs` (563 lines) - Full REST API endpoints
- `src/velostream/server/progress_streaming.rs` - Real-time streaming support
- `tests/unit/server/progress_monitoring_integration_test.rs` - Comprehensive test coverage

**Implemented Features**:
```rust
// ✅ Progress tracking with atomic counters
pub struct TableProgressTracker {
    records_loaded: AtomicUsize,
    bytes_processed: AtomicU64,
    loading_rate: f64,      // records/sec
    bytes_per_second: f64,  // bytes/sec
    estimated_completion: Option<DateTime<Utc>>,
    progress_percentage: Option<f64>,
}

// ✅ Health dashboard REST API
GET /health/tables          // Overall health status
GET /health/table/{name}    // Individual table health
GET /health/progress        // Loading progress for all tables
GET /health/metrics         // Comprehensive metrics + Prometheus format
GET /health/connections     // Streaming connection stats
POST /health/table/{name}/wait  // Wait for table with progress

// ✅ Real-time streaming
pub enum ProgressEvent {
    InitialSnapshot, TableUpdate, SummaryUpdate,
    TableCompleted, TableFailed, KeepAlive
}
```

**✅ Deliverables - ALL COMPLETED**:
- ✅ Real-time progress tracking with atomic operations
- ✅ Loading rate calculation (records/sec + bytes/sec)
- ✅ ETA estimation based on current rates
- ✅ Health dashboard integration with REST API
- ✅ Progress streaming with broadcast channels
- ✅ Prometheus metrics export
- ✅ Comprehensive test coverage

#### **🟡 Phase 4: Advanced Coordination - PARTIALLY COMPLETE**
**Timeline**: October 22-28, 2024
**Status**: 🟡 **1 of 3 features complete, 2 remaining**

**✅ COMPLETED: Circuit Breaker Pattern**
- **File**: `src/velostream/sql/execution/circuit_breaker.rs` (674 lines)
- **Features**: Full circuit breaker states (Closed, Open, HalfOpen), configurable thresholds, automatic recovery, failure rate calculation
- **Test Coverage**: 13 comprehensive tests passing

**❌ REMAINING: Dependency Graph Resolution**
```rust
// TODO: Implement table dependency tracking
pub struct TableDependencyGraph {
    nodes: HashMap<String, TableNode>,
    edges: Vec<(String, String)>, // dependencies
}

impl TableDependencyGraph {
    pub fn topological_load_order(&self) -> Result<Vec<String>, CycleError> {
        // Determine optimal table loading order
    }

    pub fn detect_cycles(&self) -> Result<(), CycleError> {
        // Detect circular dependencies
    }
}
```

**❌ REMAINING: Parallel Loading with Dependencies**
```rust
// TODO: Implement parallel loading coordinator
pub async fn load_tables_with_dependencies(
    tables: Vec<TableDefinition>,
    max_parallel: usize
) -> Result<(), SqlError> {
    let graph = build_dependency_graph(&tables);
    let load_order = graph.topological_load_order()?;

    // Load in waves respecting dependencies
    for wave in load_order.chunks(max_parallel) {
        join_all(wave.iter().map(|t| load_table(t))).await?;
    }
}
```

**Deliverables Status**:
- ❌ Dependency graph resolution (NOT STARTED) - **[Implementation Plan Available](../docs/feature/fr-025-phase-4-parallel-loading-implementation-plan.md)**
- ❌ Parallel loading optimization (NOT STARTED) - **[Implementation Plan Available](../docs/feature/fr-025-phase-4-parallel-loading-implementation-plan.md)**
- ✅ Circuit breaker pattern (COMPLETE)
- ✅ Advanced retry strategies (via graceful degradation - COMPLETE)

**📋 Implementation Plan**: See [fr-025-phase-4-parallel-loading-implementation-plan.md](../docs/feature/fr-025-phase-4-parallel-loading-implementation-plan.md) for detailed 2-week implementation guide with code examples, test cases, and integration points.

### **Success Metrics**

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| **Startup Coordination** | 0% | 100% | ALL streams wait for tables |
| **Missing Data Incidents** | Unknown | 0 | Zero incomplete enrichment |
| **Average Wait Time** | N/A | < 30s | Time waiting for tables |
| **Retry Success Rate** | 0% | > 95% | Successful retries after initial failure |
| **Visibility** | None | 100% | Full progress monitoring |

### **Testing Strategy**

1. **Unit Tests**: Synchronization primitives, timeout handling
2. **Integration Tests**: Full startup coordination flow
3. **Chaos Tests**: Slow loading, failures, network issues
4. **Load Tests**: 50K+ record tables, multiple dependencies
5. **Production Simulation**: Real data patterns and volumes

### **Risk Mitigation**

- **Timeout Defaults**: Conservative 60s default, configurable per-table
- **Monitoring**: Comprehensive metrics from day 1
- **Fail-Safe Defaults**: Start with strict coordination, relax as needed
- **Testing Coverage**: Extensive testing before marking feature complete

---

## 🔄 **NEXT DEVELOPMENT PRIORITIES**

### ✅ **PHASE 3: Stream-Table Joins - COMPLETED September 27, 2024**

**Status**: ✅ **COMPLETED** - Moved to [todo-complete.md](todo-complete.md)
**Achievement**: 840x performance improvement with advanced optimization suite
**Production Status**: Enterprise-ready with 98K+ records/sec throughput

---

### ✅ **PHASE 4: Enhanced CREATE TABLE Features - COMPLETED September 28, 2024**

**Status**: ✅ **COMPLETED**
**Timeline**: Completed in 1 day
**Achievement**: Full AUTO_OFFSET support and comprehensive documentation

#### **Feature 1: Wildcard Field Discovery**
**Status**: ✅ **VERIFIED SUPPORTED**
- Parser fully supports `SelectField::Wildcard`
- `CREATE TABLE AS SELECT *` works in production
- Documentation created at `docs/sql/create-table-wildcard.md`

#### **Feature 2: AUTO_OFFSET Configuration for TABLEs**
**Status**: ✅ **IMPLEMENTED**
- Added `new_with_properties()` method to Table
- Updated CTAS processor to pass properties
- Full test coverage added
- Backward compatible (defaults to `earliest`)

**Completed Implementation**:
```sql
-- Use latest offset (now working!)
CREATE TABLE real_time_data AS
SELECT * FROM kafka_stream
WITH ("auto.offset.reset" = "latest");

-- Use earliest offset (default)
CREATE TABLE historical_data AS
SELECT * FROM kafka_stream
WITH ("auto.offset.reset" = "earliest");
```

---

### ✅ **PHASE 5: Missing Source Handling - COMPLETED September 28, 2024**

**Status**: ✅ **CORE FUNCTIONALITY COMPLETED**
**Timeline**: Completed in 1 day
**Achievement**: Robust Kafka retry logic with configurable timeouts

#### **✅ Completed Features**

##### **✅ Task 1: Kafka Topic Wait/Retry**
- ✅ Added `topic.wait.timeout` property support
- ✅ Added `topic.retry.interval` configuration
- ✅ Implemented retry loop with logging
- ✅ Backward compatible (no wait by default)

```sql
-- NOW WORKING:
CREATE TABLE events AS
SELECT * FROM kafka_topic
WITH (
    "topic.wait.timeout" = "60s",
    "topic.retry.interval" = "5s"
);
```

##### **✅ Task 2: Utility Functions**
- ✅ Duration parsing utility (`parse_duration`)
- ✅ Topic missing error detection (`is_topic_missing_error`)
- ✅ Enhanced error message formatting
- ✅ Comprehensive test coverage

##### **✅ Task 3: Integration**
- ✅ Updated `Table::new_with_properties` with retry logic
- ✅ All CTAS operations now support retry
- ✅ Full test suite added
- ✅ Documentation updated

#### **✅ Fully Completed**
- ✅ **File Source Retry**: Complete implementation with comprehensive test suite ✅ **COMPLETED September 28, 2024**

#### **Success Metrics**
- [x] Zero manual intervention for transient missing Kafka topics
- [x] Zero manual intervention for transient missing file sources ✅ **NEW**
- [x] Clear error messages with solutions
- [x] Configurable retry behavior
- [x] Backward compatible (no retry by default)
- [x] Production-ready timeout handling for Kafka and file sources ✅ **EXPANDED**

**Key Benefits**:
- **No more immediate failures** for missing Kafka topics or file sources
- **Configurable wait times** up to any duration for both Kafka and file sources
- **Intelligent retry intervals** with comprehensive logging
- **100% backward compatible** - existing code unchanged
- **Pattern matching support** - wait for glob patterns like `*.json` to appear
- **File watching integration** - seamlessly works with existing file watching features

---

### 🟡 **PRIORITY 2: Advanced Window Functions**
**Timeline**: 4 weeks
**Dependencies**: ✅ Prerequisites met (Phase 2 complete)
**Status**: 🔄 **READY TO START**

### 🟡 **PRIORITY 3: Enhanced JOIN Operations**
**Timeline**: 8 weeks
**Dependencies**: Stream-Table joins completion
**Status**: ❌ **PENDING** (depends on Priority 1)

### 🟡 **PRIORITY 4: Comprehensive Aggregation Functions**
**Timeline**: 5 weeks
**Dependencies**: ✅ Prerequisites met (OptimizedTableImpl complete)
**Status**: 🔄 **READY TO START**

### 🟡 **PRIORITY 5: Advanced SQL Features**
**Timeline**: 12 weeks
**Dependencies**: Stream-Table joins completion
**Status**: ❌ **PENDING** (depends on Priority 1)

---

## 📊 **Overall Progress Summary**

| Phase | Status | Completion | Timeline | Dates |
|-------|--------|------------|----------|-------|
| **Phase 1**: SQL Subquery Foundation | ✅ **COMPLETED** | 100% | Weeks 1-3 | Aug 1-21, 2024 ✅ |
| **Phase 2**: OptimizedTableImpl & CTAS | ✅ **COMPLETED** | 100% | Weeks 4-8 | Aug 22 - Sep 26, 2024 ✅ |
| **Phase 3**: Stream-Table Joins | ✅ **COMPLETED** | 100% | Week 9 | Sep 27, 2024 ✅ |
| **Phase 4**: Advanced Streaming Features | 🔄 **READY TO START** | 0% | Weeks 10-17 | Sep 28 - Dec 21, 2024 |

### **Key Achievements**
- ✅ **OptimizedTableImpl**: 90% code reduction with 1.85M+ lookups/sec performance
- ✅ **Stream-Table Joins**: 40,404 trades/sec with real-time enrichment capability
- ✅ **Enhanced SQL Validator**: Intelligent JOIN performance analysis (Stream-Table vs Stream-Stream)
- ✅ **SQL Aggregation**: COUNT and SUM operations with proper type handling
- ✅ **Reserved Keywords**: STATUS, METRICS, PROPERTIES fixed for production use
- ✅ **Test Coverage**: 222 unit + 1513+ comprehensive + 56 doc tests all passing
- ✅ **Financial Precision**: ScaledInteger for exact arithmetic operations
- ✅ **Multi-Table Joins**: Complete pipeline (user profiles + market data + limits)
- ✅ **Production Ready**: Complete validation with enterprise benchmarks

### **Recent Milestone Achievement**
**🎯 Target**: Complete Phase 3 Stream-Table Joins by October 25, 2024 → **✅ COMPLETED September 27, 2024**
- **Progress**: 100% complete (3 weeks ahead of schedule!)
- **Achievement**: Real-time trade enrichment with KTable joins fully implemented
- **Foundation**: ✅ OptimizedTableImpl provides enterprise performance foundation
- **Results**: 40,404 trades/sec throughput with complete financial enrichment pipeline
- **Quality**: Enhanced SQL validation with intelligent JOIN performance warnings

### **Next Development Priorities**
**📅 Phase 4 (Sep 28 - Dec 21, 2024)**: Advanced Streaming Features (NOW READY TO START)
- Advanced Window Functions with complex aggregations
- Enhanced JOIN Operations across multiple streams
- Comprehensive Aggregation Functions
- Advanced SQL Features and optimization
- Production Deployment Readiness

**🚀 Accelerated Timeline**: Phase 3 completion 3 weeks early opens opportunity for expanded Phase 4 scope

---

*This document focuses on active development priorities. See [todo-consolidated.md](todo-consolidated.md) for comprehensive historical context and [todo-complete.md](todo-complete.md) for completed work archive.*