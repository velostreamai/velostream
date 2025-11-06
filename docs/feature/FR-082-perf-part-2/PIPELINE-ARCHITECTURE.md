# Velostream Pipeline Architecture

## High-Level Data Flow

```
External Data Sources
    ↓
┌─────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                           │
│  Kafka / File / Other Connectors                            │
│  • Deserializes records                                     │
│  • Applies schema (JSON/Avro/Protobuf)                     │
│  • Produces StreamRecord                                    │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    BATCH COLLECTION                          │
│  Collects N records or waits for timeout                    │
│  • Memory buffering                                         │
│  • Timeout management                                       │
│  Produces: Vec<StreamRecord> batch                          │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│               JOB SERVER V2 COORDINATION                     │
│  (src/velostream/server/v2/coordinator.rs)                  │
│                                                              │
│  Responsibilities:                                           │
│  • Query parsing & validation                               │
│  • Partition strategy selection (Hash/Range/Round-robin)    │
│  • Per-record routing to HashRouter                         │
│  • Result collection from all partitions                    │
│  • Output sink forwarding                                   │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    HASH ROUTER                               │
│  (src/velostream/server/v2/hash_router.rs)                  │
│                                                              │
│  Responsibilities:                                           │
│  • Extract partition key from record                        │
│  • Hash(partition_key) % num_partitions → partition_id     │
│  • Route record to PartitionStateManager[partition_id]      │
│  • Maintains partition assignment consistency               │
└────────────────────────┬────────────────────────────────────┘
                         ↓
        ┌────────────────┼────────────────┐
        ↓                ↓                ↓
   ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
   │ Partition 0 │ │ Partition 1 │ │ Partition N │
   └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
          ↓                ↓                ↓
┌─────────────────────────────────────────────────────────────┐
│            PARTITION STATE MANAGER (Per-Partition)           │
│  (src/velostream/server/v2/partition_manager.rs)             │
│                                                              │
│  Per-Partition Responsibilities:                            │
│  ✅ Metrics Tracking                                        │
│     • Throughput monitoring (rec/sec)                       │
│     • Latency tracking (per-record processing time)         │
│     • Backpressure detection                                │
│     • Data: PartitionMetrics                                │
│                                                              │
│  ✅ Watermark Management (Phase 4)                          │
│     • Tracks event_time watermark per partition             │
│     • Detects late arrivals: is_late = watermark > event_time │
│     • Applies late-record strategy:                         │
│       - Drop: rejects late records                          │
│       - ProcessWithWarning: logs and continues              │
│       - ProcessAll: silently continues                      │
│     • Data: WatermarkManager                                │
│                                                              │
│  ❌ DOES NOT duplicate window processing                    │
│     • Window logic lives in window_v2 engine only           │
│     • Window state in ProcessorContext                      │
│     • Per-query, not per-partition                          │
│                                                              │
│  Function: process_record(record) → Result<(), SqlError>   │
│     1. Update watermark from record.event_time               │
│     2. Check late-record strategy                            │
│     3. Forward valid record to engine                        │
│     4. Track metrics                                         │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│             STREAM EXECUTION ENGINE (Query Executor)         │
│  (src/velostream/sql/execution/engine.rs)                    │
│                                                              │
│  Responsibilities:                                           │
│  • Parse & compile query once                               │
│  • Maintain query execution state                           │
│  • Route records to QueryProcessor                          │
│  • Manage output_receiver channel (for EMIT CHANGES)        │
│  • Data: StreamExecutionEngine                              │
│                                                              │
│  Key Methods:                                                │
│  • execute_with_record(record) → emits through channel      │
│  • take_output_receiver() → for batch draining              │
│  • return_output_receiver() → restores channel              │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│               QUERY PROCESSOR DISPATCHER                     │
│  (src/velostream/sql/execution/processors/mod.rs)            │
│                                                              │
│  Routes Query to Appropriate Handler:                       │
│                                                              │
│  SELECT with WINDOW clause?                                 │
│     ↓ YES: Route to WindowProcessor                         │
│     ↓ NO:  Route to SelectProcessor                         │
│                                                              │
│  Other:                                                      │
│  • INSERT INTO → InsertProcessor                            │
│  • UPDATE → UpdateProcessor                                 │
│  • DELETE → DeleteProcessor                                 │
│  • SHOW → ShowProcessor                                     │
└────────────────────────┬────────────────────────────────────┘
                         ↓
        (Only windowed queries proceed to window engine)
        (Non-windowed queries exit here with results)
                         ↓
┌─────────────────────────────────────────────────────────────┐
│               WINDOW PROCESSOR (Router)                      │
│  (src/velostream/sql/execution/processors/window.rs)         │
│                                                              │
│  Responsibilities:                                           │
│  • Detect query has WINDOW clause                           │
│  • Verify window_v2 is enabled (always true now)            │
│  • Route to WindowAdapter for actual processing             │
│                                                              │
│  Returns: Option<StreamRecord> (None = buffering, Some = emit) │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│         WINDOW ADAPTER (window_v2 Integration)               │
│  (src/velostream/sql/execution/window_v2/adapter.rs)         │
│                                                              │
│  Responsibilities:                                           │
│  • Initialize window_v2 state (first call)                  │
│  • Get/create appropriate WindowStrategy                    │
│  • Get/create EmissionStrategy                              │
│  • Extract GROUP BY columns                                 │
│  • Store state in ProcessorContext.window_v2_states         │
│                                                              │
│  Data Structures:                                            │
│  • window_v2_states: HashMap<String, Box<WindowV2State>>    │
│    - Key: "window_v2:{query_id}"                            │
│    - Value: WindowV2State with strategy + emission_strategy │
└────────────────────────┬────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│            WINDOW_V2 ENGINE (Strategy Execution)             │
│  (src/velostream/sql/execution/window_v2/)                   │
│                                                              │
│  Core Components:                                            │
│                                                              │
│  1. WindowStrategy (Pluggable)                              │
│     • TumblingWindowStrategy                                │
│     • SlidingWindowStrategy                                 │
│     • SessionWindowStrategy                                 │
│     • RowsWindowStrategy ← Used for ROWS WINDOW            │
│                                                              │
│     Methods:                                                │
│     • add_record(record) → bool (should_emit)               │
│     • get_window_records() → Vec<SharedRecord>              │
│     • should_emit(current_time) → bool                      │
│     • clear()                                                │
│     • get_stats() → WindowStats                             │
│                                                              │
│  2. EmissionStrategy (Pluggable)                            │
│     • EmitFinalStrategy (emit once per window)              │
│     • EmitChangesStrategy (emit on every record)            │
│                                                              │
│     Methods:                                                │
│     • process_record(record, window_strategy)               │
│       → EmitDecision (Emit, Skip, EmitAndClear)             │
│                                                              │
│  3. AccumulatorManager                                      │
│     • GROUP BY state management                             │
│     • Aggregate function computation                        │
│     • (COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE)        │
│                                                              │
│  Zero-Copy Design:                                          │
│  • Records wrapped in Arc<StreamRecord> (SharedRecord)      │
│  • No expensive cloning in window buffers                   │
│  • Multiple components share same record data               │
│                                                              │
│  State Storage:                                              │
│  • Persisted in ProcessorContext.window_v2_states           │
│  • Survives across record-by-record processing              │
│  • Per-query state (not per-partition)                      │
│                                                              │
│  Processing Logic:                                          │
│  1. WindowStrategy.add_record() → should_emit?              │
│  2. If yes: get_window_records()                            │
│  3. Apply EmissionStrategy + AccumulatorManager             │
│  4. Compute aggregations (GROUP BY results)                 │
│  5. Return emitted records                                  │
│  6. If EmitAndClear: clear window for next cycle            │
└────────────────────────┬────────────────────────────────────┘
                         ↓
        (Results from window_v2 engine go to...)
                         ↓
┌─────────────────────────────────────────────────────────────┐
│          BATCH PROCESSOR RESULT COLLECTION                   │
│  (src/velostream/server/processors/common.rs)                │
│                                                              │
│  Entry Points:                                               │
│  • process_batch_with_output()                              │
│                                                              │
│  Dual-Path Processing:                                      │
│                                                              │
│  ┌─── EMIT CHANGES Path (if query has EMIT CHANGES) ───┐    │
│  │  1. Temporarily take output_receiver from engine    │    │
│  │  2. For each record in batch:                       │    │
│  │     a. engine.execute_with_record(query, record)    │    │
│  │     b. Drain output_receiver with try_recv()        │    │
│  │     c. Collect emitted_records                      │    │
│  │  3. Final drain after all records processed         │    │
│  │  4. Return receiver to engine                       │    │
│  │                                                      │    │
│  │  Result: output_records = all emitted results       │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─── Standard Path (non-EMIT queries) ────────────────┐    │
│  │  1. Get state once at batch start (minimal lock)   │    │
│  │  2. Process entire batch with local state copies   │    │
│  │  3. Sync state back once at batch end (minimal lock) │   │
│  │                                                      │    │
│  │  Result: output_records = standard query results   │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                              │
│  Returns: BatchProcessingResultWithOutput                   │
│  • records_processed                                        │
│  • records_failed                                           │
│  • output_records: Vec<Arc<StreamRecord>>                   │
│  • error_details                                            │
└────────────────────────┬────────────────────────────────────┘
                         ↓
        ┌────────────────┼────────────────┐
        ↓                ↓                ↓
   ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
   │   Sink 0    │ │   Sink 1    │ │   Sink N    │
   └─────────────┘ └─────────────┘ └─────────────┘
        ↓                ↓                ↓
┌─────────────────────────────────────────────────────────────┐
│                    OUTPUT SINKS                              │
│  (src/velostream/datasource/)                                │
│                                                              │
│  Multiple Output Formats:                                   │
│  • Kafka (serialized: JSON/Avro/Protobuf)                   │
│  • File (CSV, Parquet, JSON)                                │
│  • Stdout (for testing/debugging)                           │
│  • Custom (pluggable DataSink trait)                        │
└─────────────────────────────────────────────────────────────┘
                         ↓
                  External Storage
```

---

## Key Data Structures

### Record Journey

```
StreamRecord (input)
├─ fields: HashMap<String, FieldValue>
├─ event_time: Option<i64> (for watermarks)
├─ _timestamp: SystemTimestamp (injected)
├─ _partition: SystemPartition (injected)
├─ _offset: SystemOffset (injected)
└─ _event_time: SystemEventTime (injected)
       ↓
SharedRecord (in window buffers)
├─ Arc<StreamRecord> (zero-copy wrapper)
└─ Shared across buffer references
       ↓
ProcessorResult (query result)
├─ record: Option<StreamRecord>
├─ header_mutations: Vec<HeaderOperation>
└─ should_count: bool
```

### State Management

```
ProcessorContext (per-query execution state)
├─ metadata: HashMap<String, String> (configuration)
├─ window_v2_states: HashMap<String, Box<dyn Any>>
│  └─ Key: "window_v2:{query_id}"
│  └─ Value: WindowV2State
│     ├─ strategy: Box<dyn WindowStrategy>
│     ├─ emission_strategy: Box<dyn EmissionStrategy>
│     └─ group_by_columns: Option<Vec<String>>
├─ data_sources: HashMap<String, Arc<dyn DataSource>>
├─ schemas: HashMap<String, Arc<Schema>>
└─ ... other context fields
       ↓
StreamExecutionEngine (query lifecycle)
├─ query: StreamingQuery (parsed SQL)
├─ context: ProcessorContext (reused across records)
├─ output_sender: Sender<StreamRecord> (for EMIT CHANGES)
├─ output_receiver: Receiver<StreamRecord> (for batch draining)
└─ metrics: ExecutionMetrics
       ↓
PartitionStateManager (per-partition state)
├─ partition_id: usize
├─ metrics: Arc<PartitionMetrics> (throughput, latency)
├─ watermark_manager: Arc<WatermarkManager>
│  ├─ current_watermark: i64 (from max event_time)
│  ├─ late_record_strategy: LateRecordStrategy
│  └─ last_watermark_update: Instant
└─ [Does NOT have window state - that's in engine]
```

---

## Processing Modes

### Mode 1: Non-Windowed SELECT
```
Record → PartitionStateManager (watermark check)
       → Engine → QueryProcessor → SelectProcessor
       → Returns single result record
       → Sink
```

### Mode 2: Windowed SELECT with EMIT FINAL
```
Record → PartitionStateManager (watermark check)
       → Engine → QueryProcessor → WindowProcessor → WindowAdapter
       → window_v2 strategy: add_record() returns false (buffering)
       → EmissionStrategy: only emit at window boundary
       → When: should_emit() = true
           → Compute aggregations
           → Return all group results
           → Sink receives multiple records (one per group)
       → Clear window
       → Repeat
```

### Mode 3: Windowed SELECT with EMIT CHANGES
```
Record → PartitionStateManager (watermark check)
       → Engine.execute_with_record()
       → Sends through output_sender channel
       → Engine → QueryProcessor → WindowProcessor → WindowAdapter
       → window_v2 strategy: add_record() returns true (should emit)
       → EmissionStrategy: EMIT CHANGES = emit on every record
       → For each record added:
           → Compute aggregations (all groups)
           → Send results through output_sender
       → Batch processor drains channel
       → Sink receives continuous stream of results
```

---

## Partition Watermark Integration

```
Record with event_time = 1000000

         ↓
PartitionStateManager.process_record()
    ├─ watermark_manager.update(1000000)
    │  └─ Updates watermark from max(watermark, 1000000)
    │
    ├─ (is_late, should_drop) = watermark_manager.is_late(1000000)
    │  └─ is_late: 1000000 < current_watermark + slack
    │  └─ should_drop: determined by late_record_strategy
    │
    ├─ If should_drop: return Err() → Record dropped
    │
    └─ If valid: Forward to engine

         ↓
Engine → window_v2
    ├─ Receives "valid" record (passed watermark check)
    ├─ Processes through window strategy
    ├─ No duplicate watermark checking (already done)
    └─ Emits when window boundary hit
```

**Key Point**: Watermark is a partition-level optimization, NOT a window-level concern.
- Partition watermark prevents processing stale data
- Window engine always works with "fresh enough" data

---

## Reusability & Composition

The architecture uses **composition** not **inheritance**:

```
ProcessorContext + window_v2_states
    ↓
    Reused by: SelectProcessor, WindowProcessor, JoinProcessor

WindowStrategy trait
    ↓
    Implementations: Tumbling, Sliding, Session, Rows

EmissionStrategy trait
    ↓
    Implementations: EmitFinal, EmitChanges

PartitionStateManager
    ↓
    Reused by: All partitions in coordinator

StreamExecutionEngine
    ↓
    Reused for: All records in a job
```

Each component is **independently testable** and **independently deployable**.

---

## Performance Characteristics

| Component | Complexity | Per-Record Cost | Notes |
|-----------|-----------|-----------------|-------|
| PartitionStateManager | O(1) | ~1μs | Just metrics + watermark update |
| QueryProcessor dispatch | O(1) | ~0.5μs | Simple pattern match |
| WindowStrategy.add_record() | O(1)* | ~2-5μs | *ROWS window with fixed buffer |
| EmissionStrategy.process_record() | O(n) | ~n*0.5μs | n = group count for aggregations |
| Arc<StreamRecord> sharing | O(1) | ~0.1μs | Zero-copy reference counting |
| Channel operations | O(1) | ~0.2μs | Single atomic compare-and-swap |

**Target Phase 5**: 1.5M rec/sec on 8 cores = 125K rec/sec per partition
- At 2μs per window_v2 operation: ~2 million ops in 1.5M / 2 = 750K rec worth of time
- Leaves 250K rec capacity for I/O, coordination, etc.

---

## Summary

**The pipeline separates concerns:**
1. **Input**: Deserialization → Records
2. **Coordination**: Hash routing → Partitions
3. **Metrics**: Throughput tracking → Partition metrics
4. **Watermarks**: Event-time ordering → Late record filtering
5. **Queries**: SQL execution → QueryProcessor dispatch
6. **Windows**: Buffering & aggregation → window_v2 engine
7. **Emission**: Result generation → Batch processor collection
8. **Output**: Serialization → Sinks

**No duplication**: Each level has one responsibility.
**No leaky abstractions**: Partition manager doesn't know window details.
**Maximum reuse**: window_v2 engine works with all partition strategies.
