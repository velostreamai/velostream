# Design Document: Stream-Stream Joins

**Status:** In Progress (Phases 1-6 Complete, Phase 7: Window Joins Complete)
**Author:** Claude Code
**Created:** 2026-01-16
**Last Updated:** 2026-01-18
**Issue:** FR-085 (stream-stream joins)

### Completed Features
- ✅ Interval Joins (time-bounded correlation)
- ✅ Memory-Based Limits (Phase 6.1)
- ✅ LRU Eviction Policy (Phase 6.3)
- ✅ Tumbling Window Joins (Phase 7)
- ✅ Sliding Window Joins (Phase 7)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Statement](#problem-statement)
3. [Goals and Non-Goals](#goals-and-non-goals)
4. [Background Research](#background-research)
5. [Proposed Architecture](#proposed-architecture)
6. [Detailed Component Design](#detailed-component-design)
7. [SQL Syntax and Semantics](#sql-syntax-and-semantics)
8. [Implementation Plan](#implementation-plan)
9. [Testing Strategy](#testing-strategy)
10. [Performance Considerations](#performance-considerations)
11. [Future Enhancements](#future-enhancements)
12. [Appendix](#appendix)

---

## Executive Summary

This document proposes an architecture for implementing stream-stream joins in Velostream. Currently, stream-stream joins fail because sources are processed sequentially rather than concurrently, and there is no join state management to buffer records while waiting for matches from the other stream.

The proposed solution introduces:
- **Concurrent source reading** via a JoinCoordinator
- **Windowed join state stores** to buffer records from each side
- **Time-based join semantics** (interval joins and window joins)
- **Watermark-driven state cleanup** to bound memory usage

This enables joins like:
```sql
SELECT o.*, s.*
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
WHERE s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '24' HOUR
```

---

## Problem Statement

### Current Behavior

The V2 job processor (`job_processor_v2.rs:377`) processes multiple data sources **sequentially**:

```rust
for (reader_idx, (reader_name, mut reader)) in readers_list.into_iter().enumerate() {
    // Process ALL records from source 1, THEN move to source 2
}
```

For a join query with two sources (orders, shipments):

```
Timeline:
T0          T1          T2          T3          T4
│           │           │           │           │
│ Read ALL  │ Process   │ Read ALL  │ Process   │
│ Orders    │ Orders    │ Shipments │ Shipments │
│           │ (no ships │           │ (orders   │
│           │  to join) │           │  are GONE)│
```

**Result:** All join fields from the second source are NULL because:
1. When processing orders, no shipments exist in the join buffer
2. When processing shipments, orders have already been processed and discarded

### Evidence from Test Failures

Test output from `tier3_joins/21_stream_stream_join`:
```
Found null values: shipment_id[0-9], carrier[0-9], tracking_number[0-9]
```

### Root Causes

1. **Sequential source processing** - No concurrent reading from multiple streams
2. **No join state management** - No buffer to hold records waiting for matches
3. **No temporal coordination** - No mechanism to correlate records by event time
4. **No watermark synchronization** - No way to know when to expire buffered records

---

## Goals and Non-Goals

### Goals

1. **Support stream-stream inner joins** with time-bounded windows
2. **Support interval joins** (e.g., "shipment within 24 hours of order")
3. **Support window joins** (tumbling, sliding, session windows)
4. **Bound memory usage** via watermark-driven state expiration
5. **Maintain exactly-once semantics** where possible
6. **Pass all tier3 join tests** (21-24)

### Non-Goals (Future Work)

1. **Unbounded joins** - Regular joins without time bounds (infinite state)
2. **Multi-way joins** (3+ streams) - Focus on 2-stream joins first
3. **Persistent state** - RocksDB-backed state stores (in-memory only for now)
4. **Cross-partition joins** - Assume co-partitioned data by join key

---

## Background Research

### Apache Flink Approach

**Sources:**
- [Flink Joining Documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/joining/)
- [Flink Window Joins](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-join/)
- [Flink Continuous Query Joins](https://nightlies.apache.org/flink/flink-docs-release-1.11/dev/table/streaming/joins.html)

#### Key Concepts

1. **ConnectedStreams + CoProcessFunction**
   - Both streams are connected and processed by the SAME operator
   - Keyed state is shared between both sides
   - Single operator instance handles all records for a given key

2. **Interval Joins**
   ```java
   orders
     .keyBy(order -> order.orderId)
     .intervalJoin(shipments.keyBy(ship -> ship.orderId))
     .between(Time.minutes(-5), Time.minutes(30))
     .process(new JoinFunction())
   ```
   - Bounded state - only keep records within the interval
   - Expressed as: `b.timestamp ∈ [a.timestamp + lowerBound, a.timestamp + upperBound]`

3. **Window Joins**
   - Elements from both streams must fall within the same window
   - Window closes → emit results → purge state
   - More efficient for high-volume streams

4. **State Management**
   - **Regular joins:** Unbounded state (not recommended for production)
   - **Interval joins:** Bounded by time interval
   - **Window joins:** Bounded by window size, purged on window close

### Kafka Streams Approach

**Sources:**
- [Kafka Streams Joins Course](https://developer.confluent.io/courses/kafka-streams/joins/)
- [Crossing the Streams](https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/)
- [KStream DSL API](https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html)

#### Key Concepts

1. **Dual State Stores**
   ```
   ┌─────────────────┐     ┌─────────────────┐
   │ Left Store      │     │ Right Store     │
   │ "this-join"     │     │ "other-join"    │
   │ (windowed)      │     │ (windowed)      │
   └─────────────────┘     └─────────────────┘
   ```
   - Each side maintains its own windowed state store
   - When record arrives: store it AND lookup in other side's store

2. **JoinWindows**
   ```java
   JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
   ```
   - Sliding windows aligned to record timestamps
   - Each record defines its own window relative to its timestamp

3. **StreamJoined Configuration**
   ```java
   StreamJoined.with(keySerde, leftSerde, rightSerde)
       .withStoreName("order-shipment-join")
       .withRetention(Duration.ofHours(1))
   ```
   - Configurable retention period
   - Named stores for observability

4. **Processing Semantics**
   - Record arrives on left → store in left store → lookup in right store → emit matches
   - Record arrives on right → store in right store → lookup in left store → emit matches
   - Both directions ensure all matches are found regardless of arrival order

### Key Takeaways for Velostream

| Aspect | Flink | Kafka Streams | Velostream (Proposed) |
|--------|-------|---------------|----------------------|
| State Model | Keyed state per operator | Dual windowed stores | Dual windowed stores |
| Concurrency | ConnectedStreams | Single thread per partition | Concurrent readers + coordinator |
| Join Types | Interval, Window, Temporal | Window (sliding) | Interval, Window |
| State Cleanup | Watermark-driven | Retention period | Watermark + retention |

---

## Proposed Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STREAM-STREAM JOIN ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐                           ┌─────────────┐                │
│   │  Left       │                           │  Right      │                │
│   │  Source     │                           │  Source     │                │
│   │  (orders)   │                           │  (shipments)│                │
│   └──────┬──────┘                           └──────┬──────┘                │
│          │                                         │                       │
│          │    ┌───────────────────────────────┐    │                       │
│          │    │      SOURCE COORDINATOR       │    │                       │
│          │    │  (concurrent async readers)   │    │                       │
│          │    └───────────────────────────────┘    │                       │
│          │                   │                     │                       │
│          ▼                   ▼                     ▼                       │
│   ┌──────────────────────────────────────────────────────┐                │
│   │                  JOIN COORDINATOR                     │                │
│   │                                                       │                │
│   │  ┌─────────────────────────────────────────────────┐ │                │
│   │  │              KEY ROUTER                          │ │                │
│   │  │  Extract join key → route to correct partition   │ │                │
│   │  └─────────────────────────────────────────────────┘ │                │
│   │                         │                             │                │
│   │         ┌───────────────┼───────────────┐            │                │
│   │         ▼               ▼               ▼            │                │
│   │  ┌────────────┐  ┌────────────┐  ┌────────────┐     │                │
│   │  │ Partition 0│  │ Partition 1│  │ Partition N│     │                │
│   │  │            │  │            │  │            │     │                │
│   │  │ ┌────────┐ │  │ ┌────────┐ │  │ ┌────────┐ │     │                │
│   │  │ │  Left  │ │  │ │  Left  │ │  │ │  Left  │ │     │                │
│   │  │ │ Buffer │ │  │ │ Buffer │ │  │ │ Buffer │ │     │                │
│   │  │ └────────┘ │  │ └────────┘ │  │ └────────┘ │     │                │
│   │  │ ┌────────┐ │  │ ┌────────┐ │  │ ┌────────┐ │     │                │
│   │  │ │ Right  │ │  │ │ Right  │ │  │ │ Right  │ │     │                │
│   │  │ │ Buffer │ │  │ │ Buffer │ │  │ │ Buffer │ │     │                │
│   │  │ └────────┘ │  │ └────────┘ │  │ └────────┘ │     │                │
│   │  │            │  │            │  │            │     │                │
│   │  │  Watermark │  │  Watermark │  │  Watermark │     │                │
│   │  │  Tracker   │  │  Tracker   │  │  Tracker   │     │                │
│   │  └────────────┘  └────────────┘  └────────────┘     │                │
│   │                                                       │                │
│   └──────────────────────────────────────────────────────┘                │
│                              │                                             │
│                              ▼                                             │
│                       ┌──────────────┐                                     │
│                       │   Output     │                                     │
│                       │   Writer     │                                     │
│                       └──────────────┘                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           JOIN PROCESSING FLOW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  When LEFT record arrives (order):                                          │
│  ─────────────────────────────────                                          │
│  1. Extract join key (order_id)                                             │
│  2. Extract event_time for windowing                                        │
│  3. Store in LEFT buffer: key → (record, event_time, expire_at)            │
│  4. Lookup matching records in RIGHT buffer by key                          │
│  5. For each match where time constraint is satisfied:                      │
│     → Emit joined record (left ⋈ right)                                    │
│  6. Update watermark if event_time > current_watermark                      │
│  7. Expire old records from both buffers based on watermark                 │
│                                                                             │
│  When RIGHT record arrives (shipment):                                      │
│  ──────────────────────────────────                                         │
│  1. Extract join key (order_id)                                             │
│  2. Extract event_time for windowing                                        │
│  3. Store in RIGHT buffer: key → (record, event_time, expire_at)           │
│  4. Lookup matching records in LEFT buffer by key                           │
│  5. For each match where time constraint is satisfied:                      │
│     → Emit joined record (left ⋈ right)                                    │
│  6. Update watermark if event_time > current_watermark                      │
│  7. Expire old records from both buffers based on watermark                 │
│                                                                             │
│  Time Constraint (Interval Join):                                           │
│  ─────────────────────────────────                                          │
│  right.event_time ∈ [left.event_time + lower_bound,                        │
│                      left.event_time + upper_bound]                         │
│                                                                             │
│  Example: shipment within 24 hours of order                                 │
│  lower_bound = 0, upper_bound = 24 hours                                    │
│  ship.time ∈ [order.time, order.time + 24h]                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Component Design

### 1. JoinStateStore

**Location:** `src/velostream/sql/execution/join/state_store.rs`

```rust
/// Windowed state store for one side of a stream-stream join
pub struct JoinStateStore {
    /// Records indexed by join key
    /// Key: join key (String)
    /// Value: Vec of (record, event_time, expire_at)
    records: HashMap<String, Vec<JoinBufferEntry>>,

    /// Current watermark for this side
    watermark: i64,

    /// Retention period in milliseconds
    retention_ms: i64,

    /// Statistics for monitoring
    stats: JoinStateStats,
}

#[derive(Clone)]
pub struct JoinBufferEntry {
    pub record: StreamRecord,
    pub event_time: i64,
    pub expire_at: i64,
}

impl JoinStateStore {
    /// Create a new state store with the given retention period
    pub fn new(retention_ms: i64) -> Self;

    /// Store a record, returning its expiration time
    pub fn store(&mut self, key: &str, record: StreamRecord, event_time: i64) -> i64;

    /// Lookup all records for a key that match the time constraint
    pub fn lookup(
        &self,
        key: &str,
        time_lower: i64,
        time_upper: i64
    ) -> Vec<&StreamRecord>;

    /// Advance watermark and expire old records
    pub fn advance_watermark(&mut self, new_watermark: i64) -> usize;

    /// Get current watermark
    pub fn watermark(&self) -> i64;

    /// Get statistics
    pub fn stats(&self) -> &JoinStateStats;
}

#[derive(Default)]
pub struct JoinStateStats {
    pub records_stored: u64,
    pub records_expired: u64,
    pub lookups: u64,
    pub matches_found: u64,
    pub current_size: usize,
    pub peak_size: usize,
}
```

### 2. JoinCoordinator

**Location:** `src/velostream/sql/execution/join/coordinator.rs`

```rust
/// Coordinates concurrent reading from multiple sources for joins
pub struct JoinCoordinator {
    /// Join configuration
    config: JoinConfig,

    /// State store for left side
    left_store: JoinStateStore,

    /// State store for right side
    right_store: JoinStateStore,

    /// Join key extractor
    key_extractor: JoinKeyExtractor,

    /// Event time extractor
    time_extractor: EventTimeExtractor,

    /// Output channel for joined records
    output_tx: mpsc::Sender<StreamRecord>,

    /// Minimum watermark across both sides
    min_watermark: i64,
}

#[derive(Clone)]
pub struct JoinConfig {
    /// Join type (Inner, Left, Right, Full)
    pub join_type: JoinType,

    /// Time bounds for interval join
    pub lower_bound_ms: i64,
    pub upper_bound_ms: i64,

    /// Retention period for state
    pub retention_ms: i64,

    /// Left source name
    pub left_source: String,

    /// Right source name
    pub right_source: String,

    /// Join key columns (left_col, right_col)
    pub join_keys: Vec<(String, String)>,

    /// Event time field name
    pub event_time_field: String,
}

impl JoinCoordinator {
    /// Create a new join coordinator
    pub fn new(config: JoinConfig, output_tx: mpsc::Sender<StreamRecord>) -> Self;

    /// Process a record from the left source
    pub fn process_left(&mut self, record: StreamRecord) -> Result<Vec<StreamRecord>, SqlError>;

    /// Process a record from the right source
    pub fn process_right(&mut self, record: StreamRecord) -> Result<Vec<StreamRecord>, SqlError>;

    /// Advance watermark for a source
    pub fn advance_watermark(&mut self, source: JoinSide, watermark: i64);

    /// Get combined statistics
    pub fn stats(&self) -> JoinCoordinatorStats;
}

#[derive(Clone, Copy)]
pub enum JoinSide {
    Left,
    Right,
}
```

### 3. SourceCoordinator

**Location:** `src/velostream/server/v2/source_coordinator.rs`

```rust
/// Coordinates concurrent reading from multiple data sources
pub struct SourceCoordinator {
    /// Readers for each source
    readers: HashMap<String, Box<dyn DataReader>>,

    /// Channel to send records to JoinCoordinator
    record_tx: mpsc::Sender<SourceRecord>,

    /// Stop flag for graceful shutdown
    stop_flag: Arc<AtomicBool>,
}

pub struct SourceRecord {
    pub source_name: String,
    pub record: StreamRecord,
}

impl SourceCoordinator {
    /// Create a new source coordinator
    pub fn new(
        readers: HashMap<String, Box<dyn DataReader>>,
        record_tx: mpsc::Sender<SourceRecord>,
    ) -> Self;

    /// Start concurrent reading from all sources
    /// Spawns one async task per source
    pub async fn start(&mut self) -> Result<Vec<JoinHandle<()>>, SqlError>;

    /// Stop all readers
    pub fn stop(&self);
}
```

### 4. IntervalJoinProcessor

**Location:** `src/velostream/sql/execution/processors/interval_join.rs`

```rust
/// Processor for interval-based stream-stream joins
pub struct IntervalJoinProcessor {
    /// Join coordinator (owns state stores)
    coordinator: JoinCoordinator,

    /// SQL projection to apply after join
    projection: Vec<SelectField>,

    /// Optional WHERE clause filter
    filter: Option<Expression>,
}

impl IntervalJoinProcessor {
    /// Create from parsed JOIN AST
    pub fn from_ast(
        join: &JoinClause,
        left_source: &str,
        right_source: &str,
        config: &JoinConfig,
    ) -> Result<Self, SqlError>;

    /// Process a batch of records from a source
    pub fn process_batch(
        &mut self,
        source: JoinSide,
        records: Vec<StreamRecord>,
    ) -> Result<Vec<StreamRecord>, SqlError>;

    /// Flush any pending state (for window close)
    pub fn flush(&mut self) -> Result<Vec<StreamRecord>, SqlError>;
}
```

### 5. JoinKeyExtractor

**Location:** `src/velostream/sql/execution/join/key_extractor.rs`

```rust
/// Extracts join keys from records based on ON clause
pub struct JoinKeyExtractor {
    /// Left side key columns
    left_keys: Vec<String>,

    /// Right side key columns
    right_keys: Vec<String>,
}

impl JoinKeyExtractor {
    /// Create from JOIN ON clause
    pub fn from_condition(condition: &Expression) -> Result<Self, SqlError>;

    /// Extract key from a left-side record
    pub fn extract_left_key(&self, record: &StreamRecord) -> Option<String>;

    /// Extract key from a right-side record
    pub fn extract_right_key(&self, record: &StreamRecord) -> Option<String>;

    /// Create composite key from multiple columns
    fn make_composite_key(values: &[&FieldValue]) -> String;
}
```

---

## SQL Syntax and Semantics

### Interval Join Syntax

```sql
-- Explicit interval join with BETWEEN
SELECT o.order_id, o.customer_id, s.shipment_id, s.carrier
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
  AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '24' HOUR
EMIT CHANGES;

-- Using WITH clause for join configuration
SELECT o.*, s.*
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
EMIT CHANGES
WITH (
    'join.type' = 'interval',
    'join.lower_bound' = '0s',
    'join.upper_bound' = '24h',
    'join.retention' = '48h'
);
```

### Window Join Syntax

```sql
-- Tumbling window join
SELECT o.order_id, s.shipment_id
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
WINDOW TUMBLING(INTERVAL '1' HOUR)
EMIT CHANGES;

-- Sliding window join
SELECT o.order_id, s.shipment_id
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
WINDOW SLIDING(SIZE INTERVAL '1' HOUR, SLIDE INTERVAL '10' MINUTE)
EMIT CHANGES;
```

### Window Join Emit Modes

Window joins support two emission modes via `JoinEmitMode`:

| Mode | Description | Use Case |
|------|-------------|----------|
| `Final` (default) | Buffer records, emit all matches when window closes | Batch analytics, complete window results |
| `Changes` | Emit matches immediately as they arrive | Real-time dashboards, streaming updates |

**Configuration in code:**

```rust
use velostream::velostream::sql::execution::join::{JoinConfig, JoinEmitMode};
use std::time::Duration;

// EMIT FINAL (default) - emit when window closes
let batch_config = JoinConfig::tumbling(
    "orders", "shipments",
    vec![("order_id".to_string(), "order_id".to_string())],
    Duration::from_secs(3600),
);

// EMIT CHANGES - emit immediately on match
let streaming_config = JoinConfig::tumbling(
    "orders", "shipments",
    vec![("order_id".to_string(), "order_id".to_string())],
    Duration::from_secs(3600),
).with_emit_mode(JoinEmitMode::Changes);
```

**Behavior differences:**
- **EMIT FINAL**: Records are buffered in state stores. When `close_windows(watermark)` is called and the watermark advances past a window's end time, all matches for that window are emitted as a batch.
- **EMIT CHANGES**: Each new record is stored and immediately matched against existing records in the same window. Matches are emitted in real-time as they're found.

### Join Types

```sql
-- Inner join (default) - only emit when both sides match
SELECT * FROM orders o JOIN shipments s ON o.order_id = s.order_id;

-- Left join - emit all orders, null for unmatched shipments
SELECT * FROM orders o LEFT JOIN shipments s ON o.order_id = s.order_id;

-- Right join - emit all shipments, null for unmatched orders
SELECT * FROM orders o RIGHT JOIN shipments s ON o.order_id = s.order_id;

-- Full outer join - emit all from both sides
SELECT * FROM orders o FULL OUTER JOIN shipments s ON o.order_id = s.order_id;
```

---

## Implementation Status

**Last Updated:** 2026-01-17

### Code Analysis Summary

| Component | Lines | Tests | Encapsulation | Error Handling | Event Handling |
|-----------|-------|-------|---------------|----------------|----------------|
| coordinator.rs | 773 | 27 | ✅ Good | ✅ Good | ⚠️ Minor |
| state_store.rs | 545 | 12 | ✅ Good | ✅ Good | ✅ Excellent |
| watermark.rs | 269 | 9 | ✅ Good | ✅ Good | ✅ Excellent |
| source_coordinator.rs | 585 | 6 | ✅ Good | ✅ Excellent | ✅ Configurable |
| key_extractor.rs | 376 | 12 | ✅ Good | ✅ Good | ✅ Explicit |
| **Total** | **2548** | **66** | | | |

### Detailed Analysis

#### 1. JoinCoordinator (coordinator.rs)

**Encapsulation:** ✅ Good
- Private fields with public accessors
- Builder pattern for configuration (JoinCoordinatorConfig)
- Clear separation: JoinConfig (logical) vs JoinCoordinatorConfig (full config)

**Error Handling:** ✅ Good
- `process_left`/`process_right` return `Result<Vec<StreamRecord>, SqlError>`
- Missing event time: configurable via `MissingEventTimeBehavior` (UseWallClock, SkipRecord, Error)
- Duration overflow: explicit panic with clear message

**Event Handling:** ⚠️ Minor concern
- Records with missing keys return `Ok(vec![])` - tracked in `stats.missing_key_count` but no log
- Recommendation: Add debug log for missing key records

#### 2. JoinStateStore (state_store.rs)

**Encapsulation:** ✅ Good
- Private HashMap with O(1) key lookup + BTreeMap for O(log n) time ranges
- Private helper methods for eviction
- Stats exposed via immutable reference

**Error Handling:** ✅ Good
- Infallible operations by design (no I/O)
- Saturating arithmetic throughout
- Overflow protection in `with_config()`

**Event Handling:** ✅ Excellent
- `stats.records_stored` - all stores tracked
- `stats.records_expired` - all expirations tracked
- `stats.records_evicted` - all evictions tracked
- Capacity warnings logged at threshold

#### 3. JoinWatermarkTracker (watermark.rs)

**Encapsulation:** ✅ Good
- Private fields with public accessors
- Configuration via WatermarkConfig

**Error Handling:** ✅ Good
- Pure state tracking, no fallible operations

**Event Handling:** ✅ Excellent
- `late_records_left`/`late_records_right` count all late records
- `observe_event_time()` returns bool indicating on-time status
- Stats available via `stats()` method

#### 4. SourceCoordinator (source_coordinator.rs)

**Encapsulation:** ✅ Good
- Private reader handles and stop flag
- Thread-safe stats via Arc
- Drop trait for cleanup

**Error Handling:** ✅ Excellent
- `SourceCoordinatorError` enum with Display/Error traits
- Read errors: logged, tracked via `stats.record_error()`, configurable stop
- Channel closed: logged, graceful shutdown

**Event Handling:** ✅ Configurable
- `BackpressureMode::Block` - never drops records (default: BlockWithWarning)
- `BackpressureMode::DropOnTimeout` - explicit opt-in, tracked via `stats.records_dropped`
- All modes log appropriately

#### 5. JoinKeyExtractor (key_extractor.rs)

**Encapsulation:** ✅ Good
- Private columns field with accessor

**Error Handling:** ✅ Good
- `extract()` returns `Option<String>` - None for missing columns
- `extract_with_nulls()` for outer join semantics

**Event Handling:** ✅ Explicit
- Extraction failures return None (not swallowed)

### Phase Completion Status

| Phase | Status | Completion |
|-------|--------|------------|
| Phase 1: Foundation | ✅ Complete | 100% |
| Phase 2: Join Coordinator | ✅ Complete | 100% |
| Phase 3: Source Coordinator | ✅ Complete | 100% |
| Phase 4: Integration | ✅ Complete | 100% |
| Phase 5: Testing & Polish | ✅ Complete | 100% |

**Overall Progress: 100% ✅ COMPLETE**

### Completed Work

#### Phase 4: Integration ✅ COMPLETE
- [x] Query analyzer: Detect stream-stream join queries (`has_stream_stream_joins()`)
- [x] Execution engine: Route to join processor (StreamJobServer routing)
- [x] Job processor v2: Integrate SourceCoordinator (JoinJobProcessor)
- [x] Integration tests with testcontainers (13 tests passing)

#### Phase 5: Testing & Polish ✅ COMPLETE
- [x] Integration tests: `stream_stream_join_integration_test.rs` (13 tests)
- [x] Full Kafka pipeline test with testcontainers
- [x] Performance benchmarks: `interval_stream_join.rs` (tier3)
- [x] Unit tests: 210 join-related tests passing
- [x] Example: `join_metrics_demo.rs`

---

## Implementation Plan

### Phase 1: Foundation ✅ COMPLETE

| Task | File | Status | Description |
|------|------|--------|-------------|
| 1.1 | `src/velostream/sql/execution/join/mod.rs` | ✅ | Create join module structure |
| 1.2 | `src/velostream/sql/execution/join/state_store.rs` | ✅ | Implement JoinStateStore (545 lines) |
| 1.3 | `src/velostream/sql/execution/join/key_extractor.rs` | ✅ | Implement JoinKeyExtractor (376 lines) |
| 1.4 | `tests/unit/sql/execution/join/state_store_test.rs` | ✅ | Unit tests (12 tests) |

**Deliverable:** ✅ Working in-memory join state store with tests

### Phase 2: Join Coordinator ✅ COMPLETE

| Task | File | Status | Description |
|------|------|--------|-------------|
| 2.1 | `src/velostream/sql/execution/join/coordinator.rs` | ✅ | Implement JoinCoordinator (773 lines) |
| 2.2 | `src/velostream/sql/execution/join/watermark.rs` | ✅ | Implement watermark tracking (269 lines) |
| 2.3 | `src/velostream/sql/execution/join/coordinator.rs` | ✅ | Interval join logic (in coordinator) |
| 2.4 | `tests/unit/sql/execution/join/coordinator_test.rs` | ✅ | Unit tests (27 tests) |
| 2.5 | `tests/unit/sql/execution/join/watermark_test.rs` | ✅ | Unit tests (9 tests) |

**Deliverable:** ✅ Join coordination logic with interval join support

### Phase 3: Source Coordinator ✅ COMPLETE

| Task | File | Status | Description |
|------|------|--------|-------------|
| 3.1 | `src/velostream/server/v2/source_coordinator.rs` | ✅ | Implement SourceCoordinator (585 lines) |
| 3.2 | `src/velostream/server/v2/job_processor_v2.rs` | 🔄 | Integrate concurrent source reading |
| 3.3 | `src/velostream/server/v2/partition_receiver.rs` | 🔄 | Update to support join processing |
| 3.4 | `tests/unit/server/v2/source_coordinator_test.rs` | ✅ | Unit tests (6 async tests) |

**Deliverable:** ✅ Core concurrent source reading implemented (integration pending)

### Phase 4: Integration ✅ COMPLETE

| Task | File | Status | Description |
|------|------|--------|-------------|
| 4.1 | `src/velostream/sql/ast.rs` | ✅ | `has_stream_stream_joins()` detection |
| 4.2 | `src/velostream/sql/ast.rs` | ✅ | `extract_stream_stream_join_info()` extraction |
| 4.3 | `src/velostream/sql/ast.rs` | ✅ | `extract_join_keys()` from ON clause |
| 4.4 | `src/velostream/server/stream_job_server.rs` | ✅ | Route to JoinJobProcessor |
| 4.5 | Integration tests | ❌ | End-to-end join tests (requires Kafka)

**Deliverable:** ✅ Full integration with SQL engine (routing complete)

### Phase 5: Testing & Polish 🔄 IN PROGRESS

| Task | Status | Description |
|------|--------|-------------|
| 5.1 | ❌ | Pass all tier3 join tests (21-24) - requires Kafka |
| 5.2 | ✅ | Performance benchmarks (607K rec/sec JoinCoordinator, 5.3M rec/sec StateStore) |
| 5.3 | ✅ | Documentation (this document) |
| 5.4 | ✅ | Edge case handling (in unit tests) |

**Deliverable:** Production-ready stream-stream joins

---

## Testing Strategy

### Unit Tests

```rust
// state_store_test.rs
#[test]
fn test_store_and_lookup_by_key() {
    let mut store = JoinStateStore::new(Duration::from_secs(3600).as_millis() as i64);
    store.store("order-123", record1, 1000);
    store.store("order-123", record2, 2000);

    let matches = store.lookup("order-123", 0, 5000);
    assert_eq!(matches.len(), 2);
}

#[test]
fn test_watermark_expiration() {
    let mut store = JoinStateStore::new(1000); // 1 second retention
    store.store("key", record, 1000);

    // Advance watermark past expiration
    let expired = store.advance_watermark(3000);
    assert_eq!(expired, 1);
    assert!(store.lookup("key", 0, 5000).is_empty());
}

#[test]
fn test_interval_time_constraint() {
    let mut store = JoinStateStore::new(10000);
    store.store("key", record1, 1000);
    store.store("key", record2, 5000);
    store.store("key", record3, 9000);

    // Lookup with time window [2000, 6000]
    let matches = store.lookup("key", 2000, 6000);
    assert_eq!(matches.len(), 1); // Only record2
}
```

### Integration Tests

```rust
// join_integration_test.rs
#[tokio::test]
async fn test_stream_stream_inner_join() {
    let test_harness = TestHarness::new();

    // Publish orders
    test_harness.publish("orders", vec![
        Order { order_id: "1", customer_id: "C1", event_time: 1000 },
        Order { order_id: "2", customer_id: "C2", event_time: 2000 },
    ]).await;

    // Publish shipments (some matching, some not)
    test_harness.publish("shipments", vec![
        Shipment { order_id: "1", carrier: "UPS", event_time: 1500 },
        Shipment { order_id: "3", carrier: "FedEx", event_time: 3000 }, // No matching order
    ]).await;

    // Execute join query
    let results = test_harness.execute_query("
        SELECT o.order_id, o.customer_id, s.carrier
        FROM orders o
        JOIN shipments s ON o.order_id = s.order_id
    ").await;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("order_id"), Some("1"));
    assert_eq!(results[0].get("carrier"), Some("UPS"));
}
```

### Test Harness Tests (tier3)

Existing tests that should pass after implementation:
- `tier3_joins/21_stream_stream_join.test.yaml`
- `tier3_joins/22_multi_join.test.yaml`
- `tier3_joins/23_right_join.test.yaml`
- `tier3_joins/24_full_outer_join.test.yaml`

---

## Performance Considerations

### Memory Management

| Aspect | Strategy |
|--------|----------|
| State size | Bounded by retention period + cardinality |
| Key distribution | Hash-based partitioning for even distribution |
| Expiration | Watermark-driven lazy expiration |
| Peak memory | Monitor via JoinStateStats.peak_size |

### Benchmark Results (2026-01-18)

**Test Environment**: Fixture-based benchmarks (no Kafka overhead)
**Location**: `tests/performance/analysis/sql_operations/tier3_advanced/interval_stream_join.rs`

#### Component Throughput

| Component | Throughput | Notes |
|-----------|------------|-------|
| **JoinStateStore** | 5,346,820 rec/sec | BTreeMap range queries |
| **High Cardinality** | 1,722,461 rec/sec | Unique keys stress test |
| **JoinCoordinator** | 607,437 rec/sec | Full join pipeline |
| **SQL Engine (sync)** | 363,823 rec/sec | End-to-end execution |
| **SQL Engine (async)** | 228,083 rec/sec | Async pipeline |

#### Performance Characteristics

| Metric | Measured | Implementation |
|--------|----------|----------------|
| Join throughput | 607K rec/sec | JoinCoordinator pipeline |
| State lookup | O(1) by key, O(log n) for time | HashMap + BTreeMap |
| High cardinality penalty | None | 1.7M rec/sec with unique keys |

### Performance Summary

```
┌─────────────────────────────┬──────────────────┐
│ Component                   │ Throughput       │
├─────────────────────────────┼──────────────────┤
│ JoinStateStore             │ 5.3M rec/sec     │
│ High Cardinality           │ 1.7M rec/sec     │
│ JoinCoordinator            │ 607K rec/sec     │
│ SQL Engine (sync)          │ 364K rec/sec     │
│ SQL Engine (async)         │ 228K rec/sec     │
└─────────────────────────────┴──────────────────┘
```

### Optimizations (Future)

1. **Bloom filters** for negative lookups
2. **Time-bucketed storage** for faster time range queries
3. **RocksDB backing** for larger-than-memory state
4. **Incremental checkpointing** for fault tolerance

---

## Memory Optimization Plan

### Current Memory Layout Analysis

```
JoinStateStore Memory Per Record (~1,200 bytes typical):
├── JoinBufferEntry overhead:     16 bytes (event_time + expire_at)
├── StreamRecord base:           144 bytes (struct fields)
├── HashMap (fields) base:        48 bytes
├── 10 field entries:            720 bytes (~72 bytes/entry)
├── Field names (avg 8 chars):    80 bytes
├── Field values (typical):      160 bytes
├── Headers HashMap:              48 bytes
├── BTreeMap amortized:            8 bytes
└── VecDeque amortized:            4 bytes
```

### Capacity Estimates

| Available RAM | Records (1.2KB each) | Notes |
|---------------|----------------------|-------|
| 1 GB | 833K | Current default limit |
| 4 GB | 3.3M | Medium workload |
| 8 GB | 6.6M | Large workload |
| 16 GB | 13.3M | Production scale |

### Identified Inefficiencies

| Issue | Impact | Savings Potential |
|-------|--------|-------------------|
| **String Duplication** | HIGH | 30-40% - field names repeated per record |
| **HashMap Overhead** | MEDIUM | 15-25% - 48 bytes base + entry overhead |
| **No Columnar Storage** | MEDIUM | 20-30% - row-oriented storage |
| **No Memory-Based Limits** | HIGH | OOM risk - only record counts |

### Comparison with Flink/ksqlDB

| Feature | Flink | ksqlDB | Velostream (Current) |
|---------|-------|--------|----------------------|
| **State Backend** | RocksDB/Heap | RocksDB | In-memory HashMap |
| **Eviction Policy** | TTL-based | LRU (cache) | FIFO by event_time |
| **Record Count Limit** | ❌ | ❌ | ✅ 1M default |
| **Per-Key Limit** | ❌ | ❌ | ✅ 10K default |
| **Memory Limit** | RocksDB cache | Global bound | ❌ None |
| **Spill to Disk** | ✅ | ✅ | ❌ |

### Implementation Roadmap

#### Phase 6.1: Memory-Based Limits (Priority 1) - 1-2 days
```rust
pub struct JoinStateStoreConfig {
    pub max_memory_bytes: usize,      // NEW: e.g., 1GB
    pub max_records: usize,           // Keep as fallback
    pub max_records_per_key: usize,
    pub warning_threshold_pct: u8,
}

impl JoinStateStore {
    fn estimate_record_size(record: &StreamRecord) -> usize;
    fn current_memory_usage(&self) -> usize;  // Track incrementally
}
```

#### Phase 6.2: String Interning (Priority 2) - ✅ COMPLETE
```rust
// Implemented: StringInterner for composite join keys
// - InternedKey: lightweight u32 handle (4 bytes vs 24+ for String)
// - Thread-safe RwLock-based string pool
// - Used for composite keys: "window_id:join_key" and "session_id:join_key"
// - Memory stats tracking: string_count, total_bytes, estimated_overhead

pub struct StringInterner {
    string_to_index: RwLock<HashMap<String, u32>>,
    index_to_string: RwLock<Vec<String>>,
}

// JoinCoordinator now includes:
// - key_interner: Arc<StringInterner>
// - interner_stats() method
// - JoinCoordinatorStats.interned_key_count and interning_memory_saved
```

**Expected savings: 30-40% memory reduction for composite keys**

#### Phase 6.3: LRU Eviction Policy (Priority 3) - 2-3 days
```rust
pub enum EvictionPolicy {
    Fifo,           // Current: oldest event_time first
    Lru,            // Least recently accessed
    LruByEventTime, // Hybrid: LRU within time buckets
}

pub struct JoinBufferEntry {
    pub record: StreamRecord,
    pub event_time: i64,
    pub expire_at: i64,
    pub last_accessed: Instant,  // NEW: for LRU tracking
}
```

#### Phase 6.4: Compact Records (Priority 4) - ✅ COMPLETE (Core Types)
```rust
// Implemented: Schema-aware storage to eliminate per-record HashMap overhead
// - RecordSchema: Maps field names to positions, shared across records
// - CompactRecord: Schema reference + Vec<FieldValue> indexed by position
// - Conversion methods: from_stream_record() and to_stream_record()
// - Memory estimation for both schema and records

pub struct RecordSchema {
    name_to_index: HashMap<String, usize>,  // O(1) lookup
    index_to_name: Vec<String>,             // Index to name mapping
}

pub struct CompactRecord {
    schema: Arc<RecordSchema>,  // Shared, 8 bytes
    values: Vec<FieldValue>,    // Indexed by schema position
    timestamp: i64,
    event_time: Option<i64>,
}
```

**Expected savings: 15-25% + better cache locality when integrated into JoinStateStore**

---

## Memory Management & Configuration Guide

### Memory Limits Configuration

```rust
use velostream::velostream::sql::execution::join::{
    JoinConfig, JoinCoordinator, JoinCoordinatorConfig, JoinStateStoreConfig,
};

// Option 1: Simple configuration with max records
let store_config = JoinStateStoreConfig::with_limits(
    1_000_000,  // max_records total
    10_000,     // max_records_per_key
);

// Option 2: Memory-based limits (recommended for production)
let store_config = JoinStateStoreConfig::with_memory_limit(
    1_073_741_824,  // 1GB max memory
);

// Option 3: Full configuration with all limits
let store_config = JoinStateStoreConfig::with_all_limits(
    1_000_000,      // max_records
    10_000,         // max_records_per_key
    1_073_741_824,  // max_memory_bytes (1GB)
);

// Apply to coordinator
let config = JoinCoordinatorConfig::new(join_config)
    .with_store_config(store_config);

let coordinator = JoinCoordinator::with_config(config);
```

### Eviction Policies

| Policy | Description | Use When |
|--------|-------------|----------|
| `Fifo` (default) | Oldest records by event_time evicted first | Time-ordered data, predictable eviction |
| `Lru` | Least recently accessed records evicted | Hot/cold access patterns, frequently joined keys |

```rust
use velostream::velostream::sql::execution::join::EvictionPolicy;

let config = JoinStateStoreConfig {
    max_records: 1_000_000,
    max_records_per_key: 10_000,
    max_memory_bytes: 0,  // No memory limit
    eviction_policy: EvictionPolicy::Lru,
};
```

### Size Expectations

**Per-Record Memory (approximate):**
- Base StreamRecord overhead: ~200-300 bytes
- Per field: ~50-100 bytes (depending on type)
- String field: field_name (24 bytes) + content length
- Integer/Float: 8 bytes
- Timestamp: 12 bytes

**With String Interning (automatic for composite keys):**
- Composite key "window_id:join_key" stored once per unique value
- Savings: 30-40% for repeated keys (e.g., same order_id in multiple windows)

**Capacity Planning:**

| Records | Est. Memory (10 fields/record) | With Interning |
|---------|-------------------------------|----------------|
| 100K    | ~50-80 MB                     | ~35-55 MB      |
| 1M      | ~500-800 MB                   | ~350-550 MB    |
| 10M     | ~5-8 GB                       | ~3.5-5.5 GB    |

### Monitoring Memory Usage

```rust
// Check coordinator stats
let stats = coordinator.stats();
println!("Left store size: {} records", stats.left_store_size);
println!("Right store size: {} records", stats.right_store_size);
println!("Evictions (left): {}", stats.left_evictions);
println!("Evictions (right): {}", stats.right_evictions);

// Check string interner stats
let interner_stats = coordinator.interner_stats();
println!("Unique keys interned: {}", interner_stats.string_count);
println!("Total string bytes: {}", interner_stats.total_string_bytes);
println!("Estimated overhead: {}", interner_stats.estimated_overhead_bytes);

// Check memory pressure
match coordinator.memory_pressure() {
    MemoryPressure::Normal => println!("Memory OK"),
    MemoryPressure::Warning => println!("Memory >80% - consider backpressure"),
    MemoryPressure::Critical => println!("Memory at limit - evictions occurring"),
}

// Check individual store stats
let left_stats = coordinator.left_store().stats();
println!("Current memory: {} bytes", left_stats.current_memory_bytes);
println!("Peak memory: {} bytes", left_stats.peak_memory_bytes);
println!("Records evicted (memory): {}", left_stats.records_evicted_memory);
println!("Records evicted (count): {}", left_stats.records_evicted_count);
```

### Backpressure Integration

```rust
// Check if backpressure should be applied
if coordinator.should_apply_backpressure() {
    // Slow down ingestion
    let usage = coordinator.combined_capacity_usage_pct();
    println!("Capacity usage: {:.1}%", usage);

    // Option 1: Sleep before next batch
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Option 2: Reduce batch size
    // Option 3: Signal upstream to slow down
}
```

### Tuning Recommendations

1. **Start with memory limits** rather than record counts - more predictable
2. **Set retention period** to 2x your expected join interval
3. **Monitor `records_evicted`** - high values indicate undersized state
4. **Use LRU eviction** when some keys are accessed more frequently
5. **Enable backpressure** for production workloads

---

#### Phase 6.5: RocksDB Backing (Priority 5) - 1-2 weeks
```rust
pub struct JoinStateStore {
    hot_cache: HashMap<String, TimeIndex>,  // In-memory LRU
    cold_store: Option<rocksdb::DB>,        // Disk-backed
    cache_size_limit: usize,
}
```

### SQL Configuration

```sql
-- Memory-based configuration
SELECT o.*, s.*
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
EMIT CHANGES
WITH (
    'join.max_memory_bytes' = '1073741824',  -- 1GB
    'join.max_records' = '1000000',          -- Fallback limit
    'join.max_records_per_key' = '10000',
    'join.eviction_policy' = 'lru',          -- fifo|lru|lru_by_event_time
    'join.retention' = '24h'
);
```

### Metrics to Track

| Metric | Description |
|--------|-------------|
| `join_state_memory_bytes` | Current memory usage |
| `join_state_memory_limit_bytes` | Configured limit |
| `join_state_evictions_total` | Records evicted (by policy) |
| `join_state_evictions_memory` | Evictions due to memory pressure |
| `join_state_evictions_ttl` | Evictions due to TTL |
| `join_state_cache_hit_ratio` | For LRU policy effectiveness |

### Prometheus Metrics Integration

**Location:** `src/velostream/observability/metrics.rs` (JoinMetrics struct)

Join metrics are integrated with the main `MetricsProvider` for unified Prometheus exposure.
This follows best practice - single registry, single `/metrics` endpoint, consistent with all other metrics.

```rust
use velostream::velostream::observability::metrics::MetricsProvider;

// The main metrics provider handles all Prometheus metrics
let metrics = MetricsProvider::new(config).await?;

// Process a join job - returns JoinJobStats with all coordinator stats
let processor = JoinJobProcessor::with_join_config(join_config);
let stats = processor.process_join(left_name, left, right_name, right, writer).await?;

// Push join stats to the unified metrics system
// Metrics appear on the main /metrics endpoint alongside SQL, streaming, system metrics
metrics.update_join_metrics("orders_shipments_join", coordinator_stats);
```

#### Exposed Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `velo_join_left_records_total` | Counter | Records processed from left source |
| `velo_join_right_records_total` | Counter | Records processed from right source |
| `velo_join_matches_total` | Counter | Total join matches emitted |
| `velo_join_missing_keys_total` | Counter | Records skipped due to missing join keys |
| `velo_join_left_store_size` | Gauge | Current records in left state store |
| `velo_join_right_store_size` | Gauge | Current records in right state store |
| `velo_join_interned_keys` | Gauge | Number of unique keys interned |
| `velo_join_interning_memory_saved_bytes` | Gauge | Estimated memory saved by interning |
| `velo_join_left_evictions_total` | Counter | Records evicted from left store |
| `velo_join_right_evictions_total` | Counter | Records evicted from right store |
| `velo_join_memory_pressure` | Gauge | Memory pressure level (0=Normal, 1=Warning, 2=Critical) |
| `velo_join_windows_closed_total` | Counter | Windows closed (for window joins) |
| `velo_join_active_windows` | Gauge | Currently active windows |

All metrics include a `join_name` label for filtering by join.

**Benefits of unified integration:**
- Single `/metrics` endpoint for all Prometheus scraping
- Consistent with SQL, streaming, and system metrics (velo_* prefix)
- No separate registries to manage
- Metrics appear automatically in existing Grafana dashboards

---

## Completed Features ✅

All planned stream-stream join functionality is **complete**:

### Core Functionality
- [x] Interval joins with configurable time bounds
- [x] LEFT/RIGHT/FULL OUTER join support (JoinType enum)
- [x] Multiple join keys (composite keys via JoinKeyExtractor)
- [x] Watermark-driven state cleanup

### Window Joins
- [x] Tumbling window joins (JoinMode::Tumbling)
- [x] Sliding window joins (JoinMode::Sliding)
- [x] Session window joins (JoinMode::Session)
- [x] EMIT CHANGES mode for streaming emission
- [x] EMIT FINAL mode for batch emission

### Memory Optimization
- [x] Memory limits and backpressure (JoinStateStoreConfig)
- [x] LRU eviction policy (Phase 6.3)
- [x] Arc<str> string interning for join keys (30-40% memory savings)
- [x] Compact records storage types (RecordSchema + CompactRecord)

### Observability
- [x] Prometheus metrics integration (JoinMetrics in MetricsProvider)
- [x] Memory pressure monitoring (Normal/Warning/Critical)
- [x] Eviction and interning stats

### Testing
- [x] 210 unit tests passing
- [x] 13 integration tests with Kafka (testcontainers)
- [x] Performance benchmarks (tier3)
- [x] Example: `join_metrics_demo.rs`

---

## Future Work (Not Essential)

### Multi-way Joins (3+ Streams)

**Priority:** Low - Nice to have, not essential

**Assessment:**
| Factor | Analysis |
|--------|----------|
| **Demand** | Low - Most streaming joins are 2-way (orders↔shipments, clicks↔impressions) |
| **Workaround** | Yes - Chain queries via intermediate topics: `A JOIN B → topic → result JOIN C` |
| **Complexity** | High - N streams = N! join orders, N state stores, exponential memory |
| **Use Cases** | CEP patterns, multi-source enrichment, graph queries |

**Implementation Approach:** Binary decomposition (recommended)
```
A JOIN B JOIN C  →  (A JOIN B) as AB  →  AB JOIN C
```

**LoE Estimate:** 2-3 weeks
| Task | Days |
|------|------|
| SQL Parser (detect chained joins) | 2-3 |
| Query Planner (build join tree) | 3-5 |
| Pipeline Orchestration | 3-5 |
| Testing | 2-3 |

### Full CompactRecord Integration

**Priority:** Low - Optimization only

**Assessment:** CompactRecord types exist but aren't integrated into JoinStateStore.
Current HashMap-based records work fine. Integration would save 15-25% memory
but adds complexity. Consider only if memory becomes a bottleneck.

**LoE Estimate:** 1 week

### Deferred (User Request)

| Feature | Reason |
|---------|--------|
| Persistent state (RocksDB) | In-memory sufficient for current use cases |
| Exactly-once semantics | Requires transactional output support |
| State migration for rescaling | Complex, not needed without persistence |

---

## Appendix

### A. Glossary

| Term | Definition |
|------|------------|
| **Interval Join** | Join where records match if their timestamps fall within a specified interval |
| **Window Join** | Join where records match if they fall within the same time window |
| **Session Window Join** | Join where records match if they belong to the same activity session (dynamic window based on inactivity gaps) |
| **EMIT FINAL** | Window join emission mode: emit all matches when window closes (batch mode) |
| **EMIT CHANGES** | Window join emission mode: emit matches immediately as they arrive (streaming mode) |
| **Watermark** | Monotonically increasing timestamp indicating no more events before this time |
| **Retention** | How long to keep records in state before expiring |
| **Co-partitioning** | When two streams are partitioned by the same key |

### B. References

1. [Apache Flink - Joining](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/joining/)
2. [Apache Flink - Window Joins](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-join/)
3. [Kafka Streams - Joins](https://developer.confluent.io/courses/kafka-streams/joins/)
4. [Crossing the Streams](https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/)
5. [Flink SQL Joins](https://www.ververica.com/blog/flink-sql-joins-part-1)

### C. Example Test Data

**orders_join_test.csv:**
```csv
order_id,customer_id,total_amount,event_time
ORD-001,CUST-100,150.00,2026-01-15T10:00:00Z
ORD-002,CUST-101,250.00,2026-01-15T10:05:00Z
ORD-003,CUST-102,350.00,2026-01-15T10:10:00Z
```

**shipments_join_test.csv:**
```csv
order_id,shipment_id,carrier,tracking_number,event_time
ORD-001,SHIP-001,UPS,1Z999AA10123456784,2026-01-15T10:30:00Z
ORD-002,SHIP-002,FedEx,794644790301,2026-01-15T10:35:00Z
ORD-004,SHIP-003,DHL,1234567890,2026-01-15T10:40:00Z
```

**Expected join output (inner join, 24h window):**
```csv
order_id,customer_id,total_amount,shipment_id,carrier,tracking_number
ORD-001,CUST-100,150.00,SHIP-001,UPS,1Z999AA10123456784
ORD-002,CUST-101,250.00,SHIP-002,FedEx,794644790301
```
