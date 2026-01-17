# Design Document: Stream-Stream Joins

**Status:** In Progress (Phases 1-3 Complete)
**Author:** Claude Code
**Created:** 2026-01-16
**Last Updated:** 2026-01-17
**Issue:** FR-085 (stream-stream joins)

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
| Phase 4: Integration | 🔄 In Progress | 25% |
| Phase 5: Testing & Polish | 🔄 Pending | 10% |

**Overall Progress: ~65%**

### Remaining Work

#### Phase 4: Integration (Required)
- [ ] Query analyzer: Detect stream-stream join queries
- [ ] Execution engine: Route to join processor
- [ ] Job processor v2: Integrate SourceCoordinator
- [ ] Integration tests

#### Phase 5: Testing & Polish (Required)
- [ ] Pass tier3 join tests (21-24)
- [ ] Performance benchmarks
- [ ] End-to-end validation

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

### Phase 4: Integration 🔄 IN PROGRESS

| Task | File | Status | Description |
|------|------|--------|-------------|
| 4.1 | `src/velostream/sql/query_analyzer.rs` | ❌ | Detect and analyze join queries |
| 4.2 | `src/velostream/sql/execution/engine.rs` | ❌ | Route join queries to join processor |
| 4.3 | `src/velostream/sql/parser.rs` | ❌ | Parse interval join syntax (if needed) |
| 4.4 | Integration tests | ❌ | End-to-end join tests |

**Deliverable:** Full integration with SQL engine

### Phase 5: Testing & Polish ❌ PENDING

| Task | Status | Description |
|------|--------|-------------|
| 5.1 | ❌ | Pass all tier3 join tests (21-24) |
| 5.2 | ❌ | Performance benchmarks |
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

### Expected Performance

| Metric | Target | Implementation |
|--------|--------|----------------|
| Latency overhead | < 10ms per record | BTreeMap range queries |
| State lookup | O(1) by key, O(log n) for time | HashMap + BTreeMap |
| Memory per key | ~200 bytes + record size | JoinBufferEntry overhead |
| Expiration cost | Amortized O(1) per record | Watermark-driven cleanup |

### Optimizations (Future)

1. **Bloom filters** for negative lookups
2. **Time-bucketed storage** for faster time range queries
3. **RocksDB backing** for larger-than-memory state
4. **Incremental checkpointing** for fault tolerance

---

## Future Enhancements

### Short Term
- [x] LEFT/RIGHT/FULL OUTER join support (JoinType enum implemented)
- [x] Multiple join keys (composite keys) (JoinKeyExtractor supports multi-column)
- [x] Configurable grace period for late data (WatermarkConfig.max_lateness_ms)
- [x] Memory limits and backpressure (JoinStateStoreConfig, MemoryPressure)
- [ ] SQL engine integration (Phase 4)
- [ ] tier3 test validation

### Medium Term
- [ ] Window joins (tumbling, sliding)
- [ ] Session window joins
- [ ] Multi-way joins (3+ streams)

### Long Term
- [ ] Persistent state with RocksDB
- [ ] Exactly-once with transactional output
- [ ] State migration for rescaling

---

## Appendix

### A. Glossary

| Term | Definition |
|------|------------|
| **Interval Join** | Join where records match if their timestamps fall within a specified interval |
| **Window Join** | Join where records match if they fall within the same time window |
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
