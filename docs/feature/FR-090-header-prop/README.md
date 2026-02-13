# Header Propagation Analysis for Velostream

## Executive Summary

This document analyzes how streaming SQL engines handle Kafka message header propagation through various SQL operations,
with specific focus on aggregations, joins, and window functions. We also examine distributed tracing requirements and
propose a design for Velostream.

### Current Implementation Status (2026-02-10)

| Operation                            | FR-090 Expected    | Status   | Implementation                                                    |
|--------------------------------------|--------------------|----------|-------------------------------------------------------------------|
| SELECT (passthrough)                 | Preserve           | **DONE** | `select.rs` — headers flow through                                |
| WHERE (filter)                       | Preserve           | **DONE** | Same path as SELECT                                               |
| Projection                           | Preserve           | **DONE** | Same path as SELECT                                               |
| ROWS OVER window                     | Preserve           | **DONE** | 1:1 output per input                                              |
| GROUP BY aggregation                 | Last-event-wins    | **DONE** | `engine.rs` — `with_headers_from(sample_record)`                  |
| TUMBLING/HOP/SESSION WINDOW          | Last-event-wins    | **DONE** | `adapter.rs` — `with_headers_from(sample_record)`                 |
| JOIN (inner)                         | Left-side default  | **DONE** | `join.rs` — `left_record.headers.clone()`                         |
| SET_HEADER / REMOVE_HEADER           | Apply mutations    | **DONE** | `engine.rs` — `apply_header_mutations_to_record()`                |
| `_event_time` header inject          | On write           | **DONE** | `writer.rs` — injected by KafkaDataWriter                         |
| Distributed tracing injection        | Post-SQL overwrite | **DONE** | `observability_helper.rs` — `inject_trace_context_into_records()` |
| `_event_time` safety in aggregations | Strip stale header | **DONE** | `with_headers_from()` removes `_event_time`                       |
| `@propagate_headers` annotation      | Configurable       | DEFERRED | Phase 2                                                           |
| `@join_header_source` annotation     | Configurable       | DEFERRED | Phase 2                                                           |
| Header SQL functions                 | FIRST_HEADER, etc. | DEFERRED | Phase 2                                                           |

**Key safety guarantees**:

- Distributed tracing NOT broken: `inject_trace_context_into_records()` overwrites `traceparent`/`tracestate` after SQL
  execution
- `_event_time` NOT broken: `with_headers_from()` strips stale `_event_time` header, allowing KafkaDataWriter to inject
  correct output event_time
- GROUP BY correctness preserved: all records in a group share GROUP BY key values, so last-vs-first doesn't affect key
  field correctness

---

## 1. Industry Analysis

### 1.1 Apache Flink SQL

**Approach**: Metadata as Virtual Columns

Flink SQL exposes Kafka metadata (including headers) as **virtual columns** declared in the table definition:

```sql
CREATE TABLE kafka_table
(
    `_key`       BYTES METADATA FROM 'key',
    `_topic`     STRING METADATA FROM 'topic' VIRTUAL,
    `_partition` INT METADATA FROM 'partition' VIRTUAL,
    `_headers`   MAP<STRING,
    BYTES> METADATA FROM 'headers' VIRTUAL,
    `_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    -- data columns
    symbol       STRING,
    price DOUBLE
) WITH ('connector' = 'kafka',...);
```

**Header Handling in Transformations**:

| Operation                | Flink Behavior                                       |
|--------------------------|------------------------------------------------------|
| Passthrough (`SELECT *`) | Headers accessible if declared as column             |
| Filter (`WHERE`)         | Headers preserved                                    |
| Projection               | Headers preserved if selected                        |
| Aggregation (`GROUP BY`) | Headers NOT preserved (multiple inputs → one output) |
| Window Aggregation       | Headers NOT preserved                                |
| Join                     | Headers NOT preserved (ambiguous source)             |

**Key Insight**: Flink treats headers as **data columns** that must be explicitly selected. If you want headers on
output, you must include them in SELECT and handle conflicts manually.

Sources:

- [Flink Window Aggregation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-agg/)
- [Confluent Flink SQL Window TVF](https://docs.confluent.io/cloud/current/flink/reference/queries/window-tvf.html)

---

### 1.2 Kafka Streams

**Approach**: Explicit Header Control via Processor API

Kafka Streams has evolved header support through several KIPs:

| KIP     | Feature                                |
|---------|----------------------------------------|
| KIP-82  | Headers introduced to Kafka            |
| KIP-244 | Header access in Processor API         |
| KIP-634 | DSL-level header support (in progress) |

**Current Limitations
** ([KIP-634](https://cwiki.apache.org/confluence/display/KAFKA/KIP-634:+Complementary+support+for+headers+and+record+metadata+in+Kafka+Streams+DSL)):

> "Using header values in more complex computations already available in DSL as joins/aggregations is more complex, as
> it will require reimplementing joins/aggregations with custom Processor."

**Header Propagation Rules**:

| Operation            | Kafka Streams Behavior                                      |
|----------------------|-------------------------------------------------------------|
| `map()` / `filter()` | Headers auto-propagated via `context.forward()`             |
| `flatMap()`          | Headers propagated to all output records                    |
| `aggregate()`        | Headers **dropped** - not stored in state stores            |
| `join()`             | Headers from **left** side only (by default)                |
| Changelog topics     | Headers **explicitly dropped** - not needed for restoration |

**The "Missing Headers" Problem
** ([Medium Article](https://tkaszuba.medium.com/the-curious-case-of-missing-headers-in-kafka-streams-c0099d077938)):

> "Once you try to use Kafka headers in Kafka Streams you quickly find out that there is no support for them when doing
> any sort of stateful transformations."

Sources:

- [Kafka Streams Access Headers](https://medium.com/@pavan.sarvan/kafka-streams-access-headers-423318c60113)
- [Kafka Streams Processor API](https://docs.confluent.io/platform/current/streams/developer-guide/processor-api.html)

---

### 1.3 ksqlDB

**Approach**: System Columns + Functions

ksqlDB provides pseudo-columns and functions for header access:

```sql
-- Access headers via HEADERS pseudo-column
SELECT HEADERS ->>'trace-id' as trace_id, symbol, price
FROM trades;

-- Check for header existence
SELECT *
FROM trades
WHERE HEADER('correlation-id') IS NOT NULL;
```

**Header Behavior**:

- Headers accessible as system columns
- NOT automatically propagated through aggregations
- Must be explicitly carried through if needed

Sources:

- [Lenses.io: Kafka Headers with SQL](https://lenses.io/blog/2020/12/kafka-distributed-tracing-with-message-headers/)

---

## 2. Operation-Specific Analysis

### 2.1 Simple Transformations (1:1)

**Operations**: `SELECT`, `WHERE`, `CASE`, scalar functions

**Recommendation**: **Preserve all headers**

```
Input Record          Output Record
┌─────────────────┐   ┌─────────────────┐
│ headers: {      │   │ headers: {      │
│   trace-id: X   │──▶│   trace-id: X   │  ✓ PRESERVE
│   corr-id: Y    │   │   corr-id: Y    │
│ }               │   │ }               │
│ value: {...}    │   │ value: {...}    │
└─────────────────┘   └─────────────────┘
```

**Rationale**: One input → one output. No ambiguity.

---

### 2.2 Aggregations (N:1)

**Operations**: `COUNT`, `SUM`, `AVG`, `GROUP BY`

**Challenge**: Multiple input records with potentially different headers merge into one output.

```
Input Records              Output Record
┌─────────────────┐        ┌─────────────────┐
│ trace-id: A     │        │ headers: ???    │
│ symbol: AAPL    │        │                 │
│ price: 150      │        │ symbol: AAPL    │
├─────────────────┤   ──▶  │ total: 450      │
│ trace-id: B     │        │ count: 3        │
│ symbol: AAPL    │        │                 │
│ price: 150      │        └─────────────────┘
├─────────────────┤
│ trace-id: C     │
│ symbol: AAPL    │
│ price: 150      │
└─────────────────┘
```

**Industry Solutions**:

| Approach           | Description                                          | Used By                 |
|--------------------|------------------------------------------------------|-------------------------|
| Drop all           | Don't propagate any headers                          | Kafka Streams (default) |
| First wins         | Use headers from first record in group               | -                       |
| Last wins          | Use headers from last record in group                | -                       |
| Merge              | Combine all headers (comma-separated for duplicates) | -                       |
| Explicit selection | User specifies which record's headers to use         | Flink (via columns)     |

**Recommendation for Velostream**:

1. **Default**: **Last-event-wins** - use headers from last record in group
2. **Annotation override**: `@propagate_headers = 'first' | 'last' | 'merge' | 'none'`
3. **SQL function**: `FIRST_HEADER('trace-id')` / `LAST_HEADER('trace-id')` for explicit control

**Rationale for Last-Event-Wins**:

- **Temporal causality**: The last event in an aggregation window is what triggers emission
- **Trace continuity**: Maintains an unbroken trace chain for debugging (last request → aggregation output)
- **Event-time alignment**: For event-time windows, the last event by event-time is semantically "most recent"
- **Simpler mental model**: "What triggered this aggregation?" → the last event

---

### 2.3 Window Functions (ROWS OVER)

**Operations**: `LAG`, `LEAD`, `ROW_NUMBER`, `RANK`, sliding window aggregates

**Key Insight**: Unlike GROUP BY aggregations, ROWS OVER produces **one output per input row**.

```sql
SELECT symbol,
       price,
       AVG(price) OVER (PARTITION BY symbol ROWS WINDOW BUFFER 10 ROWS) as moving_avg
FROM trades;
```

```
Input Record              Output Record
┌─────────────────┐       ┌─────────────────┐
│ trace-id: A     │       │ trace-id: A     │  ✓ PRESERVE
│ symbol: AAPL    │  ──▶  │ symbol: AAPL    │
│ price: 150      │       │ price: 150      │
│                 │       │ moving_avg: 148 │
└─────────────────┘       └─────────────────┘
```

**Recommendation**: **Preserve headers** - each output corresponds to exactly one input.

---

### 2.4 Tumbling/Sliding/Session Windows

**Operations**: `WINDOW TUMBLING`, `WINDOW HOPPING`, `WINDOW SESSION`

These are **aggregation windows** - multiple records per window bucket.

**Recommendation**: Same as GROUP BY aggregations - **last-event-wins** by default.

**Rationale**: The last event in a window triggers watermark advancement and emission. Using its headers provides:

- Trace continuity from the "triggering" event
- Consistent behavior with GROUP BY aggregations
- Meaningful correlation-ID propagation for debugging

---

### 2.5 Joins

**Operations**: `JOIN`, `LEFT JOIN`, `INNER JOIN`

**Challenge**: Two input records merge into one output.

```
Left Record           Right Record          Output Record
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│ trace-id: A  │  ╲   │ trace-id: B  │      │ trace-id: ?  │
│ order_id: 1  │   ╲  │ order_id: 1  │  ──▶ │ order_id: 1  │
│              │    ╲ │ customer: X  │      │ customer: X  │
└──────────────┘     ╲└──────────────┘      └──────────────┘
```

**Industry Practice**:

- Kafka Streams: Left side headers only
- Flink: User must explicitly SELECT headers from desired side

**Recommendation**:

1. **Default**: Left side headers (matches Kafka Streams)
2. **Annotation**: `@join_header_source = 'left' | 'right' | 'merge' | 'none'`
3. **SQL function**: `LEFT_HEADERS()`, `RIGHT_HEADERS()`, `MERGE_HEADERS()`

---

## 3. Distributed Tracing Considerations

### 3.1 W3C Trace Context Headers

The standard headers for distributed tracing:

| Header        | Purpose                  | Example                                                   |
|---------------|--------------------------|-----------------------------------------------------------|
| `traceparent` | Trace ID, Span ID, flags | `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01` |
| `tracestate`  | Vendor-specific state    | `congo=t61rcWkgMzE,rojo=00f067aa0ba902b7`                 |

### 3.2 Current Velostream Support

We already have W3C Trace Context support:

```rust
// src/velostream/observability/trace_propagation.rs
pub fn extract_trace_context(headers: &HashMap<String, String>) -> Option<SpanContext>
pub fn inject_trace_context(span_context: &SpanContext, headers: &mut HashMap<String, String>)
```

And we inject trace context in processors:

```rust
// src/velostream/server/processors/observability_helper.rs
ObservabilityHelper::inject_trace_context_into_records(...)
```

### 3.3 The Problem (RESOLVED)

**Previous flow** (headers lost in aggregations):

```
Kafka Consumer → StreamRecord (headers captured)
                      ↓
               SQL Execution (StreamRecord::new() - headers LOST for aggregations)
                      ↓
               Kafka Producer (no headers to inject)
```

**Current flow** (FR-090 implemented):

```
Kafka Consumer → StreamRecord (headers captured)
                      ↓
               SQL Execution (headers PROPAGATED based on operation type)
                  • 1:1 ops: Preserve all headers
                  • Aggregations: Last-event-wins via with_headers_from()
                  • Joins: Left-side headers
                      ↓
               Trace Injection (traceparent/tracestate OVERWRITTEN by observability_helper)
                      ↓
               Kafka Producer (headers preserved + _event_time injected by writer)
```

### 3.4 Tracing Through Aggregations

For aggregations, we need a **new span** that links to parent spans:

```
                    ┌───────────────────────┐
                    │ Aggregation Span      │
    ┌──────────┐    │ trace-id: NEW         │    ┌──────────┐
    │ Span A   │───▶│ links: [A, B, C]      │───▶│ Output   │
    │ Span B   │───▶│                       │    │ Span     │
    │ Span C   │───▶└───────────────────────┘    └──────────┘
```

This preserves traceability while acknowledging the N:1 nature.

Sources:

- [Confluent: Distributed Tracing for Kafka](https://www.confluent.io/blog/importance-of-distributed-tracing-for-apache-kafka-based-applications/)
- [OpenTelemetry Context Propagation](https://opentelemetry.io/docs/concepts/context-propagation/)
- [New Relic: Distributed Tracing with Kafka](https://newrelic.com/blog/how-to-relic/distributed-tracing-with-kafka)

---

## 4. Proposed Design for Velostream

### 4.1 Header Propagation Modes

```rust
// Implemented in src/velostream/sql/execution/types.rs
pub enum HeaderPropagationMode {
    /// Preserve all headers from source (default for 1:1 operations)
    Preserve,
    /// Drop all headers
    Drop,
    /// Use headers from last record in group (default for aggregations)
    Last,
    /// Use headers from first record in group
    First,
    /// Merge headers (right-side values overwrite left for duplicate keys)
    Merge,
    // Custom(String) — deferred to Phase 2
}
```

### 4.2 Default Behavior by Operation Type

| Operation              | Default Mode | Rationale                                |
|------------------------|--------------|------------------------------------------|
| `SELECT` (passthrough) | `Preserve`   | 1:1 mapping                              |
| `WHERE` (filter)       | `Preserve`   | 1:1 mapping                              |
| `SELECT` (projection)  | `Preserve`   | 1:1 mapping                              |
| `ROWS OVER` window     | `Preserve`   | 1:1 output per input                     |
| `GROUP BY`             | `Last`       | Last event triggers emission             |
| `TUMBLING WINDOW`      | `Last`       | Last event triggers watermark/emission   |
| `HOP WINDOW`           | `Last`       | Last event triggers watermark/emission   |
| `SESSION WINDOW`       | `Last`       | Last event triggers gap timeout/emission |
| `JOIN`                 | `Left`       | Industry standard                        |
| `LEFT JOIN`            | `Left`       | Left side always present                 |

### 4.3 SQL Annotations for Override

```sql
-- Annotation approach
-- @propagate_headers = 'first'
-- @trace_headers = 'traceparent,tracestate,correlation-id'
CREATE
STREAM high_value_trades AS
SELECT symbol, SUM(value) as total_value
FROM trades
WHERE value > 10000
GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '1' MINUTE);
```

### 4.4 WITH Clause Configuration

```sql
CREATE
STREAM aggregated_trades AS
SELECT symbol, SUM(value) as total_value
FROM trades
GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '1' MINUTE)
WITH (
    'header.propagation' = 'first', 'header.trace.create_span' = 'true', 'header.trace.link_parents' = 'true'
    );
```

### 4.5 Protected Headers

Certain headers should have special treatment:

| Header Pattern                       | Treatment                               |
|--------------------------------------|-----------------------------------------|
| `traceparent`                        | Always create new span, link to parents |
| `tracestate`                         | Merge vendor states                     |
| `correlation-id`, `x-correlation-id` | Propagate (first or merge)              |
| `x-request-id`                       | Propagate (first or merge)              |
| `baggage`                            | Merge (W3C Baggage spec)                |

### 4.6 Implementation Changes

#### 4.6.1 StreamRecord Enhancement (IMPLEMENTED)

```rust
// Implemented in src/velostream/sql/execution/types.rs
impl StreamRecord {
    /// Create a new record preserving headers from a source record (FR-090).
    /// Strips `_event_time` header to prevent stale input values from
    /// blocking correct event_time injection by KafkaDataWriter.
    pub fn with_headers_from(
        fields: HashMap<String, FieldValue>,
        source: &StreamRecord,
    ) -> Self {
        let mut headers = source.headers.clone();
        headers.remove("_event_time"); // Strip stale input event_time
        Self {
            fields,
            timestamp: source.timestamp,
            offset: 0,
            partition: 0,
            headers,
            event_time: None, // Caller sets this (e.g., window_end_time)
            topic: None,
            key: None,
        }
    }
}
```

**Note**: The originally proposed `from_aggregation()` with mode parameter is deferred.
The current implementation uses `with_headers_from()` with the `GroupAccumulator.sample_record`
(last-event-wins semantics) which covers the default aggregation behavior.

#### 4.6.2 Tracing-Aware Header Handling

```rust
impl StreamRecord {
    /// Create headers for aggregation output with proper trace linking
    pub fn create_aggregation_trace_headers(
        sources: &[&StreamRecord],
        telemetry: &TelemetryManager,
    ) -> HashMap<String, String> {
        let mut headers = HashMap::new();

        // Collect parent trace contexts
        let parent_contexts: Vec<SpanContext> = sources
            .iter()
            .filter_map(|s| extract_trace_context(&s.headers))
            .collect();

        // Create new span with links to all parents
        if let Some(new_context) = telemetry.create_linked_span(
            "aggregation",
            &parent_contexts,
        ) {
            inject_trace_context(&new_context, &mut headers);
        }

        // Propagate correlation IDs (merge if different)
        Self::propagate_correlation_headers(sources, &mut headers);

        headers
    }
}
```

---

## 5. Implementation Phases

### Phase 1: Foundation (Immediate) -- COMPLETED

- [x] Add `HeaderPropagationMode` enum (`types.rs`) -- Preserve, Drop, Last, First, Merge
- [x] Add `with_headers_from()` constructor to StreamRecord (strips `_event_time` header)
- [x] SELECT/WHERE/projection already preserve headers (`select.rs:924-933`)
- [x] Header mutations work (SET_HEADER/REMOVE_HEADER via `engine.rs:629-652`)
- [ ] Add header propagation config to StreamingConfig (deferred to Phase 2)

### Phase 2: ROWS OVER Windows -- COMPLETED (already compliant)

- [x] ROWS OVER operations preserve headers (1:1 output per input, headers passed through)
- [x] LAG/LEAD/ROW_NUMBER preserve headers from the current row

### Phase 3: Aggregations -- COMPLETED

- [x] `GroupAccumulator.set_sample_record()` changed to **last-event-wins** (`internal.rs`)
- [x] `flush_final_aggregations` uses `with_headers_from()` (`engine.rs:1456`)
- [x] Windowed aggregation (`build_result_record`) uses `with_headers_from()` (`adapter.rs:1084`)
- [x] `_event_time` header stripped from aggregation output to prevent stale values
- [ ] `@propagate_headers` annotation parsing (deferred)
- [ ] Trace linking for aggregation spans (deferred)

### Phase 4: Joins -- COMPLETED

- [x] Default to left-side headers in `combine_records()` (`join.rs:425`)
- [x] `combine_records_with_nulls()` preserves base record headers (`join.rs:453-461`)
- [x] `merge_headers()` retained for future `@join_header_source = 'merge'` support
- [ ] `@join_header_source` annotation (deferred)
- [ ] LEFT_HEADERS()/RIGHT_HEADERS()/MERGE_HEADERS() SQL functions (deferred)

### Phase 5: Documentation & Testing -- COMPLETED

- [x] Unit tests for `HeaderPropagationMode`, `with_headers_from()`, last-event-wins, join headers
- [x] Tests in `tests/unit/sql/types/header_propagation_test.rs`
- [x] FR-090 README updated with current status
- [ ] Integration tests for trace propagation through aggregations (deferred)
- [ ] Performance benchmarks for header handling overhead (deferred)

### Deferred Items (Future PR)

- `@propagate_headers` annotation parsing
- `@join_header_source` annotation
- `FIRST_HEADER()` / `LAST_HEADER()` SQL functions
- `LEFT_HEADERS()` / `RIGHT_HEADERS()` / `MERGE_HEADERS()` SQL functions
- Trace linking for aggregation spans (new span with links to all input spans)
- `Custom(String)` variant in `HeaderPropagationMode`
- WITH clause configuration (`'header.propagation' = 'first'`)

---

## 6. Summary

| Question                                | Answer                                       |
|-----------------------------------------|----------------------------------------------|
| Should 1:1 operations preserve headers? | **Yes** - always                             |
| Should aggregations preserve headers?   | **Last-event-wins** by default, configurable |
| Should ROWS OVER preserve headers?      | **Yes** - 1:1 output                         |
| Should joins preserve headers?          | **Left side** by default                     |
| How to handle tracing?                  | Create new span, link to parents             |
| Configuration mechanism?                | Annotations + WITH clause                    |

### Why Last-Event-Wins Over Drop for Aggregations?

| Consideration               | Drop                          | Last-Event-Wins                        |
|-----------------------------|-------------------------------|----------------------------------------|
| **Trace debugging**         | ❌ Broken chain                | ✅ Unbroken chain from triggering event |
| **Temporal causality**      | ❌ No context                  | ✅ "What caused this output?"           |
| **Correlation-ID tracking** | ❌ Lost                        | ✅ Preserved from last request          |
| **Mental model**            | "Aggregations have no source" | "Last event triggered this"            |
| **Industry precedent**      | Kafka Streams                 | Velostream differentiation             |

---

## References

- [Apache Flink Window Aggregation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-agg/)
- [KIP-634: Headers in Kafka Streams DSL](https://cwiki.apache.org/confluence/display/KAFKA/KIP-634:+Complementary+support+for+headers+and+record+metadata+in+Kafka+Streams+DSL)
- [Redpanda: Kafka Headers Best Practices](https://www.redpanda.com/guides/kafka-cloud-kafka-headers)
- [OpenTelemetry Context Propagation](https://opentelemetry.io/docs/concepts/context-propagation/)
- [Confluent: Distributed Tracing for Kafka](https://www.confluent.io/blog/importance-of-distributed-tracing-for-apache-kafka-based-applications/)
- [Lenses.io: Kafka Headers with SQL](https://lenses.io/blog/2020/12/kafka-distributed-tracing-with-message-headers/)
