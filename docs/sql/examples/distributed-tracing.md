# Distributed Tracing with System Columns and Headers

This example demonstrates how Velostream uniquely enables distributed tracing directly in SQL by combining system columns (`_partition`, `_offset`, `_timestamp`) with Kafka headers (trace-id, span-id, etc.).

## The Problem: Distributed Tracing Across Services

When processing streaming data across multiple services, you need to:
1. **Correlate messages** across services using trace IDs
2. **Track processing** across Kafka partitions
3. **Record timing** at each stage (processing-time)
4. **Propagate context** for downstream services

Traditional streaming SQL engines don't provide native support for this - you need external tracing systems or custom code.

## Velostream's Unique Solution

Velostream allows you to do this **directly in SQL** by combining:
- **System columns**: Track which partition and offset processed each message
- **Headers**: Propagate distributed tracing context
- **Timestamps**: Record exact processing times

## Automatic Header Propagation

As of FR-090, Velostream **automatically propagates** Kafka message headers through SQL operations:

- **Passthrough queries** (SELECT, WHERE): All headers preserved — no `SET_HEADER()` needed
- **Aggregations** (GROUP BY, WINDOW): Headers from the last record in each group propagate automatically (last-event-wins)
- **Joins**: Left-side headers propagate by default

### W3C Trace Context (`traceparent` / `tracestate`)

Velostream's observability layer automatically injects W3C Trace Context headers **after** SQL execution:

1. SQL processing propagates input headers (including any existing `traceparent`)
2. After SQL, `inject_trace_context_into_records()` **overwrites** `traceparent` and `tracestate` with a fresh span
3. The output record carries the new span, linked to the processor's trace

This means you do **not** need to manually propagate `traceparent` via `SET_HEADER()` for standard tracing. Manual `SET_HEADER()` is only needed when you want to add **custom** tracing headers (like `x-trace-id` in the examples below) that are outside the W3C standard.

## Complete Example: E-Commerce Order Processing

### Scenario

Orders flow through a Kafka topic. Each order includes:
- Order data (customer_id, amount, items)
- Distributed tracing headers from the upstream service
- Processing metadata (partition, offset, timestamp)

We'll implement a processing pipeline that:
1. Validates the order
2. Enriches with tracing context
3. Propagates trace information downstream
4. Maintains complete audit trail

### Query 1: Order Validation with Tracing

```sql
-- Step 1: Validate orders and add tracing context
CREATE TABLE validated_orders AS
SELECT
    -- Propagate upstream trace context
    SET_HEADER('x-trace-id', HEADER('x-trace-id')) as trace_id,
    SET_HEADER('x-parent-span-id', HEADER('x-span-id')) as parent_span,

    -- Add new span for this stage
    SET_HEADER('x-span-id', 'span-validate-' || _partition || '-' || _offset) as new_span,
    SET_HEADER('x-span-operation', 'validate-order') as operation,

    -- Add message metadata
    SET_HEADER('x-kafka-partition', _partition) as kafka_part,
    SET_HEADER('x-kafka-offset', _offset) as kafka_offset,
    SET_HEADER('x-processed-timestamp', _timestamp) as validated_at,

    -- Add processing status
    SET_HEADER('x-validation-status', CASE
        WHEN amount > 0 AND customer_id IS NOT NULL THEN 'valid'
        ELSE 'invalid'
    END) as validation_status,

    -- Business data
    customer_id,
    amount,
    order_id,
    items

FROM orders
WHERE HAS_HEADER('x-trace-id')
EMIT CHANGES;
```

### Query 2: Enrichment Stage

```sql
-- Step 2: Enrich orders with customer data and update trace
CREATE TABLE enriched_orders AS
SELECT
    -- Pass through trace context
    SET_HEADER('x-trace-id', HEADER('x-trace-id')) as trace_id,
    SET_HEADER('x-parent-span-id', HEADER('x-span-id')) as parent_span,

    -- Add enrichment span
    SET_HEADER('x-span-id', 'span-enrich-' || _partition || '-' || _offset) as enrichment_span,
    SET_HEADER('x-span-operation', 'enrich-order') as operation,

    -- Enrich with customer segment
    SET_HEADER('x-customer-segment', CASE
        WHEN amount > 10000 THEN 'vip'
        WHEN amount > 1000 THEN 'premium'
        ELSE 'standard'
    END) as segment,

    -- Add processing metadata
    SET_HEADER('x-enrichment-timestamp', _timestamp) as enriched_at,
    SET_HEADER('x-enrichment-partition', _partition) as enrich_part,

    -- Calculate processing latency
    SET_HEADER('x-validation-latency-ms',
        CAST((_timestamp - UNIX_TIMESTAMP(HEADER('x-processed-timestamp')) * 1000) AS STRING)) as validation_latency,

    customer_id,
    amount,
    order_id,
    items

FROM validated_orders
WHERE HAS_HEADER('x-validation-status') AND HEADER('x-validation-status') = 'valid'
EMIT CHANGES;
```

### Query 3: Fraud Detection

```sql
-- Step 3: Fraud check and audit logging
CREATE TABLE fraud_checked_orders AS
SELECT
    -- Complete trace propagation
    SET_HEADER('x-trace-id', HEADER('x-trace-id')) as trace_id,
    SET_HEADER('x-parent-span-id', HEADER('x-span-id')) as parent_span,
    SET_HEADER('x-span-id', 'span-fraud-check-' || _partition || '-' || _offset) as fraud_span,
    SET_HEADER('x-span-operation', 'fraud-detection') as operation,

    -- Fraud detection result
    SET_HEADER('x-fraud-risk', CASE
        WHEN amount > 50000 THEN 'high'
        WHEN amount > 5000 THEN 'medium'
        ELSE 'low'
    END) as fraud_risk,

    -- Audit trail
    SET_HEADER('x-audit-timestamp', _timestamp) as audit_ts,
    SET_HEADER('x-audit-partition', _partition) as audit_part,
    SET_HEADER('x-audit-offset', _offset) as audit_offset,

    -- Request routing
    SET_HEADER('x-route', CASE
        WHEN amount > 50000 THEN 'manual-review'
        WHEN amount > 5000 THEN 'fast-track'
        ELSE 'auto-approve'
    END) as routing,

    -- Complete context
    SET_HEADER('x-customer-segment', HEADER('x-customer-segment')) as segment,
    SET_HEADER('x-processing-stage', 'fraud-checked') as stage,

    customer_id,
    amount,
    order_id,
    items

FROM enriched_orders
EMIT CHANGES;
```

## Advanced Pattern: Complete Observability

### Query 4: Observability Log

This query creates a structured observability log that captures complete context:

```sql
-- Create observability/audit log with complete context
CREATE TABLE order_processing_log AS
SELECT
    -- Tracing identifiers
    HEADER('x-trace-id') as trace_id,
    HEADER('x-span-id') as span_id,
    HEADER('x-customer-segment') as customer_segment,

    -- Message identifiers
    _partition as kafka_partition,
    _offset as kafka_offset,
    FROM_UNIXTIME(_timestamp / 1000) as processed_at,

    -- Order data
    customer_id,
    amount,
    order_id,

    -- Processing status
    HEADER('x-validation-status') as validation_status,
    HEADER('x-fraud-risk') as fraud_risk,
    HEADER('x-route') as routing_decision,

    -- Context headers
    HEADER_KEYS() as all_headers,

    -- Processing latency calculations
    CAST((_timestamp - UNIX_TIMESTAMP(HEADER('x-processed-timestamp')) * 1000) AS BIGINT) / 1000 as total_processing_latency_ms

FROM fraud_checked_orders
WHERE HAS_HEADER('x-trace-id')
EMIT CHANGES;
```

## Real-World Query: Multi-Stage Analysis

### Detect Orders with Anomalies

```sql
-- Identify orders that took longer than expected to process
SELECT
    HEADER('x-trace-id') as trace_id,
    customer_id,
    amount,
    HEADER('x-customer-segment') as segment,
    _partition,
    _offset,
    FROM_UNIXTIME(_timestamp / 1000) as completed_at,

    -- Calculate total pipeline latency
    (_timestamp - UNIX_TIMESTAMP(HEADER('x-processed-timestamp')) * 1000) / 1000 as total_latency_seconds,

    -- Identify slow processing
    CASE
        WHEN (_timestamp - UNIX_TIMESTAMP(HEADER('x-processed-timestamp')) * 1000) / 1000 > 5 THEN 'slow'
        WHEN (_timestamp - UNIX_TIMESTAMP(HEADER('x-processed-timestamp')) * 1000) / 1000 > 2 THEN 'medium'
        ELSE 'fast'
    END as processing_speed

FROM fraud_checked_orders
WHERE HAS_HEADER('x-trace-id')
  AND (_timestamp - UNIX_TIMESTAMP(HEADER('x-processed-timestamp')) * 1000) / 1000 > 2
EMIT CHANGES;
```

## Extracting Observability Metrics

### Partition Health Dashboard

```sql
-- Real-time partition health metrics
SELECT
    _partition as partition_id,
    TUMBLE_START(FROM_UNIXTIME(_timestamp / 1000), INTERVAL '1' MINUTE) as minute,
    COUNT(*) as message_count,
    COUNT(DISTINCT HEADER('x-trace-id')) as unique_traces,
    SUM(CASE WHEN HEADER('x-fraud-risk') = 'high' THEN 1 ELSE 0 END) as high_risk_count,
    AVG(amount) as avg_amount,
    MIN(_offset) as first_offset,
    MAX(_offset) as last_offset

FROM fraud_checked_orders
WINDOW TUMBLING(1m)
GROUP BY _partition
EMIT CHANGES;
```

### Trace Path Analysis

```sql
-- Track unique trace paths through the system
SELECT
    HEADER('x-trace-id') as trace_id,
    COUNT(DISTINCT _partition) as partitions_involved,
    MIN(_offset) as first_message_offset,
    MAX(_offset) as last_message_offset,
    COUNT(*) as total_messages,
    MIN(FROM_UNIXTIME(_timestamp / 1000)) as started_at,
    MAX(FROM_UNIXTIME(_timestamp / 1000)) as completed_at,
    SUM(amount) as total_amount

FROM fraud_checked_orders
WHERE HAS_HEADER('x-trace-id')
GROUP BY HEADER('x-trace-id')
EMIT CHANGES;
```

## Why This Is Unique to Velostream

| Capability | Velostream | Flink SQL | ksqlDB |
|-----------|-----------|-----------|--------|
| Access partition/offset in SQL | ✅ Yes | ❌ DataStream API only | ❌ Not available |
| Access Kafka headers in SQL | ✅ Yes | ❌ Not in SQL | ❌ Not available |
| Modify headers in SQL | ✅ Yes (NEW!) | ❌ Not available | ❌ Not available |
| Complete audit trail in SQL | ✅ Yes | ❌ Requires external code | ❌ Limited |
| Distributed tracing in pure SQL | ✅ Yes (ONLY) | ❌ Impossible | ❌ Impossible |

## Benefits of This Approach

### 1. **Complete Lineage Tracking**
- Every message can be traced through partitions
- Offset numbers provide absolute message ordering
- Partition information identifies scaling bottlenecks

### 2. **No External Dependencies**
- All tracing logic in SQL queries
- No separate tracing infrastructure needed
- Works in cloud, on-prem, hybrid

### 3. **Automatic Context Propagation**
- Headers automatically flow through Kafka
- System columns always available
- Downstream services get full context

### 4. **Queryable Audit Trail**
- All processing captured in Kafka topics
- Historical queries for debugging
- Compliance/regulatory requirements

### 5. **Performance**
- Zero overhead for header access
- System columns are O(1) lookups
- Millions of messages/second

## Troubleshooting

### Trace IDs Disappearing

```sql
-- Debug: Check if headers are present
SELECT
    _partition,
    _offset,
    HEADER_KEYS() as headers,
    HEADER('x-trace-id') as trace_id
FROM orders
LIMIT 10;
```

### Latency Calculations Wrong

```sql
-- Verify timestamp values
SELECT
    _timestamp as processing_timestamp,
    UNIX_TIMESTAMP(HEADER('x-processed-timestamp')) * 1000 as validation_timestamp,
    (_timestamp - UNIX_TIMESTAMP(HEADER('x-processed-timestamp')) * 1000) / 1000 as latency_seconds
FROM fraud_checked_orders
LIMIT 5;
```

### Headers Not Propagating

Headers now propagate **automatically** through SQL operations (FR-090). If headers are missing in output:

1. **Check input headers exist**: Use `HEADER_KEYS()` to inspect input
2. **Aggregation output**: Headers come from the last record in the group (last-event-wins). If the last record had no headers, output won't either.
3. **Joins**: Only left-side headers propagate by default
4. **`_event_time` header**: Automatically stripped from aggregation output (the Kafka writer injects the correct one)
5. **`traceparent`/`tracestate`**: Overwritten by trace injection after SQL — this is expected behavior

```sql
-- Debug: Check if headers are present on input
SELECT HEADER_KEYS() as all_keys, HEADER('x-trace-id') as trace_id
FROM orders
LIMIT 10;
```

## See Also

- [System Columns Guide](../system-columns.md)
- [Header Access Guide](../header-access.md) — Includes automatic header propagation reference
- [Advanced Query Features](../advanced-query-features.md)
- [FR-090 Header Propagation Analysis](../../feature/FR-090-header-prop/README.md) — Full design rationale
- [Event Time Guide](../../user-guides/event-time-guide.md) — `_event_time` header behavior in aggregations
