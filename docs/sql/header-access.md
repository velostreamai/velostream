# Kafka Header Access in Velostream

Kafka headers provide a way to attach metadata to messages without modifying the message value. Velostream makes it easy to read and write headers directly in SQL using built-in functions.

## Why Use Headers?

Headers are ideal for:
- **Distributed Tracing**: Store trace IDs and span IDs
- **Message Correlation**: Track related messages
- **Context Propagation**: Pass metadata through processing pipelines
- **Message Enrichment**: Add fields without changing schema
- **Compliance**: Mark sensitive data or audit trails

## Reading Headers

### HEADER() - Get Header Value

Returns the value of a header or NULL if not present.

```sql
SELECT
    HEADER('trace-id') as trace_id,
    HEADER('span-id') as span_id,
    customer_id,
    amount
FROM orders
EMIT CHANGES;
```

**Characteristics**:
- Returns `STRING` or `NULL`
- Case-sensitive header keys (just like in Kafka)
- Returns `NULL` if header doesn't exist (safe)

### HAS_HEADER() - Check Header Existence

Returns true/false if a header exists.

```sql
-- Process only messages with trace context
SELECT
    customer_id,
    amount,
    HEADER('trace-id') as trace_id
FROM orders
WHERE HAS_HEADER('trace-id')
EMIT CHANGES;
```

**Characteristics**:
- Returns `BOOLEAN`
- Efficient way to filter on header presence
- Useful for conditional processing

### HEADER_KEYS() - List All Headers

Returns a comma-separated string of all header keys.

```sql
SELECT
    HEADER_KEYS() as all_headers,
    customer_id,
    amount
FROM orders
EMIT CHANGES;
```

**Characteristics**:
- Returns `STRING`
- Format: `"key1,key2,key3"`
- Useful for debugging or logging

## Writing Headers

### SET_HEADER() - Add/Update Header

Adds or updates a header in the output message. Supports dynamic expressions.

#### Basic Literal Usage

```sql
SELECT
    SET_HEADER('processed-timestamp', '2024-10-27T14:30:45Z') as processed_at,
    SET_HEADER('processor-id', 'velostream-1') as processor,
    customer_id,
    amount
FROM orders
EMIT CHANGES;
```

#### Field Reference Support (NEW!)

```sql
-- Use field values as headers
SELECT
    SET_HEADER('customer-id', customer_id) as customer_header,      -- Field reference
    SET_HEADER('order-id', order_id) as order_header,
    SET_HEADER('region', region_code) as region_header,
    amount
FROM orders
EMIT CHANGES;
```

#### System Column Support (NEW!)

```sql
-- Correlate with partition and offset
SELECT
    SET_HEADER('kafka-partition', _partition) as partition_header,
    SET_HEADER('kafka-offset', _offset) as offset_header,
    SET_HEADER('processing-timestamp', _timestamp) as timestamp_header,
    customer_id,
    amount
FROM orders
EMIT CHANGES;
```

#### Type Conversion (NEW!)

```sql
-- Automatic type conversion to strings
SELECT
    SET_HEADER('amount', amount) as amount_header,                     -- Numeric → string
    SET_HEADER('is-premium', is_premium) as premium_header,           -- Boolean → string
    SET_HEADER('event-time', _event_time) as event_header,           -- Timestamp → string
    customer_id
FROM orders
EMIT CHANGES;
```

#### Complex Expressions (NEW!)

```sql
-- Even CAST expressions work
SELECT
    SET_HEADER('numeric-partition', CAST(_partition AS STRING)) as part,
    SET_HEADER('formatted-timestamp', CAST(_timestamp AS STRING)) as ts,
    customer_id,
    amount
FROM orders
EMIT CHANGES;
```

**Characteristics**:
- Supports both literal strings and field references
- Supports all FieldValue types with automatic conversion
- Returns the header value for use in SELECT
- ScaledInteger fields preserve decimal precision
- **NEW**: Dynamic expression evaluation (commit 2762fa8)

### REMOVE_HEADER() - Delete Header

Removes a header from the output message.

```sql
SELECT
    REMOVE_HEADER('internal-metadata') as removed_value,
    customer_id,
    amount
FROM orders
EMIT CHANGES;
```

**Characteristics**:
- Returns the removed header's value (or NULL if didn't exist)
- Header is removed from output message
- Useful for filtering sensitive metadata

## Automatic Header Propagation (FR-090)

Velostream automatically propagates Kafka message headers through SQL operations. The propagation behavior depends on the operation type:

| Operation | Propagation | Behavior |
|-----------|-------------|----------|
| SELECT, WHERE, projection | **Preserve** | All input headers flow to output unchanged |
| ROWS OVER window functions | **Preserve** | 1:1 output per input — headers pass through |
| GROUP BY aggregation | **Last-event-wins** | Output carries headers from the last input record in the group |
| TUMBLING / HOP / SESSION WINDOW | **Last-event-wins** | Output carries headers from the last input record in the window |
| JOIN | **Left-side** | Output carries headers from the left (stream) side only |

### What This Means in Practice

For **passthrough** and **filter** queries, headers propagate automatically — no `SET_HEADER()` needed:

```sql
-- Input headers automatically appear in output
SELECT customer_id, amount
FROM orders
WHERE amount > 100;
-- Output message will have ALL original headers (trace-id, correlation-id, etc.)
```

For **aggregations**, the last record processed in each group determines the output headers:

```sql
SELECT symbol, SUM(price) as total, COUNT(*) as cnt
FROM trades
GROUP BY symbol;
-- Output headers come from the LAST trade record in each symbol group
```

For **joins**, the left (stream) side headers are used:

```sql
SELECT t.*, r.name
FROM trades t
LEFT JOIN reference r ON t.symbol = r.symbol;
-- Output headers come from the trades (left) side only
```

### Special Header: `_event_time`

The `_event_time` header is **automatically stripped** from aggregation output to prevent stale values. The Kafka writer injects the correct output event time (e.g., `window_end_time`) independently.

### Distributed Tracing Headers

`traceparent` and `tracestate` headers propagate through SQL but are **overwritten** after SQL execution by the trace context injector (`inject_trace_context_into_records()`). This means:
- Input trace context is preserved through SQL processing
- Output records get a fresh span from the Velostream processor
- Manual `SET_HEADER('traceparent', ...)` is not needed for standard tracing

### Explicit Header Mutations

Use `SET_HEADER()` and `REMOVE_HEADER()` when you need to **change** headers, not just propagate them. These mutations are applied on top of the automatically propagated headers.

For the full design rationale, see [FR-090 Header Propagation Analysis](../feature/FR-090-header-prop/README.md).

## Real-World Use Cases

### 1. Distributed Tracing Integration

```sql
-- Propagate trace context and add span information
SELECT
    -- Pass through existing trace context
    SET_HEADER('trace-id', HEADER('trace-id')) as trace_id,
    SET_HEADER('parent-span-id', HEADER('span-id')) as parent_span,

    -- Add new span information
    SET_HEADER('span-id', 'velostream-span-' || _partition || '-' || _offset) as new_span,
    SET_HEADER('span-operation', 'enrich-order') as operation,
    SET_HEADER('processing-timestamp', _timestamp) as processed_at,

    -- Include data
    customer_id,
    amount
FROM orders
WHERE HAS_HEADER('trace-id')
EMIT CHANGES;
```

### 2. Message Enrichment Pipeline

```sql
-- Enrich messages with computed fields and context
SELECT
    SET_HEADER('enrichment-timestamp', _timestamp) as enriched_at,
    SET_HEADER('enrichment-partition', _partition) as part,
    SET_HEADER('customer-segment', CASE
        WHEN amount > 1000 THEN 'high-value'
        WHEN amount > 100 THEN 'medium-value'
        ELSE 'low-value'
    END) as segment_header,
    SET_HEADER('region-code', region) as region_header,

    -- Add processed flag
    SET_HEADER('processed', 'true') as processed,

    customer_id,
    amount,
    region
FROM orders
EMIT CHANGES;
```

### 3. Sensitive Data Filtering

```sql
-- Remove PII headers before downstream processing
SELECT
    REMOVE_HEADER('credit-card-last4') as removed_cc,
    REMOVE_HEADER('ssn') as removed_ssn,
    REMOVE_HEADER('email') as removed_email,

    customer_id,
    amount
FROM orders
WHERE HAS_HEADER('credit-card-last4')
EMIT CHANGES;
```

### 4. Audit Trail

```sql
-- Add comprehensive audit information
SELECT
    SET_HEADER('audit-timestamp', _timestamp) as audit_ts,
    SET_HEADER('audit-partition', _partition) as audit_part,
    SET_HEADER('audit-offset', _offset) as audit_offset,
    SET_HEADER('audit-user', HEADER('user-id')) as audit_user,
    SET_HEADER('audit-operation', 'order-processing') as operation,
    SET_HEADER('audit-status', 'completed') as status,

    customer_id,
    amount
FROM orders
EMIT CHANGES;
```

### 5. Conditional Header Routing

```sql
-- Set different headers based on order characteristics
SELECT
    SET_HEADER('priority', CASE
        WHEN amount > 10000 THEN 'critical'
        WHEN amount > 1000 THEN 'high'
        ELSE 'normal'
    END) as priority_header,

    SET_HEADER('requires-review', CASE
        WHEN is_international = true THEN 'true'
        ELSE 'false'
    END) as review_header,

    SET_HEADER('fraud-score', fraud_score) as fraud_header,

    customer_id,
    amount,
    is_international
FROM orders
EMIT CHANGES;
```

## Header Functions Reference

### Summary

| Function | Purpose | Returns | Input |
|----------|---------|---------|-------|
| `HEADER(key)` | Read header value | STRING or NULL | Header key name |
| `HAS_HEADER(key)` | Check header exists | BOOLEAN | Header key name |
| `HEADER_KEYS()` | List all headers | STRING (comma-separated) | None |
| `SET_HEADER(key, value)` | Add/update header | The value | Key + any field/literal |
| `REMOVE_HEADER(key)` | Delete header | Old value or NULL | Header key name |

### Data Type Support for SET_HEADER

| Data Type | Conversion | Example |
|-----------|-----------|---------|
| STRING | Direct | `SET_HEADER('key', 'value')` |
| INTEGER | To string | `SET_HEADER('partition', 123)` → `"123"` |
| FLOAT | To string | `SET_HEADER('price', 99.99)` → `"99.99"` |
| BOOLEAN | To string | `SET_HEADER('flag', true)` → `"true"` |
| TIMESTAMP | To ISO string | `SET_HEADER('time', _event_time)` → `"2024-10-27T14:30:00Z"` |
| SCALEDINTEGER | Decimal string | `SET_HEADER('amount', price_field)` → `"123.45"` |
| NULL | String "null" | `SET_HEADER('nullable', null_field)` → `"null"` |

## Combining Headers with System Columns

The most powerful pattern is combining headers with system columns:

```sql
-- Complete context propagation
SELECT
    -- Distributed tracing headers
    SET_HEADER('trace-id', HEADER('trace-id')) as trace,
    SET_HEADER('span-id', 'span-' || _partition || '-' || _offset) as span,

    -- Message metadata
    SET_HEADER('kafka-partition', _partition) as partition,
    SET_HEADER('kafka-offset', _offset) as offset,
    SET_HEADER('processing-timestamp', _timestamp) as ts,

    -- Business context
    SET_HEADER('customer-id', customer_id) as customer,
    SET_HEADER('order-value', amount) as value,

    customer_id,
    amount
FROM orders
WHERE HAS_HEADER('trace-id')
EMIT CHANGES;
```

## Best Practices

### 1. **Use Consistent Header Names**

```sql
-- Good: Standardized naming
SET_HEADER('x-trace-id', trace_id)          -- OpenTelemetry convention
SET_HEADER('x-span-id', span_id)
SET_HEADER('x-request-id', request_id)

-- Avoid: Inconsistent naming
SET_HEADER('traceID', trace_id)             -- Mixing cases
SET_HEADER('trace_id', trace_id)            -- Mixing conventions
```

### 2. **Handle Missing Headers Gracefully**

```sql
-- Good: Use COALESCE for defaults
SELECT
    SET_HEADER('trace-id', COALESCE(HEADER('trace-id'), 'no-trace')) as trace
FROM orders;

-- Better: Check existence first
SELECT
    CASE WHEN HAS_HEADER('trace-id')
        THEN SET_HEADER('trace-id', HEADER('trace-id'))
        ELSE SET_HEADER('trace-id', 'generated-' || _partition || '-' || _offset)
    END as trace
FROM orders;
```

### 3. **Avoid Large Headers**

```sql
-- Good: Small, structured headers
SET_HEADER('trace-id', trace_id)            -- 36 bytes (UUID)
SET_HEADER('user-id', user_id)              -- ~10-50 bytes

-- Avoid: Large data in headers
SET_HEADER('full-customer-record', entire_json)  -- Inefficient
```

### 4. **Document Header Schema**

```sql
-- Always document your header contract
-- Expected headers from upstream:
--   x-trace-id: distributed tracing ID
--   x-user-id: authenticated user ID
--   x-request-id: request correlation ID
--
-- Headers added by this query:
--   x-partition: Kafka partition number
--   x-processed-at: processing timestamp

SELECT
    SET_HEADER('x-partition', _partition) as part,
    SET_HEADER('x-processed-at', _timestamp) as ts,
    customer_id,
    amount
FROM orders;
```

## Performance Considerations

- **Header access**: O(1) - constant time lookup
- **Header modification**: O(n) where n is number of headers (typically < 20)
- **No serialization overhead**: Headers processed independently from message value
- **Suitable for high-throughput**: Millions of messages/second

## Troubleshooting

### Headers Not Appearing in Output

```sql
-- Issue: Using SET_HEADER without EMIT
-- Fix: Make sure your query has EMIT CHANGES or outputs to Kafka
SELECT SET_HEADER('key', 'value') as h, customer_id FROM orders EMIT CHANGES;
```

### Type Conversion Issues

```sql
-- Headers are always strings
-- If you need specific types downstream, parse in consumer

-- In SQL (for debugging):
SELECT
    CAST(HEADER('partition') AS INTEGER) as partition_num,
    HEADER('amount') as amount_str
FROM orders;
```

## See Also

- [System Columns Guide](system-columns.md) - Combining headers with system columns
- [Distributed Tracing Example](examples/distributed-tracing.md)
- [Advanced Query Features](advanced-query-features.md)
- [FR-090 Header Propagation Analysis](../feature/FR-090-header-prop/README.md) - Full design rationale
- [Event Time Guide](../user-guides/event-time-guide.md) - `_event_time` header behavior
