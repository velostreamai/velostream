# System Columns in Velostream

System columns provide access to Kafka metadata and Velostream's internal metadata directly in your SQL queries. These are special columns that are automatically available for every stream without needing to be explicitly defined in your table schema.

## What Are System Columns?

System columns expose metadata about each message in your stream, such as:
- **When** the message was received (`_timestamp`, `_event_time`)
- **Where** it came from (`_partition`, `_offset`)
- **Window boundaries** for aggregations (`_window_start`, `_window_end`)

This enables powerful analytics that combine business data with metadata context.

## Available System Columns

### Processing-Time Metadata

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `_timestamp` | `INT64` | Message processing time in milliseconds since Unix epoch | `1697296245000` |
| `_offset` | `INT64` | Kafka partition offset (message sequence number) | `12345` |
| `_partition` | `INT32` | Kafka partition number | `0`, `1`, `2` |

### Event-Time Metadata

| Column | Type | Description |
|--------|------|-------------|
| `_event_time` | `INT64` | Event time in milliseconds since Unix epoch. Propagated via Kafka message timestamp (header), not as a JSON payload field. See [Event Time Guide](../user-guides/event-time-guide.md). |

### Window Metadata

| Column | Type | Description |
|--------|------|-------------|
| `_window_start` | `INT64` | Window start time in milliseconds (tumbling/sliding windows) |
| `_window_end` | `INT64` | Window end time in milliseconds (tumbling/sliding windows) |

## Basic Usage

### Select System Columns Directly

```sql
-- Get metadata for each message
SELECT
    _timestamp,
    _partition,
    _offset,
    customer_id,
    amount
FROM orders;
```

### Convert to Human-Readable Format

System columns store timestamps as milliseconds since epoch. Convert them to readable dates:

```sql
-- Convert processing timestamp to ISO format
SELECT
    FROM_UNIXTIME(_timestamp / 1000) as received_at,
    _partition,
    _offset,
    customer_id,
    amount
FROM orders;
```

### Filter by Time Range

```sql
-- Get messages from the last hour
SELECT customer_id, amount
FROM orders
WHERE _timestamp > (UNIX_TIMESTAMP() - 3600) * 1000;
```

## Real-World Use Cases

### 1. Data Quality Monitoring

Track message arrival patterns and latency:

```sql
-- Monitor message flow across partitions
SELECT
    _partition,
    COUNT(*) as message_count,
    MIN(_timestamp) as earliest,
    MAX(_timestamp) as latest,
    (MAX(_timestamp) - MIN(_timestamp)) / 1000 as duration_seconds
FROM orders
GROUP BY _partition
EMIT CHANGES;
```

### 2. Multi-Partition Analytics

Analyze data distribution across Kafka partitions:

```sql
-- Find skewed partitions
SELECT
    _partition,
    COUNT(*) as msg_count,
    SUM(amount) as total_volume,
    AVG(amount) as avg_order_value
FROM orders
GROUP BY _partition
HAVING COUNT(*) > 1000
ORDER BY msg_count DESC;
```

### 3. Message Replay Detection

Identify late-arriving or replayed messages:

```sql
-- Detect messages arriving out of order
SELECT
    _partition,
    _offset,
    _timestamp,
    customer_id,
    order_date
FROM orders
WHERE _timestamp < UNIX_TIMESTAMP(order_date) * 1000
LIMIT 100;
```

### 4. Offset-Based Recovery

Use offsets to track processing progress:

```sql
-- Find the highest processed offset per partition
SELECT
    _partition,
    MAX(_offset) as latest_offset,
    COUNT(*) as total_messages
FROM orders
GROUP BY _partition;
```

## Advanced Patterns

### Combine with Aggregations

```sql
-- Get hourly stats with partition information
SELECT
    _partition,
    TUMBLE_START(FROM_UNIXTIME(_timestamp / 1000), INTERVAL '1' HOUR) as hour,
    COUNT(*) as messages,
    SUM(amount) as revenue
FROM orders
WINDOW TUMBLING(1h)
GROUP BY _partition
EMIT CHANGES;
```

### Detect Late Data

```sql
-- Identify events arriving more than 1 minute late
SELECT
    customer_id,
    order_date,
    FROM_UNIXTIME(_timestamp / 1000) as received_at,
    (_timestamp - UNIX_TIMESTAMP(order_date) * 1000) / 1000 as latency_seconds
FROM orders
WHERE _timestamp - UNIX_TIMESTAMP(order_date) * 1000 > 60000
EMIT CHANGES;
```

### Offset-Based Windowing

```sql
-- Process every 1000 messages per partition
SELECT
    _partition,
    _offset,
    COUNT(*) OVER (
        PARTITION BY _partition
        ORDER BY _offset
        ROWS BETWEEN CURRENT ROW AND 999 FOLLOWING
    ) as batch_position,
    customer_id,
    amount
FROM orders;
```

## System Column Considerations

### 1. **Case Sensitivity**

System columns are **case-insensitive**:
```sql
-- All of these are equivalent
SELECT _timestamp FROM orders;
SELECT _TIMESTAMP FROM orders;
SELECT _Timestamp FROM orders;
```

### 2. **Not in WHERE Before GROUP BY**

When using GROUP BY, system columns follow SQL standard semantics:
```sql
-- This works (system column in GROUP BY)
SELECT _partition, COUNT(*) FROM orders GROUP BY _partition;

-- This requires aggregation (system column in SELECT)
SELECT _partition, _offset FROM orders GROUP BY _partition;
-- Error: _offset is not in GROUP BY
```

### 3. **Wildcard Selection**

Wildcard (`*`) includes only regular columns, not system columns:
```sql
-- This selects only: customer_id, amount
SELECT * FROM orders;

-- To include system columns, add them explicitly
SELECT _timestamp, _partition, * FROM orders;
```

### 4. **Performance Notes**

- System column access is **O(1)** - instant lookups
- No additional memory overhead
- Efficient for high-throughput streams (millions of messages/sec)

## Combining System Columns with Headers

System columns work seamlessly with Kafka header functions:

```sql
-- Correlate headers with message metadata
SELECT
    _partition,
    _offset,
    HEADER('trace-id') as trace_id,
    HEADER('span-id') as span_id,
    customer_id,
    amount
FROM orders
WHERE HAS_HEADER('trace-id')
EMIT CHANGES;
```

## Migration Guide

### From Kafka Consumer Offsets

If you're tracking offsets externally, use system columns instead:

```sql
-- Instead of external offset tracking, query the stream:
SELECT MAX(_offset) as last_processed_offset
FROM orders
WHERE _partition = 0
GROUP BY _partition;
```

### From Application-Level Timestamps

Store event timestamps in Kafka instead of relying on application logs:

```sql
-- Use _event_time for proper event-time semantics
SELECT
    _event_time,
    _timestamp,
    (CAST(_timestamp - UNIX_TIMESTAMP(_event_time) * 1000 AS BIGINT)) / 1000 as processing_latency_seconds
FROM orders;
```

## Comparison with Other Streaming SQL Engines

| Feature | Velostream | Flink SQL | ksqlDB |
|---------|-----------|-----------|--------|
| Partition metadata | ✅ `_partition` | ❌ DataStream API only | ❌ Not available |
| Offset in SQL | ✅ `_offset` | ❌ DataStream API only | ❌ Not available |
| Processing timestamp | ✅ `_timestamp` | ✅ `$proctime` | ❌ Not standard |
| Event-time | ✅ `_event_time` | ✅ `$rowtime` | ✅ `ROWTIME` |
| Window boundaries | ✅ `_window_start/end` | Implicit | Implicit |
| Combined in WHERE/GROUP BY | ✅ Yes | Depends | Limited |

## Examples: Complete Queries

### Example 1: Real-Time Partition Health Check

```sql
-- Monitor partition lag and throughput
SELECT
    _partition,
    COUNT(*) as messages_in_window,
    MAX(_offset) as latest_offset,
    FROM_UNIXTIME(MAX(_timestamp) / 1000) as latest_message_time,
    (MAX(_timestamp) - MIN(_timestamp)) / 1000 as window_duration_seconds,
    ROUND(COUNT(*) * 1000.0 / (MAX(_timestamp) - MIN(_timestamp)), 2) as msgs_per_second
FROM orders
WINDOW TUMBLING(30s)
GROUP BY _partition
EMIT CHANGES;
```

### Example 2: Outlier Detection by Partition

```sql
-- Find orders with unusual characteristics per partition
SELECT
    _partition,
    _offset,
    customer_id,
    amount,
    AVG(amount) OVER (PARTITION BY _partition ORDER BY _offset ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) as rolling_avg
FROM orders
WHERE amount > (SELECT AVG(amount) * 2 FROM orders)
EMIT CHANGES;
```

### Example 3: Message Offset Recovery

```sql
-- Resume processing from a specific offset per partition
SELECT *
FROM orders
WHERE _partition = 1 AND _offset >= 12345
EMIT CHANGES;
```

## See Also

- [Header Access Guide](header-access.md) - Working with Kafka headers
- [Distributed Tracing with Headers and System Columns](../examples/distributed-tracing.md)
- [Advanced Query Features](advanced-query-features.md)
