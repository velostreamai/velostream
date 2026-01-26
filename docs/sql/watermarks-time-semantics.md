# Watermarks & Time Semantics Guide

## Overview

Phase 1B introduces comprehensive time semantics to Velostream, enabling proper handling of out-of-order and late-arriving data through watermark-based processing. This guide explains how to configure and use event-time processing, watermarks, and late data strategies.

## Key Concepts

### Event-Time vs Processing-Time

**Processing-Time**: When the record was processed by the engine (always available in `timestamp` field)
**Event-Time**: When the event actually occurred (optional, extracted from event data into `event_time` field)

```rust
pub struct StreamRecord {
    pub data: HashMap<String, FieldValue>,
    pub timestamp: NaiveDateTime,    // Processing-time (always set)
    pub event_time: Option<NaiveDateTime>, // Event-time (optional)
    pub headers: Option<HashMap<String, String>>,
}
```

### Watermarks

Watermarks are timestamps that indicate "all events with event-time ≤ watermark have been seen". They drive window emission and late data detection.

## Configuration

### Basic Watermark Configuration

```rust
use velostream::velo::sql::execution::{
    watermarks::{WatermarkStrategy, WatermarkConfig},
    config::{LateDataStrategy, StreamingConfig}
};

let config = StreamingConfig {
    // Enable event-time semantics
    event_time_semantics: true,
    
    // Configure watermarks
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_secs(10) // Allow 10s out-of-order
        },
        idle_timeout: Some(Duration::from_secs(60)), // 60s idle timeout
    }),
    
    // Late data handling strategy
    late_data_strategy: LateDataStrategy::DeadLetter,
    
    // ... other config
};
```

### Watermark Strategies

#### 1. BoundedOutOfOrderness
For streams with known maximum out-of-orderness:

```rust
WatermarkStrategy::BoundedOutOfOrderness {
    max_out_of_orderness: Duration::from_secs(10)
}
```

**Use case**: Financial data feeds with known processing delays

#### 2. Ascending
For strictly ordered streams:

```rust
WatermarkStrategy::Ascending
```

**Use case**: Log files, sequential data sources

#### 3. Punctuated
Using explicit watermark events:

```rust
WatermarkStrategy::Punctuated {
    punctuation_field: "watermark_timestamp".to_string()
}
```

**Use case**: Custom watermark injection, IoT sensors with heartbeats

### Late Data Strategies

#### Drop
Silently discard late data:
```rust
LateDataStrategy::Drop
```

#### DeadLetter
Route to dead letter queue for analysis:
```rust
LateDataStrategy::DeadLetter
```

#### IncludeInNextWindow
Process in the next available window:
```rust
LateDataStrategy::IncludeInNextWindow
```

#### UpdatePrevious
Update previous window results (use with caution):
```rust
LateDataStrategy::UpdatePrevious
```

## SQL Integration

### Event-Time Extraction

Configure event-time extraction from data source fields:

```sql
CREATE STREAM trades AS
SELECT
    ticker,
    price,
    volume,
    event_time
FROM kafka_source
WITH (
    'event.time.field' = 'event_timestamp',
    'event.time.format' = 'iso8601'
);
```

### Event-Time Format Options

Velostream supports multiple timestamp formats for extracting event-time from string fields:

#### ISO 8601 Format (Recommended)

For standard timestamp strings with timezone information:

```sql
CREATE STREAM orders AS
SELECT order_id, amount, event_time
FROM kafka_source
WITH (
    'event.time.field' = 'timestamp',
    'event.time.format' = 'iso8601'
);
```

**Supported ISO 8601 examples:**
- `2024-01-15T10:30:00Z` (UTC with Z suffix)
- `2024-01-15T10:30:00+00:00` (UTC with offset)
- `2024-01-15T10:30:00-05:00` (Eastern Time with offset)
- `2024-01-15T10:30:00.123Z` (With milliseconds)

#### Epoch Timestamps

For numeric Unix timestamps:

```sql
-- Milliseconds since epoch (e.g., 1705318200000)
CREATE STREAM trades AS
SELECT symbol, price, event_time
FROM kafka_source
WITH (
    'event.time.field' = 'timestamp_ms',
    'event.time.format' = 'epoch_millis'
);

-- Seconds since epoch (e.g., 1705318200)
CREATE STREAM trades AS
SELECT symbol, price, event_time
FROM kafka_source
WITH (
    'event.time.field' = 'timestamp_sec',
    'event.time.format' = 'epoch_seconds'
);
```

**Note:** Epoch formats accept both integer and string field values.

#### Custom Formats

For application-specific timestamp formats using [chrono format specifiers](https://docs.rs/chrono/latest/chrono/format/strftime/index.html):

```sql
-- Custom datetime format with milliseconds
CREATE STREAM logs AS
SELECT level, message, event_time
FROM file_source
WITH (
    'event.time.field' = 'log_timestamp',
    'event.time.format' = '%Y-%m-%d %H:%M:%S%.3f'
);

-- Without milliseconds
CREATE STREAM events AS
SELECT event_type, event_time
FROM file_source
WITH (
    'event.time.field' = 'occurred_at',
    'event.time.format' = '%Y-%m-%d %H:%M:%S'
);

-- US date format with AM/PM and milliseconds
CREATE STREAM activities AS
SELECT activity_type, event_time
FROM file_source
WITH (
    'event.time.field' = 'timestamp',
    'event.time.format' = '%m/%d/%Y %I:%M:%S%.3f %p'
);
```

**Common chrono format specifiers:**
- `%Y` - Year (4 digits, e.g., 2024)
- `%m` - Month (01-12)
- `%d` - Day (01-31)
- `%H` - Hour 24-hour format (00-23)
- `%I` - Hour 12-hour format (01-12)
- `%M` - Minute (00-59)
- `%S` - Second (00-59)
- `%.3f` - Milliseconds (3 digits, e.g., .123)
- `%.6f` - Microseconds (6 digits, e.g., .123456)
- `%.9f` - Nanoseconds (9 digits, e.g., .123456789)
- `%p` - AM/PM
- `%Z` - Timezone name
- `%z` - Timezone offset (+0000)

#### Auto-Detection

When no format is specified, Velostream attempts automatic format detection:

```sql
CREATE STREAM data AS
SELECT id, value, event_time
FROM kafka_source
WITH (
    'event.time.field' = 'timestamp'
    -- No format specified, auto-detection enabled
);
```

**Auto-detection strategy:**
1. Try parsing as epoch milliseconds (integer)
2. If that fails, try ISO 8601 format
3. If both fail, log warning and set `event_time` to None

**When to use auto-detection:**
- ✅ Prototyping and development
- ✅ Data sources with consistent but unknown format
- ❌ Production (explicit format is more reliable)

#### Complete Format Reference

| Format Type | Configuration Value | Field Example | Notes |
|-------------|-------------------|---------------|-------|
| ISO 8601 | `iso8601` or `ISO8601` | `"2024-01-15T10:30:00Z"` | Recommended for string timestamps |
| Epoch milliseconds | `epoch_millis` | `1705318200000` or `"1705318200000"` | JavaScript `Date.now()` |
| Epoch seconds | `epoch_seconds` or `epoch` | `1705318200` or `"1705318200"` | Unix timestamp |
| Custom with millis | `%Y-%m-%d %H:%M:%S%.3f` | `"2024-01-15 10:30:00.123"` | Chrono format with milliseconds |
| Custom without millis | `%Y-%m-%d %H:%M:%S` | `"2024-01-15 10:30:00"` | Any chrono format |
| Auto-detect | Omit format property | Integer or ISO 8601 string | Development only |

### Event-Time Windows

Use event-time for windowing operations:

```sql
-- Tumbling window based on event-time
SELECT 
    ticker,
    COUNT(*) as trade_count,
    AVG(price) as avg_price
FROM trades
WHERE volume > 1000
GROUP BY 
    ticker,
    TUMBLE(event_time, INTERVAL '1' MINUTE)
EMIT CHANGES;
```

### Watermark-Aware Processing

Windows emit only when watermarks advance beyond window end:

```sql
-- 5-minute tumbling windows with 30-second grace period
SELECT 
    merchant_category,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM transactions
GROUP BY 
    merchant_category,
    TUMBLE(event_time, INTERVAL '5' MINUTE)
WITH (
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '30s',
    'late.data.strategy' = 'dead_letter'
)
EMIT CHANGES;
```

## Programming API

### Engine Configuration

```rust
use velostream::velo::sql::execution::{
    StreamExecutionEngine, ProcessorContext, 
    watermarks::WatermarkManager
};

// Create engine with watermark support
let (tx, rx) = mpsc::unbounded_channel();
let mut engine = StreamExecutionEngine::new(tx);

// Enable watermarks on processor context
let mut context = ProcessorContext::new();
context.enable_watermarks(watermark_config);

// Process records with watermark awareness
engine.execute_with_context(&query, record, &mut context).await?;
```

### Custom Watermark Generation

```rust
use velostream::velo::sql::execution::watermarks::{
    WatermarkManager, WatermarkStrategy
};

// Create custom watermark manager
let mut watermark_manager = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
    max_out_of_orderness: Duration::from_secs(5)
});

// Update watermarks with new event
watermark_manager.update_watermark("source1", event_time)?;

// Get current watermark
let current_watermark = watermark_manager.get_global_watermark();
```

### Late Data Handling

```rust
use velostream::velo::sql::execution::watermarks::LateDataAction;

// Check if data is late
if let Some(action) = watermark_manager.handle_late_data(&record, &strategy) {
    match action {
        LateDataAction::Drop => {
            // Record dropped
        },
        LateDataAction::DeadLetter => {
            // Route to dead letter queue
        },
        LateDataAction::ProcessInWindow(window_id) => {
            // Process in specified window
        }
    }
}
```

## Monitoring & Observability

### Watermark Metrics

```rust
// Track watermark lag
watermark_manager.record_watermark_lag(source_id, lag_duration);

// Monitor late data rates
watermark_manager.record_late_data_event(source_id, lateness);
```

### Key Metrics to Monitor

- **Watermark Lag**: Difference between current time and watermark
- **Late Data Rate**: Percentage of records arriving late
- **Window Emission Delay**: Time between window end and emission
- **Dead Letter Queue Size**: Volume of rejected late data

## Performance Considerations

### Memory Management

- **Window Buffer Size**: Configure based on expected out-of-orderness
- **Watermark Update Frequency**: Balance accuracy vs performance
- **Late Data Buffer**: Size based on late data strategy

### Optimization Tips

1. **Choose Appropriate Strategy**: Use `Ascending` for ordered data
2. **Tune Out-of-Orderness**: Minimize while maintaining data completeness
3. **Monitor Late Data**: Adjust watermark configuration based on patterns
4. **Idle Source Handling**: Configure timeouts for sparse data sources

## Examples

### Financial Trading System

```rust
// High-frequency trading with tight timing requirements
let config = StreamingConfig {
    event_time_semantics: true,
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_millis(100) // 100ms max delay
        },
        idle_timeout: Some(Duration::from_secs(5)),
    }),
    late_data_strategy: LateDataStrategy::Drop, // Drop late trades
    // ...
};
```

### IoT Sensor Network

```rust
// IoT sensors with variable network delays
let config = StreamingConfig {
    event_time_semantics: true,
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_secs(30) // 30s tolerance
        },
        idle_timeout: Some(Duration::from_minutes(5)),
    }),
    late_data_strategy: LateDataStrategy::IncludeInNextWindow,
    // ...
};
```

### Log Processing

```rust
// Sequential log file processing
let config = StreamingConfig {
    event_time_semantics: true,
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::Ascending, // Strictly ordered
        idle_timeout: None, // No idle timeout needed
    }),
    late_data_strategy: LateDataStrategy::Drop,
    // ...
};
```

## Troubleshooting

### Common Issues

1. **Windows Not Emitting**: Check watermark advancement
2. **High Late Data Rate**: Increase max_out_of_orderness
3. **Memory Growth**: Monitor window buffer sizes
4. **Performance Issues**: Optimize watermark update frequency

### Debug Tools

```rust
// Enable watermark debugging
let config = StreamingConfig {
    debug_watermarks: true,
    // ...
};

// Log watermark information
watermark_manager.log_watermark_status();
```

## Derived Event Time via _EVENT_TIME Aliasing

In addition to extracting event time at the source level via `event.time.field`, Velostream supports **deriving event time via SQL expressions** by aliasing a field or expression as `_EVENT_TIME`.

### Use Cases

1. **Time Simulation** - Testing with synthetic or replayed timestamps
2. **Derived Event Time** - Parsing timestamps from nested JSON, combining date/time fields
3. **Event Time from Computed Expressions** - Using UDFs or complex logic to determine event time
4. **Post-Join Event Time Selection** - Choosing which input's event time to use after a JOIN

### Basic Syntax

```sql
-- Derive event time from a payload field
SELECT
    trade_timestamp AS _EVENT_TIME,  -- Sets record.event_time
    symbol,
    price
FROM trades;

-- Derive from computed expression (nested JSON)
SELECT
    CAST(JSON_EXTRACT(payload, '$.event_ts') AS BIGINT) AS _EVENT_TIME,
    *
FROM raw_events;

-- Derive from combined fields
SELECT
    UNIX_TIMESTAMP(CONCAT(event_date, ' ', event_time)) AS _EVENT_TIME,
    *
FROM legacy_events;
```

### Supported Value Types

The `_EVENT_TIME` alias accepts:

| Type | Interpretation |
|------|---------------|
| `INTEGER` (BIGINT) | Milliseconds since Unix epoch |
| `TIMESTAMP` | Converted to DateTime<Utc> |
| Other types | Falls back to input record's `_EVENT_TIME` |

### Event Time Propagation Through Queries

When `_EVENT_TIME` is **not explicitly aliased**, Velostream preserves the input record's `_EVENT_TIME`:

```sql
-- _EVENT_TIME is preserved from input (no explicit _EVENT_TIME alias)
SELECT symbol, price FROM trades;  -- Output inherits input's _EVENT_TIME
```

### Event Time in JOINs

For JOIN operations, event time propagation follows these rules:

#### Inner/Outer JOIN
- **When both inputs have `_EVENT_TIME`**: Uses the **maximum** (newer) of the two
- **When only one has `_EVENT_TIME`**: Uses that one
- **When neither has `_EVENT_TIME`**: Output has no event time

```sql
-- Output _EVENT_TIME = MAX(orders._EVENT_TIME, shipments._EVENT_TIME)
SELECT o.*, s.*
FROM orders o
JOIN shipments s ON o.order_id = s.order_id;
```

#### Selecting Specific Event Time from JOIN

To explicitly choose which input's event time to use:

```sql
-- Use the order's event time as the output event time
SELECT
    o.order_timestamp AS _EVENT_TIME,  -- Explicitly set from orders
    o.order_id,
    s.shipped_at
FROM orders o
JOIN shipments s ON o.order_id = s.order_id;

-- Use the shipment's event time instead
SELECT
    s.shipped_at AS _EVENT_TIME,  -- Explicitly set from shipments
    o.order_id,
    o.order_timestamp
FROM orders o
JOIN shipments s ON o.order_id = s.order_id;
```

### Event Time in GROUP BY

Event time aliasing also works with aggregations:

```sql
-- Use the maximum trade timestamp as the group's event time
SELECT
    MAX(trade_timestamp) AS _EVENT_TIME,
    symbol,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume
FROM trades
GROUP BY symbol;
```

### Accessing _EVENT_TIME in Queries

The `_EVENT_TIME` system column is always available and falls back to `_TIMESTAMP` (processing time) if event time was not set:

```sql
-- _EVENT_TIME always returns a value (never NULL)
SELECT
    _EVENT_TIME,        -- Event time (or falls back to processing time)
    _TIMESTAMP,         -- Processing time (Kafka message timestamp)
    symbol,
    price
FROM trades;
```

### Case-Insensitive System Columns

System columns (`_EVENT_TIME`, `_TIMESTAMP`, `_OFFSET`, `_PARTITION`) are matched **case-insensitively**. All of the following are equivalent:

```sql
-- All of these reference the same system column
SELECT _EVENT_TIME FROM trades;      -- Uppercase (canonical)
SELECT _event_time FROM trades;      -- Lowercase
SELECT _Event_Time FROM trades;      -- Mixed case

-- Same applies in window definitions
WINDOW SLIDING(_event_time, 5m, 1m)  -- Lowercase works
WINDOW SLIDING(_EVENT_TIME, 5m, 1m)  -- Uppercase works

-- And in ORDER BY clauses
ORDER BY _event_time                  -- Lowercase works
ORDER BY _EVENT_TIME                  -- Uppercase works
```

This case-insensitivity applies to:
- SELECT expressions
- WHERE clause predicates
- ORDER BY clauses (including within ROWS WINDOW)
- WINDOW time column specifications
- AS alias assignments (for derived event time)

**Note**: While system columns are case-insensitive, regular field names from your data are case-sensitive and must match exactly.

### Comparison with Flink/ksqlDB

| Feature | Velostream | Flink SQL | ksqlDB |
|---------|------------|-----------|--------|
| Source-level event time | `event.time.field` | `WATERMARK FOR` | `TIMESTAMP` property |
| Derived event time | `AS _EVENT_TIME` alias | Computed columns | CSAS with TIMESTAMP |
| Event time in JOIN output | MAX of inputs + override via alias | Implicit | Not configurable |
| Event time fallback | Falls back to `_TIMESTAMP` | Requires explicit config | ROWTIME always available |

## Related Features

- [Observability Guide](../ops/OBSERVABILITY.md) - Monitoring watermark performance
- [Resource Management](../ops/RESOURCE_MANAGEMENT.md) - Memory management for windows
- [Circuit Breakers](./CIRCUIT_BREAKERS.md) - Fault tolerance with watermarks

## References

- [Apache Flink Watermarks](https://flink.apache.org/news/2015/12/04/Introducing-windows.html)
- [Streaming Systems Book](https://www.oreilly.com/library/view/streaming-systems/9781491983874/)
- [Dataflow Model Paper](https://research.google.com/pubs/pub43864.html)