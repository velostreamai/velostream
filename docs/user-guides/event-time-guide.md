# Event Time Guide

Velostream separates **event time** (when something happened) from **processing time** (when a record was produced to Kafka). This separation is fundamental to correct stream processing — it enables time-travel, watermarking, windowed aggregations, and accurate latency measurement.

## Two Time Paths

Every record in Velostream has two independent timestamps:

| Time Path | System Column | Storage | Semantics |
|-----------|--------------|---------|-----------|
| **Event time** | `_EVENT_TIME` | Kafka message timestamp (header) | When the event occurred (simulation time, source system time) |
| **Processing time** | `_TIMESTAMP` | `record.timestamp` field | When the record was written to Kafka (wall-clock) |

### Why This Matters

```
Source event at 14:00:00 ──→ Arrives at Kafka at 14:00:05 ──→ Processed at 14:00:07
                   │                        │                           │
            _EVENT_TIME              _TIMESTAMP (producer)      _TIMESTAMP (consumer)
```

- **Windowed aggregations** should use `_EVENT_TIME` — a 1-minute tumbling window over event time groups events by when they happened, not when they arrived.
- **Latency monitoring** compares `_EVENT_TIME` to `NOW()` — the difference is end-to-end latency.
- **Time simulation** replays historical data with correct event-time ordering without touching the processing timestamp.

## How `_EVENT_TIME` Flows

`_EVENT_TIME` is **not** a JSON payload field. It is Kafka message metadata that flows through the system:

```
┌──────────────┐    Kafka message     ┌──────────────┐    record.event_time    ┌──────────────┐
│   Producer   │ ──── timestamp ────→ │    Broker    │ ──── from_kafka() ────→ │  Velostream  │
│  (or test    │    (header, not      │              │    extracts into        │  SQL Engine  │
│   harness)   │     payload)         │              │    StreamRecord         │              │
└──────────────┘                      └──────────────┘                         └──────┬───────┘
                                                                                      │
                                                                          ┌───────────┴───────────┐
                                                                          │                       │
                                                                    _EVENT_TIME            Kafka Writer
                                                                    system column          sets message
                                                                    in SQL queries         timestamp from
                                                                                          record.event_time
                                                                                                │
                                                                                          ┌─────┴─────┐
                                                                                          │ Downstream │
                                                                                          │ Consumer   │
                                                                                          └───────────┘
```

Key points:
- `_EVENT_TIME` is **stripped from output JSON fields** — it lives in Kafka message metadata only
- The Kafka writer **propagates** `record.event_time` as the output message timestamp
- Downstream consumers receive event time via the Kafka message timestamp, not a payload field

## Configuration

### Source-Level Event Time Extraction

When consuming from an external Kafka topic where event time is stored in a payload field, use `event.time.field`:

```sql
CREATE STREAM processed_events AS
SELECT symbol, price, volume
FROM raw_events
WITH (
    'raw_events.type' = 'kafka_source',
    'raw_events.topic.name' = 'raw_events',
    'event.time.field' = 'event_timestamp',
    'event.time.format' = 'epoch_millis'
);
```

Supported formats for `event.time.format`:
- `epoch_millis` — milliseconds since Unix epoch (default)
- `epoch_seconds` — seconds since Unix epoch
- `iso8601` — ISO 8601 string
- Custom format string (e.g., `%Y-%m-%d %H:%M:%S`)

### Kafka Message Timestamp (No Config Needed)

When the upstream producer already sets the correct Kafka message timestamp (e.g., Velostream output topics, or well-configured producers), **no configuration is needed**. The Kafka message timestamp is used automatically as `_EVENT_TIME`:

```sql
-- No event.time.field needed — uses Kafka message timestamp
CREATE STREAM enriched AS
SELECT m.symbol, m.price, r.instrument_name
FROM market_data_ts m
LEFT JOIN instrument_reference r ON m.symbol = r.symbol
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '5s'
);
```

### Watermark Configuration

Watermarks are enabled when either `event.time.field` or `watermark.strategy` is set:

```sql
WITH (
    -- Watermark strategy (required for windowed queries)
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '5s',

    -- Late data handling
    'late.data.strategy' = 'dead_letter'   -- or 'drop', 'update_previous'
)
```

Available watermark strategies:
- `bounded_out_of_orderness` — allows events up to N seconds late
- `ascending` — assumes strictly ordered event times
- `punctuated` — uses special marker records

## Using `_EVENT_TIME` in SQL

### Direct Access

```sql
-- Access event time alongside business data
SELECT
    _EVENT_TIME,
    _TIMESTAMP,
    symbol,
    price,
    (NOW() - _EVENT_TIME) / 1000.0 AS latency_seconds
FROM market_data_ts;
```

### Aliasing (Derived Event Time)

Set the output record's event time by aliasing a field or expression as `_EVENT_TIME`:

```sql
-- Use a payload field as the output event time
SELECT
    trade_timestamp AS _EVENT_TIME,
    symbol,
    price
FROM trades;

-- Computed event time
SELECT
    MAX(trade_timestamp) AS _EVENT_TIME,
    symbol,
    COUNT(*) AS trade_count
FROM trades
GROUP BY symbol;
```

### In Window Definitions

```sql
-- Tumbling window on event time
SELECT
    symbol,
    AVG(price) AS avg_price,
    COUNT(*) AS trade_count
FROM market_data_ts
WINDOW TUMBLING(_event_time, INTERVAL '1' SECOND)
GROUP BY symbol
EMIT CHANGES;

-- Sliding window on event time
SELECT
    symbol,
    AVG(volume) AS avg_volume
FROM market_data_ts
WINDOW SLIDING(_event_time, 5m, 1m)
GROUP BY symbol
EMIT CHANGES;
```

### In JOINs

For JOIN operations, event time propagation follows these rules:
- **Both inputs have `_EVENT_TIME`**: Uses the maximum (newer) of the two
- **Only one has `_EVENT_TIME`**: Uses that one
- **Override**: Alias a specific field as `_EVENT_TIME` to choose explicitly

```sql
-- Event time = MAX(positions._event_time, market._event_time)
SELECT
    p.trader_id, p.symbol, p.position_size,
    m.price AS current_price
FROM trading_positions_ts p
LEFT JOIN market_data_ts m ON p.symbol = m.symbol;

-- Explicitly choose position event time
SELECT
    p._event_time AS _EVENT_TIME,
    p.trader_id, p.symbol,
    m.price AS current_price
FROM trading_positions_ts p
LEFT JOIN market_data_ts m ON p.symbol = m.symbol;
```

### Interval JOINs

Use `_event_time` in temporal join conditions:

```sql
SELECT p.*, m.price
FROM trading_positions_ts p
LEFT JOIN market_data_ts m
    ON p.symbol = m.symbol
    AND m._event_time BETWEEN p._event_time - INTERVAL '30' SECOND
                          AND p._event_time + INTERVAL '30' SECOND;
```

## Event-Time Fallback Behavior

When `_EVENT_TIME` is accessed but the record has no event time set (e.g., the source didn't provide one), the behavior is controlled by the `VELOSTREAM_EVENT_TIME_FALLBACK` environment variable:

| Mode | Value | Behavior |
|------|-------|----------|
| **Processing Time** | `processing_time` (default) | Silently falls back to `_TIMESTAMP` |
| **Warn** | `warn` | Falls back to `_TIMESTAMP` and logs a warning per record |
| **Null** | `null` | Returns `NULL` — use `COALESCE` to handle explicitly |

```bash
# Default: silent fallback to processing time
export VELOSTREAM_EVENT_TIME_FALLBACK=processing_time

# Diagnostic: log warnings when event_time is missing
export VELOSTREAM_EVENT_TIME_FALLBACK=warn

# Strict: return NULL so queries can handle it explicitly
export VELOSTREAM_EVENT_TIME_FALLBACK=null
```

### Null Mode with COALESCE

```sql
-- When VELOSTREAM_EVENT_TIME_FALLBACK=null
SELECT
    COALESCE(_EVENT_TIME, _TIMESTAMP) AS effective_time,
    COALESCE((NOW() - _EVENT_TIME) / 1000.0, 0.0) AS latency_seconds,
    symbol,
    price
FROM market_data_ts;
```

## Case Insensitivity

System columns are matched case-insensitively:

```sql
-- All equivalent
SELECT _EVENT_TIME FROM trades;
SELECT _event_time FROM trades;
SELECT _Event_Time FROM trades;

-- In window definitions
WINDOW TUMBLING(_event_time, INTERVAL '1' SECOND)
WINDOW TUMBLING(_EVENT_TIME, INTERVAL '1' SECOND)
```

## Trading Demo Example

The trading demo (`demo/trading/`) demonstrates the two-path architecture:

**Input schemas** define `timestamp` as wall-clock production time (no `timestamp_epoch_ms` constraint — the generator produces `Utc::now()`). Event time is set separately in the Kafka message header by the test harness time simulation.

**SQL apps** use `_event_time` for windowed aggregations and latency calculations:

```sql
-- From app_market_data.sql: 1-second OHLCV candles on event time
CREATE STREAM tick_buckets AS
SELECT
    symbol PRIMARY KEY,
    TUMBLE_START(_event_time, INTERVAL '1' SECOND) as bucket_start,
    TUMBLE_END(_event_time, INTERVAL '1' SECOND) as bucket_end,
    AVG(price) as avg_price,
    COUNT(*) as trade_count
FROM market_data_ts
WINDOW TUMBLING(_event_time, INTERVAL '1' SECOND)
GROUP BY symbol
EMIT CHANGES;

-- From app_market_data.sql: enrichment latency using event time
CREATE STREAM enriched_market_data AS
SELECT
    m.symbol PRIMARY KEY,
    m.price,
    r.instrument_name,
    COALESCE((NOW() - m._event_time) / 1000.0, 0.0) as enrichment_latency_seconds
FROM market_data_ts m
LEFT JOIN instrument_reference r ON m.symbol = r.symbol;
```

**Watermarks** are configured per query — no `event.time.field` needed because upstream topics already have correct Kafka message timestamps:

```sql
WITH (
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '5s',
    'late.data.strategy' = 'dead_letter'
)
```

## See Also

- [System Columns Reference](../sql/system-columns.md) — All system columns
- [Watermarks and Time Semantics](../sql/watermarks-time-semantics.md) — Watermark strategies, late data handling
- [SQL Annotations](sql-annotations.md) — `@metric` annotations for observability
- [SQL Copy-Paste Examples](../sql/COPY_PASTE_EXAMPLES.md) — Working SQL examples
