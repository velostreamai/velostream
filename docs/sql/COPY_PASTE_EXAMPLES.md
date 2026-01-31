# Copy-Paste SQL Examples (Tested & Correct)

**These examples are tested and work correctly. Copy them exactly.**

Use these as templates for your queries. When in doubt, copy the pattern from here.

> **Important**: Velostream supports two DDL types:
> - **CREATE STREAM AS SELECT (CSAS)** - For continuous streaming transformations
> - **CREATE TABLE AS SELECT (CTAS)** - For materialized aggregations/tables
>
> Plain SELECT statements are only valid for ad-hoc queries, not for deployed streaming jobs.

---

## Quick Reference Table

### Aggregate Functions

| Function | Syntax | Input Types | Description |
|----------|--------|-------------|-------------|
| COUNT | `COUNT(*)` or `COUNT(column)` | Any | Count rows or non-NULL values |
| SUM | `SUM(column)` | Numeric only | Sum of numeric values |
| SUM(DISTINCT) | `SUM(DISTINCT column)` | Numeric only | Sum of unique numeric values |
| AVG | `AVG(column)` | Numeric only | Average of numeric values |
| MIN | `MIN(column)` | Numeric, String, Timestamp | Minimum value (lexicographic for strings) |
| MAX | `MAX(column)` | Numeric, String, Timestamp | Maximum value (lexicographic for strings) |
| COUNT(DISTINCT) | `COUNT(DISTINCT column)` | Any | Count unique values |
| STDDEV_POP | `STDDEV_POP(column)` | Numeric only | Population standard deviation |
| STDDEV_SAMP | `STDDEV_SAMP(column)` | Numeric only | Sample standard deviation |
| VAR_POP | `VAR_POP(column)` | Numeric only | Population variance |
| VAR_SAMP | `VAR_SAMP(column)` | Numeric only | Sample variance |
| MEDIAN | `MEDIAN(column)` | Numeric only | Median value |
| APPROX_COUNT_DISTINCT | `APPROX_COUNT_DISTINCT(column)` | Any | Approximate unique count (HyperLogLog, ~2% error) |
| STRING_AGG | `STRING_AGG(column, ',')` | Any | Concatenate values with separator (aliases: GROUP_CONCAT, LISTAGG, COLLECT) |
| FIRST | `FIRST(column)` | Any | First non-NULL value (alias: FIRST_VALUE) |
| LAST | `LAST(column)` | Any | Last non-NULL value (alias: LAST_VALUE) |

> **Type Safety**: Functions marked "Numeric only" accept INTEGER, FLOAT, and SCALED_INTEGER (decimal).
> Passing a STRING or other non-numeric type to these functions returns an error.
> MIN/MAX work on any ordered type: numeric values compare by magnitude, strings compare lexicographically, timestamps compare chronologically.
> Mixed incompatible types (e.g., `MAX` over a column with both STRING and INTEGER values) returns an error.

### Window Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| LAG | `LAG(col, n) OVER (...)` | Value from n rows before |
| LEAD | `LEAD(col, n) OVER (...)` | Value from n rows after |
| ROW_NUMBER | `ROW_NUMBER() OVER (...)` | Sequential row number |
| RANK | `RANK() OVER (...)` | Rank with gaps |
| DENSE_RANK | `DENSE_RANK() OVER (...)` | Rank without gaps |
| FIRST_VALUE | `FIRST_VALUE(col) OVER (...)` | First value in window |
| LAST_VALUE | `LAST_VALUE(col) OVER (...)` | Last value in window |
| NTILE | `NTILE(n) OVER (...)` | Divide into n buckets |

### String Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| UPPER | `UPPER(string)` | Convert to uppercase |
| LOWER | `LOWER(string)` | Convert to lowercase |
| CONCAT | `CONCAT(s1, s2, ...)` | Concatenate strings |
| SUBSTRING | `SUBSTRING(str, start, len)` | Extract substring |
| TRIM | `TRIM(string)` | Remove leading/trailing spaces |
| REPLACE | `REPLACE(str, old, new)` | Replace occurrences |
| LENGTH | `LENGTH(string)` | String length |
| LEFT | `LEFT(string, n)` | First n characters |
| RIGHT | `RIGHT(string, n)` | Last n characters |

### Math Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| ABS | `ABS(number)` | Absolute value |
| ROUND | `ROUND(num, decimals)` | Round to n decimal places |
| FLOOR | `FLOOR(number)` | Round down to integer |
| CEIL | `CEIL(number)` | Round up to integer |
| SQRT | `SQRT(number)` | Square root |
| POWER | `POWER(base, exp)` | Raise to power |
| MOD | `MOD(a, b)` | Modulo (remainder) |
| GREATEST | `GREATEST(a, b, c)` | Maximum of values |
| LEAST | `LEAST(a, b, c)` | Minimum of values |

### Date/Time Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| EXTRACT | `EXTRACT(part FROM ts)` | Extract date/time part |
| NOW | `NOW()` | Current timestamp |
| DATEDIFF | `DATEDIFF(unit, start, end)` | Difference between dates |
| FROM_UNIXTIME | `FROM_UNIXTIME(epoch)` | Unix timestamp to datetime |
| UNIX_TIMESTAMP | `UNIX_TIMESTAMP(ts)` | Datetime to Unix timestamp |

### Type Conversion & Null Handling

| Function | Syntax | Description |
|----------|--------|-------------|
| CAST | `CAST(value AS type)` | Convert to specified type |
| COALESCE | `COALESCE(a, b, c)` | First non-NULL value |
| NULLIF | `NULLIF(a, b)` | NULL if a equals b |

### Conditional Logic

| Function | Syntax | Description |
|----------|--------|-------------|
| CASE | `CASE WHEN cond THEN val ELSE val END` | Conditional expression |
| IN | `col IN (a, b, c)` | Check membership |
| BETWEEN | `col BETWEEN a AND b` | Range check |

### JSON Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| JSON_EXTRACT | `JSON_EXTRACT(json, '$.path')` | Extract JSON value |
| JSON_VALUE | `JSON_VALUE(json, '$.path')` | Extract JSON scalar |

### Kafka Header Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| HEADER | `HEADER('key')` | Get header value |
| HAS_HEADER | `HAS_HEADER('key')` | Check if header exists |
| HEADER_KEYS | `HEADER_KEYS()` | List all header keys |

### System Columns

| Column | Description |
|--------|-------------|
| `_timestamp` | Kafka message timestamp |
| `_offset` | Kafka message offset |
| `_partition` | Kafka partition number |
| `_window_start` | Window start timestamp |
| `_window_end` | Window end timestamp |

### Kafka Message Key Annotation (FR-089)

| Syntax | Description |
|--------|-------------|
| `column PRIMARY KEY` | Mark column as Kafka message key |
| `column AS alias PRIMARY KEY` | Mark aliased column as key |
| `col1 PRIMARY KEY, col2 PRIMARY KEY` | Compound key (multiple columns) |

---

## CSAS vs CTAS - When to Use Which

| Use Case | DDL Type | Example |
|----------|----------|---------|
| Filter/transform stream | CSAS | Passthrough with WHERE clause |
| Enrich stream with lookup | CSAS | JOIN with reference table |
| Window functions (LAG, LEAD) | CSAS | Row-by-row analytics |
| Time-windowed aggregations | CTAS | 5-minute trade summaries |
| Materialized views | CTAS | Latest state per key |
| GROUP BY with EMIT FINAL | CTAS | Batch-style aggregations |

---

## Simple Streaming Queries (CSAS)

### Basic Filter (Passthrough with WHERE)

```sql
CREATE STREAM filtered_orders AS
SELECT order_id, customer_id, amount
FROM orders
WHERE amount > 100
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'filtered_orders.type' = 'kafka_sink',
    'filtered_orders.topic' = 'filtered_orders_output',
    'filtered_orders.format' = 'json'
);
```

**When to use**: Filtering data, selecting specific columns

### Multiple Filter Conditions

```sql
CREATE STREAM high_value_trades AS
SELECT symbol, price, quantity, event_time
FROM trades
WHERE price > 50
  AND quantity >= 100
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'high_value_trades.type' = 'kafka_sink',
    'high_value_trades.topic' = 'high_value_trades_output',
    'high_value_trades.format' = 'json'
);
```

**When to use**: Multiple filter conditions with AND/OR

### Column Aliases

```sql
CREATE STREAM renamed_orders AS
SELECT order_id AS id,
       customer_id AS cust_id,
       amount AS total
FROM orders
WHERE amount > 100
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'renamed_orders.type' = 'kafka_sink',
    'renamed_orders.topic' = 'renamed_orders_output',
    'renamed_orders.format' = 'json'
);
```

**When to use**: Renaming columns in output

---

## Aggregation with Time Windows (CTAS)

### TUMBLING Window - 1 Minute

```sql
CREATE TABLE trade_stats AS
SELECT symbol,
       COUNT(*) as trade_count,
       AVG(price) as avg_price
FROM market_data
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',
    'trade_stats.type' = 'kafka_sink',
    'trade_stats.topic' = 'trade_stats_output',
    'trade_stats.format' = 'json'
);
```

**When to use**: Fixed-size time windows (1m, 5m, 1h, etc)

### TUMBLING Window - 5 Minutes with Multiple Aggregates

```sql
CREATE TABLE symbol_aggregates AS
SELECT symbol,
       COUNT(*) as trade_count,
       AVG(price) as avg_price,
       MIN(price) as min_price,
       MAX(price) as max_price,
       SUM(quantity) as total_qty
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'symbol_aggregates.type' = 'kafka_sink',
    'symbol_aggregates.topic' = 'symbol_aggregates_output',
    'symbol_aggregates.format' = 'json'
);
```

**When to use**: Multiple calculations per group in time window

### GROUP BY with HAVING

```sql
CREATE TABLE active_customers AS
SELECT customer_id,
       COUNT(*) as order_count,
       SUM(amount) as total_spent
FROM orders
GROUP BY customer_id
WINDOW TUMBLING(INTERVAL '1' HOUR)
HAVING COUNT(*) > 5
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'active_customers.type' = 'kafka_sink',
    'active_customers.topic' = 'active_customers_output',
    'active_customers.format' = 'json'
);
```

**When to use**: Filter groups after aggregation

### GROUP BY with WHERE and HAVING

```sql
CREATE TABLE active_symbols AS
SELECT symbol,
       COUNT(*) as cnt,
       AVG(price) as avg_price
FROM market_data
WHERE price > 0
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
HAVING COUNT(*) >= 10
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',
    'active_symbols.type' = 'kafka_sink',
    'active_symbols.topic' = 'active_symbols_output',
    'active_symbols.format' = 'json'
);
```

**When to use**: Pre-filter rows, then aggregate, then filter groups

---

## Different Window Types (CTAS)

### SLIDING Window - 10m Window, 2m Advance

```sql
CREATE TABLE moving_averages AS
SELECT symbol,
       COUNT(*) as trade_count,
       AVG(price) as moving_avg
FROM market_data
GROUP BY symbol
WINDOW SLIDING(INTERVAL '10' MINUTE, INTERVAL '2' MINUTE)
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',
    'moving_averages.type' = 'kafka_sink',
    'moving_averages.topic' = 'moving_averages_output',
    'moving_averages.format' = 'json'
);
```

**When to use**: Overlapping windows with smooth progression

### SESSION Window - 30 Second Gap

```sql
CREATE TABLE user_sessions AS
SELECT user_id,
       COUNT(*) as actions_in_session,
       COUNT(DISTINCT page) as pages_visited
FROM user_events
GROUP BY user_id
WINDOW SESSION(INTERVAL '30' SECOND)
EMIT CHANGES
WITH (
    'user_events.type' = 'kafka_source',
    'user_events.topic' = 'user_events_input',
    'user_events.format' = 'json',
    'user_sessions.type' = 'kafka_sink',
    'user_sessions.topic' = 'user_sessions_output',
    'user_sessions.format' = 'json'
);
```

**When to use**: Group events by inactivity (user sessions, trading sessions)

### Time Window with Specific Time Column

```sql
CREATE TABLE trade_time_aggs AS
SELECT symbol,
       COUNT(*) as cnt,
       AVG(price) as avg_price
FROM market_data
GROUP BY symbol
WINDOW TUMBLING(market_data.trade_time, INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',
    'trade_time_aggs.type' = 'kafka_sink',
    'trade_time_aggs.topic' = 'trade_time_aggs_output',
    'trade_time_aggs.format' = 'json'
);
```

**When to use**: Specify which timestamp column to use for windowing

---

## ROWS WINDOW - Window Functions (CSAS)

### LAG - Previous Row Value

```sql
CREATE STREAM price_changes AS
SELECT symbol,
       price,
       event_time,
       LAG(price, 1) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
       ) as prev_price
FROM trades
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'price_changes.type' = 'kafka_sink',
    'price_changes.topic' = 'price_changes_output',
    'price_changes.format' = 'json'
);
```

**When to use**: Compare current row to previous row (price changes, deltas)

### Moving Average - Last N Rows

```sql
CREATE STREAM moving_avg_stream AS
SELECT symbol,
       price,
       event_time,
       AVG(price) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
       ) as moving_avg_100
FROM market_data
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',
    'moving_avg_stream.type' = 'kafka_sink',
    'moving_avg_stream.topic' = 'moving_avg_stream_output',
    'moving_avg_stream.format' = 'json'
);
```

**When to use**: Calculate moving average over last N rows

### COUNT with ROWS WINDOW

```sql
CREATE STREAM trade_counts AS
SELECT trader_id,
       symbol,
       price,
       COUNT(*) OVER (
           ROWS WINDOW BUFFER 1000 ROWS
           PARTITION BY trader_id
           ORDER BY event_time
       ) as trade_count_1000
FROM trades
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'trade_counts.type' = 'kafka_sink',
    'trade_counts.topic' = 'trade_counts_output',
    'trade_counts.format' = 'json'
);
```

**When to use**: Count rows in last N-row window

### ROWS WINDOW with Window Frame (Last 50 of 100)

```sql
CREATE STREAM qty_last_50 AS
SELECT symbol,
       price,
       SUM(quantity) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
           ROWS BETWEEN 50 PRECEDING AND CURRENT ROW
       ) as qty_last_50
FROM trades
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'qty_last_50.type' = 'kafka_sink',
    'qty_last_50.topic' = 'qty_last_50_output',
    'qty_last_50.format' = 'json'
);
```

**When to use**: Use subset of buffer (last 50 of 100 rows)

### Multiple Window Functions

```sql
CREATE STREAM trade_analytics AS
SELECT symbol,
       price,
       event_time,
       LAG(price, 1) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
       ) as prev_price,
       AVG(price) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
       ) as moving_avg,
       ROW_NUMBER() OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
       ) as row_num
FROM trades
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'trade_analytics.type' = 'kafka_sink',
    'trade_analytics.topic' = 'trade_analytics_output',
    'trade_analytics.format' = 'json'
);
```

**When to use**: Multiple analyses on same data window

---

## Complex Queries (GROUP BY + WINDOW)

### GROUP BY with Time Window and Filters (CTAS)

```sql
CREATE TABLE trader_symbol_stats AS
SELECT trader_id,
       symbol,
       COUNT(*) as trade_count,
       AVG(price) as avg_price,
       SUM(quantity) as total_qty
FROM market_data
WHERE price > 0
GROUP BY trader_id, symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',
    'trader_symbol_stats.type' = 'kafka_sink',
    'trader_symbol_stats.topic' = 'trader_symbol_stats_output',
    'trader_symbol_stats.format' = 'json'
);
```

**When to use**: Group data, then window results, with pre/post filtering

---

## SQL-Native Metrics (@metric Annotations)

**CRITICAL**: Metric annotations must be placed **immediately before** each `CREATE STREAM` statement.
The parser only associates metrics with the query that directly follows the annotations.

### Counter Metric (CSAS)

```sql
-- @metric: velo_trading_market_data_total
-- @metric_type: counter
-- @metric_help: "Total market data records processed"
-- @metric_labels: symbol, exchange
CREATE STREAM market_data_ts AS
SELECT symbol,
       exchange,
       price,
       volume,
       event_time
FROM raw_market_data
EMIT CHANGES
WITH (
    'raw_market_data.type' = 'kafka_source',
    'raw_market_data.topic' = 'market_data_input',
    'market_data_ts.type' = 'kafka_sink',
    'market_data_ts.topic' = 'market_data_output'
);
```

**When to use**: Count records processed, errors, events. No `@metric_field` needed - increments by 1 per record.

### Gauge Metric with Field (CSAS)

```sql
-- @metric: velo_trading_current_price
-- @metric_type: gauge
-- @metric_help: "Current market price per symbol"
-- @metric_field: price
-- @metric_labels: symbol
CREATE STREAM price_updates AS
SELECT symbol,
       price,
       bid_price,
       ask_price,
       event_time
FROM market_data_ts
WHERE price > 0
EMIT CHANGES
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic' = 'market_data_ts',
    'price_updates.type' = 'kafka_sink',
    'price_updates.topic' = 'price_updates_output'
);
```

**When to use**: Track current values (price, volume, count). **`@metric_field` is REQUIRED** - specifies which output field to measure.

### Histogram Metric (CSAS)

```sql
-- @metric: velo_trading_latency_seconds
-- @metric_type: histogram
-- @metric_help: "Processing latency distribution"
-- @metric_field: processing_latency_ms
-- @metric_labels: symbol
-- @metric_buckets: 1, 5, 10, 50, 100, 500, 1000
CREATE STREAM latency_tracking AS
SELECT symbol,
       EXTRACT(EPOCH FROM (NOW() - event_time)) * 1000 as processing_latency_ms,
       event_time
FROM market_data_ts
EMIT CHANGES
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic' = 'market_data_ts',
    'latency_tracking.type' = 'kafka_sink',
    'latency_tracking.topic' = 'latency_output'
);
```

**When to use**: Measure distributions (latency, response times). **`@metric_field` is REQUIRED**. `@metric_buckets` defines histogram boundaries.

### Multiple Metrics per Query (CSAS)

```sql
-- @metric: velo_trading_tick_buckets_total
-- @metric_type: counter
-- @metric_help: "Total OHLCV tick buckets generated"
-- @metric_labels: symbol
--
-- @metric: velo_trading_tick_volume
-- @metric_type: gauge
-- @metric_help: "Current tick volume"
-- @metric_field: total_volume
-- @metric_labels: symbol
--
-- @metric: velo_trading_tick_price_distribution
-- @metric_type: histogram
-- @metric_help: "Price distribution per tick"
-- @metric_field: avg_price
-- @metric_labels: symbol
-- @metric_buckets: 10, 50, 100, 200, 500, 1000
CREATE STREAM tick_buckets AS
SELECT symbol,
       AVG(price) as avg_price,
       SUM(volume) as total_volume,
       COUNT(*) as trade_count,
       _window_start AS window_start,
       _window_end AS window_end
FROM market_data_ts
GROUP BY symbol
WINDOW TUMBLING(event_time, INTERVAL '1' SECOND)
EMIT CHANGES
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic' = 'market_data_ts',
    'tick_buckets.type' = 'kafka_sink',
    'tick_buckets.topic' = 'tick_buckets_output'
);
```

**When to use**: Multiple metrics from same query. Separate metric blocks with `--` blank comment line.

### Conditional Metrics (CSAS)

```sql
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_help: "Volume spike detections by classification"
-- @metric_labels: symbol, spike_classification
-- @metric_condition: spike_classification IN ('EXTREME_SPIKE', 'HIGH_SPIKE')
CREATE STREAM volume_spikes AS
SELECT symbol,
       volume,
       CASE
           WHEN volume > avg_volume * 5 THEN 'EXTREME_SPIKE'
           WHEN volume > avg_volume * 3 THEN 'HIGH_SPIKE'
           ELSE 'NORMAL'
       END as spike_classification
FROM volume_analysis
WHERE volume > avg_volume * 3
EMIT CHANGES
WITH (
    'volume_analysis.type' = 'kafka_source',
    'volume_analysis.topic' = 'volume_analysis',
    'volume_spikes.type' = 'kafka_sink',
    'volume_spikes.topic' = 'volume_spikes_output'
);
```

**When to use**: Only emit metrics when conditions are met. `@metric_condition` filters which records trigger metric emission.

### Metric Annotation Reference

| Annotation | Required | Type | Description |
|------------|----------|------|-------------|
| `@metric` | Yes | string | Metric name (use snake_case, prefix with `velo_`) |
| `@metric_type` | Yes | counter/gauge/histogram | Prometheus metric type |
| `@metric_help` | Yes | string | Help text shown in Prometheus |
| `@metric_field` | **gauge/histogram only** | string | Output field to measure |
| `@metric_labels` | No | string | Comma-separated field names for labels |
| `@metric_condition` | No | SQL expr | Only emit when condition is true |
| `@metric_buckets` | No | numbers | Histogram bucket boundaries |
| `@metric_sample_rate` | No | 0.0-1.0 | Sampling rate (default: 1.0 = 100%) |

### Common Mistakes

```sql
-- ❌ WRONG: Metrics in header, far from CREATE STREAM
-- @metric: my_metric
-- ... 50 lines of other comments ...
CREATE STREAM my_stream AS ...  -- Parser won't associate metric!

-- ✅ CORRECT: Metrics immediately before CREATE STREAM
-- @metric: my_metric
-- @metric_type: counter
CREATE STREAM my_stream AS ...  -- Metric properly attached!

-- ❌ WRONG: Gauge without @metric_field
-- @metric: current_price
-- @metric_type: gauge
CREATE STREAM ...  -- Won't know which field to measure!

-- ✅ CORRECT: Gauge with @metric_field
-- @metric: current_price
-- @metric_type: gauge
-- @metric_field: price
CREATE STREAM ...  -- Measures the 'price' output field
```

### Recommended Workflow: Use `velo-test annotate`

Instead of manually writing metrics, use `velo-test annotate` to auto-generate them:

```bash
# 1. Write your SQL (no metrics needed)
vim apps/my_app.sql

# 2. Generate annotated SQL with metrics placed correctly
velo-test annotate apps/my_app.sql \
  --output apps/my_app.annotated.sql \
  --monitoring monitoring

# 3. Run the annotated version
velo-test run apps/my_app.annotated.sql
```

**Benefits:**
- Metrics are automatically placed immediately before each `CREATE STREAM`
- Generates appropriate metrics based on query analysis (counters, gauges, histograms)
- Creates Grafana dashboards and Prometheus config
- No manual placement errors

See [CLI Reference - velo-test annotate](../test-harness/CLI_REFERENCE.md#velo-test-annotate) for full documentation.

---

## JOIN Queries

### Simple Stream-Table JOIN (CSAS)

```sql
CREATE STREAM enriched_orders AS
SELECT o.order_id,
       o.customer_id,
       c.customer_name,
       o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 100
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'customers.type' = 'kafka_source',
    'customers.topic' = 'customers_input',
    'customers.format' = 'json',
    'enriched_orders.type' = 'kafka_sink',
    'enriched_orders.topic' = 'enriched_orders_output',
    'enriched_orders.format' = 'json'
);
```

**When to use**: Combine data from two streams/tables

### JOIN with Aggregation (CTAS)

```sql
CREATE TABLE customer_order_stats AS
SELECT c.customer_id,
       c.customer_name,
       COUNT(*) as order_count,
       AVG(o.amount) as avg_order
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 50
GROUP BY c.customer_id, c.customer_name
WINDOW TUMBLING(INTERVAL '1' HOUR)
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'customers.type' = 'kafka_source',
    'customers.topic' = 'customers_input',
    'customers.format' = 'json',
    'customer_order_stats.type' = 'kafka_sink',
    'customer_order_stats.topic' = 'customer_order_stats_output',
    'customer_order_stats.format' = 'json'
);
```

**When to use**: Join then aggregate

### Stream-Stream Interval JOIN (FR-085)

```sql
-- Orders joined with shipments within 24 hours
CREATE STREAM order_shipments AS
SELECT o.order_id,
       o.customer_id,
       o.total_amount,
       s.shipment_id,
       s.carrier,
       s.tracking_number
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
  AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '24' HOUR
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'shipments.type' = 'kafka_source',
    'shipments.topic' = 'shipments_input',
    'shipments.format' = 'json',
    'order_shipments.type' = 'kafka_sink',
    'order_shipments.topic' = 'order_shipments_output',
    'order_shipments.format' = 'json'
);
```

**When to use**: Join two unbounded streams with a time constraint
**Performance**: ~517K rec/sec (JoinCoordinator), ~5.1M rec/sec (JoinStateStore)

### Stream-Stream Interval JOIN with Configuration

```sql
-- Click attribution: join clicks with purchases within 30 minutes
CREATE STREAM click_attribution AS
SELECT c.user_id,
       c.ad_id,
       c.click_time,
       p.purchase_id,
       p.amount
FROM ad_clicks c
JOIN purchases p ON c.user_id = p.user_id
  AND p.event_time BETWEEN c.event_time AND c.event_time + INTERVAL '30' MINUTE
EMIT CHANGES
WITH (
    'join.type' = 'interval',
    'join.lower_bound' = '0s',
    'join.upper_bound' = '30m',
    'join.retention' = '1h',
    'ad_clicks.type' = 'kafka_source',
    'ad_clicks.topic' = 'ad_clicks_input',
    'ad_clicks.format' = 'json',
    'purchases.type' = 'kafka_source',
    'purchases.topic' = 'purchases_input',
    'purchases.format' = 'json',
    'click_attribution.type' = 'kafka_sink',
    'click_attribution.topic' = 'click_attribution_output',
    'click_attribution.format' = 'json'
);
```

**When to use**: Stream-stream join with explicit retention and interval configuration
**Key config options**:
- `join.type`: `interval` for time-bounded joins
- `join.lower_bound`: Minimum time difference (e.g., `0s`, `-5m`)
- `join.upper_bound`: Maximum time difference (e.g., `30m`, `24h`)
- `join.retention`: How long to keep records in state (e.g., `1h`, `48h`)

---

## Kafka Message Keys - PRIMARY KEY Annotation (FR-089)

The `PRIMARY KEY` annotation allows you to explicitly specify which field(s) should be used as the Kafka message key.
This is essential for proper partitioning in downstream consumers. This follows the SQL standard syntax used by Flink, RisingWave, and Materialize.

### Single Key Field (CSAS)

```sql
CREATE STREAM keyed_trades AS
SELECT symbol PRIMARY KEY, price, quantity, event_time
FROM trades
WHERE price > 0
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'keyed_trades.type' = 'kafka_sink',
    'keyed_trades.topic' = 'keyed_trades_output',
    'keyed_trades.format' = 'json'
);
```

**When to use**: Ensure consistent partitioning by a specific field

### Compound Key (Multiple Fields) (CSAS)

```sql
CREATE STREAM region_product_keyed AS
SELECT region PRIMARY KEY, product PRIMARY KEY, quantity, revenue
FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'region_product_keyed.type' = 'kafka_sink',
    'region_product_keyed.topic' = 'region_product_output',
    'region_product_keyed.format' = 'json'
);
```

**When to use**: Partition by multiple fields (creates pipe-delimited compound key: `"US|Widget"`)

### PRIMARY KEY with Alias (CSAS)

```sql
CREATE STREAM symbol_keyed AS
SELECT stock_symbol AS sym PRIMARY KEY, price, volume
FROM market_data
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',
    'symbol_keyed.type' = 'kafka_sink',
    'symbol_keyed.topic' = 'symbol_keyed_output',
    'symbol_keyed.format' = 'json'
);
```

**When to use**: Rename field and use alias as key

### PRIMARY KEY with GROUP BY Aggregation (CTAS)

```sql
CREATE TABLE symbol_stats AS
SELECT symbol PRIMARY KEY, COUNT(*) as trade_count, AVG(price) as avg_price
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'symbol_stats.type' = 'kafka_sink',
    'symbol_stats.topic' = 'symbol_stats_output',
    'symbol_stats.format' = 'json'
);
```

**When to use**: Explicitly set key for aggregated output (GROUP BY columns are implicitly keyed, but PRIMARY KEY makes it explicit)

### Compound PRIMARY KEY with GROUP BY (CTAS)

```sql
CREATE TABLE regional_stats AS
SELECT
    region PRIMARY KEY,
    product PRIMARY KEY,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY region, product
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'regional_stats.type' = 'kafka_sink',
    'regional_stats.topic' = 'regional_stats_output',
    'regional_stats.format' = 'json'
);
```

**When to use**: Aggregate by multiple dimensions with explicit compound key (key = `"US|Widget"`)

### PRIMARY KEY vs GROUP BY Behavior

| Scenario | Key Format | Example |
|----------|------------|---------|
| `GROUP BY symbol` (single, no PRIMARY KEY) | Raw value | `"AAPL"` |
| `GROUP BY region, product` (compound, no PRIMARY KEY) | Pipe-delimited | `"US\|Widget"` |
| `SELECT symbol PRIMARY KEY ... GROUP BY symbol` | Raw value | `"AAPL"` (explicit) |
| `SELECT a PRIMARY KEY, b PRIMARY KEY ... GROUP BY a, b` | Pipe-delimited | `"X\|Y"` (explicit) |
| `SELECT a PRIMARY KEY ... GROUP BY a, b` | Raw value | `"X"` (PRIMARY KEY wins) |
| `SELECT symbol PRIMARY KEY ...` (no GROUP BY) | Raw value | `"AAPL"` |
| No PRIMARY KEY, no GROUP BY | Null | Round-robin partitioning |

---

## System Columns (Kafka Metadata)

### Using _timestamp, _offset, _partition (CSAS)

```sql
CREATE STREAM orders_with_metadata AS
SELECT customer_id,
       amount,
       _timestamp as kafka_time,
       _offset as kafka_offset,
       _partition as kafka_partition
FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'orders_with_metadata.type' = 'kafka_sink',
    'orders_with_metadata.topic' = 'orders_with_metadata_output',
    'orders_with_metadata.format' = 'json'
);
```

**When to use**: Access Kafka message metadata

---

## Type Conversion & Null Handling

### CAST - Type Conversion (CSAS)

```sql
CREATE STREAM typed_orders AS
SELECT order_id,
       CAST(amount AS INTEGER) as amount_int,
       CAST(price AS STRING) as price_str,
       CAST(quantity AS FLOAT) as qty_float
FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'typed_orders.type' = 'kafka_sink',
    'typed_orders.topic' = 'typed_orders_output',
    'typed_orders.format' = 'json'
);
```

**Supported types**: INTEGER, FLOAT, DOUBLE, STRING, VARCHAR, BOOLEAN, DATE, TIMESTAMP, DECIMAL

### COALESCE - First Non-NULL Value (CSAS)

```sql
CREATE STREAM customers_with_defaults AS
SELECT customer_id,
       COALESCE(nickname, first_name, 'Unknown') as display_name,
       COALESCE(discount, 0) as discount_applied
FROM customers
WITH (
    'customers.type' = 'kafka_source',
    'customers.topic' = 'customers_input',
    'customers.format' = 'json',
    'customers_with_defaults.type' = 'kafka_sink',
    'customers_with_defaults.topic' = 'customers_with_defaults_output',
    'customers_with_defaults.format' = 'json'
);
```

**When to use**: Provide default values when data might be NULL

### NULLIF - Return NULL on Match (CSAS)

```sql
CREATE STREAM cleaned_customers AS
SELECT customer_id,
       NULLIF(email, '') as email_or_null,
       total_revenue / NULLIF(quantity_sold, 0) as avg_price
FROM customers
WITH (
    'customers.type' = 'kafka_source',
    'customers.topic' = 'customers_input',
    'customers.format' = 'json',
    'cleaned_customers.type' = 'kafka_sink',
    'cleaned_customers.topic' = 'cleaned_customers_output',
    'cleaned_customers.format' = 'json'
);
```

**When to use**: Convert specific values to NULL (empty strings, sentinel values, zeros)

---

## Conditional Logic (CASE WHEN)

### Basic CASE Expression (CSAS)

```sql
CREATE STREAM categorized_orders AS
SELECT order_id,
       amount,
       CASE
           WHEN amount > 1000 THEN 'large'
           WHEN amount > 100 THEN 'medium'
           ELSE 'small'
       END as order_size
FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'categorized_orders.type' = 'kafka_sink',
    'categorized_orders.topic' = 'categorized_orders_output',
    'categorized_orders.format' = 'json'
);
```

**When to use**: Categorize data based on conditions

### CASE in Aggregation - Conditional Counting (CTAS)

```sql
CREATE TABLE user_status_counts AS
SELECT COUNT(CASE WHEN status = 'active' THEN 1 END) as active_count,
       COUNT(CASE WHEN status = 'inactive' THEN 1 END) as inactive_count,
       COUNT(*) as total_count
FROM users
GROUP BY 1
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    'users.type' = 'kafka_source',
    'users.topic' = 'users_input',
    'users.format' = 'json',
    'user_status_counts.type' = 'kafka_sink',
    'user_status_counts.topic' = 'user_status_counts_output',
    'user_status_counts.format' = 'json'
);
```

**When to use**: Pivot data, count/sum conditionally within a single query

---

## String Functions (CSAS)

### UPPER, LOWER, CONCAT

```sql
CREATE STREAM formatted_customers AS
SELECT customer_id,
       UPPER(name) as name_upper,
       LOWER(email) as email_lower,
       CONCAT(first_name, ' ', last_name) as full_name
FROM customers
WITH (
    'customers.type' = 'kafka_source',
    'customers.topic' = 'customers_input',
    'customers.format' = 'json',
    'formatted_customers.type' = 'kafka_sink',
    'formatted_customers.topic' = 'formatted_customers_output',
    'formatted_customers.format' = 'json'
);
```

### SUBSTRING, LEFT, RIGHT

```sql
CREATE STREAM parsed_skus AS
SELECT sku,
       SUBSTRING(sku, 1, 3) as prefix,
       LEFT(sku, 3) as left_prefix,
       RIGHT(sku, 4) as suffix
FROM products
WITH (
    'products.type' = 'kafka_source',
    'products.topic' = 'products_input',
    'products.format' = 'json',
    'parsed_skus.type' = 'kafka_sink',
    'parsed_skus.topic' = 'parsed_skus_output',
    'parsed_skus.format' = 'json'
);
```

### TRIM and REPLACE

```sql
CREATE STREAM cleaned_data AS
SELECT TRIM(user_input) as cleaned,
       REPLACE(phone, '-', '') as phone_digits
FROM form_data
WITH (
    'form_data.type' = 'kafka_source',
    'form_data.topic' = 'form_data_input',
    'form_data.format' = 'json',
    'cleaned_data.type' = 'kafka_sink',
    'cleaned_data.topic' = 'cleaned_data_output',
    'cleaned_data.format' = 'json'
);
```

---

## Math Functions (CSAS)

### ABS, ROUND, FLOOR, CEIL

```sql
CREATE STREAM calculated_values AS
SELECT account_id,
       balance,
       ABS(balance) as absolute_balance,
       ROUND(price, 2) as rounded_price,
       FLOOR(price) as floor_price,
       CEIL(price) as ceil_price
FROM accounts
WITH (
    'accounts.type' = 'kafka_source',
    'accounts.topic' = 'accounts_input',
    'accounts.format' = 'json',
    'calculated_values.type' = 'kafka_sink',
    'calculated_values.topic' = 'calculated_values_output',
    'calculated_values.format' = 'json'
);
```

### SQRT, POWER, MOD

```sql
CREATE STREAM math_results AS
SELECT value,
       SQRT(value) as square_root,
       POWER(value, 2) as squared,
       MOD(id, 10) as bucket
FROM numbers
WITH (
    'numbers.type' = 'kafka_source',
    'numbers.topic' = 'numbers_input',
    'numbers.format' = 'json',
    'math_results.type' = 'kafka_sink',
    'math_results.topic' = 'math_results_output',
    'math_results.format' = 'json'
);
```

### GREATEST and LEAST

```sql
CREATE STREAM price_bounds AS
SELECT symbol,
       bid,
       ask,
       GREATEST(bid, ask, last_price) as max_price,
       LEAST(bid, ask, last_price) as min_price
FROM quotes
WITH (
    'quotes.type' = 'kafka_source',
    'quotes.topic' = 'quotes_input',
    'quotes.format' = 'json',
    'price_bounds.type' = 'kafka_sink',
    'price_bounds.topic' = 'price_bounds_output',
    'price_bounds.format' = 'json'
);
```

---

## Date/Time Functions (CSAS)

### EXTRACT - Get Date/Time Parts

```sql
CREATE STREAM date_parts AS
SELECT order_date,
       EXTRACT(YEAR FROM order_date) as year,
       EXTRACT(MONTH FROM order_date) as month,
       EXTRACT(DAY FROM order_date) as day,
       EXTRACT(HOUR FROM order_date) as hour
FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'date_parts.type' = 'kafka_sink',
    'date_parts.topic' = 'date_parts_output',
    'date_parts.format' = 'json'
);
```

**EXTRACT parts**: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOW, DOY, QUARTER, WEEK, EPOCH, MILLISECOND

### NOW and DATEDIFF

```sql
CREATE STREAM timestamped_orders AS
SELECT order_id,
       amount,
       NOW() as processed_at,
       DATEDIFF('days', order_date, ship_date) as days_to_ship
FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'timestamped_orders.type' = 'kafka_sink',
    'timestamped_orders.topic' = 'timestamped_orders_output',
    'timestamped_orders.format' = 'json'
);
```

### FROM_UNIXTIME and UNIX_TIMESTAMP

```sql
CREATE STREAM converted_timestamps AS
SELECT event_id,
       epoch_seconds,
       FROM_UNIXTIME(epoch_seconds) as event_time,
       UNIX_TIMESTAMP(order_date) as epoch
FROM events
WITH (
    'events.type' = 'kafka_source',
    'events.topic' = 'events_input',
    'events.format' = 'json',
    'converted_timestamps.type' = 'kafka_sink',
    'converted_timestamps.topic' = 'converted_timestamps_output',
    'converted_timestamps.format' = 'json'
);
```

---

## Additional Window Functions (CSAS)

### LEAD - Next Row Value

```sql
CREATE STREAM next_prices AS
SELECT symbol,
       price,
       event_time,
       LEAD(price, 1) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
       ) as next_price
FROM trades
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'next_prices.type' = 'kafka_sink',
    'next_prices.topic' = 'next_prices_output',
    'next_prices.format' = 'json'
);
```

### RANK and DENSE_RANK

```sql
CREATE STREAM price_rankings AS
SELECT symbol,
       price,
       RANK() OVER (
           ROWS WINDOW BUFFER 100 ROWS
           ORDER BY price DESC
       ) as price_rank,
       DENSE_RANK() OVER (
           ROWS WINDOW BUFFER 100 ROWS
           ORDER BY price DESC
       ) as dense_price_rank
FROM trades
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'price_rankings.type' = 'kafka_sink',
    'price_rankings.topic' = 'price_rankings_output',
    'price_rankings.format' = 'json'
);
```

### FIRST_VALUE and LAST_VALUE

```sql
CREATE STREAM price_extremes AS
SELECT symbol,
       price,
       event_time,
       FIRST_VALUE(price) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
       ) as opening_price,
       LAST_VALUE(price) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY event_time
       ) as latest_price
FROM trades
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'price_extremes.type' = 'kafka_sink',
    'price_extremes.topic' = 'price_extremes_output',
    'price_extremes.format' = 'json'
);
```

### NTILE - Divide into Buckets

```sql
CREATE STREAM score_quartiles AS
SELECT student_id,
       score,
       NTILE(4) OVER (
           ROWS WINDOW BUFFER 1000 ROWS
           ORDER BY score DESC
       ) as quartile
FROM exam_results
WITH (
    'exam_results.type' = 'kafka_source',
    'exam_results.topic' = 'exam_results_input',
    'exam_results.format' = 'json',
    'score_quartiles.type' = 'kafka_sink',
    'score_quartiles.topic' = 'score_quartiles_output',
    'score_quartiles.format' = 'json'
);
```

---

## Statistical Functions (CTAS)

### STDDEV_POP and STDDEV_SAMP

```sql
CREATE TABLE trade_volatility AS
SELECT symbol,
       COUNT(*) as trade_count,
       AVG(price) as avg_price,
       STDDEV_POP(price) as price_stddev_pop,
       STDDEV_SAMP(price) as price_stddev_samp
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'trade_volatility.type' = 'kafka_sink',
    'trade_volatility.topic' = 'trade_volatility_output',
    'trade_volatility.format' = 'json'
);
```

> **STDDEV_POP vs STDDEV_SAMP**: `STDDEV_POP` divides by N (population), `STDDEV_SAMP` divides by N-1 (sample, returns NULL for single-element input).

### VAR_POP and VAR_SAMP

```sql
CREATE TABLE price_variance AS
SELECT symbol,
       VAR_POP(price) as price_var_pop,
       VAR_SAMP(price) as price_var_samp
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'price_variance.type' = 'kafka_sink',
    'price_variance.topic' = 'price_variance_output',
    'price_variance.format' = 'json'
);
```

### MEDIAN

```sql
CREATE TABLE category_medians AS
SELECT category,
       MEDIAN(price) as median_price
FROM products
GROUP BY category
WINDOW TUMBLING(INTERVAL '1' HOUR)
EMIT CHANGES
WITH (
    'products.type' = 'kafka_source',
    'products.topic' = 'products_input',
    'products.format' = 'json',
    'category_medians.type' = 'kafka_sink',
    'category_medians.topic' = 'category_medians_output',
    'category_medians.format' = 'json'
);
```

### CORR - Correlation

```sql
CREATE TABLE price_volume_corr AS
SELECT symbol,
       CORR(price, volume) as price_volume_correlation
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'price_volume_corr.type' = 'kafka_sink',
    'price_volume_corr.topic' = 'price_volume_corr_output',
    'price_volume_corr.format' = 'json'
);
```

### LISTAGG and COLLECT

```sql
CREATE TABLE symbol_traders AS
SELECT symbol,
       COUNT(*) as trade_count,
       LISTAGG(trader_id) as all_traders,
       COLLECT(exchange) as exchanges
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'symbol_traders.type' = 'kafka_sink',
    'symbol_traders.topic' = 'symbol_traders_output',
    'symbol_traders.format' = 'json'
);
```

> **LISTAGG vs COLLECT**: Both produce comma-separated strings. `LISTAGG` is the SQL standard name, `COLLECT` is the Flink-compatible alias.

### SUM(DISTINCT) and COUNT(DISTINCT)

```sql
CREATE TABLE unique_trade_stats AS
SELECT symbol,
       COUNT(DISTINCT trader_id) as unique_traders,
       SUM(DISTINCT quantity) as unique_qty_sum
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'unique_trade_stats.type' = 'kafka_sink',
    'unique_trade_stats.topic' = 'unique_trade_stats_output',
    'unique_trade_stats.format' = 'json'
);
```

### MAX/MIN on Strings and Timestamps

```sql
-- MAX/MIN work on strings (lexicographic) and timestamps (chronological)
CREATE TABLE name_range AS
SELECT department,
       MIN(employee_name) as first_alphabetically,
       MAX(employee_name) as last_alphabetically,
       MIN(hire_date) as earliest_hire,
       MAX(hire_date) as latest_hire
FROM employees
GROUP BY department
WINDOW TUMBLING(INTERVAL '1' HOUR)
EMIT CHANGES
WITH (
    'employees.type' = 'kafka_source',
    'employees.topic' = 'employees_input',
    'employees.format' = 'json',
    'name_range.type' = 'kafka_sink',
    'name_range.topic' = 'name_range_output',
    'name_range.format' = 'json'
);
```

### Type Safety - Common Mistakes

```sql
-- ❌ WRONG: SUM on a string column — returns error
SELECT SUM(customer_name) FROM orders  -- Error: SUM requires numeric input, got STRING

-- ❌ WRONG: AVG on a string column — returns error
SELECT AVG(status) FROM orders  -- Error: AVG requires numeric input, got STRING

-- ❌ WRONG: STDDEV_POP on a string column — returns error
SELECT STDDEV_POP(name) FROM users  -- Error: STDDEV_POP requires numeric input, got STRING

-- ✅ CORRECT: Numeric aggregates on numeric columns
SELECT SUM(amount), AVG(price), STDDEV_POP(quantity) FROM trades

-- ✅ CORRECT: MAX/MIN on strings (lexicographic comparison)
SELECT MAX(symbol), MIN(symbol) FROM trades

-- ✅ CORRECT: MAX/MIN on timestamps (chronological comparison)
SELECT MAX(event_time), MIN(event_time) FROM trades

-- ✅ CORRECT: COUNT works on any type
SELECT COUNT(*), COUNT(name), COUNT(DISTINCT status) FROM orders

-- ✅ CORRECT: Mixed numeric types (Integer + Float) are fine
SELECT SUM(price), AVG(quantity) FROM trades  -- Works even if price is FLOAT and quantity is INTEGER
```

---

## JSON Functions (CSAS)

### JSON_EXTRACT and JSON_VALUE

```sql
CREATE STREAM extracted_json AS
SELECT event_id,
       JSON_EXTRACT(payload, '$.user.id') as user_id,
       JSON_EXTRACT(payload, '$.action') as action,
       JSON_VALUE(settings, '$.timeout') as timeout
FROM events
WITH (
    'events.type' = 'kafka_source',
    'events.topic' = 'events_input',
    'events.format' = 'json',
    'extracted_json.type' = 'kafka_sink',
    'extracted_json.topic' = 'extracted_json_output',
    'extracted_json.format' = 'json'
);
```

---

## Kafka Header Functions (CSAS)

### HEADER and HAS_HEADER

```sql
CREATE STREAM traced_orders AS
SELECT order_id,
       amount,
       HEADER('trace-id') as trace_id,
       HEADER('correlation-id') as correlation_id,
       CASE
           WHEN HAS_HEADER('express') THEN 'express'
           ELSE 'standard'
       END as shipping_type
FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'traced_orders.type' = 'kafka_sink',
    'traced_orders.topic' = 'traced_orders_output',
    'traced_orders.format' = 'json'
);
```

---

## Window Boundaries - System Columns (CTAS)

### Using _window_start and _window_end

```sql
CREATE TABLE windowed_trades AS
SELECT symbol,
       COUNT(*) as trade_count,
       AVG(price) as avg_price,
       _window_start as window_start,
       _window_end as window_end
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'windowed_trades.type' = 'kafka_sink',
    'windowed_trades.topic' = 'windowed_trades_output',
    'windowed_trades.format' = 'json'
);
```

---

## IN and BETWEEN Operators (CSAS)

### IN - Check Membership

```sql
CREATE STREAM active_orders AS
SELECT *
FROM orders
WHERE status IN ('pending', 'processing', 'shipped')
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'active_orders.type' = 'kafka_sink',
    'active_orders.topic' = 'active_orders_output',
    'active_orders.format' = 'json'
);
```

### BETWEEN - Range Check

```sql
CREATE STREAM mid_range_orders AS
SELECT *
FROM orders
WHERE amount BETWEEN 100 AND 500
WITH (
    'orders.type' = 'kafka_sink',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'mid_range_orders.type' = 'kafka_sink',
    'mid_range_orders.topic' = 'mid_range_orders_output',
    'mid_range_orders.format' = 'json'
);
```

---

## Common Mistakes (Don't Do This!)

### ❌ Wrong: Plain SELECT (not valid for streaming jobs)

```sql
-- ❌ WRONG - Plain SELECT not valid for deployed jobs
SELECT * FROM orders WHERE amount > 100

-- ✅ CORRECT - Use CREATE STREAM or CREATE TABLE
CREATE STREAM filtered_orders AS
SELECT * FROM orders WHERE amount > 100
WITH (...);
```

### ❌ Wrong: Using output.type instead of named sink

```sql
-- ❌ WRONG - Generic output.type
CREATE STREAM my_stream AS
SELECT * FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'output.type' = 'kafka_sink',        -- WRONG!
    'output.topic' = 'my_output'
);

-- ✅ CORRECT - Named sink matches stream name
CREATE STREAM my_stream AS
SELECT * FROM orders
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',
    'my_stream.type' = 'kafka_sink',     -- Matches stream name!
    'my_stream.topic' = 'my_stream_output',
    'my_stream.format' = 'json'
);
```

### ❌ Wrong: CSAS for aggregations (use CTAS)

```sql
-- ❌ WRONG - CSAS with aggregation
CREATE STREAM trade_stats AS
SELECT symbol, COUNT(*) as cnt
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE);

-- ✅ CORRECT - CTAS for aggregations
CREATE TABLE trade_stats AS
SELECT symbol, COUNT(*) as cnt
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (...);
```

### ❌ Wrong: ROWS WINDOW syntax

```sql
-- ❌ WRONG
ROWS BUFFER 100 ROWS
ROWS WINDOW BUFFER 100
WINDOW ROWS 100

-- ✅ CORRECT
ROWS WINDOW BUFFER 100 ROWS
```

### ❌ Wrong: Missing parentheses in OVER clause

```sql
-- ❌ WRONG
AVG(price) OVER ROWS WINDOW BUFFER 100 ROWS

-- ✅ CORRECT
AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)
```

### ❌ Wrong: Confusing Window Types

```sql
-- ❌ WRONG - ROWS is count-based, not time-based
ROWS WINDOW TUMBLING(INTERVAL '1' MINUTE)

-- ✅ CORRECT - Time-based window
WINDOW TUMBLING(INTERVAL '1' MINUTE)

-- ✅ CORRECT - Count-based window
ROWS WINDOW BUFFER 100 ROWS
```

---

## How to Extend These Examples

### Change Time Intervals

```sql
-- 1-minute window
WINDOW TUMBLING(INTERVAL '1' MINUTE)

-- 5-minute window
WINDOW TUMBLING(INTERVAL '5' MINUTE)

-- 1-hour window
WINDOW TUMBLING(INTERVAL '1' HOUR)

-- 30-second window
WINDOW TUMBLING(INTERVAL '30' SECOND)
```

### Change Buffer Sizes

```sql
-- 100-row window
ROWS WINDOW BUFFER 100 ROWS

-- 500-row window
ROWS WINDOW BUFFER 500 ROWS

-- 1000-row window
ROWS WINDOW BUFFER 1000 ROWS
```

### Change Partition Columns

```sql
-- Single partition column
PARTITION BY symbol

-- Multiple partition columns
PARTITION BY symbol, trader_id

-- No partition (all rows together)
-- Just omit PARTITION BY
```

---

## WITH Clause Pattern Reference

### Source Configuration

```sql
'<source_name>.type' = 'kafka_source',
'<source_name>.topic' = '<topic_name>',
'<source_name>.format' = 'json'  -- or 'avro', 'protobuf'
```

### Sink Configuration (Named - matches CSAS/CTAS name)

```sql
'<stream_or_table_name>.type' = 'kafka_sink',
'<stream_or_table_name>.topic' = '<output_topic>',
'<stream_or_table_name>.format' = 'json'
```

### Full Example Pattern

```sql
CREATE STREAM|TABLE <name> AS
SELECT ...
FROM <source>
[WHERE ...]
[GROUP BY ...]
[WINDOW ...]
[EMIT CHANGES|FINAL]
WITH (
    -- Source(s)
    '<source>.type' = 'kafka_source',
    '<source>.topic' = '<input_topic>',
    '<source>.format' = 'json',

    -- Sink (name matches CREATE name)
    '<name>.type' = 'kafka_sink',
    '<name>.topic' = '<output_topic>',
    '<name>.format' = 'json'
);
```

---

## Data Generation Annotations (@data.*)

Embed test data generation hints directly in SQL files. These replace separate schema YAML files.

### Global Hints

```sql
-- @data.source: in_market_data        -- Source stream name (required for multi-source SQL)
-- @data.record_count: 1000
-- @data.time_simulation: sequential
-- @data.time_start: "-1h"
-- @data.time_end: "now"
-- @data.seed: 42
```

### Field Hints (Type Required)

```sql
-- String with enum constraint
-- @data.symbol.type: string
-- @data.symbol: enum ["AAPL", "GOOGL", "MSFT"], weights: [0.4, 0.3, 0.3]

-- Decimal with random walk (financial prices)
-- @data.price.type: decimal(4)
-- @data.price: range [100, 500], distribution: random_walk, volatility: 0.02, group_by: symbol

-- Integer with log-normal distribution
-- @data.volume.type: integer
-- @data.volume: range [100, 50000], distribution: log_normal

-- Sequential timestamp
-- @data.event_time.type: timestamp
-- @data.event_time: timestamp, sequential: true

-- UUID
-- @data.order_id.type: uuid

-- Boolean
-- @data.is_active.type: boolean

-- Derived field (calculated from other fields)
-- @data.bid_price.type: decimal(4)
-- @data.bid_price.derived: "price * random(0.998, 0.9999)"
```

### Full Trading Demo Example

```sql
-- =============================================================================
-- DATA GENERATION HINTS for in_market_data
-- =============================================================================
-- @data.record_count: 1000
-- @data.time_simulation: sequential
--
-- @data.symbol.type: string
-- @data.symbol: enum ["AAPL", "GOOGL", "MSFT", "AMZN"], weights: [0.25, 0.25, 0.25, 0.25]
--
-- @data.exchange.type: string
-- @data.exchange: enum ["NASDAQ", "NYSE"], weights: [0.6, 0.4]
--
-- @data.price.type: decimal(4)
-- @data.price: range [150, 400], distribution: random_walk, volatility: 0.02, group_by: symbol
--
-- @data.volume.type: integer
-- @data.volume: range [1000, 500000], distribution: log_normal
--
-- @data.timestamp.type: timestamp
-- @data.timestamp: timestamp, sequential: true

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

CREATE STREAM market_data_ts AS
SELECT
    symbol PRIMARY KEY,
    exchange,
    price,
    volume,
    timestamp
FROM in_market_data
EMIT CHANGES
WITH (
    'in_market_data.type' = 'kafka_source',
    'in_market_data.topic.name' = 'in_market_data',
    'market_data_ts.type' = 'kafka_sink',
    'market_data_ts.topic.name' = 'market_data_ts'
);
```

### Available Distributions

| Distribution | Description | Parameters |
|--------------|-------------|------------|
| `uniform` | Equal probability (default) | None |
| `normal` | Bell curve | `mean`, `std_dev` |
| `log_normal` | Right-skewed | `mean`, `std_dev` |
| `zipf` | Power law | `exponent` |
| `random_walk` | GBM for realistic prices | `volatility`, `drift`, `group_by` |

### Priority Rules

1. **Schema YAML wins**: If `schemas/<source>.schema.yaml` exists, it takes precedence
2. **Type required**: All fields need `@data.<field>.type: <type>`
3. **Strict validation**: Errors if hint field doesn't match source fields
