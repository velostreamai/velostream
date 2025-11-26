# Copy-Paste SQL Examples (Tested & Correct)

**These examples are tested and work correctly. Copy them exactly.**

Use these as templates for your queries. When in doubt, copy the pattern from here.

---

## Quick Reference Table

### Aggregate Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| COUNT | `COUNT(*)` or `COUNT(column)` | Count rows or non-NULL values |
| SUM | `SUM(column)` | Sum of numeric values |
| AVG | `AVG(column)` | Average of numeric values |
| MIN | `MIN(column)` | Minimum value |
| MAX | `MAX(column)` | Maximum value |
| COUNT_DISTINCT | `COUNT(DISTINCT column)` | Count unique values |
| STDDEV | `STDDEV(column)` | Sample standard deviation |
| VARIANCE | `VARIANCE(column)` | Sample variance |
| MEDIAN | `MEDIAN(column)` | Median value |
| LISTAGG | `LISTAGG(column, ',')` | Concatenate values with separator |

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

---

## Simple SELECT Queries

### Basic SELECT with WHERE

```sql
SELECT order_id, customer_id, amount
FROM orders
WHERE amount > 100 LIMIT 10
```

**When to use**: Filtering data, selecting specific columns, limiting results

### SELECT with Multiple Conditions

```sql
SELECT symbol, price, quantity, timestamp
FROM trades
WHERE price
    > 50
  AND quantity >= 100
  AND timestamp >= 1000000
    LIMIT 100
```

**When to use**: Multiple filter conditions with AND/OR

### SELECT with Column Alias

```sql
SELECT order_id    AS id,
       customer_id AS cust_id,
       amount      AS total
FROM orders
WHERE amount > 100
```

**When to use**: Renaming columns in output

---

## Aggregation without Windows

### GROUP BY with COUNT

```sql
SELECT category, COUNT(*) as item_count
FROM products
GROUP BY category
```

**When to use**: Count items per group

### GROUP BY with Multiple Aggregates

```sql
SELECT symbol,
       COUNT(*)      as trade_count,
       AVG(price)    as avg_price,
       MIN(price)    as min_price,
       MAX(price)    as max_price,
       SUM(quantity) as total_qty
FROM trades
GROUP BY symbol
```

**When to use**: Multiple calculations per group

### GROUP BY with HAVING

```sql
SELECT customer_id,
       COUNT(*)    as order_count,
       SUM(amount) as total_spent
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 5
```

**When to use**: Filter groups after aggregation

### GROUP BY with WHERE and HAVING

```sql
SELECT symbol,
       COUNT(*)   as cnt,
       AVG(price) as avg_price
FROM market_data
WHERE price > 0
GROUP BY symbol
HAVING COUNT(*) >= 10
```

**When to use**: Pre-filter rows, then aggregate, then filter groups

---

## Time-Based Windows (WINDOW Clause on SELECT)

### TUMBLING Window - 1 Minute

```sql
SELECT symbol,
       COUNT(*)   as trade_count,
       AVG(price) as avg_price
FROM market_data
GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '1' MINUTE)
```

**When to use**: Fixed-size time windows (1m, 5m, 1h, etc)

### TUMBLING Window - Different Intervals

```sql
-- 5-minute window
SELECT symbol, COUNT(*)
FROM trades
GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '5' MINUTE)

-- 1-hour window
SELECT customer_id, SUM(amount)
FROM orders
GROUP BY customer_id
    WINDOW TUMBLING(INTERVAL '1' HOUR)

-- 30-second window (high-frequency data)
SELECT sensor_id, AVG(temperature)
FROM sensors
GROUP BY sensor_id
    WINDOW TUMBLING(INTERVAL '30' SECOND)
```

**When to use**: Adjust interval for your data frequency

### SLIDING Window - 10m Window, 2m Advance

```sql
SELECT symbol,
       COUNT(*)   as trade_count,
       AVG(price) as moving_avg
FROM market_data
GROUP BY symbol
    WINDOW SLIDING(INTERVAL '10' MINUTE, INTERVAL '2' MINUTE)
```

**When to use**: Overlapping windows with smooth progression

### SESSION Window - 30 Second Gap

```sql
SELECT user_id,
       COUNT(*)             as actions_in_session,
       COUNT(DISTINCT page) as pages_visited
FROM user_events
GROUP BY user_id
    WINDOW SESSION(INTERVAL '30' SECOND)
```

**When to use**: Group events by inactivity (user sessions, trading sessions)

### Time Window with Specific Time Column

```sql
SELECT symbol,
       COUNT(*)   as cnt,
       AVG(price) as avg_price
FROM market_data
GROUP BY symbol
    WINDOW TUMBLING(market_data.trade_time, INTERVAL '5' MINUTE)
```

**When to use**: Specify which timestamp column to use for windowing

---

## ROWS WINDOW (In OVER Clause for Window Functions)

### LAG - Previous Row Value

```sql
SELECT symbol,
       price, timestamp, LAG(price, 1) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
    ) as prev_price
FROM trades
```

**When to use**: Compare current row to previous row (price changes, deltas)

### Moving Average - Last N Rows

```sql
SELECT symbol,
       price, timestamp, AVG (price) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
    ) as moving_avg_100
FROM market_data
```

**When to use**: Calculate moving average over last N rows

### COUNT with ROWS WINDOW

```sql
SELECT trader_id,
       symbol,
       price,
       COUNT(*) OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY trader_id
        ORDER BY timestamp
    ) as trade_count_1000
FROM trades
```

**When to use**: Count rows in last N-row window

### ROWS WINDOW with Window Frame (Last 50 of 100)

```sql
SELECT symbol,
       price,
       SUM(quantity) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
        ROWS BETWEEN 50 PRECEDING AND CURRENT ROW
    ) as qty_last_50
FROM trades
```

**When to use**: Use subset of buffer (last 50 of 100 rows)

### ROWS WINDOW with EMIT CHANGES

```sql
SELECT trader_id,
       symbol,
       COUNT(*) OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY trader_id
        EMIT CHANGES
    ) as running_count
FROM market_data
```

**When to use**: Emit result on every new row (streaming aggregation)

### Multiple Window Functions

```sql
SELECT symbol,
       price, timestamp, LAG(price, 1) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
    ) as prev_price, AVG (price) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
    ) as moving_avg, ROW_NUMBER() OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
    ) as row_num
FROM trades
```

**When to use**: Multiple analyses on same data window

---

## Complex Queries (GROUP BY + WINDOW)

### GROUP BY with Time Window

```sql
SELECT trader_id,
       symbol,
       COUNT(*)      as trade_count,
       AVG(price)    as avg_price,
       SUM(quantity) as total_qty
FROM market_data
WHERE price > 0
GROUP BY trader_id, symbol
    WINDOW TUMBLING(INTERVAL '1' MINUTE)
HAVING COUNT (*) > 5
```

**When to use**: Group data, then window results, with pre/post filtering

### GROUP BY + ROWS WINDOW (Combined)

```sql
SELECT trader_id,
       symbol,
       COUNT(*) as trade_count,
       AVG(price)  OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY trader_id, symbol
        ORDER BY timestamp
    ) as moving_avg
FROM market_data
GROUP BY trader_id, symbol
    WINDOW TUMBLING(INTERVAL '1' MINUTE)
```

**When to use**: Group with time window AND compute moving window stats

### GROUP BY with ORDER BY and LIMIT

```sql
SELECT symbol,
       COUNT(*)      as trade_count,
       SUM(quantity) as total_qty
FROM trades
WHERE price > 100
GROUP BY symbol
HAVING COUNT(*) >= 10
ORDER BY trade_count DESC LIMIT 100
```

**When to use**: Sort grouped results and limit top results

---

## JOIN Queries

### Simple JOIN

```sql
SELECT o.order_id,
       o.customer_id,
       c.customer_name,
       o.amount
FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 100
```

**When to use**: Combine data from two streams/tables

### JOIN with Aggregation

```sql
SELECT c.customer_id,
       c.customer_name,
       COUNT(*)      as order_count,
       AVG(o.amount) as avg_order
FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 50
GROUP BY c.customer_id, c.customer_name
```

**When to use**: Join then aggregate

---

## Field Naming and Aliases

### Column Aliases (All Valid)

```sql
-- Explicit AS keyword (recommended)
SELECT column_name AS alias_name
FROM table_name

-- Implicit alias (also works)
SELECT column_name alias_name
FROM table_name

-- Multiple aliases
SELECT order_id    AS id,
       customer_id AS cust_id,
       amount      AS total
FROM orders
```

### Table Aliases

```sql
-- Explicit AS
SELECT *
FROM market_data AS m
WHERE m.price > 100

-- Implicit (shorthand)
SELECT *
FROM market_data m
WHERE m.price > 100

-- Qualified column names
SELECT m.symbol,
       m.price,
       m.timestamp
FROM market_data m
WHERE m.price > 50
```

---

## System Columns (Kafka Metadata)

### Using _timestamp, _offset, _partition

```sql
SELECT customer_id,
       amount,
       _timestamp as kafka_time,
       _offset    as kafka_offset,
       _partition as kafka_partition
FROM orders LIMIT 100
```

**When to use**: Access Kafka message metadata

---

## Type Conversion & Null Handling

### CAST - Type Conversion

```sql
-- Cast to INTEGER (truncates decimals)
SELECT order_id,
       CAST(amount AS INTEGER) as amount_int
FROM orders

-- Cast to STRING
SELECT symbol,
       CAST(price AS STRING) as price_str
FROM trades

-- Cast to FLOAT
SELECT product_id,
       CAST(quantity AS FLOAT) as qty_float
FROM inventory

-- Cast to TIMESTAMP (from Unix epoch)
SELECT event_id,
       CAST(epoch_seconds AS TIMESTAMP) as event_time
FROM logs

-- Cast to BOOLEAN
SELECT user_id,
       CAST(is_active AS BOOLEAN) as active
FROM users
```

**When to use**: Convert between types (INTEGER, FLOAT, STRING, BOOLEAN, TIMESTAMP, DATE, DECIMAL)

**Supported types**: INTEGER, FLOAT, DOUBLE, STRING, VARCHAR, BOOLEAN, DATE, TIMESTAMP, DECIMAL

### COALESCE - First Non-NULL Value

```sql
-- Use fallback values for NULL
SELECT customer_id,
       COALESCE(nickname, first_name, 'Unknown') as display_name
FROM customers

-- Default for missing values
SELECT order_id,
       COALESCE(discount, 0) as discount_applied
FROM orders

-- Multiple fallbacks
SELECT product_id,
       COALESCE(sale_price, regular_price, list_price) as final_price
FROM products
```

**When to use**: Provide default values when data might be NULL

### NULLIF - Return NULL on Match

```sql
-- Convert empty strings to NULL
SELECT customer_id,
       NULLIF(email, '') as email_or_null
FROM customers

-- Convert zero to NULL (avoid division by zero)
SELECT product_id,
       total_revenue / NULLIF(quantity_sold, 0) as avg_price
FROM sales

-- Convert placeholder values to NULL
SELECT sensor_id,
       NULLIF(reading, -999) as valid_reading
FROM sensor_data
```

**When to use**: Convert specific values to NULL (empty strings, sentinel values, zeros)

---

## Conditional Logic (CASE WHEN)

### Basic CASE Expression

```sql
-- Simple categorization
SELECT order_id,
       amount,
       CASE
           WHEN amount > 1000 THEN 'large'
           WHEN amount > 100 THEN 'medium'
           ELSE 'small'
       END as order_size
FROM orders
```

**When to use**: Categorize data based on conditions

### CASE with Multiple Conditions

```sql
-- Complex categorization with AND/OR
SELECT user_id,
       CASE
           WHEN age >= 65 THEN 'senior'
           WHEN age >= 18 AND age < 65 THEN 'adult'
           WHEN age >= 13 THEN 'teen'
           ELSE 'child'
       END as age_group
FROM users
```

### CASE in Aggregation (Conditional Counting)

```sql
-- Count by condition
SELECT COUNT(CASE WHEN status = 'active' THEN 1 END) as active_count,
       COUNT(CASE WHEN status = 'inactive' THEN 1 END) as inactive_count,
       COUNT(*) as total_count
FROM users

-- Sum by condition
SELECT SUM(CASE WHEN priority = 'high' THEN amount ELSE 0 END) as high_priority_total,
       SUM(CASE WHEN priority = 'low' THEN amount ELSE 0 END) as low_priority_total
FROM orders
```

**When to use**: Pivot data, count/sum conditionally within a single query

### CASE Without ELSE (Returns NULL)

```sql
-- NULL when no condition matches
SELECT order_id,
       CASE
           WHEN status = 'shipped' THEN shipped_date
           WHEN status = 'delivered' THEN delivered_date
       END as relevant_date
FROM orders
```

**When to use**: When you want NULL for unmatched cases

---

## String Functions

### UPPER and LOWER

```sql
SELECT customer_id,
       UPPER(name) as name_upper,
       LOWER(email) as email_lower
FROM customers
```

**When to use**: Normalize text for comparison or display

### CONCAT - Concatenate Strings

```sql
-- Join multiple strings
SELECT CONCAT(first_name, ' ', last_name) as full_name
FROM customers

-- Build formatted output
SELECT CONCAT(city, ', ', state, ' ', zip) as address
FROM locations
```

**When to use**: Combine text fields

### SUBSTRING - Extract Part of String

```sql
-- Extract first 3 characters (area code)
SELECT phone,
       SUBSTRING(phone, 1, 3) as area_code
FROM contacts

-- Extract middle portion
SELECT sku,
       SUBSTRING(sku, 4, 6) as category_code
FROM products
```

**When to use**: Extract portions of text

### TRIM, LTRIM, RTRIM - Remove Whitespace

```sql
SELECT TRIM(user_input) as cleaned,
       LTRIM(user_input) as left_trimmed,
       RTRIM(user_input) as right_trimmed
FROM form_data
```

**When to use**: Clean user input or imported data

### REPLACE - Replace Text

```sql
SELECT description,
       REPLACE(description, 'old_term', 'new_term') as updated
FROM products

-- Remove characters
SELECT phone,
       REPLACE(REPLACE(phone, '-', ''), ' ', '') as digits_only
FROM contacts
```

**When to use**: Find and replace text patterns

### LENGTH - String Length

```sql
SELECT name,
       LENGTH(name) as name_length
FROM customers
WHERE LENGTH(name) > 50
```

**When to use**: Validate or filter by string length

### LEFT and RIGHT - Extract from Ends

```sql
SELECT sku,
       LEFT(sku, 3) as prefix,
       RIGHT(sku, 4) as suffix
FROM products
```

**When to use**: Extract fixed-length prefixes or suffixes

---

## Math Functions

### ABS - Absolute Value

```sql
SELECT account_id,
       balance,
       ABS(balance) as absolute_balance
FROM accounts
WHERE ABS(balance) > 1000
```

**When to use**: Get magnitude regardless of sign

### ROUND - Round to Decimal Places

```sql
-- Round to 2 decimal places
SELECT product_id,
       price,
       ROUND(price, 2) as rounded_price
FROM products

-- Round to nearest integer
SELECT sensor_id,
       ROUND(temperature, 0) as temp_rounded
FROM sensors
```

**When to use**: Control decimal precision in output

### FLOOR and CEIL

```sql
SELECT price,
       FLOOR(price) as floor_price,
       CEIL(price) as ceil_price
FROM products
-- price=10.7 → floor=10, ceil=11
```

**When to use**: Round down (FLOOR) or up (CEIL) to integers

### SQRT and POWER

```sql
SELECT value,
       SQRT(value) as square_root,
       POWER(value, 2) as squared,
       POWER(value, 0.5) as also_sqrt
FROM numbers
```

**When to use**: Mathematical calculations

### MOD - Modulo (Remainder)

```sql
-- Check if even/odd
SELECT id,
       MOD(id, 2) as is_odd
FROM records

-- Distribute to buckets
SELECT user_id,
       MOD(user_id, 10) as bucket
FROM users
```

**When to use**: Remainder after division, bucketing

### GREATEST and LEAST

```sql
-- Find max/min across columns
SELECT symbol,
       bid,
       ask,
       GREATEST(bid, ask, last_price) as max_price,
       LEAST(bid, ask, last_price) as min_price
FROM quotes

-- Clamp values to range
SELECT sensor_id,
       GREATEST(0, LEAST(100, reading)) as clamped_reading
FROM sensors
```

**When to use**: Compare values across columns (not rows)

---

## Date/Time Functions

### EXTRACT - Get Date/Time Parts

```sql
-- SQL standard syntax
SELECT order_date,
       EXTRACT(YEAR FROM order_date) as year,
       EXTRACT(MONTH FROM order_date) as month,
       EXTRACT(DAY FROM order_date) as day,
       EXTRACT(HOUR FROM order_date) as hour
FROM orders

-- Get day of week (1=Sunday, 7=Saturday)
SELECT event_time,
       EXTRACT(DOW FROM event_time) as day_of_week
FROM events

-- Get Unix epoch
SELECT timestamp,
       EXTRACT(EPOCH FROM timestamp) as unix_timestamp
FROM logs
```

**EXTRACT parts**: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOW, DOY, QUARTER, WEEK, EPOCH, MILLISECOND

### NOW and CURRENT_TIMESTAMP

```sql
-- Add processing timestamp
SELECT order_id,
       amount,
       NOW() as processed_at
FROM orders

-- Calculate age
SELECT event_id,
       event_time,
       EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM event_time) as age_seconds
FROM events
```

**When to use**: Current timestamp for auditing or calculations

### DATEDIFF - Date Difference

```sql
-- Days between dates
SELECT order_id,
       DATEDIFF('days', order_date, ship_date) as days_to_ship
FROM orders

-- Hours between timestamps
SELECT session_id,
       DATEDIFF('hours', start_time, end_time) as session_hours
FROM sessions
```

**Units**: years, months, weeks, days, hours, minutes, seconds

### FROM_UNIXTIME - Convert Unix Timestamp

```sql
SELECT event_id,
       epoch_seconds,
       FROM_UNIXTIME(epoch_seconds) as event_time
FROM raw_events
```

**When to use**: Convert integer timestamps to datetime

### UNIX_TIMESTAMP - Convert to Epoch

```sql
SELECT order_id,
       order_date,
       UNIX_TIMESTAMP(order_date) as epoch
FROM orders
```

**When to use**: Convert datetime to integer for calculations

---

## Additional Window Functions

### LEAD - Next Row Value

```sql
SELECT symbol,
       price,
       timestamp,
       LEAD(price, 1) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY timestamp
       ) as next_price
FROM trades
```

**When to use**: Look ahead to next row (opposite of LAG)

### RANK and DENSE_RANK

```sql
-- RANK: Gaps after ties (1, 2, 2, 4)
SELECT symbol,
       price,
       RANK() OVER (
           ROWS WINDOW BUFFER 100 ROWS
           ORDER BY price DESC
       ) as price_rank
FROM trades

-- DENSE_RANK: No gaps (1, 2, 2, 3)
SELECT symbol,
       price,
       DENSE_RANK() OVER (
           ROWS WINDOW BUFFER 100 ROWS
           ORDER BY price DESC
       ) as dense_price_rank
FROM trades
```

**When to use**: Rank rows within partition

### FIRST_VALUE and LAST_VALUE

```sql
SELECT symbol,
       price,
       timestamp,
       FIRST_VALUE(price) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY timestamp
       ) as opening_price,
       LAST_VALUE(price) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY timestamp
       ) as latest_price
FROM trades
```

**When to use**: Get first/last values in window partition

### NTILE - Divide into Buckets

```sql
-- Divide into quartiles
SELECT student_id,
       score,
       NTILE(4) OVER (
           ROWS WINDOW BUFFER 1000 ROWS
           ORDER BY score DESC
       ) as quartile
FROM exam_results
-- Returns 1, 2, 3, or 4
```

**When to use**: Divide data into equal-sized groups

### NTH_VALUE - Get Nth Value

```sql
SELECT symbol,
       price,
       NTH_VALUE(price, 2) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY timestamp
       ) as second_price
FROM trades
```

**When to use**: Get specific position in window

---

## Statistical Functions

### STDDEV and VARIANCE

```sql
SELECT symbol,
       COUNT(*) as trade_count,
       AVG(price) as avg_price,
       STDDEV(price) as price_stddev,
       VARIANCE(price) as price_variance
FROM trades
GROUP BY symbol
```

**When to use**: Measure spread/volatility

### STDDEV_POP and VAR_POP (Population)

```sql
SELECT sensor_id,
       STDDEV_POP(reading) as pop_stddev,
       VAR_POP(reading) as pop_variance
FROM sensor_data
GROUP BY sensor_id
```

**When to use**: When you have entire population (not a sample)

### MEDIAN

```sql
SELECT category,
       MEDIAN(price) as median_price
FROM products
GROUP BY category
```

**When to use**: Find middle value (more robust than AVG for skewed data)

### PERCENTILE_CONT and PERCENTILE_DISC

```sql
-- Continuous percentile (interpolated)
SELECT symbol,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price) as p95
FROM trades
GROUP BY symbol

-- Discrete percentile (actual value)
SELECT symbol,
       PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY price) as median_actual
FROM trades
GROUP BY symbol
```

**When to use**: Calculate percentiles (p50=median, p95, p99, etc.)

### CORR - Correlation

```sql
SELECT symbol,
       CORR(price, volume) as price_volume_correlation
FROM trades
GROUP BY symbol
-- Returns -1 to 1 (negative, no, or positive correlation)
```

**When to use**: Measure relationship between two variables

### COVAR_SAMP and COVAR_POP

```sql
SELECT symbol,
       COVAR_SAMP(price, volume) as sample_covariance,
       COVAR_POP(price, volume) as population_covariance
FROM trades
GROUP BY symbol
```

**When to use**: Measure how two variables change together

---

## JSON Functions

### JSON_EXTRACT - Extract JSON Value

```sql
SELECT event_id,
       JSON_EXTRACT(payload, '$.user.id') as user_id,
       JSON_EXTRACT(payload, '$.action') as action
FROM events

-- Nested extraction
SELECT order_id,
       JSON_EXTRACT(metadata, '$.shipping.address.city') as city
FROM orders
```

**When to use**: Extract values from JSON columns

### JSON_VALUE - Extract Scalar Value

```sql
SELECT config_id,
       JSON_VALUE(settings, '$.timeout') as timeout,
       JSON_VALUE(settings, '$.enabled') as enabled
FROM configurations
```

**When to use**: Extract simple values (strings, numbers) from JSON

---

## Kafka Header Functions

### HEADER - Get Header Value

```sql
SELECT order_id,
       amount,
       HEADER('trace-id') as trace_id,
       HEADER('correlation-id') as correlation_id
FROM orders
```

**When to use**: Access Kafka message headers for tracing/correlation

### HAS_HEADER - Check Header Exists

```sql
-- Filter by header presence
SELECT *
FROM events
WHERE HAS_HEADER('priority')

-- Conditional logic based on headers
SELECT order_id,
       CASE
           WHEN HAS_HEADER('express') THEN 'express'
           ELSE 'standard'
       END as shipping_type
FROM orders
```

**When to use**: Check if a header is present before using it

### HEADER_KEYS - List All Headers

```sql
SELECT message_id,
       HEADER_KEYS() as all_headers
FROM messages
-- Returns comma-separated list: "trace-id,correlation-id,source"
```

**When to use**: Debug/inspect available headers

---

## Window Boundaries (System Columns for Time Windows)

### Using _window_start and _window_end

```sql
SELECT symbol,
       COUNT(*) as trade_count,
       AVG(price) as avg_price,
       _window_start as window_start,
       _window_end as window_end
FROM trades
GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '1' MINUTE)
```

**When to use**: Include window time boundaries in output for debugging or downstream processing

### TUMBLE_START and TUMBLE_END Functions

```sql
SELECT symbol,
       TUMBLE_START(timestamp, INTERVAL '5' MINUTE) as window_start,
       TUMBLE_END(timestamp, INTERVAL '5' MINUTE) as window_end,
       COUNT(*) as cnt
FROM trades
GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '5' MINUTE)
```

**When to use**: Explicitly calculate window boundaries from timestamp

---

## IN and BETWEEN Operators

### IN - Check Membership

```sql
-- Filter by list of values
SELECT *
FROM orders
WHERE status IN ('pending', 'processing', 'shipped')

-- NOT IN for exclusion
SELECT *
FROM products
WHERE category NOT IN ('discontinued', 'archived')
```

**When to use**: Check if value is in a set

### BETWEEN - Range Check

```sql
-- Inclusive range
SELECT *
FROM orders
WHERE amount BETWEEN 100 AND 500

-- Date range
SELECT *
FROM events
WHERE event_date BETWEEN '2024-01-01' AND '2024-12-31'

-- NOT BETWEEN for exclusion
SELECT *
FROM readings
WHERE temperature NOT BETWEEN 60 AND 80
```

**When to use**: Check if value is within a range (inclusive)

---

## Testing These Examples

Each example has been tested and works. If you modify them:

1. ✅ Keep keyword order the same
2. ✅ Keep OVER clause structure for ROWS WINDOW
3. ✅ Keep WINDOW clause placement (after HAVING, before ORDER BY)
4. ✅ Keep FROM required (except SELECT 1 style)

If an example doesn't work after modification:

- Check `docs/sql/PARSER_GRAMMAR.md` for syntax rules
- Run tests: `grep -r "your_pattern" tests/unit/sql/parser/`
- Compare with working example here

---

## Common Mistakes (Don't Do This!)

### ❌ Wrong Window Syntax

```sql
❌
ROWS BUFFER 100 ROWS
❌ ROWS WINDOW BUFFER 100
❌ WINDOW ROWS 100
✅ ROWS WINDOW BUFFER 100 ROWS   (Correct)
```

### ❌ Wrong Clause Order

```sql
❌
SELECT * WHERE
WHERE amount > 100
FROM orders ❌
SELECT *
FROM orders
ORDER BY amount
GROUP BY customer_id ✅
SELECT *
FROM orders
WHERE amount > 100
GROUP BY customer_id
ORDER BY amount
```

### ❌ Missing Parentheses in ROWS WINDOW

```sql
❌
AVG(price) OVER ROWS WINDOW BUFFER 100 ROWS
✅ AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)
```

### ❌ Missing FROM

```sql
❌
SELECT * WHERE amount > 100 ✅
SELECT *
FROM orders
WHERE amount > 100
```

### ❌ Confusing Window Types

```sql
❌ WINDOW
ROWS 100 ROWS               (This isn't valid)
❌ ROWS WINDOW TUMBLING(1m)           (ROWS is count-based, not time)
✅ WINDOW TUMBLING(INTERVAL '
1' MINUTE)  (For time-based)
✅ ROWS WINDOW BUFFER 100 ROWS        (For count-based)
```

---

## How to Extend These Examples

### Change Time Intervals

```sql
-- 1-minute (current)
WINDOW
TUMBLING(INTERVAL '1' MINUTE)

-- To 5 minutes
WINDOW TUMBLING(INTERVAL '5' MINUTE)

-- To 1 hour
WINDOW TUMBLING(INTERVAL '1' HOUR)

-- To 30 seconds
WINDOW TUMBLING(INTERVAL '30' SECOND)
```

### Change Buffer Sizes

```sql
-- 100-row window (current)
ROWS WINDOW BUFFER 100 ROWS

-- To 500 rows
ROWS WINDOW BUFFER 500 ROWS

-- To 1000 rows
ROWS WINDOW BUFFER 1000 ROWS
```

### Change Partition Columns

```sql
-- Single partition column
PARTITION
BY symbol

-- Multiple partition columns
PARTITION BY symbol, trader_id

-- No partition (all rows together)
-- (Just omit PARTITION BY)
```

### Change Aggregates

```sql
-- Count
COUNT(*) as cnt

-- Average
AVG(price) as avg_price

-- Sum
SUM(quantity) as total_qty

-- Min/Max
MIN(price) as min_price, MAX(price) as max_price

-- Multiple
COUNT(*) as cnt, AVG(price) as avg, SUM(qty) as total
```

