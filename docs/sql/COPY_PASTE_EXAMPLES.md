# Copy-Paste SQL Examples (Tested & Correct)

**These examples are tested and work correctly. Copy them exactly.**

Use these as templates for your queries. When in doubt, copy the pattern from here.

---

## Simple SELECT Queries

### Basic SELECT with WHERE

```sql
SELECT order_id, customer_id, amount
FROM orders
WHERE amount > 100
LIMIT 10
```

**When to use**: Filtering data, selecting specific columns, limiting results

### SELECT with Multiple Conditions

```sql
SELECT symbol, price, quantity, timestamp
FROM trades
WHERE price > 50
  AND quantity >= 100
  AND timestamp >= 1000000
LIMIT 100
```

**When to use**: Multiple filter conditions with AND/OR

### SELECT with Column Alias

```sql
SELECT
    order_id AS id,
    customer_id AS cust_id,
    amount AS total
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
SELECT
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    SUM(quantity) as total_qty
FROM trades
GROUP BY symbol
```

**When to use**: Multiple calculations per group

### GROUP BY with HAVING

```sql
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 5
```

**When to use**: Filter groups after aggregation

### GROUP BY with WHERE and HAVING

```sql
SELECT
    symbol,
    COUNT(*) as cnt,
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
SELECT
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price
FROM market_data
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
```

**When to use**: Fixed-size time windows (1m, 5m, 1h, etc)

### TUMBLING Window - Different Intervals

```sql
-- 5-minute window
SELECT symbol, COUNT(*) FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)

-- 1-hour window
SELECT customer_id, SUM(amount) FROM orders
GROUP BY customer_id
WINDOW TUMBLING(INTERVAL '1' HOUR)

-- 30-second window (high-frequency data)
SELECT sensor_id, AVG(temperature) FROM sensors
GROUP BY sensor_id
WINDOW TUMBLING(INTERVAL '30' SECOND)
```

**When to use**: Adjust interval for your data frequency

### SLIDING Window - 10m Window, 2m Advance

```sql
SELECT
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as moving_avg
FROM market_data
GROUP BY symbol
WINDOW SLIDING(INTERVAL '10' MINUTE, INTERVAL '2' MINUTE)
```

**When to use**: Overlapping windows with smooth progression

### SESSION Window - 30 Second Gap

```sql
SELECT
    user_id,
    COUNT(*) as actions_in_session,
    COUNT(DISTINCT page) as pages_visited
FROM user_events
GROUP BY user_id
WINDOW SESSION(INTERVAL '30' SECOND)
```

**When to use**: Group events by inactivity (user sessions, trading sessions)

### Time Window with Specific Time Column

```sql
SELECT
    symbol,
    COUNT(*) as cnt,
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
SELECT
    symbol,
    price,
    timestamp,
    LAG(price, 1) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
    ) as prev_price
FROM trades
```

**When to use**: Compare current row to previous row (price changes, deltas)

### Moving Average - Last N Rows

```sql
SELECT
    symbol,
    price,
    timestamp,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
    ) as moving_avg_100
FROM market_data
```

**When to use**: Calculate moving average over last N rows

### COUNT with ROWS WINDOW

```sql
SELECT
    trader_id,
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
SELECT
    symbol,
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
SELECT
    trader_id,
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
SELECT
    symbol,
    price,
    timestamp,
    LAG(price, 1) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
    ) as prev_price,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
    ) as moving_avg,
    ROW_NUMBER() OVER (
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
SELECT
    trader_id,
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    SUM(quantity) as total_qty
FROM market_data
WHERE price > 0
GROUP BY trader_id, symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5
```

**When to use**: Group data, then window results, with pre/post filtering

### GROUP BY + ROWS WINDOW (Combined)

```sql
SELECT
    trader_id,
    symbol,
    COUNT(*) as trade_count,
    AVG(price) OVER (
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
SELECT
    symbol,
    COUNT(*) as trade_count,
    SUM(quantity) as total_qty
FROM trades
WHERE price > 100
GROUP BY symbol
HAVING COUNT(*) >= 10
ORDER BY trade_count DESC
LIMIT 100
```

**When to use**: Sort grouped results and limit top results

---

## JOIN Queries

### Simple JOIN

```sql
SELECT
    o.order_id,
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
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(*) as order_count,
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
SELECT column_name AS alias_name FROM table_name

-- Implicit alias (also works)
SELECT column_name alias_name FROM table_name

-- Multiple aliases
SELECT
    order_id AS id,
    customer_id AS cust_id,
    amount AS total
FROM orders
```

### Table Aliases

```sql
-- Explicit AS
SELECT * FROM market_data AS m WHERE m.price > 100

-- Implicit (shorthand)
SELECT * FROM market_data m WHERE m.price > 100

-- Qualified column names
SELECT
    m.symbol,
    m.price,
    m.timestamp
FROM market_data m
WHERE m.price > 50
```

---

## System Columns (Kafka Metadata)

### Using _timestamp, _offset, _partition

```sql
SELECT
    customer_id,
    amount,
    _timestamp as kafka_time,
    _offset as kafka_offset,
    _partition as kafka_partition
FROM orders
LIMIT 100
```

**When to use**: Access Kafka message metadata

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
❌ ROWS BUFFER 100 ROWS
❌ ROWS WINDOW BUFFER 100
❌ WINDOW ROWS 100
✅ ROWS WINDOW BUFFER 100 ROWS   (Correct)
```

### ❌ Wrong Clause Order

```sql
❌ SELECT * WHERE WHERE amount > 100 FROM orders
❌ SELECT * FROM orders ORDER BY amount GROUP BY customer_id
✅ SELECT * FROM orders WHERE amount > 100 GROUP BY customer_id ORDER BY amount
```

### ❌ Missing Parentheses in ROWS WINDOW

```sql
❌ AVG(price) OVER ROWS WINDOW BUFFER 100 ROWS
✅ AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)
```

### ❌ Missing FROM

```sql
❌ SELECT * WHERE amount > 100
✅ SELECT * FROM orders WHERE amount > 100
```

### ❌ Confusing Window Types

```sql
❌ WINDOW ROWS 100 ROWS               (This isn't valid)
❌ ROWS WINDOW TUMBLING(1m)           (ROWS is count-based, not time)
✅ WINDOW TUMBLING(INTERVAL '1' MINUTE)  (For time-based)
✅ ROWS WINDOW BUFFER 100 ROWS        (For count-based)
```

---

## How to Extend These Examples

### Change Time Intervals

```sql
-- 1-minute (current)
WINDOW TUMBLING(INTERVAL '1' MINUTE)

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
PARTITION BY symbol

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

