# Date/Time Functions

Complete reference for date and time manipulation functions in VeloStream. Use these functions to process timestamps, extract date components, and perform temporal calculations.

## Current Time Functions

### NOW() and CURRENT_TIMESTAMP

```sql
-- Get current timestamp
SELECT
    order_id,
    NOW() as processed_at,
    CURRENT_TIMESTAMP as current_ts
FROM orders;

-- Calculate processing delays
SELECT
    order_id,
    order_timestamp,
    NOW() as current_time,
    NOW() - order_timestamp as processing_delay
FROM orders
WHERE status = 'pending';
```

## Date Formatting

### DATE_FORMAT - Format Dates as Strings

```sql
-- Format timestamps for display
SELECT
    order_id,
    DATE_FORMAT(_timestamp, '%Y-%m-%d') as order_date,
    DATE_FORMAT(_timestamp, '%Y-%m-%d %H:%M:%S') as order_datetime,
    DATE_FORMAT(_timestamp, '%a, %b %d, %Y') as friendly_date
FROM orders;

-- Group by formatted dates
SELECT
    DATE_FORMAT(_timestamp, '%Y-%m') as month,
    COUNT(*) as monthly_orders
FROM orders
GROUP BY DATE_FORMAT(_timestamp, '%Y-%m')
ORDER BY month;
```

**Common format specifiers:**
- `%Y` - 4-digit year (2024)
- `%y` - 2-digit year (24)
- `%m` - Month number (01-12)
- `%d` - Day of month (01-31)
- `%H` - Hour 24-format (00-23)
- `%h` - Hour 12-format (01-12)
- `%i` - Minutes (00-59)
- `%s` - Seconds (00-59)
- `%a` - Abbreviated weekday (Mon)
- `%b` - Abbreviated month (Jan)

## Date Component Extraction

### EXTRACT - Extract Date Parts

```sql
-- Extract various date components
SELECT
    order_id,
    _timestamp,
    EXTRACT('YEAR', _timestamp) as order_year,
    EXTRACT('MONTH', _timestamp) as order_month,
    EXTRACT('DAY', _timestamp) as order_day,
    EXTRACT('HOUR', _timestamp) as order_hour,
    EXTRACT('DOW', _timestamp) as day_of_week,  -- 0=Sunday, 1=Monday, etc.
    EXTRACT('DOY', _timestamp) as day_of_year,  -- 1-366
    EXTRACT('WEEK', _timestamp) as week_of_year -- 1-53
FROM orders;

-- Business hour analysis
SELECT
    EXTRACT('HOUR', _timestamp) as hour,
    COUNT(*) as order_count
FROM orders
GROUP BY EXTRACT('HOUR', _timestamp)
ORDER BY hour;
```

### Convenience Functions - YEAR, MONTH, DAY

```sql
-- Simplified date part extraction
SELECT
    order_id,
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    DAY(order_date) as day
FROM orders;

-- Monthly aggregation
SELECT
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    COUNT(*) as monthly_orders
FROM orders
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;
```

## Date Arithmetic

### DATEDIFF - Calculate Date Differences

```sql
-- Calculate time since order
SELECT
    order_id,
    order_timestamp,
    DATEDIFF('seconds', order_timestamp, NOW()) as seconds_since_order,
    DATEDIFF('minutes', order_timestamp, NOW()) as minutes_since_order,
    DATEDIFF('hours', order_timestamp, NOW()) as hours_since_order,
    DATEDIFF('days', order_timestamp, NOW()) as days_since_order
FROM orders;

-- Customer lifecycle analysis
SELECT
    customer_id,
    MIN(order_date) as first_order,
    MAX(order_date) as last_order,
    DATEDIFF('days', MIN(order_date), MAX(order_date)) as customer_lifespan_days
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

### INTERVAL Arithmetic

```sql
-- Add/subtract time intervals
SELECT
    order_id,
    order_date,
    order_date + INTERVAL '30' DAYS as estimated_delivery,
    order_date - INTERVAL '1' HOUR as one_hour_before,
    NOW() - INTERVAL '24' HOURS as yesterday
FROM orders;

-- Find recent orders
SELECT *
FROM orders
WHERE order_timestamp > NOW() - INTERVAL '1' WEEK;

-- Business day calculations
SELECT
    order_id,
    order_date,
    CASE EXTRACT('DOW', order_date)
        WHEN 1 THEN order_date + INTERVAL '5' DAYS  -- Monday + 5 = Saturday
        WHEN 2 THEN order_date + INTERVAL '4' DAYS  -- Tuesday + 4 = Saturday
        WHEN 3 THEN order_date + INTERVAL '3' DAYS  -- Wednesday + 3 = Saturday
        WHEN 4 THEN order_date + INTERVAL '2' DAYS  -- Thursday + 2 = Saturday
        WHEN 5 THEN order_date + INTERVAL '1' DAYS  -- Friday + 1 = Saturday
        ELSE order_date + INTERVAL '2' DAYS         -- Weekend -> Monday
    END as next_business_day_end
FROM orders;
```

## Time Zone Handling

### TIMESTAMP and UTC Conversion

```sql
-- Work with timestamps
SELECT
    event_id,
    TIMESTAMP() as processing_timestamp,
    event_timestamp,
    DATEDIFF('milliseconds', event_timestamp, TIMESTAMP()) as processing_lag_ms
FROM events;

-- Convert between time zones (conceptual - depends on system configuration)
SELECT
    order_id,
    order_timestamp as utc_time,
    -- Note: Actual timezone conversion syntax may vary
    order_timestamp + INTERVAL '8' HOURS as pst_time,
    order_timestamp + INTERVAL '5' HOURS as est_time
FROM orders;
```

## Real-World Examples

### E-commerce Order Analysis

```sql
-- Comprehensive order timing analysis
SELECT
    order_id,
    customer_id,
    order_timestamp,
    DATE_FORMAT(order_timestamp, '%Y-%m-%d') as order_date,
    EXTRACT('DOW', order_timestamp) as day_of_week,
    EXTRACT('HOUR', order_timestamp) as hour_of_day,
    CASE
        WHEN EXTRACT('DOW', order_timestamp) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    CASE
        WHEN EXTRACT('HOUR', order_timestamp) BETWEEN 9 AND 17 THEN 'Business Hours'
        WHEN EXTRACT('HOUR', order_timestamp) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night/Early Morning'
    END as time_period,
    DATEDIFF('hours', order_timestamp, NOW()) as hours_since_order,
    CASE
        WHEN DATEDIFF('hours', order_timestamp, NOW()) < 24 THEN 'Last 24h'
        WHEN DATEDIFF('days', order_timestamp, NOW()) < 7 THEN 'Last Week'
        WHEN DATEDIFF('days', order_timestamp, NOW()) < 30 THEN 'Last Month'
        ELSE 'Older'
    END as recency_bucket
FROM orders
ORDER BY order_timestamp DESC;
```

### IoT Sensor Data Analysis

```sql
-- Analyze IoT sensor readings over time
SELECT
    device_id,
    sensor_reading,
    reading_timestamp,
    DATEDIFF('hours', last_charge_time, reading_timestamp) as hours_since_charge,
    CASE
        WHEN DATEDIFF('hours', last_charge_time, reading_timestamp) > 24 THEN 'LOW_BATTERY'
        WHEN DATEDIFF('hours', last_charge_time, reading_timestamp) > 12 THEN 'MEDIUM_BATTERY'
        ELSE 'GOOD_BATTERY'
    END as battery_status,
    DATE_FORMAT(reading_timestamp, '%Y-%m-%d %H:00:00') as hourly_bucket
FROM iot_sensors
WHERE reading_timestamp > NOW() - INTERVAL '7' DAYS;
```

### Financial Trading Analysis

```sql
-- Trading session analysis
SELECT
    trade_id,
    symbol,
    trade_timestamp,
    price,
    EXTRACT('HOUR', trade_timestamp) as trade_hour,
    CASE
        WHEN EXTRACT('HOUR', trade_timestamp) BETWEEN 9 AND 16
         AND EXTRACT('DOW', trade_timestamp) BETWEEN 1 AND 5
        THEN 'Market Hours'
        WHEN EXTRACT('DOW', trade_timestamp) IN (0, 6)
        THEN 'Weekend'
        ELSE 'After Hours'
    END as trading_session,
    LAG(trade_timestamp, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp) as prev_trade_time,
    DATEDIFF('seconds',
        LAG(trade_timestamp, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp),
        trade_timestamp
    ) as seconds_since_last_trade
FROM trades
WHERE trade_timestamp > NOW() - INTERVAL '1' DAY;
```

### User Session Analysis

```sql
-- Analyze user session patterns
SELECT
    user_id,
    session_start,
    session_end,
    DATEDIFF('minutes', session_start, session_end) as session_duration_minutes,
    DATE_FORMAT(session_start, '%Y-%m-%d') as session_date,
    EXTRACT('DOW', session_start) as start_day_of_week,
    EXTRACT('HOUR', session_start) as start_hour,
    CASE
        WHEN EXTRACT('HOUR', session_start) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT('HOUR', session_start) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT('HOUR', session_start) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END as session_time_period,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start) as session_number,
    LAG(session_end, 1) OVER (PARTITION BY user_id ORDER BY session_start) as prev_session_end,
    DATEDIFF('hours',
        LAG(session_end, 1) OVER (PARTITION BY user_id ORDER BY session_start),
        session_start
    ) as hours_since_last_session
FROM user_sessions
WHERE session_start > NOW() - INTERVAL '30' DAYS;
```

## Performance Tips

### Efficient Date Queries

```sql
-- ✅ Good: Use date ranges instead of functions in WHERE clauses
SELECT * FROM orders
WHERE order_date >= '2024-01-01'
  AND order_date < '2024-02-01';

-- ⚠️ Slower: Functions in WHERE clause prevent index usage
SELECT * FROM orders
WHERE YEAR(order_date) = 2024 AND MONTH(order_date) = 1;

-- ✅ Good: Pre-calculate common date parts for grouping
SELECT
    DATE_FORMAT(order_date, '%Y-%m') as month,
    COUNT(*) as orders
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m');
```

### Time-Based Indexing

```sql
-- Consider creating indexes on date columns used frequently
-- (Conceptual - actual syntax varies by database)
-- CREATE INDEX idx_orders_date ON orders (order_date);
-- CREATE INDEX idx_events_timestamp ON events (event_timestamp);
```

## Common Patterns

### Running Date Calculations

```sql
-- Calculate running totals by date
SELECT
    DATE_FORMAT(order_date, '%Y-%m-%d') as date,
    SUM(amount) as daily_revenue,
    SUM(SUM(amount)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m-%d')) as cumulative_revenue
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m-%d')
ORDER BY date;
```

### Date Bucketing

```sql
-- Group events into time buckets
SELECT
    DATE_FORMAT(event_timestamp, '%Y-%m-%d %H:00:00') as hour_bucket,
    COUNT(*) as events_per_hour
FROM events
WHERE event_timestamp > NOW() - INTERVAL '7' DAYS
GROUP BY DATE_FORMAT(event_timestamp, '%Y-%m-%d %H:00:00')
ORDER BY hour_bucket;
```

### Age Calculations

```sql
-- Calculate ages and durations
SELECT
    customer_id,
    birth_date,
    DATEDIFF('years', birth_date, NOW()) as age,
    CASE
        WHEN DATEDIFF('years', birth_date, NOW()) < 25 THEN 'Young Adult'
        WHEN DATEDIFF('years', birth_date, NOW()) < 45 THEN 'Adult'
        WHEN DATEDIFF('years', birth_date, NOW()) < 65 THEN 'Middle Age'
        ELSE 'Senior'
    END as age_group
FROM customers
WHERE birth_date IS NOT NULL;
```

## Quick Reference

| Function | Purpose | Example |
|----------|---------|--------|
| `NOW()` | Current timestamp | `NOW()` |
| `CURRENT_TIMESTAMP` | Current timestamp (alias) | `CURRENT_TIMESTAMP` |
| `DATE_FORMAT(date, format)` | Format date as string | `DATE_FORMAT(_timestamp, '%Y-%m-%d')` |
| `EXTRACT(part, date)` | Extract date component | `EXTRACT('YEAR', order_date)` |
| `YEAR(date)` | Extract year | `YEAR(order_date)` |
| `MONTH(date)` | Extract month | `MONTH(order_date)` |
| `DAY(date)` | Extract day | `DAY(order_date)` |
| `HOUR(timestamp)` | Extract hour | `HOUR(event_timestamp)` |
| `DATEDIFF(unit, start, end)` | Calculate difference | `DATEDIFF('days', start_date, end_date)` |
| `INTERVAL 'n' UNIT` | Date arithmetic | `NOW() - INTERVAL '1' DAY` |
| `TIMESTAMP()` | Processing timestamp | `TIMESTAMP()` |

**Date difference units:** `seconds`, `minutes`, `hours`, `days`, `weeks`, `months`, `years`

**Interval units:** `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR`

## Next Steps

- [Window analysis](../by-task/window-analysis.md) - Time-based windowing
- [Essential functions](essential.md) - Most commonly used functions
- [Math functions](math.md) - Mathematical calculations