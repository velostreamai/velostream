# Time-Based Window Analysis

Learn how to analyze streaming data using time windows (TUMBLING, SLIDING, SESSION) and window functions for temporal patterns and trends.

## Time Window Types

### TUMBLING Windows - Non-Overlapping Fixed Intervals

#### Simple TUMBLING Syntax
```sql
-- 5-minute non-overlapping windows
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY customer_id
WINDOW TUMBLING(5m);  -- Simple duration syntax
```

#### Advanced TUMBLING Syntax (NEW)
```sql
-- Complex TUMBLING with explicit time column and INTERVAL syntax
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM orders
GROUP BY customer_id
WINDOW TUMBLING (event_time, INTERVAL '5' MINUTE);  -- Explicit time column

-- Table aliases supported in complex syntax
SELECT
    p.trader_id,
    m.symbol,
    COUNT(*) as trade_count,
    AVG(m.price) as avg_price
FROM market_data m
JOIN positions p ON m.symbol = p.symbol
GROUP BY p.trader_id, m.symbol
WINDOW TUMBLING (m.event_time, INTERVAL '1' HOUR);  -- Table aliases work
```

**Syntax Variations:**
- **Simple**: `TUMBLING(duration)` - e.g., `TUMBLING(1h)`, `TUMBLING(30s)`
- **Complex**: `TUMBLING (time_column, INTERVAL duration)` - e.g., `TUMBLING (event_time, INTERVAL '15' MINUTE)`
- **INTERVAL Units**: SECOND, MINUTE, HOUR, DAY (both singular and plural forms)

**Use cases:**
- **Hourly sales reports**: `WINDOW TUMBLING(1h)` or `WINDOW TUMBLING (order_time, INTERVAL '1' HOUR)`
- **Daily metrics**: `WINDOW TUMBLING(1d)` or `WINDOW TUMBLING (date_column, INTERVAL '1' DAY)`
- **Real-time monitoring**: `WINDOW TUMBLING(30s)` or `WINDOW TUMBLING (timestamp, INTERVAL '30' SECOND)`
- **Financial analytics**: `WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)` for minute-by-minute analysis

### SLIDING Windows - Overlapping Intervals

```sql
-- 10-minute window, updating every 5 minutes
SELECT
    customer_id,
    AVG(amount) as avg_amount_10min
FROM orders
GROUP BY customer_id
WINDOW SLIDING(10m, 5m);
```

**Use cases:**
- **Moving averages**: Smooth out data fluctuations
- **Trend analysis**: Continuous monitoring with overlap
- **Real-time dashboards**: Updated metrics every few minutes

### SESSION Windows - Activity-Based Grouping

```sql
-- Group events within 30-minute activity sessions
SELECT
    customer_id,
    COUNT(*) as session_events
FROM user_events
GROUP BY customer_id
WINDOW SESSION(30m);
```

**Use cases:**
- **User session analysis**: Web activity sessions
- **IoT device clustering**: Sensor activity periods
- **Transaction grouping**: Related payment events

## Duration Units

| Unit | Description | Example |
|------|-------------|---------|
| `ns` | Nanoseconds | `WINDOW TUMBLING(500ns)` |
| `us`, `μs` | Microseconds | `WINDOW TUMBLING(100us)` |
| `ms` | Milliseconds | `WINDOW TUMBLING(250ms)` |
| `s` | Seconds | `WINDOW TUMBLING(30s)` |
| `m` | Minutes | `WINDOW TUMBLING(5m)` |
| `h` | Hours | `WINDOW TUMBLING(2h)` |
| `d` | Days | `WINDOW TUMBLING(1d)` |

## Window Functions with OVER Clauses

### Row Numbering and Ranking

```sql
-- Assign sequence numbers to orders per customer
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_sequence
FROM orders;
```

```sql
-- Rank orders by amount (with gaps for ties)
SELECT
    order_id,
    customer_id,
    amount,
    RANK() OVER (ORDER BY amount DESC) as amount_rank,
    DENSE_RANK() OVER (ORDER BY amount DESC) as amount_dense_rank
FROM orders;
```

### LAG and LEAD - Access Adjacent Rows

```sql
-- Compare with previous order
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_amount,
    LAG(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_order_date
FROM orders;
```

```sql
-- Look ahead to next order
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as next_amount,
    LEAD(order_date, 2) OVER (PARTITION BY customer_id ORDER BY order_date) as next_next_order_date
FROM orders;
```

## Window Frames - Specify Calculation Range

### Running Totals and Cumulative Calculations

```sql
-- Running total per customer
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total
FROM orders;
```

### Moving Averages

```sql
-- 3-order moving average
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3_orders
FROM orders;
```

### Forward-Looking Calculations

```sql
-- Total of next 3 orders
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) as next_3_orders_total
FROM orders;
```

## Range-Based Windows

Use `RANGE BETWEEN` for value-based ranges instead of row counts:

```sql
-- Count orders up to current date
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    COUNT(*) OVER (
        ORDER BY order_date
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as orders_up_to_date,
    SUM(amount) OVER (
        ORDER BY order_date
        RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) as remaining_revenue
FROM orders;
```

## Real-World Examples

### 1. Sales Performance Dashboard

```sql
-- Hourly sales with running totals and moving averages
SELECT
    DATE_FORMAT(_timestamp, '%Y-%m-%d %H:00:00') as hour,
    COUNT(*) as hourly_orders,
    SUM(amount) as hourly_revenue,
    SUM(COUNT(*)) OVER (
        ORDER BY DATE_FORMAT(_timestamp, '%Y-%m-%d %H:00:00')
        ROWS UNBOUNDED PRECEDING
    ) as cumulative_orders,
    AVG(SUM(amount)) OVER (
        ORDER BY DATE_FORMAT(_timestamp, '%Y-%m-%d %H:00:00')
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7hr_avg_revenue
FROM orders
GROUP BY DATE_FORMAT(_timestamp, '%Y-%m-%d %H:00:00')
WINDOW TUMBLING(1h);
```

### 2. User Behavior Session Analysis

```sql
-- Analyze user session patterns
SELECT
    user_id,
    session_start,
    session_end,
    event_count,
    session_duration_minutes,
    CASE
        WHEN session_duration_minutes > 30 THEN 'LONG_SESSION'
        WHEN session_duration_minutes > 10 THEN 'MEDIUM_SESSION'
        WHEN session_duration_minutes > 2 THEN 'SHORT_SESSION'
        ELSE 'VERY_SHORT_SESSION'
    END as session_type,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start) as session_sequence
FROM (
    SELECT
        user_id,
        MIN(_timestamp) as session_start,
        MAX(_timestamp) as session_end,
        COUNT(*) as event_count,
        EXTRACT('EPOCH', MAX(_timestamp) - MIN(_timestamp)) / 60 as session_duration_minutes
    FROM user_events
    GROUP BY user_id
    WINDOW SESSION(10m)
) sessions;
```

### 3. Financial Moving Averages

```sql
-- Stock price analysis with multiple moving averages
SELECT
    symbol,
    trade_date,
    close_price,
    AVG(close_price) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as ma_5_day,
    AVG(close_price) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as ma_20_day,
    AVG(close_price) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) as ma_50_day
FROM stock_prices
ORDER BY symbol, trade_date;
```

### 4. IoT Sensor Monitoring

```sql
-- Real-time sensor anomaly detection
SELECT
    device_id,
    sensor_reading,
    reading_timestamp,
    AVG(sensor_reading) OVER (
        PARTITION BY device_id
        ORDER BY reading_timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as avg_last_10_readings,
    CASE
        WHEN ABS(sensor_reading - AVG(sensor_reading) OVER (
            PARTITION BY device_id
            ORDER BY reading_timestamp
            ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING
        )) > 2 * STDDEV(sensor_reading) OVER (
            PARTITION BY device_id
            ORDER BY reading_timestamp
            ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING
        ) THEN 'ANOMALY'
        ELSE 'NORMAL'
    END as anomaly_status
FROM sensor_readings
WINDOW TUMBLING(1m);
```

### 5. E-commerce Conversion Funnel

```sql
-- Track user journey through purchase funnel
SELECT
    user_id,
    event_type,
    event_timestamp,
    LAG(event_type, 1) OVER (PARTITION BY user_id ORDER BY event_timestamp) as prev_event,
    LAG(event_timestamp, 1) OVER (PARTITION BY user_id ORDER BY event_timestamp) as prev_timestamp,
    CASE
        WHEN LAG(event_type, 1) OVER (PARTITION BY user_id ORDER BY event_timestamp) = 'page_view'
         AND event_type = 'add_to_cart'
        THEN 'VIEW_TO_CART'
        WHEN LAG(event_type, 1) OVER (PARTITION BY user_id ORDER BY event_timestamp) = 'add_to_cart'
         AND event_type = 'purchase'
        THEN 'CART_TO_PURCHASE'
        ELSE 'OTHER_TRANSITION'
    END as funnel_step
FROM user_events
WHERE event_type IN ('page_view', 'add_to_cart', 'purchase')
WINDOW SESSION(30m);
```

## Window Frame Types Quick Reference

| Frame Type | Description | Example |
|------------|-------------|---------|
| `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | All previous rows + current | Running totals |
| `ROWS BETWEEN N PRECEDING AND CURRENT ROW` | Last N rows + current | Moving averages |
| `ROWS BETWEEN CURRENT ROW AND N FOLLOWING` | Current + next N rows | Forward-looking sums |
| `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | All values ≤ current | Cumulative by value |

## Performance Tips

### 1. Choose Appropriate Window Sizes
- **Small windows**: Lower memory usage, more frequent updates
- **Large windows**: Higher memory usage, smoother trends
- **Match business requirements**: Don't make windows larger than needed

### 2. Window Function Optimization
- Use `PARTITION BY` to reduce calculation scope
- `ORDER BY` columns should be indexed when possible
- Limit window frame size with specific `ROWS BETWEEN` clauses

### 3. Session Window Guidelines
- Choose gap duration based on actual user behavior
- Too small: Many short sessions
- Too large: Sessions merge incorrectly

## Common Patterns

### Trend Detection
```sql
-- Detect increasing/decreasing trends
SELECT *,
    CASE
        WHEN current_value > lag_value AND lag_value > lag_value_2 THEN 'INCREASING'
        WHEN current_value < lag_value AND lag_value < lag_value_2 THEN 'DECREASING'
        ELSE 'STABLE'
    END as trend
FROM (
    SELECT
        metric_value as current_value,
        LAG(metric_value, 1) OVER (ORDER BY timestamp) as lag_value,
        LAG(metric_value, 2) OVER (ORDER BY timestamp) as lag_value_2
    FROM metrics
);
```

### Percentage Change
```sql
-- Calculate percentage change from previous period
SELECT
    period,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY period) as prev_revenue,
    (revenue - LAG(revenue, 1) OVER (ORDER BY period)) * 100.0 /
        LAG(revenue, 1) OVER (ORDER BY period) as pct_change
FROM monthly_revenue;
```

## Next Steps

- [Aggregate windowed data](aggregate-data.md) - Combine with GROUP BY
- [Join windowed streams](join-streams.md) - Correlate multiple data sources
- [Filter window results](filter-data.md) - WHERE conditions with windows