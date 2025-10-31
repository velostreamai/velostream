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

#### Advanced TUMBLING Syntax
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

### ROWS Windows - Row Count-Based Analytic Windows

**Basic ROWS WINDOW Syntax:**
```sql
-- Bounded buffer of last 100 rows
SELECT
    symbol,
    price,
    AVG(price) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
    ) as moving_avg_100
FROM market_data;
```

**With Window Frame (subset of buffer):**
```sql
-- Keep last 1000 rows, but aggregate only last 50
SELECT
    symbol,
    price,
    AVG(price) OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
            ROWS BETWEEN 50 PRECEDING AND CURRENT ROW
    ) as moving_avg_50
FROM market_data;
```

**With Row Expiration (Inactivity Detection):**
```sql
-- Expire old rows after 5 minutes of inactivity
SELECT
    symbol,
    price,
    LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
            EXPIRE AFTER INTERVAL '5' MINUTE INACTIVITY
    ) as prev_price
FROM market_data;

-- Never expire rows (keep entire history in buffer)
SELECT
    symbol,
    price,
    ROW_NUMBER() OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
            EXPIRE AFTER NEVER
    ) as row_num
FROM market_data;
```

**Use cases:**
- **Bounded memory windows**: Prevent unbounded growth in long-running streams
- **Moving averages**: Real-time trending with fixed-size buffers
- **Momentum indicators**: Compare current price to historical values
- **Stateful analytics**: Window functions on row counts rather than time
- **Late data handling**: Inactivity-based cleanup for out-of-order events
- **Financial analysis**: Moving averages, LAG/LEAD, rank-based metrics

**With Compound Partition Keys (Multiple Columns):**

Partition windows by multiple columns to isolate calculations for distinct combinations:

```sql
-- Financial trader momentum per symbol
-- Each trader+symbol combination gets its own isolated window
SELECT
    trader_id,
    symbol,
    price,
    timestamp,
    LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY trader_id, symbol
            ORDER BY timestamp
    ) as prev_price,
    (price - LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY trader_id, symbol
            ORDER BY timestamp
    )) / LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY trader_id, symbol
            ORDER BY timestamp
    ) * 100 as price_change_pct
FROM trades;

-- Multi-tenant analytics: Isolate metrics per tenant+customer
SELECT
    tenant_id,
    customer_id,
    amount,
    timestamp,
    COUNT(*) OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY tenant_id, customer_id
            ORDER BY timestamp
    ) as customer_activity_count,
    AVG(amount) OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY tenant_id, customer_id
            ORDER BY timestamp
    ) as customer_avg_amount
FROM transactions;
```

**When to use compound partition keys:**
- **Multi-tenant systems**: Isolate each tenant's data completely
- **Financial trading**: Separate analytics per trader and instrument
- **E-commerce**: Track metrics per customer and product category
- **IoT networks**: Group sensors by location and device type
- **User behavior**: Analyze sessions per user and application

### Gap Eviction - Automatic Cleanup for Irregular Data Streams

**What is Gap Eviction?**

Gap eviction is an automatic cleanup mechanism in ROWS WINDOW that detects gaps (delays) in incoming data and refreshes the buffer. When there's a significant time gap between records (network delay, system pause, etc.), the buffer is cleared automatically, preventing stale data from skewing calculations.

**The Problem It Solves:**

Imagine monitoring stock prices with ROWS WINDOW(100):

```
10:00:00 - 10:05:00 ✅ Steady stream of prices
Buffer: [prices from last 100 records] → AVG(price) = accurate

10:05:00 - 10:10:05 ❌ NETWORK OUTAGE (5+ minute gap, no data)
Buffer: [stale prices from 10:05:00] → AVG(price) = WRONG! (outdated)

10:10:05 - New price arrives
WITHOUT gap eviction: Average includes 5-minute-old stale data ✗
WITH gap eviction: Buffer cleared, average uses only fresh data ✓
```

**How Gap Eviction Works:**

1. Each record tracks a `last_activity_timestamp`
2. When a new record arrives, system checks: `current_time - last_activity_time`
3. If gap exceeds threshold: **Automatically clear the buffer**
4. If gap is normal: **Keep accumulating**

**Three Gap Eviction Modes:**

#### 1. Default - 1 Minute Timeout (Most Common)

```sql
SELECT
    symbol,
    price,
    AVG(price) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
            -- No EXPIRE AFTER clause = 60 second default timeout
    ) as moving_avg
FROM market_data;
```

**Behavior:**
- If >60 seconds pass with no new data → buffer clears automatically
- Next record starts fresh with just that one record
- Ideal for: Stock quotes, sensor data, streaming with occasional jitter

#### 2. Custom Timeout - Set Your Own Threshold

```sql
-- High-frequency trading: 5 second timeout
SELECT
    symbol,
    price,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
        EXPIRE AFTER INTERVAL '5' SECOND INACTIVITY
    ) as moving_avg
FROM trades;

-- IoT sensors: 10 minute tolerance
SELECT
    device_id,
    reading,
    AVG(reading) OVER (
        ROWS WINDOW BUFFER 500 ROWS
        PARTITION BY device_id
        ORDER BY timestamp
        EXPIRE AFTER INTERVAL '10' MINUTE INACTIVITY
    ) as avg_reading
FROM sensors;

-- User events: 30 minute session timeout
SELECT
    user_id,
    event_type,
    COUNT(*) OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY user_id
        ORDER BY timestamp
        EXPIRE AFTER INTERVAL '30' MINUTE INACTIVITY
    ) as session_events
FROM user_activity;
```

**Choosing the Right Timeout:**

| Data Type | Typical Timeout | Reasoning |
|-----------|-----------------|-----------|
| Stock ticker | 30-60 seconds | Quotes update frequently; data >1min old is stale |
| High-freq trading | 5-10 seconds | Sub-second trades; delays indicate errors |
| IoT sensors | 5-15 minutes | Devices batch readings; regular update cycles |
| User activity | 15-30 minutes | Sessions have natural pauses |
| Payment events | 2-5 minutes | Financial data has strict ordering |
| Application logs | 1-5 minutes | Logs should arrive regularly |

#### 3. Never Evict - Disable Gap Detection

```sql
SELECT
    symbol,
    price,
    AVG(price) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
            EXPIRE AFTER NEVER  -- Disable gap eviction
    ) as moving_avg
FROM market_data;
```

**When to use:**
- Batch-like workloads where guaranteed-complete batches arrive
- Post-processing of archived data
- When you want buffer to only clear by reaching size limit
- Reduced per-record overhead

**Real-World Example: Stock Price Monitoring**

```sql
-- Detect stale quotes with gap eviction
SELECT
    timestamp,
    symbol,
    price,
    volume,

    -- 50-record moving average with 30-second gap detection
    AVG(price) OVER (
        ROWS WINDOW
            BUFFER 50 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
            EXPIRE AFTER INTERVAL '30' SECOND INACTIVITY
    ) as moving_avg,

    -- Momentum: price change from 10 records ago
    price - LAG(price, 10) OVER (
        ROWS WINDOW
            BUFFER 50 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
            EXPIRE AFTER INTERVAL '30' SECOND INACTIVITY
    ) as momentum_10,

    -- Flag stale data
    CASE
        WHEN timestamp > NOW() - INTERVAL 5 MINUTE THEN 'FRESH'
        WHEN timestamp > NOW() - INTERVAL 30 MINUTE THEN 'RECENT'
        ELSE 'STALE'  -- Older than 30 min, gap eviction should trigger
    END as data_freshness

FROM stock_quotes
WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL');
```

**How Data Flows Through Gap Eviction:**

```
Timeline with 30-second timeout:
═══════════════════════════════════════════════════════════

10:00:00 - Record (price=100)
Buffer: [100], activity_time=10:00:00 ✓

10:00:05 - Record (price=101)  [gap=5s, OK]
Buffer: [100, 101], activity_time=10:00:05 ✓

10:00:10 - Record (price=102)  [gap=5s, OK]
Buffer: [100, 101, 102], activity_time=10:00:10 ✓

(gap with no records for 35 seconds...)

10:00:45 - Record (price=103)  [gap=35s > 30s timeout]
❌ GAP DETECTED! Gap eviction triggered
Buffer: [103] ← cleared and reset!
activity_time=10:00:45 ✓

10:00:50 - Record (price=104)  [gap=5s, OK]
Buffer: [103, 104], activity_time=10:00:50 ✓
```

**Performance Characteristics:**

- **Overhead**: One timestamp comparison per record = `O(1)` negligible cost
- **Memory**: Freed immediately when buffer clears
- **Latency**: No impact on per-record processing time
- **Accuracy**: Prevents stale data contamination in aggregates

**When Gap Eviction Triggers Events:**

```rust
// Pseudocode showing when gap eviction triggers
if (current_timestamp - last_activity_timestamp) > timeout {
    println!("[GAP EVICTION] {} second gap detected, clearing buffer",
             (current_timestamp - last_activity_timestamp) / 1000);
    buffer.clear();  // Remove all old records
    buffer.push(current_record);  // Start fresh
}
```

**Testing Gap Eviction Behavior:**

Test different scenarios in your application:

```sql
-- Test 1: Normal data flow (no eviction)
-- Records arrive every 5 seconds, 30-second timeout
-- Expected: Buffer accumulates normally

-- Test 2: Network delay (triggers eviction)
-- Record at 10:00, next at 10:01:00 (60-second gap)
-- With 30-second timeout: Buffer clears
-- Expected: AVG should reset

-- Test 3: Boundary condition
-- Test 29.9 seconds gap (should NOT evict)
-- Test 30.1 seconds gap (should evict)
-- Expected: Precise timeout enforcement
```

**Best Practices:**

1. **Match your data characteristics**
   - Review historical data for typical gaps
   - Set timeout slightly higher than normal delays
   - Leave 10-20% margin for jitter

2. **Monitor for unexpected evictions**
   - Log when gap evictions occur
   - Alert if frequency is too high (indicates network issues)
   - Track before/after buffer states

3. **Combine with other mechanisms**
   - Use gap eviction + partition key for session detection
   - Combine with RANK() for out-of-order detection
   - Pair with COUNT(*) to measure buffer size at eviction

4. **Document in comments**
   ```sql
   -- Gap timeout set to 5 minutes based on:
   -- - Historical data analysis: 99% of gaps < 4 minutes
   -- - Network SLA: 95% delivery within 2 minutes
   -- - Application requirement: Stale data >5min unacceptable
   EXPIRE AFTER INTERVAL '5' MINUTE INACTIVITY
   ```

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

Window functions work with all window types: **TUMBLING**, **SLIDING**, **SESSION**, and **ROWS** windows. The OVER clause defines the partitioning and ordering for the window function calculation.

### Row Numbering and Ranking

```sql
-- Assign sequence numbers to orders per customer (with ROWS WINDOW)
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY customer_id
            ORDER BY order_date
    ) as order_sequence
FROM orders;

-- With ROWS WINDOW - limit to last 100 rows per customer
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY customer_id
            ORDER BY order_date
    ) as recent_order_num
FROM orders;
```

```sql
-- Rank orders by amount (with gaps for ties) using ROWS WINDOW
SELECT
    order_id,
    customer_id,
    amount,
    RANK() OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            ORDER BY amount DESC
    ) as amount_rank,
    DENSE_RANK() OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            ORDER BY amount DESC
    ) as amount_dense_rank
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
    LAG(amount, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY customer_id
            ORDER BY order_date
    ) as prev_amount,
    LAG(order_date, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY customer_id
            ORDER BY order_date
    ) as prev_order_date
FROM orders;
```

```sql
-- Look ahead to next order with bounded ROWS WINDOW
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    LEAD(amount, 1) OVER (
        ROWS WINDOW
            BUFFER 50 ROWS
            PARTITION BY customer_id
            ORDER BY order_date
            EXPIRE AFTER NEVER
    ) as next_amount,
    LEAD(order_date, 2) OVER (
        ROWS WINDOW
            BUFFER 50 ROWS
            PARTITION BY customer_id
            ORDER BY order_date
            EXPIRE AFTER NEVER
    ) as next_next_order_date
FROM orders;
```

```sql
-- Moving window comparison with row expiration
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    LAG(amount, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY customer_id
            ORDER BY order_date
            EXPIRE AFTER INTERVAL '15' MINUTE INACTIVITY
    ) as prev_order_amount,
    (amount - LAG(amount, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY customer_id
            ORDER BY order_date
            EXPIRE AFTER INTERVAL '15' MINUTE INACTIVITY
    )) as amount_change
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
        ROWS WINDOW
            BUFFER 1000 ROWS
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
        ROWS WINDOW
            BUFFER 1000 ROWS
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
        ROWS WINDOW
            BUFFER 1000 ROWS
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
        ROWS WINDOW
            BUFFER 1000 ROWS
            ORDER BY order_date
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as orders_up_to_date,
    SUM(amount) OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
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
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as ma_5_day,
    AVG(close_price) OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as ma_20_day,
    AVG(close_price) OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
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
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY device_id
            ORDER BY reading_timestamp
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as avg_last_10_readings,
    CASE
        WHEN ABS(sensor_reading - AVG(sensor_reading) OVER (
            ROWS WINDOW
                BUFFER 1000 ROWS
                PARTITION BY device_id
                ORDER BY reading_timestamp
                ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING
        )) > 2 * STDDEV(sensor_reading) OVER (
            ROWS WINDOW
                BUFFER 1000 ROWS
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