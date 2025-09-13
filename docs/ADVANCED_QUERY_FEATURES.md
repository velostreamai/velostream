# Advanced Query Features Guide

## Overview

Phase 3 introduces advanced SQL capabilities to FerrisStreams, including enhanced window functions, complex aggregations, join operations, subquery support, and advanced SQL functions. This guide covers all the new query features and their usage patterns.

## Window Functions

### Enhanced OVER Clause Support

FerrisStreams supports comprehensive window functions with partitioning and ordering:

```sql
-- ROW_NUMBER with partitioning
SELECT 
    ticker, 
    price, 
    volume,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price DESC) as price_rank
FROM stock_prices 
WINDOW TUMBLING (INTERVAL '1' MINUTE);

-- Multiple window functions
SELECT 
    ticker,
    price,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price DESC) as rank,
    RANK() OVER (PARTITION BY ticker ORDER BY price DESC) as dense_rank,
    LAG(price, 1) OVER (PARTITION BY ticker ORDER BY timestamp) as prev_price,
    LEAD(price, 1) OVER (PARTITION BY ticker ORDER BY timestamp) as next_price
FROM stock_prices;
```

### Supported Window Functions

#### Ranking Functions
- `ROW_NUMBER()`: Sequential numbering within partition
- `RANK()`: Ranking with gaps for ties
- `DENSE_RANK()`: Ranking without gaps for ties

#### Value Functions  
- `LAG(expr, offset)`: Previous value in partition
- `LEAD(expr, offset)`: Next value in partition
- `FIRST_VALUE(expr)`: First value in window frame
- `LAST_VALUE(expr)`: Last value in window frame

#### Statistical Functions
- `PERCENT_RANK()`: Relative rank as percentage
- `CUME_DIST()`: Cumulative distribution
- `NTILE(n)`: Divide partition into n buckets

### Window Frame Specification

```sql
-- Rows-based window frame
SELECT 
    ticker, 
    price,
    AVG(price) OVER (
        PARTITION BY ticker 
        ORDER BY timestamp 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as moving_avg_5
FROM stock_prices;

-- Range-based window frame
SELECT 
    ticker,
    price, 
    timestamp,
    AVG(price) OVER (
        PARTITION BY ticker
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
    ) as time_weighted_avg
FROM stock_prices;
```

## Complex Aggregations

### Nested Aggregations

```sql
-- Aggregation of aggregated results
SELECT 
    sector,
    AVG(daily_avg_price) as sector_avg_price,
    MAX(daily_max_price) as sector_peak_price
FROM (
    SELECT 
        sector,
        DATE_TRUNC('day', timestamp) as day,
        AVG(price) as daily_avg_price,
        MAX(price) as daily_max_price
    FROM stock_prices
    GROUP BY sector, DATE_TRUNC('day', timestamp)
) daily_aggregates
GROUP BY sector;
```

### HAVING Clause Support

```sql
-- Post-aggregation filtering
SELECT 
    ticker,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    SUM(volume) as total_volume
FROM trades
WHERE price > 0  -- Pre-aggregation filter
GROUP BY ticker
HAVING COUNT(*) > 100    -- Post-aggregation filter
   AND AVG(price) > 50.0
   AND SUM(volume) > 1000000;
```

### Advanced Aggregation Functions

#### Statistical Aggregations
```sql
SELECT 
    ticker,
    COUNT(*) as trade_count,
    AVG(price) as mean_price,
    STDDEV(price) as price_stddev,
    VARIANCE(price) as price_variance,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM trades
GROUP BY ticker;
```

#### String Aggregations
```sql
-- Concatenate values
SELECT 
    sector,
    LISTAGG(ticker, ', ') as tickers_in_sector
FROM stocks
GROUP BY sector;
```

## Join Operations

### Inner Joins
```sql
-- Basic inner join
SELECT 
    o.order_id,
    o.customer_id,
    p.payment_amount,
    o.order_total
FROM orders o
INNER JOIN payments p ON o.order_id = p.order_id;
```

### Outer Joins
```sql
-- Left join to include all orders
SELECT 
    o.order_id,
    o.customer_id,
    o.order_total,
    COALESCE(p.payment_amount, 0) as payment_amount
FROM orders o
LEFT JOIN payments p ON o.order_id = p.order_id;

-- Right join to include all payments
SELECT 
    o.order_id,
    o.customer_id,
    p.payment_id,
    p.payment_amount
FROM orders o
RIGHT JOIN payments p ON o.order_id = p.order_id;

-- Full outer join
SELECT 
    COALESCE(o.order_id, p.order_id) as order_id,
    o.customer_id,
    o.order_total,
    p.payment_amount
FROM orders o
FULL OUTER JOIN payments p ON o.order_id = p.order_id;
```

### Time-Based Joins
```sql
-- Join within time windows
SELECT 
    o.order_id,
    o.timestamp as order_time,
    p.timestamp as payment_time,
    p.payment_amount
FROM orders o
JOIN payments p ON o.order_id = p.order_id
WHERE p.timestamp BETWEEN o.timestamp AND o.timestamp + INTERVAL '1' HOUR;
```

### Multiple Table Joins
```sql
-- Three-way join
SELECT 
    c.customer_name,
    o.order_id,
    p.payment_amount,
    s.shipping_status
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN payments p ON o.order_id = p.order_id
LEFT JOIN shipping s ON o.order_id = s.order_id;
```

## Subquery Support

### Scalar Subqueries
```sql
-- Subquery returning single value
SELECT 
    ticker,
    price,
    price - (SELECT AVG(price) FROM stock_prices) as price_diff_from_avg
FROM stock_prices
WHERE volume > 1000;
```

### Correlated Subqueries
```sql
-- Subquery referencing outer query
SELECT 
    ticker,
    price,
    volume
FROM stock_prices outer_sp
WHERE price > (
    SELECT AVG(price) 
    FROM stock_prices inner_sp 
    WHERE inner_sp.ticker = outer_sp.ticker
);
```

### EXISTS and NOT EXISTS
```sql
-- Check for existence
SELECT DISTINCT customer_id
FROM orders o
WHERE EXISTS (
    SELECT 1 
    FROM payments p 
    WHERE p.order_id = o.order_id 
    AND p.payment_amount >= o.order_total
);

-- Check for non-existence  
SELECT customer_id, order_id
FROM orders o
WHERE NOT EXISTS (
    SELECT 1 
    FROM payments p 
    WHERE p.order_id = o.order_id
);
```

### IN and NOT IN with Subqueries
```sql
-- Membership testing
SELECT *
FROM products
WHERE category_id IN (
    SELECT category_id 
    FROM categories 
    WHERE is_featured = true
);

-- Non-membership testing
SELECT *
FROM customers 
WHERE customer_id NOT IN (
    SELECT DISTINCT customer_id 
    FROM orders 
    WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
);
```

## Advanced SQL Functions

### Mathematical Functions
```sql
SELECT 
    price,
    ABS(price - 100) as abs_diff,
    ROUND(price, 2) as rounded_price,
    CEIL(price) as ceiling_price,
    FLOOR(price) as floor_price,
    MOD(CAST(price AS INTEGER), 10) as price_mod_10,
    POWER(price, 2) as price_squared,
    SQRT(price) as price_sqrt
FROM stock_prices;
```

### String Functions
```sql
SELECT 
    customer_name,
    UPPER(customer_name) as upper_name,
    LOWER(customer_name) as lower_name,
    LENGTH(customer_name) as name_length,
    SUBSTRING(customer_name, 1, 3) as initials,
    CONCAT(first_name, ' ', last_name) as full_name,
    TRIM(BOTH ' ' FROM customer_name) as trimmed_name,
    REPLACE(customer_name, 'Corp', 'Corporation') as expanded_name,
    LEFT(customer_name, 5) as name_prefix,
    RIGHT(customer_name, 5) as name_suffix,
    POSITION('Inc' IN customer_name) as inc_position
FROM customers;
```

### Date/Time Functions
```sql
SELECT 
    order_timestamp,
    NOW() as current_time,
    CURRENT_TIMESTAMP as current_ts,
    DATE_FORMAT(order_timestamp, '%Y-%m-%d') as formatted_date,
    EXTRACT(YEAR FROM order_timestamp) as order_year,
    EXTRACT(MONTH FROM order_timestamp) as order_month,
    EXTRACT(DAY FROM order_timestamp) as order_day,
    EXTRACT(HOUR FROM order_timestamp) as order_hour,
    DATEDIFF('day', order_timestamp, NOW()) as days_since_order
FROM orders;
```

### Conditional Functions
```sql
SELECT 
    ticker,
    price,
    volume,
    CASE 
        WHEN volume > 1000000 THEN 'High'
        WHEN volume > 100000 THEN 'Medium' 
        ELSE 'Low'
    END as volume_category,
    COALESCE(last_price, price) as effective_price,
    NULLIF(price, 0) as non_zero_price
FROM stock_prices;
```

### JSON Processing Functions
```sql
-- Extract values from JSON fields
SELECT 
    event_id,
    JSON_VALUE(event_data, '$.user_id') as user_id,
    JSON_VALUE(event_data, '$.action') as action,
    JSON_EXTRACT(event_data, '$.metadata') as metadata
FROM events
WHERE JSON_VALUE(event_data, '$.event_type') = 'purchase';
```

## Performance Optimizations

### Query Plan Optimization

FerrisStreams automatically optimizes query plans:

```sql
-- Predicate pushdown optimization
SELECT c.customer_name, o.order_total
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '7' DAY
  AND c.customer_type = 'Premium';
-- Filter is pushed down to reduce join cardinality
```

### Index Hints (Future)
```sql
-- Hint for optimal execution (planned feature)
SELECT /*+ INDEX(orders, idx_customer_date) */
    customer_id, COUNT(*) 
FROM orders 
WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY customer_id;
```

## Streaming-Specific Features

### Continuous Queries
```sql
-- Continuous aggregation with complex expressions
SELECT 
    ticker,
    AVG(price) as avg_price,
    STDDEV(price) as price_volatility,
    COUNT(*) as trade_count,
    MAX(volume) as max_volume,
    CASE 
        WHEN STDDEV(price) / AVG(price) > 0.1 THEN 'Volatile'
        ELSE 'Stable'
    END as volatility_status
FROM stock_trades
GROUP BY ticker
EMIT CHANGES;
```

### Windowed Joins
```sql
-- Join within sliding time windows
SELECT 
    t.ticker,
    t.price as trade_price,
    n.sentiment_score,
    t.timestamp as trade_time
FROM trades t
JOIN news_sentiment n ON t.ticker = n.ticker
WHERE n.timestamp BETWEEN t.timestamp - INTERVAL '1' HOUR 
                      AND t.timestamp
WINDOW SLIDING (INTERVAL '5' MINUTE);
```

## Examples

### Financial Analytics
```sql
-- Comprehensive financial analytics query
WITH price_stats AS (
    SELECT 
        ticker,
        DATE_TRUNC('day', timestamp) as trading_day,
        FIRST_VALUE(price) OVER (
            PARTITION BY ticker, DATE_TRUNC('day', timestamp)
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as open_price,
        LAST_VALUE(price) OVER (
            PARTITION BY ticker, DATE_TRUNC('day', timestamp)
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  
        ) as close_price,
        MIN(price) OVER (
            PARTITION BY ticker, DATE_TRUNC('day', timestamp)
        ) as low_price,
        MAX(price) OVER (
            PARTITION BY ticker, DATE_TRUNC('day', timestamp)
        ) as high_price,
        SUM(volume) OVER (
            PARTITION BY ticker, DATE_TRUNC('day', timestamp)
        ) as daily_volume
    FROM stock_prices
)
SELECT 
    ticker,
    trading_day,
    open_price,
    close_price,
    high_price,
    low_price,
    ((close_price - open_price) / open_price) * 100 as daily_return_pct,
    daily_volume
FROM price_stats
WHERE daily_volume > 1000000
ORDER BY ticker, trading_day;
```

### Real-Time Alerting
```sql
-- Complex alerting with multiple conditions
SELECT 
    ticker,
    price,
    volume,
    timestamp,
    'ALERT' as message_type,
    CASE
        WHEN price_change_pct > 5.0 THEN 'PRICE_SPIKE'
        WHEN price_change_pct < -5.0 THEN 'PRICE_DROP'
        WHEN volume > avg_volume * 3 THEN 'VOLUME_SURGE'
        ELSE 'OTHER'
    END as alert_type
FROM (
    SELECT 
        ticker,
        price,
        volume,
        timestamp,
        ((price - LAG(price, 1) OVER (PARTITION BY ticker ORDER BY timestamp)) / 
         LAG(price, 1) OVER (PARTITION BY ticker ORDER BY timestamp)) * 100 
        as price_change_pct,
        AVG(volume) OVER (
            PARTITION BY ticker 
            ORDER BY timestamp 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as avg_volume
    FROM stock_prices
    WHERE timestamp >= NOW() - INTERVAL '1' HOUR
) price_analysis
WHERE ABS(price_change_pct) > 5.0 
   OR volume > avg_volume * 3
EMIT CHANGES;
```

## Best Practices

### Query Optimization
1. **Use appropriate indexes**: Create indexes on frequently joined/filtered columns
2. **Limit result sets**: Use LIMIT and WHERE clauses effectively
3. **Choose efficient joins**: Use EXISTS instead of IN for large subqueries
4. **Aggregate early**: Push aggregations down in complex queries

### Window Function Guidelines
1. **Partition appropriately**: Use PARTITION BY to limit window scope
2. **Order efficiently**: Choose optimal ORDER BY for your use case  
3. **Use appropriate frames**: ROWS vs RANGE based on requirements
4. **Avoid excessive functions**: Multiple window functions can impact performance

### Subquery Best Practices
1. **Prefer JOINs**: Generally more efficient than correlated subqueries
2. **Use EXISTS/NOT EXISTS**: More efficient than IN/NOT IN with NULLs
3. **Limit subquery scope**: Use WHERE clauses in subqueries
4. **Consider materialization**: Cache results of expensive subqueries

## Troubleshooting

### Common Issues
1. **Slow window functions**: Check partitioning and ordering
2. **Cartesian products**: Verify JOIN conditions
3. **NULL handling**: Use COALESCE and proper NULL checks
4. **Type mismatches**: Ensure compatible data types in operations

### Performance Tips
1. **Monitor execution plans**: Use EXPLAIN to understand query execution
2. **Profile queries**: Use profiling tools to identify bottlenecks
3. **Optimize data types**: Use appropriate types for better performance
4. **Batch operations**: Process data in appropriate batch sizes

## Related Features

- [Watermarks & Time Semantics](./WATERMARKS_TIME_SEMANTICS.md) - Time-based operations
- [Resource Management](./RESOURCE_MANAGEMENT.md) - Query resource limits  
- [Observability Guide](./OBSERVABILITY.md) - Query performance monitoring

## References

- [SQL Window Functions](https://www.postgresql.org/docs/current/tutorial-window.html)
- [SQL Joins](https://www.w3schools.com/sql/sql_join.asp)
- [Subqueries](https://www.postgresql.org/docs/current/functions-subquery.html)