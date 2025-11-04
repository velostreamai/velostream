# Aggregation Functions

Complete reference for SQL aggregation functions in Velostream. These functions calculate single values from multiple rows, typically used with GROUP BY.

## Core Aggregation Functions

### COUNT - Count Records

```sql
-- Count all rows
SELECT COUNT(*) as total_orders FROM orders;

-- Count non-null values only
SELECT COUNT(email) as customers_with_email FROM customers;

-- Count with conditions
SELECT
    COUNT(*) as total_orders,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders
FROM orders;
```

### COUNT_DISTINCT - Count Unique Values (Exact)

```sql
-- Count unique customers (exact count)
SELECT COUNT_DISTINCT(customer_id) as unique_customers FROM orders;

-- Count unique products per category
SELECT
    category,
    COUNT_DISTINCT(product_id) as unique_products
FROM products
GROUP BY category;
```

**Note:** COUNT_DISTINCT provides exact counts but requires storing all unique values in memory. For large datasets, consider using APPROX_COUNT_DISTINCT for better performance.

### APPROX_COUNT_DISTINCT - Approximate Unique Count (HyperLogLog)

Fast approximation of unique value counts using the HyperLogLog algorithm. Provides ~2% error rate while using minimal memory (12KB fixed size regardless of cardinality).

```sql
-- Approximate unique visitor count (fast, memory-efficient)
SELECT APPROX_COUNT_DISTINCT(visitor_id) as approx_unique_visitors
FROM page_views;

-- Approximate unique users per page (scales to billions)
SELECT
    page_url,
    APPROX_COUNT_DISTINCT(user_id) as approx_unique_users,
    COUNT(*) as total_views
FROM page_views
GROUP BY page_url;

-- Compare exact vs approximate for validation
SELECT
    COUNT_DISTINCT(user_id) as exact_count,
    APPROX_COUNT_DISTINCT(user_id) as approx_count,
    ABS(COUNT_DISTINCT(user_id) - APPROX_COUNT_DISTINCT(user_id)) as difference
FROM events;
```

**When to use APPROX_COUNT_DISTINCT:**
- ✅ Large datasets (millions/billions of unique values)
- ✅ Real-time dashboards requiring fast responses
- ✅ Memory-constrained environments
- ✅ When ~2% error is acceptable

**When to use COUNT_DISTINCT:**
- ✅ Small to medium datasets (< 1M unique values)
- ✅ When exact precision is required
- ✅ Financial/billing calculations

### SUM - Calculate Totals

```sql
-- Total revenue
SELECT SUM(amount) as total_revenue FROM orders;

-- Revenue by customer
SELECT
    customer_id,
    SUM(amount) as customer_total
FROM orders
GROUP BY customer_id;

-- Conditional sums
SELECT
    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_revenue,
    SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) as pending_revenue
FROM orders;
```

### AVG - Calculate Averages

```sql
-- Average order value
SELECT AVG(amount) as avg_order_value FROM orders;

-- Average by time period
SELECT
    DATE_FORMAT(_timestamp, '%Y-%m') as month,
    AVG(amount) as monthly_avg
FROM orders
GROUP BY DATE_FORMAT(_timestamp, '%Y-%m');

-- Average excluding nulls
SELECT AVG(rating) as avg_rating FROM products WHERE rating IS NOT NULL;
```

### MIN and MAX - Find Extremes

```sql
-- Find price range
SELECT
    MIN(price) as lowest_price,
    MAX(price) as highest_price
FROM products;

-- Earliest and latest orders per customer
SELECT
    customer_id,
    MIN(order_date) as first_order,
    MAX(order_date) as latest_order
FROM orders
GROUP BY customer_id;

-- Find extreme values with details
SELECT
    product_id,
    product_name,
    price
FROM products
WHERE price = (SELECT MIN(price) FROM products)
   OR price = (SELECT MAX(price) FROM products);
```

## Statistical Functions

### STDDEV - Standard Deviation

```sql
-- Price volatility by category
SELECT
    category,
    AVG(price) as avg_price,
    STDDEV(price) as price_volatility
FROM products
GROUP BY category;

-- Order amount consistency per customer
SELECT
    customer_id,
    COUNT(*) as order_count,
    AVG(amount) as avg_amount,
    STDDEV(amount) as amount_consistency
FROM orders
GROUP BY customer_id
HAVING COUNT(*) >= 5;
```

### VARIANCE - Calculate Variance

```sql
-- Score variance analysis
SELECT
    game_level,
    AVG(score) as avg_score,
    VARIANCE(score) as score_variance,
    SQRT(VARIANCE(score)) as score_std_dev
FROM game_scores
GROUP BY game_level;
```

## Positional Functions

### FIRST and LAST - First/Last Values

```sql
-- First and last status per order
SELECT
    order_id,
    FIRST(status) as initial_status,
    LAST(status) as final_status,
    COUNT(*) as status_changes
FROM order_status_history
GROUP BY order_id;

-- Track user journey
SELECT
    user_id,
    FIRST(page_url) as entry_page,
    LAST(page_url) as exit_page,
    COUNT(*) as pages_visited
FROM user_pageviews
GROUP BY user_id
WINDOW SESSION(30m);
```

### STRING_AGG - Concatenate Strings

```sql
-- Concatenate product names per order
SELECT
    order_id,
    STRING_AGG(product_name, ', ') as products_ordered
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY order_id;

-- Create tag lists
SELECT
    article_id,
    STRING_AGG(tag_name, ' | ') as all_tags
FROM article_tags
GROUP BY article_id;

-- Build comma-separated lists
SELECT
    customer_id,
    STRING_AGG(CAST(order_id AS VARCHAR), ', ' ORDER BY order_date) as order_history
FROM orders
GROUP BY customer_id;
```

## Advanced Aggregation Patterns

### Conditional Aggregation

```sql
-- Sales metrics with conditions
SELECT
    DATE_FORMAT(_timestamp, '%Y-%m') as month,
    COUNT(*) as total_orders,
    COUNT(CASE WHEN amount > 100 THEN 1 END) as high_value_orders,
    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_revenue,
    AVG(CASE WHEN amount > 0 THEN amount END) as avg_positive_amount
FROM orders
GROUP BY DATE_FORMAT(_timestamp, '%Y-%m');
```

### Multiple Aggregations

```sql
-- Comprehensive customer analysis
SELECT
    customer_id,
    COUNT(*) as total_orders,
    COUNT_DISTINCT(DATE(order_date)) as active_days,
    SUM(amount) as lifetime_value,
    AVG(amount) as avg_order_value,
    MIN(amount) as smallest_order,
    MAX(amount) as largest_order,
    STDDEV(amount) as order_consistency,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    STRING_AGG(DISTINCT status, ', ') as order_statuses
FROM orders
GROUP BY customer_id;
```

### Percentage Calculations

```sql
-- Category contribution to total sales
SELECT
    category,
    SUM(amount) as category_total,
    COUNT(*) as category_orders,
    SUM(amount) * 100.0 / (SELECT SUM(amount) FROM orders) as revenue_percentage,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders) as order_percentage
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY category
ORDER BY revenue_percentage DESC;
```

### Running Calculations

```sql
-- Cumulative sales by month
SELECT
    month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (ORDER BY month) as cumulative_revenue,
    AVG(monthly_revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_3_month_avg
FROM (
    SELECT
        DATE_FORMAT(_timestamp, '%Y-%m') as month,
        SUM(amount) as monthly_revenue
    FROM orders
    GROUP BY DATE_FORMAT(_timestamp, '%Y-%m')
) monthly_sales
ORDER BY month;
```

## Real-World Examples

### E-commerce Analytics

```sql
-- Complete product performance analysis
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(oi.order_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) as total_revenue,
    AVG(oi.unit_price) as avg_selling_price,
    MIN(oi.unit_price) as lowest_price,
    MAX(oi.unit_price) as highest_price,
    COUNT_DISTINCT(o.customer_id) as unique_customers,
    AVG(r.rating) as avg_rating,
    COUNT(r.review_id) as review_count
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id
LEFT JOIN reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.product_name, p.category
HAVING COUNT(oi.order_id) > 0
ORDER BY total_revenue DESC;
```

### Financial Analysis

```sql
-- Daily trading summary
SELECT
    trade_date,
    symbol,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume,
    AVG(price) as avg_price,
    MIN(price) as day_low,
    MAX(price) as day_high,
    FIRST(price) as opening_price,
    LAST(price) as closing_price,
    STDDEV(price) as price_volatility,
    (LAST(price) - FIRST(price)) * 100.0 / FIRST(price) as daily_return_pct
FROM trades
GROUP BY trade_date, symbol
ORDER BY trade_date, symbol;
```

### User Behavior Analysis

```sql
-- User session metrics
SELECT
    user_id,
    COUNT(*) as total_sessions,
    AVG(session_duration_minutes) as avg_session_duration,
    SUM(pages_viewed) as total_pages_viewed,
    AVG(pages_viewed) as avg_pages_per_session,
    COUNT_DISTINCT(landing_page) as unique_entry_points,
    COUNT(CASE WHEN conversion = true THEN 1 END) as conversions,
    COUNT(CASE WHEN conversion = true THEN 1 END) * 100.0 / COUNT(*) as conversion_rate,
    STRING_AGG(DISTINCT device_type, ', ') as devices_used,
    MIN(session_start) as first_session,
    MAX(session_start) as last_session
FROM user_sessions
GROUP BY user_id
HAVING COUNT(*) >= 2  -- Users with multiple sessions
ORDER BY conversion_rate DESC, total_sessions DESC;
```

## Performance Tips

### Efficient Aggregation

```sql
-- ✅ Good: Use indexes on GROUP BY columns
SELECT category, COUNT(*), AVG(price)
FROM products
GROUP BY category;  -- Ensure category is indexed

-- ✅ Good: Filter before aggregating
SELECT customer_id, SUM(amount)
FROM orders
WHERE order_date >= '2024-01-01'  -- Filter first
GROUP BY customer_id;

-- ⚠️ Careful: COUNT_DISTINCT on large datasets
SELECT COUNT_DISTINCT(customer_id) FROM large_table;  -- Can be slow
```

### Memory Management

```sql
-- Use LIMIT with aggregations on large datasets
SELECT customer_id, SUM(amount) as total
FROM orders
GROUP BY customer_id
ORDER BY total DESC
LIMIT 100;  -- Top 100 customers only
```

## Common Patterns

### Top N Analysis

```sql
-- Top 10 products by revenue
SELECT
    product_name,
    SUM(quantity * unit_price) as revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10;
```

### Ratio Calculations

```sql
-- Customer retention metrics
SELECT
    EXTRACT('YEAR', order_date) as year,
    COUNT_DISTINCT(customer_id) as customers_this_year,
    COUNT_DISTINCT(CASE
        WHEN customer_id IN (
            SELECT DISTINCT customer_id
            FROM orders
            WHERE EXTRACT('YEAR', order_date) = EXTRACT('YEAR', CURRENT_DATE) - 1
        ) THEN customer_id
    END) as returning_customers,
    COUNT_DISTINCT(CASE
        WHEN customer_id IN (
            SELECT DISTINCT customer_id
            FROM orders
            WHERE EXTRACT('YEAR', order_date) = EXTRACT('YEAR', CURRENT_DATE) - 1
        ) THEN customer_id
    END) * 100.0 / COUNT_DISTINCT(customer_id) as retention_rate
FROM orders
GROUP BY EXTRACT('YEAR', order_date)
ORDER BY year;
```

## Quick Reference

| Function | Purpose | Example Usage |
|----------|---------|---------------|
| `COUNT(*)` | Count all rows | Totals, frequencies |
| `COUNT(column)` | Count non-null values | Data quality checks |
| `COUNT_DISTINCT(column)` | Count unique values (exact) | Uniqueness analysis |
| `APPROX_COUNT_DISTINCT(column)` | Count unique values (~2% error, fast) | Large-scale uniqueness, dashboards |
| `SUM(column)` | Total numeric values | Revenue, quantities |
| `AVG(column)` | Average values | Performance metrics |
| `MIN(column)` / `MAX(column)` | Extreme values | Ranges, limits |
| `STDDEV(column)` | Standard deviation | Volatility, consistency |
| `VARIANCE(column)` | Variance | Statistical analysis |
| `FIRST(column)` / `LAST(column)` | Positional values | State tracking |
| `STRING_AGG(column, sep)` | Concatenate strings | Lists, summaries |

## Next Steps

- [GROUP BY operations](../by-task/aggregate-data.md) - Comprehensive aggregation guide
- [Window functions](window.md) - Row-by-row calculations
- [Essential functions](essential.md) - Most commonly used functions