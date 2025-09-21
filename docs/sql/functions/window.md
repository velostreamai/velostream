# Window Functions

Complete reference for window functions in Velostream. These functions perform calculations across rows related to the current row using OVER clauses, essential for ranking, running totals, and comparative analysis.

## Ranking Functions

### ROW_NUMBER() - Sequential Numbering

```sql
-- Assign unique sequence numbers within partitions
SELECT
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_sequence
FROM orders;

-- Number all records globally
SELECT
    product_id,
    product_name,
    price,
    ROW_NUMBER() OVER (ORDER BY price DESC) as price_rank_global
FROM products;
```

### RANK() - Ranking with Gaps

```sql
-- Rank with gaps for tied values
SELECT
    student_id,
    test_score,
    RANK() OVER (ORDER BY test_score DESC) as score_rank
FROM test_results;
-- If two students tie for 2nd place, next rank is 4th (gap created)

-- Rank within categories
SELECT
    product_id,
    category,
    sales_amount,
    RANK() OVER (PARTITION BY category ORDER BY sales_amount DESC) as category_rank
FROM product_sales;
```

## TOP-N Analysis with RANK Functions

### Basic TOP-N Patterns

```sql
-- Top 3 customers by spending (using RANK)
SELECT *
FROM (
    SELECT
        customer_id,
        customer_name,
        total_spent,
        RANK() OVER (ORDER BY total_spent DESC) as spending_rank
    FROM customer_totals
) ranked
WHERE spending_rank <= 3;

-- Top 5 products per category (using ROW_NUMBER for exact count)
SELECT *
FROM (
    SELECT
        product_id,
        category,
        sales_amount,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales_amount DESC) as category_rank
    FROM product_sales
) ranked
WHERE category_rank <= 5;

-- Top 10% of performers (using PERCENT_RANK)
SELECT
    employee_id,
    performance_score,
    PERCENT_RANK() OVER (ORDER BY performance_score DESC) as percentile_rank
FROM employee_reviews
WHERE PERCENT_RANK() OVER (ORDER BY performance_score DESC) <= 0.1;  -- Top 10%
```

### Advanced TOP-N with Conditions

```sql
-- Top 3 most recent high-value orders per customer
SELECT *
FROM (
    SELECT
        customer_id,
        order_date,
        order_amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) as recency_rank
    FROM orders
    WHERE order_amount > 1000  -- High-value orders only
) ranked
WHERE recency_rank <= 3;

-- Top performers by quarter with ties handling
SELECT
    quarter,
    employee_id,
    sales_total,
    RANK() OVER (PARTITION BY quarter ORDER BY sales_total DESC) as quarterly_rank,
    DENSE_RANK() OVER (PARTITION BY quarter ORDER BY sales_total DESC) as dense_rank
FROM quarterly_sales
WHERE RANK() OVER (PARTITION BY quarter ORDER BY sales_total DESC) <= 5;
```

### DENSE_RANK() - Ranking without Gaps

```sql
-- Rank without gaps for tied values
SELECT
    employee_id,
    performance_score,
    RANK() OVER (ORDER BY performance_score DESC) as rank_with_gaps,
    DENSE_RANK() OVER (ORDER BY performance_score DESC) as rank_no_gaps
FROM employee_reviews;
-- If two employees tie for 2nd place, next rank is 3rd (no gap)
```

### PERCENT_RANK() - Percentile Ranking

```sql
-- Calculate percentile rankings
SELECT
    customer_id,
    total_spent,
    PERCENT_RANK() OVER (ORDER BY total_spent) as spending_percentile,
    CASE
        WHEN PERCENT_RANK() OVER (ORDER BY total_spent) >= 0.9 THEN 'Top 10%'
        WHEN PERCENT_RANK() OVER (ORDER BY total_spent) >= 0.5 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END as spending_tier
FROM customer_totals;
```

## Value Access Functions

### LAG() - Access Previous Row Values

```sql
-- Compare with previous values
SELECT
    order_date,
    daily_sales,
    LAG(daily_sales, 1) OVER (ORDER BY order_date) as previous_day_sales,
    daily_sales - LAG(daily_sales, 1) OVER (ORDER BY order_date) as daily_change,
    ROUND(
        (daily_sales - LAG(daily_sales, 1) OVER (ORDER BY order_date)) * 100.0 /
        LAG(daily_sales, 1) OVER (ORDER BY order_date),
        2
    ) as daily_change_pct
FROM daily_sales_summary
ORDER BY order_date;

-- Compare with values from N periods ago
SELECT
    month,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY month) as prev_month,
    LAG(revenue, 12) OVER (ORDER BY month) as same_month_last_year,
    revenue - LAG(revenue, 12) OVER (ORDER BY month) as year_over_year_change
FROM monthly_revenue;
```

### LEAD() - Access Following Row Values

```sql
-- Look ahead to future values (limited in streaming contexts)
SELECT
    customer_id,
    order_date,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as next_order_amount,
    LEAD(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as next_order_date
FROM orders;

-- Forward-looking analysis
SELECT
    date,
    stock_price,
    LEAD(stock_price, 1) OVER (ORDER BY date) as next_day_price,
    CASE
        WHEN LEAD(stock_price, 1) OVER (ORDER BY date) > stock_price THEN 'Will Rise'
        WHEN LEAD(stock_price, 1) OVER (ORDER BY date) < stock_price THEN 'Will Fall'
        ELSE 'Will Stay Same'
    END as next_day_direction
FROM stock_prices;
```

### FIRST_VALUE() and LAST_VALUE() - Boundary Values

```sql
-- Access first and last values in partitions
SELECT
    customer_id,
    order_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as first_order_amount,
    LAST_VALUE(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as latest_order_amount
FROM orders;

-- Track high and low values within periods
SELECT
    trading_date,
    symbol,
    price,
    FIRST_VALUE(price) OVER (
        PARTITION BY symbol, DATE_FORMAT(trading_date, '%Y-%m')
        ORDER BY price ASC
    ) as monthly_low,
    FIRST_VALUE(price) OVER (
        PARTITION BY symbol, DATE_FORMAT(trading_date, '%Y-%m')
        ORDER BY price DESC
    ) as monthly_high
FROM stock_trades;
```

### NTH_VALUE() - Access Nth Value

```sql
-- Access specific positioned values
SELECT
    customer_id,
    order_date,
    amount,
    NTH_VALUE(amount, 2) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as second_order_amount,
    NTH_VALUE(amount, 3) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as third_order_amount
FROM orders;
```

## Distribution Functions

### CUME_DIST() - Cumulative Distribution

```sql
-- Calculate cumulative distribution
SELECT
    product_id,
    price,
    CUME_DIST() OVER (ORDER BY price) as price_cumulative_dist,
    ROUND(CUME_DIST() OVER (ORDER BY price) * 100, 1) as price_percentile
FROM products
ORDER BY price;
```

### NTILE() - Divide into Buckets

```sql
-- Divide customers into quartiles
SELECT
    customer_id,
    total_spent,
    NTILE(4) OVER (ORDER BY total_spent) as spending_quartile,
    CASE NTILE(4) OVER (ORDER BY total_spent)
        WHEN 1 THEN 'Low Spenders'
        WHEN 2 THEN 'Medium Spenders'
        WHEN 3 THEN 'High Spenders'
        WHEN 4 THEN 'VIP Customers'
    END as customer_segment
FROM customer_totals;

-- Create deciles for detailed analysis
SELECT
    employee_id,
    salary,
    NTILE(10) OVER (ORDER BY salary) as salary_decile
FROM employees;
```

## Window Frames

### ROWS BETWEEN - Physical Row Boundaries

```sql
-- Running totals
SELECT
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue
FROM daily_revenue;

-- Moving averages
SELECT
    date,
    temperature,
    AVG(temperature) OVER (
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_avg_temp
FROM weather_data;

-- Forward and backward windows
SELECT
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) as three_order_total
FROM orders;
```

### RANGE BETWEEN - Value-Based Boundaries

** Supports both numeric and time-based RANGE frames:**
- **Numeric ranges**: `RANGE BETWEEN 100 PRECEDING AND CURRENT ROW`
- **Time-based INTERVAL ranges**: `RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW`

```sql
-- Time-based ranges with traditional syntax
SELECT
    order_date,
    amount,
    COUNT(*) OVER (
        ORDER BY order_date
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as orders_up_to_date,

-- NEW: Time-based INTERVAL ranges for precise temporal windows
SELECT
    trade_timestamp,
    price,
    symbol,
    -- Moving average over last hour of trades
    AVG(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as hourly_moving_avg,
    -- Count of trades in last 5 minutes
    COUNT(*) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
    ) as trades_last_5min,
    -- Standard deviation over last day
    STDDEV(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
    ) as daily_volatility
FROM financial_trades;

-- Advanced: Table aliases in window PARTITION BY (NEW FEATURE)
SELECT
    p.trader_id,
    m.symbol,
    m.event_time,
    m.price,
    -- Use table aliases in PARTITION BY for multi-table windows
    LAG(m.price, 1) OVER (
        PARTITION BY p.trader_id, m.symbol
        ORDER BY m.event_time
        RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
    ) as prev_price,
    -- Rolling volume over last 15 minutes
    SUM(m.volume) OVER (
        PARTITION BY p.trader_id
        ORDER BY m.event_time
        RANGE BETWEEN INTERVAL '15' MINUTE PRECEDING AND CURRENT ROW
    ) as rolling_volume_15min
FROM market_data m
JOIN positions p ON m.symbol = p.symbol;

-- Numeric ranges (traditional)
SELECT
    order_date,
    amount,
    SUM(amount) OVER (
        ORDER BY order_date
        RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) as remaining_revenue
FROM orders;
```

## Advanced Window Function Patterns

### Multiple Window Specifications

```sql
-- Use different window specifications in one query
SELECT
    customer_id,
    order_date,
    amount,
    -- Global ranking
    ROW_NUMBER() OVER (ORDER BY amount DESC) as global_rank,
    -- Customer-specific ranking
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as customer_order_seq,
    -- Running customer total
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS UNBOUNDED PRECEDING
    ) as customer_running_total,
    -- Moving average of last 3 orders per customer
    AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as customer_3_order_avg
FROM orders;
```

### Window Function with Filtering

```sql
-- Conditional window functions
SELECT
    product_id,
    sale_date,
    quantity,
    -- Rank only among profitable sales
    CASE
        WHEN profit > 0 THEN
            RANK() OVER (PARTITION BY product_id ORDER BY profit DESC)
        ELSE NULL
    END as profitable_sale_rank,
    -- Running total of successful sales only
    SUM(CASE WHEN status = 'completed' THEN quantity ELSE 0 END) OVER (
        PARTITION BY product_id
        ORDER BY sale_date
        ROWS UNBOUNDED PRECEDING
    ) as completed_quantity_total
FROM sales;
```

## Real-World Examples

### Sales Performance Analysis

```sql
-- Comprehensive sales performance dashboard
SELECT
    salesperson_id,
    sale_month,
    monthly_sales,
    -- Monthly ranking
    RANK() OVER (PARTITION BY sale_month ORDER BY monthly_sales DESC) as monthly_rank,
    -- Year-to-date cumulative sales
    SUM(monthly_sales) OVER (
        PARTITION BY salesperson_id
        ORDER BY sale_month
        ROWS UNBOUNDED PRECEDING
    ) as ytd_sales,
    -- 3-month moving average
    AVG(monthly_sales) OVER (
        PARTITION BY salesperson_id
        ORDER BY sale_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as three_month_avg,
    -- Performance vs previous month
    monthly_sales - LAG(monthly_sales, 1) OVER (
        PARTITION BY salesperson_id
        ORDER BY sale_month
    ) as month_over_month_change,
    -- Quartile placement
    NTILE(4) OVER (PARTITION BY sale_month ORDER BY monthly_sales) as performance_quartile
FROM monthly_sales_summary;
```

### Customer Lifecycle Analysis

```sql
-- Track customer purchase patterns
SELECT
    customer_id,
    order_date,
    order_amount,
    -- Order sequence for each customer
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_number,
    -- Days since previous order
    order_date - LAG(order_date, 1) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) as days_since_last_order,
    -- Running lifetime value
    SUM(order_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS UNBOUNDED PRECEDING
    ) as lifetime_value,
    -- Average order value (last 5 orders)
    AVG(order_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as recent_avg_order_value,
    -- First and most recent order amounts
    FIRST_VALUE(order_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) as first_order_amount,
    LAST_VALUE(order_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as current_order_amount
FROM orders;
```

### Financial Time Series Analysis

```sql
-- Stock price analysis with technical indicators
SELECT
    symbol,
    trade_date,
    closing_price,
    -- Simple moving averages
    AVG(closing_price) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as sma_20,
    AVG(closing_price) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) as sma_50,
    -- Price change from previous day
    closing_price - LAG(closing_price, 1) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
    ) as daily_change,
    -- Percentage change
    ROUND(
        (closing_price - LAG(closing_price, 1) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
        )) * 100.0 / LAG(closing_price, 1) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
        ), 2
    ) as daily_change_pct,
    -- 52-week high and low
    MAX(closing_price) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW  -- ~252 trading days in a year
    ) as week_52_high,
    MIN(closing_price) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) as week_52_low
FROM stock_prices
ORDER BY symbol, trade_date;
```

### Inventory Management

```sql
-- Inventory turnover and stock analysis
SELECT
    product_id,
    date,
    stock_level,
    daily_sales,
    -- Days of inventory remaining (simple calculation)
    CASE
        WHEN daily_sales > 0 THEN stock_level / daily_sales
        ELSE NULL
    END as days_of_inventory,
    -- Inventory trend (increasing/decreasing)
    stock_level - LAG(stock_level, 1) OVER (
        PARTITION BY product_id
        ORDER BY date
    ) as daily_stock_change,
    -- Average sales over last 7 days
    AVG(daily_sales) OVER (
        PARTITION BY product_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as avg_sales_7_days,
    -- Stock rank among all products
    RANK() OVER (PARTITION BY date ORDER BY stock_level DESC) as stock_level_rank,
    -- Velocity rank (sales rank)
    RANK() OVER (PARTITION BY date ORDER BY daily_sales DESC) as sales_velocity_rank
FROM inventory_daily;
```

## Performance Tips

### Efficient Window Function Usage

```sql
-- ✅ Good: Use single OVER clause for multiple functions
SELECT
    customer_id,
    order_amount,
    RANK() OVER (ORDER BY order_amount DESC) as amount_rank,
    DENSE_RANK() OVER (ORDER BY order_amount DESC) as amount_dense_rank
FROM orders;

-- ✅ Good: Use window aliases for complex windows
SELECT
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER customer_window as running_total,
    AVG(amount) OVER customer_window as running_average
FROM orders
WINDOW customer_window AS (
    PARTITION BY customer_id
    ORDER BY order_date
    ROWS UNBOUNDED PRECEDING
);
```

### Memory Considerations

```sql
-- ⚠️ Be careful with large window frames
SELECT
    *,
    SUM(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  -- Can use lots of memory
    ) as running_total
FROM large_orders_table;

-- ✅ Better: Use limited window frames when possible
SELECT
    *,
    SUM(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 999 PRECEDING AND CURRENT ROW  -- Limited to last 1000 rows
    ) as rolling_1000_total
FROM large_orders_table;
```

## Quick Reference

### Ranking Functions
| Function | Purpose | Gaps for Ties |
|----------|---------|----------------|
| `ROW_NUMBER()` | Sequential numbering | N/A (always unique) |
| `RANK()` | Ranking | Yes (1,2,2,4) |
| `DENSE_RANK()` | Ranking | No (1,2,2,3) |
| `PERCENT_RANK()` | Percentile (0-1) | N/A |

### Value Access Functions
| Function | Purpose | Direction |
|----------|---------|----------|
| `LAG(expr, n)` | Previous row value | Backward |
| `LEAD(expr, n)` | Next row value | Forward |
| `FIRST_VALUE(expr)` | First in partition | Boundary |
| `LAST_VALUE(expr)` | Last in frame | Boundary |
| `NTH_VALUE(expr, n)` | Nth value | Position |

### Distribution Functions
| Function | Purpose | Output |
|----------|---------|--------|
| `CUME_DIST()` | Cumulative distribution | 0-1 |
| `NTILE(n)` | Divide into n buckets | 1-n |

### Window Frame Types
| Frame | Description | Example |
|-------|-------------|--------|
| `ROWS BETWEEN` | Physical row count | `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW` |
| `RANGE BETWEEN` | Value-based range | `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |

## Next Steps

- [Window analysis guide](../by-task/window-analysis.md) - Time-based windowing patterns
- [Aggregation functions](aggregation.md) - Functions often used with windows
- [Essential functions](essential.md) - Most commonly used functions