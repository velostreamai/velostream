# Aggregate Streaming Data

Learn how to calculate totals, averages, counts and other aggregate functions with GROUP BY operations in streaming data.

## Basic Grouping

### Count Records per Group
```sql
-- Count orders per customer
SELECT customer_id, COUNT(*) as order_count
FROM orders
GROUP BY customer_id;
```

### Multiple Aggregates
```sql
-- Multiple aggregation functions in one query
SELECT
    category,
    COUNT(*) as count,
    SUM(amount) as total,
    AVG(amount) as average,
    MIN(amount) as minimum,
    MAX(amount) as maximum
FROM transactions
GROUP BY category;
```

## Multiple Column Grouping

```sql
-- Group by customer and status
SELECT customer_id, order_status, COUNT(*) as count, SUM(amount) as total
FROM orders
GROUP BY customer_id, order_status;
```

**This creates separate groups for each unique combination:**
- Customer 1, Status 'pending'
- Customer 1, Status 'completed'
- Customer 2, Status 'pending'
- etc.

## Expression-Based Grouping

### Time-Based Grouping
```sql
-- Group by year and month
SELECT
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    COUNT(*) as monthly_orders
FROM orders
GROUP BY YEAR(order_date), MONTH(order_date);
```

### Category-Based Grouping
```sql
-- Group by price range
SELECT
    CASE
        WHEN amount < 50 THEN 'Low'
        WHEN amount < 200 THEN 'Medium'
        ELSE 'High'
    END as price_range,
    COUNT(*) as order_count,
    AVG(amount) as avg_amount
FROM orders
GROUP BY
    CASE
        WHEN amount < 50 THEN 'Low'
        WHEN amount < 200 THEN 'Medium'
        ELSE 'High'
    END;
```

## HAVING Clause - Filter Grouped Results

Use HAVING to filter groups based on aggregate conditions (after GROUP BY):

```sql
-- High-value customers only (spent more than $1000)
SELECT customer_id, SUM(amount) as total_spent
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 1000;
```

```sql
-- Complex HAVING conditions
SELECT customer_id,
       COUNT(*) as order_count,
       SUM(amount) as total_spent,
       AVG(amount) as avg_order_value
FROM orders
WHERE order_date > '2024-01-01'  -- Filter before grouping
GROUP BY customer_id
HAVING (COUNT(*) > 5 AND SUM(amount) > 1000.0)
    OR (COUNT(*) > 10 AND AVG(amount) > 100.0)
    OR SUM(amount) > 5000.0;  -- Filter after grouping
```

### HAVING vs WHERE
- **WHERE**: Filters individual rows BEFORE grouping
- **HAVING**: Filters groups AFTER aggregation

```sql
SELECT product_category, brand,
       COUNT(*) as sales_count,
       SUM(amount) as total_revenue,
       AVG(amount) as avg_sale_amount
FROM sales
WHERE sale_date >= '2024-01-01'        -- Filter rows before grouping
  AND amount > 0                       -- Only positive amounts
GROUP BY product_category, brand
HAVING (COUNT(*) >= 50 AND SUM(amount) >= 10000.0)  -- Filter groups after aggregation
    OR (brand = 'Premium' AND COUNT(*) >= 20)
    OR AVG(amount) > 200.0;
```

## Complete List of Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all rows in group | `COUNT(*)` |
| `COUNT(column)` | Count non-null values | `COUNT(email)` |
| `COUNT_DISTINCT(column)` | Count unique values | `COUNT_DISTINCT(product_id)` |
| `SUM(column)` | Sum numeric values | `SUM(amount)` |
| `AVG(column)` | Average of numeric values | `AVG(price)` |
| `MIN(column)` | Minimum value | `MIN(order_date)` |
| `MAX(column)` | Maximum value | `MAX(order_date)` |
| `STDDEV(column)` | Standard deviation | `STDDEV(amount)` |
| `VARIANCE(column)` | Variance | `VARIANCE(amount)` |
| `FIRST(column)` | First value in group | `FIRST(status)` |
| `LAST(column)` | Last value in group | `LAST(status)` |
| `STRING_AGG(column, sep)` | Concatenate strings | `STRING_AGG(name, ', ')` |

## Real-World Aggregation Examples

### Customer Analytics
```sql
-- Customer spending analysis
SELECT
    customer_id,
    COUNT(*) as total_orders,
    SUM(amount) as lifetime_value,
    AVG(amount) as avg_order_value,
    MIN(order_date) as first_order,
    MAX(order_date) as last_order,
    COUNT_DISTINCT(product_category) as categories_purchased
FROM orders
WHERE order_status = 'completed'
GROUP BY customer_id
HAVING COUNT(*) >= 3  -- Customers with 3+ orders only
ORDER BY lifetime_value DESC;
```

### Product Performance
```sql
-- Top-performing products by category
SELECT
    category,
    product_name,
    COUNT(*) as times_sold,
    SUM(quantity) as total_quantity,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_price
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY category, product_name
HAVING COUNT(*) >= 10  -- Products sold at least 10 times
ORDER BY category, total_revenue DESC;
```

### Time-Based Analytics
```sql
-- Monthly sales trends
SELECT
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    COUNT(*) as total_orders,
    COUNT_DISTINCT(customer_id) as unique_customers,
    SUM(amount) as monthly_revenue,
    AVG(amount) as avg_order_value,
    STDDEV(amount) as revenue_volatility
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;
```

### Statistical Analysis
```sql
-- Product pricing statistics by category
SELECT
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    STDDEV(price) as price_std_dev,
    VARIANCE(price) as price_variance,
    STRING_AGG(brand, ', ') as available_brands
FROM products
GROUP BY category
HAVING COUNT(*) >= 5  -- Categories with at least 5 products
ORDER BY avg_price DESC;
```

## Common Aggregation Patterns

### Ranking and Top N
```sql
-- Top 10 customers by spending
SELECT customer_id, SUM(amount) as total_spent
FROM orders
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;
```

### Percentage Calculations
```sql
-- Category contribution to total sales
SELECT
    category,
    SUM(amount) as category_total,
    SUM(amount) * 100.0 / (
        SELECT SUM(amount) FROM orders
    ) as percentage_of_total
FROM orders
GROUP BY category
ORDER BY percentage_of_total DESC;
```

### Conditional Aggregation
```sql
-- Count orders by status within each customer
SELECT
    customer_id,
    COUNT(*) as total_orders,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_orders,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_orders,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancelled_orders
FROM orders
GROUP BY customer_id;
```

## Performance Tips

1. **Use indexes on GROUP BY columns** for better performance
2. **Filter with WHERE before GROUP BY** when possible
3. **LIMIT results** when you don't need all groups
4. **Use DISTINCT carefully** - it's expensive for large datasets
5. **Consider streaming vs batch** for time-sensitive aggregations

## Next Steps

- [Window functions](window-analysis.md) - Time-based windowing for streaming data
- [Join aggregated data](join-streams.md) - Combine aggregated results
- [Essential functions](../functions/essential.md) - More functions for calculations