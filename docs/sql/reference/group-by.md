# GROUP BY SQL Reference

Quick reference for GROUP BY operations in VeloStream SQL.

## Syntax

```sql
SELECT column1, aggregate_function(column2), ...
FROM table_name
WHERE condition
GROUP BY column1, expression, ...
HAVING aggregate_condition
[WINDOW window_spec]
[EMIT {CHANGES | FINAL}]
ORDER BY column1, ...
```

### EMIT Modes (Optional)

VeloStream supports KSQL-style EMIT clauses to control when GROUP BY results are emitted:

- **`EMIT CHANGES`** - Continuous emission of results as data arrives (CDC-style)
- **`EMIT FINAL`** - Emission of final results only when windows close (requires WINDOW clause)
- **No EMIT clause** - Uses intelligent defaults based on query structure

#### Default Behavior (when EMIT is not specified):

- **With WINDOW clause**: Defaults to windowed aggregation (accumulate → emit when window closes)  
- **Without WINDOW clause**: Defaults to continuous aggregation (emit updates immediately)

## Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all rows | `SELECT COUNT(*) FROM orders GROUP BY customer_id` |
| `COUNT(column)` | Count non-null values | `SELECT COUNT(amount) FROM orders GROUP BY customer_id` |
| `COUNT_DISTINCT(column)` | Count unique values | `SELECT COUNT_DISTINCT(product_id) FROM orders GROUP BY customer_id` |
| `SUM(column)` | Sum numeric values | `SELECT SUM(amount) FROM orders GROUP BY customer_id` |
| `AVG(column)` | Average of numeric values | `SELECT AVG(amount) FROM orders GROUP BY customer_id` |
| `MIN(column)` | Minimum value | `SELECT MIN(order_date) FROM orders GROUP BY customer_id` |
| `MAX(column)` | Maximum value | `SELECT MAX(order_date) FROM orders GROUP BY customer_id` |
| `STDDEV(column)` | Standard deviation | `SELECT STDDEV(amount) FROM orders GROUP BY customer_id` |
| `VARIANCE(column)` | Variance | `SELECT VARIANCE(amount) FROM orders GROUP BY customer_id` |
| `FIRST(column)` | First value in group | `SELECT FIRST(status) FROM orders GROUP BY customer_id` |
| `LAST(column)` | Last value in group | `SELECT LAST(status) FROM orders GROUP BY customer_id` |
| `STRING_AGG(column, separator)` | Concatenate strings | `SELECT STRING_AGG(product_name, ', ') FROM orders GROUP BY customer_id` |

## Examples

### Basic Grouping
```sql
-- Count orders per customer
SELECT customer_id, COUNT(*) as order_count
FROM orders 
GROUP BY customer_id;

-- Sales by category
SELECT category, SUM(amount) as total_sales
FROM products 
GROUP BY category;
```

### Multiple Columns
```sql
-- Orders by customer and status
SELECT customer_id, status, COUNT(*) as count
FROM orders 
GROUP BY customer_id, status;
```

### With HAVING
```sql
-- High-value customers (>$1000 total)
SELECT customer_id, SUM(amount) as total
FROM orders 
GROUP BY customer_id 
HAVING SUM(amount) > 1000;
```

### Expression-based Grouping
```sql
-- Monthly sales summary
SELECT 
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    COUNT(*) as orders,
    SUM(amount) as revenue
FROM orders 
GROUP BY YEAR(order_date), MONTH(order_date);
```

### Statistical Analysis
```sql
-- Price analysis by category
SELECT 
    category,
    COUNT(*) as products,
    AVG(price) as avg_price,
    STDDEV(price) as price_stddev,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM products 
GROUP BY category;
```

### String Aggregation
```sql
-- Customer product list
SELECT 
    customer_id,
    COUNT(*) as order_count,
    STRING_AGG(product_name, ', ') as products
FROM order_items 
GROUP BY customer_id;
```

### EMIT Mode Examples

#### Continuous Aggregation (EMIT CHANGES)
```sql
-- Real-time dashboard - updates emitted immediately  
SELECT 
    category,
    COUNT(*) as item_count,
    AVG(price) as avg_price
FROM products_topic
GROUP BY category
EMIT CHANGES;
```

#### Windowed Aggregation (EMIT FINAL)
```sql
-- Hourly sales reports - results emitted only when hour completes
SELECT 
    category,
    COUNT(*) as hourly_sales,
    SUM(amount) as hourly_revenue  
FROM sales_stream
GROUP BY category
WINDOW TUMBLING(1h)
EMIT FINAL;
```

#### Default Behavior Examples
```sql
-- Default: Continuous aggregation (no WINDOW clause)
SELECT customer_id, COUNT(*) 
FROM orders 
GROUP BY customer_id;  -- Updates emitted immediately

-- Default: Windowed aggregation (has WINDOW clause)
SELECT customer_id, COUNT(*)
FROM orders 
GROUP BY customer_id 
WINDOW TUMBLING(5m);  -- Results emitted when window closes
```

## Best Practices

1. **Always include grouping columns in SELECT**: All non-aggregate columns in SELECT must be in GROUP BY
2. **Use HAVING for aggregate conditions**: Use WHERE for pre-aggregation filtering, HAVING for post-aggregation
3. **Choose appropriate EMIT mode**:
   - Use `EMIT CHANGES` for real-time dashboards and CDC scenarios
   - Use `EMIT FINAL` with windowed queries for batch reports  
   - Omit EMIT clause to use intelligent defaults
4. **Consider performance**: GROUP BY operations can be memory-intensive with many groups
5. **Handle NULLs appropriately**: NULL values are grouped together and excluded from most aggregates (except COUNT(*))
6. **EMIT FINAL validation**: `EMIT FINAL` can only be used with `WINDOW` clause - system validates this automatically

## Common Patterns

### Top N per Group
```sql
-- Top spending customer per category (requires subquery)
SELECT * FROM (
    SELECT customer_id, category, SUM(amount) as total,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(amount) DESC) as rn
    FROM orders 
    GROUP BY customer_id, category
) WHERE rn = 1;
```

### Percentage Calculations
```sql
-- Category sales percentage
SELECT 
    category,
    SUM(amount) as category_total,
    SUM(amount) * 100.0 / SUM(SUM(amount)) OVER () as percentage
FROM orders 
GROUP BY category;
```

### Time-based Analysis
```sql
-- Daily vs Monthly comparison
SELECT 
    DATE(order_date) as order_day,
    COUNT(*) as daily_orders,
    SUM(COUNT(*)) OVER (PARTITION BY YEAR(order_date), MONTH(order_date)) as monthly_orders
FROM orders 
GROUP BY DATE(order_date);
```