# Filter Streaming Data

Learn how to filter streaming data with WHERE clauses and complex conditions using logical operators.

## Basic Logical Operators

### AND Operator - Both Conditions Must Be True

```sql
-- Basic AND usage
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 100.0 AND status = 'completed';
```

```sql
-- Multiple AND conditions
SELECT product_id, name, price, category
FROM products
WHERE price > 50.0
  AND category = 'electronics'
  AND in_stock = true;
```

```sql
-- AND with different data types
SELECT user_id, login_time, session_duration
FROM user_sessions
WHERE session_duration > 300
  AND login_time > '2024-01-01'
  AND user_type = 'premium';
```

### OR Operator - At Least One Condition Must Be True

```sql
-- Basic OR usage
SELECT customer_id, status, priority
FROM orders
WHERE status = 'urgent' OR priority = 'high';
```

```sql
-- Multiple OR conditions
SELECT user_id, account_type, subscription_status
FROM users
WHERE account_type = 'premium'
   OR account_type = 'enterprise'
   OR subscription_status = 'trial';
```

```sql
-- OR with mixed conditions
SELECT order_id, amount, discount_code
FROM orders
WHERE amount > 1000.0
   OR discount_code IS NOT NULL
   OR customer_tier = 'VIP';
```

## Compound Conditions with AND/OR

### Operator Precedence
**Important:** AND has higher precedence than OR. Use parentheses for clarity!

```sql
-- Without parentheses (AND evaluated first)
SELECT customer_id, amount, status, priority
FROM orders
WHERE status = 'pending' AND amount > 100.0 OR priority = 'urgent';
-- This is equivalent to: (status = 'pending' AND amount > 100.0) OR priority = 'urgent'
```

```sql
-- With explicit parentheses for clarity (recommended)
SELECT customer_id, amount, status, priority
FROM orders
WHERE (status = 'pending' AND amount > 100.0) OR priority = 'urgent';
```

```sql
-- Different grouping changes the logic
SELECT customer_id, amount, status, priority
FROM orders
WHERE status = 'pending' AND (amount > 100.0 OR priority = 'urgent');
```

## Real-World Complex Filtering Examples

### E-commerce Order Filtering
```sql
SELECT order_id, customer_id, amount, status, payment_method
FROM orders
WHERE (status = 'confirmed' OR status = 'shipped')
  AND (payment_method = 'credit_card' OR payment_method = 'paypal')
  AND amount > 50.0
  AND customer_id NOT IN (999, 1000);  -- Exclude test customers
```

### User Activity Analysis
```sql
SELECT user_id, action_type, page_url, session_id
FROM user_actions
WHERE (action_type = 'click' OR action_type = 'view' OR action_type = 'purchase')
  AND (page_url LIKE '%product%' OR page_url LIKE '%category%')
  AND session_duration > 60
  AND NOT (user_agent LIKE '%bot%' OR user_agent LIKE '%crawler%');
```

### IoT Sensor Data Filtering
```sql
SELECT sensor_id, reading_value, reading_type, alert_level
FROM sensor_readings
WHERE (reading_type = 'temperature' AND reading_value > 80.0)
   OR (reading_type = 'humidity' AND reading_value > 90.0)
   OR (reading_type = 'pressure' AND (reading_value < 980.0 OR reading_value > 1050.0))
   AND sensor_status = 'active'
   AND reading_timestamp > NOW() - INTERVAL '1' HOUR;
```

## Logical Operators in JOIN Conditions

```sql
-- JOIN with compound ON conditions
SELECT o.order_id, o.amount, c.customer_name, p.product_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN products p ON o.product_id = p.product_id
                     AND (p.category = 'electronics' OR p.category = 'computers')
                     AND p.price > 100.0
WHERE o.status = 'completed'
  AND (o.amount > 500.0 OR c.customer_tier = 'premium');
```

```sql
-- Complex JOIN with multiple conditions
SELECT u.user_id, u.username, p.product_name, o.order_date
FROM users u
LEFT JOIN orders o ON u.user_id = o.customer_id
                   AND o.status = 'completed'
                   AND o.order_date > '2024-01-01'
LEFT JOIN products p ON o.product_id = p.product_id
                     AND (p.in_stock = true OR p.backorder_allowed = true)
WHERE (u.account_status = 'active' OR u.account_status = 'premium')
  AND (o.order_id IS NOT NULL OR u.last_login > NOW() - INTERVAL '30' DAYS);
```

## Comparison Operators Quick Reference

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `status = 'active'` |
| `!=` or `<>` | Not equal to | `status != 'cancelled'` |
| `>` | Greater than | `amount > 100.0` |
| `>=` | Greater than or equal | `price >= 50.0` |
| `<` | Less than | `quantity < 10` |
| `<=` | Less than or equal | `discount <= 0.2` |
| `LIKE` | Pattern matching | `name LIKE 'John%'` |
| `IN` | Match any value in list | `status IN ('active', 'pending')` |
| `NOT IN` | Don't match any value in list | `id NOT IN (1, 2, 3)` |
| `IS NULL` | Check for null values | `email IS NULL` |
| `IS NOT NULL` | Check for non-null values | `phone IS NOT NULL` |
| `BETWEEN` | Range check | `amount BETWEEN 100 AND 500` |

## Best Practices

1. **Use parentheses** to make complex conditions clear
2. **Put most selective conditions first** for better performance
3. **Use IN() instead of multiple OR conditions** when checking multiple values
4. **Consider NOT IN() carefully** - it excludes rows with NULL values
5. **Use BETWEEN for ranges** instead of `>= AND <=`

## Pattern Examples

### Filter by Multiple Values
```sql
-- Instead of multiple OR conditions
WHERE status = 'active' OR status = 'pending' OR status = 'processing'

-- Use IN (cleaner and often faster)
WHERE status IN ('active', 'pending', 'processing')
```

### Range Filtering
```sql
-- Instead of
WHERE amount >= 100 AND amount <= 500

-- Use BETWEEN (more readable)
WHERE amount BETWEEN 100 AND 500
```

### Text Pattern Matching
```sql
-- Starts with
WHERE customer_name LIKE 'John%'

-- Contains
WHERE product_description LIKE '%wireless%'

-- Ends with
WHERE email LIKE '%@gmail.com'
```

## Next Steps

- [Aggregate filtered data](aggregate-data.md) - GROUP BY and calculations
- [Join filtered streams](join-streams.md) - Combine multiple data sources
- [Essential SQL functions](../functions/essential.md) - Common functions for filtering