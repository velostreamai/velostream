# Essential SQL Functions

The 10 most commonly used functions in FerrisStreams with practical examples you can use immediately.

## 1. COUNT - Count Records

**Most common usage:**
```sql
-- Count all records
SELECT COUNT(*) as total_orders FROM orders;

-- Count non-null values
SELECT COUNT(email) as customers_with_email FROM customers;

-- Count unique values
SELECT COUNT_DISTINCT(customer_id) as unique_customers FROM orders;
```

**Real-world example:**
```sql
-- Daily order counts
SELECT DATE_FORMAT(_timestamp, '%Y-%m-%d') as date, COUNT(*) as daily_orders
FROM orders
GROUP BY DATE_FORMAT(_timestamp, '%Y-%m-%d');
```

## 2. SUM - Calculate Totals

```sql
-- Total revenue
SELECT SUM(amount) as total_revenue FROM orders;

-- Revenue by customer
SELECT customer_id, SUM(amount) as customer_total
FROM orders
GROUP BY customer_id;
```

**Real-world example:**
```sql
-- Monthly revenue trends
SELECT
    EXTRACT('YEAR', _timestamp) as year,
    EXTRACT('MONTH', _timestamp) as month,
    SUM(amount) as monthly_revenue
FROM orders
GROUP BY EXTRACT('YEAR', _timestamp), EXTRACT('MONTH', _timestamp);
```

## 3. AVG - Calculate Averages

```sql
-- Average order value
SELECT AVG(amount) as avg_order_value FROM orders;

-- Average by category
SELECT category, AVG(amount) as avg_amount
FROM orders
GROUP BY category;
```

## 4. UPPER/LOWER - Change Text Case

```sql
-- Standardize text data
SELECT
    customer_id,
    UPPER(first_name) as first_name_upper,
    LOWER(email) as email_lower
FROM customers;
```

**Practical use - data cleanup:**
```sql
-- Clean and standardize product names
SELECT
    product_id,
    TRIM(UPPER(product_name)) as clean_product_name
FROM products
WHERE product_name IS NOT NULL;
```

## 5. CONCAT - Combine Text

```sql
-- Create full names
SELECT
    customer_id,
    CONCAT(first_name, ' ', last_name) as full_name
FROM customers;

-- Create custom IDs
SELECT
    order_id,
    CONCAT('ORDER_', order_id, '_', customer_id) as tracking_id
FROM orders;
```

**Alternative syntax with ||:**
```sql
-- Same result with || operator
SELECT
    customer_id,
    first_name || ' ' || last_name as full_name
FROM customers;
```

## 6. NOW() - Current Timestamp

```sql
-- Add processing timestamps
SELECT
    order_id,
    amount,
    NOW() as processed_at
FROM orders;

-- Calculate time since order
SELECT
    order_id,
    DATEDIFF('minutes', order_timestamp, NOW()) as minutes_since_order
FROM orders;
```

## 7. COALESCE - Handle NULL Values

```sql
-- Provide defaults for missing data
SELECT
    customer_id,
    COALESCE(preferred_name, first_name, 'Unknown') as display_name,
    COALESCE(phone, 'No phone') as contact_phone
FROM customers;
```

**Real-world example - clean data:**
```sql
-- Ensure all customers have a usable name
SELECT
    customer_id,
    COALESCE(company_name, first_name || ' ' || last_name, email, 'Anonymous') as customer_name
FROM customers;
```

## 8. CASE WHEN - Conditional Logic

```sql
-- Categorize orders by size
SELECT
    order_id,
    amount,
    CASE
        WHEN amount < 50 THEN 'Small'
        WHEN amount < 200 THEN 'Medium'
        ELSE 'Large'
    END as order_size
FROM orders;
```

**Real-world example - customer segmentation:**
```sql
-- Customer tier classification
SELECT
    customer_id,
    SUM(amount) as total_spent,
    CASE
        WHEN SUM(amount) > 10000 THEN 'VIP'
        WHEN SUM(amount) > 1000 THEN 'Premium'
        WHEN SUM(amount) > 100 THEN 'Standard'
        ELSE 'New'
    END as customer_tier
FROM orders
GROUP BY customer_id;
```

## 9. ROUND - Format Numbers

```sql
-- Round monetary amounts
SELECT
    order_id,
    amount,
    ROUND(amount, 2) as amount_rounded,
    ROUND(amount * 1.1, 2) as amount_with_tax
FROM orders;
```

## 10. SUBSTRING - Extract Text Portions

```sql
-- Extract parts of strings
SELECT
    customer_id,
    email,
    SUBSTRING(email, POSITION('@', email) + 1) as email_domain,
    LEFT(phone, 3) as area_code
FROM customers;
```

**Real-world example - data parsing:**
```sql
-- Extract product categories from codes
SELECT
    product_id,
    product_code,
    LEFT(product_code, 3) as category_code,
    RIGHT(product_code, 4) as item_code
FROM products;
```

## Quick Reference Table

| Function | Purpose | Example |
|----------|---------|---------|
| `COUNT(*)` | Count all records | `COUNT(*) as total` |
| `COUNT(column)` | Count non-null values | `COUNT(email) as with_email` |
| `SUM(column)` | Add up numbers | `SUM(amount) as total_sales` |
| `AVG(column)` | Calculate average | `AVG(price) as avg_price` |
| `UPPER(text)` | Convert to uppercase | `UPPER(name) as name_upper` |
| `LOWER(text)` | Convert to lowercase | `LOWER(email) as email_clean` |
| `CONCAT(a, b, c)` | Join text together | `CONCAT(first, ' ', last)` |
| `NOW()` | Current timestamp | `NOW() as processed_at` |
| `COALESCE(a, b, c)` | First non-null value | `COALESCE(phone, email, 'N/A')` |
| `ROUND(number, places)` | Round numbers | `ROUND(amount, 2) as rounded` |

## Common Combinations

### Customer Analysis
```sql
SELECT
    customer_id,
    COALESCE(company_name, UPPER(first_name || ' ' || last_name)) as customer_name,
    COUNT(*) as order_count,
    SUM(amount) as total_spent,
    ROUND(AVG(amount), 2) as avg_order,
    CASE
        WHEN SUM(amount) > 5000 THEN 'VIP'
        WHEN SUM(amount) > 1000 THEN 'Premium'
        ELSE 'Standard'
    END as tier
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY customer_id;
```

### Data Cleaning
```sql
SELECT
    product_id,
    TRIM(UPPER(product_name)) as clean_name,
    COALESCE(description, 'No description') as product_desc,
    ROUND(price, 2) as price_rounded,
    CONCAT('PROD_', LPAD(product_id, 6, '0')) as product_code
FROM products;
```

## Next Steps

- [Complete function reference](../functions/) - All available functions by category
- [Aggregation guide](../by-task/aggregate-data.md) - Advanced GROUP BY and calculations
- [String processing](../functions/string.md) - Text manipulation functions
- [Date/time functions](../functions/date-time.md) - Time-based operations