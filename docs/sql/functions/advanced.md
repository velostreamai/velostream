# Advanced and Utility Functions

Complete reference for utility functions, advanced operations, and specialized functions in VeloStream.

## Null Handling Functions

### COALESCE - First Non-Null Value

```sql
-- Provide defaults for missing data
SELECT
    customer_id,
    COALESCE(preferred_name, first_name, 'Unknown Customer') as display_name,
    COALESCE(mobile_phone, home_phone, work_phone, 'No phone') as contact_phone,
    COALESCE(email, 'no-email@unknown.com') as contact_email
FROM customers;

-- Chain multiple fallback values
SELECT
    product_id,
    COALESCE(sale_price, list_price, msrp, 0.00) as display_price,
    COALESCE(short_description, long_description, product_name, 'No description') as description
FROM products;
```

### NULLIF - Return NULL if Values Equal

```sql
-- Convert zero values to NULL for calculations
SELECT
    order_id,
    discount_rate,
    NULLIF(discount_rate, 0.0) as effective_discount,  -- NULL if 0
    amount / NULLIF(discount_rate, 0.0) as undiscounted_amount
FROM orders;

-- Prevent division by zero
SELECT
    product_id,
    total_views,
    total_clicks,
    total_clicks * 100.0 / NULLIF(total_views, 0) as click_through_rate
FROM marketing_metrics;
```

### ISNULL and IS NOT NULL - Null Checking

```sql
-- Filter based on null status
SELECT *
FROM customers
WHERE email IS NOT NULL
  AND phone IS NOT NULL
  AND preferred_name IS NULL;

-- Count null vs non-null values
SELECT
    COUNT(*) as total_records,
    COUNT(email) as with_email,
    COUNT(*) - COUNT(email) as without_email,
    COUNT(phone) as with_phone,
    COUNT(address) as with_address
FROM customers;
```

## Conditional Logic

### CASE WHEN - Conditional Expressions

```sql
-- Simple case expressions
SELECT
    customer_id,
    total_orders,
    CASE
        WHEN total_orders >= 50 THEN 'VIP'
        WHEN total_orders >= 20 THEN 'Premium'
        WHEN total_orders >= 5 THEN 'Regular'
        ELSE 'New'
    END as customer_tier
FROM customer_summary;

-- Complex conditional logic
SELECT
    order_id,
    order_date,
    amount,
    status,
    CASE
        WHEN status = 'cancelled' THEN 'No Revenue'
        WHEN status = 'pending' AND DATEDIFF('days', order_date, NOW()) > 7 THEN 'At Risk'
        WHEN status = 'completed' AND amount > 1000 THEN 'High Value'
        WHEN status = 'completed' THEN 'Standard'
        ELSE 'Unknown Status'
    END as order_category,
    CASE
        WHEN EXTRACT('DOW', order_date) IN (0, 6) THEN 'Weekend'
        WHEN EXTRACT('HOUR', order_date) BETWEEN 9 AND 17 THEN 'Business Hours'
        ELSE 'After Hours'
    END as order_timing
FROM orders;
```

### Searched CASE vs Simple CASE

```sql
-- Searched CASE (more flexible)
SELECT
    product_id,
    CASE
        WHEN price > 1000 AND category = 'electronics' THEN 'Premium Electronics'
        WHEN price > 500 THEN 'Mid-Range'
        WHEN price > 100 THEN 'Budget'
        ELSE 'Economy'
    END as price_tier
FROM products;

-- Simple CASE (for exact matches)
SELECT
    order_id,
    status,
    CASE status
        WHEN 'pending' THEN 'Awaiting Processing'
        WHEN 'shipped' THEN 'In Transit'
        WHEN 'delivered' THEN 'Complete'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE 'Unknown Status'
    END as status_description
FROM orders;
```

## Timestamp and Processing Functions

### TIMESTAMP() - Processing Timestamp

```sql
-- Add processing timestamps to records
SELECT
    order_id,
    customer_id,
    amount,
    TIMESTAMP() as processed_at,
    DATEDIFF('milliseconds', order_timestamp, TIMESTAMP()) as processing_delay_ms
FROM orders;

-- Track processing pipeline stages
SELECT
    event_id,
    event_data,
    TIMESTAMP() as enrichment_timestamp,
    'enriched_with_customer_data' as processing_stage
FROM events
JOIN customers ON JSON_VALUE(event_data, '$.user_id') = customers.customer_id;
```

## Type Conversion Functions

### CAST - Type Conversion

```sql
-- Convert between data types
SELECT
    order_id,
    CAST(order_id AS VARCHAR) as order_id_string,
    CAST(amount AS INTEGER) as amount_rounded,
    CAST(order_date AS VARCHAR) as order_date_string,
    CAST('123.45' AS FLOAT) as parsed_number
FROM orders;

-- Safe type conversion with error handling
SELECT
    user_input,
    CASE
        WHEN user_input ~ '^[0-9]+$' THEN CAST(user_input AS INTEGER)
        ELSE NULL
    END as parsed_integer,
    CASE
        WHEN user_input ~ '^[0-9]+(\\.[0-9]+)?$' THEN CAST(user_input AS FLOAT)
        ELSE NULL
    END as parsed_float
FROM user_form_data;
```

## Advanced String and Pattern Functions

### REGEXP - Regular Expression Matching

```sql
-- Pattern validation
SELECT
    customer_id,
    email,
    phone,
    CASE
        WHEN email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 'Valid Email'
        ELSE 'Invalid Email'
    END as email_validation,
    CASE
        WHEN phone ~ '^\\+?1?[0-9]{10}$' THEN 'Valid Phone'
        ELSE 'Invalid Phone'
    END as phone_validation
FROM customers;

-- Extract patterns from text
SELECT
    log_entry,
    CASE
        WHEN log_entry ~ '\\bERROR\\b' THEN 'Error'
        WHEN log_entry ~ '\\bWARN(ING)?\\b' THEN 'Warning'
        WHEN log_entry ~ '\\bINFO\\b' THEN 'Info'
        ELSE 'Unknown'
    END as log_level
FROM application_logs;
```

### LPAD and RPAD - String Padding

```sql
-- Pad strings to fixed width
SELECT
    order_id,
    LPAD(CAST(order_id AS VARCHAR), 8, '0') as padded_order_id,
    RPAD(customer_name, 20, ' ') as formatted_name,
    LPAD('$' || CAST(amount AS VARCHAR), 10, ' ') as formatted_amount
FROM orders
JOIN customers ON orders.customer_id = customers.customer_id;

-- Create fixed-width reports
SELECT
    RPAD(product_name, 30, '.') as product_name_padded,
    LPAD(CAST(ROUND(price, 2) AS VARCHAR), 8, ' ') as price_padded,
    LPAD(CAST(inventory_count AS VARCHAR), 6, ' ') as inventory_padded
FROM products
ORDER BY product_name;
```

## Set and Range Operations

### IN and NOT IN - Set Membership

```sql
-- Filter using set membership
SELECT *
FROM orders
WHERE status IN ('pending', 'processing', 'shipped')
  AND customer_tier IN ('premium', 'vip')
  AND product_id NOT IN (1001, 1002, 1003);  -- Exclude test products

-- Dynamic set filtering
SELECT *
FROM products
WHERE category IN (
    SELECT DISTINCT category
    FROM best_selling_products
    WHERE sales_rank <= 10
);
```

### BETWEEN - Range Operations

```sql
-- Numeric and date ranges
SELECT *
FROM orders
WHERE amount BETWEEN 100 AND 500
  AND order_date BETWEEN '2024-01-01' AND '2024-12-31'
  AND EXTRACT('HOUR', order_timestamp) BETWEEN 9 AND 17;

-- Character ranges (alphabetical)
SELECT *
FROM customers
WHERE UPPER(last_name) BETWEEN 'A' AND 'M'
ORDER BY last_name;
```

## Advanced Analytical Functions

### GREATEST and LEAST - Min/Max of Values

```sql
-- Find maximum/minimum across columns
SELECT
    product_id,
    online_price,
    store_price,
    wholesale_price,
    GREATEST(online_price, store_price, wholesale_price) as highest_price,
    LEAST(online_price, store_price, wholesale_price) as lowest_price,
    GREATEST(online_price, store_price, wholesale_price) -
    LEAST(online_price, store_price, wholesale_price) as price_range
FROM product_pricing;

-- Safe division with bounds
SELECT
    order_id,
    total_amount,
    discount_amount,
    LEAST(discount_amount, total_amount) as applied_discount,  -- Can't discount more than total
    GREATEST(total_amount - discount_amount, 0) as final_amount  -- Can't go below zero
FROM orders;
```

## Real-World Advanced Function Examples

### Data Quality Scoring

```sql
-- Calculate comprehensive data quality scores
SELECT
    customer_id,
    -- Completeness score (0-100)
    ROUND(
        (CASE WHEN first_name IS NOT NULL THEN 10 ELSE 0 END +
         CASE WHEN last_name IS NOT NULL THEN 10 ELSE 0 END +
         CASE WHEN email IS NOT NULL THEN 20 ELSE 0 END +
         CASE WHEN phone IS NOT NULL THEN 15 ELSE 0 END +
         CASE WHEN address IS NOT NULL THEN 15 ELSE 0 END +
         CASE WHEN birth_date IS NOT NULL THEN 10 ELSE 0 END +
         CASE WHEN preferred_contact IS NOT NULL THEN 10 ELSE 0 END +
         CASE WHEN customer_since IS NOT NULL THEN 10 ELSE 0 END),
        0
    ) as completeness_score,
    -- Validity score
    ROUND(
        (CASE WHEN email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 25 ELSE 0 END +
         CASE WHEN phone ~ '^\\+?1?[0-9]{10}$' THEN 25 ELSE 0 END +
         CASE WHEN birth_date <= CURRENT_DATE AND birth_date > '1900-01-01' THEN 25 ELSE 0 END +\n         CASE WHEN customer_since <= CURRENT_DATE THEN 25 ELSE 0 END),\n        0\n    ) as validity_score,\n    CASE\n        WHEN COALESCE(\n            (CASE WHEN first_name IS NOT NULL THEN 10 ELSE 0 END +\n             CASE WHEN email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 25 ELSE 0 END), 0\n        ) >= 80 THEN 'High Quality'\n        WHEN COALESCE(\n            (CASE WHEN first_name IS NOT NULL THEN 10 ELSE 0 END +\n             CASE WHEN email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 25 ELSE 0 END), 0\n        ) >= 50 THEN 'Medium Quality'\n        ELSE 'Low Quality'\n    END as data_quality_tier\nFROM customers;\n```\n\n### Dynamic Pricing Logic\n\n```sql\n-- Complex dynamic pricing with multiple factors\nSELECT\n    product_id,\n    base_price,\n    inventory_level,\n    demand_score,\n    competition_price,\n    -- Calculate dynamic price with business rules\n    CASE\n        WHEN inventory_level = 0 THEN NULL  -- Out of stock\n        WHEN competition_price IS NULL THEN base_price * 1.1  -- No competition data\n        ELSE\n            GREATEST(  -- Don't go below cost\n                base_price * 0.8,  -- Minimum 20% margin\n                LEAST(  -- Don't price too high\n                    base_price * 1.5,  -- Maximum 50% markup\n                    CASE\n                        WHEN inventory_level < 10 AND demand_score > 80 THEN\n                            base_price * 1.3  -- High demand, low inventory\n                        WHEN inventory_level > 100 AND demand_score < 20 THEN\n                            GREATEST(base_price * 0.9, competition_price * 0.95)  -- Clear inventory\n                        WHEN competition_price > 0 THEN\n                            (base_price + competition_price) / 2  -- Match market\n                        ELSE base_price\n                    END\n                )\n            )\n    END as recommended_price,\n    -- Price change reason\n    CASE\n        WHEN inventory_level = 0 THEN 'Out of Stock'\n        WHEN inventory_level < 10 AND demand_score > 80 THEN 'High Demand Premium'\n        WHEN inventory_level > 100 AND demand_score < 20 THEN 'Inventory Clearance'\n        WHEN competition_price IS NULL THEN 'No Competition Data'\n        WHEN ABS(base_price - competition_price) / base_price > 0.2 THEN 'Market Adjustment'\n        ELSE 'Standard Pricing'\n    END as pricing_reason\nFROM product_pricing_data;\n```\n\n### Customer Segmentation\n\n```sql\n-- Multi-dimensional customer segmentation\nSELECT\n    customer_id,\n    total_spent,\n    order_frequency,\n    avg_order_value,\n    days_since_last_order,\n    -- RFM Analysis\n    NTILE(5) OVER (ORDER BY days_since_last_order ASC) as recency_score,\n    NTILE(5) OVER (ORDER BY order_frequency DESC) as frequency_score,\n    NTILE(5) OVER (ORDER BY total_spent DESC) as monetary_score,\n    -- Overall customer tier\n    CASE\n        WHEN NTILE(5) OVER (ORDER BY total_spent DESC) >= 4\n         AND NTILE(5) OVER (ORDER BY order_frequency DESC) >= 4\n         AND NTILE(5) OVER (ORDER BY days_since_last_order ASC) >= 3\n        THEN 'Champions'\n        WHEN NTILE(5) OVER (ORDER BY total_spent DESC) >= 3\n         AND NTILE(5) OVER (ORDER BY order_frequency DESC) >= 3\n        THEN 'Loyal Customers'\n        WHEN NTILE(5) OVER (ORDER BY total_spent DESC) >= 4\n         AND NTILE(5) OVER (ORDER BY days_since_last_order ASC) <= 2\n        THEN 'At Risk'\n        WHEN NTILE(5) OVER (ORDER BY days_since_last_order ASC) <= 2\n        THEN 'Lost Customers'\n        ELSE 'Need Attention'\n    END as customer_segment,\n    -- Recommended action\n    CASE\n        WHEN NTILE(5) OVER (ORDER BY total_spent DESC) >= 4 THEN 'VIP Treatment'\n        WHEN NTILE(5) OVER (ORDER BY days_since_last_order ASC) <= 2 THEN 'Win-Back Campaign'\n        WHEN order_frequency = 1 THEN 'New Customer Onboarding'\n        ELSE 'Standard Marketing'\n    END as recommended_action\nFROM customer_metrics;\n```\n\n### Error Handling and Data Recovery\n\n```sql\n-- Robust error handling in data processing\nSELECT\n    record_id,\n    raw_data,\n    -- Safe numeric conversion\n    CASE\n        WHEN raw_amount ~ '^[0-9]+(\\.[0-9]+)?$' THEN\n            CAST(raw_amount AS FLOAT)\n        WHEN raw_amount ~ '^\\$[0-9]+(\\.[0-9]+)?$' THEN\n            CAST(SUBSTRING(raw_amount, 2) AS FLOAT)\n        ELSE NULL\n    END as clean_amount,\n    -- Safe date parsing\n    CASE\n        WHEN raw_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN\n            CAST(raw_date AS DATE)\n        WHEN raw_date ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN\n            CAST(\n                SUBSTRING(raw_date, 7, 4) || '-' ||\n                SUBSTRING(raw_date, 1, 2) || '-' ||\n                SUBSTRING(raw_date, 4, 2)\n                AS DATE\n            )\n        ELSE NULL\n    END as clean_date,\n    -- Data quality flags\n    CASE\n        WHEN raw_amount IS NULL OR raw_amount = '' THEN 'MISSING_AMOUNT'\n        WHEN NOT (raw_amount ~ '^[\\$]?[0-9]+(\\.[0-9]+)?$') THEN 'INVALID_AMOUNT'\n        ELSE 'VALID_AMOUNT'\n    END as amount_status,\n    CASE\n        WHEN raw_date IS NULL OR raw_date = '' THEN 'MISSING_DATE'\n        WHEN NOT (raw_date ~ '^([0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{2}/[0-9]{2}/[0-9]{4})$') THEN 'INVALID_DATE'\n        ELSE 'VALID_DATE'\n    END as date_status\nFROM raw_import_data;\n```\n\n## Performance Tips\n\n### Efficient Conditional Logic\n\n```sql\n-- ✅ Good: Use CASE for multiple related conditions\nSELECT\n    customer_id,\n    CASE total_spent\n        WHEN total_spent > 10000 THEN 'VIP'\n        WHEN total_spent > 1000 THEN 'Premium'\n        ELSE 'Standard'\n    END as tier\nFROM customers;\n\n-- ⚠️ Less efficient: Multiple separate CASE statements\nSELECT\n    customer_id,\n    CASE WHEN total_spent > 10000 THEN 'VIP' END as vip_flag,\n    CASE WHEN total_spent > 1000 AND total_spent <= 10000 THEN 'Premium' END as premium_flag\nFROM customers;\n```\n\n### NULL Handling Performance\n\n```sql\n-- ✅ Good: Use COALESCE for multiple fallbacks\nSELECT COALESCE(preferred_name, first_name, 'Unknown') as display_name\nFROM customers;\n\n-- ⚠️ Less efficient: Nested CASE statements\nSELECT\n    CASE\n        WHEN preferred_name IS NOT NULL THEN preferred_name\n        WHEN first_name IS NOT NULL THEN first_name\n        ELSE 'Unknown'\n    END as display_name\nFROM customers;\n```\n\n## Quick Reference\n\n### Null Handling\n| Function | Purpose | Example |\n|----------|---------|--------|\n| `COALESCE(a, b, c)` | First non-null value | `COALESCE(phone, email, 'N/A')` |\n| `NULLIF(a, b)` | NULL if values equal | `NULLIF(discount, 0)` |\n| `expr IS NULL` | Check for null | `email IS NULL` |\n| `expr IS NOT NULL` | Check for non-null | `phone IS NOT NULL` |\n\n### Conditional Logic\n| Function | Purpose | Example |\n|----------|---------|--------|\n| `CASE WHEN ... THEN ... END` | Conditional expressions | `CASE WHEN amount > 100 THEN 'High' ELSE 'Low' END` |\n| `CASE expr WHEN ... THEN ...` | Simple case matching | `CASE status WHEN 'A' THEN 'Active' END` |\n\n### Type Conversion\n| Function | Purpose | Example |\n|----------|---------|--------|\n| `CAST(expr AS type)` | Convert data types | `CAST('123' AS INTEGER)` |\n| `TIMESTAMP()` | Current processing time | `TIMESTAMP()` |\n\n### Set Operations\n| Operator | Purpose | Example |\n|----------|---------|--------|\n| `IN (...)` | Set membership | `status IN ('A', 'B', 'C')` |\n| `NOT IN (...)` | Set exclusion | `id NOT IN (1, 2, 3)` |\n| `BETWEEN ... AND ...` | Range check | `amount BETWEEN 100 AND 500` |\n\n### Comparison Functions\n| Function | Purpose | Example |\n|----------|---------|--------|\n| `GREATEST(a, b, c)` | Maximum value | `GREATEST(price1, price2, price3)` |\n| `LEAST(a, b, c)` | Minimum value | `LEAST(discount1, discount2)` |\n\n## Next Steps\n\n- [Essential functions](essential.md) - Most commonly used functions\n- [Data transformation](../by-task/transform-data.md) - Advanced data transformation patterns\n- [Pattern detection](../by-task/detect-patterns.md) - Using conditional logic for pattern detection