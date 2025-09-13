# Transform and Clean Data

Learn how to transform, clean, and restructure streaming data using SQL transformation techniques.

## Data Type Transformations

### String to Numeric Conversions

```sql
-- Safe numeric conversions with validation
SELECT
    record_id,
    raw_amount,
    CASE
        WHEN raw_amount ~ '^[0-9]+(\.[0-9]+)?$' THEN CAST(raw_amount AS FLOAT)
        WHEN raw_amount ~ '^\$[0-9]+(\.[0-9]+)?$' THEN CAST(SUBSTRING(raw_amount, 2) AS FLOAT)
        WHEN raw_amount ~ '^[0-9]+,[0-9]+$' THEN CAST(REPLACE(raw_amount, ',', '') AS FLOAT)
        ELSE NULL
    END as clean_amount,
    CASE
        WHEN raw_amount ~ '^[0-9]+(\.[0-9]+)?$' THEN 'VALID_NUMERIC'
        WHEN raw_amount ~ '^\$[0-9]+(\.[0-9]+)?$' THEN 'CURRENCY_CONVERTED'
        WHEN raw_amount ~ '^[0-9]+,[0-9]+$' THEN 'COMMA_REMOVED'
        ELSE 'INVALID_FORMAT'
    END as conversion_status
FROM raw_data;
```

### Date Format Standardization

```sql
-- Convert various date formats to standard format
SELECT
    record_id,
    raw_date,
    CASE
        -- YYYY-MM-DD format
        WHEN raw_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
            CAST(raw_date AS DATE)
        -- MM/DD/YYYY format
        WHEN raw_date ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
            CAST(
                SUBSTRING(raw_date, -4) || '-' ||
                LPAD(SUBSTRING(raw_date, 1, POSITION('/', raw_date) - 1), 2, '0') || '-' ||
                LPAD(SUBSTRING(raw_date, POSITION('/', raw_date) + 1, 
                    POSITION('/', raw_date, POSITION('/', raw_date) + 1) - POSITION('/', raw_date) - 1), 2, '0')
                AS DATE
            )
        -- DD-MM-YYYY format
        WHEN raw_date ~ '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$' THEN
            CAST(
                SUBSTRING(raw_date, -4) || '-' ||
                LPAD(SUBSTRING(raw_date, POSITION('-', raw_date) + 1,
                    POSITION('-', raw_date, POSITION('-', raw_date) + 1) - POSITION('-', raw_date) - 1), 2, '0') || '-' ||
                LPAD(SUBSTRING(raw_date, 1, POSITION('-', raw_date) - 1), 2, '0')
                AS DATE
            )
        ELSE NULL
    END as standardized_date
FROM raw_imports;
```

## Data Cleaning Operations

### Text Cleaning and Standardization

```sql
-- Comprehensive text cleaning
SELECT
    customer_id,
    raw_name,
    raw_email,
    raw_phone,
    -- Clean and standardize names
    TRIM(UPPER(
        REGEXP_REPLACE(
            REGEXP_REPLACE(raw_name, '[^A-Za-z\s]', '', 'g'),
            '\s+', ' ', 'g'
        )
    )) as clean_name,
    -- Standardize email addresses
    LOWER(TRIM(raw_email)) as clean_email,
    -- Clean phone numbers
    REGEXP_REPLACE(
        REGEXP_REPLACE(raw_phone, '[^0-9]', '', 'g'),
        '^1?([0-9]{10})$', '\1', 'g'
    ) as clean_phone,
    -- Validation flags
    CASE
        WHEN raw_email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN true
        ELSE false
    END as is_valid_email,
    CASE
        WHEN LENGTH(REGEXP_REPLACE(raw_phone, '[^0-9]', '', 'g')) = 10 THEN true
        WHEN LENGTH(REGEXP_REPLACE(raw_phone, '[^0-9]', '', 'g')) = 11 
         AND LEFT(REGEXP_REPLACE(raw_phone, '[^0-9]', '', 'g'), 1) = '1' THEN true
        ELSE false
    END as is_valid_phone
FROM customer_raw_data;
```

### Address Standardization

```sql
-- Standardize address components
SELECT
    address_id,
    raw_address,
    -- Extract and clean street address
    TRIM(UPPER(
        REGEXP_REPLACE(
            SUBSTRING(raw_address, 1, 
                CASE
                    WHEN POSITION(',', raw_address) > 0 THEN POSITION(',', raw_address) - 1
                    ELSE LENGTH(raw_address)
                END
            ),
            '\s+', ' ', 'g'
        )
    )) as street_address,
    -- Extract city
    TRIM(UPPER(
        CASE
            WHEN POSITION(',', raw_address) > 0 THEN
                SUBSTRING(
                    raw_address,
                    POSITION(',', raw_address) + 1,
                    CASE
                        WHEN POSITION(',', raw_address, POSITION(',', raw_address) + 1) > 0
                        THEN POSITION(',', raw_address, POSITION(',', raw_address) + 1) - POSITION(',', raw_address) - 1
                        ELSE LENGTH(raw_address) - POSITION(',', raw_address)
                    END
                )
            ELSE NULL
        END
    )) as city,
    -- Extract state (last 2 characters before ZIP)
    TRIM(UPPER(
        CASE
            WHEN raw_address ~ '[A-Za-z]{2}\s+[0-9]{5}' THEN
                SUBSTRING(raw_address, '[A-Za-z]{2}(?=\s+[0-9]{5})')
            ELSE NULL
        END
    )) as state,
    -- Extract ZIP code
    TRIM(
        CASE
            WHEN raw_address ~ '[0-9]{5}(-[0-9]{4})?' THEN
                SUBSTRING(raw_address, '[0-9]{5}(-[0-9]{4})?')
            ELSE NULL
        END
    ) as zip_code
FROM addresses;
```

## Data Enrichment

### Calculated Fields and Derived Metrics

```sql
-- Create enriched customer profiles
SELECT
    customer_id,
    first_name,
    last_name,
    birth_date,
    total_orders,
    total_spent,
    first_order_date,
    last_order_date,
    -- Derived fields
    CONCAT(first_name, ' ', last_name) as full_name,
    DATEDIFF('years', birth_date, CURRENT_DATE) as age,
    CASE
        WHEN DATEDIFF('years', birth_date, CURRENT_DATE) < 25 THEN 'Gen Z'
        WHEN DATEDIFF('years', birth_date, CURRENT_DATE) < 41 THEN 'Millennial'
        WHEN DATEDIFF('years', birth_date, CURRENT_DATE) < 57 THEN 'Gen X'
        ELSE 'Boomer'
    END as generation,
    total_spent / NULLIF(total_orders, 0) as avg_order_value,
    DATEDIFF('days', first_order_date, last_order_date) as customer_lifespan_days,
    total_orders * 365.0 / NULLIF(DATEDIFF('days', first_order_date, CURRENT_DATE), 0) as orders_per_year,
    CASE
        WHEN total_spent > 10000 THEN 'VIP'
        WHEN total_spent > 1000 THEN 'Premium'
        WHEN total_orders > 10 THEN 'Regular'
        ELSE 'New'
    END as customer_tier,
    DATEDIFF('days', last_order_date, CURRENT_DATE) as days_since_last_order,
    CASE
        WHEN DATEDIFF('days', last_order_date, CURRENT_DATE) > 365 THEN 'Inactive'
        WHEN DATEDIFF('days', last_order_date, CURRENT_DATE) > 180 THEN 'At Risk'
        WHEN DATEDIFF('days', last_order_date, CURRENT_DATE) > 90 THEN 'Declining'
        ELSE 'Active'
    END as engagement_status
FROM customer_summary;
```

### Lookup and Reference Data Integration

```sql
-- Enrich transactions with reference data
SELECT
    t.transaction_id,
    t.customer_id,
    t.product_id,
    t.amount,
    t.transaction_date,
    -- Customer enrichment
    c.customer_name,
    c.customer_tier,
    c.signup_date,
    DATEDIFF('days', c.signup_date, t.transaction_date) as days_since_signup,
    -- Product enrichment
    p.product_name,
    p.category,
    p.brand,
    p.cost,
    t.amount - p.cost as profit,
    (t.amount - p.cost) * 100.0 / t.amount as profit_margin_pct,
    -- Geographic enrichment
    g.region,
    g.country,
    g.timezone,
    -- Time-based enrichment
    EXTRACT('DOW', t.transaction_date) as day_of_week,
    EXTRACT('HOUR', t.transaction_date) as hour_of_day,
    CASE
        WHEN EXTRACT('DOW', t.transaction_date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    CASE
        WHEN EXTRACT('HOUR', t.transaction_date) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT('HOUR', t.transaction_date) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT('HOUR', t.transaction_date) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END as time_period
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
LEFT JOIN products p ON t.product_id = p.product_id
LEFT JOIN geographic_lookup g ON c.postal_code = g.postal_code;
```

## Data Normalization

### Pivoting Data

```sql
-- Transform row-based metrics to columns
SELECT
    device_id,
    reading_date,
    MAX(CASE WHEN metric_type = 'temperature' THEN metric_value END) as temperature,
    MAX(CASE WHEN metric_type = 'humidity' THEN metric_value END) as humidity,
    MAX(CASE WHEN metric_type = 'pressure' THEN metric_value END) as pressure,
    MAX(CASE WHEN metric_type = 'battery' THEN metric_value END) as battery_level,
    -- Quality indicators
    COUNT(DISTINCT metric_type) as metrics_available,
    CASE
        WHEN COUNT(DISTINCT metric_type) = 4 THEN 'Complete'
        WHEN COUNT(DISTINCT metric_type) >= 2 THEN 'Partial'
        ELSE 'Incomplete'
    END as data_completeness
FROM sensor_readings
WHERE metric_type IN ('temperature', 'humidity', 'pressure', 'battery')
GROUP BY device_id, reading_date;
```

### Unpivoting Data

```sql
-- Transform columns to rows for analysis
SELECT
    report_date,
    metric_name,
    metric_value
FROM (
    SELECT
        report_date,
        daily_sales,
        daily_orders,
        daily_customers,
        daily_revenue
    FROM daily_summary
) ds
UNPIVOT (
    metric_value FOR metric_name IN (
        daily_sales AS 'sales',
        daily_orders AS 'orders',
        daily_customers AS 'customers',
        daily_revenue AS 'revenue'
    )
);
-- Note: UNPIVOT syntax varies by database - this shows the concept
```

## JSON Data Transformation

### Extracting and Normalizing JSON

```sql
-- Transform nested JSON to flat structure
SELECT
    event_id,
    event_timestamp,
    -- Extract top-level fields
    JSON_VALUE(payload, '$.user_id') as user_id,
    JSON_VALUE(payload, '$.session_id') as session_id,
    JSON_VALUE(payload, '$.event_type') as event_type,
    -- Extract nested user information
    JSON_VALUE(payload, '$.user.email') as user_email,
    JSON_VALUE(payload, '$.user.subscription_tier') as subscription_tier,
    JSON_VALUE(payload, '$.user.country') as user_country,
    -- Extract product information
    JSON_VALUE(payload, '$.product.id') as product_id,
    JSON_VALUE(payload, '$.product.name') as product_name,
    JSON_VALUE(payload, '$.product.category') as product_category,
    CAST(JSON_VALUE(payload, '$.product.price') AS FLOAT) as product_price,
    -- Extract metadata
    JSON_VALUE(payload, '$.metadata.source') as traffic_source,
    JSON_VALUE(payload, '$.metadata.campaign') as campaign_id,
    JSON_VALUE(payload, '$.metadata.device_type') as device_type,
    -- Transform to typed values
    CAST(JSON_VALUE(payload, '$.metrics.session_duration') AS INTEGER) as session_duration_sec,
    CAST(JSON_VALUE(payload, '$.metrics.page_views') AS INTEGER) as page_views,
    CAST(JSON_VALUE(payload, '$.metrics.bounce_rate') AS FLOAT) as bounce_rate
FROM events
WHERE JSON_EXISTS(payload, '$.user_id');
```

### JSON Array Processing

```sql
-- Extract items from JSON arrays (conceptual - syntax varies)
SELECT
    order_id,
    order_timestamp,
    JSON_VALUE(order_data, '$.customer.id') as customer_id,
    -- Extract individual items (this is a simplified example)
    item_index,
    JSON_VALUE(item_data, '$.product_id') as product_id,
    JSON_VALUE(item_data, '$.quantity') as quantity,
    CAST(JSON_VALUE(item_data, '$.unit_price') AS FLOAT) as unit_price,
    CAST(JSON_VALUE(item_data, '$.quantity') AS INTEGER) * 
    CAST(JSON_VALUE(item_data, '$.unit_price') AS FLOAT) as line_total
FROM orders
CROSS JOIN JSON_TABLE(
    order_data,
    '$.items[*]' COLUMNS (
        item_index FOR ORDINALITY,
        item_data JSON PATH '$'
    )
) AS items_table
WHERE JSON_EXISTS(order_data, '$.items');
-- Note: JSON_TABLE syntax is database-specific
```

## Data Quality Transformations

### Duplicate Detection and Removal

```sql
-- Identify and handle duplicates
SELECT
    customer_id,
    email,
    first_name,
    last_name,
    phone,
    created_date,
    -- Identify duplicates
    ROW_NUMBER() OVER (
        PARTITION BY LOWER(email) 
        ORDER BY created_date DESC
    ) as email_rank,
    ROW_NUMBER() OVER (
        PARTITION BY REGEXP_REPLACE(phone, '[^0-9]', '', 'g')
        ORDER BY created_date DESC
    ) as phone_rank,
    -- Detect potential duplicates
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY LOWER(email) ORDER BY created_date DESC) > 1
        THEN 'DUPLICATE_EMAIL'
        WHEN ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(phone, '[^0-9]', '', 'g') ORDER BY created_date DESC) > 1
        THEN 'DUPLICATE_PHONE'
        ELSE 'UNIQUE'
    END as duplicate_status
FROM customers
WHERE email IS NOT NULL OR phone IS NOT NULL;
```

### Data Completeness Scoring

```sql
-- Calculate data completeness scores
SELECT
    record_id,
    -- Individual field completeness
    CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 0 THEN 1 ELSE 0 END as has_first_name,
    CASE WHEN last_name IS NOT NULL AND LENGTH(TRIM(last_name)) > 0 THEN 1 ELSE 0 END as has_last_name,
    CASE WHEN email IS NOT NULL AND email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN 1 ELSE 0 END as has_valid_email,
    CASE WHEN phone IS NOT NULL AND phone ~ '^[+]?[0-9]{10,15}$' THEN 1 ELSE 0 END as has_valid_phone,
    CASE WHEN address IS NOT NULL AND LENGTH(TRIM(address)) > 10 THEN 1 ELSE 0 END as has_address,
    CASE WHEN birth_date IS NOT NULL AND birth_date BETWEEN '1900-01-01' AND CURRENT_DATE THEN 1 ELSE 0 END as has_valid_birth_date,
    -- Overall completeness score
    ROUND(
        (CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 0 THEN 20 ELSE 0 END +
         CASE WHEN last_name IS NOT NULL AND LENGTH(TRIM(last_name)) > 0 THEN 20 ELSE 0 END +
         CASE WHEN email IS NOT NULL AND email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN 30 ELSE 0 END +
         CASE WHEN phone IS NOT NULL AND phone ~ '^[+]?[0-9]{10,15}$' THEN 15 ELSE 0 END +
         CASE WHEN address IS NOT NULL AND LENGTH(TRIM(address)) > 10 THEN 10 ELSE 0 END +
         CASE WHEN birth_date IS NOT NULL AND birth_date BETWEEN '1900-01-01' AND CURRENT_DATE THEN 5 ELSE 0 END),
        0
    ) as completeness_score,
    -- Quality tier
    CASE
        WHEN (CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 0 THEN 20 ELSE 0 END +
              CASE WHEN email IS NOT NULL AND email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN 30 ELSE 0 END) >= 80 THEN 'High Quality'
        WHEN (CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 0 THEN 20 ELSE 0 END +
              CASE WHEN email IS NOT NULL AND email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN 30 ELSE 0 END) >= 50 THEN 'Medium Quality'
        ELSE 'Low Quality'
    END as data_quality_tier
FROM customers;
```

## Advanced Transformation Patterns

### Time Series Data Interpolation

```sql
-- Fill gaps in time series data
SELECT
    reading_timestamp,
    sensor_id,
    actual_value,
    -- Interpolate missing values
    COALESCE(
        actual_value,
        -- Use previous value if available
        LAG(actual_value, 1) OVER (
            PARTITION BY sensor_id 
            ORDER BY reading_timestamp
        ),
        -- Use next value if no previous
        LEAD(actual_value, 1) OVER (
            PARTITION BY sensor_id 
            ORDER BY reading_timestamp
        ),
        -- Use average of surrounding values
        (LAG(actual_value, 1) OVER (
            PARTITION BY sensor_id 
            ORDER BY reading_timestamp
        ) + LEAD(actual_value, 1) OVER (
            PARTITION BY sensor_id 
            ORDER BY reading_timestamp
        )) / 2
    ) as interpolated_value,
    -- Flag interpolated data
    CASE
        WHEN actual_value IS NOT NULL THEN 'ACTUAL'
        WHEN LAG(actual_value, 1) OVER (PARTITION BY sensor_id ORDER BY reading_timestamp) IS NOT NULL
        THEN 'FORWARD_FILL'
        WHEN LEAD(actual_value, 1) OVER (PARTITION BY sensor_id ORDER BY reading_timestamp) IS NOT NULL
        THEN 'BACKWARD_FILL'
        ELSE 'INTERPOLATED'
    END as value_source
FROM (
    -- Generate complete time series with gaps
    SELECT
        timestamp_series as reading_timestamp,
        s.sensor_id,
        r.reading_value as actual_value
    FROM (
        SELECT generate_series(
            '2024-01-01 00:00:00'::timestamp,
            '2024-01-31 23:59:59'::timestamp,
            '1 hour'::interval
        ) as timestamp_series
    ) ts
    CROSS JOIN (SELECT DISTINCT sensor_id FROM sensor_readings) s
    LEFT JOIN sensor_readings r ON ts.timestamp_series = r.reading_timestamp
                                AND s.sensor_id = r.sensor_id
) time_series;
```

### Hierarchical Data Transformation

```sql
-- Transform hierarchical category data
SELECT
    product_id,
    full_category_path,
    -- Extract hierarchy levels
    SPLIT_PART(full_category_path, ' > ', 1) as level_1_category,
    SPLIT_PART(full_category_path, ' > ', 2) as level_2_category,
    SPLIT_PART(full_category_path, ' > ', 3) as level_3_category,
    SPLIT_PART(full_category_path, ' > ', 4) as level_4_category,
    -- Calculate hierarchy depth
    ARRAY_LENGTH(STRING_TO_ARRAY(full_category_path, ' > '), 1) as category_depth,
    -- Create breadcrumb navigation
    CASE ARRAY_LENGTH(STRING_TO_ARRAY(full_category_path, ' > '), 1)
        WHEN 1 THEN SPLIT_PART(full_category_path, ' > ', 1)
        WHEN 2 THEN SPLIT_PART(full_category_path, ' > ', 1) || ' → ' || SPLIT_PART(full_category_path, ' > ', 2)
        WHEN 3 THEN SPLIT_PART(full_category_path, ' > ', 1) || ' → ' || SPLIT_PART(full_category_path, ' > ', 2) || ' → ' || SPLIT_PART(full_category_path, ' > ', 3)
        ELSE LEFT(full_category_path, 50) || '...'
    END as breadcrumb
FROM products
WHERE full_category_path IS NOT NULL;
```

## Performance Tips

### Efficient Data Transformation

```sql
-- ✅ Good: Transform once, use multiple times
WITH cleaned_data AS (
    SELECT
        customer_id,
        LOWER(TRIM(email)) as clean_email,
        REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as clean_phone
    FROM customers
)
SELECT
    customer_id,
    clean_email,
    clean_phone,
    clean_email || '_verified' as email_verification_token
FROM cleaned_data;

-- ⚠️ Less efficient: Repeat transformations
SELECT
    customer_id,
    LOWER(TRIM(email)) as clean_email,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as clean_phone,
    LOWER(TRIM(email)) || '_verified' as email_verification_token  -- Repeated transformation
FROM customers;
```

### Batch Processing for Large Transformations

```sql
-- Process data in batches for large datasets
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY customer_id) as rn
    FROM customers
) batched
WHERE rn BETWEEN 1 AND 10000;  -- Process first 10k records
```

## Quick Reference

### Transformation Types
| Type | Purpose | Example |
|------|---------|--------|
| **Type Conversion** | Change data types | `CAST(string_amount AS FLOAT)` |
| **Text Cleaning** | Standardize text | `TRIM(UPPER(name))` |
| **Date Standardization** | Unify date formats | Multiple CASE statements for formats |
| **Data Enrichment** | Add calculated fields | `first_name \|\| ' ' \|\| last_name` |
| **Normalization** | Restructure data | Pivot/unpivot operations |
| **JSON Extraction** | Flatten nested data | `JSON_VALUE(data, '$.field')` |

### Common Patterns
| Pattern | SQL Template | Use Case |
|---------|--------------|----------|
| **Safe Conversion** | `CASE WHEN ... THEN CAST(...) ELSE NULL END` | Avoid conversion errors |
| **Null Handling** | `COALESCE(field1, field2, 'default')` | Provide fallback values |
| **Text Cleaning** | `TRIM(UPPER(REGEXP_REPLACE(...)))` | Standardize text data |
| **Calculated Fields** | `field1 / NULLIF(field2, 0)` | Derived metrics |
| **Data Quality** | `CASE WHEN condition THEN 'VALID' ELSE 'INVALID' END` | Quality flags |

## Next Steps

- [String functions](../functions/string.md) - Text processing functions
- [Date/time functions](../functions/date-time.md) - Temporal transformations
- [JSON functions](../functions/json.md) - JSON data processing
- [Essential functions](../functions/essential.md) - Most commonly used transformation functions