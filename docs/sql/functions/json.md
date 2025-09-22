# JSON Functions

Complete reference for JSON processing functions in Velostream. Use these functions to extract, query, and validate JSON data in your streaming queries.

## JSON Value Extraction

### JSON_VALUE - Extract Scalar Values

```sql
-- Extract user ID from JSON payload
SELECT
    event_id,
    JSON_VALUE(payload, '$.user_id') as user_id,
    JSON_VALUE(payload, '$.action') as action_performed,
    JSON_VALUE(payload, '$.timestamp') as event_timestamp
FROM user_events
WHERE JSON_VALUE(payload, '$.event_type') = 'user_action';

-- Extract nested values
SELECT
    order_id,
    JSON_VALUE(order_data, '$.customer.id') as customer_id,
    JSON_VALUE(order_data, '$.customer.name') as customer_name,
    JSON_VALUE(order_data, '$.shipping.address.city') as shipping_city,
    JSON_VALUE(order_data, '$.items[0].product_id') as first_product_id
FROM orders;
```

### JSON_QUERY - Extract Objects and Arrays

```sql
-- Extract entire JSON objects
SELECT
    user_id,
    JSON_QUERY(profile_data, '$.preferences') as user_preferences,
    JSON_QUERY(profile_data, '$.address') as user_address,
    JSON_QUERY(profile_data, '$.purchase_history') as purchase_history
FROM user_profiles;

-- Extract arrays for further processing
SELECT
    product_id,
    JSON_QUERY(product_data, '$.tags') as product_tags,
    JSON_QUERY(product_data, '$.variants') as product_variants
FROM products
WHERE JSON_QUERY(product_data, '$.tags') IS NOT NULL;
```

## JSON Validation and Testing

### JSON_EXISTS - Check if Path Exists

```sql
-- Validate required fields exist
SELECT
    event_id,
    payload,
    JSON_EXISTS(payload, '$.user_id') as has_user_id,
    JSON_EXISTS(payload, '$.session_id') as has_session_id,
    JSON_EXISTS(payload, '$.timestamp') as has_timestamp
FROM events
WHERE JSON_EXISTS(payload, '$.event_type');

-- Filter records with specific JSON structure
SELECT *
FROM product_updates
WHERE JSON_EXISTS(product_data, '$.pricing.discounts')
  AND JSON_EXISTS(product_data, '$.inventory.quantity');
```

## JSON Path Expressions

### Basic Path Syntax

```sql
-- Root level properties
SELECT JSON_VALUE(data, '$.name') FROM table_name;          -- Simple property
SELECT JSON_VALUE(data, '$."complex-name"') FROM table_name; -- Property with special chars

-- Nested properties
SELECT JSON_VALUE(data, '$.user.profile.email') FROM table_name;

-- Array access
SELECT JSON_VALUE(data, '$.items[0]') FROM table_name;      -- First item
SELECT JSON_VALUE(data, '$.items[1].name') FROM table_name; -- Property of second item
SELECT JSON_VALUE(data, '$.items[-1]') FROM table_name;     -- Last item (if supported)
```

### Advanced Path Patterns

```sql
-- Wildcard and filter expressions (if supported)
SELECT
    order_id,
    -- Extract all item prices (implementation may vary)
    JSON_QUERY(order_data, '$.items[*].price') as all_prices,
    -- Extract items with specific conditions
    JSON_QUERY(order_data, '$.items[?(@.category == "electronics")]') as electronics_items
FROM orders;
```

## Real-World JSON Processing Examples

### E-commerce Event Processing

```sql
-- Process e-commerce tracking events
SELECT
    event_id,
    JSON_VALUE(event_data, '$.timestamp') as event_timestamp,
    JSON_VALUE(event_data, '$.user.id') as user_id,
    JSON_VALUE(event_data, '$.user.email') as user_email,
    JSON_VALUE(event_data, '$.event.type') as event_type,
    JSON_VALUE(event_data, '$.event.product_id') as product_id,
    JSON_VALUE(event_data, '$.event.value') as event_value,
    JSON_VALUE(event_data, '$.metadata.source') as traffic_source,
    JSON_VALUE(event_data, '$.metadata.campaign') as campaign_id,
    CASE JSON_VALUE(event_data, '$.event.type')
        WHEN 'page_view' THEN 'Engagement'
        WHEN 'add_to_cart' THEN 'Intent'
        WHEN 'purchase' THEN 'Conversion'
        ELSE 'Other'
    END as event_category
FROM user_tracking_events
WHERE JSON_EXISTS(event_data, '$.event.type')
  AND JSON_VALUE(event_data, '$.timestamp') > NOW() - INTERVAL '24' HOURS;
```

### API Response Processing

```sql
-- Process API responses with varying structures
SELECT
    api_call_id,
    request_timestamp,
    JSON_VALUE(response_body, '$.status') as response_status,
    JSON_VALUE(response_body, '$.data.result') as api_result,
    JSON_VALUE(response_body, '$.error.code') as error_code,
    JSON_VALUE(response_body, '$.error.message') as error_message,
    JSON_VALUE(response_body, '$.metadata.processing_time') as processing_time_ms,
    CASE
        WHEN JSON_EXISTS(response_body, '$.error') THEN 'Error'
        WHEN JSON_VALUE(response_body, '$.status') = 'success' THEN 'Success'
        ELSE 'Unknown'
    END as call_result
FROM api_calls
WHERE JSON_EXISTS(response_body, '$.status');
```

### IoT Sensor Data Processing

```sql
-- Process IoT sensor readings from JSON payloads
SELECT
    device_id,
    reading_timestamp,
    JSON_VALUE(sensor_data, '$.device.location.building') as building,
    JSON_VALUE(sensor_data, '$.device.location.floor') as floor,
    JSON_VALUE(sensor_data, '$.device.location.room') as room,
    JSON_VALUE(sensor_data, '$.readings.temperature') as temperature,
    JSON_VALUE(sensor_data, '$.readings.humidity') as humidity,
    JSON_VALUE(sensor_data, '$.readings.air_quality.pm25') as pm25_level,
    JSON_VALUE(sensor_data, '$.device.battery_level') as battery_level,
    JSON_VALUE(sensor_data, '$.device.signal_strength') as signal_strength,
    CASE
        WHEN CAST(JSON_VALUE(sensor_data, '$.device.battery_level') AS FLOAT) < 0.2 THEN 'Low Battery'
        WHEN CAST(JSON_VALUE(sensor_data, '$.readings.temperature') AS FLOAT) > 25 THEN 'High Temp'
        WHEN CAST(JSON_VALUE(sensor_data, '$.readings.air_quality.pm25') AS FLOAT) > 50 THEN 'Poor Air Quality'
        ELSE 'Normal'
    END as alert_status
FROM iot_sensor_readings
WHERE JSON_EXISTS(sensor_data, '$.readings.temperature');
```

### Social Media Data Processing

```sql
-- Process social media posts and engagement data
SELECT
    post_id,
    JSON_VALUE(post_data, '$.author.username') as username,
    JSON_VALUE(post_data, '$.author.follower_count') as follower_count,
    JSON_VALUE(post_data, '$.content.text') as post_text,
    JSON_VALUE(post_data, '$.content.media_type') as media_type,
    JSON_VALUE(post_data, '$.engagement.likes') as likes,
    JSON_VALUE(post_data, '$.engagement.shares') as shares,
    JSON_VALUE(post_data, '$.engagement.comments') as comments,
    JSON_QUERY(post_data, '$.hashtags') as hashtags,
    JSON_QUERY(post_data, '$.mentions') as mentions,
    -- Calculate engagement rate
    ROUND(
        (CAST(JSON_VALUE(post_data, '$.engagement.likes') AS FLOAT) +
         CAST(JSON_VALUE(post_data, '$.engagement.shares') AS FLOAT) +
         CAST(JSON_VALUE(post_data, '$.engagement.comments') AS FLOAT)) * 100.0 /
        CAST(JSON_VALUE(post_data, '$.author.follower_count') AS FLOAT),
        2
    ) as engagement_rate_pct
FROM social_media_posts
WHERE JSON_EXISTS(post_data, '$.engagement.likes')
  AND CAST(JSON_VALUE(post_data, '$.author.follower_count') AS FLOAT) > 0;
```

### Financial Transaction Processing

```sql
-- Process complex financial transaction data
SELECT
    transaction_id,
    JSON_VALUE(transaction_data, '$.timestamp') as transaction_timestamp,
    JSON_VALUE(transaction_data, '$.amount.value') as amount,
    JSON_VALUE(transaction_data, '$.amount.currency') as currency,
    JSON_VALUE(transaction_data, '$.source.account_id') as source_account,
    JSON_VALUE(transaction_data, '$.source.bank_code') as source_bank,
    JSON_VALUE(transaction_data, '$.destination.account_id') as dest_account,
    JSON_VALUE(transaction_data, '$.destination.bank_code') as dest_bank,
    JSON_VALUE(transaction_data, '$.metadata.transaction_type') as transaction_type,
    JSON_VALUE(transaction_data, '$.metadata.reference') as reference,
    JSON_VALUE(transaction_data, '$.fees.processing_fee') as processing_fee,
    JSON_VALUE(transaction_data, '$.risk_assessment.score') as risk_score,
    JSON_VALUE(transaction_data, '$.risk_assessment.reason') as risk_reason,
    CASE
        WHEN CAST(JSON_VALUE(transaction_data, '$.risk_assessment.score') AS FLOAT) > 0.8 THEN 'High Risk'
        WHEN CAST(JSON_VALUE(transaction_data, '$.risk_assessment.score') AS FLOAT) > 0.5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_category
FROM financial_transactions
WHERE JSON_EXISTS(transaction_data, '$.amount.value');
```

## Data Validation and Quality Checks

### JSON Structure Validation

```sql
-- Validate required JSON structure
SELECT
    record_id,
    raw_data,
    JSON_EXISTS(raw_data, '$.user_id') as has_user_id,
    JSON_EXISTS(raw_data, '$.timestamp') as has_timestamp,
    JSON_EXISTS(raw_data, '$.event_type') as has_event_type,
    CASE
        WHEN JSON_EXISTS(raw_data, '$.user_id') AND
             JSON_EXISTS(raw_data, '$.timestamp') AND
             JSON_EXISTS(raw_data, '$.event_type')
        THEN 'Valid'
        ELSE 'Invalid'
    END as record_validity
FROM event_stream
WHERE raw_data IS NOT NULL;
```

### Data Type Validation

```sql
-- Validate data types within JSON
SELECT
    order_id,
    order_data,
    JSON_VALUE(order_data, '$.total_amount') as total_amount_str,
    CASE
        WHEN JSON_VALUE(order_data, '$.total_amount') ~ '^[0-9]+(\.[0-9]+)?$'
        THEN CAST(JSON_VALUE(order_data, '$.total_amount') AS FLOAT)
        ELSE NULL
    END as total_amount_numeric,
    CASE
        WHEN JSON_VALUE(order_data, '$.total_amount') ~ '^[0-9]+(\.[0-9]+)?$'
        THEN 'Valid Number'
        ELSE 'Invalid Number'
    END as amount_validation
FROM orders
WHERE JSON_EXISTS(order_data, '$.total_amount');
```

## JSON Aggregation Patterns

### Counting JSON Properties

```sql
-- Count events by JSON properties
SELECT
    JSON_VALUE(event_data, '$.event_type') as event_type,
    JSON_VALUE(event_data, '$.source') as event_source,
    COUNT(*) as event_count,
    COUNT(DISTINCT JSON_VALUE(event_data, '$.user_id')) as unique_users
FROM user_events
WHERE JSON_EXISTS(event_data, '$.event_type')
GROUP BY JSON_VALUE(event_data, '$.event_type'), JSON_VALUE(event_data, '$.source')
ORDER BY event_count DESC;
```

### Aggregating JSON Numeric Values

```sql
-- Aggregate numeric values from JSON
SELECT
    DATE_FORMAT(_timestamp, '%Y-%m-%d') as date,
    JSON_VALUE(metrics, '$.source') as traffic_source,
    COUNT(*) as total_events,
    SUM(CAST(JSON_VALUE(metrics, '$.revenue') AS FLOAT)) as total_revenue,
    AVG(CAST(JSON_VALUE(metrics, '$.session_duration') AS FLOAT)) as avg_session_duration,
    MIN(CAST(JSON_VALUE(metrics, '$.page_load_time') AS FLOAT)) as min_load_time,
    MAX(CAST(JSON_VALUE(metrics, '$.page_load_time') AS FLOAT)) as max_load_time
FROM website_analytics
WHERE JSON_EXISTS(metrics, '$.revenue')
  AND CAST(JSON_VALUE(metrics, '$.revenue') AS FLOAT) > 0
GROUP BY DATE_FORMAT(_timestamp, '%Y-%m-%d'), JSON_VALUE(metrics, '$.source');
```

## Performance Tips

### Efficient JSON Queries

```sql
-- ✅ Good: Use JSON_EXISTS for filtering before extraction
SELECT JSON_VALUE(data, '$.user.id') as user_id
FROM events
WHERE JSON_EXISTS(data, '$.user.id');  -- Filter first

-- ⚠️ Less efficient: Extract then filter
SELECT JSON_VALUE(data, '$.user.id') as user_id
FROM events
WHERE JSON_VALUE(data, '$.user.id') IS NOT NULL;  -- Extracts for all rows
```

### Indexing JSON Paths

```sql
-- Consider creating functional indexes on frequently queried JSON paths
-- (Conceptual - actual syntax varies by database)
-- CREATE INDEX idx_user_events_user_id ON user_events (JSON_VALUE(payload, '$.user_id'));
-- CREATE INDEX idx_orders_customer_id ON orders (JSON_VALUE(order_data, '$.customer.id'));
```

### Type Casting Performance

```sql
-- Cache type conversions when used multiple times
SELECT
    order_id,
    amount_str,
    amount_numeric,
    amount_numeric * 1.1 as amount_with_tax
FROM (
    SELECT
        order_id,
        JSON_VALUE(order_data, '$.amount') as amount_str,
        CAST(JSON_VALUE(order_data, '$.amount') AS FLOAT) as amount_numeric
    FROM orders
) WITH_CAST
WHERE amount_numeric > 100;
```

## Common Patterns

### JSON Field Mapping

```sql
-- Map JSON fields to relational columns
CREATE VIEW user_events_normalized AS
SELECT
    event_id,
    event_timestamp,
    JSON_VALUE(payload, '$.user_id') as user_id,
    JSON_VALUE(payload, '$.session_id') as session_id,
    JSON_VALUE(payload, '$.event_type') as event_type,
    JSON_VALUE(payload, '$.page_url') as page_url,
    CAST(JSON_VALUE(payload, '$.duration') AS INTEGER) as duration_seconds,
    JSON_QUERY(payload, '$.custom_properties') as custom_properties
FROM raw_events
WHERE JSON_EXISTS(payload, '$.user_id');
```

### Conditional JSON Processing

```sql
-- Handle different JSON schemas based on event type
SELECT
    event_id,
    JSON_VALUE(data, '$.event_type') as event_type,
    CASE JSON_VALUE(data, '$.event_type')
        WHEN 'purchase' THEN JSON_VALUE(data, '$.purchase.amount')
        WHEN 'subscription' THEN JSON_VALUE(data, '$.subscription.monthly_fee')
        ELSE NULL
    END as monetary_value,
    CASE JSON_VALUE(data, '$.event_type')
        WHEN 'purchase' THEN JSON_VALUE(data, '$.purchase.product_id')
        WHEN 'subscription' THEN JSON_VALUE(data, '$.subscription.plan_id')
        ELSE NULL
    END as product_identifier
FROM events
WHERE JSON_VALUE(data, '$.event_type') IN ('purchase', 'subscription');
```

## Quick Reference

| Function | Purpose | Example |
|----------|---------|--------|
| `JSON_VALUE(json, path)` | Extract scalar value | `JSON_VALUE(data, '$.user.id')` |
| `JSON_QUERY(json, path)` | Extract object/array | `JSON_QUERY(data, '$.preferences')` |
| `JSON_EXISTS(json, path)` | Check if path exists | `JSON_EXISTS(data, '$.user.email')` |

### Common JSON Path Patterns
| Pattern | Description | Example |
|---------|-------------|--------|
| `$.property` | Root level property | `$.user_id` |
| `$.obj.prop` | Nested property | `$.user.profile.email` |
| `$.array[0]` | First array element | `$.items[0]` |
| `$.array[-1]` | Last array element | `$.items[-1]` (if supported) |
| `$."complex-name"` | Property with special chars | `$."user-id"` |

## Next Steps

- [String functions](string.md) - Text processing for extracted JSON values
- [Essential functions](essential.md) - Most commonly used functions
- [Data transformation](../by-task/transform-data.md) - Converting JSON to structured data