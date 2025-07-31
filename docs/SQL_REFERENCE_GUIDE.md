# FerrisStreams SQL Reference Guide

## Overview

FerrisStreams provides a comprehensive SQL interface for processing Kafka streams with native support for real-time analytics, JSON processing, and enterprise job management. This guide covers all available SQL features, functions, and commands.

## Table of Contents

1. [Basic Query Syntax](#basic-query-syntax)
2. [Job Lifecycle Management](#job-lifecycle-management)
3. [Built-in Functions](#built-in-functions)
4. [JSON Processing](#json-processing)
5. [String Functions](#string-functions)
6. [System Columns](#system-columns)
7. [Window Operations](#window-operations)
8. [Schema Management](#schema-management)
9. [Examples](#examples)

## Basic Query Syntax

### SELECT Statements

```sql
-- Basic SELECT with filtering
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 100.0
LIMIT 50;

-- SELECT with expressions and aliases
SELECT 
    customer_id,
    amount * 1.1 as amount_with_tax,
    UPPER(product_name) as product_name_upper
FROM orders;

-- Wildcard selection
SELECT * FROM orders WHERE status = 'completed';
```

### CREATE STREAM AS SELECT (CSAS)

```sql
-- Create a new stream from a SELECT query
CREATE STREAM high_value_orders AS
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 1000.0;

-- With properties
CREATE STREAM processed_events AS
SELECT * FROM raw_events WHERE event_type = 'purchase'
WITH (
    'replicas' = '3',
    'retention.ms' = '604800000'
);
```

### CREATE TABLE AS SELECT (CTAS)

```sql
-- Create a materialized table from aggregated data
CREATE TABLE customer_summary AS
SELECT 
    customer_id,
    COUNT(*) as total_orders,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value
FROM orders
GROUP BY customer_id;
```

## Job Lifecycle Management

### Job Control Commands

```sql
-- Start a streaming job
START JOB order_processor AS
SELECT * FROM orders WHERE amount > 100
WITH ('buffer.size' = '1000', 'timeout' = '30s');

-- Stop a job (graceful shutdown)
STOP JOB order_processor;

-- Force stop a job
STOP JOB order_processor FORCE;

-- Pause job execution
PAUSE JOB order_processor;

-- Resume paused job
RESUME JOB order_processor;
```

### Versioned Deployments

```sql
-- Deploy a new version with deployment strategy
DEPLOY JOB analytics VERSION '2.1.0' AS
SELECT 
    customer_id,
    COUNT(*) as order_count,
    AVG(amount) as avg_amount
FROM orders
GROUP BY customer_id
STRATEGY CANARY(25);

-- Available deployment strategies:
-- BLUE_GREEN (default)
-- CANARY(percentage)
-- ROLLING  
-- REPLACE

-- Rollback to previous version
ROLLBACK JOB analytics;

-- Rollback to specific version
ROLLBACK JOB analytics VERSION '2.0.0';
```

### Job Monitoring

```sql
-- Show all jobs
SHOW JOBS;

-- Show job status
SHOW STATUS;
SHOW STATUS analytics;

-- Show job versions
SHOW VERSIONS analytics;

-- Show job metrics
SHOW METRICS;
SHOW METRICS analytics;
```

## Built-in Functions

### Aggregate Functions

```sql
-- Basic aggregations
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM orders
GROUP BY customer_id;

-- Advanced analytical functions
SELECT 
    customer_id,
    FIRST_VALUE(product_name) as first_product,
    LAST_VALUE(order_date) as last_order_date,
    APPROX_COUNT_DISTINCT(product_category) as unique_categories
FROM orders
GROUP BY customer_id;
```

### Math Functions

```sql
-- Absolute value
SELECT 
    order_id,
    ABS(balance_change) as abs_change
FROM transactions;

-- Rounding functions
SELECT 
    order_id,
    ROUND(amount) as rounded_amount,
    ROUND(amount, 2) as rounded_to_cents,
    CEIL(amount) as ceiling_amount,
    CEILING(amount) as ceiling_alt,
    FLOOR(amount) as floor_amount
FROM orders;

-- Modulo and power functions
SELECT 
    order_id,
    MOD(order_id, 10) as last_digit,
    POWER(quantity, 2) as quantity_squared,
    POW(discount_rate, 2) as discount_squared,
    SQRT(area) as side_length
FROM products;
```

### String Functions

```sql
-- String concatenation and length
SELECT 
    customer_id,
    CONCAT('Customer: ', first_name, ' ', last_name) as full_name,
    LENGTH(description) as desc_length,
    LEN(product_code) as code_length
FROM customers;

-- String trimming and case conversion
SELECT 
    product_id,
    TRIM(description) as clean_description,
    LTRIM(description) as left_trimmed,
    RTRIM(description) as right_trimmed,
    UPPER(product_name) as upper_name,
    LOWER(category) as lower_category
FROM products;

-- String manipulation
SELECT 
    customer_id,
    REPLACE(phone_number, '-', '') as clean_phone,
    LEFT(product_code, 3) as category_code,
    RIGHT(order_id, 4) as order_suffix
FROM orders;
```

### Date/Time Functions

```sql
-- Current timestamp functions
SELECT 
    order_id,
    NOW() as current_time,
    CURRENT_TIMESTAMP as current_ts
FROM orders;

-- Date formatting and extraction
SELECT 
    order_id,
    DATE_FORMAT(_timestamp, '%Y-%m-%d') as order_date,
    DATE_FORMAT(_timestamp, '%Y-%m-%d %H:%M:%S') as order_datetime,
    EXTRACT('YEAR', _timestamp) as order_year,
    EXTRACT('MONTH', _timestamp) as order_month,
    EXTRACT('DAY', _timestamp) as order_day,
    EXTRACT('HOUR', _timestamp) as order_hour,
    EXTRACT('DOW', _timestamp) as day_of_week,
    EXTRACT('DOY', _timestamp) as day_of_year
FROM orders;
```

### Utility Functions

```sql
-- Null handling functions
SELECT 
    customer_id,
    COALESCE(preferred_name, first_name, 'Unknown') as display_name,
    NULLIF(discount_rate, 0.0) as effective_discount
FROM customers;

-- Timestamp function
SELECT 
    order_id,
    TIMESTAMP() as processed_at
FROM orders;

-- Type casting
SELECT 
    order_id,
    CAST(amount, 'INTEGER') as amount_int,
    CAST(customer_id, 'STRING') as customer_id_str,
    CAST(is_active, 'BOOLEAN') as is_active_bool
FROM orders;

-- String operations (legacy functions)
SELECT 
    customer_id,
    SPLIT(full_name, ' ') as first_name,
    JOIN(' - ', order_id, customer_id) as order_key
FROM orders;
```

## JSON Processing

### JSON Extraction Functions

```sql
-- Extract JSON values
SELECT 
    event_id,
    JSON_VALUE(payload, '$.user.id') as user_id,
    JSON_VALUE(payload, '$.order.total') as order_total,
    JSON_EXTRACT(payload, '$.user') as user_data
FROM kafka_events;

-- Array access in JSON
SELECT 
    event_id,
    JSON_VALUE(payload, '$.items[0].name') as first_item_name,
    JSON_VALUE(payload, '$.items[0].price') as first_item_price
FROM kafka_events;

-- Nested JSON processing
SELECT 
    event_id,
    JSON_VALUE(payload, '$.customer.address.city') as customer_city,
    JSON_VALUE(payload, '$.customer.preferences.newsletter') as newsletter_opt_in
FROM kafka_events;
```

### Real-World JSON Examples

```sql
-- Process complex Kafka message payloads
SELECT 
    _timestamp as kafka_timestamp,
    _partition as kafka_partition,
    JSON_VALUE(value, '$.eventType') as event_type,
    CAST(JSON_VALUE(value, '$.user.id'), 'INTEGER') as user_id,
    JSON_VALUE(value, '$.user.email') as user_email,
    CAST(JSON_VALUE(value, '$.order.total'), 'FLOAT') as order_total,
    JSON_VALUE(value, '$.order.items[0].name') as first_item_name
FROM kafka_topic_orders 
WHERE JSON_VALUE(value, '$.eventType') = 'ORDER_CREATED';
```

## String Functions

### SUBSTRING Function

```sql
-- Basic substring extraction
SELECT 
    customer_id,
    SUBSTRING(description, 1, 50) as short_description,
    SUBSTRING(phone_number, 1, 3) as area_code
FROM customers;

-- Substring without length (from position to end)
SELECT 
    customer_id,
    SUBSTRING(full_name, 6) as last_name
FROM customers;
```

### Combined String and JSON Processing

```sql
-- Extract and process JSON strings
SELECT 
    event_id,
    SUBSTRING(JSON_VALUE(payload, '$.description'), 1, 100) as short_desc,
    CAST(SUBSTRING(JSON_VALUE(payload, '$.phone'), 1, 3), 'INTEGER') as area_code
FROM events;
```

## System Columns

FerrisStreams provides access to Kafka message metadata through system columns:

```sql
-- Access Kafka metadata
SELECT 
    _timestamp as message_timestamp,
    _offset as message_offset,
    _partition as partition_number,
    customer_id,
    amount
FROM orders
WHERE _partition = 0;
```

### Available System Columns

- `_timestamp`: Kafka message timestamp (epoch milliseconds)
- `_offset`: Kafka message offset within partition
- `_partition`: Kafka partition number

### Header Functions

```sql
-- Access message headers
SELECT 
    order_id,
    HEADER('trace-id') as trace_id,
    HEADER('source-system') as source_system,
    HAS_HEADER('correlation-id') as has_correlation_id
FROM orders;

-- List all header keys
SELECT 
    order_id,
    HEADER_KEYS() as all_header_keys
FROM orders;
```

## Window Operations

### Window Specifications

```sql
-- Tumbling window (non-overlapping fixed intervals)
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY customer_id
WINDOW TUMBLING(5m);

-- Sliding window (overlapping intervals)
SELECT 
    customer_id,
    AVG(amount) as avg_amount
FROM orders
GROUP BY customer_id
WINDOW SLIDING(10m, 5m);

-- Session window (gap-based grouping)
SELECT 
    customer_id,
    COUNT(*) as session_events
FROM user_events
GROUP BY customer_id
WINDOW SESSION(30m);
```

### Window with Custom Time Column

```sql
-- Use custom timestamp column for windowing
SELECT 
    customer_id,
    COUNT(*) as hourly_orders
FROM orders
GROUP BY customer_id
WINDOW TUMBLING(1h, order_timestamp);
```

## Schema Management

### DESCRIBE Command

```sql
-- Describe stream/table schema
DESCRIBE orders;
DESCRIBE STREAM orders;
DESCRIBE TABLE customer_summary;
```

### SHOW Commands

```sql
-- Discovery commands
SHOW STREAMS;
SHOW TABLES;
SHOW TOPICS;
SHOW FUNCTIONS;

-- With pattern matching
SHOW STREAMS LIKE 'order%';
SHOW TABLES LIKE 'customer_*';

-- Schema and metadata
SHOW SCHEMA orders;
SHOW PROPERTIES STREAM orders;
SHOW PARTITIONS orders;
```

## Examples

### Complete Real-World Examples

#### 1. E-commerce Order Processing

```sql
-- Deploy order enrichment job
DEPLOY JOB order_enrichment VERSION '1.0.0' AS
SELECT 
    JSON_VALUE(payload, '$.orderId') as order_id,
    JSON_VALUE(payload, '$.customerId') as customer_id,
    CAST(JSON_VALUE(payload, '$.total'), 'FLOAT') as order_total,
    JSON_VALUE(payload, '$.status') as order_status,
    SUBSTRING(JSON_VALUE(payload, '$.description'), 1, 100) as short_description,
    JSON_EXTRACT(payload, '$.items') as order_items,
    TIMESTAMP() as processed_at
FROM order_events
WHERE JSON_VALUE(payload, '$.status') IN ('confirmed', 'shipped')
STRATEGY BLUE_GREEN;
```

#### 2. Real-Time Analytics

```sql
-- Customer behavior analytics with windowing
START JOB customer_analytics AS
SELECT 
    customer_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT JSON_VALUE(payload, '$.product_id')) as unique_products,
    AVG(CAST(JSON_VALUE(payload, '$.amount'), 'FLOAT')) as avg_amount,
    FIRST_VALUE(JSON_VALUE(payload, '$.campaign')) as first_campaign,
    LAST_VALUE(JSON_VALUE(payload, '$.page_url')) as last_page
FROM user_activity_events
WHERE JSON_VALUE(payload, '$.event_type') = 'purchase'
GROUP BY customer_id
WINDOW TUMBLING(1h)
HAVING COUNT(*) > 3;
```

#### 3. Fraud Detection

```sql
-- High-value transaction monitoring
DEPLOY JOB fraud_monitor VERSION '2.0.0' AS
SELECT 
    JSON_VALUE(payload, '$.transaction_id') as transaction_id,
    JSON_VALUE(payload, '$.user_id') as user_id,
    CAST(JSON_VALUE(payload, '$.amount'), 'FLOAT') as amount,
    JSON_VALUE(payload, '$.merchant') as merchant,
    JSON_VALUE(payload, '$.location.country') as country,
    SUBSTRING(JSON_VALUE(payload, '$.card_number'), -4) as card_last_four,
    _timestamp as kafka_timestamp
FROM transaction_events
WHERE CAST(JSON_VALUE(payload, '$.amount'), 'FLOAT') > 10000.0
OR JSON_VALUE(payload, '$.location.country') != JSON_VALUE(payload, '$.user.home_country')
STRATEGY CANARY(5);
```

#### 4. IoT Sensor Data Processing

```sql
-- IoT sensor aggregation with JSON processing
CREATE STREAM sensor_alerts AS
SELECT 
    JSON_VALUE(payload, '$.device_id') as device_id,
    JSON_VALUE(payload, '$.sensor_type') as sensor_type,
    CAST(JSON_VALUE(payload, '$.reading'), 'FLOAT') as reading,
    JSON_VALUE(payload, '$.location.facility') as facility,
    JSON_VALUE(payload, '$.location.room') as room,
    CASE 
        WHEN CAST(JSON_VALUE(payload, '$.reading'), 'FLOAT') > 80.0 THEN 'HIGH'
        WHEN CAST(JSON_VALUE(payload, '$.reading'), 'FLOAT') < 10.0 THEN 'LOW'
        ELSE 'NORMAL'
    END as alert_level
FROM iot_sensor_data
WHERE JSON_VALUE(payload, '$.sensor_type') = 'temperature'
AND (CAST(JSON_VALUE(payload, '$.reading'), 'FLOAT') > 80.0 
     OR CAST(JSON_VALUE(payload, '$.reading'), 'FLOAT') < 10.0);
```

## Best Practices

### Performance Tips

1. **Use specific field selection** instead of `SELECT *` when possible
2. **Apply filters early** with WHERE clauses to reduce processing overhead
3. **Use appropriate data types** with CAST for optimal performance
4. **Leverage JSON_VALUE for scalar values** and JSON_EXTRACT for complex objects
5. **Use LIMIT** for testing and development queries

### JSON Processing Guidelines

1. **Validate JSON paths** before deployment using DESCRIBE and sample data
2. **Handle missing fields gracefully** - JSON functions return NULL for missing paths
3. **Use consistent JSONPath syntax** - prefer `$.field.name` over `field.name`
4. **Consider performance impact** of deep JSON traversal in high-throughput scenarios

### Job Management Best Practices

1. **Use versioned deployments** for production changes
2. **Start with CANARY deployments** for risky changes
3. **Monitor job metrics** regularly with SHOW METRICS
4. **Test deployment strategies** in non-production environments first
5. **Use descriptive job names** and version numbers for operational clarity

## Error Handling

### Common Error Scenarios

```sql
-- Handle invalid JSON gracefully
SELECT 
    event_id,
    COALESCE(JSON_VALUE(payload, '$.user.id'), 'unknown') as user_id
FROM events;

-- Type conversion with error handling
SELECT 
    event_id,
    CASE 
        WHEN JSON_VALUE(payload, '$.amount') IS NOT NULL 
        THEN CAST(JSON_VALUE(payload, '$.amount'), 'FLOAT')
        ELSE 0.0
    END as safe_amount
FROM events;
```

## Complete Function Reference

### Math Functions (7 functions)
- `ABS(number)` - Absolute value
- `ROUND(number[, precision])` - Round to specified decimal places
- `CEIL(number)`, `CEILING(number)` - Round up to nearest integer
- `FLOOR(number)` - Round down to nearest integer
- `MOD(a, b)` - Modulo operation (remainder)
- `POWER(base, exponent)`, `POW(base, exponent)` - Exponentiation
- `SQRT(number)` - Square root

### String Functions (11 functions)
- `CONCAT(str1, str2, ...)` - Concatenate strings
- `LENGTH(string)`, `LEN(string)` - String length in characters
- `TRIM(string)` - Remove leading and trailing whitespace
- `LTRIM(string)` - Remove leading whitespace
- `RTRIM(string)` - Remove trailing whitespace
- `UPPER(string)` - Convert to uppercase
- `LOWER(string)` - Convert to lowercase
- `REPLACE(string, search, replace)` - Replace occurrences
- `LEFT(string, length)` - Get leftmost characters
- `RIGHT(string, length)` - Get rightmost characters
- `SUBSTRING(string, start[, length])` - Extract substring

### Date/Time Functions (4 functions)
- `NOW()` - Current timestamp in milliseconds
- `CURRENT_TIMESTAMP` - Current timestamp in milliseconds
- `DATE_FORMAT(timestamp, format)` - Format timestamp as string
- `EXTRACT(part, timestamp)` - Extract date/time component

### Utility Functions (6 functions)
- `COALESCE(value1, value2, ...)` - Return first non-null value
- `NULLIF(value1, value2)` - Return null if values are equal
- `CAST(value, type)` - Type conversion
- `TIMESTAMP()` - Current record processing timestamp
- `SPLIT(string, delimiter)` - Split string (returns first part)
- `JOIN(delimiter, str1, str2, ...)` - Join strings with delimiter

### Aggregate Functions (6 functions)
- `COUNT(*)` - Count records
- `SUM(column)` - Sum numeric values
- `AVG(column)` - Average of numeric values
- `MIN(column)` - Minimum value
- `MAX(column)` - Maximum value
- `APPROX_COUNT_DISTINCT(column)` - Approximate distinct count

### JSON Functions (2 functions)
- `JSON_VALUE(json_string, path)` - Extract scalar value from JSON
- `JSON_EXTRACT(json_string, path)` - Extract value/object from JSON

### Header Functions (3 functions)
- `HEADER(key)` - Get Kafka message header value
- `HAS_HEADER(key)` - Check if header exists
- `HEADER_KEYS()` - Get comma-separated list of header keys

### System Columns (3 columns)
- `_timestamp` - Kafka message timestamp
- `_offset` - Kafka message offset
- `_partition` - Kafka partition number

**Total: 42 functions + 3 system columns**

This reference guide covers all currently implemented SQL features in FerrisStreams. For the latest updates and additional examples, refer to the test suite and feature documentation.