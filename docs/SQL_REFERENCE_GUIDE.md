# FerrisStreams SQL Reference Guide

## Overview

FerrisStreams provides a comprehensive SQL interface for processing Kafka streams with native support for real-time analytics, JSON processing, and enterprise job management. This guide covers all available SQL features, functions, and commands.

## Table of Contents

1. [Basic Query Syntax](#basic-query-syntax)
2. [JOIN Operations](#join-operations)
3. [Job Lifecycle Management](#job-lifecycle-management)
4. [Built-in Functions](#built-in-functions)
   - [Window Functions](#window-functions)
   - [Statistical Functions](#statistical-functions)
   - [Aggregate Functions](#aggregate-functions)
   - [Math Functions](#math-functions)
   - [String Functions](#string-functions)
   - [Date/Time Functions](#datetime-functions)
   - [Utility Functions](#utility-functions)
   - [CASE WHEN Expressions](#case-when-expressions)
   - [INTERVAL Arithmetic](#interval-arithmetic)
5. [JSON Processing](#json-processing)
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

## JOIN Operations

FerrisStreams supports comprehensive JOIN operations for combining data from multiple streams and tables, including windowed JOINs for temporal correlation in streaming data.

### Supported JOIN Types

#### INNER JOIN
Combines records from two streams/tables where the join condition is met.

```sql
-- Basic INNER JOIN
SELECT 
    o.order_id,
    o.customer_id,
    o.amount,
    c.customer_name,
    c.email
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;

-- INNER JOIN with filtering
SELECT 
    o.order_id,
    p.product_name,
    o.quantity * p.price as total_value
FROM orders o
INNER JOIN products p ON o.product_id = p.product_id
WHERE o.amount > 100.0;
```

#### LEFT JOIN (LEFT OUTER JOIN)
Returns all records from the left stream/table and matching records from the right.

```sql
-- LEFT JOIN - all orders with optional customer details
SELECT 
    o.order_id,
    o.customer_id,
    o.amount,
    c.customer_name,
    c.email
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;

-- Alternative syntax with OUTER keyword
SELECT *
FROM orders o
LEFT OUTER JOIN customers c ON o.customer_id = c.customer_id;
```

#### RIGHT JOIN (RIGHT OUTER JOIN)
Returns all records from the right stream/table and matching records from the left.

```sql
-- RIGHT JOIN - all customers with optional order details
SELECT 
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.amount
FROM orders o
RIGHT JOIN customers c ON o.customer_id = c.customer_id;

-- Alternative syntax with OUTER keyword
SELECT *
FROM orders o
RIGHT OUTER JOIN customers c ON o.customer_id = c.customer_id;
```

#### FULL OUTER JOIN
Returns all records from both streams/tables, with NULLs where no match exists.

```sql
-- FULL OUTER JOIN - complete view of orders and customers
SELECT 
    COALESCE(o.customer_id, c.customer_id) as customer_id,
    o.order_id,
    o.amount,
    c.customer_name,
    c.email
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.customer_id;
```

### Windowed JOINs for Streaming Data

Windowed JOINs enable temporal correlation between streams, essential for real-time stream processing.

#### Time-Based Windows

```sql
-- JOIN within 5 minutes window
SELECT 
    o.order_id,
    p.payment_id,
    o.amount,
    p.payment_method
FROM orders o
INNER JOIN payments p ON o.order_id = p.order_id
WITHIN INTERVAL '5' MINUTES;

-- JOIN within 30 seconds for fast correlation
SELECT 
    click.user_id,
    click.page_url,
    purchase.order_id,
    purchase.amount
FROM user_clicks click
INNER JOIN user_purchases purchase ON click.user_id = purchase.user_id
WITHIN INTERVAL '30' SECONDS;

-- JOIN within 2 hours for longer-term correlation
SELECT 
    session.session_id,
    session.user_id,
    events.event_type,
    events.event_data
FROM user_sessions session
LEFT JOIN user_events events ON session.user_id = events.user_id
WITHIN INTERVAL '2' HOURS;
```

### Complex JOIN Conditions

```sql
-- Multiple join conditions
SELECT 
    o.order_id,
    i.item_name,
    o.quantity * i.unit_price as line_total
FROM order_items o
INNER JOIN inventory i ON o.product_id = i.product_id 
                       AND o.warehouse_id = i.warehouse_id;

-- JOIN with additional filters
SELECT 
    u.user_id,
    u.username,
    a.action_type,
    a.timestamp
FROM users u
LEFT JOIN user_actions a ON u.user_id = a.user_id 
                          AND a.action_type = 'purchase'
WHERE u.status = 'active';
```

### Stream-Table JOINs

Optimized JOINs between streaming data and materialized tables for reference data lookups.

```sql
-- Enrich streaming events with reference data
SELECT 
    events.event_id,
    events.user_id,
    events.event_type,
    users.user_name,
    users.user_tier,
    users.signup_date
FROM streaming_events events
INNER JOIN user_reference_table users ON events.user_id = users.user_id;

-- Product catalog lookup
SELECT 
    sales_events.sale_id,
    sales_events.product_id,
    sales_events.quantity,
    products.product_name,
    products.category,
    products.unit_price
FROM real_time_sales sales_events
INNER JOIN product_catalog products ON sales_events.product_id = products.product_id;
```

### Table Aliases in JOINs

```sql
-- Using table aliases for readability
SELECT 
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name,
    oi.quantity
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id;
```

### JOIN with JSON Processing

```sql
-- JOIN with JSON field extraction
SELECT 
    events.event_id,
    JSON_VALUE(events.payload, '$.user_id') as user_id,
    users.user_name,
    JSON_VALUE(events.payload, '$.action') as action_performed
FROM kafka_events events
INNER JOIN user_table users ON JSON_VALUE(events.payload, '$.user_id') = users.user_id
WHERE JSON_VALUE(events.payload, '$.event_type') = 'user_action';
```

### Performance Considerations

1. **Window Size**: Use appropriate window sizes for temporal JOINs - smaller windows reduce memory usage
2. **Join Conditions**: Ensure join conditions are selective to minimize processing overhead
3. **Stream-Table JOINs**: Prefer stream-table JOINs over stream-stream JOINs for reference data lookups
4. **Index Usage**: Consider partitioning strategies that align with join keys for optimal performance

### Common JOIN Patterns

#### Late-Arriving Data Handling
```sql
-- Grace period for late payments
SELECT 
    o.order_id,
    o.amount as order_amount,
    p.amount as payment_amount,
    p.payment_method
FROM orders o
LEFT JOIN payments p ON o.order_id = p.order_id
WITHIN INTERVAL '10' MINUTES;
```

#### Event Correlation
```sql
-- Correlate user actions within session
SELECT 
    login.user_id,
    login.login_time,
    action.action_type,
    action.action_time
FROM user_logins login
INNER JOIN user_actions action ON login.user_id = action.user_id
WITHIN INTERVAL '1' HOUR;
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

### HAVING Clause Support

The HAVING clause enables post-aggregation filtering, allowing you to filter results after GROUP BY operations:

```sql
-- Filter high-activity customers
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 10 AND SUM(amount) > 1000;

-- Social media trending hashtags with HAVING
SELECT 
    SUBSTRING(content, POSITION('#', content), POSITION(' ', content, POSITION('#', content)) - POSITION('#', content)) as hashtag,
    COUNT(*) as mention_count,
    COUNT(DISTINCT user_id) as unique_users
FROM social_posts
WHERE POSITION('#', content) > 0
GROUP BY SUBSTRING(content, POSITION('#', content), POSITION(' ', content, POSITION('#', content)) - POSITION('#', content))
HAVING COUNT(*) > 100;

-- High-value influencer activity
SELECT 
    user_id,
    username,
    follower_count,
    COUNT(*) as post_count
FROM social_posts
WHERE follower_count > 10000
GROUP BY user_id, username, follower_count
HAVING COUNT(*) > 5;
```

## Built-in Functions

### Window Functions

Window functions perform calculations across a set of rows related to the current row using OVER clauses. FerrisStreams supports the complete set of standard SQL window functions with streaming-aware semantics.

#### Available Window Functions

**Ranking Functions:**
- `ROW_NUMBER()` - Assigns unique sequential integers to rows within each partition
- `RANK()` - Assigns ranks with gaps for tied values
- `DENSE_RANK()` - Assigns ranks without gaps for tied values
- `PERCENT_RANK()` - Calculates the percentile rank of a row within the partition

**Value Access Functions:**
- `LAG(expr [, offset [, default]])` - Accesses previous row values
- `LEAD(expr [, offset [, default]])` - Accesses following row values
- `FIRST_VALUE(expr)` - Returns the first value in the partition
- `LAST_VALUE(expr)` - Returns the last value in the current partition frame
- `NTH_VALUE(expr, n)` - Returns the nth value in the partition (1-indexed)

**Distribution Functions:**
- `CUME_DIST()` - Calculates the cumulative distribution of a row
- `NTILE(n)` - Divides the partition into n buckets and assigns bucket numbers

#### Basic Window Function Examples

```sql
-- ROW_NUMBER: Sequential numbering within partitions
SELECT 
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_sequence
FROM orders;

-- RANK and DENSE_RANK: Ranking with and without gaps
SELECT 
    customer_id,
    amount,
    RANK() OVER (ORDER BY amount DESC) as amount_rank,
    DENSE_RANK() OVER (ORDER BY amount DESC) as amount_dense_rank
FROM orders;

-- PERCENT_RANK: Percentile ranking
SELECT 
    customer_id,
    amount,
    PERCENT_RANK() OVER (ORDER BY amount) as amount_percentile
FROM orders;
```

#### LAG and LEAD Functions

```sql
-- LAG: Access previous row values
SELECT 
    customer_id,
    order_date,
    amount,
    LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_amount,
    LAG(amount, 2) OVER (PARTITION BY customer_id ORDER BY order_date) as amount_2_orders_ago,
    LAG(amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_amount_with_default
FROM orders;

-- LEAD: Access following row values (returns NULL in streaming context)
SELECT 
    customer_id,
    order_date,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as next_amount,
    LEAD(amount, 1, -999) OVER (PARTITION BY customer_id ORDER BY order_date) as next_with_default
FROM orders;
```

#### Value Access Functions

```sql
-- FIRST_VALUE and LAST_VALUE: Access boundary values
SELECT 
    customer_id,
    order_date,
    amount,
    FIRST_VALUE(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as first_order_amount,
    LAST_VALUE(amount) OVER (PARTITION BY customer_id ORDER BY order_date 
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as current_last_amount
FROM orders;

-- NTH_VALUE: Access specific position values
SELECT 
    customer_id,
    order_date,
    amount,
    NTH_VALUE(amount, 2) OVER (PARTITION BY customer_id ORDER BY order_date) as second_order_amount,
    NTH_VALUE(amount, 3) OVER (PARTITION BY customer_id ORDER BY order_date) as third_order_amount
FROM orders;
```

#### Distribution Functions

```sql
-- CUME_DIST: Cumulative distribution
SELECT 
    customer_id,
    amount,
    CUME_DIST() OVER (ORDER BY amount) as cumulative_distribution,
    CUME_DIST() OVER (PARTITION BY customer_tier ORDER BY amount) as tier_distribution
FROM orders;

-- NTILE: Divide into buckets
SELECT 
    customer_id,
    amount,
    NTILE(4) OVER (ORDER BY amount) as quartile,
    NTILE(10) OVER (ORDER BY amount) as decile,
    NTILE(100) OVER (ORDER BY amount) as percentile
FROM orders;
```

#### Advanced Window Function Patterns

```sql
-- Customer behavior analysis
SELECT 
    customer_id,
    order_date,
    amount,
    -- Order sequence and gaps
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_number,
    LAG(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_order_date,
    -- Running analytics
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date 
                      ROWS UNBOUNDED PRECEDING) as customer_lifetime_value,
    AVG(amount) OVER (PARTITION BY customer_id ORDER BY order_date 
                      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as recent_avg_order,
    -- Comparative analysis
    amount / FIRST_VALUE(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as vs_first_order_ratio,
    RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as customer_order_rank
FROM orders;

-- Streaming analytics with time-based analysis
SELECT 
    device_id,
    reading_timestamp,
    temperature,
    -- Sequential analysis
    LAG(temperature, 1) OVER (PARTITION BY device_id ORDER BY reading_timestamp) as prev_temp,
    temperature - LAG(temperature, 1) OVER (PARTITION BY device_id ORDER BY reading_timestamp) as temp_change,
    -- Ranking and percentiles
    PERCENT_RANK() OVER (ORDER BY temperature) as temp_percentile,
    NTILE(5) OVER (ORDER BY temperature) as temp_quintile,
    -- Boundary analysis
    FIRST_VALUE(temperature) OVER (PARTITION BY device_id ORDER BY reading_timestamp) as session_start_temp,
    LAST_VALUE(temperature) OVER (PARTITION BY device_id ORDER BY reading_timestamp 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as current_last_temp
FROM sensor_readings;
```

#### Streaming Considerations

In streaming contexts, window functions behave differently than in batch processing:

1. **LAG functions** work normally, accessing previous values within partitions
2. **LEAD functions** typically return NULL or default values since future data isn't available
3. **FIRST_VALUE** returns the first value seen in the current partition
4. **LAST_VALUE** returns the most recent value in the streaming window
5. **RANK/DENSE_RANK** provide relative ranking within the current data
6. **PERCENT_RANK/CUME_DIST** calculate percentiles based on currently available data
7. **NTILE** distributes current data into buckets

### Statistical Functions

FerrisStreams provides advanced statistical functions for data analysis and scientific computing. These functions work with numeric data and provide various statistical measures.

#### Available Statistical Functions

**Standard Deviation Functions:**
- `STDDEV(expr)` - Standard deviation (sample)
- `STDDEV_SAMP(expr)` - Sample standard deviation (same as STDDEV)
- `STDDEV_POP(expr)` - Population standard deviation

**Variance Functions:**
- `VARIANCE(expr)` - Variance (sample)
- `VAR_SAMP(expr)` - Sample variance (same as VARIANCE)
- `VAR_POP(expr)` - Population variance

**Central Tendency:**
- `MEDIAN(expr)` - Median value (middle value or average of two middle values)

#### Statistical Function Examples

```sql
-- Basic statistical analysis
SELECT 
    product_category,
    COUNT(*) as sample_size,
    AVG(price) as mean_price,
    MEDIAN(price) as median_price,
    STDDEV(price) as price_stddev,
    VARIANCE(price) as price_variance
FROM products
GROUP BY product_category
HAVING COUNT(*) > 10;

-- Population vs Sample statistics
SELECT 
    region,
    -- Sample statistics (for sample data)
    STDDEV_SAMP(revenue) as sample_stddev,
    VAR_SAMP(revenue) as sample_variance,
    -- Population statistics (for complete data)
    STDDEV_POP(revenue) as population_stddev,
    VAR_POP(revenue) as population_variance,
    -- Central tendency
    MEDIAN(revenue) as median_revenue
FROM sales_data
GROUP BY region;
```

#### Window Functions with Statistical Analysis

```sql
-- Rolling statistical analysis
SELECT 
    order_date,
    daily_revenue,
    -- Rolling statistics over 7-day window
    STDDEV(daily_revenue) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7day_stddev,
    VARIANCE(daily_revenue) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7day_variance,
    MEDIAN(daily_revenue) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7day_median
FROM (
    SELECT 
        DATE(order_timestamp) as order_date,
        SUM(amount) as daily_revenue
    FROM orders
    GROUP BY DATE(order_timestamp)
) daily_sales;

-- Statistical analysis by segments
SELECT 
    customer_tier,
    product_category,
    -- Segment statistics
    COUNT(*) as order_count,
    AVG(amount) as mean_amount,
    MEDIAN(amount) as median_amount,
    STDDEV(amount) as amount_stddev,
    VARIANCE(amount) as amount_variance,
    -- Distribution analysis
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    (MAX(amount) - MIN(amount)) as range_amount
FROM orders
WHERE order_date >= NOW() - INTERVAL '30' DAYS
GROUP BY customer_tier, product_category
HAVING COUNT(*) >= 20;  -- Sufficient sample size
```

#### Advanced Statistical Patterns

```sql
-- Anomaly detection using statistical functions
SELECT 
    device_id,
    reading_timestamp,
    temperature,
    -- Statistical context
    AVG(temperature) OVER (PARTITION BY device_id) as device_mean_temp,
    STDDEV(temperature) OVER (PARTITION BY device_id) as device_stddev_temp,
    MEDIAN(temperature) OVER (PARTITION BY device_id) as device_median_temp,
    -- Z-score for outlier detection
    ABS(temperature - AVG(temperature) OVER (PARTITION BY device_id)) / 
        STDDEV(temperature) OVER (PARTITION BY device_id) as z_score,
    -- Anomaly classification
    CASE 
        WHEN ABS(temperature - AVG(temperature) OVER (PARTITION BY device_id)) / 
             STDDEV(temperature) OVER (PARTITION BY device_id) > 2 THEN 'OUTLIER'
        WHEN ABS(temperature - MEDIAN(temperature) OVER (PARTITION BY device_id)) > 
             1.5 * STDDEV(temperature) OVER (PARTITION BY device_id) THEN 'MODERATE_DEVIATION'
        ELSE 'NORMAL'
    END as anomaly_status
FROM sensor_readings
WHERE reading_timestamp >= NOW() - INTERVAL '24' HOURS;

-- Quality control with statistical process control
SELECT 
    production_line,
    measurement_time,
    measurement_value,
    -- Control limits (using 3-sigma rule)
    AVG(measurement_value) OVER (PARTITION BY production_line) as process_mean,
    AVG(measurement_value) OVER (PARTITION BY production_line) + 
        3 * STDDEV(measurement_value) OVER (PARTITION BY production_line) as upper_control_limit,
    AVG(measurement_value) OVER (PARTITION BY production_line) - 
        3 * STDDEV(measurement_value) OVER (PARTITION BY production_line) as lower_control_limit,
    -- Process capability
    STDDEV(measurement_value) OVER (PARTITION BY production_line) as process_variation,
    -- Alert status
    CASE 
        WHEN measurement_value > AVG(measurement_value) OVER (PARTITION BY production_line) + 
                                3 * STDDEV(measurement_value) OVER (PARTITION BY production_line) THEN 'OUT_OF_CONTROL_HIGH'
        WHEN measurement_value < AVG(measurement_value) OVER (PARTITION BY production_line) - 
                                3 * STDDEV(measurement_value) OVER (PARTITION BY production_line) THEN 'OUT_OF_CONTROL_LOW'
        ELSE 'IN_CONTROL'
    END as control_status
FROM quality_measurements;
```

#### Statistical Function Error Handling

```sql
-- Safe statistical calculations with error handling
SELECT 
    product_category,
    COUNT(*) as sample_count,
    -- Handle insufficient data
    CASE 
        WHEN COUNT(*) > 1 THEN STDDEV(price)
        ELSE NULL
    END as price_stddev,
    CASE 
        WHEN COUNT(*) > 0 THEN MEDIAN(price)
        ELSE NULL
    END as median_price,
    -- Coefficient of variation (CV)
    CASE 
        WHEN AVG(price) > 0 AND COUNT(*) > 1 THEN 
            STDDEV(price) / AVG(price) * 100
        ELSE NULL
    END as coefficient_of_variation_pct
FROM products
WHERE price IS NOT NULL
GROUP BY product_category;
```

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

-- String aggregation with LISTAGG
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    LISTAGG(product_name, ', ') as purchased_products,
    LISTAGG(DISTINCT category, '; ') as product_categories
FROM order_items
GROUP BY customer_id
HAVING COUNT(*) > 5;

-- Crisis monitoring with location aggregation
SELECT 
    crisis_type,
    COUNT(*) as mention_count,
    COUNT(DISTINCT user_id) as unique_reporters,
    LISTAGG(DISTINCT location, ', ') as affected_locations,
    MIN(timestamp) as first_mention,
    MAX(timestamp) as latest_mention
FROM crisis_reports
GROUP BY crisis_type
HAVING COUNT(*) > 50;

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

-- String position finding
SELECT 
    customer_id,
    email,
    POSITION('@', email) as at_position,
    POSITION('.', email, POSITION('@', email)) as domain_dot_position
FROM customers;
```

### Advanced String Processing with POSITION

```sql
-- Extract hashtags from social media content
SELECT 
    post_id,
    content,
    POSITION('#', content) as hashtag_start,
    SUBSTRING(content, POSITION('#', content), POSITION(' ', content, POSITION('#', content)) - POSITION('#', content)) as hashtag
FROM social_posts
WHERE POSITION('#', content) > 0;

-- Find email domains
SELECT 
    customer_id,
    email,
    SUBSTRING(email, POSITION('@', email) + 1) as email_domain
FROM customers
WHERE POSITION('@', email) > 0;
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

-- Date difference calculations
SELECT 
    order_id,
    _timestamp as order_time,
    DATEDIFF('seconds', order_timestamp, NOW()) as seconds_since_order,
    DATEDIFF('minutes', order_timestamp, NOW()) as minutes_since_order,
    DATEDIFF('hours', order_timestamp, NOW()) as hours_since_order,
    DATEDIFF('days', order_timestamp, NOW()) as days_since_order
FROM orders;

-- IoT sensor data time analysis
SELECT 
    device_id,
    sensor_reading,
    last_charge_time,
    DATEDIFF('hours', last_charge_time, TIMESTAMP()) as hours_since_charge,
    CASE 
        WHEN DATEDIFF('hours', last_charge_time, TIMESTAMP()) > 24 THEN 'LOW_BATTERY'
        ELSE 'NORMAL'
    END as battery_status
FROM iot_sensors;
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

### CASE WHEN Expressions

CASE WHEN expressions provide conditional logic in SQL queries, allowing for complex decision trees and conditional value assignment.

#### Basic CASE WHEN Syntax

```sql
-- Simple CASE WHEN expression
SELECT 
    customer_id,
    order_amount,
    CASE 
        WHEN order_amount > 1000 THEN 'VIP'
        WHEN order_amount > 500 THEN 'Premium'
        WHEN order_amount > 100 THEN 'Standard'
        ELSE 'Basic'
    END as customer_tier
FROM orders;

-- CASE without ELSE (returns NULL for unmatched conditions)
SELECT 
    product_id,
    status,
    CASE 
        WHEN status = 'active' THEN 'Available'
        WHEN status = 'maintenance' THEN 'Temporarily Unavailable'
    END as availability_message
FROM products;
```

#### CASE WHEN in Conditional Aggregations

One of the most powerful features is using CASE WHEN expressions within aggregate functions for conditional aggregations:

```sql
-- Conditional counting and summing
SELECT 
    store_id,
    COUNT(*) as total_orders,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_orders,
    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_revenue,
    SUM(CASE WHEN priority = 'urgent' THEN amount ELSE 0 END) as urgent_revenue,
    AVG(CASE WHEN rating >= 4 THEN rating END) as avg_good_rating
FROM orders
GROUP BY store_id;

-- Multi-dimensional analysis with conditional aggregations
SELECT 
    product_category,
    COUNT(*) as total_sales,
    -- Count by price tier
    COUNT(CASE WHEN price > 100 THEN 1 END) as premium_sales,
    COUNT(CASE WHEN price BETWEEN 50 AND 100 THEN 1 END) as mid_tier_sales,
    COUNT(CASE WHEN price < 50 THEN 1 END) as budget_sales,
    -- Revenue by customer type
    SUM(CASE WHEN customer_type = 'B2B' THEN amount ELSE 0 END) as b2b_revenue,
    SUM(CASE WHEN customer_type = 'B2C' THEN amount ELSE 0 END) as b2c_revenue,
    -- Average satisfaction by tier
    AVG(CASE WHEN customer_tier = 'VIP' THEN satisfaction_score END) as vip_satisfaction
FROM sales
GROUP BY product_category
HAVING COUNT(*) > 100;
```

#### Advanced CASE WHEN Patterns

```sql
-- Nested CASE expressions for complex logic
SELECT 
    customer_id,
    order_date,
    amount,
    CASE 
        WHEN customer_type = 'VIP' THEN 
            CASE 
                WHEN amount > 5000 THEN 'VIP_PLATINUM'
                WHEN amount > 1000 THEN 'VIP_GOLD'
                ELSE 'VIP_STANDARD'
            END
        WHEN customer_type = 'REGULAR' THEN
            CASE 
                WHEN amount > 2000 THEN 'REGULAR_HIGH'
                WHEN amount > 500 THEN 'REGULAR_MEDIUM'
                ELSE 'REGULAR_LOW'
            END
        ELSE 'NEW_CUSTOMER'
    END as detailed_segment
FROM orders;

-- CASE with complex conditions
SELECT 
    order_id,
    customer_id,
    amount,
    order_date,
    CASE 
        WHEN amount > 1000 AND customer_tier = 'VIP' AND EXTRACT('HOUR', order_date) BETWEEN 9 AND 17 THEN 'PRIORITY_PROCESSING'
        WHEN amount > 500 OR (customer_tier = 'PREMIUM' AND payment_method = 'CREDIT') THEN 'FAST_PROCESSING'
        WHEN EXTRACT('DAY_OF_WEEK', order_date) IN (6, 7) THEN 'WEEKEND_PROCESSING'
        ELSE 'STANDARD_PROCESSING'
    END as processing_priority
FROM orders;

-- Real-time alert generation with CASE
SELECT 
    device_id,
    sensor_reading,
    temperature,
    humidity,
    CASE 
        WHEN temperature > 85 AND humidity > 70 THEN 'CRITICAL_ALERT'
        WHEN temperature > 80 OR humidity > 80 THEN 'WARNING'
        WHEN sensor_reading IS NULL THEN 'SENSOR_OFFLINE'
        ELSE 'NORMAL'
    END as alert_status,
    CASE 
        WHEN temperature > 85 AND humidity > 70 THEN 'Immediate attention required'
        WHEN temperature > 80 THEN 'Temperature threshold exceeded'
        WHEN humidity > 80 THEN 'Humidity threshold exceeded'
        WHEN sensor_reading IS NULL THEN 'Device connection lost'
        ELSE NULL
    END as alert_message
FROM environmental_sensors;
```

#### CASE WHEN for Data Transformation

```sql
-- Data normalization and standardization
SELECT 
    user_id,
    raw_country_input,
    CASE 
        WHEN UPPER(raw_country_input) IN ('US', 'USA', 'UNITED STATES', 'AMERICA') THEN 'United States'
        WHEN UPPER(raw_country_input) IN ('UK', 'GB', 'GREAT BRITAIN', 'ENGLAND') THEN 'United Kingdom'
        WHEN UPPER(raw_country_input) IN ('DE', 'GERMANY', 'DEUTSCHLAND') THEN 'Germany'
        WHEN LENGTH(raw_country_input) = 2 THEN UPPER(raw_country_input)  -- ISO country codes
        ELSE INITCAP(raw_country_input)  -- Capitalize first letter of each word
    END as normalized_country
FROM user_data;

-- Business logic implementation
SELECT 
    product_id,
    base_price,
    quantity,
    customer_tier,
    region,
    CASE 
        WHEN customer_tier = 'VIP' AND quantity >= 100 THEN base_price * 0.8  -- 20% discount
        WHEN customer_tier = 'VIP' AND quantity >= 50 THEN base_price * 0.85   -- 15% discount
        WHEN customer_tier = 'VIP' THEN base_price * 0.9                       -- 10% discount
        WHEN quantity >= 100 THEN base_price * 0.9                             -- 10% volume discount
        WHEN region = 'DEVELOPING' THEN base_price * 0.95                      -- 5% regional discount
        ELSE base_price
    END as final_price
FROM order_items;
```

### INTERVAL Arithmetic

INTERVAL arithmetic enables time-based calculations for temporal analysis, scheduling, and time window operations.

#### Basic INTERVAL Usage

```sql
-- INTERVAL literals and arithmetic
SELECT 
    order_id,
    order_timestamp,
    order_timestamp + INTERVAL '1' HOUR as estimated_preparation,
    order_timestamp + INTERVAL '30' MINUTES as pickup_ready_time,
    order_timestamp + INTERVAL '2' HOURS as delivery_window_start,
    order_timestamp + INTERVAL '4' HOURS as delivery_window_end
FROM orders;

-- Working with different time units
SELECT 
    event_id,
    event_timestamp,
    event_timestamp - INTERVAL '5' MINUTES as pre_event_window,
    event_timestamp + INTERVAL '1' DAY as followup_date,
    event_timestamp + INTERVAL '7' DAYS as weekly_followup,
    event_timestamp + INTERVAL '1000' MILLISECONDS as microsecond_precision
FROM events;
```

#### INTERVAL with System Columns

```sql
-- Using INTERVAL with system columns for stream processing
SELECT 
    message_id,
    _timestamp as message_time,
    _timestamp + INTERVAL '15' MINUTES as alert_window_end,
    _timestamp - INTERVAL '1' HOUR as lookback_window_start,
    CASE 
        WHEN _timestamp + INTERVAL '10' MINUTES > TIMESTAMP() THEN 'RECENT'
        WHEN _timestamp + INTERVAL '1' HOUR > TIMESTAMP() THEN 'CURRENT'
        ELSE 'HISTORICAL'
    END as message_age_category
FROM message_stream;

-- Time-based filtering with INTERVAL
SELECT *
FROM sensor_readings
WHERE _timestamp > (_timestamp - INTERVAL '5' MINUTES)
  AND temperature > 75.0;
```

#### Advanced INTERVAL Operations

```sql
-- Complex time window analysis
SELECT 
    user_id,
    session_start,
    session_end,
    session_end - session_start as session_duration_ms,
    CASE 
        WHEN (session_end - session_start) > INTERVAL '30' MINUTES THEN 'LONG_SESSION'
        WHEN (session_end - session_start) > INTERVAL '10' MINUTES THEN 'MEDIUM_SESSION'
        WHEN (session_end - session_start) > INTERVAL '2' MINUTES THEN 'SHORT_SESSION'
        ELSE 'VERY_SHORT_SESSION'
    END as session_category
FROM user_sessions;

-- Sliding time windows for streaming analytics
SELECT 
    device_id,
    reading_timestamp,
    sensor_value,
    AVG(sensor_value) OVER (
        PARTITION BY device_id 
        ORDER BY reading_timestamp 
        RANGE BETWEEN INTERVAL '15' MINUTES PRECEDING AND CURRENT ROW
    ) as rolling_15min_avg,
    CASE 
        WHEN reading_timestamp - LAG(reading_timestamp) OVER (PARTITION BY device_id ORDER BY reading_timestamp) > INTERVAL '5' MINUTES 
        THEN 'DATA_GAP'
        ELSE 'CONTINUOUS'
    END as data_continuity
FROM sensor_data;

-- Business hours and scheduling logic
SELECT 
    order_id,
    order_timestamp,
    CASE 
        WHEN EXTRACT('HOUR', order_timestamp) BETWEEN 9 AND 17 THEN 'BUSINESS_HOURS'
        WHEN EXTRACT('HOUR', order_timestamp) BETWEEN 7 AND 21 THEN 'EXTENDED_HOURS'
        ELSE 'AFTER_HOURS'
    END as time_category,
    CASE 
        WHEN EXTRACT('HOUR', order_timestamp) BETWEEN 9 AND 17 THEN 
            order_timestamp + INTERVAL '2' HOURS  -- Standard processing
        WHEN EXTRACT('HOUR', order_timestamp) BETWEEN 18 AND 21 THEN 
            order_timestamp + INTERVAL '12' HOURS -- Next business day
        ELSE 
            order_timestamp + INTERVAL '1' DAY    -- Next day processing
    END as expected_processing_time
FROM orders;
```

#### Real-time Temporal Analysis

```sql
-- Real-time alerting with time-based conditions
SELECT 
    sensor_id,
    reading_timestamp,
    temperature,
    CASE 
        WHEN temperature > 80 AND 
             reading_timestamp > (TIMESTAMP() - INTERVAL '5' MINUTES) THEN 'IMMEDIATE_ALERT'
        WHEN temperature > 75 AND 
             reading_timestamp > (TIMESTAMP() - INTERVAL '10' MINUTES) THEN 'WARNING'
        ELSE 'NORMAL'
    END as alert_level,
    TIMESTAMP() - reading_timestamp as data_age_ms
FROM temperature_sensors
WHERE reading_timestamp > (TIMESTAMP() - INTERVAL '1' HOUR);

-- Time-based aggregation windows
SELECT 
    DATE_FORMAT(_timestamp, '%Y-%m-%d %H:00:00') as hourly_bucket,
    COUNT(*) as event_count,
    COUNT(CASE WHEN severity = 'HIGH' THEN 1 END) as high_severity_events,
    AVG(CASE WHEN response_time IS NOT NULL THEN response_time END) as avg_response_time,
    MIN(_timestamp) as window_start,
    MAX(_timestamp) as window_end,
    MAX(_timestamp) - MIN(_timestamp) as actual_window_duration
FROM system_events
WHERE _timestamp > (TIMESTAMP() - INTERVAL '24' HOURS)
GROUP BY DATE_FORMAT(_timestamp, '%Y-%m-%d %H:00:00')
ORDER BY hourly_bucket DESC;
```

#### Supported INTERVAL Units

- `MILLISECONDS` / `MILLISECOND`: Precise timing for high-frequency operations
- `SECONDS` / `SECOND`: Standard time operations
- `MINUTES` / `MINUTE`: Short-term scheduling and windows
- `HOURS` / `HOUR`: Business logic and daily operations
- `DAYS` / `DAY`: Long-term planning and retention

```sql
-- Examples of all supported units
SELECT 
    event_id,
    base_timestamp,
    base_timestamp + INTERVAL '500' MILLISECONDS as precise_timing,
    base_timestamp + INTERVAL '30' SECONDS as half_minute_later,
    base_timestamp + INTERVAL '15' MINUTES as quarter_hour_later,
    base_timestamp + INTERVAL '4' HOURS as same_day_later,
    base_timestamp + INTERVAL '7' DAYS as one_week_later
FROM scheduled_events;
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

FerrisStreams provides comprehensive header manipulation capabilities, allowing you to both read from and write to Kafka message headers during stream processing.

#### Reading Header Functions

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

#### Writing Header Functions

```sql
-- Set headers with static values
SELECT 
    order_id,
    SET_HEADER('processed_by', 'ferris-streams') as status,
    SET_HEADER('processing_timestamp', NOW()) as timestamp_set,
    SET_HEADER('version', '1.0.0') as version_result
FROM orders;

-- Set headers with field values
SELECT 
    order_id,
    customer_id,
    SET_HEADER('customer_tier', customer_tier) as tier_set,
    SET_HEADER('order_amount', amount) as amount_set
FROM orders;

-- Remove headers
SELECT 
    order_id,
    REMOVE_HEADER('temporary_flag') as removed_value,
    REMOVE_HEADER('old_trace_id') as old_trace
FROM orders;

-- Complex header operations
SELECT 
    order_id,
    -- Set computed headers
    SET_HEADER('order_summary', CONCAT('Order ', order_id, ' for $', amount)) as summary,
    SET_HEADER('processing_tier', 
        CASE 
            WHEN amount > 1000 THEN 'priority'
            WHEN amount > 100 THEN 'standard'
            ELSE 'basic'
        END
    ) as tier_result,
    -- Clean up old headers
    REMOVE_HEADER('temp_status') as temp_removed,
    -- Conditional header setting
    CASE 
        WHEN customer_tier = 'VIP' THEN SET_HEADER('priority_processing', 'true')
        ELSE NULL
    END as priority_set
FROM orders;
```

#### Header Function Behavior

**SET_HEADER(key, value)**
- Converts both key and value to strings automatically
- Returns the string value that was set
- Overwrites existing headers with the same key
- Handles all data types: integers, floats, booleans, NULL values

**REMOVE_HEADER(key)**
- Converts the key to string automatically
- Returns the original header value if it existed, NULL if it didn't
- Safe to call on non-existent headers

#### Advanced Header Manipulation Patterns

```sql
-- Audit trail with headers
SELECT 
    transaction_id,
    amount,
    SET_HEADER('audit_user', user_id) as audit_user_set,
    SET_HEADER('audit_timestamp', DATE_FORMAT(NOW(), '%Y-%m-%d %H:%M:%S')) as audit_time_set,
    SET_HEADER('audit_operation', 'PROCESSED') as audit_op_set,
    -- Remove temporary processing flags
    REMOVE_HEADER('temp_lock') as lock_removed,
    REMOVE_HEADER('processing_flag') as flag_removed
FROM financial_transactions;

-- Data lineage tracking
SELECT 
    record_id,
    -- Track processing pipeline
    SET_HEADER('pipeline_stage', 'enrichment') as stage_set,
    SET_HEADER('source_topic', 'raw_events') as source_set,
    SET_HEADER('transformation_version', '2.1.0') as version_set,
    -- Preserve original trace
    COALESCE(HEADER('original_trace_id'), SET_HEADER('original_trace_id', HEADER('trace_id'))) as trace_preserved
FROM event_stream;

-- Error handling and monitoring
SELECT 
    message_id,
    CASE 
        WHEN validation_error IS NOT NULL THEN 
            SET_HEADER('error_details', validation_error)
        ELSE 
            REMOVE_HEADER('error_details')
    END as error_handling,
    SET_HEADER('processing_status', 
        CASE 
            WHEN validation_error IS NOT NULL THEN 'FAILED'
            ELSE 'SUCCESS'
        END
    ) as status_set
FROM message_validation_results;

-- Multi-tenant header management
SELECT 
    tenant_id,
    user_id,
    -- Set tenant context
    SET_HEADER('tenant_id', tenant_id) as tenant_set,
    SET_HEADER('tenant_region', tenant_config.region) as region_set,
    -- Remove sensitive information
    REMOVE_HEADER('internal_user_id') as internal_removed,
    REMOVE_HEADER('debug_info') as debug_removed
FROM user_events
JOIN tenant_configs ON user_events.tenant_id = tenant_configs.tenant_id;
```

#### Integration with Other Functions

Header functions work seamlessly with all other SQL functions:

```sql
-- With string functions
SELECT 
    order_id,
    SET_HEADER('upper_status', UPPER(order_status)) as upper_status,
    SET_HEADER('order_summary', CONCAT('Order #', order_id, ' - ', LEFT(description, 50))) as summary
FROM orders;

-- With mathematical functions
SELECT 
    sensor_id,
    temperature,
    SET_HEADER('temp_rounded', ROUND(temperature, 1)) as temp_rounded,
    SET_HEADER('temp_alert', 
        CASE 
            WHEN ABS(temperature - 20) > 5 THEN 'ANOMALY'
            ELSE 'NORMAL'
        END
    ) as alert_set
FROM sensor_readings;

-- With date/time functions
SELECT 
    event_id,
    SET_HEADER('processed_date', DATE_FORMAT(NOW(), '%Y-%m-%d')) as date_set,
    SET_HEADER('processing_hour', EXTRACT('HOUR', NOW())) as hour_set,
    SET_HEADER('age_minutes', DATEDIFF('minutes', _timestamp, NOW())) as age_set
FROM events;
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

### Window Functions with OVER Clauses

Window functions perform calculations across a set of rows related to the current row using OVER clauses. They support PARTITION BY, ORDER BY, and frame specifications with ROWS BETWEEN and RANGE BETWEEN clauses.

#### Basic Window Functions

```sql
-- ROW_NUMBER: Assign unique row numbers within partitions
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_sequence
FROM orders;

-- RANK and DENSE_RANK: Ranking functions
SELECT 
    order_id,
    customer_id,
    amount,
    RANK() OVER (ORDER BY amount DESC) as amount_rank,
    DENSE_RANK() OVER (ORDER BY amount DESC) as amount_dense_rank
FROM orders;
```

#### LAG and LEAD Functions

```sql
-- LAG: Access previous row values
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_amount,
    LAG(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_order_date
FROM orders;

-- LEAD: Access following row values  
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as next_amount,
    LEAD(order_date, 2) OVER (PARTITION BY customer_id ORDER BY order_date) as next_next_order_date
FROM orders;
```

#### Window Frames with ROWS BETWEEN

Window frames define the subset of rows within the partition for calculation. ROWS BETWEEN clauses specify physical row boundaries.

```sql
-- Running totals with UNBOUNDED PRECEDING
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total
FROM orders;

-- Moving averages with physical row windows
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    AVG(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3_orders
FROM orders;

-- Forward-looking calculations
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) as next_3_orders_total
FROM orders;
```

#### Window Frames with RANGE BETWEEN

RANGE BETWEEN specifies logical value ranges instead of physical row counts.

```sql
-- Range-based window for time intervals
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    COUNT(*) OVER (
        ORDER BY order_date
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as orders_up_to_date,
    SUM(amount) OVER (
        ORDER BY order_date
        RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING  
    ) as remaining_revenue
FROM orders;
```

#### Complex Window Specifications

```sql
-- Multiple window functions with different frames
SELECT 
    order_id,
    customer_id,
    product_category,
    order_date,
    amount,
    
    -- Row number within customer partition
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as customer_order_sequence,
    
    -- Running sum of customer orders
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS UNBOUNDED PRECEDING
    ) as customer_lifetime_value,
    
    -- Moving average over last 5 orders per category
    AVG(amount) OVER (
        PARTITION BY product_category
        ORDER BY order_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as category_moving_avg,
    
    -- Percentage of total revenue
    amount / SUM(amount) OVER () * 100 as pct_of_total_revenue,
    
    -- Compare with previous and next orders
    LAG(amount, 1) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as prev_order_amount,
    
    LEAD(amount, 1) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date  
    ) as next_order_amount
FROM orders;
```

#### Streaming Analytics with Window Functions

```sql
-- Real-time customer behavior analysis
SELECT 
    customer_id,
    order_date,
    amount,
    product_category,
    
    -- Customer order frequency
    DATEDIFF('days', 
        LAG(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date),
        order_date
    ) as days_since_last_order,
    
    -- Customer spending trend (3-order moving average)
    AVG(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as spending_trend,
    
    -- Category rank within time window
    DENSE_RANK() OVER (
        PARTITION BY product_category
        ORDER BY order_date DESC
    ) as category_recency_rank,
    
    -- Running customer value percentile
    PERCENT_RANK() OVER (
        ORDER BY SUM(amount) OVER (
            PARTITION BY customer_id 
            ORDER BY order_date
            ROWS UNBOUNDED PRECEDING
        )
    ) as customer_value_percentile
FROM orders
WHERE order_date >= NOW() - INTERVAL '30' DAYS;
```

#### Performance Considerations

1. **Partition Strategy**: Use PARTITION BY to limit window scope and improve performance
2. **Frame Specification**: Smaller frames (e.g., ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) are more efficient than unbounded frames
3. **Ordering**: Ensure ORDER BY columns are indexed for optimal performance
4. **Memory Usage**: Large partitions with unbounded frames may consume significant memory

#### Common Window Function Patterns

```sql
-- Pattern 1: Top-N per group
SELECT *
FROM (
    SELECT 
        customer_id,
        order_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rn
    FROM orders
) ranked
WHERE rn <= 3;  -- Top 3 orders per customer

-- Pattern 2: Running totals and percentages
SELECT 
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (ORDER BY order_date) as cumulative_revenue,
    daily_revenue / SUM(daily_revenue) OVER () * 100 as pct_of_total
FROM (
    SELECT 
        DATE(order_date) as order_date,
        SUM(amount) as daily_revenue
    FROM orders
    GROUP BY DATE(order_date)
) daily_totals;

-- Pattern 3: Change detection
SELECT 
    customer_id,
    order_date,
    amount,
    LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_amount,
    CASE 
        WHEN amount > LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) THEN 'INCREASE'
        WHEN amount < LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) THEN 'DECREASE'
        ELSE 'SAME'
    END as amount_trend
FROM orders;
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

#### 5. Real-Time Data Enrichment with JOINs

```sql
-- Enrich streaming orders with customer and product data
DEPLOY JOB order_enrichment VERSION '1.0.0' AS
SELECT 
    o.order_id,
    o.order_date,
    o.quantity,
    c.customer_name,
    c.customer_tier,
    c.email,
    p.product_name,
    p.category,
    p.unit_price,
    o.quantity * p.unit_price as line_total
FROM streaming_orders o
INNER JOIN customer_table c ON o.customer_id = c.customer_id
INNER JOIN product_catalog p ON o.product_id = p.product_id
WHERE c.customer_tier IN ('gold', 'platinum')
STRATEGY BLUE_GREEN;
```

#### 6. Event Correlation with Windowed JOINs

```sql
-- Correlate user clicks with purchases within 30 minutes
START JOB click_to_purchase_correlation AS
SELECT 
    click.user_id,
    click.page_url,
    click.click_timestamp,
    purchase.order_id,
    purchase.amount,
    purchase.purchase_timestamp,
    (purchase.purchase_timestamp - click.click_timestamp) / 1000 / 60 as minutes_to_purchase
FROM user_clicks click
INNER JOIN user_purchases purchase ON click.user_id = purchase.user_id
WITHIN INTERVAL '30' MINUTES
WHERE click.page_url LIKE '%product%';
```

#### 7. Multi-Stream Fraud Detection

```sql
-- Advanced fraud detection with multiple stream correlation
DEPLOY JOB fraud_detection_advanced VERSION '1.0.0' AS
SELECT 
    t.transaction_id,
    t.user_id,
    t.amount,
    t.merchant,
    u.home_country,
    u.account_creation_date,
    l.current_country,
    l.ip_address,
    CASE 
        WHEN t.amount > 5000 AND u.account_creation_date > (NOW() - 86400000) THEN 'HIGH_RISK'
        WHEN l.current_country != u.home_country THEN 'LOCATION_RISK'
        WHEN t.amount > 1000 AND COUNT(*) OVER (
            PARTITION BY t.user_id 
            WINDOW TUMBLING(5m)
        ) > 3 THEN 'VELOCITY_RISK'
        ELSE 'LOW_RISK'
    END as risk_level
FROM transactions t
INNER JOIN user_profiles u ON t.user_id = u.user_id
LEFT JOIN user_locations l ON t.user_id = l.user_id
WITHIN INTERVAL '5' MINUTES
WHERE t.amount > 100
STRATEGY CANARY(10);
```

#### 8. Supply Chain Monitoring with JOINs

```sql
-- Monitor supply chain events with temporal correlation
CREATE STREAM supply_chain_alerts AS
SELECT 
    ship.shipment_id,
    ship.origin,
    ship.destination,
    ship.departure_time,
    delivery.delivery_time,
    delivery.status,
    inventory.current_stock,
    inventory.reorder_level,
    CASE 
        WHEN delivery.delivery_time - ship.departure_time > 86400000 * 3 THEN 'DELAYED'
        WHEN inventory.current_stock < inventory.reorder_level THEN 'LOW_STOCK'
        WHEN delivery.status = 'damaged' THEN 'QUALITY_ISSUE'
        ELSE 'NORMAL'
    END as alert_type
FROM shipments ship
LEFT JOIN deliveries delivery ON ship.shipment_id = delivery.shipment_id
WITHIN INTERVAL '7' DAYS
INNER JOIN inventory_levels inventory ON ship.product_id = inventory.product_id
WHERE delivery.status IS NOT NULL;
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

### Window Functions (11 functions)
- `ROW_NUMBER()` - Assigns unique sequential integers to rows within each partition
- `RANK()` - Assigns ranks with gaps for tied values
- `DENSE_RANK()` - Assigns ranks without gaps for tied values
- `PERCENT_RANK()` - Calculates the percentile rank of a row within the partition
- `LAG(expr [, offset [, default]])` - Accesses previous row values
- `LEAD(expr [, offset [, default]])` - Accesses following row values
- `FIRST_VALUE(expr)` - Returns the first value in the partition
- `LAST_VALUE(expr)` - Returns the last value in the current partition frame
- `NTH_VALUE(expr, n)` - Returns the nth value in the partition (1-indexed)
- `CUME_DIST()` - Calculates the cumulative distribution of a row
- `NTILE(n)` - Divides the partition into n buckets and assigns bucket numbers

### Statistical Functions (7 functions)
- `STDDEV(expr)` - Standard deviation (sample)
- `STDDEV_SAMP(expr)` - Sample standard deviation (same as STDDEV)
- `STDDEV_POP(expr)` - Population standard deviation
- `VARIANCE(expr)` - Variance (sample)
- `VAR_SAMP(expr)` - Sample variance (same as VARIANCE)
- `VAR_POP(expr)` - Population variance
- `MEDIAN(expr)` - Median value (middle value or average of two middle values)

### Math Functions (7 functions)
- `ABS(number)` - Absolute value
- `ROUND(number[, precision])` - Round to specified decimal places
- `CEIL(number)`, `CEILING(number)` - Round up to nearest integer
- `FLOOR(number)` - Round down to nearest integer
- `MOD(a, b)` - Modulo operation (remainder)
- `POWER(base, exponent)`, `POW(base, exponent)` - Exponentiation
- `SQRT(number)` - Square root

### String Functions (12 functions)
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
- `POSITION(substring, string[, start_position])` - Find substring position

### Date/Time Functions (5 functions)
- `NOW()` - Current timestamp in milliseconds
- `CURRENT_TIMESTAMP` - Current timestamp in milliseconds
- `DATE_FORMAT(timestamp, format)` - Format timestamp as string
- `EXTRACT(part, timestamp)` - Extract date/time component
- `DATEDIFF(unit, start_date, end_date)` - Calculate time difference between dates

### Utility Functions (6 functions)
- `COALESCE(value1, value2, ...)` - Return first non-null value
- `NULLIF(value1, value2)` - Return null if values are equal
- `CAST(value, type)` - Type conversion
- `TIMESTAMP()` - Current record processing timestamp
- `SPLIT(string, delimiter)` - Split string (returns first part)
- `JOIN(delimiter, str1, str2, ...)` - Join strings with delimiter

### Aggregate Functions (7 functions)
- `COUNT(*)` - Count records
- `SUM(column)` - Sum numeric values
- `AVG(column)` - Average of numeric values
- `MIN(column)` - Minimum value
- `MAX(column)` - Maximum value
- `APPROX_COUNT_DISTINCT(column)` - Approximate distinct count
- `LISTAGG(expression, delimiter)` - Concatenate values with delimiter

### JSON Functions (2 functions)
- `JSON_VALUE(json_string, path)` - Extract scalar value from JSON
- `JSON_EXTRACT(json_string, path)` - Extract value/object from JSON

### Header Functions (5 functions)
- `HEADER(key)` - Get Kafka message header value
- `HAS_HEADER(key)` - Check if header exists
- `HEADER_KEYS()` - Get comma-separated list of header keys
- `SET_HEADER(key, value)` - Set Kafka message header value
- `REMOVE_HEADER(key)` - Remove Kafka message header

### System Columns (3 columns)
- `_timestamp` - Kafka message timestamp
- `_offset` - Kafka message offset
- `_partition` - Kafka partition number

**Total: 65 functions + 3 system columns**

### Function Categories Summary
- **Window Functions:** 11 functions for row-by-row analysis
- **Statistical Functions:** 7 functions for advanced analytics  
- **Math Functions:** 7 functions for numeric operations
- **String Functions:** 12 functions for text processing
- **Date/Time Functions:** 5 functions for temporal operations
- **Utility Functions:** 6 functions for data manipulation
- **Aggregate Functions:** 7 functions for group operations
- **JSON Functions:** 2 functions for JSON processing
- **Header Functions:** 5 functions for message metadata
- **System Columns:** 3 columns for Kafka metadata

This reference guide covers all currently implemented SQL features in FerrisStreams. For the latest updates and additional examples, refer to the test suite and feature documentation.