# VeloStream JOIN Operations Guide

## Overview

VeloStream provides comprehensive JOIN support for combining data from multiple streams and tables in real-time. This guide covers all JOIN types, windowed JOINs for temporal correlation, and performance best practices.

## JOIN Types Supported

### INNER JOIN
Returns only records where the join condition is met in both streams/tables.

```sql
-- Basic INNER JOIN
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.email
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;
```

### LEFT JOIN (LEFT OUTER JOIN)
Returns all records from the left stream and matching records from the right stream. Records without matches have NULL values for right stream fields.

```sql
-- LEFT JOIN - all orders with optional customer details
SELECT 
    o.order_id,
    o.customer_id,
    o.amount,
    c.customer_name  -- NULL if customer not found
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;
```

### RIGHT JOIN (RIGHT OUTER JOIN)
Returns all records from the right stream and matching records from the left stream.

```sql
-- RIGHT JOIN - all customers with optional order details
SELECT 
    c.customer_id,
    c.customer_name,
    o.order_id,      -- NULL if no orders
    o.amount         -- NULL if no orders
FROM orders o
RIGHT JOIN customers c ON o.customer_id = c.customer_id;
```

### FULL OUTER JOIN
Returns all records from both streams, with NULLs where no match exists.

```sql
-- FULL OUTER JOIN - complete view
SELECT 
    COALESCE(o.customer_id, c.customer_id) as customer_id,
    o.order_id,
    o.amount,
    c.customer_name
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.customer_id;
```

## Windowed JOINs for Streaming

Windowed JOINs enable temporal correlation between streams, essential for real-time processing.

### Time-Based Windows

```sql
-- JOIN within 5 minutes - payment correlation
SELECT 
    o.order_id,
    p.payment_id,
    o.amount,
    p.payment_method
FROM orders o
INNER JOIN payments p ON o.order_id = p.order_id
WITHIN INTERVAL '5' MINUTES;

-- JOIN within 30 seconds - click-to-purchase tracking
SELECT 
    click.user_id,
    click.page_url,
    purchase.order_id,
    purchase.amount
FROM user_clicks click
INNER JOIN user_purchases purchase ON click.user_id = purchase.user_id
WITHIN INTERVAL '30' SECONDS;

-- JOIN within 2 hours - session correlation
SELECT 
    session.session_id,
    events.event_type,
    events.timestamp
FROM user_sessions session
LEFT JOIN user_events events ON session.user_id = events.user_id
WITHIN INTERVAL '2' HOURS;
```

### Supported Time Units

- **SECONDS** - For fast correlation (real-time fraud detection, click tracking)
- **MINUTES** - For payment processing, order fulfillment
- **HOURS** - For session analysis, user behavior tracking

## Stream-Table JOINs

Optimized JOINs between streaming data and materialized reference tables.

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
    sales.sale_id,
    sales.product_id,
    sales.quantity,
    products.product_name,
    products.category,
    products.unit_price,
    sales.quantity * products.unit_price as total_value
FROM real_time_sales sales
INNER JOIN product_catalog products ON sales.product_id = products.product_id;
```

## Complex JOIN Conditions

### Multiple Join Conditions

```sql
-- JOIN on multiple fields
SELECT 
    o.order_id,
    i.item_name,
    o.quantity * i.unit_price as line_total
FROM order_items o
INNER JOIN inventory i ON o.product_id = i.product_id 
                       AND o.warehouse_id = i.warehouse_id;
```

### JOINs with Additional Filters

```sql
-- JOIN with WHERE conditions
SELECT 
    u.user_id,
    u.username,
    a.action_type,
    a.timestamp
FROM users u
LEFT JOIN user_actions a ON u.user_id = a.user_id 
                          AND a.action_type = 'purchase'
WHERE u.status = 'active'
  AND u.signup_date > '2024-01-01';
```

## Table Aliases

Use aliases to improve readability and handle complex multi-table JOINs.

```sql
-- Multiple JOINs with aliases
SELECT 
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name,
    oi.quantity,
    oi.unit_price
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id
WHERE o.order_date >= '2024-01-01';
```

## JOIN with JSON Processing

Combine JOINs with JSON extraction for complex event processing.

```sql
-- JOIN with JSON field extraction
SELECT 
    events.event_id,
    JSON_VALUE(events.payload, '$.user_id') as user_id,
    users.user_name,
    JSON_VALUE(events.payload, '$.action') as action_performed,
    JSON_VALUE(events.payload, '$.metadata.campaign') as campaign
FROM kafka_events events
INNER JOIN user_table users 
    ON JSON_VALUE(events.payload, '$.user_id') = users.user_id
WHERE JSON_VALUE(events.payload, '$.event_type') = 'user_action';
```

## Real-World Use Cases

### 1. E-commerce Order Processing

```sql
-- Enrich orders with customer and product data
SELECT 
    o.order_id,
    o.order_date,
    c.customer_name,
    c.customer_tier,
    p.product_name,
    p.category,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price as line_total
FROM streaming_orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id
WHERE c.customer_tier IN ('gold', 'platinum');
```

### 2. Fraud Detection

```sql
-- Correlate transactions with user behavior
SELECT 
    t.transaction_id,
    t.amount,
    u.home_country,
    l.current_country,
    t.timestamp as transaction_time,
    l.timestamp as location_time,
    CASE 
        WHEN l.current_country != u.home_country THEN 'LOCATION_RISK'
        WHEN t.amount > 5000 THEN 'AMOUNT_RISK'
        ELSE 'LOW_RISK'
    END as risk_level
FROM transactions t
INNER JOIN user_profiles u ON t.user_id = u.user_id
LEFT JOIN user_locations l ON t.user_id = l.user_id
WITHIN INTERVAL '5' MINUTES;
```

### 3. IoT Sensor Correlation

```sql
-- Correlate temperature and humidity sensors
SELECT 
    temp.device_id,
    temp.location,
    temp.temperature_reading,
    humid.humidity_reading,
    temp.timestamp,
    CASE 
        WHEN temp.temperature_reading > 30 AND humid.humidity_reading > 80 
        THEN 'HIGH_HEAT_HUMIDITY'
        ELSE 'NORMAL'
    END as environmental_status
FROM temperature_sensors temp
INNER JOIN humidity_sensors humid 
    ON temp.device_id = humid.device_id
WITHIN INTERVAL '1' MINUTE;
```

### 4. User Journey Analysis

```sql
-- Track user journey from click to purchase
SELECT 
    click.user_id,
    click.page_url,
    click.utm_campaign,
    purchase.order_id,
    purchase.amount,
    purchase.timestamp - click.timestamp as time_to_purchase_ms
FROM user_clicks click
INNER JOIN user_purchases purchase ON click.user_id = purchase.user_id
WITHIN INTERVAL '30' MINUTES
WHERE click.page_url LIKE '%product%';
```

## Performance Optimization

### 1. Window Size Selection

Choose appropriate window sizes based on your use case:

- **Small windows (seconds)**: For real-time correlation, minimal memory usage
- **Medium windows (minutes)**: For business process correlation
- **Large windows (hours)**: For session analysis, higher memory usage

### 2. Join Key Selection

- Use selective join keys to minimize processing overhead
- Prefer integer keys over string keys when possible
- Ensure join keys are indexed in reference tables

### 3. Stream-Table vs Stream-Stream JOINs

```sql
-- Preferred: Stream-Table JOIN (optimized)
SELECT s.event_id, t.reference_data
FROM streaming_events s
INNER JOIN reference_table t ON s.key = t.key;

-- Use carefully: Stream-Stream JOIN (requires buffering)
SELECT s1.event_id, s2.correlated_data
FROM stream1 s1
INNER JOIN stream2 s2 ON s1.correlation_id = s2.correlation_id
WITHIN INTERVAL '5' MINUTES;
```

### 4. Filter Early

Apply WHERE conditions before JOINs when possible:

```sql
-- Efficient: Filter before JOIN
SELECT o.order_id, c.customer_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 1000;  -- Filter applied efficiently
```

## Error Handling

### Common Scenarios

```sql
-- Handle missing join data gracefully
SELECT 
    o.order_id,
    o.customer_id,
    COALESCE(c.customer_name, 'Unknown Customer') as customer_name,
    COALESCE(c.customer_tier, 'standard') as tier
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;

-- Validate join conditions
SELECT 
    events.event_id,
    CASE 
        WHEN users.user_id IS NOT NULL THEN users.user_name
        ELSE 'User Not Found'
    END as user_name
FROM events
LEFT JOIN users ON events.user_id = users.user_id;
```

## Best Practices

### 1. Design Guidelines

1. **Use meaningful aliases** for complex multi-table JOINs
2. **Apply filters early** to reduce processing overhead
3. **Choose appropriate window sizes** for temporal JOINs
4. **Monitor memory usage** for large window JOINs
5. **Test with realistic data volumes** before production deployment

### 2. Monitoring and Debugging

```sql
-- Add debugging fields to monitor JOIN performance
SELECT 
    o.order_id,
    c.customer_name,
    _timestamp as processing_time,
    _partition as partition_id
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;
```

### 3. Deployment Strategies

```sql
-- Use versioned deployments for JOIN operations
DEPLOY JOB order_enrichment VERSION '1.2.0' AS
SELECT 
    o.order_id,
    c.customer_name,
    p.product_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN products p ON o.product_id = p.product_id
STRATEGY CANARY(10);
```

## Technical Implementation

### Architecture

VeloStream JOIN implementation includes:

1. **Parser Support**: Full SQL JOIN syntax parsing
2. **Execution Engine**: Optimized JOIN processing with memory management
3. **Window Management**: Temporal window handling with configurable grace periods
4. **Stream-Table Optimization**: Key-based lookups for reference data
5. **Null Handling**: Proper outer JOIN semantics with NULL value handling

### Memory Management

- **Sliding Windows**: Automatic cleanup of expired records
- **Grace Periods**: Configurable handling of late-arriving data
- **Memory Limits**: Built-in protection against memory exhaustion

### Supported Patterns

- **One-to-One**: User profile enrichment
- **One-to-Many**: Order to order items relationship
- **Many-to-One**: Multiple events to single user
- **Many-to-Many**: Complex entity relationships

This comprehensive JOIN support makes VeloStream competitive with enterprise stream processing platforms like Confluent ksqlDB and Apache Flink SQL, while maintaining Rust's performance and safety guarantees.