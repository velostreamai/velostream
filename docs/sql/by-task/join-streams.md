# Join Multiple Data Streams

Learn how to combine data from multiple streams and tables using JOIN operations, including windowed JOINs for real-time stream correlation.

## Basic JOIN Types

### INNER JOIN - Only Matching Records
Returns records where the join condition is met in both streams.

```sql
-- Basic customer-order join
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    c.customer_name,
    c.email
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;
```

### LEFT JOIN - All Left Records + Matching Right
Gets all records from left stream, with NULLs where right stream doesn't match.

```sql
-- All orders with optional customer details
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    c.customer_name  -- NULL if customer not found
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;
```

### RIGHT JOIN - All Right Records + Matching Left
Gets all records from right stream, with NULLs where left stream doesn't match.

```sql
-- All customers with optional order details
SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,      -- NULL if no orders
    o.amount         -- NULL if no orders
FROM orders o
RIGHT JOIN customers c ON o.customer_id = c.customer_id;
```

### FULL OUTER JOIN - All Records from Both Streams
Returns all records from both streams, with NULLs where no match exists.

```sql
-- Complete view of customers and orders
SELECT
    COALESCE(o.customer_id, c.customer_id) as customer_id,
    o.order_id,
    o.amount,
    c.customer_name
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.customer_id;
```

## Windowed JOINs for Streaming Data

**Critical for real-time processing** - correlates events that happen within a time window.

### Time-Based Windows

```sql
-- Join within 5 minutes - payment correlation
SELECT
    o.order_id,
    p.payment_id,
    o.amount,
    p.payment_method
FROM orders o
INNER JOIN payments p ON o.order_id = p.order_id
WITHIN INTERVAL '5' MINUTES;
```

```sql
-- Join within 30 seconds - click-to-purchase tracking
SELECT
    click.user_id,
    click.page_url,
    purchase.order_id,
    purchase.amount
FROM user_clicks click
INNER JOIN user_purchases purchase ON click.user_id = purchase.user_id
WITHIN INTERVAL '30' SECONDS;
```

```sql
-- Join within 2 hours - session correlation
SELECT
    session.session_id,
    events.event_type,
    events.timestamp
FROM user_sessions session
LEFT JOIN user_events events ON session.user_id = events.user_id
WITHIN INTERVAL '2' HOURS;
```

### Supported Time Units
- **SECONDS** - Real-time fraud detection, click tracking
- **MINUTES** - Payment processing, order fulfillment
- **HOURS** - Session analysis, user behavior tracking

## Interval Stream-Stream JOINs (FR-085)

**For unbounded streams:** Join two continuous streams with time-bounded conditions. Uses the JoinCoordinator with watermark-driven state management.

### Basic Interval Join
```sql
-- Orders joined with shipments within 24 hours
SELECT
    o.order_id,
    o.customer_id,
    o.total_amount,
    s.shipment_id,
    s.carrier,
    s.tracking_number
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
  AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '24' HOUR
EMIT CHANGES;
```

### Click Attribution
```sql
-- Attribute purchases to ad clicks within 30 minutes
SELECT
    c.user_id,
    c.ad_id,
    c.click_time,
    p.purchase_id,
    p.amount
FROM ad_clicks c
JOIN purchases p ON c.user_id = p.user_id
  AND p.event_time BETWEEN c.event_time AND c.event_time + INTERVAL '30' MINUTE
EMIT CHANGES;
```

### With Explicit Configuration
```sql
-- Stream-stream join with retention config
SELECT o.*, s.*
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
  AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '24' HOUR
EMIT CHANGES
WITH (
    'join.type' = 'interval',
    'join.lower_bound' = '0s',
    'join.upper_bound' = '24h',
    'join.retention' = '48h'
);
```

### Performance Characteristics
| Component | Throughput |
|-----------|------------|
| JoinStateStore | 5.3M rec/sec |
| JoinCoordinator | 607K rec/sec |
| SQL Engine (sync) | 364K rec/sec |
| High Cardinality | 1.7M rec/sec |

**See also:** [Stream-Stream Joins Design](../../design/stream-stream-joins.md)

---

## Stream-Table JOINs

**Optimized pattern:** Join streaming data with reference tables for data enrichment.

```sql
-- Enrich streaming events with user data
SELECT
    events.event_id,
    events.user_id,
    events.event_type,
    users.user_name,
    users.user_tier,
    users.signup_date
FROM streaming_events events
INNER JOIN user_reference_table users ON events.user_id = users.user_id;
```

```sql
-- Product catalog lookup for real-time sales
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

### Multiple Join Fields
```sql
-- Join on multiple conditions
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

## Multiple Table JOINs

Use table aliases for readability:

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

## Real-World Examples

### 1. E-commerce Order Processing
```sql
-- Complete order enrichment
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
-- Real-time transaction risk assessment
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
-- Correlate environmental sensors
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
-- Track click-to-purchase conversion
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

## Performance Tips

### 1. Window Size Selection
- **Small windows (seconds)**: Minimal memory, real-time correlation
- **Medium windows (minutes)**: Business process correlation
- **Large windows (hours)**: Session analysis, higher memory usage

### 2. Join Key Optimization
- Use selective join keys to reduce processing
- Prefer integer keys over string keys
- Ensure reference table keys are indexed

### 3. Stream-Table vs Stream-Stream JOINs
```sql
-- ✅ Preferred: Stream-Table JOIN (optimized)
SELECT s.event_id, t.reference_data
FROM streaming_events s
INNER JOIN reference_table t ON s.key = t.key;

-- ⚠️ Use carefully: Stream-Stream JOIN (requires buffering)
SELECT s1.event_id, s2.correlated_data
FROM stream1 s1
INNER JOIN stream2 s2 ON s1.correlation_id = s2.correlation_id
WITHIN INTERVAL '5' MINUTES;
```

### 4. Filter Early
```sql
-- ✅ Efficient: Filter before JOIN
SELECT o.order_id, c.customer_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 1000;  -- Apply filter efficiently
```

## Handle Missing Data

```sql
-- Use COALESCE for missing join data
SELECT
    o.order_id,
    o.customer_id,
    COALESCE(c.customer_name, 'Unknown Customer') as customer_name,
    COALESCE(c.customer_tier, 'standard') as tier
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;
```

## JOIN Types Quick Reference

| JOIN Type | Returns | Use Case |
|-----------|---------|----------|
| `INNER JOIN` | Only matching records | Core data relationships |
| `LEFT JOIN` | All left + matching right | Optional enrichment |
| `RIGHT JOIN` | All right + matching left | Comprehensive views |
| `FULL OUTER JOIN` | All records from both | Complete data analysis |

## Common Patterns

### Lookup Pattern
```sql
-- Enrich with reference data
FROM streaming_data s
INNER JOIN reference_table r ON s.key = r.key
```

### Optional Enrichment
```sql
-- Add optional details
FROM core_stream c
LEFT JOIN optional_data o ON c.id = o.id
```

### Event Correlation
```sql
-- Correlate related events
FROM event_stream_1 e1
INNER JOIN event_stream_2 e2 ON e1.correlation_id = e2.correlation_id
WITHIN INTERVAL 'X' MINUTES
```

## Next Steps

- [Aggregate joined data](aggregate-data.md) - GROUP BY with multiple tables
- [Filter joined results](filter-data.md) - WHERE conditions with JOINs
- [Window analysis](window-analysis.md) - Time-based windowing