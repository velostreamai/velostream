# FerrisStreams SQL Reference Guide

## Overview

FerrisStreams provides a comprehensive SQL interface for processing Kafka streams with native support for real-time analytics, complete data lifecycle management (INSERT, UPDATE, DELETE), comprehensive schema introspection, logical operators (AND/OR) for complex conditional expressions, JSON processing, and enterprise job management. This guide covers all available SQL features, functions, and commands.

## Table of Contents

1. [Basic Query Syntax](#basic-query-syntax)
2. [Logical Operators and Compound Conditions](#logical-operators-and-compound-conditions)
3. [Data Manipulation Language (DML)](#data-manipulation-language-dml)
4. [GROUP BY Operations](#group-by-operations)
5. [JOIN Operations](#join-operations)
6. [Schema Management and Introspection](#schema-management-and-introspection)
7. [Job Lifecycle Management](#job-lifecycle-management)
8. [Built-in Functions](#built-in-functions)
   - [Window Functions](#window-functions)
   - [Statistical Functions](#statistical-functions)
   - [Aggregate Functions](#aggregate-functions)
   - [Math Functions](#math-functions)
   - [String Functions](#string-functions)
   - [Date/Time Functions](#datetime-functions)
   - [Utility Functions](#utility-functions)
   - [CASE WHEN Expressions](#case-when-expressions)
   - [INTERVAL Arithmetic](#interval-arithmetic)
   - [Set Operations (IN/NOT IN)](#set-operations-in-not-in)
9. [JSON Processing](#json-processing)
10. [System Columns](#system-columns)
11. [Window Operations](#window-operations)
12. [Type Conversion](#type-conversion)
13. [Examples](#examples)

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

## Logical Operators and Compound Conditions

FerrisStreams supports logical operators (AND, OR) for building complex conditional expressions in WHERE clauses, JOIN conditions, HAVING clauses, and any expression context.

### Logical Operators

#### AND Operator
Combines conditions where **both** must be true:

```sql
-- Basic AND usage
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 100.0 AND status = 'completed';

-- Multiple AND conditions
SELECT product_id, name, price, category
FROM products
WHERE price > 50.0 
  AND category = 'electronics' 
  AND in_stock = true;

-- AND with different data types
SELECT user_id, login_time, session_duration
FROM user_sessions
WHERE session_duration > 300 
  AND login_time > '2024-01-01' 
  AND user_type = 'premium';
```

#### OR Operator
Combines conditions where **at least one** must be true:

```sql
-- Basic OR usage
SELECT customer_id, status, priority
FROM orders
WHERE status = 'urgent' OR priority = 'high';

-- Multiple OR conditions
SELECT user_id, account_type, subscription_status
FROM users
WHERE account_type = 'premium' 
   OR account_type = 'enterprise' 
   OR subscription_status = 'trial';

-- OR with mixed conditions
SELECT order_id, amount, discount_code
FROM orders
WHERE amount > 1000.0 
   OR discount_code IS NOT NULL 
   OR customer_tier = 'VIP';
```

### Compound Conditions with AND/OR

#### Combining AND and OR
Operator precedence: AND has higher precedence than OR (use parentheses for clarity):

```sql
-- Without parentheses (AND evaluated first)
SELECT customer_id, amount, status, priority
FROM orders
WHERE status = 'pending' AND amount > 100.0 OR priority = 'urgent';
-- Equivalent to: (status = 'pending' AND amount > 100.0) OR priority = 'urgent'

-- With explicit parentheses for clarity
SELECT customer_id, amount, status, priority
FROM orders
WHERE (status = 'pending' AND amount > 100.0) OR priority = 'urgent';

-- Different grouping with parentheses
SELECT customer_id, amount, status, priority
FROM orders
WHERE status = 'pending' AND (amount > 100.0 OR priority = 'urgent');
```

#### Complex Nested Conditions

```sql
-- E-commerce order filtering
SELECT order_id, customer_id, amount, status, payment_method
FROM orders
WHERE (status = 'confirmed' OR status = 'shipped')
  AND (payment_method = 'credit_card' OR payment_method = 'paypal')
  AND amount > 50.0
  AND customer_id NOT IN (999, 1000);  -- Exclude test customers

-- User activity analysis
SELECT user_id, action_type, page_url, session_id
FROM user_actions
WHERE (action_type = 'click' OR action_type = 'view' OR action_type = 'purchase')
  AND (page_url LIKE '%product%' OR page_url LIKE '%category%')
  AND session_duration > 60
  AND NOT (user_agent LIKE '%bot%' OR user_agent LIKE '%crawler%');

-- IoT sensor data filtering
SELECT sensor_id, reading_value, reading_type, alert_level
FROM sensor_readings
WHERE (reading_type = 'temperature' AND reading_value > 80.0)
   OR (reading_type = 'humidity' AND reading_value > 90.0)
   OR (reading_type = 'pressure' AND (reading_value < 980.0 OR reading_value > 1050.0))
   AND sensor_status = 'active'
   AND reading_timestamp > NOW() - INTERVAL '1' HOUR;
```

### Logical Operators in JOIN Conditions

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

-- Complex JOIN with subqueries and logical operators
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

### Logical Operators in GROUP BY and HAVING

```sql
-- HAVING with compound conditions
SELECT customer_id, 
       COUNT(*) as order_count,
       SUM(amount) as total_spent,
       AVG(amount) as avg_order_value
FROM orders
WHERE status = 'completed'
GROUP BY customer_id
HAVING (COUNT(*) > 5 AND SUM(amount) > 1000.0)
    OR (COUNT(*) > 10 AND AVG(amount) > 100.0)
    OR SUM(amount) > 5000.0;

-- Complex aggregation with logical filtering
SELECT product_category,
       brand,
       COUNT(*) as sales_count,
       SUM(amount) as total_revenue,
       MAX(amount) as highest_sale
FROM sales_transactions
WHERE (transaction_date >= '2024-01-01' AND transaction_date < '2024-04-01')
  AND (payment_status = 'completed' OR payment_status = 'settled')
GROUP BY product_category, brand
HAVING (COUNT(*) >= 50 AND SUM(amount) >= 10000.0)
    OR (brand = 'Premium' AND COUNT(*) >= 20)
ORDER BY total_revenue DESC;
```

### Advanced Patterns with Logical Operators

#### Exclusion Patterns
```sql
-- Exclude multiple problematic conditions
SELECT order_id, customer_id, amount, status
FROM orders
WHERE NOT (
    (status = 'cancelled' OR status = 'refunded')
    AND (amount < 10.0 OR customer_id IN (999, 1000))
)
AND order_date > '2024-01-01';
```

#### Range Simulation with OR
```sql
-- Simulate BETWEEN using OR (when BETWEEN is not available)
SELECT user_id, age, registration_date
FROM users
WHERE (age >= 18 AND age <= 25)
   OR (age >= 35 AND age <= 45)
   OR (age >= 55 AND age <= 65);

-- Business hours simulation
SELECT transaction_id, amount, transaction_time
FROM transactions
WHERE EXTRACT('HOUR', transaction_time) >= 9
  AND EXTRACT('HOUR', transaction_time) <= 17
  AND (EXTRACT('DOW', transaction_time) >= 1 AND EXTRACT('DOW', transaction_time) <= 5);
```

#### Dynamic Filtering Patterns
```sql
-- Conditional filtering based on data characteristics
SELECT event_id, user_id, event_type, event_data
FROM user_events
WHERE (event_type = 'purchase' AND CAST(JSON_VALUE(event_data, '$.amount'), 'FLOAT') > 100.0)
   OR (event_type = 'signup' AND JSON_VALUE(event_data, '$.source') = 'organic')
   OR (event_type = 'login' AND JSON_VALUE(event_data, '$.device_type') = 'mobile')
   OR (event_type = 'view' AND JSON_VALUE(event_data, '$.page_category') IN ('premium', 'featured'));
```

### Performance Considerations

#### Optimization Tips
```sql
-- Good: Most selective conditions first
SELECT * FROM large_table
WHERE rare_status = 'special'        -- Most selective first
  AND (common_field > 100 OR common_field < 10)
  AND date_field > '2024-01-01';

-- Good: Use indexes effectively with AND
SELECT * FROM orders
WHERE customer_id = 12345            -- Indexed field
  AND (status = 'pending' OR status = 'processing')
  AND amount > 50.0;

-- Consider: Complex OR conditions may benefit from UNION
-- Instead of complex OR:
SELECT * FROM events WHERE (type = 'A' AND value > 100) OR (type = 'B' AND value > 200);

-- Consider using UNION for better performance:
SELECT * FROM events WHERE type = 'A' AND value > 100
UNION ALL
SELECT * FROM events WHERE type = 'B' AND value > 200;
```

#### Short-Circuit Evaluation
```sql
-- Take advantage of short-circuit evaluation in AND
SELECT * FROM orders
WHERE customer_tier = 'premium'      -- Check this first (faster)
  AND expensive_function(order_data) = 'valid';  -- Only if first condition is true

-- In OR, put most likely conditions first
SELECT * FROM user_sessions
WHERE session_active = true         -- Most common case first
   OR last_activity > NOW() - INTERVAL '5' MINUTES
   OR special_override_flag = true;
```

### Common Patterns and Use Cases

#### 1. Multi-Status Filtering
```sql
SELECT * FROM orders 
WHERE status IN ('pending', 'confirmed', 'processing')
-- Equivalent to:
-- WHERE status = 'pending' OR status = 'confirmed' OR status = 'processing'
```

#### 2. Date Range with Exclusions
```sql
SELECT * FROM transactions
WHERE transaction_date >= '2024-01-01'
  AND transaction_date < '2024-04-01'
  AND NOT (amount = 0.0 OR transaction_type = 'test');
```

#### 3. User Segmentation
```sql
SELECT user_id, segment FROM users
WHERE (age >= 18 AND age <= 34 AND income_bracket = 'middle')  -- Young professional
   OR (age >= 35 AND age <= 54 AND has_children = true)        -- Family segment  
   OR (age >= 55 AND retirement_planning = true);              -- Pre-retirement
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

### CREATE STREAM INTO (✅ Core Architecture Implemented)

Create a streaming job that reads from a source and writes to a sink with multi-config file support and environment variable resolution.

**✅ Currently Working:**
```sql
-- Basic CREATE STREAM INTO syntax (implemented)
CREATE STREAM orders_processed AS 
SELECT id, customer_id, amount, status 
FROM kafka_source 
INTO kafka_sink
WITH (
    "source_config" = "configs/kafka_source.yaml",
    "sink_config" = "configs/kafka_sink.yaml"
);

-- Alternative: Inline configuration with source./sink. prefixes
CREATE STREAM orders_inline AS 
SELECT id, customer_id, amount, status 
FROM 'kafka://localhost:9092/orders'
WITH (
    "source.group_id" = "orders_processor",
    "source.value.format" = "json"
);

-- Multi-config file support with environment variables (implemented)
-- Configuration files can use 'extends:' for inheritance
CREATE STREAM kafka_replication AS 
SELECT * FROM kafka_source 
INTO kafka_sink
WITH (
    "source_config" = "configs/kafka_${ENVIRONMENT}.yaml",
    "sink_config" = "configs/kafka_sink_${ENVIRONMENT}.yaml",
    "monitoring_config" = "configs/monitoring_${ENVIRONMENT}.yaml",
    "security_config" = "configs/security.yaml",
    "batch_size" = "1000"
);

-- Environment variable patterns supported:
-- ${VAR} - Simple substitution  
-- ${VAR:-default} - Use default value if VAR is not set
-- ${VAR:?error} - Error if VAR is not set
CREATE STREAM env_example AS 
SELECT * FROM kafka_source 
INTO kafka_sink
WITH (
    "source_config" = "${CONFIG_PATH}/kafka_${ENVIRONMENT:-dev}.yaml",
    "sink_config" = "${CONFIG_PATH}/kafka_sink_${ENVIRONMENT:?ENVIRONMENT must be set}.yaml"
);
```


```sql 
CREATE STREAM csv_to_kafka AS 
SELECT id, customer_id, amount, status 
FROM csv_source   -- TODO: Implement CSV DataSource
INTO kafka_sink
WITH (
    "source_config" = "configs/csv_orders.yaml",
    "sink_config" = "configs/kafka_sink.yaml" 
);

**🚧 TODO - Planned Multi-Source Support:**
-- TODO: PostgreSQL to S3 (architecture ready, implementations not done)
-- Configuration files use 'extends:' for inheritance instead of base_* configs
CREATE STREAM db_replication AS 
SELECT * FROM postgres_source   -- TODO: Implement PostgreSQL DataSource 
INTO s3_sink                    -- TODO: Implement S3 DataSink
WITH (
    "source_config" = "configs/postgres_${ENVIRONMENT}.yaml",
    "sink_config" = "configs/s3_${ENVIRONMENT}.yaml"
);
```

### CREATE TABLE INTO (✅ Core Architecture Implemented)

Create a materialized table job that processes aggregated data and outputs to a sink.

**✅ Currently Working:**
```sql
-- CREATE TABLE INTO with aggregation (Kafka to Kafka working)
-- Configuration files use 'extends:' for inheritance
CREATE TABLE user_analytics AS 
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value
FROM kafka_orders_stream 
GROUP BY customer_id
INTO kafka_analytics_sink
WITH (
    "source_config" = "configs/kafka_orders_${ENVIRONMENT}.yaml",
    "sink_config" = "configs/kafka_analytics_${ENVIRONMENT}.yaml",
    "batch_size" = "500"
);

-- Real-time dashboard updates
CREATE TABLE dashboard_metrics AS
SELECT 
    DATE_TRUNC('hour', order_timestamp) as hour,
    COUNT(*) as hourly_orders,
    SUM(amount) as hourly_revenue,
    COUNT(DISTINCT customer_id) as unique_customers
FROM orders_stream
GROUP BY DATE_TRUNC('hour', order_timestamp)
INTO dashboard_sink
WITH (
    "source_config" = "configs/kafka_orders.yaml",
    "sink_config" = "configs/dashboard_api.yaml",
    "update_frequency" = "1m"
);
```

#### Configuration File Support

The CREATE STREAM/TABLE INTO syntax supports multiple configuration approaches:

**Configuration Files:**
- **source_config**: Source configuration file (can inherit from base using `extends:`)
- **sink_config**: Sink configuration file (can inherit from base using `extends:`)
- **monitoring_config**: Monitoring and metrics configuration
- **security_config**: Security and authentication configuration

**Inline Configuration:**
- **source.*** properties**: Direct source configuration (e.g., `source.format`, `source.topic`)
- **sink.*** properties**: Direct sink configuration (e.g., `sink.path`, `sink.bootstrap.servers`)
- **Mixed approach**: Combine config files with inline overrides

#### YAML Configuration Inheritance

Configuration files support inheritance using the `extends:` keyword:

```yaml
# configs/kafka_prod.yaml
extends: configs/base_kafka.yaml
topic: "orders_production"
brokers: ["prod-kafka-1:9092", "prod-kafka-2:9092"]
```

This approach replaces the deprecated `base_source_config` and `base_sink_config` pattern.

#### Configuration Pattern Examples

**Pattern 1: Configuration Files Only**
```sql
CREATE STREAM processing AS 
SELECT * FROM kafka_source INTO file_sink
WITH (
    "source_config" = "configs/kafka_orders.yaml",
    "sink_config" = "configs/file_export.yaml"
);
```

**Pattern 2: Inline Configuration Only**  
```sql
CREATE STREAM file_processing AS
SELECT * FROM 'file://data/input.csv'
INTO file_sink
WITH (
    "source.format" = "csv",
    "source.has_headers" = "true",
    "source.watching" = "true",
    "sink.path" = "output/results.json",
    "sink.format" = "json",
    "sink.append" = "true"
);
```

**Pattern 3: Mixed Configuration (File + Inline Overrides)**
```sql
CREATE STREAM kafka_processing AS
SELECT * FROM kafka_source INTO kafka_sink  
WITH (
    "source_config" = "configs/kafka_base.yaml",
    "sink_config" = "configs/kafka_sink.yaml",
    -- Inline overrides for specific job requirements
    "source.group_id" = "special_processor_${ENVIRONMENT}",
    "sink.topic" = "processed_${JOB_TYPE}",
    "sink.failure_strategy" = "RetryWithBackoff"
);
```

**Pattern 4: URI with Inline Parameters**
```sql
CREATE STREAM orders AS
SELECT * FROM 'kafka://broker1:9092,broker2:9092/orders'
INTO 'file://output/processed_orders.json'
WITH (
    "source.group_id" = "orders_analytics",
    "source.value.format" = "avro",
    "sink.format" = "json",
    "sink.append" = "true"
);
```

#### Environment Variable Resolution

Environment variables are resolved at parse time using these patterns:

| Pattern | Description | Example |
|---------|-------------|---------|
| `${VAR}` | Simple substitution | `"${CONFIG_PATH}/app.yaml"` |
| `${VAR:-default}` | Use default if VAR unset | `"kafka_${ENV:-dev}.yaml"` |
| `${VAR:?message}` | Error if VAR unset | `"${REQUIRED_CONFIG:?Config required}"` |

#### Property Prefix Behavior

The `source.` and `sink.` prefixes provide powerful configuration isolation with intelligent fallback behavior:

**Prefix Priority Rules:**
1. **Prefixed properties take priority**: `source.brokers` overrides `brokers` for source configuration
2. **Fallback to unprefixed**: If `source.brokers` doesn't exist, `brokers` is used as fallback
3. **Property isolation**: Source config excludes `sink.` properties and vice versa
4. **Alias support**: Multiple property names can map to the same configuration (e.g., `bootstrap.servers` ↔ `brokers`)

**Example Priority Resolution:**
```sql
CREATE STREAM priority_example AS
SELECT * FROM kafka_source INTO kafka_sink
WITH (
    -- Source will use: source.brokers (priority)
    "brokers" = "fallback-broker:9092",
    "source.brokers" = "priority-broker:9092",
    
    -- Sink will use: brokers (fallback - no sink.brokers specified)
    -- "sink.brokers" would take priority if present
    
    -- Property isolation in action:
    "source.group_id" = "reader_group",    -- Only in source config
    "sink.topic" = "output_topic",         -- Only in sink config
    "failure_strategy" = "RetryWithBackoff" -- Shared property (both configs)
);
```

**Property Aliases:**
- **Kafka**: `brokers` ↔ `bootstrap.servers`
- **File**: `watching` ↔ `watch`, `has_headers` ↔ `header`

#### Backward Compatibility

The traditional CREATE STREAM and CREATE TABLE syntax continues to work but prints the output to STDOUT unless INTO is specified:

```sql
-- Legacy CREATE STREAM (still supported)
CREATE STREAM legacy_stream AS 
SELECT id, name FROM orders 
WITH (
    "topic" = "processed_orders",
    "replication_factor" = "3"
);

-- Legacy CREATE TABLE (still supported)
CREATE TABLE legacy_table AS 
SELECT customer_id, SUM(amount) as total 
FROM orders 
GROUP BY customer_id
WITH (
    "compaction" = "true",
    "retention_ms" = "86400000"
);
```

## Data Manipulation Language (DML)

FerrisStreams provides comprehensive DML (Data Manipulation Language) support for INSERT, UPDATE, and DELETE operations with streaming-first semantics. All DML operations maintain proper audit trails, generate tombstone records for deletions, and preserve data lineage.

### INSERT Operations

Insert new records into streams or tables with VALUES or SELECT sources.

#### INSERT with VALUES

```sql
-- Single row insert
INSERT INTO orders (order_id, customer_id, amount, status)
VALUES (1001, 100, 250.50, 'pending');

-- Multiple row insert
INSERT INTO orders (order_id, customer_id, amount, status)
VALUES 
    (1002, 101, 89.99, 'completed'),
    (1003, 102, 175.25, 'pending'),
    (1004, 100, 320.00, 'shipped');

-- Insert with expressions
INSERT INTO products (product_id, name, price, discounted_price)
VALUES (201, 'Widget Pro', 29.99, 29.99 * 0.9);
```

#### INSERT with SELECT

```sql
-- Insert from another stream/table
INSERT INTO high_value_orders
SELECT order_id, customer_id, amount, order_date
FROM orders
WHERE amount > 1000.0;

-- Insert with aggregation
INSERT INTO daily_summary
SELECT 
    DATE(order_date) as summary_date,
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value
FROM orders
GROUP BY DATE(order_date);

-- Insert with JOINs
INSERT INTO enriched_orders
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.amount,
    o.order_date
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
WHERE o.status = 'completed';
```

#### INSERT with Configuration (TODO - Coming Soon)

> **Note**: INSERT INTO syntax with configuration support is currently under development. The parser implementation is planned for a future release.

For streaming INSERT operations that require source and sink configuration, you can use the WITH clause to specify connection details:

```sql
-- INSERT with configuration files (PLANNED)
INSERT INTO high_value_orders
SELECT order_id, customer_id, amount, order_date
FROM orders
WHERE amount > 1000.0
WITH (
    "source_config" = "configs/orders_source.yaml",
    "sink_config" = "configs/high_value_sink.yaml"
);

-- INSERT with inline properties (PLANNED)  
INSERT INTO processed_transactions
SELECT 
    transaction_id,
    customer_id,
    amount,
    processed_at
FROM raw_transactions
WHERE status = 'validated'
WITH (
    "source.topic" = "raw_transactions",
    "source.group.id" = "insert-processor",
    "sink.topic" = "processed_transactions",
    "sink.type" = "kafka",
    "batch_size" = "1000"
);
```

**Key Differences from CREATE STREAM**:
- **INSERT INTO**: One-time data transfer/migration operation 
- **CREATE STREAM**: Continuous processing job that runs indefinitely

**Use Cases for INSERT INTO**:
- Data backfills and historical data migration
- One-time batch processing operations
- Manual data transfers between streams/tables
- Testing and data validation workflows

**Current Workaround**: Use CREATE STREAM AS SELECT for continuous processing needs until INSERT parsing is implemented.

### UPDATE Operations

Update existing records with conditional logic and expression-based assignments.

```sql
-- Simple update with WHERE clause
UPDATE orders 
SET status = 'shipped', shipped_date = NOW()
WHERE order_id = 1001;

-- Update with expressions
UPDATE products
SET price = price * 1.1,
    updated_at = NOW()
WHERE category = 'electronics';

-- Complex conditional update
UPDATE customers
SET 
    tier = CASE 
        WHEN total_spent > 10000 THEN 'platinum'
        WHEN total_spent > 5000 THEN 'gold'
        WHEN total_spent > 1000 THEN 'silver'
        ELSE 'bronze'
    END,
    last_updated = NOW()
WHERE active = true;

-- Update with subqueries
UPDATE orders
SET priority = 'high'
WHERE customer_id IN (
    SELECT customer_id 
    FROM customers 
    WHERE tier = 'platinum'
);
```

### DELETE Operations

Delete records with tombstone generation for streaming deletion semantics.

```sql
-- Simple delete with WHERE clause
DELETE FROM orders 
WHERE status = 'cancelled' AND order_date < '2024-01-01';

-- Delete with complex conditions
DELETE FROM products
WHERE discontinued = true 
  AND last_sold_date < NOW() - INTERVAL '1' YEAR;

-- Delete with subqueries
DELETE FROM user_sessions
WHERE user_id IN (
    SELECT user_id 
    FROM users 
    WHERE account_status = 'deleted'
);

-- Delete all records (use with caution)
DELETE FROM temp_data;
```

#### Streaming DELETE Semantics

FerrisStreams implements streaming-friendly DELETE operations:

- **Tombstone Records**: DELETE operations generate tombstone records with `__deleted = true`
- **Audit Trail**: Original timestamps and offsets are preserved in headers
- **Key Preservation**: Primary key fields are maintained for proper partitioning
- **Soft Delete Option**: Optional soft delete mode preserves original data

```sql
-- Tombstone record structure (generated automatically)
{
  "__deleted": true,
  "__deleted_at": "2024-01-15T10:30:00Z",
  "order_id": 1001,  -- Key fields preserved
  -- Original fields are removed in tombstone mode
}
```

### DML Best Practices

#### Performance Optimization
```sql
-- Use bulk INSERT for better performance
INSERT INTO orders (order_id, customer_id, amount)
VALUES 
    (1001, 100, 250.50),
    (1002, 101, 89.99),
    (1003, 102, 175.25);  -- Single operation vs multiple INSERTs

-- Use selective WHERE clauses
UPDATE orders SET status = 'processed' 
WHERE status = 'pending' AND created_date > '2024-01-01';  -- Selective condition
```

#### Data Integrity
```sql
-- Validate before UPDATE/DELETE
UPDATE orders 
SET amount = amount * 1.1
WHERE amount > 0 AND status != 'cancelled';  -- Prevent invalid updates

-- Use transactions for related operations
INSERT INTO order_items (order_id, product_id, quantity)
VALUES (1001, 201, 2);
UPDATE products SET inventory = inventory - 2 
WHERE product_id = 201;
```

## GROUP BY Operations

FerrisStreams provides comprehensive GROUP BY functionality for data aggregation with streaming semantics. All standard SQL aggregate functions are supported along with streaming-specific optimizations.

### Basic Grouping

```sql
-- Count orders per customer
SELECT customer_id, COUNT(*) as order_count
FROM orders 
GROUP BY customer_id;

-- Sum amounts by category with multiple aggregates
SELECT 
    category,
    COUNT(*) as count,
    SUM(amount) as total,
    AVG(amount) as average,
    MIN(amount) as minimum,
    MAX(amount) as maximum
FROM transactions 
GROUP BY category;
```

### Multiple Column Grouping

```sql
-- Group by customer and status
SELECT customer_id, order_status, COUNT(*) as count, SUM(amount) as total
FROM orders 
GROUP BY customer_id, order_status;
```

### Expression-based Grouping

```sql
-- Time-based grouping
SELECT 
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    COUNT(*) as monthly_orders
FROM orders 
GROUP BY YEAR(order_date), MONTH(order_date);
```

### HAVING Clause

Filter groups based on aggregate conditions:

```sql
-- High-value customers only
SELECT customer_id, SUM(amount) as total_spent
FROM orders 
GROUP BY customer_id 
HAVING SUM(amount) > 1000;
```

### Supported Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all rows | `COUNT(*)` |
| `COUNT(column)` | Count non-null values | `COUNT(amount)` |
| `COUNT_DISTINCT(column)` | Count unique values | `COUNT_DISTINCT(product_id)` |
| `SUM(column)` | Sum numeric values | `SUM(amount)` |
| `AVG(column)` | Average of numeric values | `AVG(price)` |
| `MIN(column)` | Minimum value | `MIN(order_date)` |
| `MAX(column)` | Maximum value | `MAX(order_date)` |
| `STDDEV(column)` | Standard deviation | `STDDEV(amount)` |
| `VARIANCE(column)` | Variance | `VARIANCE(amount)` |
| `FIRST(column)` | First value in group | `FIRST(status)` |
| `LAST(column)` | Last value in group | `LAST(status)` |
| `STRING_AGG(column, sep)` | Concatenate strings | `STRING_AGG(name, ', ')` |

For detailed GROUP BY documentation and examples, see: [GROUP BY Reference](SQL_REFERENCE_GROUP_BY.md)

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

#### Statistical Function Error Handling (✅ Implemented)

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

### Aggregate Functions (✅ Implemented)

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

-- String operations
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
        ELSE UPPER(SUBSTRING(raw_country_input, 1, 1)) || LOWER(SUBSTRING(raw_country_input, 2))  -- 🚧 TODO: INITCAP function
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

### Set Operations (IN/NOT IN)

Set operations allow you to test whether a value exists within a specified list of values. FerrisStreams supports comprehensive IN and NOT IN operators with proper handling of different data types, NULL values, and streaming semantics.

#### Basic IN Operator Usage

The IN operator tests whether a value exists in a list of specified values. It returns true if the value matches any item in the list.

```sql
-- Basic IN operator with integers
SELECT 
    order_id,
    customer_id,
    status
FROM orders
WHERE status IN ('pending', 'processing', 'shipped');

-- IN operator with numeric values
SELECT 
    product_id,
    price,
    category_id
FROM products
WHERE category_id IN (1, 2, 3, 5, 8);

-- IN operator with mixed numeric types
SELECT 
    customer_id,
    order_amount
FROM orders
WHERE customer_id IN (100, 200.0, 300);  -- Handles type conversion
```

#### Basic NOT IN Operator Usage

The NOT IN operator tests whether a value does not exist in the specified list. It returns true if the value doesn't match any item in the list.

```sql
-- NOT IN operator with strings
SELECT 
    customer_id,
    account_status
FROM customers
WHERE account_status NOT IN ('suspended', 'closed', 'inactive');

-- NOT IN operator excluding specific IDs
SELECT 
    order_id,
    customer_id,
    amount
FROM orders
WHERE customer_id NOT IN (999, 1000, 1001);  -- Exclude test customers

-- NOT IN with product categories
SELECT 
    product_id,
    product_name,
    category
FROM products
WHERE category NOT IN ('discontinued', 'seasonal');
```

#### Advanced IN/NOT IN Patterns

```sql
-- Complex filtering with IN/NOT IN
SELECT 
    customer_id,
    order_date,
    amount,
    status,
    payment_method
FROM orders
WHERE status IN ('confirmed', 'shipped', 'delivered')
  AND payment_method NOT IN ('cash', 'check')
  AND amount NOT IN (0.0, 0.01);  -- Exclude zero and test amounts

-- IN/NOT IN with calculated values
SELECT 
    device_id,
    temperature,
    humidity,
    ROUND(temperature) as temp_rounded
FROM sensor_readings
WHERE ROUND(temperature) IN (20, 21, 22, 23, 24)  -- Comfortable temperature range
  AND ROUND(humidity) NOT IN (0, 100);  -- Exclude extreme values

-- Multi-level filtering with IN/NOT IN
SELECT 
    user_id,
    session_id,
    page_url,
    action_type
FROM user_actions
WHERE action_type IN ('click', 'view', 'purchase')
  AND EXTRACT('HOUR', action_timestamp) IN (9, 10, 11, 14, 15, 16)  -- Business hours
  AND user_id NOT IN (
      -- Exclude admin and test users
      0, 999, 1000, 9999
  );
```

#### IN/NOT IN with Streaming Data

```sql
-- Real-time filtering with IN/NOT IN
SELECT 
    event_id,
    user_id,
    event_type,
    _timestamp as kafka_timestamp
FROM streaming_events
WHERE event_type IN ('login', 'purchase', 'logout')
  AND user_id NOT IN (0, -1, 999999);  -- Exclude system users

-- Windowed aggregation with IN/NOT IN filtering
SELECT 
    product_category,
    COUNT(*) as sales_count,
    AVG(amount) as avg_sale_amount
FROM sales_events
WHERE product_category IN ('electronics', 'clothing', 'books')
  AND customer_tier NOT IN ('test', 'internal')
GROUP BY product_category
WINDOW TUMBLING(5m);

-- Event correlation with IN/NOT IN
SELECT 
    user_id,
    COUNT(*) as critical_events,
    LISTAGG(event_type, ', ') as event_types
FROM security_events
WHERE severity IN ('high', 'critical')
  AND event_type NOT IN ('normal_login', 'routine_check')
GROUP BY user_id
HAVING COUNT(*) > 3;
```

#### NULL Handling with IN/NOT IN

IN and NOT IN operators have special behavior with NULL values:

```sql
-- NULL handling examples
SELECT 
    customer_id,
    preferred_contact_method,
    CASE 
        WHEN preferred_contact_method IN ('email', 'sms', 'phone') THEN 'has_preference'
        WHEN preferred_contact_method IS NULL THEN 'no_preference'
        ELSE 'other'
    END as contact_category
FROM customers;

-- Safe NOT IN with potential NULL values
SELECT 
    order_id,
    special_instructions
FROM orders
WHERE special_instructions IS NOT NULL 
  AND special_instructions NOT IN ('', 'none', 'N/A');
```

**Important NULL Behavior:**
- `NULL IN (1, 2, 3)` returns `NULL` (not `false`)
- `NULL NOT IN (1, 2, 3)` returns `NULL` (not `true`)  
- `1 IN (1, NULL, 3)` returns `true`
- `2 IN (1, NULL, 3)` returns `NULL` (unknown due to NULL presence)
- `4 NOT IN (1, NULL, 3)` returns `NULL` (unknown due to NULL presence)

#### Performance and Best Practices

```sql
-- Efficient IN operator usage
SELECT 
    customer_id,
    order_date,
    amount
FROM orders
WHERE customer_id IN (
    -- Use specific, known values for best performance
    12345, 12346, 12347, 12348, 12349
);

-- Combine with early filtering
SELECT 
    event_id,
    user_id,
    event_data
FROM user_events
WHERE _timestamp > (NOW() - INTERVAL '1' HOUR)  -- Filter by time first
  AND event_type IN ('purchase', 'signup', 'upgrade');  -- Then by type

-- Large lists - consider performance implications
SELECT 
    product_id,
    sku,
    availability_status
FROM inventory
WHERE product_id IN (
    -- For very large lists (100+ items), consider alternative approaches
    1001, 1002, 1003, /* ... many more items ... */ 9998, 9999
);
```

#### Common Use Cases

**1. Status Filtering**
```sql
-- Filter active orders
SELECT * FROM orders 
WHERE status IN ('pending', 'confirmed', 'processing', 'shipped');

-- Exclude problematic states
SELECT * FROM user_sessions
WHERE session_status NOT IN ('expired', 'invalid', 'terminated');
```

**2. Category/Type Filtering**
```sql
-- Include specific product categories
SELECT * FROM products
WHERE category IN ('electronics', 'computers', 'mobile_devices');

-- Exclude sensitive event types
SELECT * FROM audit_logs
WHERE event_type NOT IN ('password_change', 'security_token', 'admin_action');
```

**3. ID-based Filtering**
```sql
-- VIP customer processing
SELECT * FROM orders
WHERE customer_id IN (1001, 1002, 1003, 1004, 1005);

-- Exclude test/system accounts
SELECT * FROM user_activity
WHERE user_id NOT IN (0, 999, 9999, 99999);
```

**4. Numeric Range Simulation**
```sql
-- Business hours (simulate BETWEEN with IN)
SELECT * FROM transactions
WHERE EXTRACT('HOUR', transaction_time) IN (9, 10, 11, 12, 13, 14, 15, 16, 17);

-- Weekend processing
SELECT * FROM scheduled_tasks
WHERE EXTRACT('DOW', scheduled_time) NOT IN (0, 6);  -- Exclude Sunday(0) and Saturday(6)
```

**5. Complex Business Logic**
```sql
-- Multi-criteria product filtering
SELECT 
    product_id,
    product_name,
    price,
    category,
    brand
FROM products
WHERE category IN ('premium', 'luxury', 'professional')
  AND brand NOT IN ('generic', 'unknown')
  AND ROUND(price) NOT IN (0, 1);  -- Exclude pricing errors
```

#### Error Handling and Edge Cases

```sql
-- Handle empty results gracefully
SELECT 
    order_id,
    status,
    CASE 
        WHEN status IN ('completed', 'delivered') THEN 'fulfilled'
        WHEN status IN ('pending', 'processing') THEN 'active'
        WHEN status NOT IN ('cancelled', 'failed') THEN 'other'
        ELSE 'problematic'
    END as order_state
FROM orders;

-- Type-safe IN operations
SELECT 
    user_id,
    age,
    membership_level
FROM users
WHERE CAST(age, 'INTEGER') IN (18, 21, 25, 30, 35)
  AND membership_level IN ('gold', 'platinum', 'diamond');
```

#### Streaming Analytics with IN/NOT IN

```sql
-- Real-time KPI monitoring
SELECT 
    metric_name,
    metric_value,
    alert_threshold,
    _timestamp
FROM system_metrics
WHERE metric_name IN ('cpu_usage', 'memory_usage', 'disk_usage')
  AND metric_value NOT IN (0, -1)  -- Exclude invalid readings
  AND metric_value > alert_threshold;

-- Multi-tenant filtering
SELECT 
    tenant_id,
    user_id,
    action_type,
    resource_accessed
FROM tenant_activity
WHERE tenant_id IN ('tenant_1', 'tenant_2', 'tenant_3')
  AND action_type NOT IN ('heartbeat', 'health_check')
  AND resource_accessed IN ('api', 'dashboard', 'reports');
```

The IN and NOT IN operators provide powerful and flexible filtering capabilities for streaming SQL queries, supporting complex business logic while maintaining high performance in real-time data processing scenarios.

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

FerrisStreams supports window operations with simple duration syntax:

#### Window Syntax Format

```sql
-- Tumbling window (non-overlapping fixed intervals)
WINDOW TUMBLING(5m)     -- 5 minutes
WINDOW TUMBLING(1h)     -- 1 hour  
WINDOW TUMBLING(30s)    -- 30 seconds
WINDOW TUMBLING(1d)     -- 1 day

-- Sliding window (overlapping intervals)
WINDOW SLIDING(10m, 5m) -- 10-minute window, advance every 5 minutes
WINDOW SLIDING(1h, 15m) -- 1-hour window, advance every 15 minutes

-- Session window (activity-based grouping)
WINDOW SESSION(5m)      -- 5-minute inactivity gap
WINDOW SESSION(30m)     -- 30-minute inactivity gap
```

**Duration Units Supported:**
- `ns` - nanoseconds
- `us` or `μs` - microseconds  
- `ms` - milliseconds
- `s` - seconds
- `m` - minutes
- `h` - hours
- `d` - days

#### Window Examples

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
WINDOW TUMBLING(1h);
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

## Schema Management and Introspection

FerrisStreams provides comprehensive schema introspection capabilities for discovering and exploring streaming resources. All SHOW operations support pattern matching for efficient resource discovery.

### Stream and Table Discovery

#### SHOW STREAMS
List all registered streams with their metadata and properties.

```sql
-- Show all streams
SHOW STREAMS;

-- Sample output:
-- stream_name | topic          | schema_id      | type
-- orders      | order_events   | orders_v1      | STREAM
-- customers   | customer_data  | customers_v1   | STREAM

-- Show streams with pattern matching
SHOW STREAMS LIKE 'order%';      -- Streams starting with 'order'
SHOW STREAMS LIKE '%summary%';   -- Streams containing 'summary'
SHOW STREAMS LIKE 'user_%';      -- Streams with 'user_' prefix
```

#### SHOW TABLES
List all registered tables (materialized streams) with their metadata.

```sql
-- Show all tables
SHOW TABLES;

-- Sample output:
-- table_name      | topic            | schema_id        | type
-- daily_summary   | summary_topic    | summary_v1       | TABLE
-- customer_stats  | stats_topic      | stats_v1         | TABLE

-- Show tables with pattern matching
SHOW TABLES LIKE 'daily_%';      -- Tables starting with 'daily_'
SHOW TABLES LIKE '%_summary';    -- Tables ending with '_summary'
```

#### SHOW TOPICS
List all Kafka topics from registered streams and tables.

```sql
-- Show all topics
SHOW TOPICS;

-- Sample output:
-- topic_name     | registered
-- order_events   | true
-- customer_data  | true
-- summary_topic  | true

-- Show topics with pattern matching
SHOW TOPICS LIKE '%_events';     -- Topics ending with '_events'
```

### Function Discovery

#### SHOW FUNCTIONS
List all available built-in functions with categories and descriptions.

```sql
-- Show all functions
SHOW FUNCTIONS;

-- Sample output:
-- function_name | category    | description
-- ABS           | Mathematical| Returns absolute value
-- UPPER         | String      | Converts to uppercase
-- COUNT         | Aggregate   | Counts non-null values
-- ROW_NUMBER    | Window      | Row number within partition
-- LAG           | Window      | Previous row value
```

### Schema Inspection

#### DESCRIBE Command
Get detailed schema information for streams and tables.

```sql
-- Describe stream/table schema with full metadata
DESCRIBE orders;
DESCRIBE STREAM orders;
DESCRIBE TABLE customer_summary;

-- Sample output:
-- column_name | data_type | nullable | topic         | schema_id
-- order_id    | Integer   | false    | order_events  | orders_v1
-- customer_id | Integer   | false    | order_events  | orders_v1
-- amount      | Float     | false    | order_events  | orders_v1
-- order_date  | Timestamp | false    | order_events  | orders_v1
-- status      | String    | true     | order_events  | orders_v1
```

#### SHOW SCHEMA
Display column schema for specific resources.

```sql
-- Show schema for a specific stream/table
SHOW SCHEMA orders;

-- Sample output:
-- column_name | data_type | nullable
-- order_id    | Integer   | false
-- customer_id | Integer   | false
-- amount      | Float     | false
-- order_date  | Timestamp | false
-- status      | String    | true
```

### Resource Properties

#### SHOW PROPERTIES
Display detailed properties and configuration for streams and tables.

```sql
-- Show properties for streams
SHOW PROPERTIES STREAM orders;

-- Sample output:
-- property     | value
-- id           | orders_stream_1
-- topic        | order_events
-- schema_id    | orders_schema_v1
-- type         | STREAM
-- field_count  | 5

-- Show properties for tables
SHOW PROPERTIES TABLE customer_summary;

-- Sample output:
-- property     | value
-- id           | summary_table_1
-- topic        | summary_topic
-- schema_id    | summary_schema_v1
-- type         | TABLE
-- field_count  | 4
```

### Advanced Introspection

#### SHOW PARTITIONS
Display partition information for streams and topics.

```sql
-- Show partition information
SHOW PARTITIONS orders;
-- Note: Currently returns placeholder - full implementation planned for job management integration
```

### Pattern Matching

FerrisStreams supports SQL LIKE pattern matching for all SHOW operations:

```sql
-- Wildcard patterns
SHOW STREAMS LIKE '%';           -- All streams (equivalent to SHOW STREAMS)
SHOW TABLES LIKE '_%';           -- Tables with at least one character

-- Prefix matching
SHOW STREAMS LIKE 'order%';      -- Streams starting with 'order'
SHOW TOPICS LIKE 'kafka%';       -- Topics starting with 'kafka'

-- Suffix matching
SHOW STREAMS LIKE '%_events';    -- Streams ending with '_events'
SHOW TABLES LIKE '%_summary';    -- Tables ending with '_summary'

-- Substring matching
SHOW STREAMS LIKE '%customer%';  -- Streams containing 'customer'
SHOW TOPICS LIKE '%_data_%';     -- Topics containing '_data_'

-- Exact matching
SHOW STREAMS LIKE 'orders';      -- Exactly 'orders' stream
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
- `CAST(value, type)` - Type conversion (see [Type Conversion](#type-conversion) for details)
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

### Set Operations (2 operators)
- `IN (value1, value2, ...)` - Test if value exists in list
- `NOT IN (value1, value2, ...)` - Test if value does not exist in list

### System Columns (3 columns)
- `_timestamp` - Kafka message timestamp
- `_offset` - Kafka message offset
- `_partition` - Kafka partition number

**Total: 67 functions/operators + 3 system columns**

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
- **Set Operations:** 2 operators for list membership testing
- **System Columns:** 3 columns for Kafka metadata

## Type Conversion

### Overview

The `CAST(value, type)` function provides comprehensive type conversion capabilities for transforming data between different types during stream processing. FerrisStreams supports conversions between all built-in data types with intelligent handling of edge cases.

### Supported Data Types

#### Core Types (✅ Implemented)
- **INTEGER** (alias: **INT**) - 64-bit signed integer
- **FLOAT** - 64-bit floating point number  
- **STRING** - UTF-8 encoded text
- **BOOLEAN** - True/false value
- **TIMESTAMP** - Date and time values (basic support)
- **DECIMAL** (alias: **NUMERIC**) - High-precision scaled integer for exact financial arithmetic
- **ARRAY(type)** - Array of elements of a specific type
- **MAP(key_type, value_type)** - Key-value pairs
- **STRUCT** - Structured type with named fields

#### Extended Types (🚧 TODO - Planned)
- **DATE** - Date values (YYYY-MM-DD format) 
- **BIGINT** - Extended integer range
- **DOUBLE** - Alias for FLOAT (may be implemented as separate type)

### Type Conversion Matrix (✅ Currently Implemented)

| From\To | INTEGER | FLOAT | STRING | BOOLEAN | TIMESTAMP | DECIMAL |
|---------|---------|-------|--------|---------|-----------|---------|
| **INTEGER** | ✓ | ✓ | ✓ | ✓ | Unix¹ | ✓ |
| **FLOAT** | ✓² | ✓ | ✓ | ✓ | ❌ | ✓ |
| **STRING** | Parse | Parse | ✓ | Parse | Parse | Parse |
| **BOOLEAN** | 0/1 | 0.0/1.0 | ✓ | ✓ | ❌ | 0/1 |
| **TIMESTAMP** | ❌ | ❌ | ✓ | ❌ | ✓ | ❌ |
| **DECIMAL** | ✓² | ✓ | ✓ | ✓³ | ❌ | ✓ |
| **NULL** | NULL | NULL | "NULL"⁴ | NULL | NULL | NULL |

### Extended Conversion Matrix (🚧 TODO - Planned)

| From\To | DATE | BIGINT |
|---------|------|--------|
| **INTEGER** | ❌ | ✓ |
| **FLOAT** | ❌ | ✓² |
| **STRING** | Parse | Parse |
| **BOOLEAN** | ❌ | 0/1 |
| **TIMESTAMP** | ✓⁵ | ❌ |
| **DATE** | ✓ | ❌ |
| **DECIMAL** | ❌ | ✓² |

**Legend:**
- ✓ = Direct conversion supported
- ❌ = Not supported (returns error)  
- Parse = Attempts to parse string representation
- ¹ Unix timestamp (seconds since epoch) → TIMESTAMP
- ² Truncation occurs (fractional part discarded)
- ³ DECIMAL → BOOLEAN: false if 0, true otherwise
- ⁴ Special case: NULL → STRING returns "NULL" string
- ⁵ Date + 00:00:00 time

### Date and Time Parsing

#### DATE Format Support
```sql
-- 🚧 TODO: DATE type not implemented yet
-- CAST('2023-12-25', 'DATE')     -- YYYY-MM-DD (preferred)
-- CAST('2023/12/25', 'DATE')     -- YYYY/MM/DD  
-- CAST('12/25/2023', 'DATE')     -- MM/DD/YYYY
-- CAST('25-12-2023', 'DATE')     -- DD-MM-YYYY
```

#### TIMESTAMP Format Support
```sql
-- Supported timestamp formats
CAST('2023-12-25 14:30:45', 'TIMESTAMP')      -- Standard format
CAST('2023-12-25 14:30:45.123', 'TIMESTAMP')  -- With milliseconds
CAST('2023-12-25T14:30:45', 'TIMESTAMP')      -- ISO 8601
CAST('2023-12-25T14:30:45.123', 'TIMESTAMP')  -- ISO 8601 with ms
CAST('2023/12/25 14:30:45', 'TIMESTAMP')      -- Alternative separator
CAST('2023-12-25', 'TIMESTAMP')               -- Date only (adds 00:00:00)

-- Unix timestamp conversion
CAST(1640995200, 'TIMESTAMP')                 -- Unix seconds → TIMESTAMP
```

### DECIMAL Type - High-Precision Financial Arithmetic

The DECIMAL type (alias: NUMERIC) provides exact precision arithmetic using a ScaledInteger implementation that is **42x faster than f64** for financial calculations while maintaining perfect precision with no floating-point rounding errors.

#### Architecture
- **Internal Storage**: ScaledInteger with value and scale components
- **Precision**: Exact decimal arithmetic without floating-point errors  
- **Performance**: 42x faster than standard floating-point operations
- **Compatibility**: Serializes as decimal strings for cross-system compatibility

#### Basic DECIMAL Usage

```sql
-- Create DECIMAL literals
SELECT 123.45 as price;           -- Automatically parsed as DECIMAL
SELECT 0.0001 as interest_rate;   -- High precision maintained
SELECT 999999.999999 as amount;   -- Large precision supported

-- Create stream/table with DECIMAL columns  
CREATE STREAM financial_data AS
SELECT 
    id,
    CAST(amount, 'DECIMAL') as amount,      -- Convert to high-precision DECIMAL
    CAST(price, 'DECIMAL') as price,        -- Alias: NUMERIC also supported
    CAST(tax_rate, 'DECIMAL') as tax_rate   -- Exact precision for financial rates
FROM orders;

-- Traditional table creation (column types inferred from data)
CREATE TABLE financial_data (
    id INTEGER,
    amount DECIMAL,          -- High-precision amounts
    price NUMERIC,           -- Alias for DECIMAL
    tax_rate DECIMAL
) AS SELECT * FROM orders;
```

#### Type Conversions

```sql
CAST('123.456789012345', 'DECIMAL')           -- String → DECIMAL (high precision)
CAST(42, 'DECIMAL')                           -- INTEGER → DECIMAL  
CAST(3.14159, 'DECIMAL')                      -- FLOAT → DECIMAL
CAST(true, 'DECIMAL')                         -- BOOLEAN → 1.0 or 0.0
```

#### Financial Arithmetic (42x Performance Advantage)

```sql
-- Exact financial calculations with ScaledInteger precision
SELECT 
    order_id,
    price * quantity as subtotal,                    -- Exact multiplication
    price * quantity * tax_rate as tax_amount,       -- Compound precision
    price * quantity * (1.0 + tax_rate) as total    -- No rounding errors
FROM orders
WHERE price > 0.01;  -- Precise decimal comparison

-- Complex financial aggregations
SELECT 
    customer_id,
    SUM(amount) as total_spent,              -- Exact sum aggregation
    AVG(amount) as average_order,            -- Precise average
    COUNT(DISTINCT amount) as unique_amounts -- Exact counting
FROM transactions  
GROUP BY customer_id
HAVING SUM(amount) > 1000.00;               -- Precise threshold
```

#### Performance Comparison
- **DECIMAL (ScaledInteger)**: 1.958µs per operation (exact precision)
- **FLOAT (f64)**: 83.458µs per operation (with precision errors)
- **Performance Gain**: **42x faster** with perfect accuracy

### Practical Examples

#### JSON Data Processing
```sql
-- Extract and convert JSON values
SELECT 
    JSON_VALUE(payload, '$.user_id') as user_id_str,
    CAST(JSON_VALUE(payload, '$.user_id'), 'INTEGER') as user_id,
    CAST(JSON_VALUE(payload, '$.amount'), 'DECIMAL') as precise_amount,  -- High precision for financial data
    CAST(JSON_VALUE(payload, '$.created_at'), 'TIMESTAMP') as created_timestamp
FROM events;
```

#### Data Validation and Cleaning
```sql
-- Convert and validate data types
SELECT
    order_id,
    CASE 
        WHEN amount_str REGEXP '^[0-9]+\.[0-9]+$'  -- 🚧 TODO: REGEXP operator not implemented 
        THEN CAST(amount_str, 'DECIMAL')  -- ✅ High precision decimal conversion (implemented)
        ELSE CAST('0.00', 'FLOAT')
    END as validated_amount,
    CAST(COALESCE(created_date, '1970-01-01'), 'STRING') as safe_date  -- 🚧 TODO: Use DATE when implemented
FROM raw_orders;
```

#### Time-based Analytics
```sql
-- Date/time conversions for analytics
SELECT 
    customer_id,
    CAST(order_date_str, 'STRING') as order_date,  -- 🚧 TODO: Use DATE when implemented
    CAST(order_timestamp_str, 'TIMESTAMP') as order_timestamp,
    DATE_FORMAT(CAST(order_timestamp_str, 'TIMESTAMP'), '%Y-%m-%d') as derived_date  -- Extract date part
FROM customer_orders;
```

#### Financial Calculations
```sql
-- Precise decimal arithmetic for financial data
SELECT
    transaction_id,
    CAST(amount, 'DECIMAL') as amount_decimal,  -- High precision for financial amounts
    CAST(tax_rate, 'DECIMAL') as tax_rate_decimal,  -- Exact tax rate representation
    CAST(amount, 'DECIMAL') * CAST(tax_rate, 'DECIMAL') as tax_amount  -- Precise financial calculation
FROM financial_transactions;
```

### Error Handling

Type conversion failures result in SQL errors with descriptive messages:

```sql
-- These will generate errors:
CAST('invalid-number', 'INTEGER')           -- "Cannot cast 'invalid-number' to INTEGER"
CAST('2023-13-45', 'DATE')                  -- 🚧 TODO: DATE type not implemented yet
CAST({}, 'INTEGER')                         -- "Cannot cast MAP to INTEGER"
```

### Performance Considerations

1. **Date/Time Parsing**: Multiple format attempts may impact performance for large volumes
2. **Decimal Precision**: High-precision arithmetic is slower than native float operations
3. **String Conversions**: Generally fast, but large strings may impact memory usage
4. **Type Checking**: Runtime type validation adds minimal overhead

### Best Practices

1. **Use appropriate precision**: Choose DECIMAL only when precision is critical
2. **Validate inputs**: Check string formats before conversion when possible
3. **Handle NULLs**: Use COALESCE for NULL handling in conversions
4. **Cache conversions**: Avoid repeated CAST operations on the same values
5. **Error handling**: Wrap CAST operations in CASE statements for graceful error handling

This reference guide covers all currently implemented SQL features in FerrisStreams. For the latest updates and additional examples, refer to the test suite and feature documentation.