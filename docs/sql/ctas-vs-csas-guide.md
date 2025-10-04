# CTAS vs CSAS: Complete Decision Guide

## Overview

Velostream provides two powerful SQL constructs for continuous data processing:

- **CTAS (CREATE TABLE AS SELECT)** - Creates materialized tables with queryable state
- **CSAS (CREATE STREAM AS SELECT)** - Creates continuous stream transformations

This guide helps you choose the right approach for your use case.

---

## Quick Decision Tree

```
Do you need to query the data with SQL SELECT statements?
├─ YES → Use CTAS (CREATE TABLE AS SELECT)
│         Creates a queryable table with O(1) lookups
│
└─ NO → Do you need to transform and forward data?
          ├─ YES → Use CSAS (CREATE STREAM AS SELECT)
          │         Creates a continuous stream transformation
          │
          └─ Use both: CTAS for queryable state, CSAS for forwarding
```

---

## CTAS: CREATE TABLE AS SELECT

### Purpose
Creates a **materialized table** that maintains queryable state from a continuous stream.

### Key Characteristics
- ✅ **Queryable**: Run SQL SELECT queries against the table
- ✅ **O(1) Lookups**: OptimizedTableImpl provides 1.85M+ lookups/sec
- ✅ **Stateful**: Maintains current state (latest values, aggregations)
- ✅ **In-Memory**: Data stored in memory for fast access
- ⚠️ **Memory Usage**: Proportional to data size (use CompactTable for large datasets)

### When to Use CTAS

#### ✅ Use CTAS When You Need:

1. **Real-Time Dashboards**
   ```sql
   -- Query current market prices at any time
   CREATE TABLE current_prices AS
   SELECT symbol, price, volume, timestamp
   FROM market_data_stream
   EMIT CHANGES;

   -- Later: SELECT * FROM current_prices WHERE symbol = 'AAPL';
   ```

2. **Aggregated Analytics**
   ```sql
   -- Maintain running totals per customer
   CREATE TABLE customer_totals AS
   SELECT
       customer_id,
       COUNT(*) as order_count,
       SUM(amount) as total_spent
   FROM orders_stream
   GROUP BY customer_id
   EMIT CHANGES;

   -- Later: SELECT * FROM customer_totals WHERE total_spent > 1000;
   ```

3. **JOINs with Reference Data**
   ```sql
   -- Create enriched table for joining
   CREATE TABLE enriched_orders AS
   SELECT
       o.order_id,
       o.amount,
       c.name,
       c.tier
   FROM orders_stream o
   JOIN customers c ON o.customer_id = c.id
   EMIT CHANGES;

   -- Later: Use in other queries
   SELECT * FROM enriched_orders WHERE tier = 'GOLD';
   ```

4. **Lookup Tables**
   ```sql
   -- Maintain current user status
   CREATE TABLE user_status AS
   SELECT user_id, status, last_login, active
   FROM user_events_stream
   EMIT CHANGES;

   -- Later: Fast lookups by user_id
   SELECT status FROM user_status WHERE user_id = 12345;
   ```

### CTAS Syntax

```sql
-- Basic CTAS
CREATE TABLE table_name AS
SELECT columns
FROM source_stream
[WHERE conditions]
[WITH (properties)]
[EMIT CHANGES | EMIT FINAL];

-- CTAS with aggregation
CREATE TABLE aggregated_data AS
SELECT
    group_column,
    COUNT(*) as count,
    SUM(value) as total
FROM source_stream
GROUP BY group_column
[WINDOW TUMBLING(INTERVAL 5 MINUTES)]
EMIT CHANGES;

-- CTAS with named sink (advanced)
CREATE TABLE table_name AS
SELECT columns
FROM source_stream
WITH (
    'source_stream.config_file' = 'kafka_source.yaml',
    'table_name_sink.config_file' = 'warehouse_sink.yaml'
)
EMIT CHANGES;
```

### CTAS Performance Configuration

```sql
-- Fast queries, higher memory (< 1M records)
CREATE TABLE fast_table AS
SELECT * FROM source
WITH (
    'table_model' = 'normal',      -- Standard Table
    'kafka.batch.size' = '1000'
);

-- Memory optimized (> 1M records)
CREATE TABLE large_table AS
SELECT * FROM source
WITH (
    'table_model' = 'compact',     -- CompactTable: 90% memory reduction
    'kafka.batch.size' = '5000',
    'retention' = '30 days'
);
```

---

## CSAS: CREATE STREAM AS SELECT

### Purpose
Creates a **continuous stream transformation** that forwards transformed data to another stream/topic.

### Key Characteristics
- ✅ **No State Storage**: Data flows through without being stored
- ✅ **Low Memory**: Only processes current batch
- ✅ **Stream-to-Stream**: Transforms and forwards to Kafka topics
- ✅ **Filtering/Enrichment**: Apply transformations in real-time
- ⚠️ **Not Queryable**: Cannot run SELECT queries against the stream

### When to Use CSAS

#### ✅ Use CSAS When You Need:

1. **Real-Time Alerting**
   ```sql
   -- Create alert stream for high-value orders
   CREATE STREAM fraud_alerts AS
   SELECT
       customer_id,
       order_id,
       amount,
       'High value order' as alert_type,
       CURRENT_TIMESTAMP as alert_time
   FROM orders_stream
   WHERE amount > 10000
   EMIT CHANGES;

   -- Alerts continuously written to fraud_alerts Kafka topic
   ```

2. **Stream Filtering**
   ```sql
   -- Filter stream for specific conditions
   CREATE STREAM high_priority_events AS
   SELECT
       event_id,
       user_id,
       event_type,
       timestamp
   FROM all_events_stream
   WHERE priority = 'HIGH' AND event_type IN ('ERROR', 'CRITICAL')
   EMIT CHANGES;
   ```

3. **Data Enrichment (Stream-to-Stream)**
   ```sql
   -- Enrich orders with customer data and forward
   CREATE STREAM enriched_orders_stream AS
   SELECT
       o.order_id,
       o.amount,
       c.name as customer_name,
       c.tier as customer_tier,
       o.timestamp
   FROM orders_stream o
   JOIN customers_table c ON o.customer_id = c.customer_id
   EMIT CHANGES;
   ```

4. **Format Transformation**
   ```sql
   -- Convert JSON to Avro for downstream systems
   CREATE STREAM avro_orders AS
   SELECT
       order_id,
       customer_id,
       amount,
       status
   FROM json_orders_stream
   WITH (
       'json_orders_stream.type' = 'kafka_source',
       'json_orders_stream.topic' = 'orders_json',
       'avro_orders_sink.type' = 'kafka_sink',
       'avro_orders_sink.topic' = 'orders_avro',
       'avro_orders_sink.format' = 'avro',
       'avro_orders_sink.schema.registry.url' = 'http://schema-registry:8081'
   )
   EMIT CHANGES;
   ```

5. **Fan-Out / Broadcasting**
   ```sql
   -- Split stream into multiple streams
   CREATE STREAM us_orders AS
   SELECT * FROM orders_stream
   WHERE country = 'US'
   EMIT CHANGES;

   CREATE STREAM eu_orders AS
   SELECT * FROM orders_stream
   WHERE country IN ('UK', 'DE', 'FR')
   EMIT CHANGES;
   ```

6. **Windowed Aggregations → Stream**
   ```sql
   -- Aggregate and forward results
   CREATE STREAM hourly_metrics AS
   SELECT
       sensor_id,
       AVG(temperature) as avg_temp,
       MAX(temperature) as max_temp,
       COUNT(*) as reading_count,
       TUMBLE_START(timestamp, INTERVAL '1' HOUR) as window_start
   FROM sensor_readings_stream
   GROUP BY sensor_id, TUMBLE(timestamp, INTERVAL '1' HOUR)
   EMIT FINAL;
   ```

### CSAS Syntax

```sql
-- Basic CSAS
CREATE STREAM stream_name AS
SELECT columns
FROM source_stream
[WHERE conditions]
[WITH (properties)]
[EMIT CHANGES | EMIT FINAL];

-- CSAS with transformation
CREATE STREAM transformed_stream AS
SELECT
    field1,
    field2 * 100 as calculated_field,
    UPPER(field3) as uppercase_field
FROM source_stream
WHERE field1 IS NOT NULL
EMIT CHANGES;

-- CSAS with named sink
CREATE STREAM stream_name AS
SELECT columns
FROM source_stream
WITH (
    'source_stream.type' = 'kafka_source',
    'source_stream.topic' = 'input_topic',
    'stream_name_sink.type' = 'kafka_sink',
    'stream_name_sink.bootstrap.servers' = 'kafka:9092',
    'stream_name_sink.topic' = 'output_topic',
    'stream_name_sink.format' = 'json'
)
EMIT CHANGES;
```

---

## CTAS vs CSAS: Side-by-Side Comparison

| Feature | CTAS (Tables) | CSAS (Streams) |
|---------|---------------|----------------|
| **Purpose** | Queryable materialized state | Stream transformation pipeline |
| **SQL SELECT Support** | ✅ Yes - O(1) lookups | ❌ No - data flows through |
| **Memory Usage** | High (stores all data) | Low (only current batch) |
| **Use Cases** | Dashboards, analytics, JOINs | Filtering, alerting, ETL |
| **Output** | In-memory table | Kafka topic |
| **State** | Maintains current state | Stateless (no storage) |
| **Performance** | 1.85M+ lookups/sec | Stream throughput only |
| **Latency** | Sub-millisecond queries | Sub-millisecond forwarding |
| **Best For** | < 10M records, frequent queries | Unlimited data, one-way flow |

---

## Common Patterns: Using Both Together

### Pattern 1: Table for Lookups + Stream for Alerts

```sql
-- Create queryable customer table
CREATE TABLE customers AS
SELECT customer_id, name, tier, risk_score
FROM customer_updates_stream
EMIT CHANGES;

-- Create alert stream that references the table
CREATE STREAM high_risk_orders AS
SELECT
    o.order_id,
    o.amount,
    c.name,
    c.risk_score,
    'High risk customer order' as alert_type
FROM orders_stream o
JOIN customers c ON o.customer_id = c.customer_id
WHERE c.risk_score > 80 AND o.amount > 1000
EMIT CHANGES;
```

### Pattern 2: Multiple Streams Feeding Analytics Table

```sql
-- Stream 1: Filter high-value orders
CREATE STREAM high_value_orders AS
SELECT * FROM orders_stream
WHERE amount > 1000
EMIT CHANGES;

-- Stream 2: Filter fraud alerts
CREATE STREAM fraud_flagged_orders AS
SELECT * FROM orders_stream
WHERE fraud_score > 0.8
EMIT CHANGES;

-- Table: Combined analytics
CREATE TABLE order_analytics AS
SELECT
    order_id,
    amount,
    fraud_score,
    'high_value_or_fraud' as category
FROM high_value_orders
UNION ALL
SELECT
    order_id,
    amount,
    fraud_score,
    'high_value_or_fraud' as category
FROM fraud_flagged_orders
EMIT CHANGES;
```

### Pattern 3: Table → Stream Pipeline

```sql
-- Step 1: Create aggregated table
CREATE TABLE customer_stats AS
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent
FROM orders_stream
GROUP BY customer_id
EMIT CHANGES;

-- Step 2: Create stream from table changes
CREATE STREAM vip_customer_alerts AS
SELECT
    customer_id,
    total_spent,
    'VIP customer milestone' as alert_type
FROM customer_stats
WHERE total_spent > 100000
EMIT CHANGES;
```

---

## Memory and Performance Considerations

### CTAS Memory Guidelines

```sql
-- Small dataset (< 100K records): Use normal table model
CREATE TABLE small_dataset AS
SELECT * FROM source
WITH ('table_model' = 'normal');  -- Fast queries, ~100MB memory

-- Medium dataset (100K - 1M records): Consider compact
CREATE TABLE medium_dataset AS
SELECT * FROM source
WITH ('table_model' = 'compact');  -- 90% less memory, ~10MB

-- Large dataset (> 1M records): Use compact + retention
CREATE TABLE large_dataset AS
SELECT * FROM source
WITH (
    'table_model' = 'compact',
    'retention' = '7 days'  -- Limit data size
);

-- Very large (> 10M records): Consider CSAS instead
-- If you don't need queries, use CSAS to avoid memory issues
```

### CSAS Memory Guidelines

```sql
-- CSAS has minimal memory footprint regardless of data volume
CREATE STREAM unlimited_data AS
SELECT * FROM massive_source_stream
WHERE filter_condition
EMIT CHANGES;

-- Memory usage: ~batch_size * record_size (typically < 10MB)
```

---

## Decision Criteria Checklist

### Choose CTAS When:
- [ ] You need to query data with SQL SELECT statements
- [ ] You need fast lookups by key (O(1) access)
- [ ] Dataset is < 10M records (or use CompactTable for larger)
- [ ] You need to JOIN with other queries
- [ ] You need real-time dashboards
- [ ] You need aggregated analytics
- [ ] Memory is available (or use compact mode)

### Choose CSAS When:
- [ ] You only need to forward/transform data
- [ ] You don't need to query the data
- [ ] Dataset is unlimited/very large (> 10M records)
- [ ] You need real-time alerting/notifications
- [ ] You need to filter/enrich streams
- [ ] You want minimal memory footprint
- [ ] You need to change data format (JSON → Avro)
- [ ] You're building ETL pipelines

### Use Both When:
- [ ] You need queryable state AND forwarding
- [ ] You're building complex pipelines
- [ ] You need enrichment with reference data
- [ ] You need both analytics AND alerting

---

## Examples by Use Case

### Financial Trading

```sql
-- CTAS: Queryable current positions
CREATE TABLE current_positions AS
SELECT trader_id, symbol, position_size, avg_price, current_pnl
FROM position_updates_stream
EMIT CHANGES;

-- CSAS: Real-time risk alerts
CREATE STREAM risk_alerts AS
SELECT
    trader_id,
    symbol,
    current_pnl,
    'Position loss exceeds limit' as alert_type
FROM position_updates_stream
WHERE current_pnl < -50000
EMIT CHANGES;
```

### E-Commerce

```sql
-- CTAS: Customer lifetime value
CREATE TABLE customer_lifetime_value AS
SELECT
    customer_id,
    COUNT(*) as total_orders,
    SUM(amount) as lifetime_value,
    AVG(amount) as avg_order_value
FROM orders_stream
GROUP BY customer_id
EMIT CHANGES;

-- CSAS: High-value order notifications
CREATE STREAM vip_order_notifications AS
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    c.lifetime_value
FROM orders_stream o
JOIN customer_lifetime_value c ON o.customer_id = c.customer_id
WHERE o.amount > 1000 OR c.lifetime_value > 10000
EMIT CHANGES;
```

### IoT Monitoring

```sql
-- CTAS: Current sensor readings
CREATE TABLE current_sensor_state AS
SELECT
    device_id,
    sensor_type,
    value,
    timestamp,
    status
FROM sensor_readings_stream
EMIT CHANGES;

-- CSAS: Anomaly detection stream
CREATE STREAM sensor_anomalies AS
SELECT
    device_id,
    sensor_type,
    value,
    'Threshold exceeded' as anomaly_type
FROM sensor_readings_stream
WHERE value > 100 OR value < 0
EMIT CHANGES;
```

---

## Best Practices

### CTAS Best Practices

1. **Choose the right table model**
   ```sql
   -- < 1M records: normal
   -- > 1M records: compact
   WITH ('table_model' = 'compact')
   ```

2. **Set appropriate retention**
   ```sql
   -- Limit memory growth
   WITH ('retention' = '7 days')
   ```

3. **Use batch optimization**
   ```sql
   WITH (
       'kafka.batch.size' = '1000',
       'kafka.compression.type' = 'zstd'
   )
   ```

4. **Monitor memory usage**
   - Check table statistics regularly
   - Use CompactTable for large datasets
   - Consider CSAS if queries aren't needed

### CSAS Best Practices

1. **Use EMIT CHANGES for real-time**
   ```sql
   EMIT CHANGES;  -- Sub-millisecond latency
   ```

2. **Use EMIT FINAL for complete windows**
   ```sql
   WINDOW TUMBLING(INTERVAL 1 HOUR)
   EMIT FINAL;  -- Complete hourly results
   ```

3. **Specify output format**
   ```sql
   WITH (
       'sink.format' = 'avro',
       'sink.schema.registry.url' = 'http://schema-registry:8081'
   )
   ```

4. **Configure error handling**
   ```sql
   WITH (
       'error.handling' = 'skip',
       'error.topic' = 'processing_errors'
   )
   ```

---

## Troubleshooting

### CTAS Issues

**Problem**: Out of memory errors
```sql
-- Solution: Use CompactTable
CREATE TABLE large_data AS
SELECT * FROM source
WITH (
    'table_model' = 'compact',  -- 90% memory reduction
    'retention' = '7 days'       -- Limit data age
);
```

**Problem**: Slow queries
```sql
-- Solution: Ensure you're using OptimizedTableImpl (automatic)
-- Check table statistics to verify O(1) lookups
```

**Problem**: Table not updating
```sql
-- Solution: Ensure EMIT CHANGES is specified
EMIT CHANGES;  -- Without this, table may not update
```

### CSAS Issues

**Problem**: Data not appearing in output topic
```sql
-- Solution: Verify sink configuration
WITH (
    'sink.bootstrap.servers' = 'kafka:9092',
    'sink.topic' = 'output-topic'
)
```

**Problem**: High latency
```sql
-- Solution: Reduce batch size
WITH ('kafka.batch.size' = '100')  -- Smaller batches = lower latency
```

---

## Summary

- **CTAS** = Queryable state, dashboards, analytics
- **CSAS** = Stream transformation, alerting, ETL
- **Use CTAS** when you need SQL SELECT queries
- **Use CSAS** when you only need to forward/transform
- **Use both** for complex pipelines combining analytics and alerting

For most applications, you'll use **both**: CTAS for queryable state and CSAS for real-time processing.
