# Comprehensive Velostream SQL/CTAS Syntax Guide

## 🎯 **Complete Reference for Streaming SQL, CTAS, File Input, and CDC Streams**

**Date:** September 26, 2025
**Version:** Production-ready implementation
**Architecture:** Three-component table system (Table, CompactTable, OptimizedTableImpl)

## Table of Contents

1. [Overview](#overview)
2. [SQL Query Syntax](#sql-query-syntax)
3. [CREATE TABLE AS SELECT (CTAS)](#create-table-as-select-ctas)
4. [CREATE STREAM AS SELECT (CSAS)](#create-stream-as-select-csas)
5. [EMIT CHANGES and CDC Streams](#emit-changes-and-cdc-streams)
6. [File Input Configuration](#file-input-configuration)
7. [Kafka Integration and Sources](#kafka-integration-and-sources)
8. [Advanced Window Operations](#advanced-window-operations)
9. [Performance Configuration](#performance-configuration)
10. [Complete Examples](#complete-examples)

---

## Overview

Velostream provides a complete streaming SQL engine with enterprise-grade performance characteristics:

- **1.85M+ lookups/sec** with O(1) table operations
- **100K+ records/sec** data ingestion
- **Real-time CDC streaming** with EMIT CHANGES syntax
- **Comprehensive file format support** (JSON, Avro, Protobuf, CSV)
- **Advanced windowing** (tumbling, sliding, session windows)
- **Three-component architecture** for optimized performance

### Supported SQL Features

| Feature Category | Support Level | Examples |
|------------------|---------------|----------|
| **Basic SELECT** | ✅ Complete | `SELECT * FROM orders WHERE amount > 100` |
| **Aggregations** | ✅ Complete | `COUNT(*)`, `SUM(amount)`, `AVG(price)` |
| **Window Functions** | ✅ Complete | `WINDOW TUMBLING(INTERVAL 5 MINUTES)` |
| **JOINs** | ✅ Complete | `INNER JOIN`, `LEFT JOIN` with table aliases |
| **EMIT Modes** | ✅ Complete | `EMIT CHANGES`, `EMIT FINAL` |
| **Subqueries** | ✅ Complete | Correlated and scalar subqueries |
| **UNION** | ✅ Complete | `UNION` and `UNION ALL` operations |
| **Data Modification** | ✅ Complete | `INSERT`, `UPDATE`, `DELETE` |

---

## SQL Query Syntax

### Basic SELECT Queries

```sql
-- Simple selection with filtering
SELECT customer_id, amount, timestamp
FROM orders
WHERE amount > 100
LIMIT 50;

-- Aggregation with GROUP BY
SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total
FROM orders
GROUP BY customer_id
HAVING total > 1000;

-- Advanced filtering with functions
SELECT symbol, price, volume
FROM trades
WHERE price BETWEEN 100 AND 200
  AND EXTRACT(HOUR FROM timestamp) BETWEEN 9 AND 16;
```

### Window Operations

```sql
-- Tumbling window (fixed intervals)
SELECT symbol, AVG(price) as avg_price, COUNT(*) as trade_count
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL 5 MINUTES)
EMIT CHANGES;

-- Sliding window (overlapping intervals)
SELECT customer_id, SUM(amount) as rolling_total
FROM orders
GROUP BY customer_id
WINDOW SLIDING(INTERVAL 10 MINUTES, ADVANCE BY INTERVAL 1 MINUTE)
EMIT CHANGES;

-- Session window (activity-based)
SELECT user_id, COUNT(*) as session_actions
FROM user_events
GROUP BY user_id
WINDOW SESSION(INTERVAL 30 MINUTES)
EMIT FINAL;
```

### Advanced Features

```sql
-- Table aliases in complex queries
SELECT
    p.trader_id,
    m.symbol,
    LAG(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) as prev_price
FROM market_data m
JOIN positions p ON m.symbol = p.symbol;

-- INTERVAL window frames (SQL standard)
SELECT
    symbol, price,
    AVG(price) OVER (
        PARTITION BY symbol
        ORDER BY event_time
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as hourly_moving_avg
FROM trades;

-- Dual EXTRACT syntax support
SELECT
    EXTRACT(EPOCH FROM (end_time - start_time)) as duration_seconds,
    EXTRACT(YEAR FROM order_date) as order_year
FROM orders;
```

---

## CREATE TABLE AS SELECT (CTAS)

CTAS creates materialized tables with continuous data ingestion and high-performance SQL queries using the three-component architecture.

### Basic CTAS Syntax

```sql
-- Basic Kafka source table
CREATE TABLE market_data AS
SELECT symbol, price, volume, timestamp
FROM kafka://market-feed
EMIT CHANGES;

-- Table with performance optimization
CREATE TABLE high_volume_trades AS
SELECT symbol, price, volume, trader_id
FROM kafka://trades-topic
WHERE volume > 10000
WITH (
    'table_model' = 'compact',      -- Use CompactTable for memory efficiency
    'kafka.batch.size' = '1000',
    'retention' = '7 days'
)
EMIT CHANGES;
```

### Performance Configuration Options

#### Table Model Selection

The `table_model` property controls which table implementation to use:

```sql
-- Standard Table (default) - Fastest queries, higher memory usage
CREATE TABLE fast_queries AS
SELECT * FROM kafka://data-topic
WITH (
    'table_model' = 'normal',        -- Standard Table
    'kafka.batch.size' = '1000'
);

-- CompactTable - Memory optimized, slight CPU overhead
CREATE TABLE large_dataset AS
SELECT * FROM kafka://big-data-topic
WITH (
    'table_model' = 'compact',       -- 90% memory reduction
    'kafka.batch.size' = '5000'     -- Larger batches for efficiency
);
```

**Performance Characteristics:**

| Table Model | Memory Usage | Query Speed | Use Case |
|-------------|--------------|-------------|-----------|
| **normal** (Standard) | Higher | Fastest (O(1)) | Real-time analytics, <1M records |
| **compact** (CompactTable) | 90% less | ~10% slower | Large datasets, >1M records |

#### Kafka Configuration

```sql
CREATE TABLE configured_table AS
SELECT * FROM kafka://topic-name
WITH (
    -- Kafka consumer settings
    'kafka.batch.size' = '1000',
    'kafka.linger.ms' = '100',
    'kafka.compression.type' = 'zstd',

    -- Table settings
    'retention' = '30 days',
    'table_model' = 'compact'
);
```

### File-Based CTAS Configuration

```sql
-- Using configuration files
CREATE TABLE file_based_data AS
SELECT * FROM configured_source
WITH (
    'config_file' = 'data_sources.yaml'
);
```

**Example `data_sources.yaml`:**
```yaml
# Source configuration
source:
  type: file
  path: "/data/financial/trades.json"
  format: json
  schema:
    symbol: string
    price: decimal(10,2)
    volume: integer
    timestamp: timestamp

# Ingestion settings
ingestion:
  batch_size: 1000
  poll_interval: 1000
  error_handling: skip
```

---

## CREATE STREAM AS SELECT (CSAS)

CSAS creates continuous streams that transform and forward data to Kafka topics.

### Basic CSAS Syntax

```sql
-- Basic stream transformation
CREATE STREAM high_value_orders AS
SELECT customer_id, amount, timestamp, HEADER('source') AS source
FROM orders
WHERE amount > 1000
EMIT CHANGES;

-- Stream with aggregation
CREATE STREAM customer_metrics AS
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent,
    CURRENT_TIMESTAMP as calculation_time
FROM orders
GROUP BY customer_id
WINDOW TUMBLING(INTERVAL 1 HOUR)
EMIT FINAL;
```

### INTO Clause for Multi-Config Streaming

```sql
-- Multi-configuration streaming job
CREATE STREAM trading_pipeline AS
SELECT
    symbol,
    price,
    volume,
    price * volume as notional
FROM kafka://raw-trades
WHERE price > 0 AND volume > 0
INTO trading_sink
WITH (
    source_config = 'kafka_source.yaml',
    sink_config = 'elasticsearch_sink.yaml',
    monitoring_config = 'metrics.yaml'
)
EMIT CHANGES;
```

---

## EMIT CHANGES and CDC Streams

The `EMIT` clause controls when and how results are emitted from streaming queries, enabling Change Data Capture (CDC) patterns.

### EMIT CHANGES (CDC Mode)

Emits results immediately as changes occur:

```sql
-- Real-time order monitoring
SELECT customer_id, amount, status, timestamp
FROM orders
WHERE status IN ('pending', 'processing', 'completed')
EMIT CHANGES;

-- Continuous aggregation updates
SELECT
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    SUM(volume) as total_volume
FROM trades
GROUP BY symbol
EMIT CHANGES;
```

**Characteristics:**
- **Latency**: Sub-millisecond to millisecond
- **Use Cases**: Real-time dashboards, alerting, live metrics
- **Output**: Continuous stream of changes as they happen

### EMIT FINAL (Batch Mode)

Emits complete results when windows close:

```sql
-- Hourly summary reports
SELECT
    trader_id,
    COUNT(*) as trades,
    SUM(amount) as volume,
    AVG(price) as avg_price
FROM trading_activity
GROUP BY trader_id
WINDOW TUMBLING(INTERVAL 1 HOUR)
EMIT FINAL;
```

**Characteristics:**
- **Latency**: Window duration (minutes to hours)
- **Use Cases**: Reports, batch processing, complete aggregations
- **Output**: Complete results at window boundaries

### CDC Stream Configuration

```sql
-- Complete CDC pipeline for audit logs
CREATE STREAM audit_changes AS
SELECT
    table_name,
    operation_type,
    changed_fields,
    old_values,
    new_values,
    change_timestamp,
    user_id
FROM database_changes
WHERE operation_type IN ('INSERT', 'UPDATE', 'DELETE')
WITH (
    'cdc.format' = 'debezium',
    'cdc.include.schema.changes' = 'true',
    'kafka.batch.size' = '100'
)
EMIT CHANGES;
```

---

## File Input Configuration

Velostream supports multiple file input formats and sources with comprehensive configuration options.

### Supported File Formats

| Format | Extension | Configuration | Schema Support |
|--------|-----------|---------------|----------------|
| **JSON** | `.json`, `.jsonl` | `format: json` | ✅ Inferred |
| **CSV** | `.csv` | `format: csv` | ✅ Header-based |
| **Avro** | `.avro` | `format: avro` | ✅ Embedded schema |
| **Parquet** | `.parquet` | `format: parquet` | ✅ Embedded schema |

### File Source Configuration

#### Direct File References

```sql
-- JSON file input
CREATE TABLE customer_data AS
SELECT customer_id, name, email, registration_date
FROM file:///data/customers.json
WITH (
    'file.format' = 'json',
    'file.schema.inference' = 'true',
    'file.error.handling' = 'skip'
);

-- CSV with schema
CREATE TABLE sales_data AS
SELECT date, product_id, quantity, price
FROM file:///data/sales.csv
WITH (
    'file.format' = 'csv',
    'file.header' = 'true',
    'file.delimiter' = ',',
    'file.quote.char' = '"'
);
```

#### Configuration File-Based Input

```sql
-- Using external configuration
CREATE TABLE external_data AS
SELECT * FROM configured_file_source
WITH (
    'config_file' = 'file_inputs.yaml'
);
```

**Example `file_inputs.yaml`:**
```yaml
source:
  type: file
  paths:
    - "/data/financial/*.json"
    - "/data/trades/$(date)/*.avro"
  format: json

  # Schema configuration
  schema:
    auto_inference: true
    nullable_fields: ["optional_field"]

  # Processing options
  processing:
    batch_size: 1000
    parallel_readers: 4
    compression: gzip

  # Error handling
  error_handling:
    mode: skip  # skip, fail, dead_letter_queue
    max_errors: 100
    error_topic: "file_processing_errors"

# Monitoring
monitoring:
  metrics_topic: "file_ingestion_metrics"
  progress_reporting: 5000  # Every 5000 records
```

#### Advanced File Processing

```sql
-- Multi-file pattern with transformation
CREATE TABLE processed_logs AS
SELECT
    timestamp,
    level,
    message,
    EXTRACT_JSON(details, '$.user_id') as user_id,
    filename() as source_file
FROM file:///logs/app-*.json
WHERE level IN ('ERROR', 'WARN')
WITH (
    'file.pattern.recursive' = 'true',
    'file.processing.mode' = 'streaming',  -- Process as files arrive
    'file.watch.interval' = '5000'         -- Check every 5 seconds
)
EMIT CHANGES;
```

---

## Kafka Integration and Sources

### Basic Kafka Sources

```sql
-- Simple Kafka topic consumption
SELECT symbol, price, volume
FROM kafka://market-data-topic;

-- Kafka with consumer group
SELECT user_id, action, timestamp
FROM kafka://user-events-topic
WITH (
    'kafka.group.id' = 'analytics-consumers',
    'kafka.auto.offset.reset' = 'earliest'
);
```

### Advanced Kafka Configuration

```sql
CREATE TABLE kafka_optimized AS
SELECT * FROM kafka://high-volume-topic
WITH (
    -- Consumer performance
    'kafka.batch.size' = '1000',
    'kafka.linger.ms' = '100',
    'kafka.compression.type' = 'zstd',
    'kafka.max.poll.records' = '5000',

    -- Memory optimization
    'table_model' = 'compact',

    -- Retention and cleanup
    'retention' = '7 days',
    'kafka.cleanup.policy' = 'delete'
);
```

### Multi-Topic and Pattern Sources

```sql
-- Multiple topics
SELECT topic_name(), partition(), offset(), timestamp, data
FROM kafka://(orders-topic|payments-topic|refunds-topic)
WHERE data IS NOT NULL;

-- Topic patterns
SELECT *
FROM kafka://financial-.*
WITH (
    'kafka.topic.pattern' = 'true',
    'kafka.metadata.include' = 'true'
);
```

### Schema Registry Integration

```sql
-- Avro with Schema Registry
CREATE TABLE avro_orders AS
SELECT order_id, customer_id, amount, items
FROM kafka://orders-avro-topic
WITH (
    'value.format' = 'avro',
    'avro.schema.registry.url' = 'http://schema-registry:8081',
    'avro.schema.id' = '12345'
);

-- Protobuf with Schema Registry
CREATE TABLE protobuf_trades AS
SELECT symbol, price, quantity, trader_id
FROM kafka://trades-protobuf-topic
WITH (
    'value.format' = 'protobuf',
    'protobuf.schema.registry.url' = 'http://schema-registry:8081',
    'protobuf.message.type' = 'trading.TradeEvent'
);
```

---

## Advanced Window Operations

### Time-Based Windows

```sql
-- Fixed tumbling windows
SELECT
    symbol,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as trade_count
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL 5 MINUTES)
EMIT FINAL;

-- Sliding windows with custom advance
SELECT
    customer_id,
    SUM(amount) as rolling_30min_total
FROM orders
GROUP BY customer_id
WINDOW SLIDING(INTERVAL 30 MINUTES, ADVANCE BY INTERVAL 5 MINUTES)
EMIT CHANGES;

-- Session windows for user activity
SELECT
    user_id,
    COUNT(*) as session_events,
    MIN(timestamp) as session_start,
    MAX(timestamp) as session_end
FROM user_activities
GROUP BY user_id
WINDOW SESSION(INTERVAL 20 MINUTES)
EMIT FINAL;
```

### RANGE-Based Windows with INTERVAL

```sql
-- Price-based range window
SELECT
    symbol,
    timestamp,
    price,
    AVG(price) OVER (
        PARTITION BY symbol
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as hourly_avg
FROM stock_prices;

-- Custom time ranges
SELECT
    trader_id,
    trade_time,
    amount,
    SUM(amount) OVER (
        PARTITION BY trader_id
        ORDER BY trade_time
        RANGE BETWEEN INTERVAL '2' HOURS PRECEDING
                  AND INTERVAL '30' MINUTES FOLLOWING
    ) as context_volume
FROM trading_activity;
```

---

## Performance Configuration

### Table Performance Optimization

The three-component table architecture provides multiple performance optimization strategies:

#### 1. OptimizedTableImpl (Query Performance)
- **Best For**: High-frequency queries, real-time analytics
- **Performance**: 1.85M+ lookups/sec, O(1) operations
- **Memory**: String interning optimization

```sql
-- Automatically uses OptimizedTableImpl for SQL operations
CREATE TABLE fast_queries AS
SELECT * FROM kafka://data-topic
WITH (
    'table_model' = 'normal'  -- Standard ingestion + OptimizedTableImpl queries
);
```

#### 2. CompactTable (Memory Efficiency)
- **Best For**: Large datasets (>1M records), memory-constrained environments
- **Performance**: 90% memory reduction, ~10% CPU overhead
- **Use Cases**: Historical data, large-scale analytics

```sql
CREATE TABLE memory_efficient AS
SELECT * FROM kafka://large-dataset-topic
WITH (
    'table_model' = 'compact',     -- CompactTable for ingestion
    'kafka.batch.size' = '5000'    -- Larger batches for efficiency
);
```

#### 3. Hybrid Configuration (Best of Both)

```sql
-- High-performance hybrid setup
CREATE TABLE production_ready AS
SELECT
    customer_id,
    amount,
    timestamp,
    status
FROM kafka://orders-topic
WHERE amount > 0
WITH (
    -- Ingestion optimization
    'table_model' = 'compact',           -- Memory-efficient ingestion
    'kafka.batch.size' = '2000',         -- Optimal batch size
    'kafka.compression.type' = 'zstd',   -- Best compression

    -- Performance tuning
    'retention' = '30 days',
    'kafka.linger.ms' = '50',            -- Balance latency/throughput

    -- Query optimization (automatic OptimizedTableImpl)
    -- Gets O(1) query performance automatically
)
EMIT CHANGES;
```

### Batch Processing Configuration

```sql
-- Large-scale batch processing
CREATE TABLE batch_optimized AS
SELECT
    date_trunc('hour', timestamp) as hour,
    symbol,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume,
    AVG(price) as avg_price
FROM kafka://trades-stream
GROUP BY date_trunc('hour', timestamp), symbol
WITH (
    'table_model' = 'compact',
    'kafka.batch.size' = '10000',        -- Large batches
    'kafka.max.poll.records' = '10000',  -- Maximize throughput
    'kafka.compression.type' = 'zstd'    -- Best compression ratio
)
WINDOW TUMBLING(INTERVAL 1 HOUR)
EMIT FINAL;
```

---

## Complete Examples

### 1. Real-Time Financial Trading System

```sql
-- High-frequency trade ingestion with CompactTable
CREATE TABLE trades AS
SELECT
    symbol,
    price,
    volume,
    trader_id,
    timestamp,
    price * volume as notional
FROM kafka://trades-feed
WHERE price > 0 AND volume > 0
WITH (
    'table_model' = 'compact',           -- Memory optimized for high volume
    'kafka.batch.size' = '1000',
    'kafka.compression.type' = 'zstd',
    'retention' = '1 day'
)
EMIT CHANGES;

-- Real-time risk monitoring with OptimizedTableImpl queries
SELECT
    trader_id,
    symbol,
    SUM(notional) as position_value,
    COUNT(*) as trade_count
FROM trades
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY trader_id, symbol
HAVING SUM(notional) > 1000000  -- $1M position limit
EMIT CHANGES;

-- Market data aggregation
CREATE STREAM market_summary AS
SELECT
    symbol,
    AVG(price) as vwap,
    MIN(price) as low,
    MAX(price) as high,
    SUM(volume) as total_volume,
    COUNT(*) as trade_count
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL 1 MINUTE)
EMIT FINAL;
```

### 2. E-commerce Order Processing Pipeline

```sql
-- Multi-source order ingestion
CREATE TABLE orders AS
SELECT
    order_id,
    customer_id,
    amount,
    status,
    timestamp,
    HEADER('source_system') as source
FROM kafka://(web-orders|mobile-orders|api-orders)
WITH (
    'table_model' = 'normal',            -- Standard for fast queries
    'kafka.batch.size' = '500'
)
EMIT CHANGES;

-- Customer analytics with window functions
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value,
    MIN(timestamp) as first_order,
    MAX(timestamp) as last_order
FROM orders
GROUP BY customer_id
WINDOW SESSION(INTERVAL 30 MINUTES)
EMIT FINAL;

-- Fraud detection stream
CREATE STREAM fraud_alerts AS
SELECT
    customer_id,
    order_id,
    amount,
    'High velocity orders' as alert_type,
    CURRENT_TIMESTAMP as alert_time
FROM orders
WHERE customer_id IN (
    SELECT customer_id
    FROM orders
    GROUP BY customer_id
    WINDOW SLIDING(INTERVAL 10 MINUTES, ADVANCE BY INTERVAL 1 MINUTE)
    HAVING COUNT(*) > 5  -- More than 5 orders in 10 minutes
)
EMIT CHANGES;
```

### 3. IoT Sensor Data Processing

```sql
-- Sensor data with file and Kafka sources
CREATE TABLE sensor_readings AS
SELECT
    device_id,
    sensor_type,
    value,
    timestamp,
    location
FROM kafka://iot-sensors
UNION ALL
SELECT
    device_id,
    sensor_type,
    value,
    timestamp,
    location
FROM file:///data/historical/sensors-*.json
WITH (
    'table_model' = 'compact',           -- Handle large IoT datasets
    'kafka.batch.size' = '2000',
    'file.processing.mode' = 'streaming'
)
EMIT CHANGES;

-- Anomaly detection with statistical functions
SELECT
    device_id,
    sensor_type,
    value,
    AVG(value) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as avg_1hr,
    STDDEV(value) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as stddev_1hr
FROM sensor_readings
WHERE ABS(value - avg_1hr) > 3 * stddev_1hr  -- 3-sigma anomaly detection
EMIT CHANGES;
```

### 4. Log Processing and Monitoring

```sql
-- Multi-format log ingestion
CREATE TABLE application_logs AS
SELECT
    timestamp,
    level,
    message,
    service_name,
    CASE
        WHEN message LIKE '%ERROR%' THEN 'error'
        WHEN message LIKE '%WARN%' THEN 'warning'
        ELSE 'info'
    END as severity
FROM file:///logs/app-*.json
WHERE level IN ('ERROR', 'WARN', 'INFO')
WITH (
    'file.pattern.recursive' = 'true',
    'file.watch.interval' = '1000',
    'table_model' = 'compact'            -- Handle large log volumes
)
EMIT CHANGES;

-- Error rate monitoring
CREATE STREAM error_rates AS
SELECT
    service_name,
    COUNT(*) as total_logs,
    SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) as error_count,
    (SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as error_rate
FROM application_logs
GROUP BY service_name
WINDOW TUMBLING(INTERVAL 5 MINUTES)
HAVING error_rate > 5.0  -- Alert if error rate > 5%
EMIT FINAL;
```

---

## Configuration File Examples

### Complete Kafka Configuration

```yaml
# kafka_source.yaml
source:
  type: kafka
  brokers: "localhost:9092,localhost:9093,localhost:9094"
  topic: "financial-trades"

  # Consumer settings
  consumer:
    group_id: "velostream-analytics"
    auto_offset_reset: "earliest"
    enable_auto_commit: false
    max_poll_records: 1000

  # Serialization
  key:
    format: string
  value:
    format: avro
    schema_registry_url: "http://schema-registry:8081"

  # Performance
  performance:
    batch_size: 1000
    linger_ms: 50
    compression_type: zstd
    buffer_memory: 67108864  # 64MB
```

### Complete File Configuration

```yaml
# file_source.yaml
source:
  type: file
  paths:
    - "/data/trades/*.json"
    - "/data/orders/*.avro"

  # Format detection
  format: auto  # or json, avro, csv, parquet

  # Processing
  processing:
    batch_size: 1000
    parallel_readers: 4
    watch_mode: true
    watch_interval: 5000

  # Schema
  schema:
    auto_inference: true
    strict_mode: false

  # Error handling
  error_handling:
    mode: skip
    max_errors: 100
    error_file: "/logs/processing_errors.log"
```

---

## Complete WITH Clause Property Reference

### Overview

The `WITH` clause configures table and stream behavior. Properties are specified as key-value pairs in the format `'property.name' = 'value'`.

### Table Configuration Properties

| Property | Type | Default | Description | Example |
|----------|------|---------|-------------|---------|
| **`table_model`** | enum | `normal` | Table implementation: `normal` (Standard Table) or `compact` (CompactTable) | `'table_model' = 'compact'` |
| **`retention`** | duration | None | How long to keep data in the table | `'retention' = '7 days'` |
| **`refresh_interval`** | duration | Continuous | How often to refresh table data | `'refresh_interval' = '1 hour'` |
| **`compression`** | enum | None | Compression type: `snappy`, `gzip`, `zstd` | `'compression' = 'zstd'` |

### Kafka Consumer Properties

| Property | Type | Default | Description | Example |
|----------|------|---------|-------------|---------|
| **`kafka.bootstrap.servers`** | string | localhost:9092 | Kafka broker addresses | `'kafka.bootstrap.servers' = 'kafka:9092'` |
| **`kafka.group.id`** | string | Auto-generated | Consumer group ID | `'kafka.group.id' = 'analytics-group'` |
| **`kafka.auto.offset.reset`** | enum | `latest` | Offset reset policy: `earliest`, `latest` | `'kafka.auto.offset.reset' = 'earliest'` |
| **`kafka.batch.size`** | integer | 100 | Number of records per batch | `'kafka.batch.size' = '1000'` |
| **`kafka.linger.ms`** | integer | 0 | Wait time before sending batch (milliseconds) | `'kafka.linger.ms' = '50'` |
| **`kafka.max.poll.records`** | integer | 500 | Maximum records in single poll | `'kafka.max.poll.records' = '5000'` |
| **`kafka.compression.type`** | enum | None | Compression: `zstd`, `gzip`, `snappy`, `lz4` | `'kafka.compression.type' = 'zstd'` |
| **`kafka.enable.auto.commit`** | boolean | `false` | Enable automatic offset commits | `'kafka.enable.auto.commit' = 'true'` |
| **`kafka.cleanup.policy`** | enum | `delete` | Cleanup policy: `delete`, `compact` | `'kafka.cleanup.policy' = 'compact'` |

### File Source Properties

| Property | Type | Default | Description | Example |
|----------|------|---------|-------------|---------|
| **`file.format`** | enum | `json` | File format: `json`, `csv`, `avro`, `parquet` | `'file.format' = 'avro'` |
| **`file.path`** | string | Required | Path to file(s) - supports wildcards | `'file.path' = '/data/*.json'` |
| **`file.header`** | boolean | `true` | CSV file has header row | `'file.header' = 'true'` |
| **`file.delimiter`** | char | `,` | CSV delimiter character | `'file.delimiter' = '\|'` |
| **`file.quote.char`** | char | `"` | CSV quote character | `'file.quote.char' = '\''` |
| **`file.schema.inference`** | boolean | `true` | Automatically infer schema | `'file.schema.inference' = 'true'` |
| **`file.error.handling`** | enum | `fail` | Error handling: `skip`, `fail`, `dead_letter_queue` | `'file.error.handling' = 'skip'` |
| **`file.pattern.recursive`** | boolean | `false` | Recursively process directories | `'file.pattern.recursive' = 'true'` |
| **`file.watch.interval`** | integer | 0 | File watch interval (milliseconds), 0=disabled | `'file.watch.interval' = '5000'` |
| **`file.processing.mode`** | enum | `batch` | Processing mode: `batch`, `streaming` | `'file.processing.mode' = 'streaming'` |

### Schema Registry Properties

| Property | Type | Default | Description | Example |
|----------|------|---------|-------------|---------|
| **`value.format`** | enum | `json` | Value serialization format | `'value.format' = 'avro'` |
| **`key.format`** | enum | `string` | Key serialization format | `'key.format' = 'avro'` |
| **`avro.schema.registry.url`** | string | None | Avro schema registry URL | `'avro.schema.registry.url' = 'http://schema-registry:8081'` |
| **`avro.schema.id`** | integer | None | Specific Avro schema ID to use | `'avro.schema.id' = '12345'` |
| **`protobuf.schema.registry.url`** | string | None | Protobuf schema registry URL | `'protobuf.schema.registry.url' = 'http://schema-registry:8081'` |
| **`protobuf.message.type`** | string | None | Protobuf message type name | `'protobuf.message.type' = 'trading.Order'` |

### Multi-Config Properties (INTO Clause)

| Property | Type | Default | Description | Example |
|----------|------|---------|-------------|---------|
| **`config_file`** | path | None | Path to external YAML configuration file | `'config_file' = 'source.yaml'` |
| **`source_config`** | path | None | Path to source configuration file | `'source_config' = 'kafka_source.yaml'` |
| **`sink_config`** | path | None | Path to sink configuration file | `'sink_config' = 'warehouse_sink.yaml'` |
| **`monitoring_config`** | path | None | Path to monitoring configuration file | `'monitoring_config' = 'metrics.yaml'` |

### Error Handling Properties

| Property | Type | Default | Description | Example |
|----------|------|---------|-------------|---------|
| **`error.handling`** | enum | `fail` | Error handling strategy: `skip`, `fail`, `dead_letter_queue` | `'error.handling' = 'skip'` |
| **`error.topic`** | string | None | Dead letter queue topic name | `'error.topic' = 'processing_errors'` |
| **`error.max.retries`** | integer | 3 | Maximum retry attempts | `'error.max.retries' = '5'` |

### Complete Example: All Properties

```sql
CREATE TABLE production_ready_table AS
SELECT * FROM kafka://data-topic
WITH (
    -- Table configuration
    'table_model' = 'compact',              -- Memory-efficient CompactTable
    'retention' = '30 days',                -- Keep data for 30 days
    'compression' = 'zstd',                 -- Use ZSTD compression

    -- Kafka consumer
    'kafka.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
    'kafka.group.id' = 'analytics-consumers',
    'kafka.auto.offset.reset' = 'earliest',
    'kafka.batch.size' = '2000',
    'kafka.linger.ms' = '50',
    'kafka.max.poll.records' = '5000',
    'kafka.compression.type' = 'zstd',

    -- Schema registry
    'value.format' = 'avro',
    'avro.schema.registry.url' = 'http://schema-registry:8081',

    -- Error handling
    'error.handling' = 'skip',
    'error.topic' = 'processing_errors',
    'error.max.retries' = '3'
)
EMIT CHANGES;
```

---

## Column Definition Syntax

### Explicit Column Definitions

You can optionally specify column names and types when creating tables or streams.

#### Basic Column Definitions

```sql
-- Explicit column types
CREATE TABLE typed_orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10, 2),
    status VARCHAR(50),
    created_at TIMESTAMP
) AS
SELECT order_id, customer_id, amount, status, created_at
FROM orders_stream;
```

#### Supported Column Types

| Type | Description | Example |
|------|-------------|---------|
| **BIGINT** | 64-bit integer | `order_id BIGINT` |
| **INTEGER** / **INT** | 32-bit integer | `quantity INT` |
| **DECIMAL(p, s)** | Fixed-point decimal | `amount DECIMAL(10, 2)` |
| **FLOAT** / **DOUBLE** | Floating-point number | `price FLOAT` |
| **VARCHAR(n)** | Variable-length string | `status VARCHAR(50)` |
| **STRING** / **TEXT** | Unlimited string | `description TEXT` |
| **BOOLEAN** / **BOOL** | True/false value | `active BOOLEAN` |
| **TIMESTAMP** | Date and time | `created_at TIMESTAMP` |
| **DATE** | Date only | `order_date DATE` |

#### Implicit Column Types (Schema Inference)

```sql
-- Schema inferred from SELECT clause
CREATE TABLE inferred_schema AS
SELECT
    order_id,           -- Type inferred as BIGINT
    amount,             -- Type inferred from source
    status,             -- Type inferred as STRING
    created_at          -- Type inferred as TIMESTAMP
FROM orders_stream
EMIT CHANGES;
```

#### When to Use Explicit vs Implicit

**Use Explicit Column Definitions When:**
- ✅ You need type enforcement (e.g., DECIMAL for financial data)
- ✅ You're creating a schema contract for downstream systems
- ✅ You need to validate data types at ingestion time
- ✅ Documentation clarity is important

**Use Implicit Schema Inference When:**
- ✅ Source schema is well-defined (Avro, Protobuf)
- ✅ You want rapid development without type management
- ✅ Schema can change over time

#### Column Definitions with Constraints

```sql
-- Note: Constraints are documented but not all are enforced yet
CREATE TABLE orders_with_constraints (
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) AS
SELECT order_id, customer_id, amount, status, created_at
FROM orders_stream;
```

---

## Reproducing Performance Benchmarks

All performance benchmarks in this guide were measured on **M1 Max, 32GB RAM** running macOS.

### Running Benchmark Tests

#### 1. Comprehensive Benchmark Suite

```bash
# Run all performance benchmarks
cargo test --release --test comprehensive_benchmark_test -- --nocapture

# Sample output:
# ✅ Key Lookups: 1,851,366/sec (540ns average)
# ✅ Data Loading: 103,771 records/sec
# ✅ Query Caching: 1.4x speedup
```

#### 2. CTAS-Specific Performance Tests

```bash
# Run CTAS performance tests
cargo test --test mod ctas_performance --no-default-features -- --nocapture

# Or run integration test
cargo test --test ctas_performance_test --no-default-features -- --nocapture
```

#### 3. Table Model Comparison

```bash
# Compare Standard Table vs CompactTable
cargo test --test mod test_performance_optimizations --no-default-features -- --nocapture

# Sample output:
# Standard Table: 100MB memory, 1.85M lookups/sec
# CompactTable: 10MB memory, 1.6M lookups/sec (90% memory reduction)
```

### Benchmark Code Locations

- **Comprehensive Benchmarks**: `tests/comprehensive_benchmark_test.rs`
- **CTAS Performance**: `tests/integration/ctas_performance_test.rs`
- **Table Performance**: `tests/unit/table/ctas_test.rs`
- **OptimizedTableImpl**: `tests/unit/table/optimized_table_test.rs`

### Hardware Requirements for Benchmarks

- **CPU**: Modern multi-core processor (Apple M1/M2, Intel i7/i9, AMD Ryzen 7/9)
- **RAM**: 16GB minimum, 32GB recommended
- **Storage**: SSD recommended for consistent I/O performance
- **Kafka**: Kafka broker running locally or on fast network

### Custom Benchmark Configuration

```rust
// Example: Create your own benchmark
#[test]
fn custom_ctas_benchmark() {
    let records = 1_000_000;
    let start = Instant::now();

    // Create CTAS table
    let ctas_sql = r#"
        CREATE TABLE benchmark_table AS
        SELECT id, name, value, timestamp
        FROM kafka://benchmark-topic
        WITH ('table_model' = 'compact')
    "#;

    // Execute and measure...

    let duration = start.elapsed();
    let throughput = records as f64 / duration.as_secs_f64();

    println!("Throughput: {:.0} records/sec", throughput);
}
```

### Interpreting Benchmark Results

**Key Metrics:**
- **Lookups/sec**: Higher is better (target: > 1M/sec)
- **Data Loading**: Records ingested per second (target: > 100K/sec)
- **Memory Usage**: Lower is better (CompactTable: 90% reduction)
- **CPU Overhead**: CompactTable: ~10% additional CPU
- **Latency**: EMIT CHANGES should be sub-millisecond

**Expected Performance Ranges:**

| Metric | Standard Table | CompactTable |
|--------|----------------|--------------|
| Lookups/sec | 1.5M - 2M | 1.3M - 1.8M |
| Memory (1M records) | 100MB - 1GB | 10MB - 100MB |
| Data Loading | 100K/sec | 80K/sec |
| Query Latency | Sub-ms | Sub-ms |

---

## Troubleshooting Guide

### Common CTAS Issues

#### Issue: Out of Memory Errors

**Symptoms:**
```
Error: OutOfMemoryError
Table: customer_data
Records: 5,000,000
```

**Solution 1: Use CompactTable**
```sql
CREATE TABLE customer_data AS
SELECT * FROM source
WITH (
    'table_model' = 'compact',  -- 90% memory reduction
    'kafka.batch.size' = '5000'
);
```

**Solution 2: Add Retention Limit**
```sql
CREATE TABLE customer_data AS
SELECT * FROM source
WITH (
    'table_model' = 'compact',
    'retention' = '7 days'      -- Limit data age
);
```

**Solution 3: Use CSAS Instead**
If you don't need SQL queries, use CSAS (stream-to-stream) which has minimal memory:
```sql
CREATE STREAM customer_data_stream AS
SELECT * FROM source
EMIT CHANGES;
```

#### Issue: Table Not Updating

**Symptoms:**
- CTAS table created but not receiving updates
- Empty table even though source has data

**Solution: Ensure EMIT CHANGES**
```sql
CREATE TABLE orders AS
SELECT * FROM orders_stream
EMIT CHANGES;  -- ← Required for continuous updates
```

**Solution: Check Kafka Offset Reset**
```sql
CREATE TABLE orders AS
SELECT * FROM orders_stream
WITH (
    'kafka.auto.offset.reset' = 'earliest'  -- Start from beginning
)
EMIT CHANGES;
```

#### Issue: Slow Query Performance

**Symptoms:**
- Queries taking > 100ms
- High CPU usage during queries

**Solution: Verify Table Model**
```sql
-- Ensure you're using the right model
-- For queries < 1M records: Use normal
-- For queries > 1M records: Use compact but expect slight slowdown
WITH ('table_model' = 'normal')  -- Fastest queries
```

**Solution: Check Table Statistics**
```bash
# Query table statistics to verify OptimizedTableImpl usage
SELECT * FROM SHOW TABLE STATISTICS;
```

#### Issue: Duplicate Table Error

**Symptoms:**
```
Error: Table 'orders' already exists
```

**Solution 1: Drop Existing Table**
```sql
DROP TABLE orders;
CREATE TABLE orders AS ...
```

**Solution 2: Use IF NOT EXISTS (if supported)**
```sql
CREATE TABLE IF NOT EXISTS orders AS ...
```

### Common CSAS Issues

#### Issue: Data Not Appearing in Output Topic

**Symptoms:**
- CSAS stream created but output topic is empty
- No errors reported

**Solution: Verify Sink Configuration**
```sql
CREATE STREAM output_stream AS
SELECT * FROM source
WITH (
    'sink.bootstrap.servers' = 'localhost:9092',  -- ← Verify broker
    'sink.topic' = 'output-topic'                  -- ← Verify topic
)
EMIT CHANGES;
```

**Solution: Check Topic Creation**
```bash
# Verify topic exists
kafka-topics --list --bootstrap-server localhost:9092

# Create topic manually if needed
kafka-topics --create --topic output-topic --partitions 3 --bootstrap-server localhost:9092
```

#### Issue: High Latency in Stream Processing

**Symptoms:**
- EMIT CHANGES taking > 1 second
- Batch processing delays

**Solution: Reduce Batch Size**
```sql
CREATE STREAM fast_stream AS
SELECT * FROM source
WITH (
    'kafka.batch.size' = '100',    -- Smaller batches
    'kafka.linger.ms' = '0'        -- No batching delay
)
EMIT CHANGES;
```

#### Issue: Schema Registry Errors

**Symptoms:**
```
Error: Failed to fetch schema from registry
SchemaId: 12345
```

**Solution: Verify Schema Registry Config**
```sql
CREATE STREAM avro_stream AS
SELECT * FROM source
WITH (
    'value.format' = 'avro',
    'avro.schema.registry.url' = 'http://schema-registry:8081',  -- ← Verify URL
    'avro.schema.id' = '12345'  -- ← Verify schema ID exists
)
EMIT CHANGES;
```

### Property Validation Errors

#### Issue: Invalid Property Value

**Symptoms:**
```
Error: Invalid value for property 'table_model': 'super_fast'
Valid values: 'normal', 'compact'
```

**Solution: Use Correct Property Values**
Refer to the [Complete WITH Clause Property Reference](#complete-with-clause-property-reference) table above.

#### Issue: Conflicting Properties

**Symptoms:**
```
Warning: 'retention' specified but 'table_model' is 'normal'
Long retention without compact model may use significant memory
```

**Solution: Align Properties**
```sql
CREATE TABLE large_data AS
SELECT * FROM source
WITH (
    'table_model' = 'compact',  -- ← Use compact for long retention
    'retention' = '30 days'
);
```

### Getting Help

If you encounter issues not covered here:

1. **Check Logs**: Review Velostream logs for detailed error messages
2. **Verify Configuration**: Use the property reference table to validate your WITH clause
3. **Test with Minimal Example**: Simplify your query to isolate the issue
4. **Check Kafka**: Verify Kafka broker is running and accessible
5. **Review Documentation**: See `docs/sql/ctas-vs-csas-guide.md` for usage patterns

---

## Performance Benchmarks

Based on comprehensive testing with the three-component architecture:

### Query Performance (OptimizedTableImpl)
- **Key Lookups**: 1,851,366/sec (540ns average)
- **Data Loading**: 103,771 records/sec
- **Query Caching**: 1.1-1.4x speedup for repeated queries
- **Aggregations**: Sub-millisecond for COUNT operations

### Memory Efficiency (CompactTable vs Standard)
- **Memory Usage**: 90% reduction with CompactTable
- **CPU Overhead**: ~10% additional processing time
- **Recommended**: CompactTable for >1M records, Standard for <1M records

### Streaming Performance
- **Throughput**: 102,222 records/sec streaming
- **Latency**: Sub-millisecond for EMIT CHANGES
- **Batch Processing**: 10,000+ records/sec with optimal configuration

---

## Best Practices

### 1. Performance Optimization
- Use **CompactTable** (`'table_model' = 'compact'`) for large datasets (>1M records)
- Use **Standard Table** (`'table_model' = 'normal'`) for real-time queries (<1M records)
- OptimizedTableImpl provides O(1) query performance automatically

### 2. Memory Management
- Set appropriate retention periods: `'retention' = '7 days'`
- Use compression for high-volume topics: `'kafka.compression.type' = 'zstd'`
- Monitor memory usage with table statistics

### 3. Real-Time Processing
- Use `EMIT CHANGES` for real-time CDC streams
- Use `EMIT FINAL` for complete window results
- Configure appropriate window sizes for your latency requirements

### 4. Error Handling
- Always configure error handling for file processing
- Use dead letter queues for unprocessable messages
- Monitor processing metrics and error rates

This comprehensive guide covers all major aspects of Velostream's SQL capabilities, providing production-ready examples and configuration for enterprise streaming analytics applications.