# SQL Integration Guide for Pluggable Data Sources

## Overview

This guide demonstrates how to use the new pluggable data sources through VeloStream' SQL layer, including CREATE STREAM statements, heterogeneous joins, and cross-source data pipelines.

## Table of Contents

1. [CREATE STREAM with Various Sources](#create-stream-with-various-sources)
2. [Heterogeneous Data Joins](#heterogeneous-data-joins)
3. [Schema Management in SQL](#schema-management-in-sql)
4. [Error Handling and Recovery](#error-handling-and-recovery)
5. [Performance Optimization](#performance-optimization)
6. [Production Examples](#production-examples)

## CREATE STREAM with Various Sources

### Kafka Sources

```sql
-- Basic Kafka stream creation
CREATE STREAM orders AS
SELECT * FROM 'kafka://localhost:9092/orders-topic'
WITH (
    group_id = 'analytics-group',
    auto_offset_reset = 'earliest',
    format = 'json'
);

-- Kafka with Avro and Schema Registry
CREATE STREAM transactions AS
SELECT * FROM 'kafka://localhost:9092/transactions'
WITH (
    format = 'avro',
    schema_registry_url = 'http://localhost:8081',
    key_format = 'string',
    value_format = 'avro'
);

-- Kafka with custom deserializer
CREATE STREAM events AS
SELECT * FROM 'kafka://broker1:9092,broker2:9092/events'
WITH (
    format = 'custom',
    deserializer_class = 'com.example.CustomDeserializer',
    batch_size = 1000,
    poll_timeout_ms = 5000
);
```

### File Sources

```sql
-- CSV file with header
CREATE STREAM sales_data AS
SELECT * FROM 'file:///data/sales/*.csv'
WITH (
    format = 'csv',
    header = true,
    delimiter = ',',
    quote_char = '"',
    skip_lines = 1
);

-- JSON Lines file
CREATE STREAM log_events AS
SELECT * FROM 'file:///logs/application/*.jsonl'
WITH (
    format = 'jsonl',
    timestamp_field = 'timestamp',
    timestamp_format = 'ISO8601'
);

-- Parquet files with partitioning
CREATE STREAM historical_data AS
SELECT * FROM 'file:///warehouse/data/year=*/month=*/*.parquet'
WITH (
    format = 'parquet',
    partition_columns = ['year', 'month'],
    projection_pushdown = true
);

-- Compressed files
CREATE STREAM compressed_logs AS
SELECT * FROM 'file:///archive/logs/*.gz'
WITH (
    format = 'json',
    compression = 'gzip',
    multiline = true
);
```

### S3 Sources

```sql
-- S3 bucket with prefix
CREATE STREAM s3_events AS
SELECT * FROM 's3://my-bucket/events/2024/*'
WITH (
    region = 'us-west-2',
    format = 'json',
    access_key_id = '${AWS_ACCESS_KEY_ID}',
    secret_access_key = '${AWS_SECRET_ACCESS_KEY}',
    endpoint = 'https://s3.amazonaws.com'
);

-- S3 with STS assume role
CREATE STREAM s3_analytics AS
SELECT * FROM 's3://analytics-bucket/data/*'
WITH (
    format = 'parquet',
    role_arn = 'arn:aws:iam::123456789012:role/DataReader',
    session_name = 'velostream-reader',
    external_id = 'unique-external-id'
);

-- S3 with server-side encryption
CREATE STREAM secure_data AS
SELECT * FROM 's3://secure-bucket/sensitive/*'
WITH (
    format = 'avro',
    sse_customer_algorithm = 'AES256',
    sse_customer_key = '${ENCRYPTION_KEY}',
    request_payer = 'RequesterPays'
);
```

### PostgreSQL Sources

```sql
-- PostgreSQL table as stream
CREATE STREAM pg_orders AS
SELECT * FROM 'postgresql://user:pass@localhost:5432/shop?table=orders'
WITH (
    mode = 'cdc',  -- Change Data Capture
    publication = 'velo_publication',
    slot_name = 'velo_slot',
    include_transaction = true
);

-- PostgreSQL query as stream
CREATE STREAM pg_aggregates AS
SELECT * FROM 'postgresql://localhost/analytics'
WITH (
    query = 'SELECT date_trunc(''hour'', created_at) as hour, 
             COUNT(*) as order_count, 
             SUM(amount) as total_amount 
             FROM orders 
             GROUP BY 1',
    poll_interval = '1 minute',
    timestamp_column = 'hour'
);

-- PostgreSQL with connection pooling
CREATE STREAM pg_realtime AS
SELECT * FROM 'postgresql://localhost/db?table=events'
WITH (
    pool_size = 20,
    pool_timeout = 30,
    statement_timeout = 60000,
    ssl_mode = 'require'
);
```

### ClickHouse Sources

```sql
-- ClickHouse table stream
CREATE STREAM clickhouse_metrics AS
SELECT * FROM 'clickhouse://localhost:8123/metrics?table=system_metrics'
WITH (
    format = 'JSONEachRow',
    database = 'system',
    compression = 'lz4',
    max_block_size = 65536
);

-- ClickHouse with distributed query
CREATE STREAM ch_distributed AS
SELECT * FROM 'clickhouse://cluster/analytics'
WITH (
    query = 'SELECT * FROM distributed_table WHERE date >= today() - 7',
    cluster = 'analytics_cluster',
    sharding_key = 'user_id'
);
```

## Heterogeneous Data Joins

### Kafka to PostgreSQL Join

```sql
-- Real-time enrichment: Join Kafka stream with PostgreSQL dimension table
CREATE STREAM enriched_orders AS
SELECT 
    o.order_id,
    o.product_id,
    o.quantity,
    o.price,
    p.product_name,
    p.category,
    c.customer_name,
    c.customer_segment
FROM 'kafka://localhost:9092/orders' o
LEFT JOIN 'postgresql://localhost/products?table=products' p
    ON o.product_id = p.id
LEFT JOIN 'postgresql://localhost/customers?table=customers' c
    ON o.customer_id = c.id
WITH (
    cache_ttl = '5 minutes',  -- Cache dimension data
    batch_size = 100
);
```

### File to Kafka Pipeline

```sql
-- Process CSV files and stream to Kafka
CREATE STREAM file_to_kafka_pipeline AS
SELECT 
    CAST(order_id AS BIGINT) as order_id,
    UPPER(customer_name) as customer_name,
    CAST(amount AS DECIMAL(10,2)) as amount,
    TO_TIMESTAMP(order_date, 'YYYY-MM-DD') as order_timestamp
FROM 'file:///data/daily/*.csv'
INTO 'kafka://localhost:9092/processed-orders'
WITH (
    source_format = 'csv',
    source_header = true,
    sink_format = 'avro',
    sink_compression = 'snappy',
    sink_key = 'order_id'
);
```

### Multi-Source Aggregation

```sql
-- Combine data from multiple sources
CREATE STREAM unified_events AS
SELECT 
    'kafka' as source,
    event_id,
    event_type,
    timestamp
FROM 'kafka://localhost:9092/events'

UNION ALL

SELECT 
    'file' as source,
    event_id,
    event_type,
    timestamp
FROM 'file:///logs/events/*.jsonl'

UNION ALL

SELECT 
    's3' as source,
    event_id,
    event_type,
    timestamp
FROM 's3://bucket/events/*'
WITH (
    deduplication_key = 'event_id',
    deduplication_window = '1 hour'
);
```

## Schema Management in SQL

### Automatic Schema Discovery

```sql
-- Let VeloStream discover the schema
CREATE STREAM auto_discovered AS
SELECT * FROM 'kafka://localhost:9092/topic'
WITH (
    schema_discovery = true,
    sample_size = 1000,
    infer_types = true
);

-- View discovered schema
DESCRIBE STREAM auto_discovered;

-- Show schema in JSON format
SHOW CREATE STREAM auto_discovered FORMAT JSON;
```

### Explicit Schema Definition

```sql
-- Define schema explicitly
CREATE STREAM typed_stream (
    id BIGINT NOT NULL,
    name VARCHAR(255),
    amount DECIMAL(10,2),
    created_at TIMESTAMP WITH TIME ZONE,
    metadata MAP<VARCHAR, VARCHAR>,
    tags ARRAY<VARCHAR>
) WITH (
    datasource = 'kafka://localhost:9092/typed-topic',
    format = 'json',
    timestamp_field = 'created_at'
);

-- With complex types
CREATE STREAM nested_stream (
    user_id BIGINT,
    profile STRUCT<
        name VARCHAR,
        email VARCHAR,
        preferences MAP<VARCHAR, VARCHAR>
    >,
    orders ARRAY<STRUCT<
        order_id BIGINT,
        amount DECIMAL(10,2),
        items ARRAY<VARCHAR>
    >>
) WITH (
    datasource = 'kafka://localhost:9092/users',
    format = 'json'
);
```

### Schema Evolution

```sql
-- Add new columns (forward compatible)
ALTER STREAM orders 
ADD COLUMN discount DECIMAL(5,2) DEFAULT 0.0,
ADD COLUMN promo_code VARCHAR(50);

-- Drop columns (requires backward compatibility)
ALTER STREAM orders 
DROP COLUMN obsolete_field;

-- Modify column types (with casting)
ALTER STREAM orders 
ALTER COLUMN amount TYPE DECIMAL(12,4);

-- Add constraints
ALTER STREAM orders
ADD CONSTRAINT amount_positive CHECK (amount > 0),
ADD CONSTRAINT valid_status CHECK (status IN ('pending', 'completed', 'cancelled'));
```

### Schema Registry Integration

```sql
-- Register schema with version
CREATE OR REPLACE STREAM versioned_stream
WITH (
    datasource = 'kafka://localhost:9092/versioned',
    schema_registry_url = 'http://localhost:8081',
    schema_id = 123,
    schema_version = 'latest',
    compatibility_mode = 'BACKWARD'
);

-- Use specific schema version
CREATE STREAM historical_view AS
SELECT * FROM 'kafka://localhost:9092/topic'
WITH (
    schema_version = 3,
    upgrade_missing_fields = true
);
```

## Error Handling and Recovery

### Dead Letter Queue Configuration

```sql
-- Configure DLQ for stream
CREATE STREAM orders_with_dlq AS
SELECT * FROM 'kafka://localhost:9092/orders'
WITH (
    on_error = 'send_to_dlq',
    dlq_topic = 'orders-dlq',
    max_retries = 3,
    retry_backoff_ms = 1000,
    error_tolerance = 'all'  -- or 'none'
);

-- Create DLQ processor
CREATE STREAM dlq_processor AS
SELECT 
    CAST(payload AS JSON) as original_record,
    error_message,
    error_timestamp,
    retry_count
FROM 'kafka://localhost:9092/orders-dlq'
WHERE retry_count < 5;
```

### Circuit Breaker Configuration

```sql
-- Stream with circuit breaker
CREATE STREAM protected_stream AS
SELECT * FROM 'postgresql://localhost/db?table=events'
WITH (
    circuit_breaker_enabled = true,
    failure_threshold = 5,
    timeout_seconds = 30,
    success_threshold = 2,
    fallback_query = 'SELECT * FROM backup_events LIMIT 100'
);
```

### Error Recovery Strategies

```sql
-- Skip corrupted records
CREATE STREAM resilient_stream AS
SELECT * FROM 'kafka://localhost:9092/events'
WITH (
    on_deserialization_error = 'skip',
    log_errors = true,
    metrics_enabled = true
);

-- Use default values for missing fields
CREATE STREAM with_defaults AS
SELECT 
    COALESCE(order_id, -1) as order_id,
    COALESCE(amount, 0.0) as amount,
    COALESCE(status, 'unknown') as status
FROM 'kafka://localhost:9092/orders'
WITH (
    null_handling = 'use_defaults'
);

-- Retry with exponential backoff
CREATE STREAM retry_stream AS
SELECT * FROM 'http://api.example.com/events'
WITH (
    retry_policy = 'exponential',
    initial_retry_delay_ms = 100,
    max_retry_delay_ms = 30000,
    max_retries = 5,
    jitter_factor = 0.1
);
```

## Performance Optimization

### Batch Processing

```sql
-- Optimize with batching
CREATE STREAM batch_optimized AS
SELECT * FROM 'kafka://localhost:9092/high-volume'
WITH (
    batch_size = 10000,
    linger_ms = 100,
    compression_type = 'lz4',
    buffer_memory = 67108864,  -- 64MB
    prefetch_count = 5
);
```

### Parallel Processing

```sql
-- Parallel consumption
CREATE STREAM parallel_stream AS
SELECT * FROM 'kafka://localhost:9092/topic'
WITH (
    parallelism = 10,
    partition_assignment = 'round_robin',
    max_poll_records = 5000
);

-- Parallel file processing
CREATE STREAM parallel_files AS
SELECT * FROM 'file:///data/*.csv'
WITH (
    parallelism = 4,
    file_parallelism = true,
    chunk_size_mb = 64
);
```

### Caching Configuration

```sql
-- Enable caching for joins
CREATE STREAM cached_join AS
SELECT 
    o.*,
    c.customer_name
FROM 'kafka://localhost:9092/orders' o
LEFT JOIN 'postgresql://localhost/customers?table=customers' c
    ON o.customer_id = c.id
WITH (
    cache_enabled = true,
    cache_size_mb = 512,
    cache_ttl_seconds = 300,
    cache_refresh_interval = 60
);
```

### Pushdown Optimization

```sql
-- Projection and filter pushdown
CREATE STREAM optimized_query AS
SELECT 
    order_id,
    amount,
    status
FROM 'parquet://s3://bucket/orders/*.parquet'
WHERE status = 'completed'
    AND amount > 100
WITH (
    projection_pushdown = true,
    predicate_pushdown = true,
    column_pruning = true
);
```

## Production Examples

### Real-Time Analytics Pipeline

```sql
-- Continuous aggregation with windowing
CREATE STREAM realtime_analytics AS
SELECT 
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(response_time) as avg_response_time,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time) as p95_latency
FROM 'kafka://localhost:9092/app-metrics'
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
EMIT CHANGES;

-- Output to multiple sinks
CREATE STREAM multi_sink_pipeline AS
WITH enriched AS (
    SELECT 
        e.*,
        u.user_segment,
        u.lifetime_value
    FROM 'kafka://localhost:9092/events' e
    JOIN 'postgresql://localhost/users?table=users' u
        ON e.user_id = u.id
)
SELECT * FROM enriched
INTO 'kafka://localhost:9092/enriched-events',
     'clickhouse://localhost:8123/analytics?table=events',
     'file:///archive/events/${date}/${hour}/'
WITH (
    sink_partitioning = 'user_id',
    sink_format = 'parquet'
);
```

### CDC Pipeline

```sql
-- PostgreSQL CDC to Kafka
CREATE STREAM cdc_pipeline AS
SELECT 
    operation,
    before,
    after,
    source.ts_ms as timestamp
FROM 'postgresql://localhost/shop?mode=cdc&table=orders'
INTO 'kafka://localhost:9092/orders-cdc'
WITH (
    include_transaction_details = true,
    snapshot_mode = 'initial',
    heartbeat_interval_ms = 10000
);

-- Process CDC events
CREATE STREAM cdc_processor AS
SELECT 
    CASE 
        WHEN operation = 'INSERT' THEN after
        WHEN operation = 'UPDATE' THEN after
        WHEN operation = 'DELETE' THEN before
    END as record,
    operation,
    timestamp
FROM 'kafka://localhost:9092/orders-cdc'
WHERE operation IN ('INSERT', 'UPDATE', 'DELETE');
```

### Data Lake Ingestion

```sql
-- Stream to data lake with partitioning
CREATE STREAM data_lake_ingestion AS
SELECT 
    *,
    EXTRACT(YEAR FROM event_time) as year,
    EXTRACT(MONTH FROM event_time) as month,
    EXTRACT(DAY FROM event_time) as day
FROM 'kafka://localhost:9092/raw-events'
INTO 's3://data-lake/events/year=${year}/month=${month}/day=${day}/'
WITH (
    format = 'parquet',
    compression = 'snappy',
    file_size_mb = 128,
    flush_interval = '5 minutes',
    partition_by = ['year', 'month', 'day']
);
```

### Machine Learning Feature Pipeline

```sql
-- Feature engineering pipeline
CREATE STREAM ml_features AS
WITH user_aggregates AS (
    SELECT 
        user_id,
        COUNT(*) as transaction_count_7d,
        AVG(amount) as avg_transaction_amount_7d,
        STDDEV(amount) as stddev_amount_7d,
        MAX(amount) as max_amount_7d
    FROM 'kafka://localhost:9092/transactions'
    WHERE transaction_time > NOW() - INTERVAL '7' DAY
    GROUP BY user_id
),
user_profile AS (
    SELECT * FROM 'postgresql://localhost/users?table=user_profiles'
)
SELECT 
    up.*,
    ua.transaction_count_7d,
    ua.avg_transaction_amount_7d,
    ua.stddev_amount_7d,
    ua.max_amount_7d,
    -- Feature engineering
    CASE 
        WHEN ua.transaction_count_7d > 10 THEN 'high'
        WHEN ua.transaction_count_7d > 5 THEN 'medium'
        ELSE 'low'
    END as activity_level
FROM user_profile up
LEFT JOIN user_aggregates ua ON up.user_id = ua.user_id
INTO 'kafka://localhost:9092/ml-features'
WITH (
    update_mode = 'append',
    checkpoint_interval = '1 minute'
);
```

## Monitoring and Management

### Stream Health Monitoring

```sql
-- Check stream health
SHOW STREAM HEALTH enriched_orders;

-- View stream statistics
SELECT * FROM system.stream_statistics
WHERE stream_name = 'enriched_orders';

-- Monitor consumer lag
SELECT 
    partition,
    current_offset,
    end_offset,
    (end_offset - current_offset) as lag
FROM system.kafka_consumer_status
WHERE stream_name = 'orders';
```

### Stream Management

```sql
-- Pause stream processing
ALTER STREAM orders PAUSE;

-- Resume stream processing
ALTER STREAM orders RESUME;

-- Reset stream offset
ALTER STREAM orders RESET OFFSET TO 'earliest';
-- or
ALTER STREAM orders RESET OFFSET TO TIMESTAMP '2024-01-15 10:00:00';

-- Scale stream processing
ALTER STREAM high_volume_stream 
SET parallelism = 20;

-- Update stream configuration
ALTER STREAM orders
SET 'batch_size' = '5000',
    'poll_timeout_ms' = '3000';
```

## Best Practices

1. **Always specify explicit schemas for production streams**
2. **Use appropriate batch sizes based on throughput requirements**
3. **Configure proper error handling and DLQ for critical pipelines**
4. **Enable caching for dimension table joins**
5. **Use projection/predicate pushdown for columnar formats**
6. **Monitor consumer lag and processing latency**
7. **Implement circuit breakers for external sources**
8. **Use schema registry for schema evolution**
9. **Partition output data for efficient querying**
10. **Set appropriate checkpointing intervals for stateful operations**

## References

- [Schema Registry Guide](./SCHEMA_REGISTRY_GUIDE.md)
- [Health Monitoring Guide](./HEALTH_MONITORING_GUIDE.md)
- [Performance Tuning Guide](./PERFORMANCE_GUIDE.md)
- [Migration Guide](./MIGRATION_GUIDE.md)